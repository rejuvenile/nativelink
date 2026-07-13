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

//! #245 — `VerifyStore::inner_check_update` writer-termination contract.
//!
//! Production symptom (server journal, ~15 events/10-min on ≥18 MB blobs):
//!
//! ```text
//! FastSlowStore::update (chunked): data stream failed,
//!   err: "Sender dropped before sending EOF",
//!   "Failed to read buffer in fast_slow chunked dispatch"
//! ```
//!
//! Mechanism (per audit `.claude/audits/245-fast-slow-store-sender-drop.md`):
//!
//! 1. Production composition is
//!    `cas_STORE → ExistenceCacheStore → VerifyStore → FastSlowStore`.
//! 2. `VerifyStore::update` runs
//!    `tokio::join!(inner_store.update(rx, ...), inner_check_update(tx, ...))`
//!    over a freshly-built `(tx, rx)` pair (`verify_store.rs:279-292`).
//! 3. `inner_check_update` takes `tx` BY VALUE. On any of five Err early
//!    returns (size overshoot, EOF non-empty after exact size, EOF size
//!    mismatch, hash mismatch, mid-stream send failure) the function used
//!    to drop `tx` SILENTLY without `send_eof` / `send_error`.
//! 4. The downstream inner store's `rx.recv()` then synthesized
//!    `Code::Internal "Sender dropped before sending EOF"`
//!    (`buf_channel.rs:582`) and emitted the production log shown above.
//! 5. The merged `update_res.merge(check_res)` did surface the upstream
//!    err in the FINAL Result, but the FastSlowStore's INNER error log
//!    fired UNCONDITIONALLY first — drowning out the actionable cause.
//!
//! Fix: wrap `tx` with `WriteHalfGuard` inside `inner_check_update` and
//! call `tx_guard.fail(err.clone())` at every Err early-return so the
//! inner store sees the SAME structured err the merged result carries
//! — eliminating the synthesized "Sender dropped" derivative.
//!
//! ## Test design
//!
//! **Production composition wrapper** (mandatory, per CLAUDE.md
//! test-in-production-composition rule): each test wraps a recording
//! inner store inside `VerifyStore` and triggers ONE of the 5 Err
//! branches in `inner_check_update`. The recording inner store reads
//! from `rx` and stores the observed result. Assertion: that observed
//! error MUST contain the structured upstream cause and MUST NOT
//! contain `"Sender dropped before sending EOF"`.
//!
//! **Bespoke deadlock-detector message:** every assertion is wrapped
//! in `tokio::time::timeout(NO_DEADLOCK_TIMEOUT)` with a panic message
//! that explicitly names the bug class. A `tokio::time::Elapsed` from
//! a too-short timeout would otherwise pass a generic `is_err()` and
//! mask the contract violation.
//!
//! **Mutation step (CLAUDE.md TDD step 5; load-bearing line):**
//! pick any explicit-fail site inside `inner_check_update` (e.g. the
//! over-shoot site at `verify_store.rs:119`, the under-shoot site at
//! `:148`, or the hash-mismatch site at `:160`) and replace
//! `return Err(tx_guard.fail(make_input_err!(...)));` with a bare
//! `return Err(make_input_err!(...));` — dropping the `tx_guard.fail`
//! wrap. With the wrap removed, the only termination of `tx` left is
//! the `WriteHalfGuard::Drop` fallback, which synthesizes
//! `"buf_channel: writer dropped without commit"` instead of the
//! structured upstream cause. The corresponding test's load-bearing
//! `assert!(!observed_err.messages.iter().any(|m| m.contains(
//! DROP_FALLBACK_IDENTIFIER)))` panics — the assertions live in the
//! over-shoot / hash-mismatch / under-shoot tests below; grep this
//! file for `DROP_FALLBACK_IDENTIFIER` to find them. Each carries a
//! bespoke per-branch message of the form `"Drop-fallback identifier
//! present on <site>: explicit `tx_guard.fail(...)` was bypassed..."`
//! so the panic names the site. The companion `EXPECTED_FRAGMENT`
//! positive assertion (grep this file for `EXPECTED_FRAGMENT`) would
//! ALSO fail because the inner err no longer carries the upstream
//! cause. Verified manually on 2026-05-04 during the testing-czar
//! review (see
//! `.claude/reviews/245-writer-termination-fix/testing-czar.md`
//! Mutation step section).

use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, UploadSizeInfo,
};
use opentelemetry::context::FutureExt;
use parking_lot::Mutex;

/// Tight upper bound. A non-deadlocked Err round-trip through VerifyStore is
/// sub-millisecond; production wedges shipped at 30s+. 5s leaves headroom
/// for a slow CI runner without masking a real deadlock.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Wire-side identifier the synthesized "Sender dropped" error carries
/// when the producer drops `tx` without `send_eof`/`send_error`. Any
/// test asserting that the explicit termination path fires MUST confirm
/// this string is ABSENT from the inner store's observed err.
const SENDER_DROPPED_IDENTIFIER: &str = "Sender dropped before sending EOF";

/// Wire-side identifier the `WriteHalfGuard::Drop` fallback synthesizes.
/// We use `tx_guard.fail(...)` explicitly (not Drop fallback), so this
/// MUST also be absent from the inner store's observed err — its presence
/// would mean the `fail` call was bypassed and only the Drop net caught
/// the bug.
const DROP_FALLBACK_IDENTIFIER: &str = "buf_channel: writer dropped without commit";

/// Valid SHA-256 hex string used for digest construction. The actual hash
/// computed during the verify path will not match this constant — that
/// triggers the `Hashes do not match` Err branch when verify_hash=true.
const VALID_HASH_HEX: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Inner store fake that records the result of reading the rx half. Models
/// the production inner store (FastSlowStore) inside VerifyStore::update —
/// the actual production log fires from FastSlowStore's
/// `data_stream_fut`'s `reader.recv().await.err_tip(...)?` chain. By
/// recording the reader err verbatim we observe what the production
/// FastSlowStore would have logged.
#[derive(Debug, MetricsComponent)]
struct ReaderObservingInnerStore {
    /// Captures the result of draining rx in the most recent `update`
    /// call. `Some(Ok(()))` = clean EOF; `Some(Err(e))` = early
    /// termination (either structured via `send_error` or synthesized
    /// "Sender dropped").
    last_observed: Arc<Mutex<Option<Result<(), Error>>>>,
    /// Set true the first time `update` is invoked. Lets tests verify
    /// the inner store was actually exercised (vs. the Err short-circuiting
    /// before VerifyStore reaches `inner_store.update`).
    update_was_called: Arc<AtomicBool>,
}

default_health_status_indicator!(ReaderObservingInnerStore);

impl ReaderObservingInnerStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            last_observed: Arc::new(Mutex::new(None)),
            update_was_called: Arc::new(AtomicBool::new(false)),
        })
    }
}

#[async_trait]
impl StoreDriver for ReaderObservingInnerStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        self.update_was_called.store(true, Ordering::Release);
        // Drain rx until EOF or Err — record what we saw. This is the
        // production analog of FastSlowStore::update_via_chunked_dispatcher's
        // `reader.recv().await.err_tip(...)?` loop that emitted the #245
        // log line.
        let observed: Result<(), Error> = async {
            loop {
                let chunk = reader
                    .recv()
                    .await
                    .err_tip(|| "Failed to read buffer in fast_slow chunked dispatch")?;
                if chunk.is_empty() {
                    return Ok(());
                }
            }
        }
        .await;

        *self.last_observed.lock() = Some(observed.clone());
        // Mirror what the inner store does: bubble up the err it saw
        // so VerifyStore's tokio::join! merges both halves consistently.
        observed.map(|()| 0)
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        unreachable!("get_part not exercised by these tests")
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
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// Build a VerifyStore wrapping a fresh `ReaderObservingInnerStore`.
/// Returns the wrapped store and a clone of the inner so the test can
/// inspect what the inner observed via `rx.recv()`.
fn build_verify_around_observing(
    verify_size: bool,
    verify_hash: bool,
) -> (Arc<VerifyStore>, Arc<ReaderObservingInnerStore>) {
    let inner = ReaderObservingInnerStore::new();
    let verify = VerifyStore::new(
        &VerifySpec {
            // backend is a placeholder; the actual delegate is the
            // `inner_store` arg passed to VerifyStore::new (mirrors the
            // composability_test.rs harness).
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size,
            verify_hash,
        },
        Store::new(inner.clone()),
    );
    (verify, inner)
}

/// Send `body` chunks then `send_eof` to drive a VerifyStore::update
/// from the producer side. Mirrors the `tokio::try_join!(send_fut,
/// store.update(...))` pattern in `verify_store_test.rs:84-92`.
async fn drive_verify_update(
    verify: Arc<VerifyStore>,
    digest: DigestInfo,
    body: &[&[u8]],
    upload_size: UploadSizeInfo,
) -> Result<(), Error> {
    let (mut tx, rx) = make_buf_channel_pair();
    let body_owned: Vec<Bytes> = body.iter().map(|b| Bytes::copy_from_slice(b)).collect();
    let send_fut = async move {
        for chunk in body_owned {
            tx.send(chunk).await?;
        }
        tx.send_eof()?;
        Result::<(), Error>::Ok(())
    };
    let update_fut = async move {
        Pin::new(verify.as_ref())
            .update(StoreKey::Digest(digest), rx, upload_size)
            .await
    };
    let (send_res, update_res) = tokio::join!(send_fut, update_fut);
    // We care primarily about update_res (the merged outer result that
    // contains both halves' errs). The send_res Err on a closed pipe
    // is expected on the early-Err branches; surface its err only if
    // update_res was Ok (defensive — should never happen on these tests).
    update_res.map(|_| ()).or_else(|update_err| {
        if let Err(send_err) = send_res {
            Err(update_err.merge(send_err))
        } else {
            Err(update_err)
        }
    })
}

// -----------------------------------------------------------------------
// Branch 1: size over-shoot at `verify_store.rs:90-97` (line 92 pre-fix).
// -----------------------------------------------------------------------

/// Push a 100-byte body with declared digest size 50. `inner_check_update`
/// crosses the over-shoot threshold on the first chunk, returns
/// `make_input_err!("Expected size 50 but already received 100 on insert")`.
///
/// Pre-fix: `tx` dropped silently → inner store's `rx.recv()` returns
/// `Code::Internal "Sender dropped before sending EOF"`.
///
/// Post-fix: `tx_guard.fail(err.clone())` puts the structured
/// InvalidArgument into `terminal_error` → inner observes the SAME
/// structured InvalidArgument → no `Sender dropped` derivative.
#[nativelink_test]
async fn verify_store_inner_check_update_over_shoot_propagates_structured_err() -> Result<(), Error>
{
    const DECLARED_SIZE: u64 = 50;
    const ACTUAL_BODY: &[u8] = &[b'X'; 100];
    const EXPECTED_FRAGMENT: &str = "Expected size 50 but already received 100 on insert";

    let (verify, observing) = build_verify_around_observing(true, false);
    let digest = DigestInfo::try_new(VALID_HASH_HEX, DECLARED_SIZE)?;

    let outer_res = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        drive_verify_update(
            verify,
            digest,
            &[ACTUAL_BODY],
            UploadSizeInfo::ExactSize(DECLARED_SIZE),
        ),
    )
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_245_over_shoot: \
         VerifyStore::update did not return within 5s when inner_check_update \
         hit the size over-shoot Err branch. The `inner_check_update` \
         tx_guard.fail(...) call must terminate tx so the paired inner \
         store's rx.recv() returns instead of blocking forever.",
    );

    assert!(
        outer_res.is_err(),
        "outer res must Err on size overshoot; got {outer_res:?}",
    );
    let outer_err = outer_res.unwrap_err();
    assert!(
        outer_err
            .messages
            .iter()
            .any(|m| m.contains(EXPECTED_FRAGMENT)),
        "outer err must carry the structured size-overshoot message; got {outer_err:?}",
    );

    let observed = observing
        .last_observed
        .lock()
        .as_ref()
        .expect("inner store must have observed the rx termination")
        .clone();
    let observed_err = observed.expect_err(
        "OVER_ACTION_SIBLING_FALSE: inner store observed Ok on rx — but \
         VerifyStore::inner_check_update returned Err, so the inner store's \
         rx MUST observe an Err. If this fires the over-action contract is \
         broken (we terminated tx with EOF instead of an error).",
    );

    // The inner err must carry the SAME structured size-overshoot message
    // (via `terminal_error` populated by `tx_guard.fail` → `send_error`).
    assert!(
        observed_err
            .messages
            .iter()
            .any(|m| m.contains(EXPECTED_FRAGMENT)),
        "verify_store::inner_check_update Err must propagate the inner error to rx \
         via send_error, not synthesize 'Sender dropped before sending EOF' (#245). \
         Got inner observed: {observed_err:?}",
    );

    // Bug-class assertions: the synthesized "Sender dropped" Internal MUST
    // NOT appear (that's the production #245 symptom string). The
    // WriteHalfGuard Drop-fallback identifier ALSO must not appear (we
    // call `fail` explicitly, not Drop).
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "#245 symptom: inner observed the synthesized 'Sender dropped before sending \
         EOF' Internal — meaning inner_check_update's Err branch dropped tx without \
         calling send_error. The `tx_guard.fail(err.clone())` call at the over-shoot \
         site is the load-bearing fix. Got: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "Inner observed the WriteHalfGuard Drop-fallback identifier — that means the \
         explicit `tx_guard.fail(...)` call was bypassed and only the Drop net caught \
         the contract violation. Use `tx_guard.fail(err.clone())` at the over-shoot \
         site, not bare `return Err(...)`. Got: {observed_err:?}",
    );

    assert!(
        observing.update_was_called.load(Ordering::Acquire),
        "inner store update must have been invoked",
    );
    Ok(())
}

// -----------------------------------------------------------------------
// Branch 2: hash mismatch at `verify_store.rs:131-136` (line 133 pre-fix).
// -----------------------------------------------------------------------

/// Push exactly the right number of bytes, but the hash will not match
/// the declared digest hex (`VALID_HASH_HEX` is just a placeholder,
/// the bytes "abcde" hash to something else). `inner_check_update`
/// crosses to EOF, computes the hasher, returns
/// `make_input_err!("Hashes do not match, ...")`.
///
/// Pre-fix: `tx` dropped silently → inner observes `"Sender dropped"`.
/// Post-fix: structured "Hashes do not match" propagates via terminal_error.
#[nativelink_test]
async fn verify_store_inner_check_update_hash_mismatch_propagates_structured_err()
-> Result<(), Error> {
    const BODY: &[u8] = b"abcde";
    const EXPECTED_FRAGMENT: &str = "Hashes do not match";

    let (verify, observing) = build_verify_around_observing(false, true);
    let digest = DigestInfo::try_new(VALID_HASH_HEX, BODY.len() as u64)?;

    // Run inside a SHA-256 hash context so VerifyStore picks a hasher
    // (otherwise default_digest_hasher_func is used; either is fine for
    // mismatch, but pinning the hasher makes the test deterministic).
    let outer_res = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        drive_verify_update(
            verify,
            digest,
            &[BODY],
            UploadSizeInfo::ExactSize(BODY.len() as u64),
        )
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Sha256)?),
    )
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_245_hash_mismatch: \
         VerifyStore::update did not return within 5s when inner_check_update \
         hit the hash-mismatch Err branch. The `tx_guard.fail(...)` call must \
         terminate tx so the paired inner store's rx.recv() returns.",
    );

    assert!(
        outer_res.is_err(),
        "outer res must Err on hash mismatch; got {outer_res:?}",
    );
    let outer_err = outer_res.unwrap_err();
    assert!(
        outer_err
            .messages
            .iter()
            .any(|m| m.contains(EXPECTED_FRAGMENT)),
        "outer err must carry the structured hash-mismatch message; got {outer_err:?}",
    );

    let observed = observing
        .last_observed
        .lock()
        .as_ref()
        .expect("inner store must have observed the rx termination")
        .clone();
    let observed_err = observed.expect_err(
        "OVER_ACTION_SIBLING_FALSE: inner store observed Ok on rx — but \
         VerifyStore::inner_check_update returned Err on hash mismatch, so the \
         inner store's rx MUST observe an Err.",
    );

    assert!(
        observed_err
            .messages
            .iter()
            .any(|m| m.contains(EXPECTED_FRAGMENT)),
        "verify_store::inner_check_update Err must propagate the inner error to rx \
         via send_error, not synthesize 'Sender dropped before sending EOF' (#245). \
         Got inner observed: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "#245 symptom: inner observed the synthesized 'Sender dropped before sending \
         EOF' Internal on the hash-mismatch branch. The `tx_guard.fail(err.clone())` \
         call at the hash-mismatch site is the load-bearing fix. Got: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "Inner observed the Drop-fallback identifier on hash mismatch — explicit \
         `tx_guard.fail` was bypassed. Got: {observed_err:?}",
    );

    Ok(())
}

// -----------------------------------------------------------------------
// Branch 3: under-shoot at EOF at `verify_store.rs:119-126` (line 121 pre-fix).
// -----------------------------------------------------------------------

/// Push 3 bytes then EOF, declaring 100 bytes. `inner_check_update` sees
/// an empty (EOF) chunk while `sum_size != expected_size`, returns
/// `make_input_err!("Expected size 100 but got size 3 on insert")`.
///
/// Pre-fix: `tx` dropped silently → inner observes `"Sender dropped"`.
/// Post-fix: structured size-mismatch propagates via terminal_error.
///
/// This branch is the symmetric counterpart of the over-shoot test above
/// AND is the same case `verify_store_test.rs::verify_size_true_fails_on_update`
/// already exercises — but THAT test only asserts the OUTER err contains
/// the message; it never wraps an inner-store observer to verify the
/// rx-side observed the structured propagation. The bug shipped under
/// THAT test passing for two years (per audit "send-side test passed,
/// receive-side bug shipped"). This test is the receive-side guard.
#[nativelink_test]
async fn verify_store_inner_check_update_under_shoot_eof_propagates_structured_err()
-> Result<(), Error> {
    const DECLARED_SIZE: u64 = 100;
    const ACTUAL_BODY: &[u8] = b"abc";
    const EXPECTED_FRAGMENT: &str = "Expected size 100 but got size 3 on insert";

    let (verify, observing) = build_verify_around_observing(true, false);
    let digest = DigestInfo::try_new(VALID_HASH_HEX, DECLARED_SIZE)?;

    let outer_res = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        drive_verify_update(
            verify,
            digest,
            &[ACTUAL_BODY],
            UploadSizeInfo::ExactSize(DECLARED_SIZE),
        ),
    )
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_245_under_shoot_eof: \
         VerifyStore::update did not return within 5s when inner_check_update \
         hit the under-shoot-on-EOF Err branch. The `tx_guard.fail(...)` call \
         must terminate tx so the paired inner store's rx.recv() returns.",
    );

    assert!(
        outer_res.is_err(),
        "outer res must Err on under-shoot at EOF; got {outer_res:?}",
    );

    let observed = observing
        .last_observed
        .lock()
        .as_ref()
        .expect("inner store must have observed the rx termination")
        .clone();
    let observed_err = observed.expect_err("inner must observe rx Err on under-shoot at EOF");

    assert!(
        observed_err
            .messages
            .iter()
            .any(|m| m.contains(EXPECTED_FRAGMENT)),
        "verify_store::inner_check_update Err must propagate the inner error to rx \
         via send_error, not synthesize 'Sender dropped before sending EOF' (#245). \
         Got inner observed: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "#245 symptom: inner observed 'Sender dropped' on the under-shoot branch. \
         Got: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "Drop-fallback identifier present on under-shoot — explicit fail bypassed. \
         Got: {observed_err:?}",
    );

    Ok(())
}

// -----------------------------------------------------------------------
// Negative control: happy path. Confirms the fix didn't introduce a
// new termination bug on the success branch.
// -----------------------------------------------------------------------

/// Push exactly the declared size with a clean EOF. `inner_check_update`
/// hits the success branch (`tx_guard.commit_eof()` then break). Inner
/// store observes Ok(()). Both halves merge to Ok(()).
///
/// If this test fails, the fix has broken the happy path — the
/// `commit_eof()` either lies about completeness (sends an Err
/// instead of EOF) or fails to suppress the Drop fallback.
#[nativelink_test]
async fn verify_store_inner_check_update_happy_path_succeeds() -> Result<(), Error> {
    const BODY: &[u8] = b"hello-world";

    let (verify, observing) = build_verify_around_observing(true, false);
    let digest = DigestInfo::try_new(VALID_HASH_HEX, BODY.len() as u64)?;

    let outer_res = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        drive_verify_update(
            verify,
            digest,
            &[BODY],
            UploadSizeInfo::ExactSize(BODY.len() as u64),
        ),
    )
    .await
    .expect("happy path must complete within 5s");

    assert!(
        outer_res.is_ok(),
        "happy path must return Ok; got {outer_res:?}",
    );

    let observed = observing
        .last_observed
        .lock()
        .as_ref()
        .expect("inner store must have observed the rx termination")
        .clone();
    assert!(
        observed.is_ok(),
        "happy path: inner must observe a clean EOF (Ok(())) on rx; got {observed:?}",
    );

    assert!(
        observing.update_was_called.load(Ordering::Acquire),
        "inner store update must have been invoked",
    );
    Ok(())
}

// -----------------------------------------------------------------------
// Sibling coverage (testing-czar follow-up): the 3 of 6 explicit
// `tx_guard.fail(...)` sites in `inner_check_update` that the original
// #245 test set did NOT exercise — recv-err propagation (line 110),
// peek-EOF non-empty after exact size (line 132), mid-stream send Err
// (line 188). Each test:
//   - injects the specific failure at the targeted site,
//   - asserts the inner store observes a structured upstream cause via
//     the SAME `terminal_error` machinery the #245 fix established,
//   - uses a bespoke per-site message so a future regression panics with
//     a string that names the site,
//   - is wrapped in `tokio::time::timeout(NO_DEADLOCK_TIMEOUT)` to
//     surface contract-violation deadlocks rather than hangs.
// -----------------------------------------------------------------------

/// Drive `VerifyStore::update` with a producer that sends `body` chunks
/// then injects `tx.send_error(injected)` instead of EOF — triggers
/// `inner_check_update`'s recv-err Err branch (`verify_store.rs:110`).
async fn drive_verify_update_inject_recv_err(
    verify: Arc<VerifyStore>,
    digest: DigestInfo,
    body: &[&[u8]],
    upload_size: UploadSizeInfo,
    injected: Error,
) -> Result<(), Error> {
    let (mut tx, rx) = make_buf_channel_pair();
    let body_owned: Vec<Bytes> = body.iter().map(|b| Bytes::copy_from_slice(b)).collect();
    let send_fut = async move {
        for chunk in body_owned {
            tx.send(chunk).await?;
        }
        // Inject a structured terminal_error on the OUTER tx — the
        // VerifyStore reader will observe this as the rx.recv() Err
        // and `inner_check_update` will hit the line 110 site.
        tx.send_error(injected);
        Result::<(), Error>::Ok(())
    };
    let update_fut = async move {
        Pin::new(verify.as_ref())
            .update(StoreKey::Digest(digest), rx, upload_size)
            .await
    };
    let (send_res, update_res) = tokio::join!(send_fut, update_fut);
    update_res.map(|_| ()).or_else(|update_err| {
        if let Err(send_err) = send_res {
            Err(update_err.merge(send_err))
        } else {
            Err(update_err)
        }
    })
}

/// Branch 4 — recv-err propagation at `verify_store.rs:110`.
///
/// Producer signals a structured terminal_error on the outer tx after
/// sending one chunk. `inner_check_update`'s `rx.recv().await.err_tip(...)`
/// returns Err on the second recv → `.map_err(|err| tx_guard.fail(err))?`
/// at line 110 propagates the structured upstream cause to the inner
/// store via `terminal_error`. Without the explicit `tx_guard.fail(err)`,
/// only the Drop fallback would fire and the inner err would carry
/// `DROP_FALLBACK_IDENTIFIER` instead of the producer's injected message.
#[nativelink_test]
async fn verify_store_inner_check_update_recv_err_propagates_structured_err()
-> Result<(), Error> {
    const BODY_CHUNK: &[u8] = b"chunk-bytes";
    const INJECTED_FRAGMENT: &str = "VERIFY_RECV_ERR_PROPAGATION_PROBE";

    let (verify, observing) = build_verify_around_observing(false, false);
    // Use a generous declared size so the over-shoot branch doesn't
    // pre-empt the recv-err path. verify_size=false above also disables
    // the size cmp entirely.
    let digest = DigestInfo::try_new(VALID_HASH_HEX, 1024)?;
    let injected = make_err!(Code::Aborted, "{INJECTED_FRAGMENT}");

    let outer_res = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        drive_verify_update_inject_recv_err(
            verify,
            digest,
            &[BODY_CHUNK],
            UploadSizeInfo::MaxSize(1024),
            injected,
        ),
    )
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_245_recv_err: \
         VerifyStore::update did not return within 5s when inner_check_update \
         hit the recv-err Err branch at verify_store.rs:110. The \
         `tx_guard.fail(err)` (via `.map_err(|e| tx_guard.fail(e))?`) call \
         must terminate tx so the paired inner store's rx.recv() returns.",
    );

    assert!(
        outer_res.is_err(),
        "outer res must Err on injected recv err; got {outer_res:?}",
    );

    let observed = observing
        .last_observed
        .lock()
        .as_ref()
        .expect("inner store must have observed the rx termination")
        .clone();
    let observed_err = observed.expect_err(
        "inner store must observe rx Err when producer injects send_error \
         (recv-err propagation site at verify_store.rs:110)",
    );

    assert!(
        observed_err
            .messages
            .iter()
            .any(|m| m.contains(INJECTED_FRAGMENT)),
        "verify_store::inner_check_update recv-err site (line 110) must propagate \
         the producer's structured err to rx via tx_guard.fail. The injected \
         fragment {INJECTED_FRAGMENT:?} should appear in the inner observed err. \
         Got inner observed: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "#245 symptom on recv-err site (line 110): inner observed 'Sender dropped' \
         instead of the structured upstream cause. The line 110 \
         `.map_err(|err| tx_guard.fail(err))?` is the load-bearing fix. \
         Got: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "Drop-fallback identifier present on recv-err site (line 110) — explicit \
         `tx_guard.fail(err)` was bypassed and only the Drop net caught the \
         contract violation. Use `.map_err(|err| tx_guard.fail(err))?` at the \
         recv site, not bare `?`. Got: {observed_err:?}",
    );

    Ok(())
}

/// Branch 5 — peek-EOF non-empty after exact size at `verify_store.rs:132`.
///
/// Producer sends EXACTLY `expected_size` bytes followed by a non-empty
/// chunk (instead of an EOF chunk). `inner_check_update` enters the
/// `Equal` arm, calls `rx.peek().await`, observes the non-empty chunk,
/// returns `Err(tx_guard.fail(make_input_err!("Expected EOF chunk when
/// exact size was hit on insert in verify store - {expected_size}")))`.
/// Without the explicit `tx_guard.fail`, only the Drop fallback would
/// fire and the inner err would carry `DROP_FALLBACK_IDENTIFIER` instead
/// of the structured "Expected EOF chunk" cause.
#[nativelink_test]
async fn verify_store_inner_check_update_peek_eof_non_empty_propagates_structured_err()
-> Result<(), Error> {
    const DECLARED_SIZE: u64 = 5;
    const FIRST_CHUNK: &[u8] = b"abcde"; // exactly 5 bytes
    const TRAILING_CHUNK: &[u8] = b"X"; // non-empty after the exact-size hit
    const EXPECTED_FRAGMENT: &str = "Expected EOF chunk when exact size was hit on insert";

    let (verify, observing) = build_verify_around_observing(true, false);
    let digest = DigestInfo::try_new(VALID_HASH_HEX, DECLARED_SIZE)?;

    let outer_res = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        drive_verify_update(
            verify,
            digest,
            &[FIRST_CHUNK, TRAILING_CHUNK],
            UploadSizeInfo::ExactSize(DECLARED_SIZE),
        ),
    )
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_245_peek_eof_non_empty: \
         VerifyStore::update did not return within 5s when inner_check_update \
         hit the peek-EOF-non-empty Err branch at verify_store.rs:132. The \
         `tx_guard.fail(...)` call must terminate tx so the paired inner \
         store's rx.recv() returns.",
    );

    assert!(
        outer_res.is_err(),
        "outer res must Err when peek sees non-empty chunk after exact size; got {outer_res:?}",
    );

    let observed = observing
        .last_observed
        .lock()
        .as_ref()
        .expect("inner store must have observed the rx termination")
        .clone();
    let observed_err = observed.expect_err(
        "inner store must observe rx Err on peek-EOF-non-empty site \
         (verify_store.rs:132)",
    );

    assert!(
        observed_err
            .messages
            .iter()
            .any(|m| m.contains(EXPECTED_FRAGMENT)),
        "verify_store::inner_check_update peek-EOF-non-empty site (line 132) must \
         propagate the structured 'Expected EOF chunk' message to rx via \
         tx_guard.fail. Got inner observed: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "#245 symptom on peek-EOF-non-empty site (line 132): inner observed \
         'Sender dropped' instead of the structured upstream cause. Got: \
         {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "Drop-fallback identifier present on peek-EOF-non-empty site (line 132) \
         — explicit `tx_guard.fail(...)` was bypassed. Got: {observed_err:?}",
    );

    Ok(())
}
