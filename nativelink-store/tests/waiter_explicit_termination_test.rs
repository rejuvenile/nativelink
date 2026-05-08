// Copyright 2024-2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// See LICENSE file for details.

//! Task #191: explicit termination at the four `commit_with_inner_miss_gate`
//! call sites in `FastSlowStore::get_part`. Without WPS in the chain (the
//! COMMON case), the helper previously fell back to
//! `commit_delegated_if_ok(&Err)` which left the `WriteHalfGuard` Drop
//! fallback armed → fired the synthesized `"buf_channel: writer dropped
//! without commit"` Internal AND the `error!` log
//! `"WriteHalfGuard fired Drop fallback: function exited without explicit
//! commit_eof / commit_delegated_if_ok / fail. This is a bug …"`.
//!
//! Production observation (post-deploy of `06a6ced6`, 17-min window):
//! `"Tried to send while stream is closed"` 60.5/min → 0/min ✓ (gate works
//! for WPS callers), but `"WriteHalfGuard fired Drop fallback"` 80.8/min
//! → ~835/min (+10×) — the Drop log fires for every non-WPS slow-store
//! fallback Err, masquerading the structured upstream error as a generic
//! Internal "writer dropped" on the wire.
//!
//! Fix: extend `commit_with_inner_miss_gate` to call `guard.fail(err)`
//! when the gate is NOT set, replacing the Drop fallback with explicit
//! termination. The gate-set suppression is preserved (so WPS still
//! benefits from peer-fetch recovery on a still-open OUTER writer).
//!
//! This test guards the non-WPS path: when no `INNER_MISS_NO_TERMINATE`
//! is in effect (no WPS in the chain), the wire-side error MUST carry
//! the structured upstream Code (e.g. `NotFound` with the slow store's
//! original message) — NOT the Drop synthesizer's `"buf_channel: writer
//! dropped without commit"` Internal.

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::noop_store::NoopStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Distinctive marker the slow store puts in its NotFound error message.
/// The test asserts this marker reaches the wire — proving the structured
/// upstream error survives the helper's Err arm. With the bug, the Drop
/// fallback fires `"buf_channel: writer dropped without commit"` instead,
/// so this marker is replaced.
const SLOW_STORE_NOTFOUND_MARKER: &str = "task-191-slow-store-notfound-marker";

/// Slow store that returns `Err(NotFound, marker)` synchronously from
/// `get_part`. Models the production case where the slow tier really is
/// empty for the requested digest. The structured `make_err!` carries
/// the marker through to `tx.send_error(err)` if the helper's Err arm
/// calls `guard.fail(err)` — and is REPLACED by the synthesized
/// `"buf_channel: writer dropped without commit"` Internal if the helper
/// instead falls through to the Drop fallback (the #191 bug).
#[derive(Debug, MetricsComponent)]
struct StructuredNotFoundSlowStore {
    _marker: (),
}

default_health_status_indicator!(StructuredNotFoundSlowStore);

#[async_trait]
impl StoreDriver for StructuredNotFoundSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for r in results.iter_mut() {
            *r = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // Distinctive marker so the test can grep for it on the wire.
        // Note: we do NOT call writer.send_error / send_eof here. A
        // well-behaved store WOULD terminate on Err, but this test
        // exercises FastSlowStore's outer guard's Err handling — which
        // must terminate the OUTER writer regardless of whether the
        // sub-store's contract was violated. Either way the `err`
        // carries the marker and FastSlowStore's helper must propagate
        // it (via `guard.fail(err)`) instead of falling through to Drop.
        Err(make_err!(
            Code::NotFound,
            "{}",
            SLOW_STORE_NOTFOUND_MARKER
        ))
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
}

/// Reproducer for the #191 over-action sibling of #171: when no WPS sets
/// `INNER_MISS_NO_TERMINATE`, the helper's Err arm at the slow-store-
/// fallback (Site 1, NoopUpdates / ReadOnly bypass at
/// `fast_slow_store.rs:3219`) used to call `commit_delegated_if_ok(&Err)`
/// which left the Drop fallback armed. The reader saw the synthesized
/// Internal `"buf_channel: writer dropped without commit"`, NOT the
/// structured `Code::NotFound` carrying the slow store's original
/// message — and the loud `error!` `"WriteHalfGuard fired Drop fallback"`
/// log fired ~835/min in production.
///
/// Test design: we hand-build the (tx, rx) pair (no VerifyStore wrapper)
/// so the ONLY `WriteHalfGuard` in the chain is FastSlowStore's outer
/// guard. The reader-side `rx.recv` then observes the EXACT terminal
/// error — either the structured slow-store NotFound (fix in place) or
/// the Drop synthesizer's `"buf_channel: writer dropped without commit"`
/// Internal (bug present). The wire-side identifier is the most
/// reliable distinguisher; the operator-side `tracing::error!` log can
/// be confounded by other guards in production composition (e.g.
/// VerifyStore's tx_guard fires its OWN Drop fallback on the same
/// flow), so we deliberately avoid stacking those layers.
///
/// Mutation step (per CLAUDE.md): in
/// `fast_slow_store.rs::commit_with_inner_miss_gate`, comment out the
/// `let _ = guard.fail(err.clone());` line in the Err arm. The test
/// will then fail with the structured-marker assertion below — proving
/// the `guard.fail(err.clone())` call IS what makes the test pass.
#[nativelink_test]
async fn non_wps_slow_store_fallback_err_terminates_writer_with_structured_error()
-> Result<(), Error> {
    // Fast tier = NoopStore so FastSlowStore::get_part takes the
    // bypass branch at `fast_slow_store.rs:~3195` (`optimized_for(NoopUpdates)`
    // true) and delegates straight to slow_store.get_part. This is
    // Site 1 of the four `commit_with_inner_miss_gate` call sites.
    let fast = Store::new(NoopStore::new());
    let slow = Store::new(Arc::new(StructuredNotFoundSlowStore { _marker: () }));
    let spec = FastSlowSpec {
        fast: StoreSpec::Memory(MemorySpec::default()),
        slow: StoreSpec::Memory(MemorySpec::default()),
        fast_direction: StoreDirection::Both,
        slow_direction: StoreDirection::Both,
        chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
    };
    let fast_slow = FastSlowStore::new(&spec, fast, slow);

    let digest = DigestInfo::try_new(VALID_HASH, 100)?;

    // Hand-build a single buf_channel pair so the OUTER writer is the
    // ONLY one in the chain. No VerifyStore / WPS in the way — the
    // reader sees the exact terminal error FastSlowStore's outer guard
    // applied to its writer.
    let (mut tx, mut rx) = make_buf_channel_pair();

    // Run get_part inside a 5s deadlock detector. With the fix in place
    // the helper's `guard.fail(err)` terminates `tx` synchronously
    // before get_part returns; without the fix the WriteHalfGuard Drop
    // fallback terminates `tx` (still unblocks `rx.recv`, but with the
    // synthesized Internal we want to detect).
    //
    // We use the `StoreLike::get_part` impl directly via the
    // `FastSlowStore` value (which `Store::new` would erase to a
    // trait object); that impl forwards to
    // `as_store_driver_pin().get_part(...)`. Passing `&mut tx` lets the
    // outer-fn drop the borrow at await-completion so we can `rx.recv`
    // afterwards without an outstanding mutable borrow.
    let get_res = tokio::time::timeout(
        Duration::from_secs(5),
        fast_slow.get_part(digest, &mut tx, 0, None),
    )
    .await
    .expect(
        "must not deadlock — non-WPS slow-store fallback Err must \
         terminate the OUTER writer within 5s; if this elapses, the \
         helper's Err arm failed to fire any termination at all",
    );

    let get_err = get_res.err().expect(
        "expected Err NotFound from slow-store fallback; got Ok \
         (StructuredNotFoundSlowStore returned Ok unexpectedly, or the \
         bypass branch wasn't hit)",
    );
    assert_eq!(
        get_err.code,
        Code::NotFound,
        "expected NotFound from get_part return value, got {:?} ({get_err})",
        get_err.code,
    );
    let get_err_dbg = format!("{get_err:?}");
    assert!(
        get_err_dbg.contains(SLOW_STORE_NOTFOUND_MARKER),
        "get_part return value lost the structured slow-store marker; \
         the Err propagation through FastSlowStore::get_part stripped it. \
         Got: {get_err_dbg}",
    );

    // Drain the rx side to see what was actually written to the wire.
    // Whatever terminated the writer (explicit fail vs Drop fallback)
    // is what `rx.recv` observes when the channel closes.
    let recv_res = rx.recv().await;
    let recv_err = recv_res.err().expect(
        "expected Err on rx — tx must have been terminated with an \
         error, not EOF (no bytes were written; an Ok would be a clean \
         EOF, which would mean the helper called commit_eof)",
    );
    let recv_dbg = format!("{recv_err:?}");

    // STRONGEST ASSERTION: the structured marker MUST reach the wire.
    // The marker is unique to StructuredNotFoundSlowStore.get_part's
    // make_err!(Code::NotFound, "{SLOW_STORE_NOTFOUND_MARKER}"). If
    // `commit_with_inner_miss_gate` falls back to the Drop fallback
    // instead of `guard.fail(err)`, the wire-side err loses this
    // marker and instead carries "buf_channel: writer dropped without
    // commit" (the synthesized fallback Internal).
    assert!(
        recv_dbg.contains(SLOW_STORE_NOTFOUND_MARKER),
        "wire-side err lost the structured slow-store marker — \
         `commit_with_inner_miss_gate` Err arm fell back to Drop \
         fallback instead of calling `guard.fail(err)`. Got rx err: \
         {recv_dbg}",
    );

    // BUG REGRESSION ASSERTION: the wire-side err MUST NOT carry the
    // Drop synthesizer's identifier. If this fires, the explicit
    // `guard.fail(err.clone())` at the helper's Err arm was bypassed.
    assert!(
        !recv_dbg.contains("buf_channel: writer dropped without commit"),
        "wire-side err carries Drop-fallback identifier — the Drop \
         fallback fired instead of explicit termination; this is the \
         #191 bug. The structured slow-store NotFound was replaced by \
         a generic Internal \"writer dropped\". Got rx err: {recv_dbg}",
    );

    // Code MUST be NotFound (not Internal). With the bug, the Drop
    // fallback synthesizes Code::Internal which would replace the
    // NotFound here. With the fix, `guard.fail(err)` preserves
    // Code::NotFound from the slow store's structured Err.
    assert_eq!(
        recv_err.code,
        Code::NotFound,
        "wire-side err code wrong — expected NotFound (the slow \
         store's structured Err code), got {:?}. The Drop fallback \
         synthesizes Code::Internal; this would mean the helper's \
         explicit termination was bypassed. Got rx err: {recv_dbg}",
        recv_err.code,
    );

    // Operator-side log assertion. With the fix, the FastSlowStore
    // outer guard is committed via `guard.fail(err.clone())` so its
    // Drop body is suppressed. There are no other guards in this test
    // (no VerifyStore wrapper, no manual WriteHalfGuard around `tx`),
    // so this `logs_contain` would only fire if FastSlowStore's outer
    // guard fell through to Drop — i.e. the bug. (`#[traced_test]` is
    // applied by `nativelink_test`; `logs_contain` is in scope.)
    assert!(
        !logs_contain("WriteHalfGuard fired Drop fallback"),
        "operator-side `error!` log `\"WriteHalfGuard fired Drop \
         fallback\"` fired — this is the production log-spam the #191 \
         fix is supposed to eliminate. The helper's Err arm must call \
         `guard.fail(err)` so the FastSlowStore outer guard's Drop \
         body never fires for this path",
    );

    Ok(())
}
