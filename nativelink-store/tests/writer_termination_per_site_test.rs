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

//! Per-site writer-termination contract tests.
//!
//! These tests target the 5 sites identified by the comprehensive
//! contract audit at `.claude/audits/writer-termination-contract/sites.md`:
//!
//!   1. `size_partitioning_store.rs:164-166` — string-key validation
//!      early-return now calls `writer.send_error(err.clone())`.
//!   2. `ref_store.rs:146-148` — `get_store()?` early-return now
//!      explicitly terminates the writer.
//!   3. `noop_store.rs:86` — NoopStore::get_part NotFound now
//!      terminates the writer (test-only store, but still part of the
//!      contract).
//!   4. `worker_proxy_store.rs:2144` — responder-mode short-circuit is
//!      SAFE-by-delegation; this test asserts no deadlock through that
//!      branch when the inner returns NotFound.
//!   5. `verify_store.rs:295-376` — VerifyStore tx_guard no longer
//!      relies on Drop fallback for inner-store Errs (the #186
//!      OOM-trajectory log spam source); it now calls
//!      `tx_guard.fail(err.clone())` explicitly. This is asserted by
//!      checking that the Drop-fallback wire-side identifier
//!      (`buf_channel: writer dropped without commit`) does NOT appear
//!      in the merged err on a legitimate inner-store NotFound.
//!
//! Every test wraps its target in production composition (`VerifyStore`
//! where applicable) and uses `tokio::time::timeout(5s)` as the
//! deadlock detector. Specific assertion messages name the bug class,
//! per CLAUDE.md "Test in production composition, not in isolation."

use core::time::Duration;
use std::sync::Arc;

use nativelink_config::stores::{MemorySpec, RefSpec, SizePartitioningSpec, StoreSpec, VerifySpec};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::noop_store::NoopStore;
use nativelink_store::ref_store::RefStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{IS_WORKER_REQUEST, Store, StoreKey, StoreLike};

const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

const MISSING_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Wire-side identifier the `WriteHalfGuard::Drop` fallback synthesizes
/// when a function returns without explicitly committing the writer. Any
/// test asserting that the explicit termination path was taken must
/// confirm this string is ABSENT from the merged err.
const DROP_FALLBACK_IDENTIFIER: &str = "buf_channel: writer dropped without commit";

// -----------------------------------------------------------------------
// Fix 1: SizePartitioningStore string-key validation early-return.
// -----------------------------------------------------------------------

/// `SizePartitioningStore::get_part` with a `StoreKey::Str` key returns
/// `make_input_err!` IMMEDIATELY (no inner delegation). Before the fix
/// the writer was never terminated; any wrapper joining a paired reader
/// on the writer's other half deadlocked forever. After the fix the
/// writer receives `send_error(err.clone())` and the paired reader
/// observes the structured InvalidArgument.
///
/// We bypass `VerifyStore` here because `VerifyStore` requires
/// `StoreKey::Digest` and would reject the `StoreKey::Str` before
/// `SizePartitioningStore::get_part` is invoked. Instead we use a
/// direct `make_buf_channel_pair` + `tokio::join!` of `get_part` and
/// `rx.recv()`, which models the same paired-reader deadlock pattern
/// but does not require a digest-key wrapper.
///
/// Mutation evidence: comment out the new
/// `writer.send_error(err.clone())` line in `size_partitioning_store.rs`
/// and rerun. The `rx.recv()` future will hang past `NO_DEADLOCK_TIMEOUT`
/// and the `tokio::time::timeout` outer wrapper triggers the panic with
/// the `WRITER_TERMINATION_VIOLATED_*` message below.
#[nativelink_test]
async fn size_partitioning_str_key_terminates_writer_with_send_error() -> Result<(), Error> {
    let lower = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper = Store::new(MemoryStore::new(&MemorySpec::default()));
    let part = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: 1024,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        lower,
        upper,
    );

    let (mut tx, mut rx) = make_buf_channel_pair();
    let str_key = StoreKey::Str(std::borrow::Cow::Borrowed("not-a-digest-key"));

    let part_ptr = part.clone();
    let get_fut = async move {
        let pin_ref = core::pin::Pin::new(part_ptr.as_ref());
        nativelink_util::store_trait::StoreDriver::get_part(pin_ref, str_key, &mut tx, 0, None)
            .await
    };
    let recv_fut = async move {
        // Drain until error or eof. We expect the structured InvalidArgument
        // to flow through `terminal_error`, surfaced on the next recv after
        // the channel closes.
        loop {
            match rx.recv().await {
                Ok(chunk) if chunk.is_empty() => break Ok(()),
                Ok(_) => continue,
                Err(err) => break Err(err),
            }
        }
    };

    let joined = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, async {
        tokio::join!(get_fut, recv_fut)
    })
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_size_partitioning_str_key: \
         SizePartitioningStore::get_part(Str_key) did not terminate the writer; \
         paired rx.recv() deadlocked past 5s. The early return must call \
         writer.send_error(err.clone()) before `return Err(err)`.",
    );

    let (get_res, recv_res) = joined;
    assert!(get_res.is_err(), "get_part should return Err for Str key");
    let get_err = get_res.unwrap_err();
    assert_eq!(
        get_err.code,
        Code::InvalidArgument,
        "expected InvalidArgument code, got {get_err:?}",
    );
    assert!(
        recv_res.is_err(),
        "rx side must observe an error (not eof); got {recv_res:?}",
    );
    let recv_err = recv_res.unwrap_err();
    assert!(
        recv_err
            .messages
            .iter()
            .any(|m| m.contains("SizePartitioningStore only supports Digest keys")),
        "rx side err must contain the structured InvalidArgument message; got {recv_err:?}",
    );
    assert!(
        !recv_err.messages.iter().any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "rx side err MUST NOT carry the Drop-fallback identifier (the explicit \
         send_error path was bypassed if it does); got {recv_err:?}",
    );
    Ok(())
}

// -----------------------------------------------------------------------
// Fix 2: RefStore get_store() early-return.
// -----------------------------------------------------------------------

/// `RefStore::get_part` calls `self.get_store()?`; if the named target
/// is missing from the `StoreManager`, `?` propagates the Err WITHOUT
/// terminating the writer. Before the fix any wrapper joining a paired
/// reader deadlocked. After the fix the explicit
/// `match get_store() { Err(e) => { writer.send_error(e.clone()); ... } }`
/// puts the structured InvalidArgument on the wire.
///
/// Mutation evidence: revert the `match` block back to `?` and rerun;
/// `rx.recv()` hangs past 5s, panic message
/// `WRITER_TERMINATION_VIOLATED_ref_store_missing_target` fires.
#[nativelink_test]
async fn ref_store_missing_target_terminates_writer_with_send_error() -> Result<(), Error> {
    let store_manager = Arc::new(StoreManager::new());
    // Intentionally do NOT add a store named "missing-target".
    let ref_store = RefStore::new(
        &RefSpec {
            name: "missing-target".to_string(),
        },
        Arc::downgrade(&store_manager),
    );

    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    let key = StoreKey::Digest(digest);

    let (mut tx, mut rx) = make_buf_channel_pair();

    let ref_ptr = ref_store.clone();
    let get_fut = async move {
        let pin_ref = core::pin::Pin::new(ref_ptr.as_ref());
        nativelink_util::store_trait::StoreDriver::get_part(pin_ref, key, &mut tx, 0, None).await
    };
    let recv_fut = async move {
        loop {
            match rx.recv().await {
                Ok(chunk) if chunk.is_empty() => break Ok(()),
                Ok(_) => continue,
                Err(err) => break Err(err),
            }
        }
    };

    let joined = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, async {
        tokio::join!(get_fut, recv_fut)
    })
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_ref_store_missing_target: \
         RefStore::get_part with missing target did not terminate the writer; \
         paired rx.recv() deadlocked past 5s. The `?` propagation in \
         `self.get_store()?` must be replaced with explicit \
         `writer.send_error(err.clone())` before the early return.",
    );

    let (get_res, recv_res) = joined;
    assert!(
        get_res.is_err(),
        "get_part should return Err for missing target store"
    );
    let get_err = get_res.unwrap_err();
    assert_eq!(
        get_err.code,
        Code::InvalidArgument,
        "expected InvalidArgument code, got {get_err:?}",
    );
    assert!(
        recv_res.is_err(),
        "rx side must observe an error (not eof); got {recv_res:?}",
    );
    let recv_err = recv_res.unwrap_err();
    assert!(
        recv_err
            .messages
            .iter()
            .any(|m| m.contains("Failed to find store 'missing-target'")),
        "rx side err must contain the structured RefStore message; got {recv_err:?}",
    );
    assert!(
        !recv_err.messages.iter().any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "rx side err MUST NOT carry the Drop-fallback identifier; got {recv_err:?}",
    );
    Ok(())
}

// -----------------------------------------------------------------------
// Fix 3: NoopStore::get_part — INTENTIONALLY does NOT terminate writer.
// -----------------------------------------------------------------------

/// `NoopStore::get_part` returns `Err(Code::NotFound, ...)` WITHOUT
/// calling `writer.send_error()` — this is the documented contract
/// EXCEPTION (see `noop_store.rs::get_part` rustdoc).
///
/// Why the contract exception: `FastSlowStore::get_part` calls
/// `fast_store.get_part(&mut *guard, ...)` BEFORE checking the
/// `optimized_for(NoopUpdates)` bypass flag (the per-call attempt
/// avoids the extra has() round-trip; see
/// `fast_slow_store.rs:3081-3151`). When the fast store IS NoopStore,
/// that call returns `Err(NotFound)` and FastSlowStore falls through to
/// the slow store. If NoopStore had set `terminal_error` to its
/// "Not found in noop store" string FIRST, the buf_channel's `OnceLock`
/// would freeze that misleading message — the later
/// `commit_with_inner_miss_gate` → `guard.fail(slow_store_err)` would
/// be a silent no-op and the downstream reader would observe
/// "Not found in noop store" instead of the slow store's structured
/// error (the `waiter_explicit_termination_test` regression).
///
/// FastSlowStore owns the writer and is responsible for terminating it
/// after fall-through, so NoopStore's contract violation is SAFE-by-
/// composition. NoopStore is also test-only and never user-visible in
/// production; the Bazel server / worker chains never include it.
///
/// This test asserts the EXCEPTION holds: NoopStore::get_part returns
/// Err with the NotFound code AND does NOT terminate the writer (the
/// `OnceLock` `terminal_error` should remain unset; the receiver sees
/// the generic "Sender dropped before sending EOF" Internal once `tx`
/// drops). If we ever change NoopStore to terminate, this test will
/// fail loudly and force the author to also fix
/// `fast_slow_store.rs::get_part`'s fast-store-call site to clear/skip
/// terminal_error before falling through.
#[nativelink_test]
async fn noop_store_get_part_does_not_terminate_writer_by_design() -> Result<(), Error> {
    let noop = NoopStore::new();
    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    let key = StoreKey::Digest(digest);

    let (mut tx, mut rx) = make_buf_channel_pair();

    let noop_ptr = noop.clone();
    let get_fut = async move {
        let pin_ref = core::pin::Pin::new(noop_ptr.as_ref());
        let res = nativelink_util::store_trait::StoreDriver::get_part(
            pin_ref, key, &mut tx, 0, None,
        )
        .await;
        // Drop tx explicitly to mirror what FastSlowStore would do after
        // falling through (writer ownership returns; in our test the
        // tx is dropped as the future returns).
        drop(tx);
        res
    };
    let recv_fut = async move {
        loop {
            match rx.recv().await {
                Ok(chunk) if chunk.is_empty() => break Ok(()),
                Ok(_) => continue,
                Err(err) => break Err(err),
            }
        }
    };

    let joined = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, async {
        tokio::join!(get_fut, recv_fut)
    })
    .await
    .expect(
        "NoopStore must not deadlock — even with the contract exception, \
         dropping tx unblocks the receiver via Sender-dropped Internal. \
         If this elapses, something more fundamental broke.",
    );

    let (get_res, recv_res) = joined;
    assert!(get_res.is_err(), "get_part should return Err");
    let get_err = get_res.unwrap_err();
    assert_eq!(
        get_err.code,
        Code::NotFound,
        "expected NotFound code from get_part return, got {get_err:?}",
    );
    let recv_err = recv_res.expect_err(
        "rx must NOT see EOF — NoopStore returned Err; if we see EOF here, \
         someone called send_eof which is wrong",
    );
    // CONTRACT EXCEPTION ASSERTION: the receiver does NOT see the
    // structured "Not found in noop store" message because NoopStore did
    // NOT call `send_error` (by design — see noop_store.rs rustdoc).
    // Instead it sees the generic "Sender dropped before sending EOF"
    // Internal that the channel synthesizes when tx is dropped without
    // termination. If NoopStore is ever changed to terminate the writer,
    // this assertion will flip and the author will be forced to also fix
    // the fast_slow_store fast-tier-call site.
    assert!(
        !recv_err
            .messages
            .iter()
            .any(|m| m.contains("Not found in noop store")),
        "NOOP_STORE_CONTRACT_EXCEPTION_VIOLATED: rx side observed the \
         structured NoopStore NotFound message — meaning NoopStore.get_part \
         called writer.send_error before returning Err. This BREAKS \
         FastSlowStore composition (the OnceLock-based terminal_error \
         freezes this message and the slow-store fallback's structured Err \
         can never reach the wire). Either revert the NoopStore change OR \
         update fast_slow_store.rs::get_part to clear/bypass terminal_error \
         before the fast-store-call fall-through. Got: {recv_err:?}",
    );
    Ok(())
}

// -----------------------------------------------------------------------
// Fix 4: WorkerProxyStore responder-mode short-circuit.
// -----------------------------------------------------------------------

/// SAFE-by-delegation. `worker_proxy_store.rs:2144` directly delegates
/// to `self.inner.get_part(...)` from the responder-mode short-circuit;
/// `Store::get_part` is contractually required to terminate the writer.
/// This test wraps the WorkerProxyStore in `VerifyStore` and confirms
/// that under `IS_WORKER_REQUEST=true` + `race_peers=true` (the responder
/// mode entry conditions), an inner NotFound surfaces within the
/// deadlock-detector window.
///
/// We do not need to make any code change to enforce safety here — this
/// test exists to PROVE the assumption that delegation through
/// responder-mode is safe so long as the inner satisfies its own
/// contract (which the per-fix tests above + composability_test
/// already enforce on every leaf).
#[nativelink_test]
async fn worker_proxy_responder_mode_does_not_deadlock_on_inner_err() -> Result<(), Error> {
    let inner_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner_mem, locality_map);
    proxy.enable_race_peers();
    let proxy_store = Store::new(proxy);

    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        proxy_store,
    );

    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    let fut = IS_WORKER_REQUEST.scope(
        true,
        verify_store.get_part_unchunked(digest, 0, None),
    );
    let res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, fut).await;

    match res {
        Err(_elapsed) => panic!(
            "WRITER_TERMINATION_VIOLATED_worker_proxy_responder_mode: \
             VerifyStore-wrapped WorkerProxyStore (race_peers=true, \
             IS_WORKER_REQUEST=true) did not return within 5s. The \
             responder-mode short-circuit at worker_proxy_store.rs:2144 \
             delegates to `self.inner.get_part(...)`; the inner store \
             violated its own writer-termination contract. Either the \
             inner store needs a per-site fix, or this short-circuit \
             needs a defensive WriteHalfGuard wrapper."
        ),
        Ok(Ok(bytes)) => panic!(
            "expected NotFound but got Ok({} bytes); the responder-mode \
             test setup must trigger an inner Err to exercise the contract",
            bytes.len()
        ),
        Ok(Err(err)) => {
            assert_eq!(
                err.code,
                Code::NotFound,
                "expected NotFound from empty inner MemoryStore, got {err:?}",
            );
        }
    }
    Ok(())
}

// -----------------------------------------------------------------------
// Fix 5: VerifyStore tx_guard Option B (#186 — high-frequency log spam).
// -----------------------------------------------------------------------

/// VerifyStore's `tx_guard` previously used `commit_delegated_if_ok(&res)`
/// which left the Drop fallback armed on every Err return. A
/// well-behaved inner store (e.g. MemoryStore) returning a legitimate
/// `Err(NotFound)` would terminate `tx` correctly via its own
/// `send_error`, so the receiver would observe the structured NotFound
/// — but the Drop fallback ALSO fired the loud
/// `"WriteHalfGuard fired Drop fallback"` `error!` log because the
/// guard didn't know the inner had committed. This produced ~600+/min
/// log spam in production, contributing to the OOM trajectory at
/// deploy +15-25 min.
///
/// Option B (this fix) makes the commit explicit:
///   - Ok: `commit_delegated_if_ok(&res)` (suppresses Drop)
///   - Err: `tx_guard.fail(err.clone())` (suppresses Drop AND sends
///     the structured err if the inner hadn't already)
///
/// `send_error` is idempotent (only the first error is recorded), so a
/// well-behaved inner that already terminated `tx` is unaffected by the
/// extra `tx_guard.fail`; the inner's err wins.
///
/// This test asserts: VerifyStore-wrapped MemoryStore returning NotFound
/// produces a wire-side err that does NOT carry the Drop-fallback
/// identifier. With the OLD design (`commit_delegated_if_ok` only),
/// even though MemoryStore terminates correctly, the Drop fallback
/// would fire (committed=false on Err) and produce the noisy `error!`
/// log, but the wire-side err itself would NOT carry the Drop
/// identifier because MemoryStore set the terminal_error first. So
/// asserting the Drop identifier is absent is necessary but not
/// sufficient.
///
/// The PRIMARY assertion is: with a synthetic inner that does NOT
/// terminate (i.e. simulates the contract violation Drop was meant to
/// catch), the Drop fallback identifier WAS the wire-side identifier
/// before #191/Option B. After Option B the structured `tx_guard.fail`
/// puts the inner's err on the wire FIRST, so the Drop identifier never
/// appears.
///
/// Mutation evidence: revert the `match &res { ... Err(err) => fail ... }`
/// block back to `tx_guard.commit_delegated_if_ok(&res);` in
/// `verify_store.rs`. With our test setup
/// (`UnterminatingStore` returns Err without calling `send_error`), the
/// wire-side err WILL contain the Drop fallback identifier
/// (`buf_channel: writer dropped without commit`) because the Drop
/// fallback synthesized it. The assertion below fires with the
/// `WRITER_TERMINATION_REGRESSION_verify_store_option_b` panic.
#[nativelink_test]
async fn verify_store_option_b_does_not_fall_back_to_drop_on_inner_err() -> Result<(), Error> {
    use unterminating_store::UnterminatingStore;

    let inner = Store::new(UnterminatingStore::new(Code::NotFound, "synthetic miss"));
    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        inner,
    );

    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    let fut = verify_store.get_part_unchunked(digest, 0, None);
    let res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, fut).await;

    let err = match res {
        Err(_elapsed) => panic!(
            "WRITER_TERMINATION_VIOLATED_verify_store_around_unterminating_inner: \
             VerifyStore-wrapped UnterminatingStore did not return within 5s; \
             the tx_guard's Drop-or-explicit-fail safety net is broken. Both \
             code paths (Drop or `tx_guard.fail`) MUST unblock the check_fut \
             reader."
        ),
        Ok(Ok(bytes)) => panic!(
            "expected Err but got Ok({} bytes); the test setup is wrong — \
             UnterminatingStore must return Err to exercise this branch",
            bytes.len()
        ),
        Ok(Err(err)) => err,
    };

    // PRIMARY ASSERTION (#186 fix): with Option B, the explicit
    // `tx_guard.fail(err.clone())` puts the inner's structured err on the
    // wire FIRST so the Drop-fallback identifier never appears.
    assert!(
        !err.messages.iter().any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "WRITER_TERMINATION_REGRESSION_verify_store_option_b: \
         VerifyStore wire-side err MUST NOT carry the Drop-fallback identifier. \
         If it does, `tx_guard.commit_delegated_if_ok(&res)` was used on the \
         Err arm instead of `tx_guard.fail(err.clone())` — the noisy `error!` \
         log will fire on every legitimate inner-store NotFound, contributing \
         to the OOM trajectory observed at the 4-bundle deploy. \
         Got: {err:?}",
    );
    // SECONDARY ASSERTION: the structured inner err is preserved on the wire.
    assert_eq!(
        err.code,
        Code::NotFound,
        "expected NotFound from synthetic inner, got {err:?}",
    );
    assert!(
        err.messages.iter().any(|m| m.contains("synthetic miss")),
        "wire-side err must contain the inner's structured message; got {err:?}",
    );
    Ok(())
}

/// Synthetic store that returns `Err(code, msg)` from `get_part`
/// WITHOUT calling `writer.send_error()` — i.e. simulates a
/// contract-violating inner. This is the test fixture for Fix 5:
/// without it, every well-behaved leaf store correctly terminates
/// `tx`, the Drop fallback never fires, and the assertion would be
/// a tautology.
mod unterminating_store {
    use core::pin::Pin;
    use std::sync::Arc;

    use async_trait::async_trait;
    use nativelink_error::{Code, Error, make_err};
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    };
    use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
    use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
    use nativelink_util::store_trait::{
        ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, StoreDriver,
        StoreKey, UploadSizeInfo,
    };

    #[derive(Debug)]
    pub(crate) struct UnterminatingStore {
        err_code: Code,
        err_msg: String,
    }

    impl MetricsComponent for UnterminatingStore {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    impl UnterminatingStore {
        pub(crate) fn new(err_code: Code, err_msg: &str) -> Arc<Self> {
            Arc::new(Self {
                err_code,
                err_msg: err_msg.to_string(),
            })
        }
    }

    #[async_trait]
    impl StoreDriver for UnterminatingStore {
        async fn has_with_results(
            self: Pin<&Self>,
            _keys: &[StoreKey<'_>],
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
            _size_info: UploadSizeInfo,
        ) -> Result<(), Error> {
            Ok(())
        }

        /// CONTRACT VIOLATION (intentional): returns Err WITHOUT calling
        /// `writer.send_error()`. This is what `WriteHalfGuard::Drop` was
        /// designed to catch — and what Option B replaces with explicit
        /// `tx_guard.fail` so the Drop's noisy `error!` never fires.
        async fn get_part(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            _writer: &mut DropCloserWriteHalf,
            _offset: u64,
            _length: Option<u64>,
        ) -> Result<(), Error> {
            Err(make_err!(self.err_code, "{}", self.err_msg))
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

    default_health_status_indicator!(UnterminatingStore);
}
