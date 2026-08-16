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

//! `#fl1786-read-half`: the READ half of the digest-function latch.
//!
//! **The invariant.** A blob admitted to the CAS is retrievable from the CAS
//! under the identity it was admitted under. `c6592be4` made the WRITE side
//! admit a blob that reproduces its declared digest under ANY function the
//! server advertises. The read side still resolved ONE function from
//! `Context::current()` (falling back to the process default) and did no
//! proving — so the same blob, read without ambient context, hashed under
//! blake3, mismatched, and returned `Code::DataLoss`. Admission and retrieval
//! decided validity by different rules.
//!
//! **This file exists because no test in this repo had ever written a proven
//! blob and read it back.** That absence survived three review rounds
//! (`.claude/reviews/4b0d92a3/`, `.claude/reviews/3847750f/`) and was named by
//! both rounds' red-team as the one property that would falsify the whole
//! design. Every other test in the FL-1786 chain is write-side or render-side.
//!
//! **Reachable with no external client — but NOT via the site the dispatch
//! named.** `worker_proxy_store.rs:5223` is a raw `tokio::spawn` calling
//! `inner.get_part(..)`, but the whole race block is gated on `race_peers`
//! (`:5168`), which is `AtomicBool::new(false)` in both constructors
//! (`:1308`, `:1359`) and whose only production enabler is
//! `nativelink-worker/src/local_worker.rs:7172`. It is **worker-side only and
//! dead on the server**, and on the worker `inner` is a gRPC store, not a
//! local `VerifyStore`.
//!
//! The live server sites are in a crate nobody had looked in —
//! `nativelink-scheduler/src/api_worker_scheduler.rs`, whose prefetch
//! (`tokio::spawn` at `:10024` → `cas.get_part_unchunked(key, 0, None)` at
//! `:10124`), cache-warm (`:10269` + `JoinSet::spawn` `:10281` → `:10284`)
//! and post-timeout tree resolution (`:9598` → `:11304`) all cross a BARE
//! `tokio::spawn`, which does not inherit `Context::current()` (unlike
//! `nativelink_util::task::spawn!`, which re-attaches it at
//! `task.rs:104-111`). The scheduler holds the same `cas_STORE` handle —
//! `src/bin/nativelink.rs:338-349` comments that `scheduler_factory` (`:527`)
//! deliberately runs AFTER the `WorkerProxyStore` wrap at `:430` — and
//! `buildcache-native.json5:289` sets the scheduler's `cas_store` to
//! `cas_STORE`. All three read `(0, None)`, so all three hit
//! `should_verify`.
//!
//! `internal_no_context_read_*` below reproduces that shape. A
//! REAPI-compliant SHA-256 client also reads with no segment by spec
//! (`remote_execution.proto:235-240` MUST-omit list), so the internal and the
//! external population land on the same path.
//!
//! **What the DataLoss actually does downstream** (the dispatch had this
//! backwards, and the truth is worse): `VerifyStore` sits ABOVE
//! `ExistenceCacheStore` in the deployed chain (`cas_STORE` verify →
//! `cas_INNER` existence_cache, `buildcache-native.json5:183-190` / `:220-236`),
//! so the eviction arm at `existence_cache_store.rs:893` is UNREACHABLE for
//! this error — it fires only on a `DataLoss` minted BELOW the cache. What
//! actually happens is `:884-892`: the cache's own inner read returned `Ok`,
//! so it RE-INSERTS the digest and refreshes its LRU recency. `has()` keeps
//! answering present, `get_part` keeps answering `DataLoss`, and nothing in
//! the chain ever evicts. The read-side latch is self-reinforcing rather than
//! self-clearing.
//!
//! **The mechanism under test (mismatch-gated, not always-prove).** The happy
//! path is unchanged and pays nothing: one hasher, one read. Only the path
//! that TODAY returns `DataLoss` re-reads the inner store and proves against
//! the advertised set. `happy_path_read_does_not_re_read_the_inner_store`
//! pins the zero-cost half; without it the "zero added cost" claim is
//! unverified.

use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::{
    DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD, VerifyStore, digest_func_proven_log_decision,
};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasherFunc, default_digest_hasher_func, make_ctx_for_hash_func,
    set_default_digest_hasher_func,
};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use opentelemetry::context::{Context, FutureExt};
use pretty_assertions::assert_eq;
use tracing::{Instrument, info_span};

const VALUE: &str = "123";
/// `sha256("123")` — the declared digest of the mislabelled-but-intact blob.
/// This is the live FL-1786 shape: a `--digest_function=sha256` client's
/// `Command`/`Directory` proto.
const SHA256_OF_VALUE: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
/// `blake3("123")` — what a blake3-resolving reader computes from the same
/// bytes, i.e. the value that lands in the mismatch message today.
const BLAKE3_OF_VALUE: &str = "b3d4f8803f7e24b8f389b072e75477cdbcfbe074080fb5e500e53e26e054158e";
/// `sha256("12")` — the digest of DIFFERENT bytes. No advertised function
/// reproduces it from `VALUE`, so this is the genuinely-corrupt fixture and
/// the fail-closed control.
const SHA256_OF_OTHER: &str = "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918";
/// Same length as `VALUE`, different bytes — used to make the two proving
/// passes disagree.
const OTHER_VALUE: &str = "999";
/// `sha256("999")`.
const SHA256_OF_OTHER_VALUE: &str =
    "83cf8b609de60036a8277bd0e96135751bbc07eb234256d4b65b893360651bf2";

/// Pin the process-global digest function to BLAKE3 — the PRODUCTION value
/// (`buildcache-native.json5:748`). The assert is the load-bearing half: the
/// global is a `OnceCell`, and a silent already-set would run every
/// no-context case below against SHA-256, where the bug does not reproduce
/// and every test here would pass vacuously.
fn pin_production_default_blake3() {
    let _ = set_default_digest_hasher_func(DigestHasherFunc::Blake3);
    assert_eq!(
        default_digest_hasher_func(),
        DigestHasherFunc::Blake3,
        "#fl1786-read-half: this test binary must run with the PRODUCTION process-global digest \
         function (BLAKE3, buildcache-native.json5:748). Reading SHA-256 means something set the \
         OnceCell first, the no-context read below would resolve SHA-256, hash correctly, and \
         every round-trip assertion in this file would pass WITHOUT the proving path ever running"
    );
}

/// Production `cas_STORE` shape: `VerifyStore { verify_size: true,
/// verify_hash: true }` (`buildcache-native.json5:190-191`) over an inner store.
fn verify_store_over(inner: Store) -> Arc<VerifyStore> {
    VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        inner,
    )
}

/// The three reachable `ReadProof::reason()` strings, as they land on the
/// failure `error!`'s `proving` field.
///
/// These are OPERATOR-TRIAGE strings and they are the stated reason
/// `ReadProof` is a type rather than an `Option`: a `DataLoss` is one
/// indistinguishable line unless it says whether the blob is corrupt,
/// unreadable, or unstable — three faults with three different responses
/// (re-upload the blob / look at the store below / declare a storage-integrity
/// incident). Nothing pinned them, so swapping `Unreadable`'s for
/// `Inconsistent`'s reddened no test while pointing an operator at the wrong
/// investigation (`.claude/reviews/fc317cca/pair-b.md` T3).
///
/// The fourth variant's string, `Proven(_) => "proven"`, is deliberately NOT
/// pinned: the `Proven` arm returns before the `error!` is reached, so that
/// string is unreachable in a correct build. pair-b observed it printed only
/// under a mutation that removed the rescue (C3).
const REASON_UNPROVABLE: &str = "no advertised digest function reproduces the declared digest";
const REASON_UNREADABLE: &str = "the proving re-read of the inner store failed";
const REASON_INCONSISTENT: &str =
    "the proving re-read returned different bytes than the pass that was served";

/// Assert the failure `error!` names EXACTLY `expected` in its `proving` field.
///
/// Checks the other two strings are absent as well, so this fails on a SWAP and
/// not merely on a deletion — a swap is the realistic regression (the strings
/// are adjacent match arms) and is the one that misdirects triage rather than
/// silencing it.
fn assert_proving_reason(lines: &[&str], expected: &str) -> Result<(), String> {
    let failures: Vec<&str> = lines
        .iter()
        .filter(|l| l.contains("hash mismatch on read in verify store"))
        .copied()
        .collect();
    if failures.len() != 1 {
        return Err(format!(
            "#fl1786-read-half: expected exactly ONE `hash mismatch on read in verify store` \
             error line, saw {}. All captured lines:\n{}",
            failures.len(),
            lines.join("\n")
        ));
    }
    let line = failures[0];
    let wrong: Vec<&str> = [REASON_UNPROVABLE, REASON_UNREADABLE, REASON_INCONSISTENT]
        .into_iter()
        .filter(|r| *r != expected && line.contains(r))
        .collect();
    if !line.contains(expected) || !wrong.is_empty() {
        return Err(format!(
            "#fl1786-read-half: the failure line's `proving` field must carry \
             `ReadProof::reason()`'s string for THIS path, verbatim.\n  expected: {expected}\n  \
             other reasons wrongly present: {wrong:?}\n  line: {line}\nThese strings are what an \
             operator triages a DataLoss by — corrupt blob vs unreadable inner store vs a store \
             serving different bytes for one key are three different incidents. A swapped or \
             widened string sends the investigation to the wrong layer and no other assertion \
             notices"
        ));
    }
    Ok(())
}

/// Inner store that COUNTS `get_part` calls and can be made to vanish the
/// blob after the first one.
///
/// Two properties are being observed that a plain `MemoryStore` cannot show:
/// (a) the happy path must issue exactly ONE inner read — the whole cost
/// argument for mismatch-gating rests on it; (b) a blob evicted between the
/// two reads must fail CLOSED, and eviction is the one way the trigger
/// ("hash mismatch") and the precondition ("inner store still holds the
/// bytes") can come apart.
#[derive(MetricsComponent)]
struct CountingStore {
    inner: Arc<MemoryStore>,
    get_part_calls: AtomicUsize,
    /// When set, every `get_part` after the first returns `NotFound`.
    vanish_after_first_read: AtomicBool,
    /// When set, every `get_part` after the first serves THESE bytes instead
    /// of the stored ones — a store that is non-deterministic for one key.
    serve_after_first_read: parking_lot::Mutex<Option<Bytes>>,
}

impl CountingStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStore::new(&MemorySpec::default()),
            get_part_calls: AtomicUsize::new(0),
            vanish_after_first_read: AtomicBool::new(false),
            serve_after_first_read: parking_lot::Mutex::new(None),
        })
    }

    fn calls(&self) -> usize {
        self.get_part_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StoreDriver for CountingStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .has_with_results(digests, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        Pin::new(self.inner.as_ref())
            .update(key, reader, size_info)
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let nth = self.get_part_calls.fetch_add(1, Ordering::SeqCst);
        if nth > 0 && self.vanish_after_first_read.load(Ordering::SeqCst) {
            let err = make_err!(
                Code::NotFound,
                "CountingStore: blob evicted between the two reads"
            );
            writer.send_error(err.clone());
            return Err(err);
        }
        if nth > 0
            && let Some(bytes) = self.serve_after_first_read.lock().clone()
        {
            writer.send(bytes).await?;
            writer.send_eof()?;
            return Ok(());
        }
        Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
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

    // Single-inner wrapper over a MemoryStore: forward every delegation
    // unchanged, exactly as `VerifyStore` does, so this fake does not
    // accidentally change the chain's stable/pin/durable semantics.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_ref())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_ref())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_ref())
    }

    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner.as_ref())
    }
}

default_health_status_indicator!(CountingStore);

// ---------------------------------------------------------------------------
// THE ROUND TRIP — the point of the change.
// ---------------------------------------------------------------------------

/// Write a blob whose declared digest is SHA-256 while the process default is
/// BLAKE3, then read it back with NO ambient context. Both halves go through
/// the real `VerifyStore`, so this is a genuine admission→retrieval round
/// trip and not two independent assertions about one layer.
#[nativelink_test]
async fn sha256_keyed_blob_written_then_read_with_no_context_round_trips() -> Result<(), Error> {
    pin_production_default_blake3();
    let store = verify_store_over(Store::new(MemoryStore::new(&MemorySpec::default())));
    let digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;

    // ADMISSION. The label is blake3 (what a context-less producer stamps);
    // the declared digest is sha256. The write side proves and accepts.
    store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("mislabelled_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect(
            "#fl1786-read-half: PRECONDITION FAILED — the write side must admit this blob (that \
             is what c6592be4 does). If this expect fires the write half regressed and the read \
             assertion below would be testing nothing",
        );

    // RETRIEVAL, with no ambient context at all — the `worker_proxy_store.rs:5223`
    // shape and the REAPI-compliant-client shape.
    let got = store.get_part_unchunked(digest, 0, None).await;

    let bytes = got.expect(
        "#fl1786-read-half: INVARIANT VIOLATED — a blob the CAS ADMITTED cannot be RETRIEVED. \
         The write side accepted this blob by proving its digest function from its own bytes; \
         the read side resolved blake3 from an empty Context, hashed, mismatched, and returned \
         DataLoss. Admission and retrieval are deciding validity by different rules. This is the \
         read half of the FL-1786 latch",
    );
    assert_eq!(
        bytes,
        VALUE.as_bytes(),
        "#fl1786-read-half: the read returned Ok but with the WRONG BYTES. Proving must rescue \
         the LABEL, never substitute the payload — the bytes streamed to the caller are the ones \
         the inner store served on the first pass and they must be returned verbatim"
    );
    Ok(())
}

/// The reachability proof, modelled on the three LIVE server sites in
/// `nativelink-scheduler/src/api_worker_scheduler.rs` (prefetch `:10024`
/// → `:10124`, cache-warm `:10269`/`:10281` → `:10284`, tree resolution
/// `:9598` → `:11304`): each crosses a bare `tokio::spawn` and then reads
/// `cas_STORE` at `(0, None)`. A bare spawn does NOT carry
/// `Context::current()`. So even when the enclosing scope DOES carry a
/// sha256 context, the read that actually reaches `cas_STORE` resolves the
/// process default.
///
/// The control assertion inside the spawn is the load-bearing half: it proves
/// the context is genuinely absent there, so a green result cannot come from
/// the context having survived.
#[nativelink_test]
async fn internal_no_context_read_through_a_bare_tokio_spawn_round_trips() -> Result<(), Error> {
    pin_production_default_blake3();
    let store = verify_store_over(Store::new(MemoryStore::new(&MemorySpec::default())));
    let digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;

    store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("mislabelled_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect("#fl1786-read-half: PRECONDITION FAILED — the write side must admit this blob");

    // The enclosing scope carries the CORRECT function. A read here would
    // resolve sha256 and never reach the proving path.
    let outer_ctx = make_ctx_for_hash_func(DigestHasherFunc::Sha256)?;
    let spawned = async move {
        let store = store.clone();
        tokio::spawn(async move {
            // CONTROL: prove the mechanism is engaged for the right reason.
            assert!(
                Context::current().get::<DigestHasherFunc>().is_none(),
                "#fl1786-read-half: this test's premise is that a bare `tokio::spawn` LOSES the \
                 ambient DigestHasherFunc (api_worker_scheduler.rs:10024/10269/9598). The \
                 context survived, so the read below would resolve sha256, hash correctly, and \
                 pass WITHOUT the proving path running. Fix the test, not the code"
            );
            store.get_part_unchunked(digest, 0, None).await
        })
        .await
        .expect("#fl1786-read-half: the spawned read task panicked")
    }
    .with_context(outer_ctx)
    .await;

    let bytes = spawned.expect(
        "#fl1786-read-half: INVARIANT VIOLATED on the INTERNAL path — a server-internal reader \
         that crosses a bare `tokio::spawn` (the scheduler's prefetch / cache-warm / \
         tree-resolution tasks) cannot retrieve a blob the CAS admitted. This needs no external \
         client and no resource-name argument: the DataLoss is manufactured entirely inside the \
         server",
    );
    assert_eq!(
        bytes,
        VALUE.as_bytes(),
        "#fl1786-read-half: the internal no-context read returned the wrong bytes"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// FAIL-CLOSED — proving must not widen into an accept-anything.
// ---------------------------------------------------------------------------

/// A genuinely corrupt blob must still be rejected, with the `Code` and the
/// message BYTE-IDENTICAL to the pre-proving ones. FOUR pre-existing
/// assertions — `verify_store_test.rs:429/566/818/826` — match on the
/// `"Hash mismatch on read"` string; widening the message would break operator
/// greps and dashboards as surely as widening the verdict would break
/// integrity.
///
/// (This doc-comment said "twenty pre-existing cases in two files" until
/// 2026-08-16. That was `verify_store_test.rs`'s total TEST COUNT, and the two
/// `bytestream_read_digest_context_test.rs` hits are doc-comments rather than
/// assertions — pair-a T-6, recomputed by `grep -rn "Hash mismatch on read"`.)
#[nativelink_test]
async fn corrupt_blob_on_read_is_still_dataloss_with_the_unchanged_message() -> Result<(), Error> {
    pin_production_default_blake3();
    let inner = MemoryStore::new(&MemorySpec::default());
    let store = verify_store_over(Store::new(inner.clone()));

    // Plant bytes under a digest NO advertised function reproduces, writing
    // straight to the inner store so the write-side gate is bypassed.
    let digest = DigestInfo::try_new(SHA256_OF_OTHER, VALUE.len() as u64)?;
    inner.update_oneshot(digest, VALUE.into()).await?;

    let err = store.get_part_unchunked(digest, 0, None).await.expect_err(
        "#fl1786-read-half: FAIL-OPEN — a blob whose bytes reproduce NO advertised digest \
             function was served as Ok. Proving must rescue a mislabelled blob and nothing else; \
             if `None` from the prover does not become DataLoss, the read side has stopped being \
             an integrity check and this store's entire purpose is gone",
    );
    assert_eq!(
        err.code,
        Code::DataLoss,
        "#fl1786-read-half: the unprovable-read verdict must stay `Code::DataLoss`. \
         `existence_cache_store.rs:68-73` and `worker_proxy_store.rs:1158` both key eviction \
         decisions on this exact code; changing it silently re-routes them. got={err:?}"
    );
    assert!(
        err.to_string().contains("Hash mismatch on read"),
        "#fl1786-read-half: the unprovable-read message must remain byte-identical to the \
         pre-proving one — four pre-existing assertions (verify_store_test.rs:429/566/818/826) \
         match on this substring, and operators grep it. got={err:?}"
    );
    assert!(
        err.to_string().contains(BLAKE3_OF_VALUE),
        "#fl1786-read-half: the message must still report the hash computed under the \
         CONTEXT-RESOLVED function (blake3 here), not a prover candidate. Reporting a different \
         candidate's hash would change a string operators and dashboards read. got={err:?}"
    );
    logs_assert(|lines: &[&str]| assert_proving_reason(lines, REASON_UNPROVABLE));
    Ok(())
}

/// The trigger ("hash mismatch") and the precondition ("the inner store still
/// holds the bytes") can come apart: the blob may be evicted between the two
/// reads. That MUST fail closed. An unreadable second pass proves nothing, and
/// "proves nothing" is not "proves fine".
#[nativelink_test]
async fn blob_evicted_between_the_two_reads_fails_closed() -> Result<(), Error> {
    pin_production_default_blake3();
    let counting = CountingStore::new();
    let store = verify_store_over(Store::new(counting.clone()));
    let digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;

    store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("mislabelled_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect("#fl1786-read-half: PRECONDITION FAILED — the write side must admit this blob");

    // From here the blob is gone to every read after the first.
    counting
        .vanish_after_first_read
        .store(true, Ordering::SeqCst);

    let err = store.get_part_unchunked(digest, 0, None).await.expect_err(
        "#fl1786-read-half: FAIL-OPEN ON THE RE-READ — the blob vanished between the streaming \
         pass and the proving pass, so NOTHING was proved, and the read still returned Ok. An \
         errored or empty second pass must never be treated as a successful proof; that is the \
         difference between 'no advertised function reproduces it' and 'I could not look'",
    );
    assert_eq!(
        err.code,
        Code::DataLoss,
        "#fl1786-read-half: an unprovable read must return DataLoss whatever the reason proving \
         failed, so the verdict an operator sees does not depend on which of two internal passes \
         hit the fault. got={err:?}"
    );
    // The VERDICT is deliberately identical to the corrupt-blob one, so the
    // log's `proving` field is the ONLY thing that tells an operator the inner
    // store faulted rather than the bytes being bad.
    logs_assert(|lines: &[&str]| assert_proving_reason(lines, REASON_UNREADABLE));
    Ok(())
}

/// The proving pass proves something about the bytes IT read. The caller
/// already holds the bytes the FIRST pass read. If those two differ, the
/// proof is about a blob the caller never received — so a proof over pass 2
/// may only license pass 1's bytes when both passes saw the same thing.
///
/// Here pass 2's bytes genuinely DO reproduce the declared digest under
/// sha256, so a proving implementation that trusts the re-read blindly
/// returns `Ok` — while the caller is holding entirely different bytes. That
/// is a fail-open that serves unverified content under a verified digest, and
/// it is strictly worse than the `DataLoss` this change exists to remove.
#[nativelink_test]
async fn a_re_read_that_returns_different_bytes_does_not_license_the_served_ones()
-> Result<(), Error> {
    pin_production_default_blake3();
    let counting = CountingStore::new();
    let store = verify_store_over(Store::new(counting.clone()));

    // Declared digest is sha256("999"); the stored bytes are "123". Same
    // length, so the size check passes and execution reaches the hash check.
    let digest = DigestInfo::try_new(SHA256_OF_OTHER_VALUE, VALUE.len() as u64)?;
    counting.inner.update_oneshot(digest, VALUE.into()).await?;
    // ...but the SECOND read serves "999", which sha256-proves the digest.
    *counting.serve_after_first_read.lock() = Some(Bytes::from_static(OTHER_VALUE.as_bytes()));

    let err = store.get_part_unchunked(digest, 0, None).await.expect_err(
        "#fl1786-read-half: FAIL-OPEN — the proving re-read returned DIFFERENT bytes than the \
         pass already streamed to the caller, those bytes proved the digest, and the read was \
         certified Ok. The caller is now holding unverified content stamped with a verified \
         digest. Proving may only license the bytes it actually hashed: the labelled function's \
         hash must agree across both passes",
    );
    assert_eq!(
        err.code,
        Code::DataLoss,
        "#fl1786-read-half: an inconsistent re-read must fail closed as DataLoss. got={err:?}"
    );
    // A store that serves different bytes for one key is a storage-integrity
    // incident of a different severity from a corrupt blob — and this project
    // has an open MemoryStore partial-write corruption issue. The `proving`
    // field is the only place that distinction survives.
    logs_assert(|lines: &[&str]| assert_proving_reason(lines, REASON_INCONSISTENT));
    Ok(())
}

// ---------------------------------------------------------------------------
// THE ZERO-COST CLAIM — mismatch-gating is only worth anything if the happy
// path really is untouched.
// ---------------------------------------------------------------------------

/// The whole reason this is mismatch-gated rather than always-proving is that
/// reads dominate this system. That argument is only true if a correctly
/// labelled read issues exactly ONE inner read and folds exactly ONE hasher.
/// Without this assertion "zero added cost" is an unverified claim.
#[nativelink_test]
async fn happy_path_read_does_not_re_read_the_inner_store() -> Result<(), Error> {
    pin_production_default_blake3();
    let counting = CountingStore::new();
    let store = verify_store_over(Store::new(counting.clone()));

    // A correctly-labelled blake3 blob: the process default reproduces it, so
    // the first pass matches and proving must never be entered.
    let digest = DigestInfo::try_new(BLAKE3_OF_VALUE, VALUE.len() as u64)?;
    store.update_oneshot(digest, VALUE.into()).await?;

    let before = counting.calls();
    let bytes = store.get_part_unchunked(digest, 0, None).await?;
    let reads = counting.calls() - before;

    assert_eq!(bytes, VALUE.as_bytes(), "#fl1786-read-half: wrong bytes");
    assert_eq!(
        reads, 1,
        "#fl1786-read-half: the HAPPY PATH re-read the inner store ({reads} reads, expected 1). \
         Mismatch-gating was chosen over always-proving specifically because reads dominate this \
         system; if the matched path also pays a second full read of the blob, the design's only \
         advantage over always-proving is gone and the cost is far WORSE than always-proving \
         (a whole extra store read versus one extra hash pass)"
    );
    Ok(())
}

/// Proving is entered only on mismatch — and when it IS entered, it costs
/// exactly one extra read, not a retry loop. Pins the mismatch-path cost so a
/// future change that turns it into a per-candidate read is caught.
#[nativelink_test]
async fn proven_read_costs_exactly_one_extra_inner_read() -> Result<(), Error> {
    pin_production_default_blake3();
    let counting = CountingStore::new();
    let store = verify_store_over(Store::new(counting.clone()));
    let digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;

    store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("mislabelled_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect("#fl1786-read-half: PRECONDITION FAILED — the write side must admit this blob");

    let before = counting.calls();
    store.get_part_unchunked(digest, 0, None).await.expect(
        "#fl1786-read-half: the proven read must succeed; see \
         sha256_keyed_blob_written_then_read_with_no_context_round_trips",
    );
    let reads = counting.calls() - before;

    assert_eq!(
        reads, 2,
        "#fl1786-read-half: the proving path issued {reads} inner reads, expected exactly 2 (the \
         streaming pass plus ONE re-read that folds every candidate in a single pass). \
         `DigestFuncProver` exists precisely so N candidates cost ONE read; a count above 2 means \
         someone re-read per candidate and the cost is now O(candidates x blob bytes) of store I/O"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// OBSERVABILITY — the falsifier for this whole class.
// ---------------------------------------------------------------------------

/// `hash_verification_failures` carried TWO roles (write-side reject at
/// `verify_store.rs:348`, read-side mismatch at `:464`) — a conflation the
/// delta's own doc-comment states at `:99-104`. So the falsifier both review
/// rounds proposed ("`digest_func_proven` rising while
/// `hash_verification_failures` also rises") was not implementable: a co-rise
/// could not discriminate a rejected corrupt write from a failed read.
///
/// This pins the split: a rescued read bumps the read-side proven counter and
/// NOTHING else; an unprovable read bumps the read-side failure counter.
#[nativelink_test]
async fn read_side_counters_separate_the_rescued_from_the_rejected() -> Result<(), Error> {
    pin_production_default_blake3();
    let inner = MemoryStore::new(&MemorySpec::default());
    let store = verify_store_over(Store::new(inner.clone()));

    let proven = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;
    store
        .update_oneshot(proven, VALUE.into())
        .instrument(info_span!("mislabelled_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect("#fl1786-read-half: PRECONDITION FAILED — the write side must admit this blob");

    let failures_after_write = store.hash_verification_failure_count();
    store
        .get_part_unchunked(proven, 0, None)
        .await
        .expect("#fl1786-read-half: the proven read must succeed");

    assert_eq!(
        store.digest_func_proven_on_read_count(),
        1,
        "#fl1786-read-half: a read rescued by proving did NOT bump \
         `digest_func_proven_on_read`. This counter is the ONLY engaged-mechanism signal for the \
         read half — with it dark, a zero reading cannot be told apart from 'the mechanism never \
         fires', which is exactly the dark-counter trap this chain hit twice already"
    );
    assert_eq!(
        store.hash_verification_failure_count(),
        failures_after_write,
        "#fl1786-read-half: a read RESCUED by proving bumped the data-integrity alarm. A rescued \
         read is not a failure; ticking `hash_verification_failures` for it makes the alarm fire \
         on exactly the traffic this change exists to save, which is the 'loud signal repurposed \
         to mean something else' class"
    );
    assert_eq!(
        store.hash_verification_failure_on_read_count(),
        0,
        "#fl1786-read-half: a rescued read bumped the read-side FAILURE counter"
    );

    // Now the genuinely corrupt read.
    let corrupt = DigestInfo::try_new(SHA256_OF_OTHER, VALUE.len() as u64)?;
    inner.update_oneshot(corrupt, VALUE.into()).await?;
    let _ = store.get_part_unchunked(corrupt, 0, None).await;

    assert_eq!(
        store.hash_verification_failure_on_read_count(),
        1,
        "#fl1786-read-half: an UNPROVABLE read did not bump `hash_verification_failures_on_read`. \
         Splitting the read role out of `hash_verification_failures` is what makes the two roles \
         distinguishable at all; without this tick an operator still cannot tell a rejected \
         corrupt WRITE from a failed READ, and the falsifier both review rounds asked for stays \
         unimplementable"
    );
    assert_eq!(
        store.hash_verification_failure_count(),
        failures_after_write + 1,
        "#fl1786-read-half: the aggregate `hash_verification_failures` must STILL tick on an \
         unprovable read. It is the pre-existing alarm operators and dashboards key on; the split \
         adds a decomposition, it does not move the total"
    );
    assert_eq!(
        store.digest_func_proven_on_read_count(),
        1,
        "#fl1786-read-half: an unprovable read bumped the RESCUED counter"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// LOG VOLUME — the read side is the HOTTER of the two proving sites.
// ---------------------------------------------------------------------------

/// The sampler the read call site consumes, pinned as a pure function.
///
/// Its write-side twin `proven_write_log_is_sampled_first_then_every_64th`
/// (`verify_store_digest_func_proving_test.rs:354`) makes the identical
/// assertions; this is deliberate duplication and its marginal mutation-kill
/// power over that twin is ZERO. It is here for one reason: the two proving
/// sites share ONE sampler, that sharing is what the
/// `DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD` doc-comment claims ("the two proving
/// sites emit at the same rate"), and this file is where a reader of the read
/// half looks. The assertion that actually kills the read-side mutation is
/// `sixty_five_proven_reads_emit_two_warns_and_count_sixty_five` below.
#[nativelink_test]
async fn read_proving_warn_is_sampled_first_then_every_64th() {
    assert_eq!(
        DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD, 64,
        "#fl1786-read-half: the read site must sample at the SAME period as the write site — \
         both call `digest_func_proven_log_decision`, and the doc-comment on the constant tells \
         an operator the two proving sites emit at one rate"
    );
    assert!(
        digest_func_proven_log_decision(1),
        "#fl1786-read-half: the FIRST rescued read must always emit; it is the operator's entry \
         point into a mislabelling incident"
    );
    assert!(
        !digest_func_proven_log_decision(2),
        "#fl1786-read-half: occurrence 2 must be SUPPRESSED"
    );
    assert!(
        digest_func_proven_log_decision(DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD),
        "#fl1786-read-half: occurrence {DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD} must emit — a \
         sampler that only ever emits once makes a sustained mislabelling reader invisible"
    );
}

/// And pin that the READ CALL SITE honours the sampler, against the real store.
///
/// **This is the assertion that was missing.** Mutating
/// `if digest_func_proven_log_decision(cumulative)` to `if true` in
/// `prove_read_or_fail` left all 37 tests in this chain green
/// (`.claude/reviews/fc317cca/pair-b.md` T2) — the exact failure mode the write
/// half's own test comment names: *"Asserting only the pure function would
/// leave an unconditional `warn!` at the call site perfectly green."* The write
/// half carries both pins; the read half carried neither, on the hotter path.
///
/// ONE mislabelled blob, read 65 times — which is also the non-amortisation
/// property stated in `reread_and_prove`'s doc-comment: nothing records the
/// proven function, so every read of that blob proves again and emits again.
/// The rescued readers in production are the scheduler's prefetch / cache-warm
/// / tree-resolution tasks, which walk WHOLE DIRECTORY TREES, and `warn!` is
/// not compiled out in release (`release_max_level_info`) on a server with a
/// standing write-burst-stall incident.
///
/// The filter is the READ-side message. The seeding write is itself a proven
/// write and emits the write-side `warn!`, whose text is "accepted a write
/// whose digest function was PROVEN…"; matching the shared fragment "digest
/// function was PROVEN" would silently count it.
// NOTE: no explicit `#[tracing_test::traced_test]` — `#[nativelink_test]`
// already applies it (`nativelink-macro/src/lib.rs`), and applying it twice
// nests two capture buffers so `logs_assert` reads an EMPTY one.
#[nativelink_test]
async fn sixty_five_proven_reads_emit_two_warns_and_count_sixty_five() -> Result<(), Error> {
    pin_production_default_blake3();
    let store = verify_store_over(Store::new(MemoryStore::new(&MemorySpec::default())));
    let digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;

    store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("mislabelled_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect("#fl1786-read-half: PRECONDITION FAILED — the write side must admit this blob");

    for i in 0..65_u32 {
        store.get_part_unchunked(digest, 0, None).await.expect(
            "#fl1786-read-half: every read of a mislabelled-but-intact blob must be rescued; a \
             failure here means the round trip regressed and the log-volume assertion below \
             would be measuring nothing",
        );
        assert_eq!(
            store.digest_func_proven_on_read_count(),
            u64::from(i) + 1,
            "#fl1786-read-half: read {i} did not take the proving path. Every read pays proving \
             because nothing records the proven function; if some read is served without it, the \
             65 below are not 65 rescues and the emitted-line count means nothing"
        );
    }

    assert_eq!(
        store.digest_func_proven_on_read_count(),
        65,
        "#fl1786-read-half: the COUNTER must carry the true rate — it is what the sampled log \
         gives up precision for, and the /metrics contract the read half's observability rests on"
    );
    logs_assert(|lines: &[&str]| {
        let emitted = lines
            .iter()
            .filter(|l| l.contains("served a read whose digest function was PROVEN"))
            .count();
        if emitted == 2 {
            Ok(())
        } else {
            Err(format!(
                "#fl1786-read-half: 65 rescued reads must emit exactly 2 warns (occurrences 1 \
                 and 64); emitted {emitted}. 65 means the read call site ignores \
                 digest_func_proven_log_decision and is back to one un-rate-limited warn per \
                 rescued read — on the HOTTER of the two proving sites, where a single \
                 context-less scheduler task walking a directory tree emits one line per blob, \
                 and where warn! survives release_max_level_info. 0 means the read half's only \
                 log signal is gone entirely"
            ))
        }
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// THE READER'S OWN FUNCTION — proving must compare across the passes using the
// function THIS READER resolved, not the process default.
// ---------------------------------------------------------------------------

/// A reader that correctly names a NON-DEFAULT digest function, reading a blob
/// keyed under the OTHER advertised one.
///
/// Every other test in this file resolves the process default (blake3) for the
/// read, so `labelled_func` and `default_digest_hasher_func()` are the same
/// value everywhere and the plumbing of the reader-resolved function from
/// `get_part` -> `prove_read_or_fail` -> `reread_and_prove` is never exercised
/// with a value that could distinguish them. Mutating the cross-pass
/// consistency check's `find(|(func, _)| *func == labelled_func)` to
/// `*func == default_digest_hasher_func()` left all 37 tests green
/// (`.claude/reviews/fc317cca/pair-b.md` T4).
///
/// The uncovered failure is precisely INVERTED from the one this change fixes:
/// pass 1's hash is computed under the READER's function, so comparing it
/// against pass 2's DEFAULT-function hash can never match, giving
/// `Inconsistent` -> `DataLoss` for every read whose reader correctly names a
/// non-default function. The fix would break for exactly the population that
/// gets the label right — an `ac_server.rs:487`-style caller, or any REAPI
/// client that sends the `{digest_function}` segment.
#[nativelink_test]
async fn a_reader_naming_a_non_default_function_is_still_rescued() -> Result<(), Error> {
    pin_production_default_blake3();
    let store = verify_store_over(Store::new(MemoryStore::new(&MemorySpec::default())));

    // The blob at rest is BLAKE3-keyed and was admitted cleanly: the process
    // default reproduces its digest, so the write never touched proving.
    let digest = DigestInfo::try_new(BLAKE3_OF_VALUE, VALUE.len() as u64)?;
    store.update_oneshot(digest, VALUE.into()).await.expect(
        "#fl1786-read-half: PRECONDITION FAILED — a correctly-labelled blake3 write must be \
         admitted without proving",
    );
    assert_eq!(
        store.digest_func_proven_count(),
        0,
        "#fl1786-read-half: PRECONDITION FAILED — the seeding write must NOT have been proven; \
         if it was, the blob is not the clean blake3-keyed blob this case needs"
    );

    // The reader names SHA-256 — not the process default, and not the function
    // the blob is keyed under. Pass 1 hashes with SHA-256, mismatches the
    // declared BLAKE3 digest, and proving must rescue it.
    let got = store
        .get_part_unchunked(digest, 0, None)
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Sha256)?)
        .await;

    let bytes = got.expect(
        "#fl1786-read-half: a reader that CORRECTLY names a non-default digest function cannot \
         retrieve a blob keyed under the other advertised one. Proving must compare the two \
         passes under the function THIS READER resolved — pass 1's hash was computed under it, \
         so comparing pass 2 under the process default instead can never agree and yields \
         `Inconsistent` -> DataLoss. That fails the fix for exactly the population that names \
         its digest function correctly, which is the inverse of the bug being fixed",
    );
    assert_eq!(
        bytes,
        VALUE.as_bytes(),
        "#fl1786-read-half: the non-default-reader read returned the wrong bytes"
    );
    assert_eq!(
        store.digest_func_proven_on_read_count(),
        1,
        "#fl1786-read-half: this case must go through the PROVING path — if the read matched on \
         pass 1 the reader's SHA-256 context never reached `get_part` and the assertion above \
         passed without exercising anything"
    );
    logs_assert(|lines: &[&str]| {
        let rescued: Vec<&str> = lines
            .iter()
            .filter(|l| l.contains("served a read whose digest function was PROVEN"))
            .copied()
            .collect();
        match rescued.as_slice() {
            [line]
                if line.contains("labelled_func=SHA256") && line.contains("proven_func=BLAKE3") =>
            {
                Ok(())
            }
            _ => Err(format!(
                "#fl1786-read-half: the rescue line must attribute labelled_func=SHA256 (what \
                 THIS reader resolved, a non-default) and proven_func=BLAKE3 (what the blob is \
                 actually keyed under). Anything else means the two functions were confused \
                 somewhere between `get_part` and `reread_and_prove`, which is invisible in \
                 every test where they are equal. lines: {rescued:?}"
            )),
        }
    });
    Ok(())
}
