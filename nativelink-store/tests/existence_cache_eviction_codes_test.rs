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

//! Regression tests for the ExistenceCacheStore stale-entry preservation
//! bug.
//!
//! Pre-fix `get_part` and `batch_get_part_unchunked` only evicted the
//! cached existence entry when the inner store returned `Code::NotFound`.
//! For `Code::DataLoss` (truncation/hash mismatch from VerifyStore),
//! `Code::Internal` (storage faults), and `Code::OutOfRange` the cache
//! entry was preserved — leaving subsequent `has()` calls reporting the
//! blob as present even though it cannot be retrieved.
//!
//! Operationally this manifests as: VerifyStore catches a corruption,
//! propagates DataLoss to the caller, the existence cache still says
//! "I have it", and the next FindMissingBlobs / has_with_results /
//! upload-skip fast-path keeps trusting the stale entry. The blob
//! becomes a zombie — present in the cache, unreadable from the inner
//! store, can't be re-uploaded because the cache claims it exists.

use core::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{ExistenceCacheSpec, MemorySpec, NoopSpec, StoreSpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

// Additional stable hashes for multi-digest tests (e.g. batch_get_part).
// Each is hex-distinct so `logs_contain` can differentiate per-digest log
// entries without false matches on substrings of the staged digest.
const VALID_HASH_B: &str = "fedcba9876543210000000000000000000020000000000000fedcba987654321";
const VALID_HASH_C: &str = "aabbccddeeff001100000000000000000003000000000000aabbccddeeff0011";

/// A wrapping Store that delegates to an inner MemoryStore for `update`
/// (so we can stage data) but returns a configurable error `Code` from
/// `get_part`. This simulates VerifyStore-detected corruption,
/// internal storage faults, etc.
#[derive(Debug, MetricsComponent)]
struct ErrCodeOnGetStore {
    inner: Store,
    /// Error code to return on get_part.
    err_code: Code,
    /// Atomic count of get_part calls (to verify retry behavior etc).
    get_part_calls: AtomicU32,
    /// When true, `has_with_results` reports every queried digest as
    /// missing regardless of inner state. Used by stale-positive tests
    /// that prime the cache via a real `update_oneshot` (cache + inner
    /// both have the digest), then flip this flag so the next
    /// update / update_oneshot / batch read sees the cache claiming
    /// "yes" while the inner store reports "no" — exactly the
    /// invariant the `error!` log is meant to surface.
    has_returns_none: AtomicBool,
}

impl ErrCodeOnGetStore {
    fn new(inner: Store, err_code: Code) -> Self {
        Self {
            inner,
            err_code,
            get_part_calls: AtomicU32::new(0),
            has_returns_none: AtomicBool::new(false),
        }
    }

    fn set_has_returns_none(&self, val: bool) {
        self.has_returns_none.store(val, Ordering::SeqCst);
    }
}

default_health_status_indicator!(ErrCodeOnGetStore);

#[async_trait]
impl StoreDriver for ErrCodeOnGetStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        if self.has_returns_none.load(Ordering::SeqCst) {
            // Simulate the inner store losing track of every queried
            // digest (eviction race / data loss / cache populated by an
            // unverified path). Caller's existence cache may still
            // believe the digest is present — that mismatch is exactly
            // what the stale-positive `error!` logs guard.
            for r in results.iter_mut() {
                *r = None;
            }
            return Ok(());
        }
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.inner.update(key, reader, upload_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        self.get_part_calls.fetch_add(1, Ordering::Relaxed);
        Err(make_err!(
            self.err_code,
            "ErrCodeOnGetStore: simulated {:?} from inner store",
            self.err_code
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
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
}

/// Build an ExistenceCacheStore with the given inner-error code and
/// pre-stage a digest in the existence cache (writing succeeds; cache
/// records the entry; then reads return `err_code`).
///
/// The backing memory store is wrapped in `ErrCodeOnGetStore` so that:
/// - `update_oneshot` reaches the inner MemoryStore (blob is stored)
/// - `get_part` returns the configured error code
///
/// The cache observes the successful write via the item callback and
/// records the digest. We return the store + error-injection store +
/// digest so tests can verify cache behavior when reads fail.
async fn make_primed_cache_store(
    err_code: Code,
) -> Result<
    (
        Arc<ExistenceCacheStore<std::time::SystemTime>>,
        Arc<ErrCodeOnGetStore>,
        DigestInfo,
    ),
    Error,
> {
    let backing_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let err_store = Arc::new(ErrCodeOnGetStore::new(backing_mem, err_code));
    let inner = Store::new(err_store.clone());
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: None,
    };
    let store = ExistenceCacheStore::new(&spec, inner);

    let data = Bytes::from_static(b"primed cache data");
    let digest = DigestInfo::try_new(VALID_HASH, data.len() as u64)?;

    // Write through the cache store. The update path writes to the
    // backing memory store (via ErrCodeOnGetStore which proxies writes
    // through) then the cache observes the digest.
    store
        .update_oneshot(digest, data)
        .await
        .err_tip(|| "setup: priming write")?;

    assert!(
        store.exists_in_cache(&digest).await,
        "cache must contain the digest after priming write"
    );

    Ok((store, err_store, digest))
}

// -------------------------------------------------------------------
// 1. get_part returns DataLoss → cache MUST evict the stale entry
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_data_loss_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::DataLoss).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "get_part should fail with DataLoss");
    assert_eq!(result.unwrap_err().code, Code::DataLoss);

    // BUG: pre-fix, only NotFound triggered eviction. DataLoss left
    // the cache lying — `exists_in_cache` would return true.
    assert!(
        !store.exists_in_cache(&digest).await,
        "DataLoss must evict the cache entry — VerifyStore caught corruption \
         and the blob is unreadable"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 2. get_part returns Internal → cache MUST evict the stale entry
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_internal_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::Internal).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::Internal);

    assert!(
        !store.exists_in_cache(&digest).await,
        "Internal must evict the cache entry — storage fault means blob \
         is unrecoverable from this inner store"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 3. get_part returns OutOfRange → cache MUST evict the stale entry
//
// OutOfRange means the requested byte range exceeds blob length, which
// for a full read indicates the blob is truncated server-side.
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_out_of_range_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::OutOfRange).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::OutOfRange);

    assert!(
        !store.exists_in_cache(&digest).await,
        "OutOfRange must evict the cache entry — blob is truncated"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 4. get_part returns NotFound → cache MUST evict (regression guard)
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_not_found_evicts_cache_unchanged() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::NotFound).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::NotFound);

    assert!(
        !store.exists_in_cache(&digest).await,
        "NotFound must continue to evict (existing behavior)"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 5. get_part returns Unavailable → cache should NOT evict (transient)
//
// Unavailable signals a transient connectivity issue. Evicting on
// transient codes would force re-uploads on every blip, defeating the
// cache's purpose. Pre-fix this case fell into `Err(_) => {}` (no
// eviction). We want the FIX to preserve that exact behavior — we
// only widen on permanent-failure codes (DataLoss/Internal/OutOfRange/
// NotFound), not transient ones.
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_unavailable_does_not_evict_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::Unavailable).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::Unavailable);

    assert!(
        store.exists_in_cache(&digest).await,
        "Unavailable is transient — cache entry must be preserved \
         (otherwise every connectivity blip forces a re-upload)"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 6. batch_get_part_unchunked DataLoss → cache MUST evict
// -------------------------------------------------------------------
#[nativelink_test]
async fn batch_get_part_data_loss_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::DataLoss).await?;

    let keys: Vec<StoreKey<'_>> = vec![digest.into()];
    let results = store
        .as_store_driver_pin()
        .batch_get_part_unchunked(keys, None)
        .await;

    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
    assert_eq!(results[0].as_ref().err().unwrap().code, Code::DataLoss);

    assert!(
        !store.exists_in_cache(&digest).await,
        "batch_get_part: DataLoss must evict the cache entry"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 7. Stale-positive removal MUST surface as error!-level log.
//
// Per CLAUDE.md "be NOISY when impossible state happens": a
// stale-positive in the existence cache (`has` says yes, `get_part`
// returns NotFound on the same blob) is a contract violation — the
// inner store lost data after we cached its existence, OR the cache
// was populated by something other than a verified write, OR the
// inner store's `has()` returned a stale positive itself. None of
// these should happen in normal operation. Production saw 14,776
// "not found in inner store or any worker" errors in ~24h — many
// of which are these stale positives. They MUST be visible to
// operators at error level so the systemic issue surfaces.
//
// This test:
//   1. Primes the cache via update_oneshot (cache becomes positive)
//   2. Bypasses the cache to verify the digest is cached
//   3. Calls get_part with the inner store wired to return NotFound
//   4. Asserts the get_part returns NotFound
//   5. Asserts the cache entry was removed
//   6. Asserts an error!-level log fired with "stale positive" wording
//      and the actual digest (so operators can correlate)
//
// `nativelink_test` macro applies `#[traced_test]`; `logs_contain`
// returns true if any captured event contains the given substring
// across ALL levels — but the message MUST mention "stale positive"
// so we know the error! call (not some debug! line) fired.
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_not_found_logs_error_for_stale_positive() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::NotFound).await?;

    // Sanity: cache MUST be primed before we can claim a stale positive.
    assert!(
        store.exists_in_cache(&digest).await,
        "precondition: cache must contain the digest after the priming \
         write — otherwise get_part wouldn't be detecting a stale positive"
    );

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "get_part must propagate NotFound");
    assert_eq!(result.unwrap_err().code, Code::NotFound);

    assert!(
        !store.exists_in_cache(&digest).await,
        "NotFound must remove the stale cache entry (regression guard)"
    );

    // The error log MUST fire. Without this, 14k+ stale positives per
    // day stay invisible to operators.
    assert!(
        logs_contain("existence cache stale positive"),
        "expected error!-level log with 'existence cache stale positive' \
         when ExistenceCacheStore::get_part removes a stale entry — without \
         it operators have no way to see the cache invariant being violated \
         in production (14,776 events / 24h on buildcache 2026-04-26)"
    );

    // Specificity: the digest hash MUST appear in the log so operators can
    // correlate against upstream/downstream traces. A log saying only
    // "stale positive detected" without the offending digest is useless.
    // Grep for the stable hex hash prefix rather than `format!("{digest}")`
    // — DigestInfo's Display impl is not part of the contract this test
    // guards, so depending on it makes the test brittle to formatting tweaks.
    assert!(
        logs_contain(VALID_HASH),
        "expected the offending digest hash {VALID_HASH} in the stale-positive \
         error log so operators can grep for it"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 8. Stale positive on update() path MUST surface as error!-level log.
//
// This is a sibling of test #7. The update() path has its OWN
// `error!` site (`existence_cache_store.rs` ~line 327) that fires
// when a write arrives for a digest the cache claims to have but the
// inner store's `has()` reports as missing. Without this test the
// error! at that site can be silently regressed (testing-czar
// HOLD-MAJOR mutation finding: 3 of 4 sites shipped UNPROTECTED).
// -------------------------------------------------------------------
#[nativelink_test]
async fn update_logs_error_for_stale_positive() -> Result<(), Error> {
    // Prime cache + inner via the standard helper (err_code is unused
    // here because we only call update, not get_part).
    let (store, err_store, digest) = make_primed_cache_store(Code::NotFound).await?;
    assert!(
        store.exists_in_cache(&digest).await,
        "precondition: cache must contain the digest after the priming write"
    );

    // Now flip the inner store's has() to always report None — this
    // creates the stale-positive scenario at the moment the next
    // update() runs (cache says yes, inner says no).
    err_store.set_has_returns_none(true);

    // Drive the update path. update() takes a DropCloserReadHalf, so
    // build a buf_channel pair, send the payload + EOF on the writer,
    // and pass the reader to update(). Use try_join! so the send
    // future and the update future progress concurrently — the
    // wrapper's update() drains the reader after detecting the
    // stale positive (reader.drain()) only on the inner-has=Some
    // branch; on the inner-has=None branch update() forwards the
    // reader to inner.update(). The MemoryStore inner accepts the
    // re-upload, completing the test.
    let payload = Bytes::from_static(b"primed cache data");
    let payload_len = payload.len() as u64;
    let (mut tx, rx) = make_buf_channel_pair();
    let send_fut = async move {
        tx.send(payload).await?;
        tx.send_eof()?;
        Ok::<_, Error>(())
    };
    let update_fut = async {
        Pin::new(store.as_ref())
            .update(digest.into(), rx, UploadSizeInfo::ExactSize(payload_len))
            .await
    };
    tokio::try_join!(send_fut, update_fut)?;

    // The error! log MUST fire — assert by message + hash. Without
    // both, a regression that changes the message text but keeps
    // the digest, or vice versa, would slip through.
    assert!(
        logs_contain("existence cache stale positive"),
        "expected error!-level 'existence cache stale positive' log on \
         the update() path when inner.has() returns None for a digest \
         the cache claims to have"
    );
    assert!(
        logs_contain("update path"),
        "expected log to identify the originating path ('update path') \
         so operators can distinguish update vs update_oneshot vs \
         get_part vs batch_get_part stale-positive sources"
    );
    assert!(
        logs_contain(VALID_HASH),
        "expected the offending digest hash {VALID_HASH} in the stale-positive \
         error log on the update() path"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 9. Stale positive on update_oneshot() path MUST surface as
//    error!-level log. Sibling of tests #7 / #8.
// -------------------------------------------------------------------
#[nativelink_test]
async fn update_oneshot_logs_error_for_stale_positive() -> Result<(), Error> {
    let (store, err_store, digest) = make_primed_cache_store(Code::NotFound).await?;
    assert!(
        store.exists_in_cache(&digest).await,
        "precondition: cache must contain the digest after the priming write"
    );

    err_store.set_has_returns_none(true);

    // Re-upload via update_oneshot: the wrapper's overridden
    // update_oneshot is invoked directly here (StoreLike's default
    // update_oneshot would route through update(), which is the
    // wrong site for THIS test).
    store
        .update_oneshot(digest, Bytes::from_static(b"primed cache data"))
        .await?;

    assert!(
        logs_contain("existence cache stale positive"),
        "expected error!-level 'existence cache stale positive' log on \
         the update_oneshot() path"
    );
    assert!(
        logs_contain("update_oneshot path"),
        "expected log to identify the originating path ('update_oneshot path') \
         so operators can distinguish from update / get_part / batch sites"
    );
    assert!(
        logs_contain(VALID_HASH),
        "expected the offending digest hash {VALID_HASH} in the stale-positive \
         error log on the update_oneshot() path"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 10. Stale positive on batch_get_part_unchunked() path MUST surface
//     as error!-level log — AND must fire ONLY for the cached digest,
//     not for sibling digests in the same batch that were never
//     cached.
//
// This composite assertion guards two invariants:
//   - The error! site fires when a stale entry is removed.
//   - It does NOT fire for honest NotFounds on never-cached digests
//     (which would create false-positive operator alarm storms).
// -------------------------------------------------------------------
#[nativelink_test]
async fn batch_get_part_logs_error_for_stale_positive() -> Result<(), Error> {
    // Prime cache + inner with digest A via the standard helper. B
    // and C are NEVER cached; they go straight into the batch and
    // should hit the inner-store NotFound path without producing a
    // stale-positive log entry.
    let (store, _err_store, digest_a) = make_primed_cache_store(Code::NotFound).await?;
    let digest_b = DigestInfo::try_new(VALID_HASH_B, 17u64)?;
    let digest_c = DigestInfo::try_new(VALID_HASH_C, 17u64)?;

    assert!(store.exists_in_cache(&digest_a).await);
    assert!(!store.exists_in_cache(&digest_b).await);
    assert!(!store.exists_in_cache(&digest_c).await);

    let keys: Vec<StoreKey<'_>> = vec![digest_a.into(), digest_b.into(), digest_c.into()];
    let results = store
        .as_store_driver_pin()
        .batch_get_part_unchunked(keys, None)
        .await;
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(Result::is_err));

    // The cache entry for A must be gone (it was the stale positive).
    assert!(!store.exists_in_cache(&digest_a).await);

    // The error! log fired with the canonical wording + the hash of A.
    assert!(
        logs_contain("existence cache stale positive"),
        "expected error!-level 'existence cache stale positive' log on \
         the batch_get_part path for the cached digest"
    );
    assert!(
        logs_contain("batch_get_part"),
        "expected log to identify the originating path ('batch_get_part')"
    );
    assert!(
        logs_contain(VALID_HASH),
        "expected the cached digest's hash {VALID_HASH} in the stale-positive \
         error log"
    );

    // Crucially: B and C were honest NotFounds on never-cached
    // digests. Their hashes must NOT appear in any stale-positive
    // log entry. Without this assertion an off-by-one in the
    // `if removed` gate would produce alarm storms in production.
    assert!(
        !logs_contain(VALID_HASH_B),
        "digest B was never cached — its hash must NOT appear in any \
         stale-positive log entry (honest NotFound stays silent)"
    );
    assert!(
        !logs_contain(VALID_HASH_C),
        "digest C was never cached — its hash must NOT appear in any \
         stale-positive log entry (honest NotFound stays silent)"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 11. Honest NotFound on a never-cached digest MUST stay silent.
//
// Negative-case guard for the `if removed { error!(...) }` gate at
// every stale-positive site. If the gate were dropped, every
// FindMissingBlobs-style read on a never-seen digest would produce
// an error!-level log — drowning operators in noise and rendering
// the alarm useless. This test asserts the GATE, not just the
// presence of the log on a real stale-positive case.
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_not_found_does_not_log_error_for_never_cached_digest()
-> Result<(), Error> {
    // Build the cache store WITHOUT priming anything. The inner is
    // wired to return NotFound on get_part — this is the inner-store
    // genuinely not having the blob, not a stale cache entry.
    let backing_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let err_store = Arc::new(ErrCodeOnGetStore::new(backing_mem, Code::NotFound));
    let inner = Store::new(err_store.clone());
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: None,
    };
    let store = ExistenceCacheStore::new(&spec, inner);

    let digest = DigestInfo::try_new(VALID_HASH, 17u64)?;
    assert!(
        !store.exists_in_cache(&digest).await,
        "precondition: digest must NOT be cached (this is the negative case)"
    );

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::NotFound);

    // The log MUST NOT fire for a digest that was never in the cache.
    // A NotFound on a never-cached digest is honest, expected behavior
    // (the cache simply didn't know about it). Logging error! here
    // would produce 1000s of false alarms per minute on cold-cache
    // reads.
    assert!(
        !logs_contain("existence cache stale positive"),
        "honest NotFound on a never-cached digest must NOT produce a \
         stale-positive error log — the `if removed {{ error!(...) }}` \
         gate at every stale-positive site is what keeps this silent. \
         If this assertion fails, that gate has been dropped."
    );
    assert!(
        !logs_contain(VALID_HASH),
        "the digest hash must NOT appear in any error log for an \
         honest NotFound on a never-cached digest"
    );

    Ok(())
}
