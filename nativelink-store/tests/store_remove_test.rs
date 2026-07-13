// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! Tests for `StoreDriver::remove` through the full AC chain and CCS
//! delete-on-detection (#40 §2).
//!
//! Production AC chain shape under test:
//!   CompletenessCheckingStore
//!     └─ ac_store: ExistenceCacheStore<SystemTime>
//!          └─ inner: FastSlowStore
//!               ├─ fast: MemoryStore
//!               └─ slow: MemoryStore   (Redis covered by separate unit test)
//!     └─ cas_store: MemoryStore
//!
//! Test 1 (full-chain remove): write AR through the whole chain, call
//!   remove at top, assert has==None at fast tier, slow tier, ECS, CCS.
//! Test 2 (CCS delete-on-detection): dangling AR detected once → second
//!   get_part returns NotFound with no second "incomplete" warn.
//! Test 3 (idempotency): two sequential removes → second Ok-or-NotFound.
//! Test 4 (over-action guard): complete AR not removed by successful get.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, NoopSpec, StoreDirection,
    StoreSpec,
};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, OutputFile,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};

/// A dummy CAS digest we use as the "output file" referenced by a complete AR.
const CAS_BLOB: DigestInfo = DigestInfo::new([0xCCu8; 32], 8);
/// A dummy CAS digest that is NEVER uploaded — used for dangling ARs.
const MISSING_CAS_BLOB: DigestInfo = DigestInfo::new([0xDDu8; 32], 8);

/// Helper: production-shaped AC chain.
///
/// Returns:
///   * `ccs`       — CompletenessCheckingStore (CCS, top of chain)
///   * `ac_chain`  — Store handle wrapping the ECS (used for raw writes)
///   * `fss_fast`  — MemoryStore that is FSS fast tier (direct handle)
///   * `fss_slow`  — MemoryStore that is FSS slow tier (direct handle)
///   * `cas_store` — MemoryStore used as CAS
fn build_ac_chain() -> (
    Arc<CompletenessCheckingStore>,
    Store,
    Arc<MemoryStore>,
    Arc<MemoryStore>,
    Arc<MemoryStore>,
) {
    let fss_fast = MemoryStore::new(&MemorySpec::default());
    let fss_slow = MemoryStore::new(&MemorySpec::default());
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(fss_fast.clone()),
        Store::new(fss_slow.clone()),
    );
    let ecs_ac = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Noop(NoopSpec::default()), // not used — inner is passed directly
            eviction_policy: Some(EvictionPolicy {
                max_bytes: 10 * 1024 * 1024,
                ..Default::default()
            }),
            log_not_found_at_info: false,
        },
        Store::new(fss),
    );
    let ac_chain = Store::new(ecs_ac.clone());
    let cas_store = MemoryStore::new(&MemorySpec::default());
    let ccs = CompletenessCheckingStore::new(
        ac_chain.clone(),
        Store::new(cas_store.clone()),
    );
    (ccs, ac_chain, fss_fast, fss_slow, cas_store)
}

/// Serialize an AR referencing `cas_digest` and upload it through the CCS
/// (completeness check runs on upload too — write goes to ac_store.update).
async fn write_ar_through_ccs(
    ccs: &Arc<CompletenessCheckingStore>,
    cas_digest: DigestInfo,
) -> Result<DigestInfo, Error> {
    let ar = ProtoActionResult {
        output_files: vec![OutputFile {
            digest: Some(cas_digest.into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    serialize_and_upload_message(
        &ar,
        ccs.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await
}

/// Serialize an AR and upload it DIRECTLY to the AC backend store, bypassing
/// the CCS completeness check — used to create dangling ARs for Test 2.
async fn write_ar_direct(
    ac_store: &Store,
    cas_digest: DigestInfo,
) -> Result<DigestInfo, Error> {
    let ar = ProtoActionResult {
        output_files: vec![OutputFile {
            digest: Some(cas_digest.into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    serialize_and_upload_message(
        &ar,
        ac_store.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await
}

/// Assert `store.has(key) == None` within 5 s with bespoke deadlock message.
async fn assert_gone(store: &Store, key: DigestInfo, label: &str) {
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.has(StoreKey::from(key)),
    )
    .await
    .unwrap_or_else(|_| panic!("has() timed out checking {label}"))
    .unwrap_or_else(|e| panic!("has() error checking {label}: {e:?}"));
    assert!(
        result.is_none(),
        "stale AR survives in {label} after remove — delete-on-detection broken",
    );
}

/// Assert `store.has(key) == Some(_)` within 5 s.
async fn assert_present(store: &Store, key: DigestInfo, label: &str) {
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.has(StoreKey::from(key)),
    )
    .await
    .unwrap_or_else(|_| panic!("has() timed out checking {label}"))
    .unwrap_or_else(|e| panic!("has() error checking {label}: {e:?}"));
    assert!(result.is_some(), "entry unexpectedly absent from {label}");
}

// ─── Test 1: full-chain remove ────────────────────────────────────────────────

/// Write AR through the full chain, call `Store::remove(key)` at the CCS
/// level, then assert `has == None` at every tier:
///   • FSS fast tier (MemoryStore) direct
///   • FSS slow tier (MemoryStore) direct
///   • ECS (Store wrapping the ExistenceCacheStore)
///
/// Mutation assertions (bespoke per-tier messages):
///   skip-fast  → "stale AR survives in MemoryStore fast tier (red-team P2)"
///   skip-slow  → "stale AR survives in MemoryStore slow tier"
///   skip-ECS   → "stale AR survives in ECS top-level has"
#[nativelink_test]
async fn full_chain_remove_clears_all_tiers() -> Result<(), Error> {
    let (ccs, ac_chain, fss_fast, fss_slow, cas_store) = build_ac_chain();
    let ccs_store = Store::new(ccs.clone());
    let fss_fast_store = Store::new(fss_fast.clone());
    let fss_slow_store = Store::new(fss_slow.clone());

    // Populate the CAS so the AR is complete.
    cas_store
        .update_oneshot(CAS_BLOB, Bytes::from_static(b"12345678"))
        .await?;

    // Write the AR through the whole chain.
    let ac_key = write_ar_through_ccs(&ccs, CAS_BLOB).await?;

    // Fast tier and ECS get the entry synchronously.
    assert_present(&fss_fast_store, ac_key, "FSS fast before remove").await;
    assert_present(&ac_chain, ac_key, "ECS before remove").await;

    // The slow write in FastSlowStore is fire-and-forget (background spawn).
    // Poll the slow-tier MemoryStore directly until it appears (up to 5 s) so
    // the subsequent remove() call exercises the "key present in slow tier" path.
    // Without this wait the slow write might not have landed yet, making the
    // "MemoryStore slow tier" post-remove assertion pass trivially for the wrong
    // reason (key never written → trivially absent → mutation invisible).
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fss_slow_store
                .has(StoreKey::from(ac_key))
                .await
                .ok()
                .flatten()
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow-tier write must complete within 5 s — deadlock detector");

    // Remove from the top of the chain.
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs_store.remove(StoreKey::from(ac_key)),
    )
    .await
    .expect("remove() must not hang — deadlock detector")
    .expect("remove() must succeed for a present entry");

    // Every tier must be empty.
    assert_gone(&fss_fast_store, ac_key, "MemoryStore fast tier (red-team P2)").await;
    assert_gone(&fss_slow_store, ac_key, "MemoryStore slow tier").await;
    assert_gone(&ac_chain, ac_key, "ECS top-level has").await;
    assert_gone(&ccs_store, ac_key, "CCS top-level has after remove").await;

    Ok(())
}

// ─── Test 2: CCS delete-on-detection ─────────────────────────────────────────

/// After CCS detects a dangling AR (CAS blob missing), the AR is deleted from
/// the AC chain. A second `get_part_unchunked` on the same key must return a
/// clean `NotFound` without firing the incomplete warn a second time.
///
/// Mutation: comment out the `remove` call in CCS → second warn fires →
/// `logs_assert` fails with message "incomplete warn fired 2 time(s)".
#[nativelink_test]
async fn ccs_deletes_dangling_ar_on_detection() -> Result<(), Error> {
    let (ccs, _ac_chain, _fss_fast, _fss_slow, _cas_store) = build_ac_chain();

    // Write a dangling AR directly into the AC backend (no CAS blob).
    let ac_key = write_ar_direct(ccs.ac_store(), MISSING_CAS_BLOB).await?;

    // First get: CCS detects incomplete → NotFound → should DELETE the entry.
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("first get must not hang — 5 s deadlock detector")
    .expect_err("first get must return NotFound for dangling AR");
    assert_eq!(r1.code, Code::NotFound, "first CCS get must return NotFound");

    // Second get: entry deleted; must be a clean NotFound without warn.
    let r2 = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("second get must not hang — 5 s deadlock detector")
    .expect_err("second get must return NotFound (entry was deleted on first detection)");
    assert_eq!(r2.code, Code::NotFound, "second CCS get must return NotFound");

    // Incomplete warn must fire exactly once (warn level, bespoke message).
    // Mutation: comment out CCS remove → count == 2 → test red-fails with
    // "incomplete warn fired 2 time(s)..."; count == 0 → fails with lower-
    // bound message.
    logs_assert(|lines: &[&str]| {
        let count = lines
            .iter()
            .filter(|l| {
                l.contains(" WARN ")
                    && l.contains("ActionResult incomplete")
                    && l.contains("get_part path")
            })
            .count();
        match count {
            0 => Err("incomplete warn must fire at least once on first dangling-AR get".to_string()),
            1 => Ok(()),
            _ => Err(format!(
                "incomplete warn fired {count} time(s); delete-on-detection must \
                 suppress repeat warns — remove call in CCS missing (mutation \
                 would reproduce this)"
            )),
        }
    });

    Ok(())
}

// ─── Test 3: idempotency ──────────────────────────────────────────────────────

/// Two sequential removes on the same key must not panic.
/// Second remove may return Ok or Code::NotFound — both are acceptable.
#[nativelink_test]
async fn remove_idempotent_second_remove_ok_or_not_found() -> Result<(), Error> {
    let (ccs, _ac_chain, _fss_fast, _fss_slow, cas_store) = build_ac_chain();
    let ccs_store = Store::new(ccs.clone());

    cas_store
        .update_oneshot(CAS_BLOB, Bytes::from_static(b"12345678"))
        .await?;
    let ac_key = write_ar_through_ccs(&ccs, CAS_BLOB).await?;

    // First remove — must succeed.
    ccs_store
        .remove(StoreKey::from(ac_key))
        .await
        .expect("first remove must succeed for a present entry");

    // Second remove — Ok or NotFound, no panic.
    match ccs_store.remove(StoreKey::from(ac_key)).await {
        Ok(()) => {}
        Err(ref e) if e.code == Code::NotFound => {}
        Err(e) => panic!("second remove returned unexpected error: {e:?}"),
    }

    Ok(())
}

// ─── Test 3b: remove on never-written key ────────────────────────────────────

/// Remove a key that was never written must return `Ok(())` or
/// `Code::NotFound` without panicking.
#[nativelink_test]
async fn remove_never_written_key_no_panic() -> Result<(), Error> {
    let (ccs, ..) = build_ac_chain();
    let ccs_store = Store::new(ccs.clone());
    let never_written = DigestInfo::new([0xFFu8; 32], 42);
    match ccs_store.remove(StoreKey::from(never_written)).await {
        Ok(()) => {}
        Err(ref e) if e.code == Code::NotFound => {}
        Err(e) => panic!("remove of never-written key returned unexpected error: {e:?}"),
    }
    Ok(())
}

// ─── Test 4: over-action guard ────────────────────────────────────────────────

/// A COMPLETE AR must NOT be removed by a successful `get_part_unchunked`.
/// CCS delete-on-detection must ONLY fire on the incomplete branch.
#[nativelink_test]
async fn complete_ar_not_removed_by_successful_get() -> Result<(), Error> {
    let (ccs, ac_chain, fss_fast, _fss_slow, cas_store) = build_ac_chain();
    let fss_fast_store = Store::new(fss_fast.clone());

    cas_store
        .update_oneshot(CAS_BLOB, Bytes::from_static(b"12345678"))
        .await?;
    let ac_key = write_ar_through_ccs(&ccs, CAS_BLOB).await?;

    // Successful get — all CAS blobs present.
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("get must not hang — 5 s deadlock detector")
    .expect("get must succeed when all CAS blobs are present");

    // AR must still be present in all tiers after a complete get.
    assert_present(&fss_fast_store, ac_key, "FSS fast after complete get").await;
    assert_present(&ac_chain, ac_key, "ECS after complete get — over-action guard").await;

    Ok(())
}

