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

//! Tests for CCS pending-registry consult (#12 H4 invariant phase 3/3).
//!
//! # What this tests
//!
//! When CCS's `has_with_results` / `get_part` finds a CAS digest MISSING from
//! the real cas_store, it performs a SECOND check against the
//! `pending_output_locality_registry`:
//!
//!   (i)   digest missing from CAS + registry entry PRESENT + endpoint LIVE
//!         → has() reports PRESENT (rescue); `ccs_pending_registry_rescues_total`
//!         incremented by 1; no "ActionResult incomplete" warn fires; no delete.
//!
//!   (ii)  digest missing from CAS + registry entry present + endpoint DEAD
//!         → rescue does NOT fire; incomplete branch fires normally (warn + delete).
//!
//!   (iii) digest missing from CAS + NO registry entry → unchanged behavior
//!         (incomplete branch fires normally).
//!
//!   (iv)  digest PRESENT in CAS → registry is not consulted at all (short-
//!         circuit boundary: the consult lives only in the "missing from CAS"
//!         branch of CCS's completeness logic, not in any general has()).
//!
//!   (v)   Composite invariant (#40 §2 interaction): gate(delete) ⇒ consult-first.
//!         A registry-resident digest must NEVER trigger the delete-on-detection
//!         branch. Production composition: CCS + registry + live checker.
//!         Mutation: skip the consult → delete fires for a pending blob →
//!         bespoke red "H4 rescue bypassed — pending blob deleted as dangling".
//!
//!   (vi)  CAS-plane boundary: a registry-resident digest must NOT make the
//!         CAS-plane has() return Some. The `pending_output_locality_registry`
//!         is an AC-completeness gate ONLY — it must never bleed into the CAS
//!         layer's own has_with_results. This test composes CCS in production
//!         shape and asserts the CAS store's raw has() sees no rescue effect.
//!
//! # Mutation guidance
//!
//! - Skip the consult in `get_and_verify_single` (comment out registry check):
//!   test (i) red-fails on `has_with_results` and `get_part` paths with:
//!   "H4 rescue bypassed: registry-resident digest reported missing — consult
//!    skipped in get_and_verify_single"
//!
//! - Skip the liveness re-check inside the consult (always trust registry):
//!   test (ii) red-fails with:
//!   "liveness-skip: dead-endpoint rescue must NOT fire — consult must
//!    re-check endpoint liveness"
//!
//! - Skip the counter increment:
//!   test (i) red-fails with:
//!   "counter-skip: ccs_pending_registry_rescues_total must be 1 after rescue"
//!
//! - Skip the consult (composite, test v):
//!   test (v) red-fails on the FIRST assert with:
//!   "H4 rescue bypassed — pending blob reported missing instead of rescued;
//!    first get_part must return Ok"
//!   (the second assert "pending blob deleted as dangling" is only reached
//!   if the first somehow passes — unreachable post-mutation)
//!
//! - Wire the registry into the CAS layer's has():
//!   test (vi) red-fails with:
//!   "registry leaked into CAS-plane has() — upload short-circuit trap"

use core::time::Duration;
use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, OutputFile,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::completeness_checking_store::{
    CompletenessCheckingStore, SharedLivenessChecker, inject_h4_pending_registry_into_ac_chains,
};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::ac_pin_registry::new_shared_ac_pin_registry;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use parking_lot::Mutex;

/// A CAS digest that IS uploaded to the CAS store.
const PRESENT_CAS: DigestInfo = DigestInfo::new([0xAAu8; 32], 8);
/// A CAS digest that is NEVER uploaded — used to create dangling ARs.
const MISSING_CAS: DigestInfo = DigestInfo::new([0xBBu8; 32], 8);
/// Endpoint considered LIVE in the liveness checker.
const LIVE_EP: &str = "grpc://worker-live:50081";
/// Endpoint considered DEAD in the liveness checker.
const DEAD_EP: &str = "grpc://worker-dead:50081";

fn make_liveness_checker(live: &[&str]) -> SharedLivenessChecker {
    let set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(
        live.iter().map(|s| s.to_string()).collect(),
    ));
    Arc::new(move |ep: &str| set.lock().contains(ep))
}

/// Helper: build a CCS with the pending registry + liveness checker wired in.
fn build_ccs_with_registry(
    live_endpoints: &[&str],
) -> (
    Arc<CompletenessCheckingStore>,
    Arc<MemoryStore>, // cas_store
    nativelink_util::ac_pin_registry::SharedAcPinRegistry,
) {
    let cas_store = MemoryStore::new(&MemorySpec::default());
    let ac_backend = MemoryStore::new(&MemorySpec::default());
    let registry = new_shared_ac_pin_registry();
    let checker = make_liveness_checker(live_endpoints);
    let ccs = CompletenessCheckingStore::new_with_pending_registry(
        Store::new(ac_backend),
        Store::new(cas_store.clone()),
        Some(registry.clone()),
        Some(checker),
    );
    (ccs, cas_store, registry)
}

/// Helper: build a CCS WITHOUT any pending registry (current/default behavior).
fn build_ccs_without_registry() -> (Arc<CompletenessCheckingStore>, Arc<MemoryStore>) {
    let cas_store = MemoryStore::new(&MemorySpec::default());
    let ac_backend = MemoryStore::new(&MemorySpec::default());
    let ccs = CompletenessCheckingStore::new(Store::new(ac_backend), Store::new(cas_store.clone()));
    (ccs, cas_store)
}

/// Write an AR referencing `cas_digest` directly into the CCS's AC backend
/// (bypasses completeness check, producing a dangling AR when cas_digest is absent).
async fn write_dangling_ar(
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
        ccs.ac_store().as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await
}

// ─── Test (i): rescue path — has_with_results + get_part ──────────────────────

/// (i-a) `has_with_results` path: CAS digest missing + registry-live → PRESENT.
///
/// CCS's `has_with_results` uses `inner_has_with_results`.  After CAS reports
/// MISSING, the consult checks the pending registry.  When the entry is there
/// AND the endpoint is live, the result flips from None → Some(size).
///
/// Mutation: comment out registry consult in `check_existence_fut` loop →
/// `has_with_results` returns `None` for the AR key → test red-fails:
///   "H4 rescue bypassed: registry-resident digest reported missing — consult
///    skipped in inner_has_with_results"
///
/// Counter mutation: skip the increment →
///   "counter-skip: ccs_pending_registry_rescues_total must be 1 after rescue"
///
/// No "ActionResult incomplete" warn may fire (test uses `tracing_test`).
#[nativelink_test]
#[tracing_test::traced_test]
async fn has_with_results_rescues_registry_resident_digest() -> Result<(), Error> {
    let (ccs, _cas_store, registry) = build_ccs_with_registry(&[LIVE_EP]);
    let ccs_store = Store::new(ccs.clone());

    // Register MISSING_CAS under the live endpoint in the pending registry.
    let store_id: Arc<str> = Arc::from("");
    registry.register_ac_pin(LIVE_EP, store_id, MISSING_CAS);

    // Write a dangling AR (no CAS blob uploaded).
    let ac_key = write_dangling_ar(&ccs, MISSING_CAS).await?;

    // has_with_results must report Some (rescued).
    let mut results = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs_store.has_with_results(
            &[StoreKey::from(ac_key)],
            &mut results,
        ),
    )
    .await
    .expect("has_with_results must not hang — 5s deadlock detector")?;

    assert!(
        results[0].is_some(),
        "H4 rescue bypassed: registry-resident digest reported missing — consult \
         skipped in inner_has_with_results",
    );

    // Counter must be 1.
    let rescues = ccs.pending_registry_rescues_total();
    assert_eq!(
        rescues, 1,
        "counter-skip: ccs_pending_registry_rescues_total must be 1 after rescue; \
         got {rescues}",
    );

    // No "ActionResult incomplete" warn must fire on the rescue path.
    logs_assert(|lines: &[&str]| {
        let warn_count = lines
            .iter()
            .filter(|l| l.contains(" WARN ") && l.contains("ActionResult incomplete"))
            .count();
        if warn_count == 0 {
            Ok(())
        } else {
            Err(format!(
                "has_with_results rescue must suppress incomplete warn — \
                 incomplete warn fired {warn_count} time(s) on registry-rescue path"
            ))
        }
    });

    Ok(())
}

/// (i-b) `get_part` path: CAS digest missing + registry-live → Ok + no delete.
///
/// `get_part` uses `get_and_verify_single`. The consult must rescue the blob
/// so the returned bytes are served and the AC entry is NOT deleted.
///
/// Mutation: comment out registry consult in `get_and_verify_single` →
/// `get_part` returns `Code::NotFound` → test red-fails:
///   "H4 rescue bypassed: registry-resident digest reported missing — consult
///    skipped in get_and_verify_single"
///
/// Counter mutation: skip the increment →
///   "counter-skip: ccs_pending_registry_rescues_total must be 1 (get_part path)"
///
/// Over-action guard: AC entry must NOT be deleted after a rescued get_part.
#[nativelink_test]
#[tracing_test::traced_test]
async fn get_part_rescues_registry_resident_digest_and_no_delete() -> Result<(), Error> {
    let (ccs, _cas_store, registry) = build_ccs_with_registry(&[LIVE_EP]);

    let store_id: Arc<str> = Arc::from("");
    registry.register_ac_pin(LIVE_EP, store_id, MISSING_CAS);

    // Write a dangling AR.
    let ac_key = write_dangling_ar(&ccs, MISSING_CAS).await?;

    // get_part_unchunked must succeed (rescue path).
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("get_part must not hang — 5s deadlock detector");

    assert!(
        result.is_ok(),
        "H4 rescue bypassed: registry-resident digest reported missing — consult \
         skipped in get_and_verify_single; got {:?}",
        result.err(),
    );

    // Counter must be 1.
    let rescues = ccs.pending_registry_rescues_total();
    assert_eq!(
        rescues, 1,
        "counter-skip: ccs_pending_registry_rescues_total must be 1 (get_part path); \
         got {rescues}",
    );

    // No incomplete warn.
    logs_assert(|lines: &[&str]| {
        let warn_count = lines
            .iter()
            .filter(|l| l.contains(" WARN ") && l.contains("ActionResult incomplete"))
            .count();
        if warn_count == 0 {
            Ok(())
        } else {
            Err(format!(
                "get_part rescue must suppress incomplete warn; fired {warn_count} time(s)"
            ))
        }
    });

    // Over-action guard: AC entry must still be present after rescue.
    // A rescued AR must NOT be deleted (delete-on-detection is only for
    // genuinely incomplete ARs, not registry-rescued ones).
    let mut still_there = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs.ac_store().has_with_results(
            &[StoreKey::from(ac_key)],
            &mut still_there,
        ),
    )
    .await
    .expect("post-rescue has must not hang — 5s deadlock detector")?;
    assert!(
        still_there[0].is_some(),
        "over-action: AC entry was deleted after registry rescue — delete-on-detection \
         must ONLY fire for genuinely incomplete ARs",
    );

    Ok(())
}

// ─── Test (ii): dead endpoint — rescue does NOT fire ──────────────────────────

/// (ii) Dead-endpoint rescue must NOT fire.
///
/// The registry holds the digest under DEAD_EP (which fails liveness).
/// CCS must fall through to the incomplete branch (warn + delete).
///
/// Mutation: skip liveness re-check (treat registry entry as sufficient) →
/// has_with_results returns Some → test red-fails:
///   "liveness-skip: dead-endpoint rescue must NOT fire — consult must
///    re-check endpoint liveness"
#[nativelink_test]
#[tracing_test::traced_test]
async fn dead_endpoint_no_rescue() -> Result<(), Error> {
    // Build CCS where DEAD_EP is NOT live.
    let (ccs, _cas_store, registry) = build_ccs_with_registry(&[LIVE_EP]);
    let ccs_store = Store::new(ccs.clone());

    // Register under the DEAD endpoint.
    let store_id: Arc<str> = Arc::from("");
    registry.register_ac_pin(DEAD_EP, store_id, MISSING_CAS);

    let ac_key = write_dangling_ar(&ccs, MISSING_CAS).await?;

    // has_with_results must return None (not rescued — dead endpoint).
    let mut results = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs_store.has_with_results(
            &[StoreKey::from(ac_key)],
            &mut results,
        ),
    )
    .await
    .expect("has_with_results must not hang")?;

    assert!(
        results[0].is_none(),
        "liveness-skip: dead-endpoint rescue must NOT fire — consult must \
         re-check endpoint liveness; registry entry under dead endpoint incorrectly \
         rescued the digest",
    );

    // get_part must return NotFound (incomplete branch fires).
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("get_part must not hang")
    .expect_err("dead-endpoint must not rescue: get_part must return NotFound");
    assert_eq!(
        r.code,
        Code::NotFound,
        "dead-endpoint path must produce NotFound, got {:?}",
        r.code,
    );

    // Counter must remain 0 (no rescue fired).
    let rescues = ccs.pending_registry_rescues_total();
    assert_eq!(
        rescues, 0,
        "counter-skip (dead ep): rescue counter must remain 0 when endpoint is dead; \
         got {rescues}",
    );

    Ok(())
}

// ─── Test (iii): no registry entry → unchanged behavior ───────────────────────

/// (iii) No registry entry → incomplete branch fires as before.
///
/// Even with a registry wired in, if the digest isn't present, behavior is
/// identical to the no-registry case.
#[nativelink_test]
async fn no_registry_entry_unchanged_behavior() -> Result<(), Error> {
    let (ccs, _cas_store, _registry) = build_ccs_with_registry(&[LIVE_EP]);

    // No entry registered for MISSING_CAS.
    let ac_key = write_dangling_ar(&ccs, MISSING_CAS).await?;

    // get_part must return NotFound.
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("get_part must not hang")
    .expect_err("no-registry-entry path must return NotFound");
    assert_eq!(r.code, Code::NotFound);

    // Counter stays 0.
    assert_eq!(
        ccs.pending_registry_rescues_total(),
        0,
        "no-entry path must not increment rescue counter",
    );

    Ok(())
}

// ─── Test (iv): digest present in CAS — registry not consulted ────────────────

/// (iv) When a CAS digest IS present, the registry path is never reached.
/// `ccs_pending_registry_rescues_total` must remain 0.
///
/// Also exercises the short-circuit boundary from deliverable (a):
/// a registry-resident digest must NOT affect the CAS plane's `has_with_results`
/// result (the rescue only changes the AC completeness gate, not CAS returns).
///
/// Mutation: wire the registry consult unconditionally (not just on CAS miss) →
/// counter becomes 1 even on a successful CAS hit → test red-fails:
///   "short-circuit breach: rescue counter must be 0 when CAS hit succeeded"
#[nativelink_test]
async fn present_cas_no_registry_consult() -> Result<(), Error> {
    let (ccs, cas_store, registry) = build_ccs_with_registry(&[LIVE_EP]);

    // Put PRESENT_CAS into the CAS and into the registry.
    cas_store
        .update_oneshot(PRESENT_CAS, Bytes::from_static(b"12345678"))
        .await?;
    let store_id: Arc<str> = Arc::from("");
    registry.register_ac_pin(LIVE_EP, store_id, PRESENT_CAS);

    let ar = ProtoActionResult {
        output_files: vec![OutputFile {
            digest: Some(PRESENT_CAS.into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let ac_key = serialize_and_upload_message(
        &ar,
        ccs.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await?;

    // get_part must succeed (CAS hit, no rescue needed).
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("get_part must not hang")?;

    // Counter stays 0 — the registry was not consulted.
    assert_eq!(
        ccs.pending_registry_rescues_total(),
        0,
        "short-circuit breach: rescue counter must be 0 when CAS hit succeeded; \
         registry consult must only run on CAS miss",
    );

    Ok(())
}

// ─── Test (v): composite invariant — gate(delete) ⇒ consult-first ─────────────

/// (v) Composite invariant: delete-on-detection must NOT fire for a
/// registry-resident digest.
///
/// gate(delete) ⇒ consult-first:
///   1. CCS checks CAS → MISSING
///   2. CCS checks pending registry → PRESENT + endpoint LIVE → rescue
///   3. delete-on-detection MUST NOT fire
///
/// Mutation: comment out the registry consult in `get_and_verify_single` →
/// step 2 skips → step 3 fires → AC entry deleted →
/// second get_part returns NotFound →
/// assert "H4 rescue bypassed — pending blob deleted as dangling" fires.
///
/// Production composition: real CompletenessCheckingStore + real AcPinRegistry
/// + real liveness checker. No mocks.
#[nativelink_test]
#[tracing_test::traced_test]
async fn composite_invariant_no_delete_on_registry_rescue() -> Result<(), Error> {
    let (ccs, _cas_store, registry) = build_ccs_with_registry(&[LIVE_EP]);

    let store_id: Arc<str> = Arc::from("");
    registry.register_ac_pin(LIVE_EP, store_id, MISSING_CAS);

    let ac_key = write_dangling_ar(&ccs, MISSING_CAS).await?;

    // First get_part: rescue must fire.
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("first get must not hang — 5s deadlock detector");
    assert!(
        r1.is_ok(),
        "H4 rescue bypassed — pending blob reported missing instead of rescued; \
         first get_part must return Ok; got {:?}",
        r1.err(),
    );

    // Second get_part: entry must still be there (NOT deleted by step one).
    let r2 = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("second get must not hang — 5s deadlock detector");
    assert!(
        r2.is_ok(),
        "H4 rescue bypassed — pending blob deleted as dangling; \
         second get_part returned {:?} (entry was incorrectly deleted after the first rescued get)",
        r2.err(),
    );

    // No incomplete warn must have fired.
    logs_assert(|lines: &[&str]| {
        let n = lines
            .iter()
            .filter(|l| l.contains(" WARN ") && l.contains("ActionResult incomplete"))
            .count();
        if n == 0 {
            Ok(())
        } else {
            Err(format!(
                "composite: incomplete warn fired {n} time(s) on registry-rescue path — \
                 delete-on-detection bypassed the consult"
            ))
        }
    });

    Ok(())
}

// ─── Test (vi): CAS-plane boundary — registry does NOT affect CAS has() ───────

/// (vi) The pending registry is an AC-completeness gate ONLY.
///
/// A registry-resident digest that is ABSENT from the CAS store must NOT make
/// the CAS store's own `has_with_results` return `Some`. The rescue only
/// changes what CCS reports for the AC entry's completeness; it must never
/// bleed into the raw CAS has() path used by bytestream_server and
/// batch_update_blobs for upload dedup.
///
/// Mutation: wire the registry consult into the CAS store's has_with_results →
/// the registry intercepts the CAS-plane query → test red-fails:
///   "registry leaked into CAS-plane has() — upload short-circuit trap"
///
/// SCOPE NOTE: this test verifies construction-isolation of THIS fixture (the
/// CAS store is a plain MemoryStore with no registry reference). It cannot
/// detect an independent Arc-clone of the registry consulted by WPS /
/// bytestream / batch-update code paths; that constraint is enforced by review
/// + the phase-1 structural guard (separate registry instance, not the CAS
/// locality_map), not by this test.
#[nativelink_test]
async fn registry_does_not_affect_cas_has() -> Result<(), Error> {
    let (ccs, cas_store, registry) = build_ccs_with_registry(&[LIVE_EP]);
    let cas_store_handle = Store::new(cas_store.clone());

    // Register MISSING_CAS in the pending registry.
    let store_id: Arc<str> = Arc::from("");
    registry.register_ac_pin(LIVE_EP, store_id, MISSING_CAS);

    // The CAS store itself must report MISSING (registry is invisible here).
    let mut cas_result = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        cas_store_handle.has_with_results(
            &[StoreKey::from(MISSING_CAS)],
            &mut cas_result,
        ),
    )
    .await
    .expect("CAS has() must not hang")?;

    assert!(
        cas_result[0].is_none(),
        "registry leaked into CAS-plane has() — upload short-circuit trap: \
         pending registry must not influence the CAS store's has_with_results; \
         CAS store reported the digest as present via registry lookup",
    );

    // CCS's rescue also must not bleed into the CAS-plane: if we ask CCS
    // itself via has_with_results for the CAS digest key (not an AC key),
    // the result must be None — CCS only rescues AC entries (via the
    // completeness gate), not raw CAS blobs.
    //
    // This tests the SHORT-CIRCUIT BOUNDARY invariant stated in §3b:
    // "the consult must live ONLY in CCS's completeness logic — NOT in any
    //  general has_with_results that bytestream/batch-update upload-dedup
    //  consults."
    //
    // CCS wraps the AC store; querying it with a CAS key (not an AC key)
    // just asks the ac_store.has_with_results, which returns None (the CAS
    // digest isn't in the AC store). No rescue fires. Proof: the ac_store
    // is a fresh MemoryStore with only AR entries, not raw CAS blobs.
    let ccs_store = Store::new(ccs.clone());
    let mut ccs_raw_result = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs_store.has_with_results(
            &[StoreKey::from(MISSING_CAS)],
            &mut ccs_raw_result,
        ),
    )
    .await
    .expect("CCS has() for CAS key must not hang")?;
    // CCS ac_store has no entry for MISSING_CAS as an AC key → None.
    // If the result were Some, it would mean the registry leaked directly
    // into has_with_results before the AC decode step, which would be
    // the upload-short-circuit trap.
    assert!(
        ccs_raw_result[0].is_none(),
        "registry leaked into CAS-plane has() — upload short-circuit trap: \
         CCS's has_with_results returned Some for a raw CAS digest key that \
         exists in the registry but NOT in the AC store",
    );

    Ok(())
}

// ─── Test: None registry (current behavior unaffected) ────────────────────────

/// When no registry is wired (None), CCS behaves exactly as before phase 3.
/// The incomplete branch fires; no counter; no rescue.
#[nativelink_test]
async fn no_registry_wired_current_behavior_unchanged() -> Result<(), Error> {
    let (ccs, _cas_store) = build_ccs_without_registry();

    let ac_key = write_dangling_ar(&ccs, MISSING_CAS).await?;

    let r = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("get_part must not hang")
    .expect_err("no-registry CCS must return NotFound for dangling AR");
    assert_eq!(r.code, Code::NotFound);

    Ok(())
}

// ─── Regression test: split ac/worker_api topology (#12 H4 cross-entry fix) ──

/// (#12 H4 cross-entry scoping fix) Verify that `inject_h4_pending_registry_into_ac_chains`
/// correctly injects the registry into a CCS even when the AC store name is
/// discovered in a separate pre-scan (simulating production's split topology:
/// AC on :50051, worker_api on :50061 — no single server entry has both).
///
/// The pre-existing per-loop scoping bug meant `liveness_checker = None` on the
/// AC entry (no worker_api on that entry) → CCS injection silently skipped.
/// This test verifies the extracted free function works and that the fix's call
/// site actually injects the registry by asserting the rescue fires.
///
/// # Mutation guidance
///
/// Comment out the `inject_h4_pending_registry_into_ac_chains` call in
/// `nativelink.rs`'s pre-loop block (or equivalently, skip the `walk` call
/// inside the function):
/// → test red-fails with:
///   "H4 wiring inert under split ac/worker_api topology — CCS registry not
///    injected; split-topology regression"
#[nativelink_test]
async fn split_topology_ac_ccs_receives_registry() -> Result<(), Error> {
    // Build a CCS (initially without registry) and register it in a StoreManager
    // under a store name that simulates the AC-entry's ac_store.
    let cas_store = MemoryStore::new(&MemorySpec::default());
    let ac_backend = MemoryStore::new(&MemorySpec::default());
    let ccs = CompletenessCheckingStore::new(
        Store::new(ac_backend),
        Store::new(cas_store),
    );

    const AC_STORE_NAME: &str = "split_topology_ac_store";
    let sm = StoreManager::new();
    sm.add_store(AC_STORE_NAME, Store::new(ccs.clone()));

    // ac_store_names mirrors the pre-pass scan: collected from ALL server entries.
    let mut ac_store_names = HashSet::new();
    ac_store_names.insert(AC_STORE_NAME.to_string());

    let registry = new_shared_ac_pin_registry();
    let live_set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(
        [LIVE_EP].iter().map(|s| s.to_string()).collect(),
    ));
    let live_set2 = live_set.clone();
    let checker: SharedLivenessChecker = Arc::new(move |ep: &str| live_set2.lock().contains(ep));

    // This is the call that the pre-loop block in nativelink.rs must make.
    // Mutation: comment this call out → the CCS gets no registry → rescue below fails.
    let injected =
        inject_h4_pending_registry_into_ac_chains(&ac_store_names, &sm, &registry, &checker);

    assert_eq!(
        injected, 1,
        "H4 wiring inert under split ac/worker_api topology — \
         inject_h4_pending_registry_into_ac_chains returned {injected}; expected 1; \
         split-topology regression",
    );

    // Verify the injection actually works: register MISSING_CAS under the live
    // endpoint and write a dangling AR. The rescue must fire.
    let store_id: Arc<str> = Arc::from("");
    registry.register_ac_pin(LIVE_EP, store_id, MISSING_CAS);

    let ac_key = write_dangling_ar(&ccs, MISSING_CAS).await?;

    let r = tokio::time::timeout(
        Duration::from_secs(5),
        ccs.get_part_unchunked(StoreKey::from(ac_key), 0, None),
    )
    .await
    .expect("get_part must not hang — 5s deadlock detector");
    assert!(
        r.is_ok(),
        "H4 wiring inert under split ac/worker_api topology — CCS registry not \
         injected; split-topology regression: get_part returned {:?}",
        r.err(),
    );

    assert_eq!(
        ccs.pending_registry_rescues_total(), 1,
        "H4 wiring inert under split ac/worker_api topology — rescue counter must \
         be 1 after successful injection + rescue; split-topology regression",
    );

    Ok(())
}
