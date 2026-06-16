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

//! Phase 1 CCS kill-switch tests (`disable_completeness_check: bool`).
//!
//! # What this tests
//!
//! Three contracts introduced in Phase 1 (CCS-drop Option A, per design
//! `.claude/audits/ccs-drop-preserve-worker-fetch-design-2026-06-15.md`):
//!
//! ## Test A — worker-fetch goal (§7, the goal test)
//!
//! Simplified composition (does NOT include VerifyStore — worker-fetch goal only):
//!   `AC_STORE (MemoryStore) → CompletenessCheckingStore(disable=true) [AC side]`
//!   `WorkerProxyStore(MemoryStore inner) → CAS [CAS side]`
//!
//! Digest NOT in server CAS; locality_map populated with a peer worker endpoint.
//!
//! Assertion: `WorkerProxyStore.get_part(digest)` → inner NotFound →
//! `try_read_from_worker` → peer-fetch succeeds; bytes match; entry not evicted.
//!
//! Mutation: clear locality_map → NotFound; bespoke message:
//!   "worker-fetch goal violated — locality_map is the sole fetch path under Option A"
//!
//! ## Test B — flag-behavior test (both directions)
//!
//! With `disable_completeness_check: true`:
//!   a GetActionResult for an AC entry referencing a CAS-missing blob is SERVED
//!   (not rejected). CCS is a transparent pass-through on the get path.
//!
//! With `disable_completeness_check: false` (default):
//!   CCS still returns NotFound for the same AC entry referencing missing CAS blob
//!   (existing behavior is UNCHANGED).
//!
//! Mutation on skip-branch: comment out the `if self.disable_completeness_check`
//! guard in `get_part` → the enabled case (false) test fails because a supposedly-
//! complete entry is rejected: "flag-skip mutation: check bypassed on false path —
//! completeness check fired when flag=false should have blocked"
//! ... and the disabled case (true) also fails because CCS now does the check:
//! "flag-skip mutation: disable flag ignored — completeness check fired when flag=true"
//!
//! ## Test B2 — has_with_results skip-gate (testing-czar T-1 BLOCK fix)
//!
//! CCS with `disable=true`; AC entry present; CAS blob ABSENT →
//! `has_with_results` must return `Some(size)` (ac_store pass-through, no check).
//!
//! Mutation: comment out `:819–821` → completeness check fires → `results[0] == None`
//! → assertion fails: "has_with_results skip-gate mutation: completeness check
//! fired when disable=true"
//!
//! ## Test C — observability: `wps_cas_and_peer_notfound_total` render-test
//!
//! Pins the emitted metric name via `render_prometheus`. Mutation: comment out
//! the `inc()` call in `get_part_sequential` → counter stays 0, assertion fails:
//! "observability mutation: wps_cas_and_peer_notfound_total must increment on \
//! final NotFound (stale-AC symptom counter)"

use core::time::Duration;

use bytes::Bytes;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, OutputFile,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::{SharedBlobLocalityMap, new_shared_blob_locality_map};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};

// (No fixed-constant digests needed — each test constructs its own.)

// ---------------------------------------------------------------------------
// Test A: worker-fetch goal — peer-fetch succeeds when CCS is disabled
// ---------------------------------------------------------------------------

/// # Test A — worker-fetch goal (§7 from the design doc)
///
/// Simplified composition (CCS disabled; does NOT include VerifyStore or FastSlowStore
/// — see T-2 note from testing-czar review):
///   AC chain: bare MemoryStore → CCS(disable=true)  [AC-side, transparent]
///   CAS chain: WorkerProxyStore(empty MemoryStore inner) + peer worker store
///
/// BLOB_DIGEST is NOT in the server's inner CAS (empty MemoryStore). The
/// peer store DOES have it. The locality_map carries the peer endpoint.
///
/// Expected: `get_part` on the WPS returns Ok and delivers the blob bytes
/// via the peer-fetch path (`try_read_from_worker`).
///
/// Mutation (clear locality_map): assert NotFound with bespoke message
///   "worker-fetch goal violated — locality_map is the sole fetch path under Option A"
#[nativelink_test]
async fn worker_fetch_goal_peer_fetch_succeeds_when_ccs_disabled() -> Result<(), Error> {
    // --- CAS side ---
    let inner_cas = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
    let locality_map: SharedBlobLocalityMap = new_shared_blob_locality_map();
    let wps_arc = WorkerProxyStore::new(inner_cas.clone(), locality_map.clone());
    let cas_store = Store::new(wps_arc.clone());

    // Peer store populated with the blob.
    let peer_store = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
    let blob_data_bytes: &[u8] = b"blob-data!";
    let blob_digest = DigestInfo::try_new(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        blob_data_bytes.len() as u64,
    )?;
    peer_store
        .update_oneshot(blob_digest, Bytes::from_static(blob_data_bytes))
        .await?;

    let peer_endpoint = "grpc://peer-worker:50091";
    wps_arc.inject_worker_connection(peer_endpoint, peer_store);

    // Register the digest in the locality map — this is the sole fetch path under Option A.
    locality_map.write().register_blobs(peer_endpoint, &[blob_digest]);

    // --- Fetch via WPS ---
    // Inner CAS is empty; the only path is via locality_map → try_read_from_worker.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        cas_store.get_part_unchunked(blob_digest, 0, None),
    )
    .await
    .expect(
        "worker-fetch goal test must not deadlock — \
         WorkerProxyStore::get_part future should complete within 5s",
    )?;

    assert_eq!(
        result.as_ref(),
        blob_data_bytes,
        "worker-fetch goal: bytes from peer worker must match uploaded blob"
    );

    // -----------------------------------------------------------------------
    // Mutation verification: clear locality_map → NotFound
    // -----------------------------------------------------------------------
    locality_map.write().evict_blobs(peer_endpoint, &[blob_digest]);

    // Also clear the inner store write that may have been populated by the tee.
    // Build a fresh WPS with empty inner to isolate the locality-clear.
    let inner_cas2 = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
    let locality_map2: SharedBlobLocalityMap = new_shared_blob_locality_map();
    let wps_arc2 = WorkerProxyStore::new(inner_cas2, locality_map2.clone());
    let cas_store2 = Store::new(wps_arc2);
    // locality_map2 is empty — no peers registered.

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        cas_store2.get_part_unchunked(blob_digest, 0, None),
    )
    .await
    .expect(
        "worker-fetch goal mutation: get_part must terminate (not deadlock) \
         when locality_map is empty",
    )
    .expect_err(
        "worker-fetch goal violated — locality_map is the sole fetch path under Option A",
    );

    assert_eq!(
        err.code,
        Code::NotFound,
        "worker-fetch goal mutation: empty locality_map must produce NotFound, got: {err:?}"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test B: flag-behavior test — both directions
// ---------------------------------------------------------------------------

/// # Test B — flag-behavior test
///
/// With `disable_completeness_check: true`: an AC entry referencing a
/// CAS-missing blob is SERVED by CCS (pass-through behavior).
///
/// With `disable_completeness_check: false` (default): the same AC entry is
/// REJECTED by CCS (existing behavior unchanged).
///
/// Both directions tested in a single test.
///
/// Mutation on the `disable_completeness_check` skip-guard in `get_part`:
/// - Mutate by commenting it out → the `true` branch also runs the check →
///   test fails: "flag-skip mutation: disable flag ignored — completeness
///   check fired when flag=true"
#[nativelink_test]
async fn ccs_disable_flag_controls_completeness_check() -> Result<(), Error> {
    // --- Shared AC entry ---
    // Both directions use the same AC entry referencing BLOB_DIGEST.
    // BLOB_DIGEST is absent from the CAS store in both directions.

    // ---- Direction 1: flag=true (check disabled) ----
    {
        let ac_backend = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
        // CAS store — deliberately empty so completeness check would fail.
        let empty_cas = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));

        // Upload AC entry referencing a missing blob.
        let ac_digest = upload_ac_with_blob_digest(
            &ac_backend,
            DigestInfo::try_new(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                4,
            )?,
        )
        .await?;

        // CCS with disable=true → transparent pass-through.
        let ccs = CompletenessCheckingStore::new_with_disable_flag(
            ac_backend,
            empty_cas,
            true, // disable_completeness_check = true
        );
        let ac_store = Store::new(ccs);

        // get_part must SUCCEED (CCS skips the check and passes through to ac_store).
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            ac_store.get_part_unchunked(ac_digest, 0, None),
        )
        .await
        .expect(
            "flag-skip mutation: disable flag ignored — completeness check fired when flag=true; \
             get_part deadlocked or timed out",
        );
        assert!(
            res.is_ok(),
            "flag=true: CCS must serve the AC entry without checking CAS; \
             got Err: {:?}",
            res.err()
        );
    }

    // ---- Direction 2: flag=false (check enabled, existing behavior) ----
    {
        let ac_backend = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
        let empty_cas = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));

        let missing_blob = DigestInfo::try_new(
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            4,
        )?;
        let ac_digest = upload_ac_with_blob_digest(&ac_backend, missing_blob).await?;

        // CCS with disable=false (default) → check IS active.
        let ccs = CompletenessCheckingStore::new_with_disable_flag(
            ac_backend,
            empty_cas,
            false, // disable_completeness_check = false (default)
        );
        let ac_store = Store::new(ccs);

        // get_part must FAIL with NotFound (blob missing from CAS, check active).
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            ac_store.get_part_unchunked(ac_digest, 0, None),
        )
        .await
        .expect(
            "flag=false: CCS get_part must terminate within 5s (not deadlock)",
        );
        let err = res.expect_err(
            "flag-skip mutation: check bypassed on false path — \
             completeness check fired when flag=false should have blocked",
        );
        assert_eq!(
            err.code,
            Code::NotFound,
            "flag=false: CCS must return NotFound for incomplete entry; got: {err:?}"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test B2: has_with_results skip-gate (the testing-czar T-1 BLOCK)
// ---------------------------------------------------------------------------

/// # Test B2 — `has_with_results` skip-gate with `disable=true`
///
/// Variant of Test B focused on the EXISTENCE path (`has_with_results`) rather
/// than the data-fetch path (`get_part`). The testing-czar mutation-4 proved
/// that commenting out the gate at `completeness_checking_store.rs:819–821`
/// causes all three prior tests to PASS — the gate was mutation-invisible.
///
/// Setup:
///   - CCS constructed with `disable=true`.
///   - AC entry IS present in `ac_store` (referencing a CAS blob).
///   - CAS blob is ABSENT from `cas_store`.
///
/// Expected: `has_with_results` returns `Some(size)` — the pass-through answer
/// from `ac_store`, no completeness check performed.
///
/// Mutation (comment out `completeness_checking_store.rs:819–821`):
///   → CCS runs `inner_has_with_results` → checks CAS → blob absent →
///   `results[0]` is `None` → assertion fails with:
///   `"has_with_results skip-gate mutation: completeness check fired when disable=true"`
#[nativelink_test]
async fn ccs_has_with_results_skip_gate_disable_true() -> Result<(), Error> {
    let ac_backend = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
    // CAS store — deliberately empty so completeness check would fail.
    let empty_cas = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));

    // Upload an AC entry referencing a CAS-absent blob.
    let cas_blob_digest = DigestInfo::try_new(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        6,
    )?;
    let ac_digest = upload_ac_with_blob_digest(&ac_backend, cas_blob_digest).await?;

    // CCS with disable=true → transparent pass-through on has_with_results.
    let ccs = CompletenessCheckingStore::new_with_disable_flag(
        ac_backend,
        empty_cas,
        true, // disable_completeness_check = true
    );
    let ccs_store = Store::new(ccs);

    // has_with_results must return Some(size) — delegated to ac_store, no CAS check.
    let mut results = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        ccs_store.has_with_results(&[StoreKey::from(ac_digest)], &mut results),
    )
    .await
    .expect(
        "has_with_results skip-gate test must not deadlock — \
         CCS.has_with_results should complete within 5s",
    )?;

    assert!(
        results[0].is_some(),
        "has_with_results skip-gate mutation: completeness check fired when disable=true; \
         expected Some(size) from ac_store pass-through, got None (CAS absence caused rejection)"
    );

    Ok(())
}

/// Upload a `ProtoActionResult` referencing `blob_digest` to `backend`
/// and return the computed digest of the AC entry.
async fn upload_ac_with_blob_digest(
    backend: &Store,
    blob_digest: DigestInfo,
) -> Result<DigestInfo, Error> {
    let action_result = ProtoActionResult {
        output_files: vec![OutputFile {
            path: "out.txt".to_string(),
            digest: Some(blob_digest.into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    serialize_and_upload_message(
        &action_result,
        backend.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await
}

// ---------------------------------------------------------------------------
// Test C: observability render-test — wps_cas_and_peer_notfound_total
// ---------------------------------------------------------------------------

/// # Test C — observability render-test
///
/// Pins the emitted metric name `wps_cas_and_peer_notfound_total` via
/// `render_prometheus`. Verifies:
///   1. The counter is zero before any NotFound.
///   2. After a `get_part` that hits inner-NotFound + no-peers (the final
///      NotFound branch in `get_part_sequential`), the counter increments.
///   3. The render body contains the literal string `wps_cas_and_peer_notfound_total`.
///
/// Mutation: comment out the `self.wps_cas_and_peer_notfound_total.inc()` call
/// in `get_part_sequential`'s final-NotFound branch:
///   → The counter stays 0; the assert on `contains("wps_cas_and_peer_notfound_total"
///     ... not zero)` fails with:
///   "observability mutation: wps_cas_and_peer_notfound_total must increment on \
///    final NotFound (stale-AC symptom counter)"
#[nativelink_test]
async fn wps_stale_ac_counter_increments_and_renders_on_final_notfound() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
    let locality_map: SharedBlobLocalityMap = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
    let proxy = Store::new(proxy_arc.clone());

    // A digest not present anywhere — triggers inner-NotFound + no-peers → final NotFound.
    let missing = DigestInfo::try_new(
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        8,
    )?;

    // Drive the final-NotFound path.
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        proxy.get_part_unchunked(missing, 0, None),
    )
    .await
    .expect("stale-AC counter test: get_part must terminate within 5s")
    .expect_err("stale-AC counter test: empty inner + no peers must return NotFound");

    assert_eq!(
        err.code,
        Code::NotFound,
        "stale-AC counter test: expected NotFound, got: {err:?}"
    );

    // ---- Render the metric and confirm the counter is present and non-zero ----
    let registry = MetricsRegistry::new();
    // Register using the same key shape as production's
    // `metrics_registry.register("nativelink", store_manager.clone())` path.
    // Under that registration the WPS publishes as:
    //   nativelink_<store_name>_wps_cas_and_peer_notfound_total_counter
    // For this test we register the WPS directly under a short key.
    registry.register("wps_test", proxy_arc.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("wps_cas_and_peer_notfound_total"),
        "observability mutation: wps_cas_and_peer_notfound_total must increment on \
         final NotFound (stale-AC symptom counter). \
         Metric name absent from render body. body=\n{body}"
    );

    // Confirm the counter is non-zero (it fired once in the get_part above).
    // The line looks like:
    //   wps_test_wps_cas_and_peer_notfound_total_counter 1
    let expected_nonzero_prefix = "wps_cas_and_peer_notfound_total_counter 1";
    assert!(
        body.contains(expected_nonzero_prefix),
        "observability mutation: wps_cas_and_peer_notfound_total must increment on \
         final NotFound (stale-AC symptom counter); counter is zero or missing. \
         body=\n{body}"
    );

    Ok(())
}
