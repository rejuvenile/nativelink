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

//! Integration tests for `AcServer`'s `pending_output_locality_registry`
//! registration path (#12 H4 invariant — phase 2/3).
//!
//! # What this tests
//!
//! When a worker sends `UpdateActionResult` with a non-empty `cas_endpoint`
//! AND the `x-nativelink-worker` header set, the server's AC handler MUST:
//!
//!   (a) Validate the claimed `cas_endpoint` against the live worker set.
//!   (b) Collect the `ActionResult`'s referenced output digests.
//!   (c) Register each digest → `cas_endpoint` in `pending_output_locality_registry`.
//!   (d) Increment `pending_output_registrations_total` by 1.
//!
//! The registration MUST fire BEFORE the AC entry is committed (structural
//! ordering via the RPC handler's synchronous pre-commit sequence).
//!
//! # Mutation guidance
//!
//! - Skip the registration call → test (i) red-fails with bespoke
//!   "registration-skip: digest not in pending registry after worker UAR".
//! - Skip the counter increment → test (ii) counter==0 assertion fires.
//! - Remove the 256-byte length guard → test (iii) large-endpoint entry
//!   would appear in registry (but the AC update succeeds regardless).
//! - Remove liveness check → test (iv) dead-endpoint entry appears in registry.
//! - Skip BIS drain → test (v) stale entry survives BIS-ack.

use core::time::Duration;
use std::collections::HashSet;
use std::sync::Arc;

use nativelink_config::cas_server::WithInstanceName;
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_server::ActionCache;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, OutputFile, UpdateActionResultRequest, digest_function,
};
use nativelink_service::ac_server::{AcServer, PENDING_STORE_ID, SharedLivenessChecker};
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::ac_pin_registry::{SharedAcPinRegistry, new_shared_ac_pin_registry};
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use tonic::metadata::MetadataValue;
use tonic::Request;

const INSTANCE_NAME: &str = "h4_test";
const FILE_HASH: &str = "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd";
const FILE_SIZE: i64 = 512;
const ACTION_HASH: &str = "1122334411223344112233441122334411223344112233441122334411223344";
const ACTION_SIZE: i64 = 10;
const LIVE_ENDPOINT: &str = "grpc://worker1.local:50081";
const DEAD_ENDPOINT: &str = "grpc://worker99.local:50081";

fn make_live_checker(live: &[&str]) -> (SharedLivenessChecker, Arc<Mutex<HashSet<String>>>) {
    let set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(
        live.iter().map(|s| (*s).to_string()).collect(),
    ));
    let set2 = set.clone();
    let checker: SharedLivenessChecker = Arc::new(move |ep: &str| set2.lock().contains(ep));
    (checker, set)
}

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let sm = Arc::new(StoreManager::new());
    sm.add_store(
        "main_ac",
        store_factory(&StoreSpec::Memory(MemorySpec::default()), &sm, None).await?,
    );
    Ok(sm)
}

fn make_digest(hash: &str, size: i64) -> Digest {
    Digest {
        hash: hash.to_string(),
        size_bytes: size,
    }
}

fn action_result_with_output_file(file_hash: &str, file_size: i64) -> ActionResult {
    ActionResult {
        output_files: vec![OutputFile {
            path: "out/foo.o".to_string(),
            digest: Some(make_digest(file_hash, file_size)),
            is_executable: false,
            contents: vec![].into(),
            node_properties: None,
        }],
        ..Default::default()
    }
}

/// Build a worker-plane UpdateActionResult request (with x-nativelink-worker header).
fn worker_uar_request(
    cas_endpoint: &str,
    action_result: ActionResult,
) -> Request<UpdateActionResultRequest> {
    let mut req = Request::new(UpdateActionResultRequest {
        instance_name: INSTANCE_NAME.to_string(),
        action_digest: Some(make_digest(ACTION_HASH, ACTION_SIZE)),
        action_result: Some(action_result),
        results_cache_policy: None,
        digest_function: digest_function::Value::Sha256.into(),
        cas_endpoint: cas_endpoint.to_string(),
    });
    req.metadata_mut().insert(
        "x-nativelink-worker",
        MetadataValue::from_static("1"),
    );
    req
}

// ---------------------------------------------------------------------------
// Test (i): live cas_endpoint → registry contains output digests + counter==1
//
// Mutation: comment out `registry.register_ac_pin(...)` in ac_server's
// UpdateActionResult handler → the `assert!` on `registry.snapshot_endpoint`
// fires with "registration-skip: digest not in pending registry after
// worker UAR with live endpoint".
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn live_endpoint_registers_output_digests_and_increments_counter() -> Result<(), Error> {
    let sm = make_store_manager().await?;
    let registry = new_shared_ac_pin_registry();
    let (checker, _live_set) = make_live_checker(&[LIVE_ENDPOINT]);

    let ac_server = AcServer::new_with_pending_registry(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::AcStoreConfig {
                ac_store: "main_ac".to_string(),
                read_only: false,
            },
        }],
        &sm,
        Some(registry.clone()),
        Some(checker),
    )?;

    let ar = action_result_with_output_file(FILE_HASH, FILE_SIZE);
    let req = worker_uar_request(LIVE_ENDPOINT, ar);
    let resp = ac_server.update_action_result(req).await;
    assert!(
        resp.is_ok(),
        "AC update with live endpoint must succeed, got: {resp:?}"
    );

    // Registry must contain the output file digest.
    let snap = tokio::time::timeout(Duration::from_secs(5), async {
        registry.snapshot_endpoint(LIVE_ENDPOINT)
    })
    .await
    .expect("must not deadlock");

    assert!(
        snap.is_some(),
        "registration-skip: digest not in pending registry after worker UAR with live endpoint"
    );
    let snap = snap.unwrap();
    let registered_digests: Vec<DigestInfo> = snap.into_iter().map(|(_, d)| d).collect();
    let expected =
        DigestInfo::try_new(FILE_HASH, FILE_SIZE).expect("valid digest");
    assert!(
        registered_digests.contains(&expected),
        "output file digest {expected:?} must be registered; found {:?}",
        registered_digests
    );

    // Counter must be 1.
    let count = ac_server.pending_output_registrations_total();
    assert_eq!(
        count, 1,
        "counter-skip: pending_output_registrations_total must be 1 after one live registration"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test (ii): empty cas_endpoint → no registration, counter unchanged (over-action)
//
// Tests the over-action direction: the handler MUST NOT register when
// cas_endpoint is empty. Counter must remain 0.
//
// Mutation: remove the `if cas_endpoint.is_empty() { return; }` guard in
// register_output_locality → the endpoint "" would be passed to the liveness
// checker (which returns false for ""), causing the liveness-check warn branch
// to fire. If liveness is also bypassed, the registry receives the empty-string
// key and `snapshot_endpoint("")` returns Some, triggering:
//   "over-action: registry must not contain entries when cas_endpoint is empty"
// and the counter assert fires:
//   "over-action: counter must remain 0 when cas_endpoint is empty"
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn empty_cas_endpoint_no_registration_counter_unchanged() -> Result<(), Error> {
    let sm = make_store_manager().await?;
    let registry = new_shared_ac_pin_registry();
    let (checker, _live_set) = make_live_checker(&[LIVE_ENDPOINT]);

    let ac_server = AcServer::new_with_pending_registry(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::AcStoreConfig {
                ac_store: "main_ac".to_string(),
                read_only: false,
            },
        }],
        &sm,
        Some(registry.clone()),
        Some(checker),
    )?;

    let ar = action_result_with_output_file(FILE_HASH, FILE_SIZE);
    // Empty cas_endpoint — must not register.
    let req = worker_uar_request("", ar);
    let resp = ac_server.update_action_result(req).await;
    assert!(resp.is_ok(), "AC update with empty endpoint must succeed");

    // No entries should appear in registry.
    let snap = registry.snapshot_endpoint(LIVE_ENDPOINT);
    assert!(
        snap.is_none(),
        "over-action: registry must not contain entries when cas_endpoint is empty; \
         found {:?} entries",
        snap.as_ref().map(Vec::len)
    );

    let count = ac_server.pending_output_registrations_total();
    assert_eq!(
        count, 0,
        "over-action: counter must remain 0 when cas_endpoint is empty"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test (iii): oversized endpoint (>256 bytes) → warn + ignored, AC update succeeds
//
// Revision 5 item (c): oversized endpoint MUST NOT cause the AC update to
// fail. The endpoint MUST be silently ignored (no registration).
//
// Mutation: remove the length guard → registry.snapshot_endpoint(huge_ep) is
// Some (the entry appeared), but the key assertion here is that the AC
// update itself returns Ok.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn oversized_endpoint_ignored_ac_update_succeeds() -> Result<(), Error> {
    let sm = make_store_manager().await?;
    let registry = new_shared_ac_pin_registry();
    let huge_endpoint = "grpc://".to_string() + &"x".repeat(300) + ":50081";
    let (checker, live_set) = make_live_checker(&[]);
    // Make the huge endpoint "live" so liveness doesn't discard it first.
    live_set.lock().insert(huge_endpoint.clone());

    let ac_server = AcServer::new_with_pending_registry(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::AcStoreConfig {
                ac_store: "main_ac".to_string(),
                read_only: false,
            },
        }],
        &sm,
        Some(registry.clone()),
        Some(checker),
    )?;

    let ar = action_result_with_output_file(FILE_HASH, FILE_SIZE);
    let req = worker_uar_request(&huge_endpoint, ar);
    let resp = ac_server.update_action_result(req).await;
    assert!(
        resp.is_ok(),
        "guard-skip: oversized endpoint must NOT cause AC update failure; got {resp:?}"
    );

    // Registry must NOT contain the oversized endpoint.
    let snap = registry.snapshot_endpoint(&huge_endpoint);
    assert!(
        snap.is_none(),
        "guard-skip: oversized endpoint MUST be ignored; found {:?} entries",
        snap.as_ref().map(Vec::len)
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test (iv): dead endpoint (not in liveness set) → not registered
//
// Mutation: remove the `if !checker(cas_endpoint) { ... return; }` liveness
// guard in register_output_locality → the dead endpoint bypasses the check
// and gets registered. snapshot_endpoint(DEAD_ENDPOINT) returns Some,
// triggering:
//   "liveness check skipped: dead endpoint registered in pending registry"
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn dead_endpoint_not_registered() -> Result<(), Error> {
    let sm = make_store_manager().await?;
    let registry = new_shared_ac_pin_registry();
    // Only LIVE_ENDPOINT is live; DEAD_ENDPOINT is not.
    let (checker, _live_set) = make_live_checker(&[LIVE_ENDPOINT]);

    let ac_server = AcServer::new_with_pending_registry(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::AcStoreConfig {
                ac_store: "main_ac".to_string(),
                read_only: false,
            },
        }],
        &sm,
        Some(registry.clone()),
        Some(checker),
    )?;

    let ar = action_result_with_output_file(FILE_HASH, FILE_SIZE);
    let req = worker_uar_request(DEAD_ENDPOINT, ar);
    let resp = ac_server.update_action_result(req).await;
    // The AC update itself must succeed — dead endpoint means no registration,
    // not an error.
    assert!(
        resp.is_ok(),
        "AC update with dead endpoint must succeed (no error); got {resp:?}"
    );

    let snap = registry.snapshot_endpoint(DEAD_ENDPOINT);
    assert!(
        snap.is_none(),
        "liveness check skipped: dead endpoint registered in pending registry; \
         found {:?} entries",
        snap.as_ref().map(Vec::len)
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test (v): no x-nativelink-worker header → not registered (Bazel-plane path)
//
// This guards the over-action direction: a Bazel-plane UpdateActionResult
// (no worker header) MUST NOT register even if cas_endpoint is set.
//
// Mutation: remove the `if is_worker { self.register_output_locality(...) }`
// gate in inner_update_action_result (make the call unconditional) →
// snapshot_endpoint(LIVE_ENDPOINT) returns Some, triggering:
//   "over-action: non-worker UAR must not register in pending registry"
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn no_worker_header_no_registration() -> Result<(), Error> {
    let sm = make_store_manager().await?;
    let registry = new_shared_ac_pin_registry();
    let (checker, _live_set) = make_live_checker(&[LIVE_ENDPOINT]);

    let ac_server = AcServer::new_with_pending_registry(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::AcStoreConfig {
                ac_store: "main_ac".to_string(),
                read_only: false,
            },
        }],
        &sm,
        Some(registry.clone()),
        Some(checker),
    )?;

    let ar = action_result_with_output_file(FILE_HASH, FILE_SIZE);
    // No x-nativelink-worker header.
    let req = Request::new(UpdateActionResultRequest {
        instance_name: INSTANCE_NAME.to_string(),
        action_digest: Some(make_digest(ACTION_HASH, ACTION_SIZE)),
        action_result: Some(ar),
        results_cache_policy: None,
        digest_function: digest_function::Value::Sha256.into(),
        cas_endpoint: LIVE_ENDPOINT.to_string(),
    });
    let resp = ac_server.update_action_result(req).await;
    assert!(resp.is_ok(), "AC update without worker header must succeed");

    let snap = registry.snapshot_endpoint(LIVE_ENDPOINT);
    assert!(
        snap.is_none(),
        "over-action: non-worker UAR must not register in pending registry; \
         found {:?} entries",
        snap.as_ref().map(Vec::len)
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test for BIS drain: live entry is removed after digest is drained
// (Revision 5 item a). This tests the AcPinRegistry drain mechanism
// `remove_digests_for_endpoint_batch` that the BIS loop calls.
//
// The actual BIS loop wiring is in nativelink.rs (integration-level).
// This test verifies the registry's remove API works correctly so the
// BIS-loop wiring can rely on it.
//
// Uses PENDING_STORE_ID (the production "" constant from ac_server.rs)
// so the test is structurally coupled to the actual drain store_id.
// A rename of PENDING_STORE_ID to a non-empty value breaks compilation
// here, forcing the test to be updated alongside the constant.
//
// Mutation: comment out the `remove_digests_for_endpoint_batch` call →
// the "pending entry survives drain — stale registry" assertion fires.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn bis_drain_removes_pending_entry() -> Result<(), Error> {
    // Direct unit test of the registry's drain API — the BIS loop calls this.
    // store_id must match PENDING_STORE_ID (="") — same value production uses.
    let registry: SharedAcPinRegistry = new_shared_ac_pin_registry();
    let store_id: Arc<str> = Arc::from(PENDING_STORE_ID);
    let digest = DigestInfo::try_new(FILE_HASH, FILE_SIZE).expect("valid digest");

    // Companion: assert the production constant is the empty string.
    assert_eq!(
        PENDING_STORE_ID, "",
        "PENDING_STORE_ID identity: production drain uses empty-string store_id; \
         update this test and ac_server.rs together if this changes"
    );

    registry.register_ac_pin(LIVE_ENDPOINT, store_id.clone(), digest);
    assert!(
        registry.snapshot_endpoint(LIVE_ENDPOINT).is_some(),
        "precondition: entry must be present before drain"
    );

    // BIS loop calls `remove_digests_for_endpoint_batch` with
    // [(store_id="", &[digest])].
    let drains = vec![(store_id, std::slice::from_ref(&digest))];
    registry.remove_digests_for_endpoint_batch(LIVE_ENDPOINT, &drains);

    let after = registry.snapshot_endpoint(LIVE_ENDPOINT);
    assert!(
        after.is_none() || after.as_ref().map_or(true, Vec::is_empty),
        "pending entry survives drain — stale registry: \
         remove_digests_for_endpoint_batch did not drain the entry; \
         remaining: {:?}",
        after
    );

    Ok(())
}
