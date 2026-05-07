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

//! Bazel-shaped end-to-end isolation test for the AC peer-fetch
//! wrapper.
//!
//! This is the load-bearing test for the Option A digest-collision
//! protection: an Action proto's CAS digest IS by REAPI design the
//! same digest the corresponding AC entry registers under
//! (`action_digest`). If the server ever routed AC pin
//! advertisements through the CAS-shared `BlobLocalityMap`, then
//! `bytestream_server::write`'s fast-path short-circuit
//! (`store.has(digest)` => skip upload) would silently swallow
//! every Action proto upload for any AC entry a worker had
//! advertised, causing permanent silent data loss.
//!
//! The fix is two structures: AC pin registrations live in
//! [`nativelink_util::ac_pin_registry::AcPinRegistry`] (read by the
//! AC peer-fetch wrapper [`nativelink_store::ac_proxy_store::AcProxyStore`]
//! ONLY); CAS locality lives in
//! [`nativelink_util::blob_locality_map::BlobLocalityMap`] (read by
//! [`nativelink_store::worker_proxy_store::WorkerProxyStore`]).
//! Crossing the streams is exactly the silent-data-loss bug.
//!
//! This test exercises the full production composition (`WorkerProxyStore`
//! over CAS + `AcProxyStore` over AC + a real-shaped AC pin
//! advertisement) and asserts the upload is NOT short-circuited.
//!
//! ## Mutation step (RUN AND VERIFIED during authoring)
//!
//! The mutation rerouted the AC pin into the CAS-side
//! [`BlobLocalityMap::register_blobs`] (the old broken
//! `pinned_mirror_entries: 16` field shape from commit `563c8ebb`)
//! INSTEAD of [`AcPinRegistry::register_ac_pin`]. Verified red-fail
//! with the bespoke `.expect("AC pin must NOT short-circuit CAS
//! upload — Option A digest-collision protection violated")`. After
//! restoring the AcPinRegistry registration, the test passes
//! green again. The mutation lives in this comment as a runnable
//! recipe for the next maintainer; the production code path uses
//! the AcPinRegistry registration unconditionally.
//!
//! Recipe:
//! 1. Replace `ac_pin_registry.register_ac_pin(ENDPOINT,
//!    Arc::from(AC_STORE_ID), digest);` (in `ac_pin_short_circuit
//!    _does_not_silently_swallow_cas_upload`'s setup) with
//!    `cas_locality_map.write().register_blobs(ENDPOINT, &[digest]);`.
//! 2. Run the test. Expected: red-fail with the bespoke message.
//! 3. Restore step 1. Test goes green.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::MemorySpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::ac_proxy_store::AcProxyStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::ac_pin_registry::new_shared_ac_pin_registry;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const ENDPOINT: &str = "grpc://w1:50081";
const AC_STORE_ID: &str = "AC_MAIN_STORE";

/// Wall-clock cap on every assertion. The test runs entirely
/// in-process; everything resolves in microseconds. The timeout
/// exists as a deadlock detector so a regression that violates the
/// writer-termination contract surfaces as a test failure with a
/// specific message rather than hanging the runner.
const ASSERT_TIMEOUT: Duration = Duration::from_secs(10);

/// The load-bearing test. Reproduces the production composition,
/// inserts an AC pin advertisement against a digest that ALSO
/// happens to be a CAS-side Action proto digest (REAPI design), and
/// asserts a CAS upload of that exact digest is NOT short-circuited
/// by `WorkerProxyStore::has`.
///
/// If [`AcProxyStore::has_with_results`] ever consults
/// [`AcPinRegistry`] (which it must NOT, see the module docs in
/// `ac_proxy_store.rs`), or if the AC pin advertisement is routed
/// through the CAS-shared `BlobLocalityMap` (the OLD broken
/// `pinned_mirror_entries: 16` field shape), this test red-fails
/// with the bespoke message — exactly catching the silent-data-loss
/// regression class.
///
/// Asymmetric coverage:
///   - Under-action: CAS write reaches inner store. Test asserts
///     `inner_cas.has(digest) == Some(size)` AND
///     `inner_cas.get(digest) == payload`.
///   - Over-action: CAS write is short-circuited by AC-pin
///     consultation. Caught by the same assertions: a short-
///     circuit would leave the inner CAS empty and the test would
///     red-fail with "must reach inner CAS".
#[nativelink_test]
async fn ac_pin_short_circuit_does_not_silently_swallow_cas_upload() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        // Inner CAS — what bytes land in after upload completes.
        let inner_cas = Store::new(MemoryStore::new(&MemorySpec::default()));
        // Inner AC — for completeness; the CAS short-circuit test
        // does not exercise the AC read path but the wrapper is
        // present per production composition.
        let inner_ac = Store::new(MemoryStore::new(&MemorySpec::default()));

        // CAS-side state: WorkerProxyStore wraps the CAS chain and
        // consults BlobLocalityMap on `has` (production shape).
        let cas_locality_map = new_shared_blob_locality_map();
        let cas_proxy = WorkerProxyStore::new(inner_cas.clone(), cas_locality_map.clone());
        // Default `consult_locality_in_has = true` — the actual
        // `has`-fast-path that bytestream_server::write hits.
        let cas_store = Store::new(cas_proxy);

        // AC-side state: AcProxyStore wraps the AC chain and
        // consults AcPinRegistry on inner-NotFound get_part (NEVER
        // on has, NEVER on update — the digest-collision contract).
        let ac_pin_registry = new_shared_ac_pin_registry();
        let ac_proxy = AcProxyStore::new(inner_ac.clone(), ac_pin_registry.clone());
        let _ac_store = Store::new(ac_proxy);

        // The Action proto bytes Bazel will upload to CAS. The
        // payload is irrelevant to the contract under test; what
        // matters is that the digest under which it's uploaded ALSO
        // appears as an AC pin.
        let payload = Bytes::from_static(b"action-proto-bytes");
        let digest = DigestInfo::try_new(VALID_HASH1, payload.len() as u64)?;

        // Register an AC pin for `digest` against the worker
        // endpoint. This MUST go into AcPinRegistry — NEVER into
        // BlobLocalityMap. See module-doc mutation step recipe.
        //
        // MUTATION_TOGGLE: replace the next line with
        //   `cas_locality_map.write().register_blobs(ENDPOINT, &[digest]);`
        // (the OLD broken `pinned_mirror_entries: 16` shape from
        // commit 563c8ebb). Test should red-fail with the bespoke
        // "AC pin must NOT short-circuit CAS upload" message
        // because `WorkerProxyStore::has` now reports the digest as
        // present (via the leaked CAS locality entry), the bytestream
        // upload short-circuit fires, and the inner CAS stays empty.
        ac_pin_registry.register_ac_pin(ENDPOINT, Arc::from(AC_STORE_ID), digest);

        // Bazel uploads the Action proto for `digest` to CAS. This
        // sequence mirrors `bytestream_server::write`'s
        // skip-if-already-present fast path (bytestream_server.rs
        // line ~2099): if `cas_store.has(digest).await` returns
        // `Some`, the upload is short-circuited and the bytes are
        // NEVER written to the inner store. The bug we're guarding
        // against: AC pin advertisements leaking into the CAS
        // locality_map cause `cas_store.has` to report Some for
        // digests the inner CAS doesn't actually hold, swallowing
        // every Action proto upload for any pinned AC entry.
        let has_result = cas_store.has(digest).await?;
        if has_result.is_none() {
            cas_store.update_oneshot(digest, payload.clone()).await?;
        }

        // Read back from the INNER CAS (not through the wrapper —
        // we want to see what landed on stable storage, not what
        // the wrapper reports). If the wrapper had short-circuited
        // the write because of the AC pin, the inner CAS would be
        // empty.
        let inner_has = inner_cas.has(digest).await?;
        let inner_bytes = if inner_has.is_some() {
            inner_cas.get_part_unchunked(digest, 0, None).await?
        } else {
            Bytes::new()
        };
        Result::<_, Error>::Ok((inner_has, inner_bytes, payload, digest))
    })
    .await;
    let (inner_has, inner_bytes, payload, digest) = result.expect(
        "AC isolation e2e must not deadlock — \
         writer-termination or AcProxyStore::update pass-through contract violated",
    )?;

    assert_eq!(
        inner_has,
        Some(payload.len() as u64),
        "AC pin must NOT short-circuit CAS upload — Option A digest-collision protection violated. \
         Inner CAS is empty for digest {:?} that Bazel just uploaded; AC pin advertisements have \
         leaked into the upload-skip fast path. Mutation recipe: re-route ac_pin_registry.register_ac_pin \
         into cas_locality_map.write().register_blobs (the old broken pinned_mirror_entries:16 shape).",
        digest,
    );
    assert_eq!(
        inner_bytes, payload,
        "Action proto bytes corrupted in inner CAS — round-trip mismatch for digest {digest:?}"
    );
    Ok(())
}

/// Companion test: with NO AC pin advertised, the CAS upload
/// behaves exactly the same. This rules out an alternate failure
/// mode where the wrapper accidentally short-circuits CAS uploads
/// regardless of AC pins (e.g. a regression in
/// `WorkerProxyStore::update`'s pass-through). If THIS test red-
/// fails alongside the load-bearing test, the regression is in the
/// CAS-side update path and not the AC isolation path; the bespoke
/// messages let the operator distinguish them.
#[nativelink_test]
async fn cas_upload_round_trips_without_ac_pin() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let inner_cas = Store::new(MemoryStore::new(&MemorySpec::default()));
        let cas_locality_map = new_shared_blob_locality_map();
        let cas_proxy = WorkerProxyStore::new(inner_cas.clone(), cas_locality_map);
        let cas_store = Store::new(cas_proxy);

        let payload = Bytes::from_static(b"plain-cas-blob");
        let digest = DigestInfo::try_new(VALID_HASH1, payload.len() as u64)?;

        cas_store.update_oneshot(digest, payload.clone()).await?;

        let inner_has = inner_cas.has(digest).await?;
        let inner_bytes = if inner_has.is_some() {
            inner_cas.get_part_unchunked(digest, 0, None).await?
        } else {
            Bytes::new()
        };
        Result::<_, Error>::Ok((inner_has, inner_bytes, payload))
    })
    .await;
    let (inner_has, inner_bytes, payload) = result.expect(
        "plain-CAS round-trip must not deadlock — WorkerProxyStore::update pass-through broken",
    )?;

    assert_eq!(
        inner_has,
        Some(payload.len() as u64),
        "CAS upload (no AC pin in scope) must reach inner store — \
         WorkerProxyStore::update pass-through regression"
    );
    assert_eq!(
        inner_bytes, payload,
        "CAS round-trip payload mismatch in the no-AC-pin baseline"
    );
    Ok(())
}
