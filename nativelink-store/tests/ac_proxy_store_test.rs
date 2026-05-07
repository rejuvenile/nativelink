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

//! Unit-level tests for [`nativelink_store::ac_proxy_store::AcProxyStore`].
//!
//! These tests inject an in-memory store as the "peer" via
//! `inject_worker_connection`, bypassing the lazy GrpcStore
//! construction. The AC peer-fetch contract is exercised end-to-end
//! through the wrapper's `StoreLike` surface (production-composition,
//! not direct method calls), with a `tokio::time::timeout` deadlock
//! detector around every assertion that depends on the wrapper
//! delivering bytes to a borrowed writer.

use core::pin::Pin;
use core::time::Duration;
use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::ac_proxy_store::AcProxyStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::ac_pin_registry::{
    SharedAcPinRegistry, new_shared_ac_pin_registry,
};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;

/// Test peer that always returns the configured `Code` for every
/// `get_part`. Used to simulate transient peer errors in
/// `ac_proxy_store_keeps_pin_on_transient_peer_unreachable` without
/// dialing a real network endpoint (which would chain real
/// `connect_timeout_s=5` retries that exceed the assertion timeout
/// and look like a deadlock to the test).
#[derive(MetricsComponent)]
struct AlwaysErrorPeer {
    code: Code,
}

impl core::fmt::Debug for AlwaysErrorPeer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AlwaysErrorPeer").field("code", &self.code).finish()
    }
}

#[async_trait]
impl StoreDriver for AlwaysErrorPeer {
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
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(self.code, "AlwaysErrorPeer: simulated transient"))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        let err = make_err!(self.code, "AlwaysErrorPeer: simulated transient");
        // Mirror the writer-termination contract — surface the error
        // on the writer too so any caller reading via `bind_buffered`
        // observes it.
        let _ = writer.send_error(err.clone());
        Err(err)
    }

    fn inner_store(&self, _: Option<StoreKey>) -> &dyn StoreDriver {
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
        _: Arc<dyn ItemCallback>,
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

#[async_trait]
impl HealthStatusIndicator for AlwaysErrorPeer {
    fn get_name(&self) -> &'static str {
        "AlwaysErrorPeer"
    }
    async fn check_health(&self, _namespace: Cow<'static, str>) -> HealthStatus {
        HealthStatus::new_ok(self, std::borrow::Cow::Borrowed("ok"))
    }
}

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const PEER_ENDPOINT: &str = "grpc://w1:50081";
const AC_STORE_ID: &str = "AC_MAIN_STORE";

/// Wall-clock cap on every test assertion. The wrapper SHOULD always
/// respond within hundreds of microseconds (everything is in-process
/// memory); the timeout exists as a deadlock detector. Failure mode
/// without it: a regression that drops the writer-termination
/// contract on an early-return path would hang the CI runner. With
/// the timeout the test red-fails with a bespoke message.
const ASSERT_TIMEOUT: Duration = Duration::from_secs(5);

/// Helper: build an `AcProxyStore` over a fresh `MemoryStore` and a
/// fresh `AcPinRegistry`. Returns (wrapper_as_Store, inner_handle,
/// registry, proxy_arc) so callers can drive each layer
/// independently.
fn make_proxy() -> (Store, Store, SharedAcPinRegistry, Arc<AcProxyStore>) {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let registry = new_shared_ac_pin_registry();
    let proxy_arc = AcProxyStore::new(inner.clone(), registry.clone());
    (Store::new(proxy_arc.clone()), inner, registry, proxy_arc)
}

// -------------------------------------------------------------------
// Test 1: writes pass through to inner unchanged.
//
// Spec: AC writes NEVER short-circuit on AC pins (digest-collision
// footgun: an AC entry's digest is also an Action proto's CAS
// digest; consulting AC pins on writes would weaponize CAS upload
// short-circuits at `bytestream_server::write` and silently drop
// Action proto bytes). Verified here by registering a fake AC pin
// for the digest BEFORE writing — the write must still hit the
// inner store, otherwise a regression that wired AC-pin presence
// into the upload path would be invisible at the wrapper boundary.
//
// Asymmetric coverage:
//   - Under-action: write skipped (silent data loss). This test
//     covers it.
//   - Over-action: write succeeds AND the inner store somehow
//     mutates beyond the digest under test. Out of scope here —
//     the inner is a MemoryStore with deterministic semantics; an
//     over-action regression would surface in the inner-store's
//     own composability test.
//
// Mutation step (run during authoring, documented here for the
// reviewer):
//   1. In `ac_proxy_store.rs::update`, replace the `inner.update`
//      call with `Ok(())`.
//   2. Run this test. Expected: fails with the bespoke
//      "AC write must reach inner store…" message because
//      `inner.has` returns None.
//   3. Restore the line.
// -------------------------------------------------------------------
#[nativelink_test]
async fn ac_proxy_store_passes_through_writes() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let (wrapper, inner, registry, _proxy) = make_proxy();
        let digest = DigestInfo::try_new(VALID_HASH1, 8)?;

        // Register a fake AC pin BEFORE writing. If the wrapper
        // ever consults the registry on write to short-circuit,
        // this entry would silently swallow the upload — exactly
        // the regression we're guarding against.
        registry.register_ac_pin(PEER_ENDPOINT, Arc::from(AC_STORE_ID), digest);

        let payload = Bytes::from_static(b"action-r");
        wrapper.update_oneshot(digest, payload.clone()).await?;

        let inner_present = inner.has(digest).await?;
        Result::<_, Error>::Ok((digest, payload, inner_present))
    })
    .await;
    let (digest, payload, inner_present) = result.expect(
        "AC write through wrapper must not deadlock — \
         writer-termination or update-pass-through contract violated",
    )?;

    assert_eq!(
        inner_present,
        Some(payload.len() as u64),
        "AC write must reach inner store (expected size {} for digest {:?}) \
         — wrapper must NOT short-circuit on AC-pin presence",
        payload.len(),
        digest,
    );
    Ok(())
}

// -------------------------------------------------------------------
// Test 2: redirects to peer on inner-NotFound.
//
// Spec: when the inner AC store returns NotFound and the digest is
// registered against a worker endpoint in the AC pin registry, the
// wrapper opens a connection to that endpoint and streams the AC
// entry from the peer.
//
// We exercise this by injecting a MemoryStore as the "peer
// connection" and seeding it with the AC entry. The injection
// side-steps the lazy GrpcStore construction (which requires a real
// network endpoint) and lets the wrapper consult its
// worker_connections HashMap as if the lazy connect had completed.
//
// Asymmetric coverage:
//   - Under-action (covered): wrapper fails to redirect — get
//     returns NotFound when peer holds the bytes.
//   - Over-action: wrapper redirects when caller doesn't expect
//     it. Since redirect is unconditional on inner-NotFound, the
//     "doesn't expect" condition isn't a thing here — but we
//     verify in test 3 that the wrapper does NOT redirect when no
//     peer is registered.
//
// Mutation step:
//   1. In `ac_proxy_store.rs::get_part`, on inner NotFound replace
//      the `try_read_from_peer` call with
//      `return Err(make_err!(Code::NotFound, "..."))`.
//   2. Run this test. Expected: fails with the bespoke
//      "wrapper must deliver peer bytes…" message because
//      `get_part_unchunked` returns NotFound.
//   3. Restore the line.
// -------------------------------------------------------------------
#[nativelink_test]
async fn ac_proxy_store_redirects_to_peer_on_inner_notfound() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let (wrapper, _inner, registry, proxy) = make_proxy();
        let digest = DigestInfo::try_new(VALID_HASH1, 5)?;
        let payload = Bytes::from_static(b"hello");

        // Build the simulated peer (a separate MemoryStore that
        // already has the AC entry).
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_store.update_oneshot(digest, payload.clone()).await?;

        // Register the digest in the AC pin registry against the
        // peer endpoint AND inject the peer store as the wrapper's
        // cached worker_connection so the lazy connect path is
        // skipped.
        registry.register_ac_pin(PEER_ENDPOINT, Arc::from(AC_STORE_ID), digest);
        proxy.inject_worker_connection(PEER_ENDPOINT, peer_store);

        // Inner has nothing. Reading through the wrapper must hit
        // the peer.
        let bytes = wrapper.get_part_unchunked(digest, 0, None).await?;
        Result::<Bytes, Error>::Ok(bytes)
    })
    .await;
    let bytes = result.expect(
        "AC peer-fetch must not deadlock — \
         writer-termination contract violated on inner-NotFound peer-redirect path",
    )?;

    assert_eq!(
        bytes,
        Bytes::from_static(b"hello"),
        "wrapper must deliver peer bytes for AC entry on inner NotFound — \
         redirect path failed to forward bytes through the wrapper's writer"
    );
    Ok(())
}

// -------------------------------------------------------------------
// Test 3: returns NotFound when no worker has the digest.
//
// Spec: with neither the inner store nor the registry holding an
// entry for the digest, the wrapper surfaces a clean NotFound
// without making spurious peer connections.
//
// Asymmetric coverage:
//   - Under-action: wrapper returns NotFound. Covered.
//   - Over-action: wrapper returns Ok(empty) or some other
//     non-NotFound error. The .err().code check below catches both.
//
// Mutation step:
//   1. In `ac_proxy_store.rs::get_part`, after `try_read_from_peer`
//      replace the final `Err(make_err!(Code::NotFound, ...))` with
//      `Ok(())`.
//   2. Run this test. Expected: fails with the bespoke
//      "wrapper must surface NotFound…" message because the result
//      is Ok instead of Err(NotFound).
//   3. Restore the line.
// -------------------------------------------------------------------
#[nativelink_test]
async fn ac_proxy_store_returns_notfound_when_no_worker_has_blob() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let (wrapper, _inner, _registry, _proxy) = make_proxy();
        let digest = DigestInfo::try_new(VALID_HASH2, 3)?;

        // Neither inner nor registry holds the digest.
        let outcome = wrapper.get_part_unchunked(digest, 0, None).await;
        Result::<_, Error>::Ok((digest, outcome))
    })
    .await;
    let (digest, outcome) = result.expect(
        "AC NotFound path must not deadlock — \
         writer-termination contract violated on no-peer fall-through",
    )?;

    let err = outcome.expect_err(
        "wrapper must surface NotFound when neither inner nor any peer holds the AC entry — \
         silent Ok-empty would corrupt the consumer with a phantom AC result",
    );
    assert_eq!(
        err.code,
        Code::NotFound,
        "wrapper must surface Code::NotFound (not {:?}) for missing AC entry {:?}",
        err.code,
        digest,
    );
    Ok(())
}

// -------------------------------------------------------------------
// Test 4: peer-NotFound evicts the AC pin entry.
//
// Spec: when a peer reports NotFound for a digest the registry
// thought it held, the wrapper drops the registry entry so the next
// AC read for the same digest doesn't pay the same wasted RTT.
//
// Asymmetric coverage:
//   - Under-action (covered): wrapper fails to evict; subsequent
//     reads keep hitting the same dead peer.
//   - Over-action: wrapper evicts on transient errors (e.g.
//     Unavailable). Test 4b below covers the over-action sibling.
//
// Mutation step:
//   1. In `ac_proxy_store.rs::try_read_from_peer`, comment out the
//      `self.registry.remove_digests_for_endpoint(...)` line in the
//      `Code::NotFound` arm.
//   2. Run this test. Expected: fails with the bespoke
//      "AC pin must be evicted…" message because the snapshot still
//      contains the digest.
//   3. Restore the line.
// -------------------------------------------------------------------
#[nativelink_test]
async fn ac_proxy_store_evicts_pin_on_peer_notfound() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let (wrapper, _inner, registry, proxy) = make_proxy();
        let digest = DigestInfo::try_new(VALID_HASH1, 4)?;

        // Peer is empty — it will report NotFound for `digest`.
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));

        registry.register_ac_pin(PEER_ENDPOINT, Arc::from(AC_STORE_ID), digest);
        proxy.inject_worker_connection(PEER_ENDPOINT, peer_store);

        // Pre-condition: registry entry exists.
        let before = registry.snapshot_endpoint(PEER_ENDPOINT).unwrap_or_default();
        assert!(
            before.iter().any(|(_, d)| *d == digest),
            "test setup is broken — AC pin should be registered before the read"
        );

        // Read — peer NotFounds, wrapper must surface NotFound AND evict.
        let outcome = wrapper.get_part_unchunked(digest, 0, None).await;
        Result::<_, Error>::Ok((registry, outcome))
    })
    .await;
    let (registry, outcome) = result.expect(
        "AC peer-NotFound eviction path must not deadlock — \
         writer-termination contract violated",
    )?;

    let err = outcome.expect_err(
        "peer NotFound must propagate — wrapper failed to surface NotFound after \
         every peer reported NotFound",
    );
    assert_eq!(err.code, Code::NotFound);

    // Post-condition: registry entry was dropped.
    let after = registry
        .snapshot_endpoint(PEER_ENDPOINT)
        .unwrap_or_default();
    assert!(
        after.is_empty(),
        "AC pin must be evicted after peer-NotFound (snapshot still has {} entries)",
        after.len()
    );
    Ok(())
}

// -------------------------------------------------------------------
// Test 4b (over-action sibling of test 4): transient peer errors
// must NOT evict the AC pin.
//
// Spec: a single Unavailable / Internal / Unknown blip does not
// prove the peer has lost the AC entry — only that this fetch
// attempt failed. Evicting on transient codes destroys locality
// after one network hiccup; same hazard as the CAS sibling
// (`worker_proxy_store::should_evict_locality_on_peer_error`).
//
// Implementation: inject an `AlwaysErrorPeer` that returns
// `Code::Unavailable` on every `get_part`. The wrapper must
// surface NotFound (because peer-fetch failed), but MUST NOT touch
// the registry — only NotFound is a definitive "peer no longer
// holds it" signal.
//
// Mutation step:
//   1. In `ac_proxy_store.rs::try_read_from_peer`, MOVE the
//      `remove_digests_for_endpoint` call OUT of the `if e.code ==
//      Code::NotFound` block and into the unconditional branch
//      after `Err(e) =>`.
//   2. Run this test. Expected: fails with the bespoke "AC pin
//      must NOT be evicted…" message because the snapshot lost the
//      entry on a transient peer error.
//   3. Restore the line.
// -------------------------------------------------------------------
#[nativelink_test]
async fn ac_proxy_store_keeps_pin_on_transient_peer_error() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let (wrapper, _inner, registry, proxy) = make_proxy();
        let digest = DigestInfo::try_new(VALID_HASH1, 4)?;

        // Inject a peer that always errors with Code::Unavailable
        // (the canonical "transient" signal).
        let transient_peer = Store::new(Arc::new(AlwaysErrorPeer {
            code: Code::Unavailable,
        }));
        registry.register_ac_pin(PEER_ENDPOINT, Arc::from(AC_STORE_ID), digest);
        proxy.inject_worker_connection(PEER_ENDPOINT, transient_peer);

        let outcome = wrapper.get_part_unchunked(digest, 0, None).await;
        Result::<_, Error>::Ok((registry, outcome, digest))
    })
    .await;
    let (registry, outcome, digest) = result.expect(
        "AC transient-peer path must not deadlock — \
         writer-termination contract violated",
    )?;

    let err = outcome.expect_err("expected NotFound after every peer transient");
    assert_eq!(err.code, Code::NotFound);

    // Post-condition: registry entry STILL present (transient
    // unavailability is not proof the peer lost the AC entry).
    let after = registry
        .snapshot_endpoint(PEER_ENDPOINT)
        .unwrap_or_default();
    assert!(
        after.iter().any(|(_, d)| *d == digest),
        "AC pin must NOT be evicted on transient peer error — \
         snapshot lost entry after a single Unavailable response"
    );
    Ok(())
}

// -------------------------------------------------------------------
// Test 5: has_with_results is pass-through — does NOT consult
// the AC pin registry.
//
// Spec from module docs: "Has-pass-through: `has_with_results` for
// the AC chain DOES NOT consult the AC pin registry." This is the
// digest-collision footgun guard: if a future maintainer is tempted
// to "speed up FindMissingBlobs for AC" by consulting the registry,
// the test red-fails them.
//
// Asymmetric coverage:
//   - Under-action: has() over-reports presence (returns Some when
//     only a peer holds the AC entry). Test asserts None.
//   - Over-action: has() under-reports (returns None when inner has
//     the entry). Out of scope here; covered transitively by the
//     pass-through being a single line `self.inner.has_with_results`.
//
// Mutation step:
//   1. In `ac_proxy_store.rs::has_with_results`, after the inner
//      call, add a fall-through that consults the registry and
//      sets `results[i] = Some(digest.size_bytes())` for any
//      missing slot whose digest the registry holds.
//   2. Run this test. Expected: fails with the bespoke "AC has must
//      report None…" message.
//   3. Restore.
// -------------------------------------------------------------------
#[nativelink_test]
async fn ac_proxy_store_has_does_not_consult_registry() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let (wrapper, _inner, registry, _proxy) = make_proxy();
        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

        // Register the AC pin against a peer. Inner has nothing.
        registry.register_ac_pin(PEER_ENDPOINT, Arc::from(AC_STORE_ID), digest);

        let result = wrapper.has(digest).await?;
        Result::<_, Error>::Ok(result)
    })
    .await;
    let outcome = result.expect(
        "AC has() must not deadlock — pass-through contract violated",
    )?;

    assert_eq!(
        outcome,
        None,
        "AC has must report None when inner is empty even if a peer reports the AC pin — \
         consulting AC pins on has would weaponize the CAS upload short-circuit at \
         bytestream_server (digest-collision footgun)"
    );
    Ok(())
}

// -------------------------------------------------------------------
// Test: AcProxyStore connection cache cleared on registry wipe
// (sibling-of-#194 leak).
//
// Spec: when the production wiring (in `src/bin/nativelink.rs`)
// connects `AcPinRegistry::on_endpoint_wipe` to
// `AcProxyStore::remove_worker_endpoint`, a registry wipe MUST also
// drop the cached worker AC connection for that endpoint. Pre-fixup
// production held a stale h2 channel per endpoint per boot-epoch
// flip; over hours of reconnect churn this leaks N stale GrpcStores
// per AC store. Fix is a callback-based hook on the registry.
//
// Asymmetric coverage:
//   - Under-action (covered): wipe fires but the cached connection
//     is NOT dropped. Direct assertion via `has_cached_connection`.
//   - Over-action (covered): wipe fires for endpoint A but ALSO
//     drops the connection cached against endpoint B. We assert
//     w2's connection survives the wipe of w1.
//
// Mutation step (run during authoring):
//   1. In `nativelink-util/src/ac_pin_registry.rs::wipe_endpoint`,
//      remove the `for cb in callbacks { cb(endpoint); }` block.
//   2. Run this test. Expected: red-fails with the bespoke
//      "AcProxyStore connection cache MUST be cleared on boot-epoch
//      wipe — sibling-of-#194 leak" message because
//      `has_cached_connection("grpc://w1:50081")` still returns
//      true.
//   3. Restore.
// -------------------------------------------------------------------
#[nativelink_test]
async fn ac_proxy_store_wipe_callback_clears_connection_cache() -> Result<(), Error> {
    let endpoint_w1 = "grpc://w1:50081";
    let endpoint_w2 = "grpc://w2:50081";

    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let (_wrapper, _inner, registry, proxy) = make_proxy();

        // Wire the production callback shape (mirrors
        // `src/bin/nativelink.rs`): weak-ref captures the proxy so
        // the registry doesn't keep the proxy alive past its own
        // lifetime, and on every wipe the proxy's cached worker
        // connection for the same endpoint is dropped.
        let proxy_weak = Arc::downgrade(&proxy);
        registry.on_endpoint_wipe(Arc::new(move |endpoint: &str| {
            if let Some(p) = proxy_weak.upgrade() {
                p.remove_worker_endpoint(endpoint);
            }
        }));

        // Inject two cached connections — one for the endpoint
        // we'll wipe, one that must survive (over-action guard).
        let stub_a = Store::new(MemoryStore::new(&MemorySpec::default()));
        let stub_b = Store::new(MemoryStore::new(&MemorySpec::default()));
        proxy.inject_worker_connection(endpoint_w1, stub_a);
        proxy.inject_worker_connection(endpoint_w2, stub_b);
        assert_eq!(
            proxy.cached_connection_count(),
            2,
            "test setup precondition: both connections must be cached \
             before the wipe fires"
        );

        // Trigger the wipe for w1 only.
        registry.wipe_endpoint(endpoint_w1);

        Result::<Arc<AcProxyStore>, Error>::Ok(proxy)
    })
    .await;
    let proxy = result.expect(
        "wipe_endpoint MUST not deadlock — callback-fire contract violated",
    )?;

    // Under-action assertion: w1's cached connection is gone.
    assert!(
        !proxy.has_cached_connection(endpoint_w1),
        "AcProxyStore connection cache MUST be cleared on \
         boot-epoch wipe — sibling-of-#194 leak"
    );

    // Over-action assertion: w2's cached connection survives the
    // wipe of w1. Cross-endpoint wipes would over-clear the cache
    // and leave the proxy unable to fan out to surviving workers.
    assert!(
        proxy.has_cached_connection(endpoint_w2),
        "wipe_endpoint(w1) MUST NOT touch w2's connection — \
         over-action: cross-endpoint wipe leaked"
    );
    assert_eq!(
        proxy.cached_connection_count(),
        1,
        "exactly one connection must remain cached after a single \
         endpoint wipe — over-action: connection cache cleared too \
         many entries"
    );
    Ok(())
}
