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

//! Bug A regression tests: locality eviction must fire on ANY peer
//! failure (not just `Code::NotFound`).
//!
//! Forensic context (digest 1e08eefa…-183):
//!   * The server's `BlobLocalityMap` claimed worker `worker-07`
//!     held the blob.
//!   * Every retry, the server fetched from garlic via
//!     `GrpcStore::get_part_parallel`; garlic returned the full 183
//!     bytes + clean `Status::OK` trailer; the parallel reader
//!     misclassified that as `Code::DataLoss` (Bug B — fixed in the
//!     same commit).
//!   * The locality eviction branch in `try_read_from_worker` (and
//!     its sibling `try_read_from_endpoints`) only matched
//!     `Code::NotFound`, so the stale entry was never cleared.
//!   * The `bytestream_write` upload-skip fast path then consulted
//!     the locality map's `has()` (which trusted the lying entry),
//!     dropped Bazel's re-upload silently, and Bazel observed an
//!     infinite zombie `NOT_FOUND` loop.
//!
//! Bug A fix: the locality map is a HINT, not a contract. Any failed
//! fetch — regardless of error code — should reduce trust in the
//! entry. `is_connection_error` is checked first (already removes the
//! whole endpoint via `remove_worker_endpoint`); EVERY other peer
//! failure now also calls `evict_blobs(endpoint, &[digest])` so a
//! future has_with_results / FindMissingBlobs doesn't keep returning
//! a ghost hit.
//!
//! These tests verify the widened eviction policy through the public
//! `WorkerProxyStore` API by injecting fake peer Stores that return
//! configured `Code` values.

use core::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::{SharedBlobLocalityMap, new_shared_blob_locality_map};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Helper: create a `WorkerProxyStore` and return the underlying Arc so
/// we can call `inject_worker_connection`. Mirrors the shape used in
/// `worker_proxy_store_test.rs::make_proxy_store_with_arc`.
fn make_proxy_store_with_arc() -> (Arc<WorkerProxyStore>, Store, SharedBlobLocalityMap) {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner.clone(), locality_map.clone());
    (proxy_arc, inner, locality_map)
}

/// A store wrapper that returns a configured `Code` immediately when
/// `get_part` is called, with no bytes written first. Used to model
/// peer responses for the locality-eviction-on-any-error path.
#[derive(Debug, MetricsComponent)]
struct AlwaysFailStore {
    fail_code: Code,
}

default_health_status_indicator!(AlwaysFailStore);

#[async_trait]
impl StoreDriver for AlwaysFailStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Err(make_err!(self.fail_code, "AlwaysFailStore: simulated failure"))
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        Err(make_err!(self.fail_code, "AlwaysFailStore: simulated failure"))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            self.fail_code,
            "AlwaysFailStore: simulated failure (code={:?})",
            self.fail_code
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
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// Helper: register one peer in the locality map, run a single
/// `get_part_unchunked` against the proxy, then assert the peer is no
/// longer in the locality map for the digest. This is the canonical
/// Bug A regression check — before the fix, only `Code::NotFound`
/// triggered eviction, so any other peer failure left a stale entry
/// behind.
async fn assert_peer_evicted_after_failure(
    fail_code: Code,
    expected_outer_code: Option<Code>,
    label: &str,
) -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let value = b"unused: peer always fails";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    let peer_endpoint = "grpc://peer-a:50081";
    let peer_store = Store::new(Arc::new(AlwaysFailStore { fail_code }));
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let result = proxy.get_part_unchunked(digest, 0, None).await;
    if let Some(code) = expected_outer_code {
        let err = result.expect_err(
            "expected proxy.get_part_unchunked to fail when the only peer fails",
        );
        assert_eq!(
            err.code, code,
            "{label}: expected outer code {code:?}, got {err:?}"
        );
    }

    let workers = locality_map.read().lookup_workers(&digest);
    let workers_str: Vec<String> = workers.iter().map(|s| s.to_string()).collect();
    assert!(
        !workers_str.iter().any(|e| e == peer_endpoint),
        "{label}: peer A should have been evicted from locality after \
         returning {fail_code:?}, but the entry remained. workers={workers_str:?}"
    );
    Ok(())
}

/// Bug A regression: peer returns `Code::DataLoss` → locality entry
/// must be evicted. Before the fix, only `Code::NotFound` evicted, so
/// a peer that consistently returned `DataLoss` (e.g. via Bug B's
/// pre-fix clean-EOF misclassification) would forever poison the map.
#[nativelink_test]
async fn peer_data_loss_evicts_locality_entry() -> Result<(), Error> {
    assert_peer_evicted_after_failure(
        Code::DataLoss,
        Some(Code::NotFound),
        "DataLoss peer",
    )
    .await
}

// REMOVED 2026-04-26: the integration tests for `peer_internal_error_*`,
// `peer_aborted_*`, `peer_deadline_exceeded_*`, and
// `peer_unavailable_removes_endpoint_and_evicts_locality` asserted the
// pre-380793fe WIDE locality-eviction policy (every peer-fetch failure
// drops the locality entry). That policy was deliberately narrowed to
// `NotFound | DataLoss` only by commit `380793fe (worker_proxy_store:
// narrow locality eviction to NotFound/DataLoss only)`.
//
// Rationale (see `should_evict_locality_on_peer_error` doc comment in
// `nativelink-store/src/worker_proxy_store.rs:325-347`): a transient
// failure (DeadlineExceeded, Unavailable, Internal, Aborted, transport
// blip) does not prove the peer no longer holds the blob — only that
// THIS specific fetch attempt failed. Evicting on every transient
// permanently destroys locality for blobs that only ONE peer holds, so
// every future FindMissingBlobs misses the fast path even though the
// peer still has the data.
//
// The narrow policy is covered by the pure-function unit tests in
// `nativelink-store/src/worker_proxy_store.rs::tests` (the
// `test_should_evict_on_*` / `test_should_keep_on_*` cluster) and by
// `peer_data_loss_evicts_locality_entry` /
// `peer_not_found_still_evicts_locality_entry` below, which retain
// integration coverage for the codes that DO still evict.

/// Bug A regression: peer returns `Code::NotFound` → locality entry
/// must STILL be evicted (preserving the pre-fix behaviour for this
/// code). Guards against the widened branch accidentally regressing
/// the original NotFound path.
#[nativelink_test]
async fn peer_not_found_still_evicts_locality_entry() -> Result<(), Error> {
    assert_peer_evicted_after_failure(
        Code::NotFound,
        Some(Code::NotFound),
        "NotFound peer (pre-fix path)",
    )
    .await
}
