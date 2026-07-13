// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! #344 regression: noise-reduction for the WARN
//! `worker_proxy_store: mirror_stream: failed to stream blob to worker`
//! when the upstream tee already dropped chunks for the same digest.
//!
//! Production observation (2026-05-09 post-cascade-bundle deploy): the
//! bytestream tee at `bytestream_server.rs:1588-1605` does
//! `mirror_tx.try_send(chunk)`; on `Full` it bumps
//! `mirror_chunks_dropped_backpressure` and drops the chunk silently.
//! On `finish_write`, the producer used to let `mirror_tx` drop without
//! sending EOF (so the mirror task observed the generic
//! `Code::Internal "Sender dropped before sending EOF"`), and the
//! consumer in `worker_proxy_store::mirror_blob_via_stream` logged a
//! WARN per blob — 21/min residual rate, double-counting an event the
//! producer already logged at INFO (`receiver will re-fetch on demand`).
//!
//! Fix (this branch): when `mirror_dropped_any == true`, the producer
//! now calls `mirror_tx.send_error(Code::Aborted, MARKER)` instead of
//! letting the writer drop. The consumer matches the typed error
//! (`Code::Aborted` AND message contains `MIRROR_TEE_BACKPRESSURE_MARKER`)
//! and logs at DEBUG. Genuine network failures (different code or no
//! marker) still log at WARN.
//!
//! ## Asymmetric contract coverage (CLAUDE.md)
//!
//! - **Under-action sibling** (test 1, by-design tee backpressure):
//!   producer's `send_error(MARKER)` MUST cause the WARN site to log
//!   at DEBUG. If the gate is incorrectly skipped, the residual 21/min
//!   noise persists.
//! - **Over-action sibling** (test 2, real network failure): a
//!   `Code::Unavailable` from the peer (h2 GOAWAY / TCP RST stand-in)
//!   MUST still surface at WARN. If the gate over-matches (e.g. a
//!   coincidental string match), genuine worker failures get silenced
//!   — exactly the failure mode CLAUDE.md's
//!   `Asymmetric contract coverage` rule names.
//!
//! ## Seam crossed
//!
//! The wire-shape contract here is:
//!
//! 1. Producer (`bytestream_server.rs:process_client_stream`) sets the
//!    typed error on `mirror_tx` via `send_error(Code::Aborted, MARKER)`.
//! 2. Channel (`buf_channel`) carries the error through `terminal_error`
//!    instead of synthesizing the generic Internal.
//! 3. Consumer (`worker_proxy_store::mirror_blob_via_stream`) receives
//!    the error via the inner store's read-side propagation, matches on
//!    `is_mirror_tee_backpressure_error`, and demotes.
//!
//! These tests exercise seams (1)→(2)→(3) end-to-end: a real
//! `WorkerProxyStore`, a real `MemoryStore`-backed inner, a fake-worker
//! injected via `inject_worker_connection`, and a real
//! `make_buf_channel_pair_with_size(16)` reader/writer pair. The
//! producer side calls `send_error` directly on the writer (the same
//! call the bytestream_server tee branch makes).

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::worker_proxy_store::{
    MIRROR_TEE_BACKPRESSURE_MARKER, WorkerProxyStore,
};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair_with_size,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, UploadSizeInfo,
};

const VALID_HASH1: &str =
    "0123456789abcdef000000000000000000010000000000000123456789abcdef";

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Fake "worker" whose `update` simply drains the reader. Any error
/// surfaced by the reader (e.g. the producer's `send_error`) propagates
/// out of `update`, which is what `mirror_blob_via_stream` observes.
#[derive(Debug, MetricsComponent)]
struct DrainingWorkerStore {
    // MetricsComponent derive rejects unit structs and requires a metric
    // type it knows; a u64 satisfies both. The field is unused.
    #[metric(help = "unused")]
    _unused: u64,
}

impl DrainingWorkerStore {
    fn new() -> Self {
        Self { _unused: 0 }
    }
}

default_health_status_indicator!(DrainingWorkerStore);

#[async_trait]
impl StoreDriver for DrainingWorkerStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }
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
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                return Ok(0);
            }
        }
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(Code::NotFound, "DrainingWorkerStore has nothing to serve"))
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

/// Builds a `WorkerProxyStore` whose locality map says one peer holds
/// `digest`, and whose worker_connections has that peer pre-injected
/// (so `get_or_create_connection` returns immediately without trying
/// to dial a network endpoint).
fn build_proxy_with_one_fake_peer(
    digest: DigestInfo,
    peer_endpoint: &'static str,
) -> Arc<WorkerProxyStore> {
    let inner = Store::new(nativelink_store::memory_store::MemoryStore::new(
        &nativelink_config::stores::MemorySpec::default(),
    ));
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner, locality_map.clone());
    let fake_peer = Store::new(Arc::new(DrainingWorkerStore::new()));
    proxy.inject_worker_connection(peer_endpoint, fake_peer);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);
    proxy
}

fn digest_for_size(n: u64) -> DigestInfo {
    DigestInfo::try_new(VALID_HASH1, n).expect("valid digest")
}

// ---------------------------------------------------------------------
// Test 1 — Under-action: by-design tee backpressure logs at DEBUG
// ---------------------------------------------------------------------

/// Seam: producer `send_error(Code::Aborted, MARKER)` →
/// `buf_channel.terminal_error` → DrainingWorkerStore.update Err
/// propagation → `mirror_blob_via_stream` Err arm matches
/// `is_mirror_tee_backpressure_error` → DEBUG log.
///
/// Mutation step (executed): comment out the `if
/// is_mirror_tee_backpressure_error(&e)` branch in
/// `mirror_blob_via_stream` so EVERY error path takes the WARN. This
/// test must red-fail because the no-WARN assertion trips.
#[nativelink_test]
async fn mirror_stream_demotes_warn_when_tee_dropped_chunks() {
    // Some non-zero size so `pick_mirror_endpoint` accepts the peer.
    let digest = digest_for_size(1024);
    let proxy = build_proxy_with_one_fake_peer(
        digest,
        "grpc://fake-mirror-demote-peer:50081",
    );

    // Build the channel pair the same way the bytestream tee does
    // (`make_buf_channel_pair_with_size(16)`).
    let (mut tx, rx) = make_buf_channel_pair_with_size(16);

    // Spawn the consumer first so we can drive the producer-side
    // `send_error` from the test thread.
    let proxy_clone = proxy.clone();
    let task = tokio::spawn(async move {
        proxy_clone.mirror_blob_via_stream(digest, rx).await;
    });

    // Simulate the bytestream producer: forward a chunk (so the
    // consumer is past the empty-channel case) then signal "tee
    // backpressure" by calling `send_error` with the marker.
    tx.send(Bytes::from_static(b"hello")).await.expect("first chunk must enqueue");
    tx.send_error(make_err!(
        Code::Aborted,
        "{}",
        MIRROR_TEE_BACKPRESSURE_MARKER
    ));
    drop(tx);

    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect(
            "must not deadlock — mirror_blob_via_stream must complete within \
             5s after the producer signals tee backpressure",
        )
        .expect("mirror_blob_via_stream task must not panic");

    // The DEBUG demote-message MUST be present.
    assert!(
        logs_contain("mirror_stream: tee backpressure dropped chunks"),
        "DEBUG demote-message must fire when producer sent the marker — \
         the gate `is_mirror_tee_backpressure_error` failed to match \
         (the marker constant or the Code::Aborted check is wrong)",
    );
    // The original WARN message MUST NOT be present for this digest.
    assert!(
        !logs_contain("mirror_stream: failed to stream blob to worker"),
        "WARN message must NOT fire when producer sent the marker — \
         #344 noise-reduction failed; the gate did not demote",
    );
}

// ---------------------------------------------------------------------
// Test 2 — Over-action: real network failures still log at WARN
// ---------------------------------------------------------------------

/// Seam: producer `send_error(Code::Unavailable, "h2 GOAWAY")` →
/// `buf_channel.terminal_error` → DrainingWorkerStore.update Err
/// propagation → `mirror_blob_via_stream` Err arm DOES NOT match
/// `is_mirror_tee_backpressure_error` (different Code) → WARN log.
///
/// This is the asymmetric-contract sibling: the gate must be specific
/// enough to NOT silence genuine worker failures. Over-matching was
/// the failure mode CLAUDE.md's `Asymmetric contract coverage` names.
///
/// Mutation step (executed): change `is_mirror_tee_backpressure_error`
/// to always return `true`. This test must red-fail on the
/// WARN-required assertion.
#[nativelink_test]
async fn mirror_stream_keeps_warn_for_real_network_failures() {
    let digest = digest_for_size(2048);
    let proxy = build_proxy_with_one_fake_peer(
        digest,
        "grpc://fake-mirror-real-failure-peer:50081",
    );

    let (mut tx, rx) = make_buf_channel_pair_with_size(16);

    let proxy_clone = proxy.clone();
    let task = tokio::spawn(async move {
        proxy_clone.mirror_blob_via_stream(digest, rx).await;
    });

    // Drive a few bytes through, then signal a REAL transport failure:
    // Unavailable (h2 GOAWAY / TCP RST stand-in) carrying a generic
    // message that does NOT contain the backpressure marker.
    tx.send(Bytes::from_static(b"hello")).await.expect("first chunk must enqueue");
    tx.send_error(make_err!(
        Code::Unavailable,
        "h2 GOAWAY received from peer"
    ));
    drop(tx);

    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect(
            "must not deadlock — mirror_blob_via_stream must complete within \
             5s after the producer signals a real failure",
        )
        .expect("mirror_blob_via_stream task must not panic");

    // The original WARN MUST still fire for genuine failures.
    assert!(
        logs_contain("mirror_stream: failed to stream blob to worker"),
        "WARN MUST still fire for genuine network failures (Code::Unavailable, \
         no marker) — over-action: the demote gate is silencing real \
         failures, exactly the asymmetric-contract failure mode",
    );
    assert!(
        !logs_contain("mirror_stream: tee backpressure dropped chunks"),
        "DEBUG demote-message must NOT fire for genuine failures — \
         the gate matched a non-marker error",
    );
}
