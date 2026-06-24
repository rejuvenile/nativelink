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

//! #FL-688 §4 — WORKER→SERVER backfill upload retry-until-durable.
//!
//! The worker's server-driven backfill handler
//! (`LocalWorkerImpl::handle_upload_missing_blobs`) reads a missing blob
//! from its local fast tier and writes it to the slow store (the server's
//! CAS). Before this fix a per-blob upload that FAILED was `warn!`-logged
//! and DROPPED — no retry, no re-queue, no `failed_slow_writes` insert. The
//! server re-requested the identical missing set every BlobsAvailable tick,
//! the worker re-failed, and the missing count never decreased (the FL-688
//! stuck-backfill loop, observed live as `missing=3244` over three
//! consecutive ~62s cycles for worker `1f16f650-f5a4-65be-94f7`).
//!
//! The invariant: **a blob the worker is responsible for pushing is retried
//! until it lands on the server, never silently dropped.** The fix re-queues
//! a failed backfill upload into the shared `failed_slow_writes` set — the
//! SAME set the already-wired reconnect drainer (`drain_failed_digests` →
//! `handle_upload_missing_blobs`) reads — so the digest survives to the next
//! durable attempt.
//!
//! ## Production composition (the seam this test crosses)
//!
//! Producer:    `LocalWorkerImpl::handle_upload_missing_blobs` (real,
//!              via the `handle_upload_missing_blobs_for_test` seam).
//! Store chain: real `FastSlowStore` over a `RejectingSlowStore` (slow,
//!              Err on every `update`). The requested blob is held in the
//!              FSS `mirror_blobs` map — the production-faithful state
//!              where the worker holds a pinned mirror copy the server is
//!              asking it to upload back (`local_worker.rs:2682-2684`).
//!              The worker `get_cas_store()` FSS is NOT `local_only_reads`
//!              (`local_worker.rs:4951`), so its `has_with_results`
//!              consults slow → in-flight → mirror, and `get_part` reads
//!              mirror-first — exactly the tiers seeded here.
//! Manager:     real `RunningActionsManager` trait via
//!              `MockRunningActionsManager`, whose `get_cas_store()` returns
//!              the composed FSS.
//! Re-queue seam: the W4 Err arm → `FastSlowStore::failed_writes_inserter`
//!              → `failed_slow_writes` HashSet.
//! Drain seam:  `FastSlowStore::drain_failed_digests` (what the reconnect
//!              drainer calls) observes the re-queued digest.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, UploadSizeInfo,
};
use nativelink_worker::local_worker::handle_upload_missing_blobs_for_test;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use utils::local_worker_test_utils::MockWorkerApiClient;
use utils::mock_running_actions_manager::MockRunningActionsManager;

/// Slow store that always rejects writes with `Aborted` (modeling a
/// server-side reject WITHOUT a `BackpressureSignal` — the W2 0-retry class
/// that bubbles up to W4 as an Err). `has_with_results` reports "missing"
/// (the server does not hold the blob it is asking for) so the FSS wrapper
/// resolves the requested blob from its `mirror_blobs` map, then the upload
/// `update` is rejected — exercising the W4 Err arm.
#[derive(MetricsComponent)]
struct RejectingSlowStore {
    update_invocations: AtomicUsize,
}

impl RejectingSlowStore {
    fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoreDriver for RejectingSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut() {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Drain so the streaming-upload producer half does not deadlock on
        // a full channel before we return the Err.
        while let Ok(chunk) = reader.recv().await {
            if chunk.is_empty() {
                break;
            }
        }
        Err(make_err!(
            Code::Aborted,
            "RejectingSlowStore: server rejected the backfill upload"
        ))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::NotFound,
            "RejectingSlowStore: get_part not supported"
        ))
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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
}

default_health_status_indicator!(RejectingSlowStore);

/// Compose a production-shaped `FastSlowStore`: a real MemoryStore fast
/// tier over a `RejectingSlowStore` slow tier (which rejects every
/// upload). The requested blob is seeded into `mirror_blobs` (see
/// `seed_mirror`), not the fast tier.
fn make_fss_with_rejecting_slow() -> Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(Arc::new(RejectingSlowStore::new()));
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast,
        slow,
    )
}

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// Seed a blob into the FSS `mirror_blobs` map — the production-faithful
/// "worker holds a pinned mirror copy the server is asking it to upload
/// back" state. The worker's `get_cas_store()` returns the PLAIN (non
/// `local_only_reads`) `effective_cas_store` (`local_worker.rs:4951`),
/// whose `has_with_results` checks slow → in-flight → mirror (NOT the
/// fast tier directly — `fast_slow_store.rs:4847` "Only check the slow
/// store"). A blob held only in the fast tier would be reported MISSING
/// and the handler would early-return at the "none found locally" arm,
/// never reaching the W4 upload Err arm under test. `mirror_blobs` is the
/// tier the default `has_with_results` (`:4938`) AND `get_part`
/// (`:6114`, mirror-first) both consult, so seeding here exercises the
/// real backfill read→upload→Err→re-queue path.
fn seed_mirror(fss: &Arc<FastSlowStore>, digest: DigestInfo, payload: Bytes) {
    fss.test_insert_mirror_blob_unchecked(digest, payload);
}

/// W4: a failed backfill upload MUST re-queue the digest into
/// `failed_slow_writes` (small-blob `update_oneshot` path) so the reconnect
/// drainer re-attempts it — NOT drop it (the FL-688 stuck-loop regression).
#[nativelink_test]
async fn backfill_small_blob_upload_failure_requeues_for_retry() {
    let payload = Bytes::from_static(b"small_backfill_blob");
    let digest = mk_digest(11, payload.len());

    let fss = make_fss_with_rejecting_slow();
    seed_mirror(&fss, digest, payload);

    let ram = Arc::new(MockRunningActionsManager::new());
    ram.set_cas_store(fss.clone());

    // Drive the REAL production backfill handler.
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        handle_upload_missing_blobs_for_test::<MockWorkerApiClient, _>(&ram, vec![digest], 4),
    )
    .await
    .expect(
        "handle_upload_missing_blobs must not deadlock — backfill upload retry-until-durable",
    );

    // The slow store was actually asked to take the write (the upload was
    // attempted, not skipped).
    let drained = fss.drain_failed_digests();
    assert!(
        drained.contains(&digest),
        "backfill upload failure MUST re-queue the digest into failed_slow_writes \
         for retry-until-durable — #FL-688 stuck-loop regression (got {drained:?})",
    );
}

/// W4 (streaming path): the same contract for a >1 MiB blob, which takes
/// the streaming `update` branch of the backfill handler. Covers both
/// per-blob upload branches (oneshot AND streaming) — they share the Err
/// arm but reach it via different code.
#[nativelink_test]
async fn backfill_large_blob_upload_failure_requeues_for_retry() {
    // 1 MiB + 1 byte → exceeds STREAMING_THRESHOLD (1 MiB) in the handler.
    let size = 1024 * 1024 + 1;
    let payload = Bytes::from(vec![0xABu8; size]);
    let digest = mk_digest(12, size);

    let fss = make_fss_with_rejecting_slow();
    seed_mirror(&fss, digest, payload);

    let ram = Arc::new(MockRunningActionsManager::new());
    ram.set_cas_store(fss.clone());

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        handle_upload_missing_blobs_for_test::<MockWorkerApiClient, _>(&ram, vec![digest], 4),
    )
    .await
    .expect(
        "handle_upload_missing_blobs (streaming) must not deadlock — \
         backfill upload retry-until-durable",
    );

    let drained = fss.drain_failed_digests();
    assert!(
        drained.contains(&digest),
        "streaming backfill upload failure MUST re-queue the digest into \
         failed_slow_writes for retry-until-durable — #FL-688 stuck-loop \
         regression (got {drained:?})",
    );
}

/// Transport-exhaustion convergence (W1/W2 → W4): when the underlying
/// transport gives up (here modeled by the slow store's `Aborted` reject,
/// which is exactly the W2 non-`BackpressureSignal` 0-retry class), the Err
/// surfaces at the W4 backfill arm and the digest is re-queued, not
/// abandoned. This pins the architectural claim that W4 is the single
/// convergence point for transport give-up on the backfill path: the inline
/// W1=3 / W2-narrowing bounds stay UNCHANGED, durability comes from the
/// re-queue.
#[nativelink_test]
async fn backfill_transport_giveup_requeues_not_abandons() {
    let payload = Bytes::from_static(b"transport_giveup_blob");
    let digest = mk_digest(13, payload.len());

    let fss = make_fss_with_rejecting_slow();
    seed_mirror(&fss, digest, payload);

    let ram = Arc::new(MockRunningActionsManager::new());
    ram.set_cas_store(fss.clone());

    // Simulate two consecutive backfill ticks (the server re-requests the
    // same digest). The digest must remain recoverable across ticks — a
    // re-queue on EVERY failed tick, never a one-shot drop.
    for tick in 0..2u8 {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            handle_upload_missing_blobs_for_test::<MockWorkerApiClient, _>(
                &ram,
                vec![digest],
                4,
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("backfill tick {tick} must not deadlock — retry-until-durable")
        });
        // Peek without draining for the first tick; on the last tick drain
        // and assert. We re-insert by re-running, so just assert presence
        // via a contains check that does not consume the set.
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "after backfill tick {tick} the transport-give-up digest MUST be \
             present in failed_slow_writes (W1/W2 exhaustion re-queues at W4, \
             never abandons) — #FL-688",
        );
    }
}
