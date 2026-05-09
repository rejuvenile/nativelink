// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! #287 server-side `failed_slow_writes` drain integration tests.
//!
//! Production composition: real `FastSlowStore`, real `BlobLocalityMap`,
//! real `SmallBlobDispatcher` with a registered worker tx — same
//! components production wires up at startup. The drain-tick logic
//! itself lives in `nativelink_service::failed_writes_drain::drain_tick`.
//!
//! ## Tests in this file
//!
//! - **Under-action**
//!   (`failed_slow_writes_drains_via_upload_missing_blobs`):
//!   force a digest into `failed_slow_writes` + register that digest
//!   in `BlobLocalityMap` + register the worker_tx via the dispatcher.
//!   `drain_tick` MUST (a) remove the digest from `failed_slow_writes`,
//!   (b) deliver an `UploadMissingBlobs` message on the worker_tx.
//!   Mutation: comment out the dispatcher → no worker_tx in snapshot
//!   → digest is not dispatched but is re-inserted; the assertion on
//!   the rx side red-fails with the bespoke "dead-letter regression"
//!   message.
//!
//! - **No-worker edge case**
//!   (`failed_slow_writes_skips_when_no_worker_has_blob`):
//!   force a digest into `failed_slow_writes` BUT with an empty
//!   `BlobLocalityMap`. `drain_tick` MUST (a) NOT panic, (b) NOT
//!   dispatch any UploadMissingBlobs message, (c) re-insert the digest
//!   so a future tick can retry once a worker reports the digest.
//!
//! - **Throttled re-insert**
//!   (`failed_slow_writes_throttled_digest_reinserts`):
//!   prime the `inflight: HashMap<DigestInfo, Instant>` cooldown map
//!   with the digest at `Instant::now()`, run a tick. The drain MUST
//!   see the cooldown still active, count it as `throttled`, NOT
//!   dispatch, and re-insert the digest. (testing-czar MAJOR-2 from
//!   #287 review.)
//!
//! - **`tx.send()` Err re-insert**
//!   (`failed_slow_writes_tx_send_err_reinserts`):
//!   register a worker_tx then immediately drop the rx side so
//!   `mpsc::UnboundedSender::send()` returns `Err`. Run a tick. The
//!   drain MUST count the digest as `send_failed` (NOT `dispatched`)
//!   and re-insert it into `failed_slow_writes`. M1 invariant
//!   `drained == dispatched + no_worker + throttled + send_failed`
//!   is asserted here. (testing-czar MAJOR-3 + code-reviewer M1.)
//!
//! All tests run under `tokio::time::timeout(few seconds)` as a
//! deadlock detector + use bespoke `.expect` messages per CLAUDE.md.
//!
//! ## Mutation step
//!
//! Per CLAUDE.md mutation rule: confirmed in fixup commit
//! `failed_writes_drain` MAJOR-3 by commenting out the per-endpoint
//! dispatch loop (`for (endpoint, entries) in per_endpoint { ... }`)
//! in `nativelink-service/src/failed_writes_drain.rs`. With the loop
//! deleted, the under-action test fails INSIDE the
//! `tokio::time::timeout(DRAIN_TIMEOUT, ...)` block on
//! `worker_rx.recv().await.expect(...)` returning `None` — the
//! bespoke message
//! `"server-side failed_slow_writes must drain via UploadMissingBlobs
//! — dead-letter regression: rx returned None before any message
//! landed"` fires (because the dispatcher's tx was dropped at the end
//! of the spawned-task scope without any send, closing the channel
//! cleanly). Restored the dispatch loop, re-ran, all 4 tests green.
//! This is the loud failure path the test exists to gate.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker,
};
use nativelink_service::failed_writes_drain::{
    DEFAULT_DRAIN_BATCH_SIZE, DEFAULT_DRAIN_COOLDOWN, DEFAULT_DRAIN_INFLIGHT_CAP, drain_tick,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::small_blob_dispatcher::{SmallBlobDispatcher, SmallBlobDispatcherConfig};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
};
use tokio::sync::mpsc;

/// Bounded deadline for "drain_tick should pick up the digest +
/// dispatch + we should observe the message on rx". The drain is
/// synchronous so 5 s is generous; a regression that wires the drain
/// path incorrectly will hang on rx.recv() and red-fail with the
/// bespoke message below (see CLAUDE.md "Test in production
/// composition" — the timeout IS the deadlock detector).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

const VALID_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Build a real FastSlowStore (Memory + Memory) — same `FastSlowStore`
/// type production cas_STORE wraps. Returns the Arc directly so we
/// can call `failed_writes_inserter()` (only available on the Arc).
fn make_fss() -> Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast,
        slow,
    )
}

/// Under-action coverage. The server-side `failed_slow_writes` set
/// MUST drain via `UploadMissingBlobs` to a worker that has the digest.
#[nativelink_test]
async fn failed_slow_writes_drains_via_upload_missing_blobs() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        // Production composition: real FastSlowStore, real
        // BlobLocalityMap, real SmallBlobDispatcher, real worker_tx
        // mpsc channel.
        let fss = make_fss();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Register a worker stream — what `WorkerApiServer::connect_worker`
        // does at production setup time. The endpoint string is what
        // BlobLocalityMap keys on too.
        let worker_endpoint = "grpc://test-worker:50071";
        let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker(worker_endpoint, /* boot_epoch */ 1, worker_tx);

        // Force a digest into `failed_slow_writes` via the same closure
        // production uses (`failed_writes_inserter`) — identical to
        // the legacy update Err arm + chunked-commit Err arm + pin
        // auto-expire path.
        let digest = DigestInfo::try_new(VALID_HASH, 1024).expect("valid digest");
        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        // Register the digest in BlobLocalityMap so the drain can
        // resolve worker for digest. Fixture invariant — same call
        // production makes from `WorkerApiServer::handle_blobs_available`.
        locality_map
            .write()
            .register_blobs(worker_endpoint, &[digest]);

        // Sanity: the set contains the digest BEFORE drain.
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "fixture invariant: failed_slow_writes must contain the digest \
             before the drain tick"
        );

        // Drive the drain tick. Production composition: same
        // `drain_tick` the binary's spawn calls every 5 s.
        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let stats = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        // Stats: dispatched 1, no_worker 0, throttled 0.
        assert_eq!(
            stats.drained, 1,
            "drain_tick must drain the digest from failed_slow_writes; \
             stats={stats:?}"
        );
        assert_eq!(
            stats.dispatched, 1,
            "drain_tick must dispatch UploadMissingBlobs for the digest \
             when a connected worker has it; stats={stats:?}"
        );
        assert_eq!(stats.no_worker, 0, "stats={stats:?}");
        assert_eq!(stats.throttled, 0, "stats={stats:?}");

        // Receive-side assertion: an UploadMissingBlobs message MUST
        // land on the worker_tx within the timeout. This is the
        // deadlock-detector-via-timeout — without the drain wiring,
        // recv hangs forever and the outer timeout fires with the
        // bespoke "dead-letter regression" message.
        let received = worker_rx.recv().await.expect(
            "server-side failed_slow_writes must drain via UploadMissingBlobs — \
             dead-letter regression: rx returned None before any message landed",
        );
        match received.update {
            Some(update_for_worker::Update::UploadMissingBlobs(req)) => {
                assert_eq!(
                    req.digests.len(),
                    1,
                    "UploadMissingBlobs must carry exactly the failed digest"
                );
                assert_eq!(req.digests[0].hash, VALID_HASH);
                assert_eq!(req.digests[0].size_bytes, 1024);
            }
            other => panic!(
                "expected UploadMissingBlobs on worker_tx; got {other:?} \
                 (server-side drain must dispatch UploadMissingBlobs, not \
                 some other variant)"
            ),
        }

        // The digest MUST be GONE from `failed_slow_writes` after a
        // successful dispatch — no dead-letter retention. If a future
        // dispatch fails, the slow-tier write Err arm re-inserts
        // naturally.
        assert!(
            !fss.failed_slow_writes_contains(&digest),
            "successful dispatch MUST clear the digest from \
             failed_slow_writes — re-insert is for the failure path \
             only (post-#287 dead-letter regression)"
        );

        Ok::<(), Error>(())
    })
    .await
    .expect("drain test must not deadlock — production composition contract violated")?;
    Ok(())
}

/// No-worker edge case: a digest is in `failed_slow_writes` but no
/// worker reports having it (locality_map is empty for that digest).
/// The drain MUST (a) not panic, (b) not send any UploadMissingBlobs,
/// (c) re-insert the digest so it can be retried later.
#[nativelink_test]
async fn failed_slow_writes_skips_when_no_worker_has_blob() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        let fss = make_fss();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Register a worker tx, but DO NOT register the digest in
        // BlobLocalityMap — so `lookup_workers(digest)` returns [].
        let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker("grpc://test-worker:50071", 1, worker_tx);

        let digest = DigestInfo::try_new(VALID_HASH, 1024).expect("valid digest");
        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let stats = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        assert_eq!(
            stats.drained, 1,
            "must not panic when no worker has the failed digest; \
             stats={stats:?}"
        );
        assert_eq!(
            stats.dispatched, 0,
            "must NOT dispatch UploadMissingBlobs when no worker has \
             the digest; stats={stats:?}"
        );
        assert_eq!(
            stats.no_worker, 1,
            "must count this case as no_worker; stats={stats:?}"
        );
        assert_eq!(stats.throttled, 0, "stats={stats:?}");

        // No worker_tx send — the rx must remain empty. We use
        // try_recv to avoid hanging the test on the absence of a
        // message.
        let recv_result = worker_rx.try_recv();
        assert!(
            recv_result.is_err(),
            "no UploadMissingBlobs must be sent when no worker has the \
             digest — over-action regression: drain dispatched anyway. \
             got={recv_result:?}"
        );

        // The digest MUST be re-inserted into `failed_slow_writes` so a
        // future tick (after a worker reports it via BlobsAvailable)
        // can dispatch.
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "no-worker case MUST re-insert into failed_slow_writes — \
             otherwise a transient missing-locality entry permanently \
             loses the failed digest"
        );

        Ok::<(), Error>(())
    })
    .await
    .expect("no-worker test must not deadlock")?;
    Ok(())
}

/// Throttle-path coverage (testing-czar MAJOR-2). A digest in
/// `failed_slow_writes` whose entry is in the `inflight` cooldown map
/// MUST (a) NOT dispatch, (b) be re-inserted into `failed_slow_writes`,
/// (c) be counted as `throttled` in the stats. Without this guarantee
/// a regression that "drops on throttle" would silently lose the
/// digest forever (the second dead-letter regression of #287's
/// shape).
#[nativelink_test]
async fn failed_slow_writes_throttled_digest_reinserts() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        let fss = make_fss();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Worker IS connected and DOES claim to have the digest — so
        // without the cooldown the drain WOULD dispatch. The cooldown
        // is the gate this test exercises.
        let worker_endpoint = "grpc://test-worker:50071";
        let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker(worker_endpoint, /* boot_epoch */ 1, worker_tx);

        let digest = DigestInfo::try_new(VALID_HASH, 1024).expect("valid digest");
        locality_map
            .write()
            .register_blobs(worker_endpoint, &[digest]);

        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        // Prime the cooldown map: pretend we dispatched THIS instant.
        // The drain's first action is `inflight.retain(|_, ts|
        // now.duration_since(*ts) < cooldown)` — `Instant::now()` is
        // inside the cooldown window so the entry survives the GC,
        // and the per-digest "still throttled" branch fires.
        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        inflight.insert(digest, Instant::now());

        let stats = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        assert_eq!(
            stats.drained, 1,
            "drain_tick must drain even throttled digests; \
             stats={stats:?}"
        );
        assert_eq!(
            stats.throttled, 1,
            "drain_tick must count throttled digest in stats.throttled; \
             stats={stats:?}"
        );
        assert_eq!(
            stats.dispatched, 0,
            "throttled digest must NOT be dispatched even when a worker \
             has it; stats={stats:?}"
        );
        assert_eq!(stats.no_worker, 0, "stats={stats:?}");
        assert_eq!(stats.send_failed, 0, "stats={stats:?}");

        // No worker_tx send — the rx must remain empty.
        let recv_result = worker_rx.try_recv();
        assert!(
            recv_result.is_err(),
            "throttled digest must NOT cause UploadMissingBlobs send — \
             over-action regression: drain dispatched anyway. got={recv_result:?}"
        );

        // The digest MUST be re-inserted (testing-czar MAJOR-2 dead-letter
        // shape: throttle that drops the digest silently re-creates the
        // exact regression #287 was filed to fix).
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "throttled digest MUST be re-inserted into failed_slow_writes — \
             otherwise a single throttle silently loses the digest forever \
             (dead-letter regression of the same shape #287 was filed to fix)"
        );

        Ok::<(), Error>(())
    })
    .await
    .expect("throttle test must not deadlock")?;
    Ok(())
}

/// `tx.send()` Err coverage (testing-czar MAJOR-3 + code-reviewer M1).
/// A worker is registered, the drain decides to dispatch, but the
/// underlying `mpsc::UnboundedSender::send()` returns `Err` (the rx
/// side was dropped — half-open channel). The drain MUST (a) count
/// the digest as `send_failed` (NOT `dispatched`), (b) re-insert it
/// into `failed_slow_writes` so the next tick can retry, (c) honor
/// the M1 invariant
/// `drained == dispatched + no_worker + throttled + send_failed` so
/// counters add up cleanly for operator metrics.
///
/// Why this matters: under sustained worker disconnect the drain
/// could otherwise increment `dispatched` for every digest while the
/// digests vanish into a closed mpsc — a counter that lies AND a
/// dead-letter regression in one. M1 fix gates this.
#[nativelink_test]
async fn failed_slow_writes_tx_send_err_reinserts() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        let fss = make_fss();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Register worker, then DROP the rx side BEFORE the drain
        // runs. The dispatcher snapshot returns the tx (still alive
        // by Arc), but `tx.send()` will return Err the instant we
        // attempt the dispatch — the half-open mpsc state.
        let worker_endpoint = "grpc://test-worker:50071";
        let (worker_tx, worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker(worker_endpoint, /* boot_epoch */ 1, worker_tx);
        drop(worker_rx);

        let digest = DigestInfo::try_new(VALID_HASH, 1024).expect("valid digest");
        locality_map
            .write()
            .register_blobs(worker_endpoint, &[digest]);

        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let stats = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        assert_eq!(
            stats.drained, 1,
            "drain_tick must drain the digest even when send will fail; \
             stats={stats:?}"
        );
        assert_eq!(
            stats.send_failed, 1,
            "tx.send() Err MUST count toward send_failed — that's the \
             whole point of the M1 counter; stats={stats:?}"
        );
        assert_eq!(
            stats.dispatched, 0,
            "tx.send() Err MUST NOT count toward dispatched — the M1 \
             invariant requires `dispatched + no_worker + throttled + \
             send_failed == drained`; pre-M1 the increment ran at \
             queue-time and lied. stats={stats:?}"
        );
        assert_eq!(stats.no_worker, 0, "stats={stats:?}");
        assert_eq!(stats.throttled, 0, "stats={stats:?}");

        // M1 invariant (extended for #335 V3 self-retry counters):
        // counters add up to drained.
        assert_eq!(
            stats.dispatched
                + stats.no_worker
                + stats.throttled
                + stats.send_failed
                + stats.self_retried
                + stats.self_retry_failed,
            stats.drained,
            "M1 invariant violated: counters do not add up to drained; \
             stats={stats:?}"
        );

        // The digest MUST be re-inserted into failed_slow_writes — the
        // closed-channel re-insert path is the dead-letter hot-spot
        // #287 was filed to fix.
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "send-Err MUST re-insert into failed_slow_writes — otherwise a \
             worker disconnect during dispatch silently loses the digest \
             (dead-letter regression of the exact shape #287 was filed to fix)"
        );

        Ok::<(), Error>(())
    })
    .await
    .expect("tx-send-err test must not deadlock")?;
    Ok(())
}

// =============================================================================
// #335 V3 fix coverage (TLA+ liveness audit)
// =============================================================================
//
// Background: the TLA+ audit (agent a88faadb49a541ea2) found a permanent-
// stuck condition. After SlowWriteFailure for blob b1, the failed_slow_writes
// drainer can ONLY retry via UploadMissingBlobs to a worker. If NO worker
// has the bytes (mirror dispatch quarantined / slow / never reached the
// worker), the drainer logs "no worker for digest" forever; the fast-tier
// pin auto-expires (120 s); LRU evicts; the next Bazel read returns
// NotFound. The Bazel-acked bytes are LOST.
//
// V3 fix: the server's MemoryStore (fast tier) holds the bytes for the
// duration of the pin TTL — that's an authoritative source the drainer
// can use. drain_tick now calls FastSlowStore::try_self_retry_slow_write
// FIRST (before consulting the locality map). On Succeeded, no worker
// round-trip needed.
//
// Tests below use a `GatedSlowStore` test fixture: a slow store whose
// update_oneshot returns either Ok (proxying to an inner MemoryStore)
// or Err depending on a runtime-toggleable AtomicBool. This lets one
// test simulate "slow-write failed THEN recovered" within a single
// process composition, without needing failpoints or wall-clock waits.

/// Minimal slow-store test fixture: proxies to an inner MemoryStore but
/// can be toggled to return Err on update_oneshot/update. Used to
/// simulate transient slow-tier failures that the drainer must
/// recover from.
#[derive(Debug)]
struct GatedSlowStore {
    inner: Arc<MemoryStore>,
    fail_updates: AtomicBool,
}

impl GatedSlowStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStore::new(&MemorySpec::default()),
            fail_updates: AtomicBool::new(false),
        })
    }

    fn set_fail(&self, fail: bool) {
        self.fail_updates.store(fail, Ordering::SeqCst);
    }
}

impl MetricsComponent for GatedSlowStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[async_trait]
impl StoreDriver for GatedSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Always delegate to inner — even when fail_updates is true,
        // reads are unaffected (matches "transient write outage" model).
        Pin::new(self.inner.as_ref())
            .has_with_results(keys, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        if self.fail_updates.load(Ordering::SeqCst) {
            // Drain the reader so writers don't observe a "Sender
            // dropped before EOF" error (writer-termination contract
            // for borrowed reader).
            drop(reader.drain().await);
            return Err(make_err!(
                Code::Internal,
                "GatedSlowStore: simulated transient slow-tier failure"
            ));
        }
        Pin::new(self.inner.as_ref())
            .update(key, reader, size_info)
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await
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

    fn optimized_for(&self, _optimization: StoreOptimizations) -> bool {
        false
    }
}

default_health_status_indicator!(GatedSlowStore);

/// Build an FSS with a real MemoryStore fast tier and a togglable
/// GatedSlowStore slow tier. Returns (fss, fast_store, gated_slow) so
/// tests can pre-write bytes to the fast tier and toggle slow-tier
/// failure mode.
fn make_fss_with_gated_slow() -> (
    Arc<FastSlowStore>,
    Arc<MemoryStore>,
    Arc<GatedSlowStore>,
) {
    let fast_arc = MemoryStore::new(&MemorySpec::default());
    let slow_arc = GatedSlowStore::new();
    let fast = Store::new(fast_arc.clone());
    let slow = Store::new(slow_arc.clone());
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast,
        slow,
    );
    (fss, fast_arc, slow_arc)
}

/// V3 headline case: failed_slow_writes drainer self-retries from the
/// server's MemoryStore when the fast tier still holds the bytes.
/// No worker round-trip; closes the TLA+ liveness gap.
///
/// Production composition: real FSS (Memory fast + GatedSlow), real
/// BlobLocalityMap (DELIBERATELY EMPTY — no worker has the bytes),
/// real SmallBlobDispatcher.
#[nativelink_test]
async fn failed_slow_writes_self_retries_from_server_fast_tier() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        let (fss, fast_arc, _slow_arc) = make_fss_with_gated_slow();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        // Empty locality map: NO worker reports having this digest. This
        // is the exact TLA+ scenario — without V3, drain_tick can only
        // count `no_worker` and re-insert forever.
        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Worker IS connected (just doesn't have the digest in
        // locality_map) — proves V3 fires even when the dispatch path
        // is wired and ready.
        let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker("grpc://test-worker:50071", 1, worker_tx);

        // Pre-stage bytes in the FAST tier (matches the post-pin
        // production state after a slow-tier write failure: the byte
        // payload is still in MemoryStore via the failed_writes_inserter
        // re-pin).
        let digest = DigestInfo::try_new(VALID_HASH, 4).expect("valid digest");
        let bytes = Bytes::from_static(b"V3!!");
        Pin::new(fast_arc.as_ref())
            .update_oneshot(StoreKey::Digest(digest), bytes.clone())
            .await
            .expect("seed fast tier with bytes");

        // Mark the slow-tier write as failed (the trigger for V3
        // recovery). failed_writes_inserter mirrors the production
        // post-failure bookkeeping: insert into failed_slow_writes +
        // re-pin the fast-tier replica.
        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let stats = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        assert_eq!(
            stats.drained, 1,
            "drain_tick must drain the digest from failed_slow_writes; \
             stats={stats:?}"
        );
        assert_eq!(
            stats.self_retried, 1,
            "V3 fix: failed_slow_writes drainer must self-retry from server \
             MemoryStore when no worker source — TLA+ liveness contract \
             violated. stats={stats:?}"
        );
        assert_eq!(
            stats.no_worker, 0,
            "V3 fix: when fast tier has the bytes, drainer must NOT count \
             this as no_worker (the worker round-trip is unnecessary); \
             stats={stats:?}"
        );
        assert_eq!(
            stats.dispatched, 0,
            "V3 fix: when self-retry succeeds, NO UploadMissingBlobs is \
             dispatched; stats={stats:?}"
        );
        assert_eq!(stats.self_retry_failed, 0, "stats={stats:?}");
        assert_eq!(stats.send_failed, 0, "stats={stats:?}");
        assert_eq!(stats.throttled, 0, "stats={stats:?}");

        // Worker rx must remain empty — V3 succeeds without dispatching
        // an UploadMissingBlobs RPC. Over-action regression check.
        let recv_result = worker_rx.try_recv();
        assert!(
            recv_result.is_err(),
            "V3 fix: successful self-retry MUST NOT dispatch UploadMissingBlobs \
             (over-action regression). got={recv_result:?}"
        );

        // Slow tier must now hold the bytes (Bazel's next read can be
        // served from durable storage; pin can expire safely).
        let slow_bytes = fss
            .slow_store_handle()
            .get_part_unchunked(StoreKey::Digest(digest), 0, Some(4))
            .await
            .expect("V3 fix: slow tier MUST hold the bytes after self-retry");
        assert_eq!(
            slow_bytes,
            Bytes::from_static(b"V3!!"),
            "V3 fix: slow-tier bytes after self-retry must equal the original \
             fast-tier bytes"
        );

        // failed_slow_writes must be empty post-self-retry success
        // (FSS clears it inside try_self_retry_slow_write on Ok).
        assert!(
            !fss.failed_slow_writes_contains(&digest),
            "V3 fix: successful self-retry MUST clear the digest from \
             failed_slow_writes — otherwise a second tick would re-process \
             a permanently-recovered blob"
        );

        Ok::<(), Error>(())
    })
    .await
    .expect(
        "V3 self-retry test must not deadlock — production composition \
         contract violated (TLA+ liveness gap)",
    )?;
    Ok(())
}

/// V3 fall-through case: when the fast tier has lost the bytes (pin
/// expired before drainer ran), the drainer must fall through to the
/// pre-#335 UploadMissingBlobs path. This guards backward compatibility.
#[nativelink_test]
async fn failed_slow_writes_falls_through_to_worker_on_fast_tier_miss() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        let (fss, _fast_arc, _slow_arc) = make_fss_with_gated_slow();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Worker connected AND claims to have the digest.
        let worker_endpoint = "grpc://test-worker:50071";
        let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker(worker_endpoint, 1, worker_tx);

        let digest = DigestInfo::try_new(VALID_HASH, 1024).expect("valid digest");
        locality_map
            .write()
            .register_blobs(worker_endpoint, &[digest]);

        // DO NOT pre-stage the fast tier — simulates the post-pin-
        // expire state where MemoryStore has evicted the bytes.
        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let stats = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        assert_eq!(stats.drained, 1, "stats={stats:?}");
        assert_eq!(
            stats.self_retried, 0,
            "V3 fall-through: when fast tier is empty, self-retry MUST NOT \
             succeed; stats={stats:?}"
        );
        assert_eq!(
            stats.dispatched, 1,
            "V3 fall-through: fast-tier miss MUST dispatch UploadMissingBlobs \
             to the worker (preserves pre-#335 path); stats={stats:?}"
        );

        // Receive-side: an UploadMissingBlobs MUST land on worker_tx
        // (proves the fall-through actually wires through to dispatch).
        let received = worker_rx.recv().await.expect(
            "V3 fall-through: fast-tier miss MUST dispatch UploadMissingBlobs \
             to a worker — backward-compat regression: rx returned None",
        );
        match received.update {
            Some(update_for_worker::Update::UploadMissingBlobs(req)) => {
                assert_eq!(req.digests.len(), 1);
                assert_eq!(req.digests[0].hash, VALID_HASH);
            }
            other => panic!(
                "expected UploadMissingBlobs on worker_tx; got {other:?} \
                 (V3 fall-through wiring broken)"
            ),
        }

        Ok::<(), Error>(())
    })
    .await
    .expect("V3 fall-through test must not deadlock")?;
    Ok(())
}

/// V3 transient slow-tier failure: when the fast tier has the bytes
/// but the slow tier write returns Err, the drainer must (a) count
/// self_retry_failed, (b) re-insert the digest for the next tick,
/// (c) NOT dispatch UploadMissingBlobs (the worker round-trip would
/// re-trigger the same slow-tier failure — pointless).
#[nativelink_test]
async fn failed_slow_writes_self_retry_err_reinserts_for_next_tick() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        let (fss, fast_arc, slow_arc) = make_fss_with_gated_slow();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Pre-stage fast tier (V3 will TRY self-retry)...
        let digest = DigestInfo::try_new(VALID_HASH, 4).expect("valid digest");
        Pin::new(fast_arc.as_ref())
            .update_oneshot(StoreKey::Digest(digest), Bytes::from_static(b"data"))
            .await
            .expect("seed fast tier");

        // ...but mark the slow tier as failing.
        slow_arc.set_fail(true);

        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let stats = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        assert_eq!(stats.drained, 1, "stats={stats:?}");
        assert_eq!(
            stats.self_retry_failed, 1,
            "V3 fix: slow-tier transient failure during self-retry MUST count \
             toward self_retry_failed (NOT no_worker, NOT send_failed); \
             stats={stats:?}"
        );
        assert_eq!(stats.self_retried, 0, "stats={stats:?}");
        assert_eq!(stats.dispatched, 0, "stats={stats:?}");

        // The digest MUST be re-inserted so the next tick can retry.
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "V3 fix: slow-tier transient failure MUST re-insert into \
             failed_slow_writes — otherwise a transient outage permanently \
             loses the digest"
        );

        // M1 invariant.
        assert_eq!(
            stats.dispatched
                + stats.no_worker
                + stats.throttled
                + stats.send_failed
                + stats.self_retried
                + stats.self_retry_failed,
            stats.drained,
            "M1 invariant violated: counters do not add up to drained; \
             stats={stats:?}"
        );

        Ok::<(), Error>(())
    })
    .await
    .expect("V3 self-retry-err test must not deadlock")?;
    Ok(())
}
