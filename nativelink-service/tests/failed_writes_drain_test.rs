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
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, SizePartitioningSpec, StoreSpec,
    VerifySpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker,
};
use nativelink_service::failed_writes_drain::{
    DEFAULT_DRAIN_BATCH_SIZE, DEFAULT_DRAIN_COOLDOWN, DEFAULT_DRAIN_INFLIGHT_CAP,
    DEFAULT_SELF_RETRY_TIMEOUT, drain_tick,
};
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_store::small_blob_dispatcher::{SmallBlobDispatcher, SmallBlobDispatcherConfig};
use nativelink_store::verify_store::VerifyStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
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
            slow_writes_in_flight_max_bytes: 0,
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
        let dispatcher = Arc::new(SmallBlobDispatcher::new(
            SmallBlobDispatcherConfig::default(),
        ));

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
        let dispatcher = Arc::new(SmallBlobDispatcher::new(
            SmallBlobDispatcherConfig::default(),
        ));

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
        let dispatcher = Arc::new(SmallBlobDispatcher::new(
            SmallBlobDispatcherConfig::default(),
        ));

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
        let dispatcher = Arc::new(SmallBlobDispatcher::new(
            SmallBlobDispatcherConfig::default(),
        ));

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

        // dsr M3 closer: V3's headline contract advertises that the
        // self-retried digest is pushed to `stable_digests` so the BIS
        // broadcaster picks it up — without this, the durability
        // protocol that the rest of the cluster relies on (`mirror_blobs
        // ≥2-replica + BlobsInStableStorage ack`) would not see the
        // recovered blob, leaving downstream worker mirrors holding the
        // bytes forever (dead-letter on a different layer). This
        // assertion crosses the FSS → stable_digests seam that
        // `try_self_retry_slow_write:1784` writes; the BIS broadcaster
        // (`nativelink-service::*`) drains it on its tick.
        let stable = fss.drain_stable_digests();
        assert!(
            stable.contains(&digest),
            "V3 fix: successful self-retry MUST push the digest into \
             stable_digests so the BIS broadcaster picks it up — \
             missing means downstream worker-mirrors see no BIS ack and \
             keep the bytes pinned forever (durability invariant violated). \
             stable={stable:?}"
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

/// dsr M2 + code-reviewer + testing-czar BLOCK closer: production
/// composition descent test.
///
/// The four V3 tests above all wrap a *bare* `FastSlowStore` — they
/// exercise `drain_tick`'s self-retry branch but they never cross the
/// production wrapper chain
/// (`WorkerProxyStore` → `ExistenceCacheStore` → `VerifyStore` →
/// `SizePartitioningStore(16384)` → `FastSlowStore`).
/// The walker [`nativelink_store::wrapper_walker::find_fast_slow_via_chain`]
/// must pass [`synthetic_large_key()`] (a `u64::MAX`-sized
/// `DigestInfo`) into `inner_store(_)` so it descends
/// `SizePartitioningStore` into its upper arm — without that, the
/// walker terminates at the partitioning boundary and V3 self-retry
/// is structurally inactive for the production composition. This test
/// guards that contract bit-identically end-to-end.
///
/// Seams crossed (per dsr "name the seams" rule):
///   1. `Store::inner_store(Some(synthetic_large_key))` (drain_tick:300)
///   2. `find_fast_slow_via_chain` (`wrapper_walker.rs`):
///      - `WorkerProxyStore` (outermost in production cas_STORE chain;
///        its trait `inner_store` delegates to `self.inner.inner_store(key)`
///        so the synthetic key flows through transparently — testing-czar
///        MAJOR-2 closer: this test now crosses that delegation)
///      - `ExistenceCacheStore` downcast + recurse via typed
///        `inner_store()` accessor
///      - `VerifyStore` downcast + recurse via typed `inner_store()`
///      - `SizePartitioningStore::inner_store(Some(large_key))`
///        descends to upper arm (the FSS arm in production)
///      - `FastSlowStore` downcast → `Some(fss)`
///   3. `FastSlowStore::try_self_retry_slow_write` (the V3 entry point)
///   4. `FastSlowStore::stable_digests` push (BIS broadcast contract)
///
/// Mutation step (CLAUDE.md mandate, MAJOR-1 doc-fix): the load-bearing
/// site is NOT `failed_writes_drain.rs`'s `fss_for_store` build — that
/// call's `synthetic_large_key()` is silently recovered by the walker's
/// own internal `synthetic_large_key()` calls. The walker has resilient
/// recovery: ECS recursion (line 115), VS recursion (line 120), and the
/// generic-fallback (line 128) all pass `synthetic_large_key()`, and
/// any single one of them suffices to descend the production chain
/// because the previous hop's `None` falls through to the next downcast.
///
/// To physically demonstrate that this test guards the `synthetic_large_key`
/// pattern end-to-end, mutate ALL THREE `Some(synthetic_large_key())`
/// arguments inside `wrapper_walker.rs::find_fast_slow_via_chain`
/// (lines 115, 120, 128) to `None::<StoreKey>` simultaneously. With
/// every key argument set to None, every layer in the chain
/// (`SizePartitioningStore`, `ExistenceCacheStore`, `VerifyStore`)
/// returns `self` from `inner_store(None)`, the walker bottoms out at
/// the partitioning boundary, V3 self-retry is structurally bypassed,
/// the drainer counts `no_worker = 1` (empty locality map), and
/// `self_retried` stays at 0. The bespoke assertion below fires:
///   "V3 walker failed to descend production composition —
///    synthetic_large_key pattern broken: walker returned None for the
///    cas_INNER chain so self-retry was structurally inactive"
/// (Verified physically — see commit message; sed-style mutation is
/// `s/Some(synthetic_large_key())/None::<StoreKey>/g` inside
/// `wrapper_walker.rs`.)
#[nativelink_test]
async fn failed_slow_writes_v3_walker_descends_production_composition() -> Result<(), Error> {
    tokio::time::timeout(DRAIN_TIMEOUT, async {
        // Build the production CAS chain in miniature: ECS → VS →
        // SizePartitioning(16384) → upper: FSS{Memory + GatedSlow},
        // lower: trivial MemoryStore (small-blob path is irrelevant
        // for this test — the digest will route to upper).
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

        // Lower arm of SizePartitioning routes <16384-byte digests
        // (irrelevant for this test — the digest is large). It MUST
        // be a `FastSlowStore`, not a bare `MemoryStore`, because
        // `SizePartitioningStore::stable_delegation()` returns
        // `Many { children: [lower, upper] }` so on a re-insert
        // failure path the lower arm's `reinsert_failed_digests` is
        // also invoked. A bare `MemoryStore` declares
        // `StableDigestDelegation::Leaf` without overriding the
        // method → `debug_assert!` panic. The production
        // `SMALL_CAS_CACHED` is itself a `FastSlowStore`, so this
        // matches production shape.
        let lower_inner_fast =
            Store::new(MemoryStore::new(&MemorySpec::default()));
        let lower_inner_slow =
            Store::new(MemoryStore::new(&MemorySpec::default()));
        let lower_fss = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Memory(MemorySpec::default()),
                slow: StoreSpec::Memory(MemorySpec::default()),
                fast_direction: nativelink_config::stores::StoreDirection::default(),
                slow_direction: nativelink_config::stores::StoreDirection::default(),
                chunked_reads_enabled: false,
            },
            lower_inner_fast,
            lower_inner_slow,
        );
        let lower_dummy = Store::new(lower_fss);
        let upper_arm = Store::new(fss.clone());
        let size_part = SizePartitioningStore::new(
            &SizePartitioningSpec {
                size: 16384, // matches production `prod-server.json5` cas_INNER threshold
                lower_store: StoreSpec::Memory(MemorySpec::default()),
                upper_store: StoreSpec::Memory(MemorySpec::default()),
            },
            lower_dummy,
            upper_arm,
        );

        // VerifyStore wrap (production cas_STORE inserts VerifyStore
        // between ExistenceCacheStore and the size-partitioned inner).
        // For this test we put VS *between* ECS and SP since that's
        // how `cas_STORE → cas_INNER` reads in prod-server.json5
        // (cas_STORE = Verify{ backend: cas_INNER (ECS → SP) }).
        // Wrapping order chosen to match: outer ECS → VS → SP → FSS.
        let verify = VerifyStore::new(
            &VerifySpec {
                backend: StoreSpec::Memory(MemorySpec::default()),
                verify_size: false, // GatedSlow doesn't honor verify
                verify_hash: false,
            },
            Store::new(size_part),
        );

        // ExistenceCacheStore wrap (matches production cas_INNER's outer
        // ExistenceCacheStore).
        let ecs = ExistenceCacheStore::new(
            &ExistenceCacheSpec {
                backend: StoreSpec::Memory(MemorySpec::default()),
                eviction_policy: Some(EvictionPolicy {
                    max_count: 1024,
                    ..Default::default()
                }),
            },
            Store::new(verify),
        );

        // testing-czar MAJOR-2 closer: WorkerProxyStore wrap. The
        // production cas_STORE chain is
        // `WorkerProxyStore → ExistenceCacheStore → VerifyStore →
        // SizePartitioningStore → FastSlowStore`. Pre-fix this test
        // wrapped only ECS → VS → SP → FSS (4 of the 5 seams). WPS's
        // trait `inner_store` delegates to `self.inner.inner_store(key)`
        // today, so the synthetic key flows through transparently —
        // but if a future change shadowed WPS::inner_store to return
        // `self` (matching the ECS/VS pattern), the walker would
        // silently terminate at the WPS boundary and the V3 path
        // would re-open the liveness gap in production while every
        // other test stayed green. Wrapping WPS into the test now
        // forces the walker to cross every production seam.
        let locality_map = new_shared_blob_locality_map();
        let wps = WorkerProxyStore::new(Store::new(ecs), locality_map.clone());

        let cas_store_name = "cas_STORE_PROD";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(wps))];
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));
        // Worker IS connected but will not be picked because no entry
        // for the digest exists in the locality map — guarantees that
        // if the V3 walker fails to descend, `no_worker` increments
        // (NOT `dispatched`).
        let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker("grpc://test-worker:50071", 1, worker_tx);

        // Use a 32 KiB digest — strictly above the 16 384 partition
        // threshold so `SizePartitioningStore` routes to the upper
        // arm (the FSS). A digest <16384 would route lower and miss
        // the test seam entirely.
        let payload_size: u64 = 32 * 1024;
        let digest =
            DigestInfo::try_new(VALID_HASH, payload_size).expect("valid 32 KiB digest");
        let payload = Bytes::from(vec![0xab_u8; payload_size as usize]);

        // Pre-stage bytes in the FAST tier (matches the post-pin
        // production state after a slow-tier write failure).
        Pin::new(fast_arc.as_ref())
            .update_oneshot(StoreKey::Digest(digest), payload.clone())
            .await
            .expect("seed fast tier");

        // Mark the digest as failed so the drainer picks it up.
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

        // The load-bearing assertion: V3 walker MUST have descended
        // the full WPS-shaped chain and located the FSS, so the
        // self-retry branch fires (NOT the worker-fallback branch).
        // Bespoke message names the exact failure mode per CLAUDE.md
        // "specific .expect" rule.
        assert_eq!(
            stats.self_retried, 1,
            "V3 walker failed to descend production composition — \
             synthetic_large_key pattern broken: walker returned None \
             for the cas_INNER chain so self-retry was structurally \
             inactive (drainer fell through to no_worker fallback). \
             stats={stats:?}"
        );
        assert_eq!(
            stats.no_worker, 0,
            "production-composition test: when V3 walker descends \
             correctly and fast tier has bytes, drainer MUST self-retry \
             — no_worker > 0 means the walker bailed and the digest \
             missed the V3 path. stats={stats:?}"
        );
        assert_eq!(
            stats.dispatched, 0,
            "successful self-retry MUST NOT dispatch UploadMissingBlobs; \
             stats={stats:?}"
        );
        assert_eq!(stats.drained, 1, "stats={stats:?}");

        // Worker rx must remain empty — V3 succeeded without
        // dispatching an UploadMissingBlobs RPC.
        let recv_result = worker_rx.try_recv();
        assert!(
            recv_result.is_err(),
            "production-composition test: V3 success MUST NOT \
             dispatch UploadMissingBlobs (over-action regression); \
             got={recv_result:?}"
        );

        // BIS push assertion (dsr M3) — same as the headline test
        // but crossed via the production wrapper chain.
        let stable = fss.drain_stable_digests();
        assert!(
            stable.contains(&digest),
            "production-composition test: V3 self-retry MUST push the \
             digest into stable_digests for BIS broadcast — missing \
             means the durability invariant breaks for digests \
             recovered via the production-shaped chain. stable={stable:?}"
        );

        Ok::<(), Error>(())
    })
    .await
    .expect(
        "V3 production-composition test must not deadlock — walker \
         descent contract violated (synthetic_large_key pattern)",
    )?;
    Ok(())
}

// =============================================================
// BLOCK-2 + dsr MAJOR-1 fixtures: a slow store that blocks
// `update_oneshot` on a `tokio::sync::Notify` until released.
// Lets the test simulate a wedged slow tier (pre-fix: drainer
// hangs forever on the first failed digest) and assert the
// drainer makes progress within bounded wall-clock.
// =============================================================

/// Slow-store test fixture that blocks `update_oneshot` on a shared
/// `Notify` until `release()` is called. Used by:
///   - BLOCK-2 test: assert per-digest timeout fires under the
///     `DEFAULT_SELF_RETRY_TIMEOUT` budget (no indefinite block).
///   - dsr MAJOR-1 test: assert N concurrent slow-tier writes
///     overlap inside one tick rather than serialize.
#[derive(Debug)]
struct BlockingSlowStore {
    inner: Arc<MemoryStore>,
    gate: tokio::sync::Notify,
    /// When true, `update_oneshot` waits on `gate` before delegating
    /// to the inner store. Toggleable at runtime so a single fixture
    /// can simulate "wedged then released" within one test.
    block_updates: AtomicBool,
    /// Counter for assertions (how many times update_oneshot was
    /// entered — useful for catching a regression where the drainer
    /// stops calling try_self_retry_slow_write).
    update_attempts: core::sync::atomic::AtomicUsize,
}

impl BlockingSlowStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStore::new(&MemorySpec::default()),
            gate: tokio::sync::Notify::new(),
            block_updates: AtomicBool::new(true),
            update_attempts: core::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Release any pending update_oneshot waiters AND switch off
    /// further blocking so subsequent calls pass straight through.
    fn release(&self) {
        self.block_updates.store(false, Ordering::SeqCst);
        // notify_waiters wakes ALL currently parked waiters in one
        // call (versus notify_one which would require N calls for N
        // waiters and risks deadlock if call ordering varies).
        self.gate.notify_waiters();
    }

    fn update_attempts_count(&self) -> usize {
        self.update_attempts.load(Ordering::SeqCst)
    }
}

impl MetricsComponent for BlockingSlowStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[async_trait]
impl StoreDriver for BlockingSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .has_with_results(keys, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Streaming `update` path isn't on the V3 self-retry hot
        // path (FSS uses `update_oneshot` there); just delegate.
        Pin::new(self.inner.as_ref())
            .update(key, reader, size_info)
            .await
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
        self.update_attempts.fetch_add(1, Ordering::SeqCst);
        if self.block_updates.load(Ordering::SeqCst) {
            // Park indefinitely until `release()` is called. Without
            // a `tokio::time::timeout` wrapping this from the caller
            // (BLOCK-2 fix), the V3 drainer would wedge here
            // permanently on the first failed digest.
            self.gate.notified().await;
        }
        Pin::new(self.inner.as_ref())
            .update_oneshot(key, data)
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

default_health_status_indicator!(BlockingSlowStore);

/// Build an FSS with a `BlockingSlowStore` so tests can wedge the
/// slow tier on demand.
fn make_fss_with_blocking_slow() -> (
    Arc<FastSlowStore>,
    Arc<MemoryStore>,
    Arc<BlockingSlowStore>,
) {
    let fast_arc = MemoryStore::new(&MemorySpec::default());
    let slow_arc = BlockingSlowStore::new();
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

/// Red-team BLOCK-2 regression test: a wedged slow tier MUST NOT
/// block the V3 drainer indefinitely. The pre-fix code path
/// (`fast_slow_store.rs:1803-1811`'s `slow_store.update_oneshot`
/// without a `tokio::time::timeout`) blocked forever on the first
/// failed digest, the `failed_slow_writes` set grew without bound,
/// and the 120 s pin TTL eventually expired for everything else —
/// V3 bought ~zero seconds of recovery for the outage it was
/// designed to address.
///
/// Test shape:
///   1. Tick #1 with the slow tier wedged (Notify-blocked):
///      `try_self_retry_slow_write` MUST return `Code::DeadlineExceeded`
///      within `DEFAULT_SELF_RETRY_TIMEOUT`; the drainer counts it
///      as `self_retry_failed` and re-inserts the digest.
///   2. Release the slow tier (`slow_arc.release()`).
///   3. Tick #2 with the slow tier unblocked: the same digest
///      MUST self-retry successfully (`self_retried = 1`).
///
/// The whole test runs under a 15 s `tokio::time::timeout` deadlock
/// detector. With the BLOCK-2 fix, total wall-clock is ~2 s
/// (one timeout fires) + change. Without the fix the test wedges
/// at tick #1 and the deadlock detector triggers the bespoke
/// `.expect` message below.
#[nativelink_test]
async fn failed_slow_writes_v3_self_retry_bounded_by_timeout() -> Result<(), Error> {
    // Use a generous outer deadlock detector — the BLOCK-2 fix
    // bounds individual digests to `DEFAULT_SELF_RETRY_TIMEOUT`
    // (2 s in production); this test only enqueues 1 digest, so
    // worst-case wall-clock is ~2 s + overhead. 15 s is the
    // deadlock detector — without the BLOCK-2 fix the test would
    // hang forever on the parked Notify and this timeout fires.
    const OUTER_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(15);

    tokio::time::timeout(OUTER_DEADLOCK_TIMEOUT, async {
        let (fss, fast_arc, slow_arc) = make_fss_with_blocking_slow();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Pre-stage one digest in the fast tier so V3 will TRY
        // self-retry (the bug only manifests when the slow tier
        // gets called).
        let digest = DigestInfo::try_new(VALID_HASH, 4).expect("valid digest");
        Pin::new(fast_arc.as_ref())
            .update_oneshot(StoreKey::Digest(digest), Bytes::from_static(b"data"))
            .await
            .expect("seed fast tier");

        let inserter = fss.failed_writes_inserter();
        inserter(digest);

        // Tick #1: slow tier WEDGED. The drainer MUST come back
        // within `DEFAULT_SELF_RETRY_TIMEOUT + small overhead`.
        // Without the BLOCK-2 fix it never returns and the outer
        // timeout fires with the bespoke message.
        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let tick1_start = Instant::now();
        let stats1 = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;
        let tick1_elapsed = tick1_start.elapsed();

        assert_eq!(stats1.drained, 1, "stats1={stats1:?}");
        assert_eq!(
            stats1.self_retry_failed, 1,
            "BLOCK-2: a wedged slow tier MUST surface as \
             self_retry_failed (Code::DeadlineExceeded from the \
             timeout wrap), NOT block forever; stats1={stats1:?}"
        );
        assert_eq!(
            stats1.self_retried, 0,
            "BLOCK-2: slow tier was wedged so self-retry MUST NOT \
             succeed; stats1={stats1:?}"
        );
        assert!(
            tick1_elapsed < OUTER_DEADLOCK_TIMEOUT - Duration::from_secs(2),
            "BLOCK-2: drain_tick took {tick1_elapsed:?} (≥ deadlock \
             window) — V3 self-retry blocked indefinitely on slow tier \
             — missing tokio::time::timeout"
        );

        // Digest re-inserted for next tick (BLOCK-2 contract: a
        // transient outage MUST NOT lose the digest).
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "BLOCK-2: slow-tier timeout MUST re-insert the digest into \
             failed_slow_writes — otherwise a transient slow-tier wedge \
             permanently loses the digest"
        );

        // Reset cooldown so tick #2 isn't throttled. (Production's
        // 60 s cooldown would block the next tick; here we simulate
        // wall-clock advancing past the cooldown by clearing the map.)
        inflight.clear();

        // Release the slow tier.
        slow_arc.release();

        // Tick #2: slow tier RELEASED. Same digest MUST self-retry.
        let stats2 = drain_tick(
            &cas_stores,
            &locality_map,
            &dispatcher,
            &mut inflight,
            DEFAULT_DRAIN_COOLDOWN,
            DEFAULT_DRAIN_BATCH_SIZE,
            DEFAULT_DRAIN_INFLIGHT_CAP,
        )
        .await;

        assert_eq!(stats2.drained, 1, "stats2={stats2:?}");
        assert_eq!(
            stats2.self_retried, 1,
            "BLOCK-2: after slow-tier release, the re-inserted digest \
             MUST self-retry on the next tick (proves the timeout is a \
             transient surface, not a permanent loss); stats2={stats2:?}"
        );
        assert_eq!(stats2.self_retry_failed, 0, "stats2={stats2:?}");

        // BlockingSlowStore::update_oneshot was entered exactly twice
        // (once per tick) — proves the drainer kept calling
        // try_self_retry_slow_write rather than dropping the digest.
        assert_eq!(
            slow_arc.update_attempts_count(),
            2,
            "BLOCK-2: BlockingSlowStore should have been called \
             exactly 2× (once per tick) — got \
             {} attempts. Drainer dropped the digest after the timeout?",
            slow_arc.update_attempts_count()
        );

        // Sanity that the production timeout const is in fact short
        // enough to leave headroom inside the outer deadlock window.
        assert!(
            DEFAULT_SELF_RETRY_TIMEOUT < OUTER_DEADLOCK_TIMEOUT,
            "BLOCK-2 guard: DEFAULT_SELF_RETRY_TIMEOUT={DEFAULT_SELF_RETRY_TIMEOUT:?} \
             must be < OUTER_DEADLOCK_TIMEOUT={OUTER_DEADLOCK_TIMEOUT:?}",
        );

        Ok::<(), Error>(())
    })
    .await
    .expect(
        "V3 self-retry blocked indefinitely on slow tier — missing \
         tokio::time::timeout (BLOCK-2 regression: per-digest slow-tier \
         write must be bounded under DEFAULT_SELF_RETRY_TIMEOUT)",
    )?;
    Ok(())
}

/// dsr MAJOR-1 regression test: `drain_tick` MUST drive V3 self-
/// retries in parallel (FuturesUnordered + Semaphore) so a wedged
/// digest does not serialize against subsequent digests within the
/// same tick. Pre-fix the loop was sequential per-digest; with N
/// digests at the BLOCK-2 timeout (2 s) the worst-case per-tick
/// wall-clock was N × 2 s. With parallelism, only the first
/// `DEFAULT_SELF_RETRY_CONCURRENCY` (16) overlap inside the same
/// budget, capping per-tick wall-clock at roughly
/// `(N / 16) × DEFAULT_SELF_RETRY_TIMEOUT`.
///
/// Test shape: enqueue 10 digests with the slow tier wedged. With
/// sequential execution worst-case is 10 × 2 s = 20 s; with parallel
/// execution it's max(2 s, ceil(10/16) × 2 s) ≈ 2 s. Assert the
/// tick completes under a 5 s deadline — this margin separates the
/// two execution models. Mutation step: revert Phase B to the
/// inline `await` per-digest; the test red-fails with the bespoke
/// message below.
#[nativelink_test]
async fn failed_slow_writes_v3_self_retry_parallel_within_tick() -> Result<(), Error> {
    // 5 s budget separates parallel (~2 s) from sequential (~20 s).
    const PARALLEL_BUDGET: Duration = Duration::from_secs(5);
    // Whole-test deadlock detector — well above the parallel budget.
    const OUTER_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(30);
    const N_DIGESTS: usize = 10;

    tokio::time::timeout(OUTER_DEADLOCK_TIMEOUT, async {
        let (fss, fast_arc, slow_arc) = make_fss_with_blocking_slow();
        let cas_store_name = "cas_STORE_TEST";
        let cas_stores: Vec<(String, Store)> =
            vec![(cas_store_name.to_string(), Store::new(fss.clone()))];

        let locality_map = new_shared_blob_locality_map();
        let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

        // Pre-stage N unique digests in the fast tier and mark each
        // as failed. Each digest gets a unique hash so the FSS
        // self-retry path is exercised separately for each.
        let mut digests: Vec<DigestInfo> = Vec::with_capacity(N_DIGESTS);
        for i in 0..N_DIGESTS {
            let mut hash_bytes = [0u8; 32];
            hash_bytes[0] = (i as u8) + 1;
            let digest = DigestInfo::new(hash_bytes, 4);
            Pin::new(fast_arc.as_ref())
                .update_oneshot(StoreKey::Digest(digest), Bytes::from_static(b"data"))
                .await
                .expect("seed fast tier");
            digests.push(digest);
        }
        let inserter = fss.failed_writes_inserter();
        for d in &digests {
            inserter(*d);
        }

        // Drive one tick with the slow tier wedged. With parallel
        // execution all 10 timeouts run inside one
        // DEFAULT_SELF_RETRY_TIMEOUT window (~2 s); with sequential
        // execution they stack to ~20 s.
        let mut inflight: HashMap<DigestInfo, Instant> = HashMap::new();
        let tick_start = Instant::now();
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
        let tick_elapsed = tick_start.elapsed();

        assert_eq!(
            stats.drained,
            N_DIGESTS,
            "stats={stats:?}; expected {N_DIGESTS} drained"
        );
        assert_eq!(
            stats.self_retry_failed, N_DIGESTS,
            "BLOCK-2 + dsr MAJOR-1: every digest's slow-tier write must \
             time out (slow tier wedged); stats={stats:?}"
        );
        assert_eq!(stats.self_retried, 0, "stats={stats:?}");

        // The load-bearing assertion: per-tick wall-clock fits inside
        // the parallel budget. A sequential drain would take
        // N × DEFAULT_SELF_RETRY_TIMEOUT (~20 s) and blow this
        // budget — the assertion fires with the bespoke dsr MAJOR-1
        // regression message.
        assert!(
            tick_elapsed < PARALLEL_BUDGET,
            "dsr MAJOR-1 regression: drain_tick took {tick_elapsed:?} > \
             PARALLEL_BUDGET={PARALLEL_BUDGET:?} for {N_DIGESTS} wedged \
             digests — V3 self-retry serialized rather than running in \
             parallel via FuturesUnordered + Semaphore (each digest \
             paid the full DEFAULT_SELF_RETRY_TIMEOUT instead of \
             overlapping inside one budget)"
        );

        // BlockingSlowStore::update_oneshot was entered N times in
        // parallel — every digest got a chance, none was dropped.
        assert_eq!(
            slow_arc.update_attempts_count(),
            N_DIGESTS,
            "dsr MAJOR-1: every digest's slow-tier write must be \
             attempted in parallel; got {} attempts for {N_DIGESTS} \
             digests",
            slow_arc.update_attempts_count()
        );

        // All digests re-inserted for the next tick.
        for d in &digests {
            assert!(
                fss.failed_slow_writes_contains(d),
                "dsr MAJOR-1: digest {d:?} must be re-inserted into \
                 failed_slow_writes after slow-tier timeout"
            );
        }

        Ok::<(), Error>(())
    })
    .await
    .expect(
        "dsr MAJOR-1 regression: drain_tick must drive V3 self-retries \
         in parallel via FuturesUnordered + Semaphore — N × \
         DEFAULT_SELF_RETRY_TIMEOUT is unbounded aggregate per tick",
    )?;
    Ok(())
}
