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

use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
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
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
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
        );

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
        );

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
        );

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
        );

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

        // M1 invariant: counters add up to drained.
        assert_eq!(
            stats.dispatched + stats.no_worker + stats.throttled + stats.send_failed,
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
