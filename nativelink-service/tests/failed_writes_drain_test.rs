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
//! Both tests run under `tokio::time::timeout(few seconds)` as a
//! deadlock detector + use bespoke `.expect` messages per CLAUDE.md.

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
