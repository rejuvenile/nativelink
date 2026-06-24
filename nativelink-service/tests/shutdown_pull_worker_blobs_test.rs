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

//! Directive-2 (#58: server durability bundle) shutdown WORKER-PULL phase —
//! production-composition tests for
//! `WorkerApiServer::pull_all_worker_blobs_at_shutdown`.
//!
//! The pull REUSES the existing worker-push backfill
//! (`request_missing_blob_uploads`) driven to COMPLETION by a poll loop, so
//! the test composes the real seams:
//!
//!   locality_map (enumeration source)
//!     → `SmallBlobDispatcher::connected_workers_with_senders` (transport)
//!     → `UploadMissingBlobs` proto over the worker `worker_tx`
//!     → [FAKE worker emulating `handle_upload_missing_blobs`: writes the blob
//!        into the server CAS — the exact effect the real worker's
//!        `slow_store.update` produces]
//!     → server CAS `has_with_results` (completion detection).
//!
//! The fake worker stands in for `LocalWorker::handle_upload_missing_blobs`
//! (`local_worker.rs:2452`): on each `UploadMissingBlobs` it writes the
//! requested blobs into the server CAS, which is exactly what the worker's
//! `slow_store.update` → server gRPC CAS write does in production. We do NOT
//! fake the SERVER side — the locality enumeration, the dispatcher transport,
//! the existence check, and the completion loop are all the real code.
//!
//! Mutation guide (TDD step 5) is in each test's doc-comment.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::small_blob_dispatcher::{SmallBlobDispatcher, SmallBlobDispatcherConfig};
use nativelink_config::stores::MemorySpec;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::{SharedBlobLocalityMap, new_shared_blob_locality_map};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use tokio::sync::Notify;

const DEADLOCK_DETECTOR: Duration = Duration::from_secs(8);
const SCHEDULER_NAME: &str = "SHUTDOWN_PULL_TEST_SCHEDULER";
const BASE_WORKER_TIMEOUT_S: u64 = 100;
const HASH_A: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH_B: &str = "fedcba9876543210000000000000000000000000000000000fedcba987654321";

#[expect(clippy::unnecessary_wraps, reason = "WorkerApiServer expects a fallible time fn")]
const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(10))
}

// ----- Minimal scheduler-satisfying mock (never invoked by the pull) -----

#[derive(MetricsComponent)]
struct MockWorkerStateManager {
    #[metric(help = "unused")]
    _unused: u64,
}

#[async_trait::async_trait]
impl WorkerStateManager for MockWorkerStateManager {
    async fn update_operation(
        &self,
        _operation_id: &OperationId,
        _worker_id: &WorkerId,
        _update: UpdateOperationType,
    ) -> Result<(), Error> {
        unreachable!("pull phase does not invoke update_operation")
    }
}

/// Build a `WorkerApiServer` wired with a real CAS store, a real locality map,
/// and a real `SmallBlobDispatcher` — the three handles the pull phase reads.
/// Returns the server plus those handles so the test can register fake workers
/// and assert on the CAS.
fn make_server(
    cas_store: Store,
) -> Result<(Arc<WorkerApiServer>, SharedBlobLocalityMap, Arc<SmallBlobDispatcher>), Error> {
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager { _unused: 0 });
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager,
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();
    let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler);
    let server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map.clone()),
        Some(cas_store),
        None,
        Some(dispatcher.clone()),
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;
    Ok((Arc::new(server), locality_map, dispatcher))
}

/// Register a fake connected worker that emulates
/// `LocalWorker::handle_upload_missing_blobs`: it consumes
/// `UpdateForWorker::UploadMissingBlobs` messages off its `worker_tx`
/// receiver and writes each requested digest's bytes into `cas_store` — the
/// same effect the real worker's `slow_store.update` produces. `blobs` maps
/// every digest the worker can serve to its bytes.
///
/// Registers the worker in BOTH the dispatcher (transport) and the locality
/// map (enumeration source) under `endpoint`, mirroring production's
/// `register_worker` + `register_blobs` at connect / BlobsAvailable.
fn register_uploading_worker(
    locality_map: &SharedBlobLocalityMap,
    dispatcher: &SmallBlobDispatcher,
    cas_store: &Store,
    endpoint: &str,
    boot_epoch: u64,
    blobs: Vec<(DigestInfo, Bytes)>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<UpdateForWorker>();
    dispatcher.register_worker(endpoint, boot_epoch, tx);

    let digests: Vec<DigestInfo> = blobs.iter().map(|(d, _)| *d).collect();
    locality_map.write().register_blobs(endpoint, &digests);

    let blob_map: HashMap<DigestInfo, Bytes> = blobs.into_iter().collect();
    let cas = cas_store.clone();
    nativelink_util::background_spawn!("fake_worker_upload_handler", async move {
        while let Some(msg) = rx.recv().await {
            if let Some(update_for_worker::Update::UploadMissingBlobs(req)) = msg.update {
                for proto_digest in req.digests {
                    let Ok(digest) = DigestInfo::try_from(proto_digest) else {
                        continue;
                    };
                    if let Some(bytes) = blob_map.get(&digest) {
                        // Emulates the worker's slow_store.update → server CAS
                        // write. The blob lands durably in the server CAS.
                        drop(cas.update_oneshot(digest, bytes.clone()).await);
                    }
                }
            }
        }
    });
}

/// Register a worker in the LOCALITY MAP only — NOT in the dispatcher. Models
/// a worker that BlobsAvailable-advertised a digest but is no longer
/// connected (its `worker_tx` is gone): a zero-connected-source blob.
fn register_disconnected_source(
    locality_map: &SharedBlobLocalityMap,
    endpoint: &str,
    digest: DigestInfo,
) {
    locality_map.write().register_blobs(endpoint, &[digest]);
}

async fn server_has(cas_store: &Store, digest: DigestInfo) -> Option<u64> {
    let keys = [StoreKey::from(digest)];
    let mut results = [None];
    cas_store
        .has_with_results(&keys, &mut results)
        .await
        .expect("has_with_results must not error");
    results[0]
}

fn digest_a() -> DigestInfo {
    DigestInfo::try_new(HASH_A, 11).expect("valid digest A")
}
fn digest_b() -> DigestInfo {
    DigestInfo::try_new(HASH_B, 13).expect("valid digest B")
}

// ===================================================================
// Tests.
// ===================================================================

/// CORE durability invariant (design §10.2 proving test): a fake worker holds
/// a blob the server LACKS; the shutdown-pull future MUST NOT resolve until
/// that blob is durable in the server CAS. After the pull resolves,
/// `has_with_results` reports the blob present and `at_risk_skipped == 0`.
///
/// Mutation A (skip the pull): make `pull_all_worker_blobs_at_shutdown` return
/// immediately without sending `UploadMissingBlobs` → the blob never lands →
/// the post-pull `has` is None → red-fail with the bespoke message.
/// Mutation B (fire-and-forget, no completion loop): send the upload but
/// return without the `has_with_results` re-poll → the future could resolve
/// before the (async) worker upload lands → flaky/absent → red-fail.
#[nativelink_test]
async fn shutdown_pull_lands_worker_only_blob_on_local_disk_before_future_resolves()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (server, locality_map, dispatcher) = make_server(cas_store.clone())?;

    let digest = digest_a();
    let data = Bytes::from_static(b"hello-world");
    assert_eq!(data.len() as u64, digest.size_bytes(), "fixture self-check");

    // Pre-condition: the server does NOT have the blob.
    assert!(
        server_has(&cas_store, digest).await.is_none(),
        "pre-condition: the blob must be worker-only (absent from server CAS)"
    );

    register_uploading_worker(
        &locality_map,
        &dispatcher,
        &cas_store,
        "grpc://worker-a:50071",
        1,
        vec![(digest, data.clone())],
    );

    let summary = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        server.pull_all_worker_blobs_at_shutdown(),
    )
    .await
    .expect(
        "shutdown pull must localize the worker-only blob before resolving — \
         a hang here means the completion loop never observed the upload landing",
    );

    // The future resolved ONLY after the blob is durable in the server CAS.
    assert_eq!(
        server_has(&cas_store, digest).await,
        Some(digest.size_bytes()),
        "shutdown pull must localize worker-only blob before resolving — the \
         blob must be present in the server CAS when the pull future resolves \
         (mutation: skip the pull, or drop the has_with_results completion poll)"
    );
    assert_eq!(
        summary.at_risk_skipped, 0,
        "a reachable worker held the blob — nothing should be at-risk-skipped"
    );
    assert_eq!(summary.pulled, 1, "exactly one worker-only blob was pulled");
    Ok(())
}

/// UNREACHABLE policy (design §5): a digest whose only locality-worker is NOT
/// connected (zero connected source) must NOT wedge shutdown. The pull future
/// resolves within the deadline, the blob stays absent, and the summary
/// records `at_risk_skipped == 1`.
///
/// Mutation (wait forever on zero-source): make the skip-policy keep the digest
/// in the residual instead of escalating to at-risk → the loop never
/// terminates → the `tokio::time::timeout` fires → red-fail with the bespoke
/// "must not wedge" message.
#[nativelink_test]
async fn shutdown_pull_skips_zero_source_blob_and_does_not_hang()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (server, locality_map, _dispatcher) = make_server(cas_store.clone())?;

    let digest = digest_a();
    // The worker advertised it in the locality map but is NOT connected.
    register_disconnected_source(&locality_map, "grpc://gone:50071", digest);

    let summary = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        server.pull_all_worker_blobs_at_shutdown(),
    )
    .await
    .expect(
        "shutdown pull MUST NOT wedge on a zero-connected-source blob — a hang \
         here means the loop spun forever instead of at-risk-skipping the \
         unpullable digest (mutation: keep zero-source digests in the residual)",
    );

    assert_eq!(
        summary.at_risk_skipped, 1,
        "the zero-connected-source blob must be at-risk-skipped, not pulled"
    );
    assert_eq!(summary.pulled, 0, "nothing was pullable");
    assert!(
        server_has(&cas_store, digest).await.is_none(),
        "an unpullable blob cannot become present — it has no connected source"
    );
    Ok(())
}

/// MIXED set (design §5 + completion loop): one reachable worker holds blob A,
/// blob B has only a disconnected source. The pull must localize A AND
/// at-risk-skip B, then resolve. Proves the loop drains the pullable residual
/// to empty while not wedging on the unpullable one.
///
/// Mutation (escalate the whole residual when ANY digest is zero-source):
/// if the loop bailed out of the entire pull the moment it saw B was
/// unpullable, A would never land → `has(A)` None → red-fail.
#[nativelink_test]
async fn shutdown_pull_localizes_reachable_and_skips_unreachable()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (server, locality_map, dispatcher) = make_server(cas_store.clone())?;

    let a = digest_a();
    let b = digest_b();
    register_uploading_worker(
        &locality_map,
        &dispatcher,
        &cas_store,
        "grpc://worker-a:50071",
        1,
        vec![(a, Bytes::from_static(b"hello-world"))],
    );
    register_disconnected_source(&locality_map, "grpc://gone:50071", b);

    let summary = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        server.pull_all_worker_blobs_at_shutdown(),
    )
    .await
    .expect("mixed pull must resolve — reachable A localized, unreachable B skipped");

    assert_eq!(
        server_has(&cas_store, a).await,
        Some(a.size_bytes()),
        "the reachable blob A MUST be localized even though B is unpullable \
         (mutation: bailing the whole residual on the first zero-source digest)"
    );
    assert!(
        server_has(&cas_store, b).await.is_none(),
        "the unreachable blob B cannot be localized"
    );
    assert_eq!(summary.pulled, 1, "A pulled");
    assert_eq!(summary.at_risk_skipped, 1, "B at-risk-skipped");
    Ok(())
}

/// ALREADY-PRESENT short-circuit: a blob the server ALREADY holds (e.g. the
/// directive-1 flush already persisted it) is NOT re-pulled. The pull resolves
/// with `pulled == 0` and never contacts the worker. Proves the existence
/// check gates the enumeration (design §4.2) so the pull does not generate
/// fleet-wide redundant re-upload traffic for blobs the server has.
#[nativelink_test]
async fn shutdown_pull_skips_blobs_already_present_on_server()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (server, locality_map, dispatcher) = make_server(cas_store.clone())?;

    let digest = digest_a();
    let data = Bytes::from_static(b"hello-world");
    // Server already holds it (directive-1 flush already persisted it).
    cas_store.update_oneshot(digest, data.clone()).await?;
    assert_eq!(
        server_has(&cas_store, digest).await,
        Some(digest.size_bytes()),
        "pre-condition: server already holds the blob"
    );

    // The worker also advertises it, but the pull must NOT re-request it.
    register_uploading_worker(
        &locality_map,
        &dispatcher,
        &cas_store,
        "grpc://worker-a:50071",
        1,
        vec![(digest, data)],
    );

    let summary = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        server.pull_all_worker_blobs_at_shutdown(),
    )
    .await
    .expect("pull over an all-present set must resolve immediately");

    assert_eq!(
        summary.pulled, 0,
        "a blob already present on the server must NOT be re-pulled (existence \
         check gates the enumeration)"
    );
    assert_eq!(summary.at_risk_skipped, 0, "nothing at-risk");
    Ok(())
}
