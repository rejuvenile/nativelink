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
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::small_blob_dispatcher::{SmallBlobDispatcher, SmallBlobDispatcherConfig};
use nativelink_store::store_manager::StoreManager;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
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

/// Register a fake connected worker whose `UploadMissingBlobs` handler lands
/// each requested blob in the server FSS's FAST tier AND seeds the FSS
/// `in_flight_slow_writes` map (via `test_insert_in_flight`) — WITHOUT a durable
/// slow-tier write. This reproduces the EXACT production in-flight illusion the
/// post-pull flush exists for: the production worker push goes through the FSS
/// `update` path, which writes the fast tier + records an `in_flight_slow_writes`
/// entry + SPAWNS the slow write async. At pull-completion time the FSS
/// `has_with_results` returns `Some` from that in-flight entry (NOT from the
/// durable slow tier — see `fast_slow_store.rs` `has_with_results` server arm),
/// so the pull declares the blob "pulled" while the slow tier is still empty.
/// Seeding the in-flight map directly (instead of letting the spawn race) makes
/// the "has==Some but slow tier empty" state DETERMINISTIC.
fn register_in_flight_only_worker(
    locality_map: &SharedBlobLocalityMap,
    dispatcher: &SmallBlobDispatcher,
    fss: &Arc<FastSlowStore>,
    fast_tier: &Store,
    endpoint: &str,
    boot_epoch: u64,
    blobs: Vec<(DigestInfo, Bytes)>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<UpdateForWorker>();
    dispatcher.register_worker(endpoint, boot_epoch, tx);

    let digests: Vec<DigestInfo> = blobs.iter().map(|(d, _)| *d).collect();
    locality_map.write().register_blobs(endpoint, &digests);

    let blob_map: HashMap<DigestInfo, Bytes> = blobs.into_iter().collect();
    let fast = fast_tier.clone();
    let fss = fss.clone();
    nativelink_util::background_spawn!("fake_worker_in_flight_upload", async move {
        while let Some(msg) = rx.recv().await {
            if let Some(update_for_worker::Update::UploadMissingBlobs(req)) = msg.update {
                for proto_digest in req.digests {
                    let Ok(digest) = DigestInfo::try_from(proto_digest) else {
                        continue;
                    };
                    if let Some(bytes) = blob_map.get(&digest) {
                        // Fast tier resident + in-flight recorded, NO slow write:
                        // the durable slow-tier write has not happened (the
                        // production spawn would still be racing process exit).
                        drop(fast.update_oneshot(digest, bytes.clone()).await);
                        fss.test_insert_in_flight(
                            StoreKey::from(digest).into_owned(),
                            vec![bytes.clone()],
                        );
                    }
                }
            }
        }
    });
}

/// DURABILITY (red-team RECONSIDER + code-reviewer fix-up, item A): a pulled
/// blob is "present" per the FSS `has_with_results` (the pull's completion
/// check) while it is only in the VOLATILE fast tier — the durable slow-tier
/// write has not happened. The shutdown sequence must NOT consider the pull
/// "done" until a post-pull flush has driven those pull-landed blobs to the
/// SLOW tier. This test composes the pull's CAS as a production-shape
/// `FastSlowStore{fast: MemoryStore, slow: MemoryStore-probe}` registered in a
/// `StoreManager`; a fake worker pushes the blob into the FAST tier only; the
/// pull resolves seeing `has == Some`; then the EXACT post-pull barrier the bin
/// runs (`StoreManager::flush_slow_writes`, whose unbounded Phase-2 drains
/// fast→slow) makes the blob durable on the SLOW tier.
///
/// This crosses the seam red-team flagged: the pull trusts `has == Some`, which
/// the FSS satisfies from the in-flight/fast tier; only the post-pull flush
/// confirms durability. The smallack path proves the "await the durable write"
/// pattern for ≤16 KiB; this asserts the large/chunked class gets the same
/// guarantee via the Phase-3.6 flush.
///
/// Mutation (skip the post-pull flush — comment out the
/// `store_manager.flush_slow_writes(...)` line): the slow tier stays EMPTY when
/// the shutdown sequence would persist+evict+exit → the final
/// `slow_probe.has(...)` is None → red-fail with the bespoke message. (Mirrors
/// removing the Phase-3.6 call from `nativelink.rs`.)
#[nativelink_test]
async fn post_pull_flush_makes_pulled_blob_durable_on_slow_tier()
-> Result<(), Box<dyn core::error::Error>> {
    // Production-shape server CAS: FastSlowStore{fast=MemoryStore, slow=probe}.
    let fast_tier = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_probe = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_tier.clone(),
        slow_probe.clone(),
    );
    let cas_store = Store::new(fss.clone());
    let (server, locality_map, dispatcher) = make_server(cas_store.clone())?;

    // Register the FSS-backed CAS in a StoreManager so the bin's post-pull
    // `flush_slow_writes` walker finds it (the EXACT production wiring).
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store("cas_STORE", cas_store.clone());

    let digest = digest_a();
    let data = Bytes::from_static(b"hello-world");
    assert_eq!(data.len() as u64, digest.size_bytes(), "fixture self-check");

    // Pre-condition: neither tier holds the blob.
    assert!(
        server_has(&cas_store, digest).await.is_none(),
        "pre-condition: blob absent from the FSS"
    );
    assert!(
        slow_probe.has(digest).await?.is_none(),
        "pre-condition: slow tier empty"
    );

    // The worker holds it; on pull it lands the blob fast-tier + in-flight
    // (NO durable slow write) — the production in-flight illusion.
    register_in_flight_only_worker(
        &locality_map,
        &dispatcher,
        &fss,
        &fast_tier,
        "grpc://worker-a:50071",
        1,
        vec![(digest, data.clone())],
    );

    // Phase 3: the pull. Resolves when the FSS `has` reports the blob present —
    // which is satisfied by the FAST tier (the in-flight illusion).
    let summary = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        server.pull_all_worker_blobs_at_shutdown(),
    )
    .await
    .expect("shutdown pull must resolve once the fast tier holds the blob");
    assert_eq!(summary.pulled, 1, "the worker-only blob was pulled");
    assert_eq!(summary.at_risk_skipped, 0, "nothing at-risk");

    // The illusion: the FSS reports the blob present, but the SLOW tier is still
    // empty — the durable write has NOT happened. THIS is why has==Some is not a
    // durability proof.
    assert_eq!(
        server_has(&cas_store, digest).await,
        Some(digest.size_bytes()),
        "FSS has reports the pulled blob present (fast tier) — the pull's \
         completion check is satisfied"
    );
    assert!(
        slow_probe.has(digest).await?.is_none(),
        "the pulled blob is NOT yet durable on the slow tier — has==Some is the \
         in-flight/fast-tier illusion, not durability"
    );

    // Phase 3.6: the post-pull DURABILITY barrier (the bin runs exactly this).
    // Mutation point: comment out this line → the slow tier stays empty.
    //
    // The `flush_budget` here bounds ONLY the Phase-1 in-flight drain (the
    // synthetic in-flight entry never completes in this fake, so Phase 1 times
    // out — a SHORT budget keeps the test fast); the Phase-2 fast→slow drain it
    // runs is unbounded (`None`) and is what makes the blob durable.
    tokio::time::timeout(
        DEADLOCK_DETECTOR,
        store_manager.flush_slow_writes(Duration::from_millis(200)),
    )
    .await
    .expect("post-pull flush must not deadlock");

    // THE LOAD-BEARING ASSERTION: the pulled blob is now DURABLE on the slow
    // tier. Without the post-pull flush this is None (the pull abandoned the
    // racing async slow write at exit).
    let durable = slow_probe.get_part_unchunked(digest, 0, None).await;
    let bytes = durable.expect(
        "post-pull durability flush MUST drive the pull-landed blob to the SLOW \
         tier before persist/eviction/exit — has==Some at pull time is NOT \
         durability (mutation: skip the post-pull flush_slow_writes call)",
    );
    assert_eq!(
        bytes.as_ref(),
        data.as_ref(),
        "the slow-tier bytes must match the pulled blob"
    );
    Ok(())
}
