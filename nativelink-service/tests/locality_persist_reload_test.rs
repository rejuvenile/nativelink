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

//! Directive-3 (#58: server durability bundle) persist-locality-map +
//! readiness-gated reload — production-composition tests for
//! `WorkerApiServer::{persist_locality_to_disk, reload_locality_from_disk,
//! sweep_unconfirmed_reloaded_locality}`.
//!
//! These tests compose the REAL seams the reload reconciliation depends on:
//!
//!   reload file → `reload_locality_from_disk` → primes BOTH `locality_map`
//!     (digest → endpoint) AND `endpoint_state` (endpoint → boot_epoch + a
//!     sentinel owner)
//!     → the EXISTING #141 connect-path wipe (`inner_connect_worker`) reconciles
//!       a reconnecting worker against the persisted boot_epoch.
//!
//! The priming-trap guard (design §4.5): if reload primed ONLY `locality_map`
//! and not `endpoint_state`, a rebooted worker (different boot_epoch) would have
//! `prev == None` on reconnect, `needs_wipe == false`, and its stale persisted
//! entries would LEAK. The reconciliation test reconnects with a DIFFERENT
//! boot_epoch and asserts the stale entry is GONE — which can only happen if
//! `endpoint_state` was primed.
//!
//! Mutation guide (TDD step 5) is in each test's doc-comment.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::ConnectWorkerRequest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForScheduler;
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::{
    PersistedEndpoint, PersistedLocalityMap, SharedBlobLocalityMap, new_shared_blob_locality_map,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use tokio::sync::Notify;

const DEADLOCK_DETECTOR: Duration = Duration::from_secs(8);
const SCHEDULER_NAME: &str = "LOCALITY_PERSIST_TEST_SCHEDULER";
const BASE_WORKER_TIMEOUT_S: u64 = 100;

#[expect(clippy::unnecessary_wraps, reason = "WorkerApiServer expects a fallible time fn")]
const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(1_000))
}

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
        unreachable!("locality persist tests do not invoke update_operation")
    }
}

/// Build a `WorkerApiServer` wired with a real locality map — the handle the
/// reload repopulates and the connect path reconciles. Returns the server plus
/// the shared locality map so the test can assert reloaded entries resolve and
/// reconcile.
fn make_server() -> Result<(WorkerApiServer, SharedBlobLocalityMap), Error> {
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
        None,
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;
    Ok((server, locality_map))
}

/// Drive a real `connect_worker` handshake for `cas_endpoint` with the given
/// boot_epoch, exercising the production #141 wipe path. Returns the sender +
/// stream so the caller can keep the connection open (or drop it).
async fn open_worker_connection(
    server: &WorkerApiServer,
    cas_endpoint: &str,
    boot_epoch_id: u64,
) -> Result<
    (
        tokio::sync::mpsc::Sender<Update>,
        impl futures::Stream<Item = Result<
            nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForWorker,
            tonic::Status,
        >>,
    ),
    Error,
> {
    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        boot_epoch_id,
        ..Default::default()
    };
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut stream = server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();
    // Consume the ConnectionResult so the caller sees post-handshake state
    // (the #141 wipe has fired by this point).
    let first = stream
        .next()
        .await
        .err_tip(|| "expected ConnectionResult")?
        .err_tip(|| "stream error before ConnectionResult")?
        .update
        .err_tip(|| "ConnectionResult update missing")?;
    assert!(
        matches!(first, update_for_worker::Update::ConnectionResult(_)),
        "first update must be ConnectionResult, got {first:?}"
    );
    Ok((tx, stream))
}

fn digest(byte: u8, size: u64) -> DigestInfo {
    DigestInfo::new([byte; 32], size)
}

/// Build an on-disk persist-file bytes for the given endpoints. Mirrors what a
/// graceful shutdown would write at Phase 3.5.
fn persisted_bytes(entries: Vec<(&str, u64, Vec<DigestInfo>)>) -> Vec<u8> {
    let persisted = PersistedLocalityMap::new(
        1_000,
        entries
            .into_iter()
            .map(|(ep, epoch, digests)| PersistedEndpoint {
                cas_endpoint: ep.to_string(),
                boot_epoch: epoch,
                digests,
            })
            .collect(),
    );
    persisted
        .serialize_to_bytes()
        .expect("serialize persisted locality map")
}

async fn write_persist_file(bytes: &[u8]) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nl-dir3-test-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    tokio::fs::create_dir_all(&dir)
        .await
        .expect("create test persist dir");
    let path = dir.join("locality-map.bin");
    tokio::fs::write(&path, bytes)
        .await
        .expect("write test persist file");
    path
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ===================================================================
// Tests.
// ===================================================================

/// Round-trip the persist format: serialize a map, deserialize it, get the
/// same entries back. The header magic + version must validate.
///
/// Mutation (corrupt the magic): flip a magic byte → deserialize must Err with
/// the bespoke "bad magic" message, NOT silently decode garbage.
#[nativelink_test]
async fn persist_format_round_trips() -> Result<(), Box<dyn core::error::Error>> {
    let d1 = digest(0xA1, 100);
    let d2 = digest(0xA2, 200);
    let bytes = persisted_bytes(vec![("grpc://w-a:50081", 7, vec![d1, d2])]);

    let decoded = PersistedLocalityMap::deserialize_from_bytes(&bytes)
        .expect("round-trip decode must succeed for a well-formed file");
    assert_eq!(decoded.entries.len(), 1, "one endpoint");
    assert_eq!(decoded.entries[0].cas_endpoint, "grpc://w-a:50081");
    assert_eq!(decoded.entries[0].boot_epoch, 7);
    assert_eq!(decoded.entries[0].digests, vec![d1, d2]);

    // Corrupt the magic → must Err, not decode garbage.
    let mut corrupt = bytes.clone();
    corrupt[0] ^= 0xFF;
    let err = PersistedLocalityMap::deserialize_from_bytes(&corrupt)
        .expect_err("a corrupt magic must be rejected, not silently decoded");
    assert!(
        format!("{err}").contains("magic"),
        "deserialize must reject a bad magic with a bespoke message — got: {err}"
    );
    Ok(())
}

/// CORE invariant (design §8.2 proving test, part b): a reloaded locality entry
/// makes the blob FINDABLE — `lookup_workers` resolves the persisted endpoint
/// immediately after reload, WITHOUT any worker reconnect / BlobsAvailable
/// re-backfill.
///
/// Mutation (skip the reload entirely): if `reload_locality_from_disk` returns
/// without repopulating `locality_map`, the lookup is empty → red-fail with the
/// bespoke message.
#[nativelink_test]
async fn reloaded_entry_is_findable_without_rebackfill()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let d = digest(0xD1, 100);
    let bytes = persisted_bytes(vec![("grpc://w-a:50081", 42, vec![d])]);
    let path = write_persist_file(&bytes).await;

    // Pre-condition: the live map is empty (fresh boot).
    assert!(
        locality_map.read().lookup_workers(&d).is_empty(),
        "pre-condition: fresh map has no entry for the persisted digest"
    );

    let summary = tokio::time::timeout(DEADLOCK_DETECTOR, server.reload_locality_from_disk(&path))
        .await
        .expect("reload must not hang")
        .expect("reload of a well-formed file must succeed");
    assert_eq!(summary.endpoints_loaded, 1, "one endpoint reloaded");
    assert_eq!(summary.digests_loaded, 1, "one digest reloaded");

    // The reloaded entry resolves the lookup with ZERO worker re-backfill.
    let workers = locality_map.read().lookup_workers(&d);
    assert_eq!(
        workers.len(),
        1,
        "the reloaded locality entry must resolve lookup_workers WITHOUT any \
         worker reconnect or BlobsAvailable re-backfill (mutation: skip the \
         reload — the map stays empty)"
    );
    assert_eq!(&*workers[0], "grpc://w-a:50081");
    Ok(())
}

/// THE PRIMING-TRAP GUARD (design §4.5, the single biggest risk): a worker that
/// rebooted (reconnects with a DIFFERENT boot_epoch than the persisted one)
/// must have its stale reloaded entries WIPED — and this only happens if reload
/// primed `endpoint_state` with the persisted boot_epoch so the existing #141
/// connect-path wipe sees `prev == Some(persisted_epoch)` and fires.
///
/// Mutation (skip priming `endpoint_state` at reload): on reconnect `prev` is
/// `None`, `needs_wipe` is false, the #141 wipe never fires, and the stale
/// persisted entry LEAKS → this test red-fails with the bespoke "stale
/// persisted entry leaked — endpoint_state not primed, #141 wipe never fired".
#[nativelink_test]
async fn reloaded_entry_wiped_on_boot_epoch_mismatch_via_primed_endpoint_state()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let endpoint = "grpc://w-reboot:50081";
    let stale = digest(0xE1, 100);
    // Persisted at boot_epoch 1111.
    let bytes = persisted_bytes(vec![(endpoint, 1111, vec![stale])]);
    let path = write_persist_file(&bytes).await;

    tokio::time::timeout(DEADLOCK_DETECTOR, server.reload_locality_from_disk(&path))
        .await
        .expect("reload must not hang")
        .expect("reload must succeed");

    // The reloaded (stale) entry is present right after reload.
    assert_eq!(
        locality_map.read().lookup_workers(&stale).len(),
        1,
        "reloaded stale entry present immediately after reload"
    );

    // The worker REBOOTED: it reconnects with a DIFFERENT boot_epoch (2222).
    // The #141 wipe must fire on registration BECAUSE reload primed
    // endpoint_state[endpoint].boot_epoch = 1111 (prev = Some(1111) != 2222).
    let (_tx, _stream) = open_worker_connection(&server, endpoint, 2222).await?;

    // The stale persisted entry MUST be gone — wiped by the #141 path.
    assert!(
        locality_map.read().lookup_workers(&stale).is_empty(),
        "stale persisted entry leaked — endpoint_state not primed, #141 wipe \
         never fired (the rebooted worker reconnected with a different \
         boot_epoch, so its persisted entries from the prior boot MUST be \
         wiped). Found: {:?}",
        locality_map.read().lookup_workers(&stale)
    );
    Ok(())
}

/// COMPLEMENT to the trap guard: a worker reconnecting with the SAME boot_epoch
/// as persisted (transient drop, same process) keeps its reloaded entries —
/// they cover the reconnect→first-snapshot window. Proves the priming does not
/// over-wipe the common same-epoch reconnect.
///
/// Mutation (prime with epoch 0 instead of the persisted epoch): the #141
/// `new_boot_epoch == 0 || prev != new` test would treat the match as a
/// mismatch and wipe → red-fail here.
#[nativelink_test]
async fn reloaded_entry_kept_on_boot_epoch_match()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let endpoint = "grpc://w-same:50081";
    let kept = digest(0xF1, 100);
    let bytes = persisted_bytes(vec![(endpoint, 3333, vec![kept])]);
    let path = write_persist_file(&bytes).await;

    tokio::time::timeout(DEADLOCK_DETECTOR, server.reload_locality_from_disk(&path))
        .await
        .expect("reload must not hang")
        .expect("reload must succeed");

    // Reconnect with the SAME boot_epoch (3333) — no reboot, transient drop.
    let (_tx, _stream) = open_worker_connection(&server, endpoint, 3333).await?;

    assert_eq!(
        locality_map.read().lookup_workers(&kept).len(),
        1,
        "a same-boot_epoch reconnect must KEEP the reloaded entries (they cover \
         the reconnect→first-snapshot window); the priming must record the \
         persisted boot_epoch exactly, not 0 / a placeholder"
    );
    Ok(())
}

/// NEVER-RECONNECT TTL sweep (design §4.4): a persisted endpoint whose worker
/// never reconnects keeps its sentinel owner; the sweep drops it after the
/// grace window. A SIBLING endpoint that DID reconnect (sentinel replaced by a
/// real worker_id) must NOT be swept.
///
/// Mutation (hold-forever — make the sweep a no-op): the never-reconnect
/// entries persist past grace → the post-sweep lookup is non-empty → red-fail.
#[nativelink_test]
async fn never_reconnect_entry_swept_reconnected_sibling_kept()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let gone_ep = "grpc://w-gone:50081";
    let live_ep = "grpc://w-live:50081";
    let gone_digest = digest(0x11, 100);
    let live_digest = digest(0x22, 200);
    let bytes = persisted_bytes(vec![
        (gone_ep, 5555, vec![gone_digest]),
        (live_ep, 6666, vec![live_digest]),
    ]);
    let path = write_persist_file(&bytes).await;

    tokio::time::timeout(DEADLOCK_DETECTOR, server.reload_locality_from_disk(&path))
        .await
        .expect("reload must not hang")
        .expect("reload must succeed");

    // The LIVE sibling reconnects (same epoch) → its sentinel owner is replaced
    // by a real worker_id, so the sweep must NOT touch it.
    let (_tx, _stream) = open_worker_connection(&server, live_ep, 6666).await?;

    // Run the sweep with grace=0 so EVERY still-unconfirmed reloaded endpoint is
    // past grace. Only `gone_ep` still carries the sentinel.
    let swept = server.sweep_unconfirmed_reloaded_locality(Duration::ZERO);
    assert_eq!(
        swept, 1,
        "exactly the one never-reconnected endpoint must be swept"
    );

    assert!(
        locality_map.read().lookup_workers(&gone_digest).is_empty(),
        "the never-reconnected endpoint's entries must be swept after grace \
         (mutation: hold-forever / no-op sweep leaves them present)"
    );
    assert_eq!(
        locality_map.read().lookup_workers(&live_digest).len(),
        1,
        "a reconnected sibling (sentinel replaced by a real worker_id) must NOT \
         be swept"
    );
    Ok(())
}

/// FAIL-OPEN on a missing file (design §3.3): reload of a non-existent path must
/// succeed (Ok) with an empty summary — a missing persist file degrades to
/// worker-re-announce, it never wedges startup.
#[nativelink_test]
async fn reload_missing_file_fails_open() -> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let missing = std::env::temp_dir().join(format!("nl-dir3-absent-{}.bin", rand_suffix()));

    let summary = server
        .reload_locality_from_disk(&missing)
        .await
        .expect("a missing persist file must FAIL OPEN (Ok empty), never error");
    assert_eq!(summary.endpoints_loaded, 0, "no endpoints from a missing file");
    assert_eq!(summary.digests_loaded, 0, "no digests from a missing file");
    assert_eq!(locality_map.read().digest_count(), 0, "map stays empty");
    Ok(())
}

/// FAIL-OPEN on a corrupt file (design §2.3/§3.3): reload of a file with a bad
/// magic / truncated body must succeed (Ok) with an empty summary — corruption
/// degrades to worker-re-announce, never wedges or crashes startup.
#[nativelink_test]
async fn reload_corrupt_file_fails_open() -> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let path = write_persist_file(b"this is not a valid locality persist file").await;

    let summary = server
        .reload_locality_from_disk(&path)
        .await
        .expect("a corrupt persist file must FAIL OPEN (Ok empty), never error");
    assert_eq!(summary.endpoints_loaded, 0, "no endpoints from a corrupt file");
    assert_eq!(locality_map.read().digest_count(), 0, "map stays empty");
    drop(summary);
    Ok(())
}
