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
use nativelink_service::worker_api_server::{
    LOCALITY_PERSIST_RECONNECT_GRACE_SECS, WorkerApiServer,
};
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
    None,
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

/// FAIL-OPEN on a VALID-FRAME-but-HOSTILE-BODY file (security S1 fix-up, item D).
///
/// The raw magic+version frame guard only rejects a foreign/headerless file; it
/// does NOT cover the bincode BODY. A file carrying a VALID 6-byte frame
/// followed by a body whose `entries` `Vec` length-prefix is huge
/// (`u64::MAX` declared, then NO element bytes) must FAIL OPEN — the reload
/// returns `Ok(empty)`, the server starts, the map stays empty. It must NOT
/// propagate the decode error up to crash startup, and (with the bounded decode)
/// it must NOT drive an unbounded allocation.
///
/// DRIFT NOTE (verified against bincode-2.0.1 + serde): the security review's
/// "`Vec::with_capacity(u64::MAX)` → allocator abort" premise does NOT hold on
/// the `bincode::serde::decode_from_slice` path — serde's `Vec` visitor uses a
/// cautious capacity cap and bincode decodes element-by-element against a
/// slice-bounded reader, so a SHORT hostile body hits `UnexpectedEnd` and the
/// `with_limit` makes no observable difference for THIS input (empirically
/// confirmed: the decode returns `Err`, not an abort, with OR without the
/// limit). The bounded decode (`with_limit`) is retained as cheap cumulative-
/// allocation defense-in-depth, but the load-bearing fail-open mechanism this
/// test pins is the `Err → Ok(empty)` mapping in `reload_from_disk`, NOT the
/// limit. The existing `reload_corrupt_file_fails_open` uses a WRONG-MAGIC file
/// rejected by the FRAME guard before bincode runs; this test reaches the BODY
/// decode (valid frame) and pins that a body-decode error ALSO fails open.
///
/// Mutation (in `reload_from_disk`, change the corrupt-body arm
/// `Err(e) => { warn!(...); return Ok(empty) }` to `Err(e) => return Err(e)`):
/// the body-decode error propagates instead of failing open → the
/// `.expect("must FAIL OPEN ...")` below red-fails with its bespoke message.
#[nativelink_test]
async fn reload_valid_frame_hostile_body_fails_open()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;

    // A real serialize gives us the exact, correct 6-byte magic+version frame
    // (so the frame guard PASSES and we genuinely reach the body decode).
    let real = persisted_bytes(vec![]);
    let frame: Vec<u8> = real[..6].to_vec();

    // Hostile body: persisted_at_unix_s = 0 (varint single byte), then
    // entries.len() = u64::MAX (U64_BYTE prefix + 8 LE bytes) with NO element
    // bytes — an inconsistent/oversized length the body decode must reject.
    const U64_BYTE: u8 = 253;
    let mut bytes = frame;
    bytes.push(0x00); // persisted_at_unix_s = 0
    bytes.push(U64_BYTE);
    bytes.extend_from_slice(&u64::MAX.to_le_bytes()); // entries.len() = u64::MAX
    let path = write_persist_file(&bytes).await;

    // Must FAIL OPEN — return Ok(empty), NOT propagate the decode error to crash
    // startup.
    let summary = server
        .reload_locality_from_disk(&path)
        .await
        .expect(
            "a valid-frame + hostile-body file must FAIL OPEN (Ok empty) — the \
             body-decode error must be mapped to an empty map, NOT propagated to \
             crash startup (mutation: make reload_from_disk's corrupt-body arm \
             `return Err(e)` instead of `Ok(empty)`)",
        );
    assert_eq!(
        summary.endpoints_loaded, 0,
        "a hostile-body file must yield no endpoints (fail-open empty map)"
    );
    assert_eq!(
        locality_map.read().digest_count(),
        0,
        "map stays empty after a hostile-body reload"
    );
    Ok(())
}

/// GRACE IS LOAD-BEARING (distsys MAJOR-1 + code-reviewer fix-up, item B): the
/// never-reconnect sweep must NOT drop sentinel entries until the reload is at
/// least `grace` old. A sweep with a grace LARGER than the elapsed-since-reload
/// time must be a no-op; only once grace has elapsed (here: grace=ZERO, always
/// elapsed) does it sweep. This pins that `grace` is actually CHECKED, not
/// ignored (`let _ = grace;`).
///
/// Mutation (drop the grace gate — `let _ = grace;` / remove the
/// `baseline.elapsed() < grace` early-return in `sweep_unconfirmed`): the
/// huge-grace sweep would sweep the never-reconnect entry immediately → the
/// first `assert_eq!(swept_too_early, 0, ...)` red-fails with its bespoke
/// message.
#[nativelink_test]
async fn sweep_honors_grace_no_sweep_before_grace_elapses()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let gone_ep = "grpc://w-grace-gone:50081";
    let gone_digest = digest(0x33, 100);
    let bytes = persisted_bytes(vec![(gone_ep, 7777, vec![gone_digest])]);
    let path = write_persist_file(&bytes).await;

    tokio::time::timeout(DEADLOCK_DETECTOR, server.reload_locality_from_disk(&path))
        .await
        .expect("reload must not hang")
        .expect("reload must succeed");

    // The reloaded entry is present and the worker never reconnected (sentinel).
    assert_eq!(
        locality_map.read().lookup_workers(&gone_digest).len(),
        1,
        "pre-condition: the never-reconnect entry is present after reload"
    );

    // A grace FAR larger than the elapsed-since-reload time: the sweep must be a
    // no-op (grace not yet elapsed). If `grace` were ignored, this would sweep.
    let swept_too_early =
        server.sweep_unconfirmed_reloaded_locality(Duration::from_secs(86_400));
    assert_eq!(
        swept_too_early, 0,
        "sweep must NOT drop a sentinel entry before grace elapses — grace is \
         load-bearing (mutation: `let _ = grace;` ignores it and sweeps \
         immediately)"
    );
    assert_eq!(
        locality_map.read().lookup_workers(&gone_digest).len(),
        1,
        "the entry must SURVIVE a pre-grace sweep"
    );

    // Now sweep with grace=ZERO (always elapsed): the entry IS past grace and
    // must be swept. Proves the no-op above was the grace gate, not a dead sweep.
    let swept_after_grace = server.sweep_unconfirmed_reloaded_locality(Duration::ZERO);
    assert_eq!(
        swept_after_grace, 1,
        "with grace=ZERO (elapsed) the never-reconnect entry MUST be swept"
    );
    assert!(
        locality_map.read().lookup_workers(&gone_digest).is_empty(),
        "the never-reconnect entry must be gone once grace has elapsed"
    );
    Ok(())
}

/// RECONNECT-DURING-RELOAD must NOT clobber a LIVE entry (distsys MAJOR-2
/// fix-up, item C): the reload runs concurrently with the worker-API listener
/// bind, so a worker can connect for an endpoint BEFORE the reload's priming
/// loop reaches it. The reload's `endpoint_state` prime must be CONDITIONAL
/// (`entry().or_insert`) so it does NOT overwrite the live connection's real
/// `WorkerId` with the `__reloaded_unconfirmed__` sentinel — otherwise the
/// never-reconnect sweep would later DROP the LIVE worker's locality entries.
///
/// Composition: connect worker E live (real connect → live `endpoint_state`
/// owner) + register a live blob for E (models a BlobsAvailable that already
/// landed), THEN reload a persist file that ALSO names E (with a stale digest +
/// the persisted boot_epoch). With the conditional prime, E keeps its live
/// owner, so a subsequent grace-elapsed sweep does NOT touch it and the LIVE
/// blob survives.
///
/// Mutation (revert the conditional prime to an unconditional
/// `state.insert(...)`): the reload clobbers E's live owner with the sentinel →
/// the `sweep_unconfirmed(ZERO)` drops E → `lookup_workers(live_digest)` is
/// empty → the final assert red-fails with its bespoke message.
#[nativelink_test]
async fn reload_does_not_clobber_live_reconnected_endpoint()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let endpoint = "grpc://w-race:50081";
    let live_digest = digest(0x44, 100);
    let stale_digest = digest(0x45, 200);

    // The worker connected LIVE before the reload's priming loop (it raced the
    // reload window and won). Real connect → live endpoint_state owner.
    let (_tx, _stream) = open_worker_connection(&server, endpoint, 9999).await?;
    // A BlobsAvailable that already landed for the live worker: register a live
    // blob in the locality map under E.
    locality_map.write().register_blobs(endpoint, &[live_digest]);
    assert_eq!(
        locality_map.read().lookup_workers(&live_digest).len(),
        1,
        "pre-condition: the live worker's blob is registered"
    );

    // Reload a persist file that ALSO names E (stale digest + persisted epoch).
    // The conditional prime must NOT overwrite E's live owner with the sentinel.
    let bytes = persisted_bytes(vec![(endpoint, 1234, vec![stale_digest])]);
    let path = write_persist_file(&bytes).await;
    tokio::time::timeout(DEADLOCK_DETECTOR, server.reload_locality_from_disk(&path))
        .await
        .expect("reload must not hang")
        .expect("reload must succeed");

    // Grace-elapsed sweep (grace=ZERO). If the reload clobbered E's live owner
    // with the sentinel, this sweeps E and wipes the LIVE worker's blob.
    let swept = server.sweep_unconfirmed_reloaded_locality(Duration::ZERO);
    assert_eq!(
        swept, 0,
        "a LIVE reconnected endpoint must NOT be swept — its real owner must \
         survive the reload (mutation: unconditional reload state.insert \
         clobbers the live owner with the sentinel, so the sweep drops it)"
    );

    // THE LOAD-BEARING ASSERTION: the live worker's blob survives.
    assert_eq!(
        locality_map.read().lookup_workers(&live_digest).len(),
        1,
        "the LIVE reconnected worker's locality entry was CLOBBERED by the \
         reload and then swept — reload `state.insert` must be conditional \
         (`entry().or_insert`) so it never overwrites a live connection's owner. \
         Found: {:?}",
        locality_map.read().lookup_workers(&live_digest)
    );
    Ok(())
}

/// PRODUCTION-TIMING SCHEDULING (distsys MAJOR-1, RE-OPENED — task #66): the
/// never-reconnect sweep SCHEDULER must fire `grace` after the RELOAD BASELINE,
/// not after boot. This is the test the prior sweep tests could not catch: they
/// all call `sweep_unconfirmed_reloaded_locality(grace)` directly with `grace`
/// DECOUPLED from the production sleep timing (grace=ZERO always-elapsed or
/// grace=86400 never-elapsed), so they never compose the real
/// `sleep(grace)`-relative-to-`reload_baseline` schedule.
///
/// THE BUG this pins: `reload_from_disk` stamps `reload_baseline` at its END
/// (after the async file read + decode = `boot + reload_duration`). If the
/// scheduler sleeps `grace` from BOOT and then calls `sweep_unconfirmed(grace)`,
/// at fire time `baseline.elapsed() = grace − reload_duration < grace`, so the
/// strict `<` time-gate returns 0 EVERY boot — the single one-shot fire is a
/// guaranteed no-op and never-reconnect entries leak forever.
///
/// This test composes the PRODUCTION schedule via
/// `LocalityPersister::run_never_reconnect_sweep(reload_done, grace)`, passing
/// the REAL `reload_from_disk` future as `reload_done` (so the sleep-start is
/// COUPLED to the baseline-stamp, exactly as the bin couples it to the
/// `bazel_ready` gate that flips only after the reload returns) and the REAL
/// production `LOCALITY_PERSIST_RECONNECT_GRACE_SECS` (600 s) as `grace`. Under
/// `start_paused`, the 600 s `sleep` is virtualized: the real file read advances
/// virtual time ~0 (it parks on real I/O, not a timer), the baseline is stamped
/// at virtual≈0, then the awaited `sleep(grace)` auto-advances the clock by
/// `grace`, so `baseline.elapsed() == grace` at fire and the entry is swept.
///
/// Mutation (in `run_never_reconnect_sweep`, swap the ordering to sleep-from-boot
/// — `tokio::time::sleep(grace).await; reload_done.await;` instead of awaiting
/// `reload_done` FIRST): the sleep runs before the reload, the virtual clock
/// advances `grace`, THEN `reload_done` completes and stamps the baseline at
/// virtual=`grace`, so at fire `baseline.elapsed() ≈ 0 < grace` → the sweep
/// returns 0 and the entry LEAKS → the `assert_eq!(swept, 1, ...)` red-fails with
/// its bespoke message. This is the boot-vs-baseline bug biting.
#[nativelink_test(start_paused = true)]
async fn never_reconnect_sweep_fires_grace_after_baseline_not_boot()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, locality_map) = make_server()?;
    let gone_ep = "grpc://w-timing-gone:50081";
    let gone_digest = digest(0x55, 100);
    let bytes = persisted_bytes(vec![(gone_ep, 8888, vec![gone_digest])]);
    let path = write_persist_file(&bytes).await;

    // The persister handle the bin's sweep task owns — the EXACT production
    // handle (cheap clones of locality_map + endpoint_state + reload_baseline).
    let persister = server
        .locality_persister()
        .expect("server built with a locality_map must yield a persister");

    // Compose the PRODUCTION schedule: reload-done signal = the real
    // `reload_from_disk` future (stamps `reload_baseline` at its END), grace =
    // the real production constant. `run_never_reconnect_sweep` awaits the reload
    // FIRST, THEN sleeps grace, THEN sweeps — so the grace timer starts from the
    // baseline, never from boot.
    let grace = Duration::from_secs(LOCALITY_PERSIST_RECONNECT_GRACE_SECS);
    let reload_done = async move {
        // The bin maps any reload error to fail-open; here a well-formed file
        // must reload cleanly. We only need the future to RESOLVE (and stamp the
        // baseline) — its summary is asserted by the dedicated reload tests.
        server
            .reload_locality_from_disk(&path)
            .await
            .expect("reload of a well-formed file must succeed");
    };

    // A generous deadlock detector that, under virtual time, the awaited
    // `sleep(grace)` advances through deterministically (the real 600 s never
    // elapses on the wall clock). A genuine hang (e.g. the sweep awaiting a
    // signal that never fires) trips this instead of wedging the test runner.
    let swept = tokio::time::timeout(
        Duration::from_secs(LOCALITY_PERSIST_RECONNECT_GRACE_SECS * 4),
        persister.run_never_reconnect_sweep(reload_done, grace),
    )
    .await
    .expect("sweep scheduler must not hang — it must fire once grace elapses from the baseline");

    assert_eq!(
        swept, 1,
        "the never-reconnect sweep must FIRE grace after the reload BASELINE — \
         the scheduler sleeps grace AFTER awaiting reload-completion, so \
         baseline.elapsed() >= grace at fire. Mutation (sleep grace from BOOT \
         before awaiting reload-done) makes baseline.elapsed() = grace − \
         reload_duration < grace, the strict `<` gate returns 0, and the entry \
         leaks — swept would be 0, not 1"
    );
    assert!(
        locality_map.read().lookup_workers(&gone_digest).is_empty(),
        "the never-reconnect entry must be GONE after the production-timed sweep \
         (mutation: sleep-from-boot leaves it present because the grace gate \
         no-ops on the single one-shot fire)"
    );
    Ok(())
}
