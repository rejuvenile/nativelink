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

//! (FL-688 v3 Stage A) The PERIODIC full-snapshot heartbeat is REMOVED.
//!
//! The full snapshot now fires ONLY on (a) worker reconnect (`is_first=true`,
//! one per `run()` re-entry) and (b) the over-cap force-full-snapshot EVENT.
//! Deltas stay event-driven. There is NO timer/tick-driven full snapshot.
//!
//! These tests cross the production composition the periodic-snapshot removal
//! touches:
//!   producer  = `FilesystemStore` (the worker's fast tier, with an
//!               indefinitely-pinned pending-BIS blob present and the change
//!               tracker left UNREGISTERED so a steady-state tick has no delta)
//!   fold-gate = `LocalWorkerImpl::send_periodic_blobs_available` (driven via
//!               the `send_periodic_blobs_available_for_test` seam)
//!   sink      = the worker→scheduler stream calls captured by
//!               `MockWorkerApiClient`
//!
//! `no_periodic_full_snapshot_across_steady_state_ticks`: across 605
//! steady-state ticks (the OLD heartbeat would have fired a full snapshot at
//! tick 600) the worker emits ZERO calls — the pending-BIS digest is NEVER
//! re-advertised by a timer (it re-converges via reconnect + the Stage-2B
//! replay-until-acked reader + the over-cap event instead). Verified RED
//! against the pre-Stage-A heartbeat code (one ChunkedMessage emitted at tick
//! 600); GREEN after the heartbeat is removed.
//!
//! `reconnect_emits_full_snapshot`: a reconnect tick (`is_first=true`) STILL
//! emits a full snapshot (the kept reconnect-only behavior), proving Stage A
//! removed only the PERIODIC path, not the reconnect path.

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use std::sync::Arc;

use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_macro::nativelink_test;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreLike;
use nativelink_worker::local_worker::{
    BlobsAvailableState, BlobsAvailableTestArgs, send_periodic_blobs_available_for_test,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use utils::local_worker_test_utils::MockWorkerApiClient;
use utils::mock_running_actions_manager::MockRunningActionsManager;

/// 605 steady-state ticks — one past where the removed heartbeat fired (600 =
/// `60_000 / BLOBS_AVAILABLE_MAX_INTERVAL_MS`). A fixed bound, NOT derived from
/// any const, so the assertion does not move if the old cadence is reintroduced.
const STEADY_STATE_TICKS: u64 = 605;

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_no_heartbeat_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_no_heartbeat_temp_")
        .tempdir()
        .expect("tempdir");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_dir.path().to_string_lossy().into_owned(),
        temp_path: temp_dir.path().to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024 * 1024,
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("create filesystem store");
    (store, content_dir, temp_dir)
}

/// Across 605 steady-state (`is_first=false`) ticks with an indefinitely-pinned
/// pending-BIS blob present but no blob delta, the worker emits NOTHING. The OLD
/// periodic heartbeat would have fired a full snapshot (and folded the
/// pending-BIS pin) at tick 600; with the heartbeat removed, an otherwise-idle
/// worker is silent. Convergence for a missed `mark_stable` now comes from the
/// reconnect snapshot + the Stage-2B replay-until-acked reader + the over-cap
/// event — never a timer.
#[nativelink_test]
async fn no_periodic_full_snapshot_across_steady_state_ticks() {
    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;

    // A pending-BIS blob: written + pinned INDEFINITELY, no BIS ack. The OLD
    // heartbeat fold re-advertised exactly this digest on the Nth tick.
    let pending = DigestInfo::new([7u8; 32], 6);
    fs_store
        .as_pin()
        .update_oneshot(pending, "hello!".into())
        .await
        .expect("write pending-BIS blob");
    assert!(
        fs_store.pin_digest_indefinite_with_result(&pending),
        "indefinite pin of the pending-BIS digest should succeed"
    );

    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());
    let ram = Arc::new(MockRunningActionsManager::new());
    let client = MockWorkerApiClient::new();

    // Producer: drive 605 steady-state ticks. The change tracker is unregistered
    // and no blob changes occur, so the skip-gate suppresses every delta tick;
    // the only way a call could be emitted is a periodic full snapshot — which is
    // gone. A done-signal lets the concurrent counter stop polling once the
    // producer has driven all ticks.
    //
    // NOTE: the mock's send impls AWAIT a response, so any emit blocks the
    // producer until the counter drains+responds. The counter runs concurrently
    // for exactly that reason — under the GREEN code nothing is emitted and the
    // producer races straight to `done`; if a periodic snapshot WERE emitted the
    // counter drains it (unblocking the producer) and records the violation.
    let emitted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

    let producer = {
        let state = state.clone();
        let ram = ram.clone();
        let mut producer_client = client.clone();
        async move {
            for _ in 0..STEADY_STATE_TICKS {
                send_periodic_blobs_available_for_test(
                    &mut producer_client,
                    &state,
                    &ram,
                    /* is_first */ false,
                )
                .await
                .expect("send_periodic_blobs_available steady-state tick");
            }
            let _ = done_tx.send(());
        }
    };

    let counter = {
        let emitted = emitted.clone();
        let client = client.clone();
        let mut done_rx = done_rx;
        async move {
            loop {
                // Drain + auto-respond to any emitted call so the producer never
                // wedges on the mock's response await; count each one.
                let drained = client.drain_pending_call_count();
                emitted.fetch_add(drained, std::sync::atomic::Ordering::Relaxed);
                // Stop once the producer signalled done AND nothing is left.
                if done_rx.try_recv().is_ok() {
                    let tail = client.drain_pending_call_count();
                    emitted.fetch_add(tail, std::sync::atomic::Ordering::Relaxed);
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    };

    tokio::time::timeout(
        core::time::Duration::from_secs(10),
        async { tokio::join!(producer, counter) },
    )
    .await
    .expect("timed out driving steady-state ticks");

    let emitted = emitted.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        emitted, 0,
        "periodic full-snapshot heartbeat fired: across {STEADY_STATE_TICKS} steady-state ticks \
         (one past the removed 600-tick heartbeat) an idle worker emitted {emitted} call(s) — the \
         timer-driven full snapshot must be REMOVED (convergence is reconnect + replay-until-acked \
         + over-cap event, NOT a periodic tick)"
    );
}

/// The reconnect path is INTACT: an `is_first=true` tick STILL emits a full
/// snapshot carrying the whole-store enumeration (including the indefinitely-
/// pinned pending-BIS blob). Stage A removed only the PERIODIC snapshot, not the
/// reconnect-only one.
#[nativelink_test]
async fn reconnect_emits_full_snapshot() {
    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;

    let pending = DigestInfo::new([7u8; 32], 6);
    fs_store
        .as_pin()
        .update_oneshot(pending, "hello!".into())
        .await
        .expect("write pending-BIS blob");
    assert!(
        fs_store.pin_digest_indefinite_with_result(&pending),
        "indefinite pin should succeed"
    );

    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());
    let ram = Arc::new(MockRunningActionsManager::new());
    let client = MockWorkerApiClient::new();

    let producer = {
        let state = state.clone();
        let ram = ram.clone();
        let mut producer_client = client.clone();
        async move {
            send_periodic_blobs_available_for_test(
                &mut producer_client,
                &state,
                &ram,
                /* is_first */ true,
            )
            .await
            .expect("reconnect send_periodic_blobs_available tick");
        }
    };

    // A SMALL reconnect full snapshot takes the legacy single-message
    // `blobs_available()` path (the `if !is_first || should_chunk(...)` gate:
    // `is_first=true` + a payload below the chunk threshold ⇒ single message).
    // The consumer receives it and asserts the pending-BIS digest is enumerated
    // and `is_full_snapshot` is set. The 10s deadline doubles as a deadlock
    // detector: if the reconnect path were removed, nothing would be emitted and
    // `expect_blobs_available` would hang.
    let consumer = async {
        let notification = client.expect_blobs_available(Ok(())).await;
        assert!(
            notification.is_full_snapshot,
            "reconnect tick must emit a FULL SNAPSHOT (is_full_snapshot=true); got a delta — the \
             reconnect-only full-snapshot path must survive Stage A's periodic-heartbeat removal"
        );
        let advertised: Vec<DigestInfo> = notification
            .digest_infos
            .into_iter()
            .filter_map(|info| info.digest.and_then(|d| DigestInfo::try_from(d).ok()))
            .collect();
        assert!(
            advertised.contains(&pending),
            "reconnect full snapshot did not enumerate the indefinitely-pinned pending-BIS blob — \
             the whole-store reconnect snapshot is the convergence the removed heartbeat used to \
             provide for a missed mark_stable. snapshot digest_infos: {advertised:?}"
        );
    };

    tokio::time::timeout(
        core::time::Duration::from_secs(10),
        async { tokio::join!(producer, consumer) },
    )
    .await
    .expect(
        "timed out waiting for the reconnect BlobsAvailable full snapshot — the reconnect-only \
         snapshot path must survive Stage A",
    );
}
