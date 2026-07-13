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

//! (FL-688 v3 Stage A fix) Worker-side half of the AC-pin convergence
//! contract: a server-pushed `Update::AcPinResync` FORCES the worker to
//! re-advertise its FULL current AC-pin set on the next tick, EVEN WHEN its own
//! AC-pin set is unchanged — the case the skip-gate otherwise suppresses.
//!
//! This is the worker side of the BLOCK pair-a found on Stage A: removing the
//! periodic AC-pin full-snapshot heartbeat left no bounded-time convergence
//! path for an OUT-OF-BAND server-side registry removal (BIS-ack sweep / AcProxy
//! peer-NotFound / cap-truncation) on a STABLE long-lived connection — the
//! worker's own AC-pin set is unchanged, so its delta is empty and the skip-gate
//! suppresses the tick, so the divergence persists until the next reconnect.
//!
//! The fix CLEARS `last_sent_ac_pin_set` on receipt of the resync signal
//! (`force_ac_pin_resync`, exercised here via `test_handle_ac_pin_resync`), so
//! the next tick reports the full set as `added` → the skip-gate no longer
//! suppresses → field 17 (`pinned_ac_mirror_entries`, replace-semantics) is
//! re-sent. The server-side routing half (`notify_ac_pin_resync_for_endpoint`)
//! + the full registry-reconvergence loop are covered in
//! `nativelink-scheduler/tests/ac_pin_resync_routing_test.rs`.
//!
//! Production composition crossed here:
//!   producer  = the worker's AC FastSlowStore (`insert_local_ac_pin`), the
//!               SAME store `dispatched_ac_pin_snapshot_for_store` reads at tick
//!               time, with `last_sent_ac_pin_set` seeded to the post-ack state.
//!   seam      = `LocalWorkerImpl`'s `Update::AcPinResync` arm (driven via the
//!               `test_handle_ac_pin_resync` seam → `force_ac_pin_resync`).
//!   sink      = the worker→scheduler stream calls captured by
//!               `MockWorkerApiClient` (the AC entries ride field 17 inside the
//!               delta `ChunkedMessage`).
//!
//! Mutation guard: comment out the body of `force_ac_pin_resync`
//! (`last_sent_ac_pin_set.lock().clear()`) → `resync_forces_readvertise_*`
//! red-fails with its bespoke "AcPinRegistry diverged with no convergence path
//! on a stable connection — server-push on out-of-band removal missing".

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use std::collections::HashSet;
use std::sync::Arc;

use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::chunked_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
use nativelink_worker::local_worker::{
    AcMirrorTarget, BlobsAvailableState, BlobsAvailableTestArgs,
    send_periodic_blobs_available_for_test,
};
use nativelink_worker::running_actions_manager::Metrics;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use utils::local_worker_test_utils::MockWorkerApiClient;
use utils::mock_running_actions_manager::MockRunningActionsManager;

const AC_STORE_NAME: &str = "AC_MAIN_STORE";

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_ac_resync_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_ac_resync_temp_")
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

fn make_fss() -> Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast,
        slow,
    )
}

fn ac_target_for(fss: Arc<FastSlowStore>) -> AcMirrorTarget {
    AcMirrorTarget {
        fss,
        store_id: Arc::from(AC_STORE_NAME),
        ac_publish_pending_acks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        metrics: Arc::new(Metrics::default()),
    }
}

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// Decode the AC-pin digest set carried by field 17 of the delta the worker
/// just emitted (a `ChunkedMessage` wrapping a `BlobsAvailableChunk`).
async fn drain_one_delta_ac_pin_set(client: &MockWorkerApiClient) -> HashSet<DigestInfo> {
    let envelope = client.expect_chunked_message(Ok(())).await;
    let chunk = match envelope.payload.expect("ChunkedMessage missing payload") {
        chunked_message::Payload::BlobsAvailable(c) => c,
        other => panic!("expected BlobsAvailable chunk payload, got {other:?}"),
    };
    chunk
        .pinned_ac_mirror_entries
        .into_iter()
        .filter_map(|e| e.digest.and_then(|d| DigestInfo::try_from(d).ok()))
        .collect()
}

/// The contract that would have caught the BLOCK.
///
/// 1. Worker holds AC pin {D}; `last_sent_ac_pin_set` is seeded to {D} (the
///    post-ack state — the server acked the advertisement).
/// 2. A steady-state tick (no local change) is SUPPRESSED by the skip-gate
///    (delta empty) — this is the divergence trap: the server may have removed
///    D out-of-band and the worker would never re-advertise.
/// 3. The server pushes `AcPinResync` (the fix). The worker clears its memo.
/// 4. The NEXT steady-state tick now FIRES and re-advertises field 17 = {D},
///    which the server's `replace_endpoint_ac_pins` uses to reconverge.
///
/// On a stable connection (no reconnect, no local change) step 4 is the ONLY
/// convergence path; without the resync clear it never happens.
#[nativelink_test]
async fn resync_forces_readvertise_on_stable_connection_no_reconnect() {
    let (fs_store, _c, _t) = make_filesystem_store().await;
    let ac_fss = make_fss();
    let d = mk_digest(0xD1, 7);
    ac_fss.insert_local_ac_pin(AC_STORE_NAME, d);

    let state = BlobsAvailableState::from_test_args(
        fs_store,
        BlobsAvailableTestArgs {
            ac_mirror_target: Some(ac_target_for(ac_fss.clone())),
            ..Default::default()
        },
    );
    // Post-ack state: the worker believes the server holds {D}.
    state.test_seed_last_sent_ac_pin_set(&[d]);

    let ram = Arc::new(MockRunningActionsManager::new());
    let client = MockWorkerApiClient::new();

    // Step 2: a steady-state tick with NO local change MUST be suppressed.
    // The mock's send AWAITS a response, so an unexpected emit would block the
    // producer; run a concurrent drainer (auto-acks any emit) so the producer
    // never wedges, and count emits. Under correct (suppressed) behaviour the
    // producer races to `done` and the count is 0.
    let pre_resync_emits = {
        let state = state.clone();
        let ram = ram.clone();
        let mut producer_client = client.clone();
        let drain_client = client.clone();
        let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();
        let producer = async move {
            send_periodic_blobs_available_for_test(&mut producer_client, &state, &ram, false)
                .await
                .expect("pre-resync steady-state tick");
            let _ = done_tx.send(());
        };
        let drainer = async move {
            let mut emits = 0usize;
            loop {
                emits += drain_client.drain_pending_call_count();
                if done_rx.try_recv().is_ok() {
                    emits += drain_client.drain_pending_call_count();
                    break;
                }
                tokio::task::yield_now().await;
            }
            emits
        };
        let (_p, emits) = tokio::time::timeout(
            core::time::Duration::from_secs(10),
            async { tokio::join!(producer, drainer) },
        )
        .await
        .expect("timed out on pre-resync steady-state tick");
        emits
    };
    assert_eq!(
        pre_resync_emits, 0,
        "pre-resync steady-state tick must be SUPPRESSED by the skip-gate (delta empty: the \
         worker's AC-pin set is unchanged) — this is the divergence trap the resync push closes"
    );

    // Step 3: server pushes AcPinResync → worker clears its memo.
    state.test_handle_ac_pin_resync();

    // Step 4: the NEXT steady-state tick (still is_first=false, still no local
    // change) MUST now fire and re-advertise the FULL AC-pin set {D}.
    let emitted_ac_set = {
        let state = state.clone();
        let ram = ram.clone();
        let mut producer_client = client.clone();
        let consumer_client = client.clone();
        let producer = async move {
            send_periodic_blobs_available_for_test(&mut producer_client, &state, &ram, false)
                .await
                .expect("post-resync steady-state tick");
        };
        let consumer = async move { drain_one_delta_ac_pin_set(&consumer_client).await };
        let (_p, ac_set) = tokio::time::timeout(
            core::time::Duration::from_secs(10),
            async { tokio::join!(producer, consumer) },
        )
        .await
        .expect(
            "AcPinRegistry diverged with no convergence path on a stable connection — \
             server-push on out-of-band removal missing: the post-resync tick emitted NOTHING, so \
             the worker never re-advertised its AC-pin set and the server's registry stays stale \
             until reconnect",
        );
        ac_set
    };

    let expected: HashSet<DigestInfo> = [d].into_iter().collect();
    assert_eq!(
        emitted_ac_set, expected,
        "AcPinRegistry diverged with no convergence path on a stable connection — \
         server-push on out-of-band removal missing: the post-resync re-advertisement did not \
         carry the worker's full AC-pin set (field 17 must replace the server registry to {d:?})"
    );

    // The memo is restored to the re-advertised set: the resync is a ONE-SHOT
    // force (clear → re-advertise → re-memo), NOT a permanent disable of the
    // skip-gate. (A subsequent tick's skip-gate would re-arm, but the
    // replay-until-acked reader independently re-sends the still-unacked
    // buffered chunk every tick — that channel is orthogonal to the skip-gate
    // and covered by `blobs_available_replay_test`, so we do not re-assert
    // suppression here to avoid confounding the two convergence channels.)
    assert_eq!(
        state.test_last_sent_ac_pin_set(),
        expected,
        "after the forced re-advertisement the memo must be restored to the sent set (one-shot \
         resync: clear → re-advertise → re-memo, not a permanent skip-gate override)"
    );
}
