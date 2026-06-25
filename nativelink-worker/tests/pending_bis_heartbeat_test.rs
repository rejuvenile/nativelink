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

//! FL-681 Follow-up B (MAJOR-2 robust close-out): periodic pending-BIS CAS
//! re-advertisement.
//!
//! The seam MAJOR-2 flagged as untested: a CAS digest that is indefinitely
//! pinned (an F2 output held until the server's BlobsInStableStorage ack) but
//! whose `mark_stable` was MISSED (transient server `has_with_results` failure)
//! must be re-driven to BIS WITHOUT waiting for a reconnect. Follow-up B folds
//! the worker's still-pending-BIS CAS pin set into the periodic full-snapshot
//! heartbeat (`AC_PIN_FULL_SNAPSHOT_EVERY_N_TICKS`, ~6s) so the server re-runs
//! `mark_stable` on the present subset.
//!
//! Production composition crossed by this test:
//!   producer  = `FilesystemStore::indefinite_pinned_digests` (real enumeration)
//!   fold-gate = `LocalWorkerImpl::send_periodic_blobs_available` heartbeat arm
//!               (driven via the `send_periodic_blobs_available_for_test` seam)
//!   sink      = `BlobsAvailableNotification.digest_infos` captured at the
//!               worker→scheduler stream (the field `request_missing_blob_uploads`
//!               feeds to `has_with_results` + `mark_stable`, server-side).
//!
//! Mutation guidance: remove the `is_heartbeat_tick && !is_first` fold call in
//! `send_periodic_blobs_available`; `heartbeat_readvertises_pending_bis_cas_pin`
//! red-fails ("pending-BIS digest was never re-advertised on any of 60 ticks").

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use std::sync::Arc;

use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::chunked_message;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreLike;
use nativelink_worker::local_worker::{
    AC_PIN_FULL_SNAPSHOT_EVERY_N_TICKS, BlobsAvailableState, BlobsAvailableTestArgs,
    send_periodic_blobs_available_for_test,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use utils::local_worker_test_utils::MockWorkerApiClient;
use utils::mock_running_actions_manager::MockRunningActionsManager;

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_pending_bis_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_pending_bis_temp_")
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

/// A digest that is indefinitely pinned on the worker (pending BIS) but whose
/// `mark_stable` was MISSED is re-advertised within the heartbeat interval —
/// on the heartbeat tick, NOT only on reconnect. The change-tracker is left
/// UNREGISTERED on the store, so a normal delta tick carries no blob changes
/// and is suppressed by the empty-tick skip-gate; the pending-BIS digest can
/// reach the wire ONLY via the heartbeat fold. That makes the assertion exact:
/// across `AC_PIN_FULL_SNAPSHOT_EVERY_N_TICKS` ticks the digest is advertised
/// exactly once — on the heartbeat tick.
#[nativelink_test]
async fn heartbeat_readvertises_pending_bis_cas_pin() {
    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;

    // Write a CAS blob and pin it INDEFINITELY (the pending-BIS state) WITHOUT
    // a BIS ack — `mark_stable` is "missed", so the worker still holds the pin.
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

    let mut client = MockWorkerApiClient::new();

    // Producer: drive exactly AC_PIN_FULL_SNAPSHOT_EVERY_N_TICKS steady-state
    // (is_first=false) ticks. Only the heartbeat tick (the Nth) folds the
    // pending-BIS pin into digest_infos and therefore emits a BlobsAvailable;
    // the other ticks have no blob delta and are suppressed by the skip-gate.
    let producer = {
        let state = state.clone();
        let ram = ram.clone();
        let mut producer_client = client.clone();
        async move {
            for _ in 0..AC_PIN_FULL_SNAPSHOT_EVERY_N_TICKS {
                send_periodic_blobs_available_for_test(
                    &mut producer_client,
                    &state,
                    &ram,
                    /* is_first */ false,
                )
                .await
                .expect("send_periodic_blobs_available tick");
            }
        }
    };

    // Consumer: receive the single heartbeat BlobsAvailable and assert the
    // pending-BIS digest is present in digest_infos. The deadline doubles as a
    // deadlock detector — if the fold never fires, nothing is emitted and
    // `expect_chunked_message` would hang here.
    //
    // (FL-688 v3 §3.8 part 2) A heartbeat tick is a DELTA (is_first=false), so
    // it now rides the ACKED chunked path: the worker emits a
    // `ChunkedMessage(BlobsAvailable)`, not the legacy `blobs_available()`
    // single-message call. A small heartbeat delta chunks to one terminal
    // chunk carrying the pending-BIS digest.
    let consumer = async {
        let envelope = client.expect_chunked_message(Ok(())).await;
        let chunk = match envelope.payload.expect("heartbeat ChunkedMessage payload") {
            chunked_message::Payload::BlobsAvailable(c) => c,
            other => panic!("heartbeat delta must be a BlobsAvailable chunk; got {other:?}"),
        };
        let advertised: Vec<DigestInfo> = chunk
            .digests
            .into_iter()
            .filter_map(|info| info.digest.and_then(|d| DigestInfo::try_from(d).ok()))
            .collect();
        assert!(
            advertised.contains(&pending),
            "pending-BIS digest was never re-advertised on any of {AC_PIN_FULL_SNAPSHOT_EVERY_N_TICKS} \
             ticks: the missed-mark_stable digest would reach BIS only on reconnect, not within the \
             heartbeat interval. chunk digests advertised: {advertised:?}"
        );
    };

    // tokio::join! the producer and consumer under a hard timeout: if the
    // heartbeat fold is broken the consumer's expect_blobs_available blocks
    // forever, so the timeout converts a silent hang into a loud failure.
    tokio::time::timeout(
        core::time::Duration::from_secs(10),
        async { tokio::join!(producer, consumer) },
    )
    .await
    .expect(
        "timed out waiting for the heartbeat BlobsAvailable — the pending-BIS CAS pin fold did \
         not fire within AC_PIN_FULL_SNAPSHOT_EVERY_N_TICKS ticks",
    );
}

/// Storm-prevention guard: the fold MUST NOT fire on every tick. A normal delta
/// tick (NOT the heartbeat) with a pending-BIS pin present but no blob change
/// emits NOTHING — the digest is folded only on the heartbeat tick. Without the
/// `is_heartbeat_tick` guard the worker would re-advertise its entire pending
/// set every 100ms (the empty-tick storm the AC-pin heartbeat already fixed).
#[nativelink_test]
async fn normal_delta_tick_does_not_readvertise_pending_bis_cas_pin() {
    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;

    let pending = DigestInfo::new([9u8; 32], 4);
    fs_store
        .as_pin()
        .update_oneshot(pending, "data".into())
        .await
        .expect("write pending-BIS blob");
    assert!(
        fs_store.pin_digest_indefinite_with_result(&pending),
        "indefinite pin should succeed"
    );

    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());
    let ram = Arc::new(MockRunningActionsManager::new());
    let mut client = MockWorkerApiClient::new();

    // A SINGLE non-heartbeat tick (tick 1 of the cycle). With an unregistered
    // tracker there is no blob delta, so the skip-gate suppresses the tick
    // entirely: no BlobsAvailable is emitted. If the fold fired on every tick,
    // this tick would emit one carrying the pending-BIS digest.
    send_periodic_blobs_available_for_test(&mut client, &state, &ram, /* is_first */ false)
        .await
        .expect("single non-heartbeat tick");

    // No call must have been enqueued. `try_next_blobs_available` returns None
    // when the mock's call channel is empty.
    let emitted = client.try_next_blobs_available();
    assert_eq!(
        emitted, None,
        "a normal (non-heartbeat) delta tick with a pending-BIS pin but no blob change MUST emit \
         no BlobsAvailable — folding on every tick re-creates the empty-tick storm the heartbeat \
         gate exists to prevent"
    );
}
