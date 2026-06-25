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

//! (FL-688 v3 §3.8 — drain-on-ack flip) End-to-end convergence proof for
//! the per-tick REPLAY READER.
//!
//! ## What this proves that the Stage-2 scaffold could NOT
//!
//! Stage 2 (`af777280`) RECORDS each sent delta chunk in the resend
//! buffer and DROPS it on ack, but the buffer has no reader: a delta whose
//! send is LOST (the server never receives/acks the chunk while the
//! connection stays up) is buffered forever and NEVER retransmitted, so
//! the server's locality view stays stale until the worker reconnects.
//!
//! This test drives the REAL worker send loop
//! (`send_periodic_blobs_available_for_test`), loses a delta on the first
//! tick (the chunk reaches the wire but the server drops it and sends no
//! ack), and proves the SECOND tick — with NO new blob changes and
//! WITHOUT a reconnect (`is_first=false` on every tick) — RETRANSMITS the
//! still-unacked chunk, the server applies it, and the server's locality
//! view CONVERGES. The drop-on-ack then drains the buffer.
//!
//! ## Seams crossed (production composition)
//!
//!   1. **Worker send loop** — `send_periodic_blobs_available_for_test`
//!      (the same associated fn the worker `run` loop calls each tick).
//!   2. **Worker chunker** — `chunk_blobs_available` (forced for deltas so
//!      the small common-case delta rides the ACKED/buffered path).
//!   3. **Worker resend buffer** — `BlobsAvailableResendBuffer` buffer on
//!      send + the per-tick REPLAY READER (the new mechanism).
//!   4. **Wire** — `WorkerApiClientTrait::chunked_message` via the real
//!      `MockWorkerApiClient` handshake.
//!   5. **Server accumulator** —
//!      `BlobsAvailableAccumulator::merge_chunk_outcome` (real server-side
//!      Path-A commit gate).
//!   6. **Server locality view** —
//!      `BlobLocalityMap::register_blobs_iter` (the EXACT primitive
//!      `worker_api_server::handle_blobs_available` runs on the committed
//!      notification's digest path; convergence observable =
//!      `locality_map.has_digest`).
//!   7. **Worker drain-on-ack** — `handle_blobs_available_ack` (drops the
//!      acked slot from the resend buffer).
//!
//! ## Mutation (CLAUDE.md TDD #5)
//!
//! Comment out the body of `replay_unacked_chunks` (or the call to it at
//! the head of `send_periodic_blobs_available`) — i.e. remove the per-tick
//! replay reader. Tick 2 then carries NO new changes and the skip-gate
//! suppresses it, so nothing is retransmitted: the consumer's
//! `expect_chunked_message` wedges waiting for a retransmit that never
//! comes, the `tokio::join!` never completes, and
//! `lost_delta_is_retransmitted_until_acked_and_locality_converges`
//! red-fails at the deadlock-detector `tokio::time::timeout(...).expect(..)`
//! with
//! "timed out: the lost delta was never retransmitted on the no-change tick
//! — the per-tick replay reader (drain-on-ack flip) did not fire".
//! (The downstream `has_digest` convergence assert — "lost delta never
//! retransmitted: locality view stayed stale across a no-change tick with
//! NO reconnect ..." — is the second guard; it is only reached if the
//! producer/consumer somehow complete without converging, so the TIMEOUT
//! message above is the one that actually fires under this mutation.)

use std::sync::Arc;

use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableAck, BlobsAvailableNotification, chunked_message,
};
use nativelink_service::blobs_available_accumulator::{BlobsAvailableAccumulator, MergeOutcome};
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::blob_locality_map::BlobLocalityMap;
use nativelink_util::common::DigestInfo;
use nativelink_worker::local_worker::{
    BlobsAvailableState, BlobsAvailableTestArgs, handle_blobs_available_ack,
    send_periodic_blobs_available_for_test, send_post_action_blobs_available_delta_for_test,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use utils::local_worker_test_utils::MockWorkerApiClient;
use utils::mock_running_actions_manager::MockRunningActionsManager;

/// Deadlock detector budget. The test is pure in-process bookkeeping
/// (no syscalls beyond the tempdir store), so any expiry is a real wedge.
const TIMEOUT: core::time::Duration = core::time::Duration::from_secs(10);

/// The worker's CAS endpoint the locality map keys on. Non-empty so the
/// server-side `register_blobs_iter(endpoint, ..)` registers under a real
/// key (the post-action publish also gates on a non-empty endpoint).
const WORKER_ENDPOINT: &str = "grpc://w-replay:50081";

/// The deterministic test worker token from `from_test_args`. The ack the
/// test echoes MUST carry this so the worker's token guard accepts it.
const TEST_WORKER_TOKEN: u64 = 0xA5A5_A5A5_A5A5_A5A5;

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_ba_replay_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_ba_replay_temp_")
        .tempdir()
        .expect("tempdir");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_dir.path().to_string_lossy().into_owned(),
        temp_path: temp_dir.path().to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy::default()),
        ..Default::default()
    })
    .await
    .expect("create filesystem store");
    (store, content_dir, temp_dir)
}

/// Apply a committed notification's digest path to the locality map the
/// SAME way `worker_api_server::handle_blobs_available` does — registering
/// the worker's reported `digest_infos` under its `worker_cas_endpoint`.
/// This is the convergence observable: after this runs the server "knows"
/// the worker holds those digests (`has_digest` / `lookup_workers`).
fn apply_to_locality_map(locality_map: &mut BlobLocalityMap, n: &BlobsAvailableNotification) {
    let digests = n.digest_infos.iter().filter_map(|info| {
        info.digest
            .as_ref()
            .and_then(|d| DigestInfo::try_from(d.clone()).ok())
    });
    locality_map.register_blobs_iter(&n.worker_cas_endpoint, digests);
}

/// A LOST delta (chunked, the common case once part-2 routes small deltas
/// through the chunker) is RETRANSMITTED on the next send tick until the
/// server acks it, and the server's locality view CONVERGES — all with NO
/// reconnect (`is_first=false` on every tick). This is the exact behavior
/// the Stage-2 dead-letter buffer could not produce (it had no reader).
#[nativelink_test]
async fn lost_delta_is_retransmitted_until_acked_and_locality_converges()
-> Result<(), Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(
        fs_store,
        BlobsAvailableTestArgs {
            cas_endpoint: WORKER_ENDPOINT.to_string(),
            ..Default::default()
        },
    );
    let ram = Arc::new(MockRunningActionsManager::new());

    // The digest the worker will advertise as a DELTA. Seeded into the
    // change tracker so the next (is_first=false) tick computes a delta
    // carrying exactly this digest — the production on-insert path.
    let advertised = DigestInfo::new([0x5Au8; 32], 123);
    state.test_record_added_digest(advertised);

    // The server-side state: real accumulator + real locality map. The
    // worker→server stream is the mock; the consumer plays the server.
    let accumulator = BlobsAvailableAccumulator::new();
    let mut locality_map = BlobLocalityMap::new();

    let mut client = MockWorkerApiClient::new();

    // PRODUCER: the real worker send loop, two steady-state ticks (no
    // reconnect). Tick 1 emits the delta; tick 2 has NO new blob change,
    // so the ONLY thing that can reach the wire on tick 2 is the per-tick
    // replay of the still-unacked chunk.
    let producer = {
        let state = state.clone();
        let ram = ram.clone();
        let mut producer_client = client.clone();
        async move {
            // Tick 1 — the delta send (lost downstream; see consumer).
            send_periodic_blobs_available_for_test(
                &mut producer_client,
                &state,
                &ram,
                /* is_first */ false,
            )
            .await
            .expect("tick 1 (delta) must send");
            // Tick 2 — NO new changes. The replay reader must retransmit
            // the unacked chunk here.
            send_periodic_blobs_available_for_test(
                &mut producer_client,
                &state,
                &ram,
                /* is_first */ false,
            )
            .await
            .expect("tick 2 (replay) must send");
        }
    };

    // CONSUMER: plays the server across the two ticks.
    //   * Tick-1 chunk: ACK the wire send (Ok) but DROP the chunk — the
    //     server never processes it and emits NO ack. This is the LOST
    //     delta. Locality view stays empty.
    //   * Tick-2 chunk(s): feed each into the real accumulator; on the
    //     terminal commit, apply to the locality map (convergence) AND
    //     synthesize the per-chunk ack the real server would send, feeding
    //     it back into the worker's drain-on-ack handler.
    let consumer = {
        let state = state.clone();
        async move {
            // --- Tick 1: lose the delta. A single small delta chunks to
            // one terminal chunk. Drain exactly that one chunk and drop it.
            let lost = client.expect_chunked_message(Ok(())).await;
            let lost_chunk = match lost.payload.expect("tick-1 envelope payload") {
                chunked_message::Payload::BlobsAvailable(c) => c,
                other => panic!("tick-1 payload must be BlobsAvailable; got {other:?}"),
            };
            assert!(
                lost_chunk.is_last,
                "a single small delta must chunk to one terminal chunk"
            );
            // Server dropped it: locality view must still be empty. (Not
            // racy: the locality map only changes when THIS consumer
            // applies a committed notification, which it has not yet done.)
            assert!(
                !locality_map.has_digest(&advertised),
                "after the LOST tick-1 delta the server must NOT yet know the digest"
            );

            // --- Tick 2: the replay. Pull the retransmitted chunk(s),
            // feed the real accumulator, commit → converge → ack.
            // A single-chunk broadcast commits on its one terminal chunk.
            let replayed = client.expect_chunked_message(Ok(())).await;
            let replayed_chunk = match replayed.payload.expect("tick-2 envelope payload") {
                chunked_message::Payload::BlobsAvailable(c) => c,
                other => panic!("tick-2 payload must be BlobsAvailable; got {other:?}"),
            };
            // The replay MUST be the SAME (broadcast_id, sequence) as the
            // lost chunk — otherwise the drain-on-ack key would never match
            // and the buffer would leak.
            assert_eq!(
                (replayed_chunk.broadcast_id, replayed_chunk.sequence),
                (lost_chunk.broadcast_id, lost_chunk.sequence),
                "the replay must retransmit the SAME (broadcast_id, sequence) so drain-on-ack matches"
            );
            let ack = BlobsAvailableAck {
                broadcast_id: replayed_chunk.broadcast_id,
                sequence: replayed_chunk.sequence,
                worker_instance_token: replayed_chunk.worker_instance_token,
            };
            match accumulator.merge_chunk_outcome(replayed_chunk) {
                MergeOutcome::Accepted(Some(committed)) => {
                    // Server commits the assembled notification → locality
                    // view converges.
                    apply_to_locality_map(&mut locality_map, &committed);
                }
                MergeOutcome::Accepted(None) => {
                    panic!("single-chunk replay must be a terminal commit, not a non-terminal accept")
                }
                MergeOutcome::Dropped => {
                    panic!("the replayed chunk must be ACCEPTED by the server accumulator")
                }
            }
            // The real server acks an accepted chunk; feed it back so the
            // worker drains the slot (drain-on-ack).
            handle_blobs_available_ack(&state, &ack);

            (locality_map, advertised)
        }
    };

    let (locality_map, advertised) = tokio::time::timeout(TIMEOUT, async {
        let (_p, c) = tokio::join!(producer, consumer);
        c
    })
    .await
    .expect(
        "timed out: the lost delta was never retransmitted on the no-change tick — \
         the per-tick replay reader (drain-on-ack flip) did not fire",
    );

    // CONVERGENCE: the server now knows the worker holds the digest, with
    // NO reconnect (every tick was is_first=false). This is the property
    // the Stage-2 scaffold could not deliver.
    assert!(
        locality_map.has_digest(&advertised),
        "lost delta never retransmitted: locality view stayed stale across a no-change tick \
         with NO reconnect — the per-tick replay reader is missing (drain-on-ack flip not wired)"
    );
    let workers = locality_map.lookup_workers(&advertised);
    assert_eq!(
        workers.len(),
        1,
        "exactly the one worker endpoint must own the converged digest"
    );
    assert_eq!(
        workers[0].as_ref(),
        WORKER_ENDPOINT,
        "the converged digest must be attributed to the worker's reported cas endpoint"
    );

    // DRAIN-ON-ACK: once acked, the slot is dropped — the buffer does not
    // leak across the converged round-trip.
    assert_eq!(
        state.test_resend_buffer_len(),
        0,
        "the acked chunk must be drained from the resend buffer (drain-on-ack)"
    );
    Ok(())
}

/// A post-action output-digest delta (the `make_publish_future` publish
/// site, `local_worker.rs:~4378`) is routed through the ACKED chunked
/// path AND buffered for replay — so a lost post-action delta is
/// retransmitted by the periodic replay reader instead of being lost.
/// Pre-fix this site called the fire-and-forget `blobs_available()` which
/// the server NEVER acks.
#[nativelink_test]
async fn post_action_delta_routes_through_chunked_path_and_buffers_for_replay()
-> Result<(), Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(
        fs_store,
        BlobsAvailableTestArgs {
            cas_endpoint: WORKER_ENDPOINT.to_string(),
            ..Default::default()
        },
    );

    let output_digest = DigestInfo::new([0xC3u8; 32], 77);
    let notification = BlobsAvailableNotification {
        worker_cas_endpoint: WORKER_ENDPOINT.to_string(),
        // The production post-action publish populates the `digests` field
        // (proto field 2), which the chunker folds into `digest_infos`.
        digests: vec![ProtoDigest::from(output_digest)],
        is_full_snapshot: false,
        ..Default::default()
    };

    let mut client = MockWorkerApiClient::new();

    // PRODUCER: the post-action publish routing. A small post-action delta
    // chunks to one terminal chunk on the ACKED path.
    let producer = {
        let state = state.clone();
        let mut producer_client = client.clone();
        async move {
            send_post_action_blobs_available_delta_for_test::<_, MockRunningActionsManager>(
                &mut producer_client,
                Some(&state),
                notification,
            )
            .await
            .expect("post-action delta routing must send");
        }
    };

    // CONSUMER: assert the post-action delta arrived as a ChunkedMessage
    // (NOT the fire-and-forget `blobs_available()` non-acked path) carrying
    // the output digest.
    let consumer = async move {
        let envelope = client.expect_chunked_message(Ok(())).await;
        match envelope.payload.expect("post-action ChunkedMessage payload") {
            chunked_message::Payload::BlobsAvailable(chunk) => {
                assert!(
                    chunk.is_last,
                    "a small post-action delta must chunk to one terminal chunk"
                );
                let digests: Vec<DigestInfo> = chunk
                    .digests
                    .into_iter()
                    .filter_map(|i| i.digest.and_then(|d| DigestInfo::try_from(d).ok()))
                    .collect();
                assert!(
                    digests.contains(&output_digest),
                    "the post-action chunk must carry the action's output digest; got {digests:?}"
                );
            }
            other => panic!(
                "post-action delta MUST ride the chunked ACKED path, not the fire-and-forget \
                 blobs_available() send; got {other:?}"
            ),
        }
    };

    tokio::time::timeout(TIMEOUT, async {
        tokio::join!(producer, consumer);
    })
    .await
    .expect("post-action delta routing wedged");

    // The post-action chunk must be BUFFERED for drain-on-ack (no ack was
    // delivered, so it stays). Both tasks are joined, so this is not racy.
    assert_eq!(
        state.test_resend_buffer_len(),
        1,
        "the post-action delta chunk must be buffered for replay (drain-on-ack), not \
         fire-and-forget — a lost post-action delta would otherwise never retransmit"
    );
    Ok(())
}

/// A worker with NO BlobsAvailable reporting state (`None`) falls back to
/// the legacy fire-and-forget `blobs_available()` send for the post-action
/// delta — unchanged behavior for that configuration (the existing
/// post-action tests rely on it). This pins the `None` fallback so a future
/// change can't silently drop it (which would break workers without a fast
/// store).
#[nativelink_test]
async fn post_action_delta_falls_back_to_raw_send_when_no_state() -> Result<(), Error> {
    let output_digest = DigestInfo::new([0xD4u8; 32], 88);
    let notification = BlobsAvailableNotification {
        worker_cas_endpoint: WORKER_ENDPOINT.to_string(),
        digests: vec![ProtoDigest::from(output_digest)],
        is_full_snapshot: false,
        ..Default::default()
    };

    let mut client = MockWorkerApiClient::new();

    let producer = {
        let mut producer_client = client.clone();
        async move {
            send_post_action_blobs_available_delta_for_test::<_, MockRunningActionsManager>(
                &mut producer_client,
                None,
                notification,
            )
            .await
            .expect("post-action fallback must send");
        }
    };

    // With None state the legacy `blobs_available()` call must fire (NOT a
    // ChunkedMessage). `expect_blobs_available` panics if it sees any other
    // call variant, so this asserts the fallback path exactly.
    let consumer = async move {
        let n = client.expect_blobs_available(Ok(())).await;
        let digests: Vec<DigestInfo> = n
            .digests
            .into_iter()
            .filter_map(|d| DigestInfo::try_from(d).ok())
            .collect();
        assert!(
            digests.contains(&output_digest),
            "the raw-fallback notification must carry the output digest; got {digests:?}"
        );
    };

    tokio::time::timeout(TIMEOUT, async {
        tokio::join!(producer, consumer);
    })
    .await
    .expect("post-action fallback routing wedged");
    Ok(())
}
