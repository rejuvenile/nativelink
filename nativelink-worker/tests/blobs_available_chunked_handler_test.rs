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

//! (#99 — S2 from the 8fa531ae code-reviewer pass / testing-czar
//! CRIT-4) Worker-crate production-composition test for the chunked
//! `BlobsAvailable` emit path.
//!
//! ## Why this exists alongside the server-side e2e test
//!
//! The server-side `nativelink-service/tests/blobs_available_chunked_e2e_test.rs`
//! covers the same chunker → wire → server accumulator → locality_map
//! seam end-to-end; this test adds the **worker-crate** anchor on the
//! same seam, exercising the previously-dead-code
//! [`MockWorkerApiClient::expect_chunked_message`] helper at
//! `tests/utils/local_worker_test_utils.rs:202-219` (testing-czar
//! CRIT-4: "no consumer of the helper exists; the worker-side tests
//! exercise BIS but not BlobsAvailable chunked emission").
//!
//! ## Seams crossed
//!
//! 1. **Producer** —
//!    [`nativelink_util::blobs_available_chunking::chunk_blobs_available`]
//!    (the SAME chunker the worker emit loop calls at
//!    `nativelink-worker/src/local_worker.rs:1765`).
//! 2. **Worker wire shim** —
//!    [`nativelink_worker::worker_api_client_wrapper::WorkerApiClientTrait::chunked_message`]
//!    on the test mock at
//!    `tests/utils/local_worker_test_utils.rs:352-373`. This is the
//!    exact trait method the worker emit loop calls at
//!    `local_worker.rs:1802`.
//! 3. **Wire envelope** —
//!    `Update::ChunkedMessage(ChunkedMessage { payload: Some(
//!    chunked_message::Payload::BlobsAvailable(chunk)) })`, exactly as
//!    the server's dispatch arm (`worker_api_server.rs:876-928`)
//!    decodes.
//! 4. **Server accumulator** —
//!    [`nativelink_service::blobs_available_accumulator::BlobsAvailableAccumulator::merge_chunk`]
//!    consuming the chunks the worker emitted.
//! 5. **Path A commit** — terminal chunk + sequence-completeness gate
//!    fires the consolidated `BlobsAvailableNotification` return.
//!
//! ## Mutation step
//!
//! - Comment out the `is_last = true` set on the final chunk produced
//!   by [`nativelink_util::blobs_available_chunking::chunk_blobs_available`]:
//!   the happy-path test red-fails because the accumulator never sees
//!   a terminal and `merge_chunk` never returns `Some(notification)`
//!   (caught by the `tokio::time::timeout` deadlock detector with the
//!   bespoke "must commit terminal within 5s" expectation).
//! - Mutate the worker-side mock's `chunked_message` to drop the
//!   payload before sending: the happy-path notification arrives with
//!   missing entries (caught by the `digest_infos.len() == N`
//!   assertion).

use core::sync::atomic::Ordering;
use core::time::Duration;

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobDigestInfo, BlobsAvailableNotification, ChunkedMessage, chunked_message,
};
use nativelink_service::blobs_available_accumulator::BlobsAvailableAccumulator;
use nativelink_util::blobs_available_chunking::chunk_blobs_available;
use nativelink_worker::worker_api_client_wrapper::WorkerApiClientTrait;
use utils::local_worker_test_utils::MockWorkerApiClient;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

/// Wall-clock budget for end-to-end chunk round-trip — the test is
/// pure CPU bookkeeping (no syscalls) so 5s is generous and any
/// expiration indicates a real wedge.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Build N synthetic `BlobDigestInfo`s with distinct hashes.
fn make_digests(n: usize) -> Vec<BlobDigestInfo> {
    (0..n)
        .map(|i| {
            let mut hash_bytes = [0u8; 32];
            hash_bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            BlobDigestInfo {
                digest: Some(ProtoDigest {
                    hash: hex::encode(hash_bytes),
                    size_bytes: i64::try_from(i + 1).unwrap_or(1),
                }),
                ts_boot_epoch: 0,
                ts_counter: 0,
            }
        })
        .collect()
}

/// Build a notification carrying `n` digests + canonical header
/// scalars.
fn make_notification(n: usize, endpoint: &str) -> BlobsAvailableNotification {
    BlobsAvailableNotification {
        worker_cas_endpoint: endpoint.to_string(),
        digest_infos: make_digests(n),
        is_full_snapshot: true,
        is_full_subtree_snapshot: true,
        cpu_load_pct: 42,
        p_core_load_pct: 33,
        e_core_load_pct: 17,
        mirror_used_bytes: 1234,
        mirror_max_bytes: 5678,
        ..Default::default()
    }
}

/// Drive the worker emit loop's chunker → mock-client → server
/// accumulator pipeline.
///
/// On the consumer side: spawn a task that drains every recorded
/// `ChunkedMessage` from the mock, decodes the inner
/// `BlobsAvailableChunk`, and feeds it to `accumulator.merge_chunk`.
///
/// Returns the assembled `BlobsAvailableNotification` IF the
/// accumulator committed (terminal chunk + sequence-completeness gate
/// passed). The future is wrapped in a `tokio::time::timeout` deadlock
/// detector at the call site with bespoke `.expect(...)` per CLAUDE.md
/// "tokio::time::Elapsed passes is_err() and masks the bug".
async fn pump_chunks_into_accumulator(
    client: MockWorkerApiClient,
    accumulator: &BlobsAvailableAccumulator,
    expected_chunk_count: usize,
) -> Option<BlobsAvailableNotification> {
    let mut commit: Option<BlobsAvailableNotification> = None;
    for _ in 0..expected_chunk_count {
        let envelope = client.expect_chunked_message(Ok(())).await;
        let payload = envelope.payload.expect("ChunkedMessage missing payload");
        let chunk = match payload {
            chunked_message::Payload::BlobsAvailable(c) => c,
            other => panic!(
                "unexpected payload variant — worker emit-path on \
                 BlobsAvailable MUST send `chunked_message::Payload::BlobsAvailable`; \
                 saw {other:?}"
            ),
        };
        if let Some(notification) = accumulator.merge_chunk(chunk) {
            commit = Some(notification);
        }
    }
    commit
}

/// Happy path: worker chunker produces 3 chunks, mock client forwards
/// each to the server accumulator, terminal commits, full payload
/// reassembled.
///
/// `chunk_blobs_available(notification, broadcast_id, token, store, max_per_chunk=2)`
/// with 5 digests yields ceil(5/2) = 3 chunks (sequences 0,1,2; 2/2/1
/// digests; sequence 2 has `is_last=true`).
#[nativelink_test]
async fn happy_path_three_chunks_round_trip_via_mock_client() -> Result<(), Error> {
    let mock = MockWorkerApiClient::new();
    let accumulator = BlobsAvailableAccumulator::new();

    const N_DIGESTS: usize = 5;
    const MAX_PER_CHUNK: usize = 2;
    const BROADCAST_ID: u64 = 1;
    const TOKEN: u64 = 0xDEAD_BEEF_CAFE_BABE;
    const ENDPOINT: &str = "grpc://w-test:50081";

    let notification = make_notification(N_DIGESTS, ENDPOINT);
    let chunks = chunk_blobs_available(
        notification,
        BROADCAST_ID,
        TOKEN,
        String::new(),
        MAX_PER_CHUNK,
    )
    .expect("chunker must succeed within MAX_SEQUENCES");
    assert_eq!(
        chunks.len(),
        3,
        "5 digests / 2 per chunk = 3 chunks expected; got {}",
        chunks.len()
    );

    // Producer task: send each chunk through the mock's
    // `chunked_message` (the same trait method the worker emit loop
    // calls at local_worker.rs:1802).
    let producer_mock = mock.clone();
    let producer_task = tokio::spawn(async move {
        let mut producer = producer_mock;
        for chunk in chunks {
            let envelope = ChunkedMessage {
                payload: Some(chunked_message::Payload::BlobsAvailable(chunk)),
            };
            let res: Result<(), Error> =
                producer.chunked_message(envelope).await;
            res.expect("mock chunked_message must Ok on happy path");
        }
    });

    let commit = tokio::time::timeout(
        TIMEOUT,
        pump_chunks_into_accumulator(mock, &accumulator, 3),
    )
    .await
    .expect(
        "happy path: 3-chunk worker→server round-trip MUST complete \
         within 5s — deadlock or wedge in mock-client / accumulator pipeline",
    );

    producer_task
        .await
        .expect("producer task must not panic on happy path");

    let notification = commit.expect(
        "terminal chunk MUST commit — sequence-completeness gate failed \
         OR worker emit path didn't mark sequence=2 as is_last=true",
    );
    assert_eq!(
        notification.digest_infos.len(),
        N_DIGESTS,
        "round-trip must preserve all {N_DIGESTS} digests; saw {}",
        notification.digest_infos.len()
    );
    assert_eq!(
        notification.worker_cas_endpoint, ENDPOINT,
        "header scalar (worker_cas_endpoint) from sequence=0 must survive reassembly"
    );
    assert_eq!(
        notification.cpu_load_pct, 42,
        "header scalar (cpu_load_pct) from sequence=0 must survive reassembly"
    );
    assert_eq!(
        notification.mirror_used_bytes, 1234,
        "header scalar (mirror_used_bytes) from sequence=0 must survive reassembly"
    );
    assert!(
        notification.is_full_snapshot,
        "is_full_snapshot must round-trip"
    );
    Ok(())
}

/// Token-mismatch mid-broadcast: worker sends seq=0 (token=A), then
/// seq=0 (token=B), then seq=1 terminal (token=B). Server accumulator
/// MUST commit only the second broadcast (the first is wiped by the
/// token-mismatch rebuild path).
///
/// Also exercises the `dropped_token_mismatch` per-reason counter so
/// regressions to the token-mismatch arm fire the bookkeeping
/// invariant fixed by `token_mismatch_drift_total_accumulated_consistency`
/// in the accumulator unit tests.
#[nativelink_test]
async fn token_mismatch_mid_broadcast_only_second_commits() -> Result<(), Error> {
    let mock = MockWorkerApiClient::new();
    let accumulator = BlobsAvailableAccumulator::new();

    const BROADCAST_ID: u64 = 7;
    const TOKEN_A: u64 = 0xAAAA_AAAA_AAAA_AAAA;
    const TOKEN_B: u64 = 0xBBBB_BBBB_BBBB_BBBB;
    const ENDPOINT_A: &str = "grpc://w-old:50081";
    const ENDPOINT_B: &str = "grpc://w-new:50081";

    // First broadcast (token A): single seq=0 chunk, NOT terminal.
    let notif_a = make_notification(3, ENDPOINT_A);
    let chunks_a = chunk_blobs_available(notif_a, BROADCAST_ID, TOKEN_A, String::new(), 8)
        .expect("chunker must succeed");
    assert_eq!(
        chunks_a.len(),
        1,
        "3 digests / 8 per chunk = 1 chunk; chunker terminates the only chunk as is_last=true"
    );
    // The first broadcast's only chunk has `is_last=true` because the
    // chunker considers it the terminal of its single-chunk broadcast.
    // For the token-mismatch test we want a NON-terminal first chunk
    // so the broadcast is still in-flight when token B arrives.
    // Construct manually instead.
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsAvailableChunk;
    let chunk_a_seq0 = BlobsAvailableChunk {
        broadcast_id: BROADCAST_ID,
        sequence: 0,
        is_last: false,
        worker_instance_token: TOKEN_A,
        store_id: String::new(),
        is_full_snapshot: true,
        worker_cas_endpoint: ENDPOINT_A.to_string(),
        is_full_subtree_snapshot: true,
        cpu_load_pct: 11,
        p_core_load_pct: 0,
        e_core_load_pct: 0,
        mirror_used_bytes: 100,
        mirror_max_bytes: 200,
        digests: make_digests(3),
        cached_directory_digests: Vec::new(),
        pinned_mirror_entries: Vec::new(),
        pinned_ac_mirror_entries: Vec::new(),
        evicted_digests: Vec::new(),
        evicted_blob_infos: Vec::new(),
        added_subtree_digests: Vec::new(),
        removed_subtree_digests: Vec::new(),
        pinned_mirror_digests: Vec::new(),
        indefinite_pin_saturated: false,
        swap_used_bytes: 0,
        memory_pressure_level: 0,
        memory_pressured: false,
        available_disk_bytes: 0,
        disk_pressured: false,
        construct_latency_ms_mean: 0,
    };

    // Second broadcast (token B): seq=0 (rebuild) + seq=1 terminal.
    let notif_b = make_notification(7, ENDPOINT_B);
    let chunks_b = chunk_blobs_available(notif_b, BROADCAST_ID, TOKEN_B, String::new(), 4)
        .expect("chunker must succeed");
    assert_eq!(
        chunks_b.len(),
        2,
        "7 digests / 4 per chunk = 2 chunks expected"
    );

    // Producer task pumps chunk A then both chunks B, all via the
    // mock's chunked_message wire shim.
    let producer_mock = mock.clone();
    let producer_task = tokio::spawn(async move {
        let mut producer = producer_mock;
        for chunk in core::iter::once(chunk_a_seq0).chain(chunks_b.into_iter()) {
            let envelope = ChunkedMessage {
                payload: Some(chunked_message::Payload::BlobsAvailable(chunk)),
            };
            producer
                .chunked_message(envelope)
                .await
                .expect("mock chunked_message must Ok");
        }
    });

    let commit = tokio::time::timeout(
        TIMEOUT,
        pump_chunks_into_accumulator(mock, &accumulator, 3),
    )
    .await
    .expect(
        "token-mismatch rebuild: 3 chunks (1 + 2) MUST drain and commit \
         the second broadcast within 5s",
    );

    producer_task
        .await
        .expect("producer task must not panic");

    let notification = commit.expect(
        "second broadcast (token B) MUST commit on its terminal chunk; \
         token-mismatch wipe must not block the rebuild from committing",
    );
    assert_eq!(
        notification.digest_infos.len(),
        7,
        "committed broadcast must contain second broadcast's 7 digests, \
         NOT first broadcast's 3 (token-mismatch wipe must have dropped \
         the stale partial)"
    );
    assert_eq!(
        notification.worker_cas_endpoint, ENDPOINT_B,
        "header scalars must come from the rebuilt broadcast (token B), \
         not the wiped one (token A)"
    );
    let drops = &accumulator.drop_counts;
    assert_eq!(
        drops.dropped_token_mismatch.load(Ordering::Relaxed),
        1,
        "dropped_token_mismatch counter must increment exactly ONCE \
         on token A → token B rebuild"
    );
    assert_eq!(
        drops.dropped_incomplete_sequence.load(Ordering::Relaxed),
        0,
        "no incomplete-sequence drop expected — token B's broadcast \
         sent both seq 0 and seq 1 contiguously"
    );
    Ok(())
}

/// Sequence-gap rejection: worker sends seq=0 (NOT terminal), then
/// seq=2 terminal (skipping seq=1). The server accumulator's
/// completeness gate (Fix #1, invariant-prover BLOCK) MUST reject
/// the partial commit; the `dropped_incomplete_sequence` counter MUST
/// increment exactly once; no `BlobsAvailableNotification` is
/// produced.
#[nativelink_test]
async fn sequence_gap_terminal_rejected_by_completeness_gate() -> Result<(), Error> {
    let mock = MockWorkerApiClient::new();
    let accumulator = BlobsAvailableAccumulator::new();

    const BROADCAST_ID: u64 = 42;
    const TOKEN: u64 = 0xC0DE_C0DE_C0DE_C0DE;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsAvailableChunk;

    let chunk0 = BlobsAvailableChunk {
        broadcast_id: BROADCAST_ID,
        sequence: 0,
        is_last: false,
        worker_instance_token: TOKEN,
        store_id: String::new(),
        is_full_snapshot: true,
        worker_cas_endpoint: "grpc://w-gap:50081".to_string(),
        is_full_subtree_snapshot: false,
        cpu_load_pct: 0,
        p_core_load_pct: 0,
        e_core_load_pct: 0,
        mirror_used_bytes: 0,
        mirror_max_bytes: 0,
        digests: make_digests(2),
        cached_directory_digests: Vec::new(),
        pinned_mirror_entries: Vec::new(),
        pinned_ac_mirror_entries: Vec::new(),
        evicted_digests: Vec::new(),
        evicted_blob_infos: Vec::new(),
        added_subtree_digests: Vec::new(),
        removed_subtree_digests: Vec::new(),
        pinned_mirror_digests: Vec::new(),
        indefinite_pin_saturated: false,
        swap_used_bytes: 0,
        memory_pressure_level: 0,
        memory_pressured: false,
        available_disk_bytes: 0,
        disk_pressured: false,
        construct_latency_ms_mean: 0,
    };
    // Skip sequence 1; seq 2 is terminal — completeness gate must
    // reject.
    let chunk2 = BlobsAvailableChunk {
        sequence: 2,
        is_last: true,
        worker_cas_endpoint: String::new(),
        is_full_subtree_snapshot: false,
        cpu_load_pct: 0,
        p_core_load_pct: 0,
        e_core_load_pct: 0,
        mirror_used_bytes: 0,
        mirror_max_bytes: 0,
        digests: make_digests(2),
        ..chunk0.clone()
    };

    let producer_mock = mock.clone();
    let producer_task = tokio::spawn(async move {
        let mut producer = producer_mock;
        for chunk in [chunk0, chunk2] {
            let envelope = ChunkedMessage {
                payload: Some(chunked_message::Payload::BlobsAvailable(chunk)),
            };
            producer
                .chunked_message(envelope)
                .await
                .expect("mock chunked_message must Ok");
        }
    });

    let commit = tokio::time::timeout(
        TIMEOUT,
        pump_chunks_into_accumulator(mock, &accumulator, 2),
    )
    .await
    .expect(
        "sequence-gap test: 2 chunks must drain through the pipeline \
         within 5s even though the terminal commit is rejected",
    );

    producer_task
        .await
        .expect("producer task must not panic");

    assert!(
        commit.is_none(),
        "sequence-completeness gate MUST reject terminal-with-gap; \
         saw committed notification when worker emitted seq 0 then \
         seq 2 (skipping seq 1) — Fix #1 (invariant-prover BLOCK) \
         regression"
    );
    assert_eq!(
        accumulator
            .drop_counts
            .dropped_incomplete_sequence
            .load(Ordering::Relaxed),
        1,
        "incomplete-sequence drop counter must fire exactly once on \
         a sequence-gap terminal"
    );
    Ok(())
}
