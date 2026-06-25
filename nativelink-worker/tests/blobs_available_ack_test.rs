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

//! (FL-688 v3 §3.8) Worker-side `BlobsAvailableAck` drain-on-ack tests.
//!
//! These cover the worker's half of the reliable worker→server delta:
//! the worker buffers each unacked `BlobsAvailableChunk` it sent into a
//! bounded resend buffer keyed by `(broadcast_id, sequence)` and clears
//! the matching slot ONLY when the server's `BlobsAvailableAck` arrives.
//!
//!   1. **`ack_drops_matching_buffered_chunk`**: a buffered chunk is
//!      dropped from the resend buffer when its matching ack arrives,
//!      and the drop is PER-CHUNK — an ack for `(b, 0)` does NOT drop a
//!      sibling `(b, 1)` chunk from the same broadcast.
//!   2. **`stale_worker_token_ack_is_dropped`**: an ack echoing a
//!      worker_instance_token that is NOT the worker's current token is
//!      ignored (the buffered chunk stays — symmetric to BisAck's
//!      server_instance_token guard across a worker bounce).
//!   3. **`token_zero_ack_is_dropped`**: an ack with
//!      worker_instance_token=0 (uninitialised) is ignored.
//!   4. **`unknown_ack_is_harmless_noop`**: an ack for a
//!      `(broadcast_id, sequence)` not in the buffer is a silent no-op
//!      (the resend buffer is unchanged; idempotent under a resend that
//!      crosses an in-flight ack).
//!   5. **`over_cap_send_forces_full_snapshot_reset`**: buffering past
//!      `BLOBS_AVAILABLE_RESEND_MAX_CHUNKS` clears the buffer and signals
//!      the caller to force a fresh full snapshot (the self-correcting
//!      reset that bounds the buffer — the convergence the removed 60s
//!      heartbeat used to provide, now triggered by buffer pressure).
//!
//! Production-composition: each test drives the real
//! `BlobsAvailableState` resend buffer + the real
//! `handle_blobs_available_ack` handler (the same function the
//! `Update::BlobsAvailableAck` dispatch arm calls), so the test crosses
//! the same token-guard + per-chunk-drop code as production.
//!
//! Mutation guidance (the bespoke assertion messages each mutation must
//! re-trip):
//!   * Drop the per-chunk `(broadcast_id, sequence)` key in
//!     `BlobsAvailableResendBuffer::ack` and clear the whole broadcast
//!     instead → `ack_drops_matching_buffered_chunk` MUST fail with
//!     "PER-CHUNK ack must NOT drop a sibling chunk".
//!   * Remove the `ack.worker_instance_token != state.worker_instance_token`
//!     guard in `handle_blobs_available_ack` →
//!     `stale_worker_token_ack_is_dropped` MUST fail with "stale-token
//!     ack must NOT drop a buffered chunk".
//!   * Remove the `worker_instance_token == 0` reject →
//!     `token_zero_ack_is_dropped` MUST fail with "token-0 ack must NOT
//!     drop a buffered chunk".
//!   * Remove the over-cap → clear+force-snapshot valve in
//!     `BlobsAvailableResendBuffer::add` →
//!     `over_cap_send_forces_full_snapshot_reset` MUST fail with "over-cap
//!     buffer grew unbounded; full-snapshot reset valve missing".

use std::sync::Arc;

use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableAck, BlobsAvailableChunk,
};
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_worker::local_worker::{
    BLOBS_AVAILABLE_RESEND_MAX_CHUNKS, BlobsAvailableState, BlobsAvailableTestArgs,
    handle_blobs_available_ack,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;

/// The deterministic test worker token from `from_test_args`.
const TEST_WORKER_TOKEN: u64 = 0xA5A5_A5A5_A5A5_A5A5;

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_ba_ack_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_ba_ack_temp_")
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

/// A delta chunk with the test worker token, distinct per (broadcast, sequence).
fn delta_chunk(broadcast_id: u64, sequence: u32, is_last: bool) -> BlobsAvailableChunk {
    BlobsAvailableChunk {
        broadcast_id,
        sequence,
        is_last,
        worker_instance_token: TEST_WORKER_TOKEN,
        is_full_snapshot: false,
        ..Default::default()
    }
}

/// 1. An ack drops exactly the matching buffered chunk — and NOT a sibling.
#[nativelink_test]
async fn ack_drops_matching_buffered_chunk() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());

    // Two delta chunks of ONE broadcast buffered (drain-on-ack premise:
    // the worker keeps them until the server acks each).
    state.test_buffer_delta_chunk(delta_chunk(42, 0, false));
    state.test_buffer_delta_chunk(delta_chunk(42, 1, true));
    assert_eq!(
        state.test_resend_buffer_len(),
        2,
        "both delta chunks must be buffered before any ack (drain-on-ack)"
    );

    // Ack only (42, 0): the matching chunk drops, the sibling (42, 1) stays.
    handle_blobs_available_ack(
        &state,
        &BlobsAvailableAck {
            broadcast_id: 42,
            sequence: 0,
            worker_instance_token: TEST_WORKER_TOKEN,
        },
    );
    assert_eq!(
        state.test_resend_buffer_len(),
        1,
        "ack for (42,0) must drop exactly one chunk"
    );
    assert!(
        !state.test_resend_buffer_contains(42, 0),
        "the acked (42,0) chunk must be gone"
    );
    assert!(
        state.test_resend_buffer_contains(42, 1),
        "PER-CHUNK ack must NOT drop a sibling chunk (42,1) of the same broadcast"
    );

    // Ack the sibling too → buffer empties.
    handle_blobs_available_ack(
        &state,
        &BlobsAvailableAck {
            broadcast_id: 42,
            sequence: 1,
            worker_instance_token: TEST_WORKER_TOKEN,
        },
    );
    assert_eq!(
        state.test_resend_buffer_len(),
        0,
        "acking both chunks empties the resend buffer"
    );
    Ok(())
}

/// 2. An ack echoing a non-current worker token is ignored (worker bounce).
#[nativelink_test]
async fn stale_worker_token_ack_is_dropped() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());

    state.test_buffer_delta_chunk(delta_chunk(9, 0, true));
    assert_eq!(state.test_resend_buffer_len(), 1, "chunk buffered");

    // Server echoes a STALE token (a different worker process). The
    // worker must not let this drop the current process's buffered chunk.
    handle_blobs_available_ack(
        &state,
        &BlobsAvailableAck {
            broadcast_id: 9,
            sequence: 0,
            worker_instance_token: 0x1234_5678_9ABC_DEF0,
        },
    );
    assert_eq!(
        state.test_resend_buffer_len(),
        1,
        "stale-token ack must NOT drop a buffered chunk"
    );
    Ok(())
}

/// 3. An ack with worker_instance_token=0 (uninitialised) is ignored.
#[nativelink_test]
async fn token_zero_ack_is_dropped() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());

    state.test_buffer_delta_chunk(delta_chunk(3, 0, true));
    assert_eq!(state.test_resend_buffer_len(), 1, "chunk buffered");

    handle_blobs_available_ack(
        &state,
        &BlobsAvailableAck {
            broadcast_id: 3,
            sequence: 0,
            worker_instance_token: 0,
        },
    );
    assert_eq!(
        state.test_resend_buffer_len(),
        1,
        "token-0 ack must NOT drop a buffered chunk"
    );
    Ok(())
}

/// 4. An ack for a non-buffered (broadcast,sequence) is a harmless no-op.
#[nativelink_test]
async fn unknown_ack_is_harmless_noop() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());

    state.test_buffer_delta_chunk(delta_chunk(1, 0, true));
    assert_eq!(state.test_resend_buffer_len(), 1, "chunk buffered");

    // Ack a broadcast/sequence the buffer never held (a duplicate ack
    // crossing an already-dropped chunk).
    handle_blobs_available_ack(
        &state,
        &BlobsAvailableAck {
            broadcast_id: 999,
            sequence: 7,
            worker_instance_token: TEST_WORKER_TOKEN,
        },
    );
    assert_eq!(
        state.test_resend_buffer_len(),
        1,
        "unknown ack must be a no-op; buffer unchanged"
    );
    assert!(
        state.test_resend_buffer_contains(1, 0),
        "the genuinely-buffered chunk must survive an unknown ack"
    );
    Ok(())
}

/// 5. Buffering past the cap clears the buffer + signals a forced full snapshot.
#[nativelink_test]
async fn over_cap_send_forces_full_snapshot_reset() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());

    // Fill exactly to the cap; each add reports "no reset needed".
    for seq in 0..BLOBS_AVAILABLE_RESEND_MAX_CHUNKS {
        let forced = state.test_buffer_delta_chunk(delta_chunk(1, seq as u32, false));
        assert!(
            !forced,
            "buffering within the cap must NOT force a full snapshot (seq {seq})"
        );
    }
    assert_eq!(
        state.test_resend_buffer_len(),
        BLOBS_AVAILABLE_RESEND_MAX_CHUNKS,
        "buffer must hold exactly the cap before overflow"
    );

    // Before overflow, no forced full snapshot is pending.
    assert!(
        !state.test_take_force_full_snapshot(),
        "no full snapshot should be forced before overflow"
    );

    // One more push exceeds the cap → the valve fires: buffer cleared,
    // caller told to send a fresh full snapshot.
    let forced = state.test_buffer_delta_chunk(delta_chunk(2, 0, false));
    assert!(
        forced,
        "over-cap buffer grew unbounded; full-snapshot reset valve missing"
    );
    assert_eq!(
        state.test_resend_buffer_len(),
        0,
        "over-cap valve must CLEAR the buffer (the full snapshot supersedes all buffered deltas)"
    );
    // The over-cap path must SET the force-full-snapshot flag so the next
    // tick re-converges (the production buffer_delta_chunk seam).
    assert!(
        state.test_take_force_full_snapshot(),
        "over-cap valve must request a forced full snapshot on the next tick"
    );
    Ok(())
}
