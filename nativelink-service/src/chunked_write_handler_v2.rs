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

//! #494-v3 Phase 2: bidi-streaming `WriteChunkedV2` server-side handler.
//!
//! Per-digest multi-writer chunk-race semantics. Up to N writers may
//! concurrently upload the SAME digest; the server admits chunks
//! per-offset and emits per-chunk acks back through the response stream.
//!
//! **Why bidi (vs handshake-then-stream):** the writer must react in
//! real time to "another writer just committed offset N" so it can
//! drop offset N from its send queue. A handshake-only RPC could only
//! gate at session open, missing in-session commits by other writers.
//! Bidi gives the writer a continuous feedback channel; the wasted
//! bandwidth bound (~256 MiB/writer/digest, accepted per design) is
//! the price for the simpler, more reactive shape.
//!
//! **Composability invariants** preserved (CLAUDE.md gate-pin-evict):
//! 1. Mirror-once: the writer that flips the LAST bit in
//!    `chunks_present` runs commit; sibling writers observe via
//!    `commit_done` notify and propagate the same result.
//! 2. Cancel-handoff: a writer's `RaceWriterGuard::drop` purges the
//!    writer's `WriterId` from every `chunks_in_flight[*]` slot;
//!    chunks_present bits stay set; surviving writers can finish
//!    the blob.
//! 3. Commit-runner panic-safety: `CommitRunnerGuard::drop` publishes a
//!    synthetic `Code::Cancelled` Err if the commit-runner exited
//!    without publishing a real result (FIX-1 / FIX-2). Sibling writers
//!    waiting on `commit_done` wake immediately rather than the 60 s
//!    watchdog. The watchdog AwaitCommit branch additionally calls
//!    `force_remove(&digest)` so a fresh writer arriving after a wedge
//!    gets a clean state.
//! 4. BIS / failed_commit sinks (FIX-3, OPTIONAL): when wired via
//!    `with_v2_stable_digests_sink` / `with_v2_failed_commit_sink`, the
//!    commit-runner pushes the digest into BIS on success or into
//!    `failed_slow_writes` on failure. Mirrors the v1 reaper at
//!    `chunked_write_handler.rs:2089-2125`. Tests verify these fire
//!    EXACTLY ONCE per commit (`v2_bis_stable_digests_sink_fires_exactly_once_on_success`).
//!    PinBudget / ChunkBudget integration is NOT wired in this PR
//!    (followup tracker — v2 currently bypasses the existing
//!    admission-budget triangle; integration will require a sibling
//!    of FIX-3 across `try_admit_chunk` to reserve permits).
//!
//! **Backwards compatibility:** clients that don't speak v2 keep using
//! the unary `WriteChunked` RPC; servers that don't support v2 return
//! `Code::Unimplemented` (the bare-handler trait impl AND the
//! `ChunkedCasExtensionsAdapter` honor the `chunked_v2_enabled` config
//! flag — default OFF — so production v2 wiring is opt-in).

#![cfg(feature = "chunked_fast_slow")]

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    WriteChunk, WriteChunkedAck, WriteChunkedFrame, WriteChunkedResponse, watchdog_timeout_signal,
    write_chunked_ack, write_chunked_frame,
};
use nativelink_store::chunked::chunked_race_state::{
    AdmitOutcome, ChunkRaceState, CommitResponsibility, CommitRunnerGuard, RaceCommitResult,
    WriterId,
};
use nativelink_store::chunked_signal::encode_watchdog_timeout_signal_any;
use nativelink_store::filesystem_store::FileEntry;
use nativelink_util::common::DigestInfo;
use nativelink_util::cpu_pool::cpu_pool;
use nativelink_util::digest_hasher::{DigestHasher, default_digest_hasher_func};

use crate::chunked_write_handler::{
    CHUNKED_COMMIT_SOFT_WARN_SECS, CHUNKED_COMMIT_WATCHDOG_SECS, ChunkedWriteHandler,
    ChunkedWriteHandlerMetrics, V2_AWAITER_SOFT_WARN_SEEN,
};

/// Process-wide monotonic counter for minting `WriterId`s. Each v2
/// session at admission grabs a fresh id. The id is per-process (not
/// per-digest) which is fine — the race-state's `chunks_in_flight` map
/// is keyed by offset, not by writer, and the WriterId is only used
/// inside `purge_writer_in_flight` to identify slots to evacuate.
static WRITER_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_writer_id() -> WriterId {
    WriterId(WRITER_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// Capacity of the per-session ack channel. Bounded so a slow client
/// can't cause unbounded ack accumulation. CAPPED AT N: 64 — at chunk
/// p99 wall-clock 1-10 ms, 64 ack lag = 64-640 ms. If client's ack
/// consumer wedges longer, pwrite-side waits + RPC stream's HTTP/2
/// flow control kicks in.
const ACK_CHANNEL_CAP: usize = 64;

/// Watchdog deadline for waiting on `commit_done` after the last chunk
/// is admitted. If the commit-runner wedges (slow tier hang, panic
/// during BLAKE3 hash), siblings observe `Err(DeadlineExceeded)` and
/// propagate to their clients.
///
/// **#510 consolidation:** previously a local literal
/// `Duration::from_secs(60)` numerically identical to v1's
/// `CHUNKED_COMMIT_WATCHDOG_SECS`. The local literal was a silent-drift
/// risk: v1's `_ASSERT_WATCHDOG_ORDERING` compile-time guard pinned the
/// 30 < 60 < 120 ordering on v1's constant only; a future commit
/// editing v2's literal standalone would have escaped the assert. Now
/// derived from the v1 canonical constant; v2 inherits the same
/// compile-time guard transitively, plus the explicit v2-local assert
/// `_ASSERT_V2_WATCHDOG_TRACKS_V1` below pins the SAME-VALUE invariant
/// against any future v1 edit.
const COMMIT_WAIT_WATCHDOG: Duration = Duration::from_secs(CHUNKED_COMMIT_WATCHDOG_SECS);

/// #510: pin the SAME-VALUE invariant between v2's `COMMIT_WAIT_WATCHDOG`
/// and v1's `CHUNKED_COMMIT_WATCHDOG_SECS`. Any future commit that
/// converts `COMMIT_WAIT_WATCHDOG` back to a literal that disagrees with
/// v1 will red-fail this const-eval at build time. Mirrors v1's
/// `_ASSERT_WATCHDOG_ORDERING` style.
const _ASSERT_V2_WATCHDOG_TRACKS_V1: () = {
    assert!(
        COMMIT_WAIT_WATCHDOG.as_secs() == CHUNKED_COMMIT_WATCHDOG_SECS,
        "#510 invariant: v2's COMMIT_WAIT_WATCHDOG MUST equal v1's \
         CHUNKED_COMMIT_WATCHDOG_SECS so the soft-warn (30 s) / \
         infra-integrity (60 s) / pin-TTL (120 s) ordering on v1 \
         transitively applies to v2; a divergent value here would \
         re-introduce the silent-drift risk #510 closed",
    );
};

/// Server-side response stream type alias. Tokio's `ReceiverStream`
/// over `Result<WriteChunkedFrame, Status>` so per-chunk acks plus the
/// final response (or error) all flow through one channel.
pub type WriteChunkedV2Stream =
    tokio_stream::wrappers::ReceiverStream<Result<WriteChunkedFrame, Status>>;

impl<Fe: FileEntry> ChunkedWriteHandler<Fe> {
    /// #494-v3 Phase 2: bidi `WriteChunkedV2` entry point. Spawns a
    /// per-session task that owns the request/response loop and
    /// returns the receiver-end of the response stream to tonic.
    ///
    /// Wiring: this is invoked from the `CasExtensions::write_chunked_v2`
    /// trait impl on `ChunkedWriteHandler`.
    pub async fn write_chunked_v2(
        self: Arc<Self>,
        request: Request<Streaming<WriteChunk>>,
    ) -> Result<Response<WriteChunkedV2Stream>, Status> {
        // #247+#477 DS-reviewer disambiguation: emit one info! per v2
        // server-side RPC entry so a journal scan can attribute every
        // worker→server WriteChunkedV2 invocation to this wire shape.
        let peer_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        info!(
            target: "nativelink_service::chunked_write_handler_v2",
            writer_path = "server_v2_rpc",
            wire_shape = "v2",
            %peer_addr,
            "WriteChunkedV2 RPC entry",
        );
        let mut stream = request.into_inner();

        // Receive the first chunk so we learn the digest BEFORE
        // creating the response channel — admission failures here can
        // be returned as a unary `Status` instead of via a dangling
        // ack stream.
        let first_chunk = match stream.message().await {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Err(Status::invalid_argument(
                    "WriteChunkedV2: stream closed before any chunks were sent",
                ));
            }
            Err(status) => {
                return Err(status);
            }
        };
        let digest = match parse_digest_v2(&first_chunk) {
            Ok(d) => d,
            Err(err) => return Err(err.into()),
        };

        // Create response channel. Bounded; per ACK_CHANNEL_CAP doc.
        let (frame_tx, frame_rx) = mpsc::channel(ACK_CHANNEL_CAP);

        // Spawn the per-session driver task. It owns:
        //  - the request stream (to consume chunks)
        //  - the response sender (to push acks + final)
        //  - a writer-id + RaceWriterGuard
        let handler = Arc::clone(&self);
        tokio::spawn(async move {
            handler
                .run_v2_session(stream, frame_tx, digest, first_chunk)
                .await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            frame_rx,
        )))
    }

    /// Per-session driver. Consumes chunks from the request stream,
    /// admits them via the per-digest `ChunkRaceState`, pwrites
    /// accepted chunks, emits per-chunk acks, and on commit-trigger
    /// runs (or awaits) the commit path.
    async fn run_v2_session(
        self: Arc<Self>,
        mut stream: Streaming<WriteChunk>,
        frame_tx: mpsc::Sender<Result<WriteChunkedFrame, Status>>,
        digest: DigestInfo,
        first_chunk: WriteChunk,
    ) {
        let chunk_size_u32 = self.chunk_size_for_v2() as u32;

        // Reject zero-size blobs upfront — the v2 path is not the
        // right shape for them. Bazel emits empty digests via
        // ByteStream, not via `WriteChunkedV2`. A v2 client that
        // ships a zero-size digest is a contract violation.
        if digest.size_bytes() == 0 {
            let _ = frame_tx
                .send(Err(Status::invalid_argument(format!(
                    "WriteChunkedV2: zero-size digest {digest} not supported via v2; \
                     use the unary WriteChunked or ByteStream paths",
                ))))
                .await;
            return;
        }

        // FIX-4: Atomic get-or-create + attach via the registry. The
        // previous pattern (separate get_or_create + RaceWriterGuard::attach)
        // had an 8-line TOCTOU window where a concurrent
        // `try_remove_if_unused` could remove the entry between the
        // two calls, splitting concurrent writers across two distinct
        // race-states for the same digest. The combined call holds
        // the registry mutex across both steps.
        let writer_id = next_writer_id();
        let (race_state, race_guard) = self
            .filesystem_store_for_v2()
            .race_state_for_digest_and_attach(&digest, chunk_size_u32, writer_id);
        let mut race_guard = Some(race_guard);

        // H1 (#499 followup): register the digest in the FSS-level
        // `chunked_in_flight_digests` set via an RAII guard so
        // `FastSlowStore::has_with_results(digest)` returns Some(size)
        // for the duration of the v2 session. Without this, an
        // in-flight v2 commit is invisible to FSS::has → FMB returns
        // "missing" → Bazel re-uploads or sees FAILED_PRECONDITION on
        // dependent reads. Drop fires on commit (success / failure)
        // OR cancellation. The cancel-safety contract matches v1's
        // BazelChunkedDispatcher::dispatch path.
        let _v2_inflight_guard = self
            .chunked_in_flight_digests_for_v2()
            .map(|set| {
                crate::chunked_write_handler::InFlightChunkedGuard::new_with_caller(
                    Arc::clone(set),
                    digest,
                    self.in_flight_empty_notify_for_v2().cloned(),
                    "server_v2_session",
                )
            });

        // Update the `chunked_writers_per_digest_max` metric.
        let attached = race_state.attached_writer_count();
        let metrics = self.metrics_for_v2();
        atomic_max(&metrics.chunked_writers_per_digest_max, attached);

        info!(
            target: "nativelink_service::chunked_write_handler_v2",
            ?digest,
            ?writer_id,
            attached,
            "WriteChunkedV2: session opened",
        );

        // Send the ADMITTED_SKIP_TO hint if any chunks are already
        // contiguously committed. Best-effort; ignore send error
        // (client may have hung up).
        if let Some(skip_to) = race_state.admit_skip_to_hint_byte_offset() {
            let frame = WriteChunkedFrame {
                payload: Some(write_chunked_frame::Payload::Ack(WriteChunkedAck {
                    chunk_offset: 0,
                    outcome: write_chunked_ack::Outcome::AdmittedSkipTo as i32,
                    already_have_max_offset: skip_to,
                })),
            };
            let _ = frame_tx.send(Ok(frame)).await;
        }

        // Process the first chunk + every subsequent chunk. We track
        // whether THIS writer should run commit on the final chunk
        // observation.
        let mut commit_responsibility: Option<CommitResponsibility> = None;
        let mut chunk_iter_first = Some(first_chunk);

        loop {
            let chunk_opt = if let Some(c) = chunk_iter_first.take() {
                Some(c)
            } else {
                match stream.message().await {
                    Ok(Some(c)) => Some(c),
                    Ok(None) => None,
                    Err(status) => {
                        warn!(
                            target: "nativelink_service::chunked_write_handler_v2",
                            ?digest,
                            ?writer_id,
                            ?status,
                            "WriteChunkedV2: client stream errored mid-blob",
                        );
                        // Exit the loop; race_guard's Drop purges in-flight slots.
                        let _ = frame_tx
                            .send(Err(Status::aborted(format!(
                                "WriteChunkedV2: client stream errored mid-blob for digest {digest}",
                            ))))
                            .await;
                        return;
                    }
                }
            };
            let Some(chunk) = chunk_opt else { break };

            // Validate digest field consistency (client must keep the
            // same digest across the stream).
            let next_digest = match parse_digest_v2(&chunk) {
                Ok(d) => d,
                Err(err) => {
                    let _ = frame_tx.send(Err(err.into())).await;
                    return;
                }
            };
            if next_digest != digest {
                let _ = frame_tx
                    .send(Err(Status::invalid_argument(format!(
                        "WriteChunkedV2: digest switched mid-stream: started {digest}, got {next_digest}",
                    ))))
                    .await;
                return;
            }

            let chunk_offset = chunk.chunk_offset;
            let chunk_bytes_len = chunk.chunk_bytes.len();
            let finish = chunk.finish_chunk;

            // Validate chunk shape against the race-state.
            if let Err(err) =
                race_state.validate_chunk_shape(chunk_offset, chunk_bytes_len, finish)
            {
                let _ = frame_tx.send(Err(err.into())).await;
                return;
            }

            // Try to admit. Race-state lock taken + released inline.
            let admit_outcome = race_state.try_admit_chunk(writer_id, chunk_offset);
            match admit_outcome {
                AdmitOutcome::AlreadyHave => {
                    metrics
                        .chunked_chunks_already_have_total
                        .fetch_add(1, Ordering::Relaxed);
                    let frame = WriteChunkedFrame {
                        payload: Some(write_chunked_frame::Payload::Ack(WriteChunkedAck {
                            chunk_offset,
                            outcome: write_chunked_ack::Outcome::AlreadyHave as i32,
                            already_have_max_offset: 0,
                        })),
                    };
                    if frame_tx.send(Ok(frame)).await.is_err() {
                        return;
                    }
                    // For the final chunk, transition to AwaitCommit
                    // unconditionally. A writer that lost its final
                    // chunk to ALREADY_HAVE didn't pwrite the last
                    // bit, so it cannot be the commit-runner; await
                    // whoever did flip the last bit to publish.
                    if finish {
                        commit_responsibility = Some(CommitResponsibility::AwaitCommit);
                        break;
                    }
                    continue;
                }
                AdmitOutcome::RacingLoser => {
                    metrics
                        .chunked_chunks_racing_loser_total
                        .fetch_add(1, Ordering::Relaxed);
                    let frame = WriteChunkedFrame {
                        payload: Some(write_chunked_frame::Payload::Ack(WriteChunkedAck {
                            chunk_offset,
                            outcome: write_chunked_ack::Outcome::RacingLoser as i32,
                            already_have_max_offset: 0,
                        })),
                    };
                    if frame_tx.send(Ok(frame)).await.is_err() {
                        return;
                    }
                    // Final chunk: we're done sending, await commit.
                    if finish {
                        commit_responsibility = Some(CommitResponsibility::AwaitCommit);
                        break;
                    }
                    continue;
                }
                AdmitOutcome::Accept => {
                    // Fall through to the pwrite path below.
                }
            }

            // Per-chunk SHA-256 verify before pwrite. Mismatch = rollback
            // the in-flight slot (no bit set), report InvalidArgument
            // to the client.
            let chunk_sha256_arr: [u8; 32] = match chunk.chunk_sha256.as_slice().try_into() {
                Ok(a) => a,
                Err(_) => {
                    race_state.release_chunk_in_flight(writer_id, chunk_offset);
                    let _ = frame_tx
                        .send(Err(Status::invalid_argument(format!(
                            "WriteChunkedV2: chunk_sha256 must be 32 bytes (digest {digest}, offset {chunk_offset}); got {}",
                            chunk.chunk_sha256.len()
                        ))))
                        .await;
                    return;
                }
            };
            let chunk_bytes_for_hash: Bytes = chunk.chunk_bytes.clone();
            let computed_sha = match compute_sha256_blocking_v2(chunk_bytes_for_hash).await {
                Ok(s) => s,
                Err(err) => {
                    race_state.release_chunk_in_flight(writer_id, chunk_offset);
                    let _ = frame_tx.send(Err(err.into())).await;
                    return;
                }
            };
            if computed_sha != chunk_sha256_arr {
                race_state.release_chunk_in_flight(writer_id, chunk_offset);
                metrics
                    .sha256_per_chunk_mismatches_total
                    .fetch_add(1, Ordering::Relaxed);
                let _ = frame_tx
                    .send(Err(Status::invalid_argument(format!(
                        "WriteChunkedV2: per-chunk SHA-256 mismatch for digest {digest} at offset {chunk_offset}",
                    ))))
                    .await;
                return;
            }

            // pwrite the chunk via the FilesystemStore's chunked
            // adapter. Bytes is refcounted; the clone is cheap.
            let pwrite_bytes = chunk.chunk_bytes;
            let pwrite_res = self
                .filesystem_store_for_v2()
                .write_chunk_at_offset(&digest, chunk_offset, pwrite_bytes)
                .await;
            if let Err(err) = pwrite_res {
                race_state.release_chunk_in_flight(writer_id, chunk_offset);
                warn!(
                    target: "nativelink_service::chunked_write_handler_v2",
                    ?digest,
                    ?writer_id,
                    chunk_offset,
                    ?err,
                    "WriteChunkedV2: pwrite failed; aborting writer's session",
                );
                let _ = frame_tx.send(Err(err.into())).await;
                return;
            }

            // Mark committed in the race-state. Returns whether this
            // writer is the commit-runner (last bit flipped + first to
            // observe).
            let resp = race_state.mark_chunk_committed(writer_id, chunk_offset);

            // Send ACCEPTED ack back to client. If `resp == RunCommit`,
            // ignore the send error (client hung up between pwrite and
            // ack): we MUST still run the commit-runner path because
            // sibling writers depend on a published result. Stripping
            // the early-return here closes the wedge mode where a
            // client disconnect after the last bit-flip leaves
            // `commit_running=true` and no published result, which
            // would force every sibling to wait the full watchdog.
            metrics.chunks_admitted_total.fetch_add(1, Ordering::Relaxed);
            let frame = WriteChunkedFrame {
                payload: Some(write_chunked_frame::Payload::Ack(WriteChunkedAck {
                    chunk_offset,
                    outcome: write_chunked_ack::Outcome::Accepted as i32,
                    already_have_max_offset: 0,
                })),
            };
            let send_err = frame_tx.send(Ok(frame)).await.is_err();
            if send_err && !matches!(resp, CommitResponsibility::RunCommit) {
                // Client hung up AND we're not the commit-runner. Abort
                // — race-state guard's Drop purges in-flight markers
                // (chunks_present bit for this offset stays set;
                // surviving writers can still complete the blob).
                return;
            }
            if send_err {
                // RunCommit + send error: log so operators can correlate
                // server-side commit progress with client-disconnect
                // events; do NOT return.
                debug!(
                    target: "nativelink_service::chunked_write_handler_v2",
                    ?digest,
                    ?writer_id,
                    chunk_offset,
                    "WriteChunkedV2: ack-send failed on RunCommit path; \
                     proceeding to commit so siblings observe a result",
                );
            }

            match resp {
                CommitResponsibility::ContinueSending => {
                    if finish {
                        // Producer sent finish_chunk=true but the
                        // bitmap is not full. Wait for it to fill
                        // (other writers may still be sending). In
                        // well-formed multi-writer races, finish=true
                        // on a non-final-bit-flip is a legitimate
                        // signal — this writer is done, others are
                        // still racing. (Reference: design doc
                        // "race-loser flow" §multi-writer; ContinueSending
                        // fires when a writer's pwrite completed but the
                        // bitmap is not yet fully covered, which is the
                        // expected mid-race state.)
                        commit_responsibility = Some(CommitResponsibility::AwaitCommit);
                        break;
                    }
                    continue;
                }
                CommitResponsibility::RunCommit => {
                    commit_responsibility = Some(CommitResponsibility::RunCommit);
                    break;
                }
                CommitResponsibility::AwaitCommit => {
                    commit_responsibility = Some(CommitResponsibility::AwaitCommit);
                    break;
                }
            }
        }

        // At this point the writer has either:
        //  - exhausted the request stream (Ok(None)): commit_responsibility
        //    may be None (writer didn't send finish), or Some(_) (writer
        //    observed the trigger).
        //  - sent a finish chunk: commit_responsibility is Some(_).
        //
        // Multi-writer case: a writer that lost EVERY chunk to RACING_LOSER
        // / ALREADY_HAVE may finish its send loop with the bitmap not yet
        // complete (the winning writer is still mid-pwrite on the final
        // chunks). Such a writer must STILL wait for commit_done — the
        // commit will fire imminently, and the writer's client wants the
        // final response. Surfacing Aborted here would needlessly wedge
        // the client even though the blob WILL be committed.
        let resp = match commit_responsibility {
            Some(r) => r,
            None => {
                // Writer didn't observe a commit trigger during its send
                // loop. Two sub-cases:
                //  (a) bitmap already complete (some other writer flipped
                //      the last bit during our loop) → AwaitCommit.
                //  (b) writer truly aborted (didn't send finish, bitmap
                //      not complete) → relinquish to other writers,
                //      surface Aborted.
                if race_state.is_complete() {
                    CommitResponsibility::AwaitCommit
                } else {
                    debug!(
                        target: "nativelink_service::chunked_write_handler_v2",
                        ?digest,
                        ?writer_id,
                        "WriteChunkedV2: writer ended without finish; bitmap incomplete; \
                         relinquishing to other writers",
                    );
                    let _ = frame_tx
                        .send(Err(Status::aborted(format!(
                            "WriteChunkedV2: writer ended without finish for digest {digest}; \
                             bitmap incomplete at session end",
                        ))))
                        .await;
                    return;
                }
            }
        };

        match resp {
            CommitResponsibility::RunCommit => {
                // We're the commit-runner. Construct the runner-guard
                // FIRST so any panic / cancellation between here and
                // mark_complete() publishes a synthetic Cancelled Err
                // instead of leaving siblings to wedge on the watchdog.
                let runner_guard = CommitRunnerGuard::from_state(Arc::clone(&race_state));

                // Run the full commit path
                // (commit_to_holding → blake3 verify → finalize_holding)
                // and publish the result for sibling writers.
                let commit_result = self
                    .v2_run_commit_path(&digest, &race_state)
                    .await;

                // Publish to siblings BEFORE sending our own response —
                // we want siblings to wake immediately. Race state's
                // Notify::notify_waiters fires synchronously.
                race_state.publish_commit_result(commit_result.clone());
                // Now mark the runner-guard complete; subsequent Drop
                // is a no-op. Order matters: publish FIRST, then
                // mark_complete; if the order were reversed, a panic
                // between them would leave commit_done unpublished AND
                // the guard would skip the synthetic Cancelled Err.
                runner_guard.mark_complete();

                // FIX-3 BIS / failed_writes integration. On commit
                // success: push to stable_digests_sink so the BIS
                // broadcast loop drains worker mirror_blobs. On commit
                // failure: insert into failed_slow_writes via
                // failed_commit_sink so the worker reconnect-retry
                // picks up the digest. Mirrors the v1 reaper at
                // chunked_write_handler.rs:2089-2125.
                self.v2_fire_post_commit_sinks(&digest, &commit_result);

                // FIX-5 cross-writer metric: pull the per-state
                // counter into the exported total so operators see a
                // non-zero `chunked_chunks_accepted_from_cross_writer_total`
                // when a cross-writer race actually completed a chunk.
                v2_pump_cross_writer_metric(&race_state, &metrics);

                // Drop the race_guard EXPLICITLY before trying to remove
                // the registry entry — try_remove_if_unused only removes
                // when attached_writer_count == 0.
                if let Some(guard) = race_guard.take() {
                    guard.relinquish();
                }
                // Best-effort registry cleanup; sibling writers may still
                // be attached for a short window after notify_waiters
                // fires (they need to re-acquire the lock to read the
                // result). The next session's get_or_create will either
                // reuse this state (if any sibling still attached) or
                // mint a new one (if they all detached).
                let _ = self.filesystem_store_for_v2().try_drop_race_state(&digest);

                // Send response to our client.
                v2_send_commit_outcome_to_client(&frame_tx, &digest, commit_result, &metrics)
                    .await;
            }
            CommitResponsibility::AwaitCommit => {
                // Some other writer is the commit-runner. Wait for the
                // result via the race-state's Notify.
                let result = v2_await_commit_result(&race_state, digest, &metrics).await;

                // FIX-2 watchdog → force_remove. If the watchdog fired
                // (DeadlineExceeded), the registry entry is wedged
                // (commit_running=true, no published result). Force-
                // remove it so a fresh writer arriving after this wedge
                // gets a clean state. Bump the wedge-event metric so
                // operators can alert on it.
                let watchdog_fired = matches!(
                    &result,
                    Err(e) if e.code == Code::DeadlineExceeded
                );
                if watchdog_fired {
                    metrics
                        .chunked_race_state_force_removed_total
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "nativelink_service::chunked_write_handler_v2",
                        ?digest,
                        ?writer_id,
                        "WriteChunkedV2: commit-watchdog fired; force-removing \
                         wedged race-state entry from registry",
                    );
                    let _ = self
                        .filesystem_store_for_v2()
                        .chunked_race_registry()
                        .force_remove(&digest);
                }

                if let Some(guard) = race_guard.take() {
                    guard.relinquish();
                }
                // Best-effort registry cleanup.
                let _ = self.filesystem_store_for_v2().try_drop_race_state(&digest);
                v2_send_commit_outcome_to_client(&frame_tx, &digest, result, &metrics).await;
            }
            CommitResponsibility::ContinueSending => {
                // Unreachable: we exit the loop above before reaching
                // here unless commit_responsibility was set to
                // RunCommit / AwaitCommit. Defensive log.
                warn!(
                    target: "nativelink_service::chunked_write_handler_v2",
                    ?digest,
                    ?writer_id,
                    "WriteChunkedV2: post-loop saw ContinueSending; programmer bug",
                );
                let _ = frame_tx
                    .send(Err(Status::internal(
                        "WriteChunkedV2: post-loop ContinueSending (programmer bug)",
                    )))
                    .await;
            }
        }

        // Final guard cleanup happens in race_guard's Drop if we didn't
        // already relinquish. This handles error paths uniformly.
    }

    /// Run the commit path: commit_to_holding (rename .partial →
    /// .holding + length validation), end-to-end BLAKE3 hash verify
    /// against .holding, finalize_holding (rename .holding → canonical
    /// + chmod 0o555 + insert into evicting_map). Returns the commit
    /// result for publication to sibling writers.
    async fn v2_run_commit_path(
        self: &Arc<Self>,
        digest: &DigestInfo,
        _race_state: &Arc<ChunkRaceState>,
    ) -> Result<RaceCommitResult, Error> {
        let expected_size = digest.size_bytes();

        // Stage 1: commit_to_holding (length validation + rename to
        // .holding). On length mismatch we return Err and the discard
        // path (caller-side) will GC the partial.
        if let Err(err) = self
            .filesystem_store_for_v2()
            .commit_chunked(digest, expected_size)
            .await
        {
            // Try to GC the partial best-effort.
            let _ = self.filesystem_store_for_v2().discard_chunked(digest).await;
            self.metrics_for_v2()
                .commit_failures_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }

        // Stage 2: end-to-end hash verify against the .holding file.
        let holding_path = self.filesystem_store_for_v2().holding_content_path(digest);
        let verify_result =
            v2_verify_e2e_hash(&holding_path, digest, expected_size).await;
        if let Err(err) = verify_result {
            // Hash mismatch: unlink the holding file + discard the
            // partial entry. Surface the error.
            let _ = self.filesystem_store_for_v2().unlink_holding(digest).await;
            let _ = self.filesystem_store_for_v2().discard_chunked(digest).await;
            self.metrics_for_v2()
                .sha256_e2e_mismatches_total
                .fetch_add(1, Ordering::Relaxed);
            self.metrics_for_v2()
                .commit_failures_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }

        // Stage 3: finalize_holding (rename .holding → canonical +
        // chmod + index insert).
        if let Err(err) = self.filesystem_store_for_v2().finalize_holding(digest).await {
            self.metrics_for_v2()
                .commit_failures_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }

        self.metrics_for_v2()
            .chunks_committed_total
            .fetch_add(1, Ordering::Relaxed);
        info!(
            target: "nativelink_service::chunked_write_handler_v2",
            ?digest,
            committed_size = expected_size,
            "WriteChunkedV2: blob committed",
        );
        Ok(RaceCommitResult {
            committed_size: expected_size,
        })
    }

    /// FIX-3 BIS / failed_writes integration. Mirrors the v1 reaper at
    /// `chunked_write_handler.rs:2089-2125` exactly:
    ///   - Ok(_): push the digest into `stable_digests_sink` so the BIS
    ///     broadcast loop drains worker `mirror_blobs` for this digest.
    ///   - Err(_): insert the digest into `failed_slow_writes` via
    ///     `failed_commit_sink` so the worker reconnect-retry can
    ///     re-attempt the slow-tier write.
    ///
    /// Wires NOT installed in this PR for production (sinks gated to
    /// only fire when `with_v2_*_sink` was called in the constructor
    /// chain). Production wiring lives in `bin/nativelink.rs` behind a
    /// `chunked_v2_enabled` config flag (default OFF).
    fn v2_fire_post_commit_sinks(
        self: &Arc<Self>,
        digest: &DigestInfo,
        commit_result: &Result<RaceCommitResult, Error>,
    ) {
        match commit_result {
            Ok(_) => {
                if let Some(sink) = self.v2_stable_digests_sink_for_v2() {
                    sink(*digest);
                    debug!(
                        target: "nativelink_service::chunked_write_handler_v2",
                        ?digest,
                        "WriteChunkedV2: pushed to stable_digests on commit success",
                    );
                }
            }
            Err(_) => {
                if let Some(sink) = self.v2_failed_commit_sink_for_v2() {
                    sink(*digest);
                    debug!(
                        target: "nativelink_service::chunked_write_handler_v2",
                        ?digest,
                        "WriteChunkedV2: inserted into failed_slow_writes on commit failure",
                    );
                }
            }
        }
    }
}

/// FIX-5: pump the per-state `cross_writer_committed_chunks` counter
/// into the exported `chunked_chunks_accepted_from_cross_writer_total`
/// metric. Called at commit-runner exit so the metric reflects the
/// total cross-writer dedup work this race produced. Race state's
/// counter is monotone within its lifetime (Arc dropped at end of race),
/// so we read once at end + add to the global.
fn v2_pump_cross_writer_metric(
    race_state: &Arc<ChunkRaceState>,
    metrics: &Arc<ChunkedWriteHandlerMetrics>,
) {
    let n = race_state.cross_writer_committed_count();
    if n > 0 {
        metrics
            .chunked_chunks_accepted_from_cross_writer_total
            .fetch_add(n, Ordering::Relaxed);
    }
}

/// Test-only re-export of `v2_pump_cross_writer_metric` for the FIX-5
/// integration test. The function is module-private; tests in
/// `tests/chunked_write_handler_v2_test.rs` are in a separate compilation
/// unit and need this entry point.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub fn v2_pump_cross_writer_metric_for_test(
    race_state: &Arc<ChunkRaceState>,
    metrics: &Arc<ChunkedWriteHandlerMetrics>,
) {
    v2_pump_cross_writer_metric(race_state, metrics);
}

/// Atomic max-store helper. Reads current, updates iff new is greater.
fn atomic_max(target: &AtomicU64, new: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while new > current {
        match target.compare_exchange_weak(
            current,
            new,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn parse_digest_v2(chunk: &WriteChunk) -> Result<DigestInfo, Error> {
    let proto = chunk
        .digest
        .as_ref()
        .ok_or_else(|| make_input_err!("WriteChunk missing required digest"))?;
    DigestInfo::try_from(proto.clone()).map_err(|err| {
        make_input_err!(
            "WriteChunk digest invalid: hash={:?}, size={}, err={err:?}",
            proto.hash,
            proto.size_bytes
        )
    })
}

/// Compute SHA-256 (or whatever `default_digest_hasher_func()` returns
/// — BLAKE3 in production, SHA-256 in tests) on the cpu_pool. Same
/// pattern as the v1 handler's `compute_sha256_blocking`.
async fn compute_sha256_blocking_v2(bytes: Bytes) -> Result<[u8; 32], Error> {
    let (tx, rx) = oneshot::channel();
    cpu_pool().spawn(move || {
        let mut h = default_digest_hasher_func().hasher();
        h.update(&bytes);
        let info = h.finalize_digest();
        let _ = tx.send(**info.packed_hash());
    });
    rx.await.map_err(|_| {
        make_err!(
            Code::Internal,
            "cpu_pool worker dropped before sending v2 per-chunk hash"
        )
    })
}

/// Re-read the `.holding` file from disk and compute the end-to-end
/// hash; verify it equals the digest's hash. Catches the lying-producer
/// case AND the cross-writer race case where two writers' chunks at
/// different offsets somehow disagree (impossible with the
/// position-based chunker invariant, but the e2e verify is the
/// load-bearing safety net).
async fn v2_verify_e2e_hash(
    holding_path: &std::path::Path,
    digest: &DigestInfo,
    expected_size: u64,
) -> Result<(), Error> {
    let holding_path_owned = holding_path.to_path_buf();
    let digest_for_blocking = *digest;
    let (tx, rx) = oneshot::channel::<Result<(), Error>>();
    cpu_pool().spawn(move || {
        // Read the file in chunks to avoid loading large blobs into
        // RAM all at once.
        const READ_CHUNK: usize = 1024 * 1024;
        let mut hasher = default_digest_hasher_func().hasher();
        let result = (|| -> Result<(), Error> {
            use std::io::Read;
            let mut file = std::fs::File::open(&holding_path_owned).map_err(|e| {
                make_err!(
                    Code::Internal,
                    "WriteChunkedV2: open .holding for e2e verify failed: {e:?} (path: {})",
                    holding_path_owned.display()
                )
            })?;
            let mut buf = vec![0u8; READ_CHUNK];
            let mut total: u64 = 0;
            loop {
                let n = file.read(&mut buf).map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "WriteChunkedV2: read .holding for e2e verify failed: {e:?}"
                    )
                })?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                total += n as u64;
            }
            if total != expected_size {
                return Err(make_err!(
                    Code::Internal,
                    "WriteChunkedV2: .holding length {total} != declared size {expected_size} \
                     for digest {digest_for_blocking}"
                ));
            }
            let info = hasher.finalize_digest();
            let computed: &[u8; 32] = info.packed_hash();
            let declared: &[u8; 32] = &digest_for_blocking.packed_hash();
            if computed != declared {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "WriteChunkedV2: end-to-end SHA-256 mismatch for digest {digest_for_blocking} \
                     (computed {:?} != declared {:?})",
                    computed,
                    declared
                ));
            }
            Ok(())
        })();
        let _ = tx.send(result);
    });
    rx.await.map_err(|_| {
        make_err!(
            Code::Internal,
            "cpu_pool worker dropped before completing v2 e2e hash verify"
        )
    })?
}

/// Wait for the race-state's commit_done notify, with a watchdog. On
/// timeout, returns Err(DeadlineExceeded). The commit-runner is the
/// only producer of `commit_result`; siblings here read it.
///
/// **#501 (narrow scope):** at 30 s (half of the 60 s infra-integrity
/// watchdog), emit a `warn!` + bump
/// `metrics.commit_watchdog_soft_warn_total` once per digest (per the
/// process-wide `V2_AWAITER_SOFT_WARN_SEEN` dedup set). Soft-warn does
/// NOT change the infra-integrity behavior — the existing
/// `tokio::time::timeout(COMMIT_WAIT_WATCHDOG, notified)` path is
/// byte-identical to today, including the `WatchdogTimeoutSignal`
/// discriminator attachment on watchdog-fire (#508 contract preserved).
/// `digest` is required so the dedup set can key on the failing blob.
async fn v2_await_commit_result(
    race_state: &Arc<ChunkRaceState>,
    digest: DigestInfo,
    metrics: &Arc<ChunkedWriteHandlerMetrics>,
) -> Result<RaceCommitResult, Error> {
    // BLOCK-E (#499 followup): defense-in-depth against missed-wakeup.
    // Tokio 1.49's `Notify::notified()` captures `notify_waiters_calls`
    // at FUTURE-CREATION time (notify.rs:572) and `Notified::poll`
    // resolves immediately if the counter advanced (notify.rs:1148).
    // So `subscribe_commit_done() → ... → notified.await` is race-free
    // against publish-between-subscribe-and-poll IN THIS TOKIO VERSION.
    //
    // We still pin + enable() before peek for two reasons:
    //   1. Belt-and-suspenders: any future tokio change that altered
    //      the counter-capture semantics would re-introduce the
    //      classical missed-wakeup. `enable()` registers the waiter
    //      eagerly; `notify_waiters` after enable() definitely notifies
    //      this waiter (this is the contract documented at
    //      tokio 1.34+).
    //   2. Self-documenting code: the explicit `enable()` makes the
    //      intent ("we want to receive any subsequent notify_waiters")
    //      visible to future readers without requiring them to chase
    //      tokio internals.
    //
    // The dispatch prompt's BLOCK-E claim of an active production bug
    // here is a reasonable-but-conservative read of the API surface
    // that doesn't reflect tokio 1.49 behavior; this comment records
    // why the fix is still WORTH applying.
    let notified = race_state.subscribe_commit_done();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if let Some(result) = race_state.peek_commit_result() {
        return result;
    }
    // #501 (narrow scope) soft-warn observability layer. Same
    // `tokio::time::timeout(...)` future as before — soft-warn is a
    // PARALLEL select! arm that only logs + bumps a counter on the
    // 30 s tick; the watchdog Err arm below is byte-identical to
    // pre-#501 (including #508 discriminator attachment). Biased
    // select prefers the watchdog branch so simultaneous-poll ties
    // resolve in favor of the existing path.
    let watchdog_fut = tokio::time::timeout(COMMIT_WAIT_WATCHDOG, notified);
    tokio::pin!(watchdog_fut);
    let soft_warn_at =
        tokio::time::sleep(Duration::from_secs(CHUNKED_COMMIT_SOFT_WARN_SECS));
    tokio::pin!(soft_warn_at);
    let mut soft_warned = false;
    let watchdog_result = loop {
        tokio::select! {
            biased;
            r = &mut watchdog_fut => break r,
            () = &mut soft_warn_at, if !soft_warned => {
                soft_warned = true;
                metrics
                    .commit_watchdog_soft_warn_total
                    .fetch_add(1, Ordering::Relaxed);
                if V2_AWAITER_SOFT_WARN_SEEN.insert_one_shot(digest) {
                    warn!(
                        ?digest,
                        soft_warn_secs = CHUNKED_COMMIT_SOFT_WARN_SECS,
                        infra_integrity_secs = CHUNKED_COMMIT_WATCHDOG_SECS,
                        site = "v2_awaiter",
                        "#501: chunked commit slow — WriteChunkedV2 sibling \
                         crossed soft-warn threshold; infra-integrity \
                         watchdog will fire if no progress before destructive \
                         deadline"
                    );
                }
            }
        }
    };
    // Drain the per-digest entry from the v2 soft-warn dedup set on
    // ANY outcome of the watchdog (success or fire). Safe to call
    // unconditionally; `remove` on a missing key is a no-op.
    V2_AWAITER_SOFT_WARN_SEEN.remove(&digest);
    match watchdog_result {
        Ok(()) => race_state.peek_commit_result().unwrap_or_else(|| {
            Err(make_err!(
                Code::Internal,
                "WriteChunkedV2: commit_done fired but commit_result not published"
            ))
        }),
        Err(_) => {
            // #508: attach the `WatchdogTimeoutSignal` discriminator so
            // the chunked client's `classify_retryable` predicate
            // (`chunked_client.rs::classify_retryable`) returns
            // `Retry { WatchdogDeadline }` instead of `Abort`. Mirrors
            // v1's pattern at `chunked_write_handler.rs:2354-2357`.
            // Without the discriminator, ANY `DeadlineExceeded` —
            // including a future per-RPC `tonic::Request::set_timeout`
            // or the chunk-driver per-pwrite/e2e SHA timeouts — would
            // silently inherit the retry intended only for the
            // server-side watchdog; bare `DeadlineExceeded` MUST map to
            // `Abort` at the classifier so v2 sibling writers that see
            // the watchdog fire correctly retry the whole blob.
            let detail = encode_watchdog_timeout_signal_any(
                watchdog_timeout_signal::Reason::ChunkedCommitWatchdog,
                COMMIT_WAIT_WATCHDOG.as_secs(),
            );
            Err(Error::deadline_exceeded_with_detail(
                format!(
                    "WriteChunkedV2: commit watchdog ({} s) elapsed waiting for commit-runner",
                    COMMIT_WAIT_WATCHDOG.as_secs()
                ),
                detail,
            ))
        }
    }
}

/// #508 test-only shim: expose the module-private `v2_await_commit_result`
/// to integration tests in `tests/chunked_write_handler_v2_test.rs`. Mirrors
/// the `v2_pump_cross_writer_metric_for_test` pattern above. The integration
/// test exercises the watchdog-Err arm and asserts the synthesised Err
/// carries the `WatchdogTimeoutSignal` discriminator so that
/// `chunked_client.rs::classify_retryable` returns `Retry { WatchdogDeadline }`
/// rather than `Abort` for sibling writers seeing the wedge.
///
/// **#501 (narrow scope):** signature now carries `digest` + `metrics`
/// so the soft-warn observability layer (30 s deadline; one-shot
/// per-digest via `V2_AWAITER_SOFT_WARN_SEEN`) can dedup correctly
/// and bump the per-handler `commit_watchdog_soft_warn_total` counter.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub async fn v2_await_commit_result_for_test(
    race_state: &Arc<ChunkRaceState>,
    digest: DigestInfo,
    metrics: &Arc<ChunkedWriteHandlerMetrics>,
) -> Result<RaceCommitResult, Error> {
    v2_await_commit_result(race_state, digest, metrics).await
}

/// Send the final `WriteChunkedFrame` to the client based on the
/// commit outcome. On Err, send the gRPC Status; on Ok, send the
/// final WriteChunkedResponse.
async fn v2_send_commit_outcome_to_client(
    frame_tx: &mpsc::Sender<Result<WriteChunkedFrame, Status>>,
    digest: &DigestInfo,
    result: Result<RaceCommitResult, Error>,
    _metrics: &Arc<ChunkedWriteHandlerMetrics>,
) {
    let outcome = match result {
        Ok(r) => {
            let proto_digest =
                nativelink_proto::build::bazel::remote::execution::v2::Digest::from(*digest);
            let final_response = WriteChunkedResponse {
                committed_digest: Some(proto_digest),
                committed_size: r.committed_size,
            };
            let frame = WriteChunkedFrame {
                payload: Some(write_chunked_frame::Payload::FinalResponse(final_response)),
            };
            Ok(frame)
        }
        Err(err) => {
            warn!(
                target: "nativelink_service::chunked_write_handler_v2",
                ?digest,
                ?err,
                "WriteChunkedV2: commit failed; surfacing to client",
            );
            Err(Status::from(err))
        }
    };
    let _ = frame_tx.send(outcome).await;
}

/// Compile-time tripwire reference to the position-based-chunker
/// invariant. If a future chunker changes shape (e.g. switches to
/// FastCDC), `CHUNK_BOUNDARIES_ARE_POSITION_BASED` flips false and
/// this const-eval panics at compile time.
const _ASSERT_CHUNKER_INVARIANT: () = {
    assert!(
        nativelink_store::chunked::chunked_race_state::CHUNK_BOUNDARIES_ARE_POSITION_BASED,
        "WriteChunkedV2 per-chunk dedup safety relies on position-based chunker",
    );
};
