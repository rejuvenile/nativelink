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

use nativelink_error::{Code, Error, ResultExt as _, make_err, make_input_err};
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
use nativelink_util::digest_hasher::{
    DigestFuncProver, DigestHasher, DigestHasherFunc, default_digest_hasher_func,
};

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

/// #perf: sample period for the two per-session `WriteChunkedV2` lifecycle
/// `info!` lines ("RPC entry" at session start + "session opened" after
/// admission). Production fires each ONCE per chunked write (measured 777 in
/// one 15-min window 2026-07-14); under a 140-executor build burst these
/// multiply into the known write-burst-stall regime. The two lines exist for
/// #247/#477 wire-shape attribution — a load-bearing signal, so they are
/// SAMPLED, not demoted to `debug!` (which `release_max_level_info` strips
/// from the release binary; see the `logging-release-max-level` memory).
/// Emit the FIRST occurrence + every `V2_LIFECYCLE_LOG_SAMPLE_PERIOD`-th
/// thereafter; each emitted line carries the always-incremented `cumulative`
/// count so the true per-site rate stays recoverable from the journal. Same
/// first-then-every-Nth precedent as `fallback_log_gate`
/// (`nativelink-util/src/task.rs`) and `CHUNKED_INFLIGHT_LOG_SAMPLE_PERIOD`
/// (`chunked_write_handler.rs`).
const V2_LIFECYCLE_LOG_SAMPLE_PERIOD: u64 = 64;

/// Cumulative count of `WriteChunkedV2 RPC entry` occurrences (sampled +
/// suppressed). Incremented on EVERY RPC entry so the emitted line's
/// `cumulative` field conveys the true invocation rate even though only
/// 1-in-`V2_LIFECYCLE_LOG_SAMPLE_PERIOD` lines are written.
static V2_RPC_ENTRY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

/// Cumulative count of `WriteChunkedV2: session opened` occurrences.
/// Independent from the RPC-entry counter — a session-open is skipped on
/// early zero-size / digest-parse rejects, so the two rates diverge.
static V2_SESSION_OPENED_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

/// Returns `true` if the sampled lifecycle `info!` should be emitted for
/// cumulative occurrence `count` (1-based): the first occurrence and every
/// `V2_LIFECYCLE_LOG_SAMPLE_PERIOD`-th thereafter. Pure and deterministic
/// (count → bool) for testability.
const fn v2_lifecycle_log_gate(count: u64) -> bool {
    count == 1 || count % V2_LIFECYCLE_LOG_SAMPLE_PERIOD == 0
}

/// Increment `counter` and return `(new_count, should_log)`. The counter
/// bumps on EVERY call (unconditionally) so the true rate is preserved;
/// `should_log` samples via [`v2_lifecycle_log_gate`]. Factored so a unit
/// test can prove "counter increments every call while the log samples"
/// against a fresh atomic without exercising the RPC path.
fn v2_lifecycle_log_decision(counter: &AtomicU64) -> (u64, bool) {
    let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
    (count, v2_lifecycle_log_gate(count))
}

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
        // #247+#477 DS-reviewer disambiguation: attribute every worker→server
        // WriteChunkedV2 invocation to this wire shape.
        // #perf: SAMPLED (first + every V2_LIFECYCLE_LOG_SAMPLE_PERIOD-th) —
        // under a build burst this fired ~777/15min into the write-burst-stall
        // regime. The always-incremented V2_RPC_ENTRY_LOG_COUNT conveys the
        // true rate on each emitted line; `peer_addr` is resolved ONLY on the
        // emitted path so the suppressed path skips the SocketAddr→String alloc.
        let (rpc_entry_count, should_log_rpc_entry) =
            v2_lifecycle_log_decision(&V2_RPC_ENTRY_LOG_COUNT);
        if should_log_rpc_entry {
            let peer_addr = request
                .remote_addr()
                .map_or_else(|| "unknown".to_string(), |a| a.to_string());
            info!(
                target: "nativelink_service::chunked_write_handler_v2",
                writer_path = "server_v2_rpc",
                wire_shape = "v2",
                %peer_addr,
                cumulative = rpc_entry_count,
                "WriteChunkedV2 RPC entry",
            );
        }
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
        //
        // #548 Phase 1: source = Worker. The V2 server RPC handler is
        // reached exclusively from `CasExtensions/WriteChunkedV2` — by
        // construction the producer is a worker uploading to the server.
        // Phase 4 (#551) will consume this at `v2_fire_post_commit_sinks`.
        let source = nativelink_store::chunked::ChunkedWriteSource::Worker;
        // #548 Phase 1: record the source in the per-handler test-only
        // side map so `v2_source_for_test(&digest)` returns it. Mirrors
        // the V1 path (`InFlightEntry.source` populated at admission +
        // exposed via `ChunkedWriteInFlight::source_for_test`). V2
        // doesn't share that registry; recording here is the seam where
        // a test can assert the source threaded from the RPC entry
        // before `v2_fire_post_commit_sinks` reads it.
        //
        // CFG-GATED: production builds (no `test-utils` feature) do not
        // compile this call, do not lock the Mutex, and do not insert.
        // The in-stack `source` value is still forwarded into
        // `run_v2_session` below (and from there into
        // `v2_fire_post_commit_sinks`) — Phase 4 (#551) consumes it
        // there without any side map. Closes the "unbounded in-process
        // buffer on a network-reachable path" defect class.
        #[cfg(any(test, feature = "test-utils"))]
        self.v2_record_source(digest, source);
        let handler = Arc::clone(&self);
        tokio::spawn(async move {
            handler
                .run_v2_session(stream, frame_tx, digest, first_chunk, source)
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
        // #548 Phase 1: source threaded from the V2 RPC entry point.
        // Recorded for observability; Phase 4 (#551) will branch on
        // `source == Worker` at `v2_fire_post_commit_sinks` to elide the
        // BIS broadcast for worker-sourced writes.
        source: nativelink_store::chunked::ChunkedWriteSource,
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

        // #F3 sibling (2026-07-28): pin the digest's chunked-partial
        // state (the SpawnBlocking entry this session's
        // `write_chunk_at_offset` calls create/reuse) against the
        // idle-TTL reap for the SESSION's lifetime. RAII: the guard
        // drops on EVERY exit from this function — the deliberate
        // abort paths that leave the entry for retry-reuse (client
        // stream error, client hang-up, writer-ended-without-finish)
        // included — so the idle clock starts exactly when the session
        // ends. Acquired BEFORE the race-state attach so the pin
        // window covers the whole attachment: while ANY v2 writer is
        // attached to the digest's race-state, the reaper cannot
        // remove the partial its `chunks_present` bits describe.
        let _chunked_writer_session_guard = self
            .filesystem_store_for_v2()
            .begin_chunked_write_session(digest);

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

        // #perf: SAMPLED session-opened lifecycle line (first + every
        // V2_LIFECYCLE_LOG_SAMPLE_PERIOD-th). 1 per chunked write in prod;
        // V2_SESSION_OPENED_LOG_COUNT conveys the true rate on each emitted
        // line. #247/#477 wire-shape attribution preserved.
        let (session_log_count, should_log_session) =
            v2_lifecycle_log_decision(&V2_SESSION_OPENED_LOG_COUNT);
        if should_log_session {
            info!(
                target: "nativelink_service::chunked_write_handler_v2",
                ?digest,
                ?writer_id,
                attached,
                cumulative = session_log_count,
                "WriteChunkedV2: session opened",
            );
        }

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
            // #540 probe: per-chunk tonic frame-recv seam. Brackets the
            // server-side `Streaming::message().await` so deep-dive
            // runs can attribute per-chunk wall-clock to "waiting on
            // client" (h2 frame in-flight, congestion window) vs
            // server-side stages (SHA-256, pwrite, ack-tx backpressure).
            // Closes the #531 trace-coverage gap that left the W3
            // bimodal investigation unable to discriminate "h2 frame
            // stuck" from "commit-path stuck".
            #[cfg(feature = "bench-trace")]
            let _w3_probe_recv_start = std::time::Instant::now();
            let chunk_opt = if let Some(c) = chunk_iter_first.take() {
                Some(c)
            } else {
                match stream.message().await {
                    Ok(Some(c)) => {
                        #[cfg(feature = "bench-trace")]
                        info!(
                            target: "nativelink_service::w3_probe",
                            ?digest,
                            elapsed_us = _w3_probe_recv_start.elapsed().as_micros() as u64,
                            ok = true,
                            "tonic_stream_message recv",
                        );
                        Some(c)
                    }
                    Ok(None) => {
                        #[cfg(feature = "bench-trace")]
                        info!(
                            target: "nativelink_service::w3_probe",
                            ?digest,
                            elapsed_us = _w3_probe_recv_start.elapsed().as_micros() as u64,
                            ok = true,
                            "tonic_stream_message recv (eos)",
                        );
                        None
                    }
                    Err(status) => {
                        #[cfg(feature = "bench-trace")]
                        info!(
                            target: "nativelink_service::w3_probe",
                            ?digest,
                            elapsed_us = _w3_probe_recv_start.elapsed().as_micros() as u64,
                            ok = false,
                            "tonic_stream_message recv (err)",
                        );
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
            let chunk_sha256_arr: [u8; 32] = match chunk.chunk_sha256.as_ref().try_into() {
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
            // #538/#539: probe block is `#[cfg]`-gated on the
            // `bench-trace` feature. When OFF (the default, including
            // production builds), neither the `Instant::now()` nor the
            // `info!` macro expansion survive in the release binary —
            // verified via `strings target/release/data_plane_bench |
            // grep -c 'w3_probe' == 0`. When ON (bench deep-dive
            // builds), the probe emits at `info!` level so the
            // workspace's `release_max_level_info` pin on `tracing`
            // does NOT silently compile the message back out (which
            // is what would happen at `trace!` / `debug!`).
            #[cfg(feature = "bench-trace")]
            let _w3_probe_sha_start = std::time::Instant::now();
            #[cfg(feature = "bench-trace")]
            info!(
                target: "nativelink_service::w3_probe",
                chunk_offset,
                chunk_bytes_len,
                "compute_sha256_blocking_v2 enter"
            );
            let computed_sha = match compute_sha256_blocking_v2(chunk_bytes_for_hash).await {
                Ok(s) => s,
                Err(err) => {
                    race_state.release_chunk_in_flight(writer_id, chunk_offset);
                    let _ = frame_tx.send(Err(err.into())).await;
                    return;
                }
            };
            #[cfg(feature = "bench-trace")]
            info!(
                target: "nativelink_service::w3_probe",
                chunk_offset,
                chunk_bytes_len,
                elapsed_us = _w3_probe_sha_start.elapsed().as_micros() as u64,
                "compute_sha256_blocking_v2 exit"
            );
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
            // #538/#539 probe: see compute_sha256 probe block above for
            // the cfg-gating rationale.
            #[cfg(feature = "bench-trace")]
            let _w3_probe_pwrite_start = std::time::Instant::now();
            #[cfg(feature = "bench-trace")]
            info!(
                target: "nativelink_service::w3_probe",
                chunk_offset,
                chunk_bytes_len,
                "write_chunk_at_offset (handler) enter"
            );
            let pwrite_res = self
                .filesystem_store_for_v2()
                .write_chunk_at_offset(&digest, chunk_offset, pwrite_bytes)
                .await;
            #[cfg(feature = "bench-trace")]
            info!(
                target: "nativelink_service::w3_probe",
                chunk_offset,
                chunk_bytes_len,
                elapsed_us = _w3_probe_pwrite_start.elapsed().as_micros() as u64,
                ok = pwrite_res.is_ok(),
                "write_chunk_at_offset (handler) exit"
            );
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
            // #540 probe: per-chunk tonic frame-send seam (ACCEPTED ack
            // back to client). Wall-clock for `frame_tx.send().await`
            // measures `ACK_CHANNEL_CAP` backpressure / `ReceiverStream`
            // poll-rate / h2 write-window — orthogonal to the recv-side
            // probe above. The two together let deep-dive runs separate
            // client-uplink stall from server-downlink stall.
            #[cfg(feature = "bench-trace")]
            let _w3_probe_ack_send_start = std::time::Instant::now();
            let send_err = frame_tx.send(Ok(frame)).await.is_err();
            #[cfg(feature = "bench-trace")]
            info!(
                target: "nativelink_service::w3_probe",
                ?digest,
                chunk_offset,
                elapsed_us = _w3_probe_ack_send_start.elapsed().as_micros() as u64,
                ok = !send_err,
                "tonic_frame_send accepted_ack",
            );
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
                self.v2_fire_post_commit_sinks(&digest, &commit_result, source);

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
                // #540 probe: commit-path wrapper seam. Brackets the
                // `v2_await_commit_result` call so deep-dive runs can
                // attribute sibling-writer wait time to the per-digest
                // `Notify` wakeup latency (vs the commit-runner's own
                // 3-stage `v2_run_commit_path` probes published above).
                // Distinguishes "h2 frame stuck" (recv probe) from
                // "commit-path stuck" (this probe) — the gap the #531
                // probe wave left open per #540.
                #[cfg(feature = "bench-trace")]
                let _w3_probe_await_start = std::time::Instant::now();
                let result = v2_await_commit_result(&race_state, digest, &metrics).await;
                #[cfg(feature = "bench-trace")]
                info!(
                    target: "nativelink_service::w3_probe",
                    ?digest,
                    elapsed_us = _w3_probe_await_start.elapsed().as_micros() as u64,
                    ok = result.is_ok(),
                    "v2_await_commit_result",
                );

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

    /// Stage 2 of the commit path: the end-to-end verify of the `.holding`
    /// file, its failure cleanup, and the METRIC ATTRIBUTION of the outcome.
    ///
    /// **Why this is a named function and not four inline statements.**
    /// Review found both halves of the attribution pinned and their JOIN
    /// not: `.claude/reviews/3847750f/pair-a.md` A-1 showed that replacing
    /// the record call with the two unconditional `fetch_add`s it replaced
    /// left all six tests in `server_digest_func_proving_chunked_test` green,
    /// because nothing drove the commit path's `Err` arm. Reproduced here
    /// before the fix (`/tmp/svrprove-final-mut-A1-pre.log`, 6 passed).
    /// The whole chain — real verify → real fault discriminant → real
    /// counters — is now reachable from a test through
    /// `v2_verify_and_attribute_for_test`, driven by a REAL vanished
    /// `.holding` file (the `#256` duplicate-commit guard at
    /// `filesystem_store.rs:2394` unlinks exactly that file and its own
    /// comment anticipates "a sibling concurrent caller already unlinked
    /// it") and by a REAL unprovable one.
    ///
    /// Returns the proven digest function when pass 2 rescued the commit,
    /// `None` on the fast path. Behaviour is byte-identical to the inline
    /// form it replaces except for the site-A log sampling noted below.
    async fn v2_verify_and_attribute(
        self: &Arc<Self>,
        digest: &DigestInfo,
        holding_path: &std::path::Path,
        expected_size: u64,
    ) -> Result<Option<DigestHasherFunc>, Error> {
        // #538/#539 probe: cfg-gated; see the Stage 1 block in
        // `v2_run_commit_path`.
        #[cfg(feature = "bench-trace")]
        let _w3_probe_verify_start = std::time::Instant::now();
        #[cfg(feature = "bench-trace")]
        info!(
            target: "nativelink_service::w3_probe",
            ?digest,
            "v2_verify_e2e_hash enter"
        );
        let verify_result = v2_verify_e2e_hash(holding_path, digest, expected_size).await;
        #[cfg(feature = "bench-trace")]
        info!(
            target: "nativelink_service::w3_probe",
            ?digest,
            elapsed_us = _w3_probe_verify_start.elapsed().as_micros() as u64,
            ok = verify_result.is_ok(),
            "v2_verify_e2e_hash exit"
        );
        let maybe_proven_func = match verify_result {
            Err(fault) => {
                // Verify failed: unlink the holding file + discard the
                // partial entry. Surface the error.
                let _ = self.filesystem_store_for_v2().unlink_holding(digest).await;
                let _ = self.filesystem_store_for_v2().discard_chunked(digest).await;
                v2_record_e2e_verify_failure(&self.metrics_for_v2(), &fault);
                return Err(fault.into_error());
            }
            Ok(maybe_proven_func) => maybe_proven_func,
        };
        // `#fl1786`: `Some` means the declared digest did NOT reproduce under
        // the process-global digest function but DID under `proven_func` —
        // a commit that was rejected forever before proving existed.
        // Deliberately does NOT touch `sha256_e2e_mismatches_total`: that
        // counter is the corrupt-blob alarm and a proven blob is intact.
        if let Some(proven_func) = maybe_proven_func {
            // SAMPLED, same period and same helper as the two lifecycle
            // lines above. Proving SUCCEEDING is what makes this reachable
            // at rate: pre-fix a mislabelling producer was rejected on every
            // blob so it could not sustain traffic; post-fix it works and
            // keeps running, and `warn!` is NOT compiled out in release
            // (`release_max_level_info`). `v2_lifecycle_log_decision` does
            // the counter's `fetch_add` AND the sampling decision in ONE
            // RMW against the metric itself, so the counter still carries
            // the TRUE rate, `cumulative` is exact under concurrency, and
            // no new state is introduced.
            let metrics = self.metrics_for_v2();
            let (cumulative, should_log) =
                v2_lifecycle_log_decision(&metrics.digest_func_proven_total);
            if should_log {
                warn!(
                    target: "nativelink_service::chunked_write_handler_v2",
                    ?digest,
                    proven_digest_function = %proven_func,
                    process_default_digest_function = %default_digest_hasher_func(),
                    cumulative,
                    "WriteChunkedV2: blob accepted by PROVING its digest function from its own \
                     bytes; the declared digest does not reproduce under the process default. \
                     The producer mislabelled (or omitted) the function and this commit would \
                     have been rejected on every retry",
                );
            }
        }
        Ok(maybe_proven_func)
    }

    /// Test-only shim for [`Self::v2_verify_and_attribute`] — the SEAM where
    /// a real verify outcome becomes a counter. Separate compilation unit,
    /// same `_for_test` pattern as `v2_metrics_for_test`.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub async fn v2_verify_and_attribute_for_test(
        self: &Arc<Self>,
        digest: &DigestInfo,
        holding_path: &std::path::Path,
        expected_size: u64,
    ) -> Result<Option<DigestHasherFunc>, Error> {
        self.v2_verify_and_attribute(digest, holding_path, expected_size)
            .await
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
        // #538/#539 probe: cfg-gated on `bench-trace`. See the
        // compute_sha256 probe block in `run_v2_session` for the
        // rationale (Instant + `info!` both compile-eliminated in the
        // default / production build).
        #[cfg(feature = "bench-trace")]
        let _w3_probe_commit_start = std::time::Instant::now();
        #[cfg(feature = "bench-trace")]
        info!(
            target: "nativelink_service::w3_probe",
            ?digest,
            "commit_chunked_to_holding enter"
        );
        if let Err(err) = self
            .filesystem_store_for_v2()
            .commit_chunked(digest, expected_size)
            .await
        {
            #[cfg(feature = "bench-trace")]
            info!(
                target: "nativelink_service::w3_probe",
                ?digest,
                elapsed_us = _w3_probe_commit_start.elapsed().as_micros() as u64,
                ok = false,
                "commit_chunked_to_holding exit"
            );
            // Try to GC the partial best-effort.
            let _ = self.filesystem_store_for_v2().discard_chunked(digest).await;
            self.metrics_for_v2()
                .commit_failures_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        #[cfg(feature = "bench-trace")]
        info!(
            target: "nativelink_service::w3_probe",
            ?digest,
            elapsed_us = _w3_probe_commit_start.elapsed().as_micros() as u64,
            ok = true,
            "commit_chunked_to_holding exit"
        );

        // Stage 2: end-to-end hash verify against the .holding file,
        // its failure cleanup, and the attribution of the outcome.
        let holding_path = self.filesystem_store_for_v2().holding_content_path(digest);
        self.v2_verify_and_attribute(digest, &holding_path, expected_size)
            .await?;

        // Stage 3: finalize_holding (rename .holding → canonical +
        // chmod + index insert).
        // #538/#539 probe: cfg-gated; see Stage 1 block above.
        #[cfg(feature = "bench-trace")]
        let _w3_probe_finalize_start = std::time::Instant::now();
        #[cfg(feature = "bench-trace")]
        info!(
            target: "nativelink_service::w3_probe",
            ?digest,
            "finalize_holding enter"
        );
        let finalize_res = self.filesystem_store_for_v2().finalize_holding(digest).await;
        #[cfg(feature = "bench-trace")]
        info!(
            target: "nativelink_service::w3_probe",
            ?digest,
            elapsed_us = _w3_probe_finalize_start.elapsed().as_micros() as u64,
            ok = finalize_res.is_ok(),
            "finalize_holding exit"
        );
        if let Err(err) = finalize_res {
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
        // #548 Phase 1: source forwarded from `run_v2_session`. PHASE 4
        // (#551) will branch on `source == ChunkedWriteSource::Worker`
        // below to elide the BIS broadcast (the worker is its own durable
        // holder and releases pins on tonic-Ok). Phase 1 just records.
        source: nativelink_store::chunked::ChunkedWriteSource,
    ) {
        let _phase4_source = source; // PHASE 4 (#551): branch on source == Worker to elide BIS broadcast — worker releases its pin on tonic-Ok; Phase 1 unconditionally fires v2_stable_digests_sink_for_v2 (no behavior change)
        match commit_result {
            Ok(_) => {
                if let Some(sink) = self.v2_stable_digests_sink_for_v2() {
                    sink(*digest);
                    debug!(
                        target: "nativelink_service::chunked_write_handler_v2",
                        ?digest,
                        ?source,
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
                        ?source,
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

/// The VERDICT half of a failed end-to-end verify, carried separately from
/// the `Error` that goes to the client.
///
/// `#fl1786` fix-up. Attribution used to be INFERRED from the error's code
/// (`err.code == Code::InvalidArgument` meant "corrupt"). That made the
/// corrupt-blob alarm a function of a value any `make_err!` or `map_err`
/// inside [`v2_verify_e2e_hash`]'s closure is free to choose, and a review
/// mutation proved it was not hypothetical: appending
/// `.map_err(|e| make_err!(Code::InvalidArgument, "{e:?}"))` to the pass-2
/// re-read re-filed a pass-2 `ENOENT` under the corruption alarm with the
/// whole suite green (`.claude/reviews/3847750f/pair-b.md`, M13).
///
/// The counter decision now reads this DISCRIMINANT, so the code cannot
/// forge the verdict. Both `Corrupt` construction sites are explicit and
/// both are statements about the BYTES; everything that reaches a `?` — i.e.
/// every error either `v2_fold_holding_file` pass can produce — becomes
/// `Io` through the [`From`] impl below and can never become `Corrupt` by
/// choosing a code.
enum V2VerifyFault {
    /// INTEGRITY VERDICT: the bytes on disk DISPROVE the declared digest.
    /// Either no advertised digest function reproduces the declared hash, or
    /// the assembled length disagrees with the declared `size_bytes` —
    /// `DigestInfo` identity is hash AND size, so a length disagreement
    /// disproves the blob under every function.
    Corrupt(Error),
    /// The verify could not COMPLETE. Proves NOTHING in either direction —
    /// the blob was neither proven nor disproven.
    Io(Error),
}

impl V2VerifyFault {
    /// The error to propagate to the client. Unchanged from base at every
    /// site: the wire code is a separate concern from the attribution.
    fn into_error(self) -> Error {
        match self {
            Self::Corrupt(err) | Self::Io(err) => err,
        }
    }

    /// Whether this fault is a statement about the BYTES rather than about
    /// the server's ability to read them.
    const fn is_integrity_verdict(&self) -> bool {
        matches!(self, Self::Corrupt(_))
    }
}

/// Every `Error` that propagates out of a verify fold via `?` is an I/O
/// fault. This impl is the load-bearing half of the M13 fix: it is why the
/// integrity verdict has only the two explicit `V2VerifyFault::Corrupt(..)`
/// construction sites and why no `Code` chosen inside the folds can reach
/// the corrupt-blob alarm.
impl From<Error> for V2VerifyFault {
    fn from(err: Error) -> Self {
        Self::Io(err)
    }
}

/// File a FAILED end-to-end verify under the right counter.
///
/// `#fl1786`: `sha256_e2e_mismatches_total` is the CORRUPT-BLOB alarm — its
/// own help text tells the operator a non-zero value means the assembled
/// blob does not match its digest. Only [`V2VerifyFault::Corrupt`] is that
/// verdict.
///
/// An [`V2VerifyFault::Io`] proves NOTHING in either direction — the blob was
/// neither proven nor disproven. Filing those under the alarm is not
/// hypothetical: the pass-2 proving re-read opens `.holding` a SECOND time,
/// and that file has live concurrent unlinkers — `filesystem_store.rs:2394`
/// (the `#256` duplicate-commit guard in `finalize_holding`, whose own
/// comment reads "covers the case where a sibling concurrent caller already
/// unlinked it") plus the `#497` owner-drop class — so an `ENOENT` between
/// the two passes is reachable, on exactly the mislabelled digests the
/// proving pass exists to rescue. An operator would see the corruption alarm
/// fire on intact blobs. (This also repairs the PRE-EXISTING mis-filing of
/// pass-1 open/read faults, which the arm has had since the counter was
/// introduced.)
///
/// `commit_failures_total` is unconditional: a failed commit is a failed
/// commit whatever the cause, and it is what keeps an I/O-fault storm
/// visible somewhere.
fn v2_record_e2e_verify_failure(metrics: &Arc<ChunkedWriteHandlerMetrics>, fault: &V2VerifyFault) {
    if fault.is_integrity_verdict() {
        metrics
            .sha256_e2e_mismatches_total
            .fetch_add(1, Ordering::Relaxed);
    }
    metrics.commit_failures_total.fetch_add(1, Ordering::Relaxed);
}

/// Test-only shim for [`v2_verify_e2e_hash`], so the I/O-fault error SHAPE
/// (`Code::Internal`, never `Code::InvalidArgument`) can be pinned against a
/// real vanished `.holding` file rather than asserted from the source. The
/// verdict discriminant is deliberately NOT exposed — a test asserts the
/// verdict through the counters it produces at the real seam
/// (`v2_verify_and_attribute_for_test`), never by reading a shape the
/// production consumer does not read.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub async fn v2_verify_e2e_hash_for_test(
    holding_path: &std::path::Path,
    digest: &DigestInfo,
    expected_size: u64,
) -> Result<Option<DigestHasherFunc>, Error> {
    v2_verify_e2e_hash(holding_path, digest, expected_size)
        .await
        .map_err(V2VerifyFault::into_error)
}

/// Fold the whole `.holding` file through `sink`, returning the byte count.
///
/// Reads in `READ_CHUNK`-sized bites so a multi-hundred-MiB blob never lands
/// in RAM at once. Shared by both passes of [`v2_verify_e2e_hash`] so the
/// second pass cannot drift from the first in buffer size or error shape.
fn v2_fold_holding_file(
    path: &std::path::Path,
    sink: &mut impl FnMut(&[u8]),
) -> Result<u64, Error> {
    // Sized to ZFS `recordsize=1M` on `fast/nativelink/work`.
    const READ_CHUNK: usize = 1024 * 1024;
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|e| {
        make_err!(
            Code::Internal,
            "WriteChunkedV2: open .holding for e2e verify failed: {e:?} (path: {})",
            path.display()
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
        sink(&buf[..n]);
        total += n as u64;
    }
    Ok(total)
}

/// Re-read the `.holding` file from disk and compute the end-to-end
/// hash; verify it equals the digest's hash. Catches the lying-producer
/// case AND the cross-writer race case where two writers' chunks at
/// different offsets somehow disagree (impossible with the
/// position-based chunker invariant, but the e2e verify is the
/// load-bearing safety net).
///
/// `#fl1786-server-side-digest-function-proving`. `WriteChunk`
/// (`worker_api.proto:1463-1475`) carries no digest-function field and the
/// producer of a backfill upload has no ambient one either, so the function
/// used here USED TO BE the process-global `default_digest_hasher_func()` —
/// a GUESS about somebody else's blob. When the guess was wrong the commit
/// was rejected, the server stayed missing the blob, and the next
/// `BlobsAvailable` tick re-solicited it: a latch, not a retry.
///
/// A digest is a CHECKABLE CLAIM and this function is holding the bytes, so
/// it determines the function instead of guessing. Two passes:
///
/// 1. the process-global function, byte-identical to before — one read, one
///    hash. A correctly-labelled blob pays NOTHING extra and returns
///    `Ok(None)`. This is the 99.99% path.
/// 2. ONLY when pass 1 mismatches — i.e. only on commits that would
///    otherwise be REJECTED outright — re-read the file and fold it through
///    every candidate in `PROVABLE_DIGEST_FUNCS` in one pass.
///    `Ok(Some(func))` means the blob genuinely hashes to its declared
///    digest under `func`.
///
/// Gating pass 2 on the mismatch is what keeps the hot write path free: the
/// extra full read + N-way hash is paid only where the alternative is a
/// permanent rejection, so its cost is bounded by the mislabelled-traffic
/// rate rather than by total ingest.
///
/// **Fail-closed.** Acceptance still requires a genuine hash match:
/// `DigestFuncProver::prove` compares the WHOLE `DigestInfo` (hash AND
/// size), so a truncated or extended body proves nothing. When no candidate
/// reproduces the declared digest the blob is CORRUPT, not mislabelled, and
/// the unchanged `InvalidArgument` mismatch error is returned. An I/O
/// failure during either pass is [`V2VerifyFault::Io`] and is NOT a
/// data-integrity verdict — it must never be reported as "unprovable".
async fn v2_verify_e2e_hash(
    holding_path: &std::path::Path,
    digest: &DigestInfo,
    expected_size: u64,
) -> Result<Option<DigestHasherFunc>, V2VerifyFault> {
    let holding_path_owned = holding_path.to_path_buf();
    let digest_for_blocking = *digest;
    let (tx, rx) = oneshot::channel::<Result<Option<DigestHasherFunc>, V2VerifyFault>>();
    cpu_pool().spawn(move || {
        let result = (|| -> Result<Option<DigestHasherFunc>, V2VerifyFault> {
            // Pass 1: the process-global function, exactly as before.
            let mut hasher = default_digest_hasher_func().hasher();
            let total =
                v2_fold_holding_file(&holding_path_owned, &mut |c| hasher.update(c))?;
            if total != expected_size {
                // INTEGRITY VERDICT, not an I/O fault. `expected_size` is
                // `digest.size_bytes()` (`v2_run_commit_path`), and
                // `v2_fold_holding_file` reads to EOF, so `total` IS the
                // file's length: a disagreement says the assembled blob is
                // not the declared blob under EVERY digest function, because
                // `DigestInfo` identity is hash AND size. An earlier version
                // of this code called it an I/O fault and demoted it out of
                // the corrupt-blob alarm; that was wrong in the fail-OPEN
                // direction and is `pair-a` T-1. `Code::InvalidArgument`
                // matches the stage-1 sibling that rejects the identical
                // fault before the rename
                // (`chunked_filesystem.rs:1397-1408`, "chunked commit length
                // mismatch"), which is also what makes it correctly
                // non-retryable to the client.
                return Err(V2VerifyFault::Corrupt(make_err!(
                    Code::InvalidArgument,
                    "WriteChunkedV2: .holding length {total} != declared size {expected_size} \
                     for digest {digest_for_blocking}"
                )));
            }
            let info = hasher.finalize_digest();
            let computed: &[u8; 32] = info.packed_hash();
            let declared: &[u8; 32] = &digest_for_blocking.packed_hash();
            if computed == declared {
                return Ok(None);
            }

            // Pass 2 (#fl1786): the process default did not reproduce the
            // declared digest. Before rejecting — which is a PERMANENT
            // verdict, because the producer will re-solicit and fail
            // identically forever — prove the function from the bytes.
            let mut prover = DigestFuncProver::new();
            v2_fold_holding_file(&holding_path_owned, &mut |c| prover.update(c)).err_tip(
                || {
                    "WriteChunkedV2: re-read of .holding for digest-function proving \
                     (#fl1786). This is an I/O fault, NOT a data-integrity verdict — the \
                     blob was neither proven nor disproven"
                },
            )?;
            if let Some(proven_func) = prover.prove(&digest_for_blocking) {
                return Ok(Some(proven_func));
            }

            Err(V2VerifyFault::Corrupt(make_err!(
                Code::InvalidArgument,
                "WriteChunkedV2: end-to-end SHA-256 mismatch for digest {digest_for_blocking} \
                 (computed {:?} != declared {:?}); no advertised digest function reproduces \
                 the declared digest from these bytes, so the blob is CORRUPT rather than \
                 mislabelled",
                computed,
                declared
            )))
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

#[cfg(test)]
mod v2_verify_fault_attribution_tests {
    use nativelink_error::{Code, make_err};

    use super::V2VerifyFault;

    /// **The M13 class, closed at the type level.** Attribution used to read
    /// `err.code == Code::InvalidArgument`, so any `make_err!` or `map_err`
    /// inside `v2_verify_e2e_hash`'s closure could forge the corrupt-blob
    /// verdict; a review mutation appending one `map_err` to the pass-2
    /// re-read did exactly that with the whole suite green (pair-b M13,
    /// reproduced at `/tmp/svrprove-final-mut-M13-pre.log`).
    ///
    /// Every error either fold pass produces reaches the fault type through
    /// this `From` impl, via `?`. It must yield `Io` for EVERY code — that is
    /// what leaves the integrity verdict with only its two explicit
    /// `V2VerifyFault::Corrupt(..)` construction sites.
    #[test]
    fn no_error_code_can_forge_the_integrity_verdict() {
        for code in [
            Code::Internal,
            Code::InvalidArgument,
            Code::NotFound,
            Code::DataLoss,
            Code::Aborted,
            Code::Unknown,
        ] {
            let fault: V2VerifyFault = make_err!(code, "simulated verify-fold failure").into();
            assert!(
                !fault.is_integrity_verdict(),
                "#fl1786 M13: an Error propagating out of a verify fold must convert to \
                 V2VerifyFault::Io regardless of its Code, so the corrupt-blob alarm keeps \
                 exactly the two construction sites that are statements about the BYTES. \
                 Code::{code:?} produced an integrity verdict, which re-opens M13: one map_err \
                 inside either fold would again re-file an unread blob as CAS corruption"
            );
        }
    }

    /// The other direction, so the assertion above cannot be satisfied by a
    /// discriminant that is never `Corrupt`.
    #[test]
    fn an_explicit_corrupt_verdict_is_an_integrity_verdict() {
        let fault = V2VerifyFault::Corrupt(make_err!(Code::InvalidArgument, "no candidate"));
        assert!(
            fault.is_integrity_verdict(),
            "#fl1786: V2VerifyFault::Corrupt MUST read as an integrity verdict — otherwise the \
             corrupt-blob alarm never fires and a lying producer is indistinguishable from a \
             mislabelled one"
        );
    }

    /// The wire error survives the classification unchanged: the fault type
    /// carries the verdict, it does not rewrite what the client sees.
    #[test]
    fn classification_does_not_rewrite_the_client_facing_error() {
        let io = V2VerifyFault::Io(make_err!(Code::Internal, "open .holding failed"));
        assert_eq!(
            io.into_error().code,
            Code::Internal,
            "#fl1786: splitting the verdict from the code must not change what the client sees; \
             an I/O fault stays Code::Internal on the wire"
        );
        let corrupt = V2VerifyFault::Corrupt(make_err!(Code::InvalidArgument, "no candidate"));
        assert_eq!(
            corrupt.into_error().code,
            Code::InvalidArgument,
            "#fl1786: an integrity verdict stays Code::InvalidArgument on the wire — permanent, \
             not retryable"
        );
    }
}

#[cfg(test)]
mod v2_lifecycle_log_sampling_tests {
    use core::sync::atomic::{AtomicU64, Ordering};

    use super::{
        V2_LIFECYCLE_LOG_SAMPLE_PERIOD, v2_lifecycle_log_decision, v2_lifecycle_log_gate,
    };

    /// Numeric-constant discipline: the sample period literal is the value
    /// cited in the commit message as the volume-reduction factor for the
    /// two `WriteChunkedV2` lifecycle `info!` lines.
    #[test]
    fn sample_period_constant_is_64() {
        assert_eq!(
            V2_LIFECYCLE_LOG_SAMPLE_PERIOD, 64,
            "V2_LIFECYCLE_LOG_SAMPLE_PERIOD must be exactly 64 (cited as the \
             ~64x volume reduction for the two WriteChunkedV2 lifecycle logs)"
        );
    }

    /// The period must be a real throttle (>1); a period of 1 would emit
    /// on every call and defeat the fix.
    #[test]
    fn sample_period_actually_throttles() {
        assert!(
            V2_LIFECYCLE_LOG_SAMPLE_PERIOD > 1,
            "sample period must be >1 or the gate emits on every call — no throttle"
        );
    }

    /// The gate emits on the first occurrence, suppresses everything strictly
    /// between the first and the period boundary, and re-emits at each period
    /// boundary.
    #[test]
    fn gate_emits_first_then_every_period() {
        assert!(
            v2_lifecycle_log_gate(1),
            "first occurrence MUST log so a journal scan sees the wire shape start"
        );
        for c in 2..V2_LIFECYCLE_LOG_SAMPLE_PERIOD {
            assert!(
                !v2_lifecycle_log_gate(c),
                "occurrence {c} between first and period boundary MUST be suppressed \
                 (spam reduction)"
            );
        }
        assert!(
            v2_lifecycle_log_gate(V2_LIFECYCLE_LOG_SAMPLE_PERIOD),
            "occurrence at the period boundary MUST log (periodic heartbeat)"
        );
        assert!(
            !v2_lifecycle_log_gate(V2_LIFECYCLE_LOG_SAMPLE_PERIOD + 1),
            "occurrence just past the period boundary MUST be suppressed"
        );
        assert!(
            v2_lifecycle_log_gate(2 * V2_LIFECYCLE_LOG_SAMPLE_PERIOD),
            "second period boundary MUST log"
        );
    }

    /// The core contract the dispatch names: the counter increments on EVERY
    /// call (so the true rate stays recoverable / scrapeable) while the log
    /// SAMPLES (emits only on the first + every period-th occurrence).
    #[test]
    fn counter_increments_every_call_while_log_samples() {
        let counter = AtomicU64::new(0);
        let calls = 2 * V2_LIFECYCLE_LOG_SAMPLE_PERIOD + 1;
        let mut logged_at = Vec::new();
        for _ in 0..calls {
            let (count, should_log) = v2_lifecycle_log_decision(&counter);
            if should_log {
                logged_at.push(count);
            }
        }
        // Counter bumped on every single call — true cumulative rate.
        assert_eq!(
            counter.load(Ordering::Relaxed),
            calls,
            "counter MUST increment on every call so the true rate is recoverable \
             even when the log is sampled; if it equals the emitted-line count the \
             fetch_add was wrongly gated behind the sampler"
        );
        // Emissions land exactly on 1, PERIOD, 2*PERIOD.
        assert_eq!(
            logged_at,
            vec![
                1,
                V2_LIFECYCLE_LOG_SAMPLE_PERIOD,
                2 * V2_LIFECYCLE_LOG_SAMPLE_PERIOD
            ],
            "log MUST emit only on the first occurrence + every period-th; any other \
             set means the sampler is not throttling as specified"
        );
    }
}
