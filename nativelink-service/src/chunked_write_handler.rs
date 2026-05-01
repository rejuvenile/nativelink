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

//! #212 Phase 2.2: server-side `WriteChunked` RPC handler.
//!
//! End-to-end chunked-write path:
//!
//! ```text
//!   Worker (or any peer the server trusts via worker auth)
//!        │  client-streaming WriteChunk { digest, chunk_offset, chunk_bytes,
//!        │                                chunk_sha256, finish_chunk }
//!        ▼
//!   ChunkedWriteHandler::write_chunked()
//!        │
//!        ├─ first chunk: spawn `ChunkedDriver` per (digest); insert into
//!        │   in-flight map keyed by `DigestInfo`
//!        ├─ each chunk: per-chunk SHA-256 verify on `spawn_blocking`
//!        │   (#213 perf-opt NMA1) → admit
//!        │     1. ChunkBudget::try_acquire_chunk() → ResourceExhausted
//!        │        on full (with BackpressureSignal type_url for the
//!        │        §13.1.1 point 2 dead-channel discriminator)
//!        │     2. mpsc::Sender::try_send(...) → ResourceExhausted on full
//!        │        (per §13.1.1 admission-ordering reverse-release)
//!        ├─ on finish_chunk=true: admit, then await driver completion
//!        │   (option α — synchronous commit; the WriteChunked caller
//!        │   IS the slow-tier producer, not Bazel-facing — design Q1=(b))
//!        ▼
//!   FilesystemStore::commit_chunked() + end-to-end SHA-256 verify
//!        ▼
//!   `WriteChunkedResponse { committed_digest, committed_size }`
//! ```
//!
//! Anti-#203 invariant: this handler returns Ok ONLY after the per-blob
//! commit completes (option α). The historical #203 cascade was upstream
//! Bazel-facing latency coupling to slow-tier writes; the WriteChunked
//! RPC's caller IS the slow-tier producer (worker), so the latency lives
//! at the natural place. The Bazel-facing FastSlowStore::update path
//! remains async per the unchanged production design.

#![cfg(feature = "chunked_fast_slow")]

use std::collections::HashMap;
use std::sync::Arc;

use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, WriteChunk, WriteChunkedResponse,
    backpressure_signal,
};
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{
    ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP,
};
use nativelink_store::filesystem_store::{FileEntry, FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

/// Build a `prost_types::Any` carrying an encoded `BackpressureSignal`.
/// Inlined here (instead of importing `nativelink_store::chunked_signal::
/// encode_backpressure_signal_any` which is `pub(crate)` to that crate)
/// so the handler does not need to re-export an internal helper across
/// the crate boundary. Both encoders share the same wire-stable
/// `BACKPRESSURE_SIGNAL_TYPE_URL` constant from `nativelink-proto`.
fn build_backpressure_any(
    reason: backpressure_signal::Reason,
    retry_after_ms: u64,
) -> prost_types::Any {
    let signal = BackpressureSignal {
        reason: reason as i32,
        retry_after_ms,
    };
    prost_types::Any {
        type_url: BACKPRESSURE_SIGNAL_TYPE_URL.to_string(),
        value: signal.encode_to_vec(),
    }
}

/// Backoff hint suggested to the client on global-budget exhaustion.
/// Matches the design §13.1.1 retry-after default for the global axis.
const GLOBAL_BUDGET_RETRY_AFTER_MS: u64 = 100;

/// Backoff hint suggested to the client on per-blob mpsc-full. Per-blob
/// queues drain faster than the global budget so the hint is shorter
/// (per design §13.1.1).
const PER_BLOB_MPSC_RETRY_AFTER_MS: u64 = 25;

/// In-flight map: `DigestInfo` → live driver + sender. The sender is
/// held here (not by the spawned driver) so multiple concurrent stream
/// admissions for the SAME digest can re-use the same driver task and
/// coalesce their writes (§14.x design intent for parallel streams of
/// the same blob).
///
/// **Today (Phase 2.2/2.3)** we DO NOT support concurrent streams for
/// the same digest — see `take_or_create_in_flight` for the current
/// "first stream wins" behavior. Concurrent streams for the same digest
/// receive `Code::AlreadyExists` (a deliberate over-conservative choice;
/// design §6.7 specifies the long-term behavior is to fold them into
/// the single driver). Captured as a known limitation in the open
/// questions.
#[derive(Debug)]
struct InFlightEntry {
    sender: nativelink_store::chunked::chunked_driver::ChunkWorkSender,
    driver: Arc<ChunkedDriver>,
}

/// Per-server in-flight chunked-write tracker.
///
/// `parking_lot::Mutex` is correct: every critical section is short
/// (HashMap insert / remove / get-and-clone) and never holds across
/// an `.await`. The per-entry `ChunkedDriver` is `Arc`-shared so the
/// outer map lock releases immediately after the `Arc::clone`.
#[derive(Debug, Default)]
pub struct ChunkedWriteInFlight {
    inner: Mutex<HashMap<DigestInfo, InFlightEntry>>,
}

impl ChunkedWriteInFlight {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn contains(&self, digest: &DigestInfo) -> bool {
        self.inner.lock().contains_key(digest)
    }

    /// Returns the number of currently in-flight chunked writes.
    /// Used by tests + future metric wiring.
    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.inner.lock().len()
    }
}

/// Server-side handler for the `WriteChunked` RPC. Holds the
/// FilesystemStore the handler writes to + the in-flight tracker +
/// (today) a borrowed reference to the global ChunkBudget singleton.
///
/// One instance per server process; cheap to clone (all fields are
/// `Arc` / `&'static`).
pub struct ChunkedWriteHandler<Fe: FileEntry = FileEntryImpl> {
    filesystem_store: Arc<FilesystemStore<Fe>>,
    in_flight: Arc<ChunkedWriteInFlight>,
    chunk_budget: &'static ChunkBudget,
}

impl<Fe: FileEntry> core::fmt::Debug for ChunkedWriteHandler<Fe> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChunkedWriteHandler")
            .field(
                "in_flight_count",
                &self.in_flight.in_flight_count(),
            )
            .finish_non_exhaustive()
    }
}

impl<Fe: FileEntry> ChunkedWriteHandler<Fe> {
    /// Construct a handler bound to a particular `FilesystemStore` and
    /// to the process-wide `ChunkBudget` singleton. The in-flight map
    /// is owned by this handler so two handlers do not share state
    /// (uncommon — production deploys one handler per server).
    #[must_use]
    pub fn new(filesystem_store: Arc<FilesystemStore<Fe>>) -> Self {
        Self {
            filesystem_store,
            in_flight: ChunkedWriteInFlight::new(),
            chunk_budget: nativelink_store::chunked::chunk_budget::chunk_budget_singleton(),
        }
    }

    /// Construct a handler with externally-provided in-flight tracker
    /// + chunk budget. Used by tests so the test harness can observe
    /// the in-flight map AND so each test gets its own budget (avoiding
    /// cross-test interference on the process-wide singleton).
    #[must_use]
    pub fn new_with_state(
        filesystem_store: Arc<FilesystemStore<Fe>>,
        in_flight: Arc<ChunkedWriteInFlight>,
        chunk_budget: &'static ChunkBudget,
    ) -> Self {
        Self {
            filesystem_store,
            in_flight,
            chunk_budget,
        }
    }

    /// Test-only: read-only accessor on the in-flight tracker.
    #[must_use]
    pub fn in_flight(&self) -> &Arc<ChunkedWriteInFlight> {
        &self.in_flight
    }

    /// The actual RPC handler. Called from the WorkerApi trait method
    /// (or directly from tests).
    pub async fn write_chunked(
        &self,
        request: Request<Streaming<WriteChunk>>,
    ) -> Result<Response<WriteChunkedResponse>, Status> {
        match self.write_chunked_inner(request).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(err) => Err(err_to_status(err)),
        }
    }

    /// Inner handler that returns `Result<_, Error>` so we can use
    /// `?` + `.err_tip()` per CLAUDE.md.
    async fn write_chunked_inner(
        &self,
        request: Request<Streaming<WriteChunk>>,
    ) -> Result<WriteChunkedResponse, Error> {
        let mut stream = request.into_inner();

        // Receive the FIRST chunk to learn the digest. Until the first
        // chunk arrives we don't know which driver to spawn.
        let first_chunk = match stream.message().await {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Err(make_input_err!(
                    "WriteChunked stream closed before any chunks were sent"
                ));
            }
            Err(status) => {
                let err: Error = status.into();
                return Err(err.append("error receiving first WriteChunk"));
            }
        };
        let digest = parse_digest(&first_chunk)?;
        let stream_digest = digest;

        // Look up or create the per-blob driver. Today: one driver per
        // digest at a time; concurrent streams for the same digest are
        // rejected with AlreadyExists. This is a deliberate Phase 2.2/2.3
        // simplification — the design admits parallel-stream coalescing
        // as a future extension.
        let mut sender_opt = None;
        let mut driver_opt = None;
        {
            let mut guard = self.in_flight.inner.lock();
            if guard.contains_key(&digest) {
                return Err(make_err!(
                    Code::AlreadyExists,
                    "WriteChunked: another stream is already writing digest {digest}"
                ));
            }
            // Spawn driver. Capacity = PER_BLOB_MPSC_CAP (16).
            let (driver, sender) = ChunkedDriver::spawn_driver(
                Arc::clone(&self.filesystem_store),
                digest,
                digest.size_bytes(),
                CHUNK_SIZE,
                PER_BLOB_MPSC_CAP,
            );
            let driver_arc = Arc::new(driver);
            guard.insert(
                digest,
                InFlightEntry {
                    sender: sender.clone(),
                    driver: Arc::clone(&driver_arc),
                },
            );
            sender_opt = Some(sender);
            driver_opt = Some(driver_arc);
        }
        // SAFETY: both options are Some by the assignment above.
        let sender = sender_opt.expect("sender just inserted");
        let driver = driver_opt.expect("driver just inserted");

        // The driver is in the in-flight map; from this point on a
        // panic in our request loop must remove the entry (the Arc
        // remains live in the map even if our local `driver` Arc
        // drops, so without explicit cleanup the map would leak the
        // entry until process exit).
        let cleanup_guard = InFlightCleanup {
            in_flight: Arc::clone(&self.in_flight),
            digest,
        };

        // Process the first chunk + every subsequent chunk in the
        // stream. On finish_chunk we `await` the driver's commit and
        // return the response.
        if let Err(err) = self.admit_chunk(first_chunk, &sender, stream_digest).await {
            warn!(
                ?stream_digest,
                ?err,
                "WriteChunked: first chunk admission failed"
            );
            return Err(err);
        }

        loop {
            let next = match stream.message().await {
                Ok(Some(c)) => c,
                Ok(None) => {
                    // Stream closed before finish_chunk arrived.
                    warn!(
                        ?stream_digest,
                        "WriteChunked: client stream closed before finish_chunk; abandoning blob"
                    );
                    drop(cleanup_guard);
                    return Err(make_err!(
                        Code::Aborted,
                        "WriteChunked stream closed before finish_chunk for digest {stream_digest}"
                    ));
                }
                Err(status) => {
                    warn!(
                        ?stream_digest,
                        status_code = ?status.code(),
                        status_msg = %status.message(),
                        "WriteChunked: client stream errored mid-blob"
                    );
                    drop(cleanup_guard);
                    let err: Error = status.into();
                    return Err(err.append(format!(
                        "error mid-stream WriteChunk for digest {stream_digest}"
                    )));
                }
            };
            let next_digest = parse_digest(&next)?;
            if next_digest != stream_digest {
                drop(cleanup_guard);
                return Err(make_input_err!(
                    "WriteChunked stream switched digest mid-stream: started {stream_digest}, got {next_digest}"
                ));
            }
            let is_last = next.finish_chunk;
            self.admit_chunk(next, &sender, stream_digest).await?;
            if is_last {
                break;
            }
        }

        // Drop our sender reference — but we can't drop the in-flight
        // map's stored sender until AFTER we observe completion (the
        // driver expects no more chunks; the sender drop signals that).
        // Take the entry out of the map and drop it AFTER awaiting.
        drop(sender);
        let removed_entry = self
            .in_flight
            .inner
            .lock()
            .remove(&stream_digest);
        // Drop the entry's sender so the driver's mpsc closes if we
        // are the last sender. The driver may already have committed
        // (on the finish_chunk path it is racing the mpsc close), but
        // dropping is harmless.
        drop(removed_entry);
        // The cleanup_guard's `Drop` will see no entry to remove
        // (we just removed it ourselves) — this is fine; cleanup is
        // idempotent. Forget the guard to make intent explicit.
        core::mem::forget(cleanup_guard);

        // Synchronous-commit (option α): wait for the driver to commit
        // + verify SHA-256. The driver's `await_completion` returns
        // Err(Internal) if the driver task panicked; otherwise the
        // result is the driver's actual commit outcome.
        let commit_result = driver.await_completion().await?;

        info!(
            ?stream_digest,
            committed_size = commit_result.committed_size,
            "WriteChunked: blob committed"
        );

        let committed_digest_proto =
            nativelink_proto::build::bazel::remote::execution::v2::Digest::from(stream_digest);
        Ok(WriteChunkedResponse {
            committed_digest: Some(committed_digest_proto),
            committed_size: commit_result.committed_size,
        })
    }

    /// Per-chunk admission:
    ///   1. Validate per-chunk SHA-256 (on `spawn_blocking` per #213
    ///      perf-opt NMA1).
    ///   2. Acquire global `ChunkBudget` permit (try_acquire — never
    ///      blocks; per §13.1.1 point 1).
    ///   3. `try_send` the `ChunkWork` into the per-blob mpsc. On
    ///      mpsc-full: release the permit (reverse-release per §13.1.1)
    ///      and surface `ResourceExhausted` with `PER_BLOB_MPSC_FULL`.
    async fn admit_chunk(
        &self,
        chunk: WriteChunk,
        sender: &mpsc::Sender<ChunkWork>,
        stream_digest: DigestInfo,
    ) -> Result<(), Error> {
        let WriteChunk {
            digest: _,
            chunk_offset,
            chunk_bytes,
            chunk_sha256,
            finish_chunk,
        } = chunk;

        // Defensive: the per-chunk SHA-256 must be exactly 32 bytes.
        let chunk_sha256_arr: [u8; 32] = match chunk_sha256.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => {
                return Err(make_input_err!(
                    "WriteChunk.chunk_sha256 must be 32 bytes; got {} (digest {stream_digest}, offset {chunk_offset})",
                    chunk_sha256.len()
                ));
            }
        };

        // Convert the prost-allocated `Vec<u8>` to `Bytes` (zero-copy
        // via `Bytes::from(Vec<u8>)`).
        let chunk_bytes_bytes = bytes::Bytes::from(chunk_bytes);

        // Per #213 perf-opt NMA1: SHA-256 the chunk on spawn_blocking
        // so a 1 MiB chunk's hash burn does not block a tokio worker.
        // The hash CHECK is fast enough to run inline once computed,
        // but the COMPUTATION is the load-bearing part.
        let bytes_for_hash = chunk_bytes_bytes.clone();
        let computed_sha = tokio::task::spawn_blocking(move || -> [u8; 32] {
            let mut h = Sha256::new();
            h.update(&bytes_for_hash);
            let out = h.finalize();
            let mut a = [0u8; 32];
            a.copy_from_slice(out.as_ref());
            a
        })
        .await
        .map_err(|join_err| {
            make_err!(
                Code::Internal,
                "spawn_blocking join error in per-chunk SHA-256 verify: {join_err:?}"
            )
        })?;
        if computed_sha != chunk_sha256_arr {
            return Err(make_err!(
                Code::InvalidArgument,
                "WriteChunk per-chunk SHA-256 mismatch for digest {stream_digest} at offset {chunk_offset}"
            ));
        }

        // §13.1.1 point 1, step 1: try_acquire the global budget. On
        // None → reject with ResourceExhausted + BackpressureSignal.
        let permit = match self.chunk_budget.try_acquire_chunk() {
            Some(p) => p,
            None => {
                let detail = build_backpressure_any(
                    backpressure_signal::Reason::GlobalChunkBudgetExhausted,
                    GLOBAL_BUDGET_RETRY_AFTER_MS,
                );
                return Err(Error::resource_exhausted_backpressure(
                    format!(
                        "WriteChunked: global ChunkBudget exhausted (digest {stream_digest}, offset {chunk_offset})"
                    ),
                    detail,
                ));
            }
        };

        // §13.1.1 point 1, step 2: try_send into the per-blob mpsc.
        // On Full → release the permit (reverse-release) and reject
        // with ResourceExhausted + PER_BLOB_MPSC_FULL.
        let work = ChunkWork {
            chunk_offset,
            chunk_bytes: chunk_bytes_bytes,
            chunk_sha256: chunk_sha256_arr,
            finish: finish_chunk,
            _permit: permit,
        };
        match sender.try_send(work) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(returned)) => {
                // Drop `returned` — releases the permit. Done.
                drop(returned);
                let detail = build_backpressure_any(
                    backpressure_signal::Reason::PerBlobMpscFull,
                    PER_BLOB_MPSC_RETRY_AFTER_MS,
                );
                Err(Error::resource_exhausted_backpressure(
                    format!(
                        "WriteChunked: per-blob mpsc full (digest {stream_digest}, offset {chunk_offset})"
                    ),
                    detail,
                ))
            }
            Err(mpsc::error::TrySendError::Closed(returned)) => {
                drop(returned);
                Err(make_err!(
                    Code::Internal,
                    "WriteChunked: per-blob driver task closed before finish_chunk (digest {stream_digest})"
                ))
            }
        }
    }
}

/// On any path that exits write_chunked_inner WITHOUT having explicitly
/// removed the in-flight entry, this guard removes it. Also handles
/// the case where the request future is cancelled mid-handler (tonic
/// drops the future on connection RST).
struct InFlightCleanup {
    in_flight: Arc<ChunkedWriteInFlight>,
    digest: DigestInfo,
}

impl Drop for InFlightCleanup {
    fn drop(&mut self) {
        let removed = self.in_flight.inner.lock().remove(&self.digest);
        if removed.is_some() {
            debug!(
                digest = ?self.digest,
                "WriteChunked cleanup: removed in-flight entry on early exit"
            );
        }
    }
}

fn parse_digest(chunk: &WriteChunk) -> Result<DigestInfo, Error> {
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

fn err_to_status(err: Error) -> Status {
    // The Error → Status conversion preserves the `details` (per the
    // not_found_with_detail / resource_exhausted_backpressure pathway
    // already used elsewhere in the codebase). The wire-stable
    // BackpressureSignal type_url is what the receiver-side
    // `looks_like_dead_channel` classifier matches against.
    Status::from(err)
}

/// Wait helper used by the integration tests: poll for an in-flight
/// entry to disappear under a tokio::time::timeout. The polling loop
/// uses `yield_now` rather than `sleep` per CLAUDE.md test-discipline.
pub async fn wait_for_no_in_flight(
    in_flight: &Arc<ChunkedWriteInFlight>,
    timeout: core::time::Duration,
) -> Result<(), &'static str> {
    tokio::time::timeout(timeout, async {
        while in_flight.in_flight_count() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "in-flight entries did not drain within timeout")
}
