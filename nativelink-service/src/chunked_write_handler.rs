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

use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, publish,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    WriteChunk, WriteChunkedResponse, backpressure_signal,
};
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{
    ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP,
};
use nativelink_store::chunked_signal::encode_backpressure_signal_any;
use nativelink_store::filesystem_store::{FileEntry, FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;

/// Backoff hint suggested to the client on global-budget exhaustion.
/// Matches the design §13.1.1 retry-after default for the global axis.
const GLOBAL_BUDGET_RETRY_AFTER_MS: u64 = 100;

/// Backoff hint suggested to the client on per-blob mpsc-full. Per-blob
/// queues drain faster than the global budget so the hint is shorter
/// (per design §13.1.1).
const PER_BLOB_MPSC_RETRY_AFTER_MS: u64 = 25;

/// Backoff hint when another in-flight stream owns this digest. The
/// duration is loosely sized to "a typical commit + e2e SHA-256 verify
/// for a medium-sized blob"; on the order of seconds for multi-MiB
/// blobs. Clients should jitter. The retry-hint is advisory; the
/// authoritative termination signal is the absence of the in-flight
/// entry.
const CONCURRENT_SAME_DIGEST_RETRY_AFTER_MS: u64 = 250;

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

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub(crate) fn contains(&self, digest: &DigestInfo) -> bool {
        self.inner.lock().contains_key(digest)
    }

    /// Returns the number of currently in-flight chunked writes.
    /// Used by tests + future metric wiring.
    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.inner.lock().len()
    }
}

impl MetricsComponent for ChunkedWriteInFlight {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let streams_active = self.in_flight_count() as u64;
        publish!(
            "streams_active",
            &streams_active,
            MetricKind::Default,
            "WriteChunked: streams currently in-flight in this server (per-blob driver entries in the in-flight tracker)"
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// Aggregate counters for the WriteChunked handler. Operator-actionable
/// from the moment the `chunked_fast_slow` feature is flipped on; per-blob
/// keyed counters (digest-labeled) defer to Phase 2.5+.
///
/// `Relaxed` ordering throughout — these are observation-only counters,
/// no other state depends on the ordering relative to the work they
/// describe. The metric publish reads via `load(Relaxed)` for the same
/// reason.
#[derive(Debug, Default, MetricsComponent)]
pub struct ChunkedWriteHandlerMetrics {
    #[metric(help = "WriteChunked: chunks admitted (per-chunk SHA-256 + budget + mpsc all Ok)")]
    chunks_admitted_total: AtomicU64,
    #[metric(help = "WriteChunked: per-chunk SHA-256 verify mismatches (admission rejections)")]
    sha256_per_chunk_mismatches_total: AtomicU64,
    #[metric(
        help = "WriteChunked: end-to-end SHA-256 verify mismatches (assembled blob does not match digest)"
    )]
    sha256_e2e_mismatches_total: AtomicU64,
    #[metric(help = "WriteChunked: rejections from another in-flight stream owning the same digest")]
    concurrent_same_digest_rejections_total: AtomicU64,
    #[metric(help = "WriteChunked: per-blob mpsc full rejections (PER_BLOB_MPSC_FULL signal)")]
    mpsc_full_rejections_total: AtomicU64,
    #[metric(
        help = "WriteChunked: global ChunkBudget exhausted rejections (GLOBAL_CHUNK_BUDGET_EXHAUSTED signal)"
    )]
    global_budget_exhausted_rejections_total: AtomicU64,
    #[metric(help = "WriteChunked: blobs committed (commit + e2e SHA-256 verify both OK)")]
    chunks_committed_total: AtomicU64,
    #[metric(help = "WriteChunked: commit failures (commit_chunked or e2e SHA-256 returned Err)")]
    commit_failures_total: AtomicU64,
}

/// Server-side handler for the `WriteChunked` RPC. Holds the
/// FilesystemStore the handler writes to + the in-flight tracker +
/// (today) a borrowed reference to the global ChunkBudget singleton.
///
/// One instance per server process; cheap to clone (all fields are
/// `Arc` / `&'static`).
///
/// `chunk_size` is the contractual chunk size used when constructing
/// per-blob `ChunkedDriver`s (drives the bitmap completeness check).
/// In production this is `CHUNK_SIZE` (1 MiB). The field exists so
/// tests can use smaller chunks for speed without depending on the
/// production constant — admission still rejects mis-aligned chunks
/// in the per-chunk SHA-256 step (the producer error surface),
/// because the driver only commits when the bitmap matches the
/// expected count.
#[derive(MetricsComponent)]
pub struct ChunkedWriteHandler<Fe: FileEntry = FileEntryImpl> {
    filesystem_store: Arc<FilesystemStore<Fe>>,
    #[metric(group = "in_flight")]
    in_flight: Arc<ChunkedWriteInFlight>,
    chunk_budget: &'static ChunkBudget,
    chunk_size: usize,
    #[metric(group = "totals")]
    metrics: Arc<ChunkedWriteHandlerMetrics>,
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
    /// to the process-wide `ChunkBudget` singleton. Production callers
    /// use this constructor; the chunk size is the production CHUNK_SIZE
    /// (1 MiB).
    #[must_use]
    pub fn new(filesystem_store: Arc<FilesystemStore<Fe>>) -> Self {
        Self {
            filesystem_store,
            in_flight: ChunkedWriteInFlight::new(),
            chunk_budget: nativelink_store::chunked::chunk_budget::chunk_budget_singleton(),
            chunk_size: CHUNK_SIZE,
            metrics: Arc::new(ChunkedWriteHandlerMetrics::default()),
        }
    }

    /// Construct a handler with externally-provided in-flight tracker
    /// + chunk budget. Used by tests so the test harness can observe
    /// the in-flight map AND so each test gets its own budget (avoiding
    /// cross-test interference on the process-wide singleton).
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
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
            chunk_size: CHUNK_SIZE,
            metrics: Arc::new(ChunkedWriteHandlerMetrics::default()),
        }
    }

    /// Construct a handler with externally-provided state AND an
    /// explicit chunk size. Used by integration tests that exercise
    /// out-of-order arrival + bitmap completeness with smaller chunks
    /// (a 4 KiB chunk is fast; a 1 MiB chunk burns memory + time).
    /// NOT for production use — gated behind `#[cfg(any(test,
    /// feature = "test-utils"))]` so production builds cannot accidentally
    /// instantiate a handler with a non-`CHUNK_SIZE` chunk_size.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn new_with_state_and_chunk_size_for_test(
        filesystem_store: Arc<FilesystemStore<Fe>>,
        in_flight: Arc<ChunkedWriteInFlight>,
        chunk_budget: &'static ChunkBudget,
        chunk_size: usize,
    ) -> Self {
        Self {
            filesystem_store,
            in_flight,
            chunk_budget,
            chunk_size,
            metrics: Arc::new(ChunkedWriteHandlerMetrics::default()),
        }
    }

    /// Read-only accessor on the in-flight tracker. Used by tests for
    /// production-composition assertions (e.g. "in-flight entry must
    /// drain after commit"); `pub` because the test crate is a separate
    /// compilation unit.
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

        // M-code-1 fixup: zero-byte blob (`digest.size_bytes() == 0`)
        // bypasses the driver entirely. The driver's bitmap completeness
        // check expects `landed_offsets.len() == expected_chunk_count`
        // (= 0 for a zero-byte blob), but the producer MUST send a single
        // `finish_chunk` with empty bytes to terminate the stream — that
        // chunk would push `landed_offsets.len()` to 1, mismatching the
        // expected 0 and rejecting every empty-blob upload as
        // InvalidArgument. Bazel emits the empty-string digest
        // (`e3b0c44…-0`) frequently for empty stdout/stderr, so the
        // empty-blob path is hot.
        if digest.size_bytes() == 0 {
            return self
                .handle_empty_blob(stream, first_chunk, stream_digest)
                .await;
        }

        // Look up or create the per-blob driver. Today: one driver per
        // digest at a time; concurrent streams for the same digest are
        // rejected with `Code::Aborted` + a retry hint (M-code-2 fixup).
        // The historical `AlreadyExists` was misleading on the wire —
        // gRPC convention treats `AlreadyExists` as "the resource is
        // durably committed," which a worker-side BIS-style auto-unpinner
        // could read as a license to drop its mirror pin (losing the
        // only durable copy if the OTHER stream then errors). `Aborted`
        // means "transaction failed, retry" and carries the wire-stable
        // BackpressureSignal detail with retry-after.
        let (sender, driver) = {
            let mut guard = self.in_flight.inner.lock();
            if guard.contains_key(&digest) {
                self.metrics
                    .concurrent_same_digest_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
                let detail = encode_backpressure_signal_any(
                    backpressure_signal::Reason::PerBlobMpscFull,
                    CONCURRENT_SAME_DIGEST_RETRY_AFTER_MS,
                );
                return Err(Error::aborted_with_detail(
                    format!(
                        "WriteChunked: another stream is already writing digest {digest}; \
                         retry after a backoff (concurrent-stream rejection, NOT durable commit)"
                    ),
                    detail,
                ));
            }
            // Spawn driver. Capacity = PER_BLOB_MPSC_CAP (16).
            let (driver, sender) = ChunkedDriver::spawn_driver(
                Arc::clone(&self.filesystem_store),
                digest,
                digest.size_bytes(),
                self.chunk_size,
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
            (sender, driver_arc)
        };

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

        // M-perf-3 + B1 coupling: KEEP the in-flight entry alive across
        // `await_completion`. The entry holds an `Arc<ChunkedDriver>`,
        // so even if THIS handler future is cancelled (tonic RST,
        // upstream timeout) and our local `driver` Arc drops, the
        // map's Arc keeps the spawned task alive long enough to finish
        // the two-stage commit. Without this, the cancellation race
        // could land between rename-1 and SHA-256 verify or between
        // verify and rename-2, both of which can leave the on-disk
        // state inconsistent.
        //
        // To signal "no more chunks" to the driver, we must drop EVERY
        // sender clone — both the local one and the one stored in the
        // InFlightEntry. We swap the sender out of the entry and drop
        // both, which closes the mpsc cleanly and triggers the driver's
        // happy-path termination (§6.7 trigger a). The entry remains
        // in the map (now with no sender, just the driver Arc).
        drop(sender);
        {
            let mut guard = self.in_flight.inner.lock();
            if let Some(entry) = guard.get_mut(&stream_digest) {
                // Replace with a closed-sender placeholder so the entry's
                // sender field doesn't keep the mpsc alive. The
                // `mpsc::channel(1).0` we drop immediately is the
                // canonical "create a closed-on-drop sender of the right
                // type" pattern; we never use it for sending.
                let (placeholder_tx, _placeholder_rx) =
                    mpsc::channel::<ChunkWork>(1);
                let stored_sender = core::mem::replace(&mut entry.sender, placeholder_tx);
                drop(stored_sender);
                drop(_placeholder_rx);
            }
        }

        // Synchronous-commit (option α): wait for the driver to commit
        // + verify SHA-256 (two-stage rename per B1 fixup). The driver's
        // `await_completion` returns Err(Internal) if the driver task
        // panicked; otherwise the result is the driver's actual commit
        // outcome.
        let commit_result = driver.await_completion().await;

        // Whatever the outcome, remove the in-flight entry NOW (the
        // driver task has signaled completion; the on-disk holding /
        // partial files have either been finalized or cleaned up
        // inside `commit_and_verify`). The cleanup_guard's `Drop`
        // would also do this; we forget it because we have already
        // removed the entry on the happy path.
        let removed_entry = self.in_flight.inner.lock().remove(&stream_digest);
        drop(removed_entry);
        core::mem::forget(cleanup_guard);

        let commit_result = match commit_result {
            Ok(r) => r,
            Err(err) => {
                self.metrics
                    .commit_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                if err.code == Code::InvalidArgument
                    && err.message_string().contains("end-to-end SHA-256 mismatch")
                {
                    self.metrics
                        .sha256_e2e_mismatches_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(err);
            }
        };

        self.metrics
            .chunks_committed_total
            .fetch_add(1, Ordering::Relaxed);
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

    /// Per-chunk admission ordering (post-fixup; M-perf-1):
    ///   1. Cheap field-shape validation (sha256 length, chunk-shape per
    ///      M-code-3: offset alignment, per-chunk length, finish-chunk
    ///      total-length math).
    ///   2. **Global `ChunkBudget` `try_acquire`** (microseconds; never
    ///      blocks; per §13.1.1 point 1) — BEFORE the SHA-256 spawn so a
    ///      malicious peer streaming garbage at line rate can't burn CPU
    ///      on `spawn_blocking` SHA work that gets rejected anyway.
    ///   3. `spawn_blocking` SHA-256 verify (#213 perf-opt NMA1). On
    ///      mismatch: explicitly drop the permit (reverse-release) and
    ///      surface `Code::InvalidArgument`.
    ///   4. `try_send` the `ChunkWork` into the per-blob mpsc. On Full:
    ///      reverse-release the permit and surface
    ///      `ResourceExhausted` with `PER_BLOB_MPSC_FULL`.
    ///
    /// The reorder bounds CPU burn under bad-peer attack to the budget
    /// cap (4 GiB worth of in-flight chunks, then rejection). The happy
    /// path cost is unchanged (the SHA work still runs; it's just under
    /// permit ownership now).
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

        // Step 1a: per-chunk SHA-256 byte shape (cheap; pre-permit).
        let chunk_sha256_arr: [u8; 32] = match chunk_sha256.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => {
                return Err(make_input_err!(
                    "WriteChunk.chunk_sha256 must be 32 bytes; got {} (digest {stream_digest}, offset {chunk_offset})",
                    chunk_sha256.len()
                ));
            }
        };

        // Step 1b (M-code-3): chunk-shape validation. The proto
        // contract says (proto file 688-693): `chunk_offset` MUST be a
        // multiple of `CHUNK_SIZE`; `chunk_bytes.len()` MUST be exactly
        // `CHUNK_SIZE` for non-final chunks and at most `CHUNK_SIZE`
        // for the final chunk; the final chunk's `chunk_offset +
        // chunk_bytes.len()` MUST equal `digest.size_bytes`. Without
        // enforcement here, a malformed producer can:
        //   - send arbitrary offsets that pass per-chunk SHA (it's the
        //     producer's hash) and write into sparse-file holes that
        //     coincidentally match `digest.size_bytes`, OR
        //   - send oversized chunks that bypass the per-permit byte
        //     accounting (one permit = one CHUNK_SIZE).
        let chunk_size_u64 = self.chunk_size as u64;
        if !chunk_offset.is_multiple_of(chunk_size_u64) {
            return Err(make_input_err!(
                "WriteChunk.chunk_offset must be a multiple of CHUNK_SIZE ({} bytes); \
                 got chunk_offset={chunk_offset} for digest {stream_digest}",
                self.chunk_size
            ));
        }
        let chunk_bytes_len = chunk_bytes.len();
        if !finish_chunk && chunk_bytes_len != self.chunk_size {
            return Err(make_input_err!(
                "WriteChunk.chunk_bytes.len() must equal CHUNK_SIZE ({}) for non-final chunks; \
                 got {chunk_bytes_len} for digest {stream_digest} at offset {chunk_offset}",
                self.chunk_size
            ));
        }
        if finish_chunk && chunk_bytes_len > self.chunk_size {
            return Err(make_input_err!(
                "WriteChunk.chunk_bytes.len() must be <= CHUNK_SIZE ({}) for the final chunk; \
                 got {chunk_bytes_len} for digest {stream_digest} at offset {chunk_offset}",
                self.chunk_size
            ));
        }
        if finish_chunk {
            let declared_size = stream_digest.size_bytes();
            let chunk_end = chunk_offset.saturating_add(chunk_bytes_len as u64);
            if chunk_end != declared_size {
                return Err(make_input_err!(
                    "final WriteChunk.chunk_offset + chunk_bytes.len() must equal \
                     digest.size_bytes ({declared_size}); got {chunk_end} for digest \
                     {stream_digest} at offset {chunk_offset} with len {chunk_bytes_len}"
                ));
            }
        }

        // Convert the prost-allocated `Vec<u8>` to `Bytes` (zero-copy
        // via `Bytes::from(Vec<u8>)`).
        let chunk_bytes_bytes: Bytes = Bytes::from(chunk_bytes);

        // Step 2 (M-perf-1): try_acquire the global budget BEFORE the
        // SHA-256 spawn. `try_acquire` is cheap (single atomic + branch);
        // doing it first means a rogue peer streaming bad chunks at line
        // rate gets rejected without burning a `spawn_blocking` worker
        // on SHA work that we already know we won't accept.
        let permit = match self.chunk_budget.try_acquire_chunk() {
            Some(p) => p,
            None => {
                self.metrics
                    .global_budget_exhausted_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
                let detail = encode_backpressure_signal_any(
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

        // Step 3: per-chunk SHA-256 on `spawn_blocking` (#213 NMA1) so a
        // 1 MiB chunk's hash burn does not block a tokio worker. We hold
        // the permit across this await; on mismatch we explicitly drop
        // the permit before returning to release it back to the budget.
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
            // Permit is dropped on the early return.
            make_err!(
                Code::Internal,
                "spawn_blocking join error in per-chunk SHA-256 verify: {join_err:?}"
            )
        })?;
        if computed_sha != chunk_sha256_arr {
            self.metrics
                .sha256_per_chunk_mismatches_total
                .fetch_add(1, Ordering::Relaxed);
            // Reverse-release: drop the permit so the budget recovers.
            drop(permit);
            return Err(make_err!(
                Code::InvalidArgument,
                "WriteChunk per-chunk SHA-256 mismatch for digest {stream_digest} at offset {chunk_offset}"
            ));
        }

        // Step 4: try_send into the per-blob mpsc. On Full → release
        // the permit (reverse-release) and reject with ResourceExhausted
        // + PER_BLOB_MPSC_FULL.
        let work = ChunkWork {
            chunk_offset,
            chunk_bytes: chunk_bytes_bytes,
            chunk_sha256: chunk_sha256_arr,
            finish: finish_chunk,
            _permit: permit,
        };
        match sender.try_send(work) {
            Ok(()) => {
                self.metrics
                    .chunks_admitted_total
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(returned)) => {
                // Drop `returned` — releases the permit. Done.
                drop(returned);
                self.metrics
                    .mpsc_full_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
                let detail = encode_backpressure_signal_any(
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

    /// M-code-1 fixup: zero-byte blob shortcut. Bypasses the per-blob
    /// driver entirely — for a zero-size blob, the producer MUST send
    /// EXACTLY one `WriteChunk` with `chunk_offset == 0`,
    /// `chunk_bytes` empty, and `finish_chunk == true`. We:
    ///   1. Validate the single-chunk shape AND verify
    ///      `chunk_sha256` matches the SHA-256 of the empty string
    ///      (`e3b0c44…`).
    ///   2. Validate the digest's hash is also the empty-string SHA-256
    ///      (otherwise the producer is lying about the size or the
    ///      hash; either way it's an InvalidArgument).
    ///   3. Create the empty file at the canonical CAS path on
    ///      `spawn_blocking`.
    ///   4. Verify no further chunks arrive on the stream (a producer
    ///      that sends a second chunk after a finish is broken).
    /// Returns `Code::InvalidArgument` on any shape violation; never
    /// touches the in-flight tracker (no entry inserted).
    async fn handle_empty_blob(
        &self,
        mut stream: Streaming<WriteChunk>,
        first_chunk: WriteChunk,
        stream_digest: DigestInfo,
    ) -> Result<WriteChunkedResponse, Error> {
        // SHA-256 of the empty string. Pinned constant — guaranteed
        // wire-stable per the SHA-256 spec.
        const EMPTY_SHA256: [u8; 32] = [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55,
        ];

        // (1) chunk-shape validation for the empty-blob path.
        if first_chunk.chunk_offset != 0 {
            return Err(make_input_err!(
                "WriteChunked zero-byte blob: chunk_offset must be 0; got {} for digest {stream_digest}",
                first_chunk.chunk_offset
            ));
        }
        if !first_chunk.chunk_bytes.is_empty() {
            return Err(make_input_err!(
                "WriteChunked zero-byte blob: chunk_bytes must be empty; got len={} for digest {stream_digest}",
                first_chunk.chunk_bytes.len()
            ));
        }
        if !first_chunk.finish_chunk {
            return Err(make_input_err!(
                "WriteChunked zero-byte blob: finish_chunk must be true on the single chunk; got false for digest {stream_digest}"
            ));
        }

        // (1) per-chunk SHA-256: must equal EMPTY_SHA256.
        let chunk_sha256_arr: [u8; 32] = match first_chunk.chunk_sha256.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => {
                return Err(make_input_err!(
                    "WriteChunked zero-byte blob: chunk_sha256 must be 32 bytes; got {} for digest {stream_digest}",
                    first_chunk.chunk_sha256.len()
                ));
            }
        };
        if chunk_sha256_arr != EMPTY_SHA256 {
            self.metrics
                .sha256_per_chunk_mismatches_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(make_err!(
                Code::InvalidArgument,
                "WriteChunked zero-byte blob: chunk_sha256 must equal SHA-256(\"\") for digest {stream_digest}"
            ));
        }

        // (2) digest hash validation.
        let declared: [u8; 32] = **stream_digest.packed_hash();
        if declared != EMPTY_SHA256 {
            self.metrics
                .sha256_e2e_mismatches_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(make_err!(
                Code::InvalidArgument,
                "WriteChunked zero-byte blob: digest.hash must equal SHA-256(\"\") (e3b0c44…) for size=0; got digest {stream_digest}"
            ));
        }

        // (3) Create the empty file at the canonical CAS path. Done on
        // `spawn_blocking` because the open + close + parent-dir lookup
        // syscalls can stall on a busy ZFS pool. NO `fsync` (CLAUDE.md
        // hard rule).
        let final_path_os =
            nativelink_store::filesystem_store::digest_content_path(
                self.filesystem_store.content_path_for_chunked(),
                &stream_digest,
            );
        let final_path_pb = std::path::PathBuf::from(&final_path_os);
        let final_path_for_err = final_path_pb.clone();
        let create_result = tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
            // create_new=false so an idempotent retry of the same digest
            // (which would already exist on disk from a prior commit) is
            // a no-op. write(true) so the file is opened for the
            // immediate-close pattern; no bytes are written.
            let _file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&final_path_pb)?;
            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o555);
                if let Err(err) = std::fs::set_permissions(&final_path_pb, perms) {
                    tracing::warn!(?err, path = ?final_path_pb, "Failed to set CAS file permissions to 0o555 on zero-byte blob commit");
                }
            }
            Ok(())
        })
        .await
        .map_err(|join_err| {
            make_err!(
                Code::Internal,
                "spawn_blocking join error creating zero-byte CAS file for {stream_digest}: {join_err:?}"
            )
        })?
        .map_err(|io_err| {
            make_err!(
                Code::Internal,
                "failed to create zero-byte CAS file at {}: {io_err:?}",
                final_path_for_err.display()
            )
        });
        if let Err(err) = create_result {
            self.metrics
                .commit_failures_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }

        // (4) Verify no further chunks arrive on the stream.
        match stream.message().await {
            Ok(None) => {
                // Stream closed cleanly after the single finish chunk.
            }
            Ok(Some(_)) => {
                return Err(make_input_err!(
                    "WriteChunked zero-byte blob: producer sent a second chunk after finish_chunk for digest {stream_digest}"
                ));
            }
            Err(status) => {
                // Tolerate stream errors after a successful empty-blob
                // commit — the file is on disk and the digest is the
                // empty hash, so the commit is durable.
                debug!(
                    ?stream_digest,
                    status_code = ?status.code(),
                    status_msg = %status.message(),
                    "WriteChunked zero-byte blob: stream errored AFTER successful empty commit; ignoring"
                );
            }
        }

        self.metrics
            .chunks_committed_total
            .fetch_add(1, Ordering::Relaxed);
        info!(
            ?stream_digest,
            committed_size = 0u64,
            "WriteChunked zero-byte blob committed via empty-blob shortcut"
        );

        let committed_digest_proto =
            nativelink_proto::build::bazel::remote::execution::v2::Digest::from(stream_digest);
        Ok(WriteChunkedResponse {
            committed_digest: Some(committed_digest_proto),
            committed_size: 0,
        })
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
