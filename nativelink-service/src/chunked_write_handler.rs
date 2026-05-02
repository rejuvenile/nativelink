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

//! #212 Phase 2.2/2.7: server-side chunked-write dispatch.
//!
//! Two production producers feed the same per-blob `ChunkedDriver`
//! plumbing:
//!
//! - **Phase 2.2 — `WriteChunked` RPC** (`ChunkedWriteHandler::write_chunked`):
//!   worker→server upload via the new `WriteChunked(stream WriteChunk)`
//!   RPC. The producer (worker) is itself the slow-tier writer, so
//!   commit is **synchronous (option α)** — the RPC returns Ok only
//!   after the on-disk file lands at the canonical CAS path + SHA-256
//!   passes.
//! - **Phase 2.7 — Bazel-facing internal chunking**
//!   (`dispatch_bazel_facing_internal_chunking`): Bazel's standard
//!   ByteStream Write hits `FastSlowStore::update`; for blobs ≥
//!   `CHUNK_SIZE` and behind the `bazel_facing_internal_chunking`
//!   AtomicBool kill-switch, the server batches the in-order bytes into
//!   1 MiB chunks (computing per-chunk SHA-256 on `spawn_blocking` per
//!   #213 NMA1) and dispatches into the same per-blob driver. Commit is
//!   **asynchronous (option β)** — `update()` returns Ok as soon as the
//!   final chunk is admitted to the per-blob mpsc; the driver completes
//!   on its own task. The fast-tier (MemoryStore) write that already
//!   happens in `update()` provides the in-memory replica that satisfies
//!   the ≥2-replica invariant; the chunked-driver's drain to disk is
//!   the slow-tier replica. The reason this asymmetry vs option α exists
//!   is the **anti-#203 invariant** (CLAUDE.md
//!   `feedback_async_to_sync_requires_explicit_signoff`): if a Bazel-
//!   facing write blocks on slow-tier latency, the precise mechanism
//!   that caused the 2026-04-28 OOM cascade is reproduced.
//!
//! The two paths share `dispatch_chunks_to_driver`, which:
//!   1. Looks up or creates the per-blob driver in the in-flight map.
//!   2. Admits each prepared chunk via `admit_prepared_chunk` (validate +
//!      global-budget try_acquire + mpsc try_send; reverse-release on
//!      Err). The per-chunk SHA-256 is supplied by the caller — the
//!      `WriteChunked` RPC carries it on the wire; the Bazel-facing
//!      path computes it server-side on `spawn_blocking`.
//!   3. After the final chunk is admitted, drops the sender. In
//!      `CommitMode::Synchronous` it then awaits the driver's commit
//!      result; in `CommitMode::AsyncCommit` it returns Ok with the
//!      declared blob size (driver continues in the background, in-flight
//!      map still owns the `Arc<ChunkedDriver>`).

#![cfg(feature = "chunked_fast_slow")]

use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use futures::StreamExt as _;
use parking_lot::Mutex;
use nativelink_util::digest_hasher::{DigestHasher, default_digest_hasher_func};
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
use nativelink_store::chunked::pin_budget::{PinBudget, pin_budget_singleton};
use nativelink_store::chunked_signal::encode_backpressure_signal_any;
use nativelink_store::filesystem_store::{FileEntry, FileEntryImpl, FilesystemStore};
use nativelink_util::buf_channel::DropCloserReadHalf;
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

/// #212 Phase 2.5/2.7 fixup B1: backoff hint suggested to the client on
/// pinned-bytes-budget exhaustion. Pinned bytes drain when the chunked
/// driver completes its slow-tier commit, which depends on slow-tier
/// latency; 100 ms matches the global-budget hint as a reasonable
/// lower bound.
const PIN_BUDGET_RETRY_AFTER_MS: u64 = 100;

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

    /// Like `contains` but always available (the test-only `contains` is
    /// gated; `contains_digest` is needed by the production B2 reaper to
    /// observe driver completion). Cheap (one HashMap lookup under the
    /// parking_lot mutex; no `.await` crossing).
    #[must_use]
    pub fn contains_digest(&self, digest: &DigestInfo) -> bool {
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
    pub chunks_admitted_total: AtomicU64,
    #[metric(help = "WriteChunked: per-chunk SHA-256 verify mismatches (admission rejections)")]
    pub sha256_per_chunk_mismatches_total: AtomicU64,
    #[metric(
        help = "WriteChunked: end-to-end SHA-256 verify mismatches (assembled blob does not match digest)"
    )]
    pub sha256_e2e_mismatches_total: AtomicU64,
    #[metric(help = "WriteChunked: rejections from another in-flight stream owning the same digest")]
    pub concurrent_same_digest_rejections_total: AtomicU64,
    #[metric(help = "WriteChunked: per-blob mpsc full rejections (PER_BLOB_MPSC_FULL signal)")]
    pub mpsc_full_rejections_total: AtomicU64,
    #[metric(
        help = "WriteChunked: global ChunkBudget exhausted rejections (GLOBAL_CHUNK_BUDGET_EXHAUSTED signal)"
    )]
    pub global_budget_exhausted_rejections_total: AtomicU64,
    #[metric(
        help = "WriteChunked: global PinBudget exhausted rejections (PINNED_BYTES_EXHAUSTED signal; #212 fixup B1)"
    )]
    pub pin_budget_exhausted_rejections_total: AtomicU64,
    #[metric(help = "WriteChunked: blobs committed (commit + e2e SHA-256 verify both OK)")]
    pub chunks_committed_total: AtomicU64,
    #[metric(help = "WriteChunked: commit failures (commit_chunked or e2e SHA-256 returned Err)")]
    pub commit_failures_total: AtomicU64,
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
            // Legacy WriteChunked RPC path doesn't currently wire the
            // ChunkedReadRegistry; reads of in-flight chunked-write
            // bytes here would fall through to the slow store. Phase
            // 2.5 cascade integration with the worker-driven path is
            // out-of-scope for this fixup (which targets the Bazel-
            // facing path).
            chunked_read_registry: None,
        };

        // Process the first chunk + every subsequent chunk in the
        // stream. On finish_chunk we `await` the driver's commit and
        // return the response.
        //
        // #213 d-s-r MAJOR-1: every early-Err path in this loop must
        // discard the on-disk partial via `discard_partial_best_effort`
        // BEFORE returning. Otherwise sustained client-disconnect storms
        // accumulate `<digest>.partial` files until next FilesystemStore::new.
        if let Err(err) = self.admit_chunk(first_chunk, &sender, stream_digest).await {
            warn!(
                ?stream_digest,
                ?err,
                "WriteChunked: first chunk admission failed"
            );
            discard_partial_best_effort(&self.filesystem_store, &stream_digest).await;
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
                    discard_partial_best_effort(&self.filesystem_store, &stream_digest).await;
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
                    discard_partial_best_effort(&self.filesystem_store, &stream_digest).await;
                    drop(cleanup_guard);
                    let err: Error = status.into();
                    return Err(err.append(format!(
                        "error mid-stream WriteChunk for digest {stream_digest}"
                    )));
                }
            };
            let next_digest = parse_digest(&next)?;
            if next_digest != stream_digest {
                discard_partial_best_effort(&self.filesystem_store, &stream_digest).await;
                drop(cleanup_guard);
                return Err(make_input_err!(
                    "WriteChunked stream switched digest mid-stream: started {stream_digest}, got {next_digest}"
                ));
            }
            let is_last = next.finish_chunk;
            if let Err(err) = self.admit_chunk(next, &sender, stream_digest).await {
                discard_partial_best_effort(&self.filesystem_store, &stream_digest).await;
                return Err(err);
            }
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
    ///
    /// Step 3 (`spawn_blocking` SHA-256 verify) is performed BEFORE this
    /// function via `verify_write_chunk_sha256` so the producer's wire-
    /// supplied hash is checked against the bytes; only on success do we
    /// build a `PreparedChunk` and hand it to the shared
    /// `admit_prepared_chunk` helper. Splitting the steps keeps the
    /// shared dispatch path symmetric with the Bazel-facing path (which
    /// computes the SHA-256 itself rather than verifying a wire-supplied
    /// one).
    ///
    /// The reorder bounds CPU burn under bad-peer attack to the budget
    /// cap (4 GiB worth of in-flight chunks, then rejection). The happy
    /// path cost is unchanged (the SHA work still runs; it's just split
    /// across two helpers now).
    async fn admit_chunk(
        &self,
        chunk: WriteChunk,
        sender: &mpsc::Sender<ChunkWork>,
        stream_digest: DigestInfo,
    ) -> Result<(), Error> {
        let prepared = self
            .verify_and_prepare_chunk(chunk, stream_digest)
            .await?;
        admit_prepared_chunk(
            prepared,
            sender,
            self.chunk_budget,
            // Legacy WriteChunked RPC: producer is the worker (not Bazel)
            // — the worker's own admission already enforces per-process
            // bounds, so no PinBudget cap on this path.
            None,
            self.chunk_size,
            stream_digest,
            &self.metrics,
        )
    }

    /// Convert a wire `WriteChunk` into a `PreparedChunk` by:
    ///   1. Validating `chunk_sha256` byte-shape (must be 32 bytes).
    ///   2. Validating chunk-shape (offset alignment, length per
    ///      finish flag, total-length math) — these checks duplicate
    ///      `admit_prepared_chunk`'s validation but firing earlier
    ///      keeps the SHA-256 spawn off the failed-shape path.
    ///   3. Computing the actual SHA-256 of `chunk_bytes` on
    ///      `spawn_blocking` (#213 NMA1) and verifying it equals the
    ///      wire-supplied `chunk_sha256`.
    ///
    /// Mismatch on any of (1)–(3) is an `InvalidArgument` rejection
    /// against the producer.
    async fn verify_and_prepare_chunk(
        &self,
        chunk: WriteChunk,
        stream_digest: DigestInfo,
    ) -> Result<PreparedChunk, Error> {
        let WriteChunk {
            digest: _,
            chunk_offset,
            chunk_bytes,
            chunk_sha256,
            finish_chunk,
        } = chunk;

        // Step 1: per-chunk SHA-256 byte shape.
        let chunk_sha256_arr: [u8; 32] = match chunk_sha256.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => {
                return Err(make_input_err!(
                    "WriteChunk.chunk_sha256 must be 32 bytes; got {} (digest {stream_digest}, offset {chunk_offset})",
                    chunk_sha256.len()
                ));
            }
        };

        // Step 2 (M-code-3): chunk-shape validation.
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

        // After #212 Phase 2.4 fixup B1 part 2 the prost field is
        // already `bytes::Bytes` (was `Vec<u8>`); the conversion below
        // is a refcount move.
        let chunk_bytes_bytes: Bytes = chunk_bytes;

        // Step 3: SHA-256 verify on `spawn_blocking` (#213 NMA1).
        let computed_sha = compute_sha256_blocking(chunk_bytes_bytes.clone()).await?;
        if computed_sha != chunk_sha256_arr {
            self.metrics
                .sha256_per_chunk_mismatches_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(make_err!(
                Code::InvalidArgument,
                "WriteChunk per-chunk SHA-256 mismatch for digest {stream_digest} at offset {chunk_offset}"
            ));
        }

        Ok(PreparedChunk {
            chunk_offset,
            chunk_bytes: chunk_bytes_bytes,
            chunk_sha256: chunk_sha256_arr,
            finish: finish_chunk,
        })
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

/// #212 v4.5: `CasExtensions` trait impl. The wire-routing fix moves
/// `WriteChunked` off `WorkerApi` (port 50061 in production, never
/// reachable from `GrpcStore`'s outbound CAS-endpoint channel) onto the
/// CAS-adjacent `CasExtensions` service so it lands on the same listener
/// as `cas` / `bytestream` (port 50071) — see `bin/nativelink.rs` for
/// registration. The implementation just delegates to the inherent
/// `write_chunked` method to keep the call-site shape unchanged.
#[async_trait::async_trait]
impl<Fe: FileEntry>
    nativelink_proto::com::github::trace_machina::nativelink::remote_execution::cas_extensions_server::CasExtensions
    for ChunkedWriteHandler<Fe>
{
    async fn write_chunked(
        &self,
        request: Request<Streaming<WriteChunk>>,
    ) -> Result<Response<WriteChunkedResponse>, Status> {
        ChunkedWriteHandler::write_chunked(self, request).await
    }
}

/// On any path that exits write_chunked_inner WITHOUT having explicitly
/// removed the in-flight entry, this guard removes it. Also handles
/// the case where the request future is cancelled mid-handler (tonic
/// drops the future on connection RST).
///
/// #212 fixup S1: also deregisters the per-blob driver from the
/// `ChunkedReadRegistry` if one was wired in (so a panicked /
/// cancelled dispatch leaves no stale entry for Phase 2.5's read
/// cascade).
struct InFlightCleanup {
    in_flight: Arc<ChunkedWriteInFlight>,
    digest: DigestInfo,
    chunked_read_registry:
        Option<Arc<nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry>>,
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
        if let Some(reg) = self.chunked_read_registry.as_ref() {
            let _ = reg.deregister(&self.digest);
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

/// #213 d-s-r MAJOR-1 helper: best-effort GC of an in-flight chunked
/// partial after `update()` returns Err. Called from
/// [`dispatch_chunks_to_driver`]'s early-Err exits (chunk-stream pull
/// failure OR admission failure) so the on-disk partial is discarded
/// promptly instead of waiting for next-startup `prune_temp_path`.
///
/// Without this, sustained client-disconnect storms (network flap,
/// cancellation cascades) would accumulate `<digest>.partial` files
/// on disk AND keep `chunked_partials` map entries alive (the
/// per-blob `ChunkInProgress` entry holds the file fd until the map
/// entry is removed). The accumulation degrades the
/// `chunk_budget_used_bytes` Q4 budget monotonically until restart.
///
/// Best-effort: discard errors are logged at `warn!` and ignored.
/// The original upstream error is what surfaces to the producer.
async fn discard_partial_best_effort<Fe: FileEntry>(
    filesystem_store: &Arc<FilesystemStore<Fe>>,
    digest: &DigestInfo,
) {
    if let Err(discard_err) = filesystem_store.discard_chunked(digest).await {
        warn!(
            ?digest,
            ?discard_err,
            "WriteChunked: discard_chunked after dispatch Err failed; partial may persist \
             until next FilesystemStore::new sweep (#213 d-s-r MAJOR-1 best-effort GC)"
        );
    }
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

// =============================================================================
// Phase 2.7 — shared dispatch helper for WriteChunked (RPC) AND
// Bazel-facing internal chunking.
// =============================================================================

/// A chunk that has already passed per-chunk SHA-256 verification and
/// shape validation; ready for global-budget admission + per-blob mpsc
/// `try_send`. Producer responsibility for the SHA-256: the
/// `WriteChunked` RPC verifies the wire-supplied hash; the Bazel-facing
/// internal-chunking path computes it on `spawn_blocking` (#213 NMA1)
/// from the bytes itself.
#[derive(Debug)]
pub struct PreparedChunk {
    pub chunk_offset: u64,
    pub chunk_bytes: Bytes,
    pub chunk_sha256: [u8; 32],
    pub finish: bool,
}

/// Outcome of a chunked-dispatch session.
///
/// `committed_size` is the post-commit byte count for `Synchronous` mode
/// and the producer-declared `digest.size_bytes()` for `AsyncCommit`
/// (the actual commit may not have happened yet).
#[derive(Debug)]
pub struct DispatchOutcome {
    pub committed_size: u64,
}

/// Selects whether `dispatch_chunks_to_driver` waits for the commit
/// before returning.
///
/// - **`Synchronous`** (option α — Phase 2.2 `WriteChunked` RPC):
///   wait for the driver to finish commit + e2e SHA-256 verify, return
///   the actual committed size (or Err on commit failure). The producer
///   IS the slow-tier writer, so the latency lives at the natural
///   place — there is no upstream to back-pressure.
///
/// - **`AsyncCommit`** (option β — Phase 2.7 Bazel-facing internal
///   chunking): return Ok as soon as the final chunk is admitted to
///   the per-blob mpsc. The driver continues on its own task; the
///   in-flight map keeps the `Arc<ChunkedDriver>` alive until commit
///   completes. The fast-tier write that already happened in
///   `FastSlowStore::update` is the in-memory replica that satisfies
///   the ≥2-replica invariant. **THIS IS THE ANTI-#203 INVARIANT** —
///   blocking the Bazel-facing handler on slow-tier latency is the
///   exact mechanism the 2026-04-28 OOM cascade exhibited; CLAUDE.md
///   `feedback_async_to_sync_requires_explicit_signoff` requires
///   explicit user sign-off before flipping the kill-switch on.
#[derive(Debug, Clone, Copy)]
pub enum CommitMode {
    Synchronous,
    AsyncCommit,
}

/// Admit a `PreparedChunk` (sha256 already checked) to the per-blob
/// driver. Mirror of `ChunkedWriteHandler::admit_chunk` minus the
/// per-chunk SHA-256 verify (the caller has already done it). All the
/// shape-validation + reverse-release accounting still applies.
///
/// `chunk_size` must equal the producer's chunk size; it is used for
/// alignment + size validation (see §13.1.1).
///
/// `pin_budget` (Some): #212 fixup B1. The Bazel-facing internal-
/// chunking path passes the global `PinBudget` singleton so the
/// post-arrival in-memory pin is globally byte-capped. On exhaustion the
/// admission is rejected with `Code::ResourceExhausted` +
/// `BackpressureSignal::PinnedBytesExhausted` — the SAME admission gate
/// shape as `ChunkBudget` exhaustion, just for a different memory pool.
/// The legacy `WriteChunked` RPC path passes `None` (the producer is the
/// worker, not Bazel; the worker's own admission already enforces
/// per-process bounds).
pub fn admit_prepared_chunk(
    chunk: PreparedChunk,
    sender: &mpsc::Sender<ChunkWork>,
    chunk_budget: &'static ChunkBudget,
    pin_budget: Option<&'static PinBudget>,
    chunk_size: usize,
    stream_digest: DigestInfo,
    metrics: &ChunkedWriteHandlerMetrics,
) -> Result<(), Error> {
    let PreparedChunk {
        chunk_offset,
        chunk_bytes,
        chunk_sha256,
        finish,
    } = chunk;

    // Shape validation. Mirrors the production check in
    // `ChunkedWriteHandler::admit_chunk` (M-code-3) — the Bazel-facing
    // internal chunking path SHOULD always produce shape-correct chunks
    // (we generate them here), but defense in depth catches arithmetic
    // bugs in the chunker.
    let chunk_size_u64 = chunk_size as u64;
    if !chunk_offset.is_multiple_of(chunk_size_u64) {
        return Err(make_input_err!(
            "PreparedChunk.chunk_offset must be a multiple of CHUNK_SIZE ({} bytes); \
             got chunk_offset={chunk_offset} for digest {stream_digest}",
            chunk_size
        ));
    }
    let chunk_bytes_len = chunk_bytes.len();
    if !finish && chunk_bytes_len != chunk_size {
        return Err(make_input_err!(
            "PreparedChunk.chunk_bytes.len() must equal CHUNK_SIZE ({}) for non-final chunks; \
             got {chunk_bytes_len} for digest {stream_digest} at offset {chunk_offset}",
            chunk_size
        ));
    }
    if finish && chunk_bytes_len > chunk_size {
        return Err(make_input_err!(
            "PreparedChunk.chunk_bytes.len() must be <= CHUNK_SIZE ({}) for the final chunk; \
             got {chunk_bytes_len} for digest {stream_digest} at offset {chunk_offset}",
            chunk_size
        ));
    }
    if finish {
        let declared_size = stream_digest.size_bytes();
        let chunk_end = chunk_offset.saturating_add(chunk_bytes_len as u64);
        if chunk_end != declared_size {
            return Err(make_input_err!(
                "final PreparedChunk.chunk_offset + chunk_bytes.len() must equal \
                 digest.size_bytes ({declared_size}); got {chunk_end} for digest \
                 {stream_digest} at offset {chunk_offset} with len {chunk_bytes_len}"
            ));
        }
    }

    // Global-budget try_acquire (per §13.1.1 step 1).
    let permit = match chunk_budget.try_acquire_chunk() {
        Some(p) => p,
        None => {
            metrics
                .global_budget_exhausted_rejections_total
                .fetch_add(1, Ordering::Relaxed);
            let detail = encode_backpressure_signal_any(
                backpressure_signal::Reason::GlobalChunkBudgetExhausted,
                GLOBAL_BUDGET_RETRY_AFTER_MS,
            );
            return Err(Error::resource_exhausted_backpressure(
                format!(
                    "chunked dispatch: global ChunkBudget exhausted (digest {stream_digest}, offset {chunk_offset})"
                ),
                detail,
            ));
        }
    };

    // #212 fixup B1: PinBudget try_acquire (post-arrival pinned-bytes
    // cap; anti-#203). On exhaustion the chunk_budget permit is dropped
    // first via reverse-release so we don't leak the in-flight
    // budget on this rejection. Bazel-facing path passes Some(...);
    // legacy WriteChunked path (worker-driven) passes None and skips.
    let pin_permit = if let Some(pb) = pin_budget {
        match pb.try_acquire(chunk_bytes_len) {
            Some(p) => Some(p),
            None => {
                drop(permit);
                metrics
                    .pin_budget_exhausted_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
                let detail = encode_backpressure_signal_any(
                    backpressure_signal::Reason::PinnedBytesExhausted,
                    PIN_BUDGET_RETRY_AFTER_MS,
                );
                return Err(Error::resource_exhausted_backpressure(
                    format!(
                        "chunked dispatch: global PinBudget exhausted (digest {stream_digest}, \
                         offset {chunk_offset}, requested_bytes {chunk_bytes_len})"
                    ),
                    detail,
                ));
            }
        }
    } else {
        None
    };

    // try_send into the per-blob mpsc (per §13.1.1 step 2).
    let work = ChunkWork {
        chunk_offset,
        chunk_bytes,
        chunk_sha256,
        finish,
        _permit: permit,
        _pin_permit: pin_permit,
    };
    match sender.try_send(work) {
        Ok(()) => {
            metrics
                .chunks_admitted_total
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(returned)) => {
            // Drop releases the permit (reverse-release).
            drop(returned);
            metrics
                .mpsc_full_rejections_total
                .fetch_add(1, Ordering::Relaxed);
            let detail = encode_backpressure_signal_any(
                backpressure_signal::Reason::PerBlobMpscFull,
                PER_BLOB_MPSC_RETRY_AFTER_MS,
            );
            Err(Error::resource_exhausted_backpressure(
                format!(
                    "chunked dispatch: per-blob mpsc full (digest {stream_digest}, offset {chunk_offset})"
                ),
                detail,
            ))
        }
        Err(mpsc::error::TrySendError::Closed(returned)) => {
            // #213 testing-czar M4 / design §13.1.1 step 2 Err(Closed):
            // the per-blob driver task has terminated (panic, abort, or
            // happy-path exit raced with an admission). Drop the
            // ChunkWork — its OwnedSemaphorePermit returns to the
            // global ChunkBudget via `Drop` (reverse-release) and the
            // optional PinBudget permit returns the same way. Wire
            // status: `Code::Aborted` per spec — distinct from
            // `Code::ResourceExhausted` (admission backpressure) so the
            // classifier-tightened `looks_like_dead_channel` does not
            // treat this as a stale h2 channel; the producer should
            // start a fresh stream rather than retry on the same
            // session.
            drop(returned);
            Err(make_err!(
                Code::Aborted,
                "chunked dispatch: per-blob driver task gone (closed before finish_chunk) for digest {stream_digest} offset {chunk_offset}; restart the stream"
            ))
        }
    }
}

/// Shared dispatch helper for both the WriteChunked RPC and the Bazel-
/// facing internal-chunking path. Owns:
///   1. In-flight driver lookup / spawn (rejecting concurrent same-
///      digest streams with `Code::Aborted` + `BackpressureSignal`).
///   2. Per-chunk admission via `admit_prepared_chunk`.
///   3. Sender-drop + entry-cleanup ordering (M-perf-3 + B1 coupling).
///   4. `Synchronous` vs `AsyncCommit` post-admit behaviour.
///
/// Shutdown / cancellation safety:
/// - The in-flight map's `Arc<ChunkedDriver>` keeps the spawned task
///   alive even if the caller's future is cancelled mid-dispatch.
/// - On any error path that happens BEFORE all chunks are admitted,
///   the in-flight entry is removed (the driver hasn't seen `finish`,
///   so its mpsc-recv loop will exit cleanly when the sender drops).
pub async fn dispatch_chunks_to_driver<Fe: FileEntry>(
    filesystem_store: Arc<FilesystemStore<Fe>>,
    in_flight: Arc<ChunkedWriteInFlight>,
    chunk_budget: &'static ChunkBudget,
    pin_budget: Option<&'static PinBudget>,
    // #212 fixup S1: optional ChunkedReadRegistry. When `Some`, the
    // per-blob `Arc<ChunkedDriver>` is registered before chunk admission
    // and deregistered when the dispatch terminates (sync drop or async
    // reaper). With this wired, Phase 2.5's read cascade can find the
    // in-flight driver and serve bytes from the in-memory pin during
    // the async-commit window. Without it, the cascade falls through
    // to the slow store as if no chunked path existed.
    chunked_read_registry: Option<
        Arc<nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry>,
    >,
    chunk_size: usize,
    digest: DigestInfo,
    chunks: Pin<Box<dyn Stream<Item = Result<PreparedChunk, Error>> + Send>>,
    commit_mode: CommitMode,
    metrics: Arc<ChunkedWriteHandlerMetrics>,
) -> Result<DispatchOutcome, Error> {
    let stream_digest = digest;

    // Insert / reject-on-conflict in the in-flight map.
    let (sender, driver) = {
        let mut guard = in_flight.inner.lock();
        if guard.contains_key(&digest) {
            metrics
                .concurrent_same_digest_rejections_total
                .fetch_add(1, Ordering::Relaxed);
            let detail = encode_backpressure_signal_any(
                backpressure_signal::Reason::PerBlobMpscFull,
                CONCURRENT_SAME_DIGEST_RETRY_AFTER_MS,
            );
            return Err(Error::aborted_with_detail(
                format!(
                    "chunked dispatch: another stream is already writing digest {digest}; \
                     retry after a backoff (concurrent-stream rejection, NOT durable commit)"
                ),
                detail,
            ));
        }
        let (driver, sender) = ChunkedDriver::spawn_driver(
            Arc::clone(&filesystem_store),
            digest,
            digest.size_bytes(),
            chunk_size,
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

    // #212 fixup S1: register the per-blob driver with the read-cascade
    // registry (if one is wired). Phase 2.5's `FastSlowStore::get_part`
    // consults this registry to find in-flight drivers and serve their
    // pinned bytes. Lifetime: the entry survives until the dispatch
    // completes (sync drop OR async reaper), at which point we
    // explicitly deregister.
    if let Some(reg) = chunked_read_registry.as_ref() {
        // Programmer-bug-detector: if the registry already had an entry
        // for this digest, we'd be racing with another in-flight stream
        // — but the in_flight check above already rejected concurrent
        // streams with `Code::Aborted`. So the previous-entry case here
        // is a logic bug; `register` warns on prev != None.
        let _prev = reg.register(digest, Arc::clone(&driver));
    }

    // Cleanup guard: removes the in-flight entry on any panic or early
    // exit. We `core::mem::forget` it on the happy path after explicit
    // cleanup so we don't double-remove.
    let cleanup_guard = InFlightCleanup {
        in_flight: Arc::clone(&in_flight),
        digest,
        chunked_read_registry: chunked_read_registry.clone(),
    };

    // Pin the chunks stream so we can iterate it inline.
    let mut chunks = chunks;

    // Admit chunks one-by-one. The chunk stream itself is responsible
    // for shape (the WriteChunked RPC stream pulls from
    // `tonic::Streaming`; the Bazel-facing path pulls from a
    // `DropCloserReadHalf`-fed batcher).
    while let Some(chunk_result) = chunks.next().await {
        let chunk = match chunk_result {
            Ok(c) => c,
            Err(err) => {
                // #213 d-s-r MAJOR-1 fixup: explicit GC trigger on
                // update() Err. Per §6.7 "On upstream client drop
                // mid-update()", the partial file on disk would
                // otherwise persist until next FilesystemStore::new
                // (Q7=(c) restart-only sweep). Eagerly discard now so
                // the partial does NOT accumulate on long-running
                // servers under sustained client-disconnect storms
                // (which would otherwise degrade the chunk_budget
                // monotonically until restart). Best-effort: discard
                // errors are logged but do NOT mask the original
                // upstream error.
                discard_partial_best_effort(&filesystem_store, &stream_digest).await;
                drop(cleanup_guard);
                return Err(err);
            }
        };
        if let Err(err) = admit_prepared_chunk(
            chunk,
            &sender,
            chunk_budget,
            pin_budget,
            chunk_size,
            stream_digest,
            &metrics,
        ) {
            // #213 d-s-r MAJOR-1 fixup: same eager-GC trigger as above.
            discard_partial_best_effort(&filesystem_store, &stream_digest).await;
            drop(cleanup_guard);
            return Err(err);
        }
    }

    // M-perf-3 + B1 coupling: drop EVERY sender clone (local + the one
    // stored in InFlightEntry) so the driver's recv loop sees None and
    // proceeds to commit. The driver Arc remains in the map until we
    // explicitly remove it post-commit (Synchronous) or until the
    // driver completes on its own task (AsyncCommit — which removes it
    // from the map via... actually no, let me explain below).
    drop(sender);
    {
        let mut guard = in_flight.inner.lock();
        if let Some(entry) = guard.get_mut(&stream_digest) {
            let (placeholder_tx, _placeholder_rx) =
                mpsc::channel::<ChunkWork>(1);
            let stored_sender =
                core::mem::replace(&mut entry.sender, placeholder_tx);
            drop(stored_sender);
            drop(_placeholder_rx);
        }
    }

    match commit_mode {
        CommitMode::Synchronous => {
            // Wait for commit + e2e SHA-256 verify.
            let commit_result = driver.await_completion().await;

            // Remove the in-flight entry now that the driver has
            // signaled completion. The cleanup_guard would also do
            // this; we forget it because we explicitly removed.
            let removed_entry = in_flight.inner.lock().remove(&stream_digest);
            drop(removed_entry);
            // #212 fixup S1: deregister from the read-cascade registry
            // (if wired). The driver's pin is already cleared on its
            // own task exit, but a stale registry entry would surface
            // as a `pin_partial_misses_total` increment on every
            // subsequent read; deregister keeps the cascade-step-2
            // miss/partial-miss counters honest.
            if let Some(reg) = chunked_read_registry.as_ref() {
                let _ = reg.deregister(&stream_digest);
            }
            core::mem::forget(cleanup_guard);

            let commit_result = match commit_result {
                Ok(r) => r,
                Err(err) => {
                    metrics
                        .commit_failures_total
                        .fetch_add(1, Ordering::Relaxed);
                    if err.code == Code::InvalidArgument
                        && err
                            .message_string()
                            .contains("end-to-end SHA-256 mismatch")
                    {
                        metrics
                            .sha256_e2e_mismatches_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(err);
                }
            };

            metrics
                .chunks_committed_total
                .fetch_add(1, Ordering::Relaxed);
            info!(
                ?stream_digest,
                committed_size = commit_result.committed_size,
                mode = "synchronous",
                "chunked dispatch: blob committed"
            );

            Ok(DispatchOutcome {
                committed_size: commit_result.committed_size,
            })
        }
        CommitMode::AsyncCommit => {
            // Anti-#203 (β): return Ok as soon as admission is done.
            // The driver's `Arc<ChunkedDriver>` is still in the
            // in-flight map (entry.driver), so the spawned task stays
            // alive even though our local `driver` Arc drops below.
            //
            // We MUST NOT forget the cleanup_guard — the driver will
            // remove its OWN map entry when the commit completes, but
            // for that to work the driver task must run to completion.
            // Solution: spawn a small reaper task that awaits the
            // driver's completion (which is `oneshot::Receiver::await`
            // under the hood) and removes the entry. This decouples
            // entry removal from the upstream RPC future.
            //
            // The cleanup_guard's forget is correct here too because
            // the reaper handles removal — keeping the guard would
            // race with the reaper and double-remove (harmless but
            // noisy in logs).
            core::mem::forget(cleanup_guard);

            let in_flight_for_reaper = Arc::clone(&in_flight);
            let metrics_for_reaper = Arc::clone(&metrics);
            let driver_for_reaper = Arc::clone(&driver);
            // #212 fixup S1: clone the optional registry Arc into the
            // reaper. The reaper deregisters on commit completion
            // (success OR failure) so Phase 2.5's cascade step 2 stops
            // consulting a driver whose pin is already cleared.
            let reg_for_reaper = chunked_read_registry.clone();
            // Drop our local `driver` Arc — the reaper holds its own
            // strong ref and the in-flight entry holds another. The
            // explicit `drop(driver)` here documents that we transfer
            // ownership to the reaper.
            drop(driver);
            // `filesystem_store` was consumed by `spawn_driver` above (it
            // lives inside the `Arc<ChunkedDriver>`); nothing for us to
            // do with it here.
            tokio::spawn(async move {
                let commit_result = driver_for_reaper.await_completion().await;
                let removed_entry =
                    in_flight_for_reaper.inner.lock().remove(&stream_digest);
                drop(removed_entry);
                if let Some(reg) = reg_for_reaper.as_ref() {
                    let _ = reg.deregister(&stream_digest);
                }
                match commit_result {
                    Ok(r) => {
                        metrics_for_reaper
                            .chunks_committed_total
                            .fetch_add(1, Ordering::Relaxed);
                        info!(
                            ?stream_digest,
                            committed_size = r.committed_size,
                            mode = "async",
                            "chunked dispatch: blob committed (Bazel-facing reaper)"
                        );
                    }
                    Err(err) => {
                        metrics_for_reaper
                            .commit_failures_total
                            .fetch_add(1, Ordering::Relaxed);
                        if err.code == Code::InvalidArgument
                            && err
                                .message_string()
                                .contains("end-to-end SHA-256 mismatch")
                        {
                            metrics_for_reaper
                                .sha256_e2e_mismatches_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        warn!(
                            ?stream_digest,
                            ?err,
                            mode = "async",
                            "chunked dispatch: async-commit FAILED \
                             (Bazel-facing); blob is NOT durable on slow tier — \
                             upstream's fast-tier write is the only in-memory \
                             replica until mirror re-uploads"
                        );
                    }
                }
            });

            Ok(DispatchOutcome {
                committed_size: stream_digest.size_bytes(),
            })
        }
    }
}

/// Production implementation of the
/// `nativelink_store::chunked::BazelChunkedDispatcher` trait. Owns
/// shared references to the FilesystemStore (slow tier), in-flight
/// tracker, chunk budget, and metrics; on `dispatch()` invokes
/// `dispatch_bazel_facing_internal_chunking` with `CommitMode::AsyncCommit`.
///
/// Production wiring (Phase 2.7 deployment): construct one instance
/// per server, install on the FastSlowStore via
/// `set_bazel_chunked_dispatcher`. Toggle production behaviour with
/// `nativelink_store::chunked::set_bazel_facing_internal_chunking_enabled`.
///
/// (β) async-commit mandatory; the dispatch returns Ok as soon as
/// admission is complete, NOT after on-disk commit.
#[derive(Debug)]
pub struct BazelChunkedDispatcherImpl<Fe: FileEntry = FileEntryImpl> {
    filesystem_store: Arc<FilesystemStore<Fe>>,
    in_flight: Arc<ChunkedWriteInFlight>,
    chunk_budget: &'static ChunkBudget,
    /// #212 fixup B1: global PinBudget. Anti-#203 — caps the post-arrival
    /// in-memory pin bytes globally so a slow-tier pause cannot inflate
    /// pinned-memory unboundedly.
    pin_budget: &'static PinBudget,
    /// #212 fixup S1 (B2 + dead-wire): registry consulted by
    /// `FastSlowStore::get_part`'s read cascade. The dispatcher
    /// `register`s the per-blob `Arc<ChunkedDriver>` on dispatch entry
    /// and `deregister`s on dispatch completion (success OR failure)
    /// — wiring Phase 2.5's read-cascade step 2 with Phase 2.7's write
    /// path. `None` = no registration (tests that only want write
    /// behavior; production startup MUST install via the
    /// `_with_registry_for_test` constructor + the `with_registry`
    /// builder method).
    chunked_read_registry:
        Option<Arc<nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry>>,
    /// #212 fixup B2: chunked-path in-flight digest set shared with the
    /// FastSlowStore. The dispatcher inserts the digest on dispatch
    /// entry and removes on dispatch completion — preserving the
    /// `has_with_results` / #210 graceful-shutdown drain contracts for
    /// chunked-path blobs. SEPARATE from the legacy
    /// `in_flight_slow_writes` map (which stores `Vec<Bytes>` of the
    /// actual chunk bytes; chunked writes track bytes via the
    /// chunked-driver pin instead). `None` = no in-flight registration
    /// (tests that don't care about these contracts).
    chunked_in_flight_digests: Option<
        Arc<parking_lot::Mutex<std::collections::HashSet<DigestInfo>>>,
    >,
    /// #212 fixup B2: notify shared with the FastSlowStore so the
    /// graceful-drain in `flush_slow_writes` wakes when in-flight goes
    /// to zero (canonical lost-wakeup pattern: `Notify::notified()` AFTER
    /// the predicate evaluation; the dispatcher drives the producer
    /// side of that contract).
    in_flight_empty_notify: Option<Arc<tokio::sync::Notify>>,
    chunk_size: usize,
    metrics: Arc<ChunkedWriteHandlerMetrics>,
}

impl<Fe: FileEntry> BazelChunkedDispatcherImpl<Fe> {
    /// Construct a production-grade dispatcher pointed at the shared
    /// in-flight tracker + chunk-budget singleton. The `filesystem_store`
    /// is the slow tier of the FastSlowStore that will install this
    /// dispatcher; the `chunk_size` is the production CHUNK_SIZE.
    ///
    /// **Production wiring requires** chaining one or both builder
    /// methods after `new`:
    ///   - [`Self::with_registry`] — wires the read-side cascade
    ///     (Phase 2.5 ↔ Phase 2.7 dead-wire fixup S1).
    ///   - [`Self::with_in_flight_tracking`] — wires
    ///     in_flight_slow_writes registration so `has_with_results` /
    ///     #210 graceful-shutdown drain / read-cascade step 1 see
    ///     chunked-path blobs (B2).
    ///
    /// Without those wires, the dispatcher works but Phase 2.5 reads
    /// fall through to the slow store and graceful drain returns early.
    #[must_use]
    pub fn new(filesystem_store: Arc<FilesystemStore<Fe>>) -> Self {
        Self {
            filesystem_store,
            in_flight: ChunkedWriteInFlight::new(),
            chunk_budget: nativelink_store::chunked::chunk_budget::chunk_budget_singleton(),
            pin_budget: pin_budget_singleton(),
            chunked_read_registry: None,
            chunked_in_flight_digests: None,
            in_flight_empty_notify: None,
            chunk_size: CHUNK_SIZE,
            metrics: Arc::new(ChunkedWriteHandlerMetrics::default()),
        }
    }

    /// #212 fixup S1: wire the dispatcher to a `ChunkedReadRegistry`.
    /// Production startup builds ONE registry, installs it on the
    /// FastSlowStore via `set_chunked_read_registry`, and wires it into
    /// THIS dispatcher via this builder method. From that point, every
    /// `dispatch` call registers the per-blob `Arc<ChunkedDriver>` for
    /// the duration of the dispatch — Phase 2.5's read cascade can now
    /// find the in-flight driver and serve the bytes from the in-memory
    /// pin during the async-commit window.
    #[must_use]
    pub fn with_registry(
        mut self,
        registry: Arc<nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry>,
    ) -> Self {
        self.chunked_read_registry = Some(registry);
        self
    }

    /// #212 fixup B2: wire the dispatcher to the FastSlowStore's
    /// `chunked_in_flight_digests` set + `in_flight_empty_notify`. From
    /// that point, every `dispatch` inserts the digest in the set for
    /// the duration of the dispatch. On dispatch completion the digest
    /// is removed and the notify fires when the set empties (waking
    /// any `flush_slow_writes` waiter). Preserves the
    /// `has_with_results` + #210 graceful-shutdown drain contracts for
    /// chunked-path blobs.
    #[must_use]
    pub fn with_in_flight_tracking(
        mut self,
        chunked_in_flight_digests: Arc<
            parking_lot::Mutex<std::collections::HashSet<DigestInfo>>,
        >,
        in_flight_empty_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.chunked_in_flight_digests = Some(chunked_in_flight_digests);
        self.in_flight_empty_notify = Some(in_flight_empty_notify);
        self
    }

    /// Construct a dispatcher with externally-supplied state. Used by
    /// tests so each test can have its own in-flight tracker + chunk
    /// budget + metrics + chunk size (smaller chunks make tests faster).
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn new_with_state_for_test(
        filesystem_store: Arc<FilesystemStore<Fe>>,
        in_flight: Arc<ChunkedWriteInFlight>,
        chunk_budget: &'static ChunkBudget,
        chunk_size: usize,
    ) -> Self {
        Self {
            filesystem_store,
            in_flight,
            chunk_budget,
            pin_budget: pin_budget_singleton(),
            chunked_read_registry: None,
            chunked_in_flight_digests: None,
            in_flight_empty_notify: None,
            chunk_size,
            metrics: Arc::new(ChunkedWriteHandlerMetrics::default()),
        }
    }

    /// Like `new_with_state_for_test`, additionally accepting an
    /// externally-supplied PinBudget (so each test gets its own and we
    /// don't pollute the singleton). Used by the B1 cap-rejection
    /// regression test.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn new_with_state_and_pin_budget_for_test(
        filesystem_store: Arc<FilesystemStore<Fe>>,
        in_flight: Arc<ChunkedWriteInFlight>,
        chunk_budget: &'static ChunkBudget,
        pin_budget: &'static PinBudget,
        chunk_size: usize,
    ) -> Self {
        Self {
            filesystem_store,
            in_flight,
            chunk_budget,
            pin_budget,
            chunked_read_registry: None,
            chunked_in_flight_digests: None,
            in_flight_empty_notify: None,
            chunk_size,
            metrics: Arc::new(ChunkedWriteHandlerMetrics::default()),
        }
    }

    /// Read-only accessor on the in-flight tracker. Tests use this to
    /// observe driver lifecycle (e.g. assert the entry persists during
    /// async-commit and drains after).
    #[must_use]
    pub fn in_flight(&self) -> &Arc<ChunkedWriteInFlight> {
        &self.in_flight
    }

    /// Read-only accessor on the metrics. Tests + production scrape.
    #[must_use]
    pub fn metrics(&self) -> &Arc<ChunkedWriteHandlerMetrics> {
        &self.metrics
    }
}

#[async_trait::async_trait]
impl<Fe: FileEntry> nativelink_store::chunked::BazelChunkedDispatcher
    for BazelChunkedDispatcherImpl<Fe>
{
    async fn dispatch(
        &self,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
    ) -> Result<u64, Error> {
        // #212 fixup B2: register the digest in the FastSlowStore's
        // chunked_in_flight_digests set BEFORE dispatch. Preserves
        // these contracts for chunked-path blobs:
        //   - has_with_results: chunked check returns Some(size_bytes).
        //   - flush_slow_writes (#210 graceful drain): waits for the
        //     digest set to drain before returning.
        // The chunked digest set is intentionally separate from the
        // legacy `in_flight_slow_writes` (`Vec<Bytes>` shape) because
        // chunked-path bytes are tracked via the chunked-driver pin
        // (registered through ChunkedReadRegistry for cascade step 2).
        if let Some(set) = self.chunked_in_flight_digests.as_ref() {
            set.lock().insert(digest);
        }
        let dispatch_res = dispatch_bazel_facing_internal_chunking(
            Arc::clone(&self.filesystem_store),
            Arc::clone(&self.in_flight),
            self.chunk_budget,
            Some(self.pin_budget),
            self.chunked_read_registry.clone(),
            Arc::clone(&self.metrics),
            self.chunk_size,
            digest,
            reader,
        )
        .await;

        // Note: the chunked-driver reaper (CommitMode::AsyncCommit)
        // completes ASYNCHRONOUSLY after this returns. We need the
        // chunked_in_flight_digests entry to survive until commit
        // drains (otherwise B2's #210 graceful-drain contract is
        // violated: the drainer would return as soon as dispatch
        // returns Ok, before commit lands). Solution: spawn a small
        // reaper that waits on the chunked-driver's in_flight tracker
        // emptying for THIS digest, then removes the digest from the
        // set + notifies the empty-notify.
        //
        // On dispatch error (admission rejected before any chunk):
        // remove the digest now (no chunked driver was created or it
        // was torn down by the cleanup_guard).
        match (dispatch_res, self.chunked_in_flight_digests.clone()) {
            (Ok(outcome), Some(set)) => {
                let in_flight_for_reaper = Arc::clone(&self.in_flight);
                let notify = self.in_flight_empty_notify.clone();
                let dig = digest;
                tokio::spawn(async move {
                    loop {
                        if !in_flight_for_reaper.contains_digest(&dig) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    let mut guard = set.lock();
                    guard.remove(&dig);
                    let became_empty = guard.is_empty();
                    drop(guard);
                    if became_empty {
                        if let Some(n) = notify.as_ref() {
                            n.notify_waiters();
                        }
                    }
                });
                Ok(outcome.committed_size)
            }
            (Ok(outcome), None) => Ok(outcome.committed_size),
            (Err(err), Some(set)) => {
                let mut guard = set.lock();
                guard.remove(&digest);
                let became_empty = guard.is_empty();
                drop(guard);
                if became_empty {
                    if let Some(n) = self.in_flight_empty_notify.as_ref() {
                        n.notify_waiters();
                    }
                }
                Err(err)
            }
            (Err(err), None) => Err(err),
        }
    }
}

/// #212 fixup S1: production wiring helper. Constructs a fresh
/// `ChunkedReadRegistry` + `BazelChunkedDispatcherImpl`, wires both
/// into the supplied `FastSlowStore` (via `set_chunked_read_registry`
/// + `set_bazel_chunked_dispatcher`), and additionally hooks the
/// dispatcher into the FastSlowStore's `in_flight_slow_writes` map +
/// `in_flight_empty_notify` so chunked-path blobs are visible to
/// `has_with_results` / #210 graceful drain / read-cascade step 1.
///
/// Returns `Arc<BazelChunkedDispatcherImpl>` so the caller can keep a
/// strong handle (e.g. for metrics scraping). The dispatcher is also
/// stored inside the FastSlowStore via `set_bazel_chunked_dispatcher`,
/// which holds another `Arc<dyn BazelChunkedDispatcher>` reference, so
/// the strong handle is optional — drop it if the metrics aren't
/// needed.
///
/// **Kill-switches still default OFF.** This wiring is the load-bearing
/// pre-flight for Phase 2.5 + Phase 2.7 to be ENGAGEABLE; flipping
/// either kill-switch on (`set_bazel_facing_internal_chunking_enabled(true)`
/// for the write side, `FastSlowStore::enable_chunked_reads()` for the
/// read side) requires explicit user sign-off per the architectural-
/// change rule (CLAUDE.md `feedback_async_to_sync_requires_explicit_signoff`).
#[must_use]
pub fn wire_bazel_chunked_dispatcher<Fe: FileEntry>(
    fast_slow: &nativelink_store::fast_slow_store::FastSlowStore,
    slow_filesystem_store: Arc<FilesystemStore<Fe>>,
) -> Arc<BazelChunkedDispatcherImpl<Fe>> {
    let registry = nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry::new();
    let dispatcher = Arc::new(
        BazelChunkedDispatcherImpl::new(slow_filesystem_store)
            .with_registry(Arc::clone(&registry))
            .with_in_flight_tracking(
                fast_slow.chunked_in_flight_digests_handle(),
                fast_slow.in_flight_empty_notify_handle(),
            ),
    );
    let _prev = fast_slow.set_chunked_read_registry(Arc::clone(&registry));
    fast_slow
        .set_bazel_chunked_dispatcher(Arc::clone(&dispatcher) as Arc<dyn nativelink_store::chunked::BazelChunkedDispatcher>);
    debug!(
        "wire_bazel_chunked_dispatcher: installed registry + dispatcher; \
         kill-switches remain default OFF",
    );
    dispatcher
}

/// Phase 2.7 — Bazel-facing internal chunking.
///
/// Pulls bytes from `reader` (the `DropCloserReadHalf` end of the
/// FastSlowStore::update buf channel), batches into `chunk_size`-sized
/// chunks, computes per-chunk SHA-256 on `spawn_blocking` (#213 NMA1),
/// and dispatches into the shared `dispatch_chunks_to_driver` helper in
/// `CommitMode::AsyncCommit` mode (anti-#203).
///
/// Returns Ok as soon as the final chunk is admitted to the per-blob
/// mpsc (NOT after commit). The driver continues in the background;
/// the in-flight map's `Arc<ChunkedDriver>` keeps it alive.
///
/// **Caller contract:** the upstream `FastSlowStore::update` MUST have
/// already written the bytes to the fast tier (MemoryStore) BEFORE
/// invoking this dispatch — that fast-tier write is the in-memory
/// replica that satisfies the ≥2-replica invariant during the
/// async-commit window. If the fast-tier write is skipped, async-commit
/// produces a single-replica window between admission and disk landing,
/// violating durability. (FastSlowStore::update's existing
/// `tokio::join!(data_stream_fut, fast_store_fut)` provides this
/// ordering.)
pub async fn dispatch_bazel_facing_internal_chunking<Fe: FileEntry>(
    filesystem_store: Arc<FilesystemStore<Fe>>,
    in_flight: Arc<ChunkedWriteInFlight>,
    chunk_budget: &'static ChunkBudget,
    pin_budget: Option<&'static PinBudget>,
    chunked_read_registry: Option<
        Arc<nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry>,
    >,
    metrics: Arc<ChunkedWriteHandlerMetrics>,
    chunk_size: usize,
    digest: DigestInfo,
    reader: DropCloserReadHalf,
) -> Result<DispatchOutcome, Error> {
    debug!(
        ?digest,
        chunk_size,
        size = digest.size_bytes(),
        "bazel-facing internal chunking dispatch: start"
    );

    let chunks_stream = build_bazel_chunk_stream(reader, chunk_size, digest);
    dispatch_chunks_to_driver(
        filesystem_store,
        in_flight,
        chunk_budget,
        pin_budget,
        chunked_read_registry,
        chunk_size,
        digest,
        chunks_stream,
        CommitMode::AsyncCommit,
        metrics,
    )
    .await
}

/// Build a stream of `PreparedChunk` from a `DropCloserReadHalf` of
/// raw bytes. Per-chunk SHA-256 is computed on `spawn_blocking`. The
/// final `PreparedChunk` carries `finish=true` and may be smaller than
/// `chunk_size` (the residual bytes of the blob).
///
/// On `reader.recv()` Err: yields a single Err and terminates.
/// On EOF before declared bytes consumed: yields an Err describing the
/// short read.
fn build_bazel_chunk_stream(
    reader: DropCloserReadHalf,
    chunk_size: usize,
    digest: DigestInfo,
) -> Pin<Box<dyn Stream<Item = Result<PreparedChunk, Error>> + Send>> {
    use bytes::BytesMut;

    /// State machine driven by `futures::stream::unfold`. Carries the
    /// reader, an accumulator buffer, and progress counters; emits one
    /// `PreparedChunk` per polled iteration until `Done`.
    enum State {
        Active {
            reader: DropCloserReadHalf,
            buf: BytesMut,
            chunk_offset: u64,
            bytes_consumed: u64,
        },
        Done,
    }

    let total_bytes = digest.size_bytes();
    let initial = State::Active {
        reader,
        buf: BytesMut::with_capacity(chunk_size),
        chunk_offset: 0,
        bytes_consumed: 0,
    };

    let stream = futures::stream::unfold(initial, move |state| async move {
        let State::Active {
            mut reader,
            mut buf,
            chunk_offset,
            bytes_consumed,
        } = state
        else {
            return None;
        };

        // Drain reader until either the chunk is full OR EOF.
        let eof = loop {
            if buf.len() >= chunk_size {
                break false;
            }
            match reader.recv().await {
                Ok(b) if b.is_empty() => break true,
                Ok(b) => buf.extend_from_slice(&b),
                Err(err) => {
                    return Some((
                        Err(err
                            .append("bazel-facing internal-chunking: reader.recv()")),
                        State::Done,
                    ));
                }
            }
        };

        if eof {
            let buffered = buf.len() as u64;
            if bytes_consumed + buffered != total_bytes {
                let err = make_input_err!(
                    "bazel-facing internal-chunking: short read for digest {digest}; \
                     consumed={bytes_consumed} buffered={buffered} declared={total_bytes}"
                );
                return Some((Err(err), State::Done));
            }
            if buf.is_empty() {
                if total_bytes == 0 {
                    let err = make_err!(
                        Code::Internal,
                        "bazel-facing internal-chunking: zero-byte blob must use \
                         empty-blob shortcut, not chunked dispatch (digest {digest})"
                    );
                    return Some((Err(err), State::Done));
                }
                // Reaching here without bytes means the previous
                // iteration already emitted finish=true (caller saw
                // None as terminator).
                return None;
            }
            let final_bytes = buf.split().freeze();
            let chunk_sha256 = match compute_sha256_blocking(final_bytes.clone()).await {
                Ok(v) => v,
                Err(err) => return Some((Err(err), State::Done)),
            };
            let item = PreparedChunk {
                chunk_offset,
                chunk_bytes: final_bytes,
                chunk_sha256,
                finish: true,
            };
            return Some((Ok(item), State::Done));
        }

        let chunk_bytes = buf.split_to(chunk_size).freeze();
        let chunk_len = chunk_bytes.len() as u64;
        let new_consumed = bytes_consumed + chunk_len;
        let is_finish = new_consumed == total_bytes;
        let chunk_sha256 = match compute_sha256_blocking(chunk_bytes.clone()).await {
            Ok(v) => v,
            Err(err) => return Some((Err(err), State::Done)),
        };
        if is_finish {
            // Defensive: drain residual reader bytes; producing
            // anything past declared size is a protocol violation.
            match reader.recv().await {
                Ok(b) if b.is_empty() => {}
                Ok(_) => {
                    let err = make_input_err!(
                        "bazel-facing internal-chunking: producer sent bytes \
                         after declared size {total_bytes} for digest {digest}"
                    );
                    return Some((Err(err), State::Done));
                }
                Err(err) => {
                    return Some((
                        Err(err.append(
                            "bazel-facing internal-chunking: post-finish drain",
                        )),
                        State::Done,
                    ));
                }
            }
            let item = PreparedChunk {
                chunk_offset,
                chunk_bytes,
                chunk_sha256,
                finish: true,
            };
            return Some((Ok(item), State::Done));
        }

        let item = PreparedChunk {
            chunk_offset,
            chunk_bytes,
            chunk_sha256,
            finish: false,
        };
        let next_state = State::Active {
            reader,
            buf,
            chunk_offset: chunk_offset.saturating_add(chunk_size as u64),
            bytes_consumed: new_consumed,
        };
        Some((Ok(item), next_state))
    });
    Box::pin(stream)
}

/// Hash a single chunk on `spawn_blocking` using the process-wide
/// default digest hasher (BLAKE3 in production, SHA-256 in tests).
/// #228 fix: name preserved (`compute_sha256_blocking`) to avoid a
/// large rename diff, but the function now dispatches via
/// `DigestHasher` and produces a hash matching whatever
/// `default_digest_hasher_func()` returns. Per-blob override per REAPI
/// v2 `digest_function` can be threaded later.
async fn compute_sha256_blocking(bytes: Bytes) -> Result<[u8; 32], Error> {
    tokio::task::spawn_blocking(move || -> [u8; 32] {
        let mut h = default_digest_hasher_func().hasher();
        h.update(&bytes);
        let info = h.finalize_digest();
        **info.packed_hash()
    })
    .await
    .map_err(|join_err| {
        make_err!(
            Code::Internal,
            "spawn_blocking join error in bazel-facing per-chunk hash: {join_err:?}"
        )
    })
}
