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

//! #47 b1 Phase 2 — per-blob io_uring writev coalescer task.
//!
//! Design source of truth:
//! `.claude/audits/47-b1-chunked-coalescer-design-2026-06-03.md`.
//!
//! ## What this module is
//!
//! One [`writer_task`] per [`super::chunked_driver::ChunkedDriver`] is
//! spawned on the io_uring path (when
//! [`nativelink_util::fs::is_io_uring_available`] returns true). The
//! task drains a per-blob [`mpsc::Receiver<WriteJob>`], detects
//! contiguous-offset runs by walking a `BTreeMap`, and submits ONE
//! `IORING_OP_WRITEV` SQE per run. Up to `WRITE_PIPELINE_DEPTH` writev
//! ops are kept in-flight via `FuturesUnordered`.
//!
//! Pattern source: [`nativelink_util::fs::write_file_from_channel`] at
//! `fs.rs:702-1001`. fs.rs assumes strictly monotonic offsets
//! (`write_offset += total_len`). The chunked driver does NOT —
//! `chunked_driver.rs:370` doc says "the handler may admit chunks
//! out-of-order" and the test
//! `driver_out_of_order_chunks_then_finish_commits_successfully` at
//! `chunked_driver.rs:1710` exercises it. So this writer's coalescing
//! is offset-keyed via `BTreeMap<u64, _>`: contiguous runs collapse
//! into one writev; isolated/out-of-order offsets submit as
//! single-iovec writevs. **`coalesce_count = 1` is the expected
//! steady state** when arrivals are not contiguous — the load-bearing
//! win is the io_uring bypass of the spawn_blocking pool mutex, NOT
//! coalescing amortization (see design §1).
//!
//! ## Per CLAUDE.md
//!
//! - NO `fsync`/`fdatasync`/`O_SYNC`/`O_DIRECT` introduced anywhere.
//!   `IORING_OP_WRITEV` does not sync; durability remains
//!   mirror_blobs + BlobsInStableStorage ack.
//! - The `chunk_rx` mpsc carries owned [`bytes::Bytes`]; over-cap
//!   behavior is documented at the [`WRITE_PIPELINE_DEPTH`] constant
//!   below and the aggregate ceiling is the global
//!   [`super::chunk_budget::ChunkBudget`] semaphore (4 GiB at
//!   `chunk_budget.rs:52`).
//!
//! ## Phase status (2026-06-03)
//!
//! This module ships the **writer task body** + a [`pick_path`] unit-test
//! helper. The driver wire-up (run_driver branching on
//! `is_io_uring_available`, lazy-spawn of the writer task at first
//! non-zero-byte chunk arrival), the `ChunkInProgress::IoUringMarker`
//! variant in `chunked_filesystem.rs`, the `open_or_create_partial_marker`
//! helper, and the T1-T6 + T8 integration tests are deferred to a
//! follow-up session. See the design's Continuation TODO section.

#![cfg(feature = "chunked_fast_slow")]

use core::fmt::Debug;
use std::sync::OnceLock;

use tracing::info;

/// #47 b1 Phase 2 Step 4 / design §5 / I10: one-time activation probe.
///
/// Cached value of `is_io_uring_available()` after first observation
/// by [`emit_activation_probe_once`]. Emits `info!(io_uring_active = bool)`
/// at populate time so post-deploy operators can verify which path is
/// live without inferring from per-write probes.
static IO_URING_PATH_ACTIVE: OnceLock<bool> = OnceLock::new();

/// Idempotent activation probe — emits `info!` the first time a chunked
/// driver decides between Path A (io_uring) and Path B (spawn_blocking).
/// Subsequent calls with the same value are no-ops.
pub fn emit_activation_probe_once(io_uring_active: bool) {
    if IO_URING_PATH_ACTIVE.set(io_uring_active).is_ok() {
        info!(
            target: "nativelink_store::chunked",
            io_uring_active,
            "chunked writer path decision",
        );
    }
}

/// Decision for which write path to take per blob.
///
/// Returned by [`pick_path`]. Production callers consult
/// [`nativelink_util::fs::is_io_uring_available`] and then dispatch via
/// `pick_path(io_uring_available).await` so the same boolean drives both
/// the activation probe (design §5 / I10) and the per-driver branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePath {
    /// io_uring writev coalescer (this module's [`writer_task`]).
    IoUring,
    /// Existing spawn_blocking-per-chunk path via
    /// `chunked_filesystem::write_chunk_at_offset`. Used when the
    /// kernel does not support io_uring at runtime, or when the
    /// `io-uring` feature is compiled out / target_os != linux.
    SpawnBlocking,
}

/// Per design §10 Step 5 T7: pure function so the dispatch decision is
/// unit-testable without an env var or test-only hook.
#[inline]
#[must_use]
pub fn pick_path(io_uring_available: bool) -> WritePath {
    if io_uring_available {
        WritePath::IoUring
    } else {
        WritePath::SpawnBlocking
    }
}

// ---------------------------------------------------------------------
// io_uring writer task (Linux + feature gate)
// ---------------------------------------------------------------------

#[cfg(all(feature = "io-uring", target_os = "linux"))]
mod io_uring_impl {
    use core::time::Duration;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::FutureExt;
    use futures::stream::{FuturesUnordered, StreamExt};
    use nativelink_error::{Code, Error, make_err};
    use tokio::sync::mpsc;
    use tokio::sync::OwnedSemaphorePermit;
    use tracing::warn;

    /// CAPPED AT 1024 jobs per blob (≤ 1024 × 1 MiB = 1 GiB worst-case
    /// per blob): matches `fs.rs::WRITE_PIPELINE_DEPTH`. Over-cap
    /// behavior per blob: `chunk_tx.send().await` blocks the driver's
    /// recv loop, applying backpressure all the way back to the gRPC
    /// stream via `ChunkBudget` permit non-availability.
    ///
    /// Aggregate ceiling across ALL writer tasks is the
    /// `ChunkBudget` global semaphore (4096 permits × 1 MiB = 4 GiB) at
    /// `chunk_budget.rs:52` — see design §8 I9. The mpsc cap is the
    /// per-blob backpressure shape; the global semaphore is the hard
    /// ceiling. Per-blob mpsc cap (1024) × N concurrent blobs is NOT
    /// multiplicatively unbounded — bounded by the global cap.
    pub const WRITE_PIPELINE_DEPTH: usize = 1024;

    /// Coalescing target — once this many bytes are pending in the
    /// per-drain batch, submit the writev. Matches
    /// `fs.rs::COALESCE_TARGET = 1 MiB`, which covers ZFS recordsize up
    /// to 1M (the max on this deployment).
    pub const COALESCE_TARGET: usize = 1024 * 1024;

    /// Linux IOV_MAX, per `include/uapi/linux/uio.h` UIO_MAXIOV. A
    /// single writev SQE cannot exceed this many iovec entries.
    pub const IOV_MAX: usize = 1024;

    /// Fallback timeout for the drain-until-empty coalescing strategy.
    /// Mirrors `fs.rs::COALESCE_FALLBACK_TIMEOUT`. Only fires when the
    /// driver has a genuine gap; in the normal fast path `try_recv`
    /// drains all available jobs with zero wait.
    pub const COALESCE_FALLBACK_TIMEOUT: Duration = Duration::from_millis(1);

    /// One unit of work flowing from the driver to the writer task.
    ///
    /// The `_permit` is the [`super::super::chunk_budget::ChunkBudget`]
    /// admission permit. It is transferred OUT of the
    /// [`super::super::chunked_driver::ChunkWork`] (at driver send-site)
    /// and dropped ONLY by the writer task after the corresponding
    /// writev CQE is processed. This is the lifetime that satisfies
    /// design §8 I9 (aggregate channel-resident bytes ≤ 4 GiB).
    pub struct WriteJob {
        pub offset: u64,
        pub bytes: Bytes,
        /// See type-level doc above for permit-lifetime invariant.
        pub _permit: OwnedSemaphorePermit,
        /// Time at which the driver's `chunk_tx.send(...)` STARTED.
        /// Used by the writer's slow-write warn (design §5) to compute
        /// `enqueue_ms`.
        pub enqueue_time: Instant,
    }

    /// Result envelope returned by each in-flight writev future. The
    /// `_permits` Vec is moved into the future and dropped after the
    /// CQE is processed, releasing all chunk_budget permits for the
    /// chunks coalesced into this writev (design §6 S1 step 2 + §8 I9).
    struct WriteCompletion {
        total_len: usize,
        coalesce_count: usize,
        enqueue_time_earliest: Instant,
        submit_time: Instant,
        result: Result<usize, tokio_epoll_uring::Error<std::io::Error>>,
        _permits: Vec<OwnedSemaphorePermit>,
    }

    /// The writer task body. Spawned by the driver via `tokio::spawn`
    /// at first non-zero-byte chunk arrival. One per
    /// [`super::super::chunked_driver::ChunkedDriver`].
    ///
    /// Lifetime:
    /// - **Happy path:** driver drops `chunk_tx` → mpsc closes → writer
    ///   drains remaining jobs + flushes in-flight pipeline → returns
    ///   `Ok(())`.
    /// - **Per-chunk error:** writev CQE returns `Err`. Writer drains
    ///   `chunk_rx` synchronously to release permits, submits NO further
    ///   writev SQEs, returns the ORIGINAL writev error (no synthetic
    ///   replacement; design §6 S1 step 4).
    /// - **Cancellation:** driver dropped → `chunk_tx` dropped → mpsc
    ///   closes → writer drains remaining + returns `Ok(())`.
    ///
    /// The `fd_arc` is the partial temp file opened by the driver's
    /// `open_or_create_partial_marker` helper. The writer owns the only
    /// reference (besides whatever in-flight writev futures clone
    /// internally) and the fd closes when the task exits.
    pub async fn writer_task(
        fd_arc: Arc<std::fs::File>,
        mut chunk_rx: mpsc::Receiver<WriteJob>,
    ) -> Result<(), Error> {
        let system = tokio_epoll_uring::thread_local_system().await;

        let mut in_flight: FuturesUnordered<
            std::pin::Pin<Box<dyn std::future::Future<Output = WriteCompletion> + Send>>,
        > = FuturesUnordered::new();
        let mut first_error: Option<Error> = None;

        loop {
            // 1. Drain ready completions opportunistically (non-blocking).
            //    Mirrors fs.rs:846-854. Catches CQEs in the gap between
            //    `try_recv` drain phases without blocking the coalescer.
            loop {
                match in_flight.next().now_or_never() {
                    Some(Some(wc)) => {
                        if let Err(e) = process_completion(wc) {
                            if first_error.is_none() {
                                first_error = Some(e);
                            }
                        }
                    }
                    _ => break,
                }
            }

            // 2. If pipeline is full, block on next completion before
            //    accumulating more jobs.
            if in_flight.len() >= WRITE_PIPELINE_DEPTH {
                if let Some(wc) = in_flight.next().await {
                    if let Err(e) = process_completion(wc) {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            }

            // 3. Receive the next batch of WriteJobs.
            //    Coalescing accumulator: keyed by offset so contiguous
            //    runs can be detected by walking the BTreeMap.
            let mut pending: BTreeMap<u64, (Bytes, OwnedSemaphorePermit, Instant)> = BTreeMap::new();
            let mut pending_bytes: usize = 0;
            let mut hit_eof = false;

            // Blocking recv for the first job. EOF terminates the loop.
            let first = match chunk_rx.recv().await {
                Some(job) => job,
                None => {
                    hit_eof = true;
                    // No more jobs and pending is empty — break the
                    // outer loop after the in_flight drain below.
                    break;
                }
            };
            pending_bytes += first.bytes.len();
            pending.insert(first.offset, (first.bytes, first._permit, first.enqueue_time));

            // Drain-until-empty: pull all immediately available jobs.
            // If still under the coalesce target, do one short blocking
            // recv to catch in-transit jobs.
            while pending_bytes < COALESCE_TARGET && pending.len() < IOV_MAX {
                match chunk_rx.try_recv() {
                    Ok(job) => {
                        pending_bytes += job.bytes.len();
                        pending.insert(job.offset, (job.bytes, job._permit, job.enqueue_time));
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // One short blocking recv.
                        match tokio::time::timeout(
                            COALESCE_FALLBACK_TIMEOUT,
                            chunk_rx.recv(),
                        )
                        .await
                        {
                            Ok(Some(job)) => {
                                pending_bytes += job.bytes.len();
                                pending.insert(
                                    job.offset,
                                    (job.bytes, job._permit, job.enqueue_time),
                                );
                            }
                            Ok(None) => {
                                hit_eof = true;
                                break;
                            }
                            Err(_timeout) => break,
                        }
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        hit_eof = true;
                        break;
                    }
                }
            }

            // 4. If we already saw an error from an earlier completion,
            //    drain `pending` (drop permits) + STOP submitting new
            //    writevs. Design §6 S1 step 2-4: drain to release
            //    permits, do not submit further SQEs.
            if first_error.is_some() {
                // Drop the pending map → permits drop. Continue the
                // outer loop to keep draining chunk_rx until EOF.
                drop(pending);
                if hit_eof {
                    break;
                }
                continue;
            }

            // 5. Coalesce contiguous runs and submit one writev per run.
            while !pending.is_empty() {
                // Pop the lowest-offset entry as the run start.
                let (&start_offset, _) = pending.iter().next().expect("non-empty");
                let (start_bytes, start_permit, start_enqueue) = pending
                    .remove(&start_offset)
                    .expect("just observed");
                let mut run_offset = start_offset;
                let mut iovecs: Vec<libc::iovec> = Vec::new();
                let mut buffers: Vec<Bytes> = Vec::new();
                let mut permits: Vec<OwnedSemaphorePermit> = Vec::new();
                let mut earliest_enqueue = start_enqueue;
                let mut run_bytes = 0usize;

                // Push the start.
                iovecs.push(libc::iovec {
                    iov_base: start_bytes.as_ptr() as *mut libc::c_void,
                    iov_len: start_bytes.len(),
                });
                run_bytes += start_bytes.len();
                run_offset += start_bytes.len() as u64;
                buffers.push(start_bytes);
                permits.push(start_permit);

                // Extend the contiguous run.
                while iovecs.len() < IOV_MAX {
                    let Some((&next_off, _)) = pending.iter().next() else {
                        break;
                    };
                    if next_off != run_offset {
                        break;
                    }
                    let (next_bytes, next_permit, next_enqueue) = pending
                        .remove(&next_off)
                        .expect("just observed");
                    iovecs.push(libc::iovec {
                        iov_base: next_bytes.as_ptr() as *mut libc::c_void,
                        iov_len: next_bytes.len(),
                    });
                    run_bytes += next_bytes.len();
                    run_offset += next_bytes.len() as u64;
                    if next_enqueue < earliest_enqueue {
                        earliest_enqueue = next_enqueue;
                    }
                    buffers.push(next_bytes);
                    permits.push(next_permit);
                }

                let coalesce_count = iovecs.len();
                let submit_time = Instant::now();
                let write_fut = system.writev(
                    Arc::clone(&fd_arc),
                    start_offset,
                    iovecs,
                    buffers,
                );

                let total_len = run_bytes;
                in_flight.push(Box::pin(async move {
                    let (_fd, result) = write_fut.await;
                    WriteCompletion {
                        total_len,
                        coalesce_count,
                        enqueue_time_earliest: earliest_enqueue,
                        submit_time,
                        result,
                        _permits: permits,
                    }
                }));
            }

            if hit_eof {
                break;
            }
        }

        // 6. Drain all in-flight completions before returning.
        while let Some(wc) = in_flight.next().await {
            if let Err(e) = process_completion(wc) {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        // 7. If we entered post-error drain BEFORE EOF, finish draining
        //    chunk_rx now so all permits return to ChunkBudget. Design
        //    §6 S1 step 2 + design §9 T8 mutation target.
        if first_error.is_some() {
            while let Some(_job) = chunk_rx.recv().await {
                // _job drops here → permit returns to ChunkBudget.
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }
        Ok(())
    }

    /// Process one writev completion: extract bytes-written, emit a
    /// slow-write warn (design §5), and surface short-writes as errors
    /// (matches `fs.rs:811-818` semantics, design §8 I4).
    fn process_completion(wc: WriteCompletion) -> Result<(), Error> {
        let n = match wc.result {
            Ok(n) => n,
            Err(e) => return Err(uring_err_to_error(e, "chunked_writer writev")),
        };
        if n < wc.total_len {
            return Err(make_err!(
                Code::Internal,
                "io_uring partial writev: {n}/{} bytes (short write — \
                 CAS blob will be retried by FastSlowStore)",
                wc.total_len,
            ));
        }

        // Slow-write probe per design §5. Threshold matches
        // chunked_driver.rs:985's existing `back_edge_ms > 50` warn.
        let enqueue_ms = wc
            .submit_time
            .saturating_duration_since(wc.enqueue_time_earliest)
            .as_millis() as u64;
        let writev_ms = wc.submit_time.elapsed().as_millis() as u64;
        let submit_ms: u64 = 0; // coalesce-build cost is sub-ms; folded into enqueue_ms here.
        let total_inner_ms = enqueue_ms + submit_ms + writev_ms;
        if total_inner_ms > 50 {
            warn!(
                target: "nativelink_store::chunked",
                // NEW fields (design §5):
                enqueue_ms,
                submit_ms,
                writev_ms,
                coalesce_count = wc.coalesce_count,
                path = "io_uring",
                // LEGACY mapped fields (design §5 table):
                mutex_acquire_ms = 0u64,
                dispatch_ms = enqueue_ms + submit_ms,
                pwrite_ms = writev_ms,
                closure_to_resume_ms = 0u64,
                total_inner_ms,
                chunk_len = wc.total_len,
                "per-chunk back-edge decomposed (#449 inline-split → #47 writev)",
            );
        }
        Ok(())
    }

    /// Local copy of `fs.rs::uring_err` so this module is independent of
    /// nativelink-util internals. Kept minimal — chunked_writer only
    /// surfaces Internal-class errors here; `FastSlowStore` retries
    /// cover the user-facing semantics.
    fn uring_err_to_error(
        e: tokio_epoll_uring::Error<std::io::Error>,
        ctx: &str,
    ) -> Error {
        match e {
            tokio_epoll_uring::Error::Op(io_err) => {
                make_err!(Code::Internal, "io_uring {ctx}: {io_err:?}")
            }
            tokio_epoll_uring::Error::System(sys_err) => {
                make_err!(Code::Internal, "io_uring system error in {ctx}: {sys_err:?}")
            }
        }
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub use io_uring_impl::{
    COALESCE_TARGET, IOV_MAX, WRITE_PIPELINE_DEPTH, WriteJob, writer_task,
};

#[cfg(test)]
mod tests {
    use super::{WritePath, pick_path};

    /// T7 (per design §9): the dispatch decision is a pure function
    /// of `io_uring_available`. Two cases, no harness. Production
    /// callers consult [`nativelink_util::fs::is_io_uring_available`]
    /// and pass through here.
    #[test]
    fn b1_writev_pick_path_unit_true_returns_io_uring() {
        assert_eq!(pick_path(true), WritePath::IoUring);
    }

    #[test]
    fn b1_writev_pick_path_unit_false_returns_spawn_blocking() {
        assert_eq!(pick_path(false), WritePath::SpawnBlocking);
    }
}
