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
//! Wire-up, marker, helper, and integration tests T1 / T_multi / T_drop
//! / T2 / T4 / T5 / T6 / T7 / T8 are all landed (Phase 2 Step 4+5).
//! Cadre fix-up (2026-06-03) added T9 (pin-ordering correctness) and
//! moved pin populate from the driver send-site into
//! `process_completion` post-CQE — the load-bearing ordering that
//! prevents the pin from advertising bytes whose writev later errors.

#![cfg(feature = "chunked_fast_slow")]

use core::fmt::Debug;
use std::sync::OnceLock;

use tracing::info;

// ---------------------------------------------------------------------
// Test-only probes (T2, T4, T6, T8 — design §9)
// ---------------------------------------------------------------------
//
// HARNESS GATING (perf-optimizer F2 + cadre fix-up audit 2026-06-03):
//
// These probes are gated on `#[cfg(any(test, feature = "test-utils"))]`
// so cross-crate integration tests in
// `nativelink-store/tests/chunked_b1_writev_test.rs` (which enable the
// `test-utils` feature via `[dev-dependencies]`) can read/write them.
// Production binaries (no `test-utils`, no `cfg(test)`) compile these
// branches out entirely — same pattern as
// `chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST` (existing).
//
// Audit (workspace `grep -rn 'test-utils' Cargo.toml */Cargo.toml`):
//   - `nativelink-store/Cargo.toml:145`: `test-utils = []` (empty
//     default, no production dependent).
//   - `nativelink-service/Cargo.toml:96`: `test-utils =
//     ["nativelink-store/test-utils"]` — service crate's `test-utils`
//     forwards to ours, also `[]` at definition; both are dev-only
//     feature flags enabled by `cargo test --features test-utils`.
//   - `nativelink-worker/Cargo.toml:16`: same shape, dev-only.
//   - No `bin` target and no production crate enables `test-utils`
//     transitively. The `release` build (`just deploy`'s
//     `cargo build --release --features quic,pprof`) does NOT enable
//     `test-utils`, so the probe lookups compile out entirely.
//
// `#[cfg(test)] only` was considered: integration tests under `tests/`
// are compiled as separate crates that link the library WITHOUT
// `cfg(test)`, so probe items gated solely on `cfg(test)` would not be
// visible from `chunked_b1_writev_test.rs`. The `feature = "test-utils"`
// disjunct is load-bearing for cross-crate test access.

/// Test-only error-injection probe for T6 + T8 (design §9). Tests insert
/// a per-digest threshold `N`; the writer task synthesizes
/// `Err(io::Error::other("test-inject: writer error at writev count N"))`
/// on its (N+1)th writev submission for that digest. The error flows
/// through the standard `first_error` → drain-rx → return-Err path, so
/// T8's permits-returned assertion exercises real production drain
/// behavior (not a special test-only path).
///
/// Production builds compile the lookup out via the `cfg(test)`
/// `cfg(feature = "test-utils")` gates inside `writer_task`.
#[cfg(any(test, feature = "test-utils"))]
pub static WRITER_INJECT_ERROR_AFTER_N_BY_DIGEST: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<nativelink_util::common::DigestInfo, u64>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

/// Test-only coalesce-count probe for T2 (design §9). The writer task
/// pushes `coalesce_count` (number of iovecs in the SQE) for every
/// `writev` submission, keyed by digest so parallel tests don't
/// collide. Tests read the per-digest histogram after the writer task
/// completes to assert `sum == expected_chunks_total` (every chunk
/// accounted for in some writev) AND
/// `len <= ceil(blob_size / COALESCE_TARGET)` (coalescing actually
/// amortized).
#[cfg(any(test, feature = "test-utils"))]
pub static COALESCE_HISTOGRAM_BY_DIGEST: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<nativelink_util::common::DigestInfo, Vec<u32>>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

/// Test-only per-blob first-writev timestamp probe for T4 (design §9).
/// The writer task records the time it submits its FIRST writev SQE for
/// each digest. T4 launches two blobs in parallel via `tokio::join!` and
/// asserts the two timestamps fall within 100 ms — proving no global
/// serialization across different digests.
#[cfg(any(test, feature = "test-utils"))]
pub static WRITER_START_AT_BY_DIGEST: std::sync::LazyLock<
    parking_lot::Mutex<
        std::collections::HashMap<nativelink_util::common::DigestInfo, std::time::Instant>,
    >,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

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
    use nativelink_util::common::DigestInfo;
    use parking_lot::Mutex;
    use tokio::sync::mpsc;
    use tokio::sync::OwnedSemaphorePermit;
    use tracing::warn;

    use super::super::chunked_driver::ChunkPin;

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
    ///
    /// LOAD-BEARING DROP: `_permit` releases the
    /// `ChunkBudget` semaphore on `WriteJob` drop. Underscore is the
    /// Rust idiom for "unused name binding" — the field is NOT unused;
    /// it is held until drop fires the semaphore release. Removing the
    /// field would leak permits to the global aggregate cap (#47 b1
    /// cadre fix-up C3 / code-reviewer F4).
    pub struct WriteJob {
        pub offset: u64,
        pub bytes: Bytes,
        /// See type-level doc above for permit-lifetime invariant.
        pub _permit: OwnedSemaphorePermit,
        /// #212 Phase 2.5/2.7 fixup B1: optional `PinBudget` permit
        /// moved from `ChunkWork` to `WriteJob` so the writer task can
        /// transfer it into the `ChunkPin` AFTER the writev CQE returns
        /// Ok — preserves the "pin never advertises bytes that aren't
        /// on disk yet" contract (#47 b1 cadre fix-up P1).
        pub pin_permit: Option<OwnedSemaphorePermit>,
        /// Time at which the driver's `chunk_tx.send(...)` STARTED.
        /// Used by the writer's slow-write warn (design §5) to compute
        /// `enqueue_ms`.
        pub enqueue_time: Instant,
    }

    /// Per-chunk metadata accumulated alongside one writev SQE so that
    /// `process_completion` can populate the pin (one entry per iovec)
    /// AFTER the CQE returns Ok. `bytes` is the same `Bytes` whose ptr
    /// became the `iovec.iov_base` — reusing it (rather than cloning
    /// into a new Bytes) keeps pin population zero-copy.
    pub(super) struct ChunkMeta {
        pub offset: u64,
        pub bytes: Bytes,
        pub pin_permit: Option<OwnedSemaphorePermit>,
    }

    /// Result envelope returned by each in-flight writev future. The
    /// `_permits` Vec is moved into the future and dropped after the
    /// CQE is processed, releasing all chunk_budget permits for the
    /// chunks coalesced into this writev (design §6 S1 step 2 + §8 I9).
    ///
    /// `chunks` (#47 b1 cadre fix-up P1): per-chunk metadata used by
    /// `process_completion` to populate the in-memory pin AFTER the
    /// writev CQE returns Ok. One entry per iovec in `coalesce_count`
    /// order.
    ///
    /// `submit_started` (#47 b1 cadre fix-up C1/C2): wall-clock when
    /// `pop-from-pending` started for the run (i.e. when the writer
    /// began building the iovec batch). `submit_time` is when
    /// `system.writev(...)` returned the future. `submit_ms =
    /// submit_time - submit_started` measures the coalesce-build +
    /// io_uring-SQE-submission cost — the real "submit_ms" that
    /// dashboards expect (the previous hardcoded `submit_ms = 0` was
    /// dead local).
    struct WriteCompletion {
        total_len: usize,
        coalesce_count: usize,
        enqueue_time_earliest: Instant,
        submit_started: Instant,
        submit_time: Instant,
        /// Wall-clock when the poller loop consumed the CQE from the
        /// io_uring completion ring. Populated from the fork's new
        /// `reaped_at` return value. The gap `reaped_at - submit_time` is
        /// pure kernel I/O time; the gap `resume_now - reaped_at` is the
        /// tokio dispatch delay (eventfd→epoll→io-driver→waker hops).
        reaped_at: Instant,
        result: Result<usize, tokio_epoll_uring::Error<std::io::Error>>,
        // LOAD-BEARING DROP: `_permits` releases chunk_budget semaphore
        // on drop after CQE processing (one permit per iovec, see
        // `ChunkBudget` cap in `chunk_budget.rs:52`). The leading
        // underscore signals "unused name binding" but the field IS
        // load-bearing for permit lifetime.
        _permits: Vec<OwnedSemaphorePermit>,
        chunks: Vec<ChunkMeta>,
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
        digest: DigestInfo,
        fd_arc: Arc<std::fs::File>,
        mut chunk_rx: mpsc::Receiver<WriteJob>,
        // #47 b1 fix-up P1 (LOAD-BEARING ORDERING): pin is populated
        // here, NOT at the driver send-site, so the pin never advertises
        // bytes whose writev later errors. The driver's Path B fallback
        // populates the pin from its own success arm (where the
        // `spawn_blocking` pwrite has already returned Ok).
        pin: Arc<Mutex<ChunkPin>>,
    ) -> Result<(), Error> {
        let system = tokio_epoll_uring::thread_local_system().await;

        let mut in_flight: FuturesUnordered<
            std::pin::Pin<Box<dyn std::future::Future<Output = WriteCompletion> + Send>>,
        > = FuturesUnordered::new();
        let mut first_error: Option<Error> = None;
        // T2/T6/T8 probe support: number of writev SQEs submitted by this
        // task so far. Used to (a) compute `WRITER_INJECT_ERROR_AFTER_N`
        // trigger threshold, (b) push to `COALESCE_HISTOGRAM`, (c) decide
        // whether we are submitting the FIRST writev for `WRITER_START_AT_BY_DIGEST`.
        // Production builds don't reference the probes, but the counter
        // itself is cheap (one local u64 increment per writev).
        let mut writev_submit_count: u64 = 0;

        loop {
            // 1. Drain ready completions opportunistically (non-blocking).
            //    Mirrors fs.rs:846-854. Catches CQEs in the gap between
            //    `try_recv` drain phases without blocking the coalescer.
            loop {
                match in_flight.next().now_or_never() {
                    Some(Some(wc)) => {
                        if let Err(e) = process_completion(wc, &pin) {
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
                    if let Err(e) = process_completion(wc, &pin) {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            }

            // 3. Receive the next batch of WriteJobs.
            //    Coalescing accumulator: keyed by offset so contiguous
            //    runs can be detected by walking the BTreeMap.
            //    Per-entry value: (bytes, chunk_budget_permit,
            //    enqueue_time, pin_permit). The pin_permit rides along
            //    so `process_completion` can transfer it into the
            //    `ChunkPin` AFTER the writev CQE returns Ok.
            let mut pending: BTreeMap<
                u64,
                (Bytes, OwnedSemaphorePermit, Instant, Option<OwnedSemaphorePermit>),
            > = BTreeMap::new();
            let mut pending_bytes: usize = 0;
            let mut hit_eof = false;

            // Blocking recv for the first job. EOF terminates the loop.
            //
            // #47 b1 fix-up P1 side-effect: when `in_flight` has
            // outstanding writev SQEs, blocking PURELY on `chunk_rx.recv`
            // means a CQE arriving during the wait does not wake the
            // writer task — `process_completion` only fires when we
            // call `in_flight.next()` again. Pre-fix that didn't matter
            // because pin populate happened at the driver send-site;
            // post-fix the pin populate IS in `process_completion`, so
            // an idle CQE delays pin visibility. Race-on-test (lib
            // unit tests `pin_accessor_*` send 1 chunk + poll
            // `pinned_chunk_count`): writer submitted writev, blocked
            // recv, never polled in_flight again → test wedged.
            //
            // Fix: when `in_flight` is non-empty, race the recv against
            // the next completion via `tokio::select!`. The select
            // picks whichever resolves first; if a CQE lands, we
            // process it then loop back to retry the recv. If a job
            // arrives, we take the job. When `in_flight` is empty
            // there's nothing to race so we plain-await the recv.
            let mut maybe_first: Option<WriteJob> = None;
            if in_flight.is_empty() {
                match chunk_rx.recv().await {
                    Some(job) => maybe_first = Some(job),
                    None => {
                        hit_eof = true;
                    }
                }
            } else {
                // Race the recv against CQE completions so a pending
                // writev's pin populate can fire while we wait for
                // more chunks. Loop because a CQE wakes the select
                // without producing a job.
                loop {
                    tokio::select! {
                        biased;
                        Some(wc) = in_flight.next() => {
                            if let Err(e) = process_completion(wc, &pin) {
                                if first_error.is_none() {
                                    first_error = Some(e);
                                }
                            }
                            // CQE processed; retry the select.
                            continue;
                        }
                        maybe_job = chunk_rx.recv() => {
                            match maybe_job {
                                Some(job) => {
                                    maybe_first = Some(job);
                                    break;
                                }
                                None => {
                                    hit_eof = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if hit_eof {
                break;
            }
            let first = maybe_first.expect("either hit_eof or a job present");
            pending_bytes += first.bytes.len();
            pending.insert(
                first.offset,
                (first.bytes, first._permit, first.enqueue_time, first.pin_permit),
            );

            // Drain-until-empty: pull all immediately available jobs.
            // If still under the coalesce target, do one short blocking
            // recv to catch in-transit jobs.
            while pending_bytes < COALESCE_TARGET && pending.len() < IOV_MAX {
                match chunk_rx.try_recv() {
                    Ok(job) => {
                        pending_bytes += job.bytes.len();
                        pending.insert(
                            job.offset,
                            (job.bytes, job._permit, job.enqueue_time, job.pin_permit),
                        );
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
                                    (job.bytes, job._permit, job.enqueue_time, job.pin_permit),
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
                // #47 b1 Phase 2: TEST_PRE_WRITE_DELAY probe parity for
                // io_uring path. Mirrors the existing probe at
                // `chunked_filesystem::write_chunk_at_offset`
                // (chunked_filesystem.rs:567-574) so the
                // `driver_per_chunk_pwrite_timeout_*` tests inject a
                // pre-write wedge that applies to Path A as well as the
                // spawn_blocking Path B. Per-digest scoped so parallel
                // tests don't collide. Reuses the same static — single
                // source of truth for the delay map.
                //
                // Production builds compile this out via
                // `#[cfg(any(test, feature = "test-utils"))]`
                // (matching the static's own gate at
                // chunked_filesystem.rs:110). `feature = "test-utils"`
                // disjunct lets integration tests in
                // `tests/chunked_b1_writev_test.rs` (T9) drive the
                // probe from a separate compilation unit.
                #[cfg(any(test, feature = "test-utils"))]
                {
                    let delay = super::super::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
                        .lock()
                        .get(&digest)
                        .copied();
                    if let Some(delay_ms) = delay {
                        if delay_ms > 0 {
                            tokio::time::sleep(core::time::Duration::from_millis(delay_ms)).await;
                        }
                    }
                }

                // #47 b1 cadre fix-up C1/C2: record real submit_started
                // = wall-clock the writer began popping from `pending`
                // and building the iovec batch. `submit_time` is
                // captured immediately before `system.writev(...)`
                // submits the SQE, so
                // `submit_ms = submit_time - submit_started` is the
                // genuine coalesce-build + submission cost (the
                // previous hardcoded `submit_ms = 0` was dead local
                // per perf-optimizer F4 + code-reviewer F3).
                let submit_started = Instant::now();

                // Pop the lowest-offset entry as the run start.
                let (&start_offset, _) = pending.iter().next().expect("non-empty");
                let (start_bytes, start_permit, start_enqueue, start_pin_permit) = pending
                    .remove(&start_offset)
                    .expect("just observed");
                let mut run_offset = start_offset;
                // #47 b1 F3 (perf-optimizer re-cadre): allocate with
                // capacity `IOV_MAX` up-front so per-SQE Vec growth does
                // not incur the doubling-realloc cascade (0→4→8→...→1024
                // = ~10 reallocs without hint). Each Vec is consumed by
                // `system.writev(...)` (`iovecs`/`buffers`) or moved into
                // the in-flight async block (`permits`/`chunks_meta`), so
                // they cannot be hoisted/reused across iterations — the
                // capacity hint is the realizable allocation reduction.
                let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(IOV_MAX);
                let mut buffers: Vec<Bytes> = Vec::with_capacity(IOV_MAX);
                let mut permits: Vec<OwnedSemaphorePermit> = Vec::with_capacity(IOV_MAX);
                // #47 b1 fix-up P1: per-chunk metadata for post-CQE pin
                // populate. One entry per iovec, same order.
                let mut chunks_meta: Vec<ChunkMeta> = Vec::with_capacity(IOV_MAX);
                let mut earliest_enqueue = start_enqueue;
                let mut run_bytes = 0usize;

                // Push the start.
                iovecs.push(libc::iovec {
                    iov_base: start_bytes.as_ptr() as *mut libc::c_void,
                    iov_len: start_bytes.len(),
                });
                run_bytes += start_bytes.len();
                run_offset += start_bytes.len() as u64;
                chunks_meta.push(ChunkMeta {
                    offset: start_offset,
                    bytes: start_bytes.clone(),
                    pin_permit: start_pin_permit,
                });
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
                    let (next_bytes, next_permit, next_enqueue, next_pin_permit) = pending
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
                    chunks_meta.push(ChunkMeta {
                        offset: next_off,
                        bytes: next_bytes.clone(),
                        pin_permit: next_pin_permit,
                    });
                    buffers.push(next_bytes);
                    permits.push(next_permit);
                }

                let coalesce_count = iovecs.len();
                let submit_time = Instant::now();

                // T2 probe (design §9): record coalesce_count for every
                // writev SQE so the test can assert sum == expected
                // chunks AND len <= ceil(blob_size / COALESCE_TARGET).
                // Per-digest keyed so parallel tests do not collide.
                #[cfg(any(test, feature = "test-utils"))]
                {
                    super::COALESCE_HISTOGRAM_BY_DIGEST
                        .lock()
                        .entry(digest)
                        .or_insert_with(Vec::new)
                        .push(coalesce_count as u32);
                }

                // T4 probe (design §9): on the FIRST writev for this
                // digest, record submission timestamp. T4 launches two
                // blobs in parallel and asserts the two start times
                // differ by < 100 ms — proves no global serialization.
                #[cfg(any(test, feature = "test-utils"))]
                if writev_submit_count == 0 {
                    super::WRITER_START_AT_BY_DIGEST
                        .lock()
                        .insert(digest, submit_time);
                }

                // T6/T8 probe (design §9): if the per-digest threshold
                // is reached, synthesize an Err that flows through the
                // normal first_error → drain-rx path. This skips
                // submitting THIS writev (and any subsequent writev)
                // so the test can assert (T6) the substring survives
                // through driver's `expect_err` and (T8) all permits
                // return to ChunkBudget via the post-error drain.
                #[cfg(any(test, feature = "test-utils"))]
                {
                    let trigger = super::WRITER_INJECT_ERROR_AFTER_N_BY_DIGEST
                        .lock()
                        .get(&digest)
                        .copied();
                    if let Some(n) = trigger {
                        if writev_submit_count >= n {
                            first_error = Some(make_err!(
                                Code::Internal,
                                "test-inject: writer error at writev count {n}",
                            ));
                            // Drop pending → permits drop. Skip the
                            // submission so no further writev SQEs go
                            // out; the outer loop's first_error check
                            // will drain chunk_rx on subsequent
                            // iterations.
                            drop(pending);
                            break;
                        }
                    }
                }

                writev_submit_count += 1;

                let write_fut = system.writev(
                    Arc::clone(&fd_arc),
                    start_offset,
                    iovecs,
                    buffers,
                );

                let total_len = run_bytes;
                in_flight.push(Box::pin(async move {
                    let (_fd, reaped_at, result) = write_fut.await;
                    WriteCompletion {
                        total_len,
                        coalesce_count,
                        enqueue_time_earliest: earliest_enqueue,
                        submit_started,
                        submit_time,
                        reaped_at,
                        result,
                        _permits: permits,
                        chunks: chunks_meta,
                    }
                }));
            }

            if hit_eof {
                break;
            }
        }

        // 6. Drain all in-flight completions before returning.
        while let Some(wc) = in_flight.next().await {
            if let Err(e) = process_completion(wc, &pin) {
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

    /// Process one writev completion: extract bytes-written, populate
    /// the in-memory pin per chunk (LOAD-BEARING ORDERING for #47 b1
    /// fix-up P1 — pin populate fires only AFTER writev CQE returns Ok),
    /// emit a slow-write warn (design §5), and surface short-writes as
    /// errors (matches `fs.rs:811-818` semantics, design §8 I4).
    fn process_completion(
        wc: WriteCompletion,
        pin: &Arc<Mutex<ChunkPin>>,
    ) -> Result<(), Error> {
        let n = match wc.result {
            Ok(n) => n,
            Err(e) => {
                // LOAD-BEARING ORDERING (#47 b1 fix-up P1 / T9 mutation
                // target): on writev Err, the `wc.chunks` Vec drops
                // here — pin_permits go back to the global PinBudget,
                // and the chunk bytes are NOT inserted into the pin.
                // A concurrent reader's `try_get_chunk_from_pin` for
                // these offsets returns None and falls through to the
                // slow store, never seeing bytes whose writev errored.
                return Err(uring_err_to_error(e, "chunked_writer writev"));
            }
        };
        if n < wc.total_len {
            // Same as the Err arm: do NOT populate the pin on short
            // write — the partial bytes that DID land are not
            // operator-visible via the pin; the blob will be
            // retried by FastSlowStore.
            return Err(make_err!(
                Code::Internal,
                "io_uring partial writev: {n}/{} bytes (short write — \
                 CAS blob will be retried by FastSlowStore)",
                wc.total_len,
            ));
        }

        // #47 b1 fix-up P1: writev Ok(n) == total_len → bytes are on
        // disk. NOW populate the in-memory pin for each chunk so
        // `ChunkedDriver::try_get_chunk_from_pin` may serve them. One
        // lock acquisition for all chunks in this writev (rust-crate
        // L2: amortize critical section).
        {
            let mut pin_state = pin.lock();
            for meta in wc.chunks {
                pin_state.populate(meta.offset, meta.bytes, meta.pin_permit);
            }
        }

        // Slow-write probe per design §5. Threshold matches
        // chunked_driver.rs:985's existing `back_edge_ms > 50` warn.
        let enqueue_ms = wc
            .submit_started
            .saturating_duration_since(wc.enqueue_time_earliest)
            .as_millis() as u64;
        // #47 b1 cadre fix-up C1/C2: real `submit_ms` measures the
        // coalesce-build + io_uring-SQE-submission cost (the previous
        // hardcoded `submit_ms = 0` was dead local — perf F4 +
        // code-reviewer F3 + red-team A1 + distsys M6).
        let submit_ms = wc
            .submit_time
            .saturating_duration_since(wc.submit_started)
            .as_millis() as u64;
        // Legacy field: kept unchanged for baseline continuity.
        // `writev_ms` spans SQE-submit → future-resume; it conflates
        // kernel I/O time with tokio dispatch delay. The two new fields
        // below split it cleanly (#3 cqe-reap-timestamp).
        let writev_ms = wc.submit_time.elapsed().as_millis() as u64;
        // `cqe_kernel_ms`: time from SQE submit → CQE reap by the
        // poller loop. Pure kernel/io_uring time (inc. io-wq offload).
        let cqe_kernel_ms = wc
            .reaped_at
            .saturating_duration_since(wc.submit_time)
            .as_millis() as u64;
        // `cqe_dispatch_ms`: time from CQE reap → this function running
        // (eventfd→epoll→tokio-io-driver→waker→scheduler hops). When
        // PSI stalls are active, this component dominates and was
        // previously indistinguishable from storage latency.
        let cqe_dispatch_ms = wc
            .reaped_at
            .elapsed()
            .as_millis() as u64;
        let total_inner_ms = enqueue_ms + submit_ms + writev_ms;
        if total_inner_ms > 50 {
            warn!(
                target: "nativelink_store::chunked",
                // NEW fields (design §5):
                enqueue_ms,
                submit_ms,
                writev_ms,
                // #3 cqe-reap-timestamp split: kernel time vs dispatch time.
                cqe_kernel_ms,
                cqe_dispatch_ms,
                coalesce_count = wc.coalesce_count,
                path = "io_uring",
                // LEGACY mapped fields (design §5 table):
                mutex_acquire_ms = 0u64,
                // Renamed from `dispatch_ms` (code-reviewer, cqe-split review):
                // that key collided semantically with the new `cqe_dispatch_ms`
                // — this one is PRE-SQE time (enqueue + coalesce/submit build),
                // the cqe_ one is POST-CQE reap→resume. Same warn line must
                // not carry two opposite-phase "dispatch" keys.
                pre_sqe_ms = enqueue_ms + submit_ms,
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
