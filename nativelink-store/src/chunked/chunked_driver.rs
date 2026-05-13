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

//! Phase 2.3: per-blob driver task that consumes `ChunkWork` items from
//! a bounded mpsc, performs the slow-tier `pwrite` via the
//! FilesystemStore adapters, tracks per-chunk arrival in an in-memory
//! sidecar bitmap, and finalizes via `commit_chunked` (with end-to-end
//! SHA-256 verification per design §8.3.1) or `discard_chunked` on
//! failure.
//!
//! Lifecycle (per design §6.7 termination contract):
//!
//! - **Trigger (a) — happy path:** the upstream RPC handler closes the
//!   sender after admitting the final `finish` chunk. The driver
//!   processes any remaining chunks, then `recv() → None`. If
//!   `finish_seen` is true, the driver attempts `commit` and signals
//!   the result on the `completion_tx` oneshot. Then exits cleanly.
//! - **Trigger (b) — shutdown:** parent drops `ChunkedDriver`. The
//!   `JoinHandleDropGuard` aborts the spawned task. Any in-flight
//!   partial is left on disk (legacy `prune_temp_path` GCs it on next
//!   `FilesystemStore::new`).
//! - **Trigger (c) — bounded retry exhausted:** per-chunk SHA-256
//!   mismatch OR commit-time end-to-end SHA-256 mismatch. The driver
//!   discards the partial, signals the error on `completion_tx`, and
//!   exits.
//! - **Trigger (d) — panic safety:** any panic inside the spawned task
//!   propagates through `JoinHandleDropGuard`'s `JoinError`. The driver
//!   on-panic owes nothing to the filesystem (the pwrite was either
//!   atomic-success or atomic-failure at the syscall boundary; the
//!   state map entry remains and `prune_temp_path` GCs it on restart).
//!
//! Anti-#203 invariant (design §6.7):
//!   `update()` MUST NOT block on slow-tier drain.
//! For the WriteChunked RPC the upstream caller IS the producer
//! (worker), so the RPC handler explicitly OPTS-IN to wait on the
//! commit via `await_completion()` — the driver itself does not couple
//! the upstream RPC's `Ok(WriteChunkedResponse)` to anything beyond
//! the commit it is performing on behalf of THAT RPC. The historical
//! #203 cascade (Bazel-facing FastSlowStore::update synchronously
//! awaiting the slow tier) is a different code path and remains async.

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err};
use nativelink_util::common::DigestInfo;
use nativelink_util::cpu_pool::cpu_pool;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use parking_lot::Mutex;
use nativelink_util::digest_hasher::{DigestHasher, default_digest_hasher_func};
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use crate::filesystem_store::{FileEntry, FilesystemStore};

/// Per-blob mpsc capacity: how many `ChunkWork` items the chunker can
/// have in-flight to the per-blob driver before `try_send` returns
/// `Full` (the admission gate then signals
/// `BackpressureSignal::PerBlobMpscFull` and the unfold stream parks
/// on its own `reader.recv`, transitively pushing back to h2). Bounds
/// per-blob memory pressure at `PER_BLOB_MPSC_CAP * CHUNK_SIZE`
/// (currently `256 * 1 MiB = 256 MiB`).
///
/// **Aggregate RSS is bounded by the GLOBAL `ChunkBudget` (4 GiB /
/// 4096 permits at `chunk_budget.rs:52`) and per-blob `PinBudget`,
/// NOT by `PER_BLOB_MPSC_CAP × concurrent-blobs`.** The per-blob cap
/// only sets fan-out shape: a higher cap means fewer concurrent blobs
/// can saturate the global budget (4 GiB / 256 MiB = 16 max-saturated
/// blobs at this cap; was 64 at the prior cap of 64 MiB-per-blob, and
/// 256 at the original cap of 16 MiB-per-blob). The §13.2 12-16 GiB
/// worst-case server RSS
/// bound holds unchanged because it is derived from the global
/// budgets, not from any per-blob × N product.
///
/// **Why 256 (was 64, bumped 2026-05-12; was 16 before 2026-05-11).**
/// Audit `.claude/audits/413-large-blob-cascade-design-20260511.md`
/// observed 11 large-blob cascade events in a 17-min post-deploy
/// window where the cap-of-64 (= 64 MiB per-blob in-flight) still
/// rejected mid-stream because the chunker → driver back-edge was
/// the bottleneck — NOT the chunker fill-rate. The cascade was
/// back-edge bound (commit-and-verify BLAKE3 + per-chunk pwrite at
/// an inferred ~25-50 MB/s drain ceiling), not chunker-fill bound.
/// 9 of the 11 victim events were ≤256 MiB (audit §1.1 bucket
/// distribution: 2 in 64-128 MiB + 7 in 128-256 MiB; only the
/// 426 MB and 519 MB outliers fall outside)
/// (= `MAX_CHUNKED_BLOB_SIZE`), so cap=256 covers the entire
/// chunked-blob class for those — the per-blob mpsc can absorb the
/// whole blob without a single mid-stream rejection. Residual
/// 256-540 MB outliers (~7/hr observed) are tracked under #303
/// (server-internal driver fan-out — Option D) as a follow-up:
/// those still stream through and may cascade at high
/// offsets; the structural fix for them is sizing (CHUNK_SIZE bump)
/// or a true-streaming commit, not a further mpsc cap bump.
///
/// **Pre-bump observability gap.** The 25-50 MB/s drain rate cited
/// above is INFERRED from "543 s max single-chunk wait at the
/// chunker → driver boundary" plus rough back-of-envelope BLAKE3
/// throughput; we have NEVER directly traced per-chunk back-edge
/// wall-clock in production. The cap bump ships alongside an
/// outlier-only `warn!` probe at the per-chunk back-edge site
/// (fires only when the per-chunk back-edge exceeds 50 ms, so
/// steady-state log volume is zero) so the next cap-tuning
/// iteration has direct measurements. See the `back_edge_ms` field
/// on `"per-chunk back-edge drain exceeded 50ms"` log lines.
/// `debug!`-level was rejected because the workspace pins
/// `tracing = features = ["release_max_level_info"]`
/// (`Cargo.toml:101`), which compile-time eliminates `debug!` in
/// release builds.
///
/// **Historical context (cap=16 → 64 on 2026-05-11).** The original
/// 16 was an arbitrary §4 Q4 placeholder, not a measured value.
/// Audit `.claude/audits/chunked-admission-p99-2026-05-11.md`
/// recorded **870 `buf_channel::send: channel backpressure (>1s wait)`
/// events in 60 min, max `send_ms: 543434` (~9 min single-chunk
/// wait)** at the prior cap of 16 — h2 frames stalling because the
/// per-blob mpsc was draining slower than the network was filling
/// it. Zero `mpsc_full_rejections` were observed at that cap (the
/// gate did not reject any chunk), but the buf_channel upstream
/// transitively absorbed the back-edge stall. Bumping to 64 smoothed
/// that back-edge for small/mid blobs but did NOT structurally cover
/// the large-blob cascade class — hence this further bump to 256.
/// This is still a CAP, not a target; steady-state occupancy is
/// governed by the chunker → driver throughput ratio.
///
/// **Sibling cascade: Bazel-side Chunker NPE on retry race.** The
/// patched Bazel binary emits 2 MiB wire-chunks (was 16 KiB at the
/// time of the cap=16 → 64 bump). At cap=64 the per-blob mpsc filled
/// in 32 wire-chunks (= 64 MiB), still allowing
/// `chunked dispatch: per-blob mpsc full` cascades for blobs >64 MiB
/// — each cancel raced client-side retry → Bazel
/// `Chunker.seek():165 data == null` NPE (a known Bazel issue, see
/// upstream #28489 and prior fixes `9a823d9d` / `01d7f97d` /
/// `397266d0` / `cfef67da`). The 256-slot cap structurally eliminates
/// this cascade for blobs ≤256 MiB (the entire `MAX_CHUNKED_BLOB_SIZE`
/// range). Larger 256-540 MB outliers still stream through the
/// bounded mpsc and may cascade at higher offsets — tracked under
/// #303 (server-internal driver fan-out — Option D) / #413
/// (200-540 MB blob cascade after cap=64) sizing follow-up.
pub const PER_BLOB_MPSC_CAP: usize = 256;

/// Per-chunk wall-clock bound on the slow-tier `pwrite` step
/// (#213 perf-opt NMA2 fixup for §6.7 trigger (b)). Bounds how long
/// the driver waits for ANY single `write_chunk_at_offset` before
/// abandoning the chunk and the rest of the blob. Chosen to be 5×
/// the worst-case healthy ZFS pwrite latency (~1 s observed in the
/// 2026-03 sync=disabled deploy) so a routine slow tick does not
/// abandon a blob, while a wedged pool can't stall the driver
/// arbitrarily long.
///
/// IMPORTANT: this timeout bounds the AWAIT of the `spawn_blocking`
/// JoinHandle, NOT the underlying syscall. `spawn_blocking` work is
/// uncancellable per tokio API contract — once dispatched, the
/// closure runs to completion. So the actual upper bound on shutdown
/// drain in the worst case is `(workers_in_blocking_pool ×
/// per-chunk-syscall-wall-clock)`, which is bounded by the kernel's
/// I/O timeout heuristics (typically tens of seconds) but NOT by
/// this constant. Per perf-optimizer NMA2: this gap is acknowledged
/// and accepted as the implementation-level cost of `spawn_blocking`
/// uncancellability — the timeout still bounds the driver task's
/// AWAIT, which is what feeds back to the caller and to
/// `JoinHandleDropGuard`'s `abort()` reaching a clean state.
pub const PER_CHUNK_WRITE_TIMEOUT: core::time::Duration =
    core::time::Duration::from_secs(5);

/// #213 reviewer M8 (RECONSIDER red-team): when
/// `tokio::time::timeout` fires on a `spawn_blocking` pwrite, the
/// blocking-pool task continues to completion BUT its result is
/// discarded — the JoinHandle is dropped on the timeout-Err arm.
/// `spawn_blocking` is uncancellable per tokio API contract, so the
/// underlying syscall keeps the blocking-pool thread occupied until
/// the kernel resolves the wedge (kernel I/O timeout, on the order
/// of tens of seconds). Repeated wedges leak threads from the
/// 512-thread default pool until none remain, at which point all
/// further `spawn_blocking` calls queue indefinitely.
///
/// To make this leak observable BEFORE it's catastrophic, we
/// increment a counter on every per-chunk pwrite timeout and warn
/// when more than [`PWRITE_TIMEOUT_WARN_THRESHOLD`] timeouts occur
/// within a [`PWRITE_TIMEOUT_WARN_WINDOW`]. The SRE can correlate
/// against `tokio::runtime::RuntimeMetrics::num_blocking_threads()`
/// to confirm the pool is approaching saturation.
///
/// Counter is `pub` for downstream metrics surfaces (the workspace's
/// MetricsComponent macro doesn't gate on visibility, but explicit
/// `pub` makes it discoverable from tracing instrumentation tests).
///
/// **Why free-standing static rather than `MetricsComponent`?**
/// `MetricsComponent` requires a host struct + a `#[metric(help =
/// "...")]` field. `chunked_driver.rs` is a free-standing module
/// (no per-driver state struct lives long enough to host this — the
/// `ChunkedDriver` is a per-blob handle that drops on commit; the
/// counter must persist across blobs, across drivers, for the
/// lifetime of the process). A static fits the cardinality
/// (process-wide, not per-blob, not per-store-instance). Wiring it
/// into a `MetricsComponent` would require either (a) creating a
/// new singleton just to host the field, or (b) hosting it on
/// `FilesystemStore`'s metric struct — but the counter is
/// chunked-specific, not filesystem-specific. The
/// `tracing::warn!` rate-limited fire (see
/// [`record_pwrite_timeout_and_maybe_warn`]) AND the explicit
/// `pub` accessor (consumed by tests + future scrape integration)
/// give equivalent observability without taking on the
/// MetricsComponent host-struct overhead. (#213 reviewer round-2
/// MINOR-3.)
pub static CHUNKED_DRIVER_PWRITE_TIMEOUT_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Sliding-window threshold: warn loudly when this many per-chunk
/// pwrite timeouts fire inside a [`PWRITE_TIMEOUT_WARN_WINDOW`].
/// Chosen low enough that an early warning surfaces before all 512
/// pool threads are leaked, but high enough that a single transient
/// slow-tier blip doesn't trigger.
const PWRITE_TIMEOUT_WARN_THRESHOLD: u64 = 10;

/// Sliding window for the warn rate trigger. 60 seconds is the
/// typical scrape interval for monitoring agents; if more than
/// `PWRITE_TIMEOUT_WARN_THRESHOLD` timeouts land in the same window
/// the SRE will see the warn alongside the next scrape.
const PWRITE_TIMEOUT_WARN_WINDOW: core::time::Duration =
    core::time::Duration::from_secs(60);

/// Last-warn timestamp + count baseline (paired). When the elapsed
/// since `last_warn_at` exceeds the window OR the delta from
/// `count_at_last_warn` exceeds the threshold, we re-warn and reset
/// the baseline. Single-writer (the run_driver Err arm), so a plain
/// parking_lot mutex over a 16-byte tuple is enough.
static PWRITE_TIMEOUT_WARN_STATE: parking_lot::Mutex<Option<(std::time::Instant, u64)>> =
    parking_lot::Mutex::new(None);

/// Helper called from the per-chunk pwrite timeout-Err arm: bumps
/// the counter and emits a warn! when the rate exceeds threshold
/// within the window. No-op when the rate is healthy.
fn record_pwrite_timeout_and_maybe_warn() {
    let new_total = CHUNKED_DRIVER_PWRITE_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
    let now = std::time::Instant::now();
    let mut guard = PWRITE_TIMEOUT_WARN_STATE.lock();
    let baseline = guard.unwrap_or((now, new_total.saturating_sub(1)));
    let (last_at, count_at_last) = baseline;
    let delta = new_total.saturating_sub(count_at_last);
    let elapsed = now.duration_since(last_at);
    if delta >= PWRITE_TIMEOUT_WARN_THRESHOLD && elapsed <= PWRITE_TIMEOUT_WARN_WINDOW {
        warn!(
            target: "nativelink_store::chunked",
            total_timeouts = new_total,
            timeouts_in_window = delta,
            window_secs = PWRITE_TIMEOUT_WARN_WINDOW.as_secs(),
            "chunked driver: per-chunk pwrite timeouts exceeding {PWRITE_TIMEOUT_WARN_THRESHOLD}/window — \
             blocking-pool threads may be leaking; correlate with \
             tokio::runtime::RuntimeMetrics::num_blocking_threads() (#213 reviewer M8)",
        );
        *guard = Some((now, new_total));
    } else if elapsed > PWRITE_TIMEOUT_WARN_WINDOW {
        // Reset baseline; healthy rate.
        *guard = Some((now, new_total));
    } else {
        // First-call seed.
        if guard.is_none() {
            *guard = Some(baseline);
        }
    }
}

/// #213 reviewer M2 fixup: wall-clock bound on the post-failure
/// `discard_chunked` / `unlink_holding` awaits in `run_driver` and
/// `commit_and_verify`. Without this, a wedged slow tier — the SAME
/// failure mode that motivates [`PER_CHUNK_WRITE_TIMEOUT`] — would
/// hang the driver task indefinitely on the post-error cleanup
/// path, breaking the **post-error cleanup contract**: the driver
/// promises to settle every error path in bounded wall-clock so
/// that the spawning task's `await_completion()` returns within a
/// predictable upper bound (per-chunk timeout + a small constant).
/// This bounds trigger (a)'s commit-failure cleanup AND trigger
/// (c)'s mismatch-failure cleanup; trigger (b) is already bounded
/// by `JoinHandleDropGuard` aborting the spawned task. 5 s matches
/// the per-chunk timeout (same root cause; same upper bound).
pub const DISCARD_AFTER_FAILURE_TIMEOUT: core::time::Duration =
    core::time::Duration::from_secs(5);

/// One unit of work consumed by the per-blob driver.
///
/// Carries the chunk payload + per-chunk SHA-256 (already verified at
/// admission time per #213 perf-opt NMA1, on a `spawn_blocking` so the
/// hash burn does not block a tokio worker) + the `ChunkBudget`
/// `OwnedSemaphorePermit` minted at admission.
///
/// `finish` marks the FINAL chunk of the blob. The driver waits until
/// every chunk in the bitmap has landed, then runs commit + end-to-end
/// SHA-256 verification.
///
/// Permit lifetime = `ChunkWork` lifetime; on driver-task panic the
/// permit drops automatically (no separate reclamation path).
#[derive(Debug)]
pub struct ChunkWork {
    pub chunk_offset: u64,
    pub chunk_bytes: Bytes,
    /// True iff this is the LAST chunk of the blob. Triggers commit
    /// once all preceding chunks have landed.
    pub finish: bool,
    /// Permit lifetime = `ChunkWork` lifetime. Per §13.1.1 point 1:
    /// admission moved this permit out of the global `ChunkBudget`
    /// into the `ChunkWork`; dropping the work releases the permit.
    pub _permit: OwnedSemaphorePermit,
    /// #212 Phase 2.5/2.7 fixup B1: optional `PinBudget` permit. The
    /// driver, after pwrite + adding the chunk to the in-memory pin,
    /// transfers this permit into the `ChunkPin` so the byte budget
    /// remains held until the pin clears (post-commit). `None` for
    /// callers that don't admit through `PinBudget` (legacy
    /// `WriteChunked` RPC path; tests that bypass the budget). Carrying
    /// it on `ChunkWork` (vs admission-side bookkeeping) ensures the
    /// permit drops cleanly on driver-task panic via `Drop` of
    /// `ChunkWork` — no separate reclamation path.
    pub _pin_permit: Option<OwnedSemaphorePermit>,
}

/// Sender half of the per-blob mpsc.
///
/// Closing the sender (drop) is the §6.7 happy-path termination
/// trigger — the driver loop exits cleanly when the channel closes.
pub type ChunkWorkSender = mpsc::Sender<ChunkWork>;

/// Result of a complete chunked write.
#[derive(Debug, Clone)]
pub struct ChunkedCommitResult {
    /// Total bytes committed. Equals the digest's declared size on
    /// success (the commit refuses to rename when actual length differs
    /// from declared size — see `commit_chunked` adapter).
    pub committed_size: u64,
}

/// Per-blob in-memory sidecar state per design §7.2 (in-memory only
/// per v4.5; no on-disk sidecar).
///
/// Tracks which chunks have landed via offset-keyed `BTreeSet`. The
/// commit code path waits until `landed_offsets` has the cardinality
/// equal to the expected chunk count, then verifies full coverage.
///
/// `parking_lot::Mutex` is correct: every critical section is short
/// (insert + check) and never holds across an `.await`.
#[derive(Debug, Default)]
struct SidecarState {
    /// Set of byte-offsets where chunks have successfully landed on
    /// disk. The handler may admit chunks out-of-order, so we cannot
    /// just count `n == expected_chunks` — we must track WHICH
    /// offsets have landed to detect coverage gaps.
    landed_offsets: BTreeSet<u64>,
    /// True once the final chunk has been received. The driver does
    /// NOT commit until both (a) `finish_seen == true` AND
    /// (b) every expected offset has landed.
    finish_seen: bool,
}

/// Per-chunk in-memory pin (`failed_writes` per design §6.2 / read
/// cascade step 2 per §6.3). Chunks land here as they arrive at the
/// driver and remain reachable from the read accessor
/// [`ChunkedDriver::try_get_chunk_from_pin`] until the blob commits.
///
/// `BTreeMap<offset → Bytes>` so the read accessor can walk in offset
/// order to assemble a contiguous range, and so duplicate-offset
/// arrivals (the producer protocol violation already warned about in
/// `run_driver`) are tolerated by overwrite (idempotent at the byte
/// level since pwrite already accepted both). The `Bytes` is shared
/// (no copy) with the same buffer that `write_chunk_at_offset` already
/// streamed to disk — adding a chunk to the pin is one `clone()` of an
/// `Arc`-backed `Bytes` per chunk.
///
/// `parking_lot::Mutex` is correct for the same reason as
/// `SidecarState`: every critical section is short (insert + return),
/// never holds across an `.await`. The accessor builds the assembled
/// range via successive `Bytes::slice` calls — also non-blocking.
#[derive(Debug, Default)]
struct ChunkPin {
    /// Map of byte-offset → chunk bytes. Memory cost = sum of chunk
    /// lengths held while the driver is in-flight. Bounded by the
    /// per-blob mpsc cap (`PER_BLOB_MPSC_CAP * CHUNK_SIZE = 256 MiB`)
    /// while the driver is consuming, AND further bounded after the
    /// driver consumed but before commit by the blob size itself —
    /// already counted toward the global ChunkBudget via the Q8
    /// per-chunk permits the `ChunkWork` items hold.
    chunks: BTreeMap<u64, Bytes>,
    /// Total bytes pinned. Cached so the accessor avoids walking the
    /// map to compute coverage; updated on every insert.
    total_bytes: u64,
    /// #212 Phase 2.5/2.7 fixup B1: `PinBudget` permits transferred from
    /// admitted `ChunkWork` items. Each permit covers the byte length
    /// of one chunk; the permits remain held (and the byte budget
    /// remains consumed) until the pin clears post-commit. Drop releases
    /// the permits to the global pool.
    ///
    /// Why a `Vec` not a single combined permit: each permit is a
    /// distinct `OwnedSemaphorePermit` minted at admission with a fixed
    /// per-chunk byte count. Combining requires `merge` (not stable on
    /// `tokio::sync::OwnedSemaphorePermit`); a `Vec` is simpler and
    /// amortizes to one `Arc` bump per chunk.
    pin_permits: Vec<OwnedSemaphorePermit>,
}

/// Per-blob driver task handle.
///
/// The `JoinHandleDropGuard` ensures the task is aborted on `Drop`
/// (panic-safety belt for §6.7 trigger d). In the happy path the task
/// exits before drop because the mpsc closes when admission drops its
/// sender AND the commit completes.
///
/// Holds a `oneshot::Receiver` for the commit-result so the upstream
/// RPC handler can `await_completion()` (synchronous-commit option α
/// per Phase 2.2/2.3 design call).
#[derive(Debug)]
pub struct ChunkedDriver {
    /// Identifies the blob this driver belongs to. Logging / metrics
    /// will scope by digest.
    digest: DigestInfo,
    /// Counter incremented on every `ChunkWork` received. Used by
    /// tests + the metric gauge wiring in Phase 2.5+.
    chunks_received: Arc<AtomicU64>,
    /// Counter incremented on every chunk that lands successfully on
    /// disk. Diverges from `chunks_received` on per-chunk-write
    /// failure (the chunk was received but the pwrite errored).
    chunks_committed: Arc<AtomicU64>,
    /// Flipped to `true` by the spawned task IMMEDIATELY after the
    /// `recv()` loop exits. Load-bearing for the §6.7 trigger (a)
    /// regression test.
    loop_exited: Arc<AtomicBool>,
    /// Receiver for the commit-result. The upstream RPC handler
    /// awaits this AFTER admitting the final chunk to learn whether
    /// the commit succeeded. The Sender lives inside the spawned
    /// task; on driver-task panic the Sender drops and the Receiver
    /// observes `Err(_)` (which the handler maps to `Code::Internal`).
    completion_rx: parking_lot::Mutex<Option<oneshot::Receiver<Result<ChunkedCommitResult, Error>>>>,
    /// Per-chunk in-memory pin (the design §6.2 `failed_writes` /
    /// §6.3 step 2 pin). Shared with the spawned driver task — the
    /// task inserts on each landed chunk; [`Self::try_get_chunk_from_pin`]
    /// reads from it for the read cascade in
    /// `FastSlowStore::get_part`. Cleared by the driver after a
    /// successful `commit_and_verify` (the canonical CAS path serves
    /// the blob from disk after that point).
    pin: Arc<Mutex<ChunkPin>>,
    /// Total declared blob size in bytes. Used by the read accessor to
    /// reject offsets/lengths that overrun the blob, and by callers
    /// (e.g. `try_get_full_blob_from_pin`) that want to know whether
    /// every byte is currently pinned.
    expected_size: u64,
    /// Drop guard for the spawned task. On `Drop` of `ChunkedDriver`,
    /// the join handle is `abort()`'d if still running (§6.7 panic
    /// belt).
    _handle: JoinHandleDropGuard<()>,
}

impl ChunkedDriver {
    /// Spawn the per-blob driver task.
    ///
    /// `filesystem_store` is the slow-tier backend. `digest` identifies
    /// the blob. `expected_size` is the declared blob length (from the
    /// digest); the driver uses it to compute the expected chunk count
    /// and to pass to `commit_chunked` for length validation.
    /// `capacity` is the mpsc bound; admission code MUST pass
    /// `PER_BLOB_MPSC_CAP` in production — the argument is here so
    /// tests can exercise smaller channels without a `PER_BLOB_MPSC_CAP`-
    /// sized `ChunkWork` setup.
    ///
    /// `chunk_size` is the contractual chunk size used to compute the
    /// expected chunk-count for the bitmap completeness check. Production
    /// uses `super::CHUNK_SIZE` (1 MiB); tests can pass smaller values
    /// so a 12 KiB blob exercises real out-of-order arrival logic.
    pub fn spawn_driver<Fe: FileEntry>(
        filesystem_store: Arc<FilesystemStore<Fe>>,
        digest: DigestInfo,
        expected_size: u64,
        chunk_size: usize,
        capacity: usize,
    ) -> (Self, ChunkWorkSender) {
        Self::spawn_driver_with_per_chunk_timeout(
            filesystem_store,
            digest,
            expected_size,
            chunk_size,
            capacity,
            PER_CHUNK_WRITE_TIMEOUT,
        )
    }

    /// Same as [`Self::spawn_driver`] but accepts a custom per-chunk
    /// pwrite timeout. Production callers MUST use [`Self::spawn_driver`]
    /// (which passes [`PER_CHUNK_WRITE_TIMEOUT`]); this entry point is
    /// shared with tests that exercise the timeout path under a short
    /// bound, so a wedged-slow-tier scenario fires the timeout in
    /// bounded test wall-clock instead of waiting for the production
    /// 5 s constant.
    ///
    /// #213 reviewer M3 fixup: visibility narrowed from `pub` to
    /// `pub(crate)`. The only callers are `Self::spawn_driver` (which
    /// hard-codes the production constant) and the in-file unit test
    /// `driver_per_chunk_pwrite_timeout_returns_deadline_exceeded`; no
    /// external consumer should ever pass a non-production timeout.
    pub(crate) fn spawn_driver_with_per_chunk_timeout<Fe: FileEntry>(
        filesystem_store: Arc<FilesystemStore<Fe>>,
        digest: DigestInfo,
        expected_size: u64,
        chunk_size: usize,
        capacity: usize,
        per_chunk_timeout: core::time::Duration,
    ) -> (Self, ChunkWorkSender) {
        let (tx, rx) = mpsc::channel::<ChunkWork>(capacity);
        let chunks_received = Arc::new(AtomicU64::new(0));
        let chunks_committed = Arc::new(AtomicU64::new(0));
        let loop_exited = Arc::new(AtomicBool::new(false));
        let (completion_tx, completion_rx) = oneshot::channel();
        let pin: Arc<Mutex<ChunkPin>> = Arc::new(Mutex::new(ChunkPin::default()));

        let chunks_received_for_task = Arc::clone(&chunks_received);
        let chunks_committed_for_task = Arc::clone(&chunks_committed);
        let loop_exited_for_task = Arc::clone(&loop_exited);
        let pin_for_task = Arc::clone(&pin);

        // Compute the expected chunk count from declared size + chunk
        // size. For a blob of N bytes with chunk size C, the expected
        // count is `ceil(N / C)`. A zero-byte blob has zero chunks
        // (the producer should send a single `finish` chunk with
        // empty bytes; the driver handles this as the "no offsets
        // ever landed" path that still triggers commit).
        let expected_chunk_count = if expected_size == 0 {
            0
        } else {
            let chunk_size_u64 = chunk_size as u64;
            usize::try_from(expected_size.div_ceil(chunk_size_u64))
                .expect("ceil(expected_size / chunk_size) must fit in usize for any practical blob")
        };

        let handle = spawn!("212_chunked_driver", async move {
            let sidecar: Arc<Mutex<SidecarState>> = Arc::new(Mutex::new(SidecarState::default()));
            let task_result = run_driver(
                rx,
                filesystem_store,
                digest,
                expected_size,
                expected_chunk_count,
                Arc::clone(&sidecar),
                Arc::clone(&pin_for_task),
                Arc::clone(&chunks_received_for_task),
                Arc::clone(&chunks_committed_for_task),
                per_chunk_timeout,
            )
            .await;
            // Drop the in-memory pin once the driver loop has finished
            // (commit success → blob is on the canonical CAS path; commit
            // failure → bytes are not authoritative). Frees per-blob
            // memory promptly even if the `Arc<ChunkedDriver>` registry
            // entry survives for a tick of de-registration. Read accessor
            // calls after this point return `None` and the caller falls
            // through to the next cascade step (slow store).
            //
            // Single lock acquisition (rust-crate L2: parking_lot is
            // cheap-but-not-free; one mutex acquire instead of three).
            // Clearing `pin_permits` releases all PinBudget permits back
            // to the global pool — load-bearing for the anti-#203 cap.
            {
                let mut pin_state = pin_for_task.lock();
                pin_state.chunks.clear();
                pin_state.total_bytes = 0;
                pin_state.pin_permits.clear();
            }
            // Send commit result. The Receiver may have been dropped
            // (caller didn't care about the result, or panic'd); ignore
            // the send-error in that case — the result is logged below
            // on Err for diagnostic completeness.
            if let Err(send_err) = completion_tx.send(task_result.clone()) {
                trace!(
                    target: "nativelink_store::chunked",
                    "completion_tx receiver dropped before driver finished; result was: {:?}",
                    send_err,
                );
            }
            // Load-bearing for the §6.7 trigger (a) regression test:
            // signals the recv loop has returned. MUST be the last
            // statement before the spawned future returns.
            loop_exited_for_task.store(true, Ordering::Release);
        });

        (
            Self {
                digest,
                chunks_received,
                chunks_committed,
                loop_exited,
                completion_rx: parking_lot::Mutex::new(Some(completion_rx)),
                pin,
                expected_size,
                _handle: handle,
            },
            tx,
        )
    }

    /// Take the completion receiver. Returns `None` on the second call;
    /// upstream code is expected to await the result exactly once.
    /// Returns `Err(Code::Internal)` if the spawned task dropped the
    /// Sender without sending (driver-task panic).
    pub async fn await_completion(&self) -> Result<ChunkedCommitResult, Error> {
        let rx = self
            .completion_rx
            .lock()
            .take()
            .ok_or_else(|| make_err!(Code::Internal, "ChunkedDriver::await_completion called twice"))?;
        match rx.await {
            Ok(result) => result,
            Err(_recv_err) => Err(make_err!(
                Code::Internal,
                "ChunkedDriver task ended without signalling commit result (driver task panicked or aborted)"
            )),
        }
    }

    /// Observation hook. Phase 2.5+ will export this as
    /// `chunked_chunks_received_total{digest=...}` per-blob.
    #[must_use]
    pub fn chunks_received(&self) -> u64 {
        self.chunks_received.load(Ordering::Relaxed)
    }

    /// Observation hook. Phase 2.5+ will export this as
    /// `chunked_chunks_committed_total{digest=...}` per-blob.
    #[must_use]
    pub fn chunks_committed(&self) -> u64 {
        self.chunks_committed.load(Ordering::Relaxed)
    }

    /// Observation hook for the §6.7 trigger (a) regression test:
    /// returns `true` once the spawned task's recv loop has returned.
    /// Phase 2 may also use this for a graceful shutdown drain check.
    #[must_use]
    pub fn loop_exited(&self) -> bool {
        self.loop_exited.load(Ordering::Acquire)
    }

    /// Read-only accessor used by logging / metric labels.
    #[must_use]
    pub fn digest(&self) -> &DigestInfo {
        &self.digest
    }

    /// Phase 2.5 read-cascade hook (design §6.3 step 2 — the
    /// `failed_writes` per-chunk pin). Returns `Some(Bytes)` containing
    /// the assembled byte range `[byte_offset, byte_offset+byte_length)`
    /// if every covering chunk is currently pinned in memory; returns
    /// `None` otherwise.
    ///
    /// `None` cases (each one falls through to the next cascade step in
    /// `FastSlowStore::get_part`):
    /// 1. The driver has already committed (`chunks` cleared) — the
    ///    blob is now on the canonical CAS path, served by the slow
    ///    store.
    /// 2. The requested range is not yet fully covered by landed
    ///    chunks — partial coverage is intentionally NOT served (per
    ///    design §6.3 the cascade is per-chunk and the slow-store path
    ///    can serve already-pwritten chunks at offset, but Phase 2.5
    ///    keeps that behind the same kill-switch — we only serve from
    ///    the pin when it has the WHOLE range).
    /// 3. The requested range overruns the declared blob size — caller
    ///    bug; falls through so the slow store can return its own
    ///    well-defined OutOfRange / NotFound.
    ///
    /// **Why all-or-nothing for the requested range:** a partial result
    /// would force `get_part` to compose pin-bytes + slow-store-bytes
    /// for a single read. Per the design's per-chunk-independence the
    /// composition is legal, but Phase 2.5's wire-up is intentionally
    /// the simplest version that still demonstrates the cascade — only
    /// fully-covered ranges short-circuit. Composition is left for a
    /// later phase (or for the caller's existing buf_channel writer to
    /// stitch when a future phase splits the request).
    ///
    /// Holds the `parking_lot::Mutex` for the duration of the assembly,
    /// which is `O(chunks_in_range)` slice operations — bounded by
    /// `ceil(byte_length / CHUNK_SIZE)` in production
    /// (worst case ~256 entries for the 256 MiB
    /// `MAX_CHUNKED_BLOB_SIZE`). Never crosses an `.await`.
    #[must_use]
    pub fn try_get_chunk_from_pin(
        &self,
        byte_offset: u64,
        byte_length: u64,
    ) -> Option<Bytes> {
        // Range overruns the blob → caller bug; fall through.
        let end_offset = byte_offset.checked_add(byte_length)?;
        if end_offset > self.expected_size {
            return None;
        }
        // Empty range → empty Bytes (defensive; production callers go
        // through the buf_channel which already short-circuits empty).
        if byte_length == 0 {
            return Some(Bytes::new());
        }

        let pin = self.pin.lock();
        // Driver already committed and cleared the pin.
        if pin.chunks.is_empty() {
            return None;
        }

        // Walk the BTreeMap in offset order, skipping chunks that end
        // before our range and stopping when we have produced
        // `byte_length` bytes. Track expected-next-offset to detect
        // gaps mid-range — any gap means the range is not fully
        // covered.
        let mut assembled: Vec<Bytes> = Vec::new();
        let mut produced: u64 = 0;
        let mut cursor: u64 = byte_offset;
        for (&chunk_off, chunk_bytes) in &pin.chunks {
            let chunk_len = chunk_bytes.len() as u64;
            let chunk_end = chunk_off.saturating_add(chunk_len);
            // Skip chunks that end before our cursor.
            if chunk_end <= cursor {
                continue;
            }
            // Gap detected: the next chunk starts AFTER our cursor.
            // The requested range is not fully covered.
            if chunk_off > cursor {
                return None;
            }
            // Slice the chunk to the [cursor, end_offset) overlap.
            let slice_start = (cursor - chunk_off) as usize;
            let want = (end_offset - cursor).min(chunk_end - cursor) as usize;
            let slice_end = slice_start + want;
            assembled.push(chunk_bytes.slice(slice_start..slice_end));
            produced += want as u64;
            cursor += want as u64;
            if produced == byte_length {
                break;
            }
        }
        // Trailing-gap detection: we walked off the end of the BTreeMap
        // without filling the range → not fully covered.
        if produced != byte_length {
            return None;
        }
        // Single-chunk fast path: avoid concatenation alloc when the
        // request fit entirely in one pinned chunk.
        if assembled.len() == 1 {
            return Some(assembled.into_iter().next().expect("len==1"));
        }
        // Multi-chunk: concatenate into one contiguous Bytes. One
        // BytesMut alloc + one copy per chunk; bounded by
        // ceil(byte_length / CHUNK_SIZE) chunks (~256 worst-case for
        // the 256 MiB MAX_CHUNKED_BLOB_SIZE).
        let mut out = bytes::BytesMut::with_capacity(byte_length as usize);
        for slice in assembled {
            out.extend_from_slice(&slice);
        }
        Some(out.freeze())
    }

    /// Diagnostic accessor: returns the total bytes currently held in
    /// the in-memory pin. Used by tests + future metric wiring.
    #[must_use]
    pub fn pinned_bytes(&self) -> u64 {
        self.pin.lock().total_bytes
    }

    /// Diagnostic accessor: returns the count of chunks currently
    /// pinned in memory. Used by tests to assert the post-commit drop.
    #[must_use]
    pub fn pinned_chunk_count(&self) -> usize {
        self.pin.lock().chunks.len()
    }
}

/// The per-blob driver loop. Pulled out of `spawn_driver` so the body
/// can early-return via `?` and the post-loop completion-tx + `loop_exited`
/// flag can be set from the SAME closure regardless of the result.
///
/// On per-chunk error: returns `Err`, the Sender drops, the upstream
/// handler sees `Code::*`. The driver does NOT auto-discard mid-stream
/// — chunks already on disk remain so a retry CAN reuse them (Phase
/// 2.x retry path will be wired in a later phase). For Phase 2.3 the
/// caller is expected to issue `discard_chunked` on the FilesystemStore
/// directly when its commit returns Err.
///
/// `expected_chunk_count == 0` corresponds to a zero-byte blob; the
/// driver still expects a single `finish` chunk (with empty bytes) and
/// commits with `expected_size = 0`.
async fn run_driver<Fe: FileEntry>(
    mut rx: mpsc::Receiver<ChunkWork>,
    filesystem_store: Arc<FilesystemStore<Fe>>,
    digest: DigestInfo,
    expected_size: u64,
    expected_chunk_count: usize,
    sidecar: Arc<Mutex<SidecarState>>,
    pin: Arc<Mutex<ChunkPin>>,
    chunks_received: Arc<AtomicU64>,
    chunks_committed: Arc<AtomicU64>,
    per_chunk_timeout: core::time::Duration,
) -> Result<ChunkedCommitResult, Error> {
    while let Some(work) = rx.recv().await {
        chunks_received.fetch_add(1, Ordering::Relaxed);
        let ChunkWork {
            chunk_offset,
            chunk_bytes,
            finish,
            _permit,
            _pin_permit,
        } = work;

        let chunk_len = chunk_bytes.len();
        trace!(
            target: "nativelink_store::chunked",
            ?digest,
            chunk_offset,
            chunk_len,
            finish,
            "driver received chunk",
        );

        // Per design §8.3.1: per-chunk SHA-256 is verified at the RPC
        // handler (admission) on `spawn_blocking` per #213 perf-opt
        // NMA1, so the driver does not re-verify and `ChunkWork` does
        // not carry the per-chunk SHA-256 (#395). The end-to-end SHA-256
        // verify in `commit_chunked_to_holding` (against `.holding`,
        // BEFORE the canonical-path rename) covers the lying-producer
        // case; the admission-side per-chunk verify covers wire
        // corruption.

        // Write the chunk via the FilesystemStore adapter (which is
        // already on `spawn_blocking` internally — see
        // `chunked_filesystem::write_chunk_at_offset`).
        // The clone is one `Arc` bump (Bytes is ref-counted); the
        // landed-chunk pin populated below shares the same buffer.
        //
        // #213 NMA2 fixup: per-chunk wall-clock bound. Without this,
        // a wedged slow tier (ZFS lockup, kernel I/O hang) blocks
        // the driver task indefinitely, defeating §6.7 trigger (b)'s
        // "best-effort drain bounded by the graceful-shutdown
        // deadline" promise. The timeout bounds the AWAIT of the
        // spawn_blocking JoinHandle (uncancellable per tokio
        // contract); the underlying syscall may still complete or
        // fail later, but the driver returns control to its caller
        // (and the JoinHandleDropGuard / shutdown path) within the
        // bound.
        let bytes_for_write = chunk_bytes.clone();
        let write_fut = filesystem_store.write_chunk_at_offset(&digest, chunk_offset, bytes_for_write);
        // #413 (200-540 MB blob cascade after cap=64) pre-bump
        // instrumentation: measure per-chunk back-edge wall-clock so the
        // `PER_BLOB_MPSC_CAP` doc-comment's inferred "~25-50 MB/s back-
        // edge drain" estimate can be replaced with a direct production
        // trace. The probe spans the entire await of `write_fut`
        // (spawn_blocking pool queue + per-blob async-mutex acquire in
        // `chunked_filesystem::write_chunk_at_offset` + the actual pwrite
        // syscall) — that's the wall-clock the chunker → driver back-edge
        // actually pays, NOT just the syscall. The field is named
        // `back_edge_ms` to reflect this.
        let pwrite_started_at = std::time::Instant::now();
        let write_result = match tokio::time::timeout(per_chunk_timeout, write_fut).await {
            Ok(res) => res,
            Err(_elapsed) => {
                warn!(
                    target: "nativelink_store::chunked",
                    ?digest,
                    chunk_offset,
                    chunk_len,
                    timeout_ms = per_chunk_timeout.as_millis() as u64,
                    "chunked driver: per-chunk pwrite exceeded timeout; aborting blob \
                     (slow tier wedged?)",
                );
                // #213 reviewer M8: record timeout + warn if rate
                // exceeds threshold (helps SREs spot blocking-pool
                // thread leak before catastrophic saturation).
                record_pwrite_timeout_and_maybe_warn();
                // Per #213 reviewer M2: bound the post-timeout discard
                // by [`DISCARD_AFTER_FAILURE_TIMEOUT`]. The original
                // `discard_chunked(&digest).await` was unbounded and a
                // wedged slow tier (the same scenario that motivated
                // the per-chunk pwrite timeout) would hang the driver
                // task forever, defeating §6.7 trigger (b)'s
                // "best-effort drain bounded by deadline" promise.
                match tokio::time::timeout(
                    DISCARD_AFTER_FAILURE_TIMEOUT,
                    filesystem_store.discard_chunked(&digest),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(discard_err)) => error!(
                        target: "nativelink_store::chunked",
                        ?digest,
                        ?discard_err,
                        "chunked driver: discard after per-chunk pwrite timeout also failed",
                    ),
                    Err(_) => error!(
                        target: "nativelink_store::chunked",
                        ?digest,
                        timeout_ms = DISCARD_AFTER_FAILURE_TIMEOUT.as_millis() as u64,
                        "chunked driver: discard after per-chunk pwrite timeout also timed out; \
                         partial persists until next FilesystemStore::new sweep \
                         (#213 reviewer M2: driver-task bound, GC abandoned)",
                    ),
                }
                return Err(make_err!(
                    Code::DeadlineExceeded,
                    "chunked write per-chunk pwrite exceeded {per_chunk_timeout:?} for digest {digest} offset {chunk_offset}"
                ));
            }
        };
        if let Err(write_err) = write_result {
            warn!(
                target: "nativelink_store::chunked",
                ?digest,
                chunk_offset,
                chunk_len,
                ?write_err,
                "chunked driver: per-chunk pwrite failed; aborting blob",
            );
            // Abort the blob: discard the partial. Best-effort;
            // discard errors are logged but not surfaced (the original
            // write error is the operator-actionable one).
            //
            // Per #213 reviewer M2: bound the discard wall-clock so a
            // wedged slow tier cannot hang the driver task here either
            // (same rationale as the timeout-arm above).
            match tokio::time::timeout(
                DISCARD_AFTER_FAILURE_TIMEOUT,
                filesystem_store.discard_chunked(&digest),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(discard_err)) => error!(
                    target: "nativelink_store::chunked",
                    ?digest,
                    ?discard_err,
                    "chunked driver: discard after per-chunk write failure also failed",
                ),
                Err(_) => error!(
                    target: "nativelink_store::chunked",
                    ?digest,
                    timeout_ms = DISCARD_AFTER_FAILURE_TIMEOUT.as_millis() as u64,
                    "chunked driver: discard after per-chunk write failure also timed out; \
                     partial persists until next FilesystemStore::new sweep \
                     (#213 reviewer M2: driver-task bound, GC abandoned)",
                ),
            }
            return Err(write_err);
        }
        chunks_committed.fetch_add(1, Ordering::Relaxed);

        // #413 (200-540 MB blob cascade after cap=64) pre-bump
        // observability: emit a `warn!` ONLY when the per-chunk back-edge
        // exceeds the 50 ms threshold so production traces can replace
        // the inferred ~25-50 MB/s drain ceiling cited in the
        // `PER_BLOB_MPSC_CAP` doc comment with a direct measurement.
        //
        // Why `warn!` (not `debug!`): the workspace pins
        // `tracing = { features = ["release_max_level_info"] }` (see
        // `Cargo.toml:101`), which compile-time eliminates `debug!` /
        // `trace!` calls in release builds — a `debug!` probe here would
        // be dead code in production despite any `RUST_LOG` setting. A
        // CLAUDE.md `warn!` ("performance anomalies — slow ops,
        // contention, early evictions") preserves the tail signal in
        // release with zero log-volume risk in steady state: at the
        // ~25-50 MB/s drain ceiling and a 1 MiB chunk, the typical back-
        // edge is 20-40 ms (no log); a >50 ms back-edge means one of:
        // (a) per-blob async-mutex hold-time spike (acquire blocks
        // BEFORE spawn_blocking, so the wall-clock includes mutex
        // contention from concurrent same-digest writes — most likely
        // attribution under load); (b) spawn_blocking pool queue depth
        // (operator-visible via runtime metrics); (c) slow-tier pwrite
        // contention (ZFS recordsize mismatch / arc pressure / disk
        // write amplification). Each is operator-actionable.
        //
        // Why 50 ms threshold: chosen at the high end of the inferred
        // healthy range (40 ms = 1 MiB / 25 MB/s) plus a 25 % cushion
        // so a healthy production cluster emits zero-to-few of these,
        // and the cap-tuning iteration sees a clean signal once the
        // bottleneck is exercised. Adjust based on observed steady-
        // state if this turns out to be too noisy or too quiet.
        //
        // Coverage caveat: this probe ONLY fires on the success path.
        // The timeout arm (line ~859, `record_pwrite_timeout_and_maybe_warn`)
        // and the write-error arm (line ~908) emit their own `warn!`s
        // without `back_edge_ms`. That is acceptable for cap-tuning:
        // failures already log loudly; outlier-on-success is the gap.
        let back_edge_ms = pwrite_started_at.elapsed().as_millis() as u64;
        if back_edge_ms > 50 {
            warn!(
                target: "nativelink_store::chunked",
                back_edge_ms,
                ?digest,
                offset = chunk_offset,
                chunk_bytes = chunk_len,
                "per-chunk back-edge drain exceeded 50ms — investigate \
                 ZFS / mutex / spawn_blocking queue split (#413 \
                 (200-540 MB blob cascade after cap=64) Option A probe)",
            );
        }

        // Populate the in-memory pin (design §6.2 / §6.3 step 2).
        // Reachable from `ChunkedDriver::try_get_chunk_from_pin` — the
        // Phase 2.5 read cascade hook in `FastSlowStore::get_part`.
        // Cleared by the spawning task on driver exit (commit success
        // OR failure). Insert AFTER the pwrite has succeeded so the pin
        // never advertises bytes that aren't on disk yet — preserves the
        // step-2-then-step-3 ordering of the read cascade (a reader that
        // looks the pin up while the slow-store rename is in flight will
        // see the bytes before the slow store does, but never the other
        // way around).
        {
            let mut pin_state = pin.lock();
            // BTreeMap::insert returns the previous value — on a
            // duplicate offset (the producer-protocol violation already
            // warned about below) we replace and adjust total_bytes
            // accordingly to keep the cached total honest.
            if let Some(prev) = pin_state
                .chunks
                .insert(chunk_offset, chunk_bytes.clone())
            {
                pin_state.total_bytes =
                    pin_state.total_bytes.saturating_sub(prev.len() as u64);
            }
            pin_state.total_bytes =
                pin_state.total_bytes.saturating_add(chunk_len as u64);
            // #212 Phase 2.5/2.7 fixup B1: transfer the PinBudget permit
            // (if any) from the ChunkWork into the pin. The permit
            // remains held until the pin clears post-commit (driver
            // exit, see `pin_for_task.lock().chunks.clear()`), at which
            // point the Vec drops and the global pinned-bytes budget
            // recovers.
            if let Some(perm) = _pin_permit {
                pin_state.pin_permits.push(perm);
            }
        }

        // Update the in-memory sidecar bitmap. parking_lot::Mutex
        // critical section is just an insert + a flag set; never
        // crosses an `.await`.
        {
            let mut state = sidecar.lock();
            let inserted = state.landed_offsets.insert(chunk_offset);
            if !inserted {
                // Duplicate offset — this is a producer protocol
                // error (the WriteChunked schema says each chunk has a
                // unique offset). We tolerate the pwrite (idempotent
                // at the syscall level) but log a warning.
                warn!(
                    target: "nativelink_store::chunked",
                    ?digest,
                    chunk_offset,
                    "chunked driver: duplicate offset received; producer protocol violation",
                );
            }
            if finish {
                state.finish_seen = true;
            }
        }

        // If finish has been seen AND every expected offset has landed,
        // attempt commit. We re-check inside the lock to avoid racing
        // with another finish-arrival (which shouldn't happen since
        // the producer only sends one finish chunk, but we are
        // defensive).
        let ready_to_commit = {
            let state = sidecar.lock();
            state.finish_seen && state.landed_offsets.len() == expected_chunk_count
        };
        if ready_to_commit {
            return commit_and_verify(&filesystem_store, &digest, expected_size).await;
        }
    }

    // The mpsc closed without commit triggering. Two cases:
    //  (i) `finish_seen == false`: upstream RPC dropped before the
    //      final chunk arrived. The partial is left on disk for
    //      `prune_temp_path` to GC on next FilesystemStore::new (per
    //      Q7=(c) drop-partial-recoverable-read).
    //  (ii) `finish_seen == true` but coverage incomplete: producer
    //       protocol violation (sent `finish` before all chunks). The
    //       partial is ALSO left on disk; the operator-visible error
    //       below tells them why.
    let state = sidecar.lock();
    if state.finish_seen {
        let landed = state.landed_offsets.len();
        warn!(
            target: "nativelink_store::chunked",
            ?digest,
            landed,
            expected_chunk_count,
            "chunked driver: finish observed but coverage incomplete; not committing"
        );
        Err(make_err!(
            Code::InvalidArgument,
            "chunked write protocol violation: finish observed with {landed}/{expected_chunk_count} chunks"
        ))
    } else {
        debug!(
            target: "nativelink_store::chunked",
            ?digest,
            received = chunks_received.load(Ordering::Relaxed),
            "chunked driver: upstream dropped without finish; not committing",
        );
        Err(make_err!(
            Code::Aborted,
            "chunked write upstream dropped without finish; not committing"
        ))
    }
}

/// Run the final commit + end-to-end SHA-256 verify per design §8.3.1.
///
/// **B1 fixup (two-stage rename):**
/// 1. `commit_chunked` — rename `<digest>.partial` → `<digest>.holding`
///    (NOT canonical CAS path) + length validation. The in-flight
///    tracker entry is held by the FilesystemStore until step 4
///    completes.
/// 2. Re-open the `.holding` file on `spawn_blocking`, stream-hash it
///    with SHA-256.
/// 3. If the SHA-256 matches: `finalize_holding` atomically renames
///    `.holding` → `<digest>` (canonical) and chmods 0o555. The
///    in-flight tracker entry is removed AFTER the rename succeeds
///    (M-perf-3 + B1 coupling).
/// 4. If mismatched: `unlink_holding` removes the `.holding` file and
///    `discard_chunked` drops the in-flight entry. Returns
///    InvalidArgument.
///
/// **Why two-stage:** before the fixup, the rename landed at the
/// canonical CAS path BEFORE the SHA-256 verify. A handler-future
/// cancellation between rename and verify aborted the spawned task
/// (the only `Arc<ChunkedDriver>` held by the cancellable handler
/// dropped → JoinHandleDropGuard fired), leaving an unverified file
/// at the canonical CAS path. Subsequent reads returned wrong-but-named
/// bytes — CAS poisoning. With two-stage rename, an abort can at worst
/// leave a `.holding` file that `prune_holding_partials` GCs on next
/// `FilesystemStore::new`; the canonical path is only created AFTER
/// hash match.
///
/// Note: this runs the SHA-256 on `spawn_blocking` per #213 perf-opt
/// NMA1; SHA-256 of a multi-MiB blob at line rate burns CPU cycles
/// that should not block a tokio worker.
/// 2026-05-02 diagnostic for the production chunked-write SHA mismatch
/// burst. Capped at 64 preserved files per process boot to bound disk
/// usage; subsequent mismatches just log+unlink as before. Returns the
/// preserved path on success (or `None` if cap reached / copy failed).
///
/// The preserved file is left at `<content_path>/d/<XX>/<digest>.<ts>.diag`
/// — same shard directory as the holding file (cheap rename, atomic on
/// any filesystem) so we can read it back via `sudo` without ZFS-cross-
/// dataset issues.
static DIAG_PRESERVED_COUNT: AtomicU64 = AtomicU64::new(0);
const DIAG_PRESERVE_CAP: u64 = 64;

async fn preserve_mismatched_holding_for_diag<Fe: FileEntry>(
    filesystem_store: &Arc<FilesystemStore<Fe>>,
    digest: &DigestInfo,
) -> Option<std::path::PathBuf> {
    if DIAG_PRESERVED_COUNT.fetch_add(1, Ordering::Relaxed) >= DIAG_PRESERVE_CAP {
        return None;
    }
    let holding = filesystem_store.holding_content_path(digest);
    let mut diag = holding.clone();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    diag.set_file_name(format!("{digest}.{ts}.diag"));
    let from = holding.clone();
    let to = diag.clone();
    let copy_res = tokio::task::spawn_blocking(move || std::fs::copy(&from, &to)).await;
    match copy_res {
        Ok(Ok(_n)) => Some(diag),
        Ok(Err(io_err)) => {
            warn!(
                target: "nativelink_store::chunked",
                ?digest,
                ?io_err,
                "diag preserve: copy holding -> diag failed",
            );
            None
        }
        Err(join_err) => {
            warn!(
                target: "nativelink_store::chunked",
                ?digest,
                ?join_err,
                "diag preserve: spawn_blocking join failed",
            );
            None
        }
    }
}

async fn commit_and_verify<Fe: FileEntry>(
    filesystem_store: &Arc<FilesystemStore<Fe>>,
    digest: &DigestInfo,
    expected_size: u64,
) -> Result<ChunkedCommitResult, Error> {
    // Step 1: rename to holding path + length check.
    if let Err(commit_err) = filesystem_store.commit_chunked(digest, expected_size).await {
        warn!(
            target: "nativelink_store::chunked",
            ?digest,
            ?commit_err,
            "chunked driver: commit_chunked (rename to holding) failed; discarding partial"
        );
        // Discard the partial so we don't leak. commit_chunked_to_holding
        // leaves the temp file in place on length-mismatch per spec.
        // Per #213 reviewer M2 (round-2 MAJOR-A): bound the discard
        // wall-clock so a wedged slow tier cannot hang the driver task
        // here either (post-error cleanup contract).
        match tokio::time::timeout(
            DISCARD_AFTER_FAILURE_TIMEOUT,
            filesystem_store.discard_chunked(digest),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(discard_err)) => error!(
                target: "nativelink_store::chunked",
                ?digest,
                ?discard_err,
                "chunked driver: discard after commit failure also failed",
            ),
            Err(_) => error!(
                target: "nativelink_store::chunked",
                ?digest,
                timeout_ms = DISCARD_AFTER_FAILURE_TIMEOUT.as_millis() as u64,
                "chunked driver: discard after commit failure also timed out; \
                 partial persists until next FilesystemStore::new sweep \
                 (#213 reviewer M2 round-2 MAJOR-A: driver-task bound, GC abandoned)",
            ),
        }
        return Err(commit_err);
    }

    // Step 2: end-to-end SHA-256 over the holding file.
    //
    // Hypothesis-B fix (2026-05-12): formerly dispatched via
    // `tokio::task::spawn_blocking`, which serializes submission on a
    // single `parking_lot::Mutex<Shared>` inside tokio's blocking-pool
    // spawner. Production `ChunkedShaCommit` rate ≈ ~470/s × ~30 ms
    // each (1 MiB hash chunks); each submit was one extra acquire of
    // that mutex while the per-chunk admit-side SHA + chunked pwrite
    // sites all fanned in to the SAME mutex — the dominant
    // `dispatch_ms` source identified in
    // `.claude/audits/blocking-pool-saturation-investigation-20260512.md`.
    // Routing CPU-bound stream-hash via `cpu_pool()` (a separate rayon
    // work-stealing pool with its own per-worker deque) eliminates the
    // submission contention. The work itself is still serialized on a
    // single rayon worker (we only need 1 thread per blob); the win is
    // OFF the spawn_blocking spawner mutex, not parallelism.
    let holding_path_pb = filesystem_store.holding_content_path(digest);
    let (sha_tx, sha_rx) =
        oneshot::channel::<Result<[u8; 32], std::io::Error>>();
    let path_for_pool = holding_path_pb.clone();
    cpu_pool().spawn(move || {
        // Stream via `std::io::Read` + `DigestHasher::update` to keep
        // peak memory at the read-buffer size only (a 100 MiB blob
        // would otherwise need 100 MiB of allocation up front).
        //
        // Buffer = 1 MiB to match ZFS recordsize=1M on
        // `fast/nativelink/work` (perf-optimizer MINOR-1 fixup);
        // avoids 16× syscalls per record vs the previous 64 KiB.
        //
        // #228 fix: use the process-wide default digest hasher
        // (`blake3` in production per `default_digest_hash_function`
        // in buildcache-native.json5 / worker.json5). The previous
        // hardcoded Sha256 mismatched every BLAKE3-named declared
        // digest at the e2e check, rejecting 100% of >=1 MiB writes
        // in production.
        let result = (|| -> Result<[u8; 32], std::io::Error> {
            use std::io::Read;
            let mut file = std::fs::File::open(&path_for_pool)?;
            let mut hasher = default_digest_hasher_func().hasher();
            let mut buf = vec![0u8; 1024 * 1024];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            let info = hasher.finalize_digest();
            Ok(**info.packed_hash())
        })();
        let _ = sha_tx.send(result);
    });
    let computed = sha_rx
        .await
        .map_err(|_| {
            make_err!(
                Code::Internal,
                "cpu_pool worker dropped before sending commit-time SHA-256"
            )
        })?
        .map_err(|io_err| {
            make_err!(
                Code::Internal,
                "failed to re-read holding file for SHA-256 verification: {io_err:?}"
            )
        })?;

    // `packed_hash()` returns `&PackedHash`, which derefs to `&[u8; 32]`.
    // The double-deref + Copy gives an owned `[u8; 32]` for comparison.
    let declared: [u8; 32] = **digest.packed_hash();
    if computed != declared {
        // End-to-end hash mismatch — the per-chunk hashes all passed
        // but the assembled file does not match the declared digest.
        // The .holding file is at content_path/d/XX/<digest>.holding;
        // the canonical CAS path is NOT yet created. Stage-2 cleanup:
        // unlink the holding file, drop the in-flight tracker entry.
        //
        // 2026-05-02 diagnostic (#212 v4.5 fix-forward): production is
        // hitting this path on every >=1 MiB write since deploy of the
        // CasExtensions routing fix. Unit tests with the production
        // handler+filesystem stack PASS for multi-chunk geometry, so
        // bytes diverge somewhere upstream of the handler. Preserve the
        // holding file under `<content_path>/d/<XX>/<digest>.<ts>.diag`
        // so we can inspect actual on-disk bytes vs declared offline.
        // RATE-LIMITED: only the FIRST 64 mismatches per process boot
        // are preserved (avoids filling /srv/bulk if the bug fires
        // continuously). Subsequent mismatches still log + unlink.
        let preserved_path = preserve_mismatched_holding_for_diag(filesystem_store, digest).await;
        warn!(
            target: "nativelink_store::chunked",
            ?digest,
            computed = ?hex::encode(computed),
            declared = ?hex::encode(declared),
            preserved = ?preserved_path,
            "chunked driver: end-to-end SHA-256 mismatch; unlinking holding file"
        );
        // Per #213 reviewer M2 (round-2 MAJOR-A): bound the unlink/
        // discard wall-clock so a wedged slow tier cannot hang the
        // driver task on the mismatch cleanup path (post-error
        // cleanup contract).
        match tokio::time::timeout(
            DISCARD_AFTER_FAILURE_TIMEOUT,
            filesystem_store.unlink_holding(digest),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(unlink_err)) => error!(
                target: "nativelink_store::chunked",
                ?digest,
                ?unlink_err,
                "chunked driver: failed to unlink holding file after SHA-256 mismatch",
            ),
            Err(_) => error!(
                target: "nativelink_store::chunked",
                ?digest,
                timeout_ms = DISCARD_AFTER_FAILURE_TIMEOUT.as_millis() as u64,
                "chunked driver: unlink_holding after SHA-256 mismatch timed out; \
                 holding file persists until next FilesystemStore::new sweep \
                 (#213 reviewer M2 round-2 MAJOR-A: driver-task bound, GC abandoned)",
            ),
        }
        match tokio::time::timeout(
            DISCARD_AFTER_FAILURE_TIMEOUT,
            filesystem_store.discard_chunked(digest),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(discard_err)) => error!(
                target: "nativelink_store::chunked",
                ?digest,
                ?discard_err,
                "chunked driver: failed to discard in-flight state after SHA-256 mismatch",
            ),
            Err(_) => error!(
                target: "nativelink_store::chunked",
                ?digest,
                timeout_ms = DISCARD_AFTER_FAILURE_TIMEOUT.as_millis() as u64,
                "chunked driver: discard after SHA-256 mismatch timed out; \
                 in-flight entry persists until next FilesystemStore::new sweep \
                 (#213 reviewer M2 round-2 MAJOR-A: driver-task bound, GC abandoned)",
            ),
        }
        return Err(make_err!(
            Code::InvalidArgument,
            "chunked write end-to-end SHA-256 mismatch for digest {digest}"
        ));
    }

    // Step 3: SHA-256 verified — atomically rename .holding → canonical.
    // The in-flight tracker entry is removed inside finalize_holding
    // ONLY after the rename succeeds. This couples with M-perf-3:
    // until the rename completes, the in-flight entry's
    // `Arc<ChunkInProgress>` is alive, so a handler-future cancellation
    // can't drop the partial state mid-finalize.
    if let Err(finalize_err) = filesystem_store.finalize_holding(digest).await {
        error!(
            target: "nativelink_store::chunked",
            ?digest,
            ?finalize_err,
            "chunked driver: finalize_holding (stage 2 rename) failed; cleaning up holding"
        );
        // Best-effort cleanup: unlink the holding file (which may still
        // be there if the rename failed before completing) and drop the
        // in-flight tracker entry. Per #213 reviewer M2 (round-2
        // MAJOR-A): bound the unlink/discard wall-clock so a wedged
        // slow tier cannot hang the driver task on the finalize
        // cleanup path (post-error cleanup contract).
        match tokio::time::timeout(
            DISCARD_AFTER_FAILURE_TIMEOUT,
            filesystem_store.unlink_holding(digest),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(unlink_err)) => warn!(
                target: "nativelink_store::chunked",
                ?digest,
                ?unlink_err,
                "chunked driver: failed to unlink holding after finalize failure",
            ),
            Err(_) => warn!(
                target: "nativelink_store::chunked",
                ?digest,
                timeout_ms = DISCARD_AFTER_FAILURE_TIMEOUT.as_millis() as u64,
                "chunked driver: unlink_holding after finalize failure timed out; \
                 holding file persists until next FilesystemStore::new sweep \
                 (#213 reviewer M2 round-2 MAJOR-A: driver-task bound, GC abandoned)",
            ),
        }
        match tokio::time::timeout(
            DISCARD_AFTER_FAILURE_TIMEOUT,
            filesystem_store.discard_chunked(digest),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(discard_err)) => warn!(
                target: "nativelink_store::chunked",
                ?digest,
                ?discard_err,
                "chunked driver: failed to discard in-flight state after finalize failure",
            ),
            Err(_) => warn!(
                target: "nativelink_store::chunked",
                ?digest,
                timeout_ms = DISCARD_AFTER_FAILURE_TIMEOUT.as_millis() as u64,
                "chunked driver: discard after finalize failure timed out; \
                 in-flight entry persists until next FilesystemStore::new sweep \
                 (#213 reviewer M2 round-2 MAJOR-A: driver-task bound, GC abandoned)",
            ),
        }
        return Err(finalize_err);
    }

    info!(
        target: "nativelink_store::chunked",
        ?digest,
        size = expected_size,
        "chunked driver: blob committed + SHA-256 verified (two-stage rename)"
    );
    Ok(ChunkedCommitResult {
        committed_size: expected_size,
    })
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use bytes::Bytes;
    use nativelink_config::stores::FilesystemSpec;
    use nativelink_macro::nativelink_test;
    use nativelink_util::common::DigestInfo;
    use sha2::{Digest as _, Sha256};

    use super::super::chunk_budget::{ChunkBudget, TOTAL_CHUNK_PERMITS};
    use super::{ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP};
    use crate::filesystem_store::{FileEntryImpl, FilesystemStore};

    /// Capacity constant pin: any change is ARCHITECTURAL — re-read
    /// design §4 Q4 + audit
    /// `.claude/audits/413-large-blob-cascade-design-20260511.md`
    /// (Option A) before bumping further. 256 was chosen to
    /// structurally cover the full `MAX_CHUNKED_BLOB_SIZE` (= 256 MiB)
    /// range so the chunker → driver back-edge cannot reject a
    /// mid-stream chunk for any blob ≤256 MiB; this eliminates 9 of 11
    /// large-blob cascade events observed in the 17-min post-deploy
    /// window at the prior cap of 64 (which itself was a 2026-05-11
    /// bump from the original §4 Q4 placeholder of 16). See the
    /// `PER_BLOB_MPSC_CAP` doc comment for the full historical
    /// rationale.
    #[test]
    fn per_blob_mpsc_cap_is_two_fifty_six() {
        assert_eq!(PER_BLOB_MPSC_CAP, 256);
    }

    /// Composite invariant per CLAUDE.md "Admission/Eviction/Pin
    /// Composability": the gate corner (per-blob mpsc full →
    /// `BackpressureSignal::PerBlobMpscFull`) MUST never fire ahead
    /// of the global ChunkBudget gate (→
    /// `BackpressureSignal::GlobalChunkBudgetExhausted`) when the
    /// system is under aggregate concurrent-blob pressure. Concretely:
    /// with `PER_BLOB_MPSC_CAP=256` (= 256 MiB per-blob in-flight) and
    /// `TOTAL_CHUNK_PERMITS=4096` (= 4 GiB global), the budget is
    /// exhausted after `4096 / 256 = 16` saturated blobs. The 17th
    /// concurrent saturated blob's first chunk admission MUST get
    /// `None` from `try_acquire_chunk` (which the gate at
    /// `chunked_write_handler.rs:1415-1432` translates to
    /// `BackpressureSignal::GlobalChunkBudgetExhausted`).
    ///
    /// **Composite triangle for this gate.** The OTHER two corners:
    /// - **Eviction:** the per-blob mpsc receiver inside the driver
    ///   task drains a chunk → its `_permit: OwnedSemaphorePermit`
    ///   drop releases one slot of the global ChunkBudget AND one
    ///   slot of the per-blob mpsc. (See `chunked_driver.rs::run_driver`
    ///   `recv()` arm + `ChunkWork`'s permit ownership in
    ///   `chunked_driver.rs:347-350`.)
    /// - **Pin:** the global ChunkBudget IS the pin — every in-flight
    ///   chunk holds exactly one `OwnedSemaphorePermit` for the lifetime
    ///   of the `ChunkWork` (`chunk_budget.rs:24-31`); no separate pin
    ///   path exists for the back-edge. (Per-blob `PinBudget` is a
    ///   distinct, complementary cap — see `pin_budget.rs`.)
    ///
    /// **Composite invariant:**
    /// `gate-active ⇒ explicit-eviction-fires-before-gate`. With both
    /// corners local to the budget, the gate firing is itself the
    /// admission of the global cap; the eviction corner is the chunk
    /// commit returning the permit. No TTL needed because the budget
    /// is reactive (no time-based release).
    ///
    /// **Falsification mutation:** comment out either (a) the
    /// `PER_BLOB_MPSC_CAP` constant being ≤ `TOTAL_CHUNK_PERMITS / 16`
    /// or (b) the `_permit: OwnedSemaphorePermit` drop on `ChunkWork`
    /// drop (release on permit-ownership return). Either mutation
    /// flips the dominance order between the two gates and this test
    /// red-fails with the specific message below.
    ///
    /// **Why test the budget primitive directly (not a 16-driver
    /// composition):** the dominance-order property is a pure
    /// ChunkBudget arithmetic invariant — `PER_BLOB_MPSC_CAP ×
    /// max_concurrent_blobs ≤ TOTAL_CHUNK_PERMITS`. Spinning up 16
    /// real drivers + 4096 real ChunkWorks wouldn't add coverage of
    /// this invariant (each driver's per-blob mpsc would be
    /// independent; the cross-blob interaction is mediated entirely
    /// by the budget). A pure-budget test exercises the SAME seam as
    /// production admission code: 16 saturated drivers on the wire
    /// would drain exactly 4096 permits via the `_permit` field on
    /// each ChunkWork — the same `try_acquire_chunk → None`
    /// transition this test asserts.
    #[test]
    fn global_chunk_budget_remains_4_gib_bound_at_cap_256() {
        // Sanity: the cap-of-256 × 16-blob invariant matches the
        // global TOTAL_CHUNK_PERMITS (= 4096). If a future bump moves
        // either constant without re-verifying this composite, this
        // assertion forces the conversation.
        assert_eq!(
            PER_BLOB_MPSC_CAP * 16,
            TOTAL_CHUNK_PERMITS,
            "PER_BLOB_MPSC_CAP × 16 saturated blobs MUST equal \
             TOTAL_CHUNK_PERMITS (4 GiB / 1 MiB chunk = 4096); \
             composite invariant violated: gate dominance order shifted"
        );

        let budget = ChunkBudget::new();

        // Saturate 16 blobs' worth (= 4096 permits = the entire budget).
        // Hold them in a Vec so they don't release until end-of-test.
        let mut held: Vec<tokio::sync::OwnedSemaphorePermit> =
            Vec::with_capacity(TOTAL_CHUNK_PERMITS);
        for blob in 0..16 {
            for chunk in 0..PER_BLOB_MPSC_CAP {
                let permit = budget.try_acquire_chunk().unwrap_or_else(|| {
                    panic!(
                        "composite invariant violated: gate fired before all 16 blobs \
                         saturated their per-blob caps (failed at blob {blob} chunk {chunk}); \
                         this proves PER_BLOB_MPSC_CAP × 16 > TOTAL_CHUNK_PERMITS — \
                         the per-blob cap dominates the global cap, which would let \
                         attacker-controlled per-blob saturation evade global pressure"
                    );
                });
                held.push(permit);
            }
        }
        assert_eq!(
            budget.available_chunks(),
            0,
            "after 16 × PER_BLOB_MPSC_CAP saturating acquires, global budget MUST be \
             empty; got {} permits available",
            budget.available_chunks()
        );

        // 17th concurrent blob's first-chunk admission MUST get None
        // (= GlobalChunkBudgetExhausted in the production gate at
        // chunked_write_handler.rs:1415-1432). It MUST NOT see a
        // per-blob mpsc full (which would only fire if the same blob's
        // mpsc filled up — a 17th blob has an empty mpsc by definition).
        assert!(
            budget.try_acquire_chunk().is_none(),
            "composite invariant violated: gate active without compensating \
             eviction/pin/TTL — the 17th saturated blob's first-chunk admission \
             must be rejected by the GLOBAL ChunkBudget (translated to \
             BackpressureSignal::GlobalChunkBudgetExhausted), NOT by the per-blob \
             mpsc (which is empty for a fresh blob); if this passes, \
             PER_BLOB_MPSC_CAP × concurrent-blobs has decoupled from \
             TOTAL_CHUNK_PERMITS"
        );

        // Drop all permits to release the budget for any subsequent
        // tests in the same process.
        held.clear();
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(bytes);
        let out = h.finalize();
        let mut a = [0u8; 32];
        a.copy_from_slice(&out);
        a
    }

    /// Build a real `FilesystemStore` rooted at a fresh per-test temp
    /// directory. Returns the store + content_path so tests can stat
    /// the final CAS file directly.
    async fn make_test_store() -> (
        std::sync::Arc<FilesystemStore<FileEntryImpl>>,
        String,
    ) {
        let base = std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
        let nonce: u64 = rand::random();
        let content_path = format!("{base}/{nonce}/chunked-driver-test/content");
        let temp_path = format!("{base}/{nonce}/chunked-driver-test/temp");
        let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path,
            eviction_policy: None,
            block_size: 1,
            ..Default::default()
        })
        .await
        .expect("FilesystemStore::new must succeed");
        (store, content_path)
    }

    /// Driver receives 3 chunks IN ORDER + finish → commits successfully
    /// AND the committed file's SHA-256 matches the digest.
    #[nativelink_test]
    async fn driver_in_order_chunks_then_finish_commits_with_sha256_verify() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 3;
        let total = (N * CHUNK) as u64;

        // Build the blob bytes + the matching digest so the SHA-256
        // verify path actually succeeds.
        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0xa0u8 + i as u8).take(CHUNK));
        }
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);

        let (store, content_path) = make_test_store().await;
        let budget = ChunkBudget::new();

        let (driver, tx) = ChunkedDriver::spawn_driver(
            store.clone(),
            digest,
            total,
            CHUNK,
            PER_BLOB_MPSC_CAP,
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            for i in 0..N {
                let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
                let permit = budget.try_acquire_chunk().expect("permit");
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    finish: i == N - 1,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await
                .expect("driver mpsc still alive");
            }
            drop(tx);
            let result = driver
                .await_completion()
                .await
                .expect("commit must succeed for hash-matching blob");
            assert_eq!(result.committed_size, total);
        })
        .await
        .expect("must not deadlock — in-order commit should be prompt");

        // Final file exists at content_path with correct length.
        let final_path = format!(
            "{}/{}/{:02x}/{}",
            content_path,
            crate::filesystem_store::DIGEST_FOLDER,
            digest.packed_hash()[0],
            digest
        );
        let meta = tokio::fs::metadata(&final_path)
            .await
            .expect("final file must exist");
        assert_eq!(meta.len(), total);
    }

    /// Driver receives 3 chunks OUT OF ORDER + finish → commits
    /// successfully (relies on Phase 2.1's pwrite-at-offset).
    #[nativelink_test]
    async fn driver_out_of_order_chunks_then_finish_commits_successfully() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 3;
        let total = (N * CHUNK) as u64;

        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0xb0u8 + i as u8).take(CHUNK));
        }
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);

        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();

        let (driver, tx) = ChunkedDriver::spawn_driver(
            store.clone(),
            digest,
            total,
            CHUNK,
            PER_BLOB_MPSC_CAP,
        );

        // Send order: 2, 0, 1 (finish on the LAST sent, which is offset 1).
        let order = [2usize, 0, 1];
        tokio::time::timeout(Duration::from_secs(5), async {
            for (idx, &i) in order.iter().enumerate() {
                let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
                let permit = budget.try_acquire_chunk().expect("permit");
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    finish: idx == order.len() - 1,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await
                .expect("send");
            }
            drop(tx);
            let result = driver
                .await_completion()
                .await
                .expect("out-of-order commit must succeed");
            assert_eq!(result.committed_size, total);
        })
        .await
        .expect("must not deadlock — out-of-order commit");
    }

    /// End-to-end SHA-256 mismatch: chunks are individually well-formed
    /// (sha256 placeholder) but the assembled blob does not match the
    /// digest's hash → driver returns InvalidArgument and unlinks the
    /// committed file.
    #[nativelink_test]
    async fn driver_e2e_sha256_mismatch_returns_invalid_argument_and_unlinks_file() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 2;
        let total = (N * CHUNK) as u64;

        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0xc0u8 + i as u8).take(CHUNK));
        }
        // Use a LIE — the digest's hash is not the actual blob hash.
        // commit_chunked still succeeds (length matches); end-to-end
        // SHA-256 verify fails.
        let lying_hash = [0xffu8; 32];
        let digest = DigestInfo::new(lying_hash, total);

        let (store, content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) = ChunkedDriver::spawn_driver(
            store.clone(),
            digest,
            total,
            CHUNK,
            PER_BLOB_MPSC_CAP,
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            for i in 0..N {
                let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
                let permit = budget.try_acquire_chunk().expect("permit");
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    finish: i == N - 1,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await
                .unwrap();
            }
            drop(tx);
            let err = driver
                .await_completion()
                .await
                .expect_err("e2e SHA-256 mismatch must surface as Err");
            assert_eq!(
                err.code,
                nativelink_error::Code::InvalidArgument,
                "e2e SHA-256 mismatch must be classified as InvalidArgument; got {err:?}"
            );
            let msg = format!("{err:?}");
            assert!(
                msg.contains("end-to-end SHA-256 mismatch"),
                "error must name the contract; got {msg}"
            );
        })
        .await
        .expect("must not deadlock — e2e mismatch path");

        // The committed file MUST have been unlinked.
        let final_path = format!(
            "{}/{}/{:02x}/{}",
            content_path,
            crate::filesystem_store::DIGEST_FOLDER,
            lying_hash[0],
            digest
        );
        let meta = tokio::fs::metadata(&final_path).await;
        assert!(
            meta.is_err(),
            "post-mismatch unlink must remove the file; stat should fail; got {meta:?}"
        );
    }

    /// Upstream drops the sender BEFORE finish → driver's await_completion
    /// returns Err(Aborted), partial NOT committed (no final file).
    #[nativelink_test]
    async fn driver_upstream_drop_before_finish_returns_aborted_no_commit() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = 2 * CHUNK as u64;
        let blob_hash = sha256(&vec![0xddu8; total as usize]);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        tokio::time::timeout(Duration::from_secs(5), async {
            // Send only one chunk (no finish).
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: 0,
                chunk_bytes: Bytes::from(vec![0xddu8; CHUNK]),
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .unwrap();
            // Drop tx without finish.
            drop(tx);
            let err = driver
                .await_completion()
                .await
                .expect_err("dropped-without-finish must surface as Err");
            assert_eq!(
                err.code,
                nativelink_error::Code::Aborted,
                "dropped-without-finish must be Aborted; got {err:?}"
            );
        })
        .await
        .expect("must not deadlock — drop-without-finish path");

        // No final file was produced.
        let final_path = format!(
            "{}/{}/{:02x}/{}",
            content_path,
            crate::filesystem_store::DIGEST_FOLDER,
            blob_hash[0],
            digest
        );
        let meta = tokio::fs::metadata(&final_path).await;
        assert!(
            meta.is_err(),
            "no final file must exist when finish never arrived; got {meta:?}"
        );
    }

    /// Driver Drop mid-stream (panic-safety belt per §6.7d):
    /// - sender is held alive elsewhere; we drop the driver,
    /// - the JoinHandleDropGuard MUST abort the spawned task,
    /// - the budget recovers (ChunkWork dropped, permit released).
    #[nativelink_test]
    async fn driver_drop_aborts_spawned_task_and_recovers_budget() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = CHUNK as u64;
        let blob_hash = sha256(&vec![0xeeu8; CHUNK]);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        // Send one chunk and wait for it to be observed.
        let permit = budget.try_acquire_chunk().expect("permit");
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from(vec![0xeeu8; CHUNK]),
            finish: false,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .expect("send");
        tokio::time::timeout(Duration::from_secs(2), async {
            while driver.chunks_received() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver must observe chunk within 2s");

        // Drop the driver while sender is still alive.
        drop(driver);

        // Budget recovers (the dropped ChunkWork's permit is released
        // through the spawned task being aborted + dropped).
        let recovered = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if budget.available_chunks() == super::super::chunk_budget::TOTAL_CHUNK_PERMITS {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        recovered.expect(
            "budget must return to full capacity after driver-drop — \
             JoinHandleDropGuard did not abort the spawned task (#212 §6.7d)",
        );
    }

    /// M-testing-1 fixup (deterministic mpsc-full): construct a
    /// `mpsc::channel(1)` with NO receiver-poll, fill the slot, then
    /// attempt a second `try_send` and assert it returns
    /// `Err(TrySendError::Full(returned))`. This is the exact path the
    /// admission code at `chunked_write_handler::admit_chunk` reaches
    /// on per-blob mpsc-full; the production path then converts the
    /// Full to a `Code::ResourceExhausted` carrying a
    /// `BackpressureSignal { reason: PerBlobMpscFull }`. Asserting the
    /// `try_send` mechanic deterministically (vs the integration-time
    /// burst test which races the driver drain) closes M-v3-3.
    ///
    /// The test does NOT spawn a driver — it just exercises the mpsc
    /// channel mechanic that the admission relies on. The
    /// `ChunkWork.permit` is a real budget permit so the drop semantics
    /// are exercised end-to-end.
    #[nativelink_test]
    async fn admission_per_blob_mpsc_full_returns_try_send_full_with_returned_work() {
        let budget = ChunkBudget::new();
        let (tx, rx) = tokio::sync::mpsc::channel::<ChunkWork>(1);

        // Fill the only slot with a permit-bearing ChunkWork.
        let permit_a = budget.try_acquire_chunk().expect("permit a");
        let work_a = ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from_static(b""),
            finish: false,
            _permit: permit_a,
            _pin_permit: None,
        };
        tx.try_send(work_a).expect("first try_send into capacity-1 mpsc must succeed");

        // Second try_send must fail Full because the receiver is never
        // polled (we deliberately keep `rx` alive but un-polled).
        let permit_b = budget.try_acquire_chunk().expect("permit b");
        let work_b = ChunkWork {
            chunk_offset: 4096,
            chunk_bytes: Bytes::from_static(b""),
            finish: false,
            _permit: permit_b,
            _pin_permit: None,
        };
        let err = tx
            .try_send(work_b)
            .expect_err(
                "second try_send into a full capacity-1 mpsc MUST return Err(Full) — \
                 if this passes, M-v3-3's mpsc-full admission rejection path is dead code",
            );
        match err {
            tokio::sync::mpsc::error::TrySendError::Full(returned) => {
                // The dropped `returned` releases its OwnedSemaphorePermit
                // on the budget — the production code relies on this
                // for reverse-release per §13.1.1.
                assert_eq!(
                    returned.chunk_offset, 4096,
                    "the returned work must be the one we just attempted to send; \
                     got chunk_offset={}",
                    returned.chunk_offset
                );
                drop(returned);
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                panic!(
                    "try_send must return Full (not Closed) when the receiver is alive but \
                     un-polled and the slot is occupied"
                );
            }
        }

        // Hold rx alive across the assertion above so we don't trigger
        // the Closed path; explicitly drop now.
        drop(rx);
        drop(tx);
    }

    /// M-testing-2 fixup (§6.7 trigger b shutdown-deadline): drop the
    /// driver mid-stream and assert (i) the spawned task exits within a
    /// bounded deadline (NOT hangs past the 5s test timeout — that
    /// would indicate a silent deadlock the deadline-bounded drain is
    /// supposed to prevent), (ii) the in-flight `<digest>.partial` file
    /// remains on disk (left for `prune_temp_path` GC on next
    /// `FilesystemStore::new`), (iii) the `loop_exited` flag is NOT
    /// set (task was aborted, did not exit normally).
    #[nativelink_test]
    async fn driver_drop_with_pending_chunks_exits_within_deadline_and_leaves_partial() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 4;
        let total: u64 = (N * CHUNK) as u64;
        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0xb1u8 + i as u8).take(CHUNK));
        }
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        // Send N-1 chunks (no finish) so the driver's bitmap is
        // partially filled and the sender remains alive — we want the
        // shutdown drop, not the happy-path mpsc-close.
        for i in 0..N - 1 {
            let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: bytes,
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send pre-shutdown chunk");
        }

        // Wait for the driver to observe at least one chunk; otherwise
        // dropping the driver might race ahead of the recv loop ever
        // running.
        tokio::time::timeout(Duration::from_secs(2), async {
            while driver.chunks_received() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver must observe at least one chunk before shutdown");

        // Shutdown trigger (b): drop the driver. The spawned task is
        // aborted via `JoinHandleDropGuard`. The sender is still alive
        // — so without abort, the recv loop would block on `recv()`
        // indefinitely, the test would hang past the 5s timeout, and
        // the assertion below would fire.
        drop(driver);

        // The on-disk partial MUST remain — `prune_temp_path` will GC
        // it on next `FilesystemStore::new` per §6.7 (b). We poll the
        // partial path under a deadline; if the partial vanishes the
        // shutdown path is mistakenly auto-discarding (would silently
        // break the recovery model).
        let partial_path =
            crate::chunked::chunked_filesystem::partial_temp_path(
                store.temp_path_for_chunked(),
                &digest,
            );
        let exists = tokio::time::timeout(Duration::from_secs(5), async {
            // Wait briefly for any pending writes to land then verify.
            tokio::task::yield_now().await;
            tokio::fs::metadata(&partial_path).await.is_ok()
        })
        .await
        .expect(
            "must not deadlock — partial-existence check after driver drop should be \
             prompt (§6.7 trigger b)",
        );
        assert!(
            exists,
            "after driver drop, partial file must remain on disk for prune_temp_path GC; \
             vanished partial = shutdown-path auto-discard bug; checked path={}",
            partial_path.display(),
        );

        // Drop the sender (so any future retry path doesn't hang).
        drop(tx);
    }

    /// Driver completes happily — `loop_exited()` flips after recv loop
    /// returns. The §6.7 trigger (a) regression contract.
    #[nativelink_test]
    async fn driver_loop_exits_after_commit_path_completes() {
        const CHUNK: usize = 4 * 1024;
        let total = CHUNK as u64;
        let blob = vec![0xa1u8; CHUNK];
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        let permit = budget.try_acquire_chunk().expect("permit");
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from(blob),
            finish: true,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            // Await the completion result so the driver can finish its
            // commit + SHA-256 verify before we drop the sender.
            let _ = driver
                .await_completion()
                .await
                .expect("commit must succeed for hash-matching blob");
            drop(tx);
            // After tx drops, the recv loop returns and loop_exited
            // flips.
            loop {
                if driver.loop_exited() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver must exit loop within 5s after commit + sender-drop");
    }

    /// Phase 2.5 unit: driver receives all 3 chunks; the in-memory pin
    /// covers the entire blob; `try_get_chunk_from_pin(0, total)`
    /// returns Some with the assembled bytes EQUAL to the original
    /// blob. This is the design §6.3 step 2 hit case.
    ///
    /// The test sends chunks WITHOUT `finish` so commit_and_verify
    /// does NOT fire (which would clear the pin) — we need to observe
    /// the pin while it's still populated, simulating the read-arriving-
    /// while-write-still-in-flight production case.
    #[nativelink_test]
    async fn pin_accessor_full_range_hits_after_all_chunks_landed() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 3;
        let total = (N * CHUNK) as u64;

        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0x10u8 + i as u8).take(CHUNK));
        }
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);

        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        tokio::time::timeout(Duration::from_secs(5), async {
            // Send all N chunks without finish — pin is populated, but
            // commit_and_verify doesn't fire (and so the pin doesn't
            // get cleared by the spawning task's post-loop drop).
            for i in 0..N {
                let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
                let permit = budget.try_acquire_chunk().expect("permit");
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    finish: false,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await
                .expect("send");
            }
            // Wait for the driver to commit all N chunks to disk so the
            // pin is fully populated (the pin update is sequenced
            // AFTER the pwrite per the run_driver ordering).
            loop {
                if driver.chunks_committed() == N as u64 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("must not deadlock — pin population path");

        // Pin assertions: every byte covered, total cached, count == N.
        assert_eq!(driver.pinned_chunk_count(), N);
        assert_eq!(driver.pinned_bytes(), total);

        // Full-range read returns the whole blob.
        let assembled = driver
            .try_get_chunk_from_pin(0, total)
            .expect("full-range pin read must succeed when all chunks landed");
        assert_eq!(assembled.len(), blob.len());
        assert_eq!(&assembled[..], &blob[..]);

        // Sub-range read also works (chunk-aligned).
        let mid = driver
            .try_get_chunk_from_pin(CHUNK as u64, CHUNK as u64)
            .expect("mid-chunk pin read must succeed");
        assert_eq!(&mid[..], &blob[CHUNK..2 * CHUNK]);

        // Cross-chunk sub-range (spans two chunks).
        let cross_off = (CHUNK / 2) as u64;
        let cross_len = CHUNK as u64;
        let cross = driver
            .try_get_chunk_from_pin(cross_off, cross_len)
            .expect("cross-chunk pin read must succeed");
        let cross_off_us = cross_off as usize;
        assert_eq!(
            &cross[..],
            &blob[cross_off_us..cross_off_us + CHUNK]
        );

        drop(tx);
    }

    /// Phase 2.5 unit: chunks 0 and 2 of a 3-chunk blob have landed
    /// but chunk 1 has NOT — the request `try_get_chunk_from_pin(0,
    /// total)` returns None because the range is not contiguously
    /// covered. The accessor MUST detect the gap and refuse to serve
    /// partial bytes (per the all-or-nothing contract documented on
    /// the accessor).
    #[nativelink_test]
    async fn pin_accessor_gap_returns_none() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 3;
        let total = (N * CHUNK) as u64;

        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0x20u8 + i as u8).take(CHUNK));
        }
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);

        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        tokio::time::timeout(Duration::from_secs(5), async {
            // Send chunks 0 and 2 — chunk 1 is the gap. No finish.
            for &i in &[0usize, 2usize] {
                let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
                let permit = budget.try_acquire_chunk().expect("permit");
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    finish: false,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await
                .expect("send");
            }
            // Wait for both committed.
            loop {
                if driver.chunks_committed() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("must not deadlock — gap-pin population path");

        assert_eq!(driver.pinned_chunk_count(), 2);
        // Full-range request: includes the gap → None.
        assert!(
            driver.try_get_chunk_from_pin(0, total).is_none(),
            "full-range read across a gap MUST return None — pin accessor served a partial range",
        );

        // Range that lies entirely in chunk 0 → Some.
        let head = driver
            .try_get_chunk_from_pin(0, CHUNK as u64)
            .expect("range entirely within landed chunk 0 must succeed");
        assert_eq!(&head[..], &blob[..CHUNK]);

        // Range that lies entirely in chunk 2 → Some.
        let tail_off = (2 * CHUNK) as u64;
        let tail = driver
            .try_get_chunk_from_pin(tail_off, CHUNK as u64)
            .expect("range entirely within landed chunk 2 must succeed");
        assert_eq!(&tail[..], &blob[2 * CHUNK..3 * CHUNK]);

        // Range overlapping the gap (chunks 1+2) → None.
        let mid_off = CHUNK as u64;
        assert!(
            driver
                .try_get_chunk_from_pin(mid_off, (2 * CHUNK) as u64)
                .is_none(),
            "range overlapping gap (chunks 1+2) MUST return None — accessor leaked partial coverage",
        );

        drop(tx);
    }

    /// Phase 2.5 unit: range that overruns the declared blob size
    /// returns None (defensive — caller bug; falls through so the
    /// slow store can return its own well-defined OutOfRange).
    #[nativelink_test]
    async fn pin_accessor_overrun_returns_none() {
        const CHUNK: usize = 4 * 1024;
        let total = CHUNK as u64;
        let blob = vec![0x30u8; CHUNK];
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);

        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        tokio::time::timeout(Duration::from_secs(5), async {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: 0,
                chunk_bytes: Bytes::from(blob.clone()),
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
            loop {
                if driver.chunks_committed() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("must not deadlock — overrun setup");

        // Request 1 byte beyond the end of the blob → None.
        assert!(
            driver.try_get_chunk_from_pin(0, total + 1).is_none(),
            "overrun read MUST return None — pin accessor served past EOF",
        );
        // Pure-overrun request (offset == size, length 1) → None.
        assert!(
            driver.try_get_chunk_from_pin(total, 1).is_none(),
            "offset-at-EOF read MUST return None",
        );

        drop(tx);
    }

    /// Phase 2.5 unit: after commit_and_verify completes successfully,
    /// the spawning task clears the pin — subsequent reads return
    /// None and the cascade falls through to the slow store. This is
    /// the lifetime contract that prevents the pin from leaking
    /// memory after the canonical CAS file is on disk.
    #[nativelink_test]
    async fn pin_accessor_cleared_after_successful_commit() {
        const CHUNK: usize = 4 * 1024;
        let total = CHUNK as u64;
        let blob = vec![0x40u8; CHUNK];
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);

        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        tokio::time::timeout(Duration::from_secs(5), async {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: 0,
                chunk_bytes: Bytes::from(blob.clone()),
                finish: true,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
            // Await commit so the post-loop pin clear has run.
            let r = driver
                .await_completion()
                .await
                .expect("commit must succeed");
            assert_eq!(r.committed_size, total);
            // The clear runs AFTER commit_and_verify returns but BEFORE
            // completion_tx.send — yield once to let the task progress.
            loop {
                if driver.pinned_chunk_count() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("must not deadlock — post-commit clear path");

        assert_eq!(driver.pinned_chunk_count(), 0);
        assert_eq!(driver.pinned_bytes(), 0);
        assert!(
            driver.try_get_chunk_from_pin(0, total).is_none(),
            "post-commit pin read MUST return None — clear didn't fire",
        );

        drop(tx);
    }

    /// #213 NMA2 fixup test: per-chunk pwrite-timeout contract under
    /// a wedged slow tier. Wedges `write_chunk_at_offset` 500 ms,
    /// runs driver with 50 ms `per_chunk_timeout`, asserts
    /// `await_completion()` returns `Err(DeadlineExceeded)` naming
    /// the contract. The 5 s outer detector is the deadlock alarm
    /// (fires only if the spawned task neither sends nor drops the
    /// completion oneshot — see round-2 NIT-1 reword); the inner
    /// `expect_err` + `assert_eq!(err.code, DeadlineExceeded)` are
    /// the primary mutation guards. Reverting the
    /// `tokio::time::timeout(per_chunk_timeout, write_fut)` wrap
    /// triggers `expect_err`; reclassifying triggers `assert_eq!`.
    #[nativelink_test]
    async fn driver_per_chunk_pwrite_timeout_returns_deadline_exceeded() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = CHUNK as u64;
        let blob = vec![0xa9u8; CHUNK];
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();

        // Inject a slow-tier wedge: write_chunk_at_offset will sleep
        // for 500ms before the real pwrite. With the driver's
        // per_chunk_timeout set to 50ms, the timeout fires
        // deterministically (10× safety margin over typical CI
        // jitter). The test hook is per-digest so parallel tests in
        // the same binary do NOT collide.
        const WEDGE_MS: u64 = 500;
        const TIMEOUT_MS: u64 = 50;
        // #213 reviewer M4 fixup: LazyLock<Mutex<HashMap>> means no
        // `Option` dance — straight `.lock().insert(digest, ...)`.
        super::super::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
            .lock()
            .insert(digest, WEDGE_MS);
        // Manual scope-guard so a test panic still cleans up the
        // per-digest entry (`scopeguard` crate is not a dep).
        struct ResetWriteDelay(DigestInfo);
        impl Drop for ResetWriteDelay {
            fn drop(&mut self) {
                super::super::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
                    .lock()
                    .remove(&self.0);
            }
        }
        let _reset_guard = ResetWriteDelay(digest);

        let (driver, tx) = ChunkedDriver::spawn_driver_with_per_chunk_timeout(
            store.clone(),
            digest,
            total,
            CHUNK,
            PER_BLOB_MPSC_CAP,
            Duration::from_millis(TIMEOUT_MS),
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: 0,
                chunk_bytes: Bytes::from(blob.clone()),
                finish: true,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
            let err = driver
                .await_completion()
                .await
                .expect_err("per-chunk timeout MUST surface as Err");
            assert_eq!(
                err.code,
                nativelink_error::Code::DeadlineExceeded,
                "per-chunk pwrite timeout must classify as DeadlineExceeded; got {err:?}"
            );
            let msg = format!("{err:?}");
            assert!(
                msg.contains("per-chunk pwrite exceeded"),
                "error must name the timeout contract; got {msg}"
            );
            drop(tx);
        })
        .await
        .expect(
            "must not deadlock — per-chunk pwrite timeout fires within bound (#213 NMA2)"
        );
    }

    /// #213 testing-czar M1 fixup (§6.7 trigger b shutdown drain
    /// behavior with a wedged-slow-tier scenario WITHOUT the timeout
    /// trick — instead, drop the driver under a deadline and assert
    /// the in-flight `prune_temp_path`-eligible state is left for GC).
    ///
    /// This complements the existing
    /// `driver_drop_with_pending_chunks_exits_within_deadline_and_leaves_partial`
    /// test by exercising the case where the slow tier is making
    /// progress (real FilesystemStore) and the shutdown signal arrives
    /// while chunks are still in flight: the JoinHandleDropGuard MUST
    /// abort the spawned task within bounded wall-clock and the
    /// completion oneshot MUST resolve to `Err(_)` (sender dropped on
    /// abort).
    #[nativelink_test]
    async fn driver_drop_during_active_drain_resolves_completion_err_within_bound() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 8;
        let total: u64 = (N * CHUNK) as u64;
        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0xc7u8 + i as u8).take(CHUNK));
        }
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        // Send all chunks in (no finish on any) so the driver is
        // actively draining when we drop it.
        for i in 0..N {
            let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: bytes,
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
        }

        // Wait for the driver to actually start consuming.
        tokio::time::timeout(Duration::from_secs(2), async {
            while driver.chunks_received() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver must observe at least one chunk before shutdown");

        // Take the completion receiver BEFORE dropping the driver so
        // we can observe the abort outcome.
        let completion = driver.completion_rx.lock().take().expect("completion rx must be present");

        // Shutdown drop. The JoinHandleDropGuard aborts the spawned
        // task. completion_tx is dropped → completion_rx resolves
        // `Err(RecvError)` within bounded wall-clock.
        drop(driver);

        let result = tokio::time::timeout(Duration::from_secs(5), completion).await
            .expect("must not deadlock — completion oneshot must resolve within bound (§6.7 trigger b)");
        assert!(
            result.is_err(),
            "after JoinHandleDropGuard abort, completion_tx must drop without sending; got Ok(_)"
        );

        drop(tx);
    }

    /// #213 testing-czar M2 fixup (round-2 MAJOR-C: renamed from
    /// `driver_panic_safety_completion_resolves_err_and_pin_permits_drop`
    /// because the test does NOT inject a real panic — it tests the
    /// observable consequence of `JoinHandleDropGuard::drop` ⇒
    /// `abort()` on the spawned task, which is also the §6.7(d)
    /// panic-safety belt's effective signal but reached via the
    /// drop-the-parent path, not via a real panic).
    ///
    /// The §6.7 trigger (d) contract says: on driver panic, (i) the
    /// completion oneshot's sender drops → receiver gets `Err(_)`;
    /// (ii) per-chunk SemaphorePermits owned by ChunkWorks drop
    /// independently via Drop; (iii) the in-flight map cleanup fires
    /// via the JoinHandleDropGuard. This test asserts (i) + (ii) on
    /// the abort-on-drop path: we drop the driver, the
    /// JoinHandleDropGuard aborts the spawned task, completion_tx
    /// drops without sending, and the per-chunk permits return.
    ///
    /// **Coverage of §6.7(d).** This test covers the observable
    /// consequence (abort-on-drop ⇒ Err on completion + permits
    /// reclaimed). Real panic injection inside the spawn_blocking
    /// closure (or anywhere in the run_driver loop) is deferred to a
    /// future test using the `failpoints` crate (or equivalent
    /// fault-injection harness); without an injection mechanism, the
    /// outermost driver panic is not directly observable from a
    /// black-box integration test.
    ///
    /// Mutation step: comment out the `_handle: handle` field's
    /// `JoinHandleDropGuard` wrapping (changing `JoinHandleDropGuard`
    /// to a bare `JoinHandle`); the spawned task would no longer be
    /// aborted on driver Drop, the completion sender would never
    /// drop, and the test's `tokio::time::timeout(Duration::from_secs(5), ...)`
    /// would fire — the SPECIFIC `.expect("must not deadlock — \
    /// driver-task abort-on-drop must surface Err")` panic naming
    /// the contract.
    #[nativelink_test]
    async fn driver_drop_releases_pin_permits_within_bound_after_abort() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = CHUNK as u64;
        let blob_hash = sha256(&vec![0xeeu8; CHUNK]);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();
        let (driver, tx) =
            ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

        // Pre-baseline budget. Should equal the global cap.
        let baseline = super::super::chunk_budget::TOTAL_CHUNK_PERMITS;
        assert_eq!(
            budget.available_chunks(),
            baseline,
            "budget must be at baseline before any chunks admitted",
        );

        // Send one chunk (no finish) and wait for it to be received.
        // This puts a permit in flight inside ChunkWork → ChunkPin.
        let permit = budget.try_acquire_chunk().expect("permit");
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from(vec![0xeeu8; CHUNK]),
            finish: false,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .expect("send");
        tokio::time::timeout(Duration::from_secs(2), async {
            while driver.chunks_received() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver must observe chunk before panic injection");

        // Take the completion receiver BEFORE the abort so we can
        // observe the panic-safety belt's effective signal.
        let completion = driver.completion_rx.lock().take().expect("completion rx must be present");

        // Panic-safety belt: drop the driver. Per §6.7 (d), this is
        // the equivalent observable behavior — JoinHandleDropGuard
        // aborts the spawned task, completion_tx drops without
        // sending, the receiver resolves Err(_).
        drop(driver);

        // Bullet (i): completion oneshot resolves Err within bound.
        let result = tokio::time::timeout(Duration::from_secs(5), completion).await
            .expect("must not deadlock — driver-task abort-on-drop must surface Err (§6.7 d observable consequence)");
        assert!(
            result.is_err(),
            "after driver-task abort (panic-safety belt), completion_tx must drop without sending; got Ok(_)",
        );

        // Bullet (ii): per-chunk SemaphorePermits owned by the
        // dropped ChunkWork return to the global budget within bound.
        // The dropped ChunkWork's `Drop` releases its permit
        // independently of the in-flight map cleanup.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if budget.available_chunks() == baseline {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(
            "must not deadlock — per-chunk SemaphorePermit must drop with ChunkWork \
             after driver-task abort (§6.7 d bullet ii)",
        );

        drop(tx);
    }

    /// #213 reviewer round-2 MAJOR-B (M8 mutation test): asserts that
    /// the per-chunk pwrite timeout-Err arm increments
    /// [`super::CHUNKED_DRIVER_PWRITE_TIMEOUT_TOTAL`] by AT LEAST 1
    /// per timeout. Without this, a future change that drops the
    /// `record_pwrite_timeout_and_maybe_warn()` call (or the
    /// `fetch_add(1, ...)` inside it) would silently disable the
    /// blocking-pool-leak observability without any test failing.
    ///
    /// **Mutation step:** comment out the `record_pwrite_timeout_and_maybe_warn();`
    /// call in `run_driver`'s pwrite-timeout-Err arm; rerun this
    /// test; the `assert!(delta >= 1, ...)` panic with the bespoke
    /// message MUST fire. (Reverted in checked-in code.)
    ///
    /// Uses a baseline-snapshot + `>= 1` pattern (rather than `== 1`)
    /// because cargo runs sibling tests in parallel and the sibling
    /// `driver_per_chunk_pwrite_timeout_returns_deadline_exceeded`
    /// also fires a timeout against the same global counter — a
    /// strict equality flakes ~20% of the time when the two tests
    /// race. The mutation guard is unchanged: removing the increment
    /// drops delta to 0, failing `>= 1`.
    #[nativelink_test]
    async fn driver_per_chunk_pwrite_timeout_increments_total_counter_at_least_once() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = CHUNK as u64;
        let blob = vec![0xb3u8; CHUNK];
        let blob_hash = sha256(&blob);
        let digest = DigestInfo::new(blob_hash, total);
        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();

        // Inject the wedge so the per-chunk timeout fires.
        const WEDGE_MS: u64 = 500;
        const TIMEOUT_MS: u64 = 50;
        super::super::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
            .lock()
            .insert(digest, WEDGE_MS);
        struct ResetWriteDelay(DigestInfo);
        impl Drop for ResetWriteDelay {
            fn drop(&mut self) {
                super::super::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
                    .lock()
                    .remove(&self.0);
            }
        }
        let _reset_guard = ResetWriteDelay(digest);

        // Baseline-snapshot the global counter so this test is robust
        // to other tests in the same binary having incremented it.
        let baseline = super::CHUNKED_DRIVER_PWRITE_TIMEOUT_TOTAL
            .load(core::sync::atomic::Ordering::Relaxed);

        let (driver, tx) = ChunkedDriver::spawn_driver_with_per_chunk_timeout(
            store.clone(),
            digest,
            total,
            CHUNK,
            PER_BLOB_MPSC_CAP,
            Duration::from_millis(TIMEOUT_MS),
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: 0,
                chunk_bytes: Bytes::from(blob.clone()),
                finish: true,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
            let _ = driver
                .await_completion()
                .await
                .expect_err("per-chunk timeout MUST surface as Err");
            drop(tx);
        })
        .await
        .expect(
            "must not deadlock — per-chunk pwrite timeout fires within bound (#213 reviewer M8 counter)",
        );

        let after = super::CHUNKED_DRIVER_PWRITE_TIMEOUT_TOTAL
            .load(core::sync::atomic::Ordering::Relaxed);
        let delta = after.saturating_sub(baseline);
        assert!(
            delta >= 1,
            "CHUNKED_DRIVER_PWRITE_TIMEOUT_TOTAL must increment by AT LEAST 1 per timeout — \
             record_pwrite_timeout_and_maybe_warn() in pwrite-timeout-Err arm dropped? \
             (#213 reviewer round-2 MAJOR-B M8 mutation guard) baseline={baseline} after={after} delta={delta}",
        );
    }

    /// #213 reviewer round-2 MAJOR-A mutation test: asserts that the
    /// `discard_chunked` await in `commit_and_verify`'s end-to-end
    /// SHA-256 mismatch arm is bounded by
    /// `tokio::time::timeout(DISCARD_AFTER_FAILURE_TIMEOUT, ...)`.
    /// Without the wrap, a wedged slow tier (filesystem
    /// `discard_chunked` hung) would hang the driver task forever on
    /// the SHA-mismatch cleanup path, breaking the post-error
    /// cleanup contract.
    ///
    /// **Wedge mechanism.** Registers a 30 s `discard_chunked` delay
    /// via `TEST_PRE_DISCARD_DELAY_MS_BY_DIGEST`. Drives an e2e
    /// SHA-256 mismatch (lying digest) so `commit_and_verify` enters
    /// the mismatch arm and calls `discard_chunked` (which is now
    /// wedged). With the timeout wrap, the driver returns within 5 s
    /// (DISCARD_AFTER_FAILURE_TIMEOUT) + 5 s slop. Without it, the
    /// 30 s wedge holds and the bespoke deadlock assertion fires.
    ///
    /// Mutation step: comment out one of the
    /// `tokio::time::timeout(DISCARD_AFTER_FAILURE_TIMEOUT, ...)`
    /// wraps in `commit_and_verify` (e.g. line 1146 — discard after
    /// SHA-256 mismatch); this test panics with the bespoke message
    /// within 10 s. (Reverted in checked-in code.)
    #[nativelink_test]
    async fn driver_bounds_post_sha_mismatch_discard_under_wedged_slow_tier() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 2;
        const WEDGE_MS: u64 = 30_000;
        const ASSERT_BOUND_SECS: u64 = 10;
        let total = (N * CHUNK) as u64;
        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0xc4u8 + i as u8).take(CHUNK));
        }
        // LIE: digest hash does not match the actual blob → e2e SHA-256
        // verify will fail at commit_and_verify, driving the mismatch
        // cleanup arm that calls unlink_holding + discard_chunked.
        let lying_hash = [0xeeu8; 32];
        let digest = DigestInfo::new(lying_hash, total);

        let (store, _content_path) = make_test_store().await;
        let budget = ChunkBudget::new();

        // Wedge `discard_chunked` for this digest. RAII guard so a
        // panic doesn't leak the entry across siblings.
        super::super::chunked_filesystem::TEST_PRE_DISCARD_DELAY_MS_BY_DIGEST
            .lock()
            .insert(digest, WEDGE_MS);
        struct ResetDiscardDelay(DigestInfo);
        impl Drop for ResetDiscardDelay {
            fn drop(&mut self) {
                super::super::chunked_filesystem::TEST_PRE_DISCARD_DELAY_MS_BY_DIGEST
                    .lock()
                    .remove(&self.0);
            }
        }
        let _reset_guard = ResetDiscardDelay(digest);

        let (driver, tx) = ChunkedDriver::spawn_driver(
            store.clone(),
            digest,
            total,
            CHUNK,
            PER_BLOB_MPSC_CAP,
        );

        tokio::time::timeout(Duration::from_secs(ASSERT_BOUND_SECS), async {
            for i in 0..N {
                let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
                let permit = budget.try_acquire_chunk().expect("permit");
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    finish: i == N - 1,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await
                .expect("send");
            }
            drop(tx);
            let err = driver
                .await_completion()
                .await
                .expect_err("e2e SHA-256 mismatch must surface as Err");
            assert_eq!(
                err.code,
                nativelink_error::Code::InvalidArgument,
                "e2e SHA-256 mismatch must classify as InvalidArgument; got {err:?}",
            );
        })
        .await
        .expect(
            "must not deadlock — driver must bound discard_chunked under wedge in \
             commit_and_verify SHA-mismatch arm \
             (#213 reviewer round-2 MAJOR-A mutation guard)",
        );
    }
}
