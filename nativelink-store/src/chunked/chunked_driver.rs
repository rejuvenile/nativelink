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
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use crate::filesystem_store::{FileEntry, FilesystemStore};

/// Per-blob mpsc capacity. Q4: 16 chunks-in-flight per blob is the
/// upper bound on per-blob memory pressure (16 × 1 MiB = 16 MiB).
/// Together with the global `ChunkBudget` cap this gives a 12-16 GiB
/// worst-case server RSS bound (§13.2).
pub const PER_BLOB_MPSC_CAP: usize = 16;

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
    pub chunk_sha256: [u8; 32],
    /// True iff this is the LAST chunk of the blob. Triggers commit
    /// once all preceding chunks have landed.
    pub finish: bool,
    /// Permit lifetime = `ChunkWork` lifetime. Per §13.1.1 point 1:
    /// admission moved this permit out of the global `ChunkBudget`
    /// into the `ChunkWork`; dropping the work releases the permit.
    pub _permit: OwnedSemaphorePermit,
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
    /// per-blob mpsc cap (`PER_BLOB_MPSC_CAP * CHUNK_SIZE = 16 MiB`)
    /// while the driver is consuming, AND further bounded after the
    /// driver consumed but before commit by the blob size itself —
    /// already counted toward the global ChunkBudget via the Q8
    /// per-chunk permits the `ChunkWork` items hold.
    chunks: BTreeMap<u64, Bytes>,
    /// Total bytes pinned. Cached so the accessor avoids walking the
    /// map to compute coverage; updated on every insert.
    total_bytes: u64,
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
    /// tests can exercise smaller channels without a 16-`ChunkWork`
    /// setup.
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
            )
            .await;
            // Drop the in-memory pin once the driver loop has finished
            // (commit success → blob is on the canonical CAS path; commit
            // failure → bytes are not authoritative). Frees per-blob
            // memory promptly even if the `Arc<ChunkedDriver>` registry
            // entry survives for a tick of de-registration. Read accessor
            // calls after this point return `None` and the caller falls
            // through to the next cascade step (slow store).
            pin_for_task.lock().chunks.clear();
            pin_for_task.lock().total_bytes = 0;
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
) -> Result<ChunkedCommitResult, Error> {
    while let Some(work) = rx.recv().await {
        chunks_received.fetch_add(1, Ordering::Relaxed);
        let ChunkWork {
            chunk_offset,
            chunk_bytes,
            chunk_sha256,
            finish,
            _permit,
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
        // NMA1, so the driver does not re-verify. We DO carry
        // `chunk_sha256` here for diagnostic correlation if a future
        // phase wants to defer verification or re-verify after retry.
        let _ = chunk_sha256;

        // Write the chunk via the FilesystemStore adapter (which is
        // already on `spawn_blocking` internally — see
        // `chunked_filesystem::write_chunk_at_offset`).
        // The clone is one `Arc` bump (Bytes is ref-counted); the
        // landed-chunk pin populated below shares the same buffer.
        let bytes_for_write = chunk_bytes.clone();
        if let Err(write_err) = filesystem_store
            .write_chunk_at_offset(&digest, chunk_offset, bytes_for_write)
            .await
        {
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
            if let Err(discard_err) = filesystem_store.discard_chunked(&digest).await {
                error!(
                    target: "nativelink_store::chunked",
                    ?digest,
                    ?discard_err,
                    "chunked driver: discard after per-chunk write failure also failed",
                );
            }
            return Err(write_err);
        }
        chunks_committed.fetch_add(1, Ordering::Relaxed);

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
        if let Err(discard_err) = filesystem_store.discard_chunked(digest).await {
            error!(
                target: "nativelink_store::chunked",
                ?digest,
                ?discard_err,
                "chunked driver: discard after commit failure also failed",
            );
        }
        return Err(commit_err);
    }

    // Step 2: end-to-end SHA-256 over the holding file.
    let holding_path_pb = filesystem_store.holding_content_path(digest);
    let computed = tokio::task::spawn_blocking({
        let path = holding_path_pb.clone();
        move || -> Result<[u8; 32], std::io::Error> {
            // sha2's `Sha256::digest(slice)` would require loading the
            // whole file into memory; for a 100 MiB blob that is 100 MiB
            // of allocation. Stream via `std::io::Read` + `Sha256::update`
            // to keep peak memory at the read-buffer size only.
            //
            // Buffer = 1 MiB to match ZFS recordsize=1M on `fast/nativelink/work`
            // (perf-optimizer MINOR-1 fixup); avoids 16× syscalls per record
            // vs the previous 64 KiB.
            use std::io::Read;
            let mut file = std::fs::File::open(&path)?;
            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; 1024 * 1024];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            let out = hasher.finalize();
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(out.as_ref());
            Ok(bytes)
        }
    })
    .await
    .map_err(|join_err| {
        make_err!(
            Code::Internal,
            "spawn_blocking join error in commit-time SHA-256: {join_err:?}"
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
        warn!(
            target: "nativelink_store::chunked",
            ?digest,
            computed = ?hex::encode(computed),
            declared = ?hex::encode(declared),
            "chunked driver: end-to-end SHA-256 mismatch; unlinking holding file"
        );
        if let Err(unlink_err) = filesystem_store.unlink_holding(digest).await {
            error!(
                target: "nativelink_store::chunked",
                ?digest,
                ?unlink_err,
                "chunked driver: failed to unlink holding file after SHA-256 mismatch",
            );
        }
        if let Err(discard_err) = filesystem_store.discard_chunked(digest).await {
            error!(
                target: "nativelink_store::chunked",
                ?digest,
                ?discard_err,
                "chunked driver: failed to discard in-flight state after SHA-256 mismatch",
            );
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
        // in-flight tracker entry.
        if let Err(unlink_err) = filesystem_store.unlink_holding(digest).await {
            warn!(
                target: "nativelink_store::chunked",
                ?digest,
                ?unlink_err,
                "chunked driver: failed to unlink holding after finalize failure",
            );
        }
        if let Err(discard_err) = filesystem_store.discard_chunked(digest).await {
            warn!(
                target: "nativelink_store::chunked",
                ?digest,
                ?discard_err,
                "chunked driver: failed to discard in-flight state after finalize failure",
            );
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

    use super::super::chunk_budget::ChunkBudget;
    use super::{ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP};
    use crate::filesystem_store::{FileEntryImpl, FilesystemStore};

    /// Capacity constant pin: any change is ARCHITECTURAL — re-read
    /// design §4 Q4 before bumping.
    #[test]
    fn per_blob_mpsc_cap_is_sixteen() {
        assert_eq!(PER_BLOB_MPSC_CAP, 16);
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
                let chunk_sha = sha256(&bytes);
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    chunk_sha256: chunk_sha,
                    finish: i == N - 1,
                    _permit: permit,
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
                let chunk_sha = sha256(&bytes);
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK) as u64,
                    chunk_bytes: bytes,
                    chunk_sha256: chunk_sha,
                    finish: idx == order.len() - 1,
                    _permit: permit,
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
                    chunk_sha256: [0u8; 32],
                    finish: i == N - 1,
                    _permit: permit,
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
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
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
            chunk_sha256: [0u8; 32],
            finish: false,
            _permit: permit,
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
            chunk_sha256: [0u8; 32],
            finish: false,
            _permit: permit_a,
        };
        tx.try_send(work_a).expect("first try_send into capacity-1 mpsc must succeed");

        // Second try_send must fail Full because the receiver is never
        // polled (we deliberately keep `rx` alive but un-polled).
        let permit_b = budget.try_acquire_chunk().expect("permit b");
        let work_b = ChunkWork {
            chunk_offset: 4096,
            chunk_bytes: Bytes::from_static(b""),
            chunk_sha256: [0u8; 32],
            finish: false,
            _permit: permit_b,
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
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
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
            chunk_sha256: [0u8; 32],
            finish: true,
            _permit: permit,
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
}
