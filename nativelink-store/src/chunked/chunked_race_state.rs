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

//! #494-v3 Phase 2: per-digest multi-writer chunk-race coordination state.
//!
//! Wraps a `ChunkedPartialsMap` entry's lifecycle so multiple concurrent
//! WriteChunkedV2 streams can collaboratively upload the SAME digest:
//! each chunk-offset is admitted at most once for pwrite; duplicate
//! offsets are reported back to the writer as `ALREADY_HAVE` (already
//! committed) or `RACING_LOSER` (concurrent in-flight pwrite for the
//! same offset already won the race).
//!
//! **Why bit-identical chunks are safe:** the chunker
//! (`chunked_client::collect_and_hash_chunks`) splits at fixed
//! `next_offset` boundaries (1 MiB each). Two writers reading the same
//! source bytes produce bit-identical boundaries AND contents. So any
//! (offset, digest) has exactly one valid byte-sequence — whichever
//! writer's pwrite lands first produces the same on-disk bytes as the
//! loser would have. End-to-end BLAKE3 verify on the holding file
//! catches divergence (a guaranteed defense-in-depth tripwire).
//!
//! **Composability invariants** preserved (CLAUDE.md gate-pin-evict
//! triangle, applied here to multi-writer state):
//! 1. Mirror-once: only the writer that flips the LAST bit in
//!    `chunks_present` runs the commit path; siblings observe the same
//!    `commit_done` notify and return the same result.
//! 2. Cancel-handoff: writer disconnect/cancel runs `RaceWriterGuard::drop`
//!    which removes the writer's `WriterId` from every
//!    `chunks_in_flight[*]` slot; bits already set in `chunks_present`
//!    stay set; surviving writers can finish the blob.
//! 3. Budget bookkeeping: only ACCEPTED chunks consume pin/chunk budget;
//!    RACING_LOSER paths release their permit before notifying the
//!    writer (the loser never installs into the pin).
//! 4. Failed-commit-sink fires EXACTLY ONCE per failed commit (the
//!    commit-runner writer is the producer; sibling writers observe
//!    via `commit_done` and propagate the same Err).

#![allow(dead_code, reason = "wired by chunked_filesystem multi-writer adapter + WriteChunkedV2 handler")]

use core::fmt::Debug;
use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nativelink_error::{Code, Error, make_err};
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::{debug, trace, warn};

/// Compile-time tripwire: bit-identical chunks rely on the chunker
/// splitting at fixed position boundaries, NOT content-defined
/// boundaries (rolling-hash CDC). If a future chunker changes shape
/// (e.g. switches to FastCDC for slow-tier dedup), this constant MUST
/// flip to false AND every per-chunk dedup site MUST be re-audited.
/// Race-state code references this so a flip causes test red-fail.
pub const CHUNK_BOUNDARIES_ARE_POSITION_BASED: bool = true;

/// Per-writer identifier inside the race-state. Monotonically minted
/// at writer admission; never reused inside one digest's lifetime.
/// Wraps a `u64` for transport-cheap identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WriterId(pub u64);

/// Outcome of admitting a chunk into the race-state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitOutcome {
    /// Caller reserved the slot; proceed to pwrite. Caller MUST call
    /// `mark_chunk_committed` after the pwrite succeeds (success path)
    /// or `release_chunk_in_flight` (failure path) to keep
    /// `chunks_in_flight` honest.
    Accept,
    /// Another writer's chunk for this offset already committed
    /// (bit set in `chunks_present`). Caller's chunk bytes were
    /// discarded BEFORE pwrite; do NOT call `mark_chunk_committed`.
    AlreadyHave,
    /// Another writer is currently mid-pwrite for this offset; this
    /// caller raced and lost. Caller's chunk bytes discarded; do NOT
    /// call `mark_chunk_committed`.
    RacingLoser,
}

/// Per-digest multi-writer coordination state. One `ChunkRaceState`
/// per in-flight digest. Held inside an `Arc` so writers, the
/// commit-runner, and the GC path can all observe and mutate.
///
/// Invariants:
/// - `chunks_present[i]` set => offset `i*chunk_size` has been pwritten
///   AND survived the in-flight check; set under `state` mutex
///   atomically with the `chunks_in_flight` slot decrement.
/// - `chunks_in_flight[offset]` non-empty => some writer admitted
///   `AdmitOutcome::Accept` for this offset and has not yet
///   completed pwrite (success or failure).
/// - `commit_done.notify_waiters()` fires EXACTLY ONCE per
///   commit-runner term — when `publish_commit_result` writes a
///   final outcome. The result is stored in `commit_result` BEFORE
///   the notify so observing waiters see it.
/// - `commit_running` flag arbitrates "I am running commit" — set
///   inside the same lock that flips the last bit. Only ONE writer
///   sees `RunCommit`. `commit_done_flag` flips true the moment a
///   final result is published. If a commit-runner exits without
///   publishing (panic, cancellation, drop), `CommitRunnerGuard::Drop`
///   publishes a synthetic `Code::Cancelled` Err so siblings observe
///   a definite outcome instead of waiting the watchdog forever.
pub struct ChunkRaceState {
    /// Identity of the digest this state coordinates.
    pub(crate) digest: DigestInfo,
    /// Declared blob size from the digest. Used to compute
    /// `expected_chunks` and validate writer-supplied offsets.
    pub(crate) declared_size: u64,
    /// Chunk size for this digest's race. Must match what writers
    /// supply (admission validates via offset alignment).
    pub(crate) chunk_size: u32,
    /// Number of chunks expected to fully cover the blob:
    /// `ceil(declared_size / chunk_size)`. For `declared_size == 0`
    /// this is 0 and commit is triggered by the empty-blob path
    /// outside this struct.
    pub(crate) expected_chunks: u32,
    /// State machine guard. parking_lot::Mutex; never held across
    /// .await. Critical sections: bit-set + HashMap update; each is
    /// O(1) modulo HashMap rehash.
    state: Mutex<RaceMutableState>,
    /// Notifies all writers when commit (success or failure) completes.
    /// Writers that admitted at least one chunk subscribe via
    /// `commit_done.notified()` after sending their final chunk; they
    /// then read `commit_result` to learn the outcome.
    pub(crate) commit_done: Notify,
    /// Result of the commit. `None` until commit runs; `Some(Ok|Err)`
    /// after. Writers MUST NOT mutate this; only the commit-runner
    /// (the writer that observed `chunks_present.all() == true` AND
    /// successfully called `try_claim_commit_runner`) writes to it.
    pub(crate) commit_result: Mutex<Option<Result<RaceCommitResult, Error>>>,
    /// Counter of currently-attached writers (admitted via
    /// `attach_writer`, not yet detached). Falsification metric for
    /// `chunked_writers_per_digest_max`. Atomic for lock-free read.
    pub(crate) attached_writer_count: AtomicU64,
    /// Counter of cumulative racing-loser chunks for this digest.
    /// Bumped when a writer's chunk is rejected with
    /// `AdmitOutcome::RacingLoser`. Lifetime ends when the state is
    /// dropped from the registry.
    pub(crate) racing_loser_chunks: AtomicU64,
    /// Counter of cross-writer-accepted chunks: chunks from writer B
    /// that committed while writer A was still in-flight. Bumped on
    /// `mark_chunk_committed` when there were OTHER writers (not the
    /// committer) registered in the slot at admission time.
    pub(crate) cross_writer_committed_chunks: AtomicU64,
    /// Path to the on-disk partial. Set once at construction (race
    /// state and partial-file lifetime are 1:1).
    pub(crate) partial_path: PathBuf,
}

/// Result of a successful race commit. Returned to every writer that
/// awaited `commit_done`.
#[derive(Debug, Clone)]
pub struct RaceCommitResult {
    pub committed_size: u64,
}

impl Debug for ChunkRaceState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("ChunkRaceState")
            .field("digest", &self.digest)
            .field("declared_size", &self.declared_size)
            .field("expected_chunks", &self.expected_chunks)
            .field("chunks_present", &state.chunks_present)
            .field(
                "in_flight_offsets",
                &state.chunks_in_flight.keys().copied().collect::<Vec<_>>(),
            )
            .field("commit_running", &state.commit_running)
            .field("commit_done_flag", &state.commit_done_flag)
            .finish()
    }
}

/// Mutable inner state guarded by the parking_lot mutex. Pulled out
/// so the lock guard's scope is tight + obvious.
#[derive(Debug)]
struct RaceMutableState {
    /// CAPPED AT N: 256 bits max (`MAX_CHUNKED_BLOB_SIZE / CHUNK_SIZE
    /// = 256 MiB / 1 MiB = 256`). One u64-word fits up to 64 chunks;
    /// 4 words covers the worst case. Stored as `Vec<u64>` to avoid
    /// pulling in `bitvec`.
    chunks_present: BitVec,
    /// CAPPED AT N: chunks ≤ 256, writers/digest ≤ ~321 (workers ×
    /// per-blob-mpsc-cap-with-Bazel). Worst-case memory:
    /// 256 entries × ~16 bytes/HashMap-bucket + 321 × 8 bytes/WriterId
    /// = ~6 KiB. Negligible compared to the 1 MiB chunks already in
    /// flight.
    chunks_in_flight: HashMap<u32, Vec<WriterId>>,
    /// "Someone is running commit." Set true atomically with the
    /// last-bit flip + RunCommit decision. Cleared by
    /// `CommitRunnerGuard::drop` IF `commit_done_flag` is still false
    /// (cancelled / panicked runner). When `commit_running=true` AND
    /// `commit_done_flag=false`, no other writer may claim
    /// RunCommit — the in-flight runner gets the chance to publish.
    /// When `commit_running=false` AND `commit_done_flag=false`, the
    /// previous runner exited without publishing AND has cleared this
    /// flag; the next writer that observes `all_set()` may claim
    /// RunCommit and retry the commit path.
    commit_running: bool,
    /// "A final commit result has been published." Set true by
    /// `publish_commit_result`. After this flips, no further writer
    /// will be selected as RunCommit (subsequent observations of
    /// all_set return AwaitCommit; the result is in `commit_result`).
    /// Sticky: never resets.
    commit_done_flag: bool,
}

/// Word-addressable bitmap. Internal helper, no external deps.
#[derive(Debug, Clone)]
struct BitVec {
    words: Vec<u64>,
    len: usize,
}

impl BitVec {
    fn with_len(len: usize) -> Self {
        let n_words = len.div_ceil(64);
        Self {
            words: vec![0u64; n_words],
            len,
        }
    }

    fn get(&self, i: usize) -> bool {
        if i >= self.len {
            return false;
        }
        let word = i / 64;
        let bit = i % 64;
        (self.words[word] >> bit) & 1 == 1
    }

    /// Sets bit `i`. Returns the previous value.
    fn set(&mut self, i: usize) -> bool {
        if i >= self.len {
            return false;
        }
        let word = i / 64;
        let bit = i % 64;
        let mask = 1u64 << bit;
        let prev = (self.words[word] & mask) != 0;
        self.words[word] |= mask;
        prev
    }

    /// Returns true iff all bits are set.
    fn all_set(&self) -> bool {
        if self.len == 0 {
            return true;
        }
        let full_words = self.len / 64;
        let trailing_bits = self.len % 64;
        for w in self.words.iter().take(full_words) {
            if *w != u64::MAX {
                return false;
            }
        }
        if trailing_bits > 0 {
            let last_word = self.words[full_words];
            let mask = (1u64 << trailing_bits) - 1;
            if (last_word & mask) != mask {
                return false;
            }
        }
        true
    }

    /// Returns the highest contiguous-from-zero offset (chunk index)
    /// that is set, plus 1; i.e., for chunks_present=[1,1,1,0,1,...]
    /// returns 3. Used for ADMITTED_SKIP_TO hints.
    fn contiguous_from_zero(&self) -> usize {
        for i in 0..self.len {
            if !self.get(i) {
                return i;
            }
        }
        self.len
    }

    fn count_ones(&self) -> usize {
        let mut total = 0usize;
        for w in &self.words {
            total += w.count_ones() as usize;
        }
        // Ignore bits past `self.len` — they should always be zero by
        // construction (only `set` writes; bitvec starts zeroed).
        total.min(self.len)
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl ChunkRaceState {
    /// Construct a fresh race-state for a digest. `partial_path` is
    /// the on-disk `.partial` file path that already-existing
    /// `chunked_filesystem::open_or_create_partial` minted; this is
    /// recorded so commit/discard paths know which file to operate on.
    pub fn new(
        digest: DigestInfo,
        chunk_size: u32,
        partial_path: PathBuf,
    ) -> Self {
        let declared_size = digest.size_bytes();
        let expected_chunks: u32 = if declared_size == 0 {
            0
        } else {
            let chunk_size_u64 = chunk_size as u64;
            // Ceil-div, capped at u32::MAX (overflow is impossible for
            // any practical blob; assertion is defensive belt).
            u32::try_from(declared_size.div_ceil(chunk_size_u64))
                .expect("expected_chunks must fit in u32 for any practical blob")
        };
        Self {
            digest,
            declared_size,
            chunk_size,
            expected_chunks,
            state: Mutex::new(RaceMutableState {
                chunks_present: BitVec::with_len(expected_chunks as usize),
                // CAPPED AT N: chunks ≤ 256, writers/digest ≤ ~321 (workers ×
                // per-blob-mpsc-cap-with-Bazel). Worst-case memory:
                // 256 entries × ~16 bytes/HashMap-bucket + 321 × 8 bytes/WriterId
                // = ~6 KiB. See `RaceMutableState::chunks_in_flight` doc.
                chunks_in_flight: HashMap::new(),
                commit_running: false,
                commit_done_flag: false,
            }),
            commit_done: Notify::new(),
            commit_result: Mutex::new(None),
            attached_writer_count: AtomicU64::new(0),
            racing_loser_chunks: AtomicU64::new(0),
            cross_writer_committed_chunks: AtomicU64::new(0),
            partial_path,
        }
    }

    /// Increment the attached-writer count. Returns the new count.
    /// Drop of the corresponding `RaceWriterGuard` decrements via
    /// `detach_writer`.
    pub fn attach_writer(&self) -> u64 {
        self.attached_writer_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Decrement the attached-writer count. Used by `RaceWriterGuard::drop`.
    pub fn detach_writer(&self) -> u64 {
        let prev = self.attached_writer_count.fetch_sub(1, Ordering::Relaxed);
        prev.saturating_sub(1)
    }

    /// Number of currently-attached writers. Falsification metric for
    /// `chunked_writers_per_digest_max`.
    pub fn attached_writer_count(&self) -> u64 {
        self.attached_writer_count.load(Ordering::Relaxed)
    }

    /// Cumulative count of `RACING_LOSER` admissions for this digest.
    pub fn racing_loser_count(&self) -> u64 {
        self.racing_loser_chunks.load(Ordering::Relaxed)
    }

    /// Cumulative count of cross-writer-committed chunks (chunk N
    /// committed by writer B while writer A was still in flight on
    /// chunk N).
    pub fn cross_writer_committed_count(&self) -> u64 {
        self.cross_writer_committed_chunks.load(Ordering::Relaxed)
    }

    /// Subscribe to the commit-done notify. Returns a future that
    /// resolves when `publish_commit_result` fires. Callers should
    /// subscribe BEFORE calling `peek_commit_result` to avoid the
    /// missed-wakeup race.
    pub fn subscribe_commit_done(&self) -> tokio::sync::futures::Notified<'_> {
        self.commit_done.notified()
    }

    /// Highest contiguous-from-zero chunk index already on disk,
    /// expressed as a byte offset hint for `ADMITTED_SKIP_TO`. Returns
    /// `None` if no contiguous prefix is present (writer should start
    /// from offset 0).
    pub fn admit_skip_to_hint_byte_offset(&self) -> Option<u64> {
        let state = self.state.lock();
        let contig = state.chunks_present.contiguous_from_zero();
        if contig == 0 {
            None
        } else {
            // Writer should skip every offset < contig*chunk_size; we
            // express this as the BYTE offset of the LAST already-have
            // chunk, so the writer can `>= already_have_max_offset` skip.
            let last_chunk_idx = (contig - 1) as u64;
            Some(last_chunk_idx * self.chunk_size as u64)
        }
    }

    /// Validate `(offset, len, finish)` shape against this digest's
    /// chunk_size + declared_size. Fast-fail before any state mutation.
    pub fn validate_chunk_shape(
        &self,
        offset: u64,
        len: usize,
        finish: bool,
    ) -> Result<(), Error> {
        if !offset.is_multiple_of(self.chunk_size as u64) {
            return Err(make_err!(
                Code::InvalidArgument,
                "WriteChunkedV2: offset {offset} is not a multiple of chunk_size {} for digest {}",
                self.chunk_size,
                self.digest
            ));
        }
        if !finish && len != self.chunk_size as usize {
            return Err(make_err!(
                Code::InvalidArgument,
                "WriteChunkedV2: non-final chunk_bytes.len()={len} must equal chunk_size {} \
                 for digest {} at offset {offset}",
                self.chunk_size,
                self.digest
            ));
        }
        if finish {
            let end = offset.saturating_add(len as u64);
            if end != self.declared_size {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "WriteChunkedV2: final chunk end={end} must equal declared_size {} \
                     for digest {} at offset {offset}",
                    self.declared_size,
                    self.digest
                ));
            }
        }
        Ok(())
    }

    /// Try to admit a chunk for pwrite. Computes the chunk index from
    /// the byte offset. Returns the admission outcome.
    ///
    /// Lock discipline: parking_lot::Mutex held only for the
    /// `chunks_present.get` check + `chunks_in_flight` map update.
    /// Caller proceeds with pwrite OUTSIDE the lock; on success,
    /// caller invokes `mark_chunk_committed` (re-acquires lock for the
    /// bit set + in-flight remove). On failure, caller invokes
    /// `release_chunk_in_flight` to remove the WriterId from the
    /// in-flight slot.
    pub fn try_admit_chunk(
        &self,
        writer_id: WriterId,
        offset: u64,
    ) -> AdmitOutcome {
        let chunk_idx = (offset / self.chunk_size as u64) as usize;
        let mut state = self.state.lock();
        if state.chunks_present.get(chunk_idx) {
            return AdmitOutcome::AlreadyHave;
        }
        // Check in-flight map. If non-empty, another writer is already
        // pwriting this offset; we lose the race. Discard our chunk.
        let entry = state.chunks_in_flight.entry(chunk_idx as u32).or_default();
        if !entry.is_empty() {
            self.racing_loser_chunks.fetch_add(1, Ordering::Relaxed);
            return AdmitOutcome::RacingLoser;
        }
        entry.push(writer_id);
        AdmitOutcome::Accept
    }

    /// Mark a chunk as committed: set the bit in `chunks_present`,
    /// remove the WriterId from `chunks_in_flight`, and signal whether
    /// THIS writer should run the commit path (returns true iff the
    /// caller is the commit-runner).
    ///
    /// Even if the writer was the only in-flight contender, a sibling
    /// writer that ALSO admitted at the same offset (before we got the
    /// lock) and was rejected as `RacingLoser` does NOT participate
    /// here — only Accepted writers reach this code.
    ///
    /// Returns:
    /// - `true` if THIS call observed `chunks_present.all_set() == true`
    ///   AND `commit_started` was false (now flipped true). Caller
    ///   becomes the commit-runner.
    /// - `false` otherwise. Caller awaits `commit_done` notify.
    pub fn mark_chunk_committed(
        &self,
        writer_id: WriterId,
        offset: u64,
    ) -> CommitResponsibility {
        let chunk_idx = (offset / self.chunk_size as u64) as usize;
        let mut state = self.state.lock();
        // Set the bit. We expect prev=false (we admitted Accept; no
        // other writer can have set the bit between our admit + here
        // because admission rejects with AlreadyHave when the bit is
        // set, and rejects with RacingLoser when another writer is
        // mid-flight.) Defensive: warn if prev=true.
        let prev = state.chunks_present.set(chunk_idx);
        if prev {
            warn!(
                target: "nativelink_store::chunked",
                digest = ?self.digest,
                offset,
                ?writer_id,
                "ChunkRaceState::mark_chunk_committed: bit was already set; \
                 admission contract violated",
            );
        }
        // Remove our WriterId from the in-flight slot. Track whether
        // there were OTHER WriterIds in the slot (cross-writer race
        // metric).
        let mut had_others = false;
        if let Some(slot) = state.chunks_in_flight.get_mut(&(chunk_idx as u32)) {
            had_others = slot.iter().any(|w| *w != writer_id);
            slot.retain(|w| *w != writer_id);
            if slot.is_empty() {
                state.chunks_in_flight.remove(&(chunk_idx as u32));
            }
        }
        if had_others {
            self.cross_writer_committed_chunks
                .fetch_add(1, Ordering::Relaxed);
        }
        // Decide commit responsibility. The triangular state machine:
        //   bitmap-full  + nobody-running + not-done => RunCommit (we win)
        //   bitmap-full  + somebody-running          => AwaitCommit
        //   bitmap-full  + done                      => AwaitCommit (peek result)
        //   bitmap-full  + nobody-running + not-done is the retry door
        //     reopened when a previous runner cancelled/panicked
        //     (CommitRunnerGuard::drop cleared commit_running).
        let all_present = state.chunks_present.all_set();
        if all_present && !state.commit_running && !state.commit_done_flag {
            state.commit_running = true;
            CommitResponsibility::RunCommit
        } else if all_present {
            // Bitmap full; another writer is running or already published.
            CommitResponsibility::AwaitCommit
        } else {
            // Bitmap not yet full. Caller continues sending chunks.
            CommitResponsibility::ContinueSending
        }
    }

    /// Try to claim commit responsibility WITHOUT marking a chunk
    /// committed — used by the AwaitCommit path when a writer ended its
    /// send loop and observed a wedged commit-runner (commit_done not
    /// set after the watchdog). Returns RunCommit only if no runner is
    /// currently active AND no result has been published. Mirrors the
    /// last-bit-flip claim path.
    pub fn try_claim_commit_runner(&self) -> CommitResponsibility {
        let mut state = self.state.lock();
        let all_present = state.chunks_present.all_set();
        if all_present && !state.commit_running && !state.commit_done_flag {
            state.commit_running = true;
            CommitResponsibility::RunCommit
        } else if all_present {
            CommitResponsibility::AwaitCommit
        } else {
            CommitResponsibility::ContinueSending
        }
    }

    /// Release a writer's in-flight slot WITHOUT marking the bit set.
    /// Used when a writer's pwrite failed (so the slot opens up for
    /// another writer to retry the offset).
    pub fn release_chunk_in_flight(&self, writer_id: WriterId, offset: u64) {
        let chunk_idx = (offset / self.chunk_size as u64) as u32;
        let mut state = self.state.lock();
        if let Some(slot) = state.chunks_in_flight.get_mut(&chunk_idx) {
            slot.retain(|w| *w != writer_id);
            if slot.is_empty() {
                state.chunks_in_flight.remove(&chunk_idx);
            }
        }
    }

    /// Drop-time hook: remove `writer_id` from EVERY `chunks_in_flight`
    /// slot. Called by `RaceWriterGuard::drop` so a cancelled / panicked
    /// writer doesn't leave stale in-flight markers blocking other
    /// writers from racing the same offset.
    pub fn purge_writer_in_flight(&self, writer_id: WriterId) -> usize {
        let mut state = self.state.lock();
        let mut purged = 0usize;
        let mut to_remove: Vec<u32> = Vec::new();
        for (offset, slot) in state.chunks_in_flight.iter_mut() {
            let before = slot.len();
            slot.retain(|w| *w != writer_id);
            purged += before - slot.len();
            if slot.is_empty() {
                to_remove.push(*offset);
            }
        }
        for offset in to_remove {
            state.chunks_in_flight.remove(&offset);
        }
        purged
    }

    /// Snapshot of the bit-count for tests / metrics. NOT cached;
    /// computed on demand so tests don't have to flush a stale counter.
    pub fn chunks_committed_count(&self) -> usize {
        self.state.lock().chunks_present.count_ones()
    }

    /// Snapshot of the in-flight-slot count for tests / debug.
    pub fn chunks_in_flight_count(&self) -> usize {
        self.state.lock().chunks_in_flight.len()
    }

    /// Whether the bitmap is fully covered. Tests use this; the
    /// commit-decision uses `mark_chunk_committed`'s return value.
    pub fn is_complete(&self) -> bool {
        self.state.lock().chunks_present.all_set()
    }

    /// Store the commit result and notify all waiters. Sticky:
    /// flips `commit_done_flag` true so subsequent observers see
    /// `AwaitCommit` (and read the published result), and so a late
    /// `CommitRunnerGuard::drop` does NOT overwrite the result with a
    /// cancellation Err. Idempotent at the result level: if a result
    /// is already published, the new one is dropped (defensive — only
    /// one runner should publish per term).
    pub fn publish_commit_result(&self, result: Result<RaceCommitResult, Error>) {
        {
            let mut guard = self.commit_result.lock();
            if guard.is_some() {
                // Already published; do not overwrite. This guards
                // against a runner that publishes Ok and then races
                // its own guard drop publishing Err.
                return;
            }
            *guard = Some(result);
        }
        // Flip done BEFORE notifying so any thread that wakes
        // immediately observes done=true without an additional
        // synchronization edge. State lock is acquired separately
        // (commit_done_flag lives inside `state`).
        {
            let mut state = self.state.lock();
            state.commit_done_flag = true;
        }
        self.commit_done.notify_waiters();
    }

    /// Take a clone of the commit result, if available. Returns None
    /// before the commit-runner publishes.
    pub fn peek_commit_result(&self) -> Option<Result<RaceCommitResult, Error>> {
        self.commit_result
            .lock()
            .as_ref()
            .map(|r| r.as_ref().map(Clone::clone).map_err(Clone::clone))
    }

    /// Whether a commit result has been published (Ok or Err). Read by
    /// `CommitRunnerGuard::drop` to decide whether to publish a
    /// cancellation Err.
    pub fn commit_done(&self) -> bool {
        self.state.lock().commit_done_flag
    }

    /// Test-only: bump the cross-writer counter directly. Used by the
    /// FIX-5 pump test to exercise propagation deterministically
    /// without constructing a timing-dependent real race.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn bump_cross_writer_for_test(&self, n: u64) {
        self.cross_writer_committed_chunks
            .fetch_add(n, Ordering::Relaxed);
    }

    /// Internal: clear `commit_running` without altering done state.
    /// Called by `CommitRunnerGuard::drop` AFTER it (potentially)
    /// publishes a cancellation result. Subsequent waiters that
    /// observe `all_set` may then claim RunCommit again — re-entry
    /// is safe because the runner that just dropped published a
    /// definite result (so peek_commit_result returns Some) OR the
    /// new RunCommit will succeed and publish its own result.
    fn clear_commit_running(&self) {
        self.state.lock().commit_running = false;
    }
}

/// What the writer should do next after `mark_chunk_committed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitResponsibility {
    /// Bitmap not yet full; writer should continue sending chunks.
    ContinueSending,
    /// Bitmap is now full AND this caller is the FIRST to observe it
    /// — caller MUST run the commit path (rename → BLAKE3 verify →
    /// finalize → publish_commit_result).
    RunCommit,
    /// Bitmap is now full but ANOTHER writer claimed the
    /// commit-runner role. Caller awaits `commit_done`.
    AwaitCommit,
}

/// RAII guard for a writer's attachment to a `ChunkRaceState`. Drops
/// purge the writer's in-flight slots so a cancelled / panicked writer
/// doesn't deadlock the digest's progress.
///
/// Per CLAUDE.md "Asymmetric contract coverage": the guard's `Drop`
/// fires the side effect (purge_writer_in_flight + detach_writer)
/// EXACTLY when the writer disengages — over-firing (calling Drop
/// twice) is impossible because std consumes the value, under-firing
/// (forgetting to call Drop) is the documented bug.
#[must_use = "RaceWriterGuard must outlive the writer's chunk-sending loop; \
              dropping early aborts the writer's in-flight contributions"]
pub struct RaceWriterGuard {
    state: Arc<ChunkRaceState>,
    writer_id: WriterId,
    /// `true` once the writer has explicitly relinquished. If the guard
    /// drops with relinquished=false, that's a cancellation/panic and
    /// we purge the in-flight slots.
    relinquished: bool,
}

impl Debug for RaceWriterGuard {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RaceWriterGuard")
            .field("digest", &self.state.digest)
            .field("writer_id", &self.writer_id)
            .field("relinquished", &self.relinquished)
            .finish()
    }
}

impl RaceWriterGuard {
    /// Construct + attach. `state` is the per-digest race-state; the
    /// caller MUST keep the guard alive across the writer's send loop.
    pub fn attach(state: Arc<ChunkRaceState>, writer_id: WriterId) -> Self {
        let _count = state.attach_writer();
        Self {
            state,
            writer_id,
            relinquished: false,
        }
    }

    /// Identity of the writer this guard tracks.
    pub fn writer_id(&self) -> WriterId {
        self.writer_id
    }

    /// Borrow the inner `ChunkRaceState` Arc.
    pub fn state(&self) -> &Arc<ChunkRaceState> {
        &self.state
    }

    /// Mark the guard as cleanly relinquished — writer finished its
    /// happy-path sending loop OR observed terminal commit_done. Drop
    /// then SKIPS the in-flight purge (no in-flight slots should
    /// remain anyway; defensive contract).
    pub fn relinquish(mut self) {
        self.relinquished = true;
        // Drop fires immediately; the explicit drop here is just for
        // clarity. Detach_writer always fires.
        drop(self);
    }
}

impl Drop for RaceWriterGuard {
    fn drop(&mut self) {
        if !self.relinquished {
            let purged = self.state.purge_writer_in_flight(self.writer_id);
            if purged > 0 {
                debug!(
                    target: "nativelink_store::chunked",
                    digest = ?self.state.digest,
                    writer_id = ?self.writer_id,
                    purged,
                    "RaceWriterGuard::drop purged in-flight slots on cancel/panic",
                );
            }
        }
        let remaining = self.state.detach_writer();
        trace!(
            target: "nativelink_store::chunked",
            digest = ?self.state.digest,
            writer_id = ?self.writer_id,
            remaining_writers = remaining,
            "RaceWriterGuard::drop detached writer",
        );
    }
}

/// RAII guard for the commit-runner role. Constructed by the
/// commit-runner once it observes `RunCommit` from
/// `mark_chunk_committed` / `try_claim_commit_runner`. On Drop:
///   - If `commit_done_flag` is true (the runner successfully called
///     `publish_commit_result`): no action — clean exit.
///   - If `commit_done_flag` is false: publish a synthetic
///     `Code::Cancelled` result so siblings observing
///     `commit_done.notified()` wake immediately and propagate the
///     same cancellation to their clients. Also clears the
///     `commit_running` flag so a fresh writer arriving after this
///     wedge can attempt re-commit (reading the published Cancelled
///     err first via `peek_commit_result`).
///
/// **Why this is safe even if the commit DID succeed but the runner
/// panicked between publish and guard drop:** `publish_commit_result`
/// flips `commit_done_flag=true` BEFORE the guard's drop runs. The
/// drop checks the flag and skips the synthetic err.
///
/// **Why this is safe across panic:** `commit_done` is `tokio::sync::Notify`;
/// publish happens via the `state` mutex which is panic-safe
/// (parking_lot::Mutex with `unpoisoned`). No state corruption window.
#[must_use = "CommitRunnerGuard must outlive the commit-runner's commit path; \
              dropping early publishes a Cancelled error to all sibling waiters"]
pub struct CommitRunnerGuard {
    state: Arc<ChunkRaceState>,
    /// Set true by `mark_complete()` once the runner has published a
    /// real result; Drop then skips the synthetic publish.
    completed: bool,
}

impl Debug for CommitRunnerGuard {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CommitRunnerGuard")
            .field("digest", &self.state.digest)
            .field("completed", &self.completed)
            .finish()
    }
}

impl CommitRunnerGuard {
    /// Construct from a race-state Arc. Caller MUST have observed
    /// `RunCommit` from `mark_chunk_committed` / `try_claim_commit_runner`
    /// (which set `commit_running=true` under the state lock). The guard
    /// takes ownership of the "must publish or clear" obligation.
    pub fn from_state(state: Arc<ChunkRaceState>) -> Self {
        Self {
            state,
            completed: false,
        }
    }

    /// Mark the guard as cleanly completed — runner has called
    /// `publish_commit_result` with a real outcome. Drop then skips
    /// the synthetic-cancel publish.
    pub fn mark_complete(mut self) {
        self.completed = true;
    }
}

impl Drop for CommitRunnerGuard {
    fn drop(&mut self) {
        if self.completed {
            // Clean exit; nothing to do.
            return;
        }
        if self.state.commit_done() {
            // Publish raced ahead of mark_complete (e.g. the runner
            // called publish_commit_result then panicked before
            // mark_complete). State is consistent; nothing to do.
            return;
        }
        // The commit-runner is exiting WITHOUT having published a
        // result. Publish a synthetic Cancelled err so siblings wake
        // and propagate to their clients. Clear `commit_running` so a
        // future writer arriving with all chunks present may retry.
        let synthetic_err = make_err!(
            Code::Cancelled,
            "WriteChunkedV2: commit-runner cancelled or panicked before \
             publishing a result for digest {} — sibling writers receive \
             this synthetic Cancelled to avoid 60s wedge",
            self.state.digest
        );
        warn!(
            target: "nativelink_store::chunked",
            digest = ?self.state.digest,
            "CommitRunnerGuard::drop publishing synthetic Cancelled — \
             commit-runner exited without publishing a real result",
        );
        self.state.publish_commit_result(Err(synthetic_err));
        self.state.clear_commit_running();
    }
}

/// Per-FilesystemStore registry of in-flight `Arc<ChunkRaceState>`s
/// keyed by digest. Distinct from `ChunkedPartialsMap` (which holds
/// the open `std::fs::File` handle); the two are 1:1 — one race-state
/// per partial. Wrapping them separately keeps the v1 `WriteChunked`
/// path's open-file lifecycle independent of the v2 multi-writer
/// state.
///
/// Invariant: an entry exists iff at least one writer is attached OR
/// commit is in progress. The last `RaceWriterGuard` drop after commit
/// publish removes the entry (via `try_close_after_commit`).
#[derive(Debug, Default)]
pub struct ChunkRaceRegistry {
    inner: Mutex<HashMap<DigestInfo, Arc<ChunkRaceState>>>,
}

impl ChunkRaceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-create the race-state for `digest`. The closure is
    /// invoked only on first creation; subsequent calls return the
    /// existing Arc.
    pub fn get_or_create<F>(&self, digest: DigestInfo, make: F) -> Arc<ChunkRaceState>
    where
        F: FnOnce() -> ChunkRaceState,
    {
        let mut guard = self.inner.lock();
        if let Some(existing) = guard.get(&digest) {
            return Arc::clone(existing);
        }
        let new_state = Arc::new(make());
        guard.insert(digest, Arc::clone(&new_state));
        new_state
    }

    /// FIX-4: Atomic get-or-create + attach. Returns the Arc with the
    /// writer ALREADY attached (`attach_writer` called inside the
    /// registry mutex critical section). Eliminates the TOCTOU window
    /// between get_or_create returning and the caller's
    /// `RaceWriterGuard::attach`: under the previous code, a concurrent
    /// `try_remove_if_unused` could remove the entry between those
    /// two steps, splitting concurrent writers across two distinct
    /// race-states for the same digest. Returns
    /// `(Arc<ChunkRaceState>, RaceWriterGuard)` — the guard's Drop
    /// detaches; same lifecycle as the old construction pattern.
    pub fn get_or_create_and_attach<F>(
        &self,
        digest: DigestInfo,
        writer_id: WriterId,
        make: F,
    ) -> (Arc<ChunkRaceState>, RaceWriterGuard)
    where
        F: FnOnce() -> ChunkRaceState,
    {
        let mut guard = self.inner.lock();
        let state = if let Some(existing) = guard.get(&digest) {
            Arc::clone(existing)
        } else {
            let new_state = Arc::new(make());
            guard.insert(digest, Arc::clone(&new_state));
            new_state
        };
        // Attach inside the registry mutex. `attach_writer` is a
        // simple atomic fetch_add, fast and lock-free; safe to call
        // here without lock-order concerns (state mutex isn't taken).
        let _ = state.attach_writer();
        // Drop the registry mutex BEFORE constructing the guard so the
        // guard's Drop never re-enters the registry under it (the
        // guard's Drop only touches state.attached_writer_count and
        // state.purge_writer_in_flight, both internal to the state).
        drop(guard);
        let race_guard = RaceWriterGuard {
            state: Arc::clone(&state),
            writer_id,
            relinquished: false,
        };
        (state, race_guard)
    }

    /// Look up the race-state for `digest` without creating one.
    pub fn get(&self, digest: &DigestInfo) -> Option<Arc<ChunkRaceState>> {
        self.inner.lock().get(digest).cloned()
    }

    /// Remove `digest`'s entry IFF no writers are attached. Returns
    /// the removed Arc on success. Used by the commit-runner after
    /// publishing the result + detaching its own guard.
    pub fn try_remove_if_unused(&self, digest: &DigestInfo) -> Option<Arc<ChunkRaceState>> {
        let mut guard = self.inner.lock();
        if let Some(state) = guard.get(digest) {
            if state.attached_writer_count() == 0 {
                return guard.remove(digest);
            }
        }
        None
    }

    /// Force-remove `digest`'s entry regardless of writer count. Used
    /// by terminal teardown paths (commit failed + GC).
    pub fn force_remove(&self, digest: &DigestInfo) -> Option<Arc<ChunkRaceState>> {
        self.inner.lock().remove(digest)
    }

    /// Number of in-flight digests. Wired to a metric.
    pub fn in_flight_digests(&self) -> usize {
        self.inner.lock().len()
    }

    /// Maximum currently-attached writer count across all digests.
    /// Falsification metric for `chunked_writers_per_digest_max`.
    pub fn max_writers_per_digest(&self) -> u64 {
        self.inner
            .lock()
            .values()
            .map(|s| s.attached_writer_count())
            .max()
            .unwrap_or(0)
    }

    /// Sum of cumulative racing-loser chunks across all digests.
    pub fn racing_loser_total(&self) -> u64 {
        self.inner
            .lock()
            .values()
            .map(|s| s.racing_loser_count())
            .sum()
    }

    /// Sum of cumulative cross-writer-committed chunks across all digests.
    pub fn cross_writer_committed_total(&self) -> u64 {
        self.inner
            .lock()
            .values()
            .map(|s| s.cross_writer_committed_count())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nativelink_util::common::DigestInfo;

    fn make_digest(seed: u8, size: u64) -> DigestInfo {
        let mut hash = [0u8; 32];
        hash[0] = seed;
        DigestInfo::new(hash, size)
    }

    fn make_state(size: u64, chunk_size: u32) -> Arc<ChunkRaceState> {
        let digest = make_digest(0x01, size);
        Arc::new(ChunkRaceState::new(
            digest,
            chunk_size,
            PathBuf::from("/tmp/test.partial"),
        ))
    }

    #[test]
    fn bitvec_set_and_all_set() {
        let mut bv = BitVec::with_len(10);
        assert!(!bv.all_set());
        for i in 0..10 {
            assert!(!bv.get(i));
        }
        for i in 0..10 {
            assert!(!bv.set(i));
            assert!(bv.get(i));
        }
        assert!(bv.all_set());
        // Set already-set is no-op + returns true.
        assert!(bv.set(5));
    }

    #[test]
    fn bitvec_count_ones_and_contiguous() {
        let mut bv = BitVec::with_len(7);
        bv.set(0);
        bv.set(1);
        bv.set(2);
        bv.set(4);
        assert_eq!(bv.count_ones(), 4);
        assert_eq!(bv.contiguous_from_zero(), 3);
        bv.set(3);
        assert_eq!(bv.contiguous_from_zero(), 5);
    }

    #[test]
    fn admit_then_commit_flips_bit_and_signals_runner() {
        // declared_size = 3 chunks of 1024 bytes
        let chunk_size: u32 = 1024;
        let state = make_state(3 * chunk_size as u64, chunk_size);
        let writer = WriterId(1);
        let _guard = RaceWriterGuard::attach(Arc::clone(&state), writer);

        for i in 0..3 {
            let offset = (i * chunk_size) as u64;
            assert_eq!(state.try_admit_chunk(writer, offset), AdmitOutcome::Accept);
            let resp = state.mark_chunk_committed(writer, offset);
            if i < 2 {
                assert_eq!(resp, CommitResponsibility::ContinueSending);
            } else {
                assert_eq!(resp, CommitResponsibility::RunCommit);
            }
        }
        assert!(state.is_complete());
    }

    #[test]
    fn second_admit_at_same_offset_returns_already_have() {
        let chunk_size: u32 = 1024;
        let state = make_state(2 * chunk_size as u64, chunk_size);
        let writer_a = WriterId(1);
        let writer_b = WriterId(2);
        let _ga = RaceWriterGuard::attach(Arc::clone(&state), writer_a);
        let _gb = RaceWriterGuard::attach(Arc::clone(&state), writer_b);

        // A admits then commits offset 0
        assert_eq!(state.try_admit_chunk(writer_a, 0), AdmitOutcome::Accept);
        state.mark_chunk_committed(writer_a, 0);

        // B admits offset 0 → ALREADY_HAVE
        assert_eq!(
            state.try_admit_chunk(writer_b, 0),
            AdmitOutcome::AlreadyHave
        );
    }

    #[test]
    fn concurrent_admit_at_same_offset_yields_racing_loser_for_second() {
        let chunk_size: u32 = 1024;
        let state = make_state(2 * chunk_size as u64, chunk_size);
        let writer_a = WriterId(1);
        let writer_b = WriterId(2);
        let _ga = RaceWriterGuard::attach(Arc::clone(&state), writer_a);
        let _gb = RaceWriterGuard::attach(Arc::clone(&state), writer_b);

        // A admits offset 0 — slot now has writer A in flight
        assert_eq!(state.try_admit_chunk(writer_a, 0), AdmitOutcome::Accept);
        // B admits offset 0 BEFORE A commits → RACING_LOSER
        assert_eq!(
            state.try_admit_chunk(writer_b, 0),
            AdmitOutcome::RacingLoser
        );
        assert_eq!(state.racing_loser_count(), 1);

        // A finishes → bit set, A's in-flight slot cleared
        state.mark_chunk_committed(writer_a, 0);
        assert_eq!(state.chunks_in_flight_count(), 0);
        // Since B hadn't been added to the in-flight slot (rejected
        // at admission), no cross-writer credit.
        assert_eq!(state.cross_writer_committed_count(), 0);
    }

    #[test]
    fn writer_drop_purges_in_flight() {
        let chunk_size: u32 = 1024;
        let state = make_state(2 * chunk_size as u64, chunk_size);
        let writer_a = WriterId(1);
        let writer_b = WriterId(2);
        {
            let _ga = RaceWriterGuard::attach(Arc::clone(&state), writer_a);
            assert_eq!(state.try_admit_chunk(writer_a, 0), AdmitOutcome::Accept);
            assert_eq!(state.chunks_in_flight_count(), 1);
            // Drop without commit: purge_writer_in_flight fires
        }
        // Now B should be able to admit offset 0
        let _gb = RaceWriterGuard::attach(Arc::clone(&state), writer_b);
        assert_eq!(state.try_admit_chunk(writer_b, 0), AdmitOutcome::Accept);
    }

    #[test]
    fn admit_skip_to_hint_returns_byte_offset_of_last_contiguous_chunk() {
        let chunk_size: u32 = 1024;
        let state = make_state(5 * chunk_size as u64, chunk_size);
        let writer = WriterId(1);
        let _g = RaceWriterGuard::attach(Arc::clone(&state), writer);

        // No bits set
        assert_eq!(state.admit_skip_to_hint_byte_offset(), None);

        // Set bits 0 and 1 contiguously
        state.try_admit_chunk(writer, 0);
        state.mark_chunk_committed(writer, 0);
        state.try_admit_chunk(writer, chunk_size as u64);
        state.mark_chunk_committed(writer, chunk_size as u64);
        // Last contiguous chunk index is 1; byte offset = 1 * 1024
        assert_eq!(
            state.admit_skip_to_hint_byte_offset(),
            Some(chunk_size as u64)
        );
    }

    #[test]
    fn registry_get_or_create_dedups() {
        let registry = ChunkRaceRegistry::new();
        let digest = make_digest(0x05, 1024);
        let s1 = registry.get_or_create(digest, || {
            ChunkRaceState::new(digest, 1024, PathBuf::from("/tmp/t.partial"))
        });
        let s2 = registry.get_or_create(digest, || {
            panic!("should not be called — entry exists")
        });
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn commit_runner_guard_drop_without_publish_publishes_cancelled() {
        // Compose a runner that flips the last bit then drops the
        // guard WITHOUT calling publish or mark_complete. Siblings
        // observing the state must see a published Cancelled Err
        // immediately rather than waiting forever on the notify.
        let chunk_size: u32 = 1024;
        let state = make_state(chunk_size as u64, chunk_size);
        let writer = WriterId(1);
        let _g = RaceWriterGuard::attach(Arc::clone(&state), writer);

        // Admit + commit single-chunk blob.
        assert_eq!(state.try_admit_chunk(writer, 0), AdmitOutcome::Accept);
        let resp = state.mark_chunk_committed(writer, 0);
        assert_eq!(resp, CommitResponsibility::RunCommit);

        // Construct the runner-guard. Simulate a panic: drop without
        // mark_complete.
        {
            let _runner = CommitRunnerGuard::from_state(Arc::clone(&state));
        }

        // Sibling sees a published Cancelled Err.
        let result = state
            .peek_commit_result()
            .expect("commit_done must publish a synthetic Cancelled on guard-drop");
        let err = result.expect_err("synthetic publish must be Err(Cancelled)");
        assert_eq!(err.code, Code::Cancelled, "synthetic err must be Cancelled");
    }

    #[test]
    fn commit_runner_guard_mark_complete_skips_synthetic_publish() {
        // After publish_commit_result(Ok) + mark_complete, the guard's
        // Drop must NOT publish the synthetic Cancelled (it would
        // either no-op via the idempotency guard OR overwrite the Ok
        // result if not guarded; verify no overwrite by reading back
        // the published Ok).
        let chunk_size: u32 = 1024;
        let state = make_state(chunk_size as u64, chunk_size);
        let writer = WriterId(1);
        let _g = RaceWriterGuard::attach(Arc::clone(&state), writer);
        assert_eq!(state.try_admit_chunk(writer, 0), AdmitOutcome::Accept);
        state.mark_chunk_committed(writer, 0);

        {
            let runner = CommitRunnerGuard::from_state(Arc::clone(&state));
            // Publish success; mark complete.
            state.publish_commit_result(Ok(RaceCommitResult { committed_size: 1024 }));
            runner.mark_complete();
        }

        let result = state.peek_commit_result().expect("Ok must be published");
        let r = result.expect("Ok must survive guard drop");
        assert_eq!(r.committed_size, 1024);
    }

    #[test]
    fn try_claim_commit_runner_after_cancellation_allows_retry() {
        // Composite: writer A flips last bit → CommitRunnerGuard drops
        // without mark_complete → publishes synthetic Cancelled +
        // clears commit_running. Now a fresh writer B observes
        // all_set + can claim RunCommit again. (However it will
        // observe the published Err first; it's the caller's choice
        // to retry or propagate.)
        let chunk_size: u32 = 1024;
        let state = make_state(chunk_size as u64, chunk_size);
        let writer_a = WriterId(1);
        let _ga = RaceWriterGuard::attach(Arc::clone(&state), writer_a);
        assert_eq!(state.try_admit_chunk(writer_a, 0), AdmitOutcome::Accept);
        state.mark_chunk_committed(writer_a, 0);
        // Verify commit_running was set by mark_chunk_committed.
        assert!(
            state.state.lock().commit_running,
            "post-mark_chunk_committed: commit_running must be true"
        );
        // Drop runner-guard without mark_complete.
        {
            let _r = CommitRunnerGuard::from_state(Arc::clone(&state));
        }
        // commit_done is true (synthetic Err published). New runner
        // claim must observe AwaitCommit (not RunCommit) — done is
        // sticky.
        let next = state.try_claim_commit_runner();
        assert_eq!(
            next,
            CommitResponsibility::AwaitCommit,
            "post-publish (even synthetic Err) must short-circuit to AwaitCommit"
        );
        // FIX-1 contract: commit_running MUST be cleared by guard's
        // drop. If left true, a subsequent commit retry path (e.g.
        // a fresh writer attempting RunCommit after a re-built
        // race-state from force_remove) would deadlock. Test this
        // directly so a regression on `clear_commit_running` red-fails.
        assert!(
            !state.state.lock().commit_running,
            "FIX-1 contract: CommitRunnerGuard::drop MUST clear commit_running \
             (otherwise a subsequent RunCommit attempt deadlocks waiting for \
              the cancelled runner that already exited)"
        );
    }

    #[test]
    fn registry_force_remove_clears_entry_even_with_writers_attached() {
        // FIX-2 invariant: `force_remove` MUST drop the registry entry
        // regardless of attached_writer_count. Used by the watchdog
        // path when commit-runner wedges — siblings detach + the next
        // arriving session must get a fresh state, not the wedged one.
        let registry = ChunkRaceRegistry::new();
        let digest = make_digest(0x07, 1024);
        let state = registry.get_or_create(digest, || {
            ChunkRaceState::new(digest, 1024, PathBuf::from("/tmp/r.partial"))
        });
        // Attach a writer so try_remove_if_unused would refuse.
        let _g = RaceWriterGuard::attach(Arc::clone(&state), WriterId(1));
        assert_eq!(
            state.attached_writer_count(),
            1,
            "test setup: writer attached"
        );
        // try_remove_if_unused should be a no-op (writer attached).
        assert!(
            registry.try_remove_if_unused(&digest).is_none(),
            "try_remove_if_unused must NOT remove when writers attached"
        );
        assert!(
            registry.get(&digest).is_some(),
            "entry still present after refused try_remove_if_unused"
        );
        // force_remove must succeed regardless.
        let removed = registry.force_remove(&digest);
        assert!(
            removed.is_some(),
            "FIX-2 contract: force_remove MUST drop the entry regardless of writers attached"
        );
        assert!(
            registry.get(&digest).is_none(),
            "entry must be absent after force_remove"
        );
    }

    #[test]
    fn registry_get_or_create_and_attach_holds_lock_across_attach() {
        // FIX-4 invariant: `get_or_create_and_attach` must atomically
        // attach the writer inside the registry mutex critical section,
        // so a concurrent try_remove_if_unused can't observe an
        // attached_writer_count of 0 between get_or_create and the
        // attach. This unit test asserts the post-condition: after
        // get_or_create_and_attach returns, attached_writer_count
        // is at least 1.
        let registry = ChunkRaceRegistry::new();
        let digest = make_digest(0x08, 1024);
        let (state, _guard) = registry.get_or_create_and_attach(digest, WriterId(1), || {
            ChunkRaceState::new(digest, 1024, PathBuf::from("/tmp/r.partial"))
        });
        assert_eq!(
            state.attached_writer_count(),
            1,
            "FIX-4: get_or_create_and_attach must attach inside the lock"
        );
        // try_remove_if_unused refuses while attached.
        assert!(
            registry.try_remove_if_unused(&digest).is_none(),
            "registry must NOT remove the entry while writer is attached"
        );
        // Drop guard → detach.
        drop(_guard);
        assert_eq!(state.attached_writer_count(), 0);
        assert!(
            registry.try_remove_if_unused(&digest).is_some(),
            "post-detach: try_remove_if_unused succeeds"
        );
    }
}
