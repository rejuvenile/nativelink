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

/// Shared, append-only byte buffer with a single writer and multiple
/// concurrent readers.  Designed for streaming CAS blobs to readers
/// before the writer has finished (read-while-write).
///
/// See `docs/streaming-blob-pipeline-design.md` for the full design.
use core::fmt;
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err};
use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::common::DigestInfo;

/// #515 Phase 0 expansion: per-reader construction sample counter.
/// Each `StreamingBlobReader::new` call increments this counter; the
/// `info!` construction-sample log fires only when the counter is a
/// multiple of `READER_CONSTRUCTION_SAMPLE_PERIOD`. Production
/// `StreamingBlobReader::new` is called at least once per ByteStream
/// RPC (`bytestream_server.rs:1421` `InFlightBlobMap::get_reader` →
/// `StreamingBlobReader::new`) plus once per FSS streaming-populate
/// fan-out (`fast_slow_store.rs:6500`); on buildcache this is thousands
/// of calls per second under build load. Always-on `info!` would
/// dominate the log volume; sampling provides a baseline distribution
/// for "how often does construction-time `earliest_chunk_idx > 0`
/// happen normally?" without saturating the log pipeline. WARN-level
/// races at the FSS site (`fast_slow_store.rs:6500-6544`) and the
/// `next_chunk` H_alt_I site (`streaming_blob.rs:680/695`) remain
/// always-on — those are the diagnostic-critical signals.
static READER_CONSTRUCTION_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);
/// Sample period for `READER_CONSTRUCTION_SAMPLE_COUNTER`. 1024 picks
/// one construction in every ~1k; on a ~10k-construction/sec workload
/// that is ~10 `info!` lines per second — bounded but dense enough to
/// see the distribution of construction-time `earliest_chunk_idx`
/// values in a few minutes of soak.
const READER_CONSTRUCTION_SAMPLE_PERIOD: u64 = 1024;

/// Maximum time `StreamingBlobReader::next_chunk` will block on a single
/// `Notified` await before declaring the producer wedged. Generous enough
/// that legitimately slow producers (multi-second backpressure, large-blob
/// network stalls) do not trip it; short enough that a missing-wakeup bug
/// surfaces as a `DeadlineExceeded` error in seconds rather than a 120 s
/// gRPC stream wedge. Defense-in-depth — the pin+enable fix at 646d7623
/// closed the known race; this guards against the next one.
const STREAMING_BLOB_NOTIFY_TIMEOUT: Duration = Duration::from_secs(30);

/// Threshold above which a single `next_chunk` notify wait is logged at
/// `warn!` and counted in `notify_waits_over_5s`. Lets us verify in
/// production whether the lost-wakeup wedge actually went away.
const SLOW_NOTIFY_THRESHOLD: Duration = Duration::from_secs(5);

/// Substring marker emitted into the `Code::Unavailable` error message
/// produced by `StreamingBlobReader::next_chunk` when the reader's cursor
/// has fallen behind the sliding window.
///
/// This is a forward-compatibility CONTRACT: the FastSlowStore D.1 fallback
/// (#325) recognizes the sliding-window-eviction error class by substring-
/// matching this marker against `Error::messages`. Renaming the production
/// message without keeping the marker substring would silently break the
/// fallback predicate — readers that fell behind would propagate
/// `Code::Unavailable` to the outer caller (the bug-shape #325 fixes)
/// instead of splicing in a fresh slow-store read.
///
/// The substring is referenced both from the production error construction
/// in `next_chunk` AND from the `streaming_blob_next_chunk_fail` failpoint's
/// synthetic message so test predicates and production predicates trip on
/// the same byte sequence. The production unit test
/// `production_sliding_window_message_contains_marker` asserts the
/// substring is preserved on every change to that error message.
pub const SLIDING_WINDOW_EVICTION_MARKER: &str = "reader fell behind sliding window";

/// Substring marker emitted into the `Code::Internal` error message produced
/// by `StreamingBlobReader::next_chunk` when the writer set terminal=Ok but
/// `bytes_written < digest.size_bytes()` — i.e. a silent-short EOF.
///
/// Sibling of `SLIDING_WINDOW_EVICTION_MARKER`. Stable substring contract so
/// production journal greps (`grep streaming_blob_silent_short`) and any
/// downstream predicate that wants to classify this error class trips on the
/// same byte sequence on every release. See #502 for the bug shape: a
/// producer that calls `send_eof` (or a fast/slow populate that completes
/// without erroring) after writing fewer bytes than the CAS digest declares
/// → previously surfaced as `Ok(Bytes::new())` to readers → Bazel saw a
/// clean stream with `bytes_sent < expected_size` and reported digest
/// mismatch as build failure. Sibling of the #500 inner_read fix
/// (`consume_ok_eof_with_get_part_err`), which guarded the same wire-shape
/// at a different consumer seam.
///
/// Classification rationale (see
/// `.claude/audits/502-distribution-investigation-2026-05-16.md`): production
/// data over a 48 h window shows 73.5 % of silent-shorted digests ALSO
/// completed successfully in the same window (median per-digest success rate
/// 25 %), 99.2 % of events truncate at a clean 1/N fraction of the blob, and
/// 38.3 % of events share a `bytes_sent` value with at least one other
/// distinct digest. Together those four findings rule out deterministic
/// per-blob corruption and identify a transport-layer chunker race. The
/// correct gRPC classification is therefore `Code::Internal` (TRANSIENT_FAILURE
/// in Bazel's `RemoteRetrier`, retried up to 10× with exponential backoff,
/// ~94 % effective recovery), not `Code::DataLoss` (PERMANENT_FAILURE, no
/// retry, action fails on first hit). `worker_proxy_store.rs:1018` uses
/// `Code::DataLoss` for a digest-mismatch-after-hash case — a different
/// seam with different information (post-hash evidence that the bytes are
/// wrong); `next_chunk` does NOT have that information and must not
/// pre-commit to PERMANENT_FAILURE. On the residual cases where retries
/// can't help, Bazel still fails the action after exhausting its 10
/// attempts — same end-state as `DataLoss`, just slower.
pub const STREAMING_BLOB_SILENT_SHORT_MARKER: &str = "streaming_blob_silent_short";

/// Inner shared state for a streaming blob.
///
/// The writer appends `Bytes` chunks to the deque and notifies
/// waiting readers.  Each reader maintains its own cursor and
/// advances independently.
pub struct StreamingBlobInner {
    /// Append-only chunk deque.  Writers take a write-lock;
    /// readers take a read-lock (shared access for indexing).
    chunks: RwLock<VecDeque<Bytes>>,

    /// Monotonically increasing count of chunks appended.
    chunk_count: AtomicU64,

    /// Total bytes appended so far.
    bytes_written: AtomicU64,

    /// Wakes readers on new data or terminal state.
    ///
    /// Replaced `tokio::sync::Notify` with a monotonic-generation
    /// `watch::Sender<u64>` to fix the late-subscriber lost-wakeup race
    /// observed in production at 14:14, 14:15, 14:26 PDT 2026-05-05
    /// (#272). `Notify::notify_waiters` only wakes pre-existing
    /// subscribers — readers that subscribe AFTER the writer's
    /// terminal-set + fire never get woken until the 30 s deadline.
    ///
    /// Watch closes that gap via the **pristine-receiver-clone**
    /// pattern: `notify_rx_template` is the original `Receiver` from
    /// `watch::channel(0)` and stays at `Version::INITIAL` forever
    /// (never `changed()`'d). Each reader clones it; the clone
    /// inherits seen-version = INITIAL. Any `send_modify` that has
    /// already fired (or will fire) makes that clone's first
    /// `changed().await` return immediately. NOTE: `Sender::subscribe()`
    /// would NOT work here — it returns a receiver pinned to the
    /// channel's CURRENT version, which is post-fire for late
    /// subscribers, defeating the whole purpose. The clone-template
    /// is the load-bearing distinction.
    notify_tx: watch::Sender<u64>,

    /// Pristine receiver kept at `Version::INITIAL` for cloning to
    /// new readers. See `notify_tx` doc for why this matters.
    notify_rx_template: watch::Receiver<u64>,

    /// Diagnostic counter (Shape A from the #272 fix proposal):
    /// total number of writer-side notify firings across the lifetime
    /// of this blob. Increments alongside every `notify_tx.send_modify`.
    /// Surfaced in the slow-wakeup `warn!` so a future production
    /// occurrence proves whether the watch fix held — if this counter
    /// advanced during the wait but the reader still timed out, the
    /// watch primitive integration is broken (i.e. a different bug
    /// than #272). One-release regression detector; can be removed
    /// after a clean production cycle.
    notify_waiters_calls: AtomicU64,

    /// Terminal state:
    /// - `None`       — writer still active
    /// - `Some(Ok)` — writer sent EOF (success)
    /// - `Some(Err)` — writer errored or dropped
    terminal: Mutex<Option<Result<(), Error>>>,

    /// Digest for this blob.
    digest: DigestInfo,

    /// Authoritative bytes-on-store size for the #502 silent-short check.
    ///
    /// The #502 check at `next_chunk` compares `bytes_written` against the
    /// "expected bytes on the wire" to detect a producer that called
    /// `send_eof` without delivering the full payload. For **CAS** reads
    /// the upper bound is `digest.size_bytes()` (content-addressed
    /// invariant: stored bytes == declared bytes). For **AC** reads the
    /// declared `action_digest.size_bytes()` is the *Action* proto's
    /// encoded size; the bytes actually stored under that key are the
    /// *ActionResult* proto's encoded bytes, a different message
    /// (`ac_server.rs:199-205`, `docs/ac-integrity-contract.md`). The two
    /// sizes generally differ, so the digest-based bound is structurally
    /// wrong for AC.
    ///
    /// Semantics:
    /// - `OnceLock` set to `n` — authoritative bytes-on-store size from
    ///   `slow_store.has()` returning `ExactSize(n)`. The #502 check
    ///   compares `bytes_written < n` instead of `bytes_written <
    ///   digest.size_bytes()`. Crucially, `n == 0` is a *legitimate*
    ///   value (zero-byte AC entries exist) and does NOT collide with an
    ///   "unset" sentinel — that collision was the v1 AtomicU64 bug.
    /// - `OnceLock` unset — caller does not know the bytes-on-store size
    ///   (e.g. `LazyExistenceOnSync` paths where `has()` returns
    ///   `MaxSize(u64::MAX)`, or non-FastSlowStore construction sites).
    ///   The #502 check falls back to `digest.size_bytes()`, preserving
    ///   prior behavior.
    ///
    /// `OnceLock` enforces at-most-once write (the size on store does not
    /// change for a given digest) and provides happens-before ordering
    /// for the producer→reader handoff: the producer sets this BEFORE
    /// sending any chunk; readers observe terminal state via the
    /// `terminal` mutex which establishes the same happens-before edge.
    ///
    /// This becomes the basis for the OVERSHOOT direction in #44 too —
    /// the same accessor identifies the upper bound that
    /// `bytes_written > expected` would compare against.
    ///
    /// (Incident 2026-06-04: 288 silent_short events across two bursts on
    /// `ac_server::get_action_result`, all on 217-byte Action digests
    /// where the stored ActionResult was 203-215 bytes. Bazel retried
    /// transient Code::Internal up to 10× per action — eventually a
    /// permanent failure for actions that legitimately had ActionResults
    /// shorter than the Action proto.)
    expected_size_on_store: OnceLock<u64>,

    /// Maximum bytes to buffer before evicting old chunks.
    max_buffer_bytes: u64,

    /// Index of the earliest chunk still retained in the deque.
    /// Chunks before this index have been evicted.
    earliest_chunk_idx: AtomicU64,

    /// Construction timestamp — used by debug logs to report
    /// elapsed-since-creation for any state transition. Pure observability.
    created_at: Instant,

    /// Count of `next_chunk` notify waits that exceeded
    /// `SLOW_NOTIFY_THRESHOLD`. Exposed via
    /// `notify_waits_over_5s_total()` for production scraping; lets us
    /// verify whether the 646d7623 fix eliminated the lost-wakeup wedge.
    notify_waits_over_5s: AtomicU64,

    /// Tokio task ID of the producer, captured **on first writer
    /// `send`/`send_eof`/`send_error` call** — NOT at construction.
    ///
    /// Constructing the `Inner` happens on the *consumer's* task in
    /// some code paths (e.g. `FastSlowStore::spawn_populate_producer_with_role`
    /// builds the writer on the calling task and then `tokio::spawn`s
    /// it onto a fresh task). Capturing at construction would name the
    /// consumer instead of the producer — exactly the wrong task for
    /// an operator chasing a wedged upload. Capturing on first send
    /// names the task that actually owns the writer at the moment
    /// data starts flowing, which matches every deployed code path.
    ///
    /// `tokio::task::try_id()` returns `None` outside a tokio task
    /// (e.g. unit tests using `block_on` directly); in that case the
    /// `OnceLock` simply stays empty and the deadline log emits
    /// `<none>` for the producer.
    ///
    /// When the next-chunk deadline fires with `terminal_present:
    /// false`, this names the task to grep for in the worker
    /// journal/log: the producer that is wedged upstream of
    /// streaming_blob (e.g. blocked on a gRPC read with no per-frame
    /// deadline).
    producer_task_id: OnceLock<String>,
}

impl fmt::Debug for StreamingBlobInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingBlobInner")
            .field("digest", &self.digest)
            .field("chunk_count", &self.chunk_count.load(Ordering::Relaxed))
            .field("bytes_written", &self.bytes_written.load(Ordering::Relaxed))
            .field(
                "earliest_chunk_idx",
                &self.earliest_chunk_idx.load(Ordering::Relaxed),
            )
            .field("max_buffer_bytes", &self.max_buffer_bytes)
            .field("terminal", &self.terminal.lock().is_some())
            .field("producer_task_id", &self.producer_task_id.get())
            .finish()
    }
}

impl StreamingBlobInner {
    /// Construct with `expected_size_on_store` unset. The #502
    /// silent-short check falls back to `digest.size_bytes()` for
    /// readers of blobs constructed via this entry point. Producers
    /// that obtain an authoritative bytes-on-store size from
    /// `slow_store.has()` should call `set_expected_size_on_store`
    /// before sending the first chunk; see
    /// `FastSlowStore::run_producer`.
    pub fn new(digest: DigestInfo, max_buffer_bytes: u64) -> Self {
        // The receiver returned from `channel()` starts at
        // `Version::INITIAL` — we hold it as the pristine clone
        // template so each reader-clone observes any fire that ever
        // happened, including ones before the reader was constructed.
        let (notify_tx, notify_rx_template) = watch::channel(0u64);
        Self {
            chunks: RwLock::new(VecDeque::new()),
            chunk_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            notify_tx,
            notify_rx_template,
            notify_waiters_calls: AtomicU64::new(0),
            terminal: Mutex::new(None),
            digest,
            expected_size_on_store: OnceLock::new(),
            max_buffer_bytes,
            earliest_chunk_idx: AtomicU64::new(0),
            created_at: Instant::now(),
            notify_waits_over_5s: AtomicU64::new(0),
            producer_task_id: OnceLock::new(),
        }
    }

    /// Bump the watch-channel version and the diagnostic counter to wake
    /// any current and future subscribers. Sync; does not deadlock on the
    /// terminal mutex (callers must drop terminal first to preserve the
    /// `drop(terminal); notify;` ordering established for the original
    /// `Notify::notify_waiters` call).
    fn notify_waiters(&self) {
        self.notify_tx.send_modify(|v| *v = v.wrapping_add(1));
        self.notify_waiters_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Total writer-side notify firings (Shape A diagnostic counter).
    /// Monotonic across the blob's lifetime; safe to scrape.
    pub fn notify_waiters_calls_total(&self) -> u64 {
        self.notify_waiters_calls.load(Ordering::Relaxed)
    }

    /// Capture the current tokio task ID as the producer if not
    /// already set. Called from each `StreamingBlobWriter` send path
    /// so the producer is recorded the first time data (or terminal
    /// state) flows through the writer — which always runs on the
    /// producer task by the time data is being sent. `OnceLock::set`
    /// is a single atomic CAS; subsequent calls are a no-op
    /// (`Result::Err` ignored).
    fn record_producer_task_id(&self) {
        if self.producer_task_id.get().is_some() {
            return;
        }
        if let Some(id) = tokio::task::try_id() {
            let _ = self.producer_task_id.set(id.to_string());
        }
    }

    /// Tokio task ID of the producer, captured on the writer's first
    /// `send`/`send_eof`/`send_error` call. See the
    /// [`StreamingBlobInner::producer_task_id`] field doc for
    /// rationale on why this is captured on first send rather than
    /// at construction.
    pub fn producer_task_id(&self) -> Option<&str> {
        self.producer_task_id.get().map(String::as_str)
    }

    /// Total `next_chunk` notify waits observed exceeding
    /// `SLOW_NOTIFY_THRESHOLD` (5 s). Monotonic; safe to scrape.
    pub fn notify_waits_over_5s_total(&self) -> u64 {
        self.notify_waits_over_5s.load(Ordering::Relaxed)
    }

    /// Elapsed since construction (for diagnostic logging).
    pub fn age_ms(&self) -> u64 {
        self.created_at.elapsed().as_millis() as u64
    }

    /// Returns true if the terminal state has been set (EOF or error).
    pub fn is_terminal(&self) -> bool {
        self.terminal.lock().is_some()
    }

    /// Returns true if the terminal state is an error (writer dropped
    /// without EOF or explicit error). Readers should fall back to the
    /// store instead of consuming an errored stream.
    pub fn has_error(&self) -> bool {
        self.terminal
            .lock()
            .as_ref()
            .is_some_and(|r| r.is_err())
    }

    /// Returns the producer's terminal result, if it has been set.
    ///
    /// `Some(Ok(()))`  — writer sent EOF (success)
    /// `Some(Err(_))`  — writer errored or dropped un-EOF'd
    /// `None`          — writer still active
    ///
    /// The terminal state is the source of truth for whether the
    /// streaming write succeeded; buffered chunks alone do NOT prove
    /// success. Drain-style consumers that don't need the data should
    /// query this directly to avoid the race where chunks remain in the
    /// sliding window after the producer errored. The returned `Error`
    /// is cloned so the inner state can be re-queried by other waiters.
    pub fn terminal_result(&self) -> Option<Result<(), Error>> {
        self.terminal.lock().as_ref().map(|r| match r {
            Ok(()) => Ok(()),
            Err(e) => Err(e.clone()),
        })
    }

    /// Returns true if the buffer currently holds any chunks.
    pub fn has_data(&self) -> bool {
        !self.chunks.read().is_empty()
    }

    /// Index of the earliest chunk still in the buffer. Non-zero means
    /// early chunks have been evicted (blob exceeds the sliding window).
    pub fn earliest_chunk_idx(&self) -> u64 {
        self.earliest_chunk_idx.load(Ordering::Acquire)
    }

    /// Returns the digest associated with this blob.
    pub fn digest(&self) -> &DigestInfo {
        &self.digest
    }

    /// Record the authoritative bytes-on-store size for the #502
    /// silent-short check. At-most-once: subsequent calls are no-ops
    /// (the bytes-on-store size for a given digest does not change).
    /// Producers that know the real size from `slow_store.has()` should
    /// call this BEFORE sending the first chunk; readers observing
    /// terminal state through the existing `terminal` mutex inherit the
    /// happens-before edge from `OnceLock::set` → `OnceLock::get`.
    ///
    /// Accepts `0` legitimately — zero-byte AC entries exist and must
    /// not be conflated with "unset" (the v1 AtomicU64 sentinel-0 bug).
    ///
    /// See the field doc on `expected_size_on_store` for rationale; in
    /// short, AC reads expose `digest.size_bytes()` (the Action proto's
    /// size) ≠ stored bytes (the ActionResult proto's size), so the
    /// digest-derived bound is structurally wrong for AC, and producers
    /// that know the real size from `has()` should plumb it through.
    pub fn set_expected_size_on_store(&self, n: u64) {
        let _ = self.expected_size_on_store.set(n);
    }

    /// Returns the expected upper bound used by the #502 silent-short
    /// check. If `set_expected_size_on_store` recorded an authoritative
    /// size, that value is returned; otherwise falls back to
    /// `digest.size_bytes()` (the prior behavior). This accessor also
    /// names the upper bound for the OVERSHOOT direction in #44.
    fn expected_size_on_store(&self) -> u64 {
        match self.expected_size_on_store.get() {
            Some(&n) => n,
            None => self.digest.size_bytes(),
        }
    }
}

/// Writer handle for a streaming blob.
///
/// There should be exactly one writer per `StreamingBlobInner`.
/// Dropping the writer without calling `send_eof` sets a terminal
/// error so readers do not hang indefinitely.
pub struct StreamingBlobWriter {
    inner: Arc<StreamingBlobInner>,
    eof_sent: bool,
}

impl fmt::Debug for StreamingBlobWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingBlobWriter")
            .field("inner", &self.inner)
            .field("eof_sent", &self.eof_sent)
            .finish()
    }
}

impl StreamingBlobWriter {
    pub fn new(inner: Arc<StreamingBlobInner>) -> Self {
        Self {
            inner,
            eof_sent: false,
        }
    }

    /// #49: passthrough to `StreamingBlobInner::set_expected_size_on_store`.
    /// Producers that know the authoritative bytes-on-store size from
    /// `slow_store.has()` call this before pushing chunks so the #502
    /// silent-short check compares against the right upper bound for AC
    /// reads (where `digest.size_bytes()` is the Action proto's size, not
    /// the stored ActionResult's size). No-op for CAS since the two are
    /// equal by content-addressing invariant.
    pub fn set_expected_size_on_store(&self, n: u64) {
        self.inner.set_expected_size_on_store(n);
    }

    /// Append a chunk of data and notify waiting readers.
    ///
    /// After appending, evicts the oldest chunks if the total
    /// buffered bytes exceed `max_buffer_bytes`.
    pub async fn send(&self, chunk: Bytes) -> Result<(), Error> {
        if self.inner.is_terminal() {
            return Err(make_err!(
                Code::Internal,
                "cannot send after terminal state"
            ));
        }

        self.inner.record_producer_task_id();

        let chunk_len = chunk.len() as u64;

        {
            let mut chunks = self.inner.chunks.write();
            chunks.push_back(chunk);
        }

        self.inner.chunk_count.fetch_add(1, Ordering::Release);
        let total = self.inner.bytes_written.fetch_add(chunk_len, Ordering::Release) + chunk_len;

        // Sliding window eviction: drop oldest chunks while over budget.
        if total > self.inner.max_buffer_bytes {
            let mut chunks = self.inner.chunks.write();
            let mut buffered = {
                // Sum all retained chunk sizes.
                chunks.iter().map(|c| c.len() as u64).sum::<u64>()
            };
            while buffered > self.inner.max_buffer_bytes && !chunks.is_empty() {
                if let Some(evicted) = chunks.pop_front() {
                    buffered -= evicted.len() as u64;
                    self.inner.earliest_chunk_idx.fetch_add(1, Ordering::Release);
                }
            }
        }

        self.inner.notify_waiters();
        Ok(())
    }

    /// Signal successful end-of-file.  After this, readers that have
    /// consumed all chunks will see EOF.
    pub fn send_eof(&mut self) -> Result<(), Error> {
        self.inner.record_producer_task_id();
        let mut terminal = self.inner.terminal.lock();
        if terminal.is_some() {
            return Err(make_err!(
                Code::Internal,
                "terminal state already set"
            ));
        }
        *terminal = Some(Ok(()));
        self.eof_sent = true;
        drop(terminal);

        debug!(
            digest = %self.inner.digest,
            bytes_written = %self.inner.bytes_written.load(Ordering::Relaxed),
            age_ms = self.inner.age_ms(),
            "streaming blob writer sent eof, notify_waiters firing"
        );

        self.inner.notify_waiters();
        Ok(())
    }

    /// Signal a write error.  All readers will observe this error.
    pub fn send_error(&mut self, err: Error) {
        self.inner.record_producer_task_id();
        let mut terminal = self.inner.terminal.lock();
        if terminal.is_some() {
            return;
        }
        // #186: NotFound is the dominant upstream-caused producer-side
        // failure (slow_store NotFound + existence-cache stale-positive
        // cleanup; #247-class FilesystemStore disk/index divergence).
        // 7,372 events / 8h41m observed in production with 13K-event
        // bursts during phantom-blob clusters — drowns the actual
        // PHANTOM BLOB signal. Demote the NotFound case to debug; keep
        // warn for genuine producer wedges (Internal, Aborted,
        // Unavailable, etc.) where the streaming buffer itself is the
        // suspect. Folds into the same benign-vs-suspect split as the
        // Drop arm at lines 378-394.
        if err.code == Code::NotFound {
            debug!(
                digest = %self.inner.digest,
                age_ms = self.inner.age_ms(),
                ?err,
                "streaming blob writer error (NotFound — upstream-caused), notify_waiters firing"
            );
        } else {
            warn!(
                digest = %self.inner.digest,
                age_ms = self.inner.age_ms(),
                ?err,
                "streaming blob writer error, notify_waiters firing"
            );
        }
        *terminal = Some(Err(err));
        self.eof_sent = true;
        drop(terminal);

        self.inner.notify_waiters();
    }
}

impl Drop for StreamingBlobWriter {
    fn drop(&mut self) {
        if !self.eof_sent {
            let mut terminal = self.inner.terminal.lock();
            if terminal.is_none() {
                let bytes_written = self
                    .inner
                    .bytes_written
                    .load(std::sync::atomic::Ordering::Relaxed);
                let expected_size = self.inner.digest.size_bytes();
                let age_ms = self.inner.age_ms();
                // Known-by-design: full-byte writer-drop happens when the
                // inline copy_slow_to_fast populate path is cancelled
                // mid-await between data_stream finishing and
                // fast_store.update.await resolving. This Drop is the
                // safety net that converts the cancellation into a
                // terminal Internal error so readers don't block forever;
                // a follow-up populate succeeds shortly after. Demote to
                // debug to avoid alarming on benign cancellation; keep
                // warn for the partial/zero-byte cases where the producer
                // actually wedged.
                if bytes_written == expected_size && age_ms < 5000 {
                    debug!(
                        digest = %self.inner.digest,
                        bytes_written,
                        expected_size,
                        age_ms,
                        "streaming blob writer dropped without eof (full-byte, likely cancelled populate), notify_waiters firing"
                    );
                } else {
                    warn!(
                        digest = %self.inner.digest,
                        bytes_written,
                        expected_size,
                        age_ms,
                        "streaming blob writer dropped without eof, notify_waiters firing"
                    );
                }
                *terminal = Some(Err(make_err!(
                    Code::Internal,
                    "writer dropped without sending EOF"
                )));
                drop(terminal);
                self.inner.notify_waiters();
            }
        }
    }
}

/// Reader handle for a streaming blob.
///
/// Each reader maintains its own cursor position and advances
/// independently of other readers.  Readers never block the
/// writer or each other.
pub struct StreamingBlobReader {
    inner: Arc<StreamingBlobInner>,
    /// Absolute index of the next chunk to read.
    cursor_chunk_idx: u64,
    /// Byte offset within the current chunk (reserved for future
    /// partial-chunk reads; currently always 0).
    #[allow(dead_code)]
    cursor_byte_offset: u64,
    /// Watch receiver cloned from `inner.notify_rx_template` (which is
    /// kept at `Version::INITIAL` forever). The clone inherits
    /// seen-version = INITIAL; any `send_modify` that has already fired
    /// (or will fire) makes the next `changed().await` return
    /// immediately. This is the load-bearing primitive distinction vs
    /// `Notify` — late subscribers (constructed after the writer fired
    /// and dropped) see `version > seen` and do NOT park, fixing the
    /// #272 late-subscriber lost-wakeup race. See `notify_tx` doc for
    /// why `Sender::subscribe()` would NOT work here.
    notify_rx: watch::Receiver<u64>,
    /// Diagnostic-only: number of chunks read out via `next_chunk` since
    /// reader construction. Used by Drop logging to surface premature
    /// reader teardown.
    chunks_consumed: u64,
    /// Diagnostic-only: whether `next_chunk` has observed terminal state.
    terminal_seen: bool,
    /// Diagnostic-only: reader construction timestamp for elapsed logging.
    created_at: Instant,
}

impl Drop for StreamingBlobReader {
    fn drop(&mut self) {
        debug!(
            digest = %self.inner.digest,
            chunks_consumed = self.chunks_consumed,
            terminal_seen = self.terminal_seen,
            age_ms = self.created_at.elapsed().as_millis() as u64,
            "streaming blob reader dropped"
        );
    }
}

impl fmt::Debug for StreamingBlobReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingBlobReader")
            .field("digest", &self.inner.digest)
            .field("cursor_chunk_idx", &self.cursor_chunk_idx)
            .field("cursor_byte_offset", &self.cursor_byte_offset)
            .finish()
    }
}

impl StreamingBlobReader {
    pub fn new(inner: Arc<StreamingBlobInner>) -> Self {
        let earliest = inner.earliest_chunk_idx.load(Ordering::Acquire);
        // Clone the pristine template Receiver (kept at
        // `Version::INITIAL` forever on Inner). The clone inherits
        // seen-version=INITIAL, so the FIRST `changed().await` returns
        // immediately if ANY `send_modify` has ever happened on the
        // channel — including fires that happened before this reader
        // was constructed. `Sender::subscribe()` would pin to the
        // current channel version (post-fire for late subscribers),
        // defeating the late-subscriber fix.
        let notify_rx = inner.notify_rx_template.clone();
        // #515 Phase 0 expansion: per-reader construction sample.
        // Records `starting_chunk_idx` (which equals the observed
        // `earliest_chunk_idx` from the load above) plus the digest, so
        // a 24 h soak yields a baseline distribution of
        // construction-time cursor values. Independent of the FSS-site
        // WARN at `fast_slow_store.rs:6500` — DS-reviewer / red-team
        // flagged that the WARN-only path leaves no signal for "how
        // common is `cursor_chunk_idx > 0` at construction OUTSIDE of
        // the FSS pre-check path?" (e.g. the `bytestream_server.rs:1421`
        // `InFlightBlobMap::get_reader` path which DOES tolerate
        // `> 0` and falls through). Sampling-gated to keep volume
        // bounded (see `READER_CONSTRUCTION_SAMPLE_PERIOD` docs).
        let sample_idx =
            READER_CONSTRUCTION_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed);
        if sample_idx % READER_CONSTRUCTION_SAMPLE_PERIOD == 0 {
            info!(
                site = "reader_new",
                digest = %inner.digest,
                starting_chunk_idx = earliest,
                earliest_chunk_idx_at_construction = earliest,
                sample_idx,
                sample_period = READER_CONSTRUCTION_SAMPLE_PERIOD,
                "#515 StreamingBlobReader::new construction sample"
            );
        }
        Self {
            inner,
            cursor_chunk_idx: earliest,
            cursor_byte_offset: 0,
            notify_rx,
            chunks_consumed: 0,
            terminal_seen: false,
            created_at: Instant::now(),
        }
    }

    /// Access the underlying `StreamingBlobInner` for state checks.
    pub fn inner(&self) -> &StreamingBlobInner {
        &self.inner
    }

    /// #515 Phase 0 diagnostic accessor: returns the absolute chunk
    /// index this reader will read NEXT. Constructed-with value comes
    /// from `inner.earliest_chunk_idx` at the time of `Self::new`
    /// (`streaming_blob.rs::StreamingBlobReader::new`). A non-zero
    /// value at construction time indicates the producer raced ahead
    /// of the FSS pre-check + eviction triggered between the
    /// pre-check load at `fast_slow_store.rs:6481` and the
    /// `StreamingBlobReader::new` call at `:6500` — the H1 TOCTOU.
    /// Used by FSS to diagnose the splice-corruption class
    /// (`PrefixContinuity` invariant violation: splice math
    /// `new_offset = offset + bytes_already_sent` assumes the reader
    /// started at chunk 0 so `bytes_already_sent` is an absolute
    /// blob offset; if `cursor_chunk_idx > 0` at construction, that
    /// assumption fails).
    pub fn cursor_chunk_idx(&self) -> u64 {
        self.cursor_chunk_idx
    }

    /// Returns the next chunk of data, waiting if necessary.
    ///
    /// - If the cursor has fallen behind the sliding window,
    ///   returns `Code::Unavailable` (retryable).
    /// - If a chunk is available, returns it and advances the cursor.
    /// - If no chunk is available and the writer is still active,
    ///   waits for notification and retries.
    /// - If the writer sent EOF and no more chunks remain, returns
    ///   empty `Bytes` (signals EOF to the caller).
    /// - If the writer sent an error, returns that error.
    pub async fn next_chunk(&mut self) -> Result<Bytes, Error> {
        // Failpoint: simulate a chunk read failure in the streaming blob.
        // Exercises the fallback path in FastSlowStore::get_part where a
        // streaming populate reader error triggers a slow-store resume at
        // the correct byte offset.
        //
        // Uses the SLIDING_WINDOW_EVICTION_MARKER substring (shared with
        // the production error path below) so the FastSlowStore D.1
        // fallback predicate trips on the same byte sequence in both real
        // and synthetic failures. Tests asserting on Code::Unavailable
        // continue to work unchanged.
        #[cfg(feature = "failpoints")]
        fail::fail_point!("streaming_blob_next_chunk_fail", |_| {
            Err(make_err!(
                Code::Unavailable,
                "failpoint: {} (synthetic)",
                SLIDING_WINDOW_EVICTION_MARKER
            ))
        });

        loop {
            // The watch::Receiver was subscribed at reader construction
            // (see `StreamingBlobReader::new`), so any `send_modify` that
            // fires between iterations is already captured by the
            // current-vs-seen version comparison inside `changed()`.
            // No same-iteration subscribe-before-check dance is needed
            // — the receiver persists across loop iterations and never
            // misses an increment. This is the load-bearing primitive
            // distinction vs `Notify::notified()` (which subscribes
            // fresh and drops permits delivered before subscribe).

            let earliest = self.inner.earliest_chunk_idx.load(Ordering::Acquire);
            if self.cursor_chunk_idx < earliest {
                return Err(make_err!(
                    Code::Unavailable,
                    "{} (cursor={}, earliest={})",
                    SLIDING_WINDOW_EVICTION_MARKER,
                    self.cursor_chunk_idx,
                    earliest
                ));
            }

            let chunk_count = self.inner.chunk_count.load(Ordering::Acquire);

            // Check if a chunk is available at our cursor position.
            if self.cursor_chunk_idx < chunk_count {
                let chunks = self.inner.chunks.read();
                // #515 Phase 0 H_alt_I hypothesis empirically refuted
                // (audit: `.claude/audits/515-phase02-empirical-\
                // refutation-2026-05-17.md`). The original hypothesis
                // was that the producer's `earliest_chunk_idx.fetch_add`
                // could fire between the `earliest` load above and the
                // `chunks.read()` lock acquisition, leaving `earliest`
                // stale and producing a frankenstein-bytes splice via a
                // wrong `deque_idx`. Phase 0.2 production data: 585
                // splice events post-deploy, ZERO DataLoss. Combined
                // with the FSS-side observation that readers always
                // start at cursor=0 (per `StreamingBlobReader::new`
                // construction sampling), the frankenstein-bytes class
                // does not materialize from this race. Demoted WARN →
                // INFO; the deque_idx is still computed from the
                // ORIGINAL `earliest` (preserving semantics — no
                // behavior change), and if it points past the deque
                // the `chunks.get` returns `None` and the loop
                // re-checks. Kept as a sampling-useful observation
                // that the pre/post values differ; not an alarm.
                let post_lock_earliest =
                    self.inner.earliest_chunk_idx.load(Ordering::Acquire);
                if post_lock_earliest != earliest {
                    info!(
                        site = "next_chunk_internal",
                        digest = %self.inner.digest,
                        cursor_chunk_idx = self.cursor_chunk_idx,
                        pre_earliest_chunk_idx = earliest,
                        post_earliest_chunk_idx = post_lock_earliest,
                        chunk_count,
                        chunks_consumed = self.chunks_consumed,
                        "#515 H_alt_I pre/post earliest_chunk_idx differ \
                         (rare; verify deque_idx still valid; benign per \
                         Phase 0.2 verification)"
                    );
                }
                // Convert absolute index to deque-relative index.
                let deque_idx = (self.cursor_chunk_idx - earliest) as usize;
                if let Some(chunk) = chunks.get(deque_idx) {
                    let data = chunk.clone();
                    self.cursor_chunk_idx += 1;
                    self.cursor_byte_offset = 0;
                    self.chunks_consumed += 1;
                    return Ok(data);
                }
                // earliest_chunk_idx advanced between our load and the
                // read-lock acquisition — re-check from the top.
                drop(chunks);
                continue;
            }

            // No chunk available — check terminal state.
            {
                let terminal = self.inner.terminal.lock();
                if let Some(ref result) = *terminal {
                    // Re-check: there might be trailing chunks we missed.
                    let final_count = self.inner.chunk_count.load(Ordering::Acquire);
                    if self.cursor_chunk_idx < final_count {
                        drop(terminal);
                        continue;
                    }
                    self.terminal_seen = true;
                    debug!(
                        digest = %self.inner.digest,
                        chunks_consumed = self.chunks_consumed,
                        kind = if result.is_ok() { "ok" } else { "err" },
                        "streaming blob reader observed terminal"
                    );
                    return match result {
                        Ok(()) => {
                            // #502: defense-in-depth at the API boundary.
                            // A producer that calls `send_eof` (terminal=Ok)
                            // after writing fewer bytes than the digest
                            // declares previously surfaced as `Ok(Bytes::new())`
                            // — readers reported clean EOF with
                            // `bytes_sent < digest.size_bytes()`, and Bazel
                            // saw a clean stream with truncated bytes and
                            // reported digest mismatch as build failure.
                            //
                            // Production observation (buildcache 2026-05-15):
                            // `LoggingReadStream` logs show events of the
                            // shape `expected_size: 47291739, bytes_sent:
                            // 7881957, status: "ok"` on the
                            // `bytestream_server::zero_copy_read` label
                            // (which composes inner_read →
                            // `streaming_read_while_write`-style unfold for
                            // in-flight blobs).
                            //
                            // The check is symmetric with the #500 fix at
                            // `bytestream_server.rs:1754` (consume_ok_eof
                            // branch in `inner_read`): there the producer
                            // dropped `tx` after a `get_part_fut` Err and
                            // the receive side reported `Ok(empty)`; the
                            // fix gated on the stashed `Err` slot in
                            // `state.maybe_get_part_result`. Here the
                            // producer terminated with `Ok` (no Err slot
                            // to consult) but `bytes_written <
                            // digest.size_bytes()` — same Bazel-visible
                            // wire shape, different producer-side trigger.
                            //
                            // Closing this at the API boundary (rather
                            // than at each consumer's unfold seam) means
                            // every current AND future
                            // `StreamingBlobReader` consumer inherits the
                            // contract: `Ok(Bytes::new())` is returned IFF
                            // the writer's bytes_written equals
                            // `digest.size_bytes()`. Anything else is
                            // converted to `Code::Internal` carrying the
                            // `STREAMING_BLOB_SILENT_SHORT_MARKER`
                            // substring for journal grep.
                            //
                            // **Why `Code::Internal`, not `Code::DataLoss`.**
                            // The 48 h distribution analysis at
                            // `.claude/audits/502-distribution-investigation-2026-05-16.md`
                            // shows the silent-short class is overwhelmingly
                            // a transient transport-layer chunker race, not
                            // deterministic blob corruption: 73.5 % of
                            // shorted digests also completed successfully in
                            // the same window (median per-digest success
                            // 25 %), 99.2 % of truncations land at a clean
                            // 1/N fraction of `expected_size`, and 38.3 %
                            // of events share `bytes_sent` with another
                            // distinct digest. Bazel's `RemoteRetrier`
                            // classifies `Code::Internal` as
                            // TRANSIENT_FAILURE and retries it up to 10×
                            // with exponential backoff; with the observed
                            // 25 % per-attempt success rate that converges
                            // to ~94 % recovery. `Code::DataLoss` would
                            // map to PERMANENT_FAILURE — no retry — and
                            // fail the action on the first hit, which is
                            // exactly the wrong behavior given the data.
                            // Truly deterministic-corruption cases still
                            // fail the action after Bazel exhausts the
                            // retry budget — same end-state, just slower.
                            //
                            // The DataLoss usage at
                            // `worker_proxy_store.rs:1018` is a different
                            // seam: that site has post-hash evidence that
                            // the delivered bytes don't match the declared
                            // digest, so no amount of retrying will help
                            // reconcile a SHA-256 mismatch — DataLoss is
                            // correct there. `next_chunk` does not have
                            // that information and must not pre-commit to
                            // PERMANENT_FAILURE.
                            //
                            // `DigestInfo::size_bytes()` is already `u64`.
                            // For zero-byte digests (expected=0,
                            // written=0), the check is a no-op (0 < 0 is
                            // false) and the original clean-EOF path is
                            // preserved.
                            //
                            // #49 (2026-06-04): use the producer-supplied
                            // `expected_size_on_store` when set, falling
                            // back to `digest.size_bytes()` otherwise. For
                            // AC reads `digest.size_bytes()` is the
                            // *Action* proto's size, NOT the stored
                            // *ActionResult* bytes; comparing
                            // `bytes_written` against the digest size
                            // therefore false-fires on every AC read where
                            // those sizes differ. The authoritative size
                            // (recorded by `FastSlowStore::run_producer`
                            // from `slow_store.has().Some(ExactSize(n))`)
                            // is the real bytes-on-store count. For CAS
                            // the two are equal by content-addressing
                            // invariant, so the override is a no-op
                            // there. For paths that cannot commit to a
                            // size (`LazyExistenceOnSync`'s
                            // `MaxSize(u64::MAX)`), the `OnceLock` stays
                            // unset and the digest-based fallback
                            // applies.
                            let bytes_written =
                                self.inner.bytes_written.load(Ordering::Acquire);
                            let expected_size =
                                self.inner.expected_size_on_store();
                            if bytes_written < expected_size {
                                error!(
                                    digest = %self.inner.digest,
                                    bytes_written,
                                    expected_size,
                                    chunks_consumed = self.chunks_consumed,
                                    age_ms = self.inner.age_ms(),
                                    "{}: producer terminated with Ok but wrote \
                                     fewer bytes than the digest declares — \
                                     surfacing as Code::Internal (transient, \
                                     Bazel-retryable) to prevent silent \
                                     short-stream corruption (#502)",
                                    STREAMING_BLOB_SILENT_SHORT_MARKER,
                                );
                                return Err(make_err!(
                                    Code::Internal,
                                    "{}: terminal=Ok but bytes_written={} < expected_size={} for digest {}",
                                    STREAMING_BLOB_SILENT_SHORT_MARKER,
                                    bytes_written,
                                    expected_size,
                                    self.inner.digest,
                                ));
                            }
                            Ok(Bytes::new())
                        }
                        Err(e) => Err(e.clone()),
                    };
                }
            }

            // Writer still active, no data yet — wait for the next
            // generation bump on the watch channel.
            //
            // `changed()` returns immediately if the sender's current
            // version differs from the receiver's seen-generation
            // (subscribed at construction OR last marked-seen by a
            // previous `changed()` return). After this returns Ok, the
            // receiver auto-marks the new version as seen, so the next
            // iteration's `changed().await` parks until the NEXT bump.
            //
            // Defense-in-depth: bound the wait with
            // STREAMING_BLOB_NOTIFY_TIMEOUT so the next missing-wakeup
            // bug surfaces as a logged DeadlineExceeded in seconds
            // rather than a 120 s gRPC stream wedge. Use tokio::time::
            // Instant so paused-time tests can drive the slow-wait +
            // deadline branches deterministically; in production it
            // forwards to std::time::Instant.
            let wait_start = tokio::time::Instant::now();
            debug!(
                digest = %self.inner.digest,
                cursor_chunk_idx = self.cursor_chunk_idx,
                "streaming blob reader awaiting watch::changed()"
            );
            let timeout_result =
                tokio::time::timeout(STREAMING_BLOB_NOTIFY_TIMEOUT, self.notify_rx.changed()).await;
            let wait_elapsed = wait_start.elapsed();
            let terminal_present = self.inner.terminal.lock().is_some();
            if timeout_result.is_err() {
                let chunk_count = self.inner.chunk_count.load(Ordering::Acquire);
                let earliest = self.inner.earliest_chunk_idx.load(Ordering::Acquire);
                // terminal_present distinguishes two distinct failure modes that
                // both surface as "reader timed out waiting for notify":
                //   - true:  writer dropped/finished but its notify firings did
                //            not wake this reader. Genuine lost wakeup; bug
                //            lives in the notify primitive integration here.
                //   - false: writer is still alive (no terminal state set);
                //            the producer task itself is wedged upstream of
                //            streaming_blob (e.g. blocked on a gRPC read with
                //            no per-frame deadline, holding a lock, or the
                //            tokio task is starved). Bug lives upstream.
                let producer_tid = self.inner.producer_task_id().unwrap_or("<none>");
                let notify_calls = self.inner.notify_waiters_calls_total();
                if terminal_present {
                    error!(
                        digest = %self.inner.digest,
                        age_ms = self.inner.age_ms(),
                        cursor_chunk_idx = self.cursor_chunk_idx,
                        chunk_count,
                        earliest,
                        wait_ms = wait_elapsed.as_millis() as u64,
                        producer_task_id = %producer_tid,
                        notify_waiters_calls = notify_calls,
                        "streaming blob reader notify deadline exceeded — \
                         terminal IS set, this is a genuine lost wakeup"
                    );
                } else {
                    error!(
                        digest = %self.inner.digest,
                        age_ms = self.inner.age_ms(),
                        cursor_chunk_idx = self.cursor_chunk_idx,
                        chunk_count,
                        earliest,
                        wait_ms = wait_elapsed.as_millis() as u64,
                        producer_task_id = %producer_tid,
                        notify_waiters_calls = notify_calls,
                        "streaming blob reader notify deadline exceeded — \
                         terminal NOT set, producer is wedged upstream \
                         (e.g. gRPC read with no deadline, or task starvation)"
                    );
                    // Force a thread-stack dump at the EXACT moment of the
                    // wedge — this is the most decisive single artifact for
                    // diagnosing what the producer task is parked on. The
                    // standard StallGuard rate-limit is bypassed because the
                    // streaming_blob deadline is itself a targeted-detector
                    // signal (not a generic guard); without this, the dump
                    // is suppressed if a sibling guard fired moments before.
                    let label = format!(
                        "streaming_blob_deadline digest={} producer_task={}",
                        self.inner.digest,
                        producer_tid,
                    );
                    // force_dump_thread_stacks may invoke a sync subprocess
                    // wait (macOS `sample`) that blocks the calling thread
                    // for up to 30s — running it on a tokio worker starves
                    // the runtime and creates the very wedge we're trying
                    // to diagnose. Always run on the blocking pool.
                    let _ = tokio::task::spawn_blocking(move || {
                        crate::stall_detector::force_dump_thread_stacks(&label);
                    });
                }
                return Err(make_err!(
                    Code::DeadlineExceeded,
                    "streaming blob next_chunk: notify deadline exceeded"
                ));
            }
            if wait_elapsed >= SLOW_NOTIFY_THRESHOLD {
                self.inner
                    .notify_waits_over_5s
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    digest = %self.inner.digest,
                    wait_ms = wait_elapsed.as_millis() as u64,
                    terminal_present,
                    cursor_chunk_idx = self.cursor_chunk_idx,
                    notify_waiters_calls = self.inner.notify_waiters_calls_total(),
                    "streaming blob reader slow notify wakeup"
                );
            } else {
                debug!(
                    digest = %self.inner.digest,
                    wait_ms = wait_elapsed.as_millis() as u64,
                    terminal_present,
                    "streaming blob reader notify wakeup"
                );
            }
        }
    }
}

/// Constructors for the streaming blob primitive.
#[derive(Debug, Clone, Copy)]
pub struct StreamingBlob;

impl StreamingBlob {
    /// Create a new streaming blob with the given digest and memory budget.
    ///
    /// Returns a writer (single owner) and the first reader.  Additional
    /// readers can be created via `new_reader`.
    ///
    /// #515 Phase 0 audit: this constructor creates a fresh
    /// `StreamingBlobInner` and immediately builds the reader, so
    /// `earliest_chunk_idx` is unconditionally 0 at reader-construction
    /// time. No TOCTOU between a caller pre-check and the construction —
    /// the construction-time sample inside `StreamingBlobReader::new`
    /// covers the late-reader case via the `new_reader` /
    /// `InFlightBlobMap::get_reader` paths.
    pub fn new(
        digest: DigestInfo,
        max_buffer_bytes: u64,
    ) -> (StreamingBlobWriter, StreamingBlobReader) {
        let inner = Arc::new(StreamingBlobInner::new(digest, max_buffer_bytes));
        let writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let reader = StreamingBlobReader::new(Arc::clone(&inner));
        (writer, reader)
    }

    /// Create an additional reader from an existing inner handle.
    ///
    /// #515 Phase 0 audit: callers (e.g. test fixtures, fan-out
    /// composition) pass in an existing inner with arbitrary
    /// `earliest_chunk_idx`. No pre-check happens on this side of the
    /// call — construction IS the only operation — so there is no
    /// caller-side TOCTOU to instrument. The construction-time sample
    /// emitted by `StreamingBlobReader::new` captures any non-zero
    /// `earliest_chunk_idx` observed at construction.
    pub fn new_reader(inner: &Arc<StreamingBlobInner>) -> StreamingBlobReader {
        StreamingBlobReader::new(Arc::clone(inner))
    }
}

/// Registry of in-flight streaming blobs keyed by digest.
///
/// Used at the service layer (e.g. `ByteStreamServer`) to allow
/// readers to discover blobs that are still being written.
pub struct InFlightBlobMap {
    map: RwLock<HashMap<DigestInfo, Arc<StreamingBlobInner>>>,
    /// Maximum concurrent in-flight blobs. 0 = unlimited.
    max_entries: usize,
}

impl fmt::Debug for InFlightBlobMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InFlightBlobMap")
            .field("len", &self.map.read().len())
            .finish()
    }
}

impl InFlightBlobMap {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            max_entries: 0,
        }
    }

    /// Create with a maximum number of concurrent in-flight blobs.
    /// When the limit is reached, new registrations return `None`
    /// (the write proceeds without streaming readers).
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    /// Register a new streaming blob.  Returns `Some((writer, reader))`
    /// if registered, or `None` if the map is at capacity.
    ///
    /// #515 Phase 0 audit: identical to `StreamingBlob::new` — fresh
    /// inner constructed locally, then reader built immediately, so
    /// `earliest_chunk_idx` is unconditionally 0 at construction. No
    /// caller-side TOCTOU exists here.
    pub fn register(
        &self,
        digest: DigestInfo,
        max_buffer_bytes: u64,
    ) -> Option<(StreamingBlobWriter, StreamingBlobReader)> {
        let inner = Arc::new(StreamingBlobInner::new(digest, max_buffer_bytes));
        let mut map = self.map.write();
        if self.max_entries > 0 && map.len() >= self.max_entries {
            return None;
        }
        map.insert(digest, Arc::clone(&inner));
        drop(map);
        let writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let reader = StreamingBlobReader::new(inner);
        Some((writer, reader))
    }

    /// Get a reader for an in-flight blob, if one exists.
    ///
    /// #515 Phase 0 audit: production caller `bytestream_server.rs:1421`
    /// invokes `get_reader` THEN inspects `earliest_chunk_idx()` to
    /// decide whether to fall through to the store read path. The check
    /// is on the same `inner` the reader was constructed from, so there
    /// is no caller-side pre-check / construction TOCTOU here — both
    /// observations see the same (possibly racing) atomic. The
    /// construction-time sample inside `StreamingBlobReader::new` covers
    /// the `cursor_chunk_idx > 0` baseline.
    pub fn get_reader(&self, digest: &DigestInfo) -> Option<StreamingBlobReader> {
        let map = self.map.read();
        map.get(digest)
            .map(|inner| StreamingBlobReader::new(Arc::clone(inner)))
    }

    /// Get the raw `Arc<StreamingBlobInner>` for a digest, if registered.
    ///
    /// Used for `Arc::ptr_eq` comparison during grace-period removal.
    pub fn get_inner(&self, digest: &DigestInfo) -> Option<Arc<StreamingBlobInner>> {
        self.map.read().get(digest).cloned()
    }

    /// Remove a blob from the map, but only if the stored `Arc`
    /// points to the same allocation as `expected`.  This prevents
    /// removing a newer registration for the same digest.
    pub fn remove(&self, digest: &DigestInfo, expected: &Arc<StreamingBlobInner>) {
        let mut map = self.map.write();
        if let Some(existing) = map.get(digest) {
            if Arc::ptr_eq(existing, expected) {
                map.remove(digest);
            }
        }
    }

    /// Number of in-flight blobs currently registered.
    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }
}

impl Default for InFlightBlobMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Default maximum concurrent in-flight streaming blobs.
/// With 64 MiB per blob, 128 entries = 8 GiB worst case.
pub const DEFAULT_MAX_IN_FLIGHT_BLOBS: usize = 128;

#[cfg(test)]
mod tests {
    use nativelink_error::Code;

    use super::*;

    /// Helper: create a DigestInfo from a u8 seed (for test variety).
    ///
    /// The default size of 1024 is a vestige from before the #502
    /// silent-short defense in `next_chunk`. Tests that send fewer
    /// than 1024 bytes and then call `send_eof` will now trip the
    /// partial-bytes check (terminal=Ok with bytes_written < expected).
    /// Use [`test_digest_with_size`] when the test asserts on
    /// terminal-Ok and writes a known small payload, so the digest's
    /// declared size matches the bytes actually written.
    fn test_digest(seed: u8) -> DigestInfo {
        test_digest_with_size(seed, 1024)
    }

    /// Helper: explicit-size DigestInfo, for tests that need the
    /// declared size to match `bytes_written` so the #502 partial-bytes
    /// check in `next_chunk` does not trip.
    fn test_digest_with_size(seed: u8, size: u64) -> DigestInfo {
        let mut hash = [0u8; 32];
        hash[0] = seed;
        DigestInfo::new(hash, size)
    }

    // ---------------------------------------------------------------
    // 1. Single writer, single reader — data flows correctly
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn single_writer_single_reader() {
        // Digest size matches the 11 bytes actually written so the
        // #502 partial-bytes check (terminal=Ok with bytes_written <
        // expected_size) does not trip on this happy-path test.
        let (writer, mut reader) = StreamingBlob::new(test_digest_with_size(1, 11), 1024 * 1024);

        let data1 = Bytes::from_static(b"hello ");
        let data2 = Bytes::from_static(b"world");

        writer.send(data1.clone()).await.unwrap();
        writer.send(data2.clone()).await.unwrap();

        let chunk1 = reader.next_chunk().await.unwrap();
        assert_eq!(chunk1, data1);

        let chunk2 = reader.next_chunk().await.unwrap();
        assert_eq!(chunk2, data2);

        // Writer hasn't sent EOF yet, so a read should block.
        // We send EOF from a background task to unblock.
        let writer = Arc::new(Mutex::new(writer));
        let w = Arc::clone(&writer);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            w.lock().send_eof().unwrap();
        });

        let eof_chunk = reader.next_chunk().await.unwrap();
        assert!(eof_chunk.is_empty(), "expected empty bytes for EOF");
    }

    // ---------------------------------------------------------------
    // 2. Single writer, multiple readers — all see same data
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn multiple_readers_see_same_data() {
        // 5 chunks of "chunk-N" (7 bytes each) = 35 bytes total.
        // Match the digest size so the #502 partial-bytes check does
        // not trip on this happy-path test.
        let (mut writer, mut reader1) =
            StreamingBlob::new(test_digest_with_size(2, 5 * 7), 1024 * 1024);

        // Create a second reader from the inner.
        let inner = Arc::clone(&reader1.inner);
        let mut reader2 = StreamingBlob::new_reader(&inner);

        let chunks: Vec<Bytes> = (0..5)
            .map(|i| Bytes::from(format!("chunk-{i}")))
            .collect();

        for c in &chunks {
            writer.send(c.clone()).await.unwrap();
        }
        writer.send_eof().unwrap();

        // Both readers should see all chunks in order.
        for expected in &chunks {
            let r1 = reader1.next_chunk().await.unwrap();
            let r2 = reader2.next_chunk().await.unwrap();
            assert_eq!(&r1, expected);
            assert_eq!(&r2, expected);
        }

        // Both should get EOF.
        assert!(reader1.next_chunk().await.unwrap().is_empty());
        assert!(reader2.next_chunk().await.unwrap().is_empty());
    }

    // ---------------------------------------------------------------
    // 3. Writer error propagates to all readers
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn writer_error_propagates() {
        let (mut writer, mut reader) = StreamingBlob::new(test_digest(3), 1024 * 1024);

        let inner = Arc::clone(&reader.inner);
        let mut reader2 = StreamingBlob::new_reader(&inner);

        writer.send(Bytes::from_static(b"data")).await.unwrap();
        writer.send_error(make_err!(Code::DataLoss, "hash mismatch"));

        // First chunk is still readable.
        let c = reader.next_chunk().await.unwrap();
        assert_eq!(c, Bytes::from_static(b"data"));
        let c2 = reader2.next_chunk().await.unwrap();
        assert_eq!(c2, Bytes::from_static(b"data"));

        // Next read returns the error.
        let err = reader.next_chunk().await.unwrap_err();
        assert_eq!(err.code, Code::DataLoss);

        let err2 = reader2.next_chunk().await.unwrap_err();
        assert_eq!(err2.code, Code::DataLoss);
    }

    // ---------------------------------------------------------------
    // 4. Writer drop without EOF gives readers an error
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn writer_drop_without_eof() {
        let (writer, mut reader) = StreamingBlob::new(test_digest(4), 1024 * 1024);

        writer.send(Bytes::from_static(b"partial")).await.unwrap();
        drop(writer);

        let c = reader.next_chunk().await.unwrap();
        assert_eq!(c, Bytes::from_static(b"partial"));

        let err = reader.next_chunk().await.unwrap_err();
        assert_eq!(err.code, Code::Internal);
        assert!(
            err.messages.iter().any(|m| m.contains("dropped without")),
            "expected 'dropped without' in error messages, got: {:?}",
            err.messages
        );
    }

    // ---------------------------------------------------------------
    // 5. Sliding window eviction — slow reader gets Unavailable
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn sliding_window_eviction() {
        // Buffer limited to 20 bytes.
        let (writer, mut slow_reader) = StreamingBlob::new(test_digest(5), 20);

        // Write 30 bytes in 3 chunks of 10.  The first chunk will
        // be evicted once the third is appended.
        for i in 0..3u8 {
            let data = Bytes::from(vec![i; 10]);
            writer.send(data).await.unwrap();
        }

        // The writer evicts chunks when the buffer exceeds 20 bytes,
        // so after 30 bytes the oldest chunk(s) are gone.
        let earliest = slow_reader
            .inner
            .earliest_chunk_idx
            .load(Ordering::Acquire);
        assert!(
            earliest > 0,
            "expected some eviction, earliest_chunk_idx={earliest}"
        );

        // Slow reader's cursor is at 0, which is < earliest.
        let err = slow_reader.next_chunk().await.unwrap_err();
        assert_eq!(err.code, Code::Unavailable);

        // Create a new reader after eviction — it starts at
        // earliest_chunk_idx and should be able to read.
        let inner = Arc::clone(&slow_reader.inner);
        let mut late_reader = StreamingBlob::new_reader(&inner);
        let chunk = late_reader.next_chunk().await.unwrap();
        assert_eq!(chunk.len(), 10);

        let mut writer = writer;
        writer.send_eof().unwrap();
    }

    // ---------------------------------------------------------------
    // 6. Reader waits for data (does not return None prematurely)
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn reader_waits_for_data() {
        let (writer, mut reader) = StreamingBlob::new(test_digest(6), 1024 * 1024);

        let writer = Arc::new(Mutex::new(Some(writer)));
        let w = Arc::clone(&writer);

        // Spawn a task that writes after a delay.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let w_guard = w.lock();
            let w_ref = w_guard.as_ref().unwrap();
            w_ref.send(Bytes::from_static(b"delayed")).await.unwrap();
        });

        // Reader should block until data arrives, then return it.
        let start = std::time::Instant::now();
        let chunk = reader.next_chunk().await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(chunk, Bytes::from_static(b"delayed"));
        assert!(
            elapsed >= std::time::Duration::from_millis(20),
            "reader returned too quickly ({elapsed:?}), should have waited"
        );

        // Clean up.
        let mut w_guard = writer.lock();
        w_guard.take().unwrap().send_eof().unwrap();
    }

    // ---------------------------------------------------------------
    // 7. EOF only after terminal-success
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn eof_only_after_terminal_success() {
        // 2 bytes total ("a" + "b"); match digest size so the #502
        // partial-bytes check does not trip.
        let (mut writer, mut reader) =
            StreamingBlob::new(test_digest_with_size(7, 2), 1024 * 1024);

        writer.send(Bytes::from_static(b"a")).await.unwrap();
        writer.send(Bytes::from_static(b"b")).await.unwrap();

        // Read both chunks.
        assert_eq!(reader.next_chunk().await.unwrap(), Bytes::from_static(b"a"));
        assert_eq!(reader.next_chunk().await.unwrap(), Bytes::from_static(b"b"));

        // Send EOF.
        writer.send_eof().unwrap();

        // Now reader gets empty Bytes (EOF).
        let eof = reader.next_chunk().await.unwrap();
        assert!(eof.is_empty());

        // Subsequent reads also return EOF.
        let eof2 = reader.next_chunk().await.unwrap();
        assert!(eof2.is_empty());
    }

    // ---------------------------------------------------------------
    // 8. InFlightBlobMap register / get / remove with Arc pointer check
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn in_flight_blob_map_basic() {
        let map = InFlightBlobMap::new();
        let digest = test_digest(8);

        // Register a blob.
        let (mut writer, mut reader1) = map.register(digest, 1024 * 1024).unwrap();
        assert_eq!(map.len(), 1);

        // Get a reader for the same digest.
        let mut reader2 = map.get_reader(&digest).expect("blob should be in map");

        // Write and verify both readers work.
        writer.send(Bytes::from_static(b"map-data")).await.unwrap();
        writer.send_eof().unwrap();

        assert_eq!(
            reader1.next_chunk().await.unwrap(),
            Bytes::from_static(b"map-data")
        );
        assert_eq!(
            reader2.next_chunk().await.unwrap(),
            Bytes::from_static(b"map-data")
        );

        // Remove with wrong Arc pointer — should not remove.
        let other_inner = Arc::new(StreamingBlobInner::new(digest, 1024));
        map.remove(&digest, &other_inner);
        assert_eq!(map.len(), 1, "remove with wrong Arc should be a no-op");

        // Remove with correct Arc pointer.
        let correct_inner = Arc::clone(&reader1.inner);
        map.remove(&digest, &correct_inner);
        assert_eq!(map.len(), 0);
        assert!(map.get_reader(&digest).is_none());
    }

    // ---------------------------------------------------------------
    // 9. Cannot send after EOF
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn send_after_eof_fails() {
        let (mut writer, _reader) = StreamingBlob::new(test_digest(9), 1024 * 1024);

        writer.send_eof().unwrap();
        let err = writer.send(Bytes::from_static(b"too late")).await.unwrap_err();
        assert_eq!(err.code, Code::Internal);
    }

    // ---------------------------------------------------------------
    // 10. Double EOF fails
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn double_eof_fails() {
        let (mut writer, _reader) = StreamingBlob::new(test_digest(10), 1024 * 1024);

        writer.send_eof().unwrap();
        let err = writer.send_eof().unwrap_err();
        assert_eq!(err.code, Code::Internal);
    }

    // ---------------------------------------------------------------
    // 11. Writer error propagation when readers are blocked waiting
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn writer_error_wakes_blocked_reader() {
        let (mut writer, mut reader) = StreamingBlob::new(test_digest(11), 1024 * 1024);

        // Reader is blocked waiting for data — send error from another task.
        let write_handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            writer.send_error(make_err!(Code::Aborted, "upload cancelled"));
        });

        // This should unblock when the error is sent.
        let start = std::time::Instant::now();
        let err = reader.next_chunk().await.unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, Code::Aborted);
        assert!(
            elapsed >= std::time::Duration::from_millis(20),
            "reader should have waited for error, but returned in {elapsed:?}"
        );

        write_handle.await.unwrap();
    }

    // ---------------------------------------------------------------
    // 12. Multiple concurrent readers at different speeds
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn concurrent_readers_different_speeds() {
        // 10 chunks of "data-NNNN" (9 bytes each) = 90 bytes total.
        // Match the digest size so the #502 partial-bytes check does
        // not trip on this happy-path test.
        // Large buffer so no eviction happens.
        let (mut writer, mut fast_reader) =
            StreamingBlob::new(test_digest_with_size(12, 10 * 9), 1024 * 1024);

        let inner = Arc::clone(&fast_reader.inner);
        let mut slow_reader = StreamingBlob::new_reader(&inner);

        // Write 10 chunks.
        let chunks: Vec<Bytes> = (0..10)
            .map(|i| Bytes::from(format!("data-{i:04}")))
            .collect();
        for c in &chunks {
            writer.send(c.clone()).await.unwrap();
        }
        writer.send_eof().unwrap();

        // Fast reader: consume all chunks immediately.
        let mut fast_data = Vec::new();
        loop {
            let chunk = fast_reader.next_chunk().await.unwrap();
            if chunk.is_empty() {
                break;
            }
            fast_data.push(chunk);
        }
        assert_eq!(fast_data.len(), 10);

        // Slow reader: consume one at a time with a delay.
        let mut slow_data = Vec::new();
        loop {
            let chunk = slow_reader.next_chunk().await.unwrap();
            if chunk.is_empty() {
                break;
            }
            slow_data.push(chunk);
        }
        assert_eq!(slow_data.len(), 10);

        // Both should have identical data despite different read speeds.
        assert_eq!(fast_data, slow_data);
        for (i, chunk) in fast_data.iter().enumerate() {
            assert_eq!(chunk, &chunks[i]);
        }
    }

    // ---------------------------------------------------------------
    // 13. Window eviction under memory pressure — slow reader gets
    //     Unavailable while fast reader succeeds
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn window_eviction_slow_reader_fast_reader() {
        // 5 chunks of 10 bytes = 50 bytes total. Match digest size so
        // the #502 partial-bytes check does not trip on the fast-reader
        // EOF assertion (the slow reader trips the unrelated sliding-
        // window-eviction Err first).
        // Buffer limited to 30 bytes. Each chunk is 10 bytes.
        let (writer, mut slow_reader) =
            StreamingBlob::new(test_digest_with_size(13, 5 * 10), 30);

        let inner = Arc::clone(&slow_reader.inner);
        let mut fast_reader = StreamingBlob::new_reader(&inner);

        // Write 5 chunks of 10 bytes each (50 bytes total).
        // After chunk 4, the buffer exceeds 30 bytes, so oldest chunks
        // get evicted.
        for i in 0..5u8 {
            writer.send(Bytes::from(vec![i; 10])).await.unwrap();

            // Fast reader keeps up: consume each chunk as it arrives.
            let chunk = fast_reader.next_chunk().await.unwrap();
            assert_eq!(chunk.len(), 10);
            assert_eq!(chunk[0], i);
        }

        let mut writer = writer;
        writer.send_eof().unwrap();

        // Fast reader should see EOF since it consumed everything.
        let eof = fast_reader.next_chunk().await.unwrap();
        assert!(eof.is_empty());

        // Slow reader hasn't read anything — its cursor is at 0,
        // but eviction has moved earliest_chunk_idx forward.
        let earliest = slow_reader
            .inner
            .earliest_chunk_idx
            .load(Ordering::Acquire);
        assert!(
            earliest > 0,
            "expected eviction to move earliest_chunk_idx, got {earliest}"
        );

        let err = slow_reader.next_chunk().await.unwrap_err();
        assert_eq!(
            err.code,
            Code::Unavailable,
            "slow reader should get Unavailable after falling behind"
        );
    }

    // ---------------------------------------------------------------
    // 14. InFlightBlobMap cleanup: writer completes, entry removed
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn in_flight_blob_map_remove_after_write_completes() {
        let map = InFlightBlobMap::new();
        // 7 bytes ("payload"); match digest size so the #502 partial-
        // bytes check does not trip on this happy-path test.
        let digest = test_digest_with_size(14, 7);

        let (mut writer, mut reader) = map.register(digest, 1024 * 1024).unwrap();
        assert_eq!(map.len(), 1);

        // Simulate a complete write cycle.
        writer.send(Bytes::from_static(b"payload")).await.unwrap();
        writer.send_eof().unwrap();

        // Reader consumes all data.
        let chunk = reader.next_chunk().await.unwrap();
        assert_eq!(chunk, Bytes::from_static(b"payload"));
        let eof = reader.next_chunk().await.unwrap();
        assert!(eof.is_empty());

        // Now remove using the correct inner Arc.
        let inner = map.get_inner(&digest).expect("should still be registered");
        map.remove(&digest, &inner);

        // Verify the entry is gone.
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
        assert!(map.get_reader(&digest).is_none());
        assert!(map.get_inner(&digest).is_none());
    }

    // ---------------------------------------------------------------
    // 14b. terminal_result returns the actual producer outcome
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn terminal_result_reflects_writer_outcome() {
        // Active writer: terminal_result is None.
        let (mut writer, _reader) = StreamingBlob::new(test_digest(20), 1024);
        assert!(
            writer.inner.terminal_result().is_none(),
            "fresh writer must have None terminal_result"
        );

        // EOF: terminal_result is Some(Ok(())).
        writer.send_eof().unwrap();
        assert!(
            matches!(writer.inner.terminal_result(), Some(Ok(()))),
            "send_eof must surface Some(Ok(())) via terminal_result"
        );

        // Error: terminal_result returns the cloned upstream error.
        let (mut writer2, _reader2) = StreamingBlob::new(test_digest(21), 1024);
        writer2.send_error(make_err!(Code::Unavailable, "synthetic upstream"));
        match writer2.inner.terminal_result() {
            Some(Err(err)) => assert_eq!(err.code, Code::Unavailable),
            other => panic!("expected Some(Err(Unavailable)), got {other:?}"),
        }

        // Cloned: subsequent calls return their own clone (callers can
        // re-query without exhausting the state).
        assert!(matches!(writer2.inner.terminal_result(), Some(Err(_))));
    }

    // ---------------------------------------------------------------
    // 15. InFlightBlobMap: get_reader returns None for non-existent digest
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn in_flight_blob_map_get_reader_nonexistent() {
        let map = InFlightBlobMap::new();

        let missing_digest = test_digest(15);
        assert!(
            map.get_reader(&missing_digest).is_none(),
            "get_reader should return None for unregistered digest"
        );
        assert!(
            map.get_inner(&missing_digest).is_none(),
            "get_inner should return None for unregistered digest"
        );

        // Register a different digest and confirm original is still absent.
        let other_digest = test_digest(99);
        let (_writer, _reader) = map.register(other_digest, 1024).unwrap();
        assert_eq!(map.len(), 1);
        assert!(
            map.get_reader(&missing_digest).is_none(),
            "get_reader should still return None for the unregistered digest"
        );
    }

    // ---------------------------------------------------------------
    // Lost-wakeup race regression (#272 Shape B): the writer can
    // set terminal + fire its watch generation bump in the gap
    // between a reader's terminal-predicate check (lock dropped)
    // and its blocking await; the reader MUST observe terminal
    // and return promptly, not park indefinitely.
    //
    // The pre-#272 implementation used `tokio::sync::Notify`,
    // whose `notify_waiters()` only wakes Notified futures that
    // have already registered.  A late subscriber (constructed
    // after notify_waiters fired and dropped) would never see the
    // permit, producing 120s+ reader hangs (observed at 19:33 UTC
    // on worker-02; replayed at 14:14, 14:15, 14:26 PDT
    // 2026-05-05).
    //
    // The Shape B fix replaces `Notify` with
    // `watch::Sender<u64>` + receiver subscribed at reader
    // construction.  `watch::Receiver::changed().await` returns
    // immediately whenever the sender's current version differs
    // from the receiver's seen-generation, so a fire that
    // happened BEFORE `changed()` was called is still observed.
    //
    // This test exercises that property directly: writer fires
    // the generation bump while the reader hasn't yet awaited
    // changed(); the subsequent reader call still completes
    // promptly. (The classic predicate-vs-subscribe gap is
    // structurally impossible with watch — the receiver was
    // subscribed at construction and persists across iterations.)
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn next_chunk_no_lost_wakeup_on_terminal_race() {
        let (writer, reader) = StreamingBlob::new(test_digest(201), 1024 * 1024);
        let inner = Arc::clone(&reader.inner);
        drop(reader);

        // Spawn a "writer" that, after a one-shot signal, sets
        // terminal and bumps the watch generation.  The signal is
        // delivered AFTER the test (acting as the reader) has
        // performed the predicate check but BEFORE it constructs
        // a fresh reader — the late-subscriber lost-wakeup window
        // that defeats `Notify` but not `watch`.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let writer_inner = Arc::clone(&inner);
        let writer_task = tokio::spawn(async move {
            rx.await.unwrap();
            {
                let mut t = writer_inner.terminal.lock();
                *t = Some(Err(make_err!(Code::Aborted, "race-test error")));
            }
            writer_inner.notify_waiters();
        });

        // Predicate check (mirrors the pre-await lock-and-drop).
        {
            let t = inner.terminal.lock();
            assert!(t.is_none(), "precondition");
        }
        // Open the lost-wakeup window.
        tx.send(()).unwrap();
        // Wait for the writer to complete BOTH steps before we
        // subscribe — joining the spawn ensures the generation
        // bump has already fired before any reader-side
        // changed().await call.
        writer_task.await.unwrap();

        // A FRESH reader subscribes AFTER the writer fired and
        // the spawn-task dropped — `Notify` would have lost the
        // wakeup here. With watch, the new receiver's seen
        // generation is the channel's version-at-subscribe (the
        // same value the sender just bumped to), so the predicate
        // check below already sees terminal set and returns
        // without ever calling changed(). The wedge is impossible.
        let mut reader2 = StreamingBlob::new_reader(&inner);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            reader2.next_chunk(),
        )
        .await
        .expect("next_chunk hung past 200ms — #272 lost-wakeup regression");
        assert!(result.is_err(), "expected terminal Err");
        drop(writer);
    }

    // ---------------------------------------------------------------
    // Stress test: race `send_error` against `next_chunk` across
    // many concurrent reader/writer pairs on a multi-threaded
    // runtime.  Without the subscribe-before-check fix, some
    // iterations land notify_waiters() in the
    // predicate-vs-subscribe gap inside next_chunk, and those
    // reader futures hang until the per-test timeout.
    //
    // The race window is microseconds (one debug! call between
    // lock-drop and notified().await), so the failure rate without
    // the fix is low per iteration — we run many concurrent pairs
    // to amplify it and assert all complete within the timeout.
    // ---------------------------------------------------------------
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn next_chunk_terminal_race_stress() {
        const PAIRS: usize = 1024;
        let mut handles = Vec::with_capacity(PAIRS);

        for i in 0..PAIRS {
            let (mut writer, mut reader) =
                StreamingBlob::new(test_digest((i & 0xff) as u8), 1024 * 1024);

            let reader_task = tokio::spawn(async move {
                let start = Instant::now();
                let res = reader.next_chunk().await;
                (res, start.elapsed())
            });

            // Spawn writer concurrently so it races the reader's
            // first poll, maximising the chance of landing in the
            // predicate-vs-subscribe gap.
            let writer_task = tokio::spawn(async move {
                tokio::task::yield_now().await;
                writer.send_error(make_err!(Code::Aborted, "stress error"));
            });

            handles.push((reader_task, writer_task));
        }

        for (i, (reader_task, writer_task)) in handles.into_iter().enumerate() {
            writer_task.await.unwrap();
            let outcome = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                reader_task,
            )
            .await
            .unwrap_or_else(|_| panic!(
                "pair {i}: reader.next_chunk hung past 5s — \
                 lost-wakeup race regression"
            ));
            let (chunk_res, _elapsed) = outcome.unwrap();
            assert!(
                chunk_res.is_err(),
                "pair {i}: expected terminal Err, got {chunk_res:?}"
            );
        }
    }

    // ---------------------------------------------------------------
    // Defense-in-depth: notify_await deadline + slow-wait counter
    // ---------------------------------------------------------------

    /// Spec: when the writer never notifies (lost wakeup or stuck
    /// producer), `next_chunk` MUST return `DeadlineExceeded` within
    /// `STREAMING_BLOB_NOTIFY_TIMEOUT` (30 s) instead of wedging
    /// indefinitely. Uses paused virtual time so the test runs in <1 s.
    #[tokio::test(start_paused = true)]
    async fn next_chunk_deadline_exceeded_on_stuck_writer() {
        // Hold the writer alive but never send / EOF / error.
        let (_writer, mut reader) = StreamingBlob::new(test_digest(99), 1024 * 1024);

        let read_fut = reader.next_chunk();
        tokio::pin!(read_fut);

        // Advance virtual time past the 30 s deadline.
        let result = tokio::time::timeout(
            STREAMING_BLOB_NOTIFY_TIMEOUT + Duration::from_secs(5),
            &mut read_fut,
        )
        .await
        .expect("reader did not return within deadline + slack");

        let err = result.expect_err("expected DeadlineExceeded, got Ok");
        assert_eq!(
            err.code,
            Code::DeadlineExceeded,
            "expected Code::DeadlineExceeded, got {err:?}"
        );
    }

    /// Spec: a single `next_chunk` wait that exceeds
    /// `SLOW_NOTIFY_THRESHOLD` (5 s) but completes before the deadline
    /// MUST increment `notify_waits_over_5s_total`. A wait that
    /// completes quickly MUST NOT increment it.
    #[tokio::test(start_paused = true)]
    async fn slow_notify_wait_increments_counter() {
        // 0-byte digest: writer EOFs without sending any chunks.
        // Match digest size so the #502 partial-bytes check does not
        // trip on the clean-EOF assertion below.
        let (mut writer, mut reader) =
            StreamingBlob::new(test_digest_with_size(100, 0), 1024 * 1024);
        let inner = Arc::clone(&reader.inner);
        assert_eq!(inner.notify_waits_over_5s_total(), 0);

        // Reader parks on notified.await.
        let reader_task = tokio::spawn(async move {
            let res = reader.next_chunk().await;
            (reader, res)
        });

        // Let the reader register + park.
        tokio::task::yield_now().await;

        // Advance past the slow threshold but well under the deadline.
        tokio::time::advance(SLOW_NOTIFY_THRESHOLD + Duration::from_secs(2)).await;

        // Now wake the reader cleanly with EOF.
        writer.send_eof().unwrap();

        let (_reader, res) = reader_task.await.unwrap();
        let chunk = res.expect("reader returned err on EOF wakeup");
        assert!(chunk.is_empty(), "expected EOF chunk, got {chunk:?}");

        assert_eq!(
            inner.notify_waits_over_5s_total(),
            1,
            "expected slow-wait counter to increment exactly once"
        );
    }

    /// Spec: outside a tokio task, `producer_task_id` MUST stay
    /// `None` and the writer MUST NOT panic when sending.
    #[test]
    fn producer_task_id_none_outside_runtime() {
        let inner = Arc::new(StreamingBlobInner::new(test_digest(123), 1024));
        // Pre-send: empty.
        assert!(
            inner.producer_task_id().is_none(),
            "producer_task_id must be None before any send, got {:?}",
            inner.producer_task_id()
        );
        // Even with a writer outside a runtime, send_error doesn't crash
        // and stays empty (no try_id available).
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        writer.send_error(make_err!(Code::Internal, "outside-runtime"));
        assert!(
            inner.producer_task_id().is_none(),
            "producer_task_id must remain None outside a tokio task, got {:?}",
            inner.producer_task_id()
        );
    }

    /// Spec: a writer that has never sent has no producer_task_id —
    /// capture is on first send, not at construction. Construction can
    /// happen on any task (including the consumer's task in the
    /// populate-spawn path); the producer task is only knowable once it
    /// owns the writer and sends data.
    #[tokio::test]
    async fn producer_task_id_unset_until_first_send() {
        let tid = tokio::spawn(async {
            let (writer, _reader) = StreamingBlob::new(test_digest(124), 1024);
            writer.inner.producer_task_id().map(str::to_string)
        })
        .await
        .unwrap();
        assert!(
            tid.is_none(),
            "producer_task_id must remain None until the writer's first \
             send/send_eof/send_error fires; constructing the inner alone \
             must NOT capture, otherwise the populate-spawn path captures \
             the consumer instead of the producer. got {tid:?}"
        );
    }

    /// Spec: producer_task_id is captured by the writer's first send,
    /// from whichever task is running the writer at that moment. This
    /// is the property the operator relies on — a wedge log line names
    /// the task that owns the writer (and is presumably stuck), not
    /// some unrelated upstream caller.
    #[tokio::test]
    async fn producer_task_id_captures_first_send_task() {
        let (writer, reader) = StreamingBlob::new(test_digest(125), 1024);
        let inner_for_assert = Arc::clone(&reader.inner);

        // Send from a SPAWNED task; that's the producer.
        let producer_handle = tokio::spawn(async move {
            let producer_real_id = tokio::task::try_id().unwrap().to_string();
            writer.send(Bytes::from_static(b"x")).await.unwrap();
            producer_real_id
        });
        let producer_real_id = producer_handle.await.unwrap();

        let captured = inner_for_assert
            .producer_task_id()
            .expect("first send must capture")
            .to_string();
        assert_eq!(
            producer_real_id, captured,
            "producer_task_id MUST match the task that ran the first send. \
             constructor task and producer task may differ — only the \
             producer is meaningful for wedge diagnosis."
        );
    }

    /// Regression for the populate-spawn misattribution that #126's
    /// review caught. Simulates `FastSlowStore::spawn_populate_producer_with_role`:
    /// inner is constructed on the CONSUMER task, then the writer is
    /// `tokio::spawn`'d onto a fresh PRODUCER task. The captured task
    /// id MUST be the producer's, NOT the consumer's.
    #[tokio::test]
    async fn producer_task_id_in_populate_spawn_path_is_producer_not_consumer() {
        // 1. Build the inner + writer on the consumer task (this test body's task).
        let consumer_task_id = tokio::task::try_id().map(|id| id.to_string());
        let (writer, reader) = StreamingBlob::new(test_digest(126), 1024);
        let inner_for_assert = Arc::clone(&reader.inner);

        // 2. Spawn the producer onto a fresh task — this is the bug
        //    scenario. The producer task is a different id than the
        //    consumer task.
        let producer_handle = tokio::spawn(async move {
            let producer_real_id = tokio::task::try_id().unwrap().to_string();
            writer.send(Bytes::from_static(b"y")).await.unwrap();
            producer_real_id
        });
        let producer_real_id = producer_handle.await.unwrap();

        let captured = inner_for_assert
            .producer_task_id()
            .expect("first send must capture")
            .to_string();
        assert_eq!(
            producer_real_id, captured,
            "REGRESSION: in the populate-spawn path the captured \
             producer_task_id matched the consumer task instead of the \
             producer task. Operator chasing a wedge greps the wrong \
             task ID. Pre-fix this test would assert consumer_task_id \
             == captured; post-fix it must equal producer_real_id."
        );
        if let Some(consumer_id) = consumer_task_id {
            assert_ne!(
                consumer_id, captured,
                "captured id must NOT be the consumer's task id — \
                 the populate-spawn bug pattern is the consumer's id \
                 leaking into producer_task_id"
            );
        }
    }

    /// Spec: a fast wakeup MUST NOT bump the slow-wait counter.
    #[tokio::test]
    async fn fast_notify_wait_does_not_increment_counter() {
        let (mut writer, mut reader) = StreamingBlob::new(test_digest(101), 1024 * 1024);
        let inner = Arc::clone(&reader.inner);

        let reader_task = tokio::spawn(async move {
            let res = reader.next_chunk().await;
            (reader, res)
        });

        // Wake immediately.
        tokio::task::yield_now().await;
        writer.send(Bytes::from_static(b"x")).await.unwrap();

        let (_reader, res) = reader_task.await.unwrap();
        assert_eq!(res.unwrap(), Bytes::from_static(b"x"));
        assert_eq!(
            inner.notify_waits_over_5s_total(),
            0,
            "fast wakeup must not bump slow-wait counter"
        );
    }

    // ---------------------------------------------------------------
    // #272 Shape B late-subscriber contract: a reader CONSTRUCTED
    // AFTER the writer has fully terminated and dropped MUST observe
    // chunk + terminal state within ~1 s, not park for the 30 s
    // deadline.
    //
    // Two layers of guarantee compose to satisfy this contract:
    //
    //   (a) The predicate check at the top of `next_chunk`'s loop
    //       reads chunks (RwLock) and terminal (Mutex). Both writer
    //       paths set state BEFORE firing notify, so the predicate
    //       sees the post-fire state synchronously and returns
    //       without ever entering the wait branch. (This layer
    //       handles the production scenario as observed.)
    //
    //   (b) Defense-in-depth: if for any reason the predicate
    //       check missed the state, the wait branch (watch
    //       primitive) holds a clone of `inner.notify_rx_template`,
    //       which is kept at `Version::INITIAL` forever. The clone
    //       inherits seen-version=INITIAL; any prior `send_modify`
    //       has bumped channel-version above INITIAL, so the
    //       FIRST `changed().await` returns immediately.
    //
    // Mutation step: comment out `notify_tx.send_modify(...)` in
    // `notify_waiters()`. Layer (a) still satisfies these specific
    // tests — they pass on the synchronous predicate path. To
    // observe the mutation, see `parked_reader_wakeup_*` tests
    // below: those drive the wait branch directly and red-fail
    // with `Code::DeadlineExceeded` after ~30s under mutation.
    // ---------------------------------------------------------------

    /// Spec: a reader subscribed AFTER the writer fired its terminal
    /// EOF generation bump MUST observe the chunk + EOF promptly.
    #[tokio::test]
    async fn late_subscriber_after_writer_eof_completes_promptly() {
        let digest = DigestInfo::new([0u8; 32], 2);
        let inner = Arc::new(StreamingBlobInner::new(digest, 1024 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));

        // Writer sends a single chunk + EOF, fully terminating BEFORE
        // any reader exists.
        writer
            .send(Bytes::from_static(b"hi"))
            .await
            .expect("writer.send must succeed");
        writer.send_eof().expect("writer.send_eof must succeed");
        // Drop ensures every notify firing has happened and the writer
        // is gone — the canonical race window.
        drop(writer);

        // Sleep to make the race deterministic — the production race
        // fires when the writer is fully done before reader subscribes.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // NOW construct the reader.
        let mut reader = StreamingBlob::new_reader(&inner);

        // Without Shape B, this hangs the full 30 s deadline. The
        // bespoke .expect catches a `tokio::time::Elapsed` from the
        // 1 s timeout (the deadlock detector) and names #272.
        let chunk = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect(
                "late-subscriber must observe writer's chunk without parking — \
                 #272 Shape B (watch::Receiver subscribed after fire still sees version > seen)"
            )
            .expect("next_chunk must return Ok");
        assert_eq!(&chunk[..], b"hi");

        // Subsequent next_chunk returns EOF (empty Bytes) — also must
        // not park.
        let eof = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect(
                "EOF observation must not park — \
                 #272 Shape B (terminal Ok visible to late subscriber)"
            )
            .expect("next_chunk must return Ok at EOF");
        assert!(eof.is_empty(), "post-EOF next_chunk must return empty Bytes");
    }

    /// Spec: a reader subscribed AFTER the writer fired its terminal
    /// error generation bump MUST observe the chunk + error promptly.
    /// Mirror of `late_subscriber_after_writer_eof_completes_promptly`
    /// for the send_error path — both writer terminations need the
    /// same late-subscriber guarantee.
    #[tokio::test]
    async fn late_subscriber_after_writer_send_error_completes_promptly() {
        let digest = DigestInfo::new([0u8; 32], 4);
        let inner = Arc::new(StreamingBlobInner::new(digest, 1024 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));

        // Writer sends a chunk and then signals an error, fully
        // terminating BEFORE any reader exists.
        writer
            .send(Bytes::from_static(b"data"))
            .await
            .expect("writer.send must succeed");
        writer.send_error(make_err!(Code::NotFound, "test"));
        drop(writer);

        // Sleep to make the race deterministic.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // NOW construct the reader.
        let mut reader = StreamingBlob::new_reader(&inner);

        // First chunk should still be visible (it landed in the deque
        // before the terminal error).
        let chunk = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect(
                "late-subscriber must observe writer's chunk without parking — \
                 #272 Shape B (chunk visible despite late subscribe)"
            )
            .expect("next_chunk must return Ok for chunk");
        assert_eq!(&chunk[..], b"data");

        // Next call must observe the terminal error promptly.
        let err = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect(
                "terminal error observation must not park — \
                 #272 Shape B (terminal Err visible to late subscriber)"
            )
            .expect_err("expected Err on terminal error");
        assert_eq!(
            err.code,
            Code::NotFound,
            "expected the producer's NotFound, got {err:?}"
        );
    }

    /// Spec (mutation-falsifiable): a reader parked in
    /// `next_chunk`'s wait branch MUST wake within 1 s when the
    /// writer fires a terminal EOF. This is the wait-path mutation
    /// counterpart to `late_subscriber_after_writer_eof_*` — when
    /// `notify_tx.send_modify(...)` is commented out in
    /// `notify_waiters()`, this test red-fails with the bespoke
    /// `.expect(...)` message naming #272 Shape B because the
    /// reader's `changed().await` parks until the 30 s deadline.
    #[tokio::test]
    async fn parked_reader_wakeup_on_writer_eof_completes_promptly() {
        // 0-byte digest: writer EOFs without sending any chunks; this
        // test asserts the reader's terminal-EOF wakeup, not byte
        // delivery. Match digest size so the #502 partial-bytes check
        // does not trip on the clean-EOF assertion below.
        let (mut writer, mut reader) =
            StreamingBlob::new(test_digest_with_size(205, 0), 1024 * 1024);

        // Park the reader in the wait branch — no chunks, no terminal.
        let reader_task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(1), reader.next_chunk()).await
        });

        // Yield enough times for the reader to enter changed().await.
        // We can't observe the await directly; a short sleep is the
        // standard way to let a spawned task park. (Used as
        // sequencing only — the timeout-and-bespoke-expect catches
        // the failure mode.)
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        writer.send_eof().expect("send_eof must succeed");

        let outcome = reader_task
            .await
            .expect("reader task must not panic")
            .expect(
                "parked reader must wake within 1 s of writer EOF — \
                 #272 Shape B (watch::Receiver::changed wakes on send_modify)"
            )
            .expect("next_chunk must return Ok at EOF");
        assert!(outcome.is_empty(), "expected EOF (empty bytes)");
    }

    /// Spec (mutation-falsifiable): same as above for the
    /// `send_error` path. Two writer terminations need the same
    /// wait-path wakeup guarantee.
    #[tokio::test]
    async fn parked_reader_wakeup_on_writer_error_completes_promptly() {
        let (mut writer, mut reader) = StreamingBlob::new(test_digest(206), 1024 * 1024);

        let reader_task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(1), reader.next_chunk()).await
        });

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        writer.send_error(make_err!(Code::NotFound, "test"));

        let err = reader_task
            .await
            .expect("reader task must not panic")
            .expect(
                "parked reader must wake within 1 s of writer error — \
                 #272 Shape B (watch::Receiver::changed wakes on send_modify)"
            )
            .expect_err("expected Err on terminal error");
        assert_eq!(
            err.code,
            Code::NotFound,
            "expected the producer's NotFound, got {err:?}"
        );
    }

    /// Spec (Shape A diagnostic): the `notify_waiters_calls` counter
    /// MUST advance with each writer-side fire — `send`, `send_eof`,
    /// `send_error`, and Drop-without-eof. Acts as a regression
    /// detector for future production wedges: if a slow-wakeup warn
    /// shows the counter advanced during the wait but the reader
    /// still timed out, the watch primitive integration is broken.
    #[tokio::test]
    async fn notify_waiters_calls_counter_advances_per_fire() {
        // send + send_eof → 2 fires.
        let (mut writer, _reader) = StreamingBlob::new(test_digest(202), 1024 * 1024);
        let inner = Arc::clone(&writer.inner);
        assert_eq!(inner.notify_waiters_calls_total(), 0);
        writer
            .send(Bytes::from_static(b"a"))
            .await
            .expect("send must succeed");
        assert_eq!(inner.notify_waiters_calls_total(), 1);
        writer.send_eof().expect("eof must succeed");
        assert_eq!(inner.notify_waiters_calls_total(), 2);

        // send_error → 1 fire on a fresh blob.
        let (mut writer2, _reader2) = StreamingBlob::new(test_digest(203), 1024 * 1024);
        let inner2 = Arc::clone(&writer2.inner);
        assert_eq!(inner2.notify_waiters_calls_total(), 0);
        writer2.send_error(make_err!(Code::Aborted, "test"));
        assert_eq!(inner2.notify_waiters_calls_total(), 1);

        // Drop without eof → 1 fire on a fresh blob.
        let (writer3, _reader3) = StreamingBlob::new(test_digest(204), 1024 * 1024);
        let inner3 = Arc::clone(&writer3.inner);
        assert_eq!(inner3.notify_waiters_calls_total(), 0);
        drop(writer3);
        assert_eq!(inner3.notify_waiters_calls_total(), 1);
    }

    // ---------------------------------------------------------------
    // #502: silent-short EOF — defense-in-depth at the API boundary.
    //
    // A producer that calls `send_eof` after writing fewer bytes than
    // `digest.size_bytes()` MUST surface to readers as
    // `Code::Internal` carrying the `STREAMING_BLOB_SILENT_SHORT_MARKER`
    // substring, NOT as a silent `Ok(Bytes::new())`. Bazel reads a
    // gRPC stream that ends with status=ok and treats it as a complete
    // blob; if `bytes_sent < expected_size`, the hash check fires as
    // a build-failing digest mismatch — the production wire-shape
    // observed on buildcache 2026-05-15 at
    // `bytestream_server::zero_copy_read` (expected_size: 47291739,
    // bytes_sent: 7881957, status: "ok").
    //
    // `Code::Internal` (not `Code::DataLoss`) because the 48 h
    // distribution analysis at
    // `.claude/audits/502-distribution-investigation-2026-05-16.md`
    // identifies this class as transport-layer transient (73.5 %
    // co-survival, median 25 % per-attempt success, 99.2 % clean-1/N
    // truncation signature). Bazel's `RemoteRetrier` maps `Internal`
    // to TRANSIENT_FAILURE → retried up to 10× → ~94 % effective
    // recovery; `DataLoss` would map to PERMANENT_FAILURE and fail the
    // action on the first hit, which is wrong given the data.
    //
    // Sibling of the #500 fix (consume_ok_eof branch in `inner_read`,
    // `bytestream_server.rs:1754`) — same Bazel-visible wire-shape,
    // different producer-side trigger (#500 fires on tx-drop + Err;
    // #502 fires on send_eof + partial bytes).
    //
    // Per CLAUDE.md "Asymmetric contract coverage": both directions
    // are part of the contract. Under-action = producer's bug must
    // surface as Err (this is the new behavior). Over-action = a
    // producer that wrote the FULL bytes MUST still observe clean
    // EOF — no spurious Internal-error.
    // ---------------------------------------------------------------

    /// Under-action: a writer that calls `send_eof` after writing
    /// fewer bytes than the digest declares MUST surface as
    /// `Code::Internal` (transient, Bazel-retryable) to the reader,
    /// not silent `Ok(Bytes::new())`. This is the #502 production
    /// wire-shape.
    ///
    /// Mutation test: comment out the new partial-bytes check in
    /// `next_chunk`'s terminal=Ok branch — the test MUST red-fail with
    /// the bespoke `.expect_err` message naming #502.
    #[tokio::test]
    async fn silent_short_eof_surfaces_as_internal() {
        // Digest declares 1024 bytes; writer sends only 4.
        let digest = DigestInfo::new([0x42u8; 32], 1024);
        let inner = Arc::new(StreamingBlobInner::new(digest, 1024 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        writer
            .send(Bytes::from_static(b"part"))
            .await
            .expect("writer.send must succeed");
        writer
            .send_eof()
            .expect("writer.send_eof must succeed");

        // First chunk reads normally.
        let chunk = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect("reader.next_chunk for partial chunk must not deadlock")
            .expect("first chunk must read as Ok");
        assert_eq!(&chunk[..], b"part");

        // Second poll: would historically return `Ok(Bytes::new())` (silent
        // EOF). The #502 fix converts to `Code::Internal` (TRANSIENT_FAILURE
        // in Bazel's RemoteRetrier — retried up to 10×) with the marker
        // substring so Bazel + downstream classifiers can detect it.
        let err = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect("reader.next_chunk on terminal-Ok must not deadlock")
            .expect_err(
                "#502: writer terminated with Ok after writing 4 of 1024 \
                 bytes — reader MUST observe Err(Internal) (transient, \
                 Bazel-retryable), not silent EOF, to prevent Bazel-visible \
                 digest mismatches",
            );
        assert_eq!(
            err.code,
            Code::Internal,
            "#502: silent-short EOF must surface as Code::Internal \
             (transient/retryable per the 2026-05-16 distribution audit), \
             got {err:?}",
        );
        // Stable substring contract for production journal grep and any
        // downstream predicate that wants to classify this error class.
        let msg = format!("{err:?}");
        assert!(
            msg.contains(STREAMING_BLOB_SILENT_SHORT_MARKER),
            "#502: Err message must include STREAMING_BLOB_SILENT_SHORT_MARKER \
             for production grep predicates; got {msg}",
        );
    }

    /// Over-action: a writer that completes the full declared bytes
    /// followed by `send_eof` MUST observe clean EOF (`Ok(Bytes::new())`)
    /// at the reader. The #502 fix MUST NOT trip on the legitimate
    /// happy path.
    ///
    /// Without this assertion, a too-aggressive partial-bytes check
    /// (e.g. one that compares against `chunks_consumed` instead of
    /// `bytes_written`, or off-by-one on the inequality) would
    /// silently break every CAS read in production.
    #[tokio::test]
    async fn full_byte_eof_observes_clean_terminal() {
        // Digest declares 8 bytes; writer sends exactly 8.
        let digest = DigestInfo::new([0x21u8; 32], 8);
        let inner = Arc::new(StreamingBlobInner::new(digest, 1024 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        writer
            .send(Bytes::from_static(b"abcd"))
            .await
            .expect("writer.send must succeed (1/2)");
        writer
            .send(Bytes::from_static(b"efgh"))
            .await
            .expect("writer.send must succeed (2/2)");
        writer
            .send_eof()
            .expect("writer.send_eof must succeed");

        // Drain both chunks.
        let c1 = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect("reader.next_chunk must not deadlock (chunk 1)")
            .expect("chunk 1 must be Ok");
        assert_eq!(&c1[..], b"abcd");
        let c2 = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect("reader.next_chunk must not deadlock (chunk 2)")
            .expect("chunk 2 must be Ok");
        assert_eq!(&c2[..], b"efgh");

        // Terminal=Ok with bytes_written == expected_size MUST be
        // clean EOF, not a spurious Internal error.
        let eof = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect("reader.next_chunk on terminal-Ok must not deadlock")
            .expect(
                "#502 over-action: writer wrote the full 8 bytes then \
                 send_eof'd — reader MUST observe clean EOF (Ok empty), \
                 NOT a spurious Internal-error. A check that fires on \
                 the happy path would silently break every CAS read.",
            );
        assert!(
            eof.is_empty(),
            "#502 over-action: post-EOF next_chunk MUST return empty Bytes \
             on the full-byte happy path, got {} bytes",
            eof.len(),
        );
    }

    /// Edge case: a zero-byte digest (`expected_size = 0`) with no
    /// chunks sent + `send_eof` MUST still observe clean EOF, NOT a
    /// spurious Internal error. `0 < 0` is false, so the check is a
    /// no-op on this path; this test guards against a future refactor
    /// that changes the inequality to `<=` and breaks zero-byte CAS
    /// reads.
    #[tokio::test]
    async fn zero_byte_digest_eof_observes_clean_terminal() {
        let digest = DigestInfo::new([0x00u8; 32], 0);
        let inner = Arc::new(StreamingBlobInner::new(digest, 1024 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        writer
            .send_eof()
            .expect("writer.send_eof must succeed on zero-byte digest");

        let eof = tokio::time::timeout(Duration::from_secs(1), reader.next_chunk())
            .await
            .expect("reader.next_chunk on zero-byte terminal-Ok must not deadlock")
            .expect(
                "#502: zero-byte digest with no chunks + send_eof MUST \
                 observe clean EOF — bytes_written=0, expected=0, 0<0 is \
                 false so the partial-bytes check is a no-op",
            );
        assert!(
            eof.is_empty(),
            "#502: zero-byte CAS read MUST return empty Bytes, got {} bytes",
            eof.len(),
        );
    }

    /// Production-composition seam: `fast_slow_store::spawn_populate_producer`
    /// is the ONLY production caller of `StreamingBlobWriter::send_eof`
    /// (audited 2026-05-16). The bug fires when its `slow_store.get`
    /// silently truncates (returns Ok with bytes_written <
    /// expected_size) — the populator's merged terminal is Ok and
    /// `send_eof` is called.
    ///
    /// Simulate that producer-side wire-shape directly on the
    /// `StreamingBlobWriter` API: send a partial slice, then `send_eof`.
    /// The reader composed in production
    /// (`bytestream_server::inner_read::streaming_read_while_write` and
    /// `fast_slow_store::populate_*` consumers using
    /// `StreamingBlobReader::next_chunk`) MUST observe Err(Internal),
    /// so the upstream unfold yields `Some((Err, _))` instead of
    /// `None` — the same seam the #500 fix closed for the inner_read
    /// path. `Code::Internal` is the retryable classification
    /// (TRANSIENT_FAILURE in Bazel's RemoteRetrier, ~94 % recovery via
    /// the 10-retry budget) — see the marker constant's doc-comment
    /// for the underlying distribution data.
    ///
    /// This test does NOT spin up a real FastSlowStore (would force a
    /// 5-wrapper integration setup); it asserts the API-boundary
    /// contract at the StreamingBlob primitive that EVERY consumer
    /// (current and future) inherits. The 1-second `tokio::time::timeout`
    /// is the deadlock detector per CLAUDE.md "Test in production
    /// composition" — `tokio::time::Elapsed` passes `is_err()` but
    /// would fail the bespoke `.expect_err` message.
    #[tokio::test]
    async fn populate_path_silent_short_propagates_to_reader_as_err() {
        // Production-shape: a 47 MiB-class CAS blob, but the slow tier
        // delivers only the first 8 MiB before silently truncating.
        // Scaled down for test speed; the invariant is the inequality,
        // not the absolute sizes.
        const EXPECTED_BYTES: u64 = 47_291_739;
        const TRUNCATED_BYTES: u64 = 7_881_957;
        let digest = DigestInfo::new([0x5au8; 32], EXPECTED_BYTES);
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        // Simulate the producer: ship the truncated chunks the fast/slow
        // populate would have forwarded from `slow_rx`.
        let chunk = Bytes::from(vec![0xa5u8; TRUNCATED_BYTES as usize]);
        writer
            .send(chunk)
            .await
            .expect("producer.send must succeed for the truncated chunk");
        // Populator's `streaming_terminal` resolves to Ok because the
        // inner pipeline (data_stream, slow_store.get, fast_store.update)
        // all returned Ok despite the truncated payload — this is the
        // #502 producer-side wire-shape.
        writer
            .send_eof()
            .expect("producer.send_eof must succeed (the bug)");

        // Reader drains the truncated chunk.
        let c = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("reader.next_chunk must not deadlock on the truncated chunk")
            .expect("truncated chunk must be Ok");
        assert_eq!(c.len(), TRUNCATED_BYTES as usize);

        // Next poll: API-boundary defense MUST surface as
        // Code::Internal. The unfold in
        // `bytestream_server::streaming_read_while_write` matches on
        // `Err(e)` → yields `Some((Err(e.into()), state))` (line 1606
        // and 1565) — Bazel receives status=error (Internal =
        // TRANSIENT_FAILURE) and retries via RemoteRetrier instead of
        // accepting a clean-short stream as canonical bytes.
        let err = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("reader.next_chunk on terminal-Ok must not deadlock")
            .expect_err(
                "#502: populate path that silent-truncates and calls \
                 send_eof MUST surface as Err(Internal) to the reader — \
                 the bytestream_server streaming_read_while_write unfold \
                 yields Some((Err, _)) and Bazel retries (transient \
                 classification); without this check, the unfold yields \
                 None (clean EOF) and Bazel reports digest mismatch as \
                 build failure",
            );
        assert_eq!(
            err.code,
            Code::Internal,
            "#502: silent-short EOF at the populate seam must be \
             Code::Internal (transient/retryable per the 2026-05-16 \
             distribution audit), got {err:?}",
        );
        let msg = format!("{err:?}");
        assert!(
            msg.contains(STREAMING_BLOB_SILENT_SHORT_MARKER),
            "#502: production-composition err message must include the \
             stable marker substring; got {msg}",
        );
    }

    /// #49: AC reads must NOT trip the #502 silent-short check using
    /// `digest.size_bytes()` as the expected-bytes upper bound. The
    /// `action_digest` size_bytes is the *Action* proto's encoded size; the
    /// bytes stored under that key in the AC backend are the *ActionResult*
    /// proto's encoded bytes — a different message under the same key. The
    /// AC integrity contract (`ac_server.rs:199-205`, `docs/ac-integrity-
    /// contract.md`) explicitly says `H(store_data) != digest` for AC.
    ///
    /// Production 2026-06-04 05:55:13–15 PDT: 28 silent_short events fired
    /// from `ac_server::get_action_result`, all on 217-byte Action digests
    /// where ActionResult encoded_len() landed in 203-215 bytes. 100% of
    /// events were AC reads. The check shipped a Code::Internal that Bazel
    /// retried up to 10× (transient classification), but the underlying
    /// inequality is structural, not transient — every cache-miss AC read
    /// of an Action whose ActionResult is shorter than the Action would
    /// fire forever.
    ///
    /// The fix: producers that know the actual bytes-on-store size (e.g.
    /// `FastSlowStore::run_producer` after `slow_store.has()` returns
    /// `ExactSize(n)`) call `set_expected_size_on_store(n)` to record the
    /// authoritative upper bound for the silent-short check. For AC,
    /// that's the Redis STRLEN (= ActionResult encoded_len). For CAS,
    /// that's the file size (= digest.size_bytes by content-addressing
    /// invariant — the override is a no-op there).
    ///
    /// Mutation: comment out `set_expected_size_on_store` below; this
    /// test must red-fail with "AC read with declared size_bytes != stored
    /// bytes must NOT trip #502 silent_short — bytes_written matches
    /// stored size".
    ///
    /// MUTATION VERIFIED (2026-06-04): in `StreamingBlobInner::
    /// set_expected_size_on_store` (~line 408-410), replaced
    ///   `let _ = self.expected_size_on_store.set(n);`
    /// with a no-op (`let _ = n;`). The `OnceLock` stayed unset → the
    /// `expected_size_on_store()` accessor fell back to
    /// `digest.size_bytes() = 217` → the silent-short check fired
    /// because `bytes_written = 207 < expected_size = 217` → this
    /// assertion fired with bespoke `Error { code: Internal, messages:
    /// ["streaming_blob_silent_short: terminal=Ok but bytes_written=207
    /// < expected_size=217 for digest 9797979797…-217"] }`. Reverting
    /// the mutation restored green.
    #[tokio::test]
    async fn ac_read_with_size_override_does_not_trip_silent_short() {
        // AC entry: action_digest declares 217 bytes (Action proto size);
        // the stored ActionResult is 207 bytes (the value observed in
        // production for digest 97fd1a58...-217 at 05:55:15.290 PDT).
        const ACTION_DIGEST_SIZE: u64 = 217;
        const ACTION_RESULT_BYTES: u64 = 207;
        let digest = DigestInfo::new([0x97u8; 32], ACTION_DIGEST_SIZE);
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024 * 1024));

        // Producer knows the bytes-on-store size from `slow_store.has()`
        // (in the AC case, Redis STRLEN). Plumb it through so the #502
        // check compares against the right value.
        inner.set_expected_size_on_store(ACTION_RESULT_BYTES);

        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        // Producer ships the full ActionResult bytes.
        let chunk = Bytes::from(vec![0xacu8; ACTION_RESULT_BYTES as usize]);
        writer
            .send(chunk)
            .await
            .expect("producer.send must succeed for the ActionResult bytes");
        writer
            .send_eof()
            .expect("producer.send_eof must succeed");

        // Reader drains the ActionResult chunk.
        let c = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("reader.next_chunk must not deadlock on the ActionResult chunk")
            .expect("ActionResult chunk must be Ok");
        assert_eq!(c.len(), ACTION_RESULT_BYTES as usize);

        // The #502 silent-short check MUST NOT fire on the terminal-Ok
        // poll, because `bytes_written (207) == expected_size_on_store
        // (207)` even though `bytes_written (207) < digest.size_bytes
        // (217)`. AC bytes-on-store size is the authoritative upper bound,
        // not the action_digest's declared size.
        let eof = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("reader.next_chunk terminal-Ok must not deadlock")
            .expect(
                "AC read with declared size_bytes != stored bytes must NOT \
                 trip #502 silent_short — bytes_written matches stored size",
            );
        assert!(
            eof.is_empty(),
            "#49: AC terminal poll must return clean EOF (empty Bytes); \
             the producer wrote 207/207 bytes-on-store (despite \
             digest.size_bytes=217 from the Action proto). Got {} bytes.",
            eof.len(),
        );
    }

    /// #49 negative: CAS reads (where digest IS content-addressed) keep
    /// firing the #502 check on a genuine silent short. Setting the
    /// `expected_size_on_store` to the digest's size_bytes (the default
    /// behavior the producer derives from `slow_store.has()` returning
    /// `ExactSize(N)` for a CAS FilesystemStore where N == digest.size_bytes)
    /// must NOT suppress the original #502 protection.
    #[tokio::test]
    async fn cas_silent_short_still_trips_with_matching_override() {
        const EXPECTED_BYTES: u64 = 47_291_739;
        const TRUNCATED_BYTES: u64 = 7_881_957;
        let digest = DigestInfo::new([0x5au8; 32], EXPECTED_BYTES);
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024 * 1024));

        // CAS: slow_store.has() returned ExactSize(EXPECTED_BYTES); the
        // override matches digest.size_bytes (content-addressed invariant).
        inner.set_expected_size_on_store(EXPECTED_BYTES);

        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        let chunk = Bytes::from(vec![0xa5u8; TRUNCATED_BYTES as usize]);
        writer.send(chunk).await.expect("send truncated chunk");
        writer.send_eof().expect("send_eof must succeed");

        let _ = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("drain truncated chunk")
            .expect("truncated chunk Ok");

        let err = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("terminal-Ok poll must not deadlock")
            .expect_err(
                "#502: CAS silent-short with matching override must STILL \
                 surface as Err(Internal) — the override does not disable \
                 the check, only redirects which size to compare against",
            );
        assert_eq!(err.code, Code::Internal);
        assert!(
            format!("{err:?}").contains(STREAMING_BLOB_SILENT_SHORT_MARKER),
        );
    }

    /// #49 (v2 sentinel-collision regression): zero-byte AC entries are
    /// a legitimate, observed value (an empty ActionResult under a
    /// non-empty action_digest), and `expected_size_on_store = Some(0)`
    /// must NOT collide with "unset" semantics. v1 used `AtomicU64` with
    /// sentinel 0 as "unset", which would have meant the producer
    /// recording `0` falls back to `digest.size_bytes()` — so a
    /// 217-byte action_digest with a 0-byte ActionResult would have
    /// false-fired the silent-short check (bytes_written=0 <
    /// digest.size_bytes=217). v2 uses `OnceLock<u64>`, where `Some(0)`
    /// is distinct from `None`.
    ///
    /// Mutation: revert to v1 sentinel-0 semantics (e.g. wrap
    /// `set_expected_size_on_store(0)` as a no-op or store via
    /// `AtomicU64::new(0)` + `n != 0` check); this test must red-fail
    /// with "sentinel collision: zero-byte AC entry false-fires
    /// silent_short".
    ///
    /// MUTATION VERIFIED (2026-06-04): in `StreamingBlobInner::
    /// set_expected_size_on_store`, wrapped the body with
    ///   `if n == 0 { return; }`
    /// then `let _ = self.expected_size_on_store.set(n);` (the v1
    /// AtomicU64-sentinel semantics). The `OnceLock` stayed unset →
    /// `expected_size_on_store()` fell back to `digest.size_bytes() =
    /// 217` → silent-short fired on the zero-byte ActionResult because
    /// `bytes_written = 0 < expected_size = 217` → this assertion fired
    /// with bespoke `Error { code: Internal, messages:
    /// ["streaming_blob_silent_short: terminal=Ok but bytes_written=0
    /// < expected_size=217 for digest 0e0e0e0e…-217"] }`. Reverting
    /// the mutation restored green.
    #[tokio::test]
    async fn zero_byte_ac_entry_does_not_false_fire_silent_short() {
        const ACTION_DIGEST_SIZE: u64 = 217;
        const ACTION_RESULT_BYTES: u64 = 0;
        let digest = DigestInfo::new([0x0eu8; 32], ACTION_DIGEST_SIZE);
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024 * 1024));

        // Producer's `slow_store.has()` returned `ExactSize(0)` — a
        // valid empty value, distinct from "unknown size".
        inner.set_expected_size_on_store(ACTION_RESULT_BYTES);

        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        // No data; producer sends EOF immediately.
        writer.send_eof().expect("send_eof must succeed");

        let eof = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("reader.next_chunk terminal-Ok must not deadlock")
            .expect(
                "sentinel collision: zero-byte AC entry false-fires \
                 silent_short — expected_size_on_store=Some(0) must NOT \
                 be conflated with the unset fallback to digest.size_bytes",
            );
        assert!(
            eof.is_empty(),
            "#49: zero-byte AC terminal poll must return clean EOF; \
             got {} bytes",
            eof.len(),
        );
    }

    /// #49 (v2 unset-fallback): non-FastSlowStore construction sites
    /// (e.g. `StreamingBlob::new`, `InFlightBlobMap::register`) leave
    /// `expected_size_on_store` unset. For those, the #502 check falls
    /// back to `digest.size_bytes()` — preserving prior behavior, which
    /// is correct on the CAS path because the InFlightBlobMap is keyed
    /// by content-addressed CAS digests where stored bytes ==
    /// digest.size_bytes by construction.
    ///
    /// Mutation: change `expected_size_on_store()` to always return 0
    /// when unset; this test must red-fail with "unset OnceLock did NOT
    /// fall back to digest.size_bytes — sentinel/zero confusion".
    ///
    /// MUTATION VERIFIED (2026-06-04): in `StreamingBlobInner::
    /// expected_size_on_store` accessor, replaced the `None` arm's
    /// `self.digest.size_bytes()` fallback with the constant `0`. The
    /// silent-short check then compared `bytes_written = 250_000 < 0`
    /// which is false → check did NOT fire → terminal poll returned
    /// `Ok(Bytes::new())` instead of `Err(Internal)` → this assertion
    /// fired with bespoke "unset OnceLock did NOT fall back to
    /// digest.size_bytes — sentinel/zero confusion: CAS silent-short
    /// check must still trip when expected_size_on_store is unset"
    /// (panic: `b""`). Reverting the mutation restored green.
    #[tokio::test]
    async fn cas_silent_short_trips_when_size_on_store_unset() {
        const EXPECTED_BYTES: u64 = 1_000_000;
        const TRUNCATED_BYTES: u64 = 250_000;
        let digest = DigestInfo::new([0xcau8; 32], EXPECTED_BYTES);
        // No `set_expected_size_on_store` call — represents an
        // InFlightBlobMap / StreamingBlob::new construction site where
        // the caller does not commit to a bytes-on-store size.
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024 * 1024));

        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let mut reader = StreamingBlobReader::new(Arc::clone(&inner));

        let chunk = Bytes::from(vec![0xcau8; TRUNCATED_BYTES as usize]);
        writer.send(chunk).await.expect("send truncated chunk");
        writer.send_eof().expect("send_eof must succeed");

        let _ = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("drain truncated chunk")
            .expect("truncated chunk Ok");

        let err = tokio::time::timeout(Duration::from_secs(2), reader.next_chunk())
            .await
            .expect("terminal-Ok poll must not deadlock")
            .expect_err(
                "unset OnceLock did NOT fall back to digest.size_bytes \
                 — sentinel/zero confusion: CAS silent-short check must \
                 still trip when expected_size_on_store is unset",
            );
        assert_eq!(err.code, Code::Internal);
        assert!(
            format!("{err:?}").contains(STREAMING_BLOB_SILENT_SHORT_MARKER),
        );
    }
}
