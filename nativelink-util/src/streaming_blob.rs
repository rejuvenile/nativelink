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
use tokio::sync::Notify;
use tracing::{debug, error, warn};

use crate::common::DigestInfo;

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
    notify: Notify,

    /// Terminal state:
    /// - `None`       — writer still active
    /// - `Some(Ok)` — writer sent EOF (success)
    /// - `Some(Err)` — writer errored or dropped
    terminal: Mutex<Option<Result<(), Error>>>,

    /// Digest for this blob.
    digest: DigestInfo,

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
    pub fn new(digest: DigestInfo, max_buffer_bytes: u64) -> Self {
        Self {
            chunks: RwLock::new(VecDeque::new()),
            chunk_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            notify: Notify::new(),
            terminal: Mutex::new(None),
            digest,
            max_buffer_bytes,
            earliest_chunk_idx: AtomicU64::new(0),
            created_at: Instant::now(),
            notify_waits_over_5s: AtomicU64::new(0),
            producer_task_id: OnceLock::new(),
        }
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

        self.inner.notify.notify_waiters();
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

        self.inner.notify.notify_waiters();
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

        self.inner.notify.notify_waiters();
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
                self.inner.notify.notify_waiters();
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
        Self {
            inner,
            cursor_chunk_idx: earliest,
            cursor_byte_offset: 0,
            chunks_consumed: 0,
            terminal_seen: false,
            created_at: Instant::now(),
        }
    }

    /// Access the underlying `StreamingBlobInner` for state checks.
    pub fn inner(&self) -> &StreamingBlobInner {
        &self.inner
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
        #[cfg(feature = "failpoints")]
        fail::fail_point!("streaming_blob_next_chunk_fail", |_| {
            Err(make_err!(
                Code::Unavailable,
                "failpoint: streaming blob chunk read failed"
            ))
        });

        loop {
            // Subscribe BEFORE checking any predicates so a
            // notify_waiters() racing our predicate check / lock
            // drop is captured by this Notified future rather than
            // being silently dropped.  Same lost-wakeup pattern as
            // f1750357 (cleanup_complete_notify in
            // running_actions_manager).  Without this, the writer
            // can fire send_eof / send_error + notify_waiters in
            // the microsecond window between dropping the terminal
            // lock and calling notified().await — producing the
            // 120s reader hangs observed at 19:33:09 UTC on
            // worker-02.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let earliest = self.inner.earliest_chunk_idx.load(Ordering::Acquire);
            if self.cursor_chunk_idx < earliest {
                return Err(make_err!(
                    Code::Unavailable,
                    "reader fell behind sliding window (cursor={}, earliest={})",
                    self.cursor_chunk_idx,
                    earliest
                ));
            }

            let chunk_count = self.inner.chunk_count.load(Ordering::Acquire);

            // Check if a chunk is available at our cursor position.
            if self.cursor_chunk_idx < chunk_count {
                let chunks = self.inner.chunks.read();
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
                        Ok(()) => Ok(Bytes::new()),
                        Err(e) => Err(e.clone()),
                    };
                }
            }

            // Writer still active, no data yet — wait for
            // notification.  The Notified above was registered
            // BEFORE the predicate check, so any notify_waiters()
            // that fired since then is captured here and the
            // await returns immediately.
            //
            // Defense-in-depth: bound the wait with
            // STREAMING_BLOB_NOTIFY_TIMEOUT so the next missing-wakeup
            // bug surfaces as a logged DeadlineExceeded in seconds
            // rather than a 120 s gRPC stream wedge. The pinned
            // Notified is still passed through (preserves the
            // pin+enable correctness from 646d7623) — tokio::time::
            // timeout takes any future, including a pinned one.
            // Use tokio::time::Instant so paused-time tests can drive
            // the slow-wait + deadline branches deterministically; in
            // production it forwards to std::time::Instant.
            let wait_start = tokio::time::Instant::now();
            debug!(
                digest = %self.inner.digest,
                cursor_chunk_idx = self.cursor_chunk_idx,
                "streaming blob reader awaiting pre-registered notify"
            );
            let timeout_result =
                tokio::time::timeout(STREAMING_BLOB_NOTIFY_TIMEOUT, notified.as_mut()).await;
            let wait_elapsed = wait_start.elapsed();
            let terminal_present = self.inner.terminal.lock().is_some();
            if timeout_result.is_err() {
                let chunk_count = self.inner.chunk_count.load(Ordering::Acquire);
                let earliest = self.inner.earliest_chunk_idx.load(Ordering::Acquire);
                // terminal_present distinguishes two distinct failure modes that
                // both surface as "reader timed out waiting for notify":
                //   - true:  writer dropped/finished but its notify_waiters() did
                //            not wake this reader. Genuine lost wakeup; bug
                //            lives in the notify primitive integration here.
                //   - false: writer is still alive (no terminal state set);
                //            the producer task itself is wedged upstream of
                //            streaming_blob (e.g. blocked on a gRPC read with
                //            no per-frame deadline, holding a lock, or the
                //            tokio task is starved). Bug lives upstream.
                let producer_tid = self.inner.producer_task_id().unwrap_or("<none>");
                if terminal_present {
                    error!(
                        digest = %self.inner.digest,
                        age_ms = self.inner.age_ms(),
                        cursor_chunk_idx = self.cursor_chunk_idx,
                        chunk_count,
                        earliest,
                        wait_ms = wait_elapsed.as_millis() as u64,
                        producer_task_id = %producer_tid,
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
    fn test_digest(seed: u8) -> DigestInfo {
        let mut hash = [0u8; 32];
        hash[0] = seed;
        DigestInfo::new(hash, 1024)
    }

    // ---------------------------------------------------------------
    // 1. Single writer, single reader — data flows correctly
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn single_writer_single_reader() {
        let (writer, mut reader) = StreamingBlob::new(test_digest(1), 1024 * 1024);

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
        let (mut writer, mut reader1) = StreamingBlob::new(test_digest(2), 1024 * 1024);

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
        let (mut writer, mut reader) = StreamingBlob::new(test_digest(7), 1024 * 1024);

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
        // Large buffer so no eviction happens.
        let (mut writer, mut fast_reader) = StreamingBlob::new(test_digest(12), 1024 * 1024);

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
        // Buffer limited to 30 bytes. Each chunk is 10 bytes.
        let (writer, mut slow_reader) = StreamingBlob::new(test_digest(13), 30);

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
        let digest = test_digest(14);

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
    // Lost-wakeup race regression: a `notify_waiters()` that fires
    // strictly between the reader's terminal-predicate check (lock
    // dropped) and its `notified().await` must NOT be silently
    // dropped.  Reproduces the 120s reader hang observed at
    // 19:33:09 UTC on worker-02.
    //
    // The hang sequence:
    //
    //   1. Predicate check (terminal == None)        ← reader
    //   2. lock dropped
    //   3. terminal = Some(Err); notify_waiters()    ← writer
    //   4. notify.notified().await                   ← reader
    //
    // `tokio::sync::Notify::notify_waiters` only wakes Notified
    // futures that have already been polled (registered).  In step
    // 4 the Notified future is brand new — no registration existed
    // when notify_waiters fired — so the permit is dropped on the
    // floor and the await blocks forever (no more notifications
    // come because terminal is now sealed).
    //
    // The fix is the canonical subscribe-before-check pattern (see
    // f1750357 for cleanup_complete_notify): register the Notified
    // future BEFORE step 1 via `let n = notify.notified();
    // tokio::pin!(n); n.as_mut().enable();`.  Then step 3's
    // notify_waiters delivers a permit to the registered future
    // and the subsequent .await returns immediately.
    //
    // This test proves the underlying lost-wakeup property exists
    // on tokio::sync::Notify (so we know the bug is real), then
    // verifies that the same race driven through next_chunk does
    // not hang.
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn next_chunk_no_lost_wakeup_on_terminal_race() {
        let (writer, reader) = StreamingBlob::new(test_digest(200), 1024 * 1024);
        let inner = Arc::clone(&reader.inner);

        // Step 1+2: emulate the predicate-check window — reader
        // sees no terminal, drops the lock.  We don't call
        // `notified()` here: that's the bug we're testing for.
        {
            let t = inner.terminal.lock();
            assert!(t.is_none(), "precondition: terminal must start unset");
        }

        // Step 3: writer sets terminal and fires notify_waiters.
        // No reader is currently registered on the Notify, so this
        // wakeup is dropped on the floor (this is a defining
        // property of tokio::sync::Notify).
        {
            let mut t = inner.terminal.lock();
            *t = Some(Err(make_err!(Code::Aborted, "race-test error")));
        }
        inner.notify.notify_waiters();

        // Step 4: a freshly-constructed reader (re-using the same
        // inner) calls next_chunk.  The buggy implementation
        // checks terminal → returns the error here, so this exact
        // sequence does NOT reproduce the hang on the read path.
        // The hang reproduces when the predicate check happens
        // BEFORE the writer sets terminal.  Drive that case
        // directly using the same Notify primitive: we invoke the
        // exact two-line sequence next_chunk uses to wait, on a
        // fresh `inner` whose terminal is still None at predicate
        // time, with notify_waiters firing in the gap.
        drop(reader);

        let (writer2, reader2) = StreamingBlob::new(test_digest(201), 1024 * 1024);
        let inner2 = Arc::clone(&reader2.inner);

        // Spawn a writer that, after a one-shot signal, sets
        // terminal and fires notify_waiters.  The signal is
        // delivered AFTER the test (acting as the reader) has
        // performed the predicate check but BEFORE it has
        // subscribed to the Notify — exactly the lost-wakeup
        // window.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let writer_inner = Arc::clone(&inner2);
        let writer_task = tokio::spawn(async move {
            rx.await.unwrap();
            {
                let mut t = writer_inner.terminal.lock();
                *t = Some(Err(make_err!(Code::Aborted, "race-test error")));
            }
            writer_inner.notify.notify_waiters();
        });

        // Predicate check (mirrors lines 414-436 of next_chunk).
        {
            let t = inner2.terminal.lock();
            assert!(t.is_none(), "precondition");
        }
        // Open the lost-wakeup window.
        tx.send(()).unwrap();
        // Wait for the writer to complete BOTH steps before we
        // subscribe — this is what the buggy code does (subscribe
        // late).  joining the spawn ensures notify_waiters has
        // already fired before notified() is called.
        writer_task.await.unwrap();

        // Now mirror the buggy subscribe-after-check: brand new
        // notified() future, polled for the first time AFTER
        // notify_waiters has already fired.
        let buggy_future = inner2.notify.notified();
        let buggy_outcome = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            buggy_future,
        )
        .await;
        assert!(
            buggy_outcome.is_err(),
            "sanity: subscribe-after-notify_waiters MUST be a \
             lost wakeup (this is the bug we're guarding against)"
        );

        // Now assert that next_chunk itself does NOT exhibit this
        // hang — even when invoked AFTER terminal was set and
        // notify_waiters has already fired.  With the fix in
        // place, next_chunk's predicate check sees terminal set
        // and returns immediately.  Without the fix, the same is
        // also true on this path; the real-world hang requires
        // the write to land in the predicate-vs-subscribe gap of
        // next_chunk, which we can only prove via the structural
        // invariant: the next_chunk source must register
        // `notified()` BEFORE the terminal predicate check.
        let mut reader2 = StreamingBlob::new_reader(&inner2);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            reader2.next_chunk(),
        )
        .await
        .expect("next_chunk hung past 200ms");
        assert!(result.is_err(), "expected terminal Err");
        drop(writer);
        drop(writer2);
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
        let (mut writer, mut reader) = StreamingBlob::new(test_digest(100), 1024 * 1024);
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
}
