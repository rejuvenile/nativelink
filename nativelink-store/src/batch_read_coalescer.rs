// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! Server→worker proxy-read opportunistic coalescer for small blobs.
//!
//! Tracker: `#88: batch_read_small_blobs racing wastes bandwidth — both
//! peer and server batches run to completion`. This module addresses the
//! per-RPC overhead component: when the server's `WorkerProxyStore` falls
//! through to a worker for many concurrent small-blob reads (typical
//! Bazel input-fetch fanout), today each blob takes one ByteStream
//! `Read` RPC. This coalescer collects concurrent same-target small-blob
//! requests into one REAPI `BatchReadBlobs` RPC against the same worker.
//!
//! # Design (mirrors `small_blob_dispatcher` in spirit)
//!
//! - **Per-target queue**: one mpsc channel per worker endpoint. Multiple
//!   endpoints fan out independently; one worker's slowness does not
//!   block another.
//! - **Zero-window opportunistic coalesce** (matches `grpc_store.rs`
//!   `batch_flush_loop`): drainer pulls the first item via `recv()`,
//!   yields once with `tokio::task::yield_now()` so any concurrently
//!   ready producers can enqueue, then `try_recv()` until the queue is
//!   empty OR the byte cap is reached. No artificial sleep — the only
//!   wall-clock cost is one runtime yield.
//! - **Single-flight dedup**: requests for the same digest collapse to
//!   one slot in the outgoing batch; every caller waits on the same
//!   per-digest oneshot fanout.
//! - **Wire protocol**: REAPI `BatchReadBlobs` against the worker's CAS
//!   endpoint. The worker's `cas_server::inner_batch_read_blobs` already
//!   handles this RPC (no proto change). Justification chosen over a
//!   custom `BatchReadSmallBlobsRequest`: REAPI batch-read is already
//!   what the worker-side fetch path uses (`running_actions_manager.rs`
//!   `batch_read_small_blobs`), so no wire commitment is added and the
//!   worker's existing handler bytes-for-bytes serve the response. A
//!   custom proto would add a wire schema + a second handler with no
//!   functional gain.
//! - **Threshold**: `SMALL_BLOB_THRESHOLD = 16 KiB` from
//!   `small_blob_dispatcher`. Above the threshold the proxy continues
//!   using ByteStream `Read` (which paginates / supports offset+length).
//!
//! # Concurrency
//!
//! - `parking_lot::Mutex` for the queue map; lock held only across
//!   HashMap entry resolution + queue insertion. No `.await` under
//!   lock (CLAUDE.md "never hold locks across `.await`").
//! - The drainer task holds NO lock during `batch_read_blobs` — it
//!   moves the `BatchFn` clone (an `Arc`) into the future call.
//! - Reply oneshots are `tokio::sync::oneshot` so the per-caller wait
//!   is structured + bounded by the caller's own
//!   `tokio::time::timeout`.
//!
//! # Eligibility (caller-enforced)
//!
//! Only blobs that match ALL of the following are passed to the
//! coalescer:
//! - `offset == 0`
//! - `length == None || length >= digest.size_bytes()`
//! - `digest.size_bytes() <= SMALL_BLOB_THRESHOLD as u64`
//!
//! Anything else continues using ByteStream `Read`. `BatchReadBlobs` has
//! no offset/length semantics — partial reads and oversized blobs are
//! not eligible.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err};
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};

use crate::small_blob_dispatcher::SMALL_BLOB_THRESHOLD;

/// Default coalesce-window bytes cap. The REAPI spec recommends servers
/// support at least 4 MiB total; we cap at 4 MiB so a single batched
/// request reliably fits even when the caller's blobs all sit at the
/// 16 KiB threshold (4 MiB / 16 KiB = 256 blobs per batch).
pub const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Default per-endpoint queue capacity. Sized so that a fanout of one
/// worker's worth of small-blob reads (Bazel input-fetch can issue
/// hundreds of small reads in flight) does not block on `try_send` in
/// the steady-state. On overflow callers fall back to the per-blob
/// ByteStream Read path — no requests are dropped.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Suggested timeout to wrap around `submit` from caller code. The
/// coalescer itself does NOT impose a timeout; the caller's deadline
/// is authoritative. 30 s matches the existing per-operation
/// `stall_detector` cap.
pub const SUGGESTED_CALLER_TIMEOUT: Duration = Duration::from_secs(30);

/// Type-erased function the coalescer calls to actually issue the
/// `BatchReadBlobs` RPC against a worker. The closure owns whatever it
/// needs (e.g. an `Arc<GrpcStore>`); the coalescer only sees this
/// boxed `Fn`.
///
/// Returns a `HashMap` of per-digest results: a digest absent from the
/// map (or present with `Err`) is a per-digest failure for that batch.
/// The coalescer surfaces such per-digest results to each waiting
/// caller individually — one bad digest does not poison the others.
pub type BatchFn = Arc<
    dyn Fn(
            Arc<str>,
            Vec<DigestInfo>,
        ) -> futures::future::BoxFuture<
            'static,
            HashMap<DigestInfo, Result<Bytes, Error>>,
        > + Send
        + Sync,
>;

/// One pending request: digest + the oneshot reply slot.
struct PendingRead {
    digest: DigestInfo,
    /// Oneshot used to deliver the result to the caller. The drainer
    /// fans out per-digest results to all dedup'd siblings.
    reply: oneshot::Sender<Result<Bytes, Error>>,
}

/// Per-endpoint queue state. The drainer task holds the `Receiver`;
/// callers send via the cloned `Sender`.
struct PerEndpointState {
    sender: mpsc::Sender<PendingRead>,
}

/// Counters shared between `BatchReadCoalescer` and its per-endpoint
/// drainer tasks. Both fields are `Arc<AtomicU64>` so the drainer can
/// bump them without holding back a reference to the coalescer (which
/// would otherwise create an `Arc` cycle).
#[derive(Clone, Default)]
struct DispatcherCounters {
    batches_dispatched: Arc<AtomicU64>,
    requests_admitted: Arc<AtomicU64>,
    requests_deduped: Arc<AtomicU64>,
}

/// Public coalescer handle. One instance per `WorkerProxyStore`.
///
/// Lifetime: as long as the parent `WorkerProxyStore` lives. Drainer
/// tasks for individual endpoints exit when their `Sender`s are
/// dropped (which happens when the `BatchReadCoalescer` itself drops).
pub struct BatchReadCoalescer {
    /// Per-endpoint mpsc queues + drainer-task state. The drainer task
    /// is spawned on first use.
    queues: Mutex<HashMap<Arc<str>, PerEndpointState>>,
    /// Per-endpoint queue capacity.
    queue_capacity: usize,
    /// Cap on the cumulative `digest.size_bytes()` per outgoing batch.
    /// REAPI spec: servers SHOULD support at least 4 MiB.
    max_batch_bytes: usize,
    /// The actual batch-read RPC hook. In production this wraps
    /// `GrpcStore::batch_read_blobs`; in tests it is a fake that
    /// records the call shape.
    batch_fn: BatchFn,
    /// Shared counters; drainers clone this struct (cheap Arc bumps)
    /// so they can bump counters without holding back a reference to
    /// the coalescer.
    counters: DispatcherCounters,
}

impl core::fmt::Debug for BatchReadCoalescer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BatchReadCoalescer")
            .field("queue_capacity", &self.queue_capacity)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("batches_dispatched", &self.batches_dispatched())
            .field("requests_admitted", &self.requests_admitted())
            .field("requests_deduped", &self.requests_deduped())
            .finish()
    }
}

impl BatchReadCoalescer {
    /// Construct a coalescer that issues `BatchReadBlobs` via the given
    /// callable.
    pub fn new(batch_fn: BatchFn) -> Arc<Self> {
        Self::with_config(batch_fn, DEFAULT_QUEUE_CAPACITY, DEFAULT_MAX_BATCH_BYTES)
    }

    /// Construct with explicit per-endpoint capacity + per-batch byte cap.
    pub fn with_config(
        batch_fn: BatchFn,
        queue_capacity: usize,
        max_batch_bytes: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            queues: Mutex::new(HashMap::new()),
            queue_capacity: queue_capacity.max(1),
            max_batch_bytes: max_batch_bytes.max(SMALL_BLOB_THRESHOLD),
            batch_fn,
            counters: DispatcherCounters::default(),
        })
    }

    /// Total batches dispatched across all endpoints. Used by tests to
    /// assert N concurrent requests produced 1 batch.
    pub fn batches_dispatched(&self) -> u64 {
        self.counters.batches_dispatched.load(Ordering::Relaxed)
    }

    /// Total caller requests admitted (per-call, post-eligibility).
    pub fn requests_admitted(&self) -> u64 {
        self.counters.requests_admitted.load(Ordering::Relaxed)
    }

    /// Total caller requests that joined an in-flight batch slot via
    /// dedup (rather than allocating a fresh slot).
    pub fn requests_deduped(&self) -> u64 {
        self.counters.requests_deduped.load(Ordering::Relaxed)
    }

    /// Returns `true` if the digest is eligible for batched reads given
    /// the caller's offset / length. Above-threshold blobs and any
    /// non-trivial offset/length take the existing ByteStream Read path.
    ///
    /// Eligibility:
    /// - `offset == 0`,
    /// - `length` is `None` OR `length >= digest.size_bytes()` (a full
    ///   read disguised behind a permissive `read_limit`),
    /// - `digest.size_bytes() <= SMALL_BLOB_THRESHOLD as u64`.
    pub fn is_eligible(digest: DigestInfo, offset: u64, length: Option<u64>) -> bool {
        if offset != 0 {
            return false;
        }
        let size = digest.size_bytes();
        if size > SMALL_BLOB_THRESHOLD as u64 {
            return false;
        }
        match length {
            None => true,
            Some(l) => l >= size,
        }
    }

    /// Submit a read request for `digest` against `endpoint`. Returns
    /// the bytes for the blob (the WHOLE blob, since BatchReadBlobs
    /// returns whole blobs). The caller is responsible for forwarding
    /// the bytes to its own writer + cache (the coalescer does NOT
    /// touch any writer — the writer-termination contract stays with
    /// the caller).
    ///
    /// # Eligibility
    ///
    /// Caller MUST gate on `is_eligible(digest, offset, length)` first.
    /// The coalescer assumes whole-blob reads only.
    pub async fn submit(
        &self,
        endpoint: Arc<str>,
        digest: DigestInfo,
    ) -> Result<Bytes, Error> {
        let (tx, rx) = oneshot::channel();
        let item = PendingRead { digest, reply: tx };

        // Acquire-or-spawn the per-endpoint queue. Lock held only across
        // HashMap entry resolution + tx clone; no `.await` under lock.
        let sender = {
            let mut queues = self.queues.lock();
            if let Some(state) = queues.get(&endpoint) {
                state.sender.clone()
            } else {
                let (sender, receiver) =
                    mpsc::channel::<PendingRead>(self.queue_capacity);
                queues.insert(
                    endpoint.clone(),
                    PerEndpointState {
                        sender: sender.clone(),
                    },
                );
                drop(queues);
                tokio::spawn(drainer_task(
                    endpoint.clone(),
                    receiver,
                    self.batch_fn.clone(),
                    self.max_batch_bytes,
                    self.counters.clone(),
                ));
                trace!(%endpoint, "BatchReadCoalescer: spawned drainer for endpoint");
                sender
            }
        };

        if let Err(send_err) = sender.try_send(item) {
            // Queue overflow OR channel closed (drainer exited). Surface
            // a structured error; the caller's path will fall back to
            // per-blob ByteStream Read.
            return Err(make_err!(
                Code::ResourceExhausted,
                "BatchReadCoalescer: per-endpoint queue full or closed: {send_err:?} \
                 (endpoint={endpoint}, digest={digest}); caller must fall back to ByteStream Read"
            ));
        }

        self.counters
            .requests_admitted
            .fetch_add(1, Ordering::Relaxed);

        // Wait for the drainer to deliver our slot's result. The caller
        // is responsible for wrapping `submit` in its own
        // `tokio::time::timeout` if they want a deadline.
        match rx.await {
            Ok(result) => result,
            Err(_canceled) => Err(make_err!(
                Code::Internal,
                "BatchReadCoalescer: reply oneshot was dropped before delivery \
                 (drainer exited mid-batch?); endpoint={endpoint}, digest={digest}"
            )),
        }
    }
}

/// Per-endpoint drainer task. Runs until its `Receiver` is closed
/// (which happens when the parent `BatchReadCoalescer` is dropped, or
/// the per-endpoint queue is explicitly purged).
///
/// Per-iteration:
/// 1. `recv().await` for the first item.
/// 2. `tokio::task::yield_now()` once so concurrently-ready producers
///    can land their items without us imposing an artificial sleep.
/// 3. `try_recv()` until empty OR cumulative `digest.size_bytes()`
///    exceeds `max_batch_bytes`. Same-digest requests share one slot
///    via per-batch dedup map.
/// 4. Issue `batch_fn(endpoint, digests)`; deliver per-digest results
///    to all waiting oneshots (including dedup'd siblings).
async fn drainer_task(
    endpoint: Arc<str>,
    mut rx: mpsc::Receiver<PendingRead>,
    batch_fn: BatchFn,
    max_batch_bytes: usize,
    counters: DispatcherCounters,
) {
    debug!(%endpoint, "BatchReadCoalescer drainer: start");
    while let Some(first) = rx.recv().await {
        // Per-batch dedup: digest -> Vec<oneshot::Sender>. The first
        // request for a digest goes into `digests_in_order`; subsequent
        // same-digest siblings only attach a reply slot.
        let mut waiters: HashMap<DigestInfo, Vec<oneshot::Sender<Result<Bytes, Error>>>> =
            HashMap::new();
        let mut digests_in_order: Vec<DigestInfo> = Vec::new();
        let mut total_bytes: u64 = 0;

        admit_request(
            first,
            &mut waiters,
            &mut digests_in_order,
            &mut total_bytes,
            &counters,
        );

        // Yield once to let any concurrently-ready producers enqueue.
        // No artificial sleep — one runtime yield is the entire
        // coalesce window (CLAUDE.md "no thread::sleep/tokio::time::sleep
        // as synchronization"; the SmallBlobDispatcher + grpc_store
        // batch_flush_loop both use this same shape).
        tokio::task::yield_now().await;

        // Drain everything currently queued (non-blocking).
        loop {
            match rx.try_recv() {
                Ok(req) => {
                    // Stop accepting NEW digests once the byte cap would
                    // be exceeded — but still admit same-digest siblings
                    // (they cost zero outgoing bytes).
                    let already_admitted = waiters.contains_key(&req.digest);
                    let new_total = if already_admitted {
                        total_bytes
                    } else {
                        total_bytes.saturating_add(req.digest.size_bytes())
                    };
                    if !already_admitted
                        && new_total > max_batch_bytes as u64
                        && !digests_in_order.is_empty()
                    {
                        // Tell this caller to fall back. The reply is a
                        // structured error so the caller's
                        // ByteStream-Read fallback path can fire.
                        let size = req.digest.size_bytes();
                        // Caller may already have given up + dropped the
                        // receiver — a failed send is a no-op.
                        drop(req.reply.send(Err(make_err!(
                            Code::ResourceExhausted,
                            "BatchReadCoalescer: batch byte cap reached \
                             ({total_bytes} + {size} > {max_batch_bytes}); \
                             falling back to per-blob path"
                        ))));
                        continue;
                    }
                    admit_request(
                        req,
                        &mut waiters,
                        &mut digests_in_order,
                        &mut total_bytes,
                        &counters,
                    );
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let blob_count = digests_in_order.len();
        let batch_started = std::time::Instant::now();
        trace!(
            %endpoint,
            blob_count,
            total_bytes,
            "BatchReadCoalescer drainer: dispatching batch"
        );

        let results = (batch_fn)(endpoint.clone(), digests_in_order.clone()).await;
        counters
            .batches_dispatched
            .fetch_add(1, Ordering::Relaxed);

        // Distribute per-digest results to every waiter (including
        // dedup'd siblings). A digest absent from `results` is treated
        // as `Code::Internal` per-digest (the RPC succeeded but the
        // response failed to include it — a server bug; surface as a
        // distinct error so callers can fall back).
        let elapsed_ms = batch_started.elapsed().as_millis() as u64;
        for digest in &digests_in_order {
            let result = results.get(digest).cloned().unwrap_or_else(|| {
                Err(make_err!(
                    Code::Internal,
                    "BatchReadCoalescer: server returned no entry for {digest} \
                     in batch of {blob_count} on endpoint {endpoint}"
                ))
            });
            if let Some(slots) = waiters.remove(digest) {
                for slot in slots {
                    // Caller may have dropped its receiver (timeout /
                    // cancellation); a failed send is a no-op.
                    drop(slot.send(result.clone()));
                }
            }
        }

        debug!(
            %endpoint,
            blob_count,
            total_bytes,
            elapsed_ms,
            "BatchReadCoalescer drainer: batch complete"
        );
    }
    debug!(%endpoint, "BatchReadCoalescer drainer: queue closed; exiting");
}

/// Admit a request into the per-batch dedup map. First request for a
/// digest allocates a fresh slot in `digests_in_order` + bumps the
/// cumulative byte counter; subsequent same-digest siblings only
/// attach a reply slot and bump `requests_deduped`.
fn admit_request(
    req: PendingRead,
    waiters: &mut HashMap<DigestInfo, Vec<oneshot::Sender<Result<Bytes, Error>>>>,
    digests_in_order: &mut Vec<DigestInfo>,
    total_bytes: &mut u64,
    counters: &DispatcherCounters,
) {
    let digest = req.digest;
    let is_first = !waiters.contains_key(&digest);
    if is_first {
        digests_in_order.push(digest);
        *total_bytes = total_bytes.saturating_add(digest.size_bytes());
    } else {
        counters
            .requests_deduped
            .fetch_add(1, Ordering::Relaxed);
    }
    waiters.entry(digest).or_default().push(req.reply);
}

// ----------------------------------------------------------------------
// Unit tests
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use core::time::Duration;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::FutureExt;
    use nativelink_error::Code;
    use nativelink_util::common::DigestInfo;
    use parking_lot::Mutex as PLMutex;

    use super::*;

    const DEADLOCK: Duration = Duration::from_secs(5);

    fn d(seed: u8, size: u64) -> DigestInfo {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        DigestInfo::new(bytes, size)
    }

    #[derive(Clone, Default)]
    struct CallRecord {
        endpoint: Arc<str>,
        digests: Vec<DigestInfo>,
    }

    /// A test fake that records each invocation and returns
    /// caller-supplied per-digest bytes/errors.
    fn make_fake(
        per_digest: Arc<PLMutex<HashMap<DigestInfo, Result<Bytes, Error>>>>,
        recorded: Arc<PLMutex<Vec<CallRecord>>>,
    ) -> BatchFn {
        Arc::new(move |endpoint: Arc<str>, digests: Vec<DigestInfo>| {
            let per_digest = per_digest.clone();
            let recorded = recorded.clone();
            async move {
                recorded.lock().push(CallRecord {
                    endpoint: endpoint.clone(),
                    digests: digests.clone(),
                });
                let table = per_digest.lock();
                let mut out: HashMap<DigestInfo, Result<Bytes, Error>> = HashMap::new();
                for digest in digests {
                    let value = table.get(&digest).cloned().unwrap_or_else(|| {
                        Err(make_err!(
                            Code::NotFound,
                            "fake batch_fn: no per-digest entry for {digest}"
                        ))
                    });
                    out.insert(digest, value);
                }
                out
            }
            .boxed()
        })
    }

    #[tokio::test]
    async fn is_eligible_rejects_above_threshold() {
        let big = d(1, SMALL_BLOB_THRESHOLD as u64 + 1);
        assert!(!BatchReadCoalescer::is_eligible(big, 0, None));
    }

    #[tokio::test]
    async fn is_eligible_rejects_nonzero_offset() {
        let small = d(1, 100);
        assert!(!BatchReadCoalescer::is_eligible(small, 1, None));
    }

    #[tokio::test]
    async fn is_eligible_accepts_full_read_with_permissive_length() {
        let small = d(1, 100);
        assert!(BatchReadCoalescer::is_eligible(small, 0, None));
        assert!(BatchReadCoalescer::is_eligible(small, 0, Some(100)));
        // A length larger than the blob is still a full read (REAPI
        // ByteStream `read_limit` is permissive).
        assert!(BatchReadCoalescer::is_eligible(small, 0, Some(1_000_000)));
        // A length smaller than the blob is a partial read — not
        // eligible (BatchReadBlobs has no length semantics).
        assert!(!BatchReadCoalescer::is_eligible(small, 0, Some(50)));
    }

    /// Five concurrent small-blob requests against the same endpoint
    /// MUST coalesce into ONE outgoing batch.
    #[tokio::test]
    async fn five_concurrent_requests_become_one_batch() {
        let per_digest = Arc::new(PLMutex::new(HashMap::new()));
        let recorded = Arc::new(PLMutex::new(Vec::new()));
        for i in 1..=5u8 {
            per_digest
                .lock()
                .insert(d(i, 100), Ok(Bytes::from(vec![i; 100])));
        }
        let coalescer = BatchReadCoalescer::new(make_fake(per_digest, recorded.clone()));

        let endpoint: Arc<str> = Arc::from("grpc://peer:50071");
        let mut handles = Vec::new();
        for i in 1..=5u8 {
            let c = coalescer.clone();
            let ep = endpoint.clone();
            handles.push(tokio::spawn(async move {
                tokio::time::timeout(DEADLOCK, c.submit(ep, d(i, 100)))
                    .await
                    .expect(
                        "must not deadlock — coalescer drainer must deliver per-digest \
                         result within DEADLOCK",
                    )
            }));
        }
        let results = futures::future::join_all(handles).await;
        for (i, r) in results.into_iter().enumerate() {
            let bytes =
                r.unwrap().expect("per-digest result must be Ok for fake-supplied digest");
            assert_eq!(bytes.as_ref(), vec![(i + 1) as u8; 100].as_slice());
        }

        let calls = recorded.lock();
        assert_eq!(
            calls.len(),
            1,
            "5 concurrent same-target requests MUST produce 1 batched RPC; got {}",
            calls.len()
        );
        let call = &calls[0];
        assert_eq!(call.endpoint.as_ref(), "grpc://peer:50071");
        assert_eq!(
            call.digests.len(),
            5,
            "the single batched RPC MUST carry all 5 digests"
        );

        assert_eq!(coalescer.batches_dispatched(), 1);
        assert_eq!(coalescer.requests_admitted(), 5);
        assert_eq!(
            coalescer.requests_deduped(),
            0,
            "all 5 digests are distinct; no dedup expected"
        );
    }

    /// Three concurrent requests for the SAME digest MUST result in
    /// one slot in the batch + all three callers receiving the bytes.
    #[tokio::test]
    async fn same_digest_concurrent_requests_dedup() {
        let per_digest = Arc::new(PLMutex::new(HashMap::new()));
        let recorded = Arc::new(PLMutex::new(Vec::new()));
        per_digest
            .lock()
            .insert(d(7, 200), Ok(Bytes::from(vec![7u8; 200])));
        let coalescer = BatchReadCoalescer::new(make_fake(per_digest, recorded.clone()));

        let endpoint: Arc<str> = Arc::from("grpc://peer:50071");
        let mut handles = Vec::new();
        for _ in 0..3 {
            let c = coalescer.clone();
            let ep = endpoint.clone();
            handles.push(tokio::spawn(async move {
                tokio::time::timeout(DEADLOCK, c.submit(ep, d(7, 200)))
                    .await
                    .expect(
                        "must not deadlock — same-digest dedup must deliver to all waiters",
                    )
            }));
        }
        let results = futures::future::join_all(handles).await;
        for r in results {
            let bytes = r.unwrap().expect("dedup'd request must still get Ok bytes");
            assert_eq!(bytes.as_ref(), vec![7u8; 200].as_slice());
        }

        let calls = recorded.lock();
        assert_eq!(calls.len(), 1, "1 batch expected");
        assert_eq!(
            calls[0].digests.len(),
            1,
            "the batch MUST carry the digest exactly once (single-flight dedup)"
        );

        assert_eq!(coalescer.batches_dispatched(), 1);
        assert_eq!(coalescer.requests_admitted(), 3);
        assert_eq!(
            coalescer.requests_deduped(),
            2,
            "with 3 concurrent same-digest requests, 2 are dedup'd onto the first slot"
        );
    }

    /// Partial-failure response: 3 OK + 2 NotFound. Each caller MUST
    /// receive its own per-digest result; one bad digest does NOT
    /// poison the batch.
    #[tokio::test]
    async fn partial_failure_response_surfaces_per_digest_errors() {
        let per_digest = Arc::new(PLMutex::new(HashMap::new()));
        let recorded = Arc::new(PLMutex::new(Vec::new()));
        for i in 1..=3u8 {
            per_digest
                .lock()
                .insert(d(i, 100), Ok(Bytes::from(vec![i; 100])));
        }
        for i in 4..=5u8 {
            per_digest.lock().insert(
                d(i, 100),
                Err(make_err!(Code::NotFound, "synthetic not-found for digest {i}")),
            );
        }
        let coalescer = BatchReadCoalescer::new(make_fake(per_digest, recorded.clone()));

        let endpoint: Arc<str> = Arc::from("grpc://peer:50071");
        let mut handles = Vec::new();
        for i in 1..=5u8 {
            let c = coalescer.clone();
            let ep = endpoint.clone();
            handles.push(tokio::spawn(async move {
                let res = tokio::time::timeout(DEADLOCK, c.submit(ep, d(i, 100)))
                    .await
                    .expect("must not deadlock");
                (i, res)
            }));
        }
        let results = futures::future::join_all(handles).await;
        for r in results {
            let (i, res) = r.unwrap();
            if i <= 3 {
                let bytes = res.expect("OK digests must surface bytes");
                assert_eq!(bytes.as_ref(), vec![i; 100].as_slice());
            } else {
                let err = res.expect_err("NotFound digests MUST surface as Err");
                assert_eq!(
                    err.code,
                    Code::NotFound,
                    "per-digest NotFound MUST be preserved through batch dispatch"
                );
            }
        }

        let calls = recorded.lock();
        assert_eq!(calls.len(), 1, "single batch expected");
    }

    /// Above-threshold blobs MUST NOT be admitted by the eligibility
    /// helper. The caller is the gate; this lets the proxy fall back
    /// to ByteStream Read for large reads.
    #[tokio::test]
    async fn above_threshold_blob_is_not_eligible_for_batch() {
        let too_big = d(1, SMALL_BLOB_THRESHOLD as u64 + 1);
        assert!(
            !BatchReadCoalescer::is_eligible(too_big, 0, None),
            "blob > SMALL_BLOB_THRESHOLD MUST NOT be eligible — caller must use ByteStream Read"
        );
    }
}
