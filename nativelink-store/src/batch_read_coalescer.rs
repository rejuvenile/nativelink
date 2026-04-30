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
use nativelink_util::digest_hasher::{DigestHasherFunc, default_digest_hasher_func};
use nativelink_util::store_trait::IS_WORKER_REQUEST;
use opentelemetry::context::{Context, FutureExt as _};
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
///
/// The `DigestHasherFunc` argument is the hasher to embed in the
/// outgoing `BatchReadBlobsRequest.digest_function`; the drainer
/// captures it from the FIRST `submit()` call in a batch (callers are
/// expected to share a hasher per peer-fetch fanout — Bazel switches
/// hashers only at session boundaries).
pub type BatchFn = Arc<
    dyn Fn(
            Arc<str>,
            Vec<DigestInfo>,
            DigestHasherFunc,
        ) -> futures::future::BoxFuture<
            'static,
            HashMap<DigestInfo, Result<Bytes, Error>>,
        > + Send
        + Sync,
>;

/// One pending request: digest + the oneshot reply slot + the
/// task-local context to re-establish around the batched RPC.
///
/// Why captured per-request: `tokio::spawn` does NOT inherit either
/// `tokio::task_local!` storage (e.g. `IS_WORKER_REQUEST`) NOR the
/// OpenTelemetry `Context` (which carries `DigestHasherFunc`). The
/// drainer is `tokio::spawn`'d on first use, so it sees neither unless
/// each submission ferries them across the spawn boundary.
///
/// The drainer uses the FIRST item in each batch as the "batch
/// context": `IS_WORKER_REQUEST` (always `true` from the proxy) is
/// scoped around the BatchFn invocation so the receiving worker's
/// `batch_read_blobs` sets the `x-nativelink-worker` header (loop
/// terminator at depth 1). The `DigestHasherFunc` is forwarded to the
/// BatchFn so the outgoing `BatchReadBlobsRequest.digest_function`
/// matches the caller's hasher (Blake3 or SHA-256) — without this, a
/// non-SHA-256 caller silently degrades to SHA-256 + per-digest
/// `InvalidArgument` from the worker side.
struct PendingRead {
    digest: DigestInfo,
    /// Oneshot used to deliver the result to the caller. The drainer
    /// fans out per-digest results to all dedup'd siblings.
    reply: oneshot::Sender<Result<Bytes, Error>>,
    /// Captured `IS_WORKER_REQUEST` flag at submit-time. The drainer
    /// uses the FIRST request's value for the batch's
    /// `IS_WORKER_REQUEST.scope(...)` wrapper. Defaults to `false` if
    /// the caller wasn't in any scope.
    is_worker_request: bool,
    /// Captured `DigestHasherFunc` at submit-time (read from the
    /// OpenTelemetry `Context`). Defaults to
    /// `default_digest_hasher_func()` if no hasher was set.
    digest_function: DigestHasherFunc,
    /// Captured OpenTelemetry `Context` at submit-time. Used to
    /// re-establish context around the batched RPC inside the
    /// drainer — without this, downstream tracing spans + any other
    /// `Context::current()` reads see an empty context.
    otel_context: Context,
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
    /// `endpoint` is borrowed; the per-endpoint queue (keyed by
    /// `Arc<str>`) is allocated only on the first submission per
    /// endpoint. Steady-state submissions reuse the existing key with
    /// no allocation.
    ///
    /// # Task-local capture
    ///
    /// `tokio::spawn` does NOT inherit `tokio::task_local!` storage
    /// (`IS_WORKER_REQUEST`) or the OpenTelemetry `Context`
    /// (`DigestHasherFunc`). `submit` captures both at call-time and
    /// passes them via `PendingRead` so the drainer can re-establish
    /// them around the BatchFn — preserving the "workers NEVER chain
    /// externally" loop-terminator invariant and the caller's hasher
    /// choice (Blake3 / SHA-256).
    ///
    /// # Eligibility
    ///
    /// Caller MUST gate on `is_eligible(digest, offset, length)` first.
    /// The coalescer assumes whole-blob reads only.
    pub async fn submit(
        &self,
        endpoint: &str,
        digest: DigestInfo,
    ) -> Result<Bytes, Error> {
        let (tx, rx) = oneshot::channel();
        // Capture task-local + OpenTelemetry context BEFORE the
        // `tokio::spawn` boundary inside the queue-spawn branch (and
        // for symmetry on the queue-already-exists branch — the values
        // ride with the request to the drainer regardless).
        let is_worker_request =
            IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);
        let otel_context = Context::current();
        let digest_function = otel_context
            .get::<DigestHasherFunc>()
            .copied()
            .unwrap_or_else(default_digest_hasher_func);
        let item = PendingRead {
            digest,
            reply: tx,
            is_worker_request,
            digest_function,
            otel_context: otel_context.clone(),
        };

        // Acquire-or-spawn the per-endpoint queue. Lock held only across
        // HashMap entry resolution + tx clone; no `.await` under lock.
        // The endpoint `Arc<str>` is allocated ONLY on the insert-miss
        // path (first submission per endpoint); steady-state hits reuse
        // the cached key.
        let (sender, endpoint_for_err) = {
            let mut queues = self.queues.lock();
            if let Some((key, state)) = queues.get_key_value(endpoint) {
                (state.sender.clone(), key.clone())
            } else {
                let key: Arc<str> = Arc::from(endpoint);
                let (sender, receiver) =
                    mpsc::channel::<PendingRead>(self.queue_capacity);
                queues.insert(
                    key.clone(),
                    PerEndpointState {
                        sender: sender.clone(),
                    },
                );
                drop(queues);
                tokio::spawn(drainer_task(
                    key.clone(),
                    receiver,
                    self.batch_fn.clone(),
                    self.max_batch_bytes,
                    self.counters.clone(),
                ));
                trace!(%key, "BatchReadCoalescer: spawned drainer for endpoint");
                (sender, key)
            }
        };

        if let Err(send_err) = sender.try_send(item) {
            // Queue overflow OR channel closed (drainer exited). Surface
            // a structured error; the caller's path will fall back to
            // per-blob ByteStream Read.
            return Err(make_err!(
                Code::ResourceExhausted,
                "BatchReadCoalescer: per-endpoint queue full or closed: {send_err:?} \
                 (endpoint={endpoint_for_err}, digest={digest}); \
                 caller must fall back to ByteStream Read"
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
                 (drainer exited mid-batch?); endpoint={endpoint_for_err}, digest={digest}"
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

        // Capture the FIRST request's task-local + OTel context as the
        // batch context. `tokio::spawn` did NOT inherit these into this
        // drainer task — `submit()` ferried them across via PendingRead.
        // The wrapping `IS_WORKER_REQUEST.scope(...)` ensures the
        // BatchFn's downstream `batch_read_blobs` sees the correct flag
        // (loop-terminator invariant: peer worker enters responder mode
        // and refuses to chain externally). The captured
        // `digest_function` flows directly to the BatchFn for the
        // outgoing `BatchReadBlobsRequest.digest_function` field.
        let batch_is_worker_request = first.is_worker_request;
        let batch_digest_function = first.digest_function;
        let batch_otel_context = first.otel_context.clone();

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

        // Re-establish the captured task-local (`IS_WORKER_REQUEST`) and
        // OpenTelemetry context (`DigestHasherFunc`) around the BatchFn.
        // Without these wrappers the drainer sees an empty context (it
        // is `tokio::spawn`'d) and any downstream `IS_WORKER_REQUEST.try_with`
        // / `Context::current().get::<DigestHasherFunc>()` would silently
        // default — breaking the loop-terminator invariant and silently
        // degrading non-SHA-256 callers to SHA-256 + InvalidArgument.
        let batch_fut = (batch_fn)(
            endpoint.clone(),
            digests_in_order.clone(),
            batch_digest_function,
        );
        let scoped = IS_WORKER_REQUEST.scope(batch_is_worker_request, batch_fut);
        let results = scoped.with_context(batch_otel_context).await;
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
    use nativelink_macro::nativelink_test;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::store_trait::IS_WORKER_REQUEST;
    use opentelemetry::Context;
    use opentelemetry::context::FutureExt as _OtelFutureExt;
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
        digest_function: Option<DigestHasherFunc>,
        is_worker_request: bool,
    }

    /// A test fake that records each invocation and returns
    /// caller-supplied per-digest bytes/errors. The fake observes
    /// `IS_WORKER_REQUEST` (via `try_with`) at BatchFn invocation time
    /// — this is the same point `grpc_store::batch_read_blobs` reads
    /// the flag for the `x-nativelink-worker` header.
    fn make_fake(
        per_digest: Arc<PLMutex<HashMap<DigestInfo, Result<Bytes, Error>>>>,
        recorded: Arc<PLMutex<Vec<CallRecord>>>,
    ) -> BatchFn {
        Arc::new(
            move |endpoint: Arc<str>,
                  digests: Vec<DigestInfo>,
                  digest_function: DigestHasherFunc| {
                let per_digest = per_digest.clone();
                let recorded = recorded.clone();
                async move {
                    let is_worker_request =
                        IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);
                    recorded.lock().push(CallRecord {
                        endpoint: endpoint.clone(),
                        digests: digests.clone(),
                        digest_function: Some(digest_function),
                        is_worker_request,
                    });
                    let table = per_digest.lock();
                    let mut out: HashMap<DigestInfo, Result<Bytes, Error>> =
                        HashMap::new();
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
            },
        )
    }

    #[nativelink_test]
    async fn is_eligible_rejects_above_threshold() {
        let big = d(1, SMALL_BLOB_THRESHOLD as u64 + 1);
        assert!(!BatchReadCoalescer::is_eligible(big, 0, None));
    }

    #[nativelink_test]
    async fn is_eligible_rejects_nonzero_offset() {
        let small = d(1, 100);
        assert!(!BatchReadCoalescer::is_eligible(small, 1, None));
    }

    #[nativelink_test]
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
    #[nativelink_test]
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
                tokio::time::timeout(DEADLOCK, c.submit(&ep, d(i, 100)))
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
    #[nativelink_test]
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
                tokio::time::timeout(DEADLOCK, c.submit(&ep, d(7, 200)))
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
    #[nativelink_test]
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
                let res = tokio::time::timeout(DEADLOCK, c.submit(&ep, d(i, 100)))
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
    #[nativelink_test]
    async fn above_threshold_blob_is_not_eligible_for_batch() {
        let too_big = d(1, SMALL_BLOB_THRESHOLD as u64 + 1);
        assert!(
            !BatchReadCoalescer::is_eligible(too_big, 0, None),
            "blob > SMALL_BLOB_THRESHOLD MUST NOT be eligible — caller must use ByteStream Read"
        );
    }

    // ----------------------------------------------------------------------
    // B1 regression: IS_WORKER_REQUEST captured at submit-time MUST be
    // re-established around the BatchFn inside the spawned drainer.
    // Without this fix, the loop-terminator invariant ("workers NEVER
    // chain externally") silently breaks because `tokio::spawn` does
    // NOT inherit `tokio::task_local!` storage.
    //
    // Mutation: comment out the `IS_WORKER_REQUEST.scope(...)` wrapper
    // around `batch_fut` in `drainer_task` and verify this test fails
    // with the specific message.
    // ----------------------------------------------------------------------
    #[nativelink_test]
    async fn drainer_propagates_is_worker_request_across_spawn() {
        let per_digest = Arc::new(PLMutex::new(HashMap::new()));
        let recorded = Arc::new(PLMutex::new(Vec::new()));
        per_digest
            .lock()
            .insert(d(11, 50), Ok(Bytes::from(vec![11u8; 50])));
        let coalescer = BatchReadCoalescer::new(make_fake(per_digest, recorded.clone()));

        let endpoint: Arc<str> = Arc::from("grpc://peer-bw:50071");
        let ep = endpoint.clone();
        let c = coalescer.clone();
        // Submit FROM WITHIN an `IS_WORKER_REQUEST.scope(true, ...)`,
        // which is exactly the production composition (see
        // `worker_proxy_store::get_part_and_cache:1370`).
        let result = IS_WORKER_REQUEST
            .scope(true, async move {
                tokio::time::timeout(DEADLOCK, c.submit(&ep, d(11, 50)))
                    .await
                    .expect("must not deadlock — drainer must deliver bytes")
            })
            .await;
        assert!(result.is_ok(), "submission must succeed");

        let calls = recorded.lock();
        assert_eq!(calls.len(), 1);
        assert!(
            calls[0].is_worker_request,
            "B1 regression — IS_WORKER_REQUEST must propagate across the \
             tokio::spawn'd drainer; without the scope wrapper around BatchFn, \
             try_with returns Err and the receiving worker would not enter \
             responder mode (loop-terminator invariant: workers NEVER chain \
             externally)"
        );
    }

    // ----------------------------------------------------------------------
    // B2 regression: the captured `DigestHasherFunc` from the FIRST
    // request in a batch MUST flow into the BatchFn so the outgoing
    // BatchReadBlobsRequest.digest_function field matches the caller's
    // hasher (Blake3 or SHA-256). Without this fix, the drainer
    // silently defaults to SHA-256 (Context::current() returns empty
    // across spawn) and Blake3 callers get InvalidArgument from the
    // worker side for every batch — degrading 100% of non-SHA-256
    // deployments.
    //
    // Mutation: replace `batch_digest_function` with
    // `default_digest_hasher_func()` at the BatchFn call site and
    // verify this test fails with the specific message.
    // ----------------------------------------------------------------------
    #[nativelink_test]
    async fn drainer_propagates_digest_hasher_across_spawn() {
        let per_digest = Arc::new(PLMutex::new(HashMap::new()));
        let recorded = Arc::new(PLMutex::new(Vec::new()));
        per_digest
            .lock()
            .insert(d(13, 50), Ok(Bytes::from(vec![13u8; 50])));
        let coalescer = BatchReadCoalescer::new(make_fake(per_digest, recorded.clone()));

        let endpoint: Arc<str> = Arc::from("grpc://peer-blake:50071");
        let ep = endpoint.clone();
        let c = coalescer.clone();
        // Submit with Blake3 set in the OpenTelemetry context. This is
        // the production pattern (the request handler installs the
        // hasher into the OTel Context before dispatching). We use
        // `with_context()` from `opentelemetry::context::FutureExt` so
        // the future polls inside the Blake3 context.
        let blake_ctx = Context::current().with_value(DigestHasherFunc::Blake3);
        let submit_result = tokio::time::timeout(
            DEADLOCK,
            async { c.submit(&ep, d(13, 50)).await }.with_context(blake_ctx),
        )
        .await
        .expect("must not deadlock");
        assert!(submit_result.is_ok(), "submission must succeed");

        let calls = recorded.lock();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].digest_function,
            Some(DigestHasherFunc::Blake3),
            "B2 regression — DigestHasherFunc captured at submit-time \
             must reach the BatchFn through the spawned drainer; without \
             this propagation, non-SHA-256 callers silently degrade to \
             SHA-256 and the worker rejects every batch with InvalidArgument"
        );
    }

    // ----------------------------------------------------------------------
    // M1 regression: `submit` takes `&str` and must NOT allocate an
    // `Arc<str>` on the steady-state hit path (only on first
    // submission per endpoint when the per-endpoint queue is created).
    //
    // We can't directly count allocations from a unit test, but we can
    // assert the API shape compiles with `&str` (compile-time guard
    // against silent regression to `Arc<str>`).
    // ----------------------------------------------------------------------
    #[nativelink_test]
    async fn submit_signature_takes_borrowed_str() {
        let per_digest = Arc::new(PLMutex::new(HashMap::new()));
        let recorded = Arc::new(PLMutex::new(Vec::new()));
        per_digest
            .lock()
            .insert(d(21, 10), Ok(Bytes::from(vec![21u8; 10])));
        per_digest
            .lock()
            .insert(d(22, 10), Ok(Bytes::from(vec![22u8; 10])));
        let coalescer = BatchReadCoalescer::new(make_fake(per_digest, recorded.clone()));

        let endpoint_str: &str = "grpc://hot:50071";
        // First submission: spawns the drainer and allocates the
        // per-endpoint Arc<str> key.
        let _ = tokio::time::timeout(DEADLOCK, coalescer.submit(endpoint_str, d(21, 10)))
            .await
            .expect("must not deadlock");
        // Steady-state hits: should reuse the existing key, no
        // additional Arc<str> allocations on the hot path.
        let _ = tokio::time::timeout(DEADLOCK, coalescer.submit(endpoint_str, d(22, 10)))
            .await
            .expect("must not deadlock");

        let calls = recorded.lock();
        assert_eq!(
            calls.len(),
            2,
            "two distinct submissions => two batches against the same endpoint key"
        );
    }
}
