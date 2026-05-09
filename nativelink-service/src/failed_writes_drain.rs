// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Server-side drain of FastSlowStore `failed_slow_writes` (#287:
//! server-side drain → UploadMissingBlobs).
//!
//! ## Why this module exists
//!
//! The server's cas_STORE FastSlowStore tracks digests whose background
//! slow-tier write failed in a `failed_slow_writes` set, populated by:
//!   - `failed_writes_inserter()` (chunked-commit Err arms)
//!   - the legacy `update`/`update_oneshot` Err arms
//!   - the `PinExpireFailedWritesListener` 120 s pin auto-expire
//!   - the streaming-write watchdog
//!
//! Pre-#287 the only consumer was the WORKER's own
//! `LocalWorker::on_reconnect` calling `cas_store.drain_failed_digests()`
//! — but that's the WORKER's FSS instance, not the SERVER's. The
//! server-side set was dead-letter: filled forever, never drained.
//!
//! Fix. Periodically drain the server's `failed_slow_writes` and
//! dispatch `UploadMissingBlobs` to a worker that has the digest (per
//! `BlobLocalityMap`).
//!
//! ## Drain semantics
//!
//! - Drain is destructive (`Vec::drain` on the inner HashSet).
//! - Digests we *can't* dispatch this tick (no connected worker, send
//!   error, throttled by per-digest cooldown) are re-inserted via
//!   `Store::reinsert_failed_digests` so the next tick can retry. The
//!   inflight cooldown prevents hot-looping the same digest.
//! - On successful dispatch the digest leaves the set; trust the
//!   worker to re-upload the bytes. If the upload fails, the slow-tier
//!   write Err arm re-inserts the digest naturally.
//!
//!   TODO(#289): keep the digest in `failed_slow_writes` until BIS ack
//!   arrives for that digest — that would make BIS the single durability
//!   oath (the same shape as the worker mirror's "bytes stay until BIS
//!   arrives"). Today we trust the worker's re-upload to fail the slow
//!   tier on a true durability bug. Concrete failure modes the
//!   trust-relay misses (red-team #287 review):
//!     * worker drops `UploadMissingBlobs` (channel full, disconnect
//!       between tx-Ok and worker-side processing);
//!     * worker no-ops via its own `ExistenceCacheStore` ("have it"
//!       → no upload → no slow-tier write → no Err arm to re-insert);
//!     * server's `MemoryStore` pin TTL (120 s) expires before the
//!       worker reschedules — bytes gone, no Err fires.
//!   None are common today, but the whole drain exists precisely to
//!   close failure-class gaps; a separate `dispatched_recently` map
//!   to throttle while the digest stays in `failed_slow_writes` is
//!   the right shape. Deferred from #287 to keep the fix-up scope
//!   tight.
//!
//! ## Production lifecycle
//!
//! `nativelink.rs` spawns a `tokio::spawn`'d loop that calls
//! [`drain_tick`] every [`DEFAULT_DRAIN_INTERVAL`]. The drain logic
//! lives here (not inline in the binary) so integration tests can
//! exercise the full production composition (real FSS, real
//! BlobLocalityMap, real worker_tx) without spinning up the rest of
//! the binary.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use futures::stream::{FuturesUnordered, StreamExt};
use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, UploadMissingBlobsRequest, update_for_worker,
};
use nativelink_store::fast_slow_store::{FastSlowStore, SelfRetryOutcome};
use nativelink_store::small_blob_dispatcher::SmallBlobDispatcher;
use nativelink_store::wrapper_walker::{find_fast_slow_via_chain, synthetic_large_key};
use nativelink_util::blob_locality_map::SharedBlobLocalityMap;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
use tokio::sync::{Semaphore, mpsc};
use tracing::{info, warn};

/// Default drain tick interval. Conservative — the failure rate is
/// much lower than this so most ticks find an empty set.
pub const DEFAULT_DRAIN_INTERVAL: Duration = Duration::from_secs(5);

/// Default per-digest V3 self-retry slow-tier timeout (red-team
/// BLOCK-2). Bounds the `slow_store.update_oneshot` inside
/// [`nativelink_store::fast_slow_store::FastSlowStore::try_self_retry_slow_write`]
/// so a wedged slow tier (e.g. ZFS tank pool degraded for hours)
/// cannot block the drainer indefinitely. MUST be `≤
/// DEFAULT_DRAIN_INTERVAL` so a wedged digest at most delays its
/// own tick finish, not the next tick. With the dsr MAJOR-1
/// `FuturesUnordered` parallelism, multiple slow-tier writes
/// overlap inside this same budget, but each individual write is
/// still bounded.
pub const DEFAULT_SELF_RETRY_TIMEOUT: Duration = Duration::from_secs(2);

/// Default concurrency cap for V3 self-retry slow-tier writes within
/// a single drain tick (dsr MAJOR-1). Without parallelism the worst
/// case is `N × DEFAULT_SELF_RETRY_TIMEOUT` per tick (e.g. 723
/// digests × 2 s = 24 minutes); a `FuturesUnordered` driven from a
/// `Semaphore` of this size caps in-flight slow-tier RPCs so the
/// slow tier itself isn't saturated by the drainer (which would be
/// strictly worse than wedging on one). Set conservative — the slow
/// tier already has its own concurrency limits (filesystem write
/// capacity, network bandwidth); 16 is large enough to mask
/// individual transient spikes but small enough to leave headroom
/// for normal Bazel writes.
pub const DEFAULT_SELF_RETRY_CONCURRENCY: usize = 16;

/// Default per-digest cooldown. After we either dispatch or fail to
/// dispatch a digest, skip it for this long even if it appears in
/// the set again.
pub const DEFAULT_DRAIN_COOLDOWN: Duration = Duration::from_secs(60);

/// Default batch size — caps individual `UploadMissingBlobsRequest`
/// at 1000 digests so a wide failure storm doesn't ship a single
/// gigantic message.
pub const DEFAULT_DRAIN_BATCH_SIZE: usize = 1000;

/// Hard cap on the inflight-cooldown map size. Prevents unbounded
/// memory growth under a sustained failure storm.
pub const DEFAULT_DRAIN_INFLIGHT_CAP: usize = 100_000;

/// Result of a single drain tick. Counters are useful for logging
/// and tests; tests assert on them directly.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DrainTickStats {
    /// Total digests drained from `failed_slow_writes` across all
    /// supplied stores.
    pub drained: usize,
    /// Digests for which we successfully dispatched
    /// `UploadMissingBlobs` to some connected worker.
    pub dispatched: usize,
    /// Digests with no connected worker in the locality map (or none
    /// of the candidates had a registered worker_tx). Re-inserted.
    pub no_worker: usize,
    /// Digests skipped because they were dispatched/attempted within
    /// the cooldown window. Re-inserted.
    pub throttled: usize,
    /// Digests for which `tx.send()` returned Err (worker channel
    /// closed mid-tick) or whose endpoint vanished between the
    /// dispatcher snapshot and the dispatch step. Re-inserted via
    /// the source store. M1 invariant: digests counted here are NOT
    /// counted in `dispatched`, so
    /// `drained == dispatched + no_worker + throttled + send_failed
    /// + self_retried + self_retry_failed`.
    pub send_failed: usize,
    /// #335 V3 fix: digests for which the server's MemoryStore (fast
    /// tier) still held the bytes, so we re-issued the slow-tier
    /// write directly without a worker round-trip. Counts the
    /// successful self-retry path. Closes the TLA+ liveness gap where
    /// no worker has the bytes (mirror dispatch quarantined / slow).
    pub self_retried: usize,
    /// #335 V3 fix: digests for which self-retry was attempted (fast
    /// tier had bytes) but the slow-tier write returned Err. The
    /// digest is re-inserted into `failed_slow_writes` for the next
    /// tick (a transient slow-tier error should not lose the digest).
    pub self_retry_failed: usize,
}

// V3 walker now lives in `nativelink_store::wrapper_walker` so the
// `chunked_fast_slow` dispatcher wiring (`src/bin/nativelink.rs`) and
// this drainer share one canonical implementation. The previous
// in-file `find_fast_slow` walker descended `inner_store(None)`,
// which terminated at `SizePartitioningStore` (its `inner_store(None)`
// returns `self`) — meaning the V3 self-retry was a structural no-op
// for the production composition (`WorkerProxyStore` →
// `ExistenceCacheStore` → `VerifyStore` → `SizePartitioningStore` →
// `FastSlowStore`). The shared helper passes
// `synthetic_large_key()` to descend the upper arm correctly.
//
// TODO(#303): a separate `find_fast_slow_for_pin` (small-key) walker
// lives in `nativelink_store::small_blob_dispatcher`; once chunked-
// dispatcher consolidation lands, evaluate folding both into one
// helper parameterised by hint size.

/// Run a single drain tick. Drains `failed_slow_writes` across every
/// supplied CAS store, attempts an in-process self-retry from the
/// server's fast tier (#335 V3 fix), and for digests where the fast
/// tier no longer holds the bytes, picks a connected worker from the
/// locality map and dispatches `UploadMissingBlobs`. Returns a
/// [`DrainTickStats`] summary.
///
/// `inflight` is the per-digest last-dispatch timestamp. Mutated in
/// place: stale entries (older than `cooldown`) are GC'd at the start
/// of the tick; new dispatches/throttles are recorded.
///
/// ## #335 V3 self-retry path
///
/// For each drained digest the drainer calls
/// [`FastSlowStore::try_self_retry_slow_write`] BEFORE consulting the
/// locality map. On `SelfRetryOutcome::Succeeded` the digest is
/// already removed from `failed_slow_writes` (FSS does it) and
/// pushed to `stable_digests` for BIS broadcast — no worker dispatch.
/// On `SelfRetryOutcome::FastTierMiss` (pin expired) the drainer
/// falls through to the pre-#335 `UploadMissingBlobs` path. On Err
/// (slow-tier transient) the digest is re-inserted via
/// `reinsert_failed_digests` for the next tick.
///
/// This closes a TLA+ liveness violation where a slow-tier write
/// failure plus no-worker-source produced a permanently stuck blob:
/// the failed_slow_writes set held it forever, the fast-tier pin
/// auto-expired, and the next Bazel read saw NotFound. With self-
/// retry, the server uses its own MemoryStore as the recovery source
/// when the pin is still alive — it nearly always is, since the
/// drainer ticks every 5 s and the pin TTL is 120 s.
///
/// ## Edge cases
///
/// - **Empty set**: returns `DrainTickStats::default()` immediately.
/// - **Self-retry success**: counted in `self_retried`; not in
///   `dispatched`; no worker round-trip.
/// - **Self-retry slow-tier Err**: counted in `self_retry_failed`;
///   re-inserted into source store.
/// - **Fast-tier miss + no worker for digest**: re-inserts and
///   counts in `no_worker`.
/// - **Fast-tier miss + worker tx send error**: re-inserts the entire
///   batch and counts the failure. The worker's `unregister_worker`
///   will drop the tx from the dispatcher snapshot on the next tick,
///   so a permanently dead worker stops being picked.
/// - **Recently dispatched (cooldown)**: re-inserts and counts in
///   `throttled`. Self-retry is also gated by the cooldown so we
///   don't hammer the slow tier on a transient.
///
/// ## Lock discipline
///
/// Acquires `failed_slow_writes` (parking_lot::Mutex via
/// `drain_failed_digests`/`reinsert_failed_digests`), the
/// `BlobLocalityMap` (RwLock read), and the dispatcher's `worker_txs`
/// (parking_lot::Mutex). Self-retry holds the FSS's
/// `failed_slow_writes` and `stable_digests` Mutex briefly inside
/// `try_self_retry_slow_write`; both are dropped before any `.await`.
/// Per-digest dispatch is `tx.send()`, which returns immediately on
/// an unbounded channel.
pub async fn drain_tick(
    cas_stores: &[(String, Store)],
    locality_map: &SharedBlobLocalityMap,
    dispatcher: &SmallBlobDispatcher,
    inflight: &mut HashMap<DigestInfo, Instant>,
    cooldown: Duration,
    batch_size: usize,
    inflight_cap: usize,
) -> DrainTickStats {
    let mut stats = DrainTickStats::default();

    // Drain failed_slow_writes per CAS store.
    let mut all_failed: Vec<(String, Vec<DigestInfo>)> = Vec::new();
    for (name, store) in cas_stores {
        let drained = store.drain_failed_digests();
        if !drained.is_empty() {
            all_failed.push((name.clone(), drained));
        }
    }
    if all_failed.is_empty() {
        return stats;
    }

    let now = Instant::now();
    inflight.retain(|_, ts| now.duration_since(*ts) < cooldown);

    // Snapshot connected workers ONCE per tick.
    let connected = dispatcher.connected_workers_with_senders();
    let mut endpoint_to_tx: HashMap<Arc<str>, mpsc::UnboundedSender<UpdateForWorker>> =
        HashMap::new();
    for (ep, _epoch, tx) in connected {
        endpoint_to_tx.entry(ep).or_insert(tx);
    }

    // Group by endpoint for one UploadMissingBlobs per worker per
    // tick. Each entry carries `(store_name, digest)` so that a
    // dispatch failure (race-loser or `tx.send()` Err) re-inserts the
    // digest into the SOURCE store rather than fan-in spraying every
    // CAS store. The single-cas_store deployment masked this; the
    // `cas_stores: &[(String, Store)]` API supports N (split-routing,
    // #267).
    let mut per_endpoint: HashMap<Arc<str>, Vec<(String, DigestInfo)>> = HashMap::new();
    let mut reinsert: HashMap<String, Vec<DigestInfo>> = HashMap::new();

    // Cache wrapper-walker results per store name. The walk is cheap
    // (a few pointer compares) but doing it per-digest would still
    // bloat the hot path; cache it once per tick.
    //
    // Stored as a parallel index into `cas_stores`. None means the
    // chain doesn't terminate in a FastSlowStore (no self-retry path
    // available) — drainer falls back to pre-#335 behavior.
    //
    // CRITICAL: pass `synthetic_large_key()` (not `None`) to descend
    // `SizePartitioningStore` correctly. See `wrapper_walker` module
    // docs for the partitioning trap details.
    let fss_for_store: HashMap<&str, Option<&FastSlowStore>> = cas_stores
        .iter()
        .map(|(name, store)| {
            let driver = store.inner_store(Some(synthetic_large_key()));
            let fss = find_fast_slow_via_chain(driver);
            // M1 observability (dsr blocker): when the walker returns
            // None for a store that has digests to drain, V3 self-retry
            // is structurally inactive for that composition. Without
            // this warn the operator has no visibility — the drainer
            // silently falls through to pre-#335 worker-only retry,
            // re-opening the TLA+ liveness gap whenever no worker has
            // the bytes. Emit once per tick per store (NOT per digest)
            // so a wide failure storm doesn't spam the log; the
            // per-tick cardinality is bounded by `cas_stores.len()`
            // (production = 1, max plausible = ~10 with split-routing).
            if fss.is_none() {
                warn!(
                    store_name = %name,
                    walker_path = "no_fss_in_chain",
                    composition = "WorkerProxyStore → ExistenceCacheStore → \
                                   VerifyStore → SizePartitioningStore(upper) → \
                                   FastSlowStore (production)",
                    "failed_slow_writes_drain: V3 self-retry inactive — fast tier \
                     walker found no FastSlowStore in chain; falling through to \
                     pre-#335 worker-only retry (TLA+ liveness gap re-opens when \
                     no worker has the bytes)"
                );
            }
            (name.as_str(), fss)
        })
        .collect();

    // dsr MAJOR-1 (parallel V3 self-retry).
    //
    // Phase A (sequential, cheap): per-digest triage. Each digest
    // routes into ONE of three buckets:
    //   - `throttled` (cooldown active) → counted + re-insert below
    //   - `self_retry_queue` (FSS available + not throttled) → driven
    //     in parallel under a Semaphore in Phase B
    //   - `worker_dispatch_queue` (no FSS in chain) → handed straight
    //     to the existing worker-dispatch logic
    //
    // The pre-fix loop did the V3 `await` inline per-digest, so 723
    // digests × 2 s timeout = 24 minutes worst-case per tick. Now
    // the timeouts overlap inside the Semaphore budget, capping
    // aggregate per-tick wall-clock at roughly
    // `(N / DEFAULT_SELF_RETRY_CONCURRENCY) × DEFAULT_SELF_RETRY_TIMEOUT`.
    let mut self_retry_queue: Vec<(String, DigestInfo, &FastSlowStore)> = Vec::new();
    let mut worker_dispatch_queue: Vec<(String, DigestInfo)> = Vec::new();

    for (store_name, digests) in all_failed {
        stats.drained += digests.len();
        for digest in digests {
            // Throttle: digest dispatched/attempted within cooldown?
            // re-insert + skip. Self-retry is also throttled — a
            // transient slow-tier failure should not be hammered every
            // tick.
            if let Some(ts) = inflight.get(&digest)
                && now.duration_since(*ts) < cooldown
            {
                stats.throttled += 1;
                reinsert.entry(store_name.clone()).or_default().push(digest);
                continue;
            }

            // Route by FSS availability. The FuturesUnordered batch
            // below drives all queued self-retries in parallel under
            // the Semaphore; on miss/err we re-route here in Phase C.
            if let Some(Some(fss)) = fss_for_store.get(store_name.as_str()) {
                self_retry_queue.push((store_name.clone(), digest, *fss));
            } else {
                worker_dispatch_queue.push((store_name.clone(), digest));
            }
        }
    }

    // Phase B (parallel V3 self-retry, dsr MAJOR-1).
    //
    // Each digest acquires a permit from a `Semaphore::new(
    // DEFAULT_SELF_RETRY_CONCURRENCY)` then awaits
    // `try_self_retry_slow_write` (already bounded by
    // `DEFAULT_SELF_RETRY_TIMEOUT` per BLOCK-2). The permit is
    // released on future completion. Worst-case in-flight slow-tier
    // RPCs is bounded by the semaphore — protects the slow tier from
    // being saturated by the drainer (which would be strictly worse
    // than wedging on one digest).
    //
    // FSS references are borrowed from `fss_for_store` whose lifetime
    // is bound by `cas_stores`; the `FuturesUnordered` is awaited
    // here inline so the borrows stay valid.
    if !self_retry_queue.is_empty() {
        let semaphore = Arc::new(Semaphore::new(DEFAULT_SELF_RETRY_CONCURRENCY));
        let mut futs = FuturesUnordered::new();
        for (store_name, digest, fss) in self_retry_queue {
            let sem = Arc::clone(&semaphore);
            futs.push(async move {
                // Permit acquisition only fails if the semaphore was
                // explicitly closed; we never close it. `expect` is
                // load-bearing — a silent skip would lose the digest.
                let _permit = sem
                    .acquire_owned()
                    .await
                    .expect("DEFAULT_SELF_RETRY semaphore closed unexpectedly");
                let outcome = fss
                    .try_self_retry_slow_write(digest, DEFAULT_SELF_RETRY_TIMEOUT)
                    .await;
                (store_name, digest, outcome)
            });
        }
        while let Some((store_name, digest, outcome)) = futs.next().await {
            match outcome {
                Ok(SelfRetryOutcome::Succeeded { .. }) => {
                    stats.self_retried += 1;
                    if inflight.len() < inflight_cap {
                        inflight.insert(digest, now);
                    }
                    // Bytes already in slow tier; BIS will broadcast.
                }
                Ok(SelfRetryOutcome::FastTierMiss) => {
                    // Fall through to Phase C worker dispatch.
                    worker_dispatch_queue.push((store_name, digest));
                }
                Err(e) => {
                    warn!(
                        ?digest,
                        store_name = %store_name,
                        err = ?e,
                        "failed_slow_writes_drain: self-retry slow-tier \
                         write failed; re-inserting for next tick \
                         (transient slow-tier outage or BLOCK-2 timeout)"
                    );
                    stats.self_retry_failed += 1;
                    if inflight.len() < inflight_cap {
                        inflight.insert(digest, now);
                    }
                    reinsert.entry(store_name).or_default().push(digest);
                }
            }
        }
    }

    // Phase C (sequential worker dispatch). Same pre-#335 fall-through
    // path; entries here are either (a) digests whose chain has no
    // FSS (no V3 path available) or (b) FastTierMiss results from
    // Phase B (pin expired or never landed). The worker round-trip
    // is fast; sequential is fine.
    for (store_name, digest) in worker_dispatch_queue {
        // Pick the first worker that's currently connected.
        let workers = locality_map.read().lookup_workers(&digest);
        let picked = workers
            .iter()
            .find(|w| endpoint_to_tx.contains_key(*w))
            .cloned();
        let Some(endpoint) = picked else {
            stats.no_worker += 1;
            if inflight.len() < inflight_cap {
                inflight.insert(digest, now);
            }
            reinsert.entry(store_name).or_default().push(digest);
            continue;
        };
        per_endpoint
            .entry(endpoint)
            .or_default()
            .push((store_name, digest));
        if inflight.len() < inflight_cap {
            inflight.insert(digest, now);
        }
        // M1 fix: do NOT increment `dispatched` here. The send
        // can still fail downstream (race-loser tx-vanished or
        // `tx.send()` Err). Counter is bumped only after a
        // successful `tx.send()` Ok arm so the invariant
        // `drained == dispatched + no_worker + throttled +
        // send_failed + self_retried + self_retry_failed` holds.
    }

    // Re-insert digests we couldn't dispatch (throttled / no-worker).
    for (store_name, digests) in reinsert {
        if let Some(store) = cas_stores
            .iter()
            .find(|(n, _)| n == &store_name)
            .map(|(_, s)| s.clone())
        {
            store.reinsert_failed_digests(&digests);
        }
    }

    // Dispatch one UploadMissingBlobs per endpoint per tick, batched.
    // Each entry retains `(store_name, digest)` so that re-insert on
    // dispatch failure routes to the source store, not a fan-in spray
    // across every CAS store.
    //
    // M1 invariant: `dispatched` is incremented PER chunk only after
    // `tx.send()` Ok; `send_failed` covers both the race-vanished
    // endpoint path (entire batch) and the mid-batch send Err path
    // (the failing chunk plus any subsequent chunks not yet sent).
    for (endpoint, entries) in per_endpoint {
        let Some(tx) = endpoint_to_tx.get(&endpoint) else {
            // Race: tx vanished between snapshot and dispatch.
            // Re-insert each digest into its source store (BLOCK B1
            // fix: do NOT spray every cas_store).
            stats.send_failed += entries.len();
            reinsert_by_source_store(cas_stores, &entries);
            continue;
        };
        let mut chunks = entries.chunks(batch_size);
        while let Some(chunk) = chunks.next() {
            let proto_digests: Vec<Digest> =
                chunk.iter().map(|(_, d)| Digest::from(*d)).collect();
            let msg = UpdateForWorker {
                update: Some(update_for_worker::Update::UploadMissingBlobs(
                    UploadMissingBlobsRequest {
                        digests: proto_digests,
                    },
                )),
            };
            if tx.send(msg).is_err() {
                warn!(
                    endpoint = %endpoint,
                    count = chunk.len(),
                    "failed_slow_writes_drain: worker channel closed; \
                     re-inserting batch"
                );
                stats.send_failed += chunk.len();
                reinsert_by_source_store(cas_stores, chunk);
                // Re-insert ALL remaining chunks too — once tx is
                // closed, no later chunk will succeed.
                for remaining in chunks.by_ref() {
                    stats.send_failed += remaining.len();
                    reinsert_by_source_store(cas_stores, remaining);
                }
                break;
            }
            stats.dispatched += chunk.len();
        }
    }

    if stats.drained > 0 {
        info!(
            drained = stats.drained,
            dispatched = stats.dispatched,
            no_worker = stats.no_worker,
            throttled = stats.throttled,
            send_failed = stats.send_failed,
            self_retried = stats.self_retried,
            self_retry_failed = stats.self_retry_failed,
            "failed_slow_writes_drain: tick complete"
        );
    }
    stats
}

/// Re-insert digests into their source CAS stores, grouping by
/// `store_name`. Used by the dispatch-failure paths (race-loser when
/// `tx` vanished between snapshot and dispatch; `tx.send()` Err
/// mid-batch). Per BLOCK B1 / split-routing (#267): re-insert MUST
/// route to the source store rather than fan-in spray every CAS store.
fn reinsert_by_source_store(cas_stores: &[(String, Store)], entries: &[(String, DigestInfo)]) {
    let mut by_store: HashMap<&str, Vec<DigestInfo>> = HashMap::new();
    for (store_name, digest) in entries {
        by_store
            .entry(store_name.as_str())
            .or_default()
            .push(*digest);
    }
    for (store_name, digests) in by_store {
        if let Some(store) = cas_stores
            .iter()
            .find(|(n, _)| n == store_name)
            .map(|(_, s)| s.clone())
        {
            store.reinsert_failed_digests(&digests);
        }
    }
}
