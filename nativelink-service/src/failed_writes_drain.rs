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

use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, UploadMissingBlobsRequest, update_for_worker,
};
use nativelink_store::small_blob_dispatcher::SmallBlobDispatcher;
use nativelink_util::blob_locality_map::SharedBlobLocalityMap;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Default drain tick interval. Conservative — the failure rate is
/// much lower than this so most ticks find an empty set.
pub const DEFAULT_DRAIN_INTERVAL: Duration = Duration::from_secs(5);

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
}

/// Run a single drain tick. Drains `failed_slow_writes` across every
/// supplied CAS store, picks a connected worker per digest from the
/// locality map, and dispatches `UploadMissingBlobs` to that worker's
/// `worker_tx`. Returns a [`DrainTickStats`] summary.
///
/// `inflight` is the per-digest last-dispatch timestamp. Mutated in
/// place: stale entries (older than `cooldown`) are GC'd at the start
/// of the tick; new dispatches/throttles are recorded.
///
/// ## Edge cases
///
/// - **Empty set**: returns `DrainTickStats::default()` immediately.
/// - **No worker for digest**: re-inserts and counts in `no_worker`.
/// - **Worker tx send error**: re-inserts the entire batch and counts
///   the failure. The worker's `unregister_worker` will drop the tx
///   from the dispatcher snapshot on the next tick, so a permanently
///   dead worker stops being picked.
/// - **Recently dispatched (cooldown)**: re-inserts and counts in
///   `throttled`.
///
/// ## Lock discipline
///
/// Acquires `failed_slow_writes` (parking_lot::Mutex via
/// `drain_failed_digests`/`reinsert_failed_digests`), the
/// `BlobLocalityMap` (RwLock read), and the dispatcher's `worker_txs`
/// (parking_lot::Mutex). All locks are dropped before any `.await`
/// (none in this function). Per-digest dispatch is `tx.send()`, which
/// returns immediately on an unbounded channel.
pub fn drain_tick(
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

    for (store_name, digests) in all_failed {
        stats.drained += digests.len();
        for digest in digests {
            // Throttle: digest dispatched within cooldown? re-insert + skip.
            if let Some(ts) = inflight.get(&digest) {
                if now.duration_since(*ts) < cooldown {
                    stats.throttled += 1;
                    reinsert.entry(store_name.clone()).or_default().push(digest);
                    continue;
                }
            }
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
                reinsert.entry(store_name.clone()).or_default().push(digest);
                continue;
            };
            per_endpoint
                .entry(endpoint)
                .or_default()
                .push((store_name.clone(), digest));
            if inflight.len() < inflight_cap {
                inflight.insert(digest, now);
            }
            stats.dispatched += 1;
        }
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
    for (endpoint, entries) in per_endpoint {
        let Some(tx) = endpoint_to_tx.get(&endpoint) else {
            // Race: tx vanished between snapshot and dispatch.
            // Re-insert each digest into its source store (BLOCK B1
            // fix: do NOT spray every cas_store).
            reinsert_by_source_store(cas_stores, &entries);
            continue;
        };
        for chunk in entries.chunks(batch_size) {
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
                reinsert_by_source_store(cas_stores, chunk);
                break;
            }
        }
    }

    if stats.drained > 0 {
        info!(
            drained = stats.drained,
            dispatched = stats.dispatched,
            no_worker = stats.no_worker,
            throttled = stats.throttled,
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
