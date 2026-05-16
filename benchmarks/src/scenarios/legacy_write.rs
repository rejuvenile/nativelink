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

//! Flow W1: legacy ByteStream::Write through the production wrapper
//! chain (ExistenceCache → Verify → FastSlow{Memory, Filesystem}).
//!
//! **Invariant being anchored:** the prod write composition's wall-clock
//! and throughput for sequential and parallel writes. A regression here
//! flags any future change that adds latency to the
//! `ExistenceCache::update → Verify::update → FastSlow::update` chain
//! (e.g. an over-eager `has` probe added on the write path, a fsync
//! sneaking in, a slow-write back-pressure cap firing prematurely).

use std::collections::BTreeMap;

use bytes::Bytes;
use nativelink_util::store_trait::StoreLike;

use crate::composition::{Composition, build_prod_cas_composition, prod_defaults};
use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob, measure};

#[derive(Debug, Clone, Copy)]
pub struct WriteCell {
    pub size: usize,
    pub label: &'static str,
    pub concurrency: u32,
}

/// Production-aligned cell matrix per design Section 2:
///
/// - tiny (1 KiB) — exists for diff-stable per-op overhead anchoring
/// - small (16 KiB) — below the chunked-size threshold
/// - medium (1 MiB) — `batch_update_threshold_bytes` boundary
/// - large (16 MiB) — above SizePartitioning fast-tier threshold;
///   chunked-v2 would activate in prod for this size; here it exercises
///   the slow Filesystem write path.
///
/// Concurrencies: 1 (sequential baseline), 10 (typical Bazel burst).
/// 100 is deferred to nightly per design Section 5.
const W1_CELLS: &[WriteCell] = &[
    WriteCell { size: 1_024, label: "1KiB", concurrency: 1 },
    WriteCell { size: 16_384, label: "16KiB", concurrency: 1 },
    WriteCell { size: 1_048_576, label: "1MiB", concurrency: 1 },
    WriteCell { size: 16 * 1_048_576, label: "16MiB", concurrency: 1 },
    WriteCell { size: 1_048_576, label: "1MiB", concurrency: 10 },
];

pub async fn run(opts: &RunOpts) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(20);

    // One composition per scenario family — keeps the fresh-tempdir
    // semantics so a previous cell's evictions don't pollute the next.
    //
    // `slow_writes_in_flight_max_bytes`: must be non-zero per the
    // CLAUDE.md "unbounded buffering" rule which is enforced at
    // `FastSlowStore::new_validated`. We use the production
    // `buildcache-native.json5` value (8 GiB) so the cell exercises the
    // same admission gate as production.
    let composition = build_prod_cas_composition(
        prod_defaults::FAST_MEMORY_MAX_BYTES,
        prod_defaults::SLOW_WRITES_INFLIGHT_MAX_BYTES,
    )
    .await
    .expect("build_prod_cas_composition for W1");

    for cell in W1_CELLS {
        let scenario_name = format!(
            "w1_bytestream_write_fastslow_filesystem_{label}_c{c}",
            label = cell.label,
            c = cell.concurrency
        );
        if !opts.matches(&scenario_name) {
            continue;
        }
        out.push(run_one_cell(&composition, cell, iters, &scenario_name).await);
    }
    out
}

async fn run_one_cell(
    composition: &Composition,
    cell: &WriteCell,
    iters: u32,
    scenario_name: &str,
) -> BenchmarkResult {
    // Per-cell seed makes each cell's blob unique while staying
    // deterministic across runs.
    let seed = ((cell.size as u64) << 16) ^ (cell.concurrency as u64);
    let throughput_bytes_per_iter = (cell.size as u64) * (cell.concurrency as u64);

    let mut extras = BTreeMap::new();
    extras.insert(
        "fast_tier_max_bytes".to_string(),
        serde_json::json!(prod_defaults::FAST_MEMORY_MAX_BYTES),
    );
    extras.insert(
        "size_partitioning_threshold".to_string(),
        serde_json::json!(prod_defaults::SIZE_PARTITIONING_THRESHOLD),
    );

    let cas = composition.cas_store.clone();
    let concurrency = cell.concurrency;
    let size = cell.size;
    let cas_for_body = cas.clone();

    // Per-iteration counter so each iter writes a fresh digest. Without
    // this, ExistenceCacheStore's positive cache would short-circuit
    // every write after the first one and we'd measure cache-hit
    // overhead instead of the actual write path. We measure the WRITE
    // path; the cache-hit path is what F1 anchors.
    let iter_counter = std::sync::atomic::AtomicU64::new(0);

    measure(
        "W1",
        scenario_name,
        Some(size as u64),
        concurrency,
        CacheState::Cold,
        iters,
        Some(throughput_bytes_per_iter),
        None,
        extras,
        move || {
            let cas = cas_for_body.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                // Build N concurrent writes, each with a distinct
                // digest. The fan-out cost is folded into the cell;
                // diff tooling can compare per-cell totals because
                // concurrency is part of the diff key.
                let mut futs = Vec::with_capacity(concurrency as usize);
                for j in 0..concurrency {
                    let (digest, data): (_, Bytes) =
                        make_blob(seed ^ n ^ (j as u64), size);
                    let cas = cas.clone();
                    futs.push(async move {
                        cas.update_oneshot(digest, data)
                            .await
                            .expect("W1 write must succeed");
                    });
                }
                futures::future::join_all(futs).await;
            }
        },
    )
    .await
}
