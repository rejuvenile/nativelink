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

//! Flow W1: writes through the prod CAS wrapper chain.
//!
//! **Scenario name caveat:** the cells are named
//! `w1_store_update_oneshot_*`, NOT `w1_bytestream_*`. They exercise the
//! `StoreLike::update_oneshot` path through
//! `Verify → ExistenceCache → SizePartitioning → {SMALL_CAS_CACHED, cas_FAST_SLOW_STORE}`.
//! They DO NOT cross the `ByteStreamServer` (no gRPC service in the
//! path, no h2/QUIC framing, no WriteState machine). Wiring the
//! ByteStream front-end is a Phase 1.5 follow-up; until then, these
//! cells anchor the store wrapper chain only — reviewers comparing W1
//! numbers to production ByteStream tail latency MUST account for the
//! RPC-layer absence.
//!
//! **Invariant being anchored:** the prod write composition's wall-clock
//! and throughput for sequential writes through the wrapper chain UP TO
//! AND INCLUDING the fast-tier ack. A regression here flags any change
//! that adds latency to
//! `ExistenceCache::update → SizePartitioning::update →
//! {SMALL_CAS_CACHED, cas_FAST_SLOW_STORE}::update` BEFORE the fast tier
//! returns (e.g. an over-eager `has` probe added on the write path, a
//! fsync sneaking in, the slow-write back-pressure cap firing
//! prematurely, MemoryStore admission contention).
//!
//! **What this cell does NOT measure (#537 red-team sibling-bug fix):**
//! at any size ≥ SizePartitioning threshold (16 KiB strict `<`), the
//! upper-tier `cas_FAST_SLOW_STORE.update_oneshot` returns AS SOON AS
//! the MemoryStore fast tier accepts the bytes; the slow-tier
//! FilesystemStore write is `tokio::spawn`'d as fire-and-forget. So at
//! 1 MiB / 16 MiB the W1 sample is "fast-tier MemoryStore ack +
//! spawn-dispatch of slow-tier write", NOT "disk write to completion".
//! `extras.measures = "fast_tier_ack_then_spawn_dispatch"` (set per
//! cell) makes this visible in the JSON output so a reader cannot
//! conflate the W1 16 MiB number with a real disk-commit-to-completion
//! latency. (For the chunked-v2 commit-to-disk anchor, see W3 / W3f —
//! those wait synchronously on the v2 FinalResponse.) At small sizes
//! (≤ 16 KiB) the small-CAS path resolves entirely in MemoryStore
//! anyway, so the distinction is moot — there is no slow tier at that
//! size.
//!
//! **Composition deviation note:** `SMALL_CAS_CACHED.slow` is a
//! MemoryStore in the bench (vs Valkey/Redis in prod). Cells whose
//! payload ≤ 16 KiB go through this Memory-only fast-slow; cells with
//! payload > 16 KiB hit `cas_FAST_SLOW_STORE`'s real Filesystem slow
//! tier. `extras.composition_deviation` flags the small-CAS cells.

use std::collections::BTreeMap;
use std::path::PathBuf;

use bytes::Bytes;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreLike;

use crate::composition::{Composition, build_prod_cas_composition, prod_defaults};
use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob_with_indices, measure};

#[derive(Debug, Clone, Copy)]
pub struct WriteCell {
    pub size: usize,
    pub label: &'static str,
    pub concurrency: u32,
}

/// Production-aligned cell matrix:
///
/// - tiny (1 KiB) — exercises SMALL_CAS_CACHED (small-blob path)
/// - small (16 KiB) — exactly AT the SizePartitioning threshold;
///   `SizePartitioningStore` uses **strict `<`** at
///   `nativelink-store/src/size_partitioning_store.rs:99,148,180,208,262,355`,
///   so a blob with size == 16384 routes to UPPER
///   (`cas_FAST_SLOW_STORE` with real Filesystem slow tier), NOT lower.
///   The cell anchors the large-blob path at the boundary value.
/// - medium (1 MiB) — exercises cas_FAST_SLOW_STORE (large-blob path)
/// - large (16 MiB) — exercises cas_FAST_SLOW_STORE filesystem slow tier
/// - 1 MiB c=10 — fan-out cell; per-task overlap (single-task)
const W1_CELLS: &[WriteCell] = &[
    WriteCell { size: 1_024, label: "1KiB", concurrency: 1 },
    WriteCell { size: 16_384, label: "16KiB", concurrency: 1 },
    WriteCell { size: 1_048_576, label: "1MiB", concurrency: 1 },
    WriteCell { size: 16 * 1_048_576, label: "16MiB", concurrency: 1 },
    WriteCell { size: 1_048_576, label: "1MiB", concurrency: 10 },
];

pub async fn run(opts: &RunOpts, temp_dir_base: Option<&PathBuf>) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(20);

    for cell in W1_CELLS {
        let scenario_name = format!(
            "w1_store_update_oneshot_{label}_c{c}",
            label = cell.label,
            c = cell.concurrency
        );
        if !opts.matches(&scenario_name) {
            continue;
        }
        // Per-cell fresh composition so previous-cell state (in-flight
        // slow writes, fast-tier residency) cannot pollute the next.
        let composition =
            match build_prod_cas_composition(temp_dir_base.map(|p| p.as_path())).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[bench] W1 composition build failed for {scenario_name}: {e:?}");
                    continue;
                }
            };
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
    let throughput_bytes_per_iter = (cell.size as u64) * (cell.concurrency as u64);
    let size = cell.size;
    let concurrency = cell.concurrency;

    // Pre-generate every blob OUTSIDE the timed body. `make_blob_with_indices`
    // is a SHA-256 + PRNG per byte; at 16 MiB × 20 iters that's >40 ms of
    // fixed cost which, if folded into wall-clock, dominates sub-50ms cells.
    let prebuilt: Vec<Vec<(DigestInfo, Bytes)>> = (0..iters)
        .map(|n| {
            (0..concurrency)
                .map(|j| make_blob_with_indices(scenario_name, n as u64, j, size))
                .collect()
        })
        .collect();

    let mut extras = BTreeMap::new();
    extras.insert(
        "cas_fast_memory_max_bytes".to_string(),
        serde_json::json!(prod_defaults::CAS_FAST_MEMORY_MAX_BYTES),
    );
    extras.insert(
        "size_partitioning_threshold".to_string(),
        serde_json::json!(prod_defaults::SIZE_PARTITIONING_THRESHOLD),
    );
    // Canonical `<` predicate lives in `composition::prod_defaults`; the
    // boundary semantics are pinned by
    // `composition::tests::deviation_helper_boundary_pins_strict_lt`.
    if prod_defaults::should_emit_small_cas_deviation(size as u64) {
        extras.insert(
            "composition_deviation".to_string(),
            serde_json::json!("small_cas_redis_replaced_with_memory"),
        );
    }
    // #537 D3: self-describing JSON — name what the timed body actually
    // waits for. The W1 cell's `cas.update_oneshot` returns at the fast-
    // tier ack: at sizes ≥ 16 KiB the upper `cas_FAST_SLOW_STORE` accepts
    // the bytes into MemoryStore and `tokio::spawn`s the slow-tier
    // FilesystemStore write as fire-and-forget; at sizes < 16 KiB the
    // small-CAS path resolves in MemoryStore entirely. EITHER WAY no
    // FilesystemStore syscall is in the timed body. A reader consuming
    // a W1 baseline JSON in isolation MUST be able to derive this
    // WITHOUT reading the scenario doc-comment — see #537 red-team
    // 6-month pre-mortem.
    extras.insert(
        "measures".to_string(),
        serde_json::json!("fast_tier_ack_then_spawn_dispatch"),
    );

    let iter_counter = std::sync::atomic::AtomicU64::new(0);
    let cas = composition.cas_store.clone();
    let prebuilt = std::sync::Arc::new(prebuilt);
    let prebuilt_for_body = prebuilt.clone();

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
            let cas = cas.clone();
            let prebuilt = prebuilt_for_body.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let row = &prebuilt[n as usize];
                let mut futs = Vec::with_capacity(concurrency as usize);
                for (digest, data) in row.iter().cloned() {
                    let cas = cas.clone();
                    futs.push(async move {
                        cas.update_oneshot(digest, data)
                            .await
                            .expect("W1 write must succeed");
                    });
                }
                // join_all: single-task overlap (not multi-core
                // parallelism). Documented intentional — production
                // ByteStream gRPC handlers run on per-stream worker
                // tasks, but the store-only path is intra-task.
                futures::future::join_all(futs).await;
            }
        },
    )
    .await
}
