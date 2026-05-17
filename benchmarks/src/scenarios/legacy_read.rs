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

//! Flow R1: reads through the prod CAS wrapper chain.
//!
//! **Scenario name caveat:** the cells are named
//! `r1_store_get_part_unchunked_*`, NOT `r1_bytestream_*`. They exercise
//! `StoreLike::get_part_unchunked` through the prod CAS wrapper chain.
//! They DO NOT cross `ByteStreamServer` (no gRPC service in the path,
//! no h2/QUIC framing). The RPC-layer behavior is anchored separately.
//!
//! **Warm cells**: prepopulate ONE digest per slot, then re-read it N
//! times. Anchors fast-tier-hit (MemoryStore) latency.
//!
//! **Cold cells**: pre-write the blob through a separate, throwaway
//! composition that shares the underlying tempdir (so the
//! `FilesystemStore` content_path persists on disk), then drop that
//! composition (eliminating the fast-tier MemoryStore residency),
//! rebuild the composition pointing at the same tempdir, and read.
//! The read MUST cross from the FilesystemStore slow tier back into
//! a fresh (empty) MemoryStore fast tier. This is a true cold read.
//!
//! Falsification: the 2026-05-04 `finalize_holding` index-visibility
//! bug fired when a rename-into-canonical-path didn't update the in-
//! process `evicting_map` index — so a cold read AFTER a rebuild would
//! return NotFound even though the file is on disk. This cell would
//! catch that regression class.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreLike;

use crate::composition::{build_prod_cas_composition, prod_defaults};
use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob_with_indices, measure};

#[derive(Debug, Clone, Copy)]
pub struct ReadCell {
    pub size: usize,
    pub label: &'static str,
    pub concurrency: u32,
    /// If true, the blob is prepopulated and read N times so the cell
    /// measures fast-tier-hit latency. If false, the blob is
    /// pre-written then the fast tier is dropped (true cold read).
    pub warm: bool,
}

const R1_CELLS: &[ReadCell] = &[
    // Warm path: prepopulated, repeated reads. Fast-tier hot.
    ReadCell { size: 1_024, label: "1KiB", concurrency: 1, warm: true },
    ReadCell { size: 1_048_576, label: "1MiB", concurrency: 1, warm: true },
    ReadCell { size: 16 * 1_048_576, label: "16MiB", concurrency: 1, warm: true },
    ReadCell { size: 1_048_576, label: "1MiB", concurrency: 10, warm: true },
    // Cold path: prepopulate via throwaway composition → drop composition
    // → rebuild against same tempdir → read MUST traverse slow tier.
    ReadCell { size: 1_048_576, label: "1MiB", concurrency: 1, warm: false },
    ReadCell { size: 16 * 1_048_576, label: "16MiB", concurrency: 1, warm: false },
];

pub async fn run(opts: &RunOpts, temp_dir_base: Option<&PathBuf>) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(20);

    for cell in R1_CELLS {
        let cache = if cell.warm { "warm" } else { "cold" };
        let scenario_name = format!(
            "r1_store_get_part_unchunked_{label}_c{c}_{cache}",
            label = cell.label,
            c = cell.concurrency,
            cache = cache,
        );
        if !opts.matches(&scenario_name) {
            continue;
        }
        let result = if cell.warm {
            run_warm_cell(cell, iters, &scenario_name, temp_dir_base).await
        } else {
            run_cold_cell(cell, iters, &scenario_name, temp_dir_base).await
        };
        match result {
            Ok(r) => out.push(r),
            Err(e) => {
                eprintln!("[bench] R1 cell {scenario_name} failed: {e:?}");
            }
        }
    }
    out
}

async fn run_warm_cell(
    cell: &ReadCell,
    iters: u32,
    scenario_name: &str,
    temp_dir_base: Option<&PathBuf>,
) -> Result<BenchmarkResult, nativelink_error::Error> {
    let composition = build_prod_cas_composition(temp_dir_base.map(|p| p.as_path())).await?;
    let cas = composition.cas_store.clone();

    // Prepopulate ONE digest per concurrency slot. These are the warm
    // anchors; we read them N times.
    let mut warm_digests: Vec<(DigestInfo, usize)> =
        Vec::with_capacity(cell.concurrency as usize);
    for j in 0..cell.concurrency {
        let (digest, data) = make_blob_with_indices(scenario_name, 0, j, cell.size);
        let expected_len = data.len();
        cas.update_oneshot(digest, data).await?;
        warm_digests.push((digest, expected_len));
    }

    let mut extras = BTreeMap::new();
    extras.insert("warm".to_string(), serde_json::json!(true));
    if (cell.size as u64) <= prod_defaults::SIZE_PARTITIONING_THRESHOLD {
        extras.insert(
            "composition_deviation".to_string(),
            serde_json::json!("small_cas_redis_replaced_with_memory"),
        );
    }
    let throughput_bytes_per_iter = (cell.size as u64) * (cell.concurrency as u64);
    let warm_digests = Arc::new(warm_digests);

    Ok(measure(
        "R1",
        scenario_name,
        Some(cell.size as u64),
        cell.concurrency,
        CacheState::Warm,
        iters,
        Some(throughput_bytes_per_iter),
        None,
        extras,
        move || {
            let cas = cas.clone();
            let warm_digests = warm_digests.clone();
            async move {
                let mut futs = Vec::with_capacity(warm_digests.len());
                for (digest, expected_len) in warm_digests.iter().copied() {
                    let cas = cas.clone();
                    futs.push(async move {
                        let bytes = cas
                            .get_part_unchunked(digest, 0, None)
                            .await
                            .expect("R1 warm read must succeed");
                        assert_eq!(bytes.len(), expected_len);
                    });
                }
                futures::future::join_all(futs).await;
            }
        },
    )
    .await)
}

/// True cold-read cell: pre-write blobs through a throwaway
/// composition that shares the same FilesystemStore content_path; drop
/// that composition (eliminating the fast-tier MemoryStore residency);
/// build a fresh composition against the same content_path; time reads.
///
/// The FilesystemStore picks up existing files at startup
/// (`FilesystemStore::new` scans `content_path` and inserts each entry
/// into its `evicting_map`). The fresh MemoryStore is empty, so the
/// first read of each digest MUST cross the slow-tier — a true cold
/// read of a real persisted blob.
///
/// Each iteration uses a distinct digest (`make_blob_with_indices(n)`)
/// so even within one timed pass the cell doesn't trivially become
/// warm on iter 2+.
async fn run_cold_cell(
    cell: &ReadCell,
    iters: u32,
    scenario_name: &str,
    temp_dir_base: Option<&PathBuf>,
) -> Result<BenchmarkResult, nativelink_error::Error> {
    // Persistent on-disk roots that both compositions share. We hold
    // ONE `TempDir` outside the compositions so its lifetime brackets
    // both pop-comp and cold-comp.
    let base = match temp_dir_base {
        Some(p) => tempfile::TempDir::new_in(p),
        None => tempfile::TempDir::new(),
    }
    .map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "cold-cell base tempdir: {e:?}",
        )
    })?;
    let content_path = base.path().join("content").to_string_lossy().into_owned();
    let temp_path = base.path().join("temp").to_string_lossy().into_owned();

    // Pass 1: prepopulate every cold-read digest, then DROP the
    // composition so the fast-tier MemoryStore loses residency. We
    // give pop-comp a NEW inner TempDir that drops when pop-comp drops
    // — but content/temp paths point at `base`, which persists.
    let prebuilt_cold: Vec<(DigestInfo, usize)> = {
        let pop_inner = tempfile::TempDir::new_in(base.path()).map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "pop-comp inner tempdir: {e:?}",
            )
        })?;
        let pop_comp = crate::composition::build_prod_cas_composition_with_paths(
            pop_inner,
            content_path.clone(),
            temp_path.clone(),
        )
        .await?;
        let cas = pop_comp.cas_store.clone();
        let mut acc = Vec::with_capacity(iters as usize);
        for n in 0..iters as u64 {
            let (digest, data) =
                make_blob_with_indices(scenario_name, n, 0, cell.size);
            let expected_len = data.len();
            cas.update_oneshot(digest, data).await?;
            acc.push((digest, expected_len));
        }
        // Drop pop_comp here so the MemoryStore is destroyed; files
        // persist in `base`'s content_path.
        drop(cas);
        drop(pop_comp);
        acc
    };

    // Pass 2: build a FRESH composition against the same content_path.
    // FilesystemStore scans content_path at startup and inserts each
    // entry into `evicting_map` so `has`/`get_part` see them.
    let cold_inner = tempfile::TempDir::new_in(base.path()).map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "cold-comp inner tempdir: {e:?}",
        )
    })?;
    let cold_comp = crate::composition::build_prod_cas_composition_with_paths(
        cold_inner,
        content_path,
        temp_path,
    )
    .await?;
    let cas = cold_comp.cas_store.clone();

    // `base` is moved into the closure-captured `_base_keepalive` to
    // ensure it lives across the entire measurement. Dropping it would
    // delete the FilesystemStore's content_path mid-bench.
    let _base_keepalive = base;

    let mut extras = BTreeMap::new();
    extras.insert("warm".to_string(), serde_json::json!(false));
    extras.insert(
        "cold_mechanism".to_string(),
        serde_json::json!("rebuild_composition_on_persisted_tempdir"),
    );
    if (cell.size as u64) <= prod_defaults::SIZE_PARTITIONING_THRESHOLD {
        extras.insert(
            "composition_deviation".to_string(),
            serde_json::json!("small_cas_redis_replaced_with_memory"),
        );
    }
    let throughput_bytes_per_iter = cell.size as u64;

    let prebuilt = Arc::new(prebuilt_cold);
    let iter_counter = std::sync::atomic::AtomicU64::new(0);

    Ok(measure(
        "R1",
        scenario_name,
        Some(cell.size as u64),
        cell.concurrency,
        CacheState::Cold,
        iters,
        Some(throughput_bytes_per_iter),
        None,
        extras,
        move || {
            let cas = cas.clone();
            let prebuilt = prebuilt.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let (digest, expected_len) = prebuilt[n as usize];
                let bytes = cas
                    .get_part_unchunked(digest, 0, None)
                    .await
                    .expect("R1 cold read must succeed (digest persisted to filesystem; \
                             fresh composition's FilesystemStore should see it)");
                assert_eq!(
                    bytes.len(),
                    expected_len,
                    "R1 cold read returned wrong-size payload"
                );
            }
        },
    )
    .await)
}

