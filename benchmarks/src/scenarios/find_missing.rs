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

//! Flow F1: FindMissingBlobs through the ExistenceCache hot path.
//!
//! Two cells per the design doc: cache HIT (digest is known to the
//! cache, sub-millisecond) and cache MISS (digest is fresh, has to fall
//! through to the underlying FastSlow). Both matter — the hit-path is
//! what makes Bazel build cache lookups fast; the miss-path is what
//! makes a fresh build's first FMB sequence acceptable.

use std::collections::BTreeMap;
use std::path::PathBuf;

use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreKey, StoreLike};

use crate::composition::build_prod_cas_composition;
use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob_with_indices, measure};

/// Batch sizes worth measuring. Bazel's FMB batch is variable — a
/// fresh build often emits hundreds of digests per call; an incremental
/// build emits handfuls.
const F1_BATCH_SIZES: &[(usize, &str)] = &[
    (1, "1"),
    (16, "16"),
    (128, "128"),
    (1024, "1024"),
];

pub async fn run(opts: &RunOpts, temp_dir_base: Option<&PathBuf>) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(50);

    let composition =
        match build_prod_cas_composition(temp_dir_base.map(|p| p.as_path())).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[bench] F1 composition build failed: {e:?}");
                return out;
            }
        };

    // Prepopulate one digest pool that will be the "known" set for the
    // cache-hit cells. 1024 × 1 KiB = 1 MiB total — well under any
    // budget.
    let mut known_digests: Vec<DigestInfo> = Vec::with_capacity(1024);
    for j in 0..1024u32 {
        let (digest, data) =
            make_blob_with_indices("f1_known_pool", 0, j, 1024);
        if let Err(e) = composition.cas_store.update_oneshot(digest, data).await {
            eprintln!("[bench] F1 prepopulate failed at j={j}: {e:?}");
            return out;
        }
        known_digests.push(digest);
    }

    for &(batch, label) in F1_BATCH_SIZES {
        let hit_name = format!("f1_find_missing_cache_hit_batch{label}");
        if opts.matches(&hit_name) {
            out.push(run_one(
                &composition.cas_store,
                &hit_name,
                &known_digests[..batch],
                iters,
                CacheState::Warm,
                /* expect_present = */ true,
            )
            .await);
        }

        let miss_name = format!("f1_find_missing_cache_miss_batch{label}");
        if opts.matches(&miss_name) {
            // Fresh digests for the miss path — never seen before.
            // Per-cell seeded by the cell name so distinct cells don't
            // collide on each other.
            let mut missing = Vec::with_capacity(batch);
            for j in 0..(batch as u32) {
                let (digest, _) = make_blob_with_indices(&miss_name, 0, j, 16);
                missing.push(digest);
            }
            out.push(run_one(
                &composition.cas_store,
                &miss_name,
                &missing,
                iters,
                CacheState::Cold,
                /* expect_present = */ false,
            )
            .await);
        }
    }

    out
}

async fn run_one(
    cas: &nativelink_util::store_trait::Store,
    scenario_name: &str,
    digests: &[DigestInfo],
    iters: u32,
    cache_state: CacheState,
    expect_present: bool,
) -> BenchmarkResult {
    let mut extras = BTreeMap::new();
    extras.insert(
        "batch_size".to_string(),
        serde_json::json!(digests.len() as u64),
    );
    let elements_per_iter = digests.len() as u64;

    // Pre-allocate keys + results OUTSIDE the timed body — avoids
    // 64KiB/8KiB allocs per iter that swamp µs-scale ECS hit latency.
    let keys: Vec<StoreKey<'static>> = digests.iter().map(|d| StoreKey::from(*d)).collect();
    let cas_clone = cas.clone();
    let keys_arc = std::sync::Arc::new(keys);
    // Buffer reused per iter; len matches keys.len().
    let buf_len = digests.len();

    measure(
        "F1",
        scenario_name,
        None,
        1,
        cache_state,
        iters,
        None,
        Some(elements_per_iter),
        extras,
        move || {
            let cas = cas_clone.clone();
            let keys = keys_arc.clone();
            // Fresh result-buffer per iter; allocation cost is part of
            // the API contract for `has_with_results`, so it's fair to
            // count it. Pre-allocate to avoid grow-on-push noise.
            let mut results: Vec<Option<u64>> = vec![None; buf_len];
            async move {
                cas.has_with_results(&keys, &mut results)
                    .await
                    .expect("F1 has_with_results must succeed");
                if expect_present {
                    assert!(
                        results.iter().all(|r| r.is_some()),
                        "F1 hit cell expected every digest present"
                    );
                } else {
                    assert!(
                        results.iter().all(|r| r.is_none()),
                        "F1 miss cell expected every digest absent"
                    );
                }
            }
        },
    )
    .await
}
