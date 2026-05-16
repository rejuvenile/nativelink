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

//! Flow R1: ByteStream::Read through the production wrapper chain.
//!
//! **Invariant being anchored:** the prod read composition's wall-clock
//! for cold (slow-tier miss → Filesystem hit) and warm (fast-tier hit)
//! reads. The cold/warm comparison also serves as a falsification check
//! for the 2026-05-04 `finalize_holding` index-visibility bug — after a
//! cold read the SAME digest must be in the fast tier; a warm read that
//! is NOT ≥10× faster than the cold read suggests the index update is
//! missing again.

use core::future::Future;
use core::pin::Pin;
use std::collections::BTreeMap;

use nativelink_util::store_trait::StoreLike;

use crate::composition::{Composition, build_prod_cas_composition, prod_defaults};
use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob, measure};

#[derive(Debug, Clone, Copy)]
pub struct ReadCell {
    pub size: usize,
    pub label: &'static str,
    pub concurrency: u32,
    /// If true, the blob is prepopulated and read N times so the cell
    /// measures fast-tier-hit latency. If false, the blob is rewritten
    /// before EACH iter so the read crosses into the slow tier.
    pub warm: bool,
}

const R1_CELLS: &[ReadCell] = &[
    // Warm path: prepopulated, repeated reads. Fast-tier hot.
    ReadCell { size: 1_024, label: "1KiB", concurrency: 1, warm: true },
    ReadCell { size: 1_048_576, label: "1MiB", concurrency: 1, warm: true },
    ReadCell { size: 16 * 1_048_576, label: "16MiB", concurrency: 1, warm: true },
    ReadCell { size: 1_048_576, label: "1MiB", concurrency: 10, warm: true },
    // Cold path: same size, different digest each iter, slow-tier miss
    // forces a fast-tier populate from filesystem.
    ReadCell { size: 1_048_576, label: "1MiB", concurrency: 1, warm: false },
    ReadCell { size: 16 * 1_048_576, label: "16MiB", concurrency: 1, warm: false },
];

pub async fn run(opts: &RunOpts) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(20);

    let composition = build_prod_cas_composition(
        prod_defaults::FAST_MEMORY_MAX_BYTES,
        prod_defaults::SLOW_WRITES_INFLIGHT_MAX_BYTES,
    )
    .await
    .expect("build_prod_cas_composition for R1");

    for cell in R1_CELLS {
        let cache = if cell.warm { "warm" } else { "cold" };
        let scenario_name = format!(
            "r1_bytestream_read_fastslow_filesystem_{label}_c{c}_{cache}",
            label = cell.label,
            c = cell.concurrency,
            cache = cache,
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
    cell: &ReadCell,
    iters: u32,
    scenario_name: &str,
) -> BenchmarkResult {
    let seed = ((cell.size as u64) << 24) ^ (cell.concurrency as u64) ^ (cell.warm as u64);
    let cas = composition.cas_store.clone();
    let size = cell.size;
    let concurrency = cell.concurrency;
    let warm = cell.warm;
    let cache_state = if warm { CacheState::Warm } else { CacheState::Cold };

    // Warm path: prepopulate ONE digest per concurrency slot. Cold path
    // generates fresh digests per iter inside the body closure.
    let warm_digests = if warm {
        let mut v = Vec::with_capacity(concurrency as usize);
        for j in 0..concurrency {
            let (digest, data) = make_blob(seed ^ (j as u64), size);
            cas.update_oneshot(digest, data.clone())
                .await
                .expect("warm prepopulate must succeed");
            v.push((digest, data.len()));
        }
        Some(v)
    } else {
        None
    };

    let iter_counter = std::sync::atomic::AtomicU64::new(0);

    let mut extras = BTreeMap::new();
    extras.insert("warm".to_string(), serde_json::json!(warm));
    let throughput_bytes_per_iter = (size as u64) * (concurrency as u64);

    let cas_for_body = cas.clone();
    measure(
        "R1",
        scenario_name,
        Some(size as u64),
        concurrency,
        cache_state,
        iters,
        Some(throughput_bytes_per_iter),
        None,
        extras,
        move || {
            let cas = cas_for_body.clone();
            let warm_digests = warm_digests.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                // Box::pin the futures so the warm + cold async blocks
                // (different anon types) coexist in one Vec.
                type Fut = Pin<Box<dyn Future<Output = ()> + Send>>;
                let mut futs: Vec<Fut> = Vec::with_capacity(concurrency as usize);
                for j in 0..concurrency {
                    let cas = cas.clone();
                    if let Some(digests) = warm_digests.as_ref() {
                        let (digest, expected_len) = digests[j as usize];
                        futs.push(Box::pin(async move {
                            let bytes = cas
                                .get_part_unchunked(digest, 0, None)
                                .await
                                .expect("R1 warm read must succeed");
                            assert_eq!(bytes.len(), expected_len);
                        }));
                    } else {
                        // Cold path: write + read in the same iter so the
                        // wall-clock includes the slow-tier write + fast-
                        // tier populate. Per design Section 7 falsification
                        // #4: the warm cell at the same size MUST be ≥10×
                        // faster.
                        let (digest, data) = make_blob(seed ^ n ^ (j as u64), size);
                        let expected_len = data.len();
                        futs.push(Box::pin(async move {
                            cas.update_oneshot(digest, data)
                                .await
                                .expect("R1 cold write must succeed");
                            let bytes = cas
                                .get_part_unchunked(digest, 0, None)
                                .await
                                .expect("R1 cold read must succeed");
                            assert_eq!(bytes.len(), expected_len);
                        }));
                    }
                }
                futures::future::join_all(futs).await;
            }
        },
    )
    .await
}
