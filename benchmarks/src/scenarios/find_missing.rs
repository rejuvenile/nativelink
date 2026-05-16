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

use core::pin::Pin;
use std::collections::BTreeMap;

use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreDriver, StoreKey, StoreLike};

use crate::composition::{build_prod_cas_composition, prod_defaults};
use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob, measure};

/// Batch sizes worth measuring. Bazel's FMB batch is variable — a
/// fresh build often emits hundreds of digests per call; an incremental
/// build emits handfuls.
const F1_BATCH_SIZES: &[(usize, &str)] = &[
    (1, "1"),
    (16, "16"),
    (128, "128"),
    (1024, "1024"),
];

pub async fn run(opts: &RunOpts) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(50);

    let composition = build_prod_cas_composition(
        prod_defaults::FAST_MEMORY_MAX_BYTES,
        prod_defaults::SLOW_WRITES_INFLIGHT_MAX_BYTES,
    )
    .await
    .expect("build_prod_cas_composition for F1");

    // Prepopulate one digest pool that will be the "known" set for the
    // cache-hit cells.
    let mut known_digests: Vec<DigestInfo> = Vec::with_capacity(1024);
    for j in 0..1024u64 {
        let (digest, data) = make_blob(0xF1_C0DE_A11 ^ j, 1024);
        composition
            .cas_store
            .update_oneshot(digest, data)
            .await
            .expect("F1 prepopulate must succeed");
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
            let mut missing = Vec::with_capacity(batch);
            for j in 0..(batch as u64) {
                missing.push(DigestInfo::new(
                    {
                        let mut buf = [0u8; 32];
                        buf[..8].copy_from_slice(&j.to_le_bytes());
                        buf[8..16].copy_from_slice(&0xDEAD_BEEF_u64.to_le_bytes());
                        buf
                    },
                    999,
                ));
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

    let keys: Vec<StoreKey<'static>> = digests
        .iter()
        .map(|d| StoreKey::from(*d))
        .collect();
    let cas_clone = cas.clone();

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
            let keys = keys.clone();
            async move {
                let mut results = vec![None; keys.len()];
                StoreDriver::has_with_results(
                    Pin::new(cas.inner_store::<StoreKey<'_>>(None)),
                    &keys,
                    &mut results,
                )
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
