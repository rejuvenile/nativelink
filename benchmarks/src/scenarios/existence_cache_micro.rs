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

//! Flow C1: ExistenceCacheStore micro-bench.
//!
//! Per design Section 1 (`.claude/audits/495-data-plane-benchmark-design-2026-05-16.md`):
//!
//! > C1 | ExistenceCache lookup | `existence_cache_store.rs:289 exists_in_cache` +
//! > `has_with_results` | pure CPU-bound; should be O(50ns) per key in the hot path
//! > | micro-bench scope
//!
//! This anchors the per-key ExistenceCache hot-path latency that gates
//! Bazel's FindMissingBlobs throughput. The F1 (`find_missing.rs`)
//! scenario already exercises ECS through the full prod composition
//! (Verify → ExistenceCache → SizePartitioning → ...) at batch granularity
//! (1 / 16 / 128 / 1024 keys per call). C1 instead targets the per-key
//! arithmetic of ECS itself — minus the wrappers above it and the
//! `SizePartitioning + FastSlow` underneath. A divergence between F1's
//! per-key-amortized cost and C1's measured per-key cost localizes
//! regressions to either the ECS layer or one of the wrappers.
//!
//! **Composition cut vs Phase 1 (intentional):** the C1 cells construct
//! `ExistenceCacheStore` directly atop a slim `MemoryStore`, NOT inside
//! the prod wrapper chain. The design doc explicitly tags C1 as
//! `micro-bench scope`. The four cells emit
//! `extras.composition_deviation = "micro_no_wrappers"` so diff tooling
//! can flag them as not-fully-prod-shape (matches the existing
//! `small_cas_redis_replaced_with_memory` convention from
//! `legacy_write.rs::run_one_cell`).
//!
//! **Cells (4):**
//!
//! - `c1_exists_in_cache_single_key_hit` — `ExistenceCacheStore::exists_in_cache`
//!   on a pre-populated digest. Pure cache hit; never falls through to inner.
//!   Anchors the single-key Bazel-style "is this blob known?" probe.
//! - `c1_exists_in_cache_single_key_miss` — same API, digest never seen.
//!   Falls through to inner `MemoryStore::has_with_results` (also hot,
//!   but exercises the not-in-cache → inner-query → insert-into-cache
//!   path). Compare against the hit cell to anchor the cache-miss penalty.
//! - `c1_has_with_results_batch16_hit` — `StoreDriver::has_with_results`
//!   with 16 known digests (all hit). Anchors the batch hot-path
//!   per-key amortized cost at a Bazel-typical incremental-build size.
//! - `c1_has_with_results_batch128_hit` — same with 128 known digests.
//!   Bazel fresh-build batch size; anchors the batch hot-path at the
//!   FMB-busy scale.
//!
//! Throughput is reported in `ElementsPerSec` (keys/sec) — bytes-per-sec
//! is meaningless for a probe operation that returns Option<size> not
//! payload. Mirrors `find_missing.rs`'s F1 schema.
//!
//! **Iter count:** ECS hot path is O(50ns) per key per design doc. At
//! 20 iters of 16-key batches the total measured wall is microseconds;
//! per-iter jitter dominates. The C1 default-iter override
//! (`opts.effective_iters(2000)`) gets the measurement window into the
//! ms range so percentile arithmetic has signal. `--fast` still collapses
//! to `FAST_MODE_ITERS` (3) for smoke runs.
//!
//! **Invariant being anchored:** the per-key wall-clock and CPU cost of
//! the ECS hot path. A regression here flags any change that adds work
//! to `exists_in_cache` / `has_with_results` (e.g. a lock added to the
//! moka key-walk, an extra clone per key, a second-pass scan).
//!
//! **What this cell does NOT measure (honest scope-cut):**
//!
//! - **Eviction-callback latency.** ECS registers an `ItemCallback` on
//!   its inner store so an inner-store eviction synchronously removes
//!   the key from the cache. The MemoryStore inner here NEVER evicts
//!   (no `EvictionPolicy` set on the warm cells; populate budget tiny
//!   compared to default cap), so the callback fires only on explicit
//!   `remove_from_cache`. To anchor callback wall-clock, a Phase 3 cell
//!   would set a tight `evict_bytes` on the inner Memory and time the
//!   eviction sweep. Out of scope for the Phase 2 first-scenario.
//! - **Concurrent reader contention.** All cells run single-task; the
//!   `MokaEvictingMap` is a sharded structure and concurrent-reader
//!   contention would require N task spawns and a barrier. Phase 3
//!   adds a `concurrency=N` axis per the design doc Section 2; the
//!   first scenario sticks to `concurrency=1` to keep the harness
//!   surface minimal.
//! - **Cache-thrash / LRU sweep.** Default `EXISTENCE_CACHE_MAX_ENTRIES`
//!   is 50M (prod) — vastly above the populate budget here. The cells
//!   do NOT measure LRU-eviction-under-pressure. A Phase 3 cell would
//!   need to flood the cache past capacity and time the steady-state
//!   admission-with-eviction loop.

use std::collections::BTreeMap;
use std::sync::Arc;

use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, MemorySpec, StoreSpec,
};
use nativelink_error::Error;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};

use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob_with_indices, measure};

/// Per-cell iter default. ECS hot path is O(50ns) per key per design
/// doc Section 1. At 2000 iters of single-key cells the measured window
/// is ~100 µs; at batch 128 cells it's ~12 ms. Both comfortably above
/// the per-iter jitter floor.
const C1_DEFAULT_ITERS: u32 = 2000;

/// Number of pre-populated digests held in the cache for hit-path cells.
/// Sized to be well below the default `MemoryStore` cap so populate
/// itself can't trigger eviction-induced callback invalidations.
const C1_POPULATE_COUNT: u32 = 4096;

/// Cap the inner `MemoryStore` so populate cost is bounded but with
/// headroom over `C1_POPULATE_COUNT`. 4096 × 16 B per digest = 64 KiB;
/// 64 MiB cap is 1000× headroom — no eviction can fire during populate.
const C1_INNER_MEMORY_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Build a standalone `ExistenceCacheStore` wrapping a slim
/// `MemoryStore`. Returns the typed `Arc<ExistenceCacheStore>` so the
/// scenario can call `exists_in_cache` directly (it's NOT on the
/// `StoreDriver` trait), AND a `Store` handle for the batch
/// `has_with_results` path (which IS on the trait).
fn build_existence_cache() -> Result<
    (
        Arc<ExistenceCacheStore<std::time::SystemTime>>,
        Store,
    ),
    Error,
> {
    let inner_spec = MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: C1_INNER_MEMORY_MAX_BYTES,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    };
    // `MemoryStore::new` returns `Arc<Self>` directly without going
    // through the factory; ExistenceCacheStore::new wants a `Store`
    // wrapper so we wrap once here.
    let memory: Arc<MemoryStore> = MemoryStore::new(&inner_spec);
    let inner_store = Store::new(memory);
    // ECS uses the prod `EXISTENCE_CACHE_MAX_ENTRIES` (50M) — far above
    // C1_POPULATE_COUNT (4096), so no LRU eviction fires during the
    // hot path. The cell is intentionally NOT measuring eviction.
    let ecs_spec = ExistenceCacheSpec {
        backend: StoreSpec::Memory(inner_spec.clone()),
        eviction_policy: Some(EvictionPolicy {
            max_count: 50_000_000,
            ..Default::default()
        }),
    };
    let ecs = ExistenceCacheStore::new(&ecs_spec, inner_store);
    // Hand back two views of the same store: the typed Arc for direct
    // `exists_in_cache` calls (which the trait doesn't expose), and a
    // `Store` for the batch `has_with_results` calls that flow through
    // the trait.
    let store_view = Store::new(ecs.clone());
    Ok((ecs, store_view))
}

/// Pre-populate the cache with `C1_POPULATE_COUNT` digests. Returns the
/// digest list so cells can `StoreKey::from(d)` them at will. The
/// payload bytes are tiny (16 B) — we only need the digests in the
/// inner store; the cell never reads payloads.
async fn populate(ecs: &Store) -> Result<Vec<DigestInfo>, Error> {
    let mut digests = Vec::with_capacity(C1_POPULATE_COUNT as usize);
    for j in 0..C1_POPULATE_COUNT {
        // Distinct per-scenario-name seeding via `c1_populate` so a
        // future scenario reusing `make_blob_with_indices` cannot
        // collide with C1's populate set.
        let (digest, data) = make_blob_with_indices("c1_populate", 0, j, 16);
        ecs.update_oneshot(digest, data).await?;
        digests.push(digest);
    }
    Ok(digests)
}

pub async fn run(opts: &RunOpts) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(C1_DEFAULT_ITERS);

    let (ecs_typed, ecs_store) = match build_existence_cache() {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("[bench] C1 build failed: {e:?}");
            return out;
        }
    };
    let known_digests = match populate(&ecs_store).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[bench] C1 populate failed: {e:?}");
            return out;
        }
    };

    // Cell 1: single-key exists_in_cache HIT.
    let hit_name = "c1_exists_in_cache_single_key_hit";
    if opts.matches(hit_name) {
        out.push(run_exists_hit(ecs_typed.clone(), &known_digests, iters, hit_name).await);
    }

    // Cell 2: single-key exists_in_cache MISS — digest never seen. The
    // ECS falls through to inner.has_with_results AND inserts the
    // result; that insert grows the cache by 1 per iter. To prevent
    // cache state from polluting cell 1 if cells run in any order,
    // build a FRESH cache for the miss cell so the populate set is
    // never disturbed.
    let miss_name = "c1_exists_in_cache_single_key_miss";
    if opts.matches(miss_name) {
        match build_existence_cache() {
            Ok((miss_typed, _miss_store)) => {
                out.push(run_exists_miss(miss_typed, iters, miss_name).await);
            }
            Err(e) => {
                eprintln!("[bench] C1 miss cell build failed: {e:?}");
            }
        }
    }

    // Cell 3 + 4: batch has_with_results HIT at 16 and 128 keys.
    // These use the SHARED populated cache (no state mutation on hit
    // path since every key already cached).
    for &(batch, label) in &[(16u32, "16"), (128u32, "128")] {
        let name = format!("c1_has_with_results_batch{label}_hit");
        if !opts.matches(&name) {
            continue;
        }
        out.push(run_has_batch_hit(&ecs_store, &known_digests[..batch as usize], iters, &name).await);
    }

    out
}

async fn run_exists_hit(
    ecs: Arc<ExistenceCacheStore<std::time::SystemTime>>,
    known: &[DigestInfo],
    iters: u32,
    scenario_name: &str,
) -> BenchmarkResult {
    let mut extras = BTreeMap::new();
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!("micro_no_wrappers"),
    );
    extras.insert(
        "populate_count".to_string(),
        serde_json::json!(C1_POPULATE_COUNT),
    );
    extras.insert("hit_path".to_string(), serde_json::json!(true));

    // Round-robin across the populated digest set so we don't measure
    // a single-cacheline-hot artifact — moka shards by hash, hitting
    // the same digest N times collapses to one shard's local code path.
    let digests: Arc<Vec<DigestInfo>> = Arc::new(known.to_vec());
    let iter_counter = std::sync::atomic::AtomicU64::new(0);
    let len = digests.len() as u64;

    measure(
        "C1",
        scenario_name,
        None,
        1,
        CacheState::Warm,
        iters,
        None,
        Some(1),
        extras,
        move || {
            let ecs = ecs.clone();
            let digests = digests.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let digest = digests[(n % len) as usize];
                let present = ecs.exists_in_cache(&digest).await;
                assert!(
                    present,
                    "C1 hit cell: pre-populated digest must always be in cache — \
                     got `false` (cache state corrupted or populate sequence \
                     never reached this digest)"
                );
            }
        },
    )
    .await
}

async fn run_exists_miss(
    ecs: Arc<ExistenceCacheStore<std::time::SystemTime>>,
    iters: u32,
    scenario_name: &str,
) -> BenchmarkResult {
    let mut extras = BTreeMap::new();
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!("micro_no_wrappers"),
    );
    extras.insert("hit_path".to_string(), serde_json::json!(false));
    // Generate one fresh digest per iter — distinct from any populated
    // digest. Using `c1_miss` as the scenario seed guarantees no
    // collision with `c1_populate`'s seed pool.
    let pregen: Arc<Vec<DigestInfo>> = Arc::new(
        (0..iters)
            .map(|n| make_blob_with_indices("c1_miss", n as u64, 0, 16).0)
            .collect(),
    );
    let iter_counter = std::sync::atomic::AtomicU64::new(0);

    measure(
        "C1",
        scenario_name,
        None,
        1,
        CacheState::Cold,
        iters,
        None,
        Some(1),
        extras,
        move || {
            let ecs = ecs.clone();
            let pregen = pregen.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let digest = pregen[n as usize];
                // First call: cache miss. ECS falls through to inner
                // (which also returns None since the digest is fresh),
                // then `inner_has_with_results` does NOT insert into
                // the cache (only Some(size) results trigger an insert
                // per `existence_cache_store.rs:333-342`). So this
                // path measures the pure miss-cost: hash → not-found
                // → inner.has_with_results → return false.
                let present = ecs.exists_in_cache(&digest).await;
                assert!(
                    !present,
                    "C1 miss cell: fresh digest must NEVER be in cache — \
                     got `true` (digest collided with populate set or \
                     cache state leaked across iterations)"
                );
            }
        },
    )
    .await
}

async fn run_has_batch_hit(
    ecs: &Store,
    digests: &[DigestInfo],
    iters: u32,
    scenario_name: &str,
) -> BenchmarkResult {
    let mut extras = BTreeMap::new();
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!("micro_no_wrappers"),
    );
    extras.insert(
        "batch_size".to_string(),
        serde_json::json!(digests.len() as u64),
    );
    extras.insert("hit_path".to_string(), serde_json::json!(true));

    // Pre-build the StoreKey vec OUTSIDE the timed body. Allocating N
    // keys per iter would dominate the per-key cost at small N (16) —
    // dwarfing the actual ECS lookup we care about. Same discipline
    // as F1's `keys_arc` pre-allocation.
    let keys: Arc<Vec<StoreKey<'static>>> =
        Arc::new(digests.iter().map(|d| StoreKey::from(*d)).collect());
    let elements_per_iter = digests.len() as u64;
    let buf_len = digests.len();
    let ecs = ecs.clone();

    measure(
        "C1",
        scenario_name,
        None,
        1,
        CacheState::Warm,
        iters,
        None,
        Some(elements_per_iter),
        extras,
        move || {
            let ecs = ecs.clone();
            let keys = keys.clone();
            // Fresh `results` per iter; vec alloc is part of the
            // callsite contract for has_with_results (same justification
            // as F1's `results` buffer at `find_missing.rs:148`).
            let mut results: Vec<Option<u64>> = vec![None; buf_len];
            async move {
                ecs.has_with_results(&keys, &mut results)
                    .await
                    .expect("C1 batch hit must succeed");
                debug_assert!(
                    results.iter().all(Option::is_some),
                    "C1 batch hit: every digest in the populated subset must \
                     return Some(size) — got at least one None (cache state \
                     corrupted or populate sequence did not reach all batch \
                     digests)"
                );
            }
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity: the cell-default iter (`C1_DEFAULT_ITERS`) must stay
    /// well above `Confidence::Medium` (>= 20) so a default-iter run
    /// produces percentile numbers worth diffing. Mutation: drop
    /// `C1_DEFAULT_ITERS` below 20 — this red-fails.
    #[test]
    fn c1_default_iters_clear_medium_confidence() {
        assert!(
            C1_DEFAULT_ITERS >= 20,
            "C1 cells target nanosecond-scale ops; iters >= 20 needed for \
             percentile signal (got {C1_DEFAULT_ITERS}). Below 20 the p99 \
             degenerates to `max` and the diff loses meaning."
        );
        // Also assert the populate count is large enough that the
        // round-robin in `run_exists_hit` actually visits multiple
        // digests within one iter pass; otherwise the cell measures
        // the hot path on ONE moka shard, not the cache as a whole.
        assert!(
            C1_POPULATE_COUNT >= 64,
            "C1 populate count must be >= 64 so round-robin in \
             run_exists_hit hits multiple moka shards within one iter \
             pass (got {C1_POPULATE_COUNT}). Below 64 the cell may \
             measure a single-shard artifact."
        );
    }

    /// Build a real ECS + populate + exercise the hit path. Smoke test
    /// against the same composition the bench scenarios use.
    #[tokio::test]
    async fn c1_hit_cell_smoke() {
        let (ecs_typed, ecs_store) = build_existence_cache().expect("build");
        let digests = populate(&ecs_store).await.expect("populate");
        assert_eq!(digests.len() as u32, C1_POPULATE_COUNT);
        // Probe the first and last digests to anchor the
        // round-robin scheme. If `populate`'s seeding regresses to
        // a collision, every probe returns the SAME shard which
        // is not what the cell intends to measure.
        assert!(ecs_typed.exists_in_cache(&digests[0]).await);
        assert!(
            ecs_typed
                .exists_in_cache(digests.last().expect("populate set non-empty"))
                .await
        );
    }

    /// Build a real ECS, probe a never-populated digest — must return
    /// `false`. Catches a regression where the cell-2 assertion would
    /// fire spuriously (e.g. if `c1_miss` seed collided with
    /// `c1_populate`).
    #[tokio::test]
    async fn c1_miss_cell_seed_disjoint_from_populate() {
        let (ecs_typed, ecs_store) = build_existence_cache().expect("build");
        let _populated = populate(&ecs_store).await.expect("populate");
        // Generate the first miss digest the way the cell would.
        let (miss_digest, _) = make_blob_with_indices("c1_miss", 0, 0, 16);
        let present = ecs_typed.exists_in_cache(&miss_digest).await;
        assert!(
            !present,
            "c1_miss seed must yield digests disjoint from c1_populate's \
             pool — a collision here makes the miss cell measure the hit \
             path and quietly degrade the baseline"
        );
    }
}
