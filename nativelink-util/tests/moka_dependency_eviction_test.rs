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

//! Regression guard on the moka DEPENDENCY (not on our wrapper).
//!
//! # What this protects
//!
//! moka-rs/moka#590: a cache configured with the non-default
//! `EvictionPolicy::lru()` could reach a state where size eviction stops
//! PERMANENTLY. A stale `WriteOp::Upsert` for an entry whose CHT slot was
//! already removed reached `handle_admit` and pushed an "orphan" deque node;
//! once that orphan reached the front of the probation deque,
//! `evict_lru_entries` re-peeked it forever, evicted nothing, and reported no
//! progress. `weighted_size` then grew without bound past `max_capacity` for
//! the life of the process, with no error, no panic, and no log line.
//!
//! In production this wedged two workers at 3.3x their configured byte cap for
//! four days until they exhausted their disk budget and began rejecting writes
//! (FINDING 2, 2026-07-28). Fixed upstream in moka 0.12.16 by
//! moka-rs/moka#592, which retires an entry atomically with its CHT unlink and
//! short-circuits any later stale op. `nativelink-util/Cargo.toml` pins
//! `=0.12.16`.
//!
//! # Why this test targets raw `moka::sync::Cache` and not `MokaEvictingMap`
//!
//! Our wrapper carries a self-healing fallback evictor
//! (`maybe_selfheal_wedged_eviction`) whose entire purpose is to converge a
//! wedged cache by bypassing the deque. A wrapper-level test therefore CANNOT
//! distinguish "moka is fixed" from "moka is broken and our self-heal
//! compensated" — it would pass either way, which is exactly the shape of
//! assertion that passes for the wrong reason. Testing the dependency directly
//! is the only way to observe the property this guards.
//!
//! Asserting the version string instead would be weaker still: it tests the
//! pin, not the behavior, and would tell us nothing if a future moka release
//! regressed the fix.
//!
//! # Configuration fidelity
//!
//! The cache below mirrors `MokaEvictingMap::with_anchor`'s production
//! builder, because each of these is load-bearing for reaching the bug:
//!   * `EvictionPolicy::lru()` — the #590 trigger condition. We cannot escape
//!     it by switching to moka's default TinyLFU: TinyLFU's admission filter
//!     rejects a new entry on a frequency tie, which silently drops
//!     freshly-written blobs in a content-addressed store (see the builder
//!     comment in `moka_evicting_map.rs`).
//!   * a KB-granularity weigher (`len().div_ceil(1024)`) and a byte-derived
//!     `max_capacity`, so eviction is size-driven rather than count-driven.
//!   * an eviction listener — its presence flips
//!     `is_removal_notifier_enabled()`, which gates moka's per-key locking in
//!     `invalidate_with_hash`, so omitting it would exercise a different code
//!     path than production.
//!
//! # Test power
//!
//! The wedge is a race, so reproduction is probabilistic and these tests are
//! sized for reliable reproduction rather than speed. Measured on this host by
//! temporarily re-pinning moka to the broken 0.12.15 (mutation log:
//! `/tmp/mokabump-mutation-015.log`), 5 runs of each test:
//!
//! | mode | 0.12.15 | 0.12.16 |
//! |---|---|---|
//! | plain insert churn | 5/5 STALLED at 98.08-114.11x cap | 5/5 at cap |
//! | invalidate + reinsert churn | 5/5 STALLED at 3.16-3.28x cap | 5/5 at cap |
//!
//! The invalidate mode's 3.2x overshoot is close to the 3.36x the production
//! workers were found at; the insert mode wedges earlier in the run and so
//! ratchets much further before the writers finish.
//!
//! If either test is ever made smaller, RE-MEASURE that red rate against
//! 0.12.15 — a shrunk workload that no longer reproduces the stall is a test
//! that passes vacuously, which is the failure mode this whole file exists to
//! prevent elsewhere.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use moka::policy::EvictionPolicy;
use moka::sync::Cache;
use nativelink_macro::nativelink_test;

/// Byte cap for the cache under test.
const CAP_BYTES: u64 = 4 * 1024 * 1024;
/// Weigher scale — matches `MokaEvictingMap`'s KB granularity.
const SCALE: u64 = 1024;
/// Size of each cached value. Large relative to the cap so that every insert
/// applies real eviction pressure (~64 entries fit).
const VALUE_BYTES: usize = 64 * 1024;
/// Reused key pool. Key REUSE is required: the wedge needs the key to be
/// re-inserted and still present, so that `skip_updated_entry_ao` takes its
/// key-PRESENT branch and moves the map entry's node instead of the peeked
/// orphan.
const KEY_POOL: u64 = 200;
const CHURN_THREADS: u64 = 4;
const PRESSURE_THREADS: u64 = 4;
const ITERS: u64 = 2_000;
/// Quiesce passes after the writers join. With no concurrent mutation left,
/// any residual overshoot is a genuine failure to evict, not a transient
/// backlog.
const QUIESCE_PASSES: u32 = 256;

/// Whether churn writers explicitly invalidate before re-inserting.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ChurnMode {
    /// Mirrors our pin/unpin cycle: `pin_key_with_mode` is a
    /// `cache.invalidate`, `unpin_key` a bare `cache.insert`.
    InvalidateThenInsert,
    /// Plain `insert` on a reused pool, with NO explicit invalidation — here
    /// the CHT slot is removed by size eviction instead. This is upstream's
    /// regression shape, and it is the one that refutes our original
    /// "the wedge is specific to our pin path" conclusion.
    InsertOnly,
}

fn build_cache(evictions: Arc<AtomicU64>) -> Cache<u64, Vec<u8>> {
    Cache::builder()
        .eviction_policy(EvictionPolicy::lru())
        .max_capacity(CAP_BYTES / SCALE)
        .weigher(|_k: &u64, v: &Vec<u8>| -> u32 {
            u32::try_from((v.len() as u64).div_ceil(SCALE)).unwrap_or(u32::MAX)
        })
        .eviction_listener(move |_k, _v, _cause| {
            evictions.fetch_add(1, Ordering::Relaxed);
        })
        .build()
}

/// Drive the churn + pressure workload, quiesce, and return
/// `(weighted_size_in_weight_units, entry_count, eviction_count)`.
fn run_workload(mode: ChurnMode) -> (u64, u64, u64) {
    let evictions = Arc::new(AtomicU64::new(0));
    let cache = build_cache(Arc::clone(&evictions));

    std::thread::scope(|scope| {
        for t in 0..CHURN_THREADS {
            let cache = cache.clone();
            scope.spawn(move || {
                for i in 0..ITERS {
                    // Reused pool, deliberately overlapping across threads so
                    // several writers race on the same keys.
                    let key = (i + t) % KEY_POOL;
                    if mode == ChurnMode::InvalidateThenInsert {
                        cache.invalidate(&key);
                    }
                    cache.insert(key, vec![0u8; VALUE_BYTES]);
                }
            });
        }
        for t in 0..PRESSURE_THREADS {
            let cache = cache.clone();
            scope.spawn(move || {
                // Unique keys, never reused, to keep the eviction loop hot so
                // the orphan (if one is minted) is reached and re-peeked.
                let base = KEY_POOL + 1 + t * ITERS;
                for i in 0..ITERS {
                    cache.insert(base + i, vec![0u8; VALUE_BYTES]);
                }
            });
        }
    });

    for _ in 0..QUIESCE_PASSES {
        cache.run_pending_tasks();
    }

    (
        cache.weighted_size(),
        cache.entry_count(),
        evictions.load(Ordering::Relaxed),
    )
}

/// Shared assertion so both modes fail with the same diagnostic shape.
fn assert_converged(mode_name: &str, weighted: u64, entries: u64, evictions: u64) {
    let cap_units = CAP_BYTES / SCALE;
    let overshoot_ratio = weighted as f64 / cap_units as f64;
    assert!(
        weighted <= cap_units,
        "moka size eviction STALLED ({mode_name}): after {QUIESCE_PASSES} quiesce passes with no \
         concurrent mutation, weighted_size is {weighted} weight-units ({overshoot_ratio:.2}x the \
         {cap_units}-unit cap), entry_count={entries}, evictions={evictions}. This is the \
         moka-rs/moka#590 eviction livelock — an orphaned deque node is parked at the front of \
         the probation queue and nothing will ever be evicted again for the life of this cache. \
         The pinned moka must carry the #592 fix; check nativelink-util/Cargo.toml (=0.12.16) and \
         re-trace the self-heal's deque-independence hops in moka_evicting_map.rs before \
         relaxing that pin."
    );
    // Guards the other direction: a cache that evicted EVERYTHING would also
    // satisfy the cap. The workload inserts far more than the cap holds, so a
    // converged cache must retain a non-trivial resident set.
    assert!(
        entries > 0 && weighted > 0,
        "moka evicted the entire cache ({mode_name}): weighted_size={weighted}, \
         entry_count={entries}. The convergence assertion above would pass vacuously on an \
         empty cache, so this is a real failure, not a stricter bound."
    );
}

/// Upstream's regression shape: plain insert churn on a reused key pool, with
/// ZERO `invalidate` calls. Reproduces #590 on 0.12.15 — which is what refuted
/// our original conclusion that only our pin path could mint the orphan.
#[nativelink_test]
async fn moka_lru_size_eviction_converges_under_insert_churn() {
    let (weighted, entries, evictions) =
        tokio::task::spawn_blocking(|| run_workload(ChurnMode::InsertOnly))
            .await
            .expect("workload thread must not panic");
    assert_converged("plain insert churn", weighted, entries, evictions);
}

/// Our pin/unpin cycle's shape: invalidate-then-reinsert on a reused key pool.
/// This is the churn the production wedge ran on.
#[nativelink_test]
async fn moka_lru_size_eviction_converges_under_invalidate_reinsert_churn() {
    let (weighted, entries, evictions) =
        tokio::task::spawn_blocking(|| run_workload(ChurnMode::InvalidateThenInsert))
            .await
            .expect("workload thread must not panic");
    assert_converged("invalidate + reinsert churn", weighted, entries, evictions);
}
