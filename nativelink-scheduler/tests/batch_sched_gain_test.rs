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

//! Tests for the OBSERVABILITY-ONLY batch-scheduling subtree-assignment-delta
//! probe (`#batch-sched` / M1-replay), the CORRECT replacement for
//! `colocation_surplus` (which keyed on the exact input-root digest — a
//! near-always-0 signal for builds; design
//! `.claude/audits/batch-scheduling-subtree-assignment-metric-design-2026-07-02.md`).
//!
//! MODEL (M1-replay): the counterfactual FAITHFULLY REPLAYS the live M1
//! P-headroom gate. The contention is the FRESH in-flight count (`running`)
//! against `p_core_count` cache-tier eligibility — NOT an `max_inflight_tasks`
//! slot budget. The gate is CONFIRMED ON in prod (25,303 exclusion events since
//! boot) at `idle_threshold_pct == 0` (v1 behavior). Every action is placed;
//! when no viable worker has p_headroom the gate LIFTS and cache-tier opens to
//! all.
//!
//!   GREEDY G — walk actions in priority order; each takes the CACHE-ELIGIBLE
//!   worker that maximizes `s(i,j) − load_penalty_j`, then that worker's
//!   `running` += 1 (so it can lose p_headroom for the next action); sum chosen
//!   `s`. `load_penalty` is CONSTANT (the gate is the contention, not a ramp).
//!   BATCH B — global greedy-max-weight over CACHE-ELIGIBLE workers with
//!   intra-batch WARMING and the same gate re-evaluation; ranks by raw `s`.
//!   gain_pct = (B − G) / G * 100 (u64, guard G==0); B := max(B_heuristic, G).
//!
//! These are PURE functions so the exact assignment arithmetic is unit-testable
//! WITHOUT driving a live scheduler. Telemetry-only: nothing here asserts a
//! scheduling behavior change (there is none — the real dispatch stays greedy).
//!
//! SCOPE NOTE (why this file is thinner than its pre-M1-replay version): the
//! prior suite's fixtures (`g_zero_guard…`, `batch_beats_greedy_positive_gain`,
//! `colocation_warming_two_on_one_worker_positive_gain`,
//! `greedy_already_optimal_zero_gain`, `batch_respects_worker_capacity`) all
//! keyed contention on the RETIRED `max_inflight_tasks`-derived `capacity` slot
//! budget — the wrong model (unbounded `max_inflight_tasks` in prod → no
//! contention → structural gain 0). Those are RETIRED here; the gate-driven
//! equivalents (contended-band gain, Phase-2 lift, fresh-count recompute) live
//! as prod-shape tests in `simple_scheduler::batch_sched_gain_test` and the
//! end-to-end seam test `api_worker_scheduler::tests::
//! batch_sched_gain_flows_through_real_probe`. This file keeps the
//! MODEL-INDEPENDENT properties (constant-parity, overlap calc, empty-input
//! guard, and the `B ≥ G` lower-bound property sweep) migrated to the gate model.

use std::collections::{HashMap, HashSet};

use nativelink_scheduler::simple_scheduler::{
    BatchSchedAction, BatchSchedGateCfg, BatchSchedWorker, PER_FILE_WEIGHT,
    compute_batch_sched_gain,
};
use nativelink_util::common::DigestInfo;

/// The live-replay gate config used by these tests: enabled, threshold 0 (v1 —
/// matching the 25,303-exclusion prod data), factor 2 (prod default; inert at
/// threshold 0).
const GATE_ON_V1: BatchSchedGateCfg = BatchSchedGateCfg {
    enabled: true,
    idle_threshold_pct: 0,
    override_factor: 2,
};

/// The probe's `PER_FILE_WEIGHT` MUST match the production Tier-1.5 dispatch
/// constant (`api_worker_scheduler.rs`, 100 KB) — a divergent weight makes the
/// counterfactual score against a fiction. Pinned at the declaration value.
#[test]
fn per_file_weight_matches_dispatch() {
    assert_eq!(
        PER_FILE_WEIGHT,
        100 * 1024,
        "#batch-sched: PER_FILE_WEIGHT must be 100 KB to match the production Tier-1.5 \
         blend in api_worker_scheduler.rs; a drift here silently biases the (B-G) delta"
    );
}

/// A distinct directory digest from a single discriminator byte.
fn dir(discriminator: u8) -> DigestInfo {
    DigestInfo::new([discriminator; 32], 1)
}

/// Build a sampled action from a set of `(dir_digest, direct_bytes, direct_files)`
/// tuples. `dir_digests` is the membership set; `dir_direct_*` carry the disjoint
/// per-directory weight (mirrors `ResolvedTree`).
fn action(dirs: &[(u8, u64, u64)]) -> BatchSchedAction {
    let mut dir_digests = HashSet::new();
    let mut dir_direct_bytes = HashMap::new();
    let mut dir_direct_files = HashMap::new();
    for &(d, bytes, files) in dirs {
        let dd = dir(d);
        dir_digests.insert(dd);
        dir_direct_bytes.insert(dd, bytes);
        dir_direct_files.insert(dd, files);
    }
    BatchSchedAction {
        dir_digests,
        dir_direct_bytes,
        dir_direct_files,
    }
}

/// Build an M1-replay worker: cached subtree digests, seeded fresh `running`
/// count, `p_core_count`, `p_core_load_pct`, and a CONSTANT `load_penalty`.
fn worker(
    cached: &[u8],
    running: u64,
    p_core_count: u32,
    p_core_load_pct: u32,
    load_penalty: i64,
) -> BatchSchedWorker {
    BatchSchedWorker {
        cached_subtree_digests: cached.iter().map(|&d| dir(d)).collect(),
        running,
        p_core_count,
        // (#sched-work-conservation) e_core_count = 0 → total-core term collapses
        // to `running < p_core_count`; these gauge fixtures are unchanged.
        e_core_count: 0,
        p_core_load_pct,
        load_penalty,
    }
}

// ─────────────────────────── coverage counting / guards ─────────────────────

/// Empty inputs yield a well-defined zero (no divide-by-zero, no panic).
#[test]
fn empty_inputs_are_zero_not_panic() {
    let g_no_actions =
        compute_batch_sched_gain(&[], &[worker(&[b'a'], 0, 4, 10, 0)], GATE_ON_V1);
    assert_eq!(
        g_no_actions.gain_pct, 0,
        "#batch-sched: no actions → gain_pct 0 (G==0 guard), no panic"
    );
    assert_eq!(
        g_no_actions.sample_actions, 0,
        "#batch-sched: sample_actions must be 0 when no actions given"
    );

    let g_no_workers =
        compute_batch_sched_gain(&[action(&[(b'a', 100, 0)])], &[], GATE_ON_V1);
    assert_eq!(
        g_no_workers.gain_pct, 0,
        "#batch-sched: no workers → gain_pct 0, no panic"
    );
    assert_eq!(
        g_no_workers.sample_workers, 0,
        "#batch-sched: sample_workers must be 0 when no workers given"
    );
    assert_eq!(
        g_no_workers.mean_seed_running, 0,
        "#batch-sched: mean_seed_running must be 0 when no workers given (no divide-by-zero)"
    );
}

// ─────────────────────── greedy already optimal → gain 0 ────────────────────

/// When greedy priority-order already achieves the global max (each action's
/// best worker is distinct, free, and p_headroom-eligible), batch cannot improve
/// → gain_pct 0. Gate-model version: both workers have p_headroom
/// (`running 0 < p_core 4`), so cache-tier is open and greedy reaches each
/// action's unique holder.
#[test]
fn greedy_already_optimal_zero_gain() {
    let a0 = action(&[(b'a', 500_000, 0)]);
    let a1 = action(&[(b'x', 700_000, 0)]);
    let w0 = worker(&[b'a'], 0, 4, 10, 0);
    let w1 = worker(&[b'x'], 0, 4, 10, 0);

    let g = compute_batch_sched_gain(&[a0, a1], &[w0, w1], GATE_ON_V1);

    // Greedy: A0 → W0 (500_000), A1 → W1 (700_000). G = 1_200_000.
    // Batch: same assignment (each pairs with its unique big match). B = G.
    assert_eq!(
        g.gain_pct, 0,
        "#batch-sched: greedy priority-order already reaches the global optimum (distinct \
         best workers, both p_headroom-eligible) → gain_pct 0; got {}",
        g.gain_pct
    );
    assert_eq!(
        g.greedy_score, 1_200_000,
        "#batch-sched: greedy captures both unique holders → G=1_200_000; got {}",
        g.greedy_score
    );
}

// ───────────────────────────── subtree_overlap_pct calc ─────────────────────

/// `subtree_overlap_pct` = Σ dir_direct_bytes over dir digests appearing in ≥2
/// sampled actions' dir_digests, / total sampled subtree bytes (workers-agnostic).
/// Known layout:
///   A0 = {p:300, q:100}; A1 = {p:300, r:200}; A2 = {s:400}
///   Shared dirs (≥2 actions): only `p` (in A0 and A1). Its direct bytes counted
///   ONCE = 300. Total sampled subtree bytes = 300+100+300+200+400 = 1300.
///   overlap = 300*100/1300 = 23 (integer floor).
#[test]
fn subtree_overlap_pct_known_layout() {
    let a0 = action(&[(b'p', 300, 0), (b'q', 100, 0)]);
    let a1 = action(&[(b'p', 300, 0), (b'r', 200, 0)]);
    let a2 = action(&[(b's', 400, 0)]);
    // One p_headroom worker so the assignment runs; overlap is worker-independent.
    let w0 = worker(&[], 0, 4, 10, 0);

    let g = compute_batch_sched_gain(&[a0, a1, a2], &[w0], GATE_ON_V1);

    assert_eq!(
        g.subtree_overlap_pct, 23,
        "#batch-sched: only dir `p` is shared (A0,A1); its 300 direct bytes counted ONCE \
         over total 1300 sampled subtree bytes = 23% (floor); got {}",
        g.subtree_overlap_pct
    );
}

// ───────────────── gain_pct is a genuine LOWER BOUND: B >= G ─────────────────

/// A tiny xorshift PRNG so the property test is deterministic (no `rand` dep)
/// and reproducible: a fixed seed exercises the SAME matrices every run, so a
/// regression is not a flaky heisenbug.
struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

/// (#batch-sched, assumption-auditor FIX-FIRST) The headline `gain_pct = (B−G)/G`
/// must be a genuine LOWER BOUND on the batch gain: `B ≥ G` and therefore
/// `gain_pct ≥ 0` for EVERY input. The from-scratch batch heuristic is NOT the
/// optimum — it can score WORSE than the greedy baseline on contended cycles —
/// and a silent floor would bias the headline LOW and clip exactly the contended
/// cycles. The model defines `B := max(B_heuristic, G)` ("a real batch scheduler
/// never does worse than greedy") and records `greedy_fallback` when the
/// heuristic underperformed G, so the clip is EXPLICIT and countable.
///
/// This property test sweeps thousands of small random matrices UNDER THE M1
/// GATE (random seeded `running` vs random `p_core_count`, spanning the gate
/// boundary) and asserts the invariant holds and — the load-bearing part — that
/// at least one case actually EXERCISES the fallback (heuristic < greedy), so the
/// max() is not vacuous. Deterministic (fixed xorshift seed).
#[test]
fn gain_pct_is_lower_bound_b_ge_g_over_random_matrices() {
    let mut rng = XorShift(0x1234_5678_9abc_def0);
    // A small pool of directory digests so overlap/contention actually arises.
    let dir_pool: Vec<u8> = (0u8..8).collect();

    let mut fallback_seen = 0u64;
    let mut gate_active_seen = 0u64;
    let mut gate_lifted_seen = 0u64;
    let cases = 5000u64;
    for _ in 0..cases {
        let n_actions = rng.range(1, 6) as usize;
        let n_workers = rng.range(1, 4) as usize;

        // Random actions: each references 1-3 dirs with random direct bytes.
        let actions: Vec<BatchSchedAction> = (0..n_actions)
            .map(|_| {
                let n_dirs = rng.range(1, 3) as usize;
                let dirs: Vec<(u8, u64, u64)> = (0..n_dirs)
                    .map(|_| {
                        let d = dir_pool[rng.range(0, dir_pool.len() as u64 - 1) as usize];
                        (d, rng.range(0, 1_000_000), rng.range(0, 20))
                    })
                    .collect();
                action(&dirs)
            })
            .collect();

        // Random workers: each caches a random subset of the pool, an M4-ish
        // p_core count (2-8), and a seeded `running` SPANNING the gate boundary
        // (0..=p_core+2 — some p_headroom, some gate-excluded), a low p_load
        // (threshold 0 keeps the override inert), and a random load_penalty.
        let workers: Vec<BatchSchedWorker> = (0..n_workers)
            .map(|_| {
                let cached: Vec<u8> = dir_pool
                    .iter()
                    .copied()
                    .filter(|_| rng.range(0, 1) == 1)
                    .collect();
                let p_core = rng.range(2, 8) as u32;
                let running = rng.range(0, u64::from(p_core) + 2);
                let load_penalty = rng.range(0, 50_000) as i64;
                worker(&cached, running, p_core, 10, load_penalty)
            })
            .collect();

        let g = compute_batch_sched_gain(&actions, &workers, GATE_ON_V1);

        // Track that the sweep actually exercised BOTH regimes (some contended
        // steps, some lifted) so the gate logic is not silently no-op'd.
        if g.gate_active_frac > 0 {
            gate_active_seen += 1;
        }
        if g.gate_active_frac < 100 {
            gate_lifted_seen += 1;
        }

        // gain_pct is u64 → structurally never negative; the REAL invariant is
        // that it is an HONEST lower bound: whenever the fallback fired, gain must
        // be 0 (B was floored up to G), never a clipped-away positive.
        if g.greedy_fallback {
            fallback_seen += 1;
            assert_eq!(
                g.gain_pct, 0,
                "#batch-sched: when the heuristic underperformed greedy the fallback \
                 B:=max(B,G) makes B==G → gain_pct must be exactly 0 (an honest floor), \
                 not a clipped negative; got {}",
                g.gain_pct
            );
        }
    }

    // Load-bearing: the max() must actually catch real underperformance in this
    // sweep, else the property is vacuous and a future regression that removes
    // the guard would pass silently.
    assert!(
        fallback_seen > 0,
        "#batch-sched: over {cases} random matrices the from-scratch heuristic never \
         underperformed greedy — the B:=max(B,G) fallback is UNTESTED (vacuous). Either \
         the generator lost its contention or the fallback flag is not wired; expected \
         >0 fallback cases. fallback_seen={fallback_seen}"
    );
    // Load-bearing: the sweep must exercise the gate in BOTH regimes, else the
    // gate replay is silently inert (e.g. a bug that always lifts, or never lifts).
    assert!(
        gate_active_seen > 0 && gate_lifted_seen > 0,
        "#batch-sched M1-replay: the random sweep must exercise BOTH gate regimes \
         (some cycles with the gate active, some fully lifted) so the gate replay is not \
         vacuously no-op. gate_active_seen={gate_active_seen} gate_lifted_seen={gate_lifted_seen}"
    );
}
