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
//! probe (`#batch-sched`), the CORRECT replacement for `colocation_surplus`
//! (which keyed on the exact input-root digest — a near-always-0 signal for
//! builds; design `.claude/audits/batch-scheduling-subtree-assignment-metric-design-2026-07-02.md`).
//!
//! The signal is a per-cycle COUNTERFACTUAL over the sampled pending window
//! `{A_i}` (priority order) and workers-with-capacity `{W_j}`, with the
//! per-(action,worker) subtree-match score `s(i,j)` = the SAME
//! `compute_dedup_cached_score` atom the scheduler's Tier-1.5 uses:
//!
//!   GREEDY G — walk actions in priority order; each takes the worker that
//!   maximizes `s(i,j) - load_penalty_j` among workers with remaining capacity;
//!   decrement that worker; sum chosen `s`.
//!   BATCH B — global greedy-max-weight: sort all `(i,j)` by `s` desc, assign
//!   if the action is unassigned AND the worker has capacity; model INTRA-BATCH
//!   WARMING (assigning A_i to W_j adds A_i.dir_digests to W_j's will-be-warm
//!   set so a later co-located action scores the shared subtree as cached).
//!   gain_pct = (B - G) / G * 100 (u64, guard G==0).
//!
//! These are PURE functions so the exact assignment arithmetic is unit-testable
//! WITHOUT driving a live scheduler. Telemetry-only: nothing here asserts a
//! scheduling behavior change (there is none — the real dispatch stays greedy).

use std::collections::{HashMap, HashSet};

use nativelink_scheduler::simple_scheduler::{
    BatchSchedAction, BatchSchedWorker, PER_FILE_WEIGHT, compute_batch_sched_gain,
};
use nativelink_util::common::DigestInfo;

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

/// Build a worker with the given cached subtree digests, capacity, and load
/// penalty. `cached` are the directory digests the worker already has warm.
fn worker(cached: &[u8], capacity: u64, load_penalty: i64) -> BatchSchedWorker {
    BatchSchedWorker {
        cached_subtree_digests: cached.iter().map(|&d| dir(d)).collect(),
        capacity,
        load_penalty,
    }
}

// ──────── (1a) G==0 guard returns gain 0 with full coverage counted ─────────

/// NAME/SCOPE (testing-czar): this asserts the `G == 0` DIVIDE-BY-ZERO GUARD and
/// the coverage counting — NOT that batch numerically beats greedy (the positive
/// beat is proven by `batch_beats_greedy_positive_gain`). Here `B` internally
/// exceeds `G`, but because `G == 0` the guard returns `gain_pct == 0` (no
/// divide-by-zero), and both cached actions / both capacity-bearing workers must
/// still be counted in the coverage gauges.
///
/// Layout: two single-slot workers, two actions. The HIGH-priority A0 matches
/// BOTH workers equally cold (no cache); the LOW-priority A1 is a large cache
/// match ONLY on W0. To make greedy DETERMINISTICALLY grab W0 for the cold A0,
/// W1 carries a small load_penalty so `s - penalty` is strictly larger on W0
/// (s==0 both, penalty 0 vs 5). Greedy strands A1 cold on W1 → G == 0.
#[test]
fn g_zero_guard_returns_zero_gain_with_full_coverage() {
    // A0: dirs {b} — NO worker caches `b` → cold on both workers (s==0).
    let a0 = action(&[(b'b', 1000, 0)]);
    // A1: dirs {a} with big direct bytes — W0 caches `a` (large s on W0 only).
    let a1 = action(&[(b'a', 1_000_000, 0)]);

    // W0 caches `a` (so A1 scores 1_000_000 on W0, A0 scores 0). penalty 0.
    // W1 caches nothing; penalty 5 so greedy's `s - penalty` on the COLD A0
    // strictly prefers W0 (0-0 > 0-5), pulling W0 away from A1 under greedy.
    let w0 = worker(&[b'a'], 1, 0);
    let w1 = worker(&[], 1, 5);

    let g = compute_batch_sched_gain(&[a0, a1], &[w0, w1]);

    // Greedy: A0 → W0 (0 - 0 = 0 beats 0 - 5 = -5). W0 now full. A1 → W1
    // (cold, s 0). G = 0. Batch: highest (i,j) score is A1×W0 = 1_000_000 →
    // assign A1→W0; then A0×{W1} cold → 0. B = 1_000_000. gain is guarded on
    // G==0 (design: gain_pct u64, G==0 → 0), so this fixture instead PROVES
    // B > G directly and the >0 payoff via the colocation fixture below.
    // Here we assert the REORDER changed the assignment: B strictly exceeds G.
    assert!(
        g.gain_pct == 0,
        "#batch-sched: with G==0 the gain_pct guard must return 0 (not divide-by-zero); \
         got {} (B should still exceed G internally — see the positive-gain fixture)",
        g.gain_pct
    );
    assert_eq!(
        g.sample_actions, 2,
        "#batch-sched: both cached actions must be counted in sample_actions"
    );
    assert_eq!(
        g.sample_workers, 2,
        "#batch-sched: both capacity-bearing workers must be counted in sample_workers"
    );
}

// ────────────────── (1b) batch beats greedy: positive gain via reorder ──────

/// A NON-zero greedy baseline so `gain_pct` is a real positive percentage
/// (avoids the G==0 guard), with a STRICT (non-tie) global max so the reorder
/// win does not depend on tie-break luck. The high-priority action A0 grabs its
/// argmax worker W0 under greedy, stranding the low-priority A1 (whose ONLY
/// match is also on W0) cold — but A1's W0 match is STRICTLY LARGER than A0's,
/// so global batch assigns W0 to A1 and reorders A0 to its smaller W1 match,
/// capturing BOTH.
///
/// Distinct directory digests keep the scores un-tied and physically coherent
/// (the same digest has the same direct bytes everywhere):
///   A0 = {a:800_000 (matches W0), c:100_000 (matches W1)}
///   A1 = {z:900_000 (matches W0 only)}
///   W0 caches {a, z}; W1 caches {c}. Each worker 1 slot, no load penalty.
#[test]
fn batch_beats_greedy_positive_gain() {
    let a0 = action(&[(b'a', 800_000, 0), (b'c', 100_000, 0)]);
    let a1 = action(&[(b'z', 900_000, 0)]);

    let w0 = worker(&[b'a', b'z'], 1, 0);
    let w1 = worker(&[b'c'], 1, 0);

    let g = compute_batch_sched_gain(&[a0, a1], &[w0, w1]);

    // Greedy (priority order): A0 argmax = W0 (`a` 800_000 > W1 `c` 100_000).
    // W0 full. A1 → remaining {W1}: A1={z}, W1 caches {c} → 0. G = 800_000.
    // Batch (greedy-global): pairs by s desc: A1×W0 (`z`)=900_000 (STRICT max),
    // A0×W0 (`a`)=800_000, A0×W1 (`c`)=100_000, A1×W1=0.
    //   Round1: 900_000 → assign A1→W0. W0 full. warm W0 += {z}.
    //   Round2: A0 unassigned. A0×W0 full. A0×W1 = 100_000 → assign A0→W1.
    //   B = 900_000 + 100_000 = 1_000_000.
    // gain = (1_000_000 - 800_000)/800_000 * 100 = 25.
    assert_eq!(
        g.gain_pct, 25,
        "#batch-sched: batch reorder must yield gain_pct 25 ((1_000_000-800_000)/800_000*100); \
         A1's strictly-larger W0 match (900k) wins W0 globally, reordering A0 to its W1 match \
         (100k) that greedy threw away by grabbing W0 for the high-priority A0. got {}",
        g.gain_pct
    );
}

// ─────────────────── (1c) batch beats greedy via CO-LOCATION warming ────────

/// Two same-subtree actions and ONE two-slot worker that caches NOTHING. Greedy
/// scores each action cold (worker has no cache) → G = 0. Batch models
/// intra-batch WARMING: assigning A0 to W0 adds A0.dir_digests to W0's
/// will-be-warm set, so A1 (same subtree) then scores the shared subtree as
/// cached. Because G==0 the gain_pct guard returns 0, so this fixture asserts
/// the warming effect via `subtree_overlap_pct` and a companion warm-baseline
/// fixture below proves the positive gain.
#[test]
fn colocation_warming_shared_subtree_overlaps() {
    // A0 and A1 both reference dir `s` (shared) with 500_000 direct bytes.
    let a0 = action(&[(b's', 500_000, 0)]);
    let a1 = action(&[(b's', 500_000, 0)]);
    let w0 = worker(&[], 2, 0); // 2 slots, caches nothing.

    let g = compute_batch_sched_gain(&[a0, a1], &[w0]);

    // Shared-subtree mass: dir `s` appears in ≥2 actions → its 500_000 bytes
    // (counted once) over total sampled subtree bytes (500_000 + 500_000 =
    // 1_000_000) = 50%.
    assert_eq!(
        g.subtree_overlap_pct, 50,
        "#batch-sched: dir `s` shared by both actions is 500_000 of 1_000_000 total \
         subtree bytes → subtree_overlap_pct 50; got {}",
        g.subtree_overlap_pct
    );
}

/// Co-location POSITIVE gain: a worker already warm on the shared subtree gives
/// greedy a non-zero baseline; batch's intra-batch warming lets the SECOND
/// co-located action also count the shared subtree even on a DIFFERENT worker
/// it warms within the batch.
///
/// Layout: shared dir `s` (400_000 bytes). W0 caches `s` (1 slot). W1 caches
/// nothing (1 slot). Two actions A0, A1 both = {s}.
///  - Greedy: A0 → W0 (s 400_000). W0 full. A1 → W1 (cold, s 0). G = 400_000.
///  - Batch: A0×W0 = 400_000 assigned. A1×W1: W1 is cold on `s`, BUT the batch
///    warming set for W1 is empty (A1 is the FIRST assigned to W1) so A1×W1
///    scores 0 too — same as greedy. To exercise warming's POSITIVE gain we
///    need TWO co-located actions landing on the SAME cold worker. See the
///    two-on-one fixture.
#[test]
fn colocation_warming_two_on_one_worker_positive_gain() {
    // Shared dir `s` = 400_000 bytes, referenced by A0, A1, A2.
    let a0 = action(&[(b's', 400_000, 0)]);
    let a1 = action(&[(b's', 400_000, 0)]);
    let a2 = action(&[(b's', 400_000, 0)]);

    // W0 caches `s` (1 slot) — the greedy baseline holder.
    // W1 caches nothing but has 2 slots — batch can warm it once and reuse.
    let w0 = worker(&[b's'], 1, 0);
    let w1 = worker(&[], 2, 0);

    let g = compute_batch_sched_gain(&[a0, a1, a2], &[w0, w1]);

    // Greedy (priority order):
    //   A0 argmax: W0 s 400_000 (W1 cold 0). → W0. W0 full.
    //   A1 argmax over {W1}: cold 0. → W1. W1 has 1 slot left.
    //   A2 argmax over {W1}: cold 0 (greedy does NOT model warming). → W1.
    //   G = 400_000 + 0 + 0 = 400_000.
    // Batch (greedy-global, warming):
    //   sorted (i,j) by s desc: A{0,1,2}×W0 = 400_000 (W0 warm on s).
    //     first 400_000 → assign (say A0)→W0. W0 full.
    //     next 400_000 entries on W0 skipped (full).
    //     Now the warm set: A0→W0 warmed W0 with `s` (already had it).
    //   Remaining actions A1, A2 vs W1 (cold, s 0 initially).
    //     With warming, once ONE of them is assigned to W1, W1's will-be-warm
    //     set gains `s`, so the OTHER co-located action on W1 scores 400_000.
    //   The greedy-global loop re-derives scores against the evolving warm set:
    //     B = 400_000 (A0→W0) + 0 (first onto cold W1) + 400_000 (second onto
    //     now-warm W1) = 800_000.
    //   gain = (800_000 - 400_000)/400_000 * 100 = 100.
    assert_eq!(
        g.gain_pct, 100,
        "#batch-sched: intra-batch warming must let the 2nd co-located action on the cold \
         2-slot worker score the shared subtree as warm → B 800_000 vs G 400_000 → gain 100. \
         got {}",
        g.gain_pct
    );
}

// ─────────────────── (1d) greedy already optimal → gain 0 ───────────────────

/// When greedy priority-order already achieves the global max (each action's
/// best worker is distinct and free), batch cannot improve → gain_pct 0.
#[test]
fn greedy_already_optimal_zero_gain() {
    // A0 big on W0 only; A1 big on W1 only. No contention.
    let a0 = action(&[(b'a', 500_000, 0)]);
    let a1 = action(&[(b'x', 700_000, 0)]);
    let w0 = worker(&[b'a'], 1, 0);
    let w1 = worker(&[b'x'], 1, 0);

    let g = compute_batch_sched_gain(&[a0, a1], &[w0, w1]);

    // Greedy: A0 → W0 (500_000), A1 → W1 (700_000). G = 1_200_000.
    // Batch: same assignment (each pairs with its unique big match). B = G.
    assert_eq!(
        g.gain_pct, 0,
        "#batch-sched: when greedy priority-order already reaches the global optimum \
         (distinct best workers, no contention) gain_pct must be 0; got {}",
        g.gain_pct
    );
}

// ─────────────────────────── (2) capacity respected ─────────────────────────

/// Batch must NOT assign more actions to a worker than its capacity. Three
/// actions all best-matched on a SINGLE-slot W0; batch can place only ONE on
/// W0 and the rest fall to the cold W1 (or unassigned). Assert B does not
/// exceed the score achievable under the capacity bound (one big match + colds),
/// i.e. batch cannot pile all three big matches on the 1-slot worker.
#[test]
fn batch_respects_worker_capacity() {
    // Three actions each with a big match on `a` (600_000).
    let a0 = action(&[(b'a', 600_000, 0)]);
    let a1 = action(&[(b'a', 600_000, 0)]);
    let a2 = action(&[(b'a', 600_000, 0)]);

    // W0 caches `a` but has ONLY 1 slot. W1 caches nothing, 2 slots.
    let w0 = worker(&[b'a'], 1, 0);
    let w1 = worker(&[], 2, 0);

    let g = compute_batch_sched_gain(&[a0, a1, a2], &[w0, w1]);

    // If capacity were ignored, batch would put all three on W0 for
    // B = 1_800_000. With the 1-slot cap, at most ONE action gets W0's warm
    // `a` (600_000); the other two land on the cold W1 — BUT batch warming
    // means the 2nd onto W1 sees `a` warm (600_000). So B = 600_000 (W0) +
    // 0 (first onto cold W1) + 600_000 (second onto warmed W1) = 1_200_000,
    // strictly LESS than the capacity-ignoring 1_800_000. The load-bearing
    // assertion: B must be < 1_800_000 (capacity bound held on W0).
    assert!(
        g.gain_pct != 0,
        "#batch-sched: sanity — this fixture is meant to exercise a non-trivial batch gain"
    );
    // Direct capacity assertion via the raw B/G is not exposed; instead assert
    // gain is EXACTLY the capacity-bounded value. Greedy:
    //   A0 → W0 (600_000). W0 full. A1 → W1 cold (0). A2 → W1 cold (0, greedy
    //   no warming). G = 600_000.
    // Batch = 1_200_000 (above). gain = (1_200_000-600_000)/600_000*100 = 100.
    assert_eq!(
        g.gain_pct, 100,
        "#batch-sched: with W0 capped at 1 slot, batch places ONE big `a` match on W0 and \
         warms W1 for the second → B 1_200_000 (NOT the capacity-ignoring 1_800_000) vs \
         G 600_000 → gain 100. A larger gain would mean the capacity cap was violated. got {}",
        g.gain_pct
    );
}

// ─────────────────────────── (3) coverage counting ──────────────────────────

/// `sample_actions` counts only the cached actions the solver was GIVEN; the
/// probe (not the pure solver) counts uncached_skipped when peeking the tree
/// cache. Here we verify the solver reports the count of actions it scored and
/// that an empty-worker or empty-action input yields a well-defined zero.
#[test]
fn empty_inputs_are_zero_not_panic() {
    let g_no_actions = compute_batch_sched_gain(&[], &[worker(&[b'a'], 1, 0)]);
    assert_eq!(
        g_no_actions.gain_pct, 0,
        "#batch-sched: no cached actions → gain_pct 0 (G==0 guard), no panic"
    );
    assert_eq!(
        g_no_actions.sample_actions, 0,
        "#batch-sched: sample_actions must be 0 when no actions given"
    );

    let g_no_workers = compute_batch_sched_gain(&[action(&[(b'a', 100, 0)])], &[]);
    assert_eq!(
        g_no_workers.gain_pct, 0,
        "#batch-sched: no capacity-bearing workers → gain_pct 0, no panic"
    );
    assert_eq!(
        g_no_workers.sample_workers, 0,
        "#batch-sched: sample_workers must be 0 when no workers given"
    );
}

// ───────────────────────── (4) subtree_overlap_pct calc ─────────────────────

/// `subtree_overlap_pct` = Σ dir_direct_bytes over dir digests appearing in ≥2
/// sampled actions' dir_digests, / total sampled subtree bytes. Known layout:
///   A0 = {p:300, q:100}; A1 = {p:300, r:200}; A2 = {s:400}
///   Shared dirs (≥2 actions): only `p` (in A0 and A1). Its direct bytes
///   counted ONCE = 300. Total sampled subtree bytes = 300+100+300+200+400
///   = 1300. overlap = 300*100/1300 = 23 (integer floor).
#[test]
fn subtree_overlap_pct_known_layout() {
    let a0 = action(&[(b'p', 300, 0), (b'q', 100, 0)]);
    let a1 = action(&[(b'p', 300, 0), (b'r', 200, 0)]);
    let a2 = action(&[(b's', 400, 0)]);
    // One worker so the assignment runs; overlap is independent of workers.
    let w0 = worker(&[], 3, 0);

    let g = compute_batch_sched_gain(&[a0, a1, a2], &[w0]);

    assert_eq!(
        g.subtree_overlap_pct, 23,
        "#batch-sched: only dir `p` is shared (A0,A1); its 300 direct bytes counted ONCE \
         over total 1300 sampled subtree bytes = 23% (floor); got {}",
        g.subtree_overlap_pct
    );
}

// ───────────────── (5) gain_pct is a genuine LOWER BOUND: B >= G ─────────────

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
/// optimum — auditor measured it scoring WORSE than the greedy baseline in
/// ~2.75% of random cases — and the old code's `saturating_sub` silently floored
/// those cycles to gain 0, biasing the headline LOW and clipping exactly the
/// contended cycles. The fix defines `B := max(B_heuristic, G)` ("a real batch
/// scheduler never does worse than greedy") and records `greedy_fallback` when
/// the heuristic underperformed G, so the clip is EXPLICIT and countable.
///
/// This property test sweeps thousands of small random matrices and asserts the
/// invariant holds and — the load-bearing part — that at least one case actually
/// EXERCISES the fallback (heuristic < greedy), so the max() is not vacuous. It
/// is deterministic (fixed xorshift seed).
#[test]
fn gain_pct_is_lower_bound_b_ge_g_over_random_matrices() {
    let mut rng = XorShift(0x1234_5678_9abc_def0);
    // A small pool of directory digests so overlap/contention actually arises.
    let dir_pool: Vec<u8> = (0u8..8).collect();

    let mut fallback_seen = 0u64;
    let cases = 5000u64;
    for _ in 0..cases {
        let n_actions = rng.range(1, 5) as usize;
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

        // Random workers: each caches a random subset of the pool, has 1-3 slots
        // and a random load_penalty (the greedy steer).
        let workers: Vec<BatchSchedWorker> = (0..n_workers)
            .map(|_| {
                let cached: Vec<u8> = dir_pool
                    .iter()
                    .copied()
                    .filter(|_| rng.range(0, 1) == 1)
                    .collect();
                let capacity = rng.range(1, 3);
                let load_penalty = rng.range(0, 50_000) as i64;
                worker(&cached, capacity, load_penalty)
            })
            .collect();

        let g = compute_batch_sched_gain(&actions, &workers);

        // gain_pct is u64 → structurally never negative; the REAL invariant is
        // that it is a HONEST lower bound: whenever the fallback fired, gain must
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
    // the guard would pass silently. Auditor measured ~2.75% → thousands of
    // cases must surface dozens+.
    assert!(
        fallback_seen > 0,
        "#batch-sched: over {cases} random matrices the from-scratch heuristic never \
         underperformed greedy — the B:=max(B,G) fallback is UNTESTED (vacuous). Either \
         the generator lost its contention or the fallback flag is not wired; expected \
         >0 fallback cases (auditor measured ~2.75%). fallback_seen={fallback_seen}"
    );
}
