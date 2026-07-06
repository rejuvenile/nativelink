// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! #sched-affinity-probe QUADRATIC-COST EVIDENCE micro-timing — the evidence that
//! the obs-only probe is quadratic and therefore ships default-OFF (user decision
//! 2026-07-06). NOT a CI gate: `#[ignore]`d so CI never runs the 24s@512 case; run
//! explicitly with `--ignored --nocapture` to regenerate the table.
//!
//! Times the DOMINANT probe kernel `compute_batch_sched_gain` (flamegraph:
//! 17.84% of scheduler CPU during a live 27s match cycle — the pinned cost that
//! `record_pending_affinity_surplus` pays synchronously inside the timed match
//! region) as a function of the sampled-action count against a realistic
//! prod-shaped worker set. The table shows the cost is QUADRATIC in the
//! sampled-action count (24.6s at the 512 sample cap), which is why the probe
//! must not run always-on in prod and is instead OPT-IN via
//! `pending_affinity_probe_enabled` — see
//! `.claude/audits/affinity-probe-slow-match-2026-07-06/`.
//!
//! Prints a table; not asserted (timing on a shared build box is noisy). The
//! opt-in default (false) is pinned in `pending_affinity_probe_guard_test.rs` and
//! `nativelink-config/tests/simple_spec_default_test.rs`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use nativelink_scheduler::simple_scheduler::{
    BatchSchedAction, BatchSchedGateCfg, BatchSchedWorker, compute_batch_sched_gain,
};
use nativelink_util::common::DigestInfo;

/// Prod-shaped fleet size (Mac worker fleet 192.168.100.x).
const WORKERS: usize = 12;
/// Directories in a realistic build action's input tree (a C++ compile with
/// many header dirs). This is the inner-loop multiplier: `batch_sched_score`
/// probes each of these against every worker's cache set.
const DIR_DIGESTS_PER_ACTION: usize = 128;
/// Warm-worker locality-map cardinality (a hot worker mirrors tens of thousands
/// of its FilesystemStore directory-subtree holdings).
const WORKER_CACHE_SIZE: usize = 20_000;

fn digest(seed: u64) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0..8].copy_from_slice(&seed.to_le_bytes());
    DigestInfo::new(h, 1)
}

/// Build `n_actions` actions each carrying `DIR_DIGESTS_PER_ACTION` directory
/// digests drawn from a shared universe so worker caches overlap them
/// realistically (some hits, some misses — the real hash+probe cost).
fn build_actions(n_actions: usize) -> Vec<BatchSchedAction> {
    (0..n_actions)
        .map(|a| {
            let mut dir_digests = HashSet::with_capacity(DIR_DIGESTS_PER_ACTION);
            let mut dir_direct_bytes = HashMap::with_capacity(DIR_DIGESTS_PER_ACTION);
            let mut dir_direct_files = HashMap::with_capacity(DIR_DIGESTS_PER_ACTION);
            for d in 0..DIR_DIGESTS_PER_ACTION {
                // Universe of ~40k distinct dir digests, shared across actions.
                let seed = ((a * 7 + d * 13) % 40_000) as u64;
                let dg = digest(seed);
                dir_digests.insert(dg);
                dir_direct_bytes.insert(dg, 4096);
                dir_direct_files.insert(dg, 3);
            }
            BatchSchedAction {
                dir_digests,
                dir_direct_bytes,
                dir_direct_files,
            }
        })
        .collect()
}

fn build_workers() -> Vec<BatchSchedWorker> {
    (0..WORKERS)
        .map(|w| {
            let mut cached_subtree_digests = HashSet::with_capacity(WORKER_CACHE_SIZE);
            for i in 0..WORKER_CACHE_SIZE {
                // Each worker warms a distinct-but-overlapping slice of the
                // shared 40k-digest universe.
                let seed = ((w * 3000 + i) % 40_000) as u64;
                cached_subtree_digests.insert(digest(seed));
            }
            BatchSchedWorker {
                cached_subtree_digests,
                running: 4,
                p_core_count: 8,
                p_core_load_pct: 50,
                load_penalty: 0,
            }
        })
        .collect()
}

#[test]
#[ignore = "quadratic-cost evidence micro-timing (default-OFF justification); run with --ignored --nocapture"]
fn pending_affinity_probe_kernel_timing() {
    // Prod gate config: enabled, threshold 0 (v1), override 2 (inert at 0).
    let gate_cfg = BatchSchedGateCfg {
        enabled: true,
        idle_threshold_pct: 0,
        override_factor: 2,
    };
    let workers = build_workers();

    println!(
        "\n#sched-affinity-probe compute_batch_sched_gain kernel timing \
         (workers={WORKERS}, dir_digests/action={DIR_DIGESTS_PER_ACTION}, \
         worker_cache={WORKER_CACHE_SIZE})"
    );
    println!("  sample_actions |  median_ms | p_of_5s_match_budget | iters");
    // Low regime (a shallow queue an investigation would enable the probe on) —
    // measure it densely with many iterations. High regime (256/512) documents the
    // quadratic blow-up that justifies keeping the probe OFF by default; single-iter
    // so the harness never wedges.
    for &(n, iters) in &[
        (8usize, 15usize),
        (16, 15),
        (32, 11),
        (48, 9),
        (64, 9),
        (96, 5),
        (128, 5),
        (256, 3),
        (512, 1),
    ] {
        let actions = build_actions(n);
        let mut samples_ms: Vec<f64> = Vec::new();
        for _ in 0..iters {
            let start = Instant::now();
            let gain = compute_batch_sched_gain(&actions, &workers, gate_cfg);
            // Consume the result so the optimizer cannot elide the call.
            std::hint::black_box(gain);
            samples_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples_ms[samples_ms.len() / 2];
        let pct_of_budget = median / 5000.0 * 100.0;
        println!("  {n:>14} | {median:>10.3} | {pct_of_budget:>18.4}% | {iters:>5}");
    }
    println!();
}
