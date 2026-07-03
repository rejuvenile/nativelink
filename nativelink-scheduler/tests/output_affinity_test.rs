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

//! Tests for the OBSERVABILITY-ONLY output-affinity opportunity probe
//! (`#output-locality-probe`): the PURE match-rate computation
//! `compute_output_affinity`, which measures how often a ready action's INPUT
//! directory subtree matches an OUTPUT directory a STILL-CONNECTED worker
//! recently produced. This is the ceiling of what "route a consumer to the
//! worker that produced its inputs" could exploit — a signal the scheduler does
//! NOT currently have (its only affinity signal is input-tree overlap via
//! `cached_subtree_digests`).
//!
//! The map (`producer_map`) is keyed on the OUTPUT tree's constituent
//! `Directory` digests (root + children), NOT the `Tree` digest itself — a
//! downstream consumer references an output directory by its root/child
//! `Directory` digest, which is what appears in the consumer's input-tree
//! `dir_digests`. The `Tree` digest is a digest of a DIFFERENT message shape
//! that never appears in an input tree, so keying on it would match identically
//! zero (see the design doc + REAPI `OutputDirectory.tree_digest`).
//!
//! This is a PURE function so the exact match/byte arithmetic is unit-testable
//! WITHOUT driving a live scheduler. Telemetry-only: nothing here asserts a
//! scheduling behavior change (there is none — the scheduler has no
//! output-affinity tier).

use std::collections::{HashMap, HashSet};

use nativelink_scheduler::simple_scheduler::{
    BatchSchedAction, PER_FILE_WEIGHT, compute_output_affinity,
};
use nativelink_util::action_messages::WorkerId;
use nativelink_util::common::DigestInfo;

/// A distinct digest keyed by a single seed byte (the rest zero). Only
/// membership + the attached direct-byte/file weight matter for the match/mass.
fn dg(seed: u8) -> DigestInfo {
    DigestInfo::new([seed; 32], 0)
}

fn wid(name: &str) -> WorkerId {
    WorkerId(name.to_string())
}

/// One sampled ready action carrying `(dir_digest, direct_bytes, direct_files)`
/// triples for its INPUT tree. Reuses the batch probe's `BatchSchedAction`
/// carrier (the same owned subtree structure the `tree_cache` peek produces).
fn mk_action(dirs: &[(DigestInfo, u64, u64)]) -> BatchSchedAction {
    let mut dir_digests = HashSet::new();
    let mut dir_direct_bytes = HashMap::new();
    let mut dir_direct_files = HashMap::new();
    for (d, bytes, files) in dirs {
        dir_digests.insert(*d);
        dir_direct_bytes.insert(*d, *bytes);
        dir_direct_files.insert(*d, *files);
    }
    BatchSchedAction {
        dir_digests,
        dir_direct_bytes,
        dir_direct_files,
    }
}

fn producer_map(entries: &[(DigestInfo, WorkerId)]) -> HashMap<DigestInfo, WorkerId> {
    entries.iter().cloned().collect()
}

fn connected(workers: &[WorkerId]) -> HashSet<WorkerId> {
    workers.iter().cloned().collect()
}

/// (a) A ready action whose INPUT directory digest matches a CONNECTED producer
/// COUNTS: `match_frac == 100`, one distinct producer, and `matched_bytes`
/// equals that directory's `direct_bytes + direct_files*PER_FILE_WEIGHT`.
#[test]
fn connected_producer_match_counts() {
    let d = dg(1);
    let actions = vec![mk_action(&[(d, 500, 3)])];
    let map = producer_map(&[(d, wid("W1"))]);
    let conn = connected(&[wid("W1")]);

    let gain = compute_output_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.match_frac, 100,
        "the single sampled action has an input dir `d` that a CONNECTED producer \
         (W1) output → 1/1 actions match → match_frac must be 100. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.distinct_producers, 1,
        "exactly one producer (W1) was matched. got {}",
        gain.distinct_producers
    );
    assert_eq!(
        gain.sample_actions, 1,
        "one action was sampled (coverage denominator). got {}",
        gain.sample_actions
    );
    assert_eq!(
        gain.matched_bytes,
        500 + 3 * PER_FILE_WEIGHT,
        "matched byte-mass uses the Tier-1.5 model: direct_bytes(500) + \
         direct_files(3)*PER_FILE_WEIGHT. got {}",
        gain.matched_bytes
    );
}

/// (b) A match to a DISCONNECTED producer does NOT count. Same digest as (a),
/// but the producer W1 is absent from `connected` → 0 matches, 0 bytes, 0
/// producers. Distinguishes OPPORTUNITY (a still-connected producer exists)
/// from a producer that has since left the fleet.
#[test]
fn disconnected_producer_does_not_count() {
    let d = dg(1);
    let actions = vec![mk_action(&[(d, 500, 3)])];
    let map = producer_map(&[(d, wid("W1"))]);
    let conn = connected(&[wid("W2")]); // W1 NOT connected

    let gain = compute_output_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.match_frac, 0,
        "the only producer of `d` (W1) is DISCONNECTED, so the match must NOT \
         count → match_frac 0. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.matched_bytes, 0,
        "no connected match → 0 matched bytes. got {}",
        gain.matched_bytes
    );
    assert_eq!(
        gain.distinct_producers, 0,
        "no connected producer matched → 0 distinct producers. got {}",
        gain.distinct_producers
    );
}

/// (c) An action with NO matching input dir contributes 0. Two actions: one
/// matches a connected producer, one has an input dir absent from the map. Only
/// the first counts → `match_frac == 50` over the two-action sample.
#[test]
fn no_match_action_contributes_zero() {
    let matched = dg(1);
    let unmatched = dg(9);
    let actions = vec![
        mk_action(&[(matched, 100, 0)]),   // A0: matches W1
        mk_action(&[(unmatched, 777, 5)]), // A1: no producer for `unmatched`
    ];
    let map = producer_map(&[(matched, wid("W1"))]);
    let conn = connected(&[wid("W1")]);

    let gain = compute_output_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.match_frac, 50,
        "1 of 2 sampled actions matches a connected producer → match_frac 50. \
         The no-match action (A1) contributes nothing. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.sample_actions, 2,
        "two actions were sampled. got {}",
        gain.sample_actions
    );
    assert_eq!(
        gain.matched_bytes,
        100,
        "only A0's matched dir (direct_bytes 100, 0 files) contributes; A1's 777 \
         bytes are NOT matched and must NOT be summed. got {}",
        gain.matched_bytes
    );
    assert_eq!(
        gain.distinct_producers, 1,
        "only W1 matched. got {}",
        gain.distinct_producers
    );
}

/// (d) Byte-mass sums correctly across MULTIPLE matched directories and actions,
/// and distinct producers dedups a producer that output several matched dirs.
/// A0 matches two dirs from the SAME producer W1; A1 matches one dir from W2.
/// matched_bytes = Σ over all 3 matched dirs of (bytes + files*WEIGHT).
/// distinct_producers = 2 (W1 counted once despite two matched dirs).
#[test]
fn byte_mass_sums_across_dirs_and_actions() {
    let a = dg(1);
    let b = dg(2);
    let c = dg(3);
    let noise = dg(8); // present as input, but no producer → excluded
    let actions = vec![
        mk_action(&[(a, 200, 1), (b, 300, 2), (noise, 999, 9)]), // A0: a,b match W1
        mk_action(&[(c, 400, 0)]),                               // A1: c matches W2
    ];
    let map = producer_map(&[(a, wid("W1")), (b, wid("W1")), (c, wid("W2"))]);
    let conn = connected(&[wid("W1"), wid("W2")]);

    let gain = compute_output_affinity(&actions, &map, &conn);

    let expected_bytes =
        (200 + 1 * PER_FILE_WEIGHT) + (300 + 2 * PER_FILE_WEIGHT) + (400 + 0 * PER_FILE_WEIGHT);
    assert_eq!(
        gain.matched_bytes, expected_bytes,
        "matched byte-mass sums (a: 200+1*W) + (b: 300+2*W) + (c: 400+0*W) across \
         both actions; `noise`'s 999 bytes are unmatched and excluded. got {} want {}",
        gain.matched_bytes, expected_bytes
    );
    assert_eq!(
        gain.match_frac, 100,
        "both sampled actions have ≥1 connected match → match_frac 100. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.distinct_producers, 2,
        "W1 (two matched dirs) counts ONCE; W2 once → 2 distinct producers. got {}",
        gain.distinct_producers
    );
}

/// Empty-sample guard: no sampled actions → all-zero, no divide-by-zero.
#[test]
fn empty_sample_is_zero() {
    let map = producer_map(&[(dg(1), wid("W1"))]);
    let conn = connected(&[wid("W1")]);
    let gain = compute_output_affinity(&[], &map, &conn);
    assert_eq!(
        gain, Default::default(),
        "empty sample → default (all-zero) gain, no panic. got {gain:?}"
    );
}
