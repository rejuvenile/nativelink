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

//! Tests for the OBSERVABILITY-ONLY FILE-level output-affinity opportunity probe
//! (`#output-locality-probe` file sibling): the PURE match-rate computation
//! `compute_output_file_affinity`, which measures how often a ready action's
//! INPUT files overlap files a STILL-CONNECTED worker recently produced as
//! output. This is the ceiling for feeding outputs into the EXISTING
//! `score_and_generate_hints(&tree.file_digests, loc_map)` peer-fetch/prefetch
//! mechanism (which today never sees outputs).
//!
//! Unlike the directory probe, files are keyed on the FILE BLOB digest directly
//! (`ActionResult.output_files[].digest` — no Tree decode), and `matched_bytes`
//! sums real file SIZES (the byte-mass a peer-fetch would move) — the HEADLINE,
//! because the directory probe showed match_frac inflates on content-free digests
//! while byte-mass is the honest signal. `largest_contributor_bytes` exposes a
//! single ubiquitous-but-nonzero file dominating the mass.
//!
//! PURE function → the exact match/byte/dominator arithmetic is unit-testable
//! WITHOUT a live scheduler. Telemetry-only: no scheduling behavior change.

use std::collections::{HashMap, HashSet};

use nativelink_scheduler::simple_scheduler::{
    OutputFileAffinityAction, compute_output_file_affinity,
};
use nativelink_util::action_messages::WorkerId;
use nativelink_util::common::DigestInfo;

/// A distinct file digest keyed by a seed byte; `size` is the file's byte size.
fn fd(seed: u8, size: u64) -> (DigestInfo, u64) {
    (DigestInfo::new([seed; 32], size), size)
}

fn wid(name: &str) -> WorkerId {
    WorkerId(name.to_string())
}

/// One sampled ready action carrying its input `(file_digest, size)` pairs.
fn mk_action(files: &[(DigestInfo, u64)]) -> OutputFileAffinityAction {
    OutputFileAffinityAction {
        file_digests: files.to_vec(),
    }
}

fn producer_map(entries: &[((DigestInfo, u64), WorkerId)]) -> HashMap<DigestInfo, WorkerId> {
    entries.iter().map(|((d, _), w)| (*d, w.clone())).collect()
}

fn connected(workers: &[WorkerId]) -> HashSet<WorkerId> {
    workers.iter().cloned().collect()
}

/// (a) A ready action whose INPUT file matches a CONNECTED producer COUNTS:
/// `match_frac == 100`, one producer, and `matched_bytes` equals that file's
/// SIZE (not a weight).
#[test]
fn connected_file_match_counts_bytes_are_size() {
    let f = fd(1, 4096);
    let actions = vec![mk_action(&[f])];
    let map = producer_map(&[(f, wid("W1"))]);
    let conn = connected(&[wid("W1")]);

    let gain = compute_output_file_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.match_frac, 100,
        "#output-file: the single action's input file `f` was produced by \
         CONNECTED W1 → 1/1 → match_frac 100. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.matched_bytes, 4096,
        "#output-file: matched_bytes is the matched file's SIZE (4096), not a \
         weight. got {}",
        gain.matched_bytes
    );
    assert_eq!(
        gain.distinct_producers, 1,
        "#output-file: exactly one producer (W1). got {}",
        gain.distinct_producers
    );
    assert_eq!(
        gain.largest_contributor_bytes, 4096,
        "#output-file: the only matched digest contributes size(4096)×count(1). got {}",
        gain.largest_contributor_bytes
    );
}

/// (b) A match to a DISCONNECTED producer does NOT count.
#[test]
fn disconnected_producer_file_does_not_count() {
    let f = fd(1, 4096);
    let actions = vec![mk_action(&[f])];
    let map = producer_map(&[(f, wid("W1"))]);
    let conn = connected(&[wid("W2")]); // W1 not connected

    let gain = compute_output_file_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.match_frac, 0,
        "#output-file: `f`'s producer W1 is DISCONNECTED → no count → match_frac 0. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.matched_bytes, 0,
        "#output-file: disconnected → 0 matched bytes. got {}",
        gain.matched_bytes
    );
    assert_eq!(
        gain.largest_contributor_bytes, 0,
        "#output-file: nothing matched → 0 largest contributor. got {}",
        gain.largest_contributor_bytes
    );
}

/// (c) An action with NO matching input file contributes 0 (→ match_frac 50 over
/// a two-action sample where only one matches).
#[test]
fn no_match_file_action_contributes_zero() {
    let matched = fd(1, 1000);
    let unmatched = fd(9, 5000);
    let actions = vec![
        mk_action(&[matched]),   // A0: matches W1
        mk_action(&[unmatched]), // A1: no producer for `unmatched`
    ];
    let map = producer_map(&[(matched, wid("W1"))]);
    let conn = connected(&[wid("W1")]);

    let gain = compute_output_file_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.match_frac, 50,
        "#output-file: 1 of 2 actions matches → match_frac 50; A1 contributes \
         nothing. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.matched_bytes, 1000,
        "#output-file: only A0's matched file (1000 bytes) sums; A1's 5000 bytes \
         are unmatched and excluded. got {}",
        gain.matched_bytes
    );
    assert_eq!(
        gain.sample_actions, 2,
        "#output-file: two actions sampled. got {}",
        gain.sample_actions
    );
}

/// (d) matched_bytes sums real file SIZES across multiple matched files and
/// actions; distinct_producers dedups a producer of several files.
#[test]
fn matched_bytes_sum_file_sizes_across_files_and_actions() {
    let a = fd(1, 200);
    let b = fd(2, 300);
    let c = fd(3, 400);
    let noise = fd(8, 9999); // input file, no producer → excluded
    let actions = vec![
        mk_action(&[a, b, noise]), // A0: a,b match W1
        mk_action(&[c]),           // A1: c matches W2
    ];
    let map = producer_map(&[(a, wid("W1")), (b, wid("W1")), (c, wid("W2"))]);
    let conn = connected(&[wid("W1"), wid("W2")]);

    let gain = compute_output_file_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.matched_bytes,
        200 + 300 + 400,
        "#output-file: matched_bytes sums a(200)+b(300)+c(400); noise(9999) is \
         unmatched. got {}",
        gain.matched_bytes
    );
    assert_eq!(
        gain.match_frac, 100,
        "#output-file: both actions have ≥1 connected match → 100. got {}",
        gain.match_frac
    );
    assert_eq!(
        gain.distinct_producers, 2,
        "#output-file: W1 (two files) counts once; W2 once → 2. got {}",
        gain.distinct_producers
    );
}

/// (e) `largest_contributor_bytes` identifies a single UBIQUITOUS file dominating
/// the mass — the file-level guard against the empty-`Directory{}`-class artifact.
/// A tiny common header (size 512) appears in ALL 4 actions; a big rlib (size
/// 100000) appears in only 1. The common file contributes 512×4 = 2048; the rlib
/// 100000×1 = 100000 → the rlib is the largest single contributor even though the
/// header matched more actions.
#[test]
fn largest_contributor_surfaces_dominant_file() {
    let header = fd(1, 512); // ubiquitous small file, in every action
    let rlib = fd(2, 100_000); // one big file, in one action
    let actions = vec![
        mk_action(&[header, rlib]),
        mk_action(&[header]),
        mk_action(&[header]),
        mk_action(&[header]),
    ];
    let map = producer_map(&[(header, wid("W1")), (rlib, wid("W1"))]);
    let conn = connected(&[wid("W1")]);

    let gain = compute_output_file_affinity(&actions, &map, &conn);

    assert_eq!(
        gain.matched_bytes,
        512 * 4 + 100_000,
        "#output-file: matched_bytes = header(512)×4 + rlib(100000)×1. got {}",
        gain.matched_bytes
    );
    assert_eq!(
        gain.largest_contributor_bytes, 100_000,
        "#output-file: the rlib (100000×1) is the single largest contributor, \
         even though the header matched more actions (512×4=2048) — this is how a \
         reader spots whether the mass is one hot file. got {}",
        gain.largest_contributor_bytes
    );
    assert_eq!(
        gain.match_frac, 100,
        "#output-file: every action matched the header → 100. got {}",
        gain.match_frac
    );
}

/// Empty-sample guard: no actions → all-zero, no divide-by-zero.
#[test]
fn empty_sample_is_zero() {
    let f = fd(1, 100);
    let map = producer_map(&[(f, wid("W1"))]);
    let conn = connected(&[wid("W1")]);
    let gain = compute_output_file_affinity(&[], &map, &conn);
    assert_eq!(
        gain,
        Default::default(),
        "#output-file: empty sample → default (all-zero) gain, no panic. got {gain:?}"
    );
}
