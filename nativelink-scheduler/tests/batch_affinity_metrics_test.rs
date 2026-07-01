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

//! Tests for the OBSERVABILITY-ONLY batch-scheduling dir-cache-affinity
//! profitability probe (`#batch-affinity`).
//!
//! Two dimensions:
//!   (A) instantaneous co-location surplus over the CURRENT pending set —
//!       pure fn `colocation_surplus`.
//!   (B) temporal "delay value" — how often a related task arrives within a
//!       small window of a peer — pure fn `is_within_affinity_window` +
//!       the pinned window constant `AFFINITY_ARRIVAL_WINDOW`.
//!
//! Window timestamps are `SystemTime` (the type the scheduler's injectable
//! clock — `now_fn().now()` — produces in both prod and mock-clock tests), so
//! these tests drive fixed `SystemTime` values with no wall-clock dependence.
//!
//! These are pure functions so the exact arithmetic (surplus math, max-group,
//! window boundary) is unit-testable WITHOUT driving a real scheduler. The
//! metric is observability-only: nothing here asserts a scheduling behavior
//! change (there is none).

use core::time::Duration;
use std::time::SystemTime;

use nativelink_scheduler::simple_scheduler::{
    AFFINITY_ARRIVAL_WINDOW, MAX_PENDING_AFFINITY_SAMPLE, RECENT_ROOTS_MAX_ENTRIES,
    RecentRootsWindow, colocation_surplus, is_within_affinity_window,
};
use nativelink_util::common::DigestInfo;

/// Build a distinct `DigestInfo` from a single discriminator byte, so
/// `[A, A, B, C]`-style fixtures read clearly.
fn root(discriminator: u8) -> DigestInfo {
    DigestInfo::new([discriminator; 32], 1)
}

/// A deterministic `SystemTime` base far enough above `UNIX_EPOCH` that
/// subtracting the window never underflows.
fn base_time() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(10_000)
}

// ────────────────────────────── (A) colocation_surplus ──────────────────────

#[test]
fn colocation_surplus_mixed_group() {
    // [A, A, B, C]: 4 pending ops, 3 distinct roots → surplus 1
    // (the second A could reuse the first A's peer locality). Largest
    // same-root group is the two A's → max_group 2.
    let (surplus, max_group) =
        colocation_surplus(&[root(b'A'), root(b'A'), root(b'B'), root(b'C')]);
    assert_eq!(
        surplus, 1,
        "#batch-affinity: surplus must be n - distinct = 4 - 3 = 1 for [A,A,B,C]"
    );
    assert_eq!(
        max_group, 2,
        "#batch-affinity: max_group must be 2 (the two A's) for [A,A,B,C]"
    );
}

#[test]
fn colocation_surplus_all_distinct_is_zero() {
    // No two pending ops share a root → greedy one-at-a-time loses nothing;
    // batch scheduling has zero co-location to exploit.
    let (surplus, max_group) =
        colocation_surplus(&[root(b'A'), root(b'B'), root(b'C'), root(b'D')]);
    assert_eq!(
        surplus, 0,
        "#batch-affinity: all-distinct roots must yield surplus 0"
    );
    assert_eq!(
        max_group, 1,
        "#batch-affinity: all-distinct roots must yield max_group 1"
    );
}

#[test]
fn colocation_surplus_all_same_is_n_minus_one() {
    // All 4 share one root → 3 of the 4 assignments could reuse a peer's
    // locality → surplus n-1 = 3; the single group covers all 4.
    let (surplus, max_group) =
        colocation_surplus(&[root(b'A'), root(b'A'), root(b'A'), root(b'A')]);
    assert_eq!(
        surplus, 3,
        "#batch-affinity: all-same (n=4) must yield surplus n-1 = 3"
    );
    assert_eq!(
        max_group, 4,
        "#batch-affinity: all-same (n=4) must yield max_group 4"
    );
}

#[test]
fn colocation_surplus_empty_is_zero() {
    let (surplus, max_group) = colocation_surplus(&[]);
    assert_eq!(
        surplus, 0,
        "#batch-affinity: empty pending set must yield surplus 0"
    );
    assert_eq!(
        max_group, 0,
        "#batch-affinity: empty pending set must yield max_group 0"
    );
}

#[test]
fn colocation_surplus_single_op_is_zero() {
    let (surplus, max_group) = colocation_surplus(&[root(b'A')]);
    assert_eq!(
        surplus, 0,
        "#batch-affinity: a single pending op has no peer to batch with → surplus 0"
    );
    assert_eq!(
        max_group, 1,
        "#batch-affinity: a single pending op is its own group of 1 → max_group 1"
    );
}

/// (FIX 4 / F3) At exactly `MAX_PENDING_AFFINITY_SAMPLE` distinct roots the
/// caller samples only the prefix; `colocation_surplus` over a 512-distinct
/// prefix is 0 (all distinct). This pins the sample-cap constant and documents
/// that at saturation the surplus is a LOWER BOUND on the true pending-set
/// surplus (the caller reports `sampled_ops == cap` so operators see it).
#[test]
fn colocation_surplus_at_sample_cap_all_distinct() {
    // The dim-A caller feeds at most MAX_PENDING_AFFINITY_SAMPLE roots. Build a
    // fully-distinct prefix of exactly that length: surplus 0, max_group 1.
    let mut roots = Vec::with_capacity(MAX_PENDING_AFFINITY_SAMPLE);
    for i in 0u64..(MAX_PENDING_AFFINITY_SAMPLE as u64) {
        let mut hash = [0u8; 32];
        hash[0..8].copy_from_slice(&i.to_le_bytes());
        roots.push(DigestInfo::new(hash, 1));
    }
    assert_eq!(
        roots.len(),
        MAX_PENDING_AFFINITY_SAMPLE,
        "#batch-affinity: the dim-A sample cap must be {MAX_PENDING_AFFINITY_SAMPLE}"
    );
    let (surplus, max_group) = colocation_surplus(&roots);
    assert_eq!(
        surplus, 0,
        "#batch-affinity: a fully-distinct 512-root sample has surplus 0"
    );
    assert_eq!(
        max_group, 1,
        "#batch-affinity: a fully-distinct 512-root sample has max_group 1"
    );
}

#[test]
fn sample_cap_const_is_512() {
    // Pin the sample cap so a doc-comment edit cannot silently drift the value
    // the dim-A `sampled_ops` saturation keys on.
    assert_eq!(
        MAX_PENDING_AFFINITY_SAMPLE, 512,
        "#batch-affinity: MAX_PENDING_AFFINITY_SAMPLE must be 512"
    );
}

// ─────────────────────── (B) is_within_affinity_window ───────────────────────

#[test]
fn window_boundary_exactly_at_window_is_within() {
    let now = base_time();
    let last_seen = now - AFFINITY_ARRIVAL_WINDOW;
    assert!(
        is_within_affinity_window(now, last_seen, AFFINITY_ARRIVAL_WINDOW),
        "#batch-affinity: an arrival exactly AFFINITY_ARRIVAL_WINDOW after the peer \
         must count as within-window (closed interval)"
    );
}

#[test]
fn window_just_past_is_not_within() {
    let now = base_time();
    let last_seen = now - (AFFINITY_ARRIVAL_WINDOW + Duration::from_millis(1));
    assert!(
        !is_within_affinity_window(now, last_seen, AFFINITY_ARRIVAL_WINDOW),
        "#batch-affinity: an arrival 1ms past the window must NOT count — a delay \
         of AFFINITY_ARRIVAL_WINDOW would not have captured it"
    );
}

#[test]
fn window_well_within_is_within() {
    let now = base_time();
    let last_seen = now - Duration::from_millis(10);
    assert!(
        is_within_affinity_window(now, last_seen, AFFINITY_ARRIVAL_WINDOW),
        "#batch-affinity: a 10ms-apart arrival is comfortably within the 250ms window"
    );
}

#[test]
fn window_peer_in_future_is_within() {
    // A monotonic-clock hiccup (peer stored AFTER `now`) must not panic and is
    // treated as within-window (duration saturates to 0 <= window).
    let now = base_time();
    let last_seen = now + Duration::from_millis(5);
    assert!(
        is_within_affinity_window(now, last_seen, AFFINITY_ARRIVAL_WINDOW),
        "#batch-affinity: a peer timestamp in the future must saturate to within-window, not panic"
    );
}

#[test]
fn affinity_window_const_is_250ms() {
    // Pin the window constant so a doc-comment/description edit cannot silently
    // drift the value the counter keys on.
    assert_eq!(
        AFFINITY_ARRIVAL_WINDOW,
        Duration::from_millis(250),
        "#batch-affinity: AFFINITY_ARRIVAL_WINDOW must be 250ms"
    );
}

// ─────────────────────── (B) RecentRootsWindow bounded map ───────────────────

#[test]
fn recent_roots_counts_within_window_arrival() {
    let mut window = RecentRootsWindow::new();
    let t0 = base_time();
    // First arrival of A: no peer seen → no batch opportunity.
    assert!(
        !window.record_arrival(root(b'A'), t0),
        "#batch-affinity: first-ever arrival of a root has no peer → not a window match"
    );
    // Second arrival of A, 10ms later: a small delay would have batched them.
    let t1 = t0 + Duration::from_millis(10);
    assert!(
        window.record_arrival(root(b'A'), t1),
        "#batch-affinity: a repeat root within the window IS a captured batch opportunity"
    );
}

#[test]
fn recent_roots_stale_peer_does_not_count() {
    let mut window = RecentRootsWindow::new();
    let t0 = base_time();
    assert!(!window.record_arrival(root(b'A'), t0));
    // Repeat of A but AFTER the window has elapsed → a 250ms delay would not
    // have captured it, so it must not count.
    let t_late = t0 + AFFINITY_ARRIVAL_WINDOW + Duration::from_millis(1);
    assert!(
        !window.record_arrival(root(b'A'), t_late),
        "#batch-affinity: a repeat root past the window is NOT a captured opportunity"
    );
}

#[test]
fn recent_roots_is_bounded() {
    // The map must never exceed its cap regardless of how many distinct roots
    // arrive (network-reachable path → mandatory bound).
    let mut window = RecentRootsWindow::new();
    let t0 = base_time();
    for i in 0u64..(RECENT_ROOTS_MAX_ENTRIES as u64 + 500) {
        // Distinct 32-byte digest per iteration.
        let mut hash = [0u8; 32];
        hash[0..8].copy_from_slice(&i.to_le_bytes());
        window.record_arrival(DigestInfo::new(hash, 1), t0 + Duration::from_nanos(i));
    }
    assert!(
        window.len() <= RECENT_ROOTS_MAX_ENTRIES,
        "#batch-affinity: RecentRootsWindow must stay <= RECENT_ROOTS_MAX_ENTRIES ({}) \
         but held {}",
        RECENT_ROOTS_MAX_ENTRIES,
        window.len()
    );
}
