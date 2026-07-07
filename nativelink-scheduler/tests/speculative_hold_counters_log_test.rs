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

//! (#obs-tuning) OBSERVABILITY-ONLY test for the periodic Stage-A/Stage-B
//! speculative decision-counter info-log.
//!
//! The Stage-A (`speculative_prefetch_*`) and Stage-B (`speculative_hold_*` /
//! `hold_*`) counters are `AtomicU64` on `SchedulerMetrics` but the metrics
//! tree is DARK on the HTTP `/metrics` endpoint (empty on all ports in
//! production). The only scrape surface is journalctl `info!` logs, so a
//! periodic task emits ONE `tag = "speculative_hold_counters"` line per tick.
//!
//! These tests exercise the SAME emit function the periodic task calls
//! (`emit_speculative_hold_counters_log`) — driving the atomics and asserting
//! every counter appears in the emitted line via `tracing_test`'s
//! `logs_contain`. This is the mutation-verifiable contract: dropping any
//! counter field from the `info!` makes the matching `logs_contain` assertion
//! red-fail.

use core::sync::atomic::Ordering;

use nativelink_scheduler::api_worker_scheduler::{
    SchedulerMetrics, emit_speculative_hold_counters_log,
};

/// Drive every Stage-A/Stage-B counter to a DISTINCT value, emit the periodic
/// log line once, and assert each counter's `name=value` pair appears in the
/// emitted `info!`. Distinct values guard against a copy-paste that logs the
/// same atomic twice under two field names.
#[test]
#[tracing_test::traced_test]
fn hold_counters_log_emits_all_stage_a_and_stage_b_counters() {
    let metrics = SchedulerMetrics::default();

    // Stage B hold counters.
    metrics.speculative_hold_count.store(11, Ordering::Relaxed);
    metrics.hold_paid_off.store(22, Ordering::Relaxed);
    metrics.hold_regret.store(33, Ordering::Relaxed);
    metrics.hold_expired.store(44, Ordering::Relaxed);
    // Stage A prefetch counters.
    metrics
        .speculative_prefetch_emitted
        .store(55, Ordering::Relaxed);
    metrics.speculative_prefetch_hit.store(66, Ordering::Relaxed);
    metrics
        .speculative_prefetch_miss
        .store(77, Ordering::Relaxed);
    metrics
        .speculative_prefetch_no_target
        .store(88, Ordering::Relaxed);

    emit_speculative_hold_counters_log(&metrics);

    // Each counter must appear in the emitted line under its documented field
    // name AND with the value read from the atomic. Mutation: dropping any of
    // these fields from the `info!` red-fails the matching assertion.
    assert!(
        logs_contain("speculative_hold_count=11"),
        "periodic log must carry speculative_hold_count"
    );
    assert!(
        logs_contain("hold_paid_off=22"),
        "periodic log must carry hold_paid_off"
    );
    assert!(
        logs_contain("hold_regret=33"),
        "periodic log must carry hold_regret"
    );
    assert!(
        logs_contain("hold_expired=44"),
        "periodic log must carry hold_expired"
    );
    assert!(
        logs_contain("prefetch_emitted=55"),
        "periodic log must carry prefetch_emitted"
    );
    assert!(
        logs_contain("prefetch_hit=66"),
        "periodic log must carry prefetch_hit"
    );
    assert!(
        logs_contain("prefetch_miss=77"),
        "periodic log must carry prefetch_miss"
    );
    assert!(
        logs_contain("prefetch_no_target=88"),
        "periodic log must carry prefetch_no_target"
    );
    // The tag lets the soak operator filter these lines cheaply.
    assert!(
        logs_contain("speculative_hold_counters"),
        "periodic log must carry the tag = \"speculative_hold_counters\" filter marker"
    );
}

/// A freshly-defaulted `SchedulerMetrics` (all counters zero) still emits the
/// line with zero values — the periodic task fires unconditionally on its
/// cadence, so a zero-load fleet is distinguishable from a stalled emitter
/// (the dark-counter trap the log exists to close).
#[test]
#[tracing_test::traced_test]
fn hold_counters_log_emits_zeros_when_idle() {
    let metrics = SchedulerMetrics::default();

    emit_speculative_hold_counters_log(&metrics);

    assert!(
        logs_contain("speculative_hold_count=0"),
        "periodic log must fire with zero counters on an idle fleet"
    );
    assert!(
        logs_contain("prefetch_emitted=0"),
        "periodic log must fire with zero prefetch counters on an idle fleet"
    );
}
