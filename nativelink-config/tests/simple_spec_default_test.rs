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

//! Pins `SimpleSpec::default()` to "deserialize of an empty config".
//!
//! `#[derive(Default)]` on `SimpleSpec` was LOAD-BLIND: derive ignores the
//! `#[serde(default = "fn")]` attributes (those fire only on DEserialization),
//! so `SimpleSpec::default()` yielded `load_byte_cost == 0`,
//! `assume_core_count == 0`, `worker_match_logging_interval_s == 0` — none of
//! which match a deserialized-empty config. ~47 scheduler tests build the
//! scheduler with `SimpleSpec::default()` and therefore ran with
//! `load_byte_cost == 0` → `load_penalty == 0` for every worker → the
//! load-aware selection blend was never exercised (root cause of the flaky
//! `cache_affinity_least_loaded_holder_wins_tier1_test`). Prod is unaffected
//! (prod deserializes config → the serde defaults fire).
//!
//! The fix replaces `#[derive(Default)]` with a manual `impl Default` that
//! mirrors the serde defaults exactly. This test is the LOAD-BEARING pin: it
//! asserts, field by field, that `SimpleSpec::default()` equals
//! `serde_json5::from_str::<SimpleSpec>("{}")`. If any field of the manual
//! `impl Default` drifts from its `#[serde(default = ...)]`, this test
//! red-fails on that specific field.

use nativelink_config::schedulers::{PlacementMode, SimpleSpec, WorkerAllocationStrategy};
use pretty_assertions::assert_eq;

/// `SimpleSpec::default()` MUST equal a deserialized empty config, field for
/// field. Compared explicitly (not via a `PartialEq` derive) so that a drift
/// in ANY single field names that field in the failure, and so we do not have
/// to derive `PartialEq` across the sibling config enums.
#[test]
fn simple_spec_default_matches_deserialize_empty() {
    let derived = SimpleSpec::default();
    // Empty JSON5 object: every field falls to its serde default. This is the
    // exact shape prod hits for a `{ "type": "simple" }` scheduler with no
    // tuning keys set.
    let deserialized: SimpleSpec =
        serde_json5::from_str("{}").expect("empty SimpleSpec must deserialize");

    assert_eq!(
        derived.supported_platform_properties.is_none(),
        deserialized.supported_platform_properties.is_none(),
        "supported_platform_properties default drift (both should be None)"
    );
    assert_eq!(
        derived.retain_completed_for_s, deserialized.retain_completed_for_s,
        "retain_completed_for_s default drift"
    );
    assert_eq!(
        derived.client_action_timeout_s, deserialized.client_action_timeout_s,
        "client_action_timeout_s default drift"
    );
    assert_eq!(
        derived.worker_timeout_s, deserialized.worker_timeout_s,
        "worker_timeout_s default drift"
    );
    assert_eq!(
        derived.max_action_executing_timeout_s, deserialized.max_action_executing_timeout_s,
        "max_action_executing_timeout_s default drift"
    );
    assert_eq!(
        derived.max_job_retries, deserialized.max_job_retries,
        "max_job_retries default drift"
    );
    assert_eq!(
        matches!(
            derived.allocation_strategy,
            WorkerAllocationStrategy::LeastRecentlyUsed
        ),
        matches!(
            deserialized.allocation_strategy,
            WorkerAllocationStrategy::LeastRecentlyUsed
        ),
        "allocation_strategy default drift (both should be LeastRecentlyUsed)"
    );
    assert_eq!(
        derived.experimental_backend.is_none(),
        deserialized.experimental_backend.is_none(),
        "experimental_backend default drift (both should be None)"
    );
    assert_eq!(
        derived.worker_match_logging_interval_s, deserialized.worker_match_logging_interval_s,
        "worker_match_logging_interval_s default drift (serde default is 10, \
         not the type default 0)"
    );
    assert_eq!(
        derived.max_matches_per_client_per_cycle, deserialized.max_matches_per_client_per_cycle,
        "max_matches_per_client_per_cycle default drift"
    );
    assert_eq!(
        derived.cas_store, deserialized.cas_store,
        "cas_store default drift (both should be None)"
    );
    assert_eq!(
        derived.load_byte_cost, deserialized.load_byte_cost,
        "load_byte_cost default drift — this is THE field the bug was about; \
         serde default is 512*1024, the type default is 0"
    );
    assert_eq!(
        derived.assume_core_count, deserialized.assume_core_count,
        "assume_core_count default drift (serde default is 8, not the type \
         default 0)"
    );
    assert_eq!(
        derived.p_headroom_gate_enabled, deserialized.p_headroom_gate_enabled,
        "p_headroom_gate_enabled default drift (both should be true — \
         ENABLED by default, drift-proof, per user 2026-07-07)"
    );
    assert_eq!(
        derived.p_idle_threshold_pct, deserialized.p_idle_threshold_pct,
        "p_idle_threshold_pct default drift (both should be 0 = override OFF, \
         exact v1 behavior)"
    );
    assert_eq!(
        derived.p_headroom_override_factor, deserialized.p_headroom_override_factor,
        "p_headroom_override_factor default drift — serde default is 2 \
         (default_p_headroom_override_factor); a bare #[serde(default)] would \
         yield 0 and silently disable the override"
    );
    assert_eq!(
        derived.enable_p2p_input_prefetch, deserialized.enable_p2p_input_prefetch,
        "enable_p2p_input_prefetch default drift (both should be false = P2P \
         input prefetch OFF, byte-identical to today)"
    );
    assert_eq!(
        derived.pending_affinity_probe_enabled, deserialized.pending_affinity_probe_enabled,
        "pending_affinity_probe_enabled default drift — serde default is FALSE \
         (default_pending_affinity_probe_enabled); the quadratic observability probe \
         is OPT-IN, so an absent-in-config scheduler must leave it OFF"
    );
    assert_eq!(
        derived.enable_speculative_prefetch, deserialized.enable_speculative_prefetch,
        "enable_speculative_prefetch default drift (both should be true — ENABLED \
         by default, drift-proof, per user 2026-07-07; config `false` is the kill-switch)"
    );
    assert_eq!(
        derived.enable_speculative_hold, deserialized.enable_speculative_hold,
        "enable_speculative_hold default drift (both should be true — ENABLED \
         by default, drift-proof, per user 2026-07-07; config `false` is the kill-switch)"
    );
    assert_eq!(
        derived.scheduler_decision_trace_enabled, deserialized.scheduler_decision_trace_enabled,
        "scheduler_decision_trace_enabled default drift (both should be false — the \
         dispatch-decision diagnostic dump is OFF by default; an operator turns it ON \
         briefly via config to diagnose a placement question, then OFF)"
    );
}

/// Direct assertions on the concrete default values, so the intent is legible
/// without cross-referencing the serde attributes. These are the values a
/// deserialized-empty config produces (verified by the equivalence test
/// above); duplicating them here makes an accidental change to BOTH the serde
/// default fn and the manual `impl Default` (which would slip past the
/// equivalence test) still fail here.
#[test]
fn simple_spec_default_concrete_values() {
    let spec = SimpleSpec::default();
    assert_eq!(
        spec.load_byte_cost,
        512 * 1024,
        "load_byte_cost default must be 512 KiB (default_load_byte_cost)"
    );
    assert_eq!(
        spec.assume_core_count, 8,
        "assume_core_count default must be 8 (default_assume_core_count)"
    );
    assert_eq!(
        spec.worker_match_logging_interval_s, 10,
        "worker_match_logging_interval_s default must be 10 \
         (default_worker_match_logging_interval_s)"
    );
    assert!(
        spec.p_headroom_gate_enabled,
        "p_headroom_gate_enabled default must be TRUE (ENABLED by default, \
         drift-proof, per user 2026-07-07; config `false` is the kill-switch)"
    );
    assert_eq!(
        spec.p_idle_threshold_pct, 0,
        "p_idle_threshold_pct default must be 0 (override OFF → exact v1: \
         `p_load < 0` is never true, clause 3 never fires)"
    );
    assert_eq!(
        spec.p_headroom_override_factor, 2,
        "p_headroom_override_factor default must be 2 \
         (default_p_headroom_override_factor); NOT the u32 type default 0, \
         which would make the ceiling `p_core_count * 0 == 0` and disable the \
         override"
    );
    assert!(
        !spec.enable_p2p_input_prefetch,
        "enable_p2p_input_prefetch default must be false (P2P input prefetch \
         OFF until an operator enables it — never worse than today)"
    );
    assert!(
        !spec.pending_affinity_probe_enabled,
        "pending_affinity_probe_enabled default must be FALSE \
         (default_pending_affinity_probe_enabled) — the quadratic observability probe \
         is OPT-IN (user decision 2026-07-06); the deployed prod config leaves the \
         flag absent → probe OFF → the 20-27s do_try_match collapse cannot occur"
    );
    assert!(
        !spec.scheduler_decision_trace_enabled,
        "scheduler_decision_trace_enabled default must be FALSE — the dispatch-decision \
         diagnostic dump is a short-lived operator tool, OFF unless explicitly enabled \
         in config; a default-ON would emit the trace on every fleet with no operator ask"
    );
    // (#sched-cpu-first) The placement mode defaults to cache-affinity-first, so
    // an absent-in-config scheduler is byte-identical to the pre-#sched-cpu-first
    // matcher until an operator opts into CpuIdleFirst. Asserted via `matches!`
    // (mirroring the `WorkerAllocationStrategy` sibling above) because
    // `PlacementMode` carries no `PartialEq` — the code uses only `matches!`.
    assert!(
        matches!(spec.placement_mode, PlacementMode::CacheAffinityFirst),
        "placement_mode default must be CacheAffinityFirst (PlacementMode::default) \
         — byte-identical to today until an operator selects CpuIdleFirst"
    );
    assert_eq!(
        spec.cpu_first_synthetic_pct_per_task, 25,
        "cpu_first_synthetic_pct_per_task default must be 25 \
         (default_cpu_first_synthetic_pct_per_task); a bare #[serde(default)] would \
         yield 0 and disable the anti-pile synthetic bridge under CpuIdleFirst"
    );
}
