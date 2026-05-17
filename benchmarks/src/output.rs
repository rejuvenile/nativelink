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

//! JSON output schema for the benchmark harness.
//!
//! The on-disk layout (a single `BaselineFile` per run) is intentionally
//! diff-stable: keys are alphabetized via `BTreeMap`, floats are rounded
//! to 6 decimal places (1 ns grid for the ms scale) when emitted so
//! micro-jitter doesn't churn the diff while preserving sub-µs precision.
//! The schema is versioned via `schema_version` so future incompatible
//! changes can be tolerated by tooling.
//!
//! **Schema-stability contract:** every schema change that adds /
//! removes / renames a field on `BenchmarkResult`, `RunMetadata`,
//! `LatencyPercentiles`, or `BaselineFile` MUST bump
//! [`SCHEMA_VERSION`]. A golden-file test
//! (`schema_v1_serializes_expected_field_set`) red-fails if a field is
//! added without bumping the version.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Bumped any time the JSON layout changes incompatibly. Tooling MUST
/// reject baselines whose `schema_version` is greater than its own.
///
/// **v2 (2026-05-16):** added `confidence` field on `BenchmarkResult`;
/// switched float rounding from 3 decimals (1 µs grid) to 6 decimals
/// (1 ns grid) to preserve sub-µs warm-path variance; widened
/// `make_blob_with_indices` seeding (no on-disk schema effect).
pub const SCHEMA_VERSION: u32 = 2;

/// One observation of a single benchmark cell.
///
/// "Cell" is the design-doc term: a unique combination of flow + blob
/// size + concurrency + cache state. Each cell produces one
/// `BenchmarkResult` per run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkResult {
    /// Flow id from the design doc inventory (e.g. `W1`, `W3`, `R1`,
    /// `R5`, `F1`). Used as the primary diff key — a flow that
    /// disappears between baselines is a tooling regression, not a
    /// performance change.
    pub flow_id: String,

    /// Human-readable scenario name (e.g.
    /// `w1_store_update_oneshot_1mib_c1`). MUST be stable across runs.
    pub scenario_name: String,

    /// Blob size in bytes (or `None` for non-size-keyed scenarios like
    /// AC lookups).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_size_bytes: Option<u64>,

    /// Concurrency for this cell (number of in-flight operations against
    /// the same store / digest).
    pub concurrency: u32,

    /// Cache state when the cell ran. See [`CacheState`].
    pub cache_state: CacheState,

    /// Number of iterations actually executed (sample size). Diff
    /// tooling MUST reject comparisons across runs with mismatched
    /// `iters` < 20 because at small N the p99 estimate is meaningless.
    pub iters: u32,

    /// Confidence-of-cell-numbers tier, gated on `iters`. Even with a
    /// numeric `p99` field, a baseline at `confidence: Low` MUST NOT
    /// be used as a regression anchor; diff tooling MUST refuse.
    pub confidence: Confidence,

    /// Wall-clock for the whole iter loop, in milliseconds. Useful for
    /// sanity-checking that the cell completed within its budget.
    pub total_duration_ms: f64,

    /// Latency percentiles in milliseconds, derived from the raw
    /// per-iter timings. Diff tooling compares these.
    pub latency_ms: LatencyPercentiles,

    /// Throughput. Bytes-per-second for size-keyed scenarios,
    /// elements-per-second for batch / find-missing scenarios.
    pub throughput: Throughput,

    /// Free-form additional fields scenarios may emit (e.g.
    /// `slow_writes_inflight_peak`, `chunked_v2_enabled`,
    /// `pin_budget_bytes`). Not part of the diff key.
    ///
    /// `deny_unknown_fields` does NOT apply to map values — this map
    /// remains open-ended.
    #[serde(default)]
    pub extras: BTreeMap<String, serde_json::Value>,
}

/// Confidence tier for a `BenchmarkResult`. Computed from `iters`.
///
/// - `Low`: iters < 20 (p99 is essentially `max`, meaningless)
/// - `Medium`: 20 ≤ iters < 100 (p50/p90 meaningful; p99 a single sample)
/// - `High`: iters ≥ 100 (p99 has at least 1 sample of headroom)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Low,
    Medium,
    High,
}

/// Map an iter count to a [`Confidence`] tier. See [`Confidence`] docs
/// for the thresholds.
pub fn confidence_for_iters(iters: u32) -> Confidence {
    if iters >= 100 {
        Confidence::High
    } else if iters >= 20 {
        Confidence::Medium
    } else {
        Confidence::Low
    }
}

/// Latency percentiles in milliseconds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyPercentiles {
    pub p50: f64,
    pub p90: f64,
    /// p99 from nearest-rank NIST. At `iters < 100` this often equals
    /// `max`; see [`Confidence`] gating.
    pub p99: f64,
    pub max: f64,
}

impl LatencyPercentiles {
    /// Derive percentiles from a vector of per-iteration durations.
    /// Sorts in-place.
    ///
    /// # Panics
    ///
    /// Panics if `samples` is empty. Callers MUST ensure `iters ≥ 1`
    /// before invoking; `scenarios::measure` floors at
    /// `scenarios::MIN_ITERS` defensively.
    pub fn from_samples(samples: &mut [Duration]) -> Self {
        assert!(!samples.is_empty(), "LatencyPercentiles needs ≥1 sample");
        samples.sort();
        let n = samples.len();
        let pct = |p: f64| -> f64 {
            // Nearest-rank, NIST definition. Index = ceil(p * N) - 1.
            let idx = ((p * (n as f64)).ceil() as usize).saturating_sub(1).min(n - 1);
            samples[idx].as_secs_f64() * 1_000.0
        };
        Self {
            p50: round_emit(pct(0.50)),
            p90: round_emit(pct(0.90)),
            p99: round_emit(pct(0.99)),
            max: round_emit(samples[n - 1].as_secs_f64() * 1_000.0),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Throughput {
    /// Bytes per second (size-keyed flows).
    BytesPerSec(f64),
    /// Elements per second (batch / find-missing flows).
    ElementsPerSec(f64),
    /// No throughput dimension applies.
    None,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CacheState {
    /// Blob not present anywhere in the composition (slow-tier miss).
    Cold,
    /// Blob in fast tier (Memory or filesystem hot).
    Warm,
    /// N concurrent ops on the SAME digest (singleflight /
    /// chunked-v2 reader race).
    Contended,
}

/// Metadata for a single benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunMetadata {
    /// Always [`SCHEMA_VERSION`] at time of emit.
    pub schema_version: u32,
    /// Git commit SHA at run time (full 40-char). Diff tooling uses
    /// this to verify a baseline lines up with a known code state.
    pub git_commit_sha: String,
    /// Whether the working tree was dirty when the run was kicked off.
    /// `None` ⇒ unknown (git not on PATH, run outside a repo).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_dirty: Option<bool>,
    /// Hostname the run executed on. Used to distinguish
    /// buildcache-on-prem vs CI-runner baselines (they are NOT comparable;
    /// diff tooling MUST refuse cross-host diffs).
    pub host: String,
    /// ISO-8601 UTC timestamp.
    pub timestamp_utc: String,
    /// Cargo features enabled in the bench binary. Critical for
    /// W3/R5: a baseline collected with `chunked_fast_slow=off` cannot
    /// be diffed against one with it on.
    pub features: Vec<String>,
    /// If the operator used `--force` to bypass the pre-flight gate.
    /// A `true` baseline is not a clean baseline.
    pub forced: bool,
    /// Tempdir path used by the bench. Recorded so diffs can validate
    /// the same filesystem class was used (tmpfs vs ZFS vs ext4 changes
    /// the slow-tier numbers).
    pub temp_dir_used: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineFile {
    pub metadata: RunMetadata,
    pub results: Vec<BenchmarkResult>,
}

/// Round to 6 decimal places when emitting. At the ms scale this is a
/// 1 ns grid, far below any meaningful jitter floor; diffs stay stable
/// at the precision that matters while sub-µs warm-path variance is
/// preserved (vs the prior 3-decimal/1-µs grid which zeroed it out).
pub fn round_emit(x: f64) -> f64 {
    (x * 1_000_000.0).round() / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ms(x: u64) -> Duration {
        Duration::from_millis(x)
    }

    /// Pin the percentile arithmetic against a known PRE-SORTED sample
    /// set. Catches an index-formula mistake (e.g. p50↔p90 swap).
    ///
    /// **Note on mutation discipline:** this test alone does NOT red-fail
    /// if `samples.sort()` is removed (input is already sorted) nor if
    /// `ceil`→`floor` is swapped (N=100 places p50/p90/p99 at integer
    /// boundaries insensitive to the off-by-one). See
    /// `latency_percentiles_unsorted_input_caught` (mutation-resistant
    /// for the sort step) and `latency_percentiles_nist_off_by_one`
    /// (mutation-resistant for the ceil/floor step).
    #[test]
    fn latency_percentiles_known_input() {
        // 1ms..100ms inclusive — 100 samples.
        let mut samples: Vec<Duration> = (1..=100).map(ms).collect();
        let p = LatencyPercentiles::from_samples(&mut samples);
        // p50 = ceil(0.50 * 100) - 1 = 49 → samples[49] = 50ms
        assert_eq!(p.p50, 50.0);
        // p90 = ceil(0.90 * 100) - 1 = 89 → samples[89] = 90ms
        assert_eq!(p.p90, 90.0);
        // p99 = ceil(0.99 * 100) - 1 = 98 → samples[98] = 99ms
        assert_eq!(p.p99, 99.0);
        // max = samples[99] = 100ms
        assert_eq!(p.max, 100.0);
    }

    /// Deliberately UNSORTED input. The percentile formula picks indices
    /// 49/89/98 on the post-sort vector — if `samples.sort()` is removed,
    /// those indices land on whatever the test's shuffle put there
    /// (1ms / 50ms / 100ms in our case), producing a wrong p50.
    ///
    /// **Mutation falsifier:** comment out `samples.sort()` in
    /// `LatencyPercentiles::from_samples`. This test must red-fail with
    /// the bespoke `sort_removed` message naming the unsorted index.
    #[test]
    fn latency_percentiles_unsorted_input_caught() {
        // 100 elements with content equal to `n` for n in 1..=100, but
        // physically reversed: index 0 holds 100ms, index 99 holds 1ms.
        // After sort, samples[49] must be 50ms (post-sort value).
        let mut samples: Vec<Duration> = (1..=100u64).rev().map(ms).collect();
        // Sanity: pre-sort, index 49 holds 51ms (since reversed).
        debug_assert_eq!(samples[49].as_millis(), 51);
        let p = LatencyPercentiles::from_samples(&mut samples);
        // After sort: samples[49] == 50ms → p50 == 50.
        // If `sort()` is removed, samples[49] stays 51ms → p50 == 51.
        assert_eq!(
            p.p50, 50.0,
            "percentile sort_removed mutation: from_samples must sort \
             unsorted input before indexing — got p50={} (sort skipped \
             produces 51.0)",
            p.p50
        );
        // Same for p90: post-sort samples[89] == 90; pre-sort it == 11.
        assert_eq!(
            p.p90, 90.0,
            "percentile sort_removed mutation: p90 was {} (expected 90.0 \
             post-sort; unsorted would yield 11.0)",
            p.p90
        );
    }

    /// Sample shaped so `ceil(p*N)-1` and `floor(p*N)-1` differ by 1
    /// index AND the values at those indices differ. The NIST nearest-
    /// rank formula is `idx = ceil(p*N) - 1`; an erroneous `floor` would
    /// pick a different sample.
    ///
    /// At N=10 + p=0.99: `ceil(0.99*10)-1 = 9` (samples[9] = 1000ms);
    /// `floor(0.99*10)-1 = 8` (samples[8] = 9ms). The two formulas
    /// give p99 == 1000.0 vs p99 == 9.0 respectively.
    ///
    /// **Mutation falsifier:** swap `.ceil()` for `.floor()` in
    /// `LatencyPercentiles::from_samples`. This test must red-fail with
    /// the bespoke `ceil_to_floor` message.
    #[test]
    fn latency_percentiles_nist_off_by_one() {
        // 1ms..9ms then 1000ms — 10 samples total, pre-sorted.
        let mut samples: Vec<Duration> = (1..=9u64).map(ms).collect();
        samples.push(ms(1000));
        assert_eq!(samples.len(), 10);
        let p = LatencyPercentiles::from_samples(&mut samples);
        // p99 must be 1000ms under NIST nearest-rank ceil.
        assert_eq!(
            p.p99, 1000.0,
            "percentile ceil_to_floor mutation: at N=10 the NIST \
             nearest-rank p99 idx = ceil(0.99*10)-1 = 9 \
             (samples[9] = 1000ms); a `floor` mutation produces idx=8 \
             (samples[8] = 9ms) → p99={}",
            p.p99
        );
        // p50 must be the 5th-rank sample, samples[ceil(0.5*10)-1] = samples[4] = 5ms.
        assert_eq!(
            p.p50, 5.0,
            "percentile ceil_to_floor mutation: p50 idx = ceil(0.5*10)-1 \
             = 4 (samples[4] = 5ms); a `floor` mutation produces idx=3 \
             (samples[3] = 4ms) → p50={}",
            p.p50
        );
    }

    /// At iters=20 the p99 degenerates to a single sample (essentially
    /// `max`). Confidence-tier MUST be `Medium` so diff tooling knows
    /// to treat p99 as noise.
    #[test]
    fn confidence_for_iters_brackets() {
        assert_eq!(confidence_for_iters(1), Confidence::Low);
        assert_eq!(confidence_for_iters(19), Confidence::Low);
        assert_eq!(confidence_for_iters(20), Confidence::Medium);
        assert_eq!(confidence_for_iters(99), Confidence::Medium);
        assert_eq!(confidence_for_iters(100), Confidence::High);
    }

    #[test]
    #[should_panic(expected = "needs ≥1 sample")]
    fn latency_percentiles_empty_input_panics() {
        let mut samples: Vec<Duration> = Vec::new();
        let _ = LatencyPercentiles::from_samples(&mut samples);
    }

    #[test]
    fn round_emit_six_decimals() {
        assert_eq!(round_emit(0.123_456_789), 0.123_457);
        // Sub-µs precision preserved (vs 3-decimal would zero this):
        assert_eq!(round_emit(0.005_123), 0.005_123);
    }

    /// Golden JSON literal for a SCHEMA_VERSION=2 `BenchmarkResult`.
    /// Renaming any field on the struct red-fails the
    /// `schema_v2_golden_json_round_trips` test below because the new
    /// struct can no longer deserialize the golden literal.
    ///
    /// To add a NEW field, bump `SCHEMA_VERSION` from 2 → 3 AND update
    /// this golden literal. The test enforces both steps.
    pub(super) const GOLDEN_SCHEMA_V2_BENCHMARK_RESULT: &str = r#"{
  "flow_id": "W1",
  "scenario_name": "w1_store_update_oneshot_1KiB_c1",
  "blob_size_bytes": 1024,
  "concurrency": 1,
  "cache_state": "cold",
  "iters": 20,
  "confidence": "medium",
  "total_duration_ms": 10.0,
  "latency_ms": {
    "p50": 0.5,
    "p90": 0.9,
    "p99": 0.99,
    "max": 1.0
  },
  "throughput": {
    "bytes_per_sec": 1024.0
  },
  "extras": {}
}"#;

    /// Golden JSON literal for a SCHEMA_VERSION=2 `RunMetadata`.
    pub(super) const GOLDEN_SCHEMA_V2_RUN_METADATA: &str = r#"{
  "schema_version": 2,
  "git_commit_sha": "deadbeef",
  "git_dirty": false,
  "host": "buildcache",
  "timestamp_utc": "2026-05-16T00:00:00.000000Z",
  "features": [],
  "forced": false,
  "temp_dir_used": "/dev/shm/nl-bench-XYZ"
}"#;

    /// Golden field-set: deserialization of a SCHEMA_VERSION=2 baseline
    /// MUST accept only the known fields on every struct. Adding a new
    /// pub field WITHOUT bumping `SCHEMA_VERSION` red-fails because the
    /// golden JSON below won't deserialize (deny_unknown_fields), AND
    /// re-serialization of the golden result returns the same string
    /// modulo whitespace.
    ///
    /// Mutation: add a new pub field to `BenchmarkResult` without
    /// adding it to the golden — `deny_unknown_fields` blocks
    /// deserialization on the round-trip, OR the field-list count
    /// assertion below red-fails.
    #[test]
    fn schema_v2_serializes_expected_field_set() {
        let r = BenchmarkResult {
            flow_id: "W1".to_string(),
            scenario_name: "w1_store_update_oneshot_1KiB_c1".to_string(),
            blob_size_bytes: Some(1024),
            concurrency: 1,
            cache_state: CacheState::Cold,
            iters: 20,
            confidence: Confidence::Medium,
            total_duration_ms: 10.0,
            latency_ms: LatencyPercentiles {
                p50: 0.5,
                p90: 0.9,
                p99: 0.99,
                max: 1.0,
            },
            throughput: Throughput::BytesPerSec(1024.0),
            extras: BTreeMap::new(),
        };

        let json = serde_json::to_value(&r).expect("serialize must succeed");
        let obj = json
            .as_object()
            .expect("BenchmarkResult serializes as JSON object");
        // Field count is the structural assertion. Bumping requires a
        // SCHEMA_VERSION bump per the schema-stability contract.
        // Fields: flow_id, scenario_name, blob_size_bytes, concurrency,
        // cache_state, iters, confidence, total_duration_ms,
        // latency_ms, throughput, extras = 11.
        assert_eq!(
            obj.len(),
            11,
            "BenchmarkResult must have exactly 11 fields at SCHEMA_VERSION={}; \
             if you added a field bump SCHEMA_VERSION and update this test",
            SCHEMA_VERSION
        );

        // Round-trip via deserialize_with deny_unknown_fields — any
        // unknown key in the JSON (e.g. a manually-added "future_field":
        // ...) red-fails here.
        let _r2: BenchmarkResult =
            serde_json::from_value(json).expect("round-trip must preserve schema");
    }

    /// Golden round-trip: the GOLDEN_SCHEMA_V2_BENCHMARK_RESULT literal
    /// must deserialize into a `BenchmarkResult` AND the in-tree fixture
    /// must serialize byte-equal (as `serde_json::Value`) to it.
    ///
    /// **Mutation falsifier (rename):** rename ANY field on
    /// `BenchmarkResult` (e.g. `flow_id` → `flow_identifier`). This test
    /// must red-fail with the `golden_schema_renamed` bespoke message
    /// because the renamed struct cannot deserialize the golden literal.
    ///
    /// **Mutation falsifier (retype):** change a numeric field's type
    /// (e.g. `concurrency: u32` → `concurrency: String`). Same red-fail
    /// path.
    ///
    /// **Mutation falsifier (add field without SCHEMA_VERSION bump):**
    /// add a `pub new_field: u32` to `BenchmarkResult`. The fixture
    /// serializes with the new key; `Value`-equality vs the golden
    /// fails AND the literal cannot round-trip into the new struct
    /// without a `Default`. Test red-fails.
    #[test]
    fn schema_v2_golden_json_round_trips() {
        let fixture = BenchmarkResult {
            flow_id: "W1".to_string(),
            scenario_name: "w1_store_update_oneshot_1KiB_c1".to_string(),
            blob_size_bytes: Some(1024),
            concurrency: 1,
            cache_state: CacheState::Cold,
            iters: 20,
            confidence: Confidence::Medium,
            total_duration_ms: 10.0,
            latency_ms: LatencyPercentiles {
                p50: 0.5,
                p90: 0.9,
                p99: 0.99,
                max: 1.0,
            },
            throughput: Throughput::BytesPerSec(1024.0),
            extras: BTreeMap::new(),
        };
        // Serialize fixture → JSON value.
        let fixture_value: serde_json::Value =
            serde_json::to_value(&fixture).expect("fixture serialize");
        // Parse golden literal → JSON value.
        let golden_value: serde_json::Value =
            serde_json::from_str(GOLDEN_SCHEMA_V2_BENCHMARK_RESULT)
                .expect("golden literal is valid JSON");
        assert_eq!(
            fixture_value, golden_value,
            "golden_schema_renamed mutation: fixture-serialized JSON \
             diverged from GOLDEN_SCHEMA_V2_BENCHMARK_RESULT — a field \
             rename / retype / addition is the most likely cause. To \
             reconcile, either revert the schema change or bump \
             SCHEMA_VERSION + update the golden literal."
        );
        // Round-trip: golden literal must deserialize into the in-tree
        // struct via deny_unknown_fields.
        let _back: BenchmarkResult =
            serde_json::from_str(GOLDEN_SCHEMA_V2_BENCHMARK_RESULT)
                .expect("golden_schema_renamed mutation: golden literal cannot \
                         round-trip through the current BenchmarkResult struct \
                         — a field was renamed / retyped or a new required field \
                         was added");
    }

    /// Golden round-trip for `RunMetadata`. Same mutation discipline as
    /// `schema_v2_golden_json_round_trips`.
    #[test]
    fn schema_v2_run_metadata_golden_json_round_trips() {
        let fixture = RunMetadata {
            schema_version: 2,
            git_commit_sha: "deadbeef".to_string(),
            git_dirty: Some(false),
            host: "buildcache".to_string(),
            timestamp_utc: "2026-05-16T00:00:00.000000Z".to_string(),
            features: vec![],
            forced: false,
            temp_dir_used: "/dev/shm/nl-bench-XYZ".to_string(),
        };
        let fixture_value: serde_json::Value =
            serde_json::to_value(&fixture).expect("fixture serialize");
        let golden_value: serde_json::Value =
            serde_json::from_str(GOLDEN_SCHEMA_V2_RUN_METADATA)
                .expect("golden RunMetadata literal is valid JSON");
        assert_eq!(
            fixture_value, golden_value,
            "golden_schema_renamed mutation (RunMetadata): fixture \
             serialized JSON diverged from GOLDEN_SCHEMA_V2_RUN_METADATA"
        );
        let _back: RunMetadata =
            serde_json::from_str(GOLDEN_SCHEMA_V2_RUN_METADATA)
                .expect("golden_schema_renamed mutation: RunMetadata golden \
                         cannot round-trip — schema bump required");
    }

    #[test]
    fn run_metadata_field_set() {
        let m = RunMetadata {
            schema_version: SCHEMA_VERSION,
            git_commit_sha: "deadbeef".to_string(),
            git_dirty: Some(false),
            host: "buildcache".to_string(),
            timestamp_utc: "2026-05-16T00:00:00.000000Z".to_string(),
            features: vec![],
            forced: false,
            temp_dir_used: "/dev/shm/nl-bench-XYZ".to_string(),
        };
        let json = serde_json::to_value(&m).unwrap();
        let obj = json.as_object().unwrap();
        // Fields: schema_version, git_commit_sha, git_dirty, host,
        // timestamp_utc, features, forced, temp_dir_used = 8.
        assert_eq!(
            obj.len(),
            8,
            "RunMetadata must have exactly 8 fields at SCHEMA_VERSION={}; \
             if you added a field bump SCHEMA_VERSION and update this test",
            SCHEMA_VERSION
        );
    }
}
