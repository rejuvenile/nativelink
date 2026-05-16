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
//! to 3 decimal places when emitted so micro-jitter doesn't churn the
//! diff. The schema is versioned via `schema_version` so future
//! incompatible changes can be tolerated by tooling.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Bumped any time the JSON layout changes incompatibly. Tooling MUST
/// reject baselines whose `schema_version` is greater than its own.
pub const SCHEMA_VERSION: u32 = 1;

/// One observation of a single benchmark cell.
///
/// "Cell" is the design-doc term: a unique combination of flow + blob
/// size + concurrency + cache state. Each cell produces one
/// `BenchmarkResult` per run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Flow id from the design doc inventory (e.g. `W1`, `W3`, `R1`,
    /// `R5`, `F1`). Used as the primary diff key — a flow that
    /// disappears between baselines is a tooling regression, not a
    /// performance change.
    pub flow_id: String,

    /// Human-readable scenario name (e.g.
    /// `bytestream_write_fastslow_filesystem_1mib_seq`). MUST be stable
    /// across runs.
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
    /// `iters` ≤ 5 because at small N the p99 estimate is meaningless.
    pub iters: u32,

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
    #[serde(default)]
    pub extras: BTreeMap<String, serde_json::Value>,
}

/// Latency percentiles in milliseconds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

impl LatencyPercentiles {
    /// Derive percentiles from a vector of per-iteration durations.
    /// Sorts in-place. Empty input panics — at runtime callers ensure
    /// `iters >= 1` before invoking.
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
            p50: round3(pct(0.50)),
            p90: round3(pct(0.90)),
            p99: round3(pct(0.99)),
            max: round3(samples[n - 1].as_secs_f64() * 1_000.0),
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
pub struct RunMetadata {
    /// Always [`SCHEMA_VERSION`] at time of emit.
    pub schema_version: u32,
    /// Git commit SHA at run time (full 40-char). Diff tooling uses
    /// this to verify a baseline lines up with a known code state.
    pub git_commit_sha: String,
    /// Whether the working tree was dirty when the run was kicked off.
    pub git_dirty: bool,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineFile {
    pub metadata: RunMetadata,
    pub results: Vec<BenchmarkResult>,
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}
