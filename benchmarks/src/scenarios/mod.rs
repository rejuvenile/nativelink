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

//! One module per design-doc flow id. Each module exposes
//! `pub async fn run(opts: &RunOpts) -> Vec<BenchmarkResult>` so the
//! CLI runner can dispatch uniformly.

pub mod ac_micro;
pub mod chunked_v2;
pub mod existence_cache_micro;
pub mod find_missing;
pub mod legacy_read;
pub mod legacy_write;
pub mod prodlike;

use core::future::Future;
use std::time::Instant;

use bytes::Bytes;
use sha2::{Digest as _, Sha256};

use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher as _, default_digest_hasher_func};

use crate::output::{BenchmarkResult, CacheState, LatencyPercentiles, Throughput};

/// Compute a `DigestInfo` for `data` using the process-global default
/// hasher (BLAKE3 in this bench, matching prod via `default_digest_hash_function`).
///
/// Wrappers (VerifyStore, ChunkedDriver commit) recompute and compare
/// against the global hasher; pre-computing the declared digest with a
/// different function (e.g. hardcoded SHA-256) produces
/// `InvalidArgument: Hashes do not match` at the FastSlowStore::update
/// seam. This helper is the single point that pins "bench-declared
/// digest hash function == prod-active hash function".
pub fn digest_via_default_hasher(data: &[u8]) -> DigestInfo {
    let mut h = default_digest_hasher_func().hasher();
    h.update(data);
    h.finalize_digest()
}

/// Floor for `--fast` iterations. Below this p50 is noise.
pub const FAST_MODE_ITERS: u32 = 3;

/// Minimum permitted CLI `--iters` value. The CLI parser rejects values
/// below this (`--iters 0 --fast` would otherwise panic in
/// `LatencyPercentiles::from_samples`).
pub const MIN_ITERS: u32 = 1;

/// CLI-supplied options shared across scenarios.
#[derive(Debug, Clone)]
pub struct RunOpts {
    /// Iterations per cell. `None` means "use the per-cell default"
    /// (which may itself be a per-cell `iters_override`). `Some(N)`
    /// means the operator explicitly passed `--iters N` and that value
    /// wins over any per-cell default. Diff tooling rejects baselines
    /// with `iters < 20`; CLI parse rejects `iters < MIN_ITERS`.
    ///
    /// #536: distinguishing "operator passed --iters" from "operator
    /// accepted the bench-wide default of 20" is what lets per-cell
    /// `iters_override` (e.g. the 16 MiB c=1 cell's `Some(50)`) take
    /// effect; the previous `u32` shape with clap's `default_value_t = 20`
    /// collapsed the two states so the override was silently ignored
    /// (every `effective_iters(_)` call returned `self.iters = 20`).
    pub iters: Option<u32>,
    /// If set, only run scenarios whose name matches this substring.
    pub filter: Option<String>,
    /// Reduce iters to `FAST_MODE_ITERS` so the full smoke suite finishes
    /// in seconds rather than minutes — used for self-check runs
    /// (CI-disjoint dev iteration).
    pub fast: bool,
}

impl RunOpts {
    /// Resolve the effective iter count for a cell.
    ///
    /// - **Operator-supplied `--iters N` wins.** When `self.iters` is
    ///   `Some(N)` (operator explicitly passed `--iters N`), `N` is
    ///   honored (still subject to `--fast` clamp + `MIN_ITERS` floor).
    /// - **Otherwise the per-cell default fires.** When `self.iters` is
    ///   `None`, the `default` argument is used — this is where a cell's
    ///   `iters_override` (e.g. W3 16 MiB c=1 = `Some(50)`) flows in via
    ///   `opts.effective_iters(cell.iters_override.unwrap_or(20))`.
    /// - **`--fast` collapses to `FAST_MODE_ITERS`** regardless of the
    ///   above; intentional, the run is self-check, NOT a diff anchor.
    /// - **Defensive floor:** `default == 0` or any other slip-through
    ///   is clamped to `MIN_ITERS` to prevent the downstream
    ///   `LatencyPercentiles::from_samples` empty-vec panic.
    pub fn effective_iters(&self, default: u32) -> u32 {
        let base = match self.iters {
            Some(n) if n >= MIN_ITERS => n,
            // `iters == Some(0)` is rejected by `parse_iters`; the
            // unwrap_or() handles a hypothetical bypass safely.
            Some(_) | None => default.max(MIN_ITERS),
        };
        if self.fast {
            base.min(FAST_MODE_ITERS).max(MIN_ITERS)
        } else {
            base.max(MIN_ITERS)
        }
    }

    pub fn matches(&self, name: &str) -> bool {
        match &self.filter {
            None => true,
            Some(f) => name.contains(f),
        }
    }
}

/// Deterministic blob factory. Hash + content are pure functions of
/// `seed + size`, so the same call across runs yields the same digest.
/// Critical for "warm" scenarios where we prepopulate once and read N
/// times — and for diff stability of `extras.digest_hex`.
///
/// Digest is computed via the process-global default hasher (BLAKE3
/// per `data_plane_bench::main`, matching prod). A previous version of
/// this helper hardcoded SHA-256 with a "digests-as-labels" rationale;
/// that was incorrect — VerifyStore and the chunked-driver commit
/// barrier both recompute the digest using the global hasher, so a
/// hardcoded mismatch surfaces as `InvalidArgument: Hashes do not
/// match` at the FastSlowStore::update seam (#524).
pub fn make_blob(seed: u64, size: usize) -> (DigestInfo, Bytes) {
    let mut data = Vec::with_capacity(size);
    let mut state: u64 = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for _ in 0..size {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        data.push((state >> 33) as u8);
    }
    let digest = digest_via_default_hasher(&data);
    (digest, Bytes::from(data))
}

/// Variant of [`make_blob`] whose seed is derived from
/// `(scenario_name_hash, iter, slot)` to guarantee per-tuple uniqueness
/// without the XOR-collision hazard of `seed ^ n ^ j`.
///
/// Specifically, two distinct `(n, j)` pairs always produce distinct
/// seeds; the mixer `seed.wrapping_mul(K).wrapping_add(n*17 + j)` is
/// monotonic in both `n` and `j` for any fixed seed.
pub fn make_blob_with_indices(
    scenario_name: &str,
    n: u64,
    j: u32,
    size: usize,
) -> (DigestInfo, Bytes) {
    let mut h = Sha256::new();
    h.update(scenario_name.as_bytes());
    let seed_digest = h.finalize();
    let seed = u64::from_le_bytes(seed_digest[..8].try_into().unwrap());
    let mixed = seed
        .wrapping_mul(1_000_003)
        .wrapping_add((n.wrapping_mul(17)).wrapping_add(j as u64));
    make_blob(mixed, size)
}

/// Repeatedly run `body` and capture per-iteration durations. Returns
/// the assembled `BenchmarkResult`. Generic over the future shape so it
/// works for "one op" and "N concurrent ops" alike.
///
/// `body` is awaited in a tight loop with NO sleep between iterations;
/// the bench measures back-to-back operation latency.
#[allow(clippy::too_many_arguments)]
pub async fn measure<F, Fut>(
    flow_id: &str,
    scenario_name: &str,
    blob_size_bytes: Option<u64>,
    concurrency: u32,
    cache_state: CacheState,
    iters: u32,
    throughput_bytes_per_iter: Option<u64>,
    elements_per_iter: Option<u64>,
    extras: std::collections::BTreeMap<String, serde_json::Value>,
    mut body: F,
) -> BenchmarkResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    // Floor defensively — `RunOpts::effective_iters` already enforces
    // this, but `measure` is also a public helper.
    let iters = iters.max(MIN_ITERS);
    let mut samples = Vec::with_capacity(iters as usize);
    let total_start = Instant::now();
    for _ in 0..iters {
        let t = Instant::now();
        body().await;
        samples.push(t.elapsed());
    }
    let total_duration = total_start.elapsed();
    let total_duration_ms = round_emit(total_duration.as_secs_f64() * 1_000.0);
    let latency_ms = LatencyPercentiles::from_samples(&mut samples);
    let throughput = match (throughput_bytes_per_iter, elements_per_iter) {
        (Some(bytes), _) => {
            let total_bytes = bytes.saturating_mul(iters as u64) as f64;
            Throughput::BytesPerSec(round_emit(total_bytes / total_duration.as_secs_f64()))
        }
        (None, Some(elems)) => {
            let total_elems = elems.saturating_mul(iters as u64) as f64;
            Throughput::ElementsPerSec(round_emit(total_elems / total_duration.as_secs_f64()))
        }
        (None, None) => Throughput::None,
    };
    BenchmarkResult {
        flow_id: flow_id.to_string(),
        scenario_name: scenario_name.to_string(),
        blob_size_bytes,
        concurrency,
        cache_state,
        iters,
        confidence: crate::output::confidence_for_iters(iters),
        total_duration_ms,
        latency_ms,
        throughput,
        extras,
    }
}

fn round_emit(x: f64) -> f64 {
    crate::output::round_emit(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_opts_filter_matches_substring() {
        let opts = RunOpts {
            iters: None,
            filter: Some("w1".to_string()),
            fast: false,
        };
        assert!(opts.matches("w1_store_update_oneshot_1MiB_c1"));
        assert!(!opts.matches("r1_store_get_part_unchunked_1MiB_c1"));
    }

    #[test]
    fn run_opts_no_filter_matches_all() {
        let opts = RunOpts { iters: Some(20), filter: None, fast: false };
        assert!(opts.matches("anything"));
    }

    #[test]
    fn effective_iters_fast_caps_at_three() {
        let opts = RunOpts { iters: Some(100), filter: None, fast: true };
        assert_eq!(opts.effective_iters(20), FAST_MODE_ITERS);
    }

    #[test]
    fn effective_iters_none_uses_default() {
        let opts = RunOpts { iters: None, filter: None, fast: false };
        assert_eq!(opts.effective_iters(42), 42);
    }

    /// The CLI parser SHOULD reject `--iters 0`, but defensively
    /// `effective_iters` clamps to `MIN_ITERS` so the downstream
    /// `LatencyPercentiles::from_samples` empty-vec panic cannot fire.
    /// Mutation: remove the `.max(MIN_ITERS)` in `effective_iters` —
    /// this test must red-fail with the floor-violation message.
    #[test]
    fn effective_iters_zero_default_with_fast_floors_to_min_iters() {
        let opts = RunOpts { iters: None, filter: None, fast: true };
        assert!(
            opts.effective_iters(0) >= MIN_ITERS,
            "effective_iters MUST floor at MIN_ITERS; if it returns 0, the \
             downstream LatencyPercentiles::from_samples panics on empty input"
        );
    }

    /// #536 regression: a per-cell `iters_override` (passed in as the
    /// `default` arg) must take effect when the operator did NOT pass
    /// `--iters` on the CLI. The prior shape (`iters: u32` with clap's
    /// `default_value_t = 20`) made `self.iters` indistinguishable from
    /// "operator-supplied 20"; `effective_iters(50)` returned 20 because
    /// the `self.iters >= MIN_ITERS` branch always won.
    ///
    /// Mutation: rewrite the `Some(n) if n >= MIN_ITERS => n` arm to
    /// `Some(n) => n` (drop the `MIN_ITERS` floor guard) — this test
    /// must still pass; the discriminating test is
    /// `iters_explicit_some_overrides_per_cell_default` below.
    #[test]
    fn effective_iters_iters_none_lets_per_cell_default_win() {
        let opts = RunOpts { iters: None, filter: None, fast: false };
        assert_eq!(
            opts.effective_iters(50),
            50,
            "when --iters absent, per-cell default (e.g. iters_override) MUST fire"
        );
    }

    /// #536 regression (the discriminating case): `Some(N)` on the CLI
    /// overrides per-cell `iters_override`. This is the half of the
    /// contract that lets operators force a single iter count across
    /// the whole matrix for ad-hoc debugging.
    ///
    /// Mutation: rewrite `Some(n) if n >= MIN_ITERS => n` to
    /// `Some(_) | None => default.max(MIN_ITERS)` (always take the
    /// default) — this test must red-fail with the explicit-precedence
    /// violation.
    #[test]
    fn effective_iters_iters_some_overrides_per_cell_default() {
        let opts = RunOpts { iters: Some(7), filter: None, fast: false };
        assert_eq!(
            opts.effective_iters(50),
            7,
            "explicit --iters N MUST override per-cell iters_override (#536)"
        );
    }

    #[test]
    fn make_blob_is_deterministic() {
        let (d1, b1) = make_blob(0xDEAD_BEEF, 64);
        let (d2, b2) = make_blob(0xDEAD_BEEF, 64);
        assert_eq!(d1, d2);
        assert_eq!(b1, b2);
    }

    #[test]
    fn make_blob_size_propagates_to_digest() {
        let (d, b) = make_blob(1, 1024);
        assert_eq!(d.size_bytes(), 1024);
        assert_eq!(b.len(), 1024);
    }

    /// Distinct seeds must produce distinct digests. The W1/R1 cells
    /// rely on `make_blob_with_indices` returning unique digests for
    /// distinct `(iter, slot)` pairs; a collision silently makes the
    /// bench measure cache-hit instead of cold-write paths.
    /// Mutation: replace `wrapping_add((n*17)+j)` with `wrapping_add(0)`
    /// in `make_blob_with_indices` — this test must red-fail.
    #[test]
    fn make_blob_with_indices_distinct_for_distinct_tuples() {
        let (d00, _) = make_blob_with_indices("w1_scenario", 0, 0, 64);
        let (d01, _) = make_blob_with_indices("w1_scenario", 0, 1, 64);
        let (d10, _) = make_blob_with_indices("w1_scenario", 1, 0, 64);
        assert_ne!(d00, d01, "(n=0,j=0) MUST differ from (n=0,j=1)");
        assert_ne!(d00, d10, "(n=0,j=0) MUST differ from (n=1,j=0)");
        assert_ne!(d01, d10, "(n=0,j=1) MUST differ from (n=1,j=0)");
    }

    /// Distinct scenario names must produce distinct seeds, so two
    /// cells with the same (n, j) tuple don't collide on a digest.
    #[test]
    fn make_blob_with_indices_distinct_per_scenario() {
        let (d_w1, _) = make_blob_with_indices("w1_scenario", 0, 0, 64);
        let (d_r1, _) = make_blob_with_indices("r1_scenario", 0, 0, 64);
        assert_ne!(
            d_w1, d_r1,
            "distinct scenario names MUST yield distinct seeds — otherwise W1's \
             prepopulate would collide with R1's cold path"
        );
    }
}
