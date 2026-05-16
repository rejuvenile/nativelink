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

pub mod chunked_v2;
pub mod find_missing;
pub mod legacy_read;
pub mod legacy_write;

use core::future::Future;
use std::time::Instant;

use bytes::Bytes;
use nativelink_util::common::DigestInfo;
use sha2::{Digest as _, Sha256};

use crate::output::{BenchmarkResult, CacheState, LatencyPercentiles, Throughput};

/// CLI-supplied options shared across scenarios.
#[derive(Debug, Clone)]
pub struct RunOpts {
    /// Iterations per cell. Diff tooling rejects baselines with
    /// `iters <= 5`. Default 20 keeps cells ~30s wall-clock.
    pub iters: u32,
    /// If set, only run scenarios whose name matches this substring.
    pub filter: Option<String>,
    /// Reduce iters to a tiny number so the full smoke suite finishes
    /// in seconds rather than minutes — used for self-check runs
    /// (CI-disjoint dev iteration).
    pub fast: bool,
}

impl RunOpts {
    pub fn effective_iters(&self, default: u32) -> u32 {
        if self.fast {
            // Capped at 3 — enough to produce a sortable p50/p99 but not
            // enough to claim a clean baseline. The CLI flags the run
            // as `forced: false, fast: true` in metadata so diff tools
            // can refuse to compare against a fast-mode baseline.
            3.min(self.iters)
        } else if self.iters > 0 {
            self.iters
        } else {
            default
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
pub fn make_blob(seed: u64, size: usize) -> (DigestInfo, Bytes) {
    let mut data = Vec::with_capacity(size);
    let mut state: u64 = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for _ in 0..size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        data.push((state >> 33) as u8);
    }
    let hash = Sha256::digest(&data);
    let mut packed = [0u8; 32];
    packed.copy_from_slice(&hash);
    let digest = DigestInfo::new(packed, size as u64);
    (digest, Bytes::from(data))
}

/// Repeatedly run `body` and capture per-iteration durations. Returns
/// the assembled `BenchmarkResult`. Generic over the future shape so it
/// works for "one op" and "N concurrent ops" alike.
///
/// `body` is awaited in a tight loop with NO sleep between iterations;
/// the bench measures back-to-back operation latency.
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
    let mut samples = Vec::with_capacity(iters as usize);
    let total_start = Instant::now();
    for _ in 0..iters {
        let t = Instant::now();
        body().await;
        samples.push(t.elapsed());
    }
    let total_duration = total_start.elapsed();
    let total_duration_ms = round3(total_duration.as_secs_f64() * 1_000.0);
    let latency_ms = LatencyPercentiles::from_samples(&mut samples);
    let throughput = match (throughput_bytes_per_iter, elements_per_iter) {
        (Some(bytes), _) => {
            let total_bytes = bytes.saturating_mul(iters as u64) as f64;
            Throughput::BytesPerSec(round3(total_bytes / total_duration.as_secs_f64()))
        }
        (None, Some(elems)) => {
            let total_elems = elems.saturating_mul(iters as u64) as f64;
            Throughput::ElementsPerSec(round3(total_elems / total_duration.as_secs_f64()))
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
        total_duration_ms,
        latency_ms,
        throughput,
        extras,
    }
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_opts_filter_matches_substring() {
        let opts = RunOpts {
            iters: 0,
            filter: Some("w1".to_string()),
            fast: false,
        };
        assert!(opts.matches("w1_bytestream_write_1MiB_c1"));
        assert!(!opts.matches("r1_bytestream_read_1MiB_c1"));
    }

    #[test]
    fn run_opts_no_filter_matches_all() {
        let opts = RunOpts { iters: 20, filter: None, fast: false };
        assert!(opts.matches("anything"));
    }

    #[test]
    fn effective_iters_fast_caps_at_three() {
        let opts = RunOpts { iters: 100, filter: None, fast: true };
        assert_eq!(opts.effective_iters(20), 3);
    }

    #[test]
    fn effective_iters_zero_uses_default() {
        let opts = RunOpts { iters: 0, filter: None, fast: false };
        assert_eq!(opts.effective_iters(42), 42);
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
}
