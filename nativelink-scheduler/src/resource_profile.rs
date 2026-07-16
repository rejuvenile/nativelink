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

//! (#task-resource-profile Phase-2a) OBSERVE-ONLY per-action resource profile
//! aggregation.
//!
//! The scheduler folds each finished action's worker-reported
//! [`ActionResourceUsage`](nativelink_proto) into a bounded, instance-scoped
//! [`ProfileMap`]. Nothing in this module (or its caller in this phase) reads
//! the map to influence placement, matching, or reservation — it is pure data
//! collection so we can SEE profiles accumulate and whether the coarse key is
//! predictive (via the variance monitor). A later phase (under user
//! authorization) consumes the map for measured-override injection.
//!
//! # Design constraints (from the v3 design + cadre findings)
//!
//! * The per-dimension estimator MUST be a COMPACT FIXED-SIZE SKETCH, never a
//!   growing sample buffer (assumption-auditor N-3): a growing buffer breaks
//!   both the per-entry byte-cost bound and the LRU cap sizing. We use a
//!   log2-bucketed histogram ([`LogHistogram`]).
//! * The map MUST be bounded (CLAUDE.md unbounded-buffer rule): an
//!   [`LruCache`] with a documented cap; over-cap evicts the LRU key and the
//!   eviction is counted so working-set overflow is visible, not silent.
//! * The key MUST be instance-scoped (security S-3) so one instance's profile
//!   never leaks into another's.

use core::num::NonZeroUsize;

use lru::LruCache;

/// Number of log2 buckets per dimension in the compact histogram sketch.
///
/// Bucket `i` (for `1 <= i <= 62`) holds values in `[2^(i-1), 2^i)`; bucket `0`
/// holds the value `0`. The TOP bucket `63` is saturating: [`bucket_index`]
/// clamps any value `>= 2^62` into it, so bucket `63` covers `[2^62, 2^64)`
/// (a merged top octave — the only imprecision, at magnitudes far above any
/// real resource value). `64 * size_of::<u32>() = 256` bytes per dimension.
const HIST_BUCKETS: usize = 64;

/// (#task-resource-profile Phase-2a, S4 hardening) Maximum characters kept from
/// each [`ProfileKey`] string component. The LRU cap bounds the key COUNT, not
/// the per-key STRING bytes, so a client sending a megabyte `target_id` /
/// `action_mnemonic` (baggage is client-declared) would break the ~20 MiB
/// footprint bound. Real Bazel labels + mnemonics are far under 256 chars;
/// truncation only collides pathological/oversized keys (bounded, self-limiting).
const PROFILE_KEY_MAX_STR_LEN: usize = 256;

/// (#task-resource-profile Phase-2a) Maximum distinct keys in the bounded
/// [`ProfileMap`] LRU.
///
/// The coarse key is `(instance_name, target_id, action_mnemonic)`. A large
/// Bazel repo has tens of thousands of distinct targets, but the LRU bounds
/// the map to the recency window of ACTIVELY-completing targets rather than the
/// full target universe. `profile_map_evictions_total` surfaces when the
/// working set exceeds this cap (then percentiles are computed over a shorter
/// window for the churned keys).
///
/// Per-entry cost ≈ key (3 `String`s, ~72 B inline + ~100 B heap for a
/// ~100-char Bazel label) + [`Agg`] (4 × 256 B histograms + 8 B count = 1032 B)
/// + LRU node overhead (~48 B) ≈ **~1.25 KiB/entry**. `16384 × 1.25 KiB ≈
/// 20 MiB` — bounded and modest for the scheduler process.
pub const PROFILE_MAP_MAX_KEYS: usize = 16384;

/// (#task-resource-profile Phase-2a) Minimum samples before a key's variance is
/// trusted (and, in a later phase, before the key is eligible for measured
/// override). Below `K` the p95/p50 ratio is statistically unstable (red-team),
/// so a key with fewer than `K` samples is never counted as high-variance.
pub const PROFILE_MIN_SAMPLES: u64 = 20;

/// (#task-resource-profile Phase-2a) A key is "high variance" when its memory
/// p95/p50 ratio is `>= 2.0×` (stored ×100 to stay in integer arithmetic).
/// Above this, the coarse `(instance, target, mnemonic)` key is a poor
/// predictor of memory — which is exactly what the variance monitor exists to
/// surface (a finer per-output key would be needed there).
pub const PROFILE_HIGH_VARIANCE_RATIO_X100: u64 = 200;

/// Map a value to its log2 histogram bucket index (saturating at the top
/// bucket).
#[inline]
const fn bucket_index(v: u64) -> usize {
    if v == 0 {
        0
    } else {
        // bit-length of v == floor(log2(v)) + 1; clamp the top power-of-two
        // range into the last bucket so no index exceeds HIST_BUCKETS - 1.
        let idx = (u64::BITS - v.leading_zeros()) as usize;
        if idx >= HIST_BUCKETS {
            HIST_BUCKETS - 1
        } else {
            idx
        }
    }
}

/// The representative value reported for a bucket: the geometric-ish midpoint
/// `1.5 * 2^(i-1)` of the bucket's `[2^(i-1), 2^i)` range. This keeps a
/// percentile estimate within roughly a factor of √2 of the true value.
#[inline]
const fn bucket_representative(i: usize) -> u64 {
    if i == 0 {
        0
    } else {
        let lower = 1u64 << (i - 1);
        lower + (lower >> 1)
    }
}

/// A fixed-size, log2-bucketed histogram — a compact streaming percentile
/// sketch. `256` bytes (64 `u32` counts). It NEVER grows with the number of
/// samples folded into it (contrast a `Vec<u64>` sample buffer, which the
/// auditor flagged as breaking the per-entry cost + cap sizing).
#[derive(Clone, Debug)]
struct LogHistogram {
    buckets: [u32; HIST_BUCKETS],
}

impl Default for LogHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; HIST_BUCKETS],
        }
    }
}

impl LogHistogram {
    /// Fold one observed value into the sketch. Counts saturate (a single
    /// bucket overflowing `u32::MAX` would require >4e9 identical-magnitude
    /// samples for one key; saturation keeps the estimate monotone rather than
    /// wrapping).
    #[inline]
    const fn record(&mut self, value: u64) {
        let idx = bucket_index(value);
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
    }

    /// Estimate the value at quantile `q_num / q_den` (e.g. `19/20` = p95) given
    /// the total sample count (which equals the sum of the bucket counts — the
    /// caller tracks it once in [`Agg::sample_count`], so we do not re-sum here).
    ///
    /// Exact integer rank math (no float, no truncating cast): the 1-indexed
    /// rank is `ceil(total * q_num / q_den)`, clamped into `[1, total]`.
    fn quantile(&self, q_num: u64, q_den: u64, total: u64) -> u64 {
        if total == 0 {
            return 0;
        }
        // ceil(total * q_num / q_den) via integer arithmetic. `saturating_mul`
        // guards the (unreachable for realistic counts) overflow; the result is
        // clamped to `total` regardless.
        let rank = (total.saturating_mul(q_num).saturating_add(q_den - 1) / q_den).clamp(1, total);
        let mut cum: u64 = 0;
        for (i, &count) in self.buckets.iter().enumerate() {
            cum += u64::from(count);
            if cum >= rank {
                return bucket_representative(i);
            }
        }
        // Unreachable when `total` matches the folded counts, but stay total:
        // return the top populated bucket's representative.
        bucket_representative(HIST_BUCKETS - 1)
    }
}

/// One completed action's worker-reported resource usage, normalized for the
/// fold. `net_bytes` is the SUM of input-fetch and output-write bytes (the
/// design's single "net" dimension).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceSample {
    pub memory_kb: u64,
    pub cpu_ns: u64,
    pub disk_bytes: u64,
    pub net_bytes: u64,
}

/// Compact streaming aggregate for one [`ProfileKey`]: a fixed-size p50/p95
/// sketch per dimension plus the sample count. `~1032` bytes regardless of how
/// many samples are folded.
#[derive(Clone, Debug, Default)]
pub struct Agg {
    memory_kb: LogHistogram,
    cpu_ns: LogHistogram,
    disk_bytes: LogHistogram,
    net_bytes: LogHistogram,
    sample_count: u64,
}

impl Agg {
    /// Fold one sample into every dimension's sketch. O(1), fixed work.
    const fn fold(&mut self, sample: ResourceSample) {
        self.memory_kb.record(sample.memory_kb);
        self.cpu_ns.record(sample.cpu_ns);
        self.disk_bytes.record(sample.disk_bytes);
        self.net_bytes.record(sample.net_bytes);
        self.sample_count = self.sample_count.saturating_add(1);
    }

    #[inline]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    #[inline]
    pub fn memory_p50(&self) -> u64 {
        self.memory_kb.quantile(1, 2, self.sample_count)
    }

    #[inline]
    pub fn memory_p95(&self) -> u64 {
        self.memory_kb.quantile(19, 20, self.sample_count)
    }

    #[inline]
    pub fn cpu_ns_p95(&self) -> u64 {
        self.cpu_ns.quantile(19, 20, self.sample_count)
    }

    #[inline]
    pub fn disk_bytes_p95(&self) -> u64 {
        self.disk_bytes.quantile(19, 20, self.sample_count)
    }

    #[inline]
    pub fn net_bytes_p95(&self) -> u64 {
        self.net_bytes.quantile(19, 20, self.sample_count)
    }

    /// Variance measure = memory p95/p50 ratio, ×100 to stay in integer math.
    /// Memory is the load-bearing dimension for the eventual reservation, so
    /// variance is measured there. A ratio of exactly 1.0 reports `100`.
    /// A degenerate `p50 == 0` with `p95 > 0` reports `u64::MAX` (treat as
    /// maximally spread).
    pub fn memory_variance_ratio_x100(&self) -> u64 {
        let p50 = self.memory_p50();
        let p95 = self.memory_p95();
        if p50 == 0 {
            return if p95 == 0 { 100 } else { u64::MAX };
        }
        p95.saturating_mul(100) / p50
    }
}

/// Coarse per-action identity for the resource profile map.
///
/// Instance-scoped (`instance_name` is part of the key) so one instance's
/// profile never leaks into another's (security S-3).
///
/// FUTURE: a finer per-output key (the Bazel `nl_output_key`, per-output rather
/// than per-target) will supersede or extend these fields once it lands. This
/// is kept a plain struct so a field can be ADDED (or the whole key swapped)
/// without changing the [`ProfileMap`] API — construct via [`ProfileKey::from_parts`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProfileKey {
    pub instance_name: String,
    pub target_id: String,
    pub action_mnemonic: String,
}

impl ProfileKey {
    /// Derive the coarse key from the joined origin metadata, or `None` (SKIP —
    /// do not record) when the BAGGAGE-derived parts are absent.
    ///
    /// `target_id` and `action_mnemonic` come from the Bazel `RequestMetadata`
    /// baggage; when baggage is absent they arrive empty, and keying on empties
    /// would collapse unrelated actions into one bogus profile — so an empty
    /// `target_id` OR an empty `action_mnemonic` returns `None`.
    ///
    /// `instance_name`, by contrast, is the routing instance (NOT baggage): the
    /// empty string is the legitimate DEFAULT instance. Skipping on an empty
    /// `instance_name` would dark the feature for the entire default instance
    /// (the CLAUDE.md anti-dark rule), so an empty `instance_name` is allowed
    /// and still isolates correctly (the default instance is one logical
    /// instance — there is no cross-instance collision).
    pub fn from_parts(
        instance_name: &str,
        target_id: &str,
        action_mnemonic: &str,
    ) -> Option<Self> {
        if target_id.is_empty() || action_mnemonic.is_empty() {
            return None;
        }
        Some(Self {
            // S4: clamp each component so a client-declared megabyte string can
            // never break the map's byte-footprint bound (see PROFILE_KEY_MAX_STR_LEN).
            instance_name: clamp_key_str(instance_name),
            target_id: clamp_key_str(target_id),
            action_mnemonic: clamp_key_str(action_mnemonic),
        })
    }
}

/// Truncate a key component to at most `PROFILE_KEY_MAX_STR_LEN` CHARS (not
/// bytes — respects UTF-8 boundaries). Cheap: the common case (short label) is a
/// single `to_string`; only an oversized string pays the char-count walk.
fn clamp_key_str(s: &str) -> String {
    if s.len() <= PROFILE_KEY_MAX_STR_LEN {
        // `len()` (bytes) <= max chars implies char-count <= max, so no truncation.
        s.to_string()
    } else {
        s.chars().take(PROFILE_KEY_MAX_STR_LEN).collect()
    }
}

/// The observable effect of one [`ProfileMap::record`] call, so the caller can
/// update its `#[metric]` counters/gauges without re-locking the map.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordOutcome {
    /// An LRU key was evicted to make room for a new key.
    pub evicted: bool,
    /// Distinct keys resident after this record (for the gauge).
    pub keys_tracked: u64,
    /// Keys currently classified high-variance after this record (for the
    /// gauge). Maintained incrementally — no full-map scan.
    pub high_variance_keys: u64,
}

/// Bounded, instance-scoped in-memory map of per-action resource profiles.
///
/// # Bounded
///
// CAPPED AT PROFILE_MAP_MAX_KEYS (16384): bounded LRU; over-cap evicts the LRU
// key (= the recency window of active targets) and the eviction is counted via
// the caller's `profile_map_evictions_total`. Each entry holds a fixed-size
// `Agg` sketch (~1.25 KiB/entry incl. key + node) — no owned network bytes and
// no per-sample growth, so total footprint is bounded at ~20 MiB.
#[derive(Debug)]
pub struct ProfileMap {
    cache: LruCache<ProfileKey, Agg>,
    /// Running count of keys classified high-variance, maintained by transition
    /// deltas on record + eviction (never a full scan on the completion path).
    high_variance_keys: usize,
    /// `K`: minimum samples before variance is trusted.
    min_samples: u64,
    /// High-variance threshold, memory p95/p50 ×100.
    high_variance_ratio_x100: u64,
}

impl ProfileMap {
    /// Construct a map with an explicit cap and variance tuning.
    pub fn new(max_keys: NonZeroUsize, min_samples: u64, high_variance_ratio_x100: u64) -> Self {
        Self {
            cache: LruCache::new(max_keys),
            high_variance_keys: 0,
            min_samples,
            high_variance_ratio_x100,
        }
    }

    /// Construct with the production default cap + variance tuning.
    pub fn with_default_tuning() -> Self {
        Self::new(
            NonZeroUsize::new(PROFILE_MAP_MAX_KEYS).expect("PROFILE_MAP_MAX_KEYS is nonzero"),
            PROFILE_MIN_SAMPLES,
            PROFILE_HIGH_VARIANCE_RATIO_X100,
        )
    }

    /// Whether `agg` is currently classified high-variance (enough samples AND
    /// spread above the threshold). A pure function of the agg + fixed tuning,
    /// so the incremental `high_variance_keys` counter stays consistent.
    fn is_high_variance(&self, agg: &Agg) -> bool {
        agg.sample_count() >= self.min_samples
            && agg.memory_variance_ratio_x100() >= self.high_variance_ratio_x100
    }

    /// Fold one sample under `key`, creating the key if absent. O(1) amortized.
    /// Updates the high-variance count by transition deltas so no full-map scan
    /// is ever needed on the (hot-ish) completion path.
    pub fn record(&mut self, key: ProfileKey, sample: ResourceSample) -> RecordOutcome {
        let mut evicted = false;

        if let Some(agg) = self.cache.get_mut(&key) {
            // Existing key: `get_mut` bumps LRU recency (desired on the write
            // path). Reclassify across the fold and adjust the running count.
            let was_high = agg.sample_count() >= self.min_samples
                && agg.memory_variance_ratio_x100() >= self.high_variance_ratio_x100;
            agg.fold(sample);
            let now_high = agg.sample_count() >= self.min_samples
                && agg.memory_variance_ratio_x100() >= self.high_variance_ratio_x100;
            // saturating_sub is DEFENSIVE ONLY: the accounting is balanced by
            // construction (every decrement pairs a prior increment for that
            // key's state), and the tests assert exact counts — so a real bug
            // is caught in test, never masked. This just guarantees the
            // observe-only completion path can never panic on an underflow.
            match (was_high, now_high) {
                (false, true) => self.high_variance_keys += 1,
                (true, false) => {
                    self.high_variance_keys = self.high_variance_keys.saturating_sub(1);
                }
                _ => {}
            }
        } else {
            // New key: fold the first sample, then push. `push` on a key known
            // to be absent returns `Some((evicted_key, evicted_agg))` iff the
            // cache was at capacity (an LRU eviction), which we count and use to
            // decrement the high-variance gauge if the victim was high-variance.
            let mut agg = Agg::default();
            agg.fold(sample);
            // NOTE: a fresh 1-sample agg can NEVER be high-variance under the
            // production tuning: `sample_count == 1 < min_samples` (20), and even
            // with `min_samples == 1` a single sample lands in ONE bucket so
            // `p50 == p95` → ratio 100 < threshold (200). So this branch is a
            // no-op in prod; it is kept only to stay correct under a degenerate
            // `min_samples == 1 && threshold <= 100` config (where a 1-sample key
            // would classify high) so the incremental gauge cannot drift.
            if self.is_high_variance(&agg) {
                self.high_variance_keys += 1;
            }
            if let Some((_evicted_key, evicted_agg)) = self.cache.push(key, agg) {
                evicted = true;
                if self.is_high_variance(&evicted_agg) {
                    // saturating_sub: defensive only (see the existing-key arm).
                    self.high_variance_keys = self.high_variance_keys.saturating_sub(1);
                }
            }
        }

        RecordOutcome {
            evicted,
            keys_tracked: self.cache.len() as u64,
            high_variance_keys: self.high_variance_keys as u64,
        }
    }

    /// Read an aggregate WITHOUT bumping LRU recency (`peek`, not `get`). The
    /// eventual match-path reader must use this so an observe-only read never
    /// perturbs eviction order (design §6 / N-3). Unused in Phase-2a beyond
    /// tests, kept for the read-path chunk.
    #[cfg(test)]
    pub fn peek(&self, key: &ProfileKey) -> Option<&Agg> {
        self.cache.peek(key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    #[cfg(test)]
    #[inline]
    pub const fn high_variance_key_count(&self) -> usize {
        self.high_variance_keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(target: &str, mnemonic: &str) -> ProfileKey {
        ProfileKey::from_parts("main", target, mnemonic).expect("non-empty parts")
    }

    fn mem_sample(memory_kb: u64) -> ResourceSample {
        ResourceSample {
            memory_kb,
            cpu_ns: 0,
            disk_bytes: 0,
            net_bytes: 0,
        }
    }

    // ── (a) compact estimator: fold a known distribution, assert p50/p95 +
    //         variance land in the expected buckets ──

    #[test]
    fn estimator_p50_p95_land_in_expected_buckets() {
        // 90 samples at ~1000 KiB, 10 samples at ~1_000_000 KiB.
        let mut agg = Agg::default();
        for _ in 0..90 {
            agg.fold(mem_sample(1000));
        }
        for _ in 0..10 {
            agg.fold(mem_sample(1_000_000));
        }
        assert_eq!(agg.sample_count(), 100);

        // 1000 falls in bucket 10 = [512, 1024); representative = 768.
        assert_eq!(
            agg.memory_p50(),
            768,
            "estimator p50 must fall in the 1000-KiB bucket [512,1024) → rep 768, \
             got {}",
            agg.memory_p50()
        );
        // rank(0.95)=95 skips past the 90 low samples into the 1_000_000 bucket
        // 20 = [524288, 1048576); representative = 786432.
        assert_eq!(
            agg.memory_p95(),
            786_432,
            "estimator p95 must fall in the 1_000_000-KiB bucket [2^19,2^20) → rep \
             786432, got {}",
            agg.memory_p95()
        );
        // variance ratio ×100 = 786432*100/768 = 102400 (1024×) → high spread.
        assert_eq!(agg.memory_variance_ratio_x100(), 102_400);
    }

    #[test]
    fn estimator_tight_distribution_is_low_variance() {
        let mut agg = Agg::default();
        for _ in 0..100 {
            agg.fold(mem_sample(1000));
        }
        assert_eq!(agg.memory_p50(), agg.memory_p95());
        assert_eq!(
            agg.memory_variance_ratio_x100(),
            100,
            "a single-bucket (tight) distribution must report a 1.0× ratio (100), \
             got {}",
            agg.memory_variance_ratio_x100()
        );
    }

    #[test]
    fn estimator_dimensions_are_independent() {
        // Each dimension folds its own value; a per-dim p95 must reflect only
        // that dimension (no cross-contamination in the sketch layout).
        let mut agg = Agg::default();
        for _ in 0..10 {
            agg.fold(ResourceSample {
                memory_kb: 2048,
                cpu_ns: 1 << 30,
                disk_bytes: 4096,
                net_bytes: 8192,
            });
        }
        // 2048 → bucket 12 [2048,4096) rep 3072.
        assert_eq!(agg.memory_p95(), 3072);
        // 1<<30 → bucket 31 [2^30,2^31) rep 1610612736.
        assert_eq!(agg.cpu_ns_p95(), (1 << 30) + (1 << 29));
        // 4096 → bucket 13 [4096,8192) rep 6144.
        assert_eq!(agg.disk_bytes_p95(), 6144);
        // 8192 → bucket 14 [8192,16384) rep 12288.
        assert_eq!(agg.net_bytes_p95(), 12288);
    }

    #[test]
    fn estimator_is_fixed_size_regardless_of_sample_count() {
        // A compact sketch's size does not grow with samples. Fold many; the
        // struct is Copy-of-arrays-sized (no heap sample buffer).
        assert_eq!(
            size_of::<Agg>(),
            4 * (HIST_BUCKETS * size_of::<u32>()) + size_of::<u64>()
        );
    }

    // ── (b) bounding / LRU eviction + eviction counter ──

    #[test]
    fn map_bounds_and_counts_evictions() {
        let mut map = ProfileMap::new(NonZeroUsize::new(2).unwrap(), 1, 200);
        let (k1, k2, k3) = (key("//a", "M"), key("//b", "M"), key("//c", "M"));

        let o1 = map.record(k1.clone(), mem_sample(100));
        assert!(!o1.evicted);
        assert_eq!(o1.keys_tracked, 1);

        let o2 = map.record(k2.clone(), mem_sample(100));
        assert!(!o2.evicted);
        assert_eq!(o2.keys_tracked, 2);

        // Third distinct key over cap 2 → evicts the LRU key (k1).
        let o3 = map.record(k3.clone(), mem_sample(100));
        assert!(
            o3.evicted,
            "third distinct key over cap 2 must evict the LRU key (evicted must be \
             true so profile_map_evictions_total advances), got evicted=false"
        );
        assert_eq!(o3.keys_tracked, 2, "map must stay bounded at cap 2");
        assert!(map.peek(&k1).is_none(), "k1 was LRU → evicted");
        assert!(map.peek(&k2).is_some());
        assert!(map.peek(&k3).is_some());
    }

    #[test]
    fn map_existing_key_folds_without_eviction() {
        let mut map = ProfileMap::new(NonZeroUsize::new(2).unwrap(), 1, 200);
        let k1 = key("//a", "M");
        map.record(k1.clone(), mem_sample(100));
        let o = map.record(k1.clone(), mem_sample(200));
        assert!(!o.evicted, "re-recording an existing key must not evict");
        assert_eq!(o.keys_tracked, 1);
        assert_eq!(
            map.peek(&k1).unwrap().sample_count(),
            2,
            "second fold under the same key must increment sample_count to 2"
        );
    }

    #[test]
    fn map_get_mut_bumps_recency_so_touched_key_survives() {
        // Prove the existing-key fold path uses recency-bumping access: after
        // touching k1, the LRU victim on overflow must be k2, not k1.
        let mut map = ProfileMap::new(NonZeroUsize::new(2).unwrap(), 1, 200);
        let (k1, k2, k3) = (key("//a", "M"), key("//b", "M"), key("//c", "M"));
        map.record(k1.clone(), mem_sample(100));
        map.record(k2.clone(), mem_sample(100));
        // Touch k1 → k1 becomes most-recent, k2 becomes LRU.
        map.record(k1.clone(), mem_sample(100));
        let o = map.record(k3.clone(), mem_sample(100));
        assert!(o.evicted);
        assert!(
            map.peek(&k1).is_some(),
            "k1 was touched most-recently → must survive"
        );
        assert!(
            map.peek(&k2).is_none(),
            "k2 was LRU → must be the eviction victim"
        );
    }

    // ── high-variance gauge maintenance (incremental, no scan) ──

    #[test]
    fn high_variance_count_increments_and_decrements_on_eviction() {
        // cap 1, K=2, threshold 2.0×. Fold a spread distribution into A across
        // >=K samples → A is high-variance. Then insert B (new key) → A is
        // evicted → the high-variance count must drop back to 0.
        let mut map = ProfileMap::new(NonZeroUsize::new(1).unwrap(), 2, 200);
        let a = key("//a", "M");
        map.record(a.clone(), mem_sample(1000));
        let o = map.record(a.clone(), mem_sample(1_000_000));
        assert_eq!(
            o.high_variance_keys, 1,
            "key A has 2 samples spanning 1000→1_000_000 (ratio ≫2×, ≥K) → must be \
             counted high-variance, got {}",
            o.high_variance_keys
        );
        assert_eq!(map.high_variance_key_count(), 1);

        let b = key("//b", "M");
        let o2 = map.record(b.clone(), mem_sample(500));
        assert!(o2.evicted, "cap 1 → inserting B evicts A");
        assert_eq!(
            o2.high_variance_keys, 0,
            "evicting the high-variance key A must decrement the gauge to 0, got {}",
            o2.high_variance_keys
        );
        assert_eq!(map.high_variance_key_count(), 0);
    }

    #[test]
    fn low_sample_count_is_not_high_variance() {
        // Same spread as above but K raised above the sample count → NOT
        // high-variance (variance is untrustworthy at low sample count).
        let mut map = ProfileMap::new(NonZeroUsize::new(4).unwrap(), 20, 200);
        let a = key("//a", "M");
        map.record(a.clone(), mem_sample(1000));
        let o = map.record(a.clone(), mem_sample(1_000_000));
        assert_eq!(
            o.high_variance_keys, 0,
            "2 samples < K=20 → variance not trusted → not high-variance, got {}",
            o.high_variance_keys
        );
    }

    // ── (c) ProfileKey derivation incl. skip-on-empty-baggage ──

    #[test]
    fn key_derivation_populates_all_parts() {
        let k = ProfileKey::from_parts("main", "//foo:bar", "CppCompile")
            .expect("all parts present → Some");
        assert_eq!(k.instance_name, "main");
        assert_eq!(k.target_id, "//foo:bar");
        assert_eq!(k.action_mnemonic, "CppCompile");
    }

    #[test]
    fn key_derivation_skips_empty_baggage() {
        assert!(
            ProfileKey::from_parts("main", "", "CppCompile").is_none(),
            "empty target_id (baggage absent) must skip (None) so unrelated \
             actions are not collapsed into one bogus profile"
        );
        assert!(
            ProfileKey::from_parts("main", "//foo:bar", "").is_none(),
            "empty action_mnemonic (baggage absent) must skip (None)"
        );
    }

    #[test]
    fn key_derivation_allows_empty_instance_name() {
        // The empty instance is the DEFAULT instance (not baggage); skipping it
        // would dark the feature for the whole default instance.
        let k = ProfileKey::from_parts("", "//foo:bar", "CppCompile")
            .expect("empty instance_name is the default instance → still Some");
        assert_eq!(k.instance_name, "");
    }

    #[test]
    fn key_derivation_clamps_oversized_strings() {
        // S4: a client-declared megabyte string must be truncated so the map's
        // byte-footprint bound holds.
        let huge_target = "x".repeat(1_000_000);
        let k = ProfileKey::from_parts("main", &huge_target, "CppCompile")
            .expect("non-empty parts → Some");
        assert_eq!(
            k.target_id.chars().count(),
            PROFILE_KEY_MAX_STR_LEN,
            "an oversized target_id must be truncated to PROFILE_KEY_MAX_STR_LEN \
             chars so a megabyte key cannot break the ~20 MiB footprint bound"
        );
        // A short label is untouched (no accidental truncation of real labels).
        let short = ProfileKey::from_parts("main", "//foo:bar", "CppCompile").unwrap();
        assert_eq!(short.target_id, "//foo:bar");
    }
}
