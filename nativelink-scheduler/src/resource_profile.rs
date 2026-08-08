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
//! * (#2497 sched Phase-3 p95 policy) The per-dimension estimator is a BOUNDED
//!   SLIDING WINDOW of the last [`WINDOW_SIZE`] (20) raw samples ([`SampleWindow`]),
//!   FIFO-evicting the oldest on overflow. It is fixed-capacity (never grows past
//!   20 u64 per dimension), so the per-entry byte-cost bound and the LRU cap sizing
//!   still hold. The reservation statistic is now the window **p95** (operator
//!   policy directive) — the earlier monotone log2-bucket max is superseded; the
//!   window recomputes p50/p95/max from the ≤20 retained samples on demand.
//! * The map MUST be bounded (CLAUDE.md unbounded-buffer rule): an
//!   [`LruCache`] with a documented cap; over-cap evicts the LRU key and the
//!   eviction is counted so working-set overflow is visible, not silent.
//! * The key MUST be instance-scoped (security S-3) so one instance's profile
//!   never leaks into another's.

use core::num::NonZeroUsize;
use std::collections::VecDeque;

use lru::LruCache;
use wincode::{SchemaRead, SchemaWrite};

/// (#2497 sched Phase-3 p95 policy) Number of raw samples retained per dimension
/// in the sliding window. The window keeps the LAST `WINDOW_SIZE` completions
/// (FIFO); p50/p95/max are recomputed from the retained samples. `20 *
/// size_of::<u64>() = 160` bytes of samples per dimension. Chosen equal to
/// [`PROFILE_MIN_SAMPLES`] so a K-mature key's window is exactly full — the p95
/// estimate then spans the same sample budget the trust gate requires.
const WINDOW_SIZE: usize = 20;

/// (#task-resource-profile Phase-2a, S4 hardening) Maximum characters kept from
/// each [`ProfileKey`] string component. The LRU cap bounds the key COUNT, not
/// the per-key STRING bytes, so a client sending a megabyte `target_id` /
/// `action_mnemonic` (baggage is client-declared) would break the ~20 MiB
/// footprint bound. Real Bazel labels + mnemonics are far under 256 chars;
/// truncation only collides pathological/oversized keys (bounded, self-limiting).
const PROFILE_KEY_MAX_STR_LEN: usize = 256;

/// (#task-resource-profile Phase-2a; #2497 cap bump) Maximum distinct keys in the
/// bounded [`ProfileMap`] LRU.
///
/// The coarse key is `(instance_name, target_id, action_mnemonic)`. A large
/// Bazel repo has tens of thousands of distinct targets, but the LRU bounds
/// the map to the recency window of ACTIVELY-completing targets rather than the
/// full target universe. `profile_map_evictions_total` surfaces when the
/// working set exceeds this cap (then percentiles are computed over a shorter
/// window for the churned keys).
///
/// Per-entry cost ≈ key (3 `String`s, ~72 B inline + ~100 B heap for a
/// ~100-char Bazel label) + [`Agg`] (4 × [`WINDOW_SIZE`]-slot sliding windows =
/// 4 × 20 × 8 B = 640 B of sample bytes + 4 `VecDeque` headers ~128 B + 8 B
/// count ≈ 776 B) + LRU node overhead (~48 B) ≈ **~1 KiB/entry**. `32768 ×
/// 1 KiB ≈ 32 MiB` — bounded and modest for the scheduler process. (The 20-sample
/// window is SMALLER per-dimension than the old 256-byte histogram, so doubling the
/// key cap keeps the footprint in the same ~tens-of-MiB envelope.)
pub const PROFILE_MAP_MAX_KEYS: usize = 32768;

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

/// (#2497 sched Phase-3 p95 policy) A bounded sliding window of the last
/// [`WINDOW_SIZE`] raw samples for ONE dimension — a fixed-CAPACITY FIFO ring.
/// `record` pushes; on overflow the OLDEST sample is evicted, so the window
/// always reflects the most-recent ≤20 completions. p50/p95/max are recomputed
/// on demand from the retained samples (a ≤20-element sort — trivial cost).
///
/// This SUPERSEDES the earlier monotone log2-bucket histogram: the reservation
/// statistic is now the window p95 (operator policy directive 2026-07-18). The
/// monotone-max conservatism (which never forgot a one-off spike) is dropped;
/// the sole OOM backstop for a p95 under-estimate is the worker-side
/// `memory_gate` free-floor NAK (re-queue on real pressure).
// CAPPED AT WINDOW_SIZE (20): FIFO ring, oldest evicted on overflow — the window
// never holds more than 20 u64 per dimension, so the per-entry byte-cost bound
// and the LRU cap sizing hold (no per-sample growth beyond the fixed 20 slots).
#[derive(Clone, Debug, Default)]
struct SampleWindow {
    samples: VecDeque<u64>,
}

impl SampleWindow {
    /// Push one observed value; evict the oldest when the window would exceed
    /// [`WINDOW_SIZE`] (FIFO). O(1) amortized.
    fn record(&mut self, value: u64) {
        self.samples.push_back(value);
        if self.samples.len() > WINDOW_SIZE {
            self.samples.pop_front();
        }
    }

    /// Estimate the value at quantile `q_num / q_den` (e.g. `19/20` = p95) over
    /// the CURRENT window contents. Exact integer rank math (no float): sort the
    /// ≤20 samples ascending and return the 1-indexed rank `ceil(n * q_num /
    /// q_den)`, clamped into `[1, n]`. `0` for an empty window.
    fn quantile(&self, q_num: u64, q_den: u64) -> u64 {
        let n = self.samples.len();
        if n == 0 {
            return 0;
        }
        let mut sorted: Vec<u64> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        let rank =
            ((n as u64).saturating_mul(q_num).saturating_add(q_den - 1) / q_den).clamp(1, n as u64);
        sorted[(rank - 1) as usize]
    }

    /// The MAX sample currently in the window (observability only — the
    /// reservation basis is now p95, not this). `0` for an empty window.
    fn max(&self) -> u64 {
        self.samples.iter().copied().max().unwrap_or(0)
    }

    /// The retained samples as a plain `Vec<u64>` for a persistence snapshot
    /// (length `0..=WINDOW_SIZE`, oldest-first).
    fn samples_vec(&self) -> Vec<u64> {
        self.samples.iter().copied().collect()
    }

    /// Reconstruct a window from a snapshot's sample vector. `None` when the
    /// length exceeds [`WINDOW_SIZE`] (a corrupt / version-mismatched entry) — the
    /// caller drops the entry and starts fresh for that key, never panicking.
    fn from_samples_vec(v: &[u64]) -> Option<Self> {
        if v.len() > WINDOW_SIZE {
            return None;
        }
        Some(Self {
            samples: v.iter().copied().collect(),
        })
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

/// (#2497 sched Phase-3 p95 policy) Bounded per-[`ProfileKey`] aggregate: a
/// [`WINDOW_SIZE`]-slot sliding window per dimension plus the (monotone) total
/// sample count. Fixed-capacity — the four windows hold at most `4 × 20` u64,
/// regardless of how many samples are folded over the key's lifetime.
#[derive(Clone, Debug, Default)]
pub struct Agg {
    memory_kb: SampleWindow,
    cpu_ns: SampleWindow,
    disk_bytes: SampleWindow,
    net_bytes: SampleWindow,
    sample_count: u64,
    /// (#task-resource-profile Phase-3 §12 staleness) `true` iff this agg was
    /// reconstructed from a persisted snapshot (vs folded live this run). A live key
    /// (`false`) is always DOWN-trusted; a loaded key is DOWN-trusted for LOWERING only
    /// after a FRESH sample folds ([`Self::fresh_since_load`]) AND the snapshot age is
    /// within `resource_profile_persist_max_age_secs`. Default `false` (live).
    loaded: bool,
    /// (#task-resource-profile Phase-3 §12 staleness) `true` once a FRESH sample has
    /// folded into this agg since load. Only meaningful when [`Self::loaded`] — a shifted
    /// distribution's fresh samples raise variance (widening the DOWN margin) AND flip
    /// this, so DOWN begins trusting the key for lowering. Default `false`.
    fresh_since_load: bool,
}

impl Agg {
    /// Fold one sample into every dimension's window. O(1) amortized (each
    /// window pushes and, once full, pops the oldest).
    fn fold(&mut self, sample: ResourceSample) {
        self.memory_kb.record(sample.memory_kb);
        self.cpu_ns.record(sample.cpu_ns);
        self.disk_bytes.record(sample.disk_bytes);
        self.net_bytes.record(sample.net_bytes);
        self.sample_count = self.sample_count.saturating_add(1);
        // (#task-resource-profile Phase-3 §12) A fresh sample makes a loaded agg
        // DOWN-trustworthy (harmless no-op flag for a live agg).
        self.fresh_since_load = true;
    }

    /// (#task-resource-profile Phase-3 §12 staleness) Whether the DOWN-overcommit
    /// direction may trust THIS agg for LOWERING a reservation, given the loaded
    /// snapshot's age and the configured max age. A LIVE key (never loaded) is always
    /// trusted; a LOADED key requires (a) the snapshot age `< max_age_secs` AND (b) a
    /// fresh sample folded since load. RAISE + OBSERVE ignore this (stale ⇒ over-reserve
    /// at worst, never OOM — only LOWERING carries stale-OOM risk).
    fn down_lowering_trusted(&self, loaded_age_secs: Option<u64>, max_age_secs: u64) -> bool {
        if !self.loaded {
            return true;
        }
        self.fresh_since_load && loaded_age_secs.is_none_or(|age| age < max_age_secs)
    }

    #[inline]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    #[inline]
    pub fn memory_p50(&self) -> u64 {
        self.memory_kb.quantile(1, 2)
    }

    /// (#2497 sched Phase-3 p95 policy) The memory p95 over the sliding window —
    /// the RESERVATION statistic (operator policy). Replaces the monotone-max
    /// `memory_tail_kb` as the basis for RAISE / DOWN / undeclared injection. The
    /// window forgets a one-off spike after 20 fresher samples, so a p95
    /// under-estimate is possible; the worker `memory_gate` free-floor NAK is the
    /// sole OOM backstop for that case.
    #[inline]
    pub fn memory_p95_kb(&self) -> u64 {
        self.memory_kb.quantile(19, 20)
    }

    /// (observability) The MAX memory sample in the window. Kept for the observe
    /// path / accuracy classification; NO LONGER the reservation basis.
    #[inline]
    pub fn memory_tail_kb(&self) -> u64 {
        self.memory_kb.max()
    }

    #[inline]
    pub fn cpu_ns_p95(&self) -> u64 {
        self.cpu_ns.quantile(19, 20)
    }

    #[inline]
    pub fn disk_bytes_p95(&self) -> u64 {
        self.disk_bytes.quantile(19, 20)
    }

    #[inline]
    pub fn net_bytes_p95(&self) -> u64 {
        self.net_bytes.quantile(19, 20)
    }

    /// Variance measure = memory p95/p50 ratio, ×100 to stay in integer math.
    /// Memory is the load-bearing dimension for the reservation, so variance is
    /// measured there. A ratio of exactly 1.0 reports `100`. A degenerate
    /// `p50 == 0` with `p95 > 0` reports `u64::MAX` (treat as maximally spread).
    pub fn memory_variance_ratio_x100(&self) -> u64 {
        let p50 = self.memory_p50();
        let p95 = self.memory_p95_kb();
        if p50 == 0 {
            return if p95 == 0 { 100 } else { u64::MAX };
        }
        p95.saturating_mul(100) / p50
    }

    /// (#task-resource-profile Phase-3 §12; #2497 window format) Build a plain-data
    /// snapshot of this agg's four dimension WINDOWS + total sample count for
    /// persistence. The staleness flags are NOT persisted — they are a per-RUN
    /// property (a reloaded agg is `loaded=true, fresh_since_load=false` by
    /// construction).
    fn to_sample_dims(&self) -> (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>, u64) {
        (
            self.memory_kb.samples_vec(),
            self.cpu_ns.samples_vec(),
            self.disk_bytes.samples_vec(),
            self.net_bytes.samples_vec(),
            self.sample_count,
        )
    }

    /// (#task-resource-profile Phase-3 §12; #2497 window format) Reconstruct a LOADED
    /// agg from a snapshot's per-dimension sample vectors. `None` when any window
    /// length exceeds [`WINDOW_SIZE`], OR the sample count is implausible vs the
    /// retained window (a corrupt entry — dropped, never panics). The reconstructed
    /// agg is marked `loaded` + not-yet-`fresh_since_load`.
    fn from_sample_dims(
        mem: &[u64],
        cpu: &[u64],
        disk: &[u64],
        net: &[u64],
        sample_count: u64,
    ) -> Option<Self> {
        let memory_kb = SampleWindow::from_samples_vec(mem)?;
        let cpu_ns = SampleWindow::from_samples_vec(cpu)?;
        let disk_bytes = SampleWindow::from_samples_vec(disk)?;
        let net_bytes = SampleWindow::from_samples_vec(net)?;
        // Validation: the retained window can hold at most `sample_count` samples (it
        // is the last min(count, WINDOW_SIZE) of them). A window LONGER than the
        // declared total is a corrupt entry. All four dimensions fold together, so the
        // memory window's length is representative.
        let mem_len = memory_kb.samples.len() as u64;
        if sample_count == 0 || mem_len > sample_count {
            return None;
        }
        Some(Self {
            memory_kb,
            cpu_ns,
            disk_bytes,
            net_bytes,
            sample_count,
            loaded: true,
            fresh_since_load: false,
        })
    }
}

/// (#task-resource-profile Phase-3 §12; #2497 window format) Plain-data snapshot of
/// one profile-map entry (key parts + the four dimension SAMPLE WINDOWS + total
/// sample count) for persistence. Pure data — the persist layer serializes this
/// directly as the versioned wincode blob's entry form. The staleness flags are
/// per-run and NOT part of the snapshot.
///
/// SCHEMA CHANGE (#2497): the four dimensions are now raw `Vec<u64>` sliding-window
/// samples (was fixed-length `Vec<u32>` log2-bucket histograms). The persistence
/// blob VERSION is bumped so a pre-#2497 snapshot version-mismatches on load and is
/// DISCARDED (re-learn) rather than mis-parsed.
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ProfileEntrySnapshot {
    pub instance_name: String,
    pub target_id: String,
    pub action_mnemonic: String,
    pub memory_samples: Vec<u64>,
    pub cpu_samples: Vec<u64>,
    pub disk_samples: Vec<u64>,
    pub net_samples: Vec<u64>,
    pub sample_count: u64,
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

    /// (#task-resource-profile hierarchical-key) Derive the COARSE key
    /// `(instance_name, "", action_mnemonic)` — the fallback tier that blends all
    /// target-bearing samples sharing a mnemonic (an empty-target sample derives no
    /// fine key and so is skipped from BOTH tiers by `resource_profile_keys`), or
    /// `None` when the mnemonic is absent.
    ///
    /// The coarse key is keyed with an EMPTY `target_id`, which is the coarse
    /// MARKER: [`from_parts`](Self::from_parts) REQUIRES a non-empty `target_id`,
    /// so a fine key can NEVER have an empty target — the coarse key is therefore
    /// collision-free with the entire fine-key space in the shared [`ProfileMap`].
    /// Unlike `from_parts` (which SKIPS an empty target as absent baggage), this
    /// constructor treats the empty target as the intentional coarse discriminant,
    /// so it must be built via THIS constructor (never `from_parts`).
    ///
    /// A coarse key still needs its mnemonic to be meaningful (it groups a
    /// mnemonic's targets); an empty `action_mnemonic` (absent baggage) returns
    /// `None`, mirroring `from_parts`'s skip-on-empty-baggage. The empty
    /// `instance_name` (the DEFAULT instance, NOT baggage) is allowed, same as
    /// `from_parts`. `PROFILE_KEY_MAX_STR_LEN` clamps the mnemonic (S4).
    pub fn coarse(instance_name: &str, action_mnemonic: &str) -> Option<Self> {
        if action_mnemonic.is_empty() {
            return None;
        }
        Some(Self {
            instance_name: clamp_key_str(instance_name),
            // The coarse marker: empty target_id, collision-free with all fine keys.
            target_id: String::new(),
            action_mnemonic: clamp_key_str(action_mnemonic),
        })
    }
}

/// (#task-resource-profile hierarchical-key) Which tier of the hierarchical
/// `(instance, target, mnemonic)` → `(instance, mnemonic)` key a lookup resolved
/// to. A COARSE hit blends all target-bearing samples of a mnemonic → higher variance /
/// over-reservation risk, so a downstream consumer (the eventual Phase-3
/// DOWN-override) must NOT trust a coarse tail; it is carried through so the
/// fine-vs-coarse coverage is visible and gate-able.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProfileTier {
    /// The fine `(instance, target, mnemonic)` key.
    Fine,
    /// The coarse `(instance, mnemonic)` fallback key.
    Coarse,
}

/// (#task-resource-profile hierarchical-key) Result of a fine→coarse hierarchical
/// tail lookup with the `K` (`min_samples`) gate applied by the map.
///
/// The fallback defeats K-starvation: a fine key that has not yet reached `K`
/// samples (the ~2.5-samples/key prod regime) can borrow the ALREADY-mature
/// coarse `(instance, mnemonic)` key's tail, so the observe pipeline has
/// coverage immediately instead of after the fine keys mature (~18 h).
/// (#task-resource-profile Phase-3 §8) One key's peeked memory statistics — the
/// tail (monotone max upper bound), the p50 central estimate, the p95/p50 variance
/// ratio ×100, and the sample count. Internal bundle so [`ProfileMap::lookup_tiered`]
/// reads all four in one `peek` per tier.
#[derive(Clone, Copy, Debug)]
struct TierStats {
    tail_kb: u64,
    /// (#2497 sched Phase-3 p95 policy) The window p95 — the RESERVATION statistic
    /// (RAISE / DOWN / undeclared injection all read this).
    p95_kb: u64,
    p50_kb: u64,
    variance_ratio_x100: u64,
    samples: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TieredTail {
    /// The chosen tier (fine preferred, else coarse) reached `>= K` samples — a
    /// trusted tail. `tier` records WHICH tier; the statistics are from it.
    ///
    /// (#task-resource-profile Phase-3 §8) `p50_kb` (the central estimate) and
    /// `variance_ratio_x100` (memory p95/p50 ×100) ride alongside the `tail_kb`
    /// so the Phase-3 DOWN-overcommit margin function can size a reserve from the
    /// central estimate + the measured spread WITHOUT a second map read — the tail
    /// alone (a monotone max) cannot express "reserve p50 × (1 + margin(variance))".
    Trusted {
        tier: ProfileTier,
        tail_kb: u64,
        /// (#2497 sched Phase-3 p95 policy) The window p95 — the RESERVATION basis
        /// the enforce phase uses for RAISE, DOWN, and undeclared injection.
        /// `tail_kb` (window max) is retained for the observe/accuracy path only.
        p95_kb: u64,
        p50_kb: u64,
        variance_ratio_x100: u64,
        samples: u64,
    },
    /// A profile existed at >= 1 tier but NEITHER reached `K` samples (untrusted
    /// tail). Carries the consulted tier's tail + p95 + sample count (fine preferred) so a
    /// dispatch-time stash can represent "present-but-untrusted" for the leave-one-out
    /// check. `p50_kb`/`variance_ratio_x100` are NOT carried: they are trustworthy only
    /// at `>= K` samples (the caller gates on `samples >= K`), so only `Trusted` holds them.
    /// `p95_kb` (#2497) is carried so a `< K` dispatch stash records the SAME statistic the
    /// enforce/observe path reserves on — though it is suppressed by the K-recheck at scoring.
    LowSample {
        tier: ProfileTier,
        tail_kb: u64,
        p95_kb: u64,
        samples: u64,
    },
    /// No profile at either tier (map warming / absent baggage).
    NoProfile,
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
    /// (#task-resource-profile Phase-3 §12 staleness) Age (seconds) of the persisted
    /// snapshot at the moment it was loaded, or `None` when this map was never loaded
    /// (fresh start). The DOWN direction refuses to trust a LOADED key for lowering once
    /// this exceeds `resource_profile_persist_max_age_secs`.
    loaded_snapshot_age_secs: Option<u64>,
}

impl ProfileMap {
    /// Construct a map with an explicit cap and variance tuning.
    pub fn new(max_keys: NonZeroUsize, min_samples: u64, high_variance_ratio_x100: u64) -> Self {
        Self {
            cache: LruCache::new(max_keys),
            high_variance_keys: 0,
            min_samples,
            high_variance_ratio_x100,
            loaded_snapshot_age_secs: None,
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
    /// match-path reader must use this so an observe-only read never perturbs
    /// eviction order (design §6 / N-3).
    #[cfg(test)]
    pub fn peek(&self, key: &ProfileKey) -> Option<&Agg> {
        self.cache.peek(key)
    }

    /// (#task-resource-profile Phase-2b) OBSERVE-PATH read: the tail-aware memory
    /// statistic and current sample count for `key`, or `None` when the key is
    /// absent. Uses `peek` (never `get`) so an observe read never bumps LRU
    /// recency (N-3). The caller applies the `K` sample-count gate — a
    /// freshly-LRU-re-admitted low-sample key must NOT be trusted (pair-a MINOR /
    /// red-team A1b), which is why the raw `sample_count` is returned alongside
    /// the tail rather than pre-gated here.
    pub fn peek_memory_tail(&self, key: &ProfileKey) -> Option<(u64, u64)> {
        self.cache
            .peek(key)
            .map(|agg| (agg.memory_tail_kb(), agg.sample_count()))
    }

    /// (#task-resource-profile hierarchical-key) FINE→COARSE fallback tail lookup.
    ///
    /// Try the FINE key first: if it has `>= K` (`min_samples`) samples, use its
    /// tail and mark [`ProfileTier::Fine`]. Otherwise try the COARSE key: if it has
    /// `>= K` samples, use its tail and mark [`ProfileTier::Coarse`]. If neither
    /// tier is trusted, return [`TieredTail::LowSample`] when a profile exists at
    /// SOME tier (fine preferred for the carried data) or [`TieredTail::NoProfile`]
    /// when neither tier is present.
    ///
    /// This is the K-starvation defeat: the coarse `(instance, mnemonic)` key
    /// matures ~30× faster than a fine `(instance, target, mnemonic)` key, so a
    /// fine key still under `K` borrows the mature coarse tail immediately.
    ///
    /// Both reads use `peek` (never `get`) so an observe-only lookup NEVER bumps
    /// LRU recency (design §6 / N-3) for either tier.
    pub fn lookup_tiered(&self, fine: &ProfileKey, coarse: &ProfileKey) -> TieredTail {
        let fine_peek = self.peek_tier_stats(fine);
        if let Some(s) = fine_peek {
            if s.samples >= self.min_samples {
                return TieredTail::Trusted {
                    tier: ProfileTier::Fine,
                    tail_kb: s.tail_kb,
                    p95_kb: s.p95_kb,
                    p50_kb: s.p50_kb,
                    variance_ratio_x100: s.variance_ratio_x100,
                    samples: s.samples,
                };
            }
        }
        let coarse_peek = self.peek_tier_stats(coarse);
        if let Some(s) = coarse_peek {
            if s.samples >= self.min_samples {
                return TieredTail::Trusted {
                    tier: ProfileTier::Coarse,
                    tail_kb: s.tail_kb,
                    p95_kb: s.p95_kb,
                    p50_kb: s.p50_kb,
                    variance_ratio_x100: s.variance_ratio_x100,
                    samples: s.samples,
                };
            }
        }
        // Neither tier trusted: distinguish present-but-low-sample from absent so
        // the caller keeps the low-sample vs no-profile counters (and the accuracy
        // path's leave-one-out low-sample vs no-dispatch classification).
        match (fine_peek, coarse_peek) {
            (Some(s), _) => TieredTail::LowSample {
                tier: ProfileTier::Fine,
                tail_kb: s.tail_kb,
                p95_kb: s.p95_kb,
                samples: s.samples,
            },
            (None, Some(s)) => TieredTail::LowSample {
                tier: ProfileTier::Coarse,
                tail_kb: s.tail_kb,
                p95_kb: s.p95_kb,
                samples: s.samples,
            },
            (None, None) => TieredTail::NoProfile,
        }
    }

    /// (#task-resource-profile Phase-3 §8) Peek one key's memory statistics WITHOUT
    /// bumping LRU recency (`peek`, never `get`): the tail (monotone max), the p50
    /// central estimate, the p95/p50 variance ratio ×100, and the sample count.
    /// `None` when the key is absent. Shared by [`Self::lookup_tiered`] so both
    /// tiers derive the same stat bundle; the caller applies the `K` gate on
    /// `samples`.
    fn peek_tier_stats(&self, key: &ProfileKey) -> Option<TierStats> {
        self.cache.peek(key).map(|agg| TierStats {
            tail_kb: agg.memory_tail_kb(),
            p95_kb: agg.memory_p95_kb(),
            p50_kb: agg.memory_p50(),
            variance_ratio_x100: agg.memory_variance_ratio_x100(),
            samples: agg.sample_count(),
        })
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

    /// (#task-resource-profile Phase-3 §12) Plain-data snapshot of EVERY resident entry
    /// for persistence. `iter()` does NOT bump LRU recency (unlike `get`), so snapshotting
    /// never perturbs eviction order. The caller CLONES this out from under the lock, then
    /// serializes + writes off the lock (no I/O held across the map lock).
    pub fn snapshot_entries(&self) -> Vec<ProfileEntrySnapshot> {
        self.cache
            .iter()
            .map(|(key, agg)| {
                let (memory_samples, cpu_samples, disk_samples, net_samples, sample_count) =
                    agg.to_sample_dims();
                ProfileEntrySnapshot {
                    instance_name: key.instance_name.clone(),
                    target_id: key.target_id.clone(),
                    action_mnemonic: key.action_mnemonic.clone(),
                    memory_samples,
                    cpu_samples,
                    disk_samples,
                    net_samples,
                    sample_count,
                }
            })
            .collect()
    }

    /// (#task-resource-profile Phase-3 §12) Populate the map from a loaded snapshot BEFORE
    /// the scheduler serves. `loaded_age_secs` is the snapshot's age at load (now −
    /// snapshot wall-time), stamped so the DOWN direction can refuse a too-old snapshot.
    /// Each entry is VALIDATED (valid key via the constructors, correct histogram lengths,
    /// plausible sample count); an invalid entry is SKIPPED (never panics). Loaded aggs are
    /// marked `loaded` (DOWN-untrusted for lowering until a fresh sample folds). Returns the
    /// number of entries actually loaded. Existing high-variance keys are counted.
    pub fn load_entries(&mut self, entries: Vec<ProfileEntrySnapshot>, loaded_age_secs: u64) -> usize {
        self.loaded_snapshot_age_secs = Some(loaded_age_secs);
        let mut loaded = 0usize;
        for e in entries {
            // Reconstruct the EXACT key (coarse = empty target marker) via the validating
            // constructors — an invalid key (empty mnemonic, etc.) is skipped.
            let key = if e.target_id.is_empty() {
                ProfileKey::coarse(&e.instance_name, &e.action_mnemonic)
            } else {
                ProfileKey::from_parts(&e.instance_name, &e.target_id, &e.action_mnemonic)
            };
            let Some(key) = key else {
                continue;
            };
            let Some(agg) = Agg::from_sample_dims(
                &e.memory_samples,
                &e.cpu_samples,
                &e.disk_samples,
                &e.net_samples,
                e.sample_count,
            ) else {
                continue;
            };
            let high = self.is_high_variance(&agg);
            // push returns Some((evicted_key, evicted_agg)) if the cache was at capacity.
            if let Some((_ek, evicted)) = self.cache.push(key, agg) {
                if self.is_high_variance(&evicted) {
                    self.high_variance_keys = self.high_variance_keys.saturating_sub(1);
                }
            }
            if high {
                self.high_variance_keys += 1;
            }
            loaded += 1;
        }
        loaded
    }

    /// (#task-resource-profile Phase-3 §12 staleness) The DOWN-overcommit RESERVE
    /// STATISTIC (window p95) for the tier `lookup_tiered` would resolve to, or `None`
    /// when the DOWN direction may not LOWER the reservation at all.
    ///
    /// Determines the chosen tier (fine ≥K, else coarse ≥K, else `None` — DOWN needs a
    /// trusted profile), applies that agg's staleness gate against the loaded-snapshot age
    /// + `max_age_secs`, and — only if both pass — returns that SAME agg's
    /// [`Agg::memory_p95_kb`]. A LIVE key (never loaded) always passes the staleness gate;
    /// RAISE + INJECT + OBSERVE never call this (they reserve the fixed p95 from
    /// `lookup_tiered`, and stale ⇒ over-reserve, never OOM).
    ///
    /// Returning the statistic and the verdict TOGETHER is deliberate, and it is the
    /// reason this function exists in this shape. Previously the verdict came from here
    /// (`down_lowering_trusted -> bool`) and the number from a SEPARATE `lookup_tiered`
    /// peek, so the two could in principle be read off different tiers. Now they provably
    /// come from ONE peek of ONE agg, and "DOWN may not lower" is unrepresentable as a
    /// reservation value.
    ///
    /// That coupling is a PREREQUISITE, not a tidy-up: the staleness gate's known
    /// tier-fallback inversion (a mature-but-stale FINE key is chosen and refused instead
    /// of falling through to a fresh COARSE sibling) can only be fixed by making tier
    /// selection staleness-aware HERE. Doing that while the statistic still came from a
    /// separate `lookup_tiered` peek would grant trust from the coarse agg while reserving
    /// on the fine agg's p95 — a tier mismatch. See
    /// `.claude/audits/phase3-staleness-gate-diagnosis-2026-08-08.md` §4/§8 Option A, and
    /// `down_lowering_reserve_kb_statistic_and_verdict_come_from_the_same_tier` which pins
    /// the property this function is here to provide.
    pub fn down_lowering_reserve_kb(
        &self,
        fine: &ProfileKey,
        coarse: &ProfileKey,
        max_age_secs: u64,
    ) -> Option<u64> {
        let chosen = self
            .cache
            .peek(fine)
            .filter(|agg| agg.sample_count() >= self.min_samples)
            .or_else(|| {
                self.cache
                    .peek(coarse)
                    .filter(|agg| agg.sample_count() >= self.min_samples)
            })?;
        if !chosen.down_lowering_trusted(self.loaded_snapshot_age_secs, max_age_secs) {
            return None;
        }
        Some(chosen.memory_p95_kb())
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

    // ── (a) (#2497) sliding-window estimator: p50/p95/max over the last ≤20 raw
    //         samples (exact values, not bucket representatives) ──

    #[test]
    fn window_p50_p95_max_over_last_twenty() {
        // A spread of exactly 20 samples (window full): 15 @ 1000, 5 @ 100_000.
        let mut agg = Agg::default();
        for _ in 0..15 {
            agg.fold(mem_sample(1000));
        }
        for _ in 0..5 {
            agg.fold(mem_sample(100_000));
        }
        assert_eq!(agg.sample_count(), 20);

        // Sorted window = [1000×15, 100_000×5]. p50 rank ceil(20/2)=10 → the 10th
        // smallest = 1000 (an EXACT raw sample, not a bucket rep).
        assert_eq!(
            agg.memory_p50(),
            1000,
            "window p50 must be the exact 10th-of-20 raw sample (1000), not a bucket \
             representative; got {}",
            agg.memory_p50()
        );
        // p95 rank ceil(20*19/20)=19 → the 19th smallest = 100_000 (into the high mode).
        assert_eq!(
            agg.memory_p95_kb(),
            100_000,
            "window p95 must be the exact 19th-of-20 raw sample (100_000); got {}",
            agg.memory_p95_kb()
        );
        // Max = the largest raw sample.
        assert_eq!(agg.memory_tail_kb(), 100_000, "window max must be the largest raw sample");
        // variance ratio ×100 = 100_000*100/1000 = 10000 (100×).
        assert_eq!(agg.memory_variance_ratio_x100(), 10_000);
    }

    #[test]
    fn window_evicts_oldest_beyond_twenty() {
        // Fill the window with 20 @ 1000, then fold ONE fresh 9000. The 21st push
        // must FIFO-evict the oldest (a 1000), so the window is [1000×19, 9000] and
        // the stats reflect ONLY the last 20 — the monotone-max history is forgotten.
        let mut agg = Agg::default();
        for _ in 0..20 {
            agg.fold(mem_sample(1000));
        }
        agg.fold(mem_sample(9000));
        assert_eq!(agg.sample_count(), 21, "sample_count is the monotone total (21)");
        // Window (oldest-first) must be exactly the last 20 samples: [1000×19, 9000].
        let win = agg.to_sample_dims().0;
        assert_eq!(win.len(), WINDOW_SIZE, "the window must stay bounded at WINDOW_SIZE (20)");
        assert_eq!(
            win.iter().filter(|&&v| v == 1000).count(),
            19,
            "the 21st fold must evict exactly ONE oldest 1000 (FIFO), leaving 19"
        );
        assert_eq!(
            win.iter().filter(|&&v| v == 9000).count(),
            1,
            "the fresh 9000 must be retained as the newest sample"
        );
        // p95 rank ceil(20*19/20)=19 → 19th of [1000×19, 9000] = 1000; max = 9000.
        assert_eq!(agg.memory_p95_kb(), 1000, "stats reflect ONLY the last 20 samples");
        assert_eq!(agg.memory_tail_kb(), 9000, "max reflects the fresh sample in the window");
    }

    #[test]
    fn window_below_k_computes_over_available_samples() {
        // Fewer than 20 samples: the quantile is over the available window, not padded.
        // 4 @ 1000, 1 @ 50_000 (5 samples). Sorted [1000×4, 50_000]. p95 rank
        // ceil(5*19/20)=ceil(4.75)=5 → the 5th (last) = 50_000; p50 rank ceil(5/2)=3 → 1000.
        let mut agg = Agg::default();
        for _ in 0..4 {
            agg.fold(mem_sample(1000));
        }
        agg.fold(mem_sample(50_000));
        assert_eq!(agg.sample_count(), 5);
        assert_eq!(agg.memory_p50(), 1000, "p50 over 5 samples = 3rd smallest = 1000");
        assert_eq!(
            agg.memory_p95_kb(),
            50_000,
            "p95 over 5 samples = 5th (ceil(5*19/20)=5) = 50_000"
        );
    }

    // ── (#phase3-down-reserve) DOWN reserve statistic + verdict from ONE agg ──

    /// (#phase3-down-reserve) THE property this refactor exists to provide: the DOWN
    /// reserve statistic and the DOWN trust verdict come from the SAME tier — the one
    /// `lookup_tiered` resolves to. Asserted as an EQUALITY against `lookup_tiered`'s own
    /// `p95_kb` with the two tiers holding DELIBERATELY DIFFERENT distributions, so a
    /// tier mismatch cannot hide behind equal numbers.
    ///
    /// This is not coverage for its own sake. The staleness gate's tier-fallback
    /// inversion (a mature-but-stale FINE key is chosen and refused rather than falling
    /// through to a fresh COARSE sibling) is fixed by making tier selection
    /// staleness-aware inside `down_lowering_reserve_kb`. That fix is only SOUND while
    /// the statistic travels with the verdict: if a future edit reintroduces the split —
    /// reserving on `lookup_tiered`'s fine p95 while trusting a coarse agg — the fix
    /// silently reserves the wrong tier's number. See
    /// `.claude/audits/phase3-staleness-gate-diagnosis-2026-08-08.md` §4.
    ///
    /// MUTATION 1: swap the peek order in `down_lowering_reserve_kb` (coarse before fine)
    /// → case A red-fails (returns the coarse 900_000, not the fine 190_000).
    /// MUTATION 2: delete the `.or_else(coarse)` fallback → case B red-fails
    /// (returns None where `lookup_tiered` resolves the coarse tier).
    #[test]
    fn down_lowering_reserve_kb_statistic_and_verdict_come_from_the_same_tier() {
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));

        // ── Case A: FINE is mature (>=K) ⇒ BOTH must resolve the FINE tier. ──
        // Fine: 20 ascending samples ⇒ p95 = 190_000. Coarse: 20 samples an order of
        // magnitude larger ⇒ p95 = rank 19 = 855_000. The tiers CANNOT be confused by value.
        let mut map = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 20, 200);
        for i in 1..=20u64 {
            map.record(fine.clone(), mem_sample(i * 10_000));
            map.record(coarse.clone(), mem_sample(i * 45_000));
        }
        let TieredTail::Trusted { tier, p95_kb, .. } = map.lookup_tiered(&fine, &coarse) else {
            panic!("both tiers are >=K, so lookup_tiered must return Trusted");
        };
        assert_eq!(
            tier,
            ProfileTier::Fine,
            "fixture precondition: with a mature fine key lookup_tiered must resolve FINE"
        );
        let reserve = map.down_lowering_reserve_kb(&fine, &coarse, 1000);
        assert_eq!(
            reserve,
            Some(190_000),
            "DOWN must reserve the FINE tier's p95 (190_000) when the fine key is mature. \
             Got {reserve:?}. A value of 855_000 means the tier PREFERENCE inverted and \
             DOWN is reserving the coarse blend; None means the fine tier was refused."
        );
        assert_eq!(
            reserve,
            Some(p95_kb),
            "SPLIT-TIER REGRESSION: the DOWN reserve statistic ({reserve:?}) must equal the \
             statistic lookup_tiered resolves ({}), because arm SELECTION keys on the \
             latter. If these diverge, the arm is chosen on one tier's p95 and the \
             reservation taken from another's — the exact defect this function's \
             Option<u64> shape exists to make unrepresentable, and the soundness \
             precondition for making tier selection staleness-aware here.",
            p95_kb
        );

        // ── Case B: FINE is BELOW K ⇒ BOTH must fall through to the COARSE tier. ──
        let mut map_b = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 20, 200);
        for _ in 0..3u64 {
            map_b.record(fine.clone(), mem_sample(11_111));
        }
        for i in 1..=20u64 {
            map_b.record(coarse.clone(), mem_sample(i * 45_000));
        }
        let TieredTail::Trusted { tier: tier_b, p95_kb: p95_kb_b, .. } =
            map_b.lookup_tiered(&fine, &coarse)
        else {
            panic!("fine is <K but coarse is >=K, so lookup_tiered must return Trusted");
        };
        assert_eq!(
            tier_b,
            ProfileTier::Coarse,
            "fixture precondition: with an under-K fine key lookup_tiered must resolve COARSE"
        );
        let reserve_b = map_b.down_lowering_reserve_kb(&fine, &coarse, 1000);
        assert_eq!(
            reserve_b,
            Some(855_000),
            "DOWN must fall through to the COARSE tier's p95 (855_000) when the fine key \
             is below K. Got {reserve_b:?}. None means the coarse fallback was dropped, \
             which would silently disable DOWN for every young key — the population that \
             most needs it; 11_111 means an under-K fine tier was reserved on."
        );
        assert_eq!(
            reserve_b,
            Some(p95_kb_b),
            "SPLIT-TIER REGRESSION on the coarse fallback: DOWN reserved {reserve_b:?} but \
             lookup_tiered resolves {} — arm selection and the reservation would be read \
             off different tiers.",
            p95_kb_b
        );
    }

    /// (#phase3-down-reserve) `None`, never a number, when no tier is trusted — so
    /// "DOWN may not lower" is unrepresentable as a reservation value. A `bool` verdict
    /// plus a separately-fetched statistic could not express this.
    #[test]
    fn down_lowering_reserve_kb_is_none_when_no_tier_is_trusted() {
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        let mut cold = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 20, 200);
        cold.record(fine.clone(), mem_sample(50_000));
        assert_eq!(
            cold.down_lowering_reserve_kb(&fine, &coarse, 1000),
            None,
            "a below-K profile at BOTH tiers must yield None (DOWN may not lower), not a \
             statistic — an untrusted number is meaningless and must be unrepresentable \
             as a reserve"
        );
    }

    #[test]
    fn window_tight_distribution_is_low_variance() {
        let mut agg = Agg::default();
        for _ in 0..100 {
            agg.fold(mem_sample(1000));
        }
        assert_eq!(agg.memory_p50(), agg.memory_p95_kb());
        assert_eq!(agg.memory_tail_kb(), 1000, "a tight window's max equals its p50/p95");
        assert_eq!(
            agg.memory_variance_ratio_x100(),
            100,
            "a single-valued (tight) window must report a 1.0× ratio (100), got {}",
            agg.memory_variance_ratio_x100()
        );
    }

    #[test]
    fn window_dimensions_are_independent() {
        // Each dimension folds its own value; a per-dim p95 must reflect only that
        // dimension (independent windows — no cross-contamination).
        let mut agg = Agg::default();
        for _ in 0..10 {
            agg.fold(ResourceSample {
                memory_kb: 2048,
                cpu_ns: 1 << 30,
                disk_bytes: 4096,
                net_bytes: 8192,
            });
        }
        // All 10 samples identical per dim → p95 == that exact raw value.
        assert_eq!(agg.memory_p95_kb(), 2048);
        assert_eq!(agg.cpu_ns_p95(), 1 << 30);
        assert_eq!(agg.disk_bytes_p95(), 4096);
        assert_eq!(agg.net_bytes_p95(), 8192);
    }

    #[test]
    fn window_is_bounded_at_window_size() {
        // The window NEVER grows past WINDOW_SIZE regardless of total samples folded —
        // the per-entry cost bound that keeps the LRU footprint sizing valid.
        let mut agg = Agg::default();
        for i in 0..1000u64 {
            agg.fold(mem_sample(i + 1));
        }
        let (mem, cpu, disk, net, _count) = agg.to_sample_dims();
        assert_eq!(mem.len(), WINDOW_SIZE, "the memory window must be capped at WINDOW_SIZE (20)");
        assert_eq!(cpu.len(), WINDOW_SIZE, "every dimension window must be capped at WINDOW_SIZE");
        assert_eq!(disk.len(), WINDOW_SIZE);
        assert_eq!(net.len(), WINDOW_SIZE);
        assert_eq!(
            agg.sample_count(),
            1000,
            "sample_count is the monotone total (1000) even though the window holds only 20"
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

    // ── (#2497) p95 reservation statistic drops the single-spike max conservatism ──

    #[test]
    fn window_p95_below_max_drops_single_spike() {
        // 19 @ 1000 + 1 @ 1_000_000 (20 samples, window full). The old monotone-max
        // reservation would have stood at ~1_000_000 forever; the p95 policy reserves
        // 1000 (the single 1_000_000 spike is above the 95th percentile). The worker
        // free-floor NAK is the OOM backstop for the dropped spike.
        let mut agg = Agg::default();
        for _ in 0..19 {
            agg.fold(mem_sample(1000));
        }
        agg.fold(mem_sample(1_000_000));
        assert_eq!(agg.sample_count(), 20);

        let p95 = agg.memory_p95_kb();
        // Sorted [1000×19, 1_000_000]. p95 rank ceil(20*19/20)=19 → 19th = 1000.
        assert_eq!(
            p95, 1000,
            "p95 must be the 19th-of-20 sample (1000) — the single 1_000_000 spike sits \
             ABOVE p95, so p95 drops it (the whole point of moving off monotone-max); got {p95}"
        );
        let max = agg.memory_tail_kb();
        assert_eq!(max, 1_000_000, "the window max still sees the spike (observability only)");
        assert!(
            p95 < max,
            "on a single-spike window the p95 reservation statistic MUST be BELOW the max — \
             this is the conservatism the p95 policy intentionally drops; p95 {p95} !< max {max}"
        );
    }

    #[test]
    fn p95_and_max_of_empty_agg_are_zero() {
        let agg = Agg::default();
        assert_eq!(
            agg.memory_p95_kb(),
            0,
            "an un-sampled agg has no p95 → 0 (the K-gate keeps it out of any reservation)"
        );
        assert_eq!(agg.memory_tail_kb(), 0, "an un-sampled agg has no max → 0");
    }

    #[test]
    fn peek_memory_tail_reads_tail_and_count_without_recency_bump() {
        // cap 2: prove peek_memory_tail returns (tail, samples) for a present key,
        // None for an absent one, and (unlike get) does NOT bump LRU recency.
        let mut map = ProfileMap::new(NonZeroUsize::new(2).unwrap(), 1, 200);
        let (k1, k2, k3) = (key("//a", "M"), key("//b", "M"), key("//c", "M"));
        map.record(k1.clone(), mem_sample(2_097_152)); // window sample
        map.record(k1.clone(), mem_sample(20_971_520)); // window sample (the max)
        map.record(k2.clone(), mem_sample(100));

        assert_eq!(
            map.peek_memory_tail(&k1),
            Some((20_971_520, 2)),
            "peek_memory_tail must return (max=20_971_520, samples=2) for a profiled key"
        );
        assert_eq!(
            map.peek_memory_tail(&key("//absent", "M")),
            None,
            "peek_memory_tail must return None for an unprofiled key"
        );

        // peek did NOT bump recency: k1 is still LRU, so inserting k3 evicts k1.
        let o = map.record(k3.clone(), mem_sample(100));
        assert!(o.evicted);
        assert!(
            map.peek_memory_tail(&k1).is_none(),
            "peek must not bump recency — k1 (peeked but not get) stays LRU and evicts"
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

    fn coarse_key(mnemonic: &str) -> ProfileKey {
        ProfileKey::coarse("main", mnemonic).expect("non-empty mnemonic")
    }

    // ── (#task-resource-profile hierarchical-key) coarse key + fallback ──

    #[test]
    fn coarse_key_has_empty_target_and_is_collision_free_with_fine() {
        let c = ProfileKey::coarse("main", "CppCompile").expect("non-empty mnemonic → Some");
        assert_eq!(
            c.target_id, "",
            "the coarse key MUST carry an empty target_id (the coarse marker) so it is \
             collision-free with every fine key (from_parts requires a non-empty target)"
        );
        assert_eq!(c.instance_name, "main");
        assert_eq!(c.action_mnemonic, "CppCompile");
        // Same instance+mnemonic, fine vs coarse → DISTINCT keys (they must not collapse).
        let fine = ProfileKey::from_parts("main", "//foo:bar", "CppCompile").unwrap();
        assert_ne!(
            fine, c,
            "the fine (instance,target,mnemonic) and coarse (instance,mnemonic) keys must be \
             distinct map entries — collapsing them would double-count"
        );
    }

    #[test]
    fn coarse_key_skips_empty_mnemonic_but_allows_empty_instance() {
        assert!(
            ProfileKey::coarse("main", "").is_none(),
            "an empty action_mnemonic (absent baggage) must skip (None) — a coarse key with \
             no mnemonic groups nothing meaningful"
        );
        let c = ProfileKey::coarse("", "CppCompile")
            .expect("empty instance is the default instance → still Some");
        assert_eq!(c.instance_name, "");
    }

    #[test]
    fn coarse_key_clamps_oversized_mnemonic() {
        let huge = "m".repeat(1_000_000);
        let c = ProfileKey::coarse("main", &huge).expect("non-empty mnemonic → Some");
        assert_eq!(
            c.action_mnemonic.chars().count(),
            PROFILE_KEY_MAX_STR_LEN,
            "an oversized coarse mnemonic must be clamped to PROFILE_KEY_MAX_STR_LEN chars so \
             the coarse key cannot break the ~20 MiB footprint bound"
        );
    }

    #[test]
    fn lookup_tiered_prefers_fine_when_fine_is_trusted() {
        // K=3. Fine has 3 samples (≥K) at a HIGHER magnitude than coarse. Fallback must
        // choose FINE (the more specific tier), not coarse.
        let mut map = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        for _ in 0..3 {
            map.record(fine.clone(), mem_sample(50_000)); // exact raw sample
        }
        // Coarse has ≥K but at a different magnitude — must be ignored when fine is trusted.
        for _ in 0..5 {
            map.record(coarse.clone(), mem_sample(1000));
        }
        assert_eq!(
            map.lookup_tiered(&fine, &coarse),
            TieredTail::Trusted {
                tier: ProfileTier::Fine,
                // A tight window of 50_000 → max/p95/p50 all the exact raw sample.
                tail_kb: 50_000,
                p95_kb: 50_000,
                p50_kb: 50_000,
                variance_ratio_x100: 100,
                samples: 3,
            },
            "a fine key with ≥K samples must be chosen over coarse (Fine tier, fine's p95)"
        );
    }

    #[test]
    fn lookup_tiered_carries_p50_and_variance_for_the_margin_function() {
        // (#task-resource-profile Phase-3 §8) The DOWN-overcommit margin function
        // needs BOTH the central estimate (p50) and the spread (p95/p50 ×100) — the
        // tail alone (a monotone max) cannot express `p50 × (1 + margin(variance))`.
        // Prove a SPREAD trusted key threads them through, not just the tail.
        // 15 samples @1000 + 5 @100000 (window full at 20), K=20.
        let mut map = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 20, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        for _ in 0..15 {
            map.record(fine.clone(), mem_sample(1000));
        }
        for _ in 0..5 {
            map.record(fine.clone(), mem_sample(100_000));
        }
        assert_eq!(
            map.lookup_tiered(&fine, &coarse),
            TieredTail::Trusted {
                tier: ProfileTier::Fine,
                // Sorted window [1000×15, 100_000×5]: max = 100_000.
                tail_kb: 100_000,
                // p95 rank 19 lands in the high mode → exact 100_000.
                p95_kb: 100_000,
                // p50 rank 10 lands in the 15-sample low mode → exact 1000.
                p50_kb: 1000,
                // variance = 100_000·100/1000 = 10000 (100×).
                variance_ratio_x100: 10_000,
                samples: 20,
            },
            "lookup_tiered must carry the p95 reservation statistic AND p50/variance \
             alongside the max; a spread window must report p95=100_000, p50=1000, variance=10000"
        );
    }

    #[test]
    fn lookup_tiered_falls_back_to_coarse_when_fine_below_k() {
        // K=3. Fine has only 2 samples (<K) — the prod K-starvation regime. Coarse has
        // ≥K → the fallback must borrow the COARSE tail and mark Coarse.
        let mut map = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        for _ in 0..2 {
            map.record(fine.clone(), mem_sample(1000));
        }
        for _ in 0..4 {
            map.record(coarse.clone(), mem_sample(50_000)); // tight window @50_000
        }
        assert_eq!(
            map.lookup_tiered(&fine, &coarse),
            TieredTail::Trusted {
                tier: ProfileTier::Coarse,
                tail_kb: 50_000,
                p95_kb: 50_000,
                p50_kb: 50_000,
                variance_ratio_x100: 100,
                samples: 4,
            },
            "fine below K but coarse ≥K → the fallback must borrow the mature coarse p95 \
             (Coarse tier) — this is the K-starvation defeat"
        );
    }

    #[test]
    fn lookup_tiered_low_sample_when_neither_tier_reaches_k() {
        // K=5. Fine=2, coarse=3, both < K → LowSample (a profile exists but is untrusted).
        // The carried tier is Fine (preferred) so the dispatch stash can represent it.
        let mut map = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 5, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        for _ in 0..2 {
            map.record(fine.clone(), mem_sample(1000));
        }
        for _ in 0..3 {
            map.record(coarse.clone(), mem_sample(1000));
        }
        assert_eq!(
            map.lookup_tiered(&fine, &coarse),
            TieredTail::LowSample {
                tier: ProfileTier::Fine,
                tail_kb: 1000,
                p95_kb: 1000,
                samples: 2,
            },
            "neither tier reaching K → LowSample carrying the fine (preferred) tier's max + p95"
        );
    }

    #[test]
    fn lookup_tiered_no_profile_when_neither_tier_present() {
        let map = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        assert_eq!(
            map.lookup_tiered(&fine, &coarse),
            TieredTail::NoProfile,
            "no profile at either tier → NoProfile"
        );
    }

    #[test]
    fn lookup_tiered_does_not_bump_recency_of_either_tier() {
        // cap 2, K=1. Populate fine + coarse (2 keys, at cap). A lookup peeks BOTH; if it
        // bumped recency, inserting a 3rd key would evict differently. Prove peek-only:
        // both keys stay LRU-ordered as recorded, so the 3rd insert evicts the oldest (fine).
        let mut map = ProfileMap::new(NonZeroUsize::new(2).unwrap(), 1, 200);
        let (fine, coarse, other) = (key("//a", "M"), coarse_key("M"), key("//b", "M"));
        map.record(fine.clone(), mem_sample(1000)); // oldest
        map.record(coarse.clone(), mem_sample(1000));
        // Lookup must NOT bump recency of fine or coarse.
        let _ = map.lookup_tiered(&fine, &coarse);
        let o = map.record(other.clone(), mem_sample(1000));
        assert!(o.evicted, "cap 2 → 3rd distinct key evicts the LRU key");
        assert!(
            map.peek(&fine).is_none(),
            "lookup_tiered must not bump recency — fine (the oldest, only peeked) must remain \
             LRU and be the eviction victim"
        );
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

    // ── (#task-resource-profile Phase-3 §12) persistence snapshot + staleness ──

    /// Round-trip: `snapshot_entries` → `load_entries` reconstructs a ≥K key WITHOUT
    /// re-collecting samples — the whole point of persistence (no re-warm tax). The
    /// loaded key's tail/p50/count match the source, and it is immediately Trusted.
    ///
    /// MUTATION: make `Agg::from_sample_dims` drop the sample_count (set 0) → the loaded
    /// key falls below K → `lookup_tiered` no longer Trusted → this red-fails.
    #[test]
    fn snapshot_entries_and_load_round_trip_preserves_trusted_key() {
        let mut src = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        for _ in 0..3 {
            src.record(fine.clone(), mem_sample(50_000)); // tight window @50_000
        }
        let entries = src.snapshot_entries();
        assert_eq!(entries.len(), 1, "one resident key must snapshot to one entry");
        assert_eq!(
            entries[0].memory_samples,
            vec![50_000u64; 3],
            "the new snapshot format must persist the RAW window samples (not bucket counts)"
        );

        let mut dst = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let loaded = dst.load_entries(entries, 100);
        assert_eq!(loaded, 1, "the snapshot entry must load back into the fresh map");
        assert_eq!(
            dst.lookup_tiered(&fine, &coarse),
            TieredTail::Trusted {
                tier: ProfileTier::Fine,
                tail_kb: 50_000,
                p95_kb: 50_000,
                p50_kb: 50_000,
                variance_ratio_x100: 100,
                samples: 3,
            },
            "the loaded key must be immediately Trusted (>=K) with the SAME p95/p50/count \
             — profiles survive restart with NO re-collection (the no-re-warm-tax goal)"
        );
    }

    /// (§12 staleness) A LOADED key is NOT DOWN-trusted for lowering until (a) a FRESH
    /// sample folds AND (b) the snapshot age is within `max_age`. A LIVE key is always
    /// trusted.
    ///
    /// MUTATION: in `Agg::down_lowering_trusted`, return `true` unconditionally →
    /// the "loaded, no fresh sample" assert red-fails (a stale profile would be trusted).
    #[test]
    fn loaded_key_down_untrusted_until_fresh_sample_and_within_max_age() {
        let mut src = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        for _ in 0..3 {
            src.record(fine.clone(), mem_sample(50_000));
        }
        let entries = src.snapshot_entries();

        let mut dst = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        dst.load_entries(entries, 100); // snapshot age 100s at load

        assert_eq!(
            dst.down_lowering_reserve_kb(&fine, &coarse, 1000),
            None,
            "a LOADED key with NO fresh sample must NOT be trusted for DOWN-lowering \
             (stale distribution → over-reserve, never a stale-OOM)"
        );

        // Fold a fresh sample → now trusted (age 100 < max_age 1000).
        dst.record(fine.clone(), mem_sample(50_000));
        assert_eq!(
            dst.down_lowering_reserve_kb(&fine, &coarse, 1000),
            Some(50_000),
            "after a FRESH sample folds AND age (100) < max_age (1000) the loaded key \
             becomes DOWN-trusted for lowering, and yields its reserve statistic"
        );

        // Age gate: even WITH a fresh sample, a snapshot older than max_age is refused.
        assert_eq!(
            dst.down_lowering_reserve_kb(&fine, &coarse, 50),
            None,
            "a snapshot age (100) >= max_age (50) must refuse DOWN-lowering even with a \
             fresh sample — the age gate is the second staleness guard"
        );
    }

    /// A LIVE key (never loaded) is always DOWN-trusted (no snapshot in play).
    #[test]
    fn live_key_is_always_down_trusted() {
        let mut map = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let (fine, coarse) = (key("//a", "M"), coarse_key("M"));
        for _ in 0..3 {
            map.record(fine.clone(), mem_sample(50_000));
        }
        assert_eq!(
            map.down_lowering_reserve_kb(&fine, &coarse, 1),
            Some(50_000),
            "a live (never-loaded) key is always DOWN-trusted regardless of max_age — \
             only LOADED keys carry stale-OOM risk"
        );
    }

    /// A corrupt entry (window LONGER than WINDOW_SIZE) is SKIPPED on load, never panics.
    #[test]
    fn load_entries_skips_corrupt_entry() {
        let mut dst = ProfileMap::new(NonZeroUsize::new(16).unwrap(), 3, 200);
        let bad = ProfileEntrySnapshot {
            instance_name: "main".to_string(),
            target_id: "//a".to_string(),
            action_mnemonic: "M".to_string(),
            memory_samples: vec![0u64; WINDOW_SIZE + 1], // WRONG length (> WINDOW_SIZE)
            cpu_samples: vec![0u64; 5],
            disk_samples: vec![0u64; 5],
            net_samples: vec![0u64; 5],
            sample_count: 5,
        };
        let loaded = dst.load_entries(vec![bad], 100);
        assert_eq!(
            loaded, 0,
            "an entry with an over-long window must be SKIPPED (corrupt), not loaded \
             and not a panic"
        );
    }
}
