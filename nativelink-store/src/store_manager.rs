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

use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex as AsyncMutex;
use nativelink_config::cas_server::StoreConfig;
use nativelink_error::{Error, ResultExt};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_util::health_utils::HealthRegistryBuilder;
use nativelink_util::store_trait::Store;
use parking_lot::RwLock;
use tracing::{error, info, warn};

use crate::default_store_factory::store_factory;

/// Period for the progress log emitted while the shutdown drain is waiting.
/// Operators see "still draining N writes after Ms" so they know the
/// process hasn't wedged silently between SIGTERM and process::exit.
const FLUSH_PROGRESS_LOG_INTERVAL: core::time::Duration =
    core::time::Duration::from_secs(2);

/// (#sigkill-gap) Stall window for the shutdown-drain livelock/stall detector.
/// If `remaining > 0` AND ZERO net progress is observed across this whole
/// window, the detector escalates to `error!` + sets the `shutdown_stalled`
/// gauge so an operator can make an INFORMED force-kill decision. Multiple of
/// `FLUSH_PROGRESS_LOG_INTERVAL` so the window spans several progress samples.
/// OBSERVABILITY ONLY — nothing in this module ever auto-kills off this signal
/// (hard constraint: data loss is an operator decision, never automatic).
const SHUTDOWN_STALL_WINDOW: core::time::Duration = core::time::Duration::from_secs(20);

/// (#sigkill-gap) Operator-facing per-size-class size-class label for the
/// shutdown drain instrumentation. The two production arms of the CAS
/// `SizePartitioningStore(16 KiB)` have DIFFERENT durable backends with
/// DIFFERENT failure modes, so a single total would hide "/srv/bulk stalled while
/// Redis drained". We bucket each registered `FastSlowStore` by name:
///   - `SmallRedis`  — the `SMALL_CAS_CACHED` ≤16 KiB arm (fast Memory → Redis).
///   - `LargeTank`   — the `cas_FAST_SLOW_STORE` >16 KiB arm (fast Memory → /srv/bulk).
///   - `Other`       — any other registered FastSlowStore (e.g. AC, or a future
///                     store); reported under its own name so nothing is hidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainClass {
    SmallRedis,
    LargeTank,
    Other,
}

impl DrainClass {
    /// Classify a registered store by NAME. Heuristic, but the only signal we
    /// have at the StoreManager layer (the size-routing lives in the
    /// `SizePartitioningStore` below us). Conservative: an unrecognized name
    /// falls into `Other` and is still reported (never silently dropped).
    fn classify(name: &str) -> Self {
        let lower = name.to_ascii_lowercase();
        if lower.contains("small") {
            Self::SmallRedis
        } else if lower.contains("fast_slow") || lower.contains("cas") {
            Self::LargeTank
        } else {
            Self::Other
        }
    }
}

/// (#sigkill-gap) OPERATOR-FACING shutdown-drain gauge group. Registered on the
/// `StoreManager` metrics tree (which is a `RootMetricsComponent`) so a
/// dashboard / alert can show "N blobs, M bytes un-flushed, stalled=1" during
/// the `deactivating` window and an operator can force-kill with EYES OPEN.
///
/// **OBSERVABILITY ONLY.** No code reads these to take any automatic action;
/// the force-kill stays a manual operator decision (`systemctl kill -s
/// SIGKILL`). The hard constraint is: never auto-lose data.
///
/// Gauges are updated by the progress poller every `FLUSH_PROGRESS_LOG_INTERVAL`
/// during the shutdown drain; outside shutdown they read zero.
#[derive(Debug, Default, MetricsComponent)]
pub struct ShutdownDrainMetrics {
    /// Remaining not-yet-durable in-memory blobs in the ≤16 KiB → Redis arm.
    #[metric(help = "Shutdown drain: remaining not-yet-durable in-memory blobs \
                     in the small (≤16 KiB → Redis) size class. Non-zero during \
                     `deactivating` = blobs an operator force-kill would lose.")]
    shutdown_remaining_blobs_small_redis: AtomicU64,
    /// Remaining not-yet-durable in-memory blobs in the >16 KiB → /srv/bulk arm.
    #[metric(help = "Shutdown drain: remaining not-yet-durable in-memory blobs \
                     in the large (>16 KiB → /srv/bulk) size class. Non-zero during \
                     `deactivating` = blobs an operator force-kill would lose.")]
    shutdown_remaining_blobs_large_tank: AtomicU64,
    /// Remaining at-risk in-flight bytes in the small arm.
    #[metric(help = "Shutdown drain: remaining at-risk in-flight bytes in the \
                     small (≤16 KiB → Redis) size class.")]
    shutdown_remaining_bytes_small_redis: AtomicU64,
    /// Remaining at-risk in-flight bytes in the large arm.
    #[metric(help = "Shutdown drain: remaining at-risk in-flight bytes in the \
                     large (>16 KiB → /srv/bulk) size class.")]
    shutdown_remaining_bytes_large_tank: AtomicU64,
    /// Current shutdown phase as an integer (0 = not shutting down, 1 = Phase-1
    /// in-flight drain, 2 = Phase-2 fast→slow drain). Lets a dashboard show
    /// which phase the drain is stuck in.
    #[metric(help = "Shutdown drain phase: 0 = not shutting down, 1 = Phase-1 \
                     in-flight drain, 2 = Phase-2 fast→slow drain.")]
    shutdown_phase: AtomicU64,
    /// 1 iff the stall detector currently sees `remaining > 0` with ZERO net
    /// progress over `SHUTDOWN_STALL_WINDOW`; 0 otherwise. Drives an operator
    /// alert. NEVER triggers an automatic kill.
    #[metric(help = "Shutdown drain stalled flag: 1 when remaining > 0 with zero \
                     net drain over the stall window (slow tier likely wedged); \
                     0 otherwise. Operator-facing only — never auto-kills.")]
    shutdown_stalled: AtomicU64,
}

/// (#sigkill-gap) One shutdown-drain progress sample. Threaded between
/// successive `sample_drain_progress` calls so the poller can compute
/// drain-rate deltas and track the no-progress window for the stall detector.
#[derive(Debug, Clone, Copy, Default)]
struct DrainSample {
    remaining_total: u64,
    elapsed_ms: u64,
    /// Accumulated wall-clock with ZERO net drain and remaining > 0. Reset to 0
    /// on any net drain. The stall detector fires once this reaches
    /// `SHUTDOWN_STALL_WINDOW`.
    stalled_for_ms: u64,
}

#[derive(Debug, Default, MetricsComponent)]
pub struct StoreManager {
    #[metric]
    stores: RwLock<HashMap<String, Store>>,
    /// (#sigkill-gap) Operator-facing shutdown-drain gauges (see type doc).
    #[metric(group = "shutdown")]
    shutdown_metrics: ShutdownDrainMetrics,
}

impl StoreManager {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            shutdown_metrics: ShutdownDrainMetrics::default(),
        }
    }

    pub fn add_store(&self, name: &str, store: Store) {
        let mut stores = self.stores.write();
        stores.insert(name.to_string(), store);
    }

    pub fn get_store(&self, name: &str) -> Option<Store> {
        let stores = self.stores.read();
        if let Some(store) = stores.get(name) {
            return Some(store.clone());
        }
        None
    }

    /// (#sigkill-gap) Sample the per-size-class shutdown-drain residue across
    /// `targets`, log a structured progress line (with drain-rate deltas vs the
    /// previous sample + the incoming-solicit escape counter the caller passes
    /// in), update the operator-facing gauges, and run the stall/livelock
    /// detector. Returns the current total remaining blobs (so the caller can
    /// track convergence). OBSERVABILITY ONLY — never auto-acts.
    ///
    /// `prev` carries the previous sample so this can compute deltas and track
    /// the no-progress window; the caller threads the returned `DrainSample`
    /// into the next call.
    fn sample_drain_progress(
        &self,
        phase: u64,
        targets: &[(String, Store, usize)],
        started: tokio::time::Instant,
        prev: &DrainSample,
    ) -> DrainSample {
        use crate::wrapper_walker::{find_fast_slow_via_chain, synthetic_large_key};
        use nativelink_util::store_trait::StoreDriver;

        let mut small_blobs: u64 = 0;
        let mut small_bytes: u64 = 0;
        let mut large_blobs: u64 = 0;
        let mut large_bytes: u64 = 0;
        let mut other_blobs: u64 = 0;
        for (name, store, _) in targets {
            let driver: &dyn StoreDriver = store.inner_store(Some(synthetic_large_key()));
            let Some(fss) = find_fast_slow_via_chain(driver) else {
                continue;
            };
            let blobs = fss.at_risk_count() as u64;
            let bytes = fss.in_flight_slow_write_bytes();
            match DrainClass::classify(name) {
                DrainClass::SmallRedis => {
                    small_blobs += blobs;
                    small_bytes += bytes;
                }
                DrainClass::LargeTank => {
                    large_blobs += blobs;
                    large_bytes += bytes;
                }
                DrainClass::Other => other_blobs += blobs,
            }
        }
        let remaining_total = small_blobs + large_blobs + other_blobs;
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let interval_ms = elapsed_ms.saturating_sub(prev.elapsed_ms).max(1);
        // drained_since_last = prev_remaining - now_remaining (clamped ≥ 0; a
        // negative would mean the residue GREW, which after Phase 0b quiesce
        // should not happen — a growth is itself a quiesce-escape signal).
        let drained_since_last = prev.remaining_total.saturating_sub(remaining_total);
        let grew_since_last = remaining_total.saturating_sub(prev.remaining_total);
        let drain_rate_blobs_per_s = drained_since_last.saturating_mul(1000) / interval_ms;

        // Update operator-facing gauges (observability only).
        self.shutdown_metrics
            .shutdown_phase
            .store(phase, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_blobs_small_redis
            .store(small_blobs, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_blobs_large_tank
            .store(large_blobs, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_bytes_small_redis
            .store(small_bytes, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_bytes_large_tank
            .store(large_bytes, Ordering::Relaxed);

        // No-progress window tracking for the stall detector. We accumulate the
        // elapsed time since the last sample that showed net drain.
        let stalled_for_ms = if drained_since_last == 0 && remaining_total > 0 {
            prev.stalled_for_ms.saturating_add(interval_ms)
        } else {
            0
        };
        let stalled = remaining_total > 0
            && stalled_for_ms >= u64::try_from(SHUTDOWN_STALL_WINDOW.as_millis()).unwrap_or(u64::MAX);
        self.shutdown_metrics
            .shutdown_stalled
            .store(u64::from(stalled), Ordering::Relaxed);

        // Single greppable progress schema (`event = "drain-progress"`).
        warn!(
            event = "drain-progress",
            phase,
            small_redis_remaining_blobs = small_blobs,
            small_redis_remaining_bytes = small_bytes,
            large_tank_remaining_blobs = large_blobs,
            large_tank_remaining_bytes = large_bytes,
            other_remaining_blobs = other_blobs,
            remaining_total,
            drained_since_last,
            grew_since_last,
            drain_rate_blobs_per_s,
            elapsed_ms,
            "flush_slow_writes: shutdown drain progress (per size class)",
        );

        if stalled {
            // STALL / LIVELOCK detector. After Phase 0b quiesce, the
            // worker-solicited intake is OFF, so a flat residue with zero drain
            // means the SLOW TIER is wedged (NOT storm-fed) — exactly the case
            // where an operator force-kill is a legitimate, INFORMED decision.
            // We escalate to `error!` and set the `shutdown_stalled` gauge; we
            // do NOT kill anything (hard constraint: data loss is an operator
            // decision, never automatic).
            error!(
                event = "drain-stalled",
                phase,
                remaining_total,
                small_redis_remaining_blobs = small_blobs,
                small_redis_remaining_bytes = small_bytes,
                large_tank_remaining_blobs = large_blobs,
                large_tank_remaining_bytes = large_bytes,
                stalled_for_ms,
                "flush_slow_writes: shutdown drain STALLED — progress flat with \
                 work remaining; slow tier likely wedged (intake quiesced at \
                 Phase 0b, so NOT storm-fed). Operator decision required: wait, \
                 or force-kill ACCEPTING the listed per-size-class loss. This \
                 process will NOT auto-kill.",
            );
        }

        DrainSample {
            remaining_total,
            elapsed_ms,
            stalled_for_ms,
        }
    }

    /// Flush all in-flight background slow writes across every registered
    /// `FastSlowStore`.
    ///
    /// **Why this exists:** during graceful shutdown the fast tier of
    /// `cas_FAST_SLOW_STORE` is a `MemoryStore` that vanishes with the
    /// process. Action results in Redis (AC) reference those blob digests
    /// the moment a fast-store write succeeds. If we exit before the
    /// fire-and-forget background slow write reaches the `FilesystemStore`,
    /// the AC entry survives but the blob does not — manifesting in
    /// production as Bazel "Lost inputs no longer available remotely"
    /// failures across many crates after a restart.
    ///
    /// **UNBOUNDED (hard constraint — no automatic data loss).** Neither
    /// Phase 1 (in-flight drain) nor Phase 2 (fast→slow drain) has an
    /// abandoning timeout. The `flush_budget` argument bounds ONLY how long the
    /// per-store Phase-1 `Notify`-wait *sleeps between re-checks*; it NEVER
    /// abandons un-flushed data — any digest still in-flight when the Phase-1
    /// budget elapses is a strict SUBSET of the Phase-2 at-risk set
    /// (`in_flight_slow_writes ⊆ in_flight ∪ chunked ∪ failed`), so Phase 2
    /// drains it unbounded. The prior outer `tokio::time::timeout` that returned
    /// `Vec::new()` + logged "some slow writes will be lost" was AUTOMATIC DATA
    /// LOSS and is REMOVED (#sigkill-gap correction 3). Convergence (not a
    /// deadline) is what stops the drain; the per-size-class progress
    /// instrumentation + stall detector make a wedged slow tier visible so an
    /// operator can make an informed force-kill decision. systemd
    /// `TimeoutStopSec=infinity` is required for this to take effect.
    pub async fn flush_slow_writes(&self, flush_budget: core::time::Duration) {
        let timeout = flush_budget;
        use crate::fast_slow_store::FastSlowStore;
        use crate::wrapper_walker::{find_fast_slow_via_chain, synthetic_large_key};
        use nativelink_util::store_trait::StoreDriver;

        // BLOCK-1 fix (#335 follow-up): the previous local walker
        // descended `inner_store(None)`, which terminates at
        // `SizePartitioningStore` (its `inner_store(None)` returns
        // `self`). For the production composition (verified against
        // `prod-server.json5:169-230`, SHA 139a0653: `cas_STORE` IS the
        // `VerifyStore`, which wraps `cas_INNER = ExistenceCacheStore` —
        // the prior version of this comment had Verify/ExistenceCache swapped)
        // (`WorkerProxyStore` → `VerifyStore` → `ExistenceCacheStore(50M)` →
        // `SizePartitioningStore(16384)` → {lower `SMALL_CAS_CACHED` =
        // FSS{Memory→Redis}; upper `cas_FAST_SLOW_STORE` =
        // FSS{Memory→Filesystem}}), the walker
        // returned `None` and `flush_slow_writes` silently logged
        // "no FastSlowStore registered; skipping" on every SIGTERM —
        // defeating the #210 graceful-shutdown fix. Migrate to the
        // canonical `find_fast_slow_via_chain` (shared with the V3
        // self-retry drainer in `nativelink-service`), which passes
        // `synthetic_large_key()` to descend the upper arm correctly.

        let stores: Vec<(String, Store)> = {
            let guard = self.stores.read();
            guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        // Build the (name, FastSlowStore, initial_count) work list up front so
        // the operator log shows the total drain workload before we start
        // waiting. We collect Arc clones via the wrapping `Store` so the
        // FastSlowStore stays alive for the duration of the flush even if
        // some other path drops its handle.
        let mut targets: Vec<(String, Store, usize)> = Vec::new();
        let mut total_pending: usize = 0;
        for (name, store) in stores {
            let initial = {
                let driver: &dyn StoreDriver =
                    store.inner_store(Some(synthetic_large_key()));
                find_fast_slow_via_chain(driver)
                    .map(FastSlowStore::in_flight_slow_write_count)
            };
            if let Some(count) = initial {
                total_pending += count;
                targets.push((name, store, count));
            }
        }

        if targets.is_empty() {
            // No FastSlowStore registered — nothing to flush. Logging at
            // info so operators can confirm shutdown went through this path.
            info!("flush_slow_writes: no FastSlowStore registered; skipping");
            return;
        }

        // (#sigkill-gap correction 3) NO DATA-LOSS arm in Phase 1. The
        // per-store `flush_slow_writes(flush_budget)` waits up to `flush_budget`
        // for its in-flight + chunked maps to drain and then RETURNS its
        // residual count WITHOUT abandoning anything — every digest still
        // in-flight stays in the maps and is a strict SUBSET of the Phase-2
        // at-risk set (`in_flight ⊆ in_flight ∪ chunked ∪ failed`), so Phase 2
        // (unbounded) drains it. The REMOVED piece is the OUTER
        // `tokio::time::timeout(timeout, drain_with_progress)` that on elapse
        // returned `Vec::new()` and logged "some slow writes will be lost" — THAT
        // was the AUTOMATIC DATA-LOSS arm (it discarded the per-store results +
        // mislabeled the safe residue as lost). `flush_budget` is retained ONLY
        // as the per-store in-flight-wait bound (NOT an abandoning deadline);
        // there is no longer any outer wall-clock guard.
        info!(
            stores = targets.len(),
            total_pending,
            event = "phase-enter",
            phase = 1u64,
            timeout_secs = timeout.as_secs(),
            "flush_slow_writes: Phase 1 — draining in-flight slow writes \
             (no abandoning outer timeout; residue handed to unbounded Phase 2)",
        );

        self.shutdown_metrics.shutdown_phase.store(1, Ordering::Relaxed);
        // `tokio::time::Instant` (NOT `std::time::Instant`) so the stall
        // detector's elapsed/no-progress window tracks the SAME clock the
        // progress `interval` ticks on — under `tokio::time::pause()` (tests)
        // both advance together; in production (never paused) it delegates to
        // the real monotonic clock, so behavior is identical.
        let started = tokio::time::Instant::now();

        // Drive every store's flush concurrently. Each per-store flush loops on
        // a Notify until its in-flight maps drain; concurrency lets them overlap.
        let mut joins: Vec<tokio::task::JoinHandle<(String, usize, core::time::Duration)>> =
            Vec::with_capacity(targets.len());
        for (name, store, _) in &targets {
            let name_owned = name.clone();
            let store_clone = store.clone();
            joins.push(tokio::spawn(async move {
                // Re-resolve the FastSlowStore from the cloned wrapper so
                // we do not borrow across the spawn boundary. Pass
                // `synthetic_large_key()` so any inner
                // `SizePartitioningStore` descends into its upper arm
                // (where production's `FastSlowStore` lives).
                let driver: &dyn StoreDriver =
                    store_clone.inner_store(Some(synthetic_large_key()));
                let Some(fss) = find_fast_slow_via_chain(driver) else {
                    return (name_owned, 0, core::time::Duration::ZERO);
                };
                let store_started = std::time::Instant::now();
                let remaining = fss.flush_slow_writes(timeout).await;
                (name_owned, remaining, store_started.elapsed())
            }));
        }

        let drain_all = async move {
            let mut results: Vec<(String, usize, core::time::Duration)> =
                Vec::with_capacity(joins.len());
            for join in joins {
                match join.await {
                    Ok(tuple) => results.push(tuple),
                    Err(e) => warn!(error = ?e, "flush_slow_writes: drain task panicked"),
                }
            }
            results
        };

        let mut progress_ticker = tokio::time::interval(FLUSH_PROGRESS_LOG_INTERVAL);
        progress_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately; consume it so we don't double-log
        // alongside the "Phase 1 enter" line above.
        progress_ticker.tick().await;

        // No outer `tokio::time::timeout`: the prior outer guard returned
        // `Vec::new()` + logged "some slow writes will be lost" on elapse =
        // AUTOMATIC DATA LOSS, which violates the hard constraint. Removed
        // (#sigkill-gap correction 3). Phase-1 now runs to its fixed point; the
        // per-size-class progress poller + stall detector make a wedged tier
        // visible for an INFORMED operator decision.
        let results = {
            tokio::pin!(drain_all);
            let mut sample = DrainSample::default();
            loop {
                tokio::select! {
                    res = &mut drain_all => break res,
                    _ = progress_ticker.tick() => {
                        sample = self.sample_drain_progress(1, &targets, started, &sample);
                    }
                }
            }
        };

        let total_remaining: usize = results.iter().map(|(_, r, _)| *r).sum();
        let elapsed_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

        for (name, remaining, dur) in &results {
            let store_ms =
                u64::try_from(dur.as_millis()).unwrap_or(u64::MAX);
            if *remaining > 0 {
                warn!(
                    store = %name,
                    remaining,
                    store_ms,
                    "flush_slow_writes: store did not fully drain",
                );
            } else {
                info!(
                    store = %name,
                    store_ms,
                    "flush_slow_writes: store drained",
                );
            }
        }

        if total_remaining > 0 {
            // Loud, single-line summary: this is the line ops will grep for
            // when investigating "Lost inputs" reports after a restart.
            warn!(
                total_pending,
                total_remaining,
                elapsed_ms,
                "flush_slow_writes: COMPLETED WITH UNFLUSHED WRITES — \
                 AC entries may now reference blobs that are not durable",
            );
        } else {
            info!(
                total_pending,
                elapsed_ms,
                "flush_slow_writes: all background slow writes drained",
            );
        }

        // Phase 2 (#210): drain MemoryStore-only blobs that have no
        // in-flight write entry. Without this step, blobs read from the
        // slow tier and back-populated into the fast MemoryStore — or
        // blobs whose in-flight write was already removed (failed,
        // watchdog-marked, etc.) — vanish on exit because the
        // MemoryStore dies with the process. #206 observed 9904 of 66174
        // worker-reported blobs missing for 6+ hours after restart, with
        // 4 of 5 sampled small-blob digests missing from Redis.
        //
        // UNBOUNDED (operator directive 2026-06-23): Phase 2 runs to
        // COMPLETION with NO deadline. The prior bounded version lost
        // 827,204 SMALL_CAS_CACHED blobs at the 2026-06-23 17:38 restart
        // when its 30 s budget fired mid-drain (`flushed=0
        // deadline_exceeded=827204`). Passing `None` to
        // `flush_fast_to_slow_at_shutdown` makes each store drain every
        // memory-only blob to its slow tier however long it takes; the
        // per-store flush logs forward progress so a long drain is visibly
        // not a wedge. Trade-off: a genuinely wedged slow tier now blocks
        // SIGTERM-to-exit indefinitely (see #210 commit risk note). This is
        // the explicitly accepted cost of never again losing CAS blobs on
        // restart. systemd's `TimeoutStopSec` must be `infinity` for this to
        // take effect (otherwise systemd SIGKILLs first).
        info!(
            stores = targets.len(),
            event = "phase-enter",
            phase = 2u64,
            "flush_slow_writes: Phase 2 — flushing MemoryStore-only blobs to slow tier (unbounded)",
        );
        self.shutdown_metrics.shutdown_phase.store(2, Ordering::Relaxed);

        // `tokio::time::Instant` for the same clock-consistency reason as Phase 1.
        let phase_2_started = tokio::time::Instant::now();
        let mut phase_2_joins: Vec<
            tokio::task::JoinHandle<(String, usize, core::time::Duration)>,
        > = Vec::with_capacity(targets.len());
        for (name, store, _) in &targets {
            let name_owned = name.clone();
            let store_clone = store.clone();
            phase_2_joins.push(tokio::spawn(async move {
                let driver: &dyn StoreDriver =
                    store_clone.inner_store(Some(synthetic_large_key()));
                let Some(fss) = find_fast_slow_via_chain(driver) else {
                    return (name_owned, 0, core::time::Duration::ZERO);
                };
                let store_started = std::time::Instant::now();
                // F5 (#F5): FIRST spill the C2* subset (mirror ∩ failed) to
                // LOCAL DISK so a restart is non-lossy for RAM-only sole-copy
                // mirror blobs. This MUST run before the C1→server flush
                // below: the spill is the only NEW restart-loss path (C1 is
                // already disk-durable), and on a degraded connection the
                // server-bound flush fails harmlessly. The spill emits the
                // terminal "shutdown mirror-spill complete" line the deploy
                // restart sequence gates on. On instances with no mirror map
                // the C2* set is empty and this is a cheap no-op.
                let spill_failed = fss.spill_mirror_to_disk_at_shutdown().await;
                if spill_failed > 0 {
                    warn!(
                        store = %name_owned,
                        spill_failed,
                        "flush_slow_writes: Phase 2 mirror-spill left C2* blobs unspilled \
                         (e.g. ENOSPC) — these RAM-only sole copies will be lost on restart",
                    );
                }
                // Unbounded (R2): drain the not-yet-durable at-risk subset to
                // completion, no deadline. (durability-ack v3 Change A.)
                let unflushed = fss.flush_fast_to_slow_at_shutdown().await;
                (name_owned, unflushed, store_started.elapsed())
            }));
        }

        // No outer wall-clock guard: Phase 2 is unbounded by directive. Await
        // every per-store drain to completion, polling the per-size-class
        // progress + stall detector every `FLUSH_PROGRESS_LOG_INTERVAL` so a
        // wedged slow tier is visible (and the stall gauge fires) for an
        // INFORMED operator decision.
        let phase_2_drain = async move {
            let mut results: Vec<(String, usize, core::time::Duration)> =
                Vec::with_capacity(phase_2_joins.len());
            for join in phase_2_joins {
                match join.await {
                    Ok(tuple) => results.push(tuple),
                    Err(e) => warn!(error = ?e, "flush_slow_writes: Phase 2 task panicked"),
                }
            }
            results
        };
        let mut phase_2_ticker = tokio::time::interval(FLUSH_PROGRESS_LOG_INTERVAL);
        phase_2_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        phase_2_ticker.tick().await;
        let phase_2_results = {
            tokio::pin!(phase_2_drain);
            let mut sample = DrainSample::default();
            loop {
                tokio::select! {
                    res = &mut phase_2_drain => break res,
                    _ = phase_2_ticker.tick() => {
                        sample = self.sample_drain_progress(2, &targets, phase_2_started, &sample);
                    }
                }
            }
        };

        let phase_2_total_unflushed: usize =
            phase_2_results.iter().map(|(_, r, _)| *r).sum();
        let phase_2_elapsed_ms =
            u64::try_from(phase_2_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        for (name, unflushed, dur) in &phase_2_results {
            let store_ms = u64::try_from(dur.as_millis()).unwrap_or(u64::MAX);
            if *unflushed > 0 {
                warn!(
                    store = %name,
                    unflushed,
                    store_ms,
                    "flush_slow_writes: Phase 2 store did not fully drain MemoryStore",
                );
            } else {
                info!(
                    store = %name,
                    store_ms,
                    "flush_slow_writes: Phase 2 store drained MemoryStore",
                );
            }
        }
        if phase_2_total_unflushed > 0 {
            warn!(
                phase_2_total_unflushed,
                phase_2_elapsed_ms,
                "flush_slow_writes: Phase 2 COMPLETED WITH UNFLUSHED MEMORY-ONLY \
                 BLOBS — these will be lost on exit (#210)",
            );
        } else {
            info!(
                phase_2_elapsed_ms,
                "flush_slow_writes: Phase 2 — all MemoryStore-only blobs flushed",
            );
        }

        // (#sigkill-gap) This invocation's drain converged. Reset the
        // operator-facing gauges to 0 so a dashboard does not show a stale
        // residue between the pre-pull flush and the Phase-3.6 post-pull flush.
        // (`shutdown_phase` is set again on the next invocation's Phase-1 enter.)
        self.shutdown_metrics.shutdown_phase.store(0, Ordering::Relaxed);
        self.shutdown_metrics.shutdown_stalled.store(0, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_blobs_small_redis
            .store(0, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_blobs_large_tank
            .store(0, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_bytes_small_redis
            .store(0, Ordering::Relaxed);
        self.shutdown_metrics
            .shutdown_remaining_bytes_large_tank
            .store(0, Ordering::Relaxed);
    }
}

impl StoreManager {
    /// (#sigkill-gap) Test-visibility snapshot of the operator-facing
    /// shutdown-drain gauges. Returns `(phase, stalled, small_blobs,
    /// large_blobs, small_bytes, large_bytes)`. Lets the instrumentation tests
    /// assert the stall detector fired and the per-size-class residue is
    /// reflected WITHOUT scraping the full metrics tree. Reads are `Relaxed`
    /// atomics (the same gauges the metrics tree exposes).
    #[doc(hidden)]
    #[must_use]
    pub fn shutdown_drain_gauges_for_testing(&self) -> (u64, u64, u64, u64, u64, u64) {
        let m = &self.shutdown_metrics;
        (
            m.shutdown_phase.load(Ordering::Relaxed),
            m.shutdown_stalled.load(Ordering::Relaxed),
            m.shutdown_remaining_blobs_small_redis.load(Ordering::Relaxed),
            m.shutdown_remaining_blobs_large_tank.load(Ordering::Relaxed),
            m.shutdown_remaining_bytes_small_redis.load(Ordering::Relaxed),
            m.shutdown_remaining_bytes_large_tank.load(Ordering::Relaxed),
        )
    }
}

impl RootMetricsComponent for StoreManager {}

/// Build a populated `StoreManager` from a list of `StoreConfig` entries.
///
/// This is the single canonical store-stack constructor. Production
/// (`src/bin/nativelink.rs::inner_main`) and the bench harness
/// (`benchmarks/src/composition.rs`) both call this so any drift between
/// "the store stack production ships" and "the store stack a bench
/// measures" becomes impossible by construction.
///
/// `health_registry_builder` is the caller-owned root registry; each
/// store registers a `stores/<name>` sub-builder under it. The Arc is
/// borrowed (not consumed) so the caller can keep registering other
/// components against the same root after this returns.
///
/// The async-recursive `store_factory` walks the spec tree and resolves
/// `RefStore` lookups against the in-progress `StoreManager`, so stores
/// must be processed in declaration order — exactly as the inline code
/// did before extraction.
pub async fn build_store_manager(
    stores: &[StoreConfig],
    health_registry_builder: &Arc<AsyncMutex<HealthRegistryBuilder>>,
) -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    let mut health_registry_lock = health_registry_builder.lock().await;

    for StoreConfig { name, spec } in stores {
        let health_component_name = format!("stores/{name}");
        let mut health_register_store =
            health_registry_lock.sub_builder(&health_component_name);
        let store = store_factory(spec, &store_manager, Some(&mut health_register_store))
            .await
            .err_tip(|| format!("Failed to create store '{name}'"))?;
        store_manager.add_store(name, store);
    }

    // NOTE (v1.6.1 merge): there is intentionally NO post_init / run_post_init
    // pass here. Upstream drives an eager `run_post_init` loop over every store
    // after construction; this fork deliberately does NOT adopt it. Our stores
    // self-initialize inside their own constructor (`store_factory` above), and
    // the lazy `RefStore` architecture resolves cross-store references on first
    // use against the in-progress `StoreManager` — so there is no second,
    // root-driven initialization phase to run. Do not reintroduce an eager
    // post_init loop without re-checking that lazy `RefStore` resolution still
    // covers every cross-store dependency.
    Ok(store_manager)
}
