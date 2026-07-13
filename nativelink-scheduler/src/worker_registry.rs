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

use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use async_lock::RwLock;
use nativelink_util::action_messages::WorkerId;
use tracing::{debug, trace, warn};

/// SIGKILL aggregation: emit a worker-aggregate warn after this many SIGKILLs
/// within `SIGKILL_AGGREGATE_WINDOW` for a single stable worker identity.
/// 5/10min covers the 2026-05-11 incident (10/worker in 27min on 4-5 workers,
/// per `.claude/audits/384-exec-log-2026-05-10/server-worker-oom-detection.md`).
pub const SIGKILL_AGGREGATE_THRESHOLD: usize = 5;

/// Sliding window for the SIGKILL aggregate counter.
pub const SIGKILL_AGGREGATE_WINDOW: Duration = Duration::from_secs(600);

/// Cooldown between consecutive aggregate warns for the same stable identity.
/// Prevents the warn from re-firing on every SIGKILL once the threshold is
/// already breached.
pub const SIGKILL_AGGREGATE_COOLDOWN: Duration = Duration::from_secs(300);

/// Fleet-wide SIGKILL aggregation: emit a fleet-aggregate warn after this many
/// SIGKILLs across ALL workers within `FLEET_SIGKILL_WINDOW`. Calibrated for
/// the 2026-05-11 incident shape (41 SIGKILLs in 27 min across 4-5 workers):
/// at ~1.5 SIGKILL/min fleet-wide, threshold=15 fires at minute ~10 — earlier
/// than the per-worker aggregate (`SIGKILL_AGGREGATE_THRESHOLD=5`) requires
/// any single worker to accumulate, and catches diffuse jetsam storms where
/// no single worker crosses 5/10min but the fleet collectively does.
pub const FLEET_SIGKILL_THRESHOLD: usize = 15;

/// Sliding window for the fleet-wide SIGKILL aggregate counter.
pub const FLEET_SIGKILL_WINDOW: Duration = Duration::from_secs(600);

/// Cooldown between consecutive fleet-aggregate warns. Longer than the
/// per-worker cooldown (300s) because the fleet-wide signal is rarer and
/// should not re-fire mid-storm — operator has already been notified;
/// re-firing every 5 min during a 30 min incident is noise.
pub const FLEET_SIGKILL_COOLDOWN: Duration = Duration::from_secs(600);

/// Per-stable-identity SIGKILL aggregate state.
#[derive(Debug, Default)]
struct SigkillAggregate {
    // CAPPED AT SIGKILL_AGGREGATE_THRESHOLD + a-handful: front-evicted on every
    // record_sigkill, so the deque never grows beyond ~threshold for a worker
    // firing at steady-state. Healthy workers stay near 0. Bounded.
    timestamps: VecDeque<Instant>,
    last_warn_at: Option<Instant>,
}

/// Fleet-wide SIGKILL aggregate state. One instance per `WorkerRegistry`
/// (single counter across all workers). Front-evicted on every
/// `record_fleet_sigkill` call, so the deque is bounded by the SIGKILL rate
/// over `FLEET_SIGKILL_WINDOW` — in practice a-few-times-threshold during a
/// storm, zero in steady-state.
#[derive(Debug, Default)]
struct FleetSigkillAggregate {
    // CAPPED AT ~FLEET_SIGKILL_THRESHOLD * worst-case-rate: front-evicted on
    // every record_fleet_sigkill. Healthy steady-state: empty. Storm
    // worst-case observed (2026-05-11): 41 events in 27 min ≈ 1.5/min, so the
    // deque holds at most ~15-20 elements within the 10 min window. Bounded.
    timestamps: VecDeque<Instant>,
    last_warn_at: Option<Instant>,
}

/// In-memory worker registry that tracks worker liveness and per-worker
/// SIGKILL aggregate state.
#[derive(Debug)]
pub struct WorkerRegistry {
    workers: RwLock<HashMap<WorkerId, SystemTime>>,
    /// `worker_id` → stable identity (`cas_endpoint`) for SIGKILL aggregation.
    /// `worker_id` is regenerated server-side on every worker reconnect
    /// (`worker_api_server.rs:498-502`); `cas_endpoint` is the worker's own
    /// stable identity (same precedent as the BIS resend buffer keying in
    /// `api_worker_scheduler.rs:267-282`). When `cas_endpoint` is empty
    /// (e.g. tests), we fall back to the `worker_id` string so the
    /// aggregator still functions on a single-connect lifetime.
    worker_id_to_stable_id: RwLock<HashMap<WorkerId, String>>,
    /// `stable_id` → SIGKILL aggregate state. Persists across worker
    /// reconnects so a flapping/jetsam-pressured worker accumulates events
    /// against its `cas_endpoint`, not its (regenerated) `worker_id`.
    sigkill_state: RwLock<HashMap<String, SigkillAggregate>>,
    /// Fleet-wide SIGKILL aggregate. Single counter across ALL workers —
    /// catches diffuse jetsam storms (e.g. 2026-05-11: 41 SIGKILLs in 27 min
    /// across 4-5 workers, none crossing the per-worker threshold of 5/10min
    /// in lockstep) that the per-worker aggregator would only fire on
    /// 4-5 times — once per affected worker, scattered across the storm
    /// minutes — whereas the fleet-wide signal fires ONCE at minute ~10
    /// with a clear "fleet-wide jetsam storm" diagnosis.
    fleet_sigkill_state: RwLock<FleetSigkillAggregate>,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerRegistry {
    /// Creates a new worker registry.
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
            worker_id_to_stable_id: RwLock::new(HashMap::new()),
            sigkill_state: RwLock::new(HashMap::new()),
            fleet_sigkill_state: RwLock::new(FleetSigkillAggregate::default()),
        }
    }

    /// Updates the heartbeat timestamp for a worker.
    pub async fn update_worker_heartbeat(&self, worker_id: &WorkerId, now: SystemTime) {
        let mut workers = self.workers.write().await;
        workers.insert(worker_id.clone(), now);
        trace!(?worker_id, now = %humantime::format_rfc3339(now), "FLOW: Worker heartbeat updated in registry");
    }

    pub async fn register_worker(&self, worker_id: &WorkerId, now: SystemTime) {
        self.register_worker_with_endpoint(worker_id, "", now).await;
    }

    /// Registers a worker and records its `cas_endpoint` (stable identity) so
    /// `record_sigkill` can aggregate across reconnects. Empty `cas_endpoint`
    /// falls back to keying on `worker_id` (single-connect-lifetime
    /// aggregation; suitable for tests).
    pub async fn register_worker_with_endpoint(
        &self,
        worker_id: &WorkerId,
        cas_endpoint: &str,
        now: SystemTime,
    ) {
        let mut workers = self.workers.write().await;
        workers.insert(worker_id.clone(), now);
        drop(workers);
        let stable_id = if cas_endpoint.is_empty() {
            worker_id.to_string()
        } else {
            cas_endpoint.to_string()
        };
        let mut id_map = self.worker_id_to_stable_id.write().await;
        id_map.insert(worker_id.clone(), stable_id);
        debug!(?worker_id, %cas_endpoint, "FLOW: Worker registered in registry");
    }

    pub async fn remove_worker(&self, worker_id: &WorkerId) {
        let mut workers = self.workers.write().await;
        workers.remove(worker_id);
        drop(workers);
        // Drop the worker_id->stable_id mapping but KEEP the per-stable-id
        // sigkill_state. A reconnect re-registers a new worker_id under the
        // same stable identity (cas_endpoint), and we want the aggregate
        // counter to persist across that boundary. (Stale entries for
        // permanently-departed workers age out via the sliding window — the
        // deque empties as timestamps fall out of `SIGKILL_AGGREGATE_WINDOW`,
        // and `record_sigkill` never inserts when no event is observed.)
        let mut id_map = self.worker_id_to_stable_id.write().await;
        id_map.remove(worker_id);
        debug!(?worker_id, "FLOW: Worker removed from registry");
    }

    pub async fn is_worker_alive(
        &self,
        worker_id: &WorkerId,
        timeout: Duration,
        now: SystemTime,
    ) -> bool {
        let workers = self.workers.read().await;

        if let Some(last_seen) = workers.get(worker_id)
            && let Some(deadline) = last_seen.checked_add(timeout)
        {
            let is_alive = deadline > now;
            trace!(
                ?worker_id,
                last_seen = %humantime::format_rfc3339(*last_seen),
                ?timeout,
                is_alive,
                "FLOW: Worker liveness check"
            );
            return is_alive;
        }

        trace!(?worker_id, "FLOW: Worker not found or timed out");
        false
    }

    pub async fn get_worker_last_seen(&self, worker_id: &WorkerId) -> Option<SystemTime> {
        let workers = self.workers.read().await;
        workers.get(worker_id).copied()
    }

    /// Record a SIGKILL event for the given `worker_id`'s stable identity
    /// (`cas_endpoint` if registered with one, else the `worker_id` string).
    /// Drops timestamps older than `SIGKILL_AGGREGATE_WINDOW` from the front
    /// of the per-identity deque, appends `now`, and emits a single
    /// aggregate `warn!` if the deque length crosses
    /// `SIGKILL_AGGREGATE_THRESHOLD` and the last warn (if any) was more
    /// than `SIGKILL_AGGREGATE_COOLDOWN` ago. Returns the post-prune deque
    /// length (visible to callers / tests).
    pub async fn record_sigkill(&self, worker_id: &WorkerId, now: Instant) -> usize {
        // Resolve stable identity. If the worker has not registered (or
        // already departed and the mapping was cleared), fall back to the
        // worker_id string — better partial-credit than dropping the event.
        let stable_id = {
            let id_map = self.worker_id_to_stable_id.read().await;
            id_map
                .get(worker_id)
                .cloned()
                .unwrap_or_else(|| worker_id.to_string())
        };

        let mut state = self.sigkill_state.write().await;
        let entry = state.entry(stable_id.clone()).or_default();
        // Front-evict expired timestamps.
        let cutoff = now.checked_sub(SIGKILL_AGGREGATE_WINDOW);
        if let Some(cutoff) = cutoff {
            while let Some(&front) = entry.timestamps.front() {
                if front < cutoff {
                    entry.timestamps.pop_front();
                } else {
                    break;
                }
            }
        }
        entry.timestamps.push_back(now);
        let count = entry.timestamps.len();

        // Threshold + cooldown check. Emit once per cooldown window.
        if count >= SIGKILL_AGGREGATE_THRESHOLD {
            let cooled = match entry.last_warn_at {
                None => true,
                Some(prev) => now.duration_since(prev) >= SIGKILL_AGGREGATE_COOLDOWN,
            };
            if cooled {
                entry.last_warn_at = Some(now);
                warn!(
                    %stable_id,
                    ?worker_id,
                    recent_sigkills = count,
                    window_secs = SIGKILL_AGGREGATE_WINDOW.as_secs(),
                    threshold = SIGKILL_AGGREGATE_THRESHOLD,
                    "worker accumulating SIGKILLs in window — likely memory-pressured \
                     (jetsam/OOM); investigate worker host"
                );
            }
        }
        count
    }

    /// Record a SIGKILL event against the fleet-wide aggregate (one counter,
    /// no per-worker keying). Drops timestamps older than
    /// `FLEET_SIGKILL_WINDOW` from the front of the deque, appends `now`, and
    /// emits a single fleet-aggregate `warn!` if the deque length crosses
    /// `FLEET_SIGKILL_THRESHOLD` and the last fleet-warn (if any) was more
    /// than `FLEET_SIGKILL_COOLDOWN` ago. Returns the post-prune deque length
    /// (visible to callers / tests).
    ///
    /// Companion to `record_sigkill`: per-worker fires at 5/10min for ANY
    /// SINGLE worker; fleet-wide fires at 15/10min ACROSS ALL workers. They
    /// catch different incident shapes — per-worker for hot-spot OOM
    /// (one machine with a leak), fleet-wide for diffuse jetsam storms
    /// (system-wide memory pressure, kernel pageout, host upgrade rolling
    /// through the fleet).
    pub async fn record_fleet_sigkill(&self, now: Instant) -> usize {
        let mut state = self.fleet_sigkill_state.write().await;
        // Front-evict expired timestamps.
        let cutoff = now.checked_sub(FLEET_SIGKILL_WINDOW);
        if let Some(cutoff) = cutoff {
            while let Some(&front) = state.timestamps.front() {
                if front < cutoff {
                    state.timestamps.pop_front();
                } else {
                    break;
                }
            }
        }
        state.timestamps.push_back(now);
        let count = state.timestamps.len();

        // Threshold + cooldown check. Emit once per cooldown window.
        if count >= FLEET_SIGKILL_THRESHOLD {
            let cooled = match state.last_warn_at {
                None => true,
                Some(prev) => now.duration_since(prev) >= FLEET_SIGKILL_COOLDOWN,
            };
            if cooled {
                state.last_warn_at = Some(now);
                warn!(
                    recent_sigkills = count,
                    window_secs = FLEET_SIGKILL_WINDOW.as_secs(),
                    threshold = FLEET_SIGKILL_THRESHOLD,
                    "fleet-wide jetsam storm: SIGKILLs across fleet in last window — \
                     check workers' vm_stat for memory pressure, look for memory leaks \
                     in long-running actions, verify action mnemonics carry memory_kb hints"
                );
            }
        }
        count
    }
}

pub type SharedWorkerRegistry = Arc<WorkerRegistry>;

#[cfg(test)]
mod tests {
    use nativelink_macro::nativelink_test;

    use super::*;

    /// Bespoke marker emitted by the SIGKILL aggregate `warn!`. Tests count
    /// occurrences via `tracing_test`'s injected log buffer.
    const SIGKILL_WARN_MARKER: &str = "worker accumulating SIGKILLs in window";

    #[nativelink_test]
    async fn test_worker_heartbeat() {
        let registry = WorkerRegistry::new();
        let worker_id = WorkerId::from(String::from("test"));
        let now = SystemTime::now();

        // Worker not registered yet
        assert!(
            !registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );

        // Register worker
        registry.register_worker(&worker_id, now).await;
        assert!(
            registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );

        // Check with expired timeout
        let future = now.checked_add(Duration::from_secs(10)).unwrap();
        assert!(
            !registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), future)
                .await
        );

        // Update heartbeat
        registry.update_worker_heartbeat(&worker_id, future).await;
        assert!(
            registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), future)
                .await
        );
    }

    #[nativelink_test]
    async fn test_remove_worker() {
        let registry = WorkerRegistry::new();
        let worker_id = WorkerId::from(String::from("test-worker"));
        let now = SystemTime::now();

        registry.register_worker(&worker_id, now).await;
        assert!(
            registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );

        registry.remove_worker(&worker_id).await;
        assert!(
            !registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );
    }

    /// Counts occurrences of `SIGKILL_WARN_MARKER` in the `tracing_test`
    /// in-memory log buffer for the given scope (the test fn name, which
    /// is the scope `#[traced_test]` uses to filter its global log buffer).
    /// The marker is unique to the SIGKILL aggregate `warn!`; counting
    /// lines that contain it lets us assert exact emit-cardinality (the
    /// fires-once-per-cooldown contract).
    fn count_sigkill_warns(scope: &str) -> usize {
        let observed = std::sync::Mutex::new(0_usize);
        tracing_test::internal::logs_assert(scope, |lines: &[&str]| {
            let n = lines
                .iter()
                .filter(|l| l.contains(SIGKILL_WARN_MARKER))
                .count();
            *observed.lock().unwrap() = n;
            Ok(())
        })
        .expect("logs_assert closure always returns Ok");
        let n = *observed.lock().unwrap();
        n
    }

    /// Drives the SIGKILL aggregate contract end-to-end:
    /// - `THRESHOLD` events in <10 min → one aggregate warn fires
    /// - 6th event immediately after → must NOT re-warn (cooldown holds)
    /// - advance past `SIGKILL_AGGREGATE_COOLDOWN` → next event re-warns
    /// Bespoke failure-mode marker: "worker-aggregate-SIGKILL warn missing
    /// — operator-blind to OOM pattern" — assertion message names the
    /// invariant so a mutation step that comments out the warn-emit
    /// red-fails with exactly that string.
    #[nativelink_test]
    async fn test_sigkill_aggregate_warn_fires_once_then_cools_down() {
        const SCOPE: &str = "test_sigkill_aggregate_warn_fires_once_then_cools_down";
        let registry = WorkerRegistry::new();
        let worker_id = WorkerId::from(String::from("worker-oom"));
        let cas_endpoint = "grpcs://worker-oom.local:50071";
        registry
            .register_worker_with_endpoint(&worker_id, cas_endpoint, SystemTime::now())
            .await;

        // Inject a fixed base instant so we can fast-forward without
        // touching the real clock. `record_sigkill` takes the timestamp
        // explicitly to keep tests independent of wall time
        // (parking-lot/sync-context constraints aside, the registry uses
        // async_lock, so injection is purely for test determinism).
        let t0 = Instant::now();
        let inside_window = Duration::from_secs(1); // any < SIGKILL_AGGREGATE_WINDOW

        // Seed THRESHOLD-1 events; none should warn yet.
        for i in 0..(SIGKILL_AGGREGATE_THRESHOLD - 1) {
            let count = registry
                .record_sigkill(&worker_id, t0 + inside_window * (i as u32))
                .await;
            assert!(
                count < SIGKILL_AGGREGATE_THRESHOLD,
                "pre-threshold count must be < {SIGKILL_AGGREGATE_THRESHOLD}, got {count}"
            );
        }
        assert_eq!(
            count_sigkill_warns(SCOPE),
            0,
            "no aggregate warn before threshold crossed"
        );

        // The Nth event crosses the threshold → exactly one warn.
        let count = registry
            .record_sigkill(
                &worker_id,
                t0 + inside_window * (SIGKILL_AGGREGATE_THRESHOLD as u32),
            )
            .await;
        assert_eq!(count, SIGKILL_AGGREGATE_THRESHOLD);
        assert_eq!(
            count_sigkill_warns(SCOPE),
            1,
            "worker-aggregate-SIGKILL warn missing — operator-blind to OOM pattern",
        );

        // Subsequent event still inside cooldown → MUST NOT re-warn.
        let count = registry
            .record_sigkill(
                &worker_id,
                t0 + inside_window * (SIGKILL_AGGREGATE_THRESHOLD as u32 + 1),
            )
            .await;
        assert!(count > SIGKILL_AGGREGATE_THRESHOLD);
        assert_eq!(
            count_sigkill_warns(SCOPE),
            1,
            "cooldown violated — aggregate warn re-fired inside cooldown window",
        );

        // Jump past cooldown → the next event re-warns.
        let post_cooldown = t0
            + inside_window * (SIGKILL_AGGREGATE_THRESHOLD as u32 + 1)
            + SIGKILL_AGGREGATE_COOLDOWN
            + Duration::from_secs(1);
        let _ = registry.record_sigkill(&worker_id, post_cooldown).await;
        assert_eq!(
            count_sigkill_warns(SCOPE),
            2,
            "post-cooldown re-warn missing — aggregate must re-arm after cooldown",
        );
    }

    /// Sliding-window contract: events older than `SIGKILL_AGGREGATE_WINDOW`
    /// must evict from the front of the deque, so the threshold is measured
    /// against the *recent* window — not lifetime cumulative.
    #[nativelink_test]
    async fn test_sigkill_aggregate_window_evicts_old_events() {
        let registry = WorkerRegistry::new();
        let worker_id = WorkerId::from(String::from("worker-slow-leak"));
        registry
            .register_worker_with_endpoint(&worker_id, "grpcs://slow.local:50071", SystemTime::now())
            .await;

        let t0 = Instant::now();
        // Two events at t0.
        let c1 = registry.record_sigkill(&worker_id, t0).await;
        let c2 = registry
            .record_sigkill(&worker_id, t0 + Duration::from_secs(1))
            .await;
        assert_eq!(c1, 1);
        assert_eq!(c2, 2);

        // Jump past the window; the next event should see ONLY itself
        // (the two earlier events evict from the front).
        let after_window = t0 + SIGKILL_AGGREGATE_WINDOW + Duration::from_secs(2);
        let c3 = registry.record_sigkill(&worker_id, after_window).await;
        assert_eq!(
            c3, 1,
            "sliding-window violated — old events not front-evicted (count expected 1, got {c3})"
        );
    }

    /// Stable-identity contract: a worker that re-registers with a fresh
    /// `WorkerId` but the SAME `cas_endpoint` MUST accumulate SIGKILLs
    /// against the shared `cas_endpoint`, not against the regenerated
    /// `WorkerId`. This is the bug-of-record from 2026-05-11 if violated:
    /// each reconnect would zero the counter and the aggregate warn would
    /// never fire on a flap-and-OOM worker.
    #[nativelink_test]
    async fn test_sigkill_aggregate_persists_across_worker_id_regeneration() {
        const SCOPE: &str = "test_sigkill_aggregate_persists_across_worker_id_regeneration";
        let registry = WorkerRegistry::new();
        let cas_endpoint = "grpcs://flap.local:50071";
        let t0 = Instant::now();
        // Two pre-reconnect events.
        let wid1 = WorkerId::from(String::from("worker-pre-reconnect"));
        registry
            .register_worker_with_endpoint(&wid1, cas_endpoint, SystemTime::now())
            .await;
        let _ = registry.record_sigkill(&wid1, t0).await;
        let _ = registry
            .record_sigkill(&wid1, t0 + Duration::from_secs(1))
            .await;
        registry.remove_worker(&wid1).await;

        // Reconnect with a regenerated WorkerId but identical cas_endpoint.
        let wid2 = WorkerId::from(String::from("worker-post-reconnect"));
        registry
            .register_worker_with_endpoint(&wid2, cas_endpoint, SystemTime::now())
            .await;
        // THRESHOLD-2 more events under the new WorkerId; total reaches
        // THRESHOLD because the per-endpoint deque persisted across the
        // remove_worker call (drop semantics covered in remove_worker doc).
        for i in 0..(SIGKILL_AGGREGATE_THRESHOLD - 2) {
            let _ = registry
                .record_sigkill(&wid2, t0 + Duration::from_secs(2 + i as u64))
                .await;
        }
        assert_eq!(
            count_sigkill_warns(SCOPE),
            1,
            "stable-identity violated — aggregate did not persist across worker_id regeneration",
        );
    }

    /// Bespoke marker emitted by the fleet-wide SIGKILL aggregate `warn!`.
    /// Distinct from the per-worker marker — same `count_*_warns` strategy.
    const FLEET_SIGKILL_WARN_MARKER: &str = "fleet-wide jetsam storm";

    fn count_fleet_sigkill_warns(scope: &str) -> usize {
        let observed = std::sync::Mutex::new(0_usize);
        tracing_test::internal::logs_assert(scope, |lines: &[&str]| {
            let n = lines
                .iter()
                .filter(|l| l.contains(FLEET_SIGKILL_WARN_MARKER))
                .count();
            *observed.lock().unwrap() = n;
            Ok(())
        })
        .expect("logs_assert closure always returns Ok");
        let n = *observed.lock().unwrap();
        n
    }

    /// Fleet-wide aggregate contract: THRESHOLD events across the fleet
    /// (here all on one worker, but the aggregate keys are global — the
    /// `record_fleet_sigkill` API takes only `now`, no worker_id) in
    /// <FLEET_SIGKILL_WINDOW → exactly one fleet-aggregate warn fires.
    /// Bespoke failure-mode marker: "fleet-wide jetsam storm warn missing —
    /// operator-blind to global pressure" — mutation step that comments
    /// out the warn-emit red-fails with exactly that string.
    #[nativelink_test]
    async fn test_fleet_sigkill_warn_fires_at_threshold() {
        const SCOPE: &str = "test_fleet_sigkill_warn_fires_at_threshold";
        let registry = WorkerRegistry::new();

        let t0 = Instant::now();
        let inside_window = Duration::from_secs(1);

        // Seed THRESHOLD-1 events; none should fleet-warn yet.
        for i in 0..(FLEET_SIGKILL_THRESHOLD - 1) {
            let count = registry
                .record_fleet_sigkill(t0 + inside_window * (i as u32))
                .await;
            assert!(
                count < FLEET_SIGKILL_THRESHOLD,
                "pre-threshold count must be < {FLEET_SIGKILL_THRESHOLD}, got {count}"
            );
        }
        assert_eq!(
            count_fleet_sigkill_warns(SCOPE),
            0,
            "no fleet-aggregate warn before threshold crossed"
        );

        // The Nth event crosses the threshold → exactly one fleet-warn.
        let count = registry
            .record_fleet_sigkill(t0 + inside_window * (FLEET_SIGKILL_THRESHOLD as u32))
            .await;
        assert_eq!(count, FLEET_SIGKILL_THRESHOLD);
        assert_eq!(
            count_fleet_sigkill_warns(SCOPE),
            1,
            "fleet-wide jetsam storm warn missing — operator-blind to global pressure",
        );
    }

    /// Sliding-window contract for the fleet aggregate: events older than
    /// `FLEET_SIGKILL_WINDOW` must evict from the front, so the threshold
    /// is measured against the *recent* window — not lifetime cumulative.
    #[nativelink_test]
    async fn test_fleet_sigkill_window_evicts_stale() {
        const SCOPE: &str = "test_fleet_sigkill_window_evicts_stale";
        let registry = WorkerRegistry::new();

        let t0 = Instant::now();
        // Seed THRESHOLD events spaced just inside the window (so all live).
        for i in 0..FLEET_SIGKILL_THRESHOLD {
            let _ = registry
                .record_fleet_sigkill(t0 + Duration::from_secs(i as u64))
                .await;
        }
        // Sanity: that should have fired exactly one fleet-warn.
        assert_eq!(
            count_fleet_sigkill_warns(SCOPE),
            1,
            "setup: threshold reached → one fleet-warn expected before window-expiry stage",
        );

        // Now advance past the window AND past the cooldown. All earlier
        // events must front-evict; the next record_fleet_sigkill should see
        // only itself (count == 1) and NOT emit a new warn.
        let past_window = t0
            + FLEET_SIGKILL_WINDOW
            + FLEET_SIGKILL_COOLDOWN
            + Duration::from_secs(5);
        let count = registry.record_fleet_sigkill(past_window).await;
        assert_eq!(
            count, 1,
            "sliding-window violated — stale fleet events not front-evicted (count expected 1, got {count})",
        );
        // No additional warn — the single fresh event is far below threshold.
        assert_eq!(
            count_fleet_sigkill_warns(SCOPE),
            1,
            "post-window: solo event below threshold must not emit fleet-warn",
        );
    }

    /// Cooldown contract for the fleet aggregate: after firing once,
    /// subsequent threshold-crossing events INSIDE the cooldown must NOT
    /// re-fire; after the cooldown elapses, the next threshold-crossing
    /// event MUST re-fire (re-arm).
    #[nativelink_test]
    async fn test_fleet_sigkill_cooldown_then_rearm() {
        const SCOPE: &str = "test_fleet_sigkill_cooldown_then_rearm";
        let registry = WorkerRegistry::new();

        let t0 = Instant::now();
        let step = Duration::from_secs(1);

        // First storm: THRESHOLD events in <window → 1 warn.
        for i in 0..FLEET_SIGKILL_THRESHOLD {
            let _ = registry.record_fleet_sigkill(t0 + step * (i as u32)).await;
        }
        assert_eq!(
            count_fleet_sigkill_warns(SCOPE),
            1,
            "first storm: fleet-warn must fire at threshold",
        );

        // Another event still inside cooldown → must NOT re-warn.
        let inside_cd = t0 + step * (FLEET_SIGKILL_THRESHOLD as u32 + 1);
        let _ = registry.record_fleet_sigkill(inside_cd).await;
        assert_eq!(
            count_fleet_sigkill_warns(SCOPE),
            1,
            "cooldown violated — fleet-warn re-fired inside cooldown window",
        );

        // Advance past cooldown AND past the window so the prior deque is
        // empty; then seed a fresh threshold-worth of events. Must re-warn.
        let post_cd = t0
            + FLEET_SIGKILL_WINDOW
            + FLEET_SIGKILL_COOLDOWN
            + Duration::from_secs(5);
        for i in 0..FLEET_SIGKILL_THRESHOLD {
            let _ = registry
                .record_fleet_sigkill(post_cd + step * (i as u32))
                .await;
        }
        assert_eq!(
            count_fleet_sigkill_warns(SCOPE),
            2,
            "post-cooldown re-warn missing — fleet-aggregate must re-arm after cooldown",
        );
    }
}
