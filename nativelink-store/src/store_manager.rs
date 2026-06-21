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

use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex as AsyncMutex;
use nativelink_config::cas_server::StoreConfig;
use nativelink_error::{Error, ResultExt};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_util::health_utils::HealthRegistryBuilder;
use nativelink_util::store_trait::Store;
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::default_store_factory::store_factory;

/// Period for the progress log emitted while the shutdown drain is waiting.
/// Operators see "still draining N writes after Ms" so they know the
/// process hasn't wedged silently between SIGTERM and process::exit.
const FLUSH_PROGRESS_LOG_INTERVAL: core::time::Duration =
    core::time::Duration::from_secs(2);

#[derive(Debug, Default, MetricsComponent)]
pub struct StoreManager {
    #[metric]
    stores: RwLock<HashMap<String, Store>>,
}

impl StoreManager {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
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

    /// Flush all in-flight background slow writes across every registered
    /// `FastSlowStore`, returning once they all complete or `timeout` elapses.
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
    /// `timeout` is the **wall-clock budget for the whole drain**, not a
    /// per-store budget — operators reason about a single SIGTERM-to-exit
    /// deadline, not N×deadline. Stores are flushed concurrently so a
    /// single backend that takes its full budget does not starve the others.
    pub async fn flush_slow_writes(&self, timeout: core::time::Duration) {
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

        info!(
            stores = targets.len(),
            total_pending,
            timeout_secs = timeout.as_secs(),
            "flush_slow_writes: starting drain of background slow writes",
        );

        let started = std::time::Instant::now();

        // Drive every store's flush concurrently under a single global
        // deadline. Each per-store flush already loops on a Notify and
        // honors its own timeout, so concurrency just lets them overlap.
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

        // Periodically log progress while we wait. Operators see this in the
        // SIGTERM-to-exit window and know the process is making progress
        // rather than wedged. We poll counters via `targets` (not via
        // joins.len()) because joins finish in any order.
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
        // alongside the "starting drain" line above.
        progress_ticker.tick().await;

        let drain_with_progress = async {
            tokio::pin!(drain_all);
            loop {
                tokio::select! {
                    res = &mut drain_all => return res,
                    _ = progress_ticker.tick() => {
                        let snapshot: usize = targets.iter()
                            .filter_map(|(_, store, _)| {
                                let driver: &dyn StoreDriver =
                                    store.inner_store(Some(synthetic_large_key()));
                                find_fast_slow_via_chain(driver)
                                    .map(FastSlowStore::in_flight_slow_write_count)
                            })
                            .sum();
                        warn!(
                            still_pending = snapshot,
                            elapsed_ms = u64::try_from(started.elapsed().as_millis())
                                .unwrap_or(u64::MAX),
                            "flush_slow_writes: still draining slow writes",
                        );
                    }
                }
            }
        };

        // Outer wall-clock guard: even if a per-store flush misbehaves, we
        // refuse to block shutdown longer than `timeout`. The per-store
        // flush already honors its own timeout, so this is belt-and-braces.
        let results = match tokio::time::timeout(timeout, drain_with_progress).await {
            Ok(r) => r,
            Err(_) => {
                warn!(
                    elapsed_ms = u64::try_from(started.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                    "flush_slow_writes: outer wall-clock timeout fired; \
                     some slow writes will be lost",
                );
                Vec::new()
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
        // 4 of 5 sampled small-blob digests missing from Redis. We use
        // the deadline budget that remains after Phase 1; if Phase 1
        // consumed all of it, Phase 2 still gets a small floor (1
        // second) so it can at least make progress on a near-empty fast
        // tier rather than reporting the entire snapshot as unflushed.
        const PHASE_2_FLOOR: core::time::Duration =
            core::time::Duration::from_secs(1);
        let phase_2_deadline = timeout
            .checked_sub(started.elapsed())
            .unwrap_or(PHASE_2_FLOOR)
            .max(PHASE_2_FLOOR);
        info!(
            phase_2_deadline_secs = phase_2_deadline.as_secs(),
            stores = targets.len(),
            "flush_slow_writes: Phase 2 — flushing MemoryStore-only blobs to slow tier",
        );

        let phase_2_started = std::time::Instant::now();
        let mut phase_2_joins: Vec<
            tokio::task::JoinHandle<(String, usize, core::time::Duration)>,
        > = Vec::with_capacity(targets.len());
        for (name, store, _) in &targets {
            let name_owned = name.clone();
            let store_clone = store.clone();
            let phase_2_per_store = phase_2_deadline;
            phase_2_joins.push(tokio::spawn(async move {
                let driver: &dyn StoreDriver =
                    store_clone.inner_store(Some(synthetic_large_key()));
                let Some(fss) = find_fast_slow_via_chain(driver) else {
                    return (name_owned, 0, core::time::Duration::ZERO);
                };
                let store_started = std::time::Instant::now();
                let unflushed = fss
                    .flush_fast_to_slow_at_shutdown(phase_2_per_store)
                    .await;
                (name_owned, unflushed, store_started.elapsed())
            }));
        }

        let phase_2_drain = async {
            let mut results: Vec<(String, usize, core::time::Duration)> =
                Vec::with_capacity(phase_2_joins.len());
            for join in phase_2_joins {
                match join.await {
                    Ok(tuple) => results.push(tuple),
                    Err(e) => {
                        warn!(error = ?e, "flush_slow_writes: Phase 2 task panicked")
                    }
                }
            }
            results
        };
        let phase_2_results = match tokio::time::timeout(
            phase_2_deadline + core::time::Duration::from_secs(2),
            phase_2_drain,
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                warn!(
                    elapsed_ms = u64::try_from(phase_2_started.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                    "flush_slow_writes: Phase 2 outer wall-clock timeout fired",
                );
                Vec::new()
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

    Ok(store_manager)
}
