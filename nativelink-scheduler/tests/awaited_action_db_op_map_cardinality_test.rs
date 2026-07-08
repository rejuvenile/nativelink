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

//! Cardinality-guard render test for the `AwaitedActionDbImpl` operation maps
//! (`#metrics-scrape-59mb-per-op-cardinality`).
//!
//! Root cause this guards: the three per-operation maps
//! (`operation_id_to_awaited_action`, `connected_clients_for_operation_id`,
//! `action_info_hash_key_to_awaited_action`) were tagged
//! `#[metric(group = "…")]`, which reached the blanket
//! `impl MetricsComponent for BTreeMap/HashMap` (nativelink-metric/src/lib.rs) —
//! an unconditional per-entry loop. `operation_id_to_awaited_action`'s value is a
//! `watch::Sender<AwaitedAction>`, so EACH live operation emitted ~18
//! `AwaitedAction` fields. At ~1,345 retained ops that is ~24k `/metrics` lines
//! from ONE map, driving a 59 MB scrape. The invariant the fix restores: the
//! render is O(distinct-metric-families), NOT O(live-operations) — no per-entity
//! cardinality on these maps.
//!
//! Exercises the REAL metrics path: a `SimpleScheduler` built with a
//! `MemoryAwaitedActionDb`, N actions added (each populates all three maps),
//! rendered via `render_prometheus` — the same path the production `/metrics`
//! listener uses. The `AwaitedActionDbImpl` maps sit under the group path
//! (see `simple_scheduler.rs:1471`, `simple_scheduler_state_manager.rs:280`):
//!
//!   scheduler.test.action
//!     .matching_engine_state_manager   (SimpleScheduler field group)
//!     .action_db                        (SimpleSchedulerStateManager field group)
//!     .<AwaitedActionDbImpl field>      (leaf scalar count, post-fix)
//!
//! Mutation rule: revert any of the three fields back to
//! `#[metric(group = "…")]` (per-entry render); this test must red-fail on the
//! reappearance of the per-UUID group prefix (e.g. `_action_db_operation_ids_`)
//! and/or the missing single-count line.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::operation_state_manager::ClientStateManager;
use tokio::sync::Notify;

const INSTANCE_NAME: &str = "test_instance";
const NOW_TIME: u64 = 10_000;

/// Number of distinct cacheable actions added. Each distinct action creates one
/// operation → one entry in each of the three op maps under test.
const N_OPS: u64 = 3;

/// The group-path prefix that precedes every `AwaitedActionDbImpl` field on the
/// action-scheduler registration. Verified against `simple_scheduler.rs:1471`
/// (`matching_engine_state_manager`) and `simple_scheduler_state_manager.rs:280`
/// (`action_db`).
const DB_PREFIX: &str = "scheduler_test_action_matching_engine_state_manager_action_db";

fn make_action_info(digest: DigestInfo, ts_offset: u64) -> Arc<ActionInfo> {
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp: UNIX_EPOCH
            .checked_add(Duration::from_secs(NOW_TIME + ts_offset))
            .unwrap(),
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest,
        }),
    })
}

fn new_scheduler() -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>, Arc<Notify>) {
    let task_notify = Arc::new(Notify::new());
    let awaited_action_db =
        memory_awaited_action_db_factory(0, &task_notify.clone(), MockInstantWrapped::default);
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    let (scheduler, worker_scheduler) = SimpleScheduler::new_with_callback(
        &spec,
        awaited_action_db,
        || async move {},
        task_notify.clone(),
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    (scheduler, worker_scheduler, task_notify)
}

fn register(
    scheduler: Arc<SimpleScheduler>,
    worker_scheduler: Arc<dyn WorkerScheduler>,
) -> MetricsRegistry {
    let registry = MetricsRegistry::new();
    registry.register("scheduler.test.action", scheduler);
    let ws_component: Arc<
        dyn nativelink_util::metrics_publisher::MetricsComponentTrait + Send + Sync,
    > = worker_scheduler;
    registry.register_dyn("scheduler.test.worker".to_string(), ws_component);
    registry
}

/// Count lines in `body` whose metric name starts with `needle`. A Prometheus
/// exposition line is `<name>{...}? <value>` — we match the raw line prefix.
fn count_lines_with_prefix(body: &str, needle: &str) -> usize {
    body.lines().filter(|l| l.starts_with(needle)).count()
}

/// #metrics-scrape-59mb-per-op-cardinality: the three per-operation maps each
/// render as a SINGLE scalar count equal to the number of live operations, and
/// emit ZERO per-UUID (per-entry) lines — no matter how many operations exist.
///
/// This is the render-cost invariant: `/metrics` size is O(distinct families),
/// not O(live-operations). With the pre-fix per-entry render, adding N ops added
/// ~18·N lines from `operation_id_to_awaited_action` alone; post-fix it adds
/// exactly ONE line per map.
#[nativelink_test]
async fn op_maps_render_as_single_count_not_per_entry() -> Result<(), Error> {
    let (scheduler, worker_scheduler, _notify) = new_scheduler();

    // Add N distinct cacheable actions. `add_action` → `make_client_awaited_action`
    // inserts into `operation_id_to_awaited_action` (memory_awaited_action_db.rs:812)
    // and `connected_clients_for_operation_id` (:814); `add_action` inserts into
    // `action_info_hash_key_to_awaited_action` (:871) for cacheable actions. No
    // workers are registered, so all N stay Queued and retained in every map.
    for i in 0..N_OPS {
        scheduler
            .add_action(
                OperationId::default(),
                make_action_info(DigestInfo::new([i as u8 + 1; 32], 0), i),
            )
            .await
            .expect("#metrics-scrape setup: add_action must succeed");
    }

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    // ── (a) Each map emits exactly ONE scalar count line equal to N_OPS ──────
    //
    // The leaf name is the field name (the derive's `handler` publishes
    // `stringify!(field_name)`), the group attribute having been dropped (a
    // single-scalar group added only a redundant/doubled path segment). So the
    // Prometheus name is `<DB_PREFIX>_<field_name>` with value N_OPS.
    for field in [
        "operation_id_to_awaited_action",
        "connected_clients_for_operation_id",
        "action_info_hash_key_to_awaited_action",
    ] {
        let line = format!("\n{DB_PREFIX}_{field} {N_OPS}\n");
        assert!(
            body.contains(&line),
            "#metrics-scrape-59mb-per-op-cardinality MISSING or WRONG VALUE: map `{field}` must \
             render as a SINGLE scalar count == {N_OPS} at `{DB_PREFIX}_{field}`. If this line is \
             absent the map is either rendering per-entry (unbounded cardinality) or not at all. \
             body=\n{body}"
        );
        // Exactly one line for this metric name (a per-entry render would emit
        // many lines whose names begin with this field's group span instead).
        let exact_prefix = format!("{DB_PREFIX}_{field} ");
        let n = count_lines_with_prefix(&body, &exact_prefix);
        assert_eq!(
            n, 1,
            "#metrics-scrape-59mb-per-op-cardinality: map `{field}` must emit EXACTLY ONE count \
             line; found {n} lines starting with `{exact_prefix}`. body=\n{body}"
        );
    }

    // ── (b) ZERO per-UUID (per-entry) lines for the operation-id map ─────────
    //
    // Pre-fix, `#[metric(group = "operation_ids")]` on
    // `operation_id_to_awaited_action` made the blanket `BTreeMap` impl enter a
    // span per OperationId key, so every op emitted `AwaitedAction`'s ~18 fields
    // under `_action_db_operation_ids_<uuid>_…`. The dropped group means the
    // `operation_ids` span never exists post-fix. This is the load-bearing
    // cardinality assertion.
    let per_entry_op_ids = format!("{DB_PREFIX}_operation_ids_");
    assert!(
        !body.contains(&per_entry_op_ids),
        "#metrics-scrape-59mb-per-op-cardinality PER-ENTRY CARDINALITY LEAK: found the per-op \
         group prefix `{per_entry_op_ids}` in the render — `operation_id_to_awaited_action` is \
         emitting one span-group PER OperationId (the 59 MB / 131k-series defect). It must emit a \
         single count instead. body=\n{body}"
    );

    // Corroborate: NONE of the per-op-map AwaitedAction leaf fields leak. If a
    // single op's `watch::Sender<AwaitedAction>` were still rendered per-entry,
    // these AwaitedAction field leaves would appear under the op-id path.
    for leaked_leaf in [
        "_action_db_operation_ids_",
        "_action_db_connected_clients_for_operation_id_",
        "_action_db_action_info_hash_key_to_awaited_action_",
    ] {
        assert!(
            !body.contains(leaked_leaf),
            "#metrics-scrape-59mb-per-op-cardinality PER-ENTRY CARDINALITY LEAK: found per-entry \
             group prefix `{leaked_leaf}` — a per-operation map is still rendering per-entry. \
             body=\n{body}"
        );
    }

    Ok(())
}

/// #metrics-scrape-59mb-per-op-cardinality: the count lines are PRESENT at their
/// floor (0) when the scheduler is idle — so an idle scheduler yields a complete
/// time series rather than a gap that reads as "metric not found" on dashboards.
#[nativelink_test]
async fn op_maps_counts_present_at_zero_when_idle() -> Result<(), Error> {
    let (scheduler, worker_scheduler, _notify) = new_scheduler();

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    for field in [
        "operation_id_to_awaited_action",
        "connected_clients_for_operation_id",
        "action_info_hash_key_to_awaited_action",
    ] {
        let line = format!("\n{DB_PREFIX}_{field} 0\n");
        assert!(
            body.contains(&line),
            "#metrics-scrape-59mb-per-op-cardinality idle: map `{field}` count must emit 0 even \
             when no operations exist (absence would read as `no data` on an idle scheduler). \
             body=\n{body}"
        );
    }

    Ok(())
}
