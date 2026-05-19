// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! #564 fix-up: regression for the server-vs-worker discriminator that
//! feeds `is_server_process()`.
//!
//! ## Why this test exists (BLOCK convergence)
//!
//! The first revision of #564 used `!cfg.servers.is_empty()` as the
//! discriminator in `src/bin/nativelink.rs`. That predicate is `true`
//! on every production worker, because every production worker config
//! defines server listeners — `~/fl/bld/infra/nativelink/worker.json5`
//! at `:211-278` exposes the public peer-CAS on :50051 AND a private
//! worker_api/admin/health/metrics block on :50061. The bug shipped
//! zero savings on workers (the entire goal of #564), and was caught
//! by red-team + distributed-systems-reviewer + assumption-auditor
//! converging in the same review pass.
//!
//! The BLOCK slipped past 7 reviewers in the first pass because the
//! existing `fast_slow_store_564_pusher_metric_worker_gate_test.rs`
//! and `fast_slow_store_554_pusher_metric_completeness_test.rs` tests
//! bypass the discriminator entirely via `set_is_server_process_for_test`.
//! No test crossed the `CasConfig → is_server_process_from_config →
//! set_is_server_process → IS_SERVER_PROCESS_RUNTIME → gate` seam at
//! its config-parse end.
//!
//! ## Seams crossed by this test
//!
//! - `serde_json5::from_str::<CasConfig>(...)` — config-load seam.
//! - `is_server_process_from_config(&cfg)` — discriminator seam (the
//!   one that shipped wrong).
//!
//! Together with the existing tests in
//! `fast_slow_store_564_pusher_metric_worker_gate_test.rs` (which
//! cover the `set_is_server_process_for_test → IS_SERVER_PROCESS_RUNTIME
//! → gate` seams) and `fast_slow_store_554_pusher_metric_completeness_test.rs`
//! (which covers the default-server end-to-end), all five seams in
//! the chain have at least one test crossing them.
//!
//! ## Mutation step (test-MUST-red-fail check)
//!
//! Revert the `is_server_process_from_config` body to
//! `!cfg.workers.as_ref().is_some_and(|w| !w.is_empty())` — wait, that
//! IS the fix. The mutation is to revert it to the BROKEN predicate:
//!
//! ```ignore
//! pub fn is_server_process_from_config(cfg: &CasConfig) -> bool {
//!     !cfg.servers.is_empty()  // <-- BROKEN: classifies every prod worker as server
//! }
//! ```
//!
//! Running `cargo test -p nativelink-util --test phase0_metrics_discriminator_test`
//! after that mutation MUST red-fail `worker_config_classifies_as_worker_not_server`
//! with the bespoke message
//! `"#564: production worker config classified as server — discriminator broken"`.
//! Restoring the fix MUST make the test pass.
//!
//! Documented as the mutation step per CLAUDE.md "Test-first development
//! (TDD)" item 5 — most-skipped-most-needed.

use nativelink_config::cas_server::CasConfig;
use nativelink_util::phase0_metrics::is_server_process_from_config;

/// Production-shape config for a worker: non-empty `workers` AND
/// non-empty `servers` (every prod worker exposes peer-CAS on :50051
/// plus a private admin/health/metrics block on :50061).
///
/// Minimised to the fields the discriminator reads (`workers` +
/// `servers`); everything else relies on `#[serde(default)]`. The
/// `stores`, `services`, etc. fields the worker refers to from inside
/// each block are NOT needed because the discriminator never looks at
/// them — and `CasConfig` allows them to be absent at the top level
/// because `stores: Vec<StoreConfig>` defaults to empty.
const WORKER_FIXTURE: &str = r#"{
  "stores": [
    { "name": "WORKER_FAST_SLOW", "memory": {} }
  ],
  "workers": [
    {
      "local": {
        "worker_api_endpoint": { "uri": "grpc://example:50061" },
        "cas_fast_slow_store": "WORKER_FAST_SLOW",
        "work_directory": "/tmp/nativelink-test-worker",
        "platform_properties": {}
      }
    }
  ],
  "servers": [
    {
      "name": "public",
      "listener": { "http": { "socket_address": "0.0.0.0:50051" } },
      "services": {}
    },
    {
      "name": "private",
      "listener": { "http": { "socket_address": "0.0.0.0:50061" } },
      "services": {}
    }
  ]
}"#;

/// Production-shape config for a server (buildcache): empty `workers`
/// (`Some(vec![])`) + non-empty `servers`. Mirrors
/// `~/fl/bld/infra/nativelink/prod-server.json5:286-289`.
const SERVER_FIXTURE: &str = r#"{
  "stores": [
    { "name": "CAS_MAIN", "memory": {} }
  ],
  "workers": [],
  "servers": [
    {
      "name": "public",
      "listener": { "http": { "socket_address": "0.0.0.0:50051" } },
      "services": {}
    }
  ]
}"#;

/// Production-shape config for a server (legacy) where `workers` is
/// entirely absent (`None`) rather than `Some(vec![])`.
const SERVER_FIXTURE_NO_WORKERS_KEY: &str = r#"{
  "stores": [
    { "name": "CAS_MAIN", "memory": {} }
  ],
  "servers": [
    {
      "name": "public",
      "listener": { "http": { "socket_address": "0.0.0.0:50051" } },
      "services": {}
    }
  ]
}"#;

fn parse_or_panic(fixture_name: &str, fixture: &str) -> CasConfig {
    serde_json5::from_str::<CasConfig>(fixture).unwrap_or_else(|err| {
        panic!(
            "test fixture `{fixture_name}` failed to parse as CasConfig — \
             update the fixture if a serde schema landed: {err:?}"
        )
    })
}

/// The BLOCK regression: a production-worker-shape config (non-empty
/// `workers` AND non-empty `servers`) MUST classify as a worker.
/// The broken pre-fix predicate (`!cfg.servers.is_empty()`) returned
/// `true` here on every production worker, defeating #564 entirely.
#[test]
fn worker_config_classifies_as_worker_not_server() {
    let cfg = parse_or_panic("WORKER_FIXTURE", WORKER_FIXTURE);
    assert!(
        !cfg.servers.is_empty(),
        "fixture sanity: worker config must have non-empty servers — \
         otherwise the test doesn't exercise the broken predicate"
    );
    assert!(
        cfg.workers.as_ref().is_some_and(|w| !w.is_empty()),
        "fixture sanity: worker config must have non-empty workers — \
         otherwise the test doesn't model production"
    );
    assert!(
        !is_server_process_from_config(&cfg),
        "#564: production worker config classified as server — discriminator broken"
    );
}

/// A server-shape config matching buildcache (empty `workers` +
/// non-empty `servers`) MUST classify as a server.
#[test]
fn server_config_with_empty_workers_classifies_as_server() {
    let cfg = parse_or_panic("SERVER_FIXTURE", SERVER_FIXTURE);
    assert!(
        !cfg.servers.is_empty(),
        "fixture sanity: server config must have non-empty servers"
    );
    assert!(
        cfg.workers.as_ref().is_some_and(|w| w.is_empty()),
        "fixture sanity: buildcache-shape config must have Some(empty) workers"
    );
    assert!(
        is_server_process_from_config(&cfg),
        "#564: production server config classified as worker — discriminator broken"
    );
}

/// A server-shape config with `workers` field entirely absent
/// (`None`) MUST classify as a server. Covers legacy / non-worker
/// configs that omit the key.
#[test]
fn server_config_with_none_workers_classifies_as_server() {
    let cfg = parse_or_panic(
        "SERVER_FIXTURE_NO_WORKERS_KEY",
        SERVER_FIXTURE_NO_WORKERS_KEY,
    );
    assert!(
        cfg.workers.is_none(),
        "fixture sanity: this fixture must have workers: None"
    );
    assert!(
        is_server_process_from_config(&cfg),
        "#564: production server config (no workers key) classified as worker — discriminator broken"
    );
}
