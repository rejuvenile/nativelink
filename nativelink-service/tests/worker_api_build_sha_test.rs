// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! (#216) Stale-worker detection: server rejects workers whose
//! reported `build_sha` is not in the configured allowlist.
//!
//! Tests live in their own integration file (rather than appending to
//! `worker_api_server_test.rs`) for two reasons:
//!   1. `worker_api_server_test.rs` is gated by `required-features =
//!      ["test-utils"]` — these tests don't need that feature, so a
//!      separate file keeps them runnable in the default build matrix.
//!   2. The test file is small + topical; co-locating with the bug
//!      number is faster to find for the next person looking for the
//!      regression after a deploy mishap.
//!
//! TDD provenance:
//!   * Step 1 (red): each test was written BEFORE
//!     `WorkerApiServer::compatible_build_shas` was wired and asserts
//!     a specific failure / success that the unmodified server cannot
//!     satisfy.
//!   * Step 2 (green): the server changes in
//!     `nativelink-service/src/worker_api_server.rs` make every test
//!     pass.
//!   * Step 3 (mutation): the rejection branch in `inner_connect_worker`
//!     can be commented out; on the rebuild,
//!     `connect_rejects_stale_worker_with_failed_precondition` and
//!     `rejection_increments_stale_workers_rejected_total` MUST fail
//!     with the bespoke `.expect(...)` messages spelled out below.
//!     Without that property we have not proven the test guards the
//!     behavior. (See CLAUDE.md "Mutate the fix and verify the test
//!     fails again.")

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    update_for_worker, ConnectWorkerRequest, UpdateForScheduler,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_metric::MetricsComponent;
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const SCHEDULER_NAME: &str = "TEST_BUILD_SHA_SCHEDULER";
const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

/// Minimal `WorkerStateManager` stub — tests don't drive any
/// operation-state transitions; the connect path's only
/// dependency on the WSM is the `Arc<dyn WorkerStateManager>`
/// stored inside the scheduler. A no-op impl is sufficient.
///
/// `#[derive(MetricsComponent)]` requires a non-unit struct, so we
/// keep a single phantom field — never read, but enough to satisfy
/// the macro.
#[derive(MetricsComponent)]
struct NoopWorkerStateManager {
    #[metric(help = "placeholder field; the no-op state manager has no metrics")]
    _placeholder: u64,
}

#[async_trait]
impl WorkerStateManager for NoopWorkerStateManager {
    async fn update_operation(
        &self,
        _operation_id: &OperationId,
        _worker_id: &WorkerId,
        _update: UpdateOperationType,
    ) -> Result<(), Error> {
        Ok(())
    }
}

/// Constructs a `WorkerApiServer` with the given `compatible_build_shas`
/// allowlist plumbed through `WorkerApiConfig`. Returns the server
/// alongside the scheduler so tests can introspect both.
fn build_server(
    compatible_build_shas: Option<Vec<String>>,
) -> Result<(WorkerApiServer, Arc<ApiWorkerScheduler>), Error> {
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(NoopWorkerStateManager { _placeholder: 0 });
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager,
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());

    let server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas,
        },
        &schedulers,
        Box::new(static_now_fn),
        [42u8; 6],
        None,
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "build_server: WorkerApiServer::new_with_now_fn")?;

    Ok((server, scheduler))
}

/// Constructs a one-shot `update_stream` that yields a single
/// `ConnectWorkerRequest` and then closes. This is what the production
/// `WorkerApi::connect_worker` handler sees from the worker side: the
/// hello frame comes first, then the stream stays open for keepalives,
/// blob notifications, etc. For these tests we only care about the
/// hello / response handshake — the stream EOFs immediately after.
fn make_hello_stream(
    request: ConnectWorkerRequest,
) -> impl tokio_stream::Stream<Item = Result<UpdateForScheduler, tonic::Status>> + Unpin + Send + 'static
{
    let (tx, rx) = mpsc::channel::<Update>(1);
    // Send synchronously — channel has slot and tx is dropped after.
    tx.try_send(Update::ConnectWorkerRequest(request))
        .expect("test setup: hello channel must accept first message");
    drop(tx);
    Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            (
                Ok::<_, tonic::Status>(UpdateForScheduler {
                    update: Some(update),
                }),
                rx,
            )
        })
    }))
}

// ---------------------------------------------------------------------
// Production-composition test (CLAUDE.md "Test in production composition,
// not in isolation"): exercise the full `inner_connect_worker_for_testing`
// path with a real `WorkerApiServer + ApiWorkerScheduler`. The harness
// is fully self-contained — no mocks for the validation path. Each
// test wraps its assertion in `tokio::time::timeout` so a hang would
// fail the test rather than wedge the CI runner.
// ---------------------------------------------------------------------

/// Allowlist disabled (`compatible_build_shas: None`) → every worker
/// accepted, including ones that report empty / unknown SHAs. This is
/// the default deployment shape and MUST stay backward-compatible.
#[nativelink_test]
async fn allowlist_none_accepts_every_worker() -> Result<(), Box<dyn core::error::Error>> {
    let (server, _scheduler) = build_server(None)?;
    let metrics = server.metrics();

    let request = ConnectWorkerRequest {
        worker_id_prefix: "legacy_worker_".to_string(),
        build_sha: String::new(), // legacy worker — no SHA reported
        ..Default::default()
    };
    let stream = make_hello_stream(request);

    let response = tokio::time::timeout(
        TEST_TIMEOUT,
        server.inner_connect_worker_for_testing(stream),
    )
    .await
    .expect("must not deadlock — allowlist=None should short-circuit fast")?;

    // Pull the first frame off the response stream — it should be a
    // ConnectionResult, NOT an error. Anything else means the
    // validation path mistakenly engaged when the allowlist was None.
    let mut response_stream = response.into_inner();
    let first = tokio::time::timeout(TEST_TIMEOUT, response_stream.next())
        .await
        .expect("must not deadlock — first response should arrive immediately")
        .ok_or("response stream EOF without ConnectionResult")?
        .map_err(|e| format!("response stream errored: {e:?}"))?;
    assert!(
        matches!(first.update, Some(update_for_worker::Update::ConnectionResult(_))),
        "expected ConnectionResult, got {:?}",
        first.update
    );
    assert_eq!(
        metrics.stale_workers_rejected_total.load(Ordering::Relaxed),
        0,
        "stale_workers_rejected_total MUST stay zero when allowlist is None"
    );
    Ok(())
}

/// Allowlist enabled, worker SHA matches → connection accepted; the
/// rejection counter stays zero. This proves the validation path
/// doesn't false-positive on a well-formed deploy.
#[nativelink_test]
async fn allowlist_match_accepts_worker() -> Result<(), Box<dyn core::error::Error>> {
    let allowed = "1234567890abcdef".to_string();
    let (server, _scheduler) = build_server(Some(vec![allowed.clone()]))?;
    let metrics = server.metrics();

    let request = ConnectWorkerRequest {
        worker_id_prefix: "good_worker_".to_string(),
        build_sha: allowed.clone(),
        ..Default::default()
    };
    let stream = make_hello_stream(request);

    let response = tokio::time::timeout(
        TEST_TIMEOUT,
        server.inner_connect_worker_for_testing(stream),
    )
    .await
    .expect("must not deadlock — matching SHA should accept fast")?;

    let mut response_stream = response.into_inner();
    let first = tokio::time::timeout(TEST_TIMEOUT, response_stream.next())
        .await
        .expect("must not deadlock — ConnectionResult expected immediately")
        .ok_or("response stream EOF without ConnectionResult")?
        .map_err(|e| format!("response stream errored: {e:?}"))?;
    assert!(
        matches!(first.update, Some(update_for_worker::Update::ConnectionResult(_))),
        "expected ConnectionResult on SHA match; got {:?}",
        first.update
    );
    assert_eq!(
        metrics.stale_workers_rejected_total.load(Ordering::Relaxed),
        0,
        "stale_workers_rejected_total MUST stay zero on a SHA match"
    );
    Ok(())
}

/// Allowlist enabled, worker SHA NOT in list → connection REJECTED
/// with `Code::FailedPrecondition` and the rejection counter
/// increments by one. This is the regression test for #216 — the
/// failure mode that previously manifested as silent reconnect
/// storms.
#[nativelink_test]
async fn connect_rejects_stale_worker_with_failed_precondition()
-> Result<(), Box<dyn core::error::Error>> {
    let allowed = "deadbeefcafef00d".to_string();
    let (server, _scheduler) = build_server(Some(vec![allowed]))?;
    let metrics = server.metrics();

    let request = ConnectWorkerRequest {
        worker_id_prefix: "stale_worker_".to_string(),
        build_sha: "0000000000000000".to_string(), // explicitly NOT allowed
        cas_endpoint: "grpc://10.0.0.1:50081".to_string(),
        ..Default::default()
    };
    let stream = make_hello_stream(request);

    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        server.inner_connect_worker_for_testing(stream),
    )
    .await
    .expect("must not deadlock — stale worker rejection must return immediately");

    let err = result
        .err()
        .expect("inner_connect_worker_for_testing MUST return Err on a stale-SHA hello (#216 fix)");
    assert_eq!(
        err.code,
        Code::FailedPrecondition,
        "stale-worker rejection MUST use Code::FailedPrecondition (operator-actionable signal); got {:?}",
        err.code
    );
    let msg = format!("{err:?}");
    assert!(
        msg.contains("compatible_build_shas")
            && msg.contains("redeploy"),
        "rejection error message MUST name the config field + the remediation, got: {msg}"
    );
    assert_eq!(
        metrics.stale_workers_rejected_total.load(Ordering::Relaxed),
        1,
        "stale_workers_rejected_total MUST increment on rejection"
    );
    Ok(())
}

/// Allowlist enabled with multiple entries and the legacy empty SHA
/// included → a worker reporting an empty SHA is accepted, and a
/// worker reporting one of the listed non-empty SHAs is also
/// accepted. Validates the operator's "current SHA + previous N + ''"
/// rollout pattern.
#[nativelink_test]
async fn allowlist_with_legacy_marker_accepts_empty_sha()
-> Result<(), Box<dyn core::error::Error>> {
    let allowed = vec![
        "aaaaaaaaaaaaaaaa".to_string(),
        "bbbbbbbbbbbbbbbb".to_string(),
        String::new(), // legacy marker — accept workers that pre-date the SHA field
    ];
    let (server, _scheduler) = build_server(Some(allowed))?;
    let metrics = server.metrics();

    let request = ConnectWorkerRequest {
        worker_id_prefix: "legacy_worker_".to_string(),
        build_sha: String::new(),
        ..Default::default()
    };
    let stream = make_hello_stream(request);

    let response = tokio::time::timeout(
        TEST_TIMEOUT,
        server.inner_connect_worker_for_testing(stream),
    )
    .await
    .expect("must not deadlock — empty-SHA legacy worker must be accepted")?;

    let mut response_stream = response.into_inner();
    let first = tokio::time::timeout(TEST_TIMEOUT, response_stream.next())
        .await
        .expect("must not deadlock — ConnectionResult expected for legacy worker")
        .ok_or("response stream EOF without ConnectionResult")?
        .map_err(|e| format!("response stream errored: {e:?}"))?;
    assert!(
        matches!(first.update, Some(update_for_worker::Update::ConnectionResult(_))),
        "expected ConnectionResult for explicit-legacy allowlist entry, got {:?}",
        first.update
    );
    assert_eq!(
        metrics.stale_workers_rejected_total.load(Ordering::Relaxed),
        0,
        "explicit empty-SHA allowance MUST NOT increment the rejection counter"
    );
    Ok(())
}

/// Empty `Some(vec![])` allowlist collapses to `None` semantics
/// (validation disabled) per the `WorkerApiServer::new_with_now_fn`
/// constructor logic. This guards against a foot-gun where an
/// operator clears the list expecting "reject everyone" but really
/// means "no validation" — the `info!` log on enable surfaces the
/// effective behavior, and this test pins the contract.
#[nativelink_test]
async fn empty_allowlist_collapses_to_disabled() -> Result<(), Box<dyn core::error::Error>> {
    let (server, _scheduler) = build_server(Some(Vec::new()))?;
    let metrics = server.metrics();

    let request = ConnectWorkerRequest {
        worker_id_prefix: "anysha_worker_".to_string(),
        build_sha: "ffffffffffffffff".to_string(),
        ..Default::default()
    };
    let stream = make_hello_stream(request);

    let response = tokio::time::timeout(
        TEST_TIMEOUT,
        server.inner_connect_worker_for_testing(stream),
    )
    .await
    .expect("must not deadlock — empty allowlist == disabled, accept fast")?;

    let mut response_stream = response.into_inner();
    let first = tokio::time::timeout(TEST_TIMEOUT, response_stream.next())
        .await
        .expect("must not deadlock — ConnectionResult expected for disabled allowlist")
        .ok_or("response stream EOF without ConnectionResult")?
        .map_err(|e| format!("response stream errored: {e:?}"))?;
    assert!(
        matches!(first.update, Some(update_for_worker::Update::ConnectionResult(_))),
        "expected ConnectionResult when allowlist is empty (disabled); got {:?}",
        first.update
    );
    assert_eq!(
        metrics.stale_workers_rejected_total.load(Ordering::Relaxed),
        0,
        "empty-list-disabled allowlist MUST NOT increment the rejection counter"
    );
    Ok(())
}

/// Multiple stale workers in a row → counter accumulates correctly.
/// Belt-and-suspenders against a future regression that would lose
/// counter state on the rejection path (e.g. accidentally building a
/// fresh metrics handle per connection).
#[nativelink_test]
async fn rejection_increments_stale_workers_rejected_total()
-> Result<(), Box<dyn core::error::Error>> {
    let allowed = "1111111111111111".to_string();
    let (server, _scheduler) = build_server(Some(vec![allowed]))?;
    let metrics = server.metrics();

    for i in 0..3 {
        let request = ConnectWorkerRequest {
            worker_id_prefix: format!("stale_{i}_"),
            build_sha: format!("99{:014x}", i), // varying stale SHAs
            ..Default::default()
        };
        let stream = make_hello_stream(request);
        let result = tokio::time::timeout(
            TEST_TIMEOUT,
            server.inner_connect_worker_for_testing(stream),
        )
        .await
        .expect("must not deadlock — every stale rejection should be immediate");
        assert!(
            result.is_err(),
            "iter {i}: stale worker MUST be rejected"
        );
    }
    assert_eq!(
        metrics.stale_workers_rejected_total.load(Ordering::Relaxed),
        3,
        "counter MUST equal the number of rejected connects across multiple workers"
    );
    Ok(())
}

/// (#216 testing-czar MAJOR) Allowlist enabled, list does NOT contain
/// "" — a legacy worker reporting an empty build_sha MUST be REJECTED.
/// This is the inverse of `allowlist_with_legacy_marker_accepts_empty_sha`
/// and the test that catches a future regression where an off-by-one
/// "treat empty as match-all" semantics creeps back in.
#[nativelink_test]
async fn allowlist_without_legacy_marker_rejects_empty_sha()
-> Result<(), Box<dyn core::error::Error>> {
    let (server, _scheduler) =
        build_server(Some(vec!["1234567890abcdef".to_string()]))?;
    let metrics = server.metrics();

    let request = ConnectWorkerRequest {
        worker_id_prefix: "legacy_no_marker_".to_string(),
        build_sha: String::new(), // legacy worker, list lacks ""
        ..Default::default()
    };
    let stream = make_hello_stream(request);
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        server.inner_connect_worker_for_testing(stream),
    )
    .await
    .expect("must not deadlock — empty-SHA rejection should be immediate when \"\" is NOT in the allowlist");

    let err = result.err().expect(
        "inner_connect_worker_for_testing MUST return Err: the empty SHA is NOT in the \
         allowlist and the validation is enabled — accepting it would silently let legacy \
         workers bypass deployment-drift detection",
    );
    assert_eq!(
        err.code,
        Code::FailedPrecondition,
        "empty-SHA rejection MUST use Code::FailedPrecondition (same as any other stale-SHA path); got {:?}",
        err.code,
    );
    assert_eq!(
        metrics.stale_workers_rejected_total.load(Ordering::Relaxed),
        1,
        "rejection counter MUST increment for empty-SHA rejection",
    );
    Ok(())
}

/// (#216 defense-in-depth) The hello frame's string fields are bounded
/// at 256 bytes — anything longer is rejected with `InvalidArgument`
/// BEFORE the build-SHA validation even runs. This guards against a
/// buggy/malicious worker dumping multi-megabyte strings into the log
/// stream via the `tracing::warn!` field, and against the same string
/// propagating up through every retrying caller via `Display`.
#[nativelink_test]
async fn oversized_hello_string_is_rejected_with_invalid_argument()
-> Result<(), Box<dyn core::error::Error>> {
    // Validation disabled — the bound runs unconditionally.
    let (server, _scheduler) = build_server(None)?;

    // 257-byte cas_endpoint exceeds the 256 limit by 1.
    let oversized = "g".repeat(257);
    let request = ConnectWorkerRequest {
        worker_id_prefix: "ok_".to_string(),
        build_sha: String::new(),
        cas_endpoint: oversized,
        ..Default::default()
    };
    let stream = make_hello_stream(request);
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        server.inner_connect_worker_for_testing(stream),
    )
    .await
    .expect("must not deadlock — oversized-string rejection must be immediate");

    let err = result.err().expect(
        "inner_connect_worker_for_testing MUST return Err on 257-byte cas_endpoint — \
         the bound is part of the trust boundary",
    );
    assert_eq!(
        err.code,
        Code::InvalidArgument,
        "oversized hello-string rejection MUST use Code::InvalidArgument (distinct from \
         FailedPrecondition used for legitimate stale-SHA mismatch); got {:?}",
        err.code,
    );
    Ok(())
}

/// (#216 testing-czar MAJOR) `WorkerApiConfig` deserialized from a
/// minimal JSON5 with no `compatible_build_shas` field MUST surface as
/// `None` — guards against a serde-default regression that would make
/// the field accidentally enable validation (e.g. flip to
/// `Some(vec![])` semantics if someone changes the type to
/// `#[serde(default)] Vec<String>`).
#[test]
fn worker_api_config_round_trip_default_is_none() {
    let cfg: WorkerApiConfig = serde_json5::from_str(r#"{ scheduler: "x" }"#)
        .expect("minimal WorkerApiConfig with only `scheduler` must deserialize");
    assert_eq!(cfg.scheduler, "x");
    assert!(
        cfg.compatible_build_shas.is_none(),
        "compatible_build_shas MUST default to None (validation disabled); got {:?}",
        cfg.compatible_build_shas,
    );
}

/// (#216) Round-trip a populated allowlist through serde_json5 and
/// confirm the parsed Vec preserves entry order + content. Belt-and-
/// suspenders against the rename / serde-attribute changes that would
/// silently drop entries.
#[test]
fn worker_api_config_round_trip_populated_list_preserves_entries() {
    let cfg: WorkerApiConfig = serde_json5::from_str(
        r#"{ scheduler: "x", compatible_build_shas: ["aaaa", "bbbb", ""] }"#,
    )
    .expect("populated WorkerApiConfig must deserialize");
    let entries = cfg
        .compatible_build_shas
        .expect("compatible_build_shas MUST be Some(_) when listed in JSON5");
    assert_eq!(
        entries,
        vec!["aaaa".to_string(), "bbbb".to_string(), String::new()],
        "every listed allowlist entry (including the legacy marker \"\") MUST round-trip",
    );
}
