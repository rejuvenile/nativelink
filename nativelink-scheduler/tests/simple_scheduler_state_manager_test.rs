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

//! P0 operation-lifecycle state-machine coverage for
//! `SimpleSchedulerStateManager::inner_update_operation`
//! (`nativelink-scheduler/src/simple_scheduler_state_manager.rs`).
//!
//! These tests pin the retry-budget / error-classification / version-conflict
//! state machine that a TLA+/TLC model verified holds. The model abstracts the
//! numeric details; these tests cover fidelity against the real implementation
//! (the real `MemoryAwaitedActionDb` + the real
//! `SimpleSchedulerStateManager::inner_update_operation`).
//!
//! Behaviors pinned (cite `src/simple_scheduler_state_manager.rs` line numbers):
//!   * RETRY-BUDGET: a non-backpressure, non-`FailedPrecondition` error bumps
//!     `attempts` each time (`:837-839`) and reaches `Completed(error)` only
//!     once `attempts > max_job_retries` (`:841-857`).
//!   * `ResourceExhausted` (backpressure) does NOT consume the attempt budget
//!     (`:817,:837`).
//!   * `FailedPrecondition` is terminal-immediate — no requeue, one attempt
//!     bump then `Completed(error)` (`:836,:841-857`).
//!   * SIGKILL (`exit_code == 9`) bumps `attempts` + boosts priority + requeues
//!     to `Queued` while budget permits, terminal once exhausted (`:757-786`).
//!   * A bounded number of version conflicts (`Code::Aborted` from
//!     `update_awaited_action`) is retried up to `MAX_UPDATE_RETRIES` (`:649`)
//!     and, when the CAS eventually succeeds, the operation still reaches
//!     `Executing` (assign path, `:1285`).
//!
//! FIXTURE / SEAM NOTES (STEP 0):
//!   * `attempts` (`awaited_action.rs:92`) is `pub` and readable from this
//!     external test crate via a subscriber `.borrow()`; `version()`,
//!     `boost_priority()`, `worker_id()` are `pub(crate)` and NOT reachable —
//!     so read-back is via `attempts` + `state().stage` + the queue-order
//!     stream, never via `version()`/`sort_key()` directly.
//!   * The state manager OWNS its `AwaitedActionDb` by value with no public
//!     accessor, and `attempts` is not on `ActionState` (only on
//!     `AwaitedAction`). To read `attempts` back we hand the manager a
//!     `TestDb` wrapper that holds an `Arc<MemoryAwaitedActionDb>`; the test
//!     retains a clone of that same `Arc` and reads `attempts`/`stage` through
//!     it via `get_by_operation_id(op).borrow()`.
//!   * Version conflicts: `MemoryAwaitedActionDb::update_awaited_action`
//!     (`memory_awaited_action_db.rs:674-688`) returns `Code::Aborted` when the
//!     stored version differs from the written version. Forcing that at the
//!     exact read-modify-write window inside `inner_update_operation` from an
//!     external test is a race with no deterministic in-process barrier. The
//!     `AwaitedActionDb` trait IS the production seam the state manager writes
//!     through, so the same `TestDb` wrapper injects `Code::Aborted` at that
//!     seam for the first N `update_awaited_action` calls. This drives the
//!     exact retry branch (`:899`) the model verified — the state machine
//!     treats any `Aborted` from `update_awaited_action` identically
//!     regardless of source. No production code is changed.

use core::ops::Bound;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use futures::{Stream, StreamExt};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_scheduler::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedAction,
    SortedAwaitedActionState,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::memory_awaited_action_db::MemoryAwaitedActionDb;
use nativelink_scheduler::simple_scheduler_state_manager::SimpleSchedulerStateManager;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionUniqueKey, ActionUniqueQualifier, OperationId,
    WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::{
    MatchingEngineStateManager, OperationFilter, OperationStageFlags, OrderDirection,
    UpdateOperationType, WorkerStateManager,
};
use tokio::sync::Notify;

/// The `max_job_retries` this suite pins the state machine to. Chosen `> 1` and
/// `< MAX_UPDATE_RETRIES (=5)` so the retry-budget exhaustion and the
/// bounded-version-conflict tests exercise distinct bounds without collision.
const MAX_JOB_RETRIES: usize = 3;

// Both the DB and the state manager share ONE frozen mock clock
// (`MockInstantWrapped::default` => `UNIX_EPOCH + MockClock::time()`, which stays
// at epoch unless a test advances it). Sharing the clock matters for the
// filter path: `apply_filter_predicate` (state_manager.rs:399) compares the
// DB-stored `last_client_keepalive_timestamp` against the state manager's
// `now_fn` — a real-clock SM over a mock-clock DB would treat every action as
// client-timed-out and hide it from `filter_operations`. `drops_missing_actions`
// never hits that path, so it can (and does) mix `SystemTime::now` with a mock
// DB; the queue-order read-back here cannot.
type NowFnT = fn() -> MockInstantWrapped;
type MemDb = MemoryAwaitedActionDb<MockInstantWrapped, NowFnT>;

fn make_action_info(seed: u8, priority: i32) -> Arc<ActionInfo> {
    let mut hash = [0u8; 32];
    hash[0] = seed;
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::from_secs(1),
        platform_properties: std::collections::HashMap::new(),
        priority,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            instance_name: "test_instance".to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: DigestInfo::new(hash, 1),
        }),
    })
}

/// Like `make_action_info` but with an explicit `insert_timestamp` — used by
/// the P1-1 priority/FIFO ordering test to give equal-priority actions
/// DISTINCT insert times (the second-tier sort key, awaited_action.rs:232).
fn make_action_info_ts(seed: u8, priority: i32, insert_timestamp: SystemTime) -> Arc<ActionInfo> {
    let mut hash = [0u8; 32];
    hash[0] = seed;
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::from_secs(1),
        platform_properties: std::collections::HashMap::new(),
        priority,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp,
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            instance_name: "test_instance".to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: DigestInfo::new(hash, 1),
        }),
    })
}

/// Test-only `AwaitedActionDb` wrapping a shared `Arc<MemoryAwaitedActionDb>`.
///
/// Purpose: (1) let the test read `attempts` (a `pub` field on `AwaitedAction`,
/// NOT on `ActionState`) back after handing the DB to the manager — the manager
/// owns its DB and exposes no accessor, so the test keeps a clone of the same
/// `Arc`; (2) optionally inject `Code::Aborted` version conflicts at the
/// `update_awaited_action` seam. Every other method delegates verbatim.
#[derive(MetricsComponent)]
struct TestDb {
    #[metric]
    inner: Arc<MemDb>,
    /// Remaining `Code::Aborted` to inject on `update_awaited_action`.
    remaining_aborts: Arc<AtomicUsize>,
    /// Count of aborts actually injected (for assertions).
    injected_aborts: Arc<AtomicUsize>,
}

impl TestDb {
    fn new(inner: Arc<MemDb>, conflicts: usize) -> Self {
        Self {
            inner,
            remaining_aborts: Arc::new(AtomicUsize::new(conflicts)),
            injected_aborts: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl AwaitedActionDb for TestDb {
    type Subscriber = <MemDb as AwaitedActionDb>::Subscriber;

    async fn get_awaited_action_by_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        self.inner.get_awaited_action_by_id(client_operation_id).await
    }

    async fn get_all_awaited_actions(
        &self,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        self.inner.get_all_awaited_actions().await
    }

    async fn get_by_operation_id(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        self.inner.get_by_operation_id(operation_id).await
    }

    async fn get_range_of_actions(
        &self,
        state: SortedAwaitedActionState,
        start: Bound<SortedAwaitedAction>,
        end: Bound<SortedAwaitedAction>,
        desc: bool,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        self.inner.get_range_of_actions(state, start, end, desc).await
    }

    async fn update_awaited_action(
        &self,
        new_awaited_action: AwaitedAction,
    ) -> Result<(), Error> {
        // Inject a version conflict for the first N calls. We return
        // `Code::Aborted` WITHOUT touching the real store, so the next re-read
        // observes the same (unincremented) version — identical to the
        // retry-loop contract, where the state manager re-reads then retries
        // after an `Aborted` (state_manager.rs:899).
        if self.remaining_aborts.load(Ordering::SeqCst) > 0 {
            self.remaining_aborts.fetch_sub(1, Ordering::SeqCst);
            self.injected_aborts.fetch_add(1, Ordering::SeqCst);
            return Err(make_err!(
                Code::Aborted,
                "injected version conflict (test seam)"
            ));
        }
        self.inner.update_awaited_action(new_awaited_action).await
    }

    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
        no_event_action_timeout: Duration,
    ) -> Result<Self::Subscriber, Error> {
        self.inner
            .add_action(client_operation_id, action_info, no_event_action_timeout)
            .await
    }
}

/// Full fixture: a shared `Arc<MemDb>` the test reads through, plus a
/// `SimpleSchedulerStateManager` driving the SAME DB (via a `TestDb` wrapper).
struct Fixture {
    db: Arc<MemDb>,
    injected_aborts: Arc<AtomicUsize>,
    state_manager: Arc<SimpleSchedulerStateManager<TestDb, MockInstantWrapped, NowFnT>>,
}

impl Fixture {
    /// `conflicts` = number of `Code::Aborted` version conflicts to inject at
    /// the `update_awaited_action` seam before the CAS is allowed to land.
    fn new(conflicts: usize) -> Self {
        let notify = Arc::new(Notify::new());
        // Bind through the alias so the fn item coerces to the fn pointer the
        // `MemDb` / `Fixture` types expect (no explicit `as` cast needed). The
        // SAME `now_fn` drives both the DB and the state manager.
        let now_fn: NowFnT = MockInstantWrapped::default;
        let db: Arc<MemDb> =
            Arc::new(memory_awaited_action_db_factory(0, &notify, now_fn));
        let test_db = TestDb::new(db.clone(), conflicts);
        let injected_aborts = test_db.injected_aborts.clone();
        let state_manager = SimpleSchedulerStateManager::new(
            // max_job_retries — set EXPLICITLY (assertions depend on it).
            MAX_JOB_RETRIES,
            Duration::from_secs(10), // no_event_action_timeout
            Duration::from_secs(10), // client_action_timeout
            Duration::ZERO,          // max_executing_timeout (disabled)
            test_db,
            now_fn,
            None, // worker_registry
        );
        Self {
            db,
            injected_aborts,
            state_manager,
        }
    }

    /// Seed a fresh `Queued` action and return its INTERNAL operation id
    /// (`add_action` mints a fresh `OperationId::default()` internally —
    /// `memory_awaited_action_db.rs:843` — distinct from the client id).
    async fn seed_queued(&self, seed: u8) -> OperationId {
        self.seed_queued_with_priority(seed, 0).await
    }

    /// Like `seed_queued` but with an explicit action priority — used by the
    /// SIGKILL test to give the peer a non-default priority so a MISSING
    /// `boost_priority()` is observable in the Queued sort order.
    async fn seed_queued_with_priority(&self, seed: u8, priority: i32) -> OperationId {
        let subscriber = self
            .db
            .add_action(
                OperationId::default(),
                make_action_info(seed, priority),
                Duration::from_secs(60),
            )
            .await
            .expect("seed add_action must succeed");
        let awaited = subscriber.borrow().await.expect("seed borrow must succeed");
        assert_eq!(
            awaited.state().stage,
            ActionStage::Queued,
            "seeded action must start Queued"
        );
        awaited.operation_id().clone()
    }

    /// Like `seed_queued_with_priority` but with an explicit `insert_timestamp`
    /// so the P1-1 test can distinguish equal-priority actions by insert order.
    async fn seed_queued_full(
        &self,
        seed: u8,
        priority: i32,
        insert_timestamp: SystemTime,
    ) -> OperationId {
        let subscriber = self
            .db
            .add_action(
                OperationId::default(),
                make_action_info_ts(seed, priority, insert_timestamp),
                Duration::from_secs(60),
            )
            .await
            .expect("seed add_action must succeed");
        let awaited = subscriber.borrow().await.expect("seed borrow must succeed");
        assert_eq!(
            awaited.state().stage,
            ActionStage::Queued,
            "seeded action must start Queued"
        );
        awaited.operation_id().clone()
    }

    /// Read `(attempts, stage)` for `op` directly off the shared DB. `attempts`
    /// is `pub`; this is the only read-back path observing the retry budget.
    async fn read_back(&self, op: &OperationId) -> (usize, ActionStage) {
        let subscriber = self
            .db
            .get_by_operation_id(op)
            .await
            .expect("get_by_operation_id must succeed")
            .expect("operation must exist for read-back");
        let awaited = subscriber.borrow().await.expect("borrow must succeed");
        (awaited.attempts, awaited.state().stage.clone())
    }

    async fn stage(&self, op: &OperationId) -> ActionStage {
        self.read_back(op).await.1
    }

    /// Return the current Queued operation ids in priority-DESCENDING order
    /// (queue front first) via the real
    /// `MatchingEngineStateManager::filter_operations` sort path.
    async fn queued_order_desc(&self) -> Vec<OperationId> {
        let filter = OperationFilter {
            stages: OperationStageFlags::Queued,
            order_by_priority_direction: Some(OrderDirection::Desc),
            ..OperationFilter::default()
        };
        let mut stream =
            MatchingEngineStateManager::filter_operations(self.state_manager.as_ref(), filter)
                .await
                .expect("filter_operations must succeed");
        let mut out = Vec::new();
        while let Some(result) = stream.next().await {
            // `MatchingEngineActionStateResult::as_state` surfaces the stored
            // `ActionState`; `client_operation_id` there carries the INTERNAL
            // operation id (no client rewrite for the matching-engine path — see
            // `AwaitedAction::new` at awaited_action.rs:108 and
            // `worker_set_state` at state_manager.rs:883), so use it directly.
            let (state, _) = result.as_state().await.expect("as_state must succeed");
            out.push(state.client_operation_id.clone());
        }
        out
    }
}

fn worker() -> WorkerId {
    WorkerId("test-worker".to_string())
}

/// A generic (non-backpressure, non-`FailedPrecondition`) error — the class
/// that MUST consume the attempt budget (`:838`).
fn generic_error() -> Error {
    make_err!(Code::Internal, "transient internal failure")
}

// ---------------------------------------------------------------------------
// Test 1 — RETRY-BUDGET exhaustion reaches terminal after exactly N real errors
// ---------------------------------------------------------------------------

/// A generic error retried: `attempts` increments each error
/// (`simple_scheduler_state_manager.rs:838`) and the op reaches
/// `Completed(error)` ONLY after `attempts > max_job_retries` (`:842-857`) —
/// not sooner, not infinitely.
///
/// With `max_job_retries = 3`: errors 1,2,3 requeue (`attempts` 1,2,3, stage
/// stays `Queued`); error 4 pushes `attempts = 4 > 3` and flips to
/// `Completed(error)`.
#[nativelink_test]
async fn retry_budget_exhaustion_reaches_terminal() {
    let fx = Fixture::new(0);
    let op = fx.seed_queued(1).await;

    for attempt in 1..=MAX_JOB_RETRIES {
        fx.state_manager
            .update_operation(&op, &worker(), UpdateOperationType::UpdateWithError(generic_error()))
            .await
            .expect("update_operation with generic error must not itself error");

        let (attempts, stage) = fx.read_back(&op).await;
        assert_eq!(
            attempts, attempt,
            "attempt {attempt}: attempts must increment by exactly 1 per real error \
             (retry-budget invariant, state_manager.rs:838) — got {attempts}"
        );
        assert_eq!(
            stage,
            ActionStage::Queued,
            "attempt {attempt}: action must requeue (not terminal) while attempts \
             ({attempts}) <= max_job_retries ({MAX_JOB_RETRIES})"
        );
    }

    // One more real error: attempts -> 4 > max_job_retries(3) => terminal.
    fx.state_manager
        .update_operation(&op, &worker(), UpdateOperationType::UpdateWithError(generic_error()))
        .await
        .expect("final update_operation must not itself error");

    let (attempts, stage) = fx.read_back(&op).await;
    assert_eq!(
        attempts,
        MAX_JOB_RETRIES + 1,
        "budget-exhausting error must push attempts to max_job_retries+1 ({}) — got {attempts}",
        MAX_JOB_RETRIES + 1
    );
    assert!(
        matches!(stage, ActionStage::Completed(ref r) if r.error.is_some()),
        "action must reach Completed(error) exactly when attempts ({attempts}) > \
         max_job_retries ({MAX_JOB_RETRIES}) — retry-budget terminal invariant \
         (state_manager.rs:842-857); got stage {stage:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 2a — FailedPrecondition is terminal, NOT requeued
// ---------------------------------------------------------------------------

/// `Code::FailedPrecondition` classifies as `missing_inputs`
/// (`state_manager.rs:836`) → terminal `Completed(error)` immediately on the
/// FIRST error (`:841-857`), with `attempts` bumped exactly once and NO
/// requeue. Contrast with `generic_error_is_retryable`.
#[nativelink_test]
async fn failed_precondition_is_terminal_not_requeued() {
    let fx = Fixture::new(0);
    let op = fx.seed_queued(2).await;

    fx.state_manager
        .update_operation(
            &op,
            &worker(),
            UpdateOperationType::UpdateWithError(make_err!(
                Code::FailedPrecondition,
                "missing inputs — client must correct the request"
            )),
        )
        .await
        .expect("update_operation must not itself error");

    let (attempts, stage) = fx.read_back(&op).await;
    assert_eq!(
        attempts, 1,
        "FailedPrecondition still bumps attempts once (not backpressure) — got {attempts}"
    );
    assert!(
        matches!(stage, ActionStage::Completed(ref r) if r.error.is_some()),
        "FailedPrecondition must be terminal-immediate on the FIRST error (no requeue) — \
         state_manager.rs:836,841; got stage {stage:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 2b — generic/Internal error IS retryable (stays non-terminal)
// ---------------------------------------------------------------------------

/// A generic `Code::Internal` error is retried: `attempts` bumps and the action
/// requeues to `Queued` (non-terminal) as long as `attempts <= max_job_retries`
/// (`state_manager.rs:858-860`). Direct counterpart to
/// `failed_precondition_is_terminal_not_requeued` — same input shape, opposite
/// terminality — proving the classification branch discriminates on the code.
#[nativelink_test]
async fn generic_error_is_retryable() {
    let fx = Fixture::new(0);
    let op = fx.seed_queued(3).await;

    fx.state_manager
        .update_operation(&op, &worker(), UpdateOperationType::UpdateWithError(generic_error()))
        .await
        .expect("update_operation must not itself error");

    let (attempts, stage) = fx.read_back(&op).await;
    assert_eq!(attempts, 1, "generic error bumps attempts once — got {attempts}");
    assert_eq!(
        stage,
        ActionStage::Queued,
        "a generic/Internal error under budget must REQUEUE (non-terminal) — \
         state_manager.rs:858-860; got {stage:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — backpressure (ResourceExhausted) does NOT consume the attempt budget
// ---------------------------------------------------------------------------

/// TLA+-proposed RETRY-BUDGET invariant: `Code::ResourceExhausted` is flagged
/// `due_to_backpressure` (`state_manager.rs:817`) and therefore does NOT
/// increment `attempts` (`:837-839`). Interleaving backpressure errors with
/// real errors, the action reaches terminal `Completed` after exactly
/// `max_job_retries` REAL errors — the backpressure ones never count.
#[nativelink_test]
async fn retry_budget_backpressure_does_not_consume_attempts() {
    let fx = Fixture::new(0);
    let op = fx.seed_queued(4).await;

    let backpressure = || {
        UpdateOperationType::UpdateWithError(make_err!(
            Code::ResourceExhausted,
            "scheduler backpressure — capacity temporarily exhausted"
        ))
    };

    // Fire many backpressure errors up front: none may consume the budget.
    for i in 0..10 {
        fx.state_manager
            .update_operation(&op, &worker(), backpressure())
            .await
            .expect("backpressure update_operation must not itself error");
        let (attempts, stage) = fx.read_back(&op).await;
        assert_eq!(
            attempts, 0,
            "backpressure #{i} must NOT consume the attempt budget \
             (state_manager.rs:817,837) — attempts became {attempts}"
        );
        assert_eq!(
            stage,
            ActionStage::Queued,
            "backpressure #{i} requeues without terminality — got {stage:?}"
        );
    }

    // Interleave: real error then a backpressure error. Only REAL errors count.
    let mut real_errors = 0usize;
    while real_errors < MAX_JOB_RETRIES {
        fx.state_manager
            .update_operation(&op, &worker(), UpdateOperationType::UpdateWithError(generic_error()))
            .await
            .expect("real-error update_operation must not itself error");
        real_errors += 1;
        fx.state_manager
            .update_operation(&op, &worker(), backpressure())
            .await
            .expect("interleaved backpressure must not itself error");

        let (attempts, stage) = fx.read_back(&op).await;
        assert_eq!(
            attempts, real_errors,
            "after {real_errors} real error(s) + interleaved backpressure, attempts must \
             equal the REAL-error count only — got {attempts}"
        );
        assert_eq!(
            stage,
            ActionStage::Queued,
            "still under budget ({real_errors} <= {MAX_JOB_RETRIES}) — must requeue; got {stage:?}"
        );
    }

    // The (max_job_retries + 1)th REAL error crosses the budget => terminal.
    fx.state_manager
        .update_operation(&op, &worker(), UpdateOperationType::UpdateWithError(generic_error()))
        .await
        .expect("budget-crossing update_operation must not itself error");
    let (attempts, stage) = fx.read_back(&op).await;
    assert_eq!(
        attempts,
        MAX_JOB_RETRIES + 1,
        "terminal must occur after exactly {} REAL errors regardless of how many backpressure \
         errors interleaved — got attempts {attempts}",
        MAX_JOB_RETRIES + 1
    );
    assert!(
        matches!(stage, ActionStage::Completed(ref r) if r.error.is_some()),
        "action terminal ONLY after budget of REAL errors exhausted — got {stage:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 4 — bounded version conflict still dispatches to Executing
// ---------------------------------------------------------------------------

/// TLA+-proposed bounded-conflict (strong-fairness) residual: inject
/// `N-1 < MAX_UPDATE_RETRIES(=5)` `Code::Aborted` version conflicts at the
/// `AwaitedActionDb::update_awaited_action` seam, then let the CAS succeed. The
/// `inner_update_operation` retry loop (`state_manager.rs:649`, `:899`) must
/// ride out the bounded conflicts and the assign still lands the op in
/// `Executing` (`:1285`).
///
/// A BOUNDED count is deliberate: an infinite-conflict test would correctly
/// never terminate (the loop gives up after `MAX_UPDATE_RETRIES` with an
/// `Aborted` error — the intended failure, not a dispatch).
#[nativelink_test]
async fn assign_operation_bounded_version_conflict_still_dispatches() {
    // Inject 4 aborts (MAX_UPDATE_RETRIES = 5, so attempt #5 succeeds).
    const CONFLICTS: usize = 4;
    let fx = Fixture::new(CONFLICTS);
    let op = fx.seed_queued(5).await;

    tokio::time::timeout(
        Duration::from_secs(5),
        fx.state_manager.assign_operation(&op, Ok(&worker())),
    )
    .await
    .expect("assign_operation must not hang past the 5s deadlock detector")
    .expect("assign_operation must succeed despite bounded version conflicts");

    assert_eq!(
        fx.injected_aborts.load(Ordering::SeqCst),
        CONFLICTS,
        "the retry loop must have consumed exactly {CONFLICTS} injected conflicts before \
         the CAS succeeded (state_manager.rs:899) — consumed {}",
        fx.injected_aborts.load(Ordering::SeqCst)
    );

    let stage = fx.stage(&op).await;
    assert_eq!(
        stage,
        ActionStage::Executing,
        "after {CONFLICTS} bounded conflicts + a successful CAS, assign_operation must land the \
         op in Executing (state_manager.rs:1285) — got {stage:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — SIGKILL (exit 9) bumps attempts + boosts priority + requeues
// ---------------------------------------------------------------------------

/// `exit_code == 9` (SIGKILL / OOM-killer) completion is treated as a retryable
/// infra failure (`state_manager.rs:757-786`): `attempts += 1`,
/// `boost_priority()` (moves ahead in the Queued order), and requeue to
/// `Queued` while budget permits; terminal (keeps `Completed`) once
/// `attempts > max_job_retries`.
///
/// Priority boost is observed via queue ORDER: seed a HIGHER-than-default
/// priority peer (priority 100), then the default-priority (0) SIGKILL victim.
/// The peer's priority 100 out-ranks the victim's default 0, so WITHOUT the
/// boost the victim sorts AFTER the peer in a priority-desc scan; the SIGKILL
/// `boost_priority()` (`awaited_action.rs:168` = `i32::MAX`) must vault the
/// victim AHEAD of the peer. This asymmetry is what makes a missing
/// `boost_priority()` (state_manager.rs:771) observable — with equal sort keys
/// the order would be arbitrary and the test blind to the mutation.
#[nativelink_test]
async fn sigkill_exit9_increments_attempts_and_boosts_priority() {
    let fx = Fixture::new(0);

    // A HIGHER-priority (100) peer that out-ranks the default victim until the
    // victim is boosted. Seeded FIRST.
    let peer_op = fx.seed_queued_with_priority(6, 100).await;
    // The SIGKILL victim at default priority (0).
    let victim_op = fx.seed_queued(7).await;

    // Drive the victim to Executing (a worker picked it up).
    fx.state_manager
        .assign_operation(&victim_op, Ok(&worker()))
        .await
        .expect("assign victim to Executing");
    assert_eq!(
        fx.stage(&victim_op).await,
        ActionStage::Executing,
        "victim must be Executing before the SIGKILL completion"
    );

    // Worker reports exit_code 9 (SIGKILL).
    fx.state_manager
        .update_operation(
            &victim_op,
            &worker(),
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(ActionResult {
                exit_code: 9,
                ..ActionResult::default()
            })),
        )
        .await
        .expect("SIGKILL completion update must not itself error");

    let (attempts, stage) = fx.read_back(&victim_op).await;
    assert_eq!(
        attempts, 1,
        "SIGKILL (exit 9) must bump attempts (state_manager.rs:763) — got {attempts}"
    );
    assert_eq!(
        stage,
        ActionStage::Queued,
        "SIGKILL under budget must REQUEUE (state_manager.rs:772) — got {stage:?}"
    );

    // Priority boost: in a priority-DESC Queued scan the boosted victim must
    // come out FIRST, ahead of the never-boosted peer.
    let queued_order = fx.queued_order_desc().await;
    assert!(
        queued_order.contains(&victim_op) && queued_order.contains(&peer_op),
        "both victim and peer must be Queued; got order {queued_order:?}"
    );
    let victim_pos = queued_order
        .iter()
        .position(|o| o == &victim_op)
        .expect("victim must be in queued order");
    let peer_pos = queued_order
        .iter()
        .position(|o| o == &peer_op)
        .expect("peer must be in queued order");
    assert!(
        victim_pos < peer_pos,
        "boost_priority() must move the SIGKILL victim AHEAD of the normal peer in the \
         priority-desc Queued order (state_manager.rs:771) — victim at {victim_pos}, \
         peer at {peer_pos}; order {queued_order:?}"
    );

    // Exhaust the remaining budget with more SIGKILLs, then confirm terminal.
    for _ in 0..MAX_JOB_RETRIES {
        // Re-assign to Executing so the next completion is accepted (a finished
        // action would be ignored as a late update, state_manager.rs:718).
        fx.state_manager
            .assign_operation(&victim_op, Ok(&worker()))
            .await
            .expect("re-assign victim between SIGKILLs");
        fx.state_manager
            .update_operation(
                &victim_op,
                &worker(),
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(ActionResult {
                    exit_code: 9,
                    ..ActionResult::default()
                })),
            )
            .await
            .expect("subsequent SIGKILL completion update must not itself error");
    }

    let (attempts, stage) = fx.read_back(&victim_op).await;
    assert!(
        attempts > MAX_JOB_RETRIES,
        "after enough SIGKILLs attempts must exceed max_job_retries ({MAX_JOB_RETRIES}) — \
         got {attempts}"
    );
    assert!(
        matches!(stage, ActionStage::Completed(ref r) if r.exit_code == 9),
        "once the retry budget is exhausted, a SIGKILL completion is terminal and keeps its \
         exit_code 9 (state_manager.rs:779) — got {stage:?}"
    );
}

// ---------------------------------------------------------------------------
// The original regression test (kept).
// ---------------------------------------------------------------------------

#[nativelink_test]
async fn drops_missing_actions() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let awaited_action_db = memory_awaited_action_db_factory(
        0,
        &task_change_notify.clone(),
        MockInstantWrapped::default,
    );
    let state_manager = SimpleSchedulerStateManager::new(
        0,
        Duration::from_secs(10),
        Duration::from_secs(10),
        Duration::ZERO,
        awaited_action_db,
        SystemTime::now,
        None,
    );
    state_manager
        .update_operation(
            &OperationId::Uuid(uuid::Uuid::parse_str(
                "c458c1f4-136e-486d-b9cd-cea07460cde4",
            )?),
            &WorkerId::default(),
            UpdateOperationType::ExecutionComplete,
        )
        .await
        .unwrap();

    assert!(logs_contain(
        "Unable to update action due to it being missing, probably dropped operation_id=c458c1f4-136e-486d-b9cd-cea07460cde4"
    ));
    Ok(())
}

// ---------------------------------------------------------------------------
// P1-1 — priority ordering + FIFO tiebreak (queue-front sort)
// ---------------------------------------------------------------------------

/// The Queued sort order is: (1) higher `priority` first, (2) among equal
/// priorities, the EARLIER `insert_timestamp` first (FIFO). This is the order
/// the matcher consumes (`SimpleScheduler::get_queued_operations` ->
/// `filter_operations(Queued, Desc)` -> `get_range_of_actions(Queued, ...)`),
/// so the first-served action is the queue front here.
///
/// `AwaitedActionSortKey` (awaited_action.rs:228-233,249-269) packs `priority`
/// (bias-shifted) in the high bytes and the (bit-inverted) `insert_timestamp`
/// in the low bytes, so priority dominates and, within a priority, a smaller
/// timestamp yields a larger key (earlier = higher). We read the order back
/// through the REAL `filter_operations` Desc sort path.
#[nativelink_test]
async fn queued_order_is_priority_then_fifo_by_insert_timestamp() {
    let fx = Fixture::new(0);

    // Seed OUT of final order to prove the sort, not insertion luck:
    //   - high  (priority 100, ts=200)   -> must be FIRST (top priority)
    //   - mid_late (priority 0, ts=300)  -> equal-priority peer, LATER insert
    //   - mid_early (priority 0, ts=100) -> equal-priority peer, EARLIER insert
    //   - low  (priority -50, ts=50)     -> must be LAST (lowest priority),
    //                                       even though it was inserted earliest
    let ts = |secs: u64| SystemTime::UNIX_EPOCH + Duration::from_secs(secs);
    let high = fx.seed_queued_full(0x10, 100, ts(200)).await;
    let mid_late = fx.seed_queued_full(0x11, 0, ts(300)).await;
    let mid_early = fx.seed_queued_full(0x12, 0, ts(100)).await;
    let low = fx.seed_queued_full(0x13, -50, ts(50)).await;

    let order = fx.queued_order_desc().await;

    // Priority dominates: high first, low last.
    assert_eq!(
        order.first(),
        Some(&high),
        "the highest-priority action (100) must be at the queue front — priority \
         is the primary sort key (awaited_action.rs:231); got order {order:?}"
    );
    assert_eq!(
        order.last(),
        Some(&low),
        "the lowest-priority action (-50) must be at the queue tail even though \
         it was inserted earliest — priority outranks insert order; got {order:?}"
    );

    // FIFO tiebreak among the two equal-priority (0) actions: the EARLIER
    // insert_timestamp (mid_early, ts=100) must precede the LATER (mid_late,
    // ts=300).
    let pos_early = order
        .iter()
        .position(|o| o == &mid_early)
        .expect("mid_early must be in the queued order");
    let pos_late = order
        .iter()
        .position(|o| o == &mid_late)
        .expect("mid_late must be in the queued order");
    assert!(
        pos_early < pos_late,
        "equal-priority actions must be ordered FIFO by insert_timestamp \
         (earlier first): mid_early (ts=100, pos {pos_early}) must precede \
         mid_late (ts=300, pos {pos_late}) — the second-tier sort key \
         (awaited_action.rs:232) was not honored; order {order:?}"
    );

    // Full expected order, pinned exactly.
    assert_eq!(
        order,
        vec![high.clone(), mid_early.clone(), mid_late.clone(), low.clone()],
        "queued order must be [high(p100), mid_early(p0 ts100), \
         mid_late(p0 ts300), low(p-50)] — priority-desc then insert-asc"
    );
}
