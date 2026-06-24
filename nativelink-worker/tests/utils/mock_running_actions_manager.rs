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

use std::sync::Arc;

use async_lock::Mutex;
use nativelink_error::{Error, make_input_err};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_util::action_messages::{ActionResult, OperationId};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_worker::running_actions_manager::{Metrics, RunningAction, RunningActionsManager};
use tokio::sync::mpsc;

#[derive(Debug)]
enum RunningActionManagerCalls {
    CreateAndAddAction((String, StartExecute)),
    CacheActionResult(Box<(DigestInfo, ActionResult, DigestHasherFunc)>),
}

enum RunningActionManagerReturns {
    CreateAndAddAction(Result<Arc<MockRunningAction>, Error>),
}

pub(crate) struct MockRunningActionsManager {
    rx_call: Mutex<mpsc::UnboundedReceiver<RunningActionManagerCalls>>,
    tx_call: mpsc::UnboundedSender<RunningActionManagerCalls>,

    rx_resp: Mutex<mpsc::UnboundedReceiver<RunningActionManagerReturns>>,
    tx_resp: mpsc::UnboundedSender<RunningActionManagerReturns>,

    rx_kill_all: Mutex<mpsc::UnboundedReceiver<()>>,
    tx_kill_all: mpsc::UnboundedSender<()>,

    rx_kill_operation: Mutex<mpsc::UnboundedReceiver<OperationId>>,
    tx_kill_operation: mpsc::UnboundedSender<OperationId>,
    metrics: Arc<Metrics>,

    // #O15 (2026-06-07): when set, `cache_action_result` awaits this
    // Notify BEFORE recording the call into `tx_call`. Allows tests to
    // verify the publish closure returns BEFORE the AC write completes
    // (closure-detach contract).
    cache_action_result_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    // #O15: counter of cache_action_result invocations. Used by
    // suppression tests to assert the spawn body returned early
    // without calling cache_action_result.
    cache_action_result_invocations: Arc<std::sync::atomic::AtomicU64>,
    // #O15 fix-up (2026-06-07): when set, `cache_action_result` clones
    // and returns this Err in lieu of Ok. Allows T4 to drive the
    // error! log path inside the detached AC-write spawn body.
    cache_action_result_err: Mutex<Option<Error>>,

    // (FL-681 re-saturation gate) value returned by the mocked
    // `indefinite_pin_saturated()` trait method. Lets the post-action-delta
    // regression test drive a still-saturated worker so it can assert the
    // delta reports the real value instead of a hardcoded `false`.
    indefinite_pin_saturated: std::sync::atomic::AtomicBool,

    // (#FL-688 §4 backfill retry-until-durable) when set, the
    // `get_cas_store()` trait method returns this `FastSlowStore` instead
    // of the default `None`. Lets `handle_upload_missing_blobs` run its
    // real per-blob upload fan-out against a production-composed store so
    // the W4 failed-upload re-queue contract is exercised end-to-end.
    // `std::sync::Mutex` (not the async `Mutex`) because the
    // `get_cas_store` trait method is synchronous; the store is installed
    // once before the backfill call and never contended.
    cas_store: std::sync::Mutex<Option<Arc<FastSlowStore>>>,
}

impl Default for MockRunningActionsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRunningActionsManager {
    pub(crate) fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        let (tx_kill_all, rx_kill_all) = mpsc::unbounded_channel();
        let (tx_kill_operation, rx_kill_operation) = mpsc::unbounded_channel();
        Self {
            rx_call: Mutex::new(rx_call),
            tx_call,
            rx_resp: Mutex::new(rx_resp),
            tx_resp,
            rx_kill_all: Mutex::new(rx_kill_all),
            tx_kill_all,
            rx_kill_operation: Mutex::new(rx_kill_operation),
            tx_kill_operation,
            metrics: Arc::new(Metrics::default()),
            cache_action_result_gate: Mutex::new(None),
            cache_action_result_invocations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cache_action_result_err: Mutex::new(None),
            indefinite_pin_saturated: std::sync::atomic::AtomicBool::new(false),
            cas_store: std::sync::Mutex::new(None),
        }
    }

    /// (#FL-688 §4) Install the `FastSlowStore` returned by
    /// `get_cas_store()`. The backfill retry-until-durable test composes a
    /// real FSS whose slow tier rejects writes, installs it here, then
    /// drives `handle_upload_missing_blobs` so the W4 Err arm fires against
    /// production composition.
    #[allow(dead_code, reason = "consumed by #FL-688 backfill retry test")]
    pub(crate) fn set_cas_store(&self, cas_store: Arc<FastSlowStore>) {
        *self.cas_store.lock().expect("cas_store mutex poisoned") = Some(cas_store);
    }

    /// #O15 (2026-06-07): install a `Notify` that `cache_action_result`
    /// will await BEFORE recording its call into the call channel.
    /// Triggering the Notify releases the AC write. Used by the
    /// closure-detach contract test.
    #[allow(dead_code, reason = "consumed by #O15 closure-detach test")]
    pub(crate) async fn set_cache_action_result_gate(
        &self,
        gate: Arc<tokio::sync::Notify>,
    ) {
        let mut slot = self.cache_action_result_gate.lock().await;
        *slot = Some(gate);
    }

    /// #O15 (2026-06-07): read the number of times `cache_action_result`
    /// has been invoked (counted at the start of the call, BEFORE the
    /// gate await). Cancel-suppression tests assert this stays 0.
    #[allow(dead_code, reason = "consumed by #O15 closure-detach test")]
    pub(crate) fn cache_action_result_invocations(&self) -> u64 {
        self.cache_action_result_invocations
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// #O15 fix-up (2026-06-07): install an Err that
    /// `cache_action_result` will clone-and-return in lieu of Ok.
    /// `None` reverts to Ok-by-default. Used by T4 to drive the
    /// detached spawn body's `error!` path so the assertion on
    /// `logs_contain("Error saving action in store")` exercises the
    /// real production log site rather than the trivial-Ok branch.
    #[allow(dead_code, reason = "consumed by #O15 fix-up T4")]
    pub(crate) async fn set_cache_action_result_err(&self, err: Option<Error>) {
        let mut slot = self.cache_action_result_err.lock().await;
        *slot = err;
    }

    /// (FL-681 re-saturation gate) Set the value the mocked
    /// `indefinite_pin_saturated()` trait method returns. The post-action-delta
    /// regression test sets `true` to model a still-saturated worker.
    #[allow(dead_code, reason = "consumed by FL-681 post-action-delta test")]
    pub(crate) fn set_indefinite_pin_saturated(&self, saturated: bool) {
        self.indefinite_pin_saturated
            .store(saturated, std::sync::atomic::Ordering::Release);
    }
}

impl MockRunningActionsManager {
    pub(crate) async fn expect_create_and_add_action(
        &self,
        result: Result<Arc<MockRunningAction>, Error>,
    ) -> (String, StartExecute) {
        let mut rx_call_lock = self.rx_call.lock().await;
        let RunningActionManagerCalls::CreateAndAddAction(req) = rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        else {
            panic!("Got incorrect call waiting for create_and_add_action")
        };
        self.tx_resp
            .send(RunningActionManagerReturns::CreateAndAddAction(result))
            .map_err(|_| make_input_err!("Could not send request to mpsc"))
            .unwrap();
        req
    }

    pub(crate) async fn expect_cache_action_result(
        &self,
    ) -> (DigestInfo, ActionResult, DigestHasherFunc) {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionManagerCalls::CacheActionResult(req) => *req,
            RunningActionManagerCalls::CreateAndAddAction(_) => {
                panic!("Got incorrect call waiting for cache_action_result")
            }
        }
    }

    pub(crate) async fn expect_kill_all(&self) {
        let mut rx_kill_all_lock = self.rx_kill_all.lock().await;
        rx_kill_all_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc");
    }

    pub(crate) async fn expect_kill_operation(&self) -> OperationId {
        let mut rx_kill_operation_lock = self.rx_kill_operation.lock().await;
        rx_kill_operation_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
    }
}

impl RunningActionsManager for MockRunningActionsManager {
    type RunningAction = MockRunningAction;

    async fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<Self::RunningAction>, Error> {
        self.tx_call
            .send(RunningActionManagerCalls::CreateAndAddAction((
                worker_id,
                start_execute,
            )))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionManagerReturns::CreateAndAddAction(result) => result,
        }
    }

    async fn cache_action_result(
        &self,
        action_digest: DigestInfo,
        action_result: &mut ActionResult,
        digest_function: DigestHasherFunc,
        _op_id: &OperationId,
        _worker_id: &str,
    ) -> Result<(), Error> {
        // #O15 (2026-06-07): count BEFORE the gate so cancel-suppression
        // tests can distinguish "spawn body entered cache_action_result"
        // from "spawn body returned early on cancel".
        self.cache_action_result_invocations
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        // #O15: if a gate is installed, await it. This lets the test
        // observe that the publish closure has returned (e.g. a second
        // action got accepted) BEFORE the AC write completes.
        let gate = self.cache_action_result_gate.lock().await.clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        self.tx_call
            .send(RunningActionManagerCalls::CacheActionResult(Box::new((
                action_digest,
                action_result.clone(),
                digest_function,
            ))))
            .expect("Could not send request to mpsc");
        // #O15 fix-up: if an Err was installed via
        // set_cache_action_result_err, clone-return it so the spawn
        // body hits its error! log path.
        if let Some(err) = self.cache_action_result_err.lock().await.clone() {
            return Err(err);
        }
        Ok(())
    }

    async fn kill_operation(&self, operation_id: &OperationId) -> Result<(), Error> {
        self.tx_kill_operation
            .send(operation_id.clone())
            .expect("Could not send request to mpsc");
        Ok(())
    }

    async fn kill_all(&self) {
        self.tx_kill_all
            .send(())
            .expect("Could not send request to mpsc");
    }

    fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    fn indefinite_pin_saturated(&self) -> bool {
        self.indefinite_pin_saturated
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn get_cas_store(&self) -> Option<Arc<FastSlowStore>> {
        self.cas_store
            .lock()
            .expect("cas_store mutex poisoned")
            .clone()
    }

    async fn cached_directory_digests(&self) -> Vec<DigestInfo> {
        Vec::new()
    }

    async fn all_subtree_digests(&self) -> Vec<DigestInfo> {
        Vec::new()
    }

    async fn take_pending_subtree_changes(&self) -> (Vec<DigestInfo>, Vec<DigestInfo>) {
        (Vec::new(), Vec::new())
    }
}

#[derive(Debug)]
enum RunningActionCalls {
    PrepareAction,
    Execute,
    UploadResults,
    Cleanup,
    GetFinishedResult,
}

#[derive(Debug)]
enum RunningActionReturns {
    PrepareAction(Result<Arc<MockRunningAction>, Error>),
    Execute(Result<Arc<MockRunningAction>, Error>),
    UploadResults(Result<Arc<MockRunningAction>, Error>),
    Cleanup(Result<Arc<MockRunningAction>, Error>),
    GetFinishedResult(Box<Result<ActionResult, Error>>),
}

#[derive(Debug)]
pub(crate) struct MockRunningAction {
    rx_call: Mutex<mpsc::UnboundedReceiver<RunningActionCalls>>,
    tx_call: mpsc::UnboundedSender<RunningActionCalls>,

    rx_resp: Mutex<mpsc::UnboundedReceiver<RunningActionReturns>>,
    tx_resp: mpsc::UnboundedSender<RunningActionReturns>,

    // #O15 (2026-06-07): controllable cancel flag. The publish closure
    // reads `is_cancelled()` INSIDE the spawned AC-write task; tests
    // that exercise the cancel-during-AC-write contract toggle this
    // and verify the spawn body returns early without calling
    // cache_action_result.
    cancelled: std::sync::atomic::AtomicBool,
}

impl Default for MockRunningAction {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRunningAction {
    pub(crate) fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Mutex::new(rx_call),
            tx_call,
            rx_resp: Mutex::new(rx_resp),
            tx_resp,
            cancelled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// #O15 (2026-06-07): toggle the cancel flag. Mirrors the
    /// Release-store side of
    /// `RunningActionsManagerImpl::kill_operation` so the publish
    /// closure's spawned AC-write task sees `is_cancelled() == true`
    /// via Acquire load.
    #[allow(dead_code, reason = "consumed by #O15 closure-detach test")]
    pub(crate) fn set_cancelled(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn simple_expect_get_finished_result(
        self: &Arc<Self>,
        result: Result<ActionResult, Error>,
    ) -> Result<(), Error> {
        self.expect_prepare_action(Ok(())).await?;
        self.expect_execute(Ok(())).await?;
        self.upload_results(Ok(())).await?;
        let result = self.get_finished_result(result).await;
        self.cleanup(Ok(())).await?;
        result
    }

    pub(crate) async fn expect_prepare_action(
        self: &Arc<Self>,
        result: Result<(), Error>,
    ) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::PrepareAction => (),
            req => panic!("expect_prepare_action expected PrepareAction, got : {req:?}"),
        }
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::PrepareAction(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub(crate) async fn expect_execute(
        self: &Arc<Self>,
        result: Result<(), Error>,
    ) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::Execute => (),
            req => panic!("expect_execute expected Execute, got : {req:?}"),
        }
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::Execute(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub(crate) async fn upload_results(
        self: &Arc<Self>,
        result: Result<(), Error>,
    ) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::UploadResults => (),
            req => panic!("expect_upload_results expected UploadResults, got : {req:?}"),
        }
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::UploadResults(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub(crate) async fn cleanup(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::Cleanup => (),
            req => panic!("expect_cleanup expected Cleanup, got : {req:?}"),
        }
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::Cleanup(result))
            .expect("Could not send request to mpsc");
        Ok(())
    }

    pub(crate) async fn get_finished_result(
        self: &Arc<Self>,
        result: Result<ActionResult, Error>,
    ) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionCalls::GetFinishedResult => (),
            req => panic!("expect_get_finished_result expected GetFinishedResult, got : {req:?}"),
        }
        self.tx_resp
            .send(RunningActionReturns::GetFinishedResult(Box::new(result)))
            .expect("Could not send request to mpsc");
        Ok(())
    }
}

impl RunningAction for MockRunningAction {
    fn get_operation_id(&self) -> &OperationId {
        // For testing purposes we create a static OperationId that's
        // initialized once.
        static OPERATION_ID: std::sync::OnceLock<OperationId> = std::sync::OnceLock::new();
        OPERATION_ID.get_or_init(OperationId::default)
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Acquire)
    }

    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::PrepareAction)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::PrepareAction(result) => result,
            resp => panic!("execution_response expected PrepareAction response, received {resp:?}"),
        }
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::Execute)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::Execute(result) => result,
            resp => panic!("execution_response expected Execute response, received {resp:?}"),
        }
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::UploadResults)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::UploadResults(result) => result,
            resp => panic!("execution_response expected UploadResults response, received {resp:?}"),
        }
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::Cleanup)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::Cleanup(result) => result,
            resp => panic!("execution_response expected Cleanup response, received {resp:?}"),
        }
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        self.tx_call
            .send(RunningActionCalls::GetFinishedResult)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            RunningActionReturns::GetFinishedResult(result) => *result,
            resp => {
                panic!("execution_response expected GetFinishedResult response, received {resp:?}")
            }
        }
    }

    fn get_work_directory(&self) -> &String {
        unreachable!();
    }
}
