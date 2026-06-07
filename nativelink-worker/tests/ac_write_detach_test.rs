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

//! #O15 (2026-06-07): tests for the publish-closure AC-write detach.
//!
//! Production composition: publish closure (`LocalWorkerImpl::run`'s
//! `make_publish_future`) → `tokio::spawn(ac_write_task)` →
//! `RunningActionsManager::cache_action_result` → (mock) AC store.
//!
//! Each test installs a `Notify` gate on the mock's
//! `cache_action_result` so the test can observe what happens to the
//! publish closure WHILE the AC write is suspended mid-call. The
//! closure-detach contract is: the closure MUST return before the AC
//! write completes; the production signal is "the worker accepts the
//! next StartAction even though the prior AC write is still pending."
//!
//! All four tests wrap a `tokio::time::timeout(5s)` outer deadlock
//! detector and assert via bespoke red-fail strings. Each names a
//! mutation under the relevant test.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use hyper::body::Frame;
use nativelink_error::{Error, make_input_err};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Platform;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, StartExecute, UpdateForWorker,
};
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionUniqueKey, ActionUniqueQualifier,
};
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::digest_hasher::DigestHasherFunc;
use tokio::sync::Notify;
use utils::local_worker_test_utils::{setup_local_worker, setup_local_worker_with_ac_cap};
use utils::mock_running_actions_manager::MockRunningAction;

const INSTANCE_NAME: &str = "foo";

fn make_action_info(digest_byte: u8) -> ActionInfo {
    let action_digest = DigestInfo::new([digest_byte; 32], 10);
    ActionInfo {
        command_digest: DigestInfo::new([1u8; 32], 10),
        input_root_digest: DigestInfo::new([2u8; 32], 10),
        timeout: Duration::from_secs(1),
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: action_digest,
        }),
    }
}

async fn send_start_action(
    tx_stream: &tokio::sync::mpsc::Sender<Frame<bytes::Bytes>>,
    worker_id: &str,
    action_info: &ActionInfo,
    op_id: &str,
) -> Result<(), Error> {
    tx_stream
        .send(Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::StartAction(StartExecute {
                    execute_request: Some(action_info.into()),
                    operation_id: op_id.to_string(),
                    queued_timestamp: None,
                    platform: Some(Platform::default()),
                    worker_id: worker_id.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                })),
            })
            .unwrap(),
        ))
        .await
        .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    Ok(())
}

/// #O15 T1 (2026-06-07): closure-detach contract — the publish closure
/// returns BEFORE `cache_action_result` completes. Production seam
/// crossed: `LocalWorkerImpl::run` publish closure → `tokio::spawn`
/// AC-write task → `RunningActionsManager::cache_action_result`.
///
/// Wall-clock signal: with `cache_action_result` blocked on a Notify
/// gate, the `trace!(tag = "publish_closure_returned", ...)` probe at
/// the END of the publish closure body must fire WHILE the gate is
/// still held. If the closure awaited the AC write inline, the probe
/// would not fire until the gate is released (which the test does
/// only AFTER asserting the probe has fired).
///
/// Mutation: revert `tokio::spawn(...)` to inline `.await` on
/// `cache_action_result` at `local_worker.rs:~2740-2820`. This test
/// must red-fail with the bespoke "closure waited for AC write" panic.
#[nativelink_test]
async fn closure_returns_before_ac_write_completes() -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut test_context = setup_local_worker(HashMap::new()).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();
        test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;

        let worker_id = "o15_t1_worker".to_string();
        let tx_stream = test_context.maybe_tx_stream.take().unwrap();
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;

        // Install the gate BEFORE driving the first action so the
        // detached AC-write task hits the gate.
        let gate = Arc::new(Notify::new());
        test_context
            .actions_manager
            .set_cache_action_result_gate(gate.clone())
            .await;

        // Drive action #1 through its full lifecycle.
        let action_info1 = make_action_info(0x10);
        send_start_action(&tx_stream, &worker_id, &action_info1, "o15-op-1").await?;

        let running_action1 = Arc::new(MockRunningAction::new());
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action1.clone()))
            .await;
        running_action1
            .simple_expect_get_finished_result(Ok(ActionResult::default()))
            .await?;
        // Drain the gRPC mock's execution_response so the publish
        // closure can proceed past the response send.
        test_context.client.expect_execution_response(Ok(())).await;

        // Poll for the closure-returned probe WHILE the gate is
        // still held. With detach, the closure body completes after
        // spawn dispatch and the probe fires immediately. With
        // inline `.await`, the closure is stuck awaiting the gated
        // cache_action_result and the probe does NOT fire.
        let mut attempts = 0;
        while !logs_contain("publish_closure_returned") {
            attempts += 1;
            if attempts > 300 {
                panic!(
                    "closure waited for AC write: T1 closure-detach contract \
                     violated — publish_closure_returned probe did not fire \
                     within 3s while ac_store was gated; closure must be \
                     detached at local_worker.rs:~2740-2820"
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Release the gate so the detached AC write for #1 can
        // complete and the recorded call lands on the channel.
        gate.notify_waiters();
        // Drain the recorded cache_action_result call for #1.
        let _ = test_context
            .actions_manager
            .expect_cache_action_result()
            .await;
        Ok::<_, Error>(())
    })
    .await
    .unwrap_or_else(|_| panic!(
        "closure waited for AC write: T1 closure-detach contract violated — \
         publish_closure_returned probe did not fire within 5s; \
         publish closure at local_worker.rs:~2740-2820 must spawn the AC \
         write into a background task and return immediately"
    ))?;
    Ok(())
}

/// #O15 T2 (2026-06-07): the AC write still happens after detach.
/// Over-action coverage: a detached spawn that never fires would
/// silently lose AC writes. Production seam: closure → spawn →
/// cache_action_result.
///
/// Mutation: comment out the `cache_action_result(...).await` call
/// inside the `tokio::spawn` body at `local_worker.rs:~2790-2810`.
/// This test must red-fail with the bespoke "detached AC write never
/// landed" panic.
#[nativelink_test]
async fn detached_ac_write_still_lands() -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut test_context = setup_local_worker(HashMap::new()).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();
        test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;

        let worker_id = "o15_t2_worker".to_string();
        let tx_stream = test_context.maybe_tx_stream.take().unwrap();
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;

        let action_info = make_action_info(0x30);
        send_start_action(&tx_stream, &worker_id, &action_info, "o15-op-t2").await?;
        let running_action = Arc::new(MockRunningAction::new());
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action.clone()))
            .await;
        running_action
            .simple_expect_get_finished_result(Ok(ActionResult::default()))
            .await?;
        test_context.client.expect_execution_response(Ok(())).await;

        // Assert the AC write actually happens (detached spawn must
        // call cache_action_result). expect_cache_action_result
        // blocks until the call lands on the mock channel.
        let (stored_digest, _stored_result, _digest_hasher) = test_context
            .actions_manager
            .expect_cache_action_result()
            .await;
        assert_eq!(
            stored_digest.packed_hash().to_string(),
            DigestInfo::new([0x30u8; 32], 10).packed_hash().to_string(),
            "AC write digest mismatch after detach"
        );
        Ok::<_, Error>(())
    })
    .await
    .unwrap_or_else(|_| panic!(
        "detached AC write never landed: T2 closure-detach contract \
         violated — cache_action_result was not invoked after publish \
         closure returned; spawn body at local_worker.rs:~2780-2820 \
         must call running_actions_manager.cache_action_result"
    ))?;
    Ok(())
}

/// #O15 T3 (2026-06-07): cancel arriving during the AC write window
/// suppresses the write. Composite invariant Phase D (AC-poisoning
/// residual-window guard) preserved across the detach refactor: the
/// is_cancelled() check moved INSIDE the spawn so a kill arriving
/// AFTER spawn dispatch but BEFORE cache_action_result still
/// suppresses.
///
/// Seam: closure → spawn → is_cancelled() Acquire → early return.
///
/// Mutation: remove the `if ac_write_action_for_publish.is_cancelled() {
/// ... return; }` block inside the spawn body at `local_worker.rs:~2790`.
/// This test must red-fail with the bespoke "cancel during detached AC
/// write did not suppress" panic.
#[nativelink_test]
async fn cancel_during_detached_ac_write_suppresses() -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut test_context = setup_local_worker(HashMap::new()).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();
        test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;

        let worker_id = "o15_t3_worker".to_string();
        let tx_stream = test_context.maybe_tx_stream.take().unwrap();
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;

        let action_info = make_action_info(0x40);
        send_start_action(&tx_stream, &worker_id, &action_info, "o15-op-t3").await?;
        let running_action = Arc::new(MockRunningAction::new());
        // Set cancelled BEFORE the publish closure spawns the
        // AC-write task. The captured Arc reads is_cancelled()
        // INSIDE the spawn so the load sees true.
        running_action.set_cancelled();
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action.clone()))
            .await;
        running_action
            .simple_expect_get_finished_result(Ok(ActionResult::default()))
            .await?;
        test_context.client.expect_execution_response(Ok(())).await;

        // Drive a SECOND action so the test can reach a steady
        // state where we know action #1's publish closure has run
        // its spawn body. expect_create_and_add_action for #2 is
        // the synchronization barrier — by the time it returns,
        // the worker has pulled the next Update::StartAction off
        // the stream, which means the prior publish closure
        // returned (which means the spawn body for #1 has been
        // scheduled). expect_cache_action_result for #2 then
        // proves the cancel for #1 suppressed without recording
        // a cache_action_result call.
        let action_info2 = make_action_info(0x41);
        send_start_action(&tx_stream, &worker_id, &action_info2, "o15-op-t3b").await?;
        let running_action2 = Arc::new(MockRunningAction::new());
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action2.clone()))
            .await;
        running_action2
            .simple_expect_get_finished_result(Ok(ActionResult::default()))
            .await?;
        test_context.client.expect_execution_response(Ok(())).await;

        // Drain #2's AC write (which IS expected to happen — #2
        // was not cancelled). After this returns, the test knows
        // #1's spawn body either suppressed (correct) or also
        // recorded a CAS call which would have been dequeued FIRST
        // (since the mpsc is FIFO and #1's spawn started before #2's).
        let (stored_digest, _, _) = test_context
            .actions_manager
            .expect_cache_action_result()
            .await;
        assert_eq!(
            stored_digest.packed_hash().to_string(),
            DigestInfo::new([0x41u8; 32], 10).packed_hash().to_string(),
            "cancel during detached AC write did not suppress: \
             expected #2's digest [0x41;32] but got {:?} — #1's spawn \
             body fired cache_action_result despite is_cancelled() == true",
            stored_digest,
        );

        // Belt-and-suspenders: the suppression counter must reflect #1.
        // Note: the inner counter is on the trait's
        // `cache_action_result` invocations; we instead verify that
        // only one cache_action_result was invoked (for #2).
        let invocations = test_context
            .actions_manager
            .cache_action_result_invocations();
        assert_eq!(
            invocations, 1,
            "cancel during detached AC write did not suppress: \
             invocations counter = {invocations}, expected 1 (only #2). \
             #1's spawn must early-return on is_cancelled() == true"
        );
        Ok::<_, Error>(())
    })
    .await
    .unwrap_or_else(|_| panic!(
        "cancel during detached AC write did not suppress: T3 \
         residual-window guard removed during O15 detach refactor; \
         the is_cancelled() check at local_worker.rs:~2790 (INSIDE \
         the spawn body) must early-return when the captured Arc \
         reads cancelled=true"
    ))?;
    Ok(())
}

/// #O15 T4 (2026-06-07): an error inside the detached task logs at
/// error! level and does NOT poison the publish closure. The closure
/// returned Ok before the spawn body ran; an AC-write failure must
/// not propagate to the closure result. Seam: closure → spawn →
/// cache_action_result Err → error! macro at `local_worker.rs:~2806`.
///
/// Fix-up (2026-06-07): driven via
/// `MockRunningActionsManager::set_cache_action_result_err` so the
/// mock returns Err and the spawn body's actual `error!` site fires.
/// The assertion checks both `logs_contain("Error saving action in
/// store")` (the literal message string) AND
/// `logs_contain("ac_write_action_digest")` (a field emitted only on
/// the error! path) to bind the assertion to the real production log
/// site rather than any incidental debug log.
///
/// Mutation stamp 2026-06-07: swap `error!` → `debug!` at
/// `local_worker.rs:~2806`. This test must red-fail with the bespoke
/// "AC write Err did not log at error! level" panic.
#[nativelink_test]
async fn error_inside_detached_task_logs_at_error_level() -> Result<(), Error> {
    use nativelink_error::{Code, make_err};

    tokio::time::timeout(Duration::from_secs(5), async {
        let mut test_context = setup_local_worker(HashMap::new()).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();
        test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;

        let worker_id = "o15_t4_worker".to_string();
        let tx_stream = test_context.maybe_tx_stream.take().unwrap();
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;

        // Install a known Err BEFORE the publish closure spawns the
        // AC-write task. The spawn body sees `cache_action_result`
        // return Err and must hit the `error!` log site.
        let injected_err = make_err!(Code::Internal, "T4 injected ac-write failure");
        test_context
            .actions_manager
            .set_cache_action_result_err(Some(injected_err))
            .await;

        let action_info = make_action_info(0x50);
        send_start_action(&tx_stream, &worker_id, &action_info, "o15-op-t4").await?;
        let running_action = Arc::new(MockRunningAction::new());
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action.clone()))
            .await;
        running_action
            .simple_expect_get_finished_result(Ok(ActionResult::default()))
            .await?;
        test_context.client.expect_execution_response(Ok(())).await;

        // Drain the recorded cache_action_result call so we know the
        // spawn body ran and the Err propagated through the mock.
        // The mock records the call BEFORE returning Err, so this
        // resolves regardless of the injected error.
        let _ = test_context
            .actions_manager
            .expect_cache_action_result()
            .await;

        // Poll for the required log fragments. All three must be
        // present on the error! branch in local_worker.rs:~2806:
        //   - "Error saving action in store" (message literal)
        //   - "ac_write_action_digest" (field emitted only on err
        //     branch)
        //   - "ERROR" (level prefix from `tracing_test::traced_test`
        //     formatter — `tracing_test` emits the level uppercase
        //     in each captured line; mutating error! → debug! drops
        //     this prefix to "DEBUG" and the assertion red-fails).
        let mut attempts = 0;
        loop {
            let got_msg = logs_contain("Error saving action in store");
            let got_field = logs_contain("ac_write_action_digest");
            let got_level = logs_contain("ERROR");
            if got_msg && got_field && got_level {
                break;
            }
            attempts += 1;
            if attempts > 300 {
                panic!(
                    "AC write Err did not log at error! level: T4 \
                     closure-detach error-path contract violated — \
                     expected (msg=\"Error saving action in store\", \
                     field=\"ac_write_action_digest\", level=\"ERROR\") \
                     within 3s after spawn body returned Err but got \
                     (msg={got_msg}, field={got_field}, level={got_level}). \
                     error! site at local_worker.rs:~2806 must fire on \
                     cache_action_result Err with the \
                     ac_write_action_digest field at error! level (NOT \
                     debug!/warn!/info!/trace!)."
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, Error>(())
    })
    .await
    .unwrap_or_else(|_| panic!(
        "AC write Err did not log at error! level: T4 closure-detach \
         contract violated within 5s — spawn body at local_worker.rs:~2790-2820 \
         must (a) call cache_action_result, (b) log Err at error! level \
         with ac_write_action_digest field, (c) not poison the publish closure result"
    ))?;
    Ok(())
}

/// #O15 T5 fix-up (2026-06-07): cap-saturated path logs warn and
/// skips the AC write synchronously. F1 over-cap behavior: when
/// `try_acquire_owned` fails on the
/// `AC_WRITE_DETACHED_INFLIGHT_CAP` semaphore, the publish closure
/// must log
/// `warn!(operation_id, "AC write detached-spawn cap reached; AC
/// entry will be retried on next action ingress via cache-miss
/// recovery")` AND skip the `tokio::spawn` entirely — graceful
/// degradation, no closure-blocking acquire.
///
/// Forced saturation: `setup_local_worker_with_ac_cap(.., 0)`
/// installs `Semaphore::new(0)`, so the first action's publish
/// closure hits the over-cap branch on the very first attempt.
///
/// Mutation stamp 2026-06-07: remove the
/// `Arc::clone(&self.ac_write_semaphore).try_acquire_owned()` guard
/// at `local_worker.rs:~2780` (replace with unconditional spawn).
/// This test must red-fail with the bespoke "cap-saturated path did
/// not warn or did not skip the AC write" panic.
#[nativelink_test]
async fn cap_saturated_logs_warn_and_skips_ac_write() -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(5), async {
        // Force the cap to 0 so the very first detached AC write hits
        // the over-cap branch.
        let mut test_context = setup_local_worker_with_ac_cap(HashMap::new(), 0).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();
        test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;

        let worker_id = "o15_t5_worker".to_string();
        let tx_stream = test_context.maybe_tx_stream.take().unwrap();
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;

        let action_info = make_action_info(0x60);
        send_start_action(&tx_stream, &worker_id, &action_info, "o15-op-t5").await?;
        let running_action = Arc::new(MockRunningAction::new());
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action.clone()))
            .await;
        running_action
            .simple_expect_get_finished_result(Ok(ActionResult::default()))
            .await?;
        test_context.client.expect_execution_response(Ok(())).await;

        // Drive a SECOND action to synchronize: by the time
        // expect_create_and_add_action for #2 returns, the worker has
        // pulled the next Update::StartAction off the stream, which
        // means #1's publish closure has already run its body
        // (including the cap-saturation log + skip). #2 will also
        // hit the over-cap branch, so its AC write should ALSO be
        // skipped — which we use to confirm `cache_action_result`
        // was never called.
        let action_info2 = make_action_info(0x61);
        send_start_action(&tx_stream, &worker_id, &action_info2, "o15-op-t5b").await?;
        let running_action2 = Arc::new(MockRunningAction::new());
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action2.clone()))
            .await;
        running_action2
            .simple_expect_get_finished_result(Ok(ActionResult::default()))
            .await?;
        test_context.client.expect_execution_response(Ok(())).await;

        // Assertion 1: the over-cap warn fired.
        let mut attempts = 0;
        while !logs_contain("AC write detached-spawn cap reached") {
            attempts += 1;
            if attempts > 300 {
                panic!(
                    "cap-saturated path did not warn or did not skip the \
                     AC write: T5 over-cap log absent within 3s — \
                     publish closure at local_worker.rs:~2780 must call \
                     `try_acquire_owned()` on the AC-write semaphore and \
                     emit warn!(\"AC write detached-spawn cap reached; \
                     AC entry will be retried on next action ingress via \
                     cache-miss recovery\") when the cap is saturated."
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Assertion 2: cache_action_result was NEVER invoked because
        // BOTH actions hit the over-cap skip. (cap=0 makes both #1
        // and #2 fail try_acquire_owned.)
        let invocations = test_context
            .actions_manager
            .cache_action_result_invocations();
        assert_eq!(
            invocations, 0,
            "cap-saturated path did not warn or did not skip the AC write: \
             invocations counter = {invocations}, expected 0 (cap=0 must \
             skip ALL AC writes). publish closure must `return` after the \
             warn! without spawning the AC-write task."
        );
        Ok::<_, Error>(())
    })
    .await
    .unwrap_or_else(|_| panic!(
        "cap-saturated path did not warn or did not skip the AC write: \
         T5 over-cap contract violated within 5s — publish closure must \
         `try_acquire_owned()` BEFORE spawn, log warn! on Err, and skip \
         the AC write synchronously without blocking the closure."
    ))?;
    Ok(())
}
