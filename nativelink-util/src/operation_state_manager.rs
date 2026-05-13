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

use core::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bitflags::bitflags;
use futures::Stream;
use nativelink_error::Error;
use nativelink_metric::MetricsComponent;

use crate::action_messages::{
    ActionInfo, ActionStage, ActionState, ActionUniqueKey, OperationId, WorkerId,
};
use crate::common::DigestInfo;
use crate::known_platform_property_provider::KnownPlatformPropertyProvider;
use crate::origin_event::OriginMetadata;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OperationStageFlags: u32 {
        const CacheCheck = 1 << 1;
        const Queued     = 1 << 2;
        const Executing  = 1 << 3;
        const Completed  = 1 << 4;
        const Any        = u32::MAX;
    }
}

impl Default for OperationStageFlags {
    fn default() -> Self {
        Self::Any
    }
}

#[async_trait]
pub trait ActionStateResult: Send + Sync + 'static {
    /// Provides the current state of the action.
    async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error>;
    /// Waits for the state of the action to change.
    async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error>;
    /// Provide result as action info. This behavior will not be supported by all implementations.
    async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error>;
}

/// The direction in which the results are ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// The filters used to query operations from the state manager.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationFilter {
    // TODO(palfrey): create rust builder pattern?
    /// The stage(s) that the operation must be in.
    pub stages: OperationStageFlags,

    /// The client operation id.
    pub client_operation_id: Option<OperationId>,

    /// The operation id.
    pub operation_id: Option<OperationId>,

    /// The worker that the operation must be assigned to.
    pub worker_id: Option<WorkerId>,

    /// The digest of the action that the operation must have.
    pub action_digest: Option<DigestInfo>,

    /// The operation must have its worker timestamp before this time.
    pub worker_update_before: Option<SystemTime>,

    /// The operation must have been completed before this time.
    pub completed_before: Option<SystemTime>,

    /// The unique key for filtering specific action results.
    pub unique_key: Option<ActionUniqueKey>,

    /// If the results should be ordered by priority and in which direction.
    pub order_by_priority_direction: Option<OrderDirection>,
}

pub type ActionStateResultStream<'a> =
    Pin<Box<dyn Stream<Item = Box<dyn ActionStateResult>> + Send + 'a>>;

#[async_trait]
pub trait ClientStateManager: Sync + Send + Unpin + MetricsComponent + 'static {
    /// Add a new action to the queue or joins an existing action.
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error>;

    /// Returns a stream of operations that match the filter.
    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error>;

    /// Routes a cancellation request for `operation_id` to the worker
    /// running it (if any). Called from two arrival points:
    /// - explicit `cancel_operation` RPC (`execution_server.rs`)
    /// - stream-drop guard on the Execute / `WaitExecution` response
    ///   stream (`ExecuteStreamCancelGuard`)
    ///
    /// Idempotent: an unknown / already-finished operation returns
    /// `Ok(())`. Concrete schedulers that own the worker pool override
    /// to dispatch `KillOperationRequest`. Pure-proxy wrappers
    /// (`PropertyModifierScheduler`, `CacheLookupScheduler`,
    /// `GrpcScheduler`) forward to their inner scheduler. The default
    /// no-op is the safe fallback for any future implementor.
    ///
    /// `operation_id` may be either a CLIENT operation_id (the value
    /// surfaced as `Operation.name` to Bazel) or an INTERNAL
    /// operation_id (the matching engine's id used to key worker
    /// `running_action_infos`). `SimpleScheduler::cancel_operation`
    /// uses `client_operation_id_to_operation_id` to translate the
    /// former to the latter and falls through to the latter
    /// unchanged. This is what makes the AC-poisoning fix's
    /// `ExecuteStreamCancelGuard` (which captures the client form,
    /// because that's what the streaming RPC handler has) reach the
    /// worker.
    ///
    /// AC-poisoning fix (composite invariant): a cancel signal
    /// arriving via this path before the worker's
    /// `cache_action_result` issues `update_action_result` MUST
    /// suppress the AC write. See base design Phase D for the full
    /// invariant statement.
    async fn cancel_operation(&self, _operation_id: &OperationId) -> Result<(), Error> {
        Ok(())
    }

    /// Translate a CLIENT operation_id (the value surfaced as
    /// `Operation.name` to Bazel) into the INTERNAL operation_id
    /// (the matching engine's id used to key worker `running_action_infos`).
    /// Returns `Ok(None)` if no action with this client_operation_id
    /// is known to this scheduler. The default returns `Ok(None)` so
    /// proxy wrappers without action-db access can rely on the
    /// trait's default. `SimpleSchedulerStateManager` overrides to
    /// query its inner `AwaitedActionDb`.
    ///
    /// Used by `SimpleScheduler::cancel_operation` to resolve
    /// Bazel-issued cancellations (which carry the client form). If
    /// translation succeeds, the internal id is forwarded to
    /// `worker_scheduler.cancel_operation_internal`; if it returns
    /// `None`, the caller's `operation_id` is forwarded as-is (the
    /// caller may have passed an internal id directly, e.g.
    /// `cancel_operation_routing_test`).
    async fn client_operation_id_to_operation_id(
        &self,
        _client_operation_id: &OperationId,
    ) -> Result<Option<OperationId>, Error> {
        Ok(None)
    }

    /// Returns the known platform property provider for the given instance
    /// if this implementation supports it.
    // TODO(https://github.com/rust-lang/rust/issues/65991) When this lands we can
    // remove this and have the call sites instead try to cast the ClientStateManager
    // into a KnownPlatformPropertyProvider instead. Rust currently does not support
    // casting traits to other traits.
    fn as_known_platform_property_provider(&self) -> Option<&dyn KnownPlatformPropertyProvider>;
}

/// The type of update to perform on an operation.
#[derive(Debug, PartialEq, Clone)]
#[allow(
    clippy::large_enum_variant,
    reason = "TODO Fix this. Breaks on stable, but not on nightly"
)]
pub enum UpdateOperationType {
    /// Notification that the operation is still alive.
    KeepAlive,

    /// Notification that the operation has been updated.
    UpdateWithActionStage(ActionStage),

    /// Notification that the operation has been completed.
    UpdateWithError(Error),

    /// Notification that the worker disconnected.
    UpdateWithDisconnect,

    /// Notification that the execution stage has completed and it's just IO happening now.
    ExecutionComplete,
}

#[async_trait]
pub trait WorkerStateManager: Sync + Send + MetricsComponent {
    /// Update that state of an operation.
    /// The worker must also send periodic updates even if the state
    /// did not change with a modified timestamp in order to prevent
    /// the operation from being considered stale and being rescheduled.
    async fn update_operation(
        &self,
        operation_id: &OperationId,
        worker_id: &WorkerId,
        update: UpdateOperationType,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait MatchingEngineStateManager: Sync + Send + MetricsComponent {
    /// Returns a stream of operations that match the filter.
    async fn filter_operations<'a>(
        &'a self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'a>, Error>;

    /// Assign an operation to a worker or unassign it.
    async fn assign_operation(
        &self,
        operation_id: &OperationId,
        worker_id_or_reason_for_unassign: Result<&WorkerId, Error>,
    ) -> Result<(), Error>;
}
