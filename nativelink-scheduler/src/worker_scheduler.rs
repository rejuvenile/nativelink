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

use std::collections::HashSet;

use async_trait::async_trait;
use nativelink_error::Error;
use nativelink_metric::RootMetricsComponent;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::UpdateOperationType;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::store_trait::Store;

use crate::platform_property_manager::PlatformPropertyManager;
use crate::worker::{Worker, WorkerTimestamp};

/// `WorkerScheduler` interface is responsible for interactions between the scheduler
/// and worker related operations.
#[async_trait]
pub trait WorkerScheduler: Sync + Send + Unpin + RootMetricsComponent + 'static {
    /// Returns the platform property manager.
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager;

    /// Adds a worker to the scheduler and begin using it to execute actions (when able).
    async fn add_worker(&self, worker: Worker) -> Result<(), Error>;

    /// Updates the status of an action to the scheduler from the worker.
    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<(), Error>;

    /// Event for when the keep alive message was received from the worker.
    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error>;

    /// Removes worker from pool and reschedule any tasks that might be running on it.
    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<(), Error>;

    /// Evict all workers from the scheduler, setting their actions back to queued.
    async fn shutdown(&self, shutdown_guard: ShutdownGuard);

    /// Removes timed out workers from the pool. This is called periodically by an
    /// external source.
    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error>;

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error>;

    /// Updates the CPU load reported by a worker.
    /// `cpu_load_pct` is aggregate load (0-100). 0 means unknown.
    /// `p_core_load_pct` and `e_core_load_pct` are per-core-type loads
    /// on heterogeneous CPUs (Apple Silicon). 0 means unknown.
    async fn update_worker_load(
        &self,
        worker_id: &WorkerId,
        cpu_load_pct: u32,
        p_core_load_pct: u32,
        e_core_load_pct: u32,
    ) -> Result<(), Error>;

    /// (FL-681 re-saturation gate) Updates whether the worker's local CAS
    /// FilesystemStore reported its indefinite-pin cap saturated. Carried on
    /// every `BlobsAvailable` the worker sends (NOT KeepAlive — only the
    /// BlobsAvailable path queries the FilesystemStore): both the periodic
    /// heartbeat AND the one-shot post-action delta report the authoritative
    /// value, so a saturated worker's flag is not clobbered to `false` the
    /// instant an action completes. When `true`, the matcher skips this worker
    /// for new actions so a saturated-but-idle worker is not re-dispatched into
    /// the worker-NAK → re-queue → re-dispatch spin. Default impl is a no-op so
    /// schedulers that do not run a worker pool (or do not care about F2
    /// saturation) need not implement it.
    async fn update_worker_indefinite_pin_saturation(
        &self,
        _worker_id: &WorkerId,
        _indefinite_pin_saturated: bool,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Updates the set of cached directory digests for a worker.
    /// The scheduler uses this to give routing preference to workers that
    /// already have the action's input_root_digest cached in their directory cache.
    async fn update_cached_directories(
        &self,
        worker_id: &WorkerId,
        digests: HashSet<DigestInfo>,
    ) -> Result<(), Error>;

    /// Updates the set of cached subtree digests for a worker using delta encoding.
    ///
    /// When `is_full_snapshot` is true, `full_set` replaces the entire set.
    /// When `is_full_snapshot` is false, `added` digests are inserted and
    /// `removed` digests are deleted from the existing set.
    async fn update_cached_subtrees(
        &self,
        worker_id: &WorkerId,
        is_full_snapshot: bool,
        full_set: Vec<DigestInfo>,
        added: Vec<DigestInfo>,
        removed: Vec<DigestInfo>,
    ) -> Result<(), Error>;

    /// Broadcast a `BlobsInStableStorage` notification to all connected workers,
    /// telling them that the given digests are now safe on stable storage and can
    /// be unpinned from local CAS. Default implementation is a no-op.
    async fn broadcast_blobs_in_stable_storage(&self, _digests: Vec<DigestInfo>) {}

    /// (#97) Chunked variant of `broadcast_blobs_in_stable_storage`.
    ///
    /// `store_id` tags each chunk with the source store so workers can
    /// route AC chunks (`store_id = "AC_MAIN_STORE"` etc.) to AC pin
    /// drains separately from CAS chunks (empty string = CAS, the
    /// historic single-store wire shape preserved for forward compat).
    ///
    /// **No default implementation** is provided on purpose: the only
    /// behaviorally-correct fallback would be `broadcast_blobs_in_stable_storage(digests)`,
    /// which silently DROPS `store_id`. A future scheduler that omits
    /// this method while having an AC store wired would route AC chunks
    /// to the CAS handler (Option-A review distributed-systems MINOR-2).
    /// Forcing the override surfaces the decision at compile time:
    /// implementors must either honor `store_id` (production
    /// `ApiWorkerScheduler`), forward to a delegate that does
    /// (`SimpleScheduler`), or explicitly NO-OP / panic with a
    /// bespoke message documenting that the scheduler does not support
    /// AC broadcasts.
    async fn broadcast_blobs_in_stable_storage_chunked(
        &self,
        digests: Vec<DigestInfo>,
        store_id: &str,
    );

    /// (#97) Notify the scheduler that a worker has acked one BIS chunk.
    /// The scheduler drops the matching `(broadcast_id, sequence)` from
    /// its per-worker resend buffer if and only if `server_instance_token`
    /// matches the scheduler's current token (red-team #5: stale acks
    /// across server bounces would otherwise drop unrelated chunks).
    /// Default impl is a no-op.
    async fn bis_ack_received(
        &self,
        _worker_id: &WorkerId,
        _broadcast_id: u64,
        _sequence: u32,
        _server_instance_token: u64,
    ) {
    }

    /// (#97) Drop the BIS resend buffer for a worker's `cas_endpoint`.
    /// Called on `boot_epoch_id` change so the new worker process — which
    /// has fresh pin state — doesn't get bombarded with replays for
    /// digests that no longer exist in its CAS. Default impl is a no-op.
    async fn clear_bis_resend_buffer_for_endpoint(&self, _cas_endpoint: &str) {}

    /// Returns the captured `cas_store` Arc the scheduler uses for tree
    /// resolution (see `resolve_tree_from_cas`). Default impl returns
    /// `None`. The production `ApiWorkerScheduler` overrides this to
    /// expose its internal handle so wiring tests can verify that the
    /// scheduler holds the `WorkerProxyStore`-WRAPPED chain (with
    /// peer-fetch fallback) rather than the raw chain. See #261 for
    /// the bug where `scheduler_factory` ran BEFORE the wrap and the
    /// scheduler captured the unwrapped clone, surfacing NotFound for
    /// tiny Directory blobs that lived only on a peer worker.
    fn cas_store(&self) -> Option<&Store> {
        None
    }
}
