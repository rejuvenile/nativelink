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

use async_trait::async_trait;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, StoreDriver,
    StoreKey, StoreOptimizations, UploadSizeInfo,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopStore;

impl MetricsComponent for NoopStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl NoopStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl StoreDriver for NoopStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for result in results.iter_mut() {
            *result = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        // We need to drain the reader to avoid the writer complaining that we dropped
        // the connection prematurely.
        reader.drain().await.err_tip(|| "In NoopStore::update")?;
        Ok(())
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        optimization == StoreOptimizations::NoopUpdates
            || optimization == StoreOptimizations::NoopDownloads
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // CONTRACT EXCEPTION (intentionally NOT calling
        // `_writer.send_error`): NoopStore is the canonical "skip me"
        // fast tier for `FastSlowStore` tests (see
        // `fast_slow_store.rs:3275-3307`'s `optimized_for(NoopUpdates)`
        // bypass branch). FastSlowStore calls
        // `fast_store.get_part(&mut *guard, ...)` UNCONDITIONALLY before
        // checking the bypass flag in some code paths
        // (line ~3084-3151). When the fast store is NoopStore, that call
        // returns `Err(NotFound)` and FastSlowStore falls through to the
        // slow store. If NoopStore had called `writer.send_error(NotFound)`
        // first, the buf_channel's `terminal_error` `OnceLock` would be
        // set to "Not found in noop store" — and the later
        // `commit_with_inner_miss_gate` → `guard.fail(slow_store_err)`
        // would be a silent no-op (`OnceLock::set` returns Err if already
        // set). The downstream reader would observe the misleading
        // "Not found in noop store" instead of the slow store's
        // structured error message. Tests like
        // `waiter_explicit_termination_test::non_wps_slow_store_fallback_err_terminates_writer_with_structured_error`
        // would break.
        //
        // The writer-termination contract violation here is therefore
        // intentional and SAFE in production composition: the wrapping
        // FastSlowStore owns the writer and either (a) falls through to
        // the slow store and propagates the slow store's structured Err,
        // or (b) when the slow store also returns Err, fires
        // `commit_with_inner_miss_gate` which terminates the writer
        // explicitly. NoopStore is test-only and never directly user-
        // visible; its NotFound is a SIGNAL ("I don't have it, fall
        // through") not a terminal error.
        Err(make_err!(Code::NotFound, "Not found in noop store"))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        // does nothing, so drop
        Ok(())
    }

    /// `NoopStore` accepts callbacks but never fires them — by design,
    /// it stores nothing. Returning `false` lets
    /// `FastSlowStore::register_slow_eviction_stable_set_listener` (#367)
    /// emit an operator-visible startup `warn!` if `cas_FAST_SLOW_STORE.slow
    /// = NoopStore` is misconfigured into a durability-sensitive role.
    fn supports_removal_callbacks(&self) -> bool {
        false
    }

    /// NoopStore is a leaf — no data is stored, no inner store contributes
    /// to the BIS chain.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    /// NoopStore is a leaf — pin requests are silently ignored (default
    /// `pin_digests` is a no-op for `Leaf` per the trait contract).
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    /// NoopStore is a leaf — `mark_stable` is a no-op (no data is stored,
    /// nothing to advertise via BIS). (Task #157.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

default_health_status_indicator!(NoopStore);
