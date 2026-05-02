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

//! #212 Phase 2.5: read-side registry of in-flight `ChunkedDriver`s.
//!
//! The chunked-write path (`nativelink-service::chunked_write_handler`,
//! Phase 2.7-owned) creates one [`ChunkedDriver`] per blob currently
//! being uploaded. The driver retains a per-chunk in-memory pin (the
//! design §6.2 `failed_writes` pin / §6.3 read-cascade step 2) for the
//! duration of the write. Reads that arrive while the driver is still
//! in flight should be served from the pin BEFORE falling through to
//! the slow store (which doesn't yet have the canonical CAS file —
//! `commit_and_verify` runs only after the last chunk lands).
//!
//! [`ChunkedReadRegistry`] is the lookup table that lets
//! [`crate::fast_slow_store::FastSlowStore::get_part`] find the live
//! driver for a digest. The chunked-write handler `register`s the
//! driver on first chunk; the handler `deregister`s on commit /
//! failure / drop. FastSlowStore queries via [`Self::get`] and reads
//! via the driver's [`ChunkedDriver::try_get_chunk_from_pin`] read
//! accessor.
//!
//! **Lock discipline:** the inner map is a `parking_lot::Mutex` over a
//! `HashMap<DigestInfo, Arc<ChunkedDriver>>`. Every critical section
//! is one HashMap op + an `Arc::clone`; no `.await` crosses the lock.
//!
//! **Lifetime + cleanup:** the registry holds `Arc<ChunkedDriver>`,
//! NOT `Weak`. The chunked-write handler owns the registration
//! lifetime — `register` on first chunk, `deregister` on driver
//! termination. Until `deregister` fires, the read cascade can find
//! the driver and consult the pin. After `deregister`, subsequent
//! reads fall through to the slow store as if the chunked path had
//! never existed for this digest. Holding `Arc` (not `Weak`) means
//! the driver task survives at least as long as the registry entry,
//! which is the correctness requirement for the read accessor (the
//! pin's `Arc<Mutex<ChunkPin>>` is shared between the spawned task
//! and the `ChunkedDriver` value, so dropping the value is what
//! triggers the §6.7 panic-belt abort + the pin clear).

use std::collections::HashMap;
use std::sync::Arc;

use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, publish,
};
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use tracing::{debug, trace};

use super::chunked_driver::ChunkedDriver;

/// In-flight driver registry consulted by `FastSlowStore::get_part`
/// for the design §6.3 step 2 read cascade.
///
/// Default-constructed (no entries) when the `chunked_fast_slow`
/// feature is enabled but the chunked-write path has not yet
/// admitted any blob. The registry is wired into FastSlowStore via
/// [`crate::fast_slow_store::FastSlowStore::set_chunked_read_registry`]
/// and queried from [`crate::fast_slow_store::FastSlowStore::get_part`].
///
/// The chunked-write handler (Phase 2.7) registers on first chunk
/// admission and de-registers on driver termination (success OR
/// failure OR drop). Phase 2.5 only owns the registry shape + the
/// read-side query; the write-side wire-up lands in Phase 2.7.
#[derive(Debug, Default)]
pub struct ChunkedReadRegistry {
    inner: Mutex<HashMap<DigestInfo, Arc<ChunkedDriver>>>,
    metrics: ChunkedReadRegistryMetrics,
}

/// Counters published via the [`MetricsComponent`] derive on
/// [`ChunkedReadRegistry`]. Operator-actionable from the moment
/// the `chunked_fast_slow` feature is flipped on.
#[derive(Debug, Default, MetricsComponent)]
struct ChunkedReadRegistryMetrics {
    /// Cascade-step-2 hit: `try_get_chunk_from_pin` returned `Some`
    /// for a digest that had a live driver entry. Each hit is one
    /// blob (or one byte range) served from in-memory chunks before
    /// the slow store had the canonical CAS file.
    #[metric(help = "FastSlowStore: chunked-pin hits (read cascade step 2 served from memory)")]
    pin_hits_total: core::sync::atomic::AtomicU64,
    /// Registry miss: `get` returned `None`. The slow store is the
    /// next cascade step. Tracked so operators can correlate
    /// pin-hit-rate vs cascade-fallthrough volume.
    #[metric(help = "FastSlowStore: chunked-pin misses (no in-flight driver for this digest)")]
    pin_misses_total: core::sync::atomic::AtomicU64,
    /// `try_get_chunk_from_pin` returned `None` despite the registry
    /// having a driver entry. Surfaces "in flight but the requested
    /// range isn't pinned" — usually a partially-arrived blob whose
    /// tail chunk hasn't landed.
    #[metric(help = "FastSlowStore: chunked-pin partial misses (driver present but range not covered)")]
    pin_partial_misses_total: core::sync::atomic::AtomicU64,
}

impl ChunkedReadRegistry {
    /// Construct a fresh empty registry. Wrapped in `Arc` for
    /// sharing between the chunked-write handler (Phase 2.7) and
    /// every `FastSlowStore` instance that consults it.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register an in-flight driver. Called by the chunked-write
    /// handler (Phase 2.7) once admission of the first chunk for a
    /// digest succeeds. If a previous entry exists for the same
    /// digest (concurrent re-upload of the same digest), the
    /// previous entry is dropped — the chunked-write handler's
    /// `take_or_create_in_flight` is the authoritative serializer
    /// for concurrent same-digest uploads (returns AlreadyExists
    /// instead of letting two streams race).
    ///
    /// Returns the previous entry (if any) so the caller can decide
    /// whether the displacement is intentional. Today's handler
    /// always rejects concurrent same-digest streams BEFORE
    /// reaching this method, so a non-`None` return is always a
    /// programmer bug.
    pub fn register(&self, digest: DigestInfo, driver: Arc<ChunkedDriver>) -> Option<Arc<ChunkedDriver>> {
        let prev = self.inner.lock().insert(digest, driver);
        if prev.is_some() {
            debug!(
                target: "nativelink_store::chunked",
                ?digest,
                "ChunkedReadRegistry::register replaced an existing driver entry; \
                 chunked-write handler should have prevented concurrent streams for this digest"
            );
        } else {
            trace!(
                target: "nativelink_store::chunked",
                ?digest,
                "ChunkedReadRegistry::register added driver",
            );
        }
        prev
    }

    /// De-register a driver. Called by the chunked-write handler
    /// (Phase 2.7) when the driver terminates (commit success OR
    /// failure OR drop). Returns the removed entry if any.
    pub fn deregister(&self, digest: &DigestInfo) -> Option<Arc<ChunkedDriver>> {
        let removed = self.inner.lock().remove(digest);
        if removed.is_some() {
            trace!(
                target: "nativelink_store::chunked",
                ?digest,
                "ChunkedReadRegistry::deregister removed driver",
            );
        }
        removed
    }

    /// Get a clone of the live driver for a digest (or `None` if
    /// none registered). The clone is one `Arc` bump; the caller is
    /// expected to call `try_get_chunk_from_pin` on the returned
    /// driver and then drop it. The lock is released before the
    /// caller invokes the read accessor.
    #[must_use]
    pub fn get(&self, digest: &DigestInfo) -> Option<Arc<ChunkedDriver>> {
        self.inner.lock().get(digest).cloned()
    }

    /// Diagnostic accessor: returns the number of currently-registered
    /// drivers. Used by tests + operator metric correlation.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// `len() == 0` convenience for the shutdown-flush integration
    /// point (design §6.5) and for assertions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Internal helper: increment the pin-hit counter. Called by
    /// `FastSlowStore::get_part` after a successful pin read.
    pub(crate) fn record_pin_hit(&self) {
        self.metrics
            .pin_hits_total
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    /// Internal helper: increment the pin-miss counter (no driver
    /// entry for the digest). Called by `FastSlowStore::get_part`.
    pub(crate) fn record_pin_miss(&self) {
        self.metrics
            .pin_misses_total
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    /// Internal helper: increment the pin-partial-miss counter
    /// (driver entry present but the requested byte range isn't
    /// covered).
    pub(crate) fn record_pin_partial_miss(&self) {
        self.metrics
            .pin_partial_misses_total
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }
}

impl MetricsComponent for ChunkedReadRegistry {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let drivers_active = self.len() as u64;
        publish!(
            "drivers_active",
            &drivers_active,
            MetricKind::Default,
            "ChunkedReadRegistry: in-flight chunked-write drivers currently registered"
        );
        self.metrics.publish(kind, field_metadata)?;
        Ok(MetricPublishKnownKindData::Component)
    }
}
