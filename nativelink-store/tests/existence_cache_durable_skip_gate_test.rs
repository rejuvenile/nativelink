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

//! FL-688 backfill OPT-1 — `ExistenceCacheStore` durable-skip-gate SEAM tests.
//!
//! ## The bug these tests pin
//!
//! `ExistenceCacheStore::update` / `::update_oneshot` decide whether to SKIP
//! the inner-store write by probing the inner store's *existence*. Before
//! OPT-1 that probe was `has_with_results` (RAM-INCLUSIVE). A blob held only
//! in the server's RAM-only `mirror_blobs` map (the worker is the sole durable
//! holder) returns `Some` from `FastSlowStore::has_with_results` (the mirror
//! OR-merge at `fast_slow_store.rs:5454-5463`). So a re-uploaded pinned-mirror
//! blob hit the skip branch, drained, returned `Ok`, and NEVER wrote the slow
//! (durable) tier — `has_durably` stayed `None` and the worker-API pull feed
//! re-solicited the upload forever (the FL-688 backfill non-convergence loop).
//!
//! OPT-1 swaps the skip-gate to `has_durably` (slow-tier-only, via ECS
//! `durable_delegation = Inner` → `FastSlowStore::has_durably` at
//! `fast_slow_store.rs:5482-5488`). A RAM-only blob now reports not-durable →
//! the write FALLS THROUGH to `inner_store.update(...)` → `FastSlowStore`
//! normal path → the unconditional background slow-write spawn
//! (`fast_slow_store.rs:5964`) → the durable copy lands → the loop converges.
//!
//! ## Seams crossed (production composition)
//!
//! `ExistenceCacheStore::new(FastSlowStore{fast: MemoryStore, slow:
//! MemoryStore})`. The durable assertion routes:
//!   `ECS::has_durably` (durable_delegation = Inner, bypasses the moka cache)
//!     → `FastSlowStore::has_durably` (slow tier ONLY — NOT fast/in-flight/mirror)
//!       → slow MemoryStore.
//! This is the exact chain the production CAS path uses; the only production
//! wrapper omitted is `SizePartitioningStore` (per-digest small/large routing),
//! which forwards `update`/`has_durably` undistorted and does not gate the
//! skip decision — the gate under test lives entirely in `ExistenceCacheStore`.
//!
//! ## Mutation (TDD step 5)
//!
//! Revert the OPT-1 gate to `has_with_results` (the bug). The RAM-only blob
//! then hits the skip branch, the slow tier stays empty, and BOTH
//! `ecs_update_writes_through_when_only_durably_absent` and
//! `ecs_update_oneshot_writes_through_when_only_durably_absent` red-fail on
//! their bespoke `tokio::time::timeout` expect message
//! ("blob acked but never durable — RAM-only skip ...").

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;

const VALID_HASH: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";

/// A few-second deadlock / non-convergence detector. The background
/// slow-write spawn (`fast_slow_store.rs:5964`) lands near-instantly with a
/// MemoryStore slow tier; this bound is generous noise headroom.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(5);

/// Slow-tier MemoryStore wrapper counting `update` / `update_oneshot` calls.
/// Used by the asymmetric no-regression test to prove a genuinely-durable
/// blob STILL dedup-skips (no redundant inner write reaches the slow tier).
#[derive(Debug, MetricsComponent)]
struct CountingSlowStore {
    inner: Store,
    writes: Arc<AtomicU64>,
}

default_health_status_indicator!(CountingSlowStore);

#[async_trait]
impl StoreDriver for CountingSlowStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.update(key, reader, upload_size).await
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.update_oneshot(key, data).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.inner.get_part(key, writer, offset, length).await
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
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }

    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner.as_store_driver())
    }
}

/// Build the production-shaped chain `ExistenceCacheStore(FastSlowStore{fast:
/// Memory, slow})`. Returns the wrapping ECS `Store` and the held
/// `Arc<FastSlowStore>` so the test can seed the RAM-only `mirror_blobs` entry
/// on the SAME instance the chain uses.
fn build_chain(slow_store: Store) -> (Store, Arc<FastSlowStore>) {
    let fast_slow_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        slow_store,
    );
    let existence_cache = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1_000_000,
                ..Default::default()
            }),
            log_not_found_at_info: false,
        },
        Store::new(fast_slow_arc.clone()),
    ));
    (existence_cache, fast_slow_arc)
}

/// Poll `ecs.has_durably(digest)` until it reports `Some`, bounded by
/// `CONVERGENCE_TIMEOUT`. The break is keyed on the actual durable-presence
/// observation (NOT elapsed time), so the 10 ms backoff between polls is
/// backoff, not sleep-as-synchronization. Panics with the bespoke
/// non-convergence message on timeout.
async fn await_durable(ecs: &Store, digest: DigestInfo, path_label: &str) -> u64 {
    let polled = tokio::time::timeout(CONVERGENCE_TIMEOUT, async {
        loop {
            let mut durable = [None];
            ecs.has_durably(&[digest.into()], &mut durable)
                .await
                .expect("has_durably probe must not error");
            if let Some(size) = durable[0] {
                return size;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    polled.unwrap_or_else(|_elapsed| {
        panic!(
            "blob acked but never durable — RAM-only skip: \
             ExistenceCacheStore::{path_label} skipped the inner-store write \
             for a blob present only in FastSlowStore mirror_blobs (RAM), so \
             the background slow-write never spawned and has_durably stayed \
             None past {CONVERGENCE_TIMEOUT:?} (FL-688 backfill \
             non-convergence regressed — the skip-gate must probe has_durably, \
             not has_with_results)"
        )
    })
}

/// SEAM (a): `ExistenceCacheStore::update` (the reader-based path; the
/// dominant ByteStream CAS write route in production).
///
/// Pre-condition mirrors the production bug trigger: the blob is present
/// ONLY in the FSS RAM-only `mirror_blobs` map — `has_with_results = Some`
/// (mirror OR-merge) but `has_durably = None` (slow tier empty). OPT-1 must
/// make `update` fall through to the inner write so the durable copy forms.
#[nativelink_test]
async fn ecs_update_writes_through_when_only_durably_absent() -> Result<(), Error> {
    let value: Vec<u8> = (0..4_096u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, value.len() as u64)?;

    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (ecs, fast_slow_arc) = build_chain(slow);

    // Seed the RAM-only mirror_blobs entry (correct size so the OR-merge
    // reports the true length). No fast-tier, no slow-tier copy.
    fast_slow_arc.test_insert_mirror_blob_unchecked(digest, Bytes::from(value.clone()));

    // Pre-condition: has_with_results = Some (mirror OR-merge), but
    // has_durably = None (slow tier empty). This is the state that fired
    // the bug.
    let mut present = [None];
    ecs.has_with_results(&[digest.into()], &mut present).await?;
    assert_eq!(
        present[0],
        Some(value.len() as u64),
        "pre-condition: has_with_results must see the RAM-only mirror blob \
         (the OR-merge that made the old gate skip the write)",
    );
    let mut durable = [None];
    ecs.has_durably(&[digest.into()], &mut durable).await?;
    assert_eq!(
        durable[0], None,
        "pre-condition: has_durably must report the mirror-only blob as NOT \
         durable (slow tier empty) — else the test cannot observe the fix",
    );

    // Drive ECS::update with the bytes (reader-based path).
    let (mut tx, rx) = make_buf_channel_pair();
    let value_for_writer = value.clone();
    let writer = tokio::spawn(async move {
        tx.send(Bytes::from(value_for_writer)).await?;
        tx.send_eof()
    });
    ecs.update(
        digest,
        rx,
        UploadSizeInfo::ExactSize(value.len() as u64),
    )
    .await
    .err_tip(|| "ECS::update must return Ok (ack is preserved)")?;
    writer
        .await
        .expect("writer task panicked")
        .err_tip(|| "writer feeding ECS::update failed")?;

    // The fix: the write fell through to FSS, whose background slow-write
    // spawn lands the durable copy. has_durably must flip to Some.
    let size = await_durable(&ecs, digest, "update").await;
    assert_eq!(
        size,
        value.len() as u64,
        "durable copy landed with the wrong size",
    );

    Ok(())
}

/// SEAM (b): `ExistenceCacheStore::update_oneshot` — the load-bearing
/// sibling. LIVE in production via `BatchUpdateBlobs`
/// (`cas_server.rs:488/505`, `is_mirror=false` for a normal client/worker
/// upload). It carried the IDENTICAL skip-gate and MUST be fixed in the
/// same bundle (sibling-audit rule).
#[nativelink_test]
async fn ecs_update_oneshot_writes_through_when_only_durably_absent() -> Result<(), Error> {
    let value: Vec<u8> = (0..4_096u32).map(|i| ((i + 7) & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, value.len() as u64)?;

    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (ecs, fast_slow_arc) = build_chain(slow);

    fast_slow_arc.test_insert_mirror_blob_unchecked(digest, Bytes::from(value.clone()));

    let mut present = [None];
    ecs.has_with_results(&[digest.into()], &mut present).await?;
    assert_eq!(
        present[0],
        Some(value.len() as u64),
        "pre-condition (oneshot): has_with_results must see the RAM-only \
         mirror blob",
    );
    let mut durable = [None];
    ecs.has_durably(&[digest.into()], &mut durable).await?;
    assert_eq!(
        durable[0], None,
        "pre-condition (oneshot): has_durably must report mirror-only blob \
         as NOT durable",
    );

    // Drive ECS::update_oneshot (the BatchUpdateBlobs path).
    ecs.update_oneshot(digest, Bytes::from(value.clone()))
        .await
        .err_tip(|| "ECS::update_oneshot must return Ok (ack is preserved)")?;

    let size = await_durable(&ecs, digest, "update_oneshot").await;
    assert_eq!(
        size,
        value.len() as u64,
        "durable copy landed with the wrong size (oneshot)",
    );

    Ok(())
}

/// Asymmetric coverage: a genuinely-DURABLE blob must STILL dedup-skip — no
/// redundant inner write. Proves OPT-1 narrows the skip (RAM-only no longer
/// skips) WITHOUT regressing the steady-state Bazel dedup case
/// (`has_durably = Some` ⇒ skip). The `CountingSlowStore` asserts the second
/// update performs ZERO new slow-tier write.
#[nativelink_test]
async fn ecs_update_skips_when_already_durable_no_redundant_write() -> Result<(), Error> {
    let value: Vec<u8> = (0..4_096u32).map(|i| ((i + 19) & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, value.len() as u64)?;

    let writes = Arc::new(AtomicU64::new(0));
    let counting_slow = Store::new(Arc::new(CountingSlowStore {
        inner: Store::new(MemoryStore::new(&MemorySpec::default())),
        writes: writes.clone(),
    }));
    let (ecs, _fast_slow_arc) = build_chain(counting_slow);

    // First upload: nothing durable yet → write flows through to the slow
    // tier (one counted write) and the durable copy forms.
    ecs.update_oneshot(digest, Bytes::from(value.clone()))
        .await
        .err_tip(|| "first ECS::update_oneshot")?;
    let first_size = await_durable(&ecs, digest, "update_oneshot").await;
    assert_eq!(first_size, value.len() as u64, "durable size after first write");
    assert_eq!(
        writes.load(Ordering::SeqCst),
        1,
        "first update must perform exactly one slow-tier write",
    );

    // Second upload of the SAME (now-durable) blob: has_durably = Some →
    // the skip-gate must dedup → NO new slow-tier write.
    ecs.update_oneshot(digest, Bytes::from(value.clone()))
        .await
        .err_tip(|| "second ECS::update_oneshot (durable dedup)")?;
    // Yield generously to let any erroneously-spawned background slow write
    // run before we assert the counter is unchanged.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        writes.load(Ordering::SeqCst),
        1,
        "redundant write on already-durable blob: the durable-skip gate must \
         dedup when has_durably = Some — a second slow-tier write means the \
         steady-state Bazel dedup path regressed",
    );

    Ok(())
}
