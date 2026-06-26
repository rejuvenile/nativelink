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

//! F5 (#F5) — C2\* mirror-spill to local disk at graceful shutdown.
//!
//! `FastSlowStore::spill_mirror_to_disk_at_shutdown` makes a worker restart
//! NON-LOSSY for RAM-only sole-copy mirror blobs (class **C2\*** =
//! `mirror_blobs ∩ failed_slow_writes`). The existing
//! `flush_fast_to_slow_at_shutdown` MISSES C2\* because it enumerates from
//! `fast_store.list()` (DISK) and C2\* is not on disk.
//!
//! ## Production composition (the seam this exercises)
//!
//! The tests build a **worker-shaped** `FastSlowStore`: fast = a real
//! `FilesystemStore` (tempdir, the worker's local disk tier), slow = a
//! fault-injected store standing in for a DEGRADED server (every slow write
//! `Err`s). A blob is inserted via the `IS_MIRROR_REQUEST.scope(true, …)`
//! `update_oneshot` path so it lands in `mirror_blobs` ONLY (NOT on disk), then
//! marked C2\* via `requeue_failed_push`. On-disk presence is asserted via the
//! in-process index (`FilesystemStore::has`) within a `tokio::time::timeout`
//! deadlock detector — NOT `tokio::fs::metadata` (the index-visibility rule,
//! `.claude/rules/testing-contracts.md`).
//!
//! ## Tests
//!
//! * `c2star_mirror_spilled_to_disk_and_pinned` (primary, red→green): a
//!   mirror-only C2\* blob IS on the local `FilesystemStore` (disk) after the
//!   spill AND is held under an INDEFINITE pin that SURVIVES a real
//!   `expire_stale_pins` sweep; the degraded slow store was NEVER written.
//! * `plain_mirror_not_in_failed_writes_is_not_spilled` (over-action /
//!   confirm-necessity of `∩ failed_slow_writes`): a PLAIN mirror blob (NOT in
//!   `failed_slow_writes`) is NOT spilled to disk and is counted skipped — the
//!   pass does not bloat the disk tier with server-durable mirrors.
//! * `spill_enospc_is_tolerated_per_entry` (distsys F5-2 / SEC-4): a spill
//!   write that returns ENOSPC is tolerated — the spill RETURNS (no hang),
//!   counts the failure, and LEAVES the digest in `failed_slow_writes`.
//!
//! ## Mutations (TDD step 5) — all verified to red-fail with the bespoke msg
//!
//! 1. Drain-to-server-only (route C2\* through `slow_store.update_oneshot` and
//!    mask its Err as success) → the C2\* blob never lands on disk. NOTE: when
//!    the masked slow write is allowed to Err (not masked), the test reds FIRST
//!    at the `errored == 0` guard (test:~383); with the Err masked it reds at
//!    the on-disk assertion (test:~396) with "C2* mirror blob not spilled to
//!    disk — lost on restart".
//! 2. Spill-without-pin (drop the pin after the disk write) → the
//!    freshly-materialized entry is plain-LRU-evictable →
//!    `c2star_mirror_spilled_to_disk_and_pinned` reds at the FIRST pin check
//!    with "C2* blob spilled but NOT pinned — F3b eviction-drain can evict it
//!    before restart".
//! 3. Enumeration-source bug (enumerate `fast_store.list()` instead of
//!    `mirror_blob_digests()`) → C2\* is not on disk → never enumerated →
//!    reds at the on-disk assertion with "C2* mirror blob not spilled to disk —
//!    lost on restart".
//! 4. Non-indefinite pin (pair-a MAJOR; revert the spill to the time-bounded
//!    `fast_store.pin_digests`) → the pin SURVIVES the first check but
//!    `expire_stale_pins` DEMOTES it (non-indefinite, >120s) → the post-sweep
//!    check reds with "C2* spill pin demoted by expire_stale_pins — would be
//!    re-lost on a >120s shutdown; must be INDEFINITE".
//! 5. Remove-on-ENOSPC (remove the digest from `failed_slow_writes` on a
//!    per-entry failure) → `spill_enospc_is_tolerated_per_entry` reds with
//!    "ENOSPC'd C2* digest must REMAIN in failed_slow_writes after a tolerated
//!    spill failure".

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DurableDelegation, IS_MIRROR_REQUEST, ItemCallback, MarkStableDelegation, PinDelegation,
    StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tempfile::TempDir;

const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

fn digest_of(size: usize) -> DigestInfo {
    DigestInfo::try_new(VALID_HASH, size as u64).unwrap()
}

// ---------------------------------------------------------------------
// ErroringSlowStore: a slow tier whose every write Errs (degraded server).
//
// Reads/has delegate to an inner MemoryStore so the FSS composes normally;
// only the slow WRITE path fails — exactly the §6 degraded-connection
// scenario. Counts write attempts so the primary test can assert the spill
// went to DISK, not the server (count == 0).
// ---------------------------------------------------------------------
#[derive(Debug, MetricsComponent)]
struct ErroringSlowStore {
    inner: Store,
    write_attempts: Arc<AtomicU64>,
}
default_health_status_indicator!(ErroringSlowStore);

#[async_trait]
impl StoreDriver for ErroringSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }
    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        Err(make_err!(
            Code::DeadlineExceeded,
            "ErroringSlowStore: degraded server — update failed"
        ))
    }
    async fn update_oneshot(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _data: Bytes,
    ) -> Result<(), Error> {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        Err(make_err!(
            Code::DeadlineExceeded,
            "ErroringSlowStore: degraded server — update_oneshot failed"
        ))
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
    fn register_item_callback(self: Arc<Self>, _cb: Arc<dyn ItemCallback>) -> Result<(), Error> {
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

// ---------------------------------------------------------------------
// EnospcFastStore: a fast tier whose update_oneshot returns a ResourceExhausted
// (ENOSPC-class) error, used to prove per-entry ENOSPC tolerance. Everything
// else delegates to an inner MemoryStore so pin/has/get behave normally.
// ---------------------------------------------------------------------
#[derive(Debug, MetricsComponent)]
struct EnospcFastStore {
    inner: Store,
}
default_health_status_indicator!(EnospcFastStore);

#[async_trait]
impl StoreDriver for EnospcFastStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }
    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::ResourceExhausted,
            "EnospcFastStore: No space left on device (os error 28)"
        ))
    }
    async fn update_oneshot(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _data: Bytes,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::ResourceExhausted,
            "EnospcFastStore: No space left on device (os error 28)"
        ))
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
    fn register_item_callback(self: Arc<Self>, _cb: Arc<dyn ItemCallback>) -> Result<(), Error> {
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

/// A worker-shaped FSS with a REAL `FilesystemStore` fast tier (the worker's
/// local disk) + a degraded (every-write-`Err`) slow tier. Returns the FSS, the
/// concrete `FilesystemStore` (for the index-visibility `has` + pin probe), the
/// slow-write-attempt counter, and the `TempDir` guard (kept alive).
struct DiskHarness {
    fss: Arc<FastSlowStore>,
    fs_store: Arc<FilesystemStore>,
    slow_write_attempts: Arc<AtomicU64>,
    _temp: TempDir,
}

async fn make_disk_harness() -> Result<DiskHarness, Error> {
    let temp = tempfile::Builder::new()
        .prefix("shutdown_mirror_spill_")
        .tempdir()
        .expect("tempdir");
    let content_path = temp.path().join("content");
    let temp_path = temp.path().join("temp");
    tokio::fs::create_dir_all(&content_path).await.unwrap();
    tokio::fs::create_dir_all(&temp_path).await.unwrap();
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.to_string_lossy().into_owned(),
        temp_path: temp_path.to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 4 * 1024 * 1024,
            ..Default::default()
        }),
        ..Default::default()
    })
    .await?;
    let fast_store = Store::new(fs_store.clone());

    let slow_write_attempts = Arc::new(AtomicU64::new(0));
    let slow = Store::new(Arc::new(ErroringSlowStore {
        inner: Store::new(MemoryStore::new(&MemorySpec::default())),
        write_attempts: Arc::clone(&slow_write_attempts),
    }) as Arc<dyn StoreDriver>);

    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_store,
        slow,
    );
    Ok(DiskHarness {
        fss,
        fs_store,
        slow_write_attempts,
        _temp: temp,
    })
}

/// Drive the FSS into the graceful-shutdown state the same way production does
/// — `flush_slow_writes` (Phase 1) sets `shutting_down` and drains in-flight
/// slow writes. With no in-flight writes it returns immediately. This matches
/// the `store_manager.rs` Phase-2 ordering (flush THEN spill) without a
/// test-only setter. Phase 1 touches only `in_flight_slow_writes` /
/// `chunked_in_flight_digests`, never `mirror_blobs` / `failed_slow_writes`.
async fn enter_shutdown(fss: &Arc<FastSlowStore>) {
    let pending = fss.flush_slow_writes(Duration::from_secs(5)).await;
    assert_eq!(pending, 0, "no in-flight slow writes to drain in this test");
}

/// Insert a blob into `mirror_blobs` ONLY (the RAM-only mirror map) via the
/// production `IS_MIRROR_REQUEST` task-local — the same path the bytestream/cas
/// servers take for an `x-nativelink-mirror` push.
async fn write_mirror_only(fss: &Arc<FastSlowStore>, digest: DigestInfo, data: Bytes) {
    let store: Store = Store::new(fss.clone());
    IS_MIRROR_REQUEST
        .scope(true, async move {
            store
                .update_oneshot(digest, data)
                .await
                .expect("mirror write");
        })
        .await;
}

/// PRIMARY (red→green): a mirror-only C2\* blob — present ONLY in
/// `mirror_blobs ∩ failed_slow_writes`, NOT on disk — IS on the local
/// `FilesystemStore` (disk) after `spill_mirror_to_disk_at_shutdown` AND is
/// pinned; the degraded slow store is NEVER written.
#[nativelink_test]
async fn c2star_mirror_spilled_to_disk_and_pinned() -> Result<(), Error> {
    let h = make_disk_harness().await?;
    let data = Bytes::from(vec![0xC2u8; 4096]);
    let digest = digest_of(data.len());

    // (1) Insert as a mirror — lands in mirror_blobs ONLY.
    write_mirror_only(&h.fss, digest, data.clone()).await;
    assert_eq!(
        h.fss.mirror_blob_digests(),
        vec![digest],
        "setup: blob must be in the RAM-only mirror map"
    );
    // Precondition: it is NOT yet on disk (the FilesystemStore index has no
    // entry — the mirror write held it in memory only).
    assert!(
        h.fs_store.has(digest).await?.is_none(),
        "setup: a mirror-only blob must NOT be on disk before the spill"
    );

    // (2) Mark it C2* — the FL-688 sole-copy subset.
    assert!(
        h.fss.requeue_failed_push(digest),
        "setup: requeue_failed_push must accept the digest"
    );

    // (3) Simulate graceful shutdown + run the spill.
    enter_shutdown(&h.fss).await;
    let errored = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        h.fss.spill_mirror_to_disk_at_shutdown(),
    )
    .await
    .expect(
        "DEADLOCK/HANG: spill_mirror_to_disk_at_shutdown did not return; the spill of a \
         single in-memory mirror blob must finish in milliseconds.",
    );

    // The spill reported zero failures.
    assert_eq!(
        errored, 0,
        "spill of a single C2* blob must report 0 errored; got {errored}"
    );

    // ---- The load-bearing assertion (Mutations 1 & 3 red here) ----
    // On disk via the IN-PROCESS INDEX (index-visibility rule — NOT
    // tokio::fs::metadata). Mutation 1 (route to slow/server) and Mutation 3
    // (enumerate fast_store.list() — misses the RAM-only mirror) both make this
    // None.
    let on_disk = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, h.fs_store.has(digest))
        .await
        .expect("DEADLOCK: fs_store.has did not return")?;
    assert_eq!(
        on_disk,
        Some(data.len() as u64),
        "C2* mirror blob not spilled to disk — lost on restart"
    );

    // ---- The pin assertion: the spill must take an INDEFINITE pin that
    // SURVIVES the 120s expire_stale_pins sweep (pair-a MAJOR + F5-1). ----
    //
    // Step 1: the entry is pinned at all (Mutation: no pin → false here). A
    // pinned entry lives in the side `pinned` map; `test_force_pin_expired`
    // returns true iff present AND rewinds its `pinned_at` to >120s ago so the
    // very next sweep would demote a NON-indefinite pin (an indefinite pin is
    // exempt and ignores `pinned_at`).
    assert!(
        h.fs_store.test_force_pin_expired(&digest),
        "C2* blob spilled but NOT pinned — F3b eviction-drain can evict it before restart"
    );

    // Step 2: run the REAL pin-expiry sweep (the same call the 10s background
    // eviction loop makes during shutdown). An INDEFINITE pin is exempt and
    // remains pinned; a time-bounded pin (the bug) is DEMOTED to plain LRU.
    tokio::time::timeout(NO_DEADLOCK_TIMEOUT, h.fs_store.test_expire_stale_pins())
        .await
        .expect("DEADLOCK: expire_stale_pins did not return");

    // Step 3: STILL pinned after the sweep == the indefinite pin survived. A
    // non-indefinite pin (Mutation: revert to fast_store.pin_digests) is now
    // demoted → this is false → the blob would be evicted on a >120s shutdown.
    assert!(
        h.fs_store.test_force_pin_expired(&digest),
        "C2* spill pin demoted by expire_stale_pins — would be re-lost on a >120s shutdown; \
         must be INDEFINITE"
    );

    // ---- The spill went to DISK, not the server ----
    assert_eq!(
        h.slow_write_attempts.load(Ordering::SeqCst),
        0,
        "the spill must write to the local disk (fast tier), NEVER the degraded slow \
         store/server; got {} slow-write attempts",
        h.slow_write_attempts.load(Ordering::SeqCst)
    );

    Ok(())
}

/// OVER-ACTION / confirm-necessity of `∩ failed_slow_writes`: a PLAIN mirror
/// blob (in `mirror_blobs`, NOT in `failed_slow_writes`) is server-durable and
/// does NOT need a disk copy — it must NOT be spilled (else the pass bloats the
/// 40 GiB disk tier with content the server already owns).
#[nativelink_test]
async fn plain_mirror_not_in_failed_writes_is_not_spilled() -> Result<(), Error> {
    let h = make_disk_harness().await?;
    let data = Bytes::from(vec![0xABu8; 2048]);
    let digest = digest_of(data.len());

    // Mirror-only, but NOT marked C2* (never requeue_failed_push'd).
    write_mirror_only(&h.fss, digest, data.clone()).await;
    assert!(
        !h.fss.failed_slow_writes_contains(&digest),
        "setup: a plain mirror must NOT be in failed_slow_writes"
    );

    enter_shutdown(&h.fss).await;
    let errored = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        h.fss.spill_mirror_to_disk_at_shutdown(),
    )
    .await
    .expect("DEADLOCK: spill did not return");
    assert_eq!(errored, 0, "no C2* blobs → 0 errored");

    // A plain (server-durable) mirror must NOT have been copied to disk.
    assert!(
        h.fs_store.has(digest).await?.is_none(),
        "plain mirror (not in failed_slow_writes) must NOT be spilled to disk — the \
         intersection with failed_slow_writes is necessity-minimal"
    );
    assert_eq!(
        h.slow_write_attempts.load(Ordering::SeqCst),
        0,
        "no spill at all → no slow-store writes either"
    );
    Ok(())
}

/// PER-ENTRY ENOSPC TOLERANCE (distsys F5-2 / SEC-4): on a near-full disk a
/// spill write can hit ENOSPC. The spill MUST tolerate it per-entry — RETURN
/// (no crash/hang), count the failure, and LEAVE the digest in
/// `failed_slow_writes` so a later attempt can retry. We inject ENOSPC by
/// making the FAST tier's `update_oneshot` return ResourceExhausted.
#[nativelink_test]
async fn spill_enospc_is_tolerated_per_entry() -> Result<(), Error> {
    let temp = tempfile::Builder::new()
        .prefix("shutdown_mirror_spill_enospc_")
        .tempdir()
        .expect("tempdir");
    // The ENOSPC fast tier delegates non-write ops to a MemoryStore; no real
    // disk needed (the write Errs before touching disk). TempDir kept alive
    // only to mirror the harness shape.
    let _ = &temp;

    let enospc_fast = Store::new(Arc::new(EnospcFastStore {
        inner: Store::new(MemoryStore::new(&MemorySpec::default())),
    }) as Arc<dyn StoreDriver>);
    let slow_write_attempts = Arc::new(AtomicU64::new(0));
    let slow = Store::new(Arc::new(ErroringSlowStore {
        inner: Store::new(MemoryStore::new(&MemorySpec::default())),
        write_attempts: Arc::clone(&slow_write_attempts),
    }) as Arc<dyn StoreDriver>);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        enospc_fast,
        slow,
    );

    let data = Bytes::from(vec![0xE5u8; 1024]);
    let digest = digest_of(data.len());

    // A C2* blob (mirror-only + failed_slow_writes). The mirror insert goes to
    // the FSS's in-memory mirror map (independent of the fast store), so it
    // succeeds even though the fast store ENOSPCs on a real disk write.
    write_mirror_only(&fss, digest, data.clone()).await;
    assert!(fss.requeue_failed_push(digest), "setup: mark C2*");
    assert!(
        fss.failed_slow_writes_contains(&digest),
        "setup: digest is in failed_slow_writes before the spill"
    );

    enter_shutdown(&fss).await;
    // The spill MUST RETURN (no hang) despite the per-entry ENOSPC.
    let errored = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, fss.spill_mirror_to_disk_at_shutdown())
        .await
        .expect(
            "HANG: spill_mirror_to_disk_at_shutdown did not return on ENOSPC — a per-entry \
             write failure must NOT crash or hang the shutdown (distsys F5-2 / SEC-4)",
        );

    // The ENOSPC'd entry is COUNTED as errored.
    assert_eq!(
        errored, 1,
        "the single ENOSPC'd spill write must be counted errored; got {errored}"
    );

    // The digest STAYS in failed_slow_writes so a later attempt can retry it
    // (it was NOT removed by a tolerated failure).
    assert!(
        fss.failed_slow_writes_contains(&digest),
        "ENOSPC'd C2* digest must REMAIN in failed_slow_writes after a tolerated spill failure"
    );

    // Tolerance is per-FAST-tier — the spill must NOT fall back to the server.
    assert_eq!(
        slow_write_attempts.load(Ordering::SeqCst),
        0,
        "ENOSPC on the disk spill must NOT fall back to writing the degraded server"
    );
    Ok(())
}
