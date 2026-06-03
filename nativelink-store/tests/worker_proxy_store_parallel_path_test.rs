// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! Task #171 (parallel-path coverage extension): the deployed
//! `INNER_MISS_NO_TERMINATE` gate at
//! `WorkerProxyStore::get_part_sequential` covers the POPULATOR caller
//! of `FastSlowStore::get_part`, but the WAITER path inside
//! `FastSlowStore::get_part` (taken when a second concurrent caller
//! attaches to an in-flight populate via `populating_digests`) does
//! NOT honor the gate at its slow-store-fallback site
//! (`fast_slow_store.rs:3372-3377`).
//!
//! Production trigger: when the worker issues `get_part_parallel` to
//! the SERVER for a 22.5 MB blob, it splits into 3 ranged Read RPCs.
//! On the SERVER side these 3 RPCs each enter `WorkerProxyStore::
//! get_part_sequential` (each task's own scope, so each task's own
//! task-local). All 3 land in the SAME `FastSlowStore::get_part`
//! against the SAME digest. ONE wins the `populating_digests` lock
//! (becomes populator), the OTHER TWO become WAITERS. Producer's
//! `slow_store.has()` returns NotFound → `streaming_writer.send_error
//! (NotFound)` → terminal state.
//!
//! - The POPULATOR's `FastSlowStore::get_part` reaches the gated
//!   terminal-state branch (line 3210-3221) and returns Err NotFound
//!   WITHOUT terminating the OUTER writer. ✓
//! - Each WAITER's `FastSlowStore::get_part` reaches the streaming-
//!   reader-error branch in the `loop { reader.next_chunk() }` body
//!   (line 3318) with `is_populator_caller=false`, falls into the
//!   slow-store-fallback at line 3372-3377. `slow_store.get_part(...)`
//!   ALSO returns Err NotFound (slow tier is empty), and
//!   `commit_delegated_if_ok(&Err)` does NOT commit, so the
//!   `WriteHalfGuard` Drop fallback fires `send_error(...)` on the
//!   OUTER writer — terminating it.
//! - The waiter's WPS sees `inner.get_part(...)` returned Err NotFound,
//!   falls through to `try_read_from_endpoints` → `get_part_and_cache`
//!   → `peer.get_part(... &mut writer ...)`. Peer returns the bytes,
//!   but `writer.send(chunk)` fails with `Code::Internal "Tried to
//!   send while stream is closed"` because the OUTER writer was
//!   already terminated by the waiter's Drop fallback.
//!
//! Same contract as the populator-NotFound branch, same bug shape,
//! different site. The fix extends the `INNER_MISS_NO_TERMINATE` gate
//! to the WAITER path's slow-store-fallback (and the peer terminal
//! branches that delegate to the slow store) so the OUTER writer
//! survives the local NotFound for the waiter too.

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
    VerifySpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// A peer-side store that delays before serving any chunk. Models a
/// real network peer that takes ~200ms to respond. The delay opens
/// the race window that production exhibits.
#[derive(Debug, MetricsComponent)]
struct DelayedPeerStore {
    inner: Store,
    delay: Duration,
}

default_health_status_indicator!(DelayedPeerStore);

#[async_trait]
impl StoreDriver for DelayedPeerStore {
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
    ) -> Result<(), Error> {
        self.inner.update(key, reader, upload_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        tokio::time::sleep(self.delay).await;
        self.inner
            .get_part(key.borrow(), writer, offset, length)
            .await
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
}

/// A slow store that stalls `has_with_results`/`get_part` for a
/// configurable delay before returning NotFound. Used as the SLOW tier
/// of FastSlowStore so the producer's `run_producer` head-result
/// (`slow_store.has`) is held long enough for a second concurrent
/// caller to attach as a WAITER on the same `populating_digests` entry.
#[derive(Debug, MetricsComponent)]
struct SlowNotFoundStore {
    delay: Duration,
}

default_health_status_indicator!(SlowNotFoundStore);

#[async_trait]
impl StoreDriver for SlowNotFoundStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        tokio::time::sleep(self.delay).await;
        for r in results.iter_mut() {
            *r = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        tokio::time::sleep(self.delay).await;
        Err(make_err!(
            Code::NotFound,
            "SlowNotFoundStore: not found {:?}",
            key.borrow()
        ))
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
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

/// Reproducer for the #171 parallel-path leak: WAITER inside
/// `FastSlowStore::get_part` does NOT honor `INNER_MISS_NO_TERMINATE`,
/// so its Drop fallback closes the OUTER writer the moment the
/// populator's NotFound flows through the streaming buffer.
///
/// When the waiter's `WorkerProxyStore::get_part_sequential` falls
/// through to peer-fetch, the peer's `writer.send(chunk)` fails with
/// `Code::Internal "Tried to send while stream is closed"` even though
/// the peer DOES return all bytes — exactly the production symptom on
/// digest fe48ed08…-22506712 (3 parallel-chunk reads, post-fix).
///
/// Production composition (server side, per-chunk):
///
/// ```text
///   bytestream tx (3 of these, one per parallel chunk)
///     │
///     ▼
///   WorkerProxyStore  (race_peers=false, IS_WORKER_REQUEST=true)
///     │ get_part_sequential sets INNER_MISS_NO_TERMINATE=true
///     │ inner.get_part(&mut tx, ...)
///     ▼
///   ExistenceCacheStore → VerifyStore (length=Some(_) → no inner tx)
///     ▼
///   FastSlowStore::get_part — populating_digests arbitrates:
///     - chunk 0 wins, becomes POPULATOR (gated path ✓)
///     - chunks 1, 2 become WAITERS (UNGATED path — THE BUG)
///                    ▼
///         streaming-reader sees producer's NotFound
///                    ▼
///         line 3372: slow_store.get_part(&mut *guard, ...) → NotFound
///                    ▼
///         commit_delegated_if_ok(&Err) does NOT commit
///                    ▼
///         WriteHalfGuard::Drop fires send_error(...) on OUTER tx
///                    ▼
///         OUTER tx is now terminated
/// ```
///
/// Then the waiter's WPS falls through to peer-fetch:
///
/// ```text
///   try_read_from_endpoints → get_part_and_cache → peer.get_part
///     │ peer DOES return bytes
///     ▼
///   writer.send(chunk) → Err Internal "Tried to send while stream is closed"
/// ```
///
/// The test runs 3 concurrent `get_part_unchunked` calls for the same
/// digest. With the bug, the waiters fail (or hang) because the OUTER
/// writer is closed mid-recovery. With the fix, all 3 succeed by
/// receiving the peer's bytes.
#[nativelink_test]
async fn waiter_path_inner_miss_with_peer_fallback_does_not_lose_peer_bytes_to_drop_termination()
-> Result<(), Error> {
    let value: Vec<u8> = (0..96_000u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Inner store chain mirrors production CAS for the per-chunk path.
    // SLOW tier is `SlowNotFoundStore` with a 250ms delay so the
    // producer's `run_producer.head=slow_store.has` is held long enough
    // for the second/third concurrent callers to land in
    // `populating_digests` as WAITERS rather than as fresh populators.
    //
    // VerifyStore wraps with verify_size=true; the read uses
    // `length=Some(_)` so VerifyStore's `should_verify` gate at line
    // 309-311 falls through to the direct inner_store path without
    // inserting an internal tx — exactly matching the production
    // per-parallel-chunk request shape.
    let fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(Arc::new(SlowNotFoundStore {
            delay: Duration::from_millis(250),
        })),
    ));
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        fast_slow,
    ));
    let existence_cache = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1_000_000,
                ..Default::default()
            }),
            log_not_found_at_info: false,
        },
        verify,
    ));

    // Wrap with WorkerProxyStore (server-side: race_peers=false).
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(existence_cache.clone(), locality_map.clone());
    let proxy = Store::new(proxy_arc.clone());

    // Peer holds the blob; serves with a small artificial delay to
    // ensure the local-NotFound notify wins the race deterministically.
    // The waiter's slow-store-fallback NotFound + Drop must terminate
    // BEFORE the peer's bytes can reach `writer.send`.
    let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_inner
        .update_oneshot(digest, Bytes::from(value.clone()))
        .await?;
    let delayed_peer = Store::new(Arc::new(DelayedPeerStore {
        inner: peer_inner,
        delay: Duration::from_millis(150),
    }));

    let peer_endpoint = "grpc://delayed-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, delayed_peer);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    // Issue 3 concurrent reads for the SAME digest with `length=Some(_)`
    // (mirrors per-parallel-chunk shape). One becomes POPULATOR; the
    // other two become WAITERS in `populating_digests`. The race-loser
    // writer-termination bug fires for the WAITERS.
    let proxy1 = proxy.clone();
    let proxy2 = proxy.clone();
    let proxy3 = proxy.clone();
    let len = value.len() as u64;
    let read1 = tokio::spawn(async move { proxy1.get_part_unchunked(digest, 0, Some(len)).await });
    let read2 = tokio::spawn(async move { proxy2.get_part_unchunked(digest, 0, Some(len)).await });
    let read3 = tokio::spawn(async move { proxy3.get_part_unchunked(digest, 0, Some(len)).await });

    let timed = tokio::time::timeout(Duration::from_secs(8), async move {
        let r1 = read1
            .await
            .map_err(|e| make_err!(Code::Internal, "task1 join: {e}"))?;
        let r2 = read2
            .await
            .map_err(|e| make_err!(Code::Internal, "task2 join: {e}"))?;
        let r3 = read3
            .await
            .map_err(|e| make_err!(Code::Internal, "task3 join: {e}"))?;
        Ok::<
            (
                Result<Bytes, Error>,
                Result<Bytes, Error>,
                Result<Bytes, Error>,
            ),
            Error,
        >((r1, r2, r3))
    })
    .await
    .expect(
        "must not deadlock — concurrent inner-miss + peer-fetch must \
         deliver bytes within 8s; WAITER-path Drop-termination bug at \
         fast_slow_store::get_part WAITER branch (slow-store-fallback \
         line 3372-3377) closed the OUTER writer before the peer's \
         bytes could be forwarded",
    )?;

    let (r1, r2, r3) = timed;

    // ALL THREE must deliver the peer's bytes. With the bug, at least
    // one WAITER fails with `Code::Internal` ("Tried to send while
    // stream is closed" / "buf_channel: writer dropped without commit").
    for (idx, res) in [&r1, &r2, &r3].iter().enumerate() {
        let bytes = res.as_ref().unwrap_or_else(|err| {
            panic!(
                "concurrent reader #{idx} failed; WAITER-path Drop-\
                 termination at fast_slow_store::get_part WAITER branch \
                 (slow-store-fallback line 3372-3377) closed the OUTER \
                 writer before peer-fetch could deliver bytes: \
                 got Err {err:?} — code {:?}",
                err.code,
            )
        });
        assert_eq!(
            bytes.len(),
            value.len(),
            "concurrent reader #{idx} short-read: got {} expected {}; \
             WAITER-path Drop-termination at fast_slow_store WAITER \
             branch terminated the OUTER writer before all peer bytes \
             could be forwarded",
            bytes.len(),
            value.len(),
        );
        assert_eq!(
            bytes.as_ref(),
            value.as_slice(),
            "concurrent reader #{idx} byte mismatch — peer bytes did \
             NOT round-trip identically through the WAITER's OUTER \
             writer; the writer was partially terminated then re-opened",
        );
    }

    Ok(())
}
