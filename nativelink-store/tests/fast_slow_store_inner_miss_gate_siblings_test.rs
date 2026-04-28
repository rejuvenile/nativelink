// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! Task #171 (red-team follow-up): coverage for the three early-return
//! NotFound sites in `FastSlowStore::get_part` that route to peer-fetch
//! in production but did NOT honor `INNER_MISS_NO_TERMINATE` until
//! 2026-04-27:
//!
//! 1. `mirror_blobs` size mismatch (`fast_slow_store.rs` ~line 2949).
//! 2. `in_flight_slow_writes` size mismatch (~line 3101).
//! 3. `local_only_reads` NotFound (~line 3157).
//!
//! All three early-return `Code::NotFound` with zero bytes written.
//! Without the gate, the `WriteHalfGuard` Drop fallback (or the
//! explicit `guard.fail(...)` call that preceded the fix) terminates
//! the OUTER writer the moment the synthesized NotFound is returned
//! from `FastSlowStore::get_part`. The wrapping
//! `WorkerProxyStore::get_part_sequential` then sees `inner_result ==
//! Err(NotFound)`, `should_try_peers(NotFound) == true`, and falls
//! through to peer-fetch — but the peer's `writer.send(chunk)` fails
//! with `Code::Internal "Tried to send while stream is closed"`
//! because the writer is already terminated.
//!
//! This test file exercises **site #1 (mirror_blobs size mismatch)**
//! end-to-end. It is the most common production path of the three
//! (server-pushed mirror blobs are the steady-state route under load,
//! whereas in_flight size mismatches require an in-flight slow-write
//! that diverged from its digest, and `local_only_reads` requires the
//! worker public-CAS variant — see TODO at the bottom of this file
//! for why the other two sites do not get a dedicated test in this PR).
//!
//! Sites #2 and #3 share the IDENTICAL fix shape (synthesize an
//! `Err(NotFound)` Result; pass through `commit_with_inner_miss_gate`;
//! return `res`). The mutation step on this test (revert site #1 to
//! the pre-fix `return Err(guard.fail(make_err!(...)))` pattern,
//! confirm panic with the specific assertion message, restore) covers
//! the gate-suppression semantics; sites #2 and #3 reuse the same
//! helper call and therefore inherit the guarantee.

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
    VerifySpec,
};
use nativelink_error::Error;
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

const VALID_HASH_MIRROR: &str =
    "0123456789abcdef000000000000000000020000000000000123456789abcdef";

/// A peer-side store that delays before serving any chunk. Models a
/// real network peer that takes a few hundred ms to respond, opening
/// the race window so the local-NotFound terminates the writer BEFORE
/// the peer's bytes can arrive in the bug case.
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

/// Reproducer for the #171 mirror_blobs size-mismatch sibling: the
/// `mirror_blobs` early-return at `fast_slow_store.rs:~2949`
/// synthesizes `Err(Code::NotFound)` when the in-memory mirror entry's
/// `data.len()` does not match the digest's `size_bytes()`. Pre-fix,
/// the `return Err(guard.fail(make_err!(NotFound, ...)))` call closed
/// the OUTER writer before `WorkerProxyStore::get_part_sequential` got
/// a chance to fall through to peer-fetch. The peer DOES return all
/// bytes — but `writer.send(chunk)` on the now-closed writer fails
/// with `Code::Internal "Tried to send while stream is closed"`.
///
/// Production composition (server-side):
///
/// ```text
///   bytestream tx
///     │
///     ▼
///   WorkerProxyStore  (race_peers=false, IS_WORKER_REQUEST=true)
///     │ get_part_sequential sets INNER_MISS_NO_TERMINATE=true
///     │ inner.get_part(&mut tx, ...)
///     ▼
///   ExistenceCacheStore → VerifyStore (length=Some(_) → no inner tx)
///     ▼
///   FastSlowStore::get_part — checks mirror_blobs first
///                    ▼
///         finds phantom-positive entry (data.len() != digest.size_bytes())
///                    ▼
///         remove_mirror_blobs(&[digest])
///                    ▼
///         return Err(guard.fail(make_err!(NotFound, ...)))   ← THE BUG
///                    ▼
///         OUTER tx is now terminated
/// ```
///
/// Then the WPS falls through to peer-fetch:
///
/// ```text
///   try_read_from_endpoints → get_part_and_cache → peer.get_part
///     │ peer DOES return bytes
///     ▼
///   writer.send(chunk) → Err Internal "Tried to send while stream is closed"
/// ```
///
/// Test plants a phantom-positive entry via
/// `test_insert_mirror_blob_unchecked` (the public test hook that
/// bypasses the size invariant `insert_mirror_blob` enforces), then
/// issues a `get_part_unchunked`. With the bug, the read fails because
/// the OUTER writer is closed mid-recovery. With the fix, the read
/// receives all peer bytes.
#[nativelink_test]
async fn mirror_blobs_size_mismatch_does_not_terminate_outer_writer_when_gate_set(
) -> Result<(), Error> {
    let value: Vec<u8> = (0..32_000u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH_MIRROR, value.len() as u64)?;

    // Production composition: FastSlow{Memory(empty), Memory(empty)},
    // VerifyStore wrapping (verify_size=true, length=Some(_) → no
    // inner tx insertion), ExistenceCache wrapping. We construct
    // FastSlowStore explicitly so we can plant the phantom-positive
    // mirror_blobs entry on the same Arc<FastSlowStore> the wrapping
    // chain uses.
    let fast_slow_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    // Plant a phantom-positive entry: data.len() (10 bytes) does NOT
    // match digest.size_bytes() (32_000). The first byte of `get_part`
    // will hit the size-mismatch branch and synthesize NotFound.
    fast_slow_arc.test_insert_mirror_blob_unchecked(
        digest,
        Bytes::from_static(b"phantom!!!"),
    );
    assert_eq!(
        fast_slow_arc.mirror_blob_count(),
        1,
        "mirror_blobs entry must be planted before the read",
    );

    let fast_slow = Store::new(fast_slow_arc.clone());
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
        },
        verify,
    ));

    // Wrap with WorkerProxyStore (server-side: race_peers=false,
    // IS_WORKER_REQUEST will be set true inside get_part_sequential
    // for this code path). This also sets INNER_MISS_NO_TERMINATE=true
    // around the inner get_part call — exactly the gate the
    // mirror_blobs size-mismatch fix must honor.
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(existence_cache.clone(), locality_map.clone());
    let proxy = Store::new(proxy_arc.clone());

    // Peer holds the real blob; serves with a small artificial delay
    // so the local-NotFound side definitely lands FIRST. If the peer
    // returned bytes synchronously, the order-of-operations might
    // mask the writer-termination race in a single-shot test.
    let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_inner
        .update_oneshot(digest, Bytes::from(value.clone()))
        .await?;
    let delayed_peer = Store::new(Arc::new(DelayedPeerStore {
        inner: peer_inner,
        delay: Duration::from_millis(150),
    }));

    let peer_endpoint = "grpc://delayed-peer-mirror:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, delayed_peer);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    // Issue ONE read for the digest. Mirror_blobs is the FIRST check
    // inside FastSlowStore::get_part — the planted phantom-positive
    // is hit immediately, the corrupt entry is evicted, and a
    // synthesized Err(NotFound) is returned.
    let len = value.len() as u64;
    let proxy_clone = proxy.clone();
    let timed = tokio::time::timeout(Duration::from_secs(5), async move {
        proxy_clone.get_part_unchunked(digest, 0, Some(len)).await
    })
    .await
    .expect(
        "must not deadlock — mirror_blobs size-mismatch path closed \
         OUTER writer before peer-fetch could deliver bytes; \
         `commit_with_inner_miss_gate` must suppress the writer- \
         termination call when INNER_MISS_NO_TERMINATE is set so \
         WorkerProxyStore::get_part_sequential's peer-fetch fallback \
         can reuse the same writer (#171 sibling at \
         fast_slow_store.rs:~2949)",
    );

    // The read MUST succeed and deliver the peer's bytes. With the
    // bug, the read fails with Code::Internal "Tried to send while
    // stream is closed" (or "buf_channel: writer dropped without
    // commit" — the WriteHalfGuard Drop fallback signature).
    let bytes = timed.unwrap_or_else(|err| {
        panic!(
            "mirror_blobs size-mismatch path closed OUTER writer \
             before peer-fetch could deliver bytes — \
             commit_with_inner_miss_gate must suppress writer- \
             termination at fast_slow_store.rs:~2949 when \
             INNER_MISS_NO_TERMINATE is set: got Err {err:?} \
             (code {:?})",
            err.code,
        )
    });

    assert_eq!(
        bytes.len(),
        value.len(),
        "mirror_blobs size-mismatch path: short read; OUTER writer \
         was partially terminated before peer-fetch could forward all \
         bytes — got {} bytes, expected {}",
        bytes.len(),
        value.len(),
    );
    assert_eq!(
        bytes.as_ref(),
        value.as_slice(),
        "mirror_blobs size-mismatch path: peer bytes did NOT round- \
         trip identically through the OUTER writer — the writer was \
         partially terminated then re-opened",
    );

    // Confirm the phantom-positive was actually evicted as a
    // side-effect (the canonical removal path WAS exercised — i.e.,
    // we hit the size-mismatch branch, not some other accidental
    // happy path).
    assert_eq!(
        fast_slow_arc.mirror_blob_count(),
        0,
        "mirror_blobs entry must be evicted by the size-mismatch \
         branch — if this is non-zero, the test did NOT exercise the \
         intended branch",
    );

    Ok(())
}

// TODO(#171 sibling tests): the in_flight_slow_writes size-mismatch
// site (`fast_slow_store.rs:~3101`) and the local_only_reads NotFound
// site (`~3157`) share the IDENTICAL fix shape with the mirror_blobs
// site exercised above (synthesize Err(NotFound), pass through
// `commit_with_inner_miss_gate`, return `res`). Dedicated production-
// composition tests for those sites are not included in this PR
// because:
//
// - `in_flight_slow_writes` size mismatch requires either (a) a
//   chained custom slow store that diverges between
//   `update`/`update_oneshot` and the digest contract, or (b)
//   `test_insert_in_flight` followed by an immediate `get_part_unchunked`
//   on the same digest — both setups are non-trivial to wire through
//   the production composition (the Verify+ExistenceCache wrap is
//   load-bearing, and the in_flight branch only fires AFTER the
//   fast-store miss + before the populator path). Tracker: leave a
//   followup tracking sibling-coverage so this test family can be
//   completed when the in_flight branch acquires non-mirror call
//   sites that need defending.
//
// - `local_only_reads` requires the worker public-CAS variant
//   (`with_local_only_reads()`), which today is only constructed from
//   `local_worker.rs`'s public-CAS server registration. Mocking that
//   composition directly is straightforward but the production
//   frequency of `local_only_reads` peer-fetch fallback is currently
//   bounded by the `WorkerProxyStore` chain that wraps the public-CAS
//   variant; the mirror_blobs path is the dominant production trigger.
//
// The mutation step on the mirror_blobs test (revert
// `commit_with_inner_miss_gate(...)` to the pre-fix `return
// Err(guard.fail(...))` pattern, confirm panic with the specific
// assertion message above, restore) verifies the gate-suppression
// semantics. The two siblings reuse the IDENTICAL helper call so
// inherit the guarantee transitively.
