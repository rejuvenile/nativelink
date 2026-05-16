// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #500 — `WorkerProxyStore` defensive 0-byte-Ok guard must cover the
//! chunked (`length=Some(N)`) path, not only the whole-blob
//! (`length=None`) path.
//!
//! ## Root cause
//!
//! Both `WorkerProxyStore::try_read_from_worker` and
//! `WorkerProxyStore::try_read_from_endpoints` consume the peer's
//! stream into the OUTER writer via
//! `WorkerProxyStore::get_part_and_cache`, then check on the
//! `Ok(())` branch whether
//! the peer actually delivered bytes. The pre-fix logic on the worker
//! side was:
//!
//! ```ignore
//! let was_full_read = current_offset == offset
//!     && remaining_length == length
//!     && length.is_none();          // ← THE BUG: only fires when length is None
//! if bytes_written_this_peer == 0
//!     && expected_size > 0
//!     && was_full_read              // ← chunked length=Some(N) BYPASSES the guard
//! { ... evict ... }
//! ```
//!
//! The endpoint side had NO guard at all — any `Ok(())` with zero
//! bytes propagated straight to the consumer.
//!
//! Production trigger: Bazel parallel-chunk reads split a single
//! `Read` into N range-requests, each carrying `length=Some(chunk_size)`.
//! When a peer is reachable but its inner store has shed the blob
//! (race with eviction, or stale locality entry surviving a worker
//! restart), the peer can return `Ok` with zero bytes — `Read` over
//! gRPC ByteStream terminates cleanly with `OK` trailer, no data
//! frames in between. The pre-fix WPS accepted that as canonical EOF
//! for the chunked request, signalled `Ok` upstream, and the
//! aggregating consumer assembled `N * 0 = 0` bytes for a non-zero
//! blob — silent truncation.
//!
//! ## Test design
//!
//! Two production-composition tests, one per call-site:
//!
//! 1. `try_read_from_worker_chunked_length_guards_zero_byte_ok`
//!    targets `try_read_from_worker`. Inner store returns NotFound
//!    so the request falls through to the locality-map peer path;
//!    fake peer always returns `Ok` with zero bytes for any
//!    `(offset, length)`. With the bug, WPS returns `Ok` upstream and
//!    bytes_drained == 0. With the fix, WPS evicts the locality entry
//!    and returns a NotFound (no more peers to try).
//!
//! 2. `try_read_from_endpoints_chunked_length_guards_zero_byte_ok`
//!    targets `try_read_from_endpoints`. Inner store returns
//!    `FailedPrecondition` with the `REDIRECT_PREFIX{peer}|` envelope
//!    that triggers the redirect branch; fake peer returns `Ok` with
//!    zero bytes. Same expected post-fix behavior.
//!
//! Each test wraps the WPS in a `tokio::time::timeout(5s)` deadlock
//! detector. The bespoke `expect` / panic messages name the call-site
//! and the bug class for trivially-grep-able regression localization.
//!
//! ## Mutation
//!
//! Revert the corresponding fix; the test MUST red-fail with the
//! "silent 0-byte-Ok response on chunked length=Some(N)" message.

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, REDIRECT_PREFIX, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};

const VALID_HASH1: &str =
    "0123456789abcdef000000000000000000010000000000000123456789abcdef";

// ----- Fake peer that always returns Ok+0-bytes (silent EOF) -------

/// A fake peer store whose `get_part` opens the writer, never sends
/// any chunks, then commits a clean EOF. Returns `Ok(())`. Models a
/// peer on a stale-locality, post-eviction race: the peer is healthy
/// (RPC trailer is `OK`) but it has no data for the requested digest.
#[derive(Debug, MetricsComponent)]
struct OkEmptyPeerStore {
    // Required so MetricsComponent derive has a struct body.
    #[metric(help = "unused")]
    _unused: u64,
}

default_health_status_indicator!(OkEmptyPeerStore);

#[async_trait]
impl StoreDriver for OkEmptyPeerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Report present — mirrors a stale locality-positive on the
        // peer side. The bug fires on the get_part side regardless.
        for (i, _) in digests.iter().enumerate() {
            if i < results.len() {
                results[i] = Some(0);
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::Unimplemented,
            "OkEmptyPeerStore does not support update"
        ))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // Silent EOF: no chunks, then clean OK trailer.
        writer
            .send_eof()
            .err_tip(|| "OkEmptyPeerStore: send_eof failed")?;
        Ok(())
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

// ----- Inner-store wrapper that emits REDIRECT_PREFIX -----

/// Inner store that immediately returns
/// `Code::FailedPrecondition` carrying the redirect envelope WPS
/// parses to enter `try_read_from_endpoints`.
#[derive(Debug, MetricsComponent)]
struct RedirectInnerStore {
    endpoint: String,
}

default_health_status_indicator!(RedirectInnerStore);

#[async_trait]
impl StoreDriver for RedirectInnerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for (i, _) in digests.iter().enumerate() {
            if i < results.len() {
                results[i] = None;
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::Unimplemented,
            "RedirectInnerStore does not support update"
        ))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::FailedPrecondition,
            "{REDIRECT_PREFIX}{}|",
            self.endpoint
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

// ----- Helper: drain a reader counting bytes until EOF -----

async fn drain_count(reader: DropCloserReadHalf) -> Result<u64, Error> {
    let mut reader = reader;
    let mut total: u64 = 0;
    loop {
        let chunk = reader.recv().await?;
        if chunk.is_empty() {
            break;
        }
        total += chunk.len() as u64;
    }
    Ok(total)
}

// ===================================================================
// Test 1: try_read_from_worker — chunked (length=Some) path
// ===================================================================

/// Reaches `try_read_from_worker` by:
///  - inner store returns NotFound for the digest
///  - locality_map carries the fake peer endpoint
///
/// The pre-fix guard inside `WorkerProxyStore::try_read_from_worker`
/// (the `was_full_read` predicate on the `Ok(())` arm of the
/// streaming `get_part_and_cache` attempt) required `length.is_none()`
/// to fire, so a `length=Some(_)` chunked request bypassed it and
/// returned `Ok` + 0 bytes silently.
///
/// Post-fix: the guard fires for `length=Some(_)` too; WPS evicts the
/// locality entry, retries with no remaining peers, and surfaces a
/// `NotFound` to the caller.
#[nativelink_test]
async fn try_read_from_worker_chunked_length_guards_zero_byte_ok() -> Result<(), Error> {
    let blob_size: u64 = 2 * 1024 * 1024;
    let chunk_length: u64 = 1024 * 1024; // 1 MiB chunked request
    let chunk_offset: u64 = 0;
    let digest = DigestInfo::try_new(VALID_HASH1, blob_size).unwrap();

    // Inner is an EMPTY MemoryStore (so inner.get_part returns
    // NotFound, falling through to the locality-map peer path).
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());

    // Register the fake peer in both the locality map AND the
    // worker_connections map.
    let peer_endpoint = "grpc://ok-empty-peer:50081";
    let peer = Store::new(Arc::new(OkEmptyPeerStore { _unused: 0 }));
    proxy_arc.inject_worker_connection(peer_endpoint, peer);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let proxy = Store::new(proxy_arc);

    // Request a chunked range with length=Some(N) — exactly Bazel's
    // parallel-chunk read shape.
    let (writer, reader) = make_buf_channel_pair();
    let get_fut = async {
        proxy
            .get_part(digest, writer, chunk_offset, Some(chunk_length))
            .await
    };
    let drain_fut = drain_count(reader);

    let (get_res, drain_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(get_fut, drain_fut) },
    )
    .await
    .expect(
        "must not deadlock — chunked length=Some(N) Read via \
         try_read_from_worker must complete within 5s; if this panic \
         fires, the deadlock is unrelated to the silent-zero class.",
    );

    let bytes_drained = drain_res.expect(
        "reader drain returned Err — composition broke between writer \
         and reader (NOT the silent-zero class)",
    );

    // Post-fix invariant: a chunked request against a peer that
    // returned Ok+0-bytes for a non-zero digest MUST NOT surface as
    // Ok+0-bytes upstream. The fix evicts the locality entry and the
    // request fails (no more peers → NotFound).
    if get_res.is_ok() {
        assert_ne!(
            bytes_drained, 0,
            "silent 0-byte-Ok response on chunked length=Some({chunk_length}) \
             — WorkerProxyStore::try_read_from_worker pre-fix `was_full_read` \
             guard only covered length.is_none(); chunked reads bypassed the \
             guard and a peer returning Ok+0-bytes for a non-zero digest \
             (size={blob_size}) propagated as canonical EOF. This is #500 \
             (sibling 1 of 2: WPS chunked-read guard miss).",
        );
    } else {
        let err = get_res.unwrap_err();
        // Acceptable post-fix outcomes: NotFound (no more peers, or
        // locality already evicted) — anything that DOESN'T silently
        // truncate is fine.
        assert!(
            matches!(err.code, Code::NotFound),
            "post-fix WPS should surface NotFound (peer evicted, no more peers) \
             but got {:?}: {err:?}",
            err.code,
        );
        // No bytes should have been written upstream — the guard
        // refuses to commit the empty stream.
        assert_eq!(
            bytes_drained, 0,
            "post-fix WPS must NOT forward any bytes when evicting; \
             got {bytes_drained}",
        );
    }

    Ok(())
}

// ===================================================================
// Test 2: try_read_from_endpoints — chunked (length=Some) path
// ===================================================================

/// Reaches `try_read_from_endpoints` by:
///  - inner store returns `FailedPrecondition` with `REDIRECT_PREFIX`
///    pointing at the fake peer
///
/// The pre-fix `WorkerProxyStore::try_read_from_endpoints` `Ok(())`
/// arm of the per-endpoint `get_part_and_cache` attempt had NO
/// 0-byte guard at all; ANY peer returning Ok with empty body
/// propagated as canonical EOF, regardless of length=None vs
/// length=Some.
///
/// Post-fix: the same eviction guard is installed; WPS surfaces
/// NotFound to the caller.
#[nativelink_test]
async fn try_read_from_endpoints_chunked_length_guards_zero_byte_ok() -> Result<(), Error> {
    let blob_size: u64 = 2 * 1024 * 1024;
    let chunk_length: u64 = 1024 * 1024;
    let chunk_offset: u64 = 0;
    let digest = DigestInfo::try_new(VALID_HASH1, blob_size).unwrap();

    let peer_endpoint = "grpc://ok-empty-peer-redirect:50081";

    // Inner store always returns FailedPrecondition+REDIRECT_PREFIX.
    let inner = Store::new(Arc::new(RedirectInnerStore {
        endpoint: peer_endpoint.to_string(),
    }));
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());

    // Inject fake peer; do NOT register in locality_map (the redirect
    // is what routes us to try_read_from_endpoints, not the locality
    // map).
    let peer = Store::new(Arc::new(OkEmptyPeerStore { _unused: 0 }));
    proxy_arc.inject_worker_connection(peer_endpoint, peer);

    let proxy = Store::new(proxy_arc);

    let (writer, reader) = make_buf_channel_pair();
    let get_fut = async {
        proxy
            .get_part(digest, writer, chunk_offset, Some(chunk_length))
            .await
    };
    let drain_fut = drain_count(reader);

    let (get_res, drain_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(get_fut, drain_fut) },
    )
    .await
    .expect(
        "must not deadlock — chunked length=Some(N) Read via \
         try_read_from_endpoints must complete within 5s; if this \
         panic fires, the deadlock is unrelated to the silent-zero \
         class.",
    );

    let bytes_drained = drain_res.expect(
        "reader drain returned Err — composition broke between writer \
         and reader (NOT the silent-zero class)",
    );

    if get_res.is_ok() {
        assert_ne!(
            bytes_drained, 0,
            "silent 0-byte-Ok response on chunked length=Some({chunk_length}) \
             — WorkerProxyStore::try_read_from_endpoints had NO 0-byte-Ok \
             guard; a peer returning Ok+0-bytes for a non-zero digest \
             (size={blob_size}) propagated as canonical EOF. This is #500 \
             (sibling 2 of 2: WPS endpoint-redirect guard miss).",
        );
    } else {
        let err = get_res.unwrap_err();
        // Post-fix acceptable outcomes: NotFound (all peers exhausted),
        // or the inner store's residual redirect (FailedPrecondition)
        // after every peer was evicted and the inner-store retry path
        // re-fires the same redirect. The KEY invariant: the call must
        // NOT silently swallow the peer's empty-stream as canonical.
        assert!(
            matches!(err.code, Code::NotFound | Code::FailedPrecondition),
            "post-fix WPS should surface NotFound or FailedPrecondition (no \
             peers, or inner-store residual redirect) but got {:?}: {err:?}",
            err.code,
        );
        assert_eq!(
            bytes_drained, 0,
            "post-fix WPS must NOT forward any bytes when evicting; \
             got {bytes_drained}",
        );
    }

    Ok(())
}

// ===================================================================
// Test 3: try_read_from_worker — multi-peer per-attempt scope
//
// Perf MAJOR-1: the 0-byte-Ok guard inside try_read_from_worker
// initially subtracted from `bytes_before_proxy` (the pre-LOOP byte
// count), not from a per-attempt baseline. When peer A partial-Err'd
// (writes some bytes, then returns a NON-evicting Err like
// Code::Unavailable), the resume math advanced current_offset by the
// partial bytes and continued to peer B. Peer B then returned Ok+0
// bytes; the guard observed
// `bytes_written_in_attempt = (A_partial + 0) - 0 = A_partial`,
// which is NON-zero, so the guard did NOT fire and the silent-zero
// propagated upstream as canonical EOF.
//
// Post-fix: the guard captures `bytes_before_attempt` immediately
// before the per-peer get_part_and_cache call, so the delta observed
// is ONLY the bytes peer B contributed (0). The guard fires; locality
// is evicted; the next iteration exhausts peers and the call surfaces
// an Err to the caller.
// ===================================================================

/// A fake peer that writes `partial_bytes` bytes of arbitrary content
/// before returning `Err(Code::Unavailable, ...)`. `Unavailable` is
/// NOT in `should_evict_locality_on_peer_error`'s eviction set
/// (NotFound | DataLoss), so the WPS retains the locality entry and
/// resumes from the next peer at `offset + partial_bytes`.
#[derive(Debug, MetricsComponent)]
struct PartialThenUnavailablePeerStore {
    #[metric(help = "bytes to write before erroring")]
    partial_bytes: u64,
}

default_health_status_indicator!(PartialThenUnavailablePeerStore);

#[async_trait]
impl StoreDriver for PartialThenUnavailablePeerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for (i, _) in digests.iter().enumerate() {
            if i < results.len() {
                results[i] = Some(0);
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::Unimplemented,
            "PartialThenUnavailablePeerStore does not support update"
        ))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // Write partial_bytes of filler content, then return a
        // transient Err. WPS will retain the locality entry (Unavailable
        // is non-evicting), advance current_offset by partial_bytes,
        // and try the next peer.
        let n = usize::try_from(self.partial_bytes).unwrap_or(0);
        if n > 0 {
            let buf = Bytes::from(vec![0xABu8; n]);
            writer
                .send(buf)
                .await
                .err_tip(|| "PartialThenUnavailablePeerStore: send failed")?;
        }
        Err(make_err!(
            Code::Unavailable,
            "PartialThenUnavailablePeerStore: synthetic transient err after \
             writing {} bytes",
            self.partial_bytes
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

/// Multi-peer resume regression: peer A writes 500 bytes then returns
/// non-evicting Err; peer B returns Ok+0. The pre-fix guard at the
/// streaming-path `Ok(())` arm used `bytes_before_proxy` (pre-loop) as
/// the subtrahend, so peer B's per-attempt 0-byte delta was masked by
/// peer A's accumulated 500 bytes — guard did NOT fire and the silent
/// truncation propagated upstream. Post-fix the guard uses a
/// per-attempt baseline captured just before each
/// `get_part_and_cache` call, observing peer B's true 0-byte delta and
/// evicting/erroring as designed.
#[nativelink_test]
async fn try_read_from_worker_multi_peer_partial_err_then_zero_ok_guards()
-> Result<(), Error> {
    let blob_size: u64 = 2 * 1024 * 1024;
    let chunk_length: u64 = 1024 * 1024;
    let chunk_offset: u64 = 0;
    let digest = DigestInfo::try_new(VALID_HASH1, blob_size).unwrap();

    // Inner store: NotFound → fall through to locality-map peer path.
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());

    // Two peers: A (partial-then-Unavailable) and B (Ok+0). Register A
    // FIRST so WPS attempts A then B in that order (EndpointList is
    // insertion-ordered).
    let peer_a_endpoint = "grpc://partial-err-peer-a:50081";
    let peer_b_endpoint = "grpc://ok-empty-peer-b:50081";
    let partial_bytes: u64 = 500;

    let peer_a = Store::new(Arc::new(PartialThenUnavailablePeerStore {
        partial_bytes,
    }));
    let peer_b = Store::new(Arc::new(OkEmptyPeerStore { _unused: 0 }));

    proxy_arc.inject_worker_connection(peer_a_endpoint, peer_a);
    proxy_arc.inject_worker_connection(peer_b_endpoint, peer_b);

    {
        let mut map = locality_map.write();
        map.register_blobs(peer_a_endpoint, &[digest]);
        map.register_blobs(peer_b_endpoint, &[digest]);
    }

    let proxy = Store::new(proxy_arc);

    let (writer, reader) = make_buf_channel_pair();
    let get_fut = async {
        proxy
            .get_part(digest, writer, chunk_offset, Some(chunk_length))
            .await
    };
    let drain_fut = drain_count(reader);

    let (get_res, drain_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(get_fut, drain_fut) },
    )
    .await
    .expect(
        "must not deadlock — multi-peer resume (A partial+Err, B Ok+0) must \
         complete within 5s; if this panic fires, the deadlock is unrelated \
         to the silent-zero class.",
    );

    let bytes_drained = drain_res.expect(
        "reader drain returned Err — composition broke between writer \
         and reader (NOT the silent-zero class)",
    );

    // Post-fix invariant: per-attempt scope ensures peer B's 0-byte
    // contribution is observed even after peer A's partial write. The
    // call must NOT surface as Ok + only-peer-A-bytes.
    if get_res.is_ok() {
        panic!(
            "silent 0-byte-ok on multi-peer resume (A partial+Err, B Ok+0) — \
             worker_proxy_store.rs:1551 guard used pre-loop scope instead of \
             per-attempt — perf MAJOR-1 #500. \
             bytes_drained={bytes_drained} (peer A wrote {partial_bytes} \
             before non-evicting Err; peer B returned Ok+0 and the pre-fix \
             guard's `bytes_before_proxy` subtrahend masked B's per-attempt \
             zero by A's accumulated bytes)."
        );
    } else {
        let err = get_res.unwrap_err();
        // Acceptable post-fix outcomes: NotFound (peer B evicted, no
        // more peers), FailedPrecondition (chained), or any non-Ok
        // surface that prevents the silent truncation.
        assert!(
            !matches!(err.code, Code::Ok),
            "post-fix WPS must NOT return Code::Ok on the multi-peer \
             partial+silent-zero scenario; got {:?}: {err:?}",
            err.code,
        );
    }

    Ok(())
}
