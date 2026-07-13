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

//! #500 — "silent 0-byte-ok response" regression for
//! `FastSlowStore::get_part` on the streaming-populate consumer loop.
//!
//! Mechanism (root cause): the consumer-loop end-of-range calculation
//! at `fast_slow_store.rs:6252` computes
//!     `let end = offset + length.unwrap_or(u64::MAX);`
//! which, in release mode, wraps for `offset > 0` AND `length == None`
//! (Bazel's parallel-chunk resume-read shape: `read_offset=N,
//! read_limit=0` → server-side `length=None`). `end` becomes
//! `offset - 1`; the per-chunk `pos < end` predicate is false from the
//! very first iteration; no `guard.send(...)` ever fires; the loop
//! falls through to `guard.commit_eof()` which signals canonical EOF
//! to the consumer with zero bytes written.
//!
//! `LoggingReadStream` (and any other consumer counting `bytes_sent`)
//! sees `Poll::Ready(None)` immediately, `status == "ok"`,
//! `bytes_sent == 0`. The blob is non-zero. Bazel reads the truncated
//! response as canonical, then "verifies" the zero bytes against the
//! declared content-addressed digest and surfaces a digest mismatch
//! N seconds later — far from the actual fault.
//!
//! Production composition the bug fires in (server side):
//!
//! ```text
//!   bytestream tx (per parallel chunk; Bazel uses 2-3 in parallel)
//!     │
//!     ▼
//!   WorkerProxyStore  (race_peers=false, IS_WORKER_REQUEST=false)
//!     │ inner.get_part(&mut tx, offset=N, length=None)
//!     ▼
//!   ExistenceCacheStore → VerifyStore
//!     ▼
//!   FastSlowStore::get_part — populating_digests arbitrates:
//!     - first caller becomes POPULATOR
//!     - producer fills the 64 MiB streaming buffer from slow tier
//!     - populator enters the streaming-buffer consumer loop at
//!       fast_slow_store.rs:6253 with offset=N, length=None
//!                    ▼
//!         end = N + u64::MAX  →  WRAPS to N - 1
//!         for every chunk: pos < end is FALSE  →  no send fires
//!         loop breaks when pos >= end  →  fall through
//!                    ▼
//!         guard.commit_eof()  →  Ok with bytes_written=0
//! ```
//!
//! Test design (production composition, deadlock-protected, bespoke-message):
//!  - Real `FastSlowStore` with `MemoryStore` fast + `MemoryStore`
//!    slow. The blob is only in slow → populator path engages.
//!  - Wrap in `VerifyStore { verify_size: true }` so callers above the
//!    populator (bytestream → WorkerProxyStore → ExistenceCacheStore
//!    → VerifyStore → FastSlowStore) see exactly the production
//!    error-shape if the contract is violated.
//!  - Call `get_part(digest, &mut writer, offset, length=None)` with a
//!    non-zero offset on a 2 MiB blob. Drain the receiver concurrently.
//!  - 5-second `tokio::time::timeout` is the deadlock detector. The
//!    bespoke `.expect(...)` message names the exact bug class so a
//!    future maintainer can grep the panic to the diagnosis.
//!
//! With the bug present:
//!  - `get_part` returns `Ok(())`.
//!  - `bytes_written` (the writer's running total) is exactly 0.
//!  - The wrapped `VerifyStore` then commits EOF with zero bytes for a
//!    non-zero digest — a silent 0-byte-ok response.
//!
//! With the fix:
//!  - `end` is `None` (unbounded — caller passed `length=None`), so
//!    the loop sends the slice `[offset .. blob_end]`.
//!  - `get_part` returns `Ok(())` with `bytes_written ==
//!    blob_size - offset`.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    FastSlowSpec, MemorySpec, StoreDirection, StoreSpec, VerifySpec,
};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};

const MEGABYTE_SZ: u64 = 1024 * 1024;

const VALID_HASH: &str =
    "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Production-composition regression test for #500: `FastSlowStore::get_part`
/// must NOT commit a silent 0-byte EOF when called with `offset > 0` AND
/// `length == None` against the streaming-populate consumer loop.
///
/// Bespoke panic message names the exact mechanism (consumer-loop
/// integer overflow in end-of-range computation) so the next failure
/// surfacing on this site grep-locates the root-cause diagnosis.
#[nativelink_test]
async fn populate_get_part_partial_offset_no_length_must_not_silent_zero()
-> Result<(), Error> {
    // 2 MiB blob with deterministic content so we can verify the
    // exact byte range returned matches the requested slice.
    let blob_size = 2 * MEGABYTE_SZ;
    let original: Vec<u8> = (0..blob_size as u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, blob_size).unwrap();

    // Tiered store: fast=empty MemoryStore, slow=MemoryStore-with-blob.
    // This forces the populator path: fast.get_part returns NotFound,
    // FastSlowStore spawns a producer to fill the streaming buffer
    // from slow, and the populating caller enters the consumer loop
    // at fast_slow_store.rs:6253.
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    slow.update_oneshot(digest, Bytes::from(original.clone())).await?;

    let fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast,
        slow,
    ));

    // Wrap in VerifyStore { verify_size: true } to mirror production
    // composition (CAS chain on buildcache is ExistenceCache → Verify →
    // FastSlow). verify_size DOES NOT engage on length=Some(_) only
    // reads — for length=None reads it expects the full size, so any
    // size-mismatch surfaces here.
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        fast_slow,
    ));

    // Request bytes [offset .. EOF] with offset > 0 and length=None.
    // This is exactly Bazel's parallel-chunk resume-read shape: the
    // gRPC `read_offset=N, read_limit=0` translates to server-side
    // `(offset=N, length=None)` per `bytestream_server.rs`.
    let offset = MEGABYTE_SZ; // 1 MiB into a 2 MiB blob → expect 1 MiB
    let length: Option<u64> = None;
    let expected_bytes = blob_size - offset;

    let (writer, mut reader) = make_buf_channel_pair();
    // Move `writer` INTO get_fut so it drops when get_part returns,
    // not at outer-scope-end. Matches the production seam shape and
    // ensures the drain_fut observes EOF immediately on inner
    // termination. `StoreLike::get_part` accepts `impl
    // BorrowMut<DropCloserWriteHalf>` so passing by value is the
    // documented seam.
    let get_fut = async move {
        verify
            .get_part(digest, writer, offset, length)
            .await
            .err_tip(|| "get_part on (offset>0, length=None)")
    };
    let drain_fut = async move {
        let mut total: u64 = 0;
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            total += chunk.len() as u64;
        }
        Ok::<u64, Error>(total)
    };

    // 5-second deadlock detector. The bespoke `.expect(...)` message
    // is the failure-class label — a future panic on this site greps
    // to #500.
    let (get_res, drain_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async move { tokio::join!(get_fut, drain_fut) },
    )
    .await
    .expect(
        "must not deadlock — get_part offset>0 length=None must produce \
         bytes via streaming-populate consumer loop within 5s; if this \
         panic fires, the bug is composition/wiring (not the silent-zero \
         class); compare with the assertions below.",
    );

    // With the BUG: both futures complete cleanly (no deadlock) and
    // bytes_drained == 0 — the silent zero-byte-ok response. With the
    // FIX: bytes_drained == expected_bytes (blob_size - offset).
    let bytes_drained = drain_res.expect(
        "reader drain returned Err — composition broke between writer \
         and reader (NOT the silent-zero class)",
    );

    if get_res.is_ok() {
        assert_ne!(
            bytes_drained, 0,
            "silent 0-byte-ok response on (offset={offset}, length=None) for \
             non-zero digest size={blob_size} — FastSlowStore consumer-loop \
             integer overflow in end-of-range computation at \
             fast_slow_store.rs:6252 (`let end = offset + \
             length.unwrap_or(u64::MAX);` wraps to `offset - 1` when \
             length=None and offset>0); loop breaks before any send fires; \
             guard.commit_eof() signals canonical EOF with zero bytes for \
             a non-zero digest. This is #500.",
        );
        assert_eq!(
            bytes_drained, expected_bytes,
            "short-read on (offset={offset}, length=None): got \
             {bytes_drained} bytes, expected {expected_bytes} \
             (blob_size {blob_size} − offset {offset}). Consumer-loop \
             end-of-range computation incorrect.",
        );
    } else {
        // The bug produces Ok(()) + zero bytes. If get_part returns
        // Err instead, the bug is a DIFFERENT (related) failure mode
        // — surface that as well rather than masking with is_ok().
        panic!(
            "get_part returned Err on (offset={offset}, length=None) \
             against a populated 2 MiB blob: {:?}. Pre-#500 baseline \
             was Ok+0-bytes (silent); post-fix should be Ok with \
             {expected_bytes} bytes drained. This Err indicates a \
             secondary regression in the streaming-populate path.",
            get_res.unwrap_err(),
        );
    }

    Ok(())
}

/// Companion regression: the (offset>0, length=None) case must also
/// hold when the SAME blob is requested twice concurrently — exercises
/// the WAITER branch of the streaming-populate path in addition to the
/// POPULATOR branch. The Bazel parallel-chunk read fires 2-3 ranged
/// reads simultaneously; in production exactly one becomes the
/// populator and the rest wait. With the bug, ANY caller (populator
/// or waiter) that reaches the consumer loop with length=None silently
/// truncates.
///
/// Note: This test composes a raw `FastSlowStore` (without the
/// VerifyStore wrapper used by `populate_get_part_partial_offset_no_length_must_not_silent_zero`),
/// because the consumer-loop overflow is intrinsic to FastSlowStore's
/// streaming-populate path and a wrapper would mask the silent-zero
/// behavior behind a size-mismatch Err. The single-populator companion
/// test above covers the CAS-chain composition; this test isolates the
/// concurrent populator/waiter interaction.
#[nativelink_test]
async fn concurrent_offset_no_length_must_not_silent_zero() -> Result<(), Error> {
    let blob_size = 2 * MEGABYTE_SZ;
    let original: Vec<u8> = (0..blob_size as u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, blob_size).unwrap();

    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    slow.update_oneshot(digest, Bytes::from(original.clone())).await?;

    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast,
        slow,
    );

    let offset = MEGABYTE_SZ;
    let expected_bytes = blob_size - offset;

    // Three concurrent (offset>0, length=None) reads against the
    // SAME digest. With the bug, all three return Ok+0-bytes
    // (populator and waiters both go through the buggy consumer
    // loop). With the fix, all three deliver `expected_bytes`.
    let mk_attempt = |store: Arc<FastSlowStore>| async move {
        let (writer, mut reader) = make_buf_channel_pair();
        let drain_fut = async move {
            let mut total: u64 = 0;
            loop {
                let chunk = reader.recv().await?;
                if chunk.is_empty() {
                    break;
                }
                total += chunk.len() as u64;
            }
            Ok::<u64, Error>(total)
        };
        let get_fut = async move {
            store.get_part(digest, writer, offset, None).await
        };
        let (get_res, drain_res) = tokio::join!(get_fut, drain_fut);
        let drained = drain_res?;
        get_res?;
        Ok::<u64, Error>(drained)
    };

    let (r1, r2, r3) = tokio::time::timeout(
        Duration::from_secs(5),
        async {
            tokio::join!(
                mk_attempt(fast_slow.clone()),
                mk_attempt(fast_slow.clone()),
                mk_attempt(fast_slow.clone()),
            )
        },
    )
    .await
    .map_err(|_| {
        make_err!(
            Code::DeadlineExceeded,
            "must not deadlock — three concurrent (offset>0, length=None) reads must complete within 5s"
        )
    })?;

    for (idx, res) in [&r1, &r2, &r3].iter().enumerate() {
        let drained = res.as_ref().unwrap_or_else(|err| {
            panic!(
                "concurrent reader #{idx} failed: {err:?} — pre-#500 \
                 baseline was Ok+0-bytes (silent); post-fix should be \
                 Ok with {expected_bytes} bytes",
            )
        });
        assert_ne!(
            *drained, 0,
            "silent 0-byte-ok response on concurrent reader #{idx} for \
             (offset={offset}, length=None) on non-zero digest — \
             FastSlowStore consumer-loop integer overflow in end-of-range \
             computation at fast_slow_store.rs:6252 affects both POPULATOR \
             and WAITER paths; this is #500.",
        );
        assert_eq!(
            *drained, expected_bytes,
            "reader #{idx} short-read on (offset={offset}, length=None): \
             got {drained} expected {expected_bytes}",
        );
    }

    Ok(())
}
