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

//! #212 Phase 2.7 fixup D6 — Str-keyed `update()` must skip the
//! chunked-dispatch gate entirely.
//!
//! When the `chunked_fast_slow` kill-switch is ON and a chunked
//! dispatcher has been installed, the gate at
//! `fast_slow_store.rs::update` previously called
//! `key.borrow().into_digest()` BEFORE checking the size threshold.
//! For Str-keyed AC entries, that synthesizes a digest by Blake3-
//! hashing the entire key string — hot-path cost on every AC update
//! once the kill-switch is flipped ON in production (code-reviewer
//! §MAJOR 3, deferred follow-up D6).
//!
//! Fix: gate on `StoreKey::Digest` first; Str-keyed updates take the
//! legacy decoupled path with zero hashing.
//!
//! Coverage:
//!   1. Str-keyed `update()` (the streaming path the Bazel server
//!      actually hits) with kill-switch ON + tiny threshold +
//!      dispatcher installed: dispatcher MUST NOT be invoked, write
//!      MUST succeed, blob MUST be readable through the FastSlowStore
//!      (semantic-equivalence to kill-switch OFF).
//!   2. Positive control: same setup but with a Digest key larger than
//!      the threshold — dispatcher MUST be invoked exactly once.
//!
//! Mutation step: revert the `if let StoreKey::Digest(...)` gate to the
//! original `key.borrow().into_digest()` form and re-run; test (1)
//! red-fails with "dispatcher invoked for Str-keyed update".
//!
//! Note: the override `update_oneshot` on `FastSlowStore` does NOT route
//! through the chunked-dispatch gate (it has its own bypassing path
//! that writes the fast tier inline + spawns a background slow-tier
//! task). The Bazel server's gRPC handler hits `update()` (streaming
//! reader), so the test drives `update()` via a buf channel directly.

#![cfg(feature = "chunked_fast_slow")]

use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::chunked::{
    BazelChunkedDispatcher, BazelChunkedDispatcherArc, disable_bazel_facing_internal_chunking,
    enable_bazel_facing_internal_chunking,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;

/// Process-wide kill-switch is global state. Cargo runs `#[nativelink_test]`
/// integration tests on a multi-threaded runtime; serialize ourselves so
/// a sibling test toggling the same switch doesn't observe a flipped state.
fn kill_switch_lock() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Fake dispatcher that bumps a counter every time `dispatch` is called
/// and drains the reader to EOF (so the data-stream future doesn't
/// stall on backpressure). The counter is visible to the test via the
/// `Arc<AtomicUsize>` it shares with the constructor.
#[derive(Debug)]
struct CountingDispatcher {
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl BazelChunkedDispatcher for CountingDispatcher {
    async fn dispatch(
        &self,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
    ) -> Result<u64, Error> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        // Drain the reader so the upstream tee future can complete.
        loop {
            let buf = reader.recv().await?;
            if buf.is_empty() {
                break;
            }
        }
        Ok(digest.size_bytes())
    }
}

fn make_fast_slow() -> (Arc<FastSlowStore>, Store, Store) {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast.clone(),
        slow.clone(),
    );
    (fss, fast, slow)
}

/// Drive `StoreLike::update` (the streaming path; the chunked-dispatch
/// gate lives here, NOT in `update_oneshot`) with a single-shot
/// payload. Mirrors what the Bazel server's gRPC ByteStream Write
/// handler does to a `FastSlowStore`.
async fn drive_update(store: &Store, key: StoreKey<'_>, payload: Bytes) -> Result<(), Error> {
    let (mut tx, rx) = make_buf_channel_pair();
    let payload_len = payload.len() as u64;
    let send_fut = async move {
        tx.send(payload).await?;
        tx.send_eof()?;
        Ok::<(), Error>(())
    };
    let update_fut = store.update(key, rx, UploadSizeInfo::ExactSize(payload_len));
    let (send_res, update_res) = tokio::join!(send_fut, update_fut);
    send_res?;
    update_res
}

/// Test 1 — the D6 fix itself.
///
/// Kill-switch ON + tiny chunked-size threshold (1 byte) + dispatcher
/// installed. A Str-keyed `update()` must:
///   (a) succeed,
///   (b) write data to fast + slow tiers as normal,
///   (c) NOT invoke the chunked dispatcher even once,
///   (d) read back identical bytes.
///
/// The mutation step (revert the `StoreKey::Digest` gate) flips (c) to
/// "dispatcher invoked once" and the assertion red-fails with the
/// specific message below.
#[nativelink_test]
async fn str_keyed_update_skips_chunked_dispatcher() -> Result<(), Error> {
    let _guard = kill_switch_lock().lock().await;

    let (fss, _fast, _slow) = make_fast_slow();
    let invocations = Arc::new(AtomicUsize::new(0));
    let dispatcher: BazelChunkedDispatcherArc = Arc::new(CountingDispatcher {
        invocations: invocations.clone(),
    });
    fss.set_bazel_chunked_dispatcher(dispatcher);
    // 1-byte threshold so any non-empty payload would otherwise dispatch.
    fss.set_chunked_size_threshold_for_test(1);

    enable_bazel_facing_internal_chunking();

    // Str key shaped like a real AC entry name (long enough that an
    // accidental Blake3 hash on it would have a measurable cost).
    let key_str =
        "ac/some-instance/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789/4096";
    let key = StoreKey::from(key_str);
    let payload = Bytes::from_static(b"AC-style ActionResult payload bytes");

    let store: Store = Store::new(fss.clone());

    // Deadlock-detector timeout — a wedge in the data-stream/tee path
    // for Str keys would surface as a hang rather than a silent pass.
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, key.borrow(), payload.clone()),
    )
    .await
    .expect("update must not hang for Str-keyed update")?;

    let n = invocations.load(Ordering::SeqCst);

    // Restore kill-switch BEFORE the assertion so a failing assert
    // doesn't leak the toggle to sibling tests in the same process.
    disable_bazel_facing_internal_chunking();

    assert_eq!(
        n, 0,
        "dispatcher invoked for Str-keyed update — D6 gate (StoreKey::Digest first) regressed (count={n})",
    );

    // Behavior parity check: read-back works as if the kill-switch had
    // been OFF. The legacy decoupled path writes to the fast tier
    // inline + spawns the slow-tier write in the background; reads
    // resolve from the fast tier.
    let read_back = tokio::time::timeout(
        Duration::from_secs(5),
        store.get_part_unchunked(key.borrow(), 0, None),
    )
    .await
    .expect("get_part_unchunked must not hang for Str-keyed read-back")?;
    assert_eq!(
        read_back, payload,
        "Str-keyed read-back must equal the payload"
    );

    Ok(())
}

/// Test 2 — positive control. Confirms the dispatcher IS invoked for
/// Digest-keyed `update()` above the size threshold, so test 1's "0
/// invocations" assertion is meaningful (not just because the
/// dispatcher is broken everywhere).
#[nativelink_test]
async fn digest_keyed_update_invokes_chunked_dispatcher() -> Result<(), Error> {
    let _guard = kill_switch_lock().lock().await;

    let (fss, _fast, _slow) = make_fast_slow();
    let invocations = Arc::new(AtomicUsize::new(0));
    let dispatcher: BazelChunkedDispatcherArc = Arc::new(CountingDispatcher {
        invocations: invocations.clone(),
    });
    fss.set_bazel_chunked_dispatcher(dispatcher);
    fss.set_chunked_size_threshold_for_test(1);

    enable_bazel_facing_internal_chunking();

    // Digest key with size_bytes >= threshold (1 byte) — eligible.
    let payload = Bytes::from_static(b"some payload");
    let mut hash = [0u8; 32];
    hash[0] = 0x42;
    let digest = DigestInfo::new(hash, payload.len() as u64);
    let key: StoreKey<'static> = digest.into();

    let store: Store = Store::new(fss.clone());
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, key.borrow(), payload),
    )
    .await
    .expect("update must not hang for Digest-keyed update")?;

    let n = invocations.load(Ordering::SeqCst);

    // Restore kill-switch BEFORE the assertion so a failing assert
    // doesn't leak the toggle to sibling tests in the same process.
    disable_bazel_facing_internal_chunking();

    assert_eq!(
        n, 1,
        "dispatcher must be invoked exactly once for Digest-keyed update over threshold (count={n}); \
         if 0, the chunked-dispatch wiring itself is broken and test 1's 0-invocation assertion proves nothing",
    );

    Ok(())
}
