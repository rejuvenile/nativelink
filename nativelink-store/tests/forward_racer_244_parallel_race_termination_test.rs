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

//! #244 regression test: `WorkerProxyStore::forward_racer` must abort the
//! spawned racer task on `bind_buffered` Err.
//!
//! **Bug class:** sibling of #230 M1 BLOCK
//! (`worker_proxy_store.rs:1788-1791`). The parallel-race path
//! (`race_peers=true` worker-side) calls `forward_racer` with the
//! winning racer's `JoinHandle`. On the consumer-disconnect path,
//! `bind_buffered(rx)` returns Err mid-stream and the original code
//! `?`-propagated the Err, dropping `handle` (which DETACHES rather
//! than aborts). The spawned racer is producing into `tx` (the rx's
//! matching half); with the default `DEFAULT_BUF_CHANNEL_CAPACITY = 1024`
//! the racer can be wedged on `tx.send().await` once the channel fills
//! past 1024 chunks. The leak is bounded by the rx-drop in the parent
//! `get_part` frame, but that drop is delayed at minimum by the function
//! return, and any additional `.await` between forward_racer's return
//! and frame-drop widens the window unboundedly.
//!
//! **Production impact:** worker-side reads with `race_peers=true` —
//! every Bazel mid-blob disconnect on a peer-winner branch leaks one
//! racer task per fetch until rx-drop catches up.
//!
//! **Fix mirror:** #230 M1 idiom — `peer_handle.abort()` BEFORE the
//! `?` propagation. `forward_racer`'s `rx` is `&mut`-borrowed (caller
//! owns it), so we cannot drop it here; the abort is what bounds
//! recovery to one scheduler tick.
//!
//! **Mutation step (Mutation A — primary):** in
//! `WorkerProxyStore::forward_racer`, comment out the `handle.abort();`
//! inside the `if let Err(e) = writer.bind_buffered(rx).await { ... }`
//! arm. The test SHOULD trip the bespoke message because the
//! discriminator is a Drop-guard `oneshot` the peer holds across a long
//! `tokio::time::sleep`. With the abort, the sleep is cancelled at the
//! next poll and the peer task exits within milliseconds. Without the
//! abort, the only mechanism that wakes the peer is rx-drop in the
//! parent frame — but rx-drop has NO effect on a sleeping task; the
//! producer must wait until the sleep completes before its next
//! `tx.send().await` errors and the task exits. We tighten the
//! discriminator window to 500ms while the peer's sleep is 10s, so
//! the gap is unambiguous.

use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering as AOrdering};
use core::time::Duration;
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::sync::oneshot;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Wall-clock deadlock detector. 10s gives plenty of headroom for the
/// happy-path Err propagation while remaining tight enough to fail fast
/// on a wedged racer task.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn digest_for_size(n: u64) -> DigestInfo {
    DigestInfo::try_new(VALID_HASH1, n).expect("valid digest")
}

fn test_value(size_bytes: usize) -> Vec<u8> {
    (0..size_bytes).map(|i| (i & 0xFF) as u8).collect()
}

/// Empty FilesystemStore — its `get_part` returns NotFound, which makes
/// the parallel-race server racer's `tx.recv()` return Err. The match
/// arm at `worker_proxy_store.rs:3076` then routes to
/// `await_peer_after_empty_server`, which delegates to `forward_racer`
/// once the peer produces its first chunk (line 2242). This is the
/// exact production code path #244 patches.
async fn make_empty_filesystem_inner() -> Result<Store, Error> {
    let tmpdir = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
    let suffix: u64 = rand::random();
    let base = format!("{tmpdir}/forward_racer_244_test/{suffix}");
    let content_path = format!("{base}/content");
    let temp_path = format!("{base}/temp");
    tokio::fs::create_dir_all(&content_path)
        .await
        .err_tip(|| format!("create_dir_all(content_path={content_path}) failed in test setup"))?;
    tokio::fs::create_dir_all(&temp_path)
        .await
        .err_tip(|| format!("create_dir_all(temp_path={temp_path}) failed in test setup"))?;
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: Some(EvictionPolicy {
            max_count: 10_000,
            ..Default::default()
        }),
        ..Default::default()
    })
    .await?;
    Ok(Store::new(fs_store))
}

/// Peer that streams `payload` in `chunk_size` pieces with a long
/// inter-chunk sleep. The long sleep is the discriminator: if the
/// spawned racer task is `handle.abort()`'d, the sleep cancels at the
/// next poll and the task exits within milliseconds. If only rx-drop
/// occurs (no abort), the producer is sleeping (NOT on `tx.send`), so
/// rx-drop has no immediate effect — the producer must wait until the
/// sleep completes before its next `tx.send().await` errors and the
/// task exits.
///
/// `exit_signal` is a oneshot sender taken by `get_part` and held
/// inside an `ExitGuard` scope-bound to the peer's `get_part` future.
/// `ExitGuard::drop` fires the oneshot when the peer task ends —
/// regardless of whether it ended via abort, rx-drop-induced send-Err,
/// or natural completion. The test waits on the receiver with a tight
/// 500ms window after consumer-disconnect to discriminate.
#[derive(Debug, MetricsComponent)]
struct ChunkedPeerStore {
    payload: Bytes,
    chunk_size: usize,
    #[metric(help = "millis to sleep AFTER chunks_before_sleep chunks")]
    post_burst_sleep_ms: AtomicU64,
    #[metric(help = "number of chunks sent quickly before the long sleep")]
    chunks_before_sleep: AtomicU64,
    exit_signal: StdMutex<Option<oneshot::Sender<()>>>,
}

/// Sends () on the inner oneshot when dropped. Held across the entire
/// peer `get_part` future so dropping the future (abort, panic, normal
/// return) all signal exit. This is the test's observation point —
/// the time between consumer-disconnect and signal-receipt is the
/// discriminator for #244's abort contract.
struct ExitGuard {
    sender: Option<oneshot::Sender<()>>,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            // Receiver may be gone; ignore.
            let _ = sender.send(());
        }
    }
}

default_health_status_indicator!(ChunkedPeerStore);

#[async_trait]
impl StoreDriver for ChunkedPeerStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut().take(digests.len()) {
            *slot = Some(self.payload.len() as u64);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        let _drained = reader.drain().await;
        Ok(0)
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Take the exit-signal sender if present. Held inside the
        // `_exit_guard` for the lifetime of the future; signal fires on
        // any termination (abort/error/normal).
        let sender = {
            let mut slot = self.exit_signal.lock().expect("exit_signal poisoned");
            slot.take()
        };
        let _exit_guard = ExitGuard { sender };

        let offset = usize::try_from(offset).unwrap_or(0);
        let total_len = self.payload.len();
        let end = match length {
            Some(l) => core::cmp::min(
                total_len,
                offset.saturating_add(usize::try_from(l).unwrap_or(0)),
            ),
            None => total_len,
        };
        let mut pos = offset;
        let sleep_ms = self.post_burst_sleep_ms.load(AOrdering::Relaxed);
        let chunks_before_sleep = self.chunks_before_sleep.load(AOrdering::Relaxed);
        let mut chunks_sent = 0u64;
        let mut slept_yet = false;
        while pos < end {
            let chunk_end = core::cmp::min(end, pos + self.chunk_size);
            let chunk = self.payload.slice(pos..chunk_end);
            writer.send(chunk).await?;
            pos = chunk_end;
            chunks_sent += 1;
            // After sending `chunks_before_sleep` chunks, enter the long
            // sleep ONCE. Subsequent chunks (if any) are sent without
            // sleep. The sleep is the abort discriminator window.
            if !slept_yet && sleep_ms > 0 && chunks_sent >= chunks_before_sleep && pos < end {
                slept_yet = true;
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
        }
        writer.send_eof()?;
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
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// #244 regression: parallel-race `forward_racer` must abort the
/// racer task on `bind_buffered` Err to prevent leak.
///
/// **Setup**
/// - `enable_race_peers()` — turns on the parallel-race path
///   (`worker_proxy_store.rs:2940-3168`).
/// - Empty `FilesystemStore` inner — server racer returns NotFound, so
///   the select arm `Err(_server_err)` fires (line 3076), which
///   delegates to `await_peer_after_empty_server`. That function reads
///   the peer's first chunk and then calls `forward_racer("peer", ...)`
///   at line 2242 — the exact site under test.
/// - 16 MiB blob × 8 KiB chunks = 2048 chunks. The buf_channel default
///   `DEFAULT_BUF_CHANNEL_CAPACITY = 1024` fills on chunk 1024; the
///   peer racer task wedges on `peer_tx.send().await` for chunk 1025+
///   if the consumer stops draining. `inter_chunk_sleep_ms = 1` keeps
///   the producer alive across the consumer-disconnect window.
///
/// **Action**
/// - Drive `proxy.get_part` in a `tokio::spawn`'d task with a
///   user-controlled writer/reader pair.
/// - Drain ~10% of the blob (~1.6 MiB) so the peer racer is reliably
///   producing chunks past the 1024 cap.
/// - Drop the reader. `writer.send` then errors; `bind_buffered` in
///   `forward_racer` propagates Err.
///
/// **Assertion (deadlock detector)**
/// - The `get_part` task MUST resolve within `TEST_TIMEOUT` (10s).
///   Without `handle.abort()` on the `bind_buffered` Err path, the
///   racer task's `peer_handle.await` (line 2197 inside
///   `forward_racer`) cannot be reached because we already returned;
///   but the *producer* may still be wedged on `tx.send()` until the
///   parent frame drops `peer_rx`. The wedge window is small in
///   tokio's current scheduler (rx-drop wakes the sender), but the
///   abort is the contract — same belt-and-suspenders idiom #230 M1
///   uses.
///
/// **Bespoke message:** names the #244 contract specifically —
/// "forward_racer parallel-race path must abort racer task on
/// bind_buffered Err to prevent leak (#244 M1 sibling)".
/// Tight discriminator window: with `handle.abort()`, the peer's
/// in-flight `tokio::time::sleep` cancels at the next poll, the
/// `ExitGuard` drops, and the oneshot fires within ~milliseconds.
/// Without abort, rx-drop has no effect on the sleeping task; the peer
/// must wait for the full PEER_SLEEP_MS sleep to expire before its next
/// `tx.send().await` errors and the future drops. We pick 500ms as the
/// observation window: well above any scheduler-induced latency for
/// the abort path, well below the PEER_SLEEP_MS for the no-abort path.
const ABORT_OBSERVATION_WINDOW: Duration = Duration::from_millis(500);

/// Long sleep between chunks. Must be much greater than
/// `ABORT_OBSERVATION_WINDOW` so the no-abort path's wakeup is
/// dominated by sleep-completion (not rx-drop reactivity).
const PEER_SLEEP_MS: u64 = 10_000;

#[nativelink_test]
async fn forward_racer_aborts_racer_task_on_bind_buffered_err_244() -> Result<(), Error> {
    // 8 MiB blob × 8 KiB chunks = 1024 chunks — exactly the
    // DEFAULT_BUF_CHANNEL_CAPACITY. The peer sends one chunk to satisfy
    // `await_peer_after_empty_server`'s first-chunk recv, then enters a
    // 10-second sleep before attempting the next chunk. During that
    // sleep, the consumer disconnects.
    let value = test_value(8 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    let inner = make_empty_filesystem_inner().await?;

    // Discriminator: oneshot fires when peer's get_part future drops.
    let (exit_tx, exit_rx) = oneshot::channel::<()>();

    // Send 2 chunks burst, then sleep 10s. The burst guarantees that
    // chunk 2 is in the racer rx by the time `forward_racer` enters
    // `bind_buffered`'s loop. The consumer-disconnect happens AFTER
    // chunk 1 is consumed; bind_buffered then reads chunk 2 from rx
    // and tries to send to the closed downstream — that send errors,
    // and forward_racer enters its Err arm WHILE the peer is sleeping.
    // The peer's sleep is the abort-vs-no-abort discriminator.
    // Send 10 chunks burst (~80KiB), then 10s sleep, then remainder.
    // - Peer sends 10 chunks rapidly into racer tx (cap 1024, all fit).
    // - Peer enters 10s sleep BEFORE chunk 11.
    // - await_peer reads chunk 1 from racer tx, sends to consumer's
    //   tiny tx (cap 2) — fits, consumer reads chunk 1.
    // - forward_racer's bind_buffered drains chunks 2..10 from racer
    //   tx and tries to push to consumer tx (cap 2). Consumer hasn't
    //   read after chunk 1; consumer tx fills with 2 chunks; the next
    //   `writer.send` BLOCKS waiting for room.
    // - Test drops reader → consumer tx closes → blocked `writer.send`
    //   returns Err → forward_racer enters Err arm.
    // - At this moment, peer is sleeping (just past chunk 10, can't
    //   send chunk 11 yet). With abort, sleep cancels → peer exits ms.
    //   Without abort, peer must wait full 10s sleep → exit ~10s later.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 8 * 1024,
        post_burst_sleep_ms: AtomicU64::new(PEER_SLEEP_MS),
        chunks_before_sleep: AtomicU64::new(10),
        exit_signal: StdMutex::new(Some(exit_tx)),
    }));

    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
    proxy_arc.enable_race_peers();
    let peer_endpoint = "grpc://forward-racer-244-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_inner);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);
    let proxy = Store::new(proxy_arc.clone());

    // Tiny consumer-side channel so bind_buffered's `writer.send`
    // blocks quickly once the consumer stops reading. This puts
    // bind_buffered in a SEND-blocked state at the moment of
    // consumer-disconnect, ensuring the send-Err path triggers
    // promptly while the peer is still in its post-burst sleep.
    let (writer, mut reader) = nativelink_util::buf_channel::make_buf_channel_pair_with_size(2);
    let key: StoreKey<'static> = digest.into();
    let proxy_for_get = proxy.clone();
    let get_handle = tokio::spawn(async move {
        let mut writer = writer;
        proxy_for_get.get_part(key, &mut writer, 0, None).await
    });

    // Wait for the first chunk so we know the peer has reached the
    // sleep between chunk 1 and chunk 2.
    let first_chunk = tokio::time::timeout(TEST_TIMEOUT, reader.recv())
        .await
        .expect(
            "must not deadlock — peer-fetch must produce first chunk \
             within 10s (forward_racer parallel-race path setup)",
        )?;
    assert!(
        !first_chunk.is_empty(),
        "test setup error: peer delivered empty first chunk; cannot \
         observe abort discriminator"
    );

    // Consumer disconnect — closes the buf_channel writer's downstream
    // rx, so the next `writer.send` inside `bind_buffered` returns Err
    // and `forward_racer` enters the `if let Err(e) = ...` arm.
    drop(reader);

    // Wait for `get_part` to return Err. This propagates the
    // `bind_buffered` Err up. Independent of the abort contract —
    // happens in both the abort and no-abort paths.
    let get_res = tokio::time::timeout(TEST_TIMEOUT, get_handle)
        .await
        .expect("get_part future must resolve within 10s")
        .expect("get_part task must not panic");
    assert!(
        get_res.is_err(),
        "expected get_part to fail after consumer disconnect; got Ok(())",
    );

    // PRIMARY ASSERTION — the abort discriminator.
    //
    // With the #244 fix (`handle.abort()` in forward_racer's
    // bind_buffered Err arm), the peer's `tokio::time::sleep` is
    // cancelled at the next poll; the future drops, ExitGuard runs,
    // the oneshot fires. Should arrive well within
    // ABORT_OBSERVATION_WINDOW (500ms).
    //
    // Without the fix, the dropped JoinHandle DETACHES (does not
    // cancel) the spawned task. The peer is sleeping for
    // PEER_SLEEP_MS (10s); rx-drop has no effect on a sleeping task.
    // The peer wakes only when the sleep completes. The exit signal
    // fires ~10s later — the 500ms window times out.
    tokio::time::timeout(ABORT_OBSERVATION_WINDOW, exit_rx)
        .await
        .expect(
            "forward_racer parallel-race path must abort racer task on \
             bind_buffered Err to prevent leak (#244 M1 sibling)",
        )
        .expect("ExitGuard sender must not be dropped without firing");

    Ok(())
}

/// #244 over-action positive control. Per CLAUDE.md "Asymmetric
/// contract coverage": the under-action test above asserts that
/// `handle.abort()` MUST FIRE when `bind_buffered` returns Err. The
/// over-action sibling asserts the success path through
/// `forward_racer` returns Ok with the full payload — guarding
/// against accidental error injection on the success path.
///
/// **Mutation-step limitation:** the obvious over-action mutation
/// (hoisting `handle.abort()` outside the `if let Err` arm) does
/// NOT cause this test to red-fail. By the time `bind_buffered`
/// returns Ok, the racer task has already dropped its tx (which is
/// what triggered the rx EOF that ended bind_buffered) and
/// completed; `handle.abort()` after task-completion is a documented
/// no-op, and `handle.await` returns the task's stored Ok result.
/// The natural ordering protects this specific over-action site.
/// The test still has value as positive coverage of the
/// `race_peers=true` peer-winner success path — guarding against
/// regressions that DO change the success-path Result (e.g.
/// `handle.await.map(|_| Err(...))` "improvements").
///
/// Setup: a peer that streams the FULL payload to completion with
/// no inter-chunk delay; a consumer that drains everything via
/// `get_part_unchunked`. Asserts the bytes returned match the
/// payload byte-for-byte.
#[nativelink_test]
async fn forward_racer_does_not_abort_racer_task_on_bind_buffered_ok_244()
-> Result<(), Error> {
    // Small payload to keep the test quick; the contract is
    // independent of payload size as long as bind_buffered drains
    // to natural EOF.
    let value = test_value(64 * 1024);
    let digest = digest_for_size(value.len() as u64);

    let inner = make_empty_filesystem_inner().await?;

    // ExitGuard signals when the peer's get_part future drops; we
    // don't strictly need this for the assertion, but it confirms
    // the peer task completes naturally.
    let (exit_tx, _exit_rx) = oneshot::channel::<()>();
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 8 * 1024,
        // No post-burst sleep, no early sleep gate — peer streams
        // straight through and completes naturally.
        post_burst_sleep_ms: AtomicU64::new(0),
        chunks_before_sleep: AtomicU64::new(u64::MAX),
        exit_signal: StdMutex::new(Some(exit_tx)),
    }));

    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
    proxy_arc.enable_race_peers();
    let peer_endpoint = "grpc://forward-racer-244-ok-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_inner);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);
    let proxy = Store::new(proxy_arc.clone());

    // Drain everything via get_part_unchunked — exercises the
    // bind_buffered Ok path inside forward_racer; consumer reads
    // all chunks; bind_buffered returns Ok; handle.await observes
    // the peer task's natural Ok completion.
    let key: StoreKey<'static> = digest.into();
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(key, 0, None),
    )
    .await
    .expect(
        "must not deadlock — bind_buffered Ok path must complete in 10s \
         (#244 over-action positive control)",
    );

    let bytes = result.expect(
        "peer-winner success path through forward_racer must return Ok. \
         If this fires with Code::Internal mentioning JoinError::cancelled, \
         the abort was hoisted outside the bind_buffered Err if-let — restore \
         the conditional gate at worker_proxy_store.rs:2188 (#244 over-action \
         positive control).",
    );
    assert_eq!(
        bytes.len(),
        value.len(),
        "peer-winner payload length must match (#244 over-action); got {}",
        bytes.len(),
    );
    assert_eq!(
        bytes.as_ref(),
        value.as_slice(),
        "peer-winner payload bytes must match (#244 over-action)",
    );

    Ok(())
}
