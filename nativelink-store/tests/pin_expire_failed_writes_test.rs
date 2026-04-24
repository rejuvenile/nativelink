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

//! Regression tests for the durability gap where a hung slow-store
//! write let the fast-store pin auto-expire (PIN_TIMEOUT_SECS = 120s)
//! WITHOUT marking the digest in `failed_slow_writes`. Symptom captured
//! at worker-08 2026-04-23T00:08:53: blob written to fast tier, slow
//! write produced no completion log AND no error log; pin auto-expired
//! at 00:10:58 (`auto-unpinning expired pin`). Worker reconnect did not
//! retry the upload because `drain_failed_digests` returned empty.
//!
//! Two complementary failure modes are guarded here:
//!
//!   Test A — pin auto-expire path: simulates a slow-write that has
//!   not produced a result by the time `expire_stale_pins` runs. The
//!   `on_pin_expired` callback registered by `FastSlowStore::new` must
//!   land the digest in `failed_slow_writes`.
//!
//!   Test B — watchdog path: simulates a slow-write that hangs (no
//!   error, no completion). The per-spawn watchdog fires after
//!   `SLOW_WRITE_WATCHDOG_SECS` (= 60s) and inserts the digest into
//!   `failed_slow_writes` WITHOUT aborting the spawn (so a delayed
//!   completion still has a chance to ack-clear the failed entry).

use core::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tempfile::TempDir;
use tokio::sync::Notify;

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

// ---------------------------------------------------------------------
// Hanging slow store: drives a `Notify` from `update`/`update_oneshot`
// and never returns until the test releases it. Other operations defer
// to a backing MemoryStore so unrelated machinery (has, get_part, etc.)
// keeps working.
// ---------------------------------------------------------------------

#[derive(MetricsComponent)]
struct HangingSlowStore {
    inner: Arc<MemoryStore>,
    update_entered: Arc<Notify>,
    release_update: Arc<Notify>,
}

#[async_trait]
impl StoreDriver for HangingSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .has_with_results(digests, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        digest: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Drop the reader so the upstream sender doesn't block
        // forever (FastSlowStore's send_fut would otherwise wedge on
        // the buf_channel).
        drop(reader);
        self.update_entered.notify_waiters();
        self.release_update.notified().await;
        // After release, complete to inner so `has` reflects the write.
        let (mut tx, rx) = nativelink_util::buf_channel::make_buf_channel_pair_with_size(8);
        let inner_fut = Pin::new(self.inner.as_ref()).update(digest, rx, size_info);
        let send_fut = async {
            tx.send_eof().err_tip(|| "send_eof in HangingSlowStore::update")?;
            Result::<(), Error>::Ok(())
        };
        let (write_res, send_res) = tokio::join!(inner_fut, send_fut);
        send_res.and(write_res)
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
        self.update_entered.notify_waiters();
        self.release_update.notified().await;
        Pin::new(self.inner.as_ref()).update_oneshot(key, data).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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
}

default_health_status_indicator!(HangingSlowStore);

// ---------------------------------------------------------------------
// Common setup: build a FastSlowStore { fast: real FilesystemStore,
// slow: HangingSlowStore } so we can drive `test_expire_stale_pins`
// against a real eviction map and observe the on_pin_expired plumbing.
// ---------------------------------------------------------------------

struct Harness {
    fss: Arc<FastSlowStore>,
    fs_store: Arc<FilesystemStore>,
    release_update: Arc<Notify>,
    update_entered: Arc<Notify>,
    _temp: TempDir,
}

async fn make_harness() -> Result<Harness, Error> {
    let temp = tempfile::Builder::new()
        .prefix("pin_expire_failed_")
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

    let inner_slow = MemoryStore::new(&MemorySpec::default());
    let update_entered = Arc::new(Notify::new());
    let release_update = Arc::new(Notify::new());
    let hanging = Arc::new(HangingSlowStore {
        inner: inner_slow,
        update_entered: Arc::clone(&update_entered),
        release_update: Arc::clone(&release_update),
    });
    let slow_store = Store::new(hanging);

    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        fast_store,
        slow_store,
    );
    Ok(Harness {
        fss,
        fs_store,
        release_update,
        update_entered,
        _temp: temp,
    })
}

// ---------------------------------------------------------------------
// Test A: pin auto-expire fires the durability callback.
//
// Spec (from worker-08 2026-04-23T00:08:53): a slow-write that produces
// no completion AND no error before PIN_TIMEOUT_SECS=120 must result in
// the digest being queued in `failed_slow_writes`. The pin TTL firing
// in pure absence-of-completion proves the slow-write path silently
// dropped its durability obligation; the auto-unpin callback is the
// only place left to recover it.
// ---------------------------------------------------------------------

#[nativelink_test]
async fn pin_auto_expire_inserts_digest_into_failed_slow_writes() -> Result<(), Error> {
    let h = make_harness().await?;
    let digest = DigestInfo::try_new(VALID_HASH, 1024).unwrap();
    let data = Bytes::from(vec![0xA5; 1024]);

    // Trigger the FastSlowStore write path. The slow store is hanging:
    // update_oneshot's spawn enters slow_store.update_oneshot and waits
    // on `release_update`. Caller returns Ok immediately because the
    // fast write succeeded.
    let entered = h.update_entered.notified();
    h.fss
        .clone()
        .as_store()
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "fss.update_oneshot")?;

    // Wait for the spawned slow write to enter (so the pin is in
    // place) and then deliberately leave it hung.
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "slow write never entered"))?;

    // Sanity: the digest was pinned by the fast write.
    assert!(
        h.fs_store.test_force_pin_expired(&digest),
        "digest must be pinned after fast write"
    );

    // failed_slow_writes is empty BEFORE the auto-unpin sweep — the
    // slow write hasn't completed (success or error) yet.
    let failed_before = h.fss.drain_failed_digests();
    assert!(
        failed_before.is_empty(),
        "failed_slow_writes should be empty before auto-unpin: got {failed_before:?}"
    );
    // drain_failed_digests removed nothing; reset state.
    assert_eq!(failed_before.len(), 0);

    // Run the pin-expiry sweep. The on_pin_expired callback registered
    // by FastSlowStore::new must insert the digest into
    // failed_slow_writes.
    h.fs_store.test_expire_stale_pins().await;

    let failed_after = h.fss.drain_failed_digests();
    assert!(
        failed_after.iter().any(|d| *d == digest),
        "digest must be in failed_slow_writes after pin auto-expire: \
         got {failed_after:?}"
    );

    // Release the slow-store hang so we don't leak a hung spawn into
    // the next test.
    h.release_update.notify_waiters();
    Ok(())
}

trait FastSlowStoreExt {
    fn as_store(self: Arc<Self>) -> Store;
}

impl FastSlowStoreExt for FastSlowStore {
    fn as_store(self: Arc<Self>) -> Store {
        Store::new(self)
    }
}

// ---------------------------------------------------------------------
// Test B: the slow-write watchdog inserts into failed_slow_writes when
// the spawn body exceeds SLOW_WRITE_WATCHDOG_SECS. With paused tokio
// time we advance past the watchdog while leaving the slow-write
// permanently hung, then assert failed_slow_writes contains the digest.
//
// Critically: the watchdog must NOT abort the slow-write task — if it
// eventually completes, the existing in_flight bookkeeping handles the
// success path. We verify by checking the spawn has not panicked /
// completed (the in_flight count remains > 0 because we never release).
// ---------------------------------------------------------------------

#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn slow_write_watchdog_inserts_into_failed_slow_writes() -> Result<(), Error> {
    let h = make_harness().await?;
    let digest = DigestInfo::try_new(VALID_HASH, 2048).unwrap();
    let data = Bytes::from(vec![0x77; 2048]);

    let entered = h.update_entered.notified();
    h.fss
        .clone()
        .as_store()
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "fss.update_oneshot")?;

    // Wait for the slow-write spawn to enter the hanging store. Use
    // `tokio::time::timeout` against paused time — the timeout never
    // fires because we await the notification which depends on real
    // scheduler progress, not paused time.
    tokio::task::yield_now().await;
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "slow write never entered"))?;

    // Before the watchdog deadline, no digest should be in failed.
    let failed_before = {
        let mut g = h.fss.drain_failed_digests();
        g.sort_by_key(|d| *d.packed_hash());
        g
    };
    assert!(
        failed_before.is_empty(),
        "failed_slow_writes should be empty before watchdog fires: {failed_before:?}",
    );

    // Advance paused tokio time PAST the watchdog deadline. The
    // watchdog future was created inside the spawn with
    // tokio::time::sleep, which respects paused time.
    tokio::time::advance(Duration::from_secs(61)).await;
    // Yield several times so the watchdog task gets a chance to run
    // its post-sleep code (insert + pin_digests) on the runtime.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    let failed_after = h.fss.drain_failed_digests();
    assert!(
        failed_after.iter().any(|d| *d == digest),
        "digest must be in failed_slow_writes after watchdog fires: \
         got {failed_after:?}"
    );

    // The slow-write spawn must NOT be aborted — verify the in-flight
    // bookkeeping still holds the digest (release_update never fired,
    // so the spawn is still parked).
    assert!(
        h.fss.in_flight_slow_write_count() > 0,
        "watchdog must NOT abort the in-flight slow write spawn"
    );

    // Release so the spawn unwinds cleanly.
    h.release_update.notify_waiters();
    Ok(())
}
