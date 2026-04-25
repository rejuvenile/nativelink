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

// ---------------------------------------------------------------------
// Test C — Fix B regression (download-pin does NOT populate
// failed_slow_writes).
//
// Scenario: `directory_cache.rs` pins blobs that arrived via DOWNLOAD
// (not via a slow-store write). When the pin TTL fires (~120s for
// long actions), the pin-expiry listener used to unconditionally insert
// the digest into `failed_slow_writes` — producing dead-weight entries
// that, on next worker reconnect, would attempt to re-upload blobs the
// server already has. The fix gates the listener on the wrapper's own
// `in_flight_slow_writes`: only digests with an outstanding slow-write
// spawn get queued for retry. This test pins a digest WITHOUT a
// preceding `update`/`update_oneshot`, runs the sweep, and asserts the
// failed set stays empty.
//
// Mutation step (not in test code — to verify the test guards
// behavior): comment out the `if !self.in_flight_slow_writes.lock()
// .contains_key(&owned_key) { return; }` early-return in
// `PinExpireFailedWritesListener::on_pin_expired`. The test must then
// FAIL because the digest gets inserted unconditionally.
// ---------------------------------------------------------------------

#[nativelink_test]
async fn download_pin_does_not_populate_failed_slow_writes() -> Result<(), Error> {
    let h = make_harness().await?;
    let digest = DigestInfo::try_new(VALID_HASH, 1024).unwrap();

    // Stage the blob into the fast store directly via the FilesystemStore
    // (NOT via the FastSlowStore::update path — we don't want a slow-write
    // to populate `in_flight_slow_writes`). This mirrors how
    // `populate_fast_store_unchecked` lands a downloaded blob: the bytes
    // arrive via `copy_slow_to_fast` → `fast_store.update`, never
    // touching the wrapper's in-flight bookkeeping.
    let data = Bytes::from(vec![0xC3; 1024]);
    Pin::new(h.fs_store.as_ref())
        .update_oneshot(digest.into(), data)
        .await
        .err_tip(|| "fs_store.update_oneshot direct seed")?;

    // Pin via the FilesystemStore directly — same call shape as
    // `directory_cache.rs:2553` (`fss.fast_store().pin_digests(&[digest])`).
    Pin::new(h.fs_store.as_ref()).pin_digests(&[digest]);

    // Sanity: the pin landed.
    assert!(
        h.fs_store.test_force_pin_expired(&digest),
        "digest must be pinned via direct fs_store.pin_digests"
    );

    // Sanity: no slow-write went through the FastSlowStore for this
    // digest, so in_flight is empty for this wrapper.
    assert_eq!(
        h.fss.in_flight_slow_write_count(),
        0,
        "no FastSlowStore::update call was made; in_flight must be empty",
    );

    // Trigger the pin-expiry sweep. With the fix, the listener consults
    // in_flight, sees nothing for this digest, and skips both the warn
    // AND the failed_slow_writes insert.
    h.fs_store.test_expire_stale_pins().await;

    let failed = h.fss.drain_failed_digests();
    assert!(
        !failed.iter().any(|d| *d == digest),
        "download-only pin must NOT populate failed_slow_writes — \
         no slow-write was outstanding for this digest. Got: {failed:?}",
    );
    Ok(())
}

// ---------------------------------------------------------------------
// Test D — Fix A regression (pin-expire listener registration is
// idempotent across multiple FastSlowStore wrappers sharing the same
// fast store).
//
// Scenario: `local_worker.rs` constructs THREE `FastSlowStore`
// wrappers around the SAME underlying fast store (one per
// `FastSlowStore::new` / `new_with_shared_failed_writes` site). Each
// registers a `PinExpireFailedWritesListener` on the shared fast
// store. Without dedup, every pin expiry fires N callbacks (observed:
// 3× warn amplification, 5774 events / 10 min on workers).
//
// The fix gates each listener on its OWNING wrapper's
// `in_flight_slow_writes`. A real silent slow-write hang shows up in
// the in-flight of ONLY the wrapper that owned the spawn, so exactly
// one listener fires the warn + insert.
//
// Test shape: build TWO FastSlowStore wrappers around the SAME
// FilesystemStore. Use `FastSlowStore::new` (separate failed sets) so
// we can directly count per-wrapper inserts. Issue an `update_oneshot`
// via wrapper A only; the slow-write hangs. Force the pin to expire,
// sweep, assert:
//   - A's failed_slow_writes contains the digest (A owned the
//     in-flight).
//   - B's failed_slow_writes does NOT contain the digest (B's
//     in-flight is empty for it; the listener skipped).
//
// Without the fix, both listeners would insert into their respective
// failed sets and the assertion on B would fail.
//
// Mutation step: same as Test C — comment out the `contains_key` gate
// in `on_pin_expired`. B's failed_slow_writes will then contain the
// digest, failing the test.
// ---------------------------------------------------------------------

#[nativelink_test]
async fn pin_expire_listener_registration_is_idempotent() -> Result<(), Error> {
    let temp = tempfile::Builder::new()
        .prefix("pin_expire_idempotent_")
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
    let fast_store_handle = Store::new(fs_store.clone());

    // Slow store A: hangs (mirrors the production silent-hang case).
    let inner_slow_a = MemoryStore::new(&MemorySpec::default());
    let update_entered_a = Arc::new(Notify::new());
    let release_update_a = Arc::new(Notify::new());
    let hanging_a = Arc::new(HangingSlowStore {
        inner: inner_slow_a,
        update_entered: Arc::clone(&update_entered_a),
        release_update: Arc::clone(&release_update_a),
    });

    // Slow store B: regular MemoryStore — never actually used in this
    // test because we don't issue any update via wrapper B for the
    // shared digest, but it must be a valid Store.
    let inner_slow_b = MemoryStore::new(&MemorySpec::default());

    // Build two FastSlowStore wrappers around the SAME fast_store.
    // Use `::new` (NOT `new_with_shared_failed_writes`) so each has its
    // own failed_slow_writes — letting us count per-wrapper inserts
    // directly via `drain_failed_digests`.
    let fss_a = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        fast_store_handle.clone(),
        Store::new(hanging_a),
    );
    let fss_b = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        fast_store_handle.clone(),
        Store::new(inner_slow_b),
    );

    let digest = DigestInfo::try_new(VALID_HASH, 1024).unwrap();
    let data = Bytes::from(vec![0xA5; 1024]);

    // Issue update_oneshot via wrapper A — this populates A's
    // in_flight_slow_writes AND pins the digest in the shared fast
    // store. The hanging slow store causes the spawn to park.
    let entered = update_entered_a.notified();
    fss_a
        .clone()
        .as_store()
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "fss_a.update_oneshot")?;
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "slow write A never entered"))?;

    // Sanity: A has the digest in flight; B does not.
    assert!(
        fss_a.in_flight_slow_write_count() > 0,
        "wrapper A must have an outstanding in-flight slow-write"
    );
    assert_eq!(
        fss_b.in_flight_slow_write_count(),
        0,
        "wrapper B must NOT have any in-flight slow-write for this digest"
    );

    // Sanity: digest is pinned (by A's slow-write spawn).
    assert!(
        fs_store.test_force_pin_expired(&digest),
        "digest must be pinned in the shared fast store after A's update"
    );

    // Run the pin-expiry sweep. Both listeners (A's and B's) fire on
    // pin expiry. With the fix:
    //   - A's listener: in_flight_A contains digest → inserts into
    //     A's failed_slow_writes.
    //   - B's listener: in_flight_B is EMPTY → returns early without
    //     warn or insert.
    fs_store.test_expire_stale_pins().await;

    let failed_a = fss_a.drain_failed_digests();
    let failed_b = fss_b.drain_failed_digests();
    assert!(
        failed_a.iter().any(|d| *d == digest),
        "wrapper A (the in-flight owner) MUST insert digest into its \
         failed_slow_writes. Got A: {failed_a:?}",
    );
    assert!(
        !failed_b.iter().any(|d| *d == digest),
        "wrapper B (not the in-flight owner) MUST NOT insert digest \
         into its failed_slow_writes — the per-wrapper in_flight gate \
         dedupes the listener fan-out. Got B: {failed_b:?}",
    );

    // The original bug was 3× warn amplification (5774 events / 10 min on
    // workers). With the fix, exactly ONE wrapper's listener fires its
    // warn per pin expiry. Without the fix, BOTH wrappers' listeners
    // would fire, producing two of these warns. Asserting the count
    // guards the warn-amplification regression directly (not just the
    // failed_slow_writes side effect).
    logs_assert(|lines: &[&str]| {
        let warn_count = lines
            .iter()
            .filter(|l| l.contains("fast-store pin auto-expired with in-flight slow-write"))
            .count();
        if warn_count == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly 1 pin-auto-expire warn (one per wrapper \
                 with the digest in its in_flight set; only A qualifies). \
                 Got {warn_count} warns. The fix gates each listener on \
                 its OWN in_flight; without the fix, both A's and B's \
                 listeners fire and produce 2 warns."
            ))
        }
    });

    // Release so the hung spawn unwinds cleanly.
    release_update_a.notify_waiters();
    drop(temp);
    Ok(())
}
