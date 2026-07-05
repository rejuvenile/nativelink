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

//! (#locality-map-drift) MANDATORY composite production-composition test.
//!
//! Invariant: at quiescence, `server_map[endpoint] ∋ digest ⟺ digest ∈
//! resident_set(endpoint)`. The bug this closes: moka's ASYNC evict callback
//! is delivered OUT OF ORDER vs the SYNC `on_insert`, so a late evict
//! un-registers a re-admitted hot blob → persistent false-missing.
//!
//! This test composes the THREE real types the invariant spans:
//!   * worker `MokaEvictingMap` (the churn source; value-carried `(boot_epoch,
//!     counter)` ts frozen at insert),
//!   * worker `BlobChangeTracker` (`ItemCallback` → per-digest LWW `pending`),
//!   * server `BlobLocalityMap` (ts-gated apply path with the ABSENT≻PRESENT
//!     tie-break).
//!
//! The delta produced by `BlobChangeTracker::swap()` — `(digest, state,
//! boot_epoch, counter)` — is EXACTLY what the worker serializes to
//! `BlobDigestInfo`/`evicted_digests` and the server applies via the ts-gate
//! (`worker_api_server.rs` `handle_blobs_available`). We drive that apply here
//! by calling the SAME `BlobLocalityMap::{register_blobs_gated,
//! evict_blobs_gated}` the server calls, so the seam under test is the real
//! server gate, not a re-implementation.
//!
//! Three sub-scenarios mirror the three TLC counterexamples (asymmetric
//! coverage), each with its own bespoke assertion message and a mutation
//! that must red-fail:
//!   1. async-reorder + LWW suppression (Model A counterexample).
//!   2. restart dominance (bare-counter counterexample).
//!   3. ungated force_evict + heal (self-heal-disabled counterexample).

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_macro::nativelink_test;
use nativelink_store::callback_utils::ItemCallbackHolder;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::blob_locality_map::{BlobLocalityMap, Stamp};
use nativelink_util::common::DigestInfo;
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use nativelink_util::store_trait::{StoreDriver, StoreKey, StoreLike};
use nativelink_worker::local_worker::{BlobChangeTracker, BlobState};
use tokio::sync::Notify;

// ---------------------------------------------------------------
// Test value type — mirrors `FileEntryImpl`'s value-carried stamp:
// an interior-mutable `AtomicU64` the map writes at insert and reads
// at eviction. This is the exact mechanism `FileEntryImpl.ts` uses in
// production (the map calls `LenEntry::set_stamp` before `cache.insert`
// and `LenEntry::stamp` from the eviction listener).
// ---------------------------------------------------------------
#[derive(Debug)]
struct StampedBytes {
    size: u64,
    stamp: AtomicU64,
}

impl StampedBytes {
    fn new(size: u64) -> Arc<Self> {
        Arc::new(Self {
            size,
            stamp: AtomicU64::new(0),
        })
    }
}

impl LenEntry for StampedBytes {
    fn len(&self) -> u64 {
        self.size
    }
    fn is_empty(&self) -> bool {
        self.size == 0
    }
    fn stamp(&self) -> u64 {
        self.stamp.load(Ordering::Acquire)
    }
    fn set_stamp(&self, ts: u64) {
        self.stamp.store(ts, Ordering::Release);
    }
}

type TestMap = MokaEvictingMap<
    StoreKey<'static>,
    StoreKey<'static>,
    Arc<StampedBytes>,
    SystemTime,
    ItemCallbackHolder,
>;

fn digest(n: u8) -> DigestInfo {
    DigestInfo::new([n; 32], 1000)
}

fn key(d: DigestInfo) -> StoreKey<'static> {
    StoreKey::Digest(d)
}

/// Build a worker-shaped `MokaEvictingMap` with the given boot_epoch and a
/// registered real `BlobChangeTracker`. `max_bytes` is large so nothing is
/// evicted implicitly — evictions in these tests are all EXPLICIT (via
/// `remove`) so we control the interleaving deterministically.
fn build_map_with_tracker(boot_epoch: u64) -> (Arc<TestMap>, Arc<BlobChangeTracker>) {
    let policy = EvictionPolicy {
        max_bytes: 1 << 30,
        evict_bytes: 0,
        max_seconds: 0,
        max_count: 0,
    };
    let map = Arc::new(TestMap::with_anchor_and_boot_epoch(
        &policy,
        SystemTime::now(),
        boot_epoch,
    ));
    let notify = Arc::new(Notify::new());
    let tracker = BlobChangeTracker::new(notify);
    map.add_item_callback(ItemCallbackHolder::new(tracker.clone()));
    (map, tracker)
}

/// Drain a `BlobChangeTracker::swap()` result into the server `BlobLocalityMap`
/// through the SAME ts-gated apply path `worker_api_server.rs` uses:
/// evictions before registrations (the production ordering), each carrying its
/// `(boot_epoch, counter)` stamp.
fn apply_delta_to_server(map: &mut BlobLocalityMap, endpoint: &str, tracker: &BlobChangeTracker) {
    let delta = tracker.swap();
    let mut present: Vec<(DigestInfo, Stamp)> = Vec::new();
    let mut absent: Vec<(DigestInfo, Stamp)> = Vec::new();
    for (d, state, stamp) in delta {
        match state {
            BlobState::Present => present.push((d, stamp)),
            BlobState::Absent => absent.push((d, stamp)),
        }
    }
    // Server ordering: evictions BEFORE registrations.
    map.evict_blobs_gated(endpoint, &absent);
    map.register_blobs_gated(endpoint, &present);
}

// ===============================================================
// Scenario 1: async-reorder + LWW suppression (Model A counterexample).
//
// insert(d)@ts_k → evict(d) [queue the async callback carrying the FROZEN
// ts_k] → insert(d)@ts_{k+1} → deliver the LATE evict → the server gate MUST
// SUPPRESS the stale evict, has_digest(d) == true.
//
// MUTATION (documented): make the eviction listener re-mint a FRESH ts at
// delivery (Model A) instead of carrying the evicted value's frozen ts. Then
// the late evict outranks the re-insert → gate applies it → has_digest ==
// false → RED-fail "stale evict re-registered a held blob".
// ===============================================================
#[nativelink_test]
async fn scenario1_async_reorder_lww_suppression() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = "grpc://worker-a:50081";
        let (map, tracker) = build_map_with_tracker(1);
        let mut server = BlobLocalityMap::new();
        let d = digest(1);

        // insert(d)@ts_k (first value V1). on_insert fires synchronously →
        // tracker records PRESENT@(1, c1).
        map.insert(key(d), StampedBytes::new(1000)).await;

        // Model the ASYNC evict reorder: `remove` of V1 leaves the cache NOW
        // and captures V1's FROZEN stamp (`frozen_counter`), but callback
        // DELIVERY is DEFERRED via the returned closure.
        let (frozen_counter, deliver) = map
            .test_remove_defer_callback(&key(d))
            .await
            .expect("V1 must be resident to evict");

        // insert(d)@ts_{k+1} (a NEW value V2, higher counter). on_insert fires
        // synchronously → tracker records PRESENT@(1, c2>c1), superseding the
        // (not-yet-delivered) evict locally by LWW.
        map.insert(key(d), StampedBytes::new(1000)).await;

        // Deliver the LATE evict of V1 carrying V1's FROZEN stamp
        // (`frozen_counter` < c2). The tracker's local LWW must NOT let
        // (Absent, frozen) overwrite the stored (Present, c2): a re-insert's
        // fresh ts supersedes the stale evict.
        //
        // MUTATION (Model A — mint-at-eviction): replace `frozen_counter` with
        // `map.test_next_stamp()`, which mints a FRESH counter NOW (after V2's
        // insert) → the evict outranks the re-insert → the server keeps the
        // ABSENT → has_digest(d)==false → this scenario RED-fails with its
        // bespoke "stale evict re-registered a held blob" message.
        deliver(frozen_counter).await;

        // Apply the accumulated delta to the server. The server ts-gate must
        // see the stale (Absent, c1) evict LOSE to the (Present, c2) register.
        apply_delta_to_server(&mut server, endpoint, &tracker);

        assert!(
            server.has_digest(&d),
            "scenario1: stale evict re-registered a held blob — the late \
             evict carrying the OLD value's ts (c1) suppressed the re-insert's \
             PRESENT@c2 on the server, reproducing the false-missing bug. The \
             value-carried ts + ABSENT≻PRESENT LWW must keep has_digest==true."
        );
    })
    .await
    .expect("scenario1 deadlocked — composite drift detector");
}

// ===============================================================
// Scenario 2: restart convergence via the REAL reconnect wipe (MAJOR fix).
//
// `boot_epoch_id()` (worker_utils.rs) is a RANDOM u64, NOT monotone — so the
// stamp's cross-epoch comparison provides NO reliable "fresh epoch dominates"
// ordering in production. Restart safety actually rests on the server's
// WIPE-ON-EPOCH-CHANGE (`worker_api_server.rs` `remove_endpoint` when the
// reconnect's `boot_epoch_id` differs from the stored one) + connection-scoped
// inflight teardown (`drop_all_inflight` on the RST) — NOT epoch dominance.
//
// This scenario drives that REAL path at the `BlobLocalityMap` layer:
//   1. old process registers d@(epoch_old, c) — its holdings.
//   2. the worker restarts → the reconnect handler calls `remove_endpoint`
//      (the wipe) because the boot_epoch changed → d is CLEARED.
//   3. the fresh process re-registers d@(epoch_new, 1) into the wiped
//      (empty) state → d re-converges to PRESENT.
// Because the wipe cleared the endpoint's stamps, the fresh delta registers
// into an EMPTY entry — the stamp's cross-epoch comparison is NEVER reached, so
// a random (non-monotone) boot_epoch is fine. The stale (epoch_old, c) stamp is
// gone; no bare-counter/epoch-ordering wedge is possible.
//
// MUTATION (documented): comment out the `remove_endpoint` wipe (step 2). The
// stale old-epoch state then survives the restart, and the
// `!has_digest`-after-wipe assertion RED-fails — proving the WIPE (not epoch
// dominance) is the load-bearing restart-safety mechanism.
// ===============================================================
#[nativelink_test]
async fn scenario2_restart_convergence_via_wipe() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = "grpc://worker-a:50081";
        let mut server = BlobLocalityMap::new();
        let d = digest(2);

        // (1) Old process holdings: register d@(epoch_old, c).
        let epoch_old = 0x1111_2222_3333_4444u64; // a RANDOM-shaped boot_epoch
        server.register_blobs_gated(endpoint, &[(d, Stamp::new(epoch_old, 7))]);
        assert!(
            server.has_digest(&d),
            "scenario2 precondition: old-process holdings must register d present"
        );

        // (2) Worker restart → the server's reconnect handler wipes the endpoint
        // on boot_epoch change (the REAL restart-safety mechanism, NOT epoch
        // dominance). Modelled by `remove_endpoint`, exactly what
        // `worker_api_server.rs` calls.
        server.remove_endpoint(endpoint);
        assert!(
            !server.has_digest(&d),
            "scenario2: the reconnect wipe (remove_endpoint on boot_epoch change) \
             did NOT clear the endpoint's stale holdings — restart safety rests \
             on this wipe (boot_epoch_id is random, so the stamp comparator \
             gives NO cross-epoch ordering to fall back on). Without the wipe a \
             stale entry survives the restart."
        );

        // (3) Fresh process re-registers into the wiped (empty) state. Its
        // RANDOM new boot_epoch need not exceed the old one — the stored stamp
        // was cleared, so the cross-epoch comparison is never reached.
        let epoch_new = 0x0000_0001u64; // deliberately SMALLER than epoch_old
        server.register_blobs_gated(endpoint, &[(d, Stamp::new(epoch_new, 1))]);
        assert!(
            server.has_digest(&d),
            "scenario2: after the reconnect wipe, the fresh process's holdings \
             (register into the now-empty entry) must re-converge d to PRESENT \
             — even with a numerically SMALLER random boot_epoch, because the \
             wipe cleared the stored stamp so no cross-epoch comparison occurs."
        );
    })
    .await
    .expect("scenario2 deadlocked — composite drift detector");
}

// ===============================================================
// Scenario 3: ungated force_evict + heal (self-heal-disabled counterexample).
//
// Register d on the server. The server self-heal (`worker_proxy_store.rs`
// peer-fetch-NotFound → `evict_blobs`) fires UNSTAMPED while the worker STILL
// holds d. Then the worker's next delta (carrying d's current value ts)
// replays → d must RE-REGISTER.
//
// MUTATION (documented): route the self-heal through the ts-GATED evict
// instead of the ungated `evict_blobs`. A stale/absent stamp then blocks the
// force-evict (or, symmetrically, the gate refuses the unstamped evict), so
// the self-heal is disabled → assert re-registration RED-fails "self-heal
// disabled by stale ts". Here we assert the ungated force-evict ACTUALLY
// clears the entry (so a genuinely-broken peer is removed) AND the subsequent
// worker replay re-registers (so a still-held blob heals).
// ===============================================================
#[nativelink_test]
async fn scenario3_ungated_force_evict_and_heal() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = "grpc://worker-a:50081";
        let (map, tracker) = build_map_with_tracker(1);
        let mut server = BlobLocalityMap::new();
        let d = digest(3);

        // Worker holds d; its delta registers it PRESENT on the server.
        map.insert(key(d), StampedBytes::new(1000)).await;
        apply_delta_to_server(&mut server, endpoint, &tracker);
        assert!(
            server.has_digest(&d),
            "scenario3 precondition: worker holdings must register d present"
        );

        // Server self-heal: UNGATED force-evict (peer-fetch-NotFound repair).
        // It must ALWAYS remove d regardless of any ts — it is server-
        // originated ground truth, not a reorderable worker delta.
        server.evict_blobs(endpoint, &[d]);
        assert!(
            !server.has_digest(&d),
            "scenario3: the ungated force-evict (server self-heal) did NOT \
             clear d — a ts-gate must NEVER apply to the self-heal path, else \
             a poison/stale ts permanently disables the map's only self-heal."
        );

        // The worker STILL holds d (force-evict was server-side only). Its
        // existing re-advertise machinery re-emits d's CURRENT value stamp.
        // We drive that by touching d (on_get → touched → PRESENT delta at a
        // fresh ts) so a delta is produced, then applying it.
        assert!(
            map.get(&key(d)).await.is_some(),
            "scenario3: worker must still hold d after a server-side force-evict"
        );
        apply_delta_to_server(&mut server, endpoint, &tracker);

        assert!(
            server.has_digest(&d),
            "scenario3: after the ungated force-evict, the still-held blob's \
             worker replay did NOT re-register it — the value-carried ts \
             re-advertise must heal a spuriously force-evicted resident blob."
        );
    })
    .await
    .expect("scenario3 deadlocked — composite drift detector");
}

// ===============================================================
// Scenario 4: PRODUCTION-COMPOSITION eviction stamp fidelity (BLOCK-1 guard).
//
// The other scenarios use `StampedBytes`, a fake value that carries a stamp.
// The PRODUCTION value is `Arc<FileEntryImpl>`; if `FileEntryImpl` does not
// override `LenEntry::{stamp,set_stamp}`, the map's `set_stamp` is a no-op and
// the eviction listener reads `value.stamp() == 0` — so every genuine-eviction
// delta carries counter 0, loses `evict_gated` against any stored PRESENT@≥1,
// and the whole value-carried-ts fix INVERTS into a persistent stale-positive
// on every eviction (the false-negative only "looks fixed" because 0 always
// loses).
//
// This test composes the REAL `FilesystemStore<FileEntryImpl>` + a real
// `ItemCallback` (registered via `register_item_callback`, exactly as
// `local_worker.rs` wires the `BlobChangeTracker` in production) and asserts a
// genuine LRU eviction fires the removal callback carrying a NON-ZERO counter
// (the value's frozen INSERT counter), NOT 0.
//
// We assert on the RAW eviction callback ts (captured directly by a second
// ItemCallback), the faithful BLOCK-1 observable — "did the value carry its
// stamp to the evict" — independent of any downstream LWW.
//
// MUTATION (documented): comment out `FileEntryImpl::set_stamp`'s body (make it
// a no-op again, = pre-BLOCK-1). The eviction callback then fires with counter 0
// (`value.stamp()` reads the never-written 0) and this test RED-fails with its
// bespoke "eviction reported ts 0 — value-carried stamp not stored on
// FileEntryImpl" message.
#[derive(Debug)]
struct EvictCaptureCallback {
    target: DigestInfo,
    captured: std::sync::Mutex<Option<Stamp>>,
}

impl nativelink_util::store_trait::ItemCallback for EvictCaptureCallback {
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
        ts_boot_epoch: u64,
        ts_counter: u64,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = ()> + Send + 'a>> {
        if let StoreKey::Digest(d) = store_key {
            if d == self.target {
                *self.captured.lock().unwrap() = Some(Stamp::new(ts_boot_epoch, ts_counter));
            }
        }
        Box::pin(core::future::ready(()))
    }
}

#[nativelink_test]
async fn scenario4_filesystem_store_eviction_carries_nonzero_stamp() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let content_dir = tempfile::Builder::new()
            .prefix("nl_lmd_content_")
            .tempdir()
            .expect("content tempdir");
        let temp_dir = tempfile::Builder::new()
            .prefix("nl_lmd_temp_")
            .tempdir()
            .expect("temp tempdir");

        // Byte-driven cap so eviction fires on the SYNCHRONOUS `insert_inner`
        // drain path (the `max_count && max_bytes>0` re-check + inline
        // `drain_pending_evictions`), NOT only on the 10s background drain tick.
        // Blobs are ~2 KiB → weight 2 each; cap = 5000/1024 = 4 → two blobs
        // (weight 4) fit, the third (weight 6) evicts the LRU.
        let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_dir.path().to_string_lossy().into_owned(),
            temp_path: temp_dir.path().to_string_lossy().into_owned(),
            eviction_policy: Some(EvictionPolicy {
                max_bytes: 5000,
                max_count: 2,
                max_seconds: 0,
                evict_bytes: 0,
            }),
            ..Default::default()
        })
        .await
        .expect("create filesystem store");

        // (#locality-map-drift) The worker stamps the map's boot_epoch at boot;
        // do the same here so the delta carries a realistic (boot_epoch,
        // counter). The COUNTER (not the epoch) is what BLOCK-1 defangs.
        // (`FilesystemStore::new` already started the background eviction
        // drainer, so the async eviction callback fires without us kicking it.)
        store.set_map_boot_epoch(7);

        let d1 = DigestInfo::new([11u8; 32], 2000);
        let d2 = DigestInfo::new([12u8; 32], 2000);
        let d3 = DigestInfo::new([13u8; 32], 2000);

        // Register BOTH the real BlobChangeTracker (production wiring) AND a
        // capture callback that records d1's RAW eviction ts (pre-LWW).
        let notify = Arc::new(Notify::new());
        let tracker = BlobChangeTracker::new(notify);
        store
            .clone()
            .register_item_callback(tracker.clone())
            .expect("register_item_callback (tracker) on FilesystemStore");
        let capture = Arc::new(EvictCaptureCallback {
            target: d1,
            captured: std::sync::Mutex::new(None),
        });
        store
            .clone()
            .register_item_callback(capture.clone())
            .expect("register_item_callback (capture) on FilesystemStore");

        let blob = |b: u8| bytes::Bytes::from(vec![b; 2000]);

        // Insert d1, d2 (at capacity). Then promote d2 with a get so d1 is the
        // LRU victim, then insert d3 to force d1's eviction.
        store.update_oneshot(d1, blob(0xAA)).await.expect("insert d1");
        store.update_oneshot(d2, blob(0xBB)).await.expect("insert d2");
        let _ = store.get_part_unchunked(d2, 0, None).await;
        store
            .update_oneshot(d3, blob(0xCC))
            .await
            .expect("insert d3");

        // Wait for d1's genuine eviction callback (sync inside the emplace spawn
        // OR async via the background drainer).
        let mut evict_stamp: Option<Stamp> = None;
        for _ in 0..200 {
            if let Some(s) = *capture.captured.lock().unwrap() {
                evict_stamp = Some(s);
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let stamp = evict_stamp.expect(
            "scenario4: d1's genuine LRU eviction callback never fired on the real \
             FilesystemStore<FileEntryImpl> composition (max_bytes/max_count \
             eviction did not evict d1) — test cannot observe the eviction stamp",
        );
        assert!(
            stamp.counter > 0,
            "scenario4: eviction reported ts 0 — value-carried stamp not stored \
             on FileEntryImpl. The map's set_stamp is a no-op and the eviction \
             listener read value.stamp()==0, so the fix is INERT in production \
             (every eviction becomes a permanent stale-positive: a counter-0 \
             evict can never win the server LWW gate). FileEntryImpl must \
             override LenEntry::{{stamp,set_stamp}}. Got counter={}, boot_epoch={}.",
            stamp.counter,
            stamp.boot_epoch,
        );
        assert_eq!(
            stamp.boot_epoch, 7,
            "scenario4: eviction stamp must carry the map's boot_epoch (7)"
        );

        // The tracker (production consumer) must ALSO end with d1 ABSENT — the
        // emplace `still_ours` read fires on_get, but `fire_on_get` carries the
        // value's frozen insert stamp (== the insert delta, idempotent), so it
        // does NOT gate-kill d1's own genuine eviction. (scenario5 covers the
        // general read-then-evict case; here we confirm it end-to-end through
        // the REAL FilesystemStore emplace path.)
        let mut d1_absent_in_tracker = false;
        for _ in 0..50 {
            for (d, state, _stamp) in tracker.swap() {
                if d == d1 && state == BlobState::Absent {
                    d1_absent_in_tracker = true;
                }
            }
            if d1_absent_in_tracker {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            d1_absent_in_tracker,
            "scenario4: the tracker did NOT report d1 ABSENT after a genuine \
             eviction — a read on the insert→evict seam (the emplace `still_ours` \
             get) out-ranked the eviction in the LWW. `fire_on_get` must carry \
             the value's frozen insert stamp so the read is idempotent."
        );
    })
    .await
    .expect("scenario4 deadlocked — composite drift detector");
}

// ===============================================================
// Scenario 5: on_get must NOT gate-kill the value's own genuine eviction
// (the fire_on_get carry-value-stamp fix; TLC HoldingsTouch A_GateKill →
// NoGateKilledGenuineEvict VIOLATED under mint-fresh, HOLDS under
// carry-value-stamp).
//
// A hot blob is read (on_get) between its insert and its GENUINE eviction. If
// on_get mints a FRESH counter (> the value's insert counter), the read records
// PRESENT@c_fresh; the subsequent genuine evict carries the value's LOW insert
// counter and LOSES the LWW → the blob stays falsely PRESENT on the server
// FOREVER (a systematic false-POSITIVE on every read-then-evicted blob — the
// residual tail behind the still_ours patch, because genuine `get_part` reads
// still fired on_get). The fix: on_get carries the resident value's frozen
// insert counter (`value.stamp()`), so the on_get delta == the insert delta;
// the genuine evict (same counter) then TIES and the ABSENT≻PRESENT tie-break
// removes it → no false-positive.
//
// Composition: worker `MokaEvictingMap` + real `BlobChangeTracker` + server
// `BlobLocalityMap` — crossing on_get producer → tracker apply → wire → server
// gate (the exact seam the prover requested).
//
// MUTATION (documented): revert `fire_on_get` to mint-fresh (`self.next_stamp()`)
// → the read out-ranks the value stamp, the genuine evict is gate-killed, and
// this test RED-fails with its bespoke message.
#[nativelink_test]
async fn scenario5_on_get_does_not_gate_kill_genuine_eviction() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = "grpc://worker-a:50081";
        let (map, tracker) = build_map_with_tracker(1);
        let mut server = BlobLocalityMap::new();
        let d = digest(5);

        // insert(d)@c_insert → tracker PRESENT@(1, c_insert). Apply to server so
        // d is registered PRESENT there (the steady state a hot blob is in).
        map.insert(key(d), StampedBytes::new(1000)).await;
        // A genuine materialization READ of d — fires on_get. Under mint-fresh
        // this records PRESENT@(1, c_fresh > c_insert); under the fix it records
        // PRESENT@(1, c_insert) == the insert delta (idempotent).
        assert!(
            map.get(&key(d)).await.is_some(),
            "scenario5: d must be resident to read"
        );
        apply_delta_to_server(&mut server, endpoint, &tracker);
        assert!(
            server.has_digest(&d),
            "scenario5 precondition: after insert+read, the server must see d present"
        );

        // GENUINE eviction of d: the callback carries the VALUE's frozen insert
        // counter (`value.stamp()`), delivered here deterministically.
        let (frozen_counter, deliver) = map
            .test_remove_defer_callback(&key(d))
            .await
            .expect("d must be resident to evict");
        deliver(frozen_counter).await;

        // Apply the eviction delta to the server. The genuine evict MUST remove
        // d — it must NOT be gate-killed by the earlier read's stamp.
        apply_delta_to_server(&mut server, endpoint, &tracker);

        assert!(
            !server.has_digest(&d),
            "scenario5: on_get minted above the value stamp — the genuine \
             eviction (carrying the value's insert counter) was GATE-KILLED by \
             the read's higher fresh counter, so the blob is wrongly reported \
             PRESENT on the server forever. `fire_on_get` MUST carry the \
             resident value's frozen stamp (value.stamp()), not a fresh mint, \
             so the on_get delta equals the insert delta and the value's own \
             genuine eviction ties + wins the ABSENT≻PRESENT tie-break."
        );
    })
    .await
    .expect("scenario5 deadlocked — composite drift detector");
}
