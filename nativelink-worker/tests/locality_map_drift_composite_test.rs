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

use nativelink_config::stores::EvictionPolicy;
use nativelink_macro::nativelink_test;
use nativelink_store::callback_utils::ItemCallbackHolder;
use nativelink_util::blob_locality_map::{BlobLocalityMap, Stamp};
use nativelink_util::common::DigestInfo;
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use nativelink_util::store_trait::StoreKey;
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
// Scenario 2: restart dominance (bare-counter counterexample).
//
// A stale, HIGH-counter PRESENT stamp from the OLD process survives a restart
// with NO server wipe (adversarial isolation, matching HoldingsRestartEpoch.tla
// — imagine the reconnect wipe raced). The worker restarts (boot_epoch bumps
// STRICTLY, counter resets to c=1) and its FRESH-epoch delta must be able to
// TAKE CONTROL of the digest: re-register@(epoch_new, 1) must refresh the
// stored stamp, and a subsequent same-value evict@(epoch_new, 1) must then
// REMOVE d (the value is genuinely gone).
//
// The lexicographic (boot_epoch, counter) order makes a fresh epoch dominate
// ANY prior counter, so both the refresh AND the evict land. A BARE COUNTER
// wedges: (epoch_new, 1) loses to the stale high counter, so the register is
// NOT applied and the later evict is SUPPRESSED — d is stuck PRESENT with the
// dead old-epoch stamp forever, uncontrollable by the live worker.
//
// MUTATION (documented): strip the epoch from the server comparator (compare
// counter only). Then the fresh-epoch evict cannot beat the stale high counter
// → d stays PRESENT → the final `!has_digest` assertion RED-fails.
// ===============================================================
#[nativelink_test]
async fn scenario2_restart_dominance() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = "grpc://worker-a:50081";
        let mut server = BlobLocalityMap::new();
        let d = digest(2);

        // Old process registered d PRESENT at a HIGH counter and it is STILL
        // present on the server (no wipe on restart — the adversarial case).
        let epoch_old = 100u64;
        let high_c = 50u64;
        server.register_blobs_gated(endpoint, &[(d, Stamp::new(epoch_old, high_c))]);
        assert!(
            server.has_digest(&d),
            "scenario2 precondition: the old-epoch high-counter PRESENT must be \
             registered"
        );

        // Worker restarts: fresh boot_epoch (strictly greater), counter reset.
        // It re-acquires d (register@(epoch_new,1)) then evicts it
        // (evict@(epoch_new,1), same frozen value ts). Under the lexicographic
        // comparator the fresh epoch dominates the stale high counter, so the
        // register REFRESHES the stamp to (epoch_new,1) and the evict then wins
        // the ABSENT≻PRESENT tie-break and REMOVES d.
        let epoch_new = 101u64;
        server.register_blobs_gated(endpoint, &[(d, Stamp::new(epoch_new, 1))]);
        server.evict_blobs_gated(endpoint, &[(d, Stamp::new(epoch_new, 1))]);

        assert!(
            !server.has_digest(&d),
            "scenario2: the restarted worker's fresh (epoch_new, c=1) delta did \
             NOT take control of the digest — its evict could not beat the \
             stale (epoch_old, high_c) stamp, so d is wedged PRESENT with a \
             dead old-epoch stamp forever. The lexicographic (boot_epoch, \
             counter) comparator must let a fresh epoch dominate any prior \
             counter (a bare counter wedges here)."
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
