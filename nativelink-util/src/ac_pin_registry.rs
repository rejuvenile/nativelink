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

//! Server-side registry of AC pin advertisements from workers.
//!
//! This is intentionally a SEPARATE data structure from
//! [`crate::blob_locality_map::BlobLocalityMap`]. AC pins MUST NOT route
//! into the CAS-shared locality map because `action_digest` IS by REAPI
//! design the same digest as the Action proto in CAS — registering AC
//! pins against the locality map would weaponize the server's CAS
//! upload short-circuits in `bytestream_server::write` and
//! `cas_server::batch_update_blobs` (those functions skip uploads when
//! `WorkerProxyStore::has_with_results` returns Some via locality_map
//! lookup), causing permanent silent data loss of Action proto bytes.
//!
//! This commit establishes the advertisement channel ONLY. There are
//! no read-side consumers of the registry yet — the AC peer-fetch path
//! is a future commit. With no consumer, the registry's purpose is
//! purely to:
//!   - validate the wire-channel end-to-end (worker advertisement →
//!     server registration → boot-epoch wipe convergence),
//!   - allow observability tooling to surface AC pin distribution per
//!     worker without coupling AC pin advertisement into CAS reads.
//!
//! See `nativelink-proto/.../worker_api.proto:BlobsAvailableNotification.
//! pinned_ac_mirror_entries (field 17)` for the wire contract.
//!
//! # AC pin drain semantics
//!
//! The registry's per-endpoint AC pin sets are kept consistent with
//! worker truth via SIX mechanisms. The first is the steady-state
//! convergence path; the rest cover endpoint-lifecycle and
//! eventual-consistency drift.
//!
//! 1. **Field-17 replace-snapshot.** Every `BlobsAvailable` tick
//!    carries the worker's FULL CURRENT AC pin set in field 17. The
//!    server's per-endpoint set is REPLACED atomically with that
//!    snapshot via [`AcPinRegistry::replace_endpoint_ac_pins`] (called
//!    from the field-17 handler at
//!    `nativelink-service/src/worker_api_server.rs:1227`). Stale
//!    entries from prior ticks are dropped on every tick by
//!    construction — no explicit drain channel is needed for the
//!    steady-state registration path. This supersedes the historical
//!    additive [`AcPinRegistry::register_ac_pin`] shape; per
//!    `a2cb1db2` the symmetric `unregister_ac_pin` was removed when
//!    no production caller needed it, and replace-snapshot makes
//!    explicit single-pin unregister unnecessary on the registration
//!    path. `register_ac_pin` remains in the public API for tests
//!    and diagnostics that need additive insertion.
//!
//! 2. **`wipe_endpoint` on connection lifecycle events.**
//!    [`AcPinRegistry::wipe_endpoint`] is called from
//!    `worker_api_server.rs:531` (boot-epoch-flip on reconnect: the
//!    worker process is fresh, so its AC pin slate starts empty) and
//!    `:912` (disconnect cleanup: the worker is gone, so its
//!    advertised pins MUST not survive). Both paths fire
//!    independently — the boot-epoch wipe holds the
//!    `endpoint_state` mutex during the wipe so a racing disconnect
//!    cleanup observes a fully-wiped state, never a partial one. The
//!    [`AcPinRegistry::on_endpoint_wipe`] callback hook also fires
//!    (used by `AcProxyStore` to drop its cached worker connection
//!    adjacent to the registry wipe — sibling-of-#194).
//!
//! 3. **Per-endpoint cap.** [`DEFAULT_MAX_AC_PINS_PER_ENDPOINT`]
//!    bounds the per-endpoint set size. Both
//!    [`AcPinRegistry::register_ac_pin`] and
//!    [`AcPinRegistry::replace_endpoint_ac_pins`] honour the cap;
//!    over-cap entries are silently dropped with a single
//!    rate-limited cap-drop warn (per
//!    [`CAP_DROP_WARN_INTERVAL`], per endpoint). The cap is the
//!    backstop against a buggy or hostile worker; under healthy
//!    operation it should never fire.
//!
//! 4. **BIS-ack drain via [`AcPinRegistry::remove_digests_for_endpoint_in_store`].**
//!    When the server's slow-tier AC write completes and the
//!    `BlobsInStableStorage` broadcast fires, the AC sweep walks
//!    `endpoint_counts()` keys and removes the matching
//!    `(endpoint, store_id, digest)` entries from the registry
//!    (`src/bin/nativelink.rs:884`). Mirrors the worker-side
//!    `FastSlowStore::remove_local_ac_pins` drain from the same BIS
//!    broadcast — both sides converge on the BIS ack.
//!
//! 5. **AcProxyStore peer-fetch lazy prune.** When an AC peer-fetch
//!    against a worker selected via the registry returns
//!    `Code::NotFound`, the matching `(endpoint, digest)` is
//!    removed from the registry across all `store_id`s
//!    (`nativelink-store/src/ac_proxy_store.rs:354`). This closes
//!    the worker-fast-tier-eviction-vs-pin gap without a writer-side
//!    hook — the reader discovers the staleness when it tries to
//!    consume the pin, and the next reader skips straight to the
//!    next holder.
//!
//! 6. **Worker-side failure-prune via
//!    [`fast_slow_store::FastSlowStore::remove_local_ac_pin_on_failure`].**
//!    When `running_actions_manager::upload_ac_results`
//!    (`nativelink-worker/src/running_actions_manager.rs:4270`) sees
//!    `update_oneshot` return Err on the AC store, the matching
//!    `(store_id, digest)` is pruned from the worker's
//!    `dispatched_mirror_pins` BEFORE the err_tip-wrapped error
//!    propagates. The next `BlobsAvailable` tick's field-17
//!    snapshot omits the failed entry, and mechanism 1
//!    (replace-snapshot) drops it from the server registry on that
//!    tick. Sibling of CAS's `failed_slow_writes`-on-Err arm in
//!    `fast_slow_store.rs:948` (chunked-dispatcher path).
//!
//! # Open work — slow-tier asynchronous AC failure
//!
//! Mechanism 6 covers the synchronous Err path of
//! `ac_store.update_oneshot()` (typically a fast-tier failure).
//! The slow-tier ASYNCHRONOUS failure case is NOT yet covered:
//! when `update_oneshot` returns Ok on fast-tier success but the
//! spawned slow-tier write later fails, no failure-prune fires
//! today. Wiring that requires hooking into
//! `FastSlowStore::update`'s spawn-detach Err arm — the CAS analog
//! at `:948`. Tracked under the same #279 umbrella; the AC story
//! should converge on the same shape as #287's CAS-side
//! `failed_slow_writes` consumer (`fef138fb`: "drain server-side
//! failed_slow_writes via UploadMissingBlobs"; per its commit
//! message, "AC + worker-AC analogs are separate" — that work
//! lands as a follow-up).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use tracing::warn;

use crate::common::DigestInfo;

/// Per-worker AC pin set. Key is `(store_id, digest)` so multiple AC
/// stores per worker remain disambiguated server-side. The `store_id`
/// is the worker's configured AC store name (e.g. `"AC_MAIN_STORE"`).
type EndpointAcPins = HashSet<(Arc<str>, DigestInfo)>;

/// Server-side registry mapping `worker_cas_endpoint → set of (store_id,
/// digest)` AC pins advertised by that worker. Wrapped in
/// [`SharedAcPinRegistry`] for sharing across the
/// [`WorkerApiServer`](nativelink-service::worker_api_server) (which
/// registers / wipes entries) and any future AC peer-fetch reader.
///
/// **Cap on per-worker AC pin set:** the registry enforces a configured
/// cap on the number of `(store_id, digest)` tuples held per endpoint.
/// Advertisements beyond the cap are silently dropped to bound the
/// server's memory under a hostile or buggy worker. The cap default
/// matches the worker fast-tier capacity ceiling (100K AC entries per
/// worker × ~10 workers ⇒ ~1M tuples server-wide), which is also the
/// natural drain point for the BIS-based pin lifecycle.
pub struct AcPinRegistry {
    /// Per-endpoint AC pin sets.
    ///
    /// Storing the full set per endpoint (rather than a global keyed-by-
    /// `(endpoint, store_id, digest)` map) makes
    /// [`Self::wipe_endpoint`] O(1) — required for the boot-epoch wipe
    /// path which holds the `endpoint_state` mutex while wiping.
    inner: RwLock<HashMap<String, EndpointAcPins>>,
    /// Maximum number of `(store_id, digest)` tuples held per endpoint.
    /// Advertisements beyond the cap are silently dropped to bound
    /// server-side memory under a hostile or buggy worker.
    max_entries_per_endpoint: usize,
    /// Per-endpoint rate-limit state for cap-exceeded warnings.
    /// Maps `endpoint → (Option<last_warn_time>, drops_since_last_warn)`.
    /// `None` means "this endpoint has never been warned about" — the
    /// next cap-drop will warn unconditionally so operators see the
    /// first occurrence. A buggy or hostile worker advertising 1M+ AC
    /// pins must NOT be silent (operators need to see cap-burn) but
    /// must NOT flood the log either — after the first warn, emit at
    /// most one `warn!` per [`CAP_DROP_WARN_INTERVAL`] per endpoint,
    /// summarising the drops observed since the last warn.
    cap_drop_warn_state: Mutex<HashMap<String, (Option<Instant>, u64)>>,
    /// Callbacks fired (after the registry's own wipe completes) on
    /// every [`Self::wipe_endpoint`] call. Used by `AcProxyStore` to
    /// drop its cached worker AC connection on boot-epoch flip
    /// (sibling-of-#194 leak). The hook is generic enough to extend
    /// to other endpoint-keyed caches without changing every wipe
    /// call site.
    endpoint_wipe_callbacks: Mutex<Vec<EndpointWipeCallback>>,
}

impl core::fmt::Debug for AcPinRegistry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AcPinRegistry")
            .field("inner", &self.inner)
            .field("max_entries_per_endpoint", &self.max_entries_per_endpoint)
            .field("cap_drop_warn_state", &self.cap_drop_warn_state)
            .field(
                "endpoint_wipe_callbacks",
                &self.endpoint_wipe_callbacks.lock().len(),
            )
            .finish()
    }
}

/// Callback fired adjacent to [`AcPinRegistry::wipe_endpoint`] so
/// auxiliary per-endpoint state (e.g. `AcProxyStore::worker_connections`)
/// can be cleaned up without coupling the consumer of the registry to
/// every wipe call site. Receives the endpoint string that was just
/// wiped.
///
/// Re-entrance: `wipe_endpoint` snapshots and releases all of its own
/// locks before firing callbacks, so a callback MAY call back into the
/// registry (read or write). The constraint is only on locks the
/// callback itself owns elsewhere — keep callback bodies short and do
/// NOT take other contended locks.
pub type EndpointWipeCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Minimum interval between `warn!`-level cap-exceeded messages for
/// the SAME endpoint. Drops between warns are counted and reported in
/// the next warn's `drops_since_last_warn` field, so no event is lost
/// — only the per-event log line is suppressed.
const CAP_DROP_WARN_INTERVAL: core::time::Duration = core::time::Duration::from_secs(60);

/// Default cap on per-endpoint AC pin entries. Sized to the worker's
/// AC fast-tier capacity (configured today as a 100K-entry MemoryStore
/// in `worker.json5`) — a worker cannot legitimately advertise more
/// pins than its fast tier can hold. With ~10 workers × 100K = ~1M
/// tuples server-wide.
pub const DEFAULT_MAX_AC_PINS_PER_ENDPOINT: usize = 1_000_000;

impl AcPinRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries_per_endpoint: DEFAULT_MAX_AC_PINS_PER_ENDPOINT,
            cap_drop_warn_state: Mutex::new(HashMap::new()),
            endpoint_wipe_callbacks: Mutex::new(Vec::new()),
        }
    }

    pub fn with_max_entries_per_endpoint(max_entries_per_endpoint: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries_per_endpoint,
            cap_drop_warn_state: Mutex::new(HashMap::new()),
            endpoint_wipe_callbacks: Mutex::new(Vec::new()),
        }
    }

    /// Register a callback fired after every [`Self::wipe_endpoint`].
    /// Used by `AcProxyStore` (constructed in the bin) to drop its
    /// cached worker connection adjacent to the registry wipe. The
    /// callback fires AFTER the registry's own state has been cleared,
    /// with the registry's locks released, so it may take other locks
    /// freely.
    pub fn on_endpoint_wipe(&self, callback: EndpointWipeCallback) {
        self.endpoint_wipe_callbacks.lock().push(callback);
    }

    /// Register one `(store_id, digest)` AC pin against `endpoint`.
    /// Silently drops the entry if the endpoint's set is already at
    /// the configured cap (see `max_entries_per_endpoint`).
    ///
    /// Cap-exceeded drops emit a rate-limited `warn!` (at most one
    /// per [`CAP_DROP_WARN_INTERVAL`] per endpoint) so operators can
    /// observe a buggy or hostile worker burning the cap without the
    /// log being flooded. The warn includes the count of drops
    /// observed since the previous warn for the same endpoint.
    pub fn register_ac_pin(&self, endpoint: &str, store_id: Arc<str>, digest: DigestInfo) {
        let mut guard = self.inner.write();
        let cur_len = guard.get(endpoint).map_or(0, HashSet::len);
        let already_present = guard
            .get(endpoint)
            .is_some_and(|s| s.contains(&(store_id.clone(), digest)));
        if cur_len >= self.max_entries_per_endpoint && !already_present {
            // Cap reached and this would be a NEW entry — drop it.
            // Re-advertisements of EXISTING entries are still accepted
            // (HashSet::insert is idempotent on present).
            // Drop the inner write guard before taking the warn-state
            // mutex to avoid lock-order surprises with future readers.
            drop(guard);
            self.maybe_warn_cap_drop(endpoint, store_id.as_ref(), cur_len);
            return;
        }
        guard
            .entry(endpoint.to_string())
            .or_default()
            .insert((store_id, digest));
    }

    /// Replace endpoint's AC pin set atomically with `entries`. Used by
    /// the server's field-17 (`pinned_ac_mirror_entries`) handler:
    /// every `BlobsAvailable` advertisement carries the worker's
    /// FULL CURRENT AC pin set, so the server's per-endpoint set
    /// should be REPLACED with that snapshot rather than additively
    /// `register_ac_pin`'d. Stale entries from prior ticks are
    /// implicitly dropped on every advertisement — the registry
    /// tracks worker truth tick-by-tick without any explicit
    /// drain channel.
    ///
    /// Atomicity: takes ONE `inner.write()` and mutates the
    /// per-endpoint set in-place. A concurrent reader (e.g.
    /// `AcProxyStore::endpoint_holds_digest`) sees either the
    /// pre-replace state or the post-replace state, never a
    /// half-applied mix.
    ///
    /// Cap behaviour: applies the same `max_entries_per_endpoint`
    /// cap as [`Self::register_ac_pin`]. If `entries.len()` exceeds
    /// the cap, the first `max_entries_per_endpoint` entries are
    /// retained (`HashSet` ordering, but stable per-call) and the
    /// remainder are dropped with a single rate-limited cap-drop
    /// warn (NOT one per dropped entry).
    ///
    /// Empty `entries` clears the endpoint entirely (matching the
    /// wire intent: "the worker has no AC pins this tick"). The
    /// per-endpoint cap-drop warn rate-limit state is NOT cleared
    /// here — only [`Self::wipe_endpoint`] does that. Empty-entries
    /// is a normal advertisement, not a connection-lifecycle event.
    ///
    /// Supersedes [`Self::register_ac_pin`] for the field-17
    /// production path: register-as-additive was a load-bearing
    /// claim of correctness in earlier iterations, but the wire
    /// intent has always been replace-snapshot. `register_ac_pin`
    /// remains in the public API for tests / diagnostics that
    /// genuinely need additive insertion (see e.g.
    /// `nativelink-service/tests/ac_isolation_bazel_e2e_test.rs`,
    /// which seeds a single AC pin to drive the AC-vs-CAS
    /// digest-collision regression).
    pub fn replace_endpoint_ac_pins(
        &self,
        endpoint: &str,
        entries: &[(Arc<str>, DigestInfo)],
    ) {
        let cap = self.max_entries_per_endpoint;
        let drops = entries.len().saturating_sub(cap);
        // Build the new set OUTSIDE the inner write lock so a long
        // entries slice cannot extend the parking_lot critical
        // section. Truncation happens during set construction:
        // `take(cap)` stops inserting once cap is reached, so the
        // first `cap` entries (in slice order) are retained.
        let new_set: EndpointAcPins = entries.iter().take(cap).cloned().collect();

        let mut guard = self.inner.write();
        if new_set.is_empty() {
            // Empty advertisement → clear the endpoint entirely.
            // Matches the wire contract: the worker has no AC pins
            // this tick.
            guard.remove(endpoint);
        } else {
            // Replace the per-endpoint set in-place. `insert` returns
            // the prior value (Some when present, None when absent);
            // we don't need it.
            guard.insert(endpoint.to_string(), new_set);
        }
        // Drop the inner write guard before taking the warn-state
        // mutex to keep the warn lock window short (matches the
        // ordering used by `register_ac_pin`).
        drop(guard);

        if drops > 0 {
            // ONE rate-limited warn per replace call when the
            // advertisement is over-cap. We pass `cap` (not the
            // post-replace set size) as `cur_len` so the operator
            // sees the cap-firing event explicitly; `store_id` is
            // empty because the cap is per-endpoint, not per-store
            // (the prompt's intent is to surface cap-burn at the
            // endpoint level).
            self.maybe_warn_cap_drop(endpoint, "", cap);
        }
    }

    /// Rate-limit state update for a cap-exceeded drop. Emits one
    /// `warn!` per [`CAP_DROP_WARN_INTERVAL`] per endpoint; intervening
    /// drops are counted and reported in the next warn. The first
    /// drop for a previously-unseen endpoint always warns.
    fn maybe_warn_cap_drop(&self, endpoint: &str, store_id: &str, cur_len: usize) {
        let now = Instant::now();
        let mut state = self.cap_drop_warn_state.lock();
        let entry = state.entry(endpoint.to_string()).or_insert((None, 0));
        entry.1 = entry.1.saturating_add(1);
        let should_warn = entry
            .0
            .is_none_or(|last| now.duration_since(last) >= CAP_DROP_WARN_INTERVAL);
        if should_warn {
            let drops_since_last_warn = entry.1;
            entry.0 = Some(now);
            entry.1 = 0;
            // Drop the warn-state lock before emitting the trace event
            // to keep the lock window short under hostile-worker load.
            drop(state);
            warn!(
                endpoint,
                store_id,
                count = cur_len,
                cap = self.max_entries_per_endpoint,
                drops_since_last_warn,
                "ac_pin_registry: per-endpoint cap reached; dropping new AC pin"
            );
        }
    }

    /// Remove all AC pin entries for `endpoint` matching ANY of the
    /// `digests`. Used by the BIS broadcast loop's AC sweep — when an
    /// AC slow-write completes on the server, the digest is broadcast
    /// to all workers AND the matching server-side AC pin entries are
    /// dropped (the server now knows it has the AC entry stably).
    ///
    /// Removes across all `store_id`s for matching digests (a single
    /// digest under multiple AC stores collapses on confirm), mirroring
    /// the worker-side
    /// [`fast_slow_store::FastSlowStore::remove_local_ac_pins`]
    /// semantics. O(|set| + |digests|) using a `HashSet<DigestInfo>`
    /// lookup index built once per call.
    pub fn remove_digests_for_endpoint(&self, endpoint: &str, digests: &[DigestInfo]) {
        if digests.is_empty() {
            return;
        }
        let lookup: HashSet<&DigestInfo> = digests.iter().collect();
        let mut guard = self.inner.write();
        if let Some(set) = guard.get_mut(endpoint) {
            set.retain(|(_, d)| !lookup.contains(d));
            if set.is_empty() {
                guard.remove(endpoint);
            }
        }
    }

    /// Variant of [`Self::remove_digests_for_endpoint`] that ALSO
    /// constrains removals to the matching `store_id`. Used by the
    /// AC-BIS broadcast loop's per-store sweep — when an AC store's
    /// slow-tier write drains, we know exactly which `store_id` is
    /// stable, and want to scope the unregister so a digest that
    /// happened to be advertised under multiple AC stores doesn't
    /// have its OTHER store's pin entry collateral-damaged.
    pub fn remove_digests_for_endpoint_in_store(
        &self,
        endpoint: &str,
        store_id: &str,
        digests: &[DigestInfo],
    ) {
        if digests.is_empty() {
            return;
        }
        let lookup: HashSet<&DigestInfo> = digests.iter().collect();
        let mut guard = self.inner.write();
        if let Some(set) = guard.get_mut(endpoint) {
            set.retain(|(sid, d)| !(sid.as_ref() == store_id && lookup.contains(d)));
            if set.is_empty() {
                guard.remove(endpoint);
            }
        }
    }

    /// Batched variant of [`Self::remove_digests_for_endpoint_in_store`]
    /// for the BIS broadcast loop's AC sweep. Drops all matching
    /// `(store_id, digests)` tuples for one `endpoint` under a SINGLE
    /// `inner.write()` lock acquisition. With N AC stores per endpoint
    /// the per-tick lock count collapses from N to 1.
    pub fn remove_digests_for_endpoint_batch(
        &self,
        endpoint: &str,
        drains: &[(Arc<str>, &[DigestInfo])],
    ) {
        if drains.is_empty() {
            return;
        }
        let mut guard = self.inner.write();
        let Some(set) = guard.get_mut(endpoint) else {
            return;
        };
        if drains.len() == 1 {
            // Fast path: one (store_id, digests) tuple — same shape as
            // remove_digests_for_endpoint_in_store but reuses the
            // already-acquired write guard.
            let (store_id, digests) = &drains[0];
            if !digests.is_empty() {
                let lookup: HashSet<&DigestInfo> = digests.iter().collect();
                let sid = store_id.as_ref();
                set.retain(|(s, d)| !(s.as_ref() == sid && lookup.contains(d)));
            }
        } else {
            // Build per-store lookup tables once, then a single pass
            // over the set. Allocations bounded by drains.len() (today
            // at most a handful of AC stores).
            let lookups: Vec<(&str, HashSet<&DigestInfo>)> = drains
                .iter()
                .filter(|(_, d)| !d.is_empty())
                .map(|(s, d)| (s.as_ref(), d.iter().collect()))
                .collect();
            if !lookups.is_empty() {
                set.retain(|(sid, d)| {
                    !lookups
                        .iter()
                        .any(|(s, lk)| sid.as_ref() == *s && lk.contains(d))
                });
            }
        }
        if set.is_empty() {
            guard.remove(endpoint);
        }
    }

    /// Wipe every AC pin recorded for `endpoint`. Called on worker
    /// disconnect / boot-epoch change, sibling of
    /// [`crate::blob_locality_map::BlobLocalityMap::remove_endpoint`]
    /// for the CAS path (#141 / #174). O(1) over the outer map.
    ///
    /// Also clears the per-endpoint cap-drop warn rate-limit state
    /// so a reconnecting worker starts fresh.
    pub fn wipe_endpoint(&self, endpoint: &str) {
        self.inner.write().remove(endpoint);
        self.cap_drop_warn_state.lock().remove(endpoint);
        // Snapshot + release the lock before firing user callbacks to
        // avoid lock-order surprises if a callback re-enters the
        // registry (read or write).
        let callbacks: Vec<_> = self.endpoint_wipe_callbacks.lock().clone();
        for cb in callbacks {
            cb(endpoint);
        }
    }

    /// Test/diagnostic accessor: snapshot the current per-endpoint pin
    /// count map. Allocates one entry per known endpoint plus a
    /// per-endpoint `usize` count.
    pub fn endpoint_counts(&self) -> HashMap<String, usize> {
        let guard = self.inner.read();
        guard.iter().map(|(k, v)| (k.clone(), v.len())).collect()
    }

    /// Test/diagnostic accessor: number of distinct endpoints currently
    /// holding any AC pin entries.
    pub fn endpoint_count(&self) -> usize {
        self.inner.read().len()
    }

    /// Hot-path accessor: does `endpoint` currently hold ANY AC pin
    /// entry for `digest` (across all `store_id`s)? Production callers
    /// (`AcProxyStore::endpoints_holding`) consult this on every
    /// inner-NotFound to decide which workers to peer-fetch from. With
    /// ~10 workers × ~100K pins each, the prior `snapshot_endpoint+sort+scan`
    /// strategy allocated ~1M tuples + sorted them per call; this
    /// method is a single read-lock + linear scan with zero allocation.
    /// (A true O(1) reverse index would require a second per-endpoint
    /// HashMap keyed by digest; deferred until the linear-scan cost
    /// shows up in profiling.)
    pub fn endpoint_holds_digest(&self, endpoint: &str, digest: &DigestInfo) -> bool {
        self.inner
            .read()
            .get(endpoint)
            .is_some_and(|set| set.iter().any(|(_, d)| d == digest))
    }

    /// Test/diagnostic accessor: snapshot the AC pin set for `endpoint`,
    /// returning `None` when no entries are present. Allocates one Vec.
    pub fn snapshot_endpoint(&self, endpoint: &str) -> Option<Vec<(Arc<str>, DigestInfo)>> {
        let guard = self.inner.read();
        guard.get(endpoint).map(|set| {
            let mut out: Vec<_> = set.iter().cloned().collect();
            // Sort for deterministic test assertions; not required
            // semantically since registrations are unordered.
            out.sort();
            out
        })
    }
}

impl Default for AcPinRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared handle on the AC pin registry. Cloned into
/// [`WorkerApiServer`](nativelink-service::worker_api_server) so the
/// `BlobsAvailable` handler can call [`AcPinRegistry::register_ac_pin`]
/// and the disconnect / boot-epoch paths can call
/// [`AcPinRegistry::wipe_endpoint`].
pub type SharedAcPinRegistry = Arc<AcPinRegistry>;

/// Construct a fresh `SharedAcPinRegistry`.
pub fn new_shared_ac_pin_registry() -> SharedAcPinRegistry {
    Arc::new(AcPinRegistry::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(byte: u8) -> DigestInfo {
        DigestInfo::new([byte; 32], 100)
    }

    #[test]
    fn register_and_snapshot_returns_entries() {
        let reg = AcPinRegistry::new();
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(1));
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(2));
        reg.register_ac_pin("grpc://w2:50081", store_id, d(3));

        let snap1 = reg.snapshot_endpoint("grpc://w1:50081").unwrap();
        assert_eq!(snap1.len(), 2);
        let snap2 = reg.snapshot_endpoint("grpc://w2:50081").unwrap();
        assert_eq!(snap2.len(), 1);
        assert_eq!(reg.snapshot_endpoint("grpc://nope:50081"), None);
    }

    #[test]
    fn wipe_endpoint_drops_only_target() {
        let reg = AcPinRegistry::new();
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(1));
        reg.register_ac_pin("grpc://w2:50081", store_id, d(2));
        reg.wipe_endpoint("grpc://w1:50081");
        assert_eq!(reg.snapshot_endpoint("grpc://w1:50081"), None);
        assert!(reg.snapshot_endpoint("grpc://w2:50081").is_some());
    }

    #[test]
    fn on_endpoint_wipe_callback_fires_with_endpoint_string() {
        use std::sync::Mutex as StdMutex;

        let reg = AcPinRegistry::new();
        let calls: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let calls_clone = Arc::clone(&calls);
        reg.on_endpoint_wipe(Arc::new(move |ep: &str| {
            calls_clone.lock().unwrap().push(ep.to_string());
        }));

        // Under-action: callback fires once per wipe with the endpoint.
        reg.wipe_endpoint("grpc://w1:50081");
        reg.wipe_endpoint("grpc://w2:50081");

        let observed = calls.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![
                "grpc://w1:50081".to_string(),
                "grpc://w2:50081".to_string(),
            ],
            "endpoint-wipe callback MUST fire for every wiped endpoint \
             with the endpoint string — sibling-of-#194 leak: \
             AcProxyStore connection cache cannot be cleared without \
             this hook"
        );
    }

    #[test]
    fn endpoint_holds_digest_returns_membership_per_endpoint() {
        let reg = AcPinRegistry::new();
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(1));
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(2));
        reg.register_ac_pin("grpc://w2:50081", store_id, d(3));

        // Under-action: matching endpoint+digest pairs return true.
        assert!(reg.endpoint_holds_digest("grpc://w1:50081", &d(1)));
        assert!(reg.endpoint_holds_digest("grpc://w1:50081", &d(2)));
        assert!(reg.endpoint_holds_digest("grpc://w2:50081", &d(3)));

        // Over-action: cross-endpoint pairs MUST return false. This is
        // the membership invariant `AcProxyStore::endpoints_holding`
        // depends on — without it the proxy would peer-fetch from
        // workers that do not hold the digest, wasting bandwidth and
        // potentially returning corrupt bytes.
        assert!(
            !reg.endpoint_holds_digest("grpc://w1:50081", &d(3)),
            "endpoint w1 MUST NOT report holding w2's digest — \
             over-action: per-endpoint set membership leaked across \
             endpoints"
        );
        assert!(
            !reg.endpoint_holds_digest("grpc://w2:50081", &d(1)),
            "endpoint w2 MUST NOT report holding w1's digest"
        );
        // Unknown endpoint returns false (does not panic / error).
        assert!(!reg.endpoint_holds_digest("grpc://nope:50081", &d(1)));
        // Unknown digest returns false.
        assert!(!reg.endpoint_holds_digest("grpc://w1:50081", &d(99)));
    }

    #[test]
    fn remove_digests_for_endpoint_drops_only_matches() {
        let reg = AcPinRegistry::new();
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(1));
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(2));
        reg.register_ac_pin("grpc://w1:50081", store_id, d(3));
        reg.remove_digests_for_endpoint("grpc://w1:50081", &[d(2)]);
        let snap = reg.snapshot_endpoint("grpc://w1:50081").unwrap();
        assert_eq!(snap.len(), 2);
        assert!(snap.iter().any(|(_, x)| *x == d(1)));
        assert!(snap.iter().any(|(_, x)| *x == d(3)));
    }

    #[test]
    fn cap_drops_new_entries_when_full() {
        let reg = AcPinRegistry::with_max_entries_per_endpoint(2);
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(1));
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(2));
        // Third NEW entry is dropped.
        reg.register_ac_pin("grpc://w1:50081", store_id.clone(), d(3));
        let snap = reg.snapshot_endpoint("grpc://w1:50081").unwrap();
        assert_eq!(snap.len(), 2);
        // Re-registering an existing entry remains a no-op (no growth).
        reg.register_ac_pin("grpc://w1:50081", store_id, d(1));
        let snap = reg.snapshot_endpoint("grpc://w1:50081").unwrap();
        assert_eq!(snap.len(), 2);
    }

    /// Cap-exceeded drops MUST emit a `warn!` (not the previous `debug!`,
    /// which is compiled out under `release_max_level_info`) so operators
    /// see hostile / buggy worker cap-burn. AND they MUST be rate-limited
    /// — a worker advertising 100s of K of pins above the cap cannot be
    /// allowed to flood the log.
    ///
    /// This test drives 100 cap-exceeded inserts for one endpoint and
    /// asserts EXACTLY one warn was emitted: the contract is "at most
    /// one `warn!` per [`CAP_DROP_WARN_INTERVAL`] (60s) per endpoint",
    /// the test wall-clock is bounded by the 5s deadlock-detector
    /// timeout, and the first drop on a previously-unseen endpoint is
    /// the unconditional baseline warn. The 60s window therefore
    /// dominates the test wall-clock by >12x — no scheduler jitter can
    /// flip a second warn into the window.
    ///
    /// The whole thing is wrapped in a `tokio::time::timeout` deadlock
    /// detector even though the registry is sync — the `traced_test`
    /// runtime is async and a regression that introduces a lock-order
    /// surprise would manifest as a hang here, not a panic.
    ///
    /// Mutation step: revert the `warn!` in
    /// [`AcPinRegistry::maybe_warn_cap_drop`] to `debug!`. This test
    /// red-fails with the bespoke "must observe exactly one WARN-level
    /// cap-drop event" message. The assertion filters on the line's
    /// `" WARN "` level prefix specifically: in test builds the
    /// `release_max_level_info` compile-time gate is inactive and a
    /// `debug!` regression would still land in the captured buffer at
    /// `DEBUG` level — only the level filter lets us distinguish them.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn cap_exceeded_emits_rate_limited_warn() {
        let result = tokio::time::timeout(core::time::Duration::from_secs(5), async {
            let reg = AcPinRegistry::with_max_entries_per_endpoint(10);
            let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
            let endpoint = "grpc://hostile-worker:50081";
            // Fill to cap.
            for i in 0..10u8 {
                reg.register_ac_pin(endpoint, store_id.clone(), d(i));
            }
            // 100 cap-exceeded NEW entries.
            for i in 100..200u8 {
                reg.register_ac_pin(endpoint, store_id.clone(), d(i));
            }
            // Cap held: still exactly 10 entries.
            let snap = reg.snapshot_endpoint(endpoint).unwrap();
            assert_eq!(snap.len(), 10, "cap must hold under cap-exceeded burst");
        })
        .await;
        result.expect(
            "cap_exceeded warn path must not deadlock — \
             rate-limit-state lock-order contract violated",
        );

        // EXACTLY one WARN-level event was emitted — `tracing-test`
        // collects all lines into a single buffer with the level
        // prefix (`WARN`, `DEBUG`, etc.). We MUST filter on `WARN`
        // specifically: the bug we are guarding against is the
        // previous `debug!` site being compiled out under
        // `release_max_level_info`. In test builds
        // `release_max_level_info` is inactive, so a regression to
        // `debug!` would still appear in the buffer if we counted any
        // level — defeating the whole test. Filtering on " WARN " is
        // what makes the mutation step bite.
        //
        // The contract is exactly one `warn!` per 60s window per
        // endpoint. Test wall-clock is bounded by the 5s deadlock
        // timeout above, so a 60s-boundary race is impossible.
        logs_assert(|lines: &[&str]| {
            let n = lines
                .iter()
                .filter(|l| {
                    l.contains(" WARN ") && l.contains("ac_pin_registry: per-endpoint cap reached")
                })
                .count();
            if n == 0 {
                Err("must observe exactly one WARN-level cap-drop event \
                     — promotion from debug! to warn! reverted?"
                    .to_string())
            } else if n == 1 {
                Ok(())
            } else {
                Err(format!(
                    "rate limit must hold — expected exactly 1 cap-drop warn, observed {n}",
                ))
            }
        });
    }

    /// Reconnect-after-wipe contract: when a worker reconnects (boot-
    /// epoch wipe via [`AcPinRegistry::wipe_endpoint`]), the per-
    /// endpoint cap-drop rate-limit state MUST be cleared so the
    /// FIRST cap-drop after reconnect emits a fresh `warn!`
    /// immediately — operators must see the new connection's first
    /// cap-burn, not silently inherit the previous connection's "warn
    /// emitted within last 60s" suppression.
    ///
    /// Asymmetric coverage:
    /// - Under-action: the warn fires on first cap-drop of the new
    ///   connection (this test).
    /// - Over-action: pre-wipe drops do NOT inflate the post-wipe
    ///   warn-count (asserted via the `n_pre == 1 && n_total == 2`
    ///   structure: a missing wipe-clear would suppress the second
    ///   warn entirely under the 60s window, leaving `n_total == 1`).
    ///
    /// Mutation step: comment out the
    /// `self.cap_drop_warn_state.lock().remove(endpoint);` line inside
    /// [`AcPinRegistry::wipe_endpoint`]. This test red-fails with the
    /// bespoke "post-wipe cap-drop must warn immediately" message
    /// because the second batch of 5 cap-drops hits the still-suppressed
    /// 60s window from the pre-wipe baseline warn.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn cap_exceeded_warns_immediately_after_wipe_endpoint() {
        let result = tokio::time::timeout(core::time::Duration::from_secs(5), async {
            let reg = AcPinRegistry::with_max_entries_per_endpoint(2);
            let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
            let endpoint = "grpc://reconnecting-worker:50081";

            // Fill to cap.
            reg.register_ac_pin(endpoint, store_id.clone(), d(1));
            reg.register_ac_pin(endpoint, store_id.clone(), d(2));

            // 5 cap-exceeded NEW entries — 1 baseline warn under the
            // 60s rate-limit window.
            for i in 100..105u8 {
                reg.register_ac_pin(endpoint, store_id.clone(), d(i));
            }

            // Wipe the endpoint (simulates worker reconnect).
            reg.wipe_endpoint(endpoint);

            // Re-register entries up to cap on the post-wipe
            // connection — same endpoint string, fresh state.
            reg.register_ac_pin(endpoint, store_id.clone(), d(1));
            reg.register_ac_pin(endpoint, store_id.clone(), d(2));

            // 5 more cap-exceeded NEW entries — must produce a SECOND
            // warn IMMEDIATELY (post-wipe state is None → first drop
            // warns unconditionally). Without the wipe-clear, the 60s
            // rate-limit window from the pre-wipe baseline warn would
            // suppress this second warn entirely (test wall-clock is
            // bounded by the 5s timeout).
            for i in 200..205u8 {
                reg.register_ac_pin(endpoint, store_id.clone(), d(i));
            }
        })
        .await;
        result.expect(
            "post-wipe cap-drop must warn immediately — \
             wipe_endpoint failed to clear cap_drop_warn_state",
        );

        logs_assert(|lines: &[&str]| {
            let n = lines
                .iter()
                .filter(|l| {
                    l.contains(" WARN ") && l.contains("ac_pin_registry: per-endpoint cap reached")
                })
                .count();
            if n == 2 {
                Ok(())
            } else {
                Err(format!(
                    "post-wipe cap-drop must warn immediately — \
                     expected exactly 2 cap-drop warns (1 pre-wipe baseline + \
                     1 post-wipe baseline), observed {n}; \
                     wipe_endpoint failed to clear cap_drop_warn_state",
                ))
            }
        });
    }

    /// Replace-snapshot under-action: registering X1, X2, X3 then
    /// REPLACING with [X1, X3] MUST drop X2. The wire contract is
    /// "field 17 carries the worker's full current pin set"; the
    /// server's per-endpoint set must mirror that snapshot tick-
    /// by-tick. An additive regression (e.g. swapping `replace`
    /// with `extend`) would resurrect X2 from the prior advertisement.
    ///
    /// Mutation step: change the body of
    /// `replace_endpoint_ac_pins`'s lock-block from `guard.insert(...)`
    /// to `guard.entry(...).or_default().extend(new_set);` (additive
    /// merge) — this test red-fails with the bespoke "MUST replace,
    /// not extend" message.
    #[test]
    fn replace_endpoint_ac_pins_replaces_set_atomically() {
        let reg = AcPinRegistry::new();
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        let endpoint = "grpc://w1:50081";
        let x1 = (store_id.clone(), d(1));
        let x2 = (store_id.clone(), d(2));
        let x3 = (store_id.clone(), d(3));

        // Initial advertisement: X1, X2, X3.
        reg.replace_endpoint_ac_pins(endpoint, &[x1.clone(), x2.clone(), x3.clone()]);
        let snap = reg.snapshot_endpoint(endpoint).unwrap();
        assert_eq!(snap.len(), 3);

        // Second advertisement: X1, X3 only — X2 dropped on the
        // wire side.
        reg.replace_endpoint_ac_pins(endpoint, &[x1.clone(), x3.clone()]);

        let snap = reg.snapshot_endpoint(endpoint).unwrap();
        assert_eq!(
            snap.len(),
            2,
            "replace_endpoint_ac_pins MUST replace, not extend — additive \
             regression would resurrect X2 from prior advertisement"
        );
        assert!(snap.contains(&x1));
        assert!(snap.contains(&x3));
        assert!(
            !snap.contains(&x2),
            "replace_endpoint_ac_pins MUST replace, not extend — additive \
             regression would resurrect X2 from prior advertisement"
        );
    }

    /// Cap honoured under replace: pre-fill cap entries via
    /// `register_ac_pin`, then replace with cap+10 entries. Exactly
    /// `cap` entries are retained on the post-replace set; the
    /// remainder are silently dropped (with a single rate-limited
    /// warn, asserted under traced_test in
    /// `replace_endpoint_ac_pins_emits_cap_drop_warn` if needed —
    /// kept out-of-scope here to keep the under-action test pure).
    ///
    /// Mutation step: change `entries.iter().take(cap)` to
    /// `entries.iter()` (no truncation). This test red-fails with
    /// the bespoke "replace_endpoint_ac_pins MUST honour the
    /// per-endpoint cap" message.
    #[test]
    fn replace_endpoint_ac_pins_honours_cap() {
        let cap = 5;
        let reg = AcPinRegistry::with_max_entries_per_endpoint(cap);
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        let endpoint = "grpc://w1:50081";

        // Build cap+10 = 15 entries.
        let entries: Vec<_> = (0..(cap + 10) as u8)
            .map(|i| (store_id.clone(), d(i)))
            .collect();
        reg.replace_endpoint_ac_pins(endpoint, &entries);

        let snap = reg.snapshot_endpoint(endpoint).unwrap();
        assert_eq!(
            snap.len(),
            cap,
            "replace_endpoint_ac_pins MUST honour the per-endpoint cap — \
             exactly {} entries must be retained, observed {}",
            cap,
            snap.len(),
        );
    }

    /// Empty entries → endpoint is cleared entirely. Wire intent:
    /// the worker has no AC pins this tick, so the server's per-
    /// endpoint row should be removed. (Alternative semantics —
    /// "leave the previous set in place" — would drift if the
    /// worker ever advertises an empty set.)
    ///
    /// Mutation step: change the empty-branch in
    /// `replace_endpoint_ac_pins` from `guard.remove(endpoint)` to
    /// `()` (no-op). This test red-fails with the bespoke
    /// "empty replace MUST clear the endpoint" message.
    #[test]
    fn replace_endpoint_ac_pins_clears_endpoint_on_empty_entries() {
        let reg = AcPinRegistry::new();
        let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        let endpoint = "grpc://w1:50081";

        // Seed two entries.
        reg.replace_endpoint_ac_pins(
            endpoint,
            &[(store_id.clone(), d(1)), (store_id.clone(), d(2))],
        );
        assert_eq!(reg.snapshot_endpoint(endpoint).unwrap().len(), 2);

        // Empty advertisement.
        reg.replace_endpoint_ac_pins(endpoint, &[]);

        assert_eq!(
            reg.snapshot_endpoint(endpoint),
            None,
            "empty replace MUST clear the endpoint — wire contract is \
             'worker has no AC pins this tick', not 'leave the previous \
             advertisement in place'",
        );
    }

    /// (#278B under-action) `remove_digests_for_endpoint_batch` MUST
    /// drop every matching `(store_id, digests)` tuple under one write
    /// lock. The test asserts the endpoint becomes empty after draining
    /// three tuples and that an OTHER endpoint's pin (also matching one
    /// of the digests) survives — over-action across endpoints.
    ///
    /// Mutation step: replace the body of
    /// `remove_digests_for_endpoint_batch` with bare `return;`. This
    /// test red-fails with the bespoke "MUST drop all matching tuples"
    /// message.
    #[test]
    fn remove_digests_for_endpoint_batch_drops_per_store_tuples() {
        let reg = AcPinRegistry::new();
        let endpoint = "grpc://w1:50081";
        let other_endpoint = "grpc://w2:50081";
        let s1: Arc<str> = Arc::from("AC_MAIN");
        let s2: Arc<str> = Arc::from("AC_AUX");
        let s3: Arc<str> = Arc::from("AC_THIRD");
        // Register digests across 3 store_ids.
        reg.register_ac_pin(endpoint, s1.clone(), d(1));
        reg.register_ac_pin(endpoint, s1.clone(), d(2));
        reg.register_ac_pin(endpoint, s2.clone(), d(2));
        reg.register_ac_pin(endpoint, s2.clone(), d(3));
        reg.register_ac_pin(endpoint, s3.clone(), d(4));
        // Over-action precondition: a different endpoint also holds d(2)
        // under the same store_id, must SURVIVE the batch.
        reg.register_ac_pin(other_endpoint, s1.clone(), d(2));

        let s1_d = [d(1), d(2)];
        let s2_d = [d(2), d(3)];
        let s3_d = [d(4)];
        let drains: Vec<(Arc<str>, &[DigestInfo])> = vec![
            (s1.clone(), &s1_d as &[_]),
            (s2.clone(), &s2_d as &[_]),
            (s3.clone(), &s3_d as &[_]),
        ];
        reg.remove_digests_for_endpoint_batch(endpoint, &drains);

        assert_eq!(
            reg.snapshot_endpoint(endpoint),
            None,
            "remove_digests_for_endpoint_batch MUST drop all matching \
             tuples — expected endpoint to be empty after draining its \
             three (store_id, digests) tuples",
        );

        // Over-action: other endpoint's pin survives.
        let snap_other = reg
            .snapshot_endpoint(other_endpoint)
            .expect(
                "other endpoint's pin MUST survive — over-action: batch \
                 leaked across endpoints",
            );
        assert_eq!(snap_other.len(), 1);
        assert_eq!(snap_other[0].1, d(2));
    }

    /// (#278B over-action) `remove_digests_for_endpoint_batch` MUST
    /// scope removals to the (store_id, digest) tuple, NOT digest-only:
    /// a pin under store_id A whose digest also appears in store_id B's
    /// drain list must survive when only B is drained.
    #[test]
    fn remove_digests_for_endpoint_batch_scoped_to_store_id() {
        let reg = AcPinRegistry::new();
        let endpoint = "grpc://w1:50081";
        let s1: Arc<str> = Arc::from("AC_MAIN");
        let s2: Arc<str> = Arc::from("AC_AUX");
        // d(7) appears under BOTH store_ids.
        reg.register_ac_pin(endpoint, s1.clone(), d(7));
        reg.register_ac_pin(endpoint, s2.clone(), d(7));

        // Drain d(7) ONLY for s2.
        let s2_d = [d(7)];
        let drains: Vec<(Arc<str>, &[DigestInfo])> = vec![(s2.clone(), &s2_d as &[_])];
        reg.remove_digests_for_endpoint_batch(endpoint, &drains);

        // s1's pin for d(7) MUST survive.
        let snap = reg
            .snapshot_endpoint(endpoint)
            .expect("endpoint MUST still hold s1's d(7)");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].0.as_ref(), "AC_MAIN");
        assert_eq!(snap[0].1, d(7));
    }
}
