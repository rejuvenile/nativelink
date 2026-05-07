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
#[derive(Debug)]
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
}

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
        }
    }

    pub fn with_max_entries_per_endpoint(max_entries_per_endpoint: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries_per_endpoint,
            cap_drop_warn_state: Mutex::new(HashMap::new()),
        }
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
    /// asserts (a) at least one warn was emitted and (b) at most a small
    /// number were emitted (rate limit holds within the test wall-clock).
    /// The whole thing is wrapped in a `tokio::time::timeout` deadlock
    /// detector even though the registry is sync — the `traced_test`
    /// runtime is async and a regression that introduces a lock-order
    /// surprise would manifest as a hang here, not a panic.
    ///
    /// Mutation step: revert the `warn!` in
    /// [`AcPinRegistry::maybe_warn_cap_drop`] to `debug!`. This test
    /// red-fails with the bespoke "must observe at least one warn" message
    /// because `traced_test` only captures `INFO`-and-above by default
    /// (`debug` is filtered out).
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

        // (a) AT LEAST ONE WARN-level event was emitted AND (b) the
        // rate limit holds — `tracing-test` collects all lines into a
        // single buffer with the level prefix (`WARN`, `DEBUG`, etc.).
        // We MUST filter on `WARN` specifically: the bug we are
        // guarding against is the previous `debug!` site being
        // compiled out under `release_max_level_info`. In test builds
        // `release_max_level_info` is inactive, so a regression to
        // `debug!` would still appear in the buffer if we counted any
        // level — defeating the whole test. Filtering on " WARN " is
        // what makes the mutation step bite.
        // We bound to <= 2 (allowing one possible race between the 60s
        // rate-limit window and the test wall-clock, although the
        // window is far longer than the test will run).
        logs_assert(|lines: &[&str]| {
            let n = lines
                .iter()
                .filter(|l| {
                    l.contains(" WARN ") && l.contains("ac_pin_registry: per-endpoint cap reached")
                })
                .count();
            if n == 0 {
                Err("must observe at least one WARN-level cap-drop event \
                     — promotion from debug! to warn! reverted?"
                    .to_string())
            } else if n <= 2 {
                Ok(())
            } else {
                Err(format!(
                    "rate limit must hold — expected <= 2 cap-drop warns, observed {n}",
                ))
            }
        });
    }
}
