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

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use nativelink_error::{Error, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, group, publish,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::common::DigestInfo;

// DEBUG INSTRUMENTATION (remove after wedge root cause confirmed):
// Targets the cover.o wedge digest to expose every locality_map mutation
// touching it (insert, evict, full-endpoint remove).
const DEBUG_DIGEST_HASH_HEX: &str =
    "3418dec2ac048e354993d688bc4cba02660d523f15a148f090a99f79d5adedaa";
const DEBUG_DIGEST_SIZE: u64 = 1_726_208;

#[inline]
fn debug_digest_match(d: &DigestInfo) -> bool {
    d.size_bytes() == DEBUG_DIGEST_SIZE
        && format!("{d}").starts_with(DEBUG_DIGEST_HASH_HEX)
}

/// A hasher that uses the first 8 bytes of a DigestInfo's packed SHA-256 hash
/// directly as the hash value. Since SHA-256 output is uniformly distributed,
/// this is a perfect hash input — no need for SipHash to re-mix it.
///
/// This saves ~20ns per HashMap operation on 40-byte DigestInfo keys, which
/// adds up to significant CPU savings when processing 500K+ digests/second
/// from worker BlobsAvailable notifications.
#[derive(Default, Clone, Copy, Debug)]
pub struct DigestHasher(u64);

impl Hasher for DigestHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Derived Hash for DigestInfo calls:
        //   1. [u8; 32]::hash → write_usize(32) then write(32_bytes)
        //   2. u64::hash → write_u64(size_bytes)
        // We capture the first 8 bytes of the SHA-256 hash (already uniformly
        // distributed) and mix in the size via write_u64 below.
        // write_usize is a no-op so the length prefix is harmlessly discarded.
        if bytes.len() >= 8 {
            self.0 = u64::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
        } else {
            // Fallback for smaller writes.
            for &b in bytes {
                self.0 = self.0.wrapping_mul(31).wrapping_add(b as u64);
            }
        }
    }

    #[inline]
    fn write_usize(&mut self, _: usize) {
        // Ignore length prefixes from [u8; N]::hash — we only care about
        // the actual hash bytes (from write) and size_bytes (from write_u64).
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        // Mix in size_bytes to differentiate digests with same hash prefix
        // but different sizes (extremely rare for SHA-256 but correct).
        self.0 = self.0.wrapping_add(i);
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub struct DigestBuildHasher;

impl BuildHasher for DigestBuildHasher {
    type Hasher = DigestHasher;

    #[inline]
    fn build_hasher(&self) -> DigestHasher {
        DigestHasher(0)
    }
}

/// (#locality-map-drift) A per-mutation logical LWW timestamp:
/// `(boot_epoch, counter)`, compared LEXICOGRAPHICALLY (boot_epoch dominates).
/// `boot_epoch` is the reporting worker's `boot_epoch_id`; `counter` is the
/// per-map monotonic counter frozen into the blob's resident value at insert.
/// A restarted worker's fresh boot_epoch outranks any stale counter from a
/// prior process. `0/0` = unset/legacy (treated as the oldest possible stamp).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stamp {
    pub boot_epoch: u64,
    pub counter: u64,
}

impl Stamp {
    #[inline]
    #[must_use]
    pub const fn new(boot_epoch: u64, counter: u64) -> Self {
        Self {
            boot_epoch,
            counter,
        }
    }

    /// Strict lexicographic `>` on `(boot_epoch, counter)`. `pub` so the worker
    /// `BlobChangeTracker`'s local LWW applies the SAME comparison as the
    /// server gate.
    #[inline]
    #[must_use]
    pub fn gt(self, other: Self) -> bool {
        self.boot_epoch > other.boot_epoch
            || (self.boot_epoch == other.boot_epoch && self.counter > other.counter)
    }

    /// Lexicographic `==` (the equal-ts case for the ABSENT ≻ PRESENT
    /// tie-break). `pub` for the worker-side LWW (see `gt`).
    #[inline]
    #[must_use]
    pub fn eq_ts(self, other: Self) -> bool {
        self.boot_epoch == other.boot_epoch && self.counter == other.counter
    }
}

/// Compact per-digest endpoint list. With only ~10 workers, a Vec with linear
/// scan is faster than HashMap due to:
/// - No hashing overhead for Arc<str> keys
/// - Cache-friendly sequential memory access
/// - No bucket array overhead (HashMap has 50%+ empty slots)
/// - Fewer allocations (one Vec vs HashMap's bucket array + entries)
///
/// (#locality-map-drift) Each PRESENT entry now carries the `(boot_epoch,
/// counter)` logical-LWW `Stamp` at which that endpoint became present for
/// this digest (frozen from the reporting worker's resident value). The
/// worker-delta apply path (`register_blobs_gated`/`evict_blobs_gated`)
/// LWW-gates against this stamp so an out-of-order STALE eviction of a
/// re-admitted hot blob is suppressed. Only PRESENT endpoints are stored
/// (evict removes the entry); the list is reclaimed to empty on
/// `evict_blobs`-to-empty + `remove_endpoint` — bounded identically to today's
/// `endpoint_blobs` index. Ungated `evict_blobs`/`register_blobs` (server
/// self-heal + full-snapshot) mutate residency without consulting the stamp.
#[derive(Debug, Clone, Default)]
pub struct EndpointList {
    // UNBOUNDED-OK: stamp lives inside each PRESENT entry (Arc<str>, Stamp);
    // reclaimed on evict-to-empty (blobs.remove) + remove_endpoint, so it is
    // bounded identically to the endpoint_blobs reverse index — no tombstones
    // accumulate (a genuinely-evicted endpoint is REMOVED, not left ABSENT).
    entries: Vec<(Arc<str>, Stamp)>,
}

impl EndpointList {
    /// Insert an endpoint if not already present, stamping it. Returns true if
    /// newly added. Ungated (full-snapshot / legacy path): a present endpoint's
    /// stamp is refreshed to `stamp` unconditionally.
    #[inline]
    fn insert_stamped(&mut self, endpoint: &Arc<str>, stamp: Stamp) -> bool {
        for existing in &mut self.entries {
            if Arc::ptr_eq(&existing.0, endpoint) || *existing.0 == **endpoint {
                existing.1 = stamp;
                return false;
            }
        }
        self.entries.push((endpoint.clone(), stamp));
        true
    }

    /// (#locality-map-drift) LWW-gated register: apply the PRESENT@`stamp` iff
    /// it is strictly newer than the stored stamp for this endpoint (a later
    /// value re-registers). If the endpoint is not present, add it (a fresh
    /// PRESENT always applies — the map has no ABSENT tombstone to lose to, so
    /// a resident blob can never wedge false-missing). Returns true if newly
    /// added.
    #[inline]
    fn register_gated(&mut self, endpoint: &Arc<str>, stamp: Stamp) -> bool {
        for existing in &mut self.entries {
            if Arc::ptr_eq(&existing.0, endpoint) || *existing.0 == **endpoint {
                if stamp.gt(existing.1) {
                    existing.1 = stamp;
                }
                return false;
            }
        }
        self.entries.push((endpoint.clone(), stamp));
        true
    }

    /// Remove an endpoint (ungated: server self-heal / full-snapshot). Returns
    /// true if it was present.
    #[inline]
    fn remove(&mut self, endpoint: &str) -> bool {
        if let Some(pos) = self.entries.iter().position(|(e, _)| &**e == endpoint) {
            self.entries.swap_remove(pos);
            true
        } else {
            false
        }
    }

    /// (#locality-map-drift) LWW-gated evict: remove the endpoint iff the
    /// incoming ABSENT@`stamp` wins the LWW — apply iff `stamp > stored` OR
    /// (`stamp == stored`), i.e. ABSENT ≻ PRESENT at equal ts (an evict-of-V is
    /// always causally after insert-of-V, so a same-value evict removes a
    /// genuinely-gone blob). A STRICTLY-OLDER evict (a stale, re-ordered
    /// eviction of a superseded value) is SUPPRESSED — this is the
    /// false-missing fix. Returns true if the endpoint was removed.
    #[inline]
    fn evict_gated(&mut self, endpoint: &str, stamp: Stamp) -> bool {
        if let Some(pos) = self.entries.iter().position(|(e, _)| &**e == endpoint) {
            let stored = self.entries[pos].1;
            // ABSENT ≻ PRESENT at equal ts (tie-break), else strict-newer.
            if stamp.gt(stored) || stamp.eq_ts(stored) {
                self.entries.swap_remove(pos);
                return true;
            }
        }
        false
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &Arc<str>> {
        self.entries.iter().map(|(e, _)| e)
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Arc<str>> {
        self.entries.iter().map(|(e, _)| e)
    }

    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(e, _)| &**e == key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the given endpoint is in the list.
    #[inline]
    pub fn get(&self, key: &str) -> Option<&Arc<str>> {
        self.entries.iter().find(|(e, _)| &**e == key).map(|(e, _)| e)
    }
}

impl<'a> IntoIterator for &'a EndpointList {
    type Item = &'a Arc<str>;
    type IntoIter = core::iter::Map<
        std::slice::Iter<'a, (Arc<str>, Stamp)>,
        fn(&'a (Arc<str>, Stamp)) -> &'a Arc<str>,
    >;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter().map(|(e, _)| e)
    }
}

pub type DigestMap<V> = HashMap<DigestInfo, V, DigestBuildHasher>;
type DigestSet = HashSet<DigestInfo, DigestBuildHasher>;

/// Tracks which worker endpoints have which blobs, enabling peer-to-peer
/// blob fetching between workers.
///
/// The map is bidirectional:
/// - `blobs`: digest → set of endpoints that hold the blob
/// - `endpoint_blobs`: endpoint → set of digests (for fast cleanup on disconnect)
///
/// Performance notes:
/// - DigestInfo keys use a passthrough hasher (first 8 bytes of SHA-256 are
///   already uniformly distributed, so SipHash re-mixing is pure waste).
/// - Per-digest endpoint lists use Vec with linear scan instead of HashMap
///   (only ~10 workers, so cache-friendly linear scan beats hashing).
///
/// The locality map is trusted-until-explicit-eviction: entries persist
/// until a worker disconnects, sends an eviction notification, or sends a
/// full snapshot. There is no per-entry staleness filter at lookup time.
#[deprecated(note = "Lookups no longer filter by age; entries persist until explicit eviction.")]
pub const LOCALITY_TTL: Duration = Duration::from_secs(120);

/// Cleanup relies on explicit eviction notifications, worker disconnect,
/// and full-snapshot replacement. Lookups never apply a staleness filter.
#[derive(Debug)]
pub struct BlobLocalityMap {
    /// digest → endpoint list
    blobs: DigestMap<EndpointList>,
    /// endpoint → set of digests (for fast cleanup on disconnect)
    endpoint_blobs: HashMap<Arc<str>, DigestSet>,
}

impl BlobLocalityMap {
    pub fn new() -> Self {
        Self {
            blobs: HashMap::with_hasher(DigestBuildHasher),
            endpoint_blobs: HashMap::new(),
        }
    }

    /// Register that the given digests are available on the given endpoint.
    ///
    /// Performance: Each digest requires one lookup in `blobs` (passthrough hash
    /// of first 8 SHA-256 bytes) plus a linear scan of <=10 endpoint entries.
    /// The `endpoint_blobs` reverse index also uses the passthrough hasher.
    /// Arc<str> cloning is avoided for existing endpoints (only atomic refcount
    /// on first insert per endpoint).
    pub fn register_blobs(&mut self, endpoint: &str, digests: &[DigestInfo]) {
        self.register_blobs_iter(endpoint, digests.iter().copied());
    }

    /// Iterator-form of [`register_blobs`]. Lets callers chain multiple
    /// digest sources (e.g. normal `digests` + `pinned_mirror`) without
    /// allocating a merged `Vec`. The endpoint `Arc<str>` is still
    /// allocated exactly once.
    pub fn register_blobs_iter<I>(&mut self, endpoint: &str, digests: I)
    where
        I: IntoIterator<Item = DigestInfo>,
    {
        // Allocate the endpoint Arc<str> once; the EndpointList.insert() only
        // clones it when the endpoint is genuinely new for that digest.
        let ep: Arc<str> = endpoint.into();
        let digest_set = self
            .endpoint_blobs
            .entry(ep.clone())
            .or_insert_with(|| HashSet::with_hasher(DigestBuildHasher));

        for digest in digests {
            digest_set.insert(digest);
            let entry = self.blobs.entry(digest).or_default();
            // (#locality-map-drift) Ungated register: this path is the
            // FULL-SNAPSHOT / reload / test register (post-`remove_endpoint`
            // wipe on reconnect, so there is no stored stamp to gate against).
            // Default `Stamp` (0,0); the first real ts-carrying delta refreshes
            // it. The DELTA apply path uses `register_blobs_gated`.
            entry.insert_stamped(&ep, Stamp::default());
            if debug_digest_match(&digest) {
                let endpoints: Vec<String> = entry.iter().map(|s| s.as_ref().to_string()).collect();
                info!(?digest, %ep, after_endpoints = ?endpoints, "DEBUG: locality_map register_blobs_iter for wedge digest");
            }
        }
    }

    /// (#locality-map-drift) LWW-gated register for the WORKER-DELTA apply path
    /// (`worker_api_server.rs` `handle_blobs_available`). Each `(digest, stamp)`
    /// carries the reporting worker's frozen `(boot_epoch, counter)`. A PRESENT
    /// entry is refreshed only by a strictly-newer stamp; a fresh PRESENT (no
    /// stored entry) always applies. Bounded identically to `register_blobs`.
    pub fn register_blobs_gated(&mut self, endpoint: &str, digests: &[(DigestInfo, Stamp)]) {
        if digests.is_empty() {
            return;
        }
        let ep: Arc<str> = endpoint.into();
        let digest_set = self
            .endpoint_blobs
            .entry(ep.clone())
            .or_insert_with(|| HashSet::with_hasher(DigestBuildHasher));
        for &(digest, stamp) in digests {
            digest_set.insert(digest);
            let entry = self.blobs.entry(digest).or_default();
            entry.register_gated(&ep, stamp);
            if debug_digest_match(&digest) {
                let endpoints: Vec<String> = entry.iter().map(|s| s.as_ref().to_string()).collect();
                info!(?digest, %ep, ?stamp, after_endpoints = ?endpoints, "DEBUG: locality_map register_blobs_gated for wedge digest");
            }
        }
    }

    /// Remove specific digests from the given endpoint (eviction notification).
    pub fn evict_blobs(&mut self, endpoint: &str, digests: &[DigestInfo]) {
        if let Some(digest_set) = self.endpoint_blobs.get_mut(endpoint) {
            for digest in digests {
                digest_set.remove(digest);
                if let Some(endpoints) = self.blobs.get_mut(digest) {
                    let before: Vec<String> = endpoints.iter().map(|s| s.as_ref().to_string()).collect();
                    endpoints.remove(endpoint);
                    let after: Vec<String> = endpoints.iter().map(|s| s.as_ref().to_string()).collect();
                    if debug_digest_match(digest) {
                        info!(?digest, %endpoint, before_endpoints = ?before, after_endpoints = ?after, "DEBUG: locality_map evict_blobs for wedge digest");
                    }
                    if endpoints.is_empty() {
                        self.blobs.remove(digest);
                        if debug_digest_match(digest) {
                            info!(?digest, "DEBUG: locality_map evict_blobs removed wedge digest entry entirely (no endpoints left)");
                        }
                    }
                } else if debug_digest_match(digest) {
                    info!(?digest, %endpoint, "DEBUG: locality_map evict_blobs for wedge digest — no entry existed");
                }
            }
            if digest_set.is_empty() {
                self.endpoint_blobs.remove(endpoint);
            }
        }
    }

    /// (#locality-map-drift) LWW-gated evict for the WORKER-DELTA apply path.
    /// Each `(digest, stamp)` carries the EVICTED value's frozen `(boot_epoch,
    /// counter)`. The endpoint is removed for the digest iff the incoming
    /// ABSENT wins the LWW (`stamp > stored` OR `stamp == stored`, i.e.
    /// ABSENT ≻ PRESENT at equal ts). A STRICTLY-OLDER (stale, re-ordered)
    /// eviction of a value that has since been re-admitted is SUPPRESSED — the
    /// held blob stays PRESENT (the false-missing fix). If the endpoint wasn't
    /// present the evict is a no-op. Reverse-index + reclaim semantics mirror
    /// the ungated `evict_blobs`.
    pub fn evict_blobs_gated(&mut self, endpoint: &str, digests: &[(DigestInfo, Stamp)]) {
        let Some(digest_set) = self.endpoint_blobs.get_mut(endpoint) else {
            return;
        };
        for &(digest, stamp) in digests {
            if let Some(endpoints) = self.blobs.get_mut(&digest) {
                let removed = endpoints.evict_gated(endpoint, stamp);
                if debug_digest_match(&digest) {
                    info!(?digest, %endpoint, ?stamp, removed, remaining = endpoints.len(), "DEBUG: locality_map evict_blobs_gated for wedge digest");
                }
                if removed {
                    // Only drop from the reverse index when the LWW actually
                    // removed the endpoint — a suppressed stale evict must NOT
                    // desync the reverse index from the forward map.
                    digest_set.remove(&digest);
                    if endpoints.is_empty() {
                        self.blobs.remove(&digest);
                    }
                }
            }
        }
        if digest_set.is_empty() {
            self.endpoint_blobs.remove(endpoint);
        }
    }

    /// Remove ALL entries for an endpoint (worker disconnect).
    pub fn remove_endpoint(&mut self, endpoint: &str) {
        if let Some(digests) = self.endpoint_blobs.remove(endpoint) {
            for digest in &digests {
                if let Some(endpoints) = self.blobs.get_mut(digest) {
                    let before: Vec<String> = endpoints.iter().map(|s| s.as_ref().to_string()).collect();
                    endpoints.remove(endpoint);
                    let after: Vec<String> = endpoints.iter().map(|s| s.as_ref().to_string()).collect();
                    if debug_digest_match(digest) {
                        info!(?digest, %endpoint, before_endpoints = ?before, after_endpoints = ?after, "DEBUG: locality_map remove_endpoint touched wedge digest");
                    }
                    if endpoints.is_empty() {
                        self.blobs.remove(digest);
                        if debug_digest_match(digest) {
                            info!(?digest, "DEBUG: locality_map remove_endpoint removed wedge digest entry entirely");
                        }
                    }
                }
            }
        }
    }

    /// Returns true if any worker endpoint has the given digest.
    pub fn has_digest(&self, digest: &DigestInfo) -> bool {
        self.blobs
            .get(digest)
            .map_or(false, |endpoints| !endpoints.is_empty())
    }

    /// Look up which worker endpoints have the given digest.
    ///
    /// Returns every endpoint currently registered for the digest. Entries
    /// persist until explicit eviction (per-blob notification, full snapshot,
    /// or worker disconnect), so no staleness filter is applied here.
    pub fn lookup_workers(&self, digest: &DigestInfo) -> Vec<Arc<str>> {
        let Some(endpoints) = self.blobs.get(digest) else {
            return Vec::new();
        };
        endpoints.iter().cloned().collect()
    }

    /// Batched variant of `lookup_workers`: processes the whole slice under a
    /// single map borrow, returning one `Vec<Arc<str>>` per input digest in
    /// input order. An empty inner vec means no worker reported that digest.
    ///
    /// Hot path: a FindMissingBlobs RPC from Bazel can carry hundreds to
    /// thousands of digests; calling the per-digest variant in a loop would
    /// re-take the outer read lock once per digest. Here we walk the slice
    /// with a single borrow.
    pub fn lookup_many(&self, digests: &[DigestInfo]) -> Vec<Vec<Arc<str>>> {
        digests
            .iter()
            .map(|digest| match self.blobs.get(digest) {
                Some(endpoints) => endpoints.iter().cloned().collect(),
                None => Vec::new(),
            })
            .collect()
    }

    /// Returns the set of all known endpoints.
    pub fn all_endpoints(&self) -> Vec<Arc<str>> {
        self.endpoint_blobs.keys().cloned().collect()
    }

    /// Returns the number of tracked digests.
    pub fn digest_count(&self) -> usize {
        self.blobs.len()
    }

    /// Returns the number of tracked endpoints.
    pub fn endpoint_count(&self) -> usize {
        self.endpoint_blobs.len()
    }

    /// Raw access to the blobs map for bulk scoring.
    /// Caller must hold the read lock.
    pub fn blobs_map(&self) -> &DigestMap<EndpointList> {
        &self.blobs
    }

    /// (#58 directive-3) Snapshot the `endpoint → digests` reverse index into an
    /// owned `Vec`, for the shutdown locality-map persist. Cloning under the
    /// caller's read lock (the `WorkerApiServer` takes `locality_map.read()`)
    /// into owned data lets the caller drop the lock before the (potentially
    /// large) bincode encode runs in `spawn_blocking` — never holding the lock
    /// across `.await`.
    ///
    /// ~72 B/pair (string-encoded DigestInfo), CAPPED AT pair_count: one owned
    /// `Vec<(Arc<str>, Vec<DigestInfo>)>`, dropped after the encode. NOT a
    /// network path — this is a shutdown-local snapshot. Blob BYTES are never
    /// collected; only the (digest hash + size) index.
    #[must_use]
    pub fn snapshot_endpoint_blobs(&self) -> Vec<(Arc<str>, Vec<DigestInfo>)> {
        self.endpoint_blobs
            .iter()
            .map(|(endpoint, digests)| (endpoint.clone(), digests.iter().copied().collect()))
            .collect()
    }
}

/// (#mapgap) OBSERVABILITY-ONLY. Renders the routing-map's SIZE at scrape
/// time so a `/metrics` scrape shows "the map knows N digests across E
/// endpoints, and endpoint X holds K of them". This quantifies the
/// locality-map completeness gap: the scheduler flags input blobs
/// "missing on worker X" that X demonstrably has, so we need to see how
/// far the map's knowledge (this gauge) diverges from the workers' actual
/// FS contents (measured worker-side by the `input_server_missing_*`
/// counters on the worker `FastSlowStore`).
///
/// This is a manual `MetricsComponent` impl (not a derive) so the counts
/// are computed AT SCRAPE from the live map — never maintained as a
/// running delta on the hot `register_blobs` / `evict_blobs` path (those
/// process 500K+ digests/sec). The gauge therefore reflects every
/// register/evict by construction: it is read, not accumulated.
///
/// Composition: the field this impl backs is
/// `ApiWorkerScheduler.locality_map: Option<SharedBlobLocalityMap>` =
/// `Option<Arc<parking_lot::RwLock<BlobLocalityMap>>>`. The library's
/// `Option`, `Arc`, and `parking_lot::RwLock` `MetricsComponent` impls
/// chain to this leaf; the `parking_lot::RwLock` impl uses `try_read()`
/// (skip-on-contention → empty `Component`), so a contended map never
/// parks the tokio worker running the metrics scrape
/// (`metrics_publisher.rs` regression class). **No behavior change** — no
/// state is mutated; this only reads counts.
impl MetricsComponent for BlobLocalityMap {
    fn publish(
        &self,
        _kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // Enter a group named after the field (`locality_map`) so the two
        // headline gauges render as `<prefix>_locality_map_digest_count`
        // and `<prefix>_locality_map_endpoint_count` — matching the
        // FuncCounterWrapper/CounterWithTime manual-publish convention.
        let _enter = group!(field_metadata.name).entered();

        publish!(
            "digest_count",
            &(self.blobs.len() as u64),
            MetricKind::Counter,
            "(#mapgap) point-in-time distinct digests the routing blob_locality_map knows about across all endpoints; compare with worker-side input_server_missing_but_fast_hit_count to size the map-vs-FS completeness gap"
        );
        publish!(
            "endpoint_count",
            &(self.endpoint_blobs.len() as u64),
            MetricKind::Counter,
            "(#mapgap) point-in-time worker endpoints the routing blob_locality_map has any holdings for"
        );

        // Per-endpoint holdings: `<prefix>_locality_map_endpoints_<ep>_blob_count`.
        // Lets a scrape read the per-worker domino (endpoint X knows K
        // digests) so an under-reporting endpoint is visible against the
        // fleet. Iterates the reverse index (`endpoint → digest set`), the
        // same source `all_endpoints()`/`endpoint_count()` read — O(E)
        // endpoints, each a `HashSet::len()`, at scrape only.
        {
            let _endpoints_enter = group!("endpoints").entered();
            for (endpoint, digests) in &self.endpoint_blobs {
                let _ep_enter = group!(endpoint.as_ref()).entered();
                publish!(
                    "blob_count",
                    &(digests.len() as u64),
                    MetricKind::Counter,
                    "(#mapgap) point-in-time distinct digests this endpoint holds per the routing blob_locality_map"
                );
            }
        }

        Ok(MetricPublishKnownKindData::Component)
    }
}

/// (#58 directive-3) Magic header for the persisted locality-map file
/// (`"NLLM"`). Validated on reload so a foreign / truncated file is rejected
/// (fail-open: the reload then starts with an empty map, never crashes).
const PERSIST_MAGIC: u32 = 0x4E4C_4C4D;

/// (#58 directive-3) Persist format version. Bump on any incompatible schema
/// change; a version mismatch on reload is rejected (fail-open → empty map).
const PERSIST_VERSION: u16 = 1;

/// (#58 directive-3) One persisted endpoint: a stable `(cas_endpoint,
/// boot_epoch)` identity plus the digests it held at graceful shutdown. The
/// `boot_epoch` is the #141 reconciliation discriminator — on reload it primes
/// `endpoint_state` so a rebooted worker (different boot_epoch) has its stale
/// entries wiped by the EXISTING connect-path wipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedEndpoint {
    /// The worker's CAS endpoint string — the exact key the locality map is
    /// keyed on; stable across IP changes (it is a hostname).
    pub cas_endpoint: String,
    /// The `boot_epoch_id` observed for this endpoint at persist time.
    pub boot_epoch: u64,
    /// The digests this endpoint held at graceful shutdown.
    pub digests: Vec<DigestInfo>,
}

/// (#58 directive-3) Raw 6-byte file frame: `PERSIST_MAGIC` (u32 LE) +
/// `PERSIST_VERSION` (u16 LE). Written ahead of the bincode payload and
/// validated from the raw leading bytes BEFORE any bincode decode runs, so a
/// foreign / truncated / corrupt file is rejected up front rather than letting
/// bincode interpret a stray length prefix and attempt a giant allocation. The
/// payload (`persisted_at_unix_s` + `entries`) is bincode-encoded after this
/// frame.
const PERSIST_HEADER_LEN: usize = 6;

/// (#58 directive-3) On-disk shape of the persisted blob-locality map. Written
/// at graceful shutdown (Phase 3.5) and reloaded at startup behind the Bazel
/// readiness gate. The on-disk layout is a raw 6-byte magic+version frame
/// (validated FIRST, before any decode) followed by a bincode-encoded payload
/// (`persisted_at_unix_s` + `entries`). The magic/version are NOT bincode
/// fields — they are a manual frame so a corrupt header is rejected before
/// bincode reads any embedded length prefix.
///
/// Best-effort: persisted on graceful SIGTERM only. A SIGKILL skips the persist;
/// the map then rebuilds from worker full-snapshot `BlobsAvailable` on reconnect
/// (slower but lossless — the map is an INDEX, not a durable copy of any blob).
///
/// `Serialize`/`Deserialize` are derived directly: the bincode body is EXACTLY
/// these two fields (the magic+version are module consts written as a raw frame,
/// NOT struct fields), so there is no separate payload twin. This avoids a full
/// `entries.clone()` on the SIGTERM persist path and removes the silent-drift
/// hazard a field-identical twin would carry (add a field to one, forget the
/// other → data-loss-on-reload).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedLocalityMap {
    /// Unix seconds at persist time. Drives the never-reconnect grace TTL: a
    /// reloaded endpoint that never reconnects is swept `grace_secs` after this.
    pub persisted_at_unix_s: u64,
    /// One entry per endpoint.
    pub entries: Vec<PersistedEndpoint>,
}

/// Upper bound on the bincode-decoded body of a reloaded persist file. With
/// `bincode::config::standard()` alone (`NoLimit`), a valid 6-byte frame
/// followed by a corrupt/oversized `Vec` length prefix drives a giant
/// `Vec::with_capacity(attacker_len)` → allocator abort → startup crash-loop,
/// violating the §3.3 fail-open guarantee. `.with_limit::<N>()` makes bincode's
/// prealloc guard fire so an oversized length surfaces as
/// `DecodeError::LimitExceeded` (which `deserialize_from_bytes` maps to `Err`
/// and `reload_from_disk` maps to fail-open + empty map) BEFORE any allocation.
///
/// 4 GiB is a generous ceiling over the §2.4 fleet-scale ~200 MB encoded-size
/// estimate (~20× headroom), so a legitimate file never trips it while a corrupt
/// length always does. Applied to the DECODE side only — the encode input is
/// ours, so `serialize_to_bytes` stays unbounded `standard()`.
const PERSIST_DECODE_LIMIT_BYTES: usize = 4 * 1024 * 1024 * 1024;

impl PersistedLocalityMap {
    /// Build a persisted map.
    #[must_use]
    pub fn new(persisted_at_unix_s: u64, entries: Vec<PersistedEndpoint>) -> Self {
        Self {
            persisted_at_unix_s,
            entries,
        }
    }

    /// Encode to the on-disk bytes: a raw 6-byte magic+version frame followed by
    /// the bincode-encoded payload.
    ///
    /// Pure CPU work — the caller runs it in `spawn_blocking` when the snapshot
    /// is large so it never blocks a tokio worker.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, Error> {
        // Encode `self` directly — the bincode body is exactly
        // (`persisted_at_unix_s`, `entries`); no twin struct, no `entries`
        // clone. Encode side stays unbounded `standard()`: the input is ours.
        let encoded = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| make_input_err!("failed to bincode-encode locality persist map: {e}"))?;
        let mut out = Vec::with_capacity(PERSIST_HEADER_LEN + encoded.len());
        out.extend_from_slice(&PERSIST_MAGIC.to_le_bytes());
        out.extend_from_slice(&PERSIST_VERSION.to_le_bytes());
        out.extend_from_slice(&encoded);
        Ok(out)
    }

    /// Validate the raw magic+version frame, THEN bincode-decode the payload.
    /// Rejects a too-short file, a wrong magic, or an unsupported version with a
    /// bespoke `Error` — BEFORE bincode interprets any length prefix — so the
    /// reload path can fail-open (log + start empty) rather than mis-decode a
    /// foreign file (or attempt a giant allocation on a corrupt length).
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < PERSIST_HEADER_LEN {
            return Err(make_input_err!(
                "locality persist file too short ({} bytes, need >= {} for the magic+version frame); ignoring",
                bytes.len(),
                PERSIST_HEADER_LEN
            ));
        }
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != PERSIST_MAGIC {
            return Err(make_input_err!(
                "locality persist file has bad magic 0x{magic:08X} (expected 0x{PERSIST_MAGIC:08X}); ignoring"
            ));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != PERSIST_VERSION {
            return Err(make_input_err!(
                "locality persist file version mismatch {version} (expected {PERSIST_VERSION}); ignoring"
            ));
        }
        // BOUNDED decode (`with_limit`): a corrupt/oversized body length prefix
        // surfaces as `DecodeError::LimitExceeded` (mapped to `Err` below →
        // fail-open empty map upstream) instead of a giant `Vec::with_capacity`
        // → allocator abort → startup crash-loop. See `PERSIST_DECODE_LIMIT_BYTES`.
        let (map, _len): (Self, usize) = bincode::serde::decode_from_slice(
            &bytes[PERSIST_HEADER_LEN..],
            bincode::config::standard().with_limit::<PERSIST_DECODE_LIMIT_BYTES>(),
        )
        .map_err(|e| make_input_err!("failed to bincode-decode locality persist map payload: {e}"))?;
        Ok(map)
    }
}

/// Result of reloading the persisted locality map (design §8.2 proving test
/// assertions read these counters).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReloadedLocalitySummary {
    /// Endpoints repopulated into `locality_map` + `endpoint_state`.
    pub endpoints_loaded: usize,
    /// Total (endpoint, digest) pairs registered into `locality_map`.
    pub digests_loaded: usize,
    /// `persisted_at_unix_s` from the reloaded file (0 when no/corrupt file).
    pub persisted_at_unix_s: u64,
}

impl ReloadedLocalitySummary {
    /// A fail-open empty summary (no file / corrupt file / read error).
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            endpoints_loaded: 0,
            digests_loaded: 0,
            persisted_at_unix_s: 0,
        }
    }
}

impl Default for BlobLocalityMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe shared handle to a `BlobLocalityMap`.
pub type SharedBlobLocalityMap = Arc<RwLock<BlobLocalityMap>>;

/// Create a new shared blob locality map.
pub fn new_shared_blob_locality_map() -> SharedBlobLocalityMap {
    Arc::new(RwLock::new(BlobLocalityMap::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_lookup() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        map.register_blobs("worker-a:50081", &[d1, d2]);
        map.register_blobs("worker-b:50081", &[d1]);

        let workers = map.lookup_workers(&d1);
        assert_eq!(workers.len(), 2);
        assert!(workers.contains(&Arc::from("worker-a:50081")));
        assert!(workers.contains(&Arc::from("worker-b:50081")));

        let workers = map.lookup_workers(&d2);
        assert_eq!(workers.len(), 1);
        assert!(workers.contains(&Arc::from("worker-a:50081")));
    }

    #[test]
    fn test_evict_blobs() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        map.register_blobs("worker-a:50081", &[d1, d2]);
        map.evict_blobs("worker-a:50081", &[d1]);

        assert!(map.lookup_workers(&d1).is_empty());
        assert_eq!(map.lookup_workers(&d2).len(), 1);
    }

    #[test]
    fn test_remove_endpoint() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        map.register_blobs("worker-a:50081", &[d1, d2]);
        map.register_blobs("worker-b:50081", &[d1]);

        map.remove_endpoint("worker-a:50081");

        // d1 still available on worker-b
        let workers = map.lookup_workers(&d1);
        assert_eq!(workers.len(), 1);
        assert!(workers.contains(&Arc::from("worker-b:50081")));

        // d2 no longer available anywhere
        assert!(map.lookup_workers(&d2).is_empty());
    }

    #[test]
    fn test_lookup_unknown_digest() {
        let map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        assert!(map.lookup_workers(&d1).is_empty());
    }

    #[test]
    fn test_blobs_map_accessor() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        map.register_blobs("worker-a:50081", &[d1, d2]);
        map.register_blobs("worker-b:50081", &[d1]);

        let blobs = map.blobs_map();
        assert_eq!(blobs.len(), 2);

        // d1 has two endpoints
        let d1_endpoints = blobs.get(&d1).unwrap();
        assert_eq!(d1_endpoints.len(), 2);
        assert!(d1_endpoints.contains_key("worker-a:50081"));
        assert!(d1_endpoints.contains_key("worker-b:50081"));

        // d2 has one endpoint
        let d2_endpoints = blobs.get(&d2).unwrap();
        assert_eq!(d2_endpoints.len(), 1);
        assert!(d2_endpoints.contains_key("worker-a:50081"));
    }

    #[test]
    fn test_re_registration_is_idempotent() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);

        map.register_blobs("worker-a", &[d1]);
        map.register_blobs("worker-a", &[d1]);

        // Re-registering an existing (digest, endpoint) pair should not
        // duplicate the endpoint entry.
        let endpoints = map.blobs_map().get(&d1).unwrap();
        assert_eq!(endpoints.len(), 1);
        assert!(endpoints.contains_key("worker-a"));
    }

    #[test]
    fn test_evict_all_blobs_removes_endpoint() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        map.register_blobs("worker-a", &[d1, d2]);
        assert_eq!(map.endpoint_count(), 1);

        map.evict_blobs("worker-a", &[d1, d2]);

        assert_eq!(map.endpoint_count(), 0);
        assert_eq!(map.digest_count(), 0);
        assert!(map.lookup_workers(&d1).is_empty());
        assert!(map.lookup_workers(&d2).is_empty());
        // endpoint_blobs should be fully cleaned up
        assert!(map.all_endpoints().is_empty());
    }

    #[test]
    fn test_partial_eviction_preserves_remaining() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);
        let d3 = DigestInfo::new([3u8; 32], 300);

        map.register_blobs("worker-a", &[d1, d2, d3]);
        assert_eq!(map.digest_count(), 3);
        assert_eq!(map.endpoint_count(), 1);

        map.evict_blobs("worker-a", &[d1]);

        assert!(map.lookup_workers(&d1).is_empty());
        assert_eq!(map.lookup_workers(&d2), vec![Arc::from("worker-a")]);
        assert_eq!(map.lookup_workers(&d3), vec![Arc::from("worker-a")]);
        assert_eq!(map.digest_count(), 2);
        assert_eq!(map.endpoint_count(), 1);
    }

    #[test]
    fn test_evict_unknown_digest_is_noop() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        map.register_blobs("worker-a", &[d1]);

        // Evict a digest that was never registered — should not panic.
        map.evict_blobs("worker-a", &[d2]);

        assert_eq!(map.lookup_workers(&d1), vec![Arc::from("worker-a")]);
        assert_eq!(map.endpoint_count(), 1);
        assert_eq!(map.digest_count(), 1);
    }

    #[test]
    fn test_complex_multi_endpoint_topology() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);
        let d3 = DigestInfo::new([3u8; 32], 300);
        let d4 = DigestInfo::new([4u8; 32], 400);
        let d5 = DigestInfo::new([5u8; 32], 500);

        map.register_blobs("worker-a", &[d1, d2, d3]);
        map.register_blobs("worker-b", &[d2, d3, d4]);
        map.register_blobs("worker-c", &[d4, d5]);

        assert_eq!(map.digest_count(), 5);
        assert_eq!(map.endpoint_count(), 3);

        // D2 on both worker-a and worker-b
        let d2_workers = map.lookup_workers(&d2);
        assert_eq!(d2_workers.len(), 2);
        assert!(d2_workers.contains(&Arc::from("worker-a")));
        assert!(d2_workers.contains(&Arc::from("worker-b")));

        // Remove worker-b
        map.remove_endpoint("worker-b");

        assert_eq!(map.endpoint_count(), 2);

        // D2 still on worker-a
        let d2_workers = map.lookup_workers(&d2);
        assert_eq!(d2_workers.len(), 1);
        assert!(d2_workers.contains(&Arc::from("worker-a")));

        // D4 still on worker-c
        let d4_workers = map.lookup_workers(&d4);
        assert_eq!(d4_workers.len(), 1);
        assert!(d4_workers.contains(&Arc::from("worker-c")));

        // D3 only on worker-a now
        let d3_workers = map.lookup_workers(&d3);
        assert_eq!(d3_workers.len(), 1);
        assert!(d3_workers.contains(&Arc::from("worker-a")));

        // D1 still on worker-a, D5 still on worker-c
        assert_eq!(map.lookup_workers(&d1).len(), 1);
        assert_eq!(map.lookup_workers(&d5).len(), 1);
        assert_eq!(map.digest_count(), 5);
    }

    #[test]
    fn test_digest_count_and_endpoint_count_consistency() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);
        let d3 = DigestInfo::new([3u8; 32], 300);

        // Step 1: Empty map.
        assert_eq!(map.digest_count(), 0);
        assert_eq!(map.endpoint_count(), 0);

        // Step 2: Register d1, d2 on worker-a.
        map.register_blobs("worker-a", &[d1, d2]);
        assert_eq!(map.digest_count(), 2);
        assert_eq!(map.endpoint_count(), 1);

        // Step 3: Register d2, d3 on worker-b (d2 shared).
        map.register_blobs("worker-b", &[d2, d3]);
        assert_eq!(map.digest_count(), 3);
        assert_eq!(map.endpoint_count(), 2);

        // Step 4: Evict d1 from worker-a (d1 disappears entirely).
        map.evict_blobs("worker-a", &[d1]);
        assert_eq!(map.digest_count(), 2);
        assert_eq!(map.endpoint_count(), 2);

        // Step 5: Evict d2 from worker-a (d2 still on worker-b).
        map.evict_blobs("worker-a", &[d2]);
        assert_eq!(map.digest_count(), 2); // d2 and d3 remain
        assert_eq!(map.endpoint_count(), 1); // worker-a removed (empty)

        // Step 6: Remove worker-b entirely.
        map.remove_endpoint("worker-b");
        assert_eq!(map.digest_count(), 0);
        assert_eq!(map.endpoint_count(), 0);
    }

    #[test]
    fn test_lookup_many() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);
        let d3 = DigestInfo::new([3u8; 32], 300);

        map.register_blobs("worker-a:50081", &[d1, d2]);
        map.register_blobs("worker-b:50081", &[d2]);

        let results = map.lookup_many(&[d1, d2, d3]);
        assert_eq!(results.len(), 3);

        // d1 → just worker-a
        assert_eq!(results[0].len(), 1);
        assert!(results[0].contains(&Arc::from("worker-a:50081")));

        // d2 → both workers
        assert_eq!(results[1].len(), 2);
        assert!(results[1].contains(&Arc::from("worker-a:50081")));
        assert!(results[1].contains(&Arc::from("worker-b:50081")));

        // d3 → unknown, empty
        assert!(results[2].is_empty());
    }

    #[test]
    fn test_has_digest() {
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        map.register_blobs("worker-a:50081", &[d1]);

        assert!(map.has_digest(&d1));
        assert!(!map.has_digest(&d2));

        // After eviction, no longer present.
        map.evict_blobs("worker-a:50081", &[d1]);
        assert!(!map.has_digest(&d1));
    }

    /// (#mapgap) Render-test: the routing-map SIZE gauges must appear on the
    /// PRODUCTION `/metrics` collection path with the CORRECT counts, and
    /// must TRACK register/evict (they are read at scrape, not accumulated).
    ///
    /// Drives the SAME walk `metrics_handler` uses in prod
    /// (`metrics_publisher::render_prometheus`) against the EXACT field
    /// composition `ApiWorkerScheduler` uses — a `#[derive(MetricsComponent)]`
    /// struct with a `#[metric] locality_map: Option<SharedBlobLocalityMap>`
    /// field (`Option<Arc<parking_lot::RwLock<BlobLocalityMap>>>`). The
    /// library `Option`/`Arc`/`parking_lot::RwLock` impls chain to
    /// `BlobLocalityMap::publish`, which self-namespaces under a
    /// `group!("locality_map")` (bare `#[metric]` field → default empty
    /// derive group, so the leaf owns its namespace, matching the
    /// `CounterWithTime`/`FuncCounterWrapper` manual-publish precedent). The
    /// resulting name shape is `<prefix>_locality_map_<leaf>`, matching prod.
    ///
    /// Mutation (per CLAUDE.md TDD): comment out the `digest_count`
    /// `publish!` in `BlobLocalityMap::publish` — the first assertion
    /// red-fails with its bespoke "#mapgap: ... dark on /metrics" message.
    #[test]
    fn blob_locality_map_size_gauges_render_on_metrics_endpoint() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        // Minimal faithful composition: the field annotation + the leaf
        // impl together (the real ApiWorkerScheduler field is
        // `#[metric] locality_map: Option<SharedBlobLocalityMap>`).
        #[derive(nativelink_metric::MetricsComponent)]
        struct SchedLike {
            #[metric]
            locality_map: Option<SharedBlobLocalityMap>,
        }

        // Two endpoints, one shared digest: digest_count = 3 distinct,
        // endpoint_count = 2, worker_a holds 2 (d1,d2), worker_b holds 2
        // (d2,d3). Distinctive counts double as a wrong-field guard.
        let mut map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);
        let d3 = DigestInfo::new([3u8; 32], 300);
        map.register_blobs("worker_a", &[d1, d2]);
        map.register_blobs("worker_b", &[d2, d3]);
        assert_eq!(map.digest_count(), 3);
        assert_eq!(map.endpoint_count(), 2);

        let sched = SchedLike {
            locality_map: Some(Arc::new(RwLock::new(map))),
        };

        let registry = MetricsRegistry::new();
        registry.register("scheduler.testsched.worker", Arc::new(sched));

        // Warm the lazy span-thread-local path once (same de-flake the
        // scheduler render test uses) then take the asserted render.
        let _warm = render_prometheus(&registry);
        let body = render_prometheus(&registry);

        assert!(
            body.contains("locality_map_digest_count"),
            "#mapgap: BlobLocalityMap.digest_count dark on /metrics — the \
             routing-map size gauge did not render; the map-vs-FS completeness \
             gap is unmeasurable. body=\n{body}"
        );
        assert!(
            body.contains(
                "\nscheduler_testsched_worker_locality_map_digest_count 3\n"
            ),
            "#mapgap: locality_map digest_count rendered the wrong value \
             (expected 3 distinct digests) — group/field routing is wrong. \
             body=\n{body}"
        );
        assert!(
            body.contains(
                "\nscheduler_testsched_worker_locality_map_endpoint_count 2\n"
            ),
            "#mapgap: locality_map endpoint_count rendered the wrong value \
             (expected 2 endpoints). body=\n{body}"
        );
        // Per-endpoint domino: worker_a and worker_b each hold 2 digests.
        assert!(
            body.contains(
                "\nscheduler_testsched_worker_locality_map_endpoints_worker_a_blob_count 2\n"
            ),
            "#mapgap: per-endpoint blob_count for worker_a dark or wrong \
             (expected 2) — the per-worker domino is not scrapeable. body=\n{body}"
        );
        assert!(
            body.contains(
                "\nscheduler_testsched_worker_locality_map_endpoints_worker_b_blob_count 2\n"
            ),
            "#mapgap: per-endpoint blob_count for worker_b dark or wrong \
             (expected 2). body=\n{body}"
        );
    }

    /// (#mapgap) The size gauges must DECREASE after an eviction — proving
    /// they are read at scrape (reflect the live map) rather than a
    /// monotonic accumulator. Evicts d1 from worker_a and re-renders.
    #[test]
    fn blob_locality_map_size_gauges_track_eviction() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        #[derive(nativelink_metric::MetricsComponent)]
        struct SchedLike {
            #[metric]
            locality_map: Option<SharedBlobLocalityMap>,
        }

        let map = BlobLocalityMap::new();
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);
        let shared: SharedBlobLocalityMap = Arc::new(RwLock::new(map));
        shared.write().register_blobs("worker_a", &[d1, d2]);

        let sched = SchedLike {
            locality_map: Some(shared.clone()),
        };
        let registry = MetricsRegistry::new();
        registry.register("sched.s.worker", Arc::new(sched));

        let _warm = render_prometheus(&registry);
        let before = render_prometheus(&registry);
        assert!(
            before.contains("\nsched_s_worker_locality_map_digest_count 2\n"),
            "#mapgap: pre-eviction digest_count expected 2. body=\n{before}"
        );

        // Evict one digest; the gauge must now read 1 at scrape.
        shared.write().evict_blobs("worker_a", &[d1]);
        let after = render_prometheus(&registry);
        assert!(
            after.contains("\nsched_s_worker_locality_map_digest_count 1\n"),
            "#mapgap: post-eviction digest_count expected 1 — the gauge did \
             NOT track the evict, so it is accumulating rather than reading \
             the live map. body=\n{after}"
        );
        assert!(
            after.contains(
                "\nsched_s_worker_locality_map_endpoints_worker_a_blob_count 1\n"
            ),
            "#mapgap: post-eviction per-endpoint blob_count expected 1. \
             body=\n{after}"
        );
    }
}
