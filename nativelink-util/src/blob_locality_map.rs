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

/// Compact per-digest endpoint list. With only ~10 workers, a Vec with linear
/// scan is faster than HashMap due to:
/// - No hashing overhead for Arc<str> keys
/// - Cache-friendly sequential memory access
/// - No bucket array overhead (HashMap has 50%+ empty slots)
/// - Fewer allocations (one Vec vs HashMap's bucket array + entries)
///
/// Per-entry timestamps were dropped: with TTL filtering removed, a
/// "freshest worker" tiebreaker is theatre — any worker carrying the digest
/// is equally good. Correctness now rests on the v2 lost-eviction invariant
/// (workers eviction-broadcast before the next FindMissingBlobs can see a
/// stale Some), which superseded the bytestream-side sync-confirm safety
/// net deleted in task #155.
#[derive(Debug, Clone, Default)]
pub struct EndpointList {
    entries: Vec<Arc<str>>,
}

impl EndpointList {
    /// Insert an endpoint if not already present. Returns true if newly added.
    #[inline]
    fn insert(&mut self, endpoint: &Arc<str>) -> bool {
        for existing in &self.entries {
            if Arc::ptr_eq(existing, endpoint) || **existing == **endpoint {
                return false;
            }
        }
        self.entries.push(endpoint.clone());
        true
    }

    /// Remove an endpoint. Returns true if it was present.
    #[inline]
    fn remove(&mut self, endpoint: &str) -> bool {
        if let Some(pos) = self.entries.iter().position(|e| &**e == endpoint) {
            self.entries.swap_remove(pos);
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &Arc<str>> {
        self.entries.iter()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Arc<str>> {
        self.entries.iter()
    }

    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|e| &**e == key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the given endpoint is in the list.
    #[inline]
    pub fn get(&self, key: &str) -> Option<&Arc<str>> {
        self.entries.iter().find(|e| &***e == key)
    }
}

impl<'a> IntoIterator for &'a EndpointList {
    type Item = &'a Arc<str>;
    type IntoIter = std::slice::Iter<'a, Arc<str>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
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
            entry.insert(&ep);
            if debug_digest_match(&digest) {
                let endpoints: Vec<String> = entry.iter().map(|s| s.as_ref().to_string()).collect();
                info!(?digest, %ep, after_endpoints = ?endpoints, "DEBUG: locality_map register_blobs_iter for wedge digest");
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
    /// CAPPED AT digest_count(): one owned `Vec<(Arc<str>, Vec<DigestInfo>)>`,
    /// ~40 B/(endpoint,digest) pair, dropped after the encode. NOT a network
    /// path — this is a shutdown-local snapshot. Blob BYTES are never collected;
    /// only the (digest hash + size) index.
    #[must_use]
    pub fn snapshot_endpoint_blobs(&self) -> Vec<(Arc<str>, Vec<DigestInfo>)> {
        self.endpoint_blobs
            .iter()
            .map(|(endpoint, digests)| (endpoint.clone(), digests.iter().copied().collect()))
            .collect()
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedLocalityMap {
    /// Unix seconds at persist time. Drives the never-reconnect grace TTL: a
    /// reloaded endpoint that never reconnects is swept `grace_secs` after this.
    pub persisted_at_unix_s: u64,
    /// One entry per endpoint.
    pub entries: Vec<PersistedEndpoint>,
}

/// The bincode-serialized payload (everything after the raw magic+version
/// frame). Split out so the frame is validated from raw bytes before any
/// bincode decode touches an attacker-/corruption-controlled length prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedPayload {
    persisted_at_unix_s: u64,
    entries: Vec<PersistedEndpoint>,
}

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
        let payload = PersistedPayload {
            persisted_at_unix_s: self.persisted_at_unix_s,
            entries: self.entries.clone(),
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
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
        let (payload, _len): (PersistedPayload, usize) = bincode::serde::decode_from_slice(
            &bytes[PERSIST_HEADER_LEN..],
            bincode::config::standard(),
        )
        .map_err(|e| make_input_err!("failed to bincode-decode locality persist map payload: {e}"))?;
        Ok(Self {
            persisted_at_unix_s: payload.persisted_at_unix_s,
            entries: payload.entries,
        })
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
}
