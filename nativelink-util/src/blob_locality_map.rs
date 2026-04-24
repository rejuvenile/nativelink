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

use crate::common::DigestInfo;
use parking_lot::RwLock;

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
/// is equally good and the bytestream sync-confirm path's own `has()` is the
/// real correctness check.
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
            self.blobs
                .entry(digest)
                .or_default()
                .insert(&ep);
        }
    }

    /// Remove specific digests from the given endpoint (eviction notification).
    pub fn evict_blobs(&mut self, endpoint: &str, digests: &[DigestInfo]) {
        if let Some(digest_set) = self.endpoint_blobs.get_mut(endpoint) {
            for digest in digests {
                digest_set.remove(digest);
                if let Some(endpoints) = self.blobs.get_mut(digest) {
                    endpoints.remove(endpoint);
                    if endpoints.is_empty() {
                        self.blobs.remove(digest);
                    }
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
                    endpoints.remove(endpoint);
                    if endpoints.is_empty() {
                        self.blobs.remove(digest);
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
