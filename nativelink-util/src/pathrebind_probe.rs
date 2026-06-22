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

// TEMP PROBE (#FL-688 path-rebind confirmation) — REVERT after capture
//
//! DIAGNOSTIC-ONLY side-table correlating the inode/mtime of the file
//! **hashed** at Phase-1 prehash (digest `D1`) with the inode/mtime of the
//! file actually **stored** into the CAS under `D1`. Confirms or refutes the
//! "hash-by-path / store-by-path, no inode pin" defect: was the output path
//! rebound to a different inode (e.g. action re-run / sandbox relink) between
//! the moment its content was hashed and the moment that path was renamed
//! into the CAS?
//!
//! ## Why a process-global side-table (not a threaded parameter)
//!
//! The two seams live in different crates and the store seam runs in a
//! detached `background_spawn!` task that does NOT receive the worker's
//! prehash map:
//!
//! - **Hash seam** — `nativelink-worker` `prehash_single_file`: the open fd
//!   that was actually hashed is `fstat`ed and recorded here keyed by `D1`.
//! - **Store seam** — `nativelink-store` `FilesystemStore::emplace_file`:
//!   after the temp→final rename into the CAS path, the final path is
//!   `stat`ed and compared against the recorded `(ino, mtime)` for `D1`.
//!
//! Threading `(ino, mtime)` through the `StoreDriver::update_with_whole_file`
//! trait signature → `FastSlowStore` → `FilesystemStore` →
//! `background_spawn!` would be a workspace-wide trait change for a probe we
//! intend to revert. Both crates already depend on `nativelink-util`, so a
//! global side-table keyed by `DigestInfo` correlates the two seams with zero
//! signature churn. This matches the existing `spawn_rate_probe` precedent.
//!
//! ## Bounding (CLAUDE.md unbounded-buffer rule)
//!
//! Not every recorded digest reaches `emplace_file`'s compare point: small
//! blobs (≤16 KiB) route to the Memory→Redis tier and never touch the
//! `FilesystemStore`; `content_is_immutable` dedup and eviction/replacement
//! both early-return before the compare. Those entries would leak, so the map
//! is a FIFO ring capped at [`CAPACITY`] — over-cap inserts evict the oldest.
//! Worst-case resident ≈ `CAPACITY` × (`DigestInfo` 40 B + `(u64,i64)` 16 B +
//! short path) ≈ a few MiB. See `// CAPPED AT` on the storage `static`.
//!
//! ## Concurrency
//!
//! All public fns are synchronous and acquire a single `parking_lot::Mutex`
//! for one map op; the lock is never held across `.await` (CLAUDE.md hard
//! rule). Per-call overhead is one mutex acquire + one hash-map op — far below
//! the syscall/rename it accompanies.

use std::collections::{HashMap, VecDeque};
use std::sync::LazyLock;

use parking_lot::Mutex;

use crate::common::DigestInfo;

/// Maximum number of in-flight hash-seam records retained. Entries are
/// removed at the store seam on a successful correlate, but leak paths exist
/// (small-blob tier, dedup skip, eviction) so the ring evicts oldest at this
/// cap. 32 Ki entries ≈ a few MiB worst case — enough to span one CI batch's
/// concurrent output uploads without dropping correlations under normal load.
// CAPPED AT 32768: FIFO ring; over-cap inserts pop_front the oldest. Bounds
// the leak from digests that never reach FilesystemStore::emplace_file
// (small-blob Memory→Redis tier, content_is_immutable dedup, eviction).
pub const CAPACITY: usize = 32_768;

/// What was captured at the hash seam for one digest.
#[derive(Debug, Clone)]
struct HashSeamRecord {
    /// `st_ino` of the open fd that was hashed at prehash.
    ino: u64,
    /// `st_mtime` whole+nanos combined into nanoseconds since epoch.
    mtime_ns: i64,
    /// Source output path that was hashed (for the log line only).
    path: String,
}

/// FIFO ring: the `HashMap` answers correlate-by-digest; the `VecDeque`
/// preserves insertion order so over-cap inserts evict the oldest digest.
struct Table {
    map: HashMap<DigestInfo, HashSeamRecord>,
    order: VecDeque<DigestInfo>,
}

static TABLE: LazyLock<Mutex<Table>> = LazyLock::new(|| {
    Mutex::new(Table {
        map: HashMap::with_capacity(CAPACITY),
        order: VecDeque::with_capacity(CAPACITY),
    })
});

/// Record, at the HASH seam, the `(ino, mtime_ns)` of the fd that produced
/// `digest`. Called from `prehash_single_file` immediately after the file is
/// hashed, while the hashed fd is still open. Synchronous; no `.await`.
///
/// On a duplicate digest the newest record overwrites the prior one (the
/// digest stays at its original ring position — overwrite is rare and the
/// probe only needs ONE valid reference inode per digest).
pub fn record_hash(digest: DigestInfo, ino: u64, mtime_ns: i64, path: &std::ffi::OsStr) {
    let record = HashSeamRecord {
        ino,
        mtime_ns,
        path: path.to_string_lossy().into_owned(),
    };
    let mut table = TABLE.lock();
    if table.map.insert(digest, record).is_none() {
        // New digest — push to the order ring and evict oldest if over cap.
        table.order.push_back(digest);
        while table.order.len() > CAPACITY {
            if let Some(old) = table.order.pop_front() {
                table.map.remove(&old);
            }
        }
    }
}

/// Outcome of a store-seam correlate, returned to the caller so it can emit
/// the LOUD log line with the store-side `(ino, mtime)` it just `stat`ed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// `st_ino` recorded at the hash seam.
    pub hash_ino: u64,
    /// `st_mtime_ns` recorded at the hash seam.
    pub hash_mtime_ns: i64,
    /// Source path that was hashed (context for the log line).
    pub hash_path: String,
}

/// At the STORE seam, look up the hash-seam record for `digest` and compare
/// against the just-`stat`ed stored `(store_ino, store_mtime_ns)`. Removes the
/// record (one digest is stored once). Synchronous; no `.await`.
///
/// Returns:
/// - `Some(Mismatch)` if a hash-seam record exists AND its inode OR mtime
///   differs from the stored file — the path-rebind signal.
/// - `None` if there was no record (digest captured on a non-worker path, or
///   already evicted) OR the stored file matches the hashed file (benign).
///
/// The caller is responsible for emitting the `error!` line on `Some` — this
/// fn does no logging so it stays a pure, unit-testable predicate.
pub fn correlate_store(digest: &DigestInfo, store_ino: u64, store_mtime_ns: i64) -> Option<Mismatch> {
    let record = {
        let mut table = TABLE.lock();
        // Remove from order too so the ring doesn't retain a dead key.
        if let Some(pos) = table.order.iter().position(|d| d == digest) {
            table.order.remove(pos);
        }
        table.map.remove(digest)
    }?;
    if record.ino != store_ino || record.mtime_ns != store_mtime_ns {
        Some(Mismatch {
            hash_ino: record.ino,
            hash_mtime_ns: record.mtime_ns,
            hash_path: record.path,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::*;

    /// The table is a process-global singleton; tests that touch it must run
    /// serially behind this lock and clear it first.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn reset() {
        let mut table = TABLE.lock();
        table.map.clear();
        table.order.clear();
    }

    fn digest(seed: u8) -> DigestInfo {
        DigestInfo::new([seed; 32], u64::from(seed))
    }

    #[test]
    fn matching_inode_and_mtime_is_benign_none() {
        let _g = TEST_LOCK.lock();
        reset();
        let d = digest(1);
        record_hash(d, 42, 1_000, OsStr::new("/work/out.o"));
        // Same ino+mtime at the store seam → no rebind → None.
        assert_eq!(correlate_store(&d, 42, 1_000), None);
    }

    #[test]
    fn different_inode_is_mismatch() {
        let _g = TEST_LOCK.lock();
        reset();
        let d = digest(2);
        record_hash(d, 42, 1_000, OsStr::new("/work/out.o"));
        let m = correlate_store(&d, 99, 1_000).expect("inode differs → must be a mismatch");
        assert_eq!(
            m,
            Mismatch {
                hash_ino: 42,
                hash_mtime_ns: 1_000,
                hash_path: "/work/out.o".to_string(),
            }
        );
    }

    #[test]
    fn different_mtime_is_mismatch() {
        let _g = TEST_LOCK.lock();
        reset();
        let d = digest(3);
        record_hash(d, 42, 1_000, OsStr::new("/work/out.o"));
        let m =
            correlate_store(&d, 42, 2_000).expect("mtime differs → must be a mismatch");
        assert_eq!(m.hash_ino, 42);
        assert_eq!(m.hash_mtime_ns, 1_000);
    }

    #[test]
    fn unknown_digest_returns_none() {
        let _g = TEST_LOCK.lock();
        reset();
        // No record_hash for this digest (e.g. small-blob tier never touched
        // FilesystemStore, or it was a non-worker write).
        assert_eq!(correlate_store(&digest(4), 1, 1), None);
    }

    #[test]
    fn correlate_consumes_the_record() {
        let _g = TEST_LOCK.lock();
        reset();
        let d = digest(5);
        record_hash(d, 42, 1_000, OsStr::new("/work/out.o"));
        // First correlate (mismatch) consumes it.
        assert!(correlate_store(&d, 7, 1_000).is_some());
        // Second correlate finds nothing.
        assert_eq!(correlate_store(&d, 7, 1_000), None);
    }

    #[test]
    fn ring_evicts_oldest_over_capacity() {
        let _g = TEST_LOCK.lock();
        reset();
        // Insert CAPACITY+2 distinct digests; the first two must be evicted.
        // Use a 4-byte little-endian counter inside the 32-byte hash so all
        // keys are distinct beyond u8 range.
        let mk = |i: u32| {
            let mut h = [0u8; 32];
            h[..4].copy_from_slice(&i.to_le_bytes());
            DigestInfo::new(h, u64::from(i))
        };
        for i in 0..(CAPACITY as u32 + 2) {
            record_hash(mk(i), u64::from(i), 0, OsStr::new("/p"));
        }
        {
            let table = TABLE.lock();
            assert_eq!(table.map.len(), CAPACITY);
            assert_eq!(table.order.len(), CAPACITY);
        }
        // Oldest two evicted → correlate finds nothing for them.
        assert_eq!(correlate_store(&mk(0), 0, 0), None);
        assert_eq!(correlate_store(&mk(1), 1, 0), None);
        // A surviving recent one still correlates (matching → None, but the
        // record existed and is now consumed).
        assert_eq!(correlate_store(&mk(CAPACITY as u32 + 1), CAPACITY as u64 + 1, 0), None);
        // Consumed: a second lookup is None regardless.
        assert_eq!(correlate_store(&mk(CAPACITY as u32 + 1), 12345, 0), None);
    }
}
