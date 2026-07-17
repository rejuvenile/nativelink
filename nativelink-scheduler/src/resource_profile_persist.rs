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

//! (#task-resource-profile Phase-3 §12) Persistence for the resource-profile map so
//! learned per-key histograms survive a server restart (no re-warm tax).
//!
//! # Hard rules (reviewers BLOCK on violation)
//!
//! * **NO fsync / fdatasync / sync_file_range / msync / O_SYNC / O_DSYNC — anywhere.**
//!   The data is ADVISORY and re-accumulates; durability is NOT required (ZFS pool is
//!   `sync=disabled`). Atomicity is via `write tmp → rename` (atomic on the FS, no torn
//!   file), which needs NO fsync.
//! * **Never block a tokio worker.** The snapshot CLONES the map under the
//!   `parking_lot` lock ([`ProfileMap::snapshot_entries`]), RELEASES the lock, and only
//!   THEN serializes + writes — no lock is EVER held across `.await`/I/O. Writes go
//!   through `tokio::fs`.
//! * **Never panic on load.** A missing file / deserialize error / version or magic
//!   mismatch logs a `warn` and starts FRESH (returns an empty profile set).

use std::path::{Path, PathBuf};

use nativelink_error::{Code, Error, ResultExt, make_err};
use wincode::{SchemaRead, SchemaWrite};

use crate::resource_profile::ProfileEntrySnapshot;

/// Preallocation cap (64 MiB) for DEserialization. UNLIKE `nativelink-store` (which uses
/// `PREALLOCATION_SIZE_LIMIT_DISABLED` because it trusts its own blobs), this data is
/// read from a persisted file that may be CORRUPT — a garbage length field must NOT
/// trigger a multi-GB allocation (a capacity-overflow abort would VIOLATE the "never
/// panic on load" rule). With a bounded cap, a bogus length fails as a graceful decode
/// `Err` (the caller warns + starts fresh). 64 MiB is generous headroom over a full
/// snapshot (16384 entries × ~1 KiB ≈ 20 MiB).
const SNAPSHOT_PREALLOC_LIMIT: usize = 64 << 20;

/// wincode configuration — bincode-style fixed-int LE layout (matches `nativelink-store`)
/// but with a BOUNDED preallocation limit for untrusted on-disk data (see
/// [`SNAPSHOT_PREALLOC_LIMIT`]).
pub type WincodeConfig = wincode::config::Configuration<
    true,
    SNAPSHOT_PREALLOC_LIMIT,
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    wincode::int_encoding::FixInt,
    u32,
>;

/// Magic prefix identifying a resource-profile snapshot file ("NLRP" — NativeLink
/// Resource Profile). A file that does not start with this is treated as foreign /
/// corrupt → start fresh.
const SNAPSHOT_MAGIC: u32 = 0x4E4C_5250;

/// On-disk schema version. Bump on ANY incompatible layout change; a loaded file whose
/// version differs starts FRESH (the histograms re-accumulate; no migration needed for
/// advisory data).
const SNAPSHOT_VERSION: u32 = 1;

/// Versioned header stamped at the front of every snapshot.
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct PersistHeader {
    magic: u32,
    version: u32,
    /// Wall-clock UNIX seconds at which the snapshot was taken — used to compute the
    /// snapshot's AGE at load (for the DOWN staleness gate).
    snapshot_unix_secs: u64,
}

/// The full snapshot blob. [`ProfileEntrySnapshot`] is itself the wincode-derived entry
/// wire form (it carries `SchemaRead`/`SchemaWrite`), so it is serialized directly — no
/// separate on-disk entry type.
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct PersistSnapshot {
    header: PersistHeader,
    entries: Vec<ProfileEntrySnapshot>,
}

/// Serialize a snapshot (versioned header + entries) to bytes.
///
/// # Errors
/// Returns `Err` if wincode serialization fails.
pub fn serialize_snapshot(
    entries: Vec<ProfileEntrySnapshot>,
    snapshot_unix_secs: u64,
) -> Result<Vec<u8>, Error> {
    let snapshot = PersistSnapshot {
        header: PersistHeader {
            magic: SNAPSHOT_MAGIC,
            version: SNAPSHOT_VERSION,
            snapshot_unix_secs,
        },
        entries,
    };
    wincode::config::serialize(&snapshot, WincodeConfig::new())
        .map_err(|e| make_err!(Code::Internal, "resource-profile snapshot serialize failed: {e:?}"))
}

/// Deserialize a snapshot blob → `(snapshot_unix_secs, entries)`, validating the magic +
/// version. A mismatch / corrupt blob returns `Err` (the caller warns + starts fresh —
/// NEVER panics).
///
/// # Errors
/// Returns `Err` on a wincode failure, a bad magic, or a version mismatch.
pub fn deserialize_snapshot(bytes: &[u8]) -> Result<(u64, Vec<ProfileEntrySnapshot>), Error> {
    let snapshot: PersistSnapshot =
        wincode::config::deserialize::<PersistSnapshot, WincodeConfig>(bytes, WincodeConfig::new())
            .map_err(|e| {
                make_err!(Code::Internal, "resource-profile snapshot deserialize failed: {e:?}")
            })?;
    if snapshot.header.magic != SNAPSHOT_MAGIC {
        return Err(make_err!(
            Code::InvalidArgument,
            "resource-profile snapshot magic mismatch: got {:#x}, want {SNAPSHOT_MAGIC:#x}",
            snapshot.header.magic
        ));
    }
    if snapshot.header.version != SNAPSHOT_VERSION {
        return Err(make_err!(
            Code::InvalidArgument,
            "resource-profile snapshot version mismatch: got {}, want {SNAPSHOT_VERSION} — \
             starting fresh (advisory data re-accumulates)",
            snapshot.header.version
        ));
    }
    Ok((snapshot.header.snapshot_unix_secs, snapshot.entries))
}

/// The sibling temp path for an atomic write (`<path>.tmp`).
fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Atomically write a serialized snapshot to `path` via `write tmp → rename`. NO fsync
/// (advisory data; ZFS `sync=disabled`). The bytes are serialized by the caller OFF the
/// map lock; this fn only does the (async) I/O.
///
/// # Errors
/// Returns `Err` on a filesystem write / rename failure (best-effort — the caller logs
/// and continues; a failed snapshot is not fatal).
pub async fn write_snapshot_bytes(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let tmp = tmp_path(path);
    // No fsync anywhere: write the full buffer to the tmp file, then rename over the
    // target. rename is atomic on the FS, so a reader never sees a torn file.
    tokio::fs::write(&tmp, bytes)
        .await
        .err_tip(|| format!("writing resource-profile snapshot tmp {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .err_tip(|| format!("renaming resource-profile snapshot into place {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(target: &str, mnemonic: &str, mem_bucket: usize, count: u64) -> ProfileEntrySnapshot {
        let mut memory_hist = vec![0u32; 64];
        memory_hist[mem_bucket] = count as u32;
        ProfileEntrySnapshot {
            instance_name: "main".to_string(),
            target_id: target.to_string(),
            action_mnemonic: mnemonic.to_string(),
            memory_hist,
            cpu_hist: vec![0u32; 64],
            disk_hist: vec![0u32; 64],
            net_hist: vec![0u32; 64],
            sample_count: count,
        }
    }

    /// Round-trip: serialize → deserialize yields the SAME entries + snapshot time.
    ///
    /// MUTATION: bump `SNAPSHOT_VERSION` on the write side only (or corrupt the magic) →
    /// `deserialize_snapshot` returns Err → the `expect` here red-fails.
    #[test]
    fn snapshot_round_trips_entries_and_time() {
        let entries = vec![entry("//a:a", "CppCompile", 16, 25), entry("//b:b", "CppLink", 20, 40)];
        let bytes = serialize_snapshot(entries.clone(), 1_700_000_000).expect("serialize");
        let (secs, got) = deserialize_snapshot(&bytes).expect("round-trip must deserialize");
        assert_eq!(secs, 1_700_000_000, "the snapshot wall-time must round-trip");
        assert_eq!(got, entries, "every entry (key + histograms + count) must round-trip");
    }

    /// A corrupt blob (random bytes) deserializes to Err — the caller starts fresh, no panic.
    #[test]
    fn corrupt_blob_deserializes_to_err_not_panic() {
        let garbage = vec![0xABu8; 37];
        assert!(
            deserialize_snapshot(&garbage).is_err(),
            "a corrupt blob must return Err (→ caller warns + starts fresh), never panic"
        );
    }

    /// A VALID header followed by an OVERSIZED `entries` length prefix must fail as a
    /// graceful `Err`, NOT a capacity-overflow abort. This is the load-bearing assertion
    /// for [`SNAPSHOT_PREALLOC_LIMIT`]: wincode reads the length and runs its preallocation
    /// check (`len * size_of::<entry>() > limit`) BEFORE allocating the `Vec`, so a hostile
    /// `1 << 40` count fails the cap instead of driving a multi-terabyte `Vec` allocation
    /// that aborts the process (which would VIOLATE the "never panic on load" rule). The
    /// random-bytes test above never reaches this path — the length there is not a valid
    /// oversized count over a valid header.
    ///
    /// MUTATION: change `SNAPSHOT_PREALLOC_LIMIT` to `usize::MAX`
    /// (`wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED`) → the prealloc guard returns
    /// `None` (no check) → wincode allocates `1 << 40` entries → allocation failure →
    /// `handle_alloc_error` abort → this test can no longer observe a graceful `Err` (the
    /// test binary aborts instead of returning).
    #[test]
    fn oversized_entries_length_deserializes_to_err_not_panic() {
        // Serialize a VALID snapshot with an EMPTY entries vec so the magic + version are
        // correct and the only hostile field is the trailing length prefix. BincodeLen
        // encodes a Vec length as a u64 (FixInt LE), so for an empty vec the final 8 bytes
        // ARE that length prefix (== 0). Overwrite them with an absurd count.
        let valid = PersistSnapshot {
            header: PersistHeader {
                magic: SNAPSHOT_MAGIC,
                version: SNAPSHOT_VERSION,
                snapshot_unix_secs: 1_700_000_000,
            },
            entries: vec![],
        };
        let mut bytes = wincode::config::serialize(&valid, WincodeConfig::new()).expect("serialize");
        let n = bytes.len();
        assert!(n >= 8, "an empty snapshot still carries the 8-byte length prefix");
        // 1 << 40 entries × the per-entry size vastly exceeds the 64 MiB cap → the
        // preallocation check must fire and return Err before any allocation.
        let oversized: u64 = 1 << 40;
        bytes[n - 8..].copy_from_slice(&oversized.to_le_bytes());

        let err = deserialize_snapshot(&bytes)
            .expect_err("an oversized entries length must fail gracefully, never abort/panic");
        assert_eq!(
            err.code,
            Code::Internal,
            "the oversized-length decode must surface as a wincode Internal decode Err (the \
             SNAPSHOT_PREALLOC_LIMIT guard tripping), never a capacity-overflow process abort"
        );
    }

    /// A version-mismatched header is rejected (Err) so the caller starts fresh.
    ///
    /// MUTATION: drop the `snapshot.header.version != SNAPSHOT_VERSION` check in
    /// `deserialize_snapshot` → a future-version file would be accepted → this red-fails.
    #[test]
    fn version_mismatch_is_rejected() {
        // Hand-build a snapshot with a bumped version, serialize it, then parse.
        let snap = PersistSnapshot {
            header: PersistHeader {
                magic: SNAPSHOT_MAGIC,
                version: SNAPSHOT_VERSION + 1,
                snapshot_unix_secs: 1,
            },
            entries: vec![],
        };
        let bytes = wincode::config::serialize(&snap, WincodeConfig::new()).expect("serialize");
        let err = deserialize_snapshot(&bytes).expect_err("a version mismatch must be rejected");
        assert_eq!(
            err.code,
            Code::InvalidArgument,
            "a version mismatch must surface as InvalidArgument so the caller starts fresh"
        );
    }

    /// A bad magic (foreign file) is rejected.
    #[test]
    fn bad_magic_is_rejected() {
        let snap = PersistSnapshot {
            header: PersistHeader {
                magic: 0xDEAD_BEEF,
                version: SNAPSHOT_VERSION,
                snapshot_unix_secs: 1,
            },
            entries: vec![],
        };
        let bytes = wincode::config::serialize(&snap, WincodeConfig::new()).expect("serialize");
        assert!(
            deserialize_snapshot(&bytes).is_err(),
            "a foreign file (bad magic) must be rejected → start fresh"
        );
    }
}
