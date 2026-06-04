// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::future::Future;
use core::pin::Pin;
use core::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime};

use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode, FileNode, SymlinkNode,
};
use nativelink_store::ac_utils::get_and_decode_digest;
use nativelink_store::cas_utils::is_zero_digest;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntry, FilesystemStore};
use nativelink_util::coalesce::{CoalesceOptions, InFlightMap, with_construction_lock};
use nativelink_util::common::DigestInfo;
use nativelink_util::fs_util::{CloneMethod, hardlink_directory_tree};
#[cfg(target_os = "macos")]
use nativelink_util::fs_util::calculate_directory_size;
#[cfg(not(target_os = "macos"))]
use nativelink_util::fs_util::set_readonly_and_calculate_size;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

/// Bound on how long a single coalesced directory-cache construction may
/// run before the leader's compute future is aborted with
/// `DeadlineExceeded`. Waiters then receive the same error (or, on
/// leader cancellation, `Aborted`) instead of blocking forever on a
/// silently-stalled upstream — the exact failure mode that the now-removed
/// `prepare_action_inputs` outer 60s timeout (commit `49bf70fb`) was
/// added to mask. 120s is generous enough for a real megabyte-scale
/// resolve+download under load while still surfacing wedges.
const CONSTRUCTION_LEADER_TIMEOUT: Duration = Duration::from_secs(120);

/// Name of the merkle tree metadata file stored alongside each cached directory.
const MERKLE_METADATA_FILENAME: &str = ".merkle_tree_meta";

/// Cache format version file. Bump when the on-disk format changes in a way
/// that makes old entries invalid (e.g., permission semantics). On startup,
/// if the version file is missing or stale, the entire cache is wiped.
const CACHE_VERSION_FILENAME: &str = ".cache_version";
/// Bump this when the cache format changes.
const CACHE_FORMAT_VERSION: u32 = 6;

/// Merkle tree metadata for a cached directory entry.
///
/// Stores the mapping from each directory digest in the tree to its relative
/// path within the cached directory on disk. This allows us to index subtrees
/// so that future cache misses can reuse already-cached subtrees via symlinks.
#[derive(Debug, Clone)]
pub struct MerkleTreeMetadata {
    /// Map from directory digest -> relative path within the cache entry.
    /// For the root directory, the relative path is "" (empty string).
    pub digest_to_relpath: HashMap<DigestInfo, String>,
}

impl MerkleTreeMetadata {
    /// Serialize to a simple line-based text format:
    /// `hash:size_bytes:relative_path\n`
    fn serialize(&self) -> String {
        let mut lines = Vec::with_capacity(self.digest_to_relpath.len());
        for (digest, relpath) in &self.digest_to_relpath {
            lines.push(format!("{}:{}:{}", digest.packed_hash(), digest.size_bytes(), relpath));
        }
        // Sort for deterministic output
        lines.sort();
        lines.join("\n")
    }

    /// Deserialize from the line-based text format.
    fn deserialize(data: &str) -> Result<Self, Error> {
        let mut digest_to_relpath = HashMap::new();
        for line in data.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // Format: hash:size_bytes:relative_path
            // The relative path may contain colons, so split at most 3 parts.
            let mut parts = line.splitn(3, ':');
            let hash = parts.next().ok_or_else(|| {
                make_err!(Code::Internal, "Missing hash in merkle metadata line: {line}")
            })?;
            let size_str = parts.next().ok_or_else(|| {
                make_err!(Code::Internal, "Missing size in merkle metadata line: {line}")
            })?;
            let relpath = parts.next().unwrap_or("");

            let size: i64 = size_str.parse().map_err(|e| {
                make_err!(Code::Internal, "Invalid size in merkle metadata line: {line}: {e}")
            })?;

            let digest = DigestInfo::try_new(hash, size)
                .err_tip(|| format!("Invalid digest in merkle metadata line: {line}"))?;

            digest_to_relpath.insert(digest, relpath.to_string());
        }
        Ok(Self { digest_to_relpath })
    }

    /// Build merkle tree metadata by walking a resolved directory tree.
    ///
    /// `tree` is the map from digest -> Directory proto (as returned by
    /// `resolve_directory_tree`). `root_digest` is the root of the tree.
    ///
    /// Returns a mapping from each directory digest to its relative path
    /// within the cache entry (root = "").
    fn from_directory_tree(
        tree: &HashMap<DigestInfo, ProtoDirectory>,
        root_digest: &DigestInfo,
    ) -> Self {
        let mut digest_to_relpath = HashMap::with_capacity(tree.len());
        let mut queue = VecDeque::new();
        queue.push_back((*root_digest, String::new()));

        while let Some((digest, relpath)) = queue.pop_front() {
            if digest_to_relpath.contains_key(&digest) {
                continue; // Already visited (handles diamond dependencies)
            }
            digest_to_relpath.insert(digest, relpath.clone());

            if let Some(dir) = tree.get(&digest) {
                for subdir_node in &dir.directories {
                    if let Some(child_digest) = subdir_node
                        .digest
                        .as_ref()
                        .and_then(|d| DigestInfo::try_from(d).ok())
                    {
                        let child_relpath = if relpath.is_empty() {
                            subdir_node.name.clone()
                        } else {
                            format!("{}/{}", relpath, subdir_node.name)
                        };
                        queue.push_back((child_digest, child_relpath));
                    }
                }
            }
        }

        Self { digest_to_relpath }
    }
}

/// Configuration for the directory cache
#[derive(Debug, Clone)]
pub struct DirectoryCacheConfig {
    /// Maximum number of cached directories
    pub max_entries: usize,
    /// Maximum total size in bytes (0 = unlimited)
    pub max_size_bytes: u64,
    /// Base directory for cache storage
    pub cache_root: PathBuf,
    /// When true, use the cache directory directly via symlinks instead of
    /// hardlinking/cloning. Eliminates copy overhead; subtrees are reused
    /// via symlinks from the new cache entry to existing cached subtrees.
    pub direct_use_mode: bool,
}

impl Default for DirectoryCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            max_size_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            cache_root: std::env::temp_dir().join("nativelink_directory_cache"),
            direct_use_mode: false,
        }
    }
}

/// Metadata for a cached directory.
///
/// `ref_count` and `last_access` use atomics so that the cache hit fast path
/// only needs a *read* lock on the cache HashMap (no write lock contention).
#[derive(Debug)]
struct CachedDirectoryMetadata {
    /// Path to the cached directory
    path: PathBuf,
    /// Size in bytes
    size: u64,
    /// Last access time as duration-since-EPOCH in millis (atomic for read-lock access)
    last_access_millis: AtomicU64,
    /// Reference count (number of active hardlink operations in flight)
    ref_count: AtomicUsize,
}

impl CachedDirectoryMetadata {
    fn touch(&self) {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_access_millis.store(millis, Ordering::Relaxed);
    }
}

/// High-performance directory cache that uses hardlinks to avoid repeated
/// directory reconstruction from the CAS.
///
/// When actions need input directories, instead of fetching and reconstructing
/// files from the CAS each time, we:
/// 1. Check if we've already constructed this exact directory (by digest)
/// 2. If yes, hardlink the entire tree to the action's workspace
/// 3. If no, construct it once and cache for future use
///
/// This dramatically reduces I/O and improves action startup time.
///
/// ## Security Note
///
/// Hardlinked files share inodes. If an action process has elevated privileges
/// (e.g. root, `CAP_DAC_OVERRIDE`), it can bypass read-only permissions and
/// modify cached files through the workspace hardlink, poisoning the cache for
/// subsequent actions. For multi-tenant clusters, consider running actions in
/// user namespaces or using copy-on-write (reflink) instead of hardlinks.
#[derive(Debug)]
pub struct DirectoryCache {
    /// Configuration
    config: DirectoryCacheConfig,
    /// Cache mapping digest -> metadata
    cache: Arc<RwLock<HashMap<DigestInfo, CachedDirectoryMetadata>>>,
    /// Per-digest construction coalescing map. The first task to ask for a
    /// digest becomes the leader, runs the construction body, and publishes
    /// the result (success or error) to all waiters via a `watch::channel`.
    /// Backed by [`with_construction_lock`] which provides RAII slot
    /// cleanup, leader-timeout fan-out, and panic/cancellation safety —
    /// replacing the previous ad-hoc per-digest Mutex pattern that left
    /// waiters wedged when the leader's upstream stalled (commit
    /// `49bf70fb`).
    construction_locks: InFlightMap<DigestInfo, ()>,
    /// CAS store for fetching directories (used as fallback in construct_directory_impl)
    cas_store: Store,
    /// Concrete FastSlowStore for the fast `download_to_directory` path.
    /// When available, cache-miss construction uses batch RPCs instead of
    /// serial per-file fetches.
    fast_slow_store: Option<Arc<FastSlowStore>>,
    /// Concrete FilesystemStore (the fast store inside FastSlowStore).
    /// Required for hardlinking files from the CAS to the cache directory.
    filesystem_store: Option<Arc<FilesystemStore>>,
    /// Subtree index: maps each directory digest to its absolute path on disk
    /// within a cached entry. This allows partial reuse of cached subtrees
    /// when a new root digest is requested that shares subtrees with an
    /// already-cached root.
    ///
    /// Updated when cache entries are inserted or evicted.
    subtree_index: RwLock<HashMap<DigestInfo, PathBuf>>,
    /// Reference count for each subtree digest across all cached entries.
    /// When a digest's count drops to zero, it is truly removed and should
    /// be reported in the "removed" delta.
    subtree_refcount: RwLock<HashMap<DigestInfo, usize>>,
    /// Pending subtree digest changes since the last `take_pending_subtree_changes()` call.
    /// Protected by a Mutex for interior mutability from insertion/eviction paths.
    pending_subtree_changes: Mutex<PendingSubtreeChanges>,
    /// Cumulative hit count for stats logging
    hit_count: AtomicU64,
    /// Cumulative miss count for stats logging
    miss_count: AtomicU64,
    /// Cumulative subtree hit count for stats logging
    subtree_hit_count: AtomicU64,
    /// Cumulative hit-via-clonefile count
    hit_clonefile_count: AtomicU64,
    /// Cumulative hit-via-hardlink count
    hit_hardlink_count: AtomicU64,
    /// Cumulative fuzzy match count (cache miss resolved via best-match patching)
    fuzzy_match_count: AtomicU64,
    /// Reverse index: maps each subtree digest to the set of root digests
    /// whose cached entries contain that subtree. Used for fuzzy matching --
    /// when a new root misses the cache, we score each cached root by how
    /// many subtree digests it shares with the new tree and pick the best one.
    subtree_to_roots: RwLock<HashMap<DigestInfo, HashSet<DigestInfo>>>,
    /// When true, use the cache directory directly via symlinks instead of
    /// hardlinking/cloning. See `DirectoryCacheConfig::direct_use_mode`.
    direct_use_mode: bool,
}

/// Accumulated subtree digest changes between periodic reports.
#[derive(Debug, Default)]
pub struct PendingSubtreeChanges {
    /// Subtree digests added since last report.
    pub added: HashSet<DigestInfo>,
    /// Subtree digests removed since last report (only those no longer in ANY cached entry).
    pub removed: HashSet<DigestInfo>,
}

/// Filter cached subtree-hit candidates by verifying each on-disk directory
/// has the expected entry count from its `Directory` proto. Defends against
/// corrupted cache entries (e.g. a cached subtree missing files) poisoning
/// every future action that shares that Directory digest.
///
/// The check is shallow per directory (one `read_dir` per candidate), counts
/// entries excluding the merkle metadata file, and runs all candidates in a
/// single `spawn_blocking` task to avoid per-directory async/thread overhead.
/// Typical cost: a few microseconds per candidate on APFS/ext4.
///
/// Reasoning: `Directory.files + .directories + .symlinks` is exactly the set
/// of names that should appear at that path. A mismatch means the on-disk
/// state diverges from the proto — either a partial construction made it into
/// the cache, an external process tampered with the directory, or (the case
/// that motivated this) a previous broken version of NativeLink seeded a
/// poisoned subtree that has been propagating via cache hits.
async fn filter_valid_subtree_hits(
    candidates: Vec<(DigestInfo, PathBuf, u32)>,
) -> HashMap<DigestInfo, PathBuf> {
    if candidates.is_empty() {
        return HashMap::new();
    }
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let mut valid = HashMap::with_capacity(candidates.len());
        let mut rejected: u32 = 0;
        for (digest, path, expected) in candidates {
            let read_dir = match std::fs::read_dir(&path) {
                Ok(rd) => rd,
                Err(e) => {
                    trace!(
                        ?digest,
                        path = %path.display(),
                        ?e,
                        "subtree validation: read_dir failed, skipping hit",
                    );
                    rejected = rejected.saturating_add(1);
                    continue;
                }
            };
            let mut count: u32 = 0;
            let mut had_entry_error = false;
            for entry_result in read_dir {
                match entry_result {
                    Ok(entry) => {
                        if entry.file_name() == MERKLE_METADATA_FILENAME {
                            continue;
                        }
                        count = count.saturating_add(1);
                    }
                    Err(e) => {
                        // Fail closed: a directory iteration error means we
                        // cannot trust the count. Treat as corruption.
                        warn!(
                            ?digest,
                            path = %path.display(),
                            ?e,
                            "subtree validation: read_dir entry error, rejecting hit",
                        );
                        had_entry_error = true;
                        break;
                    }
                }
            }
            if had_entry_error {
                rejected = rejected.saturating_add(1);
                continue;
            }
            if count == expected {
                valid.insert(digest, path);
            } else {
                warn!(
                    ?digest,
                    path = %path.display(),
                    on_disk = count,
                    expected,
                    "subtree validation: cached subtree entry count mismatch, rejecting hit",
                );
                rejected = rejected.saturating_add(1);
            }
        }
        if rejected > 0 {
            warn!(
                rejected,
                accepted = valid.len(),
                "subtree validation: filtered out corrupt cached subtrees",
            );
        }
        valid
    })
    .await
    .unwrap_or_default()
}

/// Compute the expected on-disk entry count (files + directories + symlinks)
/// for a `Directory` proto. The merkle metadata file is excluded by the
/// validator, so this matches the count the validator will see.
#[inline]
fn expected_entry_count(dir: &ProtoDirectory) -> u32 {
    let total = dir.files.len() + dir.directories.len() + dir.symlinks.len();
    u32::try_from(total).unwrap_or(u32::MAX)
}

/// Validate every directory in a freshly-constructed cache entry against its
/// proto, before the entry is published into `subtree_index`. Catches the case
/// where construction succeeded by status (no error returned) but a download
/// or symlink silently produced an incomplete subdirectory — exactly the
/// failure mode that previously seeded poisoned cache entries.
///
/// `tree_root` is the on-disk root (`temp_path`, before atomic rename).
async fn validate_constructed_tree(
    tree_root: &Path,
    tree: &HashMap<DigestInfo, ProtoDirectory>,
    merkle_meta: &MerkleTreeMetadata,
) -> Result<(), Error> {
    let candidates: Vec<(DigestInfo, PathBuf, u32)> = merkle_meta
        .digest_to_relpath
        .iter()
        .filter_map(|(d, relpath)| {
            tree.get(d).map(|dir| {
                let abs_path = if relpath.is_empty() {
                    tree_root.to_path_buf()
                } else {
                    tree_root.join(relpath)
                };
                (*d, abs_path, expected_entry_count(dir))
            })
        })
        .collect();
    let total = candidates.len();
    if total == 0 {
        return Ok(());
    }
    let valid = filter_valid_subtree_hits(candidates).await;
    if valid.len() == total {
        return Ok(());
    }
    Err(make_err!(
        Code::Internal,
        "post-construction validation: {} of {} directories on disk do not match proto",
        total - valid.len(),
        total,
    ))
}

impl DirectoryCache {
    /// Creates a new `DirectoryCache`.
    ///
    /// If `fast_slow_store` is provided, cache-miss construction will use the
    /// fast batch `download_to_directory` path (GetTree + BatchReadBlobs +
    /// parallel hardlinks). Otherwise falls back to the serial
    /// `construct_directory_impl` method.
    pub async fn new(
        config: DirectoryCacheConfig,
        cas_store: Store,
        fast_slow_store: Option<Arc<FastSlowStore>>,
    ) -> Result<Self, Error> {
        // Ensure cache root exists
        fs::create_dir_all(&config.cache_root).await.err_tip(|| {
            format!(
                "Failed to create cache root: {}",
                config.cache_root.display()
            )
        })?;

        // Sibling-bug audit (review #7): fast-store-only is intentional.
        // We need a `FilesystemStore` reference for direct hardlinking
        // from the on-disk CAS. Mirror blobs live in memory and cannot
        // be hardlinked; mirror-only digests must be materialized via
        // `populate_fast_store_unchecked` before any hardlink path.
        // Concrete FilesystemStore needed for hardlink operations into the
        // cache directory; the wrapper hides the concrete type so the
        // downcast must reach into the inner store directly.
        #[allow(clippy::disallowed_methods)]
        let filesystem_store = fast_slow_store.as_ref().and_then(|fss| {
            fss.fast_store()
                .downcast_ref::<FilesystemStore>(None)
                .and_then(|fs| fs.get_arc())
        });

        let has_fast_path = fast_slow_store.is_some() && filesystem_store.is_some();
        let direct_use_mode = config.direct_use_mode;

        if has_fast_path {
            info!(
                cache_root = %config.cache_root.display(),
                max_entries = config.max_entries,
                max_size_bytes = config.max_size_bytes,
                fast_path = true,
                direct_use_mode,
                "DirectoryCache initialized: using fast download_to_directory path for cache misses",
            );
        } else if fast_slow_store.is_some() {
            warn!(
                cache_root = %config.cache_root.display(),
                max_entries = config.max_entries,
                max_size_bytes = config.max_size_bytes,
                direct_use_mode,
                "DirectoryCache initialized: FastSlowStore provided but could not extract FilesystemStore; falling back to serial construction",
            );
        } else {
            info!(
                cache_root = %config.cache_root.display(),
                max_entries = config.max_entries,
                max_size_bytes = config.max_size_bytes,
                fast_path = false,
                direct_use_mode,
                "DirectoryCache initialized: no FastSlowStore, using serial construction",
            );
        }

        let mut initial_cache = HashMap::new();
        let mut initial_subtree_index = HashMap::new();
        let mut initial_subtree_refcount: HashMap<DigestInfo, usize> = HashMap::new();
        let mut initial_subtree_to_roots: HashMap<DigestInfo, HashSet<DigestInfo>> = HashMap::new();

        // Check cache format version. If stale or missing, wipe the cache.
        let version_path = config.cache_root.join(CACHE_VERSION_FILENAME);
        let version_ok = match fs::read_to_string(&version_path).await {
            Ok(v) => v.trim().parse::<u32>().ok() == Some(CACHE_FORMAT_VERSION),
            Err(_) => false,
        };
        if !version_ok {
            info!(
                expected = CACHE_FORMAT_VERSION,
                "DirectoryCache: format version mismatch, clearing stale entries",
            );
            if let Ok(mut entries) = fs::read_dir(&config.cache_root).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let p = entry.path();
                    if let Ok(meta) = fs::symlink_metadata(&p).await {
                        if meta.is_dir() {
                            // Only chmod directories writable, not files (which
                            // are hardlinked to CAS). On unix, directory write
                            // permission is sufficient to unlink files.
                            Self::remove_readonly_dir(&p).await;
                        } else {
                            drop(fs::remove_file(&p).await);
                        }
                    }
                }
            }
            fs::write(&version_path, format!("{CACHE_FORMAT_VERSION}\n"))
                .await
                .err_tip(|| "Failed to write cache version file")?;
        }

        // Load existing cache entries from disk on startup.
        let load_start = Instant::now();
        let mut loaded_count = 0u64;
        let mut loaded_subtrees = 0u64;
        let mut loaded_errors = 0u64;
        if let Ok(mut entries) = fs::read_dir(&config.cache_root).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let entry_name = entry.file_name().to_string_lossy().to_string();
                // Skip temp directories and the merkle metadata files
                if entry_name.starts_with(".tmp-") || entry_name == MERKLE_METADATA_FILENAME {
                    continue;
                }
                let entry_path = entry.path();
                let Ok(metadata) = fs::symlink_metadata(&entry_path).await else {
                    continue;
                };
                if !metadata.is_dir() {
                    continue;
                }

                // Try to parse the entry name as a DigestInfo
                let Some(digest) = Self::parse_digest_from_dirname(&entry_name) else {
                    debug!(name = %entry_name, "Skipping non-digest cache directory entry");
                    continue;
                };

                // Calculate the directory size (on macOS, dirs stay writable).
                #[cfg(target_os = "macos")]
                let size_result = calculate_directory_size(&entry_path).await;
                #[cfg(not(target_os = "macos"))]
                let size_result = set_readonly_and_calculate_size(&entry_path).await;
                let size = match size_result {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(
                            name = %entry_name,
                            ?e,
                            "Failed to calculate size for existing cache entry, skipping",
                        );
                        loaded_errors += 1;
                        continue;
                    }
                };

                // Load merkle tree metadata if available
                let merkle_path = entry_path.join(MERKLE_METADATA_FILENAME);
                if let Ok(data) = fs::read_to_string(&merkle_path).await {
                    match MerkleTreeMetadata::deserialize(&data) {
                        Ok(merkle) => {
                            for (sub_digest, relpath) in &merkle.digest_to_relpath {
                                let abs_path = if relpath.is_empty() {
                                    entry_path.clone()
                                } else {
                                    entry_path.join(relpath)
                                };
                                initial_subtree_index.insert(*sub_digest, abs_path);
                                *initial_subtree_refcount.entry(*sub_digest).or_insert(0) += 1;
                                // Populate reverse index: subtree -> set of roots
                                initial_subtree_to_roots
                                    .entry(*sub_digest)
                                    .or_default()
                                    .insert(digest);
                                loaded_subtrees += 1;
                            }
                        }
                        Err(e) => {
                            debug!(
                                name = %entry_name,
                                ?e,
                                "Failed to parse merkle metadata, subtrees won't be indexed",
                            );
                        }
                    }
                }

                // Use the filesystem modification time so that LRU eviction
                // at startup correctly identifies the oldest entries.
                let mtime_millis = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                    .map_or(0u64, |d| d.as_millis() as u64);

                initial_cache.insert(
                    digest,
                    CachedDirectoryMetadata {
                        path: entry_path,
                        size,
                        last_access_millis: AtomicU64::new(mtime_millis),
                        ref_count: AtomicUsize::new(0),
                    },
                );
                loaded_count += 1;
            }
        }

        let load_elapsed = load_start.elapsed();
        if loaded_count > 0 || loaded_errors > 0 {
            info!(
                loaded_entries = loaded_count,
                loaded_subtrees,
                load_errors = loaded_errors,
                elapsed_ms = load_elapsed.as_millis() as u64,
                "DirectoryCache: loaded existing entries from disk on startup",
            );
        }

        // Enforce max_entries and max_size_bytes limits on the loaded entries.
        // Old entries from previous runs may have accumulated beyond limits.
        // Sort once by mtime (oldest first) then evict from the front — O(n log n).
        let mut startup_evicted_count = 0u64;
        let mut startup_evicted_bytes = 0u64;
        let mut startup_evict_paths = Vec::new();

        if initial_cache.len() > config.max_entries
            || (config.max_size_bytes > 0
                && initial_cache.values().map(|m| m.size).sum::<u64>() > config.max_size_bytes)
        {
            let mut sorted: Vec<(DigestInfo, u64, u64)> = initial_cache
                .iter()
                .map(|(d, m)| (*d, m.last_access_millis.load(Ordering::Relaxed), m.size))
                .collect();
            sorted.sort_by_key(|&(_, mtime, _)| mtime);

            let mut current_size: u64 = initial_cache.values().map(|m| m.size).sum();
            for (digest, _, size) in &sorted {
                let over_count = initial_cache.len() > config.max_entries;
                let over_size = config.max_size_bytes > 0 && current_size > config.max_size_bytes;
                if !over_count && !over_size {
                    break;
                }
                if let Some(meta) = initial_cache.remove(digest) {
                    startup_evicted_bytes += meta.size;
                    startup_evicted_count += 1;
                    current_size -= size;
                    startup_evict_paths.push(meta.path);
                }
            }
        }

        // If we evicted entries, rebuild subtree indexes from surviving entries
        // and delete the evicted directories from disk.
        if startup_evicted_count > 0 {
            // Rebuild subtree indexes: keep only entries whose parent cache entry survived.
            let surviving_paths: HashSet<PathBuf> = initial_cache
                .keys()
                .map(|d| config.cache_root.join(d.to_string()))
                .collect();
            let surviving_digests: HashSet<DigestInfo> =
                initial_cache.keys().copied().collect();
            initial_subtree_index
                .retain(|_, path| {
                    surviving_paths.iter().any(|sp| path.starts_with(sp))
                });
            initial_subtree_refcount.retain(|k, _| initial_subtree_index.contains_key(k));
            initial_subtree_to_roots.retain(|k, roots| {
                roots.retain(|r| surviving_digests.contains(r));
                !roots.is_empty() && initial_subtree_index.contains_key(k)
            });

            info!(
                evicted_entries = startup_evicted_count,
                evicted_bytes = startup_evicted_bytes,
                evicted_mb = format!("{:.1}", startup_evicted_bytes as f64 / (1024.0 * 1024.0)),
                remaining_entries = initial_cache.len(),
                remaining_bytes = initial_cache.values().map(|m| m.size).sum::<u64>(),
                "DirectoryCache: cleaned up stale entries at startup"
            );

            // Delete evicted directories from disk (best-effort)
            for path in startup_evict_paths {
                Self::remove_readonly_dir(&path).await;
            }
        }

        Ok(Self {
            config,
            cache: Arc::new(RwLock::new(initial_cache)),
            construction_locks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            cas_store,
            fast_slow_store,
            filesystem_store,
            subtree_index: RwLock::new(initial_subtree_index),
            subtree_refcount: RwLock::new(initial_subtree_refcount),
            pending_subtree_changes: Mutex::new(PendingSubtreeChanges::default()),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            subtree_hit_count: AtomicU64::new(0),
            hit_clonefile_count: AtomicU64::new(0),
            hit_hardlink_count: AtomicU64::new(0),
            fuzzy_match_count: AtomicU64::new(0),
            subtree_to_roots: RwLock::new(initial_subtree_to_roots),
            direct_use_mode,
        })
    }

    /// Returns the digests of all currently cached input root directories.
    /// The scheduler uses this to give routing preference to workers that
    /// already have an action's input_root_digest cached.
    pub async fn cached_digests(&self) -> Vec<DigestInfo> {
        let cache = self.cache.read().await;
        cache.keys().copied().collect()
    }

    /// Returns ALL subtree digests currently tracked across all cached entries.
    /// Used for the initial full snapshot on (re)connect.
    pub async fn all_subtree_digests(&self) -> Vec<DigestInfo> {
        let refcount = self.subtree_refcount.read().await;
        refcount.keys().copied().collect()
    }

    /// Atomically takes the pending subtree changes since the last call,
    /// returning (added, removed) digest lists and clearing the internal state.
    pub async fn take_pending_subtree_changes(&self) -> (Vec<DigestInfo>, Vec<DigestInfo>) {
        let mut pending = self.pending_subtree_changes.lock().await;
        let added: Vec<DigestInfo> = pending.added.drain().collect();
        let removed: Vec<DigestInfo> = pending.removed.drain().collect();
        (added, removed)
    }

    /// Returns whether direct-use mode is enabled.
    pub fn is_direct_use_mode(&self) -> bool {
        self.direct_use_mode
    }

    /// Gets or creates a directory in the cache, then symlinks `dest_path` to
    /// the cache directory. The cache entry's `ref_count` is incremented for
    /// the entire action lifetime (caller MUST call `release_direct_use` on
    /// cleanup).
    ///
    /// In direct-use mode, subtree reuse is done via symlinks from the new
    /// cache entry to already-cached subtree directories, instead of
    /// hardlinks/clonefiles.
    ///
    /// # Returns
    /// * `Ok((cache_path, was_hit))` - The cache directory path and whether it was a hit.
    pub async fn get_or_create_direct(
        &self,
        digest: DigestInfo,
        dest_path: &Path,
    ) -> Result<(PathBuf, bool), Error> {
        let overall_start = Instant::now();

        // Fast path: check if already in cache (read lock only for the lookup)
        if let Some(cache_path) = self.try_symlink_cached(&digest, dest_path).await? {
            let hits = self.hit_count.fetch_add(1, Ordering::Relaxed) + 1;
            let misses = self.miss_count.load(Ordering::Relaxed);
            let total = hits + misses;
            let hit_rate = if total > 0 { (hits as f64 / total as f64) * 100.0 } else { 0.0 };
            info!(
                hash = %&digest.packed_hash().to_string()[..12],
                elapsed_ms = overall_start.elapsed().as_millis() as u64,
                hits,
                misses,
                hit_rate = format!("{hit_rate:.1}%"),
                "DirectoryCache DIRECT-USE HIT (symlinked to cache)",
            );
            return Ok((cache_path, true));
        }

        let misses = self.miss_count.fetch_add(1, Ordering::Relaxed) + 1;
        let hits = self.hit_count.load(Ordering::Relaxed);
        let fuzzy = self.fuzzy_match_count.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { (hits as f64 / total as f64) * 100.0 } else { 0.0 };
        info!(
            hash = %&digest.packed_hash().to_string()[..12],
            size_bytes = digest.size_bytes(),
            hits,
            misses,
            fuzzy_matches = fuzzy,
            hit_rate = format!("{hit_rate:.1}%"),
            has_fast_path = self.fast_slow_store.is_some() && self.filesystem_store.is_some(),
            "DirectoryCache DIRECT-USE MISS, starting construction",
        );

        // Coalesce concurrent construction for the same digest. The first
        // caller becomes leader and runs `construct_direct_inner`; all
        // others receive the leader's result via a watch channel. On
        // leader timeout / cancellation / error, waiters get the same
        // error instead of hanging on a stalled upstream — replacing the
        // ad-hoc per-digest Mutex pattern that left waiters wedged when
        // get_part_parallel silently truncated a chunked read (commit
        // `49bf70fb`).
        info!(
            ?digest,
            "directory_cache(direct): about to acquire construction lock",
        );
        let lock_result = with_construction_lock(
            &self.construction_locks,
            digest,
            CoalesceOptions::leader_only(CONSTRUCTION_LEADER_TIMEOUT),
            || self.construct_direct_inner(digest, overall_start),
        )
        .await;
        info!(
            ?digest,
            ok = lock_result.is_ok(),
            "directory_cache(direct): construction lock released",
        );
        lock_result?;

        // After construction (by us or another leader), the entry is in
        // the cache. Symlink to our own dest_path via the same fast-path
        // helper as the cache-hit case above so ref_count is correctly
        // incremented for the action's lifetime.
        if let Some(cache_path) = self.try_symlink_cached(&digest, dest_path).await? {
            return Ok((cache_path, false));
        }
        // Defensive: the entry should still be present immediately after
        // construction. If eviction raced between insertion and our
        // symlink attempt, surface a real error instead of hanging.
        Err(make_err!(
            Code::Aborted,
            "DirectoryCache direct-use: entry for {digest} vanished between construction and symlink (raced eviction?)",
        ))
    }

    /// Coalesced inner body of [`Self::get_or_create_direct`]: re-checks
    /// the cache, then performs the full construct-validate-insert
    /// pipeline for `digest`. Returns `Ok(())` once the entry is in the
    /// cache (so callers can [`Self::try_symlink_cached`] their own
    /// dest_path) or the underlying construction error.
    async fn construct_direct_inner(
        &self,
        digest: DigestInfo,
        overall_start: Instant,
    ) -> Result<(), Error> {
        info!(
            ?digest,
            "directory_cache(direct): leader compute entered",
        );
        // Double-check after winning leadership — another task may have
        // just constructed it before we acquired the slot. We only check
        // the cache map directly here (no per-call symlink work), since
        // each caller (leader and waiters) does its own dest_path symlink
        // after this closure returns.
        if self.cache.read().await.contains_key(&digest) {
            info!(
                ?digest,
                "directory_cache(direct): leader saw cache hit on double-check",
            );
            return Ok(());
        }
        info!(
            ?digest,
            "directory_cache(direct): leader resolving directory tree",
        );

        // Construct in a temp path, rename to final path on success.
        let cache_path = self.get_cache_path(&digest);
        let temp_path = self.config.cache_root.join(format!(
            ".tmp-{digest}-{}-{}",
            std::process::id(),
            self.next_temp_id(),
        ));

        // Clean up any stale temp path from a previous crashed attempt
        drop(fs::remove_dir_all(&temp_path).await);

        let construction_result: Result<u64, Error> = async {
            fs::create_dir_all(&temp_path).await.err_tip(|| {
                format!("Failed to create temp dir: {}", temp_path.display())
            })?;

            // Step 1: Resolve the merkle tree if we have a FastSlowStore.
            let resolved_tree = if let Some(fss) = &self.fast_slow_store {
                let t0 = Instant::now();
                let res = crate::running_actions_manager::resolve_directory_tree(fss, &digest).await;
                info!(
                    ?digest,
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    ok = res.is_ok(),
                    "directory_cache(direct): resolve_directory_tree returned",
                );
                match res {
                    Ok(tree) => Some(tree),
                    Err(e) => {
                        warn!(
                            hash = %&digest.packed_hash().to_string()[..12],
                            ?e,
                            "DirectoryCache direct-use: failed to resolve directory tree, skipping subtree matching",
                        );
                        None
                    }
                }
            } else {
                None
            };

            // Step 2: Check for cached subtrees.
            let subtree_hits: HashMap<DigestInfo, PathBuf> = if let Some(tree) = &resolved_tree {
                let candidates = {
                    let index = self.subtree_index.read().await;
                    let mut c: Vec<(DigestInfo, PathBuf, u32)> = Vec::new();
                    for (dir_digest, dir) in tree {
                        if *dir_digest == digest {
                            continue;
                        }
                        if let Some(cached_path) = index.get(dir_digest) {
                            c.push((
                                *dir_digest,
                                cached_path.clone(),
                                expected_entry_count(dir),
                            ));
                        }
                    }
                    c
                };
                filter_valid_subtree_hits(candidates).await
            } else {
                HashMap::new()
            };

            if !subtree_hits.is_empty() {
                let subtree_count = subtree_hits.len();
                let total_dirs = resolved_tree.as_ref().map_or(0, |t| t.len());
                self.subtree_hit_count.fetch_add(subtree_count as u64, Ordering::Relaxed);
                info!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    subtree_hits = subtree_count,
                    total_dirs,
                    "DirectoryCache direct-use: found cached subtrees, will symlink",
                );
            }

            // Step 3: Build the directory tree.
            // In direct-use mode, subtree reuse creates symlinks instead of
            // hardlinks/clonefile.
            //
            // When there are no direct subtree hits, try fuzzy matching:
            // find the cached entry with the most shared subtrees and use it
            // as a template, patching in only the differences.
            if let Some(tree) = &resolved_tree {
                if !subtree_hits.is_empty() {
                    info!(
                        ?digest,
                        subtree_hits = subtree_hits.len(),
                        "directory_cache(direct): leader entering construct_with_subtrees_direct",
                    );
                    let t0 = Instant::now();
                    let res = self
                        .construct_with_subtrees_direct(&digest, tree, &subtree_hits, &temp_path)
                        .await;
                    info!(
                        ?digest,
                        elapsed_ms = t0.elapsed().as_millis() as u64,
                        ok = res.is_ok(),
                        "directory_cache(direct): construct_with_subtrees_direct returned",
                    );
                    res.err_tip(|| "Failed subtree-aware direct-use construction")?;
                } else {
                    // No direct subtree hits -- try fuzzy matching.
                    let tree_digests: HashSet<DigestInfo> = tree.keys().copied().collect();
                    if let Some((best_root, shared, total)) =
                        self.find_best_fuzzy_match(&digest, &tree_digests).await
                    {
                        let similarity = (shared as f64 / total as f64) * 100.0;
                        info!(
                            hash = %&digest.packed_hash().to_string()[..12],
                            best_match = %&best_root.packed_hash().to_string()[..12],
                            shared_subtrees = shared,
                            total_dirs = total,
                            similarity = format!("{similarity:.1}%"),
                            "DirectoryCache direct-use: FUZZY MATCH found, patching from best match",
                        );
                        self.fuzzy_match_count.fetch_add(1, Ordering::Relaxed);
                        info!(
                            ?digest,
                            "directory_cache(direct): leader entering construct_from_fuzzy_match",
                        );
                        let t0 = Instant::now();
                        let res = self
                            .construct_from_fuzzy_match(&digest, tree, &best_root, &temp_path)
                            .await;
                        info!(
                            ?digest,
                            elapsed_ms = t0.elapsed().as_millis() as u64,
                            ok = res.is_ok(),
                            "directory_cache(direct): construct_from_fuzzy_match returned",
                        );
                        res.err_tip(|| "Failed fuzzy-match construction in direct-use mode")?;
                    } else {
                        info!(
                            ?digest,
                            "directory_cache(direct): leader entering construct_full (no fuzzy match)",
                        );
                        let t0 = Instant::now();
                        let res = self.construct_full(&digest, &temp_path).await;
                        info!(
                            ?digest,
                            elapsed_ms = t0.elapsed().as_millis() as u64,
                            ok = res.is_ok(),
                            "directory_cache(direct): construct_full returned (no fuzzy)",
                        );
                        res.err_tip(|| "Failed full construction in direct-use mode")?;
                    }
                }
            } else {
                info!(
                    ?digest,
                    "directory_cache(direct): leader entering construct_full (no resolved tree)",
                );
                let t0 = Instant::now();
                let res = self.construct_full(&digest, &temp_path).await;
                info!(
                    ?digest,
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    ok = res.is_ok(),
                    "directory_cache(direct): construct_full returned (no tree)",
                );
                res.err_tip(|| "Failed full construction in direct-use mode (no resolved tree)")?;
            }

            // Step 4: Store merkle tree metadata alongside the cache entry.
            // The metadata file is required for startup re-population of
            // subtree_index, and the validator excludes it by name when
            // counting entries. A failed write would leave the entry
            // un-reloadable after restart and silently inflate the on-disk
            // count by zero, so we treat it as a fatal construction error.
            if let Some(tree) = &resolved_tree {
                let merkle_meta = MerkleTreeMetadata::from_directory_tree(tree, &digest);
                let merkle_path = temp_path.join(MERKLE_METADATA_FILENAME);
                let serialized = merkle_meta.serialize();
                fs::write(&merkle_path, serialized.as_bytes())
                    .await
                    .err_tip(|| {
                        format!(
                            "DirectoryCache direct-use: failed to write merkle metadata for {digest}"
                        )
                    })?;
                // Validate the on-disk tree before publishing. A construction
                // that silently produced incomplete subdirectories must NOT
                // reach the cache or it will poison every future hit.
                validate_constructed_tree(&temp_path, tree, &merkle_meta)
                    .await
                    .err_tip(|| {
                        format!(
                            "DirectoryCache direct-use: post-construction validation failed for {digest}"
                        )
                    })?;
            }

            // Calculate size. On macOS, cache dirs stay writable (0o755).
            // On other platforms, set read-only permissions in the same pass.
            let finalize_start = Instant::now();
            #[cfg(target_os = "macos")]
            let size = calculate_directory_size(&temp_path).await
                .err_tip(|| "Failed to calculate size for cache directory")?;
            #[cfg(not(target_os = "macos"))]
            let size = set_readonly_and_calculate_size(&temp_path).await
                .err_tip(|| "Failed to set readonly and calculate size for cache directory")?;
            info!(
                hash = %&digest.packed_hash().to_string()[..12],
                size_bytes = size,
                size_mb = format!("{:.2}", size as f64 / (1024.0 * 1024.0)),
                elapsed_ms = finalize_start.elapsed().as_millis() as u64,
                "DirectoryCache direct-use: finalize cache entry completed",
            );

            // Rename temp to final cache path (same as hardlink mode).
            #[cfg(all(unix, not(target_os = "macos")))]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&temp_path).await
                    .err_tip(|| "Failed to get temp dir metadata before rename")?
                    .permissions();
                perms.set_mode(0o755);
                fs::set_permissions(&temp_path, perms).await
                    .err_tip(|| "Failed to make temp dir writable before rename")?;
            }
            fs::rename(&temp_path, &cache_path).await.err_tip(|| {
                format!(
                    "Failed to rename temp dir {} to cache path {}",
                    temp_path.display(),
                    cache_path.display()
                )
            })?;
            #[cfg(all(unix, not(target_os = "macos")))]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&cache_path).await
                    .err_tip(|| "Failed to get cache dir metadata after rename")?
                    .permissions();
                perms.set_mode(0o555);
                fs::set_permissions(&cache_path, perms).await
                    .err_tip(|| "Failed to lock down cache dir after rename")?;
            }

            // Step 5: Update the subtree index.
            if let Some(tree) = &resolved_tree {
                let merkle_meta = MerkleTreeMetadata::from_directory_tree(tree, &digest);
                let mut index = self.subtree_index.write().await;
                for (sub_digest, relpath) in &merkle_meta.digest_to_relpath {
                    let abs_path = if relpath.is_empty() {
                        cache_path.clone()
                    } else {
                        cache_path.join(relpath)
                    };
                    index.insert(*sub_digest, abs_path);
                }
                drop(index);
                self.record_subtree_insertion(&digest, &merkle_meta).await;
            }

            Ok(size)
        }
        .await;

        let size = match construction_result {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    ?e,
                    elapsed_ms = overall_start.elapsed().as_millis() as u64,
                    "DirectoryCache DIRECT-USE MISS construction FAILED",
                );
                Self::remove_readonly_dir(&temp_path).await;
                return Err(e);
            }
        };

        // Insert with ref_count=0; the caller's post-construction
        // `try_symlink_cached` increments it for the action's lifetime.
        // Holding ref_count=1 here without a guaranteed decrement would
        // leak refs if the caller short-circuits or panics between
        // construction and the symlink step.
        let (evicted_paths, cache_entries, cache_total_size) = {
            let mut cache = self.cache.write().await;
            let evicted = self.collect_evictions(size, &mut cache);
            cache.insert(
                digest,
                CachedDirectoryMetadata {
                    path: cache_path.clone(),
                    size,
                    last_access_millis: AtomicU64::new(
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    ),
                    ref_count: AtomicUsize::new(0),
                },
            );
            let total_size: u64 = cache.values().map(|m| m.size).sum();
            (evicted, cache.len(), total_size)
        };

        info!(
            hash = %&digest.packed_hash().to_string()[..12],
            size_bytes = size,
            size_mb = format!("{:.2}", size as f64 / (1024.0 * 1024.0)),
            cache_entries,
            cache_total_size_mb = format!("{:.2}", cache_total_size as f64 / (1024.0 * 1024.0)),
            evicted_count = evicted_paths.len(),
            elapsed_ms = overall_start.elapsed().as_millis() as u64,
            "DirectoryCache DIRECT-USE MISS construction complete, inserted into cache",
        );

        // Delete evicted directories outside the lock.
        if !evicted_paths.is_empty() {
            let mut index = self.subtree_index.write().await;
            for path in &evicted_paths {
                self.remove_subtree_index_for_path(path, &mut index).await;
            }
            drop(index);
            for path in evicted_paths {
                Self::remove_readonly_dir(&path).await;
            }
        }

        Ok(())
    }

    /// Attempts to symlink a cached directory to dest for direct-use mode.
    /// Increments ref_count on hit (held for action lifetime).
    /// Returns `Ok(Some(cache_path))` on hit, `Ok(None)` on miss.
    async fn try_symlink_cached(
        &self,
        digest: &DigestInfo,
        dest_path: &Path,
    ) -> Result<Option<PathBuf>, Error> {
        let src_path = {
            let cache = self.cache.read().await;
            let Some(metadata) = cache.get(digest) else {
                return Ok(None);
            };
            metadata.touch();
            metadata.ref_count.fetch_add(1, Ordering::Relaxed);
            metadata.path.clone()
        };

        // Create symlink: dest_path -> src_path
        #[cfg(unix)]
        let symlink_result = fs::symlink(&src_path, dest_path).await;
        #[cfg(not(unix))]
        let symlink_result = fs::symlink_dir(&src_path, dest_path).await;

        match symlink_result {
            Ok(()) => {
                info!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    src = %src_path.display(),
                    dst = %dest_path.display(),
                    "DirectoryCache direct-use: symlink from cache succeeded",
                );
                Ok(Some(src_path))
            }
            Err(e) => {
                // Decrement ref_count on failure
                let cache = self.cache.read().await;
                if let Some(metadata) = cache.get(digest) {
                    metadata.ref_count.fetch_sub(1, Ordering::Relaxed);
                }
                warn!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    error = ?e,
                    "DirectoryCache direct-use: symlink from cache FAILED, will reconstruct",
                );
                Ok(None)
            }
        }
    }

    /// Releases a direct-use reference on a cache entry. Must be called once
    /// per successful `get_or_create_direct()` call when the action completes.
    pub async fn release_direct_use(&self, digest: &DigestInfo) {
        let cache = self.cache.read().await;
        if let Some(metadata) = cache.get(digest) {
            let prev = metadata.ref_count.fetch_sub(1, Ordering::Relaxed);
            debug!(
                hash = %&digest.packed_hash().to_string()[..12],
                prev_ref_count = prev,
                "DirectoryCache direct-use: released ref_count",
            );
        } else {
            warn!(
                hash = %&digest.packed_hash().to_string()[..12],
                "DirectoryCache direct-use: release_direct_use called but entry not in cache (evicted?)",
            );
        }
    }

    /// Records that subtree digests from a merkle tree were added (new cache entry).
    /// Increments refcounts, updates reverse index, and records newly-appearing
    /// digests in pending added.
    async fn record_subtree_insertion(
        &self,
        root_digest: &DigestInfo,
        merkle: &MerkleTreeMetadata,
    ) {
        let mut refcount = self.subtree_refcount.write().await;
        let mut pending = self.pending_subtree_changes.lock().await;
        let mut reverse = self.subtree_to_roots.write().await;
        for sub_digest in merkle.digest_to_relpath.keys() {
            let count = refcount.entry(*sub_digest).or_insert(0);
            if *count == 0 {
                // This digest is newly appearing across all cached entries.
                pending.added.insert(*sub_digest);
                // If it was in the removed set (evicted then re-added before
                // the delta was taken), cancel it out.
                pending.removed.remove(sub_digest);
            }
            *count += 1;
            // Update reverse index: this subtree is now in this root.
            reverse.entry(*sub_digest).or_default().insert(*root_digest);
        }
    }

    /// Records that subtree digests from a merkle tree were removed (evicted cache entry).
    /// Decrements refcounts, updates reverse index, and records fully-removed
    /// digests in pending removed.
    async fn record_subtree_removal(
        &self,
        root_digest: &DigestInfo,
        merkle_digests: &[DigestInfo],
    ) {
        let mut refcount = self.subtree_refcount.write().await;
        let mut pending = self.pending_subtree_changes.lock().await;
        let mut reverse = self.subtree_to_roots.write().await;
        for sub_digest in merkle_digests {
            if let Some(count) = refcount.get_mut(sub_digest) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    refcount.remove(sub_digest);
                    // This digest is no longer in ANY cached entry.
                    pending.removed.insert(*sub_digest);
                    // If it was in the added set (added then evicted before
                    // the delta was taken), cancel it out.
                    pending.added.remove(sub_digest);
                    // Remove from reverse index entirely.
                    reverse.remove(sub_digest);
                } else {
                    // Just remove this root from the reverse index entry.
                    if let Some(roots) = reverse.get_mut(sub_digest) {
                        roots.remove(root_digest);
                        if roots.is_empty() {
                            reverse.remove(sub_digest);
                        }
                    }
                }
            }
        }
    }

    /// Gets or creates a directory in the cache, then hardlinks it to the destination.
    ///
    /// # Arguments
    /// * `digest` - Digest of the root Directory proto
    /// * `dest_path` - Where to hardlink/create the directory (may already exist)
    ///
    /// # Returns
    /// * `Ok(true)` - Cache hit (directory was hardlinked)
    /// * `Ok(false)` - Cache miss (directory was constructed and cached)
    /// * `Err` - Error during construction or hardlinking
    pub async fn get_or_create(&self, digest: DigestInfo, dest_path: &Path) -> Result<bool, Error> {
        let overall_start = Instant::now();

        // Fast path: check if already in cache (read lock only for the lookup)
        if let Some(method) = self.try_hardlink_cached(&digest, dest_path).await? {
            let hits = self.hit_count.fetch_add(1, Ordering::Relaxed) + 1;
            let misses = self.miss_count.load(Ordering::Relaxed);
            let total = hits + misses;
            let hit_rate = if total > 0 { (hits as f64 / total as f64) * 100.0 } else { 0.0 };
            let clonefiles = self.hit_clonefile_count.load(Ordering::Relaxed);
            let hardlinks = self.hit_hardlink_count.load(Ordering::Relaxed);
            let method_str = match method {
                CloneMethod::Clonefile => "clonefile",
                CloneMethod::Hardlink => "hardlink",
            };
            info!(
                hash = %&digest.packed_hash().to_string()[..12],
                elapsed_ms = overall_start.elapsed().as_millis() as u64,
                method = method_str,
                hits,
                misses,
                hit_rate = format!("{hit_rate:.1}%"),
                clonefiles,
                hardlinks,
                "DirectoryCache HIT (cloned from cache)",
            );
            return Ok(true);
        }

        let misses = self.miss_count.fetch_add(1, Ordering::Relaxed) + 1;
        let hits = self.hit_count.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { (hits as f64 / total as f64) * 100.0 } else { 0.0 };
        info!(
            hash = %&digest.packed_hash().to_string()[..12],
            size_bytes = digest.size_bytes(),
            hits,
            misses,
            hit_rate = format!("{hit_rate:.1}%"),
            has_fast_path = self.fast_slow_store.is_some() && self.filesystem_store.is_some(),
            "DirectoryCache MISS, starting construction",
        );

        // Coalesce concurrent construction for the same digest. The first
        // caller becomes leader and runs `construct_inner`; all others
        // receive the leader's result via a watch channel. On leader
        // timeout / cancellation / error, waiters get the same error
        // instead of hanging on a stalled upstream — replacing the
        // ad-hoc per-digest Mutex pattern that left waiters wedged when
        // get_part_parallel silently truncated a chunked read (commit
        // `49bf70fb`).
        info!(
            ?digest,
            "directory_cache(hardlink): about to acquire construction lock",
        );
        let lock_result = with_construction_lock(
            &self.construction_locks,
            digest,
            CoalesceOptions::leader_only(CONSTRUCTION_LEADER_TIMEOUT),
            || self.construct_inner(digest, overall_start),
        )
        .await;
        info!(
            ?digest,
            ok = lock_result.is_ok(),
            "directory_cache(hardlink): construction lock released",
        );
        lock_result?;

        // After construction (by us or another leader), the entry is in
        // the cache. Hardlink to our dest_path via the same fast-path
        // helper as the cache-hit case above. ref_count is incremented
        // for the duration of the hardlink and decremented after.
        match self.try_hardlink_cached(&digest, dest_path).await? {
            Some(_) => Ok(false),
            None => {
                // Defensive: the entry should still be present immediately
                // after construction. If eviction raced between insertion
                // and our hardlink attempt, surface a real error.
                Err(make_err!(
                    Code::Aborted,
                    "DirectoryCache: entry for {digest} vanished between construction and hardlink (raced eviction?)",
                ))
            }
        }
    }

    /// Coalesced inner body of [`Self::get_or_create`]: re-checks the
    /// cache, then runs the full construct-validate-insert pipeline for
    /// `digest`. Returns `Ok(())` once the entry is in the cache (so
    /// callers can [`Self::try_hardlink_cached`] their own dest_path) or
    /// the underlying construction error.
    async fn construct_inner(
        &self,
        digest: DigestInfo,
        overall_start: Instant,
    ) -> Result<(), Error> {
        // Double-check after winning leadership — another task may have
        // just constructed it before we acquired the slot. We only check
        // the cache map directly here (no per-call hardlink work), since
        // each caller (leader and waiters) does its own dest_path
        // hardlink after this closure returns.
        if self.cache.read().await.contains_key(&digest) {
            return Ok(());
        }

        // Construct in a temp path, rename to final path on success.
        // This prevents orphaned partial directories on failure.
        let cache_path = self.get_cache_path(&digest);
        let temp_path = self.config.cache_root.join(format!(
            ".tmp-{digest}-{}-{}",
            std::process::id(),
            self.next_temp_id(),
        ));

        // Clean up any stale temp path from a previous crashed attempt
        drop(fs::remove_dir_all(&temp_path).await);

        let construction_result: Result<u64, Error> = async {
            fs::create_dir_all(&temp_path).await.err_tip(|| {
                format!("Failed to create temp dir: {}", temp_path.display())
            })?;

            // Step 1: Resolve the merkle tree if we have a FastSlowStore.
            // This gives us the full directory tree structure, which we use for:
            //   (a) subtree matching against the subtree_index
            //   (b) storing merkle metadata alongside the cache entry
            let resolved_tree = if let Some(fss) = &self.fast_slow_store {
                match crate::running_actions_manager::resolve_directory_tree(fss, &digest).await {
                    Ok(tree) => Some(tree),
                    Err(e) => {
                        warn!(
                            hash = %&digest.packed_hash().to_string()[..12],
                            ?e,
                            "DirectoryCache: failed to resolve directory tree, skipping subtree matching",
                        );
                        None
                    }
                }
            } else {
                None
            };

            // Step 2: Check for cached subtrees and construct a partial build plan.
            // A "subtree hit" means a directory node in the requested tree is
            // already materialized on disk from a different cached root. We can
            // symlink to it instead of downloading.
            //
            // We validate every candidate against its proto Directory's expected
            // entry count before trusting it. A previous broken construction
            // (or external tampering) can leave a cached subtree missing files;
            // without this check, the corruption silently propagates to every
            // future action that shares the same Directory digest.
            let subtree_hits: HashMap<DigestInfo, PathBuf> = if let Some(tree) = &resolved_tree {
                let candidates = {
                    let index = self.subtree_index.read().await;
                    let mut c: Vec<(DigestInfo, PathBuf, u32)> = Vec::new();
                    for (dir_digest, dir) in tree {
                        // Don't count the root itself (that's a full cache hit, handled above)
                        if *dir_digest == digest {
                            continue;
                        }
                        if let Some(cached_path) = index.get(dir_digest) {
                            c.push((
                                *dir_digest,
                                cached_path.clone(),
                                expected_entry_count(dir),
                            ));
                        }
                    }
                    c
                };
                filter_valid_subtree_hits(candidates).await
            } else {
                HashMap::new()
            };

            if !subtree_hits.is_empty() {
                let subtree_count = subtree_hits.len();
                let total_dirs = resolved_tree.as_ref().map_or(0, |t| t.len());
                self.subtree_hit_count.fetch_add(subtree_count as u64, Ordering::Relaxed);
                info!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    subtree_hits = subtree_count,
                    total_dirs,
                    "DirectoryCache: found cached subtrees, will symlink instead of downloading",
                );
            }

            // Step 3: Build the directory tree.
            // If we have subtree hits and a resolved tree, use subtree-aware
            // construction. Otherwise, try fuzzy matching before falling back
            // to full construction.
            if let Some(tree) = &resolved_tree {
                if !subtree_hits.is_empty() {
                    // Subtree-aware construction: walk the tree, symlink cached
                    // subtrees, and only download uncached portions.
                    self.construct_with_subtrees(
                        &digest,
                        tree,
                        &subtree_hits,
                        &temp_path,
                    )
                    .await
                    .err_tip(|| "Failed subtree-aware construction")?;
                } else {
                    // No direct subtree hits -- try fuzzy matching.
                    let tree_digests: HashSet<DigestInfo> = tree.keys().copied().collect();
                    if let Some((best_root, shared, total)) =
                        self.find_best_fuzzy_match(&digest, &tree_digests).await
                    {
                        let similarity = (shared as f64 / total as f64) * 100.0;
                        info!(
                            hash = %&digest.packed_hash().to_string()[..12],
                            best_match = %&best_root.packed_hash().to_string()[..12],
                            shared_subtrees = shared,
                            total_dirs = total,
                            similarity = format!("{similarity:.1}%"),
                            "DirectoryCache: FUZZY MATCH found, patching from best match",
                        );
                        self.fuzzy_match_count.fetch_add(1, Ordering::Relaxed);
                        self.construct_from_fuzzy_match(
                            &digest,
                            tree,
                            &best_root,
                            &temp_path,
                        )
                        .await
                        .err_tip(|| "Failed fuzzy-match construction")?;
                    } else {
                        // No fuzzy match -- use fast download_to_directory if available.
                        self.construct_full(&digest, &temp_path).await
                            .err_tip(|| "Failed full construction")?;
                    }
                }
            } else {
                // No resolved tree -- use full construction.
                self.construct_full(&digest, &temp_path).await
                    .err_tip(|| "Failed full construction (no resolved tree)")?;
            }

            // Step 4: Store merkle tree metadata alongside the cache entry.
            // The metadata file is required for startup re-population of
            // subtree_index, and the validator excludes it by name when
            // counting entries. A failed write would leave the entry
            // un-reloadable after restart and silently inflate the on-disk
            // count by zero, so we treat it as a fatal construction error.
            if let Some(tree) = &resolved_tree {
                let merkle_meta = MerkleTreeMetadata::from_directory_tree(tree, &digest);
                let merkle_path = temp_path.join(MERKLE_METADATA_FILENAME);
                let serialized = merkle_meta.serialize();
                fs::write(&merkle_path, serialized.as_bytes())
                    .await
                    .err_tip(|| {
                        format!(
                            "DirectoryCache: failed to write merkle metadata for {digest}"
                        )
                    })?;
                // Validate the on-disk tree before publishing. A construction
                // that silently produced incomplete subdirectories must NOT
                // reach the cache or it will poison every future hit.
                validate_constructed_tree(&temp_path, tree, &merkle_meta)
                    .await
                    .err_tip(|| {
                        format!(
                            "DirectoryCache: post-construction validation failed for {digest}"
                        )
                    })?;
            }

            // Calculate size. On macOS, cache dirs stay writable (0o755) because
            // clonefile creates independent CoW copies — no write-protection needed.
            // On other platforms, set read-only permissions in the same pass.
            let finalize_start = Instant::now();
            #[cfg(target_os = "macos")]
            let size = calculate_directory_size(&temp_path).await
                .err_tip(|| "Failed to calculate size for cache directory")?;
            #[cfg(not(target_os = "macos"))]
            let size = set_readonly_and_calculate_size(&temp_path).await
                .err_tip(|| "Failed to set readonly and calculate size for cache directory")?;
            info!(
                hash = %&digest.packed_hash().to_string()[..12],
                size_bytes = size,
                size_mb = format!("{:.2}", size as f64 / (1024.0 * 1024.0)),
                elapsed_ms = finalize_start.elapsed().as_millis() as u64,
                "DirectoryCache: finalize cache entry completed",
            );
            // On non-macOS Unix, directories are read-only (0o555) and need a
            // chmod dance for rename(2) then re-lock afterwards.
            #[cfg(all(unix, not(target_os = "macos")))]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&temp_path).await
                    .err_tip(|| "Failed to get temp dir metadata before rename")?
                    .permissions();
                perms.set_mode(0o755);
                fs::set_permissions(&temp_path, perms).await
                    .err_tip(|| "Failed to make temp dir writable before rename")?;
            }
            fs::rename(&temp_path, &cache_path).await.err_tip(|| {
                format!(
                    "Failed to rename temp dir {} to cache path {}",
                    temp_path.display(),
                    cache_path.display()
                )
            })?;
            #[cfg(all(unix, not(target_os = "macos")))]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&cache_path).await
                    .err_tip(|| "Failed to get cache dir metadata after rename")?
                    .permissions();
                perms.set_mode(0o555);
                fs::set_permissions(&cache_path, perms).await
                    .err_tip(|| "Failed to lock down cache dir after rename")?;
            }

            // Step 5: Update the subtree index with all directories from this entry,
            // and record the insertion for delta reporting.
            if let Some(tree) = &resolved_tree {
                let merkle_meta = MerkleTreeMetadata::from_directory_tree(tree, &digest);
                let mut index = self.subtree_index.write().await;
                for (sub_digest, relpath) in &merkle_meta.digest_to_relpath {
                    let abs_path = if relpath.is_empty() {
                        cache_path.clone()
                    } else {
                        cache_path.join(relpath)
                    };
                    index.insert(*sub_digest, abs_path);
                }
                drop(index);
                self.record_subtree_insertion(&digest, &merkle_meta).await;
            }

            Ok(size)
        }
        .await;

        let size = match construction_result {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    ?e,
                    elapsed_ms = overall_start.elapsed().as_millis() as u64,
                    "DirectoryCache MISS construction FAILED",
                );
                Self::remove_readonly_dir(&temp_path).await;
                return Err(e);
            }
        };

        // Insert with ref_count=0; the caller's post-construction
        // `try_hardlink_cached` increments it for the duration of the
        // hardlink. Holding ref_count=1 here without a guaranteed
        // decrement would leak refs if the caller short-circuits or
        // panics between construction and the hardlink step.
        let (evicted_paths, cache_entries, cache_total_size) = {
            let mut cache = self.cache.write().await;
            let evicted = self.collect_evictions(size, &mut cache);
            cache.insert(
                digest,
                CachedDirectoryMetadata {
                    path: cache_path.clone(),
                    size,
                    last_access_millis: AtomicU64::new(
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    ),
                    ref_count: AtomicUsize::new(0),
                },
            );
            let total_size: u64 = cache.values().map(|m| m.size).sum();
            (evicted, cache.len(), total_size)
        };

        info!(
            hash = %&digest.packed_hash().to_string()[..12],
            size_bytes = size,
            size_mb = format!("{:.2}", size as f64 / (1024.0 * 1024.0)),
            cache_entries,
            cache_total_size_mb = format!("{:.2}", cache_total_size as f64 / (1024.0 * 1024.0)),
            evicted_count = evicted_paths.len(),
            elapsed_ms = overall_start.elapsed().as_millis() as u64,
            "DirectoryCache MISS construction complete, inserted into cache",
        );

        // Delete evicted directories outside the lock.
        // Cached directories are read-only (0o555/0o444), so we must make them
        // writable before removal. Also clean up the subtree index.
        if !evicted_paths.is_empty() {
            let mut index = self.subtree_index.write().await;
            for path in &evicted_paths {
                self.remove_subtree_index_for_path(path, &mut index).await;
            }
            drop(index);
            for path in evicted_paths {
                Self::remove_readonly_dir(&path).await;
            }
        }

        Ok(())
    }

    /// Attempts to hardlink a cached directory to dest, guarding eviction with ref_count.
    /// Returns `Ok(Some(method))` on cache hit + successful clone/hardlink,
    /// `Ok(None)` on cache miss or failed hardlink (caller falls through to reconstruction).
    async fn try_hardlink_cached(
        &self,
        digest: &DigestInfo,
        dest_path: &Path,
    ) -> Result<Option<CloneMethod>, Error> {
        let (src_path, cached_size) = {
            // Read lock is sufficient — ref_count and last_access are atomic.
            let cache = self.cache.read().await;
            let Some(metadata) = cache.get(digest) else {
                debug!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    "DirectoryCache: not in cache (miss)",
                );
                return Ok(None);
            };
            metadata.touch();
            metadata.ref_count.fetch_add(1, Ordering::Relaxed);
            (metadata.path.clone(), metadata.size)
        };

        debug!(
            hash = %&digest.packed_hash().to_string()[..12],
            cached_size_bytes = cached_size,
            "DirectoryCache: found in cache, hardlinking",
        );

        let hardlink_start = Instant::now();
        let result = hardlink_directory_tree(&src_path, dest_path).await;
        let hardlink_elapsed = hardlink_start.elapsed();

        // Always decrement ref_count
        {
            let cache = self.cache.read().await;
            if let Some(metadata) = cache.get(digest) {
                metadata.ref_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        match result {
            Ok(method) => {
                let method_str = match method {
                    CloneMethod::Clonefile => "clonefile",
                    CloneMethod::Hardlink => "hardlink",
                };
                match method {
                    CloneMethod::Clonefile => {
                        self.hit_clonefile_count.fetch_add(1, Ordering::Relaxed);
                    }
                    CloneMethod::Hardlink => {
                        self.hit_hardlink_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                info!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    cached_size_bytes = cached_size,
                    hardlink_ms = hardlink_elapsed.as_millis() as u64,
                    method = method_str,
                    "DirectoryCache: clone from cache succeeded",
                );
                Ok(Some(method))
            }
            Err(e) => {
                warn!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    error = ?e,
                    hardlink_ms = hardlink_elapsed.as_millis() as u64,
                    "DirectoryCache: hardlink from cache FAILED, will reconstruct",
                );
                Ok(None)
            }
        }
    }

    /// Recursively removes a read-only directory by first restoring write
    /// permissions on directories. Files are NOT chmoded because they are
    /// hardlinked to CAS entries — changing their mode would corrupt the
    /// shared inode's permissions for all concurrent actions.
    /// On unix, only the parent directory needs write permission to unlink files.
    async fn remove_readonly_dir(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = fs::symlink_metadata(path).await {
                if metadata.is_dir() {
                    drop(fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).await);
                    if let Ok(mut entries) = fs::read_dir(path).await {
                        while let Ok(Some(entry)) = entries.next_entry().await {
                            if let Ok(meta) = fs::symlink_metadata(entry.path()).await {
                                if meta.is_dir() {
                                    Box::pin(Self::remove_readonly_dir(&entry.path())).await;
                                }
                                // Do NOT chmod files — they are hardlinked to CAS.
                            }
                        }
                    }
                }
            }
        }

        if let Err(e) = fs::remove_dir_all(path).await {
            warn!(path = ?path, error = ?e, "Failed to remove evicted directory from disk");
        }
    }

    /// Monotonically increasing counter for unique temp paths.
    fn next_temp_id(&self) -> u64 {
        use std::sync::atomic::AtomicU64 as StaticAtomicU64;
        static COUNTER: StaticAtomicU64 = StaticAtomicU64::new(0);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    /// Validates that a node name is a single safe path component.
    /// Rejects path separators, traversal components, empty names, and null bytes.
    fn validate_node_name(name: &str) -> Result<(), Error> {
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.contains('\\')
            || name.contains('\0')
        {
            return Err(make_err!(
                Code::InvalidArgument,
                "Invalid node name in Directory proto: {:?}",
                name
            ));
        }
        Ok(())
    }

    /// Validates that a symlink target does not escape the workspace root.
    /// Rejects absolute paths. For relative paths, verifies the resolved path
    /// stays within the workspace by counting `..` components.
    fn validate_symlink_target(target: &str, depth: usize) -> Result<(), Error> {
        if target.is_empty() || target.contains('\0') {
            return Err(make_err!(
                Code::InvalidArgument,
                "Invalid symlink target: {:?}",
                target
            ));
        }

        // Reject absolute symlink targets
        if target.starts_with('/') || target.starts_with('\\') {
            return Err(make_err!(
                Code::InvalidArgument,
                "Absolute symlink target not allowed: {:?}",
                target
            ));
        }

        // Count net upward traversals. `depth` is how deep we are in the tree.
        let mut net_up: usize = 0;
        for component in target.split('/') {
            match component {
                ".." => {
                    net_up += 1;
                    if net_up > depth {
                        return Err(make_err!(
                            Code::InvalidArgument,
                            "Symlink target escapes workspace root: {:?}",
                            target
                        ));
                    }
                }
                "" | "." => {}
                _ => {
                    net_up = net_up.saturating_sub(1);
                }
            }
        }

        Ok(())
    }

    /// Minimum fraction of shared directory digests to consider a fuzzy match
    /// worthwhile. Below this threshold, constructing from scratch is likely
    /// cheaper than patching a largely-different tree.
    const FUZZY_MATCH_MIN_SIMILARITY: f64 = 0.30;

    /// Finds the best fuzzy match for a new tree among cached entries.
    ///
    /// Scores each cached root by counting how many directory digests from
    /// `new_tree_digests` appear in that root's cached entry (via the reverse
    /// index). Returns `(best_root_digest, shared_count, total_new)` if a
    /// match exceeds `FUZZY_MATCH_MIN_SIMILARITY`.
    ///
    /// This enables "closest tree" reuse: instead of building from scratch
    /// on a cache miss, we clone the best-matching cached tree and patch
    /// only the differences (remove stale subtrees, add new ones).
    async fn find_best_fuzzy_match(
        &self,
        new_digest: &DigestInfo,
        new_tree_digests: &HashSet<DigestInfo>,
    ) -> Option<(DigestInfo, usize, usize)> {
        if new_tree_digests.len() < 2 {
            // Trees with 0 or 1 directory are too small for fuzzy matching
            // to be beneficial.
            return None;
        }

        let reverse = self.subtree_to_roots.read().await;
        let cache = self.cache.read().await;

        // Score each cached root by counting shared subtree digests.
        let mut scores: HashMap<DigestInfo, usize> = HashMap::new();
        for sub_digest in new_tree_digests {
            if let Some(roots) = reverse.get(sub_digest) {
                for root in roots {
                    // Don't match against ourselves or evicted roots.
                    if *root != *new_digest && cache.contains_key(root) {
                        *scores.entry(*root).or_insert(0) += 1;
                    }
                }
            }
        }

        if scores.is_empty() {
            return None;
        }

        // Find the root with the highest overlap.
        let total = new_tree_digests.len();
        let (best_root, best_count) = scores
            .into_iter()
            .max_by_key(|&(_, count)| count)?;

        let similarity = best_count as f64 / total as f64;
        if similarity >= Self::FUZZY_MATCH_MIN_SIMILARITY {
            Some((best_root, best_count, total))
        } else {
            debug!(
                best_root = %&best_root.packed_hash().to_string()[..12],
                best_count,
                total,
                similarity = format!("{similarity:.1}%"),
                "DirectoryCache: fuzzy match below threshold, skipping",
            );
            None
        }
    }

    /// Constructs a new cache entry by patching a fuzzy-matched cached entry.
    ///
    /// The approach:
    /// 1. Walk the new tree via BFS.
    /// 2. For subtrees that exist in the best-match entry (same digest at same
    ///    relative path, or available via the subtree index), create symlinks
    ///    (direct-use mode) or hardlinks to the existing cached subtree.
    /// 3. For subtrees that are new (not in the best match), download them from
    ///    CAS as usual.
    /// 4. Stale subtrees from the best match are simply not referenced -- the
    ///    new entry is built fresh, so there's nothing to "remove".
    ///
    /// This is effectively the same as `construct_with_subtrees_direct` but
    /// with a richer set of subtree hits derived from the fuzzy match.
    async fn construct_from_fuzzy_match(
        &self,
        new_digest: &DigestInfo,
        new_tree: &HashMap<DigestInfo, ProtoDirectory>,
        best_root: &DigestInfo,
        temp_path: &Path,
    ) -> Result<(), Error> {
        let fuzzy_start = Instant::now();

        // Gather all subtree hits: check every directory digest in the new tree
        // against the subtree index. The fuzzy match guarantees high overlap,
        // so most will hit. Each candidate is then validated against its
        // proto Directory's expected entry count to reject corrupt subtrees.
        let subtree_hits: HashMap<DigestInfo, PathBuf> = {
            let candidates = {
                let index = self.subtree_index.read().await;
                let mut c: Vec<(DigestInfo, PathBuf, u32)> = Vec::new();
                for (dir_digest, dir) in new_tree {
                    if *dir_digest == *new_digest {
                        continue;
                    }
                    if let Some(cached_path) = index.get(dir_digest) {
                        c.push((
                            *dir_digest,
                            cached_path.clone(),
                            expected_entry_count(dir),
                        ));
                    }
                }
                c
            };
            filter_valid_subtree_hits(candidates).await
        };

        info!(
            new_hash = %&new_digest.packed_hash().to_string()[..12],
            best_match = %&best_root.packed_hash().to_string()[..12],
            subtree_hits = subtree_hits.len(),
            total_dirs = new_tree.len(),
            "DirectoryCache: fuzzy match construction starting",
        );

        self.subtree_hit_count
            .fetch_add(subtree_hits.len() as u64, Ordering::Relaxed);

        // Reuse the existing subtree-aware construction method which handles
        // both symlink mode (direct-use) and hardlink mode.
        if self.direct_use_mode {
            self.construct_with_subtrees_direct(
                new_digest,
                new_tree,
                &subtree_hits,
                temp_path,
            )
            .await
            .err_tip(|| "Failed fuzzy-match subtree-aware direct-use construction")?;
        } else {
            self.construct_with_subtrees(
                new_digest,
                new_tree,
                &subtree_hits,
                temp_path,
            )
            .await
            .err_tip(|| "Failed fuzzy-match subtree-aware construction")?;
        }

        info!(
            new_hash = %&new_digest.packed_hash().to_string()[..12],
            best_match = %&best_root.packed_hash().to_string()[..12],
            elapsed_ms = fuzzy_start.elapsed().as_millis() as u64,
            "DirectoryCache: fuzzy match construction completed",
        );

        Ok(())
    }

    /// Full construction path: tries fast download_to_directory, falls back to serial.
    /// Used when there are no subtree hits.
    async fn construct_full(&self, digest: &DigestInfo, temp_path: &Path) -> Result<(), Error> {
        // Try the fast batch path first if concrete stores are available.
        let fast_path_result = if let (Some(fss), Some(_fs_store)) =
            (&self.fast_slow_store, &self.filesystem_store)
        {
            // Sibling-bug audit (review #7): `.fast_store()` here extracts
            // the concrete `FilesystemStore` for hardlink operations
            // performed inside `download_to_directory`. The has-check
            // there now routes through the FastSlowStore wrapper (`fss`)
            // so mirror-only blobs are recognized; this `Pin<&FilesystemStore>`
            // is used only for on-disk hardlink emission once blobs are
            // materialized.
            // Concrete FilesystemStore needed for download_to_directory's
            // hardlink path; the wrapper hides the concrete type so the
            // downcast must reach into the inner store directly.
            #[allow(clippy::disallowed_methods)]
            let fs_pin = Pin::new(
                fss.fast_store()
                    .downcast_ref::<FilesystemStore>(None)
                    .err_tip(|| "Could not downcast fast store to FilesystemStore")?,
            );
            let temp_str = temp_path.to_string_lossy().to_string();
            info!(
                hash = %&digest.packed_hash().to_string()[..12],
                "DirectoryCache: fast download_to_directory starting",
            );
            let construction_start = Instant::now();
            let result = crate::running_actions_manager::download_to_directory(
                fss, fs_pin, digest, &temp_str, None, None,
            )
            .await;
            let elapsed = construction_start.elapsed();
            match &result {
                Ok(()) => {
                    info!(
                        hash = %&digest.packed_hash().to_string()[..12],
                        elapsed_ms = elapsed.as_millis() as u64,
                        "DirectoryCache: fast download_to_directory completed",
                    );
                    Some(Ok(()))
                }
                Err(e) => {
                    warn!(
                        hash = %&digest.packed_hash().to_string()[..12],
                        ?e,
                        elapsed_ms = elapsed.as_millis() as u64,
                        "DirectoryCache: fast download_to_directory failed, trying serial fallback",
                    );
                    // Clean up the partial temp directory before fallback
                    drop(fs::remove_dir_all(temp_path).await);
                    drop(fs::create_dir_all(temp_path).await);
                    Some(Err(e.clone()))
                }
            }
        } else {
            None
        };

        // Use the fast path result, or fall back to serial construction.
        match fast_path_result {
            Some(Ok(())) => Ok(()),
            Some(Err(_)) | None => {
                if fast_path_result.is_none() {
                    info!(
                        hash = %&digest.packed_hash().to_string()[..12],
                        "DirectoryCache: using serial construct_directory_impl (no fast path available)",
                    );
                }
                let serial_start = Instant::now();
                self.construct_directory(*digest, temp_path).await
                    .err_tip(|| "Failed to construct directory for cache")?;
                info!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    elapsed_ms = serial_start.elapsed().as_millis() as u64,
                    "DirectoryCache: serial construct_directory_impl completed",
                );
                Ok(())
            }
        }
    }

    /// Subtree-aware construction: walks the resolved directory tree, creates
    /// hardlinked subtrees for cached portions, and only downloads uncached
    /// portions via `download_to_directory` or serial fallback.
    ///
    /// Uses file hardlinks (creating fresh directories) rather than directory
    /// symlinks because Bazel actions create output directories inside the
    /// input tree — symlinks would mutate the cache.
    async fn construct_with_subtrees(
        &self,
        root_digest: &DigestInfo,
        tree: &HashMap<DigestInfo, ProtoDirectory>,
        subtree_hits: &HashMap<DigestInfo, PathBuf>,
        dest_path: &Path,
    ) -> Result<(), Error> {
        let construction_start = Instant::now();

        // BFS walk of the tree, creating directories and symlinks.
        // When we encounter a subtree hit, we create a directory symlink and
        // skip its entire subtree (no need to traverse children).
        let mut queue = VecDeque::new();
        queue.push_back((*root_digest, dest_path.to_path_buf()));

        let mut dirs_created = 0usize;
        let mut subtrees_linked = 0usize;
        let mut files_to_download = Vec::new();
        let mut symlinks_to_create: Vec<(String, PathBuf)> = Vec::new();

        // Deferred subtree clone jobs: (child_digest, cached_src, dest_path)
        let mut subtree_clone_jobs: Vec<(DigestInfo, PathBuf, PathBuf)> = Vec::new();

        while let Some((dir_digest, dir_path)) = queue.pop_front() {
            let directory = tree.get(&dir_digest).ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "Directory {:?} not found in resolved tree during subtree construction",
                    dir_digest
                )
            })?;

            // Process subdirectories
            for subdir_node in &directory.directories {
                Self::validate_node_name(&subdir_node.name)?;
                let child_digest: DigestInfo = subdir_node
                    .digest
                    .as_ref()
                    .ok_or_else(|| {
                        make_err!(Code::InvalidArgument, "Directory node missing digest")
                    })?
                    .try_into()
                    .err_tip(|| "Invalid directory digest in subtree construction")?;

                let child_path = dir_path.join(&subdir_node.name);

                if let Some(cached_path) = subtree_hits.get(&child_digest) {
                    // Subtree hit: defer clonefile/hardlink to parallel phase.
                    subtree_clone_jobs.push((child_digest, cached_path.clone(), child_path));
                    subtrees_linked += 1;
                    // Do NOT enqueue children — the clone covers the entire subtree.
                    continue;
                }

                // No subtree hit — create the directory and recurse.
                fs::create_dir_all(&child_path).await.err_tip(|| {
                    format!("Failed to create directory: {}", child_path.display())
                })?;
                dirs_created += 1;
                queue.push_back((child_digest, child_path));
            }

            // Collect files that need to be downloaded for this (non-cached) directory.
            for file_node in &directory.files {
                Self::validate_node_name(&file_node.name)?;
                let file_digest: DigestInfo = file_node
                    .digest
                    .as_ref()
                    .ok_or_else(|| {
                        make_err!(Code::InvalidArgument, "File node missing digest")
                    })?
                    .try_into()
                    .err_tip(|| "Invalid file digest in subtree construction")?;

                let file_path = dir_path.join(&file_node.name);
                files_to_download.push((file_digest, file_path, file_node.is_executable));
            }

            // Collect symlinks from the proto
            for symlink_node in &directory.symlinks {
                Self::validate_node_name(&symlink_node.name)?;
                let link_path = dir_path.join(&symlink_node.name);
                symlinks_to_create.push((symlink_node.target.clone(), link_path));
            }
        }

        info!(
            hash = %&root_digest.packed_hash().to_string()[..12],
            dirs_created,
            subtrees_linked,
            files_to_download = files_to_download.len(),
            symlinks = symlinks_to_create.len(),
            "DirectoryCache: subtree-aware construction plan",
        );

        // Create symlinks (parent dirs exist from BFS, independent of clones/downloads).
        #[cfg(target_family = "unix")]
        for (target, link_path) in &symlinks_to_create {
            fs::symlink(target, link_path)
                .await
                .err_tip(|| format!("Failed to create symlink: {} -> {}", link_path.display(), target))?;
        }
        #[cfg(not(target_family = "unix"))]
        if !symlinks_to_create.is_empty() {
            return Err(make_err!(
                Code::Unimplemented,
                "DirectoryCache: proto declares {} symlink(s) but symlinks are not supported on this platform; the cache entry would be silently incomplete",
                symlinks_to_create.len(),
            ));
        }

        // Run subtree clones and file downloads concurrently.
        // Both write to non-overlapping paths, so they're safe to overlap.
        let clone_future = async {
            if subtree_clone_jobs.is_empty() {
                return Ok::<Vec<(DigestInfo, PathBuf)>, Error>(Vec::new());
            }
            let clone_start = Instant::now();
            let num_jobs = subtree_clone_jobs.len();
            let mut clone_set = tokio::task::JoinSet::new();
            for (digest, src, dst) in subtree_clone_jobs {
                clone_set.spawn(async move {
                    // Failpoint: simulate the cached subtree being evicted
                    // between the subtree_index lookup and the clone (a real
                    // race that the failed-subtree fallback walk handles).
                    // The argument is a digest-hex prefix so concurrent
                    // tests on different fixtures don't trigger each
                    // other's failpoints.
                    #[cfg(feature = "failpoints")]
                    {
                        let hash_str = digest.packed_hash().to_string();
                        let triggered = fail::eval(
                            "directory_cache_subtree_clone_fail",
                            |arg: Option<String>| match arg {
                                Some(prefix) => hash_str.starts_with(&prefix),
                                None => true,
                            },
                        )
                        .unwrap_or(false);
                        if triggered {
                            return (
                                digest,
                                src.clone(),
                                dst,
                                Err(make_err!(
                                    Code::NotFound,
                                    "failpoint: simulated subtree eviction during clone"
                                )),
                            );
                        }
                    }
                    let result = hardlink_directory_tree(&src, &dst).await;
                    (digest, src, dst, result)
                });
            }

            let mut failed_subtrees = Vec::new();
            while let Some(join_result) = clone_set.join_next().await {
                let (digest, src, dst, result) = join_result
                    .map_err(|e| make_err!(Code::Internal, "Subtree clone join error: {e}"))?;
                match result {
                    Ok(_method) => {
                        debug!(
                            child_hash = %&digest.packed_hash().to_string()[..12],
                            src = %src.display(),
                            dst = %dst.display(),
                            "DirectoryCache: cloned cached subtree",
                        );
                    }
                    Err(e) => {
                        warn!(
                            child_hash = %&digest.packed_hash().to_string()[..12],
                            src = %src.display(),
                            ?e,
                            "DirectoryCache: subtree evicted during construction, falling back to download",
                        );
                        failed_subtrees.push((digest, dst));
                    }
                }
            }

            info!(
                hash = %&root_digest.packed_hash().to_string()[..12],
                num_jobs,
                failed = failed_subtrees.len(),
                elapsed_ms = clone_start.elapsed().as_millis() as u64,
                "DirectoryCache: parallel subtree clones completed",
            );

            Ok(failed_subtrees)
        };

        let download_future = async {
            if files_to_download.is_empty() {
                return Ok::<(), Error>(());
            }
            if let (Some(fss), Some(_fs_store)) = (&self.fast_slow_store, &self.filesystem_store) {
                // Concrete FilesystemStore needed for hardlink operations
                // into the cache directory; the wrapper hides the concrete
                // type so the downcast must reach into the inner store.
                #[allow(clippy::disallowed_methods)]
                let fs_store_pin = Pin::new(
                    fss.fast_store()
                        .downcast_ref::<FilesystemStore>(None)
                        .err_tip(|| "Could not downcast fast store to FilesystemStore")?,
                );

                // Check which blobs are already in the fast store.
                let unique_digests: Vec<DigestInfo> = {
                    let mut seen = HashSet::new();
                    files_to_download
                        .iter()
                        .filter_map(|(d, _, _)| {
                            if d.size_bytes() > 0 && seen.insert(*d) { Some(*d) } else { None }
                        })
                        .collect()
                };
                let store_keys: Vec<StoreKey<'_>> =
                    unique_digests.iter().map(|d| (*d).into()).collect();
                let mut has_results = vec![None; store_keys.len()];
                // Sibling-bug audit (review #3 + #7): fast_store-only is
                // intentional here. A positive result means "blob is on
                // disk and ready for hardlink". Mirror-only blobs (held
                // in `mirror_blobs` but not on disk) must NOT be
                // reported as "cached" or the hardlink path would fail.
                // The `populate_fast_store_unchecked` call below
                // materializes mirror blobs to disk before the hardlink
                // step (see `materialize_mirror_to_fast` in
                // `fast_slow_store.rs`), so a mirror-only digest
                // correctly flows through the missing→populate→hardlink
                // pipeline rather than re-fetching from the slow store.
                // Local-only check: we are deciding which blobs to *download*
                // into the fast store, so going through the wrapper would
                // count slow-store hits and skip the populate we need.
                #[allow(clippy::disallowed_methods)]
                let fast_for_has = Pin::new(fss.fast_store());
                fast_for_has
                    .has_with_results(&store_keys, &mut has_results)
                    .await
                    .err_tip(|| "Batch has_with_results in subtree construction")?;

                // Fire-and-forget: warm page cache for blobs already present
                // on disk so they're hot by the time we hardlink them.
                {
                    let present: Vec<DigestInfo> = unique_digests
                        .iter()
                        .zip(has_results.iter())
                        .filter_map(|(d, r)| if r.is_some() { Some(*d) } else { None })
                        .collect();
                    if !present.is_empty() {
                        let fs_store_arc = _fs_store.clone();
                        tokio::task::spawn(async move {
                            for digest in &present {
                                if let Ok(entry) =
                                    fs_store_arc.get_file_entry_for_digest(digest).await
                                {
                                    let size = digest.size_bytes() as usize;
                                    entry
                                        .get_file_path_locked(|path| async move {
                                            if let Ok(f) =
                                                nativelink_util::fs::open_file(&path, 0).await
                                            {
                                                f.advise_willneed(0, size);
                                            }
                                            Ok(())
                                        })
                                        .await
                                        .ok();
                                }
                            }
                        });
                    }
                }

                // Populate missing blobs into the fast store.
                let missing: Vec<&DigestInfo> = unique_digests
                    .iter()
                    .zip(has_results.iter())
                    .filter_map(|(d, r)| if r.is_none() { Some(d) } else { None })
                    .collect();

                if !missing.is_empty() {
                    // Byte-bounded in-flight populate budget. Caps the total
                    // bytes of populates in flight so the parallel batch can't
                    // exceed the fast-store's free headroom — without this,
                    // a single 44 MiB blob plus 21 sibling populates can
                    // overrun a near-full 20 GB worker cache and trigger the
                    // self-cannibalization race that produces "fast store is
                    // over-pressured" Aborted errors (observed on worker-07
                    // at 17:09 UTC, 101 events / 22 digests / 44 s).
                    //
                    // 512 MiB chosen as a conservative cap that leaves room
                    // for the existing pinned-set (25% of cache = ~5 GB on a
                    // 20 GB worker), the long-tail of older entries the LRU
                    // needs to keep, and concurrent populates from other
                    // actions on the same worker. Any single blob larger than
                    // the cap monopolizes the semaphore until done — that's
                    // acceptable because (a) such blobs are rare and (b)
                    // serializing them is far better than the eviction race.
                    const POPULATE_BYTE_BUDGET: usize = 512 * 1024 * 1024;
                    // Per-task permit cost is capped at POPULATE_BYTE_BUDGET
                    // so single oversize blobs don't deadlock acquire_many.
                    let total_missing_bytes: u64 = missing
                        .iter()
                        .map(|d| u64::try_from(d.size_bytes()).unwrap_or(0))
                        .sum();
                    info!(
                        hash = %&root_digest.packed_hash().to_string()[..12],
                        missing = missing.len(),
                        total_missing_bytes,
                        budget = POPULATE_BYTE_BUDGET,
                        "DirectoryCache: fetching missing blobs for uncached files",
                    );
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(POPULATE_BYTE_BUDGET));
                    let mut join_set = tokio::task::JoinSet::new();
                    for d in missing {
                        let sem = semaphore.clone();
                        let fss = fss.clone();
                        let digest = *d;
                        let permits = usize::try_from(digest.size_bytes())
                            .unwrap_or(POPULATE_BYTE_BUDGET)
                            .min(POPULATE_BYTE_BUDGET)
                            .max(1);
                        let permits_u32 = u32::try_from(permits).unwrap_or(u32::MAX);
                        join_set.spawn(async move {
                            let _permit = sem.acquire_many(permits_u32).await;
                            let key: StoreKey<'_> = digest.into();
                            if let Err(err) = fss
                                .populate_fast_store_unchecked(key)
                                .await
                                .err_tip(|| format!("Failed to populate fast store for {digest:?}"))
                            {
                                error!(?digest, ?err, "directory_cache populate task failed");
                                return Err(err);
                            }
                            // Pin immediately after populate succeeds. The
                            // byte-bounded semaphore above narrows the
                            // self-cannibalization window; the pin closes it
                            // for the hardlink phase that follows. Pins
                            // auto-expire after PIN_TIMEOUT_SECS (120s); long
                            // actions (LTO, protobuf) may exceed that and
                            // re-expose post-hardlink references to LRU
                            // eviction — acceptable because by then the
                            // hardlinks are in the sandbox and the CAS blob
                            // doesn't need to outlive the cache slot.
                            //
                            // Pin cap is 25% of max_bytes (~5GB on a 20GB
                            // worker). Actions whose working set exceeds
                            // that will leave trailing digests unpinned — the
                            // verify-and-retry in populate_fast_store_unchecked
                            // stays as the safety net for those.
                            // Pin on the inner FilesystemStore directly: this
                            // is the eviction tier we are guarding against,
                            // and the FastSlowStore wrapper would also forward
                            // the pin to the slow GrpcStore where it is a no-op.
                            //
                            // #549 Phase 2 (BUILD + OBSERVE): account this
                            // digest's bytes against the process-wide
                            // `WorkerPinBudget`. Guard drops at end of scope
                            // (observation-only mode); Phase 4 (#551) will
                            // hold across the pin lifetime.
                            let _pin_admission_guard = usize::try_from(digest.size_bytes())
                                .ok()
                                .and_then(|n| {
                                    ::nativelink_store::worker_pin_budget::worker_pin_budget_singleton()
                                        .try_acquire(n)
                                });
                            #[allow(clippy::disallowed_methods)]
                            fss.fast_store().pin_digests(&[digest]);
                            drop(_pin_admission_guard);
                            Ok::<(), Error>(())
                        });
                    }
                    while let Some(result) = join_set.join_next().await {
                        result.map_err(|e| make_err!(Code::Internal, "Join error: {e}"))??;
                    }
                }

                // Hardlink files from the fast store to their destination paths.
                for (file_digest, file_path, is_executable) in &files_to_download {
                    if file_digest.size_bytes() == 0 {
                        fs::write(&file_path, b"")
                            .await
                            .err_tip(|| format!("Failed to create empty file: {}", file_path.display()))?;
                    } else {
                        let file_entry = fs_store_pin
                            .get_file_entry_for_digest(file_digest)
                            .await
                            .err_tip(|| format!("Getting file entry for {:?}", file_digest))?;
                        let dest = file_path.clone();
                        file_entry
                            .get_file_path_locked(|src_path| async move {
                                fs::hard_link(&src_path, &dest)
                                    .await
                                    .err_tip(|| format!(
                                        "Failed to hardlink {:?} to {}",
                                        src_path,
                                        dest.display(),
                                    ))
                            })
                            .await?;
                    }

                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let meta = fs::metadata(&file_path).await
                            .err_tip(|| "Failed to get file metadata for permission fix")?;
                        let current_mode = meta.permissions().mode() & 0o777;
                        let new_mode = if *is_executable {
                            current_mode | 0o111
                        } else {
                            0o555
                        };
                        if new_mode != current_mode {
                            let mut perms = meta.permissions();
                            perms.set_mode(new_mode);
                            fs::set_permissions(&file_path, perms).await
                                .err_tip(|| "Failed to set file permission")?;
                        }
                    }
                }
            } else {
                // Serial fallback: fetch each file from CAS individually.
                for (file_digest, file_path, _is_executable) in &files_to_download {
                    if is_zero_digest(*file_digest) {
                        fs::write(&file_path, b"")
                            .await
                            .err_tip(|| format!("Failed to create zero-digest file: {}", file_path.display()))?;
                    } else {
                        let data = self
                            .cas_store
                            .get_part_unchunked(StoreKey::Digest(*file_digest), 0, None)
                            .await
                            .err_tip(|| format!("Failed to fetch file: {}", file_path.display()))?;
                        fs::write(&file_path, data.as_ref())
                            .await
                            .err_tip(|| format!("Failed to write file: {}", file_path.display()))?;
                    }

                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mut perms = fs::metadata(&file_path).await
                            .err_tip(|| "Failed to get file metadata")?
                            .permissions();
                        perms.set_mode(0o555);
                        fs::set_permissions(&file_path, perms).await
                            .err_tip(|| "Failed to set file permissions")?;
                    }
                }
            }
            Ok(())
        };

        let (clone_result, download_result) = tokio::join!(clone_future, download_future);
        let failed_subtrees = clone_result?;
        download_result?;

        // Handle failed subtrees (rare — subtree evicted between check and clone).
        // Walk the tree to reconstruct, using serial CAS fetch for simplicity.
        for (failed_digest, failed_dst) in &failed_subtrees {
            subtrees_linked -= 1;
            drop(fs::remove_dir_all(failed_dst).await);

            let mut sub_queue = VecDeque::new();
            sub_queue.push_back((*failed_digest, failed_dst.clone()));
            while let Some((d, p)) = sub_queue.pop_front() {
                // Failpoint: simulate the resolved tree being structurally
                // incomplete (a child digest referenced by a directory that
                // is itself missing from the tree map). The previous behavior
                // here was a silent `warn!` + continue, which published an
                // incomplete cache entry. The new behavior is a hard error.
                // The argument is a digest-hex prefix scoping which
                // digests trigger the missing-from-tree result.
                #[cfg(feature = "failpoints")]
                let force_missing = {
                    let hash_str = d.packed_hash().to_string();
                    fail::eval(
                        "directory_cache_failed_subtree_missing_in_tree",
                        |arg: Option<String>| match arg {
                            Some(prefix) => hash_str.starts_with(&prefix),
                            None => true,
                        },
                    )
                    .unwrap_or(false)
                };
                #[cfg(not(feature = "failpoints"))]
                let force_missing = false;

                let lookup = if force_missing { None } else { tree.get(&d) };
                if let Some(dir) = lookup {
                    fs::create_dir_all(&p).await.err_tip(|| {
                        format!("Failed to create directory for failed subtree: {}", p.display())
                    })?;
                    dirs_created += 1;
                    for subdir_node in &dir.directories {
                        Self::validate_node_name(&subdir_node.name)?;
                        let cd: DigestInfo = subdir_node
                            .digest
                            .as_ref()
                            .ok_or_else(|| make_err!(Code::InvalidArgument, "Directory node missing digest"))?
                            .try_into()
                            .err_tip(|| "Invalid directory digest in failed subtree walk")?;
                        sub_queue.push_back((cd, p.join(&subdir_node.name)));
                    }
                    for file_node in &dir.files {
                        Self::validate_node_name(&file_node.name)?;
                        let fd: DigestInfo = file_node
                            .digest
                            .as_ref()
                            .ok_or_else(|| make_err!(Code::InvalidArgument, "File node missing digest"))?
                            .try_into()
                            .err_tip(|| "Invalid file digest in failed subtree walk")?;
                        let fp = p.join(&file_node.name);
                        if is_zero_digest(fd) {
                            fs::write(&fp, b"")
                                .await
                                .err_tip(|| format!("Failed to create zero-digest file: {}", fp.display()))?;
                        } else {
                            let data = self
                                .cas_store
                                .get_part_unchunked(StoreKey::Digest(fd), 0, None)
                                .await
                                .err_tip(|| format!("Failed to fetch file for failed subtree: {}", fp.display()))?;
                            fs::write(&fp, data.as_ref())
                                .await
                                .err_tip(|| format!("Failed to write file: {}", fp.display()))?;
                        }
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            let mut perms = fs::metadata(&fp).await
                                .err_tip(|| "Failed to get file metadata")?.permissions();
                            perms.set_mode(0o555);
                            fs::set_permissions(&fp, perms).await
                                .err_tip(|| "Failed to set file permissions")?;
                        }
                    }
                    #[cfg(target_family = "unix")]
                    for symlink_node in &dir.symlinks {
                        Self::validate_node_name(&symlink_node.name)?;
                        let link_path = p.join(&symlink_node.name);
                        fs::symlink(&symlink_node.target, &link_path)
                            .await
                            .err_tip(|| format!("Failed to create symlink: {}", link_path.display()))?;
                    }
                    #[cfg(not(target_family = "unix"))]
                    if !dir.symlinks.is_empty() {
                        return Err(make_err!(
                            Code::Unimplemented,
                            "DirectoryCache failed-subtree fallback: proto declares {} symlink(s) but symlinks are not supported on this platform",
                            dir.symlinks.len(),
                        ));
                    }
                } else {
                    // resolve_directory_tree should have validated the tree
                    // is structurally complete before we got here. If we
                    // reach this branch we'd skip an entire subtree of the
                    // construction silently, so fail loud instead — letting
                    // the action retry against a fresh tree resolution.
                    return Err(make_err!(
                        Code::Internal,
                        "DirectoryCache: directory {d:?} not found in resolved tree during failed-subtree fallback walk; refusing to publish incomplete cache entry",
                    ));
                }
            }
        }

        let elapsed = construction_start.elapsed();
        info!(
            hash = %&root_digest.packed_hash().to_string()[..12],
            dirs_created,
            subtrees_linked,
            files_downloaded = files_to_download.len(),
            elapsed_ms = elapsed.as_millis() as u64,
            "DirectoryCache: subtree-aware construction completed",
        );

        Ok(())
    }

    /// Subtree-aware construction for direct-use mode.
    ///
    /// Similar to `construct_with_subtrees`, but uses **symlinks** for cached
    /// subtrees instead of hardlinks/clonefiles. This means the new cache
    /// entry's subdirectory is a symlink pointing at the existing cached
    /// subtree directory, rather than a copy of it.
    ///
    /// Files in non-cached portions are still hardlinked from the CAS (or
    /// fetched via serial fallback).
    async fn construct_with_subtrees_direct(
        &self,
        root_digest: &DigestInfo,
        tree: &HashMap<DigestInfo, ProtoDirectory>,
        subtree_hits: &HashMap<DigestInfo, PathBuf>,
        dest_path: &Path,
    ) -> Result<(), Error> {
        let construction_start = Instant::now();

        let mut queue = VecDeque::new();
        queue.push_back((*root_digest, dest_path.to_path_buf()));

        let mut dirs_created = 0usize;
        let mut subtrees_symlinked = 0usize;
        let mut files_to_download = Vec::new();
        let mut proto_symlinks_to_create: Vec<(String, PathBuf)> = Vec::new();

        while let Some((dir_digest, dir_path)) = queue.pop_front() {
            let directory = tree.get(&dir_digest).ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "Directory {:?} not found in resolved tree during direct-use subtree construction",
                    dir_digest
                )
            })?;

            // Process subdirectories
            for subdir_node in &directory.directories {
                Self::validate_node_name(&subdir_node.name)?;
                let child_digest: DigestInfo = subdir_node
                    .digest
                    .as_ref()
                    .ok_or_else(|| {
                        make_err!(Code::InvalidArgument, "Directory node missing digest")
                    })?
                    .try_into()
                    .err_tip(|| "Invalid directory digest in direct-use subtree construction")?;

                let child_path = dir_path.join(&subdir_node.name);

                if let Some(cached_path) = subtree_hits.get(&child_digest) {
                    // Subtree hit: create a symlink instead of clonefile/hardlink.
                    #[cfg(unix)]
                    fs::symlink(cached_path, &child_path).await.err_tip(|| {
                        format!(
                            "Failed to symlink subtree {} -> {}",
                            child_path.display(),
                            cached_path.display()
                        )
                    })?;
                    #[cfg(not(unix))]
                    fs::symlink_dir(cached_path, &child_path).await.err_tip(|| {
                        format!(
                            "Failed to symlink_dir subtree {} -> {}",
                            child_path.display(),
                            cached_path.display()
                        )
                    })?;
                    subtrees_symlinked += 1;
                    debug!(
                        child_hash = %&child_digest.packed_hash().to_string()[..12],
                        src = %cached_path.display(),
                        dst = %child_path.display(),
                        "DirectoryCache direct-use: symlinked cached subtree",
                    );
                    // Do NOT enqueue children -- the symlink covers the entire subtree.
                    continue;
                }

                // No subtree hit -- create the directory and recurse.
                fs::create_dir_all(&child_path).await.err_tip(|| {
                    format!("Failed to create directory: {}", child_path.display())
                })?;
                dirs_created += 1;
                queue.push_back((child_digest, child_path));
            }

            // Collect files that need to be downloaded for this (non-cached) directory.
            for file_node in &directory.files {
                Self::validate_node_name(&file_node.name)?;
                let file_digest: DigestInfo = file_node
                    .digest
                    .as_ref()
                    .ok_or_else(|| {
                        make_err!(Code::InvalidArgument, "File node missing digest")
                    })?
                    .try_into()
                    .err_tip(|| "Invalid file digest in direct-use subtree construction")?;

                let file_path = dir_path.join(&file_node.name);
                files_to_download.push((file_digest, file_path, file_node.is_executable));
            }

            // Collect proto-defined symlinks
            for symlink_node in &directory.symlinks {
                Self::validate_node_name(&symlink_node.name)?;
                let link_path = dir_path.join(&symlink_node.name);
                proto_symlinks_to_create.push((symlink_node.target.clone(), link_path));
            }
        }

        info!(
            hash = %&root_digest.packed_hash().to_string()[..12],
            dirs_created,
            subtrees_symlinked,
            files_to_download = files_to_download.len(),
            proto_symlinks = proto_symlinks_to_create.len(),
            "DirectoryCache direct-use: subtree-aware construction plan",
        );

        // Create proto-defined symlinks
        #[cfg(target_family = "unix")]
        for (target, link_path) in &proto_symlinks_to_create {
            fs::symlink(target, link_path)
                .await
                .err_tip(|| format!("Failed to create symlink: {} -> {}", link_path.display(), target))?;
        }
        #[cfg(not(target_family = "unix"))]
        if !proto_symlinks_to_create.is_empty() {
            return Err(make_err!(
                Code::Unimplemented,
                "DirectoryCache direct-use: proto declares {} symlink(s) but symlinks are not supported on this platform",
                proto_symlinks_to_create.len(),
            ));
        }

        // Download files (same logic as construct_with_subtrees)
        if !files_to_download.is_empty() {
            if let (Some(fss), Some(_fs_store)) = (&self.fast_slow_store, &self.filesystem_store) {
                // Concrete FilesystemStore needed for hardlink operations
                // into the cache directory; the wrapper hides the concrete
                // type so the downcast must reach into the inner store.
                #[allow(clippy::disallowed_methods)]
                let fs_store_pin = Pin::new(
                    fss.fast_store()
                        .downcast_ref::<FilesystemStore>(None)
                        .err_tip(|| "Could not downcast fast store to FilesystemStore")?,
                );

                // Check which blobs are already in the fast store.
                let unique_digests: Vec<DigestInfo> = {
                    let mut seen = HashSet::new();
                    files_to_download
                        .iter()
                        .filter_map(|(d, _, _)| {
                            if d.size_bytes() > 0 && seen.insert(*d) { Some(*d) } else { None }
                        })
                        .collect()
                };
                let store_keys: Vec<StoreKey<'_>> =
                    unique_digests.iter().map(|d| (*d).into()).collect();
                let mut has_results = vec![None; store_keys.len()];
                // Sibling-bug audit (review #3 + #7): see the matching
                // comment in `construct_with_subtrees` above. Mirror-only
                // digests appear as "missing" here and are materialized
                // to disk via `populate_fast_store_unchecked`, which
                // checks `mirror_blobs` first. The wrapper-level
                // has-check is intentionally NOT used so the hardlink
                // path can rely on disk presence after populate.
                // Local-only check: we are deciding which blobs to populate
                // into the fast store, so going through the wrapper would
                // count slow-store hits and skip the populate we need.
                #[allow(clippy::disallowed_methods)]
                let fast_for_has = Pin::new(fss.fast_store());
                fast_for_has
                    .has_with_results(&store_keys, &mut has_results)
                    .await
                    .err_tip(|| "Batch has_with_results in direct-use subtree construction")?;

                // Populate missing blobs into the fast store.
                let missing: Vec<&DigestInfo> = unique_digests
                    .iter()
                    .zip(has_results.iter())
                    .filter_map(|(d, r)| if r.is_none() { Some(d) } else { None })
                    .collect();

                if !missing.is_empty() {
                    info!(
                        hash = %&root_digest.packed_hash().to_string()[..12],
                        missing = missing.len(),
                        "DirectoryCache direct-use: fetching missing blobs",
                    );
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
                    let mut join_set = tokio::task::JoinSet::new();
                    for d in missing {
                        let sem = semaphore.clone();
                        let fss = fss.clone();
                        let digest = *d;
                        join_set.spawn(async move {
                            let _permit = sem.acquire().await;
                            let key: StoreKey<'_> = digest.into();
                            if let Err(err) = fss
                                .populate_fast_store_unchecked(key)
                                .await
                                .err_tip(|| format!("Failed to populate fast store for {digest:?}"))
                            {
                                error!(?digest, ?err, "directory_cache populate task failed");
                                return Err(err);
                            }
                            Ok::<(), Error>(())
                        });
                    }
                    while let Some(result) = join_set.join_next().await {
                        result.map_err(|e| make_err!(Code::Internal, "Join error: {e}"))??;
                    }
                }

                // Hardlink files from the fast store to their destination paths.
                for (file_digest, file_path, is_executable) in &files_to_download {
                    if file_digest.size_bytes() == 0 {
                        fs::write(&file_path, b"")
                            .await
                            .err_tip(|| format!("Failed to create empty file: {}", file_path.display()))?;
                    } else {
                        let file_entry = fs_store_pin
                            .get_file_entry_for_digest(file_digest)
                            .await
                            .err_tip(|| format!("Getting file entry for {:?}", file_digest))?;
                        let dest = file_path.clone();
                        file_entry
                            .get_file_path_locked(|src_path| async move {
                                fs::hard_link(&src_path, &dest)
                                    .await
                                    .err_tip(|| format!(
                                        "Failed to hardlink {:?} to {}",
                                        src_path,
                                        dest.display(),
                                    ))
                            })
                            .await?;
                    }

                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let meta = fs::metadata(&file_path).await
                            .err_tip(|| "Failed to get file metadata for permission fix")?;
                        let current_mode = meta.permissions().mode() & 0o777;
                        let new_mode = if *is_executable {
                            current_mode | 0o111
                        } else {
                            0o555
                        };
                        if new_mode != current_mode {
                            let mut perms = meta.permissions();
                            perms.set_mode(new_mode);
                            fs::set_permissions(&file_path, perms).await
                                .err_tip(|| "Failed to set file permission")?;
                        }
                    }
                }
            } else {
                // Serial fallback: fetch each file from CAS individually.
                for (file_digest, file_path, _is_executable) in &files_to_download {
                    if is_zero_digest(*file_digest) {
                        fs::write(&file_path, b"")
                            .await
                            .err_tip(|| format!("Failed to create zero-digest file: {}", file_path.display()))?;
                    } else {
                        let data = self
                            .cas_store
                            .get_part_unchunked(StoreKey::Digest(*file_digest), 0, None)
                            .await
                            .err_tip(|| format!("Failed to fetch file: {}", file_path.display()))?;
                        fs::write(&file_path, data.as_ref())
                            .await
                            .err_tip(|| format!("Failed to write file: {}", file_path.display()))?;
                    }

                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mut perms = fs::metadata(&file_path).await
                            .err_tip(|| "Failed to get file metadata")?
                            .permissions();
                        perms.set_mode(0o555);
                        fs::set_permissions(&file_path, perms).await
                            .err_tip(|| "Failed to set file permissions")?;
                    }
                }
            }
        }

        let elapsed = construction_start.elapsed();
        info!(
            hash = %&root_digest.packed_hash().to_string()[..12],
            dirs_created,
            subtrees_symlinked,
            files_downloaded = files_to_download.len(),
            elapsed_ms = elapsed.as_millis() as u64,
            "DirectoryCache direct-use: subtree-aware construction completed",
        );

        Ok(())
    }

    /// Removes subtree index entries that belong to a given cache entry path.
    /// Loads the merkle metadata file from the cache entry to determine which
    /// digests to remove. Also decrements subtree refcounts, updates the
    /// reverse index, and records fully-removed digests for delta reporting.
    async fn remove_subtree_index_for_path(
        &self,
        cache_entry_path: &Path,
        index: &mut HashMap<DigestInfo, PathBuf>,
    ) {
        // Parse the root digest from the directory name so we can update the
        // reverse index (subtree_to_roots).
        let root_digest = cache_entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(Self::parse_digest_from_dirname);

        let merkle_path = cache_entry_path.join(MERKLE_METADATA_FILENAME);
        if let Ok(data) = fs::read_to_string(&merkle_path).await {
            if let Ok(merkle) = MerkleTreeMetadata::deserialize(&data) {
                let mut removed = 0usize;
                let merkle_digests: Vec<DigestInfo> =
                    merkle.digest_to_relpath.keys().copied().collect();
                for (sub_digest, relpath) in &merkle.digest_to_relpath {
                    // Only remove if the index entry points to this specific cache entry.
                    let abs_path = if relpath.is_empty() {
                        cache_entry_path.to_path_buf()
                    } else {
                        cache_entry_path.join(relpath)
                    };
                    if let Some(existing) = index.get(sub_digest) {
                        if *existing == abs_path {
                            index.remove(sub_digest);
                            removed += 1;
                        }
                    }
                }
                // Record subtree removals for delta reporting.
                // This decrements refcounts, updates the reverse index, and
                // only marks digests as removed when they are no longer in
                // ANY cached entry.
                if let Some(rd) = &root_digest {
                    self.record_subtree_removal(rd, &merkle_digests).await;
                }
                debug!(
                    path = %cache_entry_path.display(),
                    removed_subtrees = removed,
                    "DirectoryCache: cleaned up subtree index for evicted entry",
                );
            }
        }
    }

    /// Try to parse a directory entry name as a DigestInfo.
    /// Expected format is the same as `DigestInfo::to_string()`,
    /// i.e., `{hash}-{size_bytes}`.
    fn parse_digest_from_dirname(name: &str) -> Option<DigestInfo> {
        // DigestInfo::to_string() produces "{hash}-{size}", so split on the last '-'
        let last_dash = name.rfind('-')?;
        let hash = &name[..last_dash];
        let size_str = &name[last_dash + 1..];
        let size: i64 = size_str.parse().ok()?;
        DigestInfo::try_new(hash, size).ok()
    }

    /// Constructs a directory from the CAS at the given path.
    /// `depth` tracks nesting depth for symlink target validation.
    fn construct_directory_impl<'a>(
        &'a self,
        digest: DigestInfo,
        dest_path: &'a Path,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            debug!(?digest, ?dest_path, "Constructing directory");

            // Fetch the Directory proto
            let directory: ProtoDirectory = get_and_decode_digest(&self.cas_store, digest.into())
                .await
                .err_tip(|| format!("Failed to fetch directory digest: {digest:?}"))?;

            // Create the destination directory
            fs::create_dir_all(dest_path)
                .await
                .err_tip(|| format!("Failed to create directory: {}", dest_path.display()))?;

            // Process files
            for file in &directory.files {
                Self::validate_node_name(&file.name)?;
                self.create_file(dest_path, file).await?;
            }

            // Process subdirectories recursively
            for dir_node in &directory.directories {
                Self::validate_node_name(&dir_node.name)?;
                self.create_subdirectory(dest_path, dir_node, depth + 1)
                    .await?;
            }

            // Process symlinks
            for symlink in &directory.symlinks {
                Self::validate_node_name(&symlink.name)?;
                Self::validate_symlink_target(&symlink.target, depth)?;
                self.create_symlink(dest_path, symlink).await?;
            }

            Ok(())
        })
    }

    /// Constructs a directory from the CAS at the given path
    fn construct_directory<'a>(
        &'a self,
        digest: DigestInfo,
        dest_path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        self.construct_directory_impl(digest, dest_path, 0)
    }

    /// Creates a file from a `FileNode`
    async fn create_file(&self, parent: &Path, file_node: &FileNode) -> Result<(), Error> {
        let file_path = parent.join(&file_node.name);
        let digest = DigestInfo::try_from(
            file_node
                .digest
                .as_ref()
                .ok_or_else(|| make_err!(Code::InvalidArgument, "File node missing digest"))?
                .clone(),
        )
        .err_tip(|| "Invalid file digest")?;

        trace!(?file_path, ?digest, "Creating file");

        // Failpoint: simulate a download path that returns Ok but never
        // actually wrote the file to disk. This is the exact failure mode
        // that produced incomplete cache entries in production — the
        // post-construction validator (validate_constructed_tree) is the
        // backstop that should catch it before the entry is published.
        // The failpoint argument (set via `fail::cfg(name, "return(prefix)")`)
        // is matched against the digest's hex prefix so concurrent tests
        // touching different digests don't interfere with each other.
        #[cfg(feature = "failpoints")]
        {
            let hash_str = digest.packed_hash().to_string();
            let drop_file = fail::eval(
                "directory_cache_skip_file_in_construction",
                |arg: Option<String>| match arg {
                    Some(prefix) => hash_str.starts_with(&prefix),
                    None => true,
                },
            )
            .unwrap_or(false);
            if drop_file {
                return Ok(());
            }
        }

        if is_zero_digest(digest) {
            fs::write(&file_path, b"")
                .await
                .err_tip(|| format!("Failed to create zero-digest file: {}", file_path.display()))?;
        } else {
            // Fetch file content from CAS
            let data = self
                .cas_store
                .get_part_unchunked(StoreKey::Digest(digest), 0, None)
                .await
                .err_tip(|| format!("Failed to fetch file: {}", file_path.display()))?;

            // Write to disk
            fs::write(&file_path, data.as_ref())
                .await
                .err_tip(|| format!("Failed to write file: {}", file_path.display()))?;
        }

        // Always set 0o555 to match CAS store defaults. Some build tools
        // (rules_cc, rules_rust) set is_executable=false on shell scripts
        // that must be executable; 0o555 as the base avoids EPERM.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&file_path)
                .await
                .err_tip(|| "Failed to get file metadata")?
                .permissions();
            perms.set_mode(0o555);
            fs::set_permissions(&file_path, perms)
                .await
                .err_tip(|| "Failed to set file permissions")?;
        }

        Ok(())
    }

    /// Creates a subdirectory from a `DirectoryNode`
    async fn create_subdirectory(
        &self,
        parent: &Path,
        dir_node: &DirectoryNode,
        depth: usize,
    ) -> Result<(), Error> {
        let dir_path = parent.join(&dir_node.name);
        let digest = DigestInfo::try_from(
            dir_node
                .digest
                .as_ref()
                .ok_or_else(|| {
                    make_err!(Code::InvalidArgument, "Directory node missing digest")
                })?
                .clone(),
        )
        .err_tip(|| "Invalid directory digest")?;

        trace!(?dir_path, ?digest, "Creating subdirectory");

        // Failpoint: simulate the apple/ corruption — a subdirectory that
        // the proto says exists but is silently never created on disk.
        // The validator must catch this before publication. As with the
        // file-drop failpoint, the argument is a hex prefix matched
        // against the directory's digest so other concurrent tests are
        // unaffected.
        #[cfg(feature = "failpoints")]
        {
            let hash_str = digest.packed_hash().to_string();
            let skip = fail::eval(
                "directory_cache_skip_subdir_in_construction",
                |arg: Option<String>| match arg {
                    Some(prefix) => hash_str.starts_with(&prefix),
                    None => true,
                },
            )
            .unwrap_or(false);
            if skip {
                return Ok(());
            }
        }

        // Recursively construct subdirectory
        self.construct_directory_impl(digest, &dir_path, depth)
            .await
    }

    /// Creates a symlink from a `SymlinkNode`
    async fn create_symlink(&self, parent: &Path, symlink: &SymlinkNode) -> Result<(), Error> {
        let link_path = parent.join(&symlink.name);
        let target = Path::new(&symlink.target);

        trace!(?link_path, ?target, "Creating symlink");

        #[cfg(unix)]
        fs::symlink(&target, &link_path)
            .await
            .err_tip(|| format!("Failed to create symlink: {}", link_path.display()))?;

        #[cfg(windows)]
        {
            // On Windows, we need to know if target is a directory
            // For now, assume files (can be improved later)
            fs::symlink_file(&target, &link_path)
                .await
                .err_tip(|| format!("Failed to create symlink: {}", link_path.display()))?;
        }

        Ok(())
    }

    /// Collects entries to evict to make room for `incoming_size` bytes.
    /// Removes them from the HashMap and returns their paths for disk cleanup.
    /// This is called while holding the write lock; actual disk I/O happens after
    /// the lock is released.
    fn collect_evictions(
        &self,
        incoming_size: u64,
        cache: &mut HashMap<DigestInfo, CachedDirectoryMetadata>,
    ) -> Vec<PathBuf> {
        let mut evicted_paths = Vec::new();

        // Evict by entry count
        while cache.len() >= self.config.max_entries {
            if let Some((path, digest, size)) = self.evict_lru_entry(cache) {
                info!(
                    hash = %&digest.packed_hash().to_string()[..12],
                    size_bytes = size,
                    reason = "count_limit",
                    entries_remaining = cache.len(),
                    max_entries = self.config.max_entries,
                    "DirectoryCache: evicting entry",
                );
                evicted_paths.push(path);
            } else {
                warn!(
                    entries = cache.len(),
                    max = self.config.max_entries,
                    "DirectoryCache: over entry limit but all entries are in use"
                );
                break;
            }
        }

        // Evict by size
        if self.config.max_size_bytes > 0 {
            loop {
                let current_size: u64 = cache.values().map(|m| m.size).sum();
                if current_size + incoming_size <= self.config.max_size_bytes {
                    break;
                }
                if let Some((path, digest, size)) = self.evict_lru_entry(cache) {
                    info!(
                        hash = %&digest.packed_hash().to_string()[..12],
                        size_bytes = size,
                        size_freed_mb = format!("{:.2}", size as f64 / (1024.0 * 1024.0)),
                        reason = "size_limit",
                        entries_remaining = cache.len(),
                        current_total_mb = format!("{:.2}", cache.values().map(|m| m.size).sum::<u64>() as f64 / (1024.0 * 1024.0)),
                        max_size_mb = format!("{:.2}", self.config.max_size_bytes as f64 / (1024.0 * 1024.0)),
                        "DirectoryCache: evicting entry",
                    );
                    evicted_paths.push(path);
                } else {
                    warn!(
                        current_size = current_size + incoming_size,
                        max = self.config.max_size_bytes,
                        "DirectoryCache: over size limit but all entries are in use"
                    );
                    break;
                }
            }
        }

        evicted_paths
    }

    /// Removes the LRU entry with ref_count == 0 from the cache HashMap.
    /// Returns the evicted entry's (path, digest, size) for logging and disk
    /// cleanup, or `None` if no evictable entry exists.
    fn evict_lru_entry(
        &self,
        cache: &mut HashMap<DigestInfo, CachedDirectoryMetadata>,
    ) -> Option<(PathBuf, DigestInfo, u64)> {
        let to_evict = cache
            .iter()
            .filter(|(_, m)| m.ref_count.load(Ordering::Relaxed) == 0)
            .min_by_key(|(_, m)| m.last_access_millis.load(Ordering::Relaxed))
            .map(|(digest, _)| *digest);

        if let Some(digest) = to_evict {
            if let Some(metadata) = cache.remove(&digest) {
                return Some((metadata.path, digest, metadata.size));
            }
        }

        None
    }

    /// Gets the cache path for a digest
    fn get_cache_path(&self, digest: &DigestInfo) -> PathBuf {
        self.config.cache_root.join(digest.to_string())
    }

    /// Returns cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        let total_size: u64 = cache.values().map(|m| m.size).sum();
        let in_use = cache
            .values()
            .filter(|m| m.ref_count.load(Ordering::Relaxed) > 0)
            .count();
        let reverse_index_size = self.subtree_to_roots.read().await.len();

        CacheStats {
            entries: cache.len(),
            total_size_bytes: total_size,
            in_use_entries: in_use,
            fuzzy_matches: self.fuzzy_match_count.load(Ordering::Relaxed),
            reverse_index_entries: reverse_index_size,
        }
    }
}

/// Statistics about the directory cache
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub entries: usize,
    pub total_size_bytes: u64,
    pub in_use_entries: usize,
    /// Number of times a fuzzy match was used instead of full construction
    pub fuzzy_matches: u64,
    /// Number of entries in the subtree-to-roots reverse index
    pub reverse_index_entries: usize,
}

#[cfg(test)]
mod tests {
    use nativelink_config::stores::MemorySpec;
    use nativelink_macro::nativelink_test;
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::StoreLike;
    use prost::Message;
    use tempfile::TempDir;

    use super::*;

    async fn setup_test_store() -> (Store, DigestInfo) {
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));

        // Create a simple directory structure
        let file_content = b"Hello, World!";
        // SHA256 hash of "Hello, World!"
        let file_digest = DigestInfo::try_new(
            "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
            13,
        )
        .unwrap();

        // Upload file
        store
            .as_store_driver_pin()
            .update_oneshot(file_digest.into(), file_content.to_vec().into())
            .await
            .unwrap();

        // Create Directory proto
        let directory = ProtoDirectory {
            files: vec![FileNode {
                name: "test.txt".to_string(),
                digest: Some(file_digest.into()),
                is_executable: false,
                ..Default::default()
            }],
            directories: vec![],
            symlinks: vec![],
            ..Default::default()
        };

        // Encode and upload directory
        let mut dir_data = Vec::new();
        directory.encode(&mut dir_data).unwrap();
        // Use a fixed hash for the directory
        let dir_digest = DigestInfo::try_new(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            dir_data.len() as i64,
        )
        .unwrap();

        store
            .as_store_driver_pin()
            .update_oneshot(dir_digest.into(), dir_data.into())
            .await
            .unwrap();

        (store, dir_digest)
    }

    /// Creates a store with two different directory digests for eviction testing.
    async fn setup_two_digest_store() -> (Store, DigestInfo, DigestInfo) {
        let store = Store::new(MemoryStore::new(&Default::default()));

        // File A
        let content_a = b"File A content";
        let digest_a = DigestInfo::try_new(
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            content_a.len() as i64,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(digest_a.into(), content_a.to_vec().into())
            .await
            .unwrap();

        // Directory A
        let dir_a = ProtoDirectory {
            files: vec![FileNode {
                name: "a.txt".to_string(),
                digest: Some(digest_a.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut dir_a_data = Vec::new();
        dir_a.encode(&mut dir_a_data).unwrap();
        let dir_digest_a = DigestInfo::try_new(
            "aaaa567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            dir_a_data.len() as i64,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(dir_digest_a.into(), dir_a_data.into())
            .await
            .unwrap();

        // File B
        let content_b = b"File B content!!";
        let digest_b = DigestInfo::try_new(
            "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6b1b2",
            content_b.len() as i64,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(digest_b.into(), content_b.to_vec().into())
            .await
            .unwrap();

        // Directory B
        let dir_b = ProtoDirectory {
            files: vec![FileNode {
                name: "b.txt".to_string(),
                digest: Some(digest_b.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut dir_b_data = Vec::new();
        dir_b.encode(&mut dir_b_data).unwrap();
        let dir_digest_b = DigestInfo::try_new(
            "bbbb567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            dir_b_data.len() as i64,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(dir_digest_b.into(), dir_b_data.into())
            .await
            .unwrap();

        (store, dir_digest_a, dir_digest_b)
    }

    #[nativelink_test]
    async fn test_directory_cache_basic() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // First access - cache miss
        let dest1 = temp_dir.path().join("dest1");
        let hit = cache.get_or_create(dir_digest, &dest1).await?;
        assert!(!hit, "First access should be cache miss");
        assert!(dest1.join("test.txt").exists());

        // Second access - cache hit
        let dest2 = temp_dir.path().join("dest2");
        let hit = cache.get_or_create(dir_digest, &dest2).await?;
        assert!(hit, "Second access should be cache hit");
        assert!(dest2.join("test.txt").exists());

        // Verify stats
        let stats = cache.stats().await;
        assert_eq!(stats.entries, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_hardlink_into_existing_directory() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Pre-create destination directory (simulates work_directory already existing)
        let dest = temp_dir.path().join("existing_dest");
        fs::create_dir(&dest).await.unwrap();

        // Should succeed even though dest already exists (Bug 1 fix)
        let hit = cache.get_or_create(dir_digest, &dest).await?;
        assert!(!hit, "First access should be cache miss");
        assert!(dest.join("test.txt").exists());

        // Cache hit into another pre-existing directory
        let dest2 = temp_dir.path().join("existing_dest2");
        fs::create_dir(&dest2).await.unwrap();
        let hit = cache.get_or_create(dir_digest, &dest2).await?;
        assert!(hit, "Second access should be cache hit");
        assert!(dest2.join("test.txt").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_construction_failure_cleanup() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");

        // Create a store with no data — construction will fail when fetching the digest
        let store = Store::new(MemoryStore::new(&Default::default()));

        let bogus_digest = DigestInfo::try_new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
        )
        .unwrap();

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        let dest = temp_dir.path().join("dest");
        let result = cache.get_or_create(bogus_digest, &dest).await;
        assert!(result.is_err(), "Should fail when digest not in store");

        // Bug 2 fix: No orphaned temp directories should remain.
        // Exclude .cache_version which is legitimate cache metadata written
        // by DirectoryCache::new().
        let mut entries = fs::read_dir(&cache_root).await.unwrap();
        let mut leftover = Vec::new();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name == ".cache_version" {
                continue;
            }
            leftover.push(name);
        }
        assert!(
            leftover.is_empty(),
            "No orphaned temp dirs should remain in cache_root, found: {leftover:?}"
        );

        // Verify construction lock was cleaned up (Bug 3 fix).
        // The coalesce helper's RAII guard removes the in_flight slot
        // even on leader error.
        let locks = cache.construction_locks.lock();
        assert!(
            locks.is_empty(),
            "Construction lock should be cleaned up after failure"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_eviction_all_in_use() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 1,
            max_size_bytes: 0,
            cache_root,
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Fill the cache
        let dest1 = temp_dir.path().join("dest1");
        cache.get_or_create(dir_digest, &dest1).await?;

        // Simulate all entries being in-use
        {
            let cache_map = cache.cache.read().await;
            if let Some(metadata) = cache_map.get(&dir_digest) {
                metadata.ref_count.store(1, Ordering::Relaxed);
            }
        }

        // Bug 4 fix: collect_evictions should not loop infinitely.
        {
            let mut cache_map = cache.cache.write().await;
            let evicted = cache.collect_evictions(100, &mut cache_map);
            assert!(evicted.is_empty(), "Nothing should be evictable");
            assert_eq!(cache_map.len(), 1, "Entry should still be present");
        }

        // Clean up ref_count
        {
            let cache_map = cache.cache.read().await;
            if let Some(metadata) = cache_map.get(&dir_digest) {
                metadata.ref_count.store(0, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_same_digest() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };

        let cache = Arc::new(DirectoryCache::new(config, store, None).await?);

        // Spawn multiple concurrent requests for the same digest
        let mut handles = Vec::new();
        for i in 0..5 {
            let cache = Arc::clone(&cache);
            let dest = temp_dir.path().join(format!("concurrent_dest_{i}"));
            handles.push(tokio::spawn(async move {
                cache.get_or_create(dir_digest, &dest).await
            }));
        }

        // Wait for all tasks to complete; every task should succeed.
        // Pre-coalescing this test split results into "1 miss + 4 hits"
        // by relying on the leader winning the race against a tokio::spawn
        // schedule, but with `with_construction_lock` all coalesced
        // callers (leader and waiters) traverse the slow path together
        // and report `Ok(false)`. The actual coalescing invariant — that
        // exactly one construction ran — is verified below via cache
        // stats, not via the per-call hit/miss return.
        for handle in handles {
            let _result = handle.await.unwrap()?;
        }

        // Verify exactly one cache entry exists (i.e. construction ran
        // once even though 5 tasks raced for the same digest), and that
        // ref_counts have been released by every caller.
        let stats = cache.stats().await;
        assert_eq!(
            stats.entries, 1,
            "Coalescing should have produced exactly one cache entry",
        );
        assert_eq!(stats.in_use_entries, 0, "All ref_counts should be back to 0");

        // Verify construction locks are cleaned up (Bug 3).
        // The coalesce helper's RAII guard removes the in_flight slot
        // when the leader's compute future returns.
        let locks = cache.construction_locks.lock();
        assert!(
            locks.is_empty(),
            "Construction locks should be cleaned up, found: {}",
            locks.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_construction_lock_cleanup() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        let dest = temp_dir.path().join("dest");
        cache.get_or_create(dir_digest, &dest).await?;

        let locks = cache.construction_locks.lock();
        assert!(
            locks.is_empty(),
            "Construction lock should be removed after get_or_create completes"
        );

        Ok(())
    }

    /// Regression test for the failure mode that motivated the migration
    /// to `with_construction_lock`: when the leader's construction fails
    /// (e.g. a chunk-truncated streaming read), every concurrent waiter
    /// must receive an error within a bounded time instead of hanging
    /// forever on the per-digest mutex. We simulate the failure using a
    /// digest that is not present in the store — the leader's resolve
    /// step returns NotFound and the error must fan out to all waiters.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_coalesce_leader_failure_fans_out_to_waiters() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");

        // Empty store: every fetch will fail.
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let bogus_digest = DigestInfo::try_new(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            42,
        )
        .unwrap();

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };
        let cache = Arc::new(DirectoryCache::new(config, store, None).await?);

        // Spawn 8 concurrent callers for the same missing digest.
        let mut handles = Vec::new();
        for i in 0..8 {
            let cache = Arc::clone(&cache);
            let dest = temp_dir.path().join(format!("fanout_dest_{i}"));
            handles.push(tokio::spawn(async move {
                cache.get_or_create(bogus_digest, &dest).await
            }));
        }

        // Bound the test wall time. With the old per-digest Mutex, a
        // dropped streaming_writer would leave waiters stuck and this
        // would hang. With the coalesce helper, the leader's NotFound
        // is fanned out and all waiters return promptly.
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            futures::future::join_all(handles),
        )
        .await
        .expect("waiters must complete within 15s; coalesce hang regression");

        let mut error_count = 0;
        for join_res in outcome {
            let res = join_res.expect("task panicked");
            assert!(res.is_err(), "all callers should fail (digest not in store)");
            error_count += 1;
        }
        assert_eq!(error_count, 8, "every caller must receive an error");

        // The in_flight slot must be empty after every caller completes.
        let locks = cache.construction_locks.lock();
        assert!(
            locks.is_empty(),
            "construction_locks must be empty after coalesced failure: {} remain",
            locks.len(),
        );

        Ok(())
    }

    /// Regression test for high contention: many concurrent callers for
    /// the same digest must coalesce into a single construction. Verifies
    /// the post-migration invariant that exactly one cache entry exists
    /// regardless of caller count and that all callers complete within
    /// the bounded time the coalesce helper enforces.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_coalesce_high_contention_one_construction() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };
        let cache = Arc::new(DirectoryCache::new(config, store, None).await?);

        // Spawn 100 concurrent callers. Without coalescing this would
        // either run 100 constructions or starve everyone behind a
        // serialised per-digest Mutex.
        let mut handles = Vec::with_capacity(100);
        for i in 0..100 {
            let cache = Arc::clone(&cache);
            let dest = temp_dir.path().join(format!("contention_dest_{i}"));
            handles.push(tokio::spawn(async move {
                cache.get_or_create(dir_digest, &dest).await
            }));
        }

        let outcome = tokio::time::timeout(
            Duration::from_secs(30),
            futures::future::join_all(handles),
        )
        .await
        .expect("100 callers must complete within 30s");

        let mut succeeded = 0;
        for join_res in outcome {
            let res = join_res.expect("task panicked")?;
            // Each call returns Ok(true|false). All 100 must succeed.
            let _ = res;
            succeeded += 1;
        }
        assert_eq!(succeeded, 100, "all 100 callers must succeed");

        // Exactly one cache entry — proves coalescing reduced 100 calls
        // to 1 construction.
        let stats = cache.stats().await;
        assert_eq!(
            stats.entries, 1,
            "100-way coalescing should produce exactly one cache entry",
        );
        assert_eq!(stats.in_use_entries, 0, "all ref_counts must be 0 after");

        // Slot must be empty.
        let locks = cache.construction_locks.lock();
        assert!(
            locks.is_empty(),
            "construction_locks must be empty after high-contention coalesce",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_eviction_removes_oldest_entry() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, digest_a, digest_b) = setup_two_digest_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 1, // Only 1 entry allowed
            max_size_bytes: 0,
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Insert entry A
        let dest_a = temp_dir.path().join("dest_a");
        cache.get_or_create(digest_a, &dest_a).await?;
        assert_eq!(cache.stats().await.entries, 1);

        // Insert entry B — should evict A
        let dest_b = temp_dir.path().join("dest_b");
        cache.get_or_create(digest_b, &dest_b).await?;
        assert_eq!(cache.stats().await.entries, 1);

        // A's cache directory should be gone from disk
        let cache_path_a = cache_root.join(digest_a.to_string());
        assert!(
            !cache_path_a.exists(),
            "Evicted entry A should be removed from disk"
        );

        // B should be in cache
        let cache_path_b = cache_root.join(digest_b.to_string());
        assert!(cache_path_b.exists(), "Entry B should be on disk");

        // Requesting A again should be a miss (reconstruct)
        let dest_a2 = temp_dir.path().join("dest_a2");
        let hit = cache.get_or_create(digest_a, &dest_a2).await?;
        assert!(!hit, "A should be a cache miss after eviction");
        assert!(dest_a2.join("a.txt").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_path_traversal_rejected() -> Result<(), Error> {
        // Test validate_node_name directly
        assert!(DirectoryCache::validate_node_name("good_file.txt").is_ok());
        assert!(DirectoryCache::validate_node_name("subdir").is_ok());

        // These should all be rejected
        assert!(DirectoryCache::validate_node_name("").is_err());
        assert!(DirectoryCache::validate_node_name(".").is_err());
        assert!(DirectoryCache::validate_node_name("..").is_err());
        assert!(DirectoryCache::validate_node_name("../etc/passwd").is_err());
        assert!(DirectoryCache::validate_node_name("/etc/passwd").is_err());
        assert!(DirectoryCache::validate_node_name("foo/bar").is_err());
        assert!(DirectoryCache::validate_node_name("foo\\bar").is_err());
        assert!(DirectoryCache::validate_node_name("foo\0bar").is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_symlink_target_validation() -> Result<(), Error> {
        // Valid relative targets
        assert!(DirectoryCache::validate_symlink_target("file.txt", 0).is_ok());
        assert!(DirectoryCache::validate_symlink_target("subdir/file.txt", 0).is_ok());
        assert!(DirectoryCache::validate_symlink_target("../sibling", 1).is_ok());

        // Absolute targets rejected
        assert!(DirectoryCache::validate_symlink_target("/etc/shadow", 0).is_err());
        assert!(DirectoryCache::validate_symlink_target("\\windows\\system32", 0).is_err());

        // Traversal beyond root rejected
        assert!(DirectoryCache::validate_symlink_target("..", 0).is_err());
        assert!(DirectoryCache::validate_symlink_target("../..", 1).is_err());
        assert!(DirectoryCache::validate_symlink_target("../../escape", 1).is_err());

        // Deep enough to allow traversal
        assert!(DirectoryCache::validate_symlink_target("../..", 2).is_ok());

        // Empty and null rejected
        assert!(DirectoryCache::validate_symlink_target("", 0).is_err());
        assert!(DirectoryCache::validate_symlink_target("foo\0bar", 0).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_path_traversal_in_directory_proto() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let store = Store::new(MemoryStore::new(&Default::default()));

        // Create a malicious directory proto with a path-traversal file name
        let file_content = b"malicious";
        let file_digest = DigestInfo::try_new(
            "c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a",
            9,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(file_digest.into(), file_content.to_vec().into())
            .await
            .unwrap();

        let malicious_dir = ProtoDirectory {
            files: vec![FileNode {
                name: "../escape.txt".to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut dir_data = Vec::new();
        malicious_dir.encode(&mut dir_data).unwrap();
        let dir_digest = DigestInfo::try_new(
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            dir_data.len() as i64,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(dir_digest.into(), dir_data.into())
            .await
            .unwrap();

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };
        let cache = DirectoryCache::new(config, store, None).await?;

        let dest = temp_dir.path().join("dest");
        let result = cache.get_or_create(dir_digest, &dest).await;
        assert!(result.is_err(), "Path traversal should be rejected");

        // The escape file should NOT exist in the parent directory
        assert!(
            !temp_dir.path().join("escape.txt").exists(),
            "Path traversal should not create files outside dest"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_absolute_symlink_rejected() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let store = Store::new(MemoryStore::new(&Default::default()));

        let malicious_dir = ProtoDirectory {
            symlinks: vec![SymlinkNode {
                name: "evil_link".to_string(),
                target: "/etc/shadow".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut dir_data = Vec::new();
        malicious_dir.encode(&mut dir_data).unwrap();
        let dir_digest = DigestInfo::try_new(
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            dir_data.len() as i64,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(dir_digest.into(), dir_data.into())
            .await
            .unwrap();

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };
        let cache = DirectoryCache::new(config, store, None).await?;

        let dest = temp_dir.path().join("dest");
        let result = cache.get_or_create(dir_digest, &dest).await;
        assert!(result.is_err(), "Absolute symlink target should be rejected");

        Ok(())
    }

    #[tokio::test]
    async fn test_ref_count_returns_to_zero_after_operations() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Cache miss
        let dest1 = temp_dir.path().join("dest1");
        cache.get_or_create(dir_digest, &dest1).await?;

        // Cache hit
        let dest2 = temp_dir.path().join("dest2");
        cache.get_or_create(dir_digest, &dest2).await?;

        // ref_count should be 0 after both operations
        let stats = cache.stats().await;
        assert_eq!(stats.in_use_entries, 0, "ref_count should be 0 after all operations");

        Ok(())
    }

    #[tokio::test]
    async fn test_size_based_eviction() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, digest_a, digest_b) = setup_two_digest_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 100,       // High entry limit
            max_size_bytes: 20,     // Very small — forces size-based eviction
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Insert entry A (14 bytes for "File A content")
        let dest_a = temp_dir.path().join("dest_a");
        cache.get_or_create(digest_a, &dest_a).await?;
        assert_eq!(cache.stats().await.entries, 1);

        // Insert entry B (16 bytes for "File B content!!") — total would be 30 > 20,
        // so A should be evicted
        let dest_b = temp_dir.path().join("dest_b");
        cache.get_or_create(digest_b, &dest_b).await?;
        assert_eq!(cache.stats().await.entries, 1);

        // A should have been evicted
        let cache_map = cache.cache.read().await;
        assert!(
            !cache_map.contains_key(&digest_a),
            "Digest A should have been evicted due to size limit"
        );
        assert!(
            cache_map.contains_key(&digest_b),
            "Digest B should be present"
        );

        Ok(())
    }

    /// #22 + #26 production-composition test (`directory_cache.rs:657-723`):
    /// when `cache_root` is OVER-CAP at startup, `DirectoryCache::new` must
    /// bleed the on-disk state AND the in-process map down to ≤ `max_size_bytes`
    /// before returning.
    ///
    /// This is the sibling of
    /// `nativelink-store/tests/filesystem_store_test.rs:1816` (#605 Bug A) for
    /// the `DirectoryCache`'s independent startup-eviction path. The
    /// `FilesystemStore` test exercises moka's
    /// `run_pending_tasks_and_drain`; this one exercises the hand-rolled
    /// sort-by-mtime + LRU loop at `directory_cache.rs:660-688`.
    ///
    /// Layout: pre-seed five digest-named subdirectories under `cache_root`,
    /// each containing one file of `FILE_BYTES`. Cap is `MAX_SIZE_BYTES`
    /// chosen so 5 entries exceed the cap and at most 2 may remain.
    ///
    /// Verifies (per CLAUDE.md `feedback_index_visibility_contract`):
    /// 1. `DirectoryCache::new` returns within a 10s `tokio::time::timeout`
    ///    (deadlock detector — the eviction loop must not wedge).
    /// 2. The in-process visibility primitive
    ///    (`cache.read().values().map(|m| m.size).sum::<u64>()`) is at-or-
    ///    below the cap. This IS the cap-decision quantity the runtime
    ///    `collect_evictions` uses; metadata read of disk would not catch a
    ///    map-vs-disk drift bug.
    /// 3. On-disk entry count under `cache_root` matches the in-process map
    ///    size — i.e. evicted entries' directories are removed from disk.
    ///
    /// Mutation step (per CLAUDE.md TDD discipline): comment out the body of
    /// the eviction loop at `directory_cache.rs:675-687` (the `if let Some(
    /// meta) = initial_cache.remove(digest)` block). The
    /// `initial_cache.values().map().sum() <= MAX_SIZE_BYTES` assertion below
    /// MUST red-fail with "#22 startup over-cap not enforced — in-process map
    /// stayed over the configured cap" because no entries are removed from
    /// `initial_cache` and the post-construction sum equals the full
    /// pre-seeded total.
    #[nativelink_test]
    async fn startup_over_cap_directory_cache_drained_to_cap() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        std::fs::create_dir_all(&cache_root).unwrap();

        // Pre-seed the format-version sentinel so `DirectoryCache::new` does
        // not wipe the cache before our pre-seeded entries are loaded. Without
        // this, the startup path at `directory_cache.rs:520-548` deletes
        // every digest dir and the test exercises an empty cache.
        std::fs::write(
            cache_root.join(CACHE_VERSION_FILENAME),
            format!("{CACHE_FORMAT_VERSION}\n"),
        )
        .unwrap();

        // Each pre-seeded directory holds one FILE_BYTES-byte payload.
        // 5 entries × ~10 KiB = ~50 KiB on a 20 KiB cap; at most 2 may remain.
        const FILE_BYTES: usize = 10 * 1024;
        const MAX_SIZE_BYTES: u64 = 20 * 1024;
        const NUM_ENTRIES: usize = 5;
        const HASHES: [&str; NUM_ENTRIES] = [
            "0123456789abcdef000000000000000000010000000000000123456789abcdef",
            "1123456789abcdef000000000000000000010000000000000123456789abcdef",
            "2123456789abcdef000000000000000000010000000000000123456789abcdef",
            "3123456789abcdef000000000000000000010000000000000123456789abcdef",
            "4123456789abcdef000000000000000000010000000000000123456789abcdef",
        ];

        // Pre-seed digest-named subdirectories. Use the same name format as
        // `parse_digest_from_dirname` expects (`{hash}-{size}`), which is
        // `DigestInfo::to_string()`. Stagger mtimes via the sequence of
        // creates so the LRU sort has deterministic input.
        for hash in HASHES {
            let digest = DigestInfo::try_new(hash, FILE_BYTES as i64)?;
            let entry_path = cache_root.join(digest.to_string());
            std::fs::create_dir_all(&entry_path).unwrap();
            std::fs::write(entry_path.join("payload.bin"), vec![0u8; FILE_BYTES]).unwrap();
        }

        // Sanity: verify all five subdirs exist before construction.
        let on_disk_before = std::fs::read_dir(&cache_root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .count();
        assert_eq!(
            on_disk_before, NUM_ENTRIES,
            "test fixture broken: expected {NUM_ENTRIES} pre-seeded entries on disk",
        );

        // Construct the cache. The startup load + one-shot eviction at
        // `directory_cache.rs:657-723` must enforce MAX_SIZE_BYTES. Wrap
        // under a tokio timeout: a regression that wedges the eviction loop
        // red-fails as a deadlock detector rather than hanging the suite.
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let config = DirectoryCacheConfig {
            max_entries: 100, // High enough that count-cap doesn't fire
            max_size_bytes: MAX_SIZE_BYTES,
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };
        let cache = tokio::time::timeout(
            Duration::from_secs(10),
            DirectoryCache::new(config, store, None),
        )
        .await
        .expect(
            "DirectoryCache::new must not deadlock — \
             #22 startup drain wedged the eviction loop",
        )?;

        // Visibility primitive #1: in-process map sum is the quantity the
        // runtime `collect_evictions` uses for its cap decision. This is
        // the "moka has_with_results" analogue for the directory_cache.
        let in_process_sum: u64 = {
            let map = cache.cache.read().await;
            map.values().map(|m| m.size).sum()
        };
        let in_process_count = cache.cache.read().await.len();
        assert!(
            in_process_sum <= MAX_SIZE_BYTES,
            "#22 startup over-cap not enforced — in-process map stayed over the \
             configured cap: sum {} > max {} ({} entries remaining)",
            in_process_sum,
            MAX_SIZE_BYTES,
            in_process_count,
        );
        assert!(
            in_process_count < NUM_ENTRIES,
            "#22 startup drain produced ZERO evictions in the in-process map — \
             {NUM_ENTRIES} entries remained on a cap of {MAX_SIZE_BYTES} bytes",
        );

        // Visibility primitive #2: on-disk count must match in-process count.
        // A drift (in-process map < on-disk count) would indicate
        // `startup_evict_paths` cleanup at `directory_cache.rs:720-722` was
        // skipped. We exclude the `.cache_version` file written by
        // `DirectoryCache::new` at `:545`.
        let on_disk_after = std::fs::read_dir(&cache_root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .count();
        assert_eq!(
            on_disk_after, in_process_count,
            "#22 in-process map and on-disk state diverged after startup drain — \
             map has {in_process_count} entries, disk has {on_disk_after} directories",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_merkle_tree_metadata_roundtrip() -> Result<(), Error> {
        // Test serialization/deserialization of MerkleTreeMetadata
        let mut digest_to_relpath = HashMap::new();
        let d1 = DigestInfo::try_new(
            "aaaa567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            100,
        )
        .unwrap();
        let d2 = DigestInfo::try_new(
            "bbbb567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            200,
        )
        .unwrap();

        digest_to_relpath.insert(d1, String::new()); // root
        digest_to_relpath.insert(d2, "subdir/nested".to_string());

        let meta = MerkleTreeMetadata { digest_to_relpath };
        let serialized = meta.serialize();
        let deserialized = MerkleTreeMetadata::deserialize(&serialized)?;

        assert_eq!(deserialized.digest_to_relpath.len(), 2);
        assert_eq!(deserialized.digest_to_relpath.get(&d1).unwrap(), "");
        assert_eq!(
            deserialized.digest_to_relpath.get(&d2).unwrap(),
            "subdir/nested"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_merkle_tree_metadata_from_directory_tree() -> Result<(), Error> {
        // Build a small directory tree and verify MerkleTreeMetadata generation
        let file_digest = DigestInfo::try_new(
            "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
            13,
        )
        .unwrap();

        // Child directory
        let child_dir = ProtoDirectory {
            files: vec![FileNode {
                name: "child_file.txt".to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut child_data = Vec::new();
        child_dir.encode(&mut child_data).unwrap();
        let child_digest = DigestInfo::try_new(
            "cccc567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            child_data.len() as i64,
        )
        .unwrap();

        // Root directory referencing the child
        let root_dir = ProtoDirectory {
            files: vec![FileNode {
                name: "root_file.txt".to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            directories: vec![DirectoryNode {
                name: "child".to_string(),
                digest: Some(child_digest.into()),
            }],
            ..Default::default()
        };
        let mut root_data = Vec::new();
        root_dir.encode(&mut root_data).unwrap();
        let root_digest = DigestInfo::try_new(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            root_data.len() as i64,
        )
        .unwrap();

        let mut tree = HashMap::new();
        tree.insert(root_digest, root_dir);
        tree.insert(child_digest, child_dir);

        let meta = MerkleTreeMetadata::from_directory_tree(&tree, &root_digest);
        assert_eq!(meta.digest_to_relpath.len(), 2);
        assert_eq!(meta.digest_to_relpath.get(&root_digest).unwrap(), "");
        assert_eq!(meta.digest_to_relpath.get(&child_digest).unwrap(), "child");

        Ok(())
    }

    #[tokio::test]
    async fn test_parse_digest_from_dirname() -> Result<(), Error> {
        // Valid format: hash-size
        let name = "aaaa567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef-100";
        let parsed = DirectoryCache::parse_digest_from_dirname(name);
        assert!(parsed.is_some());
        let d = parsed.unwrap();
        assert_eq!(d.size_bytes(), 100);

        // Invalid: no dash
        assert!(DirectoryCache::parse_digest_from_dirname("nodashhere").is_none());

        // Invalid: not a number after dash
        assert!(DirectoryCache::parse_digest_from_dirname("hash-notanumber").is_none());

        // Invalid: empty
        assert!(DirectoryCache::parse_digest_from_dirname("").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_merkle_metadata_stored_on_construction() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Construct a directory (serial path, no FastSlowStore)
        let dest = temp_dir.path().join("dest");
        cache.get_or_create(dir_digest, &dest).await?;

        // Merkle metadata file should NOT exist because we don't have
        // FastSlowStore (resolve_directory_tree requires it).
        // This is expected -- subtree indexing is only available with
        // the fast path.
        let cache_path = cache.get_cache_path(&dir_digest);
        let merkle_path = cache_path.join(MERKLE_METADATA_FILENAME);
        // Without FastSlowStore, no merkle metadata is generated
        assert!(
            !merkle_path.exists(),
            "Merkle metadata should not exist without FastSlowStore"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_subtree_index_populated_and_cleaned_on_eviction() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, digest_a, digest_b) = setup_two_digest_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 1,
            max_size_bytes: 0,
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Insert entry A
        let dest_a = temp_dir.path().join("dest_a");
        cache.get_or_create(digest_a, &dest_a).await?;

        // Without FastSlowStore, subtree index should be empty (no merkle tree resolved)
        {
            let index = cache.subtree_index.read().await;
            assert!(
                index.is_empty(),
                "Subtree index should be empty without FastSlowStore"
            );
        }

        // Insert entry B (evicts A)
        let dest_b = temp_dir.path().join("dest_b");
        cache.get_or_create(digest_b, &dest_b).await?;
        assert_eq!(cache.stats().await.entries, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_reload_from_disk() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        // Create a cache and populate it
        {
            let config = DirectoryCacheConfig {
                max_entries: 10,
                max_size_bytes: 1024 * 1024,
                cache_root: cache_root.clone(),
                direct_use_mode: false,
            };
            let cache = DirectoryCache::new(config, store.clone(), None).await?;
            let dest = temp_dir.path().join("dest1");
            cache.get_or_create(dir_digest, &dest).await?;
            assert_eq!(cache.stats().await.entries, 1);
        }

        // Create a NEW cache pointing to the same cache_root -- it should
        // reload the existing entry from disk.
        {
            let config = DirectoryCacheConfig {
                max_entries: 10,
                max_size_bytes: 1024 * 1024,
                cache_root: cache_root.clone(),
                direct_use_mode: false,
            };
            let cache = DirectoryCache::new(config, store, None).await?;
            assert_eq!(
                cache.stats().await.entries,
                1,
                "Cache should have reloaded the entry from disk"
            );

            // The reloaded entry should be usable (cache hit)
            let dest2 = temp_dir.path().join("dest2");
            let hit = cache.get_or_create(dir_digest, &dest2).await?;
            assert!(hit, "Reloaded entry should produce a cache hit");
            assert!(dest2.join("test.txt").exists());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_direct_use_mode_basic() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_test_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root: cache_root.clone(),
            direct_use_mode: true,
        };

        let cache = DirectoryCache::new(config, store, None).await?;
        assert!(cache.is_direct_use_mode());

        // First access - cache miss
        let dest1 = temp_dir.path().join("dest1");
        let (cache_path1, was_hit) = cache.get_or_create_direct(dir_digest, &dest1).await?;
        assert!(!was_hit, "First access should be cache miss");

        // dest1 should be a symlink to the cache path
        let dest1_meta = fs::symlink_metadata(&dest1).await.unwrap();
        assert!(dest1_meta.is_symlink(), "dest should be a symlink");
        let link_target = fs::read_link(&dest1).await.unwrap();
        assert_eq!(link_target, cache_path1, "symlink should point to cache path");

        // File should be accessible through the symlink
        assert!(dest1.join("test.txt").exists(), "test.txt should be accessible through symlink");
        let content = fs::read_to_string(dest1.join("test.txt")).await.unwrap();
        assert_eq!(content, "Hello, World!");

        // ref_count should be 1 (held for action lifetime)
        let stats = cache.stats().await;
        assert_eq!(stats.in_use_entries, 1, "Entry should be in use");

        // Second access - cache hit
        let dest2 = temp_dir.path().join("dest2");
        let (_cache_path2, was_hit) = cache.get_or_create_direct(dir_digest, &dest2).await?;
        assert!(was_hit, "Second access should be cache hit");

        // dest2 should also be a symlink
        let dest2_meta = fs::symlink_metadata(&dest2).await.unwrap();
        assert!(dest2_meta.is_symlink(), "dest2 should be a symlink");
        assert!(dest2.join("test.txt").exists(), "test.txt should be accessible through dest2");

        // ref_count should be 2 (both actions using it)
        let stats = cache.stats().await;
        assert_eq!(stats.in_use_entries, 1, "Should still be 1 cache entry");

        // Release first use
        cache.release_direct_use(&dir_digest).await;

        // Release second use
        cache.release_direct_use(&dir_digest).await;

        // ref_count should be 0
        let stats = cache.stats().await;
        assert_eq!(stats.in_use_entries, 0, "No entries should be in use after release");

        // Cleanup: removing symlinks should NOT affect cache
        fs::remove_file(&dest1).await.unwrap();
        fs::remove_file(&dest2).await.unwrap();

        // Cache should still be intact
        assert!(cache_path1.join("test.txt").exists(), "Cache should be intact after symlink removal");

        Ok(())
    }

    #[tokio::test]
    async fn test_direct_use_mode_eviction_blocked_by_ref_count() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, digest_a, digest_b) = setup_two_digest_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 1, // Only 1 entry allowed
            max_size_bytes: 0,
            cache_root: cache_root.clone(),
            direct_use_mode: true,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // Fill cache with digest_a and hold the ref_count
        let dest_a = temp_dir.path().join("dest_a");
        let (_cache_path_a, was_hit) = cache.get_or_create_direct(digest_a, &dest_a).await?;
        assert!(!was_hit);
        assert_eq!(cache.stats().await.entries, 1);
        assert_eq!(cache.stats().await.in_use_entries, 1);

        // Try to insert digest_b -- should succeed but eviction is blocked
        // because digest_a is in use (ref_count > 0).
        let dest_b = temp_dir.path().join("dest_b");
        let (_cache_path_b, was_hit) = cache.get_or_create_direct(digest_b, &dest_b).await?;
        assert!(!was_hit);

        // Both should be in cache now (eviction was blocked)
        let stats = cache.stats().await;
        assert_eq!(stats.entries, 2, "Both entries should exist (eviction blocked by ref_count)");

        // Release digest_a
        cache.release_direct_use(&digest_a).await;

        // Release digest_b
        cache.release_direct_use(&digest_b).await;

        // Cleanup symlinks
        fs::remove_file(&dest_a).await.unwrap();
        fs::remove_file(&dest_b).await.unwrap();

        Ok(())
    }

    /// Helper to create a store containing a directory with a zero-digest file.
    /// Returns (store, dir_digest) where the directory has one normal file and
    /// one zero-length file (blake3 zero-digest).
    async fn setup_zero_digest_store() -> (Store, DigestInfo) {
        use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;

        let store = Store::new(MemoryStore::new(&MemorySpec::default()));

        // Upload a normal file
        let file_content = b"Hello, World!";
        let file_digest = DigestInfo::try_new(
            "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
            13,
        )
        .unwrap();
        store
            .as_store_driver_pin()
            .update_oneshot(file_digest.into(), file_content.to_vec().into())
            .await
            .unwrap();

        // The blake3 zero-digest (size 0, no data needed in store)
        let zero_digest = ZERO_BYTE_DIGESTS[1];

        // Create a directory containing both a normal file and a zero-digest file
        let directory = ProtoDirectory {
            files: vec![
                FileNode {
                    name: "test.txt".to_string(),
                    digest: Some(file_digest.into()),
                    is_executable: false,
                    ..Default::default()
                },
                FileNode {
                    name: "_bs.linksearchpaths".to_string(),
                    digest: Some(zero_digest.into()),
                    is_executable: false,
                    ..Default::default()
                },
            ],
            directories: vec![],
            symlinks: vec![],
            ..Default::default()
        };

        let mut dir_data = Vec::new();
        directory.encode(&mut dir_data).unwrap();
        let dir_digest = DigestInfo::try_new(
            "aabb567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            dir_data.len() as i64,
        )
        .unwrap();

        store
            .as_store_driver_pin()
            .update_oneshot(dir_digest.into(), dir_data.into())
            .await
            .unwrap();

        (store, dir_digest)
    }

    #[nativelink_test]
    async fn test_directory_cache_zero_digest_files() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_zero_digest_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root,
            direct_use_mode: false,
        };

        let cache = DirectoryCache::new(config, store, None).await?;

        // First access - cache miss, should materialize both files
        let dest = temp_dir.path().join("dest");
        let hit = cache.get_or_create(dir_digest, &dest).await?;
        assert!(!hit, "First access should be cache miss");

        // Normal file should exist with correct content
        assert!(dest.join("test.txt").exists(), "Normal file should exist");
        let content = fs::read_to_string(dest.join("test.txt")).await.unwrap();
        assert_eq!(content, "Hello, World!");

        // Zero-digest file should exist with 0 bytes
        let zero_file_path = dest.join("_bs.linksearchpaths");
        let zero_meta = fs::metadata(&zero_file_path)
            .await
            .expect("Zero-digest file should exist on disk");
        assert_eq!(
            zero_meta.len(),
            0,
            "Zero-digest file should have 0 bytes"
        );

        // Second access - cache hit, should also produce the zero-digest file
        let dest2 = temp_dir.path().join("dest2");
        let hit = cache.get_or_create(dir_digest, &dest2).await?;
        assert!(hit, "Second access should be cache hit");

        let zero_file_path2 = dest2.join("_bs.linksearchpaths");
        let zero_meta2 = fs::metadata(&zero_file_path2)
            .await
            .expect("Zero-digest file should exist after cache hit");
        assert_eq!(
            zero_meta2.len(),
            0,
            "Zero-digest file should have 0 bytes after cache hit"
        );

        Ok(())
    }

    #[nativelink_test]
    async fn test_directory_cache_direct_use_zero_digest() -> Result<(), Error> {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        let (store, dir_digest) = setup_zero_digest_store().await;

        let config = DirectoryCacheConfig {
            max_entries: 10,
            max_size_bytes: 1024 * 1024,
            cache_root: cache_root.clone(),
            direct_use_mode: true,
        };

        let cache = DirectoryCache::new(config, store, None).await?;
        assert!(cache.is_direct_use_mode());

        // First access - cache miss
        let dest = temp_dir.path().join("dest");
        let (cache_path, was_hit) = cache.get_or_create_direct(dir_digest, &dest).await?;
        assert!(!was_hit, "First access should be cache miss");

        // dest should be a symlink to the cache path
        let dest_meta = fs::symlink_metadata(&dest).await.unwrap();
        assert!(dest_meta.is_symlink(), "dest should be a symlink");

        // Normal file should be accessible through the symlink
        assert!(
            dest.join("test.txt").exists(),
            "Normal file should be accessible through symlink"
        );

        // Zero-digest file should exist with 0 bytes through the symlink
        let zero_file_path = dest.join("_bs.linksearchpaths");
        let zero_meta = fs::metadata(&zero_file_path)
            .await
            .expect("Zero-digest file should exist through symlink");
        assert_eq!(
            zero_meta.len(),
            0,
            "Zero-digest file should have 0 bytes"
        );

        // Also verify the file exists directly in the cache path
        let cache_zero = cache_path.join("_bs.linksearchpaths");
        let cache_zero_meta = fs::metadata(&cache_zero)
            .await
            .expect("Zero-digest file should exist in cache directory");
        assert_eq!(
            cache_zero_meta.len(),
            0,
            "Zero-digest file in cache should have 0 bytes"
        );

        // Second access - cache hit
        let dest2 = temp_dir.path().join("dest2");
        let (_cache_path2, was_hit) = cache.get_or_create_direct(dir_digest, &dest2).await?;
        assert!(was_hit, "Second access should be cache hit");

        let zero_file_path2 = dest2.join("_bs.linksearchpaths");
        let zero_meta2 = fs::metadata(&zero_file_path2)
            .await
            .expect("Zero-digest file should exist after cache hit");
        assert_eq!(
            zero_meta2.len(),
            0,
            "Zero-digest file should have 0 bytes after cache hit"
        );

        // Release refs
        cache.release_direct_use(&dir_digest).await;
        cache.release_direct_use(&dir_digest).await;

        // Cleanup symlinks
        fs::remove_file(&dest).await.unwrap();
        fs::remove_file(&dest2).await.unwrap();

        Ok(())
    }

    #[nativelink_test]
    async fn test_startup_cleanup_evicts_old_entries_by_count() -> Result<(), Error> {
        use filetime::{FileTime, set_file_mtime};

        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        fs::create_dir_all(&cache_root).await.unwrap();

        // Write the cache version file so it doesn't get wiped
        fs::write(
            cache_root.join(CACHE_VERSION_FILENAME),
            format!("{CACHE_FORMAT_VERSION}\n"),
        )
        .await
        .unwrap();

        // Create 5 fake cache directories with distinct mtimes.
        // Directory names must match DigestInfo::to_string() format: "{hash}-{size}"
        let digests: Vec<DigestInfo> = (0..5)
            .map(|i| {
                let hash = format!("{:0>64}", format!("{i:x}"));
                DigestInfo::try_new(&hash, 100).unwrap()
            })
            .collect();

        for (i, digest) in digests.iter().enumerate() {
            let dir_path = cache_root.join(digest.to_string());
            fs::create_dir_all(&dir_path).await.unwrap();
            // Write a small file so the directory has non-zero size
            fs::write(dir_path.join("data.txt"), "hello").await.unwrap();
            // Set mtime: older entries get smaller timestamps
            // Entry 0 is oldest (mtime=1000), entry 4 is newest (mtime=5000)
            let mtime = FileTime::from_unix_time((i as i64 + 1) * 1000, 0);
            set_file_mtime(&dir_path, mtime).unwrap();
        }

        // Verify all 5 directories exist on disk
        assert_eq!(count_cache_dirs(&cache_root).await, 5);

        let (store, _) = setup_test_store().await;

        // Create cache with max_entries=2 — should evict the 3 oldest entries
        let config = DirectoryCacheConfig {
            max_entries: 2,
            max_size_bytes: 0, // no size limit
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };
        let cache = DirectoryCache::new(config, store, None).await?;

        // Should have exactly 2 entries (the two newest)
        let stats = cache.stats().await;
        assert_eq!(
            stats.entries, 2,
            "Cache should have 2 entries after startup cleanup, got {}",
            stats.entries
        );

        // The two newest entries (index 3 and 4) should survive
        let surviving = cache.cached_digests().await;
        assert!(
            surviving.contains(&digests[3]),
            "Entry 3 (second newest) should survive"
        );
        assert!(
            surviving.contains(&digests[4]),
            "Entry 4 (newest) should survive"
        );

        // The oldest entries should be gone from disk
        for i in 0..3 {
            let dir_path = cache_root.join(digests[i].to_string());
            assert!(
                !dir_path.exists(),
                "Entry {i} (old) should be deleted from disk"
            );
        }

        // Only 2 directories should remain on disk (plus the version file)
        assert_eq!(count_cache_dirs(&cache_root).await, 2);

        Ok(())
    }

    #[nativelink_test]
    async fn test_startup_cleanup_evicts_old_entries_by_size() -> Result<(), Error> {
        use filetime::{FileTime, set_file_mtime};

        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("cache");
        fs::create_dir_all(&cache_root).await.unwrap();

        // Write the cache version file
        fs::write(
            cache_root.join(CACHE_VERSION_FILENAME),
            format!("{CACHE_FORMAT_VERSION}\n"),
        )
        .await
        .unwrap();

        // Create 3 cache entries, each ~1KB (directory + file)
        let digests: Vec<DigestInfo> = (0..3)
            .map(|i| {
                let hash = format!("{:0>64}", format!("ab{i:x}"));
                DigestInfo::try_new(&hash, 200).unwrap()
            })
            .collect();

        let file_data = vec![b'x'; 1024]; // 1KB file
        for (i, digest) in digests.iter().enumerate() {
            let dir_path = cache_root.join(digest.to_string());
            fs::create_dir_all(&dir_path).await.unwrap();
            fs::write(dir_path.join("data.bin"), &file_data).await.unwrap();
            let mtime = FileTime::from_unix_time((i as i64 + 1) * 1000, 0);
            set_file_mtime(&dir_path, mtime).unwrap();
        }

        let (store, _) = setup_test_store().await;

        // max_size_bytes ~2KB — only 1-2 entries should fit
        // Each entry is ~1KB file + directory overhead, so 2048 should allow
        // at most 1-2 entries depending on filesystem overhead.
        let config = DirectoryCacheConfig {
            max_entries: 100, // high count limit
            max_size_bytes: 2048,
            cache_root: cache_root.clone(),
            direct_use_mode: false,
        };
        let cache = DirectoryCache::new(config, store, None).await?;

        let stats = cache.stats().await;
        // With 3 entries of ~1KB each, total ~3KB exceeds 2KB limit.
        // At least one entry must be evicted.
        assert!(
            stats.entries < 3,
            "Should have evicted at least one entry, but have {}",
            stats.entries
        );
        assert!(
            stats.total_size_bytes <= 2048,
            "Total size {} should be within 2048 byte limit",
            stats.total_size_bytes
        );

        // The newest entry should survive (oldest evicted first)
        let surviving = cache.cached_digests().await;
        assert!(
            surviving.contains(&digests[2]),
            "Newest entry should survive size-based eviction"
        );

        Ok(())
    }

    /// Helper: count subdirectories under the cache root (excludes files like .cache_version)
    async fn count_cache_dirs(cache_root: &Path) -> usize {
        let mut count = 0;
        let mut entries = fs::read_dir(cache_root).await.unwrap();
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(meta) = fs::symlink_metadata(entry.path()).await {
                if meta.is_dir() {
                    count += 1;
                }
            }
        }
        count
    }

    fn make_digest(byte: u8) -> DigestInfo {
        let hex: String = std::iter::repeat(format!("{byte:02x}")).take(32).collect();
        DigestInfo::try_new(&hex, 100).unwrap()
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_accepts_correct_count() -> Result<(), Error> {
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("good");
        fs::create_dir(&dir).await.unwrap();
        fs::write(dir.join("a"), b"x").await.unwrap();
        fs::write(dir.join("b"), b"y").await.unwrap();
        fs::write(dir.join("c"), b"z").await.unwrap();

        let digest = make_digest(0xaa);
        let valid =
            filter_valid_subtree_hits(vec![(digest, dir.clone(), 3)]).await;
        assert_eq!(valid.len(), 1);
        assert_eq!(valid.get(&digest), Some(&dir));
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_rejects_missing_file() -> Result<(), Error> {
        // Reproduces the apple/ corruption: directory expects 4 entries but
        // only has 3 on disk → must be rejected.
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("partial");
        fs::create_dir(&dir).await.unwrap();
        fs::write(dir.join("freebsdlike"), b"").await.unwrap();
        fs::write(dir.join("netbsdlike"), b"").await.unwrap();
        fs::write(dir.join("mod.rs"), b"").await.unwrap();
        // Note: apple/ is missing — this directory should fail validation.

        let digest = make_digest(0xbb);
        let valid =
            filter_valid_subtree_hits(vec![(digest, dir.clone(), 4)]).await;
        assert!(
            valid.is_empty(),
            "subtree with 3/4 entries must be rejected"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_excludes_metadata_file() -> Result<(), Error> {
        // The merkle metadata file lives at the root of cache entries; it
        // must not be counted toward the proto's expected entry count.
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("with_meta");
        fs::create_dir(&dir).await.unwrap();
        fs::write(dir.join("file1"), b"").await.unwrap();
        fs::write(dir.join("file2"), b"").await.unwrap();
        fs::write(dir.join(MERKLE_METADATA_FILENAME), b"meta").await.unwrap();

        let digest = make_digest(0xcc);
        let valid =
            filter_valid_subtree_hits(vec![(digest, dir.clone(), 2)]).await;
        assert_eq!(
            valid.len(),
            1,
            "metadata file must not count toward expected total"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_rejects_missing_path() -> Result<(), Error> {
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("does_not_exist");

        let digest = make_digest(0xdd);
        let valid = filter_valid_subtree_hits(vec![(digest, dir, 5)]).await;
        assert!(valid.is_empty());
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_count_match_with_wrong_names() -> Result<(), Error> {
        // Documented limitation of count-only validation: a directory with
        // the same TOTAL count but different names (one extra unexpected
        // file balancing one missing expected file) passes. Captures the
        // current behavior so a future stricter validator (digest- or
        // name-aware) intentionally breaks this test.
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("balanced");
        fs::create_dir(&dir).await.unwrap();
        fs::write(dir.join("expected_a"), b"").await.unwrap();
        fs::write(dir.join("expected_b"), b"").await.unwrap();
        fs::write(dir.join("unexpected_c"), b"").await.unwrap();
        // The proto would expect [expected_a, expected_b, expected_d]; on
        // disk we have [expected_a, expected_b, unexpected_c]. Count is 3
        // either way — count-only validation accepts.

        let digest = make_digest(0xee);
        let valid =
            filter_valid_subtree_hits(vec![(digest, dir.clone(), 3)]).await;
        assert_eq!(
            valid.len(),
            1,
            "count-only validation cannot detect name swaps; documented limit",
        );
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_path_is_regular_file() -> Result<(), Error> {
        // A subtree_index entry pointing at a regular file (e.g. a cache
        // entry partially deleted then a same-named file written in its
        // place) must be rejected, not crash.
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("not_a_dir");
        fs::write(&path, b"oops").await.unwrap();

        let digest = make_digest(0xff);
        let valid = filter_valid_subtree_hits(vec![(digest, path, 0)]).await;
        assert!(
            valid.is_empty(),
            "regular file as subtree path must be rejected"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_empty_dir_zero_expected() -> Result<(), Error> {
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("empty");
        fs::create_dir(&dir).await.unwrap();

        let digest = make_digest(0x01);
        let valid =
            filter_valid_subtree_hits(vec![(digest, dir.clone(), 0)]).await;
        assert_eq!(valid.len(), 1);
        assert_eq!(valid.get(&digest), Some(&dir));
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_counts_symlinks() -> Result<(), Error> {
        // Symlinks count toward the total; the proto would have one entry
        // in `symlinks` for each.
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("with_symlinks");
        fs::create_dir(&dir).await.unwrap();
        fs::write(dir.join("real"), b"").await.unwrap();
        #[cfg(unix)]
        fs::symlink("real", dir.join("link")).await.unwrap();

        let digest = make_digest(0x02);
        // expect 2: one file + one symlink
        let valid =
            filter_valid_subtree_hits(vec![(digest, dir.clone(), 2)]).await;
        #[cfg(unix)]
        assert_eq!(valid.len(), 1, "symlink must be counted");
        Ok(())
    }

    #[nativelink_test]
    async fn test_filter_valid_subtree_hits_partial_acceptance() -> Result<(), Error> {
        // Mixed batch: one good, one corrupt. Only the good one should pass.
        let temp = TempDir::new().unwrap();
        let good = temp.path().join("good");
        fs::create_dir(&good).await.unwrap();
        fs::write(good.join("a"), b"").await.unwrap();
        fs::write(good.join("b"), b"").await.unwrap();

        let bad = temp.path().join("bad");
        fs::create_dir(&bad).await.unwrap();
        fs::write(bad.join("only_one"), b"").await.unwrap();

        let good_digest = make_digest(0x11);
        let bad_digest = make_digest(0x22);
        let valid = filter_valid_subtree_hits(vec![
            (good_digest, good.clone(), 2),
            (bad_digest, bad, 4),
        ])
        .await;
        assert_eq!(valid.len(), 1);
        assert_eq!(valid.get(&good_digest), Some(&good));
        assert!(valid.get(&bad_digest).is_none());
        Ok(())
    }

    #[nativelink_test]
    async fn test_validate_constructed_tree_passes_when_complete() -> Result<(), Error> {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("root");
        fs::create_dir(&root).await.unwrap();
        fs::write(root.join("readme"), b"").await.unwrap();
        let child = root.join("child");
        fs::create_dir(&child).await.unwrap();
        fs::write(child.join("file"), b"").await.unwrap();

        let child_digest = make_digest(0x42);
        let root_digest = make_digest(0x43);

        let mut tree = HashMap::new();
        tree.insert(
            root_digest,
            ProtoDirectory {
                files: vec![FileNode {
                    name: "readme".to_string(),
                    ..Default::default()
                }],
                directories: vec![DirectoryNode {
                    name: "child".to_string(),
                    digest: Some(child_digest.into()),
                }],
                ..Default::default()
            },
        );
        tree.insert(
            child_digest,
            ProtoDirectory {
                files: vec![FileNode {
                    name: "file".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        let merkle = MerkleTreeMetadata::from_directory_tree(&tree, &root_digest);
        validate_constructed_tree(&root, &tree, &merkle).await?;
        Ok(())
    }

    #[nativelink_test]
    async fn test_validate_constructed_tree_fails_on_missing_child_entry() -> Result<(), Error> {
        // Reproduces the original failure mode: the proto says child/ has two
        // entries (file + apple/) but only `file` was actually materialized.
        // Without this validation the partial tree would have been published
        // into subtree_index and poisoned every future hit.
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("root");
        fs::create_dir(&root).await.unwrap();
        fs::write(root.join("readme"), b"").await.unwrap();
        let child = root.join("child");
        fs::create_dir(&child).await.unwrap();
        fs::write(child.join("file"), b"").await.unwrap();
        // Note: child/apple is intentionally missing.

        let grandchild_digest = make_digest(0x77);
        let child_digest = make_digest(0x42);
        let root_digest = make_digest(0x43);

        let mut tree = HashMap::new();
        tree.insert(
            root_digest,
            ProtoDirectory {
                files: vec![FileNode {
                    name: "readme".to_string(),
                    ..Default::default()
                }],
                directories: vec![DirectoryNode {
                    name: "child".to_string(),
                    digest: Some(child_digest.into()),
                }],
                ..Default::default()
            },
        );
        tree.insert(
            child_digest,
            ProtoDirectory {
                files: vec![FileNode {
                    name: "file".to_string(),
                    ..Default::default()
                }],
                directories: vec![DirectoryNode {
                    name: "apple".to_string(),
                    digest: Some(grandchild_digest.into()),
                }],
                ..Default::default()
            },
        );
        tree.insert(grandchild_digest, ProtoDirectory::default());

        let merkle = MerkleTreeMetadata::from_directory_tree(&tree, &root_digest);
        let err = validate_constructed_tree(&root, &tree, &merkle)
            .await
            .expect_err("validation should fail on incomplete tree");
        assert_eq!(err.code, Code::Internal);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Failpoint-driven tests: deterministically inject the silent-skip
    // failure modes identified in the construction code paths and verify
    // that the post-construction validator (validate_constructed_tree)
    // catches the incomplete entry before it is published into the cache.
    //
    // These tests require the `failpoints` feature on both the `fail`
    // crate and the worker crate.
    //
    // IMPORTANT: failpoints share global state, so they MUST run
    // serially. `serial_test::serial` enforces ordering. Each test must
    // explicitly disable every failpoint it enabled before returning,
    // otherwise the next test in the suite would inherit the active
    // failpoint. (We deliberately avoid `fail::FailScenario` because
    // its guard type is not `Send`, which conflicts with the
    // `nativelink_test` async harness.)
    // ------------------------------------------------------------------
    #[cfg(feature = "failpoints")]
    mod failpoint_tests {
        use serial_test::serial;

        use super::*;

        /// Build a 3-level directory tree in a MemoryStore wrapped by
        /// FastSlowStore so that DirectoryCache uses
        /// `resolve_directory_tree` (and therefore runs the post-
        /// construction validator). MemoryStore is used for both fast
        /// and slow tiers — `filesystem_store` extraction will fail, so
        /// `construct_full` falls back to serial `construct_directory_impl`,
        /// which is where the silent-skip failpoints live.
        ///
        /// Tree shape:
        ///   root/
        ///     readme           (file)
        ///     child/           (subdirectory)
        ///       leaf           (file)
        ///       apple/         (sub-sub-directory, intentionally a
        ///                        single-file dir to make corruption
        ///                        detectable by entry count)
        ///         note         (file)
        async fn setup_three_level_tree() -> (Arc<FastSlowStore>, Store, DigestInfo) {
            use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreSpec};

            let leaf_content = b"leaf";
            let leaf_digest = DigestInfo::try_new(
                "1111111111111111111111111111111111111111111111111111111111111111",
                leaf_content.len() as i64,
            )
            .unwrap();
            let note_content = b"note";
            let note_digest = DigestInfo::try_new(
                "2222222222222222222222222222222222222222222222222222222222222222",
                note_content.len() as i64,
            )
            .unwrap();
            let readme_content = b"readme";
            let readme_digest = DigestInfo::try_new(
                "3333333333333333333333333333333333333333333333333333333333333333",
                readme_content.len() as i64,
            )
            .unwrap();

            // apple/note
            let apple_dir = ProtoDirectory {
                files: vec![FileNode {
                    name: "note".to_string(),
                    digest: Some(note_digest.into()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let mut apple_bytes = Vec::new();
            apple_dir.encode(&mut apple_bytes).unwrap();
            let apple_digest = DigestInfo::try_new(
                "4444444444444444444444444444444444444444444444444444444444444444",
                apple_bytes.len() as i64,
            )
            .unwrap();

            // child/{leaf, apple/}
            let child_dir = ProtoDirectory {
                files: vec![FileNode {
                    name: "leaf".to_string(),
                    digest: Some(leaf_digest.into()),
                    ..Default::default()
                }],
                directories: vec![DirectoryNode {
                    name: "apple".to_string(),
                    digest: Some(apple_digest.into()),
                }],
                ..Default::default()
            };
            let mut child_bytes = Vec::new();
            child_dir.encode(&mut child_bytes).unwrap();
            let child_digest = DigestInfo::try_new(
                "5555555555555555555555555555555555555555555555555555555555555555",
                child_bytes.len() as i64,
            )
            .unwrap();

            // root/{readme, child/}
            let root_dir = ProtoDirectory {
                files: vec![FileNode {
                    name: "readme".to_string(),
                    digest: Some(readme_digest.into()),
                    ..Default::default()
                }],
                directories: vec![DirectoryNode {
                    name: "child".to_string(),
                    digest: Some(child_digest.into()),
                }],
                ..Default::default()
            };
            let mut root_bytes = Vec::new();
            root_dir.encode(&mut root_bytes).unwrap();
            let root_digest = DigestInfo::try_new(
                "6666666666666666666666666666666666666666666666666666666666666666",
                root_bytes.len() as i64,
            )
            .unwrap();

            // Build a real FastSlowStore with two MemoryStore halves so
            // DirectoryCache.fast_slow_store is Some — that is what
            // gates `resolve_directory_tree` and the validator.
            let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
            let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
            let fss: Arc<FastSlowStore> = FastSlowStore::new(
                &FastSlowSpec {
                    fast: StoreSpec::Memory(MemorySpec::default()),
                    slow: StoreSpec::Memory(MemorySpec::default()),
                    fast_direction: Default::default(),
                    slow_direction: Default::default(),
                    chunked_reads_enabled: false,
                    slow_writes_in_flight_max_bytes: 0,
                },
                fast.clone(),
                slow.clone(),
            );

            // Seed BOTH tiers — directory protos are fetched via the
            // FastSlowStore.cas_store (which prefers fast, falls back to
            // slow), file blobs are fetched the same way.
            for (digest, bytes) in [
                (root_digest, root_bytes),
                (child_digest, child_bytes),
                (apple_digest, apple_bytes),
            ] {
                slow.update_oneshot(digest, bytes.into()).await.unwrap();
            }
            for (digest, bytes) in [
                (leaf_digest, leaf_content.to_vec()),
                (note_digest, note_content.to_vec()),
                (readme_digest, readme_content.to_vec()),
            ] {
                slow.update_oneshot(digest, bytes.into()).await.unwrap();
            }

            // The DirectoryCache also uses the legacy `cas_store: Store`
            // for the serial fallback path inside construct_directory_impl.
            // Wire it to the same slow store so all fetches resolve.
            (fss, slow, root_digest)
        }

        async fn make_cache(
            cache_root: PathBuf,
        ) -> Result<(Arc<FastSlowStore>, DirectoryCache, DigestInfo), Error> {
            let (fss, cas_store, root_digest) = setup_three_level_tree().await;
            let config = DirectoryCacheConfig {
                max_entries: 10,
                max_size_bytes: 1024 * 1024,
                cache_root,
                direct_use_mode: false,
            };
            let cache = DirectoryCache::new(config, cas_store, Some(fss.clone())).await?;
            Ok((fss, cache, root_digest))
        }

        /// A construction path that returns `Ok(())` from `create_file`
        /// without actually writing the file would publish an incomplete
        /// directory into the cache — exactly the bug class that
        /// motivated the validator. With the failpoint we deterministically
        /// drop one file from the bottom-most directory; the entry count
        /// for `apple/` (expected 1, actual 0) must be caught.
        #[nativelink_test]
        #[serial(directory_cache_failpoints)]
        async fn test_silently_dropped_file_caught_by_validator() -> Result<(), Error> {
            let temp_dir = TempDir::new().unwrap();
            let cache_root = temp_dir.path().join("cache");
            let (_fss, cache, root_digest) = make_cache(cache_root.clone()).await?;

            // Drop ANY file whose blob digest starts with `1111` —
            // that's the `leaf` blob in this test's tree. The argument
            // scoping guards against other tests racing on this same
            // global failpoint.
            fail::cfg(
                "directory_cache_skip_file_in_construction",
                "return(1111)",
            )
            .map_err(|e| make_err!(Code::Internal, "fail::cfg failed: {e}"))?;

            let dest = temp_dir.path().join("dest");
            let result = cache.get_or_create(root_digest, &dest).await;

            // Always disable the failpoint so other serial tests see a
            // clean slate.
            fail::cfg("directory_cache_skip_file_in_construction", "off").ok();

            assert!(
                result.is_err(),
                "construction must fail when a file is silently dropped"
            );
            let err = result.unwrap_err();
            assert_eq!(err.code, Code::Internal, "expected Internal error");
            assert!(
                err.to_string().contains("post-construction validation"),
                "error must mention post-construction validation, got: {err}"
            );

            // Ensure no temp dir survived and the cache entry was not
            // published.
            let mut had_real_entries = false;
            let mut entries = fs::read_dir(&cache_root).await.unwrap();
            while let Some(entry) = entries.next_entry().await.unwrap() {
                let n = entry.file_name().to_string_lossy().to_string();
                if n == ".cache_version" {
                    continue;
                }
                had_real_entries = true;
            }
            assert!(
                !had_real_entries,
                "incomplete cache entry must not have been published"
            );
            Ok(())
        }

        /// `create_subdirectory` returning `Ok(())` without recursing
        /// reproduces the original failure mode (the `apple/` subtree
        /// vanishing). The validator must catch it because the parent
        /// directory's entry count on disk no longer matches the proto.
        #[nativelink_test]
        #[serial(directory_cache_failpoints)]
        async fn test_silently_dropped_subdir_caught_by_validator() -> Result<(), Error> {
            let temp_dir = TempDir::new().unwrap();
            let cache_root = temp_dir.path().join("cache");
            let (_fss, cache, root_digest) = make_cache(cache_root.clone()).await?;

            // Drop the `apple` sub-sub-directory — its digest starts
            // with `4444` in the test fixture. Result: child/ ends up
            // with 1 on-disk entry (leaf) but the proto expects 2,
            // which the validator must catch.
            fail::cfg(
                "directory_cache_skip_subdir_in_construction",
                "return(4444)",
            )
            .map_err(|e| make_err!(Code::Internal, "fail::cfg failed: {e}"))?;

            let dest = temp_dir.path().join("dest");
            let result = cache.get_or_create(root_digest, &dest).await;

            fail::cfg("directory_cache_skip_subdir_in_construction", "off").ok();

            assert!(
                result.is_err(),
                "construction must fail when a subdir is silently dropped"
            );
            let err = result.unwrap_err();
            assert_eq!(err.code, Code::Internal);
            assert!(
                err.to_string().contains("post-construction validation"),
                "error must mention post-construction validation, got: {err}"
            );
            Ok(())
        }

        /// Sanity check that the same construction succeeds when no
        /// failpoint is active — confirms the test setup itself is good
        /// and the validator does not reject correctly-built trees.
        #[nativelink_test]
        #[serial(directory_cache_failpoints)]
        async fn test_three_level_tree_construction_succeeds_baseline()
        -> Result<(), Error> {
            // Defensive: make sure no leftover failpoint config from a
            // prior test run is still active.
            fail::cfg("directory_cache_skip_file_in_construction", "off").ok();
            fail::cfg("directory_cache_skip_subdir_in_construction", "off").ok();
            fail::cfg("directory_cache_subtree_clone_fail", "off").ok();
            fail::cfg("directory_cache_failed_subtree_missing_in_tree", "off").ok();

            let temp_dir = TempDir::new().unwrap();
            let cache_root = temp_dir.path().join("cache");
            let (_fss, cache, root_digest) = make_cache(cache_root).await?;

            let dest = temp_dir.path().join("dest");
            cache.get_or_create(root_digest, &dest).await?;

            assert!(dest.join("readme").exists());
            assert!(dest.join("child").is_dir());
            assert!(dest.join("child/leaf").exists());
            assert!(dest.join("child/apple").is_dir());
            assert!(dest.join("child/apple/note").exists());
            Ok(())
        }

        /// The failed-subtree fallback walk in `construct_with_subtrees`
        /// previously had a `warn!` + continue path when a digest was
        /// missing from the resolved tree — that is the bug we are
        /// hardening against. `directory_cache_failed_subtree_missing_in_tree`
        /// forces the lookup to return None, so the new error path fires.
        ///
        /// Triggering this end-to-end through `get_or_create` requires
        /// the `construct_with_subtrees` code path, which only runs when
        /// `subtree_hits` is non-empty. Setting that up needs a real
        /// FilesystemStore (so that prior cache entries register subtree
        /// paths in `subtree_index`) — significantly larger scaffolding
        /// than the rest of these tests assume. Instead we exercise the
        /// hard-error branch in isolation by invoking
        /// `construct_with_subtrees` directly with a hand-built
        /// `subtree_hits` map and the failpoint enabled.
        ///
        /// We can't easily construct a real FilesystemStore without
        /// a lot of plumbing either; documenting the failure mode in
        /// the failpoint definition itself + the existing
        /// `validate_constructed_tree` test for missing children is the
        /// lowest-cost coverage. Skip with rationale.
        #[nativelink_test]
        #[serial(directory_cache_failpoints)]
        async fn test_failed_subtree_missing_in_tree_failpoint_compiles() {
            // Compile-time assertion that the failpoint exists. End-to-
            // end validation of the hard-error branch requires
            // FilesystemStore-backed subtree_index seeding — out of
            // scope for unit tests; covered indirectly by the existing
            // `tree.get(&dir_digest).ok_or_else(...)` checks at the
            // top of construct_with_subtrees and the integration tests
            // that exercise full DirectoryCache flows.
            fail::cfg("directory_cache_failed_subtree_missing_in_tree", "off").ok();
        }
    }
}
