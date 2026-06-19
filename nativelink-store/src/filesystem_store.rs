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

use core::fmt::{Debug, Formatter};
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use std::borrow::Cow;
use std::ffi::{OsStr, OsString};
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use async_lock::RwLock;
use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use futures::{Future, TryFutureExt};
use nativelink_config::stores::FilesystemSpec;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::background_spawn;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use nativelink_util::health_utils::{HealthRegistryBuilder, HealthStatus, HealthStatusIndicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, StoreDriver,
    StoreKey, StoreKeyBorrow, StoreOptimizations, UploadSizeInfo,
};
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReadDirStream;
use tracing::{debug, error, info, trace, warn};

use crate::callback_utils::ItemCallbackHolder;
use crate::cas_utils::is_zero_digest;
#[cfg(feature = "chunked_fast_slow")]
use crate::chunked::chunked_filesystem::{
    ChunkedPartialsMap, commit_chunked_to_holding as chunked_commit_to_holding,
    discard_chunked as chunked_discard, finalize_holding as chunked_finalize_holding,
    holding_content_path as chunked_holding_path,
    prune_holding_partials as chunked_prune_holding_partials,
    unlink_holding as chunked_unlink_holding,
    write_chunk_at_offset as chunked_write_chunk_at_offset,
};

// Default size to allocate memory of the buffer when reading files.
// 256 KiB reduces syscalls by 4x compared to 64 KiB. At 10Gbps, 64 KiB reads
// cause ~19,500 syscalls/sec/stream; 256 KiB brings this down to ~4,900.
// Modern NVMe SSDs perform significantly better with larger read sizes.
/// Default read buffer size. Matches the default ByteStream
/// `max_bytes_per_stream` (3 MiB) so that each disk read produces
/// exactly one chunk, avoiding BytesMut concatenation copies in
/// `buf_channel::consume()`.
const DEFAULT_BUFF_SIZE: usize = 3 * 1024 * 1024;
// Default block size of all major filesystems is 4KB
const DEFAULT_BLOCK_SIZE: u64 = 4 * 1024;

pub const STR_FOLDER: &str = "s";
pub const DIGEST_FOLDER: &str = "d";

/// Returns the expected on-disk path for a digest file under the given
/// content path. This is useful for tests and external tooling that need
/// to construct or verify file paths.
///
/// The path layout is: `{content_path}/d/{hash[0..2]}/{hash}-{size}`
///
/// # Integrity contract — depends on what backs this store
///
/// The filename embeds whatever hash the caller passes in `digest`. The
/// interpretation of that hash depends on which store backs this
/// `FilesystemStore`:
///
/// - **CAS-backed (`cas_STORE` chain):** `hash == SHA-N(file_bytes)`. A
///   `VerifyStore` wrapper enforces this on every write and read; an
///   integrity scanner that re-hashes each file MUST match the filename.
/// - **AC-backed (`ac_store` chain):** `hash == action_digest`, which is
///   the CAS digest of the *Action* proto, NOT a hash of the
///   `ActionResult` bytes stored under this filename. `H(file_bytes) !=
///   hash` in general. A `VerifyStore` wrapper would reject every AC
///   write. An integrity scanner that re-hashes AC files against their
///   filename will fail on every entry — by design.
///
/// See `docs/ac-integrity-contract.md` for the full rationale and a
/// table of CAS-vs-AC properties.
pub fn digest_content_path(content_path: &str, digest: &DigestInfo) -> OsString {
    let key: StoreKey<'_> = (*digest).into();
    to_full_path_from_key(content_path, &key)
}

#[derive(Clone, Copy, Debug)]
pub enum FileType {
    Digest,
    String,
}

#[derive(Debug, MetricsComponent)]
pub struct SharedContext {
    // Used in testing to know how many active drop() spawns are running.
    // TODO(palfrey) It is probably a good idea to use a spin lock during
    // destruction of the store to ensure that all files are actually
    // deleted (similar to how it is done in tests).
    #[metric(help = "Number of active drop spawns")]
    pub active_drop_spawns: AtomicU64,
    #[metric(help = "Path to the configured temp path")]
    temp_path: String,
    #[metric(help = "Path to the configured content path")]
    content_path: String,
}

#[derive(Eq, PartialEq, Debug)]
enum PathType {
    Content,
    Temp,
    Custom(OsString),
}

/// [`EncodedFilePath`] stores the path to the file
/// including the context, path type and key to the file.
/// The whole [`StoreKey`] is stored as opposed to solely
/// the [`DigestInfo`] so that it is more usable for things
/// such as BEP -see Issue #1108
#[derive(Debug)]
pub struct EncodedFilePath {
    shared_context: Arc<SharedContext>,
    path_type: PathType,
    key: StoreKey<'static>,
}

impl EncodedFilePath {
    #[inline]
    fn get_file_path(&self) -> Cow<'_, OsStr> {
        get_file_path_raw(&self.path_type, self.shared_context.as_ref(), &self.key)
    }
}

#[inline]
fn get_file_path_raw<'a>(
    path_type: &'a PathType,
    shared_context: &SharedContext,
    key: &StoreKey<'a>,
) -> Cow<'a, OsStr> {
    let folder = match path_type {
        PathType::Content => &shared_context.content_path,
        PathType::Temp => &shared_context.temp_path,
        PathType::Custom(path) => return Cow::Borrowed(path),
    };
    Cow::Owned(to_full_path_from_key(folder, key))
}

impl Drop for EncodedFilePath {
    fn drop(&mut self) {
        // `drop()` can be called during shutdown, so we use `path_type` flag to know if the
        // file actually needs to be deleted.
        if self.path_type == PathType::Content {
            return;
        }

        let file_path = self.get_file_path().to_os_string();
        let shared_context = self.shared_context.clone();
        // .fetch_add returns previous value, so we add one to get approximate current value
        let current_active_drop_spawns = shared_context
            .active_drop_spawns
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        debug!(
            %current_active_drop_spawns,
            ?file_path,
            "Spawned a filesystem_delete_file"
        );
        background_spawn!("filesystem_delete_file", async move {
            let result = fs::remove_file(&file_path)
                .await
                .err_tip(|| format!("Failed to remove file {}", file_path.display()));
            if let Err(err) = result {
                if err.code == Code::NotFound {
                    // File already deleted (e.g. race between eviction paths).
                    debug!(?file_path, "File already deleted, ignoring");
                } else {
                    error!(?file_path, ?err, "Failed to delete file");
                }
            } else {
                debug!(?file_path, "File deleted",);
            }
            // .fetch_sub returns previous value, so we subtract one to get approximate current value
            let current_active_drop_spawns = shared_context
                .active_drop_spawns
                .fetch_sub(1, Ordering::Relaxed)
                - 1;
            debug!(
                ?current_active_drop_spawns,
                ?file_path,
                "Dropped a filesystem_delete_file"
            );
        });
    }
}

/// Returns the 2-character hex shard prefix for a digest, derived from
/// the first byte of the packed hash. This gives 256 subdirectories
/// (00-ff), reducing per-directory file count from hundreds of thousands
/// to ~1,500 on typical deployments.
///
/// `pub(crate)` so the chunked-streaming partial-path helper in
/// `crate::chunked::chunked_filesystem` can reuse the same on-disk
/// shard layout decision (one source of truth — if the layout ever
/// changes, both the chunked partials and the legacy CAS files move
/// together).
#[inline]
pub(crate) fn digest_shard_prefix(digest_info: &DigestInfo) -> [u8; 2] {
    const HEX_LUT: &[u8; 16] = b"0123456789abcdef";
    let first_byte = digest_info.packed_hash()[0];
    [
        HEX_LUT[(first_byte >> 4) as usize],
        HEX_LUT[(first_byte & 0x0f) as usize],
    ]
}

/// This creates the file path from the [`StoreKey`]. If
/// it is a string, the string, prefixed with [`STR_PREFIX`]
/// for backwards compatibility, is stored.
///
/// If it is a [`DigestInfo`], it is prefixed by [`DIGEST_PREFIX`]
/// followed by a 2-char hex shard directory (first byte of hash),
/// then the string representation of a digest - the hash in hex,
/// a hyphen then the size in bytes.
///
/// Layout: `{folder}/d/{hash[0..2]}/{hash}-{size}`
#[inline]
fn to_full_path_from_key(folder: &str, key: &StoreKey<'_>) -> OsString {
    match key {
        StoreKey::Str(str) => format!("{folder}/{STR_FOLDER}/{str}"),
        StoreKey::Digest(digest_info) => {
            let shard = digest_shard_prefix(digest_info);
            // SAFETY: shard is always valid ASCII hex chars.
            let shard_str = unsafe { core::str::from_utf8_unchecked(&shard) };
            format!("{folder}/{DIGEST_FOLDER}/{shard_str}/{digest_info}")
        }
    }
    .into()
}

pub trait FileEntry: LenEntry + Send + Sync + Debug + 'static {
    /// Responsible for creating the underlying `FileEntry`.
    fn create(data_size: u64, block_size: u64, encoded_file_path: RwLock<EncodedFilePath>) -> Self;

    /// Creates a (usually) temp file, opens it and returns the path to the temp file.
    fn make_and_open_file(
        block_size: u64,
        encoded_file_path: EncodedFilePath,
    ) -> impl Future<Output = Result<(Self, fs::FileSlot, OsString), Error>> + Send
    where
        Self: Sized;

    /// Returns the underlying reference to the size of the data in bytes
    fn data_size_mut(&mut self) -> &mut u64;

    /// Returns the actual size of the underlying file on the disk after accounting for filesystem block size.
    fn size_on_disk(&self) -> u64;

    /// Gets the underlying `EncodedfilePath`.
    fn get_encoded_file_path(&self) -> &RwLock<EncodedFilePath>;

    /// Returns a reader that will read part of the underlying file.
    fn read_file_part(
        &self,
        offset: u64,
    ) -> impl Future<Output = Result<fs::FileSlot, Error>> + Send;

    /// This function is a safe way to extract the file name of the underlying file. To protect users from
    /// accidentally creating undefined behavior we encourage users to do the logic they need to do with
    /// the filename inside this function instead of extracting the filename and doing the logic outside.
    /// This is because the filename is not guaranteed to exist after this function returns, however inside
    /// the callback the file is always guaranteed to exist and immutable.
    /// DO NOT USE THIS FUNCTION TO EXTRACT THE FILENAME AND STORE IT FOR LATER USE.
    fn get_file_path_locked<
        T,
        Fut: Future<Output = Result<T, Error>> + Send,
        F: FnOnce(OsString) -> Fut + Send,
    >(
        &self,
        handler: F,
    ) -> impl Future<Output = Result<T, Error>> + Send;
}

pub struct FileEntryImpl {
    data_size: u64,
    block_size: u64,
    // We lock around this as it gets rewritten when we move between temp and content types
    encoded_file_path: RwLock<EncodedFilePath>,
}

impl FileEntryImpl {
    pub fn get_shared_context_for_test(&mut self) -> Arc<SharedContext> {
        self.encoded_file_path.get_mut().shared_context.clone()
    }
}

impl FileEntry for FileEntryImpl {
    fn create(data_size: u64, block_size: u64, encoded_file_path: RwLock<EncodedFilePath>) -> Self {
        Self {
            data_size,
            block_size,
            encoded_file_path,
        }
    }

    /// This encapsulates the logic for the edge case of if the file fails to create
    /// the cleanup of the file is handled without creating a `FileEntry`, which would
    /// try to cleanup the file as well during `drop()`.
    async fn make_and_open_file(
        block_size: u64,
        encoded_file_path: EncodedFilePath,
    ) -> Result<(Self, fs::FileSlot, OsString), Error> {
        let temp_full_path = encoded_file_path.get_file_path().to_os_string();
        let temp_file_result = fs::create_file(temp_full_path.clone())
            .or_else(|mut err| async {
                let remove_result = fs::remove_file(&temp_full_path).await.err_tip(|| {
                    format!(
                        "Failed to remove file {} in filesystem store",
                        temp_full_path.display()
                    )
                });
                if let Err(remove_err) = remove_result {
                    err = err.merge(remove_err);
                }
                warn!(?err, ?block_size, ?temp_full_path, "Failed to create file",);
                Err(err).err_tip(|| {
                    format!(
                        "Failed to create {} in filesystem store",
                        temp_full_path.display()
                    )
                })
            })
            .await?;

        Ok((
            <Self as FileEntry>::create(
                0, /* Unknown yet, we will fill it in later */
                block_size,
                RwLock::new(encoded_file_path),
            ),
            temp_file_result,
            temp_full_path,
        ))
    }

    fn data_size_mut(&mut self) -> &mut u64 {
        &mut self.data_size
    }

    fn size_on_disk(&self) -> u64 {
        self.data_size.div_ceil(self.block_size) * self.block_size
    }

    fn get_encoded_file_path(&self) -> &RwLock<EncodedFilePath> {
        &self.encoded_file_path
    }

    fn read_file_part(
        &self,
        offset: u64,
    ) -> impl Future<Output = Result<fs::FileSlot, Error>> + Send {
        self.get_file_path_locked(move |full_content_path| async move {
            let file = fs::open_file(&full_content_path, offset)
                .await
                .err_tip(|| {
                    format!(
                        "Failed to open file in filesystem store {}",
                        full_content_path.display()
                    )
                })?;
            Ok(file)
        })
    }

    async fn get_file_path_locked<
        T,
        Fut: Future<Output = Result<T, Error>> + Send,
        F: FnOnce(OsString) -> Fut + Send,
    >(
        &self,
        handler: F,
    ) -> Result<T, Error> {
        let encoded_file_path = self.get_encoded_file_path().read().await;
        handler(encoded_file_path.get_file_path().to_os_string()).await
    }
}

/// Reads a file entry's contents directly into `Bytes`, bypassing
/// buf_channel. Opens the file via `read_file_part` (which acquires the
/// FD semaphore), then reads in a blocking thread. Reads up to `length`
/// bytes (or until EOF if None).
async fn read_file_entry_bytes<Fe: FileEntry>(
    entry: &Fe,
    length: Option<u64>,
) -> Result<Bytes, Error> {
    let file_slot = entry.read_file_part(0).await?;

    let read_limit = length.unwrap_or(u64::MAX);
    let read_limit_usize = usize::try_from(read_limit.min(256 * 1024 * 1024))
        .unwrap_or(256 * 1024 * 1024);

    tokio::task::spawn_blocking(move || -> Result<Bytes, Error> {
        use std::io::Read;
        let mut f = file_slot;
        // Start with a reasonable initial capacity (64 KiB) and grow as needed,
        // rather than pre-allocating the full limit which could be very large.
        let initial_cap = read_limit_usize.min(64 * 1024);
        let mut buf = BytesMut::with_capacity(initial_cap);
        let mut total_read = 0usize;
        let mut read_buf = vec![0u8; 64 * 1024];
        loop {
            let remaining = read_limit_usize.saturating_sub(total_read);
            if remaining == 0 {
                break;
            }
            let to_read = read_buf.len().min(remaining);
            match f.as_std_mut().read(&mut read_buf[..to_read]) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&read_buf[..n]);
                    total_read += n;
                }
                Err(e) => return Err(make_err!(
                    Code::Internal,
                    "read_file_entry_bytes: read failed: {e:?}"
                )),
            }
        }
        Ok(buf.freeze())
    })
    .await
    .map_err(|e| make_err!(Code::Internal, "read_file_entry_bytes join error: {e:?}"))?
}

impl Debug for FileEntryImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), core::fmt::Error> {
        f.debug_struct("FileEntryImpl")
            .field("data_size", &self.data_size)
            .field("encoded_file_path", &"<behind mutex>")
            .finish()
    }
}

fn make_temp_digest(mut digest: DigestInfo) -> DigestInfo {
    static DELETE_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hash = *digest.packed_hash();
    hash[24..].clone_from_slice(
        &DELETE_FILE_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .to_le_bytes(),
    );
    digest.set_packed_hash(*hash);
    digest
}

fn make_temp_key(key: &StoreKey) -> StoreKey<'static> {
    StoreKey::Digest(make_temp_digest(key.borrow().into_digest()))
}

impl LenEntry for FileEntryImpl {
    #[inline]
    fn len(&self) -> u64 {
        self.size_on_disk()
    }

    fn is_empty(&self) -> bool {
        self.data_size == 0
    }

    // unref() only triggers when an item is removed from the eviction_map. It is possible
    // that another place in code has a reference to `FileEntryImpl` and may later read the
    // file. To support this edge case, we first move the file to a temp file and point
    // target file location to the new temp file. `unref()` should only ever be called once.
    #[inline]
    async fn unref(&self) {
        let mut encoded_file_path = self.encoded_file_path.write().await;
        if encoded_file_path.path_type == PathType::Temp {
            // Already a temp file marked for deletion on drop. This happens
            // when the entry is evicted from the map before emplace_file
            // renames it into the content path — expected under cache pressure.
            debug!(
                key = ?encoded_file_path.key,
                "File is already a temp file",
            );
            return;
        }
        let from_path = encoded_file_path.get_file_path();
        let new_key = make_temp_key(&encoded_file_path.key);

        let to_path = to_full_path_from_key(&encoded_file_path.shared_context.temp_path, &new_key);

        if let Err(err) = fs::rename(&from_path, &to_path).await {
            warn!(
                key = ?encoded_file_path.key,
                ?from_path,
                ?to_path,
                ?err,
                "Failed to rename file",
            );
        } else {
            debug!(
                key = ?encoded_file_path.key,
                ?from_path,
                ?to_path,
                "Evicted blob from filesystem cache (unref)",
            );
            encoded_file_path.path_type = PathType::Temp;
            encoded_file_path.key = new_key;
        }
    }
}

#[inline]
fn digest_from_filename(file_name: &str) -> Result<DigestInfo, Error> {
    let (hash, size) = file_name.split_once('-').err_tip(|| "")?;
    let size = size.parse::<i64>()?;
    DigestInfo::try_new(hash, size)
}

pub fn key_from_file(file_name: &str, file_type: FileType) -> Result<StoreKey<'_>, Error> {
    match file_type {
        FileType::String => Ok(StoreKey::new_str(file_name)),
        FileType::Digest => digest_from_filename(file_name).map(StoreKey::Digest),
    }
}

/// The number of files to read the metadata for at the same time when running
/// `add_files_to_cache`.
const SIMULTANEOUS_METADATA_READS: usize = 200;

type FsEvictingMap<Fe> =
    MokaEvictingMap<StoreKeyBorrow, StoreKey<'static>, Arc<Fe>, SystemTime, ItemCallbackHolder>;

async fn add_files_to_cache<Fe: FileEntry>(
    evicting_map: &FsEvictingMap<Fe>,
    anchor_time: &SystemTime,
    shared_context: &Arc<SharedContext>,
    block_size: u64,
    rename_fn: fn(&OsStr, &OsStr) -> Result<(), std::io::Error>,
) -> Result<(), Error> {
    #[expect(clippy::too_many_arguments)]
    async fn process_entry<Fe: FileEntry>(
        evicting_map: &FsEvictingMap<Fe>,
        file_name: &str,
        file_type: FileType,
        atime: SystemTime,
        data_size: u64,
        block_size: u64,
        anchor_time: &SystemTime,
        shared_context: &Arc<SharedContext>,
    ) -> Result<(), Error> {
        let key = key_from_file(file_name, file_type)?;

        let file_entry = Fe::create(
            data_size,
            block_size,
            RwLock::new(EncodedFilePath {
                shared_context: shared_context.clone(),
                path_type: PathType::Content,
                key: key.borrow().into_owned(),
            }),
        );
        // Use a negative seconds_since_anchor for files that existed before
        // the anchor time (startup). This correctly represents them as "older
        // than anything inserted during runtime" in the EvictingMap timeline.
        // Files with atime closer to startup get values closer to 0 (newer),
        // while files not accessed for days get large negative values (older).
        let seconds_since_anchor = if let Ok(before) = anchor_time.duration_since(atime) {
            let secs = before.as_secs();
            if secs > i32::MAX as u64 {
                i32::MIN
            } else {
                -(secs as i32)
            }
        } else {
            // atime is after anchor_time — anomalous but harmless.
            // Treat as most-recently-used.
            let ahead_secs = atime
                .duration_since(*anchor_time)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            warn!(
                %file_name,
                ahead_secs,
                "file access time newer than FilesystemStore start time"
            );
            0
        };
        evicting_map
            .insert_with_time(
                key.into_owned().into(),
                Arc::new(file_entry),
                seconds_since_anchor,
            )
            .await;
        Ok(())
    }

    /// Reads directory entries from a single directory, returning
    /// (file_name, atime, size, is_file) tuples.
    async fn read_dir_entries(
        dir_path: &str,
    ) -> Result<Vec<(String, SystemTime, u64, bool)>, Error> {
        let (_permit, dir_handle) = fs::read_dir(dir_path)
            .await
            .err_tip(|| {
                format!("Failed opening directory {dir_path} for iterating in filesystem store")
            })?
            .into_inner();

        let read_dir_stream = ReadDirStream::new(dir_handle);
        read_dir_stream
            .map(|dir_entry| async move {
                let dir_entry = dir_entry.unwrap();
                let file_name = dir_entry.file_name().into_string().unwrap();
                let metadata = dir_entry
                    .metadata()
                    .await
                    .err_tip(|| "Failed to get metadata in filesystem store")?;
                let is_file = metadata.is_file();
                let atime = metadata
                    .accessed()
                    .or_else(|_| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                Result::<(String, SystemTime, u64, bool), Error>::Ok((
                    file_name,
                    atime,
                    metadata.len(),
                    is_file,
                ))
            })
            .buffer_unordered(SIMULTANEOUS_METADATA_READS)
            .try_collect()
            .await
    }

    async fn read_files(
        folder: Option<&str>,
        shared_context: &SharedContext,
    ) -> Result<Vec<(String, SystemTime, u64, bool)>, Error> {
        // Note: In Dec 2024 this is for backwards compatibility with the old
        // way files were stored on disk. Previously all files were in a single
        // folder regardless of the StoreKey type. This allows old versions of
        // nativelink file layout to be upgraded at startup time.
        // This logic can be removed once more time has passed.
        let read_dir = folder.map_or_else(
            || format!("{}/", shared_context.content_path),
            |folder| format!("{}/{folder}/", shared_context.content_path),
        );

        read_dir_entries(&read_dir).await
    }

    /// Reads files from the digest folder, scanning both shard
    /// subdirectories (d/XX/) and legacy flat files (d/HASH-SIZE).
    async fn read_digest_files_sharded(
        shared_context: &SharedContext,
    ) -> Result<Vec<(String, SystemTime, u64, bool)>, Error> {
        let digest_dir = format!("{}/{DIGEST_FOLDER}", shared_context.content_path);
        let top_entries = read_dir_entries(&digest_dir).await?;

        let mut all_files = Vec::new();

        for (name, atime, size, is_file) in top_entries {
            if is_file {
                // Legacy flat file directly in d/ — include it.
                all_files.push((name, atime, size, true));
            } else if name.len() == 2 {
                // Shard subdirectory (00-ff) — scan its contents.
                let shard_path = format!("{digest_dir}/{name}");
                match read_dir_entries(&shard_path).await {
                    Ok(shard_entries) => {
                        for entry in shard_entries {
                            if entry.3 {
                                all_files.push(entry);
                            }
                        }
                    }
                    Err(err) => {
                        warn!(?err, shard = %name, "failed to read shard directory during startup scan");
                    }
                }
            }
            // Skip other directories (s/, d/ — shouldn't be here but just in case).
        }

        Ok(all_files)
    }

    /// Note: In Dec 2024 this is for backwards compatibility with the old
    /// way files were stored on disk. Previously all files were in a single
    /// folder regardless of the [`StoreKey`] type. This moves files from the old cache
    /// location to the new cache location, under [`DIGEST_FOLDER`] with shard prefix.
    async fn move_old_cache(
        shared_context: &Arc<SharedContext>,
        rename_fn: fn(&OsStr, &OsStr) -> Result<(), std::io::Error>,
    ) -> Result<(), Error> {
        let file_infos = read_files(None, shared_context).await?;

        let from_path = shared_context.content_path.to_string();

        let digest_path = format!("{}/{DIGEST_FOLDER}", shared_context.content_path);

        for (file_name, _, _, _) in file_infos.into_iter().filter(|x| x.3) {
            let from_file: OsString = format!("{from_path}/{file_name}").into();
            // Place into the shard subdirectory based on first 2 hex chars.
            let to_file: OsString = if file_name.len() >= 2 {
                let shard = &file_name[..2];
                format!("{digest_path}/{shard}/{file_name}").into()
            } else {
                format!("{digest_path}/{file_name}").into()
            };

            if let Err(err) = rename_fn(&from_file, &to_file) {
                warn!(?from_file, ?to_file, ?err, "Failed to rename file",);
            } else {
                debug!(?from_file, ?to_file, "Renamed file (old cache)",);
            }
        }
        Ok(())
    }

    /// Migrates legacy flat files from `d/HASH-SIZE` to the sharded
    /// layout `d/XX/HASH-SIZE`. Files already in shard subdirectories
    /// are left alone.
    async fn migrate_flat_to_sharded(
        shared_context: &Arc<SharedContext>,
        rename_fn: fn(&OsStr, &OsStr) -> Result<(), std::io::Error>,
    ) -> Result<(), Error> {
        let digest_dir = format!("{}/{DIGEST_FOLDER}", shared_context.content_path);
        let top_entries = read_dir_entries(&digest_dir).await?;
        let mut migrated = 0u64;

        for (file_name, _, _, is_file) in &top_entries {
            if !is_file || file_name.len() < 2 {
                continue;
            }
            let shard = &file_name[..2];
            let from_file: OsString = format!("{digest_dir}/{file_name}").into();
            let to_file: OsString = format!("{digest_dir}/{shard}/{file_name}").into();

            if let Err(err) = rename_fn(&from_file, &to_file) {
                warn!(?from_file, ?to_file, ?err, "failed to migrate flat file to shard");
            } else {
                migrated += 1;
            }
        }
        if migrated > 0 {
            info!(migrated, "migrated legacy flat CAS files to sharded layout");
        }
        Ok(())
    }

    async fn add_files_for_folder<Fe: FileEntry>(
        evicting_map: &FsEvictingMap<Fe>,
        anchor_time: &SystemTime,
        shared_context: &Arc<SharedContext>,
        block_size: u64,
        folder: &str,
    ) -> Result<(), Error> {
        let file_type = match folder {
            STR_FOLDER => FileType::String,
            DIGEST_FOLDER => FileType::Digest,
            _ => panic!("Invalid folder type"),
        };

        let mut file_infos = if folder == DIGEST_FOLDER {
            read_digest_files_sharded(shared_context).await?
        } else {
            read_files(Some(folder), shared_context).await?
        };

        // Sort by atime oldest-first so that the LRU cache ordering matches
        // actual file access recency. Without this, items are inserted in
        // directory-iteration order (random), causing recently-used files to
        // be evicted while cold files survive.
        file_infos.sort_by(|a, b| a.1.cmp(&b.1));

        let path_root = format!("{}/{folder}", shared_context.content_path);

        for (file_name, atime, data_size, _) in file_infos.into_iter().filter(|x| x.3) {
            let result = process_entry(
                evicting_map,
                &file_name,
                file_type,
                atime,
                data_size,
                block_size,
                anchor_time,
                shared_context,
            )
            .await;
            if let Err(err) = result {
                warn!(?file_name, ?err, "Failed to add file to eviction cache",);
                // Derive full path: for digests, use shard subdir; for strings, flat.
                let full_path = if folder == DIGEST_FOLDER && file_name.len() >= 2 {
                    let shard = &file_name[..2];
                    format!("{path_root}/{shard}/{file_name}")
                } else {
                    format!("{path_root}/{file_name}")
                };
                // Ignore result.
                drop(fs::remove_file(full_path).await);
            }
        }
        Ok(())
    }

    move_old_cache(shared_context, rename_fn).await?;
    migrate_flat_to_sharded(shared_context, rename_fn).await?;

    add_files_for_folder(
        evicting_map,
        anchor_time,
        shared_context,
        block_size,
        DIGEST_FOLDER,
    )
    .await?;

    add_files_for_folder(
        evicting_map,
        anchor_time,
        shared_context,
        block_size,
        STR_FOLDER,
    )
    .await?;
    Ok(())
}

async fn prune_temp_path(temp_path: &str) -> Result<(), Error> {
    async fn prune_files_in_dir(dir_path: &str) -> Result<(), Error> {
        let (_permit, dir_handle) = fs::read_dir(dir_path)
            .await
            .err_tip(
                || "Failed opening temp directory to prune partial downloads in filesystem store",
            )?
            .into_inner();

        let mut read_dir_stream = ReadDirStream::new(dir_handle);
        while let Some(dir_entry) = read_dir_stream.next().await {
            let dir_entry = dir_entry?;
            let path = dir_entry.path();
            let metadata = dir_entry.metadata().await.ok();
            if metadata.as_ref().map_or(true, |m| m.is_file()) {
                if let Err(err) = fs::remove_file(&path).await {
                    warn!(?path, ?err, "Failed to delete temp file",);
                }
            }
        }
        Ok(())
    }

    prune_files_in_dir(&format!("{temp_path}/{STR_FOLDER}")).await?;
    // Prune both flat files in d/ and files in d/XX/ shard subdirectories.
    let digest_dir = format!("{temp_path}/{DIGEST_FOLDER}");
    prune_files_in_dir(&digest_dir).await?;
    for byte in 0u8..=255 {
        let shard_dir = format!("{digest_dir}/{byte:02x}");
        // Shard dirs may not exist yet (first startup before create_subdirs).
        if let Ok(()) = prune_files_in_dir(&shard_dir).await {
            // ok
        }
    }
    Ok(())
}

/// FL-681 Fix A fix-up (MAJOR-1b): outcome of
/// [`FilesystemStore::pin_digest_indefinite_or_time_bounded`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndefinitePinOutcome {
    /// The indefinite (held-until-BIS-ack) pin was taken — full protection.
    Indefinite,
    /// The indefinite cap was exhausted; a TIME-BOUNDED pin was taken
    /// instead (counts against `pin_cap`, not the indefinite cap). The
    /// blob is protected for ~120s — the pre-FL-681 floor — NOT held until
    /// BIS-ack. Under a sustained (>120s) outage the blob can still be
    /// lost; see the helper's doc-comment for the honest scope.
    TimeBoundedFallback,
    /// Neither pin could be taken (blob absent from the eviction map — an
    /// eviction race; the blob is already gone). The retry loop's slow-tier
    /// re-read self-heals if the source survives.
    Refused,
}

#[derive(Debug, MetricsComponent)]
pub struct FilesystemStore<Fe: FileEntry = FileEntryImpl> {
    #[metric]
    shared_context: Arc<SharedContext>,
    #[metric(group = "evicting_map")]
    evicting_map: Arc<FsEvictingMap<Fe>>,
    #[metric(help = "Block size of the configured filesystem")]
    block_size: u64,
    #[metric(help = "Size of the configured read buffer size")]
    read_buffer_size: usize,
    weak_self: Weak<Self>,
    rename_fn: fn(&OsStr, &OsStr) -> Result<(), std::io::Error>,
    /// Limits concurrent write operations to prevent disk I/O saturation.
    write_semaphore: Option<Semaphore>,
    /// Skip writes when a blob with the same key already exists (CAS dedup).
    content_is_immutable: bool,
    /// Call POSIX_FADV_DONTNEED after reads/writes to drop page cache pages.
    fadvise_dontneed: bool,
    /// Optional semaphore to limit concurrent large reads (None = disabled).
    large_read_semaphore: Option<tokio::sync::Semaphore>,
    #[metric(help = "Size threshold for large read limiting")]
    large_read_threshold: u64,
    /// Per-blob in-flight state for chunked-streaming uploads (#212
    /// Phase 2.1). Holds the open temp-file handle + per-blob async
    /// mutex so concurrent `pwrite` calls for the same digest serialize
    /// safely. Different digests use disjoint entries, so cross-blob
    /// chunks proceed in parallel. Zero memory cost when the
    /// `chunked_fast_slow` feature is OFF (the field is `cfg`-gated
    /// out of the struct entirely, so `MetricsComponent` derive +
    /// production binaries are byte-identical to the pre-Phase-2.1
    /// build).
    #[cfg(feature = "chunked_fast_slow")]
    chunked_partials: Arc<ChunkedPartialsMap>,
    /// #494-v3 Phase 2: per-digest multi-writer race-state registry for
    /// the `WriteChunkedV2` bidi RPC. Distinct from `chunked_partials`
    /// (which holds the `std::fs::File` handle); the two are 1:1 — one
    /// race-state per partial. Wrapping them separately keeps the v1
    /// path's open-file lifecycle independent of the v2 multi-writer
    /// state, so the v1 fall-back (old clients) keeps working without
    /// touching the new field.
    ///
    /// CAPPED AT N: bounded by the number of in-flight digests with at
    /// least one v2 writer attached; same operational ceiling as
    /// `chunked_partials` (one entry per concurrent chunked upload).
    /// Per-entry memory: `ChunkRaceState` ≈ 200 bytes header +
    /// `chunks_present` (32 bytes max for 256 chunks) + worst-case
    /// `chunks_in_flight` HashMap (~6 KiB at 256 entries × 321 writers).
    #[cfg(feature = "chunked_fast_slow")]
    chunked_race_registry: Arc<crate::chunked::chunked_race_state::ChunkRaceRegistry>,
}

impl<Fe: FileEntry> FilesystemStore<Fe> {
    pub async fn new(spec: &FilesystemSpec) -> Result<Arc<Self>, Error> {
        Self::new_with_timeout_and_rename_fn(spec, |from, to| std::fs::rename(from, to)).await
    }

    pub async fn new_with_timeout_and_rename_fn(
        spec: &FilesystemSpec,
        rename_fn: fn(&OsStr, &OsStr) -> Result<(), std::io::Error>,
    ) -> Result<Arc<Self>, Error> {
        async fn create_subdirs(path: &str) -> Result<(), Error> {
            fs::create_dir_all(format!("{path}/{STR_FOLDER}"))
                .await
                .err_tip(|| format!("Failed to create directory {path}/{STR_FOLDER}"))?;
            fs::create_dir_all(format!("{path}/{DIGEST_FOLDER}"))
                .await
                .err_tip(|| format!("Failed to create directory {path}/{DIGEST_FOLDER}"))?;
            // Create all 256 shard subdirectories (00-ff) under the digest
            // folder. This avoids create_dir_all on every write and reduces
            // per-directory file count from hundreds of thousands to ~1,500.
            for byte in 0u8..=255 {
                let shard = format!("{byte:02x}");
                fs::create_dir_all(format!("{path}/{DIGEST_FOLDER}/{shard}"))
                    .await
                    .err_tip(|| {
                        format!("Failed to create shard directory {path}/{DIGEST_FOLDER}/{shard}")
                    })?;
            }
            Ok(())
        }

        let now = SystemTime::now();

        let empty_policy = nativelink_config::stores::EvictionPolicy::default();
        let eviction_policy = spec.eviction_policy.as_ref().unwrap_or(&empty_policy);
        // FL-681: plumb the operator-tunable indefinite-pin byte budget
        // (F2 output blobs pinned-until-BIS-durable) down to the eviction
        // map's `indefinite_pin_cap`. `0` (the default) falls back to
        // `pin_cap` inside the constructor, so existing configs are
        // unchanged.
        let evicting_map = Arc::new(MokaEvictingMap::with_anchor_and_indefinite_cap(
            eviction_policy,
            now,
            spec.pending_bis_pin_max_bytes,
        ));

        // Create temp and content directories and the s and d subdirectories.

        create_subdirs(&spec.temp_path).await?;
        create_subdirs(&spec.content_path).await?;

        let shared_context = Arc::new(SharedContext {
            active_drop_spawns: AtomicU64::new(0),
            temp_path: spec.temp_path.clone(),
            content_path: spec.content_path.clone(),
        });

        let block_size = if spec.block_size == 0 {
            DEFAULT_BLOCK_SIZE
        } else {
            spec.block_size
        };
        add_files_to_cache(
            evicting_map.as_ref(),
            &now,
            &shared_context,
            block_size,
            rename_fn,
        )
        .await?;
        // Honor `insert_startup`'s post-batch-drain contract: the startup
        // load above goes through `insert_with_time` → `insert_startup`,
        // which intentionally defers `cache.run_pending_tasks()` for
        // throughput, putting the burden on the caller. Run it now so a
        // startup overshoot — on-disk content above the configured cap —
        // is bled down SYNCHRONOUSLY before `start_background_eviction`
        // takes over. Without this, an idle worker (no runtime `insert` to
        // kick moka's per-insert capacity check) stays over-cap
        // indefinitely.
        //
        // One of N contributors to #605 (worker disk-cap overshoot, 2026-05-29
        // production observation). NOT a complete fix on its own: workers
        // observed at 70–124 GiB on a 40 GiB cap exceed even moka's design
        // ceiling of `1.25 × max_bytes` (configured cap + `pin_cap` for the
        // side `pinned` map), so the residual must come from on-disk regions
        // outside moka's authority (rename-failure orphans, etc — tracked
        // separately).
        evicting_map.run_pending_tasks_and_drain().await;
        prune_temp_path(&shared_context.temp_path).await?;

        // #212 Phase 2.2-3 B1 fixup: GC any leftover `.holding` files
        // from a process killed between stage 1 (rename to .holding)
        // and stage 2 (rename to canonical). The .holding files live
        // under `content_path/d/XX/`, which `prune_temp_path` does NOT
        // touch (it only sweeps `temp_path`). NoOp on first startup.
        #[cfg(feature = "chunked_fast_slow")]
        chunked_prune_holding_partials(&shared_context.content_path).await?;

        let read_buffer_size = if spec.read_buffer_size == 0 {
            DEFAULT_BUFF_SIZE
        } else {
            spec.read_buffer_size as usize
        };
        let write_semaphore = if spec.max_concurrent_writes > 0 {
            Some(Semaphore::new(spec.max_concurrent_writes))
        } else {
            None
        };
        evicting_map.start_background_eviction();

        // #212 Phase 2.1 chunked-partials recovery: handled by the
        // existing `prune_temp_path` await above (line ~951), which
        // unconditionally `remove_file`s every entry in `<temp_path>/d/`
        // and `<temp_path>/d/XX/` shards — including all `.partial`
        // files. That is one valid degraded form of Q7=(c) recovery
        // (GC-everything; mirror re-uploads). The spec'd length-aware
        // rename-recovery (`if stat.len() == declared_size, rename →
        // VerifyStore validates downstream`; see plan §4 Q7 +
        // §7.4) is deferred to a later Phase 2.x — it requires
        // digest-aware validation that the prune sweep doesn't have.
        // TODO(#212): implement length-aware rename-recovery per
        // `.claude/plans/212-chunk-pinned-async-slow-writes.md` §4 Q7
        // and §7.4 once the Phase 2.3 driver lands.

        Ok(Arc::new_cyclic(|weak_self| Self {
            shared_context,
            evicting_map,
            block_size,
            read_buffer_size,
            weak_self: weak_self.clone(),
            rename_fn,
            write_semaphore,
            content_is_immutable: spec.content_is_immutable,
            fadvise_dontneed: spec.fadvise_dontneed,
            large_read_semaphore: if spec.max_concurrent_large_reads > 0 {
                Some(tokio::sync::Semaphore::new(spec.max_concurrent_large_reads))
            } else {
                None
            },
            large_read_threshold: spec.large_read_threshold_bytes,
            #[cfg(feature = "chunked_fast_slow")]
            chunked_partials: Arc::new(ChunkedPartialsMap::new()),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_race_registry: Arc::new(
                crate::chunked::chunked_race_state::ChunkRaceRegistry::new(),
            ),
        }))
    }

    pub fn get_arc(&self) -> Option<Arc<Self>> {
        self.weak_self.upgrade()
    }

    /// Pin a digest to prevent eviction during background upload.
    pub fn pin_digest(&self, digest: &DigestInfo) {
        let key: StoreKey<'static> = (*digest).into();
        self.evicting_map.pin_key(StoreKeyBorrow::from(key));
    }

    /// Pin a digest and report whether the pin succeeded.
    /// Returns `false` when the blob was not in the eviction map at the
    /// moment of pinning (typically already evicted) — callers can use
    /// this to detect eviction races.
    pub fn pin_digest_with_result(&self, digest: &DigestInfo) -> bool {
        let key: StoreKey<'static> = (*digest).into();
        self.evicting_map.pin_key(StoreKeyBorrow::from(key))
    }

    /// FL-681 Fix A: pin a digest INDEFINITELY — held until the server's
    /// BlobsInStableStorage ack (`unpin_digest`), EXEMPT from the
    /// `PIN_TIMEOUT_SECS` (120s) sweep. Used by worker-local F2
    /// deferred-output uploads so an output blob's anti-eviction pin is
    /// released ONLY by BIS-durability, never by the TTL — closing the
    /// 3,881-event silent-loss leak where the deferred-upload digest
    /// (absent from `in_flight_slow_writes` because F2 bypasses
    /// `FastSlowStore::update`) was demoted at 120s and lost.
    ///
    /// Returns `false` when the blob is absent (eviction race) OR when the
    /// indefinite-pin byte cap is exhausted — the latter is BACKPRESSURE:
    /// the caller MUST keep the source readable and retry the durability
    /// upload, never drop the blob.
    pub fn pin_digest_indefinite_with_result(&self, digest: &DigestInfo) -> bool {
        let key: StoreKey<'static> = (*digest).into();
        self.evicting_map
            .pin_key_indefinite(StoreKeyBorrow::from(key))
    }

    /// FL-681 Fix A fix-up (MAJOR-1b): pin a digest INDEFINITELY, falling
    /// back to a TIME-BOUNDED pin when the indefinite cap is exhausted.
    ///
    /// The fresh-pin indefinite cap-refusal previously left the blob FULLY
    /// LRU-evictable (no pin at all) — strictly weaker than a time-bounded
    /// pin and the exact FL-681 loss class under a sustained outage
    /// (cap-saturated + evicted + not-yet-durable). This helper closes the
    /// "no pin at all" gap: on cap refusal it takes a time-bounded
    /// `pin_key` (which counts against the TOTAL `pin_cap`, NOT the
    /// indefinite cap, so it admits even when the indefinite cap is full),
    /// restoring the pre-FL-681 ~120s synchronous protection window.
    ///
    /// HONEST SCOPE: this is a MITIGATION, not a durability close-out. Under
    /// a remote outage longer than `PIN_TIMEOUT_SECS` (120s) the
    /// retry-forever loop's `RemoteWrite` failures do NOT re-pin (the source
    /// was not lost), so the time-bounded pin's TTL eventually expires
    /// unrefreshed and the blob can still be lost — the same floor the
    /// synchronous path always had. The TRUE close-out is admission-side
    /// gating (don't admit an action whose outputs exceed pending-BIS-pin
    /// headroom), tracked as an FL-681 follow-up. This helper strictly
    /// improves on "fully evictable on cap-refusal" without claiming to
    /// eliminate the saturated-cap loss window.
    ///
    /// Returns the outcome so the caller can log + meter accurately.
    pub fn pin_digest_indefinite_or_time_bounded(
        &self,
        digest: &DigestInfo,
    ) -> IndefinitePinOutcome {
        if self.pin_digest_indefinite_with_result(digest) {
            return IndefinitePinOutcome::Indefinite;
        }
        // Indefinite cap exhausted (or eviction race). Try a time-bounded
        // pin so the blob is NOT left fully evictable. `pin_key` counts
        // against the total `pin_cap`, not the indefinite cap, so it can
        // still admit while the indefinite cap is saturated.
        if self.pin_digest_with_result(digest) {
            IndefinitePinOutcome::TimeBoundedFallback
        } else {
            IndefinitePinOutcome::Refused
        }
    }

    /// Unpin a digest, allowing eviction again.
    pub fn unpin_digest(&self, digest: &DigestInfo) {
        let key: StoreKey<'static> = (*digest).into();
        self.evicting_map.unpin_key(&key);
    }

    /// Test hook: drive the pin-expiry sweep deterministically. The
    /// production background loop in `start_background_eviction` calls
    /// this once per 10s tick. Integration tests for the auto-unpin →
    /// `failed_slow_writes` plumbing call it directly so they don't have
    /// to wait the real `PIN_TIMEOUT_SECS = 120s` deadline. Doc-hidden
    /// to keep the public API surface tight.
    #[doc(hidden)]
    pub async fn test_expire_stale_pins(&self) {
        self.evicting_map.expire_stale_pins().await;
    }

    /// FL-681 Fix A fix-up: bytes currently held by INDEFINITE
    /// (pinned-until-BIS-ack) pins. Doc-hidden test observability — lets
    /// the BIS-release-seam test assert that the BIS-ack `unpin_digest`
    /// frees the indefinite accounting at the production FilesystemStore
    /// type (not just the bare eviction map).
    #[doc(hidden)]
    pub fn indefinite_pinned_bytes(&self) -> u64 {
        self.evicting_map.indefinite_pinned_bytes()
    }

    /// FL-681 Follow-up A (MAJOR-1b close-out): `true` when the indefinite-pin
    /// cap has no headroom for the next fresh F2 output — i.e. its
    /// `pin_digest_indefinite_or_time_bounded` would fall back to a
    /// time-bounded pin (the sustained-outage loss window). The worker's
    /// action-acceptance path reads this and NAKs a new action with
    /// `Code::ResourceExhausted` so the scheduler re-queues it (producer
    /// backpressure) rather than admitting an output that cannot be
    /// pinned-until-durable. Snapshot — eventually-consistent, no lock held
    /// across the read. Never gates an uncapped store.
    #[must_use]
    pub fn indefinite_pin_saturated(&self) -> bool {
        self.evicting_map.indefinite_pin_saturated()
    }

    /// FL-681 Follow-up B (MAJOR-2 robust close-out): enumerate the worker's
    /// pending-BIS (indefinite-pinned) CAS digests. The worker folds these into
    /// the periodic BlobsAvailable heartbeat's `digest_infos` so a digest whose
    /// `mark_stable` was missed (transient server existence-check failure) is
    /// re-driven to BIS within a bounded number of heartbeat ticks, without
    /// waiting for a reconnect. String-keyed entries are skipped (mirrors
    /// `get_all_digests_with_timestamps`). Bounded by the indefinite-pin cap;
    /// self-pruning (a BIS-ack `unpin_digest` drops the digest from the set).
    pub fn indefinite_pinned_digests(&self) -> Vec<DigestInfo> {
        self.evicting_map
            .indefinite_pinned_digests()
            .into_iter()
            .filter_map(|key_borrow| match StoreKey::from(key_borrow) {
                StoreKey::Digest(digest) => Some(digest),
                _ => None,
            })
            .collect()
    }

    /// FL-681 Fix A fix-up: total bytes held by ALL pins (time-bounded +
    /// indefinite). Doc-hidden test observability — the MAJOR-1b test
    /// asserts a cap-refused F2 output still holds a (time-bounded) pin,
    /// i.e. `pinned_bytes > indefinite_pinned_bytes`, rather than being
    /// left fully evictable.
    #[doc(hidden)]
    pub fn pinned_bytes(&self) -> u64 {
        self.evicting_map.pinned_bytes()
    }

    /// Test hook: force a pinned digest's deadline past
    /// `PIN_TIMEOUT_SECS` so the next sweep treats it as stale. Returns
    /// `true` if the digest was pinned. Doc-hidden — paired with
    /// `test_expire_stale_pins` for deterministic auto-unpin tests.
    #[doc(hidden)]
    pub fn test_force_pin_expired(&self, digest: &DigestInfo) -> bool {
        let key: StoreKey<'static> = (*digest).into();
        self.evicting_map.test_force_pin_expired(&key)
    }

    /// Returns all digest entries in the cache with their absolute last-access
    /// timestamps (seconds since UNIX epoch). String-keyed entries are skipped.
    /// This is a peek-only operation and does NOT promote entries in the LRU.
    pub fn get_all_digests_with_timestamps(&self) -> Vec<(DigestInfo, i64)> {
        self.evicting_map
            .get_all_entries_with_timestamps()
            .into_iter()
            .filter_map(|(key_borrow, abs_timestamp)| {
                match StoreKey::from(key_borrow) {
                    StoreKey::Digest(digest) => Some((digest, abs_timestamp)),
                    _ => None,
                }
            })
            .collect()
    }

    /// Remove a digest's entry from the evicting map so the next
    /// `populate_fast_store` is forced to re-download from the slow store.
    pub async fn remove_entry_for_digest(&self, digest: &DigestInfo) {
        self.evicting_map.remove(&digest.into()).await;
    }

    pub async fn get_file_entry_for_digest(&self, digest: &DigestInfo) -> Result<Arc<Fe>, Error> {
        if is_zero_digest(digest) {
            return Ok(Arc::new(Fe::create(
                0,
                0,
                RwLock::new(EncodedFilePath {
                    shared_context: self.shared_context.clone(),
                    path_type: PathType::Content,
                    key: digest.into(),
                }),
            )));
        }
        self.evicting_map
            .get(&digest.into())
            .await
            .ok_or_else(|| make_err!(Code::NotFound, "{digest} not found in filesystem store. This may indicate the file was evicted due to cache pressure. Consider increasing 'max_bytes' in your filesystem store's eviction_policy configuration."))
    }

    /// Batch-retrieves file entries for multiple digests in a single lock
    /// acquisition on the EvictingMap, reducing contention compared to
    /// calling `get_file_entry_for_digest()` individually for each digest.
    pub async fn get_file_entries_batch(
        &self,
        digests: &[DigestInfo],
    ) -> Vec<Option<Arc<Fe>>> {
        // Separate zero digests (which don't go through evicting_map).
        let store_keys: Vec<StoreKey<'static>> = digests
            .iter()
            .filter(|d| !is_zero_digest(**d))
            .map(|d| (*d).into())
            .collect();

        let batch_results = self.evicting_map.get_many(store_keys.iter()).await;

        // Reassemble results, inserting zero-digest entries where needed.
        // Zero-digest files have no backing file on disk, so we return None
        // to let the caller fall back to creating an empty file directly.
        let mut batch_iter = batch_results.into_iter();
        digests
            .iter()
            .map(|digest| {
                if is_zero_digest(*digest) {
                    None
                } else {
                    batch_iter.next().flatten()
                }
            })
            .collect()
    }

    async fn update_file(
        self: Pin<&Self>,
        mut entry: Fe,
        temp_file: fs::FileSlot,
        final_key: StoreKey<'static>,
        mut reader: DropCloserReadHalf,
    ) -> Result<(), Error> {
        let write_start = std::time::Instant::now();
        let (data_size, temp_file) = fs::write_file_from_channel(temp_file, &mut reader)
            .await
            .err_tip(|| "Failed to write data into filesystem store")?;
        let write_ms = write_start.elapsed().as_millis();

        if self.fadvise_dontneed {
            temp_file.advise_dontneed();
        }

        let _permit = if let Some(sem) = &self.write_semaphore {
            Some(
                sem.acquire()
                    .await
                    .map_err(|_| make_err!(Code::Internal, "Write semaphore closed"))?,
            )
        } else {
            None
        };

        trace!(?temp_file, "Dropping file to update_file");
        drop(temp_file);

        *entry.data_size_mut() = data_size;
        let emplace_start = std::time::Instant::now();
        let result = self.emplace_file(final_key.borrow().into_owned(), Arc::new(entry)).await;
        let emplace_ms = emplace_start.elapsed().as_millis();

        let total_ms = write_ms + emplace_ms;
        if total_ms > 100 {
            debug!(
                key = %final_key.as_str(),
                total_ms,
                write_ms,
                emplace_ms,
                data_size,
                "update_file slow phases (>100ms)"
            );
        }
        result
    }

    async fn emplace_file(&self, key: StoreKey<'static>, entry: Arc<Fe>) -> Result<(), Error> {
        // This sequence of events is quite tricky to understand due to the amount of triggers that
        // happen, async'ness of it and the locking. So here is a breakdown of what happens:
        // 1. Here will hold a write lock on any file operations of this FileEntry.
        // 2. Then insert the entry into the evicting map. This may trigger an eviction of other
        //    entries.
        // 3. Eviction triggers `unref()`, which grabs a write lock on the evicted FileEntry
        //    during the rename.
        // 4. It should be impossible for items to be added while eviction is happening, so there
        //    should not be a deadlock possibility. However, it is possible for the new FileEntry
        //    to be evicted before the file is moved into place. Eviction of the newly inserted
        //    item is not possible within the `insert()` call because the write lock inside the
        //    eviction map. If an eviction of new item happens after `insert()` but before
        //    `rename()` then we get to finish our operation because the `unref()` of the new item
        //    will be blocked on us because we currently have the lock.
        // 5. Move the file into place. Since we hold a write lock still anyone that gets our new
        //    FileEntry (which has not yet been placed on disk) will not be able to read the file's
        //    contents until we release the lock.
        let evicting_map = self.evicting_map.clone();
        let rename_fn = self.rename_fn;
        let content_is_immutable = self.content_is_immutable;

        // We need to guarantee that this will get to the end even if the parent future is dropped.
        // See: https://github.com/TraceMachina/nativelink/issues/495
        background_spawn!("filesystem_store_emplace_file", async move {
            let emplace_timer = std::time::Instant::now();

            // CAS optimization: if the key already exists and the store is
            // content-addressable (immutable), just promote it in the LRU
            // instead of replacing it. Same digest = same content, so
            // replacing triggers an unnecessary unref (filesystem rename).
            // Skip for mutable stores (AC) where the same key can map to
            // different values.
            if content_is_immutable {
                let owned_key = key.borrow().into_owned();
                if evicting_map.size_for_key(&owned_key).await.is_some() {
                    // Key exists, content identical — skip insert+unref cycle.
                    return Ok(());
                }
            }

            evicting_map
                .insert(key.borrow().into_owned().into(), entry.clone())
                .await;
            let map_insert_ms = emplace_timer.elapsed().as_millis();

            // The insert might have resulted in an eviction/unref so we need to check
            // it still exists in there. But first, get the lock...
            let mut encoded_file_path = entry.get_encoded_file_path().write().await;
            let lock_acquire_ms = emplace_timer.elapsed().as_millis() - map_insert_ms;

            // Check that OUR specific entry is still in the map. A concurrent
            // write for the same key may have replaced our entry (calling
            // unref which deletes our temp file). Checking just the key
            // would pass if the replacement entry exists, but our temp file
            // would already be deleted → ENOENT on rename.
            //
            // NOTE: returning Ok here is a known partial-fix. The Some/None
            // result of evicting_map.get cannot distinguish between (a) a
            // replacement Arc that holds equivalent content (data IS cached
            // under a different Arc → Ok is correct) and (b) the key being
            // gone entirely after eviction (data IS NOT cached → Ok is a
            // lie). Returning Err here would surface the eviction case but
            // also fail legitimate replacement cases AND breaks 3
            // pre-existing tests that encode this contract. Callers that
            // need to immediately use the entry must verify presence — see
            // FastSlowStore::populate_fast_store_unchecked which does
            // post-write has() + retry once.
            let still_ours = match evicting_map.get(&key).await {
                Some(map_entry) => Arc::ptr_eq(&map_entry, &entry),
                None => false,
            };
            if !still_ours {
                info!(%key, "Got eviction or replacement while emplacing, dropping");
                return Ok(());
            }

            let final_path = get_file_path_raw(
                &PathType::Content,
                encoded_file_path.shared_context.as_ref(),
                &key,
            );

            let from_path: OsString = encoded_file_path.get_file_path().into_owned();
            let final_path_owned: OsString = final_path.into_owned();
            // Run rename + set_permissions on a blocking thread to avoid
            // stalling the async runtime with syscalls.
            let from_clone = from_path.clone();
            let to_clone = final_path_owned.clone();
            let rename_start = std::time::Instant::now();
            let result = tokio::task::spawn_blocking(move || -> Result<(u128, u128), Error> {
                let rename_syscall_start = std::time::Instant::now();
                (rename_fn)(&from_clone, &to_clone)?;
                let rename_syscall_ms = rename_syscall_start.elapsed().as_millis();

                // Pre-set CAS file permissions to read+execute (0o555) so that
                // hardlinked copies already have correct permissions without
                // needing a per-file chmod during input materialization.
                let chmod_ms;
                #[cfg(target_family = "unix")]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let chmod_start = std::time::Instant::now();
                    let perms = std::fs::Permissions::from_mode(0o555);
                    if let Err(err) = std::fs::set_permissions(&to_clone, perms) {
                        tracing::warn!(?err, path = ?to_clone, "Failed to set CAS file permissions to 0o555");
                    }
                    chmod_ms = chmod_start.elapsed().as_millis();
                }
                #[cfg(not(target_family = "unix"))]
                {
                    chmod_ms = 0;
                }
                Ok((rename_syscall_ms, chmod_ms))
            })
            .await
            .map_err(|e| make_err!(Code::Internal, "Rename task join error: {e:?}"))
            .and_then(|r| r.err_tip(|| "Failed to rename temp file to final path"));
            let rename_total_ms = rename_start.elapsed().as_millis();

            match &result {
                Ok((rename_syscall_ms, chmod_ms)) => {
                    let emplace_total_ms = emplace_timer.elapsed().as_millis();
                    if emplace_total_ms > 100 {
                        warn!(
                            %key,
                            emplace_total_ms,
                            map_insert_ms,
                            lock_acquire_ms,
                            rename_total_ms,
                            rename_syscall_ms,
                            chmod_ms,
                            "emplace_file slow (>100ms)"
                        );
                    }
                    encoded_file_path.path_type = PathType::Content;
                    encoded_file_path.key = key;
                    Ok(())
                }
                Err(err) => {
                    // In the event our move from temp file to final file fails we need to ensure
                    // we remove the entry from our map.
                    // Remember: At this point it is possible for another thread to have a reference
                    // to `entry`, so we can't delete the file, only drop() should ever delete files.
                    error!(?err, ?from_path, ?final_path_owned, "Failed to rename file",);
                    // Warning: To prevent deadlock we need to release our lock or during
                    // `remove_if()` it will call `unref()`, which triggers a write-lock on
                    // `encoded_file_path`.
                    drop(encoded_file_path);
                    // It is possible that the item in our map is no longer the item we inserted,
                    // So, we need to conditionally remove it only if the pointers are the same.

                    evicting_map
                        .remove_if(&key, |map_entry| Arc::<Fe>::ptr_eq(map_entry, &entry))
                        .await;
                    Err(make_err!(
                        Code::Internal,
                        "Failed to rename temp file to final path: {err:?}"
                    ))
                }
            }
        })
        .await
        .err_tip(|| "Failed to create spawn in filesystem store update_file")?
    }
}

// =============================================================================
// #212 Phase 2.1 — chunked-streaming primitives (Q10=(a) internal API).
// =============================================================================
//
// Per-chunk write-at-offset + atomic-commit + discard primitives that the
// Phase 2.3 per-blob driver consumes. Behind the `chunked_fast_slow`
// feature flag; the entire impl block is `cfg`-gated out of the default
// build, preserving byte-identity per Phase 1's invariant.
//
// The actual I/O lives in `crate::chunked::chunked_filesystem`; this
// impl block is a thin adapter that (a) routes through the
// FilesystemStore's per-store `chunked_partials` map (so two
// FilesystemStore instances on disjoint temp paths don't share state)
// and (b) constructs the final CAS path from the existing
// `to_full_path_from_key` layout (so committed chunked blobs land at
// the SAME on-disk path as legacy `update`-produced blobs and are
// discoverable by the existing `add_files_to_cache` startup walk).
//
// **NO behavior changes** to existing `update`, `update_oneshot`,
// `get_part`, `has`, `has_with_results`. Phase 2.3 will plumb the new
// APIs from the per-blob driver without touching any of the existing
// trait methods. Composability with outer layers (VerifyStore,
// ExistenceCacheStore, etc.) is preserved because outer layers only
// see the unchanged `StoreDriver` surface.
#[cfg(feature = "chunked_fast_slow")]
#[allow(dead_code, reason = "Phase 2.1 SKELETON; consumers land in Phase 2.3 (#212)")]
impl<Fe: FileEntry> FilesystemStore<Fe> {
    /// Write a chunk at the given byte offset for an in-flight chunked
    /// upload. On first call for a digest: creates a sparse temp file at
    /// `<temp_path>/d/<XX>/<digest>.partial`. Subsequent calls reuse the
    /// open fd. Concurrent calls for the SAME digest serialize via a
    /// per-blob async mutex; concurrent calls for DIFFERENT digests
    /// proceed in parallel.
    ///
    /// Per design Q2=(a): out-of-order safe (offsets may arrive in any
    /// order — sparse `pwrite` semantics extend the file as needed).
    /// Per Q3=(a): callers should use `CHUNK_SIZE`-aligned offsets in
    /// production (1 MiB), matching the ZFS `recordsize=1M` to avoid
    /// partial-record write amplification; this method does NOT enforce
    /// alignment because Phase 2.x tests want to exercise sub-chunk
    /// offsets.
    ///
    /// **NO `fsync`.** Per CLAUDE.md hard rule and Q7=(c) durability
    /// model. Crash recovery uses file length only; mirror covers
    /// re-upload of unflushed bytes.
    ///
    /// Phase 2.3 wiring: the per-blob driver task calls this from its
    /// `recv()` loop on each `ChunkWork` item. The `ChunkWork`'s
    /// `_permit` (held by the driver) is released when the driver
    /// drops the `ChunkWork`, which happens after this call returns.
    pub async fn write_chunk_at_offset(
        &self,
        digest: &DigestInfo,
        chunk_offset: u64,
        chunk_bytes: Bytes,
    ) -> Result<(), Error> {
        chunked_write_chunk_at_offset(
            &self.chunked_partials,
            &self.shared_context.temp_path,
            digest,
            chunk_offset,
            chunk_bytes,
        )
        .await
    }

    /// #47 b1 Phase 2 Step 2: open the partial fd AND insert the io_uring
    /// marker variant into the in-process map. Returns the
    /// `Arc<std::fs::File>` that the per-blob writer task will own. The
    /// map entry only pins `(path, declared_size)` so the
    /// variant-agnostic commit / discard code keeps working.
    ///
    /// Only available when `is_io_uring_available()` returns true at
    /// driver-spawn time — the io_uring path bypasses the spawn_blocking
    /// pool mutex (the dominant cost per FL-402 RCA). On non-io-uring
    /// kernels, the driver continues to call [`Self::write_chunk_at_offset`]
    /// per chunk (Path B / fallback; zero behavior change).
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub async fn open_chunked_partial_marker(
        &self,
        digest: DigestInfo,
    ) -> Result<std::sync::Arc<std::fs::File>, Error> {
        crate::chunked::chunked_filesystem::open_or_create_partial_marker(
            &self.chunked_partials,
            digest,
            &self.shared_context.temp_path,
        )
        .await
    }

    /// Read-only accessor for the content_path the store is rooted at.
    /// Used by the Phase 2.3 chunked driver to compute the final CAS
    /// path for end-to-end SHA-256 verification (re-reading the
    /// committed file). Not on a hot path; metric / test use only
    /// otherwise.
    pub fn content_path_for_chunked(&self) -> &str {
        &self.shared_context.content_path
    }

    /// #494-v3 Phase 2: accessor for the per-store
    /// `ChunkRaceRegistry`. Used by the `WriteChunkedV2` handler to
    /// admit chunks from concurrent writers via the per-digest
    /// race-state.
    pub fn chunked_race_registry(
        &self,
    ) -> &Arc<crate::chunked::chunked_race_state::ChunkRaceRegistry> {
        &self.chunked_race_registry
    }

    /// #494-v3 Phase 2: get-or-create the race-state for `digest`,
    /// using the FilesystemStore's `chunked_partials` to mint the
    /// underlying `.partial` file. The first caller for a digest opens
    /// the file (via `write_chunk_at_offset` → `open_or_create_partial`
    /// indirectly); the race-state itself is constructed lazily on
    /// first call here.
    pub fn race_state_for_digest(
        &self,
        digest: &DigestInfo,
        chunk_size: u32,
    ) -> Arc<crate::chunked::chunked_race_state::ChunkRaceState> {
        let partial_path = crate::chunked::chunked_filesystem::partial_temp_path(
            &self.shared_context.temp_path,
            digest,
        );
        self.chunked_race_registry.get_or_create(*digest, || {
            crate::chunked::chunked_race_state::ChunkRaceState::new(
                *digest,
                chunk_size,
                partial_path,
            )
        })
    }

    /// #494-v3 Phase 2 (FIX-4): atomic get-or-create + attach. Holds the
    /// registry mutex across both steps so a concurrent
    /// `try_remove_if_unused` can't split concurrent writers across
    /// two distinct race-states. Returns `(Arc<ChunkRaceState>, RaceWriterGuard)`
    /// — guard's Drop detaches.
    pub fn race_state_for_digest_and_attach(
        &self,
        digest: &DigestInfo,
        chunk_size: u32,
        writer_id: crate::chunked::chunked_race_state::WriterId,
    ) -> (
        Arc<crate::chunked::chunked_race_state::ChunkRaceState>,
        crate::chunked::chunked_race_state::RaceWriterGuard,
    ) {
        let partial_path = crate::chunked::chunked_filesystem::partial_temp_path(
            &self.shared_context.temp_path,
            digest,
        );
        self.chunked_race_registry
            .get_or_create_and_attach(*digest, writer_id, || {
                crate::chunked::chunked_race_state::ChunkRaceState::new(
                    *digest,
                    chunk_size,
                    partial_path,
                )
            })
    }

    /// #497 Option 1: atomic get-or-create + try-attach as single-stream
    /// owner. Used by v1 paths (Bazel ByteStream chunked dispatcher,
    /// worker WriteChunked v1) to claim exclusive write authority on a
    /// digest so concurrent v2 writers transition to AwaitCommit.
    ///
    /// Returns the race-state Arc, a `RaceWriterGuard` (always pinning
    /// the registry entry for the caller's lifetime), and the attachment
    /// outcome:
    ///   - `Owner`: caller proceeds with the v1 write path. Caller MUST
    ///     construct a `SingleStreamOwnerGuard` (NOT returned by this
    ///     function — the owner-guard's lifetime is tied to the caller's
    ///     commit pipeline). The `RaceWriterGuard` returned here keeps
    ///     the entry pinned even after `relinquish` of the owner-guard,
    ///     so a sibling v2 writer arriving after the publish always sees
    ///     the SAME race-state with `commit_done_flag = true`.
    ///   - `AwaitCommit`: another writer (single-stream owner OR v2
    ///     multi-chunk writers) is active. Caller MUST drain its inbound
    ///     reader to EOF and then await `commit_done`. The
    ///     `RaceWriterGuard` keeps the entry alive so the published
    ///     result is observable.
    pub fn race_state_for_digest_and_attach_single_stream(
        &self,
        digest: &DigestInfo,
        chunk_size: u32,
        writer_id: crate::chunked::chunked_race_state::WriterId,
    ) -> (
        Arc<crate::chunked::chunked_race_state::ChunkRaceState>,
        crate::chunked::chunked_race_state::RaceWriterGuard,
        crate::chunked::chunked_race_state::SingleStreamAttachOutcome,
    ) {
        let partial_path = crate::chunked::chunked_filesystem::partial_temp_path(
            &self.shared_context.temp_path,
            digest,
        );
        self.chunked_race_registry
            .get_or_create_and_attach_single_stream(*digest, writer_id, || {
                crate::chunked::chunked_race_state::ChunkRaceState::new(
                    *digest,
                    chunk_size,
                    partial_path,
                )
            })
    }

    /// #494-v3 Phase 2: drop the race-state for `digest` if no writers
    /// are still attached. Returns the removed Arc on success. Used by
    /// the commit-runner after `publish_commit_result`.
    pub fn try_drop_race_state(
        &self,
        digest: &DigestInfo,
    ) -> Option<Arc<crate::chunked::chunked_race_state::ChunkRaceState>> {
        self.chunked_race_registry.try_remove_if_unused(digest)
    }

    /// Read-only accessor for the temp_path the store is rooted at.
    /// Used by the Phase 2.3 chunked driver / tests to observe the
    /// `.partial` file path for the M-testing-2 §6.7 trigger (b)
    /// shutdown-deadline regression test.
    pub fn temp_path_for_chunked(&self) -> &str {
        &self.shared_context.temp_path
    }

    /// Read-only accessor returning the on-disk `.partial` path for a
    /// chunked in-flight blob. Used by integration tests that observe
    /// disk state across the §6.7 termination triggers (especially
    /// the #213 d-s-r MAJOR-1 eager-GC test). Pure function over
    /// `temp_path_for_chunked()` + `digest`; no I/O.
    ///
    /// #213 reviewer M3 fixup: test-only API (no production caller);
    /// gated on `#[cfg(any(test, feature = "test-utils"))]` so
    /// production builds cannot reach it. Cross-crate access from the
    /// `nativelink-service` `chunked_write_handler_test` integration
    /// test (which already requires the `test-utils` feature) works
    /// because the `nativelink-service/test-utils` feature pulls in
    /// `nativelink-store/test-utils`. `#[doc(hidden)]` keeps this out
    /// of the rendered API docs.
    ///
    /// **Why `#[cfg(any(test, feature = "test-utils"))]` + `pub` rather
    /// than `pub(crate)`?** Two narrowing strategies coexist in this
    /// codebase: (a) `pub(crate)` for purely intra-crate test/internal
    /// helpers (e.g. `chunked_filesystem`'s
    /// `partial_temp_path` / `discard_chunked` consumed by
    /// `chunked_driver` in the SAME crate); (b) `#[cfg(any(test,
    /// feature = "test-utils"))]` + `#[doc(hidden)]` + `pub` for
    /// CROSS-CRATE test surfaces that integration tests in OTHER
    /// crates need to reach. `pub(crate)` won't traverse a crate
    /// boundary; `pub` alone leaks into production builds. The
    /// double-narrow form (`#[cfg]` to compile out of production +
    /// `#[doc(hidden)]` to suppress rustdoc + `pub` to allow the
    /// cross-crate import under the feature) gives both.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn partial_path_for_digest(&self, digest: &nativelink_util::common::DigestInfo) -> std::path::PathBuf {
        crate::chunked::chunked_filesystem::partial_temp_path(self.temp_path_for_chunked(), digest)
    }

    /// Read-only accessor returning whether a chunked partial for
    /// `digest` is currently registered in the in-process
    /// `chunked_partials` map. Used by integration tests to wait for
    /// the in-flight entry to be fully registered before triggering
    /// upstream-disconnect — `partial_path_for_digest`'s `metadata()`
    /// can succeed before the map insert completes (the file is
    /// created during `open_or_create_partial` BEFORE the map insert),
    /// so a race-free test must check map registration too.
    ///
    /// #213 reviewer M3 fixup: test-only API (no production caller);
    /// gated on `#[cfg(any(test, feature = "test-utils"))]`.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn has_in_flight_chunked_partial(&self, digest: &nativelink_util::common::DigestInfo) -> bool {
        self.chunked_partials.contains(digest)
    }

    /// #213 reviewer round-2 MAJOR-B (M2 mutation test): register a
    /// per-digest pre-discard delay so a downstream
    /// `chunked_filesystem::discard_chunked` await sleeps for
    /// `delay_ms` before doing the actual unlink. Used by integration
    /// tests that exercise the post-error cleanup contract — the
    /// `tokio::time::timeout(DISCARD_PARTIAL_TIMEOUT, ...)` /
    /// `tokio::time::timeout(DISCARD_AFTER_FAILURE_TIMEOUT, ...)`
    /// wraps must FIRE under wedged-slow-tier conditions and the
    /// caller must observe a result within the bound, instead of
    /// hanging forever.
    ///
    /// Gated on `#[cfg(any(test, feature = "test-utils"))]` so
    /// production builds cannot register a delay (the
    /// `chunked_filesystem`-side lookup is also gated under
    /// `#[cfg(test)]`, so production binaries compile out the entire
    /// path). Cross-crate access from `nativelink-service`'s
    /// integration tests works via the existing
    /// `nativelink-service/test-utils → nativelink-store/test-utils`
    /// feature chain. `#[doc(hidden)]` keeps this out of the rendered
    /// API docs.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn set_test_pre_discard_delay_ms(
        &self,
        digest: &nativelink_util::common::DigestInfo,
        delay_ms: u64,
    ) {
        crate::chunked::chunked_filesystem::TEST_PRE_DISCARD_DELAY_MS_BY_DIGEST
            .lock()
            .insert(*digest, delay_ms);
    }

    /// #213 reviewer round-2 MAJOR-B (M2 mutation test): cleanup
    /// counterpart to [`Self::set_test_pre_discard_delay_ms`]. Tests
    /// MUST call this (or use a `Drop`-based scope guard) so a panic
    /// doesn't leak the entry across tests.
    ///
    /// Gated identically to the setter.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn clear_test_pre_discard_delay(
        &self,
        digest: &nativelink_util::common::DigestInfo,
    ) {
        crate::chunked::chunked_filesystem::TEST_PRE_DISCARD_DELAY_MS_BY_DIGEST
            .lock()
            .remove(digest);
    }

    /// Atomic finalize: verify the temp file's actual length matches
    /// `expected_size` (per Q7=(c) trust file length only), then
    /// rename to the final CAS path with mode 0o555.
    ///
    /// On length mismatch: returns `Err(Code::InvalidArgument, ...)`
    /// and LEAVES the temp file in place + the in-flight state entry
    /// in the map. The caller (driver) MUST follow up with
    /// `discard_chunked` to clean up. This split lets the driver
    /// inspect / log the failed partial before discarding.
    ///
    /// On success: removes the in-flight state entry. The new CAS file
    /// is inserted into the FilesystemStore's `evicting_map` by stage 2
    /// (`finalize_holding`) AFTER the end-to-end SHA-256 verify on the
    /// `.holding` file passes (#247 fix — see `finalize_holding` doc).
    /// Stage 1 itself does not emplace because the bytes have only
    /// passed per-chunk hash checks at this point; the canonical CAS
    /// path is intentionally not exposed to readers until the e2e
    /// digest verify succeeds and the `.holding` → canonical rename in
    /// stage 2 completes.
    pub async fn commit_chunked(
        &self,
        digest: &DigestInfo,
        expected_size: u64,
    ) -> Result<(), Error> {
        // B1 fixup: stage 1 only — rename to `.holding`. Stage 2
        // (finalize_holding) runs from the driver AFTER the end-to-end
        // SHA-256 verify on the .holding file. Splitting the rename
        // collapses the cancellation window where a corrupt-but-canonically-
        // named file could land at the CAS path.
        let holding_path = chunked_holding_path(&self.shared_context.content_path, digest);
        chunked_commit_to_holding(&self.chunked_partials, digest, expected_size, holding_path)
            .await
    }

    /// B1 fixup: stage 2 of the two-stage commit. Renames the
    /// `<digest>.holding` file to the canonical CAS path and chmods to
    /// 0o555. Removes the in-flight tracker entry on success.
    /// Used by the Phase 2.3 `commit_and_verify` driver code AFTER
    /// the end-to-end SHA-256 verify against the holding file passes.
    ///
    /// #247 fix: after the rename succeeds, ALSO insert a `FileEntry`
    /// pointing at the canonical CAS path into the `evicting_map` so
    /// that `has_with_results` (which consults the in-memory index
    /// only — it never stats the disk) sees the freshly-committed
    /// blob immediately. Without this, the chunked driver returned
    /// success but the file was invisible to `has()` until the next
    /// `FilesystemStore::new` startup walk over `content_path/d/`.
    /// In production this caused `FastSlowStore::run_producer` to
    /// fall back to peer-fetch (`WorkerProxyStore`) on freshly-
    /// committed blobs → partial-byte responses → Bazel hash
    /// mismatch → build wedge.
    ///
    /// Mirrors the `add_files_to_cache` startup-walk emplace pattern
    /// (filesystem_store.rs:550-591): the file is already at its
    /// canonical `PathType::Content` location, so we construct the
    /// `FileEntry` directly via `Fe::create(...)` (no I/O) and
    /// `evicting_map.insert(...)`. The legacy `update_file` path
    /// (filesystem_store.rs:1167) instead uses `emplace_file` because
    /// THAT path's file is in `PathType::Temp` and the rename to
    /// canonical happens INSIDE `emplace_file`; the chunked path
    /// already did its own rename in `chunked_finalize_holding`, so
    /// `emplace_file`'s rename would be a no-op + an unwanted
    /// pre-rename lock dance.
    ///
    /// CAS-immutable optimization: mirror `emplace_file`'s
    /// short-circuit at filesystem_store.rs:1217-1223 — if the key
    /// already exists in the map for an immutable store, skip the
    /// insert (same digest = same content; the existing entry already
    /// points at the same on-disk path, which the rename just
    /// overwrote with byte-identical content).
    ///
    /// Lock ordering with `ChunkedPartialsMap` removal: the partial-
    /// map removal happens INSIDE `chunked_finalize_holding` (in
    /// `chunked_filesystem.rs:694`), which runs BEFORE this insert.
    /// That ordering matters for the chunked-pin read path: the
    /// `try_get_chunk_from_pin` accessor reads the in-memory pin,
    /// which is held by the per-blob `ChunkInProgress` Arc. Once the
    /// chunked driver clears its pin (post `await_completion`), the
    /// reader cascade falls through to `has_with_results`. By
    /// emplacing AFTER the partial-map removal but BEFORE returning
    /// to the driver (which then clears the pin), every reader
    /// arriving after the driver-task pin clear will find the entry
    /// in `evicting_map`. There is no window where the file is on
    /// disk and indexed in NEITHER structure.
    ///
    /// **Cancellation-safety:** the post-rename insert runs inside a
    /// `background_spawn!` so that if the caller's future is dropped
    /// after the rename succeeded but before the insert completes,
    /// the spawned task still runs to completion and the index is
    /// updated. Without this, an aborted driver task (server
    /// shutdown, panic, cancellation) could leave the file on disk
    /// without an `evicting_map` entry — a narrower-window
    /// re-introduction of #247. Mirrors the `emplace_file`
    /// `background_spawn!` pattern (filesystem_store.rs:1208) which
    /// was added for the same reason against nativelink#495.
    ///
    /// **#256 duplicate-commit guard.** Two parallel chunked writers
    /// for the SAME digest (the chunked path is always digest-keyed —
    /// `commit_chunked` takes `&DigestInfo`) racing through
    /// `finalize_holding` would, pre-#256-fix, both rename their
    /// `.holding` file → canonical CAS path (the second rename
    /// overwrites the first; OK because byte-identical) AND both
    /// invoke `evicting_map.insert(key, new_arc)`. The SECOND
    /// `insert` captures the FIRST commit's `Arc<FileEntry>` as the
    /// "old" value and calls `old.unref().await`, which renames the
    /// canonical CAS file → `temp_path-cas/d/XX/<temp-key>` (an
    /// orphan path) and immediately `Drop`s the temp via a
    /// `background_spawn!`-ed `remove_file`. The new `evicting_map`
    /// entry now claims `path_type: Content` at the canonical path,
    /// but the file is gone. Subsequent `has_with_results(key)`
    /// returns `Some(size)`; subsequent `get_part(key)` opens the
    /// canonical path and gets ENOENT → "Stale filesystem cache
    /// entry" → `FastSlowStore::run_producer` PHANTOM BLOB warn →
    /// fallback to `WorkerProxyStore` peer-fetch → partial-byte
    /// response → Bazel `OutputDigestMismatchException` → build wedge.
    ///
    /// Production trace (PID 570543, 2026-05-05): 575 PHANTOM BLOB
    /// events between 06:23 and 08:59 PDT. 89% (483/543) of unique
    /// phantom digests had ≥2 chunked-commit log lines preceding the
    /// phantom; modal commit count was 3 (320/543 cases).
    ///
    /// Fix: detect the duplicate at the START of `finalize_holding`.
    /// If `evicting_map.size_for_key(key)` is `Some`, the canonical
    /// CAS file already exists at the right path AND the index
    /// already points at it. Per CAS immutability (digest = content),
    /// the in-flight `.holding` file is byte-identical to what the
    /// existing entry points at. Just unlink the `.holding` file +
    /// remove our entry from the in-flight partials map + return Ok
    /// — without touching the canonical CAS file or the
    /// `evicting_map` entry. Critically: this gate runs BEFORE the
    /// rename, so the existing entry's `Arc<FileEntry>` is never
    /// captured by an `evicting_map.insert(...)` call and never
    /// `unref`-ed.
    ///
    /// Why the prior `content_is_immutable` gate (lines 1657-1660 in
    /// the pre-#256 version) didn't fire: production deploys run
    /// `FilesystemStore` as the slow tier of `FastSlowStore` without
    /// setting `content_is_immutable: true` (default at
    /// `nativelink-config/src/stores.rs:717` is `false`). The chunked
    /// path is INTRINSICALLY content-addressable (digest-keyed only),
    /// so the `content_is_immutable` flag — meaningful only for the
    /// `String`-keyed AC-store usage of `FilesystemStore` — is not
    /// the right gate for THIS path. The fix uses the chunked-path
    /// invariant (digest = content) directly.
    pub async fn finalize_holding(&self, digest: &DigestInfo) -> Result<(), Error> {
        let holding_path = chunked_holding_path(&self.shared_context.content_path, digest);
        let key: StoreKey<'static> = (*digest).into();
        let final_os = to_full_path_from_key(&self.shared_context.content_path, &key);
        let final_path = std::path::PathBuf::from(final_os);

        // #256 duplicate-commit guard: if the digest is already in the
        // evicting_map, the canonical CAS file is already on disk and
        // already indexed; the in-flight .holding file is byte-identical
        // (CAS invariant). Doing the rename + insert anyway would have
        // the second insert's old-Arc unref steal the canonical file. So:
        // unlink the .holding file + remove the in-flight partial entry
        // + return Ok WITHOUT renaming or inserting.
        // #256 duplicate-commit guard: if the digest is already in the
        // evicting_map, the canonical CAS file is already on disk and
        // already indexed; the in-flight .holding file is byte-identical
        // (CAS invariant). Doing the rename + insert anyway would have
        // the second insert's old-Arc unref steal the canonical file. So:
        // unlink the .holding file + remove the in-flight partial entry
        // + return Ok WITHOUT renaming or inserting.
        if self.evicting_map.size_for_key(&key).await.is_some() {
            // Best-effort unlink the holding file (NotFound is OK —
            // covers the case where a sibling concurrent caller already
            // unlinked it; `chunked_unlink_holding` itself treats
            // NotFound as Ok).
            chunked_unlink_holding(holding_path).await?;
            // Drop our in-flight partial entry. Stage 1
            // (`commit_chunked` / `chunked_commit_to_holding`) leaves
            // the entry in the partials map (chunked_filesystem.rs:696
            // "DO NOT remove from the in-flight map here") expecting
            // `chunked_finalize_holding` to remove it after the SHA-256
            // verify + rename. We're skipping that rename for the
            // duplicate case, so we own the cleanup. `discard_chunked`
            // is idempotent: returns Ok via the "no in-flight state"
            // branch if the entry is already absent.
            chunked_discard(&self.chunked_partials, digest).await?;
            return Ok(());
        }

        chunked_finalize_holding(&self.chunked_partials, digest, holding_path, final_path).await?;

        // Move the post-rename index-update onto a background task so
        // it cannot be cancelled mid-sequence by the caller. From here
        // to the spawn `tokio::spawn` is sync-only — there is no
        // `.await` between `chunked_finalize_holding` resolving Ok and
        // the spawn point — so the rename → insert pair is atomic w.r.t.
        // caller-cancellation.
        let evicting_map = self.evicting_map.clone();
        let block_size = self.block_size;
        let shared_context = self.shared_context.clone();
        let key_for_task: StoreKey<'static> = key.borrow().into_owned();
        let data_size = digest.size_bytes();

        // We need to guarantee that this will get to the end even if the
        // parent future is dropped. Mirror of `emplace_file`'s pattern;
        // see https://github.com/TraceMachina/nativelink/issues/495.
        background_spawn!("filesystem_store_finalize_holding_insert", async move {
            // #256 duplicate-commit guard, race-window second check:
            // even though the pre-rename guard above runs SYNCHRONOUSLY
            // before the rename, two callers can race past it (both see
            // `None` from `size_for_key` simultaneously, both proceed
            // to rename, both proceed to spawn this task). Re-check
            // INSIDE the spawned task: if the key is already in the
            // map, another caller's spawn-task got here first and
            // inserted; skip our insert to avoid the insert+unref-the-
            // old-Arc trap. The chunked invariant (digest = content)
            // means whichever Arc wins the race indexes a canonical
            // file with byte-identical content — readers see the same
            // bytes either way.
            if evicting_map.size_for_key(&key_for_task).await.is_some() {
                return;
            }

            // Construct a FileEntry pointing at the already-on-disk
            // canonical CAS file. `data_size = digest.size_bytes()` is
            // load-bearing: `commit_chunked` enforces that the .holding
            // file's actual length matches `expected_size = digest.
            // size_bytes()` (chunked_filesystem.rs length-mismatch path),
            // so by the time this code runs, on-disk length == digest
            // size. `block_size` is mirrored from the legacy
            // `add_files_to_cache` emplace at filesystem_store.rs:550 so
            // page-rounded LRU accounting matches the startup-walk path.
            let entry = Fe::create(
                data_size,
                block_size,
                RwLock::new(EncodedFilePath {
                    shared_context,
                    path_type: PathType::Content,
                    key: key_for_task.borrow().into_owned(),
                }),
            );
            evicting_map
                .insert(key_for_task.into_owned().into(), Arc::new(entry))
                .await;
        })
        .await
        .map_err(|e| {
            make_err!(
                Code::Internal,
                "background_spawn join error in finalize_holding: {e:?}"
            )
        })?;
        Ok(())
    }

    /// B1 fixup: best-effort unlink of the holding file. Used by the
    /// Phase 2.3 `commit_and_verify` driver code on end-to-end SHA-256
    /// mismatch. Idempotent.
    pub async fn unlink_holding(&self, digest: &DigestInfo) -> Result<(), Error> {
        let holding_path = chunked_holding_path(&self.shared_context.content_path, digest);
        chunked_unlink_holding(holding_path).await
    }

    /// B1 fixup: read-only accessor for the canonical CAS path of a
    /// digest. The Phase 2.3 driver uses this for opening the
    /// `.holding` file during end-to-end SHA-256 verify. Returns the
    /// path under `content_path/d/XX/<digest>.holding` so the driver
    /// can open it for hashing without recomputing the layout.
    pub fn holding_content_path(&self, digest: &DigestInfo) -> std::path::PathBuf {
        chunked_holding_path(&self.shared_context.content_path, digest)
    }

    /// Discard an in-flight chunked partial: removes the temp file +
    /// the in-flight state entry. Idempotent — calling on a digest
    /// that has no in-flight state returns `Ok(())` (the on-disk file
    /// is unlinked best-effort). Callers MUST distinguish
    /// "already-committed" (commit removed the state, subsequent
    /// commit returns `NotFound`) from "discard-after-discard"
    /// (returns `Ok`).
    ///
    /// Used by:
    /// - The Phase 2.3 driver after a length-mismatch `commit_chunked`
    ///   failure.
    /// - The §6.7 termination triggers (panic, shutdown, retry
    ///   exhaustion).
    pub async fn discard_chunked(&self, digest: &DigestInfo) -> Result<(), Error> {
        chunked_discard(&self.chunked_partials, digest).await
    }

    /// Cheap in-process index probe: returns `Some(size)` if the digest
    /// is already present in `evicting_map` (a canonical CAS file is on
    /// disk and indexed), `None` otherwise. Mirrors the visibility
    /// surface used by `finalize_holding`'s pre-rename guard
    /// (filesystem_store.rs:1698).
    ///
    /// Distinct from `StoreDriver::has_with_results` in that it (a)
    /// does NOT auto-create zero-length files, (b) does NOT trip the
    /// `Str`-key vs `Digest`-key size-fixup pass, and (c) takes a
    /// `&DigestInfo` directly so callers in the chunked path don't need
    /// `StoreLike` in scope. Intended for chunked-write entry points
    /// that want to short-circuit the per-chunk pwrite cost when an
    /// identical-digest blob is already canonical (CAS immutability:
    /// digest = content).
    pub async fn has_indexed_digest(&self, digest: &DigestInfo) -> Option<u64> {
        let key: StoreKey<'static> = (*digest).into();
        self.evicting_map.size_for_key(&key).await
    }
}

#[async_trait]
impl<Fe: FileEntry> StoreDriver for FilesystemStore<Fe> {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // into_owned() is required because the EvictingMap is keyed by
        // StoreKey<'static> (via StoreKeyBorrow) and the input keys have a
        // non-'static lifetime. For Digest keys (the common CAS path) this
        // is Copy and zero-cost; only Str keys allocate.
        let own_keys = keys
            .iter()
            .map(|sk| sk.borrow().into_owned())
            .collect::<Vec<_>>();
        self.evicting_map
            .sizes_for_keys(own_keys.iter(), results, false /* peek */)
            .await;
        // `sizes_for_keys` returns `LenEntry::len()`, which for
        // `FileEntryImpl` is `size_on_disk()` =
        // `data_size.div_ceil(block_size) * block_size` (page-rounded
        // for EvictingMap LRU accounting). The trait contract for
        // `has_with_results` requires the actual blob byte length;
        // returning the page-rounded value causes upstream callers
        // (e.g. `FastSlowStore::run_producer`) to construct
        // `UploadSizeInfo::ExactSize(rounded)` then stream only the
        // actual `data_size` bytes — which trips MemoryStore's ExactSize
        // enforcement and rejects every populate of a non-page-aligned
        // blob. For `Digest` keys the actual size is encoded in the
        // digest itself; for `Str` keys we have no separate logical
        // size to substitute, so we leave the LenEntry value unchanged.
        for (key, result) in keys.iter().zip(results.iter_mut()) {
            if result.is_some() {
                if let StoreKey::Digest(digest) = key.borrow() {
                    *result = Some(digest.size_bytes());
                }
            }
        }
        // We need to do a special pass to ensure our zero files exist.
        // If our results failed and the result was a zero file, we need to
        // create the file by spec.
        for (key, result) in keys.iter().zip(results.iter_mut()) {
            if result.is_some() || !is_zero_digest(key.borrow()) {
                continue;
            }
            let (mut tx, rx) = make_buf_channel_pair();
            let send_eof_result = tx.send_eof();
            self.update(key.borrow(), rx, UploadSizeInfo::ExactSize(0))
                .await
                .err_tip(|| format!("Failed to create zero file for key {}", key.as_str()))
                .merge(
                    send_eof_result
                        .err_tip(|| "Failed to send zero file EOF in filesystem store has"),
                )?;

            *result = Some(0);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            // don't need to add, because zero length files are just assumed to exist
            return Ok(());
        }

        // CAS dedup: skip write if blob already exists (same digest = same content).
        // sizes_for_keys with peek=false promotes the key in the LRU, updating
        // its access time so it won't be evicted prematurely.
        if self.content_is_immutable {
            let owned_key = key.borrow().into_owned();
            let mut exists = [None];
            self.evicting_map
                .sizes_for_keys(core::iter::once(&owned_key), &mut exists, false)
                .await;
            if exists[0].is_some() {
                reader
                    .drain()
                    .await
                    .err_tip(|| "Failed to drain reader for existing blob")?;
                return Ok(());
            }
        }

        let temp_key = make_temp_key(&key);
        let update_total_start = std::time::Instant::now();

        // There's a possibility of deadlock here where we take all of the
        // file semaphores with make_and_open_file and the semaphores for
        // whatever is populating reader is exhasted on the threads that
        // have the FileSlots and not on those which can't.  To work around
        // this we don't take the FileSlot until there's something on the
        // reader available to know that the populator is active.
        reader.peek().await?;

        let temp_create_start = std::time::Instant::now();
        let (entry, temp_file, temp_full_path) = Fe::make_and_open_file(
            self.block_size,
            EncodedFilePath {
                shared_context: self.shared_context.clone(),
                path_type: PathType::Temp,
                key: temp_key,
            },
        )
        .await?;
        let temp_create_ms = temp_create_start.elapsed().as_millis();

        let result = self.update_file(entry, temp_file, key.borrow().into_owned(), reader)
            .await
            .err_tip(|| {
                format!(
                    "While processing with temp file {}",
                    temp_full_path.display()
                )
            });

        let total_ms = update_total_start.elapsed().as_millis();
        if total_ms > 100 {
            debug!(
                key = %key.as_str(),
                total_ms,
                temp_create_ms,
                write_and_emplace_ms = total_ms.saturating_sub(temp_create_ms),
                "update slow write (>100ms)"
            );
        }
        result
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        matches!(
            optimization,
            StoreOptimizations::FileUpdates | StoreOptimizations::SubscribesToUpdateOneshot
        )
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            return Ok(());
        }

        // CAS dedup: skip write if blob already exists (same digest = same content).
        if self.content_is_immutable {
            let owned_key = key.borrow().into_owned();
            let mut exists = [None];
            self.evicting_map
                .sizes_for_keys(core::iter::once(&owned_key), &mut exists, false)
                .await;
            if exists[0].is_some() {
                return Ok(());
            }
        }

        let oneshot_total_start = std::time::Instant::now();
        let temp_key = make_temp_key(&key);
        let temp_create_start = std::time::Instant::now();
        let (mut entry, mut temp_file, temp_full_path) = Fe::make_and_open_file(
            self.block_size,
            EncodedFilePath {
                shared_context: self.shared_context.clone(),
                path_type: PathType::Temp,
                key: temp_key,
            },
        )
        .await
        .err_tip(|| "Failed to create temp file in filesystem store update_oneshot")?;
        let temp_create_ms = temp_create_start.elapsed().as_millis();

        // Write directly without channel overhead
        let data_len = data.len() as u64;
        let write_ms;
        if !data.is_empty() {
            let write_start = std::time::Instant::now();
            temp_file = fs::write_all_to_file(temp_file, data)
                .await
                .err_tip(|| {
                    format!(
                        "Failed to write data to {}",
                        temp_full_path.display()
                    )
                })?;
            write_ms = write_start.elapsed().as_millis();
        } else {
            write_ms = 0;
        }

        if self.fadvise_dontneed {
            temp_file.advise_dontneed();
        }

        let _permit = if let Some(sem) = &self.write_semaphore {
            Some(
                sem.acquire()
                    .await
                    .map_err(|_| make_err!(Code::Internal, "Write semaphore closed"))?,
            )
        } else {
            None
        };

        drop(temp_file);

        *entry.data_size_mut() = data_len;
        let emplace_start = std::time::Instant::now();
        let result = self.emplace_file(key.borrow().into_owned(), Arc::new(entry)).await;
        let emplace_ms = emplace_start.elapsed().as_millis();

        let total_ms = oneshot_total_start.elapsed().as_millis();
        if total_ms > 100 {
            warn!(
                key = %key.as_str(),
                total_ms,
                temp_create_ms,
                write_ms,
                emplace_ms,
                data_len,
                "update_oneshot slow write (>100ms)"
            );
        }
        result
    }

    async fn update_with_whole_file(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        path: OsString,
        file: fs::FileSlot,
        upload_size: UploadSizeInfo,
    ) -> Result<Option<fs::FileSlot>, Error> {
        let file_size = match upload_size {
            UploadSizeInfo::ExactSize(size) => size,
            UploadSizeInfo::MaxSize(_) => file
                .as_std()
                .metadata()
                .err_tip(|| format!("While reading metadata for {}", path.display()))?
                .len(),
        };
        if file_size == 0 {
            // don't need to add, because zero length files are just assumed to exist
            return Ok(None);
        }
        let entry = Fe::create(
            file_size,
            self.block_size,
            RwLock::new(EncodedFilePath {
                shared_context: self.shared_context.clone(),
                path_type: PathType::Custom(path),
                key: key.borrow().into_owned(),
            }),
        );
        // We are done with the file, if we hold a reference to the file here, it could
        // result in a deadlock if `emplace_file()` also needs file descriptors.
        trace!(?file, "Dropping file to to update_with_whole_file");
        drop(file);
        self.emplace_file(key.into_owned(), Arc::new(entry))
            .await
            .err_tip(|| "Could not move file into store in upload_file_to_store, maybe dest is on different volume?")?;
        return Ok(None);
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            self.has(key.borrow())
                .await
                .err_tip(|| "Failed to check if zero digest exists in filesystem store")?;
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in filesystem store get_part")?;
            return Ok(());
        }
        let owned_key = key.into_owned();
        let owned_key_for_check = owned_key.borrow().into_owned();
        let entry = self.evicting_map.get(&owned_key).await.ok_or_else(|| {
            make_err!(
                Code::NotFound,
                "{} not found in filesystem store here",
                owned_key.as_str()
            )
        })?;
        let _large_read_permit = if let Some(sem) = &self.large_read_semaphore {
            let digest_size = match owned_key.borrow() {
                StoreKey::Digest(d) => d.size_bytes(),
                _ => 0,
            };
            if digest_size > self.large_read_threshold {
                Some(
                    sem.acquire()
                        .await
                        .map_err(|_| make_err!(Code::Internal, "Large read semaphore closed"))?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let read_limit = length.unwrap_or(u64::MAX);
        let temp_file = entry.read_file_part(offset).or_else(|err| async move {
            // If the file is not found, we need to remove it from the eviction map.
            if err.code == Code::NotFound {
                warn!(
                    ?err,
                    key = ?owned_key,
                    "Stale filesystem cache entry: file not found on disk. \
                     Removed from map; upper store layer will re-fetch from remote."
                );
                self.evicting_map.remove(&owned_key).await;
            }
            Err(err)
        }).await?;

        // Hint to the kernel that we'll read sequentially — enables more
        // aggressive readahead (typically 2-4x the default 128 KiB).
        temp_file.advise_sequential();

        // By default we do NOT call advise_dontneed() after reading — the same
        // blobs are frequently read by multiple workers within seconds of each
        // other and keeping them in page cache avoids redundant disk I/O
        // (measured: 76% of read I/O is re-reads). On RAM-constrained
        // deployments, enable fadvise_dontneed to drop pages after each read.
        let bytes_before_read = writer.get_bytes_written();
        let file_slot = fs::read_file_to_channel(
            temp_file, writer, read_limit, self.read_buffer_size, offset,
        )
        .await
        .err_tip(|| "Failed to read data in filesystem store")?;
        // Disk-corruption guard: if the file is zero-bytes on disk for a
        // non-zero digest (ZFS corruption, partial write recovery, wrong
        // inode after disk swap), `read_file_to_channel` returns Ok with
        // no bytes written. Sending EOF here would emit an Ok+EOF gRPC
        // stream — the same silent-data-loss class previously caught by
        // the workaround in grpc_store.rs (now removed). Detect, evict
        // the corrupt entry from the evicting_map so the upper layer
        // re-fetches from a different source, and return NotFound.
        let bytes_written = writer.get_bytes_written() - bytes_before_read;
        if bytes_written == 0 {
            let expected_size = match owned_key_for_check.borrow() {
                StoreKey::Digest(d) => d.size_bytes(),
                StoreKey::Str(_) => 0,
            };
            // Only flag the case where the caller asked for the full blob
            // from the start. A range read with offset >= file size or
            // length=Some(0) legitimately returns no bytes and we must not
            // treat that as corruption.
            if expected_size > 0 && offset == 0 && length.is_none() {
                warn!(
                    key = ?owned_key_for_check,
                    expected_size,
                    "FilesystemStore: file on disk is empty for non-zero digest \
                     (likely corruption) — removing entry + returning NotFound"
                );
                self.evicting_map.remove(&owned_key_for_check).await;
                return Err(make_err!(
                    Code::NotFound,
                    "FilesystemStore: file for {} was empty on disk \
                     (expected {expected_size} bytes) — entry removed",
                    owned_key_for_check.as_str()
                ));
            }
        }
        if self.fadvise_dontneed {
            file_slot.advise_dontneed();
        }
        writer
            .send_eof()
            .err_tip(|| "Filed to send EOF in filesystem store get_part")?;
        Ok(())
    }

    /// Batch read that bypasses buf_channel overhead. Uses FuturesUnordered
    /// for parallelism but reads each file directly into Bytes without
    /// allocating a channel pair per key. Preserves stale-entry cleanup
    /// (removes from evicting map if file is missing on disk).
    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        let n = keys.len();
        let futs: FuturesUnordered<_> = keys
            .into_iter()
            .enumerate()
            .map(|(idx, key)| {
                let owned_key = key.into_owned();
                async move {
                    if is_zero_digest(owned_key.borrow()) {
                        return (idx, Ok(Bytes::new()));
                    }

                    let entry = match self.evicting_map.get(&owned_key).await {
                        Some(e) => e,
                        None => {
                            return (idx, Err(make_err!(
                                Code::NotFound,
                                "{} not found in filesystem store",
                                owned_key.as_str()
                            )));
                        }
                    };

                    let result = read_file_entry_bytes(entry.as_ref(), length).await;
                    match &result {
                        Ok(_) => {}
                        Err(e) if e.code == Code::NotFound => {
                            // Stale entry: file missing on disk. Remove from
                            // evicting map so the upper layer re-fetches.
                            warn!(
                                key = %owned_key.as_str(),
                                "batch_get: stale cache entry, file not found on disk"
                            );
                            self.evicting_map.remove(&owned_key).await;
                        }
                        Err(_) => {}
                    }
                    (idx, result)
                }
            })
            .collect();

        let mut results: Vec<Result<Bytes, Error>> = (0..n)
            .map(|_| Err(make_err!(Code::Internal, "batch slot not filled")))
            .collect();
        let mut stream = futs;
        while let Some((idx, result)) = stream.next().await {
            results[idx] = result;
        }
        results
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_health(self: Arc<Self>, registry: &mut HealthRegistryBuilder) {
        registry.register_indicator(self);
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        self.evicting_map
            .add_item_callback(ItemCallbackHolder::new(callback));
        Ok(())
    }

    fn pin_digests(&self, digests: &[DigestInfo]) {
        let keys: Vec<StoreKeyBorrow> = digests
            .iter()
            .map(|d| StoreKeyBorrow::from(StoreKey::from(*d)))
            .collect();
        self.evicting_map.pin_keys(&keys);
    }

    fn pin_digests_with_results(&self, digests: &[DigestInfo]) -> Vec<bool> {
        // Per-key pin so we can report individual failures. The batched
        // pin_keys path collapses run_pending_tasks() across the batch
        // and breaks early on cap exhaustion, neither of which gives the
        // per-digest visibility callers need to detect eviction races.
        digests
            .iter()
            .map(|d| {
                let key: StoreKey<'static> = (*d).into();
                self.evicting_map.pin_key(StoreKeyBorrow::from(key))
            })
            .collect()
    }

    /// #334 Fix C: trait-method form of the existing public
    /// [`Self::unpin_digest`] (singular). Routed through the
    /// `pin_delegation` chain so the server-side BIS broadcast loop
    /// can call `cas_store.unpin_digests(&...)` and have it reach
    /// FilesystemStore via the wrapping chain (FastSlowStore declares
    /// `Many(fast, slow)`, so an unpin on the FSS fans out to BOTH
    /// the MemoryStore fast tier AND this FilesystemStore slow tier).
    /// Per-digest delegation to `unpin_key` mirrors `unpin_digest`.
    fn unpin_digests(&self, digests: &[DigestInfo]) {
        for d in digests {
            let key: StoreKey<'static> = (*d).into();
            self.evicting_map.unpin_key(&key);
        }
    }

    /// FilesystemStore is a leaf — its `drain_stable_digests` is wired
    /// from `FastSlowStore::populate_fast_store` via `register_pin_expire_listener`.
    /// FilesystemStore itself does not expose a stable-digest stream;
    /// FastSlowStore owns that contract.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    /// FilesystemStore is a leaf and supports pinning natively via
    /// `MokaEvictingMap::pin_keys()`. The overrides above route directly
    /// to the evicting map.
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    /// FilesystemStore is a leaf for `mark_stable` — the BIS feeder is
    /// owned by `FastSlowStore` (the wrapper that watches FilesystemStore
    /// pin-expire events). FilesystemStore itself does not push into the
    /// BIS chain. (Task #157.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }

    /// FilesystemStore is disk-backed: a sustained latency hiccup on the
    /// underlying filesystem (ZFS txg sync, slow-tier saturation, page
    /// cache eviction storm) lets an unbounded in-flight write buffer
    /// pin one chunk per concurrent stream until OOM. Compositions like
    /// `FastSlowStore` that buffer slow-tier writes MUST carry an
    /// explicit non-zero cap when wrapping a FilesystemStore.
    /// (Path C, cascade-bundle, 2026-05-09.)
    fn requires_in_flight_buffer_cap(&self) -> bool {
        true
    }
}

#[async_trait]
impl<Fe: FileEntry> HealthStatusIndicator for FilesystemStore<Fe> {
    fn get_name(&self) -> &'static str {
        "FilesystemStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
