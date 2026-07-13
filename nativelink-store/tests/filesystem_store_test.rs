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
use core::marker::PhantomData;
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use core::time::Duration;
use std::env;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use async_lock::RwLock;
use bytes::Bytes;
use futures::executor::block_on;
use futures::task::Poll;
use futures::{Future, FutureExt, poll};
use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_store::filesystem_store::{
    DIGEST_FOLDER, EncodedFilePath, FileEntry, FileEntryImpl, FileType, FilesystemStore,
    IndefinitePinOutcome, STR_FOLDER, digest_content_path, key_from_file,
};
use nativelink_util::buf_channel::{make_buf_channel_pair, make_buf_channel_pair_with_size};
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo};
use nativelink_util::{background_spawn, spawn};
use opentelemetry::context::{Context, FutureExt as OtelFutureExt};
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};
use tokio::sync::{Barrier, Semaphore};
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReadDirStream;
use tracing::{Instrument, debug};

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

trait FileEntryHooks {
    fn on_make_and_open(
        _encoded_file_path: &EncodedFilePath,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        core::future::ready(Ok(()))
    }
    fn on_unref<Fe: FileEntry>(_entry: &Fe) {}
    fn on_drop<Fe: FileEntry>(_entry: &Fe) {}
}

struct TestFileEntry<Hooks: FileEntryHooks + 'static + Sync + Send> {
    inner: Option<FileEntryImpl>,
    _phantom: PhantomData<Hooks>,
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> Debug for TestFileEntry<Hooks> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), core::fmt::Error> {
        f.debug_struct("TestFileEntry")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> FileEntry for TestFileEntry<Hooks> {
    fn create(data_size: u64, block_size: u64, encoded_file_path: RwLock<EncodedFilePath>) -> Self {
        Self {
            inner: Some(FileEntryImpl::create(
                data_size,
                block_size,
                encoded_file_path,
            )),
            _phantom: PhantomData,
        }
    }

    async fn make_and_open_file(
        block_size: u64,
        encoded_file_path: EncodedFilePath,
    ) -> Result<(Self, fs::FileSlot, OsString), Error> {
        Hooks::on_make_and_open(&encoded_file_path).await?;
        let (inner, file_slot, path) =
            FileEntryImpl::make_and_open_file(block_size, encoded_file_path).await?;
        Ok((
            Self {
                inner: Some(inner),
                _phantom: PhantomData,
            },
            file_slot,
            path,
        ))
    }

    fn data_size_mut(&mut self) -> &mut u64 {
        self.inner.as_mut().unwrap().data_size_mut()
    }

    fn size_on_disk(&self) -> u64 {
        self.inner.as_ref().unwrap().size_on_disk()
    }

    fn get_encoded_file_path(&self) -> &RwLock<EncodedFilePath> {
        self.inner.as_ref().unwrap().get_encoded_file_path()
    }

    async fn read_file_part(&self, offset: u64) -> Result<fs::FileSlot, Error> {
        self.inner
            .as_ref()
            .unwrap()
            .read_file_part(offset)
            .await
    }

    async fn get_file_path_locked<
        T,
        Fut: Future<Output = Result<T, Error>> + Send,
        F: FnOnce(OsString) -> Fut + Send,
    >(
        &self,
        handler: F,
    ) -> Result<T, Error> {
        self.inner
            .as_ref()
            .unwrap()
            .get_file_path_locked(handler)
            .await
    }
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> LenEntry for TestFileEntry<Hooks> {
    fn len(&self) -> u64 {
        self.inner.as_ref().unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.inner.as_ref().unwrap().is_empty()
    }

    async fn unref(&self) {
        Hooks::on_unref(self);
        self.inner.as_ref().unwrap().unref().await;
    }
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> Drop for TestFileEntry<Hooks> {
    fn drop(&mut self) {
        eprintln!("TestFileEntry::drop called");
        let mut inner = self.inner.take().unwrap();
        let shared_context = inner.get_shared_context_for_test();
        let current_context = Context::current();

        // We do this complicated bit here because tokio does not give a way to run a
        // command that will wait for all tasks and sub spawns to complete.
        // Sadly we need to rely on `active_drop_spawns` to hit zero to ensure that
        // all tasks have completed.
        let fut = async move {
            // Drop the FileEntryImpl in a controlled setting then wait for the
            // `active_drop_spawns` to hit zero.
            drop(inner);
            while shared_context.active_drop_spawns.load(Ordering::Acquire) > 0 {
                tokio::task::yield_now().await;
            }
        }
        .instrument(tracing::error_span!("test_file_entry_drop"))
        .with_context(current_context);

        #[expect(clippy::disallowed_methods, reason = "testing implementation")]
        let thread_handle = {
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(fut);
            })
        };
        thread_handle.join().unwrap();
        // At this point we can guarantee our file drop spawn has completed.
        Hooks::on_drop(self);
    }
}

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data
    )
}

async fn read_file_contents(file_name: &OsStr) -> Result<Vec<u8>, Error> {
    fs::read(Path::new(file_name)).await
}

async fn wait_for_no_open_files() -> Result<(), Error> {
    let mut counter = 0;
    while fs::get_open_files_for_test() != 0 {
        sleep(Duration::from_millis(1)).await;
        counter += 1;
        if counter > 1000 {
            return Err(make_err!(
                Code::Internal,
                "Timed out waiting all files to close"
            ));
        }
    }
    Ok(())
}

/// Helper function to ensure there are no temporary or content files left.
/// Shard subdirectories (00-ff) under d/ are expected and ignored.
async fn check_storage_dir_empty(storage_path: &str) -> Result<(), Error> {
    // Check digest shard subdirectories for stray files.
    let digest_dir = format!("{storage_path}/{DIGEST_FOLDER}");
    let (_permit, dir_handle) = fs::read_dir(&digest_dir)
        .await
        .err_tip(|| "Failed opening digest directory")?
        .into_inner();

    let mut read_dir_stream = ReadDirStream::new(dir_handle);
    while let Some(entry) = read_dir_stream.next().await {
        let entry = entry?;
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            panic!(
                "No files should exist directly in digest directory, found: {}",
                entry.path().display()
            );
        }
        // For shard subdirectories, check they are empty of files.
        if metadata.is_dir() {
            let shard_path = entry.path();
            let (_permit2, shard_handle) = fs::read_dir(shard_path.to_str().unwrap())
                .await
                .err_tip(|| "Failed opening shard directory")?
                .into_inner();
            let mut shard_stream = ReadDirStream::new(shard_handle);
            if let Some(shard_entry) = shard_stream.next().await {
                let path = shard_entry?.path();
                panic!(
                    "No files should exist in shard directory, found: {}",
                    path.display()
                );
            }
        }
    }

    let (_permit, temp_dir_handle) = fs::read_dir(format!("{storage_path}/{STR_FOLDER}"))
        .await
        .err_tip(|| "Failed opening str directory")?
        .into_inner();

    let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);

    if let Some(temp_dir_entry) = read_dir_stream.next().await {
        let path = temp_dir_entry?.path();
        panic!(
            "No files should exist in str directory, found: {}",
            path.display()
        );
    }
    Ok(())
}

/// Collects all files (not directories) under a sharded digest directory.
/// Scans both flat files in `{base_dir}` and files in shard subdirs `{base_dir}/XX/`.
async fn collect_digest_dir_files(base_dir: &str) -> Result<Vec<std::path::PathBuf>, Error> {
    let (_permit, dir_handle) = fs::read_dir(base_dir)
        .await
        .err_tip(|| format!("Failed opening directory {base_dir}"))?
        .into_inner();

    let mut files = Vec::new();
    let mut read_dir_stream = ReadDirStream::new(dir_handle);
    while let Some(entry) = read_dir_stream.next().await {
        let entry = entry?;
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            files.push(entry.path());
        } else if metadata.is_dir() {
            let sub_path = entry.path();
            let (_permit2, sub_handle) = fs::read_dir(sub_path.to_str().unwrap())
                .await
                .err_tip(|| "Failed opening shard subdirectory")?
                .into_inner();
            let mut sub_stream = ReadDirStream::new(sub_handle);
            while let Some(sub_entry) = sub_stream.next().await {
                let sub_entry = sub_entry?;
                if sub_entry.metadata().await?.is_file() {
                    files.push(sub_entry.path());
                }
            }
        }
    }
    Ok(files)
}

const HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALUE1: &str = "0123456789";
const VALUE2: &str = "9876543210";
const STRING_NAME: &str = "String_Filename";

#[nativelink_test]
async fn valid_results_after_shutdown_test() -> Result<(), Error> {
    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");
    {
        let store = Store::new(
            FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: None,
                block_size: 1,
                ..Default::default()
            })
            .await?,
        );
        // Insert dummy value into store.
        store.update_oneshot(digest, VALUE1.into()).await?;

        assert_eq!(
            store.has(digest).await,
            Ok(Some(VALUE1.len() as u64)),
            "Expected filesystem store to have hash: {}",
            HASH1
        );
    }
    {
        // With a new store ensure content is still readable (ie: restores from shutdown).
        let store = Box::pin(
            FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
                content_path,
                temp_path,
                eviction_policy: None,
                ..Default::default()
            })
            .await?,
        );

        let key = StoreKey::Digest(digest);

        let content = store.get_part_unchunked(key, 0, None).await?;
        assert_eq!(content, VALUE1.as_bytes());
    }

    Ok(())
}

#[nativelink_test]
async fn temp_files_get_deleted_on_replace_test() -> Result<(), Error> {
    static DELETES_FINISHED: AtomicU32 = AtomicU32::new(0);
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            DELETES_FINISHED.fetch_add(1, Ordering::Relaxed);
        }
    }

    let digest1 = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Box::pin(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: Some(EvictionPolicy {
                max_count: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?,
    );

    store.update_oneshot(digest1, VALUE1.into()).await?;

    let expected_file_name = digest_content_path(&content_path, &digest1);
    {
        // Check to ensure our file exists where it should and content matches.
        let data = read_file_contents(&expected_file_name).await?;
        assert_eq!(
            &data[..],
            VALUE1.as_bytes(),
            "Expected file content to match"
        );
    }

    // Replace content.
    store.update_oneshot(digest1, VALUE2.into()).await?;

    {
        // Check to ensure our file now has new content.
        let data = read_file_contents(&expected_file_name).await?;
        assert_eq!(
            &data[..],
            VALUE2.as_bytes(),
            "Expected file content to match"
        );
    }

    loop {
        if DELETES_FINISHED.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert!(logs_contain(
        "Spawned a filesystem_delete_file current_active_drop_spawns=1"
    ));
    assert!(logs_contain(
        "Dropped a filesystem_delete_file current_active_drop_spawns=0"
    ));

    check_storage_dir_empty(&temp_path).await
}

// This test ensures that if a file is overridden and an open stream to the file already
// exists, the open stream will continue to work properly and when the stream is done the
// temporary file (of the object that was deleted) is cleaned up.
#[nativelink_test]
async fn file_continues_to_stream_on_content_replace_test() -> Result<(), Error> {
    static DELETES_FINISHED: AtomicU32 = AtomicU32::new(0);
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            DELETES_FINISHED.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Use a large value so the producer is still blocked mid-stream when we
    // check the temp directory. With read_buffer_size=1 and channel
    // capacity 64 (set explicitly below), the producer sends 1-byte chunks
    // and blocks once 64+8 bytes have flowed. The 100-byte payload exceeds
    // that, ensuring the producer is mid-stream and still holds an Arc to
    // the FileEntry when the test inspects temp_path.
    let large_value1: String = "abcdefghij".repeat(10); // 100 bytes
    let large_value2: String = "ABCDEFGHIJ".repeat(10); // 100 bytes
    let digest1 = DigestInfo::try_new(HASH1, large_value1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: Some(EvictionPolicy {
                max_count: 3,
                ..Default::default()
            }),
            block_size: 1,
            read_buffer_size: 1,
            ..Default::default()
        })
        .await?,
    );

    // Insert data into store.
    store
        .update_oneshot(digest1, large_value1.clone().into())
        .await?;

    // Explicit small channel so the producer backpressures before draining
    // the file. Default capacity (1024) is large enough to hold the entire
    // 100-byte payload, which would let the producer task complete and
    // drop its Arc before the test can inspect temp_path.
    let (writer, mut reader) = make_buf_channel_pair_with_size(64);
    let store_clone = store.clone();
    let digest1_clone = digest1;
    background_spawn!(
        "file_continues_to_stream_on_content_replace_test_store_get",
        async move { store_clone.get(digest1_clone, writer).await.unwrap() },
    );

    {
        // Check to ensure our first byte has been received. The future should be stalled here.
        let first_byte = reader
            .consume(Some(1))
            .await
            .err_tip(|| "Error reading first byte")?;
        assert_eq!(
            first_byte[0],
            large_value1.as_bytes()[0],
            "Expected first byte to match"
        );
    }

    // Replace content.
    store
        .update_oneshot(digest1, large_value2.into())
        .await?;

    // Ensure we let any background tasks finish.
    tokio::task::yield_now().await;

    {
        // Now ensure we only have 1 file in our temp path - we know it is a digest.
        let temp_files = collect_digest_dir_files(&format!("{temp_path}/{DIGEST_FOLDER}")).await?;
        assert_eq!(
            temp_files.len(), 1,
            "There should only be one file in the temp directory"
        );
        let data = read_file_contents(temp_files[0].as_os_str()).await?;
        assert_eq!(
            &data[..],
            large_value1.as_bytes(),
            "Expected file content to match"
        );
    }

    let remaining_file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading remaining bytes")?;

    assert_eq!(
        &remaining_file_data,
        &large_value1.as_bytes()[1..],
        "Expected file content to match"
    );

    loop {
        if DELETES_FINISHED.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Now ensure our temp file was cleaned up.
    check_storage_dir_empty(&temp_path).await
}

// Eviction has a different code path than a file replacement, so we check that if a
// file is evicted and has an open stream on it, it will stay alive and eventually
// get deleted.
#[nativelink_test]
async fn file_gets_cleans_up_on_cache_eviction() -> Result<(), Error> {
    static DELETES_FINISHED: AtomicU32 = AtomicU32::new(0);
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            DELETES_FINISHED.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Use a large value so the producer is still blocked mid-stream when we
    // check the temp directory. With read_buffer_size=1 and channel capacity 64,
    // the producer sends 1-byte chunks. It needs well over 64 bytes to ensure
    // it can't finish before the test inspects temp_path. With a small value
    // (e.g. 10 bytes), all chunks fit in the channel buffer, the get task
    // completes immediately, and the background delete can race ahead of the
    // temp directory inspection.
    let large_value1: String = "abcdefghij".repeat(10); // 100 bytes
    let large_value2: String = "ABCDEFGHIJ".repeat(10); // 100 bytes
    let digest1 = DigestInfo::try_new(HASH1, large_value1.len())?;
    let digest2 = DigestInfo::try_new(HASH2, large_value2.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1,
                ..Default::default()
            }),
            block_size: 1,
            read_buffer_size: 1,
            ..Default::default()
        })
        .await?,
    );

    // Insert data into store.
    store
        .update_oneshot(digest1, large_value1.clone().into())
        .await
        .unwrap();

    let (writer, mut reader) = make_buf_channel_pair();
    let store_clone = store.clone();
    background_spawn!(
        "file_gets_cleans_up_on_cache_eviction_store_get",
        async move { store_clone.get(digest1, writer).await.unwrap() },
    );

    {
        // Check to ensure our first byte has been received. The future should be stalled
        // here because the large value exceeds the channel capacity with read_buffer_size=1.
        let first_byte = reader
            .consume(Some(1))
            .await
            .err_tip(|| "Error reading first byte")?;
        assert_eq!(
            first_byte[0],
            large_value1.as_bytes()[0],
            "Expected first byte to match"
        );
    }

    // Insert new content. This will evict the old item.
    store
        .update_oneshot(digest2, large_value2.into())
        .await?;

    // Ensure we let any background tasks finish.
    tokio::task::yield_now().await;

    {
        // Now ensure we only have 1 file in our temp path - we know it is a digest.
        let temp_files = collect_digest_dir_files(&format!("{temp_path}/{DIGEST_FOLDER}")).await?;
        assert_eq!(
            temp_files.len(), 1,
            "There should only be one file in the temp directory"
        );
        let data = read_file_contents(temp_files[0].as_os_str()).await?;
        assert_eq!(
            &data[..],
            large_value1.as_bytes(),
            "Expected file content to match"
        );
    }

    let remaining_file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading remaining bytes")?;

    assert_eq!(
        &remaining_file_data,
        &large_value1.as_bytes()[1..],
        "Expected file content to match"
    );

    loop {
        if DELETES_FINISHED.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Now ensure our temp file was cleaned up.
    check_storage_dir_empty(&temp_path).await
}

// Test to ensure that if we are holding a reference to `FileEntry` and the contents are
// replaced, the `FileEntry` continues to use the old data.
// `FileEntry` file contents should be immutable for the lifetime of the object.
#[nativelink_test]
async fn digest_contents_replaced_continues_using_old_data() -> Result<(), Error> {
    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: make_temp_path("content_path"),
            temp_path: make_temp_path("temp_path"),
            eviction_policy: None,
            ..Default::default()
        })
        .await?,
    );
    // Insert data into store.
    store.update_oneshot(digest, VALUE1.into()).await?;
    let file_entry = store.get_file_entry_for_digest(&digest).await?;
    {
        // The file contents should equal our initial data.
        let mut reader = file_entry.read_file_part(0).await?;
        let mut file_contents = String::new();
        std::io::Read::read_to_string(reader.as_std_mut(), &mut file_contents)?;
        assert_eq!(file_contents, VALUE1);
    }

    // Now replace the data.
    store.update_oneshot(digest, VALUE2.into()).await?;

    {
        // The file contents still equal our old data.
        let mut reader = file_entry.read_file_part(0).await?;
        let mut file_contents = String::new();
        std::io::Read::read_to_string(reader.as_std_mut(), &mut file_contents)?;
        assert_eq!(file_contents, VALUE1);
    }

    Ok(())
}

#[nativelink_test]
async fn eviction_on_insert_calls_unref_once() -> Result<(), Error> {
    const SMALL_VALUE: &str = "01";
    const BIG_VALUE: &str = "0123";

    static UNREFED_DIGESTS: LazyLock<Mutex<Vec<StoreKey<'static>>>> =
        LazyLock::new(|| Mutex::new(Vec::new()));
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_unref<Fe: FileEntry>(file_entry: &Fe) {
            block_on(file_entry.get_file_path_locked(move |path_str| async move {
                let path = Path::new(&path_str);
                let digest = key_from_file(
                    path.file_name().unwrap().to_str().unwrap(),
                    FileType::Digest,
                )
                .unwrap();
                UNREFED_DIGESTS.lock().push(digest.borrow().into_owned());
                Ok(())
            }))
            .unwrap();
        }
    }

    let small_digest = StoreKey::Digest(DigestInfo::try_new(HASH1, SMALL_VALUE.len())?);
    let big_digest = DigestInfo::try_new(HASH1, BIG_VALUE.len())?;

    let store = Box::pin(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(&FilesystemSpec {
            content_path: make_temp_path("content_path"),
            temp_path: make_temp_path("temp_path"),
            eviction_policy: Some(EvictionPolicy {
                max_bytes: 5,
                ..Default::default()
            }),
            block_size: 1,
            ..Default::default()
        })
        .await?,
    );
    // Insert data into store.
    store
        .update_oneshot(small_digest.borrow(), SMALL_VALUE.into())
        .await?;
    store.update_oneshot(big_digest, BIG_VALUE.into()).await?;

    {
        // Our first digest should have been unrefed exactly once.
        let unrefed_digests = UNREFED_DIGESTS.lock();
        assert_eq!(
            unrefed_digests.len(),
            1,
            "Expected exactly 1 unrefed digest"
        );
        assert_eq!(unrefed_digests[0], small_digest, "Expected digest to match");
    }

    Ok(())
}

#[nativelink_test]
async fn rename_on_insert_fails_due_to_filesystem_error_proper_cleanup_happens() -> Result<(), Error>
{
    const INITIAL_CONTENT: &str = "hello";

    async fn wait_for_temp_file<Fut: Future<Output = Result<(), Error>>, F: Fn() -> Fut>(
        temp_path: &str,
        yield_fn: F,
    ) -> Result<fs::DirEntry, Error> {
        loop {
            yield_fn().await?;
            // Scan all shard subdirectories for exactly one temp file.
            let temp_files =
                collect_digest_dir_files(&format!("{temp_path}/{DIGEST_FOLDER}")).await?;
            if temp_files.len() == 1 {
                let path = &temp_files[0];
                {
                    // Some filesystems won't sync automatically, so force it.
                    let file_handle = fs::open_file(path.clone().into_os_string(), 0)
                        .await
                        .err_tip(|| "Failed to open temp file")?;
                    // We don't care if it fails, this is only best attempt.
                    drop(file_handle.as_std().sync_all());
                }
                let metadata = tokio::fs::metadata(path).await?;
                if metadata.len() >= INITIAL_CONTENT.len() as u64 {
                    // Re-read the directory entry to return the proper type.
                    let parent = path.parent().unwrap();
                    let file_name = path.file_name().unwrap();
                    let (_permit, dir_handle) =
                        fs::read_dir(parent.to_str().unwrap()).await?.into_inner();
                    let mut stream = ReadDirStream::new(dir_handle);
                    while let Some(entry) = stream.next().await {
                        let entry = entry?;
                        if entry.file_name() == file_name {
                            return Ok(entry);
                        }
                    }
                }
            }
            assert!(
                temp_files.len() <= 1,
                "There should only be one file in temp directory, found: {}",
                temp_files.len()
            );
        }
        // Unreachable.
    }

    static FILE_DELETED_BARRIER: LazyLock<Arc<Barrier>> =
        LazyLock::new(|| Arc::new(Barrier::new(2)));

    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            background_spawn!(
                "rename_on_insert_fails_due_to_filesystem_error_proper_cleanup_happens_local_hooks_on_drop",
                FILE_DELETED_BARRIER.wait()
            );
        }
    }

    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Box::pin(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: None,
            ..Default::default()
        })
        .await?,
    );

    let (mut tx, rx) = make_buf_channel_pair();
    let update_fut = Arc::new(async_lock::Mutex::new(store.update(
        digest,
        rx,
        UploadSizeInfo::MaxSize(100),
    )));
    // This will process as much of the future as it can before it needs to pause.
    // Our temp file will be created and opened and ready to have contents streamed
    // to it.
    assert_eq!(poll!(&mut *update_fut.lock().await)?, Poll::Pending);
    tx.send(INITIAL_CONTENT.into()).await?;

    // Now we extract that temp file that is generated.
    wait_for_temp_file(&temp_path, || {
        let update_fut_clone = update_fut.clone();
        async move {
            // This will ensure we yield to our future and other potential spawns.
            tokio::task::yield_now().await;
            assert_eq!(poll!(&mut *update_fut_clone.lock().await)?, Poll::Pending);
            Ok(())
        }
    })
    .await?;

    // Now make it impossible for the file to be moved into the final path.
    // This will trigger an error on `rename()`.
    fs::remove_dir_all(&content_path).await?;

    // Because send_eof() waits for shutdown of the rx side, we cannot just await in this thread.
    background_spawn!(
        "rename_on_insert_fails_due_to_filesystem_error_proper_cleanup_happens_send_eof",
        async move {
            tx.send_eof().unwrap();
        },
    );

    // Now finish waiting on update(). This should result in an error because we deleted our dest
    // folder.
    let update_result = &mut *update_fut.lock().await;
    assert!(
        update_result.await.is_err(),
        "Expected update to fail due to temp file being deleted before rename"
    );

    // Delete may happen on another thread, so wait for it.
    FILE_DELETED_BARRIER.wait().await;

    // Now it should have cleaned up its temp files.
    {
        check_storage_dir_empty(&temp_path).await?;
    }

    // Finally ensure that our entry is not in the store.
    assert_eq!(
        store.has(digest).await?,
        None,
        "Entry should not be in store"
    );
    Ok(())
}

#[nativelink_test]
async fn get_part_timeout_test() -> Result<(), Error> {
    let large_value = "x".repeat(1024);
    let digest = DigestInfo::try_new(HASH1, large_value.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    store
        .update_oneshot(digest, large_value.clone().into())
        .await?;

    let (writer, mut reader) = make_buf_channel_pair();
    let store_clone = store.clone();
    let digest_clone = digest;

    let _drop_guard = spawn!("get_part_timeout_test_get", async move {
        store_clone.get(digest_clone, writer).await
    });

    let file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading bytes")?;

    assert_eq!(
        &file_data,
        large_value.as_bytes(),
        "Expected file content to match"
    );

    Ok(())
}

#[nativelink_test]
async fn get_part_is_zero_digest() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let store_clone = store.clone();
    let (mut writer, mut reader) = make_buf_channel_pair();

    let _drop_guard = spawn!("get_part_is_zero_digest_get_part", async move {
        drop(
            store_clone
                .get_part(digest, &mut writer, 0, None)
                .await
                .err_tip(|| "Failed to get_part"),
        );
    });

    let file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading bytes")?;

    let empty_bytes = Bytes::new();
    assert_eq!(&file_data, &empty_bytes, "Expected file content to match");

    Ok(())
}

#[nativelink_test]
async fn has_with_results_on_zero_digests() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let keys = vec![digest.into()];
    let mut results = vec![None];
    drop(
        store
            .has_with_results(&keys, &mut results)
            .await
            .err_tip(|| "Failed to get_part"),
    );
    assert_eq!(results, vec![Some(0)]);

    check_storage_dir_empty(&content_path).await?;

    Ok(())
}

async fn wrap_update_zero_digest<F>(updater: F) -> Result<(), Error>
where
    F: AsyncFnOnce(DigestInfo, Arc<FilesystemStore>) -> Result<(), Error>,
{
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
        &FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            read_buffer_size: 1,
            ..Default::default()
        },
        |from, to| std::fs::rename(from, to),
    )
    .await?;
    updater(digest, store).await?;
    check_storage_dir_empty(&content_path).await?;
    check_storage_dir_empty(&temp_path).await?;
    Ok(())
}

#[nativelink_test]
async fn update_whole_file_with_zero_digest() -> Result<(), Error> {
    wrap_update_zero_digest(async |digest, store| {
        let temp_file_dir = make_temp_path("update_with_zero_digest");
        std::fs::create_dir_all(&temp_file_dir)?;
        let temp_file_path = Path::new(&temp_file_dir).join("zero-length-file");
        std::fs::write(&temp_file_path, b"")
            .err_tip(|| format!("Writing to {temp_file_path:?}"))?;
        let file_slot = fs::open_file(&temp_file_path, 0).await?;
        store
            .update_with_whole_file(
                digest,
                temp_file_path.into(),
                file_slot,
                UploadSizeInfo::ExactSize(0),
            )
            .await?;
        Ok(())
    })
    .await
}

#[nativelink_test]
async fn update_oneshot_with_zero_digest() -> Result<(), Error> {
    wrap_update_zero_digest(async |digest, store| store.update_oneshot(digest, Bytes::new()).await)
        .await
}

#[nativelink_test]
async fn update_with_zero_digest() -> Result<(), Error> {
    wrap_update_zero_digest(async |digest, store| {
        let (_writer, reader) = make_buf_channel_pair();
        store
            .update(digest, reader, UploadSizeInfo::ExactSize(0))
            .await
            .map(|_| ())
    })
    .await
}

#[nativelink_test]
async fn get_file_entry_for_zero_digest() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
        &FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            read_buffer_size: 1,
            ..Default::default()
        },
        |from, to| std::fs::rename(from, to),
    )
    .await?;

    // #2346: a zero-digest has no backing FileEntry, so the singular accessor
    // returns NotFound (matching `get_file_entries_batch`, which returns None)
    // instead of a synthetic entry pointing at a nonexistent path. Every caller
    // special-cases zero digests before calling.
    let err = store
        .get_file_entry_for_digest(&digest)
        .await
        .expect_err("zero-digest must not return a synthetic file entry");
    assert_eq!(
        err.code,
        Code::NotFound,
        "zero-digest file entry lookup must surface NotFound"
    );
    Ok(())
}

/// Regression test for: https://github.com/TraceMachina/nativelink/issues/495.
#[nativelink_test(flavor = "multi_thread")]
async fn update_file_future_drops_before_rename() -> Result<(), Error> {
    // Mutex can be used to signal to the rename function to pause execution.
    static RENAME_REQUEST_PAUSE_MUX: async_lock::Mutex<()> = async_lock::Mutex::new(());
    // Boolean used to know if the rename function is currently paused.
    static RENAME_IS_PAUSED: AtomicBool = AtomicBool::new(false);

    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let content_path = make_temp_path("content_path");
    let store = Arc::pin(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: make_temp_path("temp_path"),
                eviction_policy: None,
                ..Default::default()
            },
            |from, to| {
                // If someone locked our mutex, it means we need to pause, so we
                // simply request a lock on the same mutex.
                if RENAME_REQUEST_PAUSE_MUX.try_lock().is_none() {
                    RENAME_IS_PAUSED.store(true, Ordering::Release);
                    while RENAME_REQUEST_PAUSE_MUX.try_lock().is_none() {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    RENAME_IS_PAUSED.store(false, Ordering::Release);
                }
                std::fs::rename(from, to)
            },
        )
        .await?,
    );

    // Populate our first store entry.
    let first_file_entry = {
        store.update_oneshot(digest, VALUE1.into()).await?;
        store.get_file_entry_for_digest(&digest).await?
    };

    // 1. Request the next rename function to block.
    // 2. Request to replace our data.
    // 3. When we are certain that our rename function is paused, drop
    //    the replace/update future.
    // 4. Then drop the lock.
    {
        let rename_pause_request_lock = RENAME_REQUEST_PAUSE_MUX.lock().await;
        let mut update_fut = store.update_oneshot(digest, VALUE2.into()).boxed();

        loop {
            // Try to advance our update future.
            assert_eq!(poll!(&mut update_fut), Poll::Pending);

            // Once we are sure the rename function is paused break.
            if RENAME_IS_PAUSED.load(Ordering::Acquire) {
                break;
            }
            // Give a little time for background/kernel threads to run.
            sleep(Duration::from_millis(1)).await;
        }
        // Writing these out explicitly so users know this is what we are testing.
        // Note: The order they are dropped matters.
        drop(update_fut);
        drop(rename_pause_request_lock);
    }
    // Grab the newly inserted item in our store.
    let new_file_entry = store.get_file_entry_for_digest(&digest).await?;
    assert!(
        !Arc::ptr_eq(&first_file_entry, &new_file_entry),
        "Expected file entries to not be the same"
    );

    // Ensure the entry we inserted was properly flagged as moved (from temp -> content dir).
    new_file_entry
        .get_file_path_locked(move |file_path| async move {
            assert_eq!(
                file_path,
                digest_content_path(&content_path, &digest)
            );
            Ok(())
        })
        .await?;

    Ok(())
}

#[nativelink_test]
async fn deleted_file_removed_from_store() -> Result<(), Error> {
    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    store.update_oneshot(digest, VALUE1.into()).await?;

    let stored_file_path = digest_content_path(&content_path, &digest);
    std::fs::remove_file(stored_file_path)?;

    let get_part_res = store.get_part_unchunked(digest, 0, None).await;
    assert_eq!(get_part_res.unwrap_err().code, Code::NotFound);

    // Repeat with a string typed key.

    let string_key = StoreKey::new_str(STRING_NAME);

    store
        .update_oneshot(string_key.borrow(), VALUE2.into())
        .await
        .unwrap();

    let stored_file_path = OsString::from(format!("{content_path}/{STR_FOLDER}/{STRING_NAME}"));
    std::fs::remove_file(stored_file_path)?;

    let string_digest_get_part_res = store.get_part_unchunked(string_key, 0, None).await;
    assert_eq!(string_digest_get_part_res.unwrap_err().code, Code::NotFound);

    Ok(())
}

// Ensure that get_file_size() returns the correct number
// ceil(content length / block_size) * block_size
// assume block size 4K
// 1B data size = 4K size on disk
// 5K data size = 8K size on disk
#[nativelink_test]
async fn get_file_size_uses_block_size() -> Result<(), Error> {
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let value_1kb: String = "x".repeat(1024);
    let value_5kb: String = "xabcd".repeat(1024);

    let digest_1kb = DigestInfo::try_new(HASH1, value_1kb.len())?;
    let digest_5kb = DigestInfo::try_new(HASH2, value_5kb.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    store.update_oneshot(digest_1kb, value_1kb.into()).await?;
    let short_entry = store.get_file_entry_for_digest(&digest_1kb).await?;
    assert_eq!(short_entry.size_on_disk(), 4 * 1024);

    store.update_oneshot(digest_5kb, value_5kb.into()).await?;
    let long_entry = store.get_file_entry_for_digest(&digest_5kb).await?;
    assert_eq!(long_entry.size_on_disk(), 8 * 1024);
    Ok(())
}

// Regression: FilesystemStore::has_with_results MUST return the actual
// blob byte length, NOT the page-rounded `size_on_disk()` used internally
// for EvictingMap accounting.
//
// Production bug (2026-04-26): commit `0ff03300` added an
// `UploadSizeInfo::ExactSize` enforcement check in `MemoryStore::update`,
// which exposed a long-latent contract violation in
// `FilesystemStore::has_with_results`: the underlying `LenEntry::len()`
// for `FileEntryImpl` returns `size_on_disk()` =
// `data_size.div_ceil(block_size) * block_size` (page-rounded), and that
// value leaks back through `EvictingMap::sizes_for_keys` as the "size"
// that callers like `FastSlowStore::run_producer` then use to construct
// `UploadSizeInfo::ExactSize(size)` for the populate-fast-store stream.
// The stream only carries the actual `data_size` bytes, so MemoryStore's
// new enforcement rejects every populate of a non-page-aligned blob with
// `MemoryStore::update: ExactSize declared X bytes but received Y` where
// X is page-aligned and Y is the real digest size. Bazel reads fail with
// INVALID_ARGUMENT at ~100/sec until either the rounding is fixed or the
// MemoryStore enforcement is reverted.
//
// The fix belongs at the `FilesystemStore::has_with_results` boundary —
// LenEntry::len()'s page-rounding is intentional for LRU accounting and
// should stay; what must NOT page-round is the value reported as the
// blob's logical size.
#[nativelink_test]
async fn has_with_results_returns_actual_data_size_not_page_rounded() -> Result<(), Error> {
    // Deliberately non-page-aligned: 2653392 bytes is exactly the size
    // observed in production logs for a Bazel blob whose declared size
    // came back rounded to 2654208 (= 648 * 4096). Reproducing the
    // exact size makes the failure message match the production log
    // line for instant pattern recognition.
    const ACTUAL_BLOB_SIZE: usize = 2_653_392;

    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    // Default block_size = 4096 (production setting). The bug only
    // manifests when block_size > 1, which is why the existing test
    // suite — every existing FilesystemStore test uses
    // `block_size: 1` — never caught it.
    let store = Arc::new(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 4 * 1024,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let blob_data = make_random_data(ACTUAL_BLOB_SIZE);
    let digest = DigestInfo::try_new(HASH1, ACTUAL_BLOB_SIZE)?;
    store
        .update_oneshot(digest, Bytes::from(blob_data))
        .await?;

    // Sanity: confirm the LRU-accounting value IS page-rounded (this is
    // the source of the leak, not a regression target).
    let entry = store.get_file_entry_for_digest(&digest).await?;
    let page_rounded = ACTUAL_BLOB_SIZE.next_multiple_of(4096);
    assert_eq!(
        entry.size_on_disk(),
        page_rounded as u64,
        "size_on_disk() must remain page-rounded for LRU accounting; \
         this assertion documents the bug source, not the fix",
    );

    // The actual contract under test: has() must report the ACTUAL
    // blob size so callers can use it as `UploadSizeInfo::ExactSize`
    // without truncating downstream readers.
    let has_size = store
        .has(digest)
        .await?
        .expect("blob just written; has() must return Some");
    assert_eq!(
        has_size, ACTUAL_BLOB_SIZE as u64,
        "has() returned page-rounded size_on_disk ({page_rounded}) \
         instead of actual digest size ({ACTUAL_BLOB_SIZE}); this is \
         the production bug — populate_fast_store_unchecked then \
         streams ExactSize({page_rounded}) into MemoryStore, which \
         rejects the partial write because the file only has \
         {ACTUAL_BLOB_SIZE} bytes",
    );

    // has_with_results must also report actual size — same code path,
    // but assert it explicitly so a future change to has() doesn't
    // accidentally leave has_with_results broken.
    let keys = vec![digest.into()];
    let mut results = vec![None];
    store.has_with_results(&keys, &mut results).await?;
    assert_eq!(
        results,
        vec![Some(ACTUAL_BLOB_SIZE as u64)],
        "has_with_results returned page-rounded size_on_disk; same \
         underlying bug as has() above",
    );

    Ok(())
}

#[nativelink_test]
async fn update_with_whole_file_closes_file() -> Result<(), Error> {
    #[expect(clippy::collection_is_never_read)] // TODO(jhpratt) investigate
    let mut permits = vec![];
    // Grab all permits to ensure only 1 permit is available.
    {
        wait_for_no_open_files().await?;
        while fs::OPEN_FILE_SEMAPHORE.available_permits() > 1 {
            permits.push(fs::get_permit().await);
        }
        assert_eq!(
            fs::OPEN_FILE_SEMAPHORE.available_permits(),
            1,
            "Expected 1 permit to be available"
        );
    }
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let value = "x".repeat(1024);

    let digest = DigestInfo::try_new(HASH1, value.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            read_buffer_size: 1,
            ..Default::default()
        })
        .await?,
    );
    store.update_oneshot(digest, value.clone().into()).await?;

    let file_path = OsString::from(format!("{temp_path}/dummy_file"));
    let mut file = fs::create_file(&file_path).await?;
    {
        use std::io::{Seek, Write};
        file.as_std_mut().write_all(value.as_bytes())
            .err_tip(|| "Could not write to file")?;
        file.as_std().sync_all()
            .err_tip(|| "Could not sync file")?;
        file.as_std_mut().seek(std::io::SeekFrom::Start(0))
            .err_tip(|| "Could not seek file")?;
    }

    store
        .update_with_whole_file(
            digest,
            file_path,
            file,
            UploadSizeInfo::ExactSize(value.len() as u64),
        )
        .await?;
    Ok(())
}

// Ensure that update_with_whole_file() moves the file without making a copy.
#[cfg(target_family = "unix")]
#[nativelink_test]
async fn update_with_whole_file_uses_same_inode() -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let value: String = "x".repeat(1024);

    let digest = DigestInfo::try_new(HASH1, value.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let file_path = OsString::from(format!("{temp_path}/dummy_file"));
    let original_inode = {
        let file = fs::create_file(&file_path).await?;
        let original_inode = file.as_std().metadata()
            .err_tip(|| "Could not get metadata")?.ino();

        let result = store
            .update_with_whole_file(
                digest,
                file_path,
                file,
                UploadSizeInfo::ExactSize(value.len() as u64),
            )
            .await?;
        assert!(
            result.is_none(),
            "Expected filesystem store to consume the file"
        );
        original_inode
    };

    let expected_file_name = digest_content_path(&content_path, &digest);
    let new_inode = tokio::fs::metadata(&expected_file_name).await?.ino();
    assert_eq!(
        original_inode, new_inode,
        "Expected the same inode for the file"
    );

    Ok(())
}

#[nativelink_test]
async fn file_slot_taken_when_ready() -> Result<(), Error> {
    static FILE_SEMAPHORE: Semaphore = Semaphore::const_new(1);
    static WRITER_SEMAPHORE: Semaphore = Semaphore::const_new(1);
    static FILE_PERMIT: Mutex<Option<tokio::sync::SemaphorePermit<'_>>> = Mutex::new(None);
    static WRITER_PERMIT: Mutex<Option<tokio::sync::SemaphorePermit<'_>>> = Mutex::new(None);

    struct SingleSemaphoreHooks;
    impl FileEntryHooks for SingleSemaphoreHooks {
        async fn on_make_and_open(_encoded_file_path: &EncodedFilePath) -> Result<(), Error> {
            *FILE_PERMIT.lock() =
                Some(FILE_SEMAPHORE.acquire().await.map_err(|e| {
                    make_err!(Code::Internal, "Unable to acquire semaphore: {e:?}")
                })?);
            // Drop the writer permit now that we have one.
            WRITER_PERMIT.lock().take();
            Ok(())
        }
    }

    *WRITER_PERMIT.lock() = Some(WRITER_SEMAPHORE.acquire().await.unwrap());
    *FILE_PERMIT.lock() = Some(FILE_SEMAPHORE.acquire().await.unwrap());

    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let value_1: String = "x".repeat(1024);
    let value_2: String = "y".repeat(1024);

    let digest_1 = DigestInfo::try_new(HASH1, value_1.len())?;
    let digest_2 = DigestInfo::try_new(HASH2, value_2.len())?;

    let store = Box::pin(
        FilesystemStore::<TestFileEntry<SingleSemaphoreHooks>>::new_with_timeout_and_rename_fn(
            &FilesystemSpec {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let value_1 = Bytes::from(value_1);
    let value_2 = Bytes::from(value_2);

    let (mut writer_1, reader_1) = make_buf_channel_pair();
    let (mut writer_2, reader_2) = make_buf_channel_pair();
    let size_1 = UploadSizeInfo::ExactSize(value_1.len().try_into()?);
    let size_2 = UploadSizeInfo::ExactSize(value_2.len().try_into()?);
    let store_ref = &store;
    let update_1_fut = async move {
        let result = store_ref.update(digest_1, reader_1, size_1).await;
        FILE_PERMIT.lock().take();
        result
    };
    let update_2_fut = async move {
        let result = store_ref.update(digest_2, reader_2, size_2).await;
        FILE_PERMIT.lock().take();
        result
    };

    let writer_1_fut = async move {
        let _permit = WRITER_SEMAPHORE.acquire().await.unwrap();
        writer_1.send(value_1.slice(0..1)).await?;
        writer_1.send(value_1.slice(1..2)).await?;
        writer_1.send(value_1.slice(2..3)).await?;
        writer_1.send(value_1.slice(3..)).await?;
        writer_1.send_eof()?;
        Ok::<_, Error>(())
    };
    let writer_2_fut = async move {
        writer_2.send(value_2.slice(0..1)).await?;
        writer_2.send(value_2.slice(1..2)).await?;
        writer_2.send(value_2.slice(2..3)).await?;
        // Allow the update to get a file permit.
        FILE_PERMIT.lock().take();
        writer_2.send(value_2.slice(3..)).await?;
        writer_2.send_eof()?;
        Ok::<_, Error>(())
    };

    let (res_1, res_2, res_3, res_4) = tokio::time::timeout(Duration::from_secs(10), async move {
        tokio::join!(update_1_fut, update_2_fut, writer_1_fut, writer_2_fut)
    })
    .await
    .map_err(|_| make_err!(Code::Internal, "Deadlock detected"))?;
    res_1.merge(res_2).merge(res_3).merge(res_4)
}

// If we insert a file larger than the max_bytes eviction policy, it should be safely
// evicted, without deadlocking.
#[nativelink_test]
async fn safe_small_safe_eviction() -> Result<(), Error> {
    let store_spec = FilesystemSpec {
        content_path: "/tmp/nativelink/safe_fs".into(),
        temp_path: "/tmp/nativelink/safe_fs_temp".into(),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let store = Store::new(<FilesystemStore>::new(&store_spec).await?);

    // > than the max_bytes
    let bytes = 2;

    let data = make_random_data(bytes);
    let digest = DigestInfo::try_new(VALID_HASH, data.len()).unwrap();

    assert_eq!(
        store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store"
    );

    store.update_oneshot(digest, data.clone().into()).await?;

    assert_eq!(
        store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store, because eviction"
    );

    let (tx, mut rx) = make_buf_channel_pair();

    assert_eq!(
        store.get(digest, tx).await,
        Err(Error {
            code: Code::NotFound,
            messages: vec![format!(
                "{VALID_HASH}-{bytes} not found in filesystem store here"
            )],
            details: vec![],
        }),
        "Expected data to not exist in store, because eviction"
    );

    assert!(rx.recv().await.is_err());

    Ok(())
}

#[nativelink_test]
async fn add_too_early_files() -> Result<(), Error> {
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let demo_file_folder = format!("{content_path}/s");
    fs::create_dir_all(&demo_file_folder).await?;
    let demo_file_path = format!("{demo_file_folder}/foo");
    std::fs::write(&demo_file_path, "demo text")
        .err_tip(|| format!("writing to {demo_file_path}"))?;
    debug!(%demo_file_path, "demo file path");

    // Add 60 seconds to the access time to trigger the logging message about access times
    fs_set_times::set_atime(
        &demo_file_path,
        SystemTime::now()
            .checked_add(Duration::from_secs(60))
            .unwrap()
            .into(),
    )?;

    FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.clone(),
        temp_path: temp_path.clone(),
        read_buffer_size: 1,
        ..Default::default()
    })
    .await
    .err_tip(|| "during FileSystemStore::new")?;

    assert!(logs_contain(
        "file access time newer than FilesystemStore start time file_name=foo"
    ));

    Ok(())
}

#[nativelink_test]
async fn test_get_file_entries_batch_zero_digest_returns_none() -> Result<(), Error> {
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
        &FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            read_buffer_size: 1,
            ..Default::default()
        },
        |from, to| std::fs::rename(from, to),
    )
    .await?;

    // Upload a normal file so we have something real in the store
    let normal_digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
    store
        .update_oneshot(normal_digest, VALUE1.into())
        .await?;

    // Both sha256 and blake3 zero digests
    let sha256_zero = ZERO_BYTE_DIGESTS[0];
    let blake3_zero = ZERO_BYTE_DIGESTS[1];

    // Batch with: normal digest, sha256 zero, blake3 zero, normal digest again
    let digests = vec![normal_digest, sha256_zero, blake3_zero, normal_digest];
    let results = store.get_file_entries_batch(&digests).await;

    assert_eq!(results.len(), 4, "Should return one result per input digest");

    // Normal digest should return Some (it exists in the store)
    assert!(
        results[0].is_some(),
        "Normal digest should return Some from get_file_entries_batch"
    );

    // SHA256 zero digest should return None (not a synthetic FileEntry)
    assert!(
        results[1].is_none(),
        "SHA256 zero digest should return None from get_file_entries_batch"
    );

    // Blake3 zero digest should return None (not a synthetic FileEntry)
    assert!(
        results[2].is_none(),
        "Blake3 zero digest should return None from get_file_entries_batch"
    );

    // Second normal digest should also return Some
    assert!(
        results[3].is_some(),
        "Duplicate normal digest should return Some from get_file_entries_batch"
    );

    Ok(())
}

#[nativelink_test]
async fn pin_digest_with_result_reports_eviction_race() -> Result<(), Error> {
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await?;

    let present_digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let absent_digest = DigestInfo::try_new(HASH2, VALUE2.len())?;

    Store::new(fs_store.clone())
        .update_oneshot(present_digest, VALUE1.into())
        .await?;

    // Present digest pins successfully.
    assert!(
        fs_store.pin_digest_with_result(&present_digest),
        "pin should succeed for a digest that is in the store"
    );

    // Absent digest reports the failure (the eviction-race signal we
    // need at the worker upload site to fall back to slow-store recovery).
    assert!(
        !fs_store.pin_digest_with_result(&absent_digest),
        "pin should report false for a digest that is not in the store"
    );

    // Batch variant returns one bool per input in order.
    let mut results = Vec::new();
    for d in [present_digest, absent_digest, present_digest] {
        results.push(fs_store.pin_digest_with_result(&d));
    }
    assert_eq!(results, vec![true, false, true]);

    Ok(())
}

/// #605 Bug A regression (production-composition seam): when
/// `content_path` is OVER-CAP at startup, `FilesystemStore::new` must
/// bleed it down to the configured byte cap before returning. The unit
/// test in `moka_evicting_map.rs`
/// (`run_pending_tasks_and_drain_evicts_startup_overshoot`) proves moka
/// fires the eviction listener; this test proves the LISTENER →
/// `FileEntryImpl::unref` → `content_path → temp_path` rename actually
/// runs against real on-disk files via real `FilesystemStore::new` —
/// the seam the unit test cannot reach.
///
/// Layout: each pre-seeded file lives at
/// `{content_path}/d/{hash[0..2]}/{hash}-{size}` (matches
/// `digest_content_path`). With `block_size = 1`, `size_on_disk ==
/// data_size`, so moka's KB-rounded weight is deterministic.
///
/// Mutation step: comment out the
/// `evicting_map.run_pending_tasks_and_drain().await;` line in
/// `FilesystemStore::new` (after `add_files_to_cache`) — the
/// post-construction file-count assertion below MUST red-fail with
/// "#605 fix: content_path must be bled down to cap during startup"
/// because moka's listener never fires for the startup overshoot and
/// every pre-seeded file remains in `content_path`.
#[nativelink_test]
async fn startup_over_cap_content_path_drained_to_cap() -> Result<(), Error> {
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    // Five 10 KiB files = 50 KiB on disk; cap = 20 KiB. moka's KB-scaled
    // capacity: max_capacity = 20 KiB / 1024 = 20; weight per file =
    // div_ceil(10 KiB, 1024) = 10. After drain at most 2 files (weight 20)
    // may remain — strictly less than 5.
    const FILE_BYTES: usize = 10 * 1024;
    const MAX_BYTES: usize = 20 * 1024;
    const NUM_FILES: usize = 5;
    const HASHES: [&str; NUM_FILES] = [
        "0123456789abcdef000000000000000000010000000000000123456789abcdef",
        "1123456789abcdef000000000000000000010000000000000123456789abcdef",
        "2123456789abcdef000000000000000000010000000000000123456789abcdef",
        "3123456789abcdef000000000000000000010000000000000123456789abcdef",
        "4123456789abcdef000000000000000000010000000000000123456789abcdef",
    ];

    let payload = make_random_data(FILE_BYTES);
    let mut digests = Vec::with_capacity(NUM_FILES);
    for hash in HASHES {
        let digest = DigestInfo::try_new(hash, FILE_BYTES)?;
        let file_path = digest_content_path(&content_path, &digest);
        // Create the sharded parent directory (`{content_path}/d/{shard}`).
        let parent = Path::new(&file_path)
            .parent()
            .expect("digest path must have a parent shard dir");
        fs::create_dir_all(parent).await?;
        std::fs::write(&file_path, &payload)
            .err_tip(|| format!("writing pre-seeded over-cap file {:?}", file_path))?;
        digests.push(digest);
    }

    // Build the store with a small byte cap. The drain inside
    // `FilesystemStore::new` MUST evict enough entries to bring the
    // moka cache at-or-below cap, which in turn renames their on-disk
    // files out of `content_path` and into `temp_path`. Wrap in a
    // tokio timeout: deadlock detector (e.g. drain looping forever)
    // beats hanging the suite.
    let store = tokio::time::timeout(
        Duration::from_secs(10),
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: Some(EvictionPolicy {
                max_bytes: MAX_BYTES,
                ..Default::default()
            }),
            block_size: 1,
            ..Default::default()
        }),
    )
    .await
    .expect("FilesystemStore::new must not deadlock — #605 startup drain contract violated")?;

    // The drain enqueues eviction events for moka's background listener
    // drain task; that task `spawn`s on the runtime so the rename can
    // land after `new` returns. Yield until the file count settles
    // at-or-below cap, bounded by a tokio timeout.
    let content_dir = format!("{content_path}/{DIGEST_FOLDER}");
    let drained: Result<(), Error> = tokio::time::timeout(
        Duration::from_secs(5),
        async {
            loop {
                let remaining = collect_digest_dir_files(&content_dir).await?;
                if remaining.len() <= 2 {
                    return Ok(());
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .map_err(|_| {
        make_err!(
            Code::Internal,
            "#605 fix: content_path must be bled down to cap during startup — \
             timed out waiting for drained eviction-listener renames"
        )
    })?;
    drained?;

    let remaining_content = collect_digest_dir_files(&content_dir).await?;
    let remaining_n = remaining_content.len();
    assert!(
        remaining_n <= 2,
        "#605 fix: content_path must be bled down to cap during startup — \
         pre-seeded {NUM_FILES}× {FILE_BYTES}-byte files on a {MAX_BYTES}-byte cap, \
         expected ≤ 2 remaining in content_path after `FilesystemStore::new`, got {remaining_n}",
    );
    assert!(
        remaining_n < NUM_FILES,
        "#605 fix: content_path drain produced ZERO evictions — startup overshoot was not enforced",
    );

    // (Renamed-to-temp files are picked up immediately by
    // `prune_temp_path`, which runs right after the drain inside
    // `FilesystemStore::new`. So we don't assert on temp_path contents
    // here — they're transient and gone by the time `new` returns.)

    // The moka index agrees with the disk: surviving digests are
    // visible via `has`, evicted ones are not. Use the store's own
    // visibility primitive (this is the seam the unit test cannot
    // reach).
    let store = Store::new(store);
    let mut visible = 0usize;
    for digest in &digests {
        if store.has(*digest).await?.is_some() {
            visible += 1;
        }
    }
    assert_eq!(
        visible, remaining_n,
        "#605 fix: moka entry count must match remaining on-disk file count after startup drain"
    );

    Ok(())
}

/// FL-681 follow-up: the `pending_bis_pin_max_bytes` config field plumbs
/// through `FilesystemStore::new` to the eviction map's
/// `indefinite_pin_cap`. With an EXPLICIT small value, indefinite pins
/// (the F2 pinned-until-BIS-ack pins) are capped at exactly that value —
/// NOT at the much larger default `pin_cap` (25% of `max_bytes`).
///
/// This is the load-bearing plumbing assertion: `max_bytes = 1 MiB` makes
/// `pin_cap = 256 KiB`, so the total-pin check never fires for our tiny
/// blobs; the ONLY thing that can refuse the third 2048-byte indefinite
/// pin is the explicit `pending_bis_pin_max_bytes = 4096` reaching the
/// constructor. If the config value were dropped on the way down (the
/// mutation below), the cap would default to `pin_cap = 256 KiB` and the
/// third pin would WRONGLY succeed.
///
/// Mutation (Change 1 plumbing): in `filesystem_store.rs`, revert the
/// `with_anchor_and_indefinite_cap(eviction_policy, now, <field>)` call to
/// `with_anchor(eviction_policy, now)` (which always passes `0`). This
/// test must red-fail at the bespoke "explicit pending_bis_pin_max_bytes
/// must cap indefinite pins" assertion: the third pin would return `true`
/// because the cap silently reverted to `pin_cap`.
#[nativelink_test]
async fn pending_bis_pin_max_bytes_caps_indefinite_pins() -> Result<(), Error> {
    const BLOB_SIZE: usize = 2048;
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            // pin_cap = 25% × 1 MiB = 256 KiB — far above 3×2048 so the
            // TOTAL pin check never fires; only the explicit indefinite
            // cap can refuse a pin.
            max_bytes: 1024 * 1024,
            ..Default::default()
        }),
        // EXPLICIT indefinite-pin cap: fits exactly two 2048-byte blobs.
        pending_bis_pin_max_bytes: 4096,
        block_size: 1,
        ..Default::default()
    })
    .await?;

    let d0 = make_distinct_blob(&store, 0, BLOB_SIZE).await?;
    let d1 = make_distinct_blob(&store, 1, BLOB_SIZE).await?;
    let d2 = make_distinct_blob(&store, 2, BLOB_SIZE).await?;

    assert!(
        store.pin_digest_indefinite_with_result(&d0),
        "first indefinite pin (2048 ≤ 4096) must fit under the explicit cap"
    );
    assert!(
        store.pin_digest_indefinite_with_result(&d1),
        "second indefinite pin (4096 ≤ 4096) must fill the explicit cap exactly"
    );
    assert!(
        !store.pin_digest_indefinite_with_result(&d2),
        "explicit pending_bis_pin_max_bytes must cap indefinite pins: the third \
         2048-byte indefinite pin (total 6144 > 4096) must be REFUSED \
         (backpressure). If this returns true, the config value never reached the \
         eviction map constructor and the cap silently reverted to pin_cap (256 KiB)."
    );

    Ok(())
}

/// FL-681 follow-up backward-compat: when `pending_bis_pin_max_bytes` is
/// UNSET (0, the default), the indefinite-pin cap falls back to the
/// eviction map's `pin_cap` (25% of `max_bytes`). Existing configs (which
/// never set this field) therefore behave EXACTLY as they did before the
/// field existed: indefinite pins share the normal total pin budget.
///
/// `max_bytes = 16 KiB` → `pin_cap = 4096`. With the field unset, the
/// indefinite cap resolves to `pin_cap = 4096`, so the first 2048-byte
/// indefinite pin fits and the third (total 6144) is refused. This proves
/// `0` resolves to a NON-ZERO, `pin_cap`-derived value — existing configs
/// (which never set the field) keep their pre-field behavior.
///
/// Mutation (default fallback): in `moka_evicting_map.rs`
/// `with_anchor_and_indefinite_cap`, delete the `if indefinite_pin_cap_bytes
/// == 0 { pin_cap }` branch so the `0` default is used VERBATIM as the cap
/// (`let indefinite_pin_cap = indefinite_pin_cap_bytes;`). The
/// `indefinite_cap_admits` check then becomes `current + size <= 0`, which
/// refuses EVERY indefinite pin (size > 0). This test red-fails at the
/// bespoke "unset pending_bis_pin_max_bytes must fall back to pin_cap (not
/// be used verbatim as 0)" assertion on the FIRST pin — verified
/// 2026-06-18 (`/tmp/fl681-followup-mutation-fallback2.log`). (A
/// `0 => u64::MAX` mutation is NOT a valid falsification here: the separate
/// total-`pin_cap` gate at `pin_key_with_mode` would still refuse the third
/// pin, masking the change — the verbatim-0 mutation is the one that
/// isolates the fallback.)
#[nativelink_test]
async fn pending_bis_pin_unset_falls_back_to_pin_cap() -> Result<(), Error> {
    const BLOB_SIZE: usize = 2048;
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            // pin_cap = 25% × 16 KiB = 4096 — exactly two 2048-byte blobs.
            max_bytes: 16 * 1024,
            ..Default::default()
        }),
        // UNSET: 0 must fall back to pin_cap (= 4096 here).
        pending_bis_pin_max_bytes: 0,
        block_size: 1,
        ..Default::default()
    })
    .await?;

    let d0 = make_distinct_blob(&store, 0, BLOB_SIZE).await?;
    let d1 = make_distinct_blob(&store, 1, BLOB_SIZE).await?;
    let d2 = make_distinct_blob(&store, 2, BLOB_SIZE).await?;

    assert!(
        store.pin_digest_indefinite_with_result(&d0),
        "unset pending_bis_pin_max_bytes must fall back to pin_cap (not be used \
         verbatim as 0): the first 2048-byte indefinite pin must fit under \
         pin_cap (4096). If this is false, the 0 default was used verbatim as \
         the cap and refuses every indefinite pin — backward-compat broken."
    );
    assert!(
        store.pin_digest_indefinite_with_result(&d1),
        "second indefinite pin (4096 ≤ pin_cap 4096) must fill pin_cap exactly"
    );
    assert!(
        !store.pin_digest_indefinite_with_result(&d2),
        "unset pending_bis_pin_max_bytes (fallback = pin_cap 4096): the third \
         2048-byte indefinite pin (total 6144 > 4096) must be REFUSED \
         (backpressure)."
    );

    Ok(())
}

/// FL-681 fix-up (MAJOR-2): the BIS-release seam at the production
/// FilesystemStore type. An F2 output blob is pinned INDEFINITELY (held
/// until the server's BlobsInStableStorage ack). The release MUST depend
/// ONLY on the BIS-ack (`unpin_digest`, the primitive the worker's BIS-ack
/// handler calls at `local_worker.rs` after decoding a `BlobsInStableStorageChunk`),
/// NEVER on the 120s TTL sweep.
///
/// This is the seam distributed-systems-reviewer + red-team flagged as
/// having ZERO coverage: for an already-present output (server returns OK
/// without writing → no BIS emitted from the write path), the indefinite
/// pin's release comes from the delta `BlobsAvailable → mark_stable → BIS`
/// path. The MISSED-tick recoverability of that delivery path is a
/// documented residual (self-heals on worker reconnect — the reconnect
/// full snapshot at `get_all_entries_with_timestamps` includes pinned
/// entries; bounded by `indefinite_pin_cap`; see the FL-681 design doc
/// MAJOR-2 section). What this test pins is the RELEASE CONTRACT itself:
/// whenever the BIS-ack DOES arrive (via any delivery path), `unpin_digest`
/// releases the indefinite pin and frees BOTH `pinned_bytes` and
/// `indefinite_pinned_bytes` so the next pending-BIS blob gets cap headroom.
///
/// Mutation: in `moka_evicting_map.rs` `unpin_key`, comment out the
/// `if entry.indefinite { ...fetch_sub... }` decrement. This test must
/// red-fail at the bespoke "BIS-ack did not free indefinite-cap headroom"
/// assertion (the pin would release the total but leak the indefinite
/// accounting — the exact MAJOR-1a class on the release side).
#[nativelink_test]
async fn bis_ack_releases_indefinite_f2_pin_independent_of_ttl() -> Result<(), Error> {
    const BLOB_SIZE: usize = 2048;
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024 * 1024,
            ..Default::default()
        }),
        // Explicit small indefinite cap so we can prove the BIS-ack frees
        // headroom: with the cap at 4096, a third pin is refused until an
        // ack releases one.
        pending_bis_pin_max_bytes: 4096,
        block_size: 1,
        ..Default::default()
    })
    .await?;

    let d0 = make_distinct_blob(&store, 0, BLOB_SIZE).await?;
    let d1 = make_distinct_blob(&store, 1, BLOB_SIZE).await?;
    let d2 = make_distinct_blob(&store, 2, BLOB_SIZE).await?;

    // The F2 output pins (indefinite, until BIS-ack).
    assert!(
        store.pin_digest_indefinite_with_result(&d0),
        "first F2 indefinite pin must succeed"
    );
    assert!(
        store.pin_digest_indefinite_with_result(&d1),
        "second F2 indefinite pin fills the indefinite cap exactly"
    );
    assert_eq!(
        store.indefinite_pinned_bytes(),
        4096,
        "two indefinite F2 pins must account for exactly 4096 bytes"
    );

    // Drive the TTL sweep past the 120s deadline with NO BIS-ack. The
    // indefinite pins MUST survive — release is decoupled from the TTL.
    assert!(
        store.test_force_pin_expired(&d0),
        "d0 must be pinned so its deadline can be rewound"
    );
    assert!(
        store.test_force_pin_expired(&d1),
        "d1 must be pinned so its deadline can be rewound"
    );
    store.test_expire_stale_pins().await;
    assert_eq!(
        store.indefinite_pinned_bytes(),
        4096,
        "release-seam violation: the 120s TTL sweep released an F2 indefinite pin — \
         the indefinite pin must be released ONLY by the BIS-ack, never by the TTL \
         (this is the 3,881-event silent-loss leak)"
    );

    // A third pin is still refused (cap full) — proving the pins are held.
    assert!(
        !store.pin_digest_indefinite_with_result(&d2),
        "third indefinite pin must be refused while the cap is full of un-acked pins"
    );

    // Deliver the BIS-ack for d0 (the `unpin_digest` primitive the worker's
    // BIS-ack handler calls). This MUST release the indefinite pin AND free
    // its indefinite-cap headroom.
    store.unpin_digest(&d0);
    assert_eq!(
        store.indefinite_pinned_bytes(),
        2048,
        "BIS-ack did not free indefinite-cap headroom: unpin_digest released the pin \
         but leaked the indefinite_pinned_bytes accounting — the next pending-BIS blob \
         is wrongly refused a pin and falls back to the 120s TTL"
    );

    // With headroom freed by the ack, the previously-refused pin now fits —
    // proving the release seam actually reclaims cap capacity end-to-end.
    assert!(
        store.pin_digest_indefinite_with_result(&d2),
        "after the BIS-ack freed cap headroom, the backpressured F2 output must pin"
    );

    Ok(())
}

/// FL-681 fix-up (MAJOR-1b): a cap-refused fresh F2 output pin MUST fall
/// back to a TIME-BOUNDED pin, NOT be left fully evictable.
///
/// The pre-fix behavior: when the indefinite cap was exhausted,
/// `pin_digest_indefinite_with_result` returned `false` and inserted NO
/// pin — the F2 output was fully LRU-evictable, the exact loss class under
/// a sustained outage (cap-saturated + evicted + not-yet-durable).
///
/// `pin_digest_indefinite_or_time_bounded` closes the "no pin at all" gap:
/// on cap refusal it takes a time-bounded `pin_key` (counts against the
/// total `pin_cap`, NOT the indefinite cap, so it admits while the
/// indefinite cap is full). After the fallback, `pinned_bytes` includes the
/// blob but `indefinite_pinned_bytes` does NOT — proving it is a
/// time-bounded (not indefinite) pin and the indefinite cap was not
/// exceeded.
///
/// This is a MITIGATION (pre-FL-681 ~120s floor), not durability closure —
/// see the helper doc-comment. The test pins the floor: the cap-refused
/// output is protected, not abandoned.
///
/// Mutation: in `filesystem_store.rs`
/// `pin_digest_indefinite_or_time_bounded`, replace the
/// `self.pin_digest_with_result(digest)` fallback branch with `false` (i.e.
/// revert to the old "leave it evictable" behavior). This test must
/// red-fail at the bespoke "cap-refused F2 output left fully evictable"
/// assertion: `pinned_bytes` would stay at the 2 indefinite pins (4096)
/// instead of growing to include the time-bounded fallback.
#[nativelink_test]
async fn cap_refused_f2_pin_falls_back_to_time_bounded_not_evictable() -> Result<(), Error> {
    const BLOB_SIZE: usize = 2048;
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            // pin_cap = 25% × 1 MiB = 256 KiB — far above our blobs, so the
            // TOTAL pin check never refuses a time-bounded fallback.
            max_bytes: 1024 * 1024,
            ..Default::default()
        }),
        // Indefinite cap fits exactly two 2048-byte blobs.
        pending_bis_pin_max_bytes: 4096,
        block_size: 1,
        ..Default::default()
    })
    .await?;

    let d0 = make_distinct_blob(&store, 0, BLOB_SIZE).await?;
    let d1 = make_distinct_blob(&store, 1, BLOB_SIZE).await?;
    let d2 = make_distinct_blob(&store, 2, BLOB_SIZE).await?;

    // Saturate the indefinite cap with two F2 output pins.
    assert_eq!(
        store.pin_digest_indefinite_or_time_bounded(&d0),
        IndefinitePinOutcome::Indefinite,
        "first F2 output should get a full indefinite pin (cap not yet saturated)"
    );
    assert_eq!(
        store.pin_digest_indefinite_or_time_bounded(&d1),
        IndefinitePinOutcome::Indefinite,
        "second F2 output should get a full indefinite pin (fills cap exactly)"
    );
    assert_eq!(
        store.indefinite_pinned_bytes(),
        4096,
        "two indefinite pins fill the 4096-byte cap"
    );
    assert_eq!(store.pinned_bytes(), 4096, "all pinned bytes are indefinite so far");

    // A third F2 output arrives while the indefinite cap is SATURATED. It
    // MUST NOT be left fully evictable — the fallback takes a time-bounded
    // pin instead.
    assert_eq!(
        store.pin_digest_indefinite_or_time_bounded(&d2),
        IndefinitePinOutcome::TimeBoundedFallback,
        "cap-saturated F2 output must fall back to a TIME-BOUNDED pin, not be refused outright"
    );

    // The fallback pin protects the blob: total pinned bytes grew to include
    // d2 (6144), but the indefinite accounting did NOT (still 4096) — so the
    // blob is a time-bounded pin, not left fully evictable and not wrongly
    // counted against the indefinite cap.
    assert_eq!(
        store.indefinite_pinned_bytes(),
        4096,
        "the time-bounded fallback must NOT be counted against the indefinite cap \
         (it would corrupt the BIS-pin backlog gauge)"
    );
    assert_eq!(
        store.pinned_bytes(),
        6144,
        "cap-refused F2 output left fully evictable: the time-bounded fallback pin was \
         not taken, so the blob is unprotected and lost under cap-saturation + eviction \
         + not-yet-durable (the FL-681 loss class). Expected pinned_bytes to include the \
         fallback (6144), got the two indefinite pins only."
    );

    Ok(())
}

/// FL-681 follow-up helper: write a distinct content-addressed blob of
/// `size` bytes (first byte = `idx`) through the store so it lands in the
/// eviction map and can be pinned. Returns its digest.
async fn make_distinct_blob(
    store: &FilesystemStore<FileEntryImpl>,
    idx: u8,
    size: usize,
) -> Result<DigestInfo, Error> {
    let mut data = make_random_data(size);
    data[0] = idx;
    let hash: [u8; 32] = Sha256::digest(&data).into();
    let digest = DigestInfo::new(hash, size as u64);
    store
        .update_oneshot(digest, Bytes::from(data))
        .await
        .err_tip(|| "writing pinnable blob in FL-681 follow-up test")?;
    Ok(digest)
}

// ═══════════════════════════════════════════════════════════════════════════
// FL-688 v3 Stage C: startup reconcile gate + reconcile-pin tests
//
// Three invariants tested:
//
// (a+b+c) RECONCILE-PIN SURVIVAL: a blob re-admitted UNPINNED at startup
//   but then reconcile-pinned (indefinite) BEFORE a runtime insert fires moka's
//   per-insert capacity check (which would evict the over-cap LRU entry) MUST
//   survive — the pin moves it out of moka's evictable set.
//
// (d) GATE-SEMANTICS: `set_startup_reconcile_gate` arms the gate (flag=false);
//   `release_startup_reconcile_gate` releases it (flag=true). The
//   `reconcile_complete_flag()` Arc reflects both transitions. The background
//   `drain_interval` tick in `drain_evictions` reads this flag; when false it
//   skips `run_pending_tasks_and_drain`. This test verifies the flag
//   transitions, which are the ONLY observable contract of the gate from
//   outside the background task.
//
// (e) GATE-ONLY-RELEASED-BY-SIGNAL: the gate flag is NOT released by
//   `pin_digest_indefinite_or_time_bounded` (the reconcile-pin call). Only
//   `release_startup_reconcile_gate` releases it. Calling reconcile-pin while
//   the gate is armed leaves the gate armed.
// ═══════════════════════════════════════════════════════════════════════════

/// FL-688 v3 Stage C — Tests (a)+(b)+(c): Reconcile-pin (indefinite) established
/// BEFORE a runtime insert fires moka's per-insert capacity check.
///
/// Setup: store with `max_bytes = 2 × BLOB_SIZE` (two blobs fit). Two blobs
/// are written (store is AT cap). Then:
///
///   1. The startup reconcile gate is armed (`set_startup_reconcile_gate`).
///   2. `d0` is reconcile-pinned INDEFINITELY via `pin_digest_indefinite_with_result`.
///      This moves d0 out of moka's evictable set into the pinned DashMap.
///   3. A RUNTIME INSERT (`d2`, size = BLOB_SIZE) puts the cache OVER cap.
///      Moka's per-insert capacity check fires. d0 is in the pinned DashMap
///      so only d1 is evictable. d0 MUST survive.
///   4. Assert `d0` is still pinned (indefinite pin bytes > 0) and readable.
///
/// Mutation: skip `pin_digest_indefinite_with_result` at step 2. Without the
/// pin, d0 remains in moka's evictable set. The runtime insert may evict d0.
/// The assertion "needed blob evicted before reconcile-pin: d0 must still be
/// pinned after the runtime insert" fires.
///
/// The assertion tests the pin invariant: a PINNED entry cannot be evicted by
/// moka's capacity check (pinned entries live in a side DashMap outside the
/// moka cache). This is the production invariant, not a scheduling assumption.
#[nativelink_test]
async fn v3c_reconcile_pin_survives_runtime_insert() -> Result<(), Error> {
    // N blobs fit; pin_cap = 25% x max_bytes. We need pin_cap >= BLOB_SIZE so
    // that pin_digest_indefinite_with_result can admit at least one blob.
    // With N_AT_CAP=5: pin_cap = 25% x (5 x BLOB_SIZE) = 1.25 x BLOB_SIZE >= BLOB_SIZE.
    const BLOB_SIZE: usize = 1024;
    const N_AT_CAP: u8 = 5; // store fits exactly 5 blobs; 6th triggers eviction

    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: (N_AT_CAP as usize) * BLOB_SIZE,
            evict_bytes: 0,
            max_seconds: 0,
            max_count: 0,
            pin_cap_bytes: 0,
        }),
        block_size: 1,
        // pending_bis_pin_max_bytes = 0 falls back to pin_cap = 1280 >= 1024.
        ..Default::default()
    })
    .await?;

    // Write N_AT_CAP blobs -> store is AT cap. d0 is inserted first (LRU target
    // without pin; with pin it is protected from moka's capacity check).
    let d0 = make_distinct_blob(&store, 0, BLOB_SIZE).await?;
    for i in 1..N_AT_CAP {
        let _ = make_distinct_blob(&store, i, BLOB_SIZE).await?;
    }

    // Step 1: arm the startup reconcile gate.
    store.set_startup_reconcile_gate();

    // MUTATION TARGET: comment out the next two lines to simulate
    // "skip reconcile-pin". Without the pin, d0 remains in moka's evictable
    // set. The runtime insert below puts the cache over cap and moka may
    // evict d0. The assertion at step 4 fires:
    // "needed blob evicted before reconcile-pin: d0 must still be pinned
    //  after the runtime insert"
    //
    // Step 2: reconcile-pin d0 INDEFINITELY before any runtime insert.
    let pinned = store.pin_digest_indefinite_with_result(&d0);
    assert!(
        pinned,
        "reconcile-pin precondition: d0 must be in eviction map at pin time          (no runtime insert has happened; if this fails check max_bytes config)"
    );

    // Step 3: runtime insert (blob N_AT_CAP+1) fires moka's per-insert
    // capacity check. Cache goes to N_AT_CAP+1 blobs (over cap). d0 is in
    // the pinned DashMap so moka cannot evict it. d0 MUST survive.
    let _ = make_distinct_blob(&store, N_AT_CAP, BLOB_SIZE).await?;

    // Step 4: d0 must still be pinned.
    let indef_bytes = store.indefinite_pinned_bytes();
    assert!(
        indef_bytes >= BLOB_SIZE as u64,
        "needed blob evicted before reconcile-pin: d0 must still be pinned          after the runtime insert (indefinite_pinned_bytes = {}, expected >= {});          mutation: skip pin_digest_indefinite_with_result above and moka may          evict d0 as LRU, indef_bytes drops to 0 and this assertion fires",
        indef_bytes,
        BLOB_SIZE
    );

    // d0 must still be readable.
    let key = StoreKey::Digest(d0);
    tokio::time::timeout(
        Duration::from_secs(5),
        store.get_part_unchunked(key, 0, None),
    )
    .await
    .expect("must not deadlock -- reconcile-pin survival contract")
    .expect(
        "needed blob evicted before reconcile-pin: d0 not readable after          runtime insert; pinning failed to protect it from moka per-insert eviction"
    );

    Ok(())
}

/// FL-688 v3 Stage C — Test (d): gate-semantics: arm → false, release → true.
///
/// `set_startup_reconcile_gate` must store `false` into the shared
/// `Arc<AtomicBool>` (gate ARMED = drain blocked).
/// `release_startup_reconcile_gate` must store `true` (gate RELEASED = drain
/// runs normally).
/// `reconcile_complete_flag()` must return the SAME Arc that reflects both
/// transitions.
///
/// The background `drain_interval` tick in `drain_evictions` reads this
/// `Arc<AtomicBool>` with `Ordering::Acquire` and skips
/// `run_pending_tasks_and_drain` when it is `false`. This test verifies the
/// observable contract of the gate (the flag transitions) without racing the
/// 10-second tick interval.
///
/// Mutation: in `moka_evicting_map.rs::set_startup_reconcile_gate`, change
/// `store(false, ...)` to `store(true, ...)`. The gate becomes a no-op (drain
/// runs during reconcile window). The assertion "gate must be ARMED (false)
/// after set_startup_reconcile_gate" fires.
#[nativelink_test]
async fn v3c_gate_arm_and_release_semantics() -> Result<(), Error> {
    use core::sync::atomic::Ordering;

    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 64 * 1024,
            ..Default::default()
        }),
        block_size: 1,
        ..Default::default()
    })
    .await?;

    // Before arming: flag is `true` (default = gate off = drain runs normally).
    let flag = store.reconcile_complete_flag();
    assert!(
        flag.load(Ordering::Acquire),
        "gate must start RELEASED (true) by default — server-side stores must \
         never have their drain suppressed by an un-armed gate"
    );

    // MUTATION TARGET: set_startup_reconcile_gate must set flag to false.
    store.set_startup_reconcile_gate();
    assert!(
        !flag.load(Ordering::Acquire),
        "gate must be ARMED (false) after set_startup_reconcile_gate — the background \
         drain_interval tick checks this flag; if it stays true, the drain runs during \
         the reconcile window and can race the reconcile-pin calls"
    );

    // release_startup_reconcile_gate must flip flag back to true.
    store.release_startup_reconcile_gate();
    assert!(
        flag.load(Ordering::Acquire),
        "gate must be RELEASED (true) after release_startup_reconcile_gate — \
         normal periodic eviction must resume; if this stays false the drain is \
         permanently suppressed and the store never converges to cap after reconcile"
    );

    // The Arc returned by reconcile_complete_flag() must reflect live changes —
    // arm/release again and verify via the same Arc handle.
    store.set_startup_reconcile_gate();
    assert!(
        !flag.load(Ordering::Acquire),
        "reconcile_complete_flag() Arc must reflect live gate transitions — \
         the returned handle must NOT be a snapshot but a live shared reference"
    );

    Ok(())
}

/// FL-688 v3 Stage C — Test (e): the gate is NOT released by reconcile-pin.
///
/// Calling `pin_digest_indefinite_or_time_bounded` (the reconcile-pin
/// primitive) while the gate is armed MUST leave the gate armed. Only
/// `release_startup_reconcile_gate` releases the gate.
///
/// Ordering A requires: (1) arm gate, (2) pin blobs, (3) release gate.
/// If pinning released the gate, the executor might unblock before all blobs
/// are pinned, allowing runtime inserts to race the remaining pin calls.
///
/// Mutation: in `filesystem_store.rs::pin_digest_indefinite_or_time_bounded`,
/// add `self.release_startup_reconcile_gate()` before returning. The assertion
/// "gate must remain ARMED after reconcile-pin" fires.
#[nativelink_test]
async fn v3c_gate_not_released_by_reconcile_pin() -> Result<(), Error> {
    use core::sync::atomic::Ordering;

    const BLOB_SIZE: usize = 2048;

    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 64 * 1024,
            ..Default::default()
        }),
        block_size: 1,
        pending_bis_pin_max_bytes: BLOB_SIZE as u64 * 4,
        ..Default::default()
    })
    .await?;

    let d0 = make_distinct_blob(&store, 0, BLOB_SIZE).await?;

    // Arm the gate.
    store.set_startup_reconcile_gate();
    let flag = store.reconcile_complete_flag();
    assert!(
        !flag.load(Ordering::Acquire),
        "precondition: gate must be ARMED before the pin call"
    );

    // MUTATION TARGET: call reconcile-pin. The gate must stay armed.
    let outcome = store.pin_digest_indefinite_or_time_bounded(&d0);
    assert!(
        matches!(outcome, IndefinitePinOutcome::Indefinite),
        "reconcile-pin must succeed (blob is in map, cap not saturated)"
    );
    assert!(
        !flag.load(Ordering::Acquire),
        "gate must remain ARMED after reconcile-pin: only release_startup_reconcile_gate \
         releases the gate; pin calls must not release it (Ordering A: executor unblock \
         must happen AFTER all blobs are pinned, not after each individual pin)"
    );

    // Verify the gate is released by the correct call.
    store.release_startup_reconcile_gate();
    assert!(
        flag.load(Ordering::Acquire),
        "gate must be RELEASED after release_startup_reconcile_gate"
    );

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// FL-688 v3 Stage C BLOCK-2: boot-drain composite test
//
// Invariant: when `startup_reconcile_gate: true`, the one-shot boot drain
// (`run_pending_tasks_and_drain` at `filesystem_store.rs:1039`) is SUPPRESSED.
// A needed blob that was loaded from disk during `add_files_to_cache` MUST be
// reconcile-pinnable after construction (not evicted by the boot drain).
//
// Mutation target: change `if !spec.startup_reconcile_gate` in
// `filesystem_store.rs` to `if true` (always drain) → boot drain fires even
// when gate is armed → blob evicted → reconcile-pin returns Refused → the
// assertion "needed blob evicted by boot drain before gate armed — BLOCK-2
// regression" fires.
// ═══════════════════════════════════════════════════════════════════════════

/// FL-688 v3 Stage C — BLOCK-2: arm gate at construction, boot drain suppressed.
///
/// A single blob is written to disk and then the store is reopened with
/// `startup_reconcile_gate: true` at EXACTLY-cap (so ANY boot drain eviction
/// would remove it). The blob must be reconcile-pinnable after construction,
/// proving the boot drain was suppressed.
///
/// Mutation: change `if !spec.startup_reconcile_gate` to `if true` in
/// `filesystem_store.rs:1039` → boot drain fires → blob evicted → pin Refused →
/// assertion fires: "needed blob evicted by boot drain before gate armed —
/// BLOCK-2 regression".
#[nativelink_test]
async fn v3c_block2_boot_drain_suppressed_when_gate_armed_at_construction() -> Result<(), Error> {
    // Strategy: seed N+1 blobs so the store is OVER CAP at startup. The boot
    // drain (`run_pending_tasks_and_drain`) removes exactly 1 blob (the LRU).
    // With the gate suppressing the boot drain, ALL N+1 blobs must survive:
    // `has_with_results` returns Some for every seeded digest. Without the
    // gate, exactly 1 returns None (LRU-evicted). The assertion checks that
    // ALL N+1 are present, which is only true when the drain was suppressed.
    //
    // N_AT_CAP = 5: max_bytes = 5 × BLOB_SIZE; pin_cap = 25% × 5 × 1024 = 1280.
    // The indefinite pin check needs pin_cap >= BLOB_SIZE (1024) — satisfied.
    const BLOB_SIZE: usize = 1024;
    const N_AT_CAP: usize = 5; // max_bytes = N_AT_CAP × BLOB_SIZE; exactly 5 fit

    let content_path = make_temp_path("v3c_block2_content");
    let temp_path = make_temp_path("v3c_block2_temp");

    // Step 1: seed N+1 blobs on disk (no cap limit).
    let mut seeded_digests: Vec<DigestInfo> = Vec::new();
    {
        let seed_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: None,
            block_size: 1,
            ..Default::default()
        })
        .await?;
        for i in 0..=(N_AT_CAP as u8) {
            let data: Vec<u8> = (0..BLOB_SIZE).map(|j| ((j + i as usize) % 0x7f) as u8).collect();
            let hash: [u8; 32] = Sha256::digest(&data).into();
            let digest = DigestInfo::new(hash, BLOB_SIZE as u64);
            seed_store
                .update_oneshot(digest, Bytes::from(data))
                .await
                .err_tip(|| "writing seed blob")?;
            seeded_digests.push(digest);
        }
        // N_AT_CAP+1 = 6 blobs seeded; cap = N_AT_CAP = 5 → over cap by 1.
    }
    assert_eq!(seeded_digests.len(), N_AT_CAP + 1);

    // Step 2: re-open with gate armed + cap = N_AT_CAP blobs.
    // Boot drain suppressed → all 6 blobs survive.
    let store = tokio::time::timeout(
        Duration::from_secs(10),
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: Some(EvictionPolicy {
                max_bytes: N_AT_CAP * BLOB_SIZE,
                evict_bytes: 0,
                max_seconds: 0,
                max_count: 0,
                pin_cap_bytes: 0,
            }),
            block_size: 1,
            startup_reconcile_gate: true, // BLOCK-2 fix: suppress boot drain
            ..Default::default()
        }),
    )
    .await
    .expect("store construction must not deadlock (10s)")?;

    // Step 3: all seeded blobs must be present (boot drain was suppressed).
    let keys: Vec<StoreKey<'static>> = seeded_digests.iter().map(|d| StoreKey::from(*d)).collect();
    let mut results = vec![None; keys.len()];
    tokio::time::timeout(
        Duration::from_secs(5),
        store.has_with_results(&keys, &mut results),
    )
    .await
    .expect("has_with_results must not deadlock (boot drain suppression test)")
    .err_tip(|| "has_with_results failed")?;

    // If the boot drain ran, one blob would be None (evicted). All must be Some.
    let missing: Vec<DigestInfo> = seeded_digests
        .iter()
        .zip(results.iter())
        .filter_map(|(d, r)| if r.is_none() { Some(*d) } else { None })
        .collect();
    assert!(
        missing.is_empty(),
        "BLOCK-2 boot-drain-suppression regression: {} of {} seeded blobs evicted during \
         store construction (expected 0 — gate suppresses boot drain). Missing: {missing:?}. \
         MUTATION target: change `if !spec.startup_reconcile_gate` to `if true` at \
         filesystem_store.rs:1060 — boot drain runs → 1 blob evicted → `missing` is non-empty",
        missing.len(),
        seeded_digests.len(),
    );

    store.release_startup_reconcile_gate();
    Ok(())
}

/// FL-688 v3 Stage C — `startup_reconcile_gate: true` in FilesystemSpec arms the
/// shared `Arc<AtomicBool>` in MokaEvictingMap; `release_startup_reconcile_gate`
/// releases it. `reconcile_complete_flag()` returns that SAME Arc so all three
/// observing sites (FilesystemStore wrapper, MokaEvictingMap drain loop, local_worker
/// fail-open timer) agree on the gate state.
///
/// Mutation: remove `evicting_map.set_startup_reconcile_gate()` from
/// `FilesystemStore::new`. The flag is never set to `false` → the first assertion
/// fires: "Arc-sharing: startup_reconcile_gate must arm the shared flag".
#[nativelink_test]
async fn v3c_drain_tick_suppressed_gate_release_confirms_arc_shared() -> Result<(), Error> {
    let store = tokio::time::timeout(
        Duration::from_secs(10),
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: make_temp_path("v3c_arc_content"),
            temp_path: make_temp_path("v3c_arc_tmp"),
            block_size: 1,
            startup_reconcile_gate: true,
            ..Default::default()
        }),
    )
    .await
    .expect("store construction must not deadlock (10s)")?;

    let flag = store.reconcile_complete_flag();

    // Gate armed at construction: flag must be false.
    assert!(
        !flag.load(Ordering::Acquire),
        "Arc-sharing: startup_reconcile_gate must arm the shared flag (set false). \
         MUTATION target: remove `evicting_map.set_startup_reconcile_gate()` from \
         FilesystemStore::new at filesystem_store.rs:1007 — flag stays true (default) \
         and this assertion fires"
    );

    // Release: flag must flip to true.
    store.release_startup_reconcile_gate();
    assert!(
        flag.load(Ordering::Acquire),
        "Arc-sharing: release_startup_reconcile_gate must set the shared flag to true. \
         The Arc is shared with MokaEvictingMap's drain loop so both observe the release. \
         MUTATION target: change `Ordering::Release` to `Ordering::Relaxed` in \
         moka_evicting_map.rs::release_startup_reconcile_gate — no observable change here, \
         but the drain-tick suppression test in moka_evicting_map.rs will catch it."
    );

    Ok(())
}


// ═══════════════════════════════════════════════════════════════════════════
// #speculative-prefetch P0 — C3/C5 production-composition disjoint-budget test.
//
// invariant-prover BLOCK (TLC-proven `.claude/tla/SpeculativePinBudget.tla` →
// NoStarve VIOLATED under the SHARED budget; `SpeculativePinBudgetFixed.tla` →
// HOLDS with the DISJOINT sub-budget). This is the store-level realization of
// that proof: a REAL `MokaEvictingMap`-backed `FilesystemStore` (the fast tier
// the worker's speculative construct pins into), a small `pin_cap`, SPECULATIVE
// pins saturating the sub-budget, then a REAL pin of a DIFFERENT digest → the
// real pin MUST be admitted (its blob stays resident → its populate is NOT
// starved into `Aborted`). Under the pre-fix shared budget the speculative pins
// consume the real pin_cap headroom and the real pin is REFUSED.
//
// This test is SEQUENTIAL (pin spec, then pin real, one task): it guards the
// disjoint-budget ARITHMETIC (the `.saturating_sub(speculative_pinned_bytes)`
// subtraction), which the mutation red-fails. The genuine interleaved race (the
// two-Relaxed-load TOCTOU) is machine-checked in
// `.claude/tla/SpeculativePinBudgetImplTOCTOU.tla`, not here (invariant-prover
// NIT: hence the name drops "concurrent").
// ═══════════════════════════════════════════════════════════════════════════
#[nativelink_test]
async fn speculative_pins_never_refuse_a_real_pin() -> Result<(), Error> {
    // max_bytes = 40_000 → pin_cap = 10_000 (25%), speculative_pin_cap = 2_000
    // (5%). All blobs fit in the cache (total < 40_000), so eviction never
    // confounds the pin-admission assertion.
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 40_000,
            ..Default::default()
        }),
        block_size: 1,
        ..Default::default()
    })
    .await?;

    // SPECULATIVE side: two 1000-byte blobs fill the 2000-byte speculative
    // sub-budget exactly. These add 2000 bytes to the SHARED pinned_bytes.
    let spec0 = make_distinct_blob(&store, 0, 1000).await?;
    let spec1 = make_distinct_blob(&store, 1, 1000).await?;
    let spec_res = store.pin_digests_speculative_with_results(&[spec0, spec1]);
    assert_eq!(
        spec_res,
        vec![true, true],
        "both speculative pins (2 × 1000 = 2000 = speculative_pin_cap) must be admitted"
    );

    // REAL side: a 9000-byte blob for a DIFFERENT digest. Real-pin admission
    // check EXCLUDES speculative bytes: (2000 − 2000) + 9000 = 9000 ≤ 10_000
    // pin_cap → ADMIT. Under the pre-fix shared budget it would be
    // 2000 + 9000 = 11_000 > 10_000 → REFUSED (the C3/C5 starvation: the real
    // action's blob stays LRU-evictable and its populate can be Aborted).
    let real = make_distinct_blob(&store, 2, 9000).await?;
    let real_res = store.pin_digests_with_results(&[real]);
    assert_eq!(
        real_res,
        vec![true],
        "composite invariant violated (2026-07-05): a speculative construct's pins \
         (2000 bytes) refused a concurrent real action's 9000-byte pin via the shared \
         pin_cap — the real blob would stay evictable and its populate could be \
         Aborted (the C3/C5 starvation the invariant-prover machine-checked). The \
         disjoint speculative sub-budget must be subtracted from the real-pin check."
    );

    // A second real pin adds up to the pin_cap boundary using ONLY real bytes:
    // real-only total 9000 + 900 = 9900 ≤ 10_000 → still admitted; counting the
    // 2000 speculative bytes (11_900 > 10_000) would refuse it under the bug.
    let real2 = make_distinct_blob(&store, 3, 900).await?;
    let real2_res = store.pin_digests_with_results(&[real2]);
    assert_eq!(
        real2_res,
        vec![true],
        "composite invariant violated (2026-07-05): real-only pinned total is 9900 ≤ \
         10_000 pin_cap; only the shared-budget bug (adding 2000 speculative bytes → \
         11_900 > 10_000) can refuse this second real pin"
    );

    Ok(())
}

/// #2424 (ported): `rename` ENOENT is ambiguous — a missing temp directory must
/// not be mistaken for a vanished source. With the content file still present,
/// `unref` must warn ("Failed to rename file") and leave the content file intact
/// rather than take the benign vanished-source path (which would flip the entry
/// to `Temp` and orphan the content file on disk). Guards the `source_gone`
/// `fs::metadata` disambiguation at `filesystem_store.rs` unref.
#[nativelink_test]
async fn unref_does_not_orphan_content_file_when_temp_dir_missing() -> Result<(), Error> {
    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");
    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: None,
            ..Default::default()
        })
        .await?,
    );
    store.update_oneshot(digest, VALUE1.into()).await?;
    let file_entry = store.get_file_entry_for_digest(&digest).await?;

    // Remove the temp dir so `unref`'s rename destination parent is gone
    // (ENOENT) while the source content file is perfectly intact. Removing the
    // whole DIGEST_FOLDER (with its shard subdirs) guarantees the rename
    // destination's parent shard directory is missing.
    fs::remove_dir_all(format!("{temp_path}/{DIGEST_FOLDER}")).await?;

    file_entry.unref().await;

    assert!(
        logs_contain("Failed to rename file"),
        "missing temp dir (source present) must warn, not be treated as benign"
    );
    assert!(
        !logs_contain("treating as benign"),
        "an intact content file must not take the benign vanished-source path"
    );

    // The content file must still exist — not orphaned by a wrong Temp flip.
    let content_file = digest_content_path(&content_path, &digest);
    let data = read_file_contents(&content_file).await?;
    assert_eq!(
        &data[..],
        VALUE1.as_bytes(),
        "content file must remain intact after a failed unref rename"
    );

    Ok(())
}
