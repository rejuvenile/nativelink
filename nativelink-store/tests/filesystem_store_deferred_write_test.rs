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

//! Ported from upstream v1.6.1 `filesystem_store_test.rs`. Isolated in its OWN
//! test binary because it lowers the PROCESS-WIDE `RLIMIT_FSIZE`. The fork runs
//! a single test file's tests concurrently, so if this test shared a binary
//! with the rest of the filesystem suite, a concurrent sibling writing >1 MiB
//! would fail with `EFBIG` during this test's rlimit window (observed: it broke
//! `file_gets_cleans_up_on_cache_eviction`). Alone in its own process there is
//! no sibling to interfere with.

use std::env;

use nativelink_config::stores::FilesystemSpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::filesystem_store::{DIGEST_FOLDER, FileEntryImpl, FilesystemStore};
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::store_trait::StoreLike;
use rand::Rng;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReadDirStream;

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data
    )
}

/// Collects all files (not directories) under a sharded digest directory.
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

/// This test simulates a full disk without needing one. It writes past the
/// `RLIMIT_FSIZE` cap, thus failing with `EFBIG`, which tokio defers exactly
/// like `ENOSPC`. The `SIGXFSZ` signal must be ignored or the kernel will kill
/// the process instead of failing the write.
///
/// SAFETY: `SIG_IGN` is process-wide but this binary runs only this test.
#[cfg(unix)]
#[nativelink_test]
async fn deferred_write_error_does_not_emplace_truncated_file() -> Result<(), Error> {
    use rlimit::Resource;

    const FILE_SIZE_LIMIT: u64 = 1024 * 1024;

    let content_path = make_temp_path("content_path");
    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: make_temp_path("temp_path"),
            ..Default::default()
        })
        .await?,
    );
    let data = vec![0u8; 2 * 1024 * 1024]; // 2 MiB
    let digest = DigestInfo::try_new(&"aa".repeat(32), data.len())?;

    // SAFETY: ignoring SIGXFSZ so the EFBIG write fails the syscall instead of
    // killing the process; this binary runs only this test.
    unsafe { libc::signal(libc::SIGXFSZ, libc::SIG_IGN) };
    let (old_soft, hard) = Resource::FSIZE.get()?;
    Resource::FSIZE.set(FILE_SIZE_LIMIT, hard)?;

    let result = store.update_oneshot(digest, data.into()).await;
    Resource::FSIZE.set(old_soft, hard)?;

    assert!(result.is_err(), "deferred write error must surface");

    // Fork adaptation: the content path is sharded (`d/{hash[0..2]}/...`), so
    // recurse the shard subdirs and assert no FILE landed anywhere.
    let ghosts = collect_digest_dir_files(&format!("{content_path}/{DIGEST_FOLDER}")).await?;
    assert!(
        ghosts.is_empty(),
        "no file may reach the content path, found {ghosts:?}"
    );
    Ok(())
}
