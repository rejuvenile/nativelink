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

use std::path::{Path, PathBuf};

use nativelink_error::{Error, make_err};

/// Indicates which method was used to clone a directory tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneMethod {
    /// macOS `clonefile(2)` CoW clone.
    Clonefile,
    /// Per-file `hard_link` + directory creation.
    Hardlink,
}

/// Collected tree operations for batch execution via io_uring.
/// Directories are created inline during collection (DFS walk ensures
/// parents exist before children), so only file and symlink ops remain.
struct TreeOps {
    /// (src, dst) pairs for hardlink.
    files: Vec<(PathBuf, PathBuf)>,
    /// (target, linkpath) pairs — target preserved as-is (may be relative).
    symlinks: Vec<(PathBuf, PathBuf)>,
}

impl TreeOps {
    fn total_ops(&self) -> usize {
        self.files.len() + self.symlinks.len()
    }
}

/// Walk `src` recursively, creating directories inline and collecting
/// hardlink/symlink operations into `TreeOps`. Runs synchronously inside
/// `spawn_blocking`. The root destination directory must already exist.
fn collect_tree_ops_sync(
    src: &Path,
    dst: &Path,
) -> Result<TreeOps, Error> {
    let mut ops = TreeOps {
        files: Vec::new(),
        symlinks: Vec::new(),
    };
    collect_tree_ops_recursive(src, dst, &mut ops)?;
    Ok(ops)
}

fn collect_tree_ops_recursive(
    src: &Path,
    dst: &Path,
    ops: &mut TreeOps,
) -> Result<(), Error> {
    for entry in std::fs::read_dir(src).map_err(|e| {
        make_err!(
            nativelink_error::Code::Internal,
            "Failed to read directory {}: {e}",
            src.display()
        )
    })? {
        let entry = entry.map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to read entry in {}: {e}",
                src.display()
            )
        })?;
        let ft = entry.file_type().map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to get file type for {:?}: {e}",
                entry.path()
            )
        })?;
        let dst_path = dst.join(entry.file_name());

        if ft.is_dir() {
            // Create directory immediately — DFS walk guarantees parent
            // already exists. This avoids collecting dirs and doing
            // separate depth-sorted batch creation.
            //
            // O5 concurrency: [C] (prepare_output_directory) may pre-create
            // subdirectories (e.g. `bazel-out/`) in the work_dir before [B2]'s
            // hardlink_directory_tree reaches them. Tolerate AlreadyExists when
            // the existing entry is a directory — a non-directory at a directory
            // path is still a genuine conflict. Mirrors the pattern in
            // download_to_directory's BFS mkdir at running_actions_manager.rs:1354.
            //
            // INVARIANT (O5, 333ce15e): concurrent writers to the paths this walk
            // touches — i.e. `[C]` = prepare_output_directory
            // (running_actions_manager.rs:2046) — MUST create only DIRECTORIES, never
            // files or symlinks. A non-dir at a directory path trips the is_dir() check
            // below into a `Code::AlreadyExists` error; a future `[C]` that placed a file
            // or symlink at an output path would surface as an opaque AlreadyExists with
            // no hint of this `[B2]`/`[C]` timing dependency.
            match std::fs::create_dir(&dst_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Verify it's a directory, not a file/symlink that
                    // happens to have the same name.
                    let m = std::fs::symlink_metadata(&dst_path).map_err(|me| {
                        make_err!(
                            nativelink_error::Code::Internal,
                            "Failed to create directory {}: already exists but could not stat: {me}",
                            dst_path.display()
                        )
                    })?;
                    if !m.is_dir() {
                        return Err(make_err!(
                            nativelink_error::Code::AlreadyExists,
                            "Failed to create directory {}: a non-directory entry already exists",
                            dst_path.display()
                        ));
                    }
                    // Pre-created by concurrent [C] — treat as Ok(()).
                }
                Err(e) => {
                    return Err(make_err!(
                        nativelink_error::Code::Internal,
                        "Failed to create directory {}: {e}",
                        dst_path.display()
                    ));
                }
            }
            collect_tree_ops_recursive(&entry.path(), &dst_path, ops)?;
        } else if ft.is_file() {
            ops.files.push((entry.path(), dst_path));
        } else if ft.is_symlink() {
            let target = std::fs::read_link(entry.path()).map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to read symlink {:?}: {e}",
                    entry.path()
                )
            })?;
            // Preserve the symlink target as-is (may be relative).
            ops.symlinks.push((target, dst_path));
        }
    }
    Ok(())
}

/// Execute pre-collected tree operations synchronously using std::fs calls.
/// Hardlinks files and creates symlinks. Directories are already created
/// during collection. Assumes the root destination directory already exists.
fn execute_tree_ops_sync(ops: &TreeOps) -> Result<(), Error> {
    for (src, dst) in &ops.files {
        std::fs::hard_link(src, dst).map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to hardlink {} to {}: {e}",
                src.display(),
                dst.display()
            )
        })?;
    }

    for (target, linkpath) in &ops.symlinks {
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, linkpath).map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to create symlink {}: {e}",
                linkpath.display()
            )
        })?;
        #[cfg(windows)]
        {
            if target.is_dir() {
                std::os::windows::fs::symlink_dir(target, linkpath).map_err(|e| {
                    make_err!(
                        nativelink_error::Code::Internal,
                        "Failed to create dir symlink {}: {e}",
                        linkpath.display()
                    )
                })?;
            } else {
                std::os::windows::fs::symlink_file(target, linkpath).map_err(|e| {
                    make_err!(
                        nativelink_error::Code::Internal,
                        "Failed to create file symlink {}: {e}",
                        linkpath.display()
                    )
                })?;
            }
        }
    }

    Ok(())
}

/// Copies an entire directory tree from source to destination using the
/// fastest available method:
///
/// - **macOS (APFS)**: Uses `clonefile(2)` for a CoW clone of the entire tree
///   in a single syscall (~1ms regardless of tree size). Falls back to hardlink
///   if clonefile fails (cross-device, non-APFS, etc.).
/// - **Linux with io_uring**: Creates directories inline during a single readdir
///   walk (DFS ensures parent-before-child), then batches hardlink and symlink
///   operations as io_uring SQEs for minimal syscall overhead.
/// - **Other platforms / small trees**: Hardlinks each file individually via
///   `std::fs::hard_link` inside `spawn_blocking`.
///
/// After a successful clonefile, directories are made writable (0o755) since the
/// clone inherits the cache's read-only permissions and actions need to create
/// output files.
pub async fn hardlink_directory_tree(src_dir: &Path, dst_dir: &Path) -> Result<CloneMethod, Error> {
    let src = src_dir.to_path_buf();
    let dst = dst_dir.to_path_buf();

    // macOS: try clonefile first.
    #[cfg(target_os = "macos")]
    {
        let src_clone = src.clone();
        let dst_clone = dst.clone();
        let clone_result = tokio::task::spawn_blocking(move || {
            try_clonefile(&src_clone, &dst_clone)
        })
        .await
        .map_err(|e| make_err!(nativelink_error::Code::Internal, "spawn_blocking join error: {e}"))?;

        match clone_result {
            Ok(()) => return Ok(CloneMethod::Clonefile),
            // #clonefile-fallback: the fallback is logged authoritatively INSIDE
            // `try_clonefile` at the classifying site (genuine syscall/fs failure
            // → error!, the perf-critical ~600× slowdown; expected dst-non-empty
            // precondition → warn!). Only the class-blind Err flows back here, so
            // do NOT re-log — it would double-emit and lose the class distinction.
            Err(_) => {}
        }
    }

    // Collect tree operations via synchronous readdir walk.
    // For small trees (< BATCH_THRESHOLD ops), execute directly in the same
    // spawn_blocking call to avoid a second spawn_blocking + re-walk.
    const BATCH_THRESHOLD: usize = 20;
    let src_collect = src.clone();
    let dst_collect = dst.clone();
    let tree_ops_result = tokio::task::spawn_blocking(move || {
        if !src_collect.exists() {
            return Err(make_err!(
                nativelink_error::Code::InvalidArgument,
                "Source directory does not exist: {}",
                src_collect.display()
            ));
        }
        // Create root destination before collection walk, since
        // collect_tree_ops_sync now creates subdirectories inline.
        std::fs::create_dir_all(&dst_collect).map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to create destination directory {}: {e}",
                dst_collect.display()
            )
        })?;
        let ops = collect_tree_ops_sync(&src_collect, &dst_collect)?;
        if ops.total_ops() < BATCH_THRESHOLD {
            // Small tree: execute file/symlink ops directly (no re-walk).
            execute_tree_ops_sync(&ops)?;
            return Ok(None);
        }
        Ok(Some(ops))
    })
    .await
    .map_err(|e| make_err!(nativelink_error::Code::Internal, "spawn_blocking join error: {e}"))??;

    // If small tree was already handled inside spawn_blocking, return early.
    let tree_ops_result = match tree_ops_result {
        Some(ops) => ops,
        None => return Ok(CloneMethod::Hardlink),
    };

    // Directories were already created during the collection walk.
    // Only file hardlinks and symlinks remain for io_uring batching.

    // Phase 1: Hardlink all files in one batch.
    if !tree_ops_result.files.is_empty() {
        let file_refs: Vec<(&Path, &Path)> = tree_ops_result
            .files
            .iter()
            .map(|(s, d)| (s.as_path(), d.as_path()))
            .collect();
        let results = crate::fs::hard_link_batch(&file_refs).await;
        for (i, result) in results.into_iter().enumerate() {
            result.map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to hardlink {} to {}: {e}",
                    tree_ops_result.files[i].0.display(),
                    tree_ops_result.files[i].1.display()
                )
            })?;
        }
    }

    // Phase 2: Create all symlinks in one batch.
    if !tree_ops_result.symlinks.is_empty() {
        let symlink_refs: Vec<(&Path, &Path)> = tree_ops_result
            .symlinks
            .iter()
            .map(|(t, l)| (t.as_path(), l.as_path()))
            .collect();
        let results = crate::fs::symlink_batch(&symlink_refs).await;
        for (i, result) in results.into_iter().enumerate() {
            result.map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to create symlink {}: {e}",
                    tree_ops_result.symlinks[i].1.display()
                )
            })?;
        }
    }

    Ok(CloneMethod::Hardlink)
}

/// #clonefile-fallback: emit the GENUINE (perf-critical) clonefile-fallback log
/// at `error!` level. `stage` names which step failed (the syscall itself, or
/// the pre-syscall dst-clear); `err` carries the OS reason (errno).
///
/// This is a genuine failure — NOT the expected dst-non-empty precondition (the
/// O5 race), which is a normal fallback logged at `warn!` inline. A genuine
/// clonefile fallback means EVERY directory materialise takes the ~600×-slower
/// per-file hardlink path instead of the ~1ms whole-tree CoW clone, so it MUST
/// be loud. `error!` survives the workspace `release_max_level_info` strip (a
/// prior `debug!` was compiled OUT of the prod binary → the failure was
/// invisible → a fleet-wide 600× slowdown hid for hours; #clonefile-fallback).
///
/// Not `#[cfg(target_os = "macos")]`-gated (though its only callers are) so the
/// error-level discipline is unit-testable on every platform (a macOS-only test
/// cannot run on the Linux build box).
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn log_clonefile_genuine_failure(src: &Path, dst: &Path, stage: &str, err: &std::io::Error) {
    tracing::error!(
        src = %src.display(),
        dst = %dst.display(),
        stage,
        os_error = %err,
        "clonefile FAILED (genuine, NOT the normal dst-non-empty precondition); fell back to (slow) per-file hardlink — expect ~600× slower directory materialisation on EVERY action. Investigate the OS error (cross-device / non-APFS / permissions)"
    );
}

/// Uses macOS `clonefile(2)` to CoW-clone an entire directory tree in one syscall.
/// Handles pre-existing (empty) destination by removing it first.
///
/// Cache directories are stored with writable permissions (0o755) on macOS,
/// so the clone inherits those permissions directly — no post-clone chmod walk
/// is needed. This works because clonefile creates CoW copies that are
/// independent of the cache, so write-protection is unnecessary.
#[cfg(target_os = "macos")]
fn try_clonefile(src: &Path, dst: &Path) -> Result<(), Error> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    unsafe extern "C" {
        fn clonefile(
            src: *const std::ffi::c_char,
            dst: *const std::ffi::c_char,
            flags: std::ffi::c_int,
        ) -> std::ffi::c_int;
    }

    if !src.exists() {
        return Err(make_err!(
            nativelink_error::Code::InvalidArgument,
            "Source directory does not exist: {}",
            src.display()
        ));
    }

    let src_c = CString::new(src.as_os_str().as_bytes()).map_err(|e| {
        make_err!(
            nativelink_error::Code::Internal,
            "Invalid src path for clonefile: {e}"
        )
    })?;
    let dst_c = CString::new(dst.as_os_str().as_bytes()).map_err(|e| {
        make_err!(
            nativelink_error::Code::Internal,
            "Invalid dst path for clonefile: {e}"
        )
    })?;

    // clonefile(2) requires the destination to not exist.
    // The work directory may have been pre-created (by [B1]) — remove it if empty.
    //
    // O5 concurrency: [C] may have already populated dst (e.g. created
    // `dst/bazel-out/`), making remove_dir fail with ENOTEMPTY. Do NOT
    // attempt to remove a non-empty dst — that would destroy work [C] did.
    // Instead return Err here to trigger the hardlink fallback in the caller.
    // Post-[B1] the work_dir always exists, so clonefile into a genuinely-
    // absent dst is only valid when dst is truly empty.
    if dst.exists() {
        match std::fs::remove_dir(dst) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                // dst already has content from concurrent [C]; clonefile
                // cannot proceed. Signal the caller to use the hardlink path.
                // #clonefile-fallback: this is an EXPECTED precondition-miss (the
                // O5 [C]-pre-populates-work-dir race), a NORMAL fallback — NOT a
                // genuine failure. Log at warn! (visible in release under
                // release_max_level_info) so it can be quantified, without the
                // error!-level alarm reserved for the perf-critical syscall
                // failure below.
                tracing::warn!(
                    src = %src.display(),
                    dst = %dst.display(),
                    "clonefile dst non-empty (concurrent output-dir prep pre-populated it); falling back to (slow) hardlink"
                );
                return Err(make_err!(
                    nativelink_error::Code::Internal,
                    "clonefile {} → {}: dst non-empty (concurrent [C] pre-populated); falling back to hardlink",
                    src.display(),
                    dst.display()
                ));
            }
            Err(e) => {
                // A remove_dir failure that is NOT dst-non-empty is a genuine fs
                // error preventing the whole-tree CoW fast path → the ~600×-slower
                // per-file hardlink fallback. Loud (error!) via the shared helper —
                // a real problem, not the expected O5 precondition.
                log_clonefile_genuine_failure(src, dst, "clonefile dst-clear (remove_dir)", &e);
                return Err(make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to remove existing dst for clonefile {}: {e}",
                    dst.display()
                ));
            }
        }
    }

    // SAFETY: src_c and dst_c are valid CStrings with nul terminators.
    let ret = unsafe { clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        // #clonefile-fallback (user directive: "a failed clonefile on macos
        // should be logged as error"): the GENUINE, PERF-CRITICAL failure — the
        // whole-tree ~1ms CoW clone did not happen. Emit at error! (survives
        // release_max_level_info) via the shared helper.
        log_clonefile_genuine_failure(src, dst, "clonefile(2) syscall", &err);
        return Err(make_err!(
            nativelink_error::Code::Internal,
            "clonefile {} → {}: {err}",
            src.display(),
            dst.display()
        ));
    }

    Ok(())
}


/// Sets a directory tree to read-only recursively.
///
/// Uses `spawn_blocking` with synchronous `std::fs` for performance.
pub async fn set_readonly_recursive(dir: &Path) -> Result<(), Error> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || set_readonly_recursive_sync(&dir))
        .await
        .map_err(|e| make_err!(nativelink_error::Code::Internal, "spawn_blocking join error: {e}"))?
}

fn set_readonly_recursive_sync(path: &Path) -> Result<(), Error> {
    let metadata = std::fs::symlink_metadata(path).map_err(|e| {
        make_err!(
            nativelink_error::Code::Internal,
            "Failed to get metadata for {}: {e}",
            path.display()
        )
    })?;

    if metadata.is_symlink() {
        return Ok(());
    }

    if metadata.is_dir() {
        for entry in std::fs::read_dir(path).map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to read directory {}: {e}",
                path.display()
            )
        })? {
            let entry = entry.map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to read entry in {}: {e}",
                    path.display()
                )
            })?;
            set_readonly_recursive_sync(&entry.path())?;
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = metadata.permissions();
        let mode = perms.mode() & !0o222;
        perms.set_mode(mode);
        std::fs::set_permissions(path, perms).map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to set permissions for {}: {e}",
                path.display()
            )
        })?;
    }

    #[cfg(windows)]
    {
        let mut perms = metadata.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(path, perms).map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to set permissions for {}: {e}",
                path.display()
            )
        })?;
    }

    Ok(())
}

/// Sets a directory tree to read-only and calculates total size in one pass.
///
/// Uses `spawn_blocking` with synchronous `std::fs` for performance.
/// Combines two walks into one to halve I/O for large trees.
pub async fn set_readonly_and_calculate_size(dir: &Path) -> Result<u64, Error> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || set_readonly_and_size_sync(&dir))
        .await
        .map_err(|e| make_err!(nativelink_error::Code::Internal, "spawn_blocking join error: {e}"))?
}

fn set_readonly_and_size_sync(path: &Path) -> Result<u64, Error> {
    let metadata = std::fs::symlink_metadata(path).map_err(|e| {
        make_err!(
            nativelink_error::Code::Internal,
            "Failed to get metadata for {}: {e}",
            path.display()
        )
    })?;

    if metadata.is_symlink() {
        return Ok(0);
    }

    if metadata.is_dir() {
        let mut total = 0u64;
        for entry in std::fs::read_dir(path).map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to read directory {}: {e}",
                path.display()
            )
        })? {
            let entry = entry.map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to read entry in {}: {e}",
                    path.display()
                )
            })?;
            total += set_readonly_and_size_sync(&entry.path())?;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = metadata.permissions();
            let mode = perms.mode() & !0o222;
            perms.set_mode(mode);
            std::fs::set_permissions(path, perms).map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to set permissions for {}: {e}",
                    path.display()
                )
            })?;
        }

        #[cfg(windows)]
        {
            let mut perms = metadata.permissions();
            perms.set_readonly(true);
            std::fs::set_permissions(path, perms).map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to set permissions for {}: {e}",
                    path.display()
                )
            })?;
        }

        Ok(total)
    } else if metadata.is_file() {
        let size = metadata.len();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let current_mode = metadata.permissions().mode() & 0o777;
            let readonly_mode = current_mode & !0o222;
            if current_mode != readonly_mode {
                let mut perms = metadata.permissions();
                perms.set_mode(readonly_mode);
                std::fs::set_permissions(path, perms).map_err(|e| {
                    make_err!(
                        nativelink_error::Code::Internal,
                        "Failed to set permissions for {}: {e}",
                        path.display()
                    )
                })?;
            }
        }

        #[cfg(windows)]
        {
            let mut perms = metadata.permissions();
            perms.set_readonly(true);
            std::fs::set_permissions(path, perms).map_err(|e| {
                make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to set permissions for {}: {e}",
                    path.display()
                )
            })?;
        }

        Ok(size)
    } else {
        Ok(0)
    }
}

/// Calculates the total size of a directory tree in bytes.
///
/// Uses `spawn_blocking` with synchronous `std::fs` for performance.
pub async fn calculate_directory_size(dir: &Path) -> Result<u64, Error> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || calculate_size_sync(&dir))
        .await
        .map_err(|e| make_err!(nativelink_error::Code::Internal, "spawn_blocking join error: {e}"))?
}

fn calculate_size_sync(path: &Path) -> Result<u64, Error> {
    let metadata = std::fs::symlink_metadata(path).map_err(|e| {
        make_err!(
            nativelink_error::Code::Internal,
            "Failed to get metadata for {}: {e}",
            path.display()
        )
    })?;

    if metadata.is_symlink() {
        return Ok(0);
    }

    if metadata.is_file() {
        return Ok(metadata.len());
    }

    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0u64;
    for entry in std::fs::read_dir(path).map_err(|e| {
        make_err!(
            nativelink_error::Code::Internal,
            "Failed to read directory {}: {e}",
            path.display()
        )
    })? {
        let entry = entry.map_err(|e| {
            make_err!(
                nativelink_error::Code::Internal,
                "Failed to read entry in {}: {e}",
                path.display()
            )
        })?;
        total += calculate_size_sync(&entry.path())?;
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::PathBuf;

    use nativelink_error::ResultExt;
    use nativelink_macro::nativelink_test;
    use tempfile::TempDir;
    use tokio::fs;

    use super::*;

    async fn create_test_directory() -> Result<(TempDir, PathBuf), Error> {
        let temp_dir = TempDir::new().err_tip(|| "Failed to create temp directory")?;
        let test_dir = temp_dir.path().join("test_src");

        std::fs::create_dir(&test_dir).err_tip(|| "create test_src")?;

        let file1 = test_dir.join("file1.txt");
        let mut f = std::fs::File::create(&file1).err_tip(|| "create file1")?;
        f.write_all(b"Hello, World!").err_tip(|| "write file1")?;
        drop(f);

        let subdir = test_dir.join("subdir");
        std::fs::create_dir(&subdir).err_tip(|| "create subdir")?;

        let file2 = subdir.join("file2.txt");
        let mut f = std::fs::File::create(&file2).err_tip(|| "create file2")?;
        f.write_all(b"Nested file").err_tip(|| "write file2")?;
        drop(f);

        Ok((temp_dir, test_dir))
    }

    #[nativelink_test("crate")]
    async fn test_hardlink_directory_tree() -> Result<(), Error> {
        let (temp_dir, src_dir) = create_test_directory().await?;
        let dst_dir = temp_dir.path().join("test_dst");

        // Hardlink the directory
        let method = hardlink_directory_tree(&src_dir, &dst_dir).await?;
        // On macOS this will be Clonefile, on Linux it will be Hardlink
        assert!(method == CloneMethod::Clonefile || method == CloneMethod::Hardlink);

        // Verify structure
        assert!(dst_dir.join("file1.txt").exists());
        assert!(dst_dir.join("subdir").is_dir());
        assert!(dst_dir.join("subdir/file2.txt").exists());

        // Verify contents
        let content1 = fs::read_to_string(dst_dir.join("file1.txt")).await?;
        assert_eq!(content1, "Hello, World!");

        let content2 = fs::read_to_string(dst_dir.join("subdir/file2.txt")).await?;
        assert_eq!(content2, "Nested file");

        // Verify files are hardlinked (same inode on Unix)
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let src_meta = fs::metadata(src_dir.join("file1.txt")).await?;
            let dst_meta = fs::metadata(dst_dir.join("file1.txt")).await?;
            assert_eq!(
                src_meta.ino(),
                dst_meta.ino(),
                "Files should have same inode (hardlinked)"
            );
        }

        Ok(())
    }

    #[nativelink_test("crate")]
    async fn test_set_readonly_recursive() -> Result<(), Error> {
        let (_temp_dir, test_dir) = create_test_directory().await?;

        set_readonly_recursive(&test_dir).await?;

        // Verify files are read-only
        let metadata = fs::metadata(test_dir.join("file1.txt")).await?;
        assert!(metadata.permissions().readonly());

        let metadata = fs::metadata(test_dir.join("subdir/file2.txt")).await?;
        assert!(metadata.permissions().readonly());

        Ok(())
    }

    #[nativelink_test("crate")]
    async fn test_calculate_directory_size() -> Result<(), Error> {
        let (_temp_dir, test_dir) = create_test_directory().await?;

        let size = calculate_directory_size(&test_dir).await?;

        // "Hello, World!" = 13 bytes
        // "Nested file" = 11 bytes
        // Total = 24 bytes
        assert_eq!(size, 24);

        Ok(())
    }

    #[nativelink_test("crate")]
    async fn test_hardlink_nonexistent_source() {
        let temp_dir = TempDir::new().unwrap();
        let src = temp_dir.path().join("nonexistent");
        let dst = temp_dir.path().join("dest");

        let result = hardlink_directory_tree(&src, &dst).await;
        assert!(result.is_err());
    }

    /// Regression test for the DirectoryCache EEXIST regression introduced by O5
    /// (commit 333ce15e). In `!is_direct_use` mode, output-dir prep [C] runs
    /// concurrently with hardlink [B2]. [C] calls `prepare_output_directory` which
    /// creates `work_dir/bazel-out/...` BEFORE `hardlink_directory_tree`'s
    /// `collect_tree_ops_recursive` reaches the same directory. Without tolerance the
    /// bare `create_dir` fails with AlreadyExists → cache hit becomes a miss.
    ///
    /// The existing `test_hardlink_into_existing_destination` pre-creates only the
    /// *root* dst dir (not a subdirectory), so `collect_tree_ops_recursive` never
    /// races with a pre-existing sub-dir — it would NOT catch this bug.
    #[nativelink_test("crate")]
    async fn test_hardlink_into_dst_with_pre_existing_subdir() -> Result<(), Error> {
        let temp_dir = TempDir::new().err_tip(|| "Failed to create temp dir")?;

        // Build a src tree that contains a `bazel-out/` subdirectory with a file —
        // this simulates a cached action tree.
        let src_dir = temp_dir.path().join("cache_src");
        std::fs::create_dir(&src_dir).err_tip(|| "create cache_src")?;
        let bazel_out = src_dir.join("bazel-out");
        std::fs::create_dir(&bazel_out).err_tip(|| "create bazel-out in src")?;
        {
            let mut f = std::fs::File::create(bazel_out.join("artifact.txt"))
                .err_tip(|| "create artifact.txt")?;
            std::io::Write::write_all(&mut f, b"build artifact")
                .err_tip(|| "write artifact.txt")?;
        }

        // Pre-create the dst root AND the dst/bazel-out subdirectory — simulating
        // [C] (prepare_output_directory) winning the race against [B2]'s hardlink.
        let dst_dir = temp_dir.path().join("work_dir");
        std::fs::create_dir(&dst_dir).err_tip(|| "create work_dir")?;
        // [C] pre-creates the output subdir:
        std::fs::create_dir(dst_dir.join("bazel-out"))
            .err_tip(|| "pre-create bazel-out in dst (simulates [C])")?;

        // Must succeed — EEXIST on a pre-existing directory must be tolerated.
        hardlink_directory_tree(&src_dir, &dst_dir).await.err_tip(|| {
            "hardlink into dst with a pre-existing bazel-out subdir must tolerate EEXIST (concurrent [C] race)"
        })?;

        // Verify the file was materialized correctly.
        assert!(
            dst_dir.join("bazel-out").is_dir(),
            "bazel-out must be a directory"
        );
        let content = tokio::fs::read_to_string(dst_dir.join("bazel-out/artifact.txt"))
            .await
            .err_tip(|| "read artifact.txt in dst")?;
        assert_eq!(content, "build artifact", "artifact content mismatch");

        // Verify hardlink (same inode on Unix).
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let src_meta = std::fs::metadata(src_dir.join("bazel-out/artifact.txt"))
                .err_tip(|| "stat src artifact")?;
            let dst_meta = std::fs::metadata(dst_dir.join("bazel-out/artifact.txt"))
                .err_tip(|| "stat dst artifact")?;
            assert_eq!(
                src_meta.ino(),
                dst_meta.ino(),
                "artifact.txt must be hardlinked (same inode)"
            );
        }

        Ok(())
    }

    /// Coverage for the non-directory-conflict branch: if `[C]` (or stale debris)
    /// leaves a FILE where the cached tree expects a DIRECTORY, the EEXIST tolerance
    /// must NOT silently proceed — it must reject with `Code::AlreadyExists`. Guards
    /// the `!m.is_dir()` branch in `collect_tree_ops_recursive` (a future accidental
    /// removal of that guard would otherwise go undetected).
    #[nativelink_test("crate")]
    async fn test_hardlink_fails_when_dst_has_file_where_dir_expected() -> Result<(), Error> {
        let temp_dir = TempDir::new().err_tip(|| "Failed to create temp dir")?;

        // src tree: `bazel-out/` is a DIRECTORY (with a file inside).
        let src_dir = temp_dir.path().join("cache_src");
        std::fs::create_dir(&src_dir).err_tip(|| "create cache_src")?;
        let bazel_out = src_dir.join("bazel-out");
        std::fs::create_dir(&bazel_out).err_tip(|| "create bazel-out in src")?;
        {
            let mut f = std::fs::File::create(bazel_out.join("artifact.txt"))
                .err_tip(|| "create artifact.txt")?;
            std::io::Write::write_all(&mut f, b"build artifact").err_tip(|| "write artifact.txt")?;
        }

        // dst: a FILE named `bazel-out` where the cached tree expects a directory —
        // debris from a prior failed action, or a hypothetical future [C] mistake.
        let dst_dir = temp_dir.path().join("work_dir");
        std::fs::create_dir(&dst_dir).err_tip(|| "create work_dir")?;
        {
            let mut f = std::fs::File::create(dst_dir.join("bazel-out"))
                .err_tip(|| "create file at dst/bazel-out")?;
            std::io::Write::write_all(&mut f, b"not a dir").err_tip(|| "write dst/bazel-out file")?;
        }

        // Must FAIL: a non-directory at a directory path is a genuine conflict, NOT a
        // tolerable concurrent-[C] pre-create.
        let err = hardlink_directory_tree(&src_dir, &dst_dir).await.expect_err(
            "hardlink into a dst with a FILE where a directory is expected must error, \
             not silently proceed (the !is_dir() guard must fire)",
        );
        assert_eq!(
            err.code,
            nativelink_error::Code::AlreadyExists,
            "non-directory conflict must surface as Code::AlreadyExists (the !is_dir() guard); got: {err:?}"
        );
        assert!(
            format!("{err:?}").contains("non-directory"),
            "error must name the non-directory conflict; got: {err:?}"
        );

        Ok(())
    }

    #[nativelink_test("crate")]
    async fn test_hardlink_into_existing_destination() -> Result<(), Error> {
        let (temp_dir, src_dir) = create_test_directory().await?;
        let dst_dir = temp_dir.path().join("existing");

        // Pre-create the destination directory (simulates work_directory already existing)
        fs::create_dir(&dst_dir).await?;

        // Should succeed — hardlink contents into existing directory
        hardlink_directory_tree(&src_dir, &dst_dir).await?;

        // Verify structure
        assert!(dst_dir.join("file1.txt").exists());
        assert!(dst_dir.join("subdir").is_dir());
        assert!(dst_dir.join("subdir/file2.txt").exists());

        // Verify contents
        let content1 = fs::read_to_string(dst_dir.join("file1.txt")).await?;
        assert_eq!(content1, "Hello, World!");

        Ok(())
    }

    /// #clonefile-fallback (user directive: "a failed clonefile on macos should
    /// be logged as error"): the GENUINE clonefile fallback log MUST emit at
    /// `error!` level so it survives the workspace `release_max_level_info` strip
    /// in the prod binary. A prior `debug!` was compiled OUT of release → the
    /// failure was invisible → a fleet-wide ~600× slowdown hid for hours.
    ///
    /// The genuine syscall-failure path (`clonefile(2)` returning non-zero) is
    /// `#[cfg(target_os = "macos")]` and needs a REAL cross-device / non-APFS
    /// failure to trigger — not deterministically forcible in a unit test, and
    /// not even compiled on the Linux build box. So the error-level discipline is
    /// factored into the non-gated `log_clonefile_genuine_failure` helper (the
    /// single emit site both macOS callers use) and asserted directly here on
    /// every platform.
    ///
    /// Filtering on the ` ERROR ` level prefix is load-bearing: `tracing-test`
    /// captures all levels in test builds, so a bare substring match would still
    /// pass if the emit regressed to `warn!`/`debug!`. The level filter is what
    /// makes the mutation step bite.
    ///
    /// Mutation: change `tracing::error!` → `tracing::debug!` (or `warn!`) in
    /// `log_clonefile_genuine_failure` → this test red-fails with the bespoke
    /// message (a `debug!`/`warn!` line does not carry the ` ERROR ` prefix).
    #[nativelink_test("crate")]
    async fn clonefile_genuine_failure_logs_at_error_level() {
        let err = std::io::Error::from_raw_os_error(18); // EXDEV (cross-device)
        log_clonefile_genuine_failure(
            Path::new("/cache/src-tree"),
            Path::new("/work/dst-tree"),
            "clonefile(2) syscall",
            &err,
        );
        logs_assert(|lines: &[&str]| {
            let n = lines
                .iter()
                .filter(|l| {
                    l.contains(" ERROR ")
                        && l.contains("clonefile FAILED (genuine")
                        && l.contains("600× slower")
                })
                .count();
            if n == 0 {
                Err("clonefile genuine-fallback must emit at ERROR level — a \
                     regression to debug!/warn! would be stripped/quieted and the \
                     perf-critical fallback would be invisible in the prod journal \
                     (release_max_level_info)"
                    .to_string())
            } else {
                Ok(())
            }
        });
    }
}
