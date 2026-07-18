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

//! FL-1383 portable rustc-incremental — WORKER-side FIXED_PREFIX provisioning,
//! §12 startup asserts, and `O_EXCL`/`O_NOFOLLOW` filesystem primitives
//! (design `docs/portable-rustc-incremental-v4.md`, §4/§9/§12).
//!
//! This is the chunk-2a FOUNDATION. It is INERT on the execution path: when the
//! worker gate ([`nativelink_config::cas_server::LocalWorkerConfig::portable_incr`])
//! is disabled, nothing here runs; when it is enabled, this module ONLY
//! provisions the machine-local `<FIXED_PREFIX>` root, asserts the §12
//! host-provisioning invariants, and reports whether the feature is effectively
//! enabled. It does NOT rewire `make_action_directory`, chdir, wipe, or the
//! seed lease — that is chunk 2b (TODO(#FL-1383)).
//!
//! The `O_EXCL` directory creation + `O_NOFOLLOW` open helpers are provided here
//! for the chunk-2b materialize / output-relocation / EXDEV copy-fallback paths
//! (design §9) and are exercised in 2a by the startup EXDEV probe and the tests.
//!
//! DURABILITY: no `fsync`/`O_SYNC`/sync-write primitive appears here (CLAUDE.md
//! hard rule). All filesystem syscalls are BLOCKING and MUST run inside
//! `spawn_blocking`; [`provision_and_assert`] does exactly that so the tokio
//! worker is never blocked.

use core::ffi::c_int;
use std::ffi::CString;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use tracing::info;

/// The outcome of a successful worker-side portable-incr provisioning: the
/// FIXED_PREFIX root that passed every §12 assert. Chunk 2b threads this into
/// the execution path; chunk 2a only logs it.
#[derive(Debug, Clone)]
pub struct PortableIncrProvision {
    /// The provisioned, asserted machine-local FIXED_PREFIX root.
    pub fixed_prefix: PathBuf,
}

/// Provision FIXED_PREFIX and run the §12 startup asserts, returning the
/// effective-enabled state of the worker-side portable-incr feature.
///
/// Returns:
/// - `None` when `enabled` is `false` — the INERT state (no provisioning, no
///   assert, no filesystem touch);
/// - `None` when `enabled` is `true` but any precondition/assert FAILS — the
///   fail-loud-DISABLE state: a clear error is logged and the feature is left
///   off. The worker is NOT panicked and startup continues.
/// - `Some(PortableIncrProvision)` when `enabled` is `true` and every §12
///   assert passes — the feature is effectively ENABLED (but still unused until
///   chunk 2b wires the execution path).
///
/// `execroot_probe_dir` is the worker's execroot volume anchor (its
/// `work_directory`); the EXDEV probe hardlinks a temp file from FIXED_PREFIX
/// into this dir to prove they co-locate on one volume (design §9).
///
/// All blocking filesystem work runs inside a single `spawn_blocking`.
pub async fn provision_and_assert(
    enabled: bool,
    fixed_prefix: Option<&Path>,
    sysroot_path: Option<&Path>,
    execroot_probe_dir: &Path,
) -> Option<PortableIncrProvision> {
    // INERT: the flag-off path touches nothing.
    if !enabled {
        return None;
    }

    let fixed_prefix = fixed_prefix.map(Path::to_path_buf);
    let sysroot_path = sysroot_path.map(Path::to_path_buf);
    let execroot_probe_dir = execroot_probe_dir.to_path_buf();

    let joined = tokio::task::spawn_blocking(move || {
        provision_and_assert_blocking(
            fixed_prefix.as_deref(),
            sysroot_path.as_deref(),
            &execroot_probe_dir,
        )
    })
    .await;

    match joined {
        Ok(Ok(provision)) => {
            info!(
                fixed_prefix = %provision.fixed_prefix.display(),
                "portable_incr: FIXED_PREFIX provisioned and all §12 startup asserts passed \
                 — feature ENABLED (execution-path rewire is chunk 2b; still unused)"
            );
            Some(provision)
        }
        Ok(Err(err)) => {
            // Fail-loud-DISABLE: log the specific assert failure; do NOT panic.
            tracing::error!(
                ?err,
                "portable_incr: startup assert FAILED — feature left DISABLED; worker continues"
            );
            None
        }
        Err(join_err) => {
            tracing::error!(
                ?join_err,
                "portable_incr: provisioning task panicked — feature left DISABLED; worker continues"
            );
            None
        }
    }
}

/// The synchronous body of [`provision_and_assert`]. Runs the ordered §12
/// asserts, each logged on success. Any `Err` here means fail-loud-DISABLE.
fn provision_and_assert_blocking(
    fixed_prefix: Option<&Path>,
    sysroot_path: Option<&Path>,
    execroot_probe_dir: &Path,
) -> Result<PortableIncrProvision, Error> {
    let fixed_prefix = fixed_prefix.ok_or_else(|| {
        make_input_err!(
            "portable_incr.enabled=true but portable_incr_fixed_prefix is unset — cannot provision"
        )
    })?;
    let sysroot_path = sysroot_path.ok_or_else(|| {
        make_input_err!(
            "portable_incr.enabled=true but portable_incr_sysroot_path is unset — cannot assert sysroot"
        )
    })?;

    // §12 (a): FIXED_PREFIX exists, owned by the worker uid, mode 0755,
    // machine-local (not a shared network FS).
    provision_fixed_prefix(fixed_prefix).err_tip(|| "§12 assert (a) FIXED_PREFIX provisioning")?;
    info!(
        fixed_prefix = %fixed_prefix.display(),
        "portable_incr §12(a): FIXED_PREFIX owned-by-worker-uid + mode 0755 + machine-local — OK"
    );

    // §12 (b): the rustc sysroot absolute path is byte-identical (not reached
    // via an output_base symlink).
    assert_sysroot_identity(sysroot_path).err_tip(|| "§12 assert (b) sysroot byte-identical")?;
    info!(
        sysroot = %sysroot_path.display(),
        "portable_incr §12(b): sysroot is absolute and canonicalizes to itself (no symlink) — OK"
    );

    // §12 (c): link()-to-execroot-volume works (FIXED_PREFIX co-locates on the
    // execroot volume); EXDEV ⇒ refuse (copy-fallback is chunk 2b).
    assert_link_to_execroot(fixed_prefix, execroot_probe_dir)
        .err_tip(|| "§12 assert (c) link()-to-execroot EXDEV probe")?;
    info!(
        fixed_prefix = %fixed_prefix.display(),
        execroot = %execroot_probe_dir.display(),
        "portable_incr §12(c): link() from FIXED_PREFIX into the execroot volume works — OK"
    );

    Ok(PortableIncrProvision {
        fixed_prefix: fixed_prefix.to_path_buf(),
    })
}

/// Ensure FIXED_PREFIX exists (creating it exclusively + 0755 if absent), then
/// assert §12 (a): it is a real directory owned by the worker uid, mode exactly
/// 0755 (NOT world-writable), on a machine-local filesystem.
fn provision_fixed_prefix(path: &Path) -> Result<(), Error> {
    match mkdir_exclusive_raw(path, 0o755) {
        Ok(()) => {
            // mkdir's mode is umask-masked; set 0755 exactly on the dir we just
            // created (no symlink can exist at a path mkdir just created).
            chmod_raw(path, 0o755)
                .map_err(|e| make_err!(Code::Internal, "chmod 0755 {}: {e}", path.display()))?;
        }
        Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {
            // Pre-existing; verified (uid/mode/local) below — we never mutate a
            // dir we did not create.
        }
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
            // Parent missing: create the ancestry (non-exclusive) then retry the
            // exclusive leaf create so a concurrent racer still loses cleanly.
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|pe| {
                    make_err!(
                        Code::Internal,
                        "create FIXED_PREFIX parent {}: {pe}",
                        parent.display()
                    )
                })?;
            }
            match mkdir_exclusive_raw(path, 0o755) {
                Ok(()) => chmod_raw(path, 0o755)
                    .map_err(|e| make_err!(Code::Internal, "chmod 0755 {}: {e}", path.display()))?,
                Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {}
                Err(e) => {
                    return Err(make_err!(
                        Code::Internal,
                        "mkdir FIXED_PREFIX {}: {e}",
                        path.display()
                    ));
                }
            }
        }
        Err(e) => {
            return Err(make_err!(
                Code::Internal,
                "mkdir FIXED_PREFIX {}: {e}",
                path.display()
            ));
        }
    }

    assert_fixed_prefix_ownership(path)
}

/// §12 (a) verification: `lstat` (never follow a final-component symlink) the
/// FIXED_PREFIX and require: real directory, `uid == geteuid`, mode `& 0o777 ==
/// 0o755`, and a machine-local filesystem.
fn assert_fixed_prefix_ownership(path: &Path) -> Result<(), Error> {
    let c_path = path_to_cstring(path)?;

    // SAFETY: `st` is a correctly-sized zeroed `libc::stat`; `c_path` is a valid
    // NUL-terminated C string. `lstat` does not follow a final symlink.
    let mut st: libc::stat = unsafe { core::mem::zeroed() };
    let ret = unsafe { libc::lstat(c_path.as_ptr(), &raw mut st) };
    if ret != 0 {
        return Err(make_err!(
            Code::FailedPrecondition,
            "lstat FIXED_PREFIX {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }

    // `st_mode`, `S_IFMT`, `S_IFDIR` are `mode_t`, which is `u16` on macOS and
    // `u32` on Linux; widen everything to `u32` via `From` so the comparison is
    // portable (the widening is a no-op on Linux, load-bearing on macOS).
    let mode = u32::from(st.st_mode);
    let s_ifmt = u32::from(libc::S_IFMT);
    let s_ifdir = u32::from(libc::S_IFDIR);
    if mode & s_ifmt != s_ifdir {
        return Err(make_err!(
            Code::FailedPrecondition,
            "FIXED_PREFIX {} is not a directory (st_mode {:#o}) — refusing to enable portable_incr",
            path.display(),
            mode
        ));
    }

    // SAFETY: `geteuid` is always successful on POSIX.
    let euid = unsafe { libc::geteuid() };
    if st.st_uid != euid {
        return Err(make_err!(
            Code::FailedPrecondition,
            "FIXED_PREFIX {} uid {} != worker uid {} — refusing to enable portable_incr",
            path.display(),
            st.st_uid,
            euid
        ));
    }

    let perm = mode & 0o777;
    if perm != 0o755 {
        return Err(make_err!(
            Code::FailedPrecondition,
            "FIXED_PREFIX {} mode {:#o} != 0755 (world-writable/shared perms rejected) — \
             refusing to enable portable_incr",
            path.display(),
            perm
        ));
    }

    if !is_machine_local(&c_path)? {
        return Err(make_err!(
            Code::FailedPrecondition,
            "FIXED_PREFIX {} is on a network filesystem, not machine-local — refusing to enable \
             portable_incr (design §4: FIXED_PREFIX must be machine-local)",
            path.display()
        ));
    }

    Ok(())
}

/// §12 (b): the sysroot absolute path is byte-identical (the local proxy for the
/// fleet host-provisioning invariant): it is absolute AND canonicalizes to
/// itself — i.e. it is NOT reached through an `output_base` symlink. rustc
/// realpaths the sysroot, so a symlink component diverges the path cross-machine
/// and defeats reuse (design §2).
fn assert_sysroot_identity(sysroot: &Path) -> Result<(), Error> {
    if !sysroot.is_absolute() {
        return Err(make_err!(
            Code::FailedPrecondition,
            "rustc sysroot {} is not an absolute path — refusing to enable portable_incr",
            sysroot.display()
        ));
    }
    let real = std::fs::canonicalize(sysroot).map_err(|e| {
        make_err!(
            Code::FailedPrecondition,
            "rustc sysroot {} could not be canonicalized (missing?): {e} — refusing to enable \
             portable_incr",
            sysroot.display()
        )
    })?;
    if real.as_path() != sysroot {
        return Err(make_err!(
            Code::FailedPrecondition,
            "rustc sysroot {} is reached via a symlink (realpath {}); rustc realpaths → \
             cross-machine path divergence. Provision an execroot-relative REAL sysroot — \
             refusing to enable portable_incr",
            sysroot.display(),
            real.display()
        ));
    }
    Ok(())
}

/// §12 (c): prove `link()` works from FIXED_PREFIX into the execroot volume by
/// creating a temp probe file under FIXED_PREFIX and hardlinking it into
/// `execroot_dir`. `EXDEV` means FIXED_PREFIX and the execroot are on SEPARATE
/// volumes (e.g. `/Volumes/CrowAgent` vs the Data volume, design §2/§9): in
/// chunk 2a we REFUSE (copy-fallback is chunk 2b, TODO(#FL-1383)). Both probe
/// files are removed regardless of outcome.
fn assert_link_to_execroot(fixed_prefix: &Path, execroot_dir: &Path) -> Result<(), Error> {
    let pid = std::process::id();
    let probe = fixed_prefix.join(format!(".fl1383_exdev_probe.{pid}"));
    let link = execroot_dir.join(format!(".fl1383_exdev_link.{pid}"));

    // Clear any stale probes from a prior aborted run (best-effort).
    drop(std::fs::remove_file(&probe));
    drop(std::fs::remove_file(&link));

    // Create the probe exclusively + no-follow, then close it (the inode
    // persists for the link probe).
    let probe_fd = create_file_exclusive_no_follow(&probe, 0o600)
        .err_tip(|| "creating EXDEV probe file under FIXED_PREFIX")?;
    drop(probe_fd);

    let link_result = link_raw(&probe, &link);

    // Always clean up both probe artifacts.
    drop(std::fs::remove_file(&link));
    drop(std::fs::remove_file(&probe));

    match link_result {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::EXDEV) => Err(make_err!(
            Code::FailedPrecondition,
            "FIXED_PREFIX {} and the execroot {} are on SEPARATE volumes (link() → EXDEV); \
             portable_incr requires them co-located on one volume (design §9). Copy-fallback is \
             chunk 2b — refusing to enable portable_incr",
            fixed_prefix.display(),
            execroot_dir.display()
        )),
        Err(e) => Err(make_err!(
            Code::Internal,
            "EXDEV link probe from {} to {} failed unexpectedly: {e}",
            probe.display(),
            link.display()
        )),
    }
}

// ---------------------------------------------------------------------------
// O_EXCL / O_NOFOLLOW primitives (public — chunk 2b materialize/relocation/copy
// paths). All are BLOCKING; callers on an async path MUST use `spawn_blocking`.
// ---------------------------------------------------------------------------

/// Create `path` as a NEW directory, failing with an already-exists error if it
/// already exists (mkdir(2)'s `EEXIST` exclusivity is the directory analogue of
/// `O_EXCL`). The mode is `0o755` before umask; callers needing an exact mode
/// must chmod afterwards. BLOCKING.
pub fn create_dir_exclusive(path: &Path) -> Result<(), Error> {
    mkdir_exclusive_raw(path, 0o755).map_err(|e| {
        make_err!(
            Code::Internal,
            "exclusive create_dir {}: {e}",
            path.display()
        )
    })
}

/// Open `path` with `O_NOFOLLOW` (and `O_CLOEXEC`) so a symlink at the final
/// component is REFUSED (`ELOOP`) rather than silently traversed — a symlink-swap
/// defense for the chunk-2b materialize/relocation paths (design §9). `flags`
/// are OR'd with `O_NOFOLLOW | O_CLOEXEC`. BLOCKING.
pub fn open_no_follow(path: &Path, flags: c_int) -> Result<OwnedFd, Error> {
    let c_path = path_to_cstring(path)?;
    // SAFETY: valid NUL-terminated path; standard open(2). O_NOFOLLOW refuses a
    // final-component symlink instead of following it.
    let fd = unsafe { libc::open(c_path.as_ptr(), flags | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(make_err!(
            Code::Internal,
            "open(O_NOFOLLOW) {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: `fd` is a freshly-opened, owned, valid file descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Create `path` as a NEW regular file with `O_CREAT | O_EXCL | O_NOFOLLOW |
/// O_CLOEXEC | O_WRONLY`: fails `EEXIST` if the path exists (including as a
/// symlink) and never follows/creates through a symlink. `mode` is the create
/// mode (umask-masked). Returns the owned fd. BLOCKING.
pub fn create_file_exclusive_no_follow(path: &Path, mode: u32) -> Result<OwnedFd, Error> {
    let c_path = path_to_cstring(path)?;
    let flags =
        libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_WRONLY;
    // SAFETY: valid NUL-terminated path; open(2) is variadic in `mode` for
    // O_CREAT. `mode: u32` is ABI-compatible with the `c_uint` open expects.
    let fd = unsafe { libc::open(c_path.as_ptr(), flags, mode) };
    if fd < 0 {
        return Err(make_err!(
            Code::Internal,
            "open(O_CREAT|O_EXCL|O_NOFOLLOW) {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: `fd` is a freshly-opened, owned, valid file descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

// ---------------------------------------------------------------------------
// Raw syscall helpers (internal; io::Error-returning so callers can match errno)
// ---------------------------------------------------------------------------

fn mkdir_exclusive_raw(path: &Path, mode: libc::mode_t) -> std::io::Result<()> {
    let c_path = path_to_cstring_io(path)?;
    // SAFETY: valid NUL-terminated path; mkdir(2) fails EEXIST if present.
    let ret = unsafe { libc::mkdir(c_path.as_ptr(), mode) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn chmod_raw(path: &Path, mode: libc::mode_t) -> std::io::Result<()> {
    let c_path = path_to_cstring_io(path)?;
    // SAFETY: valid NUL-terminated path; chmod on a just-created real directory.
    let ret = unsafe { libc::chmod(c_path.as_ptr(), mode) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn link_raw(src: &Path, dst: &Path) -> std::io::Result<()> {
    let c_src = path_to_cstring_io(src)?;
    let c_dst = path_to_cstring_io(dst)?;
    // SAFETY: two valid NUL-terminated paths; link(2). EXDEV on cross-device.
    let ret = unsafe { libc::link(c_src.as_ptr(), c_dst.as_ptr()) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Whether the filesystem containing `path` is machine-local (not a network FS).
#[cfg(target_os = "macos")]
fn is_machine_local(path: &std::ffi::CStr) -> Result<bool, Error> {
    // SAFETY: zeroed `libc::statfs`; valid NUL-terminated path.
    let mut buf: libc::statfs = unsafe { core::mem::zeroed() };
    let ret = unsafe { libc::statfs(path.as_ptr(), &raw mut buf) };
    if ret != 0 {
        return Err(make_err!(
            Code::FailedPrecondition,
            "statfs machine-local probe failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // macOS: MNT_LOCAL is set for filesystems on locally-attached devices.
    Ok(u64::from(buf.f_flags) & (libc::MNT_LOCAL as u64) != 0)
}

/// Whether the filesystem containing `path` is machine-local (not a network FS).
#[cfg(target_os = "linux")]
fn is_machine_local(path: &std::ffi::CStr) -> Result<bool, Error> {
    // SAFETY: zeroed `libc::statfs`; valid NUL-terminated path.
    let mut buf: libc::statfs = unsafe { core::mem::zeroed() };
    let ret = unsafe { libc::statfs(path.as_ptr(), &raw mut buf) };
    if ret != 0 {
        return Err(make_err!(
            Code::FailedPrecondition,
            "statfs machine-local probe failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // Linux: reject the known NETWORK filesystem magics; everything else
    // (tmpfs/ext4/xfs/zfs/apfs-via-fuse-local/…) is treated as machine-local.
    const NFS_SUPER_MAGIC: i64 = 0x6969;
    const SMB_SUPER_MAGIC: i64 = 0x517B;
    const CIFS_MAGIC_NUMBER: i64 = 0xFF53_4D42;
    const SMB2_MAGIC_NUMBER: i64 = 0xFE53_4D42;
    let f_type = i64::from(buf.f_type);
    Ok(!matches!(
        f_type,
        NFS_SUPER_MAGIC | SMB_SUPER_MAGIC | CIFS_MAGIC_NUMBER | SMB2_MAGIC_NUMBER
    ))
}

fn path_to_cstring(path: &Path) -> Result<CString, Error> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|e| make_input_err!("path {} contains an interior NUL byte: {e}", path.display()))
}

fn path_to_cstring_io(path: &Path) -> std::io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
}
