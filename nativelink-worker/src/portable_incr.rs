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

//! FL-1383 portable rustc-incremental — WORKER-side FIXED_PREFIX provisioning +
//! §12 startup asserts (chunk 2a) AND the byte-identical execroot machinery
//! (chunk 2b) (design `docs/portable-rustc-incremental-v4.md`, §4/§5/§7/§9/§12).
//!
//! - **Chunk 2a (foundation):** [`provision_and_assert`] provisions the
//!   machine-local `<FIXED_PREFIX>` root, asserts the §12 host-provisioning
//!   invariants, and reports the effective-enabled state; the `O_EXCL` /
//!   `O_NOFOLLOW` primitives back the materialize / relocation / EXDEV paths.
//! - **Chunk 2b (execroot core):** [`PortableIncrContext`] +
//!   [`PortableExecroot`] pick the byte-identical execroot
//!   `<FIXED_PREFIX>/<targetkey>` for an enabled+allowlisted action (§4), hold
//!   the machine-local §5 ownership lease (owner warm / contender isolated),
//!   wipe-preserving-`-incr` (§7), and confine every delete to FIXED_PREFIX
//!   (§9). The `running_actions_manager` execution path consumes these.
//!
//! INERT: everything is gated on a `Some` [`PortableIncrContext`], which is
//! `None` unless the worker gate
//! ([`nativelink_config::cas_server::LocalWorkerConfig::portable_incr`]) is
//! enabled AND every §12 assert passed — so it is `None` on the entire live
//! fleet and both chunks are fully inert there.
//!
//! DURABILITY: no `fsync`/`O_SYNC`/sync-write primitive appears here (CLAUDE.md
//! hard rule). All filesystem syscalls are BLOCKING and MUST run inside
//! `spawn_blocking`; [`provision_and_assert`] and the chunk-2b execution-path
//! caller both do exactly that so the tokio worker is never blocked.

use core::ffi::c_int;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use nativelink_config::cas_server::PortableIncrConfig;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_util::targetkey::TargetKey;
use tracing::info;
use uuid::Uuid;

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
// Chunk 2b: the byte-identical execroot machinery (design §4/§5/§7/§9/§11).
//
// EVERYTHING below is gated on a live [`PortableIncrContext`], which is `Some`
// only when the feature is enabled AND the chunk-2a §12 startup asserts passed
// — so it is `None` on the entire live fleet and this whole path is INERT.
// ---------------------------------------------------------------------------

/// Action Platform property carrying the fleet-agreed `targetkey` (64 lowercase
/// hex) for an allowlisted portable-incr action (design §3b carrier). Attached
/// by the Bazel client; the server validates it at ingestion; the worker reads
/// it to pick the byte-identical execroot BEFORE the `Command` is fetched.
pub const CARRIER_TARGETKEY_PROP: &str = "nl_incr_targetkey";

/// Action Platform property carrying the (config-stripped) primary output path
/// that the `targetkey` hashes (design §3b carrier). The worker matches it
/// against the allowlist and, once the `Command` is fetched, verifies the
/// worker-derived key equals the carrier.
pub const CARRIER_PRIMARY_OUTPUT_PROP: &str = "nl_incr_primary_output";

/// Whether a direct child entry of the execroot belongs to the rustc
/// incremental SEED that the per-build wipe must PRESERVE (design §7): the
/// `<label>-incr` compilation-incremental tree and the pipelined
/// `<label>-incr-metadata` tree (`rules_rust` `rustc.bzl:2255-2274`). Every
/// other entry is content-emptied each build. Note `-incr-metadata` does NOT
/// end in `-incr`, so both suffixes are matched explicitly.
#[must_use]
pub fn is_incr_seed_entry(name: &str) -> bool {
    name.ends_with("-incr") || name.ends_with("-incr-metadata")
}

/// A well-formed carrier `targetkey` is exactly 64 lowercase-hex characters
/// (blake3-256). A malformed carrier is treated as non-portable (→ normal
/// path), never as an execroot path segment — a path-traversal defense on the
/// FIXED_PREFIX join (design §9).
fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

// UNBOUNDED-OK: machine-local, in-process ownership set keyed by the OWNER
// execroot path (`<FIXED_PREFIX>/<targetkey>`). Holds at most one entry per
// DISTINCT targetkey currently OWNED by an in-flight portable action on THIS
// machine; each entry is removed on the owning action's Drop (RAII, via
// `OwnershipLeaseGuard`). Bounded by concurrent portable-action count
// (allowlisted crates only) — never a network-driven buffer, no owned bytes,
// not a durability/data path. This is the §5 machine-local lease registry.
static EXECROOT_OWNERSHIP: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Worker-side portable-incr execution context. Present (`Some`) ONLY when the
/// worker gate is enabled AND the chunk-2a §12 startup asserts passed, so it is
/// `None` on the entire live fleet — the single INERT gate for chunk 2b.
#[derive(Debug, Clone)]
pub struct PortableIncrContext {
    fixed_prefix: PathBuf,
    config: PortableIncrConfig,
}

impl PortableIncrContext {
    /// Build the context from a successful chunk-2a provision plus the worker
    /// config. Returns `None` when unprovisioned (feature off, or the §12
    /// asserts fail-loud-DISABLED it) — the INERT gate. `enabled == false` is
    /// also inert: [`Self::plan`] short-circuits before any carrier read.
    #[must_use]
    pub fn from_provision(
        provision: Option<PortableIncrProvision>,
        config: PortableIncrConfig,
    ) -> Option<Self> {
        let provision = provision?;
        Some(Self {
            fixed_prefix: provision.fixed_prefix,
            config,
        })
    }

    /// The provisioned FIXED_PREFIX (test/introspection accessor).
    #[must_use]
    pub fn fixed_prefix(&self) -> &Path {
        &self.fixed_prefix
    }

    /// Plan the portable execroot for ONE action from its carrier Platform
    /// properties (design §4/§5). Returns `None` — meaning the NORMAL path,
    /// byte-for-byte today's behavior — unless ALL of:
    /// - the feature is `enabled`;
    /// - both carrier properties are present and the `targetkey` is well-formed
    ///   64-hex (a malformed carrier → normal, never a path segment);
    /// - the carrier primary output is `is_allowlisted`.
    ///
    /// On the eligible path it acquires the machine-local ownership lease: the
    /// FIRST in-flight action for a `targetkey` on this machine is the OWNER and
    /// runs warm in `<FIXED_PREFIX>/<targetkey>`; a concurrent same-`targetkey`
    /// CONTENDER gets an ISOLATED `<FIXED_PREFIX>/<targetkey>.<uuid>` dir that is
    /// cold-discarded on cleanup (no serialization, no blocking).
    ///
    /// This performs NO filesystem work — only the (cheap, sync) lease-set
    /// insert. Directory creation + the §7 wipe happen later in
    /// [`PortableExecroot::ensure_and_wipe_execroot`] under `spawn_blocking`.
    #[must_use]
    pub fn plan(&self, platform_properties: &HashMap<String, String>) -> Option<PortableExecroot> {
        if !self.config.enabled {
            return None;
        }
        let carrier_targetkey = platform_properties.get(CARRIER_TARGETKEY_PROP)?;
        let carrier_primary_output = platform_properties.get(CARRIER_PRIMARY_OUTPUT_PROP)?;
        if !is_hex64(carrier_targetkey) {
            return None;
        }
        if !self.config.is_allowlisted(carrier_primary_output) {
            return None;
        }

        let canonical = self.fixed_prefix.join(carrier_targetkey);
        let mut owned = EXECROOT_OWNERSHIP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (execroot, role, lease) = if owned.insert(canonical.clone()) {
            (
                canonical.clone(),
                ExecrootRole::Owner,
                OwnershipLeaseGuard {
                    owned: Some(canonical),
                },
            )
        } else {
            let isolated = self
                .fixed_prefix
                .join(format!("{carrier_targetkey}.{}", Uuid::new_v4().simple()));
            (isolated, ExecrootRole::Contender, OwnershipLeaseGuard { owned: None })
        };
        drop(owned);

        Some(PortableExecroot {
            execroot,
            fixed_prefix: self.fixed_prefix.clone(),
            role,
            carrier_targetkey: carrier_targetkey.clone(),
            carrier_primary_output: carrier_primary_output.clone(),
            _lease: lease,
        })
    }
}

/// Whether this action is the warm OWNER of `<FIXED_PREFIX>/<targetkey>` or an
/// isolated CONTENDER (design §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecrootRole {
    /// Owns the byte-identical warm dir; its `-incr` seed is preserved across
    /// builds and NEVER deleted by cleanup.
    Owner,
    /// Runs in an isolated per-process dir that is cold-discarded on cleanup.
    Contender,
}

/// The planned portable execroot for one action (design §4/§5). Threaded into
/// `RunningActionImpl` and consumed on the execution path; its Drop releases the
/// §5 ownership lease.
#[derive(Debug)]
pub struct PortableExecroot {
    /// The action's build cwd (== its `work_directory`, NO `/work` segment).
    /// Owner: `<FIXED_PREFIX>/<targetkey>`. Contender:
    /// `<FIXED_PREFIX>/<targetkey>.<uuid>`.
    execroot: PathBuf,
    /// The provisioned FIXED_PREFIX — the delete-containment boundary for the
    /// §7 wipe and the §9 contender discard.
    fixed_prefix: PathBuf,
    role: ExecrootRole,
    /// Carrier `targetkey` retained for the [`Self::verify_against_command_outputs`]
    /// integrity check once the `Command` is fetched.
    carrier_targetkey: String,
    /// Carrier primary output retained for the same integrity check.
    carrier_primary_output: String,
    /// RAII §5 lease: an Owner releases its `targetkey` from the machine-local
    /// ownership set on Drop; a Contender owns nothing here.
    _lease: OwnershipLeaseGuard,
}

impl PortableExecroot {
    /// The action's build cwd / `work_directory` (design §4).
    #[must_use]
    pub fn execroot(&self) -> &Path {
        &self.execroot
    }

    /// Owner (warm) vs Contender (isolated) — design §5.
    #[must_use]
    pub fn role(&self) -> ExecrootRole {
        self.role
    }

    /// The provisioned FIXED_PREFIX (the §7/§9 delete-containment boundary).
    #[must_use]
    pub fn fixed_prefix(&self) -> &Path {
        &self.fixed_prefix
    }

    /// The isolated dir a CONTENDER must cold-discard on cleanup (design §9),
    /// paired with FIXED_PREFIX for the containment gate. `None` for an Owner
    /// (its warm dir is PRESERVED — never deleted by cleanup).
    #[must_use]
    pub fn contender_discard_target(&self) -> Option<(PathBuf, PathBuf)> {
        match self.role {
            ExecrootRole::Contender => Some((self.execroot.clone(), self.fixed_prefix.clone())),
            ExecrootRole::Owner => None,
        }
    }

    /// Integrity verify (design §11 item 1): the worker-derived [`TargetKey`]
    /// from the fetched `Command.output_paths` MUST equal the carrier. A
    /// mismatch means the carrier does not describe THIS Command's outputs —
    /// under the trusted-action model that is corruption/forgery, so we fail
    /// loud rather than build in (or seed from) a possibly-wrong warm execroot.
    /// (The server already did a `blake3(primary)==targetkey` check at
    /// ingestion; this is the worker-side defense against a carrier that
    /// disagrees with the actual Command.)
    pub fn verify_against_command_outputs(&self, output_paths: &[String]) -> Result<(), Error> {
        let derived = TargetKey::derive(output_paths).ok_or_else(|| {
            make_err!(
                Code::Internal,
                "portable_incr: action carried targetkey {} but its Command has no output_paths \
                 to derive from",
                self.carrier_targetkey
            )
        })?;
        if derived.key() != self.carrier_targetkey {
            return Err(make_err!(
                Code::Internal,
                "portable_incr: carrier targetkey {} != worker-derived {} (from Command \
                 output_paths) — refusing to build in the shared execroot",
                self.carrier_targetkey,
                derived.key()
            ));
        }
        if derived.primary_output() != self.carrier_primary_output {
            return Err(make_err!(
                Code::Internal,
                "portable_incr: carrier primary_output {:?} != worker-derived {:?} — refusing to \
                 build in the shared execroot",
                self.carrier_primary_output,
                derived.primary_output()
            ));
        }
        Ok(())
    }

    /// Ensure the execroot exists, then content-empty it PRESERVING the `-incr`
    /// / `-incr-metadata` seed trees (design §7). The Owner's warm dir keeps its
    /// seed across builds; a Contender's freshly-created isolated dir wipes to
    /// empty (a no-op). ALL deletes are confined to a subtree of FIXED_PREFIX
    /// (design §9): the function refuses if the execroot does not canonicalize
    /// UNDER FIXED_PREFIX, so a wipe can never escape the machine-local prefix.
    ///
    /// BLOCKING (mkdir/readdir/unlink syscalls) — call under `spawn_blocking`.
    /// No `fsync`/sync-write primitive (CLAUDE.md hard rule).
    pub fn ensure_and_wipe_execroot(&self) -> Result<(), Error> {
        ensure_and_wipe_execroot_at(&self.execroot, &self.fixed_prefix)
    }
}

/// Free-function form of [`PortableExecroot::ensure_and_wipe_execroot`] so the
/// async execution path can move owned `PathBuf`s into `spawn_blocking` without
/// borrowing the (non-`Send`-cloneable, lease-holding) [`PortableExecroot`].
/// BLOCKING — call under `spawn_blocking`.
pub fn ensure_and_wipe_execroot_at(execroot: &Path, fixed_prefix: &Path) -> Result<(), Error> {
    ensure_execroot_dir(execroot, fixed_prefix)?;
    wipe_contents_except(execroot, fixed_prefix, is_incr_seed_entry)
}

/// RAII §5 ownership lease. An Owner holds `Some(canonical execroot)` and, on
/// Drop, releases it from [`EXECROOT_OWNERSHIP`] so the next same-`targetkey`
/// action can reuse the warm dir. A Contender holds `None` and releases
/// nothing (its isolated dir is discarded by cleanup, not here).
#[derive(Debug)]
struct OwnershipLeaseGuard {
    owned: Option<PathBuf>,
}

impl Drop for OwnershipLeaseGuard {
    fn drop(&mut self) {
        if let Some(key) = self.owned.take() {
            EXECROOT_OWNERSHIP
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&key);
        }
    }
}

/// Confinement gate (design §9): assert `path` canonicalizes STRICTLY under
/// `fixed_prefix` before any recursive delete of it. Callers (the contender
/// cold-discard in `do_cleanup`) MUST call this before deleting so a wipe/discard
/// can never escape the machine-local prefix. BLOCKING (canonicalize).
pub fn assert_under_prefix(path: &Path, fixed_prefix: &Path) -> Result<(), Error> {
    let canon_prefix = std::fs::canonicalize(fixed_prefix).map_err(|e| {
        make_err!(
            Code::Internal,
            "portable_incr: cannot canonicalize FIXED_PREFIX {} for discard containment: {e}",
            fixed_prefix.display()
        )
    })?;
    let canon_path = std::fs::canonicalize(path).map_err(|e| {
        make_err!(
            Code::Internal,
            "portable_incr: cannot canonicalize discard target {} for containment: {e}",
            path.display()
        )
    })?;
    if !canon_path.starts_with(&canon_prefix) || canon_path == canon_prefix {
        return Err(make_err!(
            Code::Internal,
            "portable_incr: discard target {} escapes FIXED_PREFIX {} — refusing to delete",
            canon_path.display(),
            canon_prefix.display()
        ));
    }
    Ok(())
}

/// Create the execroot dir (mode 0755) if absent; a pre-existing dir (Owner
/// reuse) is left in place. The parent FIXED_PREFIX was provisioned + asserted
/// at startup (chunk 2a), so `ENOENT` on the leaf create should not happen; we
/// still create the ancestry defensively and retry. BLOCKING.
fn ensure_execroot_dir(execroot: &Path, fixed_prefix: &Path) -> Result<(), Error> {
    match mkdir_exclusive_raw(execroot, 0o755) {
        Ok(()) => chmod_raw(execroot, 0o755)
            .map_err(|e| make_err!(Code::Internal, "chmod 0755 {}: {e}", execroot.display())),
        Err(e) if e.raw_os_error() == Some(libc::EEXIST) => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
            std::fs::create_dir_all(fixed_prefix).map_err(|pe| {
                make_err!(
                    Code::Internal,
                    "create FIXED_PREFIX {} for execroot: {pe}",
                    fixed_prefix.display()
                )
            })?;
            match mkdir_exclusive_raw(execroot, 0o755) {
                Ok(()) => chmod_raw(execroot, 0o755).map_err(|e| {
                    make_err!(Code::Internal, "chmod 0755 {}: {e}", execroot.display())
                }),
                Err(e) if e.raw_os_error() == Some(libc::EEXIST) => Ok(()),
                Err(e) => Err(make_err!(
                    Code::Internal,
                    "mkdir execroot {}: {e}",
                    execroot.display()
                )),
            }
        }
        Err(e) => Err(make_err!(
            Code::Internal,
            "mkdir execroot {}: {e}",
            execroot.display()
        )),
    }
}

/// Content-empty `dir` in place, PRESERVING every direct child for which
/// `preserve(name)` is true (design §7). The delete is confined to a subtree of
/// `fixed_prefix` (design §9): `dir` MUST canonicalize under `fixed_prefix`.
/// Symlink children are unlinked (never followed). BLOCKING.
fn wipe_contents_except(
    dir: &Path,
    fixed_prefix: &Path,
    preserve: impl Fn(&str) -> bool,
) -> Result<(), Error> {
    // Containment FIRST — never read/delete under a dir that escapes the prefix.
    assert_under_prefix(dir, fixed_prefix)?;

    let entries = std::fs::read_dir(dir)
        .map_err(|e| make_err!(Code::Internal, "read execroot {} for wipe: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry
            .map_err(|e| make_err!(Code::Internal, "read execroot entry in {}: {e}", dir.display()))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if preserve(&name_str) {
            continue;
        }
        let path = entry.path();
        // `symlink_metadata` does NOT follow a final-component symlink, so a
        // symlink child is unlinked as a file (its target is never touched).
        let md = std::fs::symlink_metadata(&path).map_err(|e| {
            make_err!(Code::Internal, "lstat execroot child {}: {e}", path.display())
        })?;
        let res = if md.file_type().is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        res.map_err(|e| {
            make_err!(
                Code::Internal,
                "wipe execroot child {}: {e}",
                path.display()
            )
        })?;
    }
    Ok(())
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
