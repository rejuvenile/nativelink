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
//!   invariants, and reports the effective-enabled state; the
//!   `create_file_exclusive_no_follow` primitive backs the §12(c) EXDEV probe.
//! - **Chunk 2b (execroot core):** [`PortableIncrContext`] +
//!   [`PortableExecroot`] pick the byte-identical execroot
//!   `<FIXED_PREFIX>/<targetkey>` for an enabled+allowlisted action (§4), hold
//!   the machine-local §5 ownership lease (owner reuses the byte-identical dir /
//!   contender isolated), FULL-EMPTY wipe the execroot each build (§7 — the
//!   `-incr` seed is re-fetched fresh AFTER inputs by the out-of-band fetch, so
//!   nothing on-disk is preserved), and confine every delete to FIXED_PREFIX
//!   (§9). The `running_actions_manager` execution path consumes these.
//!
//! GATE: everything is gated on a `Some` [`PortableIncrContext`], which is
//! `None` unless the worker gate
//! ([`nativelink_config::cas_server::LocalWorkerConfig::portable_incr`]) is
//! enabled AND every §12 assert passed. When `None`, both chunks are fully
//! inert.
//!
//! ★ DEPLOYMENT STATE IS NOT A PROPERTY OF THIS CODE — DO NOT ASSERT IT HERE.
//! Docs across this module, `running_actions_manager` and `local_worker` used to
//! claim the gate was `None` "on the entire live fleet". That was true when
//! written and is now FALSE: as of 2026-08-13 the feature is ENABLED on all 10
//! workers (`worker.json5` `portable_incr.enabled: true`, `fixed_prefix`
//! `/Volumes/CrowAgent/fl-incr-execroots`), and the live logs show it running —
//! 3588 `execroot full-empty ensure+wipe`, 1710 `seed fetch complete`, and 1863
//! eviction passes that logged. Those stale claims sat in the source for weeks
//! and read as "this code cannot run in production", which is exactly the
//! premise a reviewer would rely on. Describe the GATE (what makes it
//! `Some`/`None`) and let the deployed config answer where it is on — a comment
//! cannot track a config field, and one that tries will rot silently.
//!
//! If you need to re-run the sweep that found these, note that a LINE-oriented
//! grep CANNOT: the phrases wrap across line breaks inside `///` blocks
//! (`` `None` on `` / `` the fleet ``), which is exactly how the first sweep
//! missed 15 of them. Join contiguous comment lines first, then match.
//!
//! DURABILITY: no `fsync`/`O_SYNC`/sync-write primitive appears here (CLAUDE.md
//! hard rule). All filesystem syscalls are BLOCKING and MUST run inside
//! `spawn_blocking`; [`provision_and_assert`] and the chunk-2b execution-path
//! caller both do exactly that so the tokio worker is never blocked.

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::collections::HashMap;
use std::ffi::{CString, OsString};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

use nativelink_config::cas_server::PortableIncrConfig;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_util::targetkey::TargetKey;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};
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
// O_EXCL / O_NOFOLLOW primitive backing the §12(c) EXDEV probe. BLOCKING;
// callers on an async path MUST use `spawn_blocking`.
// ---------------------------------------------------------------------------

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
// only when the feature is enabled AND the chunk-2a §12 startup asserts passed.
// When `None`, this whole path is INERT. (Where the gate is currently ON is a
// deployed-config question — see the module doc; do not restate it here.)
// ---------------------------------------------------------------------------

/// Action Platform property carrying the fleet-agreed `targetkey` (64 lowercase
/// hex) for an allowlisted portable-incr action (design §3b carrier). Attached
/// by the Bazel client; the server validates it at ingestion; the worker reads
/// it to pick the byte-identical execroot BEFORE the `Command` is fetched.
pub const CARRIER_TARGETKEY_PROP: &str = "nl_incr_targetkey";

/// Action Platform property carrying the primary output path the `targetkey`
/// commits to (design §3b carrier): the lexicographically-smallest of the
/// action's `Command.output_paths`, hashed VERBATIM by [`TargetKey::derive`]
/// (the derivation does NOT strip any config/platform segment — the raw sorted
/// path is what the key hashes). The worker matches it against the allowlist
/// and, once the `Command` is fetched, verifies the worker-derived key equals
/// the carrier.
pub const CARRIER_PRIMARY_OUTPUT_PROP: &str = "nl_incr_primary_output";

/// A well-formed carrier `targetkey` is exactly 64 lowercase-hex characters
/// (blake3-256). A malformed carrier is treated as non-portable (→ normal
/// path), never as an execroot path segment — a path-traversal defense on the
/// FIXED_PREFIX join (design §9).
fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Who is holding an entry in [`EXECROOT_OWNERSHIP`].
///
/// ★ THESE TWO ARE NOT INTERCHANGEABLE AND MUST NOT SHARE A BARE SET. Both mean
/// "do not evict this path right now", but they imply OPPOSITE things about the
/// path's BYTES: a live owner's bytes are staying on disk, whereas an eviction
/// sentinel means a peer pass is at this moment `remove_dir_all`-ing the tree,
/// so those bytes are leaving. A `HashSet` cannot tell them apart, and the
/// eviction loop consequently treated a peer pass's victim as "bytes stay" and
/// evicted an EXTRA LRU dir to cover a shortfall that was already being covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnershipHolder {
    /// An in-flight portable action owns this execroot (§5 lease). Its bytes are
    /// staying — never subtract them.
    LiveOwner,
    /// An eviction pass has claimed this path and is deleting it. Its bytes are
    /// on their way off disk — subtract them, but do NOT credit them to this
    /// pass's `dirs_evicted`/`bytes_freed`.
    EvictionSentinel,
}

// UNBOUNDED-OK: machine-local, in-process ownership registry keyed by the OWNER
// execroot path (`<FIXED_PREFIX>/<targetkey>`). Holds at most one entry per
// DISTINCT targetkey currently owned by an in-flight portable action OR claimed
// by an in-flight eviction discard on THIS machine; each entry is removed on the
// holder's Drop (RAII, via `OwnershipLeaseGuard`). Bounded by (concurrent
// portable-action count + concurrent eviction passes), allowlisted crates only —
// never a network-driven buffer, no owned bytes, not a durability/data path.
// This is the §5 machine-local lease registry.
static EXECROOT_OWNERSHIP: LazyLock<Mutex<HashMap<PathBuf, OwnershipHolder>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Worker-side portable-incr execution context. Present (`Some`) ONLY when the
/// worker gate is enabled AND the chunk-2a §12 startup asserts passed — the
/// single gate for chunk 2b; `None` makes the whole rewire inert.
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
        // Vacant-only insert. An entry held by EITHER holder means this action
        // must NOT take the canonical dir: a live owner is using it, and an
        // eviction sentinel means it is being deleted right now. Both cases
        // become a CONTENDER, exactly as the previous `HashSet::insert` bool did
        // — this is a preserved behaviour, not a new one.
        let claimed_canonical =
            match owned.entry(canonical.clone()) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(OwnershipHolder::LiveOwner);
                    true
                }
                std::collections::hash_map::Entry::Occupied(_) => false,
            };
        let (execroot, role, lease) = if claimed_canonical {
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

    /// §8 STARTUP SWEEP: remove stale CONTENDER dirs
    /// (`<FIXED_PREFIX>/<targetkey>.<uuid>`) orphaned by a prior worker run that
    /// crashed before its [`PortableExecroot`] Drop cold-discarded them. Owner
    /// warm dirs (`<FIXED_PREFIX>/<targetkey>`) are NEVER contenders and are
    /// preserved. Returns the number of contender dirs removed.
    ///
    /// Call ONCE at worker startup, BEFORE any action plans an execroot (so no
    /// contender is live) — the wiring agent invokes it from `local_worker`
    /// startup after the `Some(PortableIncrContext)` is built. INERT (no-op,
    /// returns `Ok(0)`) when the feature is disabled.
    ///
    /// BLOCKING (readdir/unlink syscalls) — call under `spawn_blocking`. No
    /// `fsync`/sync-write primitive (CLAUDE.md hard rule).
    pub fn sweep_stale_contender_dirs(&self) -> Result<usize, Error> {
        if !self.config.enabled {
            return Ok(0);
        }
        sweep_stale_contender_dirs_at(&self.fixed_prefix)
    }

    /// §8 WARM-DIR EVICTION: bound the on-disk materialized-`-incr` pool at
    /// `<FIXED_PREFIX>` to `budget_bytes` by evicting least-recently-used warm
    /// OWNER dirs (whole-dir removal — an evicted `targetkey` cold-starts +
    /// re-fetches, design §8: safe). A dir currently LEASED by an in-flight
    /// owner is NEVER evicted (respects the §5 lease via the same
    /// [`EXECROOT_OWNERSHIP`] claim `plan` uses); if the pool is over budget but
    /// every remaining candidate is leased, it REFUSES to evict a live dir and
    /// returns `still_over_budget` (backpressure), never touching a live one.
    ///
    /// Call AFTER an action's cleanup (post-execution) — the wiring agent
    /// invokes it from `running_actions_manager` cleanup, passing
    /// [`DEFAULT_WARM_DIR_BUDGET_BYTES`] (or a future config value). INERT (no-op,
    /// returns [`EvictionOutcome::default`]) when the feature is disabled.
    ///
    /// BLOCKING (readdir/lstat/unlink syscalls) — call under `spawn_blocking`.
    /// No `fsync`/sync-write primitive (CLAUDE.md hard rule).
    pub fn evict_warm_dirs_over_budget(
        &self,
        budget_bytes: u64,
    ) -> Result<EvictionOutcome, Error> {
        if !self.config.enabled {
            return Ok(EvictionOutcome::default());
        }
        evict_warm_dirs_over_budget_at(&self.fixed_prefix, budget_bytes)
    }

    /// §8 STARTUP BACKFILL: create the `<targetkey>.lock` lease record for every
    /// warm OWNER dir that predates the A3 lease adoption. Returns the number of
    /// records created. See [`ensure_owner_lock_files_at`] for why eviction
    /// cannot work on this pool without it.
    ///
    /// Call ONCE at worker startup, next to
    /// [`Self::sweep_stale_contender_dirs`]. INERT (`Ok(0)`) when the feature is
    /// disabled. BLOCKING (readdir/open syscalls) — call under `spawn_blocking`.
    pub fn ensure_owner_lock_files(&self) -> Result<usize, Error> {
        if !self.config.enabled {
            return Ok(0);
        }
        ensure_owner_lock_files_at(&self.fixed_prefix)
    }
}

/// Whether this action is the warm OWNER of `<FIXED_PREFIX>/<targetkey>` or an
/// isolated CONTENDER (design §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecrootRole {
    /// Owns the byte-identical dir `<FIXED_PREFIX>/<targetkey>`, reused across
    /// builds for path stability (rustc reuse is absolute-path-bound). The dir
    /// is FULL-EMPTY wiped each build (§7) and its `-incr` seed re-fetched fresh;
    /// it is NEVER deleted by cleanup (only by the §8 warm-dir budget eviction).
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
    /// (its dir is kept for path reuse — never deleted by cleanup, only by §8
    /// budget eviction).
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

    /// Ensure the execroot exists, then FULL-EMPTY content-wipe it (design §7):
    /// EVERY direct child is removed, preserving NOTHING. The `-incr` seed is not
    /// kept on disk — it is an evictable fetch-cache (design §7) re-materialized
    /// fresh AFTER inputs by the out-of-band seed fetch (§6.3), so the Stage-1
    /// model treats it as "evicted every build → re-fetch" (the warm same-worker
    /// on-disk preserve is a deferred optimization, mirroring the §6.5 deferred
    /// content-pin). A full-empty execroot also restores the macOS clonefile fast
    /// path for input materialization (an empty dst). ALL deletes are confined to
    /// a subtree of FIXED_PREFIX (design §9): the function refuses if the execroot
    /// does not canonicalize UNDER FIXED_PREFIX, so a wipe can never escape the
    /// machine-local prefix; symlink children are unlinked (never followed).
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
    // A3 companion: give an OWNER execroot the sibling lease record that the §8
    // eviction, the reaper and the local branch all key on. Owner-only (a
    // contender is neither actor's candidate) and NON-FATAL: this record gates a
    // disk-budget lease, and an action must never fail because of one. The cost
    // of losing it is visible, not silent — eviction then fails closed on that
    // dir and reports `no_lease_record_skipped`.
    if execroot
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(is_hex64)
        && let Err(err) = ensure_owner_lock_file(execroot)
    {
        warn!(
            ?err,
            execroot = %execroot.display(),
            "FL-1383 portable_incr: could not create the execroot lease record; \
             eviction will fail closed on this dir and the reaper will refuse it"
        );
    }
    wipe_all_contents(execroot, fixed_prefix)
}

/// RAII §5 ownership lease. A holder (a live Owner, or an eviction pass's
/// delete-sentinel) keeps `Some(path)` and, on Drop, releases it from
/// [`EXECROOT_OWNERSHIP`] so the next same-`targetkey` action can reuse the warm
/// dir. A Contender holds `None` and releases nothing (its isolated dir is
/// discarded by cleanup, not here). The guard is holder-kind agnostic: the tag
/// lives in the registry value, and removal is keyed on the path alone.
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

/// Result of the §9 confinement gate for a delete target.
///
/// ★ THE TWO NON-`Confined` OUTCOMES ARE NOT THE SAME EVENT AND MUST NEVER BE
/// COLLAPSED. A path that cannot be canonicalized because it NO LONGER EXISTS is
/// benign — the delete we were about to perform has already happened. A path
/// that canonicalizes OUTSIDE the prefix is a CONTAINMENT FAILURE and stays a
/// hard `Err` on every path through this module. `Vanished` is therefore an
/// `Ok` variant the caller must handle explicitly, while an escape remains
/// unrepresentable as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Containment {
    /// `path` canonicalizes STRICTLY under `fixed_prefix` — safe to delete.
    Confined,
    /// `path` does not exist (ENOENT). Containment is neither satisfied nor
    /// violated: there is nothing left to delete. NOT a containment failure.
    Vanished,
}

/// Whether a non-existent `path` is an error for a given caller of the
/// confinement gate. Private: the choice is made by which public entry point is
/// called, never by the caller passing a flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingPath {
    /// ENOENT on `path` is a hard error ([`assert_under_prefix`]).
    IsError,
    /// ENOENT on `path` yields [`Containment::Vanished`] ([`check_under_prefix`]).
    IsVanished,
}

/// Confinement gate (design §9): assert `path` canonicalizes STRICTLY under
/// `fixed_prefix` before any recursive delete of it. Callers (the contender
/// cold-discard in `do_cleanup`) MUST call this before deleting so a wipe/discard
/// can never escape the machine-local prefix. BLOCKING (canonicalize).
///
/// A path that does not exist is an ERROR here. Callers that race a concurrent
/// remover want [`check_under_prefix`] instead; this entry point is for callers
/// that require the target to exist.
pub fn assert_under_prefix(path: &Path, fixed_prefix: &Path) -> Result<(), Error> {
    match check_under_prefix_impl(path, fixed_prefix, MissingPath::IsError)? {
        Containment::Confined => Ok(()),
        // Unreachable: `MissingPath::IsError` turns ENOENT into `Err` above.
        // Represented rather than `unreachable!()` so a future edit to the impl
        // cannot silently convert a missing path into a successful delete.
        Containment::Vanished => Err(make_err!(
            Code::Internal,
            "portable_incr: confinement gate returned Vanished under MissingPath::IsError for {}",
            path.display()
        )),
    }
}

/// The ONLY error an eviction pass may treat as "the object is gone": ENOENT.
/// Every other errno is a REAL FAULT and must abort the pass.
///
/// ★ FUNNELLED DELIBERATELY — do not re-open-code this test. It previously
/// appeared inline at SIX call sites, and a review mutation proved FIVE of them
/// could be widened to swallow EVERY errno with the entire 234-test lib suite
/// still green: the tolerance was actually tested at exactly ONE site (the
/// size-walk `read_dir`, the only one a `chmod 000` fixture can reach).
///
/// The gap had a DIRECTION, which is what made it dangerous: every unguarded arm
/// fails toward silently skipping or under-counting, so the disk-growth guard
/// fails OPEN. `<FIXED_PREFIX>` lives on an external volume
/// (`/Volumes/CrowAgent`), where **EIO** is the realistic errno — and an EIO
/// storm would have read as "every candidate vanished, the pool is empty,
/// nothing to do" while the disk filled. One predicate with one test over
/// constructed `io::Error`s (`errno_classification_tests`) is testable where
/// six inline copies were not, and the copies can no longer drift apart.
fn is_vanished(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound
}

/// Confinement gate (design §9) for callers that RACE a concurrent remover: same
/// containment guarantee as [`assert_under_prefix`], but a target that has
/// already been removed (ENOENT) reports [`Containment::Vanished`] instead of
/// erroring.
///
/// ★ ONLY `std::io::ErrorKind::NotFound` on the TARGET is tolerated. A `FIXED_PREFIX`
/// that cannot be canonicalized (including ENOENT — an unprovisioned or
/// corrupted root, never a benign race) and every other errno on the target
/// (EACCES from a permissions regression, ELOOP, ENOTDIR, EIO …) still abort.
/// BLOCKING (canonicalize).
pub(crate) fn check_under_prefix(path: &Path, fixed_prefix: &Path) -> Result<Containment, Error> {
    check_under_prefix_impl(path, fixed_prefix, MissingPath::IsVanished)
}

/// Shared implementation of the §9 confinement gate. The ESCAPE branch and its
/// error text are deliberately shared by both public entry points so the two can
/// never drift; `missing` selects ONLY the disposition of ENOENT on the target.
fn check_under_prefix_impl(
    path: &Path,
    fixed_prefix: &Path,
    missing: MissingPath,
) -> Result<Containment, Error> {
    // The PREFIX must always resolve. A FIXED_PREFIX that cannot be
    // canonicalized is an unprovisioned or corrupted root — never a benign
    // race — so ENOENT here stays HARD for every caller.
    let canon_prefix = std::fs::canonicalize(fixed_prefix).map_err(|e| {
        make_err!(
            Code::Internal,
            "portable_incr: cannot canonicalize FIXED_PREFIX {} for discard containment: {e}",
            fixed_prefix.display()
        )
    })?;
    let canon_path = match std::fs::canonicalize(path) {
        Ok(canon_path) => canon_path,
        // Matched EXHAUSTIVELY on `MissingPath` (no `_` arm, no `==`): a future
        // third variant must fail to compile here rather than silently inherit
        // the hard-error path. Every other enum in this module is matched the
        // same way.
        Err(e) => match missing {
            MissingPath::IsVanished if is_vanished(&e) => {
                return Ok(Containment::Vanished);
            }
            MissingPath::IsVanished | MissingPath::IsError => {
                return Err(make_err!(
                    Code::Internal,
                    "portable_incr: cannot canonicalize discard target {} for containment: {e}",
                    path.display()
                ));
            }
        },
    };
    if !canon_path.starts_with(&canon_prefix) || canon_path == canon_prefix {
        return Err(make_err!(
            Code::Internal,
            "portable_incr: discard target {} escapes FIXED_PREFIX {} — refusing to delete",
            canon_path.display(),
            canon_prefix.display()
        ));
    }
    Ok(Containment::Confined)
}

// ---------------------------------------------------------------------------
// §8 disk budget: warm-dir eviction + contender-dir startup sweep (design §8).
//
// The materialized `-incr` dirs at `<FIXED_PREFIX>/<targetkey>` are the only
// genuinely-NEW on-disk pool (the CAS-resident `-incr` CONTENT is handled by
// §6.5). No shared disk-budget authority exists between the FilesystemStore and
// DirectoryCache budgets, so the operator decision (design §8) is a STATIC
// reservation carve-out bounded by this module's own LRU under a static cap.
// ---------------------------------------------------------------------------

// CAPPED AT 40 GiB (raised from 20 GiB 2026-08-13, FL-1383 T92).
// ★ THE ORIGINAL SIZING PROOF IS SUPERSEDED, AND THAT IS WHY THIS MOVED. Design v4
// §6.5 bounded the materialized warm-dir pool at a full-CI-widen worst case of
// 27×400 MB = 10.9 GB (one-crate = 0.46 GB), and 20 GiB was chosen to leave ≥9 GiB
// headroom over THAT number. The measured publish generation once RustcLink enters
// the portable scope is ~27 GB — 768 targetkeys (194 RustcLink targets × 4
// incremental lanes) at ~35 MB each — i.e. 2.5× the figure the 20 GiB was sized
// against, and ~1.26× the 21.47 GB the old cap actually is. The pool would have been
// OVER BUDGET BY CONSTRUCTION on day one of the widening.
// ★ WHAT THIS COSTS, stated because the old comment's second clause no longer holds:
// 20 GiB was not arbitrary — it EQUALLED the FL-688 pin-budget ceiling the §8
// carve-out is sized against. 40 GiB deliberately breaks that equality. The carve-out
// is a STATIC reservation with no shared disk-budget authority between the
// FilesystemStore and DirectoryCache budgets (see the block above), so raising it
// takes 20 GiB of headroom from whatever else shares the volume rather than from a
// negotiated pool. That is the trade being made: over-budget-by-construction churn
// on every portable action, against 20 GiB of unreserved disk.
// ★ WHY OVER-BUDGET WAS NOT MERELY WASTEFUL: `evict_warm_dirs_over_budget_at`
// recursively size-walks every hex64 dir on every call, post-action for every
// portable action — a trigger count the widening multiplies ~5.6× — and the LRU has
// NO mnemonic partition, so RustcLink pressure would evict Rustc dirs. Eviction is
// functionally safe (an evicted key cold-starts and re-fetches) but not free.
// Over-budget → LRU-evict cold (safe: an evicted `targetkey` cold-starts +
// re-fetches, design §8), never grows unbounded at CI-widen. The wiring agent
// passes this (or a future config value) to `evict_warm_dirs_over_budget`.
/// Default static on-disk budget (bytes) for the materialized-`-incr` execroot
/// pool at `<FIXED_PREFIX>` — the design v4 §8 static-reservation carve-out.
pub const DEFAULT_WARM_DIR_BUDGET_BYTES: u64 = 40 * 1024 * 1024 * 1024;

/// Outcome of one [`PortableIncrContext::evict_warm_dirs_over_budget`] pass. The
/// wiring agent turns these into the design §12 counters; this module performs
/// NO metric registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EvictionOutcome {
    /// Number of warm owner dirs removed this pass.
    pub dirs_evicted: usize,
    /// Apparent bytes reclaimed by the evicted dirs.
    pub bytes_freed: u64,
    /// Apparent bytes still resident in the warm pool after this pass.
    pub bytes_remaining: u64,
    /// Over-budget candidates SKIPPED because they were LEASED by a live owner
    /// IN THIS PROCESS (the §5 backpressure signal — a live warm dir is never
    /// evicted). Its cross-process counterpart is [`Self::xproc_leased_skipped`].
    pub leased_skipped: usize,
    /// Over-budget candidates SKIPPED because ANOTHER PROCESS holds the
    /// `flock(2)` lease on `<targetkey>.lock` — a `process_wrapper` local-branch
    /// build compiling into that execroot, or the out-of-repo reaper
    /// mid-eviction of it. Distinct from [`Self::leased_skipped`] because that
    /// one is decided by our in-process registry and this one by the kernel:
    /// they have different blind spots, and collapsing them would hide which
    /// side the contention came from. Like an in-process lease, the bytes STAY
    /// (nobody is deleting them on our behalf), so they remain in `total` and
    /// can raise `still_over_budget`.
    pub xproc_leased_skipped: usize,
    /// Over-budget candidates SKIPPED because `<targetkey>.lock` DOES NOT EXIST,
    /// so no lease could be taken and nothing proves the tree is unowned.
    ///
    /// ★ FAIL-CLOSED ON PURPOSE, and the reason is not ours — it was red-teamed
    /// in the reaper (`bld/fl-incr-execroot-reaper.zsh`, `★ THE LEASE` point 3).
    /// Creating the lock file here instead would make a FRESH inode and lock
    /// THAT, while a live builder holds the real (since-unlinked) one: two
    /// holders in one execroot, which is the torn-`.rlib` race (FL-1383 red-team
    /// B1). The worker therefore creates the lock when it creates the execroot
    /// and BACKFILLS the pool at startup ([`PortableIncrContext::ensure_owner_lock_files`]),
    /// and treats a missing record at eviction time as an anomaly, never as
    /// permission to delete. Counted so the skip is not silent: the bytes stay
    /// in `total`, so a pool held up by missing records surfaces as
    /// `still_over_budget` on the operator's `warn!`.
    pub no_lease_record_skipped: usize,
    /// Candidates that VANISHED under this pass (ENOENT) — removed concurrently
    /// between enumeration and eviction, at the lstat, the size walk, or the
    /// discard. Benign: the desired end state, reached by someone else. Counted
    /// so a fail-soft is not a silent one — a pass that skips everything and a
    /// pass that had nothing to do are otherwise indistinguishable.
    ///
    /// ★ WHO THE "SOMEONE ELSE" WAS: overwhelmingly THIS WORKER. `cleanup()`
    /// used to spawn an independent full-pool pass per portable action with no
    /// pass-level serialization, so passes routinely overlapped (9 eviction
    /// events in one second were observed on one host). The decisive evidence
    /// that the remover was us and not an external actor: after the 20→40 GiB
    /// budget raise the ENOENT rate fell to 0 across 117 invocations, exactly
    /// when evictions stopped — an external remover would not stop when we do.
    ///
    /// ★ THE SINGLE-FLIGHT ACTOR ([`EvictionActor`]) REMOVED THAT CONTRIBUTOR,
    /// NOT THE SOURCE — do not read it as "this field is now dead" and delete
    /// the tolerance. Three removers remain that we do not control (the armed
    /// out-of-repo reaper, which `rename(2)`s a victim out of the namespace
    /// mid-walk before `rm`; `bld/incr-reuse-ci-gate.zsh`'s unleased `rm -rf`;
    /// crash recovery and manual cleanup) — and the deepest reason is not about
    /// any of them: the size walk is a NON-ATOMIC READ OF A MUTABLE TREE by
    /// construction, so tolerance is the only correct semantics for the
    /// enumeration itself, not a belt for a particular peer.
    ///
    /// NOT credited to `dirs_evicted`/`bytes_freed`: this pass did not free
    /// those bytes.
    pub vanished_skipped: usize,
    /// Candidates skipped because a CONCURRENT EVICTION PASS on this worker had
    /// already claimed them and was deleting them. Distinct from
    /// `leased_skipped` (a live owner, whose bytes STAY) because a peer pass's
    /// bytes are LEAVING: they are subtracted from `bytes_remaining` but, like
    /// `vanished_skipped`, are NOT credited to `dirs_evicted`/`bytes_freed`.
    ///
    /// ★ PROVABLY 0 UNDER [`EvictionActor`], AND KEPT AS THE PROOF. One actor
    /// owns the pool for the process lifetime, so no in-process pass can observe
    /// a sentinel it did not itself place. A non-zero value therefore means the
    /// actor has been BYPASSED — a second call path to
    /// [`PortableIncrContext::evict_warm_dirs_over_budget`], or a second actor —
    /// which is precisely the regression single-flight can suffer and nothing
    /// else would notice.
    ///
    /// D2 IS NOT DEAD, only this counter's accounting purpose is: the
    /// `EXECROOT_OWNERSHIP` sentinel must remain because `plan()` still races
    /// the eviction, and a concurrent same-`targetkey` action must see it and
    /// become an isolated contender.
    pub peer_pass_skipped: usize,
    /// `true` iff the pool is STILL over budget after evicting every non-leased
    /// candidate (every remaining dir is leased → refuse, don't evict a live
    /// one). A diagnostic for the operator: the static reservation is too small
    /// for the concurrent live-owner working set.
    pub still_over_budget: bool,
}

/// Which log arm an [`EvictionOutcome`] selects in [`log_eviction_outcome`].
///
/// ★ THIS EXISTS TO MAKE THE LOG CONDITION TESTABLE. It used to be an `if/else
/// if` chain inline at the call site, and a review mutation deleted the
/// `vanished_skipped` reader from it with the entire 234-test lib suite still
/// green — the exact "computed-and-unread signal" the field's own doc warns
/// about. A log line cannot be asserted on cheaply; a pure function returning
/// this enum can, so the arm selection is decided here and merely rendered
/// there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictionLogArm {
    /// Still over budget after the pass — operator `warn!`.
    ///
    /// ★ THE CAUSE IS NO LONGER SINGULAR, WHICH IS WHY THE MESSAGE MUST NOT NAME
    /// ONE. Pre-A3, `still_over_budget ⟹ leased_skipped > 0` was an invariant:
    /// every other disposition subtracts from `total`, so an in-process lease was
    /// the only way to end over budget, and the message could say so. A3 breaks
    /// that invariant on purpose — a cross-process lease (`xproc_leased_skipped`)
    /// and a missing lease record (`no_lease_record_skipped`) both leave the bytes
    /// in `total`. Those three need OPPOSITE operator responses (wait / wait for
    /// another process / find who deleted the records), so the text names the
    /// alternatives and the fields carry the discrimination.
    Backpressure,
    /// This pass actually removed dirs.
    Evicted,
    /// This pass removed NOTHING: its candidates vanished under it, a
    /// concurrent pass on this worker had already claimed them, or another
    /// PROCESS held (or had removed) their lease. Deliberately a DISTINCT arm
    /// from `Evicted` so "eviction is working" keeps meaning something.
    FreedNothing,
    /// Pool within budget, nothing to do — `debug!` (compiled out in release).
    WithinBudget,
}

impl EvictionOutcome {
    /// Select the log arm. Pure; see [`EvictionLogArm`] for why this is not
    /// inline at the call site.
    pub(crate) fn log_arm(&self) -> EvictionLogArm {
        if self.still_over_budget {
            EvictionLogArm::Backpressure
        } else if self.dirs_evicted > 0 {
            EvictionLogArm::Evicted
        } else if self.vanished_skipped > 0
            || self.peer_pass_skipped > 0
            || self.xproc_leased_skipped > 0
            || self.no_lease_record_skipped > 0
        {
            EvictionLogArm::FreedNothing
        } else {
            EvictionLogArm::WithinBudget
        }
    }
}

// ---------------------------------------------------------------------------
// §8 SINGLE-FLIGHT EVICTION ACTOR (design A2).
//
// ★ WHAT THIS REPLACES AND WHY. `RunningActionImpl::cleanup` used to
// `spawn_blocking` a WHOLE-POOL eviction pass and `.await` it, once per portable
// action, with no pass-level serialization anywhere: 9 eviction events in one
// second were observed on one host, and every one of those passes recursively
// size-walked every warm dir in the pool BEFORE comparing against the budget.
// Two consequences, and the second is the larger one:
//
//   (a) passes raced each other's deletes — the dominant source of the ENOENT
//       aborts the tolerance above exists for (`vanished_skipped`);
//   (b) every portable action PAID for a full-pool recursive walk on its own
//       cleanup path, i.e. eviction sat on the action's critical path.
//
// One long-lived actor fixes both: `cleanup()` now stores at most one wakeup and
// returns immediately, and exactly one pass can be in flight in this process.
//
// ★ WHAT IT DOES NOT FIX, stated here because the opposite is easy to assume:
//
//   - It does NOT remove the ENOENT source, only its dominant contributor. The
//     reaper `rename(2)`s a victim out of the namespace before `rm`, the CI gate
//     `rm -rf`s execroots holding no lease, and — deepest — a recursive size walk
//     is a NON-ATOMIC READ OF A MUTABLE TREE by construction, which nothing can
//     make atomic. `vanished_skipped` and `is_vanished` stay.
//   - It does NOT remove D2. Under single-flight `peer_pass_skipped` is provably
//     0, so the holder-kind split's ACCOUNTING purpose is dead — but the
//     `EXECROOT_OWNERSHIP` sentinel itself must remain, because `plan()` still
//     races the eviction: a concurrent same-`targetkey` action must see the
//     sentinel and become an isolated contender. Deleting the sentinel
//     reintroduces the plan-side TOCTOU.
// ---------------------------------------------------------------------------

/// Responsiveness floor for the eviction actor: a pass runs at least this often
/// even with no arrivals.
///
/// Purpose is convergence, not scheduling — every real trigger arrives as a
/// notification from `cleanup()`. The tick exists so a pool that goes over
/// budget through a channel we do NOT observe (an external actor, a pass that
/// errored, a notification lost to a bug) still gets corrected.
///
/// ★ WHAT 5 MINUTES COSTS, STATED IN BOTH DIRECTIONS — an earlier version of this
/// comment claimed the tick is "STRICTLY LESS eviction work than the pre-A2
/// shape", which is only true UNDER LOAD and false at idle, and idle is a normal
/// fleet state. Under load it is much less (pre-A2 ran one full-pool walk per
/// portable action, ~9/s observed at peak; now a burst coalesces to at most one
/// pass plus one pending). At IDLE it is strictly MORE: pre-A2 an idle worker ran
/// zero passes, and this runs one every 300 s forever — measured at ~1.5 ms per
/// warm dir, i.e. ~2.8 s on a ci-mac-1-sized 1,863-dir pool, so roughly a 1%
/// duty cycle of one blocking thread on a worker with nothing to do. That is the
/// trade being made for a bounded worst-case staleness of the guard; it is not
/// free, and a future reader deciding whether to keep the tick should weigh the
/// idle cost, not the loaded comparison.
const WARM_DIR_EVICTION_TICK: Duration = Duration::from_secs(300);

/// Handle to the process's single warm-dir eviction actor (design A2).
///
/// OWNED BY `RunningActionsManagerImpl` so it dies with the manager: [`Drop`]
/// aborts the task. Cloning is deliberately not offered — one actor per process
/// is the entire point.
#[derive(Debug)]
pub struct EvictionActor {
    /// Wakeup channel. `Notify::notify_one` stores AT MOST ONE permit, which is
    /// exactly the arrival policy this actor wants: at most one pass in flight,
    /// at most one pending. See [`Self::request`].
    notify: Arc<Notify>,
    /// Total wakeups requested since spawn. Read by the actor to report how many
    /// arrivals each pass COALESCED — the direct evidence for (or against) the
    /// coalescing choice, and the counter the design names as its own falsifier
    /// ("instrument passes-requested vs passes-executed").
    requests: Arc<AtomicU64>,
    /// Unix-millis at which the most recent pass COMPLETED; 0 = none yet.
    ///
    /// ★ THE ACTOR'S LIVENESS OBSERVABLE, and it is deliberately a timestamp the
    /// actor must keep REFRESHING rather than a flag it sets once. Residue —
    /// "a pass ran at some point" — is a one-way latch: it turns on and can never
    /// turn off, so it would still read healthy after the actor died, which is
    /// exactly the regression it exists to catch. See [`Self::liveness`].
    last_pass_completed_ms: Arc<AtomicU64>,
    /// Whether the last [`Self::request`] already reported a non-live actor, so a
    /// wedged pool logs on the TRANSITION rather than once per portable action.
    /// Two-state (cleared when liveness returns), never a latch.
    reported_not_live: Arc<AtomicBool>,
    /// Unix-millis at spawn — the reference for "overdue" before any pass has
    /// completed, so a wedged FIRST pass is detectable too.
    spawned_ms: u64,
    /// How long since the last completed pass before a still-running actor is
    /// reported overdue ([`ACTOR_OVERDUE_AFTER_TICKS`] × the tick it was spawned
    /// with, so a test's short tick gets a proportionally short threshold).
    overdue_after: Duration,
    task: tokio::task::JoinHandle<()>,
}

/// What [`EvictionActor::request`] observed about the actor at the instant it
/// asked. Computed from LIVE STATE (`JoinHandle::is_finished` + the last pass's
/// wall-clock) — never from residue.
///
/// ★ WHY THIS EXISTS. A2 concentrates every eviction into one task, and both
/// review pairs converged on the same failure mode: if that task dies or wedges
/// inside `spawn_blocking` on a hung `/Volumes/CrowAgent`, eviction stops
/// FOREVER and nothing says so — `request()` would keep bumping a counter nobody
/// reads while the pool grows, and the only arm that would have fired
/// (`WithinBudget`) is `debug!`, compiled out of the release worker. Pre-A2 the
/// same hang wedged the ACTION, which is loud. Converting a loud failure into a
/// silent one is not an acceptable trade for a disk-growth guard, so the
/// requester — which is by construction still alive — checks on the actor's
/// behalf instead of the actor checking on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorLiveness {
    /// The task is running and a pass has completed recently enough.
    Live,
    /// The task has finished: it panicked, or something aborted it. Eviction is
    /// over for this process.
    TaskGone,
    /// The task still exists but no pass has completed in far longer than the
    /// tick — the shape a hung blocking syscall produces.
    PassOverdue { since_ms: u64 },
}

/// Decide liveness from the four live inputs. Split out as a PURE function so it
/// is table-testable without timing: a liveness detector that can only be
/// exercised by waiting is a detector nobody checks in both directions.
///
/// `last_pass_ms == 0` means "no pass has completed yet", which is normal at
/// startup, so the reference is the spawn time until the first pass lands.
pub(crate) fn actor_liveness_from(
    task_finished: bool,
    last_pass_ms: u64,
    spawned_ms: u64,
    now_ms: u64,
    overdue_after: Duration,
) -> ActorLiveness {
    if task_finished {
        return ActorLiveness::TaskGone;
    }
    let reference = if last_pass_ms == 0 {
        spawned_ms
    } else {
        last_pass_ms
    };
    let since_ms = now_ms.saturating_sub(reference);
    if u128::from(since_ms) > overdue_after.as_millis() {
        return ActorLiveness::PassOverdue { since_ms };
    }
    ActorLiveness::Live
}

/// How long after the last completed pass a still-running actor is called
/// overdue. Three ticks: one to be scheduled, one to run (a pass is ~2.8 s on a
/// ci-mac-1-sized pool, so the tick dwarfs it), and one of slack before crying
/// wolf on a merely busy box.
const ACTOR_OVERDUE_AFTER_TICKS: u32 = 3;

fn unix_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl EvictionActor {
    /// Spawn the actor with the production tick. Requires a tokio runtime context
    /// (the production caller is `RunningActionsManagerImpl::set_portable_incr`,
    /// which runs inside `new_local_worker`).
    #[must_use]
    pub fn spawn(ctx: PortableIncrContext, budget_bytes: u64) -> Self {
        Self::spawn_with_tick(ctx, budget_bytes, WARM_DIR_EVICTION_TICK)
    }

    /// [`Self::spawn`] with an explicit convergence floor. The tick is a
    /// parameter so a test can prove the floor ACTUALLY FIRES — with it baked in
    /// as a 300 s constant, "the tick converges the pool" and "the tick can never
    /// fire" are indistinguishable, which is how a review found both mutants
    /// surviving.
    #[must_use]
    pub fn spawn_with_tick(ctx: PortableIncrContext, budget_bytes: u64, tick: Duration) -> Self {
        let notify = Arc::new(Notify::new());
        let requests = Arc::new(AtomicU64::new(0));
        let last_pass_completed_ms = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(eviction_actor_loop(
            ctx,
            budget_bytes,
            tick,
            Arc::clone(&notify),
            Arc::clone(&requests),
            Arc::clone(&last_pass_completed_ms),
        ));
        Self {
            notify,
            requests,
            last_pass_completed_ms,
            reported_not_live: Arc::new(AtomicBool::new(false)),
            spawned_ms: unix_millis_now(),
            overdue_after: tick * ACTOR_OVERDUE_AFTER_TICKS,
            task,
        }
    }

    /// Observe the actor's liveness from live state. See [`ActorLiveness`].
    pub(crate) fn liveness(&self) -> ActorLiveness {
        actor_liveness_from(
            self.task.is_finished(),
            self.last_pass_completed_ms.load(Ordering::Relaxed),
            self.spawned_ms,
            unix_millis_now(),
            self.overdue_after,
        )
    }

    /// Ask for an eviction pass. NON-BLOCKING and not `async`: this is what
    /// replaced a `spawn_blocking(...).await` of a whole-pool walk on the
    /// action-cleanup critical path.
    ///
    /// ★ ARRIVAL POLICY: COALESCE — never queue, never silently skip. A pass
    /// reads GLOBAL state, so two passes queued back-to-back compute the same
    /// answer twice and the second one buys nothing but another full walk.
    /// Skipping is unsafe in the tail case: the last portable action of a build
    /// is exactly the one whose over-budget state would otherwise never be
    /// observed. `notify_one`'s "at most one stored permit" is precisely "at
    /// most one in flight, at most one pending".
    ///
    /// It also CHECKS THE ACTOR IS STILL ALIVE, on the actor's behalf. See
    /// [`ActorLiveness`] for why the requester does the checking; the report
    /// fires on the TRANSITION so a wedged pool does not emit one line per
    /// portable action.
    pub fn request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();

        match self.liveness() {
            ActorLiveness::Live => {
                self.reported_not_live.store(false, Ordering::Relaxed);
            }
            not_live => {
                if !self.reported_not_live.swap(true, Ordering::Relaxed) {
                    error!(
                        liveness = ?not_live,
                        requests = self.requests(),
                        "FL-1383 portable_incr: the warm-dir eviction actor is not \
                         running — the pool is NO LONGER BOUNDED and will grow \
                         until this worker restarts"
                    );
                }
            }
        }
    }

    /// Total passes requested since spawn (the wiring's observable — a request
    /// is fire-and-forget, so there is nothing to await).
    #[must_use]
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }
}

impl Drop for EvictionActor {
    fn drop(&mut self) {
        // The actor holds no state anyone else can observe, so abort is the
        // whole shutdown path. A pass already inside `spawn_blocking` is NOT
        // cancellable and runs its syscalls to completion on the blocking pool;
        // that is bounded (one pool walk) and leaves no partial state — a discard
        // is `remove_dir_all` of a tree already chosen for deletion.
        self.task.abort();
    }
}

/// The actor body: wait for an arrival OR the tick, run exactly one pass, repeat.
///
/// ★ A DROPPED `Notified` DOES NOT LOSE THE WAKEUP, which is what makes
/// `select!` safe here. When the tick branch wins, `select!` drops the
/// half-polled `notified()` future; tokio's `Notified::drop` re-delivers a
/// received-but-unconsumed `notify_one` (`tokio-1.52.3 src/sync/notify.rs`
/// `drop_notified`, which calls `notify_locked`; with no other waiter its
/// `EMPTY | NOTIFIED` arm stores the permit back). Verified against the vendored
/// source, not assumed.
async fn eviction_actor_loop(
    ctx: PortableIncrContext,
    budget_bytes: u64,
    tick: Duration,
    notify: Arc<Notify>,
    requests: Arc<AtomicU64>,
    last_pass_completed_ms: Arc<AtomicU64>,
) {
    let mut requests_at_last_pass: u64 = 0;
    loop {
        tokio::select! {
            () = notify.notified() => {}
            () = tokio::time::sleep(tick) => {}
        }
        let requests_now = requests.load(Ordering::Relaxed);
        let coalesced = requests_now.saturating_sub(requests_at_last_pass);
        requests_at_last_pass = requests_now;

        let pass_ctx = ctx.clone();
        // The pass is BLOCKING (readdir/lstat/unlink), so it runs on the
        // blocking pool — but nothing on an action's path awaits it any more.
        match tokio::task::spawn_blocking(move || {
            pass_ctx.evict_warm_dirs_over_budget(budget_bytes)
        })
        .await
        {
            Ok(Ok(outcome)) => log_eviction_outcome(&outcome, coalesced),
            Ok(Err(err)) => {
                error!(
                    ?err,
                    "FL-1383 portable_incr: warm-dir eviction failed; continuing"
                );
            }
            Err(err) => {
                error!(
                    ?err,
                    "FL-1383 portable_incr: warm-dir eviction task join failed; continuing"
                );
            }
        }
        // Refreshed on EVERY completed pass, including a failed one: the signal
        // being kept alive is "this actor is still turning", not "a pass
        // succeeded". A pass that errors is loud on its own arm above; an actor
        // that stopped turning is what nothing else can see.
        last_pass_completed_ms.store(unix_millis_now(), Ordering::Relaxed);
    }
}

/// Render one pass's outcome. Arm selection lives in [`EvictionOutcome::log_arm`]
/// so it can be unit-tested; this only renders. See [`EvictionLogArm`].
///
/// This feature has ZERO metric series (964 on the endpoint, 0 matching
/// `portable_incr`), so THE LOG LINE IS THE INTERFACE and its wording is
/// load-bearing.
fn log_eviction_outcome(outcome: &EvictionOutcome, requests_coalesced: u64) {
    match outcome.log_arm() {
        EvictionLogArm::Backpressure => warn!(
            dirs_evicted = outcome.dirs_evicted,
            leased_skipped = outcome.leased_skipped,
            xproc_leased_skipped = outcome.xproc_leased_skipped,
            no_lease_record_skipped = outcome.no_lease_record_skipped,
            vanished_skipped = outcome.vanished_skipped,
            peer_pass_skipped = outcome.peer_pass_skipped,
            bytes_remaining = outcome.bytes_remaining,
            requests_coalesced,
            "FL-1383 portable_incr: warm-dir pool STILL over budget after the pass — every remaining candidate is leased by a live action, leased by another process, or missing its lease record; read the *_skipped fields to tell which — backpressure"
        ),
        EvictionLogArm::Evicted => info!(
            dirs_evicted = outcome.dirs_evicted,
            bytes_freed = outcome.bytes_freed,
            vanished_skipped = outcome.vanished_skipped,
            peer_pass_skipped = outcome.peer_pass_skipped,
            leased_skipped = outcome.leased_skipped,
            xproc_leased_skipped = outcome.xproc_leased_skipped,
            no_lease_record_skipped = outcome.no_lease_record_skipped,
            bytes_remaining = outcome.bytes_remaining,
            requests_coalesced,
            "FL-1383 portable_incr: evicted warm dirs over budget"
        ),
        // ★ DISTINCT MESSAGE, ON PURPOSE. This pass evicted NOTHING; its
        // candidates were removed by someone else (`vanished_skipped`), were
        // already being deleted by a concurrent pass (`peer_pass_skipped`), or
        // are leased by / missing a record for another process. Reusing the
        // "evicted warm dirs" text here would undo the accounting discipline the
        // struct enforces: `AlreadyGone` is deliberately NOT credited to
        // `dirs_evicted` so that "eviction is working" keeps meaning something,
        // and a shared log string would hand exactly that false credit back to
        // anyone grepping the message.
        //
        // It still logs at `info!` rather than `debug!`: these passes previously
        // emitted `error!` (722 of the 1863 passes visible in the live fleet logs
        // at 2026-08-13T18:30Z, all ENOENT), and demoting them would replace a
        // false alarm with a blind spot.
        EvictionLogArm::FreedNothing => info!(
            vanished_skipped = outcome.vanished_skipped,
            peer_pass_skipped = outcome.peer_pass_skipped,
            leased_skipped = outcome.leased_skipped,
            xproc_leased_skipped = outcome.xproc_leased_skipped,
            no_lease_record_skipped = outcome.no_lease_record_skipped,
            bytes_remaining = outcome.bytes_remaining,
            requests_coalesced,
            "FL-1383 portable_incr: warm-dir eviction pass freed nothing — every candidate vanished, was claimed by a concurrent pass, or is leased by another process"
        ),
        EvictionLogArm::WithinBudget => debug!(
            bytes_remaining = outcome.bytes_remaining,
            requests_coalesced,
            "FL-1383 portable_incr: warm-dir pool within budget, nothing evicted"
        ),
    }
}

/// Whether `name` is a CONTENDER dir name `<64-hex-targetkey>.<32-hex-uuid>` (as
/// minted by [`PortableIncrContext::plan`]: `format!("{targetkey}.{uuid}")` with
/// `Uuid::simple()` = 32 lowercase hex). An OWNER warm dir is exactly 64 hex (no
/// `.`) → not a contender; the `.fl1383_exdev_*` probe files start with `.` (empty
/// key) → not a contender. This is the sole predicate that decides removal in the
/// startup sweep, so it is deliberately strict.
fn is_contender_dir_name(name: &str) -> bool {
    match name.split_once('.') {
        Some((key, suffix)) => {
            is_hex64(key)
                && suffix.len() == 32
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        }
        None => false,
    }
}

/// Free-function form of [`PortableIncrContext::sweep_stale_contender_dirs`].
/// BLOCKING — call under `spawn_blocking`.
fn sweep_stale_contender_dirs_at(fixed_prefix: &Path) -> Result<usize, Error> {
    let entries = std::fs::read_dir(fixed_prefix).map_err(|e| {
        make_err!(
            Code::Internal,
            "portable_incr: read FIXED_PREFIX {} for contender sweep: {e}",
            fixed_prefix.display()
        )
    })?;
    let mut removed = 0usize;
    for entry in entries {
        let entry = entry.map_err(|e| {
            make_err!(
                Code::Internal,
                "portable_incr: read FIXED_PREFIX entry in {}: {e}",
                fixed_prefix.display()
            )
        })?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Only contender-shaped dirs are stale orphans; owner warm dirs (and any
        // `-incr` seed inside them) and probe files are PRESERVED.
        if !is_contender_dir_name(&name_str) {
            continue;
        }
        let path = entry.path();
        // A contender-shaped NON-directory is left untouched (never expected).
        let md = std::fs::symlink_metadata(&path).map_err(|e| {
            make_err!(Code::Internal, "lstat sweep candidate {}: {e}", path.display())
        })?;
        if !md.file_type().is_dir() {
            continue;
        }
        // Cold-discard the whole isolated contender dir (it holds no `-incr`
        // seed to preserve), confined to a subtree of FIXED_PREFIX (§9).
        // This sweep runs at startup before any action can plan an execroot, so
        // nothing should race it; `AlreadyGone` is nonetheless not a removal and
        // is not counted as one.
        match discard_dir_tree_confined(&path, fixed_prefix)? {
            DiscardOutcome::Removed => removed += 1,
            DiscardOutcome::AlreadyGone => {}
        }
    }
    Ok(removed)
}

/// TEST-ONLY seam. The §8 eviction races a concurrent remover — usually another
/// eviction pass on THIS worker — at several
/// distinct points, and the only honest way to prove each ENOENT branch is
/// load-bearing is to make the vanish happen at exactly that point rather than
/// sleeping and hoping. `#[cfg(test)]` on a library target: this does not exist
/// in the shipped worker.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbePoint {
    /// Immediately before a candidate's enumeration `lstat`.
    BeforeEnumLstat,
    /// After the §9 containment gate passed, immediately before `remove_dir_all`.
    BeforeRemoveDirAll,
    /// Immediately before a size-walk child's `lstat` (the longest window).
    BeforeChildLstat,
    /// After the ENTIRE enumeration loop has finished (every candidate read,
    /// lstat'ed and sized) and before the discard loop takes its first
    /// [`EXECROOT_OWNERSHIP`] claim. This is a RENDEZVOUS point, not a vanish
    /// point: it exists so a test can know the pass has actually enumerated,
    /// instead of sleeping and hoping it has.
    AfterEnumeration,
}

/// Rendezvous for [`ProbePoint::AfterEnumeration`]. `Mutex::new`/`Condvar::new`
/// are both `const`, so these need no `LazyLock`.
#[cfg(test)]
static ENUMERATION_DONE: Mutex<bool> = Mutex::new(false);
#[cfg(test)]
static ENUMERATION_DONE_CV: std::sync::Condvar = std::sync::Condvar::new();

#[cfg(test)]
static VANISH_PROBE: Mutex<Option<fn(ProbePoint, &Path)>> = Mutex::new(None);

#[cfg(test)]
fn fire_vanish_probe(point: ProbePoint, path: &Path) {
    let probe = *VANISH_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(probe) = probe {
        probe(point, path);
    }
}

// ---------------------------------------------------------------------------
// §8 CROSS-PROCESS PER-VICTIM LEASE (design A3; FL-1383 D5).
//
// ★ THIS PROTOCOL IS NOT NEW AND MUST NOT BE RE-INVENTED HERE. Three actors
// already share `<FIXED_PREFIX>` on a machine, and TWO of them already speak
// `flock(2)` on the sibling `<targetkey>.lock`:
//
//   1. the rules_rust `process_wrapper` local branch — `incr_execroot.rs`
//      `acquire_lease()`: `open(<targetkey>.lock, O_CREAT|O_WRONLY)` — note it
//      does NOT pass `O_NOFOLLOW`, where this module and the reaper both do —
//      `flock(LOCK_EX|LOCK_NB)`, held for the whole build by the `Session`,
//      released by the KERNEL on close (including SIGKILL), and the lock file is
//      DELIBERATELY NEVER UNLINKED;
//   2. the out-of-repo reaper `bld/fl-incr-execroot-reaper.zsh` — the same
//      `flock(LOCK_EX|LOCK_NB)` on the same path, per-victim, non-blocking,
//      skip-on-contention (`KEEP … reason=lease-held`);
//   3. this worker, which until now took only an in-process
//      `Mutex<HashMap<…>>` and was the one actor that did not participate.
//
// ★ SCOPE, because "the worker now participates" would be too strong: only the
// worker's EVICTION participates. The worker does NOT hold the flock for the
// duration of an ACTION the way the local branch's `Session` does, so a live
// worker build is still not protected BY THIS from an actor that honours the
// lease. Closing that is a design step, not an addition — see
// `#fl1383-worker-holds-the-execroot-lease`.
//
// The lock lives on the OPEN FILE DESCRIPTION, which is why closing the fd (or
// dying) releases it and why the file must never be unlinked: a racing holder of
// an unlinked lock file and a holder of a freshly-created one are locking
// DIFFERENT inodes and exclude nothing — the two-holders shape behind the
// torn-`.rlib` finding (FL-1383 red-team B1) that retired the earlier
// O_EXCL-with-TTL-steal scheme.
//
// What this CANNOT exclude: actors that take no lock at all —
// `bld/incr-reuse-ci-gate.zsh:535` (`rm -rf <dir> <dir>.lock`, no lease), the
// option-M stash hook, manual cleanup, crash recovery. The ENOENT tolerance
// above therefore stays; see `EvictionOutcome::vanished_skipped`.
// ---------------------------------------------------------------------------

/// The sibling lease path for a warm OWNER execroot: `<FIXED_PREFIX>/<key>.lock`.
///
/// ★ APPENDS `.lock`; it is NOT `Path::with_extension("lock")`, which REPLACES a
/// trailing `.<ext>`. The two agree on a 64-hex owner name (no dot) and disagree
/// on a CONTENDER name `<key>.<uuid>`, where `with_extension` would silently
/// return the OWNER's lock path — i.e. one contender would lease, and gate the
/// deletion of, a different action's warm dir. Pinned by
/// `owner_lock_path_appends_and_never_replaces_a_suffix`.
fn owner_lock_path(execroot: &Path) -> PathBuf {
    let mut name = execroot
        .file_name()
        .map_or_else(OsString::new, std::ffi::OsStr::to_os_string);
    name.push(".lock");
    execroot.with_file_name(name)
}

/// The ONLY `flock(2)` failure that means "someone else holds this lease":
/// `EWOULDBLOCK`. Matched through [`std::io::ErrorKind::WouldBlock`] — the same
/// shape [`is_vanished`] uses — because `EWOULDBLOCK` and `EAGAIN` are the SAME
/// VALUE on both darwin and linux, so spelling them as two match arms does not
/// widen the predicate, it only fails to compile cleanly. Every other errno is a
/// REAL FAULT.
///
/// ★ FUNNELLED FOR THE SAME REASON AS [`is_vanished`], and after the same
/// mistake: the ENOENT fix-up found FIVE of six inline errno tests could be
/// widened to swallow every errno with the whole suite green, and every
/// uncovered arm failed toward silently skipping — i.e. the disk-growth guard
/// failed OPEN. A lease predicate that treated `EIO` as contention would read an
/// I/O-failing volume as "every dir is busy, evict nothing" while the disk
/// filled. One predicate, one test over CONSTRUCTED errnos
/// (`only_ewouldblock_is_lease_contention`).
fn is_lease_contended(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::WouldBlock
}

/// The outcome of trying to take the cross-process lease on one eviction victim.
#[derive(Debug)]
enum VictimLease {
    /// Acquired. The fd MUST outlive the discard: dropping it releases the lock.
    Acquired(OwnedFd),
    /// Another PROCESS holds it — a live local-branch build, or the reaper
    /// mid-eviction. Skip the victim, exactly as an in-process lease does.
    Contended,
    /// `<key>.lock` does not exist, so there is no lease record to take. Skip:
    /// see [`EvictionOutcome::no_lease_record_skipped`] for why creating one
    /// here would be unsafe rather than convenient.
    NoRecord,
}

/// Try to take `flock(LOCK_EX | LOCK_NB)` on `<execroot>.lock` for the duration
/// of one victim's discard.
///
/// Opened `O_RDWR | O_NOFOLLOW | O_CLOEXEC` and **without `O_CREAT`** — the same
/// flags the reaper's `sysopen(O_RDWR|O_NOFOLLOW)` uses, for the same two
/// reasons: a symlink at the lock path must not be followed out of the prefix,
/// and a MISSING record must fail closed rather than mint a second lock inode.
/// BLOCKING (open/flock/close).
fn try_acquire_victim_lease(execroot: &Path) -> Result<VictimLease, Error> {
    let lock_path = owner_lock_path(execroot);
    let c_path = path_to_cstring(&lock_path)?;
    let flags = libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: valid NUL-terminated path; no `O_CREAT`, so the variadic mode
    // argument is not read.
    let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
    if fd < 0 {
        let e = std::io::Error::last_os_error();
        if is_vanished(&e) {
            return Ok(VictimLease::NoRecord);
        }
        return Err(make_err!(
            Code::Internal,
            "portable_incr: open lease record {} for eviction: {e}",
            lock_path.display()
        ));
    }
    // SAFETY: `fd` is a freshly-opened, owned, valid file descriptor. Owned from
    // here on, so every path below (including the error return) closes it.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: `owned` is a live fd; `flock` is non-blocking here (`LOCK_NB`).
    let rc = unsafe { libc::flock(owned.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(VictimLease::Acquired(owned));
    }
    let e = std::io::Error::last_os_error();
    if is_lease_contended(&e) {
        return Ok(VictimLease::Contended);
    }
    Err(make_err!(
        Code::Internal,
        "portable_incr: flock lease record {} for eviction: {e}",
        lock_path.display()
    ))
}

/// Create `<execroot>.lock` if it is absent, so the cross-process lease has a
/// record to take. Idempotent, never truncates, and NEVER unlinks.
///
/// ★ THE COMPANION CHANGE THAT MAKES A3 WORK, AND IT ALSO REPAIRS A SECOND
/// ACTOR. The reaper opens the lease record WITHOUT `O_CREAT` and, on a missing
/// one, emits `REFUSE reason=no-lease-record` and exits 4 — so every
/// worker-created execroot that ages past `--days` is permanently un-reapable
/// AND reddens that daily job the moment a crow-agent is provisioned on a worker
/// host. The worker creating the record it already relies on fixes both sides.
///
/// Called for OWNER execroots only: a contender dir is per-action, isolated, and
/// cold-discarded, so it is never an eviction candidate (the pass enumerates
/// 64-hex names) and never a reaper candidate (`^[0-9a-f]{64}$`). BLOCKING.
fn ensure_owner_lock_file(execroot: &Path) -> Result<(), Error> {
    let lock_path = owner_lock_path(execroot);
    let c_path = path_to_cstring(&lock_path)?;
    // `O_CREAT` WITHOUT `O_EXCL`: an existing record — possibly flocked right
    // now by a live builder — must be left exactly as it is. `O_NOFOLLOW`
    // refuses a symlink planted at the path rather than creating through it.
    let flags = libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: valid NUL-terminated path; `open` is variadic in `mode` for
    // `O_CREAT`, and `0o644` is ABI-compatible with the `c_uint` it expects.
    let fd = unsafe { libc::open(c_path.as_ptr(), flags, 0o644) };
    if fd < 0 {
        return Err(make_err!(
            Code::Internal,
            "portable_incr: create lease record {}: {}",
            lock_path.display(),
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: freshly-opened owned fd; closing it does NOT remove the file and
    // does not disturb any `flock` another process holds on the same inode.
    drop(unsafe { OwnedFd::from_raw_fd(fd) });
    Ok(())
}

/// BACKFILL: create the missing lease record for every warm OWNER dir already in
/// `fixed_prefix`. Returns the number of records created.
///
/// ★ WHY A BACKFILL IS NEEDED AND NOT MERELY TIDY. Eviction fails CLOSED on a
/// missing record, and the LRU victim is by definition the dir that has NOT been
/// used recently — i.e. exactly the dir that [`ensure_owner_lock_file`]'s
/// create-on-use will never reach. Without this, the first pass after this
/// change ships would refuse precisely the dirs it most needs to evict, and the
/// disk-growth guard would be inert until every warm dir happened to be rebuilt.
///
/// ★ THE PRECONDITION THIS NEEDS, STATED HONESTLY — AND A CORRECTION. An earlier
/// version of this comment claimed the backfill is unconditionally safe against a
/// live pool because "a record can only be absent when no process holds a lease
/// on it (a holder creates it first and never unlinks it)". **That is FALSE, and
/// it is refuted by a measured, reproduced experiment in the header of the very
/// script this module cites as its authority** — `bld/fl-incr-execroot-reaper.zsh`,
/// ★ THE LEASE point 3: *"control reports `KEEP reason=lease-held`; after only
/// `rm -f *.lock`, the same fixture reports `EVICT` and the live build's execroot
/// is deleted."* **Unlinking a lock file does not release the `flock` on it.**
/// "No record" means "nobody created one *that still has this name*", which is
/// not the same as "no holder".
///
/// So this is the ONE path in this module that can mint a FRESH INODE at a name a
/// live holder's open file description is still locked to — the two-holders shape
/// (FL-1383 red-team B1) that the `NoRecord` arm deliberately refuses to create at
/// eviction time. The real precondition:
///
/// > The backfill is safe iff no other process is building against this prefix.
/// > It is unsafe after any actor unlinks a `.lock` while a holder lives — which
/// > the reaper's header records as OBSERVED, with `rm -f *.lock` under disk
/// > pressure named as the realistic trigger (ci-mac-1 carries 7,066 orphan
/// > records, which invites exactly that cleanup).
///
/// It holds today because no crow-agent — hence no local-branch builder and no
/// reaper — is provisioned on the worker hosts. It stops holding on one
/// provisioning decision, and note this is NOT a one-time migration: it re-runs
/// every startup and creates only what is absent, so it re-arms on precisely the
/// trigger the reaper documents. The complete fix is for the worker to hold the
/// lease for the ACTION's lifetime; that is a design step, not an addition, and
/// it is tracked as `#fl1383-worker-holds-the-execroot-lease`.
///
/// Called ONCE at startup alongside the §8 contender sweep. BLOCKING.
fn ensure_owner_lock_files_at(fixed_prefix: &Path) -> Result<usize, Error> {
    let entries = std::fs::read_dir(fixed_prefix).map_err(|e| {
        make_err!(
            Code::Internal,
            "portable_incr: read FIXED_PREFIX {} for lease-record backfill: {e}",
            fixed_prefix.display()
        )
    })?;
    let mut created = 0usize;
    for entry in entries {
        let entry = entry.map_err(|e| {
            make_err!(
                Code::Internal,
                "portable_incr: read FIXED_PREFIX entry in {}: {e}",
                fixed_prefix.display()
            )
        })?;
        let name = entry.file_name();
        // Owner warm dirs ONLY — the same 64-hex predicate the eviction pass and
        // the reaper's `^[0-9a-f]{64}$` candidate filter use.
        if !is_hex64(&name.to_string_lossy()) {
            continue;
        }
        let path = entry.path();
        let md = match std::fs::symlink_metadata(&path) {
            Ok(md) => md,
            // Raced away between readdir and lstat: nothing to give a record to.
            Err(e) if is_vanished(&e) => continue,
            Err(e) => {
                return Err(make_err!(
                    Code::Internal,
                    "lstat backfill candidate {}: {e}",
                    path.display()
                ));
            }
        };
        if !md.file_type().is_dir() {
            continue;
        }
        if owner_lock_path(&path).try_exists().unwrap_or(false) {
            continue;
        }
        ensure_owner_lock_file(&path)?;
        created += 1;
    }
    Ok(created)
}

/// Free-function form of [`PortableIncrContext::evict_warm_dirs_over_budget`].
/// BLOCKING — call under `spawn_blocking`.
fn evict_warm_dirs_over_budget_at(
    fixed_prefix: &Path,
    budget_bytes: u64,
) -> Result<EvictionOutcome, Error> {
    // Enumerate the warm OWNER dirs (exactly-64-hex names) and their sizes +
    // recency. Contender dirs and probe files are NOT part of the persistent
    // pool (contenders are per-action + cold-discarded/swept), so they are not
    // eviction candidates here.
    let entries = std::fs::read_dir(fixed_prefix).map_err(|e| {
        make_err!(
            Code::Internal,
            "portable_incr: read FIXED_PREFIX {} for eviction: {e}",
            fixed_prefix.display()
        )
    })?;
    // CAPPED AT (concurrent-portable-targetkey count on THIS machine): one entry
    // per distinct warm `targetkey` dir on local disk — bounded by the
    // allowlisted-crate set, not a network-driven buffer; holds only PathBufs,
    // no owned payload bytes, off the durability/data path.
    let mut candidates: Vec<(PathBuf, u64, SystemTime)> = Vec::new();
    let mut total: u64 = 0;
    // Candidates that disappeared under us at ANY point in this pass (§5 TOCTOU
    // with a concurrent remover). Counted, not fatal — see `vanished_skipped`.
    let mut vanished_skipped: usize = 0;
    for entry in entries {
        let entry = entry.map_err(|e| {
            make_err!(
                Code::Internal,
                "portable_incr: read FIXED_PREFIX entry in {}: {e}",
                fixed_prefix.display()
            )
        })?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if !is_hex64(&name_str) {
            continue;
        }
        let path = entry.path();
        #[cfg(test)]
        fire_vanish_probe(ProbePoint::BeforeEnumLstat, &path);
        let md = match std::fs::symlink_metadata(&path) {
            Ok(md) => md,
            // ★ VANISHED between readdir and lstat. A warm dir that no longer
            // exists is the END STATE THIS PASS IS TRYING TO REACH, reached by
            // someone else — skip the candidate and keep going. Before this,
            // one such dir aborted the whole pass with `Code::Internal`.
            //
            // The window here is NOT the microseconds between this dirent and
            // this lstat: it is the whole preceding SIZE WALK of every earlier
            // candidate — seconds. That is why this is the largest observed
            // shape (405 of 722 fleet aborts) even though the syscall pair looks
            // adjacent.
            Err(e) if is_vanished(&e) => {
                vanished_skipped += 1;
                continue;
            }
            Err(e) => {
                return Err(make_err!(
                    Code::Internal,
                    "lstat warm dir {}: {e}",
                    path.display()
                ));
            }
        };
        if !md.file_type().is_dir() {
            continue;
        }
        // Vanished during its own size walk — same disposition as above.
        let Some(size) = dir_apparent_size_bytes(&path)? else {
            vanished_skipped += 1;
            continue;
        };
        // The dir's own mtime is the LRU signal: the §7 wipe adds/removes direct
        // children every build, so a recently-built warm dir has a recent mtime.
        let mtime = md.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        total = total.saturating_add(size);
        candidates.push((path, size, mtime));
    }

    #[cfg(test)]
    fire_vanish_probe(ProbePoint::AfterEnumeration, fixed_prefix);

    let mut outcome = EvictionOutcome {
        bytes_remaining: total,
        vanished_skipped,
        ..EvictionOutcome::default()
    };
    if total <= budget_bytes {
        return Ok(outcome);
    }

    // Least-recently-used first.
    candidates.sort_by_key(|(_, _, mtime)| *mtime);
    for (path, size, _mtime) in candidates {
        if total <= budget_bytes {
            break;
        }
        // Atomic claim via the §5 lease registry: we proceed ONLY if the slot is
        // vacant. This closes the TOCTOU with `plan`: a concurrent
        // same-`targetkey` action sees our sentinel and becomes an isolated
        // CONTENDER (its own dir) rather than racing the delete of this warm dir.
        let existing_holder = match EXECROOT_OWNERSHIP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(path.clone())
        {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(OwnershipHolder::EvictionSentinel);
                None
            }
            std::collections::hash_map::Entry::Occupied(held) => Some(*held.get()),
        };
        match existing_holder {
            None => {}
            // Never evict a live warm dir (§5 backpressure). Its bytes are
            // STAYING, so they correctly remain in `total`.
            Some(OwnershipHolder::LiveOwner) => {
                outcome.leased_skipped += 1;
                continue;
            }
            // ★ A PEER EVICTION PASS is mid-`remove_dir_all` on this exact dir.
            // Its bytes are LEAVING, so they must leave `total` too — otherwise
            // this pass believes it is still short and evicts an EXTRA live LRU
            // dir to cover a shortfall the peer is already covering. This is the
            // same reasoning `AlreadyGone` gets below; before the holder tag
            // existed, a bare `HashSet` made this indistinguishable from a live
            // owner and it silently took the "bytes stay" branch.
            Some(OwnershipHolder::EvictionSentinel) => {
                total = total.saturating_sub(size);
                outcome.peer_pass_skipped += 1;
                continue;
            }
        }
        // RAII: releases our delete-sentinel from the lease set on drop, even if
        // `discard_dir_tree_confined` errors.
        let _sentinel = OwnershipLeaseGuard {
            owned: Some(path.clone()),
        };
        // ★ CROSS-PROCESS LEASE (A3), taken AFTER the in-process claim and HELD
        // ACROSS THE DISCARD. Order is deliberate: the in-process registry is a
        // memory read that already excludes our own live owners, so the syscall
        // is only paid for a candidate we are genuinely about to delete. The fd
        // must stay alive until `remove_dir_all` returns — dropping it releases
        // the lock — so it is bound here and not in a temporary.
        //
        // NOTE the lock file is a SIBLING of the victim, not a child, so
        // `remove_dir_all` below does not remove it. That is required, not
        // incidental: this module must NEVER unlink a `.lock`.
        let _victim_lease = match try_acquire_victim_lease(&path)? {
            VictimLease::Acquired(fd) => fd,
            // Another PROCESS is using this execroot. Same disposition as an
            // in-process lease: its bytes STAY, so they stay in `total`.
            VictimLease::Contended => {
                outcome.xproc_leased_skipped += 1;
                continue;
            }
            // No lease record ⇒ nothing proves the tree is unowned ⇒ do not
            // delete it. Bytes stay in `total`, so a pool wedged this way
            // surfaces as `still_over_budget`.
            VictimLease::NoRecord => {
                outcome.no_lease_record_skipped += 1;
                continue;
            }
        };
        match discard_dir_tree_confined(&path, fixed_prefix)? {
            DiscardOutcome::Removed => {
                total = total.saturating_sub(size);
                outcome.dirs_evicted += 1;
                outcome.bytes_freed = outcome.bytes_freed.saturating_add(size);
            }
            // ★ Removed by someone else between enumeration and here. The bytes
            // ARE off the disk, so they leave the running total (not doing so
            // would over-evict live dirs to make up an imaginary shortfall), but
            // we did NOT free them, so `dirs_evicted`/`bytes_freed` — the
            // "eviction is working" signal — are not credited.
            DiscardOutcome::AlreadyGone => {
                total = total.saturating_sub(size);
                outcome.vanished_skipped += 1;
            }
        }
    }

    outcome.bytes_remaining = total;
    outcome.still_over_budget = total > budget_bytes;
    Ok(outcome)
}

/// Outcome of one confined discard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiscardOutcome {
    /// The tree existed and this call removed it.
    Removed,
    /// The tree was already gone (ENOENT) — the desired end state, reached by a
    /// concurrent remover. NOT an error, and NOT credited as our removal.
    AlreadyGone,
}

/// Recursively remove `path` after checking it canonicalizes STRICTLY under
/// `fixed_prefix` (§9 containment). Used by both the startup sweep (contender
/// discard) and eviction (cold warm-dir removal).
///
/// ENOENT — at the containment gate or at the removal itself — is
/// [`DiscardOutcome::AlreadyGone`], because "this tree does not exist" is
/// exactly the postcondition this function is asked to establish. A containment
/// violation and every other errno remain hard errors. BLOCKING.
fn discard_dir_tree_confined(path: &Path, fixed_prefix: &Path) -> Result<DiscardOutcome, Error> {
    match check_under_prefix(path, fixed_prefix)? {
        Containment::Vanished => return Ok(DiscardOutcome::AlreadyGone),
        Containment::Confined => {}
    }
    #[cfg(test)]
    fire_vanish_probe(ProbePoint::BeforeRemoveDirAll, path);
    // ★ THE REMOVAL DELIBERATELY USES `path`, NOT `canon_path`, AND ITS SAFETY
    // AGAINST A POST-GATE SYMLINK SWAP COMES FROM std, NOT FROM THIS CODE.
    // There is a real window between the containment gate above and this call.
    // It is closed only because Rust's unix `remove_dir_all` `lstat`s first and
    // UNLINKS a top-level symlink rather than following it, then descends with
    // `openat(… O_NOFOLLOW | O_DIRECTORY)` so interior components cannot be
    // swapped either (`library/std/src/sys/fs/unix.rs`). A reviewer verified the
    // property empirically by swapping the candidate for a symlink-to-outside in
    // exactly this gap: the outside tree survived.
    //
    // CONSEQUENCE: switching to `remove_dir_all(&canon_path)` or to a
    // hand-rolled recursive walker would SILENTLY delete this guarantee. If you
    // change this line, the airtight form is one `openat` fd used for both the
    // containment check and the removal.
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(DiscardOutcome::Removed),
        // Removed between the containment gate and here — same benign race.
        Err(e) if is_vanished(&e) => Ok(DiscardOutcome::AlreadyGone),
        Err(e) => Err(make_err!(
            Code::Internal,
            "portable_incr: discard dir tree {}: {e}",
            path.display()
        )),
    }
}

/// Apparent on-disk size (sum of regular-file lengths) of the tree rooted at
/// `dir`, NOT following symlinks (a symlink child contributes its own small link
/// length, never its target). Apparent size slightly OVER-counts hardlinked
/// inputs vs physical blocks, so the budget errs toward evicting sooner — the
/// conservative direction for a disk-growth guard.
///
/// Returns `Ok(None)` iff `dir` ITSELF no longer exists (ENOENT) — the candidate
/// vanished and has no size to contribute.
///
/// ★ THIS WALK IS THE LONGEST-LIVED RACE WINDOW IN AN EVICTION PASS: it descends
/// the whole tree of EVERY candidate, including dirs a live owner is actively
/// building into (the §7 wipe and rustc both add and remove children under
/// `bazel-out/` continuously), and it runs before the lease check that would
/// exclude them. A child that disappears mid-walk therefore contributes 0 and
/// the walk CONTINUES: the total is an estimate feeding a budget heuristic, and
/// an under-count by one already-deleted file is strictly better than aborting
/// the pass. Every non-ENOENT error still aborts. BLOCKING.
fn dir_apparent_size_bytes(dir: &Path) -> Result<Option<u64>, Error> {
    let mut total: u64 = 0;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if is_vanished(&e) => return Ok(None),
        Err(e) => {
            return Err(make_err!(
                Code::Internal,
                "read dir {} for sizing: {e}",
                dir.display()
            ));
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            // The directory (or the entry) went away mid-iteration.
            Err(e) if is_vanished(&e) => continue,
            Err(e) => {
                return Err(make_err!(
                    Code::Internal,
                    "read dir entry in {} for sizing: {e}",
                    dir.display()
                ));
            }
        };
        let path = entry.path();
        #[cfg(test)]
        fire_vanish_probe(ProbePoint::BeforeChildLstat, &path);
        let md = match std::fs::symlink_metadata(&path) {
            Ok(md) => md,
            // Unlinked between readdir and lstat — contributes 0.
            Err(e) if is_vanished(&e) => continue,
            Err(e) => {
                return Err(make_err!(
                    Code::Internal,
                    "lstat {} for sizing: {e}",
                    path.display()
                ));
            }
        };
        if md.file_type().is_dir() {
            // A subtree that vanished mid-descent contributes 0; the parent is
            // still being sized, so this is NOT propagated as `None`.
            total = total.saturating_add(dir_apparent_size_bytes(&path)?.unwrap_or(0));
        } else {
            total = total.saturating_add(md.len());
        }
    }
    Ok(Some(total))
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

/// FULL-EMPTY content-wipe `dir` in place, removing EVERY direct child (design
/// §7 — nothing is preserved). The delete is confined to a subtree of
/// `fixed_prefix` (design §9): `dir` MUST canonicalize under `fixed_prefix`.
/// Symlink children are unlinked (never followed). BLOCKING.
fn wipe_all_contents(dir: &Path, fixed_prefix: &Path) -> Result<(), Error> {
    // Containment FIRST — never read/delete under a dir that escapes the prefix.
    assert_under_prefix(dir, fixed_prefix)?;

    let entries = std::fs::read_dir(dir)
        .map_err(|e| make_err!(Code::Internal, "read execroot {} for wipe: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry
            .map_err(|e| make_err!(Code::Internal, "read execroot entry in {}: {e}", dir.display()))?;
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

// ---------------------------------------------------------------------------
// §8 eviction TOCTOU unit tests.
//
// These live in-module (not in `tests/portable_incr_execroot_test.rs`) because
// they drive the PRIVATE `evict_warm_dirs_over_budget_at` and the PRIVATE
// `EXECROOT_OWNERSHIP` lease mutex. That mutex is what makes the mid-pass
// vanish DETERMINISTIC rather than a sleep-and-hope race: the eviction loop
// must acquire it before it can discard ANY candidate, so a test holding it has
// an airtight guarantee that no discard has yet occurred when it removes a
// candidate from underneath the pass.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod eviction_toctou_tests {
    use super::*;

    /// A warm OWNER dir at `<root>/<key>` whose apparent size is exactly
    /// `filler_bytes` (one regular file; the dir itself contributes nothing),
    /// WITH the sibling `<key>.lock` lease record — the production shape, since
    /// `ensure_and_wipe_execroot_at` creates one for every owner execroot.
    ///
    /// The record is created by the PRODUCTION function so the fixture cannot
    /// drift from it. The path itself is pinned independently by
    /// `owner_lock_path_appends_and_never_replaces_a_suffix`, so a bug in
    /// `owner_lock_path` cannot hide behind a fixture that shares it.
    fn make_warm_dir(root: &Path, key: &str, filler_bytes: usize) {
        make_warm_dir_without_lease(root, key, filler_bytes);
        ensure_owner_lock_file(&root.join(key)).expect("mk lease record");
    }

    /// A warm OWNER dir with NO lease record — the pre-A3 on-disk shape, and the
    /// fixture for the fail-closed path.
    fn make_warm_dir_without_lease(root: &Path, key: &str, filler_bytes: usize) {
        let d = root.join(key);
        std::fs::create_dir_all(&d).expect("mk warm dir");
        std::fs::write(d.join("filler"), vec![0u8; filler_bytes]).expect("filler");
    }

    /// Pin a warm dir's own mtime — the eviction's LRU key — so victim ORDER is
    /// decided by the fixture and not by the order the dirs happened to be made.
    fn set_warm_dir_mtime(dir: &Path, unix_secs: i64) {
        filetime::set_file_mtime(dir, filetime::FileTime::from_unix_time(unix_secs, 0))
            .expect("set warm dir mtime");
    }

    fn hex64(c: char) -> String {
        std::iter::repeat_n(c, 64).collect()
    }

    /// A canonicalized temp root (macOS `/var` -> `/private/var`), so the
    /// containment comparison is against a stable prefix.
    fn canonical_tempdir() -> (tempfile::TempDir, PathBuf) {
        let td = tempfile::tempdir().expect("tempdir");
        let root = std::fs::canonicalize(td.path()).expect("canonicalize tempdir");
        (td, root)
    }

    /// ★ THE REGRESSION THIS FILE EXISTS FOR.
    ///
    /// Measured 2026-08-13T18:30Z over the LIVE `~/Library/Logs/nativelink-worker.log`
    /// on all 10 workers: 722 eviction passes aborted vs 1141 that evicted
    /// something — 38.8% — and 722/722 of the aborts were `os error 2` (ENOENT).
    /// Zero other errnos fleet-wide. Including the rotated `.gz` logs the counts
    /// are 12082 vs 16298 (42.6%), so the RATE is stable across windows even
    /// though the absolute counts depend on which window you read.
    ///
    /// NOTE the denominator: a pass that finds the pool WITHIN budget logs at
    /// `debug!`, which `release_max_level_info` compiles out of the shipped
    /// worker. So this is the failure rate among passes that DID work, not among
    /// all passes — the true per-invocation rate is lower and is not observable
    /// from the logs at all (see the eviction-observability gap, FL-1383 §12).
    ///
    /// A candidate removed BETWEEN enumeration and eviction must be skipped and
    /// the pass must CONTINUE to evict the remaining candidates. Before the fix
    /// this returned `Err(Code::Internal)` and the whole pass was lost.
    ///
    /// Determinism: the test holds `EXECROOT_OWNERSHIP` across the removal, and
    /// the eviction loop cannot discard anything without it. The removal is
    /// therefore guaranteed to land before any discard. Whether the pass sees
    /// the vanish at the enumeration lstat, at the size walk, or at the discard
    /// containment gate depends on how far it got before blocking — ALL THREE
    /// ARE THE FIX, and the budget below is chosen so all three produce
    /// identical asserted outcomes.
    #[test]
    fn eviction_continues_when_a_candidate_vanishes_mid_pass() {
        // Shares the serialization of the probe tests: their probe is a GLOBAL
        // and would otherwise fire against this test's candidates too.
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let (key_a, key_b, key_c) = (hex64('a'), hex64('b'), hex64('c'));
        for key in [&key_a, &key_b, &key_c] {
            make_warm_dir(&root, key, 50);
        }

        // Barrier: taken BEFORE the pass starts, released only after key_a is
        // gone. No discard can occur in between.
        let barrier = EXECROOT_OWNERSHIP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // ★ RENDEZVOUS, NOT A SLEEP. This test used to `sleep(250ms)` here and
        // claimed "correctness does not depend on this sleep — the mutex does".
        // That was WRONG, and a reviewer proved it by deleting the sleep: the
        // test failed with `vanished_skipped: 0`. The barrier guarantees no
        // DISCARD precedes the removal; it does NOT guarantee the pass thread has
        // reached `read_dir`. On a loaded box — exactly where CI runs — the
        // candidate could be removed before it was ever enumerated, so it never
        // appeared as a dirent and nothing was counted. CLAUDE.md forbids
        // sleep-as-synchronization; this waits for the pass to SAY it has
        // finished enumerating.
        *ENUMERATION_DONE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(|point, _path: &Path| {
                if point == ProbePoint::AfterEnumeration {
                    *ENUMERATION_DONE
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
                    ENUMERATION_DONE_CV.notify_all();
                }
            });

        let root_for_pass = root.clone();
        let pass = std::thread::spawn(move || {
            // Budget 40 < any single dir (50), so the loop consumes ALL
            // candidates regardless of LRU order.
            evict_warm_dirs_over_budget_at(&root_for_pass, 40)
        });

        // Wait until every candidate (including key_a) has been read, lstat'ed
        // and sized. The deadline is a DEADLOCK DETECTOR with a specific
        // message, not a synchronisation device — it can only fire if the pass
        // never reaches the end of enumeration.
        {
            let mut done = ENUMERATION_DONE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*done {
                let (guard, timeout) = ENUMERATION_DONE_CV
                    .wait_timeout(done, std::time::Duration::from_secs(30))
                    .unwrap_or_else(|e| e.into_inner());
                done = guard;
                assert!(
                    !timeout.timed_out() || *done,
                    "the eviction pass never reached the end of enumeration within 30s \
                     — it is wedged, most likely on the EXECROOT_OWNERSHIP barrier \
                     moving above the enumeration loop"
                );
            }
        }
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;

        std::fs::remove_dir_all(root.join(&key_a)).expect("remove candidate mid-pass");
        drop(barrier);

        let outcome = pass
            .join()
            .expect("eviction thread must not panic")
            .expect("★ a vanished candidate must NOT abort the pass");

        assert_eq!(
            outcome.vanished_skipped, 1,
            "the vanished candidate must be COUNTED, not silently dropped"
        );
        assert_eq!(
            outcome.dirs_evicted, 2,
            "★ the pass must CONTINUE and evict the two surviving candidates"
        );
        assert_eq!(
            outcome.bytes_freed, 100,
            "only the two dirs WE removed are credited; the vanished one is not"
        );
        assert_eq!(
            outcome.bytes_remaining, 0,
            "★ the vanished candidate's bytes must LEAVE the running total — they \
             are really off disk, and not subtracting them makes the pass believe \
             it is still over budget and evict LIVE dirs to cover an imaginary \
             shortfall"
        );
        assert_eq!(
            outcome.leased_skipped, 0,
            "no candidate was leased by a live owner in this fixture — a non-zero \
             count here means the vanish was misclassified as a lease"
        );
        assert!(!root.join(&key_b).exists(), "surviving candidate B evicted");
        assert!(!root.join(&key_c).exists(), "surviving candidate C evicted");
    }

    /// Serializes the tests that install the global [`VANISH_PROBE`].
    static PROBE_LOCK: Mutex<()> = Mutex::new(());

    /// Probe body: makes candidate `aaa…` vanish at whichever point is armed.
    /// A plain `fn` pointer (no captures), so it needs to recognise its target
    /// from the path it is handed.
    fn vanish_candidate_a(_point: ProbePoint, path: &Path) {
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("aaaa"))
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }

    /// Installs the probe, runs an eviction over three 50-byte candidates with a
    /// budget of 40 (so the loop must consume ALL of them, making the assertions
    /// independent of LRU/readdir order), and returns the outcome.
    /// Returns the `TempDir` so the CALLER keeps it alive for its on-disk
    /// assertions and it is still cleaned up on drop. (It was previously
    /// `mem::forget`-ed to extend its life, which leaked one temp tree per run.)
    fn evict_with_probe_at(point: ProbePoint) -> (tempfile::TempDir, PathBuf, EvictionOutcome) {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (td, root) = canonical_tempdir();
        for key in [hex64('a'), hex64('b'), hex64('c')] {
            make_warm_dir(&root, &key, 50);
        }
        // The probe fires at every point; it is armed for one by construction
        // (each test installs it for the site it is exercising).
        let armed: fn(ProbePoint, &Path) = match point {
            ProbePoint::BeforeEnumLstat => |p, path| {
                if p == ProbePoint::BeforeEnumLstat {
                    vanish_candidate_a(p, path);
                }
            },
            ProbePoint::BeforeRemoveDirAll => |p, path| {
                if p == ProbePoint::BeforeRemoveDirAll {
                    vanish_candidate_a(p, path);
                }
            },
            ProbePoint::BeforeChildLstat => {
                unreachable!("the size-walk probe is installed inline by its own test")
            }
            ProbePoint::AfterEnumeration => {
                unreachable!(
                    "AfterEnumeration is a RENDEZVOUS point, not a vanish point; \
                     it is installed inline by the mid-pass test"
                )
            }
        };
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(armed);
        let result = evict_warm_dirs_over_budget_at(&root, 40);
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let outcome = result.expect("★ a vanished candidate must NOT abort the pass");
        (td, root, outcome)
    }

    /// ★ THE LARGEST FLEET SHAPE: `lstat warm dir <path>` — 405 of the 722
    /// aborts in the live-log window above (the three shapes were 405 + 274 + 43,
    /// summing to exactly 722, which is how we know these are the ONLY shapes).
    /// The candidate disappears between `read_dir` returning its dirent and the
    /// `lstat` of it.
    #[test]
    fn eviction_continues_when_a_candidate_vanishes_before_its_lstat() {
        let (_td, root, outcome) = evict_with_probe_at(ProbePoint::BeforeEnumLstat);
        assert_eq!(outcome.vanished_skipped, 1, "vanished candidate counted");
        assert_eq!(
            outcome.dirs_evicted, 2,
            "★ the pass CONTINUES and evicts the two survivors"
        );
        assert_eq!(outcome.bytes_freed, 100);
        assert_eq!(outcome.bytes_remaining, 0);
        assert!(!root.join(hex64('b')).exists());
        assert!(!root.join(hex64('c')).exists());
    }

    /// The narrowest window: the candidate survives the containment gate and is
    /// removed before `remove_dir_all`. Unobserved on the fleet (0 of 722 — it
    /// has no distinct message, so an occurrence would have surfaced as the
    /// `discard dir tree` text, which appears zero times) but the same race, and
    /// its tolerance must not rot untested.
    #[test]
    fn eviction_continues_when_a_candidate_vanishes_before_removal() {
        let (_td, root, outcome) = evict_with_probe_at(ProbePoint::BeforeRemoveDirAll);
        assert_eq!(
            outcome.vanished_skipped, 1,
            "the vanished candidate must be COUNTED, not silently dropped"
        );
        assert_eq!(
            outcome.dirs_evicted, 2,
            "★ AlreadyGone must NOT be credited as an eviction: only the two dirs \
             THIS pass removed count, or `dirs_evicted` stops meaning \
             'eviction is working' and a pool that is being emptied by someone \
             else looks identical to one we are keeping under budget"
        );
        assert_eq!(
            outcome.bytes_freed, 100,
            "the concurrently-removed dir is NOT credited to this pass"
        );
        assert_eq!(
            outcome.bytes_remaining, 0,
            "★ AlreadyGone bytes must still LEAVE the running total — they are \
             really off disk, and not subtracting them makes the pass believe it \
             is still over budget and evict LIVE dirs to cover an imaginary shortfall"
        );
        assert!(!root.join(hex64('b')).exists());
        assert!(!root.join(hex64('c')).exists());
    }

    /// The counterweight: a NON-ENOENT failure must still abort the whole pass.
    /// An unreadable candidate (EACCES from the size walk) is a real fault —
    /// a permissions regression — and must never be swallowed as "vanished".
    #[test]
    fn eviction_still_aborts_on_a_non_enoent_error() {
        use std::os::unix::fs::PermissionsExt;

        // This test runs a real eviction pass, so the GLOBAL [`VANISH_PROBE`]
        // fires against ITS candidates too — and its EACCES fixture is named
        // `aaaa…`, exactly what the probe body targets. Without this lock a
        // concurrently-installed probe could delete the fixture out from under
        // the assertion and turn the safety check green for the wrong reason.
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let (key_a, key_b) = (hex64('a'), hex64('b'));
        make_warm_dir(&root, &key_a, 50);
        make_warm_dir(&root, &key_b, 50);

        let denied = root.join(&key_a);
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000))
            .expect("chmod 000");

        // Running as root defeats the permission bit; skip rather than assert a
        // falsehood.
        if std::fs::read_dir(&denied).is_ok() {
            std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o755)).ok();
            eprintln!("skipping: euid can read a 0o000 dir (root?)");
            return;
        }

        let err = evict_warm_dirs_over_budget_at(&root, 40)
            .expect_err("★ EACCES is a real fault and MUST abort the pass");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("for sizing"),
            "expected the size-walk error to propagate, got: {msg}"
        );
        assert!(
            !msg.contains("os error 2"),
            "the aborting error must NOT be an ENOENT, got: {msg}"
        );

        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o755)).ok();
    }

    /// The safety distinction the fix must NOT collapse: a target that is gone
    /// is benign; a target that resolves OUTSIDE the prefix is a containment
    /// failure and stays hard — including when it is reached through a symlink,
    /// which is the only way ENOENT-tolerance could have opened a hole.
    #[test]
    fn containment_separates_vanished_from_escaped() {
        let (_td, root) = canonical_tempdir();
        let prefix = root.join("fp");
        std::fs::create_dir_all(prefix.join("child")).expect("mk child");
        let outside = root.join("outside");
        std::fs::create_dir_all(&outside).expect("mk outside");

        // Benign: simply not there.
        assert_eq!(
            check_under_prefix(&prefix.join("gone"), &prefix).expect("ENOENT is not an error"),
            Containment::Vanished
        );
        // ...and the strict entry point is UNCHANGED for its existing callers
        // (`wipe_all_contents`, and the contender discard in
        // `running_actions_manager`), down to the error it produces. Pinning the
        // text keeps `assert_under_prefix` from being quietly re-pointed at the
        // ENOENT-tolerant policy.
        let strict = assert_under_prefix(&prefix.join("gone"), &prefix)
            .expect_err("assert_under_prefix must still treat a missing target as an error");
        assert!(
            format!("{strict:?}").contains("cannot canonicalize discard target"),
            "the strict entry point must still fail AT THE CANONICALIZE, got: {strict:?}"
        );

        // Containment failures stay hard through BOTH entry points. NOTE both
        // targets must EXIST, or they would (correctly) report `Vanished` —
        // "not there" is decided before "not contained", and a path that does
        // not exist is never deleted either way.
        let sibling = root.join("fp-evil");
        std::fs::create_dir_all(&sibling).expect("mk sibling");
        // Assert on the ERROR TEXT, not merely `is_err()`: these must fail as
        // CONTAINMENT violations. A bare `is_err()` would also be satisfied by
        // the gate erroring for some unrelated reason, which is how an escape
        // check rots into a check of something else.
        let self_err = check_under_prefix(&prefix, &prefix)
            .expect_err("the prefix itself is not STRICTLY under the prefix");
        assert!(
            format!("{self_err:?}").contains("escapes FIXED_PREFIX"),
            "the prefix itself must be refused AS AN ESCAPE, got: {self_err:?}"
        );
        let sibling_err = check_under_prefix(&sibling, &prefix)
            .expect_err("a string-prefix sibling is not under the prefix");
        assert!(
            format!("{sibling_err:?}").contains("escapes FIXED_PREFIX"),
            "★ `fp-evil` shares the STRING prefix `fp` but is not under it; this \
             must be an escape, got: {sibling_err:?}"
        );

        // ★ The hole ENOENT-tolerance could have opened: a symlink INSIDE the
        // prefix pointing at a real directory OUTSIDE it. `canonicalize`
        // resolves it, so this must be an ESCAPE, never `Vanished`.
        let escape = prefix.join("escape");
        std::os::unix::fs::symlink(&outside, &escape).expect("symlink");
        let err = check_under_prefix(&escape, &prefix)
            .expect_err("★ a symlink out of the prefix is a containment failure");
        assert!(
            format!("{err:?}").contains("escapes FIXED_PREFIX"),
            "must be reported as an escape, got: {err:?}"
        );

        // And the discard helper refuses it, leaving the outside dir intact.
        assert!(discard_dir_tree_confined(&escape, &prefix).is_err());
        assert!(outside.exists(), "★ the out-of-prefix dir must NOT be deleted");

        // A dangling symlink is ENOENT at canonicalize -> Vanished, and since
        // nothing is deleted, that is safe.
        let dangling = prefix.join("dangling");
        std::os::unix::fs::symlink(root.join("nope"), &dangling).expect("symlink");
        assert_eq!(
            check_under_prefix(&dangling, &prefix).expect("dangling is ENOENT"),
            Containment::Vanished
        );
        assert_eq!(
            discard_dir_tree_confined(&dangling, &prefix).expect("no error"),
            DiscardOutcome::AlreadyGone
        );

        // A FIXED_PREFIX that does not exist is a corrupted root, NOT a race.
        assert!(
            check_under_prefix(&prefix.join("child"), &root.join("no-such-prefix")).is_err(),
            "a missing FIXED_PREFIX must stay a hard error"
        );
    }

    /// The size walk is the longest race window (it descends every candidate,
    /// including dirs a live owner is actively building into). A child removed
    /// mid-walk contributes 0 and the walk continues; a missing ROOT is `None`.
    #[test]
    fn size_walk_tolerates_enoent_but_reports_a_missing_root() {
        let (_td, root) = canonical_tempdir();
        let d = root.join("tree");
        std::fs::create_dir_all(d.join("sub")).expect("mk");
        std::fs::write(d.join("a"), vec![0u8; 10]).expect("a");
        std::fs::write(d.join("sub").join("b"), vec![0u8; 7]).expect("b");
        assert_eq!(
            dir_apparent_size_bytes(&d).expect("walk"),
            Some(17),
            "regular files summed across the tree"
        );

        assert_eq!(
            dir_apparent_size_bytes(&root.join("absent")).expect("missing root is not an error"),
            None,
            "a candidate that vanished has no size to contribute"
        );
    }

    /// ★ THE T1 GUARD. The tolerance predicate is one function precisely so it
    /// can be tested against CONSTRUCTED errors, which is the only way to cover
    /// errnos the filesystem will not hand us on demand: you cannot provoke
    /// EACCES at an enumeration `lstat` without first breaking the `read_dir`
    /// that precedes it, and you cannot provoke EIO at all without a failing
    /// volume. Before the funnel, five of the six sites could be widened to
    /// swallow EVERY errno with the whole suite green.
    #[test]
    fn only_enoent_is_tolerated_as_vanished() {
        use std::io::Error;

        assert!(
            is_vanished(&Error::from_raw_os_error(libc::ENOENT)),
            "★ ENOENT is the ONLY tolerated errno — the object is gone, which is \
             the postcondition every tolerant call site is establishing"
        );

        // Every one of these means the object may still BE there. Tolerating any
        // of them makes the disk-growth guard fail OPEN: the pass under-counts
        // the pool and silently declines to evict.
        for (errno, name) in [
            (libc::EACCES, "EACCES (a permissions regression)"),
            (libc::EIO, "EIO (the realistic errno on the external /Volumes/CrowAgent)"),
            (libc::ELOOP, "ELOOP (symlink cycle)"),
            (libc::ENOTDIR, "ENOTDIR (a path component is not a directory)"),
            (libc::EPERM, "EPERM"),
            (libc::ENAMETOOLONG, "ENAMETOOLONG"),
        ] {
            let err = Error::from_raw_os_error(errno);
            assert!(
                !is_vanished(&err),
                "★ {name} MUST abort the pass, never be classified as vanished — \
                 an {name} storm would otherwise read as \"every candidate \
                 vanished, the pool is empty, nothing to do\" while the disk fills \
                 (got kind {:?})",
                err.kind()
            );
        }
    }

    /// Run `body` with `dir` at `mode`, restoring 0o755 afterwards. Returns
    /// `false` (and skips) when the euid defeats the permission bit — running as
    /// root must not turn a safety test into a passing falsehood.
    fn with_dir_mode(dir: &Path, mode: u32, body: impl FnOnce()) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).expect("chmod");
        // `r` without `x`: readdir succeeds, but resolving any CHILD needs search
        // permission and yields EACCES. This is the fixture that reaches the
        // enumeration `lstat` and the size-walk child `lstat`, which a review
        // judged unreachable ("you cannot produce EACCES at an enumeration lstat
        // without breaking the read_dir before it") — you can, by removing `x`
        // from the PARENT and leaving `r`.
        let effective = std::fs::read_dir(dir).is_ok()
            && std::fs::symlink_metadata(dir.join("probe-nonexistent"))
                .err()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied);
        if effective {
            body();
        }
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).ok();
        if !effective {
            eprintln!("skipping: euid defeats the permission bit on {} (root?)", dir.display());
        }
        effective
    }

    /// ★ PER-SITE T1 COVERAGE, site 1 of 3: the ENUMERATION `lstat`.
    ///
    /// The funnel (`is_vanished`) stops the six copies drifting apart, but a
    /// SITE that bypasses the predicate entirely (`Err(e) if true`) is only
    /// caught by driving a non-ENOENT errno through that exact site. This is the
    /// largest fleet shape (405 of 722), so it is the one that most needs it.
    #[test]
    fn enumeration_lstat_aborts_on_eacces_not_treated_as_vanished() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        make_warm_dir(&root, &hex64('a'), 50);
        make_warm_dir(&root, &hex64('b'), 50);

        with_dir_mode(&root, 0o600, || {
            let err = evict_warm_dirs_over_budget_at(&root, 40)
                .expect_err("★ EACCES at the enumeration lstat MUST abort the pass");
            let msg = format!("{err:?}");
            assert!(
                msg.contains("lstat warm dir"),
                "expected the ENUMERATION lstat to propagate, got: {msg}"
            );
            assert!(
                !msg.contains("os error 2"),
                "★ the aborting error must not be an ENOENT — that would mean the \
                 fixture missed the site it is aiming at, got: {msg}"
            );
        });
    }

    /// ★ PER-SITE T1 COVERAGE, site 2 of 3: the SIZE-WALK CHILD `lstat`
    /// (the `lstat … for sizing` fleet shape).
    #[test]
    fn size_walk_child_lstat_aborts_on_eacces_not_treated_as_vanished() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let key = hex64('a');
        make_warm_dir(&root, &key, 50);
        let candidate = root.join(&key);

        // The PREFIX stays traversable, so enumeration succeeds and the failure
        // lands inside the size walk of this candidate.
        with_dir_mode(&candidate, 0o600, || {
            let err = evict_warm_dirs_over_budget_at(&root, 10)
                .expect_err("★ EACCES inside the size walk MUST abort the pass");
            let msg = format!("{err:?}");
            assert!(
                msg.contains("for sizing"),
                "expected the SIZE-WALK error to propagate, got: {msg}"
            );
            assert!(
                !msg.contains("os error 2"),
                "★ the aborting error must not be an ENOENT, got: {msg}"
            );
        });
    }

    /// ★ PER-SITE T1 COVERAGE: the DISCARD (`remove_dir_all`).
    ///
    /// Fixture: strip `w` from the PREFIX but keep `r-x`. Enumeration, the size
    /// walk and the containment gate all still succeed (they only read and
    /// traverse); only the final `rmdir` needs write permission on the parent,
    /// so EACCES lands on exactly this site and nowhere earlier.
    #[test]
    fn discard_aborts_on_eacces_not_treated_as_already_gone() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        make_warm_dir(&root, &hex64('a'), 50);

        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o500)).expect("chmod");
        // Confirm the fixture actually bites before asserting on it (root defeats
        // it). The probe must be NON-DESTRUCTIVE: a `remove_dir_all` probe would
        // unlink the candidate's contents before failing at the final `rmdir`,
        // destroying the very fixture under test. Creating a directory needs the
        // same `w` on the parent and leaves the candidate untouched.
        let probe = root.join("probe-write");
        let effective = std::fs::read_dir(&root).is_ok()
            && std::fs::create_dir(&probe)
                .err()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied);
        if effective {
            let err = evict_warm_dirs_over_budget_at(&root, 10)
                .expect_err("★ EACCES at the discard MUST abort the pass");
            let msg = format!("{err:?}");
            assert!(
                msg.contains("discard dir tree"),
                "expected the DISCARD error to propagate, got: {msg}"
            );
            assert!(
                !msg.contains("os error 2"),
                "★ an undeletable dir must never be classified AlreadyGone — its \
                 bytes are still on disk and would be subtracted from the pool, \
                 got: {msg}"
            );
        }
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).ok();
        if !effective {
            eprintln!("skipping: euid defeats the write bit (root?)");
        }
    }

    /// ★ PER-SITE T1 COVERAGE, site 3 of 3: the CONTAINMENT GATE's canonicalize.
    /// A target that cannot be resolved because of a PERMISSION fault is not
    /// "vanished" — the tree may well still be there, and calling it gone makes
    /// the disk-growth guard fail open.
    #[test]
    fn containment_gate_aborts_on_eacces_not_treated_as_vanished() {
        let (_td, root) = canonical_tempdir();
        let prefix = root.join("fp");
        std::fs::create_dir_all(prefix.join("child")).expect("mk child");

        with_dir_mode(&prefix, 0o600, || {
            let err = check_under_prefix(&prefix.join("child"), &prefix).expect_err(
                "★ EACCES at the containment gate MUST be a hard error, never Vanished",
            );
            let msg = format!("{err:?}");
            assert!(
                msg.contains("cannot canonicalize discard target"),
                "expected the containment gate to propagate, got: {msg}"
            );
            assert!(
                !msg.contains("os error 2"),
                "★ must not be reported as ENOENT, got: {msg}"
            );
        });
    }

    /// ★ THE T3 GUARD: the log arm is the feature's ONLY interface (964 metric
    /// series on the endpoint, 0 matching `portable_incr`), and deleting the
    /// `vanished_skipped` reader from it previously passed all 234 lib tests.
    #[test]
    fn log_arm_distinguishes_freed_nothing_from_evicted() {
        let evicted = EvictionOutcome {
            dirs_evicted: 2,
            bytes_freed: 100,
            ..EvictionOutcome::default()
        };
        assert_eq!(evicted.log_arm(), EvictionLogArm::Evicted);

        // ★ The shape the distinct arm exists for: nothing evicted, candidates
        // vanished under the pass.
        let all_vanished = EvictionOutcome {
            vanished_skipped: 3,
            ..EvictionOutcome::default()
        };
        assert_eq!(
            all_vanished.log_arm(),
            EvictionLogArm::FreedNothing,
            "★ a pass that freed NOTHING must not report as 'evicted warm dirs' — \
             that is the false credit the accounting discipline exists to prevent"
        );

        // Same for a pass whose candidates were all claimed by a peer pass.
        let all_peer = EvictionOutcome {
            peer_pass_skipped: 2,
            ..EvictionOutcome::default()
        };
        assert_eq!(
            all_peer.log_arm(),
            EvictionLogArm::FreedNothing,
            "★ peer_pass_skipped must also have a READER, or it is another \
             computed-and-unread signal"
        );

        // ...and for the two A3 arms. Every counter this struct adds must reach
        // the log line, which is the feature's ONLY interface.
        let all_xproc = EvictionOutcome {
            xproc_leased_skipped: 2,
            ..EvictionOutcome::default()
        };
        assert_eq!(
            all_xproc.log_arm(),
            EvictionLogArm::FreedNothing,
            "★ a pass that freed nothing because another PROCESS held every \
             lease must not report as 'evicted warm dirs'"
        );
        let all_no_record = EvictionOutcome {
            no_lease_record_skipped: 3,
            ..EvictionOutcome::default()
        };
        assert_eq!(
            all_no_record.log_arm(),
            EvictionLogArm::FreedNothing,
            "★ nor one that refused every candidate for want of a lease record — \
             that is the arm an operator must be able to see, because it means \
             the guard has stopped shrinking the pool"
        );

        // A silent pass stays silent (this arm is `debug!`, compiled out in release).
        assert_eq!(
            EvictionOutcome::default().log_arm(),
            EvictionLogArm::WithinBudget
        );

        // Backpressure outranks everything.
        let wedged = EvictionOutcome {
            still_over_budget: true,
            vanished_skipped: 1,
            dirs_evicted: 1,
            ..EvictionOutcome::default()
        };
        assert_eq!(
            wedged.log_arm(),
            EvictionLogArm::Backpressure,
            "the operator's only warn! must win over the info! arms"
        );
    }

    /// ★ THE D2 GUARD. `EXECROOT_OWNERSHIP` holds two holder kinds that mean
    /// OPPOSITE things about a path's bytes. A live owner's bytes are staying;
    /// an eviction sentinel means a peer pass is deleting the tree right now, so
    /// those bytes are leaving. When one bare `HashSet` conflated them, the
    /// eviction loop took the "bytes stay" branch for a peer's victim and evicted
    /// an EXTRA live LRU dir to cover a shortfall already being covered.
    ///
    /// The two cases are driven through the SAME production function with the
    /// same fixture; only the holder tag differs, and the outcomes must diverge.
    #[test]
    fn peer_eviction_sentinel_subtracts_but_a_live_owner_does_not() {
        // Returns the TempDir so it is cleaned up on drop (no `mem::forget`);
        // this test asserts only on the outcome, never on on-disk state.
        fn run_with_holder(holder: OwnershipHolder) -> (tempfile::TempDir, EvictionOutcome) {
            let (td, root) = canonical_tempdir();
            for key in [hex64('a'), hex64('b'), hex64('c')] {
                make_warm_dir(&root, &key, 50);
            }
            let held = root.join(hex64('a'));
            EXECROOT_OWNERSHIP
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(held.clone(), holder);
            // Budget 40 < any single dir (50), so the loop consumes ALL
            // candidates and the assertions do not depend on LRU order.
            let outcome = evict_warm_dirs_over_budget_at(&root, 40).expect("pass must succeed");
            EXECROOT_OWNERSHIP
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&held);
            (td, outcome)
        }

        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // A LIVE OWNER: its 50 bytes are STAYING, so they must remain in the
        // total and the pool is correctly still over budget.
        let (_owner_td, owner) = run_with_holder(OwnershipHolder::LiveOwner);
        assert_eq!(owner.leased_skipped, 1, "the live owner is skipped");
        assert_eq!(owner.peer_pass_skipped, 0);
        assert_eq!(owner.dirs_evicted, 2);
        assert_eq!(
            owner.bytes_remaining, 50,
            "★ a LIVE owner's bytes must STAY in the total — they are still on disk"
        );
        assert!(
            owner.still_over_budget,
            "★ and the pool is genuinely still over budget, which is the §5 \
             backpressure signal the operator's only warn! keys on"
        );

        // A PEER EVICTION PASS: the same 50 bytes are LEAVING, so they must go.
        let (_peer_td, peer) = run_with_holder(OwnershipHolder::EvictionSentinel);
        assert_eq!(
            peer.peer_pass_skipped, 1,
            "a peer pass's victim is counted separately from a live-owner lease"
        );
        assert_eq!(peer.leased_skipped, 0, "★ a peer pass is NOT a live-owner lease");
        assert_eq!(
            peer.dirs_evicted, 2,
            "the peer's dir is not credited to us; we evicted the other two"
        );
        assert_eq!(
            peer.bytes_remaining, 0,
            "★ a peer pass's bytes must LEAVE the total — otherwise this pass \
             believes it is still short and evicts an EXTRA live LRU dir to cover \
             a shortfall the peer is already covering"
        );
        assert!(
            !peer.still_over_budget,
            "★ and it must NOT raise backpressure: the pool is not over budget \
             once the peer's delete lands"
        );
    }

    /// The observed `lstat <path> for sizing` shape: a CHILD unlinked between
    /// the parent's `readdir` and the child's `lstat`. This is the common case
    /// in production, because the size walk descends dirs a live owner is
    /// actively building into. The child contributes 0 and the walk CONTINUES.
    #[test]
    fn size_walk_skips_a_child_unlinked_mid_walk() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let d = root.join("tree");
        std::fs::create_dir_all(&d).expect("mk");
        std::fs::write(d.join("keep_a"), vec![0u8; 10]).expect("keep_a");
        std::fs::write(d.join("vanishing_child"), vec![0u8; 1000]).expect("vanishing");
        std::fs::write(d.join("keep_b"), vec![0u8; 7]).expect("keep_b");

        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(|point, path: &Path| {
                if point == ProbePoint::BeforeChildLstat
                    && path.file_name().and_then(|n| n.to_str()) == Some("vanishing_child")
                {
                    let _ = std::fs::remove_file(path);
                }
            });
        let size = dir_apparent_size_bytes(&d);
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;

        assert_eq!(
            size.expect("★ an unlinked child must NOT abort the size walk"),
            Some(17),
            "the vanished 1000-byte child contributes 0 and BOTH survivors are still summed"
        );
    }

    // -- A3: the cross-process per-victim lease --------------------------------

    /// ★ `with_extension` WOULD BE A CROSS-TARGET DELETE BUG, so the path helper
    /// is pinned directly. For a 64-hex OWNER name the two agree; for a CONTENDER
    /// name `<key>.<uuid>` — the other name this module mints under the same
    /// prefix — `with_extension("lock")` REPLACES the uuid and returns the
    /// OWNER's lock path, so one action's contender would lease (and gate the
    /// deletion of) a different action's warm dir.
    #[test]
    fn owner_lock_path_appends_and_never_replaces_a_suffix() {
        let prefix = Path::new("/p");
        let key = hex64('a');
        assert_eq!(
            owner_lock_path(&prefix.join(&key)),
            prefix.join(format!("{key}.lock")),
            "an owner execroot's record is its name plus `.lock`"
        );

        let contender = prefix.join(format!("{key}.{}", "d".repeat(32)));
        assert_eq!(
            owner_lock_path(&contender),
            prefix.join(format!("{key}.{}.lock", "d".repeat(32))),
            "★ the suffix must be APPENDED; `with_extension` would return the \
             OWNER's lock path here and cross two targets' leases"
        );
        assert_ne!(
            owner_lock_path(&contender),
            prefix.join(format!("{key}.lock")),
            "★ regression guard for the `with_extension` form specifically"
        );
    }

    /// ★ THE A3 GUARD, same shape as the T1 guard above and for the same reason.
    /// Only `EWOULDBLOCK` means "another holder"; treating any other errno as
    /// contention would make the disk-growth guard fail OPEN — an `EIO` storm on
    /// the external `/Volumes/CrowAgent` would read as "every dir is busy,
    /// nothing to evict" while the volume filled.
    #[test]
    fn only_ewouldblock_is_lease_contention() {
        use std::io::Error;

        assert!(
            is_lease_contended(&Error::from_raw_os_error(libc::EWOULDBLOCK)),
            "★ EWOULDBLOCK is the ONLY tolerated flock errno — it is the one that \
             means a live holder exists"
        );
        for (errno, name) in [
            (libc::EACCES, "EACCES (a permissions regression)"),
            (
                libc::EIO,
                "EIO (the realistic errno on the external /Volumes/CrowAgent)",
            ),
            (libc::ENOLCK, "ENOLCK (the kernel is out of lock records)"),
            (libc::EBADF, "EBADF"),
            (libc::EINTR, "EINTR"),
            (libc::ENOENT, "ENOENT"),
        ] {
            let err = Error::from_raw_os_error(errno);
            assert!(
                !is_lease_contended(&err),
                "★ {name} MUST abort the pass, never be read as lease contention — \
                 a {name} storm would otherwise read as \"every candidate is \
                 leased, nothing to do\" while the disk fills (got kind {:?})",
                err.kind()
            );
        }
    }

    /// Spawn a helper process that takes `flock(LOCK_EX)` on `lock_path` and
    /// holds it until its stdin closes. Returns `None` when no `perl` is
    /// available (skip rather than assert a falsehood).
    ///
    /// `perl` is used because it is the primitive the REAPER uses for exactly
    /// this lease, and because it is the only `flock(2)`-interoperating one
    /// present on both this host and the darwin workers (`zsh`'s `zsystem flock`
    /// does NOT interoperate on linux — see the reaper's `★ THE LEASE` note).
    ///
    /// The handshake is a token on stdout, not a sleep: the caller does not
    /// proceed until the child has printed `LOCKED`, i.e. until the lock is
    /// actually held.
    fn spawn_external_lock_holder(lock_path: &Path) -> Option<std::process::Child> {
        use std::io::BufRead as _;
        let mut child = std::process::Command::new("perl")
            .arg("-e")
            .arg(
                r#"open(my $fh, "+<", $ARGV[0]) or die "open: $!";
                   flock($fh, 2 | 4) or die "flock: $!";
                   $| = 1; print "LOCKED\n";
                   my $ignored = <STDIN>;"#,
            )
            .arg(lock_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .ok()?;
        let mut line = String::new();
        let stdout = child.stdout.as_mut().expect("piped stdout");
        std::io::BufReader::new(stdout)
            .read_line(&mut line)
            .expect("read the holder's handshake");
        assert_eq!(
            line.trim(),
            "LOCKED",
            "the external holder must confirm it took the lock before the pass runs"
        );
        Some(child)
    }

    /// ★ THE ONLY TEST THAT CAN DISTINGUISH A3 FROM A1/A2, and the arm D5 is
    /// about: a SECOND PROCESS holds the `flock` on `<key>.lock`. Every
    /// in-process mechanism — the `EXECROOT_OWNERSHIP` registry, a `Semaphore`,
    /// the single-flight actor — is blind to it and would delete the tree out
    /// from under a live `process_wrapper` build.
    #[test]
    fn eviction_skips_a_victim_leased_by_another_process() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let (key_a, key_b) = (hex64('a'), hex64('b'));
        make_warm_dir(&root, &key_a, 50);
        make_warm_dir(&root, &key_b, 50);
        // A is the LRU, so it is the first victim the loop reaches.
        set_warm_dir_mtime(&root.join(&key_a), 100);
        set_warm_dir_mtime(&root.join(&key_b), 200);

        let Some(mut holder) = spawn_external_lock_holder(&owner_lock_path(&root.join(&key_a)))
        else {
            eprintln!("skipping: no perl available to hold an external flock");
            return;
        };

        // total 100 > budget 40: A is the LRU but is leased by another PROCESS →
        // skip it and evict B instead.
        let outcome = evict_warm_dirs_over_budget_at(&root, 40).expect("pass must succeed");

        drop(holder.stdin.take());
        holder.wait().expect("holder exits");

        assert!(
            root.join(&key_a).exists(),
            "★ a warm dir another PROCESS holds the flock on MUST NOT be evicted \
             — that process is a live build compiling into it, and no in-process \
             lock can see it"
        );
        assert_eq!(
            outcome.xproc_leased_skipped, 1,
            "the cross-process lease must be COUNTED, and separately from the \
             in-process one"
        );
        assert_eq!(
            outcome.leased_skipped, 0,
            "★ a cross-process holder is NOT an in-process lease; collapsing them \
             would hide which side the contention came from"
        );
        assert_eq!(
            outcome.no_lease_record_skipped, 0,
            "the record exists — a count here means the fixture missed its target"
        );
        assert!(
            !root.join(&key_b).exists(),
            "the next candidate is evicted: one contended victim must not stall \
             the pass"
        );
        assert_eq!(outcome.dirs_evicted, 1);
    }

    /// ★ THE LEASE MUST BE HELD ACROSS THE DISCARD, not merely taken and
    /// dropped. Probed at the exact instant before `remove_dir_all`: a second
    /// open file description — which `flock(2)` treats independently even inside
    /// one process — must be REFUSED. Without this, `let _victim_lease = …`
    /// degrading to a temporary (released at the end of the statement) is
    /// invisible: every other assertion in this file still passes.
    #[test]
    fn eviction_holds_the_victim_lease_across_the_discard() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        make_warm_dir(&root, &hex64('a'), 50);

        *PROBE_LEASE_PROBE_RESULT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let lease_probe: fn(ProbePoint, &Path) = |point, path| {
            if point != ProbePoint::BeforeRemoveDirAll {
                return;
            }
            // A SECOND open file description on the same lock file. `flock`
            // treats descriptions independently — even within one process — so
            // this is refused iff the eviction still holds its own.
            let taken = match try_acquire_victim_lease(path) {
                Ok(VictimLease::Acquired(_)) => Some(true),
                Ok(VictimLease::Contended) => Some(false),
                Ok(VictimLease::NoRecord) | Err(_) => None,
            };
            *PROBE_LEASE_PROBE_RESULT
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = taken;
        };
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(lease_probe);
        let outcome = evict_warm_dirs_over_budget_at(&root, 10);
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        outcome.expect("pass must succeed");

        assert_eq!(
            *PROBE_LEASE_PROBE_RESULT
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Some(false),
            "★ the victim lease must STILL BE HELD at `remove_dir_all` — \
             `Some(true)` means it was released early (a racing builder could \
             acquire it and start compiling into a tree we are deleting); `None` \
             means the probe never reached the lease at all"
        );
    }

    /// Result slot for the lease-held-across-discard probe (a `fn` pointer takes
    /// no captures).
    static PROBE_LEASE_PROBE_RESULT: Mutex<Option<bool>> = Mutex::new(None);

    /// ★ FAIL CLOSED ON A MISSING RECORD. Nothing proves an unrecorded tree is
    /// unowned, and minting a record here would flock a FRESH inode while a live
    /// builder holds the real (unlinked) one — the two-holders shape behind the
    /// torn-`.rlib` finding. The skip is counted, and the bytes stay in the
    /// total, so a pool wedged this way raises backpressure instead of going
    /// quiet.
    #[test]
    fn eviction_fails_closed_when_the_lease_record_is_missing() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let (key_a, key_b) = (hex64('a'), hex64('b'));
        // A is the LRU and has NO record (the pre-A3 on-disk shape).
        make_warm_dir_without_lease(&root, &key_a, 50);
        make_warm_dir(&root, &key_b, 50);
        set_warm_dir_mtime(&root.join(&key_a), 100);
        set_warm_dir_mtime(&root.join(&key_b), 200);

        let outcome = evict_warm_dirs_over_budget_at(&root, 40).expect("pass must succeed");

        assert!(
            root.join(&key_a).exists(),
            "★ a warm dir with no lease record MUST NOT be deleted — the record \
             is the only proof no build owns it"
        );
        assert_eq!(
            outcome.no_lease_record_skipped, 1,
            "the refusal must be COUNTED, or a pool that stops shrinking looks \
             identical to one that is already within budget"
        );
        assert_eq!(
            outcome.xproc_leased_skipped, 0,
            "a MISSING record is not a HELD one; they need different repairs \
             (backfill vs wait)"
        );
        assert_eq!(
            outcome.dirs_evicted, 1,
            "the recorded candidate is still evicted"
        );
        assert_eq!(
            outcome.bytes_remaining, 50,
            "★ the refused dir's bytes STAY in the total — they are still on \
             disk, and pretending otherwise would make the next pass believe it \
             is within budget"
        );
        assert!(
            outcome.still_over_budget,
            "★ …which is what makes the refusal LOUD: the pool is genuinely \
             still over budget, so this raises backpressure rather than going \
             quiet about a guard that has stopped working"
        );
        assert_eq!(
            outcome.log_arm(),
            EvictionLogArm::Backpressure,
            "the operator's only warn! is the arm a wedged pool must select"
        );
    }

    /// The record is a SIBLING of the victim, and this module must never unlink
    /// one: the lock lives on the open file description, so removing the file
    /// lets a racing build create a fresh inode and lock it independently — two
    /// holders in one execroot.
    #[test]
    fn eviction_never_unlinks_a_lease_record() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let keys = [hex64('a'), hex64('b')];
        for key in &keys {
            make_warm_dir(&root, key, 50);
        }

        let outcome = evict_warm_dirs_over_budget_at(&root, 10).expect("pass must succeed");
        assert_eq!(
            outcome.dirs_evicted, 2,
            "both dirs are over budget and evicted"
        );

        for key in &keys {
            assert!(!root.join(key).exists(), "the execroot is gone");
            assert!(
                owner_lock_path(&root.join(key)).exists(),
                "★ the `.lock` record MUST survive its execroot's eviction — \
                 deleting it reopens the two-holders race (FL-1383 red-team B1)"
            );
        }
    }

    /// ★ PER-SITE ERRNO COVERAGE FOR THE NEW SITE. An unopenable record is not
    /// a missing one: the tree may well be owned, and reading a permission fault
    /// as "no record" (or as contention) is the same fail-open the T1 funnel
    /// exists to prevent, one site further along.
    #[test]
    fn lease_open_aborts_on_eacces_not_treated_as_missing() {
        use std::os::unix::fs::PermissionsExt;

        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_td, root) = canonical_tempdir();
        let key = hex64('a');
        make_warm_dir(&root, &key, 50);
        let record = owner_lock_path(&root.join(&key));
        std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o000))
            .expect("chmod 000");

        // Running as root defeats the permission bit; skip rather than assert a
        // falsehood.
        if std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&record)
            .is_ok()
        {
            std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o644)).ok();
            eprintln!("skipping: euid can open a 0o000 file (root?)");
            return;
        }

        let err = evict_warm_dirs_over_budget_at(&root, 10)
            .expect_err("★ EACCES on the lease record MUST abort the pass");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("open lease record"),
            "expected the LEASE-OPEN error to propagate, got: {msg}"
        );
        assert!(
            !msg.contains("os error 2"),
            "★ the aborting error must not be an ENOENT — that would mean the \
             fixture missed the site it is aiming at, got: {msg}"
        );
        assert!(
            root.join(&key).exists(),
            "★ and nothing may be deleted on the way out"
        );

        std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o644)).ok();
    }

    /// The companion change: creating an OWNER execroot creates its record.
    /// A CONTENDER gets none — it is neither an eviction candidate (the pass
    /// enumerates 64-hex names) nor a reaper candidate (`^[0-9a-f]{64}$`) — and
    /// crucially must not create the OWNER's.
    #[test]
    fn ensure_and_wipe_creates_the_owner_record_but_none_for_a_contender() {
        let (_td, root) = canonical_tempdir();
        let key = hex64('a');

        let owner = root.join(&key);
        ensure_and_wipe_execroot_at(&owner, &root).expect("owner ensure+wipe");
        assert!(
            root.join(format!("{key}.lock")).exists(),
            "★ the worker must create the record it (and the reaper) rely on — \
             without this the reaper REFUSEs every worker execroot and reddens \
             its daily job"
        );

        let contender = root.join(format!("{key}.{}", "d".repeat(32)));
        ensure_and_wipe_execroot_at(&contender, &root).expect("contender ensure+wipe");
        assert!(
            !root.join(format!("{key}.{}.lock", "d".repeat(32))).exists(),
            "a contender needs no lease record: nothing enumerates it"
        );
    }

    /// The backfill exists because create-on-use never reaches the dirs that
    /// matter: the LRU victim is by definition the one that is NOT being rebuilt.
    #[test]
    fn startup_backfill_records_owner_dirs_only() {
        let (_td, root) = canonical_tempdir();
        let (key_a, key_b) = (hex64('a'), hex64('b'));
        make_warm_dir_without_lease(&root, &key_a, 10);
        make_warm_dir(&root, &key_b, 10);
        let contender = root.join(format!("{}.{}", hex64('c'), "d".repeat(32)));
        std::fs::create_dir_all(&contender).expect("mk contender");
        std::fs::write(root.join("not-a-key"), b"x").expect("stray file");

        let created = ensure_owner_lock_files_at(&root).expect("backfill");

        assert_eq!(
            created, 1,
            "★ exactly the ONE owner dir that lacked a record — an existing \
             record must never be re-created (a live builder may hold it) and a \
             contender never gets one"
        );
        assert!(owner_lock_path(&root.join(&key_a)).exists());
        assert!(owner_lock_path(&root.join(&key_b)).exists());
        assert!(!owner_lock_path(&contender).exists());
        assert!(!root.join("not-a-key.lock").exists());

        assert_eq!(
            ensure_owner_lock_files_at(&root).expect("idempotent"),
            0,
            "a second backfill creates nothing"
        );
    }

    // -- A2: the single-flight eviction actor ----------------------------------

    /// Rendezvous state for the actor test. One mutex over the whole struct so a
    /// waiter can never see a torn view of "how many passes are running".
    struct PassRendezvous {
        /// Passes that reached the end of enumeration.
        started: u64,
        /// Passes currently inside the probe.
        in_flight: usize,
        /// High-water mark of `in_flight` — THE single-flight observable.
        max_in_flight: usize,
        /// Passes that left the probe.
        finished: u64,
        /// Set by the test to let parked passes proceed.
        release: bool,
        /// Set by the probe if its deadlock detector fired.
        wedged: bool,
    }

    static PASS_RV: Mutex<PassRendezvous> = Mutex::new(PassRendezvous {
        started: 0,
        in_flight: 0,
        max_in_flight: 0,
        finished: 0,
        release: false,
        wedged: false,
    });
    static PASS_RV_CV: std::sync::Condvar = std::sync::Condvar::new();

    /// Probe body: park every pass at the end of enumeration until the test
    /// releases it, recording the concurrency high-water mark while parked.
    ///
    /// The 30s wait is a DEADLOCK DETECTOR, not synchronisation — it can only
    /// fire if the test never sets `release`.
    fn rendezvous_probe(point: ProbePoint, _path: &Path) {
        if point != ProbePoint::AfterEnumeration {
            return;
        }
        let mut st = PASS_RV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        st.started += 1;
        st.in_flight += 1;
        st.max_in_flight = st.max_in_flight.max(st.in_flight);
        PASS_RV_CV.notify_all();
        while !st.release {
            let (guard, timeout) = PASS_RV_CV
                .wait_timeout(st, Duration::from_secs(30))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st = guard;
            if timeout.timed_out() && !st.release {
                st.wedged = true;
                break;
            }
        }
        st.in_flight -= 1;
        st.finished += 1;
        PASS_RV_CV.notify_all();
    }

    /// Block until `pred` holds, or the deadline passes. Returns whether it held.
    fn wait_for_pass_state(pred: impl Fn(&PassRendezvous) -> bool, deadline: Duration) -> bool {
        let mut st = PASS_RV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let start = std::time::Instant::now();
        while !pred(&st) {
            let remaining = match deadline.checked_sub(start.elapsed()) {
                Some(remaining) if !remaining.is_zero() => remaining,
                _ => return false,
            };
            let (guard, _) = PASS_RV_CV
                .wait_timeout(st, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st = guard;
        }
        true
    }

    /// ★ THE A2 REGRESSION TEST — pair-b's T8 shape (two concurrent passes over
    /// one prefix), inverted. Before the actor, `cleanup()` `spawn_blocking`ed an
    /// independent whole-pool pass per portable action with NO pass-level
    /// serialization anywhere (9 eviction events in one second on one host), and
    /// two passes racing each other's deletes was the dominant ENOENT source.
    /// With one actor owning the pool, observing two passes at once must become
    /// IMPOSSIBLE — so this test's value is that it fails if the actor is
    /// bypassed.
    ///
    /// It pins BOTH halves of the arrival policy, and they fail differently:
    ///
    ///  - **single-flight** — `max_in_flight`. Deterministic and POSITIVE: pass 1
    ///    does not leave the probe until this test says so, so under a
    ///    spawn-per-arrival implementation the other three passes are *forced* to
    ///    pile up inside it. No timeout is involved in detecting that.
    ///  - **coalescing** — `started == 2`. Four arrivals, three of them during
    ///    pass 1, must collapse to exactly one follow-up.
    ///
    /// The weakest assertion here is the last one (no THIRD pass), which is an
    /// absence and therefore bounded by a timeout; it is what catches a QUEUE
    /// (where the three arrivals would each buy their own pass). Named as such
    /// rather than dressed up.
    #[test]
    fn eviction_actor_is_single_flight_and_coalesces_arrivals() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *PASS_RV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = PassRendezvous {
            started: 0,
            in_flight: 0,
            max_in_flight: 0,
            finished: 0,
            release: false,
            wedged: false,
        };

        let (_td, root) = canonical_tempdir();
        for key in [hex64('a'), hex64('b'), hex64('c')] {
            make_warm_dir(&root, &key, 50);
        }
        let ctx = PortableIncrContext {
            fixed_prefix: root.clone(),
            config: PortableIncrConfig {
                enabled: true,
                action_output_allowlist: vec![],
            },
        };
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(rendezvous_probe);

        // A multi-thread runtime the TEST THREAD IS NOT PART OF: the test body
        // blocks on condvars, so it must not occupy a runtime worker.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let _enter = rt.enter();
        // Budget 40 < any single dir (50) ⇒ the first pass consumes every
        // candidate, so nothing depends on LRU order.
        let actor = EvictionActor::spawn(ctx, 40);

        actor.request();
        assert!(
            wait_for_pass_state(|st| st.started >= 1, Duration::from_secs(30)),
            "the actor never ran a pass within 30s — it is wedged, or `request` \
             does not reach it"
        );

        // Three more arrivals, GUARANTEED to land during pass 1: pass 1 is parked
        // inside the probe and cannot leave until this test sets `release`.
        for _ in 0..3 {
            actor.request();
        }
        // Pass 1 is PARKED, so any pass that starts now is by construction
        // CONCURRENT with it. This is the forcing step that makes the
        // single-flight check positive rather than a hope about timing.
        assert!(
            !wait_for_pass_state(|st| st.started >= 2, Duration::from_millis(500)),
            "★ a second pass STARTED while the first was still parked mid-pass — \
             the single-flight actor has been bypassed (a spawn-per-arrival, a \
             second actor, or a direct call to `evict_warm_dirs_over_budget`). \
             That is exactly the shape that made peer passes the dominant ENOENT \
             source: 9 eviction events in one second on one host"
        );

        {
            let mut st = PASS_RV
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.release = true;
            PASS_RV_CV.notify_all();
        }
        assert!(
            wait_for_pass_state(|st| st.finished >= 2, Duration::from_secs(30)),
            "the coalesced follow-up pass never ran — a notification arriving \
             DURING a pass must not be dropped, or the last action of a build \
             leaves the pool over budget with nothing scheduled"
        );

        // Absence check, and the only timeout-bounded assertion here: a QUEUE
        // would run passes 3 and 4 for the two remaining arrivals.
        let third = wait_for_pass_state(|st| st.started >= 3, Duration::from_millis(500));

        let st = PASS_RV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!st.wedged, "the probe's deadlock detector fired");
        assert_eq!(
            st.max_in_flight, 1,
            "★ at most ONE pass may ever be in flight in this process"
        );
        assert!(
            !third,
            "★ a THIRD pass ran: the four arrivals were QUEUED, not COALESCED. \
             Every pass reads the same GLOBAL pool state, so a queued pass \
             recomputes an answer already computed and buys one extra full \
             recursive walk per waiter"
        );
        assert_eq!(
            st.started, 2,
            "★ 4 requests must collapse to exactly 2 passes: one in flight, one \
             pending (`Notify::notify_one` stores at most one permit)"
        );
        assert_eq!(actor.requests(), 4, "every request is still counted");
        drop(st);

        assert_eq!(
            std::fs::read_dir(&root)
                .expect("read root")
                .filter_map(Result::ok)
                .filter(|e| is_hex64(&e.file_name().to_string_lossy()))
                .count(),
            0,
            "★ and the actor really evicted: single-flight must not become \
             no-flight"
        );

        drop(actor);
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// ★ THE CONVERGENCE FLOOR MUST BE PROVED TO FIRE. Both review pairs found
    /// that with the tick baked in as a 300 s constant, "the tick converges the
    /// pool" and "the tick can never fire" were INDISTINGUISHABLE — two mutants
    /// survived on it. It is a spawn parameter now, so this test drives it with
    /// zero requests and asserts a pass happens anyway.
    ///
    /// The 20 ms tick is not synchronisation: the assertion waits on the probe's
    /// condvar, so the tick length changes only how long the test takes, never
    /// whether it is correct. The 30 s bound is a deadlock detector.
    #[test]
    fn eviction_actor_tick_runs_a_pass_with_no_requests() {
        let _serial = PROBE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *PASS_RV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = PassRendezvous {
            started: 0,
            in_flight: 0,
            max_in_flight: 0,
            finished: 0,
            // Passes run straight through: this test is about the tick ARRIVING,
            // not about holding one open.
            release: true,
            wedged: false,
        };

        let (_td, root) = canonical_tempdir();
        for key in [hex64('a'), hex64('b')] {
            make_warm_dir(&root, &key, 50);
        }
        let ctx = PortableIncrContext {
            fixed_prefix: root.clone(),
            config: PortableIncrConfig {
                enabled: true,
                action_output_allowlist: vec![],
            },
        };
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(rendezvous_probe);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let _enter = rt.enter();
        let actor = EvictionActor::spawn_with_tick(ctx, 40, Duration::from_millis(20));

        // NOT ONE `request()` is made.
        assert!(
            wait_for_pass_state(|st| st.started >= 1, Duration::from_secs(30)),
            "★ the convergence floor never fired: an eviction pass must run on the \
             TICK alone. Without this a pool driven over budget through a channel \
             we do not observe — an external actor, a pass that errored — is never \
             corrected, and the actor looks identical to one whose timer is dead"
        );
        assert_eq!(
            actor.requests(),
            0,
            "★ and it must have been the TICK, not a stray request"
        );

        // ★ THE DETECTOR'S FALSE-POSITIVE DIRECTION, on a really-running actor.
        // `overdue_after` here is 3 × 20 ms = 60 ms, so by the 4th tick a
        // `last_pass_completed_ms` that is never REFRESHED would already read
        // `PassOverdue` — i.e. this fails if the timestamp is written once (or
        // not at all) instead of after every pass. The correct code keeps
        // refreshing it every 20 ms, so scheduling delay cannot make this flake:
        // a stall produces MORE passes, not fewer.
        assert!(
            wait_for_pass_state(|st| st.started >= 4, Duration::from_secs(30)),
            "the actor stopped ticking"
        );
        assert_eq!(
            actor.liveness(),
            ActorLiveness::Live,
            "★ a healthy, actively-ticking actor must NOT report overdue — a \
             liveness detector that cries wolf on a working worker gets muted, \
             and then it is not a detector at all"
        );

        drop(actor);
        *VANISH_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// ★ THE LIVENESS DETECTOR, EXERCISED IN BOTH DIRECTIONS. A2 concentrates all
    /// eviction into one task, so both review pairs converged on the same hazard:
    /// if that task dies or wedges, the pool grows with nothing said — the only
    /// arm that would fire is `WithinBudget`, which is `debug!` and compiled out
    /// of the release worker. A detector that is only ever asserted in its
    /// healthy direction is not a detector.
    ///
    /// Driven through the PURE function so no case depends on wall-clock timing;
    /// `liveness()` supplies exactly these four inputs from live state.
    #[test]
    fn actor_liveness_reports_both_directions() {
        let tick = Duration::from_secs(300);
        let overdue = tick * ACTOR_OVERDUE_AFTER_TICKS;
        const SPAWN: u64 = 1_000_000;

        assert_eq!(
            actor_liveness_from(false, SPAWN + 10, SPAWN, SPAWN + 20, overdue),
            ActorLiveness::Live,
            "a running actor whose last pass just completed is Live"
        );
        assert_eq!(
            actor_liveness_from(false, 0, SPAWN, SPAWN + 1000, overdue),
            ActorLiveness::Live,
            "★ no pass yet is NORMAL at startup and must not cry wolf — the \
             reference is the spawn time until the first pass completes"
        );

        // ★ The direction that matters: the task is gone.
        assert_eq!(
            actor_liveness_from(true, SPAWN + 10, SPAWN, SPAWN + 20, overdue),
            ActorLiveness::TaskGone,
            "★ a finished task means eviction is OVER for this process — it must \
             never read as Live merely because a pass completed recently. That is \
             the residue-vs-live-state distinction: 'a pass ran once' can only \
             turn on, so it would report healthy forever after the actor died"
        );

        // ★ And the wedged case: the task exists but has stopped turning.
        let overdue_ms = u64::try_from(overdue.as_millis()).expect("fits");
        assert_eq!(
            actor_liveness_from(false, SPAWN, SPAWN, SPAWN + overdue_ms + 1, overdue),
            ActorLiveness::PassOverdue {
                since_ms: overdue_ms + 1
            },
            "★ a hung `spawn_blocking` on the external /Volumes/CrowAgent leaves \
             the task alive and the pool unbounded; `is_finished()` alone cannot \
             see it, which is why the timestamp is checked too"
        );
        // The boundary is not overdue — an off-by-one here would page on every
        // healthy idle worker.
        assert_eq!(
            actor_liveness_from(false, SPAWN, SPAWN, SPAWN + overdue_ms, overdue),
            ActorLiveness::Live,
            "exactly at the threshold is still live"
        );
        // A clock that goes backwards must not manufacture an alarm.
        assert_eq!(
            actor_liveness_from(false, SPAWN + 5000, SPAWN, SPAWN, overdue),
            ActorLiveness::Live,
            "a backwards clock saturates to 0 elapsed, never to a huge one"
        );
    }

    /// The live-state half of the same detector: an actor whose task has actually
    /// been aborted must report `TaskGone` through the REAL accessor, not just
    /// through the pure function. This pins `liveness()` to
    /// `JoinHandle::is_finished` rather than to something that cannot observe a
    /// dead task.
    #[test]
    fn liveness_observes_a_really_aborted_task() {
        let (_td, root) = canonical_tempdir();
        let ctx = PortableIncrContext {
            fixed_prefix: root.clone(),
            config: PortableIncrConfig {
                enabled: true,
                action_output_allowlist: vec![],
            },
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let _enter = rt.enter();
        // A long tick so the actor is parked in `select!` and nothing races us.
        let actor = EvictionActor::spawn_with_tick(ctx, u64::MAX, Duration::from_secs(3600));
        assert_eq!(
            actor.liveness(),
            ActorLiveness::Live,
            "a freshly-spawned actor is Live"
        );

        actor.task.abort();
        // Assert on the OBSERVABLE (`is_finished`), not on a duration; the bound
        // is a deadlock detector.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !actor.task.is_finished() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(
            actor.liveness(),
            ActorLiveness::TaskGone,
            "★ a dead actor MUST be observable from the requester's side — \
             `request()` is the only thing still running when this happens, and \
             if it cannot tell, the pool grows silently until the worker restarts"
        );
    }
}
