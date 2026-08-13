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
//! Every doc in this module and in `running_actions_manager` used to claim the
//! gate was `None` "on the entire live fleet". That was true when written and is
//! now FALSE: as of 2026-08-13 the feature is ENABLED on all 10 workers
//! (`worker.json5` `portable_incr.enabled: true`, `fixed_prefix`
//! `/Volumes/CrowAgent/fl-incr-execroots`), and the live logs show it running —
//! 3588 `execroot full-empty ensure+wipe`, 1710 `seed fetch complete`, and 1863
//! eviction passes that logged. Those stale claims sat in the source for weeks
//! and read as "this code cannot run in production", which is exactly the
//! premise a reviewer would rely on. Describe the GATE (what makes it
//! `Some`/`None`) and let the deployed config answer where it is on — a comment
//! cannot track a config field, and one that tries will rot silently.
//!
//! DURABILITY: no `fsync`/`O_SYNC`/sync-write primitive appears here (CLAUDE.md
//! hard rule). All filesystem syscalls are BLOCKING and MUST run inside
//! `spawn_blocking`; [`provision_and_assert`] and the chunk-2b execution-path
//! caller both do exactly that so the tokio worker is never blocked.

use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::SystemTime;

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
    wipe_all_contents(execroot, fixed_prefix)
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
pub enum Containment {
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
pub fn check_under_prefix(path: &Path, fixed_prefix: &Path) -> Result<Containment, Error> {
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
        Err(e) if missing == MissingPath::IsVanished && e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Containment::Vanished);
        }
        Err(e) => {
            return Err(make_err!(
                Code::Internal,
                "portable_incr: cannot canonicalize discard target {} for containment: {e}",
                path.display()
            ));
        }
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
    /// (the §5 backpressure signal — a live warm dir is never evicted).
    pub leased_skipped: usize,
    /// Candidates that VANISHED under this pass (ENOENT) — removed concurrently
    /// between enumeration and eviction, at the lstat, the size walk, or the
    /// discard. Benign: the desired end state, reached by someone else. Counted
    /// so a fail-soft is not a silent one — a pass that skips everything and a
    /// pass that had nothing to do are otherwise indistinguishable.
    ///
    /// NOT credited to `dirs_evicted`/`bytes_freed`: this pass did not free
    /// those bytes.
    pub vanished_skipped: usize,
    /// `true` iff the pool is STILL over budget after evicting every non-leased
    /// candidate (every remaining dir is leased → refuse, don't evict a live
    /// one). A diagnostic for the operator: the static reservation is too small
    /// for the concurrent live-owner working set.
    pub still_over_budget: bool,
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

/// TEST-ONLY seam. The §8 eviction races a concurrent remover at several
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
}

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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
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
        // Atomic claim via the §5 lease set: `insert` returns `true` only when
        // the dir is NOT currently leased by a live owner. This closes the
        // TOCTOU with `plan`: a concurrent same-`targetkey` action sees our
        // sentinel and becomes an isolated CONTENDER (its own dir) rather than
        // racing the delete of this warm dir.
        let claimed = EXECROOT_OWNERSHIP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(path.clone());
        if !claimed {
            // Leased by a live owner — never evict a live warm dir (backpressure).
            outcome.leased_skipped += 1;
            continue;
        }
        // RAII: releases our delete-sentinel from the lease set on drop, even if
        // `discard_dir_tree_confined` errors.
        let _sentinel = OwnershipLeaseGuard {
            owned: Some(path.clone()),
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
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(DiscardOutcome::Removed),
        // Removed between the containment gate and here — same benign race.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(DiscardOutcome::AlreadyGone),
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
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
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
    /// `filler_bytes` (one regular file; the dir itself contributes nothing).
    fn make_warm_dir(root: &Path, key: &str, filler_bytes: usize) {
        let d = root.join(key);
        std::fs::create_dir_all(&d).expect("mk warm dir");
        std::fs::write(d.join("filler"), vec![0u8; filler_bytes]).expect("filler");
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

        let root_for_pass = root.clone();
        let pass = std::thread::spawn(move || {
            // Budget 40 < any single dir (50), so the loop consumes ALL
            // candidates regardless of LRU order.
            evict_warm_dirs_over_budget_at(&root_for_pass, 40)
        });

        // Let the pass reach the barrier (or at least start). Correctness does
        // not depend on this sleep — the mutex does; the sleep only steers
        // which of the three ENOENT sites observes the vanish.
        std::thread::sleep(std::time::Duration::from_millis(250));
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
        assert_eq!(outcome.bytes_remaining, 0);
        assert_eq!(outcome.leased_skipped, 0);
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
}
