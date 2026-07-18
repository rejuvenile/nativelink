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

//! FL-1383 chunk 2a — worker-side FIXED_PREFIX provisioning + §12 startup
//! asserts. These tests pin the foundation contract:
//!  (a) flag OFF ⇒ fully inert (no provisioning, no assert, no dir created);
//!  (b) flag ON ⇒ FIXED_PREFIX provisioned owned-by-worker-uid, mode 0755;
//!  (c) any §12 assert violation ⇒ fail-loud-DISABLE (returns `None`, worker
//!      keeps running) — probed via wrong perms, a symlinked sysroot, a missing
//!      path config, and the EXDEV cross-volume case.
//!
//! Chunk 2a is INERT on the execution path: `provision_and_assert` only
//! provisions + asserts + gates; nothing here rewires `make_action_directory`
//! (that is chunk 2b).

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use nativelink_macro::nativelink_test;
use nativelink_worker::portable_incr::{
    create_dir_exclusive, create_file_exclusive_no_follow, provision_and_assert,
};

/// Canonicalized temp root so every path fed to the sysroot byte-identical
/// assert is already realpath-stable (the test box's `TMPDIR` may itself be a
/// symlink; we do not want that to confound the sysroot-symlink assertion).
fn canonical_tempdir() -> (tempfile::TempDir, PathBuf) {
    let td = tempfile::TempDir::new().expect("tempdir");
    let canon = fs::canonicalize(td.path()).expect("canonicalize tempdir");
    (td, canon)
}

// (a) Flag OFF ⇒ inert. No FIXED_PREFIX is provisioned; the configured path is
// never touched; the result is DISABLED (`None`).
#[nativelink_test]
async fn flag_off_is_inert_no_provisioning() {
    let (_td, root) = canonical_tempdir();
    let fixed_prefix = root.join("should_not_exist");

    let outcome = provision_and_assert(
        /* enabled */ false,
        Some(fixed_prefix.as_path()),
        Some(root.as_path()),
        root.as_path(),
    )
    .await;

    assert!(
        outcome.is_none(),
        "flag OFF must yield DISABLED (None), the inert state"
    );
    assert!(
        !fixed_prefix.exists(),
        "flag OFF must NOT provision the FIXED_PREFIX dir — inertness violated"
    );
}

// (b) Flag ON with a clean environment ⇒ FIXED_PREFIX provisioned, owned by the
// worker uid, mode exactly 0755 (NOT world-writable), and the feature is
// ENABLED (Some).
#[nativelink_test]
async fn provisions_dir_owned_by_worker_uid_mode_0755() {
    let (_td, root) = canonical_tempdir();
    let fixed_prefix = root.join("fixed_prefix");
    // sysroot: a real, absolute, canonical dir (canonicalizes to itself).
    let sysroot = root.join("sysroot");
    fs::create_dir(&sysroot).expect("create sysroot");
    let sysroot = fs::canonicalize(&sysroot).expect("canonicalize sysroot");

    let outcome = provision_and_assert(
        true,
        Some(fixed_prefix.as_path()),
        Some(sysroot.as_path()),
        root.as_path(),
    )
    .await
    .expect("clean environment must provision + pass all §12 asserts ⇒ Some");

    assert_eq!(
        outcome.fixed_prefix, fixed_prefix,
        "the returned FIXED_PREFIX must be the provisioned path"
    );

    let meta = fs::symlink_metadata(&fixed_prefix).expect("FIXED_PREFIX must exist after provisioning");
    assert!(meta.is_dir(), "FIXED_PREFIX must be a real directory");
    assert_eq!(
        meta.permissions().mode() & 0o777,
        0o755,
        "FIXED_PREFIX must be mode 0755, not world-writable"
    );
    // The tempdir root was created by THIS process (== the worker uid running
    // the provisioner), so its uid is the worker uid reference.
    let worker_uid = fs::metadata(&root).expect("root meta").uid();
    assert_eq!(
        meta.uid(),
        worker_uid,
        "FIXED_PREFIX must be owned by the worker uid"
    );
}

// (c) Fail-loud-DISABLE: a pre-existing FIXED_PREFIX with world-writable perms
// (the `/Users/Shared` 1777 anti-pattern §9 forbids) ⇒ DISABLED, dir untouched,
// worker keeps running (function returns, does not panic).
#[nativelink_test]
async fn wrong_perms_fail_loud_disable() {
    let (_td, root) = canonical_tempdir();
    let fixed_prefix = root.join("preexisting_bad_perms");
    fs::create_dir(&fixed_prefix).expect("create dir");
    fs::set_permissions(&fixed_prefix, fs::Permissions::from_mode(0o777)).expect("chmod 0777");
    let sysroot = root.clone();

    let outcome = provision_and_assert(
        true,
        Some(fixed_prefix.as_path()),
        Some(sysroot.as_path()),
        root.as_path(),
    )
    .await;

    assert!(
        outcome.is_none(),
        "a world-writable pre-existing FIXED_PREFIX must fail-loud-DISABLE (None)"
    );
    // The provisioner must not silently "fix" perms it did not create — the dir
    // is left as found and the feature is simply disabled.
    assert_eq!(
        fs::symlink_metadata(&fixed_prefix)
            .expect("dir still present")
            .permissions()
            .mode()
            & 0o777,
        0o777,
        "provisioner must not mutate a rejected pre-existing dir"
    );
}

// (c) Fail-loud-DISABLE: a sysroot reached via a symlink canonicalizes to a
// different real path (the `output_base`-symlink hazard §2 forbids) ⇒ DISABLED.
#[nativelink_test]
async fn symlinked_sysroot_fail_loud_disable() {
    let (_td, root) = canonical_tempdir();
    let fixed_prefix = root.join("fixed_prefix");
    let real_sysroot = root.join("real_sysroot");
    fs::create_dir(&real_sysroot).expect("create real sysroot");
    let sysroot_link = root.join("sysroot_link");
    symlink(&real_sysroot, &sysroot_link).expect("create sysroot symlink");

    let outcome = provision_and_assert(
        true,
        Some(fixed_prefix.as_path()),
        Some(sysroot_link.as_path()),
        root.as_path(),
    )
    .await;

    assert!(
        outcome.is_none(),
        "a symlinked sysroot (realpath != path) must fail-loud-DISABLE (None)"
    );
}

// (c) Fail-loud-DISABLE: enabled but the required FIXED_PREFIX config is unset.
#[nativelink_test]
async fn enabled_but_no_fixed_prefix_disables() {
    let (_td, root) = canonical_tempdir();
    let outcome =
        provision_and_assert(true, None, Some(root.as_path()), root.as_path()).await;
    assert!(
        outcome.is_none(),
        "enabled with no FIXED_PREFIX configured must fail-loud-DISABLE"
    );
}

// (c) Fail-loud-DISABLE: enabled but the required sysroot config is unset.
#[nativelink_test]
async fn enabled_but_no_sysroot_disables() {
    let (_td, root) = canonical_tempdir();
    let fixed_prefix = root.join("fixed_prefix");
    let outcome =
        provision_and_assert(true, Some(fixed_prefix.as_path()), None, root.as_path()).await;
    assert!(
        outcome.is_none(),
        "enabled with no sysroot configured must fail-loud-DISABLE"
    );
}

/// Probe candidate mount points for one on a DIFFERENT device than `reference`,
/// so we can exercise the EXDEV cross-volume branch. Returns `None` when the
/// runner exposes no second local filesystem (then the caller documents a skip
/// rather than falsely passing).
fn find_cross_device_dir(reference: &Path) -> Option<PathBuf> {
    let ref_dev = fs::metadata(reference).ok()?.dev();
    for cand in ["/dev/shm", "/run", "/tmp"] {
        let p = Path::new(cand);
        if let Ok(meta) = fs::metadata(p) {
            if meta.dev() != ref_dev {
                return Some(p.to_path_buf());
            }
        }
    }
    None
}

// (c) EXDEV: FIXED_PREFIX on a SEPARATE volume from the execroot ⇒ link() fails
// EXDEV ⇒ the feature refuses (DISABLED) in chunk 2a (copy-fallback is 2b).
#[nativelink_test]
async fn exdev_cross_volume_refuses() {
    let (_td, root) = canonical_tempdir();
    let Some(other_dev_dir) = find_cross_device_dir(&root) else {
        eprintln!(
            "SKIP exdev_cross_volume_refuses: runner exposes no second local filesystem \
             distinct from {}",
            root.display()
        );
        return;
    };
    // FIXED_PREFIX lives on the OTHER device; execroot probe dir is `root`.
    let other = tempfile::TempDir::new_in(&other_dev_dir).expect("tempdir on other device");
    let other = fs::canonicalize(other.path()).expect("canonicalize other");
    let fixed_prefix = other.join("fixed_prefix");
    let sysroot = root.clone();

    let outcome = provision_and_assert(
        true,
        Some(fixed_prefix.as_path()),
        Some(sysroot.as_path()),
        root.as_path(),
    )
    .await;

    assert!(
        outcome.is_none(),
        "FIXED_PREFIX on a separate volume (link ⇒ EXDEV) must refuse/DISABLE in chunk 2a"
    );
}

// Helper contract (for chunk 2b): create_dir_exclusive is O_EXCL — it refuses a
// pre-existing path (EEXIST) rather than silently succeeding.
#[nativelink_test]
async fn create_dir_exclusive_refuses_existing() {
    let (_td, root) = canonical_tempdir();
    let dir = root.join("excl");
    create_dir_exclusive(&dir).expect("first exclusive create succeeds");
    let err = create_dir_exclusive(&dir).expect_err("second exclusive create must fail (EEXIST)");
    assert!(
        format!("{err:?}").to_lowercase().contains("exist"),
        "exclusive dir create must report an already-exists error, got: {err:?}"
    );
}

// Helper contract (for chunk 2b): create_file_exclusive_no_follow refuses to
// open through a symlink (O_NOFOLLOW ⇒ ELOOP) — a symlink-swap defense.
#[nativelink_test]
async fn create_file_no_follow_refuses_symlink() {
    let (_td, root) = canonical_tempdir();
    let target = root.join("target");
    fs::write(&target, b"x").expect("write target");
    let link = root.join("link");
    symlink(&target, &link).expect("symlink");
    let err = create_file_exclusive_no_follow(&link, 0o600)
        .expect_err("O_NOFOLLOW create through a symlink must fail");
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("symbolic") || msg.contains("loop") || msg.contains("exist"),
        "no-follow create through a symlink must be rejected, got: {err:?}"
    );
}
