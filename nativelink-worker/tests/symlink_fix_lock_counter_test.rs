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

//! #86: counter wiring for `symlink_fix_lock` slow-path entries vs total
//! acquires. Drives the #83 O14 (`Mutex` → `RwLock`) decision; without
//! empirical slow-path-rate data, the conversion may be unjustified
//! overhead.
//!
//! The counters now live in the process-global
//! `nativelink_util::o11_probes::SYMLINK_FIX_COUNTERS` singleton
//! (registered with `MetricsRegistry` so they appear on `/metrics`), not
//! in the per-instance `RunningActionsManagerImpl::Metrics` struct.
//!
//! Tests read a DELTA (before − after) because the static accumulates
//! across all tests in the same process. Each test snapshots the counter
//! before calling `prepare_output_directory`, then asserts the delta.
//!
//! Asymmetric contract on `symlink_fix_slow_path_entries_total`:
//! - **Under-action (T1):** when the under-lock fast-path re-check fails
//!   and the closure proceeds to walk the symlink tree, the counter MUST
//!   increment exactly once. Verified by
//!   `symlink_fix_slow_path_increments_on_slow_path_entry`.
//! - **Over-action (T2):** when the fast-path early-return succeeds (no
//!   lock acquired), the counter MUST stay unchanged. Verified by
//!   `symlink_fix_slow_path_does_not_increment_on_fast_path`.
//!
//! Both tests invoke the same `prepare_output_directory` helper that the
//! production composition (`RunningActionImpl::inner_prepare_action`)
//! calls per-output-file inside `try_join_all`, so behavior is exercised
//! in production-shape — not a mock.

#![cfg(target_family = "unix")]

use core::sync::atomic::Ordering;
use std::os::unix::fs::PermissionsExt;

use nativelink_macro::nativelink_test;
use nativelink_util::common::fs;
use nativelink_util::o11_probes::symlink_fix_counters;
use nativelink_worker::running_actions_manager::prepare_output_directory;
use pretty_assertions::assert_eq;
use rand::Rng;

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data
    )
}

/// T1 (under-action): when the parent directory pre-exists as a read-only
/// directory (mode 0o555), the fast-path `create_dir_all` succeeds but
/// `dir_writable=false` — we fall into the slow path. Counter MUST
/// increment exactly once (one output file = one slow-path entry).
///
/// Reads a DELTA from the process-global singleton because the static
/// accumulates across tests.
///
/// Mutation 2026-06-15: comment out `symlink_fix_counters().record_slow_path_entry()`
/// in `running_actions_manager.rs::prepare_output_directory` → this test
/// MUST red-fail with bespoke "symlink_fix_slow_path_entries counter did
/// NOT increment on slow-path entry (delta=0, expected 1)".
#[nativelink_test]
async fn symlink_fix_slow_path_increments_on_slow_path_entry()
-> Result<(), Box<dyn core::error::Error>> {
    let work_dir = make_temp_path("work_dir_t1");
    fs::create_dir_all(&work_dir).await?;

    // Pre-create the read-only parent directory so the fast-path
    // create_dir_all succeeds (no-op, dir already exists) but the
    // writability check fails. This drives the closure into the slow
    // path, which (after the under-lock re-check) bumps the
    // slow-path-entries counter.
    let parent_dir = format!("{work_dir}/readonly_parent");
    fs::create_dir(&parent_dir).await?;
    let perms = std::fs::Permissions::from_mode(0o555);
    fs::set_permissions(&parent_dir, perms).await?;

    let lock = tokio::sync::Mutex::new(());

    // Snapshot singleton counters BEFORE the call (delta baseline).
    let entries_before = symlink_fix_counters()
        .slow_path_entries
        .load(Ordering::Acquire);
    let acquires_before = symlink_fix_counters()
        .acquires
        .load(Ordering::Acquire);

    // Production composition: same helper called by RunningActionImpl
    // for every output file.
    let res = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        prepare_output_directory(
            &work_dir,
            "",
            "readonly_parent/out.txt",
            &lock,
        ),
    )
    .await
    .expect(
        "must not deadlock — prepare_output_directory slow-path lock contract violated",
    );
    res?;

    // The slow-path walk should have chmod'd readonly_parent back to
    // writable (0o200 | 0o555 = 0o755), so the operation succeeds AND
    // the counter increments.
    let slow_delta = symlink_fix_counters()
        .slow_path_entries
        .load(Ordering::Acquire)
        .wrapping_sub(entries_before);
    assert_eq!(
        slow_delta, 1,
        "symlink_fix_slow_path_entries counter did NOT increment on slow-path entry (delta={slow_delta}, expected 1)",
    );
    // Denominator: lock was acquired exactly once.
    let acquire_delta = symlink_fix_counters()
        .acquires
        .load(Ordering::Acquire)
        .wrapping_sub(acquires_before);
    assert_eq!(
        acquire_delta, 1,
        "symlink_fix_lock_acquires counter did NOT increment on lock acquire (delta={acquire_delta}, expected 1)",
    );
    Ok::<(), Box<dyn core::error::Error>>(())
}

/// T2 (over-action): when the output file's parent does NOT pre-exist
/// (or is normally writable), the fast-path `create_dir_all` succeeds
/// and the writability check passes — the closure returns immediately
/// without acquiring the lock. Counter MUST stay unchanged (delta == 0).
///
/// Mutation 2026-06-15: move `symlink_fix_counters().record_slow_path_entry()`
/// from the post-re-check slow-path entry point to fire on every acquire
/// (e.g. place it next to `record_acquire()`), or remove the
/// `if dir_writable { return Ok(()); }` fast-path early-return →
/// this test MUST red-fail with bespoke "fast-path early-return
/// spuriously incremented slow-path counter (delta=1, expected 0)".
#[nativelink_test]
async fn symlink_fix_slow_path_does_not_increment_on_fast_path()
-> Result<(), Box<dyn core::error::Error>> {
    let work_dir = make_temp_path("work_dir_t2");
    fs::create_dir_all(&work_dir).await?;

    let lock = tokio::sync::Mutex::new(());

    // Snapshot singleton counters BEFORE the call (delta baseline).
    let entries_before = symlink_fix_counters()
        .slow_path_entries
        .load(Ordering::Acquire);
    let acquires_before = symlink_fix_counters()
        .acquires
        .load(Ordering::Acquire);

    // Fast path: parent doesn't exist yet, `create_dir_all` creates it
    // (writable by default), the closure returns Ok before the lock is
    // touched.
    let res = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        prepare_output_directory(
            &work_dir,
            "",
            "fresh_parent/out.txt",
            &lock,
        ),
    )
    .await
    .expect(
        "must not deadlock — prepare_output_directory fast-path contract violated",
    );
    res?;

    // Confirm the parent was actually created (proves the fast path ran).
    assert!(
        fs::metadata(format!("{work_dir}/fresh_parent")).await.is_ok(),
        "fast-path create_dir_all did not create fresh_parent",
    );

    // Slow-path counter MUST be unchanged (delta == 0).
    let slow_delta = symlink_fix_counters()
        .slow_path_entries
        .load(Ordering::Acquire)
        .wrapping_sub(entries_before);
    assert_eq!(
        slow_delta, 0,
        "fast-path early-return spuriously incremented slow-path counter (delta={slow_delta}, expected 0)",
    );
    // Lock-acquire counter MUST also be unchanged (fast path never
    // acquires the lock).
    let acquire_delta = symlink_fix_counters()
        .acquires
        .load(Ordering::Acquire)
        .wrapping_sub(acquires_before);
    assert_eq!(
        acquire_delta, 0,
        "fast-path early-return spuriously incremented lock-acquires counter (delta={acquire_delta}, expected 0)",
    );
    Ok::<(), Box<dyn core::error::Error>>(())
}
