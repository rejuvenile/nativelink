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

//! FL-681: operator-tunable worker→server upload fan-out cap.
//!
//! `handle_upload_missing_blobs` constructs a per-call
//! `Semaphore::new(N)` that throttles concurrent blob uploads back to
//! the server. Historically `N` was the hardcoded
//! `MAX_CONCURRENT_UPLOADS = 32`. It is now resolved from
//! `LocalWorkerConfig.max_concurrent_uploads` via
//! `effective_max_concurrent_uploads`, with the `0` sentinel preserving
//! the historical default so existing deployed configs behave
//! identically.
//!
//! These tests pin the exact integer fed to `Semaphore::new`. The
//! semaphore's permit count IS `effective_max_concurrent_uploads(cfg)`,
//! so asserting the resolver is asserting the semaphore.

use nativelink_util::o11_probes::MAX_CONCURRENT_UPLOADS;
use nativelink_worker::local_worker::effective_max_concurrent_uploads;

/// Backward-compat: the `0` sentinel (what every config that does not
/// set the field deserializes to) resolves to the historical hardcoded
/// `MAX_CONCURRENT_UPLOADS = 32`. If the default fallback regresses, an
/// existing deployed worker silently changes its upload fan-out.
#[test]
fn unset_sentinel_resolves_to_hardcoded_default() {
    assert_eq!(
        effective_max_concurrent_uploads(0),
        MAX_CONCURRENT_UPLOADS,
        "FL-681: max_concurrent_uploads=0 (unset sentinel) MUST resolve \
         to the hardcoded MAX_CONCURRENT_UPLOADS={MAX_CONCURRENT_UPLOADS} \
         so existing deployed worker configs keep the historical upload \
         fan-out cap — default fallback regressed"
    );
}

/// The historical default value is 32 — pin it so a future edit to the
/// constant is a deliberate, reviewed change rather than an accident.
#[test]
fn hardcoded_default_is_thirty_two() {
    assert_eq!(
        MAX_CONCURRENT_UPLOADS, 32,
        "FL-681: MAX_CONCURRENT_UPLOADS is the documented worker→server \
         upload fan-out default (32); changing it is a throughput / \
         server-load tradeoff that must be intentional"
    );
}

/// An explicit operator override takes effect verbatim — the resolver
/// passes a non-zero value straight through to `Semaphore::new`.
#[test]
fn explicit_override_takes_effect() {
    assert_eq!(
        effective_max_concurrent_uploads(128),
        128,
        "FL-681: an explicit max_concurrent_uploads=128 MUST feed \
         Semaphore::new(128) unchanged — operator override ignored"
    );
    assert_eq!(
        effective_max_concurrent_uploads(1),
        1,
        "FL-681: max_concurrent_uploads=1 MUST resolve to 1, not the \
         default — the resolver must not treat small non-zero values as \
         unset"
    );
}
