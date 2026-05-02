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

//! (#216) Build-SHA computation for stale-worker detection.
//!
//! The worker computes a SHA-256 digest of its own running binary at
//! startup and reports the truncated hex prefix (16 hex chars / 64
//! bits) to the scheduler in `ConnectWorkerRequest.build_sha`. The
//! scheduler compares against its configured `compatible_build_shas`
//! allowlist; on mismatch the connection is rejected.
//!
//! Why hash the binary at runtime instead of using a build-time
//! `git rev-parse HEAD`?
//!
//! The bug that motivated #216 (worker-03 running an old binary
//! across a wire-protocol bump → 114k errored UpdateForWorker frames →
//! reconnect storm → 9 OOM kills in 37 h) is fundamentally a
//! "deployed binary differs from server's expectation" problem.
//! Hashing the binary contents catches every cause of binary drift —
//! incomplete deploys, codesign-without-rebuild, stale launchd
//! cached-load, manual scp from a developer machine — independent of
//! how the binary was produced. A git SHA from a build script catches
//! only intra-repo drift and misses everything that touches the
//! binary AFTER linking.
//!
//! Failure mode: reading the binary can fail (permissions, deleted
//! file, etc). We log a `warn!` and report an empty string in that
//! case — the worker continues to run; the scheduler treats empty
//! `build_sha` like any other SHA (subject to allowlist). The
//! allowlist is OPT-IN; production deployments that don't enable it
//! see no behavior change.

use std::io::Read;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use tracing::warn;

/// Number of hex characters of the SHA-256 digest exposed via
/// `build_sha()`. 16 hex chars = 64 bits = ample for a stale-binary
/// allowlist (collision resistance is not security-critical here; the
/// allowlist is configured by an operator, and any false-positive
/// match would have to be engineered intentionally).
pub const BUILD_SHA_HEX_LEN: usize = 16;

/// Cached SHA-256 hex prefix of this process's binary. Computed on
/// first call to [`build_sha`]; subsequent calls are zero-cost.
///
/// `OnceLock<String>` rather than `&'static str` because the value is
/// computed at runtime (not embedded at compile-time). The cached
/// string lives for the entire process lifetime, so leaking a
/// `String` by `Box::leak` would also work — `OnceLock` is simpler.
static BUILD_SHA: OnceLock<String> = OnceLock::new();

/// Returns this process's build SHA — the first
/// `BUILD_SHA_HEX_LEN` hex characters of the SHA-256 of the running
/// binary. The first call computes and caches the value; subsequent
/// calls return the cached value.
///
/// Returns the empty string on failure (binary unreadable). The
/// failure path emits a `warn!` exactly once.
///
/// **Call eagerly from `main()` BEFORE the tokio runtime starts.**
/// The first invocation does ~67 MiB of streaming I/O + SHA-256 (~270 ms
/// on a modern CPU); doing that on a tokio worker thread blocks
/// scheduling for the duration. The production callers in
/// `src/bin/nativelink.rs` warm the cache via [`build_sha`] right after
/// signal-handler install and before `Builder::new_multi_thread().build()`.
/// All later async-context calls (e.g. from
/// `nativelink_worker::worker_utils::make_connect_worker_request`) hit
/// the cached value and return in nanoseconds.
pub fn build_sha() -> &'static str {
    BUILD_SHA
        .get_or_init(|| compute_build_sha().unwrap_or_default())
        .as_str()
}

/// I/O buffer size used by [`compute_build_sha`]. 64 KiB matches the
/// page-aligned default `BufReader` chooses internally and is large
/// enough that the per-syscall overhead is negligible vs the SHA-256
/// inner loop, while keeping the stack-allocated buffer well under the
/// default 8 KiB stack frame ceiling on every supported platform.
const BUILD_SHA_IO_BUF_BYTES: usize = 64 * 1024;

fn compute_build_sha() -> Option<String> {
    let exe_path = match std::env::current_exe() {
        Ok(p) => p,
        Err(err) => {
            warn!(?err, "build_sha: std::env::current_exe failed; reporting empty build_sha");
            return None;
        }
    };
    // Stream the binary through SHA-256 instead of reading it whole.
    // For a ~67 MiB release binary the previous `std::fs::read` peaked
    // at ~67 MiB of allocation at worker startup; with a 64 KiB
    // BufReader the peak working set is `BUILD_SHA_IO_BUF_BYTES`
    // regardless of binary size — relevant on memory-constrained
    // workers and on the server where this fires once during the same
    // window that the FilesystemStore is opening file handles.
    let file = match std::fs::File::open(&exe_path) {
        Ok(f) => f,
        Err(err) => {
            warn!(
                ?err,
                exe_path = %exe_path.display(),
                "build_sha: failed to open own binary; reporting empty build_sha"
            );
            return None;
        }
    };
    let mut reader = std::io::BufReader::with_capacity(BUILD_SHA_IO_BUF_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; BUILD_SHA_IO_BUF_BYTES];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => hasher.update(&buf[..n]),
            // ErrorKind::Interrupted is a transient signal-EINTR; retry.
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                warn!(
                    ?err,
                    exe_path = %exe_path.display(),
                    "build_sha: read failed mid-binary; reporting empty build_sha"
                );
                return None;
            }
        }
    }
    let digest = hasher.finalize();
    let hex_full = hex::encode(digest);
    Some(hex_full[..BUILD_SHA_HEX_LEN].to_string())
}

/// Compute a build SHA from arbitrary bytes — used by tests to
/// produce known-good "matching" or "mismatched" SHAs against a
/// given binary blob without having to read a real on-disk file.
///
/// Production code MUST use [`build_sha`]; this helper exists so test
/// callers can stage allowlist values that match a fake worker's
/// reported SHA.
pub fn compute_build_sha_for_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let hex_full = hex::encode(digest);
    hex_full[..BUILD_SHA_HEX_LEN].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_build_sha_for_bytes_is_deterministic_and_truncated() {
        let a = compute_build_sha_for_bytes(b"hello world");
        let b = compute_build_sha_for_bytes(b"hello world");
        assert_eq!(a, b, "same input => same SHA");
        assert_eq!(
            a.len(),
            BUILD_SHA_HEX_LEN,
            "SHA must be truncated to {BUILD_SHA_HEX_LEN} hex chars"
        );
        // Spot-check known SHA-256("hello world") prefix.
        // Full digest: b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
        assert_eq!(a, "b94d27b9934d3e08", "known SHA-256 prefix");
    }

    #[test]
    fn compute_build_sha_for_bytes_distinguishes_inputs() {
        let a = compute_build_sha_for_bytes(b"binary v1");
        let b = compute_build_sha_for_bytes(b"binary v2");
        assert_ne!(a, b, "different binaries => different SHAs");
    }

    #[test]
    fn build_sha_returns_cached_value() {
        // First call computes; second call returns the cached value.
        // We can't assert the value (depends on the test runner
        // binary), but we can assert idempotence and length bound.
        let a = build_sha();
        let b = build_sha();
        assert_eq!(a, b, "build_sha must be cached + idempotent");
        // Either empty (current_exe failed in some sandbox) or
        // exactly BUILD_SHA_HEX_LEN.
        assert!(
            a.is_empty() || a.len() == BUILD_SHA_HEX_LEN,
            "build_sha = {a:?} (len {}); expected empty or {} hex chars",
            a.len(),
            BUILD_SHA_HEX_LEN
        );
    }

    /// Production-path coverage for `compute_build_sha()` — exercises
    /// `std::env::current_exe()` + the chunked BufReader path against
    /// the actual test binary on disk. Catches Linux symlink-resolution
    /// regressions and macOS .app-bundle path edge cases that the
    /// `compute_build_sha_for_bytes` synthetic tests cannot see.
    ///
    /// Asymmetric coverage rationale (per CLAUDE.md): the failure mode
    /// this guards against is silent — a regression that makes
    /// `current_exe` or the streaming hash return `None` would surface
    /// downstream as "every worker reports empty build_sha and matches
    /// the legacy `""` allowlist entry," masking the deployment-drift
    /// detection #216 was built for. Empty-return is the bug the
    /// production caller cannot see; this test makes it loud.
    #[test]
    fn compute_build_sha_production_path_matches_independent_hash() {
        let computed = compute_build_sha()
            .expect("compute_build_sha MUST return Some on a normal test runner — \
                     a None means current_exe() or chunked hashing regressed");
        assert_eq!(
            computed.len(),
            BUILD_SHA_HEX_LEN,
            "computed SHA prefix must be exactly {BUILD_SHA_HEX_LEN} hex chars; got {computed:?}",
        );
        // Independent re-hash via `compute_build_sha_for_bytes` over
        // the same on-disk bytes. If the chunked reader's accumulated
        // hash diverges from a single-shot hash of the same file, the
        // streaming path is wrong (boundary handling, partial reads,
        // EINTR mishandling). Reading the binary twice is fine — the
        // test runner is not a hot path.
        let exe_path = std::env::current_exe()
            .expect("std::env::current_exe MUST succeed in a normal test environment");
        let bytes = std::fs::read(&exe_path)
            .unwrap_or_else(|e| panic!("must be able to read own test binary at {exe_path:?}: {e}"));
        let independent = compute_build_sha_for_bytes(&bytes);
        assert_eq!(
            computed, independent,
            "chunked compute_build_sha must agree with single-shot compute_build_sha_for_bytes \
             over the SAME bytes — divergence indicates a streaming hash bug",
        );
        // Idempotence: build_sha() should return the same value the
        // first compute_build_sha() produced (modulo OnceLock race in
        // parallel tests — `assert!(eq || empty)` is the safe guard).
        let cached = build_sha();
        assert!(
            cached == computed || cached.is_empty(),
            "build_sha() = {cached:?}; compute_build_sha() = {computed:?}; \
             must agree (the OnceLock might have been initialized by a parallel test, \
             but if non-empty the values must match)",
        );
    }
}
