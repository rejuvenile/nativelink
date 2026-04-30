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

//! Phase 1 SKELETON for #212 chunked-streaming architecture.
//!
//! This module gathers the infrastructure scaffolding required by the
//! design doc at `.claude/plans/212-chunk-pinned-async-slow-writes.md`
//! (v4.5). The contents are gated behind the `chunked_fast_slow` feature
//! and are NOT wired into any data path yet — Phase 2 PRs will pick up
//! these primitives. Today this only adds the constant, the global
//! byte-budget Semaphore, and the per-blob driver task skeleton with
//! tests.
//!
//! NO behavior change is intended in this PR. With the feature flag off
//! the entire module is `cfg`-gated out, so production binaries are
//! byte-identical to before this commit (modulo independent test changes
//! for the `looks_like_dead_channel` classifier in `grpc_store.rs`,
//! which is the only Phase 1 change that lands in the default-features
//! build).

#![cfg(feature = "chunked_fast_slow")]
// Phase 1 ships infrastructure with no production call-sites — every
// public-in-module item is only consumed by the Phase 1 test harness.
// Phase 2 PRs replace this allow with real wiring (admission point in
// FastSlowStore, driver storage in BlobInFlightState, metric
// registration in cas_STORE).
#![allow(dead_code, reason = "Phase 1 SKELETON; consumers land in Phase 2 (#212)")]

pub(crate) mod chunk_budget;
pub(crate) mod chunked_driver;
pub(crate) mod chunked_filesystem;

/// Fixed chunk size for the #212 chunked transport / on-disk layout.
///
/// Per design §4 Q3=(a): 1 MiB matches the h2 frame tuning (commit
/// 2026-03-24) and the ZFS `recordsize=1M` configured on
/// `fast/nativelink/work`, eliminating partial-record write
/// amplification on the slow tier. NOT per-deployment configurable —
/// changing the chunk size would change the wire-stable contract for
/// `BackpressureSignal` permit weighting AND for the on-disk sparse
/// file layout.
pub(crate) const CHUNK_SIZE: usize = 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::CHUNK_SIZE;

    /// Pin the constant value so an accidental edit is caught before it
    /// silently breaks the global byte budget math
    /// (`4 GiB / CHUNK_SIZE = 4096 permits` per Q4) AND the on-disk
    /// `pwrite`-at-offset alignment (per Q2=(a)).
    #[test]
    fn chunk_size_is_one_mib() {
        assert_eq!(CHUNK_SIZE, 1024 * 1024);
        assert_eq!(CHUNK_SIZE, 1 << 20);
    }

    /// Sanity: 4 GiB budget divides cleanly into 4096 permits at the
    /// chosen chunk size — Phase 2 admission code relies on this.
    #[test]
    fn chunk_size_divides_four_gib_to_4096_permits() {
        const FOUR_GIB: usize = 4 * 1024 * 1024 * 1024;
        assert_eq!(FOUR_GIB / CHUNK_SIZE, 4096);
        assert_eq!(FOUR_GIB % CHUNK_SIZE, 0);
    }
}
