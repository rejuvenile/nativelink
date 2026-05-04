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
// Phase 2.2/2.3 wires the consumer (`nativelink-service`'s WriteChunked
// RPC handler). Some items remain dead-code-allowed because they are
// future-Phase observation hooks (e.g. `chunks_received()` for the
// metric gauge wiring in Phase 2.5+).
#![allow(dead_code, reason = "Phase 2.2/2.3 wires part of this; Phase 2.5+ wires the rest (#212)")]

pub mod chunk_budget;
pub mod chunked_client;
pub mod chunked_driver;
pub mod chunked_filesystem;
pub mod chunked_read_registry;
pub mod pin_budget;

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_error::Error;
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;

/// Process-wide kill-switch for #212 Phase 2.7 Bazel-facing internal
/// chunking. Default: OFF.
///
/// When `false`, `FastSlowStore::update` for Bazel-facing writes uses
/// the legacy single-stream code path (fast tier in line + background
/// `tokio::spawn` for slow tier). When `true` AND the
/// `chunked_fast_slow` feature is compiled in AND a
/// `BazelChunkedDispatcher` has been installed AND
/// `digest.size_bytes() >= CHUNK_SIZE`, the path internally chunks the
/// in-order ByteStream into 1 MiB pieces and dispatches them through
/// the per-blob `ChunkedDriver` machinery.
///
/// Per CLAUDE.md `feedback_async_to_sync_requires_explicit_signoff`,
/// the (β) async-commit semantic of the Bazel-facing path is an
/// architectural change. **Flipping this on in production requires
/// explicit user sign-off** — the kill-switch ships OFF and stays OFF
/// until the user signs off on the `update()` returning Ok before
/// commit completes (visibility via Phase 2.5's `failed_writes` /
/// in-flight pin).
static BAZEL_FACING_INTERNAL_CHUNKING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Arm the Phase 2.7 Bazel-facing internal-chunking kill-switch.
///
/// Mirrors the verb-pair pattern used by `WorkerProxyStore`
/// (`enable_X` / `disable_X` / `X_enabled()`) and `FastSlowStore`
/// (`enable_chunked_reads` / `disable_chunked_reads` /
/// `chunked_reads_enabled()`); see `#220` D2 for the unification.
/// Idempotent. Test code uses this to toggle the switch on per-test;
/// production wires this from `GlobalConfig.bazel_facing_internal_chunking_enabled`
/// in `src/bin/nativelink.rs`.
pub fn enable_bazel_facing_internal_chunking() {
    BAZEL_FACING_INTERNAL_CHUNKING_ENABLED.store(true, Ordering::SeqCst);
}

/// Re-arm the Phase 2.7 Bazel-facing internal-chunking kill-switch
/// (operator rollback path back to the legacy single-stream code path).
/// Idempotent. Test code uses this to clean up after each test.
pub fn disable_bazel_facing_internal_chunking() {
    BAZEL_FACING_INTERNAL_CHUNKING_ENABLED.store(false, Ordering::SeqCst);
}

/// Read the current value of the Phase 2.7 Bazel-facing internal
/// chunking kill-switch.
#[must_use]
pub fn bazel_facing_internal_chunking_enabled() -> bool {
    BAZEL_FACING_INTERNAL_CHUNKING_ENABLED.load(Ordering::Acquire)
}

/// Pluggable dispatcher for Phase 2.7 Bazel-facing internal chunking.
///
/// `nativelink-store::FastSlowStore` calls this when the kill-switch is
/// on and the blob is large enough; the implementation lives in
/// `nativelink-service` (where the per-blob `ChunkedDriver` machinery
/// is wired into the RPC layer) and is INJECTED via
/// `FastSlowStore::set_bazel_chunked_dispatcher`. This avoids the
/// circular dependency that would result from `nativelink-store`
/// directly depending on `nativelink-service`.
///
/// The dispatcher MUST consume the entire `reader` (drain to EOF or
/// error). It MUST return Ok as soon as the final chunk has been
/// admitted to the per-blob `ChunkedDriver`'s mpsc — NOT after commit
/// completes. This is the (β) async-commit contract per the anti-#203
/// invariant; blocking on slow-tier latency reproduces the 2026-04-28
/// OOM cascade mechanism.
#[async_trait]
pub trait BazelChunkedDispatcher: Send + Sync + core::fmt::Debug {
    /// Dispatch the bytes from `reader` (sourced from
    /// `FastSlowStore::update`'s upstream Bazel byte stream) into the
    /// per-blob chunked driver. Returns Ok with the declared blob size
    /// AS SOON AS admission completes; the driver continues in the
    /// background.
    ///
    /// On reader error: terminate dispatch, return Err. On admission
    /// rejection (global-budget exhausted, mpsc full, concurrent
    /// duplicate): return the corresponding `Error` with
    /// `BackpressureSignal` detail (Code::ResourceExhausted /
    /// Code::Aborted).
    async fn dispatch(
        &self,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
    ) -> Result<u64, Error>;
}

/// Shared `Arc` alias for the dispatcher trait object. Used as the
/// optional field on `FastSlowStore` so the runtime decision to use
/// chunked dispatch is one atomic-load + one Option-check.
pub type BazelChunkedDispatcherArc = Arc<dyn BazelChunkedDispatcher>;

/// Fixed chunk size for the #212 chunked transport / on-disk layout.
///
/// Per design §4 Q3=(a): 1 MiB matches the h2 frame tuning (commit
/// 2026-03-24) and the ZFS `recordsize=1M` configured on
/// `fast/nativelink/work`, eliminating partial-record write
/// amplification on the slow tier. NOT per-deployment configurable —
/// changing the chunk size would change the wire-stable contract for
/// `BackpressureSignal` permit weighting AND for the on-disk sparse
/// file layout.
pub const CHUNK_SIZE: usize = 1024 * 1024;

/// Maximum blob size eligible for the worker-side chunked-write path.
///
/// Blobs larger than this fall back to the legacy in-order ByteStream
/// Write path. Cap exists because the v1 chunked client buffers the
/// entire payload in memory up-front (see
/// `chunked_client::collect_and_hash_chunks`) so retry attempts can
/// re-send from a single-pass `DropCloserReadHalf` — peak per-blob
/// memory is therefore O(blob_size). At 256 MiB the worst-case
/// per-blob client memory is bounded so concurrent multi-GB writes
/// cannot OOM a worker (#203 cascade pattern).
///
/// Streaming retry (which would eliminate this cap) is deferred to
/// Phase 2.5+; tracked alongside the upfront-buffering note in
/// `chunked_client.rs::collect_and_hash_chunks`.
pub const MAX_CHUNKED_BLOB_SIZE: u64 = 256 * 1024 * 1024;

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
