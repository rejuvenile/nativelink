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

//! Production-composition store builders.
//!
//! The whole point of #495 is to anchor production-shape latency, not
//! abstract store latency. Production CAS uses
//!
//! ```text
//! ExistenceCache → Verify → FastSlow {
//!   fast: SizePartitioning(16MiB) → Memory(8GiB),
//!   slow: Filesystem (ZFS-backed)
//! }
//! ```
//!
//! For the Phase 1 smoke cells we approximate this with a self-contained
//! variant that uses a local tmpfs/NVMe-backed `FilesystemStore` in place
//! of the production ZFS dataset, and an in-process gRPC service as the
//! peer-mirror endpoint (when invoked from the W3 / R5 scenarios). All
//! wrappers — ExistenceCache, Verify, SizePartitioning, FastSlow —
//! are constructed from real `StoreSpec`s via the production
//! `nativelink_store::default_store_factory::store_factory`, so the
//! wrapper-chain shape is bit-identical to production CAS.

use std::sync::Arc;

use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, FilesystemSpec, MemorySpec,
    SizePartitioningSpec, StoreSpec, VerifySpec,
};
use nativelink_error::Error;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::Store;

/// Default sizes mirroring `buildcache-native.json5` / production CAS as of
/// 2026-05-16. If production tuning shifts, update these so the benches
/// continue to anchor the prod shape.
pub mod prod_defaults {
    /// Fast-tier MemoryStore cap (matches `buildcache-native.json5`
    /// `MEMORY_STORE.max_bytes`).
    pub const FAST_MEMORY_MAX_BYTES: usize = 8 * 1024 * 1024 * 1024;
    /// `SizePartitioningStore` threshold (matches
    /// `buildcache-native.json5`).
    pub const SIZE_PARTITIONING_THRESHOLD: u64 = 16 * 1024 * 1024;
    /// `ExistenceCacheStore` entry cap.
    pub const EXISTENCE_CACHE_MAX_ENTRIES: u64 = 50_000_000;
    /// Filesystem read buffer (matches the 2026-03-24 tuning bump to
    /// 3 MiB chunk reads).
    pub const FILESYSTEM_READ_BUFFER: u32 = 3 * 1024 * 1024;
    /// Production `FastSlowSpec.slow_writes_in_flight_max_bytes` per
    /// `buildcache-native.json5`. 0 (uncapped) is REJECTED by
    /// `FastSlowStore::new_validated` for disk-backed slow tiers, so
    /// every prod-composition bench must set this explicitly.
    pub const SLOW_WRITES_INFLIGHT_MAX_BYTES: u64 = 8u64 * 1024 * 1024 * 1024;
}

/// A built production-composition stack ready for a scenario to drive.
///
/// The stack is rooted at `cas_store` — the same handle a `CasServer` /
/// `ByteStreamServer` would receive in production. The `temp_dir` is
/// held to keep the filesystem store's content_path / temp_path alive
/// for the lifetime of the bench; dropping `Composition` purges them.
#[derive(Debug)]
pub struct Composition {
    /// Outermost wrapped handle. Mirrors what
    /// `bin/nativelink.rs` constructs for `cas_STORE` in production.
    pub cas_store: Store,
    /// Inner handle ONE-LEVEL into the stack for inspection (the
    /// `VerifyStore` wrapped store, with `ExistenceCache` stripped).
    /// Currently unused by scenarios; provided so future cells (e.g.
    /// existence-cache hit-rate measurement) don't need a refactor.
    pub _store_manager: Arc<StoreManager>,
    /// Backing tempdir; kept so on-disk files survive scenario runs.
    pub _temp_dir: tempfile::TempDir,
}

/// Build the prod-shaped CAS composition with a fresh on-disk
/// `FilesystemStore` slow tier. Used by W1 / R1 / F1 scenarios.
///
/// The composition shape, outer→inner:
///
/// ```text
/// ExistenceCache(50M entries)
///   → Verify(verify_size = true)
///     → FastSlow(slow_writes_in_flight_max_bytes = 8 GiB)
///         fast: SizePartitioning(16 MiB)
///                 lower: Memory(8 GiB)
///                 upper: Memory(8 GiB)    // bench-only — prod uses noop+slow
///         slow: Filesystem(<tempdir>)
/// ```
///
/// **Composition deviation from prod, documented:** production
/// `SizePartitioning.upper_store` is `Noop` (large blobs bypass the
/// fast tier). For benches we use a second small `MemoryStore` so the
/// `upper` branch's eviction / pin logic still receives traffic;
/// large-blob behavior is captured by R1's `large` cell which sits
/// above the partitioning threshold.
pub async fn build_prod_cas_composition(
    fast_tier_max_bytes: usize,
    slow_writes_inflight_max_bytes: u64,
) -> Result<Composition, Error> {
    let temp_dir = tempfile::TempDir::new().expect("tempdir creation must succeed");
    let content_path = temp_dir
        .path()
        .join("content")
        .to_string_lossy()
        .into_owned();
    let temp_path = temp_dir.path().join("temp").to_string_lossy().into_owned();

    // Create directories so FilesystemStore's startup scan finds them.
    tokio::fs::create_dir_all(&content_path).await.map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "create content_path: {e:?}",
        )
    })?;
    tokio::fs::create_dir_all(&temp_path).await.map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "create temp_path: {e:?}",
        )
    })?;

    let fs_spec = StoreSpec::Filesystem(FilesystemSpec {
        content_path,
        temp_path,
        read_buffer_size: prod_defaults::FILESYSTEM_READ_BUFFER,
        eviction_policy: Some(EvictionPolicy {
            // Match buildcache tank-pool usage budget; benches stay well
            // under this so eviction doesn't fire mid-cell. usize on
            // 64-bit; we never run benches on 32-bit hosts.
            max_bytes: 64usize * 1024 * 1024 * 1024,
            ..Default::default()
        }),
        block_size: 4096,
        max_concurrent_writes: 0,
        sync_data_only: true,
        content_is_immutable: true,
        fadvise_dontneed: false,
        max_concurrent_large_reads: 0,
        large_read_threshold_bytes: 4 * 1024 * 1024,
    });

    let fast_memory_spec = StoreSpec::Memory(MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: fast_tier_max_bytes,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    let upper_memory_spec = StoreSpec::Memory(MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            // Small — bench scenarios drive `lower` path (≤16 MiB)
            // mostly; this is here to keep the SizePartitioning shape
            // intact and exercise upper-branch pin/eviction logic on
            // the R1 `large` cell.
            max_bytes: 256 * 1024 * 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });

    let size_part_spec = StoreSpec::SizePartitioning(Box::new(SizePartitioningSpec {
        size: prod_defaults::SIZE_PARTITIONING_THRESHOLD,
        lower_store: fast_memory_spec,
        upper_store: upper_memory_spec,
    }));

    let fast_slow_spec = StoreSpec::FastSlow(Box::new(FastSlowSpec {
        fast: size_part_spec,
        fast_direction: Default::default(),
        slow: fs_spec,
        slow_direction: Default::default(),
        chunked_reads_enabled: false,
        slow_writes_in_flight_max_bytes: slow_writes_inflight_max_bytes,
    }));

    let verify_spec = StoreSpec::Verify(Box::new(VerifySpec {
        backend: fast_slow_spec,
        verify_size: true,
        verify_hash: false,
    }));

    let existence_cache_spec = StoreSpec::ExistenceCache(Box::new(ExistenceCacheSpec {
        backend: verify_spec,
        eviction_policy: Some(EvictionPolicy {
            max_count: prod_defaults::EXISTENCE_CACHE_MAX_ENTRIES,
            ..Default::default()
        }),
    }));

    let store_manager = Arc::new(StoreManager::new());
    let cas_store = store_factory(&existence_cache_spec, &store_manager, None).await?;

    Ok(Composition {
        cas_store,
        _store_manager: store_manager,
        _temp_dir: temp_dir,
    })
}

/// Slim Memory-only composition used by the chunked-v2 (W3 / R5) cells
/// which exercise a different wrapper chain (lower-level driver +
/// per-digest Notify) and so only need a fast leaf for size assertions.
///
/// **NOTE:** the W3 / R5 cells are currently scoped down to drive the
/// lower-level race state directly (see `scenarios::chunked_v2`) rather
/// than going through the full bidi RPC; this builder exists for any
/// follow-up that wires a full v2 server.
pub async fn build_memory_only(max_bytes: usize) -> Result<Composition, Error> {
    let temp_dir = tempfile::TempDir::new().expect("tempdir creation must succeed");
    let memory_spec = StoreSpec::Memory(MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    let store_manager = Arc::new(StoreManager::new());
    let cas_store = store_factory(&memory_spec, &store_manager, None).await?;
    Ok(Composition {
        cas_store,
        _store_manager: store_manager,
        _temp_dir: temp_dir,
    })
}
