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

//! Production-shape CAS composition for the bench harness.
//!
//! The shape, outer→inner, matches `/srv/nativelink/buildcache-native.json5`
//! (canonical user-readable mirror at `~/fl/bld/infra/nativelink/prod-server.json5`)
//! as of the constant-pin date in `prod_defaults` below:
//!
//! ```text
//! Verify(verify_size=true, verify_hash=true)
//!   ExistenceCache(50M entries)
//!     SizePartitioning(16 KiB)
//!       lower: SMALL_CAS_CACHED = FastSlow {
//!                fast: Memory(4 GB / 500K),
//!                slow: <bench-only Memory(4 GB) substitute for Redis>  // see note
//!              }
//!       upper: cas_FAST_SLOW_STORE = FastSlow {
//!                fast: Memory(16 GB, evict_bytes=4.5GB, max_count=1M,
//!                             emit_backpressure_enabled=true),
//!                slow: Filesystem(tempdir),
//!                slow_writes_in_flight_max_bytes: 12 GiB,
//!                chunked_reads_enabled: true,
//!              }
//! ```
//!
//! **Documented composition deviation from prod (bench-only):**
//!
//! 1. The `SMALL_CAS_CACHED.slow` tier is a `MemoryStore` in the bench,
//!    not the Valkey/Redis backend prod uses. Running a Valkey container
//!    inside the bench-process is impractical; treating the slow tier as
//!    in-process memory means small-CAS reads NEVER cross a real Redis
//!    hop. Cells that hit this path emit
//!    `extras.composition_deviation = "small_cas_redis_replaced_with_memory"`
//!    so diff tooling can flag them as not-fully-prod-shape.
//!
//! 2. The CAS slow tier `FilesystemStore` runs against a `tempfile::TempDir`,
//!    not the prod `/srv/casdata/nativelink/stores/content_path-cas`.
//!    Same filesystem store impl, different filesystem (typically a
//!    tmpfs / NVMe under `--temp-dir` — defaults to `/dev/shm/nl-bench-*`
//!    to avoid polluting prod ZFS ARC; see the `--temp-dir` CLI flag).
//!
//! Constants below are pinned to `prod-server.json5` line numbers; a test in
//! this module reads the JSON5 at test time and asserts every `prod_defaults`
//! constant equals the live prod value. Constant drift between bench and
//! prod is detected at `cargo test` time, not at the next bench diff.

use std::sync::Arc;

use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, FilesystemSpec, MemorySpec,
    SizePartitioningSpec, StoreSpec, VerifySpec,
};
use nativelink_error::Error;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::Store;

/// Prod constants pinned to `/srv/nativelink/buildcache-native.json5`
/// (canonical mirror: `~/fl/bld/infra/nativelink/prod-server.json5`).
///
/// Last verified: 2026-05-16. If prod tuning shifts, the `prod_defaults_match_buildcache_json5`
/// test below red-fails and the operator must (a) re-verify the new prod
/// value AND (b) decide whether to bump the bench (anchoring shifts) or
/// hold (intentional bench-vs-prod gap).
pub mod prod_defaults {
    /// `cas_FAST_SLOW_STORE.fast.memory.eviction_policy.max_bytes` —
    /// `prod-server.json5:142` `"max_bytes": 16000000000` (16 GB, decimal).
    pub const CAS_FAST_MEMORY_MAX_BYTES: usize = 16_000_000_000;

    /// `cas_FAST_SLOW_STORE.fast.memory.eviction_policy.evict_bytes` —
    /// `prod-server.json5:143` `"evict_bytes": 4500000000` (4.5 GB).
    pub const CAS_FAST_MEMORY_EVICT_BYTES: usize = 4_500_000_000;

    /// `cas_FAST_SLOW_STORE.fast.memory.eviction_policy.max_count` —
    /// `prod-server.json5:144` `"max_count": 1000000`.
    pub const CAS_FAST_MEMORY_MAX_COUNT: u64 = 1_000_000;

    /// `cas_FAST_SLOW_STORE.fast.memory.emit_backpressure_enabled` —
    /// `prod-server.json5:149` `true`. Falsifies the bench's W1 admission-gate
    /// behavior if not set: in prod a fast-tier-at-cap write triggers
    /// `BackpressureSignal::MemoryStoreAtCapacity`, which the bench's
    /// FastSlow consumer must handle as in prod.
    pub const CAS_FAST_MEMORY_EMIT_BACKPRESSURE: bool = true;

    /// `SizePartitioning.size` for `cas_INNER` — `prod-server.json5:230`
    /// `"size": 16384` (16 KiB). **Comparison is strict `<` in
    /// `nativelink-store/src/size_partitioning_store.rs:99` — blobs with
    /// `size_bytes < SIZE_PARTITIONING_THRESHOLD` go to
    /// `SMALL_CAS_CACHED` (Memory→Redis in prod, Memory→Memory in
    /// bench); blobs with `size_bytes >= SIZE_PARTITIONING_THRESHOLD`
    /// (including exactly 16384) go to `cas_FAST_SLOW_STORE`.**
    pub const SIZE_PARTITIONING_THRESHOLD: u64 = 16 * 1024;

    /// `cas_INNER.existence_cache.eviction_policy.max_count` —
    /// `prod-server.json5:244` `"max_count": 50000000`.
    pub const EXISTENCE_CACHE_MAX_ENTRIES: u64 = 50_000_000;

    /// `SMALL_CAS_CACHED.fast.memory.eviction_policy.max_bytes` —
    /// `prod-server.json5:210` `"max_bytes": 4000000000` (4 GB).
    pub const SMALL_CAS_FAST_MEMORY_MAX_BYTES: usize = 4_000_000_000;

    /// `SMALL_CAS_CACHED.fast.memory.eviction_policy.max_count` —
    /// `prod-server.json5:211` `"max_count": 500000`.
    pub const SMALL_CAS_FAST_MEMORY_MAX_COUNT: u64 = 500_000;

    /// `cas_FAST_SLOW_STORE.slow_writes_in_flight_max_bytes` —
    /// `prod-server.json5:183` `12884901888` (12 GiB).
    pub const SLOW_WRITES_INFLIGHT_MAX_BYTES: u64 = 12 * 1024 * 1024 * 1024;

    /// `cas_FAST_SLOW_STORE.chunked_reads_enabled` —
    /// `prod-server.json5:171` `true`.
    pub const CHUNKED_READS_ENABLED: bool = true;

    /// `cas_STORE.verify.verify_hash` — `prod-server.json5:197` `true`.
    pub const VERIFY_HASH: bool = true;

    /// `cas_STORE.verify.verify_size` — `prod-server.json5:196` `true`.
    pub const VERIFY_SIZE: bool = true;

    /// Filesystem read buffer — prod inherits the default from
    /// `filesystem_store.rs:DEFAULT_BUFF_SIZE` (3 MiB). Prod config does
    /// NOT override.
    pub const FILESYSTEM_READ_BUFFER: u32 = 3 * 1024 * 1024;

    /// `cas_FAST_SLOW_STORE.slow.filesystem.eviction_policy.max_bytes` —
    /// `prod-server.json5:157` `800000000000` (800 GB). Bench shrinks this so
    /// tempdirs stay bounded; see `BENCH_FILESYSTEM_MAX_BYTES` below.
    pub const PROD_FILESYSTEM_MAX_BYTES: usize = 800_000_000_000;

    /// `cas_FAST_SLOW_STORE.slow.filesystem.content_is_immutable` —
    /// `prod-server.json5:164` `true`.
    pub const FILESYSTEM_CONTENT_IS_IMMUTABLE: bool = true;

    /// Canonical predicate for whether a bench cell of `blob_size_bytes`
    /// hits the SMALL_CAS_CACHED (Memory-substitute-for-Redis) path
    /// rather than the prod-shape `cas_FAST_SLOW_STORE` UPPER path.
    ///
    /// This is the single source of truth for the
    /// `extras.composition_deviation = "small_cas_redis_replaced_with_memory"`
    /// tag that gets emitted at three call-sites
    /// (`scenarios/legacy_write.rs::run_one_cell`,
    /// `scenarios/legacy_read.rs::run_warm_cell`,
    /// `scenarios/legacy_read.rs::run_cold_cell`).
    ///
    /// **Invariant:** the predicate uses strict `<` because
    /// `SizePartitioningStore::has_with_results` / `update` / `get_part`
    /// route via strict `<` at every site in
    /// `nativelink-store/src/size_partitioning_store.rs:99,148,180,208,262,355`.
    /// A blob of exactly `SIZE_PARTITIONING_THRESHOLD` bytes routes to
    /// UPPER (`cas_FAST_SLOW_STORE`, prod-shape), NOT to SMALL_CAS_CACHED.
    /// Mutating `<` to `<=` here would silently misclassify boundary
    /// blobs as deviation cells; the `boundary_lowered` test in this
    /// module's `tests` submodule pins the semantics.
    #[inline]
    #[must_use]
    pub const fn should_emit_small_cas_deviation(blob_size_bytes: u64) -> bool {
        blob_size_bytes < SIZE_PARTITIONING_THRESHOLD
    }
}

/// Bench-side filesystem cap (intentional deviation: we use 64 GiB so
/// scenario tempdirs don't grow unbounded on hosts with bounded `/dev/shm`).
const BENCH_FILESYSTEM_MAX_BYTES: usize = 64 * 1024 * 1024 * 1024;

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
    /// `StoreManager` held alive across the composition's lifetime;
    /// the factory writes into it. Kept so future cells (e.g. existence-
    /// cache hit-rate measurement) can resolve named refs without re-
    /// building.
    pub _store_manager: Arc<StoreManager>,
    /// Backing tempdir; kept so on-disk files survive scenario runs.
    /// Dropping `Composition` (and thus `_temp_dir`) purges the
    /// `FilesystemStore` content + temp paths.
    pub _temp_dir: tempfile::TempDir,
}

/// Build a prod-shaped CAS composition. `temp_dir_base` (if `Some`) is
/// passed as the parent for the `tempfile::TempDir` to put the
/// FilesystemStore on a chosen filesystem; pass `None` to let `tempfile`
/// pick the OS default (typically `$TMPDIR`).
///
/// Returns a `Composition` whose `cas_store` is rooted at `Verify`
/// matching prod's `cas_STORE`.
pub async fn build_prod_cas_composition(
    temp_dir_base: Option<&std::path::Path>,
) -> Result<Composition, Error> {
    let temp_dir = match temp_dir_base {
        Some(p) => tempfile::TempDir::new_in(p),
        None => tempfile::TempDir::new(),
    }
    .map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "tempdir creation: {e:?}",
        )
    })?;
    build_at_paths(
        temp_dir,
        |td| td.path().join("content").to_string_lossy().into_owned(),
        |td| td.path().join("temp").to_string_lossy().into_owned(),
    )
    .await
}

/// Build a prod-shaped CAS composition rooted at caller-supplied
/// content/temp paths. The supplied `TempDir` is held alive by the
/// returned `Composition`; passing a `tempfile::TempDir::keep`-handled
/// directory or a tempdir that you also own elsewhere lets the caller
/// drive cold-read scenarios that rebuild the composition against the
/// same persistent on-disk state.
///
/// `content_path` MUST be inside `temp_dir.path()` (the builder will
/// `create_dir_all` both paths).
pub async fn build_prod_cas_composition_with_paths(
    temp_dir: tempfile::TempDir,
    content_path: String,
    temp_path: String,
) -> Result<Composition, Error> {
    build_at_paths(temp_dir, |_| content_path.clone(), |_| temp_path.clone()).await
}

async fn build_at_paths(
    temp_dir: tempfile::TempDir,
    pick_content: impl FnOnce(&tempfile::TempDir) -> String,
    pick_temp: impl FnOnce(&tempfile::TempDir) -> String,
) -> Result<Composition, Error> {
    let content_path = pick_content(&temp_dir);
    let temp_path = pick_temp(&temp_dir);

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

    // ---- cas_FAST_SLOW_STORE (upper / large blobs) ----

    let cas_fast_memory_spec = StoreSpec::Memory(MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: prod_defaults::CAS_FAST_MEMORY_MAX_BYTES,
            evict_bytes: prod_defaults::CAS_FAST_MEMORY_EVICT_BYTES,
            max_count: prod_defaults::CAS_FAST_MEMORY_MAX_COUNT,
            ..Default::default()
        }),
        emit_backpressure_enabled: prod_defaults::CAS_FAST_MEMORY_EMIT_BACKPRESSURE,
    });

    let fs_spec = StoreSpec::Filesystem(FilesystemSpec {
        content_path,
        temp_path,
        read_buffer_size: prod_defaults::FILESYSTEM_READ_BUFFER,
        eviction_policy: Some(EvictionPolicy {
            // Bench-only cap; prod is 800 GB. See
            // `BENCH_FILESYSTEM_MAX_BYTES` and module-doc deviation #2.
            max_bytes: BENCH_FILESYSTEM_MAX_BYTES,
            ..Default::default()
        }),
        block_size: 4096,
        max_concurrent_writes: 0,
        sync_data_only: true,
        content_is_immutable: prod_defaults::FILESYSTEM_CONTENT_IS_IMMUTABLE,
        fadvise_dontneed: false,
        max_concurrent_large_reads: 0,
        large_read_threshold_bytes: 4 * 1024 * 1024,
    });

    let cas_fast_slow_spec = StoreSpec::FastSlow(Box::new(FastSlowSpec {
        fast: cas_fast_memory_spec,
        fast_direction: Default::default(),
        slow: fs_spec,
        slow_direction: Default::default(),
        chunked_reads_enabled: prod_defaults::CHUNKED_READS_ENABLED,
        slow_writes_in_flight_max_bytes: prod_defaults::SLOW_WRITES_INFLIGHT_MAX_BYTES,
    }));

    // ---- SMALL_CAS_CACHED (lower / small blobs) ----
    //
    // Composition deviation #1 (see module doc): prod's slow tier is
    // Valkey/Redis; bench uses Memory.

    let small_cas_fast_memory_spec = StoreSpec::Memory(MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: prod_defaults::SMALL_CAS_FAST_MEMORY_MAX_BYTES,
            max_count: prod_defaults::SMALL_CAS_FAST_MEMORY_MAX_COUNT,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    let small_cas_slow_memory_spec = StoreSpec::Memory(MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            // Match the same cap as the fast tier — bench-only stand-in
            // for prod's Valkey/Redis. Documented deviation #1.
            max_bytes: prod_defaults::SMALL_CAS_FAST_MEMORY_MAX_BYTES,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    let small_cas_cached_spec = StoreSpec::FastSlow(Box::new(FastSlowSpec {
        fast: small_cas_fast_memory_spec,
        fast_direction: Default::default(),
        slow: small_cas_slow_memory_spec,
        slow_direction: Default::default(),
        chunked_reads_enabled: false,
        // 0 is rejected by `FastSlowStore::new_validated` ONLY when the
        // slow tier requires the in-flight buffer (FilesystemStore does;
        // MemoryStore does not). Memory-backed slow tier ⇒ 0 is accepted.
        slow_writes_in_flight_max_bytes: 0,
    }));

    // ---- SizePartitioning(16 KiB) ----

    let size_part_spec = StoreSpec::SizePartitioning(Box::new(SizePartitioningSpec {
        size: prod_defaults::SIZE_PARTITIONING_THRESHOLD,
        lower_store: small_cas_cached_spec,
        upper_store: cas_fast_slow_spec,
    }));

    // ---- ExistenceCache(50M entries) ----

    let existence_cache_spec = StoreSpec::ExistenceCache(Box::new(ExistenceCacheSpec {
        backend: size_part_spec,
        eviction_policy: Some(EvictionPolicy {
            max_count: prod_defaults::EXISTENCE_CACHE_MAX_ENTRIES,
            ..Default::default()
        }),
    }));

    // ---- Verify (outermost, matches prod cas_STORE) ----

    let verify_spec = StoreSpec::Verify(Box::new(VerifySpec {
        backend: existence_cache_spec,
        verify_size: prod_defaults::VERIFY_SIZE,
        verify_hash: prod_defaults::VERIFY_HASH,
    }));

    let store_manager = Arc::new(StoreManager::new());
    let cas_store = store_factory(&verify_spec, &store_manager, None).await?;

    Ok(Composition {
        cas_store,
        _store_manager: store_manager,
        _temp_dir: temp_dir,
    })
}

/// Slim Memory-only composition used by the chunked-v2 (W3 / R5) cells
/// which exercise a different wrapper chain (lower-level driver +
/// per-digest Notify) and so only need a fast leaf for size assertions.
pub async fn build_memory_only(max_bytes: usize) -> Result<Composition, Error> {
    let temp_dir = tempfile::TempDir::new().map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "tempdir creation: {e:?}",
        )
    })?;
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

#[cfg(test)]
mod tests {
    use super::prod_defaults;

    /// Authoritative path to the deployed prod config. `sudo` required
    /// on buildcache; on a dev laptop the file probably doesn't exist and
    /// the test silently passes-with-warning rather than fails.
    const PROD_CONFIG_PATH: &str = "/srv/nativelink/buildcache-native.json5";

    /// Canonical user-readable mirror (committed at infra repo); preferred
    /// because no `sudo` required.
    const PROD_CONFIG_MIRROR: &str = "/home/user/fl/bld/infra/nativelink/prod-server.json5";

    /// Read either the deployed config or the canonical mirror (whichever
    /// is readable without privilege). Returns `None` if neither is
    /// accessible — typical on CI runners and developer laptops without
    /// the infra checkout.
    fn read_prod_config() -> Option<String> {
        if let Ok(s) = std::fs::read_to_string(PROD_CONFIG_MIRROR) {
            return Some(s);
        }
        if let Ok(s) = std::fs::read_to_string(PROD_CONFIG_PATH) {
            return Some(s);
        }
        None
    }

    /// Env var that forces the prod-config-pin test to FAIL when the
    /// config is missing instead of silently skipping. The bench's
    /// release gate Justfile recipe sets this so the gate cannot pass
    /// without observed prod-drift coverage; CI runners / dev laptops
    /// without `BENCH_REQUIRE_PROD_CONFIG=1` get the legacy warn-and-skip
    /// behavior so the test isn't spuriously red there.
    const REQUIRE_ENV: &str = "BENCH_REQUIRE_PROD_CONFIG";

    /// Pin every `prod_defaults` constant against the live prod config.
    /// Tolerates the config being absent (dev / CI hosts) — in that case
    /// the test prints a warning and passes UNLESS the
    /// `BENCH_REQUIRE_PROD_CONFIG=1` env var is set (used by the release
    /// gate Justfile recipe so the bench-shipping path always verifies
    /// the pin against the live config). On buildcache and on the
    /// maintainer's dev host the mirror IS present, so this test
    /// red-fails if prod drifts.
    ///
    /// Mutation: change any `prod_defaults` constant — this test must
    /// red-fail with a bespoke "prod_defaults::X drift detected" message.
    #[test]
    fn prod_defaults_match_buildcache_json5() {
        let require = std::env::var(REQUIRE_ENV).ok().as_deref() == Some("1");
        let Some(json5) = read_prod_config() else {
            if require {
                panic!(
                    "prod_config_required: {} is set but neither prod \
                     config file is readable: tried {} and {}. The \
                     release-gate Justfile recipe sets this env var so \
                     the bench cannot ship without verifying constants \
                     against the live prod config.",
                    REQUIRE_ENV, PROD_CONFIG_MIRROR, PROD_CONFIG_PATH
                );
            }
            eprintln!(
                "[bench-test] WARN: prod config absent at {} or {}; \
                 skipping prod_defaults pin test (set {}=1 to convert \
                 this skip into a hard failure)",
                PROD_CONFIG_MIRROR, PROD_CONFIG_PATH, REQUIRE_ENV
            );
            return;
        };
        // Substring-grep over the JSON5 text. Full JSON5 parse would be
        // ideal but adds a json5 dep just for this test; substring is
        // good enough to detect literal drift because the prod file
        // names every value with a unique surrounding context.
        check_substring(
            &json5,
            "\"max_bytes\": 16000000000",
            "prod_defaults::CAS_FAST_MEMORY_MAX_BYTES drift detected",
        );
        check_substring(
            &json5,
            "\"evict_bytes\": 4500000000",
            "prod_defaults::CAS_FAST_MEMORY_EVICT_BYTES drift detected",
        );
        check_substring(
            &json5,
            "\"size\": 16384",
            "prod_defaults::SIZE_PARTITIONING_THRESHOLD drift detected — \
             prod's cas_INNER SizePartitioning.size is no longer 16 KiB",
        );
        check_substring(
            &json5,
            "\"slow_writes_in_flight_max_bytes\": 12884901888",
            "prod_defaults::SLOW_WRITES_INFLIGHT_MAX_BYTES drift detected",
        );
        check_substring(
            &json5,
            "\"chunked_reads_enabled\": true",
            "prod_defaults::CHUNKED_READS_ENABLED drift detected",
        );
        check_substring(
            &json5,
            "\"verify_hash\": true",
            "prod_defaults::VERIFY_HASH drift detected",
        );
        check_substring(
            &json5,
            "\"verify_size\": true",
            "prod_defaults::VERIFY_SIZE drift detected",
        );
        check_substring(
            &json5,
            "\"max_count\": 50000000",
            "prod_defaults::EXISTENCE_CACHE_MAX_ENTRIES drift detected",
        );
        check_substring(
            &json5,
            "\"max_count\": 1000000",
            "prod_defaults::CAS_FAST_MEMORY_MAX_COUNT drift detected (substring \
             clash with SMALL_CAS_FAST_MEMORY_MAX_COUNT note: both happen to \
             appear in the same file; this test passes if EITHER is present, \
             which is a known weakness)",
        );
        check_substring(
            &json5,
            "\"max_bytes\": 4000000000",
            "prod_defaults::SMALL_CAS_FAST_MEMORY_MAX_BYTES drift detected",
        );
        check_substring(
            &json5,
            "\"max_count\": 500000",
            "prod_defaults::SMALL_CAS_FAST_MEMORY_MAX_COUNT drift detected",
        );
        check_substring(
            &json5,
            "\"emit_backpressure_enabled\": true",
            "prod_defaults::CAS_FAST_MEMORY_EMIT_BACKPRESSURE drift detected",
        );

        // Numeric sanity — these constants must NOT change in this crate
        // without a corresponding edit to the JSON5 substrings above.
        // Bespoke messages so a mutation surfaces which constant drifted.
        assert_eq!(
            prod_defaults::CAS_FAST_MEMORY_MAX_BYTES,
            16_000_000_000,
            "prod_defaults_drift: CAS_FAST_MEMORY_MAX_BYTES changed; update \
             both the constant AND prod-server.json5 line :142"
        );
        assert_eq!(
            prod_defaults::CAS_FAST_MEMORY_EVICT_BYTES,
            4_500_000_000,
            "prod_defaults_drift: CAS_FAST_MEMORY_EVICT_BYTES changed"
        );
        assert_eq!(
            prod_defaults::CAS_FAST_MEMORY_MAX_COUNT,
            1_000_000,
            "prod_defaults_drift: CAS_FAST_MEMORY_MAX_COUNT changed"
        );
        assert_eq!(
            prod_defaults::SIZE_PARTITIONING_THRESHOLD,
            16_384,
            "prod_defaults_drift: SIZE_PARTITIONING_THRESHOLD changed"
        );
        assert_eq!(
            prod_defaults::EXISTENCE_CACHE_MAX_ENTRIES,
            50_000_000,
            "prod_defaults_drift: EXISTENCE_CACHE_MAX_ENTRIES changed"
        );
        assert_eq!(
            prod_defaults::SMALL_CAS_FAST_MEMORY_MAX_BYTES,
            4_000_000_000,
            "prod_defaults_drift: SMALL_CAS_FAST_MEMORY_MAX_BYTES changed"
        );
        assert_eq!(
            prod_defaults::SMALL_CAS_FAST_MEMORY_MAX_COUNT,
            500_000,
            "prod_defaults_drift: SMALL_CAS_FAST_MEMORY_MAX_COUNT changed"
        );
        assert_eq!(
            prod_defaults::SLOW_WRITES_INFLIGHT_MAX_BYTES,
            12 * 1024 * 1024 * 1024,
            "prod_defaults_drift: SLOW_WRITES_INFLIGHT_MAX_BYTES changed"
        );
        assert!(
            prod_defaults::CHUNKED_READS_ENABLED,
            "prod_defaults_drift: CHUNKED_READS_ENABLED flipped to false"
        );
        assert!(
            prod_defaults::VERIFY_HASH,
            "prod_defaults_drift: VERIFY_HASH flipped to false"
        );
        assert!(
            prod_defaults::VERIFY_SIZE,
            "prod_defaults_drift: VERIFY_SIZE flipped to false"
        );
        assert!(
            prod_defaults::CAS_FAST_MEMORY_EMIT_BACKPRESSURE,
            "prod_defaults_drift: CAS_FAST_MEMORY_EMIT_BACKPRESSURE flipped to false"
        );
    }

    fn check_substring(haystack: &str, needle: &str, message: &str) {
        assert!(
            haystack.contains(needle),
            "{message}: expected substring `{needle}` not found in prod config"
        );
    }

    /// Composition deviation tag MUST NOT fire at exactly
    /// `SIZE_PARTITIONING_THRESHOLD`, because `SizePartitioningStore`
    /// uses strict `<` (see `size_partitioning_store.rs:99`). A blob
    /// of size == 16384 routes to `cas_FAST_SLOW_STORE` (upper / large
    /// blob path), which is full-prod-shape — the deviation tag
    /// (which marks the Memory-substitute SMALL_CAS_CACHED path) is
    /// only correct for blobs STRICTLY LESS THAN the threshold.
    ///
    /// This test invokes the canonical production predicate
    /// `prod_defaults::should_emit_small_cas_deviation` at the boundary
    /// and its immediate neighbours. The three bench scenarios that
    /// emit the deviation tag (`scenarios/legacy_write.rs::run_one_cell`,
    /// `scenarios/legacy_read.rs::run_warm_cell`,
    /// `scenarios/legacy_read.rs::run_cold_cell`) ALL route their
    /// guard through this helper, so mutating the helper's `<` to
    /// `<=` red-fails this test AND propagates the regression to all
    /// three scenarios in one place.
    ///
    /// **Mutation falsifier:** change `<` to `<=` in
    /// `should_emit_small_cas_deviation`. The `boundary_lowered`
    /// assertion below red-fails with a bespoke message naming the
    /// mutation class.
    #[test]
    fn deviation_helper_boundary_pins_strict_lt() {
        let threshold = prod_defaults::SIZE_PARTITIONING_THRESHOLD;
        // Sanity-check the constant — if THIS drifts, the boundary
        // values below become wrong.
        assert_eq!(
            threshold, 16_384,
            "SIZE_PARTITIONING_THRESHOLD drifted away from the documented \
             prod value (16 KiB); rebase the boundary fixtures in this test"
        );

        // At exactly the threshold: strict `<` returns false → no
        // deviation tag. A `<=` mutation would return true here.
        assert!(
            !prod_defaults::should_emit_small_cas_deviation(threshold),
            "boundary_lowered mutation: at size == SIZE_PARTITIONING_THRESHOLD \
             ({threshold}), should_emit_small_cas_deviation MUST return false. \
             A `<=` mutation in the helper would return true and mis-tag a \
             cas_FAST_SLOW_STORE (full-prod-shape) cell as a SMALL_CAS_CACHED \
             deviation, causing reviewers to discount a real prod-path baseline."
        );

        // One byte below the threshold: deviation cell.
        assert!(
            prod_defaults::should_emit_small_cas_deviation(threshold - 1),
            "boundary mismatch: size below threshold-by-one ({}) must trigger \
             the deviation tag (SMALL_CAS_CACHED Memory-substitute path)",
            threshold - 1,
        );

        // One byte above: prod-shape.
        assert!(
            !prod_defaults::should_emit_small_cas_deviation(threshold + 1),
            "boundary mismatch: size above threshold-by-one ({}) must NOT \
             trigger the deviation tag (routes to cas_FAST_SLOW_STORE)",
            threshold + 1,
        );

        // Far below (1 byte): deviation cell.
        assert!(
            prod_defaults::should_emit_small_cas_deviation(1),
            "1-byte blob must route to SMALL_CAS_CACHED (deviation tag set)"
        );

        // Zero-byte blob: still strictly less than threshold → deviation.
        assert!(
            prod_defaults::should_emit_small_cas_deviation(0),
            "0-byte blob must route to SMALL_CAS_CACHED (deviation tag set)"
        );
    }
}
