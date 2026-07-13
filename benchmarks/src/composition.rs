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
//! The bench composes the **same** store stack production composes:
//! `benchmarks/prod-server.json5` is a snapshot of the deployed config
//! (`/home/user/fl/bld/infra/nativelink/prod-server.json5`) parsed into a
//! `CasConfig` and fed through the **same** `build_store_manager`
//! function `src/bin/nativelink.rs::inner_main` calls. The hand-mirrored
//! ~200-LOC spec construction the bench used through #589 is gone; the
//! drift class (`prod_defaults_match_buildcache_json5` substring grep
//! catching new fields one commit late) is impossible by construction.
//!
//! **Documented composition deviations from prod (bench-only):**
//!
//! 1. The two `RedisStore` declarations (`REDIS_AC_STORE`,
//!    `REDIS_CAS_SMALL_STORE`) are replaced with `MemoryStore` of
//!    equivalent caps before the spec list reaches `build_store_manager`.
//!    `RedisStore::new_standard` dials the Redis socket synchronously
//!    at construction; running a Valkey container inside the bench
//!    process is impractical. Cells whose blob size routes through the
//!    `SMALL_CAS_CACHED` chain (size < 16 KiB) emit
//!    `extras.composition_deviation = "small_cas_redis_replaced_with_memory"`
//!    so diff tooling can flag them as not-fully-prod-shape.
//!
//! 2. The `FilesystemStore` `content_path`/`temp_path` (prod:
//!    `/srv/casdata/nativelink/stores/{content_path,tmp_path}-cas`)
//!    are rewritten to caller-supplied tempdir paths, and the
//!    `eviction_policy.max_bytes` is clamped from prod's 800 GB to
//!    `BENCH_FILESYSTEM_MAX_BYTES` (64 GiB) so scenario tempdirs stay
//!    bounded on `/dev/shm`.
//!
//! All other fields (eviction caps, threshold, backpressure flag,
//! verify_size/verify_hash, chunked_reads_enabled,
//! slow_writes_in_flight_max_bytes) come straight from the parsed
//! snapshot — drift between the snapshot and the live prod config is
//! a code-review concern, not a runtime mismatch.

use std::path::Path;
use std::sync::Arc;

use async_lock::Mutex as AsyncMutex;
use nativelink_config::cas_server::{CasConfig, StoreConfig};
use nativelink_config::stores::{EvictionPolicy, MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_store::store_manager::{StoreManager, build_store_manager};
use nativelink_util::health_utils::HealthRegistryBuilder;
use nativelink_util::store_trait::Store;

/// Snapshot of the deployed config. Committed into the bench tree so the
/// bench doesn't depend on an out-of-workspace file at runtime; rebuild
/// fails immediately if the snapshot doesn't parse against the current
/// `CasConfig` schema.
///
/// Refresh procedure when prod tuning shifts: copy
/// `/home/user/fl/bld/infra/nativelink/prod-server.json5` over
/// `benchmarks/prod-server.json5`, re-run `cargo test -p nativelink-benchmarks`.
const PROD_SERVER_JSON5: &str = include_str!("../prod-server.json5");

/// Bench-side filesystem cap (intentional deviation #2: prod is 800 GB).
const BENCH_FILESYSTEM_MAX_BYTES: usize = 64 * 1024 * 1024 * 1024;

/// Name of the prod CAS root store. The bench resolves this from the
/// populated `StoreManager` to obtain the same outer wrapper a
/// production `CasServer` / `ByteStreamServer` would receive.
const CAS_ROOT_STORE_NAME: &str = "cas_STORE";

/// Names of the two prod Redis backends the bench substitutes. Any new
/// Redis backend introduced in prod-server.json5 will be missed by this
/// substitution and the bench will hang dialing Redis at startup — fail
/// loud test below pins this assumption.
const REDIS_STORE_NAMES: &[&str] = &["REDIS_AC_STORE", "REDIS_CAS_SMALL_STORE"];

/// A built production-composition stack ready for a scenario to drive.
///
/// The stack is rooted at `cas_store` — the same handle a `CasServer` /
/// `ByteStreamServer` would receive in production. The `temp_dir` is
/// held to keep the filesystem store's content_path / temp_path alive
/// for the lifetime of the bench; dropping `Composition` purges them.
#[derive(Debug)]
pub struct Composition {
    /// Outermost wrapped handle. Same `cas_STORE` a production
    /// `CasServer` / `ByteStreamServer` receives.
    pub cas_store: Store,
    /// `StoreManager` held alive across the composition's lifetime;
    /// `build_store_manager` writes into it. Kept so future cells (e.g.
    /// existence-cache hit-rate measurement) can resolve named refs
    /// without re-building.
    pub _store_manager: Arc<StoreManager>,
    /// Backing tempdir; kept so on-disk files survive scenario runs.
    /// Dropping `Composition` (and thus `_temp_dir`) purges the
    /// `FilesystemStore` content + temp paths.
    pub _temp_dir: tempfile::TempDir,
    /// `SizePartitioning.size` from the parsed snapshot (prod:
    /// `prod-server.json5:230` `"size": 16384`). Used by scenarios to (a)
    /// decide whether a cell routes through the SMALL_CAS_CACHED
    /// (deviation-tagged) path, and (b) annotate metric extras with
    /// the live threshold value.
    pub size_partitioning_threshold: u64,
    /// `cas_FAST_SLOW_STORE.fast.memory.eviction_policy.max_bytes` from
    /// the parsed snapshot. Recorded in W1 metric extras so a baseline
    /// reader can correlate observed throughput with the fast-tier cap
    /// the bench actually ran with — not a constant pinned in code.
    pub cas_fast_memory_max_bytes: usize,
}

impl Composition {
    /// Canonical predicate for whether a bench cell of `blob_size_bytes`
    /// hits the SMALL_CAS_CACHED (Memory-substitute-for-Redis) path
    /// rather than the prod-shape `cas_FAST_SLOW_STORE` UPPER path.
    ///
    /// Uses strict `<` because `SizePartitioningStore::has_with_results`
    /// / `update` / `get_part` route via strict `<` at every site in
    /// `nativelink-store/src/size_partitioning_store.rs:99,148,180,208,262,355`.
    /// A blob of exactly `size_partitioning_threshold` bytes routes to
    /// UPPER (`cas_FAST_SLOW_STORE`, prod-shape), NOT to SMALL_CAS_CACHED.
    /// Mutating `<` to `<=` here would silently misclassify boundary
    /// blobs as deviation cells; the boundary test in this module's
    /// `tests` submodule pins the semantics.
    #[inline]
    #[must_use]
    pub fn should_emit_small_cas_deviation(&self, blob_size_bytes: u64) -> bool {
        blob_size_bytes < self.size_partitioning_threshold
    }
}

/// Build a prod-shaped CAS composition by parsing the snapshot of
/// `prod-server.json5` and feeding it through the same
/// `nativelink_store::store_manager::build_store_manager` that
/// `src/bin/nativelink.rs::inner_main` calls.
///
/// `temp_dir_base` (if `Some`) is passed as the parent for the
/// `tempfile::TempDir` so a caller can choose which filesystem the
/// FilesystemStore lives on; pass `None` to let `tempfile` pick the
/// OS default (typically `$TMPDIR`).
pub async fn build_prod_cas_composition(
    temp_dir_base: Option<&Path>,
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
    let content_path = temp_dir.path().join("content").to_string_lossy().into_owned();
    let temp_path = temp_dir.path().join("temp").to_string_lossy().into_owned();
    build_at_paths(temp_dir, content_path, temp_path).await
}

/// Build a prod-shaped CAS composition rooted at caller-supplied
/// content/temp paths. The supplied `TempDir` is held alive by the
/// returned `Composition`; passing a `tempfile::TempDir::keep`-handled
/// directory or a tempdir that you also own elsewhere lets the caller
/// drive cold-read scenarios that rebuild the composition against the
/// same persistent on-disk state.
pub async fn build_prod_cas_composition_with_paths(
    temp_dir: tempfile::TempDir,
    content_path: String,
    temp_path: String,
) -> Result<Composition, Error> {
    build_at_paths(temp_dir, content_path, temp_path).await
}

async fn build_at_paths(
    temp_dir: tempfile::TempDir,
    content_path: String,
    temp_path: String,
) -> Result<Composition, Error> {
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

    let cas_config: CasConfig =
        serde_json5::from_str(PROD_SERVER_JSON5).map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "parse benchmarks/prod-server.json5 against current CasConfig schema \
                 (snapshot may be stale — refresh from \
                 /home/user/fl/bld/infra/nativelink/prod-server.json5): {e:?}",
            )
        })?;

    let mut stores = cas_config.stores;
    apply_bench_overrides(&mut stores, &content_path, &temp_path);

    // Extract values needed by scenarios for metric extras + routing,
    // BEFORE the spec list is consumed by `build_store_manager`. These
    // are pulled by name + structural walk so the bench reports values
    // from the **actually-running** composition, not constants that can
    // drift.
    let size_partitioning_threshold =
        extract_size_partitioning_threshold(&stores).ok_or_else(|| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "size_partitioning threshold not found in any store spec — \
                 prod composition shape may have changed; expected the \
                 SizePartitioning node inside cas_INNER",
            )
        })?;
    let cas_fast_memory_max_bytes =
        extract_cas_fast_memory_max_bytes(&stores).ok_or_else(|| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "cas_FAST_SLOW_STORE.fast.memory.eviction_policy.max_bytes \
                 not found — prod composition shape may have changed",
            )
        })?;

    // Caller-owned health registry — bench has no live health endpoint,
    // but `store_factory` requires a registry to register stores under.
    let health_registry_builder =
        Arc::new(AsyncMutex::new(HealthRegistryBuilder::new("nativelink-bench")));

    let store_manager = build_store_manager(&stores, &health_registry_builder).await?;

    let cas_store = store_manager.get_store(CAS_ROOT_STORE_NAME).ok_or_else(|| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "snapshot does not declare a store named `{CAS_ROOT_STORE_NAME}` — \
             the bench resolves the prod outer wrapper by this name; \
             prod-server.json5 snapshot may be malformed",
        )
    })?;

    Ok(Composition {
        cas_store,
        _store_manager: store_manager,
        _temp_dir: temp_dir,
        size_partitioning_threshold,
        cas_fast_memory_max_bytes,
    })
}

/// Mutate the parsed prod spec list in place: replace any
/// `REDIS_AC_STORE` / `REDIS_CAS_SMALL_STORE` declaration with a
/// `MemoryStore` of the same cap so the bench process never dials
/// Redis; rewrite every `FilesystemStore`'s `content_path` /
/// `temp_path` to caller-supplied tempdir paths and clamp its
/// `eviction_policy.max_bytes` to `BENCH_FILESYSTEM_MAX_BYTES`. Both
/// substitutions are deviations #1/#2 documented at module top.
fn apply_bench_overrides(
    stores: &mut [StoreConfig],
    content_path: &str,
    temp_path: &str,
) {
    for store_config in stores.iter_mut() {
        if REDIS_STORE_NAMES.contains(&store_config.name.as_str()) {
            // Cap mirrors the SMALL_CAS_CACHED fast-tier — the slow tier
            // is bench-only, so size it the same as the fast tier it
            // backs (legacy bench used this exact pattern).
            store_config.spec = StoreSpec::Memory(MemorySpec {
                eviction_policy: Some(EvictionPolicy {
                    max_bytes: 4_000_000_000,
                    ..Default::default()
                }),
                emit_backpressure_enabled: false,
            });
            continue;
        }
        rewrite_filesystem_paths(&mut store_config.spec, content_path, temp_path);
    }
}

/// Recursively walk the spec tree rooted at `spec` and rewrite the
/// `content_path` / `temp_path` of every `FilesystemStore` to point at
/// the bench's tempdir, while clamping its `eviction_policy.max_bytes`
/// to `BENCH_FILESYSTEM_MAX_BYTES`. Other `FilesystemSpec` fields
/// (block_size, read_buffer_size, content_is_immutable, etc.) come from
/// the parsed snapshot unmodified.
fn rewrite_filesystem_paths(spec: &mut StoreSpec, content_path: &str, temp_path: &str) {
    match spec {
        StoreSpec::Filesystem(fs_spec) => {
            fs_spec.content_path = content_path.to_string();
            fs_spec.temp_path = temp_path.to_string();
            if let Some(policy) = fs_spec.eviction_policy.as_mut() {
                if policy.max_bytes > BENCH_FILESYSTEM_MAX_BYTES {
                    policy.max_bytes = BENCH_FILESYSTEM_MAX_BYTES;
                }
            }
        }
        StoreSpec::FastSlow(fs_spec) => {
            rewrite_filesystem_paths(&mut fs_spec.fast, content_path, temp_path);
            rewrite_filesystem_paths(&mut fs_spec.slow, content_path, temp_path);
        }
        StoreSpec::Verify(v_spec) => {
            rewrite_filesystem_paths(&mut v_spec.backend, content_path, temp_path);
        }
        StoreSpec::ExistenceCache(ec_spec) => {
            rewrite_filesystem_paths(&mut ec_spec.backend, content_path, temp_path);
        }
        StoreSpec::SizePartitioning(sp_spec) => {
            rewrite_filesystem_paths(&mut sp_spec.lower_store, content_path, temp_path);
            rewrite_filesystem_paths(&mut sp_spec.upper_store, content_path, temp_path);
        }
        StoreSpec::Compression(c_spec) => {
            rewrite_filesystem_paths(&mut c_spec.backend, content_path, temp_path);
        }
        StoreSpec::CacheMetrics(cm_spec) => {
            rewrite_filesystem_paths(&mut cm_spec.backend, content_path, temp_path);
        }
        StoreSpec::Dedup(d_spec) => {
            rewrite_filesystem_paths(&mut d_spec.index_store, content_path, temp_path);
            rewrite_filesystem_paths(&mut d_spec.content_store, content_path, temp_path);
        }
        StoreSpec::CompletenessChecking(cc_spec) => {
            rewrite_filesystem_paths(&mut cc_spec.backend, content_path, temp_path);
            rewrite_filesystem_paths(&mut cc_spec.cas_store, content_path, temp_path);
        }
        StoreSpec::Shard(sh_spec) => {
            for shard in sh_spec.stores.iter_mut() {
                rewrite_filesystem_paths(&mut shard.store, content_path, temp_path);
            }
        }
        // Leaves with no Filesystem children. `RefStore` resolves by
        // name at runtime against `StoreManager`; its `Filesystem`
        // children (if any) are reached via the resolved store's own
        // top-level entry, which we already iterated.
        StoreSpec::Memory(_)
        | StoreSpec::RedisStore(_)
        | StoreSpec::RefStore(_)
        | StoreSpec::Noop(_)
        | StoreSpec::Grpc(_)
        | StoreSpec::ExperimentalMongo(_)
        | StoreSpec::ExperimentalCloudObjectStore(_)
        | StoreSpec::OntapS3ExistenceCache(_) => {}
    }
}

/// Find the `SizePartitioning.size` in the parsed spec tree. The prod
/// composition has exactly one (inside `cas_INNER`); we return the first
/// match anywhere in any store's spec tree.
fn extract_size_partitioning_threshold(stores: &[StoreConfig]) -> Option<u64> {
    for store_config in stores {
        if let Some(t) = find_size_partitioning(&store_config.spec) {
            return Some(t);
        }
    }
    None
}

fn find_size_partitioning(spec: &StoreSpec) -> Option<u64> {
    match spec {
        StoreSpec::SizePartitioning(sp) => Some(sp.size),
        StoreSpec::Verify(v) => find_size_partitioning(&v.backend),
        StoreSpec::ExistenceCache(ec) => find_size_partitioning(&ec.backend),
        StoreSpec::FastSlow(fs) => {
            find_size_partitioning(&fs.fast).or_else(|| find_size_partitioning(&fs.slow))
        }
        StoreSpec::Compression(c) => find_size_partitioning(&c.backend),
        StoreSpec::Dedup(d) => find_size_partitioning(&d.index_store)
            .or_else(|| find_size_partitioning(&d.content_store)),
        StoreSpec::CompletenessChecking(cc) => find_size_partitioning(&cc.backend)
            .or_else(|| find_size_partitioning(&cc.cas_store)),
        StoreSpec::Shard(sh) => sh
            .stores
            .iter()
            .find_map(|s| find_size_partitioning(&s.store)),
        _ => None,
    }
}

/// Locate `cas_FAST_SLOW_STORE.fast.memory.eviction_policy.max_bytes`
/// in the parsed spec list. Walks every store named `cas_FAST_SLOW_STORE`
/// (prod has exactly one) and reads the field off its FastSlow.fast
/// MemorySpec.
fn extract_cas_fast_memory_max_bytes(stores: &[StoreConfig]) -> Option<usize> {
    for store_config in stores {
        if store_config.name != "cas_FAST_SLOW_STORE" {
            continue;
        }
        if let StoreSpec::FastSlow(fs) = &store_config.spec {
            if let StoreSpec::Memory(m) = &fs.fast {
                if let Some(policy) = &m.eviction_policy {
                    return Some(policy.max_bytes);
                }
            }
        }
    }
    None
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
    let stores = vec![StoreConfig {
        name: CAS_ROOT_STORE_NAME.to_string(),
        spec: memory_spec,
    }];
    let health_registry_builder =
        Arc::new(AsyncMutex::new(HealthRegistryBuilder::new("nativelink-bench")));
    let store_manager = build_store_manager(&stores, &health_registry_builder).await?;
    let cas_store = store_manager.get_store(CAS_ROOT_STORE_NAME).ok_or_else(|| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "memory-only composition: store registration failed",
        )
    })?;
    Ok(Composition {
        cas_store,
        _store_manager: store_manager,
        _temp_dir: temp_dir,
        // Memory-only composition has no SizePartitioning; default both
        // metric-extras values to 0. Scenarios using this composition
        // (chunked-v2 W3/R5) don't read either field.
        size_partitioning_threshold: 0,
        cas_fast_memory_max_bytes: max_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Snapshot parses against the current `CasConfig` schema. If
    /// `nativelink-config` adds a `#[serde(deny_unknown_fields)]` field
    /// or renames a key, this red-fails immediately rather than at the
    /// next bench run.
    #[test]
    fn buildcache_snapshot_parses() {
        let cfg: CasConfig = serde_json5::from_str(PROD_SERVER_JSON5)
            .expect("benchmarks/prod-server.json5 snapshot must parse against current CasConfig schema");
        assert!(
            cfg.stores.iter().any(|s| s.name == CAS_ROOT_STORE_NAME),
            "snapshot must declare `{CAS_ROOT_STORE_NAME}` — bench resolves the \
             outer prod wrapper by this name",
        );
        // Both Redis stores the bench substitutes must be present in
        // the snapshot. If prod renames one we want loud failure here,
        // not silent fall-through to a startup-time Redis dial.
        for redis_name in REDIS_STORE_NAMES {
            assert!(
                cfg.stores.iter().any(|s| s.name == *redis_name),
                "snapshot must declare `{redis_name}` — bench substitutes this \
                 with MemoryStore; renaming in prod requires updating \
                 REDIS_STORE_NAMES in composition.rs",
            );
        }
    }

    /// Bench can build a full composition from the snapshot without
    /// dialing Redis. Tempdir is created, dropped at end of test;
    /// `cas_STORE` resolves; threshold + memory-cap are extracted.
    #[tokio::test]
    async fn build_prod_composition_round_trips() {
        let comp = build_prod_cas_composition(None)
            .await
            .expect("bench composition build must succeed against current snapshot");
        // SizePartitioning threshold is the prod value (16 KiB).
        assert_eq!(
            comp.size_partitioning_threshold, 16_384,
            "size_partitioning_threshold mismatch — snapshot may have drifted; \
             update the assertion AND verify the new prod value is intentional",
        );
        // cas_FAST_SLOW_STORE fast-tier cap is the prod value (16 GB).
        assert_eq!(
            comp.cas_fast_memory_max_bytes, 16_000_000_000,
            "cas_fast_memory_max_bytes mismatch — snapshot may have drifted",
        );
    }

    /// Deviation predicate uses strict `<` so a blob of exactly
    /// `size_partitioning_threshold` bytes routes to UPPER
    /// (`cas_FAST_SLOW_STORE`, prod-shape), NOT to SMALL_CAS_CACHED.
    /// Mutation: change `<` to `<=` in
    /// `Composition::should_emit_small_cas_deviation` — this test
    /// red-fails with a bespoke "boundary_lowered" message.
    #[tokio::test]
    async fn deviation_helper_boundary_pins_strict_lt() {
        let comp = build_prod_cas_composition(None)
            .await
            .expect("composition build must succeed");
        let threshold = comp.size_partitioning_threshold;
        assert!(
            !comp.should_emit_small_cas_deviation(threshold),
            "boundary_lowered mutation: at size == size_partitioning_threshold \
             ({threshold}), should_emit_small_cas_deviation MUST return false. \
             A `<=` mutation in the helper would return true and mis-tag a \
             cas_FAST_SLOW_STORE (full-prod-shape) cell as a SMALL_CAS_CACHED \
             deviation, causing reviewers to discount a real prod-path baseline.",
        );
        assert!(
            comp.should_emit_small_cas_deviation(threshold - 1),
            "boundary mismatch: size below threshold-by-one ({}) must trigger \
             the deviation tag (SMALL_CAS_CACHED Memory-substitute path)",
            threshold - 1,
        );
        assert!(
            !comp.should_emit_small_cas_deviation(threshold + 1),
            "boundary mismatch: size above threshold-by-one ({}) must NOT \
             trigger the deviation tag (routes to cas_FAST_SLOW_STORE)",
            threshold + 1,
        );
        assert!(
            comp.should_emit_small_cas_deviation(0),
            "0-byte blob must route to SMALL_CAS_CACHED (deviation tag set)",
        );
    }

    /// Apply-overrides eliminates every `RedisStore` so the bench never
    /// dials Redis at startup. Mutation: comment out the Redis-rewrite
    /// branch in `apply_bench_overrides` — this test red-fails.
    #[test]
    fn apply_overrides_eliminates_redis_top_level() {
        let cfg: CasConfig = serde_json5::from_str(PROD_SERVER_JSON5).unwrap();
        let mut stores = cfg.stores;
        apply_bench_overrides(&mut stores, "/tmp/bench-content", "/tmp/bench-temp");
        for store_config in &stores {
            assert!(
                !matches!(store_config.spec, StoreSpec::RedisStore(_)),
                "apply_bench_overrides failed to substitute Redis backend \
                 `{}` — bench will hang dialing Redis at startup. Add the \
                 name to REDIS_STORE_NAMES.",
                store_config.name,
            );
        }
    }

    /// Filesystem paths are rewritten — no prod paths remain post-override.
    #[test]
    fn apply_overrides_rewrites_filesystem_paths() {
        let cfg: CasConfig = serde_json5::from_str(PROD_SERVER_JSON5).unwrap();
        let mut stores = cfg.stores;
        let content = "/tmp/bench-content-XXX";
        let temp = "/tmp/bench-temp-XXX";
        apply_bench_overrides(&mut stores, content, temp);
        let mut found_filesystem = false;
        for store_config in &stores {
            check_filesystem_paths(&store_config.spec, content, temp, &mut found_filesystem);
        }
        assert!(
            found_filesystem,
            "snapshot must contain at least one FilesystemStore — \
             prod cas_FAST_SLOW_STORE.slow is a FilesystemStore. \
             If this red-fails, the snapshot's prod-shape has changed; \
             verify whether the bench's filesystem-path rewrite still \
             needs to fire.",
        );
    }

    fn check_filesystem_paths(
        spec: &StoreSpec,
        expected_content: &str,
        expected_temp: &str,
        found: &mut bool,
    ) {
        match spec {
            StoreSpec::Filesystem(fs) => {
                *found = true;
                assert_eq!(
                    fs.content_path, expected_content,
                    "filesystem content_path not rewritten",
                );
                assert_eq!(
                    fs.temp_path, expected_temp,
                    "filesystem temp_path not rewritten",
                );
                if let Some(policy) = &fs.eviction_policy {
                    assert!(
                        policy.max_bytes <= BENCH_FILESYSTEM_MAX_BYTES,
                        "filesystem eviction_policy.max_bytes ({}) not clamped \
                         to BENCH_FILESYSTEM_MAX_BYTES ({})",
                        policy.max_bytes,
                        BENCH_FILESYSTEM_MAX_BYTES,
                    );
                }
            }
            StoreSpec::FastSlow(fs) => {
                check_filesystem_paths(&fs.fast, expected_content, expected_temp, found);
                check_filesystem_paths(&fs.slow, expected_content, expected_temp, found);
            }
            StoreSpec::Verify(v) => {
                check_filesystem_paths(&v.backend, expected_content, expected_temp, found);
            }
            StoreSpec::ExistenceCache(ec) => {
                check_filesystem_paths(&ec.backend, expected_content, expected_temp, found);
            }
            StoreSpec::SizePartitioning(sp) => {
                check_filesystem_paths(
                    &sp.lower_store,
                    expected_content,
                    expected_temp,
                    found,
                );
                check_filesystem_paths(
                    &sp.upper_store,
                    expected_content,
                    expected_temp,
                    found,
                );
            }
            _ => {}
        }
    }
}
