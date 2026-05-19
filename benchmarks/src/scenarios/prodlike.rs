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

#![allow(clippy::doc_lazy_continuation)]
//! #537 — prod-like bench cells that go all-the-way to a REAL filesystem.
//!
//! ## Why this module exists
//!
//! The existing W1 and W3 cells already wire near-production store
//! compositions, but the harness defaults the bench `temp_dir` to
//! `/dev/shm` (tmpfs). On tmpfs the FilesystemStore's `write` syscall
//! never touches a real block device — page cache hits hide the
//! syscall cost that would be paid in production against
//! `/srv/casdata/nativelink/stores/`. A chunked-vs-not throughput
//! comparison done on a tmpfs leaf is unfair: per-chunk syscalls look
//! "free" because they're page-cache writes.
//!
//! The cells in this module re-run the existing W1 16 MiB c=1 and W3
//! 16 MiB c=1 scenarios with the FilesystemStore backed by a path on
//! ZFS pool `fast` (real SSD-backed dataset). That makes the
//! chunked-vs-not delta the LOAD-BEARING number — the goal of #537.
//!
//! ## Composition shape (same as W1 / W3, only the filesystem changes)
//!
//! **W1f (`w1f_*` cell family):** identical store chain to W1 — the
//! full prod composition
//!
//! ```text
//! Verify(verify_size=true, verify_hash=true)
//!   ExistenceCache(50M entries)
//!     SizePartitioning(16 KiB)
//!       lower: SMALL_CAS_CACHED  // unused at 16 MiB
//!       upper: cas_FAST_SLOW_STORE = FastSlow {
//!                fast: Memory(16 GB, evict_bytes=4.5GB, max_count=1M,
//!                             emit_backpressure_enabled=true),
//!                slow: Filesystem(<temp_dir on POOL_FAST>),  // <-- ONLY DELTA vs W1
//!                slow_writes_in_flight_max_bytes: 12 GiB,
//!                chunked_reads_enabled: true,
//!              }
//! ```
//!
//! Driven via `Store::update_oneshot` — same call-shape as W1.
//!
//! **W3f (`w3f_*` cell family):** identical store + driver to W3 —
//! `ChunkedWriteHandler` over a bare `FilesystemStore` WITH the three
//! production durability sinks wired (`with_v2_stable_digests_sink`,
//! `with_v2_failed_commit_sink`, `with_chunked_in_flight_digests`).
//! Mirrors the production wiring at `src/bin/nativelink.rs:912-925`
//! where `fs_arc` is peeled out of the FastSlowStore slow tier and
//! fed to `ChunkedWriteHandler::new` with the three sink closures
//! obtained from the sibling FastSlowStore. The CAS-chain wrappers
//! (Verify/ExistenceCache/SizePartitioning/MemoryStore) are STILL
//! skipped — the chunked path bypasses them by design (the v2 commit
//! barrier does its own dedupe via the per-digest `Notify`, not via
//! `ExistenceCacheStore::has`).
//!
//! ```text
//! ChunkedWriteHandler → FilesystemStore(<temp_dir on POOL_FAST>)
//!                                       // <-- ONLY DELTA vs W3
//! ```
//!
//! Driven via `write_chunked_v2` against an in-process tonic server,
//! same shape as W3.
//!
//! ## Backing-disk strategy
//!
//! Cells take `Option<&PathBuf>` like W1/W3, but the binary entry
//! point routes `prodlike` cells through a distinct CLI flag
//! (`--prodlike-scratch-dir`) that defaults to
//! `/srv/build/Work/nl-bench-537/<run-id>/`. The existing
//! `resolve_bench_temp_dir` gate refuses any path containing
//! `/fast/` or `/srv/bulk/` (substring match) to defend against
//! accidental writes to prod ZFS state at `/srv/nativelink/`
//! or `/srv/casdata/...`. The prodlike entry point narrows that
//! gate: prod-state subtrees (`/srv/nativelink/`,
//! `/srv/casdata/`) remain refused, but user-scoped scratch
//! under `/srv/build/Work/` is allowed — that's the canonical
//! bench-scratch location per the dispatch ("Pool `fast` is the
//! canonical bench-scratch").
//!
//! Per-cell teardown removes the `<run-id>` subdirectory so a long
//! bench run does not accumulate `n_cells * payload_size` of debris
//! on the ZFS dataset.
//!
//! ## Cell matrix (minimal, by design)
//!
//! Two cells:
//!
//! - `w1f_store_update_oneshot_16MiB_c1_prodlike` — W1 16 MiB single
//!   writer, FilesystemStore on pool `fast`. Pairs with
//!   `w1_store_update_oneshot_16MiB_c1` (tmpfs-leafed) for direct
//!   "tmpfs vs disk" delta on the W1 path.
//! - `w3f_chunked_v2_write_single_writer_16MiB_prodlike` — W3 16 MiB
//!   single writer, FilesystemStore on pool `fast`. Pairs with
//!   `w3_chunked_v2_write_single_writer_16MiB` (tmpfs-leafed) for
//!   the same delta on the chunked-v2 path. Cross-comparing
//!   `w1f_..._16MiB` against `w3f_..._16MiB` yields the
//!   *load-bearing* "chunked vs non-chunked on real disk" answer
//!   that #537 was filed to surface.
//!
//! More size / concurrency cells are a #537 followup — kept narrow
//! to ship the comparison signal first.
//!
//! ## extras.composition_deviation
//!
//! - **W1f**: emits `composition_deviation = "none"` — W1f traverses
//!   the full prod CAS wrapper chain (Verify → ExistenceCache →
//!   SizePartitioning → FastSlow{Memory,Filesystem}). The only delta
//!   vs prod is the FilesystemStore content_path's on-disk medium
//!   (substituted user-scoped ZFS dataset under `/srv/build/` for
//!   prod's `/srv/casdata/nativelink/stores/`); the dataset shares
//!   on-pool recordsize/compression with the prod CAS dataset.
//! - **W3f**: emits W3's
//!   `direct_filesystem_no_cas_chain_wrappers_no_memorystore_no_sizepartitioning`
//!   tag (NOT `"none"`). W3f INHERITS W3's composition shape —
//!   `ChunkedWriteHandler` over a bare `FilesystemStore` with NO
//!   Verify/ExistenceCache/SizePartitioning/MemoryStore wrappers and
//!   NO production sinks. Emitting `"none"` here was a #537 red-team
//!   bug (Q1): a diff-tool consumer reading `composition_deviation =
//!   "none"` for both W1f AND W3f would compare two cells with
//!   DIFFERENT wrapper chains and conclude "chunked is N× slower"
//!   when part of the delta is "W1f traverses 5 wrappers W3f skips
//!   by design". Reusing W3's tag makes the asymmetry visible in the
//!   JSON.
//!
//! ## extras.measures (#537 D3)
//!
//! Both prodlike cells (and their tmpfs siblings W1 + W3) emit an
//! `extras.measures` string naming what the timed body actually waits
//! for:
//!
//! - **W1 / W1f**: `"fast_tier_ack_then_spawn_dispatch"`. The timed
//!   body returns when the MemoryStore fast tier accepts the bytes;
//!   the slow-tier FilesystemStore write is `tokio::spawn`'d as
//!   fire-and-forget and its latency is NOT timed.
//! - **W3 / W3f**: `"chunked_commit_to_disk"`. The timed body returns
//!   when the v2 server emits `FinalResponse(committed_size)`, which
//!   it only sends after pwrite + verify + finalize-rename complete
//!   on disk.
//!
//! A reader consuming a baseline JSON in isolation (Slack snippet,
//! 6-month post-mortem) MUST be able to derive what the cell measured
//! WITHOUT chasing the scenario doc-comment, otherwise they'll
//! mis-compare cells that measure fundamentally different events
//! (red-team #537 6-month pre-mortem).

use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreLike;

use crate::composition::build_prod_cas_composition;
use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob_with_indices, measure};

/// Default scratch root under pool `fast` for the prodlike cells.
/// User-scoped path (`user/Work`), NOT prod CAS state (which lives at
/// `/srv/nativelink/...` or `/srv/casdata/...`). The CLI flag
/// `--prodlike-scratch-dir` overrides this.
pub const DEFAULT_PRODLIKE_SCRATCH_ROOT: &str = "/srv/build/Work/nl-bench-537";

/// Pinned 16 MiB payload size for the apples-to-apples W1f vs W3f
/// comparison. Matches the 16 MiB c=1 cells in W1 and W3 byte-for-byte.
pub const PRODLIKE_CELL_SIZE_BYTES: usize = 16 * 1024 * 1024;

/// W1f cell name. Pairs with `w1_store_update_oneshot_16MiB_c1`.
const W1F_SCENARIO_NAME: &str = "w1f_store_update_oneshot_16MiB_c1_prodlike";

/// W3f cell name. Pairs with `w3_chunked_v2_write_single_writer_16MiB`.
const W3F_SCENARIO_NAME: &str = "w3f_chunked_v2_write_single_writer_16MiB_prodlike";

/// Top-level entry point: run W1f then W3f. Both cells write the same
/// 16 MiB payload through the same shape of store as W1 / W3 — only the
/// FilesystemStore content_path differs (pool `fast` instead of
/// tmpfs).
///
/// `scratch_root` is the parent dir under which the per-cell tempdir
/// is created. Honors the dispatch's "pool `fast` is canonical
/// bench-scratch" choice. Use `default_scratch_root()` for the
/// stock path; pass `Some(_)` to override (tests, alternate datasets).
pub async fn run(
    opts: &RunOpts,
    scratch_root: Option<&Path>,
) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();
    let iters = opts.effective_iters(20);

    // Per-cell tempdir creation: each cell creates its own subdir
    // under `scratch_root` so the FilesystemStore content + temp
    // paths are isolated and teardown is structural (Drop on
    // tempfile::TempDir).
    let resolved_root = match scratch_root {
        Some(p) => p.to_path_buf(),
        None => PathBuf::from(DEFAULT_PRODLIKE_SCRATCH_ROOT),
    };

    // Best-effort ensure the parent exists. If this fails we surface
    // and skip; the bench should NOT silently fall back to tmpfs and
    // pretend it measured a disk-backed cell — that would defeat the
    // whole point of #537.
    if let Err(e) = tokio::fs::create_dir_all(&resolved_root).await {
        eprintln!(
            "[bench] #537 prodlike: failed to create scratch root {}: {e:?}; \
             skipping prodlike cells (refusing to silently fall back to tmpfs)",
            resolved_root.display(),
        );
        return out;
    }

    if opts.matches(W1F_SCENARIO_NAME) {
        match run_w1f(&resolved_root, iters).await {
            Ok(r) => out.push(r),
            Err(e) => {
                eprintln!("[bench] #537 W1f build/run failed: {e:?}");
            }
        }
    }

    if opts.matches(W3F_SCENARIO_NAME) {
        match run_w3f(&resolved_root, iters).await {
            Ok(r) => out.push(r),
            Err(e) => {
                eprintln!("[bench] #537 W3f build/run failed: {e:?}");
            }
        }
    }

    out
}

/// W1f: W1 16 MiB c=1 on real-disk FilesystemStore.
async fn run_w1f(
    scratch_root: &Path,
    iters: u32,
) -> Result<BenchmarkResult, nativelink_error::Error> {
    let composition = build_prod_cas_composition(Some(scratch_root)).await?;
    let concurrency: u32 = 1;
    let size = PRODLIKE_CELL_SIZE_BYTES;
    let throughput_bytes_per_iter = (size as u64) * (concurrency as u64);

    // Pre-generate every blob OUTSIDE the timed body — same hoist as W1
    // so per-iter LCG + hash cost does not contaminate the measurement.
    // At 20 iters × 16 MiB = 320 MiB of resident bytes; well under any
    // host budget we ship on.
    //
    // UNBOUNDED-OK: bench-local fixture; size is `iters × concurrency ×
    // payload`, capped by the CLI `--iters` value (default 20) ×
    // concurrency=1 × PRODLIKE_CELL_SIZE_BYTES (16 MiB), so 320 MiB
    // resident steady-state. Bench code is not network-reachable; the
    // hoist removes per-iter LCG-fill + BLAKE3 from the timed body so
    // the cell measures the wrapper-chain ack-latency, not payload-
    // build cost (#533 harness-suspect-list lesson).
    let prebuilt: Vec<Vec<(DigestInfo, Bytes)>> = (0..iters)
        .map(|n| {
            (0..concurrency)
                .map(|j| make_blob_with_indices(W1F_SCENARIO_NAME, n as u64, j, size))
                .collect()
        })
        .collect();

    let mut extras = BTreeMap::new();
    extras.insert(
        "scratch_root".to_string(),
        serde_json::json!(scratch_root.display().to_string()),
    );
    extras.insert(
        "filesystem_backed_by".to_string(),
        serde_json::json!("zfs_pool_fast_dataset"),
    );
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!("none"),
    );
    // #537 D3: self-describing JSON. W1f mirrors W1's call shape
    // (`cas.update_oneshot`), which at 16 MiB routes to the upper
    // `cas_FAST_SLOW_STORE` and returns at the MemoryStore fast-tier
    // ack; the slow-tier FilesystemStore write is `tokio::spawn`'d as
    // fire-and-forget. The on-disk WRITE happens — and on a real-disk
    // ZFS leaf is meaningfully slower than tmpfs — but it is NOT in
    // the timed body. A reader comparing W1f's 6 ms p50 to W3f's
    // 110 ms p50 MUST see this in the JSON or they'll conclude
    // "chunked is 18× slower" when part of the delta is "W1f measures
    // ack; W3f measures commit-to-disk" — see red-team #537 6-month
    // pre-mortem.
    extras.insert(
        "measures".to_string(),
        serde_json::json!("fast_tier_ack_then_spawn_dispatch"),
    );
    extras.insert(
        "paired_baseline_cell".to_string(),
        serde_json::json!("w1_store_update_oneshot_16MiB_c1"),
    );

    let iter_counter = AtomicU64::new(0);
    let cas = composition.cas_store.clone();
    let prebuilt = Arc::new(prebuilt);
    let prebuilt_for_body = prebuilt.clone();

    let result = measure(
        "W1f",
        W1F_SCENARIO_NAME,
        Some(size as u64),
        concurrency,
        CacheState::Cold,
        iters,
        Some(throughput_bytes_per_iter),
        None,
        extras,
        move || {
            let cas = cas.clone();
            let prebuilt = prebuilt_for_body.clone();
            let n = iter_counter.fetch_add(1, Ordering::Relaxed);
            async move {
                let row = &prebuilt[n as usize];
                let (digest, data) = row[0].clone();
                cas.update_oneshot(digest, data)
                    .await
                    .expect("W1f write must succeed");
            }
        },
    )
    .await;

    // Hold composition until measure completes so the FilesystemStore
    // tempdir lives across the timed body. Dropping here triggers
    // tempfile::TempDir cleanup → rm -rf of the per-cell scratch.
    drop(composition);
    Ok(result)
}

/// W3f: W3 16 MiB c=1 on real-disk FilesystemStore. Gated on
/// `chunked_fast_slow` feature — when off, returns an Err so the
/// caller logs and continues.
#[cfg(feature = "chunked_fast_slow")]
async fn run_w3f(
    scratch_root: &Path,
    iters: u32,
) -> Result<BenchmarkResult, nativelink_error::Error> {
    use core::time::Duration;

    use nativelink_config::stores::FilesystemSpec;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        WriteChunk, cas_extensions_client::CasExtensionsClient,
        cas_extensions_server::CasExtensionsServer,
    };
    use nativelink_service::chunked_write_handler::ChunkedCasExtensionsAdapter;
    use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};

    // #537 m3: reuse W3's `make_payload` + `build_chunks` +
    // `drain_v2_response` + `BENCH_CHUNK_SIZE` from `chunked_v2::enabled`
    // so future fixes to W3's chunk-building (e.g. the #524 chunk-hash
    // unification) automatically apply to W3f and the W1f/W3f comparison
    // cannot silently drift from W3.
    use crate::scenarios::chunked_v2::enabled::{
        BENCH_CHUNK_SIZE, PRODUCTION_SINKS_WIRED_TAG, build_chunks, drain_v2_response,
        make_payload,
    };
    use crate::scenarios::digest_via_default_hasher;

    // ---- Build the FilesystemStore on pool `fast` ----
    let temp_dir = tempfile::TempDir::new_in(scratch_root).map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "W3f tempdir: {e:?}",
        )
    })?;
    let content_path = temp_dir.path().join("content").to_string_lossy().into_owned();
    let temp_path = temp_dir.path().join("temp").to_string_lossy().into_owned();
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await?;

    // ---- Build the handler with production sinks (#541) ----
    //
    // W3f pairs byte-for-byte against W3; both go through the SAME
    // helper so the per-iter durability bookkeeping cost
    // (stable_digests pusher mutex+notify, failed_writes inserter,
    // chunked_in_flight RAII guard insert+remove) is included in the
    // commit-to-disk wall-clock for both cells. Pre-#541 W3f built the
    // handler inline via `new_with_state_and_chunk_size_for_test`
    // without the three sinks; the bench numbers UNDERSTATED the
    // per-iter cost production pays at
    // `src/bin/nativelink.rs:912-925`. `_sinks_state` is held until the
    // timed body completes so the drain task stays alive + the
    // stable-digests Vec keeps getting drained.
    let (handler, _sinks_state) =
        crate::scenarios::chunked_v2::enabled::make_handler_with_production_sinks(store);

    // ---- Bring up an in-process v2 server + client ----
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral bind must succeed");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let adapter = ChunkedCasExtensionsAdapter::new(handler);
    let svc = CasExtensionsServer::new(adapter);
    let _server_guard = nativelink_util::spawn!(
        "w3f-bench-server",
        async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
            {
                eprintln!("[bench] W3f v2 server exited with error: {e:?}");
            }
        }
    );
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .expect("endpoint parse must succeed")
            .connect_timeout(Duration::from_secs(5));
    let channel = endpoint
        .connect()
        .await
        .expect("client must connect to in-process W3f v2 server");
    let client = CasExtensionsClient::new(channel);

    // ---- Pre-generate payloads + chunks OUTSIDE the timer ----
    // UNBOUNDED-OK: bench-local fixture; size is `iters × payload`, capped
    // by the CLI `--iters` value (default 20) and PRODLIKE_CELL_SIZE_BYTES
    // (16 MiB), so 320 MiB resident steady-state. Bench code is not
    // network-reachable; this is hoisting per-iter LCG-fill + BLAKE3
    // out of the timed body so the cell measures commit-to-disk, not
    // payload-build cost (see #533 harness-suspect-list lesson).
    let size = PRODLIKE_CELL_SIZE_BYTES;
    let concurrency: u32 = 1;
    let prebuilt: Vec<Vec<WriteChunk>> = (0..iters as u64)
        .map(|n| {
            let payload = make_payload(size, n);
            let digest = digest_via_default_hasher(&payload);
            build_chunks(digest, &payload)
        })
        .collect();

    let mut extras = BTreeMap::new();
    extras.insert("chunk_size".to_string(), serde_json::json!(BENCH_CHUNK_SIZE));
    extras.insert("v3_anchor".to_string(), serde_json::json!("chunked_v2"));
    extras.insert("concurrent_ops_per_iter".to_string(), serde_json::json!(1));
    extras.insert(
        "scratch_root".to_string(),
        serde_json::json!(scratch_root.display().to_string()),
    );
    extras.insert(
        "filesystem_backed_by".to_string(),
        serde_json::json!("zfs_pool_fast_dataset"),
    );
    // #537 D2: W3f INHERITS W3's composition shape — it uses the SAME
    // helper as W3 (`make_handler_with_production_sinks` after #541)
    // over a bare `FilesystemStore`, WITHOUT the surrounding
    // Verify/ExistenceCache/SizePartitioning/MemoryStore wrappers.
    // The only delta vs W3 is the FilesystemStore content_path's
    // on-disk medium (ZFS pool `fast` vs tmpfs). Emitting `"none"` here
    // was WRONG (silently dropped W3's deviation tag); reuse W3's tag
    // so diff-tool consumers see the same wrapper-chain skip on both
    // cells and don't conclude "chunked is N× slower" when part of the
    // delta is "W1f traverses 5 wrappers W3f skips" (see red-team #537
    // Q1 + assumption-auditor claim 3 NIT).
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!(crate::scenarios::chunked_v2::enabled::COMPOSITION_DEVIATION_TAG),
    );
    // #541: W3f wires the three production durability sinks
    // (`stable_digests_pusher`, `failed_writes_inserter`,
    // `chunked_in_flight_digests`) via the shared
    // `make_handler_with_production_sinks` helper. Same tag value as W3
    // so diff tooling joining on `scenario_name` can filter pre-#541
    // vs post-#541 baselines consistently across both cells.
    extras.insert(
        "production_sinks_wired".to_string(),
        serde_json::json!(PRODUCTION_SINKS_WIRED_TAG),
    );
    // #537 D3: self-describing JSON. W3f's timed body wraps
    // `write_chunked_v2 + drain_v2_response` — `drain_v2_response`
    // blocks until the v2 server emits `FinalResponse(committed_size)`,
    // which the server only sends after pwrite + chunk-verify +
    // finalize-rename complete on disk. So W3f measures
    // commit-to-disk, NOT a fast-tier ack. This is the load-bearing
    // counterpoint to W1f's `fast_tier_ack_then_spawn_dispatch` — the
    // difference between the two values is the entire reason a reader
    // cannot conflate W1f vs W3f as a clean "non-chunked vs chunked"
    // comparison (see red-team #537 6-month pre-mortem).
    extras.insert(
        "measures".to_string(),
        serde_json::json!("chunked_commit_to_disk"),
    );
    extras.insert(
        "paired_baseline_cell".to_string(),
        serde_json::json!("w3_chunked_v2_write_single_writer_16MiB"),
    );

    let iter_counter = Arc::new(AtomicU64::new(0));
    let prebuilt = Arc::new(prebuilt);
    let client_for_body = client.clone();
    let bytes_per_iter = (size as u64) * (concurrency as u64);
    let size_u64 = size as u64;

    let result = measure(
        "W3f",
        W3F_SCENARIO_NAME,
        Some(size_u64),
        concurrency,
        CacheState::Cold,
        iters,
        Some(bytes_per_iter),
        None,
        extras,
        move || {
            let mut c = client_for_body.clone();
            let prebuilt = prebuilt.clone();
            let counter = iter_counter.clone();
            async move {
                let n = counter.fetch_add(1, Ordering::Relaxed);
                let chunks = prebuilt[n as usize].clone();
                let stream = tokio_stream::iter(chunks);
                let response = c
                    .write_chunked_v2(stream)
                    .await
                    .expect("W3f write_chunked_v2 must return Ok");
                let committed = drain_v2_response(response.into_inner())
                    .await
                    .expect("W3f commit must succeed");
                assert_eq!(committed, size_u64, "W3f committed bytes mismatch");
            }
        },
    )
    .await;

    // Hold tempdir until after the timed body so on-disk files exist
    // for the duration of the cell. Drop here = rm -rf of scratch.
    drop(temp_dir);
    Ok(result)
}

#[cfg(not(feature = "chunked_fast_slow"))]
async fn run_w3f(
    _scratch_root: &Path,
    _iters: u32,
) -> Result<BenchmarkResult, nativelink_error::Error> {
    Err(nativelink_error::make_err!(
        nativelink_error::Code::Unavailable,
        "W3f requires --features chunked_fast_slow",
    ))
}

// #537 m3: `make_payload`, `build_chunks`, and `drain_v2_response` used
// to live here as W3f-local copies of W3's helpers. They were promoted to
// `pub(crate)` in `chunked_v2::enabled` and imported above so future fixes
// to W3's chunk-building cannot silently diverge from W3f. Same for the
// chunk-size constant (`BENCH_CHUNK_SIZE`).

#[cfg(test)]
mod tests {
    use super::*;

    /// The scenario names MUST stay stable — checked-in baselines join
    /// on `scenario_name`. Mutation: rename either constant; this test
    /// red-fails with the bespoke "#537 scenario-name stability" msg.
    #[test]
    fn scenario_names_are_stable() {
        assert_eq!(
            W1F_SCENARIO_NAME, "w1f_store_update_oneshot_16MiB_c1_prodlike",
            "#537 scenario-name stability: W1f cell renamed; checked-in \
             baselines will silently lose continuity"
        );
        assert_eq!(
            W3F_SCENARIO_NAME, "w3f_chunked_v2_write_single_writer_16MiB_prodlike",
            "#537 scenario-name stability: W3f cell renamed; checked-in \
             baselines will silently lose continuity"
        );
    }

    /// The 16 MiB pin is load-bearing: W1f vs W3f must compare at the
    /// SAME payload size, AND must pair byte-for-byte against the W1
    /// and W3 16 MiB cells they shadow. Mutation: bump the constant —
    /// this test red-fails with a bespoke #537 message.
    #[test]
    fn prodlike_size_pinned_to_16mib() {
        assert_eq!(
            PRODLIKE_CELL_SIZE_BYTES,
            16 * 1024 * 1024,
            "#537 size pin: PRODLIKE_CELL_SIZE_BYTES drifted from 16 MiB; \
             W1f/W3f no longer comparable to the W1/W3 16 MiB cells they shadow"
        );
    }

    /// #537 D2: W3f MUST emit the SAME `composition_deviation` tag as
    /// W3 — both share `ChunkedWriteHandler → bare FilesystemStore`
    /// without the production wrapper chain. Emitting `"none"` for W3f
    /// silently dropped W3's deviation and would let diff-tool readers
    /// conclude "chunked is N× slower" when part of the delta is "W3f
    /// skips 5 wrappers W1f traverses".
    ///
    /// This test pins W3f's deviation to W3's `COMPOSITION_DEVIATION_TAG`
    /// at compile-time via re-export, so a future split of the W3 tag
    /// (e.g. when R5 wires through the Notify barrier and diverges from
    /// W3's composition) MUST re-touch W3f deliberately, not silently.
    ///
    /// **Mutation falsifier:** revert the deviation insert at the W3f
    /// extras to `serde_json::json!("none")` (the pre-fix shape). The
    /// `iters=1` smoke below will run `run_w3f` and inspect the
    /// resulting `composition_deviation` extras key; this test must
    /// red-fail with the bespoke `"#537 D2 W3f deviation tag must
    /// match W3's"` message.
    #[cfg(feature = "chunked_fast_slow")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn w3f_composition_deviation_matches_w3() {
        let scratch_root = tempfile::TempDir::new().expect("test tempdir");
        let result = run_w3f(scratch_root.path(), 1)
            .await
            .expect("W3f must run cleanly on a fresh tempdir");
        assert_eq!(result.scenario_name, W3F_SCENARIO_NAME);
        let dev = result
            .extras
            .get("composition_deviation")
            .expect("W3f result must carry composition_deviation extras");
        let dev_str = dev
            .as_str()
            .expect("composition_deviation must be a string");
        assert_eq!(
            dev_str,
            crate::scenarios::chunked_v2::enabled::COMPOSITION_DEVIATION_TAG,
            "#537 D2 W3f deviation tag must match W3's: W3 emits {:?}, W3f \
             must emit the SAME tag (got {:?}); both cells share the same \
             ChunkedWriteHandler → bare FilesystemStore shape and W3f's \
             only delta vs W3 is the on-disk medium (ZFS vs tmpfs), NOT \
             the wrapper chain — emitting 'none' would silently drop W3's \
             tag and let diff readers conclude 'chunked is N× slower' \
             when part of the delta is 'wrapper chain skipped by design'",
            crate::scenarios::chunked_v2::enabled::COMPOSITION_DEVIATION_TAG,
            dev_str
        );
    }

    /// #541: W3f MUST emit the SAME `production_sinks_wired` extras tag
    /// as W3. The tag is what the diff tool joins on to filter pre-#541
    /// vs post-#541 baselines; a drift between W3 and W3f silently
    /// splits the paired baseline diff. This is a string-level pin —
    /// the substantive behavioral assertion (the sinks ACTUALLY fire
    /// during W3f's v2 commit) lives in
    /// [`w3f_production_sinks_actually_fire_through_helper`] below.
    ///
    /// **Mutation falsifier:** change `PRODUCTION_SINKS_WIRED_TAG` or
    /// remove the extras insert in `run_w3f`. The 1-iter smoke runs
    /// `run_w3f` and inspects `production_sinks_wired`; this test
    /// red-fails with the bespoke `#541 W3f production_sinks_wired
    /// tag must match W3's` message.
    #[cfg(feature = "chunked_fast_slow")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn w3f_production_sinks_wired_extras_tag_matches_w3() {
        let scratch_root = tempfile::TempDir::new().expect("test tempdir");
        let result = run_w3f(scratch_root.path(), 1)
            .await
            .expect("W3f must run cleanly on a fresh tempdir");
        assert_eq!(result.scenario_name, W3F_SCENARIO_NAME);
        let sinks = result
            .extras
            .get("production_sinks_wired")
            .expect(
                "#541 W3f result must carry production_sinks_wired extras; \
                 absence breaks the diff tool's pre-vs-post #541 filter",
            );
        let sinks_str = sinks
            .as_str()
            .expect("production_sinks_wired must be a string");
        assert_eq!(
            sinks_str,
            crate::scenarios::chunked_v2::enabled::PRODUCTION_SINKS_WIRED_TAG,
            "#541 W3f production_sinks_wired tag must match W3's: W3 emits \
             {:?}, W3f must emit the SAME tag (got {:?}); both cells use \
             the shared `make_handler_with_production_sinks` helper and \
             pay identical per-iter durability-bookkeeping cost. A drift \
             between the two cells silently splits the W3-vs-W3f \
             paired-baseline diff.",
            crate::scenarios::chunked_v2::enabled::PRODUCTION_SINKS_WIRED_TAG,
            sinks_str
        );
    }

    /// #541 fix-up: behavioral pin for W3f's wiring. The string-tag
    /// test above pins the JSON extras key but does NOT prove the
    /// sinks actually fire during W3f's v2 commit path. This test
    /// rebuilds W3f's exact composition (FilesystemStore on a
    /// tempdir + `make_handler_with_production_sinks` + in-process v2
    /// server), drives ONE v2 commit through it, and observes the
    /// visible side effects:
    ///   - `drain_count` increments from 0 (the pusher fired).
    ///   - In-flight map is EMPTY post-commit (the RAII guard fired).
    ///   - Structurally, all three `is_*_wired` accessors return true.
    ///
    /// Mirrors `make_handler_with_production_sinks_installs_all_three`
    /// in `chunked_v2.rs` but anchored at the W3f-shape composition
    /// the prodlike scenario uses, so a future refactor that diverges
    /// W3f from W3's wiring (e.g. wires a different sink shape inline
    /// instead of going through the shared helper) cannot pass this
    /// test even if the extras tag still matches.
    ///
    /// Whole sequence wrapped in `tokio::time::timeout(10s)` as a
    /// deadlock detector — a wedge here means the v2 commit barrier
    /// did not complete (e.g. a sink held a lock across `.await`).
    ///
    /// **Mutation falsifier:** revert the `run_w3f` handler-build to
    /// `new_with_state_and_chunk_size_for_test` (no sinks). The
    /// structural assertion red-fails with "#541 W3f sink wiring
    /// violated: stable_digests_sink reported NOT wired" because the
    /// bare constructor leaves all three sinks `None`. The behavioral
    /// `drain_count > 0` assertion also red-fails — the v2 commit
    /// skips the sink invocation entirely.
    #[cfg(feature = "chunked_fast_slow")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn w3f_production_sinks_actually_fire_through_helper() {
        use core::sync::atomic::Ordering;
        use core::time::Duration;

        use nativelink_config::stores::FilesystemSpec;
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            cas_extensions_client::CasExtensionsClient,
            cas_extensions_server::CasExtensionsServer,
        };
        use nativelink_service::chunked_write_handler::ChunkedCasExtensionsAdapter;
        use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};

        use crate::scenarios::chunked_v2::enabled::{
            build_chunks, drain_v2_response, make_handler_with_production_sinks,
            make_payload,
        };
        use crate::scenarios::digest_via_default_hasher;

        // ---- Build the W3f-shape composition ----
        //
        // Same code path as `run_w3f`: a bare FilesystemStore on a
        // per-test tempdir, then `make_handler_with_production_sinks`
        // for the handler. We hold `state` so the drain task stays
        // alive AND so we can observe `drain_count` after the commit.
        let scratch_root = tempfile::TempDir::new().expect("test tempdir");
        let temp_dir = tempfile::TempDir::new_in(scratch_root.path())
            .expect("W3f-shape tempdir");
        let content_path = temp_dir
            .path()
            .join("content")
            .to_string_lossy()
            .into_owned();
        let temp_path = temp_dir
            .path()
            .join("temp")
            .to_string_lossy()
            .into_owned();
        let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path,
            temp_path,
            eviction_policy: None,
            block_size: 1,
            ..Default::default()
        })
        .await
        .expect("W3f-shape FilesystemStore build must succeed");

        let (handler, state) = make_handler_with_production_sinks(store);

        // ---- Structural: handler reports all three sinks wired ----
        assert!(
            handler.is_v2_stable_digests_sink_wired(),
            "#541 W3f sink wiring violated: stable_digests_sink \
             reported NOT wired by make_handler_with_production_sinks. \
             The W3f cell would silently skip BIS notification on \
             commit success and worker mirror_blobs would accumulate."
        );
        assert!(
            handler.is_v2_failed_commit_sink_wired(),
            "#541 W3f sink wiring violated: failed_commit_sink reported \
             NOT wired by make_handler_with_production_sinks."
        );
        assert!(
            handler.is_chunked_in_flight_digests_wired(),
            "#541 W3f sink wiring violated: chunked_in_flight_digests \
             reported NOT wired by make_handler_with_production_sinks. \
             `FastSlowStore::has_with_results` would silently return \
             None for in-flight v2 writes."
        );

        // ---- Behavioral: drive one v2 commit and observe sinks ----
        tokio::time::timeout(Duration::from_secs(10), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("ephemeral bind must succeed");
            let port = listener.local_addr().unwrap().port();
            let incoming =
                tokio_stream::wrappers::TcpListenerStream::new(listener);
            let adapter = ChunkedCasExtensionsAdapter::new(handler.clone());
            let svc = CasExtensionsServer::new(adapter);
            let _server_guard = nativelink_util::spawn!(
                "w3f-sink-wiring-test-server",
                async move {
                    if let Err(e) = tonic::transport::Server::builder()
                        .add_service(svc)
                        .serve_with_incoming(incoming)
                        .await
                    {
                        eprintln!(
                            "[test] W3f wiring v2 server exited with error: {e:?}"
                        );
                    }
                }
            );
            let endpoint = tonic::transport::Endpoint::from_shared(format!(
                "http://127.0.0.1:{port}"
            ))
            .expect("endpoint parse must succeed")
            .connect_timeout(Duration::from_secs(5));
            let channel = endpoint
                .connect()
                .await
                .expect("client must connect to in-process W3f wiring v2 server");
            let mut client = CasExtensionsClient::new(channel);

            // Tiny single-chunk payload: minimum surface for the v2
            // commit path so the test stays fast.
            let payload = make_payload(64 * 1024, 0xD15EA5E);
            let digest = digest_via_default_hasher(&payload);
            let chunks = build_chunks(digest, &payload);
            let expected_size = payload.len() as u64;
            let response = client
                .write_chunked_v2(tokio_stream::iter(chunks))
                .await
                .expect("W3f-shape write_chunked_v2 RPC must return Ok");
            let committed = drain_v2_response(response.into_inner())
                .await
                .expect("W3f-shape v2 commit must succeed");
            assert_eq!(committed, expected_size);
        })
        .await
        .expect(
            "#541 W3f sink wiring violated: v2 commit did not complete \
             within 10 s — the W3f-shape composition wedged at the v2 \
             commit barrier",
        );

        // The drain task races our resumption after the commit
        // completes; poll with a short tick for up to 5 s.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let n = state.drain_count.load(Ordering::Relaxed);
                if n > 0 {
                    return n;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect(
            "#541 W3f sink wiring violated: drain_count stayed at 0 \
             for 5 s after commit — the stable_digests_sink did not \
             fire on W3f's commit-success path. Production BIS \
             notification would also be skipped.",
        );
        assert!(
            drained >= 1,
            "drain_count must be >= 1 after one v2 commit (got {drained})"
        );

        // RAII guard must have removed the in-flight entry on session
        // drop.
        assert!(
            state.in_flight.lock().is_empty(),
            "#541 W3f sink wiring violated: chunked_in_flight map NOT \
             empty after commit (entries: {}). InFlightChunkedGuard's \
             Drop did not run, or the v2 session held the entry beyond \
             commit.",
            state.in_flight.lock().len()
        );

        drop(state);
        drop(temp_dir);
        drop(scratch_root);
    }

    /// #537 D5-m6: W3f smoke. Runs `run_w3f` end-to-end at `iters=1` and
    /// asserts: (1) the scenario name is stable, (2) throughput is
    /// non-degenerate (commit-to-disk completed for the single iter),
    /// (3) the `chunk_size` extras pin matches W3's `BENCH_CHUNK_SIZE`
    /// (1 MiB), (4) `extras.measures` is `"chunked_commit_to_disk"`
    /// EXACTLY (mirrors the D3 measures contract with the W3f-specific
    /// value, NOT just non-empty as the D3 test asserts).
    ///
    /// **Mutation falsifier:** change W3f's `extras.measures` insert to
    /// anything other than `"chunked_commit_to_disk"`; this test
    /// red-fails with the bespoke `"#537 D5-m6 W3f measures must be
    /// chunked_commit_to_disk"` message.
    #[cfg(feature = "chunked_fast_slow")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn w3f_smoke_runs_with_pinned_extras() {
        let scratch_root = tempfile::TempDir::new().expect("test tempdir");
        let result = run_w3f(scratch_root.path(), 1)
            .await
            .expect("W3f must run cleanly on a fresh tempdir");
        assert_eq!(
            result.scenario_name, W3F_SCENARIO_NAME,
            "scenario_name regression"
        );
        // Throughput non-zero confirms commit-to-disk completed for the
        // one iter (the v2 FinalResponse drained successfully).
        match &result.throughput {
            crate::output::Throughput::BytesPerSec(b) => assert!(
                *b > 0.0,
                "#537 D5-m6: W3f reported zero throughput; commit-to-disk \
                 did not complete or the cell short-circuited"
            ),
            other => panic!(
                "#537 D5-m6: W3f throughput must be BytesPerSec, got \
                 {other:?}"
            ),
        }
        // chunk_size pin: must match W3's BENCH_CHUNK_SIZE (shared via
        // `chunked_v2::enabled::BENCH_CHUNK_SIZE` after #537 m3
        // dedup). A drift here means W3f and W3 are comparing
        // apples-to-oranges chunk sizes.
        let chunk_size = result
            .extras
            .get("chunk_size")
            .and_then(|v| v.as_u64())
            .expect("#537 D5-m6: W3f extras must carry chunk_size");
        assert_eq!(
            chunk_size,
            crate::scenarios::chunked_v2::enabled::BENCH_CHUNK_SIZE as u64,
            "#537 D5-m6: W3f chunk_size ({chunk_size}) MUST match W3's \
             BENCH_CHUNK_SIZE ({}) — they share the constant via \
             `chunked_v2::enabled::BENCH_CHUNK_SIZE`",
            crate::scenarios::chunked_v2::enabled::BENCH_CHUNK_SIZE,
        );
        // measures pin: exact value, not just non-empty.
        let measures = result
            .extras
            .get("measures")
            .and_then(|v| v.as_str())
            .expect("#537 D5-m6: W3f extras must carry measures");
        assert_eq!(
            measures, "chunked_commit_to_disk",
            "#537 D5-m6 W3f measures must be chunked_commit_to_disk: \
             got {measures:?}; the W3f timed body wraps `write_chunked_v2 \
             + drain_v2_response` which blocks until the v2 server emits \
             FinalResponse (only sent after pwrite + verify + finalize-\
             rename complete on disk), so the measures string is the \
             load-bearing signal that lets a reader of this baseline \
             cell distinguish it from W1f's fast-tier-ack semantics"
        );
    }

    /// #537 D3: every W-family cell's JSON output MUST carry a non-empty
    /// `extras.measures` key naming what the timed body waits for.
    /// Without this, a reader consuming a baseline JSON in isolation
    /// (Slack snippet, post-mortem) has to chase the cell's doc-comment
    /// to know whether the latency is "fast-tier ack" vs "commit to
    /// disk" — the difference between those was the load-bearing
    /// 17.7× delta in the red-team pre-mortem.
    ///
    /// This test runs W1f + W3f with iters=1 and asserts both extras
    /// carry the field. **Mutation falsifier:** delete the
    /// `extras.insert("measures", ...)` call in EITHER `run_w1f` OR
    /// `run_w3f`; this test must red-fail with the bespoke
    /// `"#537 D3 extras.measures field must travel with every W-family
    /// cell's JSON"` message.
    #[cfg(feature = "chunked_fast_slow")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn w_family_cells_carry_extras_measures_field() {
        let scratch_root = tempfile::TempDir::new().expect("test tempdir");
        let w1f = run_w1f(scratch_root.path(), 1)
            .await
            .expect("W1f must run cleanly on a fresh tempdir");
        let w3f = run_w3f(scratch_root.path(), 1)
            .await
            .expect("W3f must run cleanly on a fresh tempdir");
        for (cell_name, result) in [
            (W1F_SCENARIO_NAME, &w1f),
            (W3F_SCENARIO_NAME, &w3f),
        ] {
            let measures = result.extras.get("measures").unwrap_or_else(|| {
                panic!(
                    "#537 D3 extras.measures field must travel with every \
                     W-family cell's JSON: cell {cell_name} has no \
                     `measures` key in extras; a reader of this baseline \
                     in isolation cannot tell whether the latency \
                     measures fast-tier ack or commit-to-disk — see \
                     red-team #537 6-month pre-mortem"
                );
            });
            let s = measures.as_str().unwrap_or_else(|| {
                panic!(
                    "#537 D3 extras.measures field must travel with every \
                     W-family cell's JSON: cell {cell_name} `measures` is \
                     not a string (got {measures:?})"
                );
            });
            assert!(
                !s.is_empty(),
                "#537 D3 extras.measures field must travel with every \
                 W-family cell's JSON: cell {cell_name} `measures` is \
                 the empty string"
            );
        }
    }

    /// The default scratch root MUST live under `/srv/build/` —
    /// user scratch, NOT prod state at `/srv/nativelink/` or
    /// `/srv/casdata/`. Mutation: change the constant to a prod
    /// dataset path; this test red-fails.
    #[test]
    fn default_scratch_root_is_user_scratch_not_prod_state() {
        let p = Path::new(DEFAULT_PRODLIKE_SCRATCH_ROOT);
        assert!(
            p.starts_with("/srv/build/"),
            "#537 scratch-root: must live under user-scoped /srv/build/ — \
             prod CAS state lives at /srv/nativelink/ or /srv/casdata/ \
             and must NOT be polluted by bench writes; got {}",
            DEFAULT_PRODLIKE_SCRATCH_ROOT
        );
        assert!(
            !DEFAULT_PRODLIKE_SCRATCH_ROOT.contains("nativelink/")
                && !DEFAULT_PRODLIKE_SCRATCH_ROOT.contains("casdata"),
            "#537 scratch-root: must not point at prod state dirs (nativelink/ \
             or casdata); got {}",
            DEFAULT_PRODLIKE_SCRATCH_ROOT
        );
    }

    /// Integration smoke test: after a single W1f iter, the cell MUST
    /// return a non-zero throughput sample. **This test does NOT verify
    /// a file ended up on disk** — the per-cell tempdir is rm-rf'd
    /// before the test could observe it. The load-bearing file-on-disk
    /// + index-visibility check lives in
    /// `w1f_drops_to_disk_under_held_tempdir` below; this test only
    /// confirms `run_w1f` itself runs end-to-end and reports a
    /// non-degenerate `Throughput::BytesPerSec` sample.
    ///
    /// #537 D5-m1 rename: the previous name
    /// `w1f_iter_writes_at_least_one_file_to_disk` overclaimed — a
    /// reader (or future sub-agent) would assume the file-on-disk
    /// contract was covered here and skip the second test. The new
    /// name matches the assertion.
    ///
    /// **Mutation falsifier:** in `run_w1f`, comment out the
    /// `measure(...).await` call (or replace `throughput_bytes_per_iter`
    /// with `Some(0)`). The test must red-fail with the bespoke
    /// "reported zero throughput" message.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn w1f_returns_nonzero_throughput() {
        // Use a tempdir under the system tempdir for the TEST
        // (the test does NOT need pool `fast` — it just needs a
        // REAL filesystem so the FilesystemStore composition builds).
        let scratch_root = tempfile::TempDir::new().expect("test tempdir");

        // Use the smallest iters the harness allows so the test runs
        // fast — the assertion is "non-zero", not "N samples".
        let iters: u32 = 1;
        let result = run_w1f(scratch_root.path(), iters)
            .await
            .expect("W1f must run cleanly on a fresh tempdir");
        assert_eq!(
            result.scenario_name, W1F_SCENARIO_NAME,
            "scenario_name regression"
        );
        // The throughput must be positive — a no-op cell would still
        // record samples but with zero bytes, giving 0 B/s.
        match &result.throughput {
            crate::output::Throughput::BytesPerSec(b) => assert!(
                *b > 0.0,
                "#537 prodlike-bench verification: W1f reported zero \
                 throughput; the cell may have short-circuited"
            ),
            other => panic!(
                "#537 prodlike-bench verification: W1f throughput must be \
                 BytesPerSec, got {other:?}"
            ),
        }
    }

    /// Load-bearing index-visibility check: build the composition under
    /// a caller-held tempdir, write ONE blob via `update_oneshot`, then
    /// query via `has_with_results` against the SAME `cas_store` handle
    /// production callers use BEFORE dropping the composition. Per
    /// CLAUDE.md's "Index-visibility contract": tests must not
    /// substitute `tokio::fs::metadata`-of-final-path or
    /// `tokio::fs::read` for the in-process visibility primitive
    /// (`has_with_results`) — those verify the kernel view, not the
    /// in-process index that the FilesystemStore's `evicting_map`
    /// gives production callers. (#537 D5-m2 fix.)
    ///
    /// We additionally walk the FilesystemStore tempdir for ≥1
    /// regular file as a belt-and-braces check: if a regression makes
    /// `update_oneshot` short-circuit entirely (e.g. returns Ok
    /// without dispatching to the slow tier), neither the file-walk
    /// nor the `has_with_results` will catch the absence of disk I/O
    /// at the upper layer; the file-walk gives that coverage
    /// independently.
    ///
    /// **Bespoke messages:**
    ///
    /// - `"#537 prodlike-bench verification: expected ≥1 file on disk
    ///   after iter; cell did NOT exercise FilesystemStore"`
    /// - `"#537 D5-m2 stale negative — index not updated post-rename"`
    ///
    /// **Mutation falsifier (file-walk):** in
    /// `build_prod_cas_composition`, swap the `StoreSpec::Filesystem(...)`
    /// with `StoreSpec::Memory(...)` — the composition still satisfies
    /// the trait, `update_oneshot` still succeeds, but no on-disk file
    /// is ever produced; the file-walk assertion red-fails with the
    /// bespoke "≥1 file on disk" message.
    ///
    /// **Mutation falsifier (has_with_results):** comment out the
    /// `update_oneshot` call entirely — `has_with_results` then
    /// returns `[None]` (no entry was inserted) and the assertion
    /// red-fails with the bespoke "stale negative — index not
    /// updated post-rename" message.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn w1f_drops_to_disk_under_held_tempdir() {
        use core::time::Duration;
        use nativelink_util::store_trait::StoreKey;

        let scratch_root = tempfile::TempDir::new().expect("test tempdir");
        let composition = build_prod_cas_composition(Some(scratch_root.path()))
            .await
            .expect("composition must build");
        // The composition's _temp_dir is the FilesystemStore root;
        // we walk THAT to count files.
        let fs_root = composition._temp_dir.path().to_path_buf();
        // Write one blob via the exact same call as run_w1f's body.
        let (digest, data) =
            make_blob_with_indices(W1F_SCENARIO_NAME, 0, 0, PRODLIKE_CELL_SIZE_BYTES);
        composition
            .cas_store
            .update_oneshot(digest, data)
            .await
            .expect("W1f update_oneshot must succeed");

        // Index-visibility check: same seam production callers cross.
        // 5-second timeout = deadlock detector per CLAUDE.md's
        // index-visibility contract (a stuck rename or a missed
        // evicting_map insert manifests as a hang, not a wrong value;
        // generic `is_err()` would mask `tokio::time::Elapsed`).
        let key: StoreKey<'static> = StoreKey::from(digest);
        let mut results: [Option<u64>; 1] = [None];
        tokio::time::timeout(
            Duration::from_secs(5),
            composition.cas_store.has_with_results(&[key], &mut results),
        )
        .await
        .expect(
            "#537 D5-m2 stale negative — index not updated post-rename: \
             has_with_results timed out (>5s); upper-layer visibility \
             primitive wedged",
        )
        .expect("has_with_results must not return Err");
        assert_eq!(
            results[0],
            Some(PRODLIKE_CELL_SIZE_BYTES as u64),
            "#537 D5-m2 stale negative — index not updated post-rename: \
             expected Some({}) after update_oneshot, got {:?}; the same \
             seam production callers cross reported the blob as absent \
             — either the upper-layer ExistenceCache wasn't populated or \
             the underlying FilesystemStore evicting_map insert was \
             skipped (mirrors the 2026-05-04 finalize_holding \
             regression class cited in CLAUDE.md)",
            PRODLIKE_CELL_SIZE_BYTES,
            results[0],
        );

        // Belt-and-braces: walk fs_root and count regular files.
        // FilesystemStore lays blobs under <content_path>/<...>; we
        // tolerate any subtree shape.
        let file_count = tokio::task::spawn_blocking(move || {
            count_regular_files(&fs_root)
        })
        .await
        .expect("walker task must not panic");
        assert!(
            file_count >= 1,
            "#537 prodlike-bench verification: expected ≥1 file on disk \
             after iter; cell did NOT exercise FilesystemStore"
        );
        // Hold composition alive past the assertion so the on-disk
        // files are present at check time.
        drop(composition);
    }

    /// Recursively count regular files under `root`. Symlinks are not
    /// followed. Used by `w1f_drops_to_disk_under_held_tempdir` to
    /// verify the FilesystemStore actually wrote files.
    fn count_regular_files(root: &Path) -> usize {
        let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
        let mut count: usize = 0;
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry_res in entries {
                let Ok(entry) = entry_res else { continue };
                let Ok(ft) = entry.file_type() else { continue };
                if ft.is_dir() {
                    stack.push(entry.path());
                } else if ft.is_file() {
                    count += 1;
                }
            }
        }
        count
    }
}
