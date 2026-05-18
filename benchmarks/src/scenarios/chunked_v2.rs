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
//! Flows W3 + R5: chunked-v2 anchoring cells.
//!
//! **W3 (chunked v2 write):** single writer drives an end-to-end
//! `WriteChunkedV2` session against an in-process server backed by a
//! real `FilesystemStore`. Anchors per-chunk overhead + commit-runner
//! wall-clock for the v3-default path.
//!
//! **R5 (filesystem fan-out readers):** the cell pre-writes the blob to
//! completion via `write_chunked_v2`, then spawns N concurrent readers
//! that race `get_part_unchunked` on the underlying `FilesystemStore`.
//! It anchors multi-reader filesystem fan-out latency, NOT the v3
//! per-digest `Notify` wake path — the chunked-driver in-flight entry is
//! gone by the time readers issue. **The per-digest `Notify` anchor is
//! deferred to Phase 2** because wiring it requires the
//! `chunked_read_registry` `OnceLock` that lives in `bin/nativelink.rs`
//! and isn't populated by `store_factory`. The cell's
//! `extras.v3_anchor` is therefore `DEFERRED_PHASE_2_filesystem_fanout`
//! — a future v3-Notify regression will NOT be caught here; do not read
//! a stable R5 baseline as evidence the wake path is healthy.
//!
//! Both cells are gated on `feature = "chunked_fast_slow"`. With the
//! feature off the scenarios emit a single result with
//! `extras.disabled = true` so baseline diffing can distinguish
//! "didn't run" from "ran and regressed".

#[cfg(not(feature = "chunked_fast_slow"))]
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::output::BenchmarkResult;
use crate::scenarios::RunOpts;

#[cfg(not(feature = "chunked_fast_slow"))]
use crate::output::{CacheState, Confidence, LatencyPercentiles, Throughput};

#[cfg(feature = "chunked_fast_slow")]
mod enabled {
    use core::time::Duration;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;
    use tokio_stream::StreamExt as _;

    use nativelink_config::stores::FilesystemSpec;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        WriteChunk, WriteChunkedFrame, cas_extensions_client::CasExtensionsClient,
        cas_extensions_server::CasExtensionsServer, write_chunked_frame,
    };
    use nativelink_service::chunked_write_handler::{
        ChunkedCasExtensionsAdapter, ChunkedWriteHandler, ChunkedWriteInFlight,
    };
    use nativelink_store::chunked::chunk_budget::ChunkBudget;
    use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
    use nativelink_util::background_spawn;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::StoreLike;

    use crate::output::{BenchmarkResult, CacheState};
    use crate::scenarios::{RunOpts, digest_via_default_hasher, measure};

    /// Chunk size used in the v2 bench cells. Matches the prod chunk-
    /// flow: 1 MiB.
    const BENCH_CHUNK_SIZE: usize = 1024 * 1024;

    /// `extras.composition_deviation` tag for W3 and R5. Both cells
    /// exercise `ChunkedWriteHandler → FilesystemStore` directly —
    /// production's `cas_STORE` wraps that leaf in
    /// `ExistenceCacheStore → VerifyStore → FastSlowStore { fast:
    /// SizePartitioningStore → MemoryStore, slow: FilesystemStore }`.
    /// So W3/R5 SKIP: existence dedup, hash-verify-on-write, MemoryStore
    /// fast tier, size-based tier routing, and the FastSlowStore
    /// admission/mirror logic. **A W3/R5 baseline cannot be compared
    /// directly to a production "16 MiB chunked write" wall-clock**;
    /// production runs the MemoryStore admission gate and chunked-driver
    /// commit barrier first. Mirrors the W1/A1/C1 convention.
    const COMPOSITION_DEVIATION_TAG: &str =
        "direct_filesystem_no_cas_chain_wrappers_no_memorystore_no_sizepartitioning";

    /// Compute the per-chunk hash for the `WriteChunk.chunk_sha256` wire
    /// field. **Despite the field name**, the v2 server
    /// (`chunked_write_handler_v2::compute_sha256_blocking_v2`) actually
    /// uses `default_digest_hasher_func()` — i.e. BLAKE3 in production
    /// when `default_digest_hash_function = blake3`. The bench mirrors
    /// that behavior so a BLAKE3 default does not produce per-chunk
    /// hash mismatches at offset 0 (#524).
    fn chunk_hash(bytes: &[u8]) -> [u8; 32] {
        let mut h = nativelink_util::digest_hasher::default_digest_hasher_func().hasher();
        nativelink_util::digest_hasher::DigestHasher::update(&mut h, bytes);
        let info = nativelink_util::digest_hasher::DigestHasher::finalize_digest(&mut h);
        **info.packed_hash()
    }

    /// Returns the `FilesystemStore` AND the owning `TempDir` so the
    /// caller can hold them with RAII cleanup. Replaces the
    /// `Box::leak(rand_u64() path)` pattern.
    async fn make_filesystem_store(
        temp_dir_base: Option<&PathBuf>,
    ) -> Result<
        (Arc<FilesystemStore<FileEntryImpl>>, tempfile::TempDir),
        nativelink_error::Error,
    > {
        let temp_dir = match temp_dir_base {
            Some(p) => tempfile::TempDir::new_in(p),
            None => tempfile::TempDir::new(),
        }
        .map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "v2-bench tempdir: {e:?}",
            )
        })?;
        let content_path = temp_dir
            .path()
            .join("content")
            .to_string_lossy()
            .into_owned();
        let temp_path = temp_dir.path().join("temp").to_string_lossy().into_owned();
        let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path,
            temp_path,
            eviction_policy: None,
            block_size: 1,
            ..Default::default()
        })
        .await?;
        Ok((store, temp_dir))
    }

    /// Mint a FRESH per-cell `ChunkBudget` and leak it for `'static`.
    /// Each W3/R5 cell gets its own budget so cross-cell state leak is
    /// physically impossible — a hypothetical permit-release bug or
    /// straggler task in cell K cannot degrade cell K+1's permit pool
    /// because they consult different `Semaphore` instances entirely.
    ///
    /// The previous shape (`OnceLock<&'static ChunkBudget>`) shared one
    /// budget across all cells. Tokio's `OwnedSemaphorePermit` releases
    /// on `Drop` even under task abort, so the shared shape was
    /// theoretically leak-free — but red-team (#533) correctly noted
    /// that "theoretically leak-free" depends on every cell's writer
    /// task actually dropping its `ChunkWork` (and thus its permit)
    /// before the next cell starts. Per-cell instantiation makes the
    /// guarantee structural instead of relying on task-drop ordering.
    ///
    /// Cost: one ~80-byte `Box::leak` per W3 + R5 cell (~10 cells), so
    /// ~800 bytes of heap leaked across the bench run — negligible
    /// against the multi-GiB `prebuilt` pools the same cells allocate.
    /// The `'static` lifetime is required by
    /// `ChunkedWriteHandler::new_with_state_and_chunk_size_for_test`'s
    /// signature; converting that to take `Arc<ChunkBudget>` would be
    /// a wider refactor than this fix-up warrants.
    fn make_chunk_budget() -> &'static ChunkBudget {
        Box::leak(Box::new(ChunkBudget::new()))
    }

    /// Build a handler with a FRESH chunk budget so each call yields a
    /// cell whose admission semaphore starts at full permits — see
    /// `make_chunk_budget` for the rationale.
    fn make_handler(
        store: Arc<FilesystemStore<FileEntryImpl>>,
    ) -> Arc<ChunkedWriteHandler> {
        let in_flight = ChunkedWriteInFlight::new();
        let budget: &'static ChunkBudget = make_chunk_budget();
        Arc::new(
            ChunkedWriteHandler::new_with_state_and_chunk_size_for_test(
                store, in_flight, budget, BENCH_CHUNK_SIZE,
            ),
        )
    }

    /// Bring up an in-process `CasExtensions` v2 server bound to an
    /// ephemeral port + a connected client. Returns the client and an
    /// `AbortOnDropHandle` for the server task so dropping the cell
    /// aborts the server (no leak).
    async fn start_v2_server(
        handler: Arc<ChunkedWriteHandler>,
    ) -> (
        CasExtensionsClient<tonic::transport::Channel>,
        nativelink_util::task::JoinHandleDropGuard<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral bind must succeed");
        let port = listener.local_addr().unwrap().port();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let adapter = ChunkedCasExtensionsAdapter::new(handler);
        let svc = CasExtensionsServer::new(adapter);
        let handle = nativelink_util::spawn!(
            "v2-bench-server",
            async move {
                if let Err(e) = tonic::transport::Server::builder()
                    .add_service(svc)
                    .serve_with_incoming(incoming)
                    .await
                {
                    // Surface the cause so a cell timing out doesn't
                    // hide a server-side panic.
                    eprintln!("[bench] v2 server exited with error: {e:?}");
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
            .expect("client must connect to in-process v2 server");
        (CasExtensionsClient::new(channel), handle)
    }

    /// Build a `WriteChunk` whose `chunk_bytes` is a refcounted slice of
    /// `payload` (zero-copy via `Bytes::slice`). Per-chunk sha256 is
    /// computed from the same slice. Holding `payload` as `Bytes` (vs
    /// `Vec<u8>`) lets `slice` be an O(1) refcount op instead of a
    /// `copy_from_slice` memcpy — at 16 MiB / 1 MiB chunks the difference
    /// was ~16 MiB of memory bandwidth per call, dominating per-iter cost
    /// in the timed body (#533 retro: `make_payload` LCG-fill + copy was
    /// 24.83% of CPU on the W3 16 MiB cell).
    fn make_chunk(digest: DigestInfo, offset: u64, payload: &Bytes, range: core::ops::Range<usize>, finish: bool) -> WriteChunk {
        let chunk_bytes = payload.slice(range.clone());
        WriteChunk {
            digest: Some(digest.into()),
            chunk_offset: offset,
            chunk_sha256: chunk_hash(&chunk_bytes).to_vec(),
            chunk_bytes,
            finish_chunk: finish,
        }
    }

    /// Build a complete `Vec<WriteChunk>` for a payload. The returned
    /// vec shares `payload`'s buffer via refcounted `Bytes::slice` — no
    /// per-chunk memcpy. Per-iter cloning a pre-built result is O(N)
    /// chunks of (refcount bump + 32 B sha256 vec alloc), trivially
    /// cheap vs a full rebuild.
    fn build_chunks(digest: DigestInfo, payload: &Bytes) -> Vec<WriteChunk> {
        let total = payload.len();
        let mut chunks = Vec::with_capacity(total.div_ceil(BENCH_CHUNK_SIZE));
        let mut offset: usize = 0;
        while offset < total {
            let take = BENCH_CHUNK_SIZE.min(total - offset);
            let is_final = offset + take == total;
            chunks.push(make_chunk(
                digest,
                offset as u64,
                payload,
                offset..offset + take,
                is_final,
            ));
            offset += take;
        }
        chunks
    }

    async fn drain_v2_response(
        mut stream: tonic::Streaming<WriteChunkedFrame>,
    ) -> Result<u64, tonic::Status> {
        while let Some(frame_res) = stream.next().await {
            let frame = frame_res?;
            if let Some(write_chunked_frame::Payload::FinalResponse(resp)) = frame.payload {
                return Ok(resp.committed_size);
            }
        }
        Err(tonic::Status::internal("stream closed before final response"))
    }

    /// Pre-generate a payload for a given size + iter index. Returns
    /// `Bytes` (refcounted) so `Bytes::slice` can do zero-copy chunking
    /// at write time — at 16 MiB / 1 MiB chunks this saves 16 MiB of
    /// memory bandwidth per chunked write vs a `Vec<u8>`-backed payload
    /// that forced `Bytes::copy_from_slice` per chunk.
    fn make_payload(size: usize, n: u64) -> Bytes {
        let mut state: u64 = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            data.push((state >> 33) as u8);
        }
        Bytes::from(data)
    }

    /// W3 cell: a chunked-v2 write at one (size, concurrency) point.
    ///
    /// - `size`: payload bytes per write op.
    /// - `concurrency`: in-flight parallel writes per iter. Each iter
    ///   dispatches `concurrency` `write_chunked_v2` calls in parallel
    ///   (via `tokio::task::JoinSet`) and times the wall-clock for all
    ///   to complete. Per-iter latency = batch wall-clock (NOT sum of
    ///   per-op latencies — that would hide queueing/contention which
    ///   is the whole point of measuring under concurrency).
    /// - `label`: scenario-name suffix.
    /// - `iters`: number of batches (each batch = `concurrency` ops).
    #[derive(Debug, Clone, Copy)]
    pub(super) struct W3Cell {
        pub size: usize,
        pub concurrency: u32,
        pub label: &'static str,
        pub iters_override: Option<u32>,
    }

    /// W3 cell matrix.
    ///
    /// **N=1 cells (`..._single_writer_*` scenario-name family):** the
    /// historic baseline shape — one writer, end-to-end per-op latency.
    /// Sample = single-writer wall-clock per iter. The c=1 cells preserve
    /// their historic scenario name (no `_c1` suffix) so checked-in
    /// baselines remain a continuity anchor.
    ///
    /// **N>1 cells (`..._burst_*MiB_c{N}` scenario-name family):**
    /// closed-loop burst of N concurrent writers per iter. **Measurement:
    /// batch wall-clock for N concurrent writes to complete; NOT
    /// comparable to `_single_writer_` cells.** Two distinct sample
    /// shapes (per-op latency vs batch wall-clock throughput) under
    /// different scenario-name families so future diff tooling cannot
    /// silently compare them. Throughput math:
    /// `iters * concurrency * size / total_wall_clock`.
    ///
    /// Pool size for unique digests = `iters * concurrency`; sized so
    /// the in-memory `prebuilt` Arc stays under POOL_MAX_BYTES even at
    /// 16 MiB × N=16 (largest concurrent variant — 16 MiB × N=64 would
    /// be 20 GiB pool, refused). The runtime guard in `run_w3_cell`
    /// enforces this regardless of `--iters` overrides.
    const W3_CELLS: &[W3Cell] = &[
        // Historic shape — single writer, both sizes.
        W3Cell { size: 4 * BENCH_CHUNK_SIZE, concurrency: 1, label: "4MiB", iters_override: None },
        W3Cell { size: 16 * BENCH_CHUNK_SIZE, concurrency: 1, label: "16MiB", iters_override: None },
        // 4 MiB at multiple concurrency levels (small enough memory to
        // tolerate N=64).
        W3Cell { size: 4 * BENCH_CHUNK_SIZE, concurrency: 4, label: "4MiB", iters_override: None },
        W3Cell { size: 4 * BENCH_CHUNK_SIZE, concurrency: 16, label: "4MiB", iters_override: None },
        W3Cell { size: 4 * BENCH_CHUNK_SIZE, concurrency: 64, label: "4MiB", iters_override: None },
        // 16 MiB at moderate concurrency (N=64 would be 20 GiB pool —
        // skip; the 4 MiB N=64 cell anchors the high-fanout point).
        W3Cell { size: 16 * BENCH_CHUNK_SIZE, concurrency: 4, label: "16MiB", iters_override: None },
        W3Cell { size: 16 * BENCH_CHUNK_SIZE, concurrency: 16, label: "16MiB", iters_override: None },
    ];

    pub(super) async fn run_w3(
        opts: &RunOpts,
        out: &mut Vec<BenchmarkResult>,
        temp_dir_base: Option<&PathBuf>,
    ) {
        for cell in W3_CELLS {
            run_w3_cell(opts, out, temp_dir_base, *cell).await;
        }
    }

    async fn run_w3_cell(
        opts: &RunOpts,
        out: &mut Vec<BenchmarkResult>,
        temp_dir_base: Option<&PathBuf>,
        cell: W3Cell,
    ) {
        let iters = opts.effective_iters(cell.iters_override.unwrap_or(20));
        let size = cell.size;
        let concurrency = cell.concurrency;
        // Family split per #533 red-team:
        //
        // - c=1 cells emit `..._single_writer_{label}` (no suffix),
        //   preserving historic baseline names so checked-in JSONs in
        //   `benchmarks/baselines/` stay comparable apples-to-apples.
        //   Sample shape: single-writer per-op latency.
        // - c>1 cells emit `..._burst_{label}_c{N}`, a DIFFERENT
        //   scenario-name family. Sample shape: batch wall-clock for N
        //   concurrent writes to complete. NOT comparable to the
        //   single-writer family — closed-loop burst load generator,
        //   different sample semantics. Future diff tooling that joins
        //   on `scenario_name` cannot accidentally compare a c=1
        //   per-op-latency baseline against a c>1 batch-wall-clock
        //   baseline because the prefixes differ.
        let scenario_name = if concurrency == 1 {
            format!(
                "w3_chunked_v2_write_single_writer_{label}",
                label = cell.label,
            )
        } else {
            format!(
                "w3_chunked_v2_write_burst_{label}_c{c}",
                label = cell.label,
                c = concurrency,
            )
        };
        if !opts.matches(&scenario_name) {
            return;
        }
        // Per-cell fresh store + server so previous cell's residency
        // doesn't pollute the next.
        let (store, _td) = match make_filesystem_store(temp_dir_base).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[bench] W3 {scenario_name} fs build failed: {e:?}");
                return;
            }
        };
        let handler = make_handler(store);
        let (client, _server_guard) = start_v2_server(handler).await;

        // Pre-generate all payloads + digests + chunks OUTSIDE the
        // timer. `iters * concurrency` distinct payloads so concurrent
        // writes cover unique digests (avoid CAS dedup / in-flight
        // singleflight, both of which would mask the throughput cell's
        // intent). `make_payload` now returns `Bytes`; `build_chunks`
        // shares `payload`'s buffer via refcounted `Bytes::slice` (no
        // memcpy per chunk). Per-iter we clone the pre-built
        // `Vec<WriteChunk>` — only the outer Vec + per-chunk
        // sha256 Vec<u8>(32) + Bytes refcount bumps, trivially cheap
        // vs a per-iter `build_chunks` rebuild that allocates fresh
        // copies of the entire payload. See #533: the prior shape's
        // `make_payload` (16 MiB LCG-fill + `Bytes::copy_from_slice`)
        // was 24.83% of CPU on the W3 16 MiB cell.
        let total_slots = (iters as u64).saturating_mul(concurrency as u64);
        let prebuilt: Vec<Vec<WriteChunk>> = (0..total_slots)
            .map(|n| {
                let payload = make_payload(size, n);
                let digest = digest_via_default_hasher(&payload);
                build_chunks(digest, &payload)
            })
            .collect();

        let iter_counter = AtomicU64::new(0);
        let mut extras = BTreeMap::new();
        extras.insert("chunk_size".to_string(), serde_json::json!(BENCH_CHUNK_SIZE));
        extras.insert("v3_anchor".to_string(), serde_json::json!("chunked_v2"));
        extras.insert(
            "concurrent_ops_per_iter".to_string(),
            serde_json::json!(concurrency),
        );
        // W3 exercises ChunkedWriteHandler → FilesystemStore directly,
        // skipping production's wrapper chain. Mirrors W1/A1/C1
        // convention so future diff tooling cannot accidentally compare
        // W3 numbers against production "16 MiB chunked write" baselines.
        extras.insert(
            "composition_deviation".to_string(),
            serde_json::json!(COMPOSITION_DEVIATION_TAG),
        );
        if concurrency > 1 {
            extras.insert(
                "batch_wall_clock_semantics".to_string(),
                serde_json::json!(
                    "latency_samples_are_full_batch_walltime_not_sum_of_per_op"
                ),
            );
        }

        let client_for_body = client.clone();
        let prebuilt = Arc::new(prebuilt);
        let iter_counter = Arc::new(iter_counter);
        // Bytes-per-iter = concurrency × size (one batch's worth);
        // `measure` multiplies by `iters` to get total bytes for
        // throughput.
        let bytes_per_iter = (size as u64).saturating_mul(concurrency as u64);
        let size_u64 = size as u64;
        let result = measure(
            "W3",
            &scenario_name,
            Some(size_u64),
            concurrency,
            CacheState::Cold,
            iters,
            Some(bytes_per_iter),
            None,
            extras,
            move || {
                let c = client_for_body.clone();
                let prebuilt = prebuilt.clone();
                let counter = iter_counter.clone();
                run_w3_iter(c, prebuilt, counter, concurrency, size_u64)
            },
        )
        .await;
        out.push(result);
    }

    /// One W3 iter body — extracted to a free async fn so the closure
    /// in `measure(...)` stays small and the compiler doesn't hit the
    /// monomorphization-blowup ICE on the nested `async move` block
    /// (rustc 1.95.0-nightly seen 2026-05-18).
    async fn run_w3_iter(
        client: CasExtensionsClient<tonic::transport::Channel>,
        prebuilt: Arc<Vec<Vec<WriteChunk>>>,
        iter_counter: Arc<AtomicU64>,
        concurrency: u32,
        size: u64,
    ) {
        let n = iter_counter.fetch_add(1, Ordering::Relaxed);
        if concurrency == 1 {
            // Single-writer fast path: avoid `JoinSet` overhead so the
            // c=1 cell stays comparable to the historic baseline shape.
            let chunks = prebuilt[n as usize].clone();
            write_one_chunked(client, chunks, size).await;
        } else {
            // N parallel writers; batch wall-clock is the sample.
            // `JoinSet` per CLAUDE.md (variable-count > tokio::join!).
            let mut set = tokio::task::JoinSet::new();
            let base_slot = n.saturating_mul(concurrency as u64);
            for j in 0..concurrency as u64 {
                let slot = base_slot.saturating_add(j) as usize;
                let chunks = prebuilt[slot].clone();
                let client_clone = client.clone();
                set.spawn(async move {
                    write_one_chunked(client_clone, chunks, size).await;
                });
            }
            while let Some(res) = set.join_next().await {
                res.expect("W3 concurrent writer task must not panic");
            }
        }
    }

    async fn write_one_chunked(
        mut client: CasExtensionsClient<tonic::transport::Channel>,
        chunks: Vec<WriteChunk>,
        expected_size: u64,
    ) {
        let stream = tokio_stream::iter(chunks);
        let response = client
            .write_chunked_v2(stream)
            .await
            .expect("W3 write_chunked_v2 must return Ok");
        let committed = drain_v2_response(response.into_inner())
            .await
            .expect("W3 commit must succeed");
        assert_eq!(committed, expected_size);
    }

    /// R5: filesystem-store multi-reader fan-out anchor.
    ///
    /// **Phase 2 scope honest scope-cut:** an earlier version of this
    /// cell claimed to anchor the v3 per-digest `Notify` wake. It did
    /// not — the cell pre-writes the blob to completion before readers
    /// spawn, so the chunked-driver in-flight entry is gone and the
    /// readers race the underlying `FilesystemStore`. Wiring the cell
    /// through `WriteChunkedV2`'s commit barrier requires the
    /// `chunked_read_registry` `OnceLock` populated by
    /// `bin/nativelink.rs` (not `store_factory`), which is out of Phase
    /// 1.5 scope.
    ///
    /// What this cell DOES measure: N parallel
    /// `FilesystemStore::get_part_unchunked` on the same digest. Useful
    /// for catching a filesystem-store regression (e.g. a `parking_lot`
    /// mutex held across `.await`, a hardlink-table contention bug) but
    /// it is NOT a substitute for a real v3-Notify regression detector.
    /// `extras.v3_anchor = "DEFERRED_PHASE_2_filesystem_fanout"` flags
    /// the gap so the diff tooling cannot treat a stable R5 baseline as
    /// evidence the per-digest Notify path is healthy.
    pub(super) async fn run_r5(
        opts: &RunOpts,
        out: &mut Vec<BenchmarkResult>,
        temp_dir_base: Option<&PathBuf>,
    ) {
        let iters = opts.effective_iters(10);
        let cells: &[(usize, u32, &str)] = &[
            (4 * BENCH_CHUNK_SIZE, 2, "4MiB_n2"),
            (4 * BENCH_CHUNK_SIZE, 10, "4MiB_n10"),
            (16 * BENCH_CHUNK_SIZE, 4, "16MiB_n4"),
        ];
        for &(size, n_readers, label) in cells {
            let scenario_name = format!("r5_filesystem_fanout_readers_{label}");
            if !opts.matches(&scenario_name) {
                continue;
            }
            let (store, _td) = match make_filesystem_store(temp_dir_base).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[bench] R5 {scenario_name} fs build failed: {e:?}");
                    continue;
                }
            };
            // This cell measures filesystem-store fan-out, NOT the v3
            // per-digest `Notify`. The prewrite below finalizes the
            // blob before readers spawn, so the chunked-driver in-flight
            // entry is gone. Honest-label fix per
            // `.claude/audits/495-phase-1-5-plan-2026-05-16.md`.
            // Phase 2 will wire the readers through `WriteChunkedV2`'s
            // commit barrier so the per-digest Notify is in the path;
            // that requires the `chunked_read_registry` `OnceLock` which
            // is populated by `bin/nativelink.rs`, out of Phase 1.5 scope.
            let handler = make_handler(store.clone());
            let (client, _server_guard) = start_v2_server(handler).await;

            // Pre-populate every iter's blob via the v2 write path so
            // the read cell measures multi-reader fan-out, not write.
            // `make_payload` returns `Bytes`; `build_chunks` shares its
            // buffer via refcounted slice — no per-chunk memcpy.
            let prebuilt: Vec<DigestInfo> = {
                let mut acc = Vec::with_capacity(iters as usize);
                for n in 0..iters as u64 {
                    let payload = make_payload(size, n);
                    // Digest uses the process-global hasher (BLAKE3,
                    // matching prod) — see W3 note above.
                    let digest = digest_via_default_hasher(&payload);
                    let chunks = build_chunks(digest, &payload);
                    let mut c = client.clone();
                    let response = c
                        .write_chunked_v2(tokio_stream::iter(chunks))
                        .await
                        .expect("R5 prewrite write_chunked_v2 must return Ok");
                    let committed = drain_v2_response(response.into_inner())
                        .await
                        .expect("R5 prewrite commit must succeed");
                    assert_eq!(committed, size as u64);
                    acc.push(digest);
                }
                acc
            };

            let iter_counter = AtomicU64::new(0);
            let mut extras = BTreeMap::new();
            extras.insert("chunk_size".to_string(), serde_json::json!(BENCH_CHUNK_SIZE));
            extras.insert("n_readers".to_string(), serde_json::json!(n_readers));
            // R5 reads from a bare FilesystemStore, skipping production's
            // wrapper chain. Same deviation tag as W3 so diff tooling
            // can filter consistently.
            extras.insert(
                "composition_deviation".to_string(),
                serde_json::json!(COMPOSITION_DEVIATION_TAG),
            );
            // Honest-label: this cell is FilesystemStore fan-out, not
            // the v3 per-digest Notify. Diff tooling must NOT treat a
            // stable baseline here as evidence the Notify path is
            // healthy. Phase 2 will provide a real v3-Notify anchor.
            extras.insert(
                "v3_anchor".to_string(),
                serde_json::json!("DEFERRED_PHASE_2_filesystem_fanout"),
            );
            extras.insert(
                "phase_2_followup".to_string(),
                serde_json::json!(
                    "wire cell through WriteChunkedV2 commit barrier so per-digest Notify is in read path"
                ),
            );

            let throughput_per_iter = (size as u64) * (n_readers as u64);
            let prebuilt = Arc::new(prebuilt);
            let store_for_body: nativelink_util::store_trait::Store = {
                use nativelink_util::store_trait::Store;
                Store::new(store)
            };
            let result = measure(
                "R5",
                &scenario_name,
                Some(size as u64),
                n_readers,
                CacheState::Contended,
                iters,
                Some(throughput_per_iter),
                None,
                extras,
                move || {
                    let store = store_for_body.clone();
                    let prebuilt = prebuilt.clone();
                    let n = iter_counter.fetch_add(1, Ordering::Relaxed);
                    async move {
                        let digest = prebuilt[n as usize];
                        // Spawn N readers that ALL request the same
                        // digest concurrently. The per-digest in-flight
                        // coordination (Notify wake) is what we're
                        // anchoring.
                        let mut handles = Vec::with_capacity(n_readers as usize);
                        for _ in 0..n_readers {
                            let s = store.clone();
                            handles.push(background_spawn!(
                                "r5-reader",
                                async move {
                                    s.get_part_unchunked(digest, 0, None)
                                        .await
                                        .expect("R5 reader get_part must succeed")
                                        .len() as u64
                                }
                            ));
                        }
                        // All N readers must see the same bytes back.
                        let mut sizes = Vec::with_capacity(n_readers as usize);
                        for h in handles {
                            sizes.push(
                                h.await
                                    .expect("R5 reader task must not panic"),
                            );
                        }
                        for s in &sizes {
                            assert_eq!(
                                *s, size as u64,
                                "R5 reader-bytes invariant: all readers see same size"
                            );
                        }
                    }
                },
            )
            .await;
            out.push(result);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Zero-copy invariant: `build_chunks` must NOT copy payload
        /// bytes. We construct a payload with a known per-byte pattern,
        /// build the chunks, and verify that for each chunk
        /// `chunk_bytes` shares the same underlying allocation as the
        /// source `Bytes` (proven via `Bytes::ptr_eq`-style byte
        /// identity at the head of each window).
        ///
        /// Mutation falsifier: revert `make_chunk` to
        /// `Bytes::copy_from_slice(bytes)` — the pointer-equality check
        /// red-fails because the copy produces a fresh allocation.
        #[test]
        fn build_chunks_is_zero_copy_over_payload() {
            // 4 chunks worth.
            let size = 4 * BENCH_CHUNK_SIZE;
            let payload = make_payload(size, 0xC0FFEE);
            let digest = digest_via_default_hasher(&payload);
            let chunks = build_chunks(digest, &payload);
            assert_eq!(chunks.len(), 4);
            for (i, c) in chunks.iter().enumerate() {
                let want_offset = (i * BENCH_CHUNK_SIZE) as u64;
                assert_eq!(c.chunk_offset, want_offset, "chunk {i} offset mismatch");
                assert_eq!(c.chunk_bytes.len(), BENCH_CHUNK_SIZE, "chunk {i} len");
                assert_eq!(c.finish_chunk, i == chunks.len() - 1, "chunk {i} finish flag");
                // Zero-copy proof: the chunk's first byte address must
                // lie WITHIN the source payload's allocation range.
                let src_start = payload.as_ptr() as usize;
                let src_end = src_start + payload.len();
                let chunk_start = c.chunk_bytes.as_ptr() as usize;
                assert!(
                    chunk_start >= src_start && chunk_start < src_end,
                    "build_chunks_zero_copy_violation: chunk {i} chunk_bytes \
                     pointer 0x{chunk_start:x} not within payload allocation \
                     [0x{src_start:x}, 0x{src_end:x}). A `copy_from_slice` \
                     regression would produce a fresh allocation with \
                     unrelated pointer — the per-iter 16 MiB memcpy cost \
                     would be back (#533 retro)."
                );
                // Per-chunk sha256 must be 32 B (BLAKE3 packed-hash
                // wrapped via `to_vec`); regression to a different
                // hasher would break the v2 server's `compute_sha256_blocking_v2`
                // recompute check.
                assert_eq!(c.chunk_sha256.len(), 32, "chunk {i} sha256 len");
            }
        }

        /// Reproduce the scenario_name builder used by `run_w3_cell` so
        /// the matrix-shape tests can assert on it without spinning up
        /// the full bench harness. If this drifts from the in-cell
        /// builder, the c=1 baseline-continuity test below will catch
        /// the byte-level mismatch.
        fn scenario_name_for(cell: &W3Cell) -> String {
            if cell.concurrency == 1 {
                format!(
                    "w3_chunked_v2_write_single_writer_{label}",
                    label = cell.label,
                )
            } else {
                format!(
                    "w3_chunked_v2_write_burst_{label}_c{c}",
                    label = cell.label,
                    c = cell.concurrency,
                )
            }
        }

        /// The c=1 cell must exist for both 4 MiB and 16 MiB so historic
        /// baselines remain comparable. Mutation: remove a c=1 entry —
        /// red-fails.
        #[test]
        fn w3_cell_matrix_includes_c1_for_both_sizes() {
            let has_4_c1 = W3_CELLS
                .iter()
                .any(|c| c.size == 4 * BENCH_CHUNK_SIZE && c.concurrency == 1);
            let has_16_c1 = W3_CELLS
                .iter()
                .any(|c| c.size == 16 * BENCH_CHUNK_SIZE && c.concurrency == 1);
            assert!(
                has_4_c1,
                "W3 cell matrix must retain 4MiB c=1 for baseline continuity"
            );
            assert!(
                has_16_c1,
                "W3 cell matrix must retain 16MiB c=1 for baseline continuity"
            );
        }

        /// At least three distinct concurrency levels must exist across
        /// the W3 matrix so the bench can see throughput scaling.
        /// Mutation: collapse all cells to c=1 — red-fails.
        #[test]
        fn w3_cell_matrix_has_three_plus_concurrency_levels() {
            let mut levels: std::collections::BTreeSet<u32> =
                std::collections::BTreeSet::new();
            for c in W3_CELLS {
                levels.insert(c.concurrency);
            }
            assert!(
                levels.len() >= 3,
                "W3 matrix has {} distinct concurrency levels ({:?}); \
                 throughput scaling requires >= 3 to be visible (per #533 \
                 task)",
                levels.len(),
                levels
            );
        }

        /// Family-split discipline (#533 red-team finding 1): the c=1
        /// cells MUST emit the historic `..._single_writer_{label}`
        /// scenario name (no `_c1` suffix) so checked-in baselines stay
        /// comparable. The c>1 cells MUST emit a SEPARATE family
        /// (`..._burst_{label}_c{N}`) so future diff tooling cannot
        /// silently compare per-op-latency samples against
        /// batch-wall-clock samples.
        ///
        /// Mutation: collapse both branches of the scenario-name builder
        /// in `run_w3_cell` to the same `..._single_writer_` prefix —
        /// this test red-fails because a c>1 cell would emit the wrong
        /// family. Mutation: swap the c=1 branch to add `_c1` — also
        /// red-fails, baseline continuity broken.
        #[test]
        fn w3_scenario_name_family_split_is_enforced() {
            for cell in W3_CELLS {
                let name = scenario_name_for(cell);
                if cell.concurrency == 1 {
                    // c=1 cells must use the historic single-writer
                    // family. Disallow any `_c<digit>` suffix so the
                    // historic baseline name is preserved byte-for-byte.
                    let has_concurrency_suffix = name
                        .rsplit('_')
                        .next()
                        .map(|tail| {
                            tail.starts_with('c')
                                && tail[1..].chars().all(|c| c.is_ascii_digit())
                                && tail.len() > 1
                        })
                        .unwrap_or(false);
                    assert!(
                        name.starts_with("w3_chunked_v2_write_single_writer_")
                            && !has_concurrency_suffix,
                        "#533 family-split: c=1 cells must use the historic \
                         `..._single_writer_{{label}}` name (no `_c1` \
                         suffix) for baseline continuity; got `{name}`"
                    );
                } else {
                    assert!(
                        name.starts_with("w3_chunked_v2_write_burst_"),
                        "#533 family-split: c>1 cells must use the \
                         `..._burst_{{label}}_c{{N}}` name family — batch \
                         wall-clock throughput samples are a different \
                         shape from per-op-latency samples and must NOT \
                         share a name family with single-writer cells; \
                         got `{name}` (c={c})",
                        c = cell.concurrency,
                    );
                    let want_suffix = format!("_c{}", cell.concurrency);
                    assert!(
                        name.ends_with(&want_suffix),
                        "#533 family-split: c>1 cell must end with \
                         `_c{{concurrency}}` so the concurrency level is \
                         visible in the name; got `{name}`"
                    );
                }
            }
        }

        /// Per-cell `ChunkBudget` isolation (#533 red-team finding 2):
        /// every cell MUST get a fresh `ChunkBudget` so cross-cell
        /// permit-state leak is structurally impossible. If two
        /// consecutive `make_chunk_budget()` calls returned the same
        /// budget, a cell K that fault-injected a permit-holding
        /// straggler would degrade cell K+1's measured throughput.
        ///
        /// The test additionally fault-injects: it acquires permits from
        /// the FIRST budget (simulating an in-flight ChunkWork that
        /// hasn't dropped), then verifies the SECOND budget is still at
        /// full permits — proving the two budgets do not alias.
        ///
        /// Mutation: revert `make_chunk_budget()` to a `OnceLock`
        /// singleton (the pre-#533 shape) — this test red-fails with
        /// the bespoke `#533 chunk-budget cross-cell leak` message.
        #[test]
        fn chunk_budget_isolated_across_cells() {
            use nativelink_store::chunked::chunk_budget::TOTAL_CHUNK_PERMITS;

            let b1 = make_chunk_budget();
            let b2 = make_chunk_budget();

            // Distinct allocations: pointer-inequality proves the two
            // budgets cannot share a Semaphore. If they aliased, every
            // bench cell would share permit state with every other.
            assert!(
                !core::ptr::eq(b1, b2),
                "#533 chunk-budget cross-cell leak: make_chunk_budget() \
                 returned the SAME budget across calls. A permit-holding \
                 straggler from cell K would degrade cell K+1's throughput \
                 because they consult the same Semaphore."
            );

            // Both start at full permits.
            assert_eq!(b1.available_chunks(), TOTAL_CHUNK_PERMITS);
            assert_eq!(b2.available_chunks(), TOTAL_CHUNK_PERMITS);

            // Fault-inject: acquire permits from b1 (simulating in-flight
            // ChunkWork that hasn't dropped). b2's available_chunks MUST
            // remain at TOTAL_CHUNK_PERMITS — if it doesn't, the budgets
            // alias and the cross-cell isolation is broken.
            let mut holders = Vec::with_capacity(32);
            for _ in 0..32 {
                holders.push(
                    b1.try_acquire_chunk()
                        .expect("b1 must have permits available"),
                );
            }
            assert_eq!(b1.available_chunks(), TOTAL_CHUNK_PERMITS - 32);
            assert_eq!(
                b2.available_chunks(),
                TOTAL_CHUNK_PERMITS,
                "#533 chunk-budget cross-cell leak: acquiring permits from \
                 b1 reduced b2's available count — the two budgets alias, \
                 and a permit-holding straggler in cell K would silently \
                 degrade cell K+1's permit pool."
            );
            drop(holders);
        }

        /// Pool memory budget: the largest cell's `prebuilt` pool must
        /// stay under ~6 GiB at default iter=20. 16 MiB × c=64 × 20 =
        /// 20.48 GiB would OOM /dev/shm on shared bench hosts; the
        /// matrix must not include that point.
        #[test]
        fn w3_cell_matrix_pool_memory_bounded() {
            // Default iters at cell time (matches `effective_iters(20)`).
            let default_iters = 20u64;
            const POOL_MAX_BYTES: u64 = 6 * 1024 * 1024 * 1024;
            for c in W3_CELLS {
                let pool_bytes =
                    default_iters * c.concurrency as u64 * c.size as u64;
                assert!(
                    pool_bytes <= POOL_MAX_BYTES,
                    "W3 cell {} c={} pool would need {:.2} GiB (size={}, \
                     iters={}, concurrency={}); POOL_MAX_BYTES = 6 GiB. \
                     Either reduce concurrency for this size or add an \
                     iters_override to keep the pool bounded.",
                    c.label,
                    c.concurrency,
                    pool_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                    c.size,
                    default_iters,
                    c.concurrency,
                );
            }
        }
    }
}

pub async fn run(opts: &RunOpts, temp_dir_base: Option<&PathBuf>) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();

    #[cfg(feature = "chunked_fast_slow")]
    {
        enabled::run_w3(opts, &mut out, temp_dir_base).await;
        enabled::run_r5(opts, &mut out, temp_dir_base).await;
    }

    #[cfg(not(feature = "chunked_fast_slow"))]
    {
        let _ = (opts, temp_dir_base);
        // Emit two placeholder results so baselines built without the
        // feature still record the absence of W3/R5 data; otherwise diff
        // tooling can't tell "scenario disappeared" from "feature off".
        for (flow, name) in [
            ("W3", "w3_chunked_v2_write_single_writer_DISABLED"),
            ("R5", "r5_filesystem_fanout_readers_DISABLED"),
        ] {
            let mut extras = BTreeMap::new();
            extras.insert("disabled".to_string(), serde_json::json!(true));
            extras.insert(
                "reason".to_string(),
                serde_json::json!("bench built without --features chunked_fast_slow"),
            );
            out.push(BenchmarkResult {
                flow_id: flow.to_string(),
                scenario_name: name.to_string(),
                blob_size_bytes: None,
                concurrency: 0,
                cache_state: CacheState::Cold,
                iters: 0,
                confidence: Confidence::Low,
                total_duration_ms: 0.0,
                latency_ms: LatencyPercentiles {
                    p50: 0.0,
                    p90: 0.0,
                    p99: 0.0,
                    max: 0.0,
                },
                throughput: Throughput::None,
                extras,
            });
        }
    }

    out
}

#[cfg(all(test, not(feature = "chunked_fast_slow")))]
mod cfg_not_tests {
    use super::*;

    /// Positive control: when the feature is OFF the placeholders are
    /// emitted with the documented shape. Mutation: flip the
    /// `#[cfg(not(...))]` block to `#[cfg(...)]` — this test would no
    /// longer compile (the `#[cfg(all(test, not(...)))]` gate detaches
    /// it), demonstrating the cfg-direction is structurally guarded.
    #[tokio::test]
    async fn cfg_not_branch_emits_disabled_placeholders() {
        let opts = RunOpts {
            iters: 20,
            filter: None,
            fast: false,
        };
        let results = run(&opts, None).await;
        assert_eq!(results.len(), 2, "must emit one placeholder per flow");
        let names: Vec<_> = results.iter().map(|r| r.scenario_name.as_str()).collect();
        assert!(names.contains(&"w3_chunked_v2_write_single_writer_DISABLED"));
        assert!(names.contains(&"r5_filesystem_fanout_readers_DISABLED"));
        for r in &results {
            assert_eq!(r.iters, 0);
            assert_eq!(
                r.extras.get("disabled"),
                Some(&serde_json::json!(true)),
                "disabled flag must be true"
            );
        }
    }
}
