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
pub(crate) mod enabled {
    use core::time::Duration;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;
    use parking_lot::Mutex;
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
    use nativelink_store::fast_slow_store::ChunkedInFlightMap;
    use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
    use nativelink_util::background_spawn;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::StoreLike;

    use crate::output::{BenchmarkResult, CacheState};
    use crate::scenarios::{RunOpts, digest_via_default_hasher, measure};

    /// Chunk size used in the v2 bench cells. Matches the prod chunk-
    /// flow: 1 MiB. Bumped to `pub(crate)` so the #537 prodlike cells
    /// (W3f) reuse the SAME constant instead of declaring their own,
    /// eliminating the m5/m3 silent-drift risk: if W3 ever changes its
    /// chunk size, W3f inherits the change for free and the
    /// `paired_baseline_cell` linkage stays apples-to-apples.
    pub(crate) const BENCH_CHUNK_SIZE: usize = 1024 * 1024;

    /// Hard cap on the W3 per-cell prebuilt-pool memory. Sized to fit
    /// under /dev/shm on a typical 256 GiB bench host while leaving
    /// ample headroom for the in-process server, in-flight chunked
    /// state, and co-resident tooling. Enforced at TWO levels:
    /// (1) `iters_override` on high-fanout W3 cells clamps the matrix
    ///     so default `--iters 20` and any user `--iters N` stay under
    ///     this cap;
    /// (2) the runtime guard in `run_w3_cell` panics BEFORE pool
    ///     allocation if the product still exceeds the cap (defends
    ///     against a future cell-matrix edit that breaks (1)).
    /// `w3_cell_matrix_pool_memory_bounded_at_max_iters_ceiling` proves
    /// (1) holds even under the W3_MAX_ITERS_CEILING shown below.
    const POOL_MAX_BYTES: u64 = 6 * 1024 * 1024 * 1024;

    /// Effective ceiling on the per-W3-cell iter count used for pool-
    /// sizing checks. The CLI's `--iters N` has no upper cap; this
    /// constant is the value used by the matrix-bound test to verify
    /// every cell's `iters_override` clamps high-fanout cells below
    /// `POOL_MAX_BYTES`. Cells whose `iters_override` is `None` are
    /// allowed to scale up to `W3_MAX_ITERS_CEILING` from the CLI; the
    /// test asserts even at that ceiling the pool stays under
    /// `POOL_MAX_BYTES`.
    #[cfg(test)]
    const W3_MAX_ITERS_CEILING: u64 = 200;

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
    ///
    /// # Phase-2 divergence guard (red-team #533 out-of-scope #1)
    ///
    /// W3 and R5 share this constant ONLY because both currently skip
    /// the same wrapper chain. If a future R5 wire-up (e.g. Phase 2
    /// routes readers through `WriteChunkedV2`'s commit barrier so the
    /// per-digest `Notify` is in the path) changes R5's composition
    /// relative to W3 — even by adding a single wrapper — this shared
    /// constant becomes silently WRONG for the divergent cell. The fix
    /// is mechanical: split into `W3_COMPOSITION_DEVIATION_TAG` +
    /// `R5_COMPOSITION_DEVIATION_TAG` at that point, NOT to keep the
    /// shared constant and "be careful." A reviewer touching either
    /// cell's `composition_deviation` extras insert MUST also re-verify
    /// the OTHER cell's composition matches before reusing this tag.
    pub(crate) const COMPOSITION_DEVIATION_TAG: &str =
        "direct_filesystem_no_cas_chain_wrappers_no_memorystore_no_sizepartitioning";

    /// Compute the per-chunk hash for the `WriteChunk.chunk_sha256` wire
    /// field. **Despite the field name**, the v2 server
    /// (`chunked_write_handler_v2::compute_sha256_blocking_v2`) actually
    /// uses `default_digest_hasher_func()` — i.e. BLAKE3 in production
    /// when `default_digest_hash_function = blake3`. The bench mirrors
    /// that behavior so a BLAKE3 default does not produce per-chunk
    /// hash mismatches at offset 0 (#524).
    pub(crate) fn chunk_hash(bytes: &[u8]) -> [u8; 32] {
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
    ///
    /// **Bare-handler shape.** None of the three production sinks
    /// (`with_v2_stable_digests_sink`, `with_v2_failed_commit_sink`,
    /// `with_chunked_in_flight_digests`) are installed. Used by R5
    /// (filesystem-fanout reader cell) where the readers race
    /// `FilesystemStore::get_part_unchunked` directly after a prewrite
    /// completes — the v2 commit path runs once during prewrite (outside
    /// the timed body) and is not the load-bearing measurement.
    ///
    /// W3 / W3f use [`make_handler_with_production_sinks`] instead
    /// (#541) so the chunked-v2 commit path pays the same per-iter
    /// bookkeeping cost it pays in production
    /// (`src/bin/nativelink.rs:912-925`).
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

    /// `extras.production_sinks_wired` value for the W3 / W3f cells
    /// after #541. Names the three sinks installed on the handler so a
    /// reader of a baseline JSON in isolation can tell that the per-iter
    /// p50/p99 includes production durability-bookkeeping cost
    /// (mutex+notify per commit on success, HashMap insert+remove per
    /// session). Pinned by `w3_production_sinks_wired_tag_is_stable`;
    /// drift of this tag breaks the diff tooling's ability to filter
    /// pre-#541 vs post-#541 baselines.
    pub(crate) const PRODUCTION_SINKS_WIRED_TAG: &str =
        "stable_digests_pusher + failed_writes_inserter + chunked_in_flight_digests";

    /// `extras.production_sinks_wired` value for cells that DO NOT wire
    /// the three production sinks. Used by R5 (which only runs the v2
    /// write path during prewrite, outside the timed body). Distinct
    /// string so the diff tooling can filter on this field rather than
    /// inferring sink-state from cell name.
    pub(crate) const PRODUCTION_SINKS_NOT_WIRED_TAG: &str = "none";

    /// Owns the per-cell state behind the three production sinks plus
    /// the drain task that mirrors `src/bin/nativelink.rs`'s BIS
    /// broadcast loop. Held alive by the W3 / W3f cell so the
    /// `stable_digests` Vec doesn't grow unbounded across iterations and
    /// the spawned drain task is aborted when the cell drops.
    ///
    /// **Bench-scope only.** Production's BIS broadcast does much more
    /// (per-store dispatch, scheduler notification, AC-vs-CAS routing).
    /// The bench equivalent just drains the queue + counts — the cost
    /// production pays at the PUSHER site (the closure invocation that
    /// runs inside the v2 commit path, inside the timed body) is what
    /// we're measuring; the BROADCAST side runs on a separate task
    /// outside the timed body in production AND here.
    pub(crate) struct ProductionSinksState {
        /// Live stable-digests Vec the pusher writes into. Held so the
        /// drain task has a target AND so the unit test can assert
        /// "helper installs a sink that mutates this Vec"; the field
        /// is read in tests only, hence `allow(dead_code)`.
        #[allow(dead_code)]
        pub(crate) stable_digests: Arc<Mutex<Vec<DigestInfo>>>,
        /// Drain task; aborted on cell drop via `JoinHandleDropGuard`.
        #[allow(dead_code)]
        drain_task: nativelink_util::task::JoinHandleDropGuard<()>,
        /// In-flight refcount HashMap; production analogue lives on the
        /// FastSlowStore. Held so the v2 session's RAII guard has
        /// something to mutate. The drain side has no per-iter work
        /// here — entries decrement to zero in `InFlightChunkedGuard::Drop`
        /// at the end of each v2 session.
        #[allow(dead_code)]
        pub(crate) in_flight: ChunkedInFlightMap,
        /// FSS-wide empty-notify; production analogue lives on the
        /// FastSlowStore. Fired by `InFlightChunkedGuard::Drop` when
        /// the map goes empty. No consumer here — `flush_slow_writes`
        /// graceful-drain isn't part of the W3 bench shape.
        #[allow(dead_code)]
        in_flight_empty_notify: Arc<tokio::sync::Notify>,
        /// Failed-writes HashSet; production analogue is the
        /// `failed_slow_writes` on FastSlowStore drained by worker
        /// reconnect. W3 expects every commit to succeed, so this stays
        /// empty in practice; held for shape parity.
        #[allow(dead_code)]
        pub(crate) failed_writes: Arc<Mutex<HashSet<DigestInfo>>>,
        /// #541 fix-up: monotonic count of digests drained by the
        /// background drain task. Bumped by `drained.len()` after each
        /// `core::mem::take`. Tests use this to observe that the pusher
        /// sink fired without racing the drain task (the drain
        /// immediately empties `stable_digests`, so a direct `lock()`
        /// from the test side sees zero even though the sink fired).
        ///
        /// Bench-runtime cost: one `AtomicU64::fetch_add` per drain
        /// batch (not per push) — entirely off the timed body since the
        /// drain task runs on a separate spawn.
        #[allow(dead_code)]
        pub(crate) drain_count: Arc<AtomicU64>,
    }

    /// Build a handler wired with the three production durability sinks
    /// the v2 commit path consumes in production (see
    /// `src/bin/nativelink.rs:912-925`). Returns the handler **and** a
    /// [`ProductionSinksState`] that the cell MUST hold until the timed
    /// body completes (otherwise the drain task aborts mid-iter and the
    /// stable-digests Vec grows unbounded).
    ///
    /// **#541 fidelity intent.** Pre-#541 the W3 / W3f cells called
    /// `new_with_state_and_chunk_size_for_test` and SKIPPED the three
    /// sinks, so the bench numbers UNDERSTATED the per-iter cost
    /// production pays:
    ///
    /// - **`with_v2_stable_digests_sink`** — fires once per commit
    ///   success; one `parking_lot::Mutex` lock + `Vec::push` +
    ///   `Notify::notify_one`. Production wiring at
    ///   `nativelink-store/src/fast_slow_store.rs:1417-1429`.
    /// - **`with_v2_failed_commit_sink`** — fires once per commit
    ///   failure; one `Mutex` lock + `HashSet::insert` (production
    ///   also calls `fast_store.pin_digests(&[digest])`, but the bench
    ///   has no fast store and W3 expects success so this path is
    ///   never hit). Production wiring at `fast_slow_store.rs:1460-1470`.
    /// - **`with_chunked_in_flight_digests`** — installs an RAII guard
    ///   that does one `Mutex` lock + `HashMap` insert at admission +
    ///   one lock + `HashMap` remove at commit (success or failure) +
    ///   one `Notify::notify_waiters` on refcount-zero. Production
    ///   wiring at `fast_slow_store.rs:1367-1369, 1394-1396`.
    ///
    /// The drain task mirrors the BIS broadcast loop's behavior
    /// (`src/bin/nativelink.rs:1008-1075`): wait on the
    /// `stable_notify`, drain the queue, discard. Production does
    /// MUCH more on drain (scheduler broadcast, AC-vs-CAS routing) but
    /// that work is OFF the timed body in production AND here; the
    /// pusher-side cost (which IS on the timed body) is what #541
    /// re-introduces.
    ///
    /// # Fidelity gap (acknowledged, NOT closed)
    ///
    /// The bench faithfully measures the PUSHER-SIDE cost of the three
    /// sinks (mutex lock + Vec push + notify_one; HashSet insert;
    /// HashMap insert+remove + notify_waiters on refcount-zero). Three
    /// production-side dimensions are NOT replicated and may bias the
    /// per-iter measurement vs production under load:
    ///
    /// - **`record_pusher_invoke` omitted.** Production's
    ///   `stable_digests_sink` also calls
    ///   `server_phase0_metrics().record_pusher_invoke(digest)` (a
    ///   histogram observation under a registry RwLock). The bench
    ///   skips this because there is no production metrics registry
    ///   wired in-process. Under heavy push rate the histogram-write
    ///   cost is not free; the bench may UNDER-state production push
    ///   cost by ~1-2 µs per push in tight bursts.
    /// - **`pin_digests` omitted in the failed-commit path.** Production
    ///   also calls `fast_store.pin_digests(&[digest])` on commit
    ///   failure so the in-memory replica survives until the worker
    ///   retries. The bench has no fast store and W3 expects every
    ///   commit to succeed, so this path is structurally unreachable
    ///   here; future W3 variants exercising commit failure would need
    ///   a bench fast-store + pin wiring.
    /// - **Drain-side lock-hold time understated.** The bench's drain
    ///   task does `core::mem::take(&mut *drain_stable_digests.lock())`
    ///   and discards — total lock hold ≈ tens-of-nanoseconds. Production's
    ///   drain holds the mutex through scheduler broadcast + AC pin
    ///   sweep + per-store unpin, lock hold ≈ ms-scale under load. So
    ///   the bench measures the pusher's UNCONTENDED-lock cost; under
    ///   production load the pusher pays contended-lock cost too. The
    ///   true production pusher tail under load is HIGHER than this
    ///   bench reports — by an amount proportional to the production
    ///   drain's lock-hold envelope. (Followup: simulate
    ///   production drain-side mutex-hold-time in bench, calibrated
    ///   against production timings — tracked as a #541 followup, to
    ///   be filed if perf needs the closer envelope.)
    ///
    /// These gaps are acknowledged here so future reviewers cannot
    /// silently treat the bench numbers as a complete production-cost
    /// substitute; the bench is necessary-but-not-sufficient evidence
    /// for the sinks' per-iter cost.
    pub(crate) fn make_handler_with_production_sinks(
        store: Arc<FilesystemStore<FileEntryImpl>>,
    ) -> (Arc<ChunkedWriteHandler>, ProductionSinksState) {
        let in_flight_inner = ChunkedWriteInFlight::new();
        let budget: &'static ChunkBudget = make_chunk_budget();

        // The three production sinks' backing state. Shape mirrors the
        // FastSlowStore fields at fast_slow_store.rs:1174-1184 byte-for-
        // byte (same Arc / Mutex / collection types) so the bench pays
        // production-shaped lock contention.
        let stable_digests: Arc<Mutex<Vec<DigestInfo>>> =
            Arc::new(Mutex::new(Vec::new()));
        let stable_notify = Arc::new(tokio::sync::Notify::new());
        let failed_writes: Arc<Mutex<HashSet<DigestInfo>>> =
            Arc::new(Mutex::new(HashSet::new()));
        let chunked_in_flight: ChunkedInFlightMap =
            Arc::new(Mutex::new(HashMap::new()));
        let in_flight_empty_notify = Arc::new(tokio::sync::Notify::new());

        // Closures mirror fast_slow_store.rs:1417-1429 (stable pusher)
        // and :1460-1470 (failed inserter). The pusher's
        // `server_phase0_metrics().record_pusher_invoke(digest)` call
        // is intentionally OMITTED: the bench runs in-process with no
        // production metrics registry wired and the histogram code path
        // is exercised at the production binary level, not the
        // store-helper level.
        let pusher_stable_digests = stable_digests.clone();
        let pusher_stable_notify = stable_notify.clone();
        let stable_digests_sink: Arc<dyn Fn(DigestInfo) + Send + Sync> =
            Arc::new(move |digest: DigestInfo| {
                pusher_stable_digests.lock().push(digest);
                pusher_stable_notify.notify_one();
            });
        let inserter_failed_writes = failed_writes.clone();
        let failed_commit_sink: Arc<dyn Fn(DigestInfo) + Send + Sync> =
            Arc::new(move |digest: DigestInfo| {
                inserter_failed_writes.lock().insert(digest);
                // Production also calls `fast_store.pin_digests(&[digest])`
                // here so the in-memory replica survives until the
                // worker reconnects and retries. The bench has no fast
                // store and W3 expects every commit to succeed, so this
                // path is structurally unreachable; we'd add the call
                // back if a future W3 variant exercises commit failure.
            });

        // Drain task: mirrors the BIS broadcast loop's wake-and-drain
        // shape (src/bin/nativelink.rs:1008-1075) without the broadcast
        // dispatch. Bounds the stable_digests Vec at "drained-after-
        // each-batch", same liveness contract as production. The
        // `JoinHandleDropGuard` aborts the task when the cell's
        // `ProductionSinksState` drops at end-of-cell.
        //
        // #541 fix-up: bumps `drain_count` by `drained.len()` after
        // each take so tests can observe that the pusher sink fired
        // without racing the drain task (a direct `lock()` from the
        // test side sees zero — the drain task wakes on the same
        // `notify_one` and empties the Vec before the test can read it).
        let drain_stable_digests = stable_digests.clone();
        let drain_stable_notify = stable_notify.clone();
        let drain_count: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
        let drain_count_for_task = drain_count.clone();
        let drain_task = nativelink_util::spawn!(
            "w3-bench-stable-digests-drain",
            async move {
                loop {
                    drain_stable_notify.notified().await;
                    // Drain + discard. Production fans this out to
                    // worker schedulers + AC-pin registries; the bench
                    // just needs the queue to not grow unbounded.
                    let drained: Vec<DigestInfo> =
                        core::mem::take(&mut *drain_stable_digests.lock());
                    // Bump the test-observable drain counter; the count
                    // is the only side-effect surface tests can use to
                    // assert the sink fired (the Vec itself is empty
                    // again by the time control returns to the test).
                    drain_count_for_task
                        .fetch_add(drained.len() as u64, Ordering::Relaxed);
                }
            }
        );

        let handler = Arc::new(
            ChunkedWriteHandler::new_with_state_and_chunk_size_for_test(
                store,
                in_flight_inner,
                budget,
                BENCH_CHUNK_SIZE,
            )
            .with_v2_stable_digests_sink(stable_digests_sink)
            .with_v2_failed_commit_sink(failed_commit_sink)
            .with_chunked_in_flight_digests(
                chunked_in_flight.clone(),
                in_flight_empty_notify.clone(),
            ),
        );

        let state = ProductionSinksState {
            stable_digests,
            drain_task,
            in_flight: chunked_in_flight,
            in_flight_empty_notify,
            failed_writes,
            drain_count,
        };
        (handler, state)
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
    /// 24.83% of CPU on the W3 16 MiB cell — but see the
    /// `make_payload` doc-comment for #533 D9 clarification: that 24.83%
    /// figure UNDER-credited the dominant cost, which was the per-chunk
    /// BLAKE3 hash computed here in `make_chunk`'s `chunk_hash` call
    /// (transitively hoisted by pre-building the chunk vec)).
    pub(crate) fn make_chunk(
        digest: DigestInfo,
        offset: u64,
        payload: &Bytes,
        range: core::ops::Range<usize>,
        finish: bool,
    ) -> WriteChunk {
        // `Bytes::slice` consumes the `Range<usize>` via `impl
        // RangeBounds<usize>`; no `.clone()` needed (Range is two
        // `usize`s, but the prior `.clone()` was visually noisy).
        let chunk_bytes = payload.slice(range);
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
    pub(crate) fn build_chunks(digest: DigestInfo, payload: &Bytes) -> Vec<WriteChunk> {
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

    pub(crate) async fn drain_v2_response(
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
    ///
    /// # CLARIFICATION (#533 fix-up D9): true sustained improvement is
    /// # ~10-15%, NOT 77%
    ///
    /// The original `a691efc9` commit message and the rolled-up reporting
    /// for this harness rework cited a "77% p99 drop (683 → 157 ms)" on
    /// the W3 16 MiB c=1 cell from hoisting `make_payload` + `build_chunks`
    /// out of the timed window. **That number was the LOW end of a
    /// single-sample noise floor, not a sustained win.** Subsequent
    /// re-runs on the same diff family showed p99 = 620 ms — right back
    /// inside the historic envelope.
    ///
    /// The iters=20 single-sample p99 envelope observed across five
    /// independent baselines on this exact cell (16 MiB c=1):
    ///
    /// | baseline SHA | p99 (ms) | notes                                  |
    /// |--------------|---------:|----------------------------------------|
    /// | `15b85ed7`   |      743 | checked-in baseline                    |
    /// | `e947b757`   |      634 | checked-in baseline                    |
    /// | `834c0334`   |      683 | pre-fix-of-any-kind                    |
    /// | `50e26446`   |      157 | post-fix, lucky single-sample low end  |
    /// | `f47057aa`   |      620 | post-fix + fixups, typical             |
    ///
    /// Envelope: **113–743 ms** across five SHAs. The 157 ms reading was
    /// inside the noise floor, not a new typical value. True sustained
    /// improvement is **~10-15% at p99 (683 → ~620 ms)**, not 77%.
    ///
    /// **Attribution of the (smaller) true win** — what actually moved
    /// out of the timed body when the prebuilt pool was hoisted:
    ///
    /// - `make_payload`'s LCG fill (the named offender in the prior
    ///   24.83%-of-CPU profile): **~40% of the win** (removes the
    ///   per-iter 16 MiB byte-loop CPU and the dirty-page mmap pressure).
    /// - `build_chunks`'s per-chunk BLAKE3 hashing via `make_chunk`'s
    ///   `chunk_hash` call (transitively hoisted because hashing only
    ///   happens once per pool-build, not once per iter): **~45% of the
    ///   win — the dominant cost.** The "24.83% of CPU on `make_payload`"
    ///   profile under-credited this because the CPU% attribution was
    ///   sampled at the LCG-fill frame while the BLAKE3 cost showed up
    ///   under a sibling `chunk_hash` frame.
    /// - `Bytes::slice` becoming zero-copy (the refcount change): **~5%**
    ///   (eliminates one memcpy per chunk per iter at 16 MiB / 1 MiB).
    /// - JoinSet-bypass fast path at c=1 (`run_w3_iter` branch): **<1%**
    ///   (no path change at c=1 vs the prior shape; c>1 path is new).
    /// - `Arc<prebuilt>` cloning across iters: **0%** (the refcount work
    ///   replaces work that used to be done elsewhere; net zero).
    ///
    /// # Recommendation
    ///
    /// Bump `iters_override` on the 16 MiB c=1 cell to **at least 50** to
    /// stabilize p99 measurements at this scale. iters=20 produces a
    /// single tail-task sample for p99 (where p99 == max in the data),
    /// dominated by GC pauses, allocator fragmentation, and OS scheduling
    /// hiccups on the single tail iter; ~50 samples gives the p99 enough
    /// of a tail population to be a stable signal rather than a coin flip
    /// inside a 6× envelope.
    ///
    /// # Page-fault pre-pay
    ///
    /// The `data.push(...)` loop writes every byte before `Bytes::from`
    /// hands ownership to the caller's `chunk_bytes`. That means every
    /// page in the returned allocation is **dirty + resident** when the
    /// timer starts — no latent page-fault cost is paid inside the timed
    /// body. Future reviewers asking "does the bench warm-touch the pool
    /// pages before timing?" can stop here: yes, implicitly via the LCG
    /// fill. Removing the fill (e.g. switching to `Vec::with_capacity` +
    /// `set_len` + uninit access) would re-introduce per-iter page-fault
    /// jitter into the timed window (perf-optimizer #533 MINOR-3).
    pub(crate) fn make_payload(size: usize, n: u64) -> Bytes {
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
        // 16 MiB c=1 p99 sits in a 113-743 ms single-sample envelope at
        // iters=20 (see make_payload doc-block). 50 iters narrows the
        // p99/max-of-N estimator enough that future runs land near the
        // typical ~620 ms rather than the lucky-sample lows. 50 × 16 MiB
        // = 800 MiB pool, well under POOL_MAX_BYTES.
        W3Cell { size: 16 * BENCH_CHUNK_SIZE, concurrency: 1, label: "16MiB", iters_override: Some(50) },
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
        let raw_iters = opts.effective_iters(cell.iters_override.unwrap_or(20));
        let size = cell.size;
        let concurrency = cell.concurrency;
        // Runtime pool-memory guard (#533 D5). `--iters N` accepts any
        // u32 >= 1; without this clamp a user could blow `prebuilt`
        // past POOL_MAX_BYTES (e.g. `--iters 100` on the 4 MiB c=64
        // cell wants 25.6 GiB). Compute the largest `iters` the pool
        // can hold and clamp.
        //
        // `concurrency * size` is the per-batch byte cost (one slot per
        // (iter, concurrent) tuple, each holding a `size`-byte payload
        // via Bytes refcount + chunk vec). At default-iters all cells
        // sit well under cap; the clamp fires only on a user override.
        let per_batch_bytes = (size as u64).saturating_mul(concurrency as u64);
        let max_iters_for_pool = if per_batch_bytes == 0 {
            raw_iters as u64
        } else {
            POOL_MAX_BYTES / per_batch_bytes
        };
        let iters = if (raw_iters as u64) > max_iters_for_pool {
            let clamped = max_iters_for_pool.max(1).min(u32::MAX as u64) as u32;
            eprintln!(
                "[bench] W3 {label} c={concurrency}: requested iters={raw_iters} \
                 would allocate {requested_gib:.2} GiB pool (size={size}, \
                 concurrency={concurrency}); clamping to iters={clamped} so \
                 pool stays under POOL_MAX_BYTES = {cap_gib:.2} GiB.",
                label = cell.label,
                requested_gib = (raw_iters as f64 * per_batch_bytes as f64)
                    / (1024.0 * 1024.0 * 1024.0),
                cap_gib = POOL_MAX_BYTES as f64 / (1024.0 * 1024.0 * 1024.0),
            );
            clamped
        } else {
            raw_iters
        };
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
        // #541: wire the three production durability sinks
        // (stable_digests_pusher + failed_writes_inserter +
        // chunked_in_flight_digests) so the v2 commit path's per-iter
        // bookkeeping cost is paid inside the timed body, matching the
        // production composition at src/bin/nativelink.rs:912-925.
        // `_sinks_state` is held to keep the drain task + stable-digests
        // Vec alive for the cell; dropping aborts the drain task.
        let (handler, _sinks_state) = make_handler_with_production_sinks(store);
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
        // was 24.83% of CPU on the W3 16 MiB cell. See the
        // `make_payload` doc-comment for the #533 D9 clarification on
        // true attribution: the per-chunk BLAKE3 hash transitively
        // hoisted by pre-building this `prebuilt` pool was actually
        // the dominant cost (~45% of the win), not the LCG fill
        // (~40%), and the true sustained p99 improvement is
        // ~10-15%, not the originally-reported 77%.
        //
        // Pool-build cost note (code-reviewer #533 m6): this loop runs
        // `make_payload` (LCG fill, byte-loop CPU) PLUS
        // `digest_via_default_hasher` (one BLAKE3 over the whole payload
        // for the per-blob digest) PLUS `build_chunks` (one BLAKE3 over
        // each chunk for the per-chunk `chunk_sha256` wire field). For a
        // 16 MiB c=16 × 20-iter cell that is 320 × 16 = 5120 1-MiB
        // BLAKE3 hashes + 320 16-MiB BLAKE3 hashes for the digests, all
        // single-threaded on this tokio worker BEFORE the timer starts.
        // That is several seconds of cell setup at default iters (tens
        // of seconds under `--iters 100`). Setup wall-clock is counted
        // against the bench's total run time but NOT against the
        // `samples` collected by `measure` — so it does not pollute the
        // p50/p99 numbers, only the wall-clock for the whole bench to
        // complete. Future reviewers seeing the bench wall-clock balloon
        // at high concurrency should look here first, not at the timed
        // body. (Parallelizing this loop via `rayon::scope` /
        // `spawn_blocking` would cut bench wall-clock meaningfully at
        // c=64 — perf-optimizer #533 m2 — tracked as a #535 follow-up.)
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
        // #541: as of this commit W3 wires the three production
        // durability sinks the v2 commit path consumes
        // (`with_v2_stable_digests_sink`, `with_v2_failed_commit_sink`,
        // `with_chunked_in_flight_digests`). Pre-#541 baselines did NOT
        // wire these and so understated the per-iter cost — diff tooling
        // joining on `scenario_name` MUST filter by this extras field to
        // avoid attributing the wiring-up cost to a code regression.
        extras.insert(
            "production_sinks_wired".to_string(),
            serde_json::json!(PRODUCTION_SINKS_WIRED_TAG),
        );
        // #537 D3: self-describing JSON. `measures` names what the timed
        // body actually waits for. For W3 / W3f that's the chunked-v2
        // commit-to-disk roundtrip — the client's `drain_v2_response`
        // blocks until the v2 server emits `FinalResponse(committed_size)`,
        // which the server only sends after pwrite + verify + finalize-
        // rename complete on disk. A reader consuming a W3 baseline JSON
        // in isolation (Slack snippet, 6-month post-mortem) MUST be able
        // to derive what the latency cell measured WITHOUT chasing the
        // scenario doc-comment — otherwise the cell becomes a footgun
        // (see red-team #537 6-month pre-mortem).
        extras.insert(
            "measures".to_string(),
            serde_json::json!("chunked_commit_to_disk"),
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
        // `Arc::new(iter_counter)` is asymmetric with R5's plain
        // `AtomicU64`: W3's iter body is `run_w3_iter` (extracted free
        // async fn), so the counter must cross an `.await` boundary AND
        // be `Send` + `'static` for the `set.spawn(...)` JoinSet path at
        // c>1. R5's iter body is an inline `async move` closure that
        // moves `iter_counter` directly. A future cleanup agent should
        // NOT "consistency-fix" this asymmetry — the wrapping is
        // load-bearing for the JoinSet spawn at W3 c>1 (code-reviewer
        // #533 m7).
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
    ///
    /// # c=1 vs c>1 path asymmetry (red-team #533 out-of-scope #2)
    ///
    /// The c=1 branch directly `.await`s `write_one_chunked` — no
    /// `JoinSet`, no `set.spawn`. The c>1 branch dispatches N tasks
    /// via `tokio::task::JoinSet` and drains via `join_next()`. Both
    /// branches measure the same physical work (a chunked-v2 stream),
    /// but the c>1 wall-clock includes the cost of `set.spawn(N tasks)`
    /// + `set.join_next() × N` plumbing — small, but not zero. This
    /// is intentional: the c=1 branch preserves byte-identical
    /// methodology against the historic single-writer baseline (so old
    /// JSONs under `benchmarks/baselines/` remain a continuity anchor),
    /// while the c>1 branch lives in its own scenario-name family
    /// (`..._burst_*`) where the spawn-loop overhead is part of the
    /// honest "closed-loop burst of N writers" workload definition.
    /// **Do not "unify" the two branches by routing c=1 through the
    /// JoinSet path** — that would invalidate every historic c=1
    /// baseline by adding spawn-loop overhead to the measurement.
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
            // Slot math: `n * c + j`. Realistic bounds are iters ≤ ~1000
            // and concurrency ≤ 64, so n*c+j ≤ ~64,000 — wide of u64
            // overflow. Prior `saturating_mul`/`saturating_add` shape
            // would silently truncate on a 32-bit host's `as usize`
            // cast (or saturate to u64::MAX → panic on indexing).
            // `checked_*` + `expect` makes the overflow loud and
            // explicit — bench is on 64-bit hosts where this never
            // fires, but if a future contributor hand-stamps iters or
            // concurrency to absurd values the failure is diagnostic
            // (code-reviewer #533 m3).
            let n_usize = usize::try_from(n)
                .expect("iter counter must fit in usize (bench runs on 64-bit hosts)");
            let concurrency_usize = concurrency as usize;
            let base_slot = n_usize
                .checked_mul(concurrency_usize)
                .expect("slot base n*c must not overflow usize");
            for j in 0..concurrency_usize {
                let slot = base_slot
                    .checked_add(j)
                    .expect("slot index n*c+j must not overflow usize");
                let chunks = prebuilt[slot].clone();
                let client_clone = client.clone();
                set.spawn(async move {
                    write_one_chunked(client_clone, chunks, size).await;
                });
            }
            // Drain the JoinSet. On a panic from any spawned task,
            // shut down the remaining tasks BEFORE propagating the
            // panic — otherwise stragglers continue mutating the
            // per-cell FilesystemStore tempdir after `_td: TempDir`
            // drops at the end of the cell, leaking fds into a
            // deleted directory on Linux until the straggler dies
            // (red-team #533 NIT #6). `set.shutdown()` awaits abort
            // of every still-running task and is idempotent — safe
            // to call on an already-empty set.
            while let Some(res) = set.join_next().await {
                if let Err(join_err) = res {
                    set.shutdown().await;
                    panic!(
                        "W3 concurrent writer task must not panic; \
                         remaining tasks aborted via set.shutdown() to \
                         prevent straggler-into-dropped-tempdir leaks: \
                         {join_err:?}"
                    );
                }
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
            // #541: R5 runs the v2 write path ONCE per iter during
            // prewrite (outside the timed body) and the timed body
            // exercises `FilesystemStore::get_part_unchunked` directly —
            // no v2 commit happens inside the timed body, so the three
            // production sinks would not be invoked even if wired. Tag
            // explicitly so a diff-tool reader doesn't infer sink-state
            // from cell name. If a Phase-2 R5 variant ever routes
            // readers through the `WriteChunkedV2` commit barrier, this
            // tag MUST flip to `PRODUCTION_SINKS_WIRED_TAG` and the
            // handler builder switched to
            // `make_handler_with_production_sinks`.
            extras.insert(
                "production_sinks_wired".to_string(),
                serde_json::json!(PRODUCTION_SINKS_NOT_WIRED_TAG),
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
                // Zero-copy proof: the chunk's first AND last byte
                // addresses must lie WITHIN the source payload's
                // allocation range. Head-only checks would pass under a
                // pathological implementation that aliased only the
                // chunk head to the source; the tail check rules that
                // out. Plus a byte-equality check against the canonical
                // slice defends against an allocator-arena coincidence
                // where the fresh copy happens to land at an in-range
                // address but contains different bytes (testing-czar
                // MINOR-2).
                let src_start = payload.as_ptr() as usize;
                let src_end = src_start + payload.len();
                let chunk_start = c.chunk_bytes.as_ptr() as usize;
                let chunk_end = chunk_start + c.chunk_bytes.len();
                assert!(
                    chunk_start >= src_start && chunk_start < src_end,
                    "build_chunks_zero_copy_violation: chunk {i} chunk_bytes \
                     pointer 0x{chunk_start:x} not within payload allocation \
                     [0x{src_start:x}, 0x{src_end:x}). A `copy_from_slice` \
                     regression would produce a fresh allocation with \
                     unrelated pointer — the per-iter 16 MiB memcpy cost \
                     would be back (#533 retro)."
                );
                assert!(
                    chunk_end <= src_end,
                    "build_chunks_zero_copy_violation: chunk {i} extends \
                     past payload allocation; chunk end 0x{chunk_end:x} > \
                     src end 0x{src_end:x}. A partial-aliasing regression \
                     would pass the head check but not the tail check."
                );
                let canonical = &payload[i * BENCH_CHUNK_SIZE
                    ..i * BENCH_CHUNK_SIZE + c.chunk_bytes.len()];
                assert_eq!(
                    &c.chunk_bytes[..], canonical,
                    "build_chunks_zero_copy_violation: chunk {i} bytes do not \
                     match the canonical payload slice. An allocator-arena \
                     coincidence (fresh copy reusing the freed source's \
                     address) would defeat the pointer check; this byte \
                     equality defends against that."
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

        /// `make_payload` MUST produce distinct content per `n` so the
        /// concurrent-slot pool covers `iters × concurrency` UNIQUE
        /// digests. If a future refactor drops the
        /// `n.wrapping_mul(0x9E37_79B9_7F4A_7C15)` LCG seed mixing — or
        /// a copy-paste accident overwrites it with a constant — every
        /// slot would produce identical bytes, identical digests, and
        /// the chunked-driver's per-digest singleflight would collapse
        /// concurrent writes into one. The W3 burst cells would then
        /// silently measure the dedup'd path instead of throughput.
        ///
        /// Same seed must be deterministic so prebuilt/replay invariants
        /// hold (re-running the bench on the same SHA produces the same
        /// digests).
        ///
        /// Mutation: replace the LCG seed `n.wrapping_mul(0x9E37...)`
        /// with a constant (`let mut state: u64 = 0;`) — both `assert_ne!`
        /// arms red-fail with the bespoke `#533 payload-uniqueness`
        /// message.
        #[test]
        fn make_payload_produces_unique_content_per_n() {
            // Use a small payload size so the test stays fast — the
            // uniqueness property is independent of payload length.
            let size = BENCH_CHUNK_SIZE;
            let p0 = make_payload(size, 0);
            let p1 = make_payload(size, 1);
            let p_big = make_payload(size, 0xDEAD_BEEF);
            assert_ne!(
                p0, p1,
                "#533 payload-uniqueness: make_payload(_, 0) == make_payload(_, 1); \
                 concurrent slots would collide on digest and the W3 burst cells \
                 would silently measure the dedup/singleflight path instead of \
                 throughput."
            );
            assert_ne!(
                p1, p_big,
                "#533 payload-uniqueness: make_payload(_, 1) == \
                 make_payload(_, 0xDEAD_BEEF); seed mixing is degenerate."
            );
            // Determinism: same n must produce the same bytes (load-bearing
            // for repeatable digests across bench runs).
            assert_eq!(
                make_payload(size, 1),
                make_payload(size, 1),
                "#533 payload-uniqueness: make_payload is non-deterministic for \
                 fixed n; repeat bench runs on the same SHA would produce \
                 different digests."
            );
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

        /// Pool memory bound — matrix shape at default iters.
        ///
        /// Every cell's `prebuilt` pool must stay under POOL_MAX_BYTES
        /// at the default iter budget (effective_iters(20) when CLI
        /// doesn't override). Mutation: add `(16 * BENCH_CHUNK_SIZE, 64,
        /// "16MiB", None)` to W3_CELLS — that cell's 20.48 GiB pool
        /// red-fails.
        #[test]
        fn w3_cell_matrix_pool_memory_bounded_at_default_iters() {
            let default_iters = 20u64;
            for c in W3_CELLS {
                let pool_bytes =
                    default_iters * c.concurrency as u64 * c.size as u64;
                assert!(
                    pool_bytes <= POOL_MAX_BYTES,
                    "W3 cell {} c={} pool would need {:.2} GiB (size={}, \
                     iters={}, concurrency={}); POOL_MAX_BYTES = {:.2} GiB. \
                     Either reduce concurrency for this size or add an \
                     iters_override to keep the pool bounded.",
                    c.label,
                    c.concurrency,
                    pool_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                    c.size,
                    default_iters,
                    c.concurrency,
                    POOL_MAX_BYTES as f64 / (1024.0 * 1024.0 * 1024.0),
                );
            }
        }

        /// Pool memory bound — runtime clamp under user `--iters` override.
        ///
        /// The 3-reviewer convergence (code-reviewer M1, testing-czar
        /// MAJOR-2, red-team blind-spot #2) was that the prior test
        /// only checked the matrix at default iters; CLI `--iters 100`
        /// blew straight through. The runtime guard in `run_w3_cell`
        /// clamps `iters = min(effective_iters, POOL_MAX_BYTES /
        /// (size × concurrency))`, so even at the
        /// `W3_MAX_ITERS_CEILING` stress value the pool stays under
        /// cap. This test simulates the same clamp formula and asserts
        /// the resulting pool fits.
        ///
        /// Mutation: comment out the `let iters = if (raw_iters as u64)
        /// > max_iters_for_pool { ... }` clamp in `run_w3_cell` — the
        /// formula here still passes (it's a SIMULATION of the clamp,
        /// not the clamp itself). The matched mutation for THIS test is
        /// to change `POOL_MAX_BYTES / per_batch_bytes` to
        /// `raw_iters as u64` (no clamp), which would let the assertion
        /// below evaluate to a runaway pool size — red-fails because
        /// 200 × 64 × 4 MiB = 51.2 GiB > 6 GiB cap.
        ///
        /// #541: the production-sinks-wired tag string MUST stay stable.
        /// Diff tooling joins on `scenario_name` + filters on the
        /// `production_sinks_wired` extras key; a drift of either the
        /// key NAME or the VALUE silently splits pre-#541 from
        /// post-#541 baselines and the diff tool either compares apples
        /// to oranges or drops cells. The two-pronged assertion (key
        /// presence + exact string value) makes both directions of the
        /// failure caught by one mutation.
        ///
        /// Mutation: rename `PRODUCTION_SINKS_WIRED_TAG` (or change its
        /// value) — this test red-fails with the bespoke
        /// `#541 production-sinks-wired tag drift` message.
        #[test]
        fn production_sinks_wired_tag_is_stable() {
            assert_eq!(
                PRODUCTION_SINKS_WIRED_TAG,
                "stable_digests_pusher + failed_writes_inserter + chunked_in_flight_digests",
                "#541 production-sinks-wired tag drift: the W3 / W3f extras key \
                 `production_sinks_wired` carries this string verbatim; checked-in \
                 baselines and the diff tool's filter logic both pin it. Any \
                 rename of the three sink-method names in this string requires \
                 a baseline-replay sweep so historic JSONs don't fail to join."
            );
            assert_eq!(
                PRODUCTION_SINKS_NOT_WIRED_TAG, "none",
                "#541 production-sinks-not-wired tag drift: R5 emits this \
                 string to mark its bare-handler shape; the diff tool filters \
                 on it to exclude R5 from any pre-vs-post #541 comparison",
            );
        }

        /// #541 fix-up: the sinks-wired helper MUST install all three
        /// sinks AND those sinks MUST actually fire from the v2 commit
        /// path. A future refactor that drops any single `.with_*` call
        /// would silently understate the per-iter cost and re-introduce
        /// the #541 fidelity gap.
        ///
        /// Two-tier coverage:
        ///
        /// 1. **Structural** — assert the handler reports all three
        ///    sinks wired via the `is_*_wired` test-only accessors. A
        ///    `.with_*` call that silently no-ops (e.g. setter
        ///    overwritten by a subsequent `None` assignment) would
        ///    red-fail here.
        /// 2. **Behavioral** — drive ONE real chunked-v2 write through
        ///    the in-process v2 server (the same harness W3 uses), then
        ///    observe the visible side effects:
        ///      - `drain_count` increments from 0 (the pusher fired and
        ///        the drain task drained the Vec — using `drain_count`
        ///        rather than reading `stable_digests` directly avoids
        ///        the test racing the drain task, which empties the Vec
        ///        as soon as the pusher's `notify_one` wakes it).
        ///      - `in_flight` map is EMPTY at commit completion (the
        ///        RAII `InFlightChunkedGuard` fired on session-drop and
        ///        removed the entry; if it didn't, future
        ///        `flush_slow_writes` waiters would wedge in production).
        ///
        /// Whole test runs under `tokio::time::timeout(10s)` as a
        /// deadlock detector — `tokio::time::Elapsed` would surface as
        /// a bespoke "deadlock — v2 commit barrier did not complete"
        /// message, not a generic `is_err()`.
        ///
        /// **Mutation falsifier (must red-fail for the right reason):**
        /// comment out `.with_v2_stable_digests_sink(stable_digests_sink)`
        /// in `make_handler_with_production_sinks`. The structural
        /// assertion (`is_v2_stable_digests_sink_wired()`) red-fails
        /// with "#541 v2-commit-barrier sink wiring violated:
        /// stable_digests_sink reported NOT wired". The behavioral
        /// assertion (`drain_count > 0`) also red-fails because the v2
        /// commit path skips the sink invocation entirely, the drain
        /// task is never notified, and the counter stays at 0.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn make_handler_with_production_sinks_installs_all_three() {
            // ---- Build store + handler via the helper under test ----
            let temp_dir = tempfile::TempDir::new()
                .expect("test tempdir creation must succeed");
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
            .expect("test FilesystemStore build must succeed");

            let (handler, state) =
                make_handler_with_production_sinks(store);

            // ---- (1) Structural wiring assertions ----
            //
            // Catches the silent-no-op shape where a `.with_*` setter is
            // overwritten or commented out — the v2 commit path would
            // never invoke the sink, the behavioral assertion below
            // would also red-fail, but this structural check fires
            // FIRST so the failure attribution is unambiguous (test
            // failure says "wiring violated", not "drain count was 0").
            assert!(
                handler.is_v2_stable_digests_sink_wired(),
                "#541 v2-commit-barrier sink wiring violated: \
                 stable_digests_sink reported NOT wired by \
                 make_handler_with_production_sinks. A `.with_v2_stable_digests_sink(...)` \
                 call was dropped or no-op'd — the v2 commit path will \
                 skip BIS notification and worker mirror_blobs will \
                 accumulate in production."
            );
            assert!(
                handler.is_v2_failed_commit_sink_wired(),
                "#541 v2-commit-barrier sink wiring violated: \
                 failed_commit_sink reported NOT wired by \
                 make_handler_with_production_sinks. A \
                 `.with_v2_failed_commit_sink(...)` call was dropped — \
                 v2 commit failures will not surface to the worker \
                 reconnect-retry path in production."
            );
            assert!(
                handler.is_chunked_in_flight_digests_wired(),
                "#541 v2-commit-barrier sink wiring violated: \
                 chunked_in_flight_digests reported NOT wired by \
                 make_handler_with_production_sinks. A \
                 `.with_chunked_in_flight_digests(...)` call was \
                 dropped — `FastSlowStore::has_with_results` will \
                 silently return None for in-flight v2 writes in \
                 production, breaking the chunked-aware reader-cascade \
                 contract."
            );
            // Sanity: the in-flight map starts empty before any commit.
            assert!(
                state.in_flight.lock().is_empty(),
                "#541 chunked_in_flight map must start empty"
            );
            assert_eq!(
                state.drain_count.load(Ordering::Relaxed),
                0,
                "drain_count must start at 0 before the test drives a commit"
            );

            // ---- (2) Behavioral: drive ONE real v2 commit ----
            //
            // Spin up the same in-process v2 server W3 uses, write a
            // single small payload, drain to FinalResponse. After the
            // commit completes the wired sinks MUST have fired:
            //   - stable_digests_sink → drain task wakes and increments
            //     `drain_count` by at least 1.
            //   - chunked_in_flight RAII guard → entry removed on
            //     session drop, map back to empty.
            //
            // Whole sequence wrapped in tokio::time::timeout as a
            // deadlock detector; an Elapsed here means the v2 commit
            // barrier wedged (e.g. a sink hold a lock across .await,
            // or the drain task starved).
            tokio::time::timeout(Duration::from_secs(10), async {
                let (mut client, _server_guard) =
                    start_v2_server(handler.clone()).await;

                // Tiny single-chunk payload — minimum surface for the
                // commit path (admission + one chunk + finalize) so the
                // test stays fast.
                let payload = make_payload(64 * 1024, 0xC0FFEE);
                let digest = digest_via_default_hasher(&payload);
                let chunks = build_chunks(digest, &payload);
                let expected_size = payload.len() as u64;

                let stream = tokio_stream::iter(chunks);
                let response = client
                    .write_chunked_v2(stream)
                    .await
                    .expect("v2 write_chunked_v2 RPC must return Ok");
                let committed = drain_v2_response(response.into_inner())
                    .await
                    .expect("v2 commit must succeed within the test budget");
                assert_eq!(
                    committed, expected_size,
                    "v2 commit bytes mismatch"
                );
            })
            .await
            .expect(
                "#541 v2-commit-barrier sink wiring violated: \
                 v2 commit did not complete within 10s — either the \
                 RAII in-flight guard wedged, a sink held a lock \
                 across .await, or the drain task starved",
            );

            // ---- Observe sink side effects ----
            //
            // `drain_count` is a polling target because the drain task
            // races with the test's resumption after `await`. Bounded
            // polling loop (up to 5 s of wall-clock budget, 5 ms
            // ticks) — the drain task wakes via `notify_one` from the
            // pusher inside the commit path; on a quiet single-thread
            // bench host the wake-and-drain typically completes in
            // <1 ms.
            let drained_observed = tokio::time::timeout(
                Duration::from_secs(5),
                async {
                    loop {
                        let n = state.drain_count.load(Ordering::Relaxed);
                        if n > 0 {
                            return n;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                },
            )
            .await
            .expect(
                "#541 v2-commit-barrier sink wiring violated: \
                 drain_count stayed at 0 for 5 s after commit — the \
                 stable_digests_sink did not fire on commit success. \
                 The v2 commit path skipped the pusher invocation; \
                 production BIS notification would also be skipped \
                 and worker mirror_blobs would accumulate.",
            );
            assert!(
                drained_observed >= 1,
                "drain_count must be >= 1 after one v2 commit (got {drained_observed})"
            );

            // RAII in-flight guard must have fired on session drop.
            assert!(
                state.in_flight.lock().is_empty(),
                "#541 v2-commit-barrier sink wiring violated: \
                 chunked_in_flight map NOT empty after commit \
                 (entries: {entries}). The InFlightChunkedGuard's Drop \
                 did not run, or the v2 session held the entry beyond \
                 commit. In production `FastSlowStore::has_with_results` \
                 would keep reporting in-flight after the commit, \
                 wedging readers that wait on the FSS empty-notify.",
                entries = state.in_flight.lock().len()
            );

            // Tear down the helper state — aborts the drain task
            // (JoinHandleDropGuard) so subsequent tests inherit a
            // clean tokio task tree.
            drop(state);
        }

        /// #541 fix-up — OVER-action pin: the bare `make_handler`
        /// (R5's code path) MUST NOT wire any of the three production
        /// durability sinks. The R5 cell pre-writes blobs OUTSIDE the
        /// timed body and then measures multi-reader fan-out; wiring
        /// the sinks would silently add per-iter cost the R5 timing is
        /// NOT supposed to include, breaking the apples-to-apples
        /// comparison against historic R5 baselines (which were
        /// collected pre-#541 with NO sinks).
        ///
        /// Per CLAUDE.md "Asymmetric contract coverage": the
        /// `make_handler_with_production_sinks` under-action test
        /// (above) covers the wired direction; this test covers the
        /// NOT-wired direction. A future refactor that "harmonizes"
        /// the two helpers by routing R5 through
        /// `make_handler_with_production_sinks` would silently move
        /// R5's measurement off the historic baseline.
        ///
        /// **Mutation falsifier:** swap the `make_handler(store)` call
        /// below for `make_handler_with_production_sinks(store).0` —
        /// the three `assert!(!handler.is_*_wired())` checks red-fail
        /// with the bespoke "#541 R5 bare-handler over-action
        /// violated" message.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn r5_production_sinks_wired_is_none() {
            let temp_dir = tempfile::TempDir::new()
                .expect("test tempdir creation must succeed");
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
            .expect("test FilesystemStore build must succeed");

            // The R5 code path: bare handler, no production sinks.
            let handler = make_handler(store);

            assert!(
                !handler.is_v2_stable_digests_sink_wired(),
                "#541 R5 bare-handler over-action violated: \
                 stable_digests_sink reported WIRED on the bare \
                 make_handler output. R5's timed body would silently \
                 pay per-iter pusher cost (mutex+notify on every v2 \
                 commit) the R5 baseline did NOT pay; historic R5 \
                 numbers become incomparable to post-change numbers."
            );
            assert!(
                !handler.is_v2_failed_commit_sink_wired(),
                "#541 R5 bare-handler over-action violated: \
                 failed_commit_sink reported WIRED on the bare \
                 make_handler output. R5 has no failed-commit path in \
                 the timed body, but the wiring would still bloat the \
                 handler's per-iter footprint vs the historic baseline."
            );
            assert!(
                !handler.is_chunked_in_flight_digests_wired(),
                "#541 R5 bare-handler over-action violated: \
                 chunked_in_flight_digests reported WIRED on the bare \
                 make_handler output. R5's prewrite would insert + \
                 remove into the map, adding HashMap-lock contention \
                 the historic R5 baseline did NOT pay; the
                 multi-reader fan-out timing becomes a measurement of \
                 lock contention rather than read throughput."
            );
        }

        /// Acknowledged limitation: the simulation arm above is a
        /// declarative invariant on the clamp formula. The runtime
        /// guard's behavior (clamp + log) is exercised end-to-end only
        /// by the bench binary; a future integration test would close
        /// the gap — tracked as a #533 followup.
        #[test]
        fn w3_cell_matrix_pool_memory_bounded_at_max_iters_ceiling() {
            for c in W3_CELLS {
                let per_batch_bytes = c.size as u64 * c.concurrency as u64;
                // Mirror the clamp formula in run_w3_cell.
                let max_iters_for_pool = if per_batch_bytes == 0 {
                    W3_MAX_ITERS_CEILING
                } else {
                    POOL_MAX_BYTES / per_batch_bytes
                };
                let effective = W3_MAX_ITERS_CEILING.min(max_iters_for_pool);
                let pool_bytes = effective.saturating_mul(per_batch_bytes);
                assert!(
                    pool_bytes <= POOL_MAX_BYTES,
                    "W3 cell {} c={} at requested iters={} clamps to \
                     iters={} but pool {:.2} GiB still exceeds POOL_MAX_BYTES \
                     ({:.2} GiB). Clamp formula in run_w3_cell is wrong, OR \
                     per_batch_bytes overflowed — investigate before allowing \
                     this cell to scale.",
                    c.label,
                    c.concurrency,
                    W3_MAX_ITERS_CEILING,
                    effective,
                    pool_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                    POOL_MAX_BYTES as f64 / (1024.0 * 1024.0 * 1024.0),
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
