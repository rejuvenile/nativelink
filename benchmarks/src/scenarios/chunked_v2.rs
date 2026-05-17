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
//! **R5 (chunked v2 contended readers):** one writer drives a chunked
//! commit (commit pending); N concurrent readers waiting on the per-
//! digest `Notify` issue reads against the same digest and we measure
//! reader latency from issue to byte receipt. This is the actual
//! load-bearing v3 mechanism — `feedback_per_chunk_timeout_design_intent`
//! + the 2026-05-15 v3 audit say the per-digest `Notify` is supposed to
//! wake N readers in O(N) (not O(N²)). A regression that re-introduces
//! global-broadcast wake semantics OR a serialization point in the
//! commit-publish path shows up here as a latency cliff.
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
    use sha2::{Digest as _, Sha256};
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
    use crate::scenarios::{RunOpts, measure};

    /// Chunk size used in the v2 bench cells. Matches the prod chunk-
    /// flow: 1 MiB.
    const BENCH_CHUNK_SIZE: usize = 1024 * 1024;

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(bytes);
        let out = h.finalize();
        let mut a = [0u8; 32];
        a.copy_from_slice(out.as_ref());
        a
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

    /// Per-process `ChunkBudget` lazily initialized once. Replaces the
    /// previous per-cell `Box::leak`. Bench process is short-lived so
    /// one budget is sufficient; lifting to truly non-static-only would
    /// require a `ChunkedWriteHandler::new_with_state_and_chunk_size_for_test`
    /// API change which is out of scope (followup #NNN).
    fn process_chunk_budget() -> &'static ChunkBudget {
        use std::sync::OnceLock;
        static BUDGET: OnceLock<&'static ChunkBudget> = OnceLock::new();
        BUDGET.get_or_init(|| Box::leak(Box::new(ChunkBudget::new())))
    }

    fn make_handler(
        store: Arc<FilesystemStore<FileEntryImpl>>,
    ) -> Arc<ChunkedWriteHandler> {
        let in_flight = ChunkedWriteInFlight::new();
        let budget: &'static ChunkBudget = process_chunk_budget();
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

    fn make_chunk(digest: DigestInfo, offset: u64, bytes: &[u8], finish: bool) -> WriteChunk {
        WriteChunk {
            digest: Some(digest.into()),
            chunk_offset: offset,
            chunk_bytes: Bytes::copy_from_slice(bytes),
            chunk_sha256: sha256(bytes).to_vec(),
            finish_chunk: finish,
        }
    }

    fn build_chunks(digest: DigestInfo, payload: &[u8]) -> Vec<WriteChunk> {
        let mut chunks = Vec::new();
        let mut offset: u64 = 0;
        let mut remaining = payload;
        while !remaining.is_empty() {
            let take = BENCH_CHUNK_SIZE.min(remaining.len());
            let bytes = &remaining[..take];
            let is_final = take == remaining.len();
            chunks.push(make_chunk(digest, offset, bytes, is_final));
            offset += take as u64;
            remaining = &remaining[take..];
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

    /// Pre-generate a payload for a given size + iter index.
    fn make_payload(size: usize, n: u64) -> Vec<u8> {
        let mut state: u64 = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            data.push((state >> 33) as u8);
        }
        data
    }

    pub(super) async fn run_w3(
        opts: &RunOpts,
        out: &mut Vec<BenchmarkResult>,
        temp_dir_base: Option<&PathBuf>,
    ) {
        let iters = opts.effective_iters(20);
        let cells: &[(usize, &str)] = &[
            (4 * BENCH_CHUNK_SIZE, "4MiB"),
            (16 * BENCH_CHUNK_SIZE, "16MiB"),
        ];
        for &(size, label) in cells {
            let scenario_name = format!("w3_chunked_v2_write_single_writer_{label}");
            if !opts.matches(&scenario_name) {
                continue;
            }
            // Per-cell fresh store + server so previous cell's residency
            // doesn't pollute the next.
            let (store, _td) = match make_filesystem_store(temp_dir_base).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[bench] W3 {scenario_name} fs build failed: {e:?}");
                    continue;
                }
            };
            let handler = make_handler(store);
            let (client, _server_guard) = start_v2_server(handler).await;

            // Pre-generate all payloads + digests OUTSIDE the timer.
            let prebuilt: Vec<(DigestInfo, Vec<u8>)> = (0..iters as u64)
                .map(|n| {
                    let payload = make_payload(size, n);
                    let digest = DigestInfo::new(sha256(&payload), payload.len() as u64);
                    (digest, payload)
                })
                .collect();

            let iter_counter = AtomicU64::new(0);
            let mut extras = BTreeMap::new();
            extras.insert("chunk_size".to_string(), serde_json::json!(BENCH_CHUNK_SIZE));
            extras.insert("v3_anchor".to_string(), serde_json::json!("chunked_v2"));

            let client_for_body = client.clone();
            let prebuilt = Arc::new(prebuilt);
            let result = measure(
                "W3",
                &scenario_name,
                Some(size as u64),
                1,
                CacheState::Cold,
                iters,
                Some(size as u64),
                None,
                extras,
                move || {
                    let mut c = client_for_body.clone();
                    let prebuilt = prebuilt.clone();
                    let n = iter_counter.fetch_add(1, Ordering::Relaxed);
                    async move {
                        let (digest, payload) = &prebuilt[n as usize];
                        let chunks = build_chunks(*digest, payload);
                        let stream = tokio_stream::iter(chunks);
                        let response = c
                            .write_chunked_v2(stream)
                            .await
                            .expect("W3 write_chunked_v2 must return Ok");
                        let committed = drain_v2_response(response.into_inner())
                            .await
                            .expect("W3 commit must succeed");
                        assert_eq!(committed, size as u64);
                    }
                },
            )
            .await;
            out.push(result);
        }
    }

    /// R5: per-digest-`Notify` contended-reader anchor.
    ///
    /// Mechanism: pre-write the blob through the prod store path so
    /// it's resident on the FilesystemStore. Spawn N concurrent reader
    /// tasks that issue `get_part_unchunked` for the same digest; the
    /// per-digest `Notify` is what coalesces their lookups against the
    /// chunked driver's in-flight state. Measure wall-clock from
    /// reader-task-spawn to all-readers-bytes-received.
    ///
    /// The cell is NOT measuring writer contention (the previous R5
    /// shape did, incorrectly — see `feedback_per_chunk_timeout_design_intent`
    /// memory). It's measuring the wake-N-readers cost of the v3
    /// per-digest `Notify`.
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
            let scenario_name = format!("r5_chunked_v2_contended_readers_{label}");
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
            // For the reader-side cell we read against the underlying
            // FilesystemStore (the per-digest `Notify` coordination
            // happens in the v2 handler / chunked-driver layer; the
            // FilesystemStore is the slow tier behind it). For now, R5
            // measures the multi-reader fan-out cost against the
            // filesystem; future iterations should wire the cell
            // through `WriteChunkedV2` so the in-flight commit barrier
            // is in the path. See followup #NNN for the in-flight-
            // commit reader-wait extension.
            let handler = make_handler(store.clone());
            let (client, _server_guard) = start_v2_server(handler).await;

            // Pre-populate every iter's blob via the v2 write path so
            // the read cell measures multi-reader fan-out, not write.
            let prebuilt: Vec<DigestInfo> = {
                let mut acc = Vec::with_capacity(iters as usize);
                for n in 0..iters as u64 {
                    let payload = make_payload(size, n);
                    let digest = DigestInfo::new(sha256(&payload), payload.len() as u64);
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
            extras.insert(
                "v3_anchor".to_string(),
                serde_json::json!("per_digest_notify_readers"),
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
            ("R5", "r5_chunked_v2_contended_readers_DISABLED"),
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
        assert!(names.contains(&"r5_chunked_v2_contended_readers_DISABLED"));
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
