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

//! Flows W3 + R5: chunked-v2 anchoring cells.
//!
//! **W3 (chunked v2 write):** single writer drives an end-to-end
//! `WriteChunkedV2` session against an in-process server backed by a
//! real `FilesystemStore`. Anchors per-chunk overhead + commit-runner
//! wall-clock for the v3-default path.
//!
//! **R5 (chunked v2 contended writers):** N concurrent writers race the
//! SAME digest. Anchors the per-digest `Notify` coordination overhead
//! introduced by the 2026-05-15 v3 bundle. A regression here flags any
//! future change that re-introduces global-broadcast wake semantics or
//! adds a serialization point in the commit-publish path.
//!
//! Both cells are gated on `feature = "chunked_fast_slow"`. With the
//! feature off the scenarios emit a single result with
//! `extras.disabled = true` and `iters = 0` so baseline diffing can
//! distinguish "didn't run" from "ran and regressed".

use crate::output::BenchmarkResult;
use crate::scenarios::RunOpts;

#[cfg(not(feature = "chunked_fast_slow"))]
use std::collections::BTreeMap;
#[cfg(not(feature = "chunked_fast_slow"))]
use crate::output::{CacheState, LatencyPercentiles, Throughput};

#[cfg(feature = "chunked_fast_slow")]
mod enabled {
    use core::time::Duration;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bytes::Bytes;
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
    use nativelink_util::common::DigestInfo;
    use sha2::{Digest as _, Sha256};
    use tokio_stream::StreamExt as _;

    use crate::output::{BenchmarkResult, CacheState};
    use crate::scenarios::{RunOpts, measure};

    /// Chunk size used in the v2 bench cells. Chosen at the v2 default
    /// chunk size so the bench matches the prod chunk-flow; matching
    /// `ChunkedWriteHandler::DEFAULT_CHUNK_SIZE` would be ideal but
    /// that constant is internal — the v2 test rig uses 4 KiB for
    /// fast iteration. We use 1 MiB here so the per-chunk overhead is
    /// in the right ballpark for diff comparisons against the W1
    /// non-chunked path.
    const BENCH_CHUNK_SIZE: usize = 1024 * 1024;

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(bytes);
        let out = h.finalize();
        let mut a = [0u8; 32];
        a.copy_from_slice(out.as_ref());
        a
    }

    async fn make_filesystem_store() -> Arc<FilesystemStore<FileEntryImpl>> {
        let base = std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
        let nonce: u64 = rand_u64();
        let content_path = format!("{base}/{nonce}/v2-bench/content");
        let temp_path = format!("{base}/{nonce}/v2-bench/temp");
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path,
            temp_path,
            eviction_policy: None,
            block_size: 1,
            ..Default::default()
        })
        .await
        .expect("FilesystemStore::new must succeed")
    }

    fn rand_u64() -> u64 {
        // Cheap fresh nonce — bench process is short-lived so a
        // process-id + nanos mix is plenty.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        let pid = std::process::id() as u64;
        // Monotonic counter ensures concurrent calls in the same nanos
        // bucket still produce distinct nonces.
        let bump = NONCE_BUMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        (pid << 32) ^ nanos ^ bump
    }

    static NONCE_BUMP: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0xA5A5_5A5A_DEAD_BEEF);

    fn make_handler(
        store: Arc<FilesystemStore<FileEntryImpl>>,
    ) -> Arc<ChunkedWriteHandler> {
        let in_flight = ChunkedWriteInFlight::new();
        // Leak a fresh ChunkBudget for the bench session — same pattern
        // as the v2 test rig. The bench process exits after one run so
        // the leak is bounded.
        let budget: &'static ChunkBudget = Box::leak(Box::new(ChunkBudget::new()));
        Arc::new(
            ChunkedWriteHandler::new_with_state_and_chunk_size_for_test(
                store, in_flight, budget, BENCH_CHUNK_SIZE,
            ),
        )
    }

    async fn start_v2_server(
        handler: Arc<ChunkedWriteHandler>,
    ) -> (
        CasExtensionsClient<tonic::transport::Channel>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral bind must succeed");
        let port = listener.local_addr().unwrap().port();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let adapter = ChunkedCasExtensionsAdapter::new(handler);
        let svc = CasExtensionsServer::new(adapter);
        let handle = tokio::spawn(async move {
            let _serve_result = tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await;
            drop(_serve_result);
        });
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

    pub(super) async fn run_w3(
        opts: &RunOpts,
        out: &mut Vec<BenchmarkResult>,
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
            // Build a server per cell so a previous cell's state doesn't
            // leak into the next.
            let store = make_filesystem_store().await;
            let handler = make_handler(store);
            let (client, _server) = start_v2_server(handler).await;

            // Distinct payload per iter so we measure the WRITE path on
            // a fresh digest every iter.
            let iter_counter = std::sync::atomic::AtomicU64::new(0);
            let mut extras = BTreeMap::new();
            extras.insert("chunk_size".to_string(), serde_json::json!(BENCH_CHUNK_SIZE));
            extras.insert("v3_anchor".to_string(), serde_json::json!("chunked_v2"));

            let client_for_body = client.clone();
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
                    let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    async move {
                        let payload: Vec<u8> = (0..size)
                            .map(|i| ((i.wrapping_add(n as usize)) & 0xFF) as u8)
                            .collect();
                        let digest = DigestInfo::new(sha256(&payload), payload.len() as u64);
                        let chunks = build_chunks(digest, &payload);
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

    pub(super) async fn run_r5(
        opts: &RunOpts,
        out: &mut Vec<BenchmarkResult>,
    ) {
        let iters = opts.effective_iters(10);
        // Cells: payload size × N writers racing same digest.
        let cells: &[(usize, u32, &str)] = &[
            (4 * BENCH_CHUNK_SIZE, 2, "4MiB_n2"),
            (4 * BENCH_CHUNK_SIZE, 10, "4MiB_n10"),
            (16 * BENCH_CHUNK_SIZE, 4, "16MiB_n4"),
        ];
        for &(size, n_writers, label) in cells {
            let scenario_name = format!("r5_chunked_v2_contended_writers_{label}");
            if !opts.matches(&scenario_name) {
                continue;
            }
            let store = make_filesystem_store().await;
            let handler = make_handler(store);
            let (client, _server) = start_v2_server(handler).await;

            let iter_counter = std::sync::atomic::AtomicU64::new(0);
            let mut extras = BTreeMap::new();
            extras.insert("chunk_size".to_string(), serde_json::json!(BENCH_CHUNK_SIZE));
            extras.insert("n_writers".to_string(), serde_json::json!(n_writers));
            extras.insert(
                "v3_anchor".to_string(),
                serde_json::json!("per_digest_notify"),
            );

            let client_for_body = client.clone();
            let throughput_per_iter = (size as u64) * (n_writers as u64);
            let result = measure(
                "R5",
                &scenario_name,
                Some(size as u64),
                n_writers,
                CacheState::Contended,
                iters,
                Some(throughput_per_iter),
                None,
                extras,
                move || {
                    let client = client_for_body.clone();
                    let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    async move {
                        let payload: Vec<u8> = (0..size)
                            .map(|i| ((i.wrapping_add(n as usize)) & 0xFF) as u8)
                            .collect();
                        let digest = DigestInfo::new(sha256(&payload), payload.len() as u64);
                        let mut futs = Vec::with_capacity(n_writers as usize);
                        for _ in 0..n_writers {
                            let mut c = client.clone();
                            let chunks = build_chunks(digest, &payload);
                            futs.push(tokio::spawn(async move {
                                let stream = tokio_stream::iter(chunks);
                                let response = c
                                    .write_chunked_v2(stream)
                                    .await
                                    .expect("R5 write_chunked_v2 must return Ok");
                                drain_v2_response(response.into_inner())
                                    .await
                                    .expect("R5 commit must succeed")
                            }));
                        }
                        // All N writers must see the same committed
                        // size. If they diverge, the per-digest commit
                        // contract is broken — fail loud.
                        let mut sizes = Vec::with_capacity(n_writers as usize);
                        for h in futs {
                            sizes.push(
                                h.await
                                    .expect("R5 writer task must not panic"),
                            );
                        }
                        for s in &sizes {
                            assert_eq!(
                                *s, size as u64,
                                "R5 commit-size invariant: all writers see same size"
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

pub async fn run(opts: &RunOpts) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();

    #[cfg(feature = "chunked_fast_slow")]
    {
        enabled::run_w3(opts, &mut out).await;
        enabled::run_r5(opts, &mut out).await;
    }

    #[cfg(not(feature = "chunked_fast_slow"))]
    {
        let _ = opts;
        // Emit two placeholder results so baselines built without the
        // feature still record the absence of W3/R5 data; otherwise diff
        // tooling can't tell "scenario disappeared" from "feature off".
        for (flow, name) in [
            ("W3", "w3_chunked_v2_write_single_writer_DISABLED"),
            ("R5", "r5_chunked_v2_contended_writers_DISABLED"),
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
