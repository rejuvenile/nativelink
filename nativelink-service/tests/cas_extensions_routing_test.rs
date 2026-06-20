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

//! #212 v4.5: end-to-end wire-routing test for the `WriteChunked`
//! RPC. This test is the regression harness that would have caught the
//! production routing bug — it spins up a real `tonic::transport::Server`
//! that ONLY hosts the `CasExtensions` service (NOT `WorkerApi`), then
//! drives a real `tonic::transport::Channel` against it via
//! `CasExtensionsClient::write_chunked`. The chunks reach a real
//! `ChunkedWriteHandler` backed by a real `FilesystemStore` and the
//! handler commits the blob. If the routing were still on `WorkerApi`,
//! the channel would hit the Routes builder's fallback handler and the
//! RPC would terminate with `Code::Unimplemented` carrying `"No route
//! for"` / `"unknown service"`-shaped status.
//!
//! The mutation step (documented inline below): point the client at the
//! WorkerApi proto path (the previous incorrect routing) and verify the
//! test goes red with a `Code::Unimplemented`-shaped failure. That
//! assertion proves the wire-routing fix is load-bearing — without
//! re-running this mutation by hand at change time, the routing fix
//! could silently regress to "WriteChunked back on WorkerApi" and a
//! green test sweep would let the bug ship again (the gap that let the
//! original Phase 2.4 wiring ship).
//!
//! Per CLAUDE.md test discipline:
//! - Wrapped in `tokio::time::timeout(10s)` with a SPECIFIC assertion
//!   message so a too-short timeout cannot mask a real RPC hang.
//! - The `expect()` on the timeout asserts the routing produced a
//!   server-recognized RPC reply (i.e. the URI matched a service in the
//!   Routes builder), NOT just `is_ok()`/`is_err()`.

#![cfg(all(feature = "chunked_fast_slow", feature = "test-utils"))]

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::FilesystemSpec;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    WriteChunk,
    cas_extensions_client::CasExtensionsClient,
    cas_extensions_server::CasExtensionsServer,
};
use nativelink_service::chunked_write_handler::{
    ChunkedWriteHandler, ChunkedWriteInFlight,
};
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;
use sha2::{Digest as _, Sha256};
use std::sync::OnceLock;

/// Per-test ChunkBudget singleton. Wrapped in OnceLock so the
/// handler's `&'static` lifetime is satisfied without reaching into
/// the global production budget (which would let cross-test
/// permit consumption mask real failures).
fn make_test_budget() -> &'static ChunkBudget {
    static B: OnceLock<ChunkBudget> = OnceLock::new();
    B.get_or_init(ChunkBudget::new)
}

/// Test-only chunk size (4 KiB). Smaller than the production
/// `CHUNK_SIZE` (1 MiB) so the test doesn't burn 1 MiB per chunk; the
/// handler validation paths are identical (size validation runs
/// against `self.chunk_size`, not the production constant).
const TEST_CHUNK_SIZE: usize = 4 * 1024;

/// Build a fresh `FilesystemStore` rooted at a unique per-test temp
/// directory, plus the matching ChunkedWriteHandler bound to the
/// test chunk size.
async fn make_handler() -> (Arc<ChunkedWriteHandler>, Arc<FilesystemStore<FileEntryImpl>>) {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/cas-ext-routing-test/content");
    let temp_path = format!("{base}/{nonce}/cas-ext-routing-test/temp");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed");
    let in_flight = ChunkedWriteInFlight::new();
    let handler = Arc::new(
        ChunkedWriteHandler::new_with_state_and_chunk_size_for_test(
            Arc::clone(&store),
            in_flight,
            make_test_budget(),
            TEST_CHUNK_SIZE,
        ),
    );
    (handler, store)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

/// Build the wire chunks for `payload` against `TEST_CHUNK_SIZE`. The
/// production handler validates per-chunk size + offset alignment + a
/// finish_chunk flag on the last message; we build the chunks the
/// same way the worker-side `chunked_client::collect_and_hash_chunks`
/// does so the routing test exercises the same wire shape that
/// production drives.
fn build_chunks(digest: DigestInfo, payload: &[u8]) -> Vec<WriteChunk> {
    let mut chunks = Vec::new();
    let mut offset: u64 = 0;
    let mut idx = 0usize;
    while idx < payload.len() {
        let end = (idx + TEST_CHUNK_SIZE).min(payload.len());
        let bytes = &payload[idx..end];
        let is_last_iter = end == payload.len();
        chunks.push(WriteChunk {
            digest: Some(digest.into()),
            chunk_offset: offset,
            chunk_bytes: Bytes::copy_from_slice(bytes),
            chunk_sha256: sha256(bytes).to_vec().into(),
            finish_chunk: is_last_iter,
        });
        offset += bytes.len() as u64;
        idx = end;
    }
    chunks
}

/// Wire routing E2E: a real tonic Server registers ONLY
/// `CasExtensions` (not `WorkerApi`); a real tonic Channel calls
/// `write_chunked`; the request must reach the handler, drive a
/// commit, and return `Ok` with the declared digest+size.
///
/// Mutation step (manual verification done at change time): change
/// `CasExtensionsClient::new(channel)` below to use `WorkerApiClient`
/// (and import the type). Re-run; the test must fail with a
/// `Code::Unimplemented`-shaped status because the server's Routes
/// builder has no `WorkerApi` service registered. Restore the
/// `CasExtensionsClient` line and re-confirm green.
#[nativelink_test]
async fn write_chunked_routes_via_cas_extensions_service() {
    let (handler, _store) = make_handler().await;

    // Bind the server on an ephemeral port — only `CasExtensions` is
    // registered. If WriteChunked were still on `WorkerApi` the call
    // would land on the fallback handler, NOT this handler.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral bind must succeed (CI host out of ports?)");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let svc = CasExtensionsServer::from_arc(Arc::clone(&handler));
    let server_handle = tokio::spawn(async move {
        drop(
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await,
        );
    });

    // Construct the client transport against the ephemeral port. A
    // small connect timeout keeps the test fast if anything is wrong
    // with the server bind (routing bugs do not surface here — they
    // surface AFTER the channel is open and the URI is dispatched).
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("endpoint parse must succeed")
        .connect_timeout(Duration::from_secs(2));
    let channel = endpoint
        .connect()
        .await
        .expect("client must connect to the in-process CasExtensions server");

    // Prepare a real, deterministic blob spanning `TEST_CHUNK_SIZE +
    // 17` bytes — produces ONE full chunk + ONE short final chunk so
    // the handler sees a multi-message stream with the standard
    // finish_chunk semantics.
    let payload_len_usize = TEST_CHUNK_SIZE + 17;
    let payload: Vec<u8> = (0..payload_len_usize)
        .map(|i| (i as u8).wrapping_mul(31))
        .collect();
    let payload_len = payload.len() as u64;
    let digest = DigestInfo::new(sha256(&payload), payload_len);
    let chunks = build_chunks(digest, &payload);
    assert_eq!(
        chunks.len(),
        2,
        "test geometry: payload must produce exactly 2 chunks so the handler \
         exercises both the first-chunk and the finish-chunk paths",
    );

    // Drive the RPC via the CasExtensionsClient. The wire path is
    //   /com.github.trace_machina.nativelink.remote_execution.CasExtensions/WriteChunked
    // — if WriteChunked were still routed via WorkerApi this dispatch
    // would resolve to a different (un-registered) URI on the server
    // and the call would terminate with Code::Unimplemented.
    let mut client = CasExtensionsClient::new(channel);
    let stream = tokio_stream::iter(chunks);

    let response = tokio::time::timeout(
        Duration::from_secs(10),
        client.write_chunked(stream),
    )
    .await
    .expect(
        "must not deadlock — WriteChunked routing is broken if the server \
         never replies within 10s on a single in-process chunk",
    )
    .expect(
        "WriteChunked RPC must return Ok — a `Code::Unimplemented`/`Status` \
         here proves the request reached the Routes builder's fallback \
         handler instead of the CasExtensionsServer (the production \
         routing bug being regression-tested)",
    )
    .into_inner();

    // The server acknowledged the commit AND returned the same
    // declared digest + size — confirming the handler ran (i.e. the
    // routing landed on the CasExtensions impl, not on a generic
    // tonic-fallback Unimplemented).
    let returned_digest_proto = response
        .committed_digest
        .expect("WriteChunkedResponse.committed_digest must be set on success");
    let returned_digest = DigestInfo::try_from(returned_digest_proto)
        .expect("returned digest proto must convert back to DigestInfo");
    assert_eq!(
        returned_digest, digest,
        "committed_digest must equal the declared digest — divergence \
         indicates the request landed on a stub or wrong service trait \
         impl, not the real ChunkedWriteHandler",
    );
    assert_eq!(
        response.committed_size, payload_len,
        "committed_size must equal declared payload length — divergence \
         indicates a mock or fallback returned a default response instead \
         of the real handler",
    );

    server_handle.abort();
}
