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

//! `#fl1786-server-side-digest-function-proving`: the chunked-commit site.
//!
//! **The latch this closes.** `WriteChunk` (`worker_api.proto:1463-1475`)
//! carries no digest-function field, so the server's end-to-end commit
//! verification (`chunked_write_handler_v2.rs`, `v2_verify_e2e_hash`) hashed
//! the assembled blob with the PROCESS-GLOBAL `default_digest_hasher_func()`
//! and compared against the blob's declared digest. Production sets that
//! global to BLAKE3 (`buildcache-native.json5:748`). A SHA-256-keyed blob
//! therefore failed the comparison on every attempt, the server stayed
//! missing the blob, and the next `BlobsAvailable` tick re-solicited it —
//! a LATCH, not a retry storm.
//!
//! **Production composition this file reproduces, exactly.**
//! - The process default digest function is **BLAKE3** — set below via
//!   `set_default_digest_hasher_func` and then ASSERTED, because a silent
//!   `OnceCell`-already-set would turn every test here green for the wrong
//!   reason.
//! - Per-chunk `chunk_sha256` values are computed with that same default,
//!   because that is what the worker's chunked client does
//!   (`chunked_client.rs:823` reads `default_digest_hasher_func()`), and the
//!   server checks them with its own default (`compute_sha256_blocking_v2`).
//!   Both processes agree by convention; that convention is NOT what this
//!   file is about and must not be perturbed.
//! - The BLOB digest is keyed with **SHA-256** — the mislabel. This is the
//!   live shape: the Bazel client keyed the blob sha256, the worker held the
//!   bytes, and neither the worker (no ambient context on the backfill path)
//!   nor the server (no proto field) had the function.
//! - `chunked_writes_enabled` is irrelevant on the SERVER side, but the
//!   client half of the production path is the chunked RPC: these tests
//!   drive the real `WriteChunkedV2` RPC over a real tonic server. A prior
//!   test in this area passed only because its fixture set
//!   `chunked_writes_enabled: false` — the production setting INVERTED — so
//!   nothing here is allowed to depend on that flag.
//!
//! Every test is wrapped in `tokio::time::timeout` as a deadlock detector
//! with a bespoke expect-message.

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use nativelink_config::stores::FilesystemSpec;
use nativelink_macro::nativelink_test;
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
use nativelink_util::digest_hasher::{
    DigestHasher as _, DigestHasherFunc, default_digest_hasher_func,
    set_default_digest_hasher_func,
};
use sha2::{Digest as _, Sha256};
use tokio_stream::StreamExt as _;

const TEST_CHUNK_SIZE: usize = 4 * 1024;

/// Pin the process-global digest function to BLAKE3 — the PRODUCTION value
/// (`buildcache-native.json5:748` / `worker.json5:368`). Integration tests are
/// one process per test binary, so the `OnceCell` is set once here and
/// shared by every test in this file.
///
/// The `let _ =` swallows an already-set error, so the assert is the
/// load-bearing half: without it a binary whose default was already SHA-256
/// would run every test below against the wrong global and pass for a
/// reason production cannot reproduce.
fn pin_production_default_blake3() {
    let _ = set_default_digest_hasher_func(DigestHasherFunc::Blake3);
    assert_eq!(
        default_digest_hasher_func(),
        DigestHasherFunc::Blake3,
        "#fl1786: this test binary must run with the PRODUCTION process-global \
         digest function (BLAKE3, buildcache-native.json5:748). The global is a \
         OnceCell; if it reads SHA-256 here something set it first and every \
         assertion in this file would be exercising the wrong composition"
    );
}

/// Blob digest keyed with SHA-256 — the MISLABELLED-relative-to-the-global
/// case that is the live latch.
fn sha256_digest(bytes: &[u8]) -> DigestInfo {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    DigestInfo::new(a, bytes.len() as u64)
}

/// Blob digest keyed with the process default (BLAKE3) — the correctly
/// labelled case that must keep taking the fast path.
fn default_func_digest(bytes: &[u8]) -> DigestInfo {
    let mut h = default_digest_hasher_func().hasher();
    h.update(bytes);
    h.finalize_digest()
}

/// Per-chunk hash, computed with the PROCESS DEFAULT — exactly what the
/// worker's chunked client does at `chunked_client.rs:823`. Not the blob
/// digest function; a separate, convention-shared wire checksum.
fn chunk_hash_with_default(bytes: &[u8]) -> Vec<u8> {
    let mut h = default_digest_hasher_func().hasher();
    h.update(bytes);
    (**h.finalize_digest().packed_hash()).to_vec()
}

async fn make_store() -> (Arc<FilesystemStore<FileEntryImpl>>, String) {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/fl1786-chunked/content");
    let temp_path = format!("{base}/{nonce}/fl1786-chunked/temp");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.clone(),
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed");
    (store, content_path)
}

fn make_handler(store: Arc<FilesystemStore<FileEntryImpl>>) -> Arc<ChunkedWriteHandler> {
    let budget: &'static ChunkBudget = Box::leak(Box::new(ChunkBudget::new()));
    Arc::new(ChunkedWriteHandler::new_with_state_and_chunk_size_for_test(
        store,
        ChunkedWriteInFlight::new(),
        budget,
        TEST_CHUNK_SIZE,
    ))
}

fn make_chunk(digest: DigestInfo, offset: u64, bytes: &[u8], finish: bool) -> WriteChunk {
    WriteChunk {
        digest: Some(digest.into()),
        chunk_offset: offset,
        chunk_bytes: Bytes::copy_from_slice(bytes),
        chunk_sha256: chunk_hash_with_default(bytes).into(),
        finish_chunk: finish,
    }
}

fn build_chunks(digest: DigestInfo, payload: &[u8]) -> Vec<WriteChunk> {
    let mut chunks = Vec::new();
    let mut offset: u64 = 0;
    let mut remaining = payload;
    while !remaining.is_empty() {
        let take = TEST_CHUNK_SIZE.min(remaining.len());
        let is_final = take == remaining.len();
        chunks.push(make_chunk(digest, offset, &remaining[..take], is_final));
        offset += take as u64;
        remaining = &remaining[take..];
    }
    chunks
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
    let svc = CasExtensionsServer::new(ChunkedCasExtensionsAdapter::new(handler));
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await;
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("endpoint parse must succeed")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("client must connect to in-process v2 server");
    (CasExtensionsClient::new(channel), server_handle)
}

/// Drive one complete `WriteChunkedV2` session and return the terminal
/// outcome: `Ok(committed_size)` or the gRPC `Status` the server sent.
async fn upload_via_v2(
    client: &mut CasExtensionsClient<tonic::transport::Channel>,
    chunks: Vec<WriteChunk>,
) -> Result<u64, tonic::Status> {
    let stream = client
        .write_chunked_v2(tokio_stream::iter(chunks))
        .await
        .map_err(|s| s)?;
    let mut stream: tonic::Streaming<WriteChunkedFrame> = stream.into_inner();
    while let Some(frame_res) = stream.next().await {
        match frame_res {
            Ok(frame) => match frame.payload {
                Some(write_chunked_frame::Payload::FinalResponse(resp)) => {
                    return Ok(resp.committed_size);
                }
                Some(write_chunked_frame::Payload::Ack(_)) | None => {}
            },
            Err(status) => return Err(status),
        }
    }
    Err(tonic::Status::internal(
        "v2 stream ended without a FinalResponse and without a Status",
    ))
}

fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(37).wrapping_add(11))
        .collect()
}

// -----------------------------------------------------------------------------
// 1. THE LIVE LATCH: SHA-256-keyed blob, BLAKE3 process default.
// -----------------------------------------------------------------------------

/// A blob whose declared digest was computed with SHA-256, uploaded through
/// the real `WriteChunkedV2` RPC while the process default is BLAKE3, must
/// COMMIT. Pre-fix the server hashes the assembled `.holding` file with
/// BLAKE3, the comparison fails, and the session terminates with
/// `WriteChunkedV2: end-to-end SHA-256 mismatch`.
#[nativelink_test]
async fn chunked_sha256_keyed_blob_under_blake3_default_is_accepted() {
    pin_production_default_blake3();
    let data = payload(3 * TEST_CHUNK_SIZE);
    let digest = sha256_digest(&data);

    let (store, _content_path) = make_store().await;
    let handler = make_handler(store);
    let metrics = handler.v2_metrics_for_test();
    let (mut client, _server) = start_v2_server(Arc::clone(&handler)).await;

    let committed = tokio::time::timeout(
        Duration::from_secs(20),
        upload_via_v2(&mut client, build_chunks(digest, &data)),
    )
    .await
    .expect(
        "#fl1786: WriteChunkedV2 session for the SHA-256-keyed blob did not terminate within 20s \
         — deadlock in the commit path, not a proving failure",
    );

    assert_eq!(
        committed
            .as_ref()
            .map(|n| *n)
            .map_err(|s| s.message().to_string()),
        Ok(data.len() as u64),
        "#fl1786: a blob whose bytes genuinely hash to its declared digest under SHA-256 must \
         COMMIT even though the server's process-global digest function is BLAKE3. The server \
         holds the bytes at verification time, so the digest function is a CHECKABLE CLAIM, not \
         a guess. Rejecting it is the backfill latch: the server stays missing the blob and the \
         next BlobsAvailable tick re-solicits it forever",
    );

    assert_eq!(
        metrics.digest_func_proven_total.load(Ordering::Relaxed),
        1,
        "#fl1786: the accept must have gone through the PROVING pass exactly once — a green \
         assertion above with this counter at 0 would mean the blob was accepted by some other \
         mechanism and the proving site is not what made it pass",
    );
    assert_eq!(
        metrics.sha256_e2e_mismatches_total.load(Ordering::Relaxed),
        0,
        "#fl1786: a PROVEN blob is not a data-integrity event; \
         sha256_e2e_mismatches_total is the corrupt-blob alarm and must stay clean",
    );
}

// -----------------------------------------------------------------------------
// 2. FAIL-CLOSED: genuinely corrupt blob is still rejected.
// -----------------------------------------------------------------------------

/// A blob whose bytes reproduce its declared digest under NO advertised
/// digest function must still be REJECTED. Proving must never degrade to
/// "if no candidate matches, allow".
#[nativelink_test]
async fn chunked_corrupt_blob_is_still_rejected_fail_closed() {
    pin_production_default_blake3();
    let data = payload(3 * TEST_CHUNK_SIZE);
    // Declare the SHA-256 digest of DIFFERENT bytes, then ship `data`.
    // Neither SHA-256 nor BLAKE3 of `data` reproduces it.
    let mut other = data.clone();
    other[0] ^= 0xFF;
    let lying_digest = DigestInfo::new(**sha256_digest(&other).packed_hash(), data.len() as u64);

    let (store, _content_path) = make_store().await;
    let handler = make_handler(store);
    let metrics = handler.v2_metrics_for_test();
    let (mut client, _server) = start_v2_server(Arc::clone(&handler)).await;

    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        upload_via_v2(&mut client, build_chunks(lying_digest, &data)),
    )
    .await
    .expect(
        "#fl1786: WriteChunkedV2 session for the corrupt blob did not terminate within 20s — \
         the fail-closed path must reject promptly, not hang",
    );

    let status = outcome.expect_err(
        "#fl1786 FAIL-CLOSED: a blob that reproduces its declared digest under NO advertised \
         digest function is CORRUPT, not mislabelled, and must be rejected. Accepting it would \
         make proving a fail-open bypass of the CAS integrity contract",
    );
    assert!(
        status.message().contains("end-to-end") && status.message().contains("mismatch"),
        "#fl1786 FAIL-CLOSED: the rejection must still be the end-to-end hash-mismatch error so \
         operators see an unchanged data-integrity signal; got: {}",
        status.message()
    );
    assert_eq!(
        metrics.sha256_e2e_mismatches_total.load(Ordering::Relaxed),
        1,
        "#fl1786 FAIL-CLOSED: a genuinely corrupt blob must still tick the data-integrity \
         counter exactly once",
    );
}

// -----------------------------------------------------------------------------
// 3. FAST PATH: correctly-labelled blob does NO extra hashing.
// -----------------------------------------------------------------------------

/// A blob keyed with the process default (BLAKE3) must commit WITHOUT the
/// proving pass running at all. This asserts the MECHANISM — the proving
/// counter is the direct observable for "the extra read-and-hash happened"
/// — rather than only the outcome, which a always-prove implementation
/// would also satisfy.
#[nativelink_test]
async fn chunked_correctly_labelled_blob_takes_fast_path_without_proving() {
    pin_production_default_blake3();
    let data = payload(3 * TEST_CHUNK_SIZE);
    let digest = default_func_digest(&data);

    let (store, _content_path) = make_store().await;
    let handler = make_handler(store);
    let metrics = handler.v2_metrics_for_test();
    let (mut client, _server) = start_v2_server(Arc::clone(&handler)).await;

    let committed = tokio::time::timeout(
        Duration::from_secs(20),
        upload_via_v2(&mut client, build_chunks(digest, &data)),
    )
    .await
    .expect("#fl1786: WriteChunkedV2 session for the BLAKE3-keyed blob did not terminate in 20s");

    assert_eq!(
        committed
            .as_ref()
            .map(|n| *n)
            .map_err(|s| s.message().to_string()),
        Ok(data.len() as u64),
        "#fl1786: a correctly-labelled blob must still commit",
    );
    assert_eq!(
        metrics.digest_func_proven_total.load(Ordering::Relaxed),
        0,
        "#fl1786 FAST PATH: a blob that matches under the process-global digest function must \
         NOT trigger the proving pass. A non-zero counter here means every commit on the hot \
         write path is paying an extra full read + N hashes of the blob",
    );
}

// -----------------------------------------------------------------------------
// 4. I/O FAULT ATTRIBUTION: a failed READ is not a corruption verdict.
// -----------------------------------------------------------------------------

/// `.holding` has live concurrent unlinkers: `filesystem_store.rs:2394` —
/// the `#256` duplicate-commit guard in `finalize_holding`, whose own
/// comment reads "covers the case where a sibling concurrent caller already
/// unlinked it" — plus the `#497` owner-drop class. (An earlier version of
/// this comment cited `e78d1898`'s idle-TTL reaper; that commit reaps
/// `.partial`, not `.holding`, and `pair-a` P5 refuted the cite. The
/// conclusion survives the correction: the file has concurrent unlinkers and
/// an ENOENT during verify is reachable, on exactly the mislabelled digests
/// the proving pass exists to rescue.)
///
/// **What this test observes, stated precisely because an earlier version of
/// this doc-comment certified more than the assertion could see:** the path
/// does not exist, so PASS 1's `File::open` fails and pass 2 never runs.
/// That is the pass-1 leg of the I/O-fault contract — a failed read of
/// `.holding` surfaces as `Code::Internal` on the WIRE, not the
/// `Code::InvalidArgument` a client's retry classifier reads as "these bytes
/// were disproven". The COUNTER attribution — which no longer depends on
/// this code at all — is pinned separately by
/// `real_io_fault_through_the_commit_seam_does_not_tick_the_corrupt_blob_alarm`.
#[nativelink_test]
async fn e2e_verify_io_fault_is_internal_not_a_data_integrity_verdict() {
    pin_production_default_blake3();
    let data = payload(TEST_CHUNK_SIZE);
    let digest = sha256_digest(&data);

    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    // A `.holding` path that does not exist — precisely what the concurrent
    // unlinker leaves behind.
    let missing = std::path::PathBuf::from(format!("{base}/{nonce}/fl1786-vanished.holding"));

    let err = nativelink_service::chunked_write_handler_v2::v2_verify_e2e_hash_for_test(
        &missing,
        &digest,
        data.len() as u64,
    )
    .await
    .expect_err(
        "#fl1786: verifying a .holding file that does not exist must FAIL — an Ok here would \
         mean the verify silently accepted a blob it never read",
    );

    assert_eq!(
        err.code,
        nativelink_error::Code::Internal,
        "#fl1786: a failed READ of .holding is an I/O FAULT, not a data-integrity verdict. \
         Reporting it as InvalidArgument tells every downstream consumer — the corrupt-blob \
         alarm, the operator, the retry classifier — that these bytes were disproven, when in \
         fact they were never read. The .holding file has live concurrent unlinkers \
         (e78d1898 idle-TTL reaper, #497 owner-drop), so this is reachable on the rescue path. \
         Got: {err:?}",
    );
}

// -----------------------------------------------------------------------------
// 5. THE CALL EDGE: a REAL verify outcome reaching the REAL counters.
//
// Everything below drives `v2_verify_and_attribute` — the production stage-2
// seam that `v2_run_commit_path` calls — with a REAL `.holding` file on disk,
// so the whole chain (real read → real fault discriminant → real counters) is
// under assertion. The predecessor tests handed a hand-built `Error` to a
// shim, which pinned the routing table but never the codes that arrive at it,
// and left BOTH review mutations alive:
//   * pair-a A-1: replacing the record call with two unconditional
//     `fetch_add`s left 6/6 green (`/tmp/svrprove-final-mut-A1-pre.log`).
//   * pair-b M13: re-coding the pass-2 I/O fault to `InvalidArgument` left
//     6/6 green (`/tmp/svrprove-final-mut-M13-pre.log`).
// -----------------------------------------------------------------------------

/// Write `bytes` to the digest's real `.holding` path — the file
/// `commit_chunked` produces and `v2_verify_e2e_hash` reads. Creating the
/// file directly is what lets the test choose the verify's outcome
/// deterministically; the path itself is production's own
/// (`holding_content_path`), not a fixture.
async fn write_holding(
    store: &Arc<FilesystemStore<FileEntryImpl>>,
    digest: &DigestInfo,
    bytes: &[u8],
) -> std::path::PathBuf {
    let path = store.holding_content_path(digest);
    tokio::fs::create_dir_all(path.parent().expect("holding path must have a parent"))
        .await
        .expect("holding shard dir must be creatable");
    tokio::fs::write(&path, bytes)
        .await
        .expect("holding file must be writable");
    path
}

/// A REAL I/O fault, produced by the REAL verify against a vanished
/// `.holding`, filed by the REAL commit-path seam, must land on
/// `commit_failures_total` ONLY — never on `sha256_e2e_mismatches_total`,
/// whose own help text says "corrupt-blob alarm" and whose documented
/// operator action is "alert on any non-zero".
#[nativelink_test]
async fn real_io_fault_through_the_commit_seam_does_not_tick_the_corrupt_blob_alarm() {
    pin_production_default_blake3();
    let data = payload(TEST_CHUNK_SIZE);
    let digest = sha256_digest(&data);
    let (store, _content_path) = make_store().await;
    let handler = make_handler(Arc::clone(&store));
    let metrics = handler.v2_metrics_for_test();

    // No `.holding` file: exactly what filesystem_store.rs:2394's
    // "a sibling concurrent caller already unlinked it" leaves behind.
    let vanished = store.holding_content_path(&digest);
    assert!(
        !vanished.exists(),
        "#fl1786: the .holding file must be ABSENT for this test to drive an I/O fault; if it \
         exists the verify reads it and this test proves nothing"
    );

    let err = tokio::time::timeout(
        Duration::from_secs(20),
        handler.v2_verify_and_attribute_for_test(&digest, &vanished, digest.size_bytes()),
    )
    .await
    .expect(
        "#fl1786: the stage-2 verify seam did not return within 20s on a vanished .holding — \
         a wedge in the verify or its cleanup, not an attribution failure",
    )
    .expect_err("#fl1786: verifying a .holding that does not exist must FAIL");

    assert_eq!(
        err.code,
        nativelink_error::Code::Internal,
        "#fl1786: the wire code for a failed READ of .holding must stay Code::Internal; got \
         {err:?}",
    );
    assert_eq!(
        metrics.sha256_e2e_mismatches_total.load(Ordering::Relaxed),
        0,
        "#fl1786 CALL EDGE: an I/O fault reaching the real commit seam must NOT tick \
         sha256_e2e_mismatches_total. That counter is the corrupt-blob alarm; ticking it for a \
         transient ENOENT sends an operator to open a CAS-corruption incident on blobs that are \
         intact, on exactly the digests the proving pass exists to rescue. This assertion is \
         what pair-a A-1 found missing: the routing table and the error shape were each pinned \
         and their JOIN was not, so restoring the two unconditional fetch_adds left every test \
         green",
    );
    assert_eq!(
        metrics.commit_failures_total.load(Ordering::Relaxed),
        1,
        "#fl1786: the commit still FAILED, so commit_failures_total must tick regardless of \
         cause — otherwise a storm of I/O faults is invisible on every counter",
    );
    assert_eq!(
        metrics.digest_func_proven_total.load(Ordering::Relaxed),
        0,
        "#fl1786: a blob that was never read was not PROVEN either",
    );
}

/// Control, the other direction, and the one case that folds pass 2 for
/// real: a `.holding` whose bytes reproduce the declared digest under NO
/// advertised function is an integrity verdict and MUST tick the alarm.
/// Without this the assertion above could be satisfied by never ticking it.
#[nativelink_test]
async fn real_unprovable_blob_through_the_commit_seam_ticks_the_corrupt_blob_alarm() {
    pin_production_default_blake3();
    let data = payload(TEST_CHUNK_SIZE);
    // Declare the SHA-256 digest of DIFFERENT bytes at the RIGHT length, so
    // the size check passes and pass 2 actually folds every candidate.
    let mut other = data.clone();
    other[0] ^= 0xFF;
    let lying_digest = DigestInfo::new(**sha256_digest(&other).packed_hash(), data.len() as u64);

    let (store, _content_path) = make_store().await;
    let handler = make_handler(Arc::clone(&store));
    let metrics = handler.v2_metrics_for_test();
    let holding = write_holding(&store, &lying_digest, &data).await;

    let err = tokio::time::timeout(
        Duration::from_secs(20),
        handler.v2_verify_and_attribute_for_test(&lying_digest, &holding, lying_digest.size_bytes()),
    )
    .await
    .expect("#fl1786: the stage-2 verify seam did not return within 20s on an unprovable blob")
    .expect_err(
        "#fl1786 FAIL-CLOSED: a blob that reproduces its declared digest under NO advertised \
         function is CORRUPT and must be rejected at the seam, not accepted",
    );

    assert_eq!(
        err.code,
        nativelink_error::Code::InvalidArgument,
        "#fl1786: an integrity verdict must reach the client as InvalidArgument (permanent), \
         not Internal (retryable); got {err:?}",
    );
    assert_eq!(
        metrics.sha256_e2e_mismatches_total.load(Ordering::Relaxed),
        1,
        "#fl1786 CALL EDGE: a genuine data-integrity verdict — no advertised function \
         reproduces the declared digest from bytes the server actually READ — MUST still tick \
         the corrupt-blob alarm exactly once. Suppressing it would silence the one signal that \
         distinguishes a lying producer from a mislabelled one",
    );
    assert_eq!(
        metrics.commit_failures_total.load(Ordering::Relaxed),
        1,
        "#fl1786: an integrity verdict is also a failed commit",
    );
}

/// `pair-a` T-1: the assembled-size fault is an INTEGRITY verdict, not an
/// I/O fault. `expected_size` is `digest.size_bytes()` and the fold reads to
/// EOF, so a length disagreement says the assembled blob is not the declared
/// blob under EVERY digest function — `DigestInfo` identity is hash AND size.
/// The reviewed commit demoted it out of the corrupt-blob alarm on a doc
/// claim ("proves NOTHING in either direction") that the code refutes; this
/// pins the correction in the fail-CLOSED direction.
#[nativelink_test]
async fn assembled_size_fault_is_an_integrity_verdict_not_an_io_fault() {
    pin_production_default_blake3();
    let data = payload(TEST_CHUNK_SIZE);
    // Same hash, a declared size 7 bytes longer than the bytes on disk.
    let short_digest = DigestInfo::new(**sha256_digest(&data).packed_hash(), data.len() as u64 + 7);

    let (store, _content_path) = make_store().await;
    let handler = make_handler(Arc::clone(&store));
    let metrics = handler.v2_metrics_for_test();
    let holding = write_holding(&store, &short_digest, &data).await;

    let err = tokio::time::timeout(
        Duration::from_secs(20),
        handler.v2_verify_and_attribute_for_test(
            &short_digest,
            &holding,
            short_digest.size_bytes(),
        ),
    )
    .await
    .expect("#fl1786: the stage-2 verify seam did not return within 20s on a size mismatch")
    .expect_err("#fl1786: a .holding whose length differs from the declared size must be REJECTED");

    assert_eq!(
        metrics.sha256_e2e_mismatches_total.load(Ordering::Relaxed),
        1,
        "#fl1786 / pair-a T-1: a .holding whose length disagrees with the declared size \
         DISPROVES the blob — DigestInfo identity is hash AND size — so it must tick the \
         corrupt-blob alarm. Filing it under commit_failures_total alone (which the reviewed \
         commit did, calling it 'an I/O fault that proves NOTHING in either direction') removes \
         the truncated/extended-assembly case from the only data-integrity alarm on this path, \
         which is the fail-OPEN direction. Got 0",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::InvalidArgument,
        "#fl1786 / pair-a T-1: the size fault is bad INPUT, and its stage-1 sibling for the \
         identical fault (chunked_filesystem.rs:1397-1408, 'chunked commit length mismatch') \
         already returns InvalidArgument. Returning Internal tells the client's retry \
         classifier that a permanent producer bug is a transient server fault; got {err:?}",
    );
    assert_eq!(
        metrics.commit_failures_total.load(Ordering::Relaxed),
        1,
        "#fl1786: a size fault is also a failed commit",
    );
}

/// The fold's OTHER error arm, at the same seam. The vanished-file test
/// above drives `File::open`; this drives `File::read` — a directory at the
/// `.holding` path opens fine on Linux and fails `EISDIR` on the first read
/// — so both of `v2_fold_holding_file`'s failure exits are observed reaching
/// the counters, not just one.
///
/// Scope, stated so this is not read as more than it is: this does NOT by
/// itself kill pair-b's M13, because a fold error carries `Code::Internal`
/// here and M13 forged `Code::InvalidArgument`. M13 is now INERT — the
/// counter decision no longer reads the code at all — and that inertness is
/// pinned at the type level by
/// `chunked_write_handler_v2::v2_verify_fault_attribution_tests::no_error_code_can_forge_the_integrity_verdict`.
/// What this test adds is the execution half: the fold's read arm really
/// does reach the seam and really is filed as an I/O fault.
#[nativelink_test]
async fn a_real_read_failure_at_the_seam_files_under_commit_failures_only() {
    pin_production_default_blake3();
    let data = payload(TEST_CHUNK_SIZE);
    let digest = sha256_digest(&data);
    let (store, _content_path) = make_store().await;
    let handler = make_handler(Arc::clone(&store));
    let metrics = handler.v2_metrics_for_test();

    // A DIRECTORY at the .holding path: `File::open` succeeds on Linux and
    // the first `read` returns EISDIR, so this drives the fold's read-error
    // arm rather than its open-error arm.
    let holding = store.holding_content_path(&digest);
    tokio::fs::create_dir_all(&holding)
        .await
        .expect("holding-as-directory must be creatable");

    let err = tokio::time::timeout(
        Duration::from_secs(20),
        handler.v2_verify_and_attribute_for_test(&digest, &holding, digest.size_bytes()),
    )
    .await
    .expect("#fl1786: the stage-2 verify seam did not return within 20s on an unreadable .holding")
    .expect_err("#fl1786: an unreadable .holding must FAIL the verify");

    assert!(
        err.messages.iter().any(|m| m.contains("read .holding")),
        "#fl1786: this test exists to drive the fold's READ arm, distinct from the open arm the \
         vanished-file test drives. If the error is the open-failure message instead, the \
         directory trick stopped working on this platform and the arm is unobserved; got {err:?}",
    );
    assert_eq!(
        metrics.sha256_e2e_mismatches_total.load(Ordering::Relaxed),
        0,
        "#fl1786 M13: a failure to READ .holding must never reach the corrupt-blob alarm, \
         whatever Code its error carries. Before this fix the alarm was selected by \
         `err.code == InvalidArgument`, so a single map_err inside the fold re-filed an ENOENT \
         as corruption with the whole suite green (pair-b M13). The verdict is now a \
         discriminant with two explicit construction sites, both of them statements about the \
         BYTES. Got a non-zero alarm from an unread blob: {err:?}",
    );
    assert_eq!(
        metrics.commit_failures_total.load(Ordering::Relaxed),
        1,
        "#fl1786: an unreadable .holding is still a failed commit",
    );
}

/// **Site A's log volume, both directions.** 65 proven verifies must emit
/// exactly 2 `warn!` lines (occurrences 1 and 64) while the counter reaches
/// 65. `pair-b` C1: the reviewed commit's `Behavior changes` said the
/// proven-write warn was sampled "at both sites"; site A had no decision
/// guard at all, in the file that already owned the helper. Asserting only
/// the counter would leave an unconditional `warn!` perfectly green, and
/// asserting only the log count would be satisfied by dropping the counter.
// NOTE: no explicit `#[tracing_test::traced_test]` — `#[nativelink_test]`
// already applies it (`nativelink-macro/src/lib.rs:76`), and applying it
// twice nests two capture buffers so `logs_assert` reads an EMPTY one.
#[nativelink_test]
async fn sixty_five_proven_commits_emit_two_warns_and_count_sixty_five() {
    pin_production_default_blake3();
    let (store, _content_path) = make_store().await;
    let handler = make_handler(Arc::clone(&store));
    let metrics = handler.v2_metrics_for_test();

    for i in 0..65_u32 {
        // Distinct bytes per iteration so each is a fresh blob, each keyed
        // with SHA-256 while the process default is BLAKE3 — the live
        // mislabel, so every one takes the proving pass.
        let bytes = format!("fl1786 mislabelled chunked blob {i}").into_bytes();
        let digest = sha256_digest(&bytes);
        let holding = write_holding(&store, &digest, &bytes).await;
        let proven = tokio::time::timeout(
            Duration::from_secs(20),
            handler.v2_verify_and_attribute_for_test(&digest, &holding, digest.size_bytes()),
        )
        .await
        .expect("#fl1786: proven-commit seam did not return within 20s")
        .expect("#fl1786: every mislabelled-but-intact blob must be accepted at the seam");
        assert_eq!(
            proven,
            Some(DigestHasherFunc::Sha256),
            "#fl1786: iteration {i} must have been rescued by PROVING sha256; a None here means \
             the blob passed under the process default and this loop is not exercising the \
             sampled site at all",
        );
    }

    assert_eq!(
        metrics.digest_func_proven_total.load(Ordering::Relaxed),
        65,
        "#fl1786: the COUNTER must carry the true rate — it is what the sampled log gives up \
         precision for, and the /metrics contract this change rests on. A value of 2 means the \
         counter was moved inside the sampling branch and the true rate is gone",
    );
    logs_assert(|lines: &[&str]| {
        let emitted = lines
            .iter()
            .filter(|l| l.contains("accepted by PROVING its digest function"))
            .count();
        if emitted == 2 {
            Ok(())
        } else {
            Err(format!(
                "#fl1786 / pair-b C1: 65 proven commits must emit exactly 2 warns (occurrences \
                 1 and 64); emitted {emitted}. 65 means site A is back to one un-rate-limited \
                 warn per proven commit — the shape that backs up the nonblocking log writer \
                 under a build burst, on a server with a standing write-burst-stall incident, \
                 and the shape the reviewed commit message claimed was already retired here. \
                 0 means the signal is gone entirely"
            ))
        }
    });
}
