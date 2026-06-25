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

//! Repro harness for the production CAS write-corruption fire (2026-06-24):
//! `INVALID_ARGUMENT: Hashes do not match ... In ByteStreamServer::write(zero-copy)`
//! on small single-chunk blobs uploaded concurrently in the first cold builds.
//!
//! These tests drive the **real production zero-copy write composition**:
//!
//!   `ZeroCopyByteStreamService::call(http::Request)`         (public tower seam)
//!     → `ZeroCopyWriteStream::new(body)`                     (zero_copy_codec.rs)
//!       → `ZeroCopyGrpcFrameDecoder` over `BufList`          (buf_list.rs)
//!         → `WriteRequestStreamWrapper`                      (proto_stream_utils.rs)
//!           → `ByteStreamServer::inner_write` (chunked path) (bytestream_server.rs)
//!             → `VerifyStore { verify_hash: true }`          (cas_STORE seam)
//!               → `MemoryStore`                              (leaf)
//!
//! The CAS chain seam is `VerifyStore(verify_hash=true)` exactly as the
//! production `cas_STORE` is (per `~/fl/bld/infra/nativelink/prod-server.json5`
//! and MEMORY's verified CAS chain). VerifyStore re-hashes every byte that
//! flows through to the inner store and raises the EXACT production error
//! (`verify_store.rs:174`) if the received bytes do not hash to the announced
//! digest. So if the zero-copy decode path corrupts a single byte, these
//! tests fail with the bespoke `Hashes do not match` message — the production
//! signature.
//!
//! Frame-shape coverage (the cold-build trigger is concurrent uploads whose
//! HTTP/2 DATA frames are split/packed differently than the single-frame
//! happy path the codec unit tests cover):
//!   * one gRPC message, one DATA frame                 (`single_frame`)
//!   * one gRPC message split across N DATA frames       (`split_*`)
//!   * gRPC header straddling a DATA frame boundary      (`header_split`)
//!   * multiple gRPC messages packed into one DATA frame (`packed`)
//!   * byte-at-a-time fragmentation                      (`byte_by_byte`)
//!   * many concurrent uploads over distinct services    (`concurrent`)

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::Frame;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::Store;
use prost::Message;
use tower::Service;

const INSTANCE_NAME: &str = "main";

/// Build a `ByteStreamServer` whose `main_cas` store is the production
/// `VerifyStore(verify_hash=true) → MemoryStore` seam. `verify_hash=true`
/// is what makes a corrupted-byte decode surface as the production
/// `Hashes do not match` error.
fn make_verify_backed_server() -> ByteStreamServer {
    let store_manager = Arc::new(StoreManager::new());
    let mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        mem,
    ));
    store_manager.add_store("main_cas", verify);

    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: "main_cas".to_string(),
            persist_stream_on_disconnect_timeout: 0,
            // Large enough that no per-stream cap interferes with the
            // small-blob payloads (max 16 KiB here).
            max_bytes_per_stream: 64 * 1024 * 1024,
            ..Default::default()
        },
    }];
    ByteStreamServer::new(&config, store_manager.as_ref(), None).expect("server")
}

/// Compute the real SHA-256 digest of `data` (matches the test server's
/// `default_digest_hasher_func`, which is Sha256). The resource_name carries
/// this digest so `VerifyStore` verifies the received bytes against it.
fn sha256_digest(data: &[u8]) -> DigestInfo {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(data);
    let d = hasher.finalize_digest();
    // finalize_digest stamps the size from bytes hashed.
    assert_eq!(d.size_bytes(), data.len() as u64, "hasher size stamp");
    d
}

/// Build the resource_name a Bazel client uses for a blob upload, with an
/// explicit `blobs/{hash}/{size}` (Sha256 default — no digest_function
/// segment so the server uses `default_digest_hasher_func`).
fn upload_resource_name(digest: &DigestInfo) -> String {
    format!(
        "{INSTANCE_NAME}/uploads/4dcec57e-1389-4ab5-b188-4a59f22ceb4b/blobs/{}/{}",
        digest.packed_hash(),
        digest.size_bytes(),
    )
}

/// gRPC-frame-encode a `WriteRequest`: `[0u8][u32 BE len][encoded msg]`.
fn grpc_frame(req: &WriteRequest) -> Bytes {
    let encoded = req.encode_to_vec();
    let mut buf = BytesMut::with_capacity(5 + encoded.len());
    buf.put_u8(0); // no compression
    buf.put_u32(encoded.len() as u32);
    buf.put_slice(&encoded);
    buf.freeze()
}

/// Drive `frames` through the production zero-copy write service and return
/// the resulting grpc-status message (`Ok(())` on grpc-status 0, `Err(msg)`
/// otherwise). `frames` are the raw HTTP/2 DATA-frame payloads in order.
async fn drive_zero_copy_write(
    server: ByteStreamServer,
    frames: Vec<Bytes>,
) -> Result<(), String> {
    // The production tower service: this is exactly what `nativelink.rs`
    // wires for the ByteStream/Write path.
    let mut service = server.into_zero_copy_service(64 * 1024 * 1024, 64 * 1024 * 1024);

    // ChannelBody is an `http_body::Body` that yields the frames we send.
    // Keep `tx` alive past body construction so `is_end_stream()` is false
    // (tonic::body::Body::new shortcuts to empty if the body reports EOF).
    let (tx, body) = ChannelBody::new();
    let http_body = tonic::body::Body::new(body);

    let request = http::Request::builder()
        .method("POST")
        .uri("http://localhost/google.bytestream.ByteStream/Write")
        .header("content-type", "application/grpc")
        .body(http_body)
        .expect("request");

    // Feed frames from a concurrent task so the service polls the body
    // exactly as the h2 layer would feed DATA frames.
    let feeder = tokio::spawn(async move {
        for f in frames {
            if tx.send(Frame::data(f)).await.is_err() {
                break;
            }
        }
        // Dropping tx closes the body (EOF) — the decoder then drains.
        drop(tx);
    });

    let response = service.call(request).await.expect("infallible service");
    feeder.await.expect("feeder");

    // gRPC status can arrive in TWO places:
    //   * the response HEADERS, for a "trailers-only" error response
    //     (`Status::into_http()` puts grpc-status in headers, empty body) —
    //     this is the path for `Hashes do not match`.
    //   * the response body TRAILERS, for a normal streamed response
    //     (success WriteResponse → grpc-status: 0 trailer).
    // Check headers first, then body trailers.
    let (parts, body) = response.into_parts();
    let read_status = |hdrs: &http::HeaderMap| -> Option<(String, String)> {
        let code = hdrs.get("grpc-status").and_then(|v| v.to_str().ok())?;
        let msg = hdrs
            .get("grpc-message")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<no grpc-message>")
            .to_string();
        Some((code.to_string(), msg))
    };

    if let Some((code, msg)) = read_status(&parts.headers) {
        return if code == "0" {
            Ok(())
        } else {
            Err(format!("grpc-status={code}: {msg}"))
        };
    }

    // No status in headers — collect the body and read trailers.
    let collected = body
        .collect()
        .await
        .map_err(|e| format!("body collect failed: {e}"))?;
    let trailers = collected.trailers().cloned().unwrap_or_default();
    match read_status(&trailers) {
        Some((code, _)) if code == "0" => Ok(()),
        Some((code, msg)) => Err(format!("grpc-status={code}: {msg}")),
        // Success WriteResponse body with no explicit status defaults to OK.
        None => Ok(()),
    }
}

/// Deterministic non-trivial payload (so a single flipped/duplicated byte
/// changes the hash). Mimics a small blake3 build-script object output.
fn make_payload(len: usize) -> Bytes {
    let mut v = vec![0u8; len];
    // LCG fill — every byte distinct enough that any aliasing/duplication
    // corrupts the hash.
    let mut state: u32 = 0x1234_5678;
    for b in &mut v {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        *b = (state >> 24) as u8;
    }
    Bytes::from(v)
}

/// Helper: build a single finish_write=true WriteRequest for `payload`.
fn finish_request(payload: &Bytes) -> WriteRequest {
    let digest = sha256_digest(payload);
    WriteRequest {
        resource_name: upload_resource_name(&digest),
        write_offset: 0,
        finish_write: true,
        data: payload.clone(),
    }
}

// ---------------------------------------------------------------------------
// Single message, single DATA frame — the small-blob happy path.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn single_frame_small_blob_verifies() -> Result<(), Box<dyn core::error::Error>> {
    for len in [1usize, 16, 100, 4096, 16_384] {
        let payload = make_payload(len);
        let frame = grpc_frame(&finish_request(&payload));
        let server = make_verify_backed_server();
        let res = tokio::time::timeout(
            core::time::Duration::from_secs(10),
            drive_zero_copy_write(server, vec![frame]),
        )
        .await
        .expect("zero-copy write must not deadlock (single_frame)");
        res.unwrap_or_else(|e| {
            panic!("single_frame len={len}: zero-copy decode corrupted bytes — {e}")
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// One gRPC message split across multiple DATA frames at every boundary.
// This exercises `BufList::copy_to_bytes` SLOW (multi-chunk) path and
// `advance` across chunk boundaries.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn split_message_across_frames_verifies() -> Result<(), Box<dyn core::error::Error>> {
    let payload = make_payload(8192);
    let whole = grpc_frame(&finish_request(&payload));
    // Try splitting the single gRPC frame at every byte boundary, including
    // splits inside the 5-byte header and right at the header/body seam.
    for split in [1usize, 2, 3, 4, 5, 6, 7, 100, whole.len() / 2, whole.len() - 1] {
        if split >= whole.len() {
            continue;
        }
        let part1 = whole.slice(..split);
        let part2 = whole.slice(split..);
        let server = make_verify_backed_server();
        let res = tokio::time::timeout(
            core::time::Duration::from_secs(10),
            drive_zero_copy_write(server, vec![part1, part2]),
        )
        .await
        .expect("zero-copy write must not deadlock (split)");
        res.unwrap_or_else(|e| {
            panic!("split at {split}/{}: zero-copy decode corrupted bytes — {e}", whole.len())
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// gRPC header straddling a frame boundary (1 flag byte in frame 0, the rest
// in frame 1) — exercises `try_decode_next_message` header byte-walk across
// `advance`-popped chunks.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn header_split_across_frames_verifies() -> Result<(), Box<dyn core::error::Error>> {
    let payload = make_payload(2048);
    let whole = grpc_frame(&finish_request(&payload));
    // 5 frames, one header byte each, then the body in a 6th frame.
    let mut frames: Vec<Bytes> = (0..5).map(|i| whole.slice(i..i + 1)).collect();
    frames.push(whole.slice(5..));
    let server = make_verify_backed_server();
    let res = tokio::time::timeout(
        core::time::Duration::from_secs(10),
        drive_zero_copy_write(server, frames),
    )
    .await
    .expect("zero-copy write must not deadlock (header_split)");
    res.unwrap_or_else(|e| panic!("header_split: zero-copy decode corrupted bytes — {e}"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Byte-at-a-time fragmentation of the whole frame — maximal stress on the
// BufList chunk bookkeeping (every chunk is 1 byte).
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn byte_by_byte_frames_verify() -> Result<(), Box<dyn core::error::Error>> {
    let payload = make_payload(512);
    let whole = grpc_frame(&finish_request(&payload));
    let frames: Vec<Bytes> = (0..whole.len()).map(|i| whole.slice(i..i + 1)).collect();
    let server = make_verify_backed_server();
    let res = tokio::time::timeout(
        core::time::Duration::from_secs(15),
        drive_zero_copy_write(server, frames),
    )
    .await
    .expect("zero-copy write must not deadlock (byte_by_byte)");
    res.unwrap_or_else(|e| panic!("byte_by_byte: zero-copy decode corrupted bytes — {e}"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Two gRPC messages (two data chunks of one blob) packed into a SINGLE DATA
// frame. Exercises `copy_to_bytes` fast-path running TWICE against the same
// backing `Bytes` allocation (`split_to` then `split_to`), which is the prime
// aliasing-suspect interaction when concurrent cold-build uploads coalesce
// multiple WriteRequests into one HTTP/2 frame.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn two_messages_packed_one_frame_verifies() -> Result<(), Box<dyn core::error::Error>> {
    let payload = make_payload(8192);
    let digest = sha256_digest(&payload);
    let resource_name = upload_resource_name(&digest);
    let split = 3000usize;
    let chunk1 = WriteRequest {
        resource_name: resource_name.clone(),
        write_offset: 0,
        finish_write: false,
        data: payload.slice(..split),
    };
    let chunk2 = WriteRequest {
        resource_name,
        write_offset: split as i64,
        finish_write: true,
        data: payload.slice(split..),
    };
    // Pack both gRPC frames into ONE HTTP/2 DATA frame.
    let mut packed = BytesMut::new();
    packed.extend_from_slice(&grpc_frame(&chunk1));
    packed.extend_from_slice(&grpc_frame(&chunk2));
    let server = make_verify_backed_server();
    let res = tokio::time::timeout(
        core::time::Duration::from_secs(10),
        drive_zero_copy_write(server, vec![packed.freeze()]),
    )
    .await
    .expect("zero-copy write must not deadlock (packed)");
    res.unwrap_or_else(|e| panic!("two_messages_packed: zero-copy decode corrupted bytes — {e}"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Many concurrent uploads over distinct services sharing one store manager
// shape — mirrors the cold-build "~6 concurrent small-blob uploads". Each
// upload has DISTINCT content so a cross-upload byte mix-up corrupts a hash.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn concurrent_distinct_small_blobs_verify() -> Result<(), Box<dyn core::error::Error>> {
    let mut handles = Vec::new();
    for seed in 0..16u32 {
        let server = make_verify_backed_server();
        let handle = tokio::spawn(async move {
            // Distinct payload per upload.
            let mut v = vec![0u8; 8192];
            let mut state = 0x9E37_79B9u32 ^ seed.wrapping_mul(2_654_435_761);
            for b in &mut v {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                *b = (state >> 24) as u8;
            }
            let payload = Bytes::from(v);
            let frame = grpc_frame(&finish_request(&payload));
            tokio::time::timeout(
                core::time::Duration::from_secs(15),
                drive_zero_copy_write(server, vec![frame]),
            )
            .await
            .expect("zero-copy write must not deadlock (concurrent)")
        });
        handles.push((seed, handle));
    }
    for (seed, h) in handles {
        let res = h.await.expect("join");
        res.unwrap_or_else(|e| panic!("concurrent seed={seed}: corrupted bytes — {e}"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Sanity: a DELIBERATELY corrupted payload (announced digest of correct
// bytes, but a single body byte flipped on the wire) MUST be rejected with
// the production `Hashes do not match` message. This proves the VerifyStore
// seam is actually live in this harness — i.e. a green test above is a real
// pass, not a dead verify layer (guards the "test tests dead code" trap).
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn corrupted_byte_is_rejected_by_verify_seam() -> Result<(), Box<dyn core::error::Error>> {
    let payload = make_payload(4096);
    // Announce the digest of the CORRECT payload.
    let mut req = finish_request(&payload);
    // Flip one body byte so the received bytes no longer match the digest.
    let mut corrupt = payload.to_vec();
    corrupt[123] ^= 0xFF;
    req.data = Bytes::from(corrupt);
    let frame = grpc_frame(&req);
    let server = make_verify_backed_server();
    let res = tokio::time::timeout(
        core::time::Duration::from_secs(10),
        drive_zero_copy_write(server, vec![frame]),
    )
    .await
    .expect("must not deadlock (corrupted_byte sanity)");
    let err = res.expect_err(
        "VerifyStore seam must REJECT a corrupted byte — if this succeeds, the \
         verify layer is dead and every green test in this file is meaningless",
    );
    // grpc-message in trailers-only error headers is percent-encoded per the
    // gRPC spec (`Hashes%20do%20not%20match`), so match the decoded form.
    let decoded = err.replace("%20", " ");
    assert!(
        decoded.contains("Hashes do not match") && err.contains("grpc-status=3"),
        "expected production INVALID_ARGUMENT 'Hashes do not match' signature, got: {err}"
    );
    Ok(())
}

/// Build a `ByteStreamServer` whose `main_cas` store is the FULL production
/// CAS composition (per `prod-server.json5` / MEMORY's verified CAS chain),
/// minus only the outermost `WorkerProxyStore` (which is a mirror/ack-gate
/// wrapper that does not touch the byte content the VerifyStore hashes):
///
///   `VerifyStore(verify_hash=true) → ExistenceCacheStore
///     → SizePartitioningStore(16_384) → FastSlowStore(Memory fast, Memory slow)`
///
/// The slow tier is a `MemoryStore` rather than the production
/// `FilesystemStore` because the OUTER VerifyStore hashes every byte BEFORE
/// it reaches FSS — the production `Failed to read buffer in fastslow store`
/// is a DERIVATIVE of the hash mismatch (FSS's `reader.recv()` fails after
/// VerifyStore's `check_fut` errors and drops the tx), not its cause. So the
/// FSS-leaf type does not change what VerifyStore hashes; using Memory keeps
/// the test hermetic (no temp dir) while exercising the exact FSS
/// `chunks.push(buffer.clone())` background-slow-write + size-partition
/// routing the production large-arm uses.
fn make_production_chain_server() -> ByteStreamServer {
    use nativelink_config::stores::{
        ExistenceCacheSpec, FastSlowSpec, SizePartitioningSpec, StoreDirection,
    };
    use nativelink_store::existence_cache_store::ExistenceCacheStore;
    use nativelink_store::fast_slow_store::FastSlowStore;
    use nativelink_store::size_partitioning_store::SizePartitioningStore;

    let store_manager = Arc::new(StoreManager::new());

    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast,
        slow,
    ));
    // Lower arm (≤ 16_384) of the size-partition: a Memory leaf, matching
    // the production small-blob lower arm shape.
    let lower = Store::new(MemoryStore::new(&MemorySpec::default()));
    let sp = Store::new(SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: 16_384,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        lower,
        fss,
    ));
    let ecs = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        sp,
    ));
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        ecs,
    ));
    store_manager.add_store("main_cas", verify);

    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: "main_cas".to_string(),
            persist_stream_on_disconnect_timeout: 0,
            max_bytes_per_stream: 64 * 1024 * 1024,
            ..Default::default()
        },
    }];
    ByteStreamServer::new(&config, store_manager.as_ref(), None).expect("server")
}

// ---------------------------------------------------------------------------
// FULL production CAS composition, concurrent uploads of MIXED sizes spanning
// the size-partition boundary (small arm ≤16 KiB AND FastSlow large arm
// >16 KiB — the arm the production `Failed to read buffer in fastslow store`
// error names). Each upload has DISTINCT content + frame fragmentation.
// This is the closest hermetic reproduction of the cold-build concurrent
// re-upload load against the real store chain.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn production_chain_concurrent_mixed_sizes_verify()
-> Result<(), Box<dyn core::error::Error>> {
    let mut handles = Vec::new();
    for seed in 0..24u32 {
        let handle = tokio::spawn(async move {
            // Sizes straddle the 16_384 size-partition boundary so both the
            // small (Memory) arm and the FastSlow large arm are exercised.
            let len = match seed % 4 {
                0 => 100usize,         // tiny → small arm
                1 => 16_000,           // just under boundary → small arm
                2 => 20_000,           // just over boundary → FastSlow large arm
                _ => 200_000,          // multi-frame → FastSlow large arm
            };
            let mut v = vec![0u8; len];
            let mut state = 0xDEAD_BEEFu32 ^ seed.wrapping_mul(2_246_822_519);
            for b in &mut v {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                *b = (state >> 24) as u8;
            }
            let payload = Bytes::from(v);
            let whole = grpc_frame(&finish_request(&payload));
            // Fragment every upload into 3 frames to exercise BufList
            // multi-chunk assembly under the real chain.
            let n = whole.len();
            let frames = if n >= 3 {
                vec![
                    whole.slice(..n / 3),
                    whole.slice(n / 3..2 * n / 3),
                    whole.slice(2 * n / 3..),
                ]
            } else {
                vec![whole]
            };
            let server = make_production_chain_server();
            tokio::time::timeout(
                core::time::Duration::from_secs(20),
                drive_zero_copy_write(server, frames),
            )
            .await
            .expect("zero-copy write must not deadlock (production_chain)")
        });
        handles.push((seed, handle));
    }
    for (seed, h) in handles {
        let res = h.await.expect("join");
        res.unwrap_or_else(|e| {
            panic!("production_chain seed={seed}: store chain corrupted bytes — {e}")
        });
    }
    Ok(())
}
