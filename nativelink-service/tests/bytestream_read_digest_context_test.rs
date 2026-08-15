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

//! ByteStream READ must hash-verify with the digest function the CLIENT named,
//! not with the process-wide default.
//!
//! `ByteStreamServer::read` and `ByteStreamServer::zero_copy_read` both resolve
//! the client's digest function from the resource name and call `inner_read`
//! inside `.with_context(make_ctx_for_hash_func(..))`
//! (`bytestream_server.rs:4316`, `:4457`). But `inner_read` does not *do* the
//! read — it returns a `Stream`, and the `get_part_fut` it built is only POLLED
//! later, when tonic (or `ZeroCopyReadBody`) drives that stream. By then the
//! `.with_context` scope has been dropped, so `VerifyStore::get_part`'s
//! `Context::current().get::<DigestHasherFunc>()`
//! (`verify_store.rs:411-413`) finds nothing and falls back to
//! `default_digest_hasher_func()`.
//!
//! **Production composition.** Every test pins the process-global default to
//! BLAKE3, exactly as the deployed `global.default_digest_hash_function` does
//! in `~/fl/bld/infra/nativelink/{buildcache,worker}.json5` and the live
//! `/srv/nativelink/buildcache-native.json5`. That pin is what makes the bug
//! observable at all: with the test-harness default of SHA-256 the dropped
//! context is invisible, because the fallback happens to equal what the client
//! asked for. The fleet is in exactly that lucky configuration in the other
//! direction (default blake3 == what every client sends), which is why this has
//! never bitten in production — a coincidence of configuration, not a
//! correctness property.
//!
//! The CAS chain is `VerifyStore { verify_hash, verify_size } -> MemoryStore`,
//! matching production's `cas_STORE` seam: `VerifyStore` is the layer that
//! re-hashes the served bytes and raises `Hash mismatch on read`.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::Frame;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_proto::google::bytestream::{ReadRequest, ReadResponse};
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasher, DigestHasherFunc, default_digest_hasher_func, set_default_digest_hasher_func,
};
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use tokio_stream::StreamExt;
use tonic::Request;
use tower::Service;

const INSTANCE_NAME: &str = "main";
const DEADLINE: core::time::Duration = core::time::Duration::from_secs(20);

/// Pin the process-global default digest function to BLAKE3, exactly as the
/// deployed `global.default_digest_hash_function` does on buildcache and on every
/// worker.
///
/// `DEFAULT_DIGEST_HASHER_FUNC` is a `OnceLock` whose `get_or_init` fallback is
/// SHA-256, so this MUST run before anything in the binary calls
/// `default_digest_hasher_func()`. The post-condition assert is load-bearing:
/// if some other test in this binary won the race with a different value we
/// fail loudly, instead of silently testing the SHA-256-default shape — in
/// which case a dropped digest-function context would be undetectable, because
/// the fallback would coincide with what the client asked for.
fn pin_production_blake3_default() {
    drop(set_default_digest_hasher_func(DigestHasherFunc::Blake3));
    assert_eq!(
        default_digest_hasher_func(),
        DigestHasherFunc::Blake3,
        "test harness must run with the deployed blake3 default; with the \
         SHA-256 harness default a dropped digest-function context is \
         invisible because the fallback coincides with the client's request"
    );
}

fn digest_of(func: DigestHasherFunc, data: &[u8]) -> DigestInfo {
    let mut hasher = func.hasher();
    hasher.update(data);
    hasher.finalize_digest()
}

/// Returns the server plus a DIRECT handle to the leaf `MemoryStore` behind
/// `VerifyStore`.
///
/// Tests seed the LEAF, not `VerifyStore`: a test-side `VerifyStore` write
/// carries no request context, so it would hash with the process default
/// (blake3 here) and reject a SHA-256-keyed blob for reasons that have nothing
/// to do with the code under test. Seeding the leaf models "the blob is at rest
/// in the CAS" and keeps the assertion pointed at the server's propagation of
/// the client's digest function.
fn make_server() -> (ByteStreamServer, Store) {
    let store_manager = Arc::new(StoreManager::new());
    let mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        mem.clone(),
    ));
    store_manager.add_store("main_cas", verify);

    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: "main_cas".to_string(),
            persist_stream_on_disconnect_timeout_s: 0,
            max_bytes_per_stream: 64 * 1024 * 1024,
            ..Default::default()
        },
    }];
    (
        ByteStreamServer::new(&config, store_manager.as_ref(), None).expect("server"),
        mem,
    )
}

/// Read resource name WITH an explicit `{digest_function}` segment.
fn read_name_explicit(func: &str, digest: &DigestInfo) -> String {
    format!(
        "{INSTANCE_NAME}/blobs/{func}/{}/{}",
        digest.packed_hash(),
        digest.size_bytes(),
    )
}

/// `grpc-message` is percent-encoded on the wire (tonic encodes it per the gRPC
/// spec), so `Hash mismatch on read` arrives as `Hash%20mismatch%20on%20read`.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(b);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn grpc_frame<M: Message>(msg: &M) -> Bytes {
    let encoded = msg.encode_to_vec();
    let mut buf = BytesMut::with_capacity(5 + encoded.len());
    buf.put_u8(0);
    buf.put_u32(u32::try_from(encoded.len()).expect("frame fits u32"));
    buf.put_slice(&encoded);
    buf.freeze()
}

/// Concatenate the payloads of a run of length-prefixed `ReadResponse` frames.
fn decode_read_frames(mut buf: Bytes) -> Vec<u8> {
    let mut out = Vec::new();
    while buf.len() >= 5 {
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert!(
            buf.len() >= 5 + len,
            "truncated grpc frame: need {} bytes, have {}",
            5 + len,
            buf.len()
        );
        let resp = ReadResponse::decode(buf.slice(5..5 + len)).expect("decode ReadResponse");
        out.extend_from_slice(&resp.data);
        buf = buf.slice(5 + len..);
    }
    out
}

/// Drive the PRODUCTION read entry point: `ZeroCopyByteStreamService::call`
/// (installed on the CAS/ByteStream service in `src/bin/nativelink.rs:2158`)
/// -> `ByteStreamServer::zero_copy_read` -> `inner_read` -> `VerifyStore` ->
/// `MemoryStore`, with the response body consumed as real gRPC frames.
async fn drive_zero_copy_read(
    server: ByteStreamServer,
    resource_name: String,
) -> Result<Vec<u8>, String> {
    let read_request = ReadRequest {
        resource_name,
        read_offset: 0,
        read_limit: 0,
    };
    let mut service = server.into_zero_copy_service(64 * 1024 * 1024, 64 * 1024 * 1024);

    let (tx, body) = ChannelBody::new();
    let request = http::Request::builder()
        .method("POST")
        .uri("http://localhost/google.bytestream.ByteStream/Read")
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::new(body))
        .expect("request");

    let feeder = tokio::spawn(async move {
        drop(tx.send(Frame::data(grpc_frame(&read_request))).await);
        drop(tx);
    });

    let response = tokio::time::timeout(DEADLINE, service.call(request))
        .await
        .expect("deadlock: zero-copy read service did not respond within 20s")
        .expect("infallible service");
    feeder.await.expect("feeder");

    let (parts, body) = response.into_parts();
    if let Some(status) = parts.headers.get("grpc-status")
        && status.as_bytes() != b"0"
    {
        return Err(parts
            .headers
            .get("grpc-message")
            .map_or_else(String::new, |m| {
                percent_decode(&String::from_utf8_lossy(m.as_bytes()))
            }));
    }

    let collected = tokio::time::timeout(DEADLINE, body.collect())
        .await
        .expect("deadlock: zero-copy read body did not complete within 20s")
        .expect("body");

    let trailer_err = collected.trailers().and_then(|trailers| {
        let status = trailers.get("grpc-status")?;
        (status.as_bytes() != b"0").then(|| {
            trailers.get("grpc-message").map_or_else(String::new, |m| {
                percent_decode(&String::from_utf8_lossy(m.as_bytes()))
            })
        })
    });
    if let Some(err) = trailer_err {
        return Err(err);
    }

    Ok(decode_read_frames(collected.to_bytes()))
}

/// Drive the tonic `ByteStream::read` entry point, which shares `inner_read`
/// with `zero_copy_read` and carries the identical
/// `.with_context(..).await`-a-stream-factory shape at
/// `bytestream_server.rs:4457`.
async fn drive_tonic_read(
    server: &ByteStreamServer,
    resource_name: String,
) -> Result<Vec<u8>, String> {
    let response = server
        .read(Request::new(ReadRequest {
            resource_name,
            read_offset: 0,
            read_limit: 0,
        }))
        .await
        .map_err(|s| s.message().to_string())?;
    let mut stream = response.into_inner();
    let mut out = Vec::new();
    loop {
        let next = tokio::time::timeout(DEADLINE, stream.next())
            .await
            .expect("deadlock: tonic read stream stalled >20s");
        match next {
            Some(Ok(chunk)) => out.extend_from_slice(&chunk.data),
            Some(Err(s)) => return Err(s.message().to_string()),
            None => break,
        }
    }
    Ok(out)
}

/// THE BUG, on the production read entry point. The client names `sha256`
/// explicitly in the resource name; the blob at rest is SHA-256-keyed. The
/// server must hash-verify with SHA-256. Before the fix it verified with the
/// process default (blake3), computed a blake3 hash of correct bytes, compared
/// it against a SHA-256 key, and reported the blob as corrupt.
#[nativelink_test]
async fn explicit_sha256_zero_copy_read_verifies_with_client_digest_function() {
    pin_production_blake3_default();
    let data = b"read-side digest-function context, zero-copy entry point";
    let digest = digest_of(DigestHasherFunc::Sha256, data);
    let (server, leaf) = make_server();
    leaf.update_oneshot(digest, Bytes::copy_from_slice(data))
        .await
        .expect("seed blob into the leaf MemoryStore");

    let got = drive_zero_copy_read(server, read_name_explicit("sha256", &digest)).await;
    assert_eq!(
        got,
        Ok(data.to_vec()),
        "read must hash-verify with the digest function the CLIENT named. \
         `zero_copy_read` resolves it and calls `inner_read` inside \
         `.with_context(make_ctx_for_hash_func(..))`, but `inner_read` only \
         BUILDS `get_part_fut`; the stream POLLS it after that scope is gone, \
         so `VerifyStore::get_part` reads an empty `Context::current()` and \
         falls back to the process default (blake3 here, as deployed)"
    );
}

/// Same bug, tonic `read` entry point — the second call site of `inner_read`.
/// Pinned separately so a future refactor that fixes only one entry point
/// cannot go unnoticed.
#[nativelink_test]
async fn explicit_sha256_tonic_read_verifies_with_client_digest_function() {
    pin_production_blake3_default();
    let data = b"read-side digest-function context, tonic entry point";
    let digest = digest_of(DigestHasherFunc::Sha256, data);
    let (server, leaf) = make_server();
    leaf.update_oneshot(digest, Bytes::copy_from_slice(data))
        .await
        .expect("seed blob into the leaf MemoryStore");

    let got = drive_tonic_read(&server, read_name_explicit("sha256", &digest)).await;
    assert_eq!(
        got,
        Ok(data.to_vec()),
        "tonic `read` shares `inner_read` with `zero_copy_read` and drops the \
         digest-function context the same way: the context guard dies when \
         `inner_read`'s future resolves, long before the returned stream polls \
         `get_part_fut`"
    );
}

/// The fleet's actual traffic: an explicit `blake3` segment, which coincides
/// with the deployed default. Green both before and after the fix — it is here
/// to prove the fix does not disturb the path 100% of production takes, and to
/// prove the harness is not simply rejecting every read.
#[nativelink_test]
async fn explicit_blake3_zero_copy_read_is_unchanged() {
    pin_production_blake3_default();
    let data = b"the digest function every deployed client actually sends";
    let digest = digest_of(DigestHasherFunc::Blake3, data);
    let (server, leaf) = make_server();
    leaf.update_oneshot(digest, Bytes::copy_from_slice(data))
        .await
        .expect("seed blob into the leaf MemoryStore");

    let got = drive_zero_copy_read(server, read_name_explicit("blake3", &digest)).await;
    assert_eq!(
        got,
        Ok(data.to_vec()),
        "regression guard: an explicit blake3 read is what the deployed fleet \
         sends on every request and must keep working byte-for-byte"
    );
}
