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

//! Integration test: upload and download a large blob (107 MB, same size as
//! libjpegxl.a in dbg mode) to a real NativeLink server over TCP.
//!
//! Reproduces the production failure where ByteStream.Write for a 107 MB blob
//! received zero bytes — the upload stream opened but no data arrived.
//! The test confirms the server can accept and serve large chunked uploads.
//!
//! Run against the live server:
//!
//!   NATIVELINK_ENDPOINT=https://cache.example.com:50051 \
//!   NATIVELINK_TLS_CERT=/path/to/client.crt \
//!   NATIVELINK_TLS_KEY=/path/to/client.key \
//!     cargo test -p nativelink-service --test large_blob_upload_test -- --ignored --nocapture

use std::time::Instant;

use bytes::Bytes;
use futures::stream;
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::{ReadRequest, WriteRequest};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig, Identity};

const INSTANCE_NAME: &str = "main";
const BLOB_SIZE: usize = 107_189_840; // 107 MB — same as libjpegxl.a
const CHUNK_SIZE_2MB: usize = 2 * 1024 * 1024;
const CHUNK_SIZE_16KB: usize = 16 * 1024;

fn generate_blob(size: usize) -> Vec<u8> {
    let pattern = b"LIBJPEGXL_TEST_BLOB_107MB_UPLOAD_INTEGRATION_TEST_DATA__";
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        let to_copy = remaining.min(pattern.len());
        data.extend_from_slice(&pattern[..to_copy]);
    }
    data
}

fn blake3_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

fn get_endpoint() -> String {
    std::env::var("NATIVELINK_ENDPOINT")
        .unwrap_or_else(|_| "https://cache.example.com:50051".to_string())
}

async fn connect(endpoint: &str) -> Result<ByteStreamClient<Channel>, Box<dyn std::error::Error>> {
    let mut builder = Channel::from_shared(endpoint.to_string())?
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(600));

    if endpoint.starts_with("https://") {
        let mut tls = ClientTlsConfig::new().with_native_roots();
        if let (Ok(cert), Ok(key)) = (
            std::env::var("NATIVELINK_TLS_CERT"),
            std::env::var("NATIVELINK_TLS_KEY"),
        ) {
            let cert_pem = tokio::fs::read(&cert).await?;
            let key_pem = tokio::fs::read(&key).await?;
            tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
        }
        builder = builder.tls_config(tls)?;
    }

    let channel = builder.connect().await?;
    Ok(ByteStreamClient::new(channel)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024))
}

async fn upload_blob(
    client: &mut ByteStreamClient<Channel>,
    blob: &[u8],
    hash: &str,
    chunk_size: usize,
) -> Result<(i64, std::time::Duration), Box<dyn std::error::Error>> {
    let uuid = format!("test-{:016x}", rand_u64());
    let resource_name = format!(
        "{INSTANCE_NAME}/uploads/{uuid}/blobs/blake3/{hash}/{size}",
        size = blob.len(),
    );

    let blob_len = blob.len();
    let blob_bytes = Bytes::from(blob.to_vec());

    // Build all WriteRequest messages up front.
    let mut requests = Vec::new();
    let mut offset = 0usize;
    while offset < blob_len {
        let end = (offset + chunk_size).min(blob_len);
        let is_last = end == blob_len;
        requests.push(WriteRequest {
            resource_name: if offset == 0 {
                resource_name.clone()
            } else {
                String::new()
            },
            write_offset: offset as i64,
            finish_write: is_last,
            data: blob_bytes.slice(offset..end),
        });
        offset = end;
    }

    let num_chunks = requests.len();
    let request_stream = stream::iter(requests);

    let start = Instant::now();
    let response = client.write(tonic::Request::new(request_stream)).await?;
    let elapsed = start.elapsed();

    let committed = response.into_inner().committed_size;
    eprintln!(
        "  Upload: {num_chunks} chunks, {committed} bytes committed, {elapsed:?} ({:.1} MB/s)",
        (blob_len as f64 / 1_048_576.0) / elapsed.as_secs_f64()
    );

    Ok((committed, elapsed))
}

async fn download_blob(
    client: &mut ByteStreamClient<Channel>,
    hash: &str,
    expected_size: usize,
) -> Result<(Vec<u8>, std::time::Duration), Box<dyn std::error::Error>> {
    let resource_name = format!("{INSTANCE_NAME}/blobs/blake3/{hash}/{expected_size}");

    let start = Instant::now();
    let response = client
        .read(tonic::Request::new(ReadRequest {
            resource_name,
            read_offset: 0,
            read_limit: 0,
        }))
        .await?;

    let mut read_stream = response.into_inner();
    let mut data = Vec::with_capacity(expected_size);
    while let Some(chunk) = read_stream.next().await {
        data.extend_from_slice(&chunk?.data);
    }
    let elapsed = start.elapsed();

    eprintln!(
        "  Download: {} bytes, {elapsed:?} ({:.1} MB/s)",
        data.len(),
        (data.len() as f64 / 1_048_576.0) / elapsed.as_secs_f64()
    );

    Ok((data, elapsed))
}

/// Simple random u64 for UUID generation (avoids adding rand dep).
fn rand_u64() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    h.finish()
}

/// Upload and download 107 MB over HTTP/2 with 2 MiB chunks.
#[tokio::test]
#[ignore]
async fn large_blob_upload_http2_2mb_chunks() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = get_endpoint();
    eprintln!("\n=== large_blob_upload_http2_2mb_chunks ===");
    eprintln!("Endpoint: {endpoint}");
    eprintln!("Blob: {BLOB_SIZE} bytes, chunk: {CHUNK_SIZE_2MB} bytes");

    let blob = generate_blob(BLOB_SIZE);
    let hash = blake3_hex(&blob);
    eprintln!("Blake3: {hash}");

    let mut client = connect(&endpoint).await?;

    let (committed, _) = upload_blob(&mut client, &blob, &hash, CHUNK_SIZE_2MB).await?;
    assert_eq!(committed, BLOB_SIZE as i64, "committed_size mismatch");

    let (downloaded, _) = download_blob(&mut client, &hash, BLOB_SIZE).await?;
    assert_eq!(downloaded.len(), BLOB_SIZE, "download size mismatch");
    assert_eq!(downloaded, blob, "download data mismatch");

    eprintln!("=== PASSED ===\n");
    Ok(())
}

/// Upload 107 MB with 16 KB chunks (Bazel's stock default).
#[tokio::test]
#[ignore]
async fn large_blob_upload_http2_16kb_chunks() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = get_endpoint();
    eprintln!("\n=== large_blob_upload_http2_16kb_chunks ===");
    eprintln!("Endpoint: {endpoint}");
    eprintln!("Blob: {BLOB_SIZE} bytes, chunk: {CHUNK_SIZE_16KB} bytes");

    let blob = generate_blob(BLOB_SIZE);
    let hash = blake3_hex(&blob);

    let mut client = connect(&endpoint).await?;

    let (committed, _) = upload_blob(&mut client, &blob, &hash, CHUNK_SIZE_16KB).await?;
    assert_eq!(committed, BLOB_SIZE as i64, "committed_size mismatch");

    eprintln!("=== PASSED ===\n");
    Ok(())
}
