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

//! Stress test to reproduce download truncation bug.
//!
//! Uploads DIFFERENT blob data each iteration (varying seed) so each upload
//! is fresh and actually writes to the store (no `has()` cache fast-path).
//!
//!   NATIVELINK_ENDPOINT=https://cache.example.com:50051 \
//!   NATIVELINK_TLS_CERT=/home/user/casdata/bazel/nativelink-client.crt \
//!   NATIVELINK_TLS_KEY=/home/user/casdata/bazel/nativelink-client.key \
//!     cargo test -p nativelink-service --test truncation_repro_test -- --ignored --nocapture

use std::time::Instant;

use bytes::Bytes;
use futures::stream;
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::{ReadRequest, WriteRequest};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig, Identity};

const INSTANCE_NAME: &str = "main";
const CHUNK_SIZE_2MB: usize = 2 * 1024 * 1024;

/// Generate a blob with a seed-dependent pattern so each iteration is unique.
fn generate_blob(size: usize, seed: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    // Simple LCG-based pattern — different seed = different data = different hash.
    let mut state = seed;
    while data.len() < size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let bytes = state.to_le_bytes();
        let remaining = size - data.len();
        let to_copy = remaining.min(bytes.len());
        data.extend_from_slice(&bytes[..to_copy]);
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

/// Simple random u64 for UUID generation.
fn rand_u64() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    h.finish()
}

async fn upload_blob(
    client: &mut ByteStreamClient<Channel>,
    blob: &[u8],
    hash: &str,
    chunk_size: usize,
) -> Result<i64, Box<dyn std::error::Error>> {
    let uuid = format!("test-{:016x}", rand_u64());
    let resource_name = format!(
        "{INSTANCE_NAME}/uploads/{uuid}/blobs/blake3/{hash}/{size}",
        size = blob.len(),
    );

    let blob_len = blob.len();
    let blob_bytes = Bytes::from(blob.to_vec());

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

    let request_stream = stream::iter(requests);
    let response = client.write(tonic::Request::new(request_stream)).await?;
    let committed = response.into_inner().committed_size;
    Ok(committed)
}

async fn download_blob(
    client: &mut ByteStreamClient<Channel>,
    hash: &str,
    expected_size: usize,
) -> Result<(usize, Option<String>), Box<dyn std::error::Error>> {
    let resource_name = format!("{INSTANCE_NAME}/blobs/blake3/{hash}/{expected_size}");

    let response = client
        .read(tonic::Request::new(ReadRequest {
            resource_name,
            read_offset: 0,
            read_limit: 0,
        }))
        .await?;

    let mut read_stream = response.into_inner();
    let mut data_len = 0usize;
    let mut chunk_count = 0u32;
    let mut stream_error = None;
    loop {
        match read_stream.next().await {
            Some(Ok(chunk)) => {
                data_len += chunk.data.len();
                chunk_count += 1;
            }
            Some(Err(e)) => {
                stream_error = Some(format!("gRPC stream error after {data_len} bytes, {chunk_count} chunks: {e:?}"));
                break;
            }
            None => {
                break; // Stream ended
            }
        }
    }
    if let Some(ref err) = stream_error {
        eprintln!("  STREAM ERROR: {err}");
    }
    Ok((data_len, stream_error))
}

/// Run N iterations of upload+download with different data each time.
/// Reports any truncation or error.
#[tokio::test]
#[ignore]
async fn truncation_repro_loop() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = get_endpoint();
    let iterations: usize = std::env::var("ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let base_size: usize = std::env::var("BLOB_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(107_189_840);

    eprintln!("\n=== truncation_repro_loop ===");
    eprintln!("Endpoint: {endpoint}");
    eprintln!("Iterations: {iterations}");
    eprintln!("Base blob size: {base_size} bytes");

    let mut client = connect(&endpoint).await?;

    let mut failures = Vec::new();

    for i in 0..iterations {
        // Vary size slightly each iteration (+0..+4095 bytes) to guarantee unique hashes.
        // Use process start time as a base to guarantee unique seeds across runs.
        // This ensures each run uploads genuinely new blobs (not hitting has() cache).
        let unique_base: u64 = {
            use std::sync::OnceLock;
            static BASE: OnceLock<u64> = OnceLock::new();
            *BASE.get_or_init(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            })
        };
        let seed = (i as u64).wrapping_mul(0xDEADBEEF_CAFEBABE) ^ unique_base;
        let size_delta = (seed % 4096) as usize;
        let blob_size = base_size + size_delta;

        let blob = generate_blob(blob_size, seed);
        let hash = blake3_hex(&blob);

        eprintln!("\n--- Iteration {}/{iterations} ---", i + 1);
        eprintln!("  Seed: {seed:#018x}, Size: {blob_size}, Hash: {hash}");

        // Upload
        let upload_start = Instant::now();
        let committed = upload_blob(&mut client, &blob, &hash, CHUNK_SIZE_2MB).await?;
        let upload_elapsed = upload_start.elapsed();
        eprintln!(
            "  Upload: {committed} bytes committed, {upload_elapsed:?} ({:.1} MB/s)",
            (blob_size as f64 / 1_048_576.0) / upload_elapsed.as_secs_f64()
        );

        if committed != blob_size as i64 {
            let iter = i + 1;
            let msg = format!(
                "Iteration {iter}: committed_size mismatch: got {committed}, expected {blob_size}"
            );
            eprintln!("  FAILURE: {msg}");
            failures.push(msg);
            continue;
        }

        // Wait for the in-flight streaming blob to be removed from InFlightBlobMap
        // (5-second grace period after upload). This forces the download to read
        // from the store rather than the streaming blob buffer.
        let grace_wait = std::env::var("GRACE_WAIT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        if grace_wait > 0 {
            eprintln!("  Waiting {grace_wait}s for streaming blob grace period...");
            tokio::time::sleep(std::time::Duration::from_secs(grace_wait)).await;
        }

        // Download
        let download_start = Instant::now();
        let (downloaded_len, stream_error) =
            download_blob(&mut client, &hash, blob_size).await?;
        let download_elapsed = download_start.elapsed();
        eprintln!(
            "  Download: {downloaded_len} bytes, {download_elapsed:?} ({:.1} MB/s)",
            (downloaded_len as f64 / 1_048_576.0) / download_elapsed.as_secs_f64()
        );

        if downloaded_len != blob_size {
            let delta = blob_size as i64 - downloaded_len as i64;
            let pct = (downloaded_len as f64 / blob_size as f64) * 100.0;
            let mib_received = downloaded_len as f64 / (1024.0 * 1024.0);
            let iter = i + 1;
            let msg = format!(
                "Iteration {iter}: TRUNCATION: got {downloaded_len} bytes ({mib_received:.2} MiB, {pct:.2}%), \
                 expected {blob_size}, delta={delta}, stream_error={stream_error:?}"
            );
            eprintln!("  FAILURE: {msg}");
            failures.push(msg);
        } else if let Some(err) = stream_error {
            let iter = i + 1;
            let msg = format!("Iteration {iter}: stream error but full data received: {err}");
            eprintln!("  WARNING: {msg}");
        } else {
            eprintln!("  OK");
        }
    }

    eprintln!("\n=== Summary ===");
    eprintln!("Iterations: {iterations}");
    eprintln!("Failures: {}", failures.len());
    for f in &failures {
        eprintln!("  - {f}");
    }

    assert!(
        failures.is_empty(),
        "Had {} truncation/error failures out of {iterations} iterations:\n{}",
        failures.len(),
        failures.join("\n")
    );

    eprintln!("=== ALL PASSED ===\n");
    Ok(())
}

/// Run concurrent downloads of the same blob to stress-test the read path.
#[tokio::test]
#[ignore]
async fn concurrent_download_stress() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = get_endpoint();
    let blob_size: usize = 107_189_840;
    let concurrent_readers: usize = std::env::var("CONCURRENT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    eprintln!("\n=== concurrent_download_stress ===");
    eprintln!("Endpoint: {endpoint}");
    eprintln!("Blob size: {blob_size}");
    eprintln!("Concurrent readers: {concurrent_readers}");

    // Upload a unique blob first
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let blob = generate_blob(blob_size, seed);
    let hash = blake3_hex(&blob);
    eprintln!("  Hash: {hash}");

    let mut client = connect(&endpoint).await?;
    let committed = upload_blob(&mut client, &blob, &hash, CHUNK_SIZE_2MB).await?;
    assert_eq!(committed, blob_size as i64);
    eprintln!("  Upload complete");

    // Now download concurrently
    let mut handles = Vec::new();
    for reader_id in 0..concurrent_readers {
        let endpoint = endpoint.clone();
        let hash = hash.clone();
        handles.push(tokio::spawn(async move {
            let mut client = connect(&endpoint).await.unwrap();
            let (downloaded_len, stream_error) =
                download_blob(&mut client, &hash, blob_size).await.unwrap();
            (reader_id, downloaded_len, stream_error)
        }));
    }

    let mut failures = Vec::new();
    for handle in handles {
        let (reader_id, downloaded_len, stream_error) = handle.await?;
        eprintln!(
            "  Reader {reader_id}: {downloaded_len} bytes, error={stream_error:?}"
        );
        if downloaded_len != blob_size {
            failures.push(format!(
                "Reader {reader_id}: got {downloaded_len}, expected {blob_size}, error={stream_error:?}"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "Concurrent download failures:\n{}",
        failures.join("\n")
    );

    eprintln!("=== ALL PASSED ===\n");
    Ok(())
}
