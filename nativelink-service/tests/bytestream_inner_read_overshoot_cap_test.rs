// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! #44 Layer C — `bytestream_server.rs:inner_read` unfold MUST cap the
//! emitted stream at `digest.size_bytes()` even if the underlying
//! `StreamingBlobInner` somehow holds more than `expected_size` bytes
//! (e.g. Layer A bypassed via direct atomic mutation, or a future
//! producer regression). This is the Bazel parallel-chunk-read defense:
//! `Read(offset=0, limit=0)` must not serve partial in-flight bytes
//! past the declared size.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::ReadRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::streaming_blob::{StreamingBlobInner, StreamingBlobWriter};
use tonic::Request;

const INSTANCE_NAME: &str = "test_instance";
const HASH_OVERSHOOT: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

async fn make_store_manager() -> Result<Arc<StoreManager>, Box<dyn core::error::Error>> {
    let store_manager = Arc::new(StoreManager::new());
    let memory_store = store_factory(
        &StoreSpec::Memory(MemorySpec::default()),
        &store_manager,
        None,
    )
    .await?;
    store_manager.add_store("main_cas", memory_store);
    Ok(store_manager)
}

fn make_streaming_config() -> Vec<WithInstanceName<ByteStreamConfig>> {
    vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: "main_cas".to_string(),
            persist_stream_on_disconnect_timeout_s: 0,
            max_bytes_per_stream: 1024,
            streaming_read_while_write: true,
            max_streaming_blob_buffer_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
    }]
}

/// Bazel parallel-chunk shape: even if the streaming buffer holds more
/// than `digest.size_bytes()` bytes, `Read(offset=0, limit=0)` must
/// stop at `digest.size_bytes()`. The unfold cap (Layer C) is the
/// defense; Layer A would normally prevent this state, but T3 forges
/// the over-bytes via the test-only `Inner::append_chunk_for_test` to
/// prove the unfold doesn't depend on Layer A holding.
///
/// Mutation: comment out the `bytes_sent >= digest.size_bytes()` cap
/// in the unfold at `bytestream_server.rs:inner_read`; this test must
/// red-fail with bespoke "bytestream response exceeds digest.size_bytes()".
#[nativelink_test]
pub async fn bytestream_inner_read_caps_at_digest_size_bytes()
-> Result<(), Box<dyn core::error::Error>> {
    const DECLARED_SIZE: u64 = 1024;
    const OVERSHOOT_BYTES: u64 = 5000;
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(
        ByteStreamServer::new(&make_streaming_config(), store_manager.as_ref(), None)
            .expect("Failed to make server"),
    );

    let digest = DigestInfo::try_new(HASH_OVERSHOOT, DECLARED_SIZE)?;

    // Set `expected_size_on_store` to the BUFFER total (DECLARED +
    // OVERSHOOT) so Layer B's `bytes_written > expected_size` check
    // does NOT fire — the streaming buffer holds bytes that match its
    // own contract. The digest itself declares only DECLARED_SIZE,
    // however, so Layer C MUST cap the response at the declared size
    // even though the streaming inner is internally consistent.
    //
    // This is structurally the same mismatch the audit names: a
    // streaming-blob buffer that legitimately holds N bytes (per its
    // own producer's accounting) while the CAS digest declares a
    // smaller size — Bazel's `Read(offset=0, limit=0)` request must
    // stop at `digest.size_bytes()`, not at the buffer's terminal.
    let total_in_buffer = DECLARED_SIZE + OVERSHOOT_BYTES;
    let inner = Arc::new(StreamingBlobInner::new(digest, 16 * 1024 * 1024));
    inner.set_expected_size_on_store(total_in_buffer);
    {
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        writer
            .send(Bytes::from(vec![0xAB; DECLARED_SIZE as usize]))
            .await
            .expect("baseline send within cap must succeed");
        // Forge: append the OVERSHOOT bytes directly via the test
        // helper that bypasses Layer A (the writer.send admission
        // check). Layer C in the server unfold is what must truncate
        // them at digest.size_bytes(), independent of the buffer
        // contents.
        inner.append_chunk_for_test(Bytes::from(vec![0xCD; OVERSHOOT_BYTES as usize]));
        writer
            .send_eof()
            .expect("send_eof on forged-overshoot inner must succeed");
    }

    // Inject the pre-populated inner into the server's InFlightBlobMap.
    let in_flight_blobs = bs_server
        .in_flight_blobs_for_test(INSTANCE_NAME)
        .expect("instance must be configured");
    in_flight_blobs.insert_for_test(digest, Arc::clone(&inner));

    // Issue Read(offset=0, limit=0) — Bazel's parallel-chunk shape.
    let read_request = ReadRequest {
        resource_name: format!(
            "{}/blobs/{}/{}",
            INSTANCE_NAME, HASH_OVERSHOOT, DECLARED_SIZE
        ),
        read_offset: 0,
        read_limit: 0,
    };
    let read_result = tokio::time::timeout(
        Duration::from_secs(5),
        bs_server.read(Request::new(read_request)),
    )
    .await
    .expect("server read must not deadlock — Layer C is synchronous")?;
    let mut read_stream = read_result.into_inner();

    let mut total: u64 = 0;
    loop {
        let frame_opt = tokio::time::timeout(Duration::from_secs(5), read_stream.next())
            .await
            .expect("response stream must not hang past Layer C cap");
        let Some(resp) = frame_opt else { break };
        match resp {
            Ok(read_response) => {
                if read_response.data.is_empty() {
                    break;
                }
                total += read_response.data.len() as u64;
            }
            Err(_) => break,
        }
    }

    assert_eq!(
        total, DECLARED_SIZE,
        "bytestream response exceeds digest.size_bytes(): emitted {total} bytes \
         for a digest declared as {DECLARED_SIZE} bytes; Layer C unfold cap \
         missing — pipeline-2487 inflation shape would re-fire",
    );

    // MUTATION VERIFIED (2026-06-04): comment out the
    // `bytes_sent >= digest.size_bytes()` cap in the inner_read
    // unfold → red-fail with bespoke "bytestream response exceeds
    // digest.size_bytes()"; reverted, green again.
    Ok(())
}
