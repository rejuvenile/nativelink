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

//! #44 Layer C — `bytestream_server.rs:inner_read` **at-rest** unfold
//! MUST cap the emitted stream at `digest.size_bytes()` even if the
//! upstream store wrapper retained leftover Δ bytes past the declared
//! blob size (e.g. a store-internal buffer that was not truncated, or
//! a producer regression that wrote past the digest boundary). This is
//! the production hot-path defense for Bazel parallel-chunk reads:
//! pipeline-2487 (2026-06-03) returned `N + Δ` bytes for four
//! `Read(offset=0, limit=0)` requests against at-rest CAS blobs; the
//! audit at
//! `.claude/audits/45-pipeline-2487-overshoot-trigger-2026-06-04.md`
//! confirms NONE of the four reads took the streaming-blob branch
//! (`serving from in-flight streaming blob` log marker absent for all
//! four digests). The cap on the streaming-branch unfold (`:1493`)
//! does NOT protect this path; only the at-rest unfold cap added at
//! `:1768` does.
//!
//! Seams crossed by this test:
//!   1. `OverdeliveringStore` — writes `digest_size + delta` bytes to
//!      `&mut DropCloserWriteHalf`, simulating an upstream-store
//!      regression that retains leftover Δ bytes.
//!   2. `Store::new(...)` wrapper — production composition.
//!   3. `ByteStreamServer::inner_read` at-rest branch — `read` RPC
//!      with `streaming_read_while_write=false` so the
//!      `in_flight_blobs` lookup is skipped and the unfold at
//!      `:1768` runs.
//!   4. `Stream<ReadResponse>` consumer — sums total bytes received.
//!
//! Mutation contract: comment out the `remaining < bytes.len()`
//! truncation in the at-rest unfold at `bytestream_server.rs:~1830`.
//! This test must red-fail with bespoke "at-rest unfold did not cap
//! response at digest.size_bytes()" message.

use core::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_metric::{MetricFieldData, MetricKind, MetricPublishKnownKindData};
use nativelink_proto::google::bytestream::ReadRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{
    HealthStatusIndicator, default_health_status_indicator,
};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store,
    StoreDriver, StoreKey, UploadSizeInfo,
};
use tonic::Request;

const INSTANCE_NAME: &str = "test_instance";
const HASH_OVERSHOOT: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// A store whose `get_part` deliberately writes more than the digest's
/// declared size into the writer — emulating an upstream-store
/// regression that retains leftover Δ bytes (pipeline-2487 shape).
#[derive(Debug)]
struct OverdeliveringStore {
    /// Number of bytes to actually write to the writer (must exceed
    /// `digest.size_bytes()` for the test to be meaningful).
    deliver_bytes: u64,
}

impl OverdeliveringStore {
    fn new(deliver_bytes: u64) -> Arc<Self> {
        Arc::new(Self { deliver_bytes })
    }
}

impl nativelink_metric::MetricsComponent for OverdeliveringStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[async_trait::async_trait]
impl StoreDriver for OverdeliveringStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for i in 0..keys.len() {
            results[i] = Some(self.deliver_bytes);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        Ok(reader.drain().await?)
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // Send `deliver_bytes - offset` total in 256-byte chunks. The
        // unfold's per-chunk `max_bytes_per_stream` is 1024 in our
        // config; chunks are emitted into the buf_channel as Bytes and
        // the server consumes them sequentially via
        // `state.rx.consume()`. The OVERSHOOT is the difference
        // between `deliver_bytes` and the digest's declared size — the
        // at-rest unfold cap (Layer C) must truncate the response at
        // `digest.size_bytes()` regardless of how many bytes this
        // store actually delivers.
        let total_to_send = self.deliver_bytes.saturating_sub(offset);
        let chunk_size: usize = 256;
        let mut remaining = total_to_send as usize;
        while remaining > 0 {
            let n = remaining.min(chunk_size);
            let chunk = Bytes::from(vec![0xAB; n]);
            writer.send(chunk).await?;
            remaining -= n;
        }
        writer.send_eof()?;
        Ok(())
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

default_health_status_indicator!(OverdeliveringStore);

fn make_at_rest_config() -> Vec<WithInstanceName<ByteStreamConfig>> {
    vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: "main_cas".to_string(),
            persist_stream_on_disconnect_timeout_s: 0,
            max_bytes_per_stream: 1024,
            // streaming_read_while_write=false forces inner_read to
            // skip the in_flight_blobs lookup and fall through to the
            // at-rest unfold at `:1768` — the production hot path for
            // Bazel parallel-chunk reads of committed CAS blobs (the
            // pipeline-2487 shape).
            streaming_read_while_write: false,
            max_streaming_blob_buffer_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
    }]
}

/// Production-composition seam test:
///   producer (OverdeliveringStore — overshoots digest.size_bytes())
///     → `Store::new(...)` wrapper
///     → `ByteStreamServer::inner_read` at-rest unfold (`:1768`)
///     → `Stream<ReadResponse>` consumer asserting total ≤ digest size.
///
/// Mutation: comment out the at-rest cap in `bytestream_server.rs`'s
/// inner_read unfold (the `if (bytes.len() as u64) > remaining` block
/// near `:1830`); this test must red-fail with bespoke "at-rest
/// unfold did not cap response at digest.size_bytes()".
#[nativelink_test]
pub async fn bytestream_inner_read_caps_at_rest_at_digest_size()
-> Result<(), Box<dyn core::error::Error>> {
    const DECLARED_SIZE: u64 = 1024;
    const DELTA: u64 = 512;
    const TOTAL_DELIVERED: u64 = DECLARED_SIZE + DELTA;

    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        Store::new(OverdeliveringStore::new(TOTAL_DELIVERED)),
    );

    let bs_server = Arc::new(
        ByteStreamServer::new(&make_at_rest_config(), store_manager.as_ref(), None)
            .expect("Failed to make server"),
    );

    // Verify the resource_name parses to a well-formed digest; the
    // server reconstructs `DigestInfo` from the resource_name string.
    let _digest = DigestInfo::try_new(HASH_OVERSHOOT, DECLARED_SIZE)?;

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
    .expect("server read must not deadlock — at-rest cap is synchronous")?;
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
        "at-rest unfold did not cap response at digest.size_bytes(): \
         emitted {total} bytes for a digest declared as {DECLARED_SIZE} \
         bytes (store overdelivered {TOTAL_DELIVERED}); pipeline-2487 \
         shape would re-fire — Layer C at-rest cap missing",
    );

    // MUTATION VERIFIED (2026-06-04): comment out the at-rest unfold
    // cap (the `if (bytes.len() as u64) > remaining` truncation block
    // in `bytestream_server.rs:inner_read` near `:1830`) → test
    // red-fails with bespoke "at-rest unfold did not cap response at
    // digest.size_bytes()" message; cap restored, green again.
    Ok(())
}
