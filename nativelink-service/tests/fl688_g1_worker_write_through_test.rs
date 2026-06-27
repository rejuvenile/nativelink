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

//! #FL-688: G1 carve-out for worker/mirror ByteStream writes.
//!
//! The G1 already-exists short-circuit in `bytestream_write` at
//! `bytestream_server.rs:3356` (`if has_result.is_some() && !is_chunked_in_flight`)
//! returns `Ok(WriteResponse{committed_size})` WITHOUT calling `store.update`
//! when `WorkerProxyStore::has` returns `Some` via the locality_map.
//!
//! For a WORKER or MIRROR upload, the uploader IS the durability push —
//! the blob is only on the worker (not in the server's durable store). G1
//! must NOT fire on the strength of the uploader's own locality entry, or the
//! blob is never persisted on the server and is re-requested every 60s
//! (`BACKFILL_INFLIGHT_TIMEOUT_SECS`) forever.
//!
//! Fix (Shape 1): add `&& !is_worker && !is_mirror` to the G1 condition so
//! worker/mirror uploads fall through to `store.update` → WPS → VerifyStore →
//! ECS (OPT-1 durable-aware skip) → SizePartitioning → FastSlowStore →
//! background slow-write spawn lands the durable copy.
//!
//! Seams crossed (per `.claude/rules/testing-contracts.md`):
//!   worker/mirror upload (is_worker/is_mirror=true)
//!   → `bytestream_write` G1 gate (bytestream_server.rs:3356)
//!   → FALLS THROUGH to `store.update`
//!   → `WorkerProxyStore::update` (passes through)
//!   → `MemoryStore::update` (inner durable store in this composition)
//!
//! Mutation guide (TDD step 5): removing `&& !is_worker && !is_mirror` from
//! the G1 gate restores the bug — G1 fires for worker/mirror → inner store
//! stays empty → the `has_with_results` assertion red-fails with the bespoke
//! message "worker upload was skipped by G1 — inner store still empty; G1
//! must not fire on worker/mirror uploads (mutation: remove && !is_worker &&
//! !is_mirror from bytestream_server.rs G1 gate)".
//!
//! The asymmetric (client-preserved) test is also present to verify the
//! Bazel-client fast-path optimization is not broken: a non-worker,
//! non-mirror upload with the digest locality-seeded STILL takes the G1
//! short-circuit (inner store stays empty, RPC returns committed_size).

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::BodyExt;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_error::ResultExt;
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_util::{background_spawn, spawn};
use nativelink_config::stores::MemorySpec;
use tonic::codec::{Codec, CompressionEncoding};
use tonic::metadata::MetadataValue;
use tonic::{Request, Streaming};
use tonic_prost::ProstCodec;

const DEADLOCK_DETECTOR: Duration = Duration::from_secs(5);
const INSTANCE_NAME: &str = "main";
const CAS_STORE_NAME: &str = "cas_STORE";
/// Digest used as the blob under test.
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

/// Build a `StoreManager` with `cas_STORE` = `WorkerProxyStore` wrapping a
/// fresh `MemoryStore` (the durable inner store). Returns both the manager and
/// the inner `MemoryStore` so tests can inspect it directly.
fn make_manager_with_inner() -> (Arc<StoreManager>, Arc<WorkerProxyStore>, Store) {
    let inner_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner_mem.clone(), locality);
    let proxy_arc = proxy.clone();
    let manager = Arc::new(StoreManager::new());
    manager.add_store(CAS_STORE_NAME, Store::new(proxy));
    (manager, proxy_arc, inner_mem)
}

fn make_bytestream_server(manager: &StoreManager) -> Result<Arc<ByteStreamServer>, nativelink_error::Error> {
    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: CAS_STORE_NAME.to_string(),
            persist_stream_on_disconnect_timeout: 0,
            max_bytes_per_stream: 256 * 1024,
            ..Default::default()
        },
    }];
    Ok(Arc::new(ByteStreamServer::new(&config, manager, None)?))
}

/// Drive a single-frame `ByteStream::write` RPC for `data` against the given
/// server. `extra_header` is optional (`Some(("key", "value"))`) and is added
/// to the tonic request metadata BEFORE the write is dispatched — used to
/// inject `x-nativelink-worker` or `x-nativelink-mirror`.
fn drive_write_with_header(
    bs_server: Arc<ByteStreamServer>,
    data: Bytes,
    extra_header: Option<(&'static str, &'static str)>,
) -> nativelink_util::task::JoinHandleDropGuard<
    Result<
        tonic::Response<nativelink_proto::google::bytestream::WriteResponse>,
        tonic::Status,
    >,
> {
    let (tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream = Streaming::new_request(
        codec.decoder(),
        body,
        Some(CompressionEncoding::Gzip),
        None,
    );
    let resource_name = format!(
        "{}/uploads/{}/blobs/{}/{}",
        INSTANCE_NAME,
        "22222222-2222-2222-2222-222222222222",
        HASH1,
        data.len(),
    );
    let req = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: true,
        data,
    };
    background_spawn!("drive_write_frame", async move {
        let frame = http_body_util::Full::new(
            encode_stream_proto(&req).expect("encode"),
        );
        drop(
            tx.send(http_body::Frame::data(
                frame.collect().await.expect("collect").to_bytes(),
            ))
            .await,
        );
    });
    let mut request = Request::new(stream);
    if let Some((key, value)) = extra_header {
        request
            .metadata_mut()
            .insert(key, MetadataValue::from_static(value));
    }
    spawn!("bs_server_write", async move {
        bs_server.write(request).await
    })
}

/// Seed the `proxy`'s locality_map so that `has(digest)` returns `Some` via
/// the locality fallback — WITHOUT putting the blob in the inner store. This
/// models "worker holds the blob, server does not have a durable copy yet"
/// — the exact G1-bug scenario.
fn seed_locality_only(proxy: &WorkerProxyStore, digest: DigestInfo) {
    proxy
        .locality_map()
        .write()
        .register_blobs("grpc://worker-under-test:50071", &[digest]);
}

// ===================================================================
// Tests.
// ===================================================================

/// G1 carve-out (worker): a worker upload of a locality-present-but-not-durable
/// digest MUST write through to the inner store.
///
/// Setup: locality_map has `digest` registered for a worker endpoint; inner
/// MemoryStore does NOT. Without the fix, `WorkerProxyStore::has(digest)`
/// returns `Some` via the locality OR-in → G1 fires → inner store stays empty.
/// With the fix (`&& !is_worker`), G1 is bypassed and `store.update` persists
/// the blob in the inner store.
///
/// Seam: is_worker=true → `bytestream_write` G1 gate (bytestream_server.rs:3356)
/// → `store.update` → `WorkerProxyStore::update` → `MemoryStore::update` (inner).
///
/// Mutation: remove `&& !is_worker && !is_mirror` from the G1 gate condition
/// in `bytestream_server.rs` → G1 fires → inner MemoryStore stays empty →
/// this test red-fails on the has_with_results assertion.
#[nativelink_test]
async fn worker_upload_writes_through_when_locality_present()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy, inner_store) = make_manager_with_inner();
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0xab_u8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    // Seed locality so WPS::has(digest) returns Some WITHOUT the blob in the
    // inner store. This is the G1-bug scenario: the uploading worker appears
    // to "have" the digest via its own advertisement.
    seed_locality_only(&proxy, digest);

    // Verify the inner store does NOT have the blob yet (pre-condition).
    {
        let mut pre_results = vec![None];
        inner_store
            .as_store_driver_pin()
            .has_with_results(&[digest.into()], &mut pre_results)
            .await
            .expect("has_with_results pre-check");
        assert!(
            pre_results[0].is_none(),
            "pre-condition: inner store must not have the digest before the upload"
        );
    }

    // Drive the upload WITH the x-nativelink-worker header → is_worker=true.
    let response = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        drive_write_with_header(
            bs_server,
            data.clone(),
            Some(("x-nativelink-worker", "1")),
        ),
    )
    .await
    .expect("worker upload must not hang — deadlock detector")
    .expect("join")
    .err_tip(|| "write RPC")?
    .into_inner();

    assert_eq!(
        response.committed_size,
        data.len() as i64,
        "worker upload must ack committed_size"
    );

    // The write must have reached the inner store — NOT short-circuited by G1.
    let mut post_results = vec![None];
    tokio::time::timeout(DEADLOCK_DETECTOR, async {
        inner_store
            .as_store_driver_pin()
            .has_with_results(&[digest.into()], &mut post_results)
            .await
    })
    .await
    .expect("has_with_results must not hang")
    .expect("has_with_results post-check");

    assert!(
        post_results[0].is_some(),
        "worker upload was skipped by G1 — inner store still empty; G1 must \
         not fire on worker/mirror uploads \
         (mutation: remove && !is_worker && !is_mirror from \
         bytestream_server.rs G1 gate)"
    );
    Ok(())
}

/// G1 carve-out (mirror): a mirror upload of a locality-present-but-not-durable
/// digest MUST write through to the inner store, just like the worker case.
///
/// Seam: is_mirror=true → `bytestream_write` G1 gate → `store.update` → inner.
///
/// Mutation: same as `worker_upload_writes_through_when_locality_present` —
/// remove `&& !is_worker && !is_mirror` from the G1 gate.
#[nativelink_test]
async fn mirror_upload_writes_through_when_locality_present()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy, inner_store) = make_manager_with_inner();
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0xcd_u8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    seed_locality_only(&proxy, digest);

    let response = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        drive_write_with_header(
            bs_server,
            data.clone(),
            Some(("x-nativelink-mirror", "1")),
        ),
    )
    .await
    .expect("mirror upload must not hang — deadlock detector")
    .expect("join")
    .err_tip(|| "write RPC")?
    .into_inner();

    assert_eq!(
        response.committed_size,
        data.len() as i64,
        "mirror upload must ack committed_size"
    );

    let mut post_results = vec![None];
    tokio::time::timeout(DEADLOCK_DETECTOR, async {
        inner_store
            .as_store_driver_pin()
            .has_with_results(&[digest.into()], &mut post_results)
            .await
    })
    .await
    .expect("has_with_results must not hang")
    .expect("has_with_results post-check");

    assert!(
        post_results[0].is_some(),
        "mirror upload was skipped by G1 — inner store still empty; G1 must \
         not fire on worker/mirror uploads \
         (mutation: remove && !is_worker && !is_mirror from \
         bytestream_server.rs G1 gate)"
    );
    Ok(())
}

/// Asymmetric coverage (over-action direction): a Bazel CLIENT upload
/// (is_worker=false, is_mirror=false) with the digest locality-seeded MUST
/// still trigger the G1 short-circuit — the inner store stays empty and the
/// RPC returns committed_size. This proves the client fast-path optimization
/// is preserved by the fix.
///
/// This test MUST NOT change behavior after the fix (`&& !is_worker &&
/// !is_mirror` evaluates to `true` for a client → G1 still fires → identical
/// behavior). Reverting the fix must NOT change this test.
///
/// Seam: is_worker=false, is_mirror=false, locality present → `bytestream_write`
/// G1 gate fires → drain stream → return WriteResponse without `store.update`.
#[nativelink_test]
async fn client_upload_still_short_circuits_on_locality_present()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy, inner_store) = make_manager_with_inner();
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0xef_u8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    // Seed locality so G1 can fire (same setup as worker test above).
    seed_locality_only(&proxy, digest);

    // Client upload: no x-nativelink-worker, no x-nativelink-mirror header.
    let response = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        drive_write_with_header(bs_server, data.clone(), None),
    )
    .await
    .expect("client upload must not hang — deadlock detector")
    .expect("join")
    .err_tip(|| "write RPC")?
    .into_inner();

    assert_eq!(
        response.committed_size,
        data.len() as i64,
        "client upload must still ack committed_size (G1 short-circuit preserved)"
    );

    // Inner store must remain EMPTY — G1 skipped the write for the client,
    // which is the correct optimization (locality says a worker has it).
    let mut post_results = vec![None];
    tokio::time::timeout(DEADLOCK_DETECTOR, async {
        inner_store
            .as_store_driver_pin()
            .has_with_results(&[digest.into()], &mut post_results)
            .await
    })
    .await
    .expect("has_with_results must not hang")
    .expect("has_with_results post-check");

    assert!(
        post_results[0].is_none(),
        "client upload must NOT write through when locality is present — G1 \
         must still short-circuit for is_worker=false, is_mirror=false \
         (over-action guard: the fix must not disable G1 for clients)"
    );
    Ok(())
}
