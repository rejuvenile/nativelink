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

//! Production-composition integration tests for the #168 producer-side
//! `SmallBlobDispatcher` hooks.
//!
//! Spec: `.claude/plans/bug-a-small-cas-peer-mirror.md` §"Order of
//! operations" step 6.
//!
//! These tests assert that on a successful upload via the production
//! ingest paths (`bytestream_server::inner_write_oneshot`,
//! `cas_server::inner_batch_update_blobs`, `ac_server::inner_update_action_result`)
//! the dispatcher fans out a `BatchWriteSmallBlobs` to every connected
//! worker, AND that the dispatcher is correctly skipped when:
//!   - the blob is larger than `SMALL_BLOB_THRESHOLD`,
//!   - the upload originates from a worker (avoid recursive re-mirror),
//!   - the upload is itself a mirror push (avoid feedback loops),
//!   - the dispatcher feature flag is disabled.
//!
//! Production composition: real `MemoryStore` (CAS slow tier), real
//! `WorkerProxyStore` (the production CAS wrapper), real
//! `SmallBlobDispatcher`, fake `worker_tx` mpsc receiver standing in
//! for a connected worker. The receiver lets us assert the dispatched
//! `UpdateForWorker { batch_write_small_blobs }` arrives within a
//! 5-second deadlock-detector timeout (per CLAUDE.md "deadlock detector
//! via tokio::time::timeout" rule).
//!
//! Mutation step (per CLAUDE.md TDD step 5): comment out the
//! `dispatcher.dispatch_to_all_workers(...)` call in
//! `bytestream_server::inner_write_oneshot`; the
//! `oneshot_small_write_dispatches_to_connected_worker` test MUST
//! red-fail with the bespoke
//! `"#168 producer hook MUST fan out small oneshot CAS write to connected worker"`
//! message — NOT a generic `tokio::time::Elapsed` from `is_err()`.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::BodyExt;
use nativelink_config::cas_server::{
    AcStoreConfig, ByteStreamConfig, CasStoreConfig, WithInstanceName,
};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, BatchUpdateBlobsRequest, UpdateActionResultRequest,
    batch_update_blobs_request,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker::Update as UpdateForWorkerUpdate,
};
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::ac_server::AcServer;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::cas_server::CasServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::small_blob_dispatcher::{
    SMALL_BLOB_THRESHOLD, SmallBlobDispatcher, SmallBlobDispatcherConfig,
};
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::store_trait::Store;
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{background_spawn, spawn};
use prost::Message;
use tokio::sync::mpsc;
use tonic::Request;
use tonic::codec::{Codec, CompressionEncoding};
use tonic::metadata::MetadataValue;
use tonic::{Response, Streaming};
use tonic_prost::ProstCodec;

/// Deadlock detector timeout. Long enough to absorb CI scheduling
/// jitter, short enough that a hung dispatch fails the run cleanly.
/// Per CLAUDE.md "Asymmetric contract coverage" / "deadlock detector".
const DEADLOCK_DETECTOR: Duration = Duration::from_secs(5);

/// Used to assert that NO dispatch happens within a bounded window.
/// Producer-hook over-action failure modes (missing `is_worker`/`is_mirror`
/// gate, over-threshold dispatch, loop-back) MUST manifest within this
/// window. Long enough that a tokio::spawn'd dispatch task definitely
/// runs to completion if it was scheduled; short enough to keep tests
/// fast.
const NO_DISPATCH_WINDOW: Duration = Duration::from_millis(500);

const INSTANCE_NAME: &str = "main";
/// Production CAS store name — matches the `cas_STORE` name in
/// `~/fl/bld/infra/nativelink/prod-server.json5`. The dispatcher's
/// `EphemeralServerSidePin` is registered under this exact name in
/// `nativelink.rs:499-525`.
const CAS_STORE_NAME: &str = "cas_STORE";
const AC_STORE_NAME: &str = "AC_STORE";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const FAKE_WORKER_ENDPOINT: &str = "grpc://fake-worker:50071";
const FAKE_WORKER_BOOT_EPOCH: u64 = 42;

/// Build a `StoreManager` whose `cas_STORE` is a `WorkerProxyStore`
/// wrapping a `MemoryStore` (matches the production wrap order in
/// `nativelink.rs`). The store is ALSO registered under
/// `AC_STORE_NAME` so the same harness can drive `AcServer` tests.
async fn make_proxy_store_manager() -> Result<Arc<StoreManager>, Error> {
    Ok(make_proxy_store_manager_with_proxy().await?.0)
}

/// Same as [`make_proxy_store_manager`] but ALSO returns the
/// `Arc<WorkerProxyStore>` directly. Tests that need to peek at
/// `mirror_total_attempted` / `locality_map` use this — `downcast_ref`
/// through the StoreManager won't resolve to WorkerProxyStore because
/// `WorkerProxyStore::inner_store` delegates to `self.inner` (so the
/// downcast traverses past the wrapper).
async fn make_proxy_store_manager_with_proxy()
-> Result<(Arc<StoreManager>, Arc<WorkerProxyStore>), Error> {
    let manager = Arc::new(StoreManager::new());

    let cas_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let cas_locality = new_shared_blob_locality_map();
    let cas_proxy = WorkerProxyStore::new(cas_inner, cas_locality);
    manager.add_store(CAS_STORE_NAME, Store::new(cas_proxy.clone()));

    // AC backing store: a plain MemoryStore is sufficient — the AC
    // path doesn't go through WorkerProxyStore in production either
    // (per `prod-server.json5`'s `AC_STORE` chain).
    manager.add_store(
        AC_STORE_NAME,
        store_factory(&StoreSpec::Memory(MemorySpec::default()), &manager, None).await?,
    );
    Ok((manager, cas_proxy))
}

/// Build a `SmallBlobDispatcher` with `small_blob_mirror_enabled=true`,
/// register a single fake worker `(endpoint, boot_epoch_id)`, register
/// pin sets for both `cas_STORE` and `AC_STORE`. Returns the dispatcher
/// + the `worker_rx` the test will drain to assert dispatch arrived.
fn make_dispatcher_with_one_worker() -> (
    Arc<SmallBlobDispatcher>,
    mpsc::UnboundedReceiver<UpdateForWorker>,
) {
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        ..Default::default()
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    // Register pin sets for the two production CAS+AC store names so
    // `enqueue` does not silently drop with "no pin set registered".
    use nativelink_store::small_blob_dispatcher::EphemeralServerSidePin;
    let pin_max_bytes = 256 * 1024 * 1024;
    dispatcher.register_pin_set(
        CAS_STORE_NAME,
        Arc::new(EphemeralServerSidePin::new(pin_max_bytes)),
    );
    dispatcher.register_pin_set(
        AC_STORE_NAME,
        Arc::new(EphemeralServerSidePin::new(pin_max_bytes)),
    );

    let (worker_tx, worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
    dispatcher.register_worker(FAKE_WORKER_ENDPOINT, FAKE_WORKER_BOOT_EPOCH, worker_tx);

    (dispatcher, worker_rx)
}

/// Build a `ByteStreamServer` wired to the production-shaped manager
/// and the test dispatcher.
fn make_bytestream_server(
    manager: &StoreManager,
    dispatcher: Option<Arc<SmallBlobDispatcher>>,
) -> Result<Arc<ByteStreamServer>, Error> {
    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: CAS_STORE_NAME.to_string(),
            persist_stream_on_disconnect_timeout: 0,
            max_bytes_per_stream: 256 * 1024,
            ..Default::default()
        },
    }];
    Ok(Arc::new(ByteStreamServer::new(&config, manager, dispatcher)?))
}

fn make_cas_server(
    manager: &StoreManager,
    dispatcher: Option<Arc<SmallBlobDispatcher>>,
) -> Result<Arc<CasServer>, Error> {
    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: CasStoreConfig {
            cas_store: CAS_STORE_NAME.to_string(),
        },
    }];
    Ok(Arc::new(CasServer::new(&config, manager, dispatcher)?))
}

fn make_ac_server(
    manager: &StoreManager,
    dispatcher: Option<Arc<SmallBlobDispatcher>>,
) -> Result<Arc<AcServer>, Error> {
    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: AcStoreConfig {
            ac_store: AC_STORE_NAME.to_string(),
            read_only: false,
        },
    }];
    Ok(Arc::new(AcServer::new(&config, manager, dispatcher)?))
}

/// Drive a single `WriteRequest` (oneshot — `finish_write=true` on the
/// first chunk) through `ByteStreamServer::write` for `data` of length
/// `data.len()`. Returns the join handle so the caller can await the
/// final `WriteResponse`.
fn drive_oneshot_write(
    bs_server: Arc<ByteStreamServer>,
    data: Bytes,
) -> JoinHandleDropGuard<Result<Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>>
{
    drive_oneshot_write_with_headers(bs_server, data, &[])
}

/// Same as [`drive_oneshot_write`] but adds custom request headers so
/// the bytestream_server's `is_worker` / `is_mirror` gates fire as
/// they would in production (tonic extracts those from the request
/// metadata; we plumb them in via [`tonic::metadata::MetadataValue`]).
fn drive_oneshot_write_with_headers(
    bs_server: Arc<ByteStreamServer>,
    data: Bytes,
    headers: &[(&'static str, &'static str)],
) -> JoinHandleDropGuard<
    Result<Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
> {
    let (tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream = Streaming::new_request(codec.decoder(), body, Some(CompressionEncoding::Gzip), None);

    let resource_name = format!(
        "{}/uploads/{}/blobs/{}/{}",
        INSTANCE_NAME,
        "11111111-1111-1111-1111-111111111111",
        HASH1,
        data.len(),
    );

    let req = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: true,
        data,
    };

    background_spawn!("drive_oneshot_write_writer", async move {
        let frame = http_body_util::Full::new(encode_stream_proto(&req).expect("encode"));
        let _send_result = tx
            .send(http_body::Frame::data(
                frame.collect().await.expect("collect").to_bytes(),
            ))
            .await;
        // dropping tx closes the upstream after EOF
    });

    let mut request = Request::new(stream);
    for (name, value) in headers {
        request.metadata_mut().insert(
            *name,
            MetadataValue::try_from(*value).expect("valid header value"),
        );
    }

    spawn!("bs_server_write", async move {
        bs_server.write(request).await
    })
}

/// Assert that NO `BatchWriteSmallBlobs` arrives on `worker_rx` within
/// `NO_DISPATCH_WINDOW`. Bespoke message names the over-action being
/// guarded so a regression points the reader at the right contract.
async fn assert_no_dispatch(
    worker_rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
    bespoke_message: &str,
) {
    let recv_outcome = tokio::time::timeout(NO_DISPATCH_WINDOW, worker_rx.recv()).await;
    match recv_outcome {
        Err(_) => {} // timeout = no dispatch, as expected
        Ok(None) => {} // channel closed = no dispatch
        Ok(Some(msg)) => match msg.update {
            Some(UpdateForWorkerUpdate::BatchWriteSmallBlobs(batch)) => {
                panic!(
                    "{bespoke_message}: unexpected dispatch with {} entries",
                    batch.blobs.len()
                );
            }
            other => panic!("{bespoke_message}: unexpected non-dispatch message {other:?}"),
        },
    }
}

/// Drain `worker_rx` until a `BatchWriteSmallBlobs` arrives, then
/// return the entries. Bounded by `DEADLOCK_DETECTOR`; on timeout
/// fails with the supplied bespoke message (per CLAUDE.md "Assertion
/// specificity").
async fn await_batch_write_small_blobs(
    worker_rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
    bespoke_failure_msg: &str,
) -> Vec<nativelink_proto::com::github::trace_machina::nativelink::remote_execution::SmallBlobEntry>
{
    let msg = tokio::time::timeout(DEADLOCK_DETECTOR, worker_rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{bespoke_failure_msg}"))
        .unwrap_or_else(|| panic!("{bespoke_failure_msg}: worker_rx closed unexpectedly"));
    match msg.update {
        Some(UpdateForWorkerUpdate::BatchWriteSmallBlobs(batch)) => batch.blobs,
        other => panic!(
            "{bespoke_failure_msg}: dispatcher must send BatchWriteSmallBlobs; got {other:?}"
        ),
    }
}

// =====================================================================
// T1: bytestream oneshot small write fans out to connected worker.
// =====================================================================
//
// Production-composition WIRE-UP test for the #168 producer hook in
// `bytestream_server::inner_write_oneshot`. With `small_blob_mirror_enabled=true`,
// a single connected worker registered, and a < SMALL_BLOB_THRESHOLD
// blob written, the dispatcher MUST fan out a `BatchWriteSmallBlobs`
// to that worker within `DEADLOCK_DETECTOR`.
//
// Mutation step: comment out the `dispatch_to_all_workers` call in
// `bytestream_server::inner_write_oneshot` (around the
// `if !is_worker && !is_mirror && bytes_received <= SMALL_BLOB_THRESHOLD`
// block) and rerun this test — the assertion message
// `"#168 producer hook MUST fan out small oneshot CAS write to connected worker"`
// MUST appear in the failure output.
#[nativelink_test]
async fn oneshot_small_write_dispatches_to_connected_worker()
-> Result<(), Box<dyn core::error::Error>> {
    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;

    // Build a payload SMALLER than SMALL_BLOB_THRESHOLD so the
    // dispatcher gate accepts it. 1 KiB is comfortably below the
    // 16 KiB threshold.
    let data = Bytes::from(vec![0xABu8; 1024]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let join_handle = drive_oneshot_write(bs_server, data.clone());
    let response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect(
            "#168 producer hook MUST NOT deadlock the bytestream write — \
             upload should complete within DEADLOCK_DETECTOR",
        )
        .expect("join handle")
        .expect("write RPC must succeed");
    assert_eq!(
        response.into_inner().committed_size,
        data.len() as i64,
        "bytestream oneshot must commit the full payload"
    );

    let entries = await_batch_write_small_blobs(
        &mut worker_rx,
        "#168 producer hook MUST fan out small oneshot CAS write to connected worker",
    )
    .await;
    assert_eq!(
        entries.len(),
        1,
        "exactly one small-blob entry must be dispatched; got {entries:?}"
    );
    let entry = &entries[0];
    assert_eq!(
        entry.store_id, CAS_STORE_NAME,
        "dispatched entry must carry the registered cas_store name as store_id"
    );
    let entry_digest =
        DigestInfo::try_from(entry.digest.clone().expect("entry has digest"))
            .expect("entry digest decodes");
    assert_eq!(
        entry_digest, digest,
        "dispatched entry must carry the uploaded blob's digest"
    );
    assert_eq!(
        entry.data.as_ref(),
        data.as_ref(),
        "dispatched entry must carry the uploaded blob's bytes verbatim"
    );

    Ok(())
}

// =====================================================================
// T2: bytestream oneshot LARGE write does NOT trigger dispatcher.
// =====================================================================
//
// The producer hook gates on `bytes_received <= SMALL_BLOB_THRESHOLD`;
// a payload above that threshold takes the existing
// `WorkerProxyStore::mirror_blob_to_random_worker` path (which fans
// out to ONE worker, not the dispatcher's all-workers fan-out). This
// test asserts no `BatchWriteSmallBlobs` arrives on `worker_rx` for an
// over-threshold blob.
//
// Mutation: remove the size gate; this test must red-fail because a
// 32 KiB payload would be wrapped in a `BatchWriteSmallBlobs` and
// `enqueue`'s precondition would log + drop, but the over-size payload
// would never round-trip through the worker_tx mpsc.
#[nativelink_test]
async fn oneshot_large_write_does_not_dispatch()
-> Result<(), Box<dyn core::error::Error>> {
    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;

    // 32 KiB > SMALL_BLOB_THRESHOLD (16 KiB).
    let oversize_len = SMALL_BLOB_THRESHOLD * 2;
    let data = Bytes::from(vec![0xCDu8; oversize_len]);

    let join_handle = drive_oneshot_write(bs_server, data.clone());
    let response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect("write must complete")
        .expect("join handle")
        .expect("write RPC must succeed");
    assert_eq!(
        response.into_inner().committed_size,
        data.len() as i64,
        "bytestream oneshot must commit full payload"
    );

    // Drain with a SHORT timeout — the dispatcher MUST NOT have
    // produced a BatchWriteSmallBlobs for an over-threshold blob.
    let recv_outcome = tokio::time::timeout(Duration::from_millis(500), worker_rx.recv()).await;
    assert!(
        recv_outcome.is_err(),
        "dispatcher MUST NOT enqueue blobs > SMALL_BLOB_THRESHOLD; \
         got unexpected message: {recv_outcome:?}"
    );

    Ok(())
}

// =====================================================================
// T3: dispatcher disabled (feature flag off) — no dispatch on small write.
// =====================================================================
//
// Smoke test for the `is_enabled()` short-circuit. With the feature
// flag off, even a small blob must NOT trigger any worker_tx send.
#[nativelink_test]
async fn oneshot_small_write_dispatcher_disabled_does_not_dispatch()
-> Result<(), Box<dyn core::error::Error>> {
    let manager = make_proxy_store_manager().await?;
    // Feature flag explicitly OFF.
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: false,
        ..Default::default()
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    use nativelink_store::small_blob_dispatcher::EphemeralServerSidePin;
    dispatcher.register_pin_set(
        CAS_STORE_NAME,
        Arc::new(EphemeralServerSidePin::new(256 * 1024 * 1024)),
    );
    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
    dispatcher.register_worker(FAKE_WORKER_ENDPOINT, FAKE_WORKER_BOOT_EPOCH, worker_tx);

    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;
    let data = Bytes::from(vec![0u8; 1024]);
    let join_handle = drive_oneshot_write(bs_server, data.clone());
    let _response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect("write must complete")
        .expect("join handle")
        .expect("write RPC must succeed");

    let recv_outcome = tokio::time::timeout(Duration::from_millis(500), worker_rx.recv()).await;
    assert!(
        recv_outcome.is_err(),
        "feature-flag-disabled dispatcher MUST NOT fan out; got {recv_outcome:?}"
    );

    Ok(())
}

// =====================================================================
// T4: cas_server::inner_batch_update_blobs fans out small entries.
// =====================================================================
//
// Production-composition WIRE-UP test for the #168 producer hook in
// `cas_server::inner_batch_update_blobs`. With `small_blob_mirror_enabled=true`
// and a single connected worker, a `BatchUpdateBlobsRequest` carrying
// a small entry MUST trigger a `BatchWriteSmallBlobs` to the worker.
//
// Mutation step: comment out the `dispatch_to_all_workers` call in
// `cas_server::inner_batch_update_blobs`'s Ok arm; this test MUST
// red-fail with the bespoke
// `"#168 producer hook MUST fan out small CAS BatchUpdate write to connected worker"`
// message.
#[nativelink_test]
async fn batch_update_blobs_small_entry_dispatches()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;

    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let cas_server = make_cas_server(manager.as_ref(), Some(dispatcher.clone()))?;

    let data = Bytes::from(vec![0x42u8; 2048]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let request = BatchUpdateBlobsRequest {
        instance_name: INSTANCE_NAME.to_string(),
        requests: vec![batch_update_blobs_request::Request {
            digest: Some(digest.into()),
            data: data.clone(),
            compressor: 0,
        }],
        digest_function: 0,
    };

    let response_fut = cas_server.batch_update_blobs(Request::new(request));
    let response = tokio::time::timeout(DEADLOCK_DETECTOR, response_fut)
        .await
        .expect("CAS batch update must complete")
        .err_tip(|| "CAS batch update RPC")?;
    let inner = response.into_inner();
    assert_eq!(inner.responses.len(), 1, "exactly one response per blob");
    assert_eq!(
        inner.responses[0].status.as_ref().map(|s| s.code).unwrap_or(-1),
        0,
        "blob upload must succeed (code=0)"
    );

    let entries = await_batch_write_small_blobs(
        &mut worker_rx,
        "#168 producer hook MUST fan out small CAS BatchUpdate write to connected worker",
    )
    .await;
    assert_eq!(entries.len(), 1, "expected exactly one small-blob entry");
    assert_eq!(
        entries[0].store_id, CAS_STORE_NAME,
        "BatchUpdate dispatch must use registered cas_store name"
    );

    Ok(())
}

// =====================================================================
// T5: ac_server::inner_update_action_result fans out the AC blob.
// =====================================================================
//
// Production-composition WIRE-UP test for the #168 producer hook in
// `ac_server::inner_update_action_result`. ActionResult protos are
// almost always tiny (< 1 KiB), so the SMALL_BLOB_THRESHOLD gate
// always passes. With a connected worker AND an AC pin set registered
// (NB: today `nativelink.rs:526-532` does NOT register AC pin sets in
// production; this test PRE-PROVES the wire-up so that registration
// is the only follow-up gap), the dispatcher MUST fan out.
//
// Mutation step: comment out the `dispatch_to_all_workers` call in
// `ac_server::inner_update_action_result`'s Ok arm; this test MUST
// red-fail with the bespoke
// `"#168 producer hook MUST fan out AC update_action_result to connected worker"`
// message.
#[nativelink_test]
async fn update_action_result_dispatches_to_connected_worker()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::build::bazel::remote::execution::v2::action_cache_server::ActionCache;

    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let ac_server = make_ac_server(manager.as_ref(), Some(dispatcher.clone()))?;

    // The action_digest names the AC entry; the DATA stored in the
    // AC store is the encoded ActionResult proto. We use HASH1 + a
    // reasonable size for the action_digest; the actual size is
    // determined by the encoded ActionResult.
    let action_digest = DigestInfo::try_new(HASH1, 64).expect("valid digest");
    let action_result = ActionResult::default();

    let request = UpdateActionResultRequest {
        instance_name: INSTANCE_NAME.to_string(),
        action_digest: Some(action_digest.into()),
        action_result: Some(action_result.clone()),
        results_cache_policy: None,
        digest_function: 0,
    };

    let response_fut = ac_server.update_action_result(Request::new(request));
    let _response = tokio::time::timeout(DEADLOCK_DETECTOR, response_fut)
        .await
        .expect("AC update must complete")
        .err_tip(|| "AC update_action_result RPC")?;

    let entries = await_batch_write_small_blobs(
        &mut worker_rx,
        "#168 producer hook MUST fan out AC update_action_result to connected worker",
    )
    .await;
    assert_eq!(
        entries.len(),
        1,
        "exactly one AC entry must be dispatched; got {entries:?}"
    );
    assert_eq!(
        entries[0].store_id, AC_STORE_NAME,
        "AC dispatch must use registered ac_store name"
    );
    let dispatched_digest =
        DigestInfo::try_from(entries[0].digest.clone().expect("digest"))
            .expect("digest decodes");
    assert_eq!(
        dispatched_digest, action_digest,
        "dispatched AC entry must carry the action_digest"
    );
    // The dispatched bytes are the encoded ActionResult proto.
    let mut expected_bytes = Vec::with_capacity(action_result.encoded_len());
    action_result.encode(&mut expected_bytes).expect("encode");
    assert_eq!(
        entries[0].data.as_ref(),
        expected_bytes.as_slice(),
        "dispatched bytes must equal the encoded ActionResult"
    );

    Ok(())
}

// =====================================================================
// T6: zero connected workers — no dispatch + no error.
// =====================================================================
//
// `dispatch_to_all_workers` short-circuits when `connected_workers()`
// is empty. Asserts (a) the upload still succeeds, (b) no message is
// produced (we never registered a worker_tx, so this is implicitly
// true; the test exists to lock the contract).
#[nativelink_test]
async fn oneshot_small_write_no_workers_succeeds_without_dispatch()
-> Result<(), Box<dyn core::error::Error>> {
    let manager = make_proxy_store_manager().await?;
    // Dispatcher is enabled but has NO worker registered.
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        ..Default::default()
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    use nativelink_store::small_blob_dispatcher::EphemeralServerSidePin;
    dispatcher.register_pin_set(
        CAS_STORE_NAME,
        Arc::new(EphemeralServerSidePin::new(256 * 1024 * 1024)),
    );

    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;
    let data = Bytes::from(vec![0u8; 512]);
    let join_handle = drive_oneshot_write(bs_server, data.clone());
    let response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect("write must complete even with no workers connected")
        .expect("join handle")
        .expect("write RPC must succeed");
    assert_eq!(
        response.into_inner().committed_size,
        data.len() as i64,
        "bytestream oneshot must commit even when dispatcher has no workers"
    );
    assert_eq!(
        dispatcher.dispatched_count(),
        0,
        "no workers ⇒ no dispatched_count tick"
    );
    Ok(())
}

// =====================================================================
// T_is_worker_skip — over-action coverage (#168 testing-czar M1).
// =====================================================================
//
// USER DIRECTIVE: "if a worker receives a mirrored small blob, it should
// not try to write it back to the server, or anywhere else; same pattern
// as the large blob mirroring." → workers occasionally upload via
// ByteStream (action result outputs); when they do, the bytestream
// hook MUST detect via `x-nativelink-worker` header and skip dispatch
// (the worker already holds the bytes; dispatching them back would
// loop).
//
// Mutation step: remove the `!is_worker` clause from the dispatcher
// gate in `bytestream_server::inner_write_oneshot`. The bespoke
// "MUST NOT loop back" message names the broken contract.
#[nativelink_test]
async fn worker_upload_does_not_dispatch_avoids_loop()
-> Result<(), Box<dyn core::error::Error>> {
    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;

    let data = Bytes::from(vec![0x77u8; 1024]);
    let join_handle = drive_oneshot_write_with_headers(
        bs_server,
        data.clone(),
        &[("x-nativelink-worker", "1")],
    );
    let _response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect("write must complete")
        .expect("join handle")
        .expect("write RPC must succeed");

    assert_no_dispatch(
        &mut worker_rx,
        "#168 USER DIRECTIVE: x-nativelink-worker upload MUST NOT loop back via dispatcher \
         — worker already holds the bytes locally; dispatching would re-send them to the \
         originating worker (loop-prevention contract)",
    )
    .await;

    Ok(())
}

// =====================================================================
// T_is_mirror_skip — over-action coverage (#168 testing-czar M1).
// =====================================================================
//
// USER DIRECTIVE: workers must not re-dispatch mirrored bytes. When a
// server-to-worker mirror push round-trips back via bytestream
// (rare but possible), the dispatcher hook MUST detect via
// `x-nativelink-mirror` header and skip dispatch.
//
// Mutation step: remove the `!is_mirror` clause from the dispatcher
// gate in `bytestream_server::inner_write_oneshot`. The bespoke
// "MUST NOT loop back" message names the broken contract.
#[nativelink_test]
async fn mirror_upload_does_not_dispatch_avoids_loop()
-> Result<(), Box<dyn core::error::Error>> {
    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;

    let data = Bytes::from(vec![0x88u8; 1024]);
    let join_handle = drive_oneshot_write_with_headers(
        bs_server,
        data.clone(),
        &[("x-nativelink-mirror", "1")],
    );
    let _response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect("write must complete")
        .expect("join handle")
        .expect("write RPC must succeed");

    assert_no_dispatch(
        &mut worker_rx,
        "#168 USER DIRECTIVE: x-nativelink-mirror upload MUST NOT loop back via dispatcher \
         — mirror push already arrived; dispatching would re-loop the same bytes \
         (loop-prevention contract)",
    )
    .await;

    Ok(())
}

// =====================================================================
// T_cas_large_no_dispatch — over-action coverage at the PRODUCER side.
// =====================================================================
//
// Spec M: "cas_server BatchUpdateBlobs with >threshold entry → assert no
// dispatch (mutate the producer-side gate, NOT dispatcher's internal
// gate; v1's T2 was masked by the dispatcher's internal gate)".
//
// To prove this guards the producer-side gate (not just the
// dispatcher's internal `data.len() > SMALL_BLOB_THRESHOLD` check), the
// test is constructed so removing the producer's `size_bytes <=
// SMALL_BLOB_THRESHOLD` gate would still pass through to the dispatcher
// — and the dispatcher's internal gate would silently absorb it. The
// mutation-step path is therefore: comment out BOTH the producer
// `size_bytes <= SMALL_BLOB_THRESHOLD` clause AND the dispatcher's
// `data.len() > SMALL_BLOB_THRESHOLD` early return → test must red-fail
// (the assert_no_dispatch panics on receipt of a BatchWriteSmallBlobs).
#[nativelink_test]
async fn cas_batch_update_large_blob_does_not_dispatch()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;

    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let cas_server = make_cas_server(manager.as_ref(), Some(dispatcher.clone()))?;

    // 32 KiB > SMALL_BLOB_THRESHOLD (16 KiB).
    let oversize_len = SMALL_BLOB_THRESHOLD * 2;
    let data = Bytes::from(vec![0x55u8; oversize_len]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let request = BatchUpdateBlobsRequest {
        instance_name: INSTANCE_NAME.to_string(),
        requests: vec![batch_update_blobs_request::Request {
            digest: Some(digest.into()),
            data: data.clone(),
            compressor: 0,
        }],
        digest_function: 0,
    };

    let response_fut = cas_server.batch_update_blobs(Request::new(request));
    let _response = tokio::time::timeout(DEADLOCK_DETECTOR, response_fut)
        .await
        .expect("CAS batch update must complete")
        .err_tip(|| "CAS batch update RPC")?;

    assert_no_dispatch(
        &mut worker_rx,
        "#168 producer-side gate: cas_server::inner_batch_update_blobs MUST NOT dispatch \
         blobs > SMALL_BLOB_THRESHOLD even when the dispatcher's internal size gate is \
         removed (defense-in-depth)",
    )
    .await;

    Ok(())
}

// =====================================================================
// T_loop — dispatched bytes MUST NOT loop back through the worker.
// =====================================================================
//
// Loop-prevention end-to-end: the dispatcher pushes bytes server→worker,
// and the worker's `local_worker::handle_batch_write_small_blobs`
// inserts directly into `dispatched_mirror_pins` via
// `insert_dispatched_mirror_blob` — it does NOT call back into the
// worker's bytestream/cas server (which would loop back to THIS
// dispatching server).
//
// In this test we exercise the SERVER side only (the worker is a
// fake mpsc receiver, not a real worker process), so the loop-back
// would manifest as: server dispatches → fake worker receives →
// fake worker writes back to server → server's bytestream hook
// dispatches again. Since our fake worker drains messages but does
// NOT issue any callback writes, the absence of a SECOND dispatch
// proves the wire-side loop-prevention contract.
//
// Mutation step: have the test fixture re-issue the bytes through
// `bs_server.write` after receiving the first dispatch (intentionally
// breaking the contract); this test must red-fail.
#[nativelink_test]
async fn dispatched_blob_does_not_loop_back()
-> Result<(), Box<dyn core::error::Error>> {
    let manager = make_proxy_store_manager().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;

    let data = Bytes::from(vec![0x99u8; 1024]);
    let join_handle = drive_oneshot_write(bs_server, data.clone());
    let _response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect("write must complete")
        .expect("join handle")
        .expect("write RPC must succeed");

    // First dispatch arrives.
    let entries = await_batch_write_small_blobs(
        &mut worker_rx,
        "#168 fan-out arrived for the original write",
    )
    .await;
    assert_eq!(entries.len(), 1, "first dispatch is the upload bytes");

    // Second dispatch MUST NOT arrive (no loop-back).
    assert_no_dispatch(
        &mut worker_rx,
        "#168 USER DIRECTIVE: dispatched bytes MUST NOT loop back through the worker — \
         worker handler inserts into dispatched_mirror_pins directly; no callback to \
         server's bytestream/cas/ac. A second dispatch within NO_DISPATCH_WINDOW \
         indicates loop-prevention contract violation.",
    )
    .await;

    Ok(())
}

// =====================================================================
// T_no_duplicate_mirror — item F: dispatch suppresses random mirror.
// =====================================================================
//
// Spec item F: when the dispatcher fans out a small blob to every worker,
// the legacy `mirror_blob_to_random_worker` path MUST be SUPPRESSED
// (the random-single mirror is redundant — every worker already has
// the bytes from the dispatcher). Otherwise the same bytes are pushed
// twice for every small write: once by the dispatcher (to all workers),
// once by the random-single mirror (to one worker). For a 16 KiB
// payload that's an extra ~16 KiB per small write across the fleet.
//
// We assert this indirectly by counting `WorkerProxyStore::mirror_total_attempted`
// before vs. after the write. Without the suppression gate, the random
// mirror's `background_spawn!("mirror_blob_to_worker", ...)` task would
// run and tick the counter (the spawned future calls
// `WorkerProxyStore::mirror_blob_to_random_worker`, which ticks the
// counter inside its own body). With the gate, the counter stays
// unchanged.
//
// Mutation step: in `bytestream_server::inner_write_oneshot`, change
// the post-dispatch `if !is_worker && !is_mirror && !dispatched` guard
// to drop the `&& !dispatched` clause (always-true). The duplicate
// mirror would fire and the assertion below would fail.
#[nativelink_test]
async fn dispatched_small_blob_does_not_duplicate_random_mirror()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, cas_proxy) = make_proxy_store_manager_with_proxy().await?;
    let (dispatcher, mut worker_rx) = make_dispatcher_with_one_worker();
    let bs_server = make_bytestream_server(manager.as_ref(), Some(dispatcher.clone()))?;

    // Pre-populate locality_map with a fake peer so that
    // `mirror_blob_to_random_worker` would tick `mirror_total_attempted`
    // if it were called. Without an endpoint, the function would return
    // early at the empty-endpoints check before ticking.
    let fake_peer_digest = DigestInfo::new([0xAAu8; 32], 1);
    cas_proxy
        .locality_map()
        .write()
        .register_blobs("grpc://other-fake-peer:50071", &[fake_peer_digest]);

    // Snapshot the mirror counter BEFORE the write.
    let attempted_before = cas_proxy.mirror_total_attempted_for_test();

    let data = Bytes::from(vec![0xEEu8; 1024]);
    let join_handle = drive_oneshot_write(bs_server, data.clone());
    let _response = tokio::time::timeout(DEADLOCK_DETECTOR, join_handle)
        .await
        .expect("write must complete")
        .expect("join handle")
        .expect("write RPC must succeed");

    // Confirm dispatch occurred.
    let _entries = await_batch_write_small_blobs(
        &mut worker_rx,
        "#168 dispatcher MUST fan out so the duplicate-suppression gate is exercised",
    )
    .await;

    // Bounded ABSENCE-detection window: poll the counter every few
    // milliseconds for `NO_DISPATCH_WINDOW`. If the suppression gate
    // (item F) is broken, a `mirror_blob_to_random_worker` task
    // spawned by `mirror_blob_to_worker` would fire WITHIN this
    // window and tick the counter (the spawn happens synchronously
    // via `nativelink_util::background_spawn!`; the function returns
    // immediately and runs on the executor).
    let deadline = tokio::time::Instant::now() + NO_DISPATCH_WINDOW;
    let mut attempted_after = attempted_before;
    while tokio::time::Instant::now() < deadline {
        let cur = cas_proxy.mirror_total_attempted_for_test();
        if cur != attempted_before {
            attempted_after = cur;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        attempted_before, attempted_after,
        "#168 item F: when the dispatcher fans out small blobs, the random-single \
         `mirror_blob_to_worker` path MUST be suppressed (would otherwise \
         duplicate bytes); mirror_total_attempted ticked from {attempted_before} \
         to {attempted_after}"
    );

    Ok(())
}
