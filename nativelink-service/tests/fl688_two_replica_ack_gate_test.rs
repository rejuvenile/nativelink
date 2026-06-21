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

//! #FL-688: HARD ≥2-replica ack-gate — production-composition end-to-end
//! tests for the CAS write handlers.
//!
//! These wrap the REAL `CasServer::batch_update_blobs` and
//! `ByteStreamServer::write` handlers over a production-shaped
//! `WorkerProxyStore` wrapper (matching `nativelink.rs`'s wrap order), with
//! injected fake worker connections and a registered locality map, so the
//! ack-gate, mirror-confirm, backpressure, retry, and no-peer-degraded paths
//! are exercised through the exact seams production uses.
//!
//! Seams crossed (per `.claude/rules/testing-contracts.md` identify-the-seam):
//!   producer = handler store write → ack-gate orchestrator
//!   (`WorkerProxyStore::ack_gated_write` / `confirm_chunked_replica`) →
//!   `mirror_and_confirm_data` / `confirm_via_slow_store` → gRPC status.
//!
//! Mutation guide (TDD step 5) per test is in each test's doc-comment.

use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::BodyExt;
use nativelink_config::cas_server::{ByteStreamConfig, CasStoreConfig, WithInstanceName};
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchUpdateBlobsRequest, batch_update_blobs_request,
};
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::cas_server::CasServer;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{background_spawn, spawn};
use tonic::codec::{Codec, CompressionEncoding};
use tonic::{Request, Response, Streaming};
use tonic_prost::ProstCodec;

const DEADLOCK_DETECTOR: Duration = Duration::from_secs(5);
const INSTANCE_NAME: &str = "main";
const CAS_STORE_NAME: &str = "cas_STORE";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

/// Build a production-shaped `StoreManager` whose `cas_STORE` is a
/// `WorkerProxyStore` wrapping `inner`. Returns the manager + the proxy (to
/// inject worker connections + register locality).
fn make_manager(inner: Store) -> (Arc<StoreManager>, Arc<WorkerProxyStore>) {
    let manager = Arc::new(StoreManager::new());
    let locality = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner, locality);
    manager.add_store(CAS_STORE_NAME, Store::new(proxy.clone()));
    (manager, proxy)
}

/// Inner = plain MemoryStore (reports `SubscribesToUpdateOneshot`, so a
/// single-shot bytestream write takes the `inner_write_oneshot` path →
/// `WorkerProxyStore::ack_gated_write`). Also the CAS batch handler's store.
fn make_manager_memory() -> (Arc<StoreManager>, Arc<WorkerProxyStore>) {
    make_manager(Store::new(MemoryStore::new(&MemorySpec::default())))
}

/// Inner = FastSlowStore{fast: Memory, slow: `slow`} — does NOT report
/// `SubscribesToUpdateOneshot`, so a single-shot bytestream write takes the
/// CHUNKED `inner_write` path → the tee + `confirm_chunked_replica`. This is
/// the PRODUCTION bytestream CAS path.
///
/// WHY the prod chain also takes the chunked path (premise correction, BLOCK-1,
/// SHA 139a0653 — verified against `prod-server.json5:169-230`): the prod chain is
/// `WorkerProxyStore → VerifyStore(cas_STORE) → ExistenceCacheStore(50M) →
/// SizePartitioningStore(16384) → {Redis | FastSlow}`. `ByteStreamServer::write`
/// queries `store.optimized_for(SubscribesToUpdateOneshot)`, which is
/// `WorkerProxyStore::optimized_for` delegating to
/// `self.inner.inner_store(None).optimized_for(...)`. `self.inner` is the
/// `VerifyStore`, whose `inner_store(None)` returns `self` (`verify_store.rs:519`)
/// and which has NO `optimized_for` override → the trait default `false`
/// (`store_trait.rs:1051`). So delegation STOPS at VerifyStore reporting `false`
/// → oneshot is dead in prod. It is NOT dead because "no store in the chain
/// reports it" — `ExistenceCacheStore::optimized_for` DOES report `true`
/// (`existence_cache_store.rs:661`), but it is SHADOWED by VerifyStore, which the
/// runtime consults first and never descends past. This test composition reaches
/// the chunked path for a DIFFERENT reason (it omits VerifyStore and FSS itself
/// reports `false`), but the resulting code path — chunked `inner_write` + tee +
/// `confirm_chunked_replica` — is identical to prod.
fn make_manager_fast_slow(slow: Store) -> (Arc<StoreManager>, Arc<WorkerProxyStore>) {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
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
    make_manager(fss)
}

fn make_cas_server(manager: &StoreManager) -> Result<Arc<CasServer>, Error> {
    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: CasStoreConfig {
            cas_store: CAS_STORE_NAME.to_string(),
        },
    }];
    Ok(Arc::new(CasServer::new(&config, manager, None)?))
}

fn make_bytestream_server(manager: &StoreManager) -> Result<Arc<ByteStreamServer>, Error> {
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

/// A throwaway digest that is NEVER the blob under test. Registering an
/// endpoint against THIS digest puts the endpoint into `all_endpoints()`
/// (so `mirror_and_confirm_data` routes to it) WITHOUT making
/// `has(target_digest)` report the target as already present — which would
/// make the batch/oneshot handler skip the write entirely (the production
/// fast-path). Models "worker is connected and holds other blobs, but not
/// this one yet" — the real pre-mirror state.
fn dummy_locality_digest() -> DigestInfo {
    DigestInfo::try_new(
        "ffffffffffffffff000000000000000000000000000000000fffffffffffffff",
        7,
    )
    .expect("valid dummy digest")
}

/// Register `endpoint`'s fake connection and make it appear in
/// `all_endpoints()` (via a DUMMY locality entry — see
/// [`dummy_locality_digest`]) WITHOUT pre-registering the target blob.
fn register_worker(proxy: &WorkerProxyStore, endpoint: &str, store: Store, _digest: DigestInfo) {
    proxy.inject_worker_connection(endpoint, store);
    proxy
        .locality_map()
        .write()
        .register_blobs(endpoint, &[dummy_locality_digest()]);
}

/// Drive a single oneshot `ByteStream::write` (finish_write on first chunk).
fn drive_oneshot_write(
    bs_server: Arc<ByteStreamServer>,
    data: Bytes,
) -> JoinHandleDropGuard<
    Result<Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
> {
    let (tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream =
        Streaming::new_request(codec.decoder(), body, Some(CompressionEncoding::Gzip), None);
    let resource_name = format!(
        "{}/uploads/{}/blobs/{}/{}",
        INSTANCE_NAME, "11111111-1111-1111-1111-111111111111", HASH1, data.len(),
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
    });
    let request = Request::new(stream);
    spawn!("bs_server_write", async move { bs_server.write(request).await })
}

fn make_batch_request(digest: DigestInfo, data: Bytes) -> BatchUpdateBlobsRequest {
    BatchUpdateBlobsRequest {
        instance_name: INSTANCE_NAME.to_string(),
        requests: vec![batch_update_blobs_request::Request {
            digest: Some(digest.into()),
            data,
            compressor: 0,
        }],
        digest_function: 0,
    }
}

// ===================================================================
// Fake worker stores.
// ===================================================================

/// Always-failing fake worker (non-saturation, non-connection error).
#[derive(MetricsComponent, Default)]
struct AlwaysFailPeer {
    _marker: (),
}

#[async_trait]
impl StoreDriver for AlwaysFailPeer {
    async fn has_with_results(
        self: core::pin::Pin<&Self>,
        _keys: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn update(
        self: core::pin::Pin<&Self>,
        _key: StoreKey<'_>,
        mut rx: DropCloserReadHalf,
        _size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let _drain_result = rx.drain().await;
        Err(make_err!(Code::Internal, "AlwaysFailPeer: simulated worker mirror failure"))
    }
    async fn get_part(
        self: core::pin::Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(Code::Unimplemented, "no get_part"))
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
        Err(make_err!(Code::Unimplemented, "no callbacks"))
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
}

#[async_trait]
impl HealthStatusIndicator for AlwaysFailPeer {
    fn get_name(&self) -> &'static str {
        "AlwaysFailPeer"
    }
    async fn check_health(&self, namespace: std::borrow::Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(core::pin::Pin::new(self), namespace).await
    }
}

/// Slow-tier fake whose `update`/`get_part` SLEEPS `delay` before writing into
/// a real MemoryStore. Lets the no-peer test prove the ack is AWAITED on the
/// slow write by TIMING: with the await, the RPC cannot return until the
/// slow write resolves (≥ `delay`); the FastSlowStore's pre-existing ASYNC
/// slow write is DETACHED and does not delay the RPC, so a non-awaited
/// (mutated) no-peer path returns in ~0ms.
#[derive(MetricsComponent)]
struct DelayedSlowStore {
    #[metric(group = "backing")]
    backing: Store,
    delay: Duration,
}

#[async_trait]
impl StoreDriver for DelayedSlowStore {
    async fn has_with_results(
        self: core::pin::Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.backing.as_store_driver_pin().has_with_results(keys, results).await
    }
    async fn update(
        self: core::pin::Pin<&Self>,
        key: StoreKey<'_>,
        rx: DropCloserReadHalf,
        size: UploadSizeInfo,
    ) -> Result<(), Error> {
        tokio::time::sleep(self.delay).await;
        self.backing.as_store_driver_pin().update(key, rx, size).await
    }
    async fn get_part(
        self: core::pin::Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.backing.as_store_driver_pin().get_part(key, writer, offset, length).await
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
        Err(make_err!(Code::Unimplemented, "no callbacks"))
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
}

#[async_trait]
impl HealthStatusIndicator for DelayedSlowStore {
    fn get_name(&self) -> &'static str {
        "DelayedSlowStore"
    }
    async fn check_health(&self, namespace: std::borrow::Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(core::pin::Pin::new(self), namespace).await
    }
}

// ===================================================================
// Tests.
// ===================================================================

/// ACK-GATE (batch path): with one HEALTHY worker, a `BatchUpdateBlobs`
/// returns code=0 AND the worker physically holds the blob — confirming the
/// ack gated on the 2nd replica actually landing, not a bare Ok.
///
/// Mutation: change `ack_gated_write`'s `MirrorConfirmOutcome::Confirmed`
/// arm to return Ok WITHOUT requiring the mirror (or have the batch handler
/// skip `ack_gated_write`) → the peer would be empty → this test red-fails on
/// the peer-holds-blob assertion.
#[nativelink_test]
async fn batch_ack_gate_confirms_on_healthy_worker() -> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy) = make_manager_memory();
    let cas_server = make_cas_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x42u8; 32768]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let peer = Store::new(MemoryStore::new(&MemorySpec::default()));
    register_worker(&proxy, "grpc://healthy:50071", peer.clone(), digest);

    let response = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        cas_server.batch_update_blobs(Request::new(make_batch_request(digest, data.clone()))),
    )
    .await
    .expect("batch must not hang — ack-gate must resolve against a healthy worker")
    .err_tip(|| "batch RPC")?
    .into_inner();

    assert_eq!(
        response.responses[0].status.as_ref().map(|s| s.code).unwrap_or(-1),
        0,
        "batch blob must ack code=0 once the worker confirms the 2nd replica"
    );
    let on_peer = peer.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        on_peer.as_ref(),
        data.as_ref(),
        "ack-gate must mean the 2nd replica physically landed on the worker — \
         not a bare Ok (mutation: bypass mirror_and_confirm in ack_gated_write)"
    );
    Ok(())
}

/// ACK-GATE refusal (batch path): peers EXIST but the only worker FAILS. The
/// handler must NOT ack (refuse <2 replicas) — the per-blob status is non-zero.
///
/// Mutation: make `ack_gated_write`'s `Failed` arm return `Ok(())` → this
/// test red-fails (the blob would ack code=0 despite no 2nd replica).
#[nativelink_test]
async fn batch_ack_gate_refuses_when_only_worker_fails()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy) = make_manager_memory();
    let cas_server = make_cas_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x37u8; 32768]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let bad = Store::new(Arc::new(AlwaysFailPeer::default()));
    register_worker(&proxy, "grpc://bad:50071", bad, digest);

    let response = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        cas_server.batch_update_blobs(Request::new(make_batch_request(digest, data.clone()))),
    )
    .await
    .expect("batch must not hang even when the only worker fails")
    .err_tip(|| "batch RPC")?
    .into_inner();

    let code = response.responses[0].status.as_ref().map(|s| s.code).unwrap_or(-1);
    assert_ne!(
        code, 0,
        "peers exist but the only worker failed — the handler MUST refuse to \
         ack with <2 replicas (mutation: make ack_gated_write Failed arm Ok). \
         Got code={code}"
    );
    Ok(())
}

/// RETRY (batch path): first worker fails, a second healthy worker confirms.
/// The blob acks code=0 and the SECOND worker holds the bytes.
///
/// Mutation: kill the cross-endpoint retry loop in `mirror_and_confirm_data`
/// (return Failed on first failure) → first-listed bad worker → red-fail.
#[nativelink_test]
async fn batch_ack_gate_retries_to_second_worker()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy) = make_manager_memory();
    let cas_server = make_cas_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x55u8; 32768]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let bad = Store::new(Arc::new(AlwaysFailPeer::default()));
    let good = Store::new(MemoryStore::new(&MemorySpec::default()));
    register_worker(&proxy, "grpc://bad:50071", bad, digest);
    register_worker(&proxy, "grpc://good:50071", good.clone(), digest);

    let response = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        cas_server.batch_update_blobs(Request::new(make_batch_request(digest, data.clone()))),
    )
    .await
    .expect("batch must not hang while retrying endpoints")
    .err_tip(|| "batch RPC")?
    .into_inner();

    assert_eq!(
        response.responses[0].status.as_ref().map(|s| s.code).unwrap_or(-1),
        0,
        "must retry past the failing worker to the healthy one and ack code=0"
    );
    let on_good = good.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        on_good.as_ref(),
        data.as_ref(),
        "the confirmed 2nd replica must be on the healthy second worker"
    );
    Ok(())
}

/// SMALL-blob ack-gate (batch path, FSS inner): a ≤ `SMALL_BLOB_THRESHOLD`
/// blob confirms its 2nd replica on the SLOW TIER (production durable replica
/// = Redis `SMALL_CAS_CACHED`), NOT the worker mirror. The blob acks code=0
/// AND the slow store holds the bytes; the worker mirror is NOT contacted
/// (small-blob acks are decoupled from worker reachability).
///
/// We register a worker endpoint (so `all_endpoints()` is non-empty) but the
/// small-blob path must NOT route to it — the slow tier carries the replica.
/// Uses a DELAYED slow tier + latency floor to prove the ack AWAITS it.
///
/// Mutation: change the batch handler's `confirm_via_slow_tier` to always
/// `false` (route small blobs to the worker mirror) → with no injected
/// connection for the registered endpoint, the mirror confirm fails/hangs and
/// the ack would NOT rest on the awaited slow tier → the latency floor fails
/// (or the RPC errors). Equivalently, make `ack_gated_write`'s slow-tier arm
/// skip `confirm_via_slow_store` → latency < floor.
#[nativelink_test]
async fn batch_small_blob_confirms_via_slow_tier() -> Result<(), Box<dyn core::error::Error>> {
    const SLOW_DELAY: Duration = Duration::from_millis(400);
    const LATENCY_FLOOR: Duration = Duration::from_millis(250);

    let slow_backing = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(Arc::new(DelayedSlowStore {
        backing: slow_backing.clone(),
        delay: SLOW_DELAY,
    }));
    let (manager, proxy) = make_manager_fast_slow(slow);
    let cas_server = make_cas_server(manager.as_ref())?;

    // 4 KiB ≤ SMALL_BLOB_THRESHOLD (16 KiB) → slow-tier confirm.
    let data = Bytes::from(vec![0x77u8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    // Register a HEALTHY, REACHABLE worker. The small-blob path must IGNORE it
    // (confirm via slow tier), so the ~0ms worker confirm must NOT be what the
    // ack waits on — the ~400ms slow tier must. If a mutation routes small
    // blobs to the worker mirror instead, the ack would return in ~0ms (worker
    // confirms instantly) and the latency floor would catch it.
    let peer = Store::new(MemoryStore::new(&MemorySpec::default()));
    register_worker(&proxy, "grpc://healthy:50071", peer, digest);

    let start = std::time::Instant::now();
    let response = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        cas_server.batch_update_blobs(Request::new(make_batch_request(digest, data.clone()))),
    )
    .await
    .expect("batch small-blob must not hang on the slow-tier confirm")
    .err_tip(|| "batch RPC")?
    .into_inner();
    let elapsed = start.elapsed();

    assert_eq!(
        response.responses[0].status.as_ref().map(|s| s.code).unwrap_or(-1),
        0,
        "small-blob batch must ack code=0 once the slow tier confirms"
    );
    assert!(
        elapsed >= LATENCY_FLOOR,
        "small-blob ack-gate MUST await the SLOW-TIER write before acking \
         (slow tier ~{SLOW_DELAY:?}); observed {elapsed:?} < {LATENCY_FLOOR:?} \
         means the ack did NOT confirm the 2nd replica on the slow tier \
         (mutation: confirm_via_slow_tier=false, or skip confirm_via_slow_store)."
    );
    // The bytes physically landed on the slow tier (the confirmed replica).
    let on_slow = slow_backing.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        on_slow.as_ref(),
        data.as_ref(),
        "small-blob 2nd replica must physically be on the slow tier"
    );
    Ok(())
}

/// NO-PEER degraded (bytestream oneshot, FSS inner with a DELAYED slow tier):
/// zero workers → the write must AWAIT the slow-tier write before acking.
///
/// The slow tier sleeps `SLOW_DELAY` before its write resolves. The ack-gate
/// AWAITS `confirm_via_slow_store`, so the RPC cannot return until ≥ that delay.
/// The FastSlowStore's pre-existing ASYNC slow write is DETACHED and does NOT
/// delay the RPC, so a non-awaited (mutated) no-peer path returns in ~0ms.
/// We assert the RPC's wall-clock latency ≥ a floor below `SLOW_DELAY`.
///
/// Mutation: make `confirm_chunked_replica`'s `NoPeers` arm return `Ok(())`
/// without `confirm_via_slow_store` → the RPC returns in ~0ms (only the
/// detached async FSS slow write is delayed) → the latency-floor assertion
/// red-fails.
#[nativelink_test]
async fn chunked_no_peer_awaits_slow_tier() -> Result<(), Box<dyn core::error::Error>> {
    const SLOW_DELAY: Duration = Duration::from_millis(400);
    // Floor cleanly above the ~0ms a non-awaited path takes and below the
    // ~400ms an awaited path takes (margin for scheduling jitter both ways).
    const LATENCY_FLOOR: Duration = Duration::from_millis(250);

    let slow_backing = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(Arc::new(DelayedSlowStore {
        backing: slow_backing,
        delay: SLOW_DELAY,
    }));
    let (manager, _proxy) = make_manager_fast_slow(slow);
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x11u8; 4096]);

    let start = std::time::Instant::now();
    let response = tokio::time::timeout(DEADLOCK_DETECTOR, drive_oneshot_write(bs_server, data.clone()))
        .await
        .expect("write must complete with no peers (degraded slow-tier path)")
        .expect("join")
        .err_tip(|| "write RPC")?
        .into_inner();
    let elapsed = start.elapsed();

    assert_eq!(
        response.committed_size,
        data.len() as i64,
        "no-peer oneshot must ack committed_size once the slow tier confirms"
    );
    assert!(
        elapsed >= LATENCY_FLOOR,
        "no-peer degraded oneshot MUST await the slow-tier write before acking \
         (slow tier takes ~{SLOW_DELAY:?}); observed RPC latency {elapsed:?} < \
         {LATENCY_FLOOR:?} means the ack did NOT wait for the 2nd replica \
         (mutation: NoPeers arm returns Ok without confirm_via_slow_store)."
    );
    Ok(())
}

/// ACK-GATE (bytestream ONESHOT path — MemoryStore inner reports
/// `SubscribesToUpdateOneshot`, so `inner_write_oneshot` →
/// `WorkerProxyStore::ack_gated_write`): with a healthy worker the write acks
/// AND the worker holds the blob.
#[nativelink_test]
async fn oneshot_ack_gate_confirms_on_healthy_worker()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy) = make_manager_memory();
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x22u8; 32768]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let peer = Store::new(MemoryStore::new(&MemorySpec::default()));
    register_worker(&proxy, "grpc://healthy:50071", peer.clone(), digest);

    let response = tokio::time::timeout(DEADLOCK_DETECTOR, drive_oneshot_write(bs_server, data.clone()))
        .await
        .expect("oneshot write must not hang against a healthy worker")
        .expect("join")
        .err_tip(|| "write RPC")?
        .into_inner();
    assert_eq!(response.committed_size, data.len() as i64, "oneshot write must ack");

    let on_peer = peer.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        on_peer.as_ref(),
        data.as_ref(),
        "ack-gate must place the 2nd replica on the worker before acking"
    );
    Ok(())
}

/// ACK-GATE refusal (bytestream ONESHOT path → `ack_gated_write`): the only
/// worker FAILS the mirror; the handler must refuse to ack (<2 replicas) — the
/// write RPC returns Err.
///
/// Mutation: make `ack_gated_write`'s `Failed` arm return `Ok(())` → the RPC
/// acks despite no 2nd replica → red-fails on the `is_err()` expectation.
#[nativelink_test]
async fn oneshot_ack_gate_refuses_when_only_worker_fails()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, proxy) = make_manager_memory();
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x99u8; 32768]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let bad = Store::new(Arc::new(AlwaysFailPeer::default()));
    register_worker(&proxy, "grpc://bad:50071", bad, digest);

    let result = tokio::time::timeout(DEADLOCK_DETECTOR, drive_oneshot_write(bs_server, data.clone()))
        .await
        .expect("oneshot write must not hang even when the only worker fails")
        .expect("join");

    assert!(
        result.is_err(),
        "peers exist but the only worker failed — the oneshot handler MUST \
         refuse to ack with <2 replicas (mutation: ack_gated_write Failed arm \
         returns Ok). Got Ok response."
    );
    Ok(())
}

/// ACK-GATE (bytestream CHUNKED path — FSS inner does NOT report
/// `SubscribesToUpdateOneshot`, so `inner_write` + the tee +
/// `confirm_chunked_replica` run; this is the PRODUCTION bytestream CAS path):
/// with a healthy worker the write acks AND the worker holds the blob.
#[nativelink_test]
async fn chunked_ack_gate_confirms_on_healthy_worker()
-> Result<(), Box<dyn core::error::Error>> {
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, proxy) = make_manager_fast_slow(slow);
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x22u8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let peer = Store::new(MemoryStore::new(&MemorySpec::default()));
    register_worker(&proxy, "grpc://healthy:50071", peer.clone(), digest);

    let response = tokio::time::timeout(DEADLOCK_DETECTOR, drive_oneshot_write(bs_server, data.clone()))
        .await
        .expect("chunked write must not hang against a healthy worker")
        .expect("join")
        .err_tip(|| "write RPC")?
        .into_inner();
    assert_eq!(response.committed_size, data.len() as i64, "chunked write must ack");

    let on_peer = peer.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        on_peer.as_ref(),
        data.as_ref(),
        "chunked ack-gate must place the 2nd replica on the worker before acking"
    );
    Ok(())
}

/// ACK-GATE refusal (bytestream CHUNKED path): the tee mirror FAILS and the
/// only worker is bad, so `confirm_chunked_replica`'s re-drive from the stored
/// copy ALSO fails. The handler must refuse to ack (<2 replicas) — the write
/// RPC returns Err.
///
/// This is the load-bearing GATE for the chunked path: `inner_write` AWAITs
/// the tee handle's outcome and `confirm_chunked_replica` propagates Err on an
/// unrecoverable mirror failure (vs the pre-FL-688 fire-and-forget
/// `drop(mirror_handle)`).
///
/// Mutation: revert `confirm_chunked_replica(...).await?` in `inner_write` to
/// fire-and-forget `drop(mirror_handle)` → the RPC acks Ok despite no 2nd
/// replica → red-fails on the `is_err()` expectation.
#[nativelink_test]
async fn chunked_ack_gate_refuses_when_only_worker_fails()
-> Result<(), Box<dyn core::error::Error>> {
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, proxy) = make_manager_fast_slow(slow);
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x99u8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let bad = Store::new(Arc::new(AlwaysFailPeer::default()));
    register_worker(&proxy, "grpc://bad:50071", bad, digest);

    let result = tokio::time::timeout(DEADLOCK_DETECTOR, drive_oneshot_write(bs_server, data.clone()))
        .await
        .expect("chunked write must not hang even when the only worker fails")
        .expect("join");

    assert!(
        result.is_err(),
        "peers exist but the only worker failed the tee AND the re-drive — the \
         chunked handler MUST refuse to ack with <2 replicas (mutation: revert \
         confirm_chunked_replica.await? to fire-and-forget drop(mirror_handle)). \
         Got Ok response."
    );
    Ok(())
}

/// SMALL blob (≤ `SMALL_BLOB_THRESHOLD` = 16 KiB) via `ByteStream::write`,
/// PEERS PRESENT — the scenario the false "oneshot→Redis" premise HID
/// (red-team blind-spot #2, distributed-systems-reviewer BLOCK-1, SHA 139a0653).
///
/// In production every ByteStream Write takes the CHUNKED `inner_write` path
/// (oneshot is dead — VerifyStore stops the `optimized_for` delegation), and the
/// chunked path's `confirm_chunked_replica` has NO `confirm_via_slow_tier`
/// small-blob branch: with peers present it gates on a WORKER-MIRROR confirm
/// (`MirrorConfirmOutcome::Confirmed`), NOT on Redis. So a small ByteStream blob
/// is worker-mirror-gated, contradicting the design's "small blobs are decoupled
/// from worker reachability via Redis" model (which only held for the dead
/// oneshot path). This test pins that ACTUAL behavior: a 4 KiB ByteStream write
/// with a healthy worker lands its 2nd replica ON THE WORKER.
///
/// (The accepted gap — no Redis-confirm + no SmallBlobDispatcher fan-out on this
/// path — is tracked in #FL-689; see the commit's Behavior-changes section.)
///
/// Mutation: revert `inner_write`'s `confirm_chunked_replica(...).await?` to
/// fire-and-forget `drop(mirror_handle)` → the RPC acks WITHOUT awaiting the
/// worker-mirror confirm → returns in ~0ms (the latency-floor assertion fails)
/// AND the still-in-flight slow mirror has not yet landed on the worker (the
/// peer-holds-blob assertion fails).
///
/// The worker peer is wrapped in a `DelayedSlowStore` (its `update` SLEEPS
/// `MIRROR_DELAY`) so the await-gate's effect is observable by TIMING: with the
/// gate the RPC cannot return until the mirror's slow `update` resolves; a
/// fire-and-forget (mutated) gate returns immediately while that write is still
/// in flight — deterministically empty peer at read-back. (Without the delay the
/// in-process tee can populate a plain MemoryStore peer before the read-back,
/// masking the mutation.)
#[nativelink_test]
async fn chunked_small_blob_bytestream_gates_on_worker_mirror()
-> Result<(), Box<dyn core::error::Error>> {
    const MIRROR_DELAY: Duration = Duration::from_millis(400);
    const LATENCY_FLOOR: Duration = Duration::from_millis(250);

    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, proxy) = make_manager_fast_slow(slow.clone());
    let bs_server = make_bytestream_server(manager.as_ref())?;

    // 4 KiB ≤ SMALL_BLOB_THRESHOLD (16 KiB). Driven through ByteStream::write
    // (NOT the batch path) → chunked `inner_write` (FSS inner does not report
    // SubscribesToUpdateOneshot), the live production CAS ByteStream path.
    let data = Bytes::from(vec![0x5au8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    // The peer's `update` (the mirror write target) sleeps MIRROR_DELAY before
    // landing the blob into the real backing MemoryStore.
    let peer_backing = Store::new(MemoryStore::new(&MemorySpec::default()));
    let peer = Store::new(Arc::new(DelayedSlowStore {
        backing: peer_backing.clone(),
        delay: MIRROR_DELAY,
    }));
    register_worker(&proxy, "grpc://healthy:50071", peer, digest);

    let start = std::time::Instant::now();
    let response = tokio::time::timeout(DEADLOCK_DETECTOR, drive_oneshot_write(bs_server, data.clone()))
        .await
        .expect("small-blob chunked write must not hang against a healthy worker")
        .expect("join")
        .err_tip(|| "write RPC")?
        .into_inner();
    let elapsed = start.elapsed();
    assert_eq!(response.committed_size, data.len() as i64, "small-blob chunked write must ack");

    // (1) The ack WAITED for the (slow) worker-mirror confirm — proving the
    // chunked path gates a small blob on the WORKER MIRROR, not Redis.
    assert!(
        elapsed >= LATENCY_FLOOR,
        "small (≤16 KiB) ByteStream Write takes the chunked path, which gates on \
         a WORKER-MIRROR confirm — the ack MUST await the (slow ~{MIRROR_DELAY:?}) \
         mirror write; observed {elapsed:?} < {LATENCY_FLOOR:?} means it did NOT \
         (mutation: revert confirm_chunked_replica.await? to fire-and-forget \
         drop(mirror_handle))"
    );
    // (2) The 2nd replica physically landed on the WORKER MIRROR before the ack
    // — NOT decoupled to the slow tier (the behavior the false oneshot→Redis
    // premise hid).
    let on_peer = peer_backing.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        on_peer.as_ref(),
        data.as_ref(),
        "small-blob chunked ByteStream Write MUST place its 2nd replica on the \
         WORKER before acking (mutation: fire-and-forget drop(mirror_handle) → \
         peer empty at read-back)"
    );
    Ok(())
}

/// SMALL blob (≤ 16 KiB) via `ByteStream::write`, NO PEERS — the degraded
/// chunked path lands the 2nd replica on the SLOW TIER (Redis `SMALL_CAS_CACHED`
/// lower SizePartitioning arm in prod; the FSS slow store in this composition).
///
/// This is the durability floor for the small-blob-via-ByteStream gap: even
/// without worker read-locality fan-out, durability HOLDS because the chunked
/// `NoPeers` arm reads the pinned fast-tier copy back and AWAITS
/// `confirm_via_slow_store` before acking. Pairs with
/// `chunked_small_blob_bytestream_gates_on_worker_mirror` (peers-present) to
/// cover BOTH 2nd-replica destinations for the path the false premise hid.
///
/// The slow tier is wrapped in `DelayedSlowStore` (its `update` SLEEPS
/// `SLOW_DELAY`). The load-bearing assertion is the LATENCY FLOOR: the RPC
/// cannot return until the AWAITED `confirm_via_slow_store` write resolves. A
/// landing assertion alone is NOT mutation-resistant here — the FastSlowStore's
/// pre-existing DETACHED async slow write also eventually lands the blob on the
/// same slow store, so "blob present" does not prove the ack waited for it. The
/// detached write does NOT delay the RPC; only the awaited confirm does.
///
/// Mutation: make `confirm_chunked_replica`'s `NoPeers` arm return `Ok(())`
/// without `confirm_via_slow_store` → the RPC returns in ~0ms (only the detached
/// FSS async write carries the delay) → the latency-floor assertion red-fails.
#[nativelink_test]
async fn chunked_small_blob_bytestream_no_peer_lands_on_slow_tier()
-> Result<(), Box<dyn core::error::Error>> {
    const SLOW_DELAY: Duration = Duration::from_millis(400);
    const LATENCY_FLOOR: Duration = Duration::from_millis(250);

    let slow_backing = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(Arc::new(DelayedSlowStore {
        backing: slow_backing.clone(),
        delay: SLOW_DELAY,
    }));
    // No workers registered → `mirror_and_confirm_data` returns NoPeers → the
    // chunked handler reads back the pinned copy and confirms via the slow tier.
    let (manager, _proxy) = make_manager_fast_slow(slow);
    let bs_server = make_bytestream_server(manager.as_ref())?;

    let data = Bytes::from(vec![0x6bu8; 4096]);
    let digest = DigestInfo::try_new(HASH1, data.len()).expect("valid digest");

    let start = std::time::Instant::now();
    let response = tokio::time::timeout(DEADLOCK_DETECTOR, drive_oneshot_write(bs_server, data.clone()))
        .await
        .expect("small-blob chunked write must complete on the no-peer slow path")
        .expect("join")
        .err_tip(|| "write RPC")?
        .into_inner();
    let elapsed = start.elapsed();
    assert_eq!(response.committed_size, data.len() as i64, "no-peer small-blob chunked write must ack");

    // (1) LOAD-BEARING: the ack AWAITED the slow-tier confirm before returning.
    assert!(
        elapsed >= LATENCY_FLOOR,
        "no-peer small (≤16 KiB) ByteStream Write MUST AWAIT the slow-tier write \
         before acking (slow tier ~{SLOW_DELAY:?}); observed {elapsed:?} < \
         {LATENCY_FLOOR:?} means the NoPeers arm did NOT confirm the 2nd replica \
         on the slow tier (mutation: NoPeers arm returns Ok without \
         confirm_via_slow_store)"
    );
    // (2) Durability floor: the 2nd replica physically reached the slow tier
    // (production: Redis SMALL_CAS_CACHED).
    let on_slow = slow_backing.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        on_slow.as_ref(),
        data.as_ref(),
        "no-peer small-blob 2nd replica must physically land on the slow tier"
    );
    Ok(())
}
