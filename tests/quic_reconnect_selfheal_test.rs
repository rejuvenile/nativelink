// Copyright 2025 The NativeLink Authors. All rights reserved.
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

//! Real-transport (quinn/h3) integration test for the QUIC never-poison
//! invariant (FL benchmark #6951 CAS latched-pool cascade).
//!
//! INVARIANT (verbatim): "a transient slow-store (QUIC) readiness failure must
//! not permanently latch the transport — a subsequent read must succeed — so
//! it cannot cause unnecessary duplicate action execution or truncated
//! streamed data."
//!
//! Background: each QUIC pool member is a `tower::buffer::Buffer` wrapping an
//! H3 service. `tonic_h3`'s inner `RequestSender::poll_ready` reconnects on a
//! dead connection, but returns `Err` if that reconnect's `conn.connect()`
//! fails (e.g. the server is momentarily unreachable). A plain `Buffer`
//! PERMANENTLY poisons on that first inner `poll_ready` `Err` — its worker
//! stores a `ServiceError` and exits, so every subsequent request returns
//! `"buffered service failed: …"` forever. In production ~42 genuine quinn
//! idle timeouts latched all 32 members and instant-failed 35,682 reads for
//! hours.
//!
//! The fix wraps each member's H3 service in `tower::reconnect::Reconnect`
//! INSIDE the `Buffer` (see `nativelink-util/src/tls_utils.rs`), so the
//! `Buffer` never observes an inner `poll_ready` `Err` and can never poison.
//!
//! This test drives the production `GrpcStore` QUIC client at a port with NO
//! server, so the inner `RequestSender::poll_ready`'s `conn.connect()` fails
//! and surfaces a `poll_ready` `Err` — the EXACT trigger that permanently
//! poisons a plain `Buffer`. It then stands up a REAL quinn/h3 gRPC
//! `ByteStream` server on that port and asserts a subsequent read SUCCEEDS
//! (self-heals via the `Reconnect` layer) instead of failing forever with
//! `"buffered service failed"`.
//!
//! The dead-port-first shape is deliberate: a naive server-bounce does NOT
//! discriminate the fix, because `tonic_h3`'s `RequestSender` has its own
//! re-dial in `poll_ready` and recovers on its own whenever a live server is
//! reachable at reconnect time — masking the poison. Forcing the transport
//! failure BEFORE any live connection exists makes a plain `Buffer` latch with
//! certainty (verified: a mutation dropping `Reconnect` fails this test with
//! the bespoke message), while `Reconnect` re-makes the channel until the
//! server appears.
//!
//! `#[ignore]` by default: the initial dead-port connect is bounded by quinn's
//! handshake/idle timeout, so a run takes ~15-30s of real time. The seam-level
//! guarantee is also pinned deterministically and fast by the
//! `tls_utils::reconnect_seam_tests` unit tests (Buffer-alone poisons;
//! Buffer+Reconnect recovers, with a mutation proving the discriminator). Run
//! explicitly with `--ignored`.

#![cfg(feature = "quic")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nativelink_config::cas_server::{ByteStreamConfig, CasStoreConfig, WithInstanceName};
use nativelink_config::stores::{
    EvictionPolicy, GrpcEndpoint, GrpcSpec, MemorySpec, Retry, StoreType,
};
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::cas_server::CasServer;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use sha2::{Digest, Sha256};

const INSTANCE_NAME: &str = "reconnect_test";

fn make_blob(size: usize) -> (DigestInfo, Bytes) {
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let hash = Sha256::digest(&data);
    let mut packed = [0u8; 32];
    packed.copy_from_slice(&hash);
    let digest = DigestInfo::new(packed, size as u64);
    (digest, Bytes::from(data))
}

fn make_store_manager() -> Arc<StoreManager> {
    let store_manager = Arc::new(StoreManager::new());
    let memory_store: Arc<MemoryStore> = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1_073_741_824,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    store_manager.add_store("main_cas", Store::new(memory_store));
    store_manager
}

fn make_services(store_manager: &StoreManager) -> (ByteStreamServer, CasServer) {
    let bytestream = ByteStreamServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: ByteStreamConfig {
                cas_store: "main_cas".to_string(),
                max_bytes_per_stream: 3 * 1024 * 1024,
                ..Default::default()
            },
        }],
        store_manager,
        None,
    )
    .expect("failed to create ByteStreamServer");

    let cas = CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: CasStoreConfig {
                cas_store: "main_cas".to_string(),
            },
        }],
        store_manager,
        None,
    )
    .expect("failed to create CasServer");

    (bytestream, cas)
}

struct TlsCerts {
    cert_pem: String,
    key_pem: String,
}

fn generate_tls_certs() -> TlsCerts {
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("failed to generate self-signed cert");
    TlsCerts {
        cert_pem: certified_key.cert.pem(),
        key_pem: certified_key.signing_key.serialize_pem(),
    }
}

/// Build a quinn `ServerConfig` from the shared self-signed cert.
fn quic_server_config(certs: &TlsCerts) -> quinn::ServerConfig {
    let cert_chain: Vec<CertificateDer> =
        CertificateDer::pem_reader_iter(&mut certs.cert_pem.as_bytes())
            .collect::<Result<_, _>>()
            .expect("failed to parse cert PEM");
    let key = PrivateKeyDer::from_pem_reader(&mut certs.key_pem.as_bytes())
        .expect("failed to parse key PEM");

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut tls_config = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .expect("failed to set TLS protocol versions")
    .with_no_client_auth()
    .with_single_cert(cert_chain, key)
    .expect("failed to set server cert");
    tls_config.alpn_protocols = vec![b"h3".to_vec()];
    tls_config.max_early_data_size = u32::MAX;

    let mut cfg = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
            .expect("failed to create QUIC server config"),
    ));
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(1024u32.into());
    transport.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
    cfg.transport_config(Arc::new(transport));
    cfg
}

/// A running QUIC gRPC server bound to a fixed UDP port, with a handle that
/// forcibly tears it down (closing all live client connections fast, rather
/// than waiting on idle timeout).
struct QuicServer {
    endpoint: quinn::Endpoint,
    serve_task: tokio::task::JoinHandle<()>,
}

impl QuicServer {
    /// Bind + start a server on `port`. Reusing the port across generations
    /// requires `SO_REUSEPORT` so a fresh generation can bind while the OS
    /// still holds teardown state for the old socket.
    async fn start(store_manager: &StoreManager, certs: &TlsCerts, port: u16) -> Self {
        let server_config = quic_server_config(certs);

        let sock = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .expect("server UDP socket");
        sock.set_reuse_port(true).expect("server SO_REUSEPORT");
        sock.set_nonblocking(true).expect("server nonblocking");
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        sock.bind(&addr.into()).expect("server UDP bind");
        let udp_socket = std::net::UdpSocket::from(sock);

        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            udp_socket,
            quinn::default_runtime().expect("quinn runtime"),
        )
        .expect("quinn server endpoint");

        let (bytestream, cas) = make_services(store_manager);
        let max_msg = 256 * 1024 * 1024;
        let routes = tonic::service::Routes::new(
            bytestream
                .into_service()
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg),
        )
        .add_service(
            cas.into_service()
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg),
        );

        let acceptor = tonic_h3::quinn::H3QuinnAcceptor::new(endpoint.clone());
        let h3_router = tonic_h3::server::H3Router::new(routes);
        let serve_task = tokio::spawn(async move {
            if let Err(e) = h3_router.serve(acceptor).await {
                eprintln!("QUIC gRPC server error: {e}");
            }
        });

        // Let the endpoint start accepting.
        tokio::time::sleep(Duration::from_millis(100)).await;
        Self {
            endpoint,
            serve_task,
        }
    }

    /// Forcibly kill the server: closing the endpoint sends CONNECTION_CLOSE
    /// to every live client connection, so the client's h3 driver dies fast
    /// (not idle-timeout-bound). Then wait for the socket to be released so a
    /// fresh generation can bind the same port.
    async fn kill(self) {
        self.endpoint
            .close(quinn::VarInt::from_u32(0), b"test-bounce");
        self.endpoint.wait_idle().await;
        self.serve_task.abort();
        drop(self.endpoint);
        // Give the kernel a moment to release the UDP socket.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Build the production QUIC `GrpcStore` client aimed at `127.0.0.1:port`.
async fn make_quic_client(port: u16) -> Arc<GrpcStore> {
    let spec = GrpcSpec {
        instance_name: INSTANCE_NAME.to_string(),
        endpoints: vec![GrpcEndpoint {
            address: format!("https://127.0.0.1:{port}"),
            tls_config: None,
            concurrency_limit: None,
            connect_timeout_s: 2,
            tcp_keepalive_s: 0,
            http2_keepalive_interval_s: 0,
            http2_keepalive_timeout_s: 0,
            tcp_nodelay: true,
            use_http3: true,
        }],
        store_type: StoreType::Cas,
        retry: Retry::default(),
        max_concurrent_requests: 0,
        // A SMALL pool (2 members) so a bounded number of reads is enough to
        // land the reconnect on every member within the server-down window.
        connections_per_endpoint: 2,
        rpc_timeout_s: 30,
        batch_update_threshold_bytes: 1_048_576,
        max_concurrent_batch_rpcs: 8,
        parallel_chunk_read_threshold: 8 * 1024 * 1024,
        parallel_chunk_count: 64,
        dual_transport: false,
        zstd_compression: false,
        connection_acquire_timeout_ms: None,
        chunked_writes_enabled: false,
        chunked_v2_writes_enabled: false,
    };
    GrpcStore::new(&spec)
        .await
        .expect("failed to create QUIC GrpcStore client")
}

/// Real-transport self-heal, DETERMINISTIC. The client's very first QUIC read
/// hits a port with NO server, so the inner `RequestSender::poll_ready`'s
/// `conn.connect()` fails and surfaces a `poll_ready` `Err` — the EXACT trigger
/// that permanently poisons a plain `Buffer` (FL #6951). We then start the
/// server on that port and assert a subsequent read SUCCEEDS (self-heals via
/// the `Reconnect` layer) instead of failing forever with `"buffered service
/// failed"`.
///
/// Why this discriminates the fix (unlike a server-bounce, where the inner
/// `RequestSender` re-dial can recover on its own and mask the poison): here
/// the poison is forced BEFORE any live connection exists, so a plain `Buffer`
/// latches with certainty and can never recover, while `Reconnect` re-makes the
/// channel on every request until the server appears. Verified against BOTH
/// shapes: the fixed build heals; a mutation that drops `Reconnect` fails with
/// the bespoke message below (recorded in the fix report).
///
/// `#[ignore]` by default: the initial dead-port connect is bounded by quinn's
/// handshake/idle timeout, so the run takes ~15-30s of real time. The
/// seam-level guarantee is also pinned deterministically and fast by the
/// `tls_utils::reconnect_seam_tests` unit tests. Run explicitly with
/// `cargo test --features quic --test quic_reconnect_selfheal_test -- --ignored`.
#[ignore = "real-quinn dead-port connect timing (~15-30s); seam guarantee also pinned by tls_utils::reconnect_seam_tests"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_read_self_heals_after_transient_transport_failure() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certs = generate_tls_certs();
    let store_manager = make_store_manager();

    let (digest, data) = make_blob(64 * 1024);
    {
        let store = store_manager
            .get_store("main_cas")
            .expect("main_cas not found");
        store
            .update_oneshot(digest, data.clone())
            .await
            .expect("failed to prepopulate blob");
    }

    // Reserve a fixed port by binding + dropping a UDP socket. NO server is
    // listening on it yet — the client's first connect is GUARANTEED to fail.
    let port = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve port");
        s.local_addr().unwrap().port()
    };

    let client = make_quic_client(port).await;

    // Force the transient transport failure: with no server up, the inner
    // `RequestSender::poll_ready` runs `conn.connect()` which fails, surfacing
    // a `poll_ready` `Err`. A plain `Buffer` poisons here, permanently. We
    // drive one read per pool member so every member takes the hit. Bounded so
    // the test can't hang; the read is EXPECTED to fail.
    for _ in 0..4 {
        let outcome = tokio::time::timeout(
            Duration::from_secs(30),
            client.get_part_unchunked(digest, 0, None),
        )
        .await;
        // Must NOT succeed (there is no server) — but we don't assert the
        // exact error; the point is that the transport failure occurred.
        if let Ok(Ok(_)) = outcome {
            panic!("read unexpectedly succeeded with no server listening");
        }
    }

    // Now start the server on the SAME port.
    let server = QuicServer::start(&store_manager, &certs, port).await;

    // A read after the server appears MUST succeed. With a plain `Buffer` the
    // members are permanently poisoned by the earlier `poll_ready` `Err` and
    // this fails forever with "buffered service failed". With the `Reconnect`
    // layer each member transparently re-makes its channel and this succeeds.
    // Retry a bounded number of times to absorb reconnect + round-robin
    // latency — but the outcome must be a SUCCESS, and a "buffered service
    // failed" latch is surfaced immediately as the invariant violation.
    let mut last_err: Option<String> = None;
    let mut healed = false;
    for _ in 0..30 {
        match tokio::time::timeout(
            Duration::from_secs(10),
            client.get_part_unchunked(digest, 0, None),
        )
        .await
        {
            Ok(Ok(got)) => {
                assert_eq!(
                    got.len(),
                    data.len(),
                    "post-recovery read returned wrong length (truncated stream)",
                );
                healed = true;
                break;
            }
            Ok(Err(e)) => {
                let msg = e.to_string();
                assert!(
                    !msg.contains("buffered service failed"),
                    "never-poison invariant violated: a transient QUIC transport \
                     readiness failure permanently latched the buffered pool member \
                     — the post-recovery read returned the tower ServiceError instead \
                     of self-healing: {msg}",
                );
                last_err = Some(msg);
            }
            Err(_elapsed) => {
                last_err = Some("read timed out".to_string());
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        healed,
        "never-poison invariant violated: QUIC read did not self-heal after a \
         transient transport failure within the retry budget (last error: {last_err:?})",
    );

    server.kill().await;
}
