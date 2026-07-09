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

use core::time::Duration;

use nativelink_config::stores::{ClientTlsConfig, GrpcEndpoint};
use nativelink_error::{Code, Error, make_err, make_input_err};
use tonic::transport::Uri;
use tracing::{info, warn};

pub fn load_client_config(
    config: &Option<ClientTlsConfig>,
) -> Result<Option<tonic::transport::ClientTlsConfig>, Error> {
    let Some(config) = config else {
        return Ok(None);
    };

    if config.use_native_roots == Some(true) {
        if config.ca_file.is_some() {
            warn!("native root certificates are being used, ca_file will be ignored");
        }
        let tls = tonic::transport::ClientTlsConfig::new().with_native_roots();
        // Apply client identity for mTLS even when using native roots
        let tls = if let Some(client_certificate) = &config.cert_file {
            let Some(client_key) = &config.key_file else {
                return Err(make_err!(
                    Code::Internal,
                    "Client certificate specified, but no key"
                ));
            };
            info!("loading client certificate for mTLS with native roots");
            tls.identity(tonic::transport::Identity::from_pem(
                std::fs::read_to_string(client_certificate)?,
                std::fs::read_to_string(client_key)?,
            ))
        } else {
            if config.key_file.is_some() {
                return Err(make_err!(
                    Code::Internal,
                    "Client key specified, but no certificate"
                ));
            }
            tls
        };
        return Ok(Some(tls));
    }

    let Some(ca_file) = &config.ca_file else {
        return Err(make_err!(
            Code::Internal,
            "CA certificate must be provided if not using native root certificates"
        ));
    };

    let read_config = tonic::transport::ClientTlsConfig::new().ca_certificate(
        tonic::transport::Certificate::from_pem(std::fs::read_to_string(ca_file)?),
    );
    let config = if let Some(client_certificate) = &config.cert_file {
        let Some(client_key) = &config.key_file else {
            return Err(make_err!(
                Code::Internal,
                "Client certificate specified, but no key"
            ));
        };
        read_config.identity(tonic::transport::Identity::from_pem(
            std::fs::read_to_string(client_certificate)?,
            std::fs::read_to_string(client_key)?,
        ))
    } else {
        if config.key_file.is_some() {
            return Err(make_err!(
                Code::Internal,
                "Client key specified, but no certificate"
            ));
        }
        read_config
    };

    Ok(Some(config))
}

pub fn endpoint_from(
    endpoint: &str,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
) -> Result<tonic::transport::Endpoint, Error> {
    let endpoint = Uri::try_from(endpoint)
        .map_err(|e| make_err!(Code::Internal, "Unable to parse endpoint {endpoint}: {e:?}"))?;

    // Tonic uses the TLS configuration if the scheme is "https", so replace
    // grpcs with https.
    let endpoint = if endpoint.scheme_str() == Some("grpcs") {
        let mut parts = endpoint.into_parts();
        parts.scheme = Some("https".parse().map_err(|e| {
            make_err!(
                Code::Internal,
                "https is an invalid scheme apparently? {e:?}"
            )
        })?);
        parts.try_into().map_err(|e| {
            make_err!(
                Code::Internal,
                "Error changing Uri from grpcs to https: {e:?}"
            )
        })?
    } else {
        endpoint
    };

    let endpoint_transport = if let Some(tls_config) = tls_config {
        let Some(authority) = endpoint.authority() else {
            return Err(make_input_err!(
                "Unable to determine authority of endpoint: {endpoint}"
            ));
        };
        if endpoint.scheme_str() != Some("https") {
            return Err(make_input_err!(
                "You have set TLS configuration on {endpoint}, but the scheme is not https or grpcs"
            ));
        }
        let tls_config = tls_config.domain_name(authority.host());
        tonic::transport::Endpoint::from(endpoint)
            .tls_config(tls_config)
            .map_err(|e| make_input_err!("Setting mTLS configuration: {e:?}"))?
    } else {
        if endpoint.scheme_str() == Some("https") {
            return Err(make_input_err!(
                "The scheme of {endpoint} is https or grpcs, but no TLS configuration was provided"
            ));
        }
        tonic::transport::Endpoint::from(endpoint)
    };

    // Always enable TCP_NODELAY to reduce latency on gRPC connections.
    // Nagle's algorithm delays small writes (up to 40ms), which is
    // harmful for gRPC's many small HTTP/2 frames.
    let endpoint_transport = endpoint_transport.tcp_nodelay(true);

    // Set HTTP/2 flow-control windows to match the server defaults (16 MiB
    // stream, 128 MiB connection).  Tonic/h2 defaults to 64 KiB for both,
    // which caps aggregate throughput per connection to ~128 MB/s at 0.5 ms
    // RTT — far below 10 GbE capacity when many streams share a connection.
    let endpoint_transport = endpoint_transport
        .initial_stream_window_size(16 * 1024 * 1024)
        .initial_connection_window_size(128 * 1024 * 1024);

    Ok(endpoint_transport)
}

pub fn endpoint(endpoint_config: &GrpcEndpoint) -> Result<tonic::transport::Endpoint, Error> {
    let endpoint = endpoint_from(
        &endpoint_config.address,
        load_client_config(&endpoint_config.tls_config)?,
    )?;

    let connect_timeout = if endpoint_config.connect_timeout_s > 0 {
        Duration::from_secs(endpoint_config.connect_timeout_s)
    } else {
        Duration::from_secs(30)
    };
    let tcp_keepalive = if endpoint_config.tcp_keepalive_s > 0 {
        Duration::from_secs(endpoint_config.tcp_keepalive_s)
    } else {
        Duration::from_secs(30)
    };
    let http2_keepalive_interval = if endpoint_config.http2_keepalive_interval_s > 0 {
        Duration::from_secs(endpoint_config.http2_keepalive_interval_s)
    } else {
        Duration::from_secs(30)
    };
    let http2_keepalive_timeout = if endpoint_config.http2_keepalive_timeout_s > 0 {
        Duration::from_secs(endpoint_config.http2_keepalive_timeout_s)
    } else {
        Duration::from_secs(20)
    };

    info!(
        address = %endpoint_config.address,
        concurrency_limit = ?endpoint_config.concurrency_limit,
        connect_timeout_s = connect_timeout.as_secs(),
        tcp_keepalive_s = tcp_keepalive.as_secs(),
        http2_keepalive_interval_s = http2_keepalive_interval.as_secs(),
        http2_keepalive_timeout_s = http2_keepalive_timeout.as_secs(),
        "tls_utils::endpoint: creating gRPC endpoint with keepalive",
    );

    let mut endpoint = endpoint
        .connect_timeout(connect_timeout)
        .tcp_nodelay(endpoint_config.tcp_nodelay)
        .tcp_keepalive(Some(tcp_keepalive))
        .http2_keep_alive_interval(http2_keepalive_interval)
        .keep_alive_timeout(http2_keepalive_timeout)
        .keep_alive_while_idle(true)
        // Default to 16 MiB stream window and 128 MiB connection window
        // to avoid capping per-stream throughput at ~64 MB/s with 1ms RTT
        // (hyper's default of 64 KiB is too small for high-bandwidth links).
        .initial_stream_window_size(16 * 1024 * 1024)
        .initial_connection_window_size(128 * 1024 * 1024);

    if let Some(concurrency_limit) = endpoint_config.concurrency_limit {
        endpoint = endpoint.concurrency_limit(concurrency_limit);
    }

    Ok(endpoint)
}

/// Target QUIC UDP socket buffer size: 32 MiB.
///
/// Quinn does NOT raise the kernel default UDP buffer (typically 208 KiB on
/// stock Linux) on its own — the underlying tokio/quinn `UdpSocket` inherits
/// `net.core.{rmem,wmem}_default`. Without tuning, sustained QUIC ingress
/// drops packets and tail latency spikes under load.
///
/// 32 MiB covers 10 GbE BDP bursts with substantial headroom and eliminates
/// `UdpRcvbufErrors` observed in production at exactly the 8 MiB previous
/// limit (104,379 events). Raised from 8 MiB because host rmem_max/wmem_max
/// are being raised to 64 MiB via sysctl (`net.core.rmem_max=67108864`,
/// `net.core.wmem_max=67108864`) — the `setsockopt` will succeed only after
/// those sysctl values are applied. The kernel may silently cap requests above
/// `net.core.{rmem,wmem}_max` — see `tune_quic_udp_buffers` for read-back
/// logging that surfaces the cap in production logs.
#[cfg(feature = "quic")]
pub const QUIC_UDP_BUF_BYTES: usize = 32 * 1024 * 1024;

/// Minimum acceptable post-set UDP buffer size before we warn the operator.
///
/// Below 2 MiB, QUIC tail latency degrades sharply under burst load and
/// `net.core.rmem_max` almost certainly needs raising. Stock Linux ships
/// `rmem_max = 212992` (~208 KiB), which clips every `set_recv_buffer_size`
/// request silently — the syscall returns `Ok(())` regardless.
#[cfg(feature = "quic")]
pub const QUIC_UDP_BUF_WARN_THRESHOLD: usize = 2 * 1024 * 1024;

/// Effective UDP socket buffer sizes after kernel accounting.
///
/// "Effective" here means the value usable by the application:
/// - On Linux, `getsockopt(SO_{RCV,SND}BUF)` returns 2× the value passed
///   to `setsockopt` (kernel doubles internally for bookkeeping), so
///   we halve before exposing.
/// - On macOS / BSD, no doubling — the raw value IS the effective value.
#[cfg(feature = "quic")]
#[derive(Debug, Clone, Copy)]
pub struct QuicUdpBuffers {
    pub effective_sndbuf: usize,
    pub effective_rcvbuf: usize,
}

#[cfg(feature = "quic")]
impl QuicUdpBuffers {
    pub fn below_threshold(self) -> bool {
        self.effective_rcvbuf < QUIC_UDP_BUF_WARN_THRESHOLD
            || self.effective_sndbuf < QUIC_UDP_BUF_WARN_THRESHOLD
    }
}

/// Set SO_SNDBUF and SO_RCVBUF on a QUIC UDP socket, log the actual
/// values the kernel applied, and return the effective buffer sizes.
///
/// Linux silently caps `setsockopt(SO_{RCV,SND}BUF, n)` at
/// `net.core.{rmem,wmem}_max` without returning an error. This helper
/// reads the values back via `getsockopt` and `info!`-logs them so
/// production deployments can verify their kernel is configured to
/// honor the request.
///
/// Callers receive the effective sizes (doubling-corrected on Linux,
/// raw on macOS / BSD) and should use [`warn_if_quic_udp_buffer_capped`]
/// to emit a sysctl-recommendation warn when below threshold. Loop
/// callers (e.g. connection pools) typically warn ONCE after the loop
/// — all sockets in the same process see the same `rmem_max`, so per-
/// socket warns are pure log spam (and at high pool counts can drive
/// memory pressure via mimalloc retention; see #255 / #197).
///
/// `label` distinguishes call sites in logs (e.g. `"server"`,
/// `"client"`, `"worker_peer"`).
#[cfg(feature = "quic")]
pub fn tune_quic_udp_buffers(sock: socket2::SockRef<'_>, label: &str) -> QuicUdpBuffers {
    if let Err(err) = sock.set_send_buffer_size(QUIC_UDP_BUF_BYTES) {
        warn!(?err, label, "failed to set QUIC SO_SNDBUF");
    }
    if let Err(err) = sock.set_recv_buffer_size(QUIC_UDP_BUF_BYTES) {
        warn!(?err, label, "failed to set QUIC SO_RCVBUF");
    }

    let actual_sndbuf = sock.send_buffer_size().unwrap_or(0);
    let actual_rcvbuf = sock.recv_buffer_size().unwrap_or(0);

    info!(
        label,
        requested = QUIC_UDP_BUF_BYTES,
        actual_sndbuf,
        actual_rcvbuf,
        "QUIC UDP socket buffers configured",
    );

    // Linux: `getsockopt(SO_{RCV,SND}BUF)` returns 2× what `setsockopt`
    // accepted (the kernel doubles internally for bookkeeping overhead).
    // macOS / BSD: no doubling.
    #[cfg(target_os = "linux")]
    let (effective_sndbuf, effective_rcvbuf) = (actual_sndbuf / 2, actual_rcvbuf / 2);
    #[cfg(not(target_os = "linux"))]
    let (effective_sndbuf, effective_rcvbuf) = (actual_sndbuf, actual_rcvbuf);

    QuicUdpBuffers {
        effective_sndbuf,
        effective_rcvbuf,
    }
}

/// Emit a `warn!` with the operator-actionable sysctl recommendation if
/// the effective buffer sizes fall below [`QUIC_UDP_BUF_WARN_THRESHOLD`].
/// No-op otherwise.
#[cfg(feature = "quic")]
pub fn warn_if_quic_udp_buffer_capped(buffers: QuicUdpBuffers, label: &str) {
    if !buffers.below_threshold() {
        return;
    }
    warn!(
        label,
        effective_rcvbuf = buffers.effective_rcvbuf,
        effective_sndbuf = buffers.effective_sndbuf,
        requested = QUIC_UDP_BUF_BYTES,
        warn_threshold = QUIC_UDP_BUF_WARN_THRESHOLD,
        "QUIC UDP buffer below {} MiB after setsockopt — kernel is capping; \
         raise net.core.rmem_max and net.core.wmem_max (e.g. \
         `sudo sysctl -w net.core.rmem_max={} net.core.wmem_max={}`) \
         to avoid UDP packet drops and QUIC tail-latency spikes under load",
        QUIC_UDP_BUF_WARN_THRESHOLD / (1024 * 1024),
        QUIC_UDP_BUF_BYTES,
        QUIC_UDP_BUF_BYTES,
    );
}

/// The concrete quinn-backed H3 channel a single pool member drives.
#[cfg(feature = "quic")]
type H3ChannelConcrete = tonic_h3::H3Channel<tonic_h3::quinn::H3QuinnConnector>;

/// The response type every H3 pool member yields (`H3IncomingClient` body).
#[cfg(feature = "quic")]
type H3ChannelResponse =
    hyper::Response<h3_util::client_body::H3IncomingClient<h3_quinn::RecvStream, bytes::Bytes>>;

/// Clone-able QUIC/HTTP3 channel for gRPC clients.
///
/// `tonic_h3::H3Channel` wraps a `BoxService` internally and doesn't
/// implement `Clone`, but tonic generated clients require `T: Clone`.
/// We use `tower::buffer::Buffer` which correctly serializes
/// `poll_ready`/`call` pairs through a background worker task,
/// properly routing wakers so concurrent callers don't deadlock.
///
/// The buffered service wraps a `tower::reconnect::Reconnect<H3ChannelMaker>`
/// (not a bare `H3Channel`) so a transient quinn timeout self-heals instead
/// of permanently poisoning the `Buffer`. `Reconnect::poll_ready` never
/// returns `Err` — a Connected inner `poll_ready` error transparently re-makes
/// the channel, and a make error is stashed as a per-request error, not a
/// permanent latch — so the `Buffer` worker only ever sees `Ready(Ok)` from
/// `poll_ready` and can never poison (FL #6951).
///
/// Type alias for the inner buffered H3 service. Its second parameter is the
/// wrapped service's `Future` — here `Reconnect`'s `ResponseFuture`, which
/// erases the maker-error side into the request path.
#[cfg(feature = "quic")]
type H3BufferedService = tower::buffer::Buffer<
    hyper::Request<tonic::body::Body>,
    tower::reconnect::ResponseFuture<
        futures::future::BoxFuture<'static, Result<H3ChannelResponse, tonic_h3::Error>>,
        tower::BoxError,
    >,
>;

/// A pool of QUIC/HTTP3 connections that distributes RPCs across
/// multiple independent quinn connections via round-robin. Each
/// connection has its own UDP socket, quinn Endpoint, and Connection
/// mutex, eliminating the single-mutex bottleneck that serializes
/// all streams on one connection.
///
/// `Buffer` is Clone (Arc-backed), so cloning QuicChannel is cheap.
/// Each clone gets its own `selected` index so concurrent clones
/// don't interfere with each other's poll_ready/call pairing.
#[cfg(feature = "quic")]
#[derive(Clone)]
pub struct QuicChannel {
    channels: Vec<H3BufferedService>,
    /// Global round-robin counter shared across all clones.
    counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Index selected by the most recent poll_ready on THIS clone.
    /// Per-clone (not shared) to avoid race between concurrent clones.
    selected: usize,
}

#[cfg(feature = "quic")]
impl std::fmt::Debug for QuicChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicChannel")
            .field("connections", &self.channels.len())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "quic")]
impl tower::Service<hyper::Request<tonic::body::Body>> for QuicChannel {
    type Response = hyper::Response<
        h3_util::client_body::H3IncomingClient<h3_quinn::RecvStream, bytes::Bytes>,
    >;
    type Error = tower::BoxError;
    type Future = <H3BufferedService as tower::Service<hyper::Request<tonic::body::Body>>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // Only select a new channel when we haven't committed to one yet.
        // On Pending retries, keep polling the same channel to avoid
        // waker misrouting and counter skew.
        if self.selected >= self.channels.len() {
            self.selected = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % self.channels.len();
        }
        tower::Service::poll_ready(&mut self.channels[self.selected], cx)
    }

    fn call(&mut self, req: hyper::Request<tonic::body::Body>) -> Self::Future {
        let idx = self.selected;
        // Reset so next poll_ready picks a new channel.
        self.selected = usize::MAX;
        tower::Service::call(&mut self.channels[idx], req)
    }
}

/// Build ONE fresh quinn-backed H3 channel: bind a new UDP socket, tune its
/// send/recv buffers, create the quinn `Endpoint`, and wrap it in an
/// `H3Channel`. Returns the channel plus the effective UDP buffer sizes so a
/// pool builder can emit a single cap warning.
///
/// This is the exact per-member construction the pool loop used inline; it is
/// factored out so [`H3ChannelMaker`] (the `tower::reconnect::Reconnect`
/// factory) can rebuild an identical member after a transient QUIC failure.
/// Every input is owned/cloned, so the returned channel shares nothing mutable
/// with its siblings or with prior generations of the same member.
#[cfg(feature = "quic")]
fn build_h3_channel_member(
    client_config: &quinn::ClientConfig,
    uri: &Uri,
    server_name: &str,
    label: &str,
) -> Result<(H3ChannelConcrete, QuicUdpBuffers), Error> {
    let udp_socket = std::net::UdpSocket::bind("[::]:0")
        .map_err(|e| make_err!(Code::Internal, "QUIC client UDP bind ({label}): {e:?}"))?;
    let bufs = tune_quic_udp_buffers(socket2::SockRef::from(&udp_socket), label);

    let mut client_endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        None,
        udp_socket,
        quinn::default_runtime()
            .ok_or_else(|| make_err!(Code::Internal, "No async runtime for QUIC client"))?,
    )
    .map_err(|e| make_err!(Code::Internal, "Failed to create QUIC client endpoint ({label}): {e:?}"))?;
    client_endpoint.set_default_client_config(client_config.clone());

    let connector =
        tonic_h3::quinn::H3QuinnConnector::new(uri.clone(), server_name.to_string(), client_endpoint);

    let h3_channel = tonic_h3::H3Channel::new(connector, uri.clone());
    Ok((h3_channel, bufs))
}

/// `tower::Service<()>` factory that builds a fresh [`H3ChannelConcrete`] on
/// each `call()`, used as the `MakeService` for `tower::reconnect::Reconnect`.
///
/// `Reconnect` invokes this maker to (re)establish a pool member's QUIC
/// connection: on the initial connect it is bypassed (we seed the member via
/// `Reconnect::with_connection`), and on a transient inner `poll_ready`
/// failure `Reconnect` drives it to build a NEW connection transparently.
/// Construction is fully synchronous, so `call()` returns a ready future.
///
/// A build failure (e.g. UDP bind failure under fd exhaustion) is surfaced as
/// the maker's `Error` and becomes a PER-REQUEST error on the next `call()` of
/// the buffered member — never a permanent poison. The next request re-drives
/// the maker and retries a fresh build.
#[cfg(feature = "quic")]
struct H3ChannelMaker {
    client_config: quinn::ClientConfig,
    uri: Uri,
    server_name: String,
    /// Distinguishes reconnect generations of ONE member in logs.
    label: String,
    /// Monotonic reconnect generation for this member (0 = initial seed is
    /// external; the first make here is generation 1).
    generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(feature = "quic")]
impl tower::Service<()> for H3ChannelMaker {
    type Response = H3ChannelConcrete;
    type Error = tower::BoxError;
    type Future = core::future::Ready<Result<H3ChannelConcrete, tower::BoxError>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // The factory itself is always ready — building is synchronous.
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _target: ()) -> Self::Future {
        let generation = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let gen_label = format!("{}#reconnect{generation}", self.label);
        info!(
            member = %self.label,
            generation,
            "tls_utils: rebuilding QUIC pool member after transport failure",
        );
        match build_h3_channel_member(&self.client_config, &self.uri, &self.server_name, &gen_label)
        {
            Ok((channel, _bufs)) => core::future::ready(Ok(channel)),
            Err(e) => {
                warn!(
                    member = %self.label,
                    generation,
                    ?e,
                    "tls_utils: QUIC pool member rebuild failed; will retry on next request",
                );
                // Convert the internal error into a BoxError for the
                // Reconnect per-request error path (never a poison).
                core::future::ready(Err(Box::<dyn std::error::Error + Send + Sync>::from(
                    e.to_string(),
                )))
            }
        }
    }
}

/// Create a pool of QUIC/HTTP3 channels for a gRPC endpoint.
///
/// Creates `connections` independent QUIC connections, each with its own
/// UDP socket, quinn Endpoint, and Connection mutex. RPCs are distributed
/// across connections via round-robin, eliminating the single-mutex
/// bottleneck in quinn's Connection state.
#[cfg(feature = "quic")]
pub fn h3_channel(endpoint_config: &GrpcEndpoint, connections: usize) -> Result<QuicChannel, Error> {
    use std::sync::Arc;
    use h3_quinn as _;

    let uri: Uri = endpoint_config
        .address
        .parse()
        .map_err(|e| make_input_err!("Invalid URI for QUIC endpoint: {e:?}"))?;

    let server_name = uri
        .host()
        .ok_or_else(|| make_input_err!("QUIC endpoint URI has no host: {}", uri))?
        .to_string();

    // Resolve hostname to an IPv4 address to avoid IPv6 link-local addresses
    // (fe80::) which require a zone ID and cause QUIC timeouts on Linux when
    // connecting to macOS .local hosts (mDNS returns IPv6 link-local first).
    let uri: Uri = {
        let port = uri.port_u16().unwrap_or(443);
        let resolved_host = std::net::ToSocketAddrs::to_socket_addrs(
            &(server_name.as_str(), port),
        )
        .map_err(|e| make_input_err!("Failed to resolve QUIC host {server_name}: {e:?}"))?
        .find(|addr| addr.is_ipv4())
        .ok_or_else(|| make_input_err!("No IPv4 address found for QUIC host {server_name}"))?;
        let new_uri = format!(
            "{}://{}:{}{}",
            uri.scheme_str().unwrap_or("https"),
            resolved_host.ip(),
            resolved_host.port(),
            uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/"),
        );
        info!(
            %server_name,
            resolved = %resolved_host.ip(),
            "QUIC: resolved hostname to IPv4",
        );
        new_uri
            .parse()
            .map_err(|e| make_input_err!("Failed to parse resolved QUIC URI: {e:?}"))?
    };

    // Build rustls ClientConfig with no server cert verification (internal network,
    // self-signed certs). If the endpoint has a client cert+key in tls_config,
    // present them for mTLS authentication.
    let tls_builder = rustls::ClientConfig::builder_with_provider(
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .map_err(|e| make_err!(Code::Internal, "QUIC TLS version error: {e:?}"))?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(NoCertVerification(
        rustls::crypto::aws_lc_rs::default_provider(),
    )));

    let mut tls_config = if let Some(tls_cfg) = &endpoint_config.tls_config {
        if let Some(cert_file) = &tls_cfg.cert_file {
            let key_file = tls_cfg.key_file.as_ref().ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "QUIC client certificate specified but no key file"
                )
            })?;
            use rustls::pki_types::pem::PemObject;
            let cert_pem = std::fs::read(cert_file)
                .map_err(|e| make_err!(Code::Internal, "Could not read QUIC client cert {cert_file}: {e:?}"))?;
            let key_pem = std::fs::read(key_file)
                .map_err(|e| make_err!(Code::Internal, "Could not read QUIC client key {key_file}: {e:?}"))?;
            let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
                rustls::pki_types::CertificateDer::pem_reader_iter(&mut &cert_pem[..])
                    .collect::<Result<_, _>>()
                    .map_err(|e| make_err!(Code::Internal, "Could not parse QUIC client certs: {e:?}"))?;
            let key = rustls::pki_types::PrivateKeyDer::from_pem_reader(&mut &key_pem[..])
                .map_err(|e| make_err!(Code::Internal, "Could not parse QUIC client key: {e:?}"))?;
            info!(
                %cert_file,
                %key_file,
                "QUIC: loading client certificate for mTLS",
            );
            tls_builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| make_err!(Code::Internal, "QUIC client auth cert error: {e:?}"))?
        } else {
            if tls_cfg.key_file.is_some() {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "QUIC client key_file specified without cert_file"
                ));
            }
            tls_builder.with_no_client_auth()
        }
    } else {
        tls_builder.with_no_client_auth()
    };

    tls_config.enable_early_data = true;
    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .map_err(|e| make_err!(Code::Internal, "Quinn client config error: {e:?}"))?,
    ));

    // Tune QUIC transport for 10 GbE LAN (~0.5ms RTT).
    // BDP = 1.25 GB/s × 0.5ms ≈ 625 KB. Use generous windows to
    // handle bursts and concurrent streams without flow-control stalls.
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window((16 * 1024 * 1024u32).into()); // 16 MiB per stream
    transport.receive_window((256 * 1024 * 1024u32).into()); // 256 MiB connection
    transport.send_window(256 * 1024 * 1024); // 256 MiB
    transport.max_concurrent_bidi_streams(8192u32.into()); // 8K streams per connection
    transport.max_concurrent_uni_streams(1024u32.into());
    transport.initial_rtt(Duration::from_micros(500)); // 0.5ms LAN RTT
    // Reduce ACK delay from default 25ms to 5ms for LAN.
    let mut ack_freq = quinn::AckFrequencyConfig::default();
    ack_freq.max_ack_delay(Some(Duration::from_millis(5)));
    transport.ack_frequency_config(Some(ack_freq));
    // Idle timeout: 15s. Short enough that dead connections (from server
    // restart) are detected within ~2 keepalive cycles (5s each) plus
    // this timeout, rather than blocking RPCs for the full RPC timeout.
    transport.max_idle_timeout(Some(Duration::from_secs(15).try_into().unwrap()));
    // BBR handles bursty workloads better than Cubic on high-BDP LAN.
    transport.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
    // Send QUIC keepalives every 2s to detect dead connections quickly
    // after server restart. Combined with 15s idle timeout, a dead
    // connection surfaces a readiness error within ~4-6s. `H3Channel`
    // itself has NO reconnection — the `tower::reconnect::Reconnect` layer
    // wrapped around each pool member (see `h3_channel` / `H3ChannelMaker`)
    // is what transparently rebuilds the connection on that error, well
    // before the RPC timeout (120s) expires, and prevents the surrounding
    // `Buffer` from permanently poisoning (FL #6951).
    transport.keep_alive_interval(Some(Duration::from_secs(2)));
    // Enable QUIC MTU discovery for jumbo frames. Probe up to 8952
    // bytes (9000 jumbo MTU minus 40 IPv6 + 8 UDP headers). Reduces
    // packet rate by ~6x vs default 1452.
    transport.initial_mtu(1200);
    let mut mtu_config = quinn::MtuDiscoveryConfig::default();
    mtu_config.upper_bound(8952);
    transport.mtu_discovery_config(Some(mtu_config));
    client_config.transport_config(Arc::new(transport));

    let connections = connections.max(1);
    let mut channels = Vec::with_capacity(connections);
    // Warn once per pool if the kernel is capping UDP buffers — every
    // socket sees the same rmem_max, so per-socket warns are pure spam.
    let mut pool_buffers: Option<QuicUdpBuffers> = None;

    for i in 0..connections {
        let label = format!("client[{i}]");
        // Build the INITIAL member eagerly (identical to the prior inline
        // construction) so the eager UDP-buffer tuning + one-warn-per-pool
        // diagnostic below is preserved. Only RECONNECTS are lazy.
        let (initial_channel, bufs) =
            build_h3_channel_member(&client_config, &uri, &server_name, &label)?;
        if pool_buffers.is_none() {
            pool_buffers = Some(bufs);
        }

        // Factory that rebuilds THIS member's connection on transient QUIC
        // failure. Shares the (cheap-to-clone) config/uri/server_name; each
        // make binds a fresh UDP socket + quinn Endpoint.
        let maker = H3ChannelMaker {
            client_config: client_config.clone(),
            uri: uri.clone(),
            server_name: server_name.clone(),
            label,
            generation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        // `Reconnect::with_connection` seeds the member with the eagerly-built
        // channel and never returns `Err` from `poll_ready`, so the `Buffer`
        // wrapping it can NEVER poison on a transient inner readiness error
        // (FL #6951). A Connected inner error re-makes transparently; a make
        // error becomes a per-request error, not a latch.
        let reconnect = tower::reconnect::Reconnect::with_connection(initial_channel, maker, ());
        // 1024 slots per connection. With N connections, total capacity
        // is N×1024 (e.g., 32×1024 = 32768), sufficient for burst peaks
        // while providing backpressure under transport degradation.
        let buffered = tower::buffer::Buffer::new(reconnect, 1024);
        channels.push(buffered);
    }

    if let Some(bufs) = pool_buffers {
        warn_if_quic_udp_buffer_capped(bufs, "client_pool");
    }

    info!(
        address = %endpoint_config.address,
        connections,
        "tls_utils::h3_channel: created QUIC/HTTP3 connection pool",
    );

    Ok(QuicChannel {
        channels,
        counter: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        selected: usize::MAX, // sentinel: no channel selected yet
    })
}

/// Certificate verifier that accepts any server certificate.
/// Used for internal networks with self-signed certs.
#[cfg(feature = "quic")]
#[derive(Debug)]
struct NoCertVerification(rustls::crypto::CryptoProvider);

#[cfg(feature = "quic")]
impl rustls::client::danger::ServerCertVerifier for NoCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Layering-seam tests for the never-poison invariant on the QUIC pool.
///
/// INVARIANT (the property under test, verbatim): "a transient slow-store
/// (QUIC) readiness failure must not permanently latch the transport — a
/// subsequent read must succeed — so it cannot cause unnecessary duplicate
/// action execution or truncated streamed data."
///
/// Background (FL benchmark #6951 CAS latched-pool cascade): each QUIC pool
/// member is a `tower::buffer::Buffer` wrapping an inner H3 service. A
/// `Buffer` PERMANENTLY poisons on the FIRST inner `poll_ready` error —
/// its worker task stores a `ServiceError` and exits, so every subsequent
/// `poll_ready`/`call` on any clone returns `"buffered service failed: …"`
/// forever. The inner error is a quinn idle/connection timeout; ~42 genuine
/// timeouts latched all 32 members and instant-failed 35,682 reads for hours.
///
/// The fix wraps each member's inner service in `tower::reconnect::Reconnect`
/// INSIDE the `Buffer`. `Reconnect::poll_ready` never returns `Err`: a
/// Connected inner `poll_ready` error transitions to `Idle` and transparently
/// re-makes the service, so the `Buffer` never observes an inner `poll_ready`
/// `Err` and can never poison.
///
/// These tests exercise the SEAM (Buffer-alone vs Buffer+Reconnect) with fake
/// tower services so they are deterministic and require no real transport —
/// the real-transport self-heal is covered by the `nativelink-store`
/// integration test.
#[cfg(all(test, feature = "quic"))]
mod reconnect_seam_tests {
    use core::future::{Ready, ready};
    use core::pin::Pin;
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use core::task::{Context, Poll};
    use std::sync::Arc;

    use tower::{BoxError, Service};

    /// Fake inner service that fails its FIRST `poll_ready` (across all
    /// instances that share `already_poisoned`), then is healthy forever.
    ///
    /// This models exactly one transient connection death: the first
    /// connection's first readiness check times out; any connection made
    /// afterwards is healthy from its first poll. A shared flag (rather than
    /// a per-instance one) is deliberate — if EVERY freshly-made instance
    /// errored on its first poll, `Reconnect` would spin its internal make
    /// loop forever, which is not the production scenario (a reconnect gets a
    /// fresh, healthy connection).
    struct PoisonOnce {
        already_poisoned: Arc<AtomicBool>,
    }

    impl Service<()> for PoisonOnce {
        type Response = &'static str;
        type Error = BoxError;
        type Future = Ready<Result<&'static str, BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            // `swap(true)` returns the PREVIOUS value: the very first caller
            // sees `false` → errors once; everyone after sees `true` → Ok.
            if !self.already_poisoned.swap(true, Ordering::SeqCst) {
                return Poll::Ready(Err(Box::<dyn std::error::Error + Send + Sync>::from(
                    "simulated transient quinn idle timeout",
                )));
            }
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: ()) -> Self::Future {
            ready(Ok("served"))
        }
    }

    /// Fake `MakeService` (`Service<()>`) producing a fresh `PoisonOnce` per
    /// make, all sharing the same one-shot `already_poisoned` flag, and
    /// counting makes so the test can assert a reconnect actually happened.
    struct FakeMaker {
        already_poisoned: Arc<AtomicBool>,
        makes: Arc<AtomicUsize>,
    }

    impl Service<()> for FakeMaker {
        type Response = PoisonOnce;
        type Error = BoxError;
        type Future = Ready<Result<PoisonOnce, BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            // The maker itself is always ready — mirrors `H3ChannelMaker`,
            // whose construction is synchronous and infallible-to-poll.
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _target: ()) -> Self::Future {
            self.makes.fetch_add(1, Ordering::SeqCst);
            ready(Ok(PoisonOnce {
                already_poisoned: Arc::clone(&self.already_poisoned),
            }))
        }
    }

    /// Drive a `tower::Service<()>` to readiness and issue one call, with a
    /// deadline so an infinite internal make-loop cannot hang the test.
    async fn ready_and_call<S>(svc: &mut S) -> Result<&'static str, BoxError>
    where
        S: Service<(), Response = &'static str, Error = BoxError>,
    {
        core::future::poll_fn(|cx| svc.poll_ready(cx)).await?;
        svc.call(()).await
    }

    /// (a) Documents the BUG: a plain `Buffer` permanently latches after one
    /// transient inner `poll_ready` error — every subsequent call fails.
    #[tokio::test(flavor = "current_thread")]
    async fn plain_buffer_permanently_latches_on_transient_error() {
        let mut buffered = tower::buffer::Buffer::new(
            PoisonOnce {
                already_poisoned: Arc::new(AtomicBool::new(false)),
            },
            4,
        );

        // First readiness check hits the transient error and poisons the
        // Buffer worker. The exact error surfaced here is timing-dependent
        // (the ServiceError may surface on this call or the next), so we do
        // not assert on this one — only on the PERMANENCE below.
        let _first = tokio::time::timeout(core::time::Duration::from_secs(5), ready_and_call(&mut buffered))
            .await
            .expect("plain-buffer first call must not hang");

        // The latch is permanent: a subsequent call must fail with the tower
        // ServiceError. This is the bug the fix removes.
        let second = tokio::time::timeout(core::time::Duration::from_secs(5), ready_and_call(&mut buffered))
            .await
            .expect("plain-buffer second call must not hang");
        let err = second.expect_err(
            "plain Buffer must stay latched after a transient inner poll_ready error \
             (documents the FL #6951 bug — remove this and the reproduce baseline is gone)",
        );
        assert!(
            err.to_string().contains("buffered service failed"),
            "expected tower ServiceError 'buffered service failed', got: {err}",
        );
    }

    /// (b) Proves the FIX: `Buffer::new(Reconnect::new(maker, target), cap)`
    /// self-heals after the same transient error — a subsequent call SUCCEEDS.
    ///
    /// MUTATION: replace the `Reconnect::new(...)` inner with a bare
    /// `PoisonOnce` (drop `Reconnect`) and this test MUST fail with the
    /// bespoke `.expect` message below.
    #[tokio::test(flavor = "current_thread")]
    async fn buffer_with_reconnect_self_heals_after_transient_error() {
        let already_poisoned = Arc::new(AtomicBool::new(false));
        let makes = Arc::new(AtomicUsize::new(0));
        let maker = FakeMaker {
            already_poisoned: Arc::clone(&already_poisoned),
            makes: Arc::clone(&makes),
        };

        let mut buffered =
            tower::buffer::Buffer::new(tower::reconnect::Reconnect::new(maker, ()), 4);

        // Issue calls until one succeeds. The FIRST inner `poll_ready` errors
        // (transient); `Reconnect` swallows it, re-makes a healthy service,
        // and a subsequent call must succeed — the Buffer never poisons.
        // A generic `is_ok()` here would let a `tokio::time::Elapsed` (from a
        // hung latch) masquerade as success, so we assert with a bespoke
        // deadline message AND on the served payload.
        let mut last_err: Option<BoxError> = None;
        let mut served: Option<&'static str> = None;
        for _ in 0..4 {
            let outcome: Result<Result<&'static str, BoxError>, tokio::time::error::Elapsed> =
                tokio::time::timeout(
                    core::time::Duration::from_secs(5),
                    ready_and_call(&mut buffered),
                )
                .await;
            let call_result = outcome.expect(
                "never-poison invariant violated: a transient poll_ready error \
                 permanently latched the buffered QUIC member",
            );
            match call_result {
                Ok(v) => {
                    served = Some(v);
                    break;
                }
                Err(e) => last_err = Some(e),
            }
        }

        let served = served.unwrap_or_else(|| {
            panic!(
                "never-poison invariant violated: a transient poll_ready error \
                 permanently latched the buffered QUIC member (last error: {:?})",
                last_err.map(|e| e.to_string()),
            )
        });
        assert_eq!(
            served, "served",
            "reconnected member must serve the request payload",
        );
        assert!(
            makes.load(Ordering::SeqCst) >= 2,
            "Reconnect must have re-made the inner service at least once after the \
             transient error (makes={})",
            makes.load(Ordering::SeqCst),
        );

        // Silence unused-Pin import lint if the trait path shifts; keep the
        // deadline type explicit above to prove Elapsed cannot pass as Ok.
        let _ = core::marker::PhantomData::<Pin<Box<()>>>;
    }
}
