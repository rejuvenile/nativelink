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

use nativelink_config::stores::ClientTlsConfig;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_util::tls_utils::{endpoint_from, load_client_config};
use tempfile::NamedTempFile;

#[cfg(feature = "quic")]
use core::time::Duration;
#[cfg(feature = "quic")]
use nativelink_util::tls_utils::{
    QUIC_UDP_BUF_BYTES, QUIC_UDP_BUF_WARN_THRESHOLD, tune_quic_udp_buffers,
};

#[nativelink_test]
async fn test_load_client_config_none() -> Result<(), Error> {
    let config = load_client_config(&None)?;
    assert!(config.is_none());
    Ok(())
}

#[nativelink_test]
async fn test_load_client_config_native_roots() -> Result<(), Error> {
    let config = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: Some(true),
        ca_file: None,
        cert_file: None,
        key_file: None,
    }))?;
    assert!(config.is_some());
    Ok(())
}

#[nativelink_test]
async fn test_load_client_config_missing_ca() -> Result<(), Error> {
    let result = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: None,
        ca_file: None,
        cert_file: None,
        key_file: None,
    }));
    assert!(matches!(
        result,
        Err(e) if e.to_string().contains("CA certificate must be provided")
    ));
    Ok(())
}

#[nativelink_test]
async fn test_load_client_config_cert_without_key() -> Result<(), Error> {
    let temp_file = NamedTempFile::new()?;
    let result = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: None,
        ca_file: Some(temp_file.path().to_str().unwrap().to_string()),
        cert_file: Some("tls.crt".to_string()),
        key_file: None,
    }));
    assert!(matches!(
        result,
        Err(e) if e.to_string().contains("Client certificate specified, but no key")
    ));
    Ok(())
}

#[nativelink_test]
async fn test_load_client_config_key_without_cert() -> Result<(), Error> {
    let temp_file = NamedTempFile::new()?;
    let result = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: None,
        ca_file: Some(temp_file.path().to_str().unwrap().to_string()),
        cert_file: None,
        key_file: Some("tls.key".to_string()),
    }));
    assert!(matches!(
        result,
        Err(e) if e.to_string().contains("Client key specified, but no certificate")
    ));
    Ok(())
}

#[nativelink_test]
async fn test_load_client_config_with_cert_files() -> Result<(), Error> {
    let temp_file = NamedTempFile::new()?;
    let config = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: None,
        ca_file: Some(temp_file.path().to_str().unwrap().to_string()),
        cert_file: Some(temp_file.path().to_str().unwrap().to_string()),
        key_file: Some(temp_file.path().to_str().unwrap().to_string()),
    }))?;
    assert!(config.is_some());
    Ok(())
}

#[nativelink_test]
async fn test_endpoint_from_http() -> Result<(), Error> {
    let endpoint = endpoint_from("http://localhost:50051", None)?;
    assert_eq!(endpoint.uri().scheme_str(), Some("http"));
    assert_eq!(endpoint.uri().host(), Some("localhost"));
    assert_eq!(endpoint.uri().port_u16(), Some(50051));
    Ok(())
}

#[nativelink_test]
async fn test_endpoint_from_https_with_tls() -> Result<(), Error> {
    let tls_config = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: Some(true),
        ca_file: None,
        cert_file: None,
        key_file: None,
    }))?;
    let endpoint = endpoint_from("https://example.com", tls_config)?;
    assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    assert_eq!(endpoint.uri().host(), Some("example.com"));
    Ok(())
}

#[nativelink_test]
async fn test_endpoint_from_grpcs_with_tls() -> Result<(), Error> {
    let tls_config = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: Some(true),
        ca_file: None,
        cert_file: None,
        key_file: None,
    }))?;
    let endpoint = endpoint_from("grpcs://example.com", tls_config)?;
    assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    assert_eq!(endpoint.uri().host(), Some("example.com"));
    Ok(())
}

#[nativelink_test]
async fn test_endpoint_from_https_without_tls() -> Result<(), Error> {
    let result = endpoint_from("https://example.com", None);
    assert!(matches!(
        result,
        Err(e) if e.to_string().contains("is https or grpcs, but no TLS configuration was provided")
    ));
    Ok(())
}

#[nativelink_test]
async fn test_endpoint_from_http_with_tls() -> Result<(), Error> {
    let tls_config = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: Some(true),
        ca_file: None,
        cert_file: None,
        key_file: None,
    }))?;
    let result = endpoint_from("http://example.com:8080", tls_config);
    assert!(matches!(
        result,
        Err(e) if e.to_string().contains("but the scheme is not https or grpcs")
    ));
    Ok(())
}

#[nativelink_test]
async fn test_endpoint_from_invalid_uri() -> Result<(), Error> {
    let result = endpoint_from("not a valid uri", None);
    assert!(matches!(
        result,
        Err(e) if e.to_string().contains("Unable to parse endpoint")
    ));
    Ok(())
}

#[nativelink_test]
async fn test_endpoint_from_missing_authority() -> Result<(), Error> {
    let tls_config = load_client_config(&Some(ClientTlsConfig {
        use_native_roots: Some(true),
        ca_file: None,
        cert_file: None,
        key_file: None,
    }))?;
    let result = endpoint_from("/path/no/authority", tls_config);
    assert!(matches!(
        result,
        Err(e) if e.to_string().contains("Unable to determine authority of endpoint")
    ));
    Ok(())
}

/// Production-composition regression test for the QUIC UDP buffer-tuning
/// helper used by the server, the client connection pool, and the worker
/// peer-CAS endpoint.
///
/// Quinn does NOT raise `SO_RCVBUF`/`SO_SNDBUF` on its UDP socket on its
/// own — it inherits whatever the kernel default is (often ~208 KiB on
/// stock Linux), which causes UDP packet drops and QUIC tail-latency
/// spikes under load. This test pins the contract that the helper:
///   1. Issues setsockopt for both directions on a freshly-bound UDP
///      socket (the same one Quinn will own).
///   2. Achieves an effective post-set buffer >= 2 MiB on a kernel
///      configured to honor the request, OR matches `net.core.{rmem,
///      wmem}_max` if the kernel is capping below that.
///
/// Note: Linux returns `2 * requested` from `getsockopt(SO_RCVBUF)`
/// because the kernel doubles internally for bookkeeping overhead, so a
/// successful 8 MiB request reads back as 16 MiB. We compare half of
/// the observed value against the threshold.
///
/// Wrapped in `tokio::time::timeout` purely as a deadlock detector with
/// a bespoke message — the helper itself is sync and should return
/// promptly; if it ever blocks, a generic `is_ok()` would mask the bug.
#[cfg(feature = "quic")]
#[nativelink_test]
async fn quic_udp_buffer_tuning_applies_minimum_2_mib() -> Result<(), Error> {
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        // Bind a real UDP socket the same way `h3_channel` does so we
        // exercise the production code path end-to-end (real socket,
        // real setsockopt syscall, real read-back).
        let udp_socket = std::net::UdpSocket::bind("127.0.0.1:0")
            .expect("bind 127.0.0.1:0 for UDP buffer test");
        let sock_ref = socket2::SockRef::from(&udp_socket);

        // Use the helper's return value rather than doing the
        // doubling-correction math manually — that way the test
        // exercises the actual public API contract (including the
        // platform-specific cfg-gated halving on Linux vs no-op
        // elsewhere).
        let buffers = tune_quic_udp_buffers(sock_ref, "test");
        let effective_rcvbuf = buffers.effective_rcvbuf;
        let effective_sndbuf = buffers.effective_sndbuf;

        // Read kernel caps so the assertion holds on machines that
        // can't honor the full 8 MiB request — we still want to
        // guarantee the helper raised the buffer to AT LEAST what
        // the kernel allows. Only meaningful on Linux; on macOS /
        // BSD the read returns None and floor falls back to the
        // warn threshold.
        let rmem_max = read_sysctl("/proc/sys/net/core/rmem_max").unwrap_or(usize::MAX);
        let wmem_max = read_sysctl("/proc/sys/net/core/wmem_max").unwrap_or(usize::MAX);

        let expected_rcvbuf_floor = QUIC_UDP_BUF_WARN_THRESHOLD.min(rmem_max);
        let expected_sndbuf_floor = QUIC_UDP_BUF_WARN_THRESHOLD.min(wmem_max);

        assert!(
            effective_rcvbuf >= expected_rcvbuf_floor,
            "QUIC SO_RCVBUF not raised: effective={effective_rcvbuf} bytes; \
             expected at least {expected_rcvbuf_floor} bytes \
             (min of warn-threshold {QUIC_UDP_BUF_WARN_THRESHOLD} and \
             rmem_max {rmem_max}); requested {QUIC_UDP_BUF_BYTES} bytes — \
             tune_quic_udp_buffers did not raise the recv buffer",
        );
        assert!(
            effective_sndbuf >= expected_sndbuf_floor,
            "QUIC SO_SNDBUF not raised: effective={effective_sndbuf} bytes; \
             expected at least {expected_sndbuf_floor} bytes \
             (min of warn-threshold {QUIC_UDP_BUF_WARN_THRESHOLD} and \
             wmem_max {wmem_max}); requested {QUIC_UDP_BUF_BYTES} bytes — \
             tune_quic_udp_buffers did not raise the send buffer",
        );

        eprintln!(
            "tune_quic_udp_buffers observed: \
             effective rcv={effective_rcvbuf} snd={effective_sndbuf}, \
             rmem_max={rmem_max} wmem_max={wmem_max}",
        );
    })
    .await;

    outcome.expect(
        "tune_quic_udp_buffers must complete promptly — \
         QUIC UDP buffer-tuning contract violated (helper hung)",
    );
    Ok(())
}

/// Minimal helper to read a single integer from a /proc/sys file.
/// Returns None if reading or parsing fails (e.g. non-Linux).
#[cfg(feature = "quic")]
fn read_sysctl(path: &str) -> Option<usize> {
    let raw = std::fs::read_to_string(path).ok()?;
    raw.trim().parse::<usize>().ok()
}
