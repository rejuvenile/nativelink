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

//! Gap 1 regression test: the worker→scheduler WorkerApi CONTROL-PLANE
//! endpoint must be built WITH connection keepalive so a silently
//! half-open scheduler connection (no GOAWAY / no RST) surfaces as a
//! stream error within a keepalive cycle — letting `inner.run` return and
//! the reconnect loop (`local_worker.rs:~4450`) re-establish the stream.
//!
//! Before the fix the WorkerApi endpoint was built via
//! `tls_utils::endpoint_from`, which sets ONLY `tcp_nodelay` — NO
//! keepalive (`tls_utils.rs:147`). The DATA channel built via
//! `tls_utils::endpoint` got `tcp_keepalive` + the HTTP/2 keepalive trio
//! (`tls_utils.rs:200-203`); the control plane did not, because it uses a
//! different (slimmer) config type (`EndpointConfig`) and the lower-level
//! string builder. This test pins the keepalive onto the control-plane
//! endpoint.
//!
//! tonic 0.14.5 only exposes a public getter for the TCP-level keepalive
//! (`Endpoint::get_tcp_keepalive`); the HTTP/2 keepalive params
//! (`http2_keep_alive_interval`, `keep_alive_timeout`,
//! `keep_alive_while_idle`) have no read accessors. We therefore assert on
//! `get_tcp_keepalive()`, which is `None` without the fix and `Some(30s)`
//! with it — a mutation-sensitive witness that the keepalive block runs.

use core::time::Duration;

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_worker::local_worker::{
    WORKER_API_TCP_KEEPALIVE, build_worker_api_tcp_endpoint,
};

/// The control-plane endpoint must carry TCP keepalive matching the
/// data-channel default (`tls_utils::endpoint` uses 30s when unset). A
/// half-open connection is then detected within a keepalive cycle instead
/// of hanging an `execution_response` / `complete` / `blobs_available`
/// send indefinitely.
#[nativelink_test]
async fn worker_api_endpoint_has_tcp_keepalive() -> Result<(), Error> {
    let endpoint = build_worker_api_tcp_endpoint(
        "grpc://localhost:50061",
        None,
        Duration::from_secs_f32(5.0),
    )?;

    assert_eq!(
        endpoint.get_tcp_keepalive(),
        Some(WORKER_API_TCP_KEEPALIVE),
        "WorkerApi control-plane endpoint built WITHOUT TCP keepalive — a \
         half-open scheduler connection will hang the bidi stream and block \
         reconnect (Gap 1)",
    );
    Ok(())
}

/// The keepalive constant must match the data-channel default of 30s
/// (`tls_utils::endpoint` falls back to `Duration::from_secs(30)` when the
/// config field is 0). Pinning the literal guards against a doc-comment
/// drifting away from the value actually applied.
#[nativelink_test]
async fn worker_api_tcp_keepalive_matches_data_channel_default() -> Result<(), Error> {
    assert_eq!(
        WORKER_API_TCP_KEEPALIVE,
        Duration::from_secs(30),
        "WorkerApi TCP keepalive must mirror the data-channel default \
         (tls_utils::endpoint uses 30s when tcp_keepalive_s is unset)",
    );
    Ok(())
}
