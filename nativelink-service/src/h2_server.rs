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

//! Shared construction of the HTTP/2 server builder used by
//! `src/bin/nativelink.rs::inner_main` and the bench `start_v2_server` in
//! `benchmarks/src/scenarios/chunked_v2.rs`.
//!
//! Production previously inline-constructed
//! `hyper_util::server::conn::auto::Builder<TaskExecutor>` and applied the
//! 10 settings sourced from `HttpServerConfig`; the bench used the
//! `tonic::transport::Server` high-level wrapper and applied only 3 of
//! them (#586 was a catch-up for `max_frame_size`; the #584 audit found
//! the underlying drift class). After this extraction both sites call
//! [`build_h2_server_builder`] so any future production setting change
//! propagates by construction.
//!
//! Pattern-C Phase 2 (parallels Phase 1's `build_store_manager`).

use core::time::Duration;

use hyper_util::rt::TokioTimer;
use hyper_util::server::conn::auto;
use nativelink_config::cas_server::HttpServerConfig;
use nativelink_error::{Error, ResultExt};
use nativelink_util::task::TaskExecutor;

/// Build a fully-configured `hyper_util::server::conn::auto::Builder` with
/// all 10 production HTTP/2 settings from `HttpServerConfig` applied.
///
/// Always installs a `TokioTimer` on the http2 builder; without it the
/// keep-alive `PingPong`, header-read, and stream-idle paths panic with
/// "You must supply a timer." on the first poll. The panic fires
/// regardless of whether `keep_alive_interval` is set because http2's
/// idle-stream tracker also schedules timer events. The cost is one
/// allocation per connection and the panic surface is total: every gRPC
/// request fails with `ClosedChannelException` at the client.
/// (Production outage 2026-05-12: panic on every connection at port
/// 50051/50061 after infra commit `db1ac5bf` enabled
/// `http2_keep_alive_interval=30` in `prod-server.json5`.)
///
/// Default values match production:
/// - `initial_stream_window_size`: 16 MiB (hyper default 64 KiB caps
///   per-stream throughput at ~64 MB/s with 1ms RTT)
/// - `initial_connection_window_size`: 128 MiB
/// - `max_frame_size`: 4 MiB
/// - `max_send_buf_size`: 2 MiB
///
/// All other settings are `None`-by-default in the config (hyper defaults
/// apply when the field is unset).
pub fn build_h2_server_builder(
    executor: TaskExecutor,
    config: &HttpServerConfig,
) -> Result<auto::Builder<TaskExecutor>, Error> {
    let mut http = auto::Builder::new(executor);
    http.http2().timer(TokioTimer::new());

    if let Some(value) = config.http2_keep_alive_interval {
        http.http2()
            .keep_alive_interval(Duration::from_secs(u64::from(value)));
    }

    if let Some(value) = config.experimental_http2_max_pending_accept_reset_streams {
        http.http2()
            .max_pending_accept_reset_streams(usize::try_from(value).err_tip(
                || "Could not convert experimental_http2_max_pending_accept_reset_streams",
            )?);
    }
    http.http2().initial_stream_window_size(
        config
            .experimental_http2_initial_stream_window_size
            .unwrap_or(16 * 1024 * 1024),
    );
    http.http2().initial_connection_window_size(
        config
            .experimental_http2_initial_connection_window_size
            .unwrap_or(128 * 1024 * 1024),
    );
    if let Some(value) = config.experimental_http2_adaptive_window {
        http.http2().adaptive_window(value);
    }
    http.http2().max_frame_size(
        config
            .experimental_http2_max_frame_size
            .unwrap_or(4 * 1024 * 1024),
    );
    if let Some(value) = config.experimental_http2_max_concurrent_streams {
        http.http2().max_concurrent_streams(value);
    }
    if let Some(value) = config.experimental_http2_keep_alive_timeout {
        http.http2()
            .keep_alive_timeout(Duration::from_secs(u64::from(value)));
    }
    http.http2().max_send_buf_size(
        usize::try_from(
            config
                .experimental_http2_max_send_buf_size
                .unwrap_or(2 * 1024 * 1024),
        )
        .err_tip(|| "Could not convert http2_max_send_buf_size")?,
    );
    if config.experimental_http2_enable_connect_protocol == Some(true) {
        http.http2().enable_connect_protocol();
    }
    if let Some(value) = config.experimental_http2_max_header_list_size {
        http.http2().max_header_list_size(value);
    }
    Ok(http)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The 10-setting drift detector: this test introspects the source
    /// of [`build_h2_server_builder`] to assert each named setter chain
    /// fires on the corresponding `HttpServerConfig` field. A future
    /// drift where someone deletes a setter while leaving the field on
    /// the struct would silently revert that setting to hyper's default
    /// on BOTH production AND the bench.
    ///
    /// `hyper_util::server::conn::auto::Builder` does not expose its
    /// applied settings (no `Debug` for the inner http2 config, no
    /// getters). Source-introspection is the next-best falsifier — the
    /// other option (a per-setter mock executor) would require forking
    /// the `Executor` trait, which is over-engineered for a drift
    /// detector. See `benchmarks/src/scenarios/chunked_v2.rs` for the
    /// sibling source-introspection test on the bench's W3_BENCH_*
    /// helpers (#563).
    ///
    /// Strip Rust line + block comments from `src`. Comments survive
    /// `include_str!` and would satisfy `.contains(setter)` even if a
    /// future hand-edit commented out the real setter call — the
    /// "mutate the fix and verify the test fails again" form in
    /// CLAUDE.md. Replacement keeps source offsets stable (each comment
    /// byte → space) so error messages still point at sensible lines.
    ///
    /// Limitation: the simple state machine does NOT track string or
    /// char literals. `build_h2_server_builder`'s body contains no
    /// string/char literals (verified at the time of writing), so a
    /// `//` or `/*` inside a literal cannot mis-trigger. If a future
    /// edit adds one, extend the state machine before relying on this.
    /// Block-comment nesting is also not supported (rust permits it but
    /// production code rarely uses it).
    fn strip_rust_comments(src: &str) -> String {
        let bytes = src.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            // Line comment: // to end of line. Keep the newline so line
            // numbers don't shift.
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
                while i < bytes.len() && bytes[i] != b'\n' {
                    out.push(b' ');
                    i += 1;
                }
                continue;
            }
            // Block comment: /* ... */. Keep newlines inside; replace
            // everything else with space.
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                out.push(b' ');
                out.push(b' ');
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    out.push(if bytes[i] == b'\n' { b'\n' } else { b' ' });
                    i += 1;
                }
                if i + 1 < bytes.len() {
                    out.push(b' ');
                    out.push(b' ');
                    i += 2;
                }
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        }
        // The body has no non-ASCII, but be safe.
        String::from_utf8(out).expect("comment-stripping preserves UTF-8 by replacing with ASCII space")
    }

    /// **Mutation:** delete OR comment out any one of the 11 setter
    /// chains (the `timer(TokioTimer::new())` call + the 10
    /// config-driven setters); this test red-fails with a bespoke
    /// "setter X missing" message. Also red-fails on accidental
    /// duplication (two copies of the same setter).
    #[test]
    fn builder_calls_all_setters() {
        const RAW_SRC: &str = include_str!("h2_server.rs");
        // Strip comments BEFORE isolating the fn body so a commented-out
        // setter cannot satisfy `.contains(setter)`. Without this strip
        // a future hand-edit that comments out the line — the canonical
        // CLAUDE.md "mutate to verify" form — green-passes the test and
        // production silently reverts to hyper's default. (Drift
        // detector fix-up; assumption-auditor finding on 46d6f7b6.)
        let stripped = strip_rust_comments(RAW_SRC);
        // Isolate the fn body so unrelated source matches (this test
        // file, doc-comments) cannot satisfy the contains check.
        let body = stripped
            .split("pub fn build_h2_server_builder")
            .nth(1)
            .expect("build_h2_server_builder must exist")
            .split("\n}\n")
            .next()
            .expect("fn body must terminate with newline-brace-newline");

        // The timer setter is unconditional and load-bearing (panic
        // surface, see fn doc-comment). Falsifier: if absent, the
        // builder will panic on first request rather than silently
        // misbehave — but missing-AND-noisy is a worse failure than
        // missing-AND-quiet, so still assert it here.
        assert!(
            body.contains("http.http2().timer(TokioTimer::new())"),
            "build_h2_server_builder no longer calls .timer(TokioTimer::new()); \
             hyper 1.x will panic on first request without a Timer. \
             (Production outage 2026-05-12.) Body:\n{body}"
        );

        // Each of the 10 settings on HttpServerConfig must reach the
        // builder. Listed in the same order as the fn body for
        // grep-greppability.
        for (setter, field) in [
            (".keep_alive_interval(", "http2_keep_alive_interval"),
            (
                ".max_pending_accept_reset_streams(",
                "experimental_http2_max_pending_accept_reset_streams",
            ),
            (
                ".initial_stream_window_size(",
                "experimental_http2_initial_stream_window_size",
            ),
            (
                ".initial_connection_window_size(",
                "experimental_http2_initial_connection_window_size",
            ),
            (".adaptive_window(", "experimental_http2_adaptive_window"),
            (".max_frame_size(", "experimental_http2_max_frame_size"),
            (
                ".max_concurrent_streams(",
                "experimental_http2_max_concurrent_streams",
            ),
            (
                ".keep_alive_timeout(",
                "experimental_http2_keep_alive_timeout",
            ),
            (
                ".max_send_buf_size(",
                "experimental_http2_max_send_buf_size",
            ),
            (
                ".enable_connect_protocol(",
                "experimental_http2_enable_connect_protocol",
            ),
            (
                ".max_header_list_size(",
                "experimental_http2_max_header_list_size",
            ),
        ] {
            // Count occurrences rather than just `contains`: catches
            // both removal (count == 0) AND accidental duplication
            // (count > 1). A duplicate setter call would silently apply
            // the second value, masking a paste-mistake.
            let count = body.matches(setter).count();
            assert_eq!(
                count, 1,
                "build_h2_server_builder must call {setter}...) exactly once \
                 (found {count}) — the {field} field on HttpServerConfig must \
                 reach the builder exactly one time. Body:\n{body}"
            );
            assert!(
                body.contains(field),
                "build_h2_server_builder no longer references the {field} \
                 config field — the {setter}...) call must read from it. \
                 Body:\n{body}"
            );
        }
    }

    /// Exercise the fn with a fully-populated `HttpServerConfig` to
    /// prove every conversion (`usize::try_from`, etc.) succeeds. The
    /// builder cannot be introspected for applied values; this test
    /// covers the "did the fn return Ok?" surface only.
    #[test]
    fn populated_config_constructs_ok() {
        let config = HttpServerConfig {
            http2_keep_alive_interval: Some(30),
            experimental_http2_max_pending_accept_reset_streams: Some(64),
            experimental_http2_initial_stream_window_size: Some(8 * 1024 * 1024),
            experimental_http2_initial_connection_window_size: Some(64 * 1024 * 1024),
            experimental_http2_adaptive_window: Some(true),
            experimental_http2_max_frame_size: Some(2 * 1024 * 1024),
            experimental_http2_max_concurrent_streams: Some(256),
            experimental_http2_keep_alive_timeout: Some(60),
            experimental_http2_max_send_buf_size: Some(1024 * 1024),
            experimental_http2_enable_connect_protocol: Some(true),
            experimental_http2_max_header_list_size: Some(64 * 1024),
        };
        let result = build_h2_server_builder(TaskExecutor::default(), &config);
        assert!(
            result.is_ok(),
            "build_h2_server_builder must accept a fully-populated \
             HttpServerConfig — failed with: {:?}",
            result.err()
        );
    }

    /// Default `HttpServerConfig` (all `None`) must also construct OK
    /// — the four defaulted settings (stream/conn window, max-frame,
    /// max-send-buf) apply their hyper-default-overriding constants
    /// unconditionally.
    #[test]
    fn default_config_constructs_ok() {
        let config = HttpServerConfig::default();
        let result = build_h2_server_builder(TaskExecutor::default(), &config);
        assert!(
            result.is_ok(),
            "build_h2_server_builder must accept HttpServerConfig::default() \
             — failed with: {:?}",
            result.err()
        );
    }
}
