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

//! Hand-maintained proto `type_url` string constants.
//!
//! These constants name the `prost_types::Any.type_url` wire strings for the
//! `BackpressureSignal` and `WatchdogTimeoutSignal` messages defined in
//! `com/github/trace_machina/nativelink/remote_execution/worker_api.proto`.
//!
//! **Why they live here and not in the generated `.pb.rs`:** prost does NOT
//! emit `type_url` constants for messages — only the message structs. Earlier
//! these constants were hand-injected directly into the generated
//! `com.github.trace_machina.nativelink.remote_execution.pb.rs`, so every
//! `bazel run nativelink-proto:update_protos` silently dropped them (the
//! generator `rmtree`s and rewrites the whole `genproto/` directory), breaking
//! the build until re-injected by hand. Relocating them to this hand-maintained
//! module that lives OUTSIDE `genproto/` makes proto regeneration idempotent and
//! permanently closes that footgun.
//!
//! The wire strings are derived mechanically from the proto package + message
//! name (`type.googleapis.com/<fully.qualified.MessageName>`); they are
//! wire-stable and MUST match on both encode and decode sides.

/// `BackpressureSignal` proto type URL for `prost_types::Any.type_url`.
/// Centralized constant so the encoder (`nativelink-error`) and decoder
/// (`looks_like_dead_channel` in `nativelink-store/src/grpc_store.rs`)
/// agree on the exact wire string. See design §13.1.1 point 2 for the
/// load-bearing classifier interaction.
pub const BACKPRESSURE_SIGNAL_TYPE_URL: &str =
    "type.googleapis.com/com.github.trace_machina.nativelink.remote_execution.BackpressureSignal";

/// `WatchdogTimeoutSignal` proto type URL for `prost_types::Any.type_url`.
/// Centralized constant so the encoder (`nativelink-service` watchdog)
/// and decoder (`classify_retryable` in chunked_client.rs) agree on
/// the exact wire string. The discriminator is the load-bearing gate
/// for the `DeadlineExceeded → Retry` arm.
pub const WATCHDOG_TIMEOUT_SIGNAL_TYPE_URL: &str = "type.googleapis.com/com.github.trace_machina.nativelink.remote_execution.WatchdogTimeoutSignal";
