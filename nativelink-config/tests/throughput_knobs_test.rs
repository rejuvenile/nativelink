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

//! Schema-level coverage for the FL-681 operator-tunable
//! worker→server upload-throughput knobs.
//!
//! Currently exposes one knob: `LocalWorkerConfig.max_concurrent_uploads`
//! — the per-call fan-out cap on `handle_upload_missing_blobs`
//! (`local_worker.rs`). The serde contract under test:
//!
//! 1. Field absent → `0` (the "unset → built-in default" sentinel, so
//!    every existing deployed config behaves identically — the worker
//!    resolves `0` to the historical hardcoded `MAX_CONCURRENT_UPLOADS
//!    = 32` at construction time).
//! 2. An explicit non-zero value round-trips.
//! 3. An explicit `0` round-trips (operators can write the sentinel
//!    out without changing behavior).

use nativelink_config::cas_server::LocalWorkerConfig;

/// Minimal JSON satisfying all non-defaulted required fields of
/// `LocalWorkerConfig`: `worker_api_endpoint`, `cas_fast_slow_store`,
/// `work_directory`, `platform_properties`.
const LOCAL_WORKER_BASE: &str = r#"{
    "worker_api_endpoint": {"uri": "grpc://localhost:50061"},
    "cas_fast_slow_store": "cas",
    "work_directory": "/tmp/worker_test",
    "platform_properties": {}
}"#;

/// (a) Absent → `0` sentinel. If this fails, the field was renamed
/// (key mismatch) or given a non-zero `#[serde(default = ...)]` — either
/// of which would change the fan-out cap on every config that does not
/// set the field, contradicting the backward-compat contract.
#[test]
fn local_worker_max_concurrent_uploads_default_zero() {
    let cfg: LocalWorkerConfig =
        serde_json5::from_str(LOCAL_WORKER_BASE).expect("base parse");
    assert_eq!(
        cfg.max_concurrent_uploads, 0,
        "max_concurrent_uploads must default to the 0 sentinel so the \
         worker resolves it to the hardcoded MAX_CONCURRENT_UPLOADS=32 \
         and existing deployed configs behave identically (FL-681)"
    );
}

/// (b) Explicit non-zero round-trips. If this fails, the field was
/// renamed or `#[serde(skip)]` was added, so operators cannot raise the
/// upload fan-out cap.
#[test]
fn local_worker_max_concurrent_uploads_explicit_value() {
    let json = r#"{
        "worker_api_endpoint": {"uri": "grpc://localhost:50061"},
        "cas_fast_slow_store": "cas",
        "work_directory": "/tmp/worker_test",
        "platform_properties": {},
        "max_concurrent_uploads": 128
    }"#;
    let cfg: LocalWorkerConfig = serde_json5::from_str(json).expect("explicit-value parse");
    assert_eq!(
        cfg.max_concurrent_uploads, 128,
        "max_concurrent_uploads=128 must round-trip — field renamed or \
         serde(skip) added? (FL-681)"
    );
}

/// (c) Explicit `0` round-trips (sanity — the sentinel is writable).
#[test]
fn local_worker_max_concurrent_uploads_explicit_zero() {
    let json = r#"{
        "worker_api_endpoint": {"uri": "grpc://localhost:50061"},
        "cas_fast_slow_store": "cas",
        "work_directory": "/tmp/worker_test",
        "platform_properties": {},
        "max_concurrent_uploads": 0
    }"#;
    let cfg: LocalWorkerConfig = serde_json5::from_str(json).expect("explicit-zero parse");
    assert_eq!(
        cfg.max_concurrent_uploads, 0,
        "max_concurrent_uploads=0 must round-trip as the default sentinel (FL-681)"
    );
}
