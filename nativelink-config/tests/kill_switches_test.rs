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

//! Schema-level coverage for kill-switch fields. Each test exercises:
//!
//! 1. `#[serde(default)]` returns `false` when the field is absent
//!    (backwards-compat invariant).
//! 2. Explicit `true` is accepted and reflected in the parsed
//!    struct.
//! 3. Explicit `false` is also accepted (sanity).
//!
//! Together these guarantee that the production config flips
//! cannot silently regress to default-OFF if a field is renamed,
//! removed, or accidentally hidden behind a `#[serde(skip)]`
//! attribute.
//!
//! Four chunked-streaming kill-switches (#212, 2026-05-02) and the
//! F2 deferred-output-uploads kill-switch (2026-06-12) are covered.

use nativelink_config::cas_server::{GlobalConfig, LocalWorkerConfig};
use nativelink_config::stores::{FastSlowSpec, GrpcSpec, MemorySpec};

// ----- GrpcSpec.chunked_writes_enabled (Phase 2.4) -----------------

const GRPC_BASE: &str = r#"{
    "endpoints": [{"address": "grpc://localhost:50051"}],
    "store_type": "cas"
}"#;

#[test]
fn grpc_chunked_writes_default_false() {
    let spec: GrpcSpec = serde_json5::from_str(GRPC_BASE).expect("base parse");
    assert!(
        !spec.chunked_writes_enabled,
        "chunked_writes_enabled must default to false for backwards compatibility"
    );
}

#[test]
fn grpc_chunked_writes_explicit_true() {
    let json = r#"{
        "endpoints": [{"address": "grpc://localhost:50051"}],
        "store_type": "cas",
        "chunked_writes_enabled": true
    }"#;
    let spec: GrpcSpec = serde_json5::from_str(json).expect("explicit-true parse");
    assert!(
        spec.chunked_writes_enabled,
        "chunked_writes_enabled=true must round-trip"
    );
}

#[test]
fn grpc_chunked_writes_explicit_false() {
    let json = r#"{
        "endpoints": [{"address": "grpc://localhost:50051"}],
        "store_type": "cas",
        "chunked_writes_enabled": false
    }"#;
    let spec: GrpcSpec = serde_json5::from_str(json).expect("explicit-false parse");
    assert!(
        !spec.chunked_writes_enabled,
        "chunked_writes_enabled=false must round-trip"
    );
}

// ----- FastSlowSpec.chunked_reads_enabled (Phase 2.5) ---------------

const FAST_SLOW_BASE: &str = r#"{
    "fast": {"memory": {}},
    "slow": {"memory": {}}
}"#;

#[test]
fn fast_slow_chunked_reads_default_false() {
    let spec: FastSlowSpec = serde_json5::from_str(FAST_SLOW_BASE).expect("base parse");
    assert!(
        !spec.chunked_reads_enabled,
        "chunked_reads_enabled must default to false for backwards compatibility"
    );
}

#[test]
fn fast_slow_chunked_reads_explicit_true() {
    let json = r#"{
        "fast": {"memory": {}},
        "slow": {"memory": {}},
        "chunked_reads_enabled": true
    }"#;
    let spec: FastSlowSpec = serde_json5::from_str(json).expect("explicit-true parse");
    assert!(
        spec.chunked_reads_enabled,
        "chunked_reads_enabled=true must round-trip"
    );
}

#[test]
fn fast_slow_chunked_reads_explicit_false() {
    let json = r#"{
        "fast": {"memory": {}},
        "slow": {"memory": {}},
        "chunked_reads_enabled": false
    }"#;
    let spec: FastSlowSpec = serde_json5::from_str(json).expect("explicit-false parse");
    assert!(
        !spec.chunked_reads_enabled,
        "chunked_reads_enabled=false must round-trip"
    );
}

// ----- MemorySpec.emit_backpressure_enabled (Phase 2.6) ------------

#[test]
fn memory_emit_backpressure_default_false() {
    let spec: MemorySpec = serde_json5::from_str("{}").expect("base parse");
    assert!(
        !spec.emit_backpressure_enabled,
        "emit_backpressure_enabled must default to false for backwards compatibility"
    );
}

#[test]
fn memory_emit_backpressure_explicit_true() {
    let json = r#"{ "emit_backpressure_enabled": true }"#;
    let spec: MemorySpec = serde_json5::from_str(json).expect("explicit-true parse");
    assert!(
        spec.emit_backpressure_enabled,
        "emit_backpressure_enabled=true must round-trip"
    );
}

#[test]
fn memory_emit_backpressure_explicit_false() {
    let json = r#"{ "emit_backpressure_enabled": false }"#;
    let spec: MemorySpec = serde_json5::from_str(json).expect("explicit-false parse");
    assert!(
        !spec.emit_backpressure_enabled,
        "emit_backpressure_enabled=false must round-trip"
    );
}

// ----- GlobalConfig.bazel_facing_internal_chunking_enabled (Phase 2.7) -----

const GLOBAL_BASE: &str = r#"{ "max_open_files": 1024 }"#;

#[test]
fn global_bazel_chunking_default_false() {
    let cfg: GlobalConfig = serde_json5::from_str(GLOBAL_BASE).expect("base parse");
    assert!(
        !cfg.bazel_facing_internal_chunking_enabled,
        "bazel_facing_internal_chunking_enabled must default to false for backwards compatibility"
    );
}

#[test]
fn global_bazel_chunking_explicit_true() {
    let json = r#"{
        "max_open_files": 1024,
        "bazel_facing_internal_chunking_enabled": true
    }"#;
    let cfg: GlobalConfig = serde_json5::from_str(json).expect("explicit-true parse");
    assert!(
        cfg.bazel_facing_internal_chunking_enabled,
        "bazel_facing_internal_chunking_enabled=true must round-trip"
    );
}

#[test]
fn global_bazel_chunking_explicit_false() {
    let json = r#"{
        "max_open_files": 1024,
        "bazel_facing_internal_chunking_enabled": false
    }"#;
    let cfg: GlobalConfig = serde_json5::from_str(json).expect("explicit-false parse");
    assert!(
        !cfg.bazel_facing_internal_chunking_enabled,
        "bazel_facing_internal_chunking_enabled=false must round-trip"
    );
}

// ----- Production config files (when present on the dev box) ------

/// Sanity-parse the canonical production config files at the absolute
/// paths used in CLAUDE.md. The test is `#[ignore]` by default so CI
/// (which has neither file) silently skips, but a developer running
/// `cargo test -p nativelink-config -- --ignored` against the
/// production configs gets a real signal — the json5 syntax + schema
/// have to be valid for the `bazel_facing_internal_chunking_enabled`
/// flip to round-trip through `try_from_json5_file`.
///
/// If you flipped the kill-switches in the canonical files but a
/// schema rename broke parsing, this test will fail the moment you
/// run `--ignored`.
#[test]
#[ignore = "production config files only present on the deploy box"]
fn production_buildcache_config_parses() {
    let path = "/home/user/fl/bld/infra/nativelink/prod-server.json5";
    let cfg = nativelink_config::cas_server::CasConfig::try_from_json5_file(path)
        .unwrap_or_else(|e| panic!("failed to parse {path}: {e:?}"));
    let g = cfg
        .global
        .as_ref()
        .expect("prod-server.json5 must have a global section");
    assert!(
        g.bazel_facing_internal_chunking_enabled,
        "production sign-off (2026-05-02) flipped this ON; if false, the flip regressed"
    );
}

#[test]
#[ignore = "production config files only present on the deploy box"]
fn production_worker_config_parses() {
    let path = "/home/user/fl/bld/infra/nativelink/worker.json5";
    let _cfg = nativelink_config::cas_server::CasConfig::try_from_json5_file(path)
        .unwrap_or_else(|e| panic!("failed to parse {path}: {e:?}"));
    // Asserting the chunked_writes_enabled=true flip on the slow GrpcStore
    // requires walking a Memory→Filesystem→Grpc chain; the parse-success
    // alone is the durable signal. The schema-level test
    // `grpc_chunked_writes_explicit_true` covers the field-deserialization
    // contract.
}

// ----- All four ON together (production-shape sanity) --------------

#[test]
fn all_four_kill_switches_on_together_parse_clean() {
    // Smoke-test the production shape: GrpcSpec ON, FastSlowSpec ON,
    // MemorySpec ON inside the FastSlowSpec, GlobalConfig ON. If a
    // future schema change makes any of these mutually exclusive,
    // this test fails and surfaces it at config-parse time instead
    // of at server boot in production.
    let grpc: GrpcSpec = serde_json5::from_str(
        r#"{
            "endpoints": [{"address": "grpc://localhost:50051"}],
            "store_type": "cas",
            "chunked_writes_enabled": true
        }"#,
    )
    .expect("grpc all-on parse");
    assert!(grpc.chunked_writes_enabled);

    let fss: FastSlowSpec = serde_json5::from_str(
        r#"{
            "fast": {"memory": {"emit_backpressure_enabled": true}},
            "slow": {"memory": {}},
            "chunked_reads_enabled": true
        }"#,
    )
    .expect("fss all-on parse");
    assert!(fss.chunked_reads_enabled);

    let global: GlobalConfig = serde_json5::from_str(
        r#"{
            "max_open_files": 1024,
            "bazel_facing_internal_chunking_enabled": true
        }"#,
    )
    .expect("global all-on parse");
    assert!(global.bazel_facing_internal_chunking_enabled);
}

// ----- LocalWorkerConfig.deferred_output_uploads_enabled (F2) ------
//
// These three tests guard that the F2 kill-switch default cannot
// silently regress to ON if the field is renamed, removed, or
// accidentally given a `#[serde(skip)]` attribute. They follow the
// same 3-part template as the four chunked-streaming tests above.

/// Minimal JSON that satisfies all non-defaulted required fields of
/// `LocalWorkerConfig`. Fields without `#[serde(default)]`:
/// `worker_api_endpoint`, `cas_fast_slow_store`, `work_directory`,
/// and `platform_properties`.
const LOCAL_WORKER_BASE: &str = r#"{
    "worker_api_endpoint": {"uri": "grpc://localhost:50061"},
    "cas_fast_slow_store": "cas",
    "work_directory": "/tmp/worker_test",
    "platform_properties": {}
}"#;

/// (a) Default deserializes to `false` — backwards-compat invariant.
/// If this fails, the field was either renamed (key mismatch) or the
/// default was changed to `true` (which would enable deferred uploads
/// on every worker that hasn't explicitly set the flag).
#[test]
fn local_worker_deferred_uploads_default_false() {
    let cfg: LocalWorkerConfig =
        serde_json5::from_str(LOCAL_WORKER_BASE).expect("base parse");
    assert!(
        !cfg.deferred_output_uploads_enabled,
        "deferred_output_uploads_enabled must default to false for \
        backwards compatibility — enabling by default would violate the \
        ≥2-replica durability invariant without operator awareness (#F2)"
    );
}

/// (b) Explicit `true` round-trips correctly.
/// If this fails, the field was renamed or `#[serde(skip)]` was added,
/// meaning operators can no longer enable the kill-switch.
#[test]
fn local_worker_deferred_uploads_explicit_true() {
    let json = r#"{
        "worker_api_endpoint": {"uri": "grpc://localhost:50061"},
        "cas_fast_slow_store": "cas",
        "work_directory": "/tmp/worker_test",
        "platform_properties": {},
        "deferred_output_uploads_enabled": true
    }"#;
    let cfg: LocalWorkerConfig = serde_json5::from_str(json).expect("explicit-true parse");
    assert!(
        cfg.deferred_output_uploads_enabled,
        "deferred_output_uploads_enabled=true must round-trip — field \
        renamed or serde(skip) added? (#F2)"
    );
}

/// (c) Explicit `false` round-trips correctly (sanity).
#[test]
fn local_worker_deferred_uploads_explicit_false() {
    let json = r#"{
        "worker_api_endpoint": {"uri": "grpc://localhost:50061"},
        "cas_fast_slow_store": "cas",
        "work_directory": "/tmp/worker_test",
        "platform_properties": {},
        "deferred_output_uploads_enabled": false
    }"#;
    let cfg: LocalWorkerConfig = serde_json5::from_str(json).expect("explicit-false parse");
    assert!(
        !cfg.deferred_output_uploads_enabled,
        "deferred_output_uploads_enabled=false must round-trip (#F2)"
    );
}
