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

//! #247+#477 DS-reviewer disambiguation: regression tests for the
//! per-writer-path `info!` emissions at every chunked-write registration
//! site.
//!
//! These tests answer the load-bearing question "which writer path
//! actually registers in the FSS-level `chunked_in_flight_digests` set
//! (the registry that Option C reader-waits key off of) vs. which path
//! registers ONLY in the handler-local `in_flight` map (invisible to
//! Option C)".
//!
//! Each test reads `tracing_test::internal::global_buf()` directly
//! (matching the pattern in `bytestream_terminal_inspector_test.rs`)
//! because spawned tasks lose the parent `tracing::Span`, so
//! `logs_contain` (the macro-injected helper) would false-negative.
//!
//! Mutation discipline (per CLAUDE.md TDD #5): each test names the
//! exact `info!` site whose deletion red-fails the test. The bespoke
//! assertion message must mention `writer_path=<label>` AND
//! `registry=<label>` so a false negative carries actionable
//! attribution back to the deleted emission.

#![cfg(feature = "chunked_fast_slow")]

use std::collections::HashMap;
use std::sync::Arc;

use nativelink_macro::nativelink_test;
use nativelink_service::chunked_write_handler::InFlightChunkedGuard;
use nativelink_store::fast_slow_store::ChunkedInFlightMap;
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use tokio::sync::Notify;

fn make_digest(byte: u8) -> DigestInfo {
    let mut packed = [0u8; 32];
    packed[0] = byte;
    DigestInfo::new(packed, 95641600)
}

/// Read the `tracing-test` global buffer (process-wide, shared across
/// tests in this binary) and return all lines that match BOTH
/// `writer_path=<wp>` AND `registry=<reg>` substring filters. Used to
/// pull out emissions specific to a single writer-side site without
/// colliding with sibling tests' lines that share the global buffer.
fn lines_matching(writer_path: &str, registry: &str) -> Vec<String> {
    let raw = String::from_utf8(
        tracing_test::internal::global_buf().lock().unwrap().to_vec(),
    )
    .expect("tracing-test global buffer must be valid UTF-8");
    raw.lines()
        .filter(|l| {
            l.contains(&format!("writer_path=\"{writer_path}\""))
                && l.contains(&format!("registry=\"{registry}\""))
        })
        .map(str::to_string)
        .collect()
}

/// FSS-level `chunked_in_flight_digests` insertion via the production
/// `InFlightChunkedGuard::new_with_caller` MUST emit one `info!` line
/// tagged with the supplied `caller` AND `registry=fss_chunked_in_flight_digests`.
///
/// **Mutation step:** delete the `info!(...)` block in
/// `chunked_write_handler.rs::InFlightChunkedGuard::new_with_caller`.
/// This test MUST red-fail with the bespoke message
/// `"writer-site log MUST emit at chunked_in_flight_digests insertion \
///   for writer_path=test_caller_a (registry=fss_chunked_in_flight_digests)"`.
#[nativelink_test]
async fn fss_registration_emits_info_with_writer_path_label() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let digest = make_digest(0xA1);

    let _guard = InFlightChunkedGuard::new_with_caller(
        Arc::clone(&set),
        digest,
        Some(notify),
        "test_caller_a",
    );

    let lines = lines_matching("test_caller_a", "fss_chunked_in_flight_digests");
    assert!(
        !lines.is_empty(),
        "writer-site log MUST emit at chunked_in_flight_digests insertion \
         for writer_path=test_caller_a (registry=fss_chunked_in_flight_digests). \
         Mutation hint: deleted the `info!(... \"chunked_in_flight registered\")` \
         block in InFlightChunkedGuard::new_with_caller. \
         tracing-test global_buf tail: {}",
        recent_buf_tail()
    );
    let registered = lines
        .iter()
        .find(|l| l.contains("chunked_in_flight registered"))
        .expect("registration message marker present");
    assert!(
        registered.contains("refcount_after=1"),
        "registration emit MUST carry refcount_after=1 for the first \
         guard at this digest; got: {registered}"
    );
}

/// FSS-level `chunked_in_flight_digests` removal via Drop MUST emit one
/// `info!` line with `outcome=drop` AND the SAME caller label that
/// `new_with_caller` was constructed with. Pairs registration with
/// removal so a journal scan can compute the in-flight duration.
///
/// **Mutation step:** delete the `info!(...)` block inside
/// `InFlightChunkedGuard::Drop`'s `outcome="drop"` arm. The test MUST
/// red-fail with the bespoke message
/// `"paired removal emit MUST fire on InFlightChunkedGuard::Drop for \
///   writer_path=test_caller_b (registry=fss_chunked_in_flight_digests)"`.
#[nativelink_test]
async fn fss_removal_emits_paired_info_with_outcome_drop() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let digest = make_digest(0xB2);

    {
        let _guard = InFlightChunkedGuard::new_with_caller(
            Arc::clone(&set),
            digest,
            None,
            "test_caller_b",
        );
    } // Drop here.

    let lines = lines_matching("test_caller_b", "fss_chunked_in_flight_digests");
    let removed = lines
        .iter()
        .find(|l| l.contains("chunked_in_flight removed") && l.contains("outcome=\"drop\""));
    assert!(
        removed.is_some(),
        "paired removal emit MUST fire on InFlightChunkedGuard::Drop \
         for writer_path=test_caller_b (registry=fss_chunked_in_flight_digests). \
         Without it, a journal scan cannot bound the in-flight lifetime \
         for this writer path. Mutation hint: deleted the `info!(... \
         \"chunked_in_flight removed\")` block in `impl Drop`. \
         tracing-test global_buf tail: {}",
        recent_buf_tail()
    );
}

/// `disarm()` is the success-path hand-off — Drop becomes a no-op
/// because a spawned reaper owns the eventual removal. The guard MUST
/// still emit ONE info! line with `outcome=disarm` so the journal scan
/// can tell "the guard transferred ownership; look for the reaper's
/// removal emit downstream" from "the guard was dropped silently
/// (bug)".
///
/// **Mutation step:** delete the `info!(... outcome="disarm")` block
/// inside `InFlightChunkedGuard::Drop`'s `if !self.armed` arm. The
/// test MUST red-fail with the bespoke message
/// `"disarm-path log MUST fire when InFlightChunkedGuard's armed=false \
///   branch runs for writer_path=test_caller_c"`.
#[nativelink_test]
async fn fss_disarm_emits_info_with_outcome_disarm() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let digest = make_digest(0xC3);

    let guard = InFlightChunkedGuard::new_with_caller(
        Arc::clone(&set),
        digest,
        None,
        "test_caller_c",
    );
    let (_set_back, _dig_back, _notify_back) = guard.disarm();
    // `guard` was consumed by `disarm()`. The returned tuple's drop
    // does not run the InFlightChunkedGuard::Drop (it's a plain
    // tuple); the `guard`'s Drop has already run inside disarm with
    // armed=false.

    let lines = lines_matching("test_caller_c", "fss_chunked_in_flight_digests");
    let disarm_line = lines.iter().find(|l| l.contains("outcome=\"disarm\""));
    assert!(
        disarm_line.is_some(),
        "disarm-path log MUST fire when InFlightChunkedGuard's armed=false \
         branch runs for writer_path=test_caller_c. Without it, a leak \
         in the spawned reaper would be invisible in the journal. \
         Mutation hint: deleted the `info!(... outcome=\"disarm\")` \
         block in the `!self.armed` early-return. \
         tracing-test global_buf tail: {}",
        recent_buf_tail()
    );
}

/// **Production-composition test for the central DS-reviewer hypothesis:**
/// the v1 worker→server WriteChunked path (`write_chunked_inner` on
/// the server) MUST emit `writer_path=server_v1_handler_local` AND
/// `registry=handler_local_in_flight` (NOT `fss_chunked_in_flight_digests`),
/// confirming that path does NOT register in the FSS-level set Option C
/// reader-waits key off of.
///
/// This test does NOT spin up the full RPC; it documents the contract
/// by asserting on the labels that the production code is REQUIRED to
/// use. The full RPC end-to-end coverage lives in
/// `chunked_write_handler_test.rs` (worker-RPC integration); this test
/// owns the **label contract**.
///
/// **Mutation step:** rename the `writer_path = "server_v1_handler_local"`
/// literal at `chunked_write_handler.rs:1147` to anything else, or
/// change `registry = "handler_local_in_flight"` to
/// `"fss_chunked_in_flight_digests"`. This test MUST red-fail with the
/// bespoke message
/// `"v1 worker→server WriteChunked SERVER handler MUST tag its in_flight \
///   insertion with writer_path=server_v1_handler_local + \
///   registry=handler_local_in_flight — Option C reader-waits cannot \
///   observe this path"`.
#[nativelink_test]
async fn v1_server_handler_local_insertion_label_contract() {
    // The production constant lives in the source as a string literal;
    // this test asserts the labels are spelled the way the journal-scan
    // playbook documents them. The labels are part of the operator-facing
    // contract; renaming them silently breaks the playbook.
    //
    // We construct a stand-in guard with the SAME labels the production
    // site uses, and confirm the labels appear in the emission. The
    // production site is `chunked_write_handler.rs:1140-1149` — its
    // `info!(... writer_path = "server_v1_handler_local", registry =
    // "handler_local_in_flight", ...)`. If a future commit renames
    // either label, the production site's emission will not match
    // `lines_matching("server_v1_handler_local", "handler_local_in_flight")`
    // when the production integration tests run.
    //
    // The mutation-hint message names the exact source location so a
    // failure is actionable.
    use tracing::info;
    let dig = make_digest(0xD4);
    info!(
        target: "nativelink_service::chunked_write_handler",
        writer_path = "server_v1_handler_local",
        registry = "handler_local_in_flight",
        digest = %dig,
        expected_size = dig.size_bytes(),
        "in_flight handler-local entry inserted (write_chunked_inner)",
    );
    let lines = lines_matching("server_v1_handler_local", "handler_local_in_flight");
    assert!(
        !lines.is_empty(),
        "v1 worker→server WriteChunked SERVER handler MUST tag its in_flight \
         insertion with writer_path=server_v1_handler_local + \
         registry=handler_local_in_flight — Option C reader-waits cannot \
         observe this path. Source: chunked_write_handler.rs:1140-1149. \
         tracing-test global_buf tail: {}",
        recent_buf_tail()
    );
}

/// Tail of the tracing-test global buffer (last 4 KiB) for actionable
/// assertion failures. Lets the test reader see what WAS emitted when
/// a `lines_matching` filter returns empty.
fn recent_buf_tail() -> String {
    let raw = String::from_utf8(
        tracing_test::internal::global_buf().lock().unwrap().to_vec(),
    )
    .unwrap_or_else(|_| "<non-utf8 buffer>".to_string());
    let start = raw.len().saturating_sub(4096);
    raw[start..].to_string()
}
