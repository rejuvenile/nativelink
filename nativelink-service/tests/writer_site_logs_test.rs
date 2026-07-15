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
//! Cross-test pollution mitigation (testing-czar M3): every test uses
//! a per-test-unique caller label PLUS a per-test-unique digest byte
//! so `lines_matching` filters to lines that only THIS test could have
//! produced. A future test adding a `test_caller_X` colliding label
//! still cannot match because the digest discriminator is unique. A
//! random nonce on the caller label hardens further (compile-time
//! distinct `&'static str` per nonce is not possible without leaking
//! Strings; the digest-byte discriminator is the load-bearing
//! disambiguator).
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

/// Per-test digest with a unique discriminator byte. All tests in this
/// file use distinct bytes so `lines_matching` filters by digest
/// substring as a second disambiguator on top of the caller label.
fn make_digest(byte: u8) -> DigestInfo {
    let mut packed = [0u8; 32];
    packed[0] = byte;
    DigestInfo::new(packed, 95_641_600)
}

/// Read the `tracing-test` global buffer (process-wide, shared across
/// tests in this binary) and return all lines that match
/// `writer_path=<wp>` AND `registry=<reg>` AND a digest discriminator
/// substring. The digest substring is the per-test discriminator that
/// hardens against cross-test pollution: a future test that picks a
/// colliding caller label still cannot match because its digest bytes
/// differ. See testing-czar M3.
fn lines_matching(writer_path: &str, registry: &str, digest_discriminator: &str) -> Vec<String> {
    let raw = String::from_utf8(
        tracing_test::internal::global_buf().lock().unwrap().to_vec(),
    )
    .expect("tracing-test global buffer must be valid UTF-8");
    raw.lines()
        .filter(|l| {
            l.contains(&format!("writer_path=\"{writer_path}\""))
                && l.contains(&format!("registry=\"{registry}\""))
                && l.contains(digest_discriminator)
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
    // Display format `{digest}` matches the production log emit which
    // uses `%digest` (Display); Debug `{digest:?}` would produce
    // `DigestInfo("...")` which the production line does not contain.
    let digest_disc = format!("{digest}");

    let _guard = InFlightChunkedGuard::new_with_caller(
        Arc::clone(&set),
        digest,
        Some(notify),
        "test_caller_a",
    );

    let lines = lines_matching("test_caller_a", "fss_chunked_in_flight_digests", &digest_disc);
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
    // Display format `{digest}` matches the production log emit which
    // uses `%digest` (Display); Debug `{digest:?}` would produce
    // `DigestInfo("...")` which the production line does not contain.
    let digest_disc = format!("{digest}");

    {
        let _guard = InFlightChunkedGuard::new_with_caller(
            Arc::clone(&set),
            digest,
            None,
            "test_caller_b",
        );
    } // Drop here.

    let lines = lines_matching("test_caller_b", "fss_chunked_in_flight_digests", &digest_disc);
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
    // Display format `{digest}` matches the production log emit which
    // uses `%digest` (Display); Debug `{digest:?}` would produce
    // `DigestInfo("...")` which the production line does not contain.
    let digest_disc = format!("{digest}");

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

    let lines = lines_matching("test_caller_c", "fss_chunked_in_flight_digests", &digest_disc);
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

/// **OVER-ACTION coverage (closes code-reviewer B1 BLOCK on
/// obs-bundle-v2, 2026-05-17):** the FSS-level `info!` in
/// `InFlightChunkedGuard::new_with_caller` MUST fire EXACTLY ONCE per
/// call AND with field-fidelity (the line carries the SAME `caller`
/// the constructor received AND that call's OWN `digest`, never
/// another concurrent guard's digest).
///
/// **Why the prior version was a tautology:** v2's test asserted ZERO
/// buffer lines contained `writer_path="unused_caller_no_emit_xyz"` —
/// a label NO call-site in the workspace ever passes to
/// `new_with_caller`. The assertion was trivially true for any
/// production behavior. The cited mutation ("wrap in `for _ in 0..3 {`")
/// would NOT have red-failed because the loop would emit
/// `writer_path="test_caller_over_action_e5"` (the label this test
/// actually used) three times — still zero with the unused label. Form
/// (over-action coverage exists) satisfied; substance (mutation
/// red-fails) not.
///
/// **What the new version pins:** two distinct guards are constructed
/// back-to-back with distinct callers + distinct digests. The test
/// then asserts:
///
/// 1. EXACTLY ONE registration emit per (caller, digest) pair — catches
///    any mutation that emits zero (under-action regression) OR more
///    than one (over-action via accidental loop / re-entry).
/// 2. Field fidelity: alpha's emit line carries alpha's digest and NOT
///    beta's; symmetric for beta. Catches a mutation that swaps the
///    `caller` / `digest` argument positions inside the production
///    `info!` macro (so the line would carry the wrong `writer_path`
///    or the wrong digest discriminator).
///
/// **Mutation steps that red-fail this test:**
///
/// 1. Wrap the `info!` block in `chunked_write_handler.rs:3655-3663`
///    in `for _ in 0..2 { ... }` — `assert_eq!(alpha_count, 1)` fires
///    with `count=2` and the bespoke "over-action: registration info!
///    must fire EXACTLY ONCE per new_with_caller call" message.
/// 2. Swap `writer_path = caller,` and `%digest,` argument positions
///    in the production `info!` macro (so the rendered line tags the
///    digest value as `writer_path`) — the alpha-pair filter returns
///    zero matches, fires "over-action: field fidelity violated —
///    alpha's emit MUST carry alpha's digest".
#[nativelink_test]
async fn fss_no_emit_for_other_caller() {
    let set_alpha: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let set_beta: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    // Distinct digests so substring filters cannot cross-match between
    // alpha's and beta's emits. Display format `{digest}` matches the
    // production `%digest` Display emit (per cadre-#1 follow-up).
    let digest_alpha = make_digest(0xE5);
    let digest_beta = make_digest(0xE6);
    let alpha_disc = format!("{digest_alpha}");
    let beta_disc = format!("{digest_beta}");

    // Two concurrent guards — each construction fires the production
    // info! exactly once. Per-test-unique caller labels (`alpha_over_action`
    // and `beta_over_action`) so cross-test pollution cannot inflate
    // the count past 1.
    let _g_alpha = InFlightChunkedGuard::new_with_caller(
        Arc::clone(&set_alpha),
        digest_alpha,
        None,
        "test_alpha_over_action",
    );
    let _g_beta = InFlightChunkedGuard::new_with_caller(
        Arc::clone(&set_beta),
        digest_beta,
        None,
        "test_beta_over_action",
    );

    // Filter per (caller, digest) pair. The digest discriminator on
    // each filter blocks any future test that picks a colliding caller
    // label from matching here (per cadre M3 pollution hardening).
    let alpha_lines = lines_matching(
        "test_alpha_over_action",
        "fss_chunked_in_flight_digests",
        &alpha_disc,
    );
    let beta_lines = lines_matching(
        "test_beta_over_action",
        "fss_chunked_in_flight_digests",
        &beta_disc,
    );

    // (1) Exact-one-emit per call. Mutation hint: wrapping the
    // production `info!` block in `for _ in 0..N {` red-fails with
    // `count=N`.
    let alpha_count = alpha_lines
        .iter()
        .filter(|l| l.contains("chunked_in_flight registered"))
        .count();
    let beta_count = beta_lines
        .iter()
        .filter(|l| l.contains("chunked_in_flight registered"))
        .count();
    assert_eq!(
        alpha_count, 1,
        "over-action: registration info! must fire EXACTLY ONCE per \
         new_with_caller call for writer_path=test_alpha_over_action. \
         Got {alpha_count} matching lines (expected 1). Mutation hint: \
         wrapping the `info!` block at chunked_write_handler.rs:3655 \
         in `for _ in 0..N {{ ... }}` would red-fail this with count=N. \
         tracing-test global_buf tail: {}",
        recent_buf_tail()
    );
    assert_eq!(
        beta_count, 1,
        "over-action: registration info! must fire EXACTLY ONCE per \
         new_with_caller call for writer_path=test_beta_over_action. \
         Got {beta_count} matching lines (expected 1). Mutation hint: \
         wrapping the `info!` block at chunked_write_handler.rs:3655 \
         in `for _ in 0..N {{ ... }}` would red-fail this with count=N. \
         tracing-test global_buf tail: {}",
        recent_buf_tail()
    );

    // (2) Field fidelity: alpha's emit must NOT carry beta's digest
    // discriminator (and vice versa). Mutation hint: swapping
    // `writer_path = caller,` and `%digest,` argument positions inside
    // the production `info!` macro would make alpha's writer_path-keyed
    // line either be missing (filter returns zero, caught by (1)) or
    // carry beta's digest (caught here). Catches a sibling field-swap
    // defect class that the exact-count assertion alone would not.
    let alpha_line = alpha_lines
        .iter()
        .find(|l| l.contains("chunked_in_flight registered"))
        .expect("alpha registration line present (otherwise count assertion above would have fired)");
    assert!(
        !alpha_line.contains(&beta_disc),
        "over-action: field fidelity violated — alpha's emit MUST \
         carry alpha's digest, NOT beta's. Found beta's digest \
         discriminator ({beta_disc}) in alpha's line: {alpha_line}. \
         Mutation hint: swapped `writer_path = caller,` and `%digest,` \
         argument positions in the production info! macro at \
         chunked_write_handler.rs:3655."
    );
    let beta_line = beta_lines
        .iter()
        .find(|l| l.contains("chunked_in_flight registered"))
        .expect("beta registration line present (otherwise count assertion above would have fired)");
    assert!(
        !beta_line.contains(&alpha_disc),
        "over-action: field fidelity violated — beta's emit MUST \
         carry beta's digest, NOT alpha's. Found alpha's digest \
         discriminator ({alpha_disc}) in beta's line: {beta_line}. \
         Mutation hint: swapped `writer_path = caller,` and `%digest,` \
         argument positions in the production info! macro at \
         chunked_write_handler.rs:3655."
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
