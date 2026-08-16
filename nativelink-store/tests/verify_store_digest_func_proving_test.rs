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

//! `#fl1786-server-side-digest-function-proving`: the `VerifyStore` site.
//!
//! Covers every CAS write that does NOT take the chunked RPC — in the
//! production composition that is `< CHUNK_SIZE` (1 MiB) and
//! `> MAX_CHUNKED_BLOB_SIZE` (256 MiB) worker uploads plus every Bazel-client
//! ByteStream write (`buildcache-native.json5:190-191` wires the CAS chain
//! through `VerifyStore { verify_size: true, verify_hash: true }`).
//!
//! Two distinct ways the label reaching `VerifyStore` can be WRONG, both
//! exercised here:
//!
//! 1. **Mislabelled** (FL-1786): the resource name carries an explicit
//!    digest function, but the writer had no ambient context and stamped its
//!    process default. `bytestream_server.rs:4298` parses it and
//!    `VerifyStore::update` re-hashes with it.
//! 2. **Omitted segment** (FL-1732): the resource name has NO
//!    digest-function segment at all (`use_legacy_resource_names`, or any
//!    legacy client). `bytestream_server.rs:4298-4303` `map_or_else`s to
//!    `default_digest_hasher_func()` — the server's own guess.
//!
//! Both reduce to: the server re-hashes the bytes it is holding with a
//! function nobody proved. The bytes are right there, so prove instead.
//!
//! The process-global default is pinned to BLAKE3 (the production value) so
//! case 2 reproduces the live shape rather than a test-only one.

use std::sync::Arc;

use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::{
    DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD, VerifyStore, digest_func_proven_log_decision,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasherFunc, default_digest_hasher_func, make_ctx_for_hash_func,
    set_default_digest_hasher_func,
};
use nativelink_util::store_trait::{Store, StoreLike};
use opentelemetry::context::FutureExt;
use pretty_assertions::assert_eq;
use tracing::{Instrument, info_span};

const VALUE: &str = "123";
/// `sha256("123")`.
const SHA256_OF_VALUE: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
/// `blake3("123")`.
const BLAKE3_OF_VALUE: &str = "b3d4f8803f7e24b8f389b072e75477cdbcfbe074080fb5e500e53e26e054158e";
/// `sha256("12")` — the digest of DIFFERENT bytes. No advertised function
/// reproduces it from `VALUE`, so this is the genuinely-corrupt fixture.
const SHA256_OF_OTHER: &str = "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918";

/// Pin the process-global digest function to BLAKE3 — the PRODUCTION value
/// (`buildcache-native.json5:748`). The `assert` is the load-bearing half: the
/// global is a `OnceCell` and a silent already-set would run the
/// omitted-segment test against SHA-256, which is not the live shape.
fn pin_production_default_blake3() {
    let _ = set_default_digest_hasher_func(DigestHasherFunc::Blake3);
    assert_eq!(
        default_digest_hasher_func(),
        DigestHasherFunc::Blake3,
        "#fl1786: this test binary must run with the PRODUCTION process-global digest function \
         (BLAKE3). Reading SHA-256 here means something set the OnceCell first and the \
         omitted-segment case below would not reproduce the live composition"
    );
}

fn verify_store_pair() -> (Arc<VerifyStore>, Arc<MemoryStore>) {
    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            // Production values: buildcache-native.json5:190-191.
            verify_size: true,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );
    (store, inner_store)
}

// -----------------------------------------------------------------------------
// 1. FL-1786 MISLABELLED: sha256-keyed blob, resource name says blake3.
// -----------------------------------------------------------------------------

#[nativelink_test]
async fn sha256_keyed_blob_labelled_blake3_is_accepted() -> Result<(), Error> {
    pin_production_default_blake3();
    let (store, inner_store) = verify_store_pair();
    let digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;

    let result = store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("update_oneshot"))
        // The wrong label — exactly what `GrpcStore::update` stamps into the
        // resource name when the backfill path has no ambient context
        // (`grpc_store.rs:3823` falls back to the process default).
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await;

    assert_eq!(
        result,
        Ok(()),
        "#fl1786: a blob whose bytes genuinely hash to its declared digest under SHA-256 must be \
         ACCEPTED even though the write was LABELLED blake3. The label is a claim; the digest is \
         a checkable one and the server is holding the bytes. Rejecting it is the backfill \
         latch — the server stays missing the blob and re-solicits it forever"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE.len() as u64)),
        "#fl1786: the proven blob must actually reach the inner store, not merely return Ok"
    );
    assert_eq!(
        store.digest_func_proven_count(),
        1,
        "#fl1786: the accept must be attributable to the PROVING mechanism — a green result \
         above with this counter at 0 would mean the blob landed for some other reason"
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// 2. FL-1732 OMITTED SEGMENT: no label at all, server falls back to its default.
// -----------------------------------------------------------------------------

#[nativelink_test]
async fn sha256_keyed_blob_with_no_label_is_accepted() -> Result<(), Error> {
    pin_production_default_blake3();
    let (store, inner_store) = verify_store_pair();
    let digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;

    // NO `with_context` — this is the legacy / omitted-digest-function
    // resource name. `digest_hasher_func_from_context()` falls back to the
    // process default, which production sets to BLAKE3.
    let result = store.update_oneshot(digest, VALUE.into()).await;

    assert_eq!(
        result,
        Ok(()),
        "#fl1732 OMITTED SEGMENT: when the resource name carries no digest-function segment the \
         server falls back to its own process default (BLAKE3 in production). That default is a \
         GUESS about someone else's blob. A SHA-256-keyed blob must still be accepted"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE.len() as u64)),
        "#fl1732: the proven blob must actually reach the inner store"
    );
    assert_eq!(
        store.digest_func_proven_count(),
        1,
        "#fl1732: the accept must be attributable to the PROVING mechanism"
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// 3. FAIL-CLOSED: corrupt blob still rejected, with the UNCHANGED message.
// -----------------------------------------------------------------------------

#[nativelink_test]
async fn corrupt_blob_is_still_rejected_fail_closed() -> Result<(), Error> {
    pin_production_default_blake3();
    let (store, inner_store) = verify_store_pair();
    // Declares the sha256 of "12" but ships "123": neither SHA-256 nor
    // BLAKE3 of the shipped bytes reproduces it.
    let digest = DigestInfo::try_new(SHA256_OF_OTHER, VALUE.len() as u64)?;

    let result = store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("update_oneshot"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await;

    // `expect_err`, not `unwrap_err`: when proving fails open this assertion
    // is the one that has to say WHY it matters, and a bare
    // `unwrap_err()`-on-`Ok` panic says only "called Result::unwrap_err() on
    // an Ok value: ()".
    let err = result
        .expect_err(
            "#fl1786 FAIL-CLOSED: a blob that reproduces its declared digest under NO advertised \
             digest function is CORRUPT, not mislabelled, and must be REJECTED. This write was \
             ACCEPTED, which means proving degraded into 'if no candidate matches, allow' — a \
             fail-open bypass of the CAS integrity contract that lets arbitrary bytes land under \
             an attacker-chosen digest",
        )
        .to_string();
    // The message must be byte-identical to the pre-proving one, reporting
    // the LABELLED function's computed hash — operators, dashboards and
    // `zero_copy_write_corruption_test` all key on this exact string.
    let expected_err = format!(
        "Hashes do not match, got: {SHA256_OF_OTHER} but digest hash was {BLAKE3_OF_VALUE}"
    );
    assert!(
        err.contains(&expected_err),
        "#fl1786 FAIL-CLOSED: a blob that reproduces its declared digest under NO advertised \
         digest function is CORRUPT, not mislabelled, and must be rejected with the UNCHANGED \
         '{expected_err}' message. Proving must never degrade to 'if no candidate matches, \
         allow'. Got: {err:?}"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(None),
        "#fl1786 FAIL-CLOSED: the rejected blob must not be in the inner store"
    );
    assert_eq!(
        store.hash_verification_failure_count(),
        1,
        "#fl1786 FAIL-CLOSED: a genuine corruption must still tick hash_verification_failures — \
         that counter is the operator's data-integrity alarm and proving must not silence it"
    );
    assert_eq!(
        store.digest_func_proven_count(),
        0,
        "#fl1786 FAIL-CLOSED: nothing was proven, so the proven counter must stay at 0 — a \
         corrupt blob must never be recorded as a successful relabel"
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// 4. FAST PATH: correctly-labelled blob is not counted as proven.
// -----------------------------------------------------------------------------

#[nativelink_test]
async fn correctly_labelled_blake3_blob_is_not_counted_as_proven() -> Result<(), Error> {
    pin_production_default_blake3();
    let (store, inner_store) = verify_store_pair();
    let digest = DigestInfo::try_new(BLAKE3_OF_VALUE, VALUE.len() as u64)?;

    let result = store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("update_oneshot"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await;

    assert_eq!(
        result,
        Ok(()),
        "#fl1786: a correctly-labelled blob must still be accepted"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE.len() as u64)),
        "#fl1786: the correctly-labelled blob must reach the inner store"
    );
    assert_eq!(
        store.digest_func_proven_count(),
        0,
        "#fl1786 FAST PATH: a blob that matches under its OWN label must not be recorded as \
         proven. This counter is the operator's 'how much mislabelled traffic is the fleet \
         emitting' signal; if it ticks for correctly-labelled writes it reads as 100% \
         mislabelled and is useless"
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// 5. ATTRIBUTION: a SIZE fault is not a HASH fault (pair-a F6 / pair-b F5).
// -----------------------------------------------------------------------------

/// With `verify_hash: true` and `verify_size: false` — a reachable
/// configuration, both fields are `#[serde(default)]` and `verify_hash`'s doc
/// names no coupling — the earlier size checks are skipped, so a blob whose
/// BYTES hash correctly but whose DECLARED `size_bytes` is wrong reaches the
/// candidate match. Comparing the whole `DigestInfo` folded that into the
/// hash verdict and emitted `Hashes do not match, got: X but digest hash was
/// X` (identical hashes) while ticking the data-integrity alarm.
///
/// The write must still be REJECTED — the strictness is correct and
/// fail-closed — but as a SIZE fault.
#[nativelink_test]
async fn size_mismatch_under_verify_size_false_is_reported_as_a_size_fault() -> Result<(), Error> {
    pin_production_default_blake3();
    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );
    // Correct BLAKE3 hash of "123", but the declared size is 4, not 3.
    let digest = DigestInfo::try_new(BLAKE3_OF_VALUE, VALUE.len() as u64 + 1)?;

    let err = store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("update_oneshot"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect_err(
            "#fl1786 FAIL-CLOSED: a blob whose declared size disagrees with the bytes received \
             must be REJECTED even when verify_size is off — size is half of a blob's identity, \
             and admitting it would key the CAS entry under a digest the content does not have",
        )
        .to_string();

    assert!(
        err.contains("Expected size 4 but got size 3 on insert"),
        "#fl1786: a SIZE fault must be reported with the SIZE message. The pre-fix message was \
         'Hashes do not match, got: {BLAKE3_OF_VALUE} but digest hash was {BLAKE3_OF_VALUE}' — \
         the SAME hash twice — which points an operator at corruption for what is a \
         size-declaration bug in the producer. Got: {err:?}"
    );
    assert!(
        !err.contains("Hashes do not match"),
        "#fl1786: the bytes hashed CORRECTLY under the labelled function, so the error must not \
         claim a hash mismatch. Got: {err:?}"
    );
    assert_eq!(
        store.hash_verification_failure_count(),
        0,
        "#fl1786: a size fault must NOT tick hash_verification_failures. That counter is the \
         operator's data-integrity alarm; charging a producer's wrong size_bytes to it is how a \
         corruption page gets raised for a client bug"
    );
    assert_eq!(
        store.digest_func_proven_count(),
        0,
        "#fl1786: nothing was proven — the labelled function was correct all along"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(None),
        "#fl1786 FAIL-CLOSED: the rejected blob must not be in the inner store"
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// 6. LOG VOLUME: proving SUCCEEDING is what makes the warn hot.
// -----------------------------------------------------------------------------

/// Pin the sampler directly: first occurrence plus every 64th.
#[nativelink_test]
async fn proven_write_log_is_sampled_first_then_every_64th() {
    assert_eq!(
        DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD, 64,
        "#fl1786: the sampling period must stay 64 — it matches \
         V2_LIFECYCLE_LOG_SAMPLE_PERIOD (34a18cda) so the two proving sites emit at the same \
         rate, and the number is cited in the field doc-comment"
    );
    assert!(
        digest_func_proven_log_decision(1),
        "#fl1786: the FIRST proven write must always emit — an operator seeing zero mislabelled \
         traffic then a silent rescue would have no entry point into the incident"
    );
    for count in 2..DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD {
        assert!(
            !digest_func_proven_log_decision(count),
            "#fl1786: occurrence {count} must be SUPPRESSED. Pre-fix this warn was emitted \
             unconditionally, once per proven write. The observed latch was 29 digests at \
             ~100/day, but proving makes a mislabelling client WORK, so it keeps running and a \
             single Bazel build uploads tens of thousands of blobs — and warn! is not compiled \
             out in release (release_max_level_info)"
        );
    }
    assert!(
        digest_func_proven_log_decision(DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD),
        "#fl1786: occurrence {DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD} must emit — a sampler that \
         only ever emits once would make a sustained mislabelling client invisible in the log"
    );
}

/// And pin that the CALL SITE honours the sampler, against the real store:
/// 65 proven writes must emit exactly 2 lines (occurrences 1 and 64) while
/// the counter reaches 65. Asserting only the pure function would leave an
/// unconditional `warn!` at the call site perfectly green.
// NOTE: no explicit `#[tracing_test::traced_test]` — `#[nativelink_test]`
// already applies it (`nativelink-macro/src/lib.rs`), and applying it twice
// nests two capture buffers so `logs_assert` reads an EMPTY one. Observed:
// this assertion read 0 emitted lines with the warn firing normally. Sibling
// files that stack both attributes only assert log ABSENCE, so they pass
// vacuously and never surfaced it.
#[nativelink_test]
async fn sixty_five_proven_writes_emit_two_warns_and_count_sixty_five() -> Result<(), Error> {
    pin_production_default_blake3();
    let (store, _inner_store) = verify_store_pair();

    for i in 0..65_u32 {
        // Distinct bytes per iteration so each write is a fresh blob rather
        // than a dedup no-op, each keyed with SHA-256 while labelled BLAKE3.
        let bytes = format!("mislabelled blob {i}");
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        nativelink_util::digest_hasher::DigestHasher::update(&mut hasher, bytes.as_bytes());
        let digest = nativelink_util::digest_hasher::DigestHasher::finalize_digest(&mut hasher);
        store
            .update_oneshot(digest, bytes.clone().into())
            .instrument(info_span!("proven_write"))
            .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
            .await
            .expect("#fl1786: every mislabelled-but-intact write must be accepted");
    }

    assert_eq!(
        store.digest_func_proven_count(),
        65,
        "#fl1786: the COUNTER must carry the true rate — it is what the sampled log gives up \
         precision for, and the /metrics contract this change rests on"
    );
    logs_assert(|lines: &[&str]| {
        let emitted = lines
            .iter()
            .filter(|l| l.contains("digest function was PROVEN"))
            .count();
        if emitted == 2 {
            Ok(())
        } else {
            Err(format!(
                "#fl1786: 65 proven writes must emit exactly 2 warns (occurrences 1 and 64); \
                 emitted {emitted}. 65 means the call site ignores \
                 digest_func_proven_log_decision and is back to one un-rate-limited warn per \
                 proven write — the shape that backs up the nonblocking log writer under a \
                 build burst, on a server with a standing write-burst-stall incident. 0 means \
                 the signal is gone entirely"
            ))
        }
    });
    Ok(())
}
