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

//! `#fl1732-backfill-mislabel-latch` — unit contract for the digest-function
//! PROVER.
//!
//! The backfill latch exists because the worker labels a `UploadMissingBlobs`
//! upload from the AMBIENT process default (`grpc_store.rs:3823`) on a code
//! path that carries no ambient information. Neither the server nor the worker
//! has a RECORDED digest function for a backfilled blob — but the worker holds
//! the blob's BYTES, and bytes + declared digest DETERMINE the function: the
//! function is exactly the one whose hash of those bytes reproduces the digest.
//!
//! This file pins the determination primitive. Live proof that the production
//! blobs behave this way (worker `192.168.100.222`, 2026-08-14): blob
//! `befe5b05f50f1c87f19d7fc66552d9db0616bfbc6de2c577014243992d53b1b4-145`
//! hashes to that digest under SHA-256, and to
//! `dde9b045d395c0af8f195ced41a6d030ec2ee307506e295fcbdb60cc05689a35` under
//! BLAKE3 — which is byte-for-byte the value the server's rejection message
//! reported ("but digest hash was dde9b045…"). The bytes are intact; only the
//! LABEL was wrong.

use nativelink_macro::nativelink_test;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestFuncProver, DigestHasher, DigestHasherFunc, PROVABLE_DIGEST_FUNCS, prove_digest_func,
};

/// Hash `bytes` with `func` and return the resulting digest.
fn digest_of(func: DigestHasherFunc, bytes: &[u8]) -> DigestInfo {
    let mut hasher = func.hasher();
    hasher.update(bytes);
    hasher.finalize_digest()
}

#[nativelink_test]
async fn prover_identifies_sha256_keyed_blob() {
    let bytes = b"a SHA-256-keyed action Command proto, as uploaded by a sha256 client";
    let digest = digest_of(DigestHasherFunc::Sha256, bytes);

    assert_eq!(
        prove_digest_func(&digest, bytes),
        Some(DigestHasherFunc::Sha256),
        "the prover MUST identify SHA-256 for a blob whose declared digest is \
         its SHA-256 hash — this is the whole information source the \
         #fl1732-backfill-mislabel-latch fix stands on. Returning anything else \
         means the worker relabels the blob wrong and the server rejects it \
         forever (101-106 rejections/day/digest observed on the live fleet)"
    );
}

#[nativelink_test]
async fn prover_identifies_blake3_keyed_blob() {
    let bytes = b"a BLAKE3-keyed blob, the fleet-common case";
    let digest = digest_of(DigestHasherFunc::Blake3, bytes);

    assert_eq!(
        prove_digest_func(&digest, bytes),
        Some(DigestHasherFunc::Blake3),
        "the prover MUST identify BLAKE3 for a blake3-keyed blob — this is >99% \
         of fleet traffic and it MUST be unaffected by the latch fix"
    );
}

#[nativelink_test]
async fn prover_returns_none_when_no_candidate_reproduces_the_digest() {
    // A digest that is NOT the hash of these bytes under any supported
    // function — i.e. the worker's local copy is corrupt/truncated. There is
    // nothing to prove, and inventing a label would be a lie.
    let bytes = b"local copy of a blob";
    let digest = DigestInfo::new([0x42_u8; 32], bytes.len() as u64);

    assert_eq!(
        prove_digest_func(&digest, bytes),
        None,
        "the prover MUST refuse to name a function when NO candidate reproduces \
         the declared digest. That case is a corrupt local copy, not a mislabel \
         — the caller must fall back to today's behavior and surface it, never \
         stamp a fabricated label"
    );
}

#[nativelink_test]
async fn prover_rejects_a_size_mismatch_even_when_bytes_are_a_prefix() {
    // Declared size disagrees with the bytes actually held. `finalize_digest`
    // carries the hashed byte count, so a truncated local copy cannot prove
    // any function.
    let bytes = b"truncated";
    let real = digest_of(DigestHasherFunc::Sha256, bytes);
    let digest = DigestInfo::new(**real.packed_hash(), bytes.len() as u64 + 1);

    assert_eq!(
        prove_digest_func(&digest, bytes),
        None,
        "a declared size that disagrees with the bytes held MUST NOT prove a \
         function — the size is part of the blob's identity and a truncated \
         local copy is a data-loss signal, not a relabel candidate"
    );
}

#[nativelink_test]
async fn streaming_prover_matches_the_one_shot_form_across_chunk_boundaries() {
    // The >1 MiB backfill branch feeds the prover chunk-by-chunk from the
    // local store; chunking MUST NOT change the answer.
    let bytes: Vec<u8> = (0..70_000_u32).map(|i| (i % 251) as u8).collect();
    let digest = digest_of(DigestHasherFunc::Sha256, &bytes);

    let mut prover = DigestFuncProver::new();
    for chunk in bytes.chunks(4_096) {
        prover.update(chunk);
    }

    assert_eq!(
        prover.prove(&digest),
        Some(DigestHasherFunc::Sha256),
        "the streaming prover MUST agree with the one-shot form regardless of \
         how the bytes are chunked — the >1 MiB backfill branch never has the \
         whole blob in memory, so a chunk-boundary bug there would leave large \
         SHA-256 blobs latched while small ones healed"
    );
}

#[nativelink_test]
async fn prover_handles_the_empty_blob() {
    let digest = digest_of(DigestHasherFunc::Sha256, b"");
    assert_eq!(
        prove_digest_func(&digest, b""),
        Some(DigestHasherFunc::Sha256),
        "a zero-length blob MUST still prove its function — the empty digest is \
         a real REAPI blob (empty Directory / empty file) and it appears in \
         every input root"
    );
}

// -----------------------------------------------------------------------------
// COVERAGE BINDING: every digest function a CLIENT can reach must be provable.
// -----------------------------------------------------------------------------

/// `PROVABLE_DIGEST_FUNCS` is a hand-maintained array. The compile-time
/// exhaustiveness guard in `digest_hasher.rs` fires on a new
/// `DigestHasherFunc` variant, but a developer can satisfy it by pointing the
/// new arm at an existing slot, and Rust cannot enumerate enum variants to
/// catch that.
///
/// This closes it from the other side, and needs no enumeration: sweep the
/// REAPI `DigestFunction` enum — the wire surface — through
/// `DigestHasherFunc::try_from`, and require every value the server ACCEPTS
/// to be a proving candidate. A new variant is only reachable by a client
/// once it is added to those conversions, which is the same edit that makes
/// it worth adding at all, and this reds the moment it is.
///
/// The direction is deliberate. A function ACCEPTED but not PROVABLE re-opens
/// the FL-1786 latch for it: blobs keyed with it that arrive under any other
/// label are rejected forever with no candidate able to rescue them. A
/// function provable but not accepted is harmless — a candidate that never
/// matches.
#[nativelink_test]
async fn every_wire_reachable_digest_function_is_provable() {
    // The REAPI enum's defined values are 1..=9 (`remote_execution.proto`
    // `DigestFunction.Value`); sweep well past that so a newly-generated
    // variant is included without editing this bound. 0 is "not set" and
    // resolves to the process default, which is covered by construction.
    let mut accepted = Vec::new();
    for raw in 1..64_i32 {
        let Ok(func) = DigestHasherFunc::try_from(raw) else {
            continue;
        };
        accepted.push((raw, func));
        assert!(
            PROVABLE_DIGEST_FUNCS.contains(&func),
            "#fl1786 COVERAGE HOLE: DigestHasherFunc::try_from({raw}) accepts {func:?}, so a \
             client can key blobs with it and label writes with it, but \
             PROVABLE_DIGEST_FUNCS = {PROVABLE_DIGEST_FUNCS:?} does not contain it. Any blob \
             keyed {func:?} that reaches the server under a different label is then rejected \
             FOREVER with no candidate able to rescue it — the FL-1786 latch, re-opened for a \
             new function. Add {func:?} to PROVABLE_DIGEST_FUNCS (proving is an identity check \
             against the blob's own declared digest, so an extra candidate can only fail to \
             match, never mis-accept) and size the added inline hash per its doc-comment"
        );
    }
    assert!(
        !accepted.is_empty(),
        "#fl1786: the sweep accepted NO digest function, so the assertion above ran zero times \
         and proved nothing. Either DigestHasherFunc::try_from<i32> stopped accepting the REAPI \
         values or the sweep range no longer covers them"
    );

    // Same sweep through the STRING conversion, which is the surface the
    // ByteStream resource name's `{digest_function}` segment lands on.
    for name in ["SHA256", "BLAKE3", "sha256", "blake3"] {
        let func = DigestHasherFunc::try_from(name)
            .unwrap_or_else(|err| panic!("#fl1786: {name:?} must convert: {err:?}"));
        assert!(
            PROVABLE_DIGEST_FUNCS.contains(&func),
            "#fl1786 COVERAGE HOLE: the resource-name segment {name:?} resolves to {func:?}, \
             which is not a proving candidate — a client can label a write with it and no \
             candidate can rescue a mislabel"
        );
    }
}
