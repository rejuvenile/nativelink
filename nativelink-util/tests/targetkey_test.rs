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

//! FL-1383 §3 `targetkey` derivation contract tests. Each test pins one of the
//! four properties the design requires of the key.

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_util::targetkey::TargetKey;
use pretty_assertions::assert_eq;

fn paths(entries: &[&str]) -> Vec<String> {
    entries.iter().map(|s| (*s).to_string()).collect()
}

/// Property: deterministic AND order-independent (§3). The sort inside
/// `derive` makes the key independent of the order Bazel listed the outputs;
/// identical input always yields an identical key. This is the property the
/// `sort_unstable` mutation must break.
#[nativelink_test]
async fn targetkey_is_order_independent_and_deterministic() -> Result<(), Error> {
    let forward = TargetKey::derive(&paths(&[
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib",
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib.d",
    ]))
    .expect("derive must yield a targetkey from non-empty output_paths");

    // Same set of outputs, emitted in the reverse order.
    let reversed = TargetKey::derive(&paths(&[
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib.d",
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib",
    ]))
    .expect("derive must yield a targetkey from non-empty output_paths");

    assert_eq!(
        forward.key(),
        reversed.key(),
        "targetkey must be independent of output_paths order (is the sort present?)"
    );

    // Determinism: deriving the identical input a second time is stable.
    let again = TargetKey::derive(&paths(&[
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib",
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib.d",
    ]))
    .expect("derive must yield a targetkey from non-empty output_paths");
    assert_eq!(
        forward.key(),
        again.key(),
        "targetkey must be deterministic for identical output_paths"
    );
    Ok(())
}

/// Property: edit-invariant (§3). `output_paths` do not change when only the
/// source of the target changes, so two builds of the same target (edit N and
/// edit N+1) derive the SAME key — this is what lets edit N's `-incr` seed
/// edit N+1. `derive` takes only output paths, so no source content can leak
/// into the key.
#[nativelink_test]
async fn targetkey_is_edit_invariant() -> Result<(), Error> {
    let outputs = paths(&["bazel-out/darwin_arm64-fastbuild/bin/pkg/libcommon_constants.rlib"]);

    // "edit N" and "edit N+1" of the same target: identical output_paths,
    // notionally different source content (which `derive` never sees).
    let edit_n = TargetKey::derive(&outputs).expect("derive must yield a targetkey");
    let edit_n_plus_1 = TargetKey::derive(&outputs).expect("derive must yield a targetkey");

    assert_eq!(
        edit_n.key(),
        edit_n_plus_1.key(),
        "targetkey must be edit-invariant: same output_paths => same key across source edits"
    );
    Ok(())
}

/// Property: config-discriminating (§3). The output path embeds the
/// `bazel-out/<config>` segment, so the SAME label built under a different
/// build configuration produces a different output path and thus a different
/// key (a warm seed from one config must not be offered to another).
#[nativelink_test]
async fn targetkey_discriminates_build_config() -> Result<(), Error> {
    let fastbuild = TargetKey::derive(&paths(&[
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib",
    ]))
    .expect("derive must yield a targetkey");
    let opt = TargetKey::derive(&paths(&["bazel-out/darwin_arm64-opt/bin/pkg/libfoo.rlib"]))
        .expect("derive must yield a targetkey");

    assert_ne!(
        fastbuild.key(),
        opt.key(),
        "different build config (bazel-out/<config>) must yield a different targetkey"
    );
    Ok(())
}

/// Property: collision-detection scaffolding (§3). The full primary-output path
/// is retained (the sorted-first entry) so that a caller holding a seed keyed
/// under `key()` can compare `primary_output()` against the current action's
/// primary output; a mismatch under an equal `key` is a blake3 collision and
/// must be treated as cold-fallback.
#[nativelink_test]
async fn targetkey_retains_primary_output_for_collision_detection() -> Result<(), Error> {
    let key = TargetKey::derive(&paths(&[
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib.d",
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib",
    ]))
    .expect("derive must yield a targetkey");

    assert_eq!(
        key.primary_output(),
        "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib",
        "primary output (sorted-first) must be retained beside the key for collision detection"
    );

    // The retained path is what the hash is over: a struct whose primary_output
    // differs but whose key matches would be a detectable collision.
    let other = TargetKey::derive(&paths(&["bazel-out/darwin_arm64-fastbuild/bin/pkg/libbar.rlib"]))
        .expect("derive must yield a targetkey");
    assert_ne!(
        key.primary_output(),
        other.primary_output(),
        "distinct primary outputs must be distinguishable for collision detection"
    );
    Ok(())
}

/// An action with no declared outputs has nothing to key on and is not eligible
/// for portable-incr seeding.
#[nativelink_test]
async fn targetkey_none_when_no_output_paths() -> Result<(), Error> {
    assert!(
        TargetKey::derive(&[]).is_none(),
        "empty output_paths must yield no targetkey"
    );
    Ok(())
}

/// An ILLUSTRATIVE cross-repo known-answer hashing sample: the string the shared
/// KAT hashes to lock the blake3 algorithm byte-identical across repos. It is a
/// hashing fixture only — NOT the config-stripped form the FL build actually
/// emits (a real stripped path is `bazel-out/cfg/bin/...`, with the config
/// mnemonic replaced by the literal `cfg`; this sample keeps the mnemonic purely
/// to exercise the hash over a representative path). Only the (string, key) pair
/// is load-bearing.
const KAT_PRIMARY_OUTPUT: &str = "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib";
/// The byte-verified blake3 hex of [`KAT_PRIMARY_OUTPUT`]. Shared across repos:
/// the Bazel client, the FL `incr_seed_index` tool, and this server MUST all
/// produce this exact value or the fleet-shared seed key diverges.
const KAT_KEY: &str = "a17b9c22c639c19f2951ad4d0cb28df41dbd33113bbe7ead51ac0dd577998567";

/// SHARED CROSS-REPO KNOWN-ANSWER TEST (§3). Locks our `derive` byte-identical
/// to the Bazel client + the FL `incr_seed_index` tool. If this value drifts,
/// server and client key the SAME target under DIFFERENT keys and no seed is
/// ever shared — a silent portable-incr outage. Mutation on the hash algorithm
/// (e.g. swapping blake3 for another) must red-fail here.
#[nativelink_test]
async fn targetkey_kat_locks_blake3_byte_identical_across_repos() -> Result<(), Error> {
    assert_eq!(
        TargetKey::derive(&[KAT_PRIMARY_OUTPUT.to_string()])
            .expect("derive must yield a targetkey from non-empty output_paths")
            .key(),
        KAT_KEY,
        "targetkey blake3 KAT drifted — server key no longer matches the Bazel client / incr_seed_index"
    );
    Ok(())
}

/// `from_carrier` accepts a matching (key, primary_output) pair: the recomputed
/// `blake3(primary_output)` equals the client-supplied key, so the carrier is
/// trusted verbatim (no CAS fetch). Mutation: return `None` unconditionally in
/// `from_carrier` — this must red-fail.
#[nativelink_test]
async fn targetkey_from_carrier_accepts_matching_pair() -> Result<(), Error> {
    let tk = TargetKey::from_carrier(KAT_KEY.to_string(), KAT_PRIMARY_OUTPUT.to_string())
        .expect("from_carrier must accept a key that matches blake3(primary_output)");
    assert_eq!(
        tk.key(),
        KAT_KEY,
        "from_carrier must retain the client-supplied key verbatim"
    );
    assert_eq!(
        tk.primary_output(),
        KAT_PRIMARY_OUTPUT,
        "from_carrier must retain the client-supplied primary_output verbatim"
    );
    Ok(())
}

/// `from_carrier` REJECTS a mismatched key: if the client-supplied key does not
/// equal `blake3(primary_output)`, that is client/contract drift and must fall
/// back to cold (`None`), never seed against a wrong key. Mutation: drop the
/// `!= key` guard in `from_carrier` — this must red-fail.
#[nativelink_test]
async fn targetkey_from_carrier_rejects_mismatched_key() -> Result<(), Error> {
    // A key that is NOT blake3(KAT_PRIMARY_OUTPUT).
    let wrong_key = "0".repeat(64);
    assert!(
        TargetKey::from_carrier(wrong_key, KAT_PRIMARY_OUTPUT.to_string()).is_none(),
        "from_carrier must reject a key that does not match blake3(primary_output) → cold fallback"
    );
    Ok(())
}
