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

/// SHARED CROSS-REPO KNOWN-ANSWER TEST for the `-incr` EXCLUSION (§2, FL-1383
/// Bazel-handoff). A rustc pipelined action declares FIVE `output_paths`, three
/// of which are `-incr` artifacts (`<label>-incr`, `<label>-incr-metadata`,
/// `<label>-incr-unused-inputs.txt`). Because `-` (0x2D) sorts before `.`
/// (0x2E), the bytewise-smallest RAW entry is `foo-incr` — so WITHOUT the
/// exclusion the key would land on the config-BLIND `-incr` dir, diverging from
/// the carrier and the KAT. `derive` MUST exclude every entry whose basename
/// contains `-incr` (catches all three) BEFORE choosing the smallest, so the key
/// lands on the `.rlib`.
///
/// Byte-verified with `b3sum`:
/// - `blake3("bazel-out/cfg/bin/pkg/libfoo-a1b2c3.rlib")` = [`KAT_INCR_RLIB_KEY`]
/// - `blake3("bazel-out/cfg/bin/pkg/foo-incr")`          = [`KAT_INCR_SEED_KEY`]
///
/// The NEGATIVE assertion (key != the `foo-incr` hash) is load-bearing: it proves
/// the EXCLUSION is doing the work, not merely the sort. Mutation: remove the
/// `-incr` filter in `derive` → the smallest becomes `foo-incr` → the key becomes
/// [`KAT_INCR_SEED_KEY`] and BOTH assertions here red-fail.
const KAT_INCR_OUTPUTS: &[&str] = &[
    "bazel-out/cfg/bin/pkg/libfoo-a1b2c3.rlib",
    "bazel-out/cfg/bin/pkg/libfoo-a1b2c3.rmeta",
    "bazel-out/cfg/bin/pkg/foo-incr",
    "bazel-out/cfg/bin/pkg/foo-incr-metadata",
    "bazel-out/cfg/bin/pkg/foo-incr-unused-inputs.txt",
];
/// `blake3("bazel-out/cfg/bin/pkg/libfoo-a1b2c3.rlib")` — the CORRECT key (the
/// `.rlib`, smallest AFTER the `-incr` exclusion).
const KAT_INCR_RLIB_KEY: &str =
    "4334001da81eeeb0c01c96c5871ac7c11b6e89d562395abd290ea4e73b302dfc";
/// `blake3("bazel-out/cfg/bin/pkg/foo-incr")` — the WRONG key the un-excluded
/// bytewise-min would produce; the negative assertion forbids it.
const KAT_INCR_SEED_KEY: &str =
    "c37da9bbab7592b03677363b984c845b824a681a1d6c3ec0353d0b8c13555356";

#[nativelink_test]
async fn targetkey_kat_excludes_incr_and_keys_on_rlib() -> Result<(), Error> {
    let outputs: Vec<String> = KAT_INCR_OUTPUTS.iter().map(|s| (*s).to_string()).collect();
    let tk = TargetKey::derive(&outputs)
        .expect("a 5-output rustc action must still derive a targetkey after -incr exclusion");
    assert_eq!(
        tk.primary_output(),
        "bazel-out/cfg/bin/pkg/libfoo-a1b2c3.rlib",
        "primary must be the .rlib (smallest after excluding every -incr basename), not foo-incr"
    );
    assert_eq!(
        tk.key(),
        KAT_INCR_RLIB_KEY,
        "targetkey must key on the .rlib once the -incr artifacts are excluded"
    );
    assert_ne!(
        tk.key(),
        KAT_INCR_SEED_KEY,
        "targetkey must NOT be blake3(foo-incr) — the exclusion, not just the sort, must drop -incr"
    );
    Ok(())
}

/// When EVERY output is an `-incr` artifact there is nothing left to key on after
/// the exclusion → `None` (cold, never keys on an `-incr` dir).
#[nativelink_test]
async fn targetkey_all_incr_outputs_yield_none() -> Result<(), Error> {
    let all_incr = paths(&[
        "bazel-out/cfg/bin/pkg/foo-incr",
        "bazel-out/cfg/bin/pkg/foo-incr-metadata",
        "bazel-out/cfg/bin/pkg/foo-incr-unused-inputs.txt",
    ]);
    assert!(
        TargetKey::derive(&all_incr).is_none(),
        "all-incr output_paths must yield no key (exclusion empties the set → None)"
    );
    Ok(())
}

/// The exclusion is BASENAME-scoped: a non-`-incr` output living under a
/// directory whose NAME contains `-incr` must still be eligible (only the last
/// `/`-segment is inspected).
#[nativelink_test]
async fn targetkey_incr_exclusion_is_basename_scoped() -> Result<(), Error> {
    let tk = TargetKey::derive(&paths(&[
        "bazel-out/cfg/bin/some-incr-pkg/libfoo.rlib",
    ]))
    .expect("a .rlib under an -incr-named directory must remain eligible");
    assert_eq!(
        tk.primary_output(),
        "bazel-out/cfg/bin/some-incr-pkg/libfoo.rlib",
        "only the basename is checked for -incr; an -incr directory segment must not exclude it"
    );
    Ok(())
}

/// EDGE CASE (§2, pair-a/red-team): a LEGITIMATE crate literally named `foo-incr`
/// emits real outputs whose BASENAME contains the `-incr` substring
/// (`libfoo-incr-<hash>.rlib` + `.rmeta`) — NOT the pipelined `-incr` seed dir.
/// The worker's `basename.contains("-incr")` exclusion drops EVERY such output, so
/// after the filter the candidate set is empty → `derive` returns `None` → the
/// action is not seeded → COLD build.
///
/// This is cold-not-wrong — but ONLY IF the client applies the BYTE-IDENTICAL
/// `contains("-incr")` predicate, so the client ALSO excludes these outputs and
/// derives the SAME (empty → no-seed) result. A client using `ends_with("-incr")`
/// / a regex / segment inspection would NOT exclude `libfoo-incr-<hash>.rlib`,
/// derive a NON-empty key on it, and diverge from the worker → the §11
/// `verify_against_command_outputs` mismatch fails LOUD with a hard
/// `FailedPrecondition` (not cold). This test pins the WORKER side of that
/// contract edge; the client predicate is pinned as exactly
/// `basename.contains("-incr")` in the Bazel-side handoff.
///
/// Mutation: remove the `-incr` exclusion filter in `derive` → the two
/// `-incr`-containing outputs survive → `derive` returns `Some` → the `is_none()`
/// assertion below red-fails.
#[nativelink_test]
async fn targetkey_crate_literally_named_incr_all_outputs_excluded_yields_none()
-> Result<(), Error> {
    // A crate named `foo-incr`: its rustc outputs are `libfoo-incr-<hash>.rlib`
    // and `.rmeta`, whose basenames both CONTAIN `-incr`.
    let outputs = paths(&[
        "bazel-out/cfg/bin/pkg/libfoo-incr-a1b2c3.rlib",
        "bazel-out/cfg/bin/pkg/libfoo-incr-a1b2c3.rmeta",
    ]);
    assert!(
        TargetKey::derive(&outputs).is_none(),
        "a crate literally named foo-incr — every output basename contains -incr — must be \
         FULLY excluded → None (cold, safe on the WORKER side); this is cold-not-wrong only \
         if the client applies the byte-identical contains(\"-incr\") predicate (an ends_with \
         client would derive a key here and diverge → hard FailedPrecondition at verify)"
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

// ---- FL-1383 T29/T30: the `.incrkey_*` carrier on link actions ---------------------------------
//
// A `RustcLink` action's declared outputs used to contain nothing config-dependent: the only
// non-`-incr` entry was the bare binary, and `--experimental_output_paths=strip` has already
// collapsed `bazel-out/<config>` to `bazel-out/cfg` by the time `Command.output_paths` reaches this
// crate. Every configuration of a rust_binary therefore derived ONE key and they took turns
// evicting each other's incremental session (the `targetkey_discriminates_build_config` property
// above is only reachable on UNSTRIPPED paths, which the FL fleet does not have).
//
// rules_rust now declares an extra output per link action — `.incrkey_<name>_<config_hash>`, named
// with the same `config_salt` that already names a `.rlib`. `derive` is UNCHANGED: `.` is 0x2E,
// below every ASCII alphanumeric, so the existing bytewise min picks it up. That is what lets the
// worker's `verify_against_command_outputs` agree with the client's carrier by construction —
// a disagreement there is `Code::Internal`, a hard build failure, not a cold build — so no repo had
// to deploy before any other.

/// The full `RustcLink` declared-output set as rules_rust emits it post-T29.
const KAT_LINK_OUTPUTS: &[&str] = &[
    "bazel-out/cfg/bin/pkg/.incrkey_foo_1272650966",
    "bazel-out/cfg/bin/pkg/foo",
    "bazel-out/cfg/bin/pkg/foo-incr",
    "bazel-out/cfg/bin/pkg/foo-incr-reuse",
    "bazel-out/cfg/bin/pkg/foo-incr-unused-inputs.txt",
];
/// `blake3("bazel-out/cfg/bin/pkg/.incrkey_foo_1272650966")`. BYTE-IDENTICAL to the vectors in
/// `incr_seed_index/src/targetkey.rs` and `RemoteIncrTargetKeyTest.java`. If these drift, fleet
/// builds go silently cold — never update one side alone.
const KAT_LINK_KEY: &str = "1e5379848f6b0f40dda6b5d526aa6ef8271b356689f197c8820ace3e9cfe2235";
/// `blake3("bazel-out/cfg/bin/pkg/foo")` — the WRONG key, the one the pre-T29 output set produced
/// for EVERY configuration of this target. The negative assertion forbids it.
const KAT_LINK_BARE_BINARY_KEY: &str =
    "012d3c3628532dbb41098522a4d5214ab9a2d657c3039e80264acb4650a1c090";
/// The same target, one configuration over: only the `config_salt` component of
/// `determine_output_hash` differs, and it is the ONLY byte-level difference in the whole set.
const KAT_LINK_PRIMARY_DBG: &str = "bazel-out/cfg/bin/pkg/.incrkey_foo_3122765277";
const KAT_LINK_KEY_DBG: &str = "a75c1bd9abbd0eaf7a58cc46cbc34f4551121f27fa353f550f0d0451c0d758b1";

#[nativelink_test]
async fn targetkey_kat_rustclink_selects_the_incrkey_carrier() -> Result<(), Error> {
    let outputs: Vec<String> = KAT_LINK_OUTPUTS.iter().map(|s| (*s).to_string()).collect();
    let tk = TargetKey::derive(&outputs).expect("a link action must derive a targetkey");
    assert_eq!(
        tk.primary_output(),
        "bazel-out/cfg/bin/pkg/.incrkey_foo_1272650966",
        "primary must be the config-bearing carrier, not the config-blind bare binary"
    );
    assert_eq!(tk.key(), KAT_LINK_KEY);
    assert_ne!(
        tk.key(),
        KAT_LINK_BARE_BINARY_KEY,
        "must NOT be blake3(the bare binary) — that is the pre-T29 config-blind key"
    );

    // The same set one configuration over resolves to a DIFFERENT pinned key. Pinning both, rather
    // than only asserting inequality, is what makes cross-repo drift show up as a golden-vector
    // failure instead of as a silently cold fleet.
    let dbg_outputs: Vec<String> = KAT_LINK_OUTPUTS
        .iter()
        .map(|s| {
            if s.contains(".incrkey_") {
                KAT_LINK_PRIMARY_DBG.to_string()
            } else {
                (*s).to_string()
            }
        })
        .collect();
    let dbg = TargetKey::derive(&dbg_outputs).expect("a link action must derive a targetkey");
    assert_eq!(dbg.primary_output(), KAT_LINK_PRIMARY_DBG);
    assert_eq!(dbg.key(), KAT_LINK_KEY_DBG);
    assert_ne!(
        tk.key(),
        dbg.key(),
        "two configurations of one rust_binary must not share a <FIXED_PREFIX>/<targetkey> slot"
    );
    Ok(())
}

/// ★ THE SORT IS THE WHOLE MECHANISM, SO ASSERT IT DIRECTLY. T29 relies on `.` (0x2E) sorting below
/// every ASCII alphanumeric (`0` is 0x30, `A` 0x41, `a` 0x61) — implicit magic a reviewer is right
/// to push on. This pins it against the real sibling family a rules_rust link action can declare on
/// any target OS, independently of the observed hashes.
///
/// Second property: the carrier must SURVIVE the `-incr` exclusion. If it tripped it, the carrier
/// would be dropped before the min and the key would fall straight back to the bare binary — the
/// original bug, with every implementation still agreeing and every test still green. rules_rust's
/// `_incrkey_marker_basename` makes that unreachable by emitting a basename containing no `-`.
#[nativelink_test]
async fn targetkey_incrkey_carrier_wins_the_bytewise_min() -> Result<(), Error> {
    let dir = "bazel-out/cfg/bin/pkg/";
    let carrier = format!("{dir}.incrkey_mybin_1272650966");
    for sibling in [
        "mybin",            // unix binary
        "mybin.exe",        // windows binary
        "mybin.pdb",        // msvc debug info
        "mybin.dSYM",       // darwin debug info
        "mybin.dll.lib",    // windows cdylib import library
        "mybin.rustc-output",
        "0mybin",           // adversarial: `0` (0x30) is the lowest alnum byte
        "Mybin",            // adversarial: `A`-range
    ] {
        let sibling = format!("{dir}{sibling}");
        assert!(
            carrier < sibling,
            "the carrier must sort BEFORE {sibling}, else the unchanged min rule keeps selecting \
             the config-blind output and the fix is silently inert"
        );
        let tk = TargetKey::derive(&paths(&[&sibling, &carrier])).expect("must derive");
        assert_eq!(tk.primary_output(), carrier, "min over {{carrier, {sibling}}}");
    }

    // The `-incr` exclusion must not eat the carrier. The hyphenated shape rejected during design
    // (for a rust_binary whose name begins `incr`) is the control that proves the filter is live.
    let hyphenated = format!("{dir}.incrkey-incremental-1272650966");
    let actual = format!("{dir}.incrkey_incremental_1272650966");
    assert!(
        TargetKey::derive(&paths(&[&hyphenated])).is_none(),
        "premise: the hyphenated shape WOULD be excluded as an -incr artifact"
    );
    let tk = TargetKey::derive(&paths(&[
        &actual,
        &format!("{dir}incremental"),
        &format!("{dir}incremental-incr"),
    ]))
    .expect("must derive");
    assert_eq!(tk.primary_output(), actual);
    Ok(())
}
