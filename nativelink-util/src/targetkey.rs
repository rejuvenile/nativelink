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

//! FL-1383 portable rustc-incremental — the `targetkey` derivation (design §3).
//!
//! `docs/portable-rustc-incremental-v4.md` §3 defines a portable, edit-invariant
//! key identifying a build *target* across source edits and across machines, used
//! to fetch/publish a fleet-shared `-incr` seed. This module is the pure,
//! side-effect-free derivation; the ingestion plumbing (§10) lives in
//! `nativelink-service::execution_server`, and the worker execution-path pinning
//! (§4/§7) is a later chunk that will call the same helper.

use blake3::Hash;
use serde::{Deserialize, Serialize};

/// Action Platform property name carrying the client-derived `targetkey` (§10).
/// The Bazel client attaches this for allowlisted rustc actions; its PRESENCE is
/// the client's allowlist decision. The server reads it straight from
/// `Action.platform` at ingestion — no Command fetch.
pub const CARRIER_TARGETKEY_PROPERTY: &str = "nl_incr_targetkey";

/// Action Platform property name carrying the exact primary-output string the
/// client hashed to produce [`CARRIER_TARGETKEY_PROPERTY`] (§10). Config-STRIPPED
/// under `--experimental_output_paths=strip` (no `bazel-out/<config>` segment).
pub const CARRIER_PRIMARY_OUTPUT_PROPERTY: &str = "nl_incr_primary_output";

/// A portable identity key for an action's primary output (design §3).
///
/// Derived from the action's REAPI `Command.output_paths`: the paths are
/// **sorted** (so the key is independent of the order Bazel emitted them), the
/// lexicographically smallest ("primary") path is hashed with blake3 (fixed,
/// independent of the action's digest function, ≥128-bit per §3), and the full
/// primary-output string is retained beside the
/// hash so a blake3 **collision** (equal `key`, differing `primary_output`) is
/// detectable at the seed-fetch site and falls back to a cold build.
///
/// Design properties (§3), each covered by a test in
/// `nativelink-util/tests/targetkey_test.rs`:
/// - **edit-invariant**: `output_paths` do not change on source edits of the
///   same target, so edit N's `-incr` seed can seed edit N+1.
/// - **config-discriminating (best-effort)**: config-discrimination comes from
///   the config-salt hash embedded in a `.rlib` filename, NOT from a
///   `bazel-out/<config>` path segment. Under the FL build's
///   `--experimental_output_paths=strip` + `supports-path-mapping`, Bazel's
///   `StrippingPathMapper` REPLACES the config mnemonic with the literal `cfg`
///   (it does NOT drop it), so `Command.output_paths` (and thus the carrier's
///   `primary_output`) are of the form `bazel-out/cfg/bin/...` — no per-config
///   mnemonic segment. When the bytewise-smallest output is a `<label>-incr`
///   tree dir (which carries no config-salt hash), the key is CONFIG-SHARED
///   across configs; a cross-config seed is then cold-not-wrong (safe, within
///   the §6.4 correctness floor). Allowlist prefixes MUST use the `bazel-out/cfg/`
///   form to match.
/// - **deterministic + order-independent**: the sort makes the key independent
///   of the input order; identical input always yields an identical key.
///
/// The key is deliberately **source-version-blind** (§3): it does not
/// incorporate any source content, which is what makes it edit-invariant. The
/// last-writer-wins consequence for concurrent multi-version builds is
/// documented in design §6.2.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TargetKey {
    /// Lowercase-hex blake3 digest of `primary_output`. ≥128-bit per §3.
    key: String,
    /// The sorted-first `Command.output_paths` entry, retained so that a
    /// blake3 collision (same `key`, different path) is detectable → the
    /// seed-fetch site treats it as a cold-fallback (§3).
    primary_output: String,
}

impl TargetKey {
    /// Derive the [`TargetKey`] from an action's `Command.output_paths`.
    ///
    /// Returns `None` when `output_paths` is empty — an action with no declared
    /// outputs has nothing to key on and is not eligible for portable-incr
    /// seeding.
    ///
    /// The derivation sorts a borrowed view of `output_paths` (the caller's
    /// slice is not mutated) and hashes the lexicographically smallest entry;
    /// the sort is what makes the result independent of the order Bazel listed
    /// the outputs.
    #[must_use]
    pub fn derive(output_paths: &[String]) -> Option<Self> {
        if output_paths.is_empty() {
            return None;
        }
        let mut sorted: Vec<&str> = output_paths.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        let primary_output = sorted[0].to_string();
        let key = Self::hash_primary_output(&primary_output);
        Some(Self {
            key,
            primary_output,
        })
    }

    /// Construct a [`TargetKey`] from the two client-supplied carrier strings
    /// (§10): `nl_incr_targetkey` and `nl_incr_primary_output`, read verbatim
    /// from the Action's Platform properties.
    ///
    /// As a CHEAP integrity guard — a short-string blake3 hash, NO CAS fetch —
    /// this recomputes `blake3(primary_output)` and returns `None` if it does
    /// NOT equal the client-supplied `key`. A mismatch means client/contract
    /// drift (the client's key algorithm diverged, or the two properties were
    /// mispaired); it is treated as no-targetkey → the action falls back to a
    /// cold build (safe: cold-not-wrong, never seeds against a wrong key).
    ///
    /// This is the same reference algorithm as [`Self::derive`]; the KAT in
    /// `nativelink-util/tests/targetkey_test.rs` locks both byte-identical to
    /// the Bazel client and the FL `incr_seed_index` tool.
    #[must_use]
    pub fn from_carrier(key: String, primary_output: String) -> Option<Self> {
        if Self::hash_primary_output(&primary_output) != key {
            return None;
        }
        Some(Self {
            key,
            primary_output,
        })
    }

    /// The blake3 hex key. Stable across source edits, config-discriminating,
    /// deterministic (see the type docs).
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The full primary-output path retained beside the key. The seed-fetch
    /// site compares this against the current action's primary output to detect
    /// a blake3 collision (same `key`, different output) and fall back to cold.
    ///
    /// TODO(#FL-1383): the seed-fetch / execution-path chunk consumes this for
    /// collision detection and pinned-path materialization.
    #[must_use]
    pub fn primary_output(&self) -> &str {
        &self.primary_output
    }

    fn hash_primary_output(primary_output: &str) -> String {
        let hash: Hash = blake3::hash(primary_output.as_bytes());
        hash.to_hex().to_string()
    }
}
