# FL-1383 — NativeLink→Bazel handoff: the naming/derivation contract (pin BEFORE any canary)

The NativeLink worker side is complete and inert (`main` @ `6e8bd253`: seed fetch, publish-on-success, §12 metrics, §8 eviction, all Tier-3-reviewed). Before the flag can be flipped on even one crate, the following contract points MUST be confirmed with the rules_rust (chunk-4) side. A mismatch here is **cold-not-wrong** (never a bad `.rlib`) but produces **≈0 reuse that is DARK** — `incr_reuse_fired` is 0 until chunk-4 populates it, so a broken contract hides for weeks. These are the exact strings/derivations the worker code uses today.

## 1. targetkey derivation (KAT-locked, must agree byte-for-byte)
`targetkey = blake3_hex(lexicographically-smallest entry of Command.output_paths)`.
- Worker: `nativelink-util/src/targetkey.rs::TargetKey::derive` (sorts `output_paths`, hashes `sorted[0]`).
- Client: `RemoteIncrTargetKey.java`; FL tool: `incr_seed_index`.
- KAT (all three byte-verified): `blake3("bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib") = a17b9c22c639c19f2951ad4d0cb28df41dbd33113bbe7ead51ac0dd577998567`. (Illustrative sample — the REAL path is config-STRIPPED, see §4.)

## 2. ⚠️ CRITICAL — the `-incr` dir must NOT change which output is "smallest"
`-` is `0x2D`, `.` is `0x2E`, so **`<stem>-incr` sorts BEFORE `<stem>.rlib`**. If the `-incr` directory is declared in the same `Command.output_paths` the targetkey derives from, then `derive` picks `<stem>-incr` as primary → the key is `blake3(<stem>-incr)`, NOT `blake3(<stem>.rlib)` → it diverges from the KAT AND from the carrier, and the worker's §11 `verify_against_command_outputs` REJECTS the action (cold). 
**REQUIREMENT:** rules_rust must ensure the `-incr` tree is NOT an `output_paths` entry that participates in targetkey derivation — either it isn't in `output_paths` at all (a hidden/side output), or the shared derivation explicitly excludes `-incr`-suffixed entries. Confirm which, on all three sides, so the key stays on the `.rlib`.

## 3. `-incr` materialization path (where the worker puts the fetched seed)
The worker fetches the seed Tree to **`<execroot>/<stem>-incr`** — a TOP-LEVEL execroot child, where `<stem>` = the primary output's filename stem (`libfoo.rlib` → `libfoo-incr`). Code: `running_actions_manager.rs::seed_dest_dir` (derives the stem from `TargetKey::primary_output()`). The §7 wipe preserves top-level `*-incr` children; the chunk-2 tests pin `libfoo.rlib ↔ libfoo-incr`.
**REQUIREMENT:** rules_rust must produce/consume its `-incr` at EXACTLY `<execroot>/<primary-output-stem>-incr` so rustc's incremental reuse actually fires on the materialized tree. If rules_rust names it by crate name rather than primary-output stem, or nests it, reuse is dark.

## 4. Carrier + allowlist (already implemented server-side)
- Action Platform properties on allowlisted actions: `nl_incr_targetkey` (64-hex) + `nl_incr_primary_output` (the config-STRIPPED primary-output string). Server reads at ingestion (no Command fetch), checks `blake3(primary)==targetkey` else cold, and STRIPS both props before the scheduler sees them. Declare both `nl_incr_*` as `ignore` in `supported_platform_properties` (deploy delta).
- Path-mapping: FL build runs `--experimental_output_paths=strip` + `supports-path-mapping`; `StrippingPathMapper` replaces the config mnemonic with literal `cfg` → primary is `bazel-out/cfg/bin/…`. **Allowlist prefix on all three sides MUST be `bazel-out/cfg/bin/third_party/rust/apple_a14/`.**

## 5. Content + index (split of responsibility)
- `-incr` CONTENT: a normal declared output TREE → lands in CAS via the regular action-result upload (instance `main`). The worker's publish reads its `tree_digest` from `ActionResult.output_directories` (gated on `exit==0`).
- INDEX: worker writes `update_oneshot(hash("fl-incr-seed-index:v1:"++targetkey), ActionResult{output_directories[0]={path: primary_output, tree_digest: <-incr tree>}})` to instance `incr_seed_index` (behind CompletenessCheckingStore). rules_rust does NOT write the index.
- `incr_reuse_fired`: the rustc-side counter — chunk-4 populates it; the worker registers it visible-at-0.

## Confirm-then-canary checklist
- [ ] §2 — `-incr` excluded from targetkey-deriving `output_paths` (all 3 sides), key stays on `.rlib`.
- [ ] §3 — rules_rust `-incr` dir == `<execroot>/<primary-stem>-incr`, top-level.
- [ ] §4 — allowlist prefix `bazel-out/cfg/bin/third_party/rust/apple_a14/` on all sides; carrier attached.
- [ ] §5 — `-incr` declared as an output tree so it lands in CAS; rustc-side `incr_reuse_fired` wired.
- [ ] Config deltas applied (instance `incr_seed_index`, `portable_incr_seed_index_store`, `nl_incr_*: ignore`).
