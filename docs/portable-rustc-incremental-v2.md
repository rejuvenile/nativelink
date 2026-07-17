# Portable rustc incremental via byte-identical execroot — DESIGN v2

**Status:** v2 — re-cadre 5/5 returned **NOT SIGNED OFF (2026-07-17)**; distsys WITHHELD the wiped-dir sign-off.
See `.claude/reviews/design-portable-rustc-incremental-v2/DECISION.md`. **The §6 seed-TRANSPORT needs a Rethink
+ a gating experiment before v3.** Resolved by the cadre: (1) `-incr` does NOT churn the `.rlib` key —
`unused_inputs_list` already handles that (my "invent a side-input" framing was wrong); BUT (2) the seed CAPTURE
is broken for remote-won builds under the LIVE `--remote_download_outputs=toplevel` (`.*-incr` download regex
removed, FL-681) → `RustcIncrSeed` (no-remote, local `prev_incr_dir`) reads an empty seed → cold; self-limiting.
(3) cross-machine `.rlib` cache-hit non-regression is UNPROVEN → gate on a two-machine remote-cache experiment.
Everything else (§4/§5/§7/§8-with-real-authority/§9/§10-OptionB/§11) is sound + landable; both physics experiments
passed. **Do NOT implement until §6 seed-capture is redesigned + the gating experiment is run.**
**SUPERSEDED by `portable-rustc-incremental-v3.md`** (2026-07-17) — v3 replaces the broken seed-capture with a
fleet-shared `targetkey→-incr` store (Option 3: CAS content + AC-index-by-hash(targetkey)), reframes §10 as
`-incr`-residency-in-the-existing-affinity-scorer, and relaxes §7 to worker-local-`-incr`-as-store-cache. Read v3.
Supersedes v1. **Architectural sign-off still REQUIRED** (wiped-dir invariant). **Date:** 2026-07-17.

## 0. What changed from v1 (the cadre + experiments)

- **All four collapse-risks are now RESOLVED by experiment** (numbers in §2), so the design is worth building.
- **v1's two load-bearing errors are fixed here:** (F1) the local vs remote cwd were *different strings* →
  v2 pins ONE canonical cwd on both branches (§4); (seed) "drop `no-remote` / `-incr` as a declared output+input"
  was wrong (it churns the `.rlib` action key + trips a Chesterton's Fence) → v2 makes `-incr` a **hidden
  CAS side-input that does NOT enter the `.rlib` action key** (§6).
- New hard requirements surfaced: full-empty wipe (§7), disk eviction composing with FL-688 (§8),
  action-to-action isolation on the predictable path (§9), lease-as-RAII (§5), three-state observability (§11),
  FIXED_PREFIX co-located on the execroot volume or copy-fallback (§9, EXDEV).

## 1. Goal (unchanged)

Portable rustc **incremental** reuse under Bazel `--spawn_strategy=dynamic` — a rustc action races a local
mac execroot and a remote NativeLink macOS worker; today the remote branch is always cold. Make a CAS-shared
`-incr` seed re-materialize at a **byte-identical absolute path** on whichever branch runs, so rustc reuse
fires. Must work on BOTH branches (remote-only was rejected by the operator).

## 2. Validated foundation (all measured — do NOT re-litigate)

| Property | Experiment | Result |
|----------|-----------|--------|
| Reuse is bound to the absolute path, and only the path | Step-0 raw-rustc | warm at same path, cold at diff path |
| Hardlinked inputs reuse == real-copy inputs | §11 (Linux) + exp#2 (ci-mac-3 M4/APFS) | 0.001s/0.000s warm vs ~1.4s/0.39s cold |
| **Cross-machine + cross-MODEL transfer** | exp#1 ci-mac-3 **M4** → ci-mac-1 **M2 Ultra** | **0.001s vs 0.665s cold (~600×)**; incr session name + codegen-reuse markers matched |
| macOS `getcwd` returns the literal path (no firmlink resolution) | exp#2 | pwd == pwd -P == literal for `/Users/user`, `/Users/Shared`, `/private/var/tmp` (all Data-volume dev 16777233) |
| rustc realpaths cwd/source/incr → **no symlink shortcut** | symlink sub-exp | fixed symlink → cold; real physical paths REQUIRED |
| target-cpu is a FIXED baseline (not `native`) → cross-model safe | FL `.bazelrc:580-581` | `-Ctarget-cpu=apple-a14` (M1 baseline, M1–M4 uniform); build_std inherits it |

**Two residual platform constraints (measured, addressable):**
- **EXDEV:** `/Volumes/CrowAgent` (dev 16777240) is a *different* APFS volume from the Data volume (16777233).
  Hardlinks require FIXED_PREFIX and the execroot on the **same volume** → §9.
- The cross-machine result used an identical toolchain at an identical sysroot path. [CORRECTED per re-cadre:
  this is NOT "content-addressed, by construction." The rustc sysroot flag is execroot-RELATIVE
  (`toolchain.bzl:647`); the identical ABSOLUTE path comes from §10 hardlinking the toolchain under the pinned
  cwd PLUS a HOST-PROVISIONING invariant — every worker uniformly has `/Users/user/.rustup` (`.bazelrc:512-513`;
  FL-721 salts host rustup content into the action key). exp#1 passed because both macs had that path. §11 MUST
  assert the resolved sysroot absolute path is byte-identical on both branches at startup; the local branch must
  NOT reach sysroot via an `output_base` symlink (rustc realpaths it → cold).]

## 3. Path-key scheme

`targetkey = a ≥128-bit collision-resistant hash of the REAPI `Command.output_paths` primary output`
(`bazel-out/<config>/bin/<pkg>/lib<crate>-<meta>.rlib`). Auditor-VERIFIED: `output_paths` is sorted for
consistent Action hashing (identical across machines); `determine_output_hash` (rules_rust `utils.bzl:254-260`)
is crate-root-PATH + label + `config_salt`-derived → **stable across source edits, config-discriminating**.
Store the **full output-path string** beside the dir; an owner verifies `stored_path == my_output_path` before
treating a dir as "mine" → a hash collision degrades to a detected cold-fallback, never a silent shared dir
(security F3).

## 4. THE canonical execution path (v1 F1 fix — CONVERGENT distsys ∧ red-team)

**Both branches chdir rustc into the IDENTICAL real directory string:**

```
<FIXED_PREFIX>/<targetkey>[/<Command.working_directory>]
```

- `Command.working_directory` is symmetric (same `Command` on both branches; empty for typical rust actions —
  verify per action). The v1 bug was the worker's extra `/work` segment (`work_directory = action_dir/work`,
  `running_actions_manager.rs:4800`, `:5263`). **v2: for allowlisted actions the worker materializes the input
  root directly at `<FIXED_PREFIX>/<targetkey>` and chdirs there — NO `/work` segment**; the local branch's
  process_wrapper chdirs to the same string. State the exact resolved `getcwd` in the impl PR and assert it
  identical on both (a startup/if-mismatch counter, §11).
- Real directory + real `chdir` (NOT symlink — symlink-exp proved rustc realpaths it to cold).

## 5. Concurrency isolation — lease (macOS has no namespaces)

Different targets → different real dirs → isolated. Same-`targetkey` concurrent on ONE machine
(local worktree-pool slots; remote cross-invocation) → a machine-local lease:
- **Owner** runs at `<FIXED_PREFIX>/<targetkey>` (warm); a **contender** runs at an isolated fallback
  (`operation_id`) path and produces a valid **cold** result (no serialization).
- **RAII drop-guard (distsys F5):** the lease MUST release on future-drop, scoped through
  `[acquire → materialize → rustc → finalize → RELOCATE outputs → release]` — mirror `CleanupGuard`
  (`running_actions_manager.rs:8697`) / `DirectoryCachePinGuard`. A cancelled/panicked owner must NOT leak
  permanent ownership (that would silently demote every future same-target build to cold — dark). Local `flock`
  auto-releases on death; its scope must also cover relocation. Bounded `HashMap<targetkey, Lease>` — leaf
  `parking_lot::Mutex`, `// CAPPED AT N`, released in `cleanup_action`/`do_cleanup`, no hold across `.await`.

Dynamic local+remote of the SAME action = different machines/disks → no collision (only the string must match).

## 6. Seed model — `-incr` as a HIDDEN CAS side-input (v1 seed fix — CONVERGENT ×4)

**Do NOT "drop `no-remote`" and do NOT make `-incr` a normal declared output+input.** Both are wrong:
- red-team P1.2: a declared `-incr` input churns the `.rlib` action key every build (its session names are
  random → digest changes) → converts free `.rlib` cache HITS into executions (regression).
- auditor/code-reviewer: RustcIncrSeed's `no-cache/no-remote/no-sandbox` is load-bearing (undeclared local read
  of `prev_incr_dir`, `rustc.bzl:2227`; no-@flagfile → worker-validator rejects it).

**v2 mechanism:** the prior `-incr` tree is a **content-addressed CAS artifact transported as a side input that
does NOT enter the `.rlib` action's cache key** (analogous to today's `no-sandbox` seed, but CAS-shareable
across machines). Correctness boundary = **rustc's own per-query input-fingerprint re-validation** (a
stale/wrong/torn seed → rustc rejects it → COLD, never wrong). Therefore:
- **FORBID on-disk seed persistence (security F1):** the exec dir is fully cleared and the `-incr` seed is
  re-materialized from the CAS side-input EVERY build; nothing `-incr` persists on disk between builds; only the
  path STRING is reused. A "keep the warm `-incr` on disk to skip re-materialization" optimization is FORBIDDEN
  and must be rejected in review (it would silently make failures wrong-not-cold).
- This dissolves v1 F3 (contender/declared-output tension): the contender's cold `-incr` is simply not uploaded
  as the shared seed (it's a side artifact, not a declared output), so a contender win doesn't error and doesn't
  poison the next seed.

## 7. Full content-empty wipe (v1 F2 fix)

Empty the execroot to a bare directory each build (contents removed, inode/path preserved), then re-materialize
inputs + the CAS seed — byte-identical to today's pristine-per-action behavior EXCEPT the path string is stable.
`make_action_directory` (`:8602`) + `inner_prepare_action` (`:5076`) use `create_dir` (fails EEXIST on reuse) →
change to ensure-exists-then-empty. `direct_use_mode` must be **OFF by construction** for allowlisted actions
(distsys F6: its symlink resolves `getcwd` to a digest-named path → dark cold + shared-cache corruption); the
NORMAL directory_cache hardlink mode (`get_or_create`) is already the layout we want.

## 8. Disk eviction (v1 F4 fix — distsys strongest)

The persistent `<FIXED_PREFIX>/<targetkey>` set + their `-incr` caches need a **bounded byte budget + LRU** that
**subtracts from (composes with)** the FL-688 pin budget and the DirectoryCache budget on the same volume — three
uncoordinated consumers otherwise → disk exhaustion → worker failure. An LRU-evicted targetkey simply cold-starts
next build (safe). Add the eviction accounting to the shared disk-budget authority, not a fourth independent pool.

## 9. Security + provisioning (v1 security/EXDEV fixes)

- **FIXED_PREFIX owned by + writable only by the worker/build uid (0755, NOT world-writable 1777).** `/Users/Shared`
  is 1777 — do NOT use it. Build actions run untrusted code as the same uid and `targetkey` is derivable → a
  world-writable predictable path lets an action pre-stage a poisoned seed / symlink-swap a peer action's execroot.
- **O_EXCL dir creation** (preserve `create_dir` not `create_dir_all`) + **O_NOFOLLOW / reject symlinked
  components** on input materialization and output relocation (extends the existing `:8536-8545` symlink-safety).
- **Atomic per-process output writes** (tmp-then-rename): a lease bypass then yields last-writer-wins (deterministic
  build → equivalent) or a torn file that fails downstream LOUDLY — never a silent-wrong cached `.rlib`. Document
  that the lease protects throughput/`-incr` churn; correctness under bypass = atomic writes + rustc fingerprinting.
- **EXDEV (exp#2):** FIXED_PREFIX MUST be on the **same APFS volume as the execroot** (hardlink requirement). On CI
  macs whose execroot is on `/Volumes/CrowAgent`, put FIXED_PREFIX there (`/Volumes/CrowAgent/nl-incr`); startup
  MUST assert `link()` across FIXED_PREFIX↔execroot succeeds (not merely that both dirs exist) — else **copy-fallback**
  (§11-A proved copy warms identically), correct but slower, with a counter so silent cross-volume drift is visible.
- **Cleanup guard (red-team A1.3/A1.4):** the `:8506-8523` `remove_dir_all` containment guard must still confine
  deletes to a provably-**per-worker** subtree of FIXED_PREFIX, never the fleet-shared prefix root (a targetkey
  derivation bug must not be able to wipe a peer build's live execroot). Resolve the fleet-identical-prefix vs
  per-worker-containment tension: FIXED_PREFIX is fleet-identical, but each worker's deletable subtree is namespaced.
- **Document the trusted-ACTION assumption** as explicitly as the macOS one (a future untrusted-exec mode must
  re-isolate: per-uid prefix or restored namespaces).

## 10. Change sites

**NativeLink (remote):**
- `targetkey` derivation needs the `Command` (output_paths), which `create_action_info:8609` does NOT hold —
  it's fetched later in `inner_prepare_action:4970` (code-reviewer #1). → add a small Command CAS fetch (usually
  locally cached) OR defer dir creation until after it. Effort > M.
- `make_action_directory:8601` → build `<FIXED_PREFIX>/<targetkey>` for allowlisted actions (else operation_id).
- chdir `:5263-5266` → the canonical §4 path (no `/work`). Cleanup guard `:8506-8523` per §9. Wipe per §7.
- Per-worker RAII lease (§5). Scheduler `targetkey`-affinity INSIDE the saturation/viability envelope
  (`api_worker_scheduler.rs:3781/:3913/:3945` — targetkey is the CORRECT stable signal; Tier-1's
  `input_root_digest` changes every edit) — profile `do_try_match`. Disk eviction (§8).

**Bazel / rules_rust (local):**
- `process_wrapper` (`util/process_wrapper/{options,main}.rs`) — create `<FIXED_PREFIX>/<targetkey>`, **hardlink**
  the full input tree + the CAS seed at matching `bazel-out`-relative paths, chdir there, exec rustc, **relocate**
  outputs (atomic rename) back to the declared execroot paths (all primitives exist: hardlink via `--copy-seed`,
  atomic rename `canonicalize_*`, execroot `options.rs:405`). Effort **L–XL**, gated by the Bazel-output-verify
  question (does post-action digest/mtime verification tolerate relocate-from-fixed-dir? — open, test early).
- Seed: `-incr` as the CAS side-input of §6 (NOT dropping `no-remote` on RustcIncrSeed). Effort L.
- `-Cincremental` arg unchanged (execroot-relative → resolves against the pinned cwd). No target-cpu change
  (already fixed `apple-a14`).

## 11. Observability (v1 F8 fix — three-state, per-branch)

Single warm counter is a dark-counter (this project's repeated trap). Emit, labeled by branch (local/remote):
`incr_seed_materialized_total` (seed present as input) + `incr_reuse_fired_total` (rustc actually reused) +
`incr_seed_present_but_cold_total` (the F1/path-drift/EXDEV-copy diagnostic — seed_present climbing while reuse
flat) + `incr_lease_contended_total` + `incr_lease_stale_reclaimed_total`. Consider the `-Ztime-passes`
LLVM_passes delta as ground truth. Startup: assert FIXED_PREFIX exists, is worker-uid-owned 0755, and
`link()`-succeeds to the execroot volume.

## 12. Effort + rollout

**XL, dynamic from the start** (remote-only rejected). Order:
1. rules_rust `-incr` as a CAS side-input (§6). **L.**
2. NativeLink stable-path materialization + chdir + RAII lease + eviction, allowlist-scoped. **M–L.**
3. process_wrapper per-target-dir + hardlink + chdir + relocate + local lease. **L–XL** (test the output-verify
   question first).
4. Provision FIXED_PREFIX per-machine on the execroot volume, worker-uid-owned; wire the §11 counters + assertions.
5. Enable for ONE high-value crate (a large asan/tsan crate, apple-a14); measure the three-state counters + warm-hit
   on BOTH branches under dynamic.
6. Scheduler targetkey-affinity; widen the allowlist.

## 13. Re-cadre checklist (what the design-only v2 cadre must confirm)
1. §4 canonical path — is the resolved `getcwd` provably identical on both branches for a real allowlisted action
   (Command.working_directory empty)?
2. §6 — does the `-incr` CAS side-input mechanism genuinely NOT enter the `.rlib` action key (no cache-hit
   regression)? Show the rules_rust wiring.
3. §8 — is the disk budget's composition with FL-688 + DirectoryCache concrete and bounded?
4. §10 process_wrapper output-relocation vs Bazel output verification (the L–XL gating unknown) — test.
5. §9 EXDEV copy-fallback + per-worker delete-containment under a fleet-identical prefix — airtight?
