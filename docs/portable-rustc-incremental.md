# Portable rustc incremental via byte-identical execroot (dynamic-execution)

**Status:** DRAFT for Tier-3 design cadre. **Date:** 2026-07-17.
**Scope:** two tracks — Bazel/rules_rust (local dynamic branch) + NativeLink (remote worker).
**Architectural sign-off REQUIRED before implementation** (changes the wiped-dir execution-buffer
invariant — see `.claude/rules/architectural-changes.md`).

## 1. Goal

Make rustc **incremental** compilation reuse work across a remote-execution transport, under Bazel
**`--spawn_strategy=dynamic`**. Today a dynamically-scheduled rustc action races a local mac execroot
against a remote NativeLink worker; the remote branch is always cold because rustc's incremental cache
does not transfer. We want the remote branch (and the local branch) to warm-start from a shared `-incr`
seed so large incremental-heavy crates (asan/tsan compile/link) stop paying full codegen every build.

## 2. Validated premise (Step 0 — do NOT re-litigate)

A raw-rustc `-Ztime-passes` test proved: rustc incremental reuse is bound to the **absolute execution
path, and only the path**. An `-incr` cache transported through a full wipe/re-materialize cycle (fresh
input files, restored cache) **reuses codegen when re-materialized at the same absolute path**
(LLVM_passes 0.005 s ≈ warm baseline 0.002 s) and **goes cold at a different path** (0.154 s).
`--remap-path-prefix` / `-Zremap-cwd-prefix` rewrite emitted strings only; they do **not** substitute
for a real matching path (confirmed: `incr_canon.rs` canonicalizes session-dir *names* for CAS
byte-dedup and does not touch absolute paths inside `dep-graph.bin`/`query-cache.bin`).

**Corollary the design is built on:** if every machine that executes a given rustc action presents rustc
a byte-identical **absolute `getcwd`** (physically resolved) with the same input layout, the `-incr` seed
transfers and reuse fires — on the local branch AND the remote branch.

## 3. The binding constraint added this revision: dynamic ⇒ local + remote (remote-only is a NO-GO)

Under dynamic execution the **same** action runs on both branches simultaneously; either may win. So the
byte-identical-path property must hold on **both** the local mac execroot and the remote macOS worker.
A remote-only variant (make only the worker incremental, disable the local branch) was considered and
**rejected by the operator** — it breaks dynamic-execution compatibility. Everything below must work for
the local branch too.

## 4. Platform reality that shapes the design: the fleet is macOS

Both the invoking build machines (local dynamic branch, worktree-pool macs) and the 10 execution workers
are **macOS (Apple Silicon)**. macOS has **no mount namespaces**. Therefore the reclient-style
"one fixed root `/b/f/w`, isolate concurrent actions with a per-action namespace view" model is **not
available** — the single `cas_server.rs:1428` mount-namespace hook is Linux-gated and unused on this fleet.
`/b/f/w` is a reclient/RBE-server convention, not a Bazel client flag (Bazel 9.2.0 / FL fork 9.1.0 expose
no fixed-input-root flag). This rules out the shared-single-root scheme and forces the **per-target real
fixed path** scheme below, which needs no namespaces.

## 5. Resolved path-key scheme (constraints 1–3)

**Execroot = `<FIXED_PREFIX>/<targetkey>`, a REAL directory per target, identical string on both branches.**

- **`targetkey = hash(primary output path)`**, extracted from the REAPI `Command.output_paths`
  (`bazel-out/<config>/bin/<pkg>/lib<crate>-<meta>.rlib`). This key uniquely satisfies:
  - **(1) identical across machines** — Bazel computes the same output path locally and remotely; it is
    already in the `Command`.
  - **(2) stable across incremental edits** — a source edit does NOT change the output path (same
    target+config → same `libX.rlib`). This is exactly why the action content hash / input-root digest is
    the WRONG key (changes every edit) and the output path is RIGHT.
  - **(3) config-discriminating** — different configs live under different `bazel-out/<config-hash>/`, so
    `opt`/`dbg`/asan get distinct `targetkey`s automatically.
- **`FIXED_PREFIX`** = a chosen user-writable absolute path provisioned **identically** on every invoking
  mac and every worker (e.g. `/Users/Shared/nl-incr` or a provisioned `/opt/nl-incr`). NOT `/b/f/w`
  (root-level, SIP friction on macOS). **Provisioning this identical prefix fleet-wide + on build macs is
  a hard prerequisite** (deploy-infra item).
- **Why REAL dirs, not symlink/bind-mount:** a plain `chdir` into a real directory makes `getcwd(2)`
  return the literal path — no resolution ambiguity. This **moots** the one experiment the Bazel-track
  investigator flagged (does rustc key on `getcwd`-resolved vs the arg string?), because there is no
  symlink to resolve. A real `chdir` into a real fixed dir is precisely the "same absolute path" the
  premise already proved.

Different targets → different real dirs → naturally isolated. Same-target concurrency → §7.

## 6. Local track (Bazel / vendored rules_rust / process_wrapper) — change sites

This is the invasive half; dynamic-compatibility requires it. Bazel has ONE execroot per workspace shared
by all actions (cwd = execroot), so a per-target cwd cannot come from Bazel's execroot — it must be done
**inside the action**, in `process_wrapper` (which already knows the execroot, does the `--copy-seed`
warm-start, and munges outputs).

- **`util/process_wrapper` (`options.rs`/`main.rs`):** before exec'ing rustc, (a) create the real dir
  `<FIXED_PREFIX>/<targetkey>`, (b) **hardlink** the action's inputs + the `-incr` seed tree into it at
  their `bazel-out`-relative paths (hardlink, NOT symlink, so rustc sees real files at the fixed path with
  no path-resolution divergence vs the remote's real files; requires execroot + FIXED_PREFIX on one
  volume), (c) `chdir` there, (d) exec rustc, (e) **relocate outputs** (the `.rlib`/`.rmeta` + the `-incr`
  tree) back to the declared execroot paths so Bazel finds them. **Effort L–XL.**
- **`rust/private/rustc.bzl` seed (`:2214-2246`, `--copy-seed` `:2513-2519`):** today `RustcIncrSeed` is
  `no-cache/no-remote/no-sandbox`, bin_dir-relative, a **local-only** warm-start. Change: make the `-incr`
  tree a **normal declared cacheable tree OUTPUT + INPUT** (drop `no-remote`) so Bazel uploads it to CAS
  after the winning branch and materializes it as an input to the next build on **both** branches. This is
  what lets a remote-won build seed the next local build and vice-versa. **Effort L.**
- **`-Cincremental` arg (`rustc.bzl:1501-1502`):** already execroot-relative → resolves against cwd →
  physical `<FIXED_PREFIX>/<targetkey>/bazel-out/.../X-incr`. No change to the arg; it inherits the pinned
  cwd. **S.**
- **Local same-target concurrency (worktree-pool):** two worktree-pool slots on one mac building the same
  target both want `<FIXED_PREFIX>/<targetkey>` → collision → machine-local lease (§7), a `flock` on
  `<FIXED_PREFIX>/<targetkey>.lock` inside process_wrapper. **M.**

## 7. Remote track (NativeLink) — change sites

- **`make_action_directory` (`running_actions_manager.rs:8601`):** today
  `format!("{}/{}", root_action_directory, operation_id)` (per-execution → wrong for all 3 constraints).
  Change: for allowlisted rustc actions, build the path from `targetkey` (derive in `create_action_info`
  `:8609+`, which holds the `execute_request`) rooted at `FIXED_PREFIX`. **M.**
- **chdir (`:5263-5266`):** `current_dir(<FIXED_PREFIX>/<targetkey>/work)`. **S.**
- **Cleanup safety guard (`:8506-8520`):** the `remove_dir_all` guard rejects paths outside
  `root_action_directory`; the stable path must be under a permitted root, and the operation_id-derived
  logic updated to accept `targetkey`. Keep the wipe of INPUTS between builds (seed re-materializes as an
  input); reconcile with `directory_cache` direct-use symlink (`prepare_action_inputs:3853-3915`) — a
  stable execroot cannot also be a direct-use input symlink (direct_use_mode is already default-OFF). **M–L.**
- **Per-worker lease (net-new):** bounded `HashMap<targetkey, Lease>` (`// CAPPED AT N`) on
  `RunningActionsManager`. Same-`targetkey` concurrent (cross-invocation) → owner runs at the stable path
  (warm); contender runs at the `operation_id` fallback and **discards `-incr`** (cold but correct). No
  serialization. **M.**
- **Scheduler target-affinity (`api_worker_scheduler.rs`):** extend the existing cache-affinity tier
  (Tier-1/1.5 `locality_winner`, `output_affinity` maps `:566-597`, FL-426) with a `targetkey` signal so
  same-target work prefers the worker owning that stable dir, lifting warm-hit rate. Hot path — profile
  `do_try_match`. **M.**

## 8. Concurrency isolation (constraint 3) — resolved

- **Dynamic local+remote of the same action:** different machines → different disks → **no real
  collision**; both just need the same *string*, which the scheme guarantees. Both can be warm (each seeds
  from CAS).
- **Different targets:** different `<FIXED_PREFIX>/<targetkey>` real dirs → isolated by construction.
- **Same target, concurrent, same machine** (local worktree-pool slots; remote cross-invocation): the ONE
  real collision on macOS-without-namespaces → **machine-local lease, owner-stable / contender-isolated-
  and-discards** (§6, §7). This keeps (2) and (3) from conflicting: the stable KEY never changes (2 holds);
  collisions are resolved by demoting the *loser* to a throwaway path (3 holds); rustc never sees the
  throwaway as "the target's path."

## 9. Risks / regressions (for the cadre to stress)

- **Wiped-dir invariant change is architectural** — reuses a real execution dir across builds. Safe ONLY
  IF inputs are fully re-materialized each build (premise) and NO non-seed state survives. Prove it.
- **Hermeticity:** hardlinked inputs (local) must present rustc identical absolute paths + content to the
  remote's real files; a rustc input-path *canonicalization* difference would break local≡remote — the
  micro-experiment (§11) must confirm hardlink-input + real-dir reuse.
- **Lease correctness is load-bearing:** two concurrent same-key compiles at one real path would corrupt
  the `-incr` cache. Workers are trusted, but the lease (not trust) prevents corruption. Applies on BOTH
  branches.
- **Disk:** N persistent per-target dirs + large `-incr` caches (both branches). Competes with FL-688 pin
  saturation + DirectoryCache budget on a fleet already fighting pin pressure. Needs eviction.
- **Worktree-pool parallelism:** the local lease serializes same-target builds across slots (correctness
  over throughput for that case).
- **Provisioning:** FIXED_PREFIX must exist identically on all build macs + workers, one volume with the
  execroot (hardlinks). A drift = silent cold builds (dark). Add a startup assertion + a warm-hit counter.
- **Scheduler complexity + hot-path cost** (`do_try_match`).
- **macOS-specificity:** the design is macOS-shaped; a Linux fleet would prefer the namespace model. Don't
  hard-code the assumption.

## 10. Effort + rollout

**Overall XL.** Flag-gated, one crate first, **dynamic from the start** (remote-only is off the table):
1. rules_rust: `-incr` as a normal CAS tree output+input (drop `no-remote`). **L.**
2. process_wrapper: per-target dir + hardlink inputs/seed + chdir + output relocation + local lease. **L–XL.**
3. NativeLink: stable-path materialization + chdir + worker lease, allowlist-scoped. **M.**
4. Provision FIXED_PREFIX fleet-wide + build macs; add `incr_warm_reuse_total` counter on BOTH branches
   and a startup FIXED_PREFIX assertion.
5. Enable for ONE high-value crate (large asan/tsan); measure warm-hit rate local AND remote under dynamic.
6. Scheduler target-affinity to raise the hit rate; then widen the allowlist.

## 11. The one remaining experiment — DONE, CONFIRMED WARM (2026-07-17)

Ran the A/B/C/D raw-rustc test (nightly 1.98.0, 900-fn crate, `--emit=obj -Copt-level=2 -Ztime-passes`,
`/tmp/nlincr-exp.sh`, log `/tmp/nlincr-exp-run.log`). LLVM_passes seconds:

| Condition | LLVM_passes |
|-----------|-------------|
| seed (cold first build) | 1.441 |
| **A: real-copy inputs at `<PREFIX>/tk1` + seed incr (control)** | **0.001 (warm)** |
| **B: HARDLINK inputs at `<PREFIX>/tk1` + seed incr (the test)** | **0.001 (warm)** |
| C: fresh incr, real files | 1.415 (cold) |
| D: seed incr copied to a DIFFERENT path `tk2` | 1.402 (COLD) |

**Verdict: hardlinked inputs reuse codegen IDENTICALLY to real copies (0.001 s, ~1400× vs cold).** The
hardlink shared the source inode (link count 2, same inode) and rustc still reused — no input-path
canonicalization defeats it. D independently re-confirms Step-0 path-binding (same `-incr`, different path
→ cold). The local branch's cheap hardlink path is validated.

**Caveat (open item §14):** run on Linux/ZFS, not macOS/APFS. The mechanism is platform-independent rustc
incremental logic + POSIX hardlink semantics (a hardlink is a real dirent, no resolution), so it
generalizes with high confidence — but a macOS/APFS spot-confirm on a worker (off-hours) is the final gate
before the process_wrapper L–XL work.

## 12. Interaction with the local `-incr` materialization cost (F)

With dynamic REQUIRED (not remote-only), the local branch runs locally and **still needs the `-incr` seed
materialized locally** — that cost is NOT retired (the remote-only bonus is gone). What improves: the seed
becomes a SINGLE CAS-shared `-incr` tree that both branches materialize (whoever won last build produced
it), rather than a local-only copy — so no DOUBLE cost, and a remote-won build correctly seeds the next
local build. The `--remote_download_regex` local-pull of the prior `incr_tree` (`.bazelrc:784-799`) is
subsumed by the normal declared-output CAS flow.

## 13. Prior art

- **`/b/f/w`**: reclient/RBE-server convention, not a Bazel client flag; reclient owns both submission and
  remote layout to canonicalize the working dir. Not drop-in.
- **Buildbarn `bb_runner` `buildDirectoryPath`**: fixed base but appends a per-action subdir (same
  `operation_id` problem — NativeLink's `work_directory` is the exact analog).
- **Adaptation:** per-target real fixed path + machine-local lease + dynamic-symmetric seed is the
  macOS-fleet-viable adaptation of the fixed-input-root idea.

## 14. Open questions the cadre MUST answer

1. Does hardlink-input + real-dir chdir reuse (§11)? If no, the local branch's cheap path collapses.
2. Is the disk footprint (N per-target `-incr` dirs × both branches) affordable against FL-688 pin budget,
   and what evicts them?
3. Does the local lease's same-target serialization meaningfully hurt worktree-pool throughput on real CI?
4. Output relocation in process_wrapper — any Bazel output-verification (mtime, digest) that a
   relocate-from-fixed-dir would trip?
5. FIXED_PREFIX provisioning + one-volume (hardlink) requirement across heterogeneous build macs — feasible
   and drift-safe?
