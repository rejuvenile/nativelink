# Portable rustc incremental via byte-identical execroot — DESIGN v3

**Status:** design-only, for review cadre. Supersedes v2 (whose 5/5 re-cadre found the v2 seed-CAPTURE path
broken for remote-won builds). **Date:** 2026-07-17. **Architectural sign-off REQUIRED** before implementation
(wiped-dir execution-buffer invariant). One sub-decision (§6 fetch-wiring) is **conditional on an in-flight
two-machine remote-`.rlib`-cache experiment** — flagged inline.

## 0. What changed from v2 (operator-directed)

v2's seed transport was broken: `RustcIncrSeed` (`no-remote`) reads the prior `-incr` from LOCAL disk, but under
the live `--remote_download_outputs=toplevel` (with the `.*-incr` download regex removed, FL-681) a **remote-won**
`-incr` stays in CAS-only → empty local seed → cold; self-limiting. v3 replaces the seed transport with:

- **§6 — Option 3: a fleet-shared `targetkey → -incr` store** (operator decision) that carries a build's `-incr`
  to *any* machine/branch, closing the local↔remote convergence loop. Decomposed as **CAS for content + an
  AC-shaped mutable index keyed by `hash(targetkey)`**.
- **§10 — reframed:** the `-incr` is "just another content-addressed sub-tree," so its residency is a
  **contribution to the EXISTING cache-affinity scorer**, NOT a new targetkey-affinity tier.
- **§7 — relaxed:** worker-local `-incr` becomes an **evictable, store-backed fetch-cache** (not the authority),
  which is safe (store is authoritative + rustc re-validates).
- Plus the carried v2 re-cadre fixes (§8 real disk-budget, §10 Option-B targetkey derivation, §2 toolchain
  fact-fix, security MED-1/2, `-incr-metadata` second seed).

## 1. Goal (unchanged)
Portable rustc **incremental** reuse under `--spawn_strategy=dynamic`: a CAS-shared `-incr` seed re-materializes
at a byte-identical absolute path on whichever branch runs (local mac / remote macOS worker), so reuse fires.

## 2. Validated foundation (measured — do NOT re-litigate)
- Reuse is bound to the absolute path only (Step-0). Hardlink inputs reuse == real copies (§11-lin + macOS exp#2).
- **Cross-machine + cross-MODEL transfer WORKS** (exp#1 ci-mac-3 M4 → ci-mac-1 M2 Ultra, 0.001s vs 0.665s cold).
- macOS `getcwd` returns the literal path (firmlink-transparent, exp#2); rustc **realpaths** cwd/source/incr →
  **real physical paths required, no symlink** (symlink sub-exp).
- target-cpu is FIXED `apple-a14` (`.bazelrc:580-581`), cross-model safe — no change.
- **Toolchain caveat (corrected):** identical sysroot ABSOLUTE PATH is NOT "by construction / content-addressed";
  it's execroot-relative (`toolchain.bzl:647`) + a HOST-PROVISIONING invariant (every machine has
  `/Users/user/.rustup`, `.bazelrc:512-513`). §12 asserts it byte-identical at startup; local branch must not
  reach sysroot via an `output_base` symlink (rustc realpaths → cold).
- EXDEV: `/Volumes/CrowAgent` is a separate volume → FIXED_PREFIX co-located on the execroot volume, else copy-fallback.

## 3. Path-key scheme (unchanged)
`targetkey = ≥128-bit collision-resistant hash of the REAPI `Command.output_paths` primary output`
(auditor-VERIFIED: sorted for consistent Action hashing; `determine_output_hash` is path/label/config-derived →
stable across edits, config-discriminating). Store the full output-path beside the dir; collision → detected
cold-fallback.

## 4. Canonical execution path (unchanged from v2 — the getcwd fix)
Both branches chdir into the IDENTICAL real dir `<FIXED_PREFIX>/<targetkey>` (worker drops the `/work` segment for
allowlisted actions; `Command.working_directory` is empty/symmetric — auditor-VERIFIED). Real dir, real chdir
(no symlink). `-incr` AND the pipelined `-incr-metadata` tree (`rustc.bzl:2255-2274`) BOTH need this path pinning.

## 5. Concurrency lease (unchanged from v2)
Same-`targetkey` concurrent on one machine → machine-local RAII drop-guard lease (owner warm / contender isolated
+ cold-discard, no serialization; released through output relocation; mirrors `CleanupGuard:8697`). Dynamic
local+remote of the same action = different machines → no collision.

## 6. Seed model — Option 3: fleet-shared `targetkey → -incr` store (REWRITTEN)

### 6.1 Store decomposition (CAS content + AC-shaped index)
The CAS is content-addressed (key = `hash(content)`, immutable) — it CANNOT be keyed by `targetkey`. The seed
needs **overwrite-by-stable-key** (same `targetkey`, changing `-incr` content each build). So:
- **`-incr` CONTENT → the existing CAS, unchanged.** The `-incr` tree is already a content-addressed CAS artifact
  (`incr_tree`, a declared output); dedup across builds is free. No new content store.
- **`targetkey → current -incr digest` INDEX → an AC-shaped mutable store keyed by `hash(targetkey)`**, in a
  **distinct `instance_name`** (e.g. `incr_seed_index`) so it never collides with real Bazel action-cache entries.
  Value = a record whose output-directory digest references the current `-incr` tree in CAS. Overwrite per build
  (last-writer-wins). This is the operator's "reuse existing storage with a different keyspace prefix," realized:
  reuse CAS for content + AC-shape for the index, `instance_name` = the prefix. (Impl: verify NativeLink's AC
  store accepts a synthetic `hash(targetkey)` key under a dedicated instance; else a dedicated small KV store.)

### 6.2 Convergence loop (both branches)
- **Publish (after any build, either branch):** `-incr` content → CAS (already happens); then update
  `index[hash(targetkey)] → <incr Directory digest>`.
- **Fetch (before any build, either branch):** `index[hash(targetkey)]` → CAS digest → materialize `-incr` at the
  pinned path. Remote-won seeds the next local build and vice-versa — the two-directional loop v2 lacked.
- **Staleness/races:** last-writer-wins on the index; a stale seed → rustc re-validates → cold-fallback. Correct.

### 6.3 Fetch wiring — CONDITIONAL on the in-flight experiment
The experiment answers: does a warm `incr_seed` (as an `unused_inputs_list`-excluded Bazel input, `rustc.bzl:2514/
2539`) enter the REMOTE REAPI action key?
- **If YES (churns the remote key):** the NativeLink WORKER fetches the seed from the index **out-of-band** and
  injects it at the pinned path before rustc (NEVER a Bazel input → cannot touch the remote `.rlib` key); a Bazel
  `RustcIncrSeed`-style action fetches from the index for the LOCAL branch.
- **If NO (pruned from the remote key too):** a single Bazel seed action fetches from the index and feeds it as the
  existing `unused_inputs_list`-excluded input for BOTH branches (simpler; the current wiring, re-sourced from the
  index instead of local `prev_incr_dir`).
Either way the §6.1 store is unchanged; only the fetch path differs. **[GATE: resolve before the rules_rust build item.]**

### 6.4 Correctness + security boundary
- Correctness floor = **rustc's per-query input-fingerprint re-validation** (a stale/wrong/torn seed → rustc
  rejects → COLD, never wrong). **On-disk seed persistence as the AUTHORITY is forbidden** — the store is
  authoritative; on-disk is a cache (§7).
- **Accidental vs adversarial (security MED-1):** the "never wrong" floor holds for accidental wrongness (buggy
  build / torn transfer / stale seed). An ADVERSARIALLY-forged seed whose internal fingerprints are made
  self-consistent is out-of-scope BY the trusted-ACTION assumption, NOT caught by rustc fingerprinting.
- **Publish authority (security MED-2):** because `targetkey` is DERIVABLE, the index PUBLISH path must be
  restricted to trusted producers + the `targetkey→seed` binding authenticated (an action must not publish a seed
  under an arbitrary/derivable `targetkey` a peer will trust). Content integrity is free (CAS content-addressed).

## 7. Wipe — worker-local `-incr` is an evictable store-backed cache (RELAXED)
Full content-empty wipe of the execroot each build stays for all NON-`-incr` state (`create_dir:8602/:5076` →
ensure-exists-then-empty; direct_use OFF by construction for allowlisted actions). The worker-local `-incr` is now
an **evictable fetch-cache of the §6 store, NEVER the authority**: stale (store newer) → rustc re-validates →
cold-fallback; evicted → re-fetch. v2's "forbid on-disk persistence" is thus safely relaxed to "on-disk `-incr` is
a store-backed cache; the store is authoritative; all non-`-incr` state fully wiped."

## 8. Disk budget (FIXED — no shared authority exists)
There is NO pre-existing "shared disk-budget authority" (re-cadre: FilesystemStore `eviction_policy.max_bytes` and
DirectoryCache's own pin-cap `directory_cache.rs:2891` are SEPARATE). v3 picks the simpler option: the seed/execroot
pool is a **bounded pool with a STATIC reservation carved out of the FilesystemStore + DirectoryCache configured
`max_bytes`** (not a 4th runaway pool, and not a new unification workstream). LRU within the reservation; an evicted
`targetkey` cold-starts (safe) + re-fetches from the store. (Building a truly unified disk-budget authority is a
separate, larger workstream — out of scope for v3.)

## 9. Security + provisioning + EXDEV (from v2 + §6.4)
FIXED_PREFIX owned by worker uid (0755, NOT world-writable `/Users/Shared` 1777); O_EXCL dir creation; O_NOFOLLOW /
reject-symlinked-components on materialize, output relocation, AND the EXDEV copy-fallback path (read-then-write,
wider TOCTOU). Atomic per-process output writes (tmp-then-rename) → lease bypass = last-writer-wins/loud, never
silent-wrong. Cleanup-containment: the `remove_dir_all` guard (`:8506-8523`) confines deletes to a provably
**per-worker** subtree of the fleet-identical FIXED_PREFIX (concrete namespacing scheme required). EXDEV: `link()`
startup assert + copy-fallback + counter. Publish-authority + accidental/adversarial per §6.4.

## 10. Scheduler — `-incr` residency in the EXISTING affinity scorer (REFRAMED, operator-directed)
NOT a new targetkey-affinity tier. The `-incr` is a content-addressed sub-tree, so:
- At match time, resolve `targetkey → -incr digest` from the §6 index and ADD that digest to the action's
  affinity-relevant sub-tree set. The EXISTING residency map (`output_affinity`/`output_file_affinity` `:566-597`,
  `locality_winner:3945`) + the EXISTING blend rank workers by holding it, alongside input-tree overlap, load,
  memory-gate, cpu-first, saturation. The worker holding last build's `-incr` scores higher → warm + zero fetch; a
  worker without it still runs (fetches from the store) at a lower affinity score → warm + pay-fetch.
- **This is inherently inside the saturation/viability/memory envelope** (it's a contribution to the existing
  cache-affinity component, not a parallel tier) → the v1-F7 concern is automatically satisfied; it's a preference,
  not a pin.
- **No staleness trap:** the scheduler scores by the SEED digest (prev build's `-incr` = exactly what the index
  serves + what the producing worker holds); after the build both advance.
- **Weighting:** start byte-consistent with any sub-tree; leave a knob to up-weight `-incr` residency (a warm seed
  saves full codegen, not just a fetch) if measured worth it — no special weight baked into v1.
- **Decoupled from §6.3:** affinity needs only the digest (from the index) + residency, so it works identically
  under either fetch-wiring.
- **Integration hook (verify at impl):** the residency map must SEE a worker's resident `-incr` sub-tree digest
  (it's in the worker's local content-addressed store, so likely already visible; if not, small hook to add).

## 11. Change sites (updated)
- NativeLink: `targetkey` via **Option B** — derive inside `inner_prepare_action:5047` reusing the existing Command
  fetch (zero added CAS round-trips; NOT `create_action_info:8609` which lacks output_paths). `make_action_directory
  :8601` → `<FIXED_PREFIX>/<targetkey>`; chdir `:5263` canonical (no `/work`); wipe/guard §7/§9; RAII lease §5.
  The §6 index publish/fetch (AC-shaped store, `instance_name=incr_seed_index`). The §10 residency hook.
- rules_rust: the seed FETCH-from-index action (§6.3-dependent); `-Cincremental` arg unchanged; NO target-cpu change.

## 12. Observability (three-state + store)
`incr_seed_materialized` / `incr_reuse_fired` / `incr_seed_present_but_cold` (per-branch) + lease-contention +
**store: `incr_index_fetch_{hit,miss}` / `incr_index_publish` / `incr_store_fetch_bytes`** + the FIXED_PREFIX +
sysroot-absolute-path + `link()` startup assertions.

## 13. Effort + rollout
XL, dynamic from the start. (1) §6 store: CAS reuse + AC-index-by-hash(targetkey), publish/fetch, publish-authority.
(2) NativeLink stable-path + lease + §10 residency hook + the reservation disk-budget. (3) process_wrapper /
worker fetch-wiring per §6.3 (L–XL; output-relocation-vs-Bazel-verify is the gating unknown). (4) provision
FIXED_PREFIX per-machine on the execroot volume + the §12 asserts. (5) one apple-a14 crate; measure the store +
three-state counters under dynamic. (6) widen.

## 14. Open items the cadre must weigh
1. §6.3 fetch-wiring — pending the two-machine remote-`.rlib`-cache experiment (does the seed churn the remote key?).
2. §6.1 — does NativeLink's AC store accept a synthetic `hash(targetkey)` key under a dedicated `instance_name`, or
   is a dedicated small KV store cleaner? (mutable overwrite semantics either way.)
3. §8 static-reservation carve-out vs a real unified budget — is the carve-out safe against the FL-688 pin budget
   under load?
4. §10 residency-visibility hook + the up-weight default.
5. §9 per-worker delete-containment namespacing under a fleet-identical FIXED_PREFIX.
