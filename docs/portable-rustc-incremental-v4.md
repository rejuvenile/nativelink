# Portable rustc incremental via byte-identical execroot — DESIGN v4 (FL-1383)

**Status:** implementation-ready. **Architectural / wiped-dir sign-off GRANTED** by the v3 distsys review
(`.claude/reviews/design-portable-rustc-incremental-v3/`). v4 addresses the v3 FIXES-REQUIRED bundle (5/5 cadre).
Supersedes v1–v3. **Date:** 2026-07-17. Tracking bug: **FL-1383.**

Two pre-CODE gates remain (neither reopens the architecture): (i) the publish-authority **network precondition**
(§6.6); (ii) a two-machine remote-`.rlib`-cache experiment that can only *simplify* §6.3 (default is already the
safe branch).

## 1. Goal
Portable rustc **incremental** reuse under Bazel `--spawn_strategy=dynamic`: a fleet-shared `-incr` seed
re-materializes at a **byte-identical absolute path** on whichever branch runs (local mac / remote macOS worker),
so rustc reuse fires on both.

## 2. Validated foundation (measured — proven, do not re-litigate)
- Reuse is bound to the absolute path only (Step-0). Hardlink inputs reuse == real copies (§11-lin + macOS exp#2).
- **Cross-machine + cross-MODEL transfer WORKS** (exp#1 ci-mac-3 M4 → ci-mac-1 M2 Ultra: 0.001s vs 0.665s cold).
- macOS `getcwd` returns the literal path (firmlink-transparent, exp#2); rustc **realpaths** → real physical paths
  required, no symlink (symlink sub-exp).
- target-cpu is FIXED `apple-a14` (`.bazelrc:580-581`) → cross-model safe, no change.
- Sysroot identical-path is a HOST-PROVISIONING invariant (`/Users/user/.rustup`, `.bazelrc:512-513`; execroot-
  relative flag `toolchain.bzl:647`) — NOT content-addressed. §12 asserts it byte-identical at startup; local branch
  must not reach sysroot via an `output_base` symlink (rustc realpaths → cold).
- EXDEV: `/Volumes/CrowAgent` is a separate volume → FIXED_PREFIX co-located on the execroot volume, else copy-fallback.

## 3. Path-key scheme
`targetkey = ≥128-bit hash of `Command.output_paths` primary output` (auditor-VERIFIED: sorted for consistent
hashing; `determine_output_hash` path/label/config-derived → stable across edits, config-discriminating). Store the
full output-path beside the dir; collision → detected cold-fallback. **Deliberately source-version-BLIND** (edit N's
`-incr` must seed N+1) — the concurrency consequence is documented in §6.2.

## 4. Canonical execution path (getcwd fix, sign-off basis)
Both branches chdir into the IDENTICAL real dir `<FIXED_PREFIX>/<targetkey>` (worker drops the `/work` segment for
allowlisted actions; `Command.working_directory` empty/symmetric — auditor-VERIFIED). Real dir + real chdir (no
symlink). `-incr` AND the pipelined `-incr-metadata` tree (`rustc.bzl:2255-2274`) BOTH need this path pinning.
**FIXED_PREFIX is machine-LOCAL** (never a shared network FS) — resolves the §4↔§9 containment tension.

## 5. Concurrency lease
Same-`targetkey` concurrent on one machine → machine-local RAII drop-guard lease (owner warm / contender isolated +
cold-discard, no serialization; released through output relocation; mirrors `CleanupGuard:8697`). Dynamic
local+remote of the same action = different machines → no collision.

## 6. Seed model — fleet-shared `targetkey → -incr` store (Option 3)

### 6.1 Store decomposition
- **`-incr` CONTENT → existing CAS**, unchanged (already a content-addressed `incr_tree` output; dedup free) — but
  see §6.5 (durability).
- **`targetkey → current -incr digest` INDEX → an AC-shaped MUTABLE store keyed by `hash(targetkey)`** in a distinct
  `instance_name = "incr_seed_index"`. VERIFIED real (`ac_server.rs:414-432`: AC not content-addressed,
  `update_oneshot` overwrites any digest key). Value = an ActionResult whose `output_directories[0].tree_digest`
  references the current `-incr` in CAS. **REQUIRED composition:** `incr_seed_index` MUST sit **behind
  CompletenessCheckingStore** so a dangling index entry (content evicted) resolves to CAS-NotFound → cold, never a
  live-digest-to-nowhere.

### 6.2 Convergence loop + consistency
- **Publish (after a CLEAN compile only — never the dynamic loser's torn seed):** `-incr` content → CAS + **pin**
  (§6.5); then `update_oneshot(hash(targetkey), ActionResult→incr digest)`.
- **Fetch (before build, BEST-EFFORT + bounded fallback):** `GetActionResult(hash(targetkey))` → CAS digest →
  materialize at the pinned path. `GrpcStore` has timeout=0 (invariant #10) → a slow/absent index MUST NOT stall the
  build: bounded timeout → **cold-start on miss/timeout**, `incr_index_fetch_{hit,miss,timeout,error}` counters.
- **LWW consistency — documented limitation.** `targetkey` is source-version-blind (§3), so concurrent builds of
  DIFFERENT source versions of the same crate serialize through one mutable key; a late older-commit build clobbers
  the index → peers fetch a staler seed → rustc re-validates → **cold (never wrong)**. Output-correct; reuse
  degrades under concurrent multi-version CI. Keying by `targetkey+source-hash` would kill edit-invariance (not
  viable). ACCEPTED: reuse is best-effort under concurrent multi-version load; the §12 counters
  (`seed_present_but_cold` climbing) diagnose it; expect a reduced-but-nonzero CI hit-rate floor.

### 6.3 Fetch wiring — WORKER OUT-OF-BAND (default, per distsys gate)
The NativeLink **worker fetches from the index out-of-band and injects the seed at the pinned path before rustc** —
the seed is NEVER a Bazel action input, so it cannot enter the remote `.rlib` REAPI key. Cache-HIT → rustc never
runs (seed irrelevant); MISS → rustc reads the injected seed → output identical → no poisoning. The LOCAL branch's
`RustcIncrSeed` fetches from the same index instead of the local `prev_incr_dir` (`rustc.bzl:2227`, the v2-broken
read). **[Potential simplification, gated:** a single Bazel-declared `unused_inputs_list`-excluded seed for BOTH
branches — ONLY if the two-machine experiment proves the seed does not churn the remote key; distsys's strong prior
is that it does, so out-of-band is the committed default.]

### 6.4 Correctness boundary
rustc per-query fingerprint re-validation = the correctness floor (stale/wrong/torn seed → COLD, never wrong).
On-disk seed persistence as the AUTHORITY is forbidden (store is authoritative; §7). **Accidental** wrongness → cold
(the floor). **Adversarially-forged** self-consistent seed → out-of-scope BY the trusted-ACTION assumption, NOT
caught by rustc — see §6.6.

### 6.5 CAS-content durability (NEW — FL-688 class, red-team's strongest)
The `-incr` content is LARGE (asan/tsan), churned every build, single-use from LRU's view → the ideal CAS eviction
victim under exactly the sustained load where reuse matters; the index would then point at an evicted digest → cold
(the FL-688 incident class). **The publish MUST pin/retain the `-incr` content the index authority references** — a
bounded retention on the seed content (e.g. indefinite-pin the current `-incr` blob per live `targetkey`, released
when the index overwrites), NOT relying on the general CAS LRU. Sized against the FL-688 pin budget with a proof
`(pin_high_water + incr_content_reservation + working_set) ≤ physical`. Without this, reuse is best-effort-until-
evicted — state which if the pin is deferred.

### 6.6 Publish authority (NEW — security HIGH)
The index is fleet-global, keyed by a DERIVABLE `hash(targetkey)`, last-writer-wins → an AC-poisoning surface.
NativeLink's AC write path has only read_only-vs-writable (no worker-only-writable mode; `x-nativelink-worker` is
NOT an auth boundary). **PRECONDITION to assert + verify before code:** action subprocesses have NO network egress
to the CAS/AC listener port → the publish surface closes under the trusted-worker model. If actions CAN reach the
port, a worker-only-writable control on the `incr_seed_index` endpoint is REQUIRED (BLOCK-hinge). Content integrity
is free (CAS content-addressed); the INDEX write-authority is the surface.

## 7. Wipe — worker-local `-incr` is a store-backed cache
Full content-empty wipe each build for all NON-`-incr` state (`create_dir:8602/:5076` → ensure-exists-then-empty;
direct_use OFF by construction for allowlisted actions). Worker-local `-incr` = an EVICTABLE fetch-cache of the §6
store, NEVER the authority: **verify the on-disk `-incr` against the store's content-digest before use** (security
MED); stale → rustc re-validates → cold; evicted → re-fetch. This safely relaxes v2's "forbid persistence."

## 8. Disk budget
The already-CAS-resident `-incr` CONTENT is handled by §6.5. The genuinely new on-disk pool is the **materialized
`-incr` dirs at `<FIXED_PREFIX>/<targetkey>`** — bound THIS pool: it rides the existing `DirectoryCache`
eviction+pin-cap (`directory_cache.rs:2891`, config-only) OR gets its own bounded LRU if separate. Static reservation
carved from the FilesystemStore + DirectoryCache `max_bytes` (no unified authority exists — that's a separate
workstream), with the §6.5 sizing proof against the FL-688 pin budget. LRU-evicted `targetkey` cold-starts (safe) +
re-fetches.

## 9. Security + provisioning + EXDEV
FIXED_PREFIX owned by worker uid (0755, NOT world-writable `/Users/Shared` 1777), machine-local (§4); O_EXCL dir
creation; O_NOFOLLOW on materialize, output relocation, AND the EXDEV copy-fallback path. Atomic per-process output
writes (tmp-then-rename) → lease bypass = last-writer-wins/loud, never silent-wrong. The `remove_dir_all` guard
(`:8506-8523`) confines deletes to a provably per-worker subtree of FIXED_PREFIX (concrete namespacing required).
EXDEV: `link()` startup assert + copy-fallback + counter. Index store DoS cap (derivable-targetkey flood →
targeted eviction). Publish-authority precondition per §6.6.

## 10. Scheduler — `-incr` residency in the existing affinity scorer (no new tier)
- **Route `targetkey` via `ActionInfo` at INGESTION, NOT a match-time store fetch.** `ActionInfo`
  (`action_messages.rs:280-286`) carries only `command_digest`/`input_root_digest`; deriving `targetkey` needs
  `output_paths`. Derive it ONCE at ingestion (Command already fetched) and thread it through `ActionInfo` — the
  scheduler then has it FREE at match, with NO store round-trip on the `do_try_match` hot path (avoids the
  `expensive-observability-probe-in-hot-loop` O(N²) trap).
- **Residency GOSSIP hook (net-new, named):** the worker's materialized `-incr` Directory digest must enter
  `worker.cached_subtree_digests` (`api_worker_scheduler.rs:11349`) so the existing affinity blend
  (`:3110/:3196/:3788`, `locality_winner:3945`) scores it — the recorder already ingests output-Directory digests
  (`:4924-4938`), but injecting the SEED digest into the match-time lookup set is the new piece.
- Inside the saturation/viability envelope automatically (a contribution to the existing cache-affinity component,
  not a parallel tier — F7 satisfied). No staleness trap (scores the SEED digest the producing worker holds).
  Byte-consistent weighting + optional up-weight knob (a warm seed saves codegen, not just a fetch); no special
  weight in v1. Decoupled from §6.3.

## 11. Change sites
- rules_rust (stage-1 NET-NEW, not a hook): the **seed FETCH-from-index action** (`RustcIncrSeed` re-sourced from
  `incr_seed_index` via a REAPI `GetActionResult`, replacing the local `prev_incr_dir` read) — the mechanism the
  design rests on; and the local seed materialization at the pinned path.
- NativeLink: `targetkey` via Option-B at ingestion (Command fetched `inner_prepare_action:5049`, `output_paths` by
  `:5183`); `make_action_directory:8601` → `<FIXED_PREFIX>/<targetkey>`; chdir `:5263` (no `/work`); wipe/guard
  §7/§9; RAII lease §5; the §6.2 publish (gated-on-success, pin §6.5) + worker out-of-band fetch §6.3; the index
  store instance behind CompletenessChecking §6.1; the §10 ActionInfo-targetkey + residency-gossip hook.

## 12. Observability
Per-branch `incr_seed_materialized` / `incr_reuse_fired` / `incr_seed_present_but_cold`; lease-contention;
`incr_index_fetch_{hit,miss,timeout,error}` / `incr_index_publish` / `incr_content_pin_{count,bytes,evict}`;
startup asserts: FIXED_PREFIX (uid/0755/machine-local), sysroot absolute path byte-identical, `link()`-to-execroot-
volume. (`seed_present_but_cold` climbing = LWW-clobber or CAS-eviction diagnostic; `index_fetch_hit` up while
`seed_materialized` flat = content-eviction.)

## 13. Effort + rollout (XL, dynamic from the start)
(1) rules_rust seed-fetch-from-index (stage-1 net-new) + the `incr_seed_index` store instance behind
CompletenessChecking + the §6.5 content-pin. (2) NativeLink stable-path + lease + §6.2 publish/fetch + §10
ActionInfo-targetkey + residency-gossip hook + §8 pool budget. (3) worker out-of-band injection §6.3. (4) provision
FIXED_PREFIX per-machine on the execroot volume + §12 asserts + the §6.6 egress precondition check. (5) ONE
apple-a14 crate under dynamic; measure the three-state + store + pin counters. (6) widen — WATCH `seed_present_but_
cold` for the multi-version-LWW + content-eviction floors that only surface at fleet scale (§6.2/§6.5).

## 14. Pre-code gates (architecture already signed off)
1. §6.6 publish-authority: confirm actions have no egress to the CAS/AC port (else add worker-only-writable control).
2. §6.5 CAS-content-pin sizing proof vs the FL-688 pin budget.
3. Two-machine remote-`.rlib`-cache experiment — can only *simplify* §6.3 (default = out-of-band).
