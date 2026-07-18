# Portable rustc incremental via byte-identical execroot — DESIGN v4 (FL-1383)

**Status:** implementation-ready — **ALL PRE-CODE GATES CLEARED 2026-07-17.** **Architectural / wiped-dir sign-off
GRANTED** by the v3 distsys review (`.claude/reviews/design-portable-rustc-incremental-v3/`). v4 addresses the v3
FIXES-REQUIRED bundle (5/5 cadre). Supersedes v1–v3. **Date:** 2026-07-17. Tracking bug: **FL-1383.**

Both pre-CODE gates are now resolved (neither reopened the architecture): (i) §6.6 publish-authority CLEARED by the
operator's trusted-worker decision; (ii) §6.5 CAS-content-pin sizing CLEARED — the proof HOLDS at full-fleet-widen
scale (IF we pin, it fits, via a disjoint seed pin-class), but the pin itself is **optional-pending-measurement**: a
missing seed is already safe (§6.1 CompletenessChecking + §6.4 cold-fallback), so the pin is a hit-rate optimization,
NOT a correctness requirement — **Stage 1 ships WITHOUT it** and measures the real reuse hit-rate under production CAS
pressure. The two-machine experiment (§14-3) already confirmed §6.3 out-of-band is required. **Stage 1 (rules_rust
seed-fetch action + the `incr_seed_index` store, behind a flag, one apple-a14 crate) can begin.**

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

## 3b. Cross-repo contract (SETTLED 2026-07-17 with the Bazel/rules_rust side)
KAT byte-verified: `blake3("bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib") = a17b9c22c639c19f2951ad4d0cb28df41dbd33113bbe7ead51ac0dd577998567` (our `targetkey.rs` + Bazel `RemoteIncrTargetKey.java` + FL `incr_seed_index` tool all agree; locked by a KAT in `nativelink-util/tests/targetkey_test.rs`).
- **Carrier:** the Bazel client attaches `nl_incr_targetkey` (64-hex) + `nl_incr_primary_output` (the stripped primary-output string) as Action **Platform properties** for allowlisted actions. The server READS them at ingestion (no Command fetch — the review BLOCK fix, `execution_server.rs`), presence = allowlisted; declare both `nl_incr_*` as `ignore` in `supported_platform_properties` (deploy config delta). Server-side cheap integrity check: `blake3(primary)==targetkey` else cold.
- **Index key:** `hash = blake3_hex("fl-incr-seed-index:v1:" ++ targetkey)`, `size_bytes = len(preimage)` (86 for v1). `v1:` = rotation knob. AC-shaped, NOT content-addressed, NO VerifyStore; behind CompletenessCheckingStore with a RECURSIVE FileNode existence check (dangling → NotFound → cold).
- **Index value:** `ActionResult.output_directories[0] = OutputDirectory{path: <primary_output>, tree_digest: blake3(REAPI Tree proto)}`. Fetch: collision-check `path != mine → cold`; else materialize the Tree (verify each blob digest → live-digest-to-nowhere surfaces as verify-fail → cold).
- **Digest:** blake3 (df=9) for all CAS+AC. **Content on the `main` CAS instance** (`--cas-instance=main`); the mutable **index on `instance_name="incr_seed_index"`**.
- **Path-mapping:** FL build runs `--experimental_output_paths=strip` → `Command.output_paths` are config-STRIPPED (no `bazel-out/<config>/` segment). Config-discrimination is best-effort via the `.rlib` config-salt; the `-incr` tree dir has no salt → when it's the bytewise-min output the key is CONFIG-SHARED → cross-config = cold-not-wrong. (Corrects the chunk-1 "config-discriminating" doc.)
- **FIXED_PREFIX = `/Volumes/CrowAgent/fl-incr-execroots`; chdir target `<FIXED_PREFIX>/<targetkey>` on BOTH branches.** `/Volumes/CrowAgent` (`disk5s1`) is the EXECROOT volume on the build agents; `/System/Volumes/Data` (`/Users`, `disk3s5`) is the cross-volume one (EXDEV) to AVOID. No per-worker segment (§5 lease handles same-machine concurrency).
- **PROVISIONING DEPENDENCY (blocks remote-branch enable):** `/Volumes/CrowAgent` does NOT exist on the 10 remote workers yet. It MUST be provisioned as a REAL writable mount (not a symlink/firmlink to `/Users` — rustc realpaths → cold) so the remote branch can chdir to the byte-identical path. v1: minimal CrowAgent mount for execroots only → worker CAS stays on `/Users` → inputs COPY-fallback (chunk 2a §12 EXDEV probe detects + falls back; absent volume → fail-loud-DISABLE, safe). Full-perf: host the worker CAS on CrowAgent too → inputs hardlink. Measure copy cost first.

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
- **Publish (after a CLEAN compile only — never the dynamic loser's torn seed):** `-incr` content → CAS (relying on
  normal CAS retention); then `update_oneshot(hash(targetkey), ActionResult→incr digest)`. An OPTIONAL retention step
  (pin / LRU-priority) is DEFERRED pending measured hit-rate — see §6.5 (missing seed is already safe, so retention is
  a hit-rate optimization, not a Stage-1 requirement).
- **Fetch (before build, BEST-EFFORT + bounded fallback):** `GetActionResult(hash(targetkey))` → CAS digest →
  materialize at the pinned path. `GrpcStore` has timeout=0 (invariant #10) → a slow/absent index MUST NOT stall the
  build: bounded timeout → **cold-start on miss/timeout**, `incr_index_fetch_{hit,miss,timeout,error}` counters.
- **LWW consistency — documented limitation.** `targetkey` is source-version-blind (§3), so concurrent builds of
  DIFFERENT source versions of the same crate serialize through one mutable key; a late older-commit build clobbers
  the index → peers fetch a staler seed → rustc re-validates → **cold (never wrong)**. Output-correct; reuse
  degrades under concurrent multi-version CI. Keying by `targetkey+source-hash` would kill edit-invariance (not
  viable). ACCEPTED: reuse is best-effort under concurrent multi-version load; the §12 counters
  (`seed_present_but_cold` climbing) diagnose it; expect a reduced-but-nonzero CI hit-rate floor.

### 6.3 Fetch wiring — WORKER OUT-OF-BAND (REQUIRED — experiment-confirmed)
The NativeLink **worker fetches from the index out-of-band and injects the seed at the pinned path before rustc** —
the seed is NEVER a Bazel action input, so it cannot enter the remote `.rlib` REAPI key. Cache-HIT → rustc never
runs (seed irrelevant); MISS → rustc reads the injected seed → output identical → no poisoning. The LOCAL branch's
`RustcIncrSeed` fetches from the same index instead of the local `prev_incr_dir` (`rustc.bzl:2227`, the v2-broken
read). **The Bazel-declared alternative (a single `unused_inputs_list`-excluded seed input for both branches) is
RULED OUT — the two-machine experiment (2026-07-17) CONFIRMED it churns the remote key:** two builds with
byte-identical source differing ONLY in `incr_seed` content produced DIFFERENT REAPI Action digests (`df7bff88`
vs `ff08968c`; seed content the sole cause — `unused_inputs_list` prunes the seed only on subsequent NON-executing
rebuilds, so a machine's first execution embeds its seed in the key). This loses cross-machine `.rlib` sharing on
exactly the LARGE crates this effort targets (the seed populates only above `incremental_seed_threshold_mb`, default
20 MB, `.bazelrc:747`; small crates keep an empty seed → same digest → still shareable). Out-of-band is mandatory.

### 6.4 Correctness boundary
rustc per-query fingerprint re-validation = the correctness floor (stale/wrong/torn seed → COLD, never wrong).
On-disk seed persistence as the AUTHORITY is forbidden (store is authoritative; §7). **Accidental** wrongness → cold
(the floor). **Adversarially-forged** self-consistent seed → out-of-scope BY the trusted-ACTION assumption, NOT
caught by rustc — see §6.6.

### 6.5 CAS-content retention — pin is OPTIONAL-PENDING-MEASUREMENT (sizing proof HOLDS as the fallback IF we pin)
**Pinning is NOT required for correctness.** The index + CompletenessCheckingStore + cold-fallback (§6.1/§6.4) make a
missing/evicted seed SAFE: a dangling index resolves to CAS-NotFound → cold build, never a wrong `.rlib`. The pin is
purely a hit-rate/retention optimization. Nor is the seed truly single-use from LRU's view — it is read at the start of
EVERY incremental build of that target, so a genuinely-reused seed is frequently-accessed and plain LRU already tends to
retain it; whether plain LRU evicts a seed during the inter-build window before reuse is an UNMEASURED empirical question
(CAS size vs inter-build churn).
**PLAN — Stage 1 ships WITHOUT the pin**, relying on normal CAS LRU, and MEASURES the real reuse hit-rate under
production CAS pressure via the §12 counters: `incr_index_fetch_hit` climbing while `incr_seed_materialized` /
`incr_reuse_fired` stay flat, or `seed_present_but_cold` climbing, = the index resolves but the content was evicted (a
retention miss). ONLY IF the measured hit-rate is too low do we add retention — and even then prefer the LIGHTER tool:
an LRU eviction-PRIORITY/weight (seeds evict LAST) over a hard pin, which needlessly borrows from the 20 GiB budget
FL-688 durability shares and drags in the whole `seed_pin_class` co-tenancy-isolation apparatus below.

**FALLBACK MECHANISM (IF retention is later proven necessary — the sized hard-pin design, NOT the Stage-1 default).**
Investigation (`.claude/reviews/design-portable-rustc-incremental-v3/gate65-content-pin-sizing.md`):
- **Pinning is ADDITIVE to `max_bytes`, not carved from it** (verified: `moka_evicting_map.rs:1668-1692` inserts into
  a separate `pinned` DashMap + invalidates the moka entry; `weighted_size` excludes pinned). So worker physical =
  working_set (≤40 GB) + pinned (≤20 GiB pin_cap). Cap is **20 GiB** (FL-681 50%, not the stale 10 GB).
- **Measured:** `-incr` seeds ~200 MB avg / 400 MB tail (large crates; medium crates <20 MB → never seeded). Current
  FL-688 pin high-water 64 MB / 215 pins (~300× under cap).
- **SIZING PROOF HOLDS** at one-crate (0.46 GB) AND full-CI-widen worst-case (27×400 MB = 10.9 GB) ≤ 20 GiB pin budget
  (≥9 GiB headroom); physical: all carve-outs saturated ~135 GB ≤ 228 GiB (~90 GiB headroom). No bigger disk / no
  max-targetkey cap needed. Binding constraint = CO-TENANCY (worst-case widen = 54% of the shared 20 GiB pin budget →
  would halve FL-688 durability headroom) → must be ISOLATED.
- **MECHANISM: a DISJOINT `seed_pinned_bytes`/`seed_pin_cap` pin-class (static carve-out, e.g. 4 GiB), EXCLUDED from the
  FL-688 indefinite admission check** — mirrors the in-tree `speculative_pinned_bytes`/`speculative_pin_cap` precedent
  (`moka_evicting_map.rs:247-262`). NOT folded into the FL-688 pool (mutual starvation), NOT a new store. Indefinite
  lifetime, released on index overwrite (§6.2), over-cap = backpressure-refuse → the crate's seed stays LRU-evictable
  → cold-but-correct. Composition: CAS moka 40 + 20-pin{seed 4 ⊕ FL-688 ≤14 ⊕ spec 2} + DirCache 40+10 + root 13 =
  113 GB ≤ 228 GiB — no unified authority needed.
- **CAVEAT (add to design):** the worker pin guarantees durability only if the §6.3 out-of-band fetch is PEER-ROUTED to
  the pinning producer (via the §10 residency affinity hook); a fetch that falls through to the server CAS (/srv/bulk, its
  own eviction) can be cold. So §6.5 durability depends on §10 routing, not just the pin existing.

The v3 framing below is retained as the fallback rationale; the reservation is the disjoint seed pin-class above.
The `-incr` content is LARGE (asan/tsan) and churned every build → a plausible CAS eviction victim under sustained
load; if evicted, the index points at an evicted digest → cold (the FL-688 incident class, but here output-correct by
§6.4 — merely a lost reuse). The v3 framing asserted "the publish MUST pin/retain the `-incr` content" via a bounded
per-`targetkey` retention released on index overwrite, sized against the FL-688 pin budget with a proof
`(pin_high_water + incr_content_reservation + working_set) ≤ physical`. That MUST is now DOWNGRADED to
optional-pending-measurement: **the pin IS deferred** in Stage 1 → reuse is best-effort-until-evicted, which is
exactly the hit-rate the §12 counters measure. Add retention (prefer LRU-priority over hard pin) only if the
measurement shows eviction is costing reuse.

### 6.6 Publish authority — GATE CLEARED 2026-07-17 (operator: workers implicitly trusted)
**OPERATOR DECISION (2026-07-17): workers are IMPLICITLY TRUSTED → the trusted-ACTION assumption holds (an action on
a trusted worker is trusted). The derivable-key index poisoning surface is ACCEPTED — it is no worse than the
already-writable AC/CAS the same actions can already write under the same trust model. §6.6 gate CLEARED; NO
action-isolation infra required. Documented as the trusted-action assumption (security F5).** The investigation
below is retained for the record; its "net-new action-isolation infra" is NOT pursued.
Investigation (`.claude/reviews/design-portable-rustc-incremental-v3/gate66-publish-authority.md`): the precondition
does NOT hold on the live fleet, and the naive control is insufficient. Evidence:
- AC is on 3 mTLS listeners (`:50051` public, `:50071` worker_cas, `:50072` quic), ALL writable (no `read_only`);
  `read_only` is BINARY — no worker-only-writable mode (`ac_server.rs:362-367`).
- **Build actions run as the SAME uid as the worker** (`uid=501 user`, no `setuid`/sandbox uid-drop in
  `running_actions_manager.rs`); worker mTLS key `/Users/user/Work/nativelink/tls/worker.key` is `0600 user` →
  the action IS the owner → can read it; macOS has NO mount namespace → reachable by absolute path from the action cwd.
- Egress OPEN: a process as `user` on a worker TCP-connects to buildcache `:50051`/`:50071` (verified). `x-nativelink-worker`
  is a self-asserted client header (`grpc_store.rs:1915`), NOT an authz boundary.
→ An action can read the worker key, connect to `:50071`, and `UpdateActionResult(hash(targetkey), …)` — a
fleet-global AC-poisoning write under a DERIVABLE key.
**BROADER (pre-existing) FINDING:** build actions can ALREADY poison the existing CAS/AC today (same credentials +
egress) — the index is NOT a fundamentally new capability, BUT its derivable `hash(targetkey)` key lowers the bar for
TARGETED seed poisoning (vs a content-derived Action digest), and a poisoned seed → adversarial rustc reuse → wrong
`.rlib` (security MED-1 adversarial case). **OPERATOR DECISION required:** (a) ACCEPT the existing trust posture
(actions are trusted; the index is no worse than the already-writable AC/CAS) → ship + document the assumption; OR
(b) invest in ACTION ISOLATION as net-new fleet infra — run build actions under a separate non-privileged uid
(`sandbox-exec` / a dedicated `_nljob` account) that can't read the worker index-write key AND is denied egress to
the index port via a pf anchor, PLUS a distinct `incr_seed_index` AC instance read-only on `:50051` / writable only
on the worker listener with a worker-uid-only key. (b) also closes the pre-existing AC/CAS poisoning hole (a broader
fleet hardening). Content integrity is free (CAS content-addressed); the INDEX write-authority is the surface.

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
  §7/§9; RAII lease §5; the §6.2 publish (gated-on-success; seed retention DEFERRED per §6.5) + worker out-of-band fetch §6.3; the index
  store instance behind CompletenessChecking §6.1; the §10 ActionInfo-targetkey + residency-gossip hook.

## 12. Observability
Per-branch `incr_seed_materialized` / `incr_reuse_fired` / `incr_seed_present_but_cold`; lease-contention;
`incr_index_fetch_{hit,miss,timeout,error}` / `incr_index_publish` / `incr_content_pin_{count,bytes,evict}`;
startup asserts: FIXED_PREFIX (uid/0755/machine-local), sysroot absolute path byte-identical, `link()`-to-execroot-
volume. (`seed_present_but_cold` climbing = LWW-clobber or CAS-eviction diagnostic; `index_fetch_hit` up while
`seed_materialized` flat = content-eviction.)

## 13. Effort + rollout (XL, dynamic from the start)
(1) rules_rust seed-fetch-from-index (stage-1 net-new) + the `incr_seed_index` store instance behind
CompletenessChecking (NO content-pin — deferred per §6.5). (2) NativeLink stable-path + lease + §6.2 publish/fetch +
§10 ActionInfo-targetkey + residency-gossip hook + §8 pool budget. (3) worker out-of-band injection §6.3. (4) provision
FIXED_PREFIX per-machine on the execroot volume + §12 asserts + the §6.6 egress precondition check. (5) ONE
apple-a14 crate under dynamic; measure the three-state + store counters — including the reuse HIT-RATE (§6.5) so we
know whether normal CAS LRU retains the seed. (6) widen — WATCH `seed_present_but_cold` for the multi-version-LWW +
content-eviction floors that only surface at fleet scale (§6.2/§6.5); add seed retention (prefer LRU-priority over a
hard pin, §6.5) ONLY IF the measured hit-rate shows eviction is costing reuse.

## 14. Pre-code gates (architecture already signed off) — ALL CLEARED 2026-07-17
1. ~~§6.6 publish-authority~~ **CLEARED 2026-07-17 (operator: workers implicitly trusted → trusted-action assumption
   holds; the derivable-key index poisoning surface is ACCEPTED, no worse than the already-writable AC/CAS; NO
   action-isolation infra required).**
2. ~~§6.5 CAS-content-pin sizing proof~~ **CLEARED 2026-07-17: sizing proof HOLDS at one-crate AND full-CI-widen
   worst-case (10.9 GB ≤ 20 GiB pin budget; ~135 GB ≤ 228 GiB physical) via a DISJOINT `seed_pin_class` (4 GiB
   carve-out, excluded from the FL-688 admission check, mirroring `speculative_pin`). See
   `.claude/reviews/design-portable-rustc-incremental-v3/gate65-content-pin-sizing.md`. The pin is now
   OPTIONAL-PENDING-MEASUREMENT (§6.5): the proof means IF we pin it fits — Stage 1 ships WITHOUT it (a missing seed is
   safe via §6.1/§6.4), measures the reuse hit-rate, and adds retention only if eviction is costing reuse. Durability
   (if retention is later added) depends on §10 peer-routing (caveat in §6.5).**
3. ~~Two-machine remote-`.rlib`-cache experiment~~ **DONE 2026-07-17: CONFIRMED the Bazel-declared seed churns the
   remote key (`df7bff88`≠`ff08968c`, seed-content sole cause) → §6.3 out-of-band is REQUIRED, not optional
   (regression scoped to >20 MB-incr crates = the target set).**
