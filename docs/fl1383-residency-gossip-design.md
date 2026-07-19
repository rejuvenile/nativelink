# FL-1383 §10 — residency-aware placement (design v2, cadre-reworked, IMPLEMENTATION-GATED ON MEASUREMENT)

**Status: DESIGN v2 — reworked per the 5/5 design cadre (2026-07-18, reviews in `.claude/reviews/residency-gossip-design/`). NOT approved-to-implement: the build is GATED on a measurement (see §6). v1 ("gossip a new `BlobsAvailable.IncrSeedResidency` proto field") is SUPERSEDED — the cadre converged on a proto-free shape and caught a hard correctness bug + a mischaracterized perf claim.**

Goal (unchanged): bias placement so an action carrying a `targetkey` prefers a worker whose local CAS already holds that target's `-incr` seed blobs, turning a cross-machine seed fetch into a **local fast-tier read**. Placement OPTIMIZATION, not correctness (the worker resolves + fetches the seed authoritatively at execution regardless).

## 1. What the cadre corrected in v1 (the reason for v2)
- **[code-reviewer BLOCK-1 — hard correctness bug]** The scorer checks `cached_subtree_digests.contains(seed_digest)`, and `cached_subtree_digests` holds REAPI **`Directory`** digests. But the seed exposes only `SeedOutcome::Materialized { tree_digest }` = the **`Tree` proto** digest — different bytes → the membership check **could never match, even fully wired.** Both feeds MUST key on the seed's **root `Directory` digest (`Tree.root`)**, never the Tree-proto digest.
- **[red-team + assumption-auditor — REFUTED perf claim]** v1 said "local clonefile materialize." FALSE: `materialize_tree` (`incr_seed_fetch.rs:677/746`) does `get_part_unchunked` then a full byte-copy `write_file_nofollow` — **no clonefile, no hardlink** (that variant was cadre-REFUTED, `a6a41300`; the seed is read-write so it cannot share a clonefile source). The write is paid identically local or remote; the ONLY saving is **local fast-tier blob-read vs cross-machine GrpcStore fetch**. (My earlier framing — to the operator too — was wrong.)
- **[distsys + code-reviewer + red-team — DROP THE PROTO FIELD]** A net-new `BlobsAvailable` residency field is ~3,000 lines of permanent wire surface (2 proto messages + `blobs_available_chunking.rs` + `blobs_available_accumulator.rs` + `worker_api_server` dispatch) — and unnecessary. Both feeds have a proto-free path.
- **[distsys — Feed A gossip poisons; is non-authoritative]** Worker-gossip writing the fleet-global LWW resolution cache lets a stale-but-still-held seed's snapshot LWW-clobber the scheduler's resolution → diverges from the store the worker authoritatively resolves from at execution. Feed A must be **ingestion-resolved from that same authoritative store**.

## 2. Reworked mechanism (proto-free)
The already-merged scorer is unchanged (`api_worker_scheduler.rs:3448` peek + `:3837` Tier-1 fold). Both feeds change:
- **Feed A — resolve `targetkey → seed ROOT-Directory digest` at INGESTION.** Where the carrier `targetkey` is read (server ingestion / `execution_server`), read the authoritative `incr_seed_index_store` ONCE (`GetActionResult(hash(targetkey))` → the seed `OutputDirectory.tree_digest`; then take the referenced Tree's **root Directory digest**), off the hot match loop. Thread it into `ActionInfo` (or populate the scheduler LruCache from ingestion). No gossip → no poisoning; scheduler and worker resolve from the SAME store → agree by construction.
- **Feed B — register the seed's root Directory digest in the worker's SUBTREE TRACKER at materialize.** `materialize_tree` currently registers nowhere (`all_subtree_digests()` reads only `directory_cache subtree_refcount`; grep-confirmed dark). Register the materialized seed's root Directory digest into that tracker → it rides the EXISTING `BlobsAvailable` subtree channel → lands in `cached_subtree_digests`. Withdrawal = ordinary subtree eviction the existing channel already handles snapshot-consistently → **the v1 withdrawal-race (Q4) dissolves.** Tie residency to **CAS-content presence** (what actually enables the local read), not the §8 warm-dir execroot lifecycle.
- **Match time — UNCHANGED** (already merged), now keyed on the Directory digest that both feeds agree on.

Net-new code: (1) ingestion-time resolve (one AC read per allowlisted action) + (2) the worker registering the seed Directory in the subtree tracker + the Directory-digest fix. Zero proto/chunker/accumulator/worker_api change.

## 3. The residual accuracy gap (why §6 measurement gates this)
Even reworked, the scorer routes on ONE root-Directory digest, but materialize speed depends on the **N individual file blobs**, which the worker `FilesystemStore` fast tier evicts INDEPENDENTLY of any Directory-digest presence. So "worker advertises the seed Directory" ≈ "worker fetched this seed recently" — a PROBABILISTIC proxy for "still holds the N blobs," decaying with fast-tier churn. Verified (red-team): fetched blobs DO survive the §7 wipe (it hits the execroot, not the CAS tier), subject to fast-tier LRU — so the proxy CAN be useful, but its hit-rate is unmeasured.

## 4. Correctness/safety (cadre-confirmed)
- Placement-preference-only: `has_seed_match` OR'd into Tier-1, still `&& worker_is_viable_gated`, candidates from `capability_index` → a bad/stale hint can only mis-PREFER within viable candidates, never mis-route (distsys).
- Cold-safe: worker re-resolves + fetches authoritatively at execution → a wrong/missed hint is at worst a preference that then cold-fetches; never a wrong `.rlib` (all four fetch guards intact — blake3-collision, per-file verify, symlink-reject, O_NOFOLLOW|O_EXCL; security APPROVE).
- Ingestion Feed A is non-poisonable (reads the authoritative store, not gossip).
- Bounded: `incr_seed_index` = existing CAPPED `LruCache(4096)`; Feed B rides the existing bounded subtree set.

## 5. Security (cadre-confirmed + one carry-over)
Worst case = misplacement + cold build, placement-only, no new trust boundary. Since Feed A is now ingestion-resolved (targetkey comes from the already-validated carrier, not worker gossip), the v1 "validate 64-hex targetkey before LruCache insert" flood-DoS **is moot** (no worker-supplied targetkey crosses into the index). Keep the 64-hex validation on the carrier read (already there).

## 6. ⛔ MEASUREMENT GATE — build only if these numbers clear (operator chose measure-first, 2026-07-18)
The routing win = `read_delta × fast_tier_hit_rate`, and the whole feature's first-order benefit is itself unvalidated (canary blocked on client TLS; local reuse measured NEGATIVE). So:
1. **Instrument `materialize_tree` (observe-only, safe to land now):** per materialize, record read-time vs write-time, and whether each blob READ was a worker fast-tier HIT or a slow-tier (GrpcStore→server) FETCH. Registered §12 counters. This makes the gate numbers fall out of the first real materializations — no bespoke bench.
2. **Gate thresholds (all required before building the gossip):**
   - (a) **Core reuse is net-positive** — the canary shows `incr_reuse_fired > 0` AND a real build-time win (else the whole feature shelves; routing a losing feature is pointless).
   - (b) **The read is a meaningful fraction of materialize** — if the byte-copy WRITE (always paid) dominates and the READ is small, the max routing saving is small → don't build.
   - (c) **Fast-tier survival hit-rate is high enough** — the fraction of would-be routings where the target's seed blobs are still fast-tier-resident at the next build must be high enough that `read_delta × hit_rate` beats the wiring cost.
3. **Back-of-envelope (NOT a substitute for the measurement, flagged for context):** 10GbE LAN ≈ ~160ms to move 200MB; local NVMe read ≈ tens of ms; so `read_delta` is plausibly ~O(100ms) against a materialize that also pays a ~O(100ms) write + the (Bazel-measured-negative) rustc reload. This *suggests* a marginal win — which is exactly why we measure rather than assume.

**Sequencing:** land the instrumentation (§6.1) now if desired; the (proto-free) gossip implementation waits on §6.2 clearing from real canary data. This lets "start now" proceed with zero permanent protocol surface and zero speculative build.
