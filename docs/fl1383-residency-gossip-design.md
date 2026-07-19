# FL-1383 §10 — residency-gossip: route actions to workers already holding the `-incr` seed (design, for cadre)

**Status: DESIGN — for cadre review. Wires the two dark feeds behind the ALREADY-MERGED chunk-5 placement scorer.** Feature flag-gated INERT (fires only for allowlisted portable actions that carry a `targetkey`; none on the fleet today). This is a placement OPTIMIZATION, not correctness — the feature reuses seeds fine without it (every worker fetches from the index/CAS); residency only steers an action to a worker whose LOCAL CAS already holds the seed tree, turning a cross-machine CAS fetch into a local `clonefile` materialize.

## 1. Current state (grounded against merged code)
- **Scorer — WIRED + correct.** At reserve, resolve `targetkey → seed_digest` via `incr_seed_index.peek()` (`api_worker_scheduler.rs:3448`, once per reserve, not per-candidate → no O(N²)); per candidate `has_seed_match = incr_seed_digest.is_some_and(|sd| w.cached_subtree_digests.contains(&sd))` (`:3837`), folded into the EXISTING Tier-1 gate (`has_root_match || has_subtree_match || has_seed_match`). Not a new tier.
- **Feed A setter (`record_incr_seed_residency`, `:~6203`) — exists, UNWIRED.** Body is `incr_seed_index.put(targetkey, seed_digest)` (fleet-global LWW resolution index; NOT per-worker). Only test callers (`:20171/:20222`).
- **Feed B channel (`update_cached_subtrees`, `:11463`) — live.** Applies a worker's `BlobsAvailable` subtree advertisement (`is_full_subtree_snapshot` → replace `cached_subtree_digests`; else add/remove deltas). Same channel Tier-1 input-tree affinity rides.
- **Worker advertisement source (`local_worker.rs:4497`):** the advertised subtree set = `running_actions_manager.all_subtree_digests()` — the directory-cache / held-tree Directory digests. **The `-incr` seed Directory is NOT in it:** the standalone `incr_seed_fetch::materialize_tree` writes files to the execroot and registers the seed Directory with NO directory-cache / subtree tracker (grep-confirmed — no locality/subtree/dir-cache touch in `incr_seed_fetch.rs`). **So Feed B is fully dark: the seed tree's Directory digest is advertised nowhere.**
- **Proto (`remote_execution` `BlobsAvailable*`):** `added_subtree_digests` / `removed_subtree_digests` / `is_full_subtree_snapshot` are BARE `Digest` lists — no targetkey/label channel.

## 2. The design — one gossip hook, both feeds
When a worker HOLDS a materialized `-incr` seed for a `targetkey` (it just fetched or produced it, content resident in its CAS), it advertises the pair `(targetkey, seed_dir_digest)` to the scheduler; on eviction it withdraws it. The scheduler applies BOTH feeds in one handler.

### 2a. Proto change (net-new; the load-bearing decision for the cadre)
Extend the `BlobsAvailable` message with a residency channel alongside the existing subtree deltas, snapshot-consistent with them:
```
message IncrSeedResidency { string targetkey = 1; Digest seed_dir_digest = 2; }
// in BlobsAvailable:
repeated IncrSeedResidency added_incr_seed_residency = N;
repeated IncrSeedResidency removed_incr_seed_residency = N+1;   // (or just targetkeys to remove)
// full snapshot: added_* carries ALL currently-held seeds when is_full_subtree_snapshot=true
```
(Regen via `gen_protos_tool.rs` + diff-verify per the genproto discipline; `*.pb.rs`/genproto are file-guard-blocked — hand-edit forbidden.)

### 2b. Worker side
- Track held seeds: when `fetch_and_materialize_seed` returns `Materialized` (or on publish-on-success), record `(targetkey, seed_dir_digest)` in a worker-local held-seeds set; withdraw on §8 warm-dir eviction OR when the seed content leaves the worker CAS.
- Emit `added_incr_seed_residency` on the delta advertisement; include ALL held seeds in the full-snapshot path (mirrors `added_subtree_digests`). Snapshot-consistent so it can't go dark against the periodic full replace (the chunk-5 lesson).

### 2c. Scheduler side (`update_cached_subtrees` or a sibling handler)
For each `added_incr_seed_residency (targetkey, seed_dir_digest)`:
- `worker.cached_subtree_digests.insert(seed_dir_digest)` — Feed B (existing mechanism; the scorer's membership check now hits).
- `record_incr_seed_residency(targetkey, seed_dir_digest)` — Feed A (the fleet-global resolution binding).
For `removed_*` / full-snapshot: remove the withdrawn digests from `cached_subtree_digests`; the fleet-global `incr_seed_index` binding is LWW and per-targetkey (NOT withdrawn per-worker — another worker may still hold it, and a newer publish overwrites by LWW).

### 2d. Match time — UNCHANGED (already merged)
`peek(targetkey) → seed_digest`, `cached_subtree_digests.contains(seed_digest)`, fold into Tier-1.

## 3. Invariants + failure modes
- **Staleness self-corrects.** `incr_seed_index` resolves the CURRENT seed_digest (LWW, updated on each publish). A worker advertising an OLD seed_digest simply won't match (the scorer checks the current digest) → it's just not preferred → correct (it holds a stale seed; routing elsewhere is right).
- **Per-worker residency vs fleet-global index.** `cached_subtree_digests` is per-worker (which worker holds it); `incr_seed_index` is fleet-global (targetkey→current digest). Feed A can be set by ANY holder's gossip (LWW, idempotent). Feed B is strictly per-worker.
- **Cold-safe.** If the gossip is lost/delayed, the scorer just doesn't prefer a seed-holder → the action fetches the seed from the index/CAS as today (no worse than the current dark state). Never a wrong placement (placement is a preference within capability-matched candidates).
- **Eviction lifecycle.** A worker must withdraw a seed it evicts (§8) or whose content left its CAS, else the scorer routes to a worker that will then cold-fetch (a stale-residency mis-route — still correct, just no faster than random). Tie withdrawal to the §8 eviction + CAS presence.
- **No new unbounded buffer:** held-seeds set is bounded by the §8 warm-dir count (≤ pool budget); `incr_seed_index` is the existing CAPPED LruCache.

## 4. Open questions FOR THE CADRE
1. **Combined vs separate advertisement:** ride the existing `BlobsAvailable` subtree message (one lifecycle, snapshot-consistent) vs a separate message? (Recommend combined.)
2. **Feed A source — gossip vs ingestion-resolution.** This design uses worker-gossip (the worker that holds the seed reports the binding). §10 also floated ingestion-time resolution (scheduler reads the index store once per action at ingestion). Gossip needs a proto field but is event-driven + needs no scheduler store handle; ingestion needs a scheduler→index-store read per action. Which is preferred?
3. **Is the proto/protocol change justified for a placement optimization** whose core benefit (remote reuse) is still UNVALIDATED (canary blocked on the client TLS; local reuse measured NEGATIVE)? Sequencing: build now vs. after the canary proves the core wins. (The operator directed starting now.)
4. **Withdrawal correctness:** exactly when does the worker stop advertising a seed (§8 evict / CAS-evict / execroot wiped)? Can a mis-timed withdrawal race the full-snapshot (the chunk-5 snapshot-race lesson)?
5. **Does routing-to-holder even help post-full-empty-wipe?** The seed content is in the worker's local CAS (fast clonefile) but the on-disk warm execroot is wiped each build (§7 full-empty). Confirm the local-CAS-materialize win is real vs a cross-machine fetch, and whether it's worth the wiring.

## 5. Design self-review (vs CURRENT code)
- **Existing-system-already?** The SCORER + the subtree-advertisement channel + the LruCache all exist (chunk 5 + the BlobsAvailable machinery). The net-new is ONLY the seed-residency proto field + the worker held-seeds tracking + the scheduler handler line. No redundant state.
- **Mechanism-real-not-TODO?** `update_cached_subtrees:11463`, `record_incr_seed_residency`, `all_subtree_digests:4497`, the scorer `:3837` are all real merged code. The one thing that does NOT exist and must be built: the seed-Directory advertisement (Feed B is dark, not merely untagged).
- **Bound-right-resource?** Placement preference is within capability-matched candidates (doesn't override viability/pressure gates). The held-seeds set is bounded by §8.
- **Chesterton's Fence:** the chunk-5 fix deliberately made residency come from snapshot-consistent advertisement (NOT a scheduler side-inject that races the full-snapshot). This design RESPECTS that — the gossip rides the snapshot-consistent BlobsAvailable path.
