# FL-1383 — materialize the `-incr` seed via the input-directory mechanism (design)

**Status: ⛔ REFUTED BY DESIGN CADRE (5/5, 2026-07-18) — DO NOT IMPLEMENT. Superseded by in-place 16-way parallelization of the EXISTING `materialize_tree` (keep every guard).**
**Why refuted (convergent):** (1) **distsys KILLER** — `download_to_directory` materializes via `fs::hard_link` from the CAS blob (read-only `0o555` shared inode); the `-incr` is READ-WRITE (rustc writes incremental state back into it) → hardlinking a writable seed corrupts the shared CAS blob FLEET-WIDE (no copy-fallback on the fleet). The standalone byte-COPY (`incr_seed_fetch.rs:632`) is load-bearing write-safety, NOT redundancy. (2) **auditor** — `download_to_directory` has NO clonefile/DirectoryCache (it IS the cache's miss path, hardlink-only); the win I claimed doesn't exist. (3) **operator insight** — the `-incr` is a TREE but target-UNIQUE (crate-specific dep-graph/query-cache/work-products) → cross-action DirectoryCache reuse ≈ 0 anyway. (4) **security HIGH→CRITICAL** — the `..`/absolute/symlink component-validation guards are lost → poisoned-index → execroot escape / arbitrary write (rustc writes into the seed dir). (5) **red-team/code** — lost atomic tmp-then-rename + per-blob verify; dest-dir-not-pre-created blocker; "faster" unmeasured/cold-possibly-slower. **PASSED:** the §4 hidden-side-input invariant (seed stays out of the action digest) — confirmed correct, but it guarded the wrong door. Reviews: `.claude/reviews/fl1383-seed-via-input-materialization/`. **DECISION:** keep `materialize_tree` (byte-copy + path guards + atomic swap + per-blob verify all load-bearing); the ONLY optimization is parallelizing its sequential fetch in-place (16-way, operator-directed) — in flight `a3b02fb8`.

--- ORIGINAL (REFUTED) DESIGN BELOW ---
**Status: DESIGN — for cadre review. Reworks the just-landed standalone seed-materialize (`incr_seed_fetch::materialize_tree`) to reuse the worker's input-tree materializer.** Operator-directed 2026-07-18: "the server should materialize the incremental state itself along with the rest of the inputs (via `nl_incr_targetkey`) — it can use the directory-subtree mechanisms and will be faster than the client doing it."

## 1. Problem
The landed §6.3 fetch path (`incr_seed_fetch::fetch_and_materialize_seed` → `materialize_tree`, `incr_seed_fetch.rs:270`) is a STANDALONE tree walk: it resolves the `-incr` REAPI `Tree`, then does a sequential per-`FileNode` `get_part_unchunked` + `write_file_nofollow` (`:574`). It bypasses every fast path the worker already uses to stage an action's inputs:
- **`DirectoryCache`** — an already-materialized identical subtree is re-used, not re-fetched.
- **`clonefile`/hardlink** — near-instant copy-on-write placement (APFS/ZFS) instead of byte copies.
- **level-parallel BFS** dir creation + parallel file fetch.

For a `-incr` seed (hundreds of MB, read at the start of every incremental compile), the standalone path is materially slower and duplicates logic that already exists and is battle-tested.

## 2. The existing mechanism to reuse (grounded, not aspirational)
`download_to_directory` (`running_actions_manager.rs:2803`) is the worker's input-tree materializer:
```
pub fn download_to_directory(cas_store: &FastSlowStore, filesystem_store: Pin<&FilesystemStore>,
    digest: &DigestInfo /*root Directory*/, current_directory: &str /*dest*/,
    pre_resolved_tree: Option<HashMap<DigestInfo, ProtoDirectory>>,
    server_missing_digests: Option<HashSet<DigestInfo>>, calib_input_bytes_out: Option<&AtomicU64>)
    -> BoxFuture<Result<(), Error>>
```
It resolves the tree (or takes a pre-resolved map), does level-parallel BFS dir creation + `collect_files_from_tree`, materializes files from the main CAS, and routes through the FilesystemStore (which carries the `DirectoryCache`/clonefile machinery). This is exactly what stages `input_root` today.

## 3. Proposed change
Replace `fetch_and_materialize_seed`'s standalone `materialize_tree` with a call into `download_to_directory`, materializing the `-incr` seed as an **extra input subtree**:

1. **Resolve** `nl_incr_targetkey → seed OutputDirectory.tree_digest` via the `incr_seed_index` `GetActionResult` (unchanged from today; one AC read, worker-side at input-setup time or server-side at ingestion per §10 — see §7).
2. **Convert** the REAPI `Tree` (root `Directory` + `children[]`) referenced by `tree_digest` into `(root_directory_digest, pre_resolved_tree: HashMap<DigestInfo, ProtoDirectory>)` — one decode of the `Tree` proto (already `≤10MB`-capped, `MAX_ACTION_MSG_SIZE`).
3. **Materialize** via `download_to_directory(cas_store /*main CAS holds -incr content*/, filesystem_store, &root_directory_digest, &incr_dest_path, Some(pre_resolved_tree), …)` at the declared nested `-incr` path (the §3 `seed_dest_dir` path — `<execroot>/<working_directory>/<pkg>/<label>-incr`), in the input-materialization phase, after the §7 full-empty wipe, before rustc.

The content lives on the `main` CAS (the index holds only the pointer), so `download_to_directory`'s `cas_store` (the worker's `FastSlowStore`) resolves it directly — including `DirectoryCache` reuse of an unchanged seed across builds of the same target on the same worker.

## 4. Invariant this MUST preserve (the §6 make-or-break)
**The `-incr` seed is a HIDDEN side-input: it MUST NOT enter the action's `input_root_digest` / action key.** If it did, every incremental compile would churn the `.rlib` action-cache key and we'd trade remote *caching* for remote *incrementality* — a net loss (§6, v3/v4 re-cadre). So the seed is materialized as an EXTRA subtree the worker stages alongside inputs, resolved out-of-band via `nl_incr_targetkey`; it is **not** merged into `input_root` and does not affect the action digest, GetActionResult key, or cache hit. This is the load-bearing distinction between "materialize via the input MECHANISM" (yes) and "make it an input" (no).

## 5. What's kept vs. removed
**Kept (unchanged):** the index resolution (`GetActionResult` on `incr_seed_index`); the §2/§3 targetkey contract (`-incr` exclusion, nested wd-aware `seed_dest_dir` path); the cold-fallback semantics (index miss / CAS-NotFound-via-CompletenessChecking / collision / timeout → cold, never a wrong `.rlib`); the overall bounded deadline; the publish/index-write half (worker writes the produced `-incr`→index on success — untouched, that's the output side); the §8 warm-dir eviction; the §12 counters.
**Removed:** `incr_seed_fetch::materialize_tree` + its per-blob `get_part_unchunked`/`write_file_nofollow` walk (superseded by `download_to_directory`).

## 6. process_wrapper / client implication (resolves the 2026-07-18 thread)
With the `-incr` staged as a materialized subtree before the command runs, the in-action `process_wrapper` has nothing to do on the remote branch — the seed is simply *present*, like the source inputs. The client's `FL_INCR_TOOL` local path should skip when the seed is already present (seed-present check), eliminating the ENOENT and the latent seed-clobber. No env signal strictly required; the worker MAY additionally set `NL_PORTABLE_INCR_SEEDED=1` as an explicit belt-and-suspenders (open, §9).

## 7. Open design points for the cadre
1. **Resolution location:** worker-side at input-setup (co-located with materialization, my lean) vs. server-side at ingestion (§10 "route targetkey via ActionInfo at ingestion, not match-time store I/O"). Ingestion resolution adds one AC read per allowlisted action at ingestion (once, not hot-loop). Worker-side keeps the index client on the worker (already there). Which?
2. **`DirectoryCache` semantics for a cache-of-a-cache:** the `-incr` subtree flowing through `DirectoryCache` means the seed itself gets dir-cached. Is that correct (great — same-worker warm reuse for free, subsumes the deferred §7 warm-preserve) or does it risk the dir-cache serving a STALE `-incr` (the index advanced to a newer seed)? The index resolves the CURRENT `tree_digest`; `DirectoryCache` is keyed by digest, so a newer seed = a different digest = a cache miss = re-materialize. So staleness is impossible (digest-keyed). Confirm.
3. **Collision guard placement:** today the collision guard is `OutputDirectory.path != targetkey.primary_output → cold` (path-based) + per-blob blake3 (in `materialize_tree`). `download_to_directory` verifies digests structurally (tree/blob digests). The path-collision check moves to the resolve step (before calling `download_to_directory`); the per-blob integrity is covered by `download_to_directory`'s digest-addressed fetch (VerifyStore/CompletenessChecking on the CAS chain). Confirm no integrity gap.
4. **`download_to_directory` on a NON-input dest under FIXED_PREFIX:** it's currently only called for the input_root under the execroot. Calling it for the nested `-incr` path (also under the execroot) — any assumption it makes about being the whole input_root vs. a subtree? (e.g., does it clear the dest first, or the `calib_input_bytes_out` P-A accounting?) Must be a pure "materialize this tree at this path," additive.
5. **EXDEV / clonefile for the `-incr`:** `download_to_directory` clonefiles from CAS→execroot. The §12 EXDEV probe (FIXED_PREFIX co-located with CAS) already gates portable actions; confirm the seed materialization inherits it (same execroot volume).

## 8. Test plan
- Unit/integration: a portable action with a pre-seeded index → `download_to_directory` materializes the `-incr` at the nested declared path (mutation: wrong dest / skipped call → RED); the seed does NOT appear in `input_root_digest` / the action key (mutation: merged-into-input → RED); cold-fallback on index miss / collision / CAS-NotFound (each → cold, seed absent, build proceeds); `DirectoryCache` reuse across two builds of the same target (2nd build materializes from cache, mutation: cache bypass still correct-but-slower).
- Bench: seed-materialize latency old (`materialize_tree`) vs new (`download_to_directory`) for a ~200 MB `-incr` (expect the clonefile/dir-cache win the operator predicts).

## 9. Design-self-review (vs. CURRENT code, before cadre)
- **Existing-system-already?** YES — `download_to_directory` already does fast tree materialization; the landed `materialize_tree` re-invented it. This design deletes the redundant path. (The whole point.)
- **Mechanism-real-not-TODO?** `download_to_directory:2803` is real + in the production input path. ✓
- **A∧B reachable?** trigger (portable action + resolved targetkey) ∧ mechanism (`download_to_directory` for the `-incr`) — both live on the worker. ✓
- **Bound-right-resource / math?** N/A (no cost model).
- **Chesterton's Fence:** `materialize_tree` was built standalone in chunk 3 because the fetch was scoped before the "reuse the input path" insight; no invariant depends on it being standalone. `git log -S materialize_tree` = chunk 3 only.
- **Load-bearing risk:** #4 (hidden-side-input) is the make-or-break; #7.2 (dir-cache staleness) resolved by digest-keying; #7.3 (integrity) must be confirmed no-gap.
