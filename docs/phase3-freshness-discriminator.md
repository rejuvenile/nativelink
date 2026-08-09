# Phase-3 DOWN-lowering: a real freshness discriminator — design and verdict

**Status:** DESIGN ONLY. No production code written or proposed for the discriminator.
**Date:** 2026-08-09. **Base:** local `main` `8fc23b8b`.
**Supersedes:** `.claude/audits/phase3-staleness-gate-diagnosis-2026-08-08.md` §8 option menu
(Options A/C/D), which was derived from the misreading the `a18fa12a` cadre refuted.

---

## Verdict

**Build no freshness discriminator. Do not remove the gate either. Land the instrumentation
fix, and stop the discriminator workstream at its own pre-declared gate.**

Four independent measurements, each taken for this document against the live fleet and the live
profile snapshot, converge:

1. **The workstream's own pre-declared stop condition is now met.** The measured
   staleness-suppression rate is **2.20 %** of dispatches over a 5 h 45 m window at 13 h 36 m →
   19 h 22 m uptime on one continuous PID, against the diagnosis' own pre-declared
   *"< 2 % ⇒ Option D, stop"*. The series is **51.8 % → 6.68 % → 2.20 %**. The brief's open
   question — *is 6.7 % an asymptote or still decaying?* — is now **answered: still decaying**
   (§2). 6.7 % was a mid-decay reading, not a steady state.
2. **The hazard the gate names is not present in the data.** Across all 450 mature fine keys in
   the live snapshot, **zero** show a regime change in their retained window; three independent
   stationarity contrasts are all null; and the *rarely-run* population — the one a pure age
   threshold would refuse forever — shows the **least** drift of any stratum (§3).
3. **The Fence's one observed save is a tail event, not a staleness event.** The
   `//client:fl_cli_test|RustcLink` window is tight and stationary (18 of 20 samples within
   ±3.4 %) with one 1.45× outlier that IS the recorded "actual". Any *correct* freshness
   discriminator rates that key FRESH and lets DOWN apply. The save is not recoverable by any
   freshness test — only by a quantile/margin change (§4).
4. **For two thirds of the population the profile statistic is not what gets reserved anyway.**
   **64.5 % of trusted-fine-key folds** are *floor-pinned*: `p95 < ceil(declared/factor)`, so
   `phase3_down_effective_kb` returns the profile-INDEPENDENT floor and the p95's value — fresh,
   stale, or wrong — never reaches the ledger. A discriminator on evidence the clamp then
   discards is protecting nothing about that evidence (§5).

The anti-stale-OOM property is **preserved**, because nothing is removed. What this document
does remove is the *belief* that the property is delivered by the staleness gate: on 64.5 % of
DOWN folds it is delivered by the floor, which is stale-proof by construction.

**One thing must still be built, and it is not a discriminator:** the declared falsifier cannot
fire, today, on `main`, before any Option A — `dispatch_prediction` stashes `lookup_tiered`'s
p95 while the ledger stands `clamp(p95, floor, declared)`. On the median floor-pinned key the
real reservation is **9.43×** the number the falsifier scores (§7). Fix that first, regardless
of what else happens here.

---

## 1. What the gate is, at `main` `8fc23b8b`

Read at the site, not relayed:

- `Agg::down_lowering_trusted` (`nativelink-scheduler/src/resource_profile.rs:220-225`) —
  `if !self.loaded { return true; } self.fresh_since_load && loaded_age_secs.is_none_or(|age| age < max_age_secs)`.
- `ProfileMap::down_lowering_reserve_kb` (`:860-879`) — fine-if-`>=K` else coarse-if-`>=K`,
  then the gate on the chosen agg, then `Some(chosen.memory_p95_kb())`. **Options A and C were
  never merged**; `main` is the pre-`a18fa12a` shape.
- Sole production caller: `api_worker_scheduler.rs:4106-4113`, inside the DOWN branch of
  `phase3_compute_effective_action_info`.
- `loaded_snapshot_age_secs` (`resource_profile.rs:558`) is written once at
  `load_entries:796` and read once at `:875`. It is the snapshot FILE's age at process start,
  frozen for the process lifetime. Against a deployed `resource_profile_persist_max_age_secs:
  604800` and a `resource_profile_persist_interval_secs: 300`, the age term has never bound.
- Therefore the **sole binding term is `fresh_since_load`**, which `Agg::fold` (`:203-212`)
  sets on the first fold since load. A fold requires a worker `resource_usage` report, which is
  a uniform 1/16 digest-hash sample (`CALIB_SAMPLE_PERIOD = 16`, declaration line
  `nativelink-worker/src/running_actions_manager.rs:213`). Hence ~16 dispatches of that exact
  key before the gate opens.

**Deployed config, verified live** (`ssh buildcache sudo cat /srv/nativelink/buildcache-native.json5`,
2026-08-09): `phase3_raise_enabled: true` (`:396`), `phase3_down_overcommit_enabled: true`
(`:397`), `phase3_overcommit_max_factor: 4.0` (`:404`), `resource_profile_persist_path` set
(`:410`), `resource_profile_persist_interval_secs: 300` (`:411`),
`resource_profile_persist_max_age_secs: 604800` (`:412`), `phase3_reserve_undeclared_enabled`
absent ⇒ `default_true`. Both documented kill-switches are OFF (factor is 4.0, not 1.0;
`down_overcommit_enabled` is true), so the `arm_unmodified` counter's own documented
"meaning-flip" failure mode (`api_worker_scheduler.rs:995-1001`) does not apply to any reading
below.

---

## 2. The size of the prize: 2.20 %, and still falling

Four scrapes, one continuous process. `MainPID=3001166`, `NRestarts=0`,
`ActiveEnterTimestamp=Sat 2026-08-08 12:39:52 PDT`. T0/T1/T2 read from the archived primary
scrape files, **not** from any prior agent's summary; T3 taken for this document.

| | wall clock | uptime | `find_worker_hits` | `arm_unmodified` | `arm_down` |
|---|---|---|---|---|---|
| T0 | 2026-08-08 12:46:31 PDT | 6 m | 749 | 414 | 29 |
| T1 | 2026-08-08 13:30:08 PDT | 50 m | 1,959 | 1,041 | 231 |
| T2 | 2026-08-09 02:16 PDT | 13 h 36 m | 35,016 | 3,248 | 7,978 |
| **T3** | **2026-08-09 08:01:27 PDT** | **19 h 22 m** | **47,129** | **3,514** | **15,597** |
| **T4** | **2026-08-09 08:16:03 PDT** | **19 h 36 m** | **48,472** | **3,515** | **15,731** |

Arm partition exact at T3: `15,597 + 193 + 27,825 + 3,514 = 47,129 = find_worker_hits`
and at T4: `15,731 + 193 + 29,033 + 3,515 = 48,472 = find_worker_hits`
(the counter's doc mandates a ±1 tolerance; the observed equality is exact). The observe-path
partition is also exact: `d(lookup_fine) 7,750 + d(lookup_coarse) 4,363 + d(lookup_skip) 0 =
12,113 = d(hits)`.

**Deltas — the only valid reading form** (`api_worker_scheduler.rs:971-988` documents why a
cumulative ratio measures uptime, not the mechanism):

| window | uptime span | `d(unmod)/d(hits)` | `d(unmod)/(d(unmod)+d(down))` |
|---|---|---|---|
| W1 | 6 m → 50 m | **51.82 %** (627/1,210) | 75.6 % (627/829) |
| W2 | 50 m → 13 h 36 m | **6.68 %** (2,207/33,057) | 22.17 % (2,207/9,954) |
| **W3** | **13 h 36 m → 19 h 22 m** | **2.196 %** (266/12,113) | **3.373 %** (266/7,885) |
| W4 | 19 h 22 m → 19 h 36 m | 0.074 % (1/1,343) | 0.74 % (1/135) |

**W3 is the headline; W4 is corroboration only, and its limits must travel with it.** W4 spans
14 m 36 s and contains a single `arm_unmodified` dispatch — the Poisson 95 % upper bound on one
observed event puts the true rate below ~0.35 %, but the window is also 90.0 % INJECT
(1,208/1,343), so its DOWN-territory denominator is only 135 and the 0.74 % figure carries a
very wide interval. It is reported because it is a same-PID reading taken 15 minutes after T3
and it moves in the same direction; it is not offered as an independent measurement.

W3 is not a lull artifact: 2,107 dispatches/h vs W2's 2,589/h. And the workload mix moved
**toward** the population where the gate can act — W3 is DOWN-dominated (62.9 % of dispatches)
where W2 was INJECT-dominated (69.5 %) — so the DOWN-territory normalisation (3.37 %) is the
fairer comparison and it fell nearly 7×.

Two honesty notes, both cutting the same way:

- `arm_unmodified` is an **UPPER BOUND** on staleness suppression by its own doc-comment
  (`:961-969`): no-baggage and no-trusted-profile also land there. `d(lookup_skip) = 0` in W3
  removes one of those, but the "`p95 == declared` exactly" exit is not separable from any
  counter. So **the true staleness-suppression rate in W3 is ≤ 2.196 %.**
- The rendered `#[metric(help)]` string (`:1005`) still says *"this is the DOWN-staleness-
  suppressed dispatch count"*, contradicting the doc-comment eight lines above. Pre-existing;
  flagged by the `a18fa12a` cadre; re-confirmed here. It does not affect the reading (the bound
  is one-sided in the safe direction) but it should be corrected.

**Conclusion.** The brief's stated benefit (~6.7 %) was a mid-decay reading. At 19 h 22 m the
benefit is ≤ 2.20 % and falling monotonically. The diagnosis' pre-declared kill threshold was
*"< 2 % at ≥ 24 h uptime ⇒ stop"*; we are at 2.196 % at 19 h 22 m with a strictly decreasing
series. **No discriminator design whose cost exceeds "a few lines and a config field" can be
justified against this benefit, and every candidate below costs more than that.**

---

## 3. Design question 1 — is data age even the right signal?

The hazard the diagnosis names is **behaviour change**: a new toolchain, new flags, new code
shifts an action's memory footprint, and the retained window describes the old behaviour. Time
is only a correlate. So the honest test is not *"how old is the evidence"* but *"is there
evidence of a shift"*. That is directly measurable from the retained windows.

**Method.** The window is a `VecDeque` with `push_back`/`pop_front`
(`resource_profile.rs:117-122`), so **sample order is completion order** and the persisted
`memory_samples` vector is oldest-first. 450 mature (`count >= 20`) fine keys with a full
20-sample window, from the live snapshot `snapshot_unix_secs = 1786287612`
(2026-08-09T15:00:12Z), 901,418 bytes, 2,130 entries decoded with **zero trailing bytes** and
an entry count matching the live `profile_keys_tracked = 2130` gauge exactly.

**Three pre-registered stationarity contrasts, family-wise corrected (α = 0.0167):**

| contrast | observed | null expectation | statistic | verdict |
|---|---|---|---|---|
| median(newer 10) / median(older 10) > 1 | 243 / 450 = **53.1 %** | 50 % | z = 1.31, p ≈ 0.19 | **null** |
| newest sample > max of preceding 19 | 27 / 458 = **5.90 %** | 5.00 % (exchangeable) | z = 0.88, p ≈ 0.38 | **null** |
| position of window MAX, by quartile | [118, 109, 98, 133] | 114.5 each | χ² = 5.74, df = 3, p ≈ 0.13 | **null** |

Median drift ratio = **1.003**. And the sharpest single number:

> **Keys with a clean upward level step (`min(newest 10) > max(oldest 10)`): 0 of 450.**
> Clean downward step: 0 of 450.

**Not one key in the live map is mid-regime-change.** A one-sided rule-of-three bound puts the
instantaneous prevalence of mid-transition keys below **0.67 %**. The feared event — a
`rules_rust` bump lifting `RustcLink` fleet-wide — is a *correlated* event that would step many
keys at once; it would have been the most visible thing in this census. It is not there.

**What this census can and cannot see.** The window retains only the last 20 folds, so a
regime change that *completed* more than 20 folds ago is invisible — the window shows only the
new regime, and is by then correctly fresh. So these contrasts bound *in-flight* transitions,
which is exactly the population a discriminator would need to catch. They do not prove
transitions never happen; they prove that at this instant none is in flight and that the
per-key rate is low enough that none of 450 keys is caught in one.

### 3a. And a shift *detector* has no power at this window size

The candidate the brief raises — *"do recent samples disagree with the retained window?"* — was
designed and then costed against the measured null distribution of its own statistic. With a
20-sample window the best available split is 10-vs-10. The **null** distribution of
median(newer)/median(older) across the 450 real keys:

| p50 | p75 | p90 | p95 | **p99** | max |
|---|---|---|---|---|---|
| 1.002 | 1.053 | 1.178 | 1.400 | **5.935** | 96.3 |

A per-key detector calibrated to a **1 % false-positive rate per evaluation** must therefore
not fire below **5.93×**. The scenario the design exists for — a ~30 % toolchain regression —
sits at roughly the **78th percentile of the null**, i.e. indistinguishable from ordinary
dispersion. Loosening the threshold to catch 1.3× would fire on ~7 % of keys *when nothing has
changed*; at 47 k dispatches that is thousands of spurious DOWN refusals, which is the original
bug at scale.

**D3 (distribution-shift test) is REJECTED on measured power, not on principle.** The estimator
does not have the resolution the hazard requires, and widening the window to get resolution
costs exactly the memory footprint the 20-slot cap exists to bound.

---

## 4. The Fence's one observed save is a tail event

`//client:fl_cli_test|RustcLink`, decoded from the live snapshot: `sample_count = 70`, full
20-sample window, **oldest-first**:

```
1079844 1052611 1062788 1034707 1022771 1092388 1044931 1058532 1084179 1072852
1030724 1045315 1037219 1582069 1069060 1047364 1080116 1064020 1064979 1187572
```

p50 = 1,062,788 · p95 = 1,187,572 · max = 1,582,069.

- **18 of 20 samples lie in [1,022,771 … 1,092,388]** — a ±3.4 % band.
- The recorded "predicted" **1,092,388** is present in the window: it was the cluster ceiling,
  and at n ≤ 19 this estimator's p95 *is* the window max (see §4a).
- The recorded "actual" **1,582,069** is *also* present in the window — it is the single
  outlier, and it subsequently folded in.

So the sequence was: a tight stationary distribution → the p95 equalled its ceiling → one
completion peaked at 1.45× → the peak folded in and is now the window max. **That is a tail
draw from a stationary distribution.** There is no shift, no drift, no staleness.

The consequence is decisive and it cuts against every candidate:

> Any *correct* freshness discriminator — evidence-age, build-identity, or shift-detection —
> rates this key FRESH. It is live, it re-folds, its old samples agree with its new ones, and
> its build configuration never changed. DOWN would apply and the 489,681 KB under-reservation
> would happen anyway.

The gate saved that dispatch **by accident**: it was closed for a *liveness* reason (the key had
not yet had a 1-in-16 sampled completion) while the thing it happened to block was a
*statistical* under-prediction. The `a18fa12a` red-team said this in prose; the snapshot proves
it in data.

**Answer to "what happens to that case?"** Under every option in this document, including the
recommended one, that dispatch is not protected by a freshness mechanism. Today it is protected
only while its key is un-refolded — a state that decays. What actually bounds it is
`declared/factor`: `clamp(1,092,388, 500,000, 2,000,000) = 1,092,388` against an actual of
1,582,069, so the floor does **not** cover it either. That is a real, unaddressed residual, and
it belongs to the *quantile*, not to freshness (§8).

### 4a. The estimator is already near-maximal

`SampleWindow::quantile(19, 20)` computes `rank = clamp(ceil(n*19/20), 1, n)`
(`resource_profile.rs:128-138`). For **n ≤ 19 that is `rank = n`, i.e. the window MAX**; at
n = 20 it is the second-largest. So "p95" is a near-max statistic over the last ≤20 completions —
and it still under-predicts:

- Production, mature keys only (`prior_samples >= PROFILE_MIN_SAMPLES`, `:2240-2242`):
  `accuracy_predicted_under 279 / (279 + covered 2,601)` = **9.69 %**.
- Within-window walk-forward on the live snapshot, where every sample is drawn from the same
  recent window so staleness is **zero by construction**: 1,133 unders in 8,702 trials =
  **13.02 %**, with under-magnitude p90 = 2.22× and a tail to 228×.

**~10 % of mature dispatches see a peak above everything observed in that key's last 20
completions, with no staleness involved.** That is the under-reservation rate a perfect
freshness oracle would leave untouched.

---

## 5. Design question 4 — does the existing system already provide this?

Grepped before designing. Three findings, one of them load-bearing.

### 5a. The variance-adaptive margin the gate was designed to complement was deleted

`git log -S fresh_since_load -- nativelink-scheduler/src/resource_profile.rs` returns exactly
two commits: `173ac171` (origin) and `d76de92c`.

`173ac171` introduced the gate together with persistence. Its own doc-comment states the
rationale (verbatim, `git show 173ac171`):

> *"Only meaningful when `loaded` — a shifted distribution's fresh samples raise variance
> (**widening the DOWN margin**) AND flip this, so DOWN begins trusting the key for lowering."*

At that commit DOWN reserved **`p50 × (1 + margin(variance, tier))`** — the deployed config's
own comment still says so (`buildcache-native.json5:379`). The gate was the *second* of two coupled
protections against distribution shift; the *first* was a reservation that automatically widened
when the distribution spread.

`d76de92c` (#2497) replaced that estimator with a flat window p95 and **deleted the margin**.
`phase3_down_effective_kb`'s doc-comment says so explicitly (`api_worker_scheduler.rs:2113-2116`):
*"This SUPERSEDES the earlier `p50 × (1 + margin(variance, tier))` approximation … the
margin/tier machinery that approximated a high percentile is gone."*

**Chesterton's Fence verdict:** the gate is the surviving half of a pair. The half that actually
adapted to distribution shift was removed. Adding a *third* mechanism to re-approximate what the
deleted margin did — which is what every freshness discriminator amounts to — is the wrong
repair; the direct repair is to reconsider the estimator (§8).

### 5b. The variance signal is still computed, still carried, and consumed by nobody

Searched by interface, not by one grep: every producer, every field name, every `TieredTail`
destructuring site, workspace-wide.

- Produced: `Agg::memory_variance_ratio_x100` (`resource_profile.rs:274`), computed on **every**
  `peek_tier_stats` (`:743`) and carried in both `TieredTail::Trusted` and the internal
  `TierStats`.
- Consumed outside `resource_profile.rs`: **nothing**. Every `TieredTail::Trusted` binding in
  `api_worker_scheduler.rs` uses `{ p95_kb, .. }` or `{ tier, p95_kb, samples, .. }`. The only
  cross-file `variance` reference is the observability gauge `profile_high_variance_keys`
  (`:776`, `:1526`, `:7524`).

So a per-key dispersion signal is already implemented, already on the hot path, already free —
and dropped on the floor by the reservation. It is the one existing mechanism that speaks to
the hazard §4 actually measured.

### 5c. `fresh_since_load` proves liveness, and the observe path proves nothing extra

Re-derived, not relayed: `Agg::fold` sets `fresh_since_load = true` on the **first** fold while
`SampleWindow::record` evicts one of 20, so at the instant the gate opens the released p95 is
19 pre-restart samples + 1 fresh. The gate delivers 1/20 of the freshness it charges a ~16-
dispatch warm-up for. The observe path (`observe_inject_counterfactual`, the completion fold,
`lookup_tiered`) carries no timestamp, no identity, and no freshness state that the gate does
not already read. **Nothing in the existing system supplies a freshness signal that is not
already used.**

---

## 6. The candidates, designed and judged

Each is specified far enough to be costed, then judged on measured evidence. §3's contradiction
check (trigger ∧ precondition-to-act, against the **deployed** config) is applied to each.

### D1 — evidence age: persist per-sample or per-agg timestamps

**Design.** Bump `SNAPSHOT_VERSION` 2 → 3. Add either (a) `newest_sample_unix_secs: u64` per
`ProfileEntrySnapshot`, or (b) `memory_sample_unix_secs: Vec<u64>` parallel to `memory_samples`.
Add the matching field(s) to `Agg`, stamped in `fold`. `down_lowering_trusted` compares
`now − newest_sample_unix_secs` against a new `phase3_down_max_evidence_age_secs`, default ON.
Kill-switch `0` ⇒ the clause is unsatisfiable for an unsigned age ⇒ exact revert to
`fresh_since_load` alone. All four dimensions fold together (`Agg::fold:203-212`), so one
timestamp series covers all of them.

**This is the discriminator the `a18fa12a` auditor asked for** (*"if the goal is 'trust recent
evidence', the discriminator has to be evidence age (per-agg last-fold timestamp)"*). It is
correct, cheap, and it measures what its name says. **It is still rejected.**

**Contradiction check.** Trigger (an agg whose newest sample is older than T) and
precondition-to-act (DOWN arm: declared action, trusted `>=K` profile, `p95 <= declared`) are
simultaneously reachable on the deployed config — 450 mature fine keys, 28.9 % with lifetime
count in [20,40). Unlike Option C, **D1 is not dark.** It passes the check.

**Why it is rejected — the rarely-run trap, and it is not solvable in this direction (design
question 2).**

- 130 of 450 mature fine keys (**28.9 %**, holding **14.9 %** of mature folds) have a lifetime
  fold count in [20,40). At the 1/16 sampling period those keys have been *dispatched* on the
  order of 320–640 times over the map's entire persisted history — which spans arbitrarily many
  restarts, because the window is carried forward by the snapshot.
- For such a key, "evidence age" is *legitimately* days-to-weeks old, and it will never be
  otherwise. Any threshold T short enough to exclude a stale toolchain excludes the rarely-run
  population **permanently**. That is the exact bug this workstream set out to fix, in a new
  costume — as the brief anticipates.
- The escape hatch a designer reaches for is "old *because rarely run* is fine; old *because the
  world changed* is not". **D1 cannot distinguish those** — age is identical in both cases. The
  only thing that could distinguish them is a shift test (D3, no power, §3a) or an identity
  stamp (D2, below).
- And the decisive number: **the rarely-run stratum shows the LEAST evidence of drift.** Split
  by lifetime fold count, "newest sample exceeds max of preceding 19":

  | stratum | n | drift ratio p50 | newer-heavier | newest > max19 | clean step |
  |---|---|---|---|---|---|
  | rarely-run [20,40) | 130 | 1.020 | 56.9 % | **2.3 %** | 0 |
  | medium [40,80) | 288 | 1.000 | 50.3 % | 6.9 % | 0 |
  | hot [80,∞) | 32 | 1.025 | 56.2 % | 9.4 % | 0 |

  The rarely-run keys are *below* the 5 % exchangeable expectation. D1 would impose its entire
  permanent cost on the stratum with the weakest case for paying it.

**Verdict: REJECTED.** Correct mechanism, correct semantics, wrong target — and its permanent
cost falls on the population the workstream exists to serve.

### D2 — build identity: stamp `configuration_id` alongside the samples

**Design.** REAPI `RequestMetadata` carries `configuration_id` (tag 7) and `tool_details`
(tag 1). NativeLink already carries the **whole** `RequestMetadata` proto in
`OriginMetadata::bazel_metadata` (`nativelink-util/src/origin_event.rs:155`), from which
`resource_profile_keys` (`api_worker_scheduler.rs:2278-2291`) currently reads only `target_id`
and `action_mnemonic`. So the identity is already at both the dispatch site and the fold site
at **zero protocol cost**. Design: stamp a bounded set (say ≤4, LRU) of recently-seen
`configuration_id`s per `Agg`, persist it (`SNAPSHOT_VERSION` 3), and refuse DOWN when the
dispatching action's `configuration_id` is absent from the set.

**Availability — CONFIRMED, not assumed.** The REAPI comment explicitly disclaims equality
guarantees (*"no expectation that this value will have any particular structure, or equality
across invocations"*), so this had to be checked against the actual client. Disassembled from
the deployed Bazel's own `A-server.jar` (`build-label.txt: 9.2.0-fl.825a5b2492`, matching
`~/fl/.bazeliskrc: 9.2.0`):

```
com/google/devtools/build/lib/remote/util/TracingMetadataUtils.buildMetadata
  59: invokevirtual ActionOwner.getConfigurationChecksum:()Ljava/lang/String;
  ...
  59: invokevirtual RequestMetadata$Builder.setActionMnemonic:(...)   <- proven populated
  72: invokevirtual RequestMetadata$Builder.setTargetId:(...)         <- proven populated
  85: invokevirtual RequestMetadata$Builder.setConfigurationId:(...)
```

`setConfigurationId` sits on the **same builder chain** as `setTargetId` and `setActionMnemonic`,
which the working profile map proves arrive populated in this fleet. **`configuration_id` is
available and is the build-configuration checksum.**

**Contradiction check.** Trigger (dispatching config absent from the key's set) ∧
precondition (DOWN arm) is reachable. But the deployed *workload* creates a second-order
contradiction that must be stated: `~/fl` builds under `--config=opt`, `--config=asan`,
`--config=tsan` and an exec configuration, each with a distinct configuration checksum. A key
therefore legitimately spans 2–4 concurrent configuration ids, and the **first build under a
config not used for a while refuses DOWN for every key at once** — a fleet-wide correlated
refusal precisely when a big rare build starts. That is the rarely-run trap again, with rare
*configurations* substituted for rare *targets*. Sizing this requires instrumenting the config
cardinality, which no counter does today: **UNMEASURED, and it is the load-bearing unknown.**

**Why it is rejected.**

1. **Zero power against the one observed event.** The configuration checksum is a function of
   build *options*, not of source. `//client:fl_cli_test`'s outlier (§4) occurred under an
   unchanged configuration. D2 would have rated the key trusted.
2. **Zero observed instances to catch.** §3 finds no regime change anywhere in the live map. A
   discriminator with no addressable population is a dark mechanism by construction — the
   anti-dark-flag rule's failure mode arriving through the front door rather than through a
   default-off flag.
3. **Cost exceeds the benefit.** A format change, a new per-`Agg` bounded set, a new key-space
   interaction, a new refusal mode with unmeasured cardinality — against ≤2.20 % of dispatches
   and falling.

**Verdict: REJECTED for now — but this is the candidate to revive** if the hazard ever
materialises, because it is the only one that targets the named mechanism rather than a
correlate of it. Revival trigger in §9.

### D3 — distribution-shift test

**REJECTED on measured power** (§3a): a 1 %-FPR detector cannot fire below 5.93×; a 30 %
regression is at the 78th percentile of the null.

### D4 — remove the gate

**REJECTED, on sequencing.** The gate now costs ≤2.20 % of dispatches and falling. Removing it
is a change with a small measurable benefit and a risk whose only instrument **cannot fire**
(§7). Removing a safety mechanism while its falsifier is broken is the wrong order regardless of
how small the mechanism turns out to be. Revisit only after §7 lands and produces a clean
baseline.

The brief's constraint — *"remove the gate is only acceptable with a proof the hazard is
unreachable"* — is worth restating precisely: §4 proves the **one observed instance was not a
staleness instance**, which weakens the case for the gate but is not a proof that stale-OOM is
unreachable. It is not offered as one.

### D0 — do nothing to the gate

**RECOMMENDED.** This is the diagnosis' own Option D, reached through its own pre-declared gate
(§2), and it costs nothing.

---

## 7. Design question 5 — the instrumentation, and it lands first

**The declared falsifier cannot fire today, on `main`, with no Option A anywhere near it.** This
is not a consequence of the refuted change; it is a live defect.

**Mechanism, read at both sites.**

- The ledger stands `phase3_down_effective_kb(declared_kb, down_reserve_kb, factor)` =
  `p95.clamp(ceil(declared/factor), declared)` — `api_worker_scheduler.rs:2128-2135`, called at
  `:4130-4133`.
- The falsifier stashes `dispatch_prediction` from `lookup_tiered` — `:8211-8229` — i.e. the raw
  `p95_kb`, **not** the clamped value. `classify_prediction_accuracy` (`:2226-2265`) compares
  `actual_kb` against that stash.
- The two differ **whenever the floor binds**, which is the common case, not the corner case.

**Sized against the live snapshot** (declared values read from the live `accuracy_under_offenders`
dump: `RustcLink` 2,000,000; `Rustc`/`RustcMetadata`/`TestRunner` 4,000,000; factor 4.0 from the
live config). Folds are a uniform 1/16 digest-hash sample of dispatches
(`CALIB_SAMPLE_PERIOD = 16`, verified at the declaration line; the diagnosis measured
observed/expected z = +0.76), so weighting by fold count is an unbiased weighting by dispatch:

| mnemonic | declared | floor | trusted keys | floor-pinned | folds pinned |
|---|---|---|---|---|---|
| `RustcLink` | 2,000,000 | 500,000 | 215 | 76 (35.3 %) | 4,035 / 11,431 (35.3 %) |
| `Rustc` | 4,000,000 | 1,000,000 | 116 | 115 (99.1 %) | 5,751 / 5,774 (99.6 %) |
| `RustcMetadata` | 4,000,000 | 1,000,000 | 81 | 80 (98.8 %) | 3,677 / 3,707 (99.2 %) |
| `TestRunner` | 4,000,000 | 1,000,000 | 4 | 4 (100 %) | 95 / 95 (100 %) |
| **total** | | | **416** | **275 (66.1 %)** | **13,558 / 21,007 (64.5 %)** |

On the median floor-pinned key the standing reservation is **9.43×** the number the falsifier
scores (p90 = 32.8×, max = 710×). So:

1. Most of the 115 `accuracy_under_by_arm_down` events on the live counter are **false unders** —
   the peak exceeded the p95 but sat far below the floor that was actually reserved. The counter
   is pessimistic, which is safe, but it is **not measuring the reservation** and the operator's
   revert trigger is defined on it.
2. Any change that shifts the floor-pinned share moves the counter for reasons unrelated to
   safety.
3. The documented recompute recipe in the metric's own help text (*"recompute from the
   `accuracy_under_offenders` log `declared=`/`predicted=` fields"*) does work today — for a
   reader who knows to apply the clamp themselves — but nothing in `/metrics` records the
   reserved value.

**Minimum viable fix (observe-only, ~15 lines, no behaviour change).** Not designed in detail
here; scoped so an implementer can be dispatched:

- In the DOWN branch (`:4106-4133`), carry the **clamped effective** value out alongside the arm,
  and overwrite the stash at `:8237` with it (both are already inside the same lock and branch).
  Then `classify_prediction_accuracy` scores the number that was actually reserved.
- Add `phase3_down_floor_pinned` (a per-dispatch counter, in the same `result.is_some()` block
  as the arm counters so the partition stays structural) so the ~64 % population is countable
  and can be excluded from a rate.
- Emit the reserved value in the `accuracy_under_offenders` row so the log recompute survives.

**No kill-switch, and that is deliberate.** The change is observe-only: it moves which number a
counter is compared against and adds one counter. It stands no reservation and gates no
dispatch, so there is nothing to revert operationally; the revert is `git revert`. Adding a
config flag would create a dark path (the flag's off-branch would be the broken measurement) for
no operational benefit — the project's default-ON rule argues *against* a flag here, not for one.

**Ordering.** This lands **before** any further work in this area, including the redirects in
§8. Not because a discriminator is coming — none is — but because §8's questions cannot be
answered on an instrument that scores the wrong number, and because the counter is on an
operator-facing revert trigger right now.

---

## 8. Where the measurable risk actually is (observations, not a design)

Filed so the workstream's attention moves to what the data shows, not designed here.

1. **The coarse blend, not the fine tier, dominates under-prediction.** 36–45 % of dispatches
   resolve to the coarse tier (`d(lookup_coarse)/d(hits)` = 36.0 % in W3, 45.1 % in W2 — and
   note this is *without* any Option-A-style fallback). The two largest rows in the live
   offender dump are both coarse:
   `|Rustc tier=Coarse arm=Down n=36 worst_x100=989 declared=4,000,000 predicted=404,192
   actual=1,833,266` — against a floor of 1,000,000 that is a **real 1.83× under-reservation**,
   past the floor — and `|RustcMetadata tier=Coarse arm=Inject n=46`. Coarse-tier window spreads
   from the live snapshot: `TestRunner` max/p50 = **76.9×**, `Rustc` 5.6×, `RustcMetadata` 2.9×,
   `RustcLink` 1.4×. A blend across every target of a mnemonic is a poor reservation basis for
   the high-spread mnemonics, and `api_worker_scheduler.rs:2191-2194` has said so since
   `0ece0445`.
2. **The deleted variance margin.** §5a/§5b: the reservation ignores a dispersion signal it
   already computes, and §4 shows dispersion is where the residual risk lives (per-key
   max/median: p50 = 1.58, p90 = 5.47, p99 = 45.8). Whether re-coupling the margin is worth it
   is a separate question — and it needs §7 to be answerable at all.
3. **`arm_inject` is the majority arm.** 69.5 % of W2 dispatches and 27,825 of 47,129 cumulative
   are undeclared actions receiving an injected p95, and `accuracy_under_by_arm_inject = 154` is
   the **largest** under bucket, above DOWN's 115. Whether the injected p95 is well calibrated
   for that population is unasked.

---

## 9. Cost of the format change (asked explicitly, though not recommended)

Measured on the live snapshot (901,418 B, 2,130 entries) so a future designer does not have to
re-derive it.

**On-disk.** 423.2 B/entry today. Breakdown: 134,674 B of key strings, 630,400 B of `u64` sample
payload (78,800 retained samples across four dimensions; 19,700 of them memory), 68,160 B of
`Vec` length prefixes (4 × 8 B/entry), 51,120 B of `String` length prefixes (3 × 8 B/entry),
24 B of header.

| addition | bytes | % of file | new file size |
|---|---|---|---|
| one `u64` per entry (per-agg last-fold time, or a config-id hash) | +8 × 2,130 = **17,040** | **+1.89 %** | 918 KB |
| one `u64` per **memory sample** (per-sample timestamps) | +8 × 19,700 = **157,600** | **+17.5 %** | 1.06 MB |
| a `Vec<u64>` of ≤4 config-ids per entry (8 B prefix + 8 B/id, ~2 ids typical) | ≈ +24 × 2,130 = **51,120** | **+5.7 %** | 953 KB |

At the LRU cap (`PROFILE_MAP_MAX_KEYS`, see below) rather than today's 2,130 residents, the
per-sample variant scales to ~16 MB — still an order of magnitude under any limit.

**`SNAPSHOT_PREALLOC_LIMIT` (64 MiB).** Verified against the wincode source, not the test's
doc-comment: `SeqLen::prealloc_check<T>(len)` computes `needed = len * size_of::<T>()` and errors
if `needed > limit` (`wincode-0.5.5/src/len.rs:79-99`). The limit therefore bounds
`entry_count × size_of::<ProfileEntrySnapshot>()` — the **in-memory** size, not on-disk bytes.

`size_of::<ProfileEntrySnapshot>()` = 3 × 24 (`String`) + 4 × 24 (`Vec<u64>`) + 8 = **176 B**.

| variant | `size_of` | max decodable entries | margin over the 32,768 cap |
|---|---|---|---|
| today | 176 B | 381,300 | **11.6×** |
| +1 `u64` | 184 B | 364,722 | 11.1× |
| +1 `Vec<u64>` | 200 B | 335,544 | 10.2× |

The nested `Vec<u64>` check is `len × 8 ≤ 64 MiB` ⇒ 8.4 M samples per vector against a real max
of 20. **No format change contemplated here comes within an order of magnitude of the limit.**

**`max_count` / LRU sizing.** `PROFILE_MAP_MAX_KEYS = 32768` at its declaration line
(`resource_profile.rs:80`). Two doc sites are **stale after the #2497 cap bump** and should be
corrected whenever this file is next touched — they still say 16384:
`resource_profile.rs:539` (`// CAPPED AT PROFILE_MAP_MAX_KEYS (16384)`) and
`resource_profile_persist.rs:44` (*"16384 entries × ~1 KiB ≈ 20 MiB"*). In-memory per-entry cost
today is ~1 KiB (`:73-79`); +8 B is +0.8 %, a `Vec<u64>` header +24 B is +2.3 %. Neither moves
the ~32 MiB envelope. **The format change is genuinely cheap. It is not the reason to say no.**

---

## 10. What would revive this work — pre-declared triggers

State these before closing so a future reader does not have to re-derive the stop.

| trigger | measurement | action |
|---|---|---|
| Staleness suppression stops decaying | `d(arm_unmodified)/d(find_worker_hits)` over a ≥6 h window at ≥3 d uptime **rises above 5 %** | Re-open; the saturating-numerator model is wrong and the mechanism is not what we think |
| A real regime change appears | Re-run the §3 census (decoder in the evidence index) and find **≥5 of ~450** keys with a clean upward level step, **or** a fleet-correlated step after a toolchain bump | Build **D2 (`configuration_id`)** — it is designed in §6 and the identity is already on the wire |
| DOWN under-reservation becomes real | After §7 lands: `accuracy_under_by_arm_down / phase3_dispatch_arm_down`, **scored against the reservation**, rises above its post-fix baseline over equal-length windows at comparable uptime | Investigate the estimator (§8.2), not the freshness gate |
| A worker OOMs or `memory_gate_nak_free_floor` leaves 0 | live worker counters | Pull `phase3_down_overcommit_enabled = false`; this is unrelated to freshness |

---

## 11. Design self-review (pre-cadre, against current code)

① **Does the existing system already achieve this?** Yes, partly, and that is §5: a dispersion
signal is computed on every peek and consumed by nobody, and the variance-adaptive margin the
gate was built to complement was deleted in `d76de92c`. This changed the recommendation.

② **Is every mechanism relied on actually implemented?** The recommendation relies on: the four
arm counters (landed, `f5b34006`, firing live, partition exact); `phase3_down_effective_kb`
(landed); the `accuracy_*` counters (landed); `configuration_id` on the wire (confirmed at
Bazel bytecode, not assumed). No TODOs or aspirational hooks.

③ **Does any cost/decision model reduce cleanly?** The two benefit numbers are independent and
are not multiplied together anywhere: 2.20 % is a *dispatch-share* from counter deltas; 64.5 %
is a *fold-share* from the snapshot. §7 uses the second for the instrumentation defect only. No
double-counting. Where they *would* compose (the addressable population for a discriminator is
roughly 2.2 % × 35.5 %), it is stated as a rough composition, not carried as a headline.

④ **Does each claimed bound constrain the right resource?** The `declared/factor` floor bounds
*a single wrong-low prediction*, not correlated fleet-wide error — the §8 `|Rustc` coarse row is
an observed 1.83× breach of it. `SNAPSHOT_PREALLOC_LIMIT` bounds *decode preallocation*, not
file size — corrected in §9 against the wincode source. `arm_unmodified` bounds staleness
suppression from **above** only.

⑤ **Chesterton's Fence on every field given to a new reader:** `fresh_since_load` →
`173ac171` + `d76de92c` (§5a, the load-bearing find); `snapshot_unix_secs` → `173ac171`,
`5e640b24`; `SNAPSHOT_VERSION` → `173ac171`, `5e640b24`, and its own doc's "bump on ANY
incompatible layout change, a differing version starts FRESH" (§9).

**Contradiction check (project rule, deployed config not just logic):** applied per-candidate in
§6. D1 passes (unlike the refuted Option C, whose fallback was dark because a sibling default
made every loaded agg permanently trusted). D2 passes the logical check but has an **unmeasured**
second-order trigger — concurrent configuration cardinality — which is stated as unmeasured
rather than assumed away. D3 fails on power. None is recommended.

**Negatives asserted here, and how they were searched.** *"`variance_ratio_x100` is consumed by
nobody"* — searched by field name workspace-wide, by every `TieredTail` destructuring site, and
by the producer function; the only cross-file hit is the gauge. *"Options A/C are not on
`main`"* — read `down_lowering_trusted` and `down_lowering_reserve_kb` at their sites.
*"No counter records the reserved value"* — read the stash site and every `accuracy_*` producer.

**Hedged / unresolved, marked as such.** The concurrent-`configuration_id` cardinality of the FL
build is **UNMEASURED** (§6/D2). The §3 census bounds *in-flight* transitions at one instant and
is one draw, not a longitudinal study (§3). Whether re-coupling a variance margin is worth doing
is **not analysed** (§8.2).

---

## 12. Evidence index

All artifacts under `/tmp/claude-scratch/a99ec47b-bf2a-48ee-9ab4-00a1f7006775/agent.lqzNt7/`:

- `metrics.txt` — live `/metrics`, 2026-08-09T15:01:27Z, uptime 19 h 21 m 35 s, PID 3001166,
  `NRestarts=0` (T3). `metrics-t4.txt` — same, 2026-08-09T15:16:03Z, same PID (T4).
- `snap.bin` — live `/srv/nativelink/resource_profile.snapshot`,
  `snapshot_unix_secs = 1786287612` (2026-08-09T15:00:12Z), 901,418 B, 2,130 entries, decoded
  with zero trailing bytes; entry count matches the live `profile_keys_tracked` gauge exactly.
- `live-config.json5` — `ssh buildcache sudo cat /srv/nativelink/buildcache-native.json5`.
- `analyze.py` + `analyze.log` — decode, Fence-save window, walk-forward under rate, format cost.
- `drift.py` + `drift.log` — the three stationarity contrasts, dispersion, coarse spreads.
- `regime.py` + `regime.log` — regime-change census, run-frequency strata, rarely-run sizing.
- `floor.py` + `floor.log` — floor-pinned share and the falsifier's blind spot.
- `entries.json` — the 2,130 decoded entries (re-run any of the above without re-fetching).
- `offenders.txt` — live `accuracy_under_offenders` dump (journal, last 90 min).
- `jar/.../TracingMetadataUtils.class` — extracted from
  `~/.cache/bazel/_bazel_user/install/107608e669c054177cc405a3bc536f99/A-server.jar`
  (`build-label.txt: 9.2.0-fl.825a5b2492`); `javap -c` output cited in §6/D2.
- Archived prior scrapes read as primary sources:
  `.claude/audits/phase3-arm-T{0,1,2}-scrape-*.txt`.
- Decoder lineage: `.claude/audits/phase3-zerop95-decoder.py`.
- Cadre that killed the previous attempt: `.claude/reviews/a18fa12a/{pair-a,pair-b,auditor}.md`.
