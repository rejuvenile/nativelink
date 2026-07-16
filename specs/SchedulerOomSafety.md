# SchedulerOomSafety — verdict (Phase-3 cap-removal safety bundle)

**Question (`#task-resource-profile`, design v3 §7/§8):** the measured-override / cap-removal
scheduler removes the p_headroom concurrency cap for profiled tasks and admits by a **measured
memory estimate that is a LOWER bound of true peak** (`estimate ≤ true_peak` — the poll-based
`phys_footprint` reap misses between-poll multi-process spikes, b861c68b red-team). Can the
reservation + reactive gate hold the **#1 property** — *a worker never runs a concurrent set whose
TRUE peak exceeds its RAM (`NoOOM`)* — and what margin/ceiling MUST Phase-3 enforce?

**TLC verdict:** with the estimate under-reporting and **no safety margin**, `NoOOM` is
**VIOLATED** — the reservation admits by estimate while the true concurrent peak overflows RAM
(exactly the reviewers' OOM). `NoOOM` is **RESTORED** by *either* a reservation margin sized to the
worst-case under-report factor *or* a profile-independent count ceiling sized so
`ceiling × max_true_peak ≤ RAM`. The **ceiling is the load-bearing backstop** (holds even when the
margin's under-report assumption is exceeded — security S-2). Over-reserving *or* a count-blind
ceiling destroys work-conservation, so the safe operating point is a **byte-adaptive margin at the
measured worst-case factor as the primary limiter + a worst-case-peak count ceiling as the mandatory
backstop.**

Spec: `specs/SchedulerOomSafety.tla` (+ 10 cfgs). TLC log: `/tmp/oom-tla-1090214.log`.
**This is not a proof for the fleet — finite dimensions stated in §4.**

---

## 1. State machine (per action, per worker)

```
                     Admit(w,a)  [ CeilingPermits(w)          : RunningCount(w) < Ceil
                                 ∧ ReservationPermits(w,a)   : ReservedUsed(w)+Estimate(a)*MarginMul ≤ RamCap
                                 ∧ ReactivePermits(w)        : RamCap−ReservedUsed(w) ≥ Floor ]
   ┌────────────┐   ───────────────────────────────────────────────────────►   ┌──────────────────────┐
   │  QUEUED    │                                                                │  RUNNING on w        │
   │ (reserved  │   ◄─ (no transition: reactive gate CANNOT shed; actions      │  charges Reserved(a)  │
   │  nothing)  │       never complete — adversarial fullest-set snapshot)      │  to the est-ledger;   │
   └────────────┘                                                                │  contributes         │
                                                                                 │  TruePeak(a) to the   │
                                                                                 │  worker's TRUE peak   │
                                                                                 └──────────────────────┘
   Failure classes at admission (all NAK, action stays QUEUED):
     ceiling-full   → RunningCount = Ceil
     reservation-full → est-ledger would exceed RamCap
     memory-pressure  → est-free < Floor   (BLIND to true peak — the OOM is invisible here)

   OOM state (the violation): a worker with TrueUsed(w) = Σ TruePeak(running) > RamCap.
     Reached silently: the estimate-ledger reads FREE while the true peaks co-occur over RAM.
```

The scheduler only ever reads the **estimate ledger** (`ReservedUsed`); `TrueUsed` is the physical
reality it is blind to. The gap `TrueUsed − ReservedUsed` is the under-report, and it is unbounded
in the number of co-resident under-reported actions once the p_headroom cap is removed.

## 2. Cross-product table (reservation-margin × ceiling, under a fixed under-report)

Corners: **margin** ∈ {none `M=1`, exact `M=B`, over `M>B`}; **ceiling** ∈ {none `Ceil≥N`, sized
`Ceil≤⌊RAM/maxpeak⌋`}; **under-report** `B = max true_peak/estimate` (assumed vs actual). `NoOOM` =
no true over-commit; `NoIdleWaste` = no truly-fitting queued work stranded at the admission fixpoint.

| margin \ ceiling | ceiling = NONE (`max_inflight_tasks=0`, today) | ceiling = SIZED (`≤⌊RAM/maxpeak⌋`) |
|---|---|---|
| **M = 1 (no margin)** | **BROKEN: NoOOM VIOLATED** — reservation admits by estimate, true peak overflows (cfg `BuggedNoMarginNoCeiling`, dpth 4, true 18>16) | SAFE: ceiling caps count; true ≤ RAM (cfg `FixedCeilingOnly`) — but count-blind ⇒ wastes on small actions (cfg `BuggedCeilingHeteroWaste`: NoIdleWaste VIOLATED) |
| **M = B (exact, sweet spot)** | SAFE: `Reserved=TruePeak` ⇒ reservation refuses exactly at true overflow; NoOOM ∧ NoIdleWaste both hold (cfg `FixedMarginOnly`) | SAFE: belt+suspenders; both hold (cfg `FixedBoth`) |
| **M > B (over-reserve)** | DEGRADED: NoOOM holds but **NoIdleWaste VIOLATED** — strands truly-fitting work, E-core-spill goal lost (cfg `BuggedOverMargin`, dpth 2) | DEGRADED: NoOOM holds, still over-reserves |
| **M = B_assumed < B_actual** | **BROKEN: NoOOM VIOLATED** — margin guessed too small, nothing catches it (cfg `BuggedMarginUnderestimated`, dpth 3, true 18>16) | **SAFE: ceiling SAVES IT** — profile-independent bound holds though the margin failed (cfg `FixedMarginUnderestimatedCeiling`) |

Extra ledger-hole corner (orthogonal to margin/ceiling): a **cold / unprofiled** action with a
too-low cold-default estimate OOMs even when every *profiled* task is perfectly profiled
(cfg `BuggedColdUnderProvision`: NoOOM VIOLATED, true 18>16). ⇒ design v3's *"inject a conservative
estimate for EVERY task"* is load-bearing; the cold-default must not under-charge the ledger.

**Any BROKEN cell blocks Phase-3 ship.** The top-left cell is TODAY's config the moment the cap is
removed (`max_inflight_tasks=0` = no ceiling, reservation reads a raw lower-bound estimate).

## 3. The composite invariant Phase-3 MUST enforce

> `cap-removed ⇒ ( Σ_running Reserved(a) ≤ RAM  ∧  Reserved(a) ≥ true_peak(a) )  ∨  ( running_count ≤ ⌊RAM / max_true_peak⌋ )`

i.e. **admit only if the margined reservation covers the worst-case true peak, OR the count ceiling
caps the worst-case simultaneous peak under RAM.** The second disjunct is profile-INDEPENDENT and is
the mandatory OOM backstop, because the first disjunct's `Reserved(a) ≥ true_peak(a)` rests on a
margin `M` that is itself derived from a measurement (`B` is unmeasured/under-biased — §7 caveat), so
`M` can be wrong. Concretely the two knobs:

- **Reservation margin (primary, byte-adaptive, work-conserving):** `Reserved(a) = estimate(a) × M`
  with `M ≥ B = max_a true_peak(a)/estimate(a)` observed. Setting `M = B` is the sweet spot
  (`Reserved = true_peak` ⇒ NoOOM ∧ NoIdleWaste). `M > B` wastes; `M < B_actual` OOMs.
- **Overcommit ceiling (mandatory backstop):** `Ceil ≤ ⌊RAM / max_observed_true_peak⌋`, enforced
  independent of any profile. It bites only when the margin/profile is wrong — a count bound that no
  under-report can defeat. MUST BE BUILT (today `max_inflight_tasks=0`).

## 4. TLC results (all 10 configs; log `/tmp/oom-tla-1090214.log`)

Finite model: **1 worker (2W variant confirms per-worker replication), 6 actions, RamCap = 16,
Floor = 2**, two action classes (profiled + cold/small). Uniform per-class peaks; `estimate ≤
true_peak` per class.

| cfg | knobs (M / Ceil / true:est) | result | states | note |
|---|---|---|---|---|
| `BuggedNoMarginNoCeiling` | 1 / 99 / 6:3 | **NoOOM VIOLATED** (dpth 4, true 18>16) | 37 | the reviewers' OOM |
| `BuggedColdUnderProvision` | 1 / 99 / cold 6:1, prof 6:6 | **NoOOM VIOLATED** (dpth 4, true 18) | 32 | cold ledger-hole, profiles perfect |
| `BuggedMarginUnderestimated` | 2 / 99 / 9:3 (B=3>2) | **NoOOM VIOLATED** (dpth 3, true 18) | 13 | margin guessed too small |
| `BuggedNoMarginNoCeiling2W` | 1 / 99 / 6:3, 2 workers | **NoOOM VIOLATED** (dpth 4, true 18) | 120 | OOM is per-worker |
| `FixedMarginOnly` | 2 / 99 / 6:3 (M=B) | HOLDS (NoOOM ∧ NoIdleWaste) | 22 | sweet spot |
| `FixedCeilingOnly` | 1 / 2 / 6:3 | HOLDS (NoOOM ∧ NoIdleWaste) | 22 | uniform ⇒ no waste |
| `FixedBoth` | 2 / 2 / 6:3 | HOLDS (NoOOM ∧ NoIdleWaste) | 22 | belt+suspenders |
| `FixedMarginUnderestimatedCeiling` | 2 / 1 / 9:3 | HOLDS (NoOOM) | 7 | **ceiling saves the mis-sized margin** |
| `BuggedOverMargin` | 3 / 99 / 6:3 (M>B) | NoOOM holds; **NoIdleWaste VIOLATED** (dpth 2) | 2 | over-reserve strands work |
| `BuggedCeilingHeteroWaste` | 1 / 2 / small 2:1 | NoOOM holds; **NoIdleWaste VIOLATED** (dpth 3) | 13 | count-blind ceiling strands small work |

### The OOM counterexample (`BuggedNoMarginNoCeiling`)
```
State 4: location = (a1:>w1, a2:>w1, a3:>w1, a4:>Queue, a5:>Queue, a6:>Queue)
```
3 profiled actions on w1. Estimate ledger: `ReservedUsed = 3×3 = 9 ≤ 16` (reads 7 free — reactive
gate sees `16−9=7 ≥ Floor=2`, permits). TRUE: `TrueUsed = 3×6 = 18 > 16` → **OOM**. The reservation
admitted by the lower-bound estimate; the concurrent true peak exceeded RAM; the reactive gate was
blind to it and cannot shed. This is the #203 / 2026-05-08 class the reviewers warned of.

### The ceiling-saves-it result (`FixedMarginUnderestimatedCeiling`)
Same 3× actual under-report that OOMs `BuggedMarginUnderestimated`, but `Ceil=1=⌊16/9⌋` caps the
worker at 1 running (`TrueUsed=9≤16`). The margin was WRONG yet NoOOM held — the profile-independent
ceiling is the only bound that survives a wrong under-report estimate (security S-2).

## 5. NoOOM vs work-conservation — the safe operating point

The two properties pull opposite ways: NoOOM wants to **over-reserve** (admit fewer), the E-core-spill
goal wants to **under-reserve** (admit more). The model locates the reconciling point:

- **Reservation margin at `M = B` (measured worst-case under-report factor)** is the sweet spot:
  `Reserved(a) = true_peak(a)`, so the reservation refuses *exactly* when true RAM would overflow —
  NoOOM ∧ NoIdleWaste hold simultaneously (`FixedMarginOnly`). This is **byte-adaptive**: it admits
  many small actions and few large ones, which is precisely the E-core-spill unlock.
- **Over-margin (`M>B`)** stays OOM-safe but strands truly-fitting work (`BuggedOverMargin`) — the
  work-conservation loss the task warns against. So the margin should be set at the *measured* factor,
  not inflated "to be safe."
- **The count ceiling is a poor *primary* limiter**: sized for the worst-case peak it is count-blind,
  so it strands ~75% of RAM on small/heterogeneous actions (`BuggedCeilingHeteroWaste`). It belongs as
  the **backstop** — loose enough not to bind in normal operation (byte-reservation governs), tight
  enough that no profile failure can OOM.

**Safe operating point:** byte-adaptive reservation with `M = B_observed` as the primary,
work-conserving limiter; a worst-case-peak count ceiling as the mandatory profile-independent OOM
backstop; a conservative cold-default injected for every unprofiled task; the reactive gate as a last
NAK (never relied on for OOM).

## 6. Composite test Phase-3 MUST ship (not a corner test)

A test exercising the **full composition** where **2-of-3 corners are degraded so the third must
compensate**: cap REMOVED (corner 1 off) + estimate under-reporting / a cold-default task (corner 2
degraded) + assert the CEILING (corner 3) holds `NoOOM` — i.e. drive N profiled+cold actions whose
summed *true* peak exceeds a worker's RAM, with the reservation ledger reading "free," and assert the
worker never admits past `⌊RAM/max_true_peak⌋` and never OOMs. A test that only checks the reservation
math (corner 2 in isolation) or only the ceiling count (corner 3 in isolation) does NOT count — the
2026-05-08 debacle passed every corner in isolation. Extend the `18e38cf5` p_headroom isolation tests
(I5_Bounded/I6/PrefMonotone) into the cap-removed regime (design §8 S-7).

## 7. Caveats the model does NOT capture

- **Dispatch latency / async + completion:** actions never complete (monotone-to-fixpoint = the
  adversarial fullest-concurrent-set snapshot). Real completion only frees RAM — conservative for
  NoOOM. Real dispatch latency and async reservation-commit ordering are not modeled.
- **The reactive gate's true lag + inability to shed:** modeled estimate-based (blind to true) and
  non-shedding. In production it reads a **lagging poll of real pressure** and still cannot shed — so
  the between-poll concurrent spike (the LTO / `make -j` shape from the §7 caveat) escapes it. The
  model applies the full concurrent true peak at each admit (worst co-occurrence) but does not model
  poll timing, so it neither over- nor under-credits the reactive gate; it simply shows the gate is
  not an OOM defense.
- **`B` is itself unmeasured:** the model treats the worst-case under-report factor as known. In
  reality Phase-1 must produce an unbiased running-MAX/EWMA (design §7 caveat) or `B` is a guess —
  which is exactly why the profile-independent ceiling is mandatory (proved by
  `FixedMarginUnderestimatedCeiling`).
- **Heterogeneous true_peak:** only two classes modeled (uniform within class). Sizing `M` (max
  factor) and the ceiling (max peak) over a genuinely heterogeneous in-flight distribution is design
  §10's open question; a single scalar `B`/`max_peak` is a simplification.
- **Single resource (RAM):** network as a second reservation dimension and the ingest clamp
  (over-report / upper-bound defense — orthogonal to OOM, security S-2) are not modeled.

## 8. Scope / do-not-overstate

TLC proves the properties over the FINITE models in §4 (1–2 workers, 6 actions, RamCap 16, one
under-report factor per cfg, two action classes). A green run is evidence the **admission LOGIC** is
OOM-safe and work-conserving at these shapes — it is **not** a proof for the live 10×(4+6) fleet at
arbitrary queue depth or arbitrary peak distribution. It de-risks the Phase-3 design decision: it
shows (a) the cap-removed reservation-only path OOMs under any under-report, (b) exactly which
margin/ceiling relationship restores NoOOM, and (c) that the ceiling — not the margin — is the
load-bearing backstop.

**Verdict:** `BLOCK-COMPOSITE-BROKEN` for cap-removal **without** the §7 safety bundle (the top-left
`M=1 / no-ceiling` cell is today's config and OOMs). `SHIP-CANDIDATE` for cap-removal **with** the
bundle: byte-adaptive reservation margin `M = B_observed` + mandatory count ceiling
`Ceil ≤ ⌊RAM/max_true_peak⌋` + all-task estimate injection (conservative cold-default) + reactive
gate as last NAK. Ship the §6 composite test with it.
