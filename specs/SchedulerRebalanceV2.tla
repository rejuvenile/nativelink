------------------------ MODULE SchedulerRebalanceV2 ------------------------
(***************************************************************************
  M1 v2.2 refinement: a SOFT two-tier P-headroom preference in the scheduler's
  OVERFLOW (LRU/MRU fallback) ranking. Model-checks that the refinement
  PRESERVES no-wedge (I2) AND STRENGTHENS I1 (P-headroom-first) onto the
  overflow path, per
  `.claude/audits/scheduler-pcore-first-rebalance-design-v2-2026-06-30.md` §11.

  This EXTENDS the v1 base spec SchedulerRebalance.tla (reserve→execute→
  complete ∥ evict-drain ∥ independent load-sat toggle). v1's 5 invariants
  HOLD and are teeth-proven (SchedulerRebalanceBugged: NoPHeadroomGate /
  NoA5Guard / Phase2NoLift). This spec changes ONE thing:

    ── v1 base: the fallback (LRU/MRU backstop) is P-headroom-UNGATED. It
       dispatches to ANY BaseViable worker; placement among them is not
       modeled (any viable worker is a legal fallback target). This correctly
       models the AS-BUILT M1: `inner_find_worker_for_action`
       (api_worker_scheduler.rs:983) filters by `worker_matches` (:1030 — no
       P-headroom check) and ranks by `min_by_key(effective_load_score)`
       (:1163) — the STALE worker-reported p_load. So the overflow is NOT
       steered to a P-headroom worker (red-team RECONSIDER-PREMISE, verified).

    ── v2.2 refinement: the fallback ranking becomes a SOFT two-tier sort key
       (design §11, :311-313):
         sort_key(w) = ( IF gate_on THEN ~HasPHeadroom(w) ELSE FALSE,   \* tier
                         effective_load_score(w) )                       \* within-tier
       Tier A (HasPHeadroom, key elt 0=FALSE) sorts BEFORE Tier B
       (~HasPHeadroom, key elt 1=TRUE); WITHIN a tier the existing
       effective_load_score min-by ordering is unchanged.
       * SOFT, NOT hard: Tier-B workers remain ELIGIBLE. When NO worker has
         P-headroom (Phase 2 / fully saturated), Tier A is empty, all workers
         are Tier B ranked by load, dispatch proceeds → NO WEDGE.
       * Flag OFF → first tuple elt is unconditionally FALSE → single-tier
         effective_load_score ordering byte-identical to today.

  ------------------------------------------------------------------------
  THE SIGNAL-DISAGREEMENT MODEL (the whole point of v2.2, design §11):
    The fallback's WITHIN-tier rank is `effective_load_score` = STALE
    worker-reported p_load (:514). The TIER is `HasPHeadroom` = FRESH
    scheduler dispatch-count. These are DIFFERENT signals that can DISAGREE:
    the incident's I/O-bound "citizen" ran ~10 actions at p_count=4
    (dispatch-count HIGH → ~HasPHeadroom → Tier B) while p_load ≈ 20% (load
    LOW → low effective_load_score). A peer could have dispatch-count LOW
    (HasPHeadroom → Tier A) but a HIGHER p_load.

    To model this disagreement we make load an INDEPENDENT variable
    `loadScore[w]` (the stale effective_load_score), decoupled from
    Running(w) (the fresh dispatch-count). The environment sets loadScore
    freely, so TLC explores the case where the excluded holder has a LOWER
    loadScore than the P-headroom peer — the exact case the design says the
    refinement must resolve: the holder loses on TIER despite winning on LOAD.

  ------------------------------------------------------------------------
  CITATIONS (mechanism → file:line, verified 2026-07-01):
    [fallback]   api_worker_scheduler.rs:983 inner_find_worker_for_action
    [wmatches]   :1030 worker_matches — NO P-headroom check (ungated today)
    [rank]       :1163-1170 min_by_key(effective_load_score) (single-tier today)
    [score]      :514 effective_load_score (stale worker-reported p_load)
    [phcount]    :4978 p_core_count: u32 (per-worker); running_action_infos.len()
    [design]     §11 :309-321 the SOFT two-tier sort key
    [teeth]      §11 :318-319 a HARD filter (exclude Tier B) → Phase2NoLift wedge
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,        \* set of worker IDs
    Ops,            \* set of operation IDs
    MaxToggles,     \* bound on adversary load-sat + load-score toggles (liveness)
    PCoreCount,     \* [Workers -> Nat] per-worker p_core_count (via <- override)
    Capacity,       \* hard reservation-slot cap per worker (>= max PCoreCount)
    AllowEvict,     \* TRUE => environment may evict a reserved worker
    AllowLoadSat,   \* TRUE => environment may toggle load-saturation (A4)
    GateOn,         \* TRUE => v2.2 two-tier fallback preference active (flag ON).
                    \* FALSE => single-tier fallback (flag OFF, today's behavior).
    Fallback        \* Fallback ranking mode:
                    \*   "Soft"       — v2.2 as designed (two-tier preference).
                    \*   "HardFilter" — TEETH for I2: EXCLUDE Tier-B instead of
                    \*                  deprioritizing → MUST violate NoWedge on
                    \*                  an all-P-saturated fleet (soft != hard).
                    \*   "UngatedSingleTier" — TEETH for I1-STRENGTHENED: the
                    \*                  AS-BUILT M1 fallback (ranked by loadScore
                    \*                  ONLY, NO tier preference) even with the
                    \*                  gate ON → MUST violate I1_PHeadroomFirst
                    \*                  on the disagreement fleet (the overflow
                    \*                  routes to the lower-load Tier-B holder,
                    \*                  the red-team RECONSIDER-PREMISE gap the
                    \*                  refinement closes).

PCoreCount_Het    == [w \in Workers |->            \* {w1:1, w2:2, w3:0}
                        IF w = "w1" THEN 1
                        ELSE IF w = "w2" THEN 2
                        ELSE IF w = "w3" THEN 0
                        ELSE 1]
PCoreCount_All1   == [w \in Workers |-> 1]
PCoreCount_All2   == [w \in Workers |-> 2]
PCoreCount_Zero   == [w \in Workers |-> 0]

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME PCoreCount \in [Workers -> 0..3]
ASSUME Capacity \in 1..4
ASSUME AllowEvict \in BOOLEAN
ASSUME AllowLoadSat \in BOOLEAN
ASSUME GateOn \in BOOLEAN
ASSUME Fallback \in {"Soft", "HardFilter", "UngatedSingleTier"}
ASSUME MaxToggles \in Nat

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}

(* The stale worker-reported load score is a small abstract lattice. The real
   effective_load_score is 0..MAX; here 3 levels suffice to express "holder's
   load is LOWER than the P-headroom peer's" (the disagreement) and the
   within-tier min ordering. 0 = idle-P (best), 1 = mid, 2 = saturated. *)
LoadScores == {0, 1, 2}

VARIABLES
    opStage,        \* Ops -> Stages
    opWorker,       \* Ops -> WorkerOrNone
    reserved,       \* Workers -> SUBSET Ops : running_action_infos (FRESH dispatch-count)
    workerUp,       \* Workers -> BOOLEAN
    loadSaturated,  \* Workers -> BOOLEAN : the A4 second (load-based) predicate
    loadScore,      \* Workers -> LoadScores : STALE worker-reported p_load rank,
                    \* INDEPENDENT of dispatch-count (models the signal disagreement)
    lastDispatchTier,\* Ops -> {"none","cache","fallback"} : how CURRENT dispatch routed
    togglesLeft      \* remaining adversary toggles (liveness bound only)

vars == << opStage, opWorker, reserved, workerUp, loadSaturated, loadScore,
           lastDispatchTier, togglesLeft >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ loadSaturated \in [Workers -> BOOLEAN]
    /\ loadScore \in [Workers -> LoadScores]
    /\ lastDispatchTier \in [Ops -> {"none", "cache", "fallback"}]
    /\ togglesLeft \in 0..MaxToggles

(***************************************************************************
  THE GATE + THE v2.2 FALLBACK PREFERENCE.
 ***************************************************************************)

Running(w) == Cardinality(reserved[w])

(* A5 guard: p_core_count == 0 => ALWAYS has P-headroom. FRESH signal. *)
HasPHeadroom(w) == (PCoreCount[w] = 0) \/ (Running(w) < PCoreCount[w])

HasFreeSlot(w) == workerUp[w] /\ Running(w) < Capacity
LoadSaturated(w) == loadSaturated[w]
BaseViable(w) == HasFreeSlot(w)

SomeViableHasPHeadroom == \E w \in Workers : BaseViable(w) /\ HasPHeadroom(w)

(* ── M1 cache-tier viability (v1, UNCHANGED): Phase-1 exclusion. ── *)
CacheTierViable(w) ==
    /\ BaseViable(w)
    /\ (SomeViableHasPHeadroom => HasPHeadroom(w))

ViableCount == Cardinality({w \in Workers : BaseViable(w)})
AllViableLoadSaturated == \A w \in Workers : BaseViable(w) => LoadSaturated(w)
SaturationFallThrough == ViableCount > 0 /\ AllViableLoadSaturated

CacheDispatchable(w) == CacheTierViable(w) /\ ~SaturationFallThrough

(***************************************************************************
  ── THE v2.2 FALLBACK RANKING (the refinement under test) ──

  Base eligibility for the fallback (`worker_matches`, :1030): still
  P-headroom-UNGATED — a worker is fallback-ELIGIBLE iff BaseViable. The
  refinement changes only the RANKING/PREFERENCE among eligible workers, not
  eligibility (SOFT).

  FallbackTier(w): the OUTER sort key element (design §11 :311).
    GateOn  => ~HasPHeadroom(w)  (Tier A = P-headroom = FALSE = 0 sorts first;
                                  Tier B = no-P-headroom = TRUE = 1 sorts last)
    GateOff => FALSE (all workers collapse to a single tier → today's behavior)

  FallbackKey(w): the full 2-tuple (tier, stale-load). << >> tuples compare
  lexicographically in TLC, so this is exactly the design's sort key.

  FallbackEligible(w): who is a legal fallback target.
    Fallback = "Soft"       => BaseViable(w) (Tier B stays eligible — the
                               refinement is a PREFERENCE, not a filter).
    Fallback = "HardFilter" => the TEETH bug: when the gate is on AND a
                               P-headroom worker exists, EXCLUDE Tier-B
                               entirely. On an all-P-saturated fleet no
                               P-headroom worker exists, so this reduces to
                               BaseViable — BUT the wedge we want is when the
                               fleet has NO P-headroom worker at all yet the
                               hard filter still excludes: we model the
                               canonical botched form — exclude ~HasPHeadroom
                               UNCONDITIONALLY when GateOn (design §11 :318:
                               "A HARD filter (exclude Tier B) would reintroduce
                               the Phase2NoLift wedge"). On all-P-saturated,
                               EVERY worker is Tier B → all excluded → wedge.
 ***************************************************************************)
(* UngatedSingleTier TEETH: the AS-BUILT M1 fallback ignores the tier (ranks by
   loadScore ONLY) even with the gate ON — so FallbackTier is forced to 0 for
   every worker, collapsing to a single-tier by-load ranking. This is exactly
   the ungated fallback the red-team flagged; I1_PHeadroomFirst must BREAK. *)
FallbackTier(w) ==
    IF (GateOn /\ Fallback # "UngatedSingleTier")
    THEN (IF HasPHeadroom(w) THEN 0 ELSE 1)
    ELSE 0

FallbackKey(w) == << FallbackTier(w), loadScore[w] >>

FallbackEligible(w) ==
    IF Fallback = "HardFilter" /\ GateOn
    THEN BaseViable(w) /\ HasPHeadroom(w)   \* TEETH: hard-exclude Tier B
    ELSE BaseViable(w)                       \* SOFT: Tier B stays eligible

(* The fallback fires only when the cache tiers all decline (the `else` branch
   at :1646 → inner_find_worker_for_action). Same cascade as v1 base. *)
CacheTiersDeclined == ~(\E ww \in Workers : CacheDispatchable(ww))

(* The SOFT preference is a RANKING, so the fallback dispatches to the
   BEST-ranked eligible worker: an eligible w is a legal fallback WINNER iff
   no other eligible worker has a strictly smaller FallbackKey. This is
   `min_by_key(FallbackKey)` — the exact selection the Rust performs. Ties
   (equal keys) are broken by the model nondeterministically (matches
   LRU/MRU tie-break, which the invariants do not depend on). *)
FallbackWinner(w) ==
    /\ FallbackEligible(w)
    /\ \A ww \in Workers :
         FallbackEligible(ww) =>
            ~( FallbackKey(ww)[1] < FallbackKey(w)[1]
               \/ ( FallbackKey(ww)[1] = FallbackKey(w)[1]
                    /\ FallbackKey(ww)[2] < FallbackKey(w)[2] ) )

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ loadSaturated = [w \in Workers |-> FALSE]
    /\ loadScore = [w \in Workers |-> 0]
    /\ lastDispatchTier = [o \in Ops |-> "none"]
    /\ togglesLeft = MaxToggles

(***************************************************************************
  CacheReserve(o, w): the cache-affinity tiers select w. UNCHANGED from v1 —
  the refinement touches ONLY the fallback. I1/I3 still constrain this path
  (Phase-1 exclusion), and now ALSO the fallback path (below).
 ***************************************************************************)
CacheReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ CacheDispatchable(w)
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "cache"]
    /\ UNCHANGED << workerUp, loadSaturated, loadScore, togglesLeft >>

(***************************************************************************
  FallbackReserve(o, w): the v2.2 SOFT two-tier backstop. Fires when the cache
  tiers all declined; dispatches to the BEST-ranked eligible worker
  (FallbackWinner). This is the path v2.2 refines and where the STRENGTHENED
  I1 now bites.
 ***************************************************************************)
FallbackReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ CacheTiersDeclined
    /\ FallbackWinner(w)
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "fallback"]
    /\ UNCHANGED << workerUp, loadSaturated, loadScore, togglesLeft >>

Complete(o) ==
    /\ opStage[o] = "Executing"
    /\ opWorker[o] \in Workers
    /\ workerUp[opWorker[o]]
    /\ o \in reserved[opWorker[o]]
    /\ LET w == opWorker[o] IN
        reserved' = [reserved EXCEPT ![w] = @ \ {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Completed"]
    /\ opWorker' = [opWorker EXCEPT ![o] = NONE]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "none"]
    /\ UNCHANGED << workerUp, loadSaturated, loadScore, togglesLeft >>

DrainStage(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN "Queued" ELSE opStage[o]
DrainWorker(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN NONE ELSE opWorker[o]
DrainTier(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN "none" ELSE lastDispatchTier[o]

Evict(w) ==
    /\ AllowEvict
    /\ workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = FALSE]
    /\ opStage' = [o \in Ops |-> DrainStage(o, w)]
    /\ opWorker' = [o \in Ops |-> DrainWorker(o, w)]
    /\ lastDispatchTier' = [o \in Ops |-> DrainTier(o, w)]
    /\ reserved' = [reserved EXCEPT ![w] = {}]
    /\ UNCHANGED << loadSaturated, loadScore, togglesLeft >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, loadSaturated, loadScore,
                    lastDispatchTier, togglesLeft >>

(* ToggleLoadSat(w): the A4 second predicate (load-saturation), toggled
   INDEPENDENTLY of dispatch-count. *)
ToggleLoadSat(w) ==
    /\ AllowLoadSat
    /\ togglesLeft > 0
    /\ togglesLeft' = togglesLeft - 1
    /\ loadSaturated' = [loadSaturated EXCEPT ![w] = ~@]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, loadScore, lastDispatchTier >>

(* SetLoadScore(w, s): the environment sets a worker's STALE reported load
   score, INDEPENDENTLY of its dispatch-count. This is what creates the
   signal-DISAGREEMENT states: a Tier-B (no-P-headroom) worker can carry a
   LOWER loadScore than a Tier-A (P-headroom) worker, so the model exercises
   the exact case the v2.2 refinement must resolve (holder wins on load, loses
   on tier). Bounded by the same toggle budget so liveness stays checkable. *)
SetLoadScore(w, s) ==
    /\ togglesLeft > 0
    /\ s \in LoadScores
    /\ s # loadScore[w]
    /\ togglesLeft' = togglesLeft - 1
    /\ loadScore' = [loadScore EXCEPT ![w] = s]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, loadSaturated, lastDispatchTier >>

AllCompleted == \A o \in Ops : opStage[o] = "Completed"
Done == AllCompleted /\ UNCHANGED vars

Next ==
    \/ \E o \in Ops, w \in Workers : CacheReserve(o, w)
    \/ \E o \in Ops, w \in Workers : FallbackReserve(o, w)
    \/ \E o \in Ops : Complete(o)
    \/ \E w \in Workers : Evict(w)
    \/ \E w \in Workers : Reconnect(w)
    \/ \E w \in Workers : ToggleLoadSat(w)
    \/ \E w \in Workers, s \in LoadScores : SetLoadScore(w, s)
    \/ Done

Fairness ==
    /\ \A o \in Ops, w \in Workers : WF_vars(CacheReserve(o, w))
    /\ \A o \in Ops, w \in Workers : WF_vars(FallbackReserve(o, w))
    /\ \A o \in Ops : WF_vars(Complete(o))
    /\ \A w \in Workers : WF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  INVARIANTS.
 ***************************************************************************)

(* ── I1 (P-headroom-first) STRENGTHENED — the v2.2 headline property ──
   v1: no CACHE dispatch to a P-gated worker while a P-headroom worker exists.
   v2.2 ADDS: no FALLBACK/OVERFLOW dispatch to a P-gated (Tier-B) worker while
   a viable P-headroom (Tier-A) worker exists — EVEN when the Tier-B worker's
   stale loadScore is LOWER (the signal-disagreement case, design §11 :299).

   Action-time property (same rationale as v1 I1: a Phase-2 dispatch that was
   legal when no P-headroom worker existed must stay legal after a later
   Reconnect regenerates one). Checked as [][I1Step]_vars.

   Legality at decision time:
     - a CACHE dispatch to w is legal iff (SomeViableHasPHeadroom => HasPHeadroom(w))
       [v1, unchanged].
     - a FALLBACK dispatch to w is legal iff, when the gate is ON and any
       ELIGIBLE Tier-A worker exists, w is Tier-A. I.e. the fresh dispatch-count
       steers the overflow — the STRENGTHENED clause. This is derivable from
       FallbackWinner (a Tier-A worker always has a strictly smaller
       FallbackTier than a Tier-B worker, so a Tier-B winner is impossible
       while a Tier-A eligible worker exists), but we assert it as an
       INDEPENDENT invariant so the property BITES if the ranking is wrong. *)

SomeEligibleHasPHeadroom == \E w \in Workers : FallbackEligible(w) /\ HasPHeadroom(w)

CacheDispatchPhase1Legal(w) == SomeViableHasPHeadroom => HasPHeadroom(w)

(* STRENGTHENED overflow clause: if the gate is on and an eligible P-headroom
   (Tier-A) worker exists, the fallback target must be Tier-A. Gate off => no
   constraint (single-tier, today's behavior). *)
FallbackDispatchPHeadroomFirst(w) ==
    (GateOn /\ SomeEligibleHasPHeadroom) => HasPHeadroom(w)

I1Step ==
    \A o \in Ops, w \in Workers :
        /\ ( ( /\ opStage[o] = "Queued"
               /\ opStage'[o] = "Executing"
               /\ lastDispatchTier'[o] = "cache"
               /\ opWorker'[o] = w )
             => CacheDispatchPhase1Legal(w) )
        /\ ( ( /\ opStage[o] = "Queued"
               /\ opStage'[o] = "Executing"
               /\ lastDispatchTier'[o] = "fallback"
               /\ opWorker'[o] = w )
             => FallbackDispatchPHeadroomFirst(w) )

I1_PHeadroomFirst == [][I1Step]_vars

(* I3 (locality bounded): cache-tier locality stays within the P-headroom set.
   Same cache clause as v1 (the fallback is not a locality tier). *)
I3Step ==
    \A o \in Ops, w \in Workers :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ lastDispatchTier'[o] = "cache"
          /\ opWorker'[o] = w )
        => CacheDispatchPhase1Legal(w)
I3_LocalityBounded == [][I3Step]_vars

(* I4 (heterogeneity-safe): p_count==0 workers are always ungated (A5). *)
I4_HeterogeneitySafe ==
    \A w \in Workers : (PCoreCount[w] = 0) => HasPHeadroom(w)

(* I5 (freshness): dispatch-count == live reservation set; no stale count. *)
I5_Freshness ==
    /\ \A w \in Workers : \A o \in reserved[w] :
         workerUp[w] => opStage[o] \in {"Executing"}
    /\ \A o \in Ops :
         (opStage[o] \in {"Queued", "Completed"}) =>
            \A w \in Workers : (workerUp[w] => o \notin reserved[w])

CapacityRespected == \A w \in Workers : workerUp[w] => Running(w) <= Capacity
EvictedEmpty == \A w \in Workers : ~workerUp[w] => reserved[w] = {}

Safety ==
    /\ TypeOK
    /\ I4_HeterogeneitySafe
    /\ I5_Freshness
    /\ CapacityRespected
    /\ EvictedEmpty

------------------------------------------------------------------------------
(***************************************************************************
  I2 (no-wedge / progress) — THE load-bearing re-proof under the SOFT
  preference. The refinement must NOT wedge: even when Tier A is empty (all
  workers P-saturated / no P-headroom), the SOFT fallback still dispatches
  (Tier B). Checked as the structural NoWedge safety lemma (over the full
  joint P-gate × load-sat × load-score cross-product) PLUS the temporal
  leads-to.

  The HardFilter teeth variant MUST break NoWedge here (Tier-A empty →
  hard-exclude leaves nothing → wedge = Phase2NoLift class), proving the
  soft/hard distinction is load-bearing.
 ***************************************************************************)
SomeOpQueued == \E o \in Ops : opStage[o] = "Queued"
SomeWorkerCanAccept == \E w \in Workers : HasFreeSlot(w)

(* A dispatch is enabled iff some cache dispatch is enabled OR (the cache tiers
   all declined AND some fallback WINNER exists). Under the SOFT fallback a
   winner ALWAYS exists when >=1 eligible worker exists (min_by_key over a
   nonempty eligible set), so this holds. Under HardFilter, on an
   all-P-saturated fleet the eligible set is EMPTY → no winner → wedge. *)
DispatchEnabled ==
    \/ \E o \in Ops, w \in Workers : (opStage[o] = "Queued" /\ CacheDispatchable(w))
    \/ \E o \in Ops, w \in Workers :
         ( opStage[o] = "Queued" /\ CacheTiersDeclined /\ FallbackWinner(w) )

NoWedge == (SomeOpQueued /\ SomeWorkerCanAccept) => DispatchEnabled

EventuallyDispatched ==
    \A o \in Ops :
        (opStage[o] = "Queued") ~> (opStage[o] \in {"Executing", "Completed"})

=============================================================================
