--------------------- MODULE SchedulerRebalanceV4RankerSingleHolder ---------------------
(***************************************************************************
  M1 v2 — the SINGLE-HOLDER residual (design §13 Refinement 2). The FIRST
  ranker prove (SchedulerRebalanceV4Ranker.tla) modeled the cache-eligible set
  as ALL viable workers (`CacheEligible == {w : CacheTierViable(w)}`) — every
  worker holds the hot root — which is the WORST case for I6 (the largest
  eligible set, most chances for a free-slot worker to be beaten). That prove
  DELIBERATELY skipped the case §13 R2 names: exactly ONE worker holds the hot
  root (common in a COLD `bazel build //...` burst — the dir-cache populates
  POST-materialize, so one worker holds the root first), and it is over its
  P-slot count (an override-admit).

  In THAT topology `p_headroom_pref` is INERT — it is a relative `min` over a
  ONE-element candidate set, so the sole holder wins every same-root dispatch
  regardless of its pref value. §13 R2 rules this case ACCEPTED (keeping
  same-root work on the sole holder, which HAS the materialized inputs, IS the
  intended cache-affinity) and CEILING-BRAKED: the FACTOR ceiling
  (`Running < p_count*FACTOR`) is an ABSOLUTE, candidate-count-INDEPENDENT brake.

  §13 R2 requires the implementation-phase prove to add EXPLICIT single-holder
  teeth so the coverage is HONEST:
    - I6_RankerConsistency is VACUOUSLY satisfied here — its antecedent "some
      ELIGIBLE worker has a genuine free P slot" is FALSE (the only eligible
      worker, the sole root-holder, is the over-p_count override-admit; the
      free-slot peers exist but do NOT hold the root, so they are NOT eligible
      for this same-root cache dispatch). I6 passing does NOT cover this case.
    - I5_BoundedOverride (admission COUNT <= p_count*FACTOR) is the GOVERNING
      bound and HOLDS — the sole holder is CAPPED at the ceiling, not unbounded.
    - Teeth (DropCeiling): drop the clause-3 ceiling for the single holder =>
      MUST show UNBOUNDED Running (the residual would be unbounded without the
      ceiling — the ceiling is what makes the accepted single-holder case safe).

  This module is ADDITIVE (design §13 "recorded so it is not lost"). It reuses
  the V4 ranker machinery UNCHANGED in spirit but replaces `CacheEligible` with
  a ROOT-HOLDER-restricted eligible set: only workers in `RootHolders` can win
  a cache dispatch. Set `RootHolders = {"w1"}` and make w1 an override-admit to
  model the single-holder case; w2/w3 stay free-slot NON-holders (the peers that
  I6 in the multi-holder spec would have promoted, but which are cache-INELIGIBLE
  here because they lack the root).

  Reference: .claude/audits/scheduler-m1-v2-pload-refinement-design-2026-07-01.md
    §13 Refinement 1  Q2 single-holder residual RULED accepted (FACTOR ceiling
                      is the absolute brake; candidate-count-independent)
    §13 Refinement 2  I6 restated; single-holder governed by I5_BoundedOverride;
                      "implementation-phase prove MUST add the explicit
                      |candidates|=1, sole=override-admit teeth so coverage is
                      honest (I6 vacuous, I5_Bounded governing)"

  Production citation (why single-holder is the common cold-burst shape):
    a worker publishes `cached_directory_digests` only AFTER it materializes the
    input root (BlobsAvailable is POST-materialize). During a cold burst the
    FIRST worker to run an action for a root is the ONLY holder until it finishes
    and republishes — so same-root follow-on actions have a ONE-element
    root-holder candidate set. `dir_cache_winner` (:1725) iterates `candidates`
    and keeps root/subtree holders only; with one holder it is a `min` over {w1}.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers, Ops, WorkerOrder, MaxToggles, MaxScore,
    PCoreCount, Capacity, THRESHOLD, FACTOR,
    AdmitAllThreshold, DropCeiling, BuggedStaleRankOnly, PrefFlat,
    GateActive, AllowEvict,
    RootHolders        \* NEW: the subset of Workers that hold the hot input root.
                       \* For the single-holder case set this to a SINGLETON, e.g.
                       \* {"w1"}. The cache-eligible set is restricted to these.

(* Per-cfg p_core_count assignments (TLC cfg cannot parse function literals). *)
PCoreCount_Het    == [w \in Workers |->
                        IF w = "w1" THEN 1
                        ELSE IF w = "w2" THEN 2
                        ELSE IF w = "w3" THEN 0
                        ELSE 1]
PCoreCount_All1   == [w \in Workers |-> 1]
PCoreCount_All2   == [w \in Workers |-> 2]

Order_w123 == << "w1", "w2", "w3" >>

PIdlePct == 0
PBusyPct == 100
PLoadPct(pl) == IF pl = "idle" THEN PIdlePct ELSE PBusyPct

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME RootHolders \subseteq Workers
ASSUME PCoreCount \in [Workers -> 0..3]
ASSUME Capacity \in 1..6
ASSUME THRESHOLD \in 0..101
ASSUME FACTOR \in 1..3
ASSUME MaxScore \in 1..4
ASSUME AdmitAllThreshold \in BOOLEAN
ASSUME DropCeiling \in BOOLEAN
ASSUME BuggedStaleRankOnly \in BOOLEAN
ASSUME PrefFlat \in BOOLEAN
ASSUME GateActive \in BOOLEAN
ASSUME AllowEvict \in BOOLEAN
ASSUME MaxToggles \in Nat

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}
PLoadVals == {"idle", "busy"}
MAXPREF == 1000

VARIABLES
    opStage, opWorker, reserved, workerUp, pLoad, loadScore,
    lastDispatchWinner, togglesLeft

vars == << opStage, opWorker, reserved, workerUp, pLoad, loadScore,
           lastDispatchWinner, togglesLeft >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ pLoad \in [Workers -> PLoadVals]
    /\ loadScore \in [Workers -> 0..MaxScore]
    /\ lastDispatchWinner \in [Ops -> WorkerOrNone]
    /\ togglesLeft \in 0..MaxToggles

(***************************************************************************
  THE v2 GATE (eligibility) — verbatim from V4 / design §2.
 ***************************************************************************)
Running(w) == Cardinality(reserved[w])

PReportedIdle(w) == AdmitAllThreshold \/ (PLoadPct(pLoad[w]) < THRESHOLD)
Clause3Ceiling(w) == DropCeiling \/ (Running(w) < PCoreCount[w] * FACTOR)
OverrideEligible(w) == PReportedIdle(w) /\ Clause3Ceiling(w)

HasPHeadroom(w) ==
    \/ PCoreCount[w] = 0
    \/ Running(w) < PCoreCount[w]
    \/ OverrideEligible(w)

HasFreeSlot(w) == workerUp[w] /\ Running(w) < Capacity
BaseViable(w) == HasFreeSlot(w)

(***************************************************************************
  CACHE ELIGIBILITY — THE ONE STRUCTURAL DIFFERENCE FROM V4.

  V4: CacheEligible == {w : CacheTierViable(w)}  (every worker holds the root).
  HERE: a worker can win a CACHE dispatch only if it HOLDS the root
  (w \in RootHolders). This models `dir_cache_winner` (:1725) iterating
  `candidates` and keeping only root/subtree holders. With RootHolders a
  singleton, |CacheEligible| = 1 whenever that holder is viable — the
  single-holder topology §13 R2 requires.

  CRITICAL FIDELITY POINT (verified against api_worker_scheduler.rs:1584-1617):
  the Phase-1-gate / Phase-2-lift decision `p_gate_active =
  p_headroom_gate_enabled && any_viable_has_p_headroom` computes
  `any_viable_has_p_headroom` over ALL VIABLE `candidates` (the platform-matched
  fleet), NOT just the root-holders. Root-holding is a PER-TIER filter inside
  each cache tier (`has_root_match || has_subtree_match`, :1732), applied ON TOP
  of the fleet-wide gate. So the lift fires only when NO viable worker anywhere
  has P-headroom. This is load-bearing for the single-holder case: when the sole
  holder w1 saturates its ceiling, the FREE-SLOT PEERS (w2/w3) still have
  P-headroom, so `any_viable_has_p_headroom` stays TRUE, the gate stays ACTIVE
  (no lift), w1 is correctly EXCLUDED from the cache tier (it fails
  `has_p_headroom`), the cache tiers DECLINE, and the overflow spills to the
  ungated LRU/MRU fallback which spreads to the peers. THAT is what caps w1 at
  the ceiling. (An earlier version scoped the lift to root-holders only — WRONG:
  it let the sole holder re-lift itself past the ceiling, a spec bug, not a
  design flaw.)
 ***************************************************************************)
SomeViableHasPHeadroom ==            \* FLEET-WIDE — matches any_viable_has_p_headroom
    \E w \in Workers : BaseViable(w) /\ HasPHeadroom(w)
CacheTierViable(w) ==
    /\ w \in RootHolders             \* per-tier root filter (has_root_match)
    /\ BaseViable(w)
    /\ (SomeViableHasPHeadroom => HasPHeadroom(w))   \* fleet-wide gate/lift

(***************************************************************************
  THE RANKER — the §12.1 p_headroom_pref key, verbatim from V4.
 ***************************************************************************)
GenuineFreeSlot(w) == (PCoreCount[w] = 0) \/ (Running(w) < PCoreCount[w])

PHeadroomPref(w) ==
    IF BuggedStaleRankOnly THEN 0
    ELSE IF ~GateActive THEN 0
    ELSE IF GenuineFreeSlot(w) THEN 0
    ELSE IF OverrideEligible(w)
         THEN (IF PrefFlat THEN 1 ELSE 1 + (Running(w) - PCoreCount[w]))
    ELSE MAXPREF

KeyLt(a, b) ==
    \/ PHeadroomPref(a) < PHeadroomPref(b)
    \/ ( PHeadroomPref(a) = PHeadroomPref(b) /\ loadScore[a] < loadScore[b] )

OrderIdx(w) == CHOOSE i \in 1..Len(WorkerOrder) : WorkerOrder[i] = w

Outranks(a, b) ==
    \/ KeyLt(a, b)
    \/ ( ~KeyLt(a, b) /\ ~KeyLt(b, a) /\ OrderIdx(a) < OrderIdx(b) )

(* Cache regime: eligible = root-holders passing the gate. Fallback regime
   (no root-holder eligible): P-headroom-UNGATED over ALL BaseViable workers
   — the LRU/MRU backstop, which does NOT filter to root-holders. The cascade
   is: cache tiers (root-holders) first, else the LRU/MRU fallback. *)
CacheEligible == { w \in Workers : CacheTierViable(w) }
FallbackEligible == { w \in Workers : BaseViable(w) }
EligibleSet == IF CacheEligible # {} THEN CacheEligible ELSE FallbackEligible

WinnerOf(S) == CHOOSE w \in S : \A x \in S : (x = w) \/ Outranks(w, x)

Dispatch(o) ==
    /\ opStage[o] = "Queued"
    /\ EligibleSet # {}
    /\ LET w == WinnerOf(EligibleSet) IN
         /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
         /\ opWorker' = [opWorker EXCEPT ![o] = w]
         /\ lastDispatchWinner' = [lastDispatchWinner EXCEPT ![o] = w]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ UNCHANGED << workerUp, pLoad, loadScore, togglesLeft >>

Complete(o) ==
    /\ opStage[o] = "Executing"
    /\ opWorker[o] \in Workers
    /\ workerUp[opWorker[o]]
    /\ o \in reserved[opWorker[o]]
    /\ LET w == opWorker[o] IN
        reserved' = [reserved EXCEPT ![w] = @ \ {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Completed"]
    /\ opWorker' = [opWorker EXCEPT ![o] = NONE]
    /\ lastDispatchWinner' = [lastDispatchWinner EXCEPT ![o] = NONE]
    /\ UNCHANGED << workerUp, pLoad, loadScore, togglesLeft >>

DrainStage(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN "Queued" ELSE opStage[o]
DrainWorker(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN NONE ELSE opWorker[o]
DrainWinner(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN NONE ELSE lastDispatchWinner[o]

Evict(w) ==
    /\ AllowEvict
    /\ workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = FALSE]
    /\ opStage' = [o \in Ops |-> DrainStage(o, w)]
    /\ opWorker' = [o \in Ops |-> DrainWorker(o, w)]
    /\ lastDispatchWinner' = [o \in Ops |-> DrainWinner(o, w)]
    /\ reserved' = [reserved EXCEPT ![w] = {}]
    /\ UNCHANGED << pLoad, loadScore, togglesLeft >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, pLoad, loadScore,
                    lastDispatchWinner, togglesLeft >>

StaleLoadScore(w, s) ==
    /\ togglesLeft > 0
    /\ s \in 0..MaxScore
    /\ s # loadScore[w]
    /\ togglesLeft' = togglesLeft - 1
    /\ loadScore' = [loadScore EXCEPT ![w] = s]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, pLoad,
                    lastDispatchWinner >>

StalePLoad(w, pl) ==
    /\ togglesLeft > 0
    /\ pl \in PLoadVals
    /\ pl # pLoad[w]
    /\ togglesLeft' = togglesLeft - 1
    /\ pLoad' = [pLoad EXCEPT ![w] = pl]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, loadScore,
                    lastDispatchWinner >>

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ pLoad = [w \in Workers |-> "idle"]
    /\ loadScore = [w \in Workers |-> 0]
    /\ lastDispatchWinner = [o \in Ops |-> NONE]
    /\ togglesLeft = MaxToggles

AllCompleted == \A o \in Ops : opStage[o] = "Completed"
Done == AllCompleted /\ UNCHANGED vars

Next ==
    \/ \E o \in Ops : Dispatch(o)
    \/ \E o \in Ops : Complete(o)
    \/ \E w \in Workers : Evict(w)
    \/ \E w \in Workers : Reconnect(w)
    \/ \E w \in Workers, s \in 0..MaxScore : StaleLoadScore(w, s)
    \/ \E w \in Workers, pl \in PLoadVals : StalePLoad(w, pl)
    \/ Done

Fairness ==
    /\ \A o \in Ops : WF_vars(Dispatch(o))
    /\ \A o \in Ops : WF_vars(Complete(o))
    /\ \A w \in Workers : WF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  I6_RankerConsistency — carried verbatim from V4. In the single-holder cfg its
  ANTECEDENT ("some ELIGIBLE worker has a genuine free P slot") is FALSE on every
  cache dispatch (the only eligible worker is the over-p_count sole holder), so
  I6 is VACUOUSLY TRUE — it PASSES but covers NOTHING here. That vacuity is the
  whole point of §13 R2: I6 does NOT govern the single-holder case.

  We make the vacuity OBSERVABLE with the auxiliary I6_AntecedentEverTrue below.
 ***************************************************************************)
I6Step ==
    \A o \in Ops :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ opWorker'[o] \in Workers )
        =>
        LET winner == opWorker'[o]
            elig == EligibleSet
        IN  GateActive =>
              ( (\E e \in elig : GenuineFreeSlot(e)) => GenuineFreeSlot(winner) )
I6_RankerConsistency == [][I6Step]_vars

(***************************************************************************
  I5_BoundedOverride — THE GOVERNING BOUND for the single-holder case (design
  §13 R2). ACTION-TIME: on every Dispatch into a gate-governing (Phase-1) state,
  the gated winner was strictly below the ceiling at decision time. This is what
  actually CAPS the sole holder at p_count*FACTOR. Carried from V4, retargeted
  to the fleet-wide `SomeViableHasPHeadroom` (matches any_viable_has_p_headroom). *)
I5_OverrideAdmitLegal(w) ==
    (SomeViableHasPHeadroom /\ PCoreCount[w] > 0)
        => Running(w) < PCoreCount[w] * FACTOR
I5_BoundedOverrideStep ==
    \A o \in Ops :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ opWorker'[o] \in Workers )
        => I5_OverrideAdmitLegal(opWorker'[o])
I5_BoundedOverride == [][I5_BoundedOverrideStep]_vars

(***************************************************************************
  I5_Bounded (PURE-STATE) — the absolute per-worker ceiling. On the no-lift
  single-holder topology (THRESHOLD=101 so eligibility never depends on stale
  p_load; AllowEvict=FALSE) it HOLDS: the sole holder is capped at
  p_count*FACTOR. With DropCeiling=TRUE (teeth) it is VIOLATED — the sole holder
  fills PAST the ceiling to the slot Capacity, demonstrating the ceiling is the
  ONLY thing bounding the single-holder residual (remove it => unbounded, i.e.
  bounded only by Capacity which we set large to expose it). *)
I5_Bounded ==
    \A w \in Workers :
        (workerUp[w] /\ PCoreCount[w] > 0) => Running(w) <= PCoreCount[w] * FACTOR

(***************************************************************************
  I6VacuousWhenSoleHolderOverride — the HONESTY WITNESS (design §13 R2
  "coverage honest"), CORRECTLY SCOPED.

  §13 R2's claim is NOT "I6 is vacuous in every single-holder state" — at t=0
  (and while the sole holder is still below p_count) the holder ITSELF has a
  genuine free slot, so I6's antecedent fires and I6 is trivially satisfied by
  the holder-wins-and-is-free case. The claim is narrower and exact: in the
  state §13 R2 rules ceiling-braked — the SOLE holder is an OVERRIDE-ADMIT (over
  p_count) — I6's antecedent is FALSE, because the only cache-eligible worker is
  that over-p_count holder and the free-slot peers do NOT hold the root (are not
  cache-eligible). So I6 passing there covers NOTHING; I5_Bounded governs.

  This predicate asserts EXACTLY that: whenever the cache regime is active AND
  the sole eligible holder is over its p_count (an override-admit, i.e. NOT a
  genuine free slot), the cache-eligible set contains NO genuine-free-slot
  worker — I6's antecedent is unsatisfiable, so I6 is vacuous in precisely the
  §13 R2 case. If this HOLDS, the honest-coverage claim is proven: I6's PASS on
  the SingleHolderCeiling cfg is genuinely vacuous in the case that matters, and
  the sole-holder pile is bounded by I5_Bounded, not by I6.

  (Contrast: in the multi-holder V4 spec, the peers DO hold the root and ARE
  cache-eligible, so I6's antecedent fires and I6 is substantive there — which is
  why the two specs together honestly split the coverage.)

  We prove it in TWO complementary pieces:

  (a) REACHABILITY WITNESS — SoleHolderOverrideReached (a NEGATED invariant, run
      as an INVARIANT that MUST be VIOLATED). It asserts "the §13 R2 case is
      NEVER reached." TLC violating it — with a trace showing the sole holder w1
      over p_count (Running >= 1 > PCoreCount 1... i.e. Running = PCoreCount, an
      override-admit) as the ONLY cache-eligible worker while free-slot peers
      exist — PROVES the case is genuinely exercised, not vacuously absent. A
      cfg where this HELD would be a no-op (the tell that the topology fails to
      drive the residual — memory idiom_deterministic_winner_ranker_tla). So the
      Ceiling cfg lists it under INVARIANTS and EXPECTS a violation, and we read
      the trace as the positive witness.

  (b) VACUITY OF I6 IN THAT CASE — SoleHolderMakesI6Vacuous (a genuine STATE
      invariant that HOLDS). It asserts: in EVERY reachable state where the sole
      holder is an over-p_count override-admit and is the only cache-eligible
      worker, no cache-eligible worker has a genuine free slot — so I6's
      antecedent (\E eligible with a free slot) is FALSE, i.e. I6 is vacuous
      exactly there. (This is NOT the tautology `(\A ¬F) => (¬\E F)`: the
      antecedent additionally requires the peers-have-free-slots context — that
      a free slot EXISTS in the fleet — so the predicate says "a free slot
      exists but not among the cache-eligible", which is the substantive claim.)
 ***************************************************************************)
SoleHolderIsOverrideAdmit ==
    /\ CacheEligible # {}
    /\ \A e \in CacheEligible : ~GenuineFreeSlot(e)   \* every eligible = override-admit

\* (a) run as an INVARIANT expected to be VIOLATED — the violation trace is the
\*     positive reachability witness that the §13 R2 case is exercised.
SoleHolderOverrideReached == ~SoleHolderIsOverrideAdmit

\* (b) HONEST vacuity: a free slot exists in the FLEET but NOT among the
\*     cache-eligible set, so I6's antecedent is false — I6 is vacuous here. Holds.
SoleHolderMakesI6Vacuous ==
    ( SoleHolderIsOverrideAdmit /\ (\E w \in Workers : GenuineFreeSlot(w) /\ HasFreeSlot(w)) )
        => ( ~ \E e \in CacheEligible : GenuineFreeSlot(e) )

(***************************************************************************
  Carried structural invariants (from V4).
 ***************************************************************************)
CacheDispatchPhase1Legal(w) == SomeViableHasPHeadroom => HasPHeadroom(w)
I1CacheStep ==
    \A o \in Ops :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ opWorker'[o] \in Workers
          /\ CacheEligible # {} )
        => CacheDispatchPhase1Legal(opWorker'[o])
I1_PHeadroomFirst == [][I1CacheStep]_vars

I4_HeterogeneitySafe ==
    \A w \in Workers : (PCoreCount[w] = 0) => HasPHeadroom(w)

I5_Freshness ==
    /\ \A w \in Workers : \A o \in reserved[w] :
         workerUp[w] => opStage[o] \in {"Executing"}
    /\ \A o \in Ops :
         (opStage[o] \in {"Queued", "Completed"}) =>
            \A w \in Workers : (workerUp[w] => o \notin reserved[w])

CapacityRespected == \A w \in Workers : workerUp[w] => Running(w) <= Capacity
EvictedEmpty == \A w \in Workers : ~workerUp[w] => reserved[w] = {}

SomeOpQueued == \E o \in Ops : opStage[o] = "Queued"
SomeWorkerCanAccept == \E w \in Workers : HasFreeSlot(w)
DispatchEnabled == \E o \in Ops : (opStage[o] = "Queued" /\ EligibleSet # {})
NoWedge == (SomeOpQueued /\ SomeWorkerCanAccept) => DispatchEnabled

=============================================================================
