------------------- MODULE SchedulerRebalanceV4RankerSingleHolderAsync -------------------
(***************************************************************************
  ATOMICITY DE-FUSION of SchedulerRebalanceV4RankerSingleHolder.tla
  (invariant-prover, atomicity batch D, 2026-07-17).

  ── THE FUSED ATOM in the base SingleHolder model ──
  The base module models the hot-input root-holder set as a FROZEN CONSTANT
  `RootHolders` (e.g. {"w1"}). But in production the root-holder set is an
  ASYNC, keepalive-LAGGED, EVICTABLE signal, NOT a static fact:

    * a worker publishes `cached_directory_digests` (BlobsAvailable) only AFTER
      it MATERIALIZES the input root — POST-materialize, one keepalive later
      (api_worker_scheduler.rs dir-cache is populated from worker updates).
    * that dir-cache entry can later be EVICTED (MokaEvictingMap on the worker /
      worker restart / TTL), and the scheduler learns of the drop only on a
      LATER update.

  So "which workers hold the root" is a lagged async view that CHANGES between
  the scheduler's winner-sample and later dispatches — precisely the atomicity
  gap the batch-D charter names: "a single-holder determination fused with an
  eviction/admission that races it ... evict the last holder of a blob."

  Freezing it as a CONSTANT ERASES every interleaving where the holder set
  changes under the ranker — the classic false atomicity. This module SPLITS the
  atom: `rootHolders` becomes a VARIABLE moved ONLY by two async adversary
  actions, decoupled from occupancy, exactly as V3/V4 already treat the stale
  p_load / load-score signals:

    AcquireRoot(w) : a running/up worker materializes the root and publishes it
                     (BlobsAvailable POST-materialize) — lagged, non-holder -> holder.
    EvictRoot(w)   : a holder DROPS the root (dir-cache eviction / restart) —
                     lagged, holder -> non-holder. MAY evict the LAST holder,
                     driving rootHolders to {} while same-root work is pending.

  The scheduler's cache-eligibility (`CacheTierViable`) now reads the CURRENT
  `rootHolders` view; the winner-sample and the reserve remain one atomic step
  (faithful: both happen under the do_try_match write lock), but the holder view
  can now change in the gap BETWEEN dispatches — which the frozen constant could
  never express.

  ── QUESTION under test ──
  Does de-atomizing the holder set surface a NEW failure the frozen constant hid?
  Candidates: (a) I5_Bounded ceiling breached because a holder-set change lets the
  sole holder re-lift past its ceiling; (b) I6 ranker fooled; (c) a NoWedge when
  the last holder is evicted while same-root work is queued.

  ── RESULT (see cfgs) ──
  NO new safety bug. The checked invariants are all HOLDER-IDENTITY-INDEPENDENT:
    * I5_Bounded / I5_BoundedOverride key on per-worker FRESH Running vs the
      FACTOR ceiling — independent of who holds the root.
    * NoWedge is preserved because the fallback (EvictRoot -> rootHolders={}) is
      P-headroom- AND root-holder-UNGATED (any BaseViable worker).
    * SomeViableHasPHeadroom (the Phase-1/Phase-2 lift trigger) is FLEET-wide, not
      holder-scoped, so holder churn cannot manufacture a spurious lift.
  The FACTOR ceiling remains the sole brake, now proven candidate-count-
  INDEPENDENT *and* holder-DYNAMICS-independent (strengthens design §13 R1).
  The DropCeiling teeth still bite under full holder dynamics (AsyncBugged) —
  proving the ceiling is the genuine recovery mechanism, not an artifact of the
  frozen holder set.  Reachability witnesses confirm the dangerous states (sole
  over-ceiling holder; last holder evicted with work pending) ARE exercised, so
  the HOLD is not vacuous.

  Reuses the V4 ranker machinery unchanged in spirit.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers, Ops, WorkerOrder, MaxToggles, MaxScore,
    PCoreCount, Capacity, THRESHOLD, FACTOR,
    AdmitAllThreshold, DropCeiling, BuggedStaleRankOnly, PrefFlat,
    GateActive, AllowEvict,
    InitRootHolders,   \* initial holder set (was the frozen RootHolders constant)
    HolderBudget       \* bound on async AcquireRoot/EvictRoot events (finiteness)

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
ASSUME InitRootHolders \subseteq Workers
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
ASSUME HolderBudget \in Nat

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}
PLoadVals == {"idle", "busy"}
MAXPREF == 1000

VARIABLES
    opStage, opWorker, reserved, workerUp, pLoad, loadScore,
    lastDispatchWinner, togglesLeft,
    rootHolders,       \* Workers -> BOOLEAN : the DE-ATOMIZED holder view (was a constant)
    holderBudget       \* remaining async holder-change events

vars == << opStage, opWorker, reserved, workerUp, pLoad, loadScore,
           lastDispatchWinner, togglesLeft, rootHolders, holderBudget >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ pLoad \in [Workers -> PLoadVals]
    /\ loadScore \in [Workers -> 0..MaxScore]
    /\ lastDispatchWinner \in [Ops -> WorkerOrNone]
    /\ togglesLeft \in 0..MaxToggles
    /\ rootHolders \in [Workers -> BOOLEAN]
    /\ holderBudget \in 0..HolderBudget

(***************************************************************************
  THE v2 GATE (eligibility) — verbatim from V4 SingleHolder.
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
  CACHE ELIGIBILITY — now reads the DYNAMIC rootHolders view (was: w \in
  RootHolders constant). SomeViableHasPHeadroom stays FLEET-wide (matches
  any_viable_has_p_headroom over all viable candidates — holder-INDEPENDENT).
 ***************************************************************************)
SomeViableHasPHeadroom ==
    \E w \in Workers : BaseViable(w) /\ HasPHeadroom(w)
CacheTierViable(w) ==
    /\ rootHolders[w]                                 \* DE-ATOMIZED per-tier root filter
    /\ BaseViable(w)
    /\ (SomeViableHasPHeadroom => HasPHeadroom(w))

(***************************************************************************
  THE RANKER — §12.1 p_headroom_pref key, verbatim from V4.
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

CacheEligible == { w \in Workers : CacheTierViable(w) }
FallbackEligible == { w \in Workers : BaseViable(w) }
EligibleSet == IF CacheEligible # {} THEN CacheEligible ELSE FallbackEligible

WinnerOf(S) == CHOOSE w \in S : \A x \in S : (x = w) \/ Outranks(w, x)

(***************************************************************************
  Dispatch(o): ranker-driven. Winner-sample + reserve remain ONE atomic step
  (faithful: both under the do_try_match write lock). rootHolders is UNCHANGED
  here — only the async AcquireRoot/EvictRoot move it, so the holder view can
  change in the gap BETWEEN dispatches (the de-atomized interleaving).
 ***************************************************************************)
Dispatch(o) ==
    /\ opStage[o] = "Queued"
    /\ EligibleSet # {}
    /\ LET w == WinnerOf(EligibleSet) IN
         /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
         /\ opWorker' = [opWorker EXCEPT ![o] = w]
         /\ lastDispatchWinner' = [lastDispatchWinner EXCEPT ![o] = w]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ UNCHANGED << workerUp, pLoad, loadScore, togglesLeft, rootHolders, holderBudget >>

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
    /\ UNCHANGED << workerUp, pLoad, loadScore, togglesLeft, rootHolders, holderBudget >>

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
    \* a disconnecting worker also drops its published dir-cache (holder view clears)
    /\ rootHolders' = [rootHolders EXCEPT ![w] = FALSE]
    /\ UNCHANGED << pLoad, loadScore, togglesLeft, holderBudget >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, pLoad, loadScore,
                    lastDispatchWinner, togglesLeft, rootHolders, holderBudget >>

StaleLoadScore(w, s) ==
    /\ togglesLeft > 0
    /\ s \in 0..MaxScore
    /\ s # loadScore[w]
    /\ togglesLeft' = togglesLeft - 1
    /\ loadScore' = [loadScore EXCEPT ![w] = s]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, pLoad,
                    lastDispatchWinner, rootHolders, holderBudget >>

StalePLoad(w, pl) ==
    /\ togglesLeft > 0
    /\ pl \in PLoadVals
    /\ pl # pLoad[w]
    /\ togglesLeft' = togglesLeft - 1
    /\ pLoad' = [pLoad EXCEPT ![w] = pl]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, loadScore,
                    lastDispatchWinner, rootHolders, holderBudget >>

(***************************************************************************
  THE DE-ATOMIZED HOLDER-SET ADVERSARY (the split atom).

  AcquireRoot(w): a running/up worker materializes the root and publishes it
    (BlobsAvailable POST-materialize) — lagged, non-holder -> holder. Guarded on
    "w is running an op" to model materialize-then-publish (a worker only holds a
    root after it has actually run a same-root action).

  EvictRoot(w): a holder drops the root (dir-cache eviction / restart) — lagged,
    holder -> non-holder. UNGUARDED by "is it the last holder": it CAN drive
    rootHolders to {} while same-root work is still pending — the exact
    "evict the last holder" interleaving the frozen constant erased.
 ***************************************************************************)
AcquireRoot(w) ==
    /\ holderBudget > 0
    /\ workerUp[w]
    /\ ~rootHolders[w]
    /\ \E o \in Ops : (opWorker[o] = w /\ opStage[o] = "Executing")
    /\ rootHolders' = [rootHolders EXCEPT ![w] = TRUE]
    /\ holderBudget' = holderBudget - 1
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, pLoad, loadScore,
                    lastDispatchWinner, togglesLeft >>

EvictRoot(w) ==
    /\ holderBudget > 0
    /\ rootHolders[w]
    /\ rootHolders' = [rootHolders EXCEPT ![w] = FALSE]
    /\ holderBudget' = holderBudget - 1
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, pLoad, loadScore,
                    lastDispatchWinner, togglesLeft >>

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ pLoad = [w \in Workers |-> "idle"]
    /\ loadScore = [w \in Workers |-> 0]
    /\ lastDispatchWinner = [o \in Ops |-> NONE]
    /\ togglesLeft = MaxToggles
    /\ rootHolders = [w \in Workers |-> w \in InitRootHolders]
    /\ holderBudget = HolderBudget

AllCompleted == \A o \in Ops : opStage[o] = "Completed"
Done == AllCompleted /\ UNCHANGED vars

Next ==
    \/ \E o \in Ops : Dispatch(o)
    \/ \E o \in Ops : Complete(o)
    \/ \E w \in Workers : Evict(w)
    \/ \E w \in Workers : Reconnect(w)
    \/ \E w \in Workers, s \in 0..MaxScore : StaleLoadScore(w, s)
    \/ \E w \in Workers, pl \in PLoadVals : StalePLoad(w, pl)
    \/ \E w \in Workers : AcquireRoot(w)
    \/ \E w \in Workers : EvictRoot(w)
    \/ Done

Fairness ==
    /\ \A o \in Ops : WF_vars(Dispatch(o))
    /\ \A o \in Ops : WF_vars(Complete(o))
    /\ \A w \in Workers : WF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  INVARIANTS — carried verbatim from V4 SingleHolder (must survive de-atomization).
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

I5_Bounded ==
    \A w \in Workers :
        (workerUp[w] /\ PCoreCount[w] > 0) => Running(w) <= PCoreCount[w] * FACTOR

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

------------------------------------------------------------------------------
(***************************************************************************
  REACHABILITY WITNESSES (non-vacuity — run as INVARIANTS EXPECTED TO VIOLATE;
  the violation TRACE is the positive proof the dangerous de-atomized state is
  actually exercised, so the safety HOLD above is not vacuous).
 ***************************************************************************)

(* (W1) The holder set can reach EMPTY while same-root work is still queued —
   the "evict the last holder" interleaving the frozen constant could never
   reach. NEGATED: "never (no holder AND an op still queued)". MUST VIOLATE. *)
NoHolderWithWorkPending == ~( (\A w \in Workers : ~rootHolders[w])
                              /\ (\E o \in Ops : opStage[o] = "Queued") )
LastHolderEvictedReached == NoHolderWithWorkPending

(* (W2) The sole-holder-over-ceiling state (the §13 R2 residual) is still reached
   under holder dynamics. NEGATED. MUST VIOLATE. *)
SoleHolderIsOverrideAdmit ==
    /\ CacheEligible # {}
    /\ \A e \in CacheEligible : ~GenuineFreeSlot(e)
SoleHolderOverrideReached == ~SoleHolderIsOverrideAdmit

(* (W3) The holder set genuinely CHANGES during a run (a non-initial holder
   acquires OR an initial holder drops) — proves AcquireRoot/EvictRoot fire and
   the atom is really split, not dead code. NEGATED against the initial view. *)
HolderSetChangedReached == ( rootHolders = [w \in Workers |-> w \in InitRootHolders] )

=============================================================================
