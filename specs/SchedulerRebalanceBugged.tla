---------------------- MODULE SchedulerRebalanceBugged ----------------------
(***************************************************************************
  TEETH CHECK for SchedulerRebalance. Three injectable M1 defects, selected by
  the `Bug` constant, each of which MUST make TLC produce the expected
  violation — proving the spec's invariants actually BITE:

    Bug = "NoPHeadroomGate"
       CacheTierViable DROPS the M1 P-headroom conjunct. The cache tiers no
       longer exclude a P-full worker in Phase 1. EXPECT: I1_PHeadroomFirst /
       I3_LocalityBounded VIOLATED — a cache dispatch lands on a P-full worker
       while a distinct viable worker still has P-headroom. This is the exact
       G3 sole-holder over-concentration the design fixes.

    Bug = "NoA5Guard"
       HasPHeadroom drops the `p_core_count == 0 => always headroom` guard, so
       a p_count=0 worker is PERMANENTLY P-gated. EXPECT: I4_HeterogeneitySafe
       VIOLATED (a p_count=0 worker lacks headroom) AND, on a fleet where the
       ONLY worker is p_count=0, a wedge (EventuallyDispatched VIOLATED) — the
       frozen-out worker the A5 reviewers flagged.

    Bug = "Phase2NoLift"
       The gate NEVER lifts: CacheTierViable requires HasPHeadroom
       UNCONDITIONALLY (drops the `SomeViableHasPHeadroom =>` guard), so when
       every worker is at p_count NO worker is cache-viable AND the fallback...
       (the fallback still fires — so to make the wedge observable this variant
       ALSO routes the fallback through the broken gate, modeling a botched
       implementation that put the P-gate in the shared viability helper the
       fallback also consults). EXPECT: NoWedge VIOLATED / EventuallyDispatched
       VIOLATED on the all-at-p_count fleet — the write-once-dispatch-never
       wedge (the scheduler analogue of the 2026-05-08 debacle).

  Everything else is copied VERBATIM from SchedulerRebalance.tla; only the two
  gate operators (CacheTierViable, HasPHeadroom) and, for Phase2NoLift, the
  fallback gate, change.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Workers, Ops, PCoreCount, Capacity, AllowEvict, AllowLoadSat,
          MaxToggles, Bug

PCoreCount_Het    == [w \in Workers |->
                        IF w = "w1" THEN 1
                        ELSE IF w = "w2" THEN 2
                        ELSE IF w = "w3" THEN 0
                        ELSE 1]
PCoreCount_All1   == [w \in Workers |-> 1]
PCoreCount_All2   == [w \in Workers |-> 2]
PCoreCount_Zero   == [w \in Workers |-> 0]     \* fleet of only p_count=0 workers

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME PCoreCount \in [Workers -> 0..3]
ASSUME Capacity \in 1..4
ASSUME AllowEvict \in BOOLEAN
ASSUME AllowLoadSat \in BOOLEAN
ASSUME MaxToggles \in Nat
ASSUME Bug \in {"NoPHeadroomGate", "NoA5Guard", "Phase2NoLift"}

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}

VARIABLES opStage, opWorker, reserved, workerUp, loadSaturated,
          lastDispatchTier, togglesLeft
vars == << opStage, opWorker, reserved, workerUp, loadSaturated,
           lastDispatchTier, togglesLeft >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ loadSaturated \in [Workers -> BOOLEAN]
    /\ lastDispatchTier \in [Ops -> {"none", "cache", "fallback"}]
    /\ togglesLeft \in 0..MaxToggles

Running(w) == Cardinality(reserved[w])

(* BUG NoA5Guard: drop the p_core_count==0 always-headroom guard. *)
HasPHeadroom(w) ==
    IF Bug = "NoA5Guard"
    THEN Running(w) < PCoreCount[w]                       \* no A5 guard
    ELSE (PCoreCount[w] = 0) \/ (Running(w) < PCoreCount[w])

HasFreeSlot(w) == workerUp[w] /\ Running(w) < Capacity
LoadSaturated(w) == loadSaturated[w]
BaseViable(w) == HasFreeSlot(w)
SomeViableHasPHeadroom == \E w \in Workers : BaseViable(w) /\ HasPHeadroom(w)

(* BUG NoPHeadroomGate: drop the M1 P-headroom conjunct from cache viability.
   BUG Phase2NoLift: require HasPHeadroom UNCONDITIONALLY (gate never lifts). *)
CacheTierViable(w) ==
    CASE Bug = "NoPHeadroomGate" -> BaseViable(w)          \* gate removed
      [] Bug = "Phase2NoLift"    -> BaseViable(w) /\ HasPHeadroom(w)  \* never lifts
      [] OTHER                   -> BaseViable(w)
                                    /\ (SomeViableHasPHeadroom => HasPHeadroom(w))

ViableCount == Cardinality({w \in Workers : BaseViable(w)})
AllViableLoadSaturated == \A w \in Workers : BaseViable(w) => LoadSaturated(w)
SaturationFallThrough == ViableCount > 0 /\ AllViableLoadSaturated

CacheDispatchable(w) == CacheTierViable(w) /\ ~SaturationFallThrough

(* BUG Phase2NoLift: the botched impl also routed the fallback through the
   broken P-gate (put it in the shared viability helper), so when every worker
   is at p_count the fallback ALSO refuses => total wedge. Otherwise the
   fallback is P-gate-ungated (faithful). *)
FallbackDispatchable(w) ==
    IF Bug = "Phase2NoLift"
    THEN BaseViable(w) /\ HasPHeadroom(w)                 \* fallback also gated (bug)
    ELSE BaseViable(w)

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ loadSaturated = [w \in Workers |-> FALSE]
    /\ lastDispatchTier = [o \in Ops |-> "none"]
    /\ togglesLeft = MaxToggles

CacheReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ CacheDispatchable(w)
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "cache"]
    /\ UNCHANGED << workerUp, loadSaturated, togglesLeft >>

FallbackReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ FallbackDispatchable(w)
    /\ ~(\E ww \in Workers : CacheDispatchable(ww))
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "fallback"]
    /\ UNCHANGED << workerUp, loadSaturated, togglesLeft >>

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
    /\ UNCHANGED << workerUp, loadSaturated, togglesLeft >>

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
    /\ UNCHANGED << loadSaturated, togglesLeft >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, loadSaturated, lastDispatchTier, togglesLeft >>

ToggleLoadSat(w) ==
    /\ AllowLoadSat
    /\ togglesLeft > 0
    /\ togglesLeft' = togglesLeft - 1
    /\ loadSaturated' = [loadSaturated EXCEPT ![w] = ~@]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, lastDispatchTier >>

AllCompleted == \A o \in Ops : opStage[o] = "Completed"
Done == AllCompleted /\ UNCHANGED vars

Next ==
    \/ \E o \in Ops, w \in Workers : CacheReserve(o, w)
    \/ \E o \in Ops, w \in Workers : FallbackReserve(o, w)
    \/ \E o \in Ops : Complete(o)
    \/ \E w \in Workers : Evict(w)
    \/ \E w \in Workers : Reconnect(w)
    \/ \E w \in Workers : ToggleLoadSat(w)
    \/ Done

Fairness ==
    /\ \A o \in Ops, w \in Workers : WF_vars(CacheReserve(o, w))
    /\ \A o \in Ops, w \in Workers : WF_vars(FallbackReserve(o, w))
    /\ \A o \in Ops : WF_vars(Complete(o))
    /\ \A w \in Workers : WF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
CacheDispatchPhase1Legal(w) == SomeViableHasPHeadroom => HasPHeadroom(w)

I1CacheStep ==
    \A o \in Ops, w \in Workers :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ lastDispatchTier'[o] = "cache"
          /\ opWorker'[o] = w )
        => CacheDispatchPhase1Legal(w)

I1_PHeadroomFirst == [][I1CacheStep]_vars
I3_LocalityBounded == [][I1CacheStep]_vars

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
DispatchEnabled ==
    \/ \E o \in Ops, w \in Workers : (opStage[o] = "Queued" /\ CacheDispatchable(w))
    \/ \E o \in Ops, w \in Workers :
         (opStage[o] = "Queued" /\ FallbackDispatchable(w)
            /\ ~(\E ww \in Workers : CacheDispatchable(ww)))
NoWedge == (SomeOpQueued /\ SomeWorkerCanAccept) => DispatchEnabled

EventuallyDispatched ==
    \A o \in Ops :
        (opStage[o] = "Queued") ~> (opStage[o] \in {"Executing", "Completed"})

=============================================================================
