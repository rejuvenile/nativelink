--------------------------- MODULE SchedulerOomSafety ---------------------------
(***************************************************************************
  OOM-SAFETY of the measured-override / cap-removal scheduler
  (task-resource-profile design v3, Phase 3 -- pre-implementation de-risk).

  THE #1 PROPERTY (user's words):
    "A worker must never run a concurrent set of actions whose TRUE peak
     memory exceeds its RAM (no OOM / no jetsam-SIGKILL)."
  Formalized as the SAFETY invariant NoOOM (a plain state invariant --
  an over-commit at ANY reachable state is a real OOM, so it is NOT
  quiescence-gated, unlike WorkConserving below).

  ------------------------------------------------------------------------
  THE HARD PART (reviewers' most-dangerous-assumption, b861c68b red-team;
  design v3 MEASUREMENT-FITNESS CAVEAT, sec 7):

    The scheduler does NOT know an action's true peak memory.  It has a
    MEASURED estimate that is a LOWER bound: the poll-based phys_footprint
    reap misses between-poll concurrent multi-process spikes (LTO / make -j
    parallel codegen), so estimate(a) <= true_peak(a).  The measured-
    override REPLACES the client-declared memory_kb with this estimate
    (both directions); this model folds the override into Estimate(a) and
    keeps ONLY its safety-relevant property: Estimate(a) <= TruePeak(a).

    For a PROFILED task the Track-A p_headroom concurrency cap is REMOVED,
    leaving only three limiters (design v3 sec 7):
      (1) the memory RESERVATION -- reduce_platform_properties decrements
          the ESTIMATE per assigned action; admit only if
          Sum estimates + next.estimate <= worker RAM.
      (2) a profile-independent overcommit CEILING -- a hard per-worker
          in-flight COUNT bound.  MUST BE BUILT; today max_inflight_tasks=0
          => no ceiling (model: Ceil >= |Actions| = "no ceiling").
      (3) a reactive memory-pressure gate -- NAKs NEW admits when free RAM
          < floor, but CANNOT shed already-running actions.

  ------------------------------------------------------------------------
  MODEL (finite; dimensions stated in each .cfg and the sibling .md):

    - location[a] in Workers \cup {QUEUE} -- where each action lives.
      Admission MOVES a from QUEUE onto a worker (mirrors the reservation
      decrement at dispatch).  Actions never complete: the monotone
      admit-to-fixpoint is the adversarial "fullest concurrent set" snapshot
      -- completion only frees RAM and cannot create an OOM the non-
      completing model does not already reach.

    - Two action classes so heterogeneity + the "cold" (unprofiled) task are
      expressible: ColdActions uses a cold-default estimate
      (EstimateCold, possibly << TruePeakCold); the rest are profiled
      (EstimateEach <= TruePeakEach).

    - Admission = ceiling AND reservation AND reactive-gate.  The reservation
      decrements the ESTIMATE (times a safety multiplier MarginMul); the
      worker's TRUE memory is Sum TruePeak of running -- the worst-case
      simultaneous peak the reservation is BLIND to.

    - The reactive gate reads the ESTIMATE ledger's free RAM (design's
      MODEL choice: free_RAM_by_estimate >= Floor) -- so it is blind to
      the under-report and demonstrably CANNOT prevent the OOM.  It also
      never sheds (no action-removal transition exists).

  KNOBS (per-.cfg, Bugged/Fixed mirror of the SchedulerWorkConservation suite):
    MarginMul = 1   -> reserve the raw estimate           (NO margin, unsafe)
    MarginMul = M   -> reserve estimate * M             (safety margin)
    Ceil      >= |Actions| -> "no ceiling" (max_inflight_tasks=0, unsafe)
    Ceil      = C   -> hard count bound; safe iff C*max_true_peak <= RAM
 ***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Workers,        \* set of worker IDs (model values)
    Actions,        \* set of action IDs, all initially queued (model values)
    ColdActions,    \* subset of Actions with NO profile (uses the cold-default estimate)
    RamCap,         \* per-worker RAM capacity
    TruePeakEach,   \* TRUE peak memory of a profiled action (scheduler does NOT know it)
    EstimateEach,   \* injected/measured estimate of a profiled action (<= TruePeakEach)
    TruePeakCold,   \* TRUE peak of a cold (unprofiled) action
    EstimateCold,   \* cold-default estimate injected for an unprofiled action (<= TruePeakCold)
    Ceil,           \* per-worker in-flight COUNT ceiling (>= |Actions| == "no ceiling")
    MarginMul,      \* reservation safety multiplier: Reserved(a) = Estimate(a) * MarginMul
    Floor           \* reactive memory-pressure floor (NAK a new admit when est-free < Floor)

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Actions) >= 1
ASSUME ColdActions \subseteq Actions
ASSUME RamCap \in Nat
ASSUME TruePeakEach \in Nat /\ EstimateEach \in Nat
ASSUME TruePeakCold \in Nat /\ EstimateCold \in Nat
\* The load-bearing fact: the measured/injected estimate is a LOWER bound of true peak.
ASSUME EstimateEach <= TruePeakEach
ASSUME EstimateCold <= TruePeakCold
ASSUME MarginMul \in Nat /\ MarginMul >= 1
ASSUME Ceil \in Nat /\ Ceil >= 1
ASSUME Floor \in Nat

QUEUE == "Queue"                 \* the "not admitted" location sentinel
Locations == Workers \cup {QUEUE}

VARIABLES
    location        \* Actions -> Workers \cup {QUEUE}

vars == <<location>>

TypeOK == location \in [Actions -> Locations]

(***************************************************************************
  Per-action true peak and injected estimate.  Two classes so the cold /
  unprofiled ledger-hole and heterogeneous true peaks are expressible.
 ***************************************************************************)
TruePeak(a) == IF a \in ColdActions THEN TruePeakCold ELSE TruePeakEach
Estimate(a) == IF a \in ColdActions THEN EstimateCold ELSE EstimateEach

\* What the reservation charges: the injected estimate scaled by the margin.
Reserved(a) == Estimate(a) * MarginMul

RunningSet(w) == {a \in Actions : location[a] = w}
RunningCount(w) == Cardinality(RunningSet(w))

RECURSIVE SumReserved(_)
SumReserved(S) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE y \in S : TRUE IN Reserved(x) + SumReserved(S \ {x})

RECURSIVE SumTrue(_)
SumTrue(S) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE y \in S : TRUE IN TruePeak(x) + SumTrue(S \ {x})

\* The reservation ledger the scheduler reads (estimate-based, blind to true).
ReservedUsed(w) == SumReserved(RunningSet(w))
\* The worker's ACTUAL concurrent memory -- the worst-case simultaneous peak.
TrueUsed(w) == SumTrue(RunningSet(w))

(***************************************************************************
  Admission = ceiling AND reservation AND reactive-gate (design v3 sec 7).
 ***************************************************************************)
\* (2) the profile-independent overcommit COUNT ceiling.
CeilingPermits(w) == RunningCount(w) < Ceil

\* (1) the memory reservation: Sum estimates(+margin) + next <= RAM.
ReservationPermits(w, a) == ReservedUsed(w) + Reserved(a) <= RamCap

\* (3) the reactive memory-pressure gate: NAK a NEW admit when the ESTIMATE
\* ledger's free RAM is below Floor.  It reads the estimate (blind to true
\* peak) and never sheds -- so it cannot prevent the under-report OOM.
ReactivePermits(w) == RamCap - ReservedUsed(w) >= Floor

AdmitEnabled(w, a) ==
    /\ location[a] = QUEUE
    /\ CeilingPermits(w)
    /\ ReservationPermits(w, a)
    /\ ReactivePermits(w)

Admit(w, a) ==
    /\ AdmitEnabled(w, a)
    /\ location' = [location EXCEPT ![a] = w]

\* Quiescence: no admit is enabled anywhere (the scheduler has placed
\* everything the three limiters permit).
AdmitQuiescent == \A w \in Workers, a \in Actions : ~AdmitEnabled(w, a)

\* Stutter enabled only at quiescence so TLC does not flag the fixpoint as a
\* deadlock; the invariants are still evaluated there.
Done == AdmitQuiescent /\ UNCHANGED vars

Init == location = [a \in Actions |-> QUEUE]

Next ==
    \/ \E w \in Workers, a \in Actions : Admit(w, a)
    \/ Done

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Workers)

(***************************************************************************
  INVARIANTS
 ***************************************************************************)

(* THE #1 PROPERTY (safety).  No worker's concurrent set of running actions
   has a TRUE peak exceeding its RAM.  Checked at EVERY reachable state --
   an over-commit at any point is a real OOM (the reactive gate cannot shed
   to walk it back). *)
NoOOM == \A w \in Workers : TrueUsed(w) <= RamCap

(* WORK-CONSERVATION cross-check (quiescence-gated -- the tension with NoOOM).
   At the admission fixpoint, no worker has a QUEUED action whose TRUE peak
   would still fit its remaining TRUE RAM.  If one exists, some limiter
   (an over-sized margin reservation, or a count-blind ceiling) refused work
   that genuinely fits -> capacity is stranded (the E-core-spill goal lost).
   NOTE: at the sweet spot Reserved(a) == TruePeak(a) the reservation
   refuses EXACTLY when true would overflow, so this HOLDS alongside NoOOM. *)
TrulyFits(w, a) == TrueUsed(w) + TruePeak(a) <= RamCap
NoIdleWaste ==
    AdmitQuiescent =>
        (\A w \in Workers : \A a \in Actions :
            ~(location[a] = QUEUE /\ TrulyFits(w, a)))

=============================================================================
