--------------------------- MODULE SchedulerWorkConservation ---------------------------
(***************************************************************************
  WORK-CONSERVATION of the NativeLink dispatch scheduler.

  THE #1 PROPERTY (user's words, verbatim):
    "No work should be queued while processors of any kind are idle
     (assuming that sufficient other resources exist on the worker:
      ram, network, etc.)."

  Formalized here as a SAFETY invariant, `WorkConserving`, gated on
  dispatch-quiescence (see the "QUIESCENCE" note below — it is the
  load-bearing modeling decision).

  ------------------------------------------------------------------------
  GROUND TRUTH (verified against current code this session):

    Real dispatch gate  nativelink-scheduler/src/api_worker_scheduler.rs:1038
      fn worker_has_p_headroom(w, idle_threshold_pct, override_factor) -> bool {
          let running = w.running_action_infos.len();
          w.p_core_count == 0
              || running < w.p_core_count
              || (w.p_core_load_pct < idle_threshold_pct
                  && running < w.p_core_count * override_factor)
      }
      Called at the dispatch reservation site (api_worker_scheduler.rs:2239)
      and is the SOLE per-worker concurrency limiter on top of resource fit.

    PROD CONFIG (verified):
      p_idle_threshold_pct       = 0   (schedulers.rs:447)
      p_headroom_override_factor = 2   (default_p_headroom_override_factor)
      gate enabled.
    Threshold 0 makes the override clause `p_load < 0` NEVER true, so the
    gate collapses to exactly `running < p_core_count`.  That is `GateMode
    = "PCoreOnly"` below.

    Worker hardware (verified: worker.rs p_core_count:210 / e_core_count:219):
      each M4 worker: p_core_count = 4, e_core_count = 6  (10 processors).
    `e_core_count` feeds only the cache-vs-load blend score — it is NEVER
    used as dispatch capacity.  So the prod gate caps a worker at 4 running
    actions even though 10 processors exist.

    EMPIRICALLY CONFIRMED this session: macOS spills oversubscribed
    USER_INITIATED-QoS work onto E-cores, so a worker usefully runs up to
    p_core_count + e_core_count concurrent actions on distinct processors.

    LIVE COUNTEREXAMPLE the model must reproduce: queue ~100 deep, every
    worker pinned at running = 4 (= p_core_count), P-cores 98%, all 6
    E-cores idle, RAM/network NOT the bottleneck.  6 idle processors per
    worker with fitting work queued = a work-conservation violation.

  ------------------------------------------------------------------------
  QUIESCENCE (why the invariant is gated):

    "No queued-fitting-work while a processor is idle" is FALSE as a raw
    per-state invariant in ANY dispatch system: between a submit and its
    dispatch there is a transient state with queued work and an idle
    processor — the scheduler is merely about-to-dispatch.  That transient
    is not a violation.

    The user's own equivalent form pins it down: "no reachable QUIESCENT
    state has a queued-fitting action AND a free processor."  So we assert
    WorkConserving only when NO further Dispatch is enabled (the dispatch
    fixpoint).  Under the bugged PCoreOnly gate the fixpoint is exactly the
    live counterexample: every worker at running = p_core_count, the gate
    refuses all further dispatch (`~ENABLED Dispatch`), yet fitting work is
    queued and E-cores sit idle.  Under the TotalCore gate the only
    fixpoints are "queue drained" or "no queued action fits any idle-
    processor worker's RAM" — both work-conserving.

  ------------------------------------------------------------------------
  MODEL DIMENSIONS / CHOICES (do not overstate — see the sibling .md):
    - Two gate modes selected by CONSTANT GateMode:
        "PCoreOnly"  = current prod gate (threshold-0 collapse). EXPECT VIOLATION.
        "TotalCore"  = candidate fix `running < p_core_count + e_core_count`
                       (resource-fit still required).             EXPECT HOLDS.
    - RAM is the single modeled "other resource"; network omitted in v1
      (RAM alone exercises the conditional guard).  Uniform per-action RAM
      demand `ActionRamEach` and per-worker capacity `RamCap` — uniform RAM
      is sufficient to demonstrate the guard; heterogeneous RAM is a trivial
      generalization (swap ActionRam(a) for a per-action function).
    - Actions never complete: the monotone dispatch-to-fixpoint is the
      adversarial (fullest-queue) model; completion only frees capacity and
      cannot create a violation the non-completing model does not already
      reach.  Dispatch latency, locality preference, and E-cores being
      slower than P-cores are NOT modeled (see caveats in the .md).
 ***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Workers,        \* set of worker IDs (model values)
    Actions,        \* set of action IDs, all initially queued (model values)
    PCoreCount,     \* p_core_count per worker (homogeneous fleet)
    ECoreCount,     \* e_core_count per worker (homogeneous fleet)
    RamCap,         \* per-worker RAM capacity
    ActionRamEach,  \* uniform RAM demand of each action
    GateMode        \* "PCoreOnly" (bug) or "TotalCore" (fix)

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Actions) >= 1
ASSUME PCoreCount \in Nat /\ PCoreCount >= 1
ASSUME ECoreCount \in Nat
ASSUME RamCap \in Nat
ASSUME ActionRamEach \in Nat /\ ActionRamEach >= 1
ASSUME GateMode \in {"PCoreOnly", "TotalCore"}

QUEUE == "Queue"                 \* the "not dispatched" location sentinel
Locations == Workers \cup {QUEUE}

VARIABLES
    location        \* action -> Workers \cup {QUEUE}: where each action lives

vars == <<location>>

TypeOK == location \in [Actions -> Locations]

(***************************************************************************
  Derived quantities.
 ***************************************************************************)
TotalCores == PCoreCount + ECoreCount

RunningSet(w) == {a \in Actions : location[a] = w}
RunningCount(w) == Cardinality(RunningSet(w))

\* Per-action RAM demand.  Uniform in v1; keep the (a) parameter so a future
\* heterogeneous variant only swaps this body for a per-action function.
ActionRam(a) == ActionRamEach
UsedRam(w) == RunningCount(w) * ActionRamEach

\* "sufficient other resources exist" — the action fits in remaining RAM.
Fits(w, a) == UsedRam(w) + ActionRam(a) <= RamCap

(***************************************************************************
  The dispatch gate.  Resource fit (Fits) is required by BOTH gates — it
  models the platform-properties / resource reservation that dispatch
  always respects.  worker_has_p_headroom is the ADDITIONAL concurrency
  restriction layered on top:

    PCoreOnly : running < p_core_count            (prod, threshold-0 collapse)
    TotalCore : running < p_core_count + e_core_count
 ***************************************************************************)
GatePermits(w) ==
    CASE GateMode = "PCoreOnly" -> RunningCount(w) < PCoreCount
      [] GateMode = "TotalCore" -> RunningCount(w) < TotalCores

DispatchEnabled(w, a) ==
    /\ location[a] = QUEUE
    /\ GatePermits(w)
    /\ Fits(w, a)

Dispatch(w, a) ==
    /\ DispatchEnabled(w, a)
    /\ location' = [location EXCEPT ![a] = w]

(***************************************************************************
  Quiescence: no Dispatch action is enabled anywhere (the scheduler has
  finished placing everything it is permitted + able to place).
 ***************************************************************************)
QuiescentDispatch == \A w \in Workers, a \in Actions : ~DispatchEnabled(w, a)

\* Done: a stutter enabled only at quiescence so TLC does not flag the
\* fixpoint as a deadlock; the safety invariants are still evaluated there.
Done == QuiescentDispatch /\ UNCHANGED vars

Init == location = [a \in Actions |-> QUEUE]

Next ==
    \/ \E w \in Workers, a \in Actions : Dispatch(w, a)
    \/ Done

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Workers) \cup Permutations(Actions)

(***************************************************************************
  INVARIANTS
 ***************************************************************************)

\* A processor of SOME kind (P or E) is free on worker w.
IdleProcessor(w) == RunningCount(w) < TotalCores

\* Some queued action fits worker w's remaining RAM.
QueuedFits(w) == \E a \in Actions : location[a] = QUEUE /\ Fits(w, a)

(* THE #1 PROPERTY (safety, quiescence-gated).

   At the dispatch fixpoint, no worker may simultaneously (a) have a queued
   action that fits its remaining RAM and (b) have an idle processor of any
   kind.  Equivalently: if fitting work is queued for w, every processor on
   w must be busy.  When not quiescent the implication is vacuous (transient
   about-to-dispatch states are allowed). *)
WorkConserving ==
    QuiescentDispatch =>
        (\A w \in Workers : QueuedFits(w) => ~IdleProcessor(w))

(* Resource-safety invariants the fix must NOT break — the fix must achieve
   conservation WITHOUT oversubscribing cores or overflowing RAM. *)
NoProcessorOversubscription == \A w \in Workers : RunningCount(w) <= TotalCores
NoRamOverflow == \A w \in Workers : UsedRam(w) <= RamCap

=============================================================================
