----------------------- MODULE SchedulerRebalanceV3PLoad -----------------------
(***************************************************************************
  M1 v2 — p_load refinement of the P-headroom gate. Model-checks the ONE
  new invariant the refinement stands or falls on (I5_Bounded), and re-confirms
  v1's I1-I4 carry over under the WIDENED gate predicate, per
  `.claude/audits/scheduler-m1-v2-pload-refinement-design-2026-07-01.md` §3, §8.

  This EXTENDS the v1 base spec SchedulerRebalance.tla (reserve → execute →
  complete ∥ evict-drain ∥ independent load-sat toggle). v1's 5 invariants
  HOLD and are teeth-proven (SchedulerRebalanceBugged: NoGate / NoA5 /
  Phase2NoLift). v2 changes exactly ONE function:

    ── v1 gate (SchedulerRebalance.tla:161, api_worker_scheduler.rs:550):
         HasPHeadroom(w) == (PCoreCount[w]=0) \/ (Running(w) < PCoreCount[w])
       The gate reads ONLY the FRESH scheduler-maintained in-flight count
       (Running == Cardinality(reserved[w])) — no stale worker-reported load.
       Its doc-comment claims this "closes red-team R3 / invariant I5."

    ── v2 gate (design §2): a THIRD clause is added that consults the STALE
       worker-reported p_load, but only to RELAX v1 BELOW a hard fresh-count
       ceiling — never to admit past it:
         HasPHeadroom_v2(w) ==
              (PCoreCount[w] = 0)                                   \* A5 ungated
           \/ (Running(w) < PCoreCount[w])                          \* v1 fresh signal
           \/ ( PLoad[w] reported idle (< THRESHOLD)                \* NEW override:
                /\ Running(w) < PCoreCount[w] * FACTOR )            \*   bounded by FRESH count

  ------------------------------------------------------------------------
  THE ADVERSARIAL-STALENESS MODEL (the crux — without it the proof is vacuous):

    v1 satisfied I5 trivially: the gate had no stale input. v2 RE-INTRODUCES a
    stale input (worker-reported p_load, keepalive cadence ~2.5s). The design's
    load-bearing claim (§3) is that the `Running < PCoreCount*FACTOR` clause
    BOUNDS the blast radius of an arbitrarily-stale-low p_load:

      Claim (I5_Bounded): regardless of how stale or how low p_load is, the
      override admits a worker to AT MOST PCoreCount*FACTOR in-flight actions.
      Past that, the FRESH dispatch-count (clause 3's own ceiling, updated
      synchronously and never stale) shuts the gate.

    To model this we add PLoad : Workers -> {idle, busy}, DECOUPLED from true
    occupancy Running(w), and an adversarial StalePLoad action that can hold
    PLoad[w] = idle while Running(w) climbs (the keepalive-lag stale-LOW case,
    exactly red-team R3). PLoad is NEVER auto-updated from Running: the only
    thing that changes it is the adversary. So TLC explores the fully-adversarial
    "p_load pinned idle forever while the worker fills up" case — the worst case
    the design must survive.

  THRESHOLD / FACTOR encoding:
    We encode {idle, busy} as {0, 100} and pick THRESHOLD so idle < THRESHOLD
    <= busy. `PLoad[w] reported idle` == (PLoadPct(w) < THRESHOLD). FACTOR = 2.
    The production defaults: p_idle_threshold_pct = 0 (THRESHOLD = 0 => clause 3
    NEVER fires, exact v1 behavior — ThresholdZeroParity), factor = 2.

  ------------------------------------------------------------------------
  CITATIONS (mechanism -> file:line, verified 2026-07-01):
    [gate-v1]  api_worker_scheduler.rs:550 worker_has_p_headroom (v1, replaced)
    [gate-v2]  design §2 :40-45 worker_has_p_headroom_v2 (3 clauses)
    [ceiling]  design §3 :68 `&& running < p_core_count * FACTOR` (the backstop)
    [thresh]   design §4 :96 p_idle_threshold_pct (default 0 => no-op)
    [factor]   design §4 :98 p_headroom_override_factor (default 2)
    [stale]    design §3 :62-66 p_load is worker-reported, keepalive ~2.5s
    [running]  api_worker_scheduler.rs:1873 insert / :1668 remove / :1963 drain
    [a5guard]  api_worker_scheduler.rs:551 `p_core_count == 0 ||`
    [R3/I5]    api_worker_scheduler.rs:539-540 doc-comment "closes R3 / I5"
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,        \* set of worker IDs, e.g. {w1, w2, w3}
    Ops,            \* set of operation IDs, e.g. {o1, o2, o3}
    MaxToggles,     \* bound on adversary p_load flips (LIVENESS only). Unbounded
                    \* stale-flipping can starve progress by a fairness artifact
                    \* (never firing a dispatch while flipping); SAFETY cfgs set
                    \* it large (adversary ~unbounded), LIVENESS cfgs small.
    PCoreCount,     \* [Workers -> Nat] per-worker p_core_count (via <- override).
    Capacity,       \* hard reservation-slot cap per worker. For the ceiling to be
                    \* observable we need Capacity > PCoreCount*FACTOR on SOME
                    \* worker, so the FRESH-COUNT CEILING (not the slot cap) is the
                    \* binding limit — that is the whole thing under test. If the
                    \* slot cap bound Running first, the teeth could not bite.
    THRESHOLD,      \* p_idle_threshold_pct: PLoad reported "idle" iff PLoadPct < THRESHOLD.
                    \* Prod default 0 (=> clause 3 never fires => exact v1).
    FACTOR,         \* p_headroom_override_factor: override ceiling = PCoreCount*FACTOR.
    AdmitAllThreshold, \* TEETH (BuggedThresholdAlways): TRUE => "idle" predicate
                    \* is TRUE for ALL workers regardless of PLoad (threshold admits
                    \* everything) => clause 3 gated only by its OWN ceiling; with
                    \* the ceiling ALSO dropped this is the pure-OR bug.
    DropCeiling,    \* TEETH (BuggedNoCeiling): TRUE => clause 3 drops the
                    \* `Running < PCoreCount*FACTOR` conjunct => pure `OR p_idle`
                    \* => unbounded Running under stale-idle. MUST violate I5_Bounded.
    AllowEvict      \* TRUE => environment may evict a reserved worker (drain-requeue).

(* Per-cfg p_core_count assignments (TLC cfg cannot parse function literals;
   each cfg overrides PCoreCount via `CONSTANT PCoreCount <- <name>`). *)
PCoreCount_Het    == [w \in Workers |->            \* {w1:1, w2:2, w3:0}
                        IF w = "w1" THEN 1
                        ELSE IF w = "w2" THEN 2
                        ELSE IF w = "w3" THEN 0
                        ELSE 1]
PCoreCount_All1   == [w \in Workers |-> 1]
PCoreCount_All2   == [w \in Workers |-> 2]

(* PLoad lattice encoded as percentages so `idle < THRESHOLD <= busy`. *)
PIdlePct == 0
PBusyPct == 100
PLoadPct(pl) == IF pl = "idle" THEN PIdlePct ELSE PBusyPct

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME PCoreCount \in [Workers -> 0..3]
ASSUME Capacity \in 1..6
ASSUME THRESHOLD \in 0..101
ASSUME FACTOR \in 1..3
ASSUME AdmitAllThreshold \in BOOLEAN
ASSUME DropCeiling \in BOOLEAN
ASSUME AllowEvict \in BOOLEAN
ASSUME MaxToggles \in Nat

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}
PLoadVals == {"idle", "busy"}

VARIABLES
    opStage,        \* Ops -> Stages
    opWorker,       \* Ops -> WorkerOrNone : action-DB assigned worker_id
    reserved,       \* Workers -> SUBSET Ops : running_action_infos (FRESH dispatch-count source)
    workerUp,       \* Workers -> BOOLEAN : worker present in the pool
    pLoad,          \* Workers -> PLoadVals : STALE worker-reported P-core load,
                    \* DECOUPLED from Running(w). The ONLY writer is StalePLoad
                    \* (adversary) — never auto-synced from occupancy. This is the
                    \* keepalive-lag stale signal the whole I5_Bounded claim guards.
    lastDispatchTier,\* Ops -> {"none","cache","fallback"} : how CURRENT dispatch routed
    togglesLeft      \* remaining adversary p_load flips (liveness bound only)

vars == << opStage, opWorker, reserved, workerUp, pLoad, lastDispatchTier, togglesLeft >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ pLoad \in [Workers -> PLoadVals]
    /\ lastDispatchTier \in [Ops -> {"none", "cache", "fallback"}]
    /\ togglesLeft \in 0..MaxToggles

(***************************************************************************
  THE v2 GATE — the 3-clause predicate under test.
 ***************************************************************************)

(* FRESH in-flight dispatch count = size of the reservation set (never stale). *)
Running(w) == Cardinality(reserved[w])

(* Clause 3 idle-predicate: the worker's STALE reported p_load says P is idle.
   AdmitAllThreshold TEETH forces this TRUE for every worker (threshold=infinity,
   admits everything) so clause 3 is gated ONLY by its ceiling. *)
PReportedIdle(w) == AdmitAllThreshold \/ (PLoadPct(pLoad[w]) < THRESHOLD)

(* Clause 3 ceiling: FRESH count below PCoreCount*FACTOR. DropCeiling TEETH
   removes it => pure `OR PReportedIdle` => unbounded Running under stale-idle. *)
Clause3Ceiling(w) == DropCeiling \/ (Running(w) < PCoreCount[w] * FACTOR)

(* HasPHeadroom v2 — three clauses in precedence order (design §2):
     1. A5   : p_core_count == 0 (ungated, byte-identical to v1)
     2. v1   : fresh dispatch-count below p_core_count
     3. NEW  : p_load reported idle AND fresh count below p_core_count*FACTOR *)
HasPHeadroom(w) ==
    \/ PCoreCount[w] = 0
    \/ Running(w) < PCoreCount[w]
    \/ (PReportedIdle(w) /\ Clause3Ceiling(w))

(* A worker can physically accept a new reservation (up + below hard cap). *)
HasFreeSlot(w) == workerUp[w] /\ Running(w) < Capacity

BaseViable(w) == HasFreeSlot(w)

(* Phase-aware M1 cache-tier viability (v1 structure, unchanged — inherits the
   WIDENED HasPHeadroom). Phase 1: a viable worker WITHOUT P-headroom is
   excluded. Phase 2 (no viable P-headroom worker): exclusion lifts. *)
SomeViableHasPHeadroom == \E w \in Workers : BaseViable(w) /\ HasPHeadroom(w)
CacheTierViable(w) ==
    /\ BaseViable(w)
    /\ (SomeViableHasPHeadroom => HasPHeadroom(w))

CacheDispatchable(w) == CacheTierViable(w)

(* Fallback (LRU/MRU backstop) — P-headroom-UNGATED, any BaseViable worker;
   fires only when the cache tiers all decline. The Phase-2 no-wedge path. *)
FallbackDispatchable(w) == BaseViable(w)

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ pLoad = [w \in Workers |-> "idle"]   \* worst case: reported idle from t=0
    /\ lastDispatchTier = [o \in Ops |-> "none"]
    /\ togglesLeft = MaxToggles

(***************************************************************************
  CacheReserve(o, w): the cache-affinity tiers select w. Requires the
  M1-gated (WIDENED) viability. Folds reserve->execute atomically (the base
  SchedulerMatch already proved the reserve/assign/retry interleaving safety).
  NOTE: pLoad is UNCHANGED here — dispatching does NOT refresh the stale
  worker-reported load. Only the adversary StalePLoad moves it. This is what
  lets Running climb via clause 3 while pLoad stays "idle".
 ***************************************************************************)
CacheReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ CacheDispatchable(w)
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "cache"]
    /\ UNCHANGED << workerUp, pLoad, togglesLeft >>

(***************************************************************************
  FallbackReserve(o, w): the LRU/MRU backstop. Fires only when the cache
  tiers all declined. P-headroom-UNGATED (any BaseViable worker).
 ***************************************************************************)
FallbackReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ FallbackDispatchable(w)
    /\ ~(\E ww \in Workers : CacheDispatchable(ww))
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "fallback"]
    /\ UNCHANGED << workerUp, pLoad, togglesLeft >>

(***************************************************************************
  Complete(o): worker finishes; slot freed. FRESH count drops -> P-headroom
  recomputes fresh (I5). pLoad UNCHANGED (stale signal does not track this).
 ***************************************************************************)
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
    /\ UNCHANGED << workerUp, pLoad, togglesLeft >>

(***************************************************************************
  Evict(w): worker disconnects; drain requeues its ops (base compensator).
  Frees slots (FRESH count drops -> P-headroom recomputes, I5).
 ***************************************************************************)
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
    /\ UNCHANGED << pLoad, togglesLeft >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, pLoad, lastDispatchTier, togglesLeft >>

(***************************************************************************
  StalePLoad(w, pl): THE ADVERSARY. Sets a worker's STALE reported p_load to
  ANY value, DECOUPLED from Running(w). Because pLoad is initialized "idle"
  and every dispatch leaves it UNCHANGED, the adversary can simply NEVER flip
  it back to "busy" — modeling a keepalive that lags forever / reports stale-low
  while the worker's P cores actually filled with a CPU-bound burst. This is
  exactly the red-team R3 / I5 failure mode. Bounded by togglesLeft only so
  liveness stays checkable; even with ZERO flips the initial all-"idle" state
  already exercises the worst case (stale-low from t=0).
 ***************************************************************************)
StalePLoad(w, pl) ==
    /\ togglesLeft > 0
    /\ pl \in PLoadVals
    /\ pl # pLoad[w]
    /\ togglesLeft' = togglesLeft - 1
    /\ pLoad' = [pLoad EXCEPT ![w] = pl]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, lastDispatchTier >>

AllCompleted == \A o \in Ops : opStage[o] = "Completed"
Done == AllCompleted /\ UNCHANGED vars

Next ==
    \/ \E o \in Ops, w \in Workers : CacheReserve(o, w)
    \/ \E o \in Ops, w \in Workers : FallbackReserve(o, w)
    \/ \E o \in Ops : Complete(o)
    \/ \E w \in Workers : Evict(w)
    \/ \E w \in Workers : Reconnect(w)
    \/ \E w \in Workers, pl \in PLoadVals : StalePLoad(w, pl)
    \/ Done

(***************************************************************************
  Fairness. Dispatch actions weakly fair (a Queued op with an available worker
  cannot be starved). Complete + Reconnect weakly fair (slots free, workers
  return). StalePLoad NOT fair (adversary). Evict NOT fair (adversary).
 ***************************************************************************)
Fairness ==
    /\ \A o \in Ops, w \in Workers : WF_vars(CacheReserve(o, w))
    /\ \A o \in Ops, w \in Workers : WF_vars(FallbackReserve(o, w))
    /\ \A o \in Ops : WF_vars(Complete(o))
    /\ \A w \in Workers : WF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  I5_Bounded — THE NEW HEADLINE INVARIANT (design §3, §8).

  Regardless of how stale or how low the worker-reported p_load is, the FRESH
  dispatch-count ceiling bounds per-worker in-flight concentration to at most
  PCoreCount*FACTOR. This is the claim that the `&& Running < PCoreCount*FACTOR`
  clause makes freshness (I5) provable EVEN with a stale p_load input.

  For a p_count==0 (A5-ungated) worker there is NO fresh-count ceiling by design
  (it is legacy/Linux, degrades to slot-cap behavior), so the bound for those is
  Capacity, not PCoreCount*FACTOR (which is 0). We state the invariant only for
  GATED workers (PCoreCount[w] > 0) — the ones the override actually governs.
  A5 workers are covered by CapacityRespected instead.
 ***************************************************************************)
I5_Bounded ==
    \A w \in Workers :
        (workerUp[w] /\ PCoreCount[w] > 0) => Running(w) <= PCoreCount[w] * FACTOR

(***************************************************************************
  I5_BoundedOverride — THE ACTION-TIME form of the ceiling claim, and the one
  that is actually PROVABLE / meaningful for the p_load override.

  MODELLING NOTE (invariant-prover, load-bearing): the pure STATE form
  I5_Bounded above is FALSIFIED even by v1 (THRESHOLD=0, override OFF) — NOT by
  the p_load override, but by the PRE-EXISTING Phase-2 no-wedge lift: when every
  OTHER viable worker is down/saturated, `SomeViableHasPHeadroom` is FALSE, the
  Phase-1 exclusion LIFTS, and the cache tier (correctly, to avoid a wedge — I2
  dominates) admits the last worker past ANY P-headroom-based ceiling. So
  `Running <= PCoreCount*FACTOR` as an unconditional per-worker STATE bound was
  never a v1 property and the override does not break it further.

  The design's real, provable claim is about the OVERRIDE ADMISSION DECISION:
  clause 3 (the stale-p_load override) never ADMITS a worker that already holds
  >= PCoreCount*FACTOR in-flight actions. Equivalently: whenever the gate is
  actually GOVERNING the decision (Phase 1: some viable worker HAS P-headroom, so
  the Phase-1 exclusion is in force, so a gated worker is admitted to the cache
  tier ONLY because HasPHeadroom(w) is TRUE), the admitted worker was below the
  ceiling at decision time. This isolates the override's contribution from the
  Phase-2 lift, and is exactly the §3 statement "the OVERRIDE can admit a worker
  to at most PCoreCount*FACTOR."

  Action-time: on every CACHE dispatch to w that happens IN PHASE 1
  (SomeViableHasPHeadroom holds in the pre-state, so the admit was gate-driven,
  not lift-driven), if w is a gated worker (PCoreCount>0) then its FRESH count
  was strictly below PCoreCount*FACTOR at decision time. Because CacheReserve's
  guard entails HasPHeadroom(w), and HasPHeadroom(w) for a gated worker at
  Running >= PCoreCount*FACTOR is FALSE (clause 2 needs Running<PCoreCount,
  clause 3 needs Running<PCoreCount*FACTOR), this holds by construction — and
  BuggedNoCeiling / BuggedThresholdAlways break it (teeth).
 ***************************************************************************)
I5_OverrideAdmitLegal(w) ==
    (SomeViableHasPHeadroom /\ PCoreCount[w] > 0)
        => Running(w) < PCoreCount[w] * FACTOR
I5_BoundedOverrideStep ==
    \A o \in Ops, w \in Workers :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ lastDispatchTier'[o] = "cache"
          /\ opWorker'[o] = w )
        => I5_OverrideAdmitLegal(w)
I5_BoundedOverride == [][I5_BoundedOverrideStep]_vars

(* ── I1 (P-headroom-first) — ACTION property, carried from v1 under the
   WIDENED predicate. Whenever a CACHE dispatch happens, the chosen worker was
   Phase-1-legal at DECISION time (pre-state): if some viable worker had
   P-headroom, so did the chosen one. Checked as [][I1CacheStep]_vars. The
   "P-headroom" set is now the widened v2 set — that is the point: the property
   must still hold with the enlarged extension. *)
CacheDispatchPhase1Legal(w) == SomeViableHasPHeadroom => HasPHeadroom(w)
I1CacheStep ==
    \A o \in Ops, w \in Workers :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ lastDispatchTier'[o] = "cache"
          /\ opWorker'[o] = w )
        => CacheDispatchPhase1Legal(w)
I1_PHeadroomFirst == [][I1CacheStep]_vars

(* ── I3 (locality bounded) — same cache-tier action guarantee as I1. *)
I3_LocalityBounded == [][I1CacheStep]_vars

(* ── I4 (heterogeneity-safe) — p_count==0 workers always ungated (A5, clause 1
   byte-identical to v1). The widened clauses 2/3 never REMOVE headroom, so A5
   is untouched. *)
I4_HeterogeneitySafe ==
    \A w \in Workers : (PCoreCount[w] = 0) => HasPHeadroom(w)

(* ── I5 (freshness, base form carried from v1) — the dispatch-count equals the
   live reservation set; no Completed/Queued op occupies a live worker's slot
   (which would inflate Running past the true in-flight set). The v2 override
   reads pLoad but the CEILING still reads the fresh reserved set. *)
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
    /\ I5_Bounded
    /\ I4_HeterogeneitySafe
    /\ I5_Freshness
    /\ CapacityRespected
    /\ EvictedEmpty

------------------------------------------------------------------------------
(***************************************************************************
  I2 (no-wedge / progress) — carried from v1. v2 is strictly MORE admissive
  (an OR clause), and v1 is proven no-wedge, so admitting more cannot wedge.
  Re-checked structurally (NoWedge) + temporally (EventuallyDispatched) under
  the widened predicate + the stale-p_load adversary.
 ***************************************************************************)
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

(***************************************************************************
  ThresholdZeroParity — the default-no-op proof (design §4). With THRESHOLD=0
  (production default), PReportedIdle(w) == (PLoadPct < 0) == FALSE for every
  worker (both idle=0 and busy=100 are >= 0), so clause 3 NEVER fires and the
  v2 gate collapses to the v1 gate EXACTLY. We assert HasPHeadroom equals the
  v1 formula in every reachable state — stuttering-equivalent gate behavior.
  (Only meaningful when the cfg sets THRESHOLD=0, AdmitAllThreshold=FALSE,
  DropCeiling=FALSE.)
 ***************************************************************************)
HasPHeadroom_v1(w) == (PCoreCount[w] = 0) \/ (Running(w) < PCoreCount[w])
ThresholdZeroParity == \A w \in Workers : HasPHeadroom(w) = HasPHeadroom_v1(w)

=============================================================================
