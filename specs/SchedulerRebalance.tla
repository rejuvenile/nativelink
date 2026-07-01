------------------------- MODULE SchedulerRebalance -------------------------
(***************************************************************************
  M1 rebalance gate: a dispatch-count P-headroom overflow gate layered onto
  the SchedulerMatch selection model. Model-checks the 5 M1 invariants I1-I5
  from `.claude/audits/scheduler-pcore-first-rebalance-design-v2-2026-06-30.md`
  §4.

  This EXTENDS the base SchedulerMatch state machine (reserve → assign →
  execute → complete, ∥ evict-drain) with the two-phase fleet-fill gate. The
  base spec's 6 invariants already HOLD (teeth-proven via
  SchedulerMatchBuggedNoDrainRequeue); this spec adds ONLY the selection-tier
  gate and its 5 invariants, keeping the same conventions.

  ------------------------------------------------------------------------
  THE MECHANISM MODELED (each cited to current code):

    has_p_headroom(w)  [design §3, api_worker_scheduler.rs:61 pseudo]:
        running_action_infos.len() < p_core_count
      where running_action_infos.len() is the scheduler-maintained in-flight
      count == Cardinality(reserved[w]) in this model
      [insert :1873, remove :1668, drain :1963 — all under the write lock].
      A5 GUARD (3 reviewers): a worker with p_core_count == 0 is ALWAYS
      has_p_headroom (degrade to current behavior, never frozen out).

    M1 — add has_p_headroom to `worker_is_viable` [:1302], GATED on "a viable
      P-headroom worker exists":
        Phase 1 (some viable worker has P-headroom): a viable worker WITHOUT
          P-headroom is EXCLUDED from the cache-affinity tiers (Tier 1 :1439,
          Tier 1.5 :1489, Tier 2 :1612 all call worker_is_viable).
        Phase 2 (NO viable worker has P-headroom): the exclusion LIFTS; the
          cache tiers admit all viable workers again AND the LRU/MRU backstop
          [inner_find_worker_for_action :1146-1256, which uses `worker_matches`
          NOT `worker_is_viable` — structurally ungated by P-headroom] runs.

    A4 — the SECOND, INDEPENDENT predicate `saturation_fall_through`
      [:1405] = (viable_count > 0 && all_viable_saturated), where
      all_viable_saturated is LOAD-based (cap_score.is_saturated() ==
      weighted_free <= EPSILON [:416]). This is orthogonal to the M1
      dispatch-count gate: a worker at exactly p_count actions but lightly
      loaded is P-gated (M1 excludes it) yet NOT load-saturated (the
      fall-through does not fire for it). Modeled as an independent per-worker
      boolean `loadSaturated` the environment toggles.

  THE SELECTION CASCADE (abstracted):
    - CacheDispatch(o, w): the cache-affinity tiers pick w. Requires
      worker_is_viable(w) INCLUDING the M1 P-headroom gate (Phase-1
      exclusion). A cache holder that is P-gated is skipped; the action
      overflows to a P-headroom peer (or, if none is a holder, to the
      fallback). This is the tier that I1/I3 constrain.
    - FallbackDispatch(o, w): the LRU/MRU backstop. Runs when the cache tiers
      decline (no viable cache-tier pick). NOT gated by P-headroom (matches
      `worker_matches`), so it dispatches to any live matching worker with a
      free slot. This is the Phase-2 no-wedge backstop (I2).

  The cache-vs-fallback CHOICE is abstracted: rather than model directory
  digests, we let the environment nondeterministically offer a cache-tier
  pick to ANY viable P-headroom worker (Phase 1) or ANY viable worker
  (Phase 2). Whenever no cache-tier worker is offered/viable, the fallback
  fires. This over-approximates the real selection (real code picks ONE cache
  winner) but is SOUND for the invariants: I1 says cache tiers never dispatch
  to a P-gated worker while a P-headroom worker exists — we check that on
  EVERY cache dispatch; I2 says SOME dispatch always happens — we check the
  disjunction of both paths is enabled whenever a slot exists.

  ------------------------------------------------------------------------
  CITATIONS (mechanism → file:line, verified 2026-07-01):
    [viable]       api_worker_scheduler.rs:1302 worker_is_viable (M1 insertion)
    [phcount]      :1364,:1374,:4988 p_core_count per-worker (set_core_counts)
    [running]      :1873 insert / :1668 remove / :1963 drain — in-flight count
    [tiers]        :1439,:1489,:1612 each tier calls worker_is_viable
    [fallback]     :1146-1256 inner_find_worker_for_action (worker_matches,
                   NOT worker_is_viable — ungated by P-headroom)
    [satpred]      :1405 saturation_fall_through (load-based, INDEPENDENT)
    [issat]        :416 is_saturated = weighted_free <= SATURATION_EPSILON
    [a5guard]      design §10 A5: p_core_count == 0 => always has_p_headroom
    [evict-drain]  :1963 running_action_infos.drain() (base compensator)
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,        \* set of worker IDs, e.g. {w1, w2, w3}
    Ops,            \* set of operation IDs, e.g. {o1, o2}
    MaxToggles,     \* bound on adversary load-sat toggles (for LIVENESS only).
                    \* An UNBOUNDED toggle adversary can starve a persistently-
                    \* dispatchable op by flipping the enabled disjunct
                    \* (cache vs fallback) forever without ever firing either —
                    \* a fairness-granularity artifact, NOT a design wedge (the
                    \* NoWedge SAFETY lemma proves a dispatch is ALWAYS enabled).
                    \* Bounding toggles lets WF on the dispatch actions force
                    \* progress, so EventuallyDispatched becomes checkable. The
                    \* SAFETY cfgs set MaxToggles large (adversary ~unbounded);
                    \* the LIVENESS cfgs set it small (a settling fleet).
    PCoreCount,     \* [Workers -> Nat] : each worker's OWN p_core_count
                    \* (heterogeneous; include a 0 to exercise the A5 guard).
                    \* Supplied via a CONSTANT ... <- <Def> override per cfg
                    \* (TLC cfg cannot parse `:>`/`@@` function literals), where
                    \* <Def> is one of the PCoreCount_* operators below.
    Capacity,       \* hard reservation-slot cap per worker (>= max PCoreCount
                    \* so the P-gate, not the slot cap, is the binding limit
                    \* in Phase 1; models "E cores still free above p_count")
    AllowEvict,     \* TRUE => environment may evict a reserved worker
    AllowLoadSat    \* TRUE => environment may toggle load-saturation
                    \* INDEPENDENTLY of dispatch-count (the A4 second predicate)

(* Per-cfg p_core_count assignments. TLC's cfg parser cannot express a
   function literal, so each cfg overrides the PCoreCount constant with one of
   these operators via `CONSTANT PCoreCount <- <name>`. The worker model
   values are strings, so we key off equality to the literal id. Every op
   defaults unmatched workers to 1 so a partial map never leaves a hole. *)
PCoreCount_Het    == [w \in Workers |->            \* {w1:1, w2:2, w3:0}
                        IF w = "w1" THEN 1
                        ELSE IF w = "w2" THEN 2
                        ELSE IF w = "w3" THEN 0
                        ELSE 1]
PCoreCount_All1   == [w \in Workers |-> 1]         \* every worker p_count 1
PCoreCount_All2   == [w \in Workers |-> 2]         \* every worker p_count 2

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME PCoreCount \in [Workers -> 0..3]
ASSUME Capacity \in 1..4
ASSUME AllowEvict \in BOOLEAN
ASSUME AllowLoadSat \in BOOLEAN
ASSUME MaxToggles \in Nat

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}

VARIABLES
    opStage,        \* Ops -> Stages
    opWorker,       \* Ops -> WorkerOrNone : action-DB assigned worker_id
    reserved,       \* Workers -> SUBSET Ops : running_action_infos (dispatch-count source)
    workerUp,       \* Workers -> BOOLEAN : worker present in the pool
    loadSaturated,  \* Workers -> BOOLEAN : the SECOND predicate (A4); load-based,
                    \* toggled INDEPENDENTLY of dispatch-count
    lastDispatchTier,\* Ops -> {"none","cache","fallback"} : how op's CURRENT
                     \* dispatch was routed (for the I1 witness; "none" = not dispatched)
    togglesLeft      \* remaining adversary load-sat toggles (liveness bound only)

vars == << opStage, opWorker, reserved, workerUp, loadSaturated, lastDispatchTier, togglesLeft >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ loadSaturated \in [Workers -> BOOLEAN]
    /\ lastDispatchTier \in [Ops -> {"none", "cache", "fallback"}]
    /\ togglesLeft \in 0..MaxToggles

(***************************************************************************
  THE GATE, exactly as designed.
 ***************************************************************************)

(* In-flight dispatch count = size of the reservation set (fresh, I5). *)
Running(w) == Cardinality(reserved[w])

(* A5 guard: p_core_count == 0 => ALWAYS has P-headroom (ungated). *)
HasPHeadroom(w) == (PCoreCount[w] = 0) \/ (Running(w) < PCoreCount[w])

(* A worker can physically accept a new reservation (up + below hard cap). *)
HasFreeSlot(w) == workerUp[w] /\ Running(w) < Capacity

(* Load-saturation (the A4 SECOND predicate). A never-toggled worker is not
   load-saturated. Modeled as an independent boolean, so a worker can be:
     - P-gated but NOT load-saturated (at p_count, lightly loaded)
     - load-saturated but WITH P-headroom (heavy long action on one P core)
   INDEPENDENCE is the whole point of A4. *)
LoadSaturated(w) == loadSaturated[w]

(* worker_is_viable base check (abstracted): up + free slot. In prod this is
   the platform/quarantine/pressure gate; here every worker matches the single
   platform class, so viability reduces to "up and has a physical free slot".
   The M1 gate and the load predicate layer on top of this. *)
BaseViable(w) == HasFreeSlot(w)

(* Does ANY viable worker have P-headroom right now? Design M2: the
   any_viable_has_p_headroom pre-scan folded into the all_viable_saturated
   loop [:1392-1402]. Must reference VIABLE workers only (code-reviewer C1). *)
SomeViableHasPHeadroom == \E w \in Workers : BaseViable(w) /\ HasPHeadroom(w)

(* M1 worker_is_viable INCLUDING the P-headroom gate, phase-aware:
     Phase 1 (SomeViableHasPHeadroom): a viable worker WITHOUT P-headroom is
       excluded from the cache tiers.
     Phase 2 (no viable P-headroom worker): exclusion lifts. *)
CacheTierViable(w) ==
    /\ BaseViable(w)
    /\ (SomeViableHasPHeadroom => HasPHeadroom(w))

(* saturation_fall_through [:1405]: EVERY viable worker is LOAD-saturated
   (and there is >= 1 viable worker). When true, the cache tiers all decline
   and selection drops to the fallback. INDEPENDENT of the M1 P-gate. *)
ViableCount == Cardinality({w \in Workers : BaseViable(w)})
AllViableLoadSaturated == \A w \in Workers : BaseViable(w) => LoadSaturated(w)
SaturationFallThrough == ViableCount > 0 /\ AllViableLoadSaturated

(* A cache-tier dispatch to w is possible iff w passes the M1-gated viability
   AND the load-fall-through has not forced the tiers to decline. This is the
   JOINT of the two predicates (A4). *)
CacheDispatchable(w) ==
    /\ CacheTierViable(w)
    /\ ~SaturationFallThrough

(* The fallback (LRU/MRU backstop) can dispatch to any BaseViable worker; it
   is NOT gated by P-headroom [inner_find_worker_for_action uses
   worker_matches]. It is the path taken when the cache tiers decline. *)
FallbackDispatchable(w) == BaseViable(w)

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ loadSaturated = [w \in Workers |-> FALSE]
    /\ lastDispatchTier = [o \in Ops |-> "none"]
    /\ togglesLeft = MaxToggles

(***************************************************************************
  CacheReserve(o, w): the cache-affinity tiers select w for op o. This is the
  path I1/I3 constrain. Requires the M1-gated viability AND not-fall-through.

  We fold the base reserve→assign→execute chain into ONE atomic dispatch step
  here: the base SchedulerMatch already exhaustively proved the reserve/assign/
  retry/evict interleaving safety; this spec is about the SELECTION GATE, so
  we abstract the commit as atomic (Queued -> Executing, reserving the slot
  and setting worker_id). The eviction drain (Evict) still requeues, so the
  no-wedge liveness is exercised across evict.
 ***************************************************************************)
CacheReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ CacheDispatchable(w)
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "cache"]
    /\ UNCHANGED << workerUp, loadSaturated, togglesLeft >>

(***************************************************************************
  FallbackReserve(o, w): the LRU/MRU backstop dispatches op o to w. Fires
  when the cache tiers cannot serve o — i.e. NO worker is CacheDispatchable
  for a fresh op. This is the Phase-2 no-wedge path. It is P-headroom-UNGATED
  (any BaseViable worker), matching inner_find_worker_for_action.

  Guard `~(\E ww : CacheDispatchable(ww))`: the fallback is reached only when
  every cache tier declined [the `else` branch at :1646]. This is the precise
  cascade order — cache tiers first, fallback last.
 ***************************************************************************)
FallbackReserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ FallbackDispatchable(w)
    /\ ~(\E ww \in Workers : CacheDispatchable(ww))   \* cache tiers all declined
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = w]
    /\ lastDispatchTier' = [lastDispatchTier EXCEPT ![o] = "fallback"]
    /\ UNCHANGED << workerUp, loadSaturated, togglesLeft >>

(***************************************************************************
  Complete(o): worker finishes; slot freed. Frees a P slot, so a worker that
  was P-gated regains P-headroom (freshness, I5 — the count updates on
  completion). [complete_action :1793, running_action_infos removal]
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
    /\ UNCHANGED << workerUp, loadSaturated, togglesLeft >>

(***************************************************************************
  Evict(w): worker disconnects; drain requeues its ops (base compensator).
  Frees slots (dispatch-count drops → P-headroom recomputes fresh, I5).
  [immediate_evict_worker → remove_worker → drain :1963;
   UpdateWithDisconnect => Queued + worker_id NONE :862,:873]
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
    /\ UNCHANGED << loadSaturated, togglesLeft >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, loadSaturated, lastDispatchTier, togglesLeft >>

(***************************************************************************
  ToggleLoadSat(w): the environment flips a worker's LOAD-saturation flag,
  INDEPENDENTLY of its dispatch-count (A4). This models load rising/falling
  from long or I/O-bound actions with no relation to the reservation count —
  the exact scenario A4 warns about (at p_count but lightly loaded, or below
  p_count but heavily loaded). Only enabled under AllowLoadSat so the pure
  no-wedge check can isolate the M1 gate.
 ***************************************************************************)
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

(***************************************************************************
  Fairness. Dispatch actions (both tiers) are weakly fair so a Queued op with
  an available worker cannot be starved. Complete is weakly fair so slots
  free (P-headroom regenerates). Reconnect is weakly fair. Evict and
  ToggleLoadSat are NOT fair (adversary events) — the liveness cfgs bound
  them via state constraint / AllowEvict.

  NOTE: fairness on FallbackReserve is what makes the Phase-2 lift a genuine
  progress guarantee — when all workers are at p_count (no cache-tier pick),
  the fair fallback eventually dispatches. This is the A4 no-wedge core.
 ***************************************************************************)
Fairness ==
    /\ \A o \in Ops, w \in Workers : WF_vars(CacheReserve(o, w))
    /\ \A o \in Ops, w \in Workers : WF_vars(FallbackReserve(o, w))
    /\ \A o \in Ops : WF_vars(Complete(o))
    /\ \A w \in Workers : WF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  INVARIANTS I1-I5 (VERBATIM from design §4)
 ***************************************************************************)

(* ── I1 (P-headroom-first) — ACTION property ──
   If any viable worker has P-headroom, no action is dispatched VIA THE CACHE
   TIERS to a viable worker WITHOUT P-headroom.

   This is inherently an ACTION-TIME property: "at the moment the cache tier
   selects w, w was Phase-1-legal." It CANNOT be a pure state invariant over
   the post-state — a dispatch that was legal in Phase 2 (no viable P-headroom
   worker existed) must stay legal even if a worker later RECONNECTS and
   regains P-headroom. (An earlier state-invariant encoding produced exactly
   this stale-witness false positive: cache-dispatch o2→w1 while w2,w3 down was
   legal, then w2 reconnecting retro-flagged it. Model artifact, NOT a design
   gap.)

   So I1 is checked as a TEMPORAL ACTION property `[][I1CacheStep]_vars`: on
   EVERY CacheReserve(o,w) step, the Phase-1 legality must hold in the PRE-state
   (unprimed). Because CacheReserve's own guard is `CacheDispatchable(w)` which
   entails `SomeViableHasPHeadroom => HasPHeadroom(w)`, this holds by
   construction — and the BuggedNoGate variant, which drops that conjunct,
   makes TLC produce the counterexample (teeth). *)
CacheReserveStep ==
    \E o \in Ops, w \in Workers :
        /\ opStage[o] = "Queued"
        /\ opStage'[o] = "Executing"
        /\ lastDispatchTier'[o] = "cache"
        /\ opWorker'[o] = w

(* The Phase-1 legality of the cache dispatch, evaluated in the PRE-state:
   if some viable worker has P-headroom, the chosen worker w must too. *)
CacheDispatchPhase1Legal(w) ==
    SomeViableHasPHeadroom => HasPHeadroom(w)

(* I1 as an action invariant: whenever a cache dispatch happens, the chosen
   worker was Phase-1-legal at decision time. Used in PROPERTIES as
   [][I1CacheStep]_vars. *)
I1CacheStep ==
    \A o \in Ops, w \in Workers :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ lastDispatchTier'[o] = "cache"
          /\ opWorker'[o] = w )
        => CacheDispatchPhase1Legal(w)

I1_PHeadroomFirst == [][I1CacheStep]_vars

(* ── I3 (locality bounded) — ACTION property ──
   Locality affects selection only within the P-headroom set. In this model
   "locality selection" IS the cache tier (Tier 1/1.5/2 all gate on
   worker_is_viable). So I3 is the SAME action-time guarantee as I1: every
   locality/cache dispatch lands inside the Phase-1 P-headroom set. Identical
   step predicate; kept as a distinct named property so the design's I3 line
   item is explicitly checked. *)
I3_LocalityBounded == [][I1CacheStep]_vars

(* ── I4 (heterogeneity-safe) ──
   The gate uses each worker's OWN p_core_count and OWN in-flight count — no
   fleet constant. We verify the gate is NEVER applied with a shared/fleet
   threshold: HasPHeadroom(w) must equal the per-worker formula for w's own
   PCoreCount[w]. This is a tautology in the model (HasPHeadroom is defined
   per-worker), so the REAL I4 content is: the model RUNS with heterogeneous
   PCoreCount (incl. a 0) and all other invariants still hold — i.e. no cell
   assumes homogeneity. We add a structural check that at least the guard for
   p_count==0 workers is always ungated (A5), which a fleet-constant gate
   would violate. *)
I4_HeterogeneitySafe ==
    \A w \in Workers :
        (PCoreCount[w] = 0) => HasPHeadroom(w)

(* ── I5 (freshness) ──
   The gate signal is the scheduler-maintained dispatch-count, updated on
   assign AND completion — never a stale worker-reported load. We verify the
   dispatch-count used by the gate EQUALS the live reservation-set size at all
   times (no stale snapshot): HasPHeadroom is computed from Running(w) =
   Cardinality(reserved[w]), which is mutated atomically by CacheReserve /
   FallbackReserve (assign) and Complete / Evict (completion/drain). The
   invariant is that no op is BOTH counted in reserved AND already Completed
   (which would be a stale count inflating Running past the true in-flight
   set) — i.e. the count is fresh. Also: a Completed or Queued op never
   occupies a live worker's reservation slot (stale-count source). *)
I5_Freshness ==
    /\ \A w \in Workers : \A o \in reserved[w] :
         workerUp[w] => opStage[o] \in {"Executing"}
    /\ \A o \in Ops :
         (opStage[o] \in {"Queued", "Completed"}) =>
            \A w \in Workers : (workerUp[w] => o \notin reserved[w])

(* Physical capacity never exceeded (base safety, carried). *)
CapacityRespected ==
    \A w \in Workers : workerUp[w] => Running(w) <= Capacity

(* Evicted worker holds no reservations (base safety, carried — a stale
   reservation on a down worker is a stale-count / orphan). *)
EvictedEmpty ==
    \A w \in Workers : ~workerUp[w] => reserved[w] = {}

(* State-invariant bundle (I1/I3 are TEMPORAL action properties, checked via
   PROPERTIES, not here). *)
Safety ==
    /\ TypeOK
    /\ I4_HeterogeneitySafe
    /\ I5_Freshness
    /\ CapacityRespected
    /\ EvictedEmpty

------------------------------------------------------------------------------
(***************************************************************************
  I2 (no-wedge / progress) — the load-bearing A4 cross-product liveness.

  If >= 1 worker can accept work, an action is always dispatched. This MUST
  hold across the JOINT state of BOTH predicates (M1 P-gate AND
  load-saturation): a worker at exactly p_count but lightly loaded is P-gated
  but not load-saturated — the gate must still dispatch it via the Phase-2
  lift / fallback.

  Encoding: every Queued op eventually reaches Executing or Completed, so long
  as SOME worker can physically accept work. The disjunction of CacheReserve
  and FallbackReserve must be enabled whenever a free slot exists — this is
  the structural no-wedge lemma, checked as a SAFETY invariant too
  (DispatchAlwaysPossible), plus the temporal leads-to.
 ***************************************************************************)

(* Structural no-wedge: if any worker can accept work AND some op is Queued,
   then SOME dispatch action (cache OR fallback) is enabled. This is the
   cross-product core — it must hold for EVERY joint (P-gate × load-sat)
   configuration. If this ever fails, the gate wedges. *)
SomeOpQueued == \E o \in Ops : opStage[o] = "Queued"
SomeWorkerCanAccept == \E w \in Workers : HasFreeSlot(w)

DispatchEnabled ==
    \/ \E o \in Ops, w \in Workers : (opStage[o] = "Queued" /\ CacheDispatchable(w))
    \/ \E o \in Ops, w \in Workers :
         (opStage[o] = "Queued" /\ FallbackDispatchable(w)
            /\ ~(\E ww \in Workers : CacheDispatchable(ww)))

(* THE no-wedge safety lemma (A4 cross-product). Whenever work is pending and
   a worker can take it, a dispatch is enabled — regardless of the joint
   P-gate/load-sat state. This is checked EXHAUSTIVELY over every reachable
   (P-gate × load-sat) configuration, which is exactly the A4 requirement. *)
NoWedge ==
    (SomeOpQueued /\ SomeWorkerCanAccept) => DispatchEnabled

(* The temporal liveness: every Queued op eventually dispatches. *)
EventuallyDispatched ==
    \A o \in Ops :
        (opStage[o] = "Queued") ~> (opStage[o] \in {"Executing", "Completed"})

=============================================================================
