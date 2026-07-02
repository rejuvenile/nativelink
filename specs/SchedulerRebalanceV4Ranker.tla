------------------------- MODULE SchedulerRebalanceV4Ranker -------------------------
(***************************************************************************
  M1 v2 — RANKER axis of the P-headroom refinement. The prior prove
  (SchedulerRebalanceV3PLoad.tla) modeled the GATE predicate (who is
  ELIGIBLE) + I5_Bounded (admission COUNT bounded under adversarial stale-low
  p_load). It EXPLICITLY skipped the RANKER: which eligible worker WINS a
  dispatch. The 5-reviewer design cadre (2026-07-01) returned a CONVERGENT
  RECONSIDER (red-team P + distsys MAJOR) precisely there — the winner among
  eligible workers is chosen by `min load_penalty` (cache tiers, :1603/:1685)
  and `min effective_load_score` (fallback, :1188), BOTH pure functions of the
  STALE `p_core_load_pct`. So a stale-low p_load makes an override-admitted
  worker (Running >= PCoreCount) the PREFERRED WINNER, not merely eligible — a
  bounded magnet: it wins every same-root dispatch for the ~2.5s the stale
  reading persists, piling to the ceiling on a worker whose P cores may be about
  to saturate. This spec models the RANKER and proves the §12 fix closes it.

  Reference: .claude/audits/scheduler-m1-v2-pload-refinement-design-2026-07-01.md
    §10  the convergent RECONSIDER (why the ranker matters)
    §12  the revised ranker-aware mechanism (the fix under test)
    §12.1 the p_headroom_pref ranking key
    §12.2 where it plugs in (Tier-1 :1603, Tier-1.5 :1685, Tier-2 :1727,
          fallback :1188) — all becoming (p_headroom_pref, <existing load key>)
    §12.4 I6 (ranker-consistency) — the NEW invariant this spec is built to prove

  Production citations VERIFIED against api_worker_scheduler.rs 2026-07-01:
    [rank-t1]   :1603  cache Tier-1: `min load_penalty` (= cap_score(w).load_penalty)
    [rank-t15]  :1685  cache Tier-1.5: `max (cached_score - load_penalty)`
    [rank-fb]   :1188  fallback sort-key `(bool !has_p_headroom, effective_load_score)`
    [penalty]   :440/:489 capacity_score.load_penalty — PURE FN of eff_p_load
                (:473 `100 - eff_p_load`), i.e. monotone-increasing in p_core_load_pct.
                Lower p_load => lower load_penalty => WINS min-selection. This is the
                stale-signal ranker channel the magnet exploits.
    [els]       :514  effective_load_score(p_load,...) — same monotone dependency.
    [gate-v2]   design §2 :40-45  worker_has_p_headroom_v2 (3-clause, KEPT verbatim
                for ELIGIBILITY per §12 — this spec reuses it unchanged).
    [pref]      design §12.1 :301-305  p_headroom_pref(w).

  ------------------------------------------------------------------------
  MODELLING THE RANKER (the whole point — V3 could not express this).

    V3's CacheReserve(o,w) picked an ARBITRARY eligible w (\E w). That is
    adequate for admission-COUNT invariants (I5_Bounded) but VACUOUS for a
    ranker invariant: I6 is a statement about WHICH eligible worker is chosen.
    So here Dispatch(o) does NOT existentially pick; it computes the WINNER =
    the eligible worker minimizing the lexicographic key (pref, LoadScore), and
    reserves THAT worker. Ties broken by a fixed worker order (WinnerOf), so the
    winner is a deterministic function of the state — exactly `min_by_key`
    returning the FIRST minimum (:1218 comment).

    LoadScore : Workers -> Nat  is the STALE worker-reported load rank — the
    abstraction of load_penalty / effective_load_score. Lower = preferred (the
    production `min`). It is DECOUPLED from true occupancy Running(w): the only
    writer is the adversary StaleLoadScore, which can hold it LOW on an override
    worker (Running >= PCoreCount) forever while that worker's fresh Running
    climbs — the keepalive-lag stale-low case (design §3, §10). This is the
    magnet's fuel: without the pref key, a stale-low LoadScore lets the
    override worker out-rank a genuine-free-slot worker.

    p_headroom_pref(w) (design §12.1) is the PRIMARY key; LoadScore is SECONDARY.
      pref(w) = 0                        if gate inactive OR Running<PCoreCount
              = 1 + (Running-PCoreCount) if override-eligible (clause 3 fired)
              = MAXPREF                  otherwise (no headroom; fallback-only)
    A genuine-free-slot worker (pref 0) therefore beats ANY override-admit
    (pref >= 1) regardless of LoadScore — the ranker can no longer be fooled.

  TEETH toggles (design §12.4):
    BuggedStaleRankOnly = TRUE  => the OLD key: pref is forced 0 for EVERY
        eligible worker (i.e. no p_headroom_pref at all), so selection is pure
        `min LoadScore`. A stale-low override worker then out-ranks a
        genuine-free-slot worker => MUST violate I6_RankerConsistency. This
        reproduces the exact production defect §10 describes.
    PrefFlat = TRUE  => override pref = 1 FLAT (drop the +(Running-PCoreCount)
        fresh-count term). Tier separation (0 free / 1 override / MAX none)
        remains, so I6 (free-vs-override ordering) should STILL hold; but the
        WITHIN-override-tier fresh-count decay is gone, so PrefMonotone
        (override preference strictly worsens as Running climbs) is VIOLATED.
        This isolates what the fresh-count term is load-bearing FOR.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,        \* set of worker IDs, e.g. {w1, w2, w3}
    Ops,            \* set of operation IDs
    WorkerOrder,    \* Seq of Workers giving the deterministic tie-break order
                    \* (models `min_by_key` returning the FIRST minimum, :1218).
    MaxToggles,     \* bound on adversary LoadScore flips (keeps state space finite;
                    \* liveness cfgs small, safety cfgs large ~ unbounded adversary).
    MaxScore,       \* LoadScore ranges 0..MaxScore. Small (e.g. 2) suffices — I6 is
                    \* about ORDER, not magnitude; the adversary needs only enough
                    \* distinct ranks to make an override worker out-rank a free one.
    PCoreCount,     \* [Workers -> Nat] per-worker p_core_count (via <- override).
    Capacity,       \* hard reservation-slot cap per worker. Capacity > PCoreCount*FACTOR
                    \* on the tested worker so the FRESH-count ceiling, not the slot
                    \* cap, is the binding override limit (same discipline as V3).
    THRESHOLD,      \* p_idle_threshold_pct: LoadScore reported "idle enough" iff the
                    \* worker's reported p_load pct < THRESHOLD. Encoded via PLoad.
    FACTOR,         \* p_headroom_override_factor: override ceiling = PCoreCount*FACTOR.
    AdmitAllThreshold, \* TEETH: TRUE => idle predicate TRUE for all (threshold=inf).
    DropCeiling,    \* TEETH: TRUE => drop clause-3 ceiling (pure OR p_idle).
    BuggedStaleRankOnly, \* TEETH (BuggedStaleRankWins): TRUE => pref forced 0 for all
                    \* eligible => pure `min LoadScore` => the OLD stale-only ranker.
    PrefFlat,       \* TEETH (BuggedPrefIgnoresFreshCount): TRUE => override pref = 1
                    \* flat (drop +(Running-PCoreCount)).
    GateActive,     \* whether the p_headroom gate + ranker are active. FALSE models
                    \* flag-off / Phase-2 lift => pref === 0 => v1 ranker parity.
    AllowEvict      \* TRUE => environment may evict a reserved worker (drain-requeue).

(* Per-cfg p_core_count assignments (TLC cfg cannot parse function literals). *)
PCoreCount_Het    == [w \in Workers |->
                        IF w = "w1" THEN 1
                        ELSE IF w = "w2" THEN 2
                        ELSE IF w = "w3" THEN 0
                        ELSE 1]
PCoreCount_All1   == [w \in Workers |-> 1]
PCoreCount_All2   == [w \in Workers |-> 2]

(* Deterministic tie-break order for min-by-key (matches min_by_key FIRST-min). *)
Order_w123 == << "w1", "w2", "w3" >>

(* PLoad lattice encoded as percentages so `idle < THRESHOLD <= busy`. Reused
   from V3: the STALE reported p_load that gates clause 3 (WHO is override-
   eligible). Distinct from LoadScore (the ranker channel) — both are stale, but
   PLoad gates ELIGIBILITY (clause 3) while LoadScore drives the WINNER order. *)
PIdlePct == 0
PBusyPct == 100
PLoadPct(pl) == IF pl = "idle" THEN PIdlePct ELSE PBusyPct

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
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

(* MAXPREF: the "no headroom" preference sentinel (u64::MAX in prod §12.1).
   Must dominate any real pref = 1+(Running-PCoreCount) <= 1+Capacity. *)
MAXPREF == 1000

VARIABLES
    opStage,        \* Ops -> Stages
    opWorker,       \* Ops -> WorkerOrNone : action-DB assigned worker_id
    reserved,       \* Workers -> SUBSET Ops : running_action_infos (FRESH count source)
    workerUp,       \* Workers -> BOOLEAN : worker present in the pool
    pLoad,          \* Workers -> PLoadVals : STALE reported p_load — gates clause 3.
    loadScore,      \* Workers -> 0..MaxScore : STALE reported load rank (load_penalty /
                    \* effective_load_score abstraction). DECOUPLED from Running; only
                    \* the adversary StaleLoadScore writes it. Lower = preferred (`min`).
    lastDispatchWinner, \* Ops -> WorkerOrNone : the worker the LAST Dispatch picked
                        \* (for the action-time I6 winner check).
    togglesLeft     \* remaining adversary LoadScore flips (bound only).

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
  THE v2 GATE (eligibility) — REUSED verbatim from V3 / design §2. This spec
  does NOT re-litigate the gate; §12 keeps it as-is. It only decides WHO is
  eligible; the ranker below decides who WINS.
 ***************************************************************************)
Running(w) == Cardinality(reserved[w])

PReportedIdle(w) == AdmitAllThreshold \/ (PLoadPct(pLoad[w]) < THRESHOLD)
Clause3Ceiling(w) == DropCeiling \/ (Running(w) < PCoreCount[w] * FACTOR)

(* override_eligible(w) = clause 3 fired (design §12.1 :307). *)
OverrideEligible(w) == PReportedIdle(w) /\ Clause3Ceiling(w)

HasPHeadroom(w) ==
    \/ PCoreCount[w] = 0
    \/ Running(w) < PCoreCount[w]
    \/ OverrideEligible(w)

HasFreeSlot(w) == workerUp[w] /\ Running(w) < Capacity
BaseViable(w) == HasFreeSlot(w)

(* Cache-tier viability (Phase-1 gate + Phase-2 lift), unchanged from V3. *)
SomeViableHasPHeadroom == \E w \in Workers : BaseViable(w) /\ HasPHeadroom(w)
CacheTierViable(w) ==
    /\ BaseViable(w)
    /\ (SomeViableHasPHeadroom => HasPHeadroom(w))

(***************************************************************************
  THE RANKER — the §12.1 p_headroom_pref preference key (the FIX under test).

  GenuineFreeSlot(w): a genuine FREE P slot by the FRESH count (clause 2). This
  is the "best" tier and the property I6 is about. p_count==0 (A5) workers have
  no P-slot ceiling; they count as genuine-free (they are never override-admits)
  — consistent with prod pref=0 for gate-inactive/ungated.
 ***************************************************************************)
GenuineFreeSlot(w) == (PCoreCount[w] = 0) \/ (Running(w) < PCoreCount[w])

(* p_headroom_pref(w) — design §12.1. SMALLER = preferred.
   - !GateActive  => 0 for all (v1 parity, §12.3).
   - GenuineFreeSlot => 0 (clause 2 / A5).
   - OverrideEligible => 1 + (Running - PCoreCount)  [PrefFlat TEETH drops the term => 1]
   - else => MAXPREF (no headroom; only reachable on the soft fallback path).
   BuggedStaleRankOnly TEETH: force 0 for ALL eligible workers => the OLD
   stale-only key (pure min LoadScore, no headroom preference). *)
PHeadroomPref(w) ==
    IF BuggedStaleRankOnly THEN 0
    ELSE IF ~GateActive THEN 0
    ELSE IF GenuineFreeSlot(w) THEN 0
    ELSE IF OverrideEligible(w)
         THEN (IF PrefFlat THEN 1 ELSE 1 + (Running(w) - PCoreCount[w]))
    ELSE MAXPREF

(* Lexicographic key (pref, LoadScore). Compare a BEFORE b (strictly better). *)
KeyLt(a, b) ==
    \/ PHeadroomPref(a) < PHeadroomPref(b)
    \/ ( PHeadroomPref(a) = PHeadroomPref(b) /\ loadScore[a] < loadScore[b] )

(* Position of w in the tie-break order (for FIRST-minimum determinism). *)
OrderIdx(w) ==
    CHOOSE i \in 1..Len(WorkerOrder) : WorkerOrder[i] = w

(* a out-ranks b: strictly-better key, OR equal key but earlier in tie order
   (min_by_key returns the FIRST minimum). *)
Outranks(a, b) ==
    \/ KeyLt(a, b)
    \/ ( ~KeyLt(a, b) /\ ~KeyLt(b, a) /\ OrderIdx(a) < OrderIdx(b) )

(***************************************************************************
  Eligibility for a dispatch. Two regimes mirror production:
   - CACHE regime: eligible = CacheTierViable (has root AND passes gate/lift).
     Modeled as: the op's candidate set is ALL viable workers (every worker can
     have the root — worst case for I6, the largest eligible set). The winner is
     the min-key CacheTierViable worker.
   - FALLBACK regime: fires only when NO worker is CacheTierViable-eligible;
     eligible = any BaseViable worker (P-headroom-UNGATED). Winner = min-key
     among BaseViable.
  For the I6 property we treat BOTH regimes uniformly: "eligible set for this
  dispatch" = the set the winner is chosen from. I6 must hold in EACH.
 ***************************************************************************)
CacheEligible == { w \in Workers : CacheTierViable(w) }
FallbackEligible == { w \in Workers : BaseViable(w) }

(* The eligible set actually used for the next dispatch: cache if non-empty,
   else fallback. (Matches the cascade: cache tiers first, LRU/MRU backstop.) *)
EligibleSet == IF CacheEligible # {} THEN CacheEligible ELSE FallbackEligible

(* WinnerOf(S): the unique worker in S that out-ranks all others in S (or is
   equal-key but earliest in tie order). Deterministic min-by-key. *)
WinnerOf(S) == CHOOSE w \in S : \A x \in S : (x = w) \/ Outranks(w, x)

(***************************************************************************
  Dispatch(o): the ranker-driven dispatch. Picks the WINNER (min key) among the
  eligible set and reserves it. pLoad and loadScore are UNCHANGED — dispatching
  does NOT refresh the stale reported signals; only the adversary moves them.
  This is what lets an override worker's Running climb while its loadScore stays
  stale-low.
 ***************************************************************************)
Dispatch(o) ==
    /\ opStage[o] = "Queued"
    /\ EligibleSet # {}
    /\ LET w == WinnerOf(EligibleSet) IN
         /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
         /\ opWorker' = [opWorker EXCEPT ![o] = w]
         /\ lastDispatchWinner' = [lastDispatchWinner EXCEPT ![o] = w]
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ UNCHANGED << workerUp, pLoad, loadScore, togglesLeft >>

(***************************************************************************
  Complete(o): worker finishes; FRESH count drops -> pref recomputes fresh.
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
    /\ lastDispatchWinner' = [lastDispatchWinner EXCEPT ![o] = NONE]
    /\ UNCHANGED << workerUp, pLoad, loadScore, togglesLeft >>

(***************************************************************************
  Evict(w): worker disconnects; drain requeues its ops (base compensator).
 ***************************************************************************)
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

(***************************************************************************
  StaleLoadScore(w, s) + StalePLoad(w, pl): THE ADVERSARY. Sets a worker's
  STALE reported signals to ANY value, DECOUPLED from Running(w). Because both
  are stale and every dispatch leaves them UNCHANGED, the adversary can hold an
  override worker's loadScore LOW while its Running climbs — the keepalive-lag
  stale-low magnet fuel (design §3, §10). ONE toggle budget shared across both
  keeps the state space finite; even ZERO toggles, the initial all-low-idle
  state already exercises the stale-low worst case.
 ***************************************************************************)
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
    /\ pLoad = [w \in Workers |-> "idle"]     \* worst case: reported idle from t=0
    /\ loadScore = [w \in Workers |-> 0]        \* worst case: reported LOW from t=0
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
  I6_RankerConsistency — THE NEW HEADLINE INVARIANT (design §12.4).

  When the gate is active, if ANY eligible worker for a dispatch has a genuine
  free P slot, then the dispatch WINNER has a genuine free P slot. I.e. an
  override-admit NEVER beats a genuine-free-slot eligible worker, for ANY
  LoadScore assignment (including adversarial stale-low on the override worker).

  ACTION-TIME form: checked on every Dispatch step. In the PRE-state, look at
  the eligible set the winner was chosen from; if it contained a genuine-free-
  slot worker, the winner (the worker o was assigned to) must itself be a
  genuine-free-slot worker.

  We evaluate GenuineFreeSlot in the PRE-state (unprimed) because the winner's
  eligibility/pref was decided from the pre-state — the reservation of o has not
  yet been applied to the ranking decision. (`w == WinnerOf(EligibleSet)` in
  Dispatch is a pre-state computation.)

  When GateActive = FALSE this is VACUOUSLY handled by the guard (we scope I6 to
  gate-active states); parity is proved separately by RankerParity.
 ***************************************************************************)
I6Step ==
    \A o \in Ops :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ opWorker'[o] \in Workers )   \* o was just dispatched to some worker
        =>
        LET winner == opWorker'[o]
            elig == EligibleSet           \* PRE-state eligible set (unprimed)
        IN  GateActive =>
              ( (\E e \in elig : GenuineFreeSlot(e)) => GenuineFreeSlot(winner) )
I6_RankerConsistency == [][I6Step]_vars

(***************************************************************************
  PrefMonotone — the WITHIN-override-tier invariant the fresh-count term
  (+(Running-PCoreCount)) is (hypothesized) load-bearing for. States that among
  two override-eligible workers, the one with the LARGER fresh Running has a
  STRICTLY WORSE (larger) pref — so the override preference DECAYS monotonically
  as a worker fills toward the ceiling (design §12.1 :313-315, closing red-team's
  "duration-of-preference" gap). This is a STATE invariant over the pref fn.

  With the fresh-count term: pref = 1+(Running-PCoreCount) is strictly increasing
  in Running => HOLDS. PrefFlat TEETH (pref = 1 flat) => two override workers at
  DIFFERENT Running have EQUAL pref => VIOLATED. This pinpoints exactly what the
  term buys: NOT the free-vs-override ordering (I6, which the tier gap alone
  gives), but the intra-tier fresh-count decay.

  Guarded to GateActive /\ ~BuggedStaleRankOnly /\ ~PrefFlat is NOT applied here —
  we WANT it to fail under PrefFlat. We restrict to GATE-active, non-stale-only
  states (where pref is the real key), and to workers of the SAME PCoreCount so
  the comparison isolates Running (heterogeneous p_count shifts the baseline).
 ***************************************************************************)
PrefMonotone ==
    GateActive =>
      \A a, b \in Workers :
        ( /\ ~BuggedStaleRankOnly
          /\ OverrideEligible(a) /\ ~GenuineFreeSlot(a)
          /\ OverrideEligible(b) /\ ~GenuineFreeSlot(b)
          /\ PCoreCount[a] = PCoreCount[b]
          /\ Running(a) < Running(b) )
        => PHeadroomPref(a) < PHeadroomPref(b)

(***************************************************************************
  RankerParity — with the gate inactive (GateActive=FALSE, models flag-off OR
  Phase-2 lift, design §12.3), p_headroom_pref === 0 for all workers, so the
  ranker key collapses to the SECONDARY key (LoadScore) alone — byte-identical
  to the v1 `min LoadScore` selection. We assert: in every reachable state, for
  every pair, the (pref,LoadScore) comparison agrees with the pure-LoadScore
  comparison, i.e. PHeadroomPref is constant 0 => KeyLt reduces to loadScore<.

  Proved as a STATE invariant: \A w : PHeadroomPref(w) = 0  (when gate inactive).
  Given that, KeyLt(a,b) == loadScore[a] < loadScore[b] identically, so WinnerOf
  is stuttering-equivalent to the v1 min-LoadScore winner. (Only meaningful when
  the cfg sets GateActive = FALSE.)
 ***************************************************************************)
RankerParity == GateActive \/ (\A w \in Workers : PHeadroomPref(w) = 0)

(* Byte-parity corollary made explicit: when gate inactive, KeyLt IS pure
   LoadScore order for every pair. Checked directly so a regression in the pref
   fn that happened to keep pref=0 but broke KeyLt would still be caught. *)
KeyIsPureLoad ==
    GateActive \/
      (\A a, b \in Workers : KeyLt(a, b) = (loadScore[a] < loadScore[b]))

------------------------------------------------------------------------------
(***************************************************************************
  Carried invariants (from V3 — re-checked under the RANKER dynamics).
 ***************************************************************************)

(* I5_Bounded (STATE form) — carried. As V3 established, the pure-state bound is
   FALSIFIED by the Phase-2 no-wedge lift, NOT by the ranker; checked only in the
   no-lift topology (CeilingNoLift cfg). Stated here for that cfg. *)
I5_Bounded ==
    \A w \in Workers :
        (workerUp[w] /\ PCoreCount[w] > 0) => Running(w) <= PCoreCount[w] * FACTOR

(* I5_BoundedOverride (ACTION-TIME form) — the provable admission-count claim,
   carried from V3. On every Dispatch into a gate-governing (Phase-1) state, a
   gated winner was below the ceiling at decision time. *)
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

(* I1 (P-headroom-first) — cache dispatch chose a Phase-1-legal worker. In this
   ranker spec, a cache dispatch is one where the eligible set was CacheEligible
   (non-empty). The winner is CacheTierViable, hence Phase-1-legal by
   construction. Checked action-time. *)
CacheDispatchPhase1Legal(w) == SomeViableHasPHeadroom => HasPHeadroom(w)
I1CacheStep ==
    \A o \in Ops :
        ( /\ opStage[o] = "Queued"
          /\ opStage'[o] = "Executing"
          /\ opWorker'[o] \in Workers
          /\ CacheEligible # {} )       \* cache regime (eligible from cache set)
        => CacheDispatchPhase1Legal(opWorker'[o])
I1_PHeadroomFirst == [][I1CacheStep]_vars
I3_LocalityBounded == [][I1CacheStep]_vars

(* I4 (heterogeneity-safe) — p_count==0 workers always ungated (A5). *)
I4_HeterogeneitySafe ==
    \A w \in Workers : (PCoreCount[w] = 0) => HasPHeadroom(w)

(* I5 (freshness, base) — dispatch-count = live reservation set. *)
I5_Freshness ==
    /\ \A w \in Workers : \A o \in reserved[w] :
         workerUp[w] => opStage[o] \in {"Executing"}
    /\ \A o \in Ops :
         (opStage[o] \in {"Queued", "Completed"}) =>
            \A w \in Workers : (workerUp[w] => o \notin reserved[w])

CapacityRespected == \A w \in Workers : workerUp[w] => Running(w) <= Capacity
EvictedEmpty == \A w \in Workers : ~workerUp[w] => reserved[w] = {}

(***************************************************************************
  I2 (no-wedge / progress) — carried. The ranker is strictly a REORDERING of
  the same eligible set; it never empties a non-empty eligible set, so no-wedge
  is preserved (a queued op with an available worker is always dispatchable).
 ***************************************************************************)
SomeOpQueued == \E o \in Ops : opStage[o] = "Queued"
SomeWorkerCanAccept == \E w \in Workers : HasFreeSlot(w)
DispatchEnabled == \E o \in Ops : (opStage[o] = "Queued" /\ EligibleSet # {})
NoWedge == (SomeOpQueued /\ SomeWorkerCanAccept) => DispatchEnabled

EventuallyDispatched ==
    \A o \in Ops :
        (opStage[o] = "Queued") ~> (opStage[o] \in {"Executing", "Completed"})

Safety ==
    /\ TypeOK
    /\ I4_HeterogeneitySafe
    /\ I5_Freshness
    /\ CapacityRespected
    /\ EvictedEmpty
    /\ NoWedge

=============================================================================
