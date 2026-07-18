------------------- MODULE MarkStableAsyncDurabilityV2 -------------------
(***************************************************************************
  FL-688 never-BIS-acked durability pin-leak — V2 spec (2026-07-17).

  REWRITE of MarkStableAsyncDurability.tla to model the NOW-DIAGNOSED seam
  and address the design cadre's TWO BLOCK-2 findings (review e546af44):

    BLOCK-2a: the prior ACTION 5 (ServerBecomesDurableShortCircuit) RE-FUSED
      the cross-entry hop into ONE atom — it set serverDurable AND read
      serverLocality AND appended stableDigestsQueue in a single step. That is
      the exact false-atomicity class this spec family exists to prevent, so
      "Fixed passes" was not a proof.
    BLOCK-2b: the prior spec modeled serverLocality as MONOTONIC, hiding the
      locality-drift sibling class (a d in serverLocality gate that can miss).

  This V2 SPLITS the three real events onto DISTINCT actions over DISTINCT
  entries, and models locality CHURN explicitly:

    * ServerBecomesDurable(d)      — the durability FACT alone (independent
                                     async; some prior action/worker made D
                                     durable). Sets serverDurable ONLY.
    * WorkerUploadEcsDecision(d)   — the ExistenceCacheStore::update DECISION
                                     alone. READS serverDurable (an already-
                                     established fact), does NOT set it in the
                                     skip branch, and decides whether to feed
                                     mark_stable. This is the de-fused hop.
    * DrainAndBroadcast            — the global BIS emission alone.
    * LocalityEvicted(d)           — locality CHURN (serverLocality is NOT
                                     monotonic). Shows the fix is robust: the
                                     fix uses a GLOBAL BIS with NO locality
                                     gate, so churn cannot re-open the leak.

  ------------------------------------------------------------------------
  CODE-PROVEN MECHANISM (re-diagnosis a0d734f0, 8 live leaking digests):

   * A worker pins every F2 output INDEFINITELY, released ONLY by a BIS ack
     (running_actions_manager.rs:7803, pin_digest_indefinite_with_result;
     the indefinite/TTL-exempt class is moka_evicting_map.rs PinInfo.indefinite).

   * The worker streams D to slow_store.update / update_oneshot
     (running_actions_manager.rs:8200 / :8157) — a GrpcStore to the SERVER.
     The WORKER's own FastSlowStore IS bypassed (confirmed).

   * On the server, the write enters the CAS chain
     WorkerProxyStore -> VerifyStore -> ExistenceCacheStore ->
     SizePartitioning -> FastSlowStore. The leak is the
     ExistenceCacheStore::update DURABLE-SKIP branch
     (existence_cache_store.rs:607-631; update_oneshot :774-790):
       should_skip_for_durable_presence -> inner.has_durably = Some
       -> drains the reader, refreshes the cache, returns Ok
       WITHOUT calling inner_store.update.
     -> FastSlowStore::update is never reached
     -> the stable_digests_pusher (all 5 producers live INSIDE FSS
        update/update_oneshot/chunked/self-retry/mark_stable,
        fast_slow_store.rs:1840-1851) never fires
     -> NO BIS. The only BIS for D fired in the PAST, before this worker
        pinned. The pin is orphaned.

   * NECESSARY CONDITION (both hold for the leaking minority): D was ALREADY
     durable on the server AND already resident in the worker's own fast
     FilesystemStore when the action re-produced it. Already-durable ->
     ExistenceCacheStore durable-skip -> no fresh pusher. Already-resident ->
     re-produce dedup-skips with NO fresh on_insert
     (filesystem_store.rs:2410-2422); on_get re-registers the FROZEN original
     stamp (:1595-1601, LWW tie) -> NO new-stamp holdings delta -> the
     existing has_durably-gated mark_stable re-drive on each BlobsAvailable
     tick (worker_api_server.rs:3569-3583) is DELTA-BASED and MISSES it. Heals
     only via the UNGATED full snapshot (:3187) on reconnect/over-cap, which
     does not fire while connected and below pin-cap -> 30min-6.5h ages.

   * The BIS-acked MAJORITY is the complement: the worker is FIRST to make D
     durable -> has_durably = None -> the upload flows THROUGH FSS::update ->
     pusher fires -> global BIS reaches the pin holder.

  THE FIX (resolves cadre BLOCK-1, seam located): in the ExistenceCacheStore
   durable-skip branch call self.inner_store.mark_stable(&[digest]) before
   returning Ok. mark_stable_delegation forwards to inner
   (existence_cache_store.rs:1079 -> FSS mark_stable -> push_stable_digests
   -> global BIS). It is:
     - event-driven (fires ON the upload = the durability event),
     - >=2-replica-preserving (mark_stable is has_durably-gated by
       construction — the skip fired BECAUSE has_durably=Some),
     - NO locality gate (GLOBAL BIS unpins every holder), so locality churn
       cannot re-open the leak — the prior spec's monotonic-locality concern
       is MOOT for this fix,
     - NO new cross-entry wire, NO TTL.

  ------------------------------------------------------------------------
  CONSTANTS:
    MarkStableOnDurableSkip (BOOLEAN):
      FALSE (Bugged)  — the skip branch drives NO durability event -> orphan.
      TRUE  (Fixed)   — the skip branch drives mark_stable -> global BIS.
    FixGatesOnLocality (BOOLEAN): only meaningful when MarkStableOnDurableSkip.
      FALSE (real fix)          — GLOBAL BIS, ungated by locality (robust).
      TRUE  (naive/regression)  — the NAIVE locality-gated fix; under
                                  LocalityEvicted churn it MISSES = the
                                  reintroduced locality-drift class BLOCK-2b
                                  warned about. Demonstrates why the real fix
                                  must be ungated global BIS.

  SCOPE — modeled: one worker; a finite digest set; the indefinite pin set;
    the server durable set; the server locality view + its CHURN; a PAST
    fire-once global BIS; the deferred upload + the ExistenceCacheStore skip
    DECISION as its own action; the delta-based mark_stable re-drive that
    misses no-fresh-delta digests; the global BIS drain + per-worker unpin.
  SCOPE — NOT modeled (orthogonal): byte-stream payload, h2/GOAWAY, Bazel
    client dedup; the reconnect full-snapshot heal (a SEPARATE ungated
    convergence path — the leak here exists ABSENT a reconnect); the
    slow-write-FAIL trigger (batch-A PinLifecycle, a DISTINCT path that
    reaches FSS::update).
 ***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Digests,                    \* Set of digest identifiers (2-3 for TLC).
    MarkStableOnDurableSkip,    \* TRUE => fixed; FALSE => bugged.
    FixGatesOnLocality          \* TRUE => naive locality-gated fix (regression).

ASSUME MarkStableOnDurableSkip \in BOOLEAN
ASSUME FixGatesOnLocality \in BOOLEAN

VARIABLES
    workerHas,        \* Digests resident in the worker FS cache.
    workerPins,       \* Subset of workerHas held by an INDEFINITE pin
                      \*   (released only by a BIS ack).
    serverDurable,    \* Digests the server holds DURABLY (has_durably TRUE,
                      \*   slow-tier / >=2-replica). Independent async fact.
    serverLocality,   \* Server view of "worker holds these digests". NOT
                      \*   monotonic — LocalityEvicted churns it.
    pastBisDone,      \* Digests whose PAST global fire-once BIS already fired.
    pendingActions,   \* Outputs the worker still needs to produce.
    pendingUpload,    \* Digests the worker owes an upload (ECS decision).
    advertDelta,      \* Digests with a PENDING fresh holdings delta. Only a
                      \*   FRESH (not-already-resident) production emits one.
                      \*   Feeds the delta-based mark_stable re-drive.
    stableQueue,      \* mark_stable / pusher feeder set (dedups; BIS-idempotent).
    bisInFlight,      \* Global BIS frames en route to the worker.
    markedViaSkip     \* Witness: digests fed to mark_stable BY the durable-skip
                      \*   fix path (never removed). Non-vacuity tracking only.

vars == <<workerHas, workerPins, serverDurable, serverLocality, pastBisDone,
          pendingActions, pendingUpload, advertDelta, stableQueue, bisInFlight,
          markedViaSkip>>

----------------------------------------------------------------------------
Init ==
    /\ workerHas = {}
    /\ workerPins = {}
    /\ serverDurable = {}
    /\ serverLocality = {}
    /\ pastBisDone = {}
    /\ pendingActions = Digests
    /\ pendingUpload = {}
    /\ advertDelta = {}
    /\ stableQueue = {}
    /\ bisInFlight = {}
    /\ markedViaSkip = {}

----------------------------------------------------------------------------
(* ACTION (2) ServerBecomesDurable(d): an INDEPENDENT async fact — some prior
   action or another worker made D durable on the server. Sets serverDurable
   ONLY (durable-once). Does NOT touch locality, the queue, or pins. This is
   the de-fused durability FACT: distinct from the ECS decision that later
   reads it. It may fire BEFORE OR AFTER the worker produces/pins/uploads. *)
----------------------------------------------------------------------------
ServerBecomesDurable(d) ==
    /\ d \notin serverDurable
    /\ serverDurable' = serverDurable \cup {d}
    /\ UNCHANGED <<workerHas, workerPins, serverLocality, pastBisDone,
                   pendingActions, pendingUpload, advertDelta, stableQueue,
                   bisInFlight, markedViaSkip>>

----------------------------------------------------------------------------
(* ACTION (3) PastBisBroadcast(d): the GLOBAL fire-once BIS that already fired
   for D at a past drain (api_worker_scheduler.rs:9268 — a global broadcast,
   NOT persistent per-worker state). It unpins holders PRESENT at broadcast
   time. Fire-once (pastBisDone). A worker that pins D AFTER this never learns
   from it — the orphan setup. *)
----------------------------------------------------------------------------
PastBisBroadcast(d) ==
    /\ d \in serverDurable
    /\ d \notin pastBisDone
    /\ pastBisDone' = pastBisDone \cup {d}
    /\ workerPins' = workerPins \ {d}          \* unpins holders present NOW
    /\ UNCHANGED <<workerHas, serverDurable, serverLocality, pendingActions,
                   pendingUpload, advertDelta, stableQueue, bisInFlight,
                   markedViaSkip>>

----------------------------------------------------------------------------
(* ACTION (1) WorkerProduceAndPin(d, emitsDelta): the worker completes an
   action whose output is D, writes it locally, and takes an INDEFINITE pin
   (held until a BIS ack). Registers locality and owes an upload.
     emitsDelta = TRUE  models a genuinely-new production (D was not already
                        resident) -> a fresh on_insert -> a fresh holdings
                        delta the re-drive can later act on.
     emitsDelta = FALSE models re-producing an ALREADY-RESIDENT D -> dedup-
                        skip, on_get re-registers the FROZEN stamp -> NO fresh
                        delta (the leaking-minority condition). *)
----------------------------------------------------------------------------
WorkerProduceAndPin(d, emitsDelta) ==
    /\ d \in pendingActions
    /\ workerHas' = workerHas \cup {d}
    /\ workerPins' = workerPins \cup {d}
    /\ serverLocality' = serverLocality \cup {d}   \* worker now holds D
    /\ pendingActions' = pendingActions \ {d}
    /\ pendingUpload' = pendingUpload \cup {d}
    /\ advertDelta' = IF emitsDelta THEN advertDelta \cup {d} ELSE advertDelta
    /\ UNCHANGED <<serverDurable, pastBisDone, stableQueue, bisInFlight,
                   markedViaSkip>>

----------------------------------------------------------------------------
(* ACTION (4) WorkerUploadEcsDecision(d): the worker's deferred upload reaches
   the server CAS chain; ExistenceCacheStore::update makes its DECISION. This
   is its OWN action, distinct from the durability FACT it reads.

     * If d IN serverDurable  -> DURABLE-SKIP (has_durably = Some). Returns Ok
       WITHOUT inner_store.update; FSS::update never reached; pusher bypassed.
         - Bugged (~MarkStableOnDurableSkip): feeds NOTHING -> orphan.
         - Fixed  (MarkStableOnDurableSkip):
             * real fix (~FixGatesOnLocality): feeds mark_stable UNCONDITIONALLY
               (global BIS, no locality gate).
             * naive fix (FixGatesOnLocality): feeds only if d IN serverLocality
               -> a LocalityEvicted churn between produce and upload MISSES it
               = the reintroduced locality-drift class.
       serverDurable is UNCHANGED (already durable — de-fused, not re-set here).

     * If d NOTIN serverDurable -> the write FLOWS THROUGH FSS::update: the
       fresh slow-write pusher fires (feeds mark_stable) AND the blob becomes
       durable. This is the BIS-acked majority; correct in BOTH regimes. *)
----------------------------------------------------------------------------
WorkerUploadEcsDecision(d) ==
    /\ d \in pendingUpload
    /\ pendingUpload' = pendingUpload \ {d}
    /\ IF d \in serverDurable
       THEN \* ExistenceCacheStore durable-skip branch
            /\ LET feed == MarkStableOnDurableSkip
                           /\ (~FixGatesOnLocality \/ d \in serverLocality)
               IN /\ stableQueue' = IF feed THEN stableQueue \cup {d}
                                            ELSE stableQueue
                  /\ markedViaSkip' = IF feed THEN markedViaSkip \cup {d}
                                              ELSE markedViaSkip
            /\ UNCHANGED serverDurable
       ELSE \* flows through FSS::update -> pusher fires + becomes durable
            /\ serverDurable' = serverDurable \cup {d}
            /\ stableQueue' = stableQueue \cup {d}
            /\ UNCHANGED markedViaSkip
    /\ UNCHANGED <<workerHas, workerPins, serverLocality, pastBisDone,
                   pendingActions, advertDelta, bisInFlight>>

----------------------------------------------------------------------------
(* ACTION (5) ServerProcessDelta(d): the existing has_durably-gated,
   DELTA-BASED mark_stable re-drive that runs on each BlobsAvailable tick
   (worker_api_server.rs:3569-3583). It can only act on a digest that carries
   a FRESH holdings delta (d IN advertDelta). The leaking-minority digests
   emit NO fresh delta, so they are NEVER in advertDelta and this re-drive
   CANNOT heal them — the reason the bug does not self-heal while connected.
   The delta is consumed ONE-SHOT: if D is not durable at process time, the
   poll MISSES and the delta is gone (advertise-before-durable). *)
----------------------------------------------------------------------------
ServerProcessDelta(d) ==
    /\ d \in advertDelta
    /\ advertDelta' = advertDelta \ {d}
    /\ IF d \in serverDurable                       \* has_durably gate
       THEN stableQueue' = stableQueue \cup {d}
       ELSE stableQueue' = stableQueue              \* poll miss (one-shot)
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, serverLocality,
                   pastBisDone, pendingActions, pendingUpload, bisInFlight,
                   markedViaSkip>>

----------------------------------------------------------------------------
(* ACTION (6a) DrainAndBroadcast: the BIS drain-then-fire loop
   (src/bin/nativelink.rs:1191-1236) drains stable_digests into the GLOBAL BIS
   broadcast. *)
----------------------------------------------------------------------------
DrainAndBroadcast ==
    /\ stableQueue # {}
    /\ bisInFlight' = bisInFlight \cup stableQueue
    /\ stableQueue' = {}
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, serverLocality,
                   pastBisDone, pendingActions, pendingUpload, advertDelta,
                   markedViaSkip>>

----------------------------------------------------------------------------
(* ACTION (6b) BisDeliverToWorker(d): the worker receives a BIS frame and
   RELEASES the indefinite pin. D stays resident (unpinned). Idempotent. *)
----------------------------------------------------------------------------
BisDeliverToWorker(d) ==
    /\ d \in bisInFlight
    /\ bisInFlight' = bisInFlight \ {d}
    /\ workerPins' = workerPins \ {d}
    /\ UNCHANGED <<workerHas, serverDurable, serverLocality, pastBisDone,
                   pendingActions, pendingUpload, advertDelta, stableQueue,
                   markedViaSkip>>

----------------------------------------------------------------------------
(* LocalityEvicted(d): locality CHURN — serverLocality is NOT monotonic (moka
   eviction / stale-entry cleanup un-registers a holding). Modeled to prove
   BLOCK-2b is addressed: the real fix (~FixGatesOnLocality) never reads
   locality, so this action cannot re-open the leak; the naive locality-gated
   fix (FixGatesOnLocality) CAN miss because of it. *)
----------------------------------------------------------------------------
LocalityEvicted(d) ==
    /\ d \in serverLocality
    /\ serverLocality' = serverLocality \ {d}
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, pastBisDone,
                   pendingActions, pendingUpload, advertDelta, stableQueue,
                   bisInFlight, markedViaSkip>>

----------------------------------------------------------------------------
Next ==
    \/ \E d \in Digests : ServerBecomesDurable(d)
    \/ \E d \in Digests : PastBisBroadcast(d)
    \/ \E d \in Digests : WorkerProduceAndPin(d, TRUE)
    \/ \E d \in Digests : WorkerProduceAndPin(d, FALSE)
    \/ \E d \in Digests : WorkerUploadEcsDecision(d)
    \/ \E d \in Digests : ServerProcessDelta(d)
    \/ DrainAndBroadcast
    \/ \E d \in Digests : BisDeliverToWorker(d)
    \/ \E d \in Digests : LocalityEvicted(d)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* INVARIANTS *)
----------------------------------------------------------------------------

TypeOK ==
    /\ workerHas \subseteq Digests
    /\ workerPins \subseteq workerHas
    /\ serverDurable \subseteq Digests
    /\ serverLocality \subseteq Digests
    /\ pastBisDone \subseteq serverDurable
    /\ pendingActions \subseteq Digests
    /\ pendingUpload \subseteq Digests
    /\ advertDelta \subseteq Digests
    /\ stableQueue \subseteq Digests
    /\ bisInFlight \subseteq Digests
    /\ markedViaSkip \subseteq Digests

(* Quiescent: every unit of work is done and every queue is drained. Locality
   churn (LocalityEvicted) may still fire; the invariant is evaluated across
   those churned quiescent states too, which is the robustness proof. *)
Quiescent ==
    /\ pendingActions = {}
    /\ pendingUpload = {}
    /\ advertDelta = {}
    /\ stableQueue = {}
    /\ bisInFlight = {}

(* SAFETY — the >=2-replica gate, holds in ALL regimes (the fix does NOT
   weaken durability): nothing is ever fed to mark_stable / BIS unless the
   server is already durable for it. If this failed, the fix would be
   discarding the only durable copy. Green in both Bugged and Fixed proves the
   fix is LIVENESS-ONLY. *)
StableFeedIsDurable ==
    /\ stableQueue \subseteq serverDurable
    /\ bisInFlight \subseteq serverDurable

(* THE FL-688 INVARIANT (safety-at-quiescence, durability-gated): once every
   queue settles, no digest the server holds DURABLY and the worker still has
   is left pinned. The durability antecedent (d IN serverDurable) is
   load-bearing: a not-yet-durable held pin is CORRECTLY held (only copy;
   releasing would violate >=2-replica) and must NOT count as a violation.

   Bugged: WorkerUploadEcsDecision durable-skip feeds nothing -> a quiescent
   state with D durable + still pinned -> VIOLATED (the orphaned pin).
   Fixed:  the skip feeds mark_stable -> global BIS -> release -> HOLDS. *)
DurableHeldReleasedAtQuiescence ==
    Quiescent =>
        \A d \in Digests :
            (d \in serverDurable /\ d \in workerHas) => d \notin workerPins

(* NON-VACUITY WITNESS (run as an INVARIANT expected to be VIOLATED in the
   Witness cfg): asserts the durable-skip FIX path never drives a real release
   to completion. If TLC VIOLATES it, the counterexample IS the proof that a
   digest fed via the durable-skip fix genuinely reaches a released, quiescent
   state — i.e. the Fixed "HOLDS" above is not vacuous. Expected: VIOLATED. *)
SkipReleaseUnreachable ==
    ~ ( /\ Quiescent
        /\ \E d \in markedViaSkip :
              /\ d \in serverDurable
              /\ d \in workerHas
              /\ d \notin workerPins )

============================================================================
