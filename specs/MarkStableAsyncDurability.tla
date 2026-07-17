------------------- MODULE MarkStableAsyncDurability -------------------
(***************************************************************************
  FL-688 never-BIS-acked durability pin-leak (design deliverable, 2026-07-17).

  SUCCESSOR of MarkStableViaBlobsAvailable.tla, which FUSED the async
  atoms that hid this bug: that spec set `serverCache' = serverCache \cup
  {d}` at WRITE time (MarkStableViaBlobsAvailable.tla:156), so the state
  "worker advertised d BEFORE the server is durable for d" was UNREACHABLE,
  `EveryPinReachesRelease` passed VACUOUSLY, and the FL-688-v3-StageA removal
  of the periodic re-advertise timer (local_worker.rs:4451) left that spec
  GREEN while the pin-leak shipped and pinned outputs 6.7h fleet-wide.

  Per CLAUDE.md "Formal Methods (TLA+) — atomicity discipline", this spec
  SPLITS the write / server-durability / advertisement / delivery-ack /
  short-circuit-durability / BIS-broadcast / unpin into SEVEN indivisible
  actions with the intermediate state explicit, so TLC can interleave the
  delay that opens the leak.

  ------------------------------------------------------------------------
  CODE-PROVEN ROOT CAUSE (each cite verified against current source):

   * A worker pins every F2 output INDEFINITELY, held until a BIS ack
     (running_actions_manager.rs:7790-7810,
     `pin_digest_indefinite_with_result`; the indefinite/TTL-exempt pin
     class is moka_evicting_map.rs `PinInfo.indefinite`, ~:122-129).

   * The PRIMARY BIS path is correctly EVENT-DRIVEN: a fresh
     `FastSlowStore::update` completion invokes the pusher
     (`push_stable_digests_via_arcs`, fast_slow_store.rs:1490-1502) ->
     `stable_notify` -> the drain-then-fire BIS loop
     (src/bin/nativelink.rs:1191-1236) ->
     broadcast_blobs_in_stable_storage_chunked -> worker unpin.

   * THE LEAK is the ONE durability class that BYPASSES the pusher: the
     `AlreadyExists` short-circuit (running_actions_manager.rs:8284-8302).
     Its own comment: "AlreadyExists means the slow tier short-circuits
     FastSlowStore::update WITHOUT invoking stable_digests_pusher -> no BIS
     chunk will be broadcast for this digest from THIS write." So a
     deferred upload of output D that finds D already durable drives NO
     durability event.

   * The only OTHER trigger is the one-shot `has_durably` poll at the
     BlobsAvailable advertisement (worker_api_server.rs:3560-3609). It runs
     `has_durably` at handling time and, if durable, `mark_stable`. It is
     NOT re-driven: the periodic re-advertise timer was removed in FL-688
     v3 Stage A (local_worker.rs:4440-4459). So if the advert is handled
     BEFORE the server is durable, the poll misses and nothing re-checks.

   * Result: advertise-before-durable + AlreadyExists-durability => the
     indefinite pin is stuck forever (live: 6.7h, all-durable digests).

  THE FIX (operator-confirmed, event-driven; encoded as EventDrivenOnShortCircuit=TRUE):
   The durability EVENT (a blob becoming / being confirmed durable — which
   AlreadyExists PROVES) checks the locality map and, if the blob is still
   held, drives the SAME `has_durably`-gated mark_stable/BIS the fresh path
   drives. Applied to ALL pusher-bypassing durability short-circuits.
   Release stays gated on `has_durably` (NOT on AlreadyExists alone), so the
   >=2-replica durability invariant is preserved. Event-driven; NO TTL /
   pin-expiry (forbidden by CLAUDE.md + the operator).

  ------------------------------------------------------------------------
  CONSTANT EventDrivenOnShortCircuit (TRUE/FALSE):
    * FALSE (Bugged.cfg): the short-circuit durable-write reaches
      server-durable WITHOUT emitting any durability event -> the stuck
      pin persists -> PinReleasedAtQuiescence VIOLATED.
    * TRUE (Fixed.cfg): the short-circuit durability event drives the
      locality-checked, has_durably-gated BIS -> every held pin releases.

  SCOPE — modeled: one worker; a finite digest set; the worker indefinite
    pin set; server durable set; server locality view; the one-shot
    advertisement + its delivery-ack; two durability paths (fresh-write
    pusher vs. AlreadyExists short-circuit); the mark_stable feeder queue;
    the BIS broadcast queue + unpin.
  SCOPE — NOT modeled (orthogonal / covered elsewhere): eviction races
    (indefinite pins are not evicted; the pre-#140 evict/register race is
    MarkStableViaBlobsAvailable.tla + audit Path 2); the reconnect
    full-snapshot re-advertise (a SEPARATE event-driven convergence path —
    the leak here exists ABSENT a reconnect); byte-stream payload, h2/GOAWAY,
    Bazel client dedup; wrapper routing (SizePartitioning/ECS/Ref) verified
    by integration tests against the production composition.
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Digests,                      \* Set of digest identifiers (2-3 for TLC).
    EventDrivenOnShortCircuit     \* TRUE => fixed; FALSE => bugged.

ASSUME EventDrivenOnShortCircuit \in BOOLEAN

VARIABLES
    workerHas,             \* Digests resident in the worker FS cache.
    workerPins,            \* Subset of workerHas held by an INDEFINITE pin
                           \*   (released only by a BIS ack).
    serverDurable,         \* Digests the server holds DURABLY (has_durably
                           \*   TRUE — survives restart; the >=2-replica /srv/bulk
                           \*   copy). DISTINCT from mere RAM presence.
    serverLocality,        \* Server's view of "worker holds these digests"
                           \*   (populated when an advert is handled).
    unsentAdvert,          \* Digests the worker still owes a ONE-SHOT
                           \*   BlobsAvailable advertisement for.
    advertInFlight,        \* Advert messages sent, not yet handled by server.
    stableDigestsQueue,    \* mark_stable / pusher feeder (FastSlowStore
                           \*   stable_digests + stable_notify).
    bisInFlight,           \* BIS broadcast queue (server -> worker).
    pendingActions         \* Outputs an action still needs to produce.

vars == <<workerHas, workerPins, serverDurable, serverLocality, unsentAdvert,
          advertInFlight, stableDigestsQueue, bisInFlight, pendingActions>>

----------------------------------------------------------------------------
Init ==
    /\ workerHas = {}
    /\ workerPins = {}
    /\ serverDurable = {}
    /\ serverLocality = {}
    /\ unsentAdvert = {}
    /\ advertInFlight = <<>>
    /\ stableDigestsQueue = <<>>
    /\ bisInFlight = <<>>
    /\ pendingActions = Digests

----------------------------------------------------------------------------
(* ACTION 1 — WorkerWriteAndPin(d): the worker completes an action whose
   output is d. It writes d to the local FS cache and takes an INDEFINITE
   pin (held until a BIS ack). This adds d to the WORKER only; it does NOT
   make the server durable — that is a later, independent async event
   (ACTIONs 5/6). It queues d for a one-shot advertisement. *)
----------------------------------------------------------------------------
WorkerWriteAndPin(d) ==
    /\ d \in pendingActions
    /\ workerHas' = workerHas \cup {d}
    /\ workerPins' = workerPins \cup {d}
    /\ unsentAdvert' = unsentAdvert \cup {d}
    /\ pendingActions' = pendingActions \ {d}
    /\ UNCHANGED <<serverDurable, serverLocality, advertInFlight,
                   stableDigestsQueue, bisInFlight>>

----------------------------------------------------------------------------
(* ACTION 2 — WorkerAdvertise(d): the worker sends its ONE-SHOT
   BlobsAvailable advertisement for a currently-held digest. One-shot: d
   leaves unsentAdvert and is NEVER re-queued (the periodic re-advertise
   timer was removed, local_worker.rs:4440-4459). *)
----------------------------------------------------------------------------
WorkerAdvertise(d) ==
    /\ d \in unsentAdvert
    /\ d \in workerHas
    /\ unsentAdvert' = unsentAdvert \ {d}
    /\ advertInFlight' = Append(advertInFlight, d)
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, serverLocality,
                   stableDigestsQueue, bisInFlight, pendingActions>>

----------------------------------------------------------------------------
(* ACTION 3 — ServerHandleAdvert: the DELIVERY-ack that DRAINS the
   advertisement (distinct from durability). The server registers locality
   and runs the one-shot `has_durably` poll (worker_api_server.rs:3560-3609):
   if durable AT THIS INSTANT it feeds mark_stable; otherwise nothing — and
   because the advert is one-shot, it is never re-polled. *)
----------------------------------------------------------------------------
ServerHandleAdvert ==
    /\ Len(advertInFlight) > 0
    /\ LET d == Head(advertInFlight) IN
        /\ advertInFlight' = Tail(advertInFlight)
        /\ serverLocality' = serverLocality \cup {d}
        /\ stableDigestsQueue' =
             IF d \in serverDurable                    \* has_durably poll
             THEN Append(stableDigestsQueue, d)
             ELSE stableDigestsQueue
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, unsentAdvert,
                   bisInFlight, pendingActions>>

----------------------------------------------------------------------------
(* ACTION 4 — ServerBecomesDurableFreshWrite(d): the server's fast->slow
   async write for a FRESH `FastSlowStore::update` LANDS on /srv/bulk. In
   production the completion closure invokes the pusher
   (push_stable_digests_via_arcs, fast_slow_store.rs:1490-1502) — so this
   path drives BIS in BOTH regimes. This is the CORRECT event-driven path;
   it is not the leak. Independent async action: it may fire before OR after
   the advert. *)
----------------------------------------------------------------------------
ServerBecomesDurableFreshWrite(d) ==
    /\ d \in workerHas                 \* something to become durable for
    /\ d \notin serverDurable          \* durable-once
    /\ serverDurable' = serverDurable \cup {d}
    /\ stableDigestsQueue' = Append(stableDigestsQueue, d)   \* pusher fires
    /\ UNCHANGED <<workerHas, workerPins, serverLocality, unsentAdvert,
                   advertInFlight, bisInFlight, pendingActions>>

----------------------------------------------------------------------------
(* ACTION 5 — ServerBecomesDurableShortCircuit(d): the `AlreadyExists`
   path (running_actions_manager.rs:8284-8302). The blob reaches
   server-durable WITHOUT invoking the pusher (models: the server already
   held d durably via another action/worker, and this worker's deferred
   upload short-circuits). THIS IS THE LEAK SITE.

     * Bugged (EventDrivenOnShortCircuit=FALSE): sets durable, drives NO
       durability event. If the advert was already handled (before durable),
       nothing ever re-checks -> the pin is stuck.
     * Fixed  (EventDrivenOnShortCircuit=TRUE): the durability event checks
       the locality map and, if d is still held, drives the same
       has_durably-gated mark_stable the fresh path drives. *)
----------------------------------------------------------------------------
ServerBecomesDurableShortCircuit(d) ==
    /\ d \in workerHas
    /\ d \notin serverDurable
    /\ serverDurable' = serverDurable \cup {d}
    /\ stableDigestsQueue' =
         IF EventDrivenOnShortCircuit /\ d \in serverLocality  \* locality-checked
         THEN Append(stableDigestsQueue, d)
         ELSE stableDigestsQueue
    /\ UNCHANGED <<workerHas, workerPins, serverLocality, unsentAdvert,
                   advertInFlight, bisInFlight, pendingActions>>

----------------------------------------------------------------------------
(* ACTION 6 — DrainAndBroadcast: the BIS drain-then-fire loop
   (src/bin/nativelink.rs:1191-1236) drains stable_digests into the BIS
   broadcast queue. *)
----------------------------------------------------------------------------
DrainAndBroadcast ==
    /\ Len(stableDigestsQueue) > 0
    /\ bisInFlight' = bisInFlight \o stableDigestsQueue
    /\ stableDigestsQueue' = <<>>
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, serverLocality,
                   unsentAdvert, advertInFlight, pendingActions>>

----------------------------------------------------------------------------
(* ACTION 7 — BISDeliverToWorker(d): the worker receives a BIS frame and
   RELEASES the indefinite pin. d stays resident in the FS cache (unpinned).
   Idempotent for already-unpinned d. *)
----------------------------------------------------------------------------
BISDeliverToWorker(d) ==
    /\ \E i \in 1..Len(bisInFlight) : bisInFlight[i] = d
    /\ LET i == CHOOSE k \in 1..Len(bisInFlight) : bisInFlight[k] = d IN
        bisInFlight' =
          [j \in 1..(Len(bisInFlight) - 1) |->
             IF j < i THEN bisInFlight[j] ELSE bisInFlight[j+1]]
    /\ workerPins' = workerPins \ {d}
    /\ UNCHANGED <<workerHas, serverDurable, serverLocality, unsentAdvert,
                   advertInFlight, stableDigestsQueue, pendingActions>>

----------------------------------------------------------------------------
Next ==
    \/ \E d \in Digests : WorkerWriteAndPin(d)
    \/ \E d \in Digests : WorkerAdvertise(d)
    \/ ServerHandleAdvert
    \/ \E d \in Digests : ServerBecomesDurableFreshWrite(d)
    \/ \E d \in Digests : ServerBecomesDurableShortCircuit(d)
    \/ DrainAndBroadcast
    \/ \E d \in Digests : BISDeliverToWorker(d)

(* Fairness: every worker output is eventually written; every enabled
   downstream step eventually runs. The durability action is fair too — a
   blob DOES eventually become durable (via one path or the other). We do
   NOT put fairness on ServerBecomesDurableShortCircuit vs FreshWrite
   individually beyond "durability eventually happens": TLC's safety check
   already explores the short-circuit branch; the temporal property below
   needs only that durability + the release machinery are fair. *)
Fairness ==
    /\ \A d \in Digests : WF_vars(WorkerWriteAndPin(d))
    /\ \A d \in Digests : WF_vars(WorkerAdvertise(d))
    /\ WF_vars(ServerHandleAdvert)
    /\ \A d \in Digests : WF_vars(ServerBecomesDurableFreshWrite(d))
    /\ \A d \in Digests : WF_vars(ServerBecomesDurableShortCircuit(d))
    /\ WF_vars(DrainAndBroadcast)
    /\ \A d \in Digests : WF_vars(BISDeliverToWorker(d))

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
(* INVARIANTS / PROPERTIES *)
----------------------------------------------------------------------------

TypeOK ==
    /\ workerHas \subseteq Digests
    /\ workerPins \subseteq workerHas
    /\ serverDurable \subseteq Digests
    /\ serverLocality \subseteq Digests
    /\ unsentAdvert \subseteq Digests
    /\ pendingActions \subseteq Digests
    /\ \A i \in 1..Len(advertInFlight)     : advertInFlight[i] \in Digests
    /\ \A i \in 1..Len(stableDigestsQueue) : stableDigestsQueue[i] \in Digests
    /\ \A i \in 1..Len(bisInFlight)        : bisInFlight[i] \in Digests

(* Quiescent: all action work is done and every queue is drained. *)
Quiescent ==
    /\ pendingActions = {}
    /\ unsentAdvert = {}
    /\ advertInFlight = <<>>
    /\ stableDigestsQueue = <<>>
    /\ bisInFlight = <<>>

(* SAFETY (the >=2-replica gate, holds in BOTH regimes — the fix does NOT
   weaken durability): nothing is ever fed to mark_stable / BIS unless the
   server is already durable for it. Release therefore implies prior
   durability. If this ever failed, the fix would be discarding the only
   durable copy — the exact thing the has_durably gate
   (worker_api_server.rs:3560) prevents. *)
StableFeedIsDurable ==
    /\ \A i \in 1..Len(stableDigestsQueue) : stableDigestsQueue[i] \in serverDurable
    /\ \A i \in 1..Len(bisInFlight)        : bisInFlight[i] \in serverDurable

(* THE FL-688 INVARIANT (safety-at-quiescence): once everything settles, no
   digest that the server holds DURABLY and the worker still has is left
   pinned. This is the precise "BIS-acked <= has_durably" contract. Under
   EventDrivenOnShortCircuit=FALSE the advertise-before-durable +
   AlreadyExists interleaving reaches a quiescent (deadlock) state with a
   durable-but-still-pinned digest -> VIOLATED. This is what the fused-atom
   predecessor spec (serverCache set at WRITE time) proved VACUOUSLY. *)
DurableHeldReleasedAtQuiescence ==
    Quiescent => (\A d \in Digests :
                    (d \in serverDurable /\ d \in workerHas) => d \notin workerPins)

(* DELIBERATELY NOT CHECKED — `Quiescent => workerPins = {}` is TOO STRONG
   for this model: a quiescent state where the server is NOT yet durable for
   a held digest leaves the pin CORRECTLY held (it is the only copy — the
   >=2-replica invariant FORBIDS release). That would be a spurious
   counterexample, not the FL-688 bug. The bug is durable-but-not-acked, so
   the durability antecedent (`d \in serverDurable`) above is load-bearing.
   The temporal EventuallyAllReleased below captures full release under
   fairness (durability WF-eventually happens, then release must follow). *)
PinReleasedAtQuiescence ==
    Quiescent => workerPins = {}

(* LIVENESS: every indefinite pin is eventually released for good. *)
EventuallyAllReleased == <>[](workerPins = {})

============================================================================
