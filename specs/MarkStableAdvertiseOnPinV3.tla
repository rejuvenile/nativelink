-------------------- MODULE MarkStableAdvertiseOnPinV3 --------------------
(***************************************************************************
  FL-688 never-BIS-acked durability pin-leak — ADVERTISE-ON-PIN fix (V3).

  Supersedes MarkStableAsyncDurability{,V2}.tla. Those specs modelled the
  ECS-durable-skip `mark_stable` hook (`b86bbbe3`), which the re-cadre
  DROPPED (the worker's <=1 MiB deferred upload routes via BatchUpdateBlobs
  and is acked-and-skipped at `cas_server.rs:744` BEFORE reaching
  ExistenceCacheStore, so the hook closed zero live leaks).

  THE V3 FIX = advertise-on-pin: when the worker takes an indefinite pin on
  an F2 output (`running_actions_manager.rs:7803`) it feeds that digest into
  the SAME `BlobsAvailable` delta the `BlobChangeTracker` feeds
  (`local_worker.rs:2404-2430`); the server's existing has_durably-gated
  `mark_stable` re-drive (`worker_api_server.rs:3569-3609`) then emits a BIS
  for it once durable. The pin path never fires the ItemCallback today, so a
  re-produced already-resident output emits NO delta and goes dark — the
  advertise-on-pin wiring is the ONLY new mechanism.

  ------------------------------------------------------------------------
  THE LOAD-BEARING INVARIANT THIS SPEC EXISTS TO STRESS (Task-1, verified
  end-to-end against current source, disconfirmation-first):

    A FRESH durable landing of D fires a pusher -> GLOBAL BIS that reaches
    and unpins EVERY worker holding an indefinite pin for D, even a worker
    that NEVER advertised D.

  Hop-by-hop, each a code cite whose FALSIFYING observation was checked:
    * FSS fresh-write Ok arm pushes UNCONDITIONALLY on slow-tier success
      (`fast_slow_store.rs:6519` update, `:6823` update_oneshot; `:3529`
      self-retry). Falsifier: a has_durably/locality gate around the push —
      ABSENT; the push is in the raw `Ok(())` arm.
    * drain loop (`src/bin/nativelink.rs:1300-1443`) drains CAS stable
      digests (store_id="") and calls
      `broadcast_blobs_in_stable_storage_chunked(digests, "")` on EVERY
      scheduler.
    * broadcast (`api_worker_scheduler.rs:9240`, snapshot at `:9270`
      `inner.workers.iter()`) fans the chunk to ALL connected workers.
      Falsifier: a `.filter(advertised|locality)` on the worker iteration —
      ABSENT; it is a pure fan-out over every worker.
    * worker receive (`local_worker.rs:3238-3263`, CAS store_id="") calls
      `fs_store.unpin_digest(d)` for EVERY digest in the chunk,
      UNCONDITIONALLY (the `record_bis_unpin` gate touches only the METRIC,
      not the unpin). `unpin_digest` -> `MokaEvictingMap::unpin_key`
      (`moka_evicting_map.rs:1847`) removes from `pinned` REGARDLESS of the
      `indefinite` flag. Falsifier: unpin keyed on "this worker advertised
      d" — ABSENT; keyed only on the worker's own pin set (idempotent
      no-op if not held).

  => VERDICT CONFIRMED: advertise-on-pin need only cover the durable-BEFORE-
  pin case; the durable-AFTER-pin case is FREE (the fresh write's global BIS
  reaches the holder regardless of advertise). This spec proves exactly that
  split.

  ------------------------------------------------------------------------
  ATOMICITY DISCIPLINE (CLAUDE.md). The one FAITHFUL fusion kept is
  "fresh slow-write success => durable AND pusher-push" — in the real code
  both happen in the same `Ok(())` closure with no await between
  (`fast_slow_store.rs:6512-6524`), so they ARE atomic. Everything the leak
  hides behind is de-fused: durability-fact vs advertise vs delivery-ack vs
  the already-durable SKIP (no pusher) are separate indivisible actions with
  the async BIS-delivery gap explicit.

  TWO leak-relevant orderings, distinguished by `durablePreexisting` (the
  set of digests already durable at the instant the worker pinned):
    (a) durable-BEFORE-pin  (d IN durablePreexisting): the worker's later
        upload hits the already-durable SKIP -> NO fresh write -> NO new
        global BIS. ONLY advertise-on-pin can BIS it. Bugged leaks here.
    (b) durable-AFTER-pin   (d NOTIN durablePreexisting): some fresh write
        (own upload while not-durable, or a concurrent external writer)
        fires the global BIS AFTER the pin exists -> covers it advertise-
        or-not. The Witness proves this case releases with advertise OFF.

  Toggle: AdvertiseOnPin  (TRUE=Fixed enables WorkerAdvertiseOnPin; FALSE=Bugged).
 ***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Digests,          \* Set of digest identifiers (2 for TLC).
    AdvertiseOnPin    \* TRUE => Fixed (advertise-on-pin wired); FALSE => Bugged.

ASSUME AdvertiseOnPin \in BOOLEAN

VARIABLES
    workerHas,          \* Digests resident in the worker FS cache.
    workerPins,         \* Subset of workerHas held by an INDEFINITE pin
                        \*   (released ONLY by a BIS ack — TTL-exempt).
    serverDurable,      \* Digests the server holds DURABLY (has_durably TRUE,
                        \*   slow-tier / >=2-replica). Independent async fact.
    durablePreexisting, \* Digests already durable AT the instant the worker
                        \*   pinned them (the durable-BEFORE-pin set). Monotonic
                        \*   marker; classifies orderings (a) vs (b).
    pendingActions,     \* Outputs the worker still needs to produce+pin.
    pendingUpload,      \* Digests the worker owes a deferred upload.
    advertPinned,       \* Digests with a PENDING advertise-on-pin delta
                        \*   awaiting the server's has_durably-gated re-drive.
    advertisedDone,     \* Fire-once dedup (design F1): a digest advertises at
                        \*   pin AT MOST ONCE. Monotonic.
    stableQueue,        \* mark_stable / pusher feeder set (dedups; BIS-idem).
    bisInFlight,        \* GLOBAL BIS frames en route to the worker.
    markedViaAdvert     \* Witness: digests fed to mark_stable BY the advertise-
                        \*   on-pin path (never removed). Non-vacuity tracking.

vars == <<workerHas, workerPins, serverDurable, durablePreexisting,
          pendingActions, pendingUpload, advertPinned, advertisedDone,
          stableQueue, bisInFlight, markedViaAdvert>>

----------------------------------------------------------------------------
Init ==
    /\ workerHas = {}
    /\ workerPins = {}
    /\ serverDurable = {}
    /\ durablePreexisting = {}
    /\ pendingActions = Digests
    /\ pendingUpload = {}
    /\ advertPinned = {}
    /\ advertisedDone = {}
    /\ stableQueue = {}
    /\ bisInFlight = {}
    /\ markedViaAdvert = {}

----------------------------------------------------------------------------
(* ServerBecomesDurableExternal(d): a CONCURRENT external fresh write (another
   worker / client) makes D durable-for-the-FIRST-time. FAITHFUL fusion: the
   fresh slow-write success arm sets durable AND pushes the pusher atomically
   (`fast_slow_store.rs:6512-6524`). Enqueues the GLOBAL BIS. May fire before
   the worker pins (=> sets up the durable-BEFORE-pin ordering, its BIS drains
   harmlessly while the worker doesn't hold D) or after (=> covers the
   durable-AFTER-pin ordering). Disabled once d is durable (no second fresh
   write for an already-durable digest — that path is the SKIP below). *)
----------------------------------------------------------------------------
ServerBecomesDurableExternal(d) ==
    /\ d \notin serverDurable
    /\ serverDurable' = serverDurable \cup {d}
    /\ stableQueue' = stableQueue \cup {d}          \* pusher fires -> global BIS
    /\ UNCHANGED <<workerHas, workerPins, durablePreexisting, pendingActions,
                   pendingUpload, advertPinned, advertisedDone, bisInFlight,
                   markedViaAdvert>>

----------------------------------------------------------------------------
(* WorkerProduceAndPin(d): the worker completes an F2 action, writes D to its
   own FS cache, and takes an INDEFINITE pin (held until a BIS ack;
   `running_actions_manager.rs:7803`). Owes a deferred upload. Records whether
   D was ALREADY durable at pin time (durablePreexisting) to classify the
   ordering. NOTE: the worker's own FSS is BYPASSED (it writes direct to the
   server-facing GrpcStore slow tier), so pinning does NOT itself make D
   durable and fires NO pusher. *)
----------------------------------------------------------------------------
WorkerProduceAndPin(d) ==
    /\ d \in pendingActions
    /\ workerHas' = workerHas \cup {d}
    /\ workerPins' = workerPins \cup {d}
    /\ pendingActions' = pendingActions \ {d}
    /\ pendingUpload' = pendingUpload \cup {d}
    /\ durablePreexisting' = IF d \in serverDurable
                             THEN durablePreexisting \cup {d}
                             ELSE durablePreexisting
    /\ UNCHANGED <<serverDurable, advertPinned, advertisedDone, stableQueue,
                   bisInFlight, markedViaAdvert>>

----------------------------------------------------------------------------
(* WorkerAdvertiseOnPin(d): THE NEW WIRING (Fixed only). Feeds the pinned
   digest into the BlobsAvailable delta. Fire-once (advertisedDone dedup,
   design F1 — a measured cap on the pin path). Present iff AdvertiseOnPin. *)
----------------------------------------------------------------------------
WorkerAdvertiseOnPin(d) ==
    /\ AdvertiseOnPin
    /\ d \in workerPins
    /\ d \notin advertisedDone
    /\ advertPinned' = advertPinned \cup {d}
    /\ advertisedDone' = advertisedDone \cup {d}
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, durablePreexisting,
                   pendingActions, pendingUpload, stableQueue, bisInFlight,
                   markedViaAdvert>>

----------------------------------------------------------------------------
(* WorkerUpload(d): the worker's deferred upload reaches the server CAS chain.
     * d IN serverDurable  -> already-durable SKIP (BatchUpdateBlobs
       `cas_server.rs:744` / ECS durable-skip): acked WITHOUT a fresh write,
       pusher BYPASSED. THE LEAK SITE for the durable-BEFORE-pin ordering.
     * d NOTIN serverDurable -> the write FLOWS THROUGH FSS::update: fresh
       slow-write pusher fires AND D becomes durable (faithful fusion). Covers
       the durable-AFTER-pin ordering via the worker's OWN upload. *)
----------------------------------------------------------------------------
WorkerUpload(d) ==
    /\ d \in pendingUpload
    /\ pendingUpload' = pendingUpload \ {d}
    /\ IF d \in serverDurable
       THEN \* already-durable skip: NO pusher (the orphan setup)
            /\ UNCHANGED <<serverDurable, stableQueue>>
       ELSE \* fresh write: pusher fires + becomes durable
            /\ serverDurable' = serverDurable \cup {d}
            /\ stableQueue' = stableQueue \cup {d}
    /\ UNCHANGED <<workerHas, workerPins, durablePreexisting, pendingActions,
                   advertPinned, advertisedDone, bisInFlight, markedViaAdvert>>

----------------------------------------------------------------------------
(* ServerProcessAdvert(d): the existing has_durably-gated mark_stable re-drive
   on a BlobsAvailable tick (`worker_api_server.rs:3569-3609`). Consumes the
   delta ONE-SHOT:
     * d IN serverDurable -> feeds mark_stable (stableQueue) -> global BIS.
     * d NOTIN serverDurable -> poll MISS; the delta is gone (advertise-
       before-durable). For the AFTER-pin ordering this miss is HARMLESS: the
       fresh write's global BIS still covers it. *)
----------------------------------------------------------------------------
ServerProcessAdvert(d) ==
    /\ d \in advertPinned
    /\ advertPinned' = advertPinned \ {d}
    /\ IF d \in serverDurable                        \* has_durably gate
       THEN /\ stableQueue' = stableQueue \cup {d}
            /\ markedViaAdvert' = markedViaAdvert \cup {d}
       ELSE /\ UNCHANGED <<stableQueue, markedViaAdvert>>  \* one-shot poll miss
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, durablePreexisting,
                   pendingActions, pendingUpload, advertisedDone, bisInFlight>>

----------------------------------------------------------------------------
(* DrainAndBroadcast: the BIS drain-then-fire loop
   (`src/bin/nativelink.rs:1300-1443`) drains stable_digests into the GLOBAL
   BIS broadcast. *)
----------------------------------------------------------------------------
DrainAndBroadcast ==
    /\ stableQueue # {}
    /\ bisInFlight' = bisInFlight \cup stableQueue
    /\ stableQueue' = {}
    /\ UNCHANGED <<workerHas, workerPins, serverDurable, durablePreexisting,
                   pendingActions, pendingUpload, advertPinned, advertisedDone,
                   markedViaAdvert>>

----------------------------------------------------------------------------
(* BisDeliverToWorker(d): the worker receives a global BIS frame and RELEASES
   the indefinite pin (`local_worker.rs:3243` unpin_digest, unconditional for
   every digest in the chunk). D stays resident. Idempotent — a no-op if the
   worker does not hold d. *)
----------------------------------------------------------------------------
BisDeliverToWorker(d) ==
    /\ d \in bisInFlight
    /\ bisInFlight' = bisInFlight \ {d}
    /\ workerPins' = workerPins \ {d}
    /\ UNCHANGED <<workerHas, serverDurable, durablePreexisting, pendingActions,
                   pendingUpload, advertPinned, advertisedDone, stableQueue,
                   markedViaAdvert>>

----------------------------------------------------------------------------
Next ==
    \/ \E d \in Digests : ServerBecomesDurableExternal(d)
    \/ \E d \in Digests : WorkerProduceAndPin(d)
    \/ \E d \in Digests : WorkerAdvertiseOnPin(d)
    \/ \E d \in Digests : WorkerUpload(d)
    \/ \E d \in Digests : ServerProcessAdvert(d)
    \/ DrainAndBroadcast
    \/ \E d \in Digests : BisDeliverToWorker(d)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* INVARIANTS *)
----------------------------------------------------------------------------

TypeOK ==
    /\ workerHas \subseteq Digests
    /\ workerPins \subseteq workerHas
    /\ serverDurable \subseteq Digests
    /\ durablePreexisting \subseteq serverDurable
    /\ pendingActions \subseteq Digests
    /\ pendingUpload \subseteq Digests
    /\ advertPinned \subseteq Digests
    /\ advertisedDone \subseteq Digests
    /\ stableQueue \subseteq Digests
    /\ bisInFlight \subseteq Digests
    /\ markedViaAdvert \subseteq Digests

(* NoAdvertisePending: in Fixed, a pinned-but-not-yet-advertised digest is
   NOT settled (WorkerAdvertiseOnPin is still enabled). Fold it into Quiescent
   so we never declare false quiescence while an advertise is owed. *)
NoAdvertisePending ==
    \/ ~AdvertiseOnPin
    \/ \A d \in workerPins : d \in advertisedDone

Quiescent ==
    /\ pendingActions = {}
    /\ pendingUpload = {}
    /\ advertPinned = {}
    /\ stableQueue = {}
    /\ bisInFlight = {}
    /\ NoAdvertisePending

(* SAFETY — the >=2-replica gate, holds in ALL regimes (the fix is LIVENESS-
   ONLY, never weakens durability): nothing is fed to mark_stable / BIS unless
   the server is already durable for it. *)
StableFeedIsDurable ==
    /\ stableQueue \subseteq serverDurable
    /\ bisInFlight \subseteq serverDurable

(* THE FL-688 INVARIANT (safety-at-quiescence, durability-gated): once every
   queue settles, no digest the server holds DURABLY and the worker still has
   is left pinned. The durability antecedent is load-bearing (a not-yet-durable
   held pin is CORRECTLY held — only copy).
     Bugged (AdvertiseOnPin=FALSE): a durable-BEFORE-pin digest is uploaded via
       the already-durable SKIP (no pusher) and never advertised -> quiescent
       with D durable + still pinned -> VIOLATED (the FL-688 orphan).
     Fixed  (AdvertiseOnPin=TRUE): advertise-on-pin drives has_durably ->
       mark_stable -> global BIS for the BEFORE-pin case; the fresh write's
       global BIS covers the AFTER-pin case -> HOLDS. *)
DurableHeldReleasedAtQuiescence ==
    Quiescent =>
        \A d \in Digests :
            (d \in serverDurable /\ d \in workerHas) => d \notin workerPins

(* WITNESS (run as an INVARIANT expected to be VIOLATED, AdvertiseOnPin=FALSE):
   asserts that NO durable-AFTER-pin digest ever reaches a released, quiescent
   state. TLC VIOLATES it -> the counterexample is a durable-AFTER-pin digest
   (d NOTIN durablePreexisting) that is durable, resident, and UNPINNED at
   quiescence WITH ADVERTISE OFF -> its release came from the fresh write's
   GLOBAL BIS, not from advertise. This is the crux of the Task-1 invariant:
   the after-pin case is advertise-INDEPENDENT. Expected: VIOLATED. *)
AfterPinReleaseUnreachable ==
    ~ ( /\ Quiescent
        /\ \E d \in Digests :
              /\ d \in serverDurable
              /\ d \in workerHas
              /\ d \notin durablePreexisting     \* durable-AFTER-pin
              /\ d \notin workerPins )           \* released

(* NON-VACUITY for Fixed (run as INVARIANT expected VIOLATED, AdvertiseOnPin=
   TRUE): proves the advertise-on-pin path genuinely drives a durable-BEFORE-
   pin digest to a released, quiescent state (so Fixed's "HOLDS" is not
   vacuous — the mechanism actually fires). Expected: VIOLATED. *)
AdvertReleaseUnreachable ==
    ~ ( /\ Quiescent
        /\ \E d \in Digests :
              /\ d \in markedViaAdvert           \* fed to mark_stable via advert
              /\ d \in durablePreexisting         \* the BEFORE-pin (leaking) class
              /\ d \in workerHas
              /\ d \notin workerPins )           \* released

============================================================================
