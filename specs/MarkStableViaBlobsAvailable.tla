------------------- MODULE MarkStableViaBlobsAvailable -------------------
(***************************************************************************
  BlobsAvailable -> mark_stable pin/release pairing protocol (task #140).

  Models the cross-component contract introduced by #140 (which DELETED
  the pre-existing `register_action_result_digests` mechanism — see
  audit.md Path 2 for the lost-eviction race that motivated the deletion):

    * Worker pins every digest it produces or receives
      (running_actions_manager.rs:3725-3746).
    * On every BlobsAvailable tick, the worker sends the digests it
      currently holds (local_worker.rs:1099-1133).
    * Server's `handle_blobs_available` (worker_api_server.rs:864-947)
      runs `cas_store.has_with_results` for the reported digests and
      calls `cas_store.mark_stable(present_subset)`. Per task #140 +
      red-team F2 fix: this branch ALWAYS runs (no longer cooldown-gated).
    * `mark_stable` pushes into FastSlowStore::stable_digests
      (fast_slow_store.rs mark_stable override).
    * BIS broadcast loop (src/bin/nativelink.rs:382-416) drains
      stable_digests and emits BlobsInStableStorage.
    * Worker's BIS handler unpins (local_worker.rs:706-753 -> :717).

  This spec is a SUCCESSOR of PinLifecycle.tla — that spec models the
  pre-#140 world where `register_action_result_digests` was the only
  feeder of stable_digests for already-cached outputs (which never
  fires for cached uploads, leaking pins permanently under PinV2).
  THIS spec models the new world: the BlobsAvailable channel covers
  every pin path, so coverage no longer depends on update_oneshot
  firing.

  THE BUG MODELED HERE is audit Path 2: pre-#140's register_action_result_digests
  racing evicted_digests on the same mpsc::channel(1). The trace:
    1. Worker writes digest D, `BlobChangeTracker::on_insert(D)`.
    2. Cache pressure evicts D before pin_digest can fire — eviction
       listener pushes D into `pending.evicted`.
    3. BlobsAvailable carrying `evicted_digests=[D]` reaches the server
       FIRST (before ExecuteResponse).
    4. `evict_blobs(endpoint, &[D])` runs but locality has nothing for
       D — the call no-ops.
    5. ExecuteResponse arrives. `register_action_result_digests` adds
       D to locality. NOW locality says "endpoint has D" — but the
       worker doesn't.
    6. Locality is permanently stale; downstream readers issue NotFound
       lookups, eviction notifications never refresh, etc.

  CONSTANT FixV140 (TRUE/FALSE) toggles between the old and new regimes:
    * FALSE: pre-#140; register_action_result_digests is the only mark_stable
      feeder for already-cached outputs, and it can race evict_blobs.
    * TRUE: post-#140; BlobsAvailable's has()+mark_stable covers every
      pin advertisement uniformly.

  EXPECTED TLC OUTCOMES:
    * Bugged.cfg (FixV140=FALSE): NoLostEvictionRace WILL be violated.
      Trace: evict reaches server before register; register re-adds
      stale entry; locality permanently wrong.
    * Fixed.cfg (FixV140=TRUE): clean run. Every pin allocation has a
      bounded path to release via the BlobsAvailable+mark_stable+BIS
      chain.

  SCOPE — what this spec models:
    * one worker; the protocol for N workers is the same.
    * a finite set of digest names.
    * worker pin set, server stable_digests queue, locality_map.
    * BlobsAvailable batched message (carries advertised + evicted).
    * pre-#140's separate ExecuteResponse path with race semantics.

  SCOPE — what this spec does NOT model:
    * actual byte stream / BatchUpdateBlobs payload semantics.
    * any TCP / h2 / GOAWAY semantics (upstream of the protocol).
    * Bazel client deduplication.
    * the pinned_mirror_digests bypass-cooldown branch (orthogonal).
    * mark_stable through SizePartitioning/Ref wrappers (modeled
      directly at FastSlowStore terminal here; wrapper coverage is
      verified by integration tests in
      mark_stable_on_blobs_available_test.rs against the production
      composition).
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Digests,         \* Set of digest identifiers.
    FixV140          \* TRUE => post-#140 architecture; FALSE => pre-#140.

ASSUME FixV140 \in BOOLEAN

VARIABLES
    workerHas,             \* Digests the worker has in its CAS cache (pinned or not).
    workerPins,            \* Subset of workerHas that is currently pinned.
    serverCache,           \* Digests the server has stably stored.
    serverLocality,        \* Server's view of "worker holds these digests".
    stableDigestsQueue,    \* mark_stable feeder queue (FastSlowStore).
    bisInFlight,           \* BIS broadcast queue (worker_api -> worker).
    pendingActions,        \* Outputs an action wants to land.
    unsentNotification,    \* Digests the worker still needs to dispatch
                           \* a post-write notification for. Decouples
                           \* "write happened" from "register/advert sent"
                           \* so TLC can interleave a WorkerEvict in the
                           \* race window (audit Path 2).
    \* Pre-#140 only: the mpsc::channel(1) ordering between
    \* ExecuteResponse and BlobsAvailable. Modeled as a queue of
    \* messages from worker to server; in real production the
    \* mpsc::channel(1) capacity-1 makes order non-deterministic
    \* between concurrent senders.
    workerToServer

vars == <<workerHas, workerPins, serverCache, serverLocality, stableDigestsQueue,
          bisInFlight, pendingActions, unsentNotification, workerToServer>>

\* Message tags. We model only the two relevant message kinds.
\* "Evict" carries a single digest the worker just evicted.
\* "Register" carries the action-output digest the server should
\*   register to locality (pre-#140 only — register_action_result_digests).
\* "Advert" carries the digest the worker holds (post-#140 BlobsAvailable
\*   "digests" field).
EvictMsg(d)    == [kind |-> "Evict",    digest |-> d]
RegisterMsg(d) == [kind |-> "Register", digest |-> d]
AdvertMsg(d)   == [kind |-> "Advert",   digest |-> d]

----------------------------------------------------------------------------
Init ==
    /\ workerHas = {}
    /\ workerPins = {}
    /\ serverCache = {}
    /\ serverLocality = {}
    /\ stableDigestsQueue = <<>>
    /\ bisInFlight = <<>>
    /\ pendingActions = Digests
    /\ unsentNotification = {}
    /\ workerToServer = <<>>

----------------------------------------------------------------------------
(* WorkerWriteAndPin(d): worker completes an action whose output is d.
   Worker writes d to the local CAS and pins it. Generates the
   ExecuteResponse (pre-#140) or queues d for the next BlobsAvailable
   advertisement (post-#140). *)
----------------------------------------------------------------------------
\* Worker writes d (action's update_oneshot) and pins it. The write +
\* pin pair happens atomically here; in production it's two adjacent
\* steps with the eviction listener firing in between (audit Path 2),
\* but the model captures the same race window via the separate
\* WorkerSendRegister/WorkerSendAdvert action below: the
\* "send-the-register" step can be DELAYED past a WorkerEvict.
\*
\* Key state addition: `unsentRegister` records "this digest still
\* needs its post-write notification dispatched". For pre-#140 that's
\* the Register msg; for post-#140 it's the Advert msg. Either way, the
\* delay between cache-residency and notification-dispatch is the
\* race window the bug exploits.
WorkerWriteAndPin(d) ==
    /\ d \in pendingActions
    /\ d \notin workerPins
    /\ workerHas' = workerHas \cup {d}
    /\ workerPins' = workerPins \cup {d}
    /\ pendingActions' = pendingActions \ {d}
    /\ serverCache' = serverCache \cup {d}  \* upload completed
    /\ unsentNotification' = unsentNotification \cup {d}
    /\ UNCHANGED <<serverLocality, stableDigestsQueue, bisInFlight,
                   workerToServer>>

\* Worker dispatches the post-write notification for d. Pre-#140 sends
\* a Register; post-#140 sends an Advert. By making this a separate
\* action TLC can interleave a WorkerEvict(d) BEFORE the dispatch —
\* exactly the audit Path 2 race window.
WorkerSendNotification(d) ==
    /\ d \in unsentNotification
    /\ unsentNotification' = unsentNotification \ {d}
    /\ IF FixV140
       THEN
         \* Post-#140: only BlobsAvailable advertises. Critically the
         \* Advert is gated on "worker still has d in cache" — if d
         \* was evicted before this dispatch, the next BlobsAvailable
         \* doesn't include it. So skip if d \notin workerHas.
         IF d \in workerHas
         THEN workerToServer' = Append(workerToServer, AdvertMsg(d))
         ELSE workerToServer' = workerToServer
       ELSE
         \* Pre-#140: ExecuteResponse fires register_action_result_digests
         \* unconditionally (it doesn't check workerHas — that's the bug).
         workerToServer' = Append(workerToServer, RegisterMsg(d))
    /\ UNCHANGED <<workerHas, workerPins, serverCache, serverLocality,
                   stableDigestsQueue, bisInFlight, pendingActions>>

----------------------------------------------------------------------------
(* WorkerEvict(d): worker evicts d under cache pressure (e.g. another
   action's output displaces d). Adds Evict msg to the queue. *)
----------------------------------------------------------------------------
\* Worker evicts d from cache. In production this fires the eviction
\* listener which adds d to BlobChangeTracker.pending.evicted; the next
\* BlobsAvailable carries d in evicted_digests. Pre-#140 modeled this
\* as the EvictMsg below; post-#140 the same EvictMsg piggybacks on the
\* BlobsAvailable channel (same race semantics absent the Register
\* contender). To exercise audit Path 2 we need the eviction to be
\* reportable BEFORE the upload's "Register" arrives — under pin v2,
\* eviction-while-pinned is the cache-pressure case described in
\* running_actions_manager.rs:3717-3721 ("pin_digest: blob not in fast
\* store at pin time"). We allow eviction of an unpinned blob too.
WorkerEvict(d) ==
    /\ d \in workerHas
    /\ workerHas' = workerHas \ {d}
    /\ workerPins' = workerPins \ {d}
    /\ workerToServer' = Append(workerToServer, EvictMsg(d))
    /\ UNCHANGED <<serverCache, serverLocality, stableDigestsQueue,
                   bisInFlight, pendingActions, unsentNotification>>

----------------------------------------------------------------------------
(* ServerProcessMsg: server pops next msg from workerToServer queue
   and processes it. The mpsc::channel(1) capacity-1 + concurrent
   workers means the order is non-deterministic; modeled here as
   "TLC may pick any prefix of msgs to deliver in any order".
   For simplicity we deliver in FIFO order — the bug fires for
   ANY interleaving where Evict precedes Register for the same d. *)
----------------------------------------------------------------------------
ServerProcessMsg ==
    /\ Len(workerToServer) > 0
    /\ LET msg == Head(workerToServer)
       IN
       /\ workerToServer' = Tail(workerToServer)
       /\ CASE msg.kind = "Evict" ->
                   \* Server removes from locality. If not present, no-op
                   \* (which is the bug seed under pre-#140 when the
                   \* register hasn't yet added it).
                   /\ serverLocality' = serverLocality \ {msg.digest}
                   /\ UNCHANGED <<workerHas, workerPins, serverCache,
                                  stableDigestsQueue, bisInFlight,
                                  pendingActions, unsentNotification>>
            [] msg.kind = "Register" ->
                   \* Pre-#140 only. Adds to locality unconditionally
                   \* — that's the bug. Also calls cas_store.mark_stable.
                   /\ serverLocality' = serverLocality \cup {msg.digest}
                   /\ stableDigestsQueue' =
                        IF msg.digest \in serverCache
                        THEN Append(stableDigestsQueue, msg.digest)
                        ELSE stableDigestsQueue
                   /\ UNCHANGED <<workerHas, workerPins, serverCache,
                                  bisInFlight, pendingActions,
                                  unsentNotification>>
            [] msg.kind = "Advert" ->
                   \* Post-#140. Server runs has_with_results; if
                   \* present, mark_stable. Also adds to locality
                   \* (worker_api_server.rs:865-868).
                   /\ serverLocality' = serverLocality \cup {msg.digest}
                   /\ stableDigestsQueue' =
                        IF msg.digest \in serverCache
                        THEN Append(stableDigestsQueue, msg.digest)
                        ELSE stableDigestsQueue
                   /\ UNCHANGED <<workerHas, workerPins, serverCache,
                                  bisInFlight, pendingActions,
                                  unsentNotification>>

----------------------------------------------------------------------------
(* DrainAndBroadcast: BIS loop drains stable_digests, queues for
   broadcast. *)
----------------------------------------------------------------------------
DrainAndBroadcast ==
    /\ Len(stableDigestsQueue) > 0
    /\ bisInFlight' = bisInFlight \o stableDigestsQueue
    /\ stableDigestsQueue' = <<>>
    /\ UNCHANGED <<workerHas, workerPins, serverCache, serverLocality,
                   pendingActions, unsentNotification, workerToServer>>

----------------------------------------------------------------------------
(* BISDeliverToWorker(d): worker receives BIS frame, unpins. *)
----------------------------------------------------------------------------
BISDeliverToWorker(d) ==
    /\ \E i \in 1..Len(bisInFlight) : bisInFlight[i] = d
    /\ \E i \in 1..Len(bisInFlight) :
         /\ bisInFlight[i] = d
         /\ bisInFlight' =
              [j \in 1..(Len(bisInFlight) - 1) |->
                 IF j < i THEN bisInFlight[j] ELSE bisInFlight[j+1]]
    /\ workerPins' = workerPins \ {d}
    \* BIS only releases the pin; the digest stays in workerHas (FS cache).
    /\ UNCHANGED <<workerHas, serverCache, serverLocality, stableDigestsQueue,
                   pendingActions, unsentNotification, workerToServer>>

----------------------------------------------------------------------------
Next ==
    \/ \E d \in Digests : WorkerWriteAndPin(d)
    \/ \E d \in Digests : WorkerSendNotification(d)
    \/ \E d \in Digests : WorkerEvict(d)
    \/ ServerProcessMsg
    \/ DrainAndBroadcast
    \/ \E d \in Digests : BISDeliverToWorker(d)

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A d \in Digests : WF_vars(WorkerSendNotification(d))
    /\ WF_vars(ServerProcessMsg)
    /\ WF_vars(DrainAndBroadcast)
    /\ \A d \in Digests : WF_vars(BISDeliverToWorker(d))

----------------------------------------------------------------------------
(* INVARIANTS *)
----------------------------------------------------------------------------

TypeOK ==
    /\ workerHas \subseteq Digests
    /\ workerPins \subseteq workerHas
    /\ serverCache \subseteq Digests
    /\ serverLocality \subseteq Digests
    /\ pendingActions \subseteq Digests
    /\ \A i \in 1..Len(stableDigestsQueue) : stableDigestsQueue[i] \in Digests
    /\ \A i \in 1..Len(bisInFlight) : bisInFlight[i] \in Digests

(* Quiescent: all action work has been processed and all queues drained.
   In a quiescent state, locality MUST equal workerHas (the worker's
   actual cache contents) — not workerPins, since the BIS unpin path
   leaves the digest in cache (just unpinned) and locality should
   continue to reflect "the worker has D in cache". *)
Quiescent ==
    /\ pendingActions = {}
    /\ unsentNotification = {}
    /\ stableDigestsQueue = <<>>
    /\ bisInFlight = <<>>
    /\ workerToServer = <<>>

(* SAFETY: NoLostEvictionRace
   In a quiescent state, the server's locality MUST agree with the
   worker's actual cache contents (workerHas). A locality entry for a
   digest the worker has evicted (and reported the eviction for) is
   audit Path 2: the eviction reached the server before the registration,
   so eviction's no-op silently dropped, then registration re-added the
   stale entry.

   Under FixV140=TRUE there is no Register action — every advertisement
   comes via Advert which is keyed off "what worker currently holds",
   so when the worker evicts d AFTER advertising, the next advertisement
   tick wouldn't include d AND the EvictMsg subtracts correctly.

   Note: we do NOT assert mid-flight equality (transient mismatches are
   allowed during message-in-flight windows). The invariant is only
   meaningful at quiescence. *)
NoLostEvictionRace ==
    Quiescent => serverLocality = workerHas

(* SAFETY: EveryPinReachesRelease
   At quiescence, the worker holds NO pins. (Equivalent: every alloc
   has been released by some BIS delivery.) Under FixV140=TRUE this
   holds because every pin advertisement triggers has() + mark_stable
   -> BIS unpin. Under FixV140=FALSE the bugged path can still satisfy
   this in the simple cases, but the locality stays wrong (caught by
   NoLostEvictionRace). *)
EveryPinReachesRelease ==
    Quiescent => workerPins = {}

(* LIVENESS: EventuallyConsistentLocality
   Eventually serverLocality matches workerHas. *)
EventuallyConsistentLocality == <>(serverLocality = workerHas)

============================================================================
