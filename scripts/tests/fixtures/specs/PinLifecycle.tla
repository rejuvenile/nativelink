------------------------------ MODULE PinLifecycle ------------------------------
(***************************************************************************
  Pin / BIS lifecycle protocol for NativeLink.

  Models the resource-pairing contract between:
    * a worker that calls fs_store.pin_digest(d) for every action-output
      digest (running_actions_manager.rs:3725-3746),
    * the server's cas_store (FastSlowStore) whose stable_digests queue
      is fed ONLY by the success arm of update_oneshot
      (fast_slow_store.rs:2289-2290 and :2520-2521),
    * the BIS broadcast loop (src/bin/nativelink.rs:382-416) which drains
      stable_digests and broadcasts BlobsInStableStorage to every worker,
    * the worker's BIS handler (local_worker.rs:706-753) — the ONLY
      callsite of unpin_digest in nativelink-worker/.

  The server short-circuits BatchUpdateBlobs (cas_server.rs:391-407) and
  ByteStream::write (bytestream_server.rs:2058-2072) when the blob is
  already cached: the closure returns OK *without* invoking
  update_oneshot. So no push to stable_digests, no BIS, no unpin.

  Pin v1: a worker pin auto-expires after 120 s (TTL). The leak documented
          above self-heals because the redundant pin times out.
  Pin v2: pins are durable; the ONLY way to release one is via BIS
          (handle_blobs_in_stable_storage). Under v2 the leak is permanent.

  CONSTANT PinV2 (TRUE/FALSE) toggles between the two regimes.

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * PinLifecycleV1.cfg (PinV2 = FALSE): NO invariant violation. Liveness
      property `EventuallyAllPinsReleased` holds via the TTL release path.
    * PinLifecycleV2.cfg (PinV2 = TRUE):  INVARIANT VIOLATED on
      `NoPermanentPinLeak`. TLC's trace shows: worker pins d, server's
      has(d) returns TRUE, server short-circuits without update_oneshot,
      BIS never fires for d, the system reaches a stuttering state
      where d is still in the worker's pin set and no transition can
      clear it. This is the production bug
      (.claude/reviews/bis-coverage-for-already-cached-outputs/audit.md).

  SCOPE — what this spec models:
    * one worker (call it w0); the protocol for N workers is the
      same — adding a second worker would only blow up the state
      space without exposing a new bug class for THIS bug.
    * a finite set of digest names (CONSTANT Digests).
    * server cache state: PrimedDigests (ones the server already has
      before any action). The bug is exposed by an action whose output
      is in PrimedDigests.
    * worker pin set, server stable_digests queue, in-flight broadcast.
    * pin v1 TTL release as a single non-deterministic action
      (TtlRelease) rather than a real clock — this is the standard
      TLA+ idiom for "eventually fires"; with weak fairness on
      TtlRelease we get the v1 self-heal behavior.

  SCOPE — what this spec does NOT model:
    * concurrency BETWEEN multiple workers,
    * actual byte stream / BatchUpdateBlobs payload semantics,
    * any TCP / h2 / GOAWAY semantics,
    * Bazel client deduplication,
    * the failed_slow_writes retry path (orthogonal),
    * multiple BIS broadcasts batched together (modeled as one digest
      per BIS for simplicity; the bug is per-digest),
    * the SizePartitioningStore wrapper-default-no-op bug (orthogonal;
      that's a property of the CODE, not the protocol — model checking
      the protocol with a correctly-overriding wrapper is enough).

  CITATIONS:
    [audit] .claude/reviews/bis-coverage-for-already-cached-outputs/audit.md
    [pin]   nativelink-worker/src/running_actions_manager.rs:3725-3746
    [bus]   nativelink-store/src/fast_slow_store.rs:2289-2290, :2520-2521
    [b1]    nativelink-service/src/cas_server.rs:391-407
    [b2]    nativelink-service/src/bytestream_server.rs:2058-2072
    [reg]   nativelink-service/src/worker_api_server.rs:592-632
    [bis]   src/bin/nativelink.rs:382-416
    [unpin] nativelink-worker/src/local_worker.rs:706-753 (line 717)
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Digests,         \* Set of digest identifiers, e.g. {d1, d2, d3}
    PrimedDigests,   \* Subset of Digests already cached on server at start
    PinV2            \* TRUE => pins are durable; FALSE => v1 TTL self-heal

ASSUME PrimedDigests \subseteq Digests
ASSUME PinV2 \in BOOLEAN

VARIABLES
    workerPins,           \* Set of digests the worker has pinned (no TTL state, see below)
    serverCache,          \* Set of digests the server has stored
    stableDigestsQueue,   \* Sequence of digests in the BIS feeder queue
    bisInFlight,          \* Sequence of digests currently being broadcast
    pendingActionOutputs  \* Set of digests representing "an action wants
                          \* these digests in the worker's pin set". Drained
                          \* by WorkerPinAction; once empty, no new actions
                          \* run.

vars == <<workerPins, serverCache, stableDigestsQueue, bisInFlight, pendingActionOutputs>>

----------------------------------------------------------------------------
(* Init: server starts with PrimedDigests already cached, no pins, no   *)
(* outstanding broadcasts. The worker has a pending action with all    *)
(* digests as outputs (the "deterministic build" worst case in the     *)
(* audit).                                                              *)
----------------------------------------------------------------------------
Init ==
    /\ workerPins = {}
    /\ serverCache = PrimedDigests
    /\ stableDigestsQueue = <<>>
    /\ bisInFlight = <<>>
    /\ pendingActionOutputs = Digests

----------------------------------------------------------------------------
(* WorkerPinAction(d): worker takes a pin on d as part of running an    *)
(* action whose result includes d.                                      *)
(* Models running_actions_manager.rs:3725-3746 -                       *)
(*   filesystem_store.pin_digest_with_result(&file.digest)              *)
(* The action stays in pendingActionOutputs (we don't drain here)       *)
(* because the upload phase is modeled as a separate step.              *)
----------------------------------------------------------------------------
WorkerPinAction(d) ==
    /\ d \in pendingActionOutputs
    /\ d \notin workerPins
    /\ workerPins' = workerPins \cup {d}
    /\ UNCHANGED <<serverCache, stableDigestsQueue, bisInFlight, pendingActionOutputs>>

----------------------------------------------------------------------------
(* WorkerUploadCacheHit(d): worker uploading d realises (via batch        *)
(* has_with_results) that the server already has d, and SKIPS the upload.*)
(* Models running_actions_manager.rs:2049-2078 (known_existing fast path) *)
(* AND server-side cas_server.rs:391-407 / bytestream_server.rs:2058-2072 *)
(* (both routes are equivalent at this granularity: no update_oneshot     *)
(* runs, so stable_digests gets nothing).                                 *)
(*                                                                       *)
(* This is the BUG TRIGGER. The pin remains on the worker, but the      *)
(* server queued NOTHING for BIS, so no unpin will ever follow.          *)
----------------------------------------------------------------------------
WorkerUploadCacheHit(d) ==
    /\ d \in pendingActionOutputs
    /\ d \in workerPins         \* must have pinned first
    /\ d \in serverCache        \* server short-circuits because has(d)
    /\ pendingActionOutputs' = pendingActionOutputs \ {d}
    /\ UNCHANGED <<workerPins, serverCache, stableDigestsQueue, bisInFlight>>

----------------------------------------------------------------------------
(* WorkerUploadFresh(d): worker uploads d (server doesn't have it).     *)
(* update_oneshot runs to completion; the success arm pushes d to       *)
(* stable_digests (fast_slow_store.rs:2289-2290 / :2520-2521).          *)
----------------------------------------------------------------------------
WorkerUploadFresh(d) ==
    /\ d \in pendingActionOutputs
    /\ d \in workerPins
    /\ d \notin serverCache
    /\ serverCache' = serverCache \cup {d}
    /\ stableDigestsQueue' = Append(stableDigestsQueue, d)
    /\ pendingActionOutputs' = pendingActionOutputs \ {d}
    /\ UNCHANGED <<workerPins, bisInFlight>>

----------------------------------------------------------------------------
(* DrainAndBroadcast: BIS loop wakes, drains stable_digests, queues for *)
(* broadcast. Models src/bin/nativelink.rs:382-416. We collapse drain   *)
(* and broadcast into one step; bisInFlight then plays back to workers. *)
(* Bounded: only fires when there's something to drain (line 396-397).  *)
----------------------------------------------------------------------------
DrainAndBroadcast ==
    /\ Len(stableDigestsQueue) > 0
    /\ bisInFlight' = bisInFlight \o stableDigestsQueue
    /\ stableDigestsQueue' = <<>>
    /\ UNCHANGED <<workerPins, serverCache, pendingActionOutputs>>

----------------------------------------------------------------------------
(* BISDeliverToWorker(d): worker receives a BIS frame for d. d is       *)
(* removed from the broadcast queue and unpinned on the worker.         *)
(* Models local_worker.rs:706-753 -> fs_store.unpin_digest(&d) (l. 717).*)
----------------------------------------------------------------------------
BISDeliverToWorker(d) ==
    /\ \E i \in 1..Len(bisInFlight) : bisInFlight[i] = d
    /\ \E i \in 1..Len(bisInFlight) :
         /\ bisInFlight[i] = d
         /\ bisInFlight' =
              [j \in 1..(Len(bisInFlight) - 1) |->
                 IF j < i THEN bisInFlight[j] ELSE bisInFlight[j+1]]
    /\ workerPins' = workerPins \ {d}
    /\ UNCHANGED <<serverCache, stableDigestsQueue, pendingActionOutputs>>

----------------------------------------------------------------------------
(* TtlRelease(d): in v1, every pin has a 120 s TTL after which it       *)
(* unpins itself if no BIS arrived. Modeled as a non-deterministic      *)
(* action that fires only when PinV2 = FALSE.                           *)
(* Under PinV2 = TRUE this action is DISABLED, so the only way to       *)
(* release a pin is via BIS — which exposes the leak.                   *)
----------------------------------------------------------------------------
TtlRelease(d) ==
    /\ ~PinV2
    /\ d \in workerPins
    /\ workerPins' = workerPins \ {d}
    /\ UNCHANGED <<serverCache, stableDigestsQueue, bisInFlight, pendingActionOutputs>>

----------------------------------------------------------------------------
(* Next: union of all actions with appropriate digest quantification.   *)
----------------------------------------------------------------------------
Next ==
    \/ \E d \in Digests : WorkerPinAction(d)
    \/ \E d \in Digests : WorkerUploadCacheHit(d)
    \/ \E d \in Digests : WorkerUploadFresh(d)
    \/ DrainAndBroadcast
    \/ \E d \in Digests : BISDeliverToWorker(d)
    \/ \E d \in Digests : TtlRelease(d)

----------------------------------------------------------------------------
(* Fairness: we want every BIS-able event to eventually fire.           *)
(*                                                                      *)
(* WF on DrainAndBroadcast and BISDeliverToWorker captures the          *)
(* server-side "the loop will always wake and drain" guarantee          *)
(* (modulo the 500 ms timeout in nativelink.rs:386). For v1 we add WF   *)
(* on TtlRelease so pins eventually expire.                             *)
(*                                                                      *)
(* We do NOT add fairness to WorkerUploadCacheHit / WorkerPinAction —   *)
(* the bug is exposed even when the worker stops doing more actions.   *)
----------------------------------------------------------------------------
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(DrainAndBroadcast)
    /\ \A d \in Digests : WF_vars(BISDeliverToWorker(d))
    /\ \A d \in Digests : WF_vars(TtlRelease(d))

----------------------------------------------------------------------------
(* INVARIANTS                                                            *)
----------------------------------------------------------------------------

(* TypeOK: domain bounds for TLC (sanity check, not the bug-finding     *)
(* invariant).                                                           *)
TypeOK ==
    /\ workerPins \subseteq Digests
    /\ serverCache \subseteq Digests
    /\ pendingActionOutputs \subseteq Digests
    /\ \A i \in 1..Len(stableDigestsQueue) : stableDigestsQueue[i] \in Digests
    /\ \A i \in 1..Len(bisInFlight) : bisInFlight[i] \in Digests

(* Quiescent: all action work has been processed and all server-side    *)
(* queues are empty. In a quiescent state, ANY remaining pin is a leak  *)
(* — by definition no further protocol message will arrive to clear it. *)
Quiescent ==
    /\ pendingActionOutputs = {}
    /\ stableDigestsQueue = <<>>
    /\ bisInFlight = <<>>

(* CanMakeProgressOnPins: in the current state, is there ANY enabled    *)
(* protocol action that could eventually clear at least one pin?        *)
(*                                                                      *)
(* Under PinV2 = FALSE, TtlRelease(d) is enabled for any pinned d, so   *)
(* this is TRUE whenever workerPins # {}.                                *)
(*                                                                      *)
(* Under PinV2 = TRUE, the only path to clearing a pin is via BIS,      *)
(* which requires the digest to be in stableDigestsQueue or             *)
(* bisInFlight. In a Quiescent state both are empty AND no fresh        *)
(* upload can re-fire (pendingActionOutputs = {}), so the pin is        *)
(* permanently stuck.                                                    *)
CanMakeProgressOnPins ==
    \/ ~PinV2                              \* TtlRelease can fire on any pinned d
    \/ stableDigestsQueue # <<>>           \* DrainAndBroadcast can fire
    \/ bisInFlight # <<>>                  \* BISDeliverToWorker can fire
    \/ \E d \in pendingActionOutputs :     \* a fresh upload could still push to BIS
        d \notin serverCache

(* SAFETY: NoPermanentPinLeak                                            *)
(* If the worker holds any pin, the protocol must have at least one     *)
(* enabled action that can move the system toward releasing it.         *)
(*                                                                      *)
(* Under PinV2 = TRUE this WILL be violated when an action's output is  *)
(* in PrimedDigests (the cached-output bug):                            *)
(*                                                                      *)
(*   Trace: WorkerPinAction(d1)                                          *)
(*       -> WorkerUploadCacheHit(d1)  // skips update_oneshot           *)
(*       -> drains for d2, d3                                            *)
(*       -> Quiescent with workerPins = {d1}                            *)
(*       -> CanMakeProgressOnPins = FALSE -> invariant violated.        *)
(*                                                                      *)
(* Under PinV2 = FALSE, ~PinV2 makes CanMakeProgressOnPins always TRUE  *)
(* whenever pins exist, so the invariant trivially holds.                *)
NoPermanentPinLeak ==
    workerPins = {} \/ CanMakeProgressOnPins

(* LIVENESS: EventuallyAllPinsReleased                                   *)
(* Regardless of intermediate states, eventually the worker holds no    *)
(* pins. Under WF on TtlRelease (v1) this holds; under PinV2 (TtlRelease *)
(* disabled) it does NOT for digests that triggered cache-hit uploads.  *)
EventuallyAllPinsReleased == <>(workerPins = {})

============================================================================
