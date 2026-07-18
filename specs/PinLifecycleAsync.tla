------------------------------ MODULE PinLifecycleAsync ------------------------------
(***************************************************************************
  DE-ATOMIZED variant of PinLifecycle.tla (atomicity-audit batch A,
  2026-07-17). Splits the fused atom in `WorkerUploadFresh`.

  THE FALSE ATOMICITY IN THE ORIGINAL SPEC
    PinLifecycle.tla `WorkerUploadFresh(d)` did, in ONE atomic step:
        serverCache'        = serverCache \cup {d}      \* fast-tier write OK
        stableDigestsQueue' = Append(stableDigestsQueue, d)   \* push to BIS feeder
    But in PRODUCTION these are separated by an ASYNC slow-tier write and
    its COMPLETION closure. The push to `stable_digests` happens ONLY in
    the slow-write completion arm (fast_slow_store.rs:1822-1854, :3490 —
    "the completion closures DO push to stable_digests"), NOT synchronously
    with the fast-tier write. Between the fast-tier write and the completion
    there is:
        * an async disk WRITE on the slow tier (mirror_blobs),
        * its ACK (or its FAILURE into failed_slow_writes),
        * only on the SUCCESS arm: the push to stable_digests.
    Fusing them made the "fast-written-and-worker-pinned but slow-write
    FAILED so never queued for BIS" state UNREACHABLE. The original spec
    therefore only ever caught the CACHE-HIT leak (WorkerUploadCacheHit),
    never the SLOW-WRITE-FAILURE leak — which is the FL-688 never-BIS-acked
    pin leak (git b3054e00 / ff97841d: "durability-ack re-drive missing").

  WHAT SPLITTING SURFACES
    We split WorkerUploadFresh into:
        WorkerUploadFreshFastOk(d)  -- fast tier accepts; slow write goes
                                       InFlight; NOTHING pushed to BIS yet.
        SlowWriteAck(d)             -- async slow write succeeds; the
                                       completion closure pushes d to
                                       stableDigestsQueue.
        SlowWriteFail(d)            -- async slow write fails; d lands in
                                       failedSlowWrites; NOTHING pushed to
                                       BIS -> the pin has no release path.
        SlowWriteRedrive(d)        -- (FIX) the failed_slow_writes drainer
                                       re-drives the slow write so it can
                                       ack later and finally push to BIS.

    CONSTANT Redrive toggles the compensator:
      * Redrive = FALSE (Bugged): a SlowWriteFail leaves the pin with no
        enabled release action once the system quiesces ->
        NoPermanentPinLeak VIOLATED. This is the NEW bug the fused atom hid.
      * Redrive = TRUE  (Fixed): SlowWriteRedrive is always enabled on a
        failed digest, so there is always an action driving toward release
        -> NoPermanentPinLeak HOLDS (same invariant STYLE the original spec
        used for its v1-TTL self-heal).

    We set PrimedDigests = {} in BOTH cfgs so the CACHE-HIT leak path is
    disabled and the ONLY reachable leak is the slow-write-failure gap.
    (The cache-hit leak is the ORIGINAL PinLifecycleV2.cfg's job; this
    spec isolates the gap the original could not reach.)

  CITATIONS (same code sites as PinLifecycle.tla, plus):
    [push]  nativelink-store/src/fast_slow_store.rs:1822-1854, :3490
            (slow-write completion closures push to stable_digests)
    [fail]  nativelink-store/src/fast_slow_store.rs:2033, :2370
            (in-band slow-write failure inserts into failed_slow_writes)
    [drain] nativelink-worker/src/local_worker.rs:1327-1352
            (drain_failed_digests re-drive on reconnect)
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Digests,         \* Set of digest identifiers, e.g. {d1, d2}
    PrimedDigests,   \* Subset already cached on server at start (set {} here)
    Redrive          \* TRUE => failed slow writes are re-driven (the fix)

ASSUME PrimedDigests \subseteq Digests
ASSUME Redrive \in BOOLEAN

\* Per-digest slow-write lifecycle on the FastSlowStore.
\*   "Idle"     - no slow write outstanding (fresh not yet uploaded, or
\*                a cache-hit short-circuit that never spawned one).
\*   "InFlight" - fast-tier write done, async slow write running.
\*   "Acked"    - slow write succeeded; completion pushed d to stable_digests.
\*   "Failed"   - slow write failed; d in failed_slow_writes; NOT pushed.
SlowStates == {"Idle", "InFlight", "Acked", "Failed"}

VARIABLES
    workerPins,           \* Set of digests the worker has pinned (durable, no TTL)
    serverCache,          \* Set of digests present in the server fast tier
    slowState,            \* [Digests -> SlowStates]
    stableDigestsQueue,   \* Sequence of digests in the BIS feeder queue
    bisInFlight,          \* Sequence of digests currently being broadcast
    pendingActionOutputs  \* Set of digests an action still wants pinned+uploaded

vars == <<workerPins, serverCache, slowState, stableDigestsQueue,
          bisInFlight, pendingActionOutputs>>

----------------------------------------------------------------------------
Init ==
    /\ workerPins = {}
    /\ serverCache = PrimedDigests
    /\ slowState = [d \in Digests |-> "Idle"]
    /\ stableDigestsQueue = <<>>
    /\ bisInFlight = <<>>
    /\ pendingActionOutputs = Digests

----------------------------------------------------------------------------
(* WorkerPinAction(d): worker pins d as part of running its action.        *)
----------------------------------------------------------------------------
WorkerPinAction(d) ==
    /\ d \in pendingActionOutputs
    /\ d \notin workerPins
    /\ workerPins' = workerPins \cup {d}
    /\ UNCHANGED <<serverCache, slowState, stableDigestsQueue, bisInFlight,
                    pendingActionOutputs>>

----------------------------------------------------------------------------
(* WorkerUploadCacheHit(d): server already has d -> short-circuit, no      *)
(* slow write. (Disabled when PrimedDigests = {}.) The classic cache-hit   *)
(* leak, kept for completeness; not the target of THIS spec.               *)
----------------------------------------------------------------------------
WorkerUploadCacheHit(d) ==
    /\ d \in pendingActionOutputs
    /\ d \in workerPins
    /\ d \in serverCache
    /\ pendingActionOutputs' = pendingActionOutputs \ {d}
    /\ UNCHANGED <<workerPins, serverCache, slowState, stableDigestsQueue,
                    bisInFlight>>

----------------------------------------------------------------------------
(* WorkerUploadFreshFastOk(d): fast-tier write succeeds. The async slow    *)
(* write is SPAWNED (InFlight). NOTHING is pushed to stable_digests yet —  *)
(* that only happens in the completion arm (SlowWriteAck). THIS IS THE     *)
(* SPLIT of the original fused WorkerUploadFresh.                          *)
----------------------------------------------------------------------------
WorkerUploadFreshFastOk(d) ==
    /\ d \in pendingActionOutputs
    /\ d \in workerPins
    /\ d \notin serverCache
    /\ slowState[d] = "Idle"
    /\ serverCache' = serverCache \cup {d}
    /\ slowState' = [slowState EXCEPT ![d] = "InFlight"]
    /\ pendingActionOutputs' = pendingActionOutputs \ {d}
    /\ UNCHANGED <<workerPins, stableDigestsQueue, bisInFlight>>

----------------------------------------------------------------------------
(* SlowWriteAck(d): the async slow-tier write completes OK. The completion *)
(* closure pushes d to stable_digests (fast_slow_store.rs:1822-1854).      *)
----------------------------------------------------------------------------
SlowWriteAck(d) ==
    /\ slowState[d] = "InFlight"
    /\ slowState' = [slowState EXCEPT ![d] = "Acked"]
    /\ stableDigestsQueue' = Append(stableDigestsQueue, d)
    /\ UNCHANGED <<workerPins, serverCache, bisInFlight, pendingActionOutputs>>

----------------------------------------------------------------------------
(* SlowWriteFail(d): the async slow-tier write fails. d lands in           *)
(* failed_slow_writes. NOTHING is pushed to stable_digests. In the         *)
(* Bugged (Redrive=FALSE) model this is a terminal leak state for d's pin. *)
----------------------------------------------------------------------------
SlowWriteFail(d) ==
    /\ slowState[d] = "InFlight"
    /\ slowState' = [slowState EXCEPT ![d] = "Failed"]
    /\ UNCHANGED <<workerPins, serverCache, stableDigestsQueue, bisInFlight,
                    pendingActionOutputs>>

----------------------------------------------------------------------------
(* SlowWriteRedrive(d): (FIX) the failed_slow_writes drainer re-spawns the *)
(* slow write for a failed digest, giving it another chance to Ack and     *)
(* finally push to BIS. Only enabled under Redrive = TRUE.                 *)
----------------------------------------------------------------------------
SlowWriteRedrive(d) ==
    /\ Redrive
    /\ slowState[d] = "Failed"
    /\ slowState' = [slowState EXCEPT ![d] = "InFlight"]
    /\ UNCHANGED <<workerPins, serverCache, stableDigestsQueue, bisInFlight,
                    pendingActionOutputs>>

----------------------------------------------------------------------------
(* DrainAndBroadcast: BIS loop drains stable_digests into the broadcast.   *)
----------------------------------------------------------------------------
DrainAndBroadcast ==
    /\ Len(stableDigestsQueue) > 0
    /\ bisInFlight' = bisInFlight \o stableDigestsQueue
    /\ stableDigestsQueue' = <<>>
    /\ UNCHANGED <<workerPins, serverCache, slowState, pendingActionOutputs>>

----------------------------------------------------------------------------
(* BISDeliverToWorker(d): worker receives a BIS frame for d -> unpin.      *)
----------------------------------------------------------------------------
BISDeliverToWorker(d) ==
    /\ \E i \in 1..Len(bisInFlight) : bisInFlight[i] = d
    /\ \E i \in 1..Len(bisInFlight) :
         /\ bisInFlight[i] = d
         /\ bisInFlight' =
              [j \in 1..(Len(bisInFlight) - 1) |->
                 IF j < i THEN bisInFlight[j] ELSE bisInFlight[j+1]]
    /\ workerPins' = workerPins \ {d}
    /\ UNCHANGED <<serverCache, slowState, stableDigestsQueue, pendingActionOutputs>>

----------------------------------------------------------------------------
Next ==
    \/ \E d \in Digests : WorkerPinAction(d)
    \/ \E d \in Digests : WorkerUploadCacheHit(d)
    \/ \E d \in Digests : WorkerUploadFreshFastOk(d)
    \/ \E d \in Digests : SlowWriteAck(d)
    \/ \E d \in Digests : SlowWriteFail(d)
    \/ \E d \in Digests : SlowWriteRedrive(d)
    \/ DrainAndBroadcast
    \/ \E d \in Digests : BISDeliverToWorker(d)

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(DrainAndBroadcast)
    /\ \A d \in Digests : WF_vars(BISDeliverToWorker(d))
    /\ \A d \in Digests : WF_vars(SlowWriteRedrive(d))

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ workerPins \subseteq Digests
    /\ serverCache \subseteq Digests
    /\ slowState \in [Digests -> SlowStates]
    /\ pendingActionOutputs \subseteq Digests
    /\ \A i \in 1..Len(stableDigestsQueue) : stableDigestsQueue[i] \in Digests
    /\ \A i \in 1..Len(bisInFlight) : bisInFlight[i] \in Digests

(* CanMakeProgressOnPins: is there ANY enabled action that can eventually  *)
(* clear at least one pin?                                                 *)
(*   * stable_digests non-empty      -> DrainAndBroadcast can fire         *)
(*   * bisInFlight non-empty         -> BISDeliverToWorker can fire        *)
(*   * a fresh upload still possible -> its Ack will push to BIS           *)
(*   * a slow write is InFlight      -> its Ack will push to BIS           *)
(*   * Redrive AND a digest Failed   -> re-drive can retry toward Ack      *)
(* Note: a Failed digest with Redrive=FALSE contributes NOTHING here —     *)
(* that is exactly the leak.                                               *)
CanMakeProgressOnPins ==
    \/ stableDigestsQueue # <<>>
    \/ bisInFlight # <<>>
    \/ \E d \in pendingActionOutputs : (d \notin serverCache /\ slowState[d] = "Idle")
    \/ \E d \in Digests : slowState[d] = "InFlight"
    \/ (Redrive /\ \E d \in Digests : slowState[d] = "Failed")

(* SAFETY: NoPermanentPinLeak                                              *)
(* If the worker holds a pin, some enabled action must be able to move the *)
(* system toward releasing it. Violated (Redrive=FALSE) by:                *)
(*   WorkerPinAction(d1) -> WorkerUploadFreshFastOk(d1) -> SlowWriteFail(d1)*)
(*   -> quiescent with workerPins={d1}, slowState[d1]=Failed, no redrive   *)
(*   -> CanMakeProgressOnPins = FALSE -> VIOLATED.                         *)
NoPermanentPinLeak ==
    workerPins = {} \/ CanMakeProgressOnPins

(* Reachability witness (run as an INVARIANT expected to VIOLATE in the    *)
(* Fixed cfg): proves the good, fully-released state is genuinely reached  *)
(* — i.e. NoPermanentPinLeak does not hold vacuously.                      *)
PinsNeverAllReleased ==
    ~(pendingActionOutputs = {} /\ workerPins = {})

============================================================================
