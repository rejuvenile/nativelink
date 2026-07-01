------------------- MODULE SchedulerMatchBuggedNoDrainRequeue -------------------
(***************************************************************************
  BUGGED variant of SchedulerMatch used to PROVE the safety/liveness
  invariants have teeth. Regression hypothesis: the eviction drain's
  compensating step -- `UpdateWithDisconnect => stage=Queued, worker_id=NONE`
  [state_manager.rs:862,873] -- is DROPPED, so eviction frees the worker's
  reservation slot but LEAVES the op in Executing/Assigning pointing at the
  now-gone worker.

  This is exactly the class of bug the SchedulerActionOrphan incident was
  (2026-05-10): an op the scheduler thinks is Executing on a worker that no
  longer holds it. Here we inject it at the EVICTION seam instead of the
  restart seam.

  Expected TLC results with this bug:
    - NoSlotLeak / ExecutingConsistent: op is Executing/Assigning but its
      worker is down and holds no reservation => VIOLATED (orphan).
    - EventuallyDispatched (progress): the orphaned op is stuck Executing on
      a dead worker forever, never Completes => VIOLATED.

  Everything else is copied verbatim from SchedulerMatch.tla; only Evict
  changes (BuggedEvict: no requeue, no worker_id unset).
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Workers, Ops, Capacity, MaxRetries, AllowEvict

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME Capacity \in 1..3
ASSUME MaxRetries \in 1..5
ASSUME AllowEvict \in BOOLEAN

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}

VARIABLES opStage, opWorker, reserved, workerUp, retriesLeft, matcherWorker
vars == << opStage, opWorker, reserved, workerUp, retriesLeft, matcherWorker >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ retriesLeft \in [Ops -> 0..MaxRetries]
    /\ matcherWorker \in [Ops -> WorkerOrNone]

HasFreeSlot(w) == workerUp[w] /\ Cardinality(reserved[w]) < Capacity

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ retriesLeft = [o \in Ops |-> MaxRetries]
    /\ matcherWorker = [o \in Ops |-> NONE]

Reserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ HasFreeSlot(w)
    /\ matcherWorker[o] = NONE
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Assigning"]
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = w]
    /\ UNCHANGED << opWorker, workerUp, retriesLeft >>

AssignCommit(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ workerUp[matcherWorker[o]]
    /\ o \in reserved[matcherWorker[o]]
    /\ opWorker[o] = NONE
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = matcherWorker[o]]
    /\ UNCHANGED << reserved, workerUp, retriesLeft, matcherWorker >>

AssignVersionConflict(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ retriesLeft[o] > 0
    /\ retriesLeft' = [retriesLeft EXCEPT ![o] = @ - 1]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, matcherWorker >>

AssignExhaust(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ retriesLeft[o] = 0
    /\ LET w == matcherWorker[o] IN
        reserved' = [reserved EXCEPT ![w] = IF workerUp[w] THEN @ \ {o} ELSE @]
    /\ opStage' = [opStage EXCEPT ![o] = "Queued"]
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ retriesLeft' = [retriesLeft EXCEPT ![o] = MaxRetries]
    /\ UNCHANGED << opWorker, workerUp >>

AssignAbortedAlreadyRequeued(o) ==
    /\ opStage[o] = "Queued"
    /\ matcherWorker[o] \in Workers
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, retriesLeft >>

Complete(o) ==
    /\ opStage[o] = "Executing"
    /\ opWorker[o] \in Workers
    /\ workerUp[opWorker[o]]
    /\ o \in reserved[opWorker[o]]
    /\ LET w == opWorker[o] IN
        reserved' = [reserved EXCEPT ![w] = @ \ {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Completed"]
    /\ opWorker' = [opWorker EXCEPT ![o] = NONE]
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ UNCHANGED << workerUp, retriesLeft >>

(* THE BUG: eviction frees the slot (drain) but DOES NOT requeue the op or
   unset its worker_id. The op stays Executing/Assigning pointing at the
   now-down worker => orphan. *)
BuggedEvict(w) ==
    /\ AllowEvict
    /\ workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = FALSE]
    /\ reserved' = [reserved EXCEPT ![w] = {}]
    /\ UNCHANGED << opStage, opWorker, retriesLeft, matcherWorker >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, retriesLeft, matcherWorker >>

AllCompleted == \A o \in Ops : opStage[o] = "Completed"
Done == AllCompleted /\ UNCHANGED vars

Next ==
    \/ \E o \in Ops, w \in Workers : Reserve(o, w)
    \/ \E o \in Ops : AssignCommit(o)
    \/ \E o \in Ops : AssignVersionConflict(o)
    \/ \E o \in Ops : AssignExhaust(o)
    \/ \E o \in Ops : AssignAbortedAlreadyRequeued(o)
    \/ \E o \in Ops : Complete(o)
    \/ \E w \in Workers : BuggedEvict(w)
    \/ \E w \in Workers : Reconnect(w)
    \/ Done

Fairness ==
    /\ \A o \in Ops, w \in Workers : WF_vars(Reserve(o, w))
    /\ \A o \in Ops : SF_vars(AssignCommit(o))
    /\ \A o \in Ops : WF_vars(AssignExhaust(o))
    /\ \A o \in Ops : WF_vars(AssignAbortedAlreadyRequeued(o))
    /\ \A w \in Workers : SF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

NoDoubleDispatch ==
    \A o \in Ops :
        (opStage[o] = "Executing") =>
            /\ opWorker[o] \in Workers
            /\ o \in reserved[opWorker[o]]
            /\ \A w \in Workers :
                 (w # opWorker[o] /\ workerUp[w]) => o \notin reserved[w]

NoSlotLeak ==
    /\ \A w \in Workers : ~workerUp[w] => reserved[w] = {}
    /\ \A w \in Workers : \A o \in reserved[w] :
         workerUp[w] => opStage[o] \in {"Assigning", "Executing"}
    /\ \A o \in Ops :
         (opStage[o] \in {"Queued", "Completed"}) =>
            \A w \in Workers : (workerUp[w] => o \notin reserved[w])

CapacityRespected ==
    \A w \in Workers : workerUp[w] => Cardinality(reserved[w]) <= Capacity

RetryBounded == \A o \in Ops : retriesLeft[o] \in 0..MaxRetries

ExecutingConsistent ==
    \A o \in Ops :
        (opStage[o] = "Executing") => (opWorker[o] = matcherWorker[o]
                                       \/ opWorker[o] \in Workers)

EventuallyDispatched ==
    \A o \in Ops :
        (opStage[o] = "Queued") ~> (opStage[o] \in {"Executing", "Completed"})

\* A tighter orphan detector: no op may be Executing on a DOWN worker.
NoOrphanExecuting ==
    \A o \in Ops :
        (opStage[o] = "Executing" /\ opWorker[o] \in Workers) => workerUp[opWorker[o]]

=============================================================================
