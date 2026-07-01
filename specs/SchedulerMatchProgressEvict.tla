----------------------- MODULE SchedulerMatchProgressEvict -----------------------
(***************************************************************************
  PROGRESS across eviction. Extends the SchedulerMatch model with a BOUNDED
  eviction budget so the adversary can evict a worker (the reserve->notify
  WorkerGone / disconnect drain path) a finite number of times but cannot
  starve progress by evicting forever. Under this bound, invariant (1)
  NO-WEDGE must still hold: every Queued op eventually reaches Executing or
  Completed EVEN WHEN an eviction requeues it mid-assign.

  Rationale for a FINITE eviction budget (not fair-forever eviction):
  eviction is an adversary/environment event, not a scheduler obligation.
  An adversary that evicts on every cycle forever is a real operational
  failure (a crash-looping worker) and is out of scope for the scheduler's
  progress guarantee -- the scheduler's job is: GIVEN the fleet eventually
  stabilizes, every queued op is dispatched. The bound encodes "eventually
  stabilizes." This mirrors how the SchedulerActionOrphan.tla progress
  argument bounds MaxTime.

  All the transition semantics are copied verbatim from SchedulerMatch.tla
  (kept in sync by construction) with two changes:
    - Evict decrements evictBudget and is disabled at 0.
    - Reconnect is FAIR (an evicted worker rejoins) so post-eviction the
      fleet has capacity again.

  Citations identical to SchedulerMatch.tla.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Workers, Ops, Capacity, MaxRetries, MaxEvicts

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME Capacity \in 1..3
ASSUME MaxRetries \in 1..5
ASSUME MaxEvicts \in 0..4

NONE == "none"
WorkerOrNone == Workers \cup {NONE}
Stages == {"Queued", "Assigning", "Executing", "Completed"}

VARIABLES opStage, opWorker, reserved, workerUp, retriesLeft, matcherWorker,
          evictBudget

vars == << opStage, opWorker, reserved, workerUp, retriesLeft, matcherWorker,
           evictBudget >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ retriesLeft \in [Ops -> 0..MaxRetries]
    /\ matcherWorker \in [Ops -> WorkerOrNone]
    /\ evictBudget \in 0..MaxEvicts

HasFreeSlot(w) == workerUp[w] /\ Cardinality(reserved[w]) < Capacity
SomeWorkerFree == \E w \in Workers : HasFreeSlot(w)

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ retriesLeft = [o \in Ops |-> MaxRetries]
    /\ matcherWorker = [o \in Ops |-> NONE]
    /\ evictBudget = MaxEvicts

Reserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ HasFreeSlot(w)
    /\ matcherWorker[o] = NONE
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Assigning"]
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = w]
    /\ UNCHANGED << opWorker, workerUp, retriesLeft, evictBudget >>

AssignCommit(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ workerUp[matcherWorker[o]]
    /\ o \in reserved[matcherWorker[o]]
    /\ opWorker[o] = NONE
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = matcherWorker[o]]
    /\ UNCHANGED << reserved, workerUp, retriesLeft, matcherWorker, evictBudget >>

AssignVersionConflict(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ retriesLeft[o] > 0
    /\ retriesLeft' = [retriesLeft EXCEPT ![o] = @ - 1]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, matcherWorker, evictBudget >>

AssignExhaust(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ retriesLeft[o] = 0
    /\ LET w == matcherWorker[o] IN
        reserved' = [reserved EXCEPT ![w] = IF workerUp[w] THEN @ \ {o} ELSE @]
    /\ opStage' = [opStage EXCEPT ![o] = "Queued"]
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ retriesLeft' = [retriesLeft EXCEPT ![o] = MaxRetries]
    /\ UNCHANGED << opWorker, workerUp, evictBudget >>

AssignAbortedAlreadyRequeued(o) ==
    /\ opStage[o] = "Queued"
    /\ matcherWorker[o] \in Workers
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, retriesLeft, evictBudget >>

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
    /\ UNCHANGED << workerUp, retriesLeft, evictBudget >>

DrainStage(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN "Queued" ELSE opStage[o]
DrainWorker(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN NONE ELSE opWorker[o]

Evict(w) ==
    /\ evictBudget > 0
    /\ workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = FALSE]
    /\ opStage' = [o \in Ops |-> DrainStage(o, w)]
    /\ opWorker' = [o \in Ops |-> DrainWorker(o, w)]
    /\ reserved' = [reserved EXCEPT ![w] = {}]
    /\ retriesLeft' = [o \in Ops |->
            IF o \in reserved[w] /\ opStage[o] # "Completed" THEN MaxRetries ELSE retriesLeft[o]]
    /\ evictBudget' = evictBudget - 1
    /\ UNCHANGED << matcherWorker >>

Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, retriesLeft, matcherWorker, evictBudget >>

AllCompleted == \A o \in Ops : opStage[o] = "Completed"
Done == AllCompleted /\ UNCHANGED vars

Next ==
    \/ \E o \in Ops, w \in Workers : Reserve(o, w)
    \/ \E o \in Ops : AssignCommit(o)
    \/ \E o \in Ops : AssignVersionConflict(o)
    \/ \E o \in Ops : AssignExhaust(o)
    \/ \E o \in Ops : AssignAbortedAlreadyRequeued(o)
    \/ \E o \in Ops : Complete(o)
    \/ \E w \in Workers : Evict(w)
    \/ \E w \in Workers : Reconnect(w)
    \/ Done

Fairness ==
    /\ \A o \in Ops, w \in Workers : WF_vars(Reserve(o, w))
    /\ \A o \in Ops : SF_vars(AssignCommit(o))
    /\ \A o \in Ops : WF_vars(AssignExhaust(o))
    /\ \A o \in Ops : WF_vars(AssignAbortedAlreadyRequeued(o))
    \* Reconnect is STRONGLY fair: after any eviction the worker eventually
    \* rejoins, restoring fleet capacity so the requeued op can be re-matched.
    /\ \A w \in Workers : SF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

\* Safety still holds here too (regression guard on the extended model).
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

(* (1) NO-WEDGE across eviction: every Queued op is eventually dispatched or
   completed, even when eviction requeues it mid-assign. *)
EventuallyDispatched ==
    \A o \in Ops :
        (opStage[o] = "Queued") ~> (opStage[o] \in {"Executing", "Completed"})

=============================================================================
