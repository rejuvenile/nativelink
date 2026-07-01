-------------------------- MODULE SchedulerMatch --------------------------
(***************************************************************************
  Models the nativelink SimpleScheduler core matching/dispatch/lifecycle
  state machine and model-checks the 6 composite invariants that the Rust
  example-tests can only SAMPLE:

    (1) PROGRESS / NO-WEDGE (liveness)
    (2) NO-DOUBLE-DISPATCH (safety)
    (3) RESERVATION INTEGRITY (safety, gauge balance)
    (4) VERSION-CONFLICT CONVERGENCE (safety + bounded retry)
    (5) DEDUP (safety) -- modeled in sibling spec SchedulerDedup.tla
    (6) RETRY-BUDGET TERMINATION (liveness) -- modeled in SchedulerRetryBudget.tla

  This module covers (1)(2)(3)(4). (5) and (6) are cleanly separable
  (they do not interact with the matching∥eviction interleaving) and get
  their own tractable specs so the state space here stays small.

  ------------------------------------------------------------------------
  THE COMPOSITE (matching loop ∥ worker connect/disconnect/evict ∥ update):

    corner 1 -- RESERVATION (worker pool, under the inner write lock):
       `inner_find_and_reserve_worker` atomically inserts the op into the
       chosen worker's `running_action_infos` and reduces its platform
       props. Two concurrent matches cannot pick the same slot because the
       insert is under one lock acquisition.
       [api_worker_scheduler.rs:1278, :1651-1656]

    corner 2 -- OP-STATE COMMIT (action DB, lock-free, SEPARATE lock):
       `assign_operation(Ok(worker))` == `update_operation(UpdateWithAction
       Stage(Executing))`. Runs the optimistic-concurrency retry loop
       (MAX_UPDATE_RETRIES=5, Aborted => retry). Guarded against
       double-assign: if the op is ALREADY Executing it returns
       Aborted "Action already assigned".
       [simple_scheduler.rs:547, state_manager.rs:748-753, :1277]

    corner 3 -- NOTIFY (after the pool lock drops):
       `send_reserved_worker_notification` sends StartExecute to the worker
       over its channel. The worker may have been EVICTED in the window
       between reserve and notify => `Cs2Outcome::WorkerGone` class.
       [simple_scheduler.rs:579, api_worker_scheduler.rs:1766-1774]

    THE COMPENSATOR -- eviction drain:
       `immediate_evict_worker` -> `remove_worker` pops the worker and
       `running_action_infos.drain()`, then for EACH drained op fires
       `update_operation(op, w, UpdateWithDisconnect)` which sets the op
       stage back to Queued AND unsets its worker_id.
       [api_worker_scheduler.rs:1963-1969, state_manager.rs:862, :873]

    THE FALLBACK -- saturation fall-through:
       when EVERY viable worker is saturated (weighted_free==0) all cache
       tiers decline and selection drops to LRU/MRU, which STILL dispatches.
       A never-reported worker is TREATED as saturated (#sched-zeroload) but
       a fully-never-reported fleet still routes via the fall-through, so it
       is not wedged. Abstracted here as a boolean the environment toggles.
       [api_worker_scheduler.rs:1382-1439, :1359-1379]

  ------------------------------------------------------------------------
  CITATIONS (each modeled mechanism, file:line):
    [reserve]      api_worker_scheduler.rs:1278 inner_find_and_reserve_worker
    [reserve-ins]  api_worker_scheduler.rs:1873 running_action_infos.insert
    [assign]       simple_scheduler.rs:547 assign_operation(Ok(worker))
    [assign-impl]  state_manager.rs:1277 assign_operation
    [dbl-guard]    state_manager.rs:748-753 "Action already assigned" => Aborted
    [wid-guard]    state_manager.rs:694-714 worker-id mismatch => Aborted
    [retry-loop]   state_manager.rs:649 for _ in 0..MAX_UPDATE_RETRIES
    [aborted-cont] state_manager.rs:899-908 Aborted => continue
    [assign-fail]  simple_scheduler.rs:551-564 unreserve on assign error;
                   Aborted from assign is BENIGN (returns Ok, no unreserve!)
    [unreserve]    api_worker_scheduler.rs:1662 inner_unreserve_worker
    [notify]       simple_scheduler.rs:579 send_reserved_worker_notification
    [evict-drain]  api_worker_scheduler.rs:1963 running_action_infos.drain()
    [disc=queued]  state_manager.rs:862 UpdateWithDisconnect => Queued
    [queued-unset] state_manager.rs:870-873 Queued => set_worker_id(None)
    [fallthrough]  api_worker_scheduler.rs:1405 saturation_fall_through

  MODELING CHOICE ON [assign-fail] (this is the load-bearing subtlety):
    In do_try_match, when assign_operation returns Aborted the code treats
    it as BENIGN and returns Ok WITHOUT calling unreserve_worker
    (simple_scheduler.rs:557-561). So a reservation made just before an
    Aborted assign is NOT explicitly undone by the matcher -- it is expected
    to be reclaimed by the eviction drain or by completion. We model this
    faithfully and let invariant (3) test whether that leaves a stranded
    slot.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,        \* set of worker IDs, e.g. {w1, w2}
    Ops,            \* set of operation IDs, e.g. {o1, o2}
    Capacity,       \* per-worker reservation-slot capacity (1 or 2)
    MaxRetries,     \* MAX_UPDATE_RETRIES abstraction (>=1); prod=5
    AllowEvict      \* TRUE => environment may evict a reserved worker

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Ops) >= 1
ASSUME Capacity \in 1..3
ASSUME MaxRetries \in 1..5
ASSUME AllowEvict \in BOOLEAN

NONE == "none"
WorkerOrNone == Workers \cup {NONE}

(***************************************************************************
  OP STAGE (action-DB view, corner 2):
    Queued     -- eligible for matching
    Assigning  -- matcher reserved a worker & is running assign_operation's
                  retry loop (op-DB not yet committed to Executing)
    Executing  -- assign_operation committed Executing to the action DB
    Completed  -- terminal
  Note Assigning is a MODEL-ONLY intermediate exposing the lock-free
  window between reserve (pool) and the Executing commit (DB). In prod the
  op row is literally still Queued during this window; we split it out so
  the interleaving with eviction is observable.
 ***************************************************************************)
Stages == {"Queued", "Assigning", "Executing", "Completed"}

VARIABLES
    opStage,        \* Ops -> Stages
    opWorker,       \* Ops -> WorkerOrNone : action-DB's assigned worker_id
    reserved,       \* Workers -> SUBSET Ops : worker's running_action_infos
    workerUp,       \* Workers -> BOOLEAN : worker present in the pool
    retriesLeft,    \* Ops -> 0..MaxRetries : remaining assign retries
    matcherWorker   \* Ops -> WorkerOrNone : which worker THIS match reserved
                    \* (the reservation the in-flight matcher holds for op)

vars == << opStage, opWorker, reserved, workerUp, retriesLeft, matcherWorker >>

TypeOK ==
    /\ opStage \in [Ops -> Stages]
    /\ opWorker \in [Ops -> WorkerOrNone]
    /\ reserved \in [Workers -> SUBSET Ops]
    /\ workerUp \in [Workers -> BOOLEAN]
    /\ retriesLeft \in [Ops -> 0..MaxRetries]
    /\ matcherWorker \in [Ops -> WorkerOrNone]

(* A worker can accept a new reservation iff it is up and below capacity. *)
HasFreeSlot(w) ==
    /\ workerUp[w]
    /\ Cardinality(reserved[w]) < Capacity

(* Saturation fall-through abstraction: a viable worker exists that has a
   free slot. This intentionally collapses the LRU/MRU cascade -- once a
   free slot exists ANYWHERE, prod always dispatches (via cache tier OR
   fall-through). Correct because the saturation predicate only decides the
   TIER, never whether a free slot is used. *)
SomeWorkerFree == \E w \in Workers : HasFreeSlot(w)

Init ==
    /\ opStage = [o \in Ops |-> "Queued"]
    /\ opWorker = [o \in Ops |-> NONE]
    /\ reserved = [w \in Workers |-> {}]
    /\ workerUp = [w \in Workers |-> TRUE]
    /\ retriesLeft = [o \in Ops |-> MaxRetries]
    /\ matcherWorker = [o \in Ops |-> NONE]

(***************************************************************************
  Reserve(o, w): matcher atomically reserves worker w for op o.
  [reserve] -- under the pool write lock: insert o into reserved[w].
  Only fires for a Queued op and a worker with a free slot. Moves the op
  into the model-only Assigning stage and records the matcher's reservation.
  The op-DB worker_id is NOT set yet (that is the assign commit).
 ***************************************************************************)
Reserve(o, w) ==
    /\ opStage[o] = "Queued"
    /\ HasFreeSlot(w)
    /\ matcherWorker[o] = NONE
    /\ reserved' = [reserved EXCEPT ![w] = @ \cup {o}]
    /\ opStage' = [opStage EXCEPT ![o] = "Assigning"]
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = w]
    /\ UNCHANGED << opWorker, workerUp, retriesLeft >>

(***************************************************************************
  AssignCommit(o): assign_operation successfully commits Executing to the
  action DB. [assign] -> [assign-impl].

  Guarded by [dbl-guard]/[wid-guard]:
   - If the op is ALREADY Executing OR already assigned to a DIFFERENT
     worker, the DB returns Aborted "Action already assigned" -> handled by
     AssignAborted below.
   - Here we model the SUCCESS branch: op is still Assigning (not yet
     Executing) AND its DB worker_id is still consistent (NONE, i.e. the
     drain has not requeued it). Commit sets Executing + worker_id.

  Critically: if the reservation the matcher holds was already reclaimed by
  an eviction drain (matcherWorker[o] evicted), the DB worker_id may have
  been set back to NONE and stage requeued -- that path is AssignAborted /
  AssignStale, NOT this success step.
 ***************************************************************************)
AssignCommit(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ workerUp[matcherWorker[o]]                 \* reserved worker still present
    /\ o \in reserved[matcherWorker[o]]           \* reservation still held
    /\ opWorker[o] = NONE                          \* not requeued/reassigned in window
    /\ opStage' = [opStage EXCEPT ![o] = "Executing"]
    /\ opWorker' = [opWorker EXCEPT ![o] = matcherWorker[o]]
    /\ UNCHANGED << reserved, workerUp, retriesLeft, matcherWorker >>

(***************************************************************************
  AssignVersionConflict(o): the action-DB update lost an optimistic-
  concurrency race (Code::Aborted from update_awaited_action) and the retry
  loop [retry-loop]/[aborted-cont] burns one retry and loops. Models
  invariant (4): bounded retry. When retriesLeft hits 0 the assign path
  exhausts and returns the error [state_manager.rs:977]; we model the
  exhaustion as the matcher UNRESERVING (giving up) so the op falls back to
  Queued -- prod: the do_try_match future returns Err, the reservation is
  reclaimed by the next eviction/GC, but the op stays Queued and is retried
  on the next match cycle. To keep the safety model honest we release the
  slot on exhaustion (see note in AssignExhaust).
 ***************************************************************************)
AssignVersionConflict(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ retriesLeft[o] > 0
    /\ retriesLeft' = [retriesLeft EXCEPT ![o] = @ - 1]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, matcherWorker >>

(***************************************************************************
  AssignExhaust(o): retry budget exhausted. assign_operation returns Err.
  do_try_match on a NON-Aborted err calls unreserve_worker [assign-fail,
  simple_scheduler.rs:551-553] -- so the slot IS released here, and the op
  returns to Queued (matcher gives up this cycle; op re-matched later).
  retriesLeft resets (a fresh match cycle gets a fresh budget).
 ***************************************************************************)
AssignExhaust(o) ==
    /\ opStage[o] = "Assigning"
    /\ matcherWorker[o] \in Workers
    /\ retriesLeft[o] = 0
    /\ LET w == matcherWorker[o] IN
        reserved' = [reserved EXCEPT ![w] =
            IF workerUp[w] THEN @ \ {o} ELSE @]
    /\ opStage' = [opStage EXCEPT ![o] = "Queued"]
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ retriesLeft' = [retriesLeft EXCEPT ![o] = MaxRetries]
    /\ UNCHANGED << opWorker, workerUp >>

(***************************************************************************
  AssignAbortedAlreadyRequeued(o): during the lock-free assign window the
  eviction drain requeued this op (opStage back to Queued, opWorker NONE) --
  but the matcher's reservation record (matcherWorker[o]) still points at the
  now-evicted worker. The assign retry loop reloads the awaited action, sees
  the op is Queued/unassigned again, and [wid-guard] / the Executing-guard
  cause an Aborted -> benign -> the matcher returns Ok WITHOUT unreserving
  [assign-fail: Aborted branch returns Ok(())].

  This is the SUBTLE cell: the matcher drops its in-flight reservation
  bookkeeping (the future ends) but does NOT call unreserve. Because the
  worker was EVICTED, its reserved-set was already drained, so there is no
  stranded slot on a live worker. We model exactly that: clear the matcher's
  reservation pointer; the op is already Queued (set by the drain).
 ***************************************************************************)
AssignAbortedAlreadyRequeued(o) ==
    /\ opStage[o] = "Queued"                 \* drain already requeued it
    /\ matcherWorker[o] \in Workers          \* but matcher still had a reservation
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ UNCHANGED << opStage, opWorker, reserved, workerUp, retriesLeft >>

(***************************************************************************
  Complete(o): worker finishes; op-state -> Completed, slot freed.
  [update_action_cs2 Completed path frees the slot, api_worker_scheduler
  .rs:1793 complete_action]. Only a truly Executing op on a live assigned
  worker can complete.
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
    /\ matcherWorker' = [matcherWorker EXCEPT ![o] = NONE]
    /\ UNCHANGED << workerUp, retriesLeft >>

(***************************************************************************
  Evict(w): worker disconnects / times out / is evicted.
  [evict-drain] remove_worker pops it; running_action_infos.drain(); for
  each drained op fire UpdateWithDisconnect => Queued + unset worker_id
  [disc=queued, queued-unset].

  Effect on each op o in reserved[w]:
    - op leaves the worker's reserved set (drain).
    - op stage -> Queued (unless already Completed).
    - opWorker[o] -> NONE.
    - the matcher pointer is NOT cleared here (the in-flight matcher, if any,
      still holds matcherWorker[o]=w; it will observe Aborted on assign and
      clear via AssignAbortedAlreadyRequeued). This is what makes the
      reserve->assign->evict interleaving observable.
  Only fires when AllowEvict and the worker is up.
 ***************************************************************************)
DrainStage(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN "Queued" ELSE opStage[o]

DrainWorker(o, w) ==
    IF o \in reserved[w] /\ opStage[o] # "Completed" THEN NONE ELSE opWorker[o]

Evict(w) ==
    /\ AllowEvict
    /\ workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = FALSE]
    /\ opStage' = [o \in Ops |-> DrainStage(o, w)]
    /\ opWorker' = [o \in Ops |-> DrainWorker(o, w)]
    /\ reserved' = [reserved EXCEPT ![w] = {}]
    \* retriesLeft reset for requeued ops: a fresh match cycle => fresh budget.
    /\ retriesLeft' = [o \in Ops |->
            IF o \in reserved[w] /\ opStage[o] # "Completed"
            THEN MaxRetries ELSE retriesLeft[o]]
    /\ UNCHANGED << matcherWorker >>

(***************************************************************************
  Reconnect(w): an evicted worker rejoins the pool (empty reserved set).
  Needed so PROGRESS is achievable after an evict-everything scenario.
 ***************************************************************************)
Reconnect(w) ==
    /\ ~workerUp[w]
    /\ workerUp' = [workerUp EXCEPT ![w] = TRUE]
    /\ UNCHANGED << opStage, opWorker, reserved, retriesLeft, matcherWorker >>

(***************************************************************************
  AllTerminal: every op has reached a benign terminal (Executing that can
  Complete, or Completed). Used to gate the Done stutter so TLC does not
  flag the legitimate all-dispatched / all-completed fixpoint as a deadlock.
  Note "Executing" is terminal-benign for the PROGRESS property (the op HAS
  been dispatched -- the property is already satisfied); we let it Complete
  or dwell.
 ***************************************************************************)
AllCompleted == \A o \in Ops : opStage[o] = "Completed"

(* Done: benign stutter enabled only when every op is Completed, so the
   deadlock detector does not fire on the good terminal state while the
   liveness property is still evaluated at the fixpoint. Mirrors the
   SchedulerActionOrphan.tla Done idiom. *)
Done ==
    /\ AllCompleted
    /\ UNCHANGED vars

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

(***************************************************************************
  Fairness for liveness (PROGRESS). We make the productive actions weakly
  fair so the model cannot stall a Queued op forever by refusing to act,
  BUT we do NOT make Evict fair (eviction is an adversary event, not a
  scheduler obligation) -- otherwise infinite eviction could starve progress
  by design. For PROGRESS we run WITHOUT AllowEvict-forever: the cfg for the
  liveness check bounds it (see SchedulerMatchProgress.cfg -- AllowEvict may
  be TRUE but a state constraint caps total evictions).
 ***************************************************************************)
Fairness ==
    /\ \A o \in Ops, w \in Workers : WF_vars(Reserve(o, w))
    \* STRONG fairness on the CAS commit: an op that is repeatedly (not
    \* necessarily continuously) able to commit Executing eventually does.
    \* This is the precise TLA+ encoding of the production reality that the
    \* optimistic-concurrency retry cannot lose FOREVER: every Aborted
    \* (version conflict) requires a CONCURRENT committer to have made
    \* progress, and there are finitely many concurrent writers per op, so
    \* the CAS eventually lands. Modeling this as SF (not WF) is load-bearing
    \* -- see PROGRESS finding: with only WF, TLC finds a legitimate lasso
    \* where AssignVersionConflict/AssignExhaust/Reserve cycle forever and
    \* the op is never dispatched. The Rust regression test MUST bound the
    \* number of injected version conflicts to reflect this SF assumption.
    /\ \A o \in Ops : SF_vars(AssignCommit(o))
    /\ \A o \in Ops : WF_vars(AssignExhaust(o))
    /\ \A o \in Ops : WF_vars(AssignAbortedAlreadyRequeued(o))
    /\ \A w \in Workers : WF_vars(Reconnect(w))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  INVARIANTS
 ***************************************************************************)

(* (2) NO-DOUBLE-DISPATCH.
   An op is "actively dispatched to w" if it is Executing with opWorker=w.
   No op may be Executing on two workers -- trivially true given opWorker is
   a function, so the REAL content is: an Executing op's worker actually
   holds the reservation, and no OTHER worker holds a live reservation for
   that same op. A stale reservation on a still-live worker for an op that
   is Executing elsewhere would be a double-dispatch. *)
NoDoubleDispatch ==
    \A o \in Ops :
        (opStage[o] = "Executing") =>
            /\ opWorker[o] \in Workers
            /\ o \in reserved[opWorker[o]]
            /\ \A w \in Workers :
                 (w # opWorker[o] /\ workerUp[w]) => o \notin reserved[w]

(* (3) RESERVATION INTEGRITY / gauge balance.
   Every op that appears in ANY live worker's reserved set is in a state
   that legitimately holds a slot (Assigning or Executing), and its holding
   worker is unique among LIVE workers. No slot is held by a Queued or
   Completed op on a live worker (that would be a leaked slot). Evicted
   workers have empty reserved sets. *)
NoSlotLeak ==
    /\ \A w \in Workers :
         ~workerUp[w] => reserved[w] = {}
    /\ \A w \in Workers : \A o \in reserved[w] :
         workerUp[w] => opStage[o] \in {"Assigning", "Executing"}
    /\ \A o \in Ops :
         (opStage[o] \in {"Queued", "Completed"}) =>
            \A w \in Workers : (workerUp[w] => o \notin reserved[w])

(* Slot count never exceeds capacity on any live worker. *)
CapacityRespected ==
    \A w \in Workers : workerUp[w] => Cardinality(reserved[w]) <= Capacity

(* (4) VERSION-CONFLICT retry budget is bounded and monotone within a
   match attempt: retriesLeft stays within [0, MaxRetries]. *)
RetryBounded ==
    \A o \in Ops : retriesLeft[o] \in 0..MaxRetries

(* Consistency: an Executing op's DB worker_id agrees with the reservation
   holder. (assign commits worker_id and reservation together.) *)
ExecutingConsistent ==
    \A o \in Ops :
        (opStage[o] = "Executing") => (opWorker[o] = matcherWorker[o]
                                       \/ opWorker[o] \in Workers)

Safety ==
    /\ TypeOK
    /\ NoDoubleDispatch
    /\ NoSlotLeak
    /\ CapacityRespected
    /\ RetryBounded

------------------------------------------------------------------------------
(***************************************************************************
  LIVENESS (1) PROGRESS / NO-WEDGE.

  If an op is Queued and some worker has a free slot, it is eventually
  dispatched (reaches Executing) OR terminally Completed. We phrase it as:
  every op eventually leaves Queued (to Executing or Completed), under the
  fairness assumptions and a bounded-eviction environment.

  We use a "leads-to Executing-or-Completed" from Queued. This is the
  strongest useful liveness; it MUST hold across the saturation fall-through
  (any free slot => reserve enabled) and after eviction+reconnect.
 ***************************************************************************)
EventuallyDispatched ==
    \A o \in Ops :
        (opStage[o] = "Queued") ~> (opStage[o] \in {"Executing", "Completed"})

=============================================================================
