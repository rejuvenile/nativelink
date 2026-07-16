--------------------------- MODULE SchedulerActionOrphan ---------------------------
(***************************************************************************
  Models the scheduler-action-orphan bug observed 2026-05-10 18:36:51 PDT
  on buildcache: 4 of 5 actions dispatched immediately after a server restart
  remained `stage=Executing` forever — workers never reported back, scheduler
  never re-dispatched.

  COMPOSITE INVARIANT (in the CLAUDE.md form):
    scheduler_thinks_executing
      => (worker_actually_running
          OR scheduler_re_dispatches_within_N_seconds)

  THE TRIANGLE:
    corner 1: action-stage state (Queued / Executing / Completed) lives in
              scheduler's AwaitedActionDb; stage transitions are explicit.
    corner 2: worker liveness — scheduler's `worker_timeout_s` (default 5s,
              quarantine at 1×, evict at 2×) tracks whether the worker is
              sending keepalives. is_worker_alive=true means "worker process
              alive". It does NOT mean "worker is executing the assigned op".
    corner 3: per-action keepalive — `awaited_action.last_worker_updated`
              advances when the worker submits ExecuteRequest updates for THAT
              op. The `max_action_executing_timeout_s` is the only sweep that
              re-queues an Executing action when the worker stops updating
              that specific op despite still being alive.

  THE BUG:
    `max_action_executing_timeout_s` defaults to 0 (DISABLED) and is not set
    in the prod config (`/srv/nativelink/buildcache-native.json5`,
    `~/fl/bld/infra/nativelink/prod-server.json5`). When the server restarts:
      - Scheduler reloads AwaitedActionDb from store; all in-flight
        actions are still `stage=Executing` with assigned worker.
      - Workers reconnect with NEW `boot_epoch_id`; per #141/#174 protocol
        the scheduler treats the worker's `running_action_infos` as wiped
        on boot-epoch change. Worker sends keepalives at the connection
        level but has nothing to say about the orphan ops it never knew
        about.
      - `should_timeout_operation` (state_manager:348-382) checks
        `is_worker_alive`. Worker IS alive. So the only requeue path is
        `max_executing_timeout > 0` — but it's 0. So the action sits
        forever.

  THE FIX:
    Either (a) on worker boot-epoch change, scheduler proactively scans
    `actionAssignedWorker == w` and resets stage to Queued (push), or
    (b) `max_action_executing_timeout_s > 0` is set in prod (pull / sweep).
    This spec models the (b) form (KeepaliveSweep) because it's the simpler
    closed-form invariant. The (a) form is also a valid cell — see notes.

  CITATIONS:
    [state-mgr]    nativelink-scheduler/src/simple_scheduler_state_manager.rs:303
                   max_executing_timeout field
    [should-to]    state_manager.rs:348-382 should_timeout_operation
    [config]       nativelink-config/src/schedulers.rs:142
                   max_action_executing_timeout_s default = 0 (disabled)
    [worker-wipe]  api_worker_scheduler.rs:1187 running_action_infos.drain()
                   on remove_worker
    [keep-alive]   awaited_action.rs:188 worker_keep_alive
                   sets last_worker_updated_timestamp; only on UpdateOperation
                   for THAT op_id
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Workers,            \* set of worker IDs
    Operations,         \* set of operation IDs
    THRESHOLD,          \* logical-time units after which an Executing op
                        \* with no per-op keepalive is re-queued
    MaxTime,            \* upper bound on `now` for state-space tractability
    EnableSweep         \* TRUE = KeepaliveSweep enabled (Fix);
                        \* FALSE = disabled (Bug repro)

ASSUME Cardinality(Workers) >= 1
ASSUME Cardinality(Operations) >= 1
ASSUME THRESHOLD \in Nat
ASSUME MaxTime \in Nat
ASSUME EnableSweep \in BOOLEAN

VARIABLES
    actionStage,                \* op -> {"Queued", "Executing", "Completed", "Failed"}
    actionAssignedWorker,       \* op -> Workers \cup {"None"}
    schedulerLastSeenAck,       \* op -> Nat \cup {"None"}: scheduler's
                                \* last per-op update timestamp from the
                                \* assigned worker
    workerHasAction,            \* w -> op -> BOOLEAN: worker w has op in
                                \* its `running_action_infos` (its own view)
    workerBootEpoch,            \* w -> Nat: bumped on each worker reconnect
    schedulerKnownBootEpoch,    \* w -> Nat: scheduler's view of w's epoch
    workerConnected,            \* w -> BOOLEAN
    now                         \* Nat: logical clock

vars == <<
    actionStage,
    actionAssignedWorker,
    schedulerLastSeenAck,
    workerHasAction,
    workerBootEpoch,
    schedulerKnownBootEpoch,
    workerConnected,
    now
>>

Stages == {"Queued", "Executing", "Completed", "Failed"}
WorkerOrNone == Workers \cup {"None"}
\* Use -1 as sentinel for "None" so TypeOK stays a flat integer set.
NoneAck == -1
NatOrNone == (-1)..MaxTime

TypeOK ==
    /\ actionStage \in [Operations -> Stages]
    /\ actionAssignedWorker \in [Operations -> WorkerOrNone]
    /\ schedulerLastSeenAck \in [Operations -> NatOrNone]
    /\ workerHasAction \in [Workers -> [Operations -> BOOLEAN]]
    /\ workerBootEpoch \in [Workers -> 0..MaxTime]
    /\ schedulerKnownBootEpoch \in [Workers -> 0..MaxTime]
    /\ workerConnected \in [Workers -> BOOLEAN]
    /\ now \in 0..MaxTime

(***************************************************************************
  Initial state: every op Queued, no worker assignments, all workers
  connected at boot epoch 0 with scheduler's view in sync.
 ***************************************************************************)
Init ==
    /\ actionStage = [op \in Operations |-> "Queued"]
    /\ actionAssignedWorker = [op \in Operations |-> "None"]
    /\ schedulerLastSeenAck = [op \in Operations |-> NoneAck]
    /\ workerHasAction = [w \in Workers |-> [op \in Operations |-> FALSE]]
    /\ workerBootEpoch = [w \in Workers |-> 0]
    /\ schedulerKnownBootEpoch = [w \in Workers |-> 0]
    /\ workerConnected = [w \in Workers |-> TRUE]
    /\ now = 0

(***************************************************************************
  Submit(op): no-op in this spec since we initialize as Queued. We expose
  it for clarity / extensibility. Could be used to model client submission
  AFTER restart; the bug-of-interest is the pre-restart cohort, which is
  Queued at Init.
 ***************************************************************************)
Submit(op) ==
    /\ actionStage[op] = "Queued"   \* placeholder no-op; Init starts Queued
    /\ UNCHANGED vars

(***************************************************************************
  Dispatch(op, w): scheduler decides to give op to worker w. Sets
  scheduler-side stage=Executing AND worker's local view (worker now knows
  it has the op). Both sides update at this point — this is the OK case.
 ***************************************************************************)
Dispatch(op, w) ==
    /\ actionStage[op] = "Queued"
    /\ workerConnected[w]
    /\ schedulerKnownBootEpoch[w] = workerBootEpoch[w]   \* scheduler thinks worker is fresh
    /\ actionStage' = [actionStage EXCEPT ![op] = "Executing"]
    /\ actionAssignedWorker' = [actionAssignedWorker EXCEPT ![op] = w]
    /\ schedulerLastSeenAck' = [schedulerLastSeenAck EXCEPT ![op] = now]
    /\ workerHasAction' = [workerHasAction EXCEPT ![w][op] = TRUE]
    /\ UNCHANGED <<workerBootEpoch, schedulerKnownBootEpoch, workerConnected, now>>

(***************************************************************************
  WorkerExecuteUpdate(op, w): worker submits a stage update for the op
  it's executing (e.g., ExecuteResponse partial / progress / completion).
  This is the ONLY thing that bumps `last_worker_updated_timestamp` for
  THIS op in awaited_action.rs (worker_keep_alive at line 188 is called
  via UpdateOperationType::KeepAlive in state_manager.rs:733).

  Per-op keepalive — distinct from the connection-level worker_keep_alive
  in api_worker_scheduler.rs which only proves the worker process is alive.
 ***************************************************************************)
WorkerExecuteUpdate(op, w) ==
    /\ actionStage[op] = "Executing"
    /\ actionAssignedWorker[op] = w
    /\ workerHasAction[w][op]                            \* worker actually has it
    /\ workerConnected[w]
    /\ schedulerLastSeenAck' = [schedulerLastSeenAck EXCEPT ![op] = now]
    /\ UNCHANGED <<actionStage, actionAssignedWorker, workerHasAction,
                   workerBootEpoch, schedulerKnownBootEpoch, workerConnected, now>>

(***************************************************************************
  WorkerComplete(op, w): worker reports completion.
 ***************************************************************************)
WorkerComplete(op, w) ==
    /\ actionStage[op] = "Executing"
    /\ actionAssignedWorker[op] = w
    /\ workerHasAction[w][op]
    /\ workerConnected[w]
    /\ actionStage' = [actionStage EXCEPT ![op] = "Completed"]
    /\ workerHasAction' = [workerHasAction EXCEPT ![w][op] = FALSE]
    /\ schedulerLastSeenAck' = [schedulerLastSeenAck EXCEPT ![op] = now]
    /\ UNCHANGED <<actionAssignedWorker, workerBootEpoch, schedulerKnownBootEpoch,
                   workerConnected, now>>

(***************************************************************************
  ServerRestart: server SIGTERM/restart.

  Models the production restart at 18:36:51 → 18:37:22:
    - actionStage and actionAssignedWorker survive (AwaitedActionDb is
      backed by a store; the prod scheduler reloads them).
    - schedulerLastSeenAck survives (it's part of awaited_action).
    - workerConnected drops (workers' streams broken by server gone).
    - schedulerKnownBootEpoch resets to 0 — the new scheduler process has
      not yet seen the worker reconnect.
    - workerBootEpoch DOES NOT change yet — workers haven't restarted.
    - workerHasAction unchanged — worker process still has the assignment
      in its own running_action_infos (worker did not restart).

  This faithfully models "scheduler restart, workers do not restart".
 ***************************************************************************)
ServerRestart ==
    /\ \E w \in Workers : workerConnected[w]   \* only fire when something to disconnect
    /\ workerConnected' = [w \in Workers |-> FALSE]
    /\ schedulerKnownBootEpoch' = [w \in Workers |-> 0]
    /\ UNCHANGED <<actionStage, actionAssignedWorker, schedulerLastSeenAck,
                   workerHasAction, workerBootEpoch, now>>

(***************************************************************************
  WorkerReconnectAfterRestart(w): worker reconnects to the new scheduler.
  Per #141/#174 protocol, on reconnect the scheduler treats this as a fresh
  worker — `running_action_infos` is wiped from the scheduler's view, and
  the boot_epoch is bumped (or perceived as new by the scheduler).

  Critically: the WORKER's local view of running_action_infos is wiped
  too (in real prod the worker process has restarted in many scenarios,
  OR the worker tears down its own assignment table on disconnect to stay
  consistent). We model the wipe — this captures the "post-restart cohort
  is orphaned because no one has it anymore" pattern.

  The boot_epoch is also bumped so the scheduler sees a "fresh" worker.
 ***************************************************************************)
\* Ops that were assigned to w pre-reconnect — these become orphans on
\* boot-epoch wipe (workerHasAction[w][op] flips to FALSE).
OrphansFromReconnect(w) ==
    {op \in Operations :
        actionStage[op] = "Executing" /\ actionAssignedWorker[op] = w}

WorkerReconnectAfterRestart(w) ==
    /\ ~workerConnected[w]
    /\ workerBootEpoch[w] < MaxTime
    /\ workerConnected' = [workerConnected EXCEPT ![w] = TRUE]
    /\ workerBootEpoch' = [workerBootEpoch EXCEPT ![w] = workerBootEpoch[w] + 1]
    /\ schedulerKnownBootEpoch' = [schedulerKnownBootEpoch EXCEPT ![w] = workerBootEpoch[w] + 1]
    /\ workerHasAction' = [workerHasAction EXCEPT ![w] = [op \in Operations |-> FALSE]]
    \* Fix-side behavior: when the sweep is enabled, the scheduler ALSO
    \* synchronously re-queues every op assigned to w on reconnect. This
    \* models either (a) max_action_executing_timeout_s sweep firing on
    \* the next refresh tick and seeing the just-reconnected worker has
    \* nothing-in-flight, or (b) explicit on-reconnect cohort scrub.
    \* When EnableSweep=FALSE (Bug repro), no scrub happens.
    /\ IF EnableSweep
       THEN /\ actionStage' = [op \in Operations |->
                  IF op \in OrphansFromReconnect(w) THEN "Queued" ELSE actionStage[op]]
            /\ actionAssignedWorker' = [op \in Operations |->
                  IF op \in OrphansFromReconnect(w) THEN "None" ELSE actionAssignedWorker[op]]
            /\ schedulerLastSeenAck' = [op \in Operations |->
                  IF op \in OrphansFromReconnect(w) THEN NoneAck ELSE schedulerLastSeenAck[op]]
       ELSE UNCHANGED <<actionStage, actionAssignedWorker, schedulerLastSeenAck>>
    /\ UNCHANGED <<now>>

(***************************************************************************
  KeepaliveSweep: the proposed FIX. Background sweeper that scans Executing
  actions and re-queues any whose schedulerLastSeenAck is older than
  THRESHOLD. Equivalent to setting `max_action_executing_timeout_s` >0.

  Disabled when EnableSweep = FALSE (the Bug repro).

  Modeled as: re-queue ALL due ops in a single atomic step. This is the
  "every Tick fires the full sweep" scheduling — a pessimistic upper bound
  on prod sweep latency (prod's per-action timeout in
  state_manager.rs:303 is checked on each refresh of the awaited-action
  iteration; max latency = THRESHOLD + sweep cycle period).
 ***************************************************************************)
DueOps ==
    {op \in Operations :
        /\ actionStage[op] = "Executing"
        /\ schedulerLastSeenAck[op] # NoneAck
        /\ now - schedulerLastSeenAck[op] > THRESHOLD}

\* WouldBeDueAfterTick: orphans that WOULD be due after one tick. Used by
\* Tick gating to prevent advancing time past an orphan's deadline.
WouldBeDueAfterTick ==
    {op \in Operations :
        /\ actionStage[op] = "Executing"
        /\ actionAssignedWorker[op] \in Workers
        /\ ~workerHasAction[actionAssignedWorker[op]][op]
        /\ schedulerLastSeenAck[op] # NoneAck
        /\ (now + 1) - schedulerLastSeenAck[op] > THRESHOLD}

KeepaliveSweep ==
    /\ EnableSweep
    /\ DueOps # {}
    /\ actionStage' = [op \in Operations |->
            IF op \in DueOps THEN "Queued" ELSE actionStage[op]]
    /\ actionAssignedWorker' = [op \in Operations |->
            IF op \in DueOps THEN "None" ELSE actionAssignedWorker[op]]
    /\ schedulerLastSeenAck' = [op \in Operations |->
            IF op \in DueOps THEN NoneAck ELSE schedulerLastSeenAck[op]]
    /\ UNCHANGED <<workerHasAction, workerBootEpoch, schedulerKnownBootEpoch,
                   workerConnected, now>>

(***************************************************************************
  Tick: advance logical clock. Bounded to MaxTime.

  When EnableSweep, Tick MAY NOT fire if there are due ops — the sweep
  must run first. This eliminates the "TLC explored an unfair path that
  refuses to sweep" pseudo-violation, making safety analysis tractable
  without resorting to fairness for the fix-verification.
 ***************************************************************************)
Tick ==
    /\ now < MaxTime
    /\ ~(EnableSweep /\ WouldBeDueAfterTick # {})
    /\ now' = now + 1
    /\ UNCHANGED <<actionStage, actionAssignedWorker, schedulerLastSeenAck,
                   workerHasAction, workerBootEpoch, schedulerKnownBootEpoch,
                   workerConnected>>

(***************************************************************************
  Done: enabled at MaxTime so TLC doesn't flag deadlock; allows the spec
  to dwell forever in the boundary state for safety-invariant evaluation.
 ***************************************************************************)
Done ==
    /\ now = MaxTime
    /\ UNCHANGED vars

(***************************************************************************
  Next state.
 ***************************************************************************)
Next ==
    \/ \E op \in Operations, w \in Workers :
        \/ Dispatch(op, w)
        \/ WorkerExecuteUpdate(op, w)
        \/ WorkerComplete(op, w)
    \/ ServerRestart
    \/ \E w \in Workers : WorkerReconnectAfterRestart(w)
    \/ KeepaliveSweep
    \/ Tick
    \/ Done

(***************************************************************************
  Fairness: Tick is fair so model always advances; KeepaliveSweep is fair
  so sweep eventually fires when due. WorkerComplete is intentionally NOT
  fair — completion is action-execution-bounded, not scheduler-bounded,
  and the safety property doesn't require it.
 ***************************************************************************)
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Tick)
    /\ WF_vars(KeepaliveSweep)

(***************************************************************************
  INVARIANTS (safety)
 ***************************************************************************)

(* Type safety. *)
TypeInv == TypeOK

(* Safety: NoPermanentOrphan.

   An orphan is an Executing op whose assigned worker no longer has it
   in `running_action_infos` — the post-restart state.

   "Permanent" here means "the orphan has aged longer than THRESHOLD time
   units AND has NOT been re-dispatched". This is the precise CLAUDE.md
   composite: scheduler_thinks_executing
     => (worker_actually_running OR re_dispatched_within_THRESHOLD).

   When EnableSweep=FALSE, an orphan can age unboundedly → invariant
   violated. When EnableSweep=TRUE, Tick is gated so the sweep fires
   before any orphan ages past THRESHOLD → invariant holds. *)
NoPermanentOrphan ==
    \A op \in Operations :
        (actionStage[op] = "Executing"
         /\ actionAssignedWorker[op] \in Workers
         /\ ~workerHasAction[actionAssignedWorker[op]][op]
         /\ schedulerLastSeenAck[op] # NoneAck)
        => (now - schedulerLastSeenAck[op] <= THRESHOLD)

(* The CLAUDE.md form: scheduler_thinks_executing
     => (worker_actually_running OR scheduler_re_dispatches_within_N_seconds).
   We expose it under the CLAUDE.md name for grep-ability. *)
COMPOSITE_INVARIANT == NoPermanentOrphan

(***************************************************************************
  LIVENESS
 ***************************************************************************)

(* Every Executing action eventually leaves Executing (either Completes,
   Fails, or is re-queued). Bounded by the Tick fairness assumption + the
   KeepaliveSweep fairness when EnableSweep. *)
EventualProgress ==
    \A op \in Operations :
        (actionStage[op] = "Executing")
        ~> (actionStage[op] \in {"Completed", "Failed", "Queued"})

=============================================================================
