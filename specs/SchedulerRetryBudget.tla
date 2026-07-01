------------------------ MODULE SchedulerRetryBudget ------------------------
(***************************************************************************
  Models invariant (6) RETRY-BUDGET TERMINATION of the update_operation
  UpdateWithError path (simple_scheduler_state_manager.rs:815-868).

  CONTRACT:
    - `attempts` is MONOTONIC (only ever incremented, never reset within an
      op's life).
    - A BACKPRESSURE error (Code::ResourceExhausted) does NOT increment
      attempts [state_manager.rs:817,837] -- it re-queues without consuming
      budget ("ride out the backpressure").
    - A FAILED-PRECONDITION error (missing_inputs, Code::FailedPrecondition)
      is TERMINAL immediately -- no retry [state_manager.rs:836,841].
    - Any OTHER error increments attempts [:838]; if
      `attempts > max_job_retries` the op goes to terminal Completed(error)
      [:842-857], else it re-queues [:859].
    - SIGKILL (exit_code 9) on a Completed stage is treated like a retryable
      error: attempts += 1, re-queue if <= max_job_retries else terminal
      [state_manager.rs:761-780]. Modeled as the same "retryable error" class.

  THE LIVENESS CLAIM: an op cannot retry FOREVER on NON-backpressure errors.
  It reaches a terminal state (Completed) within max_job_retries such errors.
  Backpressure errors are unbounded-in-count BUT do not consume budget, so
  the ONLY way to avoid termination is an INFINITE backpressure stream --
  which is a real environment (a permanently-overloaded fleet), out of scope
  for the retry-budget guarantee. We therefore check termination under the
  assumption that backpressure eventually stops (SF on the non-backpressure
  transitions once budget matters), and separately confirm the SAFETY part
  (attempts monotonic, terminal is sticky, budget never exceeded).

  CITATIONS:
    [bp-noincr]   state_manager.rs:817,837 backpressure => no attempts++
    [fp-terminal] state_manager.rs:836,841 FailedPrecondition => terminal
    [err-incr]    state_manager.rs:838 other error => attempts += 1
    [over-budget] state_manager.rs:842 attempts > max_job_retries => Completed
    [requeue]     state_manager.rs:859 else => Queued
    [sigkill]     state_manager.rs:761-780 exit_code 9 => retryable
    [disc-requeue] state_manager.rs:862 UpdateWithDisconnect => Queued
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    MaxJobRetries,  \* max_job_retries (prod default 3)
    MaxBackpressure \* bound on modeled backpressure events (so TLC's state
                    \* space is finite AND the "backpressure eventually
                    \* stops" assumption is explicit)

ASSUME MaxJobRetries \in 1..5
ASSUME MaxBackpressure \in 0..4

Stages == {"Queued", "Executing", "Completed"}

VARIABLES
    stage,          \* current op stage
    attempts,       \* non-backpressure attempt count (monotonic)
    bpLeft          \* remaining backpressure events the environment may inject

vars == << stage, attempts, bpLeft >>

TypeOK ==
    /\ stage \in Stages
    /\ attempts \in 0..(MaxJobRetries + 1)
    /\ bpLeft \in 0..MaxBackpressure

Init ==
    /\ stage = "Queued"
    /\ attempts = 0
    /\ bpLeft = MaxBackpressure

(* Dispatch: Queued -> Executing (a worker picks it up). *)
Dispatch ==
    /\ stage = "Queued"
    /\ stage' = "Executing"
    /\ UNCHANGED << attempts, bpLeft >>

(* Backpressure error: Executing -> Queued, attempts UNCHANGED [bp-noincr].
   Bounded by bpLeft so the state space is finite. *)
BackpressureError ==
    /\ stage = "Executing"
    /\ bpLeft > 0
    /\ stage' = "Queued"
    /\ bpLeft' = bpLeft - 1
    /\ UNCHANGED << attempts >>

(* Retryable (non-backpressure) error: attempts += 1 [err-incr]; if the new
   count exceeds the budget -> terminal Completed [over-budget], else
   re-queue [requeue]. Covers generic errors AND SIGKILL. *)
RetryableError ==
    /\ stage = "Executing"
    /\ LET newAttempts == attempts + 1 IN
        /\ attempts' = newAttempts
        /\ stage' = IF newAttempts > MaxJobRetries THEN "Completed" ELSE "Queued"
    /\ UNCHANGED << bpLeft >>

(* FailedPrecondition (missing inputs): terminal immediately, no retry
   [fp-terminal]. Attempts is still incremented in code (the ++ happens
   before the missing_inputs branch at :837-841 for non-backpressure), but
   the op terminates regardless. Model: increment then terminal. *)
FailedPrecondition ==
    /\ stage = "Executing"
    /\ attempts' = attempts + 1
    /\ stage' = "Completed"
    /\ UNCHANGED << bpLeft >>

(* Success: Executing -> Completed (normal). *)
Success ==
    /\ stage = "Executing"
    /\ stage' = "Completed"
    /\ UNCHANGED << attempts, bpLeft >>

Done == stage = "Completed" /\ UNCHANGED vars

Next ==
    \/ Dispatch
    \/ BackpressureError
    \/ RetryableError
    \/ FailedPrecondition
    \/ Success
    \/ Done

(* Fairness: Dispatch and RetryableError are weakly fair so the op keeps
   making progress toward its terminal. BackpressureError is NOT fair (it is
   an environment event, and bLeft bounds it anyway). This encodes
   "backpressure eventually stops"; once bpLeft=0 only budget-consuming
   transitions remain, forcing termination. *)
Fairness ==
    /\ WF_vars(Dispatch)
    /\ WF_vars(RetryableError)

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  INVARIANTS (safety)
 ***************************************************************************)

(* attempts never exceeds MaxJobRetries + 1 (the value at which the op is
   forced terminal). It can never grow unboundedly. *)
AttemptsBounded == attempts <= MaxJobRetries + 1

(* Terminal is sticky: once Completed, stays Completed (no resurrection). *)
CompletedSticky == (stage = "Completed") => (stage' = "Completed" \/ stage = stage')

(* An op that has consumed its full budget is terminal (cannot be Executing
   with attempts already over budget). *)
OverBudgetIsTerminal ==
    (attempts > MaxJobRetries) => (stage = "Completed")

Safety ==
    /\ TypeOK
    /\ AttemptsBounded
    /\ OverBudgetIsTerminal

------------------------------------------------------------------------------
(***************************************************************************
  LIVENESS (6): the op eventually reaches a terminal state. Under the
  fairness assumption (backpressure bounded / eventually stops), no infinite
  retry loop exists.
 ***************************************************************************)
EventuallyTerminal == (stage # "Completed") ~> (stage = "Completed")

=============================================================================
