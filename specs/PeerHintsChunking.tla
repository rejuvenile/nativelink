--------------------- MODULE PeerHintsChunking -----------------------------
(***************************************************************************
  #98 (peer_hints chunking, direct-merge design).

  CONTRACT WE'RE MODELING:
    The scheduler emits N PeerHintsChunk messages on the same gRPC stream
    as Update::StartAction. The worker registers each chunk's hints into
    its global peer_locality_map AS THEY ARRIVE — no per-action buffer,
    no count check, no wait. An action's input_fetch may start before,
    during, or after any chunk arrives; correctness does not depend on
    chunks arriving first.

  WHY MODEL THIS:
    The original design proposed a buffered-with-wait scheme where the
    worker would buffer chunks keyed on operation_id, then on StartAction
    read `peer_hints_chunk_count` from StartExecute and wait for that
    many chunks to arrive (Notify::notified()-style). That design has a
    classic lost-wakeup hazard: if the buffer reaches the target count
    BEFORE the wait subscription is installed, the notify_one is dropped
    on the floor (no waiters yet), and the subsequent subscribe ends up
    waiting on a condition whose notification has already been emitted
    and lost. Direct-merge eliminates the buffer + wait entirely; this
    spec proves the direct-merge variant always makes progress regardless
    of chunk-arrival vs subscribe-call interleaving.

  CONSTANTS:
    * NumChunks (Nat): how many PeerHintsChunk messages the scheduler
      emits per action. Production: ceil(input_count / 256), modeled as
      a small Nat (1..3) for tractability.
    * BugMode (BOOLEAN):
        - TRUE  => buffered-with-wait design (the bug). Worker buffers
                   chunks; action waits for `notify_pending` to become
                   TRUE. The notify is edge-triggered: it sets
                   `notify_pending := TRUE` ONLY at the merge transition
                   AND only if `wait_installed = TRUE`. If the merge
                   completes before subscribe, the notify is lost.
        - FALSE => direct-merge design (the fix). Each chunk independently
                   updates the locality map; the action's gate has no
                   chunk dependency.

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * PeerHintsChunkingFixed.cfg (BugMode = FALSE):
      No invariant violation. The action eventually reaches the
      "Done" state on every behavior, and every chunk's hints land
      in the locality map.
    * PeerHintsChunkingBugged.cfg (BugMode = TRUE):
      INVARIANT VIOLATED on `ActionEventuallyDone` (temporal property).
      Counter-example: chunks delivered + merged BEFORE InstallWait
      fires. notify_pending stays FALSE, action never finishes.

  SCOPE — what this spec models:
    * one operation (one action),
    * NumChunks PeerHintsChunk messages, each carrying one hint,
    * a worker-side action whose gate is a Notify-style wait in
      BugMode and a no-op in FixedMode,
    * non-deterministic interleaving of chunk arrival vs InstallWait
      vs FinishAction.

  SCOPE — what this spec does NOT model:
    * the actual grpc stream's FIFO ordering (all message orderings
      are treated as possible),
    * partial chunks / is_last semantics (each modeled chunk carries 1
      hint; chunk_iter's empty-terminal contract is unit-tested),
    * concurrent actions (one per behavior; the production code is
      stateless across actions because the locality map is global),
    * the scheduler's sort-by-size ordering (orthogonal — handled by
      `simple_scheduler_test::peer_hints_from_resolved_tree_test`).

  CITATIONS:
    [code]   nativelink-worker/src/local_worker.rs:760-820
             (`handle_peer_hints_chunk`)
    [test]   nativelink-worker/tests/peer_hints_chunk_test.rs
             (in particular `hint_after_get_part_doesnt_break_action_test`)
    [design] /home/user/.claude/projects/-src-nativelink/memory/project_streaming_design_2026_04_24.md
             (sections "Direct-merge simplification (2026-04-24, post-review)"
              and "What changes per message: peer_hints")
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    NumChunks,    \* Number of PeerHintsChunk messages emitted (Nat >= 1)
    BugMode       \* TRUE => buffered-with-wait; FALSE => direct-merge

ASSUME NumChunks \in Nat /\ NumChunks >= 1
ASSUME BugMode \in BOOLEAN

\* Chunk identifiers.
Chunks == 1..NumChunks

\* Action lifecycle states.
\*   "Pending"  - StartAction received but action hasn't reported done
\*   "Done"     - action completed (input_fetch + execute + finalize)
ActionStates == {"Pending", "Done"}

VARIABLES
    chunksDelivered,   \* set of chunk IDs delivered to the worker
    localityMap,       \* set of chunk IDs registered into peer_locality_map
    actionState,       \* one of ActionStates
    waitInstalled,     \* BugMode-only: TRUE iff worker subscribed
    notifyPending      \* BugMode-only: TRUE iff a notify is queued for the wait

vars == <<chunksDelivered, localityMap, actionState, waitInstalled, notifyPending>>

----------------------------------------------------------------------------
Init ==
    /\ chunksDelivered = {}
    /\ localityMap = {}
    /\ actionState = "Pending"
    /\ waitInstalled = FALSE
    /\ notifyPending = FALSE

----------------------------------------------------------------------------
(* DeliverChunk(c): chunk c arrives at the worker.                         *)
(*   FixedMode (BugMode = FALSE):                                          *)
(*     Register the chunk's hints into the locality map IMMEDIATELY.      *)
(*     waitInstalled / notifyPending are unused.                          *)
(*   BugMode (BugMode = TRUE):                                             *)
(*     Add to chunksDelivered. If this chunk completes the buffer (i.e.   *)
(*     reaches NumChunks), perform the buffer→localityMap merge AND fire *)
(*     a notify_one IF AND ONLY IF a wait is currently installed.         *)
(*     Otherwise the notify is dropped on the floor (the lost-wakeup).    *)
----------------------------------------------------------------------------
DeliverChunk(c) ==
    /\ c \notin chunksDelivered
    /\ chunksDelivered' = chunksDelivered \cup {c}
    /\ \/ /\ ~BugMode
          /\ localityMap' = localityMap \cup {c}
          /\ UNCHANGED <<actionState, waitInstalled, notifyPending>>
       \/ /\ BugMode
          /\ IF Cardinality(chunksDelivered \cup {c}) = NumChunks
             THEN
                /\ localityMap' = chunksDelivered \cup {c}
                /\ \* notify_one is edge-triggered: only fires the wakeup
                   \* IF a wait is currently installed. Otherwise the
                   \* notify is lost — this is the production bug.
                   notifyPending' = waitInstalled
                /\ UNCHANGED <<actionState, waitInstalled>>
             ELSE
                /\ UNCHANGED <<localityMap, actionState, waitInstalled, notifyPending>>

----------------------------------------------------------------------------
(* InstallWait: the worker calls Notify::notified() to subscribe. In      *)
(* FixedMode this is a no-op. In BugMode it installs the wait, but DOES  *)
(* NOT inspect the buffer state — it just sets `waitInstalled := TRUE`. *)
(* The classic lost-wakeup hazard: if the merge already fired without   *)
(* a waiter, the notify is gone, and this subscription will wait        *)
(* forever for a fresh notify that never comes.                          *)
----------------------------------------------------------------------------
InstallWait ==
    /\ BugMode
    /\ waitInstalled = FALSE
    /\ waitInstalled' = TRUE
    /\ UNCHANGED <<chunksDelivered, localityMap, actionState, notifyPending>>

----------------------------------------------------------------------------
(* FinishAction: the action transitions to Done.                          *)
(*   FixedMode: no chunk dependency. The action progresses based on its  *)
(*     own input_fetch / execute logic. Modeled as: gate is always       *)
(*     enabled while Pending.                                             *)
(*   BugMode: the action is gated on the notify firing. notifyPending = *)
(*     TRUE means a wakeup is queued; the FinishAction transition       *)
(*     consumes the notify and progresses.                                *)
----------------------------------------------------------------------------
FinishAction ==
    /\ actionState = "Pending"
    /\ \/ /\ ~BugMode
          /\ actionState' = "Done"
          /\ UNCHANGED <<chunksDelivered, localityMap, waitInstalled, notifyPending>>
       \/ /\ BugMode
          /\ notifyPending = TRUE
          /\ actionState' = "Done"
          /\ notifyPending' = FALSE
          /\ UNCHANGED <<chunksDelivered, localityMap, waitInstalled>>

----------------------------------------------------------------------------
Next ==
    \/ \E c \in Chunks : DeliverChunk(c)
    \/ InstallWait
    \/ FinishAction

----------------------------------------------------------------------------
\* Weak fairness on every transition that's enabled. In direct-merge
\* (FixedMode), this guarantees the action eventually fires FinishAction.
\* In BugMode, FinishAction is gated on notifyPending — which can stay
\* FALSE forever if the merge fired before the wait was installed. WF
\* on FinishAction does NOT help when its precondition is permanently
\* false; that's the lost-wakeup signature.
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(\E c \in Chunks : DeliverChunk(c))
    /\ WF_vars(InstallWait)
    /\ WF_vars(FinishAction)

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ chunksDelivered \subseteq Chunks
    /\ localityMap \subseteq Chunks
    /\ actionState \in ActionStates
    /\ waitInstalled \in BOOLEAN
    /\ notifyPending \in BOOLEAN

(* SAFETY: locality map only contains delivered chunks                    *)
LocalityMapSubsetOfDelivered ==
    localityMap \subseteq chunksDelivered

(* LIVENESS / TEMPORAL: the action eventually reaches Done                *)
(*                                                                          *)
(* FixedMode: holds. The action's gate is "actionState = Pending", which  *)
(* WF_vars(FinishAction) guarantees fires eventually.                     *)
(*                                                                          *)
(* BugMode counter-example: the merge fires before InstallWait. The      *)
(* notifyPending update sees waitInstalled = FALSE so notifyPending      *)
(* stays FALSE. InstallWait fires later but produces no notify (it's     *)
(* a one-way set, not a re-check of buffer state). FinishAction's gate  *)
(* requires notifyPending = TRUE, which never becomes TRUE again.        *)
(* Action stays Pending forever. <>(actionState = "Done") violated.     *)
ActionEventuallyDone ==
    <>(actionState = "Done")

============================================================================
