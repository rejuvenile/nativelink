--------------------- MODULE ChunkRaceWriterBuggedFix1 ---------------------
(***************************************************************************)
(* Mutation of ChunkRaceWriter that violates the FIX-1 contract in        *)
(* CommitRunnerGuard::drop: when the runner panics/cancels, the synthetic *)
(* Cancelled Err IS published BUT commit_running is NOT cleared.          *)
(*                                                                          *)
(* In production, this would correspond to deleting the                   *)
(* `self.state.clear_commit_running()` call at chunked_race_state.rs:886.*)
(* The FIX-1 contract test                                                *)
(* (chunked_race_state.rs:1253-1296                                       *)
(* try_claim_commit_runner_after_cancellation_allows_retry) red-fails    *)
(* against this; we expect TLC to red-fail the property                  *)
(* RunnerOwnsCommit (orphaned commit_running=TRUE with runnerWriter=NULL).*)
(*                                                                          *)
(* This is the "Mutation: comment out the key line; verify test fails    *)
(* again" CLAUDE.md TDD step applied at the spec level.                  *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Writers, Chunks, AllowPwriteFail, AllowRunnerCancel
ASSUME Cardinality(Writers) >= 1
ASSUME Cardinality(Chunks)  >= 1
ASSUME AllowPwriteFail   \in BOOLEAN
ASSUME AllowRunnerCancel \in BOOLEAN

WriterStates == {"NotAttached", "Attached", "Sending", "AwaitCommit",
                 "Done", "Aborted"}
ResultDomain == {"NoResult", "OkResult", "ErrResult", "CancelledResult"}

VARIABLES
    writerState, writerSeen, chunksPresent, chunksInFlight,
    commitRunning, commitDoneFlag, commitResult, runnerWriter, registryEntry

NULL == "NULL"

vars == <<writerState, writerSeen, chunksPresent, chunksInFlight,
          commitRunning, commitDoneFlag, commitResult, runnerWriter,
          registryEntry>>

Init ==
    /\ writerState    = [w \in Writers |-> "NotAttached"]
    /\ writerSeen     = [w \in Writers |-> {}]
    /\ chunksPresent  = {}
    /\ chunksInFlight = [c \in Chunks |-> {}]
    /\ commitRunning  = FALSE
    /\ commitDoneFlag = FALSE
    /\ commitResult   = "NoResult"
    /\ runnerWriter   = NULL
    /\ registryEntry  = TRUE

AttachWriter(w) ==
    /\ writerState[w] = "NotAttached"
    /\ IF registryEntry
       THEN /\ writerState' = [writerState EXCEPT ![w] = "Attached"]
            /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                           commitRunning, commitDoneFlag, commitResult,
                           runnerWriter, registryEntry>>
       ELSE /\ writerState' = [writerState EXCEPT ![w] = "Aborted"]
            /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                           commitRunning, commitDoneFlag, commitResult,
                           runnerWriter, registryEntry>>

WriterStartSending(w) ==
    /\ writerState[w] = "Attached"
    /\ writerState' = [writerState EXCEPT ![w] = "Sending"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

TryAdmitAlreadyHave(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \in chunksPresent
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

TryAdmitRacingLoser(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] # {}
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

TryAdmitAccept(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] = {}
    /\ chunksInFlight' = [chunksInFlight EXCEPT ![c] = {w}]
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter,
                   registryEntry>>

PwriteSucceed(w, c) ==
    /\ w \in chunksInFlight[c]
    /\ LET newPresent == chunksPresent \cup {c}
           newInFlight == [chunksInFlight EXCEPT ![c] = @ \ {w}]
           allPresent  == newPresent = Chunks
       IN
       /\ chunksPresent'  = newPresent
       /\ chunksInFlight' = newInFlight
       /\ IF allPresent /\ ~commitRunning /\ ~commitDoneFlag
          THEN /\ commitRunning' = TRUE
               /\ runnerWriter'  = w
               /\ writerState'   = [writerState EXCEPT ![w] = "AwaitCommit"]
          ELSE IF allPresent
          THEN /\ commitRunning' = commitRunning
               /\ runnerWriter'  = runnerWriter
               /\ writerState'   = [writerState EXCEPT ![w] = "AwaitCommit"]
          ELSE /\ commitRunning' = commitRunning
               /\ runnerWriter'  = runnerWriter
               /\ writerState'   = writerState
       /\ UNCHANGED <<writerSeen, commitDoneFlag, commitResult, registryEntry>>

PwriteFail(w, c) ==
    /\ AllowPwriteFail
    /\ w \in chunksInFlight[c]
    /\ chunksInFlight' = [c2 \in Chunks |-> chunksInFlight[c2] \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    /\ UNCHANGED <<writerSeen, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter, registryEntry>>

WriterAbortMidStream(w) ==
    /\ writerState[w] = "Sending"
    /\ chunksInFlight' = [c \in Chunks |-> chunksInFlight[c] \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    /\ UNCHANGED <<writerSeen, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter, registryEntry>>

CommitRunnerPublishOk ==
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ chunksPresent = Chunks
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "OkResult"
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Done"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, runnerWriter, registryEntry>>

CommitRunnerPublishErr ==
    /\ AllowRunnerCancel
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ chunksPresent = Chunks
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "ErrResult"
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Done"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, runnerWriter, registryEntry>>

\* BUGGED: omits `commitRunning' = FALSE` AND `runnerWriter' = NULL`.
\* This recreates the FIX-1 violation: the synthetic Cancelled IS
\* published, but commit_running stays TRUE → no future commit-runner can
\* be claimed (since the gate is !commit_running && !commit_done_flag,
\* but commit_done_flag is sticky-TRUE so this is moot for re-election;
\* however the spec invariant RunnerOwnsCommit is violated because
\* commitRunning=TRUE but runnerWriter is conceptually orphaned).
\*
\* In the unfixed CODE, the regression doesn't manifest as a FRESH-state
\* problem (because commit_done_flag is sticky), but it manifests in the
\* test contract: try_claim_commit_runner returns AwaitCommit (correct
\* under sticky done) but the assertion at chunked_race_state.rs:1289-1295
\* checks commit_running was cleared. This spec mutation is the formal
\* analog of that test mutation.
RunnerGuardDropCancelled ==
    /\ AllowRunnerCancel
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "CancelledResult"
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Aborted"]
    \* BUG: commitRunning' and runnerWriter' NOT updated.
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, runnerWriter, registryEntry>>

AwaitingWriterObservesResult(w) ==
    /\ writerState[w] = "AwaitCommit"
    /\ commitDoneFlag
    /\ writerState' = [writerState EXCEPT ![w] = "Done"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

WatchdogFires(w) ==
    /\ writerState[w] = "AwaitCommit"
    /\ ~commitDoneFlag
    /\ writerState' = [writerState EXCEPT ![w] = "Aborted"]
    /\ registryEntry' = FALSE
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter>>

WriterFinishesAfterAllSeen(w) ==
    /\ writerState[w] = "Sending"
    /\ writerSeen[w] = Chunks
    /\ ~(\E c \in Chunks : w \in chunksInFlight[c])
    /\ writerState' = [writerState EXCEPT ![w] = "AwaitCommit"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

Next ==
    \/ \E w \in Writers : AttachWriter(w)
    \/ \E w \in Writers : WriterStartSending(w)
    \/ \E w \in Writers, c \in Chunks : TryAdmitAlreadyHave(w, c)
    \/ \E w \in Writers, c \in Chunks : TryAdmitRacingLoser(w, c)
    \/ \E w \in Writers, c \in Chunks : TryAdmitAccept(w, c)
    \/ \E w \in Writers, c \in Chunks : PwriteSucceed(w, c)
    \/ \E w \in Writers, c \in Chunks : PwriteFail(w, c)
    \/ \E w \in Writers : WriterAbortMidStream(w)
    \/ \E w \in Writers : WriterFinishesAfterAllSeen(w)
    \/ CommitRunnerPublishOk
    \/ CommitRunnerPublishErr
    \/ RunnerGuardDropCancelled
    \/ \E w \in Writers : AwaitingWriterObservesResult(w)
    \/ \E w \in Writers : WatchdogFires(w)

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(CommitRunnerPublishOk)
    /\ WF_vars(RunnerGuardDropCancelled)
    /\ \A w \in Writers : WF_vars(AwaitingWriterObservesResult(w))
    /\ \A w \in Writers : SF_vars(WatchdogFires(w))
    /\ \A w \in Writers, c \in Chunks : WF_vars(PwriteSucceed(w, c))
    /\ \A w \in Writers : WF_vars(WriterFinishesAfterAllSeen(w))
    /\ \A w \in Writers, c \in Chunks :
            WF_vars(TryAdmitAlreadyHave(w, c) \/ TryAdmitRacingLoser(w, c)
                    \/ TryAdmitAccept(w, c))
    /\ \A w \in Writers : WF_vars(WriterStartSending(w))

TypeOK ==
    /\ \A w \in Writers : writerState[w] \in WriterStates
    /\ \A w \in Writers : writerSeen[w]  \subseteq Chunks
    /\ chunksPresent \subseteq Chunks
    /\ \A c \in Chunks : chunksInFlight[c] \subseteq Writers
    /\ commitRunning  \in BOOLEAN
    /\ commitDoneFlag \in BOOLEAN
    /\ commitResult   \in ResultDomain
    /\ runnerWriter   \in (Writers \cup {NULL})
    /\ registryEntry  \in BOOLEAN

\* The bug-detector invariant. The FIX-1 violation manifests as:
\* commitRunning=TRUE AND commitDoneFlag=TRUE AND runnerWriter is in
\* state "Aborted" — the cancellation path was taken (so runner did NOT
\* successfully publish Ok/Err) but commit_running was not cleared.
\*
\* This is distinct from the success-path post-state where commitRunning
\* stays TRUE permanently (production semantics: only the synthetic-cancel
\* path clears it). So the invariant is conditional on the cancel path.
\*
\* Equivalent test from chunked_race_state.rs:1289-1295:
\*   "FIX-1 contract: CommitRunnerGuard::drop MUST clear commit_running
\*    (otherwise a subsequent RunCommit attempt deadlocks waiting for
\*     the cancelled runner that already exited)"
NoOrphanedRunner ==
    \* If a runner is in Aborted state AND commit was running AT THE TIME
    \* of cancellation, the cancellation path must have cleared
    \* commitRunning. We approximate: if commitResult is CancelledResult,
    \* then commitRunning must be FALSE.
    commitResult = "CancelledResult" => ~commitRunning

============================================================================
