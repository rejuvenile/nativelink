--------------------- MODULE ChunkRaceWriterBuggedNoPurge ---------------------
(***************************************************************************)
(* Mutation of ChunkRaceWriter that violates the cancel-handoff contract: *)
(* RaceWriterGuard::drop does NOT purge the writer's WriterId from       *)
(* chunks_in_flight.                                                      *)
(*                                                                          *)
(* In production, this would correspond to deleting the                   *)
(* `purge_writer_in_flight` call at chunked_race_state.rs:775.           *)
(*                                                                          *)
(* Expected violation: a writer aborts mid-stream WITHOUT clearing its   *)
(* in-flight slots, leaving stale WriterId entries that PERMANENTLY     *)
(* block other writers from racing the same offsets — eventually all   *)
(* surviving writers are stuck in TryAdmitRacingLoser for the orphaned *)
(* offsets, the bitmap can never fill, and no commit can fire. The     *)
(* blob is permanently wedged. Watchdog Aborts everyone but no commit  *)
(* result is published.                                                  *)
(*                                                                          *)
(* The liveness property that fails: NoCorruption holds (vacuously, as  *)
(* commitResult is never OkResult), AllAttachedWritersTerminate holds  *)
(* (every writer eventually Aborts via watchdog), but the system never *)
(* successfully commits a blob even when one writer alone could have   *)
(* succeeded by re-trying the orphaned chunks.                         *)
(*                                                                          *)
(* New invariant: NoOrphanedInFlight — if a writer is in {Aborted,     *)
(* Done}, it must have NO in-flight slots remaining. The unfixed code  *)
(* violates this when WriterAbortMidStream fires but doesn't purge.   *)
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

\* BUGGED: PwriteFail does NOT purge other in-flight slots; it only
\* releases the failing chunk's slot. This corresponds to deleting the
\* purge_writer_in_flight call in RaceWriterGuard::drop.
PwriteFail(w, c) ==
    /\ AllowPwriteFail
    /\ w \in chunksInFlight[c]
    \* BUG: only release the failing chunk's slot, not all slots.
    /\ chunksInFlight' = [chunksInFlight EXCEPT ![c] = @ \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    /\ UNCHANGED <<writerSeen, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter, registryEntry>>

\* BUGGED: WriterAbortMidStream does NOT purge — leaves stale slots.
WriterAbortMidStream(w) ==
    /\ writerState[w] = "Sending"
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    \* BUG: chunksInFlight is UNCHANGED — purge_writer_in_flight
    \* would have evacuated w's WriterId from every slot.
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

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

RunnerGuardDropCancelled ==
    /\ AllowRunnerCancel
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "CancelledResult"
    /\ commitRunning'  = FALSE
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Aborted"]
    /\ runnerWriter'   = NULL
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight, registryEntry>>

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

\* Bug-detector invariant: a writer in Aborted/Done state must NOT be
\* in any chunks_in_flight slot. The unfixed code violates this when
\* WriterAbortMidStream fires.
NoOrphanedInFlight ==
    \A w \in Writers :
        writerState[w] \in {"Aborted", "Done"} =>
            \A c \in Chunks : w \notin chunksInFlight[c]

============================================================================
