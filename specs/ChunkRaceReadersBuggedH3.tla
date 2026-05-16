-------------------------- MODULE ChunkRaceReadersBuggedH3 -----------------
(***************************************************************************)
(* Bugged variant: the H3 #247 stale-negative window NEVER CLOSES.        *)
(*                                                                          *)
(* Models the hazardous case where `evicting_map.insert` is NEVER called   *)
(* after a successful commit — i.e., the background_spawn'd task never    *)
(* fires (or panics) and the canonical file on disk is invisible to      *)
(* FilesystemStore::has().                                                *)
(*                                                                          *)
(* Expected: TLC reports NO read-side safety violation (the reader sees   *)
(* has=Some via inFlightSetMembership AS LONG AS some writer remains      *)
(* registered). But once ALL writers terminate, inFlightSetMembership    *)
(* clears to FALSE; a fresh reader sees has=None. The post-commit-but-   *)
(* pre-evictingMap-insert state then permits NEW readers to observe     *)
(* False Negative.                                                       *)
(*                                                                          *)
(* This catches the H3 hazard via the EventualConsistencyStrong          *)
(* liveness property: reader Idle when commitDoneFlag=TRUE +             *)
(* OkResult-still leads to ReaderDoneBytes — but ONLY if evictingMap    *)
(* eventually flips. We model the bug by simply removing the action     *)
(* `EvictingMapInsert`. (No `WF_vars(EvictingMapInsert)` is included    *)
(* because that's the precondition we're stripping.)                    *)
(*                                                                          *)
(* This is implemented as a separate module reusing the writer + reader  *)
(* logic but with `EvictingMapInsert` removed from Next.                 *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Writers,
    Readers,
    Chunks,
    AllowPwriteFail,
    AllowRunnerCancel,
    WriterRegistersInFlight

ASSUME Cardinality(Writers) >= 1
ASSUME Cardinality(Readers) >= 1
ASSUME Cardinality(Chunks)  >= 1

WriterStates == { "NotAttached", "Attached", "Sending", "AwaitCommit", "Done", "Aborted" }
ResultDomain == { "NoResult", "OkResult", "ErrResult", "CancelledResult" }
ReaderStates == {
    "ReaderIdle", "ReaderIssuingHas",
    "ReaderHasNone", "ReaderHasSomeViaInFlight", "ReaderHasSomeViaCanonical",
    "ReaderRetryingHas",
    "ReaderDoneNotFound", "ReaderDoneBytes", "ReaderDoneCorrupt", "ReaderTimedOut"
}
ReaderTerminalStates == { "ReaderDoneNotFound", "ReaderDoneBytes", "ReaderDoneCorrupt", "ReaderTimedOut" }

VARIABLES writerState, writerSeen, chunksPresent, chunksInFlight,
          commitRunning, commitDoneFlag, commitResult, runnerWriter,
          registryEntry,
          readerState, inFlightSetMembership, evictingMap, readerObserved

NULL == "NULL"

vars == <<writerState, writerSeen, chunksPresent, chunksInFlight,
          commitRunning, commitDoneFlag, commitResult, runnerWriter,
          registryEntry, readerState, inFlightSetMembership, evictingMap,
          readerObserved>>

readerVars == <<readerState, readerObserved>>
flagVars   == <<inFlightSetMembership, evictingMap>>

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
    /\ readerState    = [r \in Readers |-> "ReaderIdle"]
    /\ inFlightSetMembership = FALSE
    /\ evictingMap    = FALSE
    /\ readerObserved = [r \in Readers |-> "NotYetIssued"]

SomeWriterInFlight == \E w \in Writers : writerState[w] \in {"Sending", "AwaitCommit"}

AttachWriter(w) ==
    /\ writerState[w] = "NotAttached"
    /\ IF registryEntry
       THEN writerState' = [writerState EXCEPT ![w] = "Attached"]
       ELSE writerState' = [writerState EXCEPT ![w] = "Aborted"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry,
                   readerVars, flagVars>>

WriterStartSending(w) ==
    /\ writerState[w] = "Attached"
    /\ writerState' = [writerState EXCEPT ![w] = "Sending"]
    /\ inFlightSetMembership' = IF WriterRegistersInFlight THEN TRUE ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry, readerVars, evictingMap>>

TryAdmitAlreadyHave(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \in chunksPresent
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry, readerVars, flagVars>>

TryAdmitRacingLoser(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] # {}
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry, readerVars, flagVars>>

TryAdmitAccept(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] = {}
    /\ chunksInFlight' = [chunksInFlight EXCEPT ![c] = {w}]
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerVars, flagVars>>

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
       /\ UNCHANGED <<writerSeen, commitDoneFlag, commitResult, registryEntry,
                      readerVars, flagVars>>

PwriteFail(w, c) ==
    /\ AllowPwriteFail
    /\ w \in chunksInFlight[c]
    /\ chunksInFlight' = [c2 \in Chunks |-> chunksInFlight[c2] \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # w
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter, registryEntry,
                   readerVars, evictingMap>>

WriterAbortMidStream(w) ==
    /\ writerState[w] = "Sending"
    /\ chunksInFlight' = [c \in Chunks |-> chunksInFlight[c] \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # w
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter, registryEntry,
                   readerVars, evictingMap>>

CommitRunnerPublishOk ==
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ chunksPresent = Chunks
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "OkResult"
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Done"]
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # runnerWriter
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, runnerWriter, registryEntry,
                   readerVars, evictingMap>>

CommitRunnerPublishErr ==
    /\ AllowRunnerCancel
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ chunksPresent = Chunks
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "ErrResult"
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Done"]
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # runnerWriter
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, runnerWriter, registryEntry,
                   readerVars, evictingMap>>

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
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # runnerWriter
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight, registryEntry,
                   readerVars, evictingMap>>

AwaitingWriterObservesResult(w) ==
    /\ writerState[w] = "AwaitCommit"
    /\ commitDoneFlag
    /\ writerState' = [writerState EXCEPT ![w] = "Done"]
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # w
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry,
                   readerVars, evictingMap>>

WatchdogFires(w) ==
    /\ writerState[w] = "AwaitCommit"
    /\ ~commitDoneFlag
    /\ writerState' = [writerState EXCEPT ![w] = "Aborted"]
    /\ registryEntry' = FALSE
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # w
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, readerVars, evictingMap>>

WriterFinishesAfterAllSeen(w) ==
    /\ writerState[w] = "Sending"
    /\ writerSeen[w] = Chunks
    /\ ~(\E c \in Chunks : w \in chunksInFlight[c])
    /\ writerState' = [writerState EXCEPT ![w] = "AwaitCommit"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry, readerVars, flagVars>>

\* DELIBERATELY OMITTED: EvictingMapInsert. evictingMap stays FALSE
\* forever in this bugged variant.

ReaderObserveHasOutcome ==
    IF inFlightSetMembership THEN "SomeViaInFlight"
    ELSE IF evictingMap      THEN "SomeViaCanonical"
    ELSE                          "None"

ReaderStartIssue(r) ==
    /\ readerState[r] = "ReaderIdle"
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderIssuingHas"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderHasReturns(r) ==
    /\ readerState[r] = "ReaderIssuingHas"
    /\ LET obs == ReaderObserveHasOutcome
       IN /\ readerObserved' = [readerObserved EXCEPT ![r] = obs]
          /\ readerState' = [readerState EXCEPT ![r] =
                CASE obs = "None"              -> "ReaderHasNone"
                  [] obs = "SomeViaInFlight"   -> "ReaderHasSomeViaInFlight"
                  [] obs = "SomeViaCanonical"  -> "ReaderHasSomeViaCanonical"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, flagVars>>

ReaderConcludeNotFound(r) ==
    /\ readerState[r] = "ReaderHasNone"
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneNotFound"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderReadCanonical(r) ==
    /\ readerState[r] = "ReaderHasSomeViaCanonical"
    /\ evictingMap
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderReadFromPin(r) ==
    /\ readerState[r] = "ReaderHasSomeViaInFlight"
    /\ chunksPresent = Chunks
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderRetryHas(r) ==
    /\ readerState[r] = "ReaderHasSomeViaInFlight"
    /\ chunksPresent # Chunks
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderRetryingHas"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderRetryServeFromPin(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ inFlightSetMembership
    /\ chunksPresent = Chunks
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderRetryServeFromCanonical(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ evictingMap
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderRetryConcludeNotFound(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ ~inFlightSetMembership
    /\ ~evictingMap
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneNotFound"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

ReaderRetryTimeout(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ inFlightSetMembership
    /\ chunksPresent # Chunks
    /\ ~evictingMap
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderTimedOut"]
    /\ UNCHANGED <<writerState, writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult, runnerWriter,
                   registryEntry, readerObserved, flagVars>>

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
    \* DELIBERATELY OMITTED: EvictingMapInsert.
    \/ \E r \in Readers : ReaderStartIssue(r)
    \/ \E r \in Readers : ReaderHasReturns(r)
    \/ \E r \in Readers : ReaderConcludeNotFound(r)
    \/ \E r \in Readers : ReaderReadCanonical(r)
    \/ \E r \in Readers : ReaderReadFromPin(r)
    \/ \E r \in Readers : ReaderRetryHas(r)
    \/ \E r \in Readers : ReaderRetryServeFromPin(r)
    \/ \E r \in Readers : ReaderRetryServeFromCanonical(r)
    \/ \E r \in Readers : ReaderRetryConcludeNotFound(r)
    \/ \E r \in Readers : ReaderRetryTimeout(r)

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
            WF_vars(TryAdmitAlreadyHave(w, c) \/ TryAdmitRacingLoser(w, c) \/ TryAdmitAccept(w, c))
    /\ \A w \in Writers : WF_vars(WriterStartSending(w))
    /\ \A r \in Readers : WF_vars(ReaderStartIssue(r))
    /\ \A r \in Readers : WF_vars(ReaderHasReturns(r))
    /\ \A r \in Readers : WF_vars(ReaderConcludeNotFound(r))
    /\ \A r \in Readers : WF_vars(ReaderReadCanonical(r))
    /\ \A r \in Readers : WF_vars(ReaderReadFromPin(r))
    /\ \A r \in Readers :
            WF_vars(ReaderReadFromPin(r) \/ ReaderRetryHas(r))
    /\ \A r \in Readers :
            WF_vars(ReaderRetryServeFromPin(r) \/ ReaderRetryServeFromCanonical(r)
                    \/ ReaderRetryConcludeNotFound(r) \/ ReaderRetryTimeout(r))

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
    /\ \A r \in Readers : readerState[r] \in ReaderStates
    /\ inFlightSetMembership \in BOOLEAN
    /\ evictingMap \in BOOLEAN

\* Invariant: a reader started after commitDoneFlag=TRUE WITH OkResult
\* AND inFlightSetMembership=FALSE (no more writers active) sees NONE.
\* That is the H3 hazard: post-commit state, no writer alive, reader
\* sees None, returns NotFound. This invariant FAILS (state is reachable).
H3HazardStateUnreachable ==
    ~(commitResult = "OkResult"
      /\ ~inFlightSetMembership
      /\ ~evictingMap)

\* Liveness: any reader that becomes idle after commit eventually sees Bytes.
\* This is the property H3 violates — without EvictingMapInsert, the reader
\* will eventually conclude ReaderDoneNotFound instead of ReaderDoneBytes.
EventualConsistencyStrong ==
    \A r \in Readers :
        (commitResult = "OkResult" /\ ~inFlightSetMembership
         /\ readerState[r] = "ReaderIdle")
            ~> (readerState[r] = "ReaderDoneBytes")

============================================================================
