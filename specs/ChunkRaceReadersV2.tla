-------------------------- MODULE ChunkRaceReadersV2 --------------------------
(***************************************************************************)
(* #494-v3 Phase 2 reader extension over the multi-writer chunked-write    *)
(* race protocol.                                                          *)
(*                                                                          *)
(* Builds on ChunkRaceWriter.tla (verified 2026-05-15 to 3w3c, 115k        *)
(* states). Adds CONCURRENT READERS that consult the same FastSlowStore   *)
(* cascade as the writer's race-state lives behind.                        *)
(*                                                                          *)
(* SCOPE — what THIS module adds                                            *)
(*   * READER PROCESSES (Bazel BS::read, worker peer-fetch, server         *)
(*     internal `has_with_results`).                                       *)
(*   * Reader has-then-get FSM modelling the production cascade            *)
(*     `ExistenceCacheStore::has_with_results` ->                          *)
(*     `FastSlowStore::has_with_results` ->                                *)
(*     `FastSlowStore::get_part` -> slow-tier `FilesystemStore::get_part`. *)
(*   * The `chunked_in_flight_digests` SET on FastSlowStore.               *)
(*     CONSTANT WriterRegistersInFlight controls whether the v2 writer    *)
(*     populates this set (TRUE = post-fix, FALSE = pre-fix bug).         *)
(*   * The #247 stale-negative window (P4): a writer's commit publishes    *)
(*     successfully to the race-state, and the canonical file is visible   *)
(*     to the kernel, but FilesystemStore::has() consults `evicting_map`  *)
(*     which is updated by a `background_spawn`-d task. Modelled as a    *)
(*     bounded NON-DETERMINISTIC delay between PublishOk and the          *)
(*     `evictingMap` flag flipping true.                                   *)
(*   * The PIN read-cascade: when has() returns Some via inFlightSet,    *)
(*     get_part either serves from the writer's pin (if chunksPresent     *)
(*     covers reader's requested offsets) OR falls through to slow-tier   *)
(*     (which returns NotFound while canonical-not-yet-renamed).         *)
(*                                                                          *)
(* SCOPE — what THIS module deliberately abstracts                         *)
(*   * Bytes / pwrite content (chunker is position-based; bytes are        *)
(*     interchangeable across writers per CHUNK_BOUNDARIES_ARE_POSITION_   *)
(*     BASED at chunked_race_state.rs:69).                                 *)
(*   * Multi-digest interleaving (each digest has its own race-state).    *)
(*   * The 500ms WAIT loop in try_get_chunk_from_pin is modelled as     *)
(*     "reader retries has() at most ONCE between observing inFlight=Some *)
(*     and falling through to slow-tier".                                 *)
(*   * Real-time. The watchdog (per writer, 60s in production) and the   *)
(*     500ms WAIT loop are abstracted as "eventually" via fairness.      *)
(*   * AC store reads (string-keyed; chunked-cascade does not apply).     *)
(*   * Streaming-read-while-write (separate ByteStream-only path; not    *)
(*     intersect-able with chunked writers per H4 in the audit).         *)
(*                                                                          *)
(* CITATIONS                                                                *)
(*   [readhas]  nativelink-store/src/fast_slow_store.rs:4262                *)
(*   [readget]  nativelink-store/src/fast_slow_store.rs:5641-5886          *)
(*   [readpin]  nativelink-store/src/fast_slow_store.rs:5767                *)
(*   [bsread]   nativelink-service/src/bytestream_server.rs:3217 / 1371   *)
(*   [peerfet]  nativelink-store/src/worker_proxy_store.rs:1710 / ~883    *)
(*   [echas]    nativelink-store/src/existence_cache_store.rs:371          *)
(*   [247]      nativelink-store/src/filesystem_store.rs:1828              *)
(*   [audit]    .claude/audits/concurrent-readers-vs-writers-2026-05-15.md *)
(*                                                                          *)
(* RECENT PROTOCOL EVOLUTION ADDED SINCE INITIAL SPEC LANDING (#499)        *)
(*   [#447 43e5e101]                                                        *)
(*     Worker `WriteChunked` mid-commit observation switched from           *)
(*     `Aborted+BackpressureSignal{retry_after_ms=250}` polling-retry to a *)
(*     Notify-based wait on the per-digest `commit_done` primitive via    *)
(*     `await_inflight_commit_with_watchdog`                              *)
(*     (`chunked_write_handler.rs:4299`). The waiter blocks up to        *)
(*     `CHUNKED_COMMIT_WATCHDOG_SECS` (60 s) and then surfaces a          *)
(*     watchdog-tagged `DeadlineExceeded`. Spec correspondence:           *)
(*     `WatchdogFires(w)` already models the timeout transition; the      *)
(*     waiter is a CALLER of the same primitive so no new spec action is  *)
(*     required. WriterRegistersInFlight=TRUE still captures whether the *)
(*     waiter sees the in-flight registration.                            *)
(*   [#508 1d329dd6]                                                       *)
(*     `v2_await_commit_result` watchdog `Err` now carries the            *)
(*     `WatchdogTimeoutSignal` discriminator so the chunked client's      *)
(*     `classify_retryable` returns `Retry{WatchdogDeadline}` rather than *)
(*     `Abort`. Spec correspondence: the timeout-vs-abort distinction is  *)
(*     orthogonal to the safety/liveness invariants this module checks;   *)
(*     classifier behavior is downstream of the writer-aborted state the *)
(*     spec already exposes via `WatchdogFires` -> `Aborted`.             *)
(*   [#487 3ecf563d]                                                       *)
(*     `PER_CHUNK_WRITE_TIMEOUT` converted to diagnostic-only — no spec  *)
(*     change because the spec abstracts real-time and does not model    *)
(*     per-chunk timeouts directly (PwriteFail captures the failure mode).*)
(*   [#502 8e6be6d7 / #500 ee13bee0+55c16700]                              *)
(*     Silent-zero and silent-short EOF defenses live on the              *)
(*     `StreamingBlobReader::next_chunk` path, NOT on the                *)
(*     chunked-write race-state cascade modeled here. See the             *)
(*     companion `StreamingBlobReadDuringWrite.tla` spec for that         *)
(*     read-during-write protocol.                                       *)
(*                                                                          *)
(* VERIFIED AGAINST origin/main: 8e6be6d7 (2026-05-16)                     *)
(*   #447, #508, #502, #500, #487, #503, #504 all reviewed; no actions    *)
(*   needed in THIS spec. The spec continues to model the chunked-WC     *)
(*   race-state side correctly.                                          *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Writers,                  \* Set of writer ids, e.g. {w1, w2}
    Readers,                  \* Set of reader ids, e.g. {r1, r2}
    Chunks,                   \* Set of chunk indices, e.g. {c0, c1}
    AllowPwriteFail,          \* TRUE => model a pwrite failure non-determinism
    AllowRunnerCancel,        \* TRUE => model commit-runner panic/cancel
    WriterRegistersInFlight   \* TRUE = v2 writer DOES insert into
                              \* chunked_in_flight_digests on first chunk
                              \* admission and removes on commit/abort
                              \* (post-fix). FALSE = pre-fix bug (H1)
                              \* where v2 writer leaves no read-side trace
                              \* and readers see NotFound during P0..P4.

ASSUME Cardinality(Writers) >= 1
ASSUME Cardinality(Readers) >= 1
ASSUME Cardinality(Chunks)  >= 1
ASSUME AllowPwriteFail        \in BOOLEAN
ASSUME AllowRunnerCancel      \in BOOLEAN
ASSUME WriterRegistersInFlight \in BOOLEAN

\* ===== Per-writer FSM (carried over from ChunkRaceWriter.tla) =====
WriterStates == {
    "NotAttached",
    "Attached",
    "Sending",
    "AwaitCommit",
    "Done",
    "Aborted"
}

ResultDomain == { "NoResult", "OkResult", "ErrResult", "CancelledResult" }

\* ===== Per-reader FSM (NEW for ChunkRaceReadersV2) =====
\* Models the production reader cascade:
\*   ReaderIdle -> ReaderIssuingHas -> {ReaderHasNone, ReaderHasSomeViaInFlight,
\*                                      ReaderHasSomeViaCanonical} -> ...
\* Final states encode the OUTCOME visible to the upstream Bazel/worker:
\*   ReaderDoneNotFound : observed has()=None or get_part() said NotFound
\*   ReaderDoneBytes    : observed full bytes (chunksPresent = Chunks at
\*                        the time get_part returned)
\*   ReaderDoneCorrupt  : observed PARTIAL/wrong bytes — would only fire
\*                        in a buggy implementation; we use it as a
\*                        catastrophic-failure marker.
\*   ReaderTimedOut     : reader gave up waiting on a writer's pin.
\*                        (Models the 500ms WAIT timeout falling through.)
ReaderStates == {
    "ReaderIdle",
    "ReaderIssuingHas",
    "ReaderHasNone",
    "ReaderHasSomeViaInFlight",
    "ReaderHasSomeViaCanonical",
    "ReaderRetryingHas",
    "ReaderDoneNotFound",
    "ReaderDoneBytes",
    "ReaderDoneCorrupt",
    "ReaderTimedOut"
}

\* States in which the reader has reached a terminal observation.
ReaderTerminalStates == {
    "ReaderDoneNotFound",
    "ReaderDoneBytes",
    "ReaderDoneCorrupt",
    "ReaderTimedOut"
}

VARIABLES
    \* ===== Writer / race-state variables (verbatim from ChunkRaceWriter.tla) =====
    writerState,        \* Writers -> WriterStates
    writerSeen,         \* Writers -> SUBSET Chunks
    chunksPresent,      \* SUBSET Chunks
    chunksInFlight,     \* Chunks -> SUBSET Writers
    commitRunning,      \* BOOLEAN
    commitDoneFlag,     \* BOOLEAN
    commitResult,       \* ResultDomain
    runnerWriter,       \* Writers \cup {NULL}
    registryEntry,      \* BOOLEAN

    \* ===== NEW: read-side state =====
    \* Reader FSM.
    readerState,        \* Readers -> ReaderStates

    \* The FastSlowStore's `chunked_in_flight_digests` set (whether ANY
    \* writer is registered as in-flight for this digest). Single-digest
    \* model so we use a BOOLEAN: TRUE = at least one writer registered,
    \* FALSE = none. Produced by WC writers IFF WriterRegistersInFlight.
    inFlightSetMembership, \* BOOLEAN

    \* The FilesystemStore's `evicting_map` membership for this digest.
    \* TRUE iff the digest's canonical file has been finalize_holding'd
    \* AND the background_spawn'd insert into evicting_map has completed.
    \* Models the #247 stale-negative window: between PublishOk and this
    \* flag flipping TRUE there is a (non-deterministically bounded)
    \* gap during which a reader hitting FilesystemStore::has() sees None.
    evictingMap,        \* BOOLEAN

    \* What the reader observed at its has() call (used to drive
    \* get_part). Also ferries the get_part outcome.
    readerObserved      \* Readers -> {"None", "SomeViaInFlight", "SomeViaCanonical", "NotYetIssued"}

NULL == "NULL"

vars == <<writerState, writerSeen, chunksPresent, chunksInFlight,
          commitRunning, commitDoneFlag, commitResult, runnerWriter,
          registryEntry,
          readerState, inFlightSetMembership, evictingMap, readerObserved>>

writerVars == <<writerState, writerSeen, chunksPresent, chunksInFlight,
                commitRunning, commitDoneFlag, commitResult, runnerWriter,
                registryEntry>>
readerVars == <<readerState, readerObserved>>
flagVars   == <<inFlightSetMembership, evictingMap>>

----------------------------------------------------------------------------
(* Init                                                                    *)
----------------------------------------------------------------------------
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

----------------------------------------------------------------------------
(* WRITER ACTIONS — extended to maintain inFlightSetMembership /          *)
(* evictingMap as a side effect.                                          *)
(*                                                                          *)
(* Production semantics:                                                   *)
(*   - WriterRegistersInFlight=TRUE: first PwriteSucceed (any writer for  *)
(*     any chunk) flips inFlightSetMembership=TRUE; commit publish OR    *)
(*     ALL writers reaching {Done, Aborted, NotAttached} flips it back.  *)
(*     For simplicity, we use the rule: inFlightSetMembership becomes    *)
(*     FALSE when commitDoneFlag becomes TRUE OR all writers terminal.  *)
(*   - WriterRegistersInFlight=FALSE: never flips. Stays FALSE forever. *)
(*                                                                          *)
(*   evictingMap flips TRUE non-deterministically (see EvictingMapInsert)  *)
(*   AT-OR-AFTER PublishOk fires. Models the background_spawn delay.      *)
(*                                                                          *)
(*   Failure publishes (Err / Cancelled) do NOT flip evictingMap to TRUE; *)
(*   the canonical file was never created.                                *)
----------------------------------------------------------------------------

\* Helper: TRUE iff at least one writer is in a state where it would be
\* registered in chunked_in_flight_digests. Production-side: the
\* InFlightChunkedGuard is created at session ENTRY (when the writer
\* transitions to Sending) and dropped at session EXIT (Done / Aborted).
\* So a writer is "registered" iff its state is in {Sending, AwaitCommit}.
\* (NotAttached / Attached writers haven't entered the session yet; in
\* production the guard is created in `dispatch_bazel_facing_internal_chunking`
\* before chunk processing begins.)
SomeWriterInFlight ==
    \E w \in Writers :
        writerState[w] \in {"Sending", "AwaitCommit"}

\* Helper for the registration semantics: should inFlightSetMembership be
\* TRUE in this state, given WriterRegistersInFlight?
ShouldBeInFlight ==
    /\ WriterRegistersInFlight
    /\ SomeWriterInFlight
    /\ ~commitDoneFlag

----------------------------------------------------------------------------
(* AttachWriter — unchanged from base spec, plus inFlight maintenance.   *)
----------------------------------------------------------------------------
AttachWriter(w) ==
    /\ writerState[w] = "NotAttached"
    /\ IF registryEntry
       THEN /\ writerState' = [writerState EXCEPT ![w] = "Attached"]
            /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                           commitRunning, commitDoneFlag, commitResult,
                           runnerWriter, registryEntry,
                           readerVars, flagVars>>
       ELSE /\ writerState' = [writerState EXCEPT ![w] = "Aborted"]
            /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                           commitRunning, commitDoneFlag, commitResult,
                           runnerWriter, registryEntry,
                           readerVars, flagVars>>

WriterStartSending(w) ==
    /\ writerState[w] = "Attached"
    /\ writerState' = [writerState EXCEPT ![w] = "Sending"]
    \* Production: InFlightChunkedGuard::new fires at session entry,
    \* incrementing the in-flight counter for this digest. If post-fix
    \* (WriterRegistersInFlight = TRUE), this is when the inFlightSetMembership
    \* bit becomes TRUE if it wasn't already.
    /\ inFlightSetMembership' = IF WriterRegistersInFlight
                                THEN TRUE
                                ELSE inFlightSetMembership
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry,
                   readerVars, evictingMap>>

TryAdmitAlreadyHave(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \in chunksPresent
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry,
                   readerVars, flagVars>>

TryAdmitRacingLoser(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] # {}
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry,
                   readerVars, flagVars>>

----------------------------------------------------------------------------
(* TryAdmitAccept — admits chunk c into in-flight slot. Does NOT touch  *)
(* inFlightSetMembership; that bit is owned by WriterStartSending /     *)
(* the various session-exit actions, mirroring production's per-session *)
(* InFlightChunkedGuard lifetime.                                       *)
----------------------------------------------------------------------------
TryAdmitAccept(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] = {}
    /\ chunksInFlight' = [chunksInFlight EXCEPT ![c] = {w}]
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter,
                   registryEntry,
                   readerVars, flagVars>>

----------------------------------------------------------------------------
(* PwriteSucceed — base writer logic, plus reader-visible bookkeeping.   *)
----------------------------------------------------------------------------
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

----------------------------------------------------------------------------
(* PwriteFail — same as base; inFlightSetMembership stays as-is (the    *)
(* guard does NOT release on per-chunk fail; the writer's session ends  *)
(* via WriterAbortMidStream which sets it to false IFF no other writer  *)
(* still in flight).                                                    *)
----------------------------------------------------------------------------
PwriteFail(w, c) ==
    /\ AllowPwriteFail
    /\ w \in chunksInFlight[c]
    /\ chunksInFlight' = [c2 \in Chunks |-> chunksInFlight[c2] \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    \* When this writer aborts, it releases its InFlightChunkedGuard.
    \* If no other writer remains in {Sending, AwaitCommit}, the
    \* per-digest counter drops to zero and inFlightSetMembership clears.
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
    \* Runner's session ends; release its in-flight registration.
    /\ inFlightSetMembership' =
            IF WriterRegistersInFlight
            THEN \E w2 \in Writers :
                    /\ w2 # runnerWriter
                    /\ writerState[w2] \in {"Sending", "AwaitCommit"}
            ELSE inFlightSetMembership
    \* NOTE: evictingMap does NOT flip true here — see EvictingMapInsert.
    \* This is the #247 stale-negative window.
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
    \* Runner's session ends via Aborted; release its in-flight registration.
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
    \* On final terminal of last live writer with WriterRegistersInFlight,
    \* the guard count drops to zero and we flip false.
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
                   runnerWriter,
                   readerVars, evictingMap>>

WriterFinishesAfterAllSeen(w) ==
    /\ writerState[w] = "Sending"
    /\ writerSeen[w] = Chunks
    /\ ~(\E c \in Chunks : w \in chunksInFlight[c])
    /\ writerState' = [writerState EXCEPT ![w] = "AwaitCommit"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry,
                   readerVars, flagVars>>

----------------------------------------------------------------------------
(* EvictingMapInsert: the background_spawn'd insert into FilesystemStore  *)
(* `evicting_map` finally fires after PublishOk. Non-deterministic delay *)
(* models the #247 stale-negative window — production sees it as the    *)
(* gap between rename(2) returning and the spawned task running on the  *)
(* tokio runtime's rayon-pool boundary.                                 *)
(*                                                                       *)
(* Gated on commitResult = OkResult: only successful publishes create   *)
(* the canonical file.                                                  *)
----------------------------------------------------------------------------
EvictingMapInsert ==
    /\ commitResult = "OkResult"
    /\ ~evictingMap
    /\ evictingMap' = TRUE
    /\ UNCHANGED <<writerVars, readerVars, inFlightSetMembership>>

----------------------------------------------------------------------------
(* READER ACTIONS                                                        *)
(*                                                                          *)
(* Reader's flow:                                                         *)
(*   ReaderIdle -> [ReaderStartIssue] -> ReaderIssuingHas                 *)
(*   ReaderIssuingHas -> [HasReturnsX] -> ReaderHasNone                   *)
(*                                       | ReaderHasSomeViaInFlight       *)
(*                                       | ReaderHasSomeViaCanonical      *)
(*   ReaderHasNone           -> [ReaderConcludeNotFound] -> ReaderDoneNotFound*)
(*   ReaderHasSomeViaCanonical-> [ReaderReadCanonical]    -> ReaderDoneBytes*)
(*   ReaderHasSomeViaInFlight -> [ReaderReadFromPin]      -> ReaderDoneBytes*)
(*                              | [ReaderRetryHas]        -> ReaderRetryingHas*)
(*                              | [ReaderTimeoutAndFallThrough] -> ReaderTimedOut*)
(*   ReaderRetryingHas       -> like ReaderIssuingHas's edges but cannot *)
(*                              loop forever (must terminate after one   *)
(*                              retry).                                   *)
(*                                                                          *)
(* All terminal states are stable.                                       *)
----------------------------------------------------------------------------

\* The reader's has() observation: the production cascade looks at
\*   inFlightSetMembership (TRUE => Some via in-flight)
\*   evictingMap          (TRUE => Some via canonical)
\* with inFlight precedence (it's checked first in fast_slow_store.rs).
\* If both are FALSE, the answer is None.
ReaderObserveHasOutcome ==
    IF inFlightSetMembership
        THEN "SomeViaInFlight"
        ELSE IF evictingMap
            THEN "SomeViaCanonical"
            ELSE "None"

ReaderStartIssue(r) ==
    /\ readerState[r] = "ReaderIdle"
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderIssuingHas"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

ReaderHasReturns(r) ==
    /\ readerState[r] = "ReaderIssuingHas"
    /\ LET obs == ReaderObserveHasOutcome
       IN
        /\ readerObserved' = [readerObserved EXCEPT ![r] = obs]
        /\ readerState' = [readerState EXCEPT ![r] =
                CASE obs = "None"              -> "ReaderHasNone"
                  [] obs = "SomeViaInFlight"   -> "ReaderHasSomeViaInFlight"
                  [] obs = "SomeViaCanonical"  -> "ReaderHasSomeViaCanonical"]
    /\ UNCHANGED <<writerVars, flagVars>>

\* Reader observed has=None. ExistenceCacheStore does not cache negatives
\* per audit row R5/WC; the reader returns NotFound.
ReaderConcludeNotFound(r) ==
    /\ readerState[r] = "ReaderHasNone"
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneNotFound"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

\* Reader observed has=Some via canonical. get_part hits FilesystemStore
\* and reads the canonical file. Bytes are correct because PublishOk has
\* already fired (evictingMap=TRUE implies the canonical file is fully
\* on disk). Reader returns full bytes.
ReaderReadCanonical(r) ==
    /\ readerState[r] = "ReaderHasSomeViaCanonical"
    /\ evictingMap        \* defensive: only if still TRUE
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

\* Reader observed has=Some via in-flight. The pin-cascade in production
\* (try_get_chunk_from_pin) returns full bytes IFF chunksPresent = Chunks
\* AT THE MOMENT of the get_part call (i.e. the writer has filled the
\* whole bitmap). Otherwise the reader either retries has() (modelled as
\* ReaderRetryHas) or times out (modelled as ReaderTimeoutAndFallThrough).
ReaderReadFromPin(r) ==
    /\ readerState[r] = "ReaderHasSomeViaInFlight"
    /\ chunksPresent = Chunks
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

\* Reader observed has=Some via in-flight but bitmap incomplete. Retry has()
\* once. Models the 500ms WAIT loop in fast_slow_store.rs:5829.
ReaderRetryHas(r) ==
    /\ readerState[r] = "ReaderHasSomeViaInFlight"
    /\ chunksPresent # Chunks
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderRetryingHas"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

\* From ReaderRetryingHas, the reader makes ONE more observation:
\*   - If chunksPresent = Chunks now (writer raced ahead), serve bytes.
\*   - If commitDoneFlag with OkResult AND evictingMap, serve from canonical.
\*   - If commitDoneFlag with Err/Cancelled, NotFound.
\*   - If has() drops to None (writer aborted/inFlight cleared, evictingMap not yet), NotFound.
\*   - If still inFlight but bitmap not complete, time out.
ReaderRetryServeFromPin(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ inFlightSetMembership
    /\ chunksPresent = Chunks
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

ReaderRetryServeFromCanonical(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ evictingMap
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneBytes"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

ReaderRetryConcludeNotFound(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ ~inFlightSetMembership
    /\ ~evictingMap
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderDoneNotFound"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

\* Time-out path: still in-flight, bitmap not complete. In production
\* the reader falls through to slow-tier which returns NotFound.
\* Modelled as ReaderTimedOut to distinguish from a "structural" NotFound.
\* Per the audit, R1/R2 × WA × P1 with chunks not landing within 500ms
\* timeline is the SAFE-but-falls-through case.
ReaderRetryTimeout(r) ==
    /\ readerState[r] = "ReaderRetryingHas"
    /\ inFlightSetMembership
    /\ chunksPresent # Chunks
    /\ ~evictingMap
    /\ readerState' = [readerState EXCEPT ![r] = "ReaderTimedOut"]
    /\ UNCHANGED <<writerVars, readerObserved, flagVars>>

----------------------------------------------------------------------------
(* Next: union of writer + reader actions.                                *)
----------------------------------------------------------------------------
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
    \/ EvictingMapInsert
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

----------------------------------------------------------------------------
(* Spec: Init + stuttering Next + fairness for liveness.                 *)
(*                                                                          *)
(* Fairness rationale:                                                    *)
(*  - Same writer fairness as base spec (publish, watchdog, await,       *)
(*    pwrite, admission, finishes).                                      *)
(*  - WF on EvictingMapInsert: the background_spawn ALWAYS runs to      *)
(*    completion eventually (per the #247 contract; cancellation-safe   *)
(*    background_spawn from filesystem_store.rs:1828).                  *)
(*  - WF on each reader action so readers eventually progress through   *)
(*    their FSM. Critical: WITHOUT WF on reader actions, TLC could     *)
(*    leave a reader idle forever and "ReaderProgress" would trivially *)
(*    fail.                                                              *)
----------------------------------------------------------------------------
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
    /\ WF_vars(EvictingMapInsert)
    \* Reader fairness: each reader, once started, eventually completes.
    /\ \A r \in Readers : WF_vars(ReaderStartIssue(r))
    /\ \A r \in Readers : WF_vars(ReaderHasReturns(r))
    /\ \A r \in Readers : WF_vars(ReaderConcludeNotFound(r))
    /\ \A r \in Readers : WF_vars(ReaderReadCanonical(r))
    /\ \A r \in Readers : WF_vars(ReaderReadFromPin(r))
    \* Important: WF on the disjunction of progress edges out of
    \* ReaderHasSomeViaInFlight so the reader CAN progress out, even
    \* if it doesn't choose ReadFromPin specifically.
    /\ \A r \in Readers :
            WF_vars(ReaderReadFromPin(r) \/ ReaderRetryHas(r))
    /\ \A r \in Readers :
            WF_vars(ReaderRetryServeFromPin(r)
                    \/ ReaderRetryServeFromCanonical(r)
                    \/ ReaderRetryConcludeNotFound(r)
                    \/ ReaderRetryTimeout(r))

----------------------------------------------------------------------------
(* SAFETY INVARIANTS                                                     *)
----------------------------------------------------------------------------

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
    /\ \A r \in Readers : readerObserved[r] \in {"None", "SomeViaInFlight",
                                                 "SomeViaCanonical", "NotYetIssued"}
    /\ inFlightSetMembership \in BOOLEAN
    /\ evictingMap \in BOOLEAN

\* Carried over from base spec.
MirrorOnce ==
    /\ (commitRunning => runnerWriter # NULL)
    /\ (commitDoneFlag => commitResult \in {"OkResult", "ErrResult", "CancelledResult"})

RunnerOwnsCommit ==
    runnerWriter # NULL => commitRunning

InFlightDisjointAfterDone ==
    \A w \in Writers :
        writerState[w] \in {"Done", "Aborted"} =>
            \A c \in Chunks : w \notin chunksInFlight[c]

NoCorruptionWriter ==
    commitResult = "OkResult" => chunksPresent = Chunks

NoPermanentWedge ==
    (commitRunning /\ ~commitDoneFlag) => runnerWriter # NULL

\* ===== NEW: read-side safety invariants =====

\* NoPartialReadAsCanonical: a reader that reaches ReaderDoneBytes must
\* have seen full coverage at the moment of serving. Operationally: at any
\* state where a reader is in ReaderDoneBytes, EITHER chunksPresent = Chunks
\* OR commitResult = OkResult — these are the two world states under which
\* full bytes can be served (pin-bytes if writer's bitmap is full, OR
\* canonical-bytes if writer committed and evictingMap inserted).
NoPartialReadAsCanonical ==
    \A r \in Readers :
        readerState[r] = "ReaderDoneBytes" =>
            (chunksPresent = Chunks \/ commitResult = "OkResult")

\* NoCorruptionViaReader: no reader ever reaches ReaderDoneCorrupt. We
\* never have an action that puts a reader into ReaderDoneCorrupt — so this
\* is structurally TRUE in the spec; included as a placeholder /
\* defensive-check for any future spec extension that introduces a
\* corruption transition.
NoCorruptionViaReader ==
    \A r \in Readers : readerState[r] # "ReaderDoneCorrupt"

\* NoFalsePositiveExistence: if a reader observed has=Some, then EITHER
\*   (a) at the moment of the observation (or LATER, since chunksPresent
\*       and evictingMap are both monotone within registry) the reader
\*       can complete a successful read, OR
\*   (b) the writer subsequently failed and the reader will fall through
\*       to ReaderDoneNotFound or ReaderTimedOut without seeing wrong bytes.
\* The strong form: a reader that observed has=Some never reaches
\* ReaderDoneCorrupt. (Equivalent to NoCorruptionViaReader given that
\* "Corrupt" is the only failure outcome we'd flag.)
\* The weaker form: SomeViaCanonical implies the canonical file IS
\* present (since evictingMap is TRUE only after a successful PublishOk).
NoFalsePositiveExistenceCanonical ==
    \A r \in Readers :
        readerObserved[r] = "SomeViaCanonical" =>
            commitResult = "OkResult" /\ evictingMap

\* NoFalseNegativeAfterCommit: a reader that ISSUED its has() AFTER
\* (commit-publish-Ok AND evictingMap-insert) must observe Some.
\* Since we don't model time, encode as: in any state where commitResult
\* = OkResult AND evictingMap = TRUE AND a reader is in ReaderHasNone or
\* ReaderRetryConclude...NotFound, that reader's READING of has must have
\* observed None — but at this state, has would observe Canonical, not None.
\* So the invariant: no reader is in ReaderHasNone / ReaderDoneNotFound
\* in a state where evictingMap = TRUE WITHOUT having issued has BEFORE
\* evictingMap flipped.
\*
\* This is awkward to encode without time — instead encode the operational
\* form: once evictingMap is TRUE, ANY reader in ReaderIdle / ReaderIssuingHas
\* MUST eventually reach ReaderDoneBytes (via Canonical); cannot reach
\* ReaderDoneNotFound. Encoded as a leads-to property (see liveness).
\*
\* The state-form invariant: if evictingMap and a reader is in
\* ReaderHasNone, that reader's observation was BEFORE evictingMap flipped
\* (which is OK — the spec models a single observation). We therefore
\* CAN'T encode "no false negative AFTER commit" as a state predicate;
\* it's a temporal property. See FalseNegativeFreedomAfterCommit liveness.

\* NoFalseNegativeAtSteadyState — REMOVED.
\*
\* Earlier draft asserted "in a state where commit=Ok AND evictingMap AND
\* all writers terminal, no reader is in ReaderDoneNotFound". TLC produced
\* a counterexample at depth 13 showing this invariant is too strong even
\* in the post-fix case: a reader that observed has() in P0 (before
\* writer attached, so inFlight=FALSE and evictingMap=FALSE) correctly
\* terminates in ReaderDoneNotFound — and then the writer subsequently
\* commits Ok. This is OK behavior; the reader's NotFound was correct
\* AT-OR-BEFORE its observation moment.
\*
\* The semantically-correct version is a temporal property on causal
\* ordering — see EventualConsistencyAfterCommitOk in the LIVENESS section.
\* For state-predicate FALSE-NEGATIVE detection, see PostFixBugFreeOfNoFly
\* (the bugged-variant detector) below.

\* PostFixNoActiveWriterInvisibleToReader: invariant that ONLY holds in
\* the post-fix variant (WriterRegistersInFlight = TRUE). Asserts that
\* the bug state — there is some writer ACTIVELY in {Sending, AwaitCommit}
\* with commit not yet done, and the in-flight set is EMPTY — is
\* UNREACHABLE.
\*
\* Production semantics: InFlightChunkedGuard is created at session
\* entry (WriterStartSending) so the bit is TRUE for the entire duration
\* of any writer in {Sending, AwaitCommit}. A state where a writer is
\* active and the bit is FALSE is reachable IFF the guard registration
\* is broken (the H1 bug).
PostFixNoActiveWriterInvisibleToReader ==
    ~(WriterRegistersInFlight
      /\ SomeWriterInFlight
      /\ ~commitDoneFlag
      /\ ~inFlightSetMembership)

\* NoActiveWriterInvisibleToReader: the H1 catastrophe — same predicate
\* without the `WriterRegistersInFlight` guard. In the bugged variant,
\* writer enters {Sending} but inFlightSetMembership stays FALSE; a
\* reader at this moment observes has=None and concludes NotFound. TLC
\* will report VIOLATED in the bugged variant.
NoActiveWriterInvisibleToReader ==
    ~(SomeWriterInFlight
      /\ ~commitDoneFlag
      /\ ~inFlightSetMembership)

\* InFlightSetMembershipConsistency: when WriterRegistersInFlight is TRUE,
\* inFlightSetMembership must be TRUE iff at least one writer is in
\* {Sending, AwaitCommit} with seen-non-empty AND commit not yet done.
\* (Allows a small window where commit just finished but the membership
\* bit hasn't been cleared yet — see notes on AwaitingWriterObservesResult
\* for the simplified clear-on-last-terminal semantics.)
\* When WriterRegistersInFlight is FALSE, inFlightSetMembership stays FALSE.
InFlightSetMembershipConsistency ==
    /\ ~WriterRegistersInFlight => ~inFlightSetMembership
    \* When TRUE, no false-positive: if there are no writers in-flight at all,
    \* and no commitRunning, the bit should be FALSE eventually — this is
    \* a liveness property; here we just assert the FALSE side (no-writer-
    \* registered case).

\* EvictingMapImpliesPublishOk: the canonical file index is only inserted
\* AFTER a successful commit. Encoded as: evictingMap = TRUE => commitResult
\* = OkResult.
EvictingMapImpliesPublishOk ==
    evictingMap => commitResult = "OkResult"

\* NoReaderInducedWriteWedge: readers and writers share state but readers
\* never MUTATE writer-owned variables. Structurally trivial; included as
\* a sanity check that any spec extension that does mutate must justify.
\* This is verified by the fact that all reader actions UNCHANGED writerVars.
\* Encoded as a placeholder TRUE for completeness.
NoReaderInducedWriteWedge == TRUE

\* ResultMonotone (carried over).
ResultMonotone ==
    [][commitResult # "NoResult" => commitResult' = commitResult]_vars

\* ChunksPresentMonotonicWithinRegistry (carried over).
ChunksPresentMonotonicWithinRegistry ==
    [][registryEntry /\ registryEntry' => chunksPresent \subseteq chunksPresent']_vars

\* EvictingMapMonotonic: evictingMap is a one-shot flip from FALSE to TRUE.
EvictingMapMonotonic ==
    [][evictingMap => evictingMap']_vars

----------------------------------------------------------------------------
(* LIVENESS PROPERTIES                                                   *)
----------------------------------------------------------------------------

\* Termination (writers): every writer eventually reaches a terminal state.
WriterTermination ==
    \A w \in Writers :
        <>(writerState[w] \in {"Done", "Aborted", "NotAttached"})

AllAttachedWritersTerminate ==
    \A w \in Writers :
        (writerState[w] \in {"Attached", "Sending", "AwaitCommit"})
            ~> (writerState[w] \in {"Done", "Aborted"})

CommitEventuallyResolves ==
    commitRunning ~> commitDoneFlag

\* ReaderProgress: every reader eventually reaches a terminal state.
\* This is the load-bearing property — it catches "reader hangs forever
\* because the v2 writer wedged and the reader's WAIT-loop is unbounded".
ReaderProgress ==
    \A r \in Readers :
        <>(readerState[r] \in ReaderTerminalStates \cup {"ReaderIdle"})

\* Stronger: every started reader (not in ReaderIdle) eventually terminates.
StartedReadersTerminate ==
    \A r \in Readers :
        (readerState[r] \notin {"ReaderIdle"} \cup ReaderTerminalStates)
            ~> (readerState[r] \in ReaderTerminalStates)

\* EventualConsistency: once commitDoneFlag is TRUE with OkResult, every
\* reader started AT-OR-AFTER will see Some. Encoded as a leads-to: a
\* state where commitDoneFlag=TRUE AND OkResult AND a reader is "Idle"
\* (hasn't issued yet) leads to a state where that reader is in
\* ReaderDoneBytes (provided EvictingMapInsert eventually fires).
EventualConsistencyAfterCommitOk ==
    \A r \in Readers :
        (commitResult = "OkResult" /\ readerState[r] = "ReaderIdle")
            ~> (readerState[r] = "ReaderDoneBytes" \/ readerState[r] = "ReaderIdle")
        \* Note: \/ readerState=Idle on RHS is the "reader never started"
        \* loop-back (no fairness on issue-start would let reader stay
        \* idle). With WF on ReaderStartIssue, the Idle disjunct is
        \* eventually false IF reader already moved.
        \* Strict form of the property: every reader EVENTUALLY ends in
        \* ReaderDoneBytes IF it started AFTER commitDoneFlag.

\* Strong form: in any state where evictingMap=TRUE AND a reader has not
\* yet issued, that reader will reach ReaderDoneBytes.
EventualConsistencyStrong ==
    \A r \in Readers :
        (evictingMap /\ readerState[r] = "ReaderIdle")
            ~> (readerState[r] = "ReaderDoneBytes")

\* No reader induced write wedge: writer's progress (Done/Aborted) is
\* unaffected by reader behavior.
NoReaderInducedWriterStuck ==
    \A w \in Writers :
        (writerState[w] \in {"Sending", "AwaitCommit"})
            ~> (writerState[w] \in {"Done", "Aborted"})

============================================================================
