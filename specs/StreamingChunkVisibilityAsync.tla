-------------------- MODULE StreamingChunkVisibilityAsync --------------------
(***************************************************************************
  ATOMICITY-AUDIT de-atomization of StreamingBlobReadDuringWrite.tla's
  `WriterSendChunk` action (batch B, chunk-race/streaming).

  WHAT THE PARENT SPEC FUSED
    StreamingBlobReadDuringWrite.tla models one `send_chunk` call as a
    single indivisible atom:
        WriterSendChunk ==
            /\ chunkCount'   = chunkCount + 1
            /\ bytesWritten' = bytesWritten + 1
    The parent spec's own comment (lines 610-629) flags this as a fused
    atom and DEFERS de-atomizing it to "a future spec extension that
    splits WriterSendChunk into two sub-actions". This IS that extension,
    plus the third real sub-step the parent omitted entirely: the physical
    push of the chunk bytes into the `chunks` VecDeque.

  THE THREE REAL SUB-STEPS IN PRODUCTION (streaming_blob.rs:692-695)
        { let mut chunks = self.inner.chunks.write();
          chunks.push_back(chunk); }                 // (1) BYTES VISIBLE
        self.inner.chunk_count.fetch_add(1, Release);// (2) COUNT VISIBLE
        self.inner.bytes_written.fetch_add(len, ...);// (3) BYTES_WRITTEN
    These are THREE separate memory operations, NOT one atom. A concurrent
    reader in `next_chunk` gates chunk-availability on
        cursor_chunk_idx < chunk_count            (streaming_blob.rs:874)
    then acquires `chunks.read()` and indexes the deque. The ORDERING
    (push_back BEFORE the chunk_count bump) is what makes it safe: a reader
    that observes chunk_count = N+1 is guaranteed the N-th chunk's bytes
    are already in the deque.

  WHY THIS ORDERING IS LOAD-BEARING (and why fusing it hid the proof)
    The team already ate a production incident from the sibling ordering
    hazard in the SAME function: the `earliest_chunk_idx` / `pop_front`
    eviction race (#515 "frankenstein bytes", SHA 374e3cf4, pipeline 3266,
    2026-06-12) — see the LOCK-INVARIANT comment at streaming_blob.rs:707.
    That is direct evidence that a reader observing a counter ahead of the
    buffer it indexes is a REAL, shipped fault class here. The fused atom
    in the parent spec makes the "count ahead of buffer" state UNREACHABLE,
    so any invariant over it passes VACUOUSLY, and a future refactor that
    moves `push_back` AFTER `chunk_count.fetch_add` would leave the parent
    spec GREEN while shipping a phantom-chunk read.

  WHAT THIS SPEC SPLITS
    `send_chunk` becomes two indivisible sub-actions with an intermediate
    "mid-chunk" state so TLC can interleave a reader between them:
        SendPushBuffer  — push_back lands the bytes (bufferLen++)
        SendBumpCount   — chunk_count.fetch_add makes the chunk countable
    A CONSTANT toggle picks the ordering:
        PushBeforeCount = TRUE   (FIXED, production ordering):
            push_back THEN bump count. A reader observing
            cursor < chunkCount always finds the bytes buffered.
        PushBeforeCount = FALSE  (BUGGED, hypothetical refactor):
            bump count THEN push_back. Between the two sub-steps a reader
            observes chunkCount = N+1 while bufferLen = N and indexes a
            chunk that isn't in the deque yet -> phantom read.

  EXPECTED TLC OUTCOMES
    StreamingChunkVisibilityAsyncFixed.cfg  (PushBeforeCount = TRUE):
        INVARIANTS HOLD. CountNeverAheadOfBuffer holds (bufferLen is
        bumped first, so bufferLen >= chunkCount always); no reader ever
        reaches DonePhantom.
    StreamingChunkVisibilityAsyncBugged.cfg (PushBeforeCount = FALSE):
        INVARIANT VIOLATED. CountNeverAheadOfBuffer fails in the mid-chunk
        state; the companion NoReaderPhantom fails when a reader interleaves
        and indexes the not-yet-buffered chunk (DonePhantom).

  REAL-CODE ANALOG OF THE SURFACED BUG
    A refactor of `send_chunk` that hoists `chunk_count.fetch_add` above
    the `chunks.push_back` (e.g. "bump the counter early so notify_waiters
    sees the new count", a plausible micro-opt) reopens exactly the phantom
    read a reader would hit: next_chunk sees cursor_chunk_idx < chunk_count,
    acquires chunks.read(), and indexes past the end of the deque (or reads
    a stale/evicted slot). The FIX is the invariant this spec proves
    load-bearing: push_back MUST precede the chunk_count bump.

  SCOPE
    One writer (production: exactly one StreamingBlobWriter per inner),
    a small chunk budget, N readers each with a cursor. bytes_written and
    terminal classification are OUT OF SCOPE here (the parent spec covers
    the #500/#502 terminal seams); this spec isolates the buffer-visibility
    ordering only, which is the sub-step the parent omitted.

  CITATIONS
    [send]  nativelink-util/src/streaming_blob.rs:688-720 (send_chunk)
    [gate]  nativelink-util/src/streaming_blob.rs:874     (reader gate)
    [515]   nativelink-util/src/streaming_blob.rs:707     (LOCK-INVARIANT)
    [parent] specs/StreamingBlobReadDuringWrite.tla:610-629 (deferred split)
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Readers,          \* set of reader ids, e.g. {r1}
    NumChunks,        \* Nat >= 1: chunks the writer will send
    PushBeforeCount   \* TRUE = production ordering (fix); FALSE = reordered (bug)

ASSUME Cardinality(Readers) >= 1
ASSUME NumChunks \in Nat \ {0}
ASSUME PushBeforeCount \in BOOLEAN

\* Writer send-phase: "Idle" = between chunks; "MidChunk" = one sub-step of
\* the current send_chunk has fired but not the other.
SendPhases == {"Idle", "MidChunk", "Eof"}

\* Reader FSM. DoneOk = consumed all chunks + saw eof. DonePhantom = the
\* reader's cursor was < chunkCount but the chunk's bytes were NOT yet in
\* the buffer (bufferLen), so it indexed a phantom chunk.
ReaderStates == {"Subscribed", "DoneOk", "DonePhantom"}

VARIABLES
    bufferLen,    \* Nat: chunks physically in the `chunks` VecDeque
    chunkCount,   \* Nat: the chunk_count atomic (reader's availability gate)
    sendPhase,    \* SendPhases
    terminal,     \* BOOLEAN: send_eof fired
    readerState,  \* Readers -> ReaderStates
    readerCursor  \* Readers -> Nat: next chunk index to read

vars == <<bufferLen, chunkCount, sendPhase, terminal, readerState, readerCursor>>

----------------------------------------------------------------------------
Init ==
    /\ bufferLen    = 0
    /\ chunkCount   = 0
    /\ sendPhase    = "Idle"
    /\ terminal     = FALSE
    /\ readerState  = [r \in Readers |-> "Subscribed"]
    /\ readerCursor = [r \in Readers |-> 0]

----------------------------------------------------------------------------
(* WRITER: send_chunk split into two indivisible sub-steps.                 *)
(*                                                                          *)
(* The invariant that must hold across the mid-chunk gap: a reader observing*)
(* chunkCount must find the corresponding bytes already in bufferLen. Under *)
(* PushBeforeCount = TRUE the push lands first, preserving it; under FALSE  *)
(* the count is bumped first, opening the gap.                             *)
----------------------------------------------------------------------------

\* Begin a new send_chunk: fire the FIRST sub-step for the current ordering.
SendFirstSubStep ==
    /\ sendPhase = "Idle"
    /\ chunkCount < NumChunks
    /\ IF PushBeforeCount
       THEN \* FIXED: push_back the bytes first.
            /\ bufferLen' = bufferLen + 1
            /\ UNCHANGED chunkCount
       ELSE \* BUGGED: bump the countable index first.
            /\ chunkCount' = chunkCount + 1
            /\ UNCHANGED bufferLen
    /\ sendPhase' = "MidChunk"
    /\ UNCHANGED <<terminal, readerState, readerCursor>>

\* Complete the send_chunk: fire the SECOND sub-step.
SendSecondSubStep ==
    /\ sendPhase = "MidChunk"
    /\ IF PushBeforeCount
       THEN \* FIXED: now make it countable.
            /\ chunkCount' = chunkCount + 1
            /\ UNCHANGED bufferLen
       ELSE \* BUGGED: now the bytes finally land.
            /\ bufferLen' = bufferLen + 1
            /\ UNCHANGED chunkCount
    /\ sendPhase' = "Idle"
    /\ UNCHANGED <<terminal, readerState, readerCursor>>

\* send_eof after all chunks are fully sent (both sub-steps done for each).
SendEof ==
    /\ sendPhase = "Idle"
    /\ chunkCount = NumChunks
    /\ bufferLen  = NumChunks
    /\ terminal'  = TRUE
    /\ sendPhase' = "Eof"
    /\ UNCHANGED <<bufferLen, chunkCount, readerState, readerCursor>>

----------------------------------------------------------------------------
(* READER: gate on chunkCount, then index the buffer.                       *)
(*                                                                          *)
(* This mirrors next_chunk: the availability check is `cursor < chunk_count`*)
(* (streaming_blob.rs:874); the reader THEN acquires chunks.read() and      *)
(* indexes. If cursor >= bufferLen at that point, the indexed chunk is not  *)
(* physically present -> phantom read.                                     *)
----------------------------------------------------------------------------
ReaderConsume(r) ==
    /\ readerState[r] = "Subscribed"
    /\ readerCursor[r] < chunkCount   \* availability gate reads chunk_count
    /\ IF readerCursor[r] < bufferLen
       THEN \* bytes physically present: valid read, advance cursor.
            /\ readerCursor' = [readerCursor EXCEPT ![r] = @ + 1]
            /\ UNCHANGED readerState
       ELSE \* counted but NOT buffered: phantom chunk read.
            /\ readerState' = [readerState EXCEPT ![r] = "DonePhantom"]
            /\ UNCHANGED readerCursor
    /\ UNCHANGED <<bufferLen, chunkCount, sendPhase, terminal>>

\* Reader caught up to all chunks and saw eof: clean completion.
ReaderObserveEof(r) ==
    /\ readerState[r] = "Subscribed"
    /\ terminal
    /\ readerCursor[r] = chunkCount
    /\ readerState' = [readerState EXCEPT ![r] = "DoneOk"]
    /\ UNCHANGED <<bufferLen, chunkCount, sendPhase, terminal, readerCursor>>

----------------------------------------------------------------------------
Next ==
    \/ SendFirstSubStep
    \/ SendSecondSubStep
    \/ SendEof
    \/ \E r \in Readers : ReaderConsume(r)
    \/ \E r \in Readers : ReaderObserveEof(r)

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(SendFirstSubStep)
    /\ WF_vars(SendSecondSubStep)
    /\ WF_vars(SendEof)
    /\ \A r \in Readers : WF_vars(ReaderConsume(r))
    /\ \A r \in Readers : WF_vars(ReaderObserveEof(r))

----------------------------------------------------------------------------
(* INVARIANTS                                                               *)
----------------------------------------------------------------------------
TypeOK ==
    /\ bufferLen  \in 0..NumChunks
    /\ chunkCount \in 0..NumChunks
    /\ sendPhase  \in SendPhases
    /\ terminal   \in BOOLEAN
    /\ \A r \in Readers : readerState[r] \in ReaderStates
    /\ \A r \in Readers : readerCursor[r] \in 0..NumChunks

\* The load-bearing ordering invariant: the countable index never runs
\* ahead of the physically-buffered bytes. Holds iff push_back precedes the
\* chunk_count bump (PushBeforeCount = TRUE).
CountNeverAheadOfBuffer ==
    chunkCount <= bufferLen

\* Reader-visible consequence: no reader ever indexes a phantom chunk.
NoReaderPhantom ==
    \A r \in Readers : readerState[r] # "DonePhantom"

----------------------------------------------------------------------------
(* LIVENESS                                                                 *)
----------------------------------------------------------------------------
\* Every reader eventually completes cleanly (only provable in the FIXED
\* ordering; listed in the Fixed cfg only).
ReadersEventuallyOk ==
    \A r \in Readers : <>(readerState[r] = "DoneOk")

============================================================================
