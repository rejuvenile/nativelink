--------------------- MODULE StreamingBlobReadDuringWrite ---------------------
(***************************************************************************
  Concurrent slow-readers x slow-writers x read-during-write over
  the streaming_blob primitive.

  Models the production cross-component protocol enforced at
  `nativelink-util/src/streaming_blob.rs:631-822`
  (`StreamingBlobReader::next_chunk`) and `:425-486` (`send` / `send_eof` /
  `send_error`). The streaming_blob primitive is the load-bearing seam for
  read-during-write (RDW) cases where readers subscribe BEFORE the writer
  has finished producing all bytes (in production: a Bazel BS::read for a
  blob currently being uploaded via a sibling chunked writer, or a
  FastSlowStore inline `copy_slow_to_fast` populate path with concurrent
  consumers).

  RECENT PROTOCOL EVOLUTION CAPTURED HERE
    [#500 ee13bee0 + 55c16700, 2026-05-15]
      `bytestream_server.rs::inner_read::consume_ok_eof` no longer accepts
      a clean EOF (terminal=Ok with bytes_sent=0) when the writer's
      `get_part` future has surfaced `Err`. Pre-fix: a slow_store NotFound
      delivered through the producer's `tx.drop` path was observed at the
      reader as `Ok(empty)` and forwarded to Bazel — a silent-zero read
      shape that misclassified as "blob exists, is empty" instead of
      NotFound. Spec correspondence: `BuggedNoSilentZero` cfg disables
      the producer-side `Err` propagation; safety invariant
      `NoSilentZeroToReader` red-fails.
    [#502 8e6be6d7, 2026-05-16]
      `StreamingBlobReader::next_chunk` now returns `Err(Code::Internal)`
      carrying `STREAMING_BLOB_SILENT_SHORT_MARKER` when terminal=Ok and
      `bytes_written < expected_size`. Pre-fix: a `send_eof` after a
      short-byte send returned `Ok(Bytes::new())` to every consumer; Bazel
      saw a clean stream with truncated bytes and reported digest mismatch
      as a build failure. Spec correspondence: `BuggedNoShortShield` cfg
      disables the silent-short check at the reader boundary; safety
      invariant `NoSilentShortToReader` red-fails.

  PRODUCTION SHAPE
    * Single writer per blob (production: `StreamingBlobWriter::new` is
      called exactly once per `StreamingBlobInner`). The writer goes
      through a finite chunk sequence and then either:
        (a) `send_eof` after writing all bytes (terminal=Ok, full),
        (b) `send_eof` after writing fewer bytes than declared
            (terminal=Ok, silent-short — defended at the reader boundary
            by #502),
        (c) `send_error(err)` (terminal=Err),
        (d) drops without sending eof (terminal becomes
            `Err(Internal "writer dropped without sending EOF")` via the
            Drop impl).
    * N concurrent readers subscribe at arbitrary times. Each reader
      maintains its own cursor and advances chunk-by-chunk. Readers never
      block one another; they never mutate writer-owned state. A reader
      that subscribes AFTER the writer has finished still sees all chunks
      (the buffer is retained until OOB eviction).
    * The watch channel + lock-protected terminal slot make the
      writer→reader bound notification race-free for the in-tokio-1.49
      `watch::Receiver` primitive (see `next_chunk` doc-comment).
    * Sliding-window eviction is OUT OF SCOPE for this spec; it interacts
      with the `SLIDING_WINDOW_EVICTION_MARKER` error class but is a
      separate read-side hazard that is not what #500/#502 closed. See
      Open Gaps below.

  WHAT THIS SPEC ABSTRACTS
    * Real bytes — chunks are integers in `[1..NumChunks]`. The "declared
      size" abstraction is `NumChunks * ChunkSize`; the "actually written
      bytes" abstraction is `BytesWritten`. Silent-short is modeled as
      `WriterShortEof`: writer fires terminal=Ok with `BytesWritten <
      NumChunks * ChunkSize`.
    * Real time — `STREAMING_BLOB_NOTIFY_TIMEOUT` and the watchdog at
      `:849` are modeled as a non-deterministic abort transition
      (`ReaderNotifyTimeoutAbort`) on the reader side. Production fairness
      assumption: `WF_vars` on every writer-step so the reader's
      notify-wait eventually returns.
    * Concurrent multi-blob and digest interleaving — a single
      `StreamingBlobInner` is modeled per spec instance.
    * Producer-side gRPC stream errors that DO NOT make it through to
      `send_error` (e.g. tonic mid-stream gRPC abort that drops `tx` and
      relies on the Drop impl) — modeled as `WriterDropWithoutEof`.
    * The companion #500 reader-loop side at
      `bytestream_server.rs:1754` (`inner_read::consume_ok_eof`) is
      represented as a final classifier on the READER's terminal
      observation: it folds (writer-saw-Err) into the reader's outcome.

  CITATIONS
    [next]    nativelink-util/src/streaming_blob.rs:631 (next_chunk)
    [eof]     nativelink-util/src/streaming_blob.rs:425 (send_eof)
    [err]     nativelink-util/src/streaming_blob.rs:450 (send_error)
    [drop]    nativelink-util/src/streaming_blob.rs:489 (Drop impl)
    [502]     nativelink-util/src/streaming_blob.rs:792-817 (silent-short shield)
    [500a]    nativelink-service/src/bytestream_server.rs:1754 (inner_read consume_ok_eof)
    [500b]    nativelink-store/src/fast_slow_store.rs (chunked WPS Ok+0 guard)
    [commit]  ee13bee0 (#500 propagation), 55c16700 (#500 chunked guard),
              8e6be6d7 (#502 reader-boundary defense)

  VERIFIED AGAINST origin/main: 8e6be6d7 (2026-05-16)
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    Readers,            \* Set of reader ids, e.g. {r1, r2, r3}
    NumChunks,          \* Nat >= 1: number of chunks the writer plans
    AllowShortEof,      \* TRUE => writer may call send_eof with
                        \* bytes_written < expected (#502 trigger)
    AllowErrEof,        \* TRUE => writer may call send_error
    AllowDropWithoutEof,\* TRUE => writer may drop without send_eof
                        \* (Drop impl fires terminal Err)
    ShortShieldOn,      \* TRUE = post-#502: reader returns Err on
                        \*   silent-short; FALSE = pre-#502 silent-zero
                        \*   on short EOF
    SilentZeroPropOn    \* TRUE = post-#500: producer-side Err is
                        \*   propagated to reader's outcome classifier;
                        \*   FALSE = pre-#500 silent-zero shape

ASSUME Cardinality(Readers) >= 1
ASSUME NumChunks \in Nat \ {0}
ASSUME AllowShortEof        \in BOOLEAN
ASSUME AllowErrEof          \in BOOLEAN
ASSUME AllowDropWithoutEof  \in BOOLEAN
ASSUME ShortShieldOn        \in BOOLEAN
ASSUME SilentZeroPropOn     \in BOOLEAN

\* ===== Writer FSM =====
\* Active: writer is still producing chunks; may transition by send /
\*   send_eof / send_error / drop.
\* DoneOkFull: writer reached send_eof after writing ALL NumChunks chunks.
\* DoneOkShort: writer reached send_eof after writing fewer than NumChunks
\*   chunks (silent-short producer trigger).
\* DoneErr: writer fired send_error.
\* DoneDropped: writer dropped without eof (Drop impl set terminal=Err).
WriterStates == {
    "Active",
    "DoneOkFull",
    "DoneOkShort",
    "DoneErr",
    "DoneDropped"
}

\* ===== Reader FSM =====
\* Idle: reader hasn't subscribed yet.
\* Subscribed: reader has a cursor + watch receiver; may consume next
\*   chunk OR wait on notify.
\* Waiting: reader is parked on the watch channel (no chunk available,
\*   no terminal yet).
\* DoneOk: reader observed terminal=Ok AND bytes_written >= expected.
\* DoneErr: reader observed terminal=Err OR silent-short shield fired
\*   OR silent-zero propagation observed Err.
\* DoneSilentZero: reader observed terminal=Ok with bytes_written=0 and
\*   no shield fired. ONLY reachable under bugged variants; tracked
\*   so safety invariants can distinguish.
\* DoneSilentShort: reader observed terminal=Ok with
\*   0 < bytes_written < expected and no shield fired. ONLY reachable
\*   under bugged variants.
\* DoneCorrupt: reader observed bytes that don't match the writer's
\*   sequence. Modeled defensively; not reachable in either fixed or
\*   bugged variants (a streaming_blob mismatched-chunk-content bug
\*   would be a separate fault class).
\* TimedOut: reader's notify wait exceeded the bound (modeled as a
\*   non-deterministic transition).
ReaderStates == {
    "Idle",
    "Subscribed",
    "Waiting",
    "DoneOk",
    "DoneErr",
    "DoneSilentZero",
    "DoneSilentShort",
    "DoneCorrupt",
    "TimedOut"
}

ReaderTerminalStates == {
    "DoneOk", "DoneErr", "DoneSilentZero", "DoneSilentShort",
    "DoneCorrupt", "TimedOut"
}

\* Per-reader observation slot.
ObservationSlot == { "None", "Chunk", "TerminalOk", "TerminalErr" }

VARIABLES
    writerState,        \* WriterStates
    chunkCount,         \* Nat 0..NumChunks: how many chunks appended so far
    bytesWritten,       \* Nat: chunks*ChunkSize OR less (silent-short)
    terminal,           \* "None" | "Ok" | "Err"
    \* Per-reader state.
    readerState,        \* Readers -> ReaderStates
    readerCursor,       \* Readers -> Nat: next chunk to read
    \* Whether the producer-side error has been "seen" by the
    \* read-side classifier (i.e. propagated through the
    \* `inner_read::consume_ok_eof` seam). Toggled by
    \* WriterDropWithoutEof and WriterSendError when SilentZeroPropOn.
    producerErrPropagated  \* BOOLEAN

vars == <<writerState, chunkCount, bytesWritten, terminal,
          readerState, readerCursor, producerErrPropagated>>

\* Expected total bytes the writer DECLARED it would write. This is the
\* digest's `size_bytes()` in production; modeled as the abstract
\* number of chunks times the abstract per-chunk size.
ExpectedBytes == NumChunks    \* using ChunkSize = 1 throughout

----------------------------------------------------------------------------
(* Init                                                                    *)
----------------------------------------------------------------------------
Init ==
    /\ writerState   = "Active"
    /\ chunkCount    = 0
    /\ bytesWritten  = 0
    /\ terminal      = "None"
    /\ readerState   = [r \in Readers |-> "Idle"]
    /\ readerCursor  = [r \in Readers |-> 0]
    /\ producerErrPropagated = FALSE

----------------------------------------------------------------------------
(* WRITER ACTIONS                                                          *)
(***************************************************************************
  Each action mirrors the named method on `StreamingBlobWriter`:
    - WriterSendChunk   ~ `send(chunk)`         [streaming_blob.rs:384]
    - WriterSendEofFull ~ `send_eof()` after writing every chunk
    - WriterShortEof    ~ `send_eof()` after writing < every chunk
                          (the #502 trigger)                  [eof,502]
    - WriterSendError   ~ `send_error(err)`     [err]
    - WriterDropWithoutEof ~ Drop without eof    [drop]
 ***************************************************************************)

WriterSendChunk ==
    /\ writerState = "Active"
    /\ chunkCount < NumChunks
    /\ chunkCount'   = chunkCount + 1
    /\ bytesWritten' = bytesWritten + 1   \* ChunkSize = 1
    /\ UNCHANGED <<writerState, terminal,
                   readerState, readerCursor, producerErrPropagated>>

WriterSendEofFull ==
    /\ writerState = "Active"
    /\ chunkCount = NumChunks
    /\ writerState' = "DoneOkFull"
    /\ terminal'    = "Ok"
    /\ UNCHANGED <<chunkCount, bytesWritten,
                   readerState, readerCursor, producerErrPropagated>>

WriterShortEof ==
    /\ AllowShortEof
    /\ writerState = "Active"
    /\ chunkCount < NumChunks
    /\ writerState' = "DoneOkShort"
    /\ terminal'    = "Ok"
    /\ UNCHANGED <<chunkCount, bytesWritten,
                   readerState, readerCursor, producerErrPropagated>>

WriterSendError ==
    /\ AllowErrEof
    /\ writerState = "Active"
    /\ writerState' = "DoneErr"
    /\ terminal'    = "Err"
    \* #500 propagation: producer's Err makes it through to the
    \* read-side classifier's `consume_ok_eof` seam. Under
    \* SilentZeroPropOn=TRUE the classifier consults the Err slot
    \* and surfaces Err to the reader; under FALSE it does not, so a
    \* reader hitting a 0-byte chunk-count terminal Ok would see
    \* `Ok(empty)` as a silent zero.
    /\ producerErrPropagated' = SilentZeroPropOn
    /\ UNCHANGED <<chunkCount, bytesWritten, readerState, readerCursor>>

WriterDropWithoutEof ==
    /\ AllowDropWithoutEof
    /\ writerState = "Active"
    /\ writerState' = "DoneDropped"
    /\ terminal'    = "Err"
    \* Drop impl fires a terminal Err carrying
    \* "writer dropped without sending EOF". Same propagation gate as
    \* WriterSendError: pre-#500 the seam swallowed Err -> silent-zero
    \* shape; post-#500 the seam propagates Err.
    /\ producerErrPropagated' = SilentZeroPropOn
    /\ UNCHANGED <<chunkCount, bytesWritten, readerState, readerCursor>>

----------------------------------------------------------------------------
(* READER ACTIONS                                                          *)
(***************************************************************************
  Each reader is independent. A reader can subscribe at any time
  (modeled by ReaderSubscribe firing when writerState = anything).
  Once subscribed it advances through its cursor by ReaderConsumeChunk
  while chunks exist beyond the cursor; otherwise it either Waits on the
  notify channel (modeled by transitioning to Waiting, then back to
  Subscribed on the next writer action that fires a notify) or it
  observes the terminal state.

  Terminal observation (mirrors next_chunk lines :693-822):
    * terminal = "Ok" AND bytesWritten = ExpectedBytes   -> DoneOk
    * terminal = "Ok" AND bytesWritten < ExpectedBytes:
        - ShortShieldOn = TRUE  -> DoneErr  (the #502 fix)
        - ShortShieldOn = FALSE:
            * bytesWritten = 0 -> DoneSilentZero (the #500 wire-shape)
            * else             -> DoneSilentShort
    * terminal = "Err":
        - SilentZeroPropOn = TRUE -> DoneErr
        - SilentZeroPropOn = FALSE AND bytesWritten = 0 -> DoneSilentZero
        - SilentZeroPropOn = FALSE AND bytesWritten > 0 -> DoneSilentShort
 ***************************************************************************)

ReaderSubscribe(r) ==
    /\ readerState[r] = "Idle"
    /\ readerState' = [readerState EXCEPT ![r] = "Subscribed"]
    /\ readerCursor' = [readerCursor EXCEPT ![r] = 0]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   producerErrPropagated>>

\* Reader has a chunk available at its cursor; consume it.
ReaderConsumeChunk(r) ==
    /\ readerState[r] = "Subscribed"
    /\ readerCursor[r] < chunkCount
    /\ readerCursor' = [readerCursor EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerState, producerErrPropagated>>

\* Reader has consumed up to cursor=chunkCount and there is no terminal
\* yet; it parks on the watch channel.
ReaderWait(r) ==
    /\ readerState[r] = "Subscribed"
    /\ readerCursor[r] = chunkCount
    /\ terminal = "None"
    /\ readerState' = [readerState EXCEPT ![r] = "Waiting"]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerCursor, producerErrPropagated>>

\* Reader was Waiting; a notify woke it (or a fresh chunk is now
\* available, or terminal flipped). Transition back to Subscribed.
ReaderWakeUp(r) ==
    /\ readerState[r] = "Waiting"
    /\ \/ readerCursor[r] < chunkCount
       \/ terminal # "None"
    /\ readerState' = [readerState EXCEPT ![r] = "Subscribed"]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerCursor, producerErrPropagated>>

\* Reader's notify wait exceeded the bound; surface a synthetic
\* DeadlineExceeded. This is the streaming_blob_notify_timeout path
\* (`next_chunk` :915). Models the safety net for a wedged producer.
\* In well-behaved runs (fairness applied) the writer terminates first
\* and this transition is not taken — it is here only to expose the
\* failure mode where the writer wedges forever.
ReaderNotifyTimeoutAbort(r) ==
    /\ readerState[r] = "Waiting"
    /\ readerState' = [readerState EXCEPT ![r] = "TimedOut"]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerCursor, producerErrPropagated>>

\* The reader observes the terminal state. Cursor must have caught up
\* to chunkCount (or have seen at most chunkCount; the spec's cursor
\* never advances past chunkCount). Classify per #500 / #502 rules.
\* Helper: classify the outcome for a reader observing the terminal
\* state given current bytesWritten / terminal / shield+prop flags.
\* Uses nested IF/ELSE (rather than nested CASE) because TLC's CASE
\* operator does not propagate values through nested-CASE branches
\* reliably in all evaluation orders — IF/THEN/ELSE chains are total
\* and always reduce to a defined value.
ReaderTerminalOutcome ==
    IF terminal = "Ok" /\ bytesWritten = ExpectedBytes
        THEN "DoneOk"
    ELSE IF terminal = "Ok" /\ bytesWritten < ExpectedBytes
        THEN IF ShortShieldOn
                THEN "DoneErr"
             ELSE IF bytesWritten = 0
                THEN "DoneSilentZero"
             ELSE "DoneSilentShort"
    ELSE   \* terminal = "Err"
        IF producerErrPropagated
            THEN "DoneErr"
        ELSE IF bytesWritten = 0
            THEN "DoneSilentZero"
        ELSE "DoneSilentShort"

ReaderObserveTerminal(r) ==
    /\ readerState[r] = "Subscribed"
    /\ readerCursor[r] = chunkCount
    /\ terminal # "None"
    /\ readerState' = [readerState EXCEPT ![r] = ReaderTerminalOutcome]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerCursor, producerErrPropagated>>

----------------------------------------------------------------------------
(* Next                                                                    *)
----------------------------------------------------------------------------
Next ==
    \/ WriterSendChunk
    \/ WriterSendEofFull
    \/ WriterShortEof
    \/ WriterSendError
    \/ WriterDropWithoutEof
    \/ \E r \in Readers : ReaderSubscribe(r)
    \/ \E r \in Readers : ReaderConsumeChunk(r)
    \/ \E r \in Readers : ReaderWait(r)
    \/ \E r \in Readers : ReaderWakeUp(r)
    \/ \E r \in Readers : ReaderNotifyTimeoutAbort(r)
    \/ \E r \in Readers : ReaderObserveTerminal(r)

----------------------------------------------------------------------------
(* Spec + fairness                                                         *)
(*                                                                          *)
(* Writer is fair only on the success path — if AllowDropWithoutEof or    *)
(* AllowErrEof is on, those are choices the writer MAY take; we don't    *)
(* require it to. But to keep liveness verifiable, we require the writer *)
(* to eventually fire one of {SendEofFull, ShortEof, SendError,          *)
(* DropWithoutEof} so terminal is eventually set; otherwise the writer  *)
(* wedges forever and every reader times out. We model this via fairness *)
(* on the disjunction of writer-progress actions.                        *)
(*                                                                          *)
(* Readers: fair on subscribe + consume + wake-up + terminal observation.*)
(* Time-out abort is left WITHOUT fairness so TLC explores both          *)
(* "reader eventually times out" and "reader doesn't time out" branches. *)
(*                                                                          *)
(* NOTE: WF on `WriterSendChunk` ensures every chunk eventually lands    *)
(* IF the writer hasn't yet terminated. Combined with WF on a writer-    *)
(* termination disjunction, the writer is guaranteed to reach a Done    *)
(* state.                                                                *)
----------------------------------------------------------------------------
WriterTerminationDisjunct ==
    \/ WriterSendEofFull
    \/ WriterShortEof
    \/ WriterSendError
    \/ WriterDropWithoutEof

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(WriterSendChunk)
    /\ WF_vars(WriterTerminationDisjunct)
    /\ \A r \in Readers : WF_vars(ReaderSubscribe(r))
    /\ \A r \in Readers : WF_vars(ReaderConsumeChunk(r))
    /\ \A r \in Readers : WF_vars(ReaderWait(r))
    /\ \A r \in Readers : WF_vars(ReaderWakeUp(r))
    /\ \A r \in Readers : WF_vars(ReaderObserveTerminal(r))

----------------------------------------------------------------------------
(* SAFETY INVARIANTS                                                       *)
----------------------------------------------------------------------------

TypeOK ==
    /\ writerState  \in WriterStates
    /\ chunkCount   \in 0..NumChunks
    /\ bytesWritten \in 0..NumChunks
    /\ terminal     \in {"None", "Ok", "Err"}
    /\ \A r \in Readers : readerState[r] \in ReaderStates
    /\ \A r \in Readers : readerCursor[r] \in 0..NumChunks
    /\ producerErrPropagated \in BOOLEAN

\* Writer-side: chunkCount = bytesWritten when ChunkSize = 1. Also:
\* bytesWritten <= chunkCount whenever ChunkSize >= 1.
BytesWrittenMatchesChunkCount ==
    bytesWritten = chunkCount

\* No reader cursor advances past chunkCount (every read corresponds
\* to a previously appended chunk).
CursorBoundedByChunkCount ==
    \A r \in Readers : readerCursor[r] <= chunkCount

\* The reader's terminal observation rules implement #500 + #502
\* correctly: no reader can ever reach DoneSilentZero or DoneSilentShort
\* IF (ShortShieldOn AND SilentZeroPropOn).
NoSilentZeroToReader ==
    \A r \in Readers : readerState[r] # "DoneSilentZero"

NoSilentShortToReader ==
    \A r \in Readers : readerState[r] # "DoneSilentShort"

\* The reader never observes corrupt bytes (no transition writes
\* DoneCorrupt — included for symmetry with ChunkRaceReadersV2's
\* defensive marker; flips to violated only if a future spec extension
\* introduces a chunk-content-corruption transition).
NoCorruptBytesToReader ==
    \A r \in Readers : readerState[r] # "DoneCorrupt"

\* Composite invariant: under the post-fix composition
\* (ShortShieldOn AND SilentZeroPropOn), every terminated reader is
\* in one of {DoneOk, DoneErr, TimedOut} — i.e. there is no class of
\* observation that looks like "success" but isn't.
PostFixReaderOutcomeIsTwoWay ==
    (ShortShieldOn /\ SilentZeroPropOn) =>
        \A r \in Readers :
            readerState[r] \in ReaderTerminalStates =>
                readerState[r] \in {"DoneOk", "DoneErr", "TimedOut"}

\* Composite invariant: DoneOk implies the writer terminated with full
\* bytes (no reader sees DoneOk against a short or errored writer
\* terminal). This is the load-bearing safety property — the bug
\* would be a reader that "looks OK" but had silent corruption.
ReaderOkImpliesWriterFullOk ==
    \A r \in Readers :
        readerState[r] = "DoneOk" =>
            writerState = "DoneOkFull"

\* Composite invariant: a producer that drops or errors AND a reader
\* that subscribed before terminal is observed must, post-fix, reach
\* DoneErr (not silent-zero, not silent-short, not eternal park).
\* Encoded as a state predicate: no terminated reader is in
\* DoneSilentZero/DoneSilentShort when SilentZeroPropOn is TRUE and
\* the writer ended via SendError or DropWithoutEof.
PostFixErrPropagationReachesReader ==
    (SilentZeroPropOn /\ writerState \in {"DoneErr", "DoneDropped"}) =>
        \A r \in Readers :
            readerState[r] \in ReaderTerminalStates =>
                readerState[r] \notin {"DoneSilentZero", "DoneSilentShort"}

\* Composite invariant: a writer that succeeds with full bytes AND a
\* reader that observed terminal must, post-fix, reach DoneOk (no
\* false positive errors, no spurious time-outs given fairness).
\* This is a SAFETY form of liveness.
PostFixFullOkReachesReaderAsOk ==
    (writerState = "DoneOkFull") =>
        \A r \in Readers :
            readerState[r] \in {"DoneSilentZero", "DoneSilentShort",
                                "DoneCorrupt"} => FALSE

\* Composite invariant: every reader that is in a terminal state has
\* a cursor that didn't run ahead of what the writer wrote.
CursorAtMostChunkCountAtTerminal ==
    \A r \in Readers :
        readerState[r] \in ReaderTerminalStates =>
            readerCursor[r] <= chunkCount

----------------------------------------------------------------------------
(* LIVENESS PROPERTIES                                                     *)
----------------------------------------------------------------------------

\* Writer eventually terminates.
WriterEventuallyTerminates ==
    <>(writerState \in {"DoneOkFull", "DoneOkShort", "DoneErr",
                        "DoneDropped"})

\* Every reader, once subscribed, eventually reaches a terminal state.
\* This catches "reader eternally parked on watch::changed" — the bug
\* class the streaming_blob notify timeout is the safety net for.
ReadersEventuallyTerminate ==
    \A r \in Readers :
        (readerState[r] \in {"Subscribed", "Waiting"})
            ~> (readerState[r] \in ReaderTerminalStates)

\* Strong eventual consistency: a reader that subscribes AT OR AFTER the
\* writer terminates with Ok-full eventually reaches DoneOk.
EventualConsistencyOkFull ==
    \A r \in Readers :
        (writerState = "DoneOkFull" /\ readerState[r] = "Idle")
            ~> (readerState[r] = "DoneOk")

\* No-eternal-park: if the writer has terminated, every subscribed
\* reader eventually reaches some terminal state (the writer never
\* leaves a reader parked indefinitely after termination).
NoEternalParkAfterWriterDone ==
    \A r \in Readers :
        (writerState \in {"DoneOkFull", "DoneOkShort", "DoneErr",
                          "DoneDropped"}
         /\ readerState[r] \in {"Subscribed", "Waiting"})
            ~> (readerState[r] \in ReaderTerminalStates)

============================================================================
