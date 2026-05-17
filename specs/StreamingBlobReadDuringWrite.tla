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
      NotFound. Spec correspondence: `BuggedSilentZero` cfg disables
      the producer-side `Err` propagation; safety invariant
      `NoSilentZeroToReader` red-fails.
    [#502 8e6be6d7, 2026-05-16]
      `StreamingBlobReader::next_chunk` now returns `Err(Code::Internal)`
      carrying `STREAMING_BLOB_SILENT_SHORT_MARKER` when terminal=Ok and
      `bytes_written < expected_size`. Pre-fix: a `send_eof` after a
      short-byte send returned `Ok(Bytes::new())` to every consumer; Bazel
      saw a clean stream with truncated bytes and reported digest mismatch
      as a build failure. Spec correspondence: `Bugged` cfg disables the
      silent-short check at the reader boundary; safety invariant
      `NoSilentShortToReader` red-fails.

  TWO DISTINCT PRODUCTION SEAMS — SEAM A vs SEAM B
    The post-#500 propagation is NOT a single boolean — it is the
    composition of two distinct state slots in two different files:

      Seam A: `StreamingBlobInner::terminal` (a `Mutex<Option<Result<(),
              Error>>>` slot set by `send_error()` at
              `streaming_blob.rs:450` and the Drop impl at
              `streaming_blob.rs:489`). The writer-side terminal slot.

      Seam B: `state.maybe_get_part_result` (a `Option<Result<(), Error>>`
              local to `inner_read::process_one`, populated when
              `get_part_fut.await` resolves at
              `bytestream_server.rs:1855`). The reader-side classifier
              consults this slot at `:1748` to refuse `Ok(empty)`-on-EOF.

    Production race window: the `tokio::select!` at `:1726-1867` chooses
    which future to poll next non-deterministically. If `consume_fut`
    (returning `Ok(empty)` because the producer dropped `tx`) wins the
    select arm BEFORE `get_part_fut` (returning `Err`) does, then
    `state.maybe_get_part_result` is still `None` at the #500 check at
    `:1748`, and the Err is NOT propagated for THIS poll. The reader's
    THIS `next_chunk` call returns `None` (clean EOF). The race window
    is acknowledged in the production code's own comment at
    `bytestream_server.rs:1856-1866` and in #500's commit message
    (ee13bee0): "Addresses one of multiple candidate mechanisms".

    This spec models the seam split via TWO state variables:
      - `writerTerminalErr`  — Seam A: writer-side `terminal` is Err.
      - `seamBPropagated`    — Seam B: classifier has consumed the
                                Err from `state.maybe_get_part_result`.

    Seam A is set atomically with the writer's terminal action
    (`WriterSendError` / `WriterDropWithoutEof`). Seam B is set by a
    SEPARATE non-deterministic action `SeamBObservesProducerErr` that
    fires under `SilentZeroPropOn=TRUE` AND `writerTerminalErr=TRUE`.

    Fairness `WF_vars(SeamBObservesProducerErr)` guarantees that under
    fairness the propagation eventually fires — so the LIVENESS form
    `PostFixErrPropagationReachesReaderEventually` holds. The SAFETY
    form (every reader observation sees the propagation) does NOT hold,
    matching the production race window. The composite remains a known
    gap that the per-RPC arm-race closure work tracks.

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

EXTENDS Naturals, FiniteSets, TLC

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
    \* Seam A: writer-side `StreamingBlobInner::terminal` is Err.
    \* Set by WriterSendError / WriterDropWithoutEof at the same instant
    \* as `writerState' = DoneErr / DoneDropped`. Independent of whether
    \* the bytestream_server classifier has yet observed the error.
    writerTerminalErr,     \* BOOLEAN
    \* Seam B: bytestream_server's `state.maybe_get_part_result` slot has
    \* been populated with the producer's Err (i.e. `get_part_fut.await`
    \* resolved AND `consume_ok_eof` picked it up via the `take()` at
    \* `bytestream_server.rs:1748`). Set ONLY by the separate
    \* `SeamBObservesProducerErr` action, NOT atomically with Seam A.
    \* Models the per-RPC select-arm race the #500 commit's "addresses
    \* one of multiple candidate mechanisms" hedge refers to.
    seamBPropagated        \* BOOLEAN

vars == <<writerState, chunkCount, bytesWritten, terminal,
          readerState, readerCursor, writerTerminalErr, seamBPropagated>>

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
    /\ writerTerminalErr = FALSE
    /\ seamBPropagated   = FALSE

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
    /\ UNCHANGED <<writerState, terminal, readerState, readerCursor,
                   writerTerminalErr, seamBPropagated>>

WriterSendEofFull ==
    /\ writerState = "Active"
    /\ chunkCount = NumChunks
    /\ writerState' = "DoneOkFull"
    /\ terminal'    = "Ok"
    /\ UNCHANGED <<chunkCount, bytesWritten, readerState, readerCursor,
                   writerTerminalErr, seamBPropagated>>

WriterShortEof ==
    /\ AllowShortEof
    /\ writerState = "Active"
    /\ chunkCount < NumChunks
    /\ writerState' = "DoneOkShort"
    /\ terminal'    = "Ok"
    /\ UNCHANGED <<chunkCount, bytesWritten, readerState, readerCursor,
                   writerTerminalErr, seamBPropagated>>

\* Seam A — writer's `terminal` slot becomes Err via `send_error()`.
\* Does NOT touch Seam B; the bytestream_server classifier observes
\* propagation via the separate `SeamBObservesProducerErr` action.
WriterSendError ==
    /\ AllowErrEof
    /\ writerState = "Active"
    /\ writerState' = "DoneErr"
    /\ terminal'    = "Err"
    /\ writerTerminalErr' = TRUE
    /\ UNCHANGED <<chunkCount, bytesWritten, readerState, readerCursor,
                   seamBPropagated>>

\* Seam A — writer's `terminal` slot becomes Err via the Drop impl
\* synthesizing "writer dropped without sending EOF".
\* Does NOT touch Seam B; same as WriterSendError.
WriterDropWithoutEof ==
    /\ AllowDropWithoutEof
    /\ writerState = "Active"
    /\ writerState' = "DoneDropped"
    /\ terminal'    = "Err"
    /\ writerTerminalErr' = TRUE
    /\ UNCHANGED <<chunkCount, bytesWritten, readerState, readerCursor,
                   seamBPropagated>>

\* Seam B — `inner_read::consume_ok_eof` consumes the producer's Err
\* from `state.maybe_get_part_result` via `take()` at
\* `bytestream_server.rs:1748`. Models the per-RPC select-arm race
\* where `get_part_fut.await` resolves AND its result is observed by
\* the classifier (i.e. `get_part_fut` wins the select arm BEFORE
\* `consume_fut` returns `Ok(empty)`). Gated on the #500 propagation
\* flag — pre-#500, this action was effectively never enabled because
\* the classifier did not consult the slot at all.
\*
\* The non-determinism (TLC can interleave a reader's terminal
\* observation BEFORE this action fires, modeling the race-loser case
\* where `consume_fut` returns `Ok(empty)` first and the classifier
\* misses the Err for THIS poll). Under WF fairness this action is
\* guaranteed to fire eventually, so the LIVENESS form of propagation
\* holds; the SAFETY form does NOT, matching the residual race window
\* the #500 commit message acknowledged as a candidate.
SeamBObservesProducerErr ==
    /\ SilentZeroPropOn
    /\ writerTerminalErr
    /\ ~seamBPropagated
    /\ seamBPropagated' = TRUE
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerState, readerCursor, writerTerminalErr>>

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
                   writerTerminalErr, seamBPropagated>>

\* Reader has a chunk available at its cursor; consume it.
ReaderConsumeChunk(r) ==
    /\ readerState[r] = "Subscribed"
    /\ readerCursor[r] < chunkCount
    /\ readerCursor' = [readerCursor EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerState, writerTerminalErr, seamBPropagated>>

\* Reader has consumed up to cursor=chunkCount and there is no terminal
\* yet; it parks on the watch channel.
ReaderWait(r) ==
    /\ readerState[r] = "Subscribed"
    /\ readerCursor[r] = chunkCount
    /\ terminal = "None"
    /\ readerState' = [readerState EXCEPT ![r] = "Waiting"]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerCursor, writerTerminalErr, seamBPropagated>>

\* Reader was Waiting; a notify woke it (or a fresh chunk is now
\* available, or terminal flipped). Transition back to Subscribed.
ReaderWakeUp(r) ==
    /\ readerState[r] = "Waiting"
    /\ \/ readerCursor[r] < chunkCount
       \/ terminal # "None"
    /\ readerState' = [readerState EXCEPT ![r] = "Subscribed"]
    /\ UNCHANGED <<writerState, chunkCount, bytesWritten, terminal,
                   readerCursor, writerTerminalErr, seamBPropagated>>

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
                   readerCursor, writerTerminalErr, seamBPropagated>>

\* The reader observes the terminal state. Cursor must have caught up
\* to chunkCount (or have seen at most chunkCount; the spec's cursor
\* never advances past chunkCount). Classify per #500 / #502 rules.
\* Helper: classify the outcome for a reader observing the terminal
\* state given current bytesWritten / terminal / shield+prop flags.
\*
\* Rationale for IF/THEN/ELSE chain (NOT a nested CASE):
\*   This helper is called only when `terminal # "None"` is guaranteed
\*   by the caller `ReaderObserveTerminal` precondition. An earlier
\*   draft attempted a nested CASE with arms keyed on `terminal = "Ok"`
\*   vs `terminal = "Err"`, and TLC reported "Attempted to evaluate a
\*   CASE with no conditions true". The cause was a SPEC BUG, not a
\*   TLC quirk: the outer CASE's catch-all `OTHER` arm needed to handle
\*   the impossible `terminal = "None"` branch (impossible by caller
\*   precondition, but TLC's evaluator does not propagate the caller's
\*   precondition into the helper's case analysis). Either:
\*     (a) the helper would need `OTHER -> "Unreachable"` plus an
\*         invariant asserting that branch is never reached, or
\*     (b) the helper folds the terminal-Ok/Err split into a single
\*         total IF/THEN/ELSE chain whose final `ELSE` branch covers
\*         `terminal = "Err"` (the only remaining case by caller
\*         precondition).
\*   Option (b) is chosen for compactness; it does NOT cover up a real
\*   reachable state — the caller precondition `terminal # "None"`
\*   eliminates the "None" branch. Reviewers verify by reading
\*   `ReaderObserveTerminal` immediately below: the helper is invoked
\*   only when `terminal # "None"`.
\*
\* Seam B consultation: the terminal=Err branch consults `seamBPropagated`
\* (not `writerTerminalErr`). The bytestream_server classifier folds
\* Err propagation from `state.maybe_get_part_result` ONLY when seam B
\* has fired; otherwise the reader misses the propagation on THIS poll
\* (race-loser case). See PostFix*ReachesReader* invariants below.
ReaderTerminalOutcome ==
    IF terminal = "Ok" /\ bytesWritten = ExpectedBytes
        THEN "DoneOk"
    ELSE IF terminal = "Ok" /\ bytesWritten < ExpectedBytes
        THEN IF ShortShieldOn
                THEN "DoneErr"
             ELSE IF bytesWritten = 0
                THEN "DoneSilentZero"
             ELSE "DoneSilentShort"
    ELSE   \* terminal = "Err" by caller precondition; "None" unreachable.
        IF seamBPropagated
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
                   readerCursor, writerTerminalErr, seamBPropagated>>

----------------------------------------------------------------------------
(* Next                                                                    *)
----------------------------------------------------------------------------
Next ==
    \/ WriterSendChunk
    \/ WriterSendEofFull
    \/ WriterShortEof
    \/ WriterSendError
    \/ WriterDropWithoutEof
    \/ SeamBObservesProducerErr
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
    \* Seam B propagation is fair: under fairness the bytestream_server
    \* classifier eventually observes the producer's Err. The Liveness
    \* form of `PostFixErrPropagationReachesReader*` holds under WF; the
    \* Safety form does NOT (an interleaved reader may observe terminal
    \* before SeamB fires, modeling the per-RPC race-loser case).
    /\ WF_vars(SeamBObservesProducerErr)
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
    /\ writerTerminalErr \in BOOLEAN
    /\ seamBPropagated   \in BOOLEAN

\* Bound (not tautology). The spec abstracts each chunk send as a SINGLE
\* atomic action that increments chunkCount AND bytesWritten together.
\* Production has TWO independent AtomicU64s
\* (`StreamingBlobInner::chunk_count` + `bytes_written`) incremented at
\* different points in `send_chunk`; a code change that swaps the
\* increment order could open a transient window where a reader sees
\* chunkCount=N+1 but bytesWritten=N. THIS SPEC DOES NOT MODEL THAT
\* ATOMICS-ORDERING HAZARD. The invariant below is a structural bound
\* on bytesWritten that does NOT prove the atomics are co-ordered;
\* it only proves the spec's abstraction stays well-typed (bytesWritten
\* tracks chunkCount in the spec's single-atomic-action model).
\*
\* Equality holds because ChunkSize=1 and writes happen atomically;
\* production's two-atomic split would need a separate spec that
\* models chunkCount and bytesWritten as separable transitions. See
\* the audit's "Phase 5 — outstanding gaps" entry for this gap.
BytesWrittenBoundedByChunkCount ==
    bytesWritten <= chunkCount

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

\* Composite "no-silent under post-fix" CANNOT be expressed as a state
\* invariant in this spec because the spec now models the per-RPC race:
\* a reader observation at a state where `seamBPropagated=FALSE` can
\* legitimately reach `DoneSilent*` even with both defenses on. Once
\* `seamBPropagated` flips TRUE in a LATER state, no STATE invariant
\* can retroactively exclude the already-terminal reader from Silent*.
\*
\* The honest formulation:
\*   - SAFETY: NONE that excludes Silent* under post-fix in the Fixed
\*     cfg. (See `PostFixReaderOutcomeIsTwoWayIfSeamBFired` for a
\*     CONDITIONAL form that does NOT hold under the race model and
\*     is therefore commented OUT from the Fixed cfg's INVARIANTS list.)
\*   - LIVENESS: `PostFixErrPropagationReachesReaderEventually` —
\*     under WF on SeamBObservesProducerErr, the seam fires under
\*     fairness, so any reader subscribing AFTER SeamB fires reaches
\*     DoneErr. Reader-side terminal-observation actions are fair too,
\*     so under repeated invocation, the leads-to is satisfied.
\*
\* OUT-OF-SCOPE: closing the per-RPC tokio::select! arm-ordering race
\* at `bytestream_server.rs:1726-1867`. The #500 commit message hedges
\* this as "one of multiple candidate mechanisms"; closing it requires
\* either (a) eliminating the `consume_fut` Ok(empty)/get_part_fut Err
\* race entirely or (b) reaching a hypothetically race-free classifier
\* that consults the same atomic state as `next_chunk`. Both are
\* outside this spec's scope.
\*
\* This conditional form is here as DOCUMENTATION of the strongest
\* safety claim that would hold IF the per-RPC race were closed in
\* production. It is NOT listed in any cfg's INVARIANTS — running it
\* would (correctly) red-fail in Fixed.cfg because the spec models
\* the race.
PostFixReaderOutcomeIsTwoWayIfSeamBFired ==
    (ShortShieldOn /\ SilentZeroPropOn /\ seamBPropagated) =>
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

\* As with `PostFixReaderOutcomeIsTwoWayIfSeamBFired`, this conditional
\* form does NOT hold under the race model and is NOT listed in any
\* cfg's INVARIANTS. Documented here as the strongest safety claim
\* that would hold IF the per-RPC race were closed in production.
\* The leads-to companion (under WF_vars(SeamBObservesProducerErr))
\* is `PostFixErrPropagationReachesReaderEventually` below.
PostFixErrPropagationReachesReaderIfSeamBFired ==
    (SilentZeroPropOn /\ writerState \in {"DoneErr", "DoneDropped"}
       /\ seamBPropagated) =>
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

\* The eventual form of #500 propagation: when the writer has set
\* `writerTerminalErr` (Seam A) under post-#500 propagation enabled,
\* the bytestream_server classifier (Seam B) eventually consumes the
\* Err. Under fairness `WF_vars(SeamBObservesProducerErr)` this holds.
\*
\* Reviewer guarantee: this property only proves that the seam
\* EVENTUALLY fires — it does NOT prove that EVERY reader observation
\* sees the post-fire state. The race-loser case (reader observes
\* terminal=Err before SeamB fires) is admitted by the safety form,
\* by design. See `PostFixReaderOutcomeIsTwoWayIfSeamBFired` for the
\* conditional safety form.
PostFixErrPropagationReachesReaderEventually ==
    (SilentZeroPropOn /\ writerTerminalErr)
        ~> seamBPropagated

============================================================================
