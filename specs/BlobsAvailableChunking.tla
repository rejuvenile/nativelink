--------------------- MODULE BlobsAvailableChunking ------------------------
(***************************************************************************
  #99 (BlobsAvailable chunking, PR3 of #80 generalized streaming design).

  CONTRACT WE'RE MODELING:
    The worker splits each BlobsAvailable notification into N chunks
    identified by (broadcast_id, sequence) under a shared
    worker_instance_token. Each chunk carries per-chunk slices of the
    five unbounded fields plus a per-chunk is_last flag. The server's
    per-connection accumulator BUFFERS each chunk; ONLY when is_last=true
    arrives does it materialise the fully-assembled
    BlobsAvailableNotification and apply it (via the existing
    handle_blobs_available -- remove_endpoint wipe + register_blobs_iter
    + AC pin replace + mirror pipeline, all atomically).

    Path A semantics: defer wipe until is_last=true. When
    is_full_snapshot=true, the wipe (remove_endpoint(...)) MUST NOT
    fire on chunk 0 -- only on the terminal chunk, after the entire
    digest set has been buffered. This prevents the half-applied-
    snapshot bug class where chunks 0..N-1 land but the terminal is
    lost (connection drop, partial replay) and the locality_map is
    left with FEWER digests than reality.

  WHY MODEL THIS:
    The bug class we are guarding against is specific to chunked
    REPLACE-snapshot semantics. Per-chunk apply (Path B) leaves the
    server's view as a strict-subset partial after a mid-broadcast
    drop. Deferred-wipe (Path A) keeps the prior full view until a
    new terminal lands.

  CONSTANTS:
    * NumChunks (Nat >= 1): how many chunks the worker emits.
    * BugMode (BOOLEAN):
        - TRUE  => wipe applies on every chunk (Path B / pre-#99
                   hypothetical bug). Mid-broadcast drop leaves the
                   serverView as a partial.
        - FALSE => wipe defers to is_last=true (Path A / shipped).
                   Mid-broadcast drop discards partial accumulator;
                   serverView keeps last-good prior terminal.

  INVARIANT:
    LocalityMapMatchesEndpointOrPriorFull -- at every reachable state
    serverView is EITHER {} (never-committed) OR = WorkerSnapshot
    (full prior commit). It is NEVER a strict-subset partial assembled
    from chunks 0..k where k < NumChunks-1.

  TEMPORAL:
    EventuallyConverges -- under fairness on DeliverChunk, eventually
    serverView = WorkerSnapshot.

  Modeled after specs/BISChunkingAck.tla (#97 precedent).
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    NumChunks,
    BugMode

ASSUME NumChunksOK == NumChunks \in Nat /\ NumChunks >= 1
ASSUME BugModeOK   == BugMode \in BOOLEAN

\* Each chunk owns a single digest-id; the full snapshot is
\* {0..NumChunks-1}. Tractable state space; the invariant holds for
\* any partition size since the accumulator's merge is order-agnostic.
WorkerSnapshot == 0..(NumChunks - 1)
ChunkDigests(i) == {i}

VARIABLES
    serverPartial,    \* per-sequence assembled slice; updated on Deliver.
    serverView,       \* server's actual locality view.
    seenSequences,    \* bitset of received sequences for current broadcast.
    pendingChunks,    \* sequences not yet delivered.
    terminalEmitted,  \* worker has emitted is_last=true.
    everCommitted

vars == <<serverPartial, serverView, seenSequences, pendingChunks,
          terminalEmitted, everCommitted>>

TypeOK ==
    /\ serverPartial \in [0..(NumChunks - 1) -> SUBSET WorkerSnapshot]
    /\ serverView \subseteq WorkerSnapshot
    /\ seenSequences \subseteq 0..(NumChunks - 1)
    /\ pendingChunks \subseteq 0..(NumChunks - 1)
    /\ terminalEmitted \in BOOLEAN
    /\ everCommitted \in BOOLEAN

Init ==
    /\ serverPartial = [s \in 0..(NumChunks - 1) |-> {}]
    /\ serverView = {}
    /\ seenSequences = {}
    /\ pendingChunks = 0..(NumChunks - 1)
    /\ terminalEmitted = FALSE
    /\ everCommitted = FALSE

\* Deliver one pending chunk. If `seq` is the terminal (NumChunks-1),
\* this is the commit point.
DeliverChunk(seq) ==
    /\ seq \in pendingChunks
    /\ pendingChunks' = pendingChunks \ {seq}
    /\ seenSequences' = seenSequences \cup {seq}
    /\ serverPartial' = [serverPartial EXCEPT ![seq] = ChunkDigests(seq)]
    /\ \/ /\ seq = NumChunks - 1
          /\ terminalEmitted' = TRUE
          \* Commit: full assembled view becomes serverView.
          /\ serverView' = UNION { serverPartial'[s] : s \in 0..(NumChunks - 1) }
          /\ everCommitted' = TRUE
       \/ /\ seq # NumChunks - 1
          /\ terminalEmitted' = terminalEmitted
          /\ everCommitted' = everCommitted
          \* Path A: serverView unchanged.
          \* Path B: per-chunk apply -- assembled-so-far view.
          /\ serverView' = IF BugMode
                           THEN UNION { serverPartial'[s] : s \in 0..(NumChunks - 1) }
                           ELSE serverView

\* Mid-broadcast connection drop. drop_all_inflight clears partial
\* accumulator state. serverView untouched (Path A keeps last-good;
\* Path B keeps the partial it already applied -- the bug).
DropConnection ==
    /\ pendingChunks # {}
    /\ ~terminalEmitted
    /\ pendingChunks' = {}
    /\ serverPartial' = [s \in 0..(NumChunks - 1) |-> {}]
    /\ seenSequences' = {}
    /\ terminalEmitted' = FALSE
    /\ everCommitted' = everCommitted
    /\ serverView' = serverView

\* Worker reconnects with fresh broadcast.
ReconnectFreshBroadcast ==
    /\ pendingChunks = {}
    /\ ~terminalEmitted
    /\ pendingChunks' = 0..(NumChunks - 1)
    /\ serverPartial' = [s \in 0..(NumChunks - 1) |-> {}]
    /\ seenSequences' = {}
    /\ terminalEmitted' = FALSE
    /\ everCommitted' = everCommitted
    /\ serverView' = serverView

\* Quiescent state after a clean commit; nothing more happens.
SteadyState ==
    /\ terminalEmitted = TRUE
    /\ pendingChunks = {}
    /\ UNCHANGED vars

Next ==
    \/ \E seq \in 0..(NumChunks - 1) : DeliverChunk(seq)
    \/ DropConnection
    \/ ReconnectFreshBroadcast
    \/ SteadyState

Spec == Init /\ [][Next]_vars
        /\ WF_vars(\E seq \in 0..(NumChunks - 1) : DeliverChunk(seq))

\* INVARIANT: serverView is {} or full-snapshot, never a partial.
\* Path A (BugMode=FALSE): holds.
\* Path B (BugMode=TRUE):  TLC produces counterexample.
LocalityMapMatchesEndpointOrPriorFull ==
    \/ serverView = {}
    \/ serverView = WorkerSnapshot

\* TEMPORAL: eventual convergence under fairness on DeliverChunk.
EventuallyConverges == <>(serverView = WorkerSnapshot)

============================================================================
