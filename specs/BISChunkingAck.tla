--------------------- MODULE BISChunkingAck --------------------------------
(***************************************************************************
  #97 (BlobsInStableStorage chunking + ack + reconnect-replay).

  CONTRACT WE'RE MODELING:
    The server splits each BIS broadcast into N chunks identified by
    (broadcast_id, sequence). Each chunk is dispatched to every connected
    worker AND added to a per-worker resend buffer keyed by cas_endpoint.
    The worker, on receiving each chunk, unpins the chunk's digests and
    emits a BisAck back to the server. The server's ack handler drops the
    matching (broadcast_id, sequence) from the resend buffer. On worker
    reconnect (same boot_epoch_id), the server replays every still-
    buffered chunk — closing the durability gap from #89 where BIS
    notifications dropped on h2/QUIC stream churn leaked pin state
    forever.

  WHY MODEL THIS:
    The class of bug we are guarding against is "ack lost in transit →
    permanent pin leak". The class of bug we MUST NOT introduce is
    "every chunk is acked, but the buffer never empties because the ack
    handler is missing or wrong". Both classes are invisible at the
    component-boundary level (each side looks correct in isolation —
    the worker emits ack, the server tracks chunks) and only fire at
    the cross-component composition. TLA+ is the right tool: every
    interleaving of (DeliverChunk, AckChunk, DropConnection,
    Reconnect) is reachable in the natural state space.

  CONSTANTS:
    * NumChunks (Nat >= 1): how many chunks the server emits in the
      one modeled broadcast.
    * BugMode (BOOLEAN):
        - TRUE  => no resend on reconnect. Chunks dropped between
                   disconnect and ack-receipt are lost forever.
                   Models the pre-#97 production path under #89-style
                   stream churn.
        - FALSE => resend on reconnect: drained chunks are replayed
                   when the worker reconnects with the same endpoint.
                   Models the #97 implementation.

  EXPECTED TLC OUTCOMES:
    * BISChunkingAckFixed.cfg (BugMode = FALSE):
      No invariant violation, no temporal-property violation. Every
      chunk eventually drives an unpin AND eventually leaves the
      server's resend buffer.
    * BISChunkingAckBugged.cfg (BugMode = TRUE):
      INVARIANT VIOLATED on AllChunksEventuallyUnpinned. Counter-
      example: chunks 1..k delivered + acked → server processes ack
      and drops them → connection drops mid-flight for chunk k+1 →
      worker reconnects → no replay → chunk k+1 never lands → its
      digests never unpin. Pin state leaks forever.

  SCOPE — what this spec models:
    * one cas_endpoint, one broadcast,
    * NumChunks chunks each carrying a single "unpin event",
    * non-deterministic interleaving of:
        - DeliverChunk(c) — server → worker
        - AckChunk(c)     — worker → server
        - DropConnection  — wire drops; in-flight chunks get lost
        - Reconnect       — worker comes back; FixedMode replays
                            unacked chunks
    * server-side ack_handler that drops (broadcast_id, sequence)
      from the per-endpoint resend buffer on receipt.

  SCOPE — what this spec does NOT model:
    * multiple concurrent broadcasts (the buffer is keyed by
      (broadcast_id, sequence) so they don't interfere; the
      single-broadcast model is sufficient for the resend invariant),
    * boot_epoch change (modeled separately via the
      clear_bis_resend_buffer_for_endpoint code path; this spec
      assumes same-epoch reconnects, which is the bug-prone case),
    * the chunk_iter empty-terminal contract (covered by unit tests),
    * worker-side state-machine for unpin idempotency (the resend
      may deliver a chunk twice; production code makes unpin a
      no-op on second invocation; here we model unpin as set-add
      so duplicates are naturally idempotent).

  CITATIONS:
    [code-server]   nativelink-scheduler/src/api_worker_scheduler.rs:2700-2880
                    (broadcast_blobs_in_stable_storage_chunked,
                     replay_bis_chunks_to_worker, bis_ack_received)
    [code-worker]   nativelink-worker/src/local_worker.rs:760-820
                    (handle_bis_chunk + ChunkedMessage arm)
    [test-server]   nativelink-scheduler/src/api_worker_scheduler.rs#tests
                    (bis_chunked_ack_lost_resend in particular)
    [test-worker]   nativelink-worker/tests/bis_chunk_handler_test.rs
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    NumChunks,    \* Number of chunks in the modeled broadcast (Nat >= 1)
    BugMode       \* TRUE => no resend on reconnect; FALSE => resend

ASSUME NumChunks \in Nat /\ NumChunks >= 1
ASSUME BugMode \in BOOLEAN

\* Chunk identifiers (= sequence within the broadcast).
Chunks == 1..NumChunks

\* Connection states for the worker.
\*   "Connected"     - chunks deliverable + acks deliverable
\*   "Disconnected"  - chunks NOT deliverable (drained from in-flight),
\*                     pending acks NOT deliverable
ConnStates == {"Connected", "Disconnected"}

VARIABLES
    \* Set of chunks still buffered for resend on the server.
    \* On dispatch, every chunk is added; on AckChunk, the matching
    \* chunk is removed.
    serverBuffer,

    \* Set of chunks the WORKER has unpinned. The unpin-invariant
    \* says every chunk must eventually appear here.
    workerUnpinned,

    \* Connection state.
    connState,

    \* Set of chunks "in flight" server → worker. On Connected,
    \* DeliverChunk pulls chunks from this set into workerUnpinned;
    \* on Disconnected, in-flight chunks are dropped (the wire
    \* terminates). On Reconnect, FixedMode re-populates this from
    \* serverBuffer (replay); BugMode does NOT.
    inFlight,

    \* Set of chunks waiting to be acked from worker → server. On
    \* Connected, AckChunk drains them into the server's ack handler;
    \* on Disconnected, they are dropped (acks lost on disconnect).
    pendingAcks

vars == <<serverBuffer, workerUnpinned, connState, inFlight, pendingAcks>>

----------------------------------------------------------------------------
\* Initial state: server has dispatched all chunks (added to buffer +
\* in-flight). Worker has unpinned none yet. Connection is up. No acks
\* are pending.
Init ==
    /\ serverBuffer = Chunks
    /\ workerUnpinned = {}
    /\ connState = "Connected"
    /\ inFlight = Chunks
    /\ pendingAcks = {}

----------------------------------------------------------------------------
\* DeliverChunk(c): worker receives chunk c (only when connected).
\* The chunk's digests are unpinned (added to workerUnpinned) and a
\* corresponding ack is queued in pendingAcks.
----------------------------------------------------------------------------
DeliverChunk(c) ==
    /\ connState = "Connected"
    /\ c \in inFlight
    /\ inFlight' = inFlight \ {c}
    /\ workerUnpinned' = workerUnpinned \cup {c}
    /\ pendingAcks' = pendingAcks \cup {c}
    /\ UNCHANGED <<serverBuffer, connState>>

----------------------------------------------------------------------------
\* AckChunk(c): server receives the worker's ack for chunk c (only
\* when connected). The ack drops c from serverBuffer.
----------------------------------------------------------------------------
AckChunk(c) ==
    /\ connState = "Connected"
    /\ c \in pendingAcks
    /\ pendingAcks' = pendingAcks \ {c}
    /\ serverBuffer' = serverBuffer \ {c}
    /\ UNCHANGED <<workerUnpinned, connState, inFlight>>

----------------------------------------------------------------------------
\* DropConnection: the wire terminates. In-flight chunks are dropped
\* (they never reach the worker). Pending acks are dropped too (they
\* never reach the server). serverBuffer is unchanged — that's the
\* point of the buffer.
----------------------------------------------------------------------------
DropConnection ==
    /\ connState = "Connected"
    /\ connState' = "Disconnected"
    /\ inFlight' = {}
    /\ pendingAcks' = {}
    /\ UNCHANGED <<serverBuffer, workerUnpinned>>

----------------------------------------------------------------------------
\* Reconnect: worker reconnects. In FixedMode, the server replays
\* every still-buffered chunk (re-populating inFlight from
\* serverBuffer). In BugMode, the server does NOT replay — the
\* still-buffered chunks are silently lost.
----------------------------------------------------------------------------
Reconnect ==
    /\ connState = "Disconnected"
    /\ connState' = "Connected"
    /\ \/ /\ ~BugMode
          /\ inFlight' = serverBuffer  \* replay
          /\ UNCHANGED <<serverBuffer, workerUnpinned, pendingAcks>>
       \/ /\ BugMode
          /\ UNCHANGED <<serverBuffer, workerUnpinned, inFlight, pendingAcks>>

----------------------------------------------------------------------------
Next ==
    \/ \E c \in Chunks : DeliverChunk(c)
    \/ \E c \in Chunks : AckChunk(c)
    \/ DropConnection
    \/ Reconnect

----------------------------------------------------------------------------
\* Strong fairness on the per-chunk delivery / ack actions. Strong
\* fairness (SF) is required because the action's enabledness
\* flickers across DropConnection — it's enabled while connState =
\* Connected and disabled while Disconnected. With weak fairness
\* alone, an infinite Drop ↔ Reconnect cycle could starve a
\* particular chunk's DeliverChunk forever even though that chunk is
\* in inFlight after every Reconnect. SF says: if DeliverChunk(c) is
\* enabled INFINITELY OFTEN, it must fire infinitely often — so the
\* chunk does eventually deliver. Same reasoning for AckChunk.
\* WF on Reconnect is sufficient because once disconnected, Reconnect
\* is continuously enabled until it fires. DropConnection is
\* intentionally NOT under any fairness — we want to allow but not
\* force drops; the spec must hold across every drop pattern.
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A c \in Chunks : SF_vars(DeliverChunk(c))
    /\ \A c \in Chunks : SF_vars(AckChunk(c))
    /\ WF_vars(Reconnect)

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ serverBuffer \subseteq Chunks
    /\ workerUnpinned \subseteq Chunks
    /\ connState \in ConnStates
    /\ inFlight \subseteq Chunks
    /\ pendingAcks \subseteq Chunks

\* SAFETY: server buffer never holds more than the original chunks
\* (no spurious additions).
ServerBufferBounded ==
    serverBuffer \subseteq Chunks

\* SAFETY: a chunk only leaves serverBuffer when the worker has
\* unpinned it. Equivalent to: serverBuffer ⊇ Chunks \ workerUnpinned.
\* (If the buffer dropped a chunk before the worker unpinned it, this
\* would fail — i.e. the ack handler is wrong.)
ServerBufferReleaseImpliesUnpin ==
    \A c \in Chunks : (c \notin serverBuffer) => (c \in workerUnpinned)

\* LIVENESS / TEMPORAL: every chunk is eventually unpinned by the
\* worker. This is the durability invariant — pin state must
\* eventually clear no matter how many drops happen.
\*
\* FixedMode (no resend bug): holds. Every drop's in-flight loss is
\* recovered by Reconnect's replay; WF on DeliverChunk drives the
\* replayed chunks to workerUnpinned.
\*
\* BugMode counter-example: drop the connection AFTER all chunks have
\* moved into inFlight but BEFORE any deliver — inFlight is wiped,
\* serverBuffer still holds them all. Reconnect (BugMode) does NOT
\* replay. inFlight stays empty forever. workerUnpinned never
\* receives any chunk. The temporal property is violated.
AllChunksEventuallyUnpinned ==
    <>(\A c \in Chunks : c \in workerUnpinned)

\* LIVENESS / TEMPORAL: server's resend buffer eventually drains.
\* Combined with AllChunksEventuallyUnpinned, this gives the full
\* contract: every chunk is unpinned AND every chunk is acked.
\*
\* FixedMode: holds. AllChunksEventuallyUnpinned ensures every chunk
\* lands on the worker → an ack is queued → AckChunk drains the
\* buffer.
\*
\* BugMode: also violated — chunks that are never delivered also
\* never ack, so the buffer keeps them forever.
ServerBufferEventuallyEmpty ==
    <>(serverBuffer = {})

============================================================================
