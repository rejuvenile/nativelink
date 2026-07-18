-------------------- MODULE SmallBlobDispatcherAsync --------------------
(***************************************************************************
  ATOMICITY REFINEMENT of SmallBlobDispatcher.tla (batch-E, 2026-07-17).

  ------------------------------------------------------------------------
  WHY THIS SPEC EXISTS
  ------------------------------------------------------------------------
  The base spec SmallBlobDispatcher.tla FUSES two production steps into one
  atom in `WorkerReceivePush`:

      WorkerReceivePush ==            \* base spec, lines 163-169
          workerMirror' = workerMirror \cup {<<msg.store_id, msg.digest>>}

  In production (task #153, plan B5) the worker does THREE separable things
  after popping a BatchWriteSmallBlobs message off the UpdateForWorker
  stream:
    (1) RECEIVE the bytes into an in-memory staging buffer (network RX),
    (2) DURABLY WRITE each entry into mirror_blobs on DISK — an async op
        that CAN FAIL (ENOSPC, fs error, worker crash mid-write, the slow
        tier rejecting the write), and
    (3) on the next BlobsAvailable tick, ADVERTISE the pinned_mirror_entries
        snapshot so the server can release its EphemeralServerSidePin.

  The base spec collapses (1) and (2) into one atom (receipt == durable) and
  its WorkerSendAck advertises `workerMirror` — i.e. it can only ever
  advertise things that are (by fusion) already durable. That erases the
  exact precondition of the FL-688 bug class: the server releasing the
  server-memory bytes (the ephemeral pin) on an advertisement of a blob the
  worker has RECEIVED but has NOT yet made DURABLE. If the durable write
  then fails, the bytes are gone from BOTH the server (unpinned) and the
  worker (write failed) -> silent data loss, the write-once-lose-never shape.

  This is the SAME defect class as the exemplar MarkStableViaBlobsAvailable.tla
  (advertise-before-durable), which shipped precisely because the fused atom
  made the buggy interleaving unreachable.

  ------------------------------------------------------------------------
  THE SPLIT
  ------------------------------------------------------------------------
    * WorkerReceivePush   : inflightPushes -> workerReceived  (RX only, in mem)
    * WorkerDurableWrite  : workerReceived -> workerMirror     (disk write OK)
    * WorkerDurableWriteFail: workerReceived -> writeFailed    (disk write FAILS,
                              permanent; no retry modeled = the adversary)
    * WorkerSendAck       : advertise a snapshot. The CONSTANT
                            AdvertiseOnlyDurable decides WHAT is advertised:
        - FALSE (BUGGED):  advertise workerReceived \cup workerMirror
                           -> advertises received-but-not-yet-durable entries
                           (advertise-before-durable).
        - TRUE  (FIXED):   advertise ONLY workerMirror
                           -> a blob is advertised (and thus can be unpinned)
                           only AFTER its durable disk write is confirmed.
    * ServerProcessAck    : pop one ack, clear the matching EphemeralServerSidePin
                            (per-store filter; the cross-store routing bug is
                            orthogonal and already covered by the base spec,
                            so per-store filtering is baked correct here).

  ------------------------------------------------------------------------
  SAFETY INVARIANT  (DurableBeforeUnpin)
  ------------------------------------------------------------------------
  The EphemeralServerSidePin exists to keep the bytes in server memory until
  the worker has DURABLY stored them. So: any pushed (s,d) that is no longer
  pinned and no longer in flight MUST be durable on the worker. Formally,
  for every pushed (s,d):
        d \in serverPins[s]                 (still protected in server mem)
     \/ (s,d) still in inflightPushes        (not yet even received)
     \/ (s,d) \in workerMirror               (DURABLE on worker disk)

  BUGGED: the server unpins on an advertisement carrying a received-but-not-
  durable entry; that pair is then neither pinned, nor in-flight, nor (yet)
  in workerMirror -> VIOLATION at the advertise-before-durable window, and it
  hardens into permanent loss once WorkerDurableWriteFail fires.

  FIXED: the server unpins only on durable advertisements, so an unpinned
  delivered pair is always in workerMirror (which is monotone here — no
  eviction modeled). Holds. When a durable write fails in the FIXED regime,
  the entry is never advertised, so the server KEEPS the pin (correct
  backpressure: bytes stay protected until durability is confirmed / retried).

  ------------------------------------------------------------------------
  MODEL SIZE: one store, two digests. The bug fires with one store + one
  digest; two digests exercise interleaving. Queue depths bounded to 2.

  CITATIONS (production analogs):
    [rx]   small_blob_dispatcher.rs  BatchWriteSmallBlobs receive path
    [disk] worker mirror_blobs durable write (async, fallible)
    [adv]  BlobsAvailable.pinned_mirror_entries (field 16) advertisement
    [unpin]WorkerApiServer.handle_blobs_available -> observe_pinned_mirror_ack
    [exemplar] specs/MarkStableViaBlobsAvailable.tla (same defect class)
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Digests,             \* Set of digest identifiers, e.g. {d1, d2}.
    Stores,              \* Set of store_id strings, e.g. {s1}.
    AdvertiseOnlyDurable \* TRUE => FIXED (advertise only durable);
                         \* FALSE => BUGGED (advertise received-but-not-durable).

ASSUME AdvertiseOnlyDurable \in BOOLEAN

VARIABLES
    serverPins,       \* [Stores -> SUBSET Digests]  (EphemeralServerSidePin)
    inflightPushes,   \* Seq of [store_id, digest]   (BatchWriteSmallBlobs RX queue)
    workerReceived,   \* SUBSET (Stores \X Digests)  (in mem, durability PENDING)
    workerMirror,     \* SUBSET (Stores \X Digests)  (DURABLE on worker disk)
    inflightAcks,     \* Seq of SETs of (s,d)        (pinned_mirror_entries in-flight)
    writeFailed,      \* SUBSET (Stores \X Digests)  (durable write FAILED, permanent)
    everPushed        \* SUBSET (Stores \X Digests)  (aux history: ever dispatched)

vars == <<serverPins, inflightPushes, workerReceived, workerMirror,
          inflightAcks, writeFailed, everPushed>>

----------------------------------------------------------------------------
Init ==
    /\ serverPins = [s \in Stores |-> {}]
    /\ inflightPushes = <<>>
    /\ workerReceived = {}
    /\ workerMirror = {}
    /\ inflightAcks = <<>>
    /\ writeFailed = {}
    /\ everPushed = {}

----------------------------------------------------------------------------
(* DispatcherPush(s,d): server pins the digest (EphemeralServerSidePin) and
   enqueues a BatchWriteSmallBlobs message. Each (s,d) is pushed at most once
   (guarded on everPushed) — the bug fires on the first push; bounding avoids
   re-push cycling. *)
----------------------------------------------------------------------------
DispatcherPush(s, d) ==
    /\ s \in Stores
    /\ d \in Digests
    /\ <<s, d>> \notin everPushed
    /\ serverPins' = [serverPins EXCEPT ![s] = serverPins[s] \cup {d}]
    /\ inflightPushes' = Append(inflightPushes,
                                [store_id |-> s, digest |-> d])
    /\ everPushed' = everPushed \cup {<<s, d>>}
    /\ UNCHANGED <<workerReceived, workerMirror, inflightAcks, writeFailed>>

----------------------------------------------------------------------------
(* WorkerReceivePush: worker pops one BatchWriteSmallBlobs message and stages
   the entry IN MEMORY. This is the network RX boundary ONLY — the durable
   disk write has NOT happened yet. *)
----------------------------------------------------------------------------
WorkerReceivePush ==
    /\ Len(inflightPushes) > 0
    /\ LET msg == Head(inflightPushes)
       IN
       /\ inflightPushes' = Tail(inflightPushes)
       /\ workerReceived' = workerReceived \cup {<<msg.store_id, msg.digest>>}
       /\ UNCHANGED <<serverPins, workerMirror, inflightAcks, writeFailed,
                      everPushed>>

----------------------------------------------------------------------------
(* WorkerDurableWrite: an async disk write for a staged entry SUCCEEDS. The
   entry becomes DURABLE (workerMirror) and leaves the staging buffer. *)
----------------------------------------------------------------------------
WorkerDurableWrite ==
    /\ \E pair \in workerReceived :
        /\ workerMirror' = workerMirror \cup {pair}
        /\ workerReceived' = workerReceived \ {pair}
        /\ UNCHANGED <<serverPins, inflightPushes, inflightAcks, writeFailed,
                       everPushed>>

----------------------------------------------------------------------------
(* WorkerDurableWriteFail: an async disk write for a staged entry FAILS
   permanently (ENOSPC / fs error / crash mid-write). The entry is dropped
   from staging and NEVER becomes durable. This is the adversary the fused
   atom erased. *)
----------------------------------------------------------------------------
WorkerDurableWriteFail ==
    /\ \E pair \in workerReceived :
        /\ writeFailed' = writeFailed \cup {pair}
        /\ workerReceived' = workerReceived \ {pair}
        /\ UNCHANGED <<serverPins, inflightPushes, workerMirror, inflightAcks,
                       everPushed>>

----------------------------------------------------------------------------
(* WorkerSendAck: worker emits a BlobsAvailable carrying the current
   pinned_mirror_entries snapshot.

   BUGGED  (AdvertiseOnlyDurable = FALSE): advertises workerReceived \cup
     workerMirror — i.e. it advertises entries that are only received-in-
     memory, not yet durable. This is the advertise-before-durable defect.
   FIXED   (AdvertiseOnlyDurable = TRUE): advertises ONLY workerMirror. A
     blob is advertised (hence unpinnable) only after its durable write. *)
----------------------------------------------------------------------------
Advertisable ==
    IF AdvertiseOnlyDurable
    THEN workerMirror
    ELSE workerReceived \cup workerMirror

WorkerSendAck ==
    /\ Advertisable /= {}
    /\ inflightAcks' = Append(inflightAcks, Advertisable)
    /\ UNCHANGED <<serverPins, inflightPushes, workerReceived, workerMirror,
                   writeFailed, everPushed>>

----------------------------------------------------------------------------
(* ServerProcessAck: server pops one BlobsAvailable ack and releases the
   EphemeralServerSidePin for each advertised (s,d), filtered per-store
   (the correct Option-F routing). Releasing the pin frees the server-memory
   bytes — legitimate ONLY if the worker has the bytes durably. *)
----------------------------------------------------------------------------
ServerProcessAck ==
    /\ Len(inflightAcks) > 0
    /\ LET ack == Head(inflightAcks)
       IN
       /\ inflightAcks' = Tail(inflightAcks)
       /\ serverPins' =
            [s \in Stores |->
                serverPins[s] \ { e[2] : e \in { x \in ack : x[1] = s } }]
       /\ UNCHANGED <<inflightPushes, workerReceived, workerMirror,
                      writeFailed, everPushed>>

----------------------------------------------------------------------------
Next ==
    \/ \E s \in Stores, d \in Digests : DispatcherPush(s, d)
    \/ WorkerReceivePush
    \/ WorkerDurableWrite
    \/ WorkerDurableWriteFail
    \/ WorkerSendAck
    \/ ServerProcessAck

Spec ==
    /\ Init
    /\ [][Next]_vars

----------------------------------------------------------------------------
(* INVARIANTS *)
----------------------------------------------------------------------------

TypeOK ==
    /\ serverPins \in [Stores -> SUBSET Digests]
    /\ workerReceived \subseteq (Stores \X Digests)
    /\ workerMirror \subseteq (Stores \X Digests)
    /\ writeFailed \subseteq (Stores \X Digests)
    /\ everPushed \subseteq (Stores \X Digests)
    /\ \A i \in 1..Len(inflightPushes) :
         /\ inflightPushes[i].store_id \in Stores
         /\ inflightPushes[i].digest \in Digests
    /\ \A i \in 1..Len(inflightAcks) :
         inflightAcks[i] \subseteq (Stores \X Digests)

InFlight(s, d) ==
    \E i \in 1..Len(inflightPushes) :
        inflightPushes[i].store_id = s /\ inflightPushes[i].digest = d

(* SAFETY: DurableBeforeUnpin.
   Every pushed (s,d) that is neither still pinned nor still in flight MUST
   be durable on the worker. The advertise-before-durable bug releases the
   pin while the bytes are only in workerReceived (or already in
   writeFailed), violating this. *)
DurableBeforeUnpin ==
    \A pair \in everPushed :
        LET s == pair[1]
            d == pair[2]
        IN
            \/ d \in serverPins[s]          \* still protected in server memory
            \/ InFlight(s, d)               \* not yet received by worker
            \/ pair \in workerMirror        \* DURABLE on worker disk

(* SAFETY: NoUnpinnedLostBlob.
   The hardened form: a pushed (s,d) whose durable write FAILED and is no
   longer pinned/in-flight is permanently lost. This fires strictly after
   DurableBeforeUnpin in the bugged trace (once the write actually fails). *)
NoUnpinnedLostBlob ==
    \A pair \in everPushed :
        (pair \in writeFailed) =>
            \/ pair[2] \in serverPins[pair[1]]
            \/ InFlight(pair[1], pair[2])

(* WITNESS (non-vacuity, run as an INVARIANT expected to be VIOLATED).
   Asserts a pushed digest is NEVER fully released via a durable path. If the
   FIXED model can drive Push -> Receive -> DurableWrite -> SendAck ->
   ServerProcessAck to completion, this is VIOLATED — the counterexample IS
   the proof that "no DurableBeforeUnpin violation" is not vacuous. *)
NoDurableUnpinReached ==
    ~(\E pair \in everPushed :
        /\ pair[2] \notin serverPins[pair[1]]
        /\ pair \in workerMirror )

----------------------------------------------------------------------------
(* State-space bound. The bug fires within one push/receive/advertise/unpin
   cycle, so 2-deep queues suffice. *)
----------------------------------------------------------------------------
StateConstraint ==
    /\ Len(inflightPushes) <= 2
    /\ Len(inflightAcks) <= 2

============================================================================
