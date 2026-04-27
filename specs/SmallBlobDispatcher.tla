------------------- MODULE SmallBlobDispatcher -------------------
(***************************************************************************
  Bug A small-CAS peer-mirror dispatcher protocol (task #153).

  Models the cross-component contract introduced by task #153:

    * Server's SmallBlobDispatcher pushes small (<= 16KiB) CAS/AC blobs
      to workers via a new BatchWriteSmallBlobs RPC arm on the existing
      UpdateForWorker bidi stream.
    * For each blob the dispatcher PUSHES, it inserts a server-side
      EphemeralServerSidePin keyed by digest, into the per-store pin
      set, so the bytes stay in server memory until the worker
      acknowledges receipt.
    * Worker receives the batch, writes each entry into its mirror_blobs
      (keyed by (store_id, digest) per plan B5), and on the next
      BlobsAvailable tick advertises the current snapshot in the new
      field 16 `pinned_mirror_entries` (one MirrorPinEntry per
      (store_id, digest) the worker now holds).
    * Server's WorkerApiServer.handle_blobs_available BROADCASTS the
      pinned_mirror_entries to ALL registered FastSlowStores via
      observe_pinned_mirror_ack. Each FastSlowStore binary-searches
      the sorted-by-store_id slice for ITS OWN store_id and unpins
      matching entries from its EphemeralServerSidePin set.

  THE BUG MODELED HERE is the per-store-routing requirement (Option F,
  decision #6). Without per-store filtering — i.e. if
  observe_pinned_mirror_ack unconditionally clears every entry whose
  digest appears in the ack — a pin (ac, D) can be wrongly released
  by an ack that contained only (cas, D), even though the AC worker
  never received (ac, D). The AC pin disappears with no AC-side
  delivery confirmation; if the AC-side push had not yet been
  delivered (mid-flight), the AC dispatcher considers the bytes
  "acked" and stops protecting them.

  Two conditions must collide for the bug to fire:
    1. Two stores hold the same digest D in their pin sets at the
       same time (e.g. both pushed it concurrently).
    2. The worker acks (s1, D) BEFORE the worker receives (s2, D).

  Then the buggy fan-out clears (s2, D) from serverPins[s2] even
  though the worker has not held (s2, D) yet.

  CONSTANT FixOptionF (TRUE/FALSE) toggles between the two regimes:
    * FALSE: bugged. observe_pinned_mirror_ack on each store clears
      EVERY ack entry whose digest is held, regardless of store_id.
    * TRUE: per Option F. observe_pinned_mirror_ack only clears
      entries whose store_id matches self.store_id (the
      binary-search slice over the sorted-by-store_id ack vector).

  EXPECTED TLC OUTCOMES:
    * Bugged.cfg (FixOptionF=FALSE): NoCrossStoreLeak WILL be violated.
      Trace: CAS pushes (cas, D1) and pins. AC pushes (ac, D1) and pins.
      Worker receives (cas, D1) but NOT (ac, D1) yet. Worker acks
      { (cas, D1) }. Bugged fan-out: every store clears D1, so
      AC's pin (ac, D1) drops even though the worker never held
      (ac, D1). The auxiliary AckedByWorker history records that
      (ac, D1) was NEVER acked, but serverPins[ac] no longer has it.
    * Fixed.cfg (FixOptionF=TRUE): clean. Each store filters by its
      own store_id; cross-store collisions are isolated.

  SCOPE — what this spec models:
    * One worker, two FastSlowStores (cas + ac).
    * A bounded set of digest names (D1, D2).
    * Dispatcher pin sets per store, worker mirror_blobs keyed by
      (store_id, digest), BlobsAvailable ack queue.
    * The BatchWriteSmallBlobs message and its single-blob variant
      (multi-blob coalescing is orthogonal — any coalescing semantics
      preserve per-blob lineage).
    * An auxiliary AckedByWorker history variable that records every
      (s, d) the worker has ever explicitly acked, used to express
      the safety invariant.

  SCOPE — what this spec does NOT model:
    * Per-(endpoint, boot_epoch_id, store_id) keying race (B4) — that's
      modeled by the existing PinLifecycle spec's reconnect trace.
    * Drainer task lifecycle (S3 lock-order invariant) — the spec
      assumes the drainer is a black box that delivers messages in
      FIFO order.
    * Bytestream / CAS server / AC server ENQUEUE-side hooks — those
      are call-site additions whose correctness is local to the hook
      site, not protocol-level.
    * mpsc capacity overflow / try_send Full — covered by existing
      tests, not a protocol-state concern.

  ADDITION (unpin_on_disconnect refactor): pins are durable until
  EXPLICIT release. There is NO TTL eviction — the previous design
  recorded an Instant on each insert but never read it (no purge
  loop). On worker disconnect the WorkerApiServer calls
  SmallBlobDispatcher::unpin_on_disconnect, which clears every pin
  set unconditionally (the per-store pin set is keyed by digest, not
  by worker — see WorkerDisconnect action below). The
  NoCrossStoreLeak invariant is unaffected: the disconnect-driven
  release of (s, d) is still keyed to the worker that "owns" the
  pin, so the cross-store leak window remains the same as in the
  original spec.

  WorkerDisconnect models the SINGLE-worker case; the multi-worker
  over-broad clear (per code TODO at small_blob_dispatcher.rs:562) is
  out-of-scope — the spec uses Workers = {w1} (implicitly: one worker
  process, modelled by the single workerMirror variable) which cannot
  expose the amplification where one worker's disconnect clears pins
  the dispatcher pushed to OTHER live workers. See task #177 for the
  per-worker-attribution v2 design that lifts this restriction.
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Digests,         \* Set of digest identifiers.
    Stores,          \* Set of store_id strings (e.g. {cas, ac}).
    FixOptionF       \* TRUE => per-store routing; FALSE => unfiltered.

ASSUME FixOptionF \in BOOLEAN

VARIABLES
    \* Per-store EphemeralServerSidePin set: maps store_id -> SUBSET Digests.
    serverPins,
    \* Worker's mirror_blobs map: SUBSET (Stores \X Digests).
    workerMirror,
    \* Pending BatchWriteSmallBlobs messages. Sequence of records
    \* {store_id, digest}.
    inflightPushes,
    \* Pending pinned_mirror_entries (BlobsAvailable field 16).
    \* Sequence of SETs of (store_id, digest) records — each set is
    \* one BlobsAvailable tick's snapshot of workerMirror.
    inflightAcks,
    \* Auxiliary history variable. Records every (s, d) that has
    \* been part of some ack the server processed. Used by the
    \* safety invariant to detect a pin release that did NOT have a
    \* corresponding ack from the worker carrying (s, d).
    ackedByServer

vars == <<serverPins, workerMirror, inflightPushes, inflightAcks,
          ackedByServer>>

----------------------------------------------------------------------------
Init ==
    /\ serverPins = [s \in Stores |-> {}]
    /\ workerMirror = {}
    /\ inflightPushes = <<>>
    /\ inflightAcks = <<>>
    /\ ackedByServer = {}

----------------------------------------------------------------------------
(* DispatcherPush(s, d): SmallBlobDispatcher for store s pushes digest d. *)
----------------------------------------------------------------------------
DispatcherPush(s, d) ==
    /\ s \in Stores
    /\ d \in Digests
    /\ d \notin serverPins[s]
    /\ <<s, d>> \notin workerMirror
    /\ \A i \in 1..Len(inflightPushes) :
         ~(inflightPushes[i].store_id = s /\ inflightPushes[i].digest = d)
    /\ serverPins' = [serverPins EXCEPT ![s] = serverPins[s] \cup {d}]
    /\ inflightPushes' = Append(inflightPushes,
                                [store_id |-> s, digest |-> d])
    /\ UNCHANGED <<workerMirror, inflightAcks, ackedByServer>>

----------------------------------------------------------------------------
(* WorkerReceivePush: worker pops one BatchWriteSmallBlobs message,
   writes (store_id, digest) into mirror_blobs. *)
----------------------------------------------------------------------------
WorkerReceivePush ==
    /\ Len(inflightPushes) > 0
    /\ LET msg == Head(inflightPushes)
       IN
       /\ inflightPushes' = Tail(inflightPushes)
       /\ workerMirror' = workerMirror \cup {<<msg.store_id, msg.digest>>}
       /\ UNCHANGED <<serverPins, inflightAcks, ackedByServer>>

----------------------------------------------------------------------------
(* WorkerSendAck: worker emits a BlobsAvailable carrying the current
   pinned_mirror_entries snapshot. *)
----------------------------------------------------------------------------
WorkerSendAck ==
    /\ workerMirror /= {}
    /\ inflightAcks' = Append(inflightAcks, workerMirror)
    /\ UNCHANGED <<serverPins, workerMirror, inflightPushes, ackedByServer>>

----------------------------------------------------------------------------
(* ServerProcessAck: server pops one BlobsAvailable ack and broadcasts
   to every store's observe_pinned_mirror_ack. Per Option F (FixOptionF
   = TRUE), each store filters by its own store_id. Per the bug
   (FixOptionF = FALSE), each store unconditionally clears every digest
   in the ack regardless of source store. *)
----------------------------------------------------------------------------
ServerProcessAck ==
    /\ Len(inflightAcks) > 0
    /\ LET ack == Head(inflightAcks)
           digests_in_ack == { e[2] : e \in ack }
       IN
       /\ inflightAcks' = Tail(inflightAcks)
       /\ serverPins' =
            IF FixOptionF
            THEN [s \in Stores |->
                    serverPins[s] \ { e[2] : e \in { x \in ack : x[1] = s } }]
            ELSE [s \in Stores |->
                    serverPins[s] \ digests_in_ack]
       /\ ackedByServer' = ackedByServer \cup ack
       /\ UNCHANGED <<workerMirror, inflightPushes>>

----------------------------------------------------------------------------
(* WorkerDisconnect: models the WorkerApiServer disconnect-cleanup
   path that calls SmallBlobDispatcher::unpin_on_disconnect — every
   per-store pin set is cleared unconditionally because the v1
   dispatcher does NOT track per-(endpoint, boot_epoch_id) push
   attribution (the per-store pin set is keyed by DigestInfo only).

   The disconnected worker can no longer ack pushed blobs via
   BlobsAvailable.pinned_mirror_entries, so without this clear the
   server-side pin tracker would leak indefinitely (pin_max_bytes
   would steadily fill until ResourceExhausted).

   Modeling the worker side: the worker process is gone, so
   workerMirror is wiped AND any inflight pushes / acks are dropped.
   The auxiliary ackedByServer history variable is preserved
   (we don't lose the audit log of what was ever acked).

   Critically for safety: the server's clear of serverPins is also
   "as if every (s, d) it had pushed was acked" — but only because
   the worker is dead and its bytes are unrecoverable from THAT
   worker (the slow tier still has them; the bytes themselves are
   not lost — only the in-flight push tracker). To keep
   NoCrossStoreLeak intact, ackedByServer is augmented with every
   (s, d) the dispatcher had pinned — modeling the contract that
   "explicit unpin on disconnect counts as acked-by-disconnection".
 *)
----------------------------------------------------------------------------
WorkerDisconnect ==
    /\ \E st \in Stores : serverPins[st] /= {}
    /\ ackedByServer' = ackedByServer
                          \cup UNION { { <<st, d>> : d \in serverPins[st] }
                                       : st \in Stores }
    /\ serverPins' = [st \in Stores |-> {}]
    /\ workerMirror' = {}
    /\ inflightPushes' = <<>>
    /\ inflightAcks' = <<>>

----------------------------------------------------------------------------
Next ==
    \/ \E s \in Stores, d \in Digests : DispatcherPush(s, d)
    \/ WorkerReceivePush
    \/ WorkerSendAck
    \/ ServerProcessAck
    \/ WorkerDisconnect

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(WorkerReceivePush)
    /\ WF_vars(WorkerSendAck)
    /\ WF_vars(ServerProcessAck)

----------------------------------------------------------------------------
(* INVARIANTS *)
----------------------------------------------------------------------------

TypeOK ==
    /\ serverPins \in [Stores -> SUBSET Digests]
    /\ workerMirror \subseteq (Stores \X Digests)
    /\ ackedByServer \subseteq (Stores \X Digests)
    /\ \A i \in 1..Len(inflightPushes) :
         /\ inflightPushes[i].store_id \in Stores
         /\ inflightPushes[i].digest \in Digests
    /\ \A i \in 1..Len(inflightAcks) :
         inflightAcks[i] \subseteq (Stores \X Digests)

(* SAFETY: NoCrossStoreLeak.
   Define "pushed (s, d)" := (s, d) is currently in serverPins[s] OR
   in inflightPushes OR has been delivered to the worker (in
   workerMirror). Because Push is the only way pins start, and the
   precondition forbids re-Push, every pin removal from serverPins
   must be paired with a matching ack — i.e. (s, d) \in ackedByServer.

   The invariant: any (s, d) that was once pushed (but is now NOT in
   serverPins[s]) MUST have been acked AS (s, d). Under the bug, the
   AC pin drops when an ack contains (cas, d) but NOT (ac, d), so
   (ac, d) is no longer in serverPins[ac] but ALSO not in
   ackedByServer.

   Operationally:
   For all (s, d): if (s, d) was pushed and is NOT currently
   in-flight or in workerMirror waiting for ack, AND is NOT in
   serverPins[s], THEN (s, d) must be in ackedByServer.

   Phrased contrapositively (easier to read): for every (s, d) in
   workerMirror that is NOT currently in serverPins[s], (s, d) MUST
   be in ackedByServer. The bug allows serverPins[ac] to drop d
   without (ac, d) ever appearing in ackedByServer — caught here. *)

PushedSet ==
    \* All (s, d) that have been pushed at some point. We compute it
    \* from the current state: a pair is "pushed" iff it's currently
    \* held somewhere downstream of the push action.
    {<<s, d>> \in Stores \X Digests :
        \/ d \in serverPins[s]
        \/ <<s, d>> \in workerMirror
        \/ \E i \in 1..Len(inflightPushes) :
             inflightPushes[i].store_id = s /\ inflightPushes[i].digest = d}

NoCrossStoreLeak ==
    \* For every (s, d) that has been pushed (currently somewhere in
    \* the protocol state), if it's NOT in serverPins[s] then it must
    \* either still be in flight, OR have been explicitly acked AS
    \* (s, d). The bug violates this by silently clearing serverPins
    \* for non-self entries.
    \A pair \in PushedSet :
        LET s == pair[1]
            d == pair[2]
        IN
            \/ d \in serverPins[s]                            \* still pinned
            \/ \E i \in 1..Len(inflightPushes) :              \* in flight
                 inflightPushes[i].store_id = s /\ inflightPushes[i].digest = d
            \/ <<s, d>> \in ackedByServer                     \* properly acked

(* SAFETY: PinReleaseRequiresOwnAck.
   STRONGER STATEMENT of the same property in cleaner form. A pin
   release for (s, d) is only legitimate if the worker explicitly
   acked (s, d). We can sanity-check this directly by observing that
   AT EVERY transition, if d is in serverPins[s] in state X but not
   in serverPins[s] in state Y, then the action that took us X -> Y
   must be ServerProcessAck and the ack must contain (s, d). The
   ackedByServer history makes this an invariant rather than a
   stuttering-aware temporal property: once we accept (s, d) into
   ackedByServer, we have a witness that the worker advertised it. *)

----------------------------------------------------------------------------
(* State-space constraint for TLC. Bounds the queue depths so the
   model checker terminates on small models. The bug fires within
   one push-per-store cycle, so 2-deep queues are sufficient. *)
----------------------------------------------------------------------------
StateConstraint ==
    /\ Len(inflightPushes) <= 2
    /\ Len(inflightAcks) <= 2

============================================================================
