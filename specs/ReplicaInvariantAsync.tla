--------------------------- MODULE ReplicaInvariantAsync ---------------------------
(***************************************************************************
  DE-ATOMIZED variant of ReplicaInvariant.tla (atomicity-audit batch A,
  2026-07-17).

  THE FALSE ATOMICITY IN THE ORIGINAL SPEC
    ReplicaInvariant.tla `MirrorWriteOk` fused the peer worker's mirror
    WRITE with the peer replica's DURABILITY-forever: once peerMirror
    flipped to "Present" it never changed again. The only eviction action
    was `FastEvict` on the SERVER's fast tier; the peer mirror had NO
    eviction. The doc even leans on this:
        "a subsequent FastEvict legitimately brings ReplicaCount to 1 and
         that is fine for read recovery (peer mirror still serves)."
    But the peer mirror is ALSO a MemoryStore — an LRU cache subject to
    its own eviction. "peer wrote it" and "peer still has it durably" are
    NOT the same step; a peer LRU eviction can fire in the gap AFTER ack.
    Fusing peer-write with peer-durable made the "both in-memory replicas
    evicted after ack" state UNREACHABLE, so `NoLossyAckCascade` (post-ack
    >= 1 replica) passed only because the model forbade the peer to evict.

    This is the FL-688 durability class: two in-memory replicas give
    AVAILABILITY at ack time but are NOT a durable floor. Durability comes
    from the slow tier + BlobsInStableStorage ack (CLAUDE.md architecture
    invariant), which is a SEPARATE, non-evictable copy the original spec
    did not model at all.

  WHAT SPLITTING SURFACES
    We add:
      * PeerEvict  -- peer MemoryStore LRU eviction, symmetric to FastEvict
                      (post-ack, same class), so BOTH in-memory replicas
                      can vanish after ack.
      * slowDurable + SlowWriteDurable -- the slow-tier write completing +
                      BIS-acked durable copy, which is NOT evictable.
      * GateOnDurable -- whether AckBazel additionally requires the durable
                      copy to exist (the real durability gate).

    Both cfgs keep GateOnMirror = TRUE (the ORIGINAL mirror fix in place),
    so we isolate the NEW durability gap: even WITH two in-memory replicas
    at ack, losing both to eviction loses the blob unless a non-evictable
    durable tier was gated on.

    NoLossyAckCascade is stated as:
        ack = "AckedOk" => (ReplicaCount >= 1 \/ slowDurable = "Durable")
      * GateOnDurable = FALSE (Bugged): ack does not require the durable
        copy; PeerEvict + FastEvict drop ReplicaCount to 0 with
        slowDurable = "Absent" -> VIOLATED.
      * GateOnDurable = TRUE  (Fixed): AckBazel requires slowDurable =
        "Durable"; the durable copy never evicts -> HOLDS even if both
        in-memory replicas evict.

  CITATIONS (same sites as ReplicaInvariant.tla, plus):
    [dur]  architecture-invariants memory: "durability comes from the
           mirror_blobs >=2-replica invariant + BlobsInStableStorage ack";
           ZFS pool sync=disabled so a written-not-yet-BIS-acked blob is
           NOT durable.
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    GateOnMirror,   \* TRUE => cache/ack require fast AND mirror (orig fix; TRUE here)
    GateOnDurable   \* TRUE => ack additionally requires the durable copy

ASSUME GateOnMirror \in BOOLEAN
ASSUME GateOnDurable \in BOOLEAN

ReplicaStates == {"Absent", "Present"}
DurableStates == {"Absent", "Durable"}
CacheStates   == {"NotPresent", "Positive"}
AckStates     == {"Pending", "AckedOk"}

VARIABLES
    serverFast,         \* ReplicaStates: server fast tier (MemoryStore)
    peerMirror,         \* ReplicaStates: peer worker MemoryStore replica
    slowDurable,        \* DurableStates: slow-tier + BIS-acked durable copy
    cacheState,         \* CacheStates
    ack,                \* AckStates
    ackedWithCount,     \* Nat: in-mem replica count at the moment ack was set
    ackedDurable        \* BOOLEAN: was the durable copy present at ack moment

vars == <<serverFast, peerMirror, slowDurable, cacheState, ack,
          ackedWithCount, ackedDurable>>

----------------------------------------------------------------------------
Init ==
    /\ serverFast     = "Absent"
    /\ peerMirror     = "Absent"
    /\ slowDurable    = "Absent"
    /\ cacheState     = "NotPresent"
    /\ ack            = "Pending"
    /\ ackedWithCount = 0
    /\ ackedDurable   = FALSE

----------------------------------------------------------------------------
(* FastWriteOk: server fast-tier write succeeds.                            *)
----------------------------------------------------------------------------
FastWriteOk ==
    /\ serverFast = "Absent"
    /\ serverFast' = "Present"
    /\ UNCHANGED <<peerMirror, slowDurable, cacheState, ack, ackedWithCount, ackedDurable>>

----------------------------------------------------------------------------
(* MirrorWriteOk: peer worker MemoryStore accepts the mirror WRITE.         *)
(* (Peer now HAS the bytes — but only in its own evictable MemoryStore.)    *)
----------------------------------------------------------------------------
MirrorWriteOk ==
    /\ peerMirror = "Absent"
    /\ peerMirror' = "Present"
    /\ UNCHANGED <<serverFast, slowDurable, cacheState, ack, ackedWithCount, ackedDurable>>

----------------------------------------------------------------------------
(* SlowWriteDurable: the async slow-tier write completes AND is BIS-acked,  *)
(* producing a NON-EVICTABLE durable copy. Requires the fast tier to have   *)
(* had the bytes at some point (write source).                             *)
----------------------------------------------------------------------------
SlowWriteDurable ==
    /\ slowDurable = "Absent"
    /\ serverFast = "Present"
    /\ slowDurable' = "Durable"
    /\ UNCHANGED <<serverFast, peerMirror, cacheState, ack, ackedWithCount, ackedDurable>>

----------------------------------------------------------------------------
(* CachePositive: cache adds a positive entry. Gated like the original.     *)
----------------------------------------------------------------------------
CachePositive ==
    /\ cacheState = "NotPresent"
    /\ serverFast = "Present"
    /\ (~GateOnMirror \/ peerMirror = "Present")
    /\ (~GateOnDurable \/ slowDurable = "Durable")
    /\ cacheState' = "Positive"
    /\ UNCHANGED <<serverFast, peerMirror, slowDurable, ack, ackedWithCount, ackedDurable>>

----------------------------------------------------------------------------
(* AckBazel: server returns Ok to Bazel.                                    *)
(*   GateOnMirror  => both in-memory replicas present at ack.               *)
(*   GateOnDurable => durable copy present at ack (the real fix).           *)
----------------------------------------------------------------------------
AckBazel ==
    /\ ack = "Pending"
    /\ serverFast = "Present"
    /\ (~GateOnMirror \/ peerMirror = "Present")
    /\ (~GateOnDurable \/ slowDurable = "Durable")
    /\ ack' = "AckedOk"
    /\ ackedWithCount' =
         (IF serverFast = "Present" THEN 1 ELSE 0) +
         (IF peerMirror = "Present" THEN 1 ELSE 0)
    /\ ackedDurable' = (slowDurable = "Durable")
    /\ UNCHANGED <<serverFast, peerMirror, slowDurable, cacheState>>

----------------------------------------------------------------------------
(* FastEvict: server fast-tier LRU eviction after ack.                      *)
----------------------------------------------------------------------------
FastEvict ==
    /\ serverFast = "Present"
    /\ ack = "AckedOk"
    /\ serverFast' = "Absent"
    /\ UNCHANGED <<peerMirror, slowDurable, cacheState, ack, ackedWithCount, ackedDurable>>

----------------------------------------------------------------------------
(* PeerEvict: peer worker MemoryStore LRU eviction after ack. THE SPLIT     *)
(* the original spec omitted — peer-write != peer-durable-forever.          *)
----------------------------------------------------------------------------
PeerEvict ==
    /\ peerMirror = "Present"
    /\ ack = "AckedOk"
    /\ peerMirror' = "Absent"
    /\ UNCHANGED <<serverFast, slowDurable, cacheState, ack, ackedWithCount, ackedDurable>>

----------------------------------------------------------------------------
Next ==
    \/ FastWriteOk
    \/ MirrorWriteOk
    \/ SlowWriteDurable
    \/ CachePositive
    \/ AckBazel
    \/ FastEvict
    \/ PeerEvict

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ serverFast  \in ReplicaStates
    /\ peerMirror  \in ReplicaStates
    /\ slowDurable \in DurableStates
    /\ cacheState  \in CacheStates
    /\ ack         \in AckStates
    /\ ackedWithCount \in 0..2
    /\ ackedDurable \in BOOLEAN

ReplicaCount ==
    (IF serverFast = "Present" THEN 1 ELSE 0) +
    (IF peerMirror = "Present" THEN 1 ELSE 0)

(* Original property: >=2 in-memory replicas AT the ack moment. Holds in   *)
(* both cfgs (GateOnMirror = TRUE everywhere here).                         *)
AckImpliesTwoReplicasAtAck ==
    ack = "AckedOk" => ackedWithCount >= 2

(* SAFETY: NoLossyAckCascade — after ack, the blob is still RECOVERABLE:    *)
(* at least one in-memory replica OR the non-evictable durable copy.        *)
(*   Bugged (GateOnDurable = FALSE): FastWriteOk -> MirrorWriteOk ->        *)
(*     AckBazel (count 2, durable Absent) -> FastEvict -> PeerEvict ->      *)
(*     ReplicaCount = 0 AND slowDurable = "Absent" -> VIOLATED.             *)
(*   Fixed  (GateOnDurable = TRUE): ack requires slowDurable = "Durable";   *)
(*     the durable copy never evicts -> HOLDS.                              *)
NoLossyAckCascade ==
    ack = "AckedOk" => (ReplicaCount >= 1 \/ slowDurable = "Durable")

(* Reachability witness (run as INVARIANT expected to VIOLATE in Fixed):    *)
(* proves the acked state is genuinely reached, so NoLossyAckCascade is     *)
(* not vacuous.                                                             *)
NeverAcked == ack # "AckedOk"

============================================================================
