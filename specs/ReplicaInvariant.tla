--------------------------- MODULE ReplicaInvariant ---------------------------
(***************************************************************************
  CAS write replica invariant: every successful write produces ≥2
  in-memory replicas BEFORE the user-visible Ack returns.

  Models the 2026-04-23 design ("project_cas_write_invariant.md"):
    1. Server's fast tier (MemoryStore).
    2. A remote worker's MemoryStore via the tee/mirror path.

  The historic bug at existence_cache_store.rs:334-337 / :402-405 and
  bytestream_server.rs:1605 / :1878-1880:
    * cache positive was set as soon as inner_store.update.is_ok(),
    * mirror was background_spawn! and not awaited,
    * Ack to Bazel returned BEFORE mirror RPC even began,
    * Bazel could then see Ok, the in-memory MemoryStore could evict,
      and a subsequent NotFound would manifest with no second replica.

  CONSTANT GateOnMirror toggles whether the cache-positive insert is
  gated on the mirror tee completing:
    * GateOnMirror = FALSE: original buggy behavior. Ack/cache-positive
      depend ONLY on the fast-tier write. Spec admits a trace where
      the cache says "have it" with replica_count = 1 -> invariant
      violated.
    * GateOnMirror = TRUE: fixed behavior. Ack/cache-positive only
      after both fast-tier AND mirror complete. Spec is bug-free.

  EXPECTED TLC OUTCOMES:
    * ReplicaInvariantBugged.cfg (GateOnMirror = FALSE): INVARIANT
      VIOLATED on `AckImpliesTwoReplicas`.
    * ReplicaInvariantFixed.cfg  (GateOnMirror = TRUE):  no violation.

  SCOPE — what this spec models:
    * one Bazel-side write of one digest D,
    * server's MemoryStore (fast tier) and a single peer worker's
      MemoryStore as the only two in-memory replicas of interest,
    * the ExistenceCache positive entry as a separate state variable,
    * the user-visible Ack as a separate state variable,
    * fast-tier eviction (the "missing blob" cascade).

  SCOPE — what this spec does NOT model:
    * multi-peer fanout (model has 1 peer; the bug shape is the same
      with N peers as long as there is at least 1 required mirror),
    * peer quarantine / no-peers-connected fallback (RESOLVED 2026-04-23
      to "fast + slow durable" — orthogonal to the gate semantics),
    * mid-upload disconnect (the design rolls these back; orthogonal),
    * network partition between server and peer (a more elaborate spec
      could add it; for the single bug class targeted here it isn't
      load-bearing),
    * locality_map tracking (orthogonal — that's a separate read-path
      bug also captured in the invariant doc).

  CITATIONS:
    [ec1] nativelink-store/src/existence_cache_store.rs:334-337 (streaming)
    [ec2] nativelink-store/src/existence_cache_store.rs:402-405 (oneshot)
    [bs1] nativelink-service/src/bytestream_server.rs:1605 (streaming tee)
    [bs2] nativelink-service/src/bytestream_server.rs:1878-1880 (oneshot)
    [doc] /home/user/.claude/projects/-src-nativelink/memory/project_cas_write_invariant.md
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    GateOnMirror   \* TRUE => cache positive only after fast AND mirror both Ok.

ASSUME GateOnMirror \in BOOLEAN

\* Replica states.
\*   "Absent"  - this replica does not have the bytes.
\*   "Present" - this replica has the bytes.
ReplicaStates == {"Absent", "Present"}

\* Cache states.
\*   "NotPresent" - cache has no positive entry.
\*   "Positive"   - cache claims the digest is present (read fast path).
CacheStates == {"NotPresent", "Positive"}

\* Ack states.
\*   "Pending" - upload in progress; Bazel hasn't been Ack'd.
\*   "AckedOk" - server told Bazel the upload succeeded.
AckStates == {"Pending", "AckedOk"}

VARIABLES
    serverFast,         \* one of ReplicaStates
    peerMirror,         \* one of ReplicaStates
    cacheState,         \* one of CacheStates
    ack,                \* one of AckStates
    ackedWithCount,     \* Nat: replica count at the moment ack was set (0 if Pending)
    cachedWithCount     \* Nat: replica count at the moment cache went Positive

vars == <<serverFast, peerMirror, cacheState, ack, ackedWithCount, cachedWithCount>>

----------------------------------------------------------------------------
Init ==
    /\ serverFast      = "Absent"
    /\ peerMirror      = "Absent"
    /\ cacheState      = "NotPresent"
    /\ ack             = "Pending"
    /\ ackedWithCount  = 0
    /\ cachedWithCount = 0

----------------------------------------------------------------------------
(* FastWriteOk: server's fast tier (MemoryStore) accepts the write.     *)
(* Models the inner_store.update success arm in the upload path.        *)
----------------------------------------------------------------------------
FastWriteOk ==
    /\ serverFast = "Absent"
    /\ serverFast' = "Present"
    /\ UNCHANGED <<peerMirror, cacheState, ack, ackedWithCount, cachedWithCount>>

----------------------------------------------------------------------------
(* MirrorWriteOk: peer worker's MemoryStore accepts the mirror.         *)
(* Models the tee/mirror RPC's success arm.                              *)
(* In the BUGGED build this is background_spawn! and may not finish     *)
(* before Ack; we DON'T model that as a separate "race" — we model     *)
(* the lack of a happens-before constraint between this and AckBazel.  *)
----------------------------------------------------------------------------
MirrorWriteOk ==
    /\ peerMirror = "Absent"
    /\ peerMirror' = "Present"
    /\ UNCHANGED <<serverFast, cacheState, ack, ackedWithCount, cachedWithCount>>

----------------------------------------------------------------------------
(* CachePositive: the cache adds a positive entry for the digest.        *)
(* In the BUGGED build (GateOnMirror = FALSE) this only requires         *)
(* serverFast = "Present" — which is the historic bug.                  *)
(* In the FIXED build it requires both serverFast AND peerMirror.       *)
----------------------------------------------------------------------------
CachePositive ==
    /\ cacheState = "NotPresent"
    /\ serverFast = "Present"
    /\ (~GateOnMirror \/ peerMirror = "Present")
    /\ cacheState' = "Positive"
    /\ cachedWithCount' =
         (IF serverFast = "Present" THEN 1 ELSE 0) +
         (IF peerMirror = "Present" THEN 1 ELSE 0)
    /\ UNCHANGED <<serverFast, peerMirror, ack, ackedWithCount>>

----------------------------------------------------------------------------
(* AckBazel: server returns Ok to Bazel for the upload.                  *)
(* Same gating as CachePositive — ack semantics and cache-positive      *)
(* semantics are coupled in the existing code.                           *)
----------------------------------------------------------------------------
AckBazel ==
    /\ ack = "Pending"
    /\ serverFast = "Present"
    /\ (~GateOnMirror \/ peerMirror = "Present")
    /\ ack' = "AckedOk"
    /\ ackedWithCount' =
         (IF serverFast = "Present" THEN 1 ELSE 0) +
         (IF peerMirror = "Present" THEN 1 ELSE 0)
    /\ UNCHANGED <<serverFast, peerMirror, cacheState, cachedWithCount>>

----------------------------------------------------------------------------
(* FastEvict: the server's fast tier evicts the blob (MemoryStore LRU). *)
(* This can happen at any time after the write completes — and is the   *)
(* cascade that turns "ack returned" into "blob missing".               *)
(* DISABLED if cacheState = "NotPresent" or ack = "Pending"; modeling   *)
(* eviction during an in-flight upload is a different bug class.        *)
----------------------------------------------------------------------------
FastEvict ==
    /\ serverFast = "Present"
    /\ ack = "AckedOk"
    /\ serverFast' = "Absent"
    /\ UNCHANGED <<peerMirror, cacheState, ack, ackedWithCount, cachedWithCount>>

----------------------------------------------------------------------------
Next ==
    \/ FastWriteOk
    \/ MirrorWriteOk
    \/ CachePositive
    \/ AckBazel
    \/ FastEvict

Spec ==
    /\ Init
    /\ [][Next]_vars
    \* Fairness omitted — invariant is purely safety.

----------------------------------------------------------------------------
(* INVARIANTS                                                            *)
----------------------------------------------------------------------------

TypeOK ==
    /\ serverFast \in ReplicaStates
    /\ peerMirror \in ReplicaStates
    /\ cacheState \in CacheStates
    /\ ack        \in AckStates
    /\ ackedWithCount \in 0..2
    /\ cachedWithCount \in 0..2

(* In-memory replica count at the current state *)
ReplicaCount ==
    (IF serverFast = "Present" THEN 1 ELSE 0) +
    (IF peerMirror = "Present" THEN 1 ELSE 0)

(* SAFETY: AckImpliesTwoReplicasAtAck                                     *)
(* AT THE MOMENT we Ack'd Bazel, there must have been at least 2        *)
(* in-memory replicas. Captured in `ackedWithCount`, set by AckBazel.   *)
(*                                                                      *)
(* Under GateOnMirror = FALSE: TLC finds a trace                         *)
(*   FastWriteOk -> AckBazel  (peerMirror still "Absent" at ack moment, *)
(*   ackedWithCount = 1) -> violated.                                    *)
(* Under GateOnMirror = TRUE: AckBazel's precondition forces both       *)
(* replicas to be Present at ack time -> ackedWithCount = 2 -> holds.  *)
(* Note: this invariant is about the MOMENT of ack, not all states     *)
(* after it — a subsequent FastEvict legitimately brings ReplicaCount  *)
(* to 1 and that is fine for read recovery (peer mirror still serves). *)
AckImpliesTwoReplicasAtAck ==
    ack = "AckedOk" => ackedWithCount >= 2

(* SAFETY: CachePositiveImpliesTwoReplicasAtInsert                        *)
(* Same as above for cache-positive: at the moment we set the cache to  *)
(* Positive, both replicas must exist. Without this gate the cache      *)
(* lies — subsequent FastEvict produces stale-positive misses.          *)
CachePositiveImpliesTwoReplicasAtInsert ==
    cacheState = "Positive" => cachedWithCount >= 2

(* SAFETY: NoLossyAckCascade                                              *)
(* After ack, at least one replica must always remain — otherwise the   *)
(* "blob missing" symptom is unavoidable. In this spec the only         *)
(* eviction step is FastEvict on the server, which leaves peerMirror   *)
(* in place. Holds whenever the gate ensured peerMirror = "Present"    *)
(* at ack time. Under GateOnMirror = FALSE the trace                    *)
(*   FastWriteOk -> AckBazel -> FastEvict                              *)
(* leaves ReplicaCount = 0 -> NoLossyAckCascade violated.              *)
NoLossyAckCascade ==
    ack = "AckedOk" => ReplicaCount >= 1

============================================================================
