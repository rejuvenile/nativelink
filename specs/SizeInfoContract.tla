--------------------------- MODULE SizeInfoContract ---------------------------
(***************************************************************************
  UploadSizeInfo producer/consumer size contract.

  Models the cross-component contract enforced (post-Bucket-B) by enforcing
  leaf stores like `MemoryStore::update`
  (nativelink-store/src/memory_store.rs:218-257) and the producer-side
  obligation imposed on every site that constructs `UploadSizeInfo` and
  hands it down through a borrowed-writer pipeline.

  PRODUCTION SHAPE (post-Bucket-B):
    * `UploadSizeInfo` is a sum type
      (nativelink-util/src/store_trait.rs:91-102):
        - `ExactSize(u64)` — declared "exactly this many bytes will be sent".
        - `MaxSize(u64)`   — declared "at most this many bytes will be sent".
      (Production has no `Unknown` variant; this spec models its absence by
      restricting the producer-side action set to the two constructors.)
    * Producers (`FastSlowStore::run_producer`,
      `BatchUpdateBlobs::update_oneshot`, `ByteStream::write`'s upload path,
      `slow_update_store_with_file`) build the `UploadSizeInfo` from
      domain-known sizes (e.g. the digest's `size_bytes` field, the
      slow-store's `has()` reply) and pass it into a leaf store's `update`.
    * The leaf store enforces the contract at insert time:
        - `ExactSize(N)` requires the byte stream to deliver EXACTLY `N`
          bytes; any `M != N` is an `InvalidArgument` error and NO entry
          is inserted.
        - `MaxSize(N)` requires the byte stream to deliver `M <= N`; any
          `M > N` is an `InvalidArgument` error and NO entry is inserted.
      (`MemoryStore::update` lines 228-257; `RedisStore::update` mirrors
      this post-task-156.)

  HISTORIC FAULT (commit 9bfe9924, surfaced 2026-04-26):
    `FilesystemStore::has_with_results` returned `LenEntry::len()` which is
    `size_on_disk()` = `data_size.div_ceil(block_size) * block_size`
    (page-rounded for EvictingMap LRU accounting). The trait contract
    requires the actual blob byte length.

    Caller `FastSlowStore::run_producer` used the returned size to
    construct `UploadSizeInfo::ExactSize(rounded)` and then streamed only
    `data_size` bytes. The MemoryStore Bucket-B enforcement caught this:
        "MemoryStore::update: ExactSize declared 139264 bytes but
         received 137515 — refusing to insert a partial entry"
    (Production blob: `data_size=2_653_392`, `block_size=4096` →
    `size_on_disk=2_654_208` → declared 2_654_208, received 2_653_392 —
    same shape, scaled to a different page-aligned blob. The 139264 /
    137515 numbers come from a smaller blob in the same incident window.)

    The bug had been latent since `LenEntry::len()` was wired to
    `size_on_disk()` (2024). Today's MemoryStore enforcement exposed it as
    a user-visible failure on every Bazel read of a non-page-aligned blob.
    Four latent consumer sites
    (`fast_slow_store.rs:948 + 1891/1900`, `bytestream_server.rs:2001`,
    `store_trait.rs:1106-1113`) all build/observe sizes that go into the
    same contract and were closed by the hot-fix.

  WHAT THIS SPEC MODELS:
    * The producer's declaration step picks one of two `UploadSizeInfo`
      kinds — `ExactSize(N)` or `MaxSize(N)` — for an abstract size value
      drawn from a small bounded set `Sizes`.
    * The stream delivers an abstract `M` byte count, also drawn from
      `Sizes`. Producer-stream coupling is non-deterministic: TLC will
      explore every (declared, delivered) pair, modeling both the
      well-coupled producer (declared = delivered) and the
      page-rounded-mismatch producer (declared > delivered) and the
      truncated-stream producer (declared > delivered, e.g. Redis timeout
      dropping the channel mid-write).
    * The consumer is parameterised by the `FixSize` CONSTANT:
        - `FixSize = TRUE`  models the post-Bucket-B leaf store: rejects
          any (declared, delivered) pair that violates the contract by
          returning an error and NOT inserting.
        - `FixSize = FALSE` models the pre-Bucket-B leaf store: inserts
          unconditionally (the historic silent-poisoning behavior).
    * A `committed` boolean records whether the consumer accepted and
      inserted the entry. A `committedSize` records what the consumer
      believes the entry's size is (in production: the declared
      `ExactSize` or stream-actual for `MaxSize`).
    * A `poisoned` boolean records whether a committed entry's
      `committedSize` differs from the actual delivered byte count `M`.
      A poisoned cache is the failure mode the bug shipped: every
      future `get_part` re-reads `committedSize` bytes but the entry
      only has `M`.

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * SizeInfoContractFixed.cfg (FixSize = TRUE):
      No invariant violation. The contract is enforced; whenever a
      mismatch occurs, the consumer rejects the entry and `poisoned`
      stays FALSE. Every committed entry has `committedSize = M`.
    * SizeInfoContractBugged.cfg (FixSize = FALSE):
      INVARIANT VIOLATED on `NoPoisonedCommit`. Trace shows: producer
      declares `ExactSize(N)`, stream delivers `M != N`, consumer
      commits anyway with `committedSize = N`, `poisoned' = TRUE`. The
      trace exactly matches the production failure: declared > delivered,
      consumer accepted, future reads return short.

  SCOPE — what this spec models:
    * one digest at a time,
    * a small bounded set of sizes (drawn to expose the page-rounding
      mismatch shape: a "page-aligned" size and its "actual data" size),
    * the producer's UploadSizeInfo construction + the stream delivery
      as two separate atomic steps (the in-between is the bug window),
    * the consumer's contract check as a single atomic transition.

  SCOPE — what this spec does NOT model:
    * the actual chunked byte stream — `M` is an abstract count, not a
      sequence of chunks. The bug fires on the total count alone; chunk
      boundaries are orthogonal.
    * the wrapper composition (VerifyStore, ExistenceCacheStore,
      SizePartitioningStore, etc.). The contract is a leaf-store
      invariant; wrappers either delegate the size info verbatim
      (most do) or perform their own check (VerifyStore does), and
      either way the leaf-store invariant is the load-bearing one.
    * concurrent updates of the same key (orthogonal — the bug fires
      on a single update),
    * cancellation / writer-termination (covered by WriterTermination.tla),
    * the difference between `update`, `update_oneshot`, and
      `update_with_whole_file` (all three feed the same leaf-store
      invariant via the same UploadSizeInfo).

  CITATIONS:
    [type] nativelink-util/src/store_trait.rs:91-102 (UploadSizeInfo enum)
    [enf]  nativelink-store/src/memory_store.rs:218-257 (MemoryStore enforcement)
    [enf2] nativelink-store/src/redis_store.rs (RedisStore mirror, task-156)
    [bug]  nativelink-store/src/filesystem_store.rs has_with_results pre-9bfe9924
    [fix]  commit 9bfe9924 (filesystem_store: return data_size, not size_on_disk)
    [c1]   nativelink-store/src/fast_slow_store.rs:948 (size from has())
    [c2]   nativelink-store/src/fast_slow_store.rs:1891,1900 (has_with_results paths)
    [c3]   nativelink-service/src/bytestream_server.rs:1999-2003 (declared from digest)
    [c4]   nativelink-util/src/store_trait.rs:1106-1113 (check_health: has() size compared to digest len)
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Sizes,        \* Set of byte-count values, e.g. {137515, 139264}.
    FixSize       \* TRUE => consumer enforces UploadSizeInfo; FALSE => bug.

ASSUME FixSize \in BOOLEAN
ASSUME Sizes \subseteq Nat /\ Cardinality(Sizes) >= 1

\* SizeInfo kinds. We model the two production constructors. A
\* "declared = none" sentinel marks the pre-decision state.
SizeKinds == {"None", "ExactSize", "MaxSize"}

\* Stream / consumer states. We model only the relevant edges.
\*   "Pending"      - haven't delivered the byte count yet.
\*   "Delivered"    - byte count M reached the consumer; awaiting check.
\*   "Committed"    - consumer accepted and inserted the entry.
\*   "Rejected"     - consumer refused to insert (post-fix invariant fire).
ConsumerStates == {"Pending", "Delivered", "Committed", "Rejected"}

VARIABLES
    declaredKind,    \* one of SizeKinds
    declaredN,       \* the value N inside ExactSize(N) / MaxSize(N); 0 if None
    deliveredM,      \* actual byte count delivered by the stream
    delivered,       \* boolean: has the stream landed?
    consumerState,   \* one of ConsumerStates
    committedSize,   \* what the consumer believes the committed entry's size is
    poisoned         \* boolean: committed entry's recorded size != actual bytes

vars == <<declaredKind, declaredN, deliveredM, delivered, consumerState,
          committedSize, poisoned>>

----------------------------------------------------------------------------
Init ==
    /\ declaredKind = "None"
    /\ declaredN = 0
    /\ deliveredM = 0
    /\ delivered = FALSE
    /\ consumerState = "Pending"
    /\ committedSize = 0
    /\ poisoned = FALSE

----------------------------------------------------------------------------
(* DeclareExactSize(n): producer constructs UploadSizeInfo::ExactSize(n).  *)
(* Models e.g. FastSlowStore::run_producer's path that calls slow_store    *)
(* .has() and uses the returned size to construct ExactSize.               *)
(*                                                                         *)
(* Critically, n is NOT constrained to equal the eventually-delivered M:   *)
(* TLC will explore every (n, M) pair, including the page-rounding         *)
(* mismatch (n > M) that surfaced in production.                           *)
----------------------------------------------------------------------------
DeclareExactSize(n) ==
    /\ declaredKind = "None"
    /\ n \in Sizes
    /\ declaredKind' = "ExactSize"
    /\ declaredN' = n
    /\ UNCHANGED <<deliveredM, delivered, consumerState, committedSize, poisoned>>

----------------------------------------------------------------------------
(* DeclareMaxSize(n): producer constructs UploadSizeInfo::MaxSize(n).      *)
(* Models e.g. FastSlowStore::run_producer's LazyExistenceOnSync arm that  *)
(* declares MaxSize(u64::MAX) when has() is skipped.                       *)
----------------------------------------------------------------------------
DeclareMaxSize(n) ==
    /\ declaredKind = "None"
    /\ n \in Sizes
    /\ declaredKind' = "MaxSize"
    /\ declaredN' = n
    /\ UNCHANGED <<deliveredM, delivered, consumerState, committedSize, poisoned>>

----------------------------------------------------------------------------
(* DeliverBytes(m): the stream delivers m bytes to the consumer (EOF).     *)
(* Models the buf_channel reaching EOF after m bytes have been pushed in.  *)
(* m is independent of the declaration — that's the bug window.            *)
----------------------------------------------------------------------------
DeliverBytes(m) ==
    /\ declaredKind \in {"ExactSize", "MaxSize"}
    /\ ~delivered
    /\ m \in Sizes
    /\ deliveredM' = m
    /\ delivered' = TRUE
    /\ consumerState' = "Delivered"
    /\ UNCHANGED <<declaredKind, declaredN, committedSize, poisoned>>

----------------------------------------------------------------------------
(* ConsumerCheck: the leaf store's update() decides commit vs reject.      *)
(*                                                                         *)
(* Under FixSize = TRUE this is the post-Bucket-B enforcement              *)
(* (memory_store.rs:228-257):                                              *)
(*   ExactSize(n) /\ M != n  -> Rejected (no insert)                       *)
(*   MaxSize(n)   /\ M >  n  -> Rejected (no insert)                       *)
(*   otherwise               -> Committed (committedSize = M; no poison)   *)
(*                                                                         *)
(* Under FixSize = FALSE this is the pre-Bucket-B silent-accept behavior:  *)
(*   ALWAYS Committed.                                                     *)
(*                                                                         *)
(* The committed entry's recorded size (committedSize) is the value that   *)
(* future reads return. Production caches the declared ExactSize as the    *)
(* entry's logical size (e.g. EvictingMap accounts for it; reads use it    *)
(* for length checks). If declared != delivered AND we commit, the cache  *)
(* is poisoned: future reads return short / over.                          *)
(*                                                                         *)
(* Specifically we model committedSize = declaredN for the ExactSize arm   *)
(* (production behaviour: the declared size is what's stored in the        *)
(* metadata) and committedSize = deliveredM for the MaxSize arm (where    *)
(* the actual stream length is known and recorded).                        *)
----------------------------------------------------------------------------
\* Predicate: does the (declared, delivered) pair satisfy the contract?
ContractHolds ==
    \/ declaredKind = "ExactSize" /\ deliveredM = declaredN
    \/ declaredKind = "MaxSize"   /\ deliveredM <= declaredN

\* What size would the consumer record on commit, given current state?
RecordedSizeOnCommit ==
    IF declaredKind = "ExactSize" THEN declaredN ELSE deliveredM

ConsumerCommit ==
    /\ consumerState = "Delivered"
    /\ \/ FixSize = FALSE                  \* bug: always commit
       \/ ContractHolds                     \* fix: commit only when contract holds
    /\ consumerState' = "Committed"
    /\ committedSize' = RecordedSizeOnCommit
    /\ poisoned' = (RecordedSizeOnCommit # deliveredM)
    /\ UNCHANGED <<declaredKind, declaredN, deliveredM, delivered>>

ConsumerReject ==
    /\ consumerState = "Delivered"
    /\ FixSize = TRUE
    /\ ~ContractHolds
    /\ consumerState' = "Rejected"
    /\ UNCHANGED <<declaredKind, declaredN, deliveredM, delivered,
                   committedSize, poisoned>>

----------------------------------------------------------------------------
Next ==
    \/ \E n \in Sizes : DeclareExactSize(n)
    \/ \E n \in Sizes : DeclareMaxSize(n)
    \/ \E m \in Sizes : DeliverBytes(m)
    \/ ConsumerCommit
    \/ ConsumerReject

----------------------------------------------------------------------------
Spec ==
    /\ Init
    /\ [][Next]_vars
    \* No fairness needed — invariants are pure safety. We do NOT claim
    \* "every declaration eventually delivers" because in production a
    \* timeout-cancelled write can leave the consumer Pending forever
    \* (deadlock detection is the WriterTermination.tla domain).

----------------------------------------------------------------------------
(* INVARIANTS *)
----------------------------------------------------------------------------

TypeOK ==
    /\ declaredKind \in SizeKinds
    /\ declaredN \in (Sizes \cup {0})
    /\ deliveredM \in (Sizes \cup {0})
    /\ delivered \in BOOLEAN
    /\ consumerState \in ConsumerStates
    /\ committedSize \in (Sizes \cup {0})
    /\ poisoned \in BOOLEAN

(* SAFETY: NoPoisonedCommit                                                 *)
(*                                                                          *)
(* The CORE invariant: a committed cache entry's recorded size MUST equal  *)
(* the actual byte count delivered. A `poisoned = TRUE` state means a      *)
(* future reader will get the wrong number of bytes — exactly the          *)
(* production failure shape from the page-rounding regression.             *)
(*                                                                          *)
(* Under FixSize = TRUE: the consumer rejects every contract-violating     *)
(* (declared, delivered) pair before committing, so this invariant holds.  *)
(* Under FixSize = FALSE: the consumer silently commits with declaredN as  *)
(* recorded size; whenever declaredN != deliveredM the invariant fires.    *)
NoPoisonedCommit ==
    ~poisoned

(* SAFETY: ContractAtCommit                                                 *)
(*                                                                          *)
(* Direct expression of the producer/consumer contract: at the moment the  *)
(* consumer commits, the (declared, delivered) pair must satisfy the       *)
(* declared kind's invariant (ExactSize: equality; MaxSize: bounded). The  *)
(* bug breaks this iff at commit time.                                     *)
(*                                                                          *)
(* This is the SHIPPABLE form of the invariant — the leaf-store enforcement *)
(* is exactly this check. Under the bugged config the consumer commits on  *)
(* every pair regardless, so this fires.                                   *)
ContractAtCommit ==
    consumerState # "Committed" \/ ContractHolds

(* SAFETY: NoCommitWithoutDeliver                                           *)
(*                                                                          *)
(* Sanity / type-style: the consumer cannot commit before the stream has   *)
(* delivered. Under both fixed and bugged this should hold trivially —     *)
(* it's the kind of accidentally-broken invariant that catches a future    *)
(* refactor that reorders the commit relative to the EOF wait.             *)
NoCommitWithoutDeliver ==
    consumerState # "Committed" \/ delivered

============================================================================
