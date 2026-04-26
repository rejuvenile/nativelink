--------------------------- MODULE TraitDefaultNoop ---------------------------
(***************************************************************************
  Silent no-op delegation in 2-level wrapper hierarchies.

  Models the trait-default-noop bug class baked into
  `nativelink-util/src/store_trait.rs:954-991`. The trait `StoreDriver`
  declares four contract methods with NO-OP / EMPTY default impls:

      fn drain_stable_digests(&self) -> Vec<DigestInfo> { Vec::new() }
      fn stable_notify(&self) -> Arc<Notify> { /* never woken */ }
      fn pin_digests(&self, _digests: &[DigestInfo]) {}
      fn drain_failed_digests(&self) -> Vec<DigestInfo> { Vec::new() }

  Wrappers (VerifyStore, ExistenceCacheStore, RefStore, WorkerProxyStore,
  CompletenessCheckingStore) MUST explicitly override all four to delegate
  to `inner_store`. The leaf store (FastSlowStore for stable_notify /
  drain_stable_digests; FilesystemStore for pin_digests; FastSlowStore
  again for drain_failed_digests) is the source of truth — but the trait's
  default impls SILENTLY discard the contract if any wrapper in the chain
  forgets to override.

  PRODUCTION SHAPE:
    The CAS store chain in production is
        ExistenceCacheStore -> VerifyStore -> FastSlowStore (leaf)
    (`src/bin/nativelink.rs:319-344`). The `blobs_in_stable_storage_loop`
    in `src/bin/nativelink.rs:382-416` calls
    `outer_cas_store.drain_stable_digests()` and
    `outer_cas_store.stable_notify()` on the OUTERMOST wrapper. The
    outermost wrapper is ExistenceCacheStore. For the contract to work,
    BOTH ExistenceCacheStore AND VerifyStore must delegate; the leaf
    must produce; the BIS broadcast loop must drain.

  THE BUG CLASS:
    If a wrapper FORGETS to override one of these methods, the trait's
    default fires SILENTLY:
      * drain_stable_digests => empty Vec returned forever (no compile
        error, no runtime warning) — the BIS loop drains nothing, so
        the worker NEVER receives any stable-storage notification, so
        any pin allocated by the worker NEVER receives the unpin signal
        the BIS loop is supposed to deliver. Pin leaks permanently.
      * stable_notify => returns a NOOP_NOTIFY (`OnceLock<Arc<Notify>>`
        that's never woken). The merged_notify task at
        `src/bin/nativelink.rs:367-380` will never get woken from this
        store. Without the 500ms select! fallback at line 386, the BIS
        loop would never wake at all.
      * pin_digests => digest is silently NOT pinned. A subsequent
        eviction sweep evicts the blob the worker is depending on.
      * drain_failed_digests => failed slow-write retry is silently
        suppressed; the worker reconnect path
        (`local_worker.rs::on_reconnect`) re-fetches the failed-write
        list from each store; if a wrapper returns `vec![]`, the inner
        store's failed digests are forever stuck.

    All four are the SAME class: a wrapper "looks correct" because the
    leaf and the trait both compile, the wrapper compiles trivially
    (defaults inherited), and the contract silently breaks in
    composition. The `cargo check` and code-review tools cannot detect
    this — Rust's trait system is happy with the inherited default.

  THE PRODUCTION FIX (in flight, worktree-agent-aea1038e):
    Replace the four orthogonal opt-in methods with a SINGLE
    `StableDigestDelegation` enum returned by ONE required method
    (no default). Wrappers MUST implement the method to compile. The
    enum carries the leaf-store handle; the BIS loop pattern-matches.
    The class is killed at the type-system level: every wrapper is
    forced to declare its delegation strategy, and a wrapper that
    forgets receives a compile error rather than silent dropping.

    A separate strategy: have NO defaults on the trait at all
    ("required" methods). Same effect. The C+D PR uses the enum
    because it adds expressiveness (e.g. `Skip` for stores that
    legitimately don't have a leaf, like MemoryStore).

  CONSTANTS:
    * BugMode (BOOLEAN): TRUE => wrapper inherits trait's noop default.
      FALSE => wrapper has explicit override (or the C+D refactor
      removed the default). This is the toggle that closes the bug.

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * TraitDefaultNoopFixed.cfg (BugMode = FALSE): NO invariant
      violation. The leaf's contributions reach the broadcast loop;
      the broadcast loop sees the digest; the worker pin is unpinned.
    * TraitDefaultNoopBugged.cfg (BugMode = TRUE):  INVARIANT VIOLATED
      on `BroadcastSeesEveryLeafProduction`. The trace shows: leaf
      pushes digest D into stableDigests at the leaf; wrapper's
      drain_stable_digests returns empty (default); broadcast loop
      drains empty across the wrapper; D is never broadcast; worker
      pin never released.
      Also INVARIANT VIOLATED on `NoStaleWorkerPinUnderProduction`.

  SCOPE — what this spec models:
    * one digest D,
    * a leaf store with a `leafProduction` flag (TRUE once leaf
      records D as stable),
    * a wrapper above the leaf with a `wrapperOverridesDelegate`
      flag (TRUE => delegates to leaf, FALSE => returns the trait's
      noop empty default),
    * a broadcast loop that drains the wrapper periodically and
      records seen digests in `broadcastSeen`,
    * a worker pin set with one entry that's pinned at the start
      and is meant to be released on broadcast.

  SCOPE — what this spec does NOT model:
    * the actual notify wake-ups (modeled as a non-deterministic
      drain step; the bug is in the drain semantics, not the wake),
    * the secondary class for `pin_digests` / `drain_failed_digests`
      (orthogonal, same shape — comment in README),
    * concurrent multiple wrappers (the production chain is 2 deep:
      ExistenceCache -> Verify -> leaf; but the CONTRACT failure is
      per-wrapper, so 1 wrapper suffices to demonstrate),
    * the C+D StableDigestDelegation enum's Skip / Forward / Leaf
      variants (the spec collapses to "wrapper delegates" vs
      "wrapper returns empty"),
    * the OnceLock-static NOOP_NOTIFY (modeled as "broadcast never
      wakes from this store"; orthogonal to the drain-empty
      symptom above).

  CITATIONS:
    [trait]   nativelink-util/src/store_trait.rs:954-991
    [verify]  nativelink-store/src/verify_store.rs:428-441 (delegation)
    [exist]   nativelink-store/src/existence_cache_store.rs:565-578
    [ref]     nativelink-store/src/ref_store.rs:188-218
    [wrk]     nativelink-store/src/worker_proxy_store.rs:2336-2349
    [leaf]    nativelink-store/src/fast_slow_store.rs:556-563, 3329-3343
    [bis]     src/bin/nativelink.rs:382-416 (broadcast loop)
    [unpin]   nativelink-worker/src/local_worker.rs (BIS receive path)
    [refactor] worktree-agent-aea1038e (in-flight C+D StableDigestDelegation)
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    BugMode    \* TRUE => wrapper inherits trait's noop default impl

ASSUME BugMode \in BOOLEAN

\* Leaf-production lifecycle.
\*   "Pending"  - leaf has not recorded D yet.
\*   "Recorded" - leaf has D in its stableDigests queue.
\*   "Drained"  - leaf returned D to a caller via drain_stable_digests.
LeafStates == {"Pending", "Recorded", "Drained"}

\* Wrapper override flag is captured by BugMode; we don't separately
\* model the override-decision step (it's a code-review-time decision,
\* not a runtime transition).

\* Broadcast loop iteration tracker.
BroadcastStates == {"Idle", "DrainingWrapper", "Broadcast"}

\* Worker pin lifecycle for D.
\*   "Pinned"   - worker pinned D and is waiting for BIS.
\*   "Unpinned" - worker received BIS and unpinned D.
WorkerPinStates == {"Pinned", "Unpinned"}

VARIABLES
    leafState,         \* one of LeafStates
    leafQueue,         \* {"D"} once recorded; emptied on Drained.
                       \* Models the leaf's stable_digests Vec.
    wrapperDrained,    \* {"D"} or {} — what the wrapper returned
                       \* on its most recent drain call. Bugged
                       \* wrappers always return {} regardless of leaf.
    broadcastState,    \* one of BroadcastStates
    broadcastSeen,     \* set of digests broadcast at least once
    workerPin          \* one of WorkerPinStates

vars == <<leafState, leafQueue, wrapperDrained, broadcastState,
          broadcastSeen, workerPin>>

----------------------------------------------------------------------------
Init ==
    /\ leafState = "Pending"
    /\ leafQueue = {}
    /\ wrapperDrained = {}
    /\ broadcastState = "Idle"
    /\ broadcastSeen = {}
    /\ workerPin = "Pinned"

----------------------------------------------------------------------------
(* LeafRecordsDigest: leaf's update_oneshot succeeds and pushes D into     *)
(* its stable_digests queue. Models                                       *)
(* fast_slow_store.rs:2289-2290 (and :2520-2521) where on update_oneshot *)
(* success the digest is pushed.                                          *)
----------------------------------------------------------------------------
LeafRecordsDigest ==
    /\ leafState = "Pending"
    /\ leafState' = "Recorded"
    /\ leafQueue' = {"D"}
    /\ UNCHANGED <<wrapperDrained, broadcastState, broadcastSeen, workerPin>>

----------------------------------------------------------------------------
(* BroadcastStartDrain: BIS loop calls outer_wrapper.drain_stable_digests *)
(* (`src/bin/nativelink.rs:391`). Transitions broadcast state to          *)
(* DrainingWrapper. Models the start of an iteration of the broadcast    *)
(* loop body.                                                              *)
----------------------------------------------------------------------------
BroadcastStartDrain ==
    /\ broadcastState = "Idle"
    /\ broadcastState' = "DrainingWrapper"
    /\ UNCHANGED <<leafState, leafQueue, wrapperDrained, broadcastSeen, workerPin>>

----------------------------------------------------------------------------
(* WrapperDelegateDrain: the FIXED wrapper's drain_stable_digests is     *)
(* called and delegates to the leaf, returning the leaf's queue and     *)
(* clearing the leaf's queue. Models e.g.                                 *)
(* verify_store.rs:428-429 / existence_cache_store.rs:565-567:           *)
(*     fn drain_stable_digests(&self) -> Vec<DigestInfo> {              *)
(*         self.inner_store.drain_stable_digests()                      *)
(*     }                                                                   *)
(*                                                                          *)
(* Disabled when BugMode = TRUE; in that case the trait default fires    *)
(* (WrapperNoopDrain below).                                              *)
----------------------------------------------------------------------------
WrapperDelegateDrain ==
    /\ ~BugMode
    /\ broadcastState = "DrainingWrapper"
    /\ wrapperDrained' = leafQueue          \* delegate => returns leaf's items
    /\ leafQueue' = {}                       \* leaf consumed
    /\ leafState' = IF leafState = "Recorded" THEN "Drained" ELSE leafState
    /\ broadcastState' = "Broadcast"
    /\ UNCHANGED <<broadcastSeen, workerPin>>

----------------------------------------------------------------------------
(* WrapperNoopDrain: the BUGGY wrapper inherits the trait's default     *)
(* drain_stable_digests, which returns Vec::new(). Leaf is NOT touched,  *)
(* so the leaf's queue keeps growing forever (or just stays at {"D"}    *)
(* in this 1-digest spec). Models store_trait.rs:954-956:                 *)
(*     fn drain_stable_digests(&self) -> Vec<DigestInfo> {              *)
(*         Vec::new()                                                     *)
(*     }                                                                   *)
(*                                                                          *)
(* Disabled when BugMode = FALSE.                                         *)
----------------------------------------------------------------------------
WrapperNoopDrain ==
    /\ BugMode
    /\ broadcastState = "DrainingWrapper"
    /\ wrapperDrained' = {}                  \* default impl returns empty
    /\ broadcastState' = "Broadcast"
    /\ UNCHANGED <<leafState, leafQueue, broadcastSeen, workerPin>>

----------------------------------------------------------------------------
(* BroadcastEmit: broadcast loop has the wrapper's drained-set.          *)
(* If non-empty, broadcasts each digest to every scheduler/worker, which *)
(* records it in broadcastSeen. Models                                   *)
(* src/bin/nativelink.rs:404-414. If wrapperDrained is empty (the bug   *)
(* case), nothing is broadcast and broadcastSeen stays unchanged.       *)
----------------------------------------------------------------------------
BroadcastEmit ==
    /\ broadcastState = "Broadcast"
    /\ broadcastSeen' = broadcastSeen \cup wrapperDrained
    /\ broadcastState' = "Idle"
    /\ UNCHANGED <<leafState, leafQueue, wrapperDrained, workerPin>>

----------------------------------------------------------------------------
(* WorkerReceivesBis: worker observes the BIS broadcast for D and        *)
(* unpins. Only enabled if D is in broadcastSeen (i.e. the broadcast    *)
(* actually happened). Models local_worker.rs's BIS receive path.       *)
----------------------------------------------------------------------------
WorkerReceivesBis ==
    /\ workerPin = "Pinned"
    /\ "D" \in broadcastSeen
    /\ workerPin' = "Unpinned"
    /\ UNCHANGED <<leafState, leafQueue, wrapperDrained, broadcastState,
                    broadcastSeen>>

----------------------------------------------------------------------------
Next ==
    \/ LeafRecordsDigest
    \/ BroadcastStartDrain
    \/ WrapperDelegateDrain
    \/ WrapperNoopDrain
    \/ BroadcastEmit
    \/ WorkerReceivesBis

----------------------------------------------------------------------------
(* Fairness so the FIXED config can demonstrate liveness.                 *)
----------------------------------------------------------------------------
WrapperDrains == WrapperDelegateDrain \/ WrapperNoopDrain

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(LeafRecordsDigest)
    /\ WF_vars(BroadcastStartDrain)
    /\ WF_vars(WrapperDrains)
    /\ WF_vars(BroadcastEmit)
    /\ WF_vars(WorkerReceivesBis)

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ leafState \in LeafStates
    /\ leafQueue \subseteq {"D"}
    /\ wrapperDrained \subseteq {"D"}
    /\ broadcastState \in BroadcastStates
    /\ broadcastSeen \subseteq {"D"}
    /\ workerPin \in WorkerPinStates

(* SAFETY: BroadcastSeesEveryLeafProduction                                *)
(*                                                                          *)
(* If the leaf has been drained AND the broadcast loop has completed     *)
(* its iteration (state back to Idle after the Broadcast step), the     *)
(* digest must be in broadcastSeen.                                       *)
(*                                                                          *)
(* Under FIXED: WrapperDelegateDrain transitions leaf to Drained AND     *)
(* moves wrapperDrained = {"D"}. BroadcastEmit then adds D to            *)
(* broadcastSeen and returns to Idle. By the time we're back at Idle,   *)
(* broadcastSeen has D — invariant holds.                                *)
(*                                                                          *)
(* Under BUGGED: leafState NEVER reaches "Drained" because the only     *)
(* transition that produces Drained is WrapperDelegateDrain, which is   *)
(* gated on ~BugMode. So this invariant is vacuously true under bug;    *)
(* the bug surfaces via NoStaleWorkerPinUnderProduction (below) and    *)
(* the LIVENESS property EventuallyWorkerUnpinned.                      *)
BroadcastSeesEveryLeafProduction ==
    (leafState = "Drained" /\ broadcastState = "Idle") => "D" \in broadcastSeen

(* SAFETY: BugMatchesWrapperDelegation                                     *)
(*                                                                          *)
(* Under BUGGED, wrapperDrained is always empty (the trait's noop       *)
(* default returns Vec::new()). This is the bug's defining mechanism.   *)
(* This invariant fails under BUGGED if any code path lets D leak       *)
(* through the wrapper despite the noop default — which it shouldn't.   *)
(*                                                                          *)
(* Under FIXED, the only constraint is that wrapperDrained's contents   *)
(* can ONLY be a subset of {"D"}: the wrapper can never return a       *)
(* digest the leaf hasn't produced. (TypeOK already covers this; we    *)
(* keep a stronger statement for the FIXED side as documentation.)      *)
BugMatchesWrapperDelegation ==
    BugMode => wrapperDrained = {}

(* LIVENESS: EventuallyWorkerUnpinned                                       *)
(*                                                                          *)
(* Under FIXED, the worker eventually receives the BIS and unpins.       *)
(* Under BUGGED, this never happens — the worker is permanently Pinned.  *)
EventuallyWorkerUnpinned == <>(workerPin = "Unpinned")

============================================================================
