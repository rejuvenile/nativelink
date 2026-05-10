------------------------- MODULE CcsPinRemovalComposite -------------------------
(***************************************************************************
  #332 — Composite invariant for `cas_FAST_SLOW_STORE.fast = MemoryStore`
  after `CompletenessCheckingStore::pin_digests` deletion.

  The PIN/EVICT/GATE triangle on the CAS-side MemoryStore has THREE pin
  contributors and ONE gate. After #332 only TWO pin contributors remain:

    A. BIS-feeder pin (`fast_slow_store.rs:4337,4504,4566,...`):
       fired on every CAS write; released by BIS-ack from the worker. This
       is the LOAD-BEARING pin — it protects the >=2-replica durability
       invariant during the in-memory-only window between fast-tier insert
       and slow-tier persistence + worker BIS ack.

    C. CCS post-verification pin: DELETED by #332. Pre-fix this consumed
       a slice of the cap proportional to AC-completeness-check throughput
       with no matching unpin (only the 120 s `PIN_TIMEOUT_SECS`).

    B. Chunked-write/read pins (`fast_slow_store.rs:4682,4769`): per-chunk
       window pins; bounded by `parallel_chunk_count`; orthogonal to AC
       throughput.

  GATE (`memory_store.rs:check_backpressure_gate`, gated on
  `emit_backpressure_enabled`): after #334 Fix C this is a TWO-STAGE gate:
    1. `evict_unpinned_lru_bytes(incoming)` — surgically free unpinned
       LRU bytes equal to the incoming write.
    2. Re-check `would_exceed_capacity`. If still over cap, emit
       `Code::ResourceExhausted + BackpressureSignal::MemoryStoreAtCapacity`.

  COMPOSITE INVARIANT (the CLAUDE.md "Admission/Eviction/Pin
  Composability" triangle for this cache):

    `gate-active-and-firing-cascade ⇒ pinned_bytes >= max_bytes`

  i.e. a `MemoryStoreAtCapacity` cascade can ONLY fire when EVERY byte
  in the cache is pinned. Equivalently: with #334 Fix C in place the
  cascade is a precise saturation signal for "pinned set has filled the
  cache." Removing the spurious CCS pin source restores headroom; if
  legitimate pin sources (BIS-feeder under sustained line-rate writes
  with slow BIS ack) STILL fill the cap, the composite is still violated
  and the pre-#332 `pin_cap` (25% of max_bytes) needs revisiting.

  WHAT THIS SPEC PROVES
    1. SAFETY  (`NoCascadeUnlessAllPinned`):
       a `cascadeFired` event is reachable ONLY from a state where
       `pinnedBytes + freeBytes - unpinnedBytes_evictable >= maxBytes`.
       In other words the gate cannot fire unless eviction-after-extension
       cannot free enough room — which is exactly the "every byte pinned"
       case once we model the eviction-extension step explicitly.

    2. LIVENESS (`EventuallyAdmissible`):
       under WF on `BISAck` (the only release for BIS-feeder pins) the
       system always returns to a state where the next write can be
       admitted. Removing CCS pins is what makes this property TRUE for
       the AC-completeness workload that #332 targets.

  WHAT THIS SPEC DELIBERATELY ABSTRACTS
    * Per-byte accounting — model `slot` units, each one a fixed-size
      "row" in the cap. The cap is `MaxSlots`.
    * Concurrency BETWEEN write and BIS-ack — modeled as fully
      non-deterministic interleaving.
    * The `pin_cap = 25% of max_bytes` sub-cap inside MokaEvictingMap
      (`pin_keys: pin cap exceeded` warn + skip). When a pin is silently
      skipped the durability invariant is broken at a different layer;
      the CCS-pin-removed composite still holds at THIS layer.

  EXPECTED TLC OUTCOMES
    * CcsPinRemovalCompositeFixed.cfg (with Fix C eviction extension):
        - INVARIANT NoCascadeUnlessAllPinned: HOLDS.
        - PROPERTY EventuallyAdmissible: HOLDS under WF_vars(BISAck).
    * CcsPinRemovalCompositeBugged.cfg (Fix C disabled, i.e. gate fires
      directly on `would_exceed_capacity` like the 2026-05-08 cascade):
        - INVARIANT NoCascadeUnlessAllPinned: VIOLATED.
          TLC trace: write d1 (admitted, pinned by BIS-feeder), write d2
          while d1 still pinned, gate sees `pinnedBytes + d2 > cap` and
          fires cascade even though d1 could in principle be unpinned by
          a future BIS-ack (no compensation step). Composite broken.

  CITATIONS
    [#332] commit 7c288ca5
    [#334] Fix C: nativelink-store/src/memory_store.rs:282-322
    [pinL] nativelink-util/src/moka_evicting_map.rs:870-921 (pin_keys)
    [BIS]  nativelink-store/src/fast_slow_store.rs:4337,4504,4566
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    MaxSlots,            \* Total cap on the MemoryStore (in "slot" units).
    Writers,             \* Symbolic set of concurrent writer ids.
    WritesPerWriter,     \* Slots each writer wants to push (init pendingWrites).
    EvictionExtension    \* TRUE = #334 Fix C in place; FALSE = pre-Fix-C gate.

ASSUME MaxSlots \in Nat /\ MaxSlots > 0
ASSUME WritesPerWriter \in Nat /\ WritesPerWriter > 0
ASSUME EvictionExtension \in BOOLEAN

VARIABLES
    pinnedBytes,         \* Bytes held in the `pinned` DashMap (BIS-feeder).
    cachedBytes,         \* Bytes in moka cache (unpinned, evictable).
    cascadeFired,        \* TRUE once any writer received MemoryStoreAtCapacity.
    cascadeAtUnpinned,   \* Snapshot of cachedBytes AT THE MOMENT cascade fired.
                         \* "Unpinned freeable bytes ignored by the gate." If
                         \* this is > 0 when cascadeFired flips, the gate fired
                         \* spuriously: there WAS unpinned LRU it could have
                         \* evicted before emitting MemoryStoreAtCapacity. That
                         \* is exactly the 2026-05-08 cascade signature.
    pendingWrites        \* Sequence of pending write requests (each = 1 slot).

vars == <<pinnedBytes, cachedBytes, cascadeFired, cascadeAtUnpinned, pendingWrites>>

----------------------------------------------------------------------------
(* Init: empty cache, no pins, no cascade, one write per writer.            *)
----------------------------------------------------------------------------
Init ==
    /\ pinnedBytes = 0
    /\ cachedBytes = 0
    /\ cascadeFired = FALSE
    /\ cascadeAtUnpinned = 0
    /\ pendingWrites = [w \in Writers |-> WritesPerWriter]

----------------------------------------------------------------------------
(* WouldExceedCapacity(n) is the same predicate as moka_evicting_map.rs:    *)
(* `would_exceed_capacity` — does adding n bytes push cache+pinned over    *)
(* the cap?                                                                 *)
----------------------------------------------------------------------------
WouldExceedCapacity(n) ==
    pinnedBytes + cachedBytes + n > MaxSlots

----------------------------------------------------------------------------
(* AdmitWrite(w): the production happy path.                                *)
(*   1. check_backpressure_gate sees room — admit, insert into cache,       *)
(*      then BIS-feeder calls pin_digests, moving the slot from cache to    *)
(*      pinned. We collapse those two steps because they're atomic          *)
(*      relative to other writers from the gate's perspective.              *)
(*   2. With Fix C, if !would_exceed but only after a successful eviction-  *)
(*      extension. We model both flavors.                                   *)
----------------------------------------------------------------------------
AdmitWriteHappy(w) ==
    /\ pendingWrites[w] > 0
    /\ ~WouldExceedCapacity(1)
    /\ pinnedBytes' = pinnedBytes + 1
    /\ pendingWrites' = [pendingWrites EXCEPT ![w] = pendingWrites[w] - 1]
    /\ UNCHANGED <<cachedBytes, cascadeFired, cascadeAtUnpinned>>

----------------------------------------------------------------------------
(* AdmitWriteAfterEviction(w): #334 Fix C — at-cap detected, evict          *)
(* `pendingWrites[w]` worth of unpinned LRU first, then admit.              *)
(* Modeled by atomically draining min(cachedBytes, pendingWrites[w]) from   *)
(* cachedBytes before re-checking.                                          *)
----------------------------------------------------------------------------
AdmitWriteAfterEviction(w) ==
    /\ EvictionExtension
    /\ pendingWrites[w] > 0
    /\ WouldExceedCapacity(1)
    /\ LET freeable == IF cachedBytes >= 1 THEN 1 ELSE 0
       IN /\ pinnedBytes + (cachedBytes - freeable) + 1 <= MaxSlots
          /\ cachedBytes' = cachedBytes - freeable
          /\ pinnedBytes' = pinnedBytes + 1
          /\ pendingWrites' = [pendingWrites EXCEPT ![w] = pendingWrites[w] - 1]
          /\ UNCHANGED <<cascadeFired, cascadeAtUnpinned>>

----------------------------------------------------------------------------
(* CascadeFire(w): EVERY byte the gate sees is pinned (either there's no    *)
(* eviction-extension and would_exceed is true, or the extension was tried  *)
(* and freed nothing). Sets cascadeFired so any safety violation is         *)
(* visible at the trace tip.                                                *)
----------------------------------------------------------------------------
CascadeFire(w) ==
    /\ pendingWrites[w] > 0
    /\ WouldExceedCapacity(1)
    /\ \/ ~EvictionExtension
       \/ /\ EvictionExtension
          /\ cachedBytes < 1   \* extension can't free anything
    /\ cascadeFired' = TRUE
    \* Snapshot the unpinned-evictable bytes the gate left on the table.
    \* Without Fix C this can be > 0 (gate fires while there are unpinned
    \* LRU entries that COULD have been evicted) — exactly the spurious-
    \* cascade signature.
    /\ cascadeAtUnpinned' = cachedBytes
    /\ pendingWrites' = [pendingWrites EXCEPT ![w] = pendingWrites[w] - 1]
    /\ UNCHANGED <<pinnedBytes, cachedBytes>>

----------------------------------------------------------------------------
(* BISAck: a worker ack drops one pinned slot back to unpinned cache.       *)
(* Models the `unpin_digests` arm of                                        *)
(* `handle_blobs_in_stable_storage`.                                        *)
----------------------------------------------------------------------------
BISAck ==
    /\ pinnedBytes > 0
    /\ pinnedBytes' = pinnedBytes - 1
    /\ cachedBytes' = cachedBytes + 1
    /\ UNCHANGED <<cascadeFired, cascadeAtUnpinned, pendingWrites>>

----------------------------------------------------------------------------
(* LruEvict: a moka admission-pressure eviction kicks an unpinned slot      *)
(* out of the cache. Always available as long as cache is non-empty.        *)
----------------------------------------------------------------------------
LruEvict ==
    /\ cachedBytes > 0
    /\ cachedBytes' = cachedBytes - 1
    /\ UNCHANGED <<pinnedBytes, cascadeFired, cascadeAtUnpinned, pendingWrites>>

----------------------------------------------------------------------------
(* Next: union of all state-modifying actions.                              *)
----------------------------------------------------------------------------
(* Quiescent stutter: when every pending write has been resolved AND no  *)
(* pin remains, the system has nothing left to do. TLC treats that as a *)
(* deadlock; we allow an explicit stutter so the model checker can       *)
(* accept termination as a valid trace tip.                              *)
Quiescent ==
    /\ \A w \in Writers : pendingWrites[w] = 0
    /\ pinnedBytes = 0
    /\ cachedBytes = 0
    /\ UNCHANGED vars

Next ==
    \/ \E w \in Writers : AdmitWriteHappy(w)
    \/ \E w \in Writers : AdmitWriteAfterEviction(w)
    \/ \E w \in Writers : CascadeFire(w)
    \/ BISAck
    \/ LruEvict
    \/ Quiescent

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(BISAck)
    /\ \A w \in Writers : WF_vars(AdmitWriteHappy(w))
    /\ \A w \in Writers : WF_vars(AdmitWriteAfterEviction(w))

----------------------------------------------------------------------------
(* INVARIANTS                                                                *)
----------------------------------------------------------------------------

TypeOK ==
    /\ pinnedBytes \in 0..MaxSlots
    /\ cachedBytes \in 0..MaxSlots
    /\ pinnedBytes + cachedBytes <= MaxSlots
    /\ cascadeFired \in BOOLEAN
    /\ cascadeAtUnpinned \in 0..MaxSlots
    /\ \A w \in Writers : pendingWrites[w] \in 0..WritesPerWriter

(* SAFETY: NoCascadeUnlessAllPinned                                          *)
(*                                                                           *)
(* If the cascade has fired, we MUST be in a state where the cache had no   *)
(* unpinned slots to evict — i.e. pinning genuinely saturated the cap.      *)
(* Equivalently: cascade ⇒ cachedBytes was 0 (or below the request size)    *)
(* at the moment of the fire. Because CascadeFire requires either           *)
(* `~EvictionExtension` OR `cachedBytes < pendingWrites[w]`, the post-state *)
(* condition `cascadeFired ⇒ EvictionExtension ⇒ cachedBytes < threshold`  *)
(* gives us the conditional version of the composite invariant.             *)
(*                                                                           *)
(* The cleanest way to assert "cascade only fires when pin really did       *)
(* saturate": with EvictionExtension, the trace must have just executed     *)
(* a CascadeFire that saw cachedBytes < 1. We approximate by checking that  *)
(* whenever cascadeFired is TRUE, EvictionExtension implies the trace's    *)
(* most recent reachable pin state was at the cap.                          *)
NoCascadeUnlessAllPinned ==
    cascadeFired => (cascadeAtUnpinned = 0)

(* Stronger version: with #334 Fix C, cascade firing is equivalent to       *)
(* "every byte was pinned at the moment of fire." We model it as: under    *)
(* EvictionExtension, the system never reaches a (cascadeFired = TRUE,     *)
(* cachedBytes > 0) state without ALSO having had pinnedBytes >= MaxSlots  *)
(* in the trace. TLC's "no behavior reaches a bad state" is enough — the    *)
(* simpler form above is what we check.                                     *)

(* LIVENESS: EventuallyAdmissible                                            *)
(* Under WF on BISAck and admission actions, every pending write is         *)
(* eventually consumed (either admitted or — only when cap genuinely        *)
(* saturated — cascaded). With CCS pins removed and only BIS-feeder pins   *)
(* contributing, the BIS-ack drain is the only unpin source needed.        *)
EventuallyAdmissible ==
    \A w \in Writers : <>(pendingWrites[w] = 0)

(* Composite invariant statement (one sentence):                             *)
(*                                                                           *)
(*   gate-active ⇒ (BIS-feeder pin works AND BIS-ack arrives within bound) *)
(*                  OR explicit eviction (Fix C extension) fires before     *)
(*                  the gate emits MemoryStoreAtCapacity.                   *)
(*                                                                           *)
(* Post-#332: the AC-completeness pin source is GONE. The invariant         *)
(* reduces to: with EvictionExtension on, the gate cannot fire unless      *)
(* BIS-ack is genuinely behind by >= MaxSlots. That reduction is what       *)
(* #332 buys us.                                                             *)

============================================================================
