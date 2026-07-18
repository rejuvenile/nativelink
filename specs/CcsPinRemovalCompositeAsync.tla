------------------------- MODULE CcsPinRemovalCompositeAsync -------------------------
(***************************************************************************
  DE-ATOMIZED variant of CcsPinRemovalComposite.tla (atomicity-audit
  batch A, 2026-07-17). Splits the fused atom the original spec ADMITS
  in its own comment on AdmitWriteHappy:

      "1. check_backpressure_gate sees room — admit, insert into cache,
          then BIS-feeder calls pin_digests, moving the slot from cache
          to pinned. We collapse those two steps because they're atomic
          relative to other writers from the gate's perspective."

  THE FALSE ATOMICITY
    The original `AdmitWriteHappy` moves a slot DIRECTLY into pinnedBytes,
    never transiting the UNPINNED-in-cache state that exists in production
    between the moka insert and the BIS-feeder `pin_digests` call. During
    that window the slot is unpinned and therefore LRU-EVICTABLE. The moka
    admission-pressure eviction (`LruEvict`, already an action in the
    original) can fire in the gap and remove the freshly-inserted slot
    BEFORE the BIS-feeder pins it. Collapsing insert+pin makes that
    "counted-toward-cap, worker-side-pinned, but server-side evicted before
    the durability pin lands" state UNREACHABLE — so the fusion silently
    ENCODES the atomic-pin fix as an unstated assumption.

  WHAT SPLITTING SURFACES
    We split AdmitWriteHappy into:
        AdmitInsert(w)   -- gate admits; slot inserted UNPINNED (pinPending)
        FeederPin        -- BIS-feeder pins a pending slot (pinPending->pinned)
    and let the pre-existing LRU eviction reach a pending slot:
        LruEvictPending  -- moka evicts a still-pin-pending slot -> the
                            admitted write's in-memory-only durability
                            window is LOST (no server-side replica, worker
                            pin now protects nothing on the server).

    CONSTANT AtomicPin toggles the compensator (the real fix):
      * AtomicPin = FALSE (Bugged): insert-then-pin; LruEvictPending is
        reachable -> NoLostDurabilityWindow VIOLATED.
      * AtomicPin = TRUE  (Fixed): the insert pins the slot ATOMICALLY
        (pin_digests holds a guard so the slot is never evictable while
        the durability window is open) -> pinPending stays 0 ->
        LruEvictPending disabled -> HOLDS. This is exactly the atom the
        original spec collapsed — proving the collapse was the fix.

  RELATION TO THE ORIGINAL INVARIANT
    The original `NoCascadeUnlessAllPinned` is NOT what breaks here — the
    de-atomized model actually has MORE evictable slots, so the cascade
    stays precise. The break is a DIFFERENT invariant the original could
    not express because it had no pin-pending state: an admitted write can
    lose its durability window to an eviction the fused atom skipped over.

  CITATIONS
    [insL] nativelink-store/src/memory_store.rs check_backpressure_gate
    [pinL] nativelink-util/src/moka_evicting_map.rs:870-921 (pin_keys — the
           separate call that can also SKIP on pin_cap overflow)
    [BIS]  nativelink-store/src/fast_slow_store.rs:4337,4504,4566
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MaxSlots,
    Writers,
    WritesPerWriter,
    AtomicPin          \* TRUE => insert pins atomically (fix); FALSE => insert-then-pin

ASSUME MaxSlots \in Nat /\ MaxSlots > 0
ASSUME WritesPerWriter \in Nat /\ WritesPerWriter > 0
ASSUME AtomicPin \in BOOLEAN

VARIABLES
    pinned,          \* slots pinned by BIS-feeder (durability window protected)
    pinPending,      \* slots inserted but BIS-feeder pin not yet run (UNPINNED, evictable)
    cached,          \* genuinely unpinned evictable slots (e.g. post BIS-ack)
    lost,            \* admitted writes whose durability window was lost to eviction
    pendingWrites

vars == <<pinned, pinPending, cached, lost, pendingWrites>>

Occupied == pinned + pinPending + cached

----------------------------------------------------------------------------
Init ==
    /\ pinned = 0
    /\ pinPending = 0
    /\ cached = 0
    /\ lost = 0
    /\ pendingWrites = [w \in Writers |-> WritesPerWriter]

----------------------------------------------------------------------------
(* AdmitInsert(w): gate admits (room in cap). Slot inserted UNPINNED.       *)
(*   AtomicPin=TRUE  -> pin lands atomically at insert (straight to pinned).*)
(*   AtomicPin=FALSE -> slot sits pinPending until FeederPin runs.          *)
----------------------------------------------------------------------------
AdmitInsert(w) ==
    /\ pendingWrites[w] > 0
    /\ Occupied + 1 <= MaxSlots
    /\ pendingWrites' = [pendingWrites EXCEPT ![w] = pendingWrites[w] - 1]
    /\ IF AtomicPin
       THEN /\ pinned' = pinned + 1
            /\ UNCHANGED <<pinPending, cached, lost>>
       ELSE /\ pinPending' = pinPending + 1
            /\ UNCHANGED <<pinned, cached, lost>>

----------------------------------------------------------------------------
(* FeederPin: BIS-feeder pins a pin-pending slot (pinPending -> pinned).    *)
----------------------------------------------------------------------------
FeederPin ==
    /\ pinPending > 0
    /\ pinPending' = pinPending - 1
    /\ pinned' = pinned + 1
    /\ UNCHANGED <<cached, lost, pendingWrites>>

----------------------------------------------------------------------------
(* LruEvictPending: moka admission-pressure eviction removes a still-       *)
(* pin-pending slot BEFORE the BIS-feeder pins it — the durability window   *)
(* for that admitted write is LOST. Reachable ONLY when insert-then-pin     *)
(* leaves a window (AtomicPin = FALSE keeps pinPending > 0).                *)
----------------------------------------------------------------------------
LruEvictPending ==
    /\ pinPending > 0
    /\ pinPending' = pinPending - 1
    /\ lost' = lost + 1
    /\ UNCHANGED <<pinned, cached, pendingWrites>>

----------------------------------------------------------------------------
(* BISAck: worker ack drops one pinned slot to unpinned cache.             *)
----------------------------------------------------------------------------
BISAck ==
    /\ pinned > 0
    /\ pinned' = pinned - 1
    /\ cached' = cached + 1
    /\ UNCHANGED <<pinPending, lost, pendingWrites>>

----------------------------------------------------------------------------
(* LruEvict: normal LRU of a genuinely-unpinned slot.                      *)
----------------------------------------------------------------------------
LruEvict ==
    /\ cached > 0
    /\ cached' = cached - 1
    /\ UNCHANGED <<pinned, pinPending, lost, pendingWrites>>

----------------------------------------------------------------------------
Quiescent ==
    /\ \A w \in Writers : pendingWrites[w] = 0
    /\ pinPending = 0
    /\ UNCHANGED vars

Next ==
    \/ \E w \in Writers : AdmitInsert(w)
    \/ FeederPin
    \/ LruEvictPending
    \/ BISAck
    \/ LruEvict
    \/ Quiescent

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(FeederPin)

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ pinned \in 0..MaxSlots
    /\ pinPending \in 0..MaxSlots
    /\ cached \in 0..MaxSlots
    /\ Occupied <= MaxSlots
    /\ lost \in 0..(Cardinality(Writers) * WritesPerWriter)
    /\ \A w \in Writers : pendingWrites[w] \in 0..WritesPerWriter

(* SAFETY: NoLostDurabilityWindow                                          *)
(* No admitted write loses its in-memory durability window to an eviction  *)
(* that fired in the insert-then-pin gap.                                  *)
(*   Bugged (AtomicPin=FALSE): AdmitInsert -> LruEvictPending -> lost=1.    *)
(*   Fixed  (AtomicPin=TRUE):  pinPending never > 0 -> holds.              *)
NoLostDurabilityWindow ==
    lost = 0

============================================================================
