--------------------- MODULE PinListenerMultiplicity -----------------------
(***************************************************************************
  3× FastSlowStore listener registration + populate-pin scope mismatch.

  Models the bug class documented at
  `/home/user/.claude/projects/-src-nativelink/memory/project_pin_listener_multiplicity_2026_04_25.md`
  and the production fix in
  `nativelink-store/src/fast_slow_store.rs:87-160`.

  PRODUCTION SHAPE:
    `local_worker.rs` constructs THREE FastSlowStore wrappers against the
    SAME underlying `FilesystemStore` fast tier (one per
    `FastSlowStore::new` / `new_with_shared_failed_writes` call site).
    Each wrapper registers a `PinExpireFailedWritesListener` on the fast
    store. So the fast store has THREE listener subscriptions for one
    physical store.

    A single pin-expire event on the fast store fires ALL THREE listeners
    in turn. Pre-fix, every listener would unconditionally execute its
    `failed_slow_writes.insert(digest)` + `warn!(...)` on every event.
    Result: 3× warn fan-out per pin expiry, AND 3× insert into the shared
    `failed_slow_writes` set, AND any digest pinned by a non-slow-write
    code path (e.g. `DirectoryCache` download pins) would also trigger
    the listeners — even though no slow-write was actually outstanding
    for that digest. Total observed: 5774 listener-fire events / 10 min
    on production workers, mostly dead-weight.

  POST-FIX (current production):
    The listener carries TWO Arcs — `failed_slow_writes` (typically shared
    across wrappers) AND `in_flight_slow_writes` (PER-wrapper). The
    `on_pin_expired` hook checks whether the per-wrapper in-flight map
    contains the digest. If not, it returns early. So:
      * download-pin expiries: no wrapper's in-flight contains the digest
        -> all three listeners no-op -> 0 spurious entries.
      * slow-write hangs: exactly one wrapper's in-flight contains the
        digest -> exactly one listener fires the warn + insert.
    The 3× amplification collapses to 1× across the composition.

  CONSTANTS:
    * NumWrappers (Nat): how many FastSlowStore wrappers register a
      listener on the same fast store. Production has 3.
    * BugMode (BOOLEAN): TRUE => pre-fix listener (no in-flight gate);
      FALSE => post-fix listener with the gate.

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * PinListenerMultiplicityFixed.cfg (BugMode = FALSE):
      No invariant violation. The amplification invariants hold.
    * PinListenerMultiplicityBugged.cfg (BugMode = TRUE):
      INVARIANT VIOLATED on `EveryEventFiresExactlyOneListener`. Trace
      shows: a single pin-expire event for a digest with one slow-write
      outstanding -> all three listeners fire -> firedListeners = 3 ->
      invariant violated.
      Also INVARIANT VIOLATED on `DownloadPinsDoNotFireWarn` for digests
      pinned via the download path (no in-flight slow-write entry).

  SCOPE — what this spec models:
    * one digest D,
    * NumWrappers wrappers (production = 3), each with a per-wrapper
      `in_flight` boolean for D (TRUE means D has an outstanding
      slow-write spawn at THIS wrapper),
    * one pin-expire event for D — modeled as a single transition that
      iterates over all wrappers' listeners,
    * a `firedListeners` counter and a `warnEmitted` boolean reflecting
      what the listeners did.

  SCOPE — what this spec does NOT model:
    * the actual MokaEvictingMap pin-TTL clock (modeled as a
      non-deterministic event),
    * concurrent pin events for multiple digests (orthogonal — the bug
      is per-digest amplification),
    * the secondary effects on `failed_slow_writes` (modeled by the
      counter; whether the inserts are unique or duplicate is a
      property of HashSet, not the listener),
    * the listener registration race (we assume all NumWrappers
      registrations completed before the event fires),
    * the `register_item_callback` error path (returns Err if
      MAX_CALLBACKS exceeded — orthogonal),
    * the `callback` arm (`on_remove`) which is unrelated — only the
      `on_pin_expired` arm is in scope.

  CITATIONS:
    [list] nativelink-store/src/fast_slow_store.rs:87-115 (listener doc)
    [impl] nativelink-store/src/fast_slow_store.rs:128-160 (on_pin_expired)
    [reg]  nativelink-store/src/fast_slow_store.rs:176-192 (register fn)
    [s1]   nativelink-store/src/fast_slow_store.rs:357-360 (registration site 1)
    [s2]   nativelink-store/src/fast_slow_store.rs:590     (registration site 2)
    [memo] /home/user/.claude/projects/-src-nativelink/memory/project_pin_listener_multiplicity_2026_04_25.md
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    NumWrappers,   \* Number of FastSlowStore wrappers (production: 3)
    BugMode        \* TRUE => pre-fix listener (no in-flight gate)

ASSUME NumWrappers \in Nat /\ NumWrappers >= 1
ASSUME BugMode \in BOOLEAN

\* Wrapper IDs.
Wrappers == 1..NumWrappers

\* Pin-source classification for the single digest D we model.
\* Uniform record shape so TLC's equality checks are well-typed:
\*   [type |-> "DownloadPin", w |-> 0]
\*     - DirectoryCache download pin: no wrapper has a slow-write
\*       outstanding for D. Should NOT fire any warn. Owner field
\*       w |-> 0 is a "no owner" sentinel (0 \notin Wrappers).
\*   [type |-> "SlowWriteAt", w |-> N]
\*     - exactly wrapper N has a slow-write outstanding for D.
\*       Should fire exactly ONE warn (from wrapper N).
DownloadPinSrc == [type |-> "DownloadPin", w |-> 0]
SlowWriteSrc(w) == [type |-> "SlowWriteAt", w |-> w]
PinSources == {DownloadPinSrc} \cup {SlowWriteSrc(w) : w \in Wrappers}

\* Pin-event lifecycle states.
\*   "NoEventYet"   - the pin TTL hasn't fired.
\*   "Firing"       - the event fired; listeners haven't all run.
\*   "Done"         - all listeners have run.
EventStates == {"NoEventYet", "Firing", "Done"}

VARIABLES
    pinSource,        \* PinSources: how the pin came into existence
    eventState,       \* one of EventStates
    listenersRemaining, \* set of wrapper IDs whose listener hasn't run yet
    firedCount,       \* number of listeners that did the insert + warn
    failedSetCount    \* number of insertions into failed_slow_writes
                      \* (in HashSet semantics this collapses; we count raw
                      \* insert calls to expose the bug's amplification)

vars == <<pinSource, eventState, listenersRemaining, firedCount, failedSetCount>>

----------------------------------------------------------------------------
(* Init: no event yet; pinSource is non-deterministically chosen at        *)
(* the first transition (PickPinSource).                                   *)
----------------------------------------------------------------------------
Init ==
    /\ pinSource = DownloadPinSrc   \* dummy; first action overwrites
    /\ eventState = "NoEventYet"
    /\ listenersRemaining = {}
    /\ firedCount = 0
    /\ failedSetCount = 0

----------------------------------------------------------------------------
(* PickPinSourceDownload / PickPinSourceSlowWrite(w): non-deterministic   *)
(* choice of how the pin came into existence. Models the worker code      *)
(* paths that pin a digest:                                                *)
(*   * DirectoryCache::download_to_cache (no slow-write follows)          *)
(*   * RunningActionsManagerImpl uploads via update -> slow-write spawn   *)
(*     populates ONE wrapper's in_flight_slow_writes.                     *)
(* Once chosen, eventState transitions to Firing in StartFiring.          *)
----------------------------------------------------------------------------
PickPinSourceDownload ==
    /\ eventState = "NoEventYet"
    /\ pinSource' = DownloadPinSrc
    /\ UNCHANGED <<eventState, listenersRemaining, firedCount, failedSetCount>>

PickPinSourceSlowWrite(w) ==
    /\ eventState = "NoEventYet"
    /\ pinSource' = SlowWriteSrc(w)
    /\ UNCHANGED <<eventState, listenersRemaining, firedCount, failedSetCount>>

----------------------------------------------------------------------------
(* StartFiring: the pin TTL fires for D. ALL NumWrappers listeners are    *)
(* enqueued to run. Models the MokaEvictingMap broadcasting the           *)
(* on_pin_expired callback to every registered listener.                  *)
----------------------------------------------------------------------------
StartFiring ==
    /\ eventState = "NoEventYet"
    /\ eventState' = "Firing"
    /\ listenersRemaining' = Wrappers
    /\ UNCHANGED <<pinSource, firedCount, failedSetCount>>

----------------------------------------------------------------------------
(* RunListener(w): listener for wrapper w runs. Whether it fires the     *)
(* warn + failed-set insert depends on BugMode.                          *)
(*                                                                        *)
(* Bugged (BugMode = TRUE): no in-flight gate. Every listener fires      *)
(*   unconditionally, regardless of pinSource.                           *)
(* Fixed  (BugMode = FALSE): gate checks if THIS wrapper's in-flight    *)
(*   contains D. The "is in_flight" predicate is:                        *)
(*     pinSource is SlowWriteAt(w)  AND  this wrapper IS w.              *)
(*   Otherwise (DownloadPin OR pinSource is for a different wrapper),    *)
(*   the listener returns early.                                          *)
----------------------------------------------------------------------------
ListenerWouldFire(w) ==
    \/ BugMode                                              \* no gate at all
    \/ pinSource = SlowWriteSrc(w)                          \* this wrapper owns

RunListener(w) ==
    /\ eventState = "Firing"
    /\ w \in listenersRemaining
    /\ listenersRemaining' = listenersRemaining \ {w}
    /\ \/ /\ ListenerWouldFire(w)
          /\ firedCount' = firedCount + 1
          /\ failedSetCount' = failedSetCount + 1
       \/ /\ ~ListenerWouldFire(w)
          /\ firedCount' = firedCount
          /\ failedSetCount' = failedSetCount
    /\ UNCHANGED <<pinSource, eventState>>

----------------------------------------------------------------------------
(* CompleteFiring: all listeners have run; transition to Done.            *)
----------------------------------------------------------------------------
CompleteFiring ==
    /\ eventState = "Firing"
    /\ listenersRemaining = {}
    /\ eventState' = "Done"
    /\ UNCHANGED <<pinSource, listenersRemaining, firedCount, failedSetCount>>

----------------------------------------------------------------------------
Next ==
    \/ PickPinSourceDownload
    \/ \E w \in Wrappers : PickPinSourceSlowWrite(w)
    \/ StartFiring
    \/ \E w \in Wrappers : RunListener(w)
    \/ CompleteFiring

----------------------------------------------------------------------------
Spec ==
    /\ Init
    /\ [][Next]_vars

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ pinSource \in PinSources
    /\ eventState \in EventStates
    /\ listenersRemaining \subseteq Wrappers
    /\ firedCount \in 0..NumWrappers
    /\ failedSetCount \in 0..NumWrappers

(* SAFETY: EveryEventFiresAtMostOneListener                                *)
(*                                                                          *)
(* The intended semantics: ONE pin-expire event for ONE digest with       *)
(* slow-writes outstanding in some wrappers fires AT MOST ONE listener    *)
(* (the one whose wrapper owns the slow-write). For DownloadPin events,   *)
(* zero listeners fire.                                                    *)
(*                                                                          *)
(* Under BugMode = TRUE every listener fires regardless of pinSource:     *)
(* firedCount can reach NumWrappers (3 in production) -> invariant        *)
(* violated.                                                                *)
EveryEventFiresAtMostOneListener ==
    eventState = "Done" => firedCount <= 1

(* SAFETY: DownloadPinsDoNotFireWarn                                        *)
(*                                                                          *)
(* If the pin originated from a `DirectoryCache` download (i.e. no       *)
(* slow-write was ever queued for D), no listener should fire. Under     *)
(* BugMode = TRUE every listener fires anyway, producing                  *)
(* failedSetCount = NumWrappers dead-weight entries that get retried     *)
(* on reconnect for blobs the server already has.                         *)
DownloadPinsDoNotFireWarn ==
    eventState = "Done" /\ pinSource = DownloadPinSrc => firedCount = 0

(* SAFETY: NoAmplificationOverNumWrappers                                  *)
(*                                                                          *)
(* This is the cleanest expression of the bug as a number. Under fix,    *)
(* failedSetCount is at most 1 per pin event regardless of NumWrappers. *)
(* Under bug, failedSetCount equals NumWrappers (3 per pin-expire on    *)
(* production). The 5774 events / 10 min are exactly this 3× factor    *)
(* applied to the underlying pin-expire rate.                            *)
NoAmplificationOverNumWrappers ==
    eventState = "Done" => failedSetCount <= 1

============================================================================
