--------------------------- MODULE FailedSlowWritesRetry ---------------------------
(***************************************************************************
  failed_slow_writes retry-on-reconnect cycle.

  Models the worker-side retry protocol that drains
  `failed_slow_writes` on every reconnect and re-uploads the digests to
  the server. The bug class: under persistent slow-store failure (e.g.
  server-side store is permanently unreachable, or the slow-store write
  is rejected for a reason that does not fix itself on retry), the
  retry path forms an infinite cycle that never converges.

  PRODUCTION SHAPE:
    Worker `LocalWorkerImpl::run_inner` (`local_worker.rs:1327-1355`)
    calls `cas_store.drain_failed_digests()` on every reconnect to the
    server. The drained digests are re-pinned and handed to
    `Self::handle_upload_missing_blobs(&ram, failed)`. On failure of
    each individual upload the function only logs `warn!`; the digest
    is NOT re-inserted into `failed_slow_writes` from this path.

    A digest re-enters `failed_slow_writes` only via two paths in
    `fast_slow_store.rs`:
      [a] in-band slow-store write failure during `update`/
          `update_oneshot` — `:2033` and `:2370`,
      [b] pin-TTL auto-expire while the slow-write is still in flight
          — `:153`, the `on_pin_expired` listener.

    Path [a] fires only when the worker writes a NEW blob to the slow
    store. After the reconnect retry, the worker is uploading via
    `slow_store.update_oneshot` directly (NOT the FastSlowStore wrapper),
    so [a] is bypassed for the retry path itself.

    Path [b] re-fires whenever the pin TTL expires for a digest still
    pinned and still in the in-flight slow-write map. Because the
    reconnect retry RE-PINS the digest before uploading
    (`local_worker.rs:1345`), the pin TTL clock restarts. If the upload
    HANGS (rather than fails fast with an error), the in-flight map
    keeps the entry, and the pin TTL eventually re-fires path [b].

    Result: under persistent server-side failure, the worker can loop:
        drain -> re-pin -> upload-hang -> pin-TTL -> re-insert ->
        next-reconnect -> drain -> ...

    Today no backoff, no max-retry counter, no retry-budget. The
    cycle is bounded only by the worker's lifetime and the rate of
    reconnect events.

  THE BUG STATEMENT:
    "Under persistent failure, the retry cycle never converges to a
    state where `failed_slow_writes` is empty AND the digest has been
    durably uploaded."

  CONSTANTS:
    * MaxRetries (Nat): bound used in the FIXED config to model a
      max-retry counter. The bugged config uses MaxRetries = 0 to
      represent "no max" (modeled as "the counter is never checked",
      i.e., the retry path is unconditional).
    * BugMode (BOOLEAN): TRUE => retry has no convergence mechanism
      (current production); FALSE => retry observes MaxRetries.

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * FailedSlowWritesRetryFixed.cfg (BugMode = FALSE, MaxRetries=2):
      No invariant violation. Liveness `EventuallyConverges` holds.
    * FailedSlowWritesRetryBugged.cfg (BugMode = TRUE):
      LIVENESS VIOLATED on `EventuallyConverges`. TLC produces a
      cycle counter-example: drain -> re-pin -> upload-fail -> pin-
      TTL re-insert -> drain -> ... where `failedSlowWrites = {"D"}`
      recurs infinitely. The trace shape is exactly the production
      symptom.

  SCOPE — what this spec models:
    * one digest D,
    * a worker with a `failedSlowWrites` set, an in-flight slow-write
      flag, and a pin-state,
    * a reconnect lifecycle: Connected -> Disconnected -> Connected,
    * an upload outcome that's non-deterministically Success or
      Failure (modeling the "server is up vs. down" distinction;
      under bug we look at the trace where Failure repeats),
    * a retry counter (per-digest) used by the FIXED config to bound
      the loop.

  SCOPE — what this spec does NOT model:
    * the actual pin-TTL clock (modeled as a non-deterministic
      transition that fires when in-flight is set),
    * `MIRROR_BLOBS_MAX_BYTES` cap on the worker mirror (orthogonal),
    * `handle_upload_missing_blobs`'s concurrent-upload semaphore
      (`MAX_CONCURRENT_UPLOADS=32` — orthogonal; the bug is per-
      digest cycle convergence),
    * the failed_slow_writes mutex (modeled as atomic set ops),
    * race between drain and re-insert (we serialize transitions),
    * the C+D StableDigestDelegation refactor in flight (orthogonal).

  CITATIONS:
    [drain]     nativelink-worker/src/local_worker.rs:1332-1352
    [retry]     nativelink-worker/src/local_worker.rs:852-1022
                  (handle_upload_missing_blobs)
    [insert-a]  nativelink-store/src/fast_slow_store.rs:2033, :2370
    [insert-b]  nativelink-store/src/fast_slow_store.rs:128-155
                  (PinExpireFailedWritesListener::on_pin_expired)
    [drain_fn]  nativelink-store/src/fast_slow_store.rs:563-575
                  (drain_failed_digests / pop)
    [test-a]    nativelink-store/tests/pin_expire_failed_writes_test.rs
                  :231-279 (pin auto-expire inserts into failed_slow_writes)
    [test-b]    nativelink-store/tests/pin_expire_failed_writes_test.rs
                  :311-360 (slow_write_watchdog inserts on hang)
    [test-c]    nativelink-store/tests/failpoint_tests.rs:858
                  (forced failure inserts into failed_slow_writes)
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MaxRetries,    \* Per-digest retry budget under the FIXED model
    BugMode        \* TRUE => no retry budget (current production)

ASSUME MaxRetries \in Nat
ASSUME BugMode \in BOOLEAN

\* Worker connection lifecycle.
\*   "Connected"    - has live gRPC stream to the server.
\*   "Disconnected" - reconnect is needed.
ConnStates == {"Connected", "Disconnected"}

\* Per-digest in-flight slow-write flag (for path [b]).
\*   "None"        - no slow-write currently outstanding for D.
\*   "InFlight"    - update_oneshot or update is running for D
\*                   (or just spawned and not yet acked).
\*   "ServerAcked" - server returned Ok; the digest is durably stored
\*                   on the slow store and we're done.
SlowWriteStates == {"None", "InFlight", "ServerAcked"}

\* Pin lifecycle for D on the fast tier.
\*   "Pinned"   - MokaEvictingMap pin held; pin TTL clock running.
\*   "Unpinned" - released (e.g. via BIS receive — out of scope here).
PinStates == {"Pinned", "Unpinned"}

VARIABLES
    conn,             \* one of ConnStates
    slowWrite,        \* one of SlowWriteStates
    pin,              \* one of PinStates
    failedSlowWrites, \* set of digests waiting for retry; subset of {"D"}
    retryCount,       \* per-digest count used by FIXED model
    droppedAfterMaxRetries  \* sticky: TRUE once D was dropped under FIXED
                            \* (modeling "operator dead-letter queue / alert")

vars == <<conn, slowWrite, pin, failedSlowWrites, retryCount,
          droppedAfterMaxRetries>>

----------------------------------------------------------------------------
Init ==
    /\ conn = "Connected"
    /\ slowWrite = "InFlight"           \* worker just spawned the write
    /\ pin = "Pinned"
    /\ failedSlowWrites = {}
    /\ retryCount = 0
    /\ droppedAfterMaxRetries = FALSE

----------------------------------------------------------------------------
(* PinTtlInsertOnHang: pin TTL fires while slow-write is still         *)
(* InFlight; the on_pin_expired listener inserts D into                *)
(* failed_slow_writes. Models fast_slow_store.rs:128-155.              *)
(* This corresponds to the "slow-write hang" production scenario.     *)
----------------------------------------------------------------------------
PinTtlInsertOnHang ==
    /\ slowWrite = "InFlight"
    /\ pin = "Pinned"
    /\ "D" \notin failedSlowWrites
    /\ failedSlowWrites' = failedSlowWrites \cup {"D"}
    /\ UNCHANGED <<conn, slowWrite, pin, retryCount,
                    droppedAfterMaxRetries>>

----------------------------------------------------------------------------
(* SlowWriteSucceeds: in-flight slow-write returns Ok. The digest is    *)
(* durably on the server. We do NOT remove it from failedSlowWrites    *)
(* — the listener inserted on the pin-TTL hang and the actual write   *)
(* outcome are orthogonal in production. The retry path will discover *)
(* via has_with_results that the blob is durably stored and skip.     *)
(*                                                                          *)
(* For convergence purposes the spec requires the retry path to drain *)
(* the digest from failedSlowWrites AND for slowWrite to be           *)
(* "ServerAcked" — only then has the cycle truly closed.               *)
----------------------------------------------------------------------------
SlowWriteSucceeds ==
    /\ slowWrite = "InFlight"
    /\ slowWrite' = "ServerAcked"
    /\ UNCHANGED <<conn, pin, failedSlowWrites, retryCount,
                    droppedAfterMaxRetries>>

----------------------------------------------------------------------------
(* SlowWriteFailsInBand: in-flight slow-write returns Err in-band      *)
(* (network blip, server returned error). Models fast_slow_store.rs   *)
(* :2033, :2370. The error path inserts the digest into                *)
(* failed_slow_writes if not already present.                            *)
----------------------------------------------------------------------------
SlowWriteFailsInBand ==
    /\ slowWrite = "InFlight"
    /\ slowWrite' = "None"
    /\ failedSlowWrites' = failedSlowWrites \cup {"D"}
    /\ UNCHANGED <<conn, pin, retryCount, droppedAfterMaxRetries>>

----------------------------------------------------------------------------
(* Disconnect: gRPC stream closes (server restart, network).            *)
----------------------------------------------------------------------------
Disconnect ==
    /\ conn = "Connected"
    /\ conn' = "Disconnected"
    /\ UNCHANGED <<slowWrite, pin, failedSlowWrites, retryCount,
                    droppedAfterMaxRetries>>

----------------------------------------------------------------------------
(* Reconnect: worker reconnects to the server. Drains                  *)
(* failed_slow_writes and re-pins the digests. Models                   *)
(* local_worker.rs:1327-1352. The retry-pipeline that follows this    *)
(* drain lives in DrainAndRetry below.                                   *)
----------------------------------------------------------------------------
Reconnect ==
    /\ conn = "Disconnected"
    /\ conn' = "Connected"
    /\ UNCHANGED <<slowWrite, pin, failedSlowWrites, retryCount,
                    droppedAfterMaxRetries>>

----------------------------------------------------------------------------
(* DrainAndRetryUnbounded (BugMode = TRUE):                              *)
(* On reconnect, drain failed_slow_writes and start a fresh slow-write *)
(* spawn for each digest. NO retry budget. Models the production       *)
(* code at local_worker.rs:1332-1352 — drain returns the set, the      *)
(* worker re-pins and spawns uploads, no counter check.                  *)
(*                                                                          *)
(* Key transition: failedSlowWrites becomes empty AND slowWrite goes  *)
(* from "None" to "InFlight" (the upload spawn). pin is refreshed     *)
(* (Pinned, possibly was Unpinned).                                     *)
----------------------------------------------------------------------------
DrainAndRetryUnbounded ==
    /\ BugMode
    /\ conn = "Connected"
    /\ "D" \in failedSlowWrites
    /\ slowWrite # "InFlight"            \* idempotent guard
    /\ failedSlowWrites' = failedSlowWrites \ {"D"}
    /\ slowWrite' = "InFlight"
    /\ pin' = "Pinned"
    /\ UNCHANGED <<conn, retryCount, droppedAfterMaxRetries>>
    \* Note: under BugMode we do NOT increment retryCount because the
    \* counter doesn't exist in production code. This also keeps the
    \* state space finite for TLC: the cycle becomes reachable as a
    \* finite SCC the model checker can detect.

----------------------------------------------------------------------------
(* DrainAndRetryBounded (BugMode = FALSE):                               *)
(* On reconnect, drain failed_slow_writes ONLY if the digest's retry   *)
(* count has not exceeded MaxRetries. If exceeded, drain and dead-     *)
(* letter (set droppedAfterMaxRetries) — do NOT re-spawn the upload.   *)
(* This models a hypothetical convergence mechanism.                     *)
----------------------------------------------------------------------------
DrainAndRetryBounded ==
    /\ ~BugMode
    /\ conn = "Connected"
    /\ "D" \in failedSlowWrites
    /\ slowWrite # "InFlight"
    /\ retryCount < MaxRetries
    /\ failedSlowWrites' = failedSlowWrites \ {"D"}
    /\ slowWrite' = "InFlight"
    /\ pin' = "Pinned"
    /\ retryCount' = retryCount + 1
    /\ UNCHANGED <<conn, droppedAfterMaxRetries>>

DrainAndDeadLetter ==
    /\ ~BugMode
    /\ conn = "Connected"
    /\ "D" \in failedSlowWrites
    /\ slowWrite # "InFlight"
    /\ retryCount >= MaxRetries
    /\ failedSlowWrites' = failedSlowWrites \ {"D"}
    /\ droppedAfterMaxRetries' = TRUE
    /\ UNCHANGED <<conn, slowWrite, pin, retryCount>>

----------------------------------------------------------------------------
Next ==
    \/ PinTtlInsertOnHang
    \/ SlowWriteSucceeds
    \/ SlowWriteFailsInBand
    \/ Disconnect
    \/ Reconnect
    \/ DrainAndRetryUnbounded
    \/ DrainAndRetryBounded
    \/ DrainAndDeadLetter

----------------------------------------------------------------------------
(* Fairness so the FIXED config can demonstrate liveness convergence.   *)
(* Note: SlowWriteSucceeds is INTENTIONALLY NOT given fairness — under  *)
(* persistent server-side failure the slow-write never succeeds. The   *)
(* only way the FIXED model converges under persistent failure is via *)
(* the dead-letter path bounded by MaxRetries.                          *)
(* SlowWriteFailsInBand IS given fairness: in the persistent-failure  *)
(* trace, every InFlight write must eventually fail.                    *)
----------------------------------------------------------------------------
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(PinTtlInsertOnHang)
    /\ WF_vars(SlowWriteFailsInBand)
    /\ WF_vars(Reconnect)
    /\ SF_vars(DrainAndRetryUnbounded)
    /\ SF_vars(DrainAndRetryBounded)
    /\ SF_vars(DrainAndDeadLetter)
    \* Disconnect is left UNFAIR: under FIXED + persistent failure,
    \* the model must converge even if Disconnect never happens after
    \* the dead-letter fires.
    \* SF on the drain actions ensures they fire even across the
    \* Disconnect cycles where they're transiently disabled — i.e.,
    \* "infinitely often enabled implies infinitely often taken".
    \* Models the production reality that the worker WILL drain and
    \* retry on EVERY reconnect, not just sometimes.

----------------------------------------------------------------------------
(* INVARIANTS                                                              *)
----------------------------------------------------------------------------

TypeOK ==
    /\ conn \in ConnStates
    /\ slowWrite \in SlowWriteStates
    /\ pin \in PinStates
    /\ failedSlowWrites \subseteq {"D"}
    /\ retryCount \in Nat
    /\ droppedAfterMaxRetries \in BOOLEAN

(* SAFETY: RetryCountIsBoundedUnderFix                                     *)
(*                                                                          *)
(* Under FIXED, retryCount must never exceed MaxRetries. This is the     *)
(* type-system bound on the convergence mechanism.                       *)
RetryCountIsBoundedUnderFix ==
    ~BugMode => retryCount <= MaxRetries

(* SAFETY: NoReinsertAfterServerAckOnDeadDigest                            *)
(*                                                                          *)
(* Once the slow-write succeeds AND the worker has had a chance to      *)
(* observe the success (modeled as DrainAndRetry firing or a future    *)
(* drain that finds the set empty), the digest should not be in         *)
(* failedSlowWrites. The weaker form here: if slowWrite is              *)
(* "ServerAcked" AND the pin has been released (Unpinned), no path     *)
(* should re-insert. Pin-TTL is gated on Pinned, so this holds         *)
(* trivially.                                                              *)
(*                                                                          *)
(* This is a sanity check on the spec, not the bug-witness; the bug    *)
(* is in convergence, exposed by the LIVENESS property.                  *)
NoReinsertAfterAckAndUnpin ==
    (slowWrite = "ServerAcked" /\ pin = "Unpinned") => "D" \notin failedSlowWrites

(* LIVENESS: EventuallyConverges                                            *)
(*                                                                          *)
(* The system eventually reaches a state where:                            *)
(*   * failedSlowWrites is empty, AND                                       *)
(*   * either: slowWrite has been ServerAcked (success path), OR           *)
(*           droppedAfterMaxRetries is TRUE (dead-letter path).            *)
(*                                                                          *)
(* Under FIXED + persistent failure (slowWrite repeatedly transitions    *)
(* InFlight -> None via SlowWriteFailsInBand): the retry loop runs at    *)
(* most MaxRetries times, then DrainAndDeadLetter fires and the system  *)
(* converges with droppedAfterMaxRetries = TRUE.                           *)
(*                                                                          *)
(* Under BUGGED + persistent failure: each reconnect re-pins + re-spawns *)
(* the upload, which in-band fails AND/OR pin-TTL re-inserts the         *)
(* digest. failedSlowWrites repeatedly returns to {"D"}; the system     *)
(* never converges. TLC produces a cycle counter-example.                 *)
(*                                                                          *)
(* Note: the success-path branch is reachable for any trace where       *)
(* SlowWriteSucceeds happens to fire — TLC explores that trace and     *)
(* the property holds along it. The bug counter-example is the         *)
(* persistent-failure cycle.                                            *)
EventuallyConverges ==
    <>(failedSlowWrites = {}
       /\ (slowWrite = "ServerAcked" \/ droppedAfterMaxRetries))

============================================================================
