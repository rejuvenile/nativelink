--------------------- MODULE PhantomBlobExistenceCache ---------------------
(***************************************************************************
  Phantom-blob false-alarm conflation in FastSlowStore::run_producer.

  Models the bug class documented at
  `/home/user/.claude/projects/-src-nativelink/memory/project_phantom_blob_false_alarm_2026_04_25.md`
  and the production fix in
  `nativelink-store/src/fast_slow_store.rs:912-1222`.

  HISTORIC BUG SHAPE (pre-fix):
    `head_was_ok` was a single boolean, set to TRUE in BOTH cases:
      (a) `slow_store.has(key)` returned `Ok(Some(size))` — actual
          existence evidence,
      (b) the LazyExistenceOnSync optimisation skipped the `.has()`
          call entirely and short-circuited to
          `Ok(UploadSizeInfo::MaxSize(u64::MAX))`.
    Downstream, the PHANTOM BLOB warn was gated on
      `head_was_ok && merged.is_err() && err.code == NotFound`
    — interpreting `head_was_ok=TRUE` as "we know slow_store said the
    blob is present", which it is NOT in case (b). Result: 178
    false-alarm warns / 10 minutes on production workers, classified
    HIGH severity by the anomaly scan, drowning real "blob disappeared"
    alerts in noise.

  POST-FIX (current production):
    The single boolean is replaced with a tri-state flag
    `has_actually_returned_some`, set ONLY in the real has-then-Some
    branch (fast_slow_store.rs:966-969). The lazy short-circuit no
    longer flips it. The PHANTOM BLOB warn is gated on the corrected
    flag (fast_slow_store.rs:1211-1222).

  WHAT THIS SPEC MODELS (and the C+D-style remediation captured by it):
    * The HEAD-decision step has THREE possible outcomes:
        - HasReturnedSome   (real Some — slow store has the blob)
        - HasReturnedNone   (real None — proper NotFound)
        - LazySkip          (LazyExistenceOnSync — skipped has())
    * The downstream POPULATE step has two outcomes (Ok / NotFound),
      gated on whether the BUG FLAG (`flag_set_by_head`) is true.
    * The BugMode CONSTANT toggles the flag-update semantics:
        - BugMode = TRUE: the flag is set on BOTH HasReturnedSome AND
          LazySkip — the historic bug.
        - BugMode = FALSE: the flag is set ONLY on HasReturnedSome —
          the post-fix code.

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * PhantomBlobExistenceCacheFixed.cfg (BugMode = FALSE):
      No invariant violation. WarnFiresOnlyOnGenuineRace holds — the
      PHANTOM BLOB warn is emitted ONLY when has() actually returned
      Some.
    * PhantomBlobExistenceCacheBugged.cfg (BugMode = TRUE):
      INVARIANT VIOLATED on `WarnFiresOnlyOnGenuineRace`. Trace
      shows: HeadStepLazySkip -> PopulateNotFound -> Warn fires
      with had_real_has_some = FALSE -> the false-alarm scenario.

  SCOPE — what this spec models:
    * one digest at a time,
    * head-decision step as a single atomic outcome (one of three),
    * populate step as a single atomic outcome (Ok / NotFound),
    * a single boolean flag that the head step writes and the warn
      step reads,
    * a single warn-emission boolean that is set when the gate fires.

  SCOPE — what this spec does NOT model:
    * actual data streaming (treated as atomic outcomes),
    * the LazyExistenceOnSync optimisation's other behaviours (its
      only relevance here is that it skips has() — modeled exactly
      that way),
    * the join3 of data_stream / slow_store / fast_store (treated as
      a single populate outcome — orthogonal to the conflation bug),
    * the warn-vs-error level distinction (the warn IS the bug
      symptom; we don't model log levels),
    * concurrent populates of the same digest (orthogonal — the
      conflation fires per-populate),
    * the broader ExistenceCacheStore wrapping FastSlowStore (its
      stale-positive prevention is a separate spec area; here the
      bug is in FastSlowStore::run_producer's flag update).

  CITATIONS:
    [head] nativelink-store/src/fast_slow_store.rs:912-973 (head_result block)
    [flag] nativelink-store/src/fast_slow_store.rs:920 (has_actually_returned_some decl)
    [set]  nativelink-store/src/fast_slow_store.rs:966-969 (the real-Some-only set)
    [warn] nativelink-store/src/fast_slow_store.rs:1201-1222 (PHANTOM BLOB gate)
    [test] nativelink-store/tests/fast_slow_store_test.rs:3008-3052
    [memo] /home/user/.claude/projects/-src-nativelink/memory/project_phantom_blob_false_alarm_2026_04_25.md
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    BugMode    \* TRUE => head flag is set on LazySkip (the historic bug).

ASSUME BugMode \in BOOLEAN

\* HEAD step states.
\*   "Pending"       - haven't decided yet.
\*   "HasReturnedSome"- has() returned Ok(Some(size)).
\*   "HasReturnedNone"- has() returned Ok(None) — proper NotFound at head.
\*   "LazySkip"      - LazyExistenceOnSync short-circuited; has() not called.
HeadStates == {"Pending", "HasReturnedSome", "HasReturnedNone", "LazySkip"}

\* POPULATE step states.
\*   "Pending"  - haven't issued the populate yet.
\*   "Ok"       - data stream + slow get + fast write all completed Ok.
\*   "NotFound" - the populate path produced NotFound (slow store missing).
\*   "Skipped"  - head returned None; populate is short-circuited to NotFound,
\*                we never get this far.
PopulateStates == {"Pending", "Ok", "NotFound", "Skipped"}

VARIABLES
    head,                 \* one of HeadStates
    flagSetByHead,        \* boolean: did the head step set the flag?
    realHasReturnedSome,  \* boolean: ground truth — did has() actually
                          \* return Some? (Used in the invariant only.)
    populate,             \* one of PopulateStates
    warnEmitted           \* boolean: did the gate fire and emit PHANTOM BLOB?

vars == <<head, flagSetByHead, realHasReturnedSome, populate, warnEmitted>>

----------------------------------------------------------------------------
Init ==
    /\ head = "Pending"
    /\ flagSetByHead = FALSE
    /\ realHasReturnedSome = FALSE
    /\ populate = "Pending"
    /\ warnEmitted = FALSE

----------------------------------------------------------------------------
(* HeadStepHasReturnedSome: the slow store's has(key) returned                *)
(* Ok(Some(_)). Real evidence the blob exists. Both bug and fix flip          *)
(* the flag here.                                                             *)
(* Models fast_slow_store.rs:944-970.                                         *)
----------------------------------------------------------------------------
HeadStepHasReturnedSome ==
    /\ head = "Pending"
    /\ head' = "HasReturnedSome"
    /\ realHasReturnedSome' = TRUE
    /\ flagSetByHead' = TRUE
    /\ UNCHANGED <<populate, warnEmitted>>

----------------------------------------------------------------------------
(* HeadStepHasReturnedNone: the slow store's has(key) returned                *)
(* Ok(None). Genuine NotFound at the head step; populate never runs.          *)
(* Models fast_slow_store.rs:950-965 (the ok_or_else NotFound arm).           *)
----------------------------------------------------------------------------
HeadStepHasReturnedNone ==
    /\ head = "Pending"
    /\ head' = "HasReturnedNone"
    /\ realHasReturnedSome' = FALSE
    /\ flagSetByHead' = FALSE
    /\ populate' = "Skipped"   \* the head Err short-circuits before populate
    /\ UNCHANGED <<warnEmitted>>

----------------------------------------------------------------------------
(* HeadStepLazySkip: LazyExistenceOnSync optimisation skipped has().         *)
(* Models fast_slow_store.rs:933-943.                                         *)
(*                                                                            *)
(* BUGGED variant (BugMode=TRUE):                                            *)
(*   `head_was_ok` was set TRUE because the head_result was Ok                *)
(*   (Ok(MaxSize(u64::MAX))) — but has() was never called.                   *)
(* FIXED variant (BugMode=FALSE):                                            *)
(*   The new flag `has_actually_returned_some` is set ONLY inside the        *)
(*   real-has-Some arm, so this branch leaves it FALSE.                      *)
----------------------------------------------------------------------------
HeadStepLazySkip ==
    /\ head = "Pending"
    /\ head' = "LazySkip"
    /\ realHasReturnedSome' = FALSE
    /\ flagSetByHead' = BugMode    \* TRUE under bug, FALSE under fix
    /\ UNCHANGED <<populate, warnEmitted>>

----------------------------------------------------------------------------
(* PopulateOk: the populate path succeeded — slow store delivered all bytes. *)
(* Models the data_stream / slow_store_fut / fast_store_fut Ok arm.          *)
(* Only enabled when head produced an Ok result (not None).                   *)
----------------------------------------------------------------------------
PopulateOk ==
    /\ head \in {"HasReturnedSome", "LazySkip"}
    /\ populate = "Pending"
    /\ populate' = "Ok"
    /\ UNCHANGED <<head, flagSetByHead, realHasReturnedSome, warnEmitted>>

----------------------------------------------------------------------------
(* PopulateNotFound: the populate path returned NotFound. Two distinct        *)
(* causes are folded into this single transition:                            *)
(*   * After HasReturnedSome: a genuine has-evict race (the blob              *)
(*     disappeared between has() and get()) — this is the case the           *)
(*     warn was originally designed to surface.                               *)
(*   * After LazySkip: the blob never existed; populate fails to find it.    *)
(*     This is a NORMAL miss, not an invariant violation — but the bug      *)
(*     interprets it as a violation because flagSetByHead is TRUE.           *)
----------------------------------------------------------------------------
PopulateNotFound ==
    /\ head \in {"HasReturnedSome", "LazySkip"}
    /\ populate = "Pending"
    /\ populate' = "NotFound"
    /\ UNCHANGED <<head, flagSetByHead, realHasReturnedSome, warnEmitted>>

----------------------------------------------------------------------------
(* WarnGate: production gate at fast_slow_store.rs:1211-1222.                *)
(*   if has_actually_returned_some {                                          *)
(*       if let Err(err) = &merged {                                         *)
(*           if err.code == Code::NotFound {                                 *)
(*               warn!(... "PHANTOM BLOB ...");                              *)
(*           }                                                                *)
(*       }                                                                    *)
(*   }                                                                        *)
(* Models the warn emission as setting `warnEmitted` to TRUE iff the gate    *)
(* opens. The bug is that the gate uses the wrong flag.                       *)
----------------------------------------------------------------------------
WarnGate ==
    /\ populate = "NotFound"
    /\ warnEmitted = FALSE
    /\ flagSetByHead = TRUE       \* the production gate
    /\ warnEmitted' = TRUE
    /\ UNCHANGED <<head, flagSetByHead, realHasReturnedSome, populate>>

----------------------------------------------------------------------------
(* WarnGateNoFire: explicitly model the case where the gate stays FALSE      *)
(* — populate completed (Ok or NotFound) but the flag is FALSE so no warn.   *)
(* Used to mark the populate-step "done" so the spec doesn't spin.           *)
----------------------------------------------------------------------------
WarnGateNoFire ==
    /\ populate \in {"Ok", "NotFound"}
    /\ warnEmitted = FALSE
    /\ flagSetByHead = FALSE
    /\ warnEmitted' = FALSE       \* no change but record we evaluated
    /\ UNCHANGED <<head, flagSetByHead, realHasReturnedSome, populate>>

----------------------------------------------------------------------------
Next ==
    \/ HeadStepHasReturnedSome
    \/ HeadStepHasReturnedNone
    \/ HeadStepLazySkip
    \/ PopulateOk
    \/ PopulateNotFound
    \/ WarnGate

----------------------------------------------------------------------------
Spec ==
    /\ Init
    /\ [][Next]_vars
    \* No fairness — invariants are pure safety; no liveness claim.

----------------------------------------------------------------------------
(* INVARIANTS                                                                *)
----------------------------------------------------------------------------

TypeOK ==
    /\ head \in HeadStates
    /\ flagSetByHead \in BOOLEAN
    /\ realHasReturnedSome \in BOOLEAN
    /\ populate \in PopulateStates
    /\ warnEmitted \in BOOLEAN

(* SAFETY: WarnFiresOnlyOnGenuineRace                                        *)
(*                                                                            *)
(* The PHANTOM BLOB warn is a high-severity production alert designed to     *)
(* surface a specific invariant violation: slow_store.has() reported Some,   *)
(* and a follow-up populate produced NotFound. The intended interpretation   *)
(* is "the blob disappeared between two consecutive operations" — a real    *)
(* eviction-race that needs investigation.                                    *)
(*                                                                            *)
(* The invariant: if `warnEmitted = TRUE`, the head step MUST have returned  *)
(* HasReturnedSome (i.e. `realHasReturnedSome = TRUE`).                       *)
(*                                                                            *)
(* Under BugMode = TRUE the flag is also set by LazySkip, so the gate fires *)
(* even when realHasReturnedSome is FALSE -> invariant violated. Under       *)
(* BugMode = FALSE the flag is set only by HasReturnedSome, so warn implies *)
(* realHasReturnedSome -> invariant holds.                                    *)
WarnFiresOnlyOnGenuineRace ==
    warnEmitted => realHasReturnedSome

(* SAFETY: GateFlagMatchesHasOutcome                                         *)
(*                                                                            *)
(* Direct expression of the conflation: the flag's semantics MUST be        *)
(* "has() returned Some". After the head step completes, the flag is true    *)
(* iff `realHasReturnedSome` is true. The bug breaks this iff equality.      *)
(* (Holds vacuously while head = "Pending"; we only check after head fires.) *)
GateFlagMatchesHasOutcome ==
    head = "Pending"
    \/ flagSetByHead = realHasReturnedSome

============================================================================
