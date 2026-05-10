--------------------- MODULE BlobsAvailableBookkeeping ---------------------
(***************************************************************************
  #99 fix-up-2 — bookkeeping invariant for the BlobsAvailable accumulator.

  CONTRACT WE'RE MODELING:
    The per-connection accumulator stores N in-flight broadcasts in a
    HashMap and maintains a SINGLE counter `total_accumulated` that
    must equal the sum of `accumulated_entries` across the live
    broadcasts (the per-conn entries cap at line 506 fires when this
    counter exceeds MAX_ACCUMULATED_ENTRIES_PER_CONN). The counter is
    not derived; it is a write-each-time scalar that must stay in
    lock-step with the HashMap. A drift makes the gate decisions
    diverge from reality.

    The fix-up-2 fix changes ONE line in the success branch of
    `merge_chunk` (`accumulator.rs:725`):

       PRE-FIX (buggy):  inner.total_accumulated = prev_total.saturating_add(delta);
       POST-FIX:         inner.total_accumulated = inner.total_accumulated.saturating_add(delta);

    The pre-fix line writes back a counter snapshot taken BEFORE the
    token-mismatch wipe at line 549-550. So any in-flight wipe (token
    mismatch, validation Err which removes-then-subtracts, or
    pre-merge cap-check which removes-then-subtracts) is silently
    overwritten by the success path's write-back of the stale
    snapshot. The post-fix line increments the CURRENT counter,
    preserving every wipe.

    This spec exhibits the drift on the `TokenMismatchRebuild` action,
    the canonical site where a wipe interleaves with a successful
    merge in the same `merge_chunk` invocation.

  CONSTANTS:
    * NumBroadcasts (Nat >= 1): how many distinct broadcast_ids may be
      in-flight concurrently. Bound by MAX_INFLIGHT_BROADCASTS_PER_CONN
      in the implementation but kept small (2-3) here for tractable
      state space.
    * MaxEntriesPerChunk (Nat >= 1): bound on the per-chunk entry
      count. Kept small (2-3) for state-space tractability.
    * BugMode (BOOLEAN):
        - TRUE  => merge-success uses prev_total + delta (the bug).
        - FALSE => merge-success uses inner.total_accumulated + delta
                   (the post-fix-up-2 fix).

  INVARIANT:
    TotalAccumulatedConsistency -- at every reachable state,
      totalAccumulated = SUM over (b in DOMAIN broadcasts) of
                          broadcasts[b].accumulatedEntries.

    Pre-fix: TLC produces a counterexample on TokenMismatchRebuild.
    Post-fix: invariant holds for all reachable states (every action
              writes through CURRENT counter, never a stale snapshot).

    Additionally:
      ErrBranchPreservesOtherBroadcasts -- when a MergeChunkErr fires
        on broadcast `b`, broadcasts `b' /= b`'s accumulatedEntries
        contributions remain intact in totalAccumulated.

  Modeled after specs/BlobsAvailableChunking.tla; this is the
  COMPANION spec covering the per-connection bookkeeping invariant
  that the parent spec does not model (the parent is
  per-broadcast sequence-completeness; this is per-conn cap-counter
  consistency).
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    NumBroadcasts,
    MaxEntriesPerChunk,
    MaxEntriesPerBroadcast,  \* state-bound to make TLC terminate.
    BugMode

ASSUME NumBroadcastsOK         == NumBroadcasts \in Nat /\ NumBroadcasts >= 1
ASSUME MaxEntriesPerChunkOK    == MaxEntriesPerChunk \in Nat /\ MaxEntriesPerChunk >= 1
ASSUME MaxEntriesPerBroadcastOK == MaxEntriesPerBroadcast \in Nat
                                   /\ MaxEntriesPerBroadcast >= MaxEntriesPerChunk
ASSUME BugModeOK               == BugMode \in BOOLEAN

\* Broadcast IDs are 1..NumBroadcasts. Token domain is small (we only
\* need >=2 distinct values to exercise mismatch).
BroadcastIds == 1..NumBroadcasts
Tokens == {1, 2}                  \* "old" and "new" tokens
ChunkSizes == 1..MaxEntriesPerChunk

\* `broadcasts` is a partial function from BroadcastId -> [token, accumulatedEntries].
\* "Not in DOMAIN" = no live accumulator for that broadcast_id. We model
\* the implementation's HashMap.
EmptyBroadcasts == [b \in {} |-> [token |-> 0, accumulatedEntries |-> 0]]

VARIABLES
    broadcasts,        \* partial function BroadcastId -> [token, accumulatedEntries]
    totalAccumulated   \* the implementation's `inner.total_accumulated`

vars == <<broadcasts, totalAccumulated>>

\* Sum of accumulatedEntries over all live broadcasts. This is the
\* GROUND TRUTH the implementation's totalAccumulated counter must
\* match. Implemented via a recursive helper because TLC has no
\* built-in fold.
RECURSIVE SumSet(_)
SumSet(S) == IF S = {} THEN 0
             ELSE LET x == CHOOSE y \in S : TRUE
                  IN broadcasts[x].accumulatedEntries + SumSet(S \ {x})

SumOverBroadcasts == SumSet(DOMAIN broadcasts)

TypeOK ==
    /\ DOMAIN broadcasts \subseteq BroadcastIds
    /\ \A b \in DOMAIN broadcasts :
         /\ broadcasts[b].token \in Tokens
         /\ broadcasts[b].accumulatedEntries \in 0..MaxEntriesPerBroadcast
    /\ totalAccumulated \in 0..(NumBroadcasts * MaxEntriesPerBroadcast * 2)

Init ==
    /\ broadcasts = EmptyBroadcasts
    /\ totalAccumulated = 0

\* Helper: insert/replace a broadcast entry.
WithBroadcast(bs, b, token, entries) ==
    [x \in (DOMAIN bs) \cup {b} |->
        IF x = b THEN [token |-> token, accumulatedEntries |-> entries]
                 ELSE bs[x]]

\* Helper: remove a broadcast entry.
WithoutBroadcast(bs, b) ==
    [x \in (DOMAIN bs) \ {b} |-> bs[x]]

\* ===========================================================================
\* MergeChunkSuccess: a fresh chunk arrives for broadcast `b` with token
\* `token` and `chunkEntries` entries; merge succeeds (no validation Err).
\*
\* Three sub-cases that exercise the bookkeeping invariant:
\*
\*   (A) New broadcast: b not in DOMAIN broadcasts. Insert entry with
\*       accumulatedEntries = chunkEntries. totalAccumulated += chunkEntries.
\*
\*   (B) Same-token continuation: b in DOMAIN, broadcasts[b].token = token.
\*       broadcasts[b].accumulatedEntries += chunkEntries.
\*       totalAccumulated += chunkEntries.
\*
\*   (C) TOKEN-MISMATCH REBUILD: b in DOMAIN, broadcasts[b].token /= token.
\*       Wipe (token-mismatch path at line 647-650): subtract the stale
\*       broadcast's accumulatedEntries from totalAccumulated, replace
\*       broadcasts[b] with a fresh accumulator (accumulatedEntries = 0).
\*       Then merge: broadcasts[b].accumulatedEntries := chunkEntries.
\*       Counter update:
\*         BugMode=TRUE  : totalAccumulated := prev_total + chunkEntries
\*                          (where prev_total = totalAccumulated BEFORE the wipe).
\*                          The wipe is silently overwritten.
\*         BugMode=FALSE : totalAccumulated := totalAccumulated + chunkEntries
\*                          (post-wipe value), preserving the wipe.
\*
\* The chunkEntries delta IS the implementation's `delta = new_entries -
\* prev_entries` since for a freshly-inserted accumulator prev_entries=0.
\* ===========================================================================

\* Per-broadcast entry cap modeled as a precondition. Mirrors the
\* implementation's pre-merge cap-check at accumulator.rs:606. Without
\* this cap as a precondition the SameToken/New actions would allow
\* unbounded growth and trip TypeOK on a violation that ISN'T the
\* bookkeeping bug.
WithinPerBroadcastCap(currentEntries, chunkEntries) ==
    currentEntries + chunkEntries <= MaxEntriesPerBroadcast

MergeChunkSuccessNew(b, token, chunkEntries) ==
    /\ b \notin DOMAIN broadcasts
    /\ chunkEntries \in ChunkSizes
    /\ WithinPerBroadcastCap(0, chunkEntries)
    /\ broadcasts' = WithBroadcast(broadcasts, b, token, chunkEntries)
    /\ totalAccumulated' = totalAccumulated + chunkEntries

MergeChunkSuccessSameToken(b, token, chunkEntries) ==
    /\ b \in DOMAIN broadcasts
    /\ broadcasts[b].token = token
    /\ chunkEntries \in ChunkSizes
    /\ WithinPerBroadcastCap(broadcasts[b].accumulatedEntries, chunkEntries)
    /\ broadcasts' = WithBroadcast(broadcasts, b, token,
                                   broadcasts[b].accumulatedEntries + chunkEntries)
    \* Both Bug and Fix produce the same answer here because there's no wipe
    \* between the prev_total snapshot and the write-back.
    /\ totalAccumulated' = totalAccumulated + chunkEntries

MergeChunkSuccessTokenMismatchRebuild(b, token, chunkEntries) ==
    /\ b \in DOMAIN broadcasts
    /\ broadcasts[b].token /= token
    /\ chunkEntries \in ChunkSizes
    /\ WithinPerBroadcastCap(0, chunkEntries)  \* fresh accumulator after wipe
    \* Snapshot of totalAccumulated taken at line 604 (`let prev_total =
    \* inner.total_accumulated;`) BEFORE the token-mismatch wipe at line
    \* 647-650.
    /\ LET prevTotal == totalAccumulated
           wipedTotal == totalAccumulated -
                         broadcasts[b].accumulatedEntries
       IN /\ broadcasts' = WithBroadcast(broadcasts, b, token, chunkEntries)
          /\ totalAccumulated' =
                IF BugMode
                \* Pre-fix: writes back prev_total + delta. Wipe lost.
                THEN prevTotal + chunkEntries
                \* Post-fix: writes back current_total + delta. Wipe preserved.
                ELSE wipedTotal + chunkEntries

MergeChunkSuccess(b, token, chunkEntries) ==
    \/ MergeChunkSuccessNew(b, token, chunkEntries)
    \/ MergeChunkSuccessSameToken(b, token, chunkEntries)
    \/ MergeChunkSuccessTokenMismatchRebuild(b, token, chunkEntries)

\* ===========================================================================
\* MergeChunkErr: simulates the production path where merge_chunk's
\* `merge_result` returns Err. Two real triggers:
\*
\*   (i)  duplicate sequence — broadcasts[b] retained; failing chunk
\*        not absorbed; subtract acc.accumulatedEntries (the prior
\*        successful sum) and remove broadcasts[b]. Net effect: the
\*        broadcast is dropped wholesale; its prior contributions are
\*        cleanly removed from totalAccumulated.
\*
\*   (ii) token mismatch + then validation Err on the *first* merged
\*        chunk — production-impossible because token-mismatch already
\*        rebuilt the accumulator with new=BroadcastAccumulator::new(...)
\*        and the failing chunk is the one that triggered the rebuild,
\*        which then succeeds (rebuild is the "fix" for the mismatch).
\*        We don't model this case (would require coupling that the
\*        production code structurally avoids).
\*
\* The invariant the SAFE-DOCUMENTED claim needs to preserve: when
\* MergeChunkErr fires for broadcast b, OTHER broadcasts (b' /= b)
\* survive intact — totalAccumulated is reduced by exactly
\* broadcasts[b].accumulatedEntries, not more.
\* ===========================================================================

MergeChunkErr(b) ==
    /\ b \in DOMAIN broadcasts
    \* Per the doc-comment audit at accumulator.rs:692-712:
    \*   "merge() Err returns at lines 196-206 all precede the
    \*    accumulated_entries mutation at line 250."
    \* So when merge() returns Err, broadcasts[b].accumulatedEntries
    \* equals its pre-call value (no in-flight increment). The Err
    \* handler removes broadcasts[b] from the map AND subtracts the
    \* (unchanged) acc.accumulatedEntries from totalAccumulated.
    /\ LET removed == broadcasts[b].accumulatedEntries
       IN /\ broadcasts' = WithoutBroadcast(broadcasts, b)
          /\ totalAccumulated' = totalAccumulated - removed

\* ===========================================================================
\* TerminalCommit: the success-branch terminal commit at lines 731-736.
\* Removes broadcasts[b] from the map AND subtracts removed.accumulated_entries
\* from totalAccumulated. Modeled separately from MergeChunkSuccess because
\* the terminal commit is structurally distinct (drains the accumulator).
\*
\* For modelling tractability, we treat "terminal" as an action the
\* environment may invoke at any time on a live broadcast. Real
\* production fires it only when is_last=true; the bookkeeping invariant
\* we're checking is independent of when the terminal arrives (all paths
\* must keep totalAccumulated == sum-over-broadcasts).
\* ===========================================================================

TerminalCommit(b) ==
    /\ b \in DOMAIN broadcasts
    /\ LET removed == broadcasts[b].accumulatedEntries
       IN /\ broadcasts' = WithoutBroadcast(broadcasts, b)
          /\ totalAccumulated' = totalAccumulated - removed

\* ===========================================================================
\* DropAllInflight: connection drop / disconnect. Clears the HashMap and
\* zeroes totalAccumulated. (Production: drop_all_inflight at line 813-817.)
\* ===========================================================================

DropAllInflight ==
    /\ broadcasts' = EmptyBroadcasts
    /\ totalAccumulated' = 0

Next ==
    \/ \E b \in BroadcastIds, t \in Tokens, sz \in ChunkSizes :
         MergeChunkSuccess(b, t, sz)
    \/ \E b \in BroadcastIds : MergeChunkErr(b)
    \/ \E b \in BroadcastIds : TerminalCommit(b)
    \/ DropAllInflight

Spec == Init /\ [][Next]_vars

\* State-space constraint: bound TLC's exploration by the entry-count
\* upper bounds. Without this the SameToken/New actions can grow
\* accumulatedEntries unboundedly; with TypeOK as an invariant TLC
\* would (correctly) trip on a "Type" violation that is NOT the
\* bookkeeping bug we're hunting. The constraint keeps TLC focused on
\* the interleavings of wipes + commits that actually exercise the
\* invariant.
StateConstraint ==
    /\ \A b \in DOMAIN broadcasts :
         broadcasts[b].accumulatedEntries <= MaxEntriesPerBroadcast
    /\ totalAccumulated <= (NumBroadcasts * MaxEntriesPerBroadcast * 2)

\* ===========================================================================
\* INVARIANTS
\* ===========================================================================

\* The load-bearing invariant for the per-conn cap. If totalAccumulated
\* drifts, the gate at accumulator.rs:506-507 fires on the wrong side of
\* reality — premature reject (bug presence) or under-protection (wrong
\* direction; not modeled here because the production-code direction is
\* always over-count).
TotalAccumulatedConsistency ==
    totalAccumulated = SumOverBroadcasts

\* Per the SAFE-DOCUMENTED claim at accumulator.rs:692-712: when
\* MergeChunkErr fires on broadcast b, broadcasts b' /= b's accumulated
\* contributions to totalAccumulated remain intact. This is implied by
\* TotalAccumulatedConsistency (if the running invariant holds across
\* every Err transition then the Err did not over-subtract by definition).
\* Stated separately for documentation and to make a future Err-branch
\* mutation (e.g. subtracting `inner.total_accumulated` instead of the
\* per-acc value) trip a NAMED check.
ErrBranchPreservesOtherBroadcasts ==
    TotalAccumulatedConsistency

\* ===========================================================================
\* TLC outcomes:
\*   BlobsAvailableBookkeepingFixed.cfg (BugMode=FALSE) -- INVARIANTS HOLD.
\*   BlobsAvailableBookkeepingBugged.cfg (BugMode=TRUE) -- COUNTEREXAMPLE on
\*     TotalAccumulatedConsistency: a TokenMismatchRebuild leaves
\*     totalAccumulated > SumOverBroadcasts.
\* ===========================================================================
============================================================================
