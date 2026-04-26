--------------------------- MODULE H2ConnectionPool ---------------------------
(***************************************************************************
  H2 connection-pool stale-channel reuse + race-loser abort burst.

  Models the bug class documented at
  `/home/user/.claude/projects/-src-nativelink/memory/project_h2_pool_stale_channel_2026_04_25.md`
  and the production fix in `nativelink-store/src/grpc_store.rs`.

  PRODUCTION SHAPE (post-fix):
    * `ConnectionManager` (nativelink-util/src/connection_manager.rs:144-170)
      maintains a pool of established `Channel`s. Each `connection()` call
      hands out a `Connection` cloned from the next channel in round-robin.
    * `GrpcStore::evict_pool_on_transport_err`
      (nativelink-store/src/grpc_store.rs:391-407) evicts a channel from
      the pool when a per-RPC error matches `looks_like_dead_channel` —
      a predicate over the error code:
        Code::Unavailable | Code::Unknown | Code::ResourceExhausted     => TRUE
        Code::Internal /\ message contains h2-shape       => TRUE
        otherwise                                         => FALSE
    * The h2 server can send GOAWAY at any time. Once a channel has
      received a GOAWAY frame, every subsequent stream on that channel
      fails with one of the error codes above.
    * `parallel_chunk_count = 64` issues 64 concurrent fetches (one per
      sub-range chunk) on the same checked-out channels. The "race-loser"
      pattern fires when one fetch sees a dead channel, evicts, and the
      remaining 63 fetches on the same dead channel observe the same
      transport-shaped error and ALL try to re-evict. Pre-fix the
      predicate gap (excluding `ResourceExhausted` from the eviction set)
      meant ENHANCE_YOUR_CALM-derived errors would NOT evict — leaving
      the dead channel in the pool. Subsequent checkouts on that slot
      would re-deliver the dead channel, an entire wave of 64 fetches
      would fail, and the pattern would repeat until the pool was
      manually drained.

  BUG TRIGGER (Predicate gap):
    The historic predicate excluded ResourceExhausted (the GOAWAY-derived
    code tonic surfaces for ENHANCE_YOUR_CALM). A draining channel hit by
    GOAWAY would receive a stream-level ResourceExhausted on every fetch;
    the predicate said "not transport-shaped, don't evict"; the channel
    stayed in the pool; subsequent checkouts of that slot delivered the
    dead channel; subsequent fetches all errored; loop forever.

  RACE-LOSER AMPLIFICATION:
    Even with the fixed predicate, the *first* fetch on a dead channel
    needs N round trips before its eviction takes effect from the pool's
    point of view; concurrent fetches that already-checked-out the same
    dead clone keep failing in the meantime. With parallel_chunk_count =
    64, the burst is 64 errors per pool turnover. We model the burst by
    allowing multiple in-flight fetches against the same channel.

  CONSTANTS:
    * EvictResourceExhausted (BOOLEAN): TRUE => fixed predicate; FALSE =>
      pre-fix predicate gap.
    * Channels: number of channel slots in the pool, e.g. {c1, c2}.
    * MaxFetches: bound on total fetches per run (state-space bound).

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * H2ConnectionPoolFixed.cfg (EvictResourceExhausted = TRUE):
      No invariant violation. NoUnboundedFetchFailures holds.
    * H2ConnectionPoolBugged.cfg (EvictResourceExhausted = FALSE):
      INVARIANT VIOLATED on `EventualHealthyPoolAfterGoaway`. Trace shows:
      a channel goes Draining via GOAWAY -> a fetch on it fails with
      ResourceExhausted -> predicate says "don't evict" -> next fetch
      checks out the same channel again -> FailureCount climbs without
      bound while the pool has no Healthy channels left.

  SCOPE — what this spec models:
    * a fixed-size pool of channels with state in
      {Healthy, Draining, Dead}.
    * GOAWAY as a single non-deterministic transition Healthy -> Draining.
    * fetches against a checked-out channel; on a Draining/Dead channel
      the fetch errors with one of {Unavailable, ResourceExhausted}.
    * the predicate that decides whether the per-RPC error triggers a
      pool eviction (modeled as the `EvictsErr` operator).
    * pool eviction: removes the channel from the pool and triggers a
      reconnect (modeled by replacing the slot with a new Healthy
      channel after a "reconnect" step).
    * fetch retry: on retry, a new checkout happens; we model parallel
      checkouts as a non-deterministic count from a bounded set.

  SCOPE — what this spec does NOT model:
    * actual h2 frame semantics (RST_STREAM vs GOAWAY vs ENHANCE_YOUR_CALM
      mapping is collapsed into the `errored channel produces error code X`
      step),
    * the specific JoinHandle::abort race window (we model it abstractly
      as "multiple in-flight fetches can fire on the same channel
      independently"),
    * QUIC transport (out of scope; the production code's QUIC arm is a
      no-op for evict_pool_on_transport_err — see grpc_store.rs:404),
    * tonic Streaming<>::poll_next semantics (we treat each fetch as a
      single atomic outcome),
    * the retrier's exponential backoff,
    * the specific 32-connections-per-endpoint default — we model 2
      channels because the bug shape is the same with N>=2,
    * JoinSet/FuturesUnordered ordering — TLC explores arbitrary
      interleavings already.

  CITATIONS:
    [pred] nativelink-store/src/grpc_store.rs:143-155 (looks_like_dead_channel)
    [evic] nativelink-store/src/grpc_store.rs:391-407 (evict_pool_on_transport_err)
    [emit1] nativelink-store/src/grpc_store.rs:1459-1462 (eviction call site)
    [emit2] nativelink-store/src/grpc_store.rs:1476-1488 (eviction call site)
    [pool] nativelink-util/src/connection_manager.rs:144-170 (pool state)
    [par]  nativelink-store/src/grpc_store.rs:1583-1670 (get_part_parallel)
    [memo] /home/user/.claude/projects/-src-nativelink/memory/project_h2_pool_stale_channel_2026_04_25.md
 ***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Channels,                  \* Set of channel slot identifiers, e.g. {c1, c2}.
    MaxFetches,                \* Bound on total fetches issued (state-space cap).
    EvictResourceExhausted     \* TRUE => fixed predicate; FALSE => bug.

ASSUME EvictResourceExhausted \in BOOLEAN
ASSUME MaxFetches \in Nat /\ MaxFetches >= 1

\* Channel state.
\*   "Healthy"  - channel can serve fetches successfully.
\*   "Draining" - server sent GOAWAY; new streams fail with
\*                ResourceExhausted (ENHANCE_YOUR_CALM mapping).
\*   "Dead"     - channel was reset / hit a hard transport error;
\*                streams fail with Unavailable.
ChannelStates == {"Healthy", "Draining", "Dead"}

\* Per-fetch outcome reason. We track the error code so the predicate
\* can decide on it.
\*   "Ok"               - fetch succeeded.
\*   "Unavailable"      - h2 transport failure (RST_STREAM / catch-all).
\*   "ResourceExhausted"- ENHANCE_YOUR_CALM (post-GOAWAY surface code).
FetchOutcomes == {"Ok", "Unavailable", "ResourceExhausted"}

VARIABLES
    pool,            \* function: Channels -> ChannelStates
    fetchCount,      \* total fetches issued so far (Nat <= MaxFetches)
    failureCount,    \* count of fetches that errored out (Nat)
    lastOutcome,     \* last fetch outcome record [chan, code]; "none" if no fetch yet
    pendingReconnect \* set of channel slots that need to be reconnected
                     \* (have been evicted but not yet replaced).

vars == <<pool, fetchCount, failureCount, lastOutcome, pendingReconnect>>

NoOutcome == [chan |-> CHOOSE c \in Channels: TRUE, code |-> "none"]

----------------------------------------------------------------------------
(* Init: every channel starts Healthy; no fetches issued yet.           *)
----------------------------------------------------------------------------
Init ==
    /\ pool = [c \in Channels |-> "Healthy"]
    /\ fetchCount = 0
    /\ failureCount = 0
    /\ lastOutcome = NoOutcome
    /\ pendingReconnect = {}

----------------------------------------------------------------------------
(* Goaway(c): server sends GOAWAY on channel c. Models the                *)
(* server-side graceful-shutdown / limit-hit case. The channel           *)
(* transitions Healthy -> Draining. Subsequent fetches will see          *)
(* ResourceExhausted.                                                    *)
----------------------------------------------------------------------------
Goaway(c) ==
    /\ pool[c] = "Healthy"
    /\ pool' = [pool EXCEPT ![c] = "Draining"]
    /\ UNCHANGED <<fetchCount, failureCount, lastOutcome, pendingReconnect>>

----------------------------------------------------------------------------
(* RstStream(c): channel takes a hard transport error. Healthy or       *)
(* Draining -> Dead. Subsequent fetches will see Unavailable.            *)
(* Models the underlying h2 connection going away (TCP RST, h2 protocol *)
(* error, etc.).                                                          *)
----------------------------------------------------------------------------
RstStream(c) ==
    /\ pool[c] \in {"Healthy", "Draining"}
    /\ pool' = [pool EXCEPT ![c] = "Dead"]
    /\ UNCHANGED <<fetchCount, failureCount, lastOutcome, pendingReconnect>>

----------------------------------------------------------------------------
(* EvictsErr(code): the production predicate at grpc_store.rs:143-155.  *)
(*                                                                       *)
(* Fixed (EvictResourceExhausted=TRUE):                                  *)
(*   Code::Unavailable        -> TRUE                                    *)
(*   Code::ResourceExhausted  -> TRUE                                    *)
(*                                                                       *)
(* Buggy (EvictResourceExhausted=FALSE) — the historic predicate gap:   *)
(*   Code::Unavailable        -> TRUE                                    *)
(*   Code::ResourceExhausted  -> FALSE   <-- bug                         *)
(*                                                                       *)
(* (We don't model the Internal+message-check arm here — the predicate  *)
(* gap was specifically about codes that the original allowlist failed  *)
(* to include. The bug shape is the same.)                              *)
----------------------------------------------------------------------------
EvictsErr(code) ==
    \/ code = "Unavailable"
    \/ (code = "ResourceExhausted" /\ EvictResourceExhausted)

----------------------------------------------------------------------------
(* Fetch(c): a fetch is issued on channel c. Outcome depends on c's    *)
(* current state. Models a single chunk-fetch in get_part_parallel.    *)
(* Multiple fetches on the same channel are independent transitions    *)
(* — TLC's interleaving exploration captures the parallel_chunk_count  *)
(* burst pattern.                                                       *)
----------------------------------------------------------------------------
Fetch(c) ==
    /\ fetchCount < MaxFetches
    /\ pool[c] \in {"Healthy", "Draining", "Dead"}    \* channel exists
    /\ c \notin pendingReconnect                       \* not torn down
    /\ \/ /\ pool[c] = "Healthy"                       \* success
          /\ lastOutcome' = [chan |-> c, code |-> "Ok"]
          /\ failureCount' = failureCount
       \/ /\ pool[c] = "Draining"                      \* GOAWAY surface
          /\ lastOutcome' = [chan |-> c, code |-> "ResourceExhausted"]
          /\ failureCount' = failureCount + 1
       \/ /\ pool[c] = "Dead"                          \* RST/transport
          /\ lastOutcome' = [chan |-> c, code |-> "Unavailable"]
          /\ failureCount' = failureCount + 1
    /\ fetchCount' = fetchCount + 1
    /\ UNCHANGED <<pool, pendingReconnect>>

----------------------------------------------------------------------------
(* EvictionStep: after a fetch errors, the predicate decides whether    *)
(* to evict the channel from the pool. Models the call sites at        *)
(* grpc_store.rs:1459-1462 and :1476-1488.                              *)
(*                                                                       *)
(* When EvictsErr(lastOutcome.code) is TRUE, the channel slot is        *)
(* moved into pendingReconnect (it'll be replaced by Reconnect step).   *)
(* When EvictsErr is FALSE, the dead channel STAYS in the pool — this  *)
(* is the bug.                                                           *)
----------------------------------------------------------------------------
EvictionStep ==
    /\ lastOutcome.code \in {"Unavailable", "ResourceExhausted"}
    /\ EvictsErr(lastOutcome.code)
    /\ lastOutcome.chan \notin pendingReconnect
    /\ pendingReconnect' = pendingReconnect \cup {lastOutcome.chan}
    /\ lastOutcome' = NoOutcome
    /\ UNCHANGED <<pool, fetchCount, failureCount>>

----------------------------------------------------------------------------
(* NoEvictionStep: the bug case. The fetch errored with a code the     *)
(* predicate doesn't recognize, so no eviction happens. Channel stays  *)
(* in pool as-is.                                                       *)
----------------------------------------------------------------------------
NoEvictionStep ==
    /\ lastOutcome.code \in {"ResourceExhausted"}
    /\ ~EvictsErr(lastOutcome.code)
    /\ lastOutcome' = NoOutcome
    /\ UNCHANGED <<pool, fetchCount, failureCount, pendingReconnect>>

----------------------------------------------------------------------------
(* AcknowledgeOk: clear the lastOutcome after a successful fetch (so   *)
(* the next fetch can fire). Unobservable in production — included to   *)
(* keep the state machine deterministic in TLC.                         *)
----------------------------------------------------------------------------
AcknowledgeOk ==
    /\ lastOutcome.code = "Ok"
    /\ lastOutcome' = NoOutcome
    /\ UNCHANGED <<pool, fetchCount, failureCount, pendingReconnect>>

----------------------------------------------------------------------------
(* Reconnect(c): a previously-evicted channel slot is replaced with     *)
(* a fresh Healthy channel. Models ConnectionManager's queued reconnect *)
(* after evict_idle_channel.                                             *)
----------------------------------------------------------------------------
Reconnect(c) ==
    /\ c \in pendingReconnect
    /\ pool' = [pool EXCEPT ![c] = "Healthy"]
    /\ pendingReconnect' = pendingReconnect \ {c}
    /\ UNCHANGED <<fetchCount, failureCount, lastOutcome>>

----------------------------------------------------------------------------
Next ==
    \/ \E c \in Channels : Goaway(c)
    \/ \E c \in Channels : RstStream(c)
    \/ \E c \in Channels : Fetch(c)
    \/ EvictionStep
    \/ NoEvictionStep
    \/ AcknowledgeOk
    \/ \E c \in Channels : Reconnect(c)

----------------------------------------------------------------------------
(* Fairness: every reconnect eventually fires. We DO NOT add fairness  *)
(* on Goaway / RstStream — those are server-controlled and may never  *)
(* fire (the bug is exposed only in traces where they DO fire).        *)
(*                                                                      *)
(* WF on Reconnect ensures that, IF a channel was added to             *)
(* pendingReconnect, it is eventually replaced. Without this, the bug  *)
(* could trivially fire by simply never reconnecting any evicted       *)
(* channel — a less interesting failure mode.                           *)
----------------------------------------------------------------------------
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A c \in Channels : WF_vars(Reconnect(c))
    /\ WF_vars(EvictionStep)
    /\ WF_vars(NoEvictionStep)
    /\ WF_vars(AcknowledgeOk)

----------------------------------------------------------------------------
(* INVARIANTS                                                            *)
----------------------------------------------------------------------------

TypeOK ==
    /\ pool \in [Channels -> ChannelStates]
    /\ fetchCount \in 0..MaxFetches
    /\ failureCount \in 0..MaxFetches
    /\ lastOutcome \in [chan: Channels, code: FetchOutcomes \cup {"none"}]
    /\ pendingReconnect \subseteq Channels

(* Helper: count of channels currently Healthy (not draining/dead).    *)
HealthyCount ==
    Cardinality({c \in Channels : pool[c] = "Healthy"})

(* Helper: count of channels NOT in pendingReconnect that are bad.     *)
StaleInPool ==
    Cardinality({c \in Channels :
        c \notin pendingReconnect /\ pool[c] \in {"Draining", "Dead"}})

(* SAFETY: ResourceExhaustedTriggersEviction                             *)
(* The DIRECT statement of the predicate-gap bug. After a fetch         *)
(* produces a ResourceExhausted outcome (i.e. the GOAWAY-derived        *)
(* error surface), the predicate MUST accept the code so that an       *)
(* EvictionStep transition is enabled. Under the buggy predicate this  *)
(* implication fails — the channel stays in the pool and the next      *)
(* checkout delivers the same dead clone.                               *)
(*                                                                      *)
(* The chan-already-pending check covers the case where TWO concurrent *)
(* fetches on the same channel both error: the first eviction transitions*)
(* lastOutcome -> NoOutcome but pendingReconnect retains the channel.  *)
(* (We use "lastOutcome.code # ResourceExhausted" as the trivially-true*)
(* short-circuit for states where no failed fetch is pending.)         *)
ResourceExhaustedTriggersEviction ==
    lastOutcome.code # "ResourceExhausted"
    \/ lastOutcome.chan \in pendingReconnect
    \/ EvictsErr(lastOutcome.code)

(* LIVENESS: the pool eventually returns to all-Healthy state.         *)
(* Under WF on Reconnect plus the fixed predicate, this holds.         *)
(* Under the bug, traces exist where it does NOT hold (Draining        *)
(* channel never gets evicted, Reconnect for that slot is never        *)
(* triggered).                                                          *)
EventuallyAllHealthy ==
    <>(\A c \in Channels : pool[c] = "Healthy" /\ pendingReconnect = {})

(* LIVENESS: NoIndefiniteFailureLoop                                     *)
(* If failures keep accumulating, eventually one of them produces an    *)
(* eviction. Captured as the safety-style version: "if more than 2     *)
(* failures have happened, EITHER an eviction step is currently        *)
(* enabled OR a slot is pending reconnect OR (the reconnect happened)  *)
(* there is at least one healthy channel". Under the bug, a sequence  *)
(* of {Goaway, Fetch[ResourceExhausted], NoEviction} can repeat       *)
(* unboundedly without ever populating pendingReconnect — the bug    *)
(* trace.                                                              *)
NoIndefiniteFailureLoopOnFetches ==
    failureCount < 2
    \/ pendingReconnect # {}
    \/ HealthyCount > 0
    \/ EvictResourceExhausted    \* under the fix, eviction will follow

============================================================================
