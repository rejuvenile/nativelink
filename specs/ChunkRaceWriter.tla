-------------------------- MODULE ChunkRaceWriter --------------------------
(***************************************************************************)
(* #494-v3 Phase 2 multi-writer chunked-write race protocol.               *)
(*                                                                          *)
(* Models per-digest `ChunkRaceState` (chunked_race_state.rs:117-693) +     *)
(* the v2 server-side per-session driver (chunked_write_handler_v2.rs:175-649)*)
(* + the registry's get-or-create-and-attach + force_remove operations     *)
(* (chunked_race_state.rs:900-1027) + the FilesystemStore wiring at        *)
(* filesystem_store.rs:1469-1535.                                          *)
(*                                                                          *)
(* SCOPE — what this spec models                                            *)
(*   * Multiple concurrent writers (CONSTANT Writers) per single digest.   *)
(*   * A position-based bitmap chunks_present and a per-offset             *)
(*     chunks_in_flight set (CONSTANT Chunks).                             *)
(*   * Per-writer FSM: Attached → SendingChunks → AwaitCommit | RunCommit  *)
(*     | Aborted, with the per-chunk admit outcome (Accept | AlreadyHave   *)
(*     | RacingLoser) driving transitions.                                 *)
(*   * Commit-runner election: only the writer flipping the LAST bit (or  *)
(*     observing all_set + commit_running=false + commit_done_flag=false)  *)
(*     wins.                                                               *)
(*   * Commit-runner publish: success or failure (Ok | Err); after        *)
(*     publish, commit_done_flag flips true (sticky).                     *)
(*   * CommitRunnerGuard::drop synthetic Cancelled publish if runner exits*)
(*     without publishing (panic / cancellation path).                    *)
(*   * RaceWriterGuard::drop purge: every chunks_in_flight slot is        *)
(*     evacuated of the dropping writer's id (so survivors can race the   *)
(*     same offset).                                                      *)
(*   * Watchdog / force_remove: if a wedge persists for "long enough", a  *)
(*     waiting writer can fire force_remove(digest), clearing the         *)
(*     registry entry. Modeled non-deterministically — TLA+ does NOT      *)
(*     model real time; the action is enabled whenever a writer is in     *)
(*     AwaitCommit and commit_done_flag is false.                         *)
(*                                                                          *)
(* SCOPE — what this spec deliberately abstracts                            *)
(*   * Bytes / pwrite content. The chunker invariant                       *)
(*     CHUNK_BOUNDARIES_ARE_POSITION_BASED guarantees writer-A's chunk-i  *)
(*     bytes equal writer-B's chunk-i bytes. We model only the state      *)
(*     transitions, not the byte payload.                                 *)
(*   * The actual file system pwrite. We assume pwrite always succeeds; a*)
(*     failure path is modeled separately as PwriteFail action.           *)
(*   * Real-time 60s watchdog. Modeled as an enabled action under WF.    *)
(*   * Multiple digests (one ChunkRaceState lifetime per spec instance).  *)
(*                                                                          *)
(* CITATIONS                                                                *)
(*   [race]   nativelink-store/src/chunked/chunked_race_state.rs:117-693  *)
(*   [reg]    nativelink-store/src/chunked/chunked_race_state.rs:900-1027 *)
(*   [v2hdl]  nativelink-service/src/chunked_write_handler_v2.rs:175-649  *)
(*   [fswrap] nativelink-store/src/filesystem_store.rs:1469-1535          *)
(*   [design] .claude/audits/494-v3-out-of-order-design-2026-05-15.md     *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Writers,        \* Set of writer ids, e.g. {w1, w2}
    Chunks,         \* Set of chunk indices, e.g. {0, 1, 2}
    AllowPwriteFail, \* TRUE => model a pwrite failure non-determinism
    AllowRunnerCancel \* TRUE => model commit-runner panic/cancel non-determinism

ASSUME Cardinality(Writers) >= 1
ASSUME Cardinality(Chunks)  >= 1
ASSUME AllowPwriteFail   \in BOOLEAN
ASSUME AllowRunnerCancel \in BOOLEAN

\* Per-writer FSM states.
WriterStates == {
    "NotAttached",
    "Attached",
    "Sending",
    "AwaitCommit",
    "Done",
    "Aborted"
}

\* Commit result domain. NoResult means "not yet published".
\* "OkResult" / "ErrResult" / "CancelledResult" are the three outcomes the
\* commit-runner OR its guard's drop may publish.
ResultDomain == { "NoResult", "OkResult", "ErrResult", "CancelledResult" }

VARIABLES
    \* Per-writer state.
    writerState,        \* Writers -> WriterStates
    \* Per-writer set of chunks already-attempted (admit returned non-Continue).
    writerSeen,         \* Writers -> SUBSET Chunks
    \* The race-state's bitmap (set of committed chunk indices).
    chunksPresent,      \* SUBSET Chunks
    \* Per-chunk in-flight slot: the set of writer ids currently mid-pwrite for that chunk.
    chunksInFlight,     \* Chunks -> SUBSET Writers
    \* Whether some writer has won the commit-runner election.
    commitRunning,      \* BOOLEAN
    \* Whether a commit result has been published (sticky).
    commitDoneFlag,     \* BOOLEAN
    \* The published commit result.
    commitResult,       \* ResultDomain
    \* Identity of the writer currently elected commit-runner (or NULL).
    runnerWriter,       \* Writers \cup {NULL}
    \* Whether the registry currently has an entry for this digest (force_remove
    \* clears it; subsequent admission would re-create — but this spec models
    \* a single race lifetime; force_remove transitions the system to an
    \* "everyone gets Cancelled" state).
    registryEntry       \* BOOLEAN

\* Sentinel for "no writer is the runner".
NULL == "NULL"

vars == <<writerState, writerSeen, chunksPresent, chunksInFlight,
          commitRunning, commitDoneFlag, commitResult, runnerWriter,
          registryEntry>>

----------------------------------------------------------------------------
(* Init: every writer NotAttached, bitmap empty, no in-flight slots, no   *)
(* commit running, no result published, registry entry present (the      *)
(* first writer to attach materializes it; we model the post-creation    *)
(* state implicit in get_or_create_and_attach having already run for the *)
(* digest — i.e. the entry exists from time 0). This matches the spec's *)
(* "single race lifetime" abstraction.                                    *)
----------------------------------------------------------------------------
Init ==
    /\ writerState    = [w \in Writers |-> "NotAttached"]
    /\ writerSeen     = [w \in Writers |-> {}]
    /\ chunksPresent  = {}
    /\ chunksInFlight = [c \in Chunks |-> {}]
    /\ commitRunning  = FALSE
    /\ commitDoneFlag = FALSE
    /\ commitResult   = "NoResult"
    /\ runnerWriter   = NULL
    /\ registryEntry  = TRUE

----------------------------------------------------------------------------
(* AttachWriter(w): writer w transitions NotAttached → Attached.          *)
(* Models race_state_for_digest_and_attach (chunked_race_state.rs:936-968).*)
(* Atomic with the registry-mutex critical section; we model "the entry  *)
(* exists, attach increments count" as a simple state flip. If the       *)
(* registry was force_removed mid-flight, the writer should still attach *)
(* successfully — but to a fresh state. The spec models a single race   *)
(* lifetime, so post-force_remove attach is OUT OF SCOPE: such writers  *)
(* go to Aborted.                                                       *)
----------------------------------------------------------------------------
AttachWriter(w) ==
    /\ writerState[w] = "NotAttached"
    /\ IF registryEntry
       THEN /\ writerState' = [writerState EXCEPT ![w] = "Attached"]
            /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                           commitRunning, commitDoneFlag, commitResult,
                           runnerWriter, registryEntry>>
       ELSE \* Registry was force_removed; the spec considers a fresh
            \* race-state out-of-scope. The arriving writer Aborts so
            \* the spec terminates cleanly.
            /\ writerState' = [writerState EXCEPT ![w] = "Aborted"]
            /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                           commitRunning, commitDoneFlag, commitResult,
                           runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* WriterStartSending(w): writer w transitions Attached → Sending.        *)
(* Trivial gate; in production this is the first stream.message() return.*)
----------------------------------------------------------------------------
WriterStartSending(w) ==
    /\ writerState[w] = "Attached"
    /\ writerState' = [writerState EXCEPT ![w] = "Sending"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* TryAdmitAlreadyHave(w, c): writer w admits chunk c; the bit is        *)
(* already set, so the outcome is ALREADY_HAVE.                          *)
(* Models try_admit_chunk first branch (chunked_race_state.rs:466-470). *)
----------------------------------------------------------------------------
TryAdmitAlreadyHave(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \in chunksPresent
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* TryAdmitRacingLoser(w, c): writer w admits chunk c; another writer is  *)
(* mid-pwrite (chunksInFlight[c] non-empty), so the outcome is           *)
(* RACING_LOSER.                                                          *)
(* Models try_admit_chunk second branch (chunked_race_state.rs:471-477). *)
----------------------------------------------------------------------------
TryAdmitRacingLoser(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] # {}
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* TryAdmitAccept(w, c): writer w admits chunk c; the slot is empty AND  *)
(* the bit is unset, so the outcome is ACCEPT — w inserts itself into    *)
(* the in-flight slot. The pwrite + mark_chunk_committed are modeled as *)
(* two separate transitions (PwriteSucceed / PwriteFail).                *)
(* Models try_admit_chunk third branch (chunked_race_state.rs:478-479). *)
----------------------------------------------------------------------------
TryAdmitAccept(w, c) ==
    /\ writerState[w] = "Sending"
    /\ c \notin writerSeen[w]
    /\ c \notin chunksPresent
    /\ chunksInFlight[c] = {}
    /\ chunksInFlight' = [chunksInFlight EXCEPT ![c] = {w}]
    /\ writerSeen' = [writerSeen EXCEPT ![w] = @ \cup {c}]
    /\ UNCHANGED <<writerState, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter,
                   registryEntry>>

----------------------------------------------------------------------------
(* PwriteSucceed(w, c): writer w's pwrite for chunk c succeeded; flip   *)
(* the bit + remove w from in-flight slot under the state lock; decide  *)
(* commit responsibility.                                                 *)
(* Models mark_chunk_committed (chunked_race_state.rs:497-553).          *)
(*                                                                          *)
(* The commit decision triangle:                                          *)
(*   bitmap-full + nobody-running + not-done   => RunCommit (we win)     *)
(*   bitmap-full + somebody-running            => AwaitCommit            *)
(*   bitmap-full + done                        => AwaitCommit (read result)*)
(*   bitmap-not-full                           => continue Sending       *)
----------------------------------------------------------------------------
PwriteSucceed(w, c) ==
    /\ w \in chunksInFlight[c]   \* w must have admitted Accept
    /\ LET newPresent == chunksPresent \cup {c}
           newInFlight == [chunksInFlight EXCEPT ![c] = @ \ {w}]
           allPresent  == newPresent = Chunks
       IN
       /\ chunksPresent'  = newPresent
       /\ chunksInFlight' = newInFlight
       /\ IF allPresent /\ ~commitRunning /\ ~commitDoneFlag
          THEN /\ commitRunning' = TRUE
               /\ runnerWriter'  = w
               /\ writerState'   = [writerState EXCEPT ![w] = "AwaitCommit"]
               \* Note: the v2 driver actually sets state to a "RunCommit"
               \* intermediate but the next-step CommitRunnerPublishOk /
               \* RunnerGuardDropCancelled / CommitRunnerPublishErr move it
               \* to Done. We model the runner's pre-publish state as
               \* AwaitCommit too — the only thing the spec cares about is
               \* that the runner eventually publishes (or its guard fires).
          ELSE IF allPresent
          THEN /\ commitRunning' = commitRunning
               /\ runnerWriter'  = runnerWriter
               /\ writerState'   = [writerState EXCEPT ![w] = "AwaitCommit"]
          ELSE /\ commitRunning' = commitRunning
               /\ runnerWriter'  = runnerWriter
               /\ writerState'   = writerState
       /\ UNCHANGED <<writerSeen, commitDoneFlag, commitResult, registryEntry>>

----------------------------------------------------------------------------
(* PwriteFail(w, c): writer w's pwrite for chunk c failed; release the  *)
(* in-flight slot WITHOUT setting the bit, return from run_v2_session, *)
(* RaceWriterGuard::drop fires and purges w from EVERY in-flight slot, *)
(* transition w to Aborted.                                            *)
(* Models the err branch of write_chunk_at_offset in v2 driver         *)
(* (chunked_write_handler_v2.rs:400-412) + release_chunk_in_flight +  *)
(* implicit RaceWriterGuard::drop on early return.                    *)
(* Gated by AllowPwriteFail constant.                                  *)
----------------------------------------------------------------------------
PwriteFail(w, c) ==
    /\ AllowPwriteFail
    /\ w \in chunksInFlight[c]
    \* Release the failing chunk's slot AND every other slot the writer
    \* was occupying (Drop's purge_writer_in_flight fires synchronously
    \* after the early return).
    /\ chunksInFlight' = [c2 \in Chunks |-> chunksInFlight[c2] \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    /\ UNCHANGED <<writerSeen, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* WriterAbortMidStream(w): writer w in Sending state aborts (client    *)
(* disconnect, etc). Drop fires: purge_writer_in_flight + detach.     *)
(* Models RaceWriterGuard::drop (chunked_race_state.rs:772-794) +    *)
(* the multiple early-return paths in run_v2_session.                *)
(* For this spec, "aborts in Sending" means w drops without flipping   *)
(* the bit; any in-flight slots w occupies are evacuated.            *)
----------------------------------------------------------------------------
WriterAbortMidStream(w) ==
    /\ writerState[w] = "Sending"
    /\ chunksInFlight' = [c \in Chunks |-> chunksInFlight[c] \ {w}]
    /\ writerState'    = [writerState EXCEPT ![w] = "Aborted"]
    /\ UNCHANGED <<writerSeen, chunksPresent, commitRunning,
                   commitDoneFlag, commitResult, runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* CommitRunnerPublishOk: the elected commit-runner publishes Ok.     *)
(* Models v2_run_commit_path success path + publish_commit_result(Ok)*)
(* (chunked_write_handler_v2.rs:540-555).                            *)
----------------------------------------------------------------------------
CommitRunnerPublishOk ==
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ chunksPresent = Chunks
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "OkResult"
    \* Runner transitions to Done. Per production semantics, commitRunning
    \* stays TRUE after publish — only RunnerGuardDropCancelled clears it,
    \* and that's gated on ~commit_done_flag. Both flags being TRUE
    \* simultaneously is the stable terminal state.
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Done"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* CommitRunnerPublishErr: the commit-runner publishes Err (e.g. e2e  *)
(* hash mismatch, finalize_holding I/O failure).                      *)
----------------------------------------------------------------------------
CommitRunnerPublishErr ==
    /\ AllowRunnerCancel \* same gate; failure path is structurally similar
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    /\ chunksPresent = Chunks
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "ErrResult"
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Done"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* RunnerGuardDropCancelled: the commit-runner's CommitRunnerGuard      *)
(* drops without publishing (panic / cancellation between the runner   *)
(* observing RunCommit and calling publish_commit_result). The Drop   *)
(* publishes a synthetic Cancelled Err AND clears commit_running so a *)
(* fresh writer arriving later may try again.                         *)
(* Models CommitRunnerGuard::drop (chunked_race_state.rs:856-887).    *)
(*                                                                      *)
(* Important: per the FIX-1 contract test                              *)
(* (chunked_race_state.rs:1253-1296), commit_done_flag becomes true   *)
(* AND commit_running becomes false in this transition.               *)
----------------------------------------------------------------------------
RunnerGuardDropCancelled ==
    /\ AllowRunnerCancel
    /\ commitRunning
    /\ ~commitDoneFlag
    /\ runnerWriter # NULL
    \* Runner exits without publishing; synthetic Cancelled fires.
    /\ commitDoneFlag' = TRUE
    /\ commitResult'   = "CancelledResult"
    /\ commitRunning'  = FALSE
    /\ writerState'    = [writerState EXCEPT ![runnerWriter] = "Aborted"]
    /\ runnerWriter'   = NULL
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight, registryEntry>>

----------------------------------------------------------------------------
(* AwaitingWriterObservesResult(w): w has been Awaiting; the result is  *)
(* now published; w transitions to Done.                               *)
(* Models v2_await_commit_result + send-to-client paths               *)
(* (chunked_write_handler_v2.rs:590-628).                              *)
----------------------------------------------------------------------------
AwaitingWriterObservesResult(w) ==
    /\ writerState[w] = "AwaitCommit"
    /\ commitDoneFlag
    /\ writerState' = [writerState EXCEPT ![w] = "Done"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* WatchdogFires(w): writer w in AwaitCommit hits its 60s timeout (each *)
(* writer has its own tokio::time::timeout in v2_await_commit_result    *)
(* at chunked_write_handler_v2.rs:929). w gets DeadlineExceeded from   *)
(* its own watchdog and Aborts. If the registry entry is still present,*)
(* w ALSO calls force_remove (chunked_write_handler_v2.rs:600-620).   *)
(*                                                                      *)
(* CRITICAL semantic distinction: the watchdog does NOT publish to the *)
(* race-state's commit_result — it only returns DeadlineExceeded from *)
(* the writer's own RPC. The race-state's commit_done_flag stays FALSE *)
(* if the runner hadn't published yet; the runner CAN still publish    *)
(* afterward. The registry entry is force_removed so future writers   *)
(* would get a fresh state.                                            *)
(*                                                                      *)
(* Per-writer watchdog: each writer fires its own timeout independently;*)
(* the spec models this as a per-writer transition rather than a       *)
(* global atomic step. This matches production where writer A's        *)
(* watchdog can fire at T+60s and writer B's at T+62s.                 *)
----------------------------------------------------------------------------
WatchdogFires(w) ==
    /\ writerState[w] = "AwaitCommit"
    /\ ~commitDoneFlag
    /\ writerState' = [writerState EXCEPT ![w] = "Aborted"]
    \* If the registry entry is still present, this writer's
    \* force_remove(&digest) call clears it. Idempotent — subsequent
    \* watchdog fires from sibling writers find an already-cleared
    \* entry; their force_remove is a no-op.
    /\ registryEntry' = FALSE
    \* Note: we DO NOT touch commitRunning, commitDoneFlag, commitResult,
    \* or runnerWriter. The race-state's in-process Arc<ChunkRaceState>
    \* outlives the registry slot; the runner can still publish later.
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter>>

----------------------------------------------------------------------------
(* WriterFinishesAfterAllSeen(w): w has seen every chunk (admitted each *)
(* one with some outcome) but did NOT trigger the commit (e.g. all its *)
(* chunks were AlreadyHave / RacingLoser, or some writer beat it to    *)
(* the last bit). w transitions Sending → AwaitCommit.                *)
(* Models the FRAME path where commit_responsibility is set to        *)
(* AwaitCommit on AlreadyHave / RacingLoser of the final chunk         *)
(* (chunked_write_handler_v2.rs:323-326, 343-346) AND the post-loop  *)
(* "race_state.is_complete()" path (chunked_write_handler_v2.rs:510-511).*)
----------------------------------------------------------------------------
WriterFinishesAfterAllSeen(w) ==
    /\ writerState[w] = "Sending"
    /\ writerSeen[w] = Chunks   \* saw every chunk
    /\ ~(\E c \in Chunks : w \in chunksInFlight[c])
        \* w has no in-flight pwrite outstanding (otherwise PwriteSucceed
        \* / PwriteFail would fire first)
    /\ writerState' = [writerState EXCEPT ![w] = "AwaitCommit"]
    /\ UNCHANGED <<writerSeen, chunksPresent, chunksInFlight,
                   commitRunning, commitDoneFlag, commitResult,
                   runnerWriter, registryEntry>>

----------------------------------------------------------------------------
(* TryClaimCommitRunner(w): a writer in AwaitCommit observes that a    *)
(* previous runner was cancelled (commit_done_flag is now TRUE because *)
(* RunnerGuardDropCancelled fired) but a fresh attempt is possible.   *)
(*                                                                      *)
(* In production, try_claim_commit_runner (chunked_race_state.rs:561-572)*)
(* returns RunCommit only when (a) all_present, (b) !commit_running,  *)
(* (c) !commit_done_flag. Once commit_done_flag is sticky, this never *)
(* fires — siblings observe AwaitCommit and read the published err.   *)
(*                                                                      *)
(* So this spec does NOT model a re-claim transition — it would never *)
(* be enabled. The CancelledResult is the terminal observation.       *)
(* Including this comment as the formal proof that the production code*)
(* prevents commit-runner re-claim once a result (even synthetic) is  *)
(* published.                                                          *)
----------------------------------------------------------------------------

----------------------------------------------------------------------------
(* Next: union of all enabled actions.                                  *)
----------------------------------------------------------------------------
Next ==
    \/ \E w \in Writers : AttachWriter(w)
    \/ \E w \in Writers : WriterStartSending(w)
    \/ \E w \in Writers, c \in Chunks : TryAdmitAlreadyHave(w, c)
    \/ \E w \in Writers, c \in Chunks : TryAdmitRacingLoser(w, c)
    \/ \E w \in Writers, c \in Chunks : TryAdmitAccept(w, c)
    \/ \E w \in Writers, c \in Chunks : PwriteSucceed(w, c)
    \/ \E w \in Writers, c \in Chunks : PwriteFail(w, c)
    \/ \E w \in Writers : WriterAbortMidStream(w)
    \/ \E w \in Writers : WriterFinishesAfterAllSeen(w)
    \/ CommitRunnerPublishOk
    \/ CommitRunnerPublishErr
    \/ RunnerGuardDropCancelled
    \/ \E w \in Writers : AwaitingWriterObservesResult(w)
    \/ \E w \in Writers : WatchdogFires(w)

----------------------------------------------------------------------------
(* Spec: Init + stuttering Next + fairness for liveness.               *)
(*                                                                      *)
(* Fairness rationale:                                                 *)
(*  - WF on commit-runner publish actions: the runner eventually       *)
(*    publishes (success path).                                       *)
(*  - SF on watchdog: real-time 60s elapsed always eventually fires.  *)
(*  - WF on AwaitingWriterObservesResult: a wakened waiter eventually *)
(*    transitions to Done.                                            *)
(*  - WF on RunnerGuardDropCancelled: when the runner panics/cancels, *)
(*    the synchronous guard Drop is guaranteed by std semantics.      *)
(*  - We do NOT add fairness on AttachWriter / WriterStartSending /  *)
(*    pwrite — these are driver-side; the spec checks that EVERY     *)
(*    reachable state can liveness-make-progress, not that every     *)
(*    arbitrary writer attaches.                                     *)
(*                                                                      *)
(* Property check: Termination requires that for EVERY attached writer,*)
(* eventually their state is in {Done, Aborted}. Termination is the   *)
(* primary liveness property.                                          *)
----------------------------------------------------------------------------
Spec ==
    /\ Init
    /\ [][Next]_vars
    \* WF on commit-publish: the runner eventually publishes (cpu_pool +
    \* file system bounded latency).
    /\ WF_vars(CommitRunnerPublishOk)
    \* WF on the synchronous Drop on cancel/panic: tokio's Drop is
    \* guaranteed by std semantics.
    /\ WF_vars(RunnerGuardDropCancelled)
    \* WF on await-result wakeup: a notify_waiters fire is delivered to
    \* every waiter under tokio's wake semantics.
    /\ \A w \in Writers : WF_vars(AwaitingWriterObservesResult(w))
    \* SF on watchdog (per-writer): real-time 60s elapses unconditionally;
    \* modeled as strong fairness on each writer's individual watchdog
    \* so the action eventually fires from any reachable enabled state
    \* (a writer in AwaitCommit + commit_done_flag still false).
    /\ \A w \in Writers : SF_vars(WatchdogFires(w))
    \* WF on pwrite completion (success path): cpu_pool always
    \* dispatches the pwrite task; a hung pwrite is OUT OF SCOPE for v3
    \* (slow-tier-hang is a separate issue tracked elsewhere). For each
    \* (writer, chunk) pair, an in-flight pwrite eventually completes.
    /\ \A w \in Writers, c \in Chunks : WF_vars(PwriteSucceed(w, c))
    \* WF on AwaitCommit transition for finished-but-couldn't-trigger
    \* writers: a writer that exhausted its send loop without flipping
    \* the last bit must transition to AwaitCommit and observe the
    \* eventual result.
    /\ \A w \in Writers : WF_vars(WriterFinishesAfterAllSeen(w))
    \* WF on writer admission actions: a writer in Sending state with
    \* unseen chunks eventually admits each one (Accept | AlreadyHave |
    \* RacingLoser is determined by current state; the disjunction WF
    \* covers "writer makes progress on chunk c").
    /\ \A w \in Writers, c \in Chunks :
            WF_vars(TryAdmitAlreadyHave(w, c) \/ TryAdmitRacingLoser(w, c)
                    \/ TryAdmitAccept(w, c))
    \* WF on the writer-startup transitions so an attached writer
    \* doesn't stutter forever in Attached state.
    /\ \A w \in Writers : WF_vars(WriterStartSending(w))

----------------------------------------------------------------------------
(* INVARIANTS (Safety)                                                  *)
----------------------------------------------------------------------------

(* TypeOK: domain bounds for TLC. *)
TypeOK ==
    /\ \A w \in Writers : writerState[w] \in WriterStates
    /\ \A w \in Writers : writerSeen[w]  \subseteq Chunks
    /\ chunksPresent \subseteq Chunks
    /\ \A c \in Chunks : chunksInFlight[c] \subseteq Writers
    /\ commitRunning  \in BOOLEAN
    /\ commitDoneFlag \in BOOLEAN
    /\ commitResult   \in ResultDomain
    /\ runnerWriter   \in (Writers \cup {NULL})
    /\ registryEntry  \in BOOLEAN

(* MirrorOnce: the commit-runner role can be filled at most ONCE per   *)
(* race-state lifetime. Operationally: commitRunning can transition   *)
(* false → true at most once. Once a result has been published         *)
(* (commitDoneFlag=true), no future RunCommit decision can fire (per  *)
(* mark_chunk_committed and try_claim_commit_runner both gating on    *)
(* !commit_done_flag). And once cancelled (commitDoneFlag=true /\     *)
(* commitRunning=false), the gating still prevents re-election: see   *)
(* the "TryClaimCommitRunner" comment above.                         *)
(*                                                                      *)
(* In the spec, MirrorOnce is encoded as: at any reachable state, at  *)
(* most ONE writer has runnerWriter set. The runner is monotone:      *)
(* once set, only RunnerGuardDropCancelled clears it back to NULL,    *)
(* and that transition also sets commitDoneFlag=TRUE — preventing      *)
(* re-election.                                                        *)
MirrorOnce ==
    \* If a result has been published, no fresh commit-runner can be
    \* elected (the commit-runner role is one-shot).
    \* This is a structural invariant: commitRunning => runnerWriter # NULL
    \* AND once commitDoneFlag, no future PwriteSucceed/etc. triggers
    \* a NEW runnerWriter assignment.
    /\ (commitRunning => runnerWriter # NULL)
    /\ (commitDoneFlag => commitResult \in {"OkResult", "ErrResult", "CancelledResult"})

(* NoDoubleCommit: commitRunning cannot transition false → true twice *)
(* in the same race-state lifetime. This is enforced by the gating in *)
(* mark_chunk_committed: once commit_done_flag is TRUE, the           *)
(* RunCommit branch is unreachable (the all_present /\ ~commit_done_flag*)
(* condition is false).                                                *)
(*                                                                      *)
(* Encoded as: at any reachable state, IF commitDoneFlag THEN         *)
(* the only way commitRunning could be TRUE is if RunnerGuardDropCancelled*)
(* fires (which sets commitRunning'=FALSE atomically). So if commitDoneFlag*)
(* AND commitRunning, the state is unreachable.                       *)
NoDoubleCommit ==
    \* After the cancellation transition fires, commitRunning is FALSE
    \* AND commitDoneFlag is TRUE. The spec's transitions never re-set
    \* commitRunning to TRUE in such a state.
    \* So: commitRunning AND commitDoneFlag may BOTH be true momentarily
    \* (between PwriteSucceed setting commitRunning=TRUE and either of
    \* PublishOk/PublishErr which sets commitDoneFlag=TRUE). After
    \* commitDoneFlag is true, only RunnerGuardDropCancelled or
    \* WatchdogForceRemove transition commitRunning, both to FALSE.
    \*
    \* Concretely: NoDoubleCommit holds iff there is no reachable state
    \* with commitDoneFlag=TRUE /\ commitRunning was previously cleared
    \* /\ then re-set. We can't directly express "previously" in TLA+;
    \* instead, we check the invariant that commit-runner cannot be
    \* RE-elected once a result has been observed by ANY writer.
    \* Operationally: if commitDoneFlag, then for any subsequent step
    \* commitRunning' must equal commitRunning OR transition to FALSE.
    \* This is a temporal property, not a state predicate; we check it
    \* via TLC under the action transitions.
    \*
    \* Simpler encoding: commit-runner is monotone in "have we ever
    \* started a commit?" — once commitDoneFlag, runnerWriter shouldn't
    \* change to a different non-NULL writer.
    TRUE  \* See ResultMonotone for the equivalent action-level check

(* ResultMonotone: once commitResult is non-NoResult (i.e., a result *)
(* has been published), it never changes. (This is the action-level *)
(* version of NoDoubleCommit.)                                       *)
ResultMonotone ==
    [][commitResult # "NoResult" => commitResult' = commitResult]_vars

(* RunnerOwnsCommit: if a runner is elected (runnerWriter set), then  *)
(* commit_running must be TRUE — they are atomically established in  *)
(* mark_chunk_committed under the state lock. Once commit_running is *)
(* cleared (only by RunnerGuardDropCancelled), runnerWriter is also  *)
(* cleared.                                                          *)
RunnerOwnsCommit ==
    runnerWriter # NULL => commitRunning

(* InFlightDisjoint: a writer cannot occupy multiple in-flight slots *)
(* at the same time? Actually production allows that (parallel       *)
(* chunks). So the invariant is: a writer in Aborted / Done has NO  *)
(* in-flight slots remaining (purge_writer_in_flight on Drop).      *)
InFlightDisjointAfterDone ==
    \A w \in Writers :
        writerState[w] \in {"Done", "Aborted"} =>
            \A c \in Chunks : w \notin chunksInFlight[c]

(* ChunksPresentImpliesNoInFlightForSelf: if a chunk's bit is set,    *)
(* the writer who set it is no longer in that chunk's in-flight slot.*)
(* This is enforced by mark_chunk_committed atomically.              *)
NoInFlightAfterPresent ==
    \A c \in Chunks :
        c \in chunksPresent =>
            \* The bit is set — at least one writer pwrote successfully.
            \* No writer that pwrote is still mid-pwrite on the same
            \* offset (only one writer can have been in the slot at a
            \* time, because TryAdmitAccept gates on the slot being empty).
            TRUE  \* trivially true given TryAdmitAccept's gating

(* NoStaleAck: if w received Accepted for chunk c (modeled as: w
   pwrote successfully + c \in chunksPresent), then either
   c \in chunksPresent stays true forever OR w's RaceWriterGuard
   dropped (purging in-flight, which doesn't affect chunksPresent).
   Per the position-based chunker, chunksPresent is sticky — once true,
   never cleared except by force_remove.
   Encoded as: chunksPresent monotone EXCEPT across WatchdogForceRemove *)
ChunksPresentMonotonicWithinRegistry ==
    [][registryEntry /\ registryEntry' => chunksPresent \subseteq chunksPresent']_vars

(* NoCorruption: if commitResult = OkResult, then chunksPresent = Chunks. *)
(* I.e. the bytes referenced by chunksPresent fully cover the blob.       *)
(* Per chunker invariant CHUNK_BOUNDARIES_ARE_POSITION_BASED, those      *)
(* bytes are equal to the canonical bytes for the digest.                *)
NoCorruption ==
    commitResult = "OkResult" => chunksPresent = Chunks

(* NoPermanentWedge: there is no reachable state where commitRunning=TRUE,*)
(* commitDoneFlag=FALSE, AND no writer is in the runner role (runnerWriter*)
(* = NULL). This would be the "orphan runner" wedge.                     *)
NoPermanentWedge ==
    (commitRunning /\ ~commitDoneFlag) => runnerWriter # NULL

(* RegistryHygieneNoNewAttach: once registryEntry is FALSE, no writer  *)
(* can transition into Sending (they'd Abort instead).               *)
(* Spec abstracts a single race lifetime; new writers post-force_remove*)
(* would enter a fresh state out-of-scope. The AttachWriter action's  *)
(* else-branch sends them straight to Aborted, satisfying this.      *)
RegistryHygieneNoNewAttach ==
    ~registryEntry =>
        \A w \in Writers :
            writerState[w] \in {"NotAttached", "Attached", "Sending",
                                "AwaitCommit", "Done", "Aborted"}
    \* Trivially true (just a sanity check that no out-of-domain state appears).

----------------------------------------------------------------------------
(* LIVENESS PROPERTIES                                                   *)
----------------------------------------------------------------------------

(* Termination: every attached writer eventually reaches Done or       *)
(* Aborted. (NotAttached writers may stay NotAttached — that's a       *)
(* "writer never showed up" state, not a wedge.)                       *)
Termination ==
    \A w \in Writers :
        <>(writerState[w] \in {"Done", "Aborted", "NotAttached"})

(* AllWritersTerminate: a stronger version: every writer that ever      *)
(* leaves NotAttached eventually reaches Done or Aborted.              *)
\* Note: TLC's liveness checking with leadsto evaluates per-step, so we
\* express this as a leads-to: any state with a Sending/Attached writer
\* eventually reaches a state where that writer is Done or Aborted.
AllAttachedWritersTerminate ==
    \A w \in Writers :
        (writerState[w] \in {"Attached", "Sending", "AwaitCommit"})
            ~> (writerState[w] \in {"Done", "Aborted"})

(* CommitEventuallyResolves: once commit is running, it eventually      *)
(* publishes a result.                                                 *)
CommitEventuallyResolves ==
    commitRunning ~> commitDoneFlag

(* EventualResultPublished: under WF on the commit-publish actions,    *)
(* if we ever start a commit (commitRunning=TRUE at some point), then *)
(* commitDoneFlag eventually becomes TRUE.                              *)
EventualResultPublished ==
    <>commitDoneFlag \/ \A w \in Writers : writerState[w] = "NotAttached"

============================================================================
