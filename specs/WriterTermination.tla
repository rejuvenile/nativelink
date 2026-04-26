--------------------------- MODULE WriterTermination ---------------------------
(***************************************************************************
  Writer-termination contract for borrowed write halves.

  Models the production composition pattern at the heart of the
  2026-04-25 multi-hour Bazel-build wedge documented in
  CLAUDE.md ("Test in production composition, not in isolation"):

      VerifyStore::get_part {
          let (tx, rx) = make_buf_channel_pair_with_size(4);
          let get_fut   = inner_store.get_part(d, &mut tx, ...);   // PRODUCER
          let check_fut = inner_check_get_part(writer, rx, ...);   // CONSUMER
          let (g, c) = tokio::join!(get_fut, check_fut);
          g.merge(c)
      }

  The producer is e.g. FastSlowStore::get_part. It owns the borrowed `tx`.
  The consumer is the verify-side reader: it loops on `rx.recv()`, exiting
  on EOF or error.

  CONTRACT: every exit path of the producer MUST terminate the writer:
    * success path: send_eof (or a final send of a 0-length chunk),
    * failure path: send_error so the consumer sees a structured Err.
  If the producer returns without terminating the writer, the channel
  stays open while `tx` is still alive. As long as `tx` exists in the
  outer scope (i.e. the join! frame), `rx.recv().await` blocks forever
  and `check_fut` never returns. `tokio::join!` waits for ALL futures,
  so the whole get_part deadlocks.

  This bug existed at fast_slow_store.rs:2912 (and 4 SIBLING sites at
  :2657, :2719, :2775, :2831) until the WriteHalfGuard RAII fix landed.
  CLAUDE.md "Sibling-bug audit" was created from this incident.

  The spec models a simplified version of this protocol:
    * a producer that may terminate, fail-with-send_error, or
      fail-WITHOUT-send_error (the bug),
    * a consumer that recv-loops and only returns on EOF or error,
    * a join state that returns once both futures complete.

  CONSTANT BugMode toggles whether the producer respects the contract
  on its NotFound branch:
    * BugMode = FALSE: producer always sends send_error before
      returning Err. Spec is deadlock-free.
    * BugMode = TRUE:  producer's NotFound branch returns Err WITHOUT
      send_error. Spec deadlocks (consumer permanently blocked).

  EXPECTED TLC OUTCOMES (see also the .cfg files):
    * WriterTerminationFixed.cfg (BugMode = FALSE): NO invariant
      violation; TLC reports "Model checking completed". The
      `JoinAlwaysCompletes` invariant holds.
    * WriterTerminationBug.cfg  (BugMode = TRUE):  INVARIANT VIOLATED
      on `JoinAlwaysCompletes`. TLC's trace shows: producer takes
      NotFound branch -> producer state = Done(Err) -> consumer state
      = Recv (still blocked) -> no transition can clear the deadlock
      because the bug branch never closed the channel.

  SCOPE — what this spec models:
    * a single (tx, rx) channel,
    * a producer with three terminating branches: Ok+EOF, Err+SendError,
      Err-without-SendError (the bug, gated on BugMode),
    * a consumer that recv-loops; recv either returns the next chunk,
      EOF, or an Error,
    * the join! frame: whether `tx` is still alive (channel open) is
      modeled by the boolean `txOwnedByJoin`. In production, `tx` lives
      for the duration of the entire join! call frame because it was
      not moved into get_fut prior to the WriteHalfGuard fix; we model
      that here as `tx` only being dropped when the join! returns,
      which itself depends on both futures completing -> deadlock.

  SCOPE — what this spec does NOT model:
    * actual byte-level chunks; the channel carries an abstract token,
    * the WriteHalfGuard RAII Drop fallback (the FIX). To model the
      FIX would mean modeling tx-move-into-get_fut so the channel
      drops when the producer finishes; we instead model BugMode as
      "did the producer call send_error?" which is the equivalent
      semantic difference.
    * other RPC layers (h2 / QUIC / tonic),
    * partial-bytes-then-Err edge cases (orthogonal),
    * graceful shutdown / cancellation,
    * the post-recv hash-verify side of inner_check_get_part.

  CITATIONS:
    [v]    nativelink-store/src/verify_store.rs:294-374
    [bug1] nativelink-store/src/fast_slow_store.rs:2912 (in_flight short stream)
    [bug2] nativelink-store/src/fast_slow_store.rs:2778 (mirror size mismatch)
    [bug3] nativelink-store/src/fast_slow_store.rs:2856 (truncated fast read)
    [guard]nativelink-store/src/fast_slow_store.rs:2740-2747 (WriteHalfGuard)
    [doc]  CLAUDE.md "Test in production composition, not in isolation"
 ***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    BugMode    \* TRUE => producer's NotFound branch omits send_error.

ASSUME BugMode \in BOOLEAN

\* Producer states.
\*   "Idle"     - producer hasn't started.
\*   "Working"  - producer is still computing the result.
\*   "DoneOk"   - producer finished, sent EOF, returned Ok.
\*   "DoneErr"  - producer finished, sent send_error, returned Err.
\*   "DoneBuggy"- producer finished, omitted send_error, returned Err.
ProducerStates == {"Idle", "Working", "DoneOk", "DoneErr", "DoneBuggy"}

\* Channel states (what the consumer's next recv would observe).
\*   "Open"   - channel open, no message pending; recv would block.
\*   "Eof"    - producer sent EOF; recv returns "EOF".
\*   "Errored"- producer sent send_error; recv returns "Err".
ChannelStates == {"Open", "Eof", "Errored"}

\* Consumer states.
\*   "Recv"   - consumer is in rx.recv().await — blocked on Open channel.
\*   "DoneEof"- consumer received EOF and returned Ok.
\*   "DoneErr"- consumer received an error frame and returned Err.
ConsumerStates == {"Recv", "DoneEof", "DoneErr"}

\* Outer-frame state.
\*   "Joining"  - both futures still going; tx alive in the join! frame.
\*   "Returned" - both futures completed; tx dropped; whole get_part
\*                returned (Ok/Err per merged result).
JoinStates == {"Joining", "Returned"}

VARIABLES
    producer,   \* one of ProducerStates
    consumer,   \* one of ConsumerStates
    channel,    \* one of ChannelStates
    join        \* one of JoinStates

vars == <<producer, consumer, channel, join>>

----------------------------------------------------------------------------
Init ==
    /\ producer = "Idle"
    /\ consumer = "Recv"
    /\ channel  = "Open"
    /\ join     = "Joining"

----------------------------------------------------------------------------
(* ProducerStart: producer transitions Idle -> Working.                  *)
----------------------------------------------------------------------------
ProducerStart ==
    /\ producer = "Idle"
    /\ producer' = "Working"
    /\ UNCHANGED <<consumer, channel, join>>

----------------------------------------------------------------------------
(* ProducerSucceed: producer finishes the happy path. send_eof, return Ok.*)
(* Models the Ok(()) arms in fast_slow_store.rs:2880, :2802, etc.       *)
----------------------------------------------------------------------------
ProducerSucceed ==
    /\ producer = "Working"
    /\ producer' = "DoneOk"
    /\ channel'  = "Eof"
    /\ UNCHANGED <<consumer, join>>

----------------------------------------------------------------------------
(* ProducerFailWithSendError: producer takes a failure branch and       *)
(* explicitly terminates the writer with send_error.                    *)
(* Models the FIXED form: e.g. fast_slow_store.rs:2778 / :2856 / :2893  *)
(*   return Err(guard.fail(make_err!(...)))                              *)
(* (where guard.fail() does the send_error).                             *)
----------------------------------------------------------------------------
ProducerFailWithSendError ==
    /\ producer = "Working"
    /\ producer' = "DoneErr"
    /\ channel'  = "Errored"
    /\ UNCHANGED <<consumer, join>>

----------------------------------------------------------------------------
(* ProducerFailWithoutSendError: producer takes the buggy NotFound      *)
(* branch and returns Err WITHOUT calling send_error. The channel       *)
(* stays "Open" (no EOF, no error frame).                               *)
(* This is the historic bug at fast_slow_store.rs:2912 (pre-fix).       *)
(* Disabled when BugMode = FALSE (the FIXED build always uses           *)
(* WriteHalfGuard so this state is unreachable).                         *)
----------------------------------------------------------------------------
ProducerFailWithoutSendError ==
    /\ BugMode
    /\ producer = "Working"
    /\ producer' = "DoneBuggy"
    /\ UNCHANGED <<consumer, channel, join>>

----------------------------------------------------------------------------
(* ConsumerRecvEof: consumer's rx.recv() returns EOF -> consumer Ok.    *)
----------------------------------------------------------------------------
ConsumerRecvEof ==
    /\ consumer = "Recv"
    /\ channel  = "Eof"
    /\ consumer' = "DoneEof"
    /\ UNCHANGED <<producer, channel, join>>

----------------------------------------------------------------------------
(* ConsumerRecvErr: consumer's rx.recv() returns an Err -> consumer Err. *)
----------------------------------------------------------------------------
ConsumerRecvErr ==
    /\ consumer = "Recv"
    /\ channel  = "Errored"
    /\ consumer' = "DoneErr"
    /\ UNCHANGED <<producer, channel, join>>

----------------------------------------------------------------------------
(* JoinReturn: tokio::join! returns once BOTH futures completed.        *)
----------------------------------------------------------------------------
ProducerDone == producer \in {"DoneOk", "DoneErr", "DoneBuggy"}
ConsumerDone == consumer \in {"DoneEof", "DoneErr"}

JoinReturn ==
    /\ join = "Joining"
    /\ ProducerDone
    /\ ConsumerDone
    /\ join' = "Returned"
    /\ UNCHANGED <<producer, consumer, channel>>

----------------------------------------------------------------------------
Next ==
    \/ ProducerStart
    \/ ProducerSucceed
    \/ ProducerFailWithSendError
    \/ ProducerFailWithoutSendError
    \/ ConsumerRecvEof
    \/ ConsumerRecvErr
    \/ JoinReturn

----------------------------------------------------------------------------
(* Fairness: every enabled internal step eventually fires.              *)
(*                                                                      *)
(* `ProducerCompletes` is the disjunction of ALL producer-completion   *)
(* steps. WF on this disjunction guarantees the producer eventually    *)
(* takes SOME completion path (Ok, Err+send, or the bug branch         *)
(* gated by BugMode), without forcing any specific branch.             *)
(*                                                                      *)
(* This means: under BugMode = FALSE, only the safe branches are      *)
(* enabled, so the scheduler picks one and the join completes ->      *)
(* liveness holds. Under BugMode = TRUE, the buggy branch is enabled  *)
(* alongside the safe ones, but TLC explores ALL traces — the trace   *)
(* that picks the buggy branch produces the safety violation, and    *)
(* the safe-branch traces produce no violation.                       *)
----------------------------------------------------------------------------
ProducerCompletes ==
    \/ ProducerSucceed
    \/ ProducerFailWithSendError
    \/ ProducerFailWithoutSendError

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(ProducerStart)
    /\ WF_vars(ProducerCompletes)
    /\ WF_vars(ConsumerRecvEof)
    /\ WF_vars(ConsumerRecvErr)
    /\ WF_vars(JoinReturn)

----------------------------------------------------------------------------
(* INVARIANTS                                                            *)
----------------------------------------------------------------------------

TypeOK ==
    /\ producer \in ProducerStates
    /\ consumer \in ConsumerStates
    /\ channel  \in ChannelStates
    /\ join     \in JoinStates

(* Stuck: a state from which no Next transition is enabled. In TLA+    *)
(* this is exactly the deadlock signal TLC reports when                  *)
(* CHECK_DEADLOCK = TRUE. We surface it as an explicit safety          *)
(* invariant so it shows up under both deadlock checking AND           *)
(* invariant checking — useful for reviewers reading TLC output.       *)
NotStuckBeforeReturn ==
    join = "Returned"
    \/ ENABLED Next

(* SAFETY: JoinAlwaysCompletes                                            *)
(* If the producer has finished (any way), the consumer must be able    *)
(* to make progress (recv EOF, recv Err, or already done). The bug at  *)
(* fast_slow_store.rs:2912 violates this: producer = "DoneBuggy",      *)
(* channel = "Open", consumer = "Recv" -> consumer cannot transition.  *)
(* Without channel closure, ConsumerRecvEof / ConsumerRecvErr are both  *)
(* disabled and the join can never return.                               *)
JoinAlwaysCompletes ==
    ProducerDone =>
        \/ ConsumerDone               \* consumer already done
        \/ channel = "Eof"            \* consumer can EOF
        \/ channel = "Errored"        \* consumer can Err
        \* If producer is done but channel is still "Open", the
        \* consumer is stuck. That's the deadlock.

(* LIVENESS: get_part eventually returns.                                *)
EventuallyJoinReturns == <>(join = "Returned")

============================================================================
