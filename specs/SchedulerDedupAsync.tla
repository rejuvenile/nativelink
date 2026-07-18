------------------------ MODULE SchedulerDedupAsync ------------------------
(***************************************************************************
  DE-ATOMIZED variant of SchedulerDedup.tla.

  ------------------------------------------------------------------------
  THE FALSE ATOMICITY IN SchedulerDedup.tla
  ------------------------------------------------------------------------
  SchedulerDedup.tla's `AddCacheable(c)` fuses THREE real steps into ONE
  atomic action AND models op-identity as the KEY itself:

      clientOp' = [clientOp EXCEPT ![c] = k]    \* identity == the key
      keyToOp'  = [keyToOp  EXCEPT ![k] = TRUE] \* lookup + insert fused

  So two cacheable clients on the same key are dedup-identical BY
  CONSTRUCTION (both get clientOp = k). The dedup invariant passes
  vacuously: the spec cannot even represent the outcome the contract
  actually forbids -- two DISTINCT `OperationId`s for one key.

  In production the real op-identity is a freshly minted
  `OperationId::default()` (memory_awaited_action_db.rs:843) /
  `(operation_id_creator)()` (store_awaited_action_db.rs:900), and
  `add_action` is:

      1. try_subscribe: LOOK UP the key in the index.
           - memory path: under ONE `async_lock::Mutex` held across the
             whole add_action -> lookup+insert IS atomic -> dedup holds.
           - REDIS/store path (store_awaited_action_db.rs): try_subscribe
             does `search_by_index_prefix` against a RediSearch index whose
             updates become visible ASYNCHRONOUSLY (indexing lag). The
             lookup SAMPLES A STALE VIEW.
      2. On miss: mint a FRESH operation_id and `update_data(
         UpdateOperationIdToAwaitedAction)` -- a CAS keyed on the
         operation_id, NOT on the unique key. Two distinct fresh ids => NO
         version conflict => BOTH writes succeed => TWO live ops per key.

  The code itself documents this gap:
    - store_awaited_action_db.rs:711-714 "closes the RediSearch index-
      visibility window where two concurrent `add_action` calls can both
      see empty and create duplicate scheduler operations."
    - memory_awaited_action_db.rs:868-874 logs
      `error!("action_info_hash_key_to_awaited_action already has
      unique_key")` -- the code anticipates a same-key double insert.

  SchedulerDedup.tla models ONLY the atomic memory path and hides the
  Redis path entirely by fusing identity to the key. This is exactly the
  MarkStableViaBlobsAvailable false-atomicity class (CLAUDE.md 2026-07-17).

  ------------------------------------------------------------------------
  WHAT THIS SPEC SPLITS
  ------------------------------------------------------------------------
  We separate the three fused steps and make the lagged index explicit:

    Sample(c)      -- try_subscribe: reads the VISIBLE index only.
    CreateOp(c)    -- on a miss, mints a fresh op (id = c) and writes it to
                      the authoritative op store (`created`). The op is NOT
                      yet visible in the index.
    IndexCatchUp(c)-- async RediSearch indexing makes creator c's op
                      visible. TLC may delay it arbitrarily, so another
                      client's Sample can fall in the gap.

  Op identity: a client that CREATES gets op-id = its own client id
  (distinct per creator -- models the fresh OperationId). A client that
  JOINS inherits the op-id it joined.

  ------------------------------------------------------------------------
  THE FIX (AtomicKeyClaim toggle)
  ------------------------------------------------------------------------
  AtomicKeyClaim = FALSE  (Bugged):  CreateOp always mints on a miss, even
      if another op for the key already exists-but-is-not-yet-visible. This
      is the current Redis behavior (CAS on op-id cannot detect a key
      collision). => two live ops => dedup VIOLATED.

  AtomicKeyClaim = TRUE   (Fixed):   CreateOp performs a key-conditional
      atomic claim against the AUTHORITATIVE op set (`created`), not the
      lagged index -- e.g. a SETNX / conditional-create keyed on the unique
      key, or holding a per-key lock across sample+insert. If an op already
      exists for the key it JOINS instead of minting. => at most one live
      op => dedup HOLDS.

  NOTE the store's retry-once-on-miss (20ms sleep, :713) is a probabilistic
  MITIGATION, not this fix: it only narrows the window (re-sample after a
  delay). It cannot close it if indexing lag exceeds the delay. We model
  the STRUCTURAL fix (authoritative key claim), which actually re-
  establishes the invariant, so Fixed is genuinely green.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Clients,        \* set of client request ids racing on ONE cacheable key
    AtomicKeyClaim  \* FALSE => Bugged (mint on stale view); TRUE => Fixed

ASSUME Cardinality(Clients) >= 2       \* need >=2 racers to exhibit the gap
ASSUME AtomicKeyClaim \in BOOLEAN

NONE == "none"

\* Deterministic pick of one op id from a non-empty set (a "live op" is
\* identified by its creator client id). Under the correct behavior the set
\* is a singleton, so which one is picked is irrelevant.
PickOne(S) == CHOOSE x \in S : TRUE

VARIABLES
    phase,          \* Clients -> {"idle","sampledMiss","done"}
    clientOp,       \* Clients -> (Clients \cup {NONE}) : op id attached to
    created,        \* SUBSET Clients : creator ids whose op EXISTS in the
                    \*   authoritative op store (each element = one live op)
    indexVisible    \* SUBSET Clients : creator ids whose key->op mapping is
                    \*   VISIBLE in the RediSearch index (LAGS `created`)

vars == << phase, clientOp, created, indexVisible >>

Phases == {"idle", "sampledMiss", "done"}

TypeOK ==
    /\ phase \in [Clients -> Phases]
    /\ clientOp \in [Clients -> (Clients \cup {NONE})]
    /\ created \subseteq Clients
    /\ indexVisible \subseteq Clients
    /\ indexVisible \subseteq created           \* index can only lag, never lead

Init ==
    /\ phase = [c \in Clients |-> "idle"]
    /\ clientOp = [c \in Clients |-> NONE]
    /\ created = {}
    /\ indexVisible = {}

(***************************************************************************
  Sample(c) -- try_subscribe. Reads ONLY the visible index.
    - If some op is index-visible: JOIN it (connected_clients += 1 path).
    - Else: Ok(None). We record a "sampledMiss" -- the STALE view. Note an
      op may ALREADY be in `created` (minted by a racing client) but not
      yet in `indexVisible`; this client cannot see it.
 ***************************************************************************)
Sample(c) ==
    /\ phase[c] = "idle"
    /\ IF indexVisible # {}
       THEN /\ clientOp' = [clientOp EXCEPT ![c] = PickOne(indexVisible)]
            /\ phase' = [phase EXCEPT ![c] = "done"]
            /\ UNCHANGED << created, indexVisible >>
       ELSE /\ phase' = [phase EXCEPT ![c] = "sampledMiss"]
            /\ UNCHANGED << clientOp, created, indexVisible >>

(***************************************************************************
  CreateOp(c) -- the create step after a try_subscribe miss.
    - AtomicKeyClaim (Fixed) AND an op already exists for the key: the
      authoritative key-conditional claim sees it -> JOIN (no mint).
    - Otherwise: mint a FRESH op (id = c), write to the store. NOT yet
      index-visible.
 ***************************************************************************)
CreateOp(c) ==
    /\ phase[c] = "sampledMiss"
    /\ IF AtomicKeyClaim /\ created # {}
       THEN /\ clientOp' = [clientOp EXCEPT ![c] = PickOne(created)]
            /\ phase' = [phase EXCEPT ![c] = "done"]
            /\ UNCHANGED << created, indexVisible >>
       ELSE /\ created' = created \cup {c}
            /\ clientOp' = [clientOp EXCEPT ![c] = c]
            /\ phase' = [phase EXCEPT ![c] = "done"]
            /\ UNCHANGED << indexVisible >>

(***************************************************************************
  IndexCatchUp(c) -- async RediSearch indexing makes creator c's op
  visible. This is the step whose DELAY opens the dedup gap. TLC can fire
  it early, late, or interleave it freely with other clients' Sample.
 ***************************************************************************)
IndexCatchUp(c) ==
    /\ c \in created
    /\ c \notin indexVisible
    /\ indexVisible' = indexVisible \cup {c}
    /\ UNCHANGED << phase, clientOp, created >>

AllDone == \A c \in Clients : phase[c] = "done"

\* Stutter once everything is settled so TLC does not flag the legitimate
\* terminal as a deadlock. (Index need not be fully caught up for safety.)
Done == AllDone /\ UNCHANGED vars

Next ==
    \/ \E c \in Clients : Sample(c)
    \/ \E c \in Clients : CreateOp(c)
    \/ \E c \in Clients : IndexCatchUp(c)
    \/ Done

Spec == Init /\ [][Next]_vars

------------------------------------------------------------------------------
(***************************************************************************
  INVARIANTS
 ***************************************************************************)

(* (5-core) AT MOST ONE LIVE OP PER KEY. This is the crisp property the
   fused spec could not express: two distinct minted ops for one cacheable
   key is a dedup failure (duplicate execution). *)
AtMostOneLiveOp == Cardinality(created) <= 1

(* (5a) Two settled cacheable clients on the same key share ONE op id. In
   this single-key model every done client is on the same key, so all
   attached op ids must be equal. *)
CacheableShareOneOp ==
    \A ca, cb \in Clients :
        (phase[ca] = "done" /\ phase[cb] = "done"
         /\ clientOp[ca] # NONE /\ clientOp[cb] # NONE)
        => clientOp[ca] = clientOp[cb]

Safety ==
    /\ TypeOK
    /\ AtMostOneLiveOp
    /\ CacheableShareOneOp

=============================================================================
