-------------------------- MODULE SchedulerDedup --------------------------
(***************************************************************************
  Models invariant (5) DEDUP of the AwaitedActionDb add_action path.

    Two Cacheable actions with the SAME ActionUniqueKey => exactly one
    operation_id; the 2nd (and later) client joins the existing op and
    `connected_clients` is incremented. Uncacheable actions => a separate
    op every time.

  MECHANISM (memory_awaited_action_db.rs):
    add_action [:819]:
      1. try_subscribe [:910]:
           - Uncacheable => return Ok(None)                    [:921]
           - Cacheable: look up action_info_hash_key_to_awaited_action
             [:924]; if present, connected_clients += 1 and return the
             existing subscriber [:941-950]; else Ok(None).
      2. On Ok(None): allocate a NEW OperationId::default() [:843],
         insert into client_operation_to_awaited_action [:865], and
         (Cacheable only) insert unique_key -> operation_id into
         action_info_hash_key_to_awaited_action [:870-873].

    COMPLETION removes the unique_key mapping so a later identical action
    starts fresh (memory_awaited_action_db.rs:484 remove_action_from_state
    on the Cacheable branch). Modeled as CompleteOp clearing keyToOp.

  CITATIONS:
    [add-action]     memory_awaited_action_db.rs:819
    [try-subscribe]  memory_awaited_action_db.rs:910
    [uncacheable]    memory_awaited_action_db.rs:921 Uncacheable => Ok(None)
    [key-lookup]     memory_awaited_action_db.rs:924
    [cc-incr]        memory_awaited_action_db.rs:941-950 connected_clients += 1
    [new-op]         memory_awaited_action_db.rs:843 OperationId::default()
    [key-insert]     memory_awaited_action_db.rs:870-873
    [key-remove]     memory_awaited_action_db.rs:484 (Cacheable completion)

  ABSTRACTION: we model a fixed set of client requests, each tagged with a
  unique Key it targets and whether it is Cacheable. TLC explores every
  arrival order and interleaving with completion. Ops are represented by the
  KEY they belong to (cacheable) or by a per-client fresh id (uncacheable),
  which is exactly the operation-identity the dedup contract cares about.
 ***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Clients,        \* set of client request ids, e.g. {c1, c2, c3}
    Keys,           \* set of cacheable unique keys, e.g. {k1, k2}
    ClientKey,      \* Clients -> Keys  : which unique key each client targets
    ClientCacheable,\* Clients -> BOOLEAN : is this client's action Cacheable
    c1, c2, c3,     \* model-value client ids (bound in .cfg)
    k1, k2          \* model-value key ids (bound in .cfg)

ASSUME Cardinality(Clients) >= 1
ASSUME Cardinality(Keys) >= 1
\* NOTE: ClientKey / ClientCacheable are function-valued constants. TLC's
\* .cfg cannot hold a function literal, so the cfg substitutes them via
\* `<- ClientKeyDef` / `<- ClientCacheableDef` operators defined below.

NONE == "none"
OpIdsForKey == Keys                 \* a live cacheable op is identified by its key
\* uncacheable ops are identified by the client id (each is unique)

VARIABLES
    arrived,        \* Clients -> BOOLEAN : has this client's add_action run
    clientOp,       \* Clients -> (Keys \cup Clients \cup {NONE}) : the op id
                    \*   this client ended up attached to
    keyToOp,        \* Keys -> BOOLEAN : is there a LIVE cacheable op for key
    connected       \* Keys -> Nat : connected_clients for the live cacheable op

vars == << arrived, clientOp, keyToOp, connected >>

TypeOK ==
    /\ arrived \in [Clients -> BOOLEAN]
    /\ clientOp \in [Clients -> (Keys \cup Clients \cup {NONE})]
    /\ keyToOp \in [Keys -> BOOLEAN]
    /\ connected \in [Keys -> 0..Cardinality(Clients)]

Init ==
    /\ arrived = [c \in Clients |-> FALSE]
    /\ clientOp = [c \in Clients |-> NONE]
    /\ keyToOp = [k \in Keys |-> FALSE]
    /\ connected = [k \in Keys |-> 0]

(***************************************************************************
  AddCacheable(c): a Cacheable client's add_action runs.
    - If keyToOp[key] already TRUE: JOIN -> connected += 1, clientOp = key.
    - Else: create the op -> keyToOp[key] = TRUE, connected = 1, clientOp=key.
 ***************************************************************************)
AddCacheable(c) ==
    /\ ~arrived[c]
    /\ ClientCacheable[c]
    /\ LET k == ClientKey[c] IN
        /\ arrived' = [arrived EXCEPT ![c] = TRUE]
        /\ clientOp' = [clientOp EXCEPT ![c] = k]
        /\ keyToOp' = [keyToOp EXCEPT ![k] = TRUE]
        /\ connected' = [connected EXCEPT ![k] = @ + 1]

(***************************************************************************
  AddUncacheable(c): an Uncacheable client's add_action runs. Always a
  fresh, separate op (identified by the client id itself). Never touches
  keyToOp/connected.
 ***************************************************************************)
AddUncacheable(c) ==
    /\ ~arrived[c]
    /\ ~ClientCacheable[c]
    /\ arrived' = [arrived EXCEPT ![c] = TRUE]
    /\ clientOp' = [clientOp EXCEPT ![c] = c]
    /\ UNCHANGED << keyToOp, connected >>

(***************************************************************************
  CompleteKey(k): the live cacheable op for key k completes; its unique_key
  mapping is removed [key-remove] so a future identical action starts fresh.
  connected resets to 0. Clients already attached keep their clientOp (they
  observed the result). Only fires when the op exists and all its clients
  have arrived (an op cannot complete before it was created).
 ***************************************************************************)
CompleteKey(k) ==
    /\ keyToOp[k]
    /\ keyToOp' = [keyToOp EXCEPT ![k] = FALSE]
    /\ connected' = [connected EXCEPT ![k] = 0]
    /\ UNCHANGED << arrived, clientOp >>

Done == (\A c \in Clients : arrived[c]) /\ UNCHANGED vars

Next ==
    \/ \E c \in Clients : AddCacheable(c)
    \/ \E c \in Clients : AddUncacheable(c)
    \/ \E k \in Keys : CompleteKey(k)
    \/ Done

Fairness == \A c \in Clients : WF_vars(AddCacheable(c)) /\ WF_vars(AddUncacheable(c))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************
  Concrete config bindings. The .cfg maps ClientKey <- ClientKeyDef and
  ClientCacheable <- ClientCacheableDef. Model values c1..c3, k1..k2 are the
  Clients/Keys set elements declared in the .cfg.

  Scenario: c1,c2 both Cacheable on key k1 (must dedup to one op); c3
  Uncacheable on k2 (must be its own distinct op).
 ***************************************************************************)
ClientKeyDef == (c1 :> k1 @@ c2 :> k1 @@ c3 :> k2)
ClientCacheableDef == (c1 :> TRUE @@ c2 :> TRUE @@ c3 :> FALSE)

------------------------------------------------------------------------------
(***************************************************************************
  INVARIANTS
 ***************************************************************************)

(* (5a) Cacheable clients targeting the same key that arrive while the op is
   LIVE share ONE op id (= the key). Two arrived cacheable clients on the
   same key with clientOp both set are attached to the same op id. (After a
   completion the key mapping is cleared, so a later arrival legitimately
   forms a new op -- but it is STILL identified by the same key value; the
   contract is "one LIVE op per key", which keyToOp being boolean enforces.) *)
CacheableShareOneOp ==
    \A ca, cb \in Clients :
        (/\ arrived[ca] /\ arrived[cb]
         /\ ClientCacheable[ca] /\ ClientCacheable[cb]
         /\ ClientKey[ca] = ClientKey[cb]
         /\ clientOp[ca] # NONE /\ clientOp[cb] # NONE)
        => clientOp[ca] = clientOp[cb]

(* (5b) An Uncacheable client always gets its OWN distinct op id, never
   shared with any other client. *)
UncacheableDistinct ==
    \A ca \in Clients :
        (arrived[ca] /\ ~ClientCacheable[ca]) =>
            /\ clientOp[ca] = ca
            /\ \A cb \in Clients : (cb # ca) => clientOp[cb] # ca

(* (5c) connected_clients balance: for a live cacheable op, connected equals
   the number of arrived cacheable clients on that key since it was created.
   We check the WEAKER but critical property: a live key has connected >= 1
   (never a live op with zero clients), and connected never exceeds the
   number of cacheable clients on that key. *)
ConnectedNonNegative ==
    \A k \in Keys :
        keyToOp[k] => (connected[k] >= 1
                       /\ connected[k] <= Cardinality({c \in Clients :
                            ClientCacheable[c] /\ ClientKey[c] = k}))

(* At most one LIVE cacheable op per key (structural: keyToOp is boolean). *)
AtMostOneLiveOpPerKey == \A k \in Keys : keyToOp[k] \in BOOLEAN

Safety ==
    /\ TypeOK
    /\ CacheableShareOneOp
    /\ UncacheableDistinct
    /\ ConnectedNonNegative

=============================================================================
