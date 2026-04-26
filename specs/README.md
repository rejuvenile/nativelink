# NativeLink TLA+ Specifications

This directory contains TLA+ specifications for NativeLink's internal
cross-component protocols. Each spec is paired with at least two `.cfg`
files: a "fixed" config that should pass model checking, and a "bugged"
config that reproduces a real production bug as a TLC trace.

## Why TLA+ for NativeLink

The bugs we have shipped repeatedly over the past two weeks share a shape:
**invariants that hold when each component is reasoned about in isolation
fail at the composition boundary.** Concretely:

- **Pin/BIS leak** (audit at
  `.claude/reviews/bis-coverage-for-already-cached-outputs/audit.md`):
  worker pins every action-output digest; the server's stable_digests
  feeder is only pushed by `update_oneshot` success; cached-blob writes
  short-circuit and skip `update_oneshot`; BIS never fires; pins leak
  permanently under v2. Each side (worker pin set, server cache, BIS
  loop) is correct in isolation. The bug is in the COVERAGE of the
  release path.
- **Writer termination** (CLAUDE.md, "Test in production composition"):
  every `*Store::get_part` early-return that takes a borrowed `&mut
  writer` must call `writer.send_error()`. Five sibling sites in
  `fast_slow_store.rs` violated this contract. The unit test for each
  site (which OWNS the writer) returned the right `Err`; the wrapping
  layer (`VerifyStore::get_part`'s `tokio::join!`) deadlocked.
- **Replica invariant**
  (`/home/user/.claude/projects/-src-nativelink/memory/project_cas_write_invariant.md`):
  cache-positive insert and Bazel Ack must wait for BOTH the server's
  fast tier AND a peer mirror to hold the bytes. The historic bug
  gates these on the fast tier alone — a subsequent eviction produces a
  "blob missing from CAS" symptom.

These all type-check, lint clean, and pass code review in isolation. They
are detectable in TLA+ for free because TLA+'s default semantics is
"every action interleaves arbitrarily" — the cross-component composition
is the model's NATURAL state space.

## Tooling

These specs target the standard TLA+ Toolbox / `tla2tools.jar` model
checker (TLC). Tested with TLC release 1.8.0, version
`2026.04.22.172729` (rev 6320a09).

Install:

    # Option 1: tla2tools.jar (preferred for CI / CLI use).
    wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
    # The CLI examples in this README assume the jar is at /tmp/tla2tools.jar.

    # Option 2: TLA+ Toolbox (GUI; easier for first-time interactive
    # exploration; download from the same GitHub releases page).

JDK 11+ is required. Tested with OpenJDK 26.0.2.

## Running the specs

All commands assume `cwd = specs/`. Add `-workers auto` for parallel
state exploration on large models; the bounded models here finish in
<1 s on a single worker.

    # Pin / BIS lifecycle:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config PinLifecycleV1.cfg PinLifecycle
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config PinLifecycleV2.cfg PinLifecycle
    #   Expected: "Invariant NoPermanentPinLeak is violated."
    #   Trace shows: WorkerPinAction(d1) -> WorkerUploadCacheHit(d1) ->
    #   ... -> Quiescent state with workerPins = {d1}.

    # Writer termination contract:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config WriterTerminationFixed.cfg WriterTermination
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config WriterTerminationBug.cfg WriterTermination
    #   Expected: "Invariant JoinAlwaysCompletes is violated."
    #   Trace shows: ProducerStart -> ProducerFailWithoutSendError ->
    #   stuck (channel=Open, consumer=Recv, producer=DoneBuggy).

    # Replica invariant:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config ReplicaInvariantFixed.cfg ReplicaInvariant
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config ReplicaInvariantBugged.cfg ReplicaInvariant
    #   Expected: "Invariant CachePositiveImpliesTwoReplicasAtInsert is violated."

    # H2 connection-pool predicate gap:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config H2ConnectionPoolFixed.cfg H2ConnectionPool
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config H2ConnectionPoolBugged.cfg H2ConnectionPool
    #   Expected: "Invariant ResourceExhaustedTriggersEviction is violated."
    #   Trace shows: Goaway(c1) -> Fetch(c1) yielding code=ResourceExhausted ->
    #   pendingReconnect still empty, EvictsErr=FALSE -> stuck.

To run only static analysis (parser + name-resolution; useful when you
want to verify a spec compiles without running model checking):

    java -cp /tmp/tla2tools.jar tla2sany.SANY <SpecName>.tla

TLC writes scratch state under `./states/` and trace-replay specs as
`*_TTrace_*.tla` / `*.bin` next to the input. Both are gitignored
(see `.gitignore` at repo root if it doesn't already cover them) — feel
free to delete after an interactive session.

## What each spec covers

### `PinLifecycle.tla`

Models the pin / BIS lifecycle protocol across:

- worker pin set (`fs_store.pin_digest` /
  `fs_store.unpin_digest`),
- server's `cas_store` cache state,
- the `stable_digests` queue inside `FastSlowStore`,
- the BIS broadcast loop in `src/bin/nativelink.rs`.

Two `.cfg` files toggle between v1 (TTL self-heal active) and v2
(durable pins; BIS is the only release path). Under v2 the spec
reproduces the EXACT trace from
`.claude/reviews/bis-coverage-for-already-cached-outputs/audit.md`:
worker pins a digest already in the server cache, the server's
`BatchUpdateBlobs` short-circuits without pushing to `stable_digests`,
BIS never carries the digest, and the pin is permanently stuck.

The `NoPermanentPinLeak` invariant is the one that fires.

### `WriterTermination.tla`

Models the borrowed-writer composition pattern:

    let (tx, rx) = make_buf_channel_pair_with_size(...);
    let get_fut   = inner_store.get_part(d, &mut tx, ...);   // PRODUCER
    let check_fut = consumer_loop(rx, ...);                  // CONSUMER
    let (g, c) = tokio::join!(get_fut, check_fut);

Two `.cfg` files toggle whether the producer's failure branch obeys the
"send_error before returning Err" contract. The bugged config exposes
the deadlock state where the producer has finished with Err, the
channel is still Open, and the consumer is permanently blocked on
`rx.recv()`.

The `JoinAlwaysCompletes` invariant is the one that fires.

### `ReplicaInvariant.tla`

Models the ≥2 in-memory replica gating for CAS writes. Two `.cfg`
files toggle whether cache-positive insert / Bazel Ack are gated on
the peer mirror completing. The bugged config exposes the trace where
the cache claims Positive with only the server fast-tier replica
present, and a subsequent eviction produces a "blob missing" cascade.

The `CachePositiveImpliesTwoReplicasAtInsert` and `NoLossyAckCascade`
invariants both fire under the bugged config.

### `H2ConnectionPool.tla`

Models the h2 connection-pool stale-channel reuse bug at
`grpc_store.rs:143-155` (`looks_like_dead_channel` predicate) and the
post-GOAWAY ResourceExhausted surface code that escaped the historic
predicate's allowlist. Two `.cfg` files toggle the predicate's
inclusion of `Code::ResourceExhausted`. The bugged config exposes the
trace where a `Draining` channel surfaces ResourceExhausted on every
fetch but is never evicted from the pool, leading to unbounded fetch
failure on subsequent checkouts. The race-loser-abort
× `parallel_chunk_count = 64` amplification shape is captured by
TLC's natural exploration of multiple in-flight fetches against the
same channel.

The `ResourceExhaustedTriggersEviction` invariant fires on the
bugged config; the trace is the minimal:
`Goaway(c1) -> Fetch(c1) [ResourceExhausted, no eviction]`.

## Scope honesty

Each spec includes an explicit ASSUMPTION block listing what is and
isn't modeled. Read those before claiming a spec covers a bug class
broader than the documented one. In particular:

- **Single worker / single peer / one digest at a time** in most
  specs. A second worker would only blow up the state space without
  exposing a new bug class for THESE bugs.
- **No actual byte-stream chunking** — the channels carry an abstract
  token. Hash verification, partial-bytes-then-error edge cases, and
  cancellation are out of scope.
- **No actual TCP / h2 / QUIC / GOAWAY semantics.** That's a separate
  bug class (the H2 stale-channel-pool bug) that warrants its own
  spec, not modeled here.
- **No clocks.** TTL release in `PinLifecycle` is modeled as a
  non-deterministic action with weak fairness, not a real timer.

## Adding new specs

1. Pick a cross-component protocol whose contract you've watched fail in
   production.
2. Identify the smallest set of state variables that capture the
   contract — one per component-state, one per message-channel.
3. Write actions that mirror production code paths, with one-line
   comments citing `file:line` for each.
4. Bound every variable. Pick the smallest CONSTANT values that still
   exhibit the bug shape.
5. Write the safety invariant as a property the BUGGED config WILL
   violate. If the bugged config passes, your invariant doesn't catch
   the bug — fix it.
6. Optionally add liveness with WF/SF fairness if you can defend the
   fairness assumption in the production code.
7. Mutate the spec ("comment out the gate predicate, re-run TLC,
   confirm the violation disappears or moves") to verify the
   invariant actually constrains the buggy behavior.

## Suggested next protocols

In rough priority order:

1. **Phantom-blob false-alarm conflation**
   (`project_phantom_blob_false_alarm_2026_04_25.md`): `head_was_ok`
   conflates `LazyExistenceOnSync skip` with `has-said-Some`. Spec the
   FastSlowStore populate path's flag vs. the head-decision path.
2. **Pin listener multiplicity**
   (`project_pin_listener_multiplicity_2026_04_25.md`): 3×
   FastSlowStore listener registration + populate-pin scope mismatch.
   Model multiple listeners over a single pin set.
3. **Trait-default no-op wrapper inheritance**
   (`store_trait.rs:954-991`): the `stable_notify`/`drain_stable_digests`
   defaults are no-ops; wrappers that forget to override silently
   swallow the contract delegation. Model a 2-level wrapper hierarchy
   and check that delegation reaches the leaf.
4. **failed_slow_writes retry-on-reconnect**: separate `failed_writes`
   set, drained on worker reconnect. Model the worker disconnect /
   reconnect cycle and check that no digest is permanently stuck in
   the failed set.

## Citations to production code

Each `.tla` file's top-level comment block lists the exact file:line
citations for the production code each operator models. When a spec
diverges from production semantics, the divergence is called out
explicitly in the SCOPE block.

## Status

These specs were authored 2026-04-26 and were each run under TLC at
that time:

- `PinLifecycleV1.cfg`: PASS (no violation)
- `PinLifecycleV2.cfg`: FAIL as designed (`NoPermanentPinLeak` violated)
- `WriterTerminationFixed.cfg`: PASS
- `WriterTerminationBug.cfg`: FAIL as designed (`JoinAlwaysCompletes` violated)
- `ReplicaInvariantFixed.cfg`: PASS
- `ReplicaInvariantBugged.cfg`: FAIL as designed
  (`CachePositiveImpliesTwoReplicasAtInsert` violated)
- `H2ConnectionPoolFixed.cfg`: PASS (no violation)
- `H2ConnectionPoolBugged.cfg`: FAIL as designed
  (`ResourceExhaustedTriggersEviction` violated)

If a fix lands that changes one of the production code paths cited in
a spec, re-run the corresponding bugged config to verify the spec
still reproduces the bug class (i.e., the spec hasn't drifted).
