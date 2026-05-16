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

    # Phantom-blob false-alarm conflation:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config PhantomBlobExistenceCacheFixed.cfg PhantomBlobExistenceCache
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config PhantomBlobExistenceCacheBugged.cfg PhantomBlobExistenceCache
    #   Expected: "Invariant GateFlagMatchesHasOutcome is violated."
    #   Trace shows: HeadStepLazySkip -> flagSetByHead=TRUE while
    #   realHasReturnedSome=FALSE -> conflation. Re-running may also surface
    #   "Invariant WarnFiresOnlyOnGenuineRace is violated" for the
    #   downstream false-alarm symptom.

    # Pin listener multiplicity (3x FastSlowStore wrapper amplification):
    java -cp /tmp/tla2tools.jar tlc2.TLC -config PinListenerMultiplicityFixed.cfg PinListenerMultiplicity
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config PinListenerMultiplicityBugged.cfg PinListenerMultiplicity
    #   Expected: "Invariant EveryEventFiresAtMostOneListener is violated."
    #   Trace shows: PickPinSourceSlowWrite(1) -> StartFiring -> RunListener
    #   for w=1,2,3 -> firedCount=3 -> CompleteFiring (3x amplification).

    # Trait-default noop wrapper inheritance:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config TraitDefaultNoopFixed.cfg TraitDefaultNoop
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config TraitDefaultNoopBugged.cfg TraitDefaultNoop
    #   Expected: "Temporal property EventuallyWorkerUnpinned was violated."
    #   Trace cycle: LeafRecordsDigest -> BroadcastStartDrain ->
    #   WrapperNoopDrain (returns {}) -> BroadcastEmit (broadcasts nothing) ->
    #   loop, worker permanently Pinned.

    # failed_slow_writes retry-on-reconnect cycle:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config FailedSlowWritesRetryFixed.cfg FailedSlowWritesRetry
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config FailedSlowWritesRetryBugged.cfg FailedSlowWritesRetry
    #   Expected: "Temporal property EventuallyConverges was violated."
    #   Trace cycle: PinTtlInsertOnHang -> SlowWriteFailsInBand ->
    #   DrainAndRetryUnbounded -> back to InFlight, failed_slow_writes
    #   recurs forever under persistent failure.

    # UploadSizeInfo producer/consumer size contract:
    java -cp /tmp/tla2tools.jar tlc2.TLC -config SizeInfoContractFixed.cfg SizeInfoContract
    #   Expected: "Model checking completed. No error has been found."
    java -cp /tmp/tla2tools.jar tlc2.TLC -config SizeInfoContractBugged.cfg SizeInfoContract
    #   Expected: "Invariant NoPoisonedCommit is violated."
    #   Trace shows: DeclareExactSize(N) -> DeliverBytes(M != N) ->
    #   ConsumerCommit -> committedSize=N, deliveredM=M, poisoned=TRUE.
    #   Matches the production page-rounding regression (commit 9bfe9924,
    #   2026-04-26): declared 139264, received 137515, silent commit
    #   poisoning every future read.

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

### `PhantomBlobExistenceCache.tla`

Models the `head_was_ok` conflation in
`fast_slow_store.rs::run_producer`. The HEAD step has THREE possible
outcomes (`HasReturnedSome`, `HasReturnedNone`, `LazySkip`); the
production code's flag must be set ONLY in the first case. Two
`.cfg` files toggle the conflation: bugged sets the flag on BOTH
`HasReturnedSome` AND `LazySkip` (the historic
`head_was_ok = head_result.is_ok()` shape); fixed sets it only in
the real-Some branch (the post-investigator
`has_actually_returned_some` flag).

The `GateFlagMatchesHasOutcome` invariant fires immediately on the
bugged config: 2-state trace `HeadStepLazySkip` produces
`flagSetByHead = TRUE` while `realHasReturnedSome = FALSE`. The
`WarnFiresOnlyOnGenuineRace` invariant fires on a longer trace
where the populate step then errors with NotFound and the gate
opens, emitting a false-alarm warn — modeling the 178 false alarms
per 10 minutes observed on production workers.

### `PinListenerMultiplicity.tla`

Models the 3× FastSlowStore listener registration + populate-pin
scope mismatch documented at
`project_pin_listener_multiplicity_2026_04_25.md` and the
production fix in `nativelink-store/src/fast_slow_store.rs:87-160`.
Production constructs THREE FastSlowStore wrappers against the
SAME underlying FilesystemStore fast tier (one per
`local_worker.rs` registration site); each registers a
`PinExpireFailedWritesListener`. Pre-fix the listener had no
in-flight gate, so a single pin-expire event fired all three
listeners unconditionally — 3× warn fan-out + 3×
`failed_slow_writes` insert per event, including for digests
pinned by `DirectoryCache` downloads where no slow-write was ever
outstanding.

Two `.cfg` files toggle the in-flight gate (`BugMode`). Bugged
(`NumWrappers=3, BugMode=TRUE`) violates
`EveryEventFiresAtMostOneListener` with a 7-state trace; also
violates `DownloadPinsDoNotFireWarn` and
`NoAmplificationOverNumWrappers`. Fixed (`BugMode=FALSE`) holds
all invariants — `firedCount <= 1` always, download-pin events
fire zero listeners. Models the 5774 listener-fire events / 10 min
observed on production workers.

### `TraitDefaultNoop.tla`

Models the silent no-op delegation bug class baked into
`nativelink-util/src/store_trait.rs:954-991`. The trait
declares `drain_stable_digests` / `stable_notify` / `pin_digests` /
`drain_failed_digests` with NO-OP / EMPTY defaults. Wrappers
(`VerifyStore`, `ExistenceCacheStore`, `RefStore`,
`WorkerProxyStore`) MUST override all four to delegate to
`inner_store`. A wrapper that forgets to override compiles
cleanly, runs cleanly, and SILENTLY swallows the contract — the
leaf store's contributions never reach the broadcast loop in
`src/bin/nativelink.rs:382-416`. Rust's trait system is happy
with the inherited default; `cargo check` and `cargo clippy` find
nothing.

Two `.cfg` files toggle `BugMode`. Bugged (`BugMode=TRUE`) violates
the LIVENESS property `EventuallyWorkerUnpinned` with a 4-state
cycle: `LeafRecordsDigest` → `BroadcastStartDrain` →
`WrapperNoopDrain` (returns `{}` despite leaf queue=`{"D"}`) →
`BroadcastEmit` (broadcasts nothing) → back to Idle, leaf still
Recorded, worker permanently Pinned. Fixed (`BugMode=FALSE`)
applies the explicit override and the worker eventually receives
the BIS and unpins. The in-flight C+D refactor at
`worktree-agent-aea1038e` addresses this class with a
`StableDigestDelegation` enum that removes the trait-level default
and forces every wrapper to declare its delegation strategy at
the type level — killing the silent-failure mode at compile time.

### `FailedSlowWritesRetry.tla`

Models the worker-side retry protocol that drains
`failed_slow_writes` on every reconnect and re-uploads the digests
to the server. Bug class: under persistent slow-store failure
(server-side store unreachable, or write rejected for a
non-self-healing reason), the retry path forms an INFINITE CYCLE
that never converges. Production today (`local_worker.rs:1327-1352`
+ `handle_upload_missing_blobs`'s "log warn and drop the result"
arm) has no retry budget, no exponential backoff, no dead-letter
queue.

Two `.cfg` files toggle `BugMode`. Bugged (`BugMode=TRUE`) violates
the LIVENESS property `EventuallyConverges` with a 4-state SCC
cycle: `PinTtlInsertOnHang` (pin TTL fires while slow-write still
in-flight) → `SlowWriteFailsInBand` (in-flight upload errors) →
`DrainAndRetryUnbounded` (drain set, re-pin, re-spawn upload) →
back to InFlight forever. Fixed (`BugMode=FALSE`) applies a
hypothetical `MaxRetries`-bounded counter with dead-letter; the
liveness property holds. Both pin-TTL re-insert
(`fast_slow_store.rs:128-155`) and in-band failure
(`fast_slow_store.rs:2033, :2370`) paths are modeled.

### `MarkStableViaBlobsAvailable.tla`

Models the post-#140 (this branch) cross-component pin/release
pairing protocol. Worker pins every digest, advertises via
BlobsAvailable on every tick; server runs `has_with_results` and
`mark_stable` for the present subset; BIS broadcast loop emits
`BlobsInStableStorage`; worker `unpin_digest`s. Bug class: audit
Path 2's `register_action_result_digests` race against
`evicted_digests` on the same `mpsc::channel(1)` — eviction
arriving before registration silently drops, then registration
re-adds the stale entry. The `FixV140` constant toggles between
the pre- and post-#140 architectures.

`MarkStableViaBlobsAvailableBugged.cfg` (FixV140=FALSE) violates
`NoLostEvictionRace` with the documented audit Path 2 trace.
`MarkStableViaBlobsAvailableFixed.cfg` (FixV140=TRUE) runs clean
AND satisfies the liveness property `EventuallyConsistentLocality`
via WF on the dispatch / drain / deliver actions.

KNOWN UNMODELED (deferred to follow-up tracker tasks; see
`.claude/audits/task-139-lost-eviction/audit.md`):
- **audit Path 1** — failed BlobsAvailable delta-send drops the
  drained deltas (worker `swap()`s the BlobChangeTracker BEFORE
  the network send acks; on Err, drained deltas are unrecoverable).
  Spec models a successful FIFO delivery without explicit drop;
  modeling Path 1 needs a separate `WorkerSendFailure` action that
  reuses the drained-but-unsent state.
- **audit Path 3** — `UploadMissingBlobs` / `TouchBlobs` reveal
  missing digests but the worker doesn't backpropagate eviction
  to the server. Spec models neither RPC; adding them needs new
  actions on the server→worker direction (currently absent).

### `SizeInfoContract.tla`

Models the `UploadSizeInfo` producer/consumer size contract that today's
hot-fix (commit `9bfe9924`,
`filesystem_store: has_with_results returns actual blob size, not
page-rounded size_on_disk`) closed at the `FilesystemStore` producer
side. Producer declares one of `ExactSize(N)` / `MaxSize(N)`; the byte
stream eventually delivers `M`; the leaf-store consumer must enforce
"`ExactSize(N)` requires `M == N`; `MaxSize(N)` requires `M <= N`" or
the cache is silently poisoned with a `committedSize != deliveredM`
that every future reader inherits.

Two `.cfg` files toggle `FixSize`. Bugged (`FixSize=FALSE`) violates
`NoPoisonedCommit` with a 4-state trace
(`DeclareExactSize(N) -> DeliverBytes(M != N) -> ConsumerCommit ->
poisoned=TRUE`) — the same shape as the production failure (declared
139264, received 137515). Fixed (`FixSize=TRUE`) holds all
invariants because the consumer rejects every contract-violating pair
before commit.

The four latent consumer sites the hot-fix audit identified
(`fast_slow_store.rs:948`, `fast_slow_store.rs:1891/1900`,
`bytestream_server.rs:2001`, `store_trait.rs:1106-1113`) all build
sizes that flow into the same leaf-store contract; this spec is the
protocol-level invariant they share. If any one of them ever again
declares an `ExactSize(N)` for a stream that delivers `M != N`, the
Bugged trace reproduces the failure.

### `ChunkRaceReadersV2.tla`

Models the chunked-write race-state cascade with concurrent readers
issuing `has_with_results` and `get_part` against
`FastSlowStore::chunked_in_flight_digests` + the FilesystemStore's
`evicting_map`. Originally landed under #499 / `e7a50f57`. Verified
against `origin/main = 8e6be6d7` (2026-05-16) — see the spec's
"RECENT PROTOCOL EVOLUTION" block for the #447 / #508 / #502 / #500
walk and why each change does or does not require a spec edit.

Two CONSTANT toggles drive the bug shape:

- `WriterRegistersInFlight = TRUE` (post-fix v2) — `InFlightChunkedGuard`
  is created at session entry; readers observing has() during P0..P4
  see Some via the in-flight set.
- `WriterRegistersInFlight = FALSE` (pre-fix H1 — see also the
  `ChunkRaceReadersV2_BuggedH1.cfg`) — readers see has=None during the
  bug window and conclude NotFound. `NoActiveWriterInvisibleToReader`
  invariant red-fails.

`Fixed.cfg` / `Bugged.cfg` are the verify-gate canonical configs;
historic informational configs (`_2w_2r_2c`, `_2w_3r_2c`, `_3w_2r_2c`,
etc.) explore larger state spaces and may exceed the 300 s gate budget.
Companion spec `ChunkRaceReadersBuggedH3.tla` models the H3 #247
stale-negative window (`evicting_map.insert` after PublishOk never
fires); its `Bugged.cfg` shows the H3 hazard reachable, its `Fixed.cfg`
asserts only structural invariants that hold in the bug spec by
construction. The H3 spec was renamed (formerly
`ChunkRaceReadersV2_BuggedH3.tla`) to drop the `ChunkRaceReadersV2`
prefix, so that `verify_tla.sh`'s `${name}*.cfg` glob no longer mixes
the H3-specific cfgs (which reference `H3HazardStateUnreachable`) into
the canonical `ChunkRaceReadersV2.tla` spec's per-cfg sweep.

### `StreamingBlobReadDuringWrite.tla`

Models concurrent slow-writers x slow-readers x read-during-write over
the `streaming_blob` primitive
(`nativelink-util/src/streaming_blob.rs:631-822` for the reader,
`:425-486` for the writer). Captures the post-#500 / post-#502 wire-
shape contract between `StreamingBlobWriter::send_eof` /
`send_error` / `Drop` and `StreamingBlobReader::next_chunk`.

CONSTANTS:

- `Readers`              — set of reader ids; the spec verifies clean
                           up to 4 concurrent readers x 2 chunks
                           within the 300 s gate budget.
- `NumChunks`            — declared chunk count (= digest size when
                           ChunkSize = 1).
- `AllowShortEof`        — writer may call `send_eof` after writing
                           fewer than `NumChunks` chunks. Models the
                           #502 trigger; pre-fix the reader saw this
                           as `Ok(Bytes::new())` (silent-short).
- `AllowErrEof`          — writer may call `send_error`.
- `AllowDropWithoutEof`  — writer may drop without `send_eof`; the
                           Drop impl fires terminal=Err.
- `ShortShieldOn`        — TRUE = post-#502 reader returns
                           `Err(Code::Internal, SILENT_SHORT)` when
                           terminal=Ok and bytes < expected.
- `SilentZeroPropOn`     — TRUE = post-#500
                           `bytestream_server::inner_read::consume_ok_eof`
                           propagates producer-side Err to the reader's
                           outcome.

Bugged variants:

- `Bugged.cfg` — `ShortShieldOn = FALSE`. `NoSilentShortToReader` is
  always violated; `NoSilentZeroToReader` also violated when writer
  shorts at chunkCount=0.
- `_BuggedSilentZero.cfg` — `SilentZeroPropOn = FALSE` with shield
  ON. Isolates the #500 fix from the #502 fix; `NoSilentZeroToReader`
  red-fails.

Safety invariants:

- `NoSilentZeroToReader`             — no reader terminates in
                                       `DoneSilentZero`.
- `NoSilentShortToReader`            — no reader terminates in
                                       `DoneSilentShort`.
- `NoCorruptBytesToReader`           — defensive placeholder.
- `PostFixReaderOutcomeIsTwoWay`     — under both fixes, terminated
                                       readers are in
                                       {DoneOk, DoneErr, TimedOut}.
- `ReaderOkImpliesWriterFullOk`      — a `DoneOk` reader implies the
                                       writer's terminal state is
                                       `DoneOkFull`.
- `PostFixErrPropagationReachesReader` — producer Err + propagation on
                                       => no silent-zero/short reader
                                       terminal.
- `PostFixFullOkReachesReaderAsOk`   — writer full-Ok implies no
                                       reader is in silent-zero/short
                                       or corrupt.
- `CursorAtMostChunkCountAtTerminal` — reader cursor never overshoots
                                       what the writer wrote.

Liveness:

- `WriterEventuallyTerminates`       — writer reaches one of the four
                                       Done states.
- `ReadersEventuallyTerminate`       — every subscribed reader reaches
                                       a terminal state.
- `EventualConsistencyOkFull`        — a reader subscribing after
                                       writer's full-Ok eventually
                                       reaches `DoneOk`.
- `NoEternalParkAfterWriterDone`     — once the writer has terminated,
                                       no reader stays
                                       Subscribed/Waiting forever.

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

1. **Mirror-blobs in-memory replica accounting**
   (`project_mirror_write_timeout_durability_hole.md` +
   `project_cas_write_invariant.md`): the worker's mirror_blobs map
   holds a digest in memory until BIS arrives, and counts as a
   replica for the ≥2-in-memory invariant. Model the mirror_blobs
   eviction (`MIRROR_BLOBS_MAX_BYTES` cap) racing against BIS
   arrival; under cap pressure, the mirror copy can be silently
   dropped while the server's fast tier is the only remaining
   replica. A subsequent server eviction violates the invariant.
2. **GOAWAY-then-reconnect race**: H2ConnectionPool models the
   predicate gap; this would model the OPPOSITE failure — the
   pool correctly evicts but races a fresh connection to the same
   endpoint, leading to pool entry duplication and a stale entry
   selected on the next checkout. Subtle interaction with the
   `connections_per_endpoint=32` round-robin selector.
3. **completeness_checking_store batch pin coverage**
   (`completeness_checking_store.rs:309, 451`): batch verification
   pins all "verified" digests, but the batch-result-merging path
   has multiple early-return arms; model whether a batch with
   mixed verified/unverified blobs always pins the verified ones
   even on partial-batch failure. Sibling pattern to PinLifecycle
   but at the batch-RPC layer.
4. **worker-mirror eviction × eviction-listener race**
   (`MokaEvictingMap` + `mirror_blobs.rs`): mirror_blobs has its
   own eviction listener distinct from the FilesystemStore's
   pin-expire listener. Model whether both listeners fire
   correctly when a mirror entry is evicted at the moment a pin
   is being released — the failure mode is double-counted
   replicas in the BIS broadcast.

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
- `PhantomBlobExistenceCacheFixed.cfg`: PASS (no violation)
- `PhantomBlobExistenceCacheBugged.cfg`: FAIL as designed
  (`GateFlagMatchesHasOutcome` violated; `WarnFiresOnlyOnGenuineRace`
   also violated on continued exploration)
- `PinListenerMultiplicityFixed.cfg`: PASS (no violation)
- `PinListenerMultiplicityBugged.cfg`: FAIL as designed
  (`EveryEventFiresAtMostOneListener` violated; trace shows 3×
   amplification across all `NumWrappers=3` listeners)
- `TraitDefaultNoopFixed.cfg`: PASS (no violation)
- `TraitDefaultNoopBugged.cfg`: FAIL as designed (LIVENESS property
  `EventuallyWorkerUnpinned` violated; 4-state cycle with empty drain)
- `FailedSlowWritesRetryFixed.cfg`: PASS (no violation)
- `FailedSlowWritesRetryBugged.cfg`: FAIL as designed (LIVENESS property
  `EventuallyConverges` violated; 4-state SCC cycle on the
  drain-retry-fail loop)
- `SizeInfoContractFixed.cfg`: PASS (no violation)
- `SizeInfoContractBugged.cfg`: FAIL as designed (`NoPoisonedCommit`
  violated; 4-state trace `DeclareExactSize(N) -> DeliverBytes(M != N) ->
  ConsumerCommit -> poisoned=TRUE` matches the production page-rounding
  regression of commit `9bfe9924` declared/received pattern)

If a fix lands that changes one of the production code paths cited in
a spec, re-run the corresponding bugged config to verify the spec
still reproduces the bug class (i.e., the spec hasn't drifted).

### Re-verification 2026-05-16 (origin/main = `8e6be6d7`)

Each existing spec was re-run after the #447 / #508 / #502 / #500 /
#487 / #503 / #504 commits landed. Three changes were applied:

1. **`ChunkRaceReadersV2.tla`**: appended a "RECENT PROTOCOL EVOLUTION"
   section to the top doc-comment block walking each commit and stating
   why the spec did or did not need an edit. No model changes — the
   spec correctly models the chunked-WC race-state cascade in both
   pre-#447 and post-#447 forms (the Notify-wait helper is a caller of
   the same primitive the spec already models).
2. **`ChunkRaceReadersV2Fixed.cfg`** + **`ChunkRaceReadersV2Bugged.cfg`**
   added so the Layer-1 verify gate recognises a canonical pair. The
   historic `ChunkRaceReadersV2.cfg` was REMOVED — it was a duplicate
   of `ChunkRaceReadersV2Fixed.cfg` whose `*V2.cfg` filename pattern
   the gate's classifier (mis-)treats as a bugged variant, which then
   failed since the cfg ran clean (it IS the post-fix configuration).
   The historic `ChunkRaceReadersV2_BuggedH1.cfg` is retained as an
   informational variant that demonstrates the H1 hazard (writer not
   in-flight-set-registered).
3. **`ChunkRaceReadersBuggedH3Fixed.cfg`** + **`ChunkRaceReadersBuggedH3Bugged.cfg`**
   added: the BuggedH3 spec is bugged by construction (it omits
   `EvictingMapInsert`), so its "Fixed" config asserts only the
   structural invariants that hold independent of the bug — `TypeOK`.
   The `Bugged.cfg` continues to assert `H3HazardStateUnreachable` and
   demonstrate its violation. The H3 spec + its cfgs were renamed
   (dropping the shared `ChunkRaceReadersV2_` prefix) so that the
   verify gate's `ChunkRaceReadersV2*.cfg` glob no longer cross-runs
   the H3 cfgs against the canonical `ChunkRaceReadersV2.tla`, which
   does not define `H3HazardStateUnreachable`.

The new `StreamingBlobReadDuringWrite.tla` spec (described above)
captures the read-during-write protocol over the streaming_blob
primitive, including the post-#500 / post-#502 wire-shape defenses.

| Spec                                       | Cfg                                          | States   | Wall-clock | Outcome     |
|--------------------------------------------|----------------------------------------------|----------|------------|-------------|
| ChunkRaceReadersV2                         | ChunkRaceReadersV2Fixed.cfg                  | 221      | ~1 s       | clean       |
| ChunkRaceReadersV2                         | ChunkRaceReadersV2Bugged.cfg                 | 4        | <1 s       | violated    |
| ChunkRaceReadersBuggedH3                   | ChunkRaceReadersBuggedH3Fixed.cfg            | 199      | <1 s       | clean       |
| ChunkRaceReadersBuggedH3                   | ChunkRaceReadersBuggedH3Bugged.cfg           | 67       | <1 s       | violated    |
| StreamingBlobReadDuringWrite               | StreamingBlobReadDuringWriteFixed.cfg        | 795      | ~1 s       | clean       |
| StreamingBlobReadDuringWrite               | StreamingBlobReadDuringWriteBugged.cfg       | 24       | <1 s       | violated    |
| StreamingBlobReadDuringWrite               | StreamingBlobReadDuringWrite_BuggedSilentZero.cfg | 32       | <1 s       | violated    |
| StreamingBlobReadDuringWrite               | StreamingBlobReadDuringWrite_1w_3r_3c.cfg    | 17 740   | ~7 s       | clean       |
| StreamingBlobReadDuringWrite               | StreamingBlobReadDuringWrite_1w_4r_2c.cfg    | 70 743   | ~2 s       | clean       |

A second set of informational ChunkRaceReadersV2 configs
(`_2w_2r_3c`, `_2w_3r_2c`, `_3w_2r_2c`, `_3w_3r_3c`) explore larger
state spaces and exceed the 300 s gate timeout; they continue to
explore without surfacing violations up to ~1.5M states.

## Verification Gate (CI / pre-commit)

A two-layer gate enforces "every protocol change is paired with a TLA+
spec change." The gate scripts live at `scripts/verify_tla.sh` and
`scripts/check_protocol_diff.sh`; their unit tests live at
`scripts/tests/`. Run both layers locally with:

    just verify-tla         # Layer 1: SANY + TLC over every spec here
    just check-protocol     # Layer 2: changed-file × TLA-pair audit

### Layer 1 — `verify_tla.sh` (correctness of specs)

For every `<Name>.tla` in this directory:

1. SANY (parser + name resolution) must succeed.
2. For every accompanying `<Name>*.cfg`, TLC runs and the outcome must
   match the filename suffix:
   * `<Name>Bugged.cfg` / `<Name>Bug.cfg` / `<Name>V2.cfg` MUST
     produce a violation (an invariant or temporal property is
     violated). A clean run is a FAIL — the spec author labelled
     this config as bug-reproducing but the bug doesn't repro.
   * `<Name>Fixed.cfg` / `<Name>V1.cfg` MUST run clean
     ("Model checking completed. No error has been found."). A
     violation here is a FAIL — the spec author labelled this
     config as the fixed model but TLC found a counter-example.
   * Any other `.cfg` is run for informational purposes only.

The gate is **skip-clean** by default: if `tla2tools.jar` is not at
`/tmp/tla2tools.jar` (override via `TLA_TOOLS_JAR=...`), or if `specs/`
is empty, the script exits 0 with an informational message. CI passes
`--strict` to convert these into hard configuration errors.

Per-spec wall-clock cap defaults to 120 s (set via `TLC_TIMEOUT=...`).

### Layer 2 — `check_protocol_diff.sh` (specs accompany code)

Reads a list of changed files on stdin (typical caller:
`git diff --name-only origin/main..HEAD`) and asserts that any
**protocol-relevant** change is accompanied by at least one `.tla`
modification or a commit-message waiver.

**Hard triggers** (any one alone fires the gate):

* `nativelink-service/src/{worker_api_server,cas_server,bytestream_server}.rs`
* `nativelink-worker/src/{local_worker,running_actions_manager}.rs`
* any `*.proto` under `nativelink-proto/`
* `nativelink-util/src/store_trait.rs`

**Soft triggers** (WARN-only by default, escalate to HARD when
`--diff-file PATH` shows a new `pub fn` or `pub trait`):

* `nativelink-store/src/*_store.rs`

**Waiver format** (in commit message body, parsed from the file at
`$COMMIT_MSG_FILE`):

    [no-tla-needed: <one-sentence rationale>]

Acceptable rationales describe why the change does NOT cross a
component boundary (e.g. "pure cosmetic — error message text only";
"rename of a private helper"; "logging-only addition").

### Why a two-layer split

Layer 1 catches **broken specs** (the TLA+ author claimed Bugged.cfg
demonstrates a bug, but TLC ran clean — the trace was lost or the
spec drifted away from the production code path). Layer 2 catches
**missing specs** (a code change crossed a component boundary but
no `.tla` was touched). Either failure mode is enough to ship a bug
that the protocol layer was supposed to prevent; the gate enforces
both independently.

### Adding a new spec

1. Write `<Name>.tla` here. Use one of the existing specs as a
   template; the "Why TLA+ for NativeLink" section above explains the
   bug shape these specs target.
2. Add `<Name>Bugged.cfg` (must violate at least one invariant) AND
   `<Name>Fixed.cfg` (must run clean). The gate enforces both.
3. Run Layer 1 locally:

       TLA_TOOLS_JAR=/tmp/tla2tools.jar bash scripts/verify_tla.sh

4. Document the spec in the "Catalog" section above (one bullet per
   spec, naming the invariants and the bug class they cover).
