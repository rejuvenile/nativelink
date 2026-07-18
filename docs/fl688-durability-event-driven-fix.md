# FL-688 never-BIS-acked durability pin-leak — event-driven fix (design)

Status: **RECONSIDER-PREMISE (design cadre 2026-07-17, `.claude/reviews/e546af44/`). DO NOT IMPLEMENT — rework required.**
Two convergent BLOCKs: (1) the durability-event seam crosses a CAS-data-plane ↔ WorkerApi/scheduler entry boundary
that has NO wire today, and the design commits to neither side (worker→server per-digest signal [=Stage-A-removed
re-advertise, must prove bounded fan-out] vs a new server-side CAS-entry→scheduler-locality dependency) — ARCHITECTURAL,
needs operator sign-off; (2) `MarkStableAsyncDurability.tla` ACTION 5 RE-FUSES the cross-entry hop into one atom (sets
serverDurable + reads serverLocality + appends stableDigestsQueue) AND models serverLocality as MONOTONIC — the same
fusion class this spec exists to prevent, so "Fixed passes" is NOT proof. MAJOR: the leak condition is
`durable ∧ held ∧ no-pending-BIS` (not "named short-circuit"); the `has_durably`-false-negative-at-advert class + the
slow-write-FAIL trigger (batch-A PinLifecycle) are both in-scope. RECOMMENDED direction (red-team framing #2): hook the
durability re-check to the EXISTING replay-until-acked reader (`local_worker.rs:4455`, one worker-side choke point, no
new cross-entry wire, subsumes all triggers); re-do the spec with a cross-entry signal queue + a `LocalityEvicted`
action before re-cadre. CONFIRMED SOUND: ≥2-replica gate (`has_durably`=slow-tier only) + no forbidden primitives.

**AUDITOR (2nd reviewer, FIX-FIRST — convergent, see `.claude/reviews/e546af44/DECISION.md`): the DIAGNOSIS is INCOMPLETE.**
All cites + both TLC outcomes reproduced verbatim, BUT the causal bridge is unproven: nobody cited the concrete
SERVER-side path that makes a digest durable-for-the-FIRST-TIME without firing the pusher (all 4 fresh-durable FSS arms
DO push — `fast_slow_store.rs:1840-1851`). Spec ACTION 5 AXIOMATIZES that unproven premise. Decisive lead:
`pin_deferred_output_digest` (`running_actions_manager.rs:~2340`) says the F2 deferred slow-store write "is the
AUTHORITATIVE upload and BYPASSES `FastSlowStore::update`" → the pusher may be bypassed for the WHOLE F2 deferred path
(leak F2-UNIVERSAL, not AlreadyExists-specific) — but the live leak is a MINORITY, so the majority is BIS-acked by some
path not yet identified. **REQUIRED BEFORE ANY FIX (supersedes the seam decision): trace a LEAKING digest's server-side
durability path — which path BIS-acks the majority, why the minority skips the pusher — and verify the F2-bypasses-FSS
claim. Re-diagnose (instrument/trace), THEN choose the seam + re-spec + re-cadre.**

**DIAGNOSIS COMPLETE 2026-07-17 (re-diagnosis agent a0d734f0 — CODE-PROVEN + empirically confirmed on 8 live leaking digests, `/tmp/fl688-rediagnosis-evidence.log`). SEAM LOCATED. Both cadre BLOCKs resolved.**
- **Complete chain:** worker pins D indefinitely (`running_actions_manager.rs:7803`) → streams D to `slow_store.update`/`update_oneshot` (`:8200`/`:8157`, a GrpcStore to the server — the WORKER's own FSS IS bypassed, confirmed) → server CAS chain `WorkerProxyStore→VerifyStore→ExistenceCacheStore→SizePartitioning→FSS` (bytestream already-exists short-circuit is carved out for workers, `bytestream_server.rs:3815` `!is_worker&&!is_mirror`, so the write ENTERS the chain) → **`ExistenceCacheStore::update` durable-skip (`existence_cache_store.rs:607-631`; `update_oneshot` `:774-790`): `should_skip_for_durable_presence`→`inner.has_durably`=Some → drains reader, refreshes cache, returns Ok WITHOUT `inner_store.update`** → `FastSlowStore::update` never reached → `stable_digests_pusher` never fires (all 5 producers live inside FSS `update`/`update_oneshot`/chunked/self-retry/`mark_stable`, `fast_slow_store.rs:1840-1851`) → NO BIS → the pin is orphaned (the only BIS for D fired in the PAST, before this worker pinned).
- **Necessary condition (the leaking minority):** D was ALREADY durable on the server AND already resident in the worker's own fast FilesystemStore when the action re-produced it. Already-durable-on-server → the ExistenceCacheStore durable-skip → no fresh pusher. Already-resident-locally → re-produce dedup-skips with no fresh `on_insert` (`filesystem_store.rs:2410-2422`,`:1560-1565`); `on_get` re-registers the FROZEN original stamp (`:1595-1601`, LWW tie) → no new-stamp holdings delta → the existing `has_durably`-gated `mark_stable` re-drive on each `BlobsAvailable` tick (`worker_api_server.rs:3569-3583`) MISSES it (delta-based; these emit no fresh delta) → heals only via the ungated full snapshot (`:3187`) on reconnect/over-cap (don't fire while connected + below pin-cap → 30min–6.5h ages). The BIS-acked MAJORITY is the complement (worker is FIRST to make D durable → `has_durably`=None → write flows through FSS::update → pusher fires → global BIS reaches the pin holder).
- **F2-bypasses-FSS verdict:** worker-side FSS bypass CONFIRMED (worker writes direct to `cas_store.slow_store()` GrpcStore → why it uses indefinite pins not the `failed_slow_writes` backstop). Server-side FSS bypass REFUTED-as-universal: the upload normally DOES reach the server FSS::update (majority BIS-acked); it bypasses ONLY when the ExistenceCacheStore durable-skip fires (already-durable minority). **This resolves the cadre's contradiction.**
- **THE FIX SEAM (resolves cadre BLOCK-1):** `ExistenceCacheStore::update` + `::update_oneshot` durable-skip branch (`existence_cache_store.rs:613-631`, `:778+`) — the exact point the server KNOWS D is durable while processing this worker's upload yet drops the pusher. **Fix: in the skip branch call `self.inner_store.mark_stable(&[digest])` before returning Ok** — `ExistenceCacheStore`'s `mark_stable_delegation` already forwards to inner (`:1079` → FSS `mark_stable` → `push_stable_digests` → global BIS). Idempotent, event-driven (fires ON the upload = the durability event), ≥2-replica-preserving (mark_stable is has_durably-gated by construction — the skip fired BECAUSE has_durably=Some), NO locality gate (global BIS unpins every holder → the spec's monotonic-locality concern is MOOT for this fix), NO new cross-entry wire, NO TTL. The bytestream `!is_worker` carve-out is NOT the fix site — it pushed the skip down to ExistenceCacheStore without adding a pusher there, which is why the leak survived that change.
- **Note re batch-A `PinLifecycle`:** that spec's slow-write-FAIL trigger is a DISTINCT path (reaches FSS::update, slow-write fails) — the LIVE 8-digest leak is entirely the ExistenceCacheStore durable-skip class, so PinLifecycle is a separate (possibly failed_slow_writes-covered) question, NOT part of this live fix.
- **One inferred link (optional to close):** that the worker's steady-state delta never re-lists these already-local digests at a stamp reaching the server mark_stable gate — code+empirically supported (heals only on full-snapshot); a temporary `debug!`/counter at `existence_cache_store.rs:613` (worker-upload durable-skips) or `worker_api_server.rs:3126-3144` (digest set) would show the drop-rate directly. NOT needed to justify the seam.
- **RE-CADRE 2026-07-17 (distsys+red-team CONVERGENT, parent-VERIFIED): the ECS fix (`b86bbbe3`) is at the WRONG SEAM for the observed leak — RECONSIDER.** The re-diagnosis conflated the two upload transports. **All 8 live leaking digests are <1 MiB** → the worker F2 deferred upload routes them via `GrpcStore::update_oneshot` → **BatchUpdateBlobs** (`BATCH_THRESHOLD=1 MiB`, `running_actions_manager.rs:7874`/`:8150`, worker CAS grpc store does NOT override `batch_update_threshold_bytes`=1 MiB). The server `inner_batch_update_blobs` handler computes `has_results` UNCONDITIONALLY (`cas_server.rs:723`) and SKIPS already-present blobs UNCONDITIONALLY (`:744`, `if has_result.is_some() { return Ok }`) — **NOT `!is_worker`-gated** (unlike the ByteStream sibling `bytestream_server.rs:3815` which IS `!is_worker && !is_mirror`-gated so worker ByteStream uploads reach the chain). So a worker's already-durable ≤1 MiB upload is acked-and-skipped at `cas_server.rs:744`, NEVER reaches `ExistenceCacheStore`, and the fix's `mark_stable` never fires → **the ECS fix closes ZERO of the 8 observed leaks** (it covers only a hypothetical >1 MiB already-durable ByteStream re-upload, unrepresented live). `cas_server.rs` has NO mark_stable/BIS hook anywhere (grep=0). The ECS fix's ≤16KB-producer question was SOUND (both leaves are FastSlow producers) but MOOT — the upload never reaches ECS. The re-diagnosis's "empirically confirmed on 8 digests" confirmed durability+size, NOT the per-digest RPC handler — the disconfirming evidence (all <1 MiB) was in hand and unchecked.
- **REAL SEAM = the worker-facing already-present SKIP at the SERVICE layer, not the store chain.** `cas_server.rs:744` (BatchUpdateBlobs, ungated) is the one the 8 hit. Red-team's framing: fix the CLASS — every place the server tells a WORKER "you already have it, don't send it" is a durability-event site; the ByteStream carve-out (`:3815`) proves the project already knows this for one transport; the BatchUpdateBlobs handler was never given the hook. **DECISION NEEDED (mechanism):** (a) add the `has_durably`-gated `mark_stable`/BIS hook at `cas_server.rs:744` (+ keep the ECS hook for the >1 MiB class + AUDIT every worker-facing already-present skip for the missing hook); OR (b) red-team's cleaner redesign — carry the unpin on the worker-facing "already-durable" RESPONSE the worker already receives (uniform, no skip-site can be missed, and it sidesteps the always-fire global-BIS amplification MAJOR). The V2 spec also needs a transport/skip action that can MISS ECS (it currently axiomatizes every upload reaching ECS — same class as V1's ACTION-5). **DO NOT LAND `b86bbbe3` as the FL-688 fix.** Full re-cadre findings: `.claude/reviews/b86bbbe3/`.

## THE FIX — advertisement-driven (operator design; flow-trace VALIDATED 2026-07-17, agent a4e173e9)
The durability event is the ADVERTISEMENT, not the upload. Worker advertises D via `BlobsAvailable` → server reconciles: `has_durably`=Some → `mark_stable`→BIS; missing → pull→pusher→BIS; pull-fail → retry→BIS. **Flow trace confirmed ALL of this already exists** except one new mechanism:
- **GAP-1 = the whole leak, CONFIRMED:** the `BlobsAvailable` delta is fed ONLY by the `BlobChangeTracker` (`on_insert`/`on_get`/evict, `local_worker.rs:2404-2430`); `pin_key_with_mode` (`moka_evicting_map.rs:1562-1695`) moves the entry to the `pinned` DashMap and NEVER fires the ItemCallback → `indefinite_pinned` ⟂ tracker set → a re-produced already-resident output emits NO delta (dedup-skip returns Ok before insert/get, `filesystem_store.rs:1560-1566`) → goes dark in steady state (heals only on the reconnect full-snapshot, which enumerates both `cache` AND `pinned`, `moka:2084-2097`). **THE ONLY NEW WIRING = advertise-on-pin.**
- **COUPLING-2 CONFIRMED:** server `request_missing_blob_uploads` (`worker_api_server.rs:3569-3609`) runs BOTH branches — `has_durably`-gated `mark_stable`→BIS ALWAYS (not cooldown-gated) + `UploadMissingBlobs` pull (5s cooldown). In-flight writes correctly distinguished (`has_with_results` RAM-inclusive for pull; `has_durably` slow-only for mark_stable). So an advertised already-durable D WOULD BIS.
- **COUPLING-3 CONFIRMED-HAS-HOLE but OFF-PATH:** the ECS durable-skip (`existence_cache_store.rs:613-630`) bypasses the pusher for already-durable re-uploads → **the design must NOT rely on the re-upload's server write for durable blobs; it must lean on COUPLING-2 (`mark_stable`).** Missing-blob pull→pusher→BIS works end-to-end.
- **COUPLING-4 CONFIRMED (the "no server retry" memory is STALE):** FSS slow-write Err → `failed_slow_writes` + RAM re-pin; drain loop #287 (`nativelink.rs:1587`) → V3 self-retry (`fast_slow_store.rs:3436-3533`) → on durable success `push_stable_digests`→BIS; on fast-tier miss re-solicits UploadMissingBlobs. Terminates in BIS. Residual: TODO(#289) — a failed digest with NO connected worker + no fast-tier bytes re-inserts forever (data NOT lost, worker pinned); advertise-on-pin IMPROVES this (re-registers the worker in locality).
- **THE FIX = (1) advertise-on-pin** feeding the same `BlobChangeTracker`/`digest_infos` channel (new wiring at the pin site `running_actions_manager.rs:7803`, which does NOT hold the tracker → must thread a handle) + **fire-once dedup** (avoid per-tick re-advertise); **(2) rely on the existing server `has_durably`→`mark_stable`→BIS** (do NOT hook the upload seams — b86bbbe3/cas_server:744 dropped); **(3) NO sweeper** (event-driven; reconnect full-snapshot + startup + `replay_unacked_chunks` cover connection loss).
- **TRIPWIRE ALREADY EXISTS (design-cadre auditor 2026-07-17 REFUTED the flow-trace's "may not exist" — build NOTHING).** `sweep_stale_indefinite_pins` (`moka_evicting_map.rs:2438`) emits `warn!(target: "nativelink::stale_pin_alert", "indefinite pin not BIS-acked after 30s")` at `STALE_INDEFINITE_PIN_ALERT_SECS=30`, releases nothing, runs once per `pin_check_interval` tick (a DISTINCT function from `expire_stale_pins`), 4 tests (`stale_pin_alert_*`). This IS the earlier live-observed 550-WARN signal; it survives on current main. The flow-trace grepped one guessed identifier (a negative-claim trap), not the behavior. The fix REFERENCES this existing tripwire; building a duplicate would double-WARN. NOTE a benign 30s false-positive: a legitimately-not-yet-durable pin whose slow-write exceeds 30s also alerts → read it as a sustained-set/rate signal, not a per-pin defect.
- **STANDING FIX-FIRST (auditor) + open for pair-a:** advertise-on-pin re-introduces a worker→server per-digest re-advertise of pins — the auditor flags this as potentially the FL-688-v3-Stage-A-removed re-advertise (architectural, bounded fan-out UNPROVEN, needs operator sign-off + the fire-once dedup SPEC'd). REBUT to verify: Stage A removed a PERIODIC AC-PIN-registry heartbeat (AcPinResync, a DIFFERENT registry) for O(N)-spurious convergence; advertise-on-pin is EVENT-driven-ONCE per new CAS F2-output pin (fire-once dedup → O(new pins), not O(N)/tick) — likely NOT the same mechanism. Chesterton's-Fence-confirm the Stage-A removal target before implementing; SPEC the fire-once dedup bound in the V3 spec.
- **NEXT:** re-model `MarkStableAsyncDurabilityV3` around advertise-on-pin (worker advertises the pinned blob; server has_durably→BIS; the dark-in-steady-state / heals-on-reconnect condition); implement (advertise-on-pin wiring + fire-once + the tripwire); re-cadre. `b86bbbe3` (ECS hook) is dropped.
Companion formal spec: `specs/MarkStableAsyncDurability.tla` (+ `…Bugged.cfg`,
`…Fixed.cfg` — NOTE the ACTION-5 refusion + monotonic-locality gaps above). Backlog: `deferred_tasks.md`
`[NARROWED 2026-07-17]` block (under the pin-saturation section).

---

## 1. Code-proven root cause

A worker takes an **indefinite** pin on every F2 output, released **only** by a
`BlobsInStableStorage` (BIS) ack:

- `nativelink-worker/src/running_actions_manager.rs:7790-7810` —
  `pin_digest_indefinite_with_result` (FL-688 v3 Stage B: indefinite-only,
  held-until-BIS; the 120 s TTL fallback was removed because deferred-mode F2
  bypasses `in_flight_slow_writes`/`failed_slow_writes`).
- The indefinite / TTL-exempt pin class is `PinInfo.indefinite` in
  `nativelink-util/src/moka_evicting_map.rs` (~:122-129) — deliberately exempt
  from the `PIN_TIMEOUT_SECS` sweep.

The **primary** BIS release path is correctly **event-driven**:

- fresh `FastSlowStore::update` completion → `push_stable_digests_via_arcs`
  (`nativelink-store/src/fast_slow_store.rs:1490-1502`) → `stable_notify`
- → drain-then-fire BIS loop (`src/bin/nativelink.rs:1191-1236`)
- → `broadcast_blobs_in_stable_storage_chunked` → worker unpin.

**The leak is the one durability class that bypasses the pusher:** the
`AlreadyExists` short-circuit at
`nativelink-worker/src/running_actions_manager.rs:8284-8302`. Its own comment
states it verbatim:

> "AlreadyExists means the slow tier short-circuits `FastSlowStore::update`
> without invoking `stable_digests_pusher` → no BIS chunk will be broadcast for
> this digest from THIS write."

So a worker's deferred upload of output D that finds D **already durable** on the
server drives **no** durability event. The **only** other release trigger is the
one-shot `has_durably` poll at the `BlobsAvailable` advertisement
(`nativelink-service/src/worker_api_server.rs:3560-3609`) — it runs `has_durably`
at handling time and, if durable, calls `mark_stable`. It is **not re-driven**:
the periodic re-advertise timer was removed in FL-688 v3 Stage A
(`nativelink-worker/src/local_worker.rs:4440-4459`).

**Failure interleaving (advertise-before-durable + AlreadyExists):**

1. Worker writes + indefinitely pins D.
2. Worker sends its one-shot `BlobsAvailable` advert for D.
3. Server handles the advert: runs `has_durably(D)` — **not yet durable** → miss,
   no `mark_stable`. Locality now records D. The advert is drained (one-shot).
4. D later becomes durable via the `AlreadyExists` short-circuit → **no pusher,
   no BIS**, and the advert poll already fired and will not re-fire.

Result: the indefinite pin is stuck **forever** (observed live: 6.7 h, on
all-durable digests).

**Verification note (no rubber-stamp):** every cite above was read against
current source. The root cause as stated in the dispatch and in the
`[NARROWED 2026-07-17]` backlog block is confirmed exactly — including that the
primary fresh-write path is already correct and that the leak is specifically the
pusher-bypassing short-circuit. One nuance surfaced while formalizing (see §5):
the naive invariant `Quiescent ⇒ workerPins = {}` is **too strong** and must be
gated on durability, because a not-yet-durable pin is *correctly* held.

## 2. The fix (operator-confirmed, event-driven)

The durability **event** — a blob becoming, or being confirmed, in stable storage
(which `AlreadyExists` **proves**) — must **check the locality map** and, if the
blob is still held by a worker, **emit a BIS** for it by driving the **same**
`has_durably`-gated `mark_stable` the fresh-write path drives. Applied to **all**
pusher-bypassing durability short-circuits, not just the fresh-write path.

- **Release stays gated on `has_durably`**, never on `AlreadyExists` alone — so
  the ≥2-replica durability invariant is preserved (the server unpin oath fires
  only when the server holds a durable copy; see
  `worker_api_server.rs:3555-3609`).
- **Event-driven, not polled. No TTL / pin-expiry** is admissible — release must
  come from a real BIS ack (forbidden otherwise by CLAUDE.md and the operator).

This is the previously-scoped option (c) — "faster BIS release" — **generalized
to all durability short-circuits and made event-driven**, which is strictly
narrower than the budget-sizing / NAK-boundary options.

### Sibling short-circuits to cover (audit all before implementing)

Every synchronous-success path in the worker upload that returns `Ok`/`break true`
**without** invoking the pusher is a candidate leak site:

- `AlreadyExists` short-circuit — `running_actions_manager.rs:8284-8302`
  (confirmed live leak).
- `content_is_immutable` / skip-write paths, if any bypass the pusher.
- dedup-drop (a digest de-duplicated against an in-flight/complete write).
- any other `Code::AlreadyExists` classification
  (`classify_upload_error` → `UploadRetryDecision::AlreadyDurable`,
  `running_actions_manager.rs:2108`, `:10188-10194`).

The fix must be applied at the **durability-event seam** common to all of them,
not patched per-call-site, so a future short-circuit inherits the release path.

## 3. Invariant-walk (5-slot template)

- **Invariant being violated:** every indefinitely-pinned worker output is
  eventually BIS-acked and unpinned once the server holds it durably
  (`BIS-acked ⇐ has_durably`).
- **Mechanism that violates it:** the `AlreadyExists` short-circuit reaches
  server-durable without invoking the pusher
  (`running_actions_manager.rs:8284-8302`), combined with the one-shot,
  never-re-driven `has_durably` advertisement poll
  (`worker_api_server.rs:3560-3609`; re-advertise timer removed at
  `local_worker.rs:4440-4459`). Advertise-before-durable → the poll misses and
  nothing re-checks.
- **Mechanism that re-establishes it after the fix:** all pusher-bypassing
  durability short-circuits drive the same `has_durably`-gated, locality-checked
  `mark_stable`/BIS the fresh path drives (`push_stable_digests_via_arcs`
  → `stable_notify` → BIS loop `src/bin/nativelink.rs:1191-1236` → unpin),
  event-driven.
- **Composite invariants this fix interacts with:**
  - ≥2-replica durability — release stays gated on `has_durably`
    (`worker_api_server.rs:3555-3573`), so no early unpin.
  - no-TTL / no pin-expiry — release remains a real BIS ack; the indefinite pin
    stays TTL-exempt (`moka_evicting_map.rs` `PinInfo.indefinite`).
  - the BIS drain-then-fire loop topology (`src/bin/nativelink.rs:1191-1236`)
    — the new event feeds the *same* queue; no second broadcast path.
  - the pin admission/eviction triangle (indefinite-pin cap headroom) —
    faster release *relieves* cap pressure; the fix does not add pins.
- **Test that proves the fix:** worker advertises an F2 output once; the server
  is **not** durable at that instant; the server **later** becomes durable via an
  `AlreadyExists` path (no fresh-write pusher); assert the pin releases within N
  ticks. No such test exists today (the pin currently never releases).

## 4. Formal spec results

`specs/MarkStableAsyncDurability.tla` de-fuses the atom that hid this bug. The
predecessor `MarkStableViaBlobsAvailable.tla` set `serverCache' = serverCache ∪
{d}` at **write** time (`:156`), making "advertise-before-durable" unreachable and
`EveryPinReachesRelease` **vacuously** true — so the FL-688 v3 Stage A removal of
the re-advertise timer left that spec green while the leak shipped.

Per the CLAUDE.md TLA+ atomicity discipline, the new spec has seven indivisible
actions with the intermediate state explicit:

1. `WorkerWriteAndPin(d)` — worker-only; adds an indefinite pin, **not**
   server-durable.
2. `WorkerAdvertise(d)` — one-shot `BlobsAvailable` (never re-queued).
3. `ServerHandleAdvert` — delivery-ack that **drains** the advert; registers
   locality and runs the one-shot `has_durably` poll.
4. `ServerBecomesDurableFreshWrite(d)` — async slow-tier landing that invokes the
   pusher (correct path; drives BIS in both regimes).
5. `ServerBecomesDurableShortCircuit(d)` — the `AlreadyExists` path; reaches
   durable **without** the pusher. **The leak site.**
6. `DrainAndBroadcast` — BIS drain-then-fire.
7. `BISDeliverToWorker(d)` — worker unpin.

Toggle `EventDrivenOnShortCircuit`:

| cfg | toggle | checked | TLC outcome |
|---|---|---|---|
| `…Bugged.cfg` | FALSE | `TypeOK`, `StableFeedIsDurable`, `DurableHeldReleasedAtQuiescence` (inv); `EventuallyAllReleased` (prop) | **`DurableHeldReleasedAtQuiescence` VIOLATED** (depth-8 CE) |
| `…Fixed.cfg` | TRUE | same | **"No error has been found"** (528 states, temporal holds) |

**Bugged counterexample (the real FL-688 leak):** write+pin d1,d2 → advertise
both → server handles both adverts while `serverDurable = {}` (one-shot poll
misses) → `ServerBecomesDurableShortCircuit(d1)` makes d1 durable with **no BIS**
→ quiescent/deadlock state with d1 **durable and still pinned**. (Logs:
`/tmp/fl688-spec-bugged2-*.log`, `/tmp/fl688-spec-fixed-*.log`.)

**Composite-invariant preservation, proven in both regimes:**
`StableFeedIsDurable` (nothing is fed to `mark_stable`/BIS unless the server is
already durable for it) **holds in both** Bugged and Fixed — i.e. the fix does
**not** weaken the ≥2-replica gate; release still implies prior durability. The
model has **no** TTL/expiry action: release comes solely from the BIS chain.

**Why `Quiescent ⇒ workerPins = {}` is deliberately NOT the checked invariant:**
a quiescent state where the server is not yet durable for a held digest leaves the
pin *correctly* held (only copy — releasing would violate ≥2-replica). The
load-bearing predicate is `DurableHeldReleasedAtQuiescence` (durable **and** held
⇒ not pinned); the durability antecedent is what makes it the FL-688 contract and
not a spurious alarm. `PinReleasedAtQuiescence` is left defined but unchecked with
this rationale in-spec.

## 5. TDD test plan

Red-first, against the production composition (WorkerProxyStore → VerifyStore →
ECS → SizePartitioning → `cas_FAST_SLOW` FSS), not a unit corner:

1. **Composite leak test (currently never passes):** worker writes + indefinitely
   pins output D; worker advertises D **once**; assert server `has_durably(D)` is
   `None` at that instant (advert poll misses). Then drive D durable via an
   `AlreadyExists` short-circuit path (no fresh-write pusher). Assert the pin
   **releases within N BIS ticks**. Before the fix this hangs; after, it releases.
   Verify the mutation: comment the new durability-event → BIS hook and confirm
   the test fails with its bespoke "pin never released" message.
2. **≥2-replica guard test:** drive the short-circuit while the server is **RAM-only,
   not durable**; assert the pin is **NOT** released (release gated on
   `has_durably`, not `AlreadyExists`) — protects against the fix over-releasing.
3. **Sibling coverage:** parametrize test 1 across each pusher-bypassing
   short-circuit (`content_is_immutable` skip, dedup-drop) so a new short-circuit
   that skips the seam fails the suite.
4. **No-double-broadcast / idempotence:** short-circuit + fresh-write both fire for
   the same D; assert exactly-once effective unpin (BIS loop dedups; `unpin` is
   idempotent) and no locality drift.

Harness discipline: `#[nativelink_test]`, channels/barriers not sleeps, `timeout`
on `cargo test`.

## 6. Open questions for the cadre

- **Seam placement:** is there a single durability-event choke point that all
  short-circuits pass through (so the locality-check + `mark_stable` is written
  once), or must each call-site opt in? Prefer the former.
- **Locality lookup cost:** the durability-event hook adds a locality-map read per
  short-circuit; confirm it is off the hot fresh-write path and cheap.
- **Reconnect-snapshot interaction:** the reconnect full-snapshot re-advertise is a
  separate event-driven convergence path (out of scope here). Confirm the new hook
  and the snapshot do not double-emit (BIS dedups, but verify locality doesn't
  drift).
