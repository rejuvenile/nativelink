# Design v2: Redis ↔ ExistenceCache Consistency

**Status**: design under revision; **NOT yet implementation-ready** per multiple reviewer BLOCKERs (see §0.2). Tracked under tasks #100 (AC store eviction callbacks) and #102 (existence-cache phantom-positive consistency hole).

**Authored** 2026-04-25 by sub-agent + iterated with 7 reviewer reports (code-reviewer, perf-optimizer, security-reviewer, code-simplifier, testing-czar, rust-crate-reviewer, red-team). Original v1 was an inline reply; this v2 folds the reviewer feedback into a single durable doc and reframes the premise per red-team's challenge.

---

## 0. Reframing the problem (red-team's premise demolition)

### 0.1 The original framing was wrong

v1 claimed: "Production wedge for digest f4c899f8...-1667 is caused by Redis `allkeys-lru` evicting the bytes ~21 hours after upload while the ExistenceCacheStore positive entry survives." This was based on the lifecycle-investigation sub-agent's diagnosis.

**Red-team falsified this.** Live Valkey state at investigation time:
- `evicted_keys: 0`
- `expired_keys: 0`
- `used_memory: 548 MB / system 251 GB`
- `maxmemory-policy: allkeys-lru`

**Valkey has had ZERO evictions in this server's lifetime.** The wedge mechanism cannot be Redis LRU on this host. The lifecycle agent's correlation was coincidence.

### 0.2 What we actually know

The wedge symptom is real (server `bytestream_server.rs:2058-2065` "skipped, blob already exists" persists across server restarts for blobs that are demonstrably absent on disk + Redis + workers). But the CAUSE of the lying ExistenceCache positive remains undiagnosed. Possible mechanisms:
- Process restart while a fast-write succeeded but slow-write hadn't yet → MemoryStore lost, FilesystemStore write didn't flush, ExistenceCache not yet populated (so this scenario doesn't fit either).
- A separate code path inserts into ExistenceCache without going through inner-store verification.
- An admin tool issues `DEL` directly on Valkey out-of-band.
- Eviction-callback path is not reaching ExistenceCacheStore for some non-LRU reason.

**Phase 0 (instrument-first) supersedes Phase 1.** Before designing eviction-driven invalidation, deploy targeted instrumentation (one `info!` log on every Redis DEL/EXPIRE/eviction-callback invocation, plus one log on every ExistenceCache positive insert with the call-stack tier). Reproduce the wedge, identify the actual mechanism, then design.

### 0.3 What this design IS still useful for

Even if today's wedge isn't Redis-LRU, the consistency invariant is real and currently unenforced. Today `RedisStore::register_item_callback` is a no-op (`redis_store.rs:1608-1614`). When the system DOES start hitting Redis eviction (under sustained ingestion pressure approaching `maxmemory`), this design closes the gap. Treat as defensive infrastructure with deferred urgency, not a fix for the active wedge.

---

## 1. Architecture (revised)

**Primary mechanism**: push via Redis keyspace notifications, per-RedisStore listener task, fan-out through the existing `ItemCallback` interface.

**Secondary mechanism (Scenario 3 — Redis restart)**: SCAN-based reconcile triggered by `RedisManager` reconnect, using a stream-process bloom-filter approach (NOT the 3.2GB HashSet from v1 — see §4.3).

### 1.1 Critical fix from perf-optimizer: single-consumer push channel

v1 said "drains a dedicated mpsc::UnboundedReceiver<PushInfo> for keyevent traffic." **Wrong.** `redis_store.rs:344` declares `subscriber_channel: Mutex<Option<UnboundedReceiver<PushInfo>>>`. The redis crate's `push_sender` (lines 583, 687) supports exactly **one** sink. `RedisSubscriptionManager::new` does `subscriber_channel.lock().take()` (line 2012), consuming it. There is no second receiver to make.

**Revised approach**: introduce a **fan-out task** that owns the single `subscriber_channel`, classifies each `PushInfo` by channel/pattern, and forwards to per-consumer bounded mpsc channels:
```
single push_sender → subscriber_channel (existing)
                  → fan-out task (new) classifies by pattern
                    ├─→ existing scheduler subscription consumer
                    └─→ new eviction listener consumer (bounded mpsc 16K)
```

This is +120 LOC over v1's "+~80 LOC" estimate. The fan-out task replaces `RedisSubscriptionManager` as the direct subscriber owner; the existing scheduler subscription consumer becomes one of the fan-out's downstream consumers.

### 1.2 ItemCallback fan-out hazard (perf-optimizer concern #2)

v1 said "iterate `Vec<Arc<dyn ItemCallback>>` and `cb.callback(...).await`". `ItemCallback::callback` returns a `Future<Send>`; `ExistenceCacheCallback` does `existence_cache.remove().await` (Moka write). Holding the `parking_lot::Mutex<Vec<...>>` across the `.await` is a CLAUDE.md violation and risks deadlock.

**Fix**: clone the `Arc<Vec<...>>` snapshot under the lock, drop the guard, then iterate.

### 1.3 Critical fix from security-reviewer: callback panic catch

A panicking `ItemCallback::callback` aborts the listener task, **silently disabling all future invalidations** — re-creating the bug we're trying to fix. **Mandatory**: wrap each callback dispatch in `tokio::task::spawn` + `AssertUnwindSafe(...).catch_unwind()`; on panic, `error!` log with digest, increment counter, continue. Acceptance test #5 covers this.

### 1.4 Idiom corrections (rust-crate-reviewer + code-simplifier)

- Use `tokio::sync::OnceCell` for `eviction_listener_started`, NOT `AtomicBool` — `redis_store.rs:341,440` already uses this pattern for `subscription_manager`.
- mpsc must be **bounded** (`tokio::sync::mpsc::channel(16_384)`), NOT unbounded. The existing `subscriber_channel` is unbounded — that's a known footgun, don't replicate. On `try_send` overflow: drop the event, increment counter, schedule a forced Phase-2 reconcile.
- `JoinHandleDropGuard` from `nativelink_util::task` (already imported at `redis_store.rs:44`) for listener lifecycle. Mirror `RedisSubscriptionManager`'s `_subscription_spawn: Arc<Mutex<JoinHandleDropGuard<()>>>` pattern at `:1865`.

### 1.5 Drop the `enable_eviction_notifications` bool (code-simplifier)

v1 proposed an opt-in config flag. **Replace with**: probe Valkey at startup (`CONFIG GET notify-keyspace-events`); if it lacks `E`+(`g`|`x`|`e`), `error!` and **refuse to start** (red-team: silent-degradation re-creates the wedge invisibly). Operator must enable it before this code can run.

---

## 2. Invalidation flow (revised)

```
Redis evicts/expires/deletes key "cas:abc...-1234"
  → Valkey emits __keyevent@1__:evicted "cas:abc...-1234" (or :expired, :del)
  → push_sender fires PushInfo into single subscriber_channel
  → fan-out task receives PushInfo
    - classifies pattern: __keyevent@<db>__:* → eviction listener
    - sends to bounded eviction mpsc (16K cap; on full: drop+counter+schedule reconcile)
  → eviction listener task receives PushInfo
    - extracts last data element (the key body)
    - STRICT prefix match: str::strip_prefix("cas:") → Option
      - None: warn! + skip (DB cross-pollution defense)
    - parses body into DigestInfo via try_from
      - Err: warn! + skip (malformed-key defense)
    - acquires Vec<Arc<dyn ItemCallback>> snapshot under lock, drops lock
    - for each cb: spawn tokio task wrapped in AssertUnwindSafe + catch_unwind
      - on Ok: increment success_count
      - on panic: error! + increment panic_count + continue
  → ExistenceCacheCallback::callback (existence_cache_store.rs:117-135)
    upgrades Weak, calls ExistenceCacheStore::callback
    (existence_cache_store.rs:93-108) → existence_cache.remove(&digest)
  → Next has() for that digest misses cache → re-queries Redis → NotFound
    → bytestream_server.rs:2059 correctly does NOT skip the upload
```

---

## 3. Sibling concerns (audit-driven)

### 3.1 Both Redis DBs (red-team)

The patch is per-RedisStore-instance. Production has TWO RedisStores:
- `REDIS_AC_STORE` @ `db=0` `key_prefix:"ac:"`
- `REDIS_CAS_SMALL_STORE` @ `db=1` `key_prefix:"cas:"`

Each instance subscribes to its own `__keyevent@<db>__:` pattern. AC store doesn't currently wrap an ExistenceCache (only CAS does), but the listener fires regardless — when AC ever gets ExistenceCache wrapping, it works. **Task #100** tracks AC specifically.

### 3.2 Sibling caller missed (code-reviewer)

`OntapS3ExistenceCacheStore::new` at `nativelink-store/src/ontap_s3_existence_cache_store.rs:371` registers an `OntapS3CacheCallback` against its inner store via the same `register_item_callback` trait. If THAT inner is ever a `RedisStore`, the same fix benefits it for free. Document but no extra work needed.

### 3.3 Wrapper-store fanout (code-reviewer confirmed)

`FastSlowStore`, `CompletenessCheckingStore`, `RefStore`, `ShardStore`, `SizePartitioningStore`, `VerifyStore`, `CompressionStore`, `WorkerProxyStore` all forward `register_item_callback` recursively. The Redis change reaches Redis through any combination — no wrapper edits needed.

### 3.4 MemoryStore eviction (red-team — pre-existing bug, not our scope)

`MemoryStore::register_item_callback` IS implemented (line 423-430). When MemoryStore in `cas_FAST_SLOW_STORE` evicts a large blob, it fires the callback — **but FilesystemStore still has it.** Today, ExistenceCache loses a valid entry, next has() pays a FilesystemStore RTT, re-inserts. This is a pre-existing bug the cache should only drop when *the bottom of the chain* loses the blob. **Out of scope; file separately.**

### 3.5 ABA hazard (red-team)

Worker uploads X → Redis stores X → Redis evicts X → notification queued → Worker re-uploads X → ExistenceCache populated → notification finally arrives → ExistenceCache wrongly drops valid entry → next has() pays Redis RTT.

Idempotent? Yes (cache repopulates on next has()). Free? No (one wasted RTT per ABA + log noise).

**Mitigation**: store `(epoch, size)` in ExistenceCache value; eviction callback only removes if cache's epoch matches the eviction's epoch. v1 dismissed epochs as too expensive at 50M entries × 8B = 400MB; reconsider per-store rather than global. Defer to Phase 2.

---

## 4. Phasing (revised)

### 4.1 Phase 0 — INSTRUMENT FIRST (red-team blocker)

- Add `info!` at every Redis DEL/EXPIRE/eviction-callback invocation
- Add `info!` at every ExistenceCache positive insert with call-stack tier source
- Deploy for 24-48h; reproduce wedge; identify actual mechanism
- **DO NOT proceed to Phase 1 until Phase 0 confirms eviction is the trigger**

### 4.2 Phase 1 — push-based invalidation (~200 LOC, was 120)

- Fan-out task replacing `RedisSubscriptionManager` as direct subscriber owner
- Per-consumer bounded mpsc (scheduler + eviction)
- Eviction listener: psubscribe `__keyevent@<db>__:expired,evicted,del`, parse, prefix-strip, fire callbacks via spawn+catch_unwind
- Startup probe: `CONFIG GET notify-keyspace-events` → refuse start if missing flags
- Files: `redis_store.rs` (major), `nativelink-config/src/stores.rs` (NO new bool flag — drop)
- Closes Scenario 1 IF Phase 0 confirms eviction is the cause

### 4.3 Phase 2 — SCAN-based reconciler on reconnect (~120 LOC, was 80)

- Stream-process SCAN cursor pages directly into a Bloom filter (60MB at 50M entries / 1% FPR), NOT a 3.2GB HashSet
- Walk MokaEvictingMap via `iter()` (per-segment lock-free per rust-crate-reviewer); for each key, check bloom; on miss with high confidence, remove
- Tolerate 1% false-keep rate (those self-heal on next has() RTT)
- Triggered by `RedisManager::reconnect_notify()` — trait method default returns never-fired Notify (so MockRedisManager compiles unchanged)
- Files: `redis_store.rs` (manager hook), `nativelink-util/src/moka_evicting_map.rs` (no new helper — Moka's `iter()` works per rust-crate-reviewer)

### 4.4 Phase 3 — overflow-triggers-reconcile glue (gating)

The bounded mpsc's overflow-handler must trigger Phase 2's reconcile. **Phase 1 cannot ship without Phase 3** (Phase 1 with silent overflow = the same wedge invisibly). Phase 3 is small (~20 LOC) but a hard gate.

---

## 5. Testing plan (testing-czar's expanded list)

v1 had 7 tests. testing-czar identified 5 more. Total 12, plus infrastructure work.

### 5.1 Required tests

1. `redis_evicted_key_invalidates_existence_cache` — basic happy-path
2. `bytestream_write_does_not_skip_after_redis_eviction` — end-to-end production wedge mechanism
3. `notify_keyspace_events_disabled_refuses_start` — startup probe (was "callback disabled" in v1; now refuse-to-start)
4. `events_lost_during_pubsub_gap_recovered_by_reconcile` — testing-czar gap, Phase 2 specific
5. `mpsc_overflow_triggers_reconcile` — Phase 3 gating; without this, Phase 1 ships silent failures
6. `mid_write_race_does_not_corrupt_cache` — testing-czar gap (re-upload during pending notification)
7. `malformed_key_in_pushinfo_does_not_crash_listener` — security-reviewer + testing-czar
8. `listener_panic_does_not_break_store` — security-reviewer CRITICAL (callback wrapping)
9. `ac_vs_cas_prefix_collision_isolated` — testing-czar; multiple stores, db-scoped psubscribe
10. `reconnect_reconcile_drops_phantom_entries` — Phase 2
11. `reconnect_reconcile_keeps_present_entries` — Phase 2
12. `reconcile_does_not_run_without_reconnect` — Phase 2 negative assertion

Each must be RED-first (TDD per CLAUDE.md), with explicit mutation step in PR description.

### 5.2 Test infrastructure (testing-czar BLOCKER)

`MockRedisConnection` has NO push/pubsub injection. Tests #1, #2, #4, #5, #8 require either:
- (a) extending the mock to deliver `PushInfo` (~150 LOC fixture extension)
- (b) spawning real `valkey-server` sidecar (~200 LOC fixture + CI dep on valkey-server binary)

Mongo tests use `mongo_runner/` sidecar pattern. Recommend (b) for Redis: same shape, more authentic, exercises real keyspace-notification semantics.

**Section 5.2 is a hard prerequisite for Phase 1.** Cannot ship Phase 1 until the test fixture exists.

---

## 6. Effort estimate (revised)

| Phase | LOC | Files | Prerequisites |
|---|---|---|---|
| 0 — instrument | ~30 | redis_store.rs, existence_cache_store.rs | none |
| infra — Valkey sidecar | ~200 | nativelink-store/tests/valkey_runner/, CI config | valkey-server installed on CI |
| 1 — push invalidation | ~200 | redis_store.rs (major), config probe | Phase 0 confirms cause; sidecar exists |
| 3 — overflow → reconcile glue | ~20 | redis_store.rs | Phase 1 + Phase 2 stubs |
| 2 — SCAN reconciler (bloom) | ~120 | redis_store.rs, mock manager | Phase 1; sidecar |

Critical path: Phase 0 → infra → Phase 1+3 → Phase 2.

---

## 7. Open questions (v2)

1. **Is the wedge actually Redis eviction?** Phase 0 must answer. If no, this whole design is shelfware; reframe as future-proofing only.
2. **Bloom filter false-positive rate trade-off**: 1% FPR keeps 1% phantom positives across reconciles. Acceptable? Or tighten to 0.1% (~600MB)?
3. **Per-RedisStore epoch counter for ABA prevention**: defer to Phase 2 or include in Phase 1?
4. **Operator coordination**: enabling `notify-keyspace-events` is global. Other tenants on this Valkey instance see the events too. We confirmed Valkey is dedicated to NativeLink; what about other future deployments?

---

## 8. Reviewer verdict matrix

| Reviewer | v1 verdict | v2 status |
|---|---|---|
| code-reviewer | NEEDS-REVISION | factual errors fixed (line cites, MockRedisManager trait method default, OntapS3 sibling note) |
| perf-optimizer | NEEDS-REVISION + 2 BLOCKERS | fan-out task addresses single-consumer collision; bloom filter addresses 3.2GB HashSet; lock-snapshot pattern fixes await-under-lock |
| security-reviewer | APPROVED w/ mitigations | strict prefix match, DB-scoped psubscribe, SCAN cursor cap, panic-catch — all in §1.3 + §2 |
| code-simplifier | OK w/ simplifications | bool flag dropped (§1.5), eager spawn (§1.4), walk-and-check via bloom (§4.3) |
| testing-czar | REJECT (mock infra) | infrastructure §5.2 marked as hard prerequisite |
| rust-crate-reviewer | PROCEED w/ minor corrections | OnceCell adopted, JoinHandleDropGuard, bounded mpsc, redis-rs already in tree |
| **red-team** | **NEEDS-REVISION + premise demolition** | **Phase 0 (instrument-first) added as gating §0.3 + §4.1; multiple-DB subscribe documented; bloom filter for memory; refuse-start on missing notify-keyspace-events** |

---

## 9. Implementation precondition checklist

Before ANY of this code lands:

- [ ] Phase 0 instrumentation deployed; wedge reproduced; mechanism confirmed as Redis-eviction (or this design becomes shelfware)
- [ ] Valkey sidecar test fixture lands as separate PR
- [ ] Operator agrees to enable `notify-keyspace-events Eeg` (or `Eg`+`Ee` minimum) on production Valkey + persist to valkey.conf
- [ ] `RedisManager` trait's `reconnect_notify` method default verified to keep MockRedisManager compiling
- [ ] All 12 tests written RED-first, verified to fail without implementation
- [ ] Phase 1 + Phase 2 + Phase 3 land as a SINGLE deploy (Phase 1 alone has silent-overflow hole)

---

## References

- Production wedge investigation (lifecycle agent): digest f4c899f8...-1667 trail
- `nativelink-store/src/redis_store.rs:1608-1614` (current no-op register_item_callback)
- `nativelink-store/src/existence_cache_store.rs:117-135` (existing ExistenceCacheCallback)
- `nativelink-store/src/redis_store.rs:1862-2022` (RedisSubscriptionManager — pattern reference)
- async-profiler PushInfo fan-out pattern (informally; see redis-rs docs)
- CLAUDE.md "Less patch-and-paper-over, more root-cause thinking" (drives Phase 0 reframing)
