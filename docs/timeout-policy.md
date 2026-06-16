# NativeLink Timeout Policy & Removal Tracker

**Living document.** Update on every timeout add/remove/convert. Audit agents
MUST consult this before classifying a timer; ship-time agents MUST update the
DEPLOYED table after push.

## Principle (user direction 2026-05-14)

> RPCs should be able to take an infinitely long time as long as the connection
> exists and keepalives return. I don't want workers or bazel or even the
> server to get stuck in timeout/retry loops which could mask bugs.
>
> Per-chunk timers are diagnostic-only (warn + counter). They never abort.

The only mechanisms authorized to terminate an RPC:

1. **TCP keepalive** (OS-level, kernel TCP_KEEPALIVE)
2. **HTTP/2 keepalive** (`http2_keep_alive_interval` / `_timeout` — 30s/20s in prod)
3. **QUIC keepalive** (5s in prod)
4. **Sender-drop on connection close** (RAII guards observe + clean up)
5. **Real upstream errors** (kernel returned Err, peer returned non-OK Status)

Application-layer timers MAY exist for **observability only** — emit `warn!` +
bump a counter when crossed, but DO NOT abort, DO NOT trigger retry, DO NOT
return `Code::DeadlineExceeded`.

## How to use this document

- **Adding a new timer?** Document below in the appropriate category. Default
  to diagnostic-only unless you can justify otherwise per the principle.
- **Removing a timer?** Dispatch reviewer cadre with mutation-verified test;
  push + deploy; update DEPLOYED table with SHA + outcome.
- **Auditing existing timers?** Cross-reference against `journalctl
  --namespace=nativelink | grep <log-message-on-elapse> | wc -l` for production
  firing rate. Static-analysis-only categorization is the trap that hid
  WriteState (see HISTORY).
- **Reviewers:** the dispatch prompt MUST require production firing-rate data
  for any timer being categorized as KEEP.

## Categories

| Category | Definition | Action |
|---|---|---|
| **MASK-BUG** | Aborts an RPC that should be unbounded; fires in production; same class as WriteState | REMOVE or convert to diagnostic-only |
| **DIAGNOSTIC-ONLY** | Fires `warn!` + bumps counter on threshold; does NOT abort | KEEP (correct shape) |
| **CLEANUP-BOUND** | Bounds work AFTER a real error has been detected | KEEP |
| **CONNECTION-LAYER** | TCP/h2/QUIC keepalive, connect-timeout | KEEP (different layer) |
| **PERIODIC TICK** | `tokio::time::interval` for sweepers, watchdogs | KEEP (not an RPC kill) |
| **TEST-ONLY** | Deadlock detector inside `#[nativelink_test]` per CLAUDE.md | KEEP |
| **RETRY BUDGET** | `for _ in 0..N { ... }` retry counters | ASSESS — bounded retries can mask bugs |

---

## DEPLOYED (in production)

| Site | Was | Now | SHA | Date | Notes |
|---|---|---|---|---|---|
| `bytestream_server.rs` `WRITE_TIMEOUT = 300s` | wall-clock kill on entire bytestream Write RPC | **REMOVED** | `2ef908aa` | 2026-05-14 | Bundle Option A. Fixed: 244 firings/24min on slow Tailscale clients. h2 keepalive + sender-drop now bound. |
| `bytestream_server.rs` `COALESCE_TIMEOUT = 300s` | wall-clock kill on coalesced waiter | **REMOVED** | `2ef908aa` | 2026-05-14 | Bundle Option A. Watch-Sender-drop on primary's RAII guard unblocks waiters. |
| `proto_stream_utils.rs` `WriteState::with_progress_timeout` | per-chunk no-progress, sets `read_stream_error = DeadlineExceeded`, terminates inner stream | **DIAGNOSTIC-ONLY**: `warn!` + `GRPC_WRITE_SLOW_CHUNK_TOTAL` AtomicU64 counter; re-arms Sleep; does NOT terminate | `39b60a92` | 2026-05-14 | Was firing 1434×/4h on mirror pushes — broke ≥2-replica durability. ROOT CAUSE of #476 cascade. |

---

## IN-FLIGHT (local worktrees, NOT pushed)

| Worktree | SHA | What | Blockers |
|---|---|---|---|
| `agent-aec9711d7da51b7a5b0039e8dcf86b4ff53bca04` | `aec9711d` | `chunked_driver.rs` `PER_CHUNK_WRITE_TIMEOUT = 5s` (kernel pwrite) → diagnostic-only | needs reviewer cadre R3 |
| `agent-a4dd5ae797988cf47` | `0b65ed65` | `running_actions_manager.rs` upload_timeout 600s wall-clock REMOVED | needs cadre R3 + #481 (set `max_action_executing_timeout_s` in prod-server.json5) |
| `agent-aa356a051aef27c3d` | `b5957393` | `directory_cache.rs` `CONSTRUCTION_LEADER_TIMEOUT = 120s` REMOVED via `CoalesceOptions::no_timeout()` | needs cadre R3 |
| `agent-ac78447566fd7c5e0` | `1e9689b0` | `api_worker_scheduler.rs` background `TREE_RESOLUTION_TIMEOUT = 60s` REMOVED (inline 500ms fast-path KEPT) | needs cadre R3 + #482 (BFS structural bounds) |
| `agent-a3577bcafb8eee978` | `b2094b9a` | Cargo deps bump (tokio 1.44.1→1.52.3, mimalloc 0.1.50, drop v3) | needs cadre |

---

## MASK-BUG candidates — production-data classified (#483 re-audit 2026-05-14)

Re-audit window: pre-WriteState-fix PID 1855082 ran 21:30 UTC → 03:22 UTC (5h 53m of pre-fix data); post-fix PID 2490053 from 03:22 UTC (28-min sample at audit time, 0 firings).

**Cross-cutting finding**: WriteState was UPSTREAM in the timer chain. The "0 firings" reading on candidate timers below is artifact — once WriteState aborted the mirror push, downstream timers never got to fire. With WriteState now diagnostic-only, the next-narrowest abort-on-elapse timer becomes the load-bearing one. **Convert proactively** before the next cascade picks them up.

| Rank | Site | Constant | Pre-fix count | Status |
|---|---|---|---|---|
| 1 | `chunked_driver.rs:864` | `PER_CHUNK_WRITE_TIMEOUT = 5s` (kernel pwrite) | 0/6h (artifact) | worktree `aec9711d` ready, NOT deployed |
| 2 | `chunked_write_handler.rs:1893` | `CHUNKED_COMMIT_WATCHDOG_SECS = 60s` | 0/6h (artifact) | **NO WORKTREE — #484** |
| 3 | `running_actions_manager.rs:3960` | `upload_timeout` (default 600s, config-driven) | 0/6h | worktree `0b65ed65` ready |
| 4 | `api_worker_scheduler.rs:2419` | `TREE_RESOLUTION_TIMEOUT = 60s` (background) | 0/6h | worktree `1e9689b0` ready |
| 5 | `directory_cache.rs:830,1414` | `CONSTRUCTION_LEADER_TIMEOUT = 120s` | 0/6h | worktree `b5957393` ready |

## DOWNSTREAM (effects of upstream MASK-BUG, will resolve when fixed)

| Site | Pre-fix count | Mechanism |
|---|---|---|
| `worker_proxy_store.rs` `mirror_stream: failed to stream blob to worker` | **1481 / 6h** | Direct downstream of WriteState aborts — broke ≥2-replica durability 1481 times |
| `worker_proxy_store.rs:284-289` `MIRROR_FAILURE_THRESHOLD=5/10s → quarantine 30s` | **12 / 6h** | Downstream of the 1481 mirror failures (5 consecutive Generic-kind failures triggers) |

These should drop to ~0 post-WriteState-fix. Re-verify at +24h on PID 2490053 alone.

## RETRY-BUDGET (dormant; revisit if firing rate climbs)

All show 0 firings in 6h pre-fix journal — were dormant when WriteState was abort-class. Post-fix may see firing rise as upstream cascades stop. Defer; revisit at +24h.

| Site | Constant | Notes |
|---|---|---|
| `chunked_client.rs:230,301` | `DEFAULT_MAX_ATTEMPTS = 3` | Sibling cause of #447 cascade |
| `running_actions_manager.rs:1042` | `MAX_RETRIES = 3` (hardlink) | Idempotent op, bounded loop OK |
| `running_actions_manager.rs:4865` | `MAX_RETRIES = 4` (input-fetch download) | Bounded |
| `simple_scheduler.rs:68` | `DEFAULT_MAX_JOB_RETRIES = 3` | Per-action job retry |
| `gcs_client/client.rs:561` | `MAX_UPLOAD_RETRIES` | Storage SDK |
| `simple_scheduler_state_manager.rs:648` | `MAX_UPDATE_RETRIES` | State update |

---

## KEEP — production-data-verified safe (#483 re-audit confirmed)

### CLEANUP-BOUND
- `chunked_driver.rs:291` `DISCARD_AFTER_FAILURE_TIMEOUT = 5s` (×7 call sites at `:887, :931, :1219, :1344, :1366, :1413, :1435`) — bounds best-effort post-error cleanup. 0 firings/6h.
- `chunked_write_handler.rs:1276` `DISCARD_PARTIAL_TIMEOUT = 5s` — handler cleanup. 0 firings/6h.
- `fast_slow_store.rs:93` `SLOW_WRITE_WATCHDOG_SECS = 60s` + drain budgets — graceful-shutdown drain
- `fast_slow_store.rs:1995` `flush_slow_writes` outer + Phase 2 — graceful-shutdown
- `fast_slow_store.rs:2445` `slow_write_timeout` self-retry — bounds SINGLE self-retry attempt (failed-set is the durability path). 2 firings/6h, expected for transient slow tier.
- `failed_writes_drain.rs:108` `DEFAULT_SELF_RETRY_TIMEOUT = 2s` — per-digest cap inside drain tick
- `store_manager.rs:204, :317` outer drain timeouts — graceful-shutdown wall-clock budget surfaced by operator deadline

### DIAGNOSTIC-ONLY (already correct shape)
- `streaming_blob.rs:42, :705` `STREAMING_BLOB_NOTIFY_TIMEOUT = 30s` — logs error then loops back + re-arms; does NOT abort. Converts missing-wakeup wedge into 30s-bounded log event. 0 firings/6h.

### DEFENDS-REAL-EXPLOIT
- `chunked_write_handler.rs:154` `EARLY_DEDUP_DRAIN_PER_RECV_TIMEOUT = 15s` (used at `:3045`, `:3104`) — defends #203 OOM-cascade exploit shape (malicious producer claims small digest, streams large payload via sibling fast-tier `MemoryStore::update`). Code path activated only on already-deduplicated writes. 0 firings/6h. KEEP.

### FIRE-AND-FORGET / NO RPC BLOCKED
- `worker_proxy_store.rs:2022` `CDN_TEE_CACHE_TASK_TIMEOUT = 60s` — bounds spawned cache-fanout task; no upstream RPC blocked. 0 firings/6h.
- `worker_proxy_store.rs:2587` `LOSER_GRACE = 50ms` — race-cleanup; bounds RST-storm risk per #147. 0 firings/6h.
- `worker_proxy_store.rs:3136` 50ms permit acquire — moves to next endpoint, doesn't abort request

### SEMANTIC-CONTRACT
- `connection_manager.rs:285` `connection_acquire_timeout_ms = 3000` — `"ConnectionRefused"` prefix consumed by `is_definitive_unreachable` quarantine classifier; bounded fast-fail by design

### LONG-POLL (by design)
- `execution_server.rs:620` `tokio::time::timeout_at(end, ...)` — long-poll WaitExecution deadline; intentional

### CONNECTION-LAYER
- TCP keepalive (`tcp_keepalive_s`) — wired in `src/bin/nativelink.rs:1796`
- HTTP/2 keepalive (`http2_keep_alive_interval=30, experimental_http2_keep_alive_timeout=20`) — wired in `src/bin/nativelink.rs:1716, 1751`; configured in `prod-server.json5:311, 390, 490`
- QUIC keepalive (5s) — wired in `src/bin/nativelink.rs:2031`
- `connect_timeout_s` (per endpoint) — bounds DNS+SYN, not data transfer
- S3 / ONTAP `connect_timeout` (15s / 30s) — connection-establish only

### PERIODIC TICK
- All `tokio::time::interval(...)` for sweepers (MokaEvictingMap, failed_writes_drain, BIS resend buffer GC, stall_detector tick, etc.) — not abort-on-elapse
- `health_server.rs:39` `DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS = 5` — health probe should be bounded; that's the contract

### TEST-ONLY
- ~85 `tokio::time::timeout(few seconds, ...)` in `tests/` and `#[cfg(test)]` blocks — deadlock-detector pattern per CLAUDE.md

---

## REAPI-MANDATED (workflow, not network)

These are wall-clock budgets set by Bazel/REAPI semantics, not network timeouts.
They MUST be respected per the protocol, not removed:

- `local_worker.rs:718` `DEFAULT_MAX_ACTION_TIMEOUT = 1200s (20min)` — Bazel-set per-action max duration
- `local_worker.rs:686` `DEFAULT_ENDPOINT_TIMEOUT_S = 5.0` — scheduler endpoint connect (connection-layer)
- `simple_scheduler.rs:59` `DEFAULT_WORKER_TIMEOUT_S = 30` — scheduler considers worker dead after 30s no-keepalive (keepalive-equivalent at scheduler layer)
- `simple_scheduler.rs:64` `DEFAULT_CLIENT_ACTION_TIMEOUT_S = 60` — mark op as errored if no client poll (client-pull semantics)

---

## Process

### Adding new code with a timer

1. **Default**: don't add one. h2/TCP keepalive + sender-drop are sufficient
   for almost all RPCs.
2. **If observability is needed**: add a per-chunk diagnostic timer that emits
   `warn!` + bumps an `AtomicU64` counter. Reuse the
   `record_slow_pwrite_and_maybe_warn` / `record_grpc_write_slow_chunk_and_maybe_warn`
   pattern (rate-limited warn at >N/window).
3. **If a real abort is required** (e.g., bounded cleanup after detected
   error): document explicitly in a comment why this is CLEANUP-BOUND and
   not MASK-BUG. Add to KEEP table here.
4. **Never** add `tokio::time::timeout(X, fut).await` that returns Err on
   Elapsed and propagates that as the operation's failure. That's MASK-BUG
   by definition.

### Removing or converting an existing timer

1. **Identify production firing rate**: `journalctl --namespace=nativelink |
   grep <log-message-on-elapse> | wc -l`. If >100/day, very strong candidate
   for diagnostic-only.
2. **Identify the bug it might be masking**: what state could cause this
   timer to fire that isn't a real RPC failure? If you can't articulate one,
   the timer might be load-bearing.
3. **Dispatch reviewer cadre**: code-reviewer + dist-sys-reviewer (mandatory) +
   testing-czar + red-team + assumption-auditor. dist-sys has architectural
   sign-off authority.
4. **Mutation-verify**: write a test that asserts the new behavior with
   bespoke `.expect("...")` message; restoring the old timer must red-fail.
5. **Push + deploy**: per `feedback_always_use_justfile_to_deploy`. Update
   the DEPLOYED table here with SHA + date + production effect.
6. **Post-deploy verify**: query journal for the firing pattern; should drop
   to 0 (REMOVED) or stay non-zero but not abort (DIAGNOSTIC-ONLY).

### Re-auditing existing categorizations

The original timeout audit (worktree `a3577bcafb8eee978` precursor, 2026-05-14)
classified WriteState::with_progress_timeout as PROGRESS-CHECK (KEEP) based on
stale `feedback_per_chunk_timeout_design_intent` memory. It was actually
firing 1434×/4h and breaking durability. **Static-analysis-only audits are
insufficient.** Re-audit triggers:

- Memory file `feedback_per_chunk_timeout_design_intent` is updated
- Production cascade signature unexplained by current categorization
- New per-chunk progress timer added that wasn't reviewed against this doc

---

## HISTORY (lessons learned)

### 2026-05-14 — WriteState miss + bundle Option A → WriteState diagnostic-only

**Context**: production cascade signature `WorkerProxyStore: inner store wrote
N bytes then failed with NotFound` recurring on builds. Initially attributed
to `WRITE_TIMEOUT = 300s` firing on slow Bazel client uploads.

**Sequence**:
1. Dispatched reviewer cadre on Option B (remove WRITE_TIMEOUT + add per-recv
   timer). Red-team RECONSIDER-PREMISE: "no documented production occurrence
   of the failure mode the per-recv timer would catch."
2. User chose Path B / Option A: pure removal, no per-recv timer.
3. Bundle Option A deployed (`2ef908aa`).
4. Cascade signature **recurred 2h post-deploy**.
5. Investigated digest `de0f9256...`: actual mechanism was
   `WriteState::with_progress_timeout` firing on **mirror push to slow worker
   worker-10** at 15s — broke ≥2-replica durability for that blob, downstream
   reads later saw NotFound.
6. Production grep: 1434× "made no progress" events in 4h. Original audit had
   classified WriteState as KEEP.
7. WriteState diagnostic-only deployed (`39b60a92`).

**Root cause of the audit miss**: prior `feedback_per_chunk_timeout_design_intent`
memory said per-chunk timers were intentional. Audit followed it faithfully.
We never cross-referenced with production firing data. The user updated the
memory mid-session; we converted the timer; deployed.

**Lesson**: any audit that categorizes a timer as KEEP based on prior
documentation MUST also produce production firing-rate data. Static analysis
alone is insufficient. This document exists to prevent re-litigating
classifications without evidence.

**Trackers**: #475 (stall_detector burst-pause), #476 (cascade), #477
(visibility-gap fix), #478 (chunked-write registry), #483 (re-audit with
production data).

---

## References

- `feedback_per_chunk_timeout_design_intent` (memory, updated 2026-05-14) — diagnostic-only canonical
- `feedback_assume_nativelink_bug_not_bazel` — server-side default for user-visible failures
- `feedback_no_cross_digest_pattern_match` — trace each fresh, don't recycle
- `feedback_async_to_sync_requires_explicit_signoff` — architectural changes need sign-off
- `feedback_no_fsync_or_synchronous_writes` — durability via mirror+BIS, not fsync
- CLAUDE.md "Fix root causes, not symptoms"
- CLAUDE.md "Falsify before fixing — instrument first, theorize second"
- CLAUDE.md "For hangs/wedges: instrument first, theorize second"
- CLAUDE.md "Sub-Agent Coordination" — every dispatch needs production-data context
- CLAUDE.md "Always dispatch reviewers" — cadre is mandatory
- CLAUDE.md `post-deploy-health-gate` skill — never claim health without traffic
