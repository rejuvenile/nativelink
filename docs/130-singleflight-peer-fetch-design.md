# #130: Singleflight/dedup concurrent peer-fetch retries to stop locality amplification

**Status:** design + red TDD scaffold (this commit). Implementation lands in a
follow-up after the parallel CDN-tee agent's worktree merges (avoids a conflict
on `nativelink-store/src/worker_proxy_store.rs`).

**Tracker context:** "Singleflight/dedup concurrent peer-fetch retries to stop
locality amplification."

## Problem statement

Production observability (per #171 audit logs) shows ~600 events/min of
`Tried to send while stream is closed` peer-fetch failures, plus a recurring
~109K events / 3h pattern in which the **same digest** is read 3-4 times within
~150 ms — all racing for the same upstream h2 channel. The first read on the
channel succeeds; the subsequent reads observe a poisoned stream-state and
return `Code::Internal` mid-stream.

Concretely, `WorkerProxyStore::get_part_sequential` →
`try_read_from_worker` → `get_part_and_cache` → `peer.get_part(...)` is
re-entered N times concurrently for the same digest because:

* The server bytestream layer accepts N independent client `Read` RPCs for the
  same blob (Bazel + worker input-fetch + worker mirror all racing).
* Each Read independently misses the ExistenceCache → VerifyStore → FastSlowStore
  fast tier (the blob isn't local), so each falls through to `WorkerProxyStore`.
* `WorkerProxyStore` has no in-flight dedup, so each call independently consults
  `locality_map`, picks the same peer endpoint (the only one that has the blob),
  and opens its own h2 stream against the same `tonic::Channel`.

`grpc_store.rs::get_part_single_stream` then sees N concurrent stream
allocations on a single channel; under transient pressure (h2 SETTINGS-frame
back-and-forth, RST_STREAM bursts, MAX_CONCURRENT_STREAMS dips) the channel
state poisons, and N-1 of the racing reads error out with the audit signature.

**Singleflight collapses the N concurrent same-digest peer-fetches into 1 — it
removes both (a) the redundant work AND (b) the channel-state-poisoning
amplifier that turns 1 concurrent fetch into N-1 production-visible failures.**

## Mechanism diagram

```
Today:
  N callers ──┬──> WorkerProxyStore::get_part   (same digest, ~ms apart)
              ├──> WorkerProxyStore::get_part
              ├──> WorkerProxyStore::get_part
              └──> WorkerProxyStore::get_part
                          │
                          ▼ (each independently)
                  N peer.get_part() calls
                          │
                          ▼ (all on the same h2 channel)
                  N h2 streams
                          │
                          ▼
                  1 succeeds, N-1 fail with
                  "Tried to send while stream is closed"

After (singleflight):
  N callers ──┬──> WorkerProxyStore::get_part
              ├──> WorkerProxyStore::get_part   ─┐
              ├──> WorkerProxyStore::get_part    ├──> 1 leader runs peer.get_part()
              └──> WorkerProxyStore::get_part   ─┘    N-1 awaiters subscribe
                                                     to the leader's bytes
                          │
                          ▼
                  All N callers receive identical bytes
                  (or all N see identical Err if leader fails)
```

## Proposed design

### Keying strategy: Option B (key alone — full-blob reads only)

We dedup **only** when `offset == 0 && length == None` (the full-blob read).
The singleflight map key is `StoreKey<'static>` (typically a `DigestInfo`).

Rationale for B over A (key + offset + length):

1. **The bug is full-blob reads.** Production audit shows the failure pattern
   on the bytestream-driven full-read path (Bazel CAS download, worker input
   fetch, worker mirror). Partial-range reads (e.g. small Action AC reads,
   header-only DownloadFile peeks) are not measurably contributing to the
   amplification.

2. **Partial-range fan-out is much harder.** With offset+length keying, two
   partial reads at different ranges on the same digest would each get their
   own singleflight slot, gaining no dedup. With "key only, full-only" keying,
   we never ask "do these ranges overlap and can I share?" — we just bypass
   singleflight for partial reads.

3. **CDN-tee parity.** The parallel CDN-tee design (in flight) uses the same
   "full-blob only" predicate (`should_cache` in `get_part_and_cache` already:
   `offset == 0 && length.is_none() && size <= MAX_CACHE_BLOB_SIZE`). Using
   the same gate keeps the two features composable: both kick in for the
   same hot path (full-blob CAS reads), both bypass for the same cold edge
   cases (range reads).

4. **Simplicity.** Key-only inflight maps are textbook moka/dashmap; range-aware
   inflight maps are a research project.

### Result fan-out

The leader buffers the entire blob into `Arc<Vec<Bytes>>` (a list of chunks,
NOT a single `Bytes::copy_from_slice` — preserving the producer's chunk
boundaries to avoid an O(blob size) copy). On success, every awaiter is woken
via a `tokio::sync::watch::Receiver` carrying
`Option<Result<Arc<Vec<Bytes>>, Error>>`. Each awaiter then iterates the
`Arc<Vec<Bytes>>` and forwards chunks into its own writer.

Rejected alternatives:

* **`tokio::sync::broadcast` of chunks.** Drops slowest receiver when the ring
  fills; semantics-incompatible with "every awaiter must receive every chunk."
  Also forces a fixed channel capacity decision that we cannot make safely.
* **Custom multi-consumer streaming primitive.** Real-time fan-out would let
  the slowest awaiter back-pressure the leader's peer-fetch — fine in theory,
  but introduces a coupling between awaiter cancelation and leader liveness
  that we don't want for v1. Buffer-then-fan-out has bounded leader latency:
  leader runs at peer's pace, awaiters get bytes as soon as the leader
  finishes. The added latency for awaiters is `min(leader's remaining time,
  zero)` — it's exactly what they would have observed if they'd raced on
  their own and lost.
* **`Arc<Bytes>` (single contiguous buffer).** Would require a `Vec<u8>`
  staging copy at the producer side. `Arc<Vec<Bytes>>` reuses the producer's
  chunks via cheap refcount bumps.

### Memory bound

Worst case: `parallel_chunk_count = 64` × max cached blob = 64 MiB
(`MAX_CACHE_BLOB_SIZE` already constrains the per-blob singleflight payload).
Per-blob fan-out width is bounded by the inflight count (typically 4-16 for
the bug pattern; 64+ in pathological bursts).

Total inflight memory: bounded by `inflight_count × payload_size`. We propose
a **soft cap of 256 MiB** total in-flight singleflight payload. When the cap
is exceeded, NEW callers bypass singleflight and do their own peer-fetch
(graceful degradation: we lose the dedup-amplification protection for those
extra callers, but no caller is blocked on cap accounting). The cap is
configurable; default `256 * 1024 * 1024`.

### Interaction with the CDN-tee (currently in progress)

* **Singleflight** collapses **concurrent** peer-fetches for the same digest
  into 1 leader fetch. This eliminates the channel-poisoning amplifier on the
  *first* cohort of concurrent callers.

* **CDN-tee** (parallel agent's work) makes the leader's bytes durable in the
  local CAS as a side effect, so **subsequent** (sequential, not racing)
  reads hit local CAS and skip the peer entirely.

* **Order of effect:** singleflight + tee fire together on the first cohort.
  Singleflight gives N concurrent callers the same bytes from 1 fetch; tee
  ensures that any later reads (after the first cohort settles) hit local
  CAS rather than going to the peer at all.

* **Composition:** both gates use the same `offset == 0 && length.is_none()`
  predicate, so they kick in and bypass on the same code paths. No extra
  coordination between the two.

### Failure semantics

* If the leader's peer-fetch fails (returns `Err(...)` from `peer.get_part`),
  ALL N awaiters receive the **same** `Err` (cloned via `Error::clone()`).
  They do NOT each independently retry — that's the bug we're fixing. The
  leader's own retrier inside `grpc_store.rs::get_part_single_stream` is what
  handles per-attempt retries.

* If the leader is canceled (its `Future` is dropped) but ≥1 awaiter is still
  alive, the next-in-line awaiter is **promoted to leader** and re-issues the
  peer-fetch. This is necessary because the leader's task carried the
  `peer.get_part` future; once dropped, the bytes are gone. (See "Edge cases"
  below for the precise mechanism.)

* If the leader returns `Ok(())` but with 0 bytes for a non-zero digest (the
  stale-positive guard already in `try_read_from_worker`), the leader's
  caller treats it as failure and the awaiters receive the same Err. The
  locality-map eviction happens on the leader path only (no double-eviction).

### Edge cases

1. **Caller cancelation (writer dropped) — single awaiter.** Awaiter drops its
   `watch::Receiver` and its slot in the awaiters list. The singleflight
   slot stays alive while ≥1 other awaiter is registered.

2. **Caller cancelation — last awaiter.** When the last awaiter drops, we
   want to cancel the leader's peer-fetch (no caller is left to receive the
   bytes). Implemented by tracking a `weak_awaiter_count: AtomicUsize` and
   having the leader periodically poll it; when it reaches zero, the leader
   aborts. This avoids a long-running peer-fetch consuming bandwidth for
   nobody.

3. **Leader cancelation while awaiters exist.** If the leader's outer Future
   is dropped but the inflight-map slot is still registered, the inflight
   slot is "orphaned" — its `watch::Sender` is dropped without sending,
   which causes ALL awaiters' receivers to fire with `Err(RecvError)`. The
   first awaiter to wake up promotes itself to leader by re-acquiring the
   slot and re-issuing the peer-fetch.
   * Implementation note: use a `tokio::sync::Mutex<Option<LeaderState>>`
     inside the inflight slot; first awaiter to find `None` becomes leader.

4. **Concurrent insertion race.** Two callers race to insert into the
   inflight map. The loser uses an entry-API-style `or_insert_with` (DashMap
   or moka's `get_with_value_initializer`) to fall through to the
   already-present slot. Standard pattern.

### Observability

Counters on `WorkerProxyStore` (new fields, all `AtomicU64`):
* `singleflight_hit_total` — incremented when an awaiter joins an existing slot
* `singleflight_miss_total` — incremented when a caller becomes the leader
* `singleflight_fanout_max` — high-water mark of awaiters per slot (gauge)
* `singleflight_leader_cancel_total` — leader Future dropped while awaiters present
* `singleflight_payload_bytes_inflight` — current total inflight payload bytes
* `singleflight_cap_bypassed_total` — callers that bypassed the inflight cap

Logs:
* `info!` on first dedup-cohort completion, with fanout count and leader byte rate
* `warn!` on leader cancelation (because that triggers a re-fetch)
* `warn!` on cap-bypass (operator signal that 256 MiB ceiling is too low)

### Out of scope for this PR

* Anything that requires touching `grpc_store.rs`'s retrier internals. The
  singleflight wrapper sits one layer above `peer.get_part` in
  `WorkerProxyStore::get_part_and_cache`; the leader still calls into the
  unmodified `peer.get_part` and the same `grpc_store.rs` retry semantics
  apply to that single leader call.

* Cross-process / cross-server singleflight (distributed dedup). Per-process
  is sufficient because the bug is "concurrent re-entry on a single server's
  WorkerProxyStore."

* Singleflight on `update()` (write path). Out of scope; mirror writes have a
  different concurrency model (per-endpoint Semaphore is the existing mechanism).

* Singleflight on `has()` / `has_with_results()`. Cheap cache lookups; not
  amplifying.

## Implementation outline (for the follow-up PR after CDN-tee merges)

1. Add `inflight_singleflight: DashMap<StoreKey<'static>, Arc<SingleflightSlot>>`
   to `WorkerProxyStore`.
2. Add `SingleflightSlot { result_tx: tokio::sync::watch::Sender<Option<Result<Arc<Vec<Bytes>>, Error>>>, leader_owned: tokio::sync::Mutex<bool>, awaiter_count: AtomicUsize }`.
3. In `get_part_and_cache`, gate the singleflight path on
   `offset == 0 && length.is_none() && digest.size_bytes() <= MAX_CACHE_BLOB_SIZE`
   (same predicate as `should_cache`, shared with the tee).
4. Leader runs `peer.get_part` collecting chunks into a `Vec<Bytes>`, then
   sends `Ok(Arc::new(chunks))` on `result_tx`. Awaiters subscribe via
   `result_rx.changed()` and on completion, iterate the `Arc<Vec<Bytes>>` and
   forward into their own writers.
5. Slot is removed from the map when the last awaiter drops (refcount on the
   `Arc<SingleflightSlot>`, with a sentinel "remove on drop" pattern).
6. Cap accounting: `singleflight_payload_bytes_inflight.fetch_add(size_bytes)` in
   leader entry; `fetch_sub` on slot drop. New callers check the cap *before*
   becoming leader; on overshoot, they bypass and call `peer.get_part` directly
   without registering a slot.

## Tests this PR ships (red, in `nativelink-store/tests/worker_proxy_singleflight_test.rs`)

All four tests **MUST FAIL** today with specific assertion messages (with the
exception noted on test 2); the implementation PR turns them green.

1. `concurrent_same_digest_reads_dedup_to_one_peer_fetch` — 16 concurrent
   `get_part_unchunked(X, 0, None)` calls; assert peer's get_part counter == 1
   after they all complete.

2. `concurrent_partial_range_reads_match_design` — 16 concurrent
   `get_part_unchunked(X, 10, Some(100))` calls; assert peer's get_part counter
   == 16 (Option B: partial reads bypass singleflight). NOTE: this assertion
   passes vacuously today (no dedup => 16 fetches matches the design); it is
   shipped to GUARD against a future regression toward Option A (key+offset+
   length keying).

3. `singleflight_failure_propagates_to_all_waiters` — peer always fails; assert
   ALL 16 callers receive Err AND counter == 1 (single leader attempt).

4. `leader_cancelation_does_not_kill_other_waiters` — spawn 4, cancel the
   first; remaining 3 still receive bytes; counter ≤ 2 (one of the surviving
   awaiters is promoted to leader OR the original leader's task survives the
   first awaiter being dropped; ≤ 2 covers the leader-promotion race window).

Each test is wrapped in `tokio::time::timeout(5s, ...)` with a specific
`.expect()` message naming the contract violated, so a hang fails loudly
rather than silently consuming CI time.
