# FastCDC chunking design (Bazel PR #28437 support)

> **Status**: design proposal. No code written. Reviewed for conceptual completeness by sub-agent on 2026-04-22.

## Summary

Bazel PR [#28437](https://github.com/bazelbuild/bazel/pull/28437) adds two new CAS RPCs:

- `SplitBlob` — server returns the chunk-digest list for a logical (composite) blob.
- `SpliceBlob` — client uploads chunks individually, then registers a logical blob as the concatenation of those chunks.

Chunking uses **FastCDC 2020** (Wen Xia et al.) with default 512 KiB average chunk size and a 4× threshold (~2 MiB). Per Bazel's benchmark across 50 BuildBuddy commits: **~40% upload bandwidth saved + ~40% disk-cache savings**.

We already have:
- A working FastCDC implementation in `nativelink-util/src/fastcdc.rs` (used by `dedup_store.rs`).
- The byte-window reassembly logic we need (`dedup_store.rs::get_part` lines 250-364).

## Wire / proto changes

Update `nativelink-proto/build/bazel/remote/execution/v2/remote_execution.proto`:

- Add 4 messages: `SplitBlobRequest`, `SplitBlobResponse`, `SpliceBlobRequest`, `SpliceBlobResponse`.
- Add `ChunkingFunction` enum (`UNKNOWN=0`, `FAST_CDC_2020=1`, `REP_MAX_CDC=2`).
- Add `FastCdc2020Params` message.
- Add `rpc SplitBlob`/`rpc SpliceBlob` to `service ContentAddressableStorage`.
- Extend `CacheCapabilities` with fields 9/10/11: `split_blob_support`, `splice_blob_support`, `fast_cdc_2020_params`.

All additive — no break for existing clients. Bump `high_api_version` to `2.4.0` for hygiene.

## Server-side changes

| File | Change | Effort |
|---|---|---|
| `nativelink-proto/.../remote_execution.proto` | Add Split/Splice RPCs, `ChunkingFunction`, `FastCdc2020Params`, capability fields | small |
| `nativelink-service/src/cas_server.rs` | Implement `split_blob` + `splice_blob` methods | medium |
| `nativelink-service/src/cas_server.rs::find_missing_blobs` | Composite expansion (see open question a) | small-medium |
| `nativelink-service/src/bytestream_server.rs::inner_read` | Detect composite digests, reassemble from chunks transparently | medium |
| `nativelink-service/src/bytestream_server.rs::inner_write` | No change — chunks upload via existing path | none |
| `nativelink-service/src/capabilities_server.rs` | Advertise the new capability fields | tiny |
| **NEW** `nativelink-store/src/splice_manifest_store.rs` | Module to record/lookup chunk-digest lists by composite digest | medium |
| `nativelink-store/src/store_manager.rs` | Wire SpliceManifestStore into store graph | small |
| `nativelink-config/src/cas_server.rs` + `stores.rs` | New optional config: `splice_manifest_store`, `chunking { enable_split_blob, enable_splice_blob, verify_splice, fast_cdc_avg_chunk_size }` | small |
| `nativelink-store/src/grpc_store.rs` | Add client methods `split_blob` + `splice_blob` (used by mirror path) | small |

## Worker-side changes

**Case A — fetching action inputs from server.** When `download_to_directory` (in `running_actions_manager.rs`) sees a large file digest and the server advertises `split_blob_support`, call `SplitBlob` first, then fetch the chunks individually, then reassemble into a single file in the worker fast store keyed by the composite digest. Same for `directory_cache.rs::populate_fast_store_unchecked` paths.

**Case B — mirroring blobs to peers** (`worker_proxy_store.rs::mirror_blob_to_random_worker`): if the server has the blob as a composite, the mirror path needs to send chunks (not the assembled blob), pre-flighting `FindMissingBlobs` against the peer to skip already-present chunks, then call `SpliceBlob` on the peer to register the composite.

**Reassembly strategy**: read chunks from the worker's local FilesystemStore via a buffered chain reader, write into a temp file via `write_temp_file` + `emplace_file`. Same total disk I/O as today (one whole-blob write), plus N small chunk reads. Reassembly is one-time-per-composite-per-worker — subsequent actions hardlink the reassembled file.

## Mirror & P2P

Two existing flows must become composite-aware: server→worker mirror (push) and worker→peer race-read (pull). Both already exist for whole blobs; the geometry changes for composites.

### Server→worker mirror (push)

Today (`worker_proxy_store.rs::mirror_blob_to_random_worker`): server writes a CAS blob, then enqueues a mirror to a random eligible worker. The worker pulls via ByteStream-Write.

With composites:

1. **Composite expansion at enqueue time.** When the server registers a `SpliceBlob`, schedule one mirror task per (peer, composite). The task body: call `FindMissingBlobs` against the peer for `manifest + chunks`, push only the deltas, then `SpliceBlob` on the peer to register the manifest. Pre-flight FMB is essential — workers that already have most chunks (which is the common case if FastCDC is doing its job) only receive the few missing ones.
2. **Per-chunk sync-confirm.** Existing per-blob sync-confirm (50ms `worker.has(digest)`) operates per chunk push. The composite `SpliceBlob` itself is the manifest "publish" — only call it after every chunk push has been ack'd, otherwise a peer can register a manifest pointing at chunks not yet visible.
3. **Mirror permits.** The current per-worker semaphore (16 permits, sized for whole-blob mirrors) caps peer concurrency. With chunked mirrors a single composite can fan out to N chunk pushes — bump permits to 32 (matches `connections_per_endpoint` and absorbs the ~2× RPC inflation cited in Bazel's benchmark) and gate the per-composite fan-out under a single permit so one composite cannot starve the queue.
4. **No reassembly on push.** Workers receive chunks individually and store them as ordinary CAS blobs. Reassembly into a composite-keyed file happens lazily on first read (Case A of "Worker-side changes" above), not at mirror time. This avoids paying reassembly disk I/O for composites that the worker may never read.

### Worker→peer race-read (pull)

Today (`worker_proxy_store.rs::get_part`, lines 1254-1399): when a worker needs a blob it doesn't have locally, it consults the worker-side `locality_map` for peers that have it and **races** server fetch + peer fetch in parallel via two `buf_channel` pairs and `tokio::select!`. First side to produce data wins; the other is dropped.

With composites the **race granularity must drop from composite to chunk**. Here's why and how:

**Why composite-granularity racing breaks down.** Per the dedup-discipline rules above, chunks have **independent eviction lifetimes**. Worker B may have the manifest + 7 of 8 chunks. Worker C may have the other 4 of 8 chunks. Neither has the full composite — but their union does. A composite-level race fails on both peers and falls back to the server, giving up the P2P benefit entirely. Worse: even when one peer has all chunks, racing the *whole* assembled composite needlessly serializes 8 chunk reads behind one bytestream stream when the chunks could come from 8 streams in parallel from the union of peers + server.

**Required design — race per chunk.**

1. **Resolve manifest first.** `get_part` consults the locality_map for the composite digest. If composite is registered, fetch the manifest (small, almost always hot in cache; if missing locally, single race against server — manifest-resolution is cheap and not on the critical path because manifests are tiny).
2. **Per-chunk race plan.** For each chunk in the manifest, query the chunk-locality submap (per open-question (b)) for the set of peers known to hold that chunk. Compose the racer set as `{server} ∪ {peers with chunk}`.
3. **Bounded parallel chunk fetch.** Use `JoinSet + Semaphore(8)` (mirrors `directory_cache.rs`'s parallel-blob discipline). Each chunk fetch is its own race across its racer set. Per-chunk losers are dropped via `buf_channel` `tx.drop()` propagation as today.
4. **Stream output in manifest order.** As chunks complete, write them to the caller's `DropCloserWriteHalf` in manifest order — i.e., chunk N+1 must wait for chunk N to finish writing even if it arrived first. A small completion buffer (`HashMap<chunk_index, Bytes>`) handles out-of-order arrival.
5. **Offset/length handling.** When the caller asks for `(offset, length)` of a composite, walk the manifest's byte-window arithmetic (reuse `dedup_store.rs::get_part` lines 250-364) to determine which chunks intersect, compute per-chunk offsets, and apply trimming on the first/last chunk. Same arithmetic that exists today; just pulled into `worker_proxy_store` for the racing path.
6. **Cache-the-winner.** Each chunk that wins its race is written to the local fast-store via `update()` (today's race already does this for the whole blob). After all chunks land locally, optionally trigger reassembly so the next read of this composite hits the assembled file (Case A path).

**Latency model.** Composite read latency drops from `latency(slowest_full_holder)` to `max_chunk(latency(slowest_winner_for_chunk_i))`. With independent chunk eviction across N peers, expected case is roughly `latency(server)` for the worst chunk and lower for the rest — strictly better than today's whole-blob race.

**Failure model.** A chunk that NotFound's on every racer (server + every peer) is a hard error — same as today's whole-blob fetch failure. Surface as `NotFound` to the caller; the upstream retry path (action rewinding, eviction-retry) covers it.

### Hot edge: race tasks and `on_get`

The existing whole-blob race fires `on_get(blob_digest)` once per successful read (via the underlying store's `get_part`). With per-chunk racing:
- Each chunk fetch fires `on_get(chunk_digest)` — natural, but ~N× more calls. Already covered by open-question (c) and the `on_get` amplification note in the Review findings — the BlobChangeTracker batching mitigation applies here too.
- The composite itself does NOT receive `on_get` from chunk reads. Per the bytestream reassembly path note (B2 in Review findings), explicitly call `Store::notify_get(composite_digest)` from `worker_proxy_store::get_part` after the manifest is resolved. Otherwise composite-keyed locality entries age out while chunks stay hot.

## Storage schema (recommended)

**(a) Manifest-as-CAS-blob.** A composite digest's manifest is itself a small CAS blob, content-addressed by its own SHA256, keyed in CAS by the composite digest with a namespace prefix (e.g., `splice:<sha256>`). Reuses all FilesystemStore infrastructure (eviction, locality-map, atime tracking).

Alternative (b): sidecar KV table — faster lookup but another store to manage. Not recommended.

### Dedup discipline (per REAPI spec)
- Chunks live in normal CAS, content-addressed by their own digests.
- **Lifetimes are independent**: touching a composite extends the manifest's atime but **NOT the chunks'**. Touching a chunk extends only that chunk. Worker fast-store eviction can evict chunks of a still-live composite — handled via NotFound → re-fetch fallback.
- For SpliceBlob verification: doing the full concat-and-hash is O(blob_size). REAPI says the server **should** verify. Make it config-flagged (`verify_splice: bool`, default true).

## Backward compat

- Old Bazel clients: never call `SplitBlob`/`SpliceBlob`, never advertise FindMissingBlobs against composite digests. **Zero impact**.
- New Bazel clients negotiate via capabilities; fall back to whole-blob if support flags are false.
- Server config default: `enable_split_blob = false`, `enable_splice_blob = false`. Operators flip when ready.
- Workers: composite-aware code paths gated on the same config + runtime check on `cas_store.capabilities()`.

## Migration phases

1. **Phase 1 — Read passthrough.** SpliceManifestStore + SpliceBlob + SplitBlob + bytestream-Read reassembly. Server stores and serves composites uploaded by Bazel-CDC clients. Workers don't yet understand composites — they fetch via ByteStream-Read which transparently reassembles. **Phase 1 already gets 100% of upload bandwidth savings + storage savings.**
2. **Phase 2 — Worker chunk awareness.** Workers learn to call SplitBlob, populate chunks individually, reassemble. Required for worker fast-store dedup.
3. **Phase 3 — Server-side splicing.** Server chunks whole-blob uploads from old Bazel clients in the background. Adds CPU + I/O cost; defer until phase 2 stable.
4. **Phase 4 — Mirror chunks.** Peer-mirror path becomes chunk-aware.

## Risks + open questions

- **(a) FindMissingBlobs semantics for composites.** Manifest exists but a chunk has been evicted → composite "present" but unreadable. Three options: (i) eager-delete manifest on chunk eviction (reverse-index, expensive); (ii) check on each FMB that all chunks exist (turns 1 RPC into N+1 lookups); (iii) treat as transient error, periodic background sweep drops manifests with missing chunks. Bazel's PR uses (iii). **Recommend (iii).**

- **(b) locality_map keying.** Today keys by `DigestInfo`. With composites: option (a) treat composite as opaque (locality_map records manifests only; chunk reads don't update); option (b) record both composite + chunks in the locality_map. **Recommend (b)** with a separate "chunk locality" submap with shorter TTL. **Required by the chunk-granularity P2P race** (see "Mirror & P2P → Worker→peer race-read"): without per-chunk locality, the race cannot construct a per-chunk racer set and degenerates to composite-granularity (or server-only).

- **(c) on_get / `ItemCallback` propagation.** When inner_read serves a composite, chunk reads fire `on_get(chunk_digest)` but no `on_get(composite_digest)`. **Must explicitly call `on_get(composite_digest)`** from the bytestream reassembly path.

- **(d) SpliceBlob verification cost.** O(blob_size) I/O. Stream chunks through Blake3/SHA256 hasher without buffering. Configurable; default on.

- **(e) Mirror queue volume.** Bazel benchmark: ~2× more RPCs. Pre-flight FindMissingBlobs against mirror target reduces redundant traffic.

- **(f) Sync-confirm fast path.** Currently operates on whole-blob digest. With composites: needs to confirm the worker has all the chunks. Mitigation: server-side, after receiving SpliceBlob, check target workers (per locality_map) have all chunks before signaling sync-confirm.

- **(g) Worker memory pressure during reassembly.** Bound concurrency (`Semaphore(8)`) — match `dedup_store::max_concurrent_fetch_per_get` discipline.

- **(h) Manifest eviction policy.** Manifests are tiny (~100s of bytes). Should never be evicted ahead of their chunks. Either: separate FilesystemStore instance with much larger eviction limit, or pin indefinitely up to a max-count.

- **(i) Chunking algorithm choice.** REAPI defines `FAST_CDC_2020` and `REP_MAX_CDC`. Bazel client only implements FastCDC 2020. **Advertise FAST_CDC_2020 only.**

## Estimated payoff

Per Bazel's benchmark (50 commits of BuildBuddy repo, similar Go/C++ workload to our Rust/Go/Swift/TS mix):

- **Upload bandwidth**: ~40% reduction. For our worker→server 10 GbE: if a typical CI day uploads ~500 GB, save ~200 GB.
- **Storage**: ~40% smaller server-side disk cache. `tank` pool ~319 GB → ~190 GB. Less ZFS churn, longer effective LRU window.
- **Worker fast-store storage**: 30-40% reduction. Workers currently sit at 19/20 GB with constant eviction pressure → effective +30% capacity.
- **Latency**: small build-time speedup for repeated builds (warm cache benefits from chunk dedup); per-blob latency rises slightly for cold reads (extra `SplitBlob` RTT + N chunk fetches).
- **RPC count**: ~2× higher.
- **Net**: very likely **net-positive** for our workload — we have many large, similar build outputs (Rust release binaries with debug info, Mach-O fat binaries) and we're disk-bound on the worker fast store.

## Review findings (2026-04-22)

Sent to a code-review agent and a performance-review agent. Headline:
**design is plausible but blocked on five items + two mandatory pre-flight
measurements**.

### Must resolve before any implementation
- **B1 — old-client write collision.** If a non-CDC client uploads bytes
  under a digest that's also a registered composite, schema is
  corruptible. Add a write-time guard in cas_server's batch_update path
  that rejects writes for a digest with a registered manifest, OR
  document inner_read precedence. Pick one.
- **B2 — `Store::notify_get(key)` missing.** Design says the bytestream
  reassembly path "must explicitly call on_get(composite_digest)" — but
  `Store` has no public `on_get` method. Add `Store::notify_get(key)`
  that fans out to registered callbacks, or plumb the callback `Arc` to
  the bytestream. Spec which.
- **B3 — composite-detection sentinel.** Spec how `inner_read` decides a
  digest is a composite (probe `splice/<hash>` via has() first,
  cacheable through ExistenceCacheStore). Design implies but doesn't
  state.
- **B4 — SpliceBlob verification must be bounded background.**
  Synchronous verification of 100 MB composite = ~250ms wall +
  ~200 syscalls per splice. At 100/sec burst: saturates 1 core.
  Move to bounded background pool with admission control (reject with
  `RESOURCE_EXHAUSTED` if backlog > N).
- **B5 — locality_map chunk-keying budget.** Design said "shorter TTL"
  for chunk submap — not a budget. Hard cap: 1M entries, TTL ~5min.
  Without this, 5M chunks × 10 workers × ~80B = ~4 GB worst case
  (today's locality_map is ~40 MB).

### Pre-flight measurements (mandatory before phase 1 commit)
- Run FastCDC over a sample of `/srv/casdata/nativelink/stores/` and
  report the dedup ratio. The 40% claim is from BuildBuddy's commit
  history — unverified for our workload. If median blob is <256 KiB,
  gains evaporate while overhead remains.
- `fio` random-read latency benchmark against the tank pool. 200
  scattered chunk reads on cold page cache is the worst case for
  composite reads — quantify before committing.

### Other adjustments to make to the design
- `splice/<hash>` not `splice:<hash>` (filesystem-friendly).
- Storage-schema option (a): manifests pinned via existing `pin_keys`
  mechanism on the same FilesystemStore — NOT a separate FilesystemStore
  instance.
- `on_get` amplification: at 1100/sec to BlobChangeTracker, need a
  microbench. If contended, batch chunk-touches into a single
  composite-touch and skip per-chunk on_get.
- Cold-read concurrency: `Semaphore(32)` server-side (matches existing
  `connections_per_endpoint`).
- Mirror permits: bump from 16 to 32 to preserve effective per-blob
  mirror parallelism with 2× chunked RPCs.
- Worker reassembled file lifetime: spec that the reassembled file IS
  stored under composite digest; chunks become evictable; on chunk
  eviction, do NOT touch the composite.
- ZFS `recordsize=1M` may be wrong for 512 KiB chunks. Verify ARC dnode
  headroom for ~8× inode count (~400 MB ARC pressure).
- Mirror sync-confirm flow needs explicit spec: chunks via existing
  per-blob sync-confirm path, manifest via SpliceBlob-mirror.
- Phase 1 claim "100% of bandwidth savings" is **server-side only**;
  worker fast-store dedup is phase 2.

## Files to read before implementing
- `nativelink-proto/build/bazel/remote/execution/v2/remote_execution.proto` — proto bump target
- `nativelink-service/src/cas_server.rs:1017-1145` — RPC trait impl; add SplitBlob/SpliceBlob here
- `nativelink-service/src/bytestream_server.rs:968-1009` (inner_read), 1323-1430 (inner_write)
- `nativelink-service/src/capabilities_server.rs:132-145` — advertise new capability fields
- `nativelink-store/src/dedup_store.rs:250-364` — reference impl of byte-window arithmetic
- `nativelink-util/src/fastcdc.rs` — existing FastCDC; reuse for phase 3 server-side splicing
- `nativelink-store/src/grpc_store.rs:526` neighborhood — add SplitBlob/SpliceBlob client methods
- `nativelink-store/src/worker_proxy_store.rs:997-1100` — mirror path needs composite expansion (per-chunk FMB + SpliceBlob-publish)
- `nativelink-store/src/worker_proxy_store.rs:1254-1399` — `get_part` race; replace whole-blob race with per-chunk race driven by chunk-locality submap
- `nativelink-worker/src/running_actions_manager.rs` + `directory_cache.rs` — worker input fetch + populate paths
- `nativelink-config/src/cas_server.rs` + `stores.rs` — config additions

New file: `nativelink-store/src/splice_manifest_store.rs`
