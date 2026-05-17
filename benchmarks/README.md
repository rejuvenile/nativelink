# `benchmarks/` — NativeLink data-plane benchmark harness

This crate is the implementation of **#495 Phase 1** (v3-anchoring smoke
cells). Design doc:
[`.claude/audits/495-data-plane-benchmark-design-2026-05-16.md`](../.claude/audits/495-data-plane-benchmark-design-2026-05-16.md).

It is **purely additive observability infrastructure** — it does not
change any production code path. The crate provides a single binary
(`data_plane_bench`) that runs a scenario suite, writes JSON baselines
to `benchmarks/baselines/`, and refuses to run on a live buildcache by
default.

## What gets measured

Per the design doc Section 5, Phase 1 covers:

| Flow | Scenario | Cells |
|------|----------|-------|
| W1 | ByteStream::Write through FastSlow→Filesystem | 1 KiB / 16 KiB / 1 MiB / 16 MiB single-writer, 1 MiB × 10 concurrent |
| R1 | ByteStream::Read through FastSlow→Filesystem | 1 KiB / 1 MiB / 16 MiB warm, 1 MiB × 10 warm, 1 MiB / 16 MiB cold |
| F1 | FindMissingBlobs through ExistenceCache | batch 1 / 16 / 128 / 1024, both cache-hit and cache-miss |
| W3 | Chunked-v2 single writer (v3 default flip) | 4 MiB / 16 MiB |
| R5 | Chunked-v2 N contended writers same digest (per-digest Notify) | 4 MiB × 2 / 4 MiB × 10 / 16 MiB × 4 |

W3 and R5 require the `chunked_fast_slow` Cargo feature. The Justfile
recipe builds with the feature on by default.

## When to run

- **Post-deploy validation.** After `just deploy`, once production is
  back in a quiet window, run the suite and diff against the prior
  baseline to confirm the deploy didn't regress any anchored flow.
- **Before major v-bundle changes.** Any change that flips an admission
  gate, eviction path, pin path, or per-digest coordination primitive
  should be preceded by a baseline collection so the post-change run can
  be diffed against a clean reference.
- **Nightly (future).** Per design Section 6 Phase 3, the full suite
  (~45 min including xlarge / huge / 1000-concurrency) belongs on a
  cron. Phase 1 only covers the smoke subset.

## How to run

```bash
# Default: writes benchmarks/baselines/<ts>-<sha>.json. Refuses on
# live buildcache.
just bench-data-plane

# Fast smoke (3 iters per cell) so the suite finishes in seconds.
# Not a clean baseline — diff tooling refuses to compare fast-mode
# baselines.
just bench-data-plane --fast

# Force run despite a non-clear pre-flight verdict. ONLY for first
# baseline collection in a known quiet window, OR for dev iteration
# off-prod.
just bench-data-plane --force

# Filter to a single scenario family.
just bench-data-plane --filter w3

# Run only W1 + R1 (skip chunked-v2 cells).
just bench-data-plane --scenarios w1,r1

# Check whether the host is clear to run without actually running.
just bench-preflight
```

## Pre-flight gate

The bench refuses to run on buildcache if either:

1. `nativelink.service` was (re)started in the last
   `MIN_NATIVELINK_UPTIME = 600s (10 min)`. After a deploy the runtime
   is still warming caches and replaying reconnects; bench numbers
   collected during that window are noise.
2. `journalctl --namespace=nativelink --since='2 min ago' --grep=ByteStream::`
   returns more than `MAX_RECENT_BYTESTREAM_LINES = 500` lines. That
   suggests active production traffic; bench numbers will be
   confounded with prod-traffic-induced contention.

Both checks live in [`src/preflight.rs`](src/preflight.rs). The numeric
thresholds are pinned by a unit test
(`preflight_thresholds_match_documented_values`) so any future drift
between doc and declaration is caught at build time.

If neither check can run (e.g. `systemctl` / `journalctl` absent —
typical on a developer laptop), the gate returns `Inconclusive` and
the bench proceeds. This is deliberate; the gate's purpose is to
protect buildcache, not to harass dev-laptop runs.

## Output format

Each run produces one JSON file under `benchmarks/baselines/`. The
file matches the [`BaselineFile`](src/output.rs) schema:

```json
{
  "metadata": {
    "schema_version": 1,
    "git_commit_sha": "...",
    "git_dirty": false,
    "host": "buildcache",
    "timestamp_utc": "2026-05-16T20:00:00.000000Z",
    "features": ["chunked_fast_slow"],
    "forced": false,
    "temp_dir_used": "/dev/shm"
  },
  "results": [
    {
      "flow_id": "W1",
      "scenario_name": "w1_store_update_oneshot_1MiB_c1",
      "blob_size_bytes": 1048576,
      "concurrency": 1,
      "cache_state": "cold",
      "iters": 20,
      "confidence": "medium",
      "total_duration_ms": 1234.567,
      "latency_ms": {
        "p50": 12.345678,
        "p90": 23.456789,
        "p99": 34.567890,
        "max": 45.678901
      },
      "throughput": {
        "bytes_per_sec": 89012345.678
      },
      "extras": {
        "cas_fast_memory_max_bytes": 16000000000,
        "size_partitioning_threshold": 16384
      }
    }
  ]
}
```

Floats are rounded to 6 decimal places at emit-time (1 ns grid at the
ms scale) so micro-jitter doesn't churn the diff while preserving
sub-µs variance. Keys are alphabetized via `BTreeMap`. The
`confidence` field gates p99 interpretability: `low` (iters < 20)
means p99 is essentially `max`; `medium` (20 ≤ iters < 100) p99 has
one sample of headroom; `high` (iters ≥ 100) p99 has statistical
legitimacy.

## Diffing against a baseline

For Phase 1 we do NOT enforce a regression gate — the design doc
explicitly says "observe → BLOCK after 2 weeks" (user decision Q3).
For now, diff manually:

```bash
diff -u benchmarks/baselines/<old>.json benchmarks/baselines/<new>.json
```

A future commit will add `tools/bench_diff.py` that flattens to CSV +
applies the per-cell thresholds named in design Section 5
(`p50 regress >25% AND new > 5ms → BLOCK`, etc.). That tool is
deliberately deferred until enough baselines accumulate to calibrate
the thresholds against observed noise.

## What's NOT here (deferred to later phases)

Per design Section 6:

- **Phase 2:** M1 (FastSlow slow-write decouple, sustained writer),
  M2 (mirror replication ack latency with 2-worker mock), R4 (worker
  peer-fetch), A1/A2 (AC), C1 (existence-cache micro).
- **Phase 3:** R5 with N=1000, xlarge/huge blobs, W5 (BatchUpdate
  coalescing), F2 (GetTree), dhat allocation-budget bench, RSS-over-
  time sustained-write probe.
- **Phase 4:** S3 / GCS / Redis backend matrix.

## Sub-crate dependencies

The crate depends on every `nativelink-*` lib via path deps so it can
exercise real production composition. It is registered in the root
workspace's `members = ["benchmarks"]` (the only other workspace
members are discovered implicitly via root path-deps).

## Composite invariant

> Data-plane semantics changes (e.g. v3 bundle's `chunked_v2_enabled`
> default flip + per-digest `Notify` + reaper-publish ownership) ship
> with anchoring benchmarks; performance regressions cannot be caught
> automatically without these benchmarks.

The benchmarks themselves are the test. Producing a baseline + diffing
against it on every push is the mechanism that re-establishes the
invariant. The first checked-in baseline file is the anchor.
