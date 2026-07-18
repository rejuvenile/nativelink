# FL-1383 — deploy config deltas (prepped; APPLY AT CANARY, not before)

Ready-to-apply deltas to the canonical `~/fl/bld/infra/nativelink/{buildcache,worker}.json5`. **Do NOT apply until:** the Bazel naming contract (`docs/fl1383-bazel-handoff-naming-contract.md`) is confirmed AND you're greenlighting the single-crate canary. The worker code is inert without `portable_incr.enabled`; these deltas are what turn it on for the allowlisted crate. Store composition detail mirrors `nativelink-config/examples/portable_incr_seed_index.json5`.

Allowlist prefix (all sides): `bazel-out/cfg/bin/third_party/rust/apple_a14/`.

---

## A. `prod-server.json5` (server: index store + AC service instance + prop ignore + ingestion allowlist)

**A1 — add the index store** to `stores[]` (near the CAS stores, ~after `cas_INNER`). `cas_store` MUST reference the same CAS that holds the `-incr` content (`cas_STORE`) so a dangling entry → CAS-NotFound → cold:
```json5
{
  "name": "INCR_SEED_INDEX_STORE",
  "completeness_checking": {
    // AC-shaped MUTABLE backend (targetkey→ActionResult). CANARY: memory is
    // fine (index is best-effort, cold-fallback-safe). For durability across
    // server restarts, back with Redis like AC_BACKEND_CACHED instead.
    "backend": {
      "memory": {
        "eviction_policy": { "max_bytes": 64000000, "max_count": 200000 }
      }
    },
    "cas_store": { "ref_store": { "name": "cas_STORE" } }
  }
}
```

**A2 — expose it as an AC service instance `incr_seed_index`** on the service the workers connect to for CAS/AC (the `worker_cas` listener at `:50071`, where `AC_MAIN_STORE`/`ac_store` is wired). Add an AC config with `instance_name: "incr_seed_index"` → `ac_store: "INCR_SEED_INDEX_STORE"` alongside the existing `main` AC/CAS wiring on that service. (Worker `update_oneshot`/`GetActionResult` on `instance_name=incr_seed_index` route here.)

**A3 — accept the carrier props** (defense-in-depth; the server already STRIPS them unconditionally at ingestion in `execution_server.rs`, so this is belt-and-suspenders). In `MAIN_SCHEDULER.simple.supported_platform_properties` (`~:343`), after `persistentWorkerKey`:
```json5
"nl_incr_targetkey": "ignore",
"nl_incr_primary_output": "ignore"
```

**A4 — server ingestion allowlist** (so the ExecutionServer reads the carrier only for allowlisted actions). On the execution service's `portable_incr` (`ExecutionConfig.portable_incr`, cas_server.rs:345):
```json5
"portable_incr": {
  "enabled": true,
  "allowlist": ["bazel-out/cfg/bin/third_party/rust/apple_a14/"]
}
```
⚠️ CONFIRM the exact JSON path for the execution service block in prod-server.json5 (it's the service that fronts `Execution`/`Capabilities` for the scheduler).

---

## B. `worker.json5` (worker: index grpc store + the four `local` fields)

**B1 — add a grpc store for the index** to `stores[]` (mirror `AC_MAIN_STORE`'s grpc slow tier — `store_type: "ac"`, `instance_name: "incr_seed_index"`, same endpoint/TLS):
```json5
{
  "name": "INCR_SEED_INDEX_STORE",
  "grpc": {
    "instance_name": "incr_seed_index",
    "endpoints": [{
      "address": "grpcs://cache.example.com:50071",
      "tls_config": {
        "cert_file": "/Users/user/Work/nativelink/tls/worker.crt",
        "key_file": "/Users/user/Work/nativelink/tls/worker.key",
        "use_native_roots": true
      }
    }],
    "connections_per_endpoint": 8,
    "rpc_timeout_s": 15,
    "store_type": "ac",
    "retry": { "max_retries": 3, "delay": 0.5, "jitter": 0.5 }
  }
}
```

**B2 — the four `LocalWorkerConfig` fields** in `workers[0].local` (alongside `cas_fast_slow_store`/`upload_action_result`):
```json5
"portable_incr": {
  "enabled": true,
  "allowlist": ["bazel-out/cfg/bin/third_party/rust/apple_a14/"]
},
"portable_incr_fixed_prefix": "/Volumes/CrowAgent/fl-incr-execroots",
"portable_incr_sysroot_path": "<HOST RUSTUP SYSROOT — CONFIRM>",
"portable_incr_seed_index_store": "INCR_SEED_INDEX_STORE"
```
- `portable_incr_fixed_prefix`: provisioned on all 10 workers (`user:staff` 0755, RunAtLoad LaunchDaemon) — same-volume as the worker CAS (`/Volumes`==`/Users` device) → hardlinks, no EXDEV.
- `portable_incr_sysroot_path`: ⚠️ CONFIRM the exact host rustup sysroot (§2: `/Users/user/.rustup/...`, execroot-relative per `.bazelrc:512-513`); §12 asserts it byte-identical at startup, so it must be the real host path (not an `output_base` symlink → rustc realpaths → cold).

---

## C. Canary rollout (after apply)
1. Confirm the Bazel side (naming contract) + apply A+B on **ONE** worker first (not the fleet) — a single `apple-a14` crate.
2. Deploy (`just … deploy`), then WATCH the registered counters: `incr_index_publish` (worker publishing), `incr_index_fetch_hit`/`_miss`, `incr_seed_materialized`, and — once chunk-4 wires it — `incr_reuse_fired`. **A healthy canary = materialized climbing AND (post-chunk-4) reuse_fired climbing.** materialized-up-but-reuse-flat = the naming contract (§2/§3 of the handoff) diverged → dark; stop and reconcile.
3. Watch `<FIXED_PREFIX>` disk (§8 eviction holds it ≤20 GiB) + the `still_over_budget` backpressure warn.
4. KILL-SWITCH: set `portable_incr.enabled=false` on both files + push-config + restart. Feature reverts to inert (byte-identical cold builds).
5. Only widen to the fleet after the single-crate canary shows real reuse + no disk/pin regression.
