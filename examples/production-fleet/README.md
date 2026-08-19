# production-fleet — the configs a real 10-worker fleet runs

These are not synthetic examples. They are the **actual** NativeLink
configuration and deploy orchestration for a running remote-cache /
remote-execution cluster: one Linux server plus ten Apple-silicon (M4)
macOS workers, serving Bazel builds.

Hostnames, usernames, filesystem layout and IP ranges have been replaced
with generic placeholders. **Nothing else was removed** — the comments,
the key ordering, the tuning rationale and the operational war stories are
the originals, because those are the parts that are hard to reconstruct and
the reason this directory exists.

Treat it as a worked example to read and adapt, not as a drop-in.

## Topology this describes

| Placeholder | Role |
|---|---|
| `buildcache` | the server: Linux, systemd unit `nativelink`, ZFS-backed stores, Valkey/Redis over a unix socket |
| `worker-01` … `worker-10` | ten macOS workers, each a `launchd` LaunchDaemon, reachable as `worker-NN.local` |
| `ci-mac-1` … `ci-mac-3`, `ci-linux-1` | CI agents that consume the cache (separate from the workers) |
| `cache.example.com` | the server's public DNS name |
| `git.example.com` | the internal git forge the deploy pulls from |
| `/srv/casdata`, `/srv/bulk`, `/srv/nativelink` | server-side storage roots (CAS content + TLS material, bulk pool, install location) |

Ports: `50051` public Bazel REAPI, `50061` worker API + metrics, `50071`
worker-facing CAS (HTTP/2), `50072` the same over QUIC, `6060` pprof.

## Files

**Server**

- `prod-server.json5` — the server config. Store chain, listeners, TLS,
  eviction, scheduler. This is the single biggest artefact here and the
  most useful thing to read: the store chain is
  `WorkerProxyStore → VerifyStore → ExistenceCacheStore → SizePartitioning
  → {small: MemoryStore→Redis, large: FastSlowStore(MemoryStore→FilesystemStore)}`
  and nearly every constant carries a comment explaining what incident set it.

**Workers**

- `worker.json5` — the production worker config.
- `worker_ultra.json5` — a reduced variant without the memory-pressure gate,
  the portable-incremental-rustc settings, the startup reconcile gate and the
  action-output allowlist. Useful as a smaller starting point.
- `com.example.nativelink-worker.plist` — the LaunchDaemon that supervises the
  worker (system domain, so it starts without a login). Note `ExitTimeOut=300`
  — the worker needs a long SIGTERM→SIGKILL window to spill in-memory mirror
  blobs to local disk on shutdown.
- `nativelink-executor.plist` — the older LaunchAgent-style unit, kept for
  reference; `com.example.nativelink-worker.plist` supersedes it.
- `com.example.nativelink-sysctl.plist` — raises `kern.maxfiles` /
  `kern.maxfilesperproc` to 524288 at boot, tolerating XNU clamping on 16 GB
  M4 hosts.
- `com.example.tmux.plist` — starts a detached tmux session at boot (operator
  convenience, not required).
- `bundle-worker.sh` — wraps the binary in a minimal `.app`. macOS ties the
  Local Network permission to `CFBundleIdentifier`, so a bare binary loses the
  permission on every rebuild; a stable bundle ID keeps it.
- `nativelink-worker.newsyslog.conf` — **deliberately not installed.** Kept
  only to document a bug: pointing `newsyslog` at a launchd-managed log
  renames the file out from under the daemon's open fd 1 and orphans all
  subsequent output. The deploy actively zeroes this file if it finds it. Use
  the in-repo `packaging/macos/rotate-log.sh` copy-and-truncate rotation
  instead.

**Deploy orchestration**

- `justfile` — the whole deploy. `just deploy` runs
  push → pull → build → install-server-binary → bundle → sign → push-config →
  deploy-server-config → restart. Worth reading for the failure handling
  rather than the happy path: per-host log capture, an ssh-unreachable vs
  real-failure classifier, a build-once-and-scp fan-out with post-transfer
  shasum + `--version` verification, and a workers-before-server restart
  order with a real TCP readiness probe.
- `worker_macs.txt.example`, `ci_hosts.txt.example` — the host lists. **The
  justfile reads these from the parent directory** (`../worker_macs.txt`,
  `../ci_hosts.txt`), so copy them one level up and drop the `.example`
  suffix, or edit `worker_list` / `ci_list` at the top of the justfile.

**Monitoring / diagnosis (read-only, zsh, macOS-aware)**

- `sched-peak-capture.sh` — samples scheduler, worker and fetch-path signals
  during a live build to separate "scheduler-capped" from "under-fed" from
  "compute-saturated" from "upstream-fetch-bound".
- `espill-refault-monitor.sh` — watches E-core spill and memory refault rates
  under load, to pick a safe `memory_gate_refault_confirm_rate`.
- `synthetic-core-metric-validate.sh` — pins a known load to a worker's P-cores
  then its E-cores and checks what `p_core_load_pct` / `e_core_load_pct`
  actually report, i.e. validates the metric the scheduler gate trusts.

## What you must change before running any of this

1. **Host lists.** `worker_macs.txt` and `ci_hosts.txt`. The first non-blank
   line of `worker_macs.txt` is the designated *builder* — the fleet's binary
   is compiled there once and fanned out, so it must be the same hardware
   generation as every other worker (see below).
2. **`server`** at the top of the justfile (`buildcache`) and every
   `cache.example.com` / `git.example.com` reference in the json5 configs.
3. **Paths.** `/srv/...` on the server and `/Users/user/...` on the workers.
   In particular `src_dir_server`, `src_dir_worker`, `worker_config` and
   `app_binary`.
4. **The unix user.** The placeholder is the literal string `user` — it
   appears in the sudoers rule, the plists' `UserName`, the newsyslog
   owner column and the log paths. Replace it with your real account.
5. **TLS material.** The configs reference cert/key *paths* only; no key or
   certificate bytes are in this directory. Point `client_ca_file`,
   `*_tls_cert_file` and `*_tls_key_file` at your own PKI. The client certs
   used by the monitoring scripts (`/srv/casdata/nativelink/tls/clients/...`)
   are likewise yours to provision.
6. **The signing step.** `just sign` calls an internal Developer-ID signing
   service (`fl_codesign_client`) that is not part of this repo. Either
   substitute your own `codesign` invocation or drop `sign` from the `deploy`
   recipe. macOS will still run an ad-hoc-signed bundle locally.
7. **Redis/Valkey.** The server config expects a unix socket at
   `/run/valkey/valkey.sock`.

## Sharp edges worth knowing

- **`-C target-cpu=native` + build-once-and-scp.** The builder worker's CPU
  picks the codegen target for the entire fleet. A mixed M1/M2/M3/M4 fleet
  will SIGILL on the older machines unless you pin a floor target first. The
  post-transfer `--version` check in `build` catches this at deploy time.
- **Restart order is load-bearing.** Workers restart before the server.
  Simultaneous restart is the dominant cause of cluster-wide blob loss: the
  server's MemoryStore is not durable and in-flight slow-tier writes are
  killed by SIGTERM.
- **`flush-redis` must stay atomic with the server restart.** Flushing without
  an immediate restart leaves the in-memory ExistenceCache reporting blobs as
  present after they are gone, which turns into `FAILED_PRECONDITION` for any
  action whose input tree references them.
- **`nuke-worker-caches` assumes the server CAS is good.** Do not run it after
  `flush-redis`.
- **NOPASSWD sudo** on every worker is a prerequisite (`just setup-sudoers`),
  scoped to an explicit binary allowlist. Read that list before running it.
