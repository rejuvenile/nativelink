// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Phase 2.1 SKELETON for #212 chunked-streaming architecture.
//!
//! Per-chunk write-at-offset + atomic-commit primitives that the
//! Phase 2.3 per-blob driver will consume. NOT yet wired into the
//! `StoreDriver` trait — these are `pub(crate)` APIs internal to
//! `nativelink-store` per design Q10=(a) (chunked APIs internal to
//! FastSlowStore + FilesystemStore + GrpcStore only, no trait-surface
//! changes).
//!
//! Design source of truth: `.claude/plans/212-chunk-pinned-async-slow-writes.md`
//! (v4.5). The decisions that bear on this file:
//!
//! - **Q2=(a) sparse file + `pwrite`-at-offset** (in-memory-only sidecar
//!   per v4.5). One inode per partial; out-of-order writes safe because
//!   each `pwrite` is atomic per syscall. Sparse semantics extend the
//!   file to the highest-offset write; the kernel + ZFS handle the gap.
//! - **Q3=(a) 1 MiB chunk size** matches the ZFS `recordsize=1M` on
//!   `fast/nativelink/work` exactly — no partial-record write
//!   amplification.
//! - **Q7=(c) drop partial-recoverable read; trust file length only on
//!   recovery; NO `fsync`.** Crash recovery just `stat()`s the partial
//!   and either renames-to-final (if `len == declared_size`) or GCs
//!   (mirror re-uploads). NO synchronous-write primitives anywhere —
//!   ZFS `sync=disabled` is the durability model.
//! - **Q10=(a) chunked APIs internal.** No `StoreDriver` change; these
//!   methods are `pub(crate)` and consumed only by Phase 2.3's per-blob
//!   driver.
//!
//! Hard rules per CLAUDE.md re-stated for the implementer:
//! - NO `fsync`, `fdatasync`, `sync_file_range`, `msync`, `O_SYNC`,
//!   `O_DSYNC`, `O_DIRECT`. Audit any new dependency for these.
//! - NEVER block a tokio worker thread. All file I/O goes through
//!   `tokio::task::spawn_blocking` (sync `pwrite` is the natural
//!   primitive; `tokio::fs::File` does not expose `pwrite` directly).
//! - Never hold locks across `.await`. Per-blob serialization uses a
//!   `tokio::sync::Mutex` so the lock guard CAN await on the
//!   spawn_blocking join handle inside the critical section.
//!
//! Phase 2.3 will:
//! - Plumb these APIs into the per-blob `ChunkedDriver` (currently the
//!   skeleton in `chunked_driver.rs` increments a counter + drops).
//! - Wire driver completion into `commit_chunked` / `discard_chunked`
//!   from the §6.7 termination triggers.
//! - Add length-bitmap correlation in the in-memory sidecar so the
//!   driver can detect "all chunks landed → commit" without re-scanning
//!   the disk.

// Phase 2.1 ships the file-system primitives; the call-sites land in
// Phase 2.3 (per-blob driver) per §6.7. The dead-code allow disappears
// when Phase 2.3 wires the driver to consume `write_chunk_at_offset`
// / `commit_chunked` / `discard_chunked`.
#![allow(dead_code, reason = "Phase 2.1 SKELETON; consumers land in Phase 2.3 (#212)")]

use core::fmt::Debug;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err};
use nativelink_util::common::DigestInfo;
use nativelink_util::spawn_rate_probe::{record, SpawnSite};
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, warn};

use crate::filesystem_store::digest_shard_prefix;

/// #213 NMA2 test hook: per-digest millisecond delay injected at
/// the start of [`write_chunk_at_offset`] (test builds only). Tests
/// that exercise the chunked_driver's per-chunk pwrite timeout
/// register their digest here so the await sleeps for `delay_ms`
/// before opening the file — long enough for
/// `tokio::time::timeout(per_chunk_timeout, ...)` to fire
/// deterministically. The map is keyed by `DigestInfo` so parallel
/// tests never collide (each test uses a unique digest).
///
/// Production builds compile out the lookup via the `#[cfg(test)]`
/// block in `write_chunk_at_offset`; the symbol exists only under
/// `#[cfg(test)]`.
///
/// Tests should clean up via `TEST_PRE_WRITE_DELAY_MS_BY_DIGEST.lock().remove(&digest)`
/// or use a manual `Drop`-based scope guard so a panic doesn't leak
/// the entry.
///
/// #213 reviewer M4 fixup: switched from
/// `Mutex<Option<HashMap<...>>>` to `LazyLock<Mutex<HashMap<...>>>`
/// so the `Option` unwrap dance disappears at the call site
/// (matches the workspace idiom in `metrics.rs`, `pin_budget.rs`,
/// `dedup_store.rs`, `worker_proxy_store.rs`).
#[cfg(test)]
pub(crate) static TEST_PRE_WRITE_DELAY_MS_BY_DIGEST: std::sync::LazyLock<
    parking_lot::Mutex<HashMap<DigestInfo, u64>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// #213 reviewer round-2 MAJOR-B (M2 mutation test): per-digest
/// millisecond delay injected at the start of [`discard_chunked`]
/// (test builds + the `test-utils` feature). Tests that exercise the
/// driver/handler post-error cleanup contract — bound on
/// `discard_chunked` / `unlink_holding` awaits via
/// `tokio::time::timeout(...)` — register their digest here so the
/// discard sleeps for `delay_ms` before removing the file. Combined
/// with a `tokio::time::timeout(...)` wrap around the discard, this
/// lets the test verify the wrap FIRES (the timeout-Elapsed branch
/// is reached) without needing to wedge an actual filesystem.
///
/// Visibility: gated on `#[cfg(any(test, feature = "test-utils"))]`
/// so cross-crate integration tests in `nativelink-service` can
/// reach it (via the wrapper APIs on `FilesystemStore`); production
/// binaries (no `test-utils` feature, no `cfg(test)`) compile the
/// lookup out entirely.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) static TEST_PRE_DISCARD_DELAY_MS_BY_DIGEST: std::sync::LazyLock<
    parking_lot::Mutex<HashMap<DigestInfo, u64>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// In-flight chunked-blob state held by the FilesystemStore for the
/// lifetime of an in-progress chunked upload. One entry per digest.
///
/// The `file` field is wrapped in a `tokio::sync::Mutex` (not
/// `parking_lot::Mutex`) because callers acquire the lock and then
/// immediately `.await` on a `spawn_blocking` join handle to do the
/// actual `pwrite`. A `parking_lot::Mutex` cannot be held across an
/// `.await` per CLAUDE.md ("Async & Concurrency"); a `tokio::sync::Mutex`
/// can. Critical-section length is one `pwrite` syscall (~µs on local
/// disk, ~ms worst-case under contention), short enough that the
/// async-mutex overhead is negligible.
///
/// `try_clone` is NOT used to allow parallel pwrites — the design's
/// per-blob lock invariant says concurrent chunks for the same blob
/// serialize. Cross-blob parallelism is preserved by the `HashMap` key
/// (different digests → different `ChunkInProgress` → no contention).
///
/// `path` is held for `discard_chunked` (file removal) and
/// `commit_chunked` (rename source). Stored as `PathBuf` rather than
/// recomputing each call, both to centralize the layout decision and
/// to avoid recomputing the shard prefix on every chunk.
pub(crate) struct ChunkInProgress {
    /// Absolute on-disk path to the partial temp file:
    /// `<temp_path>/d/<XX>/<hash>-<size>.partial`. The shard prefix
    /// matches the layout used by the rest of `FilesystemStore` so
    /// recovery + tooling can reuse the existing directory walks.
    path: PathBuf,
    /// `std::fs::File` (not `tokio::fs::File`) because every operation
    /// happens inside `spawn_blocking` and the std handle is what the
    /// `std::os::unix::fs::FileExt::write_at` (Unix `pwrite`) call
    /// requires. Wrapped in an async-mutex so concurrent `write_chunk_at_offset`
    /// callers serialize cleanly without blocking a tokio worker.
    file: AsyncMutex<std::fs::File>,
    /// Pinned digest size copy. Not load-bearing for `write_chunk_at_offset`
    /// (the caller passes `expected_size` to `commit_chunked`) but
    /// useful for log messages.
    declared_size: u64,
}

impl Debug for ChunkInProgress {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChunkInProgress")
            .field("path", &self.path)
            .field("declared_size", &self.declared_size)
            .field("file", &"<async-mutex<std::fs::File>>")
            .finish()
    }
}

/// Process-internal map of digest → in-flight chunked partial. Owned
/// by `FilesystemStore` (one per store; chunked partials are tied to
/// the store's `temp_path` layout).
///
/// `parking_lot::Mutex` is correct: every critical section is short
/// (HashMap insert / remove / get-and-clone) and never holds across
/// an `.await`. The per-blob `ChunkInProgress` is `Arc`-shared so the
/// outer map lock releases immediately after the `Arc::clone`, leaving
/// the long-running `pwrite` to serialize only on the per-blob async
/// mutex inside.
#[derive(Debug, Default)]
pub(crate) struct ChunkedPartialsMap {
    inner: Mutex<HashMap<DigestInfo, Arc<ChunkInProgress>>>,
}

impl ChunkedPartialsMap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns the current in-flight chunked-blob count. Test + future
    /// metric hook; non-load-bearing for the I/O path.
    #[allow(dead_code, reason = "wired in Phase 2.3 driver instrumentation")]
    pub(crate) fn in_flight_count(&self) -> usize {
        self.inner.lock().len()
    }

    /// Returns `true` if the given digest currently has an in-flight
    /// chunked partial. Used by tests; Phase 2.3 driver checks via
    /// the `commit_chunked` / `discard_chunked` return code instead.
    #[allow(dead_code, reason = "test-only observation; Phase 2.3 may use during drain")]
    pub(crate) fn contains(&self, digest: &DigestInfo) -> bool {
        self.inner.lock().contains_key(digest)
    }
}

/// Build the on-disk path for a partial chunked upload. Layout:
/// `<temp_path>/d/<XX>/<hash>-<size>.partial`. The `XX` shard matches
/// the existing `digest_shard_prefix` used by the legacy temp / content
/// layout (created by `create_subdirs` during `FilesystemStore::new`),
/// so the directories are guaranteed to exist before the first call.
///
/// `.partial` suffix exists for two reasons: (1) it makes the recovery
/// sweep's filename match (`*.partial`) cheap and unambiguous, (2) it
/// guarantees a chunked-partial file path NEVER collides with a
/// finalized CAS file path inside `temp_path` (the legacy `update`
/// flow uses `make_temp_key` to mint a fresh randomized digest and
/// writes to the same directory; without the `.partial` suffix a
/// recovery sweep based on filename pattern alone could mistakenly
/// GC or rename a legacy temp file).
pub(crate) fn partial_temp_path(temp_path_root: &str, digest: &DigestInfo) -> PathBuf {
    // Reuse `digest_shard_prefix` from `filesystem_store` so the on-disk
    // shard-layout decision lives in exactly one place. If the legacy
    // CAS layout ever changes (e.g. 4-byte sharding for very large
    // stores), the chunked partials follow automatically — and the
    // `unsafe { from_utf8_unchecked }` block exists in only one file
    // (rust-crate-reviewer M1).
    let shard_arr = digest_shard_prefix(digest);
    // SAFETY: shard_arr bytes are sourced from HEX_LUT (ASCII) inside
    // `digest_shard_prefix`. Same invariant as `to_full_path_from_key`.
    let shard_str = unsafe { core::str::from_utf8_unchecked(&shard_arr) };
    let mut path = PathBuf::from(temp_path_root);
    path.push("d");
    path.push(shard_str);
    path.push(format!("{digest}.partial"));
    path
}

/// B1 fixup: build the on-disk path for the *holding* file produced by
/// the first stage of the two-stage rename (post-pwrite, pre-SHA-256
/// verify). Layout: `<content_path>/d/<XX>/<hash>-<size>.holding`.
///
/// The holding file lives under `content_path` (NOT `temp_path`) because
/// the final atomic rename in stage 2 is `holding → final`; same-
/// directory rename is atomic on Linux + macOS regardless of fs type.
/// Putting the holding file in `temp_path` would force the stage-2 rename
/// to cross filesystems if `temp_path` and `content_path` happen to be
/// on different mounts, which would convert the rename into a copy +
/// unlink (non-atomic, and may also fail on EXDEV).
///
/// The `.holding` suffix prevents read-side traffic from accidentally
/// resolving to a not-yet-verified file: `FilesystemStore::has` /
/// `get_part` look up keys via `to_full_path_from_key` which produces
/// `<digest>` (no suffix), so a `<digest>.holding` file is invisible.
///
/// Crash recovery: any `.holding` file from a process killed between
/// stage 1 and stage 2 is GC'd by `prune_holding_partials` on the next
/// `FilesystemStore::new`.
pub(crate) fn holding_content_path(content_path_root: &str, digest: &DigestInfo) -> PathBuf {
    let shard_arr = digest_shard_prefix(digest);
    // SAFETY: shard_arr bytes are sourced from HEX_LUT (ASCII).
    let shard_str = unsafe { core::str::from_utf8_unchecked(&shard_arr) };
    let mut path = PathBuf::from(content_path_root);
    path.push("d");
    path.push(shard_str);
    path.push(format!("{digest}.holding"));
    path
}

/// B1 fixup: GC any leftover `.holding` files under `content_path` from
/// a previous process killed between stage 1 (rename to `.holding`) and
/// stage 2 (rename to canonical). Called from `FilesystemStore::new`
/// AFTER `prune_temp_path` (which sweeps `<temp_path>/d/`).
///
/// Best-effort sweep: errors per-file are logged at `warn!` and skipped;
/// a missing shard directory is OK (first-startup, before
/// `create_subdirs` ran).
pub(crate) async fn prune_holding_partials(content_path_root: &str) -> Result<(), Error> {
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReadDirStream;

    let digest_dir = format!("{content_path_root}/d");
    for byte in 0u8..=255 {
        let shard_dir = format!("{digest_dir}/{byte:02x}");
        let read_dir = match tokio::fs::read_dir(&shard_dir).await {
            Ok(rd) => rd,
            Err(_) => continue, // shard dir absent; ignore
        };
        let mut stream = ReadDirStream::new(read_dir);
        while let Some(entry_res) = stream.next().await {
            let entry = match entry_res {
                Ok(e) => e,
                Err(err) => {
                    warn!(
                        ?shard_dir,
                        ?err,
                        "prune_holding_partials: read_dir entry error; skipping"
                    );
                    continue;
                }
            };
            let path = entry.path();
            // Only sweep `*.holding` files; never touch canonical CAS
            // entries (which have no suffix).
            let is_holding = path
                .extension()
                .map(|ext| ext == "holding")
                .unwrap_or(false);
            if !is_holding {
                continue;
            }
            if let Err(err) = tokio::fs::remove_file(&path).await {
                warn!(
                    ?path,
                    ?err,
                    "prune_holding_partials: failed to unlink leftover .holding file; skipping"
                );
            }
        }
    }
    Ok(())
}

/// Open or create the sparse temp file for a chunked partial. Idempotent
/// for the same digest within one process: a second call returns the
/// existing `ChunkInProgress` from the map without re-opening the file.
///
/// Cross-process, this would NOT be safe (two processes opening the
/// same path with `create+write` would race). Per design §7.3 there is
/// only one writer process per `FilesystemStore` deployment; the
/// recovery sweep on startup GCs any stale `.partial` files left from
/// a previous process.
async fn open_or_create_partial(
    map: &ChunkedPartialsMap,
    digest: DigestInfo,
    temp_path_root: &str,
) -> Result<Arc<ChunkInProgress>, Error> {
    // Fast path: entry already exists. Avoid the spawn_blocking on the
    // hot per-chunk path when the file is already open.
    if let Some(existing) = map.inner.lock().get(&digest).cloned() {
        return Ok(existing);
    }

    // Slow path: open the file. Done in `spawn_blocking` because
    // `OpenOptions::open` is a blocking syscall that on a slow ZFS
    // pool can take milliseconds.
    let path = partial_temp_path(temp_path_root, &digest);
    let path_for_blocking = path.clone();
    let opened = tokio::task::spawn_blocking(move || -> Result<std::fs::File, std::io::Error> {
        // create(true) + write(true) — sparse semantics handled by the
        // kernel + ZFS on first pwrite at a non-zero offset. NO O_SYNC,
        // NO O_DSYNC, NO O_DIRECT (CLAUDE.md hard rule).
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path_for_blocking)
    })
    .await
    .map_err(|join_err| make_err!(Code::Internal, "spawn_blocking join error opening chunked partial: {join_err:?}"))?
    .map_err(|io_err| {
        make_err!(
            Code::Internal,
            "failed to open chunked partial temp file {}: {io_err:?}",
            path.display()
        )
    })?;

    let new_entry = Arc::new(ChunkInProgress {
        path,
        file: AsyncMutex::new(opened),
        declared_size: digest.size_bytes(),
    });

    // Race resolution: another caller may have created the entry
    // between our `cloned()` check above and the `open_or_create_partial`
    // re-acquire here. If so, drop our just-opened file and use theirs.
    // Cost: one extra `open` + `close` syscall for the loser. Not on
    // the steady-state hot path (only first chunk per digest).
    let mut guard = map.inner.lock();
    if let Some(existing) = guard.get(&digest).cloned() {
        debug!(?digest, "chunked partial open race resolved; using existing entry");
        drop(guard);
        // `new_entry`'s file dropped here — the actual on-disk file
        // remains (both opens went to the SAME path so the second open
        // just re-opened the same inode). The race-loser's open
        // dropped its fd; the winner still holds theirs.
        return Ok(existing);
    }
    guard.insert(digest, Arc::clone(&new_entry));
    Ok(new_entry)
}

/// Write one chunk at the given byte offset into the sparse partial.
/// Per Q2=(a): out-of-order safe via `pwrite` (atomic per syscall on
/// Linux + macOS), and the per-blob async mutex serializes concurrent
/// callers within the same digest.
///
/// On first chunk for a digest: opens / creates the temp file with
/// sparse semantics. On subsequent chunks: reuses the open fd from
/// the map.
///
/// **NO `fsync`** anywhere in this function or the call chain. The
/// CLAUDE.md hard rule (2026-04-28) prohibits sync-write primitives;
/// ZFS `sync=disabled` is the durability model. Mirror + BIS ack
/// covers crash-recovery durability.
pub(crate) async fn write_chunk_at_offset(
    map: &ChunkedPartialsMap,
    temp_path_root: &str,
    digest: &DigestInfo,
    chunk_offset: u64,
    chunk_bytes: Bytes,
) -> Result<(), Error> {
    if chunk_bytes.is_empty() {
        // Defensive: a zero-length chunk is a no-op. Don't open the
        // file for nothing. Phase 2.3 driver should never produce
        // zero-length chunks but the contract here is permissive.
        //
        // **Load-bearing for FIXME(#218) zero-byte fast-path**: this
        // early-return means a true zero-byte blob never opens a
        // partial, so `commit_chunked_to_holding`'s `!has_entry`
        // predicate (the gate for the zero-byte fast-path) is
        // satisfied by construction. If a future change makes this
        // branch open + close an empty partial, the fast-path's
        // `!has_entry` check would flip false and the path would
        // fall through to the length-check branch — still correct
        // (length 0 == expected_size 0 → rename succeeds), but
        // changes the operational profile. Update both sites
        // together if you alter this contract.
        return Ok(());
    }

    // #213 NMA2 test hook: when the per-digest test-only delay is set,
    // sleep BEFORE the actual write so the chunked_driver's per-chunk
    // `tokio::time::timeout(per_chunk_timeout, ...)` can fire
    // deterministically. Per-digest scoping keeps parallel tests from
    // bleeding into each other. Production binaries compile this branch
    // out via `#[cfg(test)]`.
    #[cfg(test)]
    let delay = TEST_PRE_WRITE_DELAY_MS_BY_DIGEST.lock().get(digest).copied();
    #[cfg(test)]
    if let Some(delay_ms) = delay {
        if delay_ms > 0 {
            tokio::time::sleep(core::time::Duration::from_millis(delay_ms)).await;
        }
    }

    let entry = open_or_create_partial(map, *digest, temp_path_root).await?;

    // Acquire the per-blob async mutex. We hold this across the
    // spawn_blocking await so that concurrent chunks for the SAME
    // digest serialize on the file handle. Different digests use
    // different `ChunkInProgress` instances and never contend.
    //
    // Cloning `chunk_bytes` is cheap (Bytes is refcounted). The actual
    // payload moves into the spawn_blocking closure.
    let len = chunk_bytes.len();
    // #449 inline split: time the mutex acquire as the first sub-stage
    // of the back-edge decomposition. Production p99 of `back_edge_ms`
    // (chunked_driver.rs:992) is 1216 ms (max 20130 ms); this probe +
    // the dispatch + pwrite probes inside the spawn_blocking closure
    // attribute that cost across (a) per-blob async-mutex acquire
    // (concurrent same-digest writes), (b) spawn_blocking pool queue
    // wait, (c) actual pwrite syscall — so the operator can tell which
    // sub-stage drives the tail before either #448 chmod-publish or
    // #449 full instrumentation are scoped.
    let mutex_started = Instant::now();
    let file_guard = entry.file.lock().await;
    let mutex_acquire_us = mutex_started.elapsed().as_micros() as u64;

    // SAFETY of write_at: the `std::os::unix::fs::FileExt::write_at`
    // method writes `bytes.len()` bytes at the given offset. It does
    // NOT use the file's seek position, so concurrent calls on the
    // SAME fd at DIFFERENT offsets are safe at the syscall level
    // (Linux + macOS). The async-mutex above is belt-and-suspenders
    // for cross-call ordering and short-write retries.
    let bytes_for_blocking = chunk_bytes;
    // We need a `try_clone` of the file so we can move it into the
    // spawn_blocking and still keep the mutex guard's exclusive
    // access pattern intact. Alternative: take the lock around a
    // `&std::fs::File` reference and pass `&` into spawn_blocking —
    // but the spawned closure has a `'static` bound, so we need an
    // owned handle. `try_clone` is cheap (one `dup` syscall) and
    // gives us another fd referring to the same open-file-description.
    let file_clone = file_guard
        .try_clone()
        .map_err(|io_err| make_err!(Code::Internal, "try_clone for chunked pwrite failed: {io_err:?}"))?;

    // #239 instrumentation: record spawn_blocking inter-arrival at the
    // chunked-pwrite hot site (fired once per chunk written). Probe is
    // sync, no .await, μs hold of a parking_lot::Mutex; safe before a
    // spawn_blocking submission. See `spawn_rate_probe.rs`.
    record(SpawnSite::ChunkedPwrite);
    // #449 inline split: capture spawn_blocking submission timestamp.
    // The closure reads `dispatch_submitted.elapsed()` as its first
    // statement to attribute the wall-clock cost between submission
    // and first poll (= pool-queue wait + blocking-pool internal mutex
    // contention). Returned alongside pwrite_us via the closure's
    // success tuple — no Arc<AtomicU64> allocation per chunk.
    let dispatch_submitted = Instant::now();
    let result = tokio::task::spawn_blocking(move || -> Result<(u64, u64), std::io::Error> {
        // First statement: dispatch latency (submit → first poll).
        let dispatch_us = dispatch_submitted.elapsed().as_micros() as u64;
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::FileExt;
            // `write_at` performs a `pwrite(2)`. A short write returns
            // `Ok(written < bytes.len())`; loop until the full buffer
            // is consumed. Per pwrite(2) man page: "If the file
            // offset is past the end of the file, the file shall be
            // extended" — sparse semantics on the gap.
            let pwrite_started = Instant::now();
            let mut written = 0usize;
            let bytes_slice = bytes_for_blocking.as_ref();
            while written < bytes_slice.len() {
                let n = file_clone.write_at(
                    &bytes_slice[written..],
                    chunk_offset + written as u64,
                )?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "pwrite returned 0 bytes — disk full or ENOSPC?",
                    ));
                }
                written += n;
            }
            let pwrite_us = pwrite_started.elapsed().as_micros() as u64;
            Ok((dispatch_us, pwrite_us))
        }
        #[cfg(not(target_family = "unix"))]
        {
            // No portable `pwrite` on non-unix; the chunked-fast-slow
            // feature is currently only built for unix targets. Phase
            // 2.x will revisit if Windows support becomes a goal.
            let _ = (file_clone, bytes_for_blocking, chunk_offset, dispatch_us);
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "chunked pwrite-at-offset not supported on non-unix targets",
            ))
        }
    })
    .await;

    drop(file_guard);

    let blocking_result = result.map_err(|join_err| {
        make_err!(Code::Internal, "spawn_blocking join error in chunked pwrite: {join_err:?}")
    })?;

    let (dispatch_us, pwrite_us) = blocking_result.map_err(|io_err| {
        make_err!(
            Code::Internal,
            "chunked pwrite at offset {chunk_offset} ({len} bytes) failed: {io_err:?}"
        )
    })?;

    // #449 inline split: emit a decomposition warn when the sum of the
    // three sub-stages exceeds the same 50 ms threshold the driver's
    // `back_edge_ms` warn (chunked_driver.rs:993) uses. Operator
    // correlates the two warns by `digest` + `offset` and reads which
    // sub-stage(s) dominated. Threshold matches so this fires on the
    // same chunks the driver flags — gives the decomposition without
    // changing the operator's existing alert volume baseline.
    //
    // Silence-is-diagnostic: `total_inner_us` measures only mutex +
    // dispatch + pwrite, which excludes four spans the driver's
    // `back_edge_ms` includes: `open_or_create_partial.await` at
    // `:458`, `try_clone()` at `:495`, the JoinHandle re-poll between
    // closure return and `.await` resuming, and `drop(file_guard)` at
    // `:555`. If the driver warn fires WITHOUT this decomposition warn,
    // the cost lives in one of those four — that absence IS the
    // operator-actionable signal pointing at FS overhead, dup syscall
    // contention, scheduler resume latency, or mutex teardown.
    //
    // Cost: 3 extra `Instant::now()` calls per chunk (~30 ns) + one
    // `as_micros()` × 3 (~10 ns) + one warn at high threshold (only on
    // the slow tail). Negligible vs the 1 MiB pwrite cost itself.
    let total_inner_us = mutex_acquire_us + dispatch_us + pwrite_us;
    if total_inner_us > 50_000 {
        warn!(
            target: "nativelink_store::chunked",
            ?digest,
            offset = chunk_offset,
            chunk_bytes = len,
            mutex_acquire_ms = mutex_acquire_us / 1000,
            dispatch_ms = dispatch_us / 1000,
            pwrite_ms = pwrite_us / 1000,
            total_inner_ms = total_inner_us / 1000,
            "per-chunk back-edge decomposed (#449 inline-split): mutex/dispatch/pwrite",
        );
    }

    Ok(())
}

/// B1 fixup: stage 1 of the two-stage commit. Verifies the partial's
/// actual length matches `expected_size`, then atomically renames
/// `<temp_path>/d/XX/<digest>.partial` → `<content_path>/d/XX/<digest>.holding`.
///
/// **Crucially does NOT yet land at the canonical CAS path.** End-to-end
/// SHA-256 verification runs against the `.holding` file in the driver;
/// only on hash match does stage 2 (`finalize_holding`) atomically
/// rename `.holding` → `<digest>` (the canonical name).
///
/// **Why two stages:** before this fixup, `commit_chunked` renamed
/// directly to the canonical path and THEN re-opened the file to verify
/// SHA-256. If the handler future was cancelled (tonic RST, upstream
/// timeout) between rename and verify, the only `Arc<ChunkedDriver>`
/// dropped, the spawned task was aborted via `JoinHandleDropGuard`, and
/// the file sat at the canonical CAS path — fully readable, chmod 0o555,
/// but never end-to-end-verified. Per-chunk SHA-256 only verifies that
/// each chunk matches the producer's per-chunk hash (which a malicious
/// producer can lie about consistently). Result: CAS poisoning. The
/// two-stage rename collapses the cancellation window — the abort can
/// at worst leave a `.holding` file that `prune_holding_partials` GCs
/// on next startup; the canonical path is only created AFTER hash match.
///
/// On length mismatch: returns `Err(Code::InvalidArgument, ...)` and
/// LEAVES the temp file in place + the in-flight state entry in the
/// map. The caller (driver) is responsible for cleanup via
/// `discard_chunked`. Same on rename failure.
///
/// **DOES NOT remove the in-flight state entry.** That happens in
/// `finalize_holding` (stage 2) AFTER the SHA-256 verify. Holding the
/// in-flight entry across the verify avoids the cancellation race
/// described above (M-perf-3).
pub(crate) async fn commit_chunked_to_holding(
    map: &ChunkedPartialsMap,
    digest: &DigestInfo,
    expected_size: u64,
    holding_path: PathBuf,
) -> Result<(), Error> {
    // #218 zero-byte fast-path: a zero-length blob has nothing to write,
    // so `write_chunk_at_offset` correctly short-circuits to `Ok(())` for
    // empty input (see line ~423) — meaning no `open_or_create_partial`
    // ever fires and the in-flight map stays empty for a true zero-byte
    // blob. Without this fast-path, the map-lookup below would fail with
    // `NotFound`. Per the #212 spec ("zero-byte commit must succeed via
    // empty-file rename"), we directly create an empty `.holding` file at
    // `holding_path`. Stage 2 (`finalize_holding`) then renames it to the
    // canonical CAS path and chmods to 0o555.
    //
    // We only take the fast-path when no in-flight entry exists. If a
    // caller did manage to open a partial for a zero-size blob (e.g. via
    // an explicit `open_or_create_partial`), the existing length-check
    // path below correctly handles it: `actual_len == 0 == expected_size`
    // → rename succeeds. So we don't disturb that case.
    if expected_size == 0 {
        let has_entry = map.inner.lock().contains_key(digest);
        if !has_entry {
            let to_path_for_blocking = holding_path.clone();
            tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
                // `create_new(true)` would refuse to overwrite a stale
                // `.holding` file; we use the default `create(true)` here
                // because `prune_holding_partials` GCs leftover holding
                // files at startup, and a duplicate same-process commit
                // is harmless (file is empty, atomic, idempotent).
                std::fs::File::create(&to_path_for_blocking).map(|_| ())
            })
            .await
            .map_err(|join_err| {
                make_err!(
                    Code::Internal,
                    "spawn_blocking join error in commit_chunked_to_holding zero-byte fast-path: {join_err:?}"
                )
            })?
            .map_err(|io_err| {
                make_err!(
                    Code::Internal,
                    "failed to create empty holding file {} for zero-byte commit: {io_err:?}",
                    holding_path.display()
                )
            })?;
            return Ok(());
        }
    }

    // Get the entry but DO NOT remove it yet — if length validation
    // fails we want the entry to remain so `discard_chunked` (called
    // by the caller) can find it.
    let entry = {
        let guard = map.inner.lock();
        guard.get(digest).cloned()
    };
    let entry = entry.ok_or_else(|| {
        make_err!(
            Code::NotFound,
            "no in-flight chunked partial for {digest} — already committed, discarded, or never opened"
        )
    })?;

    // Stat the file to verify length. spawn_blocking because
    // `metadata()` is a syscall and on a slow pool can take ms.
    let path_for_stat = entry.path.clone();
    let actual_len = tokio::task::spawn_blocking(move || -> Result<u64, std::io::Error> {
        std::fs::metadata(&path_for_stat).map(|m| m.len())
    })
    .await
    .map_err(|join_err| {
        make_err!(Code::Internal, "spawn_blocking join error in commit_chunked_to_holding stat: {join_err:?}")
    })?
    .map_err(|io_err| {
        make_err!(
            Code::Internal,
            "failed to stat chunked partial {} during commit_to_holding: {io_err:?}",
            entry.path.display()
        )
    })?;

    if actual_len != expected_size {
        // Per spec: leave the temp file in place. Caller calls
        // discard_chunked. DO NOT remove from map.
        warn!(
            ?digest,
            actual_len,
            expected_size,
            "chunked commit length mismatch; partial left in place for caller to discard"
        );
        return Err(make_err!(
            Code::InvalidArgument,
            "chunked commit length mismatch: temp={actual_len} expected={expected_size}"
        ));
    }

    // Length OK. Atomic rename to the holding path inside spawn_blocking
    // so we don't block a tokio worker on the rename syscall.
    let from_path = entry.path.clone();
    let to_path_for_blocking = holding_path.clone();
    tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
        std::fs::rename(&from_path, &to_path_for_blocking)
    })
    .await
    .map_err(|join_err| {
        make_err!(Code::Internal, "spawn_blocking join error in commit_chunked_to_holding rename: {join_err:?}")
    })?
    .map_err(|io_err| {
        make_err!(
            Code::Internal,
            "failed to rename chunked partial {} -> {} (holding): {io_err:?}",
            entry.path.display(),
            holding_path.display()
        )
    })?;

    // DO NOT remove from the in-flight map here — `finalize_holding`
    // does that after the SHA-256 verify (M-perf-3 + B1 coupling).
    Ok(())
}

/// B1 fixup: stage 2 of the two-stage commit. Atomically renames
/// `<content_path>/d/XX/<digest>.holding` → `<content_path>/d/XX/<digest>`
/// (canonical CAS path) AND chmods to 0o555 to match the existing
/// `emplace_file` convention. Removes the in-flight state entry from
/// the map ONLY after the rename succeeds.
///
/// Same-directory rename is atomic on Linux + macOS regardless of the
/// underlying filesystem (POSIX guarantee); `holding_content_path` and
/// the canonical path are intentionally co-located in the shard
/// directory.
pub(crate) async fn finalize_holding(
    map: &ChunkedPartialsMap,
    digest: &DigestInfo,
    holding_path: PathBuf,
    final_path: PathBuf,
) -> Result<(), Error> {
    let from_path = holding_path.clone();
    let to_path = final_path.clone();
    tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
        std::fs::rename(&from_path, &to_path)?;
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o555);
            if let Err(err) = std::fs::set_permissions(&to_path, perms) {
                tracing::warn!(?err, path = ?to_path, "Failed to set CAS file permissions to 0o555 after chunked commit (stage 2)");
            }
        }
        Ok(())
    })
    .await
    .map_err(|join_err| {
        make_err!(Code::Internal, "spawn_blocking join error in finalize_holding rename: {join_err:?}")
    })?
    .map_err(|io_err| {
        make_err!(
            Code::Internal,
            "failed to rename holding {} -> final {}: {io_err:?}",
            holding_path.display(),
            final_path.display()
        )
    })?;

    // Remove from the in-flight map AFTER the final rename succeeds.
    // Drop the Arc<ChunkInProgress> after the lock is released to keep
    // the lock critical section short. The file fd inside
    // `ChunkInProgress` closes on the last Arc drop; the partial path
    // it referred to has already been renamed → unlinked from its
    // original directory entry, so the closing fd is a no-op WRT the
    // canonical CAS file (a different inode at this point).
    let removed = map.inner.lock().remove(digest);
    drop(removed);
    Ok(())
}

/// B1 fixup: best-effort unlink of the holding file (used on SHA-256
/// mismatch in stage 2). Idempotent: NotFound is treated as success.
/// Does NOT touch the in-flight state entry; the caller is expected to
/// follow up with `discard_chunked` to drop the (now-renamed-away)
/// in-flight tracker entry.
pub(crate) async fn unlink_holding(holding_path: PathBuf) -> Result<(), Error> {
    let path_for_blocking = holding_path.clone();
    tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
        match std::fs::remove_file(&path_for_blocking) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    })
    .await
    .map_err(|join_err| {
        make_err!(Code::Internal, "spawn_blocking join error in unlink_holding: {join_err:?}")
    })?
    .map_err(|io_err| {
        make_err!(
            Code::Internal,
            "failed to unlink holding {}: {io_err:?}",
            holding_path.display()
        )
    })
}

/// Discard an in-flight chunked partial: remove the temp file +
/// remove the in-flight state entry. Idempotent — safe to call when
/// the digest has no in-flight state (returns Ok with no error;
/// commit-after-discard is the failure case, not double-discard).
///
/// Used by:
/// - The Phase 2.3 driver on a length-mismatch `commit_chunked`
///   failure.
/// - The §6.7 termination triggers (panic, shutdown, retry exhaustion).
/// - The startup recovery sweep (per Q7=(c)) when finding stale
///   `.partial` files from a previous process.
pub(crate) async fn discard_chunked(
    map: &ChunkedPartialsMap,
    digest: &DigestInfo,
) -> Result<(), Error> {
    // #213 reviewer round-2 MAJOR-B test hook: when the per-digest
    // test-only delay is set, sleep BEFORE the actual discard so the
    // driver/handler `tokio::time::timeout(DISCARD_AFTER_FAILURE_TIMEOUT,
    // ...)` / `tokio::time::timeout(DISCARD_PARTIAL_TIMEOUT, ...)` wraps
    // can fire deterministically. Per-digest scoping keeps parallel
    // tests from bleeding into each other. Production binaries (no
    // `test-utils` feature, no `cfg(test)`) compile this branch out.
    #[cfg(any(test, feature = "test-utils"))]
    let discard_delay = TEST_PRE_DISCARD_DELAY_MS_BY_DIGEST.lock().get(digest).copied();
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(delay_ms) = discard_delay {
        if delay_ms > 0 {
            tokio::time::sleep(core::time::Duration::from_millis(delay_ms)).await;
        }
    }

    // Take the entry out of the map under the lock. If absent: caller
    // may have already discarded (idempotent). The on-disk file may
    // still exist if discard_chunked is called WITHOUT a corresponding
    // open call in this process (e.g. startup-orphan path), but
    // `discard_chunked` only knows about in-process state. The
    // recovery sweep handles disk-only orphans.
    let entry = map.inner.lock().remove(digest);

    // If we have an in-process entry, delete its on-disk file. If not,
    // there's nothing to do — return Ok (idempotent).
    let Some(entry) = entry else {
        debug!(?digest, "discard_chunked: no in-flight state, nothing to do");
        return Ok(());
    };

    let path = entry.path.clone();
    // Drop our reference to the entry BEFORE the spawn_blocking so the
    // file fd closes promptly (the spawn_blocking's `remove_file`
    // doesn't need the fd; closing it before unlink avoids holding
    // an extra open-file-description over a syscall).
    drop(entry);

    let unlink_result = tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    })
    .await
    .map_err(|join_err| {
        make_err!(Code::Internal, "spawn_blocking join error in discard_chunked unlink: {join_err:?}")
    })?;

    unlink_result.map_err(|io_err| {
        make_err!(
            Code::Internal,
            "failed to unlink discarded chunked partial: {io_err:?}"
        )
    })?;
    Ok(())
}

// Crash-recovery for chunked partials is handled by the legacy
// `prune_temp_path` (in `filesystem_store.rs`), which runs synchronously
// during `FilesystemStore::new` and unconditionally `remove_file`s every
// entry in `<temp_path>/d/` and `<temp_path>/d/XX/` shards — including
// all `.partial` files this module produces. That is one valid form of
// the spec's Q7=(c) recovery (GC-everything; mirror re-uploads what
// didn't land). The spec's length-aware rename-recovery
// (`if stat.len() == declared_size, rename → VerifyStore validates`;
// see plan `.claude/plans/212-chunk-pinned-async-slow-writes.md` §4 Q7
// + §7.4) is intentionally deferred to a later Phase 2.x because it
// requires digest-aware validation that the bare prune sweep doesn't
// have. Shipping a parallel chunked-only sweep here would be dead code
// (prune already removed every `.partial` before our sweep would run)
// AND would lock in the GC-everything degraded form rather than the
// spec's length-aware behavior.
//
// TODO(#212): implement length-aware rename-recovery per spec §4 Q7
// and §7.4 once the Phase 2.3 driver lands.

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use bytes::Bytes;
    use nativelink_macro::nativelink_test;
    use nativelink_util::common::DigestInfo;

    use super::super::CHUNK_SIZE;
    use super::{
        ChunkedPartialsMap, commit_chunked_to_holding, discard_chunked, finalize_holding,
        partial_temp_path, write_chunk_at_offset,
    };

    /// Test-only convenience: run BOTH stages of the B1 two-stage
    /// commit (rename to .holding + rename .holding → final + chmod),
    /// matching the legacy single-shot `commit_chunked` semantics. The
    /// driver does the same flow in `commit_and_verify`.
    async fn commit_chunked_test_compat(
        map: &ChunkedPartialsMap,
        digest: &nativelink_util::common::DigestInfo,
        expected_size: u64,
        final_path: std::path::PathBuf,
    ) -> Result<(), nativelink_error::Error> {
        // Reconstruct the holding path by replacing the parent dir
        // (content_path/d/XX/) — the tests use the SAME root for content
        // and temp, so the holding sibling lives next to the final path.
        let holding_path = {
            let mut p = final_path.clone();
            let fname = p.file_name().unwrap().to_string_lossy().into_owned();
            p.set_file_name(format!("{fname}.holding"));
            p
        };
        commit_chunked_to_holding(map, digest, expected_size, holding_path.clone()).await?;
        finalize_holding(map, digest, holding_path, final_path).await
    }

    /// Test-only helper: creates a fresh temp dir under `TEST_TMPDIR`
    /// (or the system temp dir) with the FilesystemStore's expected
    /// shard layout (`d/00`..`d/ff`) so the chunked partial path's
    /// parent directory exists.
    async fn make_chunked_test_root() -> String {
        let base = std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
        let nonce: u64 = rand::random();
        let root = format!("{base}/{nonce}/chunked-fs-test");
        // Pre-create the shard dirs the legacy FilesystemStore::new
        // creates. We can't call FilesystemStore::new directly here
        // because we are under #[cfg(test)] of the chunked submodule
        // and don't want to drag in the whole evicting-map machinery.
        let root_d = format!("{root}/d");
        tokio::fs::create_dir_all(&root_d).await.unwrap();
        for byte in 0u8..=255 {
            let shard = format!("{root_d}/{byte:02x}");
            tokio::fs::create_dir_all(&shard).await.unwrap();
        }
        root
    }

    fn make_test_digest(seed: u8, size: u64) -> DigestInfo {
        let mut hash = [0u8; 32];
        hash[0] = seed;
        // Set a unique tail so different seeds produce different
        // digests + different shard prefixes.
        hash[31] = seed;
        DigestInfo::new(hash, size)
    }

    /// Per Q3=(a): the on-disk path is shaped as
    /// `<temp_path>/d/<XX>/<digest>.partial`. The shard prefix is the
    /// first byte's two-hex-char representation.
    #[test]
    fn partial_temp_path_uses_sharded_layout() {
        let digest = DigestInfo::new([0xab; 32], 1234);
        let path = partial_temp_path("/tmp/store", &digest);
        let s = path.to_str().unwrap();
        assert!(s.starts_with("/tmp/store/d/ab/"), "got {s}");
        assert!(s.ends_with(".partial"), "got {s}");
    }

    /// Three chunks IN ORDER, commit, file at the right path with
    /// correct contents. Uses 4 KiB micro-chunks to keep tests fast;
    /// the production CHUNK_SIZE is 1 MiB.
    #[nativelink_test]
    async fn write_three_chunks_in_order_then_commit_succeeds() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = 3 * CHUNK as u64;
        let root = make_chunked_test_root().await;
        let map = ChunkedPartialsMap::new();
        let digest = make_test_digest(0x01, total as i64 as u64);
        let final_path =
            std::path::PathBuf::from(format!("{root}/d/01/{digest}"));

        let chunks: Vec<Bytes> = (0..3u8)
            .map(|i| Bytes::from(vec![0xa0 + i; CHUNK]))
            .collect();

        tokio::time::timeout(Duration::from_secs(5), async {
            for (i, c) in chunks.iter().enumerate() {
                write_chunk_at_offset(&map, &root, &digest, (i * CHUNK) as u64, c.clone())
                    .await
                    .expect("in-order chunk write must succeed");
            }
            commit_chunked_test_compat(&map, &digest, total, final_path.clone())
                .await
                .expect("in-order commit must succeed");
        })
        .await
        .expect("in-order chunks + commit must finish within 5s");

        // Final file exists, has correct length + contents.
        let bytes = tokio::fs::read(&final_path)
            .await
            .expect("final file must be readable");
        assert_eq!(bytes.len() as u64, total);
        for (i, byte) in bytes.iter().enumerate() {
            let chunk_idx = i / CHUNK;
            assert_eq!(
                *byte,
                0xa0u8 + chunk_idx as u8,
                "byte {i} (chunk {chunk_idx}): got {byte:#x}"
            );
        }

        // After commit, in-flight state is gone.
        assert!(!map.contains(&digest), "commit must remove in-flight state");
        assert_eq!(map.in_flight_count(), 0);
    }

    /// Three chunks OUT OF ORDER (offsets 2*N, 0, N), commit. The
    /// sparse `pwrite` semantics MUST handle out-of-order arrivals;
    /// final file contents are correct regardless of arrival order.
    #[nativelink_test]
    async fn write_three_chunks_out_of_order_then_commit_succeeds() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = 3 * CHUNK as u64;
        let root = make_chunked_test_root().await;
        let map = ChunkedPartialsMap::new();
        let digest = make_test_digest(0x02, total);
        let final_path =
            std::path::PathBuf::from(format!("{root}/d/02/{digest}"));

        // Three chunks; we send them in order 2, 0, 1 to exercise
        // sparse-file pwrite semantics. Each chunk is filled with a
        // distinctive byte so we can verify position-correctness in
        // the final file.
        let c0 = Bytes::from(vec![0xc0u8; CHUNK]);
        let c1 = Bytes::from(vec![0xc1u8; CHUNK]);
        let c2 = Bytes::from(vec![0xc2u8; CHUNK]);

        tokio::time::timeout(Duration::from_secs(5), async {
            // Offset 2 first — extends the sparse file to 12 KiB.
            write_chunk_at_offset(&map, &root, &digest, 2 * CHUNK as u64, c2.clone())
                .await
                .expect("out-of-order chunk @ offset 2 must succeed");
            // Offset 0 next — fills the head.
            write_chunk_at_offset(&map, &root, &digest, 0, c0.clone())
                .await
                .expect("out-of-order chunk @ offset 0 must succeed");
            // Offset 1 last — fills the middle.
            write_chunk_at_offset(&map, &root, &digest, CHUNK as u64, c1.clone())
                .await
                .expect("out-of-order chunk @ offset 1 must succeed");

            commit_chunked_test_compat(&map, &digest, total, final_path.clone())
                .await
                .expect("out-of-order commit must succeed");
        })
        .await
        .expect("out-of-order chunks + commit must finish within 5s");

        let bytes = tokio::fs::read(&final_path).await.unwrap();
        assert_eq!(bytes.len() as u64, total);
        // Verify each region is its expected byte. The whole point of
        // the test: no chunk overwrote another's region.
        for i in 0..CHUNK {
            assert_eq!(bytes[i], 0xc0u8, "chunk-0 region byte {i}");
            assert_eq!(bytes[CHUNK + i], 0xc1u8, "chunk-1 region byte {i}");
            assert_eq!(bytes[2 * CHUNK + i], 0xc2u8, "chunk-2 region byte {i}");
        }
    }

    /// Commit with the wrong expected_size returns
    /// `Err(InvalidArgument)` with the SPECIFIC error message AND
    /// LEAVES the temp file in place (not auto-cleaned — caller calls
    /// discard).
    #[nativelink_test]
    async fn commit_with_wrong_expected_size_returns_invalid_argument_and_keeps_temp() {
        const CHUNK: usize = 4 * 1024;
        let actual: u64 = 2 * CHUNK as u64;
        let lying_expected: u64 = 3 * CHUNK as u64;
        let root = make_chunked_test_root().await;
        let map = ChunkedPartialsMap::new();
        let digest = make_test_digest(0x03, actual);
        let final_path =
            std::path::PathBuf::from(format!("{root}/d/03/{digest}"));

        let c0 = Bytes::from(vec![0x10u8; CHUNK]);
        let c1 = Bytes::from(vec![0x11u8; CHUNK]);

        tokio::time::timeout(Duration::from_secs(5), async {
            write_chunk_at_offset(&map, &root, &digest, 0, c0).await.unwrap();
            write_chunk_at_offset(&map, &root, &digest, CHUNK as u64, c1).await.unwrap();
            let err = commit_chunked_test_compat(&map, &digest, lying_expected, final_path.clone())
                .await
                .expect_err("commit with wrong expected size must fail");
            assert_eq!(
                err.code,
                nativelink_error::Code::InvalidArgument,
                "wrong size must be classified as InvalidArgument; got {err:?}"
            );
            let msg = format!("{err:?}");
            assert!(
                msg.contains("chunked commit length mismatch"),
                "error message must name the contract; got {msg}"
            );
            assert!(
                msg.contains(&format!("temp={actual}")),
                "error must report actual length; got {msg}"
            );
            assert!(
                msg.contains(&format!("expected={lying_expected}")),
                "error must report claimed expected length; got {msg}"
            );
        })
        .await
        .expect("must not deadlock — commit should fast-fail on length mismatch");

        // Temp file MUST still exist (caller responsibility to discard).
        let temp_path = partial_temp_path(&root, &digest);
        let meta = tokio::fs::metadata(&temp_path).await;
        assert!(
            meta.is_ok(),
            "commit failure must NOT auto-discard temp file; got: {meta:?}"
        );
        assert_eq!(meta.unwrap().len(), actual);

        // Final file MUST NOT exist.
        let final_meta = tokio::fs::metadata(&final_path).await;
        assert!(
            final_meta.is_err(),
            "failed commit must NOT create final file; got: {final_meta:?}"
        );

        // In-flight state MUST still be present (so caller can
        // discard).
        assert!(
            map.contains(&digest),
            "failed commit must leave in-flight state for discard"
        );
    }

    /// Discard removes the temp file + the in-flight state entry, and
    /// commit-after-discard returns Err(NotFound).
    #[nativelink_test]
    async fn discard_removes_temp_and_state_then_commit_returns_not_found() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = 2 * CHUNK as u64;
        let root = make_chunked_test_root().await;
        let map = ChunkedPartialsMap::new();
        let digest = make_test_digest(0x04, total);
        let final_path =
            std::path::PathBuf::from(format!("{root}/d/04/{digest}"));

        tokio::time::timeout(Duration::from_secs(5), async {
            write_chunk_at_offset(&map, &root, &digest, 0, Bytes::from(vec![0x40u8; CHUNK]))
                .await
                .unwrap();
            write_chunk_at_offset(
                &map,
                &root,
                &digest,
                CHUNK as u64,
                Bytes::from(vec![0x41u8; CHUNK]),
            )
            .await
            .unwrap();
            assert!(map.contains(&digest), "writes must register in-flight state");

            discard_chunked(&map, &digest)
                .await
                .expect("discard must succeed");

            // Temp file gone.
            let temp_path = partial_temp_path(&root, &digest);
            let temp_meta = tokio::fs::metadata(&temp_path).await;
            assert!(
                temp_meta.is_err(),
                "discard must remove temp file; got: {temp_meta:?}"
            );
            // In-flight state gone.
            assert!(
                !map.contains(&digest),
                "discard must remove in-flight state"
            );
            // Idempotent: second discard is Ok.
            discard_chunked(&map, &digest)
                .await
                .expect("discard must be idempotent");

            // Commit-after-discard fails with NotFound.
            let err = commit_chunked_test_compat(&map, &digest, total, final_path)
                .await
                .expect_err("commit-after-discard must fail");
            assert_eq!(
                err.code,
                nativelink_error::Code::NotFound,
                "commit-after-discard must return NotFound; got {err:?}"
            );
        })
        .await
        .expect("must not deadlock — discard + commit-after-discard cycle");
    }

    /// Drop the ChunkedPartialsMap mid-write: the temp file MUST
    /// remain on disk (drop is graceful, not destructive). The legacy
    /// `prune_temp_path` (run synchronously during the next
    /// `FilesystemStore::new`) GCs it per Q7=(c) (degraded form;
    /// length-aware rename-recovery deferred to Phase 2.x).
    #[nativelink_test]
    async fn dropping_partials_map_leaves_temp_file_on_disk() {
        const CHUNK: usize = 4 * 1024;
        let root = make_chunked_test_root().await;
        let digest = make_test_digest(0x05, CHUNK as u64);
        let temp_path = partial_temp_path(&root, &digest);

        tokio::time::timeout(Duration::from_secs(5), async {
            let map = ChunkedPartialsMap::new();
            write_chunk_at_offset(&map, &root, &digest, 0, Bytes::from(vec![0x50u8; CHUNK]))
                .await
                .unwrap();
            assert!(map.contains(&digest), "write must register in-flight state");
            // Drop the map — entry's Arc<ChunkInProgress> drops, but
            // the on-disk file is NOT auto-removed (Drop is graceful;
            // recovery is deferred to the legacy `prune_temp_path`
            // sweep run on the next `FilesystemStore::new`).
            drop(map);
        })
        .await
        .expect("must not deadlock — write + drop cycle should be graceful");

        // Temp file MUST still exist after map is dropped.
        let meta = tokio::fs::metadata(&temp_path).await;
        assert!(
            meta.is_ok(),
            "dropping the map must NOT delete on-disk partial; got {meta:?}"
        );
        assert_eq!(meta.unwrap().len(), CHUNK as u64);
    }

    /// Sanity: production CHUNK_SIZE matches the spec (1 MiB).
    /// Tests above use 4 KiB micro-chunks to stay fast, but the
    /// production system uses CHUNK_SIZE — guard against accidental
    /// drift.
    #[test]
    fn chunk_size_constant_pinned_for_chunked_filesystem_callers() {
        assert_eq!(CHUNK_SIZE, 1024 * 1024);
    }

    // -------------------------------------------------------------------
    // Adapter tests: exercise `FilesystemStore::write_chunk_at_offset` /
    // `commit_chunked` / `discard_chunked` (the actual Phase 2.1 ship
    // surface) through the real `FilesystemStore::new` path. The pure-
    // module-fn tests above never see the path-resolution that the
    // adapter performs (`temp_path` for the partial; `content_path` via
    // `to_full_path_from_key` for the final CAS file). A bug in the
    // adapter (e.g. `temp_path` ↔ `content_path` swap, wrong shard layout)
    // ships silently if these tests are missing.
    //
    // testing-czar M1 (#212 Phase 2.1 review).
    // -------------------------------------------------------------------

    use nativelink_config::stores::FilesystemSpec;

    use crate::filesystem_store::{DIGEST_FOLDER, FileEntryImpl, FilesystemStore};

    fn make_adapter_temp_path(label: &str) -> String {
        let base = std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
        let nonce: u64 = rand::random();
        format!("{base}/{nonce}/chunked-fs-adapter/{label}")
    }

    /// Adapter: end-to-end `write_chunk_at_offset` + `commit_chunked`
    /// through a real `FilesystemStore`. Verifies:
    /// (a) the partial lands at the store's TEMP path (not content_path),
    /// (b) after commit, the final file lands at the store's CONTENT
    ///     path (not temp_path),
    /// (c) the in-flight state is cleared from the store's
    ///     `chunked_partials` map.
    /// Mutation: swap `temp_path` ↔ `content_path` in the adapter and
    /// this test red-fails because the partial isn't reachable from the
    /// asserted CONTENT path before commit OR the final file lands at
    /// the wrong location.
    #[nativelink_test]
    async fn adapter_write_then_commit_lands_at_content_path() {
        const CHUNK: usize = 4 * 1024;
        let total: u64 = 2 * CHUNK as u64;
        let content_path = make_adapter_temp_path("content");
        let temp_path = make_adapter_temp_path("temp");
        let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: None,
            block_size: 1,
            ..Default::default()
        })
        .await
        .expect("FilesystemStore::new must succeed");

        // Use a digest whose first byte gives a known shard (0x06).
        let mut hash = [0u8; 32];
        hash[0] = 0x06;
        hash[31] = 0x06;
        let digest = DigestInfo::new(hash, total);

        let c0 = Bytes::from(vec![0x60u8; CHUNK]);
        let c1 = Bytes::from(vec![0x61u8; CHUNK]);

        tokio::time::timeout(Duration::from_secs(5), async {
            store
                .write_chunk_at_offset(&digest, 0, c0.clone())
                .await
                .expect("adapter write_chunk_at_offset @ 0 must succeed");
            store
                .write_chunk_at_offset(&digest, CHUNK as u64, c1.clone())
                .await
                .expect("adapter write_chunk_at_offset @ CHUNK must succeed");

            // Before commit: partial MUST be under TEMP path, NOT
            // content_path. This is the temp_path-vs-content_path swap
            // detector.
            let expected_partial =
                format!("{temp_path}/{DIGEST_FOLDER}/06/{digest}.partial");
            let temp_meta = tokio::fs::metadata(&expected_partial).await;
            assert!(
                temp_meta.is_ok(),
                "partial must exist under temp_path before commit; expected {expected_partial}, got {temp_meta:?}"
            );
            assert_eq!(temp_meta.unwrap().len(), total);

            // The final CAS path under content_path MUST NOT exist yet.
            let final_path = format!("{content_path}/{DIGEST_FOLDER}/06/{digest}");
            let final_meta_pre = tokio::fs::metadata(&final_path).await;
            assert!(
                final_meta_pre.is_err(),
                "final CAS file must NOT exist before commit; got {final_meta_pre:?}"
            );

            // Stage 1 (rename to .holding):
            store
                .commit_chunked(&digest, total)
                .await
                .expect("adapter commit_chunked (stage 1: rename to .holding) must succeed");

            // After stage 1: the .holding file lives under content_path
            // (not the canonical CAS name); the canonical CAS file
            // does NOT yet exist (B1 fixup contract).
            let holding_path =
                format!("{content_path}/{DIGEST_FOLDER}/06/{digest}.holding");
            let holding_meta_mid = tokio::fs::metadata(&holding_path).await;
            assert!(
                holding_meta_mid.is_ok(),
                "after stage-1 commit, .holding file must exist under content_path; got {holding_meta_mid:?}"
            );
            assert_eq!(holding_meta_mid.unwrap().len(), total);
            let final_meta_mid = tokio::fs::metadata(&final_path).await;
            assert!(
                final_meta_mid.is_err(),
                "after stage-1 commit, canonical CAS file must NOT yet exist (B1 contract); got {final_meta_mid:?}"
            );

            // Stage 2 (rename .holding → final + chmod 0o555):
            store
                .finalize_holding(&digest)
                .await
                .expect("adapter finalize_holding (stage 2 rename) must succeed");

            // After stage 2: final file is under CONTENT path with
            // canonical name; .holding sibling is gone.
            let final_meta = tokio::fs::metadata(&final_path).await;
            assert!(
                final_meta.is_ok(),
                "after stage-2 commit, final CAS file must exist under content_path; expected {final_path}, got {final_meta:?}"
            );
            assert_eq!(final_meta.unwrap().len(), total);
            let holding_meta_post = tokio::fs::metadata(&holding_path).await;
            assert!(
                holding_meta_post.is_err(),
                "after stage-2 commit, .holding file must be gone; got {holding_meta_post:?}"
            );
            // And the partial under temp_path is gone.
            let temp_meta_post = tokio::fs::metadata(&expected_partial).await;
            assert!(
                temp_meta_post.is_err(),
                "after commit, partial must be removed from temp_path; got {temp_meta_post:?}"
            );
        })
        .await
        .expect(
            "must not deadlock — adapter write+commit should finish promptly through real \
             FilesystemStore",
        );
    }

    /// Adapter: `discard_chunked` removes the temp file from the
    /// store's TEMP path and never touches the CONTENT path.
    /// Mutation: a swap of `temp_path` ↔ `content_path` in the adapter
    /// red-fails because the discard targets the wrong location.
    #[nativelink_test]
    async fn adapter_discard_removes_partial_from_temp_path() {
        const CHUNK: usize = 4 * 1024;
        let content_path = make_adapter_temp_path("content");
        let temp_path = make_adapter_temp_path("temp");
        let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            eviction_policy: None,
            block_size: 1,
            ..Default::default()
        })
        .await
        .expect("FilesystemStore::new must succeed");

        let mut hash = [0u8; 32];
        hash[0] = 0x07;
        hash[31] = 0x07;
        let digest = DigestInfo::new(hash, CHUNK as u64);

        tokio::time::timeout(Duration::from_secs(5), async {
            store
                .write_chunk_at_offset(&digest, 0, Bytes::from(vec![0x70u8; CHUNK]))
                .await
                .expect("adapter write must succeed");

            let expected_partial =
                format!("{temp_path}/{DIGEST_FOLDER}/07/{digest}.partial");
            let pre = tokio::fs::metadata(&expected_partial).await;
            assert!(
                pre.is_ok(),
                "partial must exist under temp_path; got {pre:?}"
            );

            store
                .discard_chunked(&digest)
                .await
                .expect("adapter discard_chunked must succeed");

            // Temp partial gone.
            let post = tokio::fs::metadata(&expected_partial).await;
            assert!(
                post.is_err(),
                "discard must remove partial from temp_path; got {post:?}"
            );

            // Content path was never touched.
            let final_path = format!("{content_path}/{DIGEST_FOLDER}/07/{digest}");
            let final_meta = tokio::fs::metadata(&final_path).await;
            assert!(
                final_meta.is_err(),
                "discard must NOT create anything under content_path; got {final_meta:?}"
            );
        })
        .await
        .expect(
            "must not deadlock — adapter discard should finish promptly through real \
             FilesystemStore",
        );
    }
}
