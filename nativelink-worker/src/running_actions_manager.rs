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

use core::cmp::min;
use core::convert::Into;
use core::fmt::Debug;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
#[cfg(target_family = "unix")]
use std::fs::Permissions;
#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};
use filetime::{FileTime, set_file_mtime};
use formatx::Template;
use futures::future::{
    BoxFuture, Future, FutureExt, TryFutureExt, try_join, try_join_all, try_join3,
};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use nativelink_config::cas_server::{
    EnvironmentSource, UploadActionResultConfig, UploadCacheResultsStrategy,
};
use nativelink_config::stores::StoreDirection;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult as ProtoActionResult, BatchReadBlobsRequest, Command as ProtoCommand,
    Directory as ProtoDirectory, Directory, DirectoryNode, ExecuteResponse, FileNode,
    GetTreeRequest, SymlinkNode, Tree as ProtoTree, UpdateActionResultRequest,
    batch_read_blobs_response,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    HistoricalExecuteResponse, StartExecute,
};
use nativelink_store::ac_utils::{
    ESTIMATED_DIGEST_SIZE, compute_buf_digest, get_and_decode_digest, serialize_and_upload_message,
};
use nativelink_store::cas_utils::is_zero_digest;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntry, FilesystemStore};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, DirectoryInfo, ExecutionMetadata, FileInfo, NameOrPath, OperationId,
    SymlinkInfo, to_execute_response,
};
use nativelink_util::common::{DigestInfo, fs, make_precondition_failure_any};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc, default_digest_hasher_func};
use nativelink_util::metrics_utils::{AsyncCounterWrapper, CounterWithTime};
use nativelink_util::o11_probes::symlink_fix_counters;
use nativelink_util::phase0_metrics::worker_phase0_metrics;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::store_trait::{
    IS_WORKER_REQUEST, Store, StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
};
use nativelink_util::log_utils::throughput_mbps;
use nativelink_util::{background_spawn, spawn, spawn_blocking};
use parking_lot::Mutex;
use prost::Message;
use scopeguard::{ScopeGuard, guard};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_stream::wrappers::ReadDirStream;
use opentelemetry::context::Context;
use tonic::Request;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

// =============================================================================
// Scheduler-rebalance calibration probes (P-A action-shape, P-B input-staging).
//
// Observability-only instrumentation per
// `.claude/audits/scheduler-calibration-instrumentation-spec-v2-2026-06-30.md`
// §3/§4/§6/§9. Two sampled structured-log records, both `info!`-level (so they
// survive `release_max_level_info` in the prod worker binary), both OFF the
// per-RPC hot path:
//
//   P-A (`tag="calib_action"`): per-action execution shape, emitted AFTER
//     `inner_upload_results` so `output_bytes` is known and the probe cannot
//     inflate the `exec_duration` it reports.
//   P-B (`tag="calib_staging"`): per-action input-staging cost, emitted at the
//     `download_to_directory` tail (the miss path) where the resolved tree and
//     byte totals are in scope.
//
// All decision logic (sampling, classification, record building) is factored
// into the pure functions below so it is unit-testable without driving a real
// action; the `info!` emit is a thin wrapper over the populated record struct.
// =============================================================================

/// Uniform digest-hash sampling period for the calibration probes (1/16).
///
/// Chosen smaller than `chunked_inflight_log_sampled`'s 1/64 because the
/// per-action record volume is far lower than per-chunk (one record per
/// action, not per 64 KiB chunk), so 1/16 keeps a usable sample density for
/// the offline regression fits in §6 without flooding the log.
const CALIB_SAMPLE_PERIOD: u64 = 16;

/// P-A 1/1 override: any action whose `exec_duration_ms` exceeds this is
/// recorded unconditionally (§3, §9 B4). LTO links are rare; uniform 1/16
/// sampling would lose the regression-risk tail. 60 s in milliseconds.
const CALIB_LARGE_EXEC_MS_THRESHOLD: i64 = 60_000;

/// P-B 1/1 override: any input tree larger than this is recorded
/// unconditionally (§4, §9 B4 — the override MUST carry a size threshold or it
/// never fires). 100 MiB; sizes the miss-cost curve's large-tree tail.
const CALIB_LARGE_TREE_BYTES_THRESHOLD: u64 = 100 * 1024 * 1024;

/// P-A/P-B 1/1 override on the FETCHED-PAYLOAD byte axis (GAP-2 fix). Any action
/// whose fetched payload exceeds this is recorded unconditionally, regardless of
/// digest-hash, exec duration, or proto-tree size. Closes the M1 soak's GAP-2:
/// `CALIB_LARGE_TREE_BYTES_THRESHOLD` keys on proto-structure size and
/// `CALIB_LARGE_EXEC_MS_THRESHOLD` on wall time, so a large-payload /
/// small-proto-tree / <60 s action (the LTO-link shape: one huge archive input,
/// a shallow proto tree, a fast link) can slip both floors and be dropped by the
/// unlucky uniform 1/16 — leaving a hole in R1 (re-fetch-storm) detection. 500
/// MiB matches the M1 soak protocol's large-input threshold: LTO archives sit
/// above it, and it is the byte scale at which a re-fetch is expensive enough
/// that missing it corrupts the re-fetch-risk fit.
const CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD: u64 = 500 * 1024 * 1024;

/// Extract a stable u64 sampling key from a digest's packed hash (first 8
/// bytes, little-endian). Blake3/SHA-256 outputs are uniformly distributed, so
/// `key % period == 0` gives an alloc-free ~1/period rate without a global
/// counter, identical to the `chunked_inflight_log_sampled` pattern.
fn calib_digest_sample_key(digest: &DigestInfo) -> u64 {
    let bytes: &[u8; 32] = digest.packed_hash();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// Uniform 1/`CALIB_SAMPLE_PERIOD` membership by digest-hash key, WITHOUT the
/// large-exec override. This is the poll-start predicate: the CPU-time poll (see
/// the execute loop) must be armed BEFORE the child runs, when `exec_duration_ms`
/// is not yet known, so it can only key on the uniform sample — the >60 s
/// override cannot participate. Factored out so `calib_action_sampled` composes
/// it (behavior unchanged) and the poll-start decision is unit-testable alone.
const fn calib_uniformly_sampled(sample_key: u64) -> bool {
    sample_key % CALIB_SAMPLE_PERIOD == 0
}

/// P-A sampling decision: uniform 1/`CALIB_SAMPLE_PERIOD` by digest-hash key,
/// EXCEPT actions are always sampled (1/1) when EITHER
/// `exec_duration_ms > CALIB_LARGE_EXEC_MS_THRESHOLD` (large-action override, §3)
/// OR `input_bytes > CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD` (large-payload
/// override, GAP-2). The payload floor is checkable at THIS gate because
/// `state.calib_input_bytes` is resolved during staging and read at the
/// post-upload P-A site, so the fetched-payload total is in hand before the
/// sampling decision — no emit-time fallback needed. The two overrides are
/// independent axes: exec catches slow actions, payload catches the
/// large-payload/small-tree/<60 s LTO-link shape the exec floor misses.
///
/// Pure function of (`sample_key`, `exec_duration_ms`, `input_bytes`) so both
/// boundaries (exec == 60 s not over; payload == 500 MiB not over) are
/// unit-testable.
const fn calib_action_sampled(sample_key: u64, exec_duration_ms: i64, input_bytes: u64) -> bool {
    exec_duration_ms > CALIB_LARGE_EXEC_MS_THRESHOLD
        || input_bytes > CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD
        || calib_uniformly_sampled(sample_key)
}

/// P-B sampling decision: uniform 1/`CALIB_SAMPLE_PERIOD` by digest-hash key,
/// EXCEPT staging records are always sampled (1/1) when EITHER
/// `input_tree_bytes > CALIB_LARGE_TREE_BYTES_THRESHOLD` (large-tree override,
/// §4/§9 B4) OR `input_missing_bytes > CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD`
/// (large-fetch override, GAP-2). The GAP-2 axis is `input_missing_bytes` (the
/// NETWORK-FETCH byte count = digests not already cached), not the full payload:
/// a re-fetch storm (R1) is precisely large missing-bytes, and it is the volume
/// that drives `input_staging_ms`. `input_tree_bytes` keys on proto-structure
/// size, so an LTO archive (huge fetch, shallow proto tree) slips the tree floor
/// — the missing-bytes floor catches it. `missing_bytes` is in scope at this
/// gate (the batch existence check ran upstream), so no emit-time fallback.
const fn calib_staging_sampled(
    sample_key: u64,
    input_tree_bytes: u64,
    input_missing_bytes: u64,
) -> bool {
    input_tree_bytes > CALIB_LARGE_TREE_BYTES_THRESHOLD
        || input_missing_bytes > CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD
        || sample_key % CALIB_SAMPLE_PERIOD == 0
}

/// Poll-arm decision: arm the CPU-time poll iff the child has a queryable OS PID
/// AND the action is in the uniform 1/16 sample. Pure `(pid, sample_key) → bool`
/// so BOTH branches (including the FALSE branch that bounds the per-action
/// background-task cost) are unit-testable without driving a real action. The
/// >60 s exec override cannot participate here — the duration is unknown when
/// the poll is armed, upfront of the child running.
const fn calib_should_arm_poll(pid: Option<u32>, sample_key: u64) -> bool {
    pid.is_some() && calib_uniformly_sampled(sample_key)
}

/// Completion-arm harvest gate: read the poll's last live sample IFF the poll
/// actually captured one. Returns `None` when `has_value` is unset (the poll
/// never captured: child died before the first tick / non-macOS / spawn
/// failure) so the caller records `None`, NOT `Some(0)` — a never-sampled
/// action must never masquerade as a real zero-CPU action (which would classify
/// `io_bound` and poison the §6 fit). A genuine live-but-idle capture stores
/// `Some(0)` with `has_value=true`, which this correctly returns as `Some(0)`.
/// `Acquire` on the flag pairs with the poll loop's `Release` store so a reader
/// observing `has_value=true` is guaranteed to observe the `last` that preceded
/// it (the textbook flag-and-data pairing; `last` may stay `Relaxed`).
fn calib_harvest_cpu_time(
    has_value: &AtomicBool,
    last: &core::sync::atomic::AtomicU64,
) -> Option<u64> {
    if has_value.load(Ordering::Acquire) {
        Some(last.load(Ordering::Relaxed))
    } else {
        None
    }
}

/// CPU-vs-wall shape classification of an action (§3, auditor-required bands).
///
/// `IoBound` (`ratio < 0.5`): mostly waiting on I/O, leaves cores idle.
/// `Ambiguous` (`[0.5, 1.5]`): single-threaded CPU-bound is indistinguishable
///   from I/O-bound by ratio alone — reported separately, never folded into
///   either tail.
/// `MultiCoreCpuBound` (`ratio > 1.5`): used more CPU-seconds than wall-seconds,
///   so genuinely parallel across cores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CalibActionShape {
    IoBound,
    Ambiguous,
    MultiCoreCpuBound,
}

impl CalibActionShape {
    /// Stable lowercase label emitted into the log (offline analysis keys on
    /// this string).
    const fn as_str(self) -> &'static str {
        match self {
            Self::IoBound => "io_bound",
            Self::Ambiguous => "ambiguous",
            Self::MultiCoreCpuBound => "multi_core_cpu_bound",
        }
    }
}

/// Classify by `cpu_wall_ratio` into the three §3 bands. Pure function so the
/// band boundaries (0.49 / 0.5 / 1.5 / 1.51) are unit-testable. `0.5` and `1.5`
/// fall in the ambiguous band (closed interval `[0.5, 1.5]`).
fn calib_classify(cpu_wall_ratio: f64) -> CalibActionShape {
    if cpu_wall_ratio < 0.5 {
        CalibActionShape::IoBound
    } else if cpu_wall_ratio <= 1.5 {
        CalibActionShape::Ambiguous
    } else {
        CalibActionShape::MultiCoreCpuBound
    }
}

/// Populated P-A record. Built from already-computed values at the post-upload
/// site; `emit()` is the thin `info!` wrapper. All byte/time fields are scalar
/// `u64`/`i64`/`f64` — no owned-bytes buffers, so no cap annotation applies.
#[derive(Debug, Clone, PartialEq)]
struct CalibActionRecord {
    /// Reused `execution_ms` (from `execution_start/completed_timestamp`).
    exec_duration_ms: i64,
    /// Child CPU time (user+system) in ms, or `None` when the per-OS query was
    /// unavailable (see `calib_capture_cpu_time_ms`). Kept distinct from `0`
    /// so offline analysis can drop unavailable samples rather than mistake
    /// them for a zero-CPU action.
    cpu_time_ms: Option<u64>,
    /// `cpu_time_ms / exec_duration_ms`; `None` when CPU time was unavailable
    /// or `exec_duration_ms <= 0` (clock skew / instantaneous action).
    cpu_wall_ratio: Option<f64>,
    /// Total resolved input-tree bytes for this action (carried from staging).
    input_bytes: u64,
    /// Total output-blob bytes (output files + output folder tree digests).
    output_bytes: u64,
    /// Worker-side count of in-flight actions at execute start (includes self,
    /// so always >= 1). NOT the scheduler dispatch-count.
    worker_running_actions_at_start: usize,
}

impl CalibActionRecord {
    /// Compute the cpu_wall_ratio and shape from this record's fields. Returns
    /// `None` shape when the ratio is unavailable.
    fn shape(&self) -> Option<CalibActionShape> {
        self.cpu_wall_ratio.map(calib_classify)
    }

    /// Thin `info!` wrapper. `tag="calib_action"`; `sample_period` is recorded
    /// so offline analysis can scale the 1/16-sampled counts back up.
    fn emit(&self, operation_id: &OperationId) {
        let shape = self.shape().map(CalibActionShape::as_str);
        info!(
            tag = "calib_action",
            operation_id = ?operation_id,
            sample_period = CALIB_SAMPLE_PERIOD,
            exec_duration_ms = self.exec_duration_ms,
            cpu_time_ms = ?self.cpu_time_ms,
            cpu_wall_ratio = ?self.cpu_wall_ratio,
            shape = ?shape,
            input_bytes = self.input_bytes,
            output_bytes = self.output_bytes,
            // worker-side in-flight count, includes self (>=1); NOT the
            // scheduler dispatch-count.
            worker_running_actions_at_start = self.worker_running_actions_at_start,
            "calib: action-shape execution record"
        );
    }
}

/// Build a P-A record from raw inputs, computing `cpu_wall_ratio` once.
/// `exec_duration_ms <= 0` (clock skew) or absent CPU time yields `None` ratio.
fn calib_build_action_record(
    exec_duration_ms: i64,
    cpu_time_ms: Option<u64>,
    input_bytes: u64,
    output_bytes: u64,
    worker_running_actions_at_start: usize,
) -> CalibActionRecord {
    let cpu_wall_ratio = match cpu_time_ms {
        Some(cpu) if exec_duration_ms > 0 => Some(cpu as f64 / exec_duration_ms as f64),
        _ => None,
    };
    CalibActionRecord {
        exec_duration_ms,
        cpu_time_ms,
        cpu_wall_ratio,
        input_bytes,
        output_bytes,
        worker_running_actions_at_start,
    }
}

/// Populated P-B record. All fields scalar; no owned-bytes buffer.
#[derive(Debug, Clone, PartialEq)]
struct CalibStagingRecord {
    /// `phase_start.elapsed()` over the whole `download_to_directory` body.
    input_staging_ms: u64,
    /// Always `false` at the `download_to_directory` emit site: this function
    /// IS the directory-cache miss/fallback path (a directory-cache hardlink
    /// hit returns before reaching here). Recorded for schema stability and to
    /// document the miss-path-only nature of the curve.
    dir_cache_hit: bool,
    /// PRIMARY regressor for the §6 locality fit: total input-FILE PAYLOAD bytes
    /// (sum of unique input-file digest sizes = `total_bytes` in staging scope).
    /// This is the byte volume the network fetch + hardlink actually move, so
    /// `staging_ms ≈ a + b·input_payload_bytes` yields the ms-per-payload-byte
    /// `load_byte_cost` the rebalance's locality-vs-load crossover needs
    /// (auditor Claim 3 / §6). `input_tree_bytes` below is proto-structure size,
    /// a cheap SECONDARY covariate, NOT the fetch-cost axis.
    input_payload_bytes: u64,
    /// NETWORK-FETCH byte axis: sum of the digest sizes NOT already cached
    /// locally (= `missing_bytes` in staging scope). `input_payload_bytes` above
    /// is the FULL payload (cached + missing), which conflates network-fetch
    /// volume with local-hardlink volume; `missing_bytes` is the bytes the
    /// network transfer actually moves. Carrying both lets the §6
    /// `load_byte_cost` fit separate the (dominant) fetch cost from the hardlink
    /// cost — with a fully cache-cold miss `input_missing_bytes ≈
    /// input_payload_bytes`, but under server missing-digest hints / partial
    /// cache it is smaller (auditor Claim 3 residual).
    input_missing_bytes: u64,
    /// Sum of `size_bytes()` over the resolved tree's directory digests (proto
    /// structure size, scales with directory COUNT not payload VOLUME). Kept as
    /// a secondary regressor; NOT the per-byte fetch-cost axis (see above).
    input_tree_bytes: u64,
    /// Sum of file counts over the resolved tree's directories.
    input_tree_files: u64,
}

impl CalibStagingRecord {
    /// Thin `info!` wrapper. `tag="calib_staging"`.
    fn emit(&self, digest: &DigestInfo) {
        info!(
            tag = "calib_staging",
            root = ?digest,
            sample_period = CALIB_SAMPLE_PERIOD,
            input_staging_ms = self.input_staging_ms,
            dir_cache_hit = self.dir_cache_hit,
            // primary regressor: file-payload bytes moved by the fetch/hardlink.
            input_payload_bytes = self.input_payload_bytes,
            // network-fetch axis: bytes NOT already cached (the transfer volume).
            input_missing_bytes = self.input_missing_bytes,
            // secondary covariate: proto-structure size, NOT the fetch-cost axis.
            input_tree_bytes = self.input_tree_bytes,
            input_tree_files = self.input_tree_files,
            "calib: input-staging locality record"
        );
    }
}

/// Compute the P-B byte/file totals from a resolved input tree (the
/// `tree.keys()/tree.values()` map produced by `download_to_directory`). Pure
/// over a `HashMap<DigestInfo, ProtoDirectory>` so the sums are unit-testable.
/// `input_tree_bytes` = sum of directory-digest sizes (the `:552-553` pattern,
/// §9 B3); `input_tree_files` = sum of per-directory file counts.
fn calib_tree_totals(tree: &HashMap<DigestInfo, ProtoDirectory>) -> (u64, u64) {
    let bytes: u64 = tree.keys().map(DigestInfo::size_bytes).sum();
    let files: u64 = tree.values().map(|d| d.files.len() as u64).sum();
    (bytes, files)
}

/// Defensive caps on the descendant-tree walk (`calib_capture_subtree_cpu_ns`).
/// A calibration probe must never fan out unboundedly, even if a pathological
/// action forks a fork-bomb-shaped tree.
// CAPPED AT 8: max descendant recursion depth. Real toolchain trees are shallow
// (process-wrapper → compiler → linker ≈ 3 levels); 8 is generous headroom and
// bounds the recursion regardless of a runaway action. `test`-gated in addition
// to macOS so `constants_match_spec` can pin it on the Linux build box.
#[cfg(any(target_os = "macos", test))]
const CALIB_SUBTREE_MAX_DEPTH: u32 = 8;
// CAPPED AT 512: max total pids summed per poll tick, bounding both the
// `proc_listchildpids` buffer and the per-tick syscall count. A compile/link
// action's descendant set is tens of pids; 512 caps a runaway fan-out without
// truncating any realistic tree.
#[cfg(any(target_os = "macos", test))]
const CALIB_SUBTREE_MAX_PIDS: usize = 512;

/// Pure tick→ns conversion, factored out so it is cross-platform
/// unit-testable (the timebase source is injected). `pti_total_*` are raw
/// **mach-timebase ticks**; ns = `ticks * numer / denom`.
///
/// **Drop-don't-fabricate:** returns `None` when the timebase is absent
/// (`mach_timebase_info` failed) OR its `denom` is 0. A `None` here propagates
/// to `cpu_time_ms = None` (dropped offline), NOT a fabricated 1:1 scale — a
/// 1:1 fallback would silently under-report CPU ~40× on the M-series fleet
/// (numer/denom ≈ 125/3) and misclassify every action `io_bound` with no marker
/// to drop the poisoned samples (auditor + red-team: the probe's whole purpose
/// is a valid ratio, so no code path may emit a knowingly-wrong one). The
/// `u128` intermediate keeps `ticks * numer` from wrapping u64 on a multi-hour
/// action; the final narrow saturates rather than panics.
#[cfg(any(target_os = "macos", test))]
fn calib_ticks_to_ns(ticks: u64, timebase: Option<(u32, u32)>) -> Option<u64> {
    let (numer, denom) = timebase?;
    if denom == 0 {
        return None;
    }
    Some(
        u64::try_from(u128::from(ticks) * u128::from(numer) / u128::from(denom))
            .unwrap_or(u64::MAX),
    )
}

/// Read the macOS `mach_timebase_info` ratio ONCE (it is constant per boot) and
/// cache it. `proc_pidinfo`'s `pti_total_*` fields are raw **mach-timebase
/// ticks**, NOT nanoseconds (see `calib_capture_cpu_ns`); converting a tick
/// count to ns requires `ticks * numer / denom`. Cached in a `OnceLock` so the
/// syscall runs at most once for the whole process. Returns `None` (NOT a 1:1
/// fabrication) if the syscall fails or reports `denom == 0`, and `warn!`s once
/// — so a silent 40× regression can never recur unobserved (auditor + red-team).
#[cfg(target_os = "macos")]
fn calib_mach_timebase() -> Option<(u32, u32)> {
    use std::sync::OnceLock;
    static TIMEBASE: OnceLock<Option<(u32, u32)>> = OnceLock::new();
    *TIMEBASE.get_or_init(|| {
        // `mach_timebase_info` — BOTH the struct AND the fn — are `#[deprecated]`
        // in `libc` 0.2.182 ("use the mach2 crate"), so the struct-literal below
        // AND the fn call both need `#[allow(deprecated)]`. Adding a whole crate
        // for one boot constant is not worth it; the ABI is stable. Read
        // numer/denom at RUNTIME — do NOT hardcode 125/3, future silicon differs.
        #[allow(deprecated)]
        let mut info = libc::mach_timebase_info { numer: 0, denom: 0 };
        // SAFETY: `mach_timebase_info` fills the `mach_timebase_info` struct we
        // own by pointer; it is a pure read of a boot-constant, cannot fail in
        // practice, and does not retain the pointer.
        #[allow(deprecated)]
        let rc = unsafe { libc::mach_timebase_info(core::ptr::from_mut(&mut info)) };
        if rc != 0 || info.denom == 0 {
            // Refuse, don't fabricate. This is effectively unreachable on real
            // hardware (`mach_timebase_info` is a boot constant that does not
            // fail), so the `warn!` fires at most once per process via the
            // OnceLock and converts a would-be silent 40×-wrong scale into a
            // visible signal; the caller records `cpu_time_ms = None`.
            warn!(
                rc,
                denom = info.denom,
                "calib: mach_timebase_info unavailable — cpu_time_ms will be None \
                 (refusing a fabricated 1:1 ns scale)"
            );
            None
        } else {
            Some((info.numer, info.denom))
        }
    })
}

/// Best-effort single-PID CPU time (user+system) in **nanoseconds**, or `None`
/// when unavailable for this PID (not running / reaped / permission). This is
/// the pure single-pid primitive; `calib_capture_cpu_time_ms` wraps it for the
/// per-action helper and `calib_capture_subtree_cpu_ns` sums it over the tree.
///
/// **macOS** (the production worker OS — see memory `reference-infrastructure`):
/// `proc_pidinfo(pid, PROC_PIDTASKINFO, …)` is a READ-ONLY query (it does NOT
/// reap), so it is safe to call alongside tokio's SIGCHLD reaper. It MUST be
/// called while the child is still alive: once the child exits and tokio's
/// reaper collects the zombie, `proc_pidinfo` returns 0/ESRCH → this yields
/// `None` (which is why an after-`wait()` capture was structurally always
/// `None`, and why the poll self-terminates on that `None`).
///
/// **UNITS (empirically verified — the struct field's `u64` type carries NO
/// unit and Apple's docs mislabel it):** `pti_total_user`/`pti_total_system`
/// are raw **mach-timebase ticks**, not nanoseconds, on Apple Silicon since the
/// XNU "Recount" rewrite (macOS 15 Sequoia, the M4 minimum). A controlled 3.002 s
/// single-threaded CPU spin on worker-01 (M4) reported `pti_total_user +
/// pti_total_system = 74_134_725`: as ns that is 0.074 s (40× low); as ticks ×
/// `mach_timebase_info` (numer/denom = 125/3 ≈ 41.6667 ns/tick) = 3.089 s ✓. So
/// we convert `ticks * numer / denom` via the cached `calib_mach_timebase()`.
/// (On x86 Macs and under Rosetta the timebase is 1:1, so a raw read would be
/// accidentally correct there — masking the bug off the production fleet. See
/// memory `proc-pidinfo-cpu-time-is-mach-timebase-not-ns`.)
///
/// **Linux**: deliberately yields `None`. The thread-safe per-child accounting
/// primitive is `wait4(pid, …, &rusage)`, but tokio's process reaper already
/// owns `waitpid` on this PID — calling `wait4` ourselves would DOUBLE-REAP
/// (ESRCH at best, reaping a recycled unrelated PID at worst), a correctness
/// hazard and a behavior change. There is no zero-behavior-change Linux path
/// for a tokio-managed child, and production workers are macOS, so Linux is
/// left unavailable rather than made unsafe.
#[cfg(target_os = "macos")]
fn calib_capture_cpu_ns(pid: u32) -> Option<u64> {
    // SAFETY: `proc_pidinfo` with `PROC_PIDTASKINFO` fills a `proc_taskinfo` we
    // own; we pass its exact size and only read the returned bytes when the
    // syscall reports it wrote the full struct. The call is read-only (no
    // reaping). `pid` is the child's OS PID captured before reap. Provenance
    // caveat: the `written == size` check does NOT reject a recycled-live PID —
    // if this PID were reaped AND recycled to a live unrelated process within
    // one poll interval, `proc_pidinfo` returns the FULL struct and we would
    // read THAT process's CPU. This is a bounded, vanishingly-rare data-quality
    // risk (requires reap + PID-space wraparound within one ≤250 ms interval),
    // accepted for a sampled best-effort probe — never UB.
    let mut info: libc::proc_taskinfo = unsafe { core::mem::zeroed() };
    let size = core::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let written = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            core::ptr::from_mut(&mut info).cast::<libc::c_void>(),
            size,
        )
    };
    if written != size {
        // ESRCH (reaped/exited), permission denied, or partial write.
        return None;
    }
    // Raw mach-timebase ticks (see UNITS above) → ns via the cached ratio. A
    // `None` timebase propagates to `None` here (drop-don't-fabricate — the
    // `?`), so a would-be 40×-wrong sample is dropped rather than emitted.
    let ticks = info
        .pti_total_user
        .saturating_add(info.pti_total_system);
    calib_ticks_to_ns(ticks, calib_mach_timebase())
}

// Non-macOS single-pid stub: reachable only via `calib_capture_cpu_time_ms`,
// which is itself test-only on every platform (production reads the SUBTREE via
// `calib_capture_subtree_cpu_ns`, never this single-pid helper). Gate it to the
// non-macOS test build so it neither warns dead in the Linux lib build nor
// pretends to be a production path.
#[cfg(all(not(target_os = "macos"), test))]
const fn calib_capture_cpu_ns(_pid: u32) -> Option<u64> {
    // See doc-comment above: no zero-behavior-change path on non-macOS without
    // double-reaping the tokio-managed child.
    None
}

/// Best-effort single-PID CPU time in **milliseconds** (the ns primitive
/// truncated). Pure single-pid helper, kept unit-testable; `None` when the ns
/// capture is unavailable. Production reads the whole subtree
/// (`calib_capture_subtree_cpu_ns`), so this single-pid wrapper exists ONLY for
/// the unit/live tests — `#[cfg(test)]` keeps it out of the shipped binary and
/// silences the dead-code lint on the non-macOS build box.
#[cfg(test)]
fn calib_capture_cpu_time_ms(pid: u32) -> Option<u64> {
    calib_capture_cpu_ns(pid).map(|ns| ns / 1_000_000)
}

/// Fold one pid's observed `cpu_ns` into a task-local `pid → max-cpu-ns` map.
/// Pure (cross-platform, unit-testable): keeps the MAX per pid (a transient
/// accounting glitch cannot lower a value; CPU is monotonic per LIVE process),
/// and — because the map only ever grows and never removes — a descendant that
/// exits between poll ticks KEEPS its last observed CPU, so sequential children
/// (A runs then exits, B runs) are BOTH counted. Summing `map.values()` is then
/// a monotonic non-decreasing subtree total across ticks. Gated to macOS (the
/// production subtree walk uses it) OR any test build (the Linux unit test
/// exercises it); it has no non-macOS production caller.
#[cfg(any(target_os = "macos", test))]
fn calib_accumulate_subtree_cpu(map: &mut HashMap<libc::pid_t, u64>, pid: libc::pid_t, cpu_ns: u64) {
    map.entry(pid)
        .and_modify(|prev| *prev = (*prev).max(cpu_ns))
        .or_insert(cpu_ns);
}

/// Best-effort capture of the whole descendant-tree CPU (ns) rooted at `pid`,
/// accumulated into a task-local `map` that survives across poll ticks. Sums
/// the target pid AND every descendant's `cpu_ns` (via `proc_listchildpids`
/// recursion), folding each into `map` (max-per-pid). Returns the summed subtree
/// total, or `None` if the ROOT pid itself is gone (ESRCH) — the poll's
/// self-terminating stop condition (a live root with dead children still returns
/// `Some`).
///
/// **Why subtree, not single-process:** `proc_pidinfo` sums only the target
/// TASK's threads, MISSING forked child processes (rustc → rust-lld, cc-wrapper
/// → cc1) — exactly the fork-heavy link/compile CPU the shape classification
/// cares about (auditor Claim 1 / red-team A1-2). We enumerate descendants and
/// sum them.
///
/// **Known residuals (all bias the ratio DOWN — documented, NOT solved in code;
/// the analyst must caveat the §6 short-action-fraction / Q2 shape-mix
/// conclusions accordingly; on-worker validation is the canary grep, not a unit
/// test — see `deferred_tasks.md`):**
/// - **Within-one-interval spawn+die:** a descendant that both spawns AND exits
///   inside one 250 ms tick is never sampled → its CPU is missed. The shorter
///   250 ms interval reduces but does not close this window.
/// - **Reparent-orphan:** the walk roots at the direct child and stops when the
///   ROOT returns ESRCH. If an INTERMEDIATE process exits while a descendant
///   keeps running, that descendant reparents to launchd (pid 1) and LEAVES the
///   subtree — `proc_listchildpids` no longer finds it, and its CPU after the
///   reparent point is lost. For the target RBE toolchains this is RARE because
///   the root driver waits for all real work (a `process-wrapper` / `rustc`
///   waits for its `rust-lld`, so the fork-heavy case that motivates this walk
///   IS captured), but a detached/orphaned descendant (e.g. a wrapper that
///   fork-and-exits leaving a long-running compiler) is under-counted. The
///   `live_child_subtree...` test deliberately tests the parent-OUTLIVES-child
///   shape (the `& wait` keeps the parent alive) and does NOT exercise this
///   orphan shape — it is validated on-worker via the canary ratio grep.
/// - **PID-reuse within one action:** the map keys on pid with `max()`, so if
///   pid P is descendant A (CPU X), P exits, and P is REUSED by descendant B
///   (CPU Y), the map keeps `max(X, Y)` — the smaller of the two is dropped
///   (a SUM error, not a swap). Rare for a normal action (macOS PID space vs
///   seconds-to-minutes), but a fork-storm action (thousands of short
///   `cc`/`as`/`ld`) inside one action CAN wrap the pid space; such actions
///   under-count and should be excluded from the `>1.5` band conclusions.
#[cfg(target_os = "macos")]
fn calib_capture_subtree_cpu_ns(pid: u32, map: &mut HashMap<libc::pid_t, u64>) -> Option<u64> {
    let root = pid as libc::pid_t;
    // Root gone → whole subtree gone; signal stop (self-terminating poll).
    let root_ns = calib_capture_cpu_ns(pid)?;
    calib_accumulate_subtree_cpu(map, root, root_ns);

    // Breadth-first descendant walk, depth- and total-pid-capped. `pending`
    // holds (pid, depth) frontier entries; each dequeued pid's CPU is folded
    // and its immediate children enqueued until a cap trips.
    let mut pending: VecDeque<(libc::pid_t, u32)> = VecDeque::new();
    pending.push_back((root, 0));
    let mut summed_pids: usize = 1; // root already counted
    while let Some((parent, depth)) = pending.pop_front() {
        if depth >= CALIB_SUBTREE_MAX_DEPTH || summed_pids >= CALIB_SUBTREE_MAX_PIDS {
            break;
        }
        for child in calib_list_child_pids(parent) {
            if summed_pids >= CALIB_SUBTREE_MAX_PIDS {
                break;
            }
            // A child may have exited between the listing and the read → `None`;
            // its earlier CPU (if any) is already retained in `map`, so skip.
            if let Some(child_ns) = calib_capture_cpu_ns(child as u32) {
                calib_accumulate_subtree_cpu(map, child, child_ns);
            }
            summed_pids += 1;
            pending.push_back((child, depth + 1));
        }
    }

    Some(map.values().sum())
}

/// Enumerate the immediate child pids of `ppid` via `proc_listchildpids`.
/// Returns an empty vec on any error / no children. Bounded by
/// `CALIB_SUBTREE_MAX_PIDS` so the buffer allocation is capped.
#[cfg(target_os = "macos")]
fn calib_list_child_pids(ppid: libc::pid_t) -> Vec<libc::pid_t> {
    // First call with a null buffer returns the byte size needed (≈ count *
    // size_of::<pid_t>()). Then fetch into a sized, capped buffer.
    // SAFETY: `proc_listchildpids(ppid, NULL, 0)` is a read-only sizing query.
    let needed = unsafe { libc::proc_listchildpids(ppid, core::ptr::null_mut(), 0) };
    if needed <= 0 {
        return Vec::new();
    }
    let mut count = (needed as usize) / core::mem::size_of::<libc::pid_t>();
    // CAPPED AT CALIB_SUBTREE_MAX_PIDS: bound the buffer even if the kernel
    // reports an implausibly large child count.
    count = count.min(CALIB_SUBTREE_MAX_PIDS);
    if count == 0 {
        return Vec::new();
    }
    let mut buf: Vec<libc::pid_t> = vec![0; count];
    let buf_size = (count * core::mem::size_of::<libc::pid_t>()) as libc::c_int;
    // SAFETY: `buf` owns `count` `pid_t` slots; we pass its exact byte size and
    // only read the returned pid slots. Read-only, does not retain the ptr.
    let written = unsafe {
        libc::proc_listchildpids(ppid, buf.as_mut_ptr().cast::<libc::c_void>(), buf_size)
    };
    if written <= 0 {
        return Vec::new();
    }
    // The buffer fetch returns the COUNT of pids written, NOT a byte count —
    // empirically confirmed on M4 (a 1-child parent returns 1). The earlier bug
    // divided this by size_of::<pid_t>() (1/4 = 0), yielding an empty child list
    // so the subtree walk silently degraded to a single-pid read of the ~idle
    // parent (caught by the on-worker `live_child_subtree_cpu_includes_grandchild`
    // test). The NULL sizing call above returns a generous byte-ish upper bound
    // (so `/size_of` is correct THERE for the allocation), but the fetch return
    // is a plain pid count.
    let got = (written as usize).min(count);
    buf.truncate(got);
    // Filter out any 0/negative sentinel the kernel may leave.
    buf.retain(|&p| p > 0);
    buf
}

#[cfg(not(target_os = "macos"))]
fn calib_capture_subtree_cpu_ns(_pid: u32, _map: &mut HashMap<libc::pid_t, u64>) -> Option<u64> {
    // See `calib_capture_cpu_ns`: no zero-behavior-change path on non-macOS.
    None
}

/// P-A CPU-time poll loop. Repeatedly awaits `capture()` (in production the
/// live-PID subtree CPU query in **nanoseconds**, `calib_capture_subtree_cpu_ns`
/// run in the poll task), keeping the LAST `Some` value in `last` and setting
/// `has_value` once any `Some` is seen. Sleeps `interval` between samples. STOPS
/// on the first `None` — which the capture returns once the ROOT child is
/// reaped/gone — so the task self-terminates without any external signal. The
/// stored value is a monotonic non-decreasing subtree total (the task-local map
/// only grows), so "keep the last `Some`" == "keep the accumulated total".
///
/// The value carried in `last` is unit-agnostic to this loop (production passes
/// ns; the completion arm converts ns → ms once); the loop only ever moves a
/// `u64` from `capture()` into `last`.
///
/// Factored generic over the capture future so the store-last / stop-on-None
/// core is unit-testable with an injected closure (no real syscall, no real
/// sleep — pass `Duration::ZERO`). `capture` returns a future so the production
/// call can run its syscalls inside it without borrowing the loop's signature.
///
/// Capture-then-sleep order: the first sample is taken immediately so even a
/// sub-`interval` action gets one live reading before its child exits.
async fn calib_poll_cpu_time_loop<F, Fut>(
    interval: Duration,
    mut capture: F,
    last: Arc<core::sync::atomic::AtomicU64>,
    has_value: Arc<AtomicBool>,
) where
    F: FnMut() -> Fut + Send,
    Fut: core::future::Future<Output = Option<u64>> + Send,
{
    loop {
        match capture().await {
            Some(ms) => {
                // Flag-and-data pairing (FIX): the completion arm reads
                // `has_value` then `last` from a DIFFERENT task with no other
                // happens-before edge, so `Relaxed` on both would let it observe
                // `has_value=true` with a stale `last=0` (a false `Some(0)`) —
                // real on the ARM/M4 workers. `Release` here publishes the
                // preceding `last` store; the reader's `Acquire` (see
                // `calib_harvest_cpu_time`) then observes it. `last` may stay
                // `Relaxed`; the Release/Acquire on the flag carries the order.
                last.store(ms, Ordering::Relaxed);
                has_value.store(true, Ordering::Release);
            }
            // Child reaped/gone (ESRCH) — nothing more to sample; self-terminate.
            None => break,
        }
        tokio::time::sleep(interval).await;
    }
}

// CAPPED AT 32: process-wide limit on concurrent action-cleanup directory
// deletes (`do_cleanup` -> `bounded_remove_dir_all` -> `fs::remove_dir_all`).
// Justification: `remove_dir_all` runs on the tokio blocking pool (sized to
// 1024 threads at `src/bin/nativelink.rs:2783` `.max_blocking_threads(1024)`,
// SHARED with hashing + sync-fs on the upload/download data path). Recursive
// deletes are slow and, worse, go uninterruptible D-state — the isotope wedge
// dump (2026-06-16, `nativelink-stall-1781634296551.txt`) caught 157
// blocking-pool threads simultaneously stuck in `remove_dir_all_recursive`
// (~15% of the 1024 pool consumed by deletes alone) starving the data plane
// (see `.claude/audits/isotope-cleanup-fanout-evidence-2026-06-16.md`). 32
// bounds delete occupancy to ~3% of the 1024 pool, leaving ~992 threads for
// hashing/fs; far below the observed 157. Worker action concurrency is itself
// UNCAPPED (`max_inflight_tasks` default 0 = unbounded), so this cap, NOT an
// action count, is what bounds delete occupancy. Acquired INSIDE the spawned
// cleanup task (not in `RunningActionImpl::drop`, which is sync and must not
// block): `drop` only spawns; the spawned task awaits the permit before
// deleting. Over-cap behavior: queued cleanups park on a cheap async
// permit-wait (a few Arcs + a PathBuf + the directory-cache pin guard — NO
// blocking-pool thread) and drain in 32-wide waves as permits free; a
// >32-simultaneous-completion burst DOES queue at the semaphore — that is the
// intended throttle, holding no blocking thread. Blocking-pool delete
// occupancy is the bounded resource. Cleanups EVENTUALLY drain provided the
// underlying deletes return; ≥32 simultaneously wedged D-state deletes hold
// their permits until the kernel calls return and head-of-line-block
// subsequent cleanups — the intended trade (data-plane protection over cleanup
// liveness; a D-state syscall is uninterruptible, so no timeout could free it
// anyway). Falsification: T
// `concurrent_deletes_never_exceed_cap_and_all_complete` drives M = 3×cap
// concurrent deletes through an injected counting hook; observed max must
// equal cap and all M must complete within the deadlock-detector window.
pub const CLEANUP_DELETE_INFLIGHT_CAP: usize = 32;

/// Process-singleton semaphore bounding concurrent action-cleanup directory
/// deletes at [`CLEANUP_DELETE_INFLIGHT_CAP`]. Process-level (not per-worker)
/// because the resource it protects — the tokio blocking pool — is itself
/// process-global, and `do_cleanup` is a free function reached from two call
/// sites (the `RunningActionImpl::drop` background spawn and the normal
/// `cleanup` future) that share no per-worker handle to thread an
/// `Arc<Semaphore>` through. See the `CLEANUP_DELETE_INFLIGHT_CAP` rationale.
static CLEANUP_DELETE_SEMAPHORE: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(CLEANUP_DELETE_INFLIGHT_CAP));

/// Test-only injectable delete hook. When installed, [`bounded_remove_dir_all`]
/// routes the delete through this closure (still under the real semaphore
/// permit) instead of touching the filesystem, letting a test observe the
/// max concurrent-delete count. Standard test-injection seam shape:
/// `LazyLock<Mutex<Option<..>>>`, single slot, absent from production builds.
#[cfg(feature = "test-utils")]
#[expect(clippy::type_complexity, reason = "test-only injected async delete fn")]
// UNBOUNDED-OK: single Arc slot, one per process in test; cfg-gated out of prod.
static CLEANUP_DELETE_TEST_HOOK: std::sync::LazyLock<
    parking_lot::Mutex<
        Option<
            std::sync::Arc<
                dyn Fn(
                        std::path::PathBuf,
                    ) -> core::pin::Pin<
                        Box<dyn core::future::Future<Output = Result<(), Error>> + Send>,
                    > + Send
                    + Sync,
            >,
        >,
    >,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(None));

/// Install a delete hook so the next [`bounded_remove_dir_all`] calls route
/// through `hook` (under the real semaphore) instead of `fs::remove_dir_all`.
/// Only available with the `test-utils` feature.
#[cfg(feature = "test-utils")]
#[expect(clippy::type_complexity, reason = "test-only injected async delete fn")]
pub fn install_cleanup_delete_test_hook(
    hook: std::sync::Arc<
        dyn Fn(
                std::path::PathBuf,
            )
                -> core::pin::Pin<Box<dyn core::future::Future<Output = Result<(), Error>> + Send>>
            + Send
            + Sync,
    >,
) {
    *CLEANUP_DELETE_TEST_HOOK.lock() = Some(hook);
}

/// Remove the installed delete hook so subsequent deletes hit the real
/// filesystem path again. Only available with the `test-utils` feature.
#[cfg(feature = "test-utils")]
pub fn take_cleanup_delete_test_hook() {
    *CLEANUP_DELETE_TEST_HOOK.lock() = None;
}

/// Bounded directory delete used by `do_cleanup`. Acquires a permit on the
/// process-singleton [`CLEANUP_DELETE_SEMAPHORE`] BEFORE running the blocking
/// `fs::remove_dir_all`, so a mass-drop cleanup burst cannot saturate the
/// shared tokio blocking pool (see [`CLEANUP_DELETE_INFLIGHT_CAP`]). The
/// permit is held for the duration of the delete (including the single retry)
/// and released when this future returns. Retains the existing one-retry on
/// transient failure (macOS Spotlight/Finder ENOTEMPTY races).
///
/// MUST be called from inside an already-spawned async task, never from the
/// synchronous `RunningActionImpl::drop` body — `drop` spawns the task; the
/// permit is awaited inside it.
async fn bounded_remove_dir_all(action_directory: &str) -> Result<(), Error> {
    // `acquire()` only errors if the semaphore is closed; we never close it,
    // so this cannot fail in practice. err_tip preserves the contract surface.
    let _permit = CLEANUP_DELETE_SEMAPHORE
        .acquire()
        .await
        .err_tip(|| "cleanup-delete semaphore closed")?;

    #[cfg(feature = "test-utils")]
    {
        // Snapshot+drop the lock before awaiting (never hold a sync Mutex
        // across .await). The hook runs under the permit we just acquired.
        let hook = CLEANUP_DELETE_TEST_HOOK.lock().clone();
        if let Some(hook) = hook {
            return hook(std::path::PathBuf::from(action_directory)).await;
        }
    }

    match fs::remove_dir_all(action_directory).await {
        Ok(()) => Ok(()),
        Err(_) => {
            // On macOS, Spotlight/Finder can momentarily recreate files
            // (e.g. .DS_Store) during deletion, causing ENOTEMPTY. A short
            // delay and single retry is sufficient.
            tokio::time::sleep(Duration::from_millis(100)).await;
            fs::remove_dir_all(action_directory).await
        }
    }
    .err_tip(|| format!("Could not remove working directory {action_directory}"))
}

/// For simplicity we use a fixed exit code for cases when our program is terminated
/// due to a signal.
const EXIT_CODE_FOR_SIGNAL: i32 = 9;

/// Default strategy for uploading historical results.
/// Note: If this value changes the config documentation
/// should reflect it.
const DEFAULT_HISTORICAL_RESULTS_STRATEGY: UploadCacheResultsStrategy =
    UploadCacheResultsStrategy::FailuresOnly;

/// Valid string reasons for a failure.
/// Note: If these change, the documentation should be updated.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SideChannelFailureReason {
    /// Task should be considered timed out.
    Timeout,
}

/// This represents the json data that can be passed from the running process
/// to the parent via the `SideChannelFile`. See:
/// `config::EnvironmentSource::sidechannelfile` for more details.
/// Note: Any fields added here must be added to the documentation.
#[derive(Debug, Deserialize, Default)]
struct SideChannelInfo {
    /// If the task should be considered a failure and why.
    failure: Option<SideChannelFailureReason>,
}

/// Metadata about a file to be materialized from CAS to disk.
struct FileToMaterialize {
    digest: DigestInfo,
    dest: String,
    #[cfg(target_family = "unix")]
    unix_mode: Option<u32>,
    mtime: Option<prost_types::Timestamp>,
}

/// Parse a GetTree response into a digest-keyed map. Each directory's digest
/// is computed by hashing its serialized protobuf, making the result
/// position-independent (tolerant GetTree responses with missing entries
/// are handled correctly). The resulting tree may be incomplete — the
/// caller should validate and gap-fill.
pub fn parse_get_tree_response(
    all_dirs: Vec<ProtoDirectory>,
    root_digest: &DigestInfo,
) -> HashMap<DigestInfo, ProtoDirectory> {
    // Compute each directory's content digest from its serialized proto.
    // Digest function comes from the current context; falls back to BLAKE3.
    let digest_function = Context::current()
        .get::<DigestHasherFunc>()
        .map_or_else(default_digest_hasher_func, |v| *v);

    let mut tree = HashMap::with_capacity(all_dirs.len());
    for dir in all_dirs {
        let encoded = dir.encode_to_vec();
        let mut hasher = digest_function.hasher();
        hasher.update(&encoded);
        let computed_digest = hasher.finalize_digest();
        tree.insert(computed_digest, dir);
    }

    // If the root digest isn't in the tree (different serialization produced
    // a different hash), fall back: assume position 0 is the root.
    if !tree.contains_key(root_digest) && !tree.is_empty() {
        // The root might have been computed with a different hash due to
        // protobuf serialization differences. Try to identify it by
        // matching: the root should be the only directory not referenced
        // as a child by any other directory.
        let all_child_digests: HashSet<DigestInfo> = tree
            .values()
            .flat_map(|dir| &dir.directories)
            .filter_map(|node| {
                node.digest
                    .as_ref()
                    .and_then(|d| DigestInfo::try_from(d).ok())
            })
            .collect();
        let orphans: Vec<DigestInfo> = tree
            .keys()
            .filter(|d| !all_child_digests.contains(d))
            .copied()
            .collect();
        if orphans.len() == 1 {
            // Found a unique root — re-key it under root_digest.
            if let Some(root_dir) = tree.remove(&orphans[0]) {
                tree.insert(*root_digest, root_dir);
            }
        } else {
            // 0 or >1 orphans: we cannot safely promote one to root_digest.
            // Caller will fall through to BFS rebuild, but make the silent
            // "tree without a usable root" path visible in logs.
            warn!(
                expected_root = ?root_digest,
                orphan_count = orphans.len(),
                tree_size = tree.len(),
                "parse_get_tree_response: cannot identify root from orphans; root_digest absent from GetTree response",
            );
        }
    }

    tree
}

/// Verify that a resolved directory tree is structurally complete and acyclic:
/// the root is present, every directory reachable from the root is a key in
/// the map, and no directory transitively references itself. Any violation
/// is a hard error — propagating an incomplete or cyclic tree to the
/// construction code would let it silently skip missing subdirectories or
/// loop forever in the materialization BFS.
pub(crate) fn assert_tree_complete(
    tree: &HashMap<DigestInfo, ProtoDirectory>,
    root_digest: &DigestInfo,
    source: &'static str,
) -> Result<(), Error> {
    // Iterative DFS with explicit ancestor tracking. The ancestor set on the
    // current path distinguishes a cycle (revisit of an ancestor) from a
    // diamond (revisit of a fully-explored sibling subtree).
    enum Frame {
        Enter(DigestInfo),
        Exit(DigestInfo),
    }
    let mut stack: Vec<Frame> = vec![Frame::Enter(*root_digest)];
    let mut ancestors: HashSet<DigestInfo> = HashSet::new();
    let mut finished: HashSet<DigestInfo> = HashSet::with_capacity(tree.len());
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Exit(digest) => {
                ancestors.remove(&digest);
                finished.insert(digest);
            }
            Frame::Enter(digest) => {
                if finished.contains(&digest) {
                    continue;
                }
                if !ancestors.insert(digest) {
                    return Err(make_err!(
                        Code::Internal,
                        "{source}: directory cycle detected at {digest:?} — refusing to construct cyclic tree",
                    ));
                }
                let dir = tree.get(&digest).ok_or_else(|| {
                    make_err!(
                        Code::Internal,
                        "{source}: resolved tree missing reachable directory {digest:?}",
                    )
                })?;
                stack.push(Frame::Exit(digest));
                for node in &dir.directories {
                    let child_digest: DigestInfo = node
                        .digest
                        .as_ref()
                        .ok_or_else(|| {
                            make_err!(
                                Code::InvalidArgument,
                                "{source}: directory node {} in parent {digest:?} missing digest",
                                node.name,
                            )
                        })?
                        .try_into()
                        .map_err(|e| {
                            make_err!(
                                Code::InvalidArgument,
                                "{source}: invalid digest for node {} in parent {digest:?}: {e:?}",
                                node.name,
                            )
                        })?;
                    if !tree.contains_key(&child_digest) {
                        return Err(make_err!(
                            Code::Internal,
                            "{source}: resolved tree missing referenced child {child_digest:?} (referenced by {digest:?} as {})",
                            node.name,
                        ));
                    }
                    stack.push(Frame::Enter(child_digest));
                }
            }
        }
    }
    Ok(())
}

/// Maximum size for a blob to be eligible for BatchReadBlobs (1 MiB).
/// Blobs larger than this use the existing ByteStream path.
const BATCH_READ_MAX_BLOB_SIZE: u64 = 1024 * 1024;

/// Maximum total payload per BatchReadBlobs request (4 MiB), per REAPI recommendation.
const BATCH_READ_MAX_REQUEST_SIZE: u64 = 4 * 1024 * 1024;

/// Resolve the full directory tree starting from `root_digest`.
///
/// Tries the `GetTree` RPC (single streaming call) if the slow store is a `GrpcStore`.
/// Falls back to recursive `get_and_decode_digest` calls otherwise.
///
/// Returns a map from digest to Directory proto for every directory in the tree.
pub async fn resolve_directory_tree(
    cas_store: &FastSlowStore,
    root_digest: &DigestInfo,
) -> Result<HashMap<DigestInfo, ProtoDirectory>, Error> {
    let tree_start = std::time::Instant::now();
    info!(
        root = ?root_digest,
        "resolve_directory_tree: starting tree resolution",
    );
    // Try the fast path: GetTree RPC via the underlying GrpcStore.
    if let Some(grpc_store) = cas_store.slow_store().downcast_ref::<GrpcStore>(None) {
        info!(
            root = ?root_digest,
            method = "GetTree RPC",
            "resolve_directory_tree: using GetTree RPC fast path",
        );
        let request = GetTreeRequest {
            instance_name: String::new(), // GrpcStore fills this in
            root_digest: Some((*root_digest).into()),
            page_size: 0, // server decides
            page_token: String::new(),
            digest_function: Context::current()
                .get::<DigestHasherFunc>()
                .map_or_else(default_digest_hasher_func, |v| *v)
                .proto_digest_func()
                .into(),
        };

        match grpc_store.get_tree(Request::new(request)).await {
            Ok(response) => {
                let rpc_elapsed = tree_start.elapsed();
                let mut stream = response.into_inner();
                // Collect all directories from the stream into a flat list.
                let mut all_dirs: Vec<ProtoDirectory> = Vec::new();
                loop {
                    match stream.message().await {
                        Ok(None) => break,
                        Ok(Some(resp)) => all_dirs.extend(resp.directories),
                        Err(status) => {
                            // #147: GetTree streams over the same pooled
                            // h2 channel that #147 wedges. If the body
                            // errs with a transport-shaped status,
                            // evict one idle channel before the caller
                            // surfaces the error so the next attempt
                            // gets a fresh channel.
                            let err: Error = status.into();
                            grpc_store.evict_pool_on_transport_err(&err);
                            return Err(err.append("In GetTree stream"));
                        }
                    }
                }
                let stream_elapsed = tree_start.elapsed();

                info!(
                    root = ?root_digest,
                    raw_dir_count = all_dirs.len(),
                    rpc_connect_ms = rpc_elapsed.as_millis() as u64,
                    stream_complete_ms = stream_elapsed.as_millis() as u64,
                    "resolve_directory_tree: GetTree stream received",
                );

                if !all_dirs.is_empty() {
                    let mut tree = parse_get_tree_response(all_dirs, root_digest);

                    // Validate structural completeness: every child reference
                    // should point to a digest in the tree.
                    let tree_valid = tree.contains_key(root_digest) && {
                        tree.values().all(|dir| {
                            dir.directories.iter().all(|node| {
                                node.digest
                                    .as_ref()
                                    .and_then(|d| DigestInfo::try_from(d).ok())
                                    .is_some_and(|d| tree.contains_key(&d))
                            })
                        })
                    };

                    if tree_valid {
                        let elapsed = tree_start.elapsed();
                        let total_bytes: u64 = tree.keys().map(|d| d.size_bytes()).sum();
                        let total_files: usize = tree.values().map(|d| d.files.len()).sum();
                        let total_symlinks: usize = tree.values().map(|d| d.symlinks.len()).sum();
                        info!(
                            root = ?root_digest,
                            dir_count = tree.len(),
                            total_files,
                            total_symlinks,
                            total_bytes,
                            elapsed_ms = elapsed.as_millis() as u64,
                            "resolve_directory_tree: completed via GetTree RPC"
                        );
                        return Ok(tree);
                    }
                    // Tree is incomplete — some directories missing (server may
                    // have returned a partial tree due to evicted blobs). Count
                    // the gaps and fill them via parallel BFS for only the missing
                    // directories, keeping everything GetTree already gave us.
                    let missing_children: usize = tree.values().map(|dir| {
                        dir.directories.iter().filter(|node| {
                            node.digest
                                .as_ref()
                                .and_then(|d| DigestInfo::try_from(d).ok())
                                .map_or(true, |d| !tree.contains_key(&d))
                        }).count()
                    }).sum();
                    if tree.contains_key(root_digest) && missing_children > 0 {
                        // We have the root and some subtrees but not all. Use
                        // parallel BFS to fill in just the missing subtrees.
                        info!(
                            root = ?root_digest,
                            tree_size = tree.len(),
                            missing_children,
                            "resolve_directory_tree: GetTree partial, filling gaps via parallel BFS"
                        );
                        let gap_start = std::time::Instant::now();
                        resolve_directory_tree_fill_gaps(cas_store, &mut tree).await?;
                        assert_tree_complete(
                            &tree,
                            root_digest,
                            "resolve_directory_tree GetTree+gap-fill",
                        )?;
                        let gap_elapsed = gap_start.elapsed();
                        let total_bytes: u64 = tree.keys().map(|d| d.size_bytes()).sum();
                        let total_files: usize = tree.values().map(|d| d.files.len()).sum();
                        info!(
                            root = ?root_digest,
                            dir_count = tree.len(),
                            total_files,
                            total_bytes,
                            gap_fill_ms = gap_elapsed.as_millis() as u64,
                            total_elapsed_ms = tree_start.elapsed().as_millis() as u64,
                            "resolve_directory_tree: completed via GetTree + gap fill"
                        );
                        return Ok(tree);
                    }
                    warn!(
                        root = ?root_digest,
                        tree_has_root = tree.contains_key(root_digest),
                        tree_size = tree.len(),
                        missing_children,
                        validation_elapsed_ms = tree_start.elapsed().as_millis() as u64,
                        "resolve_directory_tree: GetTree BFS validation failed, falling back to parallel BFS"
                    );
                }
            }
            Err(e) => {
                warn!(
                    root = ?root_digest,
                    err = ?e,
                    elapsed_ms = tree_start.elapsed().as_millis() as u64,
                    "resolve_directory_tree: GetTree RPC failed, falling back to parallel BFS"
                );
            }
        }
    } else {
        info!(
            root = ?root_digest,
            method = "parallel BFS",
            "resolve_directory_tree: no GrpcStore available, using parallel BFS",
        );
    }

    // Fallback: parallel BFS fetch — fetches all directories at each BFS level
    // concurrently, avoiding the sequential 134ms-per-RPC bottleneck of the old
    // recursive DFS approach.
    let parallel_start = std::time::Instant::now();
    let tree = resolve_directory_tree_parallel(cas_store, root_digest).await?;
    assert_tree_complete(&tree, root_digest, "resolve_directory_tree parallel BFS")?;
    let parallel_elapsed = parallel_start.elapsed();
    let total_elapsed = tree_start.elapsed();
    let total_bytes: u64 = tree.keys().map(|d| d.size_bytes()).sum();
    let total_files: usize = tree.values().map(|d| d.files.len()).sum();
    let total_symlinks: usize = tree.values().map(|d| d.symlinks.len()).sum();
    info!(
        root = ?root_digest,
        dir_count = tree.len(),
        total_files,
        total_symlinks,
        total_bytes,
        individual_fetches = tree.len(),
        parallel_ms = parallel_elapsed.as_millis() as u64,
        total_elapsed_ms = total_elapsed.as_millis() as u64,
        "resolve_directory_tree: completed via parallel BFS fetch"
    );
    Ok(tree)
}

/// Fetch all directories in a tree using parallel BFS.
///
/// Instead of sequential DFS (one RPC per directory, ~134ms each), this fetches
/// all directories at each BFS level concurrently using `buffer_unordered(64)`.
/// For a tree with 1000 directories across 10 levels, this reduces wall-clock
/// time from ~134s to ~1.3s (10 levels x 134ms per level).
///
/// The GrpcStore internally routes small blob reads through `BatchReadBlobs`,
/// so the 64-wide concurrency naturally batches into efficient RPCs.
async fn resolve_directory_tree_parallel(
    cas_store: &FastSlowStore,
    root_digest: &DigestInfo,
) -> Result<HashMap<DigestInfo, ProtoDirectory>, Error> {
    let mut tree = HashMap::new();
    let mut seen = HashSet::new();
    let mut queue: Vec<DigestInfo> = vec![*root_digest];
    seen.insert(*root_digest);

    let mut bfs_level: u32 = 0;

    while !queue.is_empty() {
        let level_start = std::time::Instant::now();
        let level_size = queue.len();

        // Fetch all directories in the current BFS level concurrently.
        // TODO(#speculative-prefetch-io-priority): parallel-BFS network
        // fetch-enqueue site (resolve phase, shared by real + speculative
        // constructs). A future IO-priority scheduler would read the driving
        // OpPriority here and deprioritize a Speculative resolve's fetches so
        // they never delay a Foreground action's resolve. Marker-first: the tag
        // is not threaded into this shared helper yet (build-spec §Revision-2).
        let results: Vec<Result<(DigestInfo, ProtoDirectory), Error>> =
            futures::stream::iter(queue.drain(..).map(|digest| {
                async move {
                    let dir =
                        get_and_decode_digest::<ProtoDirectory>(cas_store, digest.into())
                            .await
                            .err_tip(|| {
                                format!(
                                    "Fetching directory {digest} in parallel BFS (level {bfs_level})"
                                )
                            })?;
                    Ok((digest, dir))
                }
            }))
            .buffer_unordered(64)
            .collect()
            .await;

        // Process results: insert into tree and collect children for the next level.
        let mut new_children: u64 = 0;
        for result in results {
            let (digest, directory) = result?;
            for child_node in &directory.directories {
                let child_digest: DigestInfo = child_node
                    .digest
                    .as_ref()
                    .err_tip(|| "Expected Digest in DirectoryNode")?
                    .try_into()
                    .err_tip(|| "Parsing child directory digest in parallel BFS")?;
                if seen.insert(child_digest) {
                    queue.push(child_digest);
                    new_children += 1;
                }
            }
            tree.insert(digest, directory);
        }

        let level_ms = level_start.elapsed().as_millis() as u64;
        if level_ms > 100 {
            warn!(
                bfs_level,
                dirs_fetched = level_size,
                new_children,
                elapsed_ms = level_ms,
                "resolve_directory_tree_parallel: slow BFS level (>100ms)"
            );
        } else {
            debug!(
                bfs_level,
                dirs_fetched = level_size,
                new_children,
                elapsed_ms = level_ms,
                "resolve_directory_tree_parallel: BFS level completed"
            );
        }

        bfs_level += 1;
    }

    Ok(tree)
}

/// Fill gaps in a partially-resolved directory tree.
///
/// When GetTree returns a partial response (some directories missing due to
/// eviction), this function finds all child references that point to missing
/// directories and fetches them via parallel BFS. It modifies the tree in-place,
/// adding the missing directories.
async fn resolve_directory_tree_fill_gaps(
    cas_store: &FastSlowStore,
    tree: &mut HashMap<DigestInfo, ProtoDirectory>,
) -> Result<(), Error> {
    let mut seen: HashSet<DigestInfo> = tree.keys().copied().collect();

    // Find all child references that point to missing directories.
    let mut queue: Vec<DigestInfo> = tree
        .values()
        .flat_map(|dir| &dir.directories)
        .filter_map(|node| {
            node.digest
                .as_ref()
                .and_then(|d| DigestInfo::try_from(d).ok())
        })
        .filter(|d| !tree.contains_key(d))
        .collect();
    // Deduplicate the initial queue.
    queue.sort_unstable();
    queue.dedup();
    for d in &queue {
        seen.insert(*d);
    }

    let mut bfs_level: u32 = 0;

    while !queue.is_empty() {
        let level_start = std::time::Instant::now();
        let level_size = queue.len();

        let results: Vec<Result<(DigestInfo, ProtoDirectory), Error>> =
            futures::stream::iter(queue.drain(..).map(|digest| {
                async move {
                    let dir =
                        get_and_decode_digest::<ProtoDirectory>(cas_store, digest.into())
                            .await
                            .err_tip(|| {
                                format!("Fetching gap directory {digest} in parallel BFS")
                            })?;
                    Ok((digest, dir))
                }
            }))
            .buffer_unordered(64)
            .collect()
            .await;

        for result in results {
            let (digest, directory) = result?;
            for child_node in &directory.directories {
                let child_digest: DigestInfo = child_node
                    .digest
                    .as_ref()
                    .err_tip(|| "Expected Digest in DirectoryNode")?
                    .try_into()
                    .err_tip(|| "Parsing child directory digest in gap fill")?;
                if seen.insert(child_digest) && !tree.contains_key(&child_digest) {
                    queue.push(child_digest);
                }
            }
            tree.insert(digest, directory);
        }

        debug!(
            bfs_level,
            dirs_fetched = level_size,
            remaining = queue.len(),
            elapsed_ms = level_start.elapsed().as_millis() as u64,
            "resolve_directory_tree_fill_gaps: BFS level completed"
        );
        bfs_level += 1;
    }

    Ok(())
}

// TODO(tree-dedup): Add a tree_resolution_dedup map to RunningActionsManagerImpl
// to coalesce concurrent resolutions for the same input_root_digest. When multiple
// actions share the same input tree, only one should fetch it while others wait.

/// Walk the resolved directory tree, creating all directories and collecting
/// all files that need to be materialized. Returns the flat list of files.
fn collect_files_from_tree(
    tree: &HashMap<DigestInfo, ProtoDirectory>,
    root_digest: &DigestInfo,
    root_path: &str,
) -> Result<(Vec<FileToMaterialize>, Vec<(String, String)>), Error> {
    let mut files = Vec::new();
    // (symlink_target, dest_path)
    let mut symlinks: Vec<(String, String)> = Vec::new();
    // BFS to create directories in order and collect files.
    let mut queue = VecDeque::new();
    queue.push_back((*root_digest, root_path.to_string()));

    while let Some((dir_digest, dir_path)) = queue.pop_front() {
        let directory = tree.get(&dir_digest).ok_or_else(|| {
            make_err!(
                Code::Internal,
                "Directory {dir_digest:?} not found in resolved tree"
            )
        })?;

        for file in &directory.files {
            let digest: DigestInfo = file
                .digest
                .as_ref()
                .err_tip(|| "Expected Digest in Directory::file::digest")?
                .try_into()
                .err_tip(|| "In Directory::file::digest")?;
            let dest = format!("{}/{}", dir_path, file.name);

            #[cfg(target_family = "unix")]
            let unix_mode = {
                let (_, mut mode) = match &file.node_properties {
                    Some(properties) => (properties.mtime.clone(), properties.unix_mode),
                    None => (None, None),
                };
                if file.is_executable {
                    mode = Some(mode.unwrap_or(0o555) | 0o111);
                }
                // Default to 0o555 (read+execute, no write) to match CAS store
                // defaults. Some build tools (rules_cc, rules_rust) set
                // is_executable=false on shell scripts that must be executable;
                // using 0o555 as the base avoids breaking those actions.
                Some(mode.unwrap_or(0o555))
            };

            let mtime = file.node_properties.as_ref().and_then(|p| p.mtime.clone());

            files.push(FileToMaterialize {
                digest,
                dest,
                #[cfg(target_family = "unix")]
                unix_mode,
                mtime,
            });
        }

        for subdir in &directory.directories {
            let child_digest: DigestInfo = subdir
                .digest
                .as_ref()
                .err_tip(|| "Expected Digest in Directory::directories::digest")?
                .try_into()
                .err_tip(|| "In Directory::directories::digest")?;
            let child_path = format!("{}/{}", dir_path, subdir.name);
            queue.push_back((child_digest, child_path));
        }

        #[cfg(target_family = "unix")]
        for symlink_node in &directory.symlinks {
            let dest = format!("{}/{}", dir_path, symlink_node.name);
            symlinks.push((symlink_node.target.clone(), dest));
        }
    }

    Ok((files, symlinks))
}

/// Maximum number of concurrent BatchReadBlobs RPCs in flight.
const BATCH_READ_CONCURRENCY: usize = 32;

/// Maximum number of concurrent ByteStream fetches in flight.

/// Batch-download small blobs via `BatchReadBlobs` and write them into the fast store.
/// Returns the set of digests that were successfully fetched.
///
/// If WorkerProxyStore is available, races peer reads against server reads:
/// all digests are sent to the server, and peer-matched digests are also
/// sent to the peers that have them. First result wins per digest.
/// Connections to peers are created lazily on first use.
/// Any misses are retried via `populate_fast_store_unchecked`.
async fn batch_read_small_blobs(
    cas_store: &FastSlowStore,
    small_digests: &[DigestInfo],
) -> Result<HashSet<DigestInfo>, Error> {
    let slow_store = cas_store.slow_store();

    // Try locality-aware routing through WorkerProxyStore.
    // Use as_store_driver().as_any() instead of downcast_ref() because
    // WorkerProxyStore::inner_store() delegates to its inner GrpcStore,
    // so Store::downcast_ref (which walks inner_store()) would skip past
    // the WorkerProxyStore and never find it.
    if let Some(proxy) = slow_store.as_store_driver().as_any().downcast_ref::<WorkerProxyStore>() {
        // Assign digests to peer endpoints using the locality map.
        let mut endpoint_digests: HashMap<Arc<str>, Vec<DigestInfo>> = HashMap::new();
        // (#sbrace) Small-blob race telemetry: digests with NO peer in the
        // locality map (never raced, server-only) — the "no_peer" outcome
        // bucket, recorded once below via the WorkerProxyStore metrics home.
        // CAPPED AT small_digests.len(): a subset of the caller's already-
        // bounded input digest slice; DigestInfo is a small POD (hash+size),
        // no owned blob bytes; freed at end of fetch.
        let mut no_peer_digests: Vec<DigestInfo> = Vec::new();
        {
            let locality = proxy.locality_map().read();
            let mut round_robin_idx: usize = 0;
            for &digest in small_digests {
                let peers = locality.lookup_workers(&digest);
                if peers.is_empty() {
                    no_peer_digests.push(digest);
                } else {
                    let endpoint = peers[round_robin_idx % peers.len()].clone();
                    round_robin_idx = round_robin_idx.wrapping_add(1);
                    endpoint_digests
                        .entry(endpoint)
                        .or_default()
                        .push(digest);
                }
            }
        }

        let peer_blob_count: usize = endpoint_digests.values().map(|v| v.len()).sum();

        if peer_blob_count > 0 {
            // Lazily create connections to peer endpoints.
            let mut peer_connections: Vec<(Arc<str>, Store, Vec<DigestInfo>)> = Vec::new();
            for (endpoint, digests) in endpoint_digests {
                if let Some(store) = proxy.get_or_create_connection(&endpoint).await {
                    peer_connections.push((endpoint, store, digests));
                }
            }

            let connected_peers = peer_connections.len();
            let connected_blob_count: usize =
                peer_connections.iter().map(|(_, _, d)| d.len()).sum();

            info!(
                total = small_digests.len(),
                to_peers = connected_blob_count,
                to_server = small_digests.len(),
                peer_endpoints = connected_peers,
                "BatchReadBlobs: racing peers against server"
            );

            // Build peer batch futures. Each peer connection is owned, so we
            // spawn the batches inline and reference the store by borrow.
            let mut race_futures: Vec<
                std::pin::Pin<Box<dyn Future<Output = (&str, Result<Vec<DigestInfo>, Error>)> + Send + '_>>,
            > = Vec::new();

            for (endpoint, store, digests) in &peer_connections {
                if let Some(grpc) = store.downcast_ref::<GrpcStore>(None) {
                    for batch in partition_into_batches(digests) {
                        race_futures.push(Box::pin(async move {
                            let result = execute_batch_read(grpc, cas_store, &batch).await;
                            (endpoint.as_ref(), result)
                        }));
                    }
                }
            }

            // Server gets ALL digests (races against peers — first result wins).
            if let Some(server_grpc) = proxy.inner_store().downcast_ref::<GrpcStore>(None) {
                for batch in partition_into_batches(small_digests) {
                    race_futures.push(Box::pin(async move {
                        let result = execute_batch_read(server_grpc, cas_store, &batch).await;
                        ("server", result)
                    }));
                }
            }

            // Execute all batches in parallel — peers and server race.
            let results = futures::future::join_all(race_futures).await;

            // Collect each source's completed digests in RACE ORDER (peers
            // first, server last — join_all preserves input order). Feeds both
            // the fetched-set fold (unchanged: fetched = union of all Ok sets)
            // and the (#sbrace) offload attribution below. `is_server` is the
            // "server" label the server batches were tagged with; peer
            // endpoints are host:port URIs, never the literal "server".
            // CAPPED AT (peers+1) x small_digests.len() DigestInfo (a small POD;
            // no owned blob bytes); freed at end of fetch.
            let mut source_completions: Vec<(bool, Vec<DigestInfo>)> = Vec::new();
            for (ep, result) in results {
                match result {
                    Ok(completed) => source_completions.push((ep == "server", completed)),
                    Err(e) => info!(endpoint = ep, ?e, "BatchReadBlobs: batch failed"),
                }
            }

            let mut fetched = HashSet::new();
            for (_is_server, completed) in &source_completions {
                fetched.extend(completed.iter().copied());
            }

            // (#sbrace) Attribute the small-blob race outcome onto the
            // WorkerProxyStore counters (the registered metrics home): each
            // no-peer digest -> no_peer, each raced digest -> the FIRST source
            // (peers-first) that returned it (peer_win, else server_won). Pure
            // observability; does not affect `fetched` or control flow.
            let ordered_results: Vec<(bool, &[DigestInfo])> = source_completions
                .iter()
                .map(|(is_server, completed)| (*is_server, completed.as_slice()))
                .collect();
            proxy.record_batch_read_race_outcome(&no_peer_digests, &ordered_results);

            // Retry misses via populate_fast_store_unchecked (full store chain).
            let misses: Vec<DigestInfo> = small_digests
                .iter()
                .filter(|d| !fetched.contains(d))
                .copied()
                .collect();

            if !misses.is_empty() {
                info!(count = misses.len(), "BatchReadBlobs: fetching misses via store chain");
                let retry_results = futures::future::join_all(
                    misses.iter().map(|&digest| async move {
                        let result = cas_store
                            .populate_fast_store_unchecked(digest.into())
                            .await;
                        (digest, result)
                    }),
                )
                .await;
                let mut retry_failures = 0u32;
                for (digest, result) in retry_results {
                    match result {
                        Ok(()) => { fetched.insert(digest); }
                        Err(e) => {
                            retry_failures += 1;
                            info!(?digest, ?e, "BatchReadBlobs: retry fetch failed");
                        }
                    }
                }
                if retry_failures > 0 {
                    info!(retry_failures, "BatchReadBlobs: some retries failed");
                }
            }

            return Ok(fetched);
        }

        // (#sbrace) peer_blob_count == 0: no digest had ANY peer, so every
        // small digest is a no_peer outcome. Record it here before falling
        // through to the server-only path below (the racing block above
        // returns, so reaching this point means the map was empty for all).
        proxy.record_batch_read_race_outcome(&no_peer_digests, &[]);
    }

    // No peers available — server-only batch read.
    let grpc_store = match slow_store.downcast_ref::<GrpcStore>(None) {
        Some(store) => store,
        None => return Ok(HashSet::new()),
    };

    let batches = partition_into_batches(small_digests);
    let fetched: HashSet<DigestInfo> = futures::stream::iter(batches.into_iter())
        .map(|batch| async move { execute_batch_read(grpc_store, cas_store, &batch).await })
        .buffer_unordered(BATCH_READ_CONCURRENCY)
        .try_fold(HashSet::new(), |mut acc, completed| async move {
            acc.extend(completed);
            Ok(acc)
        })
        .await?;

    Ok(fetched)
}

/// Partition digests into 4 MiB batches for BatchReadBlobs.
fn partition_into_batches(digests: &[DigestInfo]) -> Vec<Vec<DigestInfo>> {
    let mut batches: Vec<Vec<DigestInfo>> = Vec::new();
    let mut current_batch: Vec<DigestInfo> = Vec::new();
    let mut current_size: u64 = 0;

    for &digest in digests {
        let blob_size = digest.size_bytes();
        if !current_batch.is_empty() && current_size + blob_size > BATCH_READ_MAX_REQUEST_SIZE {
            batches.push(std::mem::take(&mut current_batch));
            current_size = 0;
        }
        current_batch.push(digest);
        current_size += blob_size;
    }
    if !current_batch.is_empty() {
        batches.push(current_batch);
    }
    batches
}

/// FL-681 Fix B: classification of a deferred output-blob upload error.
///
/// A deferred upload is the AUTHORITATIVE durability path for a
/// worker-produced output (the blob is single-copy on the worker until
/// the server has it). Therefore a give-up is permanent data loss and a
/// dangling AC entry. The invariant: a deferred upload retries until it
/// succeeds; it NEVER gives up for any error that COULD succeed on a
/// later attempt. Only a genuinely, permanently impossible request (a
/// malformed or forbidden upload of the SAME bytes) is exempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadRetryDecision {
    /// The slow tier already has the blob (`AlreadyExists`) — success.
    AlreadyDurable,
    /// Retryable — retry FOREVER with capped backoff. Covers transient
    /// server/network/backpressure classes AND everything not explicitly
    /// exempted (default-retry: the cost of a needless retry is one
    /// existence RPC; the cost of a wrong give-up is permanent loss).
    Retry,
    /// Genuinely, permanently impossible to ever succeed by retrying the
    /// same bytes. Documented exemptions ONLY (malformed/forbidden
    /// request). A give-up here is correct — retrying cannot help.
    PermanentGiveUp,
}

/// Classify a deferred-upload error for the retry loop. Default is
/// `Retry` (retry forever) — only the documented permanent-request
/// classes give up.
///
/// Permanent (give-up) classes — a give-up is correct because retrying
/// the same bytes can never succeed:
/// - `InvalidArgument`: malformed request (e.g. digest/size mismatch).
/// - `PermissionDenied` / `Unauthenticated`: the worker is not allowed to
///   write; retrying the same credentials/bytes cannot succeed (a config
///   error, surfaced loudly, not data the retry could rescue).
/// - `Unimplemented`: the slow store does not implement the write RPC;
///   retrying cannot make it implemented.
///
/// Everything else — including `Aborted` (our own backpressure signal),
/// `ResourceExhausted`, `Unavailable`, `DeadlineExceeded`, `Internal`,
/// `Unknown`, `Cancelled`, `NotFound` (eviction-race re-read; self-heals),
/// `DataLoss`, `FailedPrecondition` — CAN succeed on a later attempt
/// (server restart window, network blip, transient backpressure) and so
/// retries forever.
pub(crate) fn classify_upload_error(err: &Error) -> UploadRetryDecision {
    match err.code {
        Code::AlreadyExists => UploadRetryDecision::AlreadyDurable,
        Code::InvalidArgument
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::Unimplemented => UploadRetryDecision::PermanentGiveUp,
        _ => UploadRetryDecision::Retry,
    }
}

/// FL-681 Q1: which side of a deferred upload attempt failed, used to
/// size the inter-attempt backoff.
///
/// The retry loop reads the output blob from the worker's OWN fast store
/// and writes it to the REMOTE slow store. These two sides fail for
/// categorically different reasons and want categorically different
/// backoffs:
///
/// - [`UploadFailureSide::ReadLocal`] — the re-read from the worker's own
///   fast store failed (an eviction race between completion and this
///   background task: `cas_store.get*` returns `NotFound`, or the streaming
///   read drops the channel without commit → the synthesized
///   `Internal "buf_channel: writer dropped without commit"`). This is a
///   same-host disk/index race that self-heals on the very next read once
///   the source is re-pinned (Q2). A second-scale backoff here is pure
///   wasted latency — the prod symptom was ~15 s of backoff per output
///   blob (`INITIAL_BACKOFF`=1s ×2 ×4) before a re-read that would have
///   succeeded in milliseconds.
/// - [`UploadFailureSide::RemoteWrite`] — the gRPC write to the remote slow
///   store failed (`Unavailable`/`DeadlineExceeded`/`ResourceExhausted`,
///   server restart window, network blip). Recovery is genuinely
///   second-scale (server has to come back / backpressure has to drain), so
///   the existing 1 s→cap ramp is correct here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadFailureSide {
    /// Re-read from the worker's own fast store failed (eviction race).
    ReadLocal,
    /// Write to the remote slow store failed (genuine remote transient).
    RemoteWrite,
}

/// Worker-local read-side backoff floor (Q1). A same-host fast-store
/// re-read that lost an eviction race self-heals on the next read once the
/// source is re-pinned (Q2), so the only thing the backoff must do is yield
/// to the eviction sweep + re-pin — tens of milliseconds, not seconds. The
/// jitter spreads concurrent re-reads so a burst of evicted outputs does not
/// re-read in lockstep.
const READ_LOCAL_BACKOFF_MIN: Duration = Duration::from_millis(50);
const READ_LOCAL_BACKOFF_MAX: Duration = Duration::from_millis(100);

/// FL-681 Q1: compute the next sleep before a retry, by failure side.
///
/// `ReadLocal` returns a short jittered floor in
/// `[READ_LOCAL_BACKOFF_MIN, READ_LOCAL_BACKOFF_MAX]` — independent of
/// `remote_backoff` (the read race does not need the remote ramp). The
/// `jitter` argument is a single byte mapped uniformly across the span; the
/// production caller derives it from the digest hash XOR the attempt so a
/// burst of distinct evicted outputs spreads its re-reads instead of
/// re-reading in lockstep (no RNG dependency, deterministic in tests).
///
/// `RemoteWrite` returns the caller's current `remote_backoff` unchanged —
/// the existing 1 s→`MAX_BACKOFF` ramp owned by the loop. This keeps the
/// remote ramp's state (doubling) in the loop and leaves it untouched, so a
/// sustained remote outage still backs off at the slow remote interval.
fn next_retry_backoff(
    side: UploadFailureSide,
    remote_backoff: Duration,
    jitter: u8,
) -> Duration {
    match side {
        UploadFailureSide::ReadLocal => {
            let span = READ_LOCAL_BACKOFF_MAX.saturating_sub(READ_LOCAL_BACKOFF_MIN);
            // Map jitter byte 0..=255 across the span (inclusive at both
            // ends): jitter * span / 255.
            let extra = span
                .checked_mul(u32::from(jitter))
                .map_or(span, |scaled| scaled / 255);
            READ_LOCAL_BACKOFF_MIN + extra
        }
        UploadFailureSide::RemoteWrite => remote_backoff,
    }
}

/// FL-681 Q2: how the retry loop must re-assert the source pin before its
/// next read attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepinMode {
    /// Do not re-pin (the source was not lost — e.g. a remote-write failure,
    /// or a give-up).
    None,
    /// Re-pin via the indefinite-until-BIS path (deferred mode). MUST be the
    /// same path FL-681 Fix A added (`pin_digest_indefinite_with_result`) so
    /// the re-pin is released only by the BIS-ack, never by a fresh 120s TTL.
    Indefinite,
    /// Re-pin via the time-bounded path (synchronous mode), matching the
    /// schedule-time pin whose 120s-TTL→`failed_slow_writes` backstop is live.
    TimeBounded,
}

/// FL-681 Q1+Q2: the action the retry loop takes after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryStep {
    /// `true` = stop retrying (permanent give-up, or synchronous-mode budget
    /// exhausted). `false` = retry after re-pinning + sleeping.
    pub give_up: bool,
    /// How to re-assert the source pin before the next read (Q2). Only a
    /// worker-local read race needs a re-pin; a remote-write failure did not
    /// lose the source.
    pub repin: RepinMode,
}

/// FL-681 Q1+Q2: decide what the retry loop does after one failed attempt,
/// given the error's give-up classification, which side failed, the pin
/// mode, and the attempt counter.
///
/// This is the loop's whole retry-control decision, factored out so the two
/// load-bearing contracts are unit-testable without standing up a worker:
///
/// - **Re-pin contract (Q2):** a retryable worker-local read race
///   (`ReadLocal`) re-asserts the source pin via the SAME mode the
///   schedule-time pin used (`Indefinite` in deferred mode, `TimeBounded`
///   in synchronous mode) BEFORE the next read — otherwise the retry
///   re-runs the same eviction race. A `RemoteWrite` failure never re-pins
///   (the source was not lost).
/// - **Give-up contract (Fix B):** retryable classes retry FOREVER in
///   deferred mode (`retry_forever == true`); in synchronous mode they keep
///   the prior finite `SYNC_MAX_RETRIES` bound. Permanent-request classes
///   give up regardless of mode.
///
/// `already_durable` (the classifier's `AlreadyDurable` parallel verdict)
/// is treated as success-stop with no re-pin.
const fn plan_retry_step(
    decision: UploadRetryDecision,
    side: UploadFailureSide,
    deferred_pin: bool,
    attempt: u32,
    sync_max_retries: u32,
) -> RetryStep {
    match decision {
        UploadRetryDecision::AlreadyDurable | UploadRetryDecision::PermanentGiveUp => {
            RetryStep { give_up: true, repin: RepinMode::None }
        }
        UploadRetryDecision::Retry => {
            // Synchronous mode keeps the prior finite give-up bound.
            if !deferred_pin && attempt >= sync_max_retries {
                return RetryStep { give_up: true, repin: RepinMode::None };
            }
            let repin = match side {
                UploadFailureSide::ReadLocal => {
                    if deferred_pin {
                        RepinMode::Indefinite
                    } else {
                        RepinMode::TimeBounded
                    }
                }
                UploadFailureSide::RemoteWrite => RepinMode::None,
            };
            RetryStep { give_up: false, repin }
        }
    }
}

/// #FL-688 W6: decide whether a give-up should re-queue the digest into
/// `failed_slow_writes` for retry-until-durable. The give-up arm of
/// `spawn_upload_to_remote` re-queues a digest ONLY when the loop is
/// stopping (`give_up_flag == false`, the loop's terminal failure value)
/// AND the error CAN succeed on a later attempt
/// (`classify_upload_error == Retry`, i.e. the SYNC-mode finite budget was
/// exhausted on a transient error). A `PermanentGiveUp` (InvalidArgument,
/// PermissionDenied, …) is NOT re-queued — it can never succeed by retry,
/// so a re-queue would be a dead-weight reconnect retry. `AlreadyExists`
/// (success) is handled by the loop's explicit success arm and never
/// reaches the give-up arm. Deferred mode never gives up on a retryable
/// class (retry-forever), so this returns `true` only on the production
/// synchronous path.
///
/// Single source of truth so the production give-up arm and the W6
/// regression test exercise the SAME predicate (a mutation that guts this
/// function red-fails the test).
fn should_requeue_on_giveup(e: &Error, give_up_flag: bool) -> bool {
    !give_up_flag && classify_upload_error(e) == UploadRetryDecision::Retry
}

/// (FL-688 v3 Stage B) Take the anti-eviction pin for ONE output digest on the
/// upload path, per mode. Single source of truth for the four deferred-mode
/// durability pin sites in `spawn_upload_to_remote_impl` + the schedule-time
/// `pin_one` closure (a mutation of this body red-fails the Stage B contract
/// test).
///
/// - **Deferred (F2 durability) mode** — pin INDEFINITELY (held until the
///   server's BlobsInStableStorage ack, exempt from the `PIN_TIMEOUT_SECS` TTL
///   sweep). There is NO time-bounded fallback: in deferred mode the slow-store
///   write is the AUTHORITATIVE upload and bypasses `FastSlowStore::update`, so
///   the digest never enters `in_flight_slow_writes` and the
///   `on_pin_expired`→`failed_slow_writes` retry path is dark — a time-bounded
///   pin would be DEMOTED at the 120s TTL and silently lost. On indefinite
///   cap-refusal the blob is left FULLY evictable (the accepted saturated-cap
///   loss class; producer backpressure is the worker's
///   `indefinite_pin_saturated()` NAK, tracked as a separate FL-681 admission-
///   gating follow-up). Returns `false` on cap-refusal OR eviction race so the
///   caller can warn.
/// - **Synchronous mode** — time-bounded `pin_digest_with_result`: the
///   schedule-time pin whose 120s-TTL→`failed_slow_writes` backstop is LIVE.
///
/// This is a NON-BLOCKING synchronous call (no await, no lock held across an
/// await, no channel send) — converting the deferred-mode fallback from
/// time-bounded to indefinite changes ONLY the eviction-exemption of the pinned
/// entry. It cannot deadlock.
fn pin_deferred_output_digest(
    filesystem_store: &FilesystemStore,
    deferred: bool,
    digest: &DigestInfo,
) -> bool {
    if deferred {
        filesystem_store.pin_digest_indefinite_with_result(digest)
    } else {
        filesystem_store.pin_digest_with_result(digest)
    }
}

/// FL-681 Q1+Q2: per-digest retry control for the deferred upload loop.
///
/// Owns the `attempt` counter and the `remote_backoff` ramp so the loop
/// body stays a thin caller: it computes one attempt result and hands a
/// failure here, which decides give-up vs retry, executes the planned
/// re-pin (Q2) via the injected `repin_fn`, sleeps the side-sized backoff
/// (Q1), and advances the remote ramp only on a remote-write failure.
///
/// The `repin_fn` injection is the test seam: production passes a closure
/// that calls `FilesystemStore::pin_digest_indefinite_with_result` /
/// `pin_digest`; tests pass a recording closure so the re-pin contract (mode
/// + that it fires BEFORE the next read) is asserted deterministically, and
/// the backoff timing is measured under `tokio::time::pause`.
struct DeferredUploadRetry {
    attempt: u32,
    /// The remote-transient ramp (1 s → `max_backoff`). Advanced ONLY on a
    /// `RemoteWrite` failure so an interleaved read race never inflates it.
    remote_backoff: Duration,
}

impl DeferredUploadRetry {
    const fn new(initial_remote_backoff: Duration) -> Self {
        Self { attempt: 0, remote_backoff: initial_remote_backoff }
    }

    /// Handle one failed attempt. Returns `Some(success_flag)` to STOP the
    /// loop (the bool is the loop's terminal `break` value: `false` =
    /// give-up, never `true` here) or `None` to CONTINUE retrying (after
    /// this call has already re-pinned + slept).
    ///
    /// `repin_fn` executes the planned re-pin; it is called BEFORE the sleep
    /// (and therefore before the next read), which is the Q2 contract.
    // The parameters are the loop's per-digest invariants (error, side,
    // digest, pin mode, the two bounds) plus the re-pin injection seam; each
    // is load-bearing and grouping them into a struct would only move the
    // argument list without reducing it.
    #[allow(clippy::too_many_arguments)]
    async fn after_failure(
        &mut self,
        e: &Error,
        side: UploadFailureSide,
        digest: DigestInfo,
        deferred_pin: bool,
        sync_max_retries: u32,
        max_backoff: Duration,
        mut repin_fn: impl FnMut(RepinMode),
    ) -> Option<bool> {
        let decision = classify_upload_error(e);
        let step = plan_retry_step(decision, side, deferred_pin, self.attempt, sync_max_retries);
        if step.give_up {
            match decision {
                // Defensive: AlreadyExists is handled by the explicit
                // success arm in the loop; the classifier's parallel verdict
                // is unreachable here. Treat as success-stop.
                UploadRetryDecision::AlreadyDurable => return Some(true),
                UploadRetryDecision::PermanentGiveUp => {
                    error!(
                        ?digest,
                        ?e,
                        code = ?e.code,
                        attempts = self.attempt + 1,
                        "upload_to_remote: permanent request error uploading digest, cannot succeed by retry (FL-681 Fix B documented exemption)",
                    );
                    return Some(false);
                }
                UploadRetryDecision::Retry => {
                    // Synchronous-mode finite budget exhausted. This function
                    // is the pure retry-policy decision point and takes NO
                    // side effect on the store; it returns `Some(false)` to
                    // STOP the loop. The caller (the give-up arm in
                    // `spawn_upload_to_remote`) is responsible for arming the
                    // `failed_slow_writes` backstop on this exact decision
                    // (`#FL-688 W6` — it re-derives `classify_upload_error`
                    // and calls `requeue_failed_push`). Pre-#FL-688 this loop
                    // wrote to the bare `slow_store` and recorded NOTHING on
                    // give-up; the "deferring to the backstop" claim was a
                    // load-bearing falsehood (the backstop was armed only by
                    // the EARLIER `inner_upload_results` FSS write, not by
                    // this loop). (Deferred mode never reaches here:
                    // retry_forever.)
                    error!(
                        ?digest,
                        ?e,
                        code = ?e.code,
                        attempts = self.attempt + 1,
                        "upload_to_remote: synchronous-mode retry budget exhausted; caller arms failed_slow_writes backstop for retry-until-durable (#FL-688 W6)",
                    );
                    return Some(false);
                }
            }
        }
        // Retryable: FL-681 Fix B retries FOREVER in deferred mode (the
        // source stays readable via the indefinite pin / the re-pin below),
        // so a later attempt succeeds once the server/network recovers.
        self.attempt += 1;
        // FL-681 Q2: re-assert the source pin BEFORE the next re-read on a
        // worker-local read race. The pin was taken once at schedule time,
        // but the retry re-reads via `cas_store.get*` — without re-pinning,
        // the retry re-runs the SAME eviction race. The planner chose the
        // mode: `Indefinite` reuses the indefinite-until-BIS path FL-681 Fix
        // A added (released only by the BIS-ack unpin, never by a fresh 120s
        // TTL); `TimeBounded` matches the synchronous schedule-time pin;
        // `RemoteWrite` failures plan `None` (the source was not lost).
        repin_fn(step.repin);
        // FL-681 Q1: size the sleep by failure side. A worker-local read
        // race (ReadLocal) self-heals on the next read once re-pinned, so it
        // waits only a short jittered floor (~50-100 ms) — the prod tail was
        // ~15 s of pure 1 s-ramp backoff per evicted output. A genuine remote
        // transient (RemoteWrite) keeps the existing 1 s→max ramp.
        // Jitter source: digest's first hash byte XOR the attempt's low byte
        // (`to_le_bytes()[0]` is an explicit non-truncating low-byte take).
        let jitter = digest.packed_hash()[0] ^ self.attempt.to_le_bytes()[0];
        let sleep_for = next_retry_backoff(side, self.remote_backoff, jitter);
        // Rate-limited stuck-upload visibility: warn at the threshold and
        // then once per STUCK_WARN_EVERY attempts — but KEEP RETRYING.
        if self.attempt == STUCK_WARN_THRESHOLD
            || (self.attempt > STUCK_WARN_THRESHOLD
                && (self.attempt - STUCK_WARN_THRESHOLD).is_multiple_of(STUCK_WARN_EVERY))
        {
            warn!(
                ?digest,
                ?e,
                code = ?e.code,
                attempt = self.attempt,
                ?side,
                backoff_ms = sleep_for.as_millis() as u64,
                "upload_to_remote: deferred upload stuck (retrying indefinitely until durable — FL-681 Fix B; source stays pinned via Fix A)",
            );
        } else {
            debug!(
                ?digest,
                ?e,
                code = ?e.code,
                attempt = self.attempt,
                ?side,
                backoff_ms = sleep_for.as_millis() as u64,
                "upload_to_remote: retrying failed upload",
            );
        }
        tokio::time::sleep(sleep_for).await;
        // Only the remote ramp advances; a read race must not push the
        // remote ramp up (it would punish a later genuine remote transient
        // with an inflated first wait).
        if side == UploadFailureSide::RemoteWrite {
            self.remote_backoff = min(self.remote_backoff * 2, max_backoff);
        }
        None
    }
}

/// Per-attempt threshold past which a stuck deferred upload becomes
/// operator-visible via a rate-limited `warn!` (then once per
/// [`STUCK_WARN_EVERY`] attempts). Module-level so [`DeferredUploadRetry`]
/// and the loop share one definition.
const STUCK_WARN_THRESHOLD: u32 = 5;
/// Cadence of the stuck-upload `warn!` past [`STUCK_WARN_THRESHOLD`].
const STUCK_WARN_EVERY: u32 = 20;

/// Validate `BatchReadBlobsResponse.responses` and return only entries
/// whose `data.len() == digest.size_bytes()` and whose `status.code` is OK.
///
/// The worker fast store is a `FilesystemStore` with NO `VerifyStore` in
/// front of it (server-side verification is the trust boundary for
/// content-addressed reads — verifying again on the worker is wasted CPU).
/// That makes this batch-read parser the worker's trust boundary: any
/// response whose payload length disagrees with the advertised digest
/// length is corruption (truncation or padding) and MUST be dropped, not
/// committed under a wrong `ExactSize` (which would silently propagate
/// corrupt-length blobs through subsequent reads that trust the cached
/// length).
///
/// Bug shape pre-fix: `data_len = data.len() as u64` was passed to
/// `UploadSizeInfo::ExactSize(data_len)`, so a truncated response was
/// committed under the correct hash key but with a wrong recorded length.
/// Subsequent reads through `FilesystemStore` returned the truncated
/// payload and trusted its length, producing data corruption with no signal.
///
/// Dropped digests fall into the retry path and are fetched again from
/// the server store chain (which has its own `VerifyStore`).
pub fn validate_batch_read_responses(
    responses: Vec<batch_read_blobs_response::Response>,
) -> Vec<(DigestInfo, Bytes)> {
    responses
        .into_iter()
        .filter_map(|blob_resp| {
            let status_code = blob_resp.status.as_ref().map_or(0, |s| s.code);
            if status_code != 0 {
                return None;
            }
            let proto_digest = blob_resp.digest?;
            let digest = DigestInfo::try_from(proto_digest).ok()?;
            let advertised = digest.size_bytes();
            let data_len = blob_resp.data.len() as u64;
            if data_len != advertised {
                warn!(
                    ?digest,
                    advertised,
                    actual = data_len,
                    "execute_batch_read: dropping response with mismatched length \
                     (peer returned wrong number of bytes for advertised digest)"
                );
                return None;
            }
            Some((digest, Bytes::from(blob_resp.data)))
        })
        .collect()
}

/// Execute a single BatchReadBlobs request and write results to fast store.
async fn execute_batch_read(
    grpc_store: &GrpcStore,
    cas_store: &FastSlowStore,
    digests: &[DigestInfo],
) -> Result<Vec<DigestInfo>, Error> {
    let request = BatchReadBlobsRequest {
        instance_name: String::new(), // GrpcStore fills this in
        digests: digests.iter().map(|d| (*d).into()).collect(),
        acceptable_compressors: vec![],
        digest_function: Context::current()
            .get::<DigestHasherFunc>()
            .map_or_else(default_digest_hasher_func, |v| *v)
            .proto_digest_func()
            .into(),
    };

    let response = grpc_store
        .batch_read_blobs(Request::new(request))
        .await
        .err_tip(|| "In execute_batch_read")?
        .into_inner();

    // Write directly to the fast store: these blobs were just fetched from
    // the slow (server) store via BatchReadBlobs, so routing the writeback
    // through the FastSlowStore wrapper would loop them back upstream.
    #[allow(clippy::disallowed_methods)]
    let fast_store = cas_store.fast_store();

    // Parse all valid responses first, then write to fast store concurrently.
    // Length-mismatched responses are dropped here; they fall into the retry
    // path and get fetched from the server store chain (which has VerifyStore).
    let valid_blobs: Vec<(DigestInfo, Bytes)> = validate_batch_read_responses(response.responses);

    // Write all blobs to fast store concurrently.
    let write_futures: FuturesUnordered<_> = valid_blobs
        .into_iter()
        .map(|(digest, data)| {
            let data_len = data.len() as u64;
            async move {
                let (mut tx, rx) = make_buf_channel_pair();
                let store_key: StoreKey<'_> = digest.into();
                let update_fut = fast_store.update(
                    store_key,
                    rx,
                    UploadSizeInfo::ExactSize(data_len),
                );
                let send_fut = async {
                    tx.send(data)
                        .await
                        .err_tip(|| "Sending batch blob to fast store")?;
                    tx.send_eof().err_tip(|| "Sending EOF for batch blob")?;
                    Ok::<_, Error>(())
                };
                let (update_res, send_res) = futures::join!(update_fut, send_fut);
                update_res
                    .merge(send_res)
                    .err_tip(|| format!("Writing batch-read blob {digest:?} to fast store"))?;
                Ok::<DigestInfo, Error>(digest)
            }
        })
        .collect();

    let completed: Vec<DigestInfo> = write_futures.try_collect().await?;

    Ok(completed)
}

/// Populate the fast store for a single digest and hardlink it to `dest`.
/// Contains the retry loop for cache eviction races.
async fn populate_and_hardlink(
    cas_store: &FastSlowStore,
    filesystem_store: Pin<&FilesystemStore>,
    digest: DigestInfo,
    dest: &str,
) -> Result<(), Error> {
    if is_zero_digest(digest) {
        cas_store.populate_fast_store(digest.into()).await?;
        let mut file_slot = fs::create_file(dest)
            .await
            .err_tip(|| format!("Could not create zero-digest file at {dest}"))?;
        std::io::Write::write_all(file_slot.as_std_mut(), &[])
            .err_tip(|| format!("Could not write zero-digest file at {dest}"))?;
        return Ok(());
    }

    const MAX_RETRIES: u32 = 3;
    let mut last_err = None;
    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            filesystem_store.remove_entry_for_digest(&digest).await;
        }
        cas_store.populate_fast_store(digest.into()).await?;

        let result = async {
            let file_entry = filesystem_store
                .get_file_entry_for_digest(&digest)
                .await
                .err_tip(|| "Getting file entry for hardlink")?;
            let dest_clone = dest.to_string();
            file_entry
                .get_file_path_locked(move |src| async move {
                    let src_exists = Path::new(&src).exists();
                    let result = fs::hard_link(&src, &dest_clone).await;
                    if result.is_err() {
                        warn!(
                            src = %src.to_string_lossy(),
                            src_exists = src_exists,
                            dest = %dest_clone,
                            "hard_link failed while holding read lock"
                        );
                    }
                    result
                })
                .await
        }
        .await;

        match result {
            Ok(()) => {
                last_err = None;
                break;
            }
            Err(e) if e.code == Code::NotFound => {
                warn!(
                    attempt = attempt + 1,
                    max_retries = MAX_RETRIES,
                    ?digest,
                    dest = %dest,
                    err = ?e,
                    "File evicted from cache during hardlink. Retrying."
                );
                last_err = Some(e);
            }
            Err(e) => {
                return Err(make_err!(
                    Code::Internal,
                    "Could not make hardlink, {e:?} : {dest}"
                ));
            }
        }
    }
    if let Some(e) = last_err {
        return Err(make_err!(
            Code::Internal,
            "Could not make hardlink after {MAX_RETRIES} attempts, \
            file was repeatedly evicted from cache. {e:?} : {dest}\n\
            This error often occurs when the filesystem store's max_bytes is too small for your workload.\n\
            To fix this issue:\n\
            1. Increase the 'max_bytes' value in your filesystem store configuration\n\
            2. Example: Change 'max_bytes: 10000000000' to 'max_bytes: 50000000000' (or higher)\n\
            3. The setting is typically found in your nativelink.json config under:\n\
            stores -> [your_filesystem_store] -> filesystem -> eviction_policy -> max_bytes\n\
            4. Restart NativeLink after making the change\n\n\
            If this error persists after increasing max_bytes several times, please report at:\n\
            https://github.com/TraceMachina/nativelink/issues\n\
            Include your config file and both server and client logs to help us assist you."
        ));
    }
    Ok(())
}

/// Like `hardlink_and_set_metadata` but uses a pre-fetched file entry
/// (from batch `get_file_entries_batch`) to avoid per-file EvictingMap lock
/// contention. Falls back to the regular path on cache miss.
async fn hardlink_and_set_metadata_prefetched(
    cas_store: &FastSlowStore,
    filesystem_store: Pin<&FilesystemStore>,
    file: FileToMaterialize,
    prefetched_entry: Option<Arc<nativelink_store::filesystem_store::FileEntryImpl>>,
) -> Result<(), Error> {
    let digest = file.digest;
    let dest = file.dest.clone();

    if let Some(file_entry) = prefetched_entry {
        // We have a pre-fetched entry — try hardlink directly.
        let dest_clone = dest.clone();
        let result = file_entry
            .get_file_path_locked(move |src| async move {
                fs::hard_link(&src, &dest_clone).await
            })
            .await;

        match result {
            Ok(()) => {
                // Success — apply permissions and mtime, then return.
            }
            Err(e) if e.code == Code::NotFound => {
                // File was evicted between pre-fetch and hardlink.
                // Fall back to full populate+hardlink.
                populate_and_hardlink(cas_store, filesystem_store, digest, &dest).await?;
            }
            Err(e) => {
                return Err(make_err!(
                    Code::Internal,
                    "Could not make hardlink (prefetched), {e:?} : {dest}"
                ));
            }
        }
    } else {
        // No pre-fetched entry (cache miss or zero digest).
        populate_and_hardlink(cas_store, filesystem_store, digest, &dest).await?;
    }

    // Always set permissions — CAS files default to 0o555 but concurrent
    // hardlinks from other actions can change the shared inode's mode.
    // We must unconditionally chmod to ensure correctness.
    #[cfg(target_family = "unix")]
    if let Some(unix_mode) = file.unix_mode {
        fs::set_permissions(&dest, Permissions::from_mode(unix_mode))
            .await
            .err_tip(|| format!("Could not set unix mode in download_to_directory {dest}"))?;
    }

    // Apply mtime.
    if let Some(mtime) = file.mtime {
        let dest_owned = dest.clone();
        // TODO(#speculative-prefetch-io-priority): disk-materialize spawn_blocking
        // site in the construct's download path. A future IO-priority scheduler
        // would read the driving OpPriority here (once threaded through
        // download_to_directory) and route Speculative disk writes/hardlinks to a
        // lower-priority blocking pool so they never contend with a Foreground
        // action's disk I/O. Marker-first in Increment 1 (build-spec §Revision-2).
        spawn_blocking!("download_to_directory_set_mtime", move || {
            set_file_mtime(
                &dest_owned,
                FileTime::from_unix_time(mtime.seconds, mtime.nanos as u32),
            )
            .err_tip(|| format!("Failed to set mtime in download_to_directory {dest_owned}"))
        })
        .await
        .err_tip(|| "Failed to launch spawn_blocking in download_to_directory")??;
    }

    Ok(())
}

/// Aggressively download the digests of files and make a local folder from it.
///
/// This optimized version:
/// 1. Resolves the full directory tree via `GetTree` RPC (single streaming call)
///    instead of issuing recursive individual `get_and_decode_digest` calls.
/// 2. Batch-checks which blobs are already in the fast store via `has_with_results`
///    (maps to `FindMissingBlobs` on GrpcStore), avoiding per-file existence RPCs.
/// 3. Fetches small missing blobs (<1 MiB) via `BatchReadBlobs` in 4 MiB batches,
///    with large blobs using the existing ByteStream path.
///
/// We require the `FilesystemStore` to be the `fast` store of `FastSlowStore`.
/// We will request the `FastSlowStore` to populate the entry then we will
/// assume the `FilesystemStore` has the file available immediately after and hardlink the file
/// to a new location.
pub fn download_to_directory<'a>(
    cas_store: &'a FastSlowStore,
    filesystem_store: Pin<&'a FilesystemStore>,
    digest: &'a DigestInfo,
    current_directory: &'a str,
    pre_resolved_tree: Option<HashMap<DigestInfo, ProtoDirectory>>,
    server_missing_digests: Option<HashSet<DigestInfo>>,
    // Calibration probe P-A sink (observability-only): when `Some`, the total
    // resolved input-tree bytes this call staged are written here for the
    // caller (`inner_prepare_action`) to carry into the post-upload P-A record.
    // `None` for callers that don't feed P-A (directory-cache construct, tests).
    // Written exactly once before this future resolves Ok; on the early
    // directory-only return it is set to 0 (no input bytes staged).
    calib_input_bytes_out: Option<&'a core::sync::atomic::AtomicU64>,
) -> BoxFuture<'a, Result<(), Error>> {
    async move {
        let phase_start = std::time::Instant::now();

        // Step 1: Resolve the full directory tree. Use pre-resolved tree
        // from the scheduler if available, otherwise fall back to GetTree RPC.
        let (tree, tree_resolve_ms) = if let Some(tree) = pre_resolved_tree {
            info!(
                root = ?digest,
                dirs = tree.len(),
                "download_to_directory: using pre-resolved tree from scheduler (skipping GetTree RPC)"
            );
            (tree, 0u128)
        } else {
            let tree = resolve_directory_tree(cas_store, digest).await?;
            let ms = phase_start.elapsed().as_millis();
            (tree, ms)
        };

        // Step 2: Walk the tree, creating all directories and collecting files.
        let (files, symlinks) = collect_files_from_tree(&tree, digest, current_directory)?;

        info!(
            root = ?digest,
            total_dirs = tree.len(),
            total_files = files.len(),
            total_symlinks = symlinks.len(),
            "download_to_directory: starting materialization",
        );

        // Create all subdirectories using level-parallel BFS — siblings at
        // the same depth are created concurrently while parent-before-child
        // ordering is maintained (each level completes before the next starts).
        let mkdir_start = std::time::Instant::now();
        let mut dirs_created: usize = 0;
        let mut mkdir_depth: u32 = 0;
        {
            let mut current_level = vec![(*digest, current_directory.to_string())];
            while !current_level.is_empty() {
                let mut next_level = Vec::new();
                for (dir_digest, dir_path) in &current_level {
                    // Tree completeness is asserted by resolve_directory_tree;
                    // a missing entry here would silently skip the directory
                    // and everything below it. Fail loud instead.
                    let directory = tree.get(dir_digest).ok_or_else(|| {
                        make_err!(
                            Code::Internal,
                            "download_to_directory: directory {dir_digest:?} missing from resolved tree at depth {mkdir_depth} (path {dir_path}); refusing to materialize incomplete input tree",
                        )
                    })?;
                    debug!(
                        depth = mkdir_depth,
                        path = %dir_path,
                        files = directory.files.len(),
                        subdirs = directory.directories.len(),
                        "download_to_directory: processing directory",
                    );
                    for subdir in &directory.directories {
                        let child_digest: DigestInfo = subdir
                            .digest
                            .as_ref()
                            .err_tip(|| "Expected Digest")?
                            .try_into()
                            .err_tip(|| "In Directory::directories::digest")?;
                        let child_path = format!("{}/{}", dir_path, subdir.name);
                        next_level.push((child_digest, child_path));
                    }
                }
                if !next_level.is_empty() {
                    dirs_created += next_level.len();
                    try_join_all(next_level.iter().map(|(_, path)| {
                        let path = path.clone();
                        async move {
                            // O5 prereq: tolerate AlreadyExists when the existing
                            // entry is a directory (pre-created by a concurrent
                            // prepare_output_directory [C]). An existing *file* at
                            // a directory path is still a genuine conflict.
                            match fs::create_dir(&path).await {
                                Ok(()) => Ok(()),
                                Err(e) if e.code == Code::AlreadyExists => {
                                    let m = fs::metadata(&path).await.err_tip(|| {
                                        format!("Could not create directory {path}: already exists but could not stat")
                                    })?;
                                    if m.is_dir() {
                                        Ok(())
                                    } else {
                                        Err(make_err!(
                                            Code::AlreadyExists,
                                            "Could not create directory {path}: a non-directory entry already exists"
                                        ))
                                    }
                                }
                                Err(e) => {
                                    Err(e).err_tip(|| format!("Could not create directory {path}"))
                                }
                            }
                        }
                    }))
                    .await?;
                }
                mkdir_depth += 1;
                current_level = next_level;
            }
        }
        let mkdir_elapsed = mkdir_start.elapsed();
        info!(
            dirs_created,
            mkdir_depth_levels = mkdir_depth,
            mkdir_ms = mkdir_elapsed.as_millis() as u64,
            "download_to_directory: directories created",
        );

        // Create symlinks concurrently.
        #[cfg(target_family = "unix")]
        {
            let symlink_futures: FuturesUnordered<_> = symlinks
                .iter()
                .map(|(target, dest)| async move {
                    fs::symlink(target, dest)
                        .await
                        .err_tip(|| format!("Could not create symlink {target} -> {dest}"))
                })
                .collect();
            symlink_futures
                .try_for_each(|()| futures::future::ready(Ok(())))
                .await?;
        }

        if files.is_empty() {
            info!(
                root = ?digest,
                "download_to_directory: no files to materialize (directory-only tree)",
            );
            // Calibration P-A sink: directory-only tree staged zero input bytes.
            if let Some(out) = calib_input_bytes_out {
                out.store(0, core::sync::atomic::Ordering::Relaxed);
            }
            return Ok(());
        }

        // Step 3: Determine which blobs are already cached and which are missing.
        // Deduplicate digests first to avoid redundant checks.
        let unique_digests: Vec<DigestInfo> = {
            let mut seen = HashSet::with_capacity(files.len());
            files
                .iter()
                .filter_map(|f| {
                    if seen.insert(f.digest) {
                        Some(f.digest)
                    } else {
                        None
                    }
                })
                .collect()
        };

        let has_check_start = std::time::Instant::now();

        // When the scheduler provides missing_digests hints (computed from
        // the locality map at dispatch time), trust those hints and skip the
        // expensive has_with_results round-trip to the fast store. This saves
        // 5-50ms per action. If the hints are stale (a blob was evicted
        // between dispatch and now), the fetch will repopulate it via the
        // normal FastSlowStore path.
        let (cached_set, missing_digests) = if let Some(ref server_missing) = server_missing_digests {
            let cached: HashSet<DigestInfo> = unique_digests
                .iter()
                .filter(|d| !server_missing.contains(d))
                .copied()
                .collect();
            let missing: Vec<DigestInfo> = unique_digests
                .iter()
                .filter(|d| server_missing.contains(d))
                .copied()
                .collect();
            info!(
                total_files = files.len(),
                unique_digests = unique_digests.len(),
                cached = cached.len(),
                missing = missing.len(),
                server_hints = server_missing.len(),
                "download_to_directory: using server-provided missing digest hints (skipping has_with_results)"
            );
            (cached, missing)
        } else {
            // No server hints — fall back to the full has_with_results check.
            let store_keys: Vec<StoreKey<'_>> =
                unique_digests.iter().map(|d| (*d).into()).collect();
            let mut has_results = vec![None; store_keys.len()];
            // Route via the FastSlowStore wrapper (NOT cas_store.fast_store()).
            // The wrapper's has_with_results also checks `mirror_blobs` so a
            // server-pushed mirror copy held in memory is treated as already
            // present and not re-downloaded. populate_and_hardlink will pull
            // the bytes onto disk on demand for the actual hardlink. Pre-fix
            // this asked only the FilesystemStore, so a mirror-only blob
            // would be re-fetched from the slow store unnecessarily — and on
            // a server restart with the only durable copy in mirror_blobs,
            // could appear missing entirely.
            let cas_store_arc = cas_store.get_arc().ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "cas_store weak ref upgrade failed during has_with_results"
                )
            })?;
            let cas_store_wrapped = Store::new(cas_store_arc);
            // Check in chunks to reduce Mutex hold time in the fast store,
            // allowing concurrent operations from other actions to interleave.
            const HAS_CHECK_CHUNK: usize = 2000;
            for start in (0..store_keys.len()).step_by(HAS_CHECK_CHUNK) {
                let end = (start + HAS_CHECK_CHUNK).min(store_keys.len());
                cas_store_wrapped
                    .has_with_results(&store_keys[start..end], &mut has_results[start..end])
                    .await
                    .err_tip(|| "Batch has_with_results on fast store")?;
            }

            let cached: HashSet<DigestInfo> = unique_digests
                .iter()
                .zip(has_results.iter())
                .filter_map(|(digest, result)| result.map(|_| *digest))
                .collect();

            let missing: Vec<DigestInfo> = unique_digests
                .iter()
                .zip(has_results.iter())
                .filter_map(|(digest, result)| if result.is_none() { Some(*digest) } else { None })
                .collect();

            (cached, missing)
        };

        let has_check_elapsed = has_check_start.elapsed();
        let has_check_ms = phase_start.elapsed().as_millis();

        let cached_bytes: u64 = cached_set.iter().map(|d| d.size_bytes()).sum();
        let missing_bytes: u64 = missing_digests.iter().map(|d| d.size_bytes()).sum();
        info!(
            total_files = files.len(),
            unique_digests = unique_digests.len(),
            cached = cached_set.len(),
            cached_bytes,
            missing = missing_digests.len(),
            missing_bytes,
            used_server_hints = server_missing_digests.is_some(),
            elapsed_ms = has_check_elapsed.as_millis() as u64,
            "download_to_directory: batch existence check complete"
        );

        // (#mapgap) OBSERVABILITY-ONLY false-missing probe, SPLIT BY SOURCE.
        // The gap being hunted: the scheduler flags input blobs "missing on
        // worker X" that X already holds (the routing blob_locality_map
        // under-reports holdings). When the SERVER supplied `missing_digests`
        // hints (the path above TRUSTS them and skips the per-digest
        // `has_with_results` for latency), we do NOT otherwise learn whether
        // those digests were truly absent — or, if present, from WHERE.
        // `probe_input_missing_sources` resolves each server-flagged-missing
        // digest 3 ways (mirroring `populate_fast_store`'s branch):
        //   - DISK: on the on-disk FilesystemStore. Prefetch pushes are
        //     NORMAL writes (hit disk + fire the tracker → reported), so a
        //     high disk-hit rate means the map is STALE about REPORTED disk
        //     holdings (lag/report bug), not the mirror gap.
        //   - MIRROR: in the in-memory `mirror_blobs` buffer (≥2-replica
        //     durability copy — skips disk + tracker → held-but-UNREPORTED).
        //     THE mirror gap; a high mirror-hit rate is the smoking gun.
        //   - FETCHED: neither → a genuine miss.
        // Counts render under the worker CAS FSS tree
        // (`nativelink_WORKER_FAST_SLOW_STORE_input_server_missing_hit_{disk,
        // mirror}_*` + `_fetched_*`) and pair with the server-side
        // `scheduler...locality_map.digest_count` gauge + the worker
        // `mirror_blobs_{digest_count,total_bytes}` gauges.
        //
        // Gated to the server-hints branch (the only case where the gap
        // exists) AND a non-empty missing set. Cost: one batched in-memory
        // fast-store has() + one `mirror_blobs` read over the missing set
        // (NO disk I/O) — dwarfed by the fetch it precedes; results feed
        // counters only, never routing (the fetch pipeline still uses
        // `missing_digests` unchanged). NO behavior change: no state mutated,
        // no fetch/hardlink decision altered; the redundant fetch of a
        // disk/mirror-hit digest by the fetcher's
        // `populate_fast_store_unchecked` (no has() short-circuit) is
        // OBSERVED here, not suppressed.
        if server_missing_digests.is_some() && !missing_digests.is_empty() {
            match cas_store.probe_input_missing_sources(&missing_digests).await {
                Ok(probe) => {
                    if probe.mirror_count > 0 || probe.disk_count > 0 {
                        warn!(
                            hit_disk_count = probe.disk_count,
                            hit_disk_bytes = probe.disk_bytes,
                            hit_mirror_count = probe.mirror_count,
                            hit_mirror_bytes = probe.mirror_bytes,
                            fetched_count = probe.fetched_count,
                            server_missing = missing_digests.len(),
                            "download_to_directory: #mapgap server flagged digests missing that the worker already held (false-missing; locality-map under-report). mirror_count = the held-but-unreported mirror gap"
                        );
                    }
                }
                Err(err) => {
                    // Observability probe must never fail the action.
                    warn!(
                        ?err,
                        "download_to_directory: #mapgap false-missing probe failed; skipping count for this action"
                    );
                }
            }
        }

        // Steps 4+5 (pipelined): Three concurrent futures:
        //
        //   Fetcher: launches ALL missing blob fetches at once with bounded
        //     concurrency. As each blob arrives it is inserted into a
        //     `fetched_set` so the producer knows it is ready.
        //
        //   Producer: iterates files in batches. Files whose blobs are already
        //     cached go to the channel immediately. Files whose blobs are
        //     still being fetched are deferred and retried after a short
        //     yield. This means hardlinking starts right away for cached
        //     files while fetches proceed in parallel.
        //
        //   Consumer: reads from the channel, hardlinks with bounded
        //     concurrency (unchanged from before).
        //
        const HARDLINK_CONCURRENCY: usize = 64;
        const HARDLINK_BATCH: usize = 64;

        // Adaptive fetch concurrency: scale up for large input trees to
        // keep the network saturated. Small trees use 128 (the previous
        // fixed default) to avoid over-subscribing connections.
        let fetch_concurrency: usize = match missing_digests.len() {
            0..=500 => 128,
            501..=2000 => 256,
            _ => 512,
        };
        // Channel capacity: buffer ahead of the consumer.
        const CHANNEL_CAPACITY: usize = HARDLINK_BATCH * 2;

        type PipelineItem = (
            FileToMaterialize,
            Option<Arc<nativelink_store::filesystem_store::FileEntryImpl>>,
        );

        let total_files_to_link = files.len();
        let (tx, rx) = mpsc::channel::<PipelineItem>(CHANNEL_CAPACITY);

        let fetch_start = std::time::Instant::now();

        let missing_set: HashSet<DigestInfo> = missing_digests.iter().copied().collect();

        info!(
            total_files = total_files_to_link,
            cached = cached_set.len(),
            missing = missing_digests.len(),
            missing_bytes,
            fetch_concurrency = fetch_concurrency,
            hardlink_concurrency = HARDLINK_CONCURRENCY,
            "download_to_directory: starting pipelined fetch+hardlink",
        );

        // --- Shared state: tracks which missing digests have arrived ---
        let fetched_set: Arc<std::sync::Mutex<HashSet<DigestInfo>>> =
            Arc::new(std::sync::Mutex::new(HashSet::with_capacity(missing_digests.len())));
        let fetch_error: Arc<std::sync::Mutex<Option<Error>>> =
            Arc::new(std::sync::Mutex::new(None));
        let fetched_notify = Arc::new(Notify::new());

        // --- Fetcher future ---
        // Launches all missing blob fetches concurrently (bounded).
        let fetcher_start = std::time::Instant::now();
        let fetched_set_ref = &fetched_set;
        let fetch_error_ref = &fetch_error;
        let fetched_notify_ref = &fetched_notify;
        let fetcher_fut = async {
            // Partition into small (BatchReadBlobs) and large (ByteStream).
            let mut small: Vec<DigestInfo> = Vec::new();
            let mut large: Vec<DigestInfo> = Vec::new();
            for &d in &missing_digests {
                if is_zero_digest(d) {
                    // Zero digests don't need fetching; mark as ready.
                    fetched_set_ref.lock().unwrap().insert(d);
                    continue;
                }
                if d.size_bytes() <= BATCH_READ_MAX_BLOB_SIZE {
                    small.push(d);
                } else {
                    large.push(d);
                }
            }

            info!(
                small = small.len(),
                large = large.len(),
                missing_bytes,
                "fetcher: starting all blob fetches",
            );

            let small_count = small.len();
            let large_count = large.len();

            // Fetch small blobs via BatchReadBlobs (already batches internally).
            let batch_read_fut = async {
                if small.is_empty() {
                    return Ok::<(), Error>(());
                }
                let fetched = batch_read_small_blobs(cas_store, &small).await?;
                // Mark all successfully fetched small blobs as ready.
                {
                    let mut set = fetched_set_ref.lock().unwrap();
                    for &d in &small {
                        // batch_read_small_blobs returns the set of blobs it
                        // actually got; unfetched ones need ByteStream fallback.
                        if fetched.contains(&d) {
                            set.insert(d);
                        }
                    }
                }
                fetched_notify_ref.notify_one();

                // Fallback for small blobs not returned by BatchReadBlobs.
                let fallback: Vec<DigestInfo> = small
                    .iter()
                    .filter(|d| !fetched.contains(d))
                    .copied()
                    .collect();
                if !fallback.is_empty() {
                    debug!(
                        count = fallback.len(),
                        "fetcher: BatchReadBlobs fallback via ByteStream",
                    );
                    futures::stream::iter(fallback.into_iter().map(Ok::<_, Error>))
                        .try_for_each_concurrent(fetch_concurrency, |d| async move {
                            cas_store
                                .populate_fast_store_unchecked(d.into())
                                .await
                                .err_tip(|| format!("Populating fast store (fallback) for {d:?}"))?;
                            fetched_set_ref.lock().unwrap().insert(d);
                            fetched_notify_ref.notify_one();
                            Ok(())
                        })
                        .await?;
                }
                Ok(())
            };

            // Fetch large blobs via ByteStream with bounded concurrency.
            let bytestream_fut = async {
                if large.is_empty() {
                    return Ok::<(), Error>(());
                }
                futures::stream::iter(large.into_iter().map(Ok::<_, Error>))
                    .try_for_each_concurrent(fetch_concurrency, |d| async move {
                        let blob_start = std::time::Instant::now();
                        cas_store
                            .populate_fast_store_unchecked(d.into())
                            .await
                            .err_tip(|| format!("Populating fast store for {d:?}"))?;
                        let blob_elapsed = blob_start.elapsed();
                        if blob_elapsed.as_secs() >= 2 {
                            warn!(
                                digest = ?d,
                                size_bytes = d.size_bytes(),
                                elapsed_ms = blob_elapsed.as_millis() as u64,
                                "fetcher: slow blob fetch (>2s)",
                            );
                        }
                        fetched_set_ref.lock().unwrap().insert(d);
                        fetched_notify_ref.notify_one();
                        Ok(())
                    })
                    .await
            };

            // Run small and large fetches concurrently.
            let (batch_result, bs_result) =
                futures::future::join(batch_read_fut, bytestream_fut).await;

            let fetcher_elapsed = fetcher_start.elapsed();

            // If either failed, record the error so the producer can see it.
            if let Err(e) = batch_result {
                error!(
                    err = %e,
                    small_count,
                    "fetcher: BatchReadBlobs fetch failed",
                );
                *fetch_error_ref.lock().unwrap() = Some(e);
                fetched_notify_ref.notify_one();
            }
            if let Err(e) = bs_result {
                error!(
                    err = %e,
                    large_count,
                    "fetcher: ByteStream fetch failed",
                );
                let mut guard = fetch_error_ref.lock().unwrap();
                if guard.is_none() {
                    *guard = Some(e);
                }
                fetched_notify_ref.notify_one();
            }

            info!(
                elapsed_ms = fetcher_elapsed.as_millis() as u64,
                fetched = fetched_set_ref.lock().unwrap().len(),
                missing_total = missing_digests.len(),
                throughput_mbps = format!("{:.1}", throughput_mbps(missing_bytes, fetcher_elapsed)),
                "fetcher: all blob fetches complete",
            );
        };

        // --- Producer future ---
        // Iterates files, sends cached ones immediately, waits for missing
        // ones as they arrive from the fetcher.
        let producer_start = std::time::Instant::now();
        let producer_fut = async {
            let mut files_sent: usize = 0;
            let mut deferred_count: usize = 0;

            // Process files in batches for entry pre-fetching efficiency.
            for batch_files in files.chunks(HARDLINK_BATCH) {
                // Separate into ready (cached or already fetched) and pending.
                let mut ready_files: Vec<&FileToMaterialize> = Vec::new();
                let mut pending_files: Vec<&FileToMaterialize> = Vec::new();

                {
                    let fetched = fetched_set_ref.lock().unwrap();
                    for f in batch_files {
                        if !missing_set.contains(&f.digest) || fetched.contains(&f.digest) {
                            ready_files.push(f);
                        } else {
                            pending_files.push(f);
                        }
                    }
                }

                // Send ready files immediately.
                if !ready_files.is_empty() {
                    let ready_digests: Vec<DigestInfo> =
                        ready_files.iter().map(|f| f.digest).collect();
                    let entries =
                        filesystem_store.get_file_entries_batch(&ready_digests).await;

                    for (file, entry) in ready_files.iter().zip(entries) {
                        if entry.is_none() && !is_zero_digest(file.digest) {
                            warn!(
                                dest = %file.dest,
                                digest = ?file.digest,
                                "producer: no file entry for non-zero digest (ready batch)",
                            );
                        }
                        let item: PipelineItem = (
                            FileToMaterialize {
                                digest: file.digest,
                                dest: file.dest.clone(),
                                #[cfg(target_family = "unix")]
                                unix_mode: file.unix_mode,
                                mtime: file.mtime.clone(),
                            },
                            entry,
                        );
                        if tx.send(item).await.is_err() {
                            return Ok::<_, Error>(producer_start.elapsed());
                        }
                        files_sent += 1;
                    }
                }

                // Wait for pending files as their blobs arrive.
                if !pending_files.is_empty() {
                    deferred_count += pending_files.len();
                    let mut remaining = pending_files;

                    loop {
                        if remaining.is_empty() {
                            break;
                        }

                        // Check for fetcher errors.
                        if let Some(e) = fetch_error_ref.lock().unwrap().take() {
                            return Err(e);
                        }

                        // #92: subscribe-before-predicate. Construct the
                        // `notified()` future and arm it via `enable()`
                        // BEFORE snapshotting `fetched_set`. Any
                        // notification issued from this point on is
                        // captured by the pre-armed Notified, even if it
                        // fires between the snapshot and the await.
                        // Same shape as the cleanup_wait_notify reference
                        // at line ~5720-5759 (parity test documents the
                        // contract).
                        let notified = fetched_notify_ref.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();

                        // Partition remaining into newly ready and still pending.
                        let mut newly_ready: Vec<&FileToMaterialize> = Vec::new();
                        let mut still_pending: Vec<&FileToMaterialize> = Vec::new();
                        {
                            let fetched = fetched_set_ref.lock().unwrap();
                            for f in remaining {
                                if fetched.contains(&f.digest) {
                                    newly_ready.push(f);
                                } else {
                                    still_pending.push(f);
                                }
                            }
                        }

                        if !newly_ready.is_empty() {
                            let ready_digests: Vec<DigestInfo> =
                                newly_ready.iter().map(|f| f.digest).collect();
                            let entries =
                                filesystem_store.get_file_entries_batch(&ready_digests).await;

                            for (file, entry) in newly_ready.iter().zip(entries) {
                                if entry.is_none() && !is_zero_digest(file.digest) {
                                    warn!(
                                        dest = %file.dest,
                                        digest = ?file.digest,
                                        "producer: no file entry for non-zero digest (deferred batch)",
                                    );
                                }
                                let item: PipelineItem = (
                                    FileToMaterialize {
                                        digest: file.digest,
                                        dest: file.dest.clone(),
                                        #[cfg(target_family = "unix")]
                                        unix_mode: file.unix_mode,
                                        mtime: file.mtime.clone(),
                                    },
                                    entry,
                                );
                                if tx.send(item).await.is_err() {
                                    return Ok(producer_start.elapsed());
                                }
                                files_sent += 1;
                            }
                        }

                        remaining = still_pending;
                        if !remaining.is_empty() {
                            // Wait until the fetcher signals new arrivals.
                            // The `notified` future was armed BEFORE the
                            // snapshot, so any notification issued during
                            // the snapshot/dispatch window is delivered
                            // here.
                            notified.as_mut().await;
                        }
                    }
                }
            }

            let producer_elapsed = producer_start.elapsed();
            info!(
                files_sent,
                deferred = deferred_count,
                elapsed_ms = producer_elapsed.as_millis() as u64,
                "producer: finished sending all files",
            );

            // Explicitly drop the sender so the consumer's rx.recv()
            // returns None and the stream ends. join3 keeps all futures
            // alive until all complete, so without this the consumer
            // would wait forever.
            drop(tx);

            Ok(producer_start.elapsed())
        };

        // --- Consumer future ---
        // Reads from the channel and hardlinks with bounded concurrency.
        let hardlink_start = std::time::Instant::now();
        let slow_hardlinks = std::sync::atomic::AtomicU32::new(0);
        let max_hardlink_ms = std::sync::atomic::AtomicU64::new(0);
        let links_completed = std::sync::atomic::AtomicUsize::new(0);

        let consumer_fut = async {
            let stream = futures::stream::unfold(rx, |mut rx| async {
                rx.recv().await.map(|item| (Ok::<PipelineItem, Error>(item), rx))
            });

            stream
                .try_for_each_concurrent(HARDLINK_CONCURRENCY, |(file, prefetched)| {
                    let slow_hardlinks = &slow_hardlinks;
                    let max_hardlink_ms = &max_hardlink_ms;
                    let links_completed = &links_completed;
                    async move {
                        let digest = file.digest;
                        let dest = file.dest.clone();
                        let dest_for_err = dest.clone();
                        let link_start = std::time::Instant::now();
                        hardlink_and_set_metadata_prefetched(
                            cas_store, filesystem_store, file, prefetched,
                        )
                        .await
                        .map_err(move |e| {
                            warn!(
                                dest = %dest_for_err,
                                ?digest,
                                err = %e,
                                "download_to_directory: failed to materialize input file",
                            );
                            let mut e = e.append(format!("for digest {digest}"));
                            if e.code == Code::NotFound {
                                e.details.push(make_precondition_failure_any(digest));
                            }
                            e
                        })?;
                        let link_elapsed = link_start.elapsed();
                        let link_ms = link_elapsed.as_millis() as u64;

                        links_completed.fetch_add(1, Ordering::Relaxed);
                        max_hardlink_ms.fetch_max(link_ms, Ordering::Relaxed);

                        if link_ms > 50 {
                            slow_hardlinks.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                dest = %dest,
                                digest = ?digest,
                                elapsed_ms = link_ms,
                                "pipeline: slow hardlink (>50ms)",
                            );
                        }
                        Ok(())
                    }
                })
                .await
        };

        // Run all three concurrently. The fetcher and producer share state
        // via fetched_set + Notify. The producer and consumer share the
        // mpsc channel. The consumer drops when the producer's tx drops.
        let (_, producer_result, consumer_result) =
            futures::future::join3(fetcher_fut, producer_fut, consumer_fut).await;

        // Check consumer first (it's the critical path).
        consumer_result?;
        // Then check producer.
        let producer_elapsed = producer_result?;

        let hardlink_elapsed = hardlink_start.elapsed();
        let fetch_elapsed = fetch_start.elapsed();
        let slow_count = slow_hardlinks.load(Ordering::Relaxed);
        let max_link_ms = max_hardlink_ms.load(Ordering::Relaxed);
        let total_linked = links_completed.load(Ordering::Relaxed);
        let fetcher_elapsed = fetcher_start.elapsed();

        info!(
            total_missing = missing_digests.len(),
            total_missing_bytes = missing_bytes,
            fetch_elapsed_ms = fetcher_elapsed.as_millis() as u64,
            throughput_mbps = format!("{:.1}", throughput_mbps(missing_bytes, fetcher_elapsed)),
            "download_to_directory: fetch phase completed",
        );

        info!(
            total_links = total_linked,
            elapsed_ms = hardlink_elapsed.as_millis() as u64,
            slow_links_over_50ms = slow_count,
            max_link_ms,
            avg_link_us = if total_linked > 0 {
                hardlink_elapsed.as_micros() as u64 / total_linked as u64
            } else { 0 },
            producer_ms = producer_elapsed.as_millis() as u64,
            total_elapsed_ms = fetch_elapsed.as_millis() as u64,
            "download_to_directory: hardlink phase completed",
        );

        let total_bytes: u64 = unique_digests.iter().map(|d| d.size_bytes()).sum();
        let total_ms = phase_start.elapsed().as_millis();
        info!(
            tree_resolve_ms,
            has_check_ms = has_check_ms - tree_resolve_ms,
            fetch_ms = fetcher_elapsed.as_millis() as u64,
            hardlink_ms = hardlink_elapsed.as_millis() as u64,
            total_ms,
            num_files = unique_digests.len(),
            total_bytes,
            throughput_mbps = format!("{:.1}", throughput_mbps(total_bytes, phase_start.elapsed())),
            "download_to_directory completed",
        );

        // Calibration probe P-A sink: hand the staged input-file payload bytes
        // (`total_bytes` = sum of unique input-file digest sizes) to the caller
        // for the post-upload P-A record.
        if let Some(out) = calib_input_bytes_out {
            out.store(total_bytes, core::sync::atomic::Ordering::Relaxed);
        }

        // Calibration probe P-B (`tag="calib_staging"`): per-action input-staging
        // record on the MISS path (this function is the directory-cache
        // miss/fallback/construct path; a cache hardlink hit returns before
        // here). PRIMARY regressor `input_payload_bytes` = `total_bytes` (the
        // unique input-file payload the fetch/hardlink actually moves — the axis
        // the §6 locality `load_byte_cost` fit needs). `input_tree_bytes`/
        // `input_tree_files` are the resolved-tree directory-PROTO sizes + file
        // counts (§9 B3, the :552-553 pattern) — a secondary covariate, NOT the
        // fetch-cost axis (auditor Claim 3). Sampled uniform 1/16 by root-digest
        // hash, with the 1/1 large-tree override (§9 B4). `dir_cache_hit` is
        // structurally `false` here (see CalibStagingRecord).
        let (calib_tree_bytes, calib_tree_files) = calib_tree_totals(&tree);
        let calib_key = calib_digest_sample_key(digest);
        // GAP-2: `missing_bytes` (the network-fetch axis) also drives the 1/1
        // override, so a large-fetch/small-proto-tree LTO input is never dropped.
        if calib_staging_sampled(calib_key, calib_tree_bytes, missing_bytes) {
            CalibStagingRecord {
                input_staging_ms: total_ms as u64,
                dir_cache_hit: false,
                input_payload_bytes: total_bytes,
                // network-fetch axis: bytes not already cached locally, i.e. the
                // volume the network transfer moved (in scope from the batch
                // existence check above). Separates fetch cost from hardlink cost
                // in the §6 fit (auditor Claim 3 residual).
                input_missing_bytes: missing_bytes,
                input_tree_bytes: calib_tree_bytes,
                input_tree_files: calib_tree_files,
            }
            .emit(digest);
        }

        Ok(())
    }
    .boxed()
}

/// Prepare a single output file's parent directory.
///
/// Fast path: just `create_dir_all` and confirm the directory is writable.
///
/// Slow path (lock-serialized): walk the parent-path components, replacing
/// any read-only symlink-into-cache with a writable shallow-copy directory
/// AND chmod'ing any read-only-but-not-symlink directory to writable. This
/// is required because the worker's input-fetch hardlink mode marks the
/// input root + its sub-trees read-only to preserve cache integrity, but
/// bazel requires the output-file's parent to be writable.
///
/// `lock` serializes the slow-path symlink replacement to avoid concurrent
/// tasks racing on the same symlink (EEXIST / ENOENT).
///
/// Increments to the `#86` O14 counters are now routed to the
/// process-global `nativelink_util::o11_probes::SYMLINK_FIX_COUNTERS`
/// singleton registered with the `MetricsRegistry`, so they appear on the
/// `/metrics` endpoint. The per-instance `Metrics` struct no longer carries
/// these two fields.
///
/// **Contract (asymmetric):**
/// - Under-action: increment MUST fire on slow-path entry. Verified by
///   `symlink_fix_slow_path_increments_on_slow_path_entry` (T1).
/// - Over-action: increment MUST NOT fire on fast-path early-return.
///   Verified by `symlink_fix_slow_path_does_not_increment_on_fast_path`
///   (T2).
#[doc(hidden)]
pub async fn prepare_output_directory(
    work_dir: &str,
    working_directory: &str,
    output_file: &str,
    lock: &tokio::sync::Mutex<()>,
) -> Result<(), Error> {
    let full_output_path = if working_directory.is_empty() {
        format!("{work_dir}/{output_file}")
    } else {
        format!("{work_dir}/{working_directory}/{output_file}")
    };
    let full_parent_path = Path::new(&full_output_path)
        .parent()
        .err_tip(|| format!("Parent path for {full_output_path} has no parent"))?;

    // Fast path: create_dir_all and verify the directory is writable.
    // create_dir_all succeeds even if the directory is read-only
    // (it already exists), but rustc needs write access for outputs.
    if fs::create_dir_all(full_parent_path).await.is_ok() {
        let mut dir_writable = true;
        #[cfg(target_family = "unix")]
        if let Ok(m) = fs::metadata(full_parent_path).await {
            dir_writable = m.mode() & 0o200 != 0;
        }
        if dir_writable {
            return Ok(());
        }
        // Directory exists but is not writable (likely through
        // a symlink to the read-only cache). Fall through to fix.
    }

    // Slow path: serialize to avoid concurrent symlink replacement races.
    //
    // CONTRACT (O5 invariant): this path is only expected to fire in
    // direct-use mode, where the directory cache materialises entries as
    // read-only symlinks. In normal mode, `download_to_directory` creates
    // the work directory as a real writable directory, so
    // `dir_writable == true` above and this branch is unreachable.
    //
    // PRE-MORTEM: if a future change makes normal-mode dirs read-only, this
    // branch WILL fire — `fs::remove_file` on a real directory fails with
    // EISDIR and surfaces as "Failed to remove symlink: …", pointing nowhere
    // near the true cause. Name this invariant now so that change is caught.
    //
    // TIMING (post-#clonefile-fallback): [C] output-dir prep now runs
    // sequentially AFTER [B2] materialises the input tree, so it walks over a
    // clonefile'd tree. The invariant still holds because clonefile produces
    // writable (0o755) dirs (`fs_util.rs` clone perms) — but a change to that
    // clone-perms behavior, not just to `download_to_directory`, would now also
    // trip this branch. Fence-watch both.
    let _guard = lock.lock().await;
    // #86: every acquire (denominator for slow-path rate). Routed to the
    // process-global singleton registered with MetricsRegistry so this
    // counter appears on the /metrics endpoint.
    symlink_fix_counters().record_acquire();

    // Re-check under lock — another task may have already fixed it.
    if fs::create_dir_all(full_parent_path).await.is_ok() {
        let mut dir_writable = true;
        #[cfg(target_family = "unix")]
        if let Ok(m) = fs::metadata(full_parent_path).await {
            dir_writable = m.mode() & 0o200 != 0;
        }
        if dir_writable {
            return Ok(());
        }
    }
    // #86: numerator — true slow-path entry (under-lock fast-path
    // re-check failed, real symlink fix-up work is about to run).
    symlink_fix_counters().record_slow_path_entry();

    // Walk the path and replace blocking symlinks with writable
    // shallow-copy directories that preserve access to all
    // original entries via absolute symlinks.
    let work_root = Path::new(work_dir);
    let relative = full_parent_path.strip_prefix(work_root).map_err(|_| {
        make_err!(
            Code::Internal,
            "Output path {} not under work dir {}",
            full_parent_path.display(),
            work_root.display()
        )
    })?;

    let mut current = work_root.to_path_buf();
    for component in relative.components() {
        let component_name = component.as_os_str();
        let next = current.join(component_name);

        match fs::symlink_metadata(&next).await {
            Ok(meta) => {
                #[cfg(target_family = "unix")]
                if meta.is_symlink() {
                    // Check if resolved target is a read-only directory
                    let needs_replace = match fs::canonicalize(&next).await {
                        Ok(resolved) => match fs::metadata(&resolved).await {
                            Ok(m) => m.is_dir() && (m.mode() & 0o200 == 0),
                            Err(_) => false,
                        },
                        Err(_) => false,
                    };

                    if needs_replace {
                        let resolved = fs::canonicalize(&next).await.err_tip(|| {
                            format!("Failed to resolve: {}", next.display())
                        })?;

                        // Replace symlink with a writable shallow-copy directory.
                        // Each entry in the original target gets an absolute symlink,
                        // except for self-referential entries (e.g., bazel-out -> .).
                        fs::remove_file(&next).await.err_tip(|| {
                            format!("Failed to remove symlink: {}", next.display())
                        })?;
                        fs::create_dir(&next).await.err_tip(|| {
                            format!("Failed to create dir: {}", next.display())
                        })?;

                        let rd = fs::read_dir(&resolved).await.err_tip(|| {
                            format!("Failed to read dir: {}", resolved.display())
                        })?;
                        let (_permit, mut inner_rd) = rd.into_inner();
                        while let Some(entry) = inner_rd.next_entry().await.err_tip(|| {
                            format!("Failed to iterate: {}", resolved.display())
                        })? {
                            let entry_name = entry.file_name();
                            // Skip self-referential entries (bazel-out -> . creates
                            // an entry pointing back to the replaced dir itself).
                            if entry_name == component_name {
                                continue;
                            }
                            let abs_target = resolved.join(&entry_name);
                            let link = next.join(&entry_name);
                            if let Err(e) = fs::symlink(&abs_target, &link).await {
                                warn!(
                                    link = %link.display(),
                                    target = %abs_target.display(),
                                    ?e,
                                    "prepare_output_dirs: failed to create shallow-copy symlink",
                                );
                            }
                        }

                        // Retry — the fix at this level may be sufficient.
                        if fs::create_dir_all(full_parent_path).await.is_ok() {
                            return Ok(());
                        }
                    }
                }

                #[cfg(target_family = "unix")]
                if meta.is_dir() && (meta.mode() & 0o200 == 0) {
                    // Read-only directory in the work tree (not through symlink).
                    // Safe to make writable since work dirs are independent copies.
                    let mut perms = meta.permissions();
                    perms.set_mode(meta.mode() | 0o200);
                    drop(fs::set_permissions(&next, perms).await);
                }
            }
            Err(_) => {
                // Path doesn't exist — create remaining dirs.
                fs::create_dir_all(full_parent_path).await.err_tip(|| {
                    format!(
                        "Error creating output directory {}",
                        full_parent_path.display()
                    )
                })?;
                return Ok(());
            }
        }

        current = next;
    }

    // Final attempt after all fixes applied.
    fs::create_dir_all(full_parent_path).await.err_tip(|| {
        format!(
            "Error creating output directory {} (after symlink fixes)",
            full_parent_path.display()
        )
    })?;
    Ok(())
}

/// Prepares action inputs by first trying the directory cache (if available),
/// then falling back to traditional `download_to_directory`.
///
/// This provides a significant performance improvement for repeated builds
/// with the same input directories.
///
/// # Returns
/// * `Ok(None)` - Normal mode (hardlink or download). Caller should clean up
///   the work directory normally.
/// * `Ok(Some((digest, pin_guard)))` - Direct-use mode. The work directory is
///   a symlink to the cache. Caller MUST hold the returned guard for the
///   action's lifetime; dropping it releases the cache pin synchronously
///   (including on cancellation paths). The digest is retained for the
///   work-symlink cleanup logic in `do_cleanup`.
pub async fn prepare_action_inputs(
    directory_cache: &Option<Arc<crate::directory_cache::DirectoryCache>>,
    cas_store: &FastSlowStore,
    filesystem_store: Pin<&FilesystemStore>,
    digest: &DigestInfo,
    work_directory: &str,
    pre_resolved_tree: Option<HashMap<DigestInfo, ProtoDirectory>>,
    server_missing_digests: Option<HashSet<DigestInfo>>,
    // Calibration P-A sink, forwarded to `download_to_directory` (observability
    // only). Only populated on the traditional/fallback staging path; on a
    // directory-cache hit it stays unset (cache hits stage ~0 fetched bytes).
    calib_input_bytes_out: Option<&core::sync::atomic::AtomicU64>,
) -> Result<Option<(DigestInfo, crate::directory_cache::DirectoryCachePinGuard)>, Error> {
    info!(?digest, work_directory, "prepare_action_inputs: entered");
    // Try cache first if available
    if let Some(cache) = directory_cache {
        if cache.is_direct_use_mode() {
            // Direct-use mode: symlink work_directory -> cache_path.
            // The work directory must NOT exist yet (it becomes the symlink).
            info!(?digest, "prepare_action_inputs: calling directory_cache.get_or_create_direct");
            let res = cache
                .get_or_create_direct(*digest, Path::new(work_directory))
                .await;
            info!(
                ?digest,
                ok = res.is_ok(),
                "prepare_action_inputs: directory_cache.get_or_create_direct returned"
            );
            match res {
                Ok((_cache_path, _was_hit, pin_guard)) => {
                    info!(
                        ?digest,
                        work_directory,
                        was_hit = _was_hit,
                        cache_path = %_cache_path.display(),
                        "Successfully prepared inputs via directory cache (direct-use mode)",
                    );
                    return Ok(Some((*digest, pin_guard)));
                }
                Err(e) => {
                    warn!(
                        ?digest,
                        ?e,
                        "Directory cache direct-use failed, falling back to traditional download"
                    );
                    // Fall through to traditional path.
                    // Create the work directory since direct-use didn't create it.
                    fs::create_dir_all(work_directory)
                        .await
                        .err_tip(|| format!("Error creating work directory {work_directory} after direct-use fallback"))?;
                }
            }
        } else {
            // Normal hardlink mode
            info!(?digest, "prepare_action_inputs: calling directory_cache.get_or_create");
            let res = cache
                .get_or_create(*digest, Path::new(work_directory))
                .await;
            info!(
                ?digest,
                ok = res.is_ok(),
                "prepare_action_inputs: directory_cache.get_or_create returned"
            );
            match res {
                Ok(cache_hit) => {
                    trace!(
                        ?digest,
                        work_directory, cache_hit, "Successfully prepared inputs via directory cache"
                    );
                    return Ok(None);
                }
                Err(e) => {
                    warn!(
                        ?digest,
                        ?e,
                        "Directory cache failed, falling back to traditional download"
                    );
                    // Fall through to traditional path
                }
            }
        }
    }

    // Traditional path (cache disabled or failed)
    info!(?digest, work_directory, "prepare_action_inputs: falling back to download_to_directory");
    let res = download_to_directory(cas_store, filesystem_store, digest, work_directory, pre_resolved_tree, server_missing_digests, calib_input_bytes_out).await;
    info!(
        ?digest,
        ok = res.is_ok(),
        "prepare_action_inputs: fallback download_to_directory returned"
    );
    res?;
    Ok(None)
}

#[cfg(target_family = "windows")]
fn is_executable(_metadata: &std::fs::Metadata, full_path: &impl AsRef<Path>) -> bool {
    static EXECUTABLE_EXTENSIONS: &[&str] = &["exe", "bat", "com"];
    EXECUTABLE_EXTENSIONS
        .iter()
        .any(|ext| full_path.as_ref().extension().map_or(false, |v| v == *ext))
}

#[cfg(target_family = "unix")]
fn is_executable(metadata: &std::fs::Metadata, _full_path: &impl AsRef<Path>) -> bool {
    (metadata.mode() & 0o111) != 0
}

type DigestUploader = Arc<tokio::sync::OnceCell<()>>;

/// Hash a single file at `full_path` and return `(path, digest)`.
/// Returns `Ok(None)` if the path is not a regular file (or a symlink that
/// resolves to one) — callers skip non-file paths silently.
/// Used by Phase 1 prehash to avoid re-reading the file in Phase 2.
async fn prehash_single_file(
    full_path: OsString,
    hasher: DigestHasherFunc,
) -> Result<Option<(OsString, DigestInfo)>, Error> {
    let metadata = match fs::symlink_metadata(&full_path).await {
        Ok(m) if m.is_file() => m,
        // Symlinks that resolve to files are also hashable.
        Ok(m) if m.is_symlink() => {
            match fs::metadata(&full_path).await {
                Ok(rm) if rm.is_file() => rm,
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    let file_size = metadata.len();
    let file = fs::open_file(&full_path, 0)
        .await
        .err_tip(|| format!("Could not open file {full_path:?} for pre-hash"))?;
    let (digest, _file) = hasher
        .hasher()
        .digest_for_file(&full_path, file, Some(file_size))
        .await
        .err_tip(|| format!("Failed to pre-hash {full_path:?}"))?;
    Ok(Some((full_path, digest)))
}

/// Recursively walk a directory tree and pre-hash every regular file,
/// returning all `(absolute_path, digest)` pairs in a `Vec`.
///
/// All internal error branches return `Ok(Vec::new())` — a missing or
/// unreadable directory is silently treated as empty so Phase 1 degrades
/// gracefully (Phase 2 `upload_file` will catch the real error).
/// The function CAN return `Err` in theory (the type admits it) but in
/// practice all error paths are converted to empty-Ok before propagating.
///
/// The walk structure (readdir → push file/dir futures) mirrors
/// `upload_directory`, but the concurrency regime differs:
/// `file_futures` is drained to completion before `dir_futures` begins
/// polling (sequential-phase drain), whereas `upload_directory` uses
/// `try_join3` for true simultaneous polling of all three future sets.
/// For deep trees this means depth-N files do not start hashing until
/// depths 1..N-1 have finished; the latency penalty is O(tree_depth)
/// serial phases but I/O within each phase is concurrent.
fn prehash_directory_tree(
    dir_path: OsString,
    hasher: DigestHasherFunc,
) -> BoxFuture<'static, Result<Vec<(OsString, DigestInfo)>, Error>> {
    Box::pin(async move {
        // Skip if this path is not a directory (e.g. a top-level output_file).
        match fs::symlink_metadata(&dir_path).await {
            Ok(m) if m.is_dir() => {}
            _ => return Ok(Vec::new()),
        }

        let (_permit, dir_handle) = match fs::read_dir(&dir_path).await {
            Ok(v) => v.into_inner(),
            Err(_) => return Ok(Vec::new()),
        };
        let mut dir_stream = ReadDirStream::new(dir_handle);

        let mut file_futures: FuturesUnordered<
            BoxFuture<'static, Result<Option<(OsString, DigestInfo)>, Error>>,
        > = FuturesUnordered::new();
        let mut dir_futures: FuturesUnordered<
            BoxFuture<'static, Result<Vec<(OsString, DigestInfo)>, Error>>,
        > = FuturesUnordered::new();

        while let Some(entry_result) = dir_stream.next().await {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => continue,
            };
            let file_type = match entry.file_type().await {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            let full_path = OsString::from(
                Path::new(&dir_path).join(entry.path())
            );
            if file_type.is_file() {
                file_futures.push(prehash_single_file(full_path, hasher).boxed());
            } else if file_type.is_dir() {
                dir_futures.push(prehash_directory_tree(full_path, hasher).boxed());
            }
            // Symlinks inside directories are uploaded as symlinks by
            // upload_directory; skip them in the prehash walk.
        }

        let mut results = Vec::new();

        // Collect file hashes.
        while let Some(res) = file_futures.next().await {
            if let Ok(Some(pair)) = res {
                results.push(pair);
            }
        }

        // Collect recursive subdirectory hashes.
        while let Some(res) = dir_futures.next().await {
            if let Ok(mut pairs) = res {
                results.append(&mut pairs);
            }
        }

        Ok(results)
    })
}

async fn upload_file(
    cas_store: Pin<&impl StoreLike>,
    full_path: impl AsRef<Path> + Debug + Send + Sync,
    hasher: DigestHasherFunc,
    metadata: std::fs::Metadata,
    digest_uploaders: Arc<Mutex<HashMap<DigestInfo, DigestUploader>>>,
    known_existing: Arc<Mutex<HashSet<DigestInfo>>>,
    // Digests submitted to the Phase 1 batch `has_with_results` call.
    // If the digest is in this set, the batch already answered the
    // existence question — skip the individual `has()` RPC and go
    // directly to upload (or skip if `known_existing` confirms it exists).
    batch_checked: Arc<HashSet<DigestInfo>>,
    prehash_digest: Option<DigestInfo>,
) -> Result<FileInfo, Error> {
    let is_executable = is_executable(&metadata, &full_path);
    let file_size = metadata.len();
    let file = fs::open_file(&full_path, 0)
        .await
        .err_tip(|| format!("Could not open file {full_path:?}"))?;

    // Use the pre-computed digest from Phase 1 if available, avoiding
    // a redundant full-file read + hash.
    let (digest, mut file) = if let Some(digest) = prehash_digest {
        (digest, file)
    } else {
        hasher
            .hasher()
            .digest_for_file(&full_path, file, Some(file_size))
            .await
            .err_tip(|| format!("Failed to hash file in digest_for_file failed for {full_path:?}"))?
    };

    let digest_uploader = match digest_uploaders.lock().entry(digest) {
        std::collections::hash_map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
        std::collections::hash_map::Entry::Vacant(vacant_entry) => vacant_entry
            .insert(Arc::new(tokio::sync::OnceCell::new()))
            .clone(),
    };

    // Only upload a file with a given hash once.  The file may exist multiple
    // times in the output with different names.
    digest_uploader
        .get_or_try_init(async || {
            // Check the batch-populated known_existing set first to avoid
            // an individual gRPC round-trip for each digest.
            if known_existing.lock().contains(&digest) {
                trace!(
                    ?digest,
                    "upload_file: digest already confirmed by batch has(), skipping upload",
                );
                return Ok(());
            }

            let cas_store = cas_store.as_store_driver_pin();
            let store_key: StoreKey<'_> = digest.into();

            // For digests NOT covered by the Phase 1 batch (e.g. directory
            // tree protos created during upload), do the individual has()
            // before attempting the upload.
            if !batch_checked.contains(&digest) {
                let has_start = std::time::Instant::now();
                if cas_store
                    .has(store_key.borrow())
                    .await
                    .is_ok_and(|result| result.is_some())
                {
                    trace!(
                        ?digest,
                        has_elapsed_ms = has_start.elapsed().as_millis(),
                        "upload_file: digest already exists in CAS, skipping upload",
                    );
                    known_existing.lock().insert(digest);
                    return Ok(());
                }
                trace!(
                    ?digest,
                    has_elapsed_ms = has_start.elapsed().as_millis(),
                    file_size = digest.size_bytes(),
                    "upload_file: digest not in CAS, starting upload",
                );
            } else {
                trace!(
                    ?digest,
                    "upload_file: digest covered by Phase 1 batch (not found), uploading",
                );
            }

            std::io::Seek::seek(file.as_std_mut(), std::io::SeekFrom::Start(0))
                .err_tip(|| "Could not rewind file")?;

            // Note: For unknown reasons we appear to be hitting:
            // https://github.com/rust-lang/rust/issues/92096
            // or a similar issue if we try to use the non-store driver function, so we
            // are using the store driver function here.
            let store_key_for_upload = store_key.clone();
            let file_upload_start = std::time::Instant::now();
            let upload_result = cas_store
                .update_with_whole_file(
                    store_key_for_upload,
                    full_path.as_ref().into(),
                    file,
                    UploadSizeInfo::ExactSize(digest.size_bytes()),
                )
                .await
                .map(|_slot| ());
            let upload_elapsed = file_upload_start.elapsed();

            match &upload_result {
                Ok(()) => {
                    info!(
                        ?digest,
                        size_bytes = digest.size_bytes(),
                        elapsed_ms = upload_elapsed.as_millis() as u64,
                        throughput_mbps = format!("{:.1}", throughput_mbps(digest.size_bytes(), upload_elapsed)),
                        "upload_file: CAS write completed",
                    );
                }
                Err(e) => {
                    error!(
                        ?digest,
                        size_bytes = digest.size_bytes(),
                        elapsed_ms = upload_elapsed.as_millis() as u64,
                        ?e,
                        "upload_file: CAS write failed",
                    );
                }
            }

            match upload_result {
                Ok(()) => Ok(()),
                Err(err) => {
                    // Output uploads run concurrently and may overlap (e.g. a file is listed
                    // both as an output file and inside an output directory). When another
                    // upload has already moved the file into CAS, this update can fail with
                    // NotFound even though the digest is now present. Per the RE spec, missing
                    // outputs should be ignored, so treat this as success if the digest exists.
                    if err.code == Code::NotFound
                        && cas_store
                            .has(store_key.borrow())
                            .await
                            .is_ok_and(|result| result.is_some())
                    {
                        Ok(())
                    } else {
                        Err(err)
                    }
                }
            }
        })
        .await
        .err_tip(|| format!("for {full_path:?}"))?;

    let name = full_path
        .as_ref()
        .file_name()
        .err_tip(|| format!("Expected file_name to exist on {full_path:?}"))?
        .to_str()
        .err_tip(|| {
            make_err!(
                Code::Internal,
                "Could not convert {:?} to string",
                full_path
            )
        })?
        .to_string();

    Ok(FileInfo {
        name_or_path: NameOrPath::Name(name),
        digest,
        is_executable,
    })
}

/// Normalize a relative path in-memory by resolving `.` and `..` components.
/// The RE API spec requires symlink targets to be relative paths without `..`.
/// Unlike `Path::canonicalize`, this does not touch the filesystem.
/// Normalize a relative path by resolving `.` and `..` components.
/// Leading `..` that would escape the root are preserved (not silently
/// dropped) so the caller can detect symlinks pointing outside the
/// work directory.
fn normalize_relative_path(path: &str) -> String {
    let mut components: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if components.last().map_or(true, |c| *c == "..") {
                    // Can't go above root — preserve the ".." so caller
                    // sees the escape attempt.
                    components.push("..");
                } else {
                    components.pop();
                }
            }
            _ => components.push(part),
        }
    }
    components.join("/")
}

async fn upload_symlink(
    full_path: impl AsRef<Path> + Debug,
    full_work_directory_path: impl AsRef<Path>,
) -> Result<SymlinkInfo, Error> {
    let full_target_path = fs::read_link(full_path.as_ref())
        .await
        .err_tip(|| format!("Could not get read_link path of {full_path:?}"))?;

    // Detect if our symlink is inside our work directory, if it is find the
    // relative path otherwise use the absolute path.
    let target = if full_target_path.starts_with(full_work_directory_path.as_ref()) {
        let raw = full_target_path
            .strip_prefix(full_work_directory_path.as_ref())
            .map_err(|e| make_err!(Code::Internal, "Could not strip work dir prefix: {}", e))?
            .to_str()
            .err_tip(|| {
                make_err!(
                    Code::Internal,
                    "Could not convert '{:?}' to string",
                    full_target_path
                )
            })?;
        // strip_prefix does not normalize `..` components, but the RE API
        // requires symlink targets to be clean relative paths. Normalize
        // in-memory to resolve any `.` or `..` segments.
        normalize_relative_path(raw)
    } else {
        full_target_path
            .to_str()
            .err_tip(|| {
                make_err!(
                    Code::Internal,
                    "Could not convert '{:?}' to string",
                    full_target_path
                )
            })?
            .to_string()
    };

    let name = full_path
        .as_ref()
        .file_name()
        .err_tip(|| format!("Expected file_name to exist on {full_path:?}"))?
        .to_str()
        .err_tip(|| {
            make_err!(
                Code::Internal,
                "Could not convert {:?} to string",
                full_path
            )
        })?
        .to_string();

    Ok(SymlinkInfo {
        name_or_path: NameOrPath::Name(name),
        target,
    })
}

fn upload_directory<'a, P: AsRef<Path> + Debug + Send + Sync + Clone + 'a>(
    cas_store: Pin<&'a impl StoreLike>,
    full_dir_path: P,
    full_work_directory: &'a str,
    hasher: DigestHasherFunc,
    digest_uploaders: Arc<Mutex<HashMap<DigestInfo, DigestUploader>>>,
    known_existing: Arc<Mutex<HashSet<DigestInfo>>>,
    // Digests in the Phase 1 batch (covered regardless of found/not-found).
    batch_checked: Arc<HashSet<DigestInfo>>,
    // Pre-computed digests from Phase 1; keyed by absolute path (OsString).
    // Allows upload_file to skip the re-hash step for directory-interior files.
    prehash_digests: Arc<HashMap<OsString, DigestInfo>>,
) -> BoxFuture<'a, Result<(Directory, VecDeque<ProtoDirectory>), Error>> {
    Box::pin(async move {
        let file_futures = FuturesUnordered::new();
        let dir_futures = FuturesUnordered::new();
        let symlink_futures = FuturesUnordered::new();
        {
            let (_permit, dir_handle) = fs::read_dir(&full_dir_path)
                .await
                .err_tip(|| format!("Error reading dir for reading {full_dir_path:?}"))?
                .into_inner();
            let mut dir_stream = ReadDirStream::new(dir_handle);
            // Note: Try very hard to not leave file descriptors open. Try to keep them as short
            // lived as possible. This is why we iterate the directory and then build a bunch of
            // futures with all the work we are wanting to do then execute it. It allows us to
            // close the directory iterator file descriptor, then open the child files/folders.
            while let Some(entry_result) = dir_stream.next().await {
                let entry = entry_result.err_tip(|| "Error while iterating directory")?;
                let file_type = entry
                    .file_type()
                    .await
                    .err_tip(|| format!("Error running file_type() on {entry:?}"))?;
                let full_path = full_dir_path.as_ref().join(entry.path());
                if file_type.is_dir() {
                    let full_dir_path = full_dir_path.clone();
                    let known_existing = known_existing.clone();
                    let batch_checked = batch_checked.clone();
                    let prehash_digests = prehash_digests.clone();
                    dir_futures.push(
                        upload_directory(
                            cas_store,
                            full_path.clone(),
                            full_work_directory,
                            hasher,
                            digest_uploaders.clone(),
                            known_existing,
                            batch_checked,
                            prehash_digests,
                        )
                        .and_then(|(dir, all_dirs)| async move {
                            let directory_name = full_path
                                .file_name()
                                .err_tip(|| {
                                    format!("Expected file_name to exist on {full_dir_path:?}")
                                })?
                                .to_str()
                                .err_tip(|| {
                                    make_err!(
                                        Code::Internal,
                                        "Could not convert {:?} to string",
                                        full_dir_path
                                    )
                                })?
                                .to_string();

                            let digest =
                                serialize_and_upload_message(&dir, cas_store, &mut hasher.hasher())
                                    .await
                                    .err_tip(|| format!("for {}", full_path.display()))?;

                            Result::<(DirectoryNode, VecDeque<Directory>), Error>::Ok((
                                DirectoryNode {
                                    name: directory_name,
                                    digest: Some(digest.into()),
                                },
                                all_dirs,
                            ))
                        })
                        .boxed(),
                    );
                } else if file_type.is_file() {
                    let digest_uploaders = digest_uploaders.clone();
                    let known_existing = known_existing.clone();
                    let batch_checked = batch_checked.clone();
                    let prehash_digests = prehash_digests.clone();
                    let full_path_key = OsString::from(&full_path);
                    file_futures.push(async move {
                        let metadata = fs::metadata(&full_path)
                            .await
                            .err_tip(|| format!("Could not open file {}", full_path.display()))?;
                        // Use the pre-computed digest from Phase 1 if available.
                        let cached_digest = prehash_digests.get(&full_path_key).copied();
                        upload_file(
                            cas_store,
                            &full_path,
                            hasher,
                            metadata,
                            digest_uploaders,
                            known_existing,
                            batch_checked,
                            cached_digest,
                        )
                            .map_ok(TryInto::try_into)
                            .await?
                    });
                } else if file_type.is_symlink() {
                    symlink_futures.push(
                        upload_symlink(full_path, &full_work_directory)
                            .map(|symlink| symlink?.try_into()),
                    );
                }
            }
        }

        let (mut file_nodes, dir_entries, mut symlinks) = try_join3(
            file_futures.try_collect::<Vec<FileNode>>(),
            dir_futures.try_collect::<Vec<(DirectoryNode, VecDeque<Directory>)>>(),
            symlink_futures.try_collect::<Vec<SymlinkNode>>(),
        )
        .await?;

        let mut directory_nodes = Vec::with_capacity(dir_entries.len());
        // For efficiency we use a deque because it allows cheap concat of Vecs.
        // We make the assumption here that when performance is important it is because
        // our directory is quite large. This allows us to cheaply merge large amounts of
        // directories into one VecDeque. Then after we are done we need to collapse it
        // down into a single Vec.
        let mut all_child_directories = VecDeque::with_capacity(dir_entries.len());
        for (directory_node, mut recursive_child_directories) in dir_entries {
            directory_nodes.push(directory_node);
            all_child_directories.append(&mut recursive_child_directories);
        }

        file_nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        directory_nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        symlinks.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        let directory = Directory {
            files: file_nodes,
            directories: directory_nodes,
            symlinks,
            node_properties: None, // We don't support file properties.
        };
        all_child_directories.push_back(directory.clone());

        Ok((directory, all_child_directories))
    })
}

async fn process_side_channel_file(
    side_channel_file: Cow<'_, OsStr>,
    args: &[&OsStr],
    timeout: Duration,
) -> Result<Option<Error>, Error> {
    let mut json_contents = String::new();
    {
        // Note: Scoping `file_slot` allows the file_slot semaphore to be released faster.
        let mut file_slot = match fs::open_file(side_channel_file, 0).await {
            Ok(file_slot) => file_slot,
            Err(e) => {
                if e.code != Code::NotFound {
                    return Err(e).err_tip(|| "Error opening side channel file");
                }
                // Note: If file does not exist, it's ok. Users are not required to create this file.
                return Ok(None);
            }
        };
        std::io::Read::read_to_string(file_slot.as_std_mut(), &mut json_contents)
            .err_tip(|| "Error reading side channel file")?;
    }

    let side_channel_info: SideChannelInfo =
        serde_json5::from_str(&json_contents).map_err(|e| {
            make_input_err!(
                "Could not convert contents of side channel file (json) to SideChannelInfo : {e:?}"
            )
        })?;
    Ok(side_channel_info.failure.map(|failure| match failure {
        SideChannelFailureReason::Timeout => Error::new(
            Code::DeadlineExceeded,
            format!(
                "Command '{}' timed out after {} seconds",
                args.join(OsStr::new(" ")).to_string_lossy(),
                timeout.as_secs_f32()
            ),
        ),
    }))
}

async fn do_cleanup(
    running_actions_manager: &Arc<RunningActionsManagerImpl>,
    operation_id: &OperationId,
    action_directory: &str,
    direct_use_pin: Option<(DigestInfo, crate::directory_cache::DirectoryCachePinGuard)>,
) -> Result<(), Error> {
    // Mark this operation as being cleaned up
    let Some(_cleaning_guard) = running_actions_manager.perform_cleanup(operation_id.clone())
    else {
        // Cleanup is already happening elsewhere.
        return Ok(());
    };

    debug!("Worker cleaning up");

    // The pin guard (if Some) owns the cache ref_count decrement via sync
    // Drop. Holding it for the duration of this function preserves today's
    // ordering: the directory-cache pin is released as the function
    // returns, after the work-symlink and action-directory removals. The
    // `is_direct_use` boolean is captured up-front so we can drive the
    // symlink-removal branch below without re-inspecting the guard.
    let is_direct_use = direct_use_pin.is_some();

    // Note: We need to be careful to keep trying to cleanup even if one of the steps fails.
    //
    // In direct-use mode, the work directory (action_directory/work) is a
    // symlink to the cache. We must NOT follow that symlink when deleting.
    // `remove_dir_all` would follow the symlink and destroy the cache entry.
    //
    // Strategy: if direct-use is active, first remove the work symlink, then
    // remove the action directory normally (which now only contains non-symlink
    // artifacts like stdout/stderr files).
    let remove_dir_result = if is_direct_use {
        let work_symlink = PathBuf::from(action_directory).join("work");
        // Remove the symlink itself (not its target). On unix, symlinks to
        // directories are removed with `remove_file`, not `remove_dir`.
        let symlink_result = fs::remove_file(&work_symlink).await;
        if let Err(ref e) = symlink_result {
            // The work symlink may not exist if prepare_action failed before
            // creating it, or may have already been cleaned up. Not fatal.
            debug!(
                %operation_id,
                path = %work_symlink.display(),
                ?e,
                "do_cleanup: could not remove direct-use work symlink (may not exist)",
            );
        }
        // Now remove the rest of the action directory normally. Bounded by
        // CLEANUP_DELETE_SEMAPHORE so a mass-drop burst can't saturate the
        // blocking pool; the one-retry lives inside the helper.
        bounded_remove_dir_all(action_directory).await
    } else {
        // Bounded delete (see CLEANUP_DELETE_INFLIGHT_CAP). The macOS
        // Spotlight/Finder ENOTEMPTY one-retry now lives in the helper.
        bounded_remove_dir_all(action_directory).await
    };

    // Explicit drop after the work-symlink + action-directory removals.
    // Releases the directory-cache ref_count synchronously (sync fetch_sub)
    // — see DirectoryCachePinGuard. If we returned early above on an
    // unwind, the guard is dropped on the unwinding stack with identical
    // semantics. #57 §3 Choice A.
    drop(direct_use_pin);

    if let Err(err) = running_actions_manager.cleanup_action(operation_id) {
        error!(%operation_id, ?err, "Error cleaning up action");
        Result::<(), Error>::Err(err).merge(remove_dir_result)
    } else if let Err(err) = remove_dir_result {
        error!(%operation_id, ?err, "Error removing working directory");
        Err(err)
    } else {
        Ok(())
    }
}

pub trait RunningAction: Sync + Send + Sized + Unpin + 'static {
    /// Returns the action id of the action.
    fn get_operation_id(&self) -> &OperationId;

    /// Anything that needs to execute before the actions is actually executed should happen here.
    fn prepare_action(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Actually perform the execution of the action.
    fn execute(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Any uploading, processing or analyzing of the results should happen here.
    fn upload_results(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Cleanup any residual files, handles or other junk resulting from running the action.
    fn cleanup(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Returns the final result. As a general rule this action should be thought of as
    /// a consumption of `self`, meaning once a return happens here the lifetime of `Self`
    /// is over and any action performed on it after this call is undefined behavior.
    fn get_finished_result(
        self: Arc<Self>,
    ) -> impl Future<Output = Result<ActionResult, Error>> + Send;

    /// Returns the work directory of the action.
    fn get_work_directory(&self) -> &String;

    /// Returns whether this action has been cancelled. AC-poisoning fix
    /// residual-window guard: the publish closure at
    /// `local_worker.rs:~2474` reads this via a captured `Arc<Self>`
    /// to suppress AC writes after `kill_operation` arrives in the
    /// gap between child-exit and `cache_action_result`. Default
    /// returns `false` so test stubs don't have to implement it.
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct RunningActionImplExecutionResult {
    stdout: Bytes,
    stderr: Bytes,
    exit_code: i32,
}

#[derive(Debug)]
struct RunningActionImplState {
    command_proto: Option<ProtoCommand>,
    // TODO(palfrey) Kill is not implemented yet, but is instrumented.
    // However, it is used if the worker disconnects to destroy current jobs.
    kill_channel_tx: Option<oneshot::Sender<()>>,
    kill_channel_rx: Option<oneshot::Receiver<()>>,
    execution_result: Option<RunningActionImplExecutionResult>,
    action_result: Option<ActionResult>,
    execution_metadata: ExecutionMetadata,
    // If there was an internal error, this will be set.
    // This should NOT be set if everything was fine, but the process had a
    // non-zero exit code. Instead this should be used for internal errors
    // that prevented the action from running, upload failures, timeouts, exc...
    // but we have (or could have) the action results (like stderr/stdout).
    error: Option<Error>,
    /// When direct-use mode is active, holds the input root digest and the
    /// RAII pin guard that owns the cache ref_count. The digest drives the
    /// work-symlink cleanup branch in `do_cleanup`; the guard's Drop
    /// releases the pin synchronously on any exit path (success, error,
    /// panic, cancellation), including before this slot is populated (the
    /// guard lives on the async stack of `inner_prepare_action` from
    /// fetch_add to the hand-off at `state.direct_use_pin = Some(...)`).
    /// None means normal hardlink mode. #57 §2 hand-off seam.
    direct_use_pin: Option<(DigestInfo, crate::directory_cache::DirectoryCachePinGuard)>,
    /// Calibration probe P-A field (observability-only): total resolved
    /// input-tree bytes, carried from staging (`download_to_directory`'s
    /// `total_bytes`) to the post-upload P-A emit site. `None` until staging
    /// populates it (e.g. directory-only trees that early-return, or the
    /// direct-use cache-hit path which never calls `download_to_directory`).
    /// Scalar `u64`; no buffer, no cap annotation needed.
    calib_input_bytes: Option<u64>,
    /// Calibration probe P-A field (observability-only): the just-completed
    /// child's CPU time (user+system) in ms, captured best-effort at child
    /// `wait()` and carried to the post-upload P-A emit site (the child is
    /// reaped by then, so it cannot be re-queried). `None` when the per-OS
    /// query was unavailable. See `calib_capture_cpu_time_ms`. Scalar.
    calib_cpu_time_ms: Option<u64>,
    /// Calibration probe P-A field (observability-only): the worker-side
    /// in-flight action count (`running_actions.lock().len()`) sampled at
    /// execute start, carried to the post-upload P-A emit site. Includes self
    /// (so >= 1); this is the WORKER's view, NOT the scheduler dispatch-count.
    /// `None` until execute start populates it. Scalar.
    calib_running_at_start: Option<usize>,
}

#[derive(Debug)]
pub struct RunningActionImpl {
    operation_id: OperationId,
    action_directory: String,
    work_directory: String,
    action_info: ActionInfo,
    timeout: Duration,
    running_actions_manager: Arc<RunningActionsManagerImpl>,
    state: Mutex<RunningActionImplState>,
    has_manager_entry: AtomicBool,
    did_cleanup: AtomicBool,
    /// AC-poisoning fix residual-window guard. Set by `kill_operation`
    /// to suppress AC writes after a kill arrives in the residual
    /// window between child-exit and `cache_action_result`. Composes
    /// with the existing `kill_channel_tx` (which wakes the
    /// `tokio::select!` arm during child-process wait): the channel
    /// covers the window during execute; this flag covers the gap
    /// after that arm has returned. Only ever transitions false →
    /// true; once set, stays set for the action's lifetime. The
    /// publish closure at `local_worker.rs:~2474` reads this via a
    /// captured `Arc<RunningActionImpl>` (Arc-capture refactor — IC1
    /// in v3-final design) so cleanup removing the `running_actions`
    /// map entry cannot race with the read.
    pub(crate) cancelled: AtomicBool,
    /// Awaitable companion to `cancelled` for the upload tail.
    ///
    /// `kill_channel_tx`/`kill_channel_rx` (the oneshot) is consumed by
    /// `inner_execute` to preempt the child-process wait. Once the child
    /// has exited that receiver is gone, so a kill arriving DURING the
    /// upload tail has no awaitable signal — only the `cancelled`
    /// AtomicBool, which nothing in the upload path polls. Without this a
    /// kill that lands after child-exit was ignored for the full
    /// `max_upload_timeout` (600s).
    ///
    /// `kill_operation` calls `notify_one()` here (storing a permit if no
    /// waiter is parked yet), so `upload_results`'s `select!` kill arm
    /// fires whether the kill arrives before the arm subscribes or while
    /// the upload is in-flight. Composes with `cancelled`: the flag is the
    /// durable record (read by the AC-poisoning publish guard), this is
    /// the wakeup edge for the upload tail.
    kill_notify: Notify,
    /// Pre-resolved directory tree from the scheduler (if provided in
    /// StartExecute). Used once during prepare_action to skip the GetTree
    /// RPC, then taken (dropped) to free memory.
    pre_resolved_tree: Mutex<Option<HashMap<DigestInfo, ProtoDirectory>>>,
    /// Server-provided hints about which input digests the worker is
    /// believed to be missing. Used once during prepare_action to skip
    /// the has_with_results round-trip, then taken (dropped) to free memory.
    server_missing_digests: Mutex<Option<HashSet<DigestInfo>>>,
    /// #O3/O13: per-action cache of Tree protos just written by
    /// `inner_upload_results` so the publish-side readers
    /// (`expand_tree_file_digests`, `spawn_upload_to_remote`) can read
    /// them from memory instead of re-decoding through the storage layer.
    /// Populated immediately after `serialize_and_upload_message`; consumed
    /// twice on the publish path (peek by `expand_tree_file_digests`, take
    /// by `spawn_upload_to_remote`). A cache miss falls back to
    /// `get_and_decode_digest` (correctness-safe).
    ///
    /// UNBOUNDED-OK: scope is bound by RunningActionImpl::Drop. The cache
    /// is populated only by this action's own `inner_upload_results`; the
    /// number of entries is bounded by `action_result.output_folders.len()`
    /// (Bazel `output_paths` hint), typically O(1..10). The whole HashMap
    /// is dropped when `RunningActionImpl` drops — on success, error,
    /// cancel, or panic — so no process-wide leak class exists.
    tree_proto_cache: Mutex<HashMap<DigestInfo, ProtoTree>>,
}

impl RunningActionImpl {
    pub fn new(
        execution_metadata: ExecutionMetadata,
        operation_id: OperationId,
        action_directory: String,
        action_info: ActionInfo,
        timeout: Duration,
        running_actions_manager: Arc<RunningActionsManagerImpl>,
        pre_resolved_tree: Option<HashMap<DigestInfo, ProtoDirectory>>,
        server_missing_digests: Option<HashSet<DigestInfo>>,
    ) -> Self {
        let work_directory = format!("{}/{}", action_directory, "work");
        let (kill_channel_tx, kill_channel_rx) = oneshot::channel();
        Self {
            operation_id,
            action_directory,
            work_directory,
            action_info,
            timeout,
            running_actions_manager,
            state: Mutex::new(RunningActionImplState {
                command_proto: None,
                kill_channel_rx: Some(kill_channel_rx),
                kill_channel_tx: Some(kill_channel_tx),
                execution_result: None,
                action_result: None,
                execution_metadata,
                error: None,
                direct_use_pin: None,
                calib_input_bytes: None,
                calib_cpu_time_ms: None,
                calib_running_at_start: None,
            }),
            // Always need to ensure that we're removed from the manager on Drop.
            has_manager_entry: AtomicBool::new(true),
            // Only needs to be cleaned up after a prepare_action call, set there.
            did_cleanup: AtomicBool::new(true),
            // AC-poisoning fix: residual-window guard, set by kill_operation.
            cancelled: AtomicBool::new(false),
            // Wakeup edge for the upload-tail kill arm (Gap 2). Notified by
            // kill_operation alongside `cancelled`.
            kill_notify: Notify::new(),
            pre_resolved_tree: Mutex::new(pre_resolved_tree),
            server_missing_digests: Mutex::new(server_missing_digests),
            // #O3/O13 per-action Tree-proto cache: lifetime = this action.
            // Drop fires on every termination path so no leak class exists.
            tree_proto_cache: Mutex::new(HashMap::new()),
        }
    }

    #[allow(
        clippy::missing_const_for_fn,
        reason = "False positive on stable, but not on nightly"
    )]
    fn metrics(&self) -> &Arc<Metrics> {
        &self.running_actions_manager.metrics
    }

    /// Test-only: set `cancelled` WITHOUT firing `kill_notify`. Production
    /// kills go through `kill_operation`, which sets `cancelled` AND stores
    /// a `kill_notify` permit. This setter isolates the `upload_results`
    /// kill-arm FAST PATH (`if !cancelled` at the top of `kill_fut`): with a
    /// stored permit, the `notified().await` would resolve immediately even
    /// if the fast-path check were removed, masking a regression. Setting
    /// `cancelled` alone lets a test prove the durable-flag check (not the
    /// notify permit) is what preempts an upload for an action that was
    /// already cancelled before `upload_results` first polled.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_cancelled_for_test(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// #O3/O13: cache a Tree proto just written to CAS so the publish-side
    /// readers can skip the storage-layer re-decode. Per-action scope —
    /// entries are dropped automatically when this `RunningActionImpl`
    /// drops (success, error, cancel, or panic).
    #[doc(hidden)]
    pub fn cache_tree_proto(&self, digest: DigestInfo, tree: ProtoTree) {
        self.tree_proto_cache.lock().insert(digest, tree);
    }

    /// #O3/O13: clone a previously-cached Tree proto without removing it.
    /// Used by `expand_tree_file_digests` (reader 1), which runs before
    /// `spawn_upload_to_remote` (reader 2) and must leave the entry for
    /// the second reader. `None` on miss; callers MUST fall back to
    /// `get_and_decode_digest` to preserve correctness.
    #[doc(hidden)]
    pub fn peek_cached_tree_proto(&self, digest: &DigestInfo) -> Option<ProtoTree> {
        self.tree_proto_cache.lock().get(digest).cloned()
    }

    /// #O3/O13: take a previously-cached Tree proto, removing the entry.
    /// Used by `spawn_upload_to_remote` (reader 2 / final reader). `None`
    /// on miss; callers MUST fall back to `get_and_decode_digest` to
    /// preserve correctness.
    #[doc(hidden)]
    pub fn take_cached_tree_proto(&self, digest: &DigestInfo) -> Option<ProtoTree> {
        self.tree_proto_cache.lock().remove(digest)
    }

    /// Prepares any actions needed to execute this action. This action will do the following:
    ///
    /// * Download any files needed to execute the action
    /// * Build a folder with all files needed to execute the action.
    ///
    /// This function will aggressively download and spawn potentially thousands of futures. It is
    /// up to the stores to rate limit if needed.
    fn inner_prepare_action(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<Self>, Error>> {
        Box::pin(async move {
        let operation_id = self.operation_id.clone();
        info!(%operation_id, "inner_prepare_action: entered");
        // #36 Phase 6 §6 Phase 0 probe P-WORKER-FETCH-START: mark the
        // wall-clock at which this action's input-fetch begins, in the
        // same epoch-micros format as P-WORKER-BOUNDARY. Together the
        // pair lets a log scan compute (worker, op_id_n, op_id_n+1) →
        // boundary-to-fetch-start latency, which is the wall-clock Phase 6
        // would hide. Observability only, no behaviour change.
        let phase6_fetch_start_at_us = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        info!(
            tag = "phase6_input_fetch_start",
            op_id = %operation_id,
            fetch_start_at_us = phase6_fetch_start_at_us,
            "phase6 input fetch starting"
        );
        {
            let mut state = self.state.lock();
            state.execution_metadata.input_fetch_start_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
        }
        // O5: Overlap output-directory prep [C] with input download [B2].
        //
        // The dependency graph is:
        //   [A] = fetch Command proto (small blob, fast, ~1-5ms)
        //   [B1] = create work_directory (one mkdir, microseconds)
        //   [B2] = prepare_action_inputs — resolves input tree, downloads missing
        //          blobs, hardlinks files into work_directory (~5-100ms on cache hit)
        //   [C]  = prepare_output_directories — creates parent dirs for declared
        //          outputs (O(output_paths) * mkdir, ~0.1-2ms total, zero dependency
        //          on [B2] result — only needs command.{output_files,output_paths,
        //          working_directory} from [A])
        //
        // Shape (normal mode !is_direct_use):
        //   [A] alone → [B1] alone → [B2] alone → [C]
        //   ([B2] materialises into the EMPTY work_dir, THEN [C] creates the
        //   output-path parent dirs.)
        //
        // #clonefile-fallback (reverts the O5 [B2]∥[C] overlap, commit 333ce15e):
        //   [B2]'s DirectoryCache-hit materialise bottoms out in
        //   `hardlink_directory_tree` → `try_clonefile` (macOS), which requires
        //   an empty/absent dst. The O5 overlap ran [C] concurrently with [B2]
        //   into the SAME work_dir, so [C] pre-created output dirs (`bazel-out/`)
        //   before the materialise ran → non-empty dst → clonefile ALWAYS
        //   preempted → every action fell back to the ~600ms per-file hardlink
        //   (`dir_cache_hit_clonefile_total = 0` fleet-wide). Serialising [B2]
        //   before [C] restores the empty-dst precondition so the ~1ms
        //   whole-tree clonefile(2) fires.
        //
        //   Chesterton / net tradeoff: O5 (333ce15e) hid [C]'s mkdir behind the
        //   input download for an UNMEASURED design estimate of ~2-10ms
        //   (data_plane_bench has zero action-prep cells; the in-process FU-8
        //   measurement was confounded by spawn_blocking contention). The
        //   clonefile it blocked is ~600ms measured (the sole
        //   `record_hit_assemble_ms` site). Dropping the overlap to unblock
        //   clonefile is a net-~600ms win on the critical path (hit AND
        //   post-miss-construct), far exceeding the ~2-10ms overlap it gives up.
        //
        // Safety: the O5 prereq (commit 21e51dce / 6305159d) that makes the
        // materialise BFS tolerate AlreadyExists-on-directory is retained (it is
        // now a pure no-op since [C] no longer pre-populates in normal mode, but
        // it still guards the direct-use path and any future concurrency).
        //
        // Direct-use mode is unchanged: try_join([A],[B]) → [C] sequential;
        // work_directory is a symlink created by get_or_create_direct before [C]
        // runs. [C] is created in the shared post-block below in BOTH modes.
        let command_digest = self.action_info.command_digest;
        let is_direct_use = self.running_actions_manager.directory_cache
            .as_ref()
            .map_or(false, |c| c.is_direct_use_mode());

        // Calibration probe P-A: sink for the staged input-tree bytes, written
        // by `download_to_directory` on the fallback path and carried into
        // `state.calib_input_bytes` after staging. Stays 0 on a directory-cache
        // hit (no `download_to_directory` call → ~0 fetched bytes).
        // Observability-only; nothing on the execution path reads it.
        let calib_input_bytes = core::sync::atomic::AtomicU64::new(0);

        // [A]: Fetch the Command proto. In normal mode we need it before
        // launching [C] and [B2] separately; in direct-use mode we put it
        // back in a try_join with [B] (unchanged pre-O5 shape).
        let command;
        let direct_use_pin;

        if is_direct_use {
            // Direct-use mode: original shape — try_join([A],[B]) → [C].
            // [C] runs after the join so get_or_create_direct has already
            // established work_directory as a symlink before [C]'s
            // create_dir_all runs. Do NOT apply the overlap here.
            let op_id_for_cmd = operation_id.clone();
            let command_fut = self.metrics().get_proto_command_from_store.wrap(async {
                info!(%op_id_for_cmd, ?command_digest, "inner_prepare_action: command_fut entered (direct-use)");
                let res = get_and_decode_digest::<ProtoCommand>(
                    self.running_actions_manager.cas_store.as_ref(),
                    command_digest.into(),
                )
                .await
                .err_tip(|| "Converting command_digest to Command")
                .map_err(|mut e| {
                    if e.code == Code::NotFound {
                        e.details.push(make_precondition_failure_any(command_digest));
                    }
                    e
                });
                info!(%op_id_for_cmd, ?command_digest, ok = res.is_ok(), "inner_prepare_action: command_fut complete (direct-use)");
                res
            });
            let filesystem_store_pin =
                Pin::new(self.running_actions_manager.filesystem_store.as_ref());
            let pre_resolved_tree = self.pre_resolved_tree.lock().take();
            let server_missing_digests = self.server_missing_digests.lock().take();
            let op_id_for_inputs = operation_id.clone();
            info!(%operation_id, "inner_prepare_action: try_join(command_fut, prepare_action_inputs) [direct-use, no overlap]");
            let (cmd, pin) = try_join(command_fut, async {
                info!(%op_id_for_inputs, "inner_prepare_action: prepare_action_inputs branch entered (direct-use)");
                // Direct-use: work_directory is created as a symlink by
                // get_or_create_direct; did_cleanup is set after.
                self.did_cleanup.store(false, Ordering::Release);
                let res = self.metrics()
                    .download_to_directory
                    .wrap(prepare_action_inputs(
                        &self.running_actions_manager.directory_cache,
                        &self.running_actions_manager.cas_store,
                        filesystem_store_pin,
                        &self.action_info.input_root_digest,
                        &self.work_directory,
                        pre_resolved_tree,
                        server_missing_digests,
                        Some(&calib_input_bytes),
                    ))
                    .await;
                info!(%op_id_for_inputs, ok = res.is_ok(), "inner_prepare_action: prepare_action_inputs branch complete (direct-use)");
                res
            })
            .await?;
            command = cmd;
            direct_use_pin = pin;
        } else {
            // Normal mode (#clonefile-fallback): [A] → [B1] → [B2] → [C].
            // [B2] materialises into the empty work_dir before [C] creates
            // output dirs, so the macOS clonefile(2) fast path can fire.
            let op_id_for_cmd = operation_id.clone();
            info!(%operation_id, "inner_prepare_action: fetching command [A] alone (materialise-first, normal mode)");
            let cmd: ProtoCommand = self.metrics().get_proto_command_from_store.wrap(async {
                info!(%op_id_for_cmd, ?command_digest, "inner_prepare_action: command_fut entered");
                let res = get_and_decode_digest::<ProtoCommand>(
                    self.running_actions_manager.cas_store.as_ref(),
                    command_digest.into(),
                )
                .await
                .err_tip(|| "Converting command_digest to Command")
                .map_err(|mut e| {
                    // REAPI v2 §2.2.4: a missing Command must be surfaced
                    // with a PreconditionFailure MISSING violation so Bazel
                    // can re-upload the blob. Input files get this detail
                    // attached in download_to_directory; the command path
                    // previously returned NotFound with empty details.
                    if e.code == Code::NotFound {
                        e.details.push(make_precondition_failure_any(command_digest));
                    }
                    e
                });
                info!(%op_id_for_cmd, ?command_digest, ok = res.is_ok(), "inner_prepare_action: command_fut complete");
                res
            })
            .await?;

            // [B1]: Create the (empty) work_directory before [B2] materialises
            // into it. [B2]'s clonefile(2) fast path (macOS) removes this empty
            // dir and re-creates it as the CoW clone root; a non-empty dir would
            // preempt clonefile (see #clonefile-fallback note above).
            info!(%operation_id, "inner_prepare_action: creating work_directory [B1]");
            fs::create_dir(&self.work_directory)
                .await
                .err_tip(|| format!("Error creating work directory {}", self.work_directory))?;
            // Mark cleanup needed once the directory exists.
            self.did_cleanup.store(false, Ordering::Release);

            // #clonefile-fallback: materialise the input tree [B2] into the
            // EMPTY work_directory FIRST, then create the output-only dirs [C]
            // AFTER (unified post-block, below). The prior O5 overlap ran [C]
            // concurrently with [B2] into the same work_directory, so [C]
            // pre-created output dirs (e.g. `bazel-out/`) before [B2]'s
            // materialise ran — leaving the work dir non-empty. On macOS
            // `try_clonefile` requires an empty/absent dst, so the whole-tree
            // clonefile(2) fast path (~1ms CoW) was ALWAYS preempted and every
            // action fell back to the ~600ms per-file hardlink
            // (`dir_cache_hit_clonefile_total = 0` fleet-wide). Serialising the
            // materialise before the output-dir mkdir restores the empty-dst
            // precondition. See the O5 Chesterton note in the commit message:
            // the overlap saved an unmeasured ~2-10ms; the clonefile it blocked
            // saves ~600ms — a net-huge win even dropping the overlap entirely.
            let filesystem_store_pin =
                Pin::new(self.running_actions_manager.filesystem_store.as_ref());
            let pre_resolved_tree = self.pre_resolved_tree.lock().take();
            let server_missing_digests = self.server_missing_digests.lock().take();

            // [B2]: download/materialise input tree into the empty work_dir.
            info!(%operation_id, "inner_prepare_action: prepare_action_inputs [B2] into empty work_dir (materialise-first)");
            let pin = self.metrics()
                .download_to_directory
                .wrap(prepare_action_inputs(
                    &self.running_actions_manager.directory_cache,
                    &self.running_actions_manager.cas_store,
                    filesystem_store_pin,
                    &self.action_info.input_root_digest,
                    &self.work_directory,
                    pre_resolved_tree,
                    server_missing_digests,
                    Some(&calib_input_bytes),
                ))
                .await?;
            info!(%operation_id, "inner_prepare_action: prepare_action_inputs [B2] complete (materialise-first)");

            command = cmd;
            direct_use_pin = pin;
        }

        // Hand-off seam (#57 §4): the guard moves from this async stack
        // into `state.direct_use_pin`. There is no `.await` between
        // `direct_use_pin` (the local) going out of scope and the
        // assignment, so the ARMED guard is transferred atomically from
        // any cancellation point. If the input-materialise (normal mode: the
        // `?` on [B2]; direct-use mode: the `try_join`) returned Err, the local
        // never bound — the guard was dropped on the unwinding stack inside the
        // materialise future, firing fetch_sub.
        if let Some((digest, pin_guard)) = direct_use_pin {
            let mut state = self.state.lock();
            state.direct_use_pin = Some((digest, pin_guard));
        }
        // [C]: create the output-path parent directories AFTER the input-tree
        // materialise [B2], in BOTH modes.
        //
        // #clonefile-fallback: normal mode now creates output dirs here
        // (post-materialise) instead of concurrently with [B2] — so [B2]'s
        // materialise ran against an empty work_dir and the macOS clonefile(2)
        // fast path can fire. `prepare_output_directory` bottoms out in
        // idempotent `create_dir_all`, so any input-tree directory the clone
        // already brought is a no-op; the output-only dirs get created. This is
        // exactly the sequence direct-use mode already used post-join.
        //
        // (Direct-use mode is unchanged: work_directory is a symlink into the
        // cache created by get_or_create_direct; output dirs are created inside
        // that symlinked tree here, as before.)
        {
            // Create all directories needed for our output paths.
            let work_dir_for_output = self.work_directory.clone();
            // Mutex serializes the slow-path symlink replacement to avoid
            // concurrent tasks racing on the same symlink (EEXIST / ENOENT).
            let symlink_fix_lock = Arc::new(tokio::sync::Mutex::new(()));
            // #86: O14 counters are now routed to the process-global
            // symlink_fix_counters() singleton in nativelink_util::o11_probes,
            // registered with MetricsRegistry — no per-instance metrics clone
            // needed.
            let working_directory_for_output = command.working_directory.clone();
            let prepare_output_directories = |output_file: &String| {
                let work_dir = work_dir_for_output.clone();
                let lock = symlink_fix_lock.clone();
                let working_directory = working_directory_for_output.clone();
                let output_file = output_file.clone();
                async move {
                    prepare_output_directory(
                        &work_dir,
                        &working_directory,
                        &output_file,
                        &lock,
                    )
                    .await
                }
            };
            self.metrics()
                .prepare_output_files
                .wrap(try_join_all(
                    command.output_files.iter().map(prepare_output_directories),
                ))
                .await?;
            self.metrics()
                .prepare_output_paths
                .wrap(try_join_all(
                    command.output_paths.iter().map(prepare_output_directories),
                ))
                .await?;
        }
        // Log command args but NOT environment_variables — they may contain secrets.
        debug!(
            args = ?command.arguments,
            output_paths = ?command.output_paths,
            working_directory = ?command.working_directory,
            "Worker received command"
        );
        {
            let mut state = self.state.lock();
            state.command_proto = Some(command);
            state.execution_metadata.input_fetch_completed_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
            // Calibration probe P-A: carry the staged input-tree bytes into
            // state for the post-upload P-A record. `Relaxed` is sufficient —
            // the staging future has completed (try_join awaited) before this
            // read, establishing happens-before via the await point.
            state.calib_input_bytes =
                Some(calib_input_bytes.load(core::sync::atomic::Ordering::Relaxed));
        }
        Ok(self)
        })
    }

    async fn inner_execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        // Calibration probe P-A: snapshot the worker-side in-flight action count
        // at execute start. Taken BEFORE the `state` lock to avoid nesting the
        // `running_actions` mutex under `state` (lock-ordering discipline —
        // this file documents a Mutex/watch deadlock class). Includes self
        // (>= 1); worker-side view, NOT the scheduler dispatch-count.
        let calib_running_at_start = self.running_actions_manager.running_actions.lock().len();
        let (command_proto, mut kill_channel_rx) = {
            let mut state = self.state.lock();
            state.execution_metadata.execution_start_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
            state.calib_running_at_start = Some(calib_running_at_start);
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .kill_channel_rx
                    .take()
                    .err_tip(|| "Expected state to have kill_channel_rx in execute()")?
                    // This is important as we may be killed at any point.
                    .fuse(),
            )
        };
        if command_proto.arguments.is_empty() {
            return Err(make_input_err!("No arguments provided in Command proto"));
        }
        let args: Vec<&OsStr> = if let Some(entrypoint) = &self
            .running_actions_manager
            .execution_configuration
            .entrypoint
        {
            core::iter::once(entrypoint.as_ref())
                .chain(command_proto.arguments.iter().map(AsRef::as_ref))
                .collect()
        } else {
            command_proto.arguments.iter().map(AsRef::as_ref).collect()
        };
        // TODO(palfrey): This should probably be in debug, but currently
        //                    that's too busy and we often rely on this to
        //                    figure out toolchain misconfiguration issues.
        //                    De-bloat the `debug` level by using the `trace`
        //                    level more effectively and adjust this.
        info!(?args, "Executing command",);

        let mut command_builder = process::Command::new(args[0]);
        command_builder
            .args(&args[1..])
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(format!(
                "{}/{}",
                self.work_directory, command_proto.working_directory
            ))
            .env_clear();

        let requested_timeout = if self.action_info.timeout.is_zero() {
            self.running_actions_manager.max_action_timeout
        } else {
            self.action_info.timeout
        };

        let mut maybe_side_channel_file: Option<Cow<'_, OsStr>> = None;
        if let Some(additional_environment) = &self
            .running_actions_manager
            .execution_configuration
            .additional_environment
        {
            for (name, source) in additional_environment {
                let value = match source {
                    EnvironmentSource::Property(property) => self
                        .action_info
                        .platform_properties
                        .get(property)
                        .map_or_else(|| Cow::Borrowed(""), |v| Cow::Borrowed(v.as_str())),
                    EnvironmentSource::Value(value) => Cow::Borrowed(value.as_str()),
                    EnvironmentSource::FromEnvironment => {
                        Cow::Owned(env::var(name).unwrap_or_default())
                    }
                    EnvironmentSource::TimeoutMillis => {
                        Cow::Owned(requested_timeout.as_millis().to_string())
                    }
                    EnvironmentSource::SideChannelFile => {
                        let file_cow =
                            format!("{}/{}", self.action_directory, Uuid::new_v4().simple());
                        maybe_side_channel_file = Some(Cow::Owned(file_cow.clone().into()));
                        Cow::Owned(file_cow)
                    }
                    EnvironmentSource::ActionDirectory => {
                        Cow::Borrowed(self.action_directory.as_str())
                    }
                };
                command_builder.env(name, value.as_ref());
            }
        }

        #[cfg(target_family = "unix")]
        let envs = &command_proto.environment_variables;
        // If SystemRoot is not set on windows we set it to default. Failing to do
        // this causes all commands to fail.
        #[cfg(target_family = "windows")]
        let envs = {
            let mut envs = command_proto.environment_variables.clone();
            if !envs.iter().any(|v| v.name.to_uppercase() == "SYSTEMROOT") {
                envs.push(
                    nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable {
                        name: "SystemRoot".to_string(),
                        value: "C:\\Windows".to_string(),
                    },
                );
            }
            if !envs.iter().any(|v| v.name.to_uppercase() == "PATH") {
                envs.push(
                    nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable {
                        name: "PATH".to_string(),
                        value: "C:\\Windows\\System32".to_string(),
                    },
                );
            }
            envs
        };
        for environment_variable in envs {
            command_builder.env(&environment_variable.name, &environment_variable.value);
        }

        let mut child_process = command_builder
            .spawn()
            .err_tip(|| format!("Could not execute command {args:?}"))?;
        let mut stdout_reader = child_process
            .stdout
            .take()
            .err_tip(|| "Expected stdout to exist on command this should never happen")?;
        let mut stderr_reader = child_process
            .stderr
            .take()
            .err_tip(|| "Expected stderr to exist on command this should never happen")?;

        // Calibration probe P-A: capture the child's OS PID BEFORE the child is
        // moved into the cleanup guard and (on completion) reaped by tokio's
        // SIGCHLD reaper. The PID feeds the `cpu_time_ms` poll below; once the
        // child is reaped the value is gone (`proc_pidinfo` → ESRCH → `None`),
        // which is why the poll runs DURING execution rather than after `wait()`.
        // Observability-only; `None` if the OS did not assign a queryable PID.
        let calib_child_pid: Option<u32> = child_process.id();

        // Calibration probe P-A: arm the child CPU-time poll UPFRONT (the after-
        // `wait()` capture was structurally always `None` — the reap had already
        // happened by then). Eligible iff the child has a queryable PID AND the
        // action is in the uniform 1/16 sample (`calib_should_arm_poll` — the
        // >60 s exec override cannot participate here, its duration is not yet
        // known; large actions NOT in the uniform sample therefore emit P-A with
        // `cpu_time_ms = None`, acceptable: the uniform 1/16 still yields a
        // representative cpu-shape sample including large actions proportionally,
        // and this is far cheaper than polling every action). While eligible we
        // spawn a background task that every 250 ms reads the LIVE child's whole
        // DESCENDANT-SUBTREE CPU (`calib_capture_subtree_cpu_ns`: the target task
        // plus every forked descendant — rustc→rust-lld, cc-wrapper→cc1 — that
        // `proc_pidinfo` alone would miss) off the tokio worker (`spawn_blocking!`),
        // keeping the last accumulated ns in shared scalar atoms (no owned bytes
        // → no cap annotation). The per-pid `HashMap` accumulator is task-LOCAL
        // (only its summed value crosses the atom); it survives child exits so
        // sequential subprocesses are all counted.
        //
        // Overhead (honest syscall count for 250 ms + subtree fan-out): only
        // 1/16 of actions arm the poll; each sampled action holds at most ONE
        // inflight `spawn_blocking` at a time (the loop awaits before re-issuing).
        // That one call does the WHOLE subtree walk: per internal node TWO
        // `proc_listchildpids` calls (a NULL sizing call + a fetch) plus one
        // `proc_pidinfo` per pid. A realistic ~30-pid toolchain tree (~10 internal
        // + 20 leaf) is ~70 read-only syscalls at ~2-5 µs each ≈ 140-350 µs per
        // walk, at 4/s. At the 2026-06-16 incident peak (~352 in-flight → ~22
        // sampled poll tasks) that is ≤22 blocking-pool slots held ~250 µs each
        // per 250 ms window ≈ 22 × 250µs / 250ms ≈ 2.2% time-averaged occupancy
        // of the 1024-thread pool — call it ~1-3% typical. That clears the 15.3%
        // (157/1024) the isotope wedge consumed by ~5-13×, AND unlike those
        // recursive `remove_dir_all` deletes these syscalls are FAST read-only
        // queries that do NOT go uninterruptible D-state — they hold a thread for
        // µs, not the unbounded D-state stalls that actually starved the isotope
        // data plane. What bounds the PATHOLOGICAL tail (a fork-bomb-shaped action
        // that also lands in the 1/16 sample) is NOT this point estimate but the
        // caps: CALIB_SUBTREE_MAX_PIDS=512 / _DEPTH=8. The 512-pid worst case is
        // ~1.5k syscalls ≈ 3-8 ms/walk → ~27-68% occupancy — still cap-terminated,
        // non-D-state, 1/16-sampled, and per-action-scoped, so real-world risk is
        // low; the caps, not the ~2% figure, are the tail guarantee. Negligible in
        // the common case.
        //
        // The task is bounded: it self-terminates on the first `None` (ROOT child
        // reaped/gone) and only runs for the 1/16 sampled actions; the `spawn!`
        // drop-guard aborts it on every exit path from this method so it cannot
        // linger.
        let calib_cpu_last = Arc::new(core::sync::atomic::AtomicU64::new(0));
        let calib_cpu_has_value = Arc::new(AtomicBool::new(false));
        let calib_poll_guard = if calib_should_arm_poll(
            calib_child_pid,
            calib_digest_sample_key(&self.action_info.input_root_digest),
        ) {
            let pid = calib_child_pid.expect("calib_should_arm_poll gated pid.is_some()");
            let last = Arc::clone(&calib_cpu_last);
            let has_value = Arc::clone(&calib_cpu_has_value);
            // Task-LOCAL pid→max-cpu-ns accumulator, persisting across ticks so an
            // exited descendant's CPU stays counted. Wrapped in an `Arc<Mutex>`
            // only to hand it into each per-tick `spawn_blocking` closure — no
            // other task touches it, so the mutex is uncontended (task-local by
            // usage, not shared state).
            // UNBOUNDED-OK: task-local pid→cpu-ns accumulator, freed when the poll
            // task ends (action completion — the Arc drops on guard-abort or the
            // None-break; verified it does not outlive the action). It grows with
            // distinct-pids-forked-per-action (tens for a real toolchain tree,
            // ~48 B/entry), NOT process-lifetime, and only 1/16 of actions arm it;
            // the per-tick fan-out is separately capped at CALIB_SUBTREE_MAX_PIDS.
            let subtree_map: Arc<Mutex<HashMap<libc::pid_t, u64>>> =
                Arc::new(Mutex::new(HashMap::new()));
            Some(spawn!("calib_cpu_time_poll", async move {
                calib_poll_cpu_time_loop(
                    Duration::from_millis(250),
                    move || {
                        // Off the tokio worker: the subtree walk is a burst of
                        // µs-scale blocking syscalls. Flatten the JoinError/None
                        // into a plain `None` so a spawn failure ends the poll
                        // cleanly.
                        let map = Arc::clone(&subtree_map);
                        async move {
                            spawn_blocking!("calib_capture_cpu_time", move || {
                                calib_capture_subtree_cpu_ns(pid, &mut map.lock())
                            })
                            .await
                            .ok()
                            .flatten()
                        }
                    },
                    last,
                    has_value,
                )
                .await;
            }))
        } else {
            None
        };

        let mut child_process_guard = guard(child_process, |mut child_process| {
            let result: Result<Option<std::process::ExitStatus>, std::io::Error> =
                child_process.try_wait();
            match result {
                Ok(res) if res.is_some() => {
                    // The child already exited, probably a timeout or kill operation
                }
                result => {
                    error!(
                        ?result,
                        "Child process was not cleaned up before dropping the call to execute(), killing in background spawn."
                    );
                    background_spawn!("running_actions_manager_kill_child_process", async move {
                        child_process.kill().await
                    });
                }
            }
        });

        let all_stdout_fut = spawn!("stdout_reader", async move {
            let mut all_stdout = BytesMut::new();
            loop {
                let sz = stdout_reader
                    .read_buf(&mut all_stdout)
                    .await
                    .err_tip(|| "Error reading stdout stream")?;
                if sz == 0 {
                    break; // EOF.
                }
            }
            Result::<Bytes, Error>::Ok(all_stdout.freeze())
        });
        let all_stderr_fut = spawn!("stderr_reader", async move {
            let mut all_stderr = BytesMut::new();
            loop {
                let sz = stderr_reader
                    .read_buf(&mut all_stderr)
                    .await
                    .err_tip(|| "Error reading stderr stream")?;
                if sz == 0 {
                    break; // EOF.
                }
            }
            Result::<Bytes, Error>::Ok(all_stderr.freeze())
        });
        let mut killed_action = false;

        let timer = self.metrics().child_process.begin_timer();
        let mut sleep_fut = (self.running_actions_manager.callbacks.sleep_fn)(self.timeout).fuse();
        loop {
            tokio::select! {
                () = &mut sleep_fut => {
                    self.running_actions_manager.metrics.task_timeouts.inc();
                    killed_action = true;
                    if let Err(err) = child_process_guard.kill().await {
                        error!(
                            ?err,
                            "Could not kill process in RunningActionsManager for action timeout",
                        );
                    }
                    {
                        let joined_command = args.join(OsStr::new(" "));
                        let command = joined_command.to_string_lossy();
                        info!(
                            seconds = self.action_info.timeout.as_secs_f32(),
                            %command,
                            "Command timed out"
                        );
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), Some(Error::new(
                            Code::DeadlineExceeded,
                            format!(
                                "Command '{}' timed out after {} seconds",
                                command,
                                self.action_info.timeout.as_secs_f32()
                            )
                        )));
                    }
                },
                maybe_exit_status = child_process_guard.wait() => {
                    // Defuse our guard so it does not try to cleanup and make senseless logs.
                    drop(ScopeGuard::<_, _>::into_inner(child_process_guard));
                    let exit_status = maybe_exit_status.err_tip(|| "Failed to collect exit code of process")?;
                    // TODO(palfrey) We should implement stderr/stdout streaming to client here.
                    // If we get killed before the stream is started, then these will lock up.
                    // TODO(palfrey) There is a significant bug here. If we kill the action and the action creates
                    // child processes, it can create zombies. See: https://github.com/tracemachina/nativelink/issues/225
                    let (stdout, stderr) = if killed_action {
                        drop(timer);
                        (Bytes::new(), Bytes::new())
                    } else {
                        timer.measure();
                        let (maybe_all_stdout, maybe_all_stderr) = tokio::join!(all_stdout_fut, all_stderr_fut);
                        (
                            maybe_all_stdout.err_tip(|| "Internal error reading from stdout of worker task")??,
                            maybe_all_stderr.err_tip(|| "Internal error reading from stderr of worker task")??
                        )
                    };

                    let exit_code = exit_status.code().map_or(EXIT_CODE_FOR_SIGNAL, |exit_code| {
                        if exit_code == 0 {
                            self.metrics().child_process_success_error_code.inc();
                        } else {
                            self.metrics().child_process_failure_error_code.inc();
                        }
                        exit_code
                    });

                    info!(?args, "Command complete");

                    let maybe_error_override = if let Some(side_channel_file) = maybe_side_channel_file {
                        process_side_channel_file(side_channel_file.clone(), &args, requested_timeout).await
                        .err_tip(|| format!("Error processing side channel file: {}", side_channel_file.display()))?
                    } else {
                        None
                    };
                    {
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), maybe_error_override);

                        state.command_proto = Some(command_proto);
                        state.execution_result = Some(RunningActionImplExecutionResult{
                            stdout,
                            stderr,
                            exit_code,
                        });
                        state.execution_metadata.execution_completed_timestamp = (self.running_actions_manager.callbacks.now_fn)();
                    }
                    // Calibration probe P-A: harvest the poll's last live subtree
                    // CPU sample (ns). The poll (armed before this loop, for
                    // uniform-1/16 sampled actions) read the LIVE child's subtree
                    // via `proc_pidinfo`; the after-`wait()` capture that used to
                    // live here was always `None` because the child is reaped by
                    // now (ESRCH). `calib_harvest_cpu_time` gates on `has_value`
                    // (Acquire, pairing with the poll loop's Release) so a
                    // never-captured poll harvests `None`, NOT a false `Some(0)`;
                    // it also carries `last`'s happens-before. The stored ns is
                    // converted to ms once here (the record's `cpu_time_ms`).
                    // Dropping the guard aborts the poll task at once so it does
                    // not linger up to the 250 ms interval. Zero behavior change:
                    // no action-EXECUTION decision reads `calib_cpu_time_ms` (only
                    // the P-A log emit consumes it).
                    if let Some(cpu_ns) =
                        calib_harvest_cpu_time(&calib_cpu_has_value, &calib_cpu_last)
                    {
                        self.state.lock().calib_cpu_time_ms = Some(cpu_ns / 1_000_000);
                    }
                    drop(calib_poll_guard);
                    return Ok(self);
                },
                _ = &mut kill_channel_rx => {
                    killed_action = true;
                    if let Err(err) = child_process_guard.kill().await {
                        error!(
                            operation_id = ?self.operation_id,
                            ?err,
                            "Could not kill process",
                        );
                    }
                    {
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), Some(Error::new(
                            Code::Aborted,
                            format!(
                                "Command '{}' was killed by scheduler",
                                args.join(OsStr::new(" ")).to_string_lossy()
                            )
                        )));
                    }
                },
            }
        }
        // Unreachable.
    }

    async fn inner_upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        enum OutputType {
            None,
            File(FileInfo),
            Directory(DirectoryInfo),
            FileSymlink(SymlinkInfo),
            DirectorySymlink(SymlinkInfo),
        }

        let upload_start = std::time::Instant::now();
        debug!(
            operation_id = ?self.operation_id,
            "Worker uploading results - starting",
        );
        let (mut command_proto, execution_result, mut execution_metadata) = {
            let mut state = self.state.lock();
            state.execution_metadata.output_upload_start_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .execution_result
                    .take()
                    .err_tip(|| "Execution result does not exist at upload_results stage")?,
                state.execution_metadata.clone(),
            )
        };
        // F2 kill-switch: when deferred_output_uploads_enabled is true, write
        // outputs to the local fast store (FilesystemStore) only.  The remote
        // slow-store upload is deferred to spawn_upload_to_remote, which runs
        // AFTER execution_complete frees the worker slot.  Completion is then
        // gated only on the local disk write (~1 ms) instead of the remote RPC
        // (p50 ≈ 152 ms, p99 ≈ 720 ms; n=140 actions, worker-01 + worker-02,
        // 2026-06-12; raw data /tmp/workerlifecycle-phase-timing.log).
        //
        // When false (default): upload through the full FastSlowStore so the
        // has() check in upload_file queries the slow store (remote CAS).
        // This prevents skipping uploads for blobs that exist locally but were
        // never persisted to the remote (e.g. prior background upload failed).
        // FastSlowStore::update_with_whole_file writes to both stores.
        //
        // Safety preconditions for deferred mode (all verified live):
        //   (P1) Outputs written to on-disk FilesystemStore (crash-survivable).
        //   (P2) Outputs pinned immediately below (running_actions_manager.rs:3851).
        //   (P3) BlobsAvailable sent before execution_response (#129 ordering).
        //        Primary read path (ByteStream::Read) is protected by the
        //        BlobLocalityMap populated from BlobsAvailable.
        //   (P4) H4 pending-output-locality-registry (#12) is populated
        //        ASYNCHRONOUSLY via the detached AC write task (ac_server.rs:222,
        //        called after execution_complete), NOT before execution_response.
        //        P4 covers CCS completeness checks that arrive after the AC write
        //        lands. Narrow race between execution_response and AC write landing
        //        is covered by Bazel --remote_retries.
        // See .claude/audits/f2-deferred-output-uploads-design-2026-06-12.md.
        //
        // `cas_store_owned` holds the `Store` that lives for the scope of this
        // function; `cas_store` borrows it so the async closures below can capture
        // a single uniform `&Store` reference regardless of which branch is taken.
        let cas_store_owned: Store =
            if self.running_actions_manager.deferred_output_uploads_enabled {
                // Deferred: write to fast store (local FilesystemStore) only.
                self.running_actions_manager.cas_store.fast_store().clone()
            } else {
                // Synchronous (default): write through the full FastSlowStore.
                Store::new(self.running_actions_manager.cas_store.clone())
            };
        let cas_store = &cas_store_owned;
        let hasher = self.action_info.unique_qualifier.digest_function();

        let mut output_path_futures = FuturesUnordered::new();
        let mut output_paths = command_proto.output_paths;
        // Phase 1: Hash all output files — top-level AND files inside
        // declared output_directories — in parallel, then do a single
        // batch has_with_results() call covering all of them.
        //
        // This replaces N individual gRPC round-trips with one batch call.
        // For directory-heavy actions (C++/Rust builds that declare whole
        // output dirs), the old code paid one individual has() per file inside
        // each directory tree (commit 2013977a only walked top-level paths).
        //
        // `prehash_digests`: path → digest, lets upload_file skip re-hashing.
        // `batch_checked`: all digests in the batch (found OR not-found).
        //   upload_file skips the individual has() for any batch_checked digest —
        //   the batch already answered the existence question.
        // `known_existing`: digests the batch confirmed ARE in CAS → skip upload.
        let (known_existing, batch_checked, prehash_digests) = {
            let hash_start = std::time::Instant::now();
            // Single-file hash futures: each returns Option<(path, digest)>.
            let mut file_hash_futures: FuturesUnordered<
                BoxFuture<'static, Result<Option<(OsString, DigestInfo)>, Error>>,
            > = FuturesUnordered::new();
            // Directory-tree hash futures: each returns Vec<(path, digest)>
            // for all files inside that tree.
            let mut dir_hash_futures: FuturesUnordered<
                BoxFuture<'static, Result<Vec<(OsString, DigestInfo)>, Error>>,
            > = FuturesUnordered::new();

            // Build a full path from a relative entry name.
            // Capture work_directory + working_directory by value to avoid
            // borrowing command_proto across the subsequent mutable appends.
            let work_dir = self.work_directory.clone();
            let sub_dir = command_proto.working_directory.clone();
            let make_full_path = |entry: &str| -> OsString {
                OsString::from(if sub_dir.is_empty() {
                    format!("{work_dir}/{entry}")
                } else {
                    format!("{work_dir}/{sub_dir}/{entry}")
                })
            };

            if output_paths.is_empty() {
                // REAPI split available: route without any extra stat syscall.
                // output_files → prehash_single_file (hash-as-file, no dir walk).
                // output_directories → prehash_directory_tree (recursive walk).
                for entry in &command_proto.output_files {
                    file_hash_futures
                        .push(prehash_single_file(make_full_path(entry), hasher).boxed());
                }
                for entry in &command_proto.output_directories {
                    dir_hash_futures
                        .push(prehash_directory_tree(make_full_path(entry), hasher).boxed());
                }
                // Merge for Phase 2 iteration (must happen after Phase 1 routing).
                output_paths
                    .reserve(command_proto.output_files.len() + command_proto.output_directories.len());
                output_paths.append(&mut command_proto.output_files);
                output_paths.append(&mut command_proto.output_directories);
            } else {
                // Legacy REAPI: output_paths is a merged list with no type info.
                // One symlink_metadata per entry to decide file vs directory.
                for entry in &output_paths {
                    let full_path = make_full_path(entry);
                    let is_dir = match fs::symlink_metadata(&full_path).await {
                        Ok(m) => m.is_dir(),
                        Err(_) => {
                            // Missing or unreadable path: skip prehash; Phase 2
                            // will surface the real error or treat it as absent.
                            continue;
                        }
                    };
                    if is_dir {
                        dir_hash_futures
                            .push(prehash_directory_tree(full_path, hasher).boxed());
                    } else {
                        file_hash_futures
                            .push(prehash_single_file(full_path, hasher).boxed());
                    }
                }
            }

            // UNBOUNDED-OK: action-scoped digest metadata; freed after inner_upload_results
            // returns. Bounded by the action's output file count, which is itself bounded
            // by the REAPI Command proto size limit (~4 MiB max proto; typical actions
            // have O(10)–O(10_000) output files; DigestInfo is 40 bytes + OsString ~64
            // bytes per path → well under any practical memory limit per action).
            let mut all_digests = Vec::new();
            // UNBOUNDED-OK: same bound as all_digests above. path → digest map lets
            // upload_file reuse the Phase 1 hash, avoiding a second full-file read.
            let mut path_digests: HashMap<OsString, DigestInfo> = HashMap::new();

            // Drain single-file results.
            while let Some(result) = file_hash_futures.next().await {
                if let Ok(Some((path, digest))) = result {
                    all_digests.push(digest);
                    path_digests.insert(path, digest);
                }
            }

            // Drain directory-tree results (each is a Vec of file pairs).
            while let Some(result) = dir_hash_futures.next().await {
                if let Ok(pairs) = result {
                    for (path, digest) in pairs {
                        all_digests.push(digest);
                        path_digests.insert(path, digest);
                    }
                }
            }

            // Deduplicate digests before the batch call.
            all_digests.sort_unstable();
            all_digests.dedup();

            let mut existing = HashSet::new();
            // UNBOUNDED-OK: same bound as all_digests/path_digests above;
            // contains one DigestInfo (40 bytes) per unique output file in the action.
            let mut checked: HashSet<DigestInfo> = HashSet::new();
            // #N3: In deferred mode (F2) `cas_store` is the local FilesystemStore.
            // The batch has_with_results queries digests for files JUST produced
            // in work_directory — for a NOVEL output (the common case) the digest
            // is not in that store yet, so the batch returns empty and delivers
            // ZERO skips (pure RPC overhead). The batch's only load-bearing side
            // effect is populating `batch_checked`, which suppresses the per-file
            // individual has() inside upload_file
            // (`if !batch_checked.contains(&digest)`). So skip the batch RPC but
            // STILL populate `checked` from all prehashed digests; leave
            // `existing` empty. Downstream is then identical (individual has()
            // still suppressed, all files upload) minus one RPC.
            //
            // The rare case — a prior action produced an identical output digest
            // that is still in the persistent fast store, today caught by the
            // batch as a `known_existing` skip — becomes upload-then-dedup with
            // the same outcome and NO duplicate write. The duplicate write is
            // absorbed by the FilesystemStore `emplace_file` short-circuit
            // (filesystem_store.rs:1274-1280): when the key already exists AND
            // the store is immutable, emplace_file returns Ok BEFORE the rename.
            // F2 PRECONDITION: that short-circuit is gated on
            // `content_is_immutable: true`, set on the fast tier in the deployed
            // worker config (worker.json5:68). The FilesystemStore TYPE is
            // enforced by the constructor downcast; `content_is_immutable: true`
            // is what makes this rare-case dedup free, and is enforced only by
            // config. In the NON-deferred path `cas_store` is the full
            // FastSlowStore, so the batch hits the remote CAS (slow tier) and
            // delivers REAL skips — keep it.
            if !all_digests.is_empty() {
                if self.running_actions_manager.deferred_output_uploads_enabled {
                    // #N3: skip the fast-store batch has() (empty for novel
                    // outputs; prior-present digests dedup via emplace_file);
                    // mark every prehashed digest checked so upload_file
                    // suppresses the individual has() and uploads directly.
                    for digest in &all_digests {
                        checked.insert(*digest);
                    }
                    debug!(
                        total_digests = all_digests.len(),
                        hash_ms = hash_start.elapsed().as_millis() as u64,
                        "upload_results: deferred mode, skipping fast-store batch has() check (empty for novel outputs; prior-present digests dedup via emplace_file)"
                    );
                } else {
                    let store_keys: Vec<StoreKey<'_>> = all_digests.iter()
                        .map(|d| StoreKey::from(*d))
                        .collect();
                    let mut results = vec![None; store_keys.len()];
                    let batch_start = std::time::Instant::now();
                    if let Err(e) = cas_store.has_with_results(&store_keys, &mut results).await {
                        warn!(
                            ?e,
                            "batch has_with_results failed, falling back to individual checks"
                        );
                    } else {
                        for (digest, result) in all_digests.iter().zip(results.iter()) {
                            // All digests in the batch are "checked" — upload_file
                            // skips the individual has() for these.
                            checked.insert(*digest);
                            if result.is_some() {
                                existing.insert(*digest);
                            }
                        }
                        debug!(
                            total_digests = all_digests.len(),
                            already_existing = existing.len(),
                            batch_ms = batch_start.elapsed().as_millis() as u64,
                            hash_ms = hash_start.elapsed().as_millis() as u64,
                            "upload_results: batch has() check completed"
                        );
                    }
                }
            }

            (
                Arc::new(Mutex::new(existing)),
                Arc::new(checked),
                Arc::new(path_digests),
            )
        };

        let digest_uploaders = Arc::new(Mutex::new(HashMap::new()));
        // #O3/O13: Reference to this action so the Tree-proto write sites
        // below can populate the per-action `tree_proto_cache`, letting
        // `expand_tree_file_digests` / `spawn_upload_to_remote` skip the
        // storage-layer re-read of the same Tree proto. Per-action scope
        // means the cache lifetime equals this `RunningActionImpl`; Drop
        // evicts every entry on success, error, cancel, or panic.
        let action: &RunningActionImpl = &self;
        for entry in output_paths {
            let full_path = OsString::from(if command_proto.working_directory.is_empty() {
                format!("{}/{}", self.work_directory, entry)
            } else {
                format!(
                    "{}/{}/{}",
                    self.work_directory, command_proto.working_directory, entry
                )
            });
            let work_directory = &self.work_directory;
            let digest_uploaders = digest_uploaders.clone();
            let known_existing = known_existing.clone();
            let batch_checked = batch_checked.clone();
            let prehash_digests = prehash_digests.clone();
            output_path_futures.push(async move {
                let cached_digest = prehash_digests.get(&full_path).copied();
                let metadata = {
                    let metadata = match fs::symlink_metadata(&full_path).await {
                        Ok(file) => file,
                        Err(e) => {
                            if e.code == Code::NotFound {
                                // In the event our output does not exist, according to the bazel remote
                                // execution spec, we simply ignore it continue.
                                return Result::<OutputType, Error>::Ok(OutputType::None);
                            }
                            return Err(e).err_tip(|| {
                                format!("Could not open file {}", full_path.display())
                            });
                        }
                    };

                    if metadata.is_file() {
                        return Ok(OutputType::File(
                            upload_file(
                                cas_store.as_pin(),
                                &full_path,
                                hasher,
                                metadata,
                                digest_uploaders,
                                known_existing,
                                batch_checked,
                                cached_digest,
                            )
                            .await
                            .map(|mut file_info| {
                                file_info.name_or_path = NameOrPath::Path(entry);
                                file_info
                            })
                            .err_tip(|| format!("Uploading file {}", full_path.display()))?,
                        ));
                    }
                    metadata
                };
                if metadata.is_dir() {
                    Ok(OutputType::Directory(
                        upload_directory(
                            cas_store.as_pin(),
                            &full_path,
                            work_directory,
                            hasher,
                            digest_uploaders,
                            known_existing,
                            batch_checked,
                            prehash_digests,
                        )
                        .and_then(|(root_dir, children)| async move {
                            let tree = ProtoTree {
                                root: Some(root_dir),
                                children: children.into(),
                            };
                            let tree_digest = serialize_and_upload_message(
                                &tree,
                                cas_store.as_pin(),
                                &mut hasher.hasher(),
                            )
                            .await
                            .err_tip(|| format!("While processing {entry}"))?;
                            // #O3/O13: cache the just-written Tree on the
                            // per-action cache so the publish-side readers
                            // can skip the storage re-decode. Move `tree`
                            // in — neither this closure nor `DirectoryInfo`
                            // needs it again.
                            action.cache_tree_proto(tree_digest, tree);
                            Ok(DirectoryInfo {
                                path: entry,
                                tree_digest,
                            })
                        })
                        .await
                        .err_tip(|| format!("Uploading directory {}", full_path.display()))?,
                    ))
                } else if metadata.is_symlink() {
                    // Resolve the symlink to determine what it points to.
                    // Symlinks created by DirectoryCache (absolute paths into
                    // the cache directory) must NOT be uploaded as symlinks —
                    // the target path is worker-local and meaningless to the
                    // client. Instead, follow the symlink and upload the
                    // resolved content (file or directory).
                    let target = fs::read_link(&full_path)
                        .await
                        .err_tip(|| format!("Reading symlink target for {}", full_path.display()))?;
                    let is_absolute_symlink = Path::new(&target).is_absolute();

                    if is_absolute_symlink {
                        // Absolute symlink — resolve and upload contents.
                        match fs::metadata(&full_path).await {
                            Ok(resolved_meta) => {
                                if resolved_meta.is_dir() {
                                    // Upload as directory (Tree proto).
                                    Ok(OutputType::Directory(
                                        upload_directory(
                                            cas_store.as_pin(),
                                            &full_path,
                                            work_directory,
                                            hasher,
                                            digest_uploaders,
                                            known_existing,
                                            batch_checked,
                                            prehash_digests,
                                        )
                                        .and_then(|(root_dir, children)| async move {
                                            let tree = ProtoTree {
                                                root: Some(root_dir),
                                                children: children.into(),
                                            };
                                            let tree_digest = serialize_and_upload_message(
                                                &tree,
                                                cas_store.as_pin(),
                                                &mut hasher.hasher(),
                                            )
                                            .await
                                            .err_tip(|| format!("While processing {entry}"))?;
                                            // #O3/O13: cache the just-written
                                            // Tree (symlinked-dir variant)
                                            // on the per-action cache for
                                            // the publish-side readers.
                                            action.cache_tree_proto(tree_digest, tree);
                                            Ok(DirectoryInfo {
                                                path: entry,
                                                tree_digest,
                                            })
                                        })
                                        .await
                                        .err_tip(|| format!("Uploading symlinked directory {}", full_path.display()))?,
                                    ))
                                } else {
                                    // Upload as file (follow symlink).
                                    Ok(OutputType::File(
                                        upload_file(
                                            cas_store.as_pin(),
                                            &full_path,
                                            hasher,
                                            resolved_meta,
                                            digest_uploaders,
                                            known_existing,
                                            batch_checked,
                                            cached_digest,
                                        )
                                        .await
                                        .map(|mut file_info| {
                                            file_info.name_or_path = NameOrPath::Path(entry);
                                            file_info
                                        })
                                        .err_tip(|| format!("Uploading symlinked file {}", full_path.display()))?,
                                    ))
                                }
                            }
                            Err(e) => {
                                if e.code != Code::NotFound {
                                    return Err(e).err_tip(|| {
                                        format!(
                                            "While resolving absolute symlink {}",
                                            full_path.display()
                                        )
                                    });
                                }
                                Ok(OutputType::None)
                            }
                        }
                    } else {
                        // Relative symlink — action intentionally created it.
                        // Upload as a proper symlink.
                        let output_symlink = upload_symlink(&full_path, work_directory)
                            .await
                            .map(|mut symlink_info| {
                                symlink_info.name_or_path = NameOrPath::Path(entry);
                                symlink_info
                            })
                            .err_tip(|| format!("Uploading symlink {}", full_path.display()))?;
                        match fs::metadata(&full_path).await {
                            Ok(metadata) => {
                                if metadata.is_dir() {
                                    Ok(OutputType::DirectorySymlink(output_symlink))
                                } else {
                                    Ok(OutputType::FileSymlink(output_symlink))
                                }
                            }
                            Err(e) => {
                                if e.code != Code::NotFound {
                                    return Err(e).err_tip(|| {
                                        format!(
                                            "While querying target symlink metadata for {}",
                                            full_path.display()
                                        )
                                    });
                                }
                                Ok(OutputType::FileSymlink(output_symlink))
                            }
                        }
                    }
                } else {
                    Err(make_err!(
                        Code::Internal,
                        "{full_path:?} was not a file, folder or symlink. Must be one.",
                    ))
                }
            });
        }
        let mut output_files = vec![];
        let mut output_folders = vec![];
        let mut output_directory_symlinks = vec![];
        let mut output_file_symlinks = vec![];

        if execution_result.exit_code != 0 {
            let stdout = core::str::from_utf8(&execution_result.stdout).unwrap_or("<no-utf8>");
            let stderr = core::str::from_utf8(&execution_result.stderr).unwrap_or("<no-utf8>");
            error!(
                exit_code = ?execution_result.exit_code,
                stdout = ?stdout[..min(stdout.len(), 1000)],
                stderr = ?stderr[..min(stderr.len(), 1000)],
                "Command returned non-zero exit code",
            );
        }

        let stdout_digest_fut = self.metrics().upload_stdout.wrap(async {
            let start = std::time::Instant::now();
            let data = execution_result.stdout;
            let data_len = data.len();
            let digest = compute_buf_digest(&data, &mut hasher.hasher());
            cas_store
                .update_oneshot(digest, data)
                .await
                .err_tip(|| "Uploading stdout")?;
            let elapsed = start.elapsed();
            info!(
                ?digest,
                size_bytes = data_len,
                elapsed_ms = elapsed.as_millis() as u64,
                throughput_mbps = format!("{:.1}", throughput_mbps(data_len as u64, elapsed)),
                "upload_results: stdout upload completed",
            );
            Result::<DigestInfo, Error>::Ok(digest)
        });
        let stderr_digest_fut = self.metrics().upload_stderr.wrap(async {
            let start = std::time::Instant::now();
            let data = execution_result.stderr;
            let data_len = data.len();
            let digest = compute_buf_digest(&data, &mut hasher.hasher());
            cas_store
                .update_oneshot(digest, data)
                .await
                .err_tip(|| "Uploading  stderr")?;
            let elapsed = start.elapsed();
            info!(
                ?digest,
                size_bytes = data_len,
                elapsed_ms = elapsed.as_millis() as u64,
                throughput_mbps = format!("{:.1}", throughput_mbps(data_len as u64, elapsed)),
                "upload_results: stderr upload completed",
            );
            Result::<DigestInfo, Error>::Ok(digest)
        });

        debug!(
            operation_id = ?self.operation_id,
            num_output_paths = output_path_futures.len(),
            "upload_results: starting stdout/stderr/output_paths uploads",
        );
        let join_start = std::time::Instant::now();
        let upload_result = futures::try_join!(stdout_digest_fut, stderr_digest_fut, async {
            while let Some(output_type) = output_path_futures.try_next().await? {
                match output_type {
                    OutputType::File(output_file) => output_files.push(output_file),
                    OutputType::Directory(output_folder) => output_folders.push(output_folder),
                    OutputType::FileSymlink(output_symlink) => {
                        output_file_symlinks.push(output_symlink);
                    }
                    OutputType::DirectorySymlink(output_symlink) => {
                        output_directory_symlinks.push(output_symlink);
                    }
                    OutputType::None => { /* Safe to ignore */ }
                }
            }
            Ok(())
        });
        drop(output_path_futures);
        debug!(
            operation_id = ?self.operation_id,
            elapsed_ms = join_start.elapsed().as_millis(),
            success = upload_result.is_ok(),
            "upload_results: all uploads completed",
        );
        let (stdout_digest, stderr_digest) = match upload_result {
            Ok((stdout_digest, stderr_digest, ())) => (stdout_digest, stderr_digest),
            Err(e) => return Err(e).err_tip(|| "Error while uploading results"),
        };

        execution_metadata.output_upload_completed_timestamp =
            (self.running_actions_manager.callbacks.now_fn)();
        output_files.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_folders.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        output_file_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_directory_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        let num_output_files = output_files.len();
        let num_output_folders = output_folders.len();

        // Pin all known output digests in the FilesystemStore IMMEDIATELY
        // after upload_results finishes writing them. The blobs were just
        // written via cas_store.update_oneshot(...) (stdout/stderr) and
        // upload_file/serialize_and_upload_message (output files + tree
        // protos) above. Under load — e.g. the post-restart queue surge
        // where many actions complete in quick succession — the EvictingMap
        // can evict these small fresh blobs before spawn_upload_to_remote
        // gets a chance to pin them, producing the symptom
        //   "upload_to_remote: failed to pre-read small blob from fast store ... NotFound".
        // Pinning here closes that eviction window: from "upload completed"
        // to "background upload task running" the blobs are now protected.
        // (Tree-children file digests are still pinned later inside
        // spawn_upload_to_remote, since they require decoding the tree.)
        // The pin call inside spawn_upload_to_remote is preserved as a
        // defense-in-depth idempotent re-pin (refreshes pinned_at).
        {
            let filesystem_store = &self.running_actions_manager.filesystem_store;
            // FL-681 Fix A: in F2 deferred-upload mode the slow-store write
            // is the AUTHORITATIVE upload and bypasses `FastSlowStore::update`,
            // so the digest never enters `in_flight_slow_writes` and the
            // `on_pin_expired`→`failed_slow_writes` retry path is dark. A
            // time-bounded pin would therefore be demoted at the 120s TTL
            // and silently lost. Pin INDEFINITELY (released only by BIS-ack)
            // in deferred mode; keep the time-bounded pin in the synchronous
            // path where the TTL→failed_slow_writes backstop is live.
            let deferred = self
                .running_actions_manager
                .deferred_output_uploads_enabled;
            let pin_one = |digest: &DigestInfo| -> bool {
                pin_deferred_output_digest(filesystem_store, deferred, digest)
            };
            let warn_pin_miss = |digest: &DigestInfo| {
                warn!(
                    %digest,
                    deferred,
                    "pin_digest: blob not in fast store at pin time, eviction race likely"
                );
            };
            for file in &output_files {
                if file.digest.size_bytes() > 0 && !pin_one(&file.digest) {
                    warn_pin_miss(&file.digest);
                }
            }
            for folder in &output_folders {
                if folder.tree_digest.size_bytes() > 0 && !pin_one(&folder.tree_digest) {
                    warn_pin_miss(&folder.tree_digest);
                }
            }
            if stdout_digest.size_bytes() > 0 && !pin_one(&stdout_digest) {
                warn_pin_miss(&stdout_digest);
            }
            if stderr_digest.size_bytes() > 0 && !pin_one(&stderr_digest) {
                warn_pin_miss(&stderr_digest);
            }
        }

        // Calibration probe P-A: total output-blob bytes (output-file digests +
        // output-folder tree digests). Computed BEFORE `output_files`/
        // `output_folders` are moved into `state.action_result` below; mirrors
        // the existing pin-loop accessors (`file.digest`, `folder.tree_digest`).
        let calib_output_bytes: u64 = output_files
            .iter()
            .map(|f| f.digest.size_bytes())
            .chain(output_folders.iter().map(|d| d.tree_digest.size_bytes()))
            .sum();

        // P-A record, built inside the state lock (to read the carried calib
        // fields) but EMITTED after the lock is released to keep the critical
        // section minimal. `None` until populated.
        let mut calib_action_record: Option<CalibActionRecord> = None;

        {
            let mut state = self.state.lock();
            execution_metadata.worker_completed_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();

            // Log phase durations for every action so we can diagnose latency.
            let duration_ms = |start: SystemTime, end: SystemTime| -> i64 {
                end.duration_since(start)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or_else(|e| -(e.duration().as_millis() as i64))
            };
            let em = &execution_metadata;
            let calib_exec_ms =
                duration_ms(em.execution_start_timestamp, em.execution_completed_timestamp);
            info!(
                operation_id = ?self.operation_id,
                queue_ms = duration_ms(em.queued_timestamp, em.worker_start_timestamp),
                input_fetch_ms = duration_ms(em.input_fetch_start_timestamp, em.input_fetch_completed_timestamp),
                execution_ms = calib_exec_ms,
                output_upload_ms = duration_ms(em.output_upload_start_timestamp, em.output_upload_completed_timestamp),
                worker_overhead_ms = duration_ms(em.worker_start_timestamp, em.input_fetch_start_timestamp),
                total_worker_ms = duration_ms(em.worker_start_timestamp, em.worker_completed_timestamp),
                "Action phase timing",
            );

            // Calibration probe P-A: build the action-shape record from the
            // already-computed exec duration + the calib fields carried in
            // state (input bytes from staging, child CPU time from execute,
            // worker in-flight count from execute start). Sampled uniform 1/16
            // by input-root-digest hash, with the 1/1 large-exec override (§3)
            // AND the 1/1 large-payload override (GAP-2) — the staged input
            // bytes are already resolved into `state.calib_input_bytes` here, so
            // the payload floor is applied at THIS gate (not emit-time). Only the
            // populated `Option` is emitted (after the lock).
            let calib_input_bytes = state.calib_input_bytes.unwrap_or(0);
            let calib_key =
                calib_digest_sample_key(&self.action_info.input_root_digest);
            if calib_action_sampled(calib_key, calib_exec_ms, calib_input_bytes) {
                calib_action_record = Some(calib_build_action_record(
                    calib_exec_ms,
                    state.calib_cpu_time_ms,
                    calib_input_bytes,
                    calib_output_bytes,
                    state.calib_running_at_start.unwrap_or(1),
                ));
            }

            state.action_result = Some(ActionResult {
                output_files,
                output_folders,
                output_directory_symlinks,
                output_file_symlinks,
                exit_code: execution_result.exit_code,
                stdout_digest,
                stderr_digest,
                execution_metadata,
                server_logs: HashMap::default(), // TODO(palfrey) Not implemented.
                error: state.error.clone(),
                message: String::new(), // Will be filled in on cache_action_result if needed.
            });
        }
        // Calibration probe P-A emit (`tag="calib_action"`), off the state lock.
        if let Some(record) = calib_action_record {
            record.emit(&self.operation_id);
        }
        debug!(
            operation_id = ?self.operation_id,
            total_elapsed_ms = upload_start.elapsed().as_millis(),
            num_output_files,
            num_output_folders,
            "upload_results: inner_upload_results completed successfully",
        );
        Ok(self)
    }

    async fn inner_get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        let mut state = self.state.lock();
        state
            .action_result
            .take()
            .err_tip(|| "Expected action_result to exist in get_finished_result")
    }
}

impl Drop for RunningActionImpl {
    fn drop(&mut self) {
        if self.did_cleanup.load(Ordering::Acquire) {
            if self.has_manager_entry.load(Ordering::Acquire) {
                drop(
                    self.running_actions_manager
                        .cleanup_action(&self.operation_id),
                );
            }
            return;
        }
        let operation_id = self.operation_id.clone();
        error!(
            %operation_id,
            "RunningActionImpl did not cleanup. This is a violation of the requirements, will attempt to do it in the background."
        );
        let running_actions_manager = self.running_actions_manager.clone();
        let action_directory = self.action_directory.clone();
        // Take the direct_use_pin (digest + guard) from state so the guard's
        // sync Drop releases the cache ref_count when do_cleanup completes.
        let direct_use_pin = self.state.lock().direct_use_pin.take();
        background_spawn!("running_action_impl_drop", async move {
            let Err(err) =
                do_cleanup(&running_actions_manager, &operation_id, &action_directory, direct_use_pin).await
            else {
                return;
            };
            error!(
                %operation_id,
                ?action_directory,
                ?err,
                "Error cleaning up action"
            );
        });
    }
}

impl RunningAction for RunningActionImpl {
    fn get_operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let operation_id = self.operation_id.clone();
        let start = std::time::Instant::now();
        info!(%operation_id, "action: prepare_action starting (input fetch + materialization)");
        let res = self
            .metrics()
            .clone()
            .prepare_action
            .wrap(Self::inner_prepare_action(self))
            .await;
        match &res {
            Ok(_) => info!(
                %operation_id,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "action: prepare_action complete",
            ),
            Err(e) => warn!(%operation_id, ?e, "action: prepare_action failed"),
        }
        res
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let operation_id = self.operation_id.clone();
        let start = std::time::Instant::now();
        info!(%operation_id, "action: execute starting (command spawn)");
        let res = self
            .metrics()
            .clone()
            .execute
            .wrap(Self::inner_execute(self))
            .await;
        match &res {
            Ok(_) => info!(
                %operation_id,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "action: execute complete",
            ),
            Err(e) => warn!(%operation_id, ?e, "action: execute failed"),
        }
        res
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let upload_timeout = self.running_actions_manager.max_upload_timeout;
        let operation_id = self.operation_id.clone();
        info!(
            ?operation_id,
            upload_timeout_s = upload_timeout.as_secs(),
            "upload_results: starting with timeout",
        );
        let metrics = self.metrics().clone();
        // Gap 2: capture an Arc clone BEFORE `self` is moved into
        // `inner_upload_results` so the kill arm can await the action's
        // `kill_notify` / read `cancelled` while the upload is in-flight.
        let kill_action = Arc::clone(&self);
        let upload_fut = metrics
            .upload_results
            .wrap(Self::inner_upload_results(self));

        // Gap 2: preempt an in-flight upload if a kill arrives DURING the
        // upload tail. The oneshot `kill_channel` is already consumed by
        // `inner_execute`, so once the child has exited the only kill
        // signal is `cancelled` (+ its `kill_notify` wakeup edge), which
        // this arm awaits. On kill it abandons the stalled upload and
        // returns `Ok(self)` carrying a terminal `ActionResult{error:
        // Aborted}` — the SAME shape the normal pipeline produces for a
        // kill-during-execute (#1899). This routes the publish closure's
        // Ok arm → `ExecuteResponse(Completed{Aborted})` to the scheduler,
        // which is TERMINAL (`ActionStage::is_finished()`).
        //
        // M1 (cadre fix-up #2): returning `Err(Aborted)` instead would take
        // the publish closure's Err arm → `InternalError(Aborted)` →
        // `UpdateOperationType::UpdateWithError`. The scheduler's
        // `inner_update_operation` (`simple_scheduler_state_manager.rs:837-859`)
        // does NOT treat `Code::Aborted` as terminal: it is neither
        // `ResourceExhausted` (backpressure) nor `FailedPrecondition`
        // (missing inputs), so `attempts += 1` then `ActionStage::Queued`
        // while `attempts <= max_job_retries` — RE-QUEUEING the killed action
        // for a spurious re-execution (the scheduler-side kill path
        // `cancel_operation_internal` only sends `KillOperationRequest`; it
        // never marks the awaited-action finished, so the already-completed
        // guard does not save us). The Ok-arm `Completed{Aborted}` carries an
        // `ActionResult`, so `is_finished()` is true and the op lands
        // terminally with no retry.
        let kill_fut = async move {
            // Fast path: a kill that landed before this future is first
            // polled (e.g. during execute) already set `cancelled`. Without
            // this check we would rely solely on the stored `notify_one`
            // permit; checking the durable flag too makes the preemption
            // robust to permit accounting.
            if !kill_action.cancelled.load(Ordering::Acquire) {
                kill_action.kill_notify.notified().await;
            }
            // Synthesize the terminal result the abandoned upload never got
            // to write. Carry the `Aborted` error that `kill_operation` /
            // `inner_execute`'s kill arm recorded in `state.error`; if (in a
            // race) `state.error` is unset, stamp a fresh `Aborted` so the
            // result always classifies as a killed action.
            {
                let mut state = kill_action.state.lock();
                let error = state.error.take().unwrap_or_else(|| {
                    make_err!(
                        Code::Aborted,
                        "upload_results aborted by kill for operation {:?}",
                        kill_action.operation_id,
                    )
                });
                state.action_result = Some(ActionResult {
                    error: Some(error),
                    ..ActionResult::default()
                });
            }
            Ok(kill_action)
        };

        let stall_warned = AtomicBool::new(false);
        let stall_warn_fut = async {
            let mut elapsed_secs = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                elapsed_secs += 60;
                stall_warned.store(true, Ordering::Relaxed);
                warn!(
                    ?operation_id,
                    elapsed_s = elapsed_secs,
                    timeout_s = upload_timeout.as_secs(),
                    "upload_results: still in progress — possible stall",
                );
            }
        };

        let upload_start = Instant::now();
        let res = tokio::time::timeout(upload_timeout, async {
            tokio::pin!(upload_fut);
            tokio::pin!(stall_warn_fut);
            tokio::pin!(kill_fut);
            tokio::select! {
                // `biased`: poll the upload FIRST every wakeup. The kill arm
                // therefore wins ONLY when the upload is still Pending
                // (genuinely in-flight) — a kill that races a near-complete
                // upload lets the upload finish. BOTH outcomes carry the
                // SAME terminal shape: `Ok(Arc<Self>)` whose `action_result`
                // holds `error: Aborted` (the upload path stamps it from
                // `state.error`; the kill arm synthesizes it). So whether the
                // upload completes-with-Aborted or the kill arm preempts a
                // stalled upload, `get_finished_result` → publish-closure Ok
                // arm → `ExecuteResponse(Completed{Aborted})` is identical and
                // matches #1899's kill-during-execute. M1 (cadre fix-up #2):
                // the kill arm is a preemption of a STALLED upload that
                // PRESERVES the terminal wire shape — not a switch to the
                // Err-arm `InternalError(Aborted)` (which the scheduler would
                // re-queue; see the `kill_fut` doc-comment above).
                biased;
                result = &mut upload_fut => result,
                killed = &mut kill_fut => killed,
                () = &mut stall_warn_fut => unreachable!(),
            }
        })
        .await
        .map_err(|_| {
            make_err!(
                Code::DeadlineExceeded,
                "Upload results timed out after {}s for operation {:?}",
                upload_timeout.as_secs(),
                operation_id,
            )
        })?;
        match &res {
            Ok(_) if stall_warned.load(Ordering::Relaxed) => {
                info!(
                    ?operation_id,
                    elapsed_ms = upload_start.elapsed().as_millis() as u64,
                    "action: upload_results completed after stall",
                );
            }
            Ok(_) => {
                info!(
                    ?operation_id,
                    elapsed_ms = upload_start.elapsed().as_millis() as u64,
                    "action: upload_results complete",
                );
            }
            Err(e) => {
                warn!(?operation_id, ?e, "action: upload_results failed");
            }
        }
        res
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let res = self
            .metrics()
            .clone()
            .cleanup
            .wrap(async move {
                let direct_use_pin = self.state.lock().direct_use_pin.take();
                let result = do_cleanup(
                    &self.running_actions_manager,
                    &self.operation_id,
                    &self.action_directory,
                    direct_use_pin,
                )
                .await;
                self.has_manager_entry.store(false, Ordering::Release);
                self.did_cleanup.store(true, Ordering::Release);
                result.map(move |()| self)
            })
            .await;
        if let Err(ref e) = res {
            warn!(?e, "Error during cleanup");
        }
        res
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        self.metrics()
            .clone()
            .get_finished_result
            .wrap(Self::inner_get_finished_result(self))
            .await
    }

    fn get_work_directory(&self) -> &String {
        &self.work_directory
    }

    /// Returns true once `kill_operation` has set the cancelled flag.
    /// AC-poisoning fix residual-window guard. The publish closure at
    /// `local_worker.rs:~2474` reads this via a captured Arc.
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

pub trait RunningActionsManager: Sync + Send + Sized + Unpin + 'static {
    type RunningAction: RunningAction;

    fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> impl Future<Output = Result<Arc<Self::RunningAction>, Error>> + Send;

    fn cache_action_result(
        &self,
        action_digest: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
        // #37 Phase 2 (Q1): op_id + worker_id threaded through to AC
        // publish failure-path logging at upload_ac_results so the
        // operator can correlate a failed write back to the action.
        op_id: &OperationId,
        worker_id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn kill_all(&self) -> impl Future<Output = ()> + Send;

    fn kill_operation(
        &self,
        operation_id: &OperationId,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Spawn a background task to upload action output blobs from the local
    /// fast store to the remote slow store. No-op by default.
    ///
    /// #O3/O13: `_action` (optional) carries the per-action Tree-proto cache
    /// populated by `inner_upload_results`. Production passes
    /// `Some(&action_for_publish)` so the spawned task can use cached Tree
    /// protos in lieu of storage-layer re-decodes. Tests / stubs pass `None`.
    fn spawn_upload_to_remote(
        self: &Arc<Self>,
        _action_result: &ActionResult,
        _action: Option<&Arc<Self::RunningAction>>,
    ) {
    }

    /// Expand output directory Tree protos and return the contained file digests.
    /// Used to register tree file digests in the locality map before reporting
    /// the execution result, so the server can proxy reads immediately.
    ///
    /// #O3/O13: `_action` (optional) carries the per-action Tree-proto cache
    /// populated by `inner_upload_results`. When `Some`, cache hits are
    /// drained synchronously (no per-folder future allocation). Tests /
    /// stubs pass `None`.
    fn expand_tree_file_digests(
        &self,
        _action_result: &ActionResult,
        _action: Option<&Arc<Self::RunningAction>>,
    ) -> impl Future<Output = Vec<DigestInfo>> + Send {
        std::future::ready(Vec::new())
    }

    fn metrics(&self) -> &Arc<Metrics>;

    /// Returns the CAS FastSlowStore if available, used for server-requested
    /// blob backfill uploads.
    fn get_cas_store(&self) -> Option<Arc<FastSlowStore>> {
        None
    }

    /// Returns this worker's `DirectoryCache` if one is configured. Used by the
    /// speculative pre-fetch (`Update::PrefetchInputs`) path to pre-construct an
    /// action's input-root cache entry ahead of real dispatch. Default `None`
    /// for stubs / workers without a directory cache.
    fn get_directory_cache(&self) -> Option<Arc<crate::directory_cache::DirectoryCache>> {
        None
    }

    /// (FL-681 re-saturation gate) Returns whether this worker's local CAS
    /// `FilesystemStore` indefinite-pin cap is currently saturated. This is the
    /// SAME value the worker-side admission gate checks in
    /// `create_and_add_action` (both read `filesystem_store.indefinite_pin_saturated()`).
    /// The post-action `BlobsAvailable` delta in `LocalWorkerImpl::run` reports
    /// it so the scheduler's matcher does not clobber a prior `true` to `false`
    /// right after an action completes (re-opening the re-saturation spin until
    /// the next heartbeat). Default no-op returns `false` for stubs.
    fn indefinite_pin_saturated(&self) -> bool {
        false
    }

    /// Returns the digests of input root directories cached in the worker's
    /// directory cache. Returns an empty Vec if no directory cache is configured.
    fn cached_directory_digests(&self) -> impl Future<Output = Vec<DigestInfo>> + Send;

    /// Returns ALL subtree digests across all cached directory entries.
    /// Used for the initial full snapshot on (re)connect.
    fn all_subtree_digests(&self) -> impl Future<Output = Vec<DigestInfo>> + Send;

    /// Atomically takes the pending subtree digest changes since the last call.
    /// Returns (added, removed) digest lists and clears the internal state.
    fn take_pending_subtree_changes(
        &self,
    ) -> impl Future<Output = (Vec<DigestInfo>, Vec<DigestInfo>)> + Send;
}

/// A function to get the current system time, used to allow mocking for tests
type NowFn = fn() -> SystemTime;
type SleepFn = fn(Duration) -> BoxFuture<'static, ()>;

/// Functions that may be injected for testing purposes, during standard control
/// flows these are specified by the new function.
#[derive(Clone, Copy)]
pub struct Callbacks {
    /// A function that gets the current time.
    pub now_fn: NowFn,
    /// A function that sleeps for a given Duration.
    pub sleep_fn: SleepFn,
}

impl Debug for Callbacks {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Callbacks").finish_non_exhaustive()
    }
}

/// The set of additional information for executing an action over and above
/// those given in the `ActionInfo` passed to the worker.  This allows
/// modification of the action for execution on this particular worker.  This
/// may be used to run the action with a particular set of additional
/// environment variables, or perhaps configure it to execute within a
/// container.
#[derive(Debug, Default)]
pub struct ExecutionConfiguration {
    /// If set, will be executed instead of the first argument passed in the
    /// `ActionInfo` with all of the arguments in the `ActionInfo` passed as
    /// arguments to this command.
    pub entrypoint: Option<String>,
    /// The only environment variables that will be specified when the command
    /// executes other than those in the `ActionInfo`.  On Windows, `SystemRoot`
    /// and PATH are also assigned (see `inner_execute`).
    pub additional_environment: Option<HashMap<String, EnvironmentSource>>,
}

/// #37 Phase 2 (Q5): cap on the AC BIS-ack pending-acks observability
/// map. See `UploadActionResults::ac_publish_pending_acks` doc-comment
/// for the rationale. 100_000 entries × ~40 bytes = ~4 MB worst-case.
const AC_PUBLISH_PENDING_ACKS_MAX: usize = 100_000;

/// #37 Phase 2 (Q5): how often the BIS-ack timeout reaper walks the
/// pending-acks map. Set to half the configured timeout so worst-case
/// detection latency is 1.5 × timeout (one tick to observe + one tick
/// of pre-existing age).
const fn bis_ack_reaper_interval(timeout: Duration) -> Duration {
    // Floor at 5 s to keep the tick from going pathologically frequent
    // for very small (test-time) timeouts.
    let half = timeout.as_secs() / 2;
    if half < 5 {
        Duration::from_secs(5)
    } else {
        Duration::from_secs(half)
    }
}

/// #37 Phase 2 (Q5 / F5): collect-and-remove expired entries from the
/// pending-acks map under a single lock acquisition. Extracted so
/// tests can drive the same filter logic the spawned reaper uses
/// (rather than re-implementing it inline). Returns `(digest, age)`
/// for each removed entry; the caller emits logs / bumps counters
/// outside the lock to keep the critical section tight.
///
/// `pub` so integration tests in `tests/` can reach it.
pub fn collect_expired_bis_acks(
    map: &mut HashMap<DigestInfo, Instant>,
    now: Instant,
    timeout: Duration,
) -> Vec<(DigestInfo, Duration)> {
    let expired: Vec<(DigestInfo, Duration)> = map
        .iter()
        .filter_map(|(d, t)| {
            let age = now.saturating_duration_since(*t);
            (age >= timeout).then(|| (*d, age))
        })
        .collect();
    for (digest, _) in &expired {
        map.remove(digest);
    }
    expired
}

/// #37 Phase 2 (Q5): spawn the BIS-ack timeout reaper task. Periodically
/// scans `ac_publish_pending_acks` for entries older than `timeout`,
/// emitting an `error!` log + `worker_bis_ack_missing` counter
/// increment for each, then removes them (one-shot fire).
fn spawn_bis_ack_timeout_reaper(
    pending_acks: Arc<Mutex<HashMap<DigestInfo, Instant>>>,
    metrics: Arc<Metrics>,
    timeout: Duration,
) {
    let interval = bis_ack_reaper_interval(timeout);
    background_spawn!("ac_bis_ack_timeout_reaper", async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let now = Instant::now();
            // Collect expired digests under the lock; emit/inc outside
            // the lock to keep the critical section tight.
            let expired = {
                let mut guard = pending_acks.lock();
                collect_expired_bis_acks(&mut guard, now, timeout)
            };
            for (digest, age) in expired {
                error!(
                    ?digest,
                    age_since_publish_ms = age.as_millis() as u64,
                    "AC BIS-ack missing — publish landed locally but server never \
                     confirmed stable-storage"
                );
                metrics.worker_bis_ack_missing.inc();
            }
        }
    });
}

#[derive(Debug)]
struct UploadActionResults {
    upload_ac_results_strategy: UploadCacheResultsStrategy,
    upload_historical_results_strategy: UploadCacheResultsStrategy,
    ac_store: Option<Store>,
    /// When Some, [`upload_ac_results`] records each successful AC
    /// write in the AC FastSlowStore's `dispatched_mirror_pins` index
    /// so the next `BlobsAvailable` tick advertises the digest via the
    /// `pinned_ac_mirror_entries` field (proto field 17). See
    /// [`crate::local_worker::AcMirrorTarget`] for the type-level
    /// invariant that pairs the FSS handle with the configured
    /// store name.
    ac_mirror_target: Option<crate::local_worker::AcMirrorTarget>,
    historical_store: Store,
    success_message_template: Template,
    failure_message_template: Template,
    /// #37 Phase 2: Shared metrics handle for AC publish counters
    /// (per-Code failure, slow-publish, success). Cloned from the
    /// owning `RunningActionsManagerImpl::metrics` at construction.
    metrics: Arc<Metrics>,
    /// (#12 H4 phase 2) The worker's own CAS advertised endpoint
    /// (e.g. `grpc://worker1.local:50081`). Populated by
    /// `RunningActionsManagerArgs::cas_endpoint` (sourced from
    /// `local_worker.rs:cas_advertised_endpoint`). Set on every
    /// `UpdateActionResultRequest` so the server's AC handler can
    /// pre-register output locality BEFORE committing the AC entry.
    ///
    /// Empty when the worker has no `cas_server_port` (no peer-blob
    /// sharing) — the server skips registration when empty.
    cas_endpoint: String,
}

impl UploadActionResults {
    fn new(
        config: &UploadActionResultConfig,
        ac_store: Option<Store>,
        ac_mirror_target: Option<crate::local_worker::AcMirrorTarget>,
        historical_store: Store,
        metrics: Arc<Metrics>,
        cas_endpoint: String,
    ) -> Result<Self, Error> {
        let upload_historical_results_strategy = config
            .upload_historical_results_strategy
            .unwrap_or(DEFAULT_HISTORICAL_RESULTS_STRATEGY);
        if !matches!(
            config.upload_ac_results_strategy,
            UploadCacheResultsStrategy::Never
        ) && ac_store.is_none()
        {
            return Err(make_input_err!(
                "upload_ac_results_strategy is set, but no ac_store is configured"
            ));
        }
        Ok(Self {
            upload_ac_results_strategy: config.upload_ac_results_strategy,
            upload_historical_results_strategy,
            ac_store,
            ac_mirror_target,
            historical_store,
            success_message_template: Template::new(&config.success_message_template).map_err(
                |e| {
                    make_input_err!(
                        "Could not convert success_message_template to rust template: {} : {e:?}",
                        config.success_message_template
                    )
                },
            )?,
            failure_message_template: Template::new(&config.failure_message_template).map_err(
                |e| {
                    make_input_err!(
                        "Could not convert failure_message_template to rust template: {} : {e:?}",
                        config.success_message_template
                    )
                },
            )?,
            metrics,
            cas_endpoint,
        })
    }

    const fn should_cache_result(
        strategy: UploadCacheResultsStrategy,
        action_result: &ActionResult,
        treat_infra_error_as_failure: bool,
    ) -> bool {
        let did_fail = action_result.exit_code != 0
            || (treat_infra_error_as_failure && action_result.error.is_some());
        match strategy {
            UploadCacheResultsStrategy::SuccessOnly => !did_fail,
            UploadCacheResultsStrategy::Never => false,
            // Never cache internal errors or timeouts.
            UploadCacheResultsStrategy::Everything => {
                treat_infra_error_as_failure || action_result.error.is_none()
            }
            UploadCacheResultsStrategy::FailuresOnly => did_fail,
        }
    }

    /// Formats the message field in `ExecuteResponse` from the `success_message_template`
    /// or `failure_message_template` config templates.
    fn format_execute_response_message(
        mut template_str: Template,
        action_digest_info: DigestInfo,
        maybe_historical_digest_info: Option<DigestInfo>,
        hasher: DigestHasherFunc,
    ) -> Result<String, Error> {
        template_str.replace(
            "digest_function",
            hasher.proto_digest_func().as_str_name().to_lowercase(),
        );
        template_str.replace(
            "action_digest_hash",
            action_digest_info.packed_hash().to_string(),
        );
        template_str.replace("action_digest_size", action_digest_info.size_bytes());
        if let Some(historical_digest_info) = maybe_historical_digest_info {
            template_str.replace(
                "historical_results_hash",
                format!("{}", historical_digest_info.packed_hash()),
            );
            template_str.replace(
                "historical_results_size",
                historical_digest_info.size_bytes(),
            );
        } else {
            template_str.replace("historical_results_hash", "");
            template_str.replace("historical_results_size", "");
        }
        template_str
            .text()
            .map_err(|e| make_input_err!("Could not convert template to text: {e:?}"))
    }

    async fn upload_ac_results(
        &self,
        action_digest: DigestInfo,
        action_result: ProtoActionResult,
        hasher: DigestHasherFunc,
        op_id: &OperationId,
        worker_id: &str,
    ) -> Result<(), Error> {
        let Some(ac_store) = self.ac_store.as_ref() else {
            return Ok(());
        };
        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = ac_store.downcast_ref::<GrpcStore>(Some(action_digest.into())) {
            let update_action_request = UpdateActionResultRequest {
                // This is populated by `update_action_result`.
                instance_name: String::new(),
                action_digest: Some(action_digest.into()),
                action_result: Some(action_result),
                results_cache_policy: None,
                digest_function: hasher.proto_digest_func().into(),
                // (#12 H4 phase 2) Worker's CAS endpoint so the server's
                // AC handler can pre-register output locality BEFORE
                // committing the AC entry (H4 invariant: locality-visible
                // happens-before AC-publish). Propagated by GrpcStore as
                // the `x-nativelink-worker` header via IS_WORKER_REQUEST
                // scope below. Empty when no peer CAS endpoint is
                // configured (server skips registration).
                cas_endpoint: self.cas_endpoint.clone(),
            };
            let size_bytes = update_action_request.encoded_len() as u64;
            let start = std::time::Instant::now();
            // #37 Phase 2 (Q1+Q2+Q3): compute elapsed BEFORE the `?`
            // propagation so the Err arm can log duration too.
            //
            // (#12 H4 phase 2) Wrap with IS_WORKER_REQUEST=true so
            // GrpcStore propagates the `x-nativelink-worker` header to the
            // server's AC handler. The server extracts this header to gate
            // the pending_output_locality_registry registration path.
            // Revision 4 (auditor): this was the missing server-side wiring
            // point — IS_WORKER was dead on the AC path without this scope.
            let res = IS_WORKER_REQUEST
                .scope(
                    true,
                    grpc_store.update_action_result(Request::new(update_action_request)),
                )
                .await
                .map(|_| ())
                .err_tip(|| "Caching ActionResult");
            let elapsed = start.elapsed();
            if let Err(err) = &res {
                error!(
                    %op_id,
                    %worker_id,
                    ?action_digest,
                    size_bytes,
                    elapsed_ms = elapsed.as_millis() as u64,
                    code = ?err.code,
                    err = %err,
                    "AC write failed (sync path, grpc)",
                );
                self.metrics.worker_ac_publish_fail_by_code(err.code);
                return res;
            }
            self.metrics.worker_ac_publish_success.inc();
            if elapsed >= Duration::from_millis(500) {
                self.metrics.worker_ac_publish_slow.inc();
                warn!(
                    %op_id,
                    %worker_id,
                    ?action_digest,
                    size_bytes,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "AC write slow (>500ms, grpc)",
                );
            }
            info!(
                %op_id,
                %worker_id,
                ?action_digest,
                size_bytes,
                elapsed_ms = elapsed.as_millis() as u64,
                throughput_mbps = format!("{:.1}", throughput_mbps(size_bytes, elapsed)),
                "AC write completed (grpc)",
            );
            return Ok(());
        }

        let mut store_data = BytesMut::with_capacity(ESTIMATED_DIGEST_SIZE);
        action_result
            .encode(&mut store_data)
            .err_tip(|| "Encoding ActionResult for caching")?;

        let size_bytes = store_data.len() as u64;
        let start = std::time::Instant::now();
        let res = ac_store
            .update_oneshot(action_digest, store_data.split().freeze())
            .await;
        if let Err(err) = &res {
            let elapsed = start.elapsed();
            error!(
                %op_id,
                %worker_id,
                ?action_digest,
                size_bytes,
                elapsed_ms = elapsed.as_millis() as u64,
                code = ?err.code,
                err = %err,
                store_class = "ac",
                "AC write failed (sync path)",
            );
            self.metrics.worker_ac_publish_fail_by_code(err.code);
            // Synchronous AC write failure (typically a fast-tier
            // failure since slow-tier is async-spawned). Defensive
            // failure-prune of the worker-local AC pin: in the
            // common case the pin was NEVER inserted (insert is on
            // the Ok path below), so this call is idempotent.
            // Non-defensive case: a previous successful tick may
            // have already pinned the same `(store_id, digest)`
            // tuple, and the write here is a re-attempt that just
            // failed — without the prune the worker would keep
            // re-advertising a pin whose authoritative durability
            // claim was just invalidated. Sibling of CAS's
            // `failed_slow_writes`-on-Err arm in
            // `fast_slow_store.rs:948` (chunked-dispatcher path).
            if let Some(target) = self.ac_mirror_target.as_ref() {
                target.fss.remove_local_ac_pin_on_failure(
                    target.store_id.as_ref(),
                    &action_digest,
                );
            }
            // Continue with the original `?` propagation behavior so
            // the err_tip context lands on the returned error.
            return Err(err.clone()).err_tip(|| "Caching ActionResult");
        }
        let elapsed = start.elapsed();
        self.metrics.worker_ac_publish_success.inc();
        if elapsed >= Duration::from_millis(500) {
            self.metrics.worker_ac_publish_slow.inc();
            warn!(
                %op_id,
                %worker_id,
                ?action_digest,
                size_bytes,
                elapsed_ms = elapsed.as_millis() as u64,
                "AC write slow (>500ms)",
            );
        }
        info!(
            %op_id,
            %worker_id,
            ?action_digest,
            size_bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            throughput_mbps = format!("{:.1}", throughput_mbps(size_bytes, elapsed)),
            "AC write completed",
        );
        // #37 Phase 2 (Q5): record this digest in the AC mirror
        // target's pending-acks map for BIS-ack delay observability.
        // The map is capped at AC_PUBLISH_PENDING_ACKS_MAX; over-cap
        // insertions skip with a warn (observability data loss only —
        // durability lifecycle is independent, see field doc).
        if let Some(target) = self.ac_mirror_target.as_ref() {
            let mut guard = target.ac_publish_pending_acks.lock();
            if guard.len() >= AC_PUBLISH_PENDING_ACKS_MAX {
                warn!(
                    ?action_digest,
                    cap = AC_PUBLISH_PENDING_ACKS_MAX,
                    "AC publish pending-acks map at cap; skipping insert (observability gap, no correctness impact)"
                );
            } else {
                guard.insert(action_digest, Instant::now());
            }
        }
        // Record this AC entry as a worker-local pin so the worker's
        // BlobsAvailable loop advertises it to the server during the
        // slow-write window, via the dedicated proto field
        // `pinned_ac_mirror_entries` (field 17). The fast-tier write
        // above has already returned (sync ack point); the slow-tier
        // write is in flight via FastSlowStore's spawned background
        // task. The server's BIS broadcast for the AC store (when its
        // slow-tier write completes) triggers the matching
        // `remove_local_ac_pins` via `handle_blobs_in_stable_storage`.
        //
        // Cancellation safety: this insert runs ONLY on the success
        // path of `update_oneshot`. If the write returned Err above,
        // the failure-prune fires (defense in depth) before the early
        // return — no pin recorded for failed writes, AND any pin
        // from a previous tick is removed.
        //
        // No-op when this worker's AC store is not a FastSlowStore
        // (e.g. direct GrpcStore — handled by the early-return
        // shortcut at the top of this function before we reach here).
        if let Some(target) = self.ac_mirror_target.as_ref() {
            target
                .fss
                .insert_local_ac_pin(target.store_id.as_ref(), action_digest);
        }
        Ok(())
    }

    async fn upload_historical_results_with_message(
        &self,
        action_digest: DigestInfo,
        execute_response: ExecuteResponse,
        message_template: Template,
        hasher: DigestHasherFunc,
    ) -> Result<String, Error> {
        let historical_digest_info = serialize_and_upload_message(
            &HistoricalExecuteResponse {
                action_digest: Some(action_digest.into()),
                execute_response: Some(execute_response.clone()),
            },
            self.historical_store.as_pin(),
            &mut hasher.hasher(),
        )
        .await
        .err_tip(|| format!("Caching HistoricalExecuteResponse for digest: {action_digest}"))?;

        Self::format_execute_response_message(
            message_template,
            action_digest,
            Some(historical_digest_info),
            hasher,
        )
        .err_tip(|| "Could not format message in upload_historical_results_with_message")
    }

    async fn cache_action_result(
        &self,
        action_info: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
        op_id: &OperationId,
        worker_id: &str,
    ) -> Result<(), Error> {
        let should_upload_historical_results =
            Self::should_cache_result(self.upload_historical_results_strategy, action_result, true);
        let should_upload_ac_results =
            Self::should_cache_result(self.upload_ac_results_strategy, action_result, false);
        // Shortcut so we don't need to convert to proto if not needed.
        if !should_upload_ac_results && !should_upload_historical_results {
            return Ok(());
        }

        let execute_response = to_execute_response(action_result.clone());

        // In theory exit code should always be != 0 if there's an error, but for safety we
        // catch both.
        let message_template = if action_result.exit_code == 0 && action_result.error.is_none() {
            self.success_message_template.clone()
        } else {
            self.failure_message_template.clone()
        };

        // Extract AC result proto before concurrent uploads (independent of message).
        let ac_result_proto = if should_upload_ac_results {
            Some(
                execute_response
                    .result
                    .clone()
                    .err_tip(|| "No result set in cache_action_result")?,
            )
        } else {
            None
        };

        // Run historical + AC uploads concurrently — they are independent.
        let historical_fut = async {
            if should_upload_historical_results {
                match self
                    .upload_historical_results_with_message(
                        action_info,
                        execute_response,
                        message_template,
                        hasher,
                    )
                    .await
                {
                    Ok(message) => Ok(Some(message)),
                    Err(e) => Err(e),
                }
            } else {
                match Self::format_execute_response_message(
                    message_template,
                    action_info,
                    None,
                    hasher,
                ) {
                    Ok(message) => Ok(Some(message)),
                    Err(e) => {
                        Err(e).err_tip(|| "Could not format message in cache_action_result")
                    }
                }
            }
        };

        let ac_fut = async {
            if let Some(proto) = ac_result_proto {
                self.upload_ac_results(action_info, proto, hasher, op_id, worker_id).await
            } else {
                Ok(())
            }
        };

        let (historical_result, ac_result) = futures::future::join(historical_fut, ac_fut).await;

        // Apply message from historical upload.
        if let Ok(Some(message)) = &historical_result {
            action_result.message.clone_from(message);
        }

        historical_result
            .map(|_| ())
            .merge(ac_result)
    }
}

#[derive(Debug)]
pub struct RunningActionsManagerArgs<'a> {
    pub root_action_directory: String,
    pub execution_configuration: ExecutionConfiguration,
    pub cas_store: Arc<FastSlowStore>,
    pub ac_store: Option<Store>,
    /// Optional `(FastSlowStore Arc, store_id)` pairing for AC pin
    /// advertisement. Some only when the worker's `ac_store` resolves
    /// to a `FastSlowStore` via the `find_fast_slow_for_pin` walker
    /// AND the worker config provides an `ac_store_name`. Threaded
    /// through to `UploadActionResults` so `upload_ac_results` can
    /// register an AC pin entry on each successful write — the pin
    /// rides the dedicated `pinned_ac_mirror_entries` proto field
    /// (field 17), HARD-PARTITIONED from the CAS-shared
    /// `pinned_mirror_entries` channel.
    pub ac_mirror_target: Option<crate::local_worker::AcMirrorTarget>,
    pub historical_store: Store,
    pub upload_action_result_config: &'a UploadActionResultConfig,
    pub max_action_timeout: Duration,
    pub max_upload_timeout: Duration,
    pub timeout_handled_externally: bool,
    pub directory_cache: Option<Arc<crate::directory_cache::DirectoryCache>>,
    /// #37 Phase 2 (Q5): timeout for the AC BIS-ack missing-detection
    /// reaper. From `LocalWorkerConfig::bis_ack_timeout_secs`. Default
    /// 60s.
    pub bis_ack_timeout: Duration,
    /// #37 Phase 2: pre-constructed metrics handle. Cloned into
    /// `AcMirrorTarget` before this struct is built, so the BIS-ack
    /// receive site can share the same counter set as the publish
    /// site. Treat as `None` for legacy/test callers; the
    /// constructor will create a fresh Arc.
    pub metrics: Option<Arc<Metrics>>,
    /// (#12 H4 phase 2) Worker's advertised CAS endpoint for
    /// pending-output locality pre-registration (e.g.
    /// `grpc://worker1.local:50081`). Set from
    /// `local_worker::cas_advertised_endpoint`. Empty string when
    /// `cas_server_port` is not configured (peer-blob sharing
    /// disabled); the server skips registration on empty.
    pub cas_endpoint: String,
    /// F2 kill-switch: when `true`, `inner_upload_results` writes outputs
    /// to the local fast store only; the remote slow-store upload is
    /// deferred to `spawn_upload_to_remote` after `execution_complete`.
    /// Default: `false` (synchronous path, current behavior).
    /// See `LocalWorkerConfig::deferred_output_uploads_enabled` and
    /// `.claude/audits/f2-deferred-output-uploads-design-2026-06-12.md`.
    pub deferred_output_uploads_enabled: bool,
}

struct CleanupGuard {
    manager: Weak<RunningActionsManagerImpl>,
    operation_id: OperationId,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let mut cleaning = manager.cleaning_up_operations.lock();
        cleaning.remove(&self.operation_id);
        manager.cleanup_complete_notify.notify_waiters();
    }
}

/// Holds state info about what is being executed and the interface for interacting
/// with actions while they are running.
#[derive(Debug)]
pub struct RunningActionsManagerImpl {
    root_action_directory: String,
    execution_configuration: ExecutionConfiguration,
    cas_store: Arc<FastSlowStore>,
    filesystem_store: Arc<FilesystemStore>,
    upload_action_results: UploadActionResults,
    max_action_timeout: Duration,
    max_upload_timeout: Duration,
    timeout_handled_externally: bool,
    running_actions: Mutex<HashMap<OperationId, Weak<RunningActionImpl>>>,
    // Note: We don't use Notify because we need to support a .wait_for()-like function, which
    // Notify does not support.
    action_done_tx: watch::Sender<()>,
    callbacks: Callbacks,
    metrics: Arc<Metrics>,
    /// Track operations being cleaned up to avoid directory collisions during action retries.
    /// When an action fails and is retried on the same worker, we need to ensure the previous
    /// attempt's directory is fully cleaned up before creating a new one.
    /// See: <https://github.com/TraceMachina/nativelink/issues/1859>
    cleaning_up_operations: Mutex<HashSet<OperationId>>,
    /// Notify waiters when a cleanup operation completes. This is used in conjunction with
    /// `cleaning_up_operations` to coordinate directory cleanup and creation.
    cleanup_complete_notify: Arc<Notify>,
    /// Optional directory cache for improving performance by caching reconstructed
    /// input directories and using hardlinks.
    directory_cache: Option<Arc<crate::directory_cache::DirectoryCache>>,
    /// F2 kill-switch: when `true`, `inner_upload_results` writes outputs to the
    /// local fast store only and defers the remote slow-store upload to
    /// `spawn_upload_to_remote`.  `false` = current synchronous behavior.
    deferred_output_uploads_enabled: bool,
}

impl RunningActionsManagerImpl {
    /// Maximum time to wait for a cleanup operation to complete before timing out.
    /// TODO(marcussorealheis): Consider making cleanup wait timeout configurable in the future
    const MAX_WAIT: Duration = Duration::from_secs(30);
    /// Maximum backoff duration for exponential backoff when waiting for cleanup.
    const MAX_BACKOFF: Duration = Duration::from_millis(500);
    pub fn new_with_callbacks(
        args: RunningActionsManagerArgs<'_>,
        callbacks: Callbacks,
    ) -> Result<Self, Error> {
        // Sadly because of some limitations of how Any works we need to clone more times than optimal.
        // Concrete FilesystemStore needed for hardlink and pin operations on
        // the action sandbox; the FastSlowStore wrapper hides the concrete
        // type so the downcast must reach into the inner store directly.
        #[allow(clippy::disallowed_methods)]
        let filesystem_store = args
            .cas_store
            .fast_store()
            .downcast_ref::<FilesystemStore>(None)
            .err_tip(
                || "Expected FilesystemStore store for .fast_store() in RunningActionsManagerImpl",
            )?
            .get_arc()
            .err_tip(|| "FilesystemStore's internal Arc was lost")?;
        let (action_done_tx, _) = watch::channel(());
        let metrics = args.metrics.unwrap_or_else(|| Arc::new(Metrics::default()));
        // #37 Phase 2 (Q5): spawn the BIS-ack timeout reaper iff there
        // is an AC mirror target (which is the only path that
        // populates `ac_publish_pending_acks`). The reaper walks the
        // map at half the configured timeout and surfaces missing-ack
        // entries via `error!` + `worker_bis_ack_missing` counter.
        if let Some(target) = args.ac_mirror_target.as_ref() {
            spawn_bis_ack_timeout_reaper(
                target.ac_publish_pending_acks.clone(),
                metrics.clone(),
                args.bis_ack_timeout,
            );
        }
        let upload_action_results = UploadActionResults::new(
            args.upload_action_result_config,
            args.ac_store,
            args.ac_mirror_target,
            args.historical_store,
            metrics.clone(),
            args.cas_endpoint,
        )
        .err_tip(|| "During RunningActionsManagerImpl construction")?;
        Ok(Self {
            root_action_directory: args.root_action_directory,
            execution_configuration: args.execution_configuration,
            cas_store: args.cas_store,
            filesystem_store,
            upload_action_results,
            max_action_timeout: args.max_action_timeout,
            max_upload_timeout: args.max_upload_timeout,
            timeout_handled_externally: args.timeout_handled_externally,
            running_actions: Mutex::new(HashMap::new()),
            action_done_tx,
            callbacks,
            metrics,
            cleaning_up_operations: Mutex::new(HashSet::new()),
            cleanup_complete_notify: Arc::new(Notify::new()),
            directory_cache: args.directory_cache,
            deferred_output_uploads_enabled: args.deferred_output_uploads_enabled,
        })
    }

    pub fn new(args: RunningActionsManagerArgs<'_>) -> Result<Self, Error> {
        Self::new_with_callbacks(
            args,
            Callbacks {
                now_fn: SystemTime::now,
                sleep_fn: |duration| Box::pin(tokio::time::sleep(duration)),
            },
        )
    }

    /// (#12 H4 phase 2) Test accessor: returns the `cas_endpoint` stored in
    /// `upload_action_results`. Verifies the arg chain
    /// `RunningActionsManagerArgs::cas_endpoint` → `UploadActionResults::new`
    /// → `upload_ac_results` `UpdateActionResultRequest.cas_endpoint` is intact.
    #[doc(hidden)]
    pub fn cas_endpoint_for_test(&self) -> &str {
        &self.upload_action_results.cas_endpoint
    }

    /// Expand Tree protos from output folders and return the contained file
    /// digests. Used to register tree file digests in the locality map before
    /// reporting the execution result, so the server can proxy reads immediately.
    ///
    /// #O3/O13: `action` (optional) carries the per-action Tree-proto cache
    /// populated by `inner_upload_results`. When `Some`, each output folder's
    /// Tree is partitioned hit-vs-miss BEFORE any future is allocated, so
    /// cache hits short-circuit the async dispatch entirely (zero per-folder
    /// `FuturesUnordered` slot). Misses fall back to `get_and_decode_digest`
    /// over the local fast store, run concurrently via `FuturesUnordered`
    /// (preserves #A4 parallel decode). `None` skips the cache check
    /// entirely — test-only.
    pub async fn expand_tree_file_digests(
        &self,
        action_result: &ActionResult,
        action: Option<&Arc<RunningActionImpl>>,
    ) -> Vec<DigestInfo> {
        // Safe to read directly from fast_store (skipping the FastSlowStore
        // wrapper / mirror_blobs path): Tree protos here are produced
        // locally by the worker's own action upload and written to the
        // FilesystemStore. Mirror blobs are server-pushed CAS data, never
        // Tree-shaped, so a mirror-only Tree digest is impossible.
        // Read tree protos from the local fast store only: the action just
        // produced these on this worker, so consulting slow (server) on miss
        // would block the locality-registration hot path on a network round
        // trip for a blob that is supposed to be local.
        #[allow(clippy::disallowed_methods)]
        let fast_store = self.cas_store.fast_store();
        // (Probe #1) Total wall-clock for the tree-expansion loop, summed
        // over every output folder's Tree decode. Lives on the worker
        // post-action publish critical path so latency is attributable
        // to this step when locality-hint emission lags.
        //
        // (#A4 2026-06-07) Decode all output_folders' Tree protos in
        // parallel via `FuturesUnordered`. Disk reads + protobuf decodes
        // are independent per folder, so the prior sequential loop
        // walled N folders × per-folder latency on the publish path.
        // Concurrency is bounded by `output_folders.len()` (an
        // ActionResult-level bound; output trees are produced by the
        // just-finished action and are typically O(1..10)). Decode
        // failures are still logged-and-skipped (same semantics as the
        // pre-fix loop) so a single corrupt tree does not fail the
        // whole expansion.
        let expand_tree_start = Instant::now();
        let folder_count = action_result.output_folders.len();
        // #O3/O13 × #A4 hit/miss partition: drain cache HITS synchronously
        // (zero per-folder future allocation, zero scheduler dispatch);
        // dispatch only MISSES into `FuturesUnordered` for concurrent decode
        // through the fast store. The prior `.map(|folder| async {...})`
        // form allocated a future for every folder regardless of cache
        // outcome; this partition is the F2 (perf-claim) fix.
        let mut hits: Vec<(DigestInfo, ProtoTree)> = Vec::new();
        let mut miss_digests: Vec<DigestInfo> = Vec::new();
        for folder in &action_result.output_folders {
            let tree_digest = folder.tree_digest;
            if tree_digest.size_bytes() == 0 {
                continue;
            }
            match action.and_then(|a| a.peek_cached_tree_proto(&tree_digest)) {
                Some(tree) => hits.push((tree_digest, tree)),
                None => miss_digests.push(tree_digest),
            }
        }
        let hits_count = hits.len();
        let misses_count = miss_digests.len();
        let decodes: FuturesUnordered<_> = miss_digests
            .into_iter()
            .map(|tree_digest| async move {
                let res = get_and_decode_digest::<ProtoTree>(fast_store, tree_digest.into()).await;
                (tree_digest, res)
            })
            .collect();
        let miss_results: Vec<_> = decodes.collect().await;
        let mut file_digests = Vec::new();
        // Hits first: synchronous-drained, never fail.
        for (tree_digest, tree) in hits {
            let digests: Vec<DigestInfo> = tree
                .children
                .into_iter()
                .chain(tree.root)
                .flat_map(|dir| dir.files)
                .filter_map(|f| f.digest.and_then(|d| DigestInfo::try_from(d).ok()))
                .filter(|d| d.size_bytes() > 0)
                .collect();
            info!(
                ?tree_digest,
                file_count = digests.len(),
                "expanded tree for locality hints (cache hit)",
            );
            file_digests.extend(digests);
        }
        for (tree_digest, res) in miss_results {
            match res {
                Ok(tree) => {
                    let digests: Vec<DigestInfo> = tree
                        .children
                        .into_iter()
                        .chain(tree.root)
                        .flat_map(|dir| dir.files)
                        .filter_map(|f| f.digest.and_then(|d| DigestInfo::try_from(d).ok()))
                        .filter(|d| d.size_bytes() > 0)
                        .collect();
                    info!(
                        ?tree_digest,
                        file_count = digests.len(),
                        "expanded tree for locality hints",
                    );
                    file_digests.extend(digests);
                }
                Err(e) => {
                    warn!(
                        ?tree_digest,
                        ?e,
                        "failed to expand tree for locality hints",
                    );
                }
            }
        }
        let expand_tree_total_ms = expand_tree_start.elapsed().as_millis() as u64;
        info!(
            folder_count,
            hits_count,
            misses_count,
            file_digest_count = file_digests.len(),
            expand_tree_total_ms,
            "expand_tree_file_digests complete",
        );
        file_digests
    }

    /// Spawn a background task that uploads all action output blobs from the
    /// fast store (local FilesystemStore) to the slow store (remote CAS).
    /// This is called after the execution result has been reported to the
    /// scheduler, so it does not block action completion latency.
    ///
    /// To prevent a race condition where the EvictingMap evicts small blobs
    /// before the background task can read them, we pre-read all small blobs
    /// (<=1 MiB) from the fast store *before* spawning the background task.
    /// The pre-read data is passed into the spawned task via a HashMap, so
    /// the background upload never needs to re-read small blobs from the
    /// store. Large blobs are streamed directly from the store as before
    /// (they are much less likely to be evicted quickly due to their size).
    pub fn spawn_upload_to_remote(
        self: &Arc<Self>,
        action_result: &ActionResult,
        action: Option<&Arc<RunningActionImpl>>,
    ) {
        // Production entry point: schedule the background upload and DROP the
        // JoinHandle (fire-and-forget, exactly as before). The `_impl` carries
        // the body and returns the handle so the #FL-688 W6 production-
        // composition test can `.await` the real upload task to completion
        // and observe the give-up arm's `requeue_failed_push` deterministically
        // (no detached-spawn polling). Behavior change: NONE — the handle is
        // immediately dropped here, which is identical to the prior
        // `tokio::spawn(...)`-without-binding.
        drop(self.spawn_upload_to_remote_impl(action_result, action));
    }

    /// Body of [`Self::spawn_upload_to_remote`]; returns the spawned upload
    /// task's `JoinHandle` (or `None` when the upload is skipped — noop slow
    /// store, read-only/get slow direction, or no eligible output digests).
    /// Production drops the handle; the W6 test awaits it.
    fn spawn_upload_to_remote_impl(
        self: &Arc<Self>,
        action_result: &ActionResult,
        action: Option<&Arc<RunningActionImpl>>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let slow_store = self.cas_store.slow_store();
        if slow_store
            .inner_store(None::<StoreKey<'_>>)
            .optimized_for(StoreOptimizations::NoopUpdates)
        {
            return None;
        }
        // Respect slow_direction config — when set to Get or ReadOnly,
        // the slow store should not receive writes (same check as
        // FastSlowStore::update).
        let dir = self.cas_store.slow_direction();
        if dir == StoreDirection::Get || dir == StoreDirection::ReadOnly {
            return None;
        }

        let mut digests = Vec::new();
        let mut tree_digests = Vec::new();
        for file in &action_result.output_files {
            if file.digest.size_bytes() > 0 {
                digests.push(file.digest);
            }
        }
        for folder in &action_result.output_folders {
            if folder.tree_digest.size_bytes() > 0 {
                digests.push(folder.tree_digest);
                tree_digests.push(folder.tree_digest);
            }
        }
        if action_result.stdout_digest.size_bytes() > 0 {
            digests.push(action_result.stdout_digest);
        }
        if action_result.stderr_digest.size_bytes() > 0 {
            digests.push(action_result.stderr_digest);
        }
        if digests.is_empty() {
            return None;
        }

        // Pin output digests to prevent eviction during background upload.
        let filesystem_store = self.filesystem_store.clone();
        // FL-681 Fix A: in F2 deferred mode this background upload is the
        // AUTHORITATIVE durability path (bypasses `FastSlowStore::update`),
        // so the source pin must survive past the 120s TTL until BIS-ack.
        // Pin indefinitely in deferred mode; keep the time-bounded pin (with
        // its live TTL→failed_slow_writes backstop) in synchronous mode.
        let deferred_pin = self.deferred_output_uploads_enabled;
        for digest in &digests {
            if deferred_pin {
                // FL-688 v3 Stage B: indefinite-ONLY (held-until-BIS). The
                // time-bounded fallback was the deferred-mode loss window (F2
                // bypasses in_flight_slow_writes/failed_slow_writes, so a
                // 120s-demoted pin is silently lost). On indefinite cap-refusal
                // the blob is left fully evictable (the accepted saturated-cap
                // loss class; producer backpressure is the worker
                // indefinite_pin_saturated NAK).
                if !filesystem_store.pin_digest_indefinite_with_result(digest) {
                    warn!(
                        %digest,
                        "FL-688 v3 Stage B: indefinite-pin cap exhausted (or eviction race) \
                         scheduling F2 upload; the held-until-BIS pin was REFUSED and the F2 \
                         output is now fully evictable — see indefinite_pin_cap headroom",
                    );
                }
            } else {
                filesystem_store.pin_digest(digest);
            }
        }
        // #547 fix-up CF5: record_pin_acquired moved out of this loop into
        // the per-digest upload `Ok(())` arm (see `:5000-5005` below).
        // Rationale: the previous unconditional acquire at this pre-upload
        // site leaked the `worker_concurrent_pinned_bytes_live` gauge on
        // every non-fresh-write break path (AlreadyExists, permanent-error,
        // retry-exhausted) because the BIS-unpin handler only ever
        // decrements digests that recorded a tonic-Ok timestamp (and CF2
        // intentionally skips that for AlreadyExists). Tying acquire to
        // the same control-flow seam as `record_tonic_ok` makes
        // acquire+release structurally symmetric and closes the leak class
        // (distributed-systems-reviewer re-verify MAJOR-A, 2026-05-15).
        // The FilesystemStore pin itself stays unconditional — it
        // prevents eviction during the upload attempt regardless of
        // outcome; only the Phase 0 metric tracking moves.

        // #547 Phase 0 instrumentation: assign a per-action key so the
        // BIS-unpin handler can fold per-digest gaps into the action's
        // total + max pin-extension histograms. The key is a fresh
        // process-wide atomic counter; uniqueness across worker
        // lifecycle is sufficient for the per-action histogram emit.
        // Pure observability.
        let phase0_action_key = {
            use core::sync::atomic::{AtomicU64, Ordering};
            static PHASE0_ACTION_COUNTER: AtomicU64 = AtomicU64::new(1);
            PHASE0_ACTION_COUNTER.fetch_add(1, Ordering::Relaxed)
        };

        // Lifecycle log: emitted unconditionally so silent loss of the
        // spawned upload task (drop, panic, runtime shutdown before run)
        // is detectable by absence of a matching "background CAS upload
        // completed" line for the same digest count.
        info!(
            initial_digest_count = digests.len(),
            tree_count = tree_digests.len(),
            "spawn_upload_to_remote: scheduling background CAS upload",
        );

        let cas_store = self.cas_store.clone();
        // #O3/O13: drain the action's per-action Tree-proto cache for the
        // tree_digests we're about to upload, synchronously and BEFORE the
        // tokio::spawn boundary. Drained entries are moved into the spawned
        // task via the `cached_trees` HashMap so the in-task decode loop
        // never needs to touch the action's cache. The action's Mutex stays
        // confined to the calling task; the spawned task owns its data.
        let mut cached_trees: HashMap<DigestInfo, ProtoTree> = HashMap::new();
        if let Some(action) = action {
            for tree_digest in &tree_digests {
                if let Some(tree) = action.take_cached_tree_proto(tree_digest) {
                    cached_trees.insert(*tree_digest, tree);
                }
            }
        }
        let upload_task = tokio::spawn(async move {
            let slow_store = cas_store.slow_store();
            let start = std::time::Instant::now();

            // Small blobs use update_oneshot which routes through
            // BatchUpdateBlobs for efficient coalescing. Large blobs
            // stream through a channel to avoid loading into memory.
            const BATCH_THRESHOLD: u64 = 1024 * 1024; // 1 MiB

            // Phase 1: Pre-read all known small blobs into memory to
            // prevent the eviction race condition. The EvictingMap can
            // evict tiny blobs (e.g. 4-byte tree blobs, stdout, stderr)
            // before the background task gets a chance to read them.
            // By reading them eagerly at the start of the spawned task
            // (which runs immediately), we capture the data before any
            // subsequent action's uploads can trigger eviction.
            let mut preread_data: HashMap<DigestInfo, Bytes> =
                HashMap::with_capacity(digests.len());

            // Pre-read initial small digests (stdout, stderr, tree blobs,
            // small output files). Read through cas_store so an eviction
            // race between action completion and this background task
            // self-heals via FastSlowStore's slow-store fallback.
            let cas_store_ref = cas_store.as_ref();
            let preread_futures: FuturesUnordered<_> = digests
                .iter()
                .filter(|d| d.size_bytes() <= BATCH_THRESHOLD)
                .copied()
                .map(|digest| async move {
                    let result = cas_store_ref.get_part_unchunked(digest, 0, None).await;
                    (digest, result)
                })
                .collect();
            let preread_results: Vec<_> = preread_futures.collect().await;
            for (digest, result) in preread_results {
                match result {
                    Ok(data) => {
                        preread_data.insert(digest, data);
                    }
                    Err(e) => {
                        warn!(
                            ?digest,
                            ?e,
                            "upload_to_remote: failed to pre-read small blob from fast store",
                        );
                    }
                }
            }

            // Extract file digests from output directory trees. Use
            // pre-read data if available (avoids re-reading from store).
            // Fallback path reads through cas_store so the same eviction-
            // race self-heal applies (slow-store fallback) — using
            // fast_store directly would silently lose the tree if the
            // pin race fired between completion and this task.
            for tree_digest in &tree_digests {
                // #O3/O13: prefer the drained `cached_trees` populated from
                // the per-action cache before this task was spawned. Cache
                // hits skip both the pre-read and the storage decode; the
                // action's cache is already drained at this point. Miss
                // falls back to pre-read data, then to a storage-layer
                // re-decode (correctness-safe).
                let tree_result = if let Some(tree) = cached_trees.remove(tree_digest) {
                    Ok(tree)
                } else if let Some(data) = preread_data.get(tree_digest) {
                    ProtoTree::decode(data.clone())
                        .map_err(|e| make_err!(Code::Internal, "Failed to decode Tree proto: {e}"))
                } else {
                    get_and_decode_digest::<ProtoTree>(cas_store_ref, (*tree_digest).into()).await
                };
                match tree_result {
                    Ok(tree) => {
                        let file_digests: Vec<DigestInfo> = tree
                            .children
                            .into_iter()
                            .chain(tree.root)
                            .flat_map(|dir| dir.files)
                            .filter_map(|f| f.digest.and_then(|d| DigestInfo::try_from(d).ok()))
                            .filter(|d| d.size_bytes() > 0)
                            .collect();
                        info!(
                            ?tree_digest,
                            file_count = file_digests.len(),
                            "upload_to_remote: extracted file digests from output directory tree",
                        );
                        // Pre-read any newly-discovered small file digests.
                        // Use cas_store so an eviction during the pre-read
                        // window self-heals via the slow-store fallback.
                        let new_preread_futures: FuturesUnordered<_> = file_digests
                            .iter()
                            .filter(|d| {
                                d.size_bytes() <= BATCH_THRESHOLD
                                    && !preread_data.contains_key(d)
                            })
                            .copied()
                            .map(|digest| async move {
                                let result =
                                    cas_store_ref.get_part_unchunked(digest, 0, None).await;
                                (digest, result)
                            })
                            .collect();
                        let new_results: Vec<_> = new_preread_futures.collect().await;
                        for (digest, result) in new_results {
                            match result {
                                Ok(data) => {
                                    preread_data.insert(digest, data);
                                }
                                Err(e) => {
                                    warn!(
                                        ?digest,
                                        ?e,
                                        "upload_to_remote: failed to pre-read tree file blob",
                                    );
                                }
                            }
                        }
                        // Pin tree file digests to prevent eviction.
                        // #547 fix-up CF5: see the matching comment above
                        // the initial digest-pin loop — `record_pin_acquired`
                        // has moved to the per-digest upload `Ok(())` arm
                        // so the gauge only tracks bytes-pinned-AND-tonic-
                        // Ok'd-awaiting-BIS, which is exactly the slice
                        // Phase 2 (#549) needs for cap sizing.
                        //
                        // #547 fix-up CF5-MINOR-B: a digest that appears
                        // in BOTH the initial `digests` (from output_files
                        // / stdout / stderr) AND in `file_digests` here
                        // (tree-extracted) will be pinned twice and
                        // uploaded twice. Under Option A the upload Ok
                        // arm fires `record_pin_acquired` per upload, so
                        // a double-success would double-acquire the
                        // gauge. The matching BIS-unpin handler fires
                        // once per BIS chunk per digest (single
                        // `record_pin_released`). Net: gauge over-counts
                        // by one digest-size per duplicate. Magnitude is
                        // small in production (Bazel rarely duplicate-
                        // lists a file in both output_files and
                        // output_directories); the published metric
                        // help-text in `WorkerPhase0Metrics::publish`
                        // documents this drift inline so operators
                        // sizing Phase 2 caps know the gauge is
                        // approximate-upward.
                        // FL-681 Fix A: tree-extracted file digests are the
                        // same deferred-durability sources — pin indefinitely
                        // in F2 mode so the 120s TTL cannot drop them before
                        // BIS-ack.
                        for digest in &file_digests {
                            if deferred_pin {
                                // FL-688 v3 Stage B: indefinite-ONLY
                                // (held-until-BIS) for tree-extracted F2 outputs;
                                // the time-bounded fallback was the deferred-mode
                                // loss window. Cap-refusal → fully evictable
                                // (accepted saturated-cap loss class).
                                if !filesystem_store.pin_digest_indefinite_with_result(digest) {
                                    warn!(
                                        %digest,
                                        "FL-688 v3 Stage B: indefinite-pin cap exhausted (or \
                                         eviction race) pinning tree-extracted F2 output; the \
                                         held-until-BIS pin was REFUSED and the output is now \
                                         fully evictable",
                                    );
                                }
                            } else {
                                filesystem_store.pin_digest(digest);
                            }
                        }
                        digests.extend(file_digests);
                    }
                    Err(e) => {
                        warn!(
                            ?tree_digest,
                            ?e,
                            "upload_to_remote: failed to decode tree for file digest extraction",
                        );
                    }
                }
            }

            let total = digests.len();
            let preread_count = preread_data.len();
            info!(
                total_digests = total,
                preread_count,
                tree_count = tree_digests.len(),
                "upload_to_remote: starting background CAS upload",
            );

            // Phase 2: Upload all digests to the slow store. Small blobs
            // use pre-read data; large blobs stream from the fast store.
            //
            // FL-681 Fix B: a deferred upload retries until it SUCCEEDS —
            // it NEVER gives up for a retryable error, because a give-up is
            // permanent data loss + a dangling AC entry (the blob is
            // single-copy on the worker until the server has it durably).
            // Safe precisely because FL-681 Fix A keeps the source pinned
            // (indefinitely, until BIS-ack) and therefore readable across
            // every retry. Backoff is capped (`MAX_BACKOFF`) so a sustained
            // server/BIS outage retries at a steady interval — it does not
            // hammer and it does not stop. Only the documented
            // permanent-request classes (`classify_upload_error` →
            // `PermanentGiveUp`) terminate.
            const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
            const MAX_BACKOFF: Duration = Duration::from_secs(30);
            // (Stuck-upload `warn!` cadence lives in the module-level
            // `STUCK_WARN_THRESHOLD` / `STUCK_WARN_EVERY` consts, shared with
            // `DeferredUploadRetry`.)

            let mut success_count = 0u64;
            let mut fail_count = 0u64;
            let mut uploads = FuturesUnordered::new();
            for &digest in &digests {
                // Use pre-read data for small blobs that were captured
                // eagerly. This avoids the eviction race where EvictingMap
                // removes the blob before we can read it.
                let cached_data = preread_data.remove(&digest);
                // FL-681 Q2: each per-digest retry future needs the fast
                // store to re-assert the pin before a read-side retry
                // (cheap Arc clone; the store itself is shared).
                let filesystem_store = filesystem_store.clone();
                // FL-681 Fix B safety gate (A+B interaction): infinite retry
                // is safe ONLY when the source is pinned indefinitely (Fix A,
                // deferred mode). "B without A = retry re-reads an evicted
                // source → fails." In the SYNCHRONOUS path the pin is
                // time-bounded and the blob was already written to the full
                // FastSlowStore by `inner_upload_results`; that path keeps a
                // FINITE retry, and on give-up THIS loop now arms the
                // `failed_slow_writes` / reconnect-`UploadMissingBlobs`
                // backstop itself (#FL-688 W6 — the give-up arm below calls
                // `requeue_failed_push`). Pre-#FL-688 the backstop was armed
                // only by the EARLIER `inner_upload_results` FSS write; this
                // loop's give-up recorded nothing (it writes to the bare
                // `slow_store`), so a digest whose FSS write succeeded but
                // whose subsequent bare-`slow_store` push exhausted retries
                // was never re-queued by this loop. `SYNC_MAX_RETRIES`
                // preserves the prior synchronous-mode give-up bound (was
                // `MAX_RETRIES = 4`).
                // `deferred_pin` IS the retry-forever switch: the planner
                // (`plan_retry_step`) treats `deferred_pin == true` as
                // retry-forever and `false` as the finite `SYNC_MAX_RETRIES`
                // bound.
                const SYNC_MAX_RETRIES: u32 = 4;
                // #FL-688 (A follow-up): set IS_WORKER_REQUEST=true for the
                // whole per-digest retry future so GrpcStore stamps
                // `x-nativelink-worker` on the wire.
                //
                // This path runs inside `tokio::spawn` (:6461) which STRIPS
                // task-locals: without this scope the spawned task has no
                // IS_WORKER_REQUEST → GrpcStore stamps NO header → the server
                // sees is_worker=false → if the digest is locality-advertised,
                // the G1 short-circuit phantom-acks it (same data-loss window
                // the G1 (B) fix closed for backfill; here it is only
                // *recovered* later by backfill).
                //
                // Per-FUTURE scoping (not wrapping the spawn body). CORRECT
                // because the chain from this point through to GrpcStore is
                // SPAWN-FREE: `slow_store` here is WorkerProxyStore (default
                // `update_oneshot` → inline `update` → GrpcStore::update →
                // ByteStream `write`; no nested `tokio::spawn`). If any link
                // is later refactored to spawn, IS_WORKER_REQUEST is SILENTLY
                // lost → FL-688 re-opens. Any new spawn MUST re-establish the
                // scope INSIDE the spawned task.
                // is_worker only — fresh outputs are a worker upload, not a
                // mirror push (do NOT set IS_MIRROR_REQUEST).
                uploads.push(IS_WORKER_REQUEST.scope(true, async move {
                    // FL-681 Q1+Q2: the retry controller owns the attempt
                    // counter + the remote ramp and performs the re-pin +
                    // side-sized backoff (`DeferredUploadRetry`).
                    let mut retry = DeferredUploadRetry::new(INITIAL_BACKOFF);
                    loop {
                        // FL-681 Q1: the attempt result is tagged with WHICH
                        // side failed (`UploadFailureSide`) so the retry can
                        // (a) re-pin the source on a worker-local read race
                        // before re-reading (Q2) and (b) size the backoff by
                        // side — a same-host re-read race wants a ~50-100 ms
                        // floor, not the 1 s remote ramp.
                        let result: Result<(), (Error, UploadFailureSide)> = if let Some(ref data) = cached_data {
                            // Data was pre-read -- upload directly without
                            // touching the fast store. The only failure here
                            // is the remote write (no fast-store read).
                            slow_store
                                .update_oneshot(digest, data.clone())
                                .await
                                .map_err(|e| (e, UploadFailureSide::RemoteWrite))
                        } else if digest.size_bytes() <= BATCH_THRESHOLD {
                            // Small blob that wasn't pre-read (e.g. pre-read
                            // failed). Read through cas_store so an eviction
                            // race self-heals via the slow-store fallback in
                            // FastSlowStore::get_part.
                            match cas_store_ref.get_part_unchunked(digest, 0, None).await {
                                Ok(data) => slow_store
                                    .update_oneshot(digest, data)
                                    .await
                                    .map_err(|e| (e, UploadFailureSide::RemoteWrite)),
                                // Read-side failure: the source was not
                                // readable from the worker's own fast store
                                // (eviction race). Tag ReadLocal so the retry
                                // re-pins + uses the short backoff.
                                Err(e) => Err((e, UploadFailureSide::ReadLocal)),
                            }
                        } else {
                            let (tx, rx) = make_buf_channel_pair();
                            // Read via cas_store so the self-healing fallback
                            // applies on eviction during streaming uploads too.
                            //
                            // Phase-tagged tracing: name the read and write
                            // halves separately so when either wedges, the
                            // post-mortem log shows which side stalled. The
                            // `tokio::join!` below does not natively report
                            // which half is slow, so we wrap each side in an
                            // async block that logs a `warn!` if it exceeds
                            // `SLOW_PHASE_WARN`. This converts an opaque
                            // upload-stalled event into "read from fast
                            // store took Ms" or "gRPC write to slow store
                            // took Ms" — which names the wedged side
                            // directly.
                            const SLOW_PHASE_WARN: Duration = Duration::from_secs(5);
                            let upload_phase_start = std::time::Instant::now();
                            let read_fut = async {
                                let phase_start = std::time::Instant::now();
                                let res = cas_store_ref.get(digest, tx).await;
                                let elapsed = phase_start.elapsed();
                                if elapsed >= SLOW_PHASE_WARN {
                                    warn!(
                                        ?digest,
                                        size_bytes = digest.size_bytes(),
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "upload_to_remote: slow fast-store read phase",
                                    );
                                }
                                res
                            };
                            let write_fut = async {
                                let phase_start = std::time::Instant::now();
                                let res = slow_store.update(
                                    digest,
                                    rx,
                                    UploadSizeInfo::ExactSize(digest.size_bytes()),
                                ).await;
                                let elapsed = phase_start.elapsed();
                                if elapsed >= SLOW_PHASE_WARN {
                                    warn!(
                                        ?digest,
                                        size_bytes = digest.size_bytes(),
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "upload_to_remote: slow slow-store write phase (gRPC send)",
                                    );
                                }
                                res
                            };
                            let (read_res, write_res) = tokio::join!(read_fut, write_fut);
                            let total_elapsed = upload_phase_start.elapsed();
                            if total_elapsed >= SLOW_PHASE_WARN {
                                // Surface the combined-phase wedge — useful when
                                // one half fails fast (e.g. EOF) but the other
                                // hangs. Logged with read+write completion status
                                // so the operator can tell which half actually
                                // wedged (the one that did NOT complete cleanly).
                                warn!(
                                    ?digest,
                                    size_bytes = digest.size_bytes(),
                                    total_elapsed_ms = total_elapsed.as_millis() as u64,
                                    read_ok = read_res.is_ok(),
                                    write_ok = write_res.is_ok(),
                                    "upload_to_remote: slow streaming upload (combined)",
                                );
                            }
                            // If the write succeeded, the upload is done even if
                            // the read side got a "receiver disconnected" error
                            // (e.g. server already had the blob and closed early).
                            if write_res.is_ok() {
                                Ok(())
                            } else if read_res.is_err() {
                                // Read-side failure (eviction race): the
                                // worker's own fast store could not produce
                                // the bytes — `get` returned NotFound and the
                                // dropped channel synthesized
                                // `Internal "buf_channel: writer dropped
                                // without commit"`. Tag ReadLocal so the retry
                                // re-pins the source (Q2) and uses the short
                                // backoff (Q1) instead of the 1 s remote ramp.
                                // `read_res.merge(write_res)` preserves the
                                // read code as the primary (merge keeps
                                // `self.code`) plus the write context.
                                Err((read_res.merge(write_res).unwrap_err(), UploadFailureSide::ReadLocal))
                            } else {
                                // Only the remote write failed (read was Ok):
                                // a genuine remote transient — keep the remote
                                // ramp.
                                Err((write_res.unwrap_err(), UploadFailureSide::RemoteWrite))
                            }
                        };
                        match result {
                            Ok(()) => {
                                // #547 Phase 0 instrumentation: record the
                                // tonic-Ok timestamp keyed by digest + this
                                // action's key. The BIS-unpin handler on
                                // this worker will compute the per-digest
                                // pin-release latency when the matching
                                // BIS chunk arrives. Pure observability.
                                worker_phase0_metrics()
                                    .record_tonic_ok(digest, phase0_action_key);
                                // #547 fix-up CF5: acquire the live-pin
                                // gauge HERE (adjacent to record_tonic_ok)
                                // rather than at the pre-upload pin sites.
                                // This ties acquire to the same control-
                                // flow seam as the release in the BIS-
                                // unpin handler (gated on
                                // `record_bis_unpin().is_some()`, which
                                // requires the tonic_ok_timestamps entry
                                // this call populates). Non-Ok break arms
                                // (AlreadyExists, permanent-error, retry-
                                // exhausted) never acquire, so they
                                // structurally cannot leak the gauge.
                                worker_phase0_metrics()
                                    .record_pin_acquired(digest.size_bytes());
                                break true;
                            }
                            Err((e, _side)) if e.code == Code::AlreadyExists => {
                                // #547 fix-up CF2 (perf-optimizer N4): do
                                // NOT record_tonic_ok here. AlreadyExists
                                // means the slow tier short-circuits
                                // FastSlowStore::update without invoking
                                // stable_digests_pusher → no BIS chunk
                                // will be broadcast for this digest from
                                // THIS write. The side-channel entry
                                // would then sit until 10 min TTL evicts
                                // it (cache leak, capped by
                                // TONIC_OK_TS_CACHE_CAPACITY at ~12 MiB
                                // but operator-confusing on the
                                // pin-release histogram which would
                                // appear under-populated). The action's
                                // per-digest histogram is correctly
                                // skipped for AlreadyExists; the action
                                // accumulator's per-action max/total are
                                // load-bearing on fresh-write semantics.
                                break true;
                            }
                            Err((e, side)) => {
                                // FL-681 Q1+Q2: hand the failure to the retry
                                // controller. It classifies give-up vs retry,
                                // re-pins the source BEFORE the next read on a
                                // worker-local read race (Q2 — via the closure
                                // below, using the SAME indefinite-until-BIS
                                // path in deferred mode / time-bounded in
                                // synchronous mode), sleeps the side-sized
                                // backoff (Q1 — short floor for a read race,
                                // the 1 s→cap ramp for a remote transient), and
                                // advances the remote ramp only on a remote
                                // failure. `Some(flag)` = STOP (give-up);
                                // `None` = CONTINUE (already re-pinned + slept).
                                let outcome = retry
                                    .after_failure(
                                        &e,
                                        side,
                                        digest,
                                        deferred_pin,
                                        SYNC_MAX_RETRIES,
                                        MAX_BACKOFF,
                                        |mode| match mode {
                                            RepinMode::Indefinite => {
                                                // FL-688 v3 Stage B: indefinite-
                                                // ONLY re-pin (held-until-BIS),
                                                // matching the schedule-time
                                                // deferred pin. The time-bounded
                                                // fallback was the deferred-mode
                                                // loss window; on cap-refusal the
                                                // source is left fully evictable
                                                // (the read-race retry's slow-tier
                                                // re-read self-heals if the source
                                                // survives). Aligns with the
                                                // RepinMode::Indefinite contract
                                                // doc (pin_digest_indefinite_with_result).
                                                let _ = filesystem_store
                                                    .pin_digest_indefinite_with_result(&digest);
                                            }
                                            RepinMode::TimeBounded => {
                                                filesystem_store.pin_digest(&digest);
                                            }
                                            RepinMode::None => {}
                                        },
                                    )
                                    .await;
                                if let Some(flag) = outcome {
                                    // #FL-688 W6 retry-until-durable: when the
                                    // SYNCHRONOUS-mode finite retry budget is
                                    // exhausted (decision == Retry give-up),
                                    // arm the `failed_slow_writes` backstop so
                                    // the reconnect drainer re-attempts the
                                    // push — closing the load-bearing-false
                                    // doc gap (`:1378-1391` claimed a backstop
                                    // this loop never armed: it writes to the
                                    // bare `slow_store`, not the FSS, so its
                                    // give-up recorded nothing). A
                                    // `PermanentGiveUp` (e.g. InvalidArgument)
                                    // is NOT re-queued — it can never succeed
                                    // by retry. `AlreadyExists` is handled by
                                    // the explicit success arm above and never
                                    // reaches here. Deferred mode never gives
                                    // up on a retryable class (retry-forever),
                                    // so this only fires in the production
                                    // synchronous path.
                                    if should_requeue_on_giveup(&e, flag) {
                                        let requeued =
                                            cas_store_ref.requeue_failed_push(digest);
                                        if requeued {
                                            warn!(
                                                ?digest,
                                                "upload_to_remote: sync-mode retry budget \
                                                 exhausted; re-queued into failed_slow_writes \
                                                 for retry-until-durable (#FL-688 W6)"
                                            );
                                        } else {
                                            warn!(
                                                ?digest,
                                                "upload_to_remote: sync-mode retry budget \
                                                 exhausted AND failed_slow_writes is at cap — \
                                                 digest NOT re-queued; server BlobsAvailable \
                                                 re-request is the remaining retry path (#FL-688 W6)"
                                            );
                                        }
                                    }
                                    break flag;
                                }
                            }
                        }
                    }
                }));
            }
            while let Some(ok) = uploads.next().await {
                if ok {
                    success_count += 1;
                } else {
                    fail_count += 1;
                }
            }

            // Blobs remain pinned after upload completes. They will be
            // unpinned when the server sends BlobsInStableStorage confirming
            // the blobs have been persisted to stable storage (e.g.
            // FilesystemStore, not just MemoryStore). This prevents the
            // worker from evicting blobs that the server hasn't durably
            // stored yet.

            info!(
                total_digests = total,
                success_count,
                fail_count,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "upload_to_remote: background CAS upload completed",
            );

            // #547 Phase 0 instrumentation: schedule the per-action
            // accumulator commit after a BIS-round-trip wait window.
            // The window is generous (30s) because under healthy load
            // BIS arrives well under 1s; under degraded load we accept
            // partial folding rather than block the spawned task. Pure
            // observability — the commit is a single Cache lookup +
            // histogram observe + invalidate; latency-irrelevant. If
            // no per-digest gaps were folded the commit is a no-op.
            //
            // **New tokio::spawn acknowledgment (code-reviewer MAJOR-1):**
            // This adds one deferred-commit `tokio::spawn` per
            // `spawn_upload_to_remote` call (i.e. per Bazel action with
            // outputs). At a typical steady-state of ~1k actions/min
            // and a 30s sleep window, this is ~500 sleeping tasks
            // resident at any moment. Tokio handles this trivially
            // (each sleeping task is one timer-wheel entry, ~80 B
            // futures); steady-state cost is <1 MiB. The futures
            // perform no I/O, no channel sends, no lock acquisition —
            // they fire one Cache lookup + at most two histogram
            // observations + one Cache invalidate before completing.
            // It will be visible in stack dumps and
            // `RuntimeMetrics::tokio_total_tasks`, which is the only
            // operator-observable difference from a "no behavior
            // change" diff. No flow-control change; the spawn is
            // independent of the upload task's lifecycle.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                worker_phase0_metrics()
                    .commit_action_pin_extension(phase0_action_key);
            });
        });
        // The upload task's `while let Some(ok) = uploads.next().await` (the
        // per-digest retry loop, including the #FL-688 W6 give-up arm that
        // calls `requeue_failed_push`) completes BEFORE the detached 30 s
        // phase0-commit spawn above, so awaiting this handle observes the
        // give-up arm deterministically without waiting on that timer.
        Some(upload_task)
    }

    /// #FL-688 W6 production-composition seam: schedule the real upload task
    /// and return its `JoinHandle` so a test can `.await` it to completion and
    /// observe the give-up arm's `requeue_failed_push` side effect on the
    /// shared `failed_slow_writes` set — driving the REAL
    /// `spawn_upload_to_remote_impl` body (sync-retry exhaustion → the real
    /// `should_requeue_on_giveup` + `requeue_failed_push` call), not a
    /// re-implemented gate. Identical to [`Self::spawn_upload_to_remote`]
    /// except the handle is returned instead of dropped.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn spawn_upload_to_remote_for_test(
        self: &Arc<Self>,
        action_result: &ActionResult,
        action: Option<&Arc<RunningActionImpl>>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        self.spawn_upload_to_remote_impl(action_result, action)
    }

    /// Fixes a race condition that occurs when an action fails to execute on a worker, and the same worker
    /// attempts to re-execute the same action before the physical cleanup (file is removed) completes.
    /// See this issue for additional details: <https://github.com/TraceMachina/nativelink/issues/1859>
    async fn wait_for_cleanup_if_needed(&self, operation_id: &OperationId) -> Result<(), Error> {
        let start = Instant::now();
        let mut backoff = Duration::from_millis(10);
        let mut has_waited = false;

        loop {
            // Subscribe to the Notify BEFORE observing `cleaning_up_operations`
            // so any wake-up that fires after the predicate but before we await
            // is still delivered. `enable()` registers the waker eagerly so the
            // Notified future will accept a permit issued from this point on.
            // The sleep-arm in the select! below remains as belt-and-suspenders.
            let notified = self.cleanup_complete_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let should_wait = {
                let cleaning = self.cleaning_up_operations.lock();
                cleaning.contains(operation_id)
            };

            if !should_wait {
                let dir_path =
                    PathBuf::from(&self.root_action_directory).join(operation_id.to_string());

                if !dir_path.exists() {
                    return Ok(());
                }

                // Safety check: ensure we're only removing directories under root_action_directory
                let root_path = Path::new(&self.root_action_directory);
                let canonical_root = root_path.canonicalize().err_tip(|| {
                    format!(
                        "Failed to canonicalize root directory: {}",
                        self.root_action_directory
                    )
                })?;
                let canonical_dir = dir_path.canonicalize().err_tip(|| {
                    format!("Failed to canonicalize directory: {}", dir_path.display())
                })?;

                if !canonical_dir.starts_with(&canonical_root) {
                    return Err(make_err!(
                        Code::Internal,
                        "Attempted to remove directory outside of root_action_directory: {}",
                        dir_path.display()
                    ));
                }

                // Directory exists but not being cleaned - remove it
                warn!(
                    "Removing stale directory for {}: {}",
                    operation_id,
                    dir_path.display()
                );
                self.metrics.stale_removals.inc();

                // Before remove_dir_all, check if there's a "work" symlink
                // inside (from direct-use mode). If so, remove the symlink
                // first to avoid following it into the cache directory.
                let work_path = dir_path.join("work");
                if let Ok(meta) = fs::symlink_metadata(&work_path).await {
                    if meta.is_symlink() {
                        debug!(
                            "Removing direct-use work symlink before stale cleanup: {}",
                            work_path.display()
                        );
                        drop(fs::remove_file(&work_path).await);
                    }
                }

                // Try to remove the directory, with one retry on failure
                let remove_result = fs::remove_dir_all(&dir_path).await;
                if let Err(e) = remove_result {
                    // Retry once after a short delay in case the directory is temporarily locked
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    fs::remove_dir_all(&dir_path).await.err_tip(|| {
                        format!(
                            "Failed to remove stale directory {} for retry of {} after retry (original error: {})",
                            dir_path.display(),
                            operation_id,
                            e
                        )
                    })?;
                }
                return Ok(());
            }

            if start.elapsed() > Self::MAX_WAIT {
                self.metrics.cleanup_wait_timeouts.inc();
                return Err(make_err!(
                    Code::DeadlineExceeded,
                    "Timeout waiting for previous operation cleanup: {} (waited {:?})",
                    operation_id,
                    start.elapsed()
                ));
            }

            if !has_waited {
                self.metrics.cleanup_waits.inc();
                has_waited = true;
            }

            trace!(
                "Waiting for cleanup of {} (elapsed: {:?}, backoff: {:?})",
                operation_id,
                start.elapsed(),
                backoff
            );

            tokio::select! {
                () = notified.as_mut() => {},
                () = tokio::time::sleep(backoff) => {
                    // Exponential backoff
                    backoff = (backoff * 2).min(Self::MAX_BACKOFF);
                },
            }
        }
    }

    fn make_action_directory<'a>(
        &'a self,
        operation_id: &'a OperationId,
    ) -> impl Future<Output = Result<String, Error>> + 'a {
        self.metrics.make_action_directory.wrap(async move {
            let action_directory = format!("{}/{}", self.root_action_directory, operation_id);
            fs::create_dir(&action_directory)
                .await
                .err_tip(|| format!("Error creating action directory {action_directory}"))?;
            Ok(action_directory)
        })
    }

    fn create_action_info(
        &self,
        start_execute: StartExecute,
        queued_timestamp: SystemTime,
    ) -> impl Future<Output = Result<ActionInfo, Error>> + '_ {
        self.metrics.create_action_info.wrap(async move {
            let execute_request = start_execute
                .execute_request
                .err_tip(|| "Expected execute_request to exist in StartExecute")?;
            let action_digest: DigestInfo = execute_request
                .action_digest
                .clone()
                .err_tip(|| "Expected action_digest to exist on StartExecute")?
                .try_into()?;
            let load_start_timestamp = (self.callbacks.now_fn)();
            let action =
                get_and_decode_digest::<Action>(self.cas_store.as_ref(), action_digest.into())
                    .await
                    .err_tip(|| "During start_action")?;
            let action_info = ActionInfo::try_from_action_and_execute_request(
                execute_request,
                action,
                load_start_timestamp,
                queued_timestamp,
            )
            .err_tip(|| "Could not create ActionInfo in create_and_add_action()")?;
            Ok(action_info)
        })
    }

    fn cleanup_action(&self, operation_id: &OperationId) -> Result<(), Error> {
        // Drop the `running_actions` Mutex guard BEFORE `send_modify`. Holding
        // it across the watch-WRITE that `send_modify` takes formed a lock-order
        // cycle with `kill_all`, whose `wait_for` predicate takes
        // `running_actions.lock()` WHILE holding the watch-READ (Mutex ->
        // watch-write here vs. watch-read -> Mutex there). See the regression
        // test `cleanup_action_kill_all_lock_order_tests`.
        let result = {
            let mut running_actions = self.running_actions.lock();
            running_actions.remove(operation_id).err_tip(|| {
                format!(
                    "Expected operation id '{operation_id}' to exist in RunningActionsManagerImpl"
                )
            })
        }; // guard dropped here — nothing is held across send_modify.
        // No need to copy anything, we just are telling the receivers an event happened.
        self.action_done_tx.send_modify(|()| {});
        result.map(|_| ())
    }

    // Note: We do not capture metrics on this call, only `.kill_all()`.
    // Important: When the future returns the process may still be running.
    async fn kill_operation(action: Arc<RunningActionImpl>) {
        warn!(
            operation_id = ?action.operation_id,
            "Sending kill to running operation",
        );
        // AC-poisoning fix: set cancelled BEFORE the oneshot send.
        // Order matters: a racing publish-closure check at
        // `local_worker.rs:~2474` must see `cancelled=true` even if
        // the oneshot send-loses to the `tokio::select!` returning
        // (e.g. child already exited). Release ordering pairs with
        // the publish closure's Acquire load. Composes with the
        // existing kill_channel_tx (which wakes the select! arm
        // during child-process wait); this flag covers the gap
        // after that arm has returned.
        action.cancelled.store(true, Ordering::Release);
        // Gap 2: wake the upload-tail kill arm. The oneshot below only
        // covers the child-process wait in `inner_execute` (its receiver is
        // gone once the child has exited); this notify is the awaitable
        // signal for a kill that lands DURING the upload tail. `notify_one`
        // stores a permit if no waiter is parked yet, so a kill that races
        // ahead of `upload_results` subscribing is not lost.
        action.kill_notify.notify_one();
        let kill_channel_tx = {
            let mut action_state = action.state.lock();
            action_state.kill_channel_tx.take()
        };
        if let Some(kill_channel_tx) = kill_channel_tx {
            if kill_channel_tx.send(()).is_err() {
                error!(
                    operation_id = ?action.operation_id,
                    "Error sending kill to running operation",
                );
            }
        }
    }

    fn perform_cleanup(self: &Arc<Self>, operation_id: OperationId) -> Option<CleanupGuard> {
        let mut cleaning = self.cleaning_up_operations.lock();
        cleaning
            .insert(operation_id.clone())
            .then_some(CleanupGuard {
                manager: Arc::downgrade(self),
                operation_id,
            })
    }
}

impl RunningActionsManager for RunningActionsManagerImpl {
    type RunningAction = RunningActionImpl;

    async fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        mut start_execute: StartExecute,
    ) -> Result<Arc<RunningActionImpl>, Error> {
        self.metrics
            .create_and_add_action
            .wrap(async move {
                // FL-681 Follow-up A (MAJOR-1b true close-out): admission-side
                // backpressure. In F2 deferred-output mode every output must
                // obtain an INDEFINITE (held-until-BIS-ack) pin. By the time the
                // four pin sites run, the action has already executed and its
                // outputs are on local disk — too late to refuse. The only point
                // where a refusal applies real backpressure to the producer (the
                // scheduler dispatching actions to this worker) is BEFORE the
                // action starts. When the indefinite-pin byte cap is saturated,
                // a fresh output's indefinite pin would be REFUSED and fall back
                // to a time-bounded pin that can expire-and-lose under a sustained
                // outage. NAK the action with `Code::ResourceExhausted` so the
                // scheduler re-queues it (verified: the worker→scheduler
                // ResourceExhausted is treated as re-queue WITHOUT consuming a
                // retry attempt, in
                // `simple_scheduler_state_manager::inner_update_operation`).
                // CAVEAT (verified at `api_worker_scheduler::update_action`): the
                // worker is paused ONLY if it `has_actions()` after the NAKed
                // action is removed; the pending-BIS backlog lives in the PIN set,
                // not `running_action_infos`, so a cap-saturated worker with no
                // other in-flight action is NOT paused, and the re-queued action
                // may re-dispatch here and re-NAK: a bounded RPC-rate spin, NO
                // data loss, self-clearing as BIS-acks drain the cap.
                // TODO(#fl681-resaturation-spin): matcher-side saturation gate or
                // BIS-ack-driven unpause — see
                // .claude/audits/fl681-resaturation-spin-2026-06-18.md.
                // The cap drains as
                // BIS-acks release pins (`unpin_digest`), then admission resumes.
                //
                // Snapshot check — synchronous, eventually-consistent, no lock
                // held across an `.await`; mirrors the `slow_writes_in_flight`
                // byte-budget gate. NOT an async↔sync trip-wire: try-and-NAK,
                // never block-until-headroom on this control path.
                //
                // Composite invariant (admission/eviction/pin triangle):
                //   gate-active ⇒ (indefinite pin works AND BIS-ack release
                //   fires) OR the time-bounded TTL fallback compensates.
                // The gate is the admission corner; the BIS-ack `unpin_digest`
                // pin-release is the corner that drains the cap to clear it.
                // Gate is F2-ONLY: the synchronous path takes no indefinite pins,
                // so a saturated indefinite-pin cap is not a constraint it imposes
                // and the gate stays inert there.
                if self.deferred_output_uploads_enabled
                    && self.filesystem_store.indefinite_pin_saturated()
                {
                    // (FL-681 NAK boundary fix) Count the NAK on the process
                    // singleton so `/metrics` shows the gate is actually firing
                    // (a dead gate reads 0 here, indistinguishable from a healthy
                    // pin budget only by cross-referencing the pinned_bytes/pin_cap
                    // gauges — the FL-681 incident's missing signal). Registered
                    // in nativelink.rs under prefix "worker_admission".
                    nativelink_util::o11_probes::worker_admission_nak_counters()
                        .record_nak_pin_saturated();
                    return Err(make_err!(
                        Code::ResourceExhausted,
                        "worker indefinite-pin cap saturated (pending-BIS durability backlog); \
                         refusing new action so the scheduler re-queues it as backpressure — \
                         admitting it would lose the output when its pin falls back to the 120s TTL"
                    ));
                }

                // Peer hints used to ride inside `StartExecute.peer_hints` and
                // get registered here. As of #98 (peer-hints chunking) hints
                // arrive on a separate `Update::ChunkedMessage` stream owned
                // by `LocalWorkerImpl::run` — they're already in
                // `peer_locality_map` by the time this code runs (or will be
                // shortly; the worker tolerates either ordering because each
                // chunk's hints are independently meaningful).

                // Extract pre-resolved directory tree from the scheduler
                // before consuming start_execute. The parallel arrays are
                // zipped into a HashMap<DigestInfo, Directory>.
                let pre_resolved_tree = if !start_execute.resolved_directories.is_empty()
                    && start_execute.resolved_directories.len()
                        == start_execute.resolved_directory_digests.len()
                {
                    let mut tree = HashMap::with_capacity(
                        start_execute.resolved_directories.len(),
                    );
                    for (dir, digest_proto) in start_execute
                        .resolved_directories
                        .drain(..)
                        .zip(start_execute.resolved_directory_digests.drain(..))
                    {
                        if let Ok(digest_info) = DigestInfo::try_from(&digest_proto) {
                            tree.insert(digest_info, dir);
                        }
                    }
                    info!(
                        dirs = tree.len(),
                        "Received pre-resolved directory tree from scheduler"
                    );
                    Some(tree)
                } else {
                    None
                };

                // Extract server-provided missing digest hints before
                // consuming start_execute.
                let server_missing_digests = if !start_execute.missing_digests.is_empty() {
                    let set: HashSet<DigestInfo> = start_execute
                        .missing_digests
                        .drain(..)
                        .filter_map(|d| DigestInfo::try_from(&d).ok())
                        .collect();
                    info!(
                        hints = set.len(),
                        "Received missing digest hints from scheduler"
                    );
                    Some(set)
                } else {
                    None
                };

                let queued_timestamp = start_execute
                    .queued_timestamp
                    .and_then(|time| time.try_into().ok())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let operation_id = start_execute
                    .operation_id.as_str().into();
                let action_info = self.create_action_info(start_execute, queued_timestamp).await?;
                debug!(
                    ?action_info,
                    "Worker received action",
                );
                // Wait for any previous cleanup to complete before creating directory
                self.wait_for_cleanup_if_needed(&operation_id).await?;
                let action_directory = self.make_action_directory(&operation_id).await?;
                let execution_metadata = ExecutionMetadata {
                    worker: worker_id,
                    queued_timestamp: action_info.insert_timestamp,
                    worker_start_timestamp: action_info.load_timestamp,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                };
                let timeout = if action_info.timeout.is_zero() || self.timeout_handled_externally {
                    self.max_action_timeout
                } else {
                    action_info.timeout
                };
                if timeout > self.max_action_timeout {
                    return Err(make_err!(
                        Code::InvalidArgument,
                        "Action timeout of {} seconds is greater than the maximum allowed timeout of {} seconds",
                        timeout.as_secs_f32(),
                        self.max_action_timeout.as_secs_f32()
                    ));
                }
                let running_action = Arc::new(RunningActionImpl::new(
                    execution_metadata,
                    operation_id.clone(),
                    action_directory,
                    action_info,
                    timeout,
                    self.clone(),
                    pre_resolved_tree,
                    server_missing_digests,
                ));
                {
                    let mut running_actions = self.running_actions.lock();
                    // Check if action already exists and is still alive
                    if let Some(existing_weak) = running_actions.get(&operation_id) {
                        if let Some(_existing_action) = existing_weak.upgrade() {
                            return Err(make_err!(
                                Code::AlreadyExists,
                                "Action with operation_id {} is already running",
                                operation_id
                            ));
                        }
                    }
                    running_actions.insert(operation_id, Arc::downgrade(&running_action));
                }
                Ok(running_action)
            })
            .await
    }

    async fn cache_action_result(
        &self,
        action_info: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
        op_id: &OperationId,
        worker_id: &str,
    ) -> Result<(), Error> {
        self.metrics
            .cache_action_result
            .wrap(self.upload_action_results.cache_action_result(
                action_info,
                action_result,
                hasher,
                op_id,
                worker_id,
            ))
            .await
    }

    async fn kill_operation(&self, operation_id: &OperationId) -> Result<(), Error> {
        let running_action = {
            let running_actions = self.running_actions.lock();
            running_actions
                .get(operation_id)
                .and_then(Weak::upgrade)
                .ok_or_else(|| make_input_err!("Failed to get running action {operation_id}"))?
        };
        Self::kill_operation(running_action).await;
        Ok(())
    }

    // Note: When the future returns the process should be fully killed and cleaned up.
    async fn kill_all(&self) {
        self.metrics
            .kill_all
            .wrap_no_capture_result(async move {
                let kill_operations: Vec<Arc<RunningActionImpl>> = {
                    let running_actions = self.running_actions.lock();
                    running_actions
                        .iter()
                        .filter_map(|(_operation_id, action)| action.upgrade())
                        .collect()
                };
                let mut kill_futures: FuturesUnordered<_> = kill_operations
                    .into_iter()
                    .map(Self::kill_operation)
                    .collect();
                while kill_futures.next().await.is_some() {}
            })
            .await;
        // Ignore error. If error happens it means there's no sender, which is not a problem.
        // Note: Sanity check this API will always check current value then future values:
        // https://play.rust-lang.org/?version=stable&edition=2021&gist=23103652cc1276a97e5f9938da87fdb2
        drop(
            self.action_done_tx
                .subscribe()
                .wait_for(|()| self.running_actions.lock().is_empty())
                .await,
        );
    }

    fn expand_tree_file_digests(
        &self,
        action_result: &ActionResult,
        action: Option<&Arc<Self::RunningAction>>,
    ) -> impl Future<Output = Vec<DigestInfo>> + Send {
        RunningActionsManagerImpl::expand_tree_file_digests(self, action_result, action)
    }

    fn spawn_upload_to_remote(
        self: &Arc<Self>,
        action_result: &ActionResult,
        action: Option<&Arc<Self::RunningAction>>,
    ) {
        RunningActionsManagerImpl::spawn_upload_to_remote(self, action_result, action);
    }

    fn get_cas_store(&self) -> Option<Arc<FastSlowStore>> {
        Some(self.cas_store.clone())
    }

    fn get_directory_cache(&self) -> Option<Arc<crate::directory_cache::DirectoryCache>> {
        self.directory_cache.clone()
    }

    #[inline]
    fn indefinite_pin_saturated(&self) -> bool {
        // Identical store + accessor as the admission gate above
        // (`create_and_add_action`, `:7196`): one relaxed atomic load +
        // compare, no lock, no await. Reporting it on the post-action delta
        // keeps the scheduler's saturation flag honest between heartbeats.
        self.filesystem_store.indefinite_pin_saturated()
    }

    #[inline]
    fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    async fn cached_directory_digests(&self) -> Vec<DigestInfo> {
        match &self.directory_cache {
            Some(cache) => cache.cached_digests().await,
            None => Vec::new(),
        }
    }

    async fn all_subtree_digests(&self) -> Vec<DigestInfo> {
        match &self.directory_cache {
            Some(cache) => cache.all_subtree_digests().await,
            None => Vec::new(),
        }
    }

    async fn take_pending_subtree_changes(&self) -> (Vec<DigestInfo>, Vec<DigestInfo>) {
        match &self.directory_cache {
            Some(cache) => cache.take_pending_subtree_changes().await,
            None => (Vec::new(), Vec::new()),
        }
    }
}

#[derive(Debug, Default, MetricsComponent)]
pub struct Metrics {
    // Note: most fields below are pub(crate)-visible by virtue of the
    // struct itself being `pub` and the fields being inside this
    // crate; the `Metrics` handle is cloned out via `Arc<Metrics>`
    // (returned from `RunningActionsManagerImpl::metrics()` and
    // exposed on `AcMirrorTarget` for #37 Phase 2 BIS-ack tracking).
    #[metric(help = "Stats about the create_and_add_action command.")]
    create_and_add_action: AsyncCounterWrapper,
    #[metric(help = "Stats about the cache_action_result command.")]
    cache_action_result: AsyncCounterWrapper,
    #[metric(help = "Stats about the kill_all command.")]
    kill_all: AsyncCounterWrapper,
    #[metric(help = "Stats about the create_action_info command.")]
    create_action_info: AsyncCounterWrapper,
    #[metric(help = "Stats about the make_work_directory command.")]
    make_action_directory: AsyncCounterWrapper,
    #[metric(help = "Stats about the prepare_action command.")]
    prepare_action: AsyncCounterWrapper,
    #[metric(help = "Stats about the execute command.")]
    execute: AsyncCounterWrapper,
    #[metric(help = "Stats about the upload_results command.")]
    upload_results: AsyncCounterWrapper,
    #[metric(help = "Stats about the cleanup command.")]
    cleanup: AsyncCounterWrapper,
    #[metric(help = "Stats about the get_finished_result command.")]
    get_finished_result: AsyncCounterWrapper,
    #[metric(help = "Number of times an action waited for cleanup to complete.")]
    cleanup_waits: CounterWithTime,
    #[metric(help = "Number of stale directories removed during action retries.")]
    stale_removals: CounterWithTime,
    #[metric(help = "Number of timeouts while waiting for cleanup to complete.")]
    cleanup_wait_timeouts: CounterWithTime,
    #[metric(help = "Stats about the get_proto_command_from_store command.")]
    get_proto_command_from_store: AsyncCounterWrapper,
    #[metric(help = "Stats about the download_to_directory command.")]
    download_to_directory: AsyncCounterWrapper,
    #[metric(help = "Stats about the prepare_output_files command.")]
    prepare_output_files: AsyncCounterWrapper,
    #[metric(help = "Stats about the prepare_output_paths command.")]
    prepare_output_paths: AsyncCounterWrapper,
    #[metric(help = "Stats about the child_process command.")]
    child_process: AsyncCounterWrapper,
    #[metric(help = "Stats about the child_process_success_error_code command.")]
    child_process_success_error_code: CounterWithTime,
    #[metric(help = "Stats about the child_process_failure_error_code command.")]
    child_process_failure_error_code: CounterWithTime,
    #[metric(help = "Total time spent uploading stdout.")]
    upload_stdout: AsyncCounterWrapper,
    #[metric(help = "Total time spent uploading stderr.")]
    upload_stderr: AsyncCounterWrapper,
    #[metric(help = "Total number of task timeouts.")]
    task_timeouts: CounterWithTime,
    // #37 Phase 2 (Q1+Q2+Q3): worker AC publish observability counters.
    // Hand-rolled per-Code field set (design v2 §2.4) — `MetricsComponent`
    // derive does not support `HashMap<Code, Counter>` natively; the
    // helper `worker_ac_publish_fail_by_code` dispatches to the right
    // field via `match`. All `pub` so integration tests can read
    // `.counter.load(Ordering::Acquire)` directly.
    #[metric(help = "Worker AC publish success count.")]
    pub worker_ac_publish_success: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — Aborted.")]
    pub worker_ac_publish_fail_aborted: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — Internal.")]
    pub worker_ac_publish_fail_internal: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — NotFound.")]
    pub worker_ac_publish_fail_not_found: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — ResourceExhausted.")]
    pub worker_ac_publish_fail_resource_exhausted: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — Unavailable.")]
    pub worker_ac_publish_fail_unavailable: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — DeadlineExceeded.")]
    pub worker_ac_publish_fail_deadline_exceeded: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — Unknown.")]
    pub worker_ac_publish_fail_unknown: CounterWithTime,
    #[metric(help = "Worker AC publish fail count — all other codes.")]
    pub worker_ac_publish_fail_other: CounterWithTime,
    #[metric(help = "Worker AC publish events exceeding 500ms.")]
    pub worker_ac_publish_slow: CounterWithTime,
    // #37 Phase 2 (Q5): BIS-ack observability counters. `pub`
    // because the BIS-ack receive site lives in `local_worker.rs`
    // (different module) and increments via `target.metrics.<field>`.
    #[metric(help = "Worker AC BIS-acks received from server (per-digest).")]
    pub worker_bis_ack_received: CounterWithTime,
    #[metric(help = "Worker AC BIS-acks confirmed missing after timeout.")]
    pub worker_bis_ack_missing: CounterWithTime,
    // #37 Phase 2 (Q4): slow-tier async failure per store_class. `pub`
    // so integration tests in `tests/` can read
    // `.counter.load(Ordering::Acquire)` to verify the cross-crate
    // sink plumbed the increment end-to-end (T3).
    #[metric(help = "Worker AC slow-tier async write fail count.")]
    pub worker_slow_tier_async_fail_ac: CounterWithTime,
    #[metric(help = "Worker CAS slow-tier async write fail count.")]
    pub worker_slow_tier_async_fail_cas: CounterWithTime,
    #[metric(help = "Worker slow-tier async write fail — unknown store class.")]
    pub worker_slow_tier_async_fail_unknown: CounterWithTime,
    // #86 NOTE: `symlink_fix_lock_acquires_total` and
    // `symlink_fix_slow_path_entries_total` were removed from this struct.
    // They are now a process-global singleton in
    // `nativelink_util::o11_probes::SYMLINK_FIX_COUNTERS`, registered with
    // `MetricsRegistry` and visible on `/metrics`. Increments route through
    // `symlink_fix_counters()` in `prepare_output_directory`.
}

impl Metrics {
    /// Dispatch a failure increment for an AC publish error to the
    /// matching per-Code field. Unmapped codes land in `other`.
    /// See design v2 §2.4 and the hand-rolled field set above.
    pub fn worker_ac_publish_fail_by_code(&self, code: Code) {
        match code {
            Code::Aborted => self.worker_ac_publish_fail_aborted.inc(),
            Code::Internal => self.worker_ac_publish_fail_internal.inc(),
            Code::NotFound => self.worker_ac_publish_fail_not_found.inc(),
            Code::ResourceExhausted => self.worker_ac_publish_fail_resource_exhausted.inc(),
            Code::Unavailable => self.worker_ac_publish_fail_unavailable.inc(),
            Code::DeadlineExceeded => self.worker_ac_publish_fail_deadline_exceeded.inc(),
            Code::Unknown => self.worker_ac_publish_fail_unknown.inc(),
            _ => self.worker_ac_publish_fail_other.inc(),
        }
    }

    /// Dispatch a slow-tier async failure increment per store_class label.
    /// Unknown labels land in the `unknown` bucket. `pub` so callers
    /// outside the crate (sink impls, future server-side use) can
    /// dispatch by label.
    pub fn worker_slow_tier_async_fail_by_class(&self, store_class: &str) {
        match store_class {
            "ac" => self.worker_slow_tier_async_fail_ac.inc(),
            "cas" => self.worker_slow_tier_async_fail_cas.inc(),
            _ => self.worker_slow_tier_async_fail_unknown.inc(),
        }
    }
}

#[cfg(test)]
mod assert_tree_complete_tests {
    //! Tests for [`assert_tree_complete`] structural validation. The
    //! function is the last line of defense between [`resolve_directory_tree`]
    //! and [`DirectoryCache::get_or_construct`] — an incomplete or cyclic
    //! tree slipped past here would either drop subdirectories silently or
    //! loop forever in the materialization BFS.

    use std::collections::HashMap;

    use nativelink_error::Code;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Digest as ProtoDigest, Directory as ProtoDirectory, DirectoryNode,
    };
    use nativelink_util::common::DigestInfo;

    use super::assert_tree_complete;

    /// Build a `DigestInfo` from a single-byte hash pattern. The hash is
    /// deterministic by `tag`, which makes failure messages readable.
    fn digest(tag: u8, size: u64) -> DigestInfo {
        let hex: String = (0..32).map(|_| format!("{tag:02x}")).collect();
        DigestInfo::try_new(&hex, size).unwrap()
    }

    /// Proto digest that points to the same content as `DigestInfo::digest(tag, size)`.
    fn proto_digest(tag: u8, size: u64) -> ProtoDigest {
        let hex: String = (0..32).map(|_| format!("{tag:02x}")).collect();
        ProtoDigest {
            hash: hex,
            size_bytes: size as i64,
        }
    }

    /// Construct a directory node (child reference) with the given name/digest.
    fn dir_node(name: &str, d: ProtoDigest) -> DirectoryNode {
        DirectoryNode {
            name: name.to_string(),
            digest: Some(d),
        }
    }

    // (f) happy path: root -> child, both digests present in the tree, no
    // cycles. Must succeed.
    #[test]
    fn happy_path_two_node_tree_ok() {
        let root = digest(1, 100);
        let child = digest(2, 200);
        let mut tree = HashMap::new();
        tree.insert(
            root,
            ProtoDirectory {
                directories: vec![dir_node("subdir", proto_digest(2, 200))],
                ..Default::default()
            },
        );
        tree.insert(child, ProtoDirectory::default());
        assert!(
            assert_tree_complete(&tree, &root, "test: happy").is_ok(),
            "two-node acyclic tree should validate",
        );
    }

    // (g) missing root: resolved map does not contain root_digest. Must
    // return Internal error with the "missing reachable directory" message.
    #[test]
    fn missing_root_returns_internal_error() {
        let root = digest(1, 100);
        let tree: HashMap<DigestInfo, ProtoDirectory> = HashMap::new();
        let err = assert_tree_complete(&tree, &root, "test: missing-root")
            .expect_err("missing root must error");
        assert_eq!(err.code, Code::Internal, "got {err:?}");
        assert!(
            err.messages.iter().any(|m| m.contains("missing reachable directory")),
            "got {:?}",
            err.messages,
        );
    }

    // (h) missing child: root references a child digest that is not in
    // the tree map. Must return Internal with "missing referenced child".
    #[test]
    fn missing_child_returns_internal_error() {
        let root = digest(1, 100);
        let mut tree = HashMap::new();
        tree.insert(
            root,
            ProtoDirectory {
                directories: vec![dir_node("dangling", proto_digest(2, 200))],
                ..Default::default()
            },
        );
        // Note: digest(2, 200) intentionally NOT inserted.
        let err = assert_tree_complete(&tree, &root, "test: missing-child")
            .expect_err("missing child must error");
        assert_eq!(err.code, Code::Internal, "got {err:?}");
        assert!(
            err.messages.iter().any(|m| m.contains("missing referenced child")),
            "got {:?}",
            err.messages,
        );
    }

    // (i) malformed child digest: node has no digest set at all. Must
    // return InvalidArgument with "missing digest".
    #[test]
    fn malformed_child_digest_returns_invalid_argument() {
        let root = digest(1, 100);
        let mut tree = HashMap::new();
        tree.insert(
            root,
            ProtoDirectory {
                directories: vec![DirectoryNode {
                    name: "missing-digest".to_string(),
                    digest: None, // intentionally malformed
                }],
                ..Default::default()
            },
        );
        let err = assert_tree_complete(&tree, &root, "test: malformed")
            .expect_err("missing digest must error");
        assert_eq!(err.code, Code::InvalidArgument, "got {err:?}");
        assert!(
            err.messages.iter().any(|m| m.contains("missing digest")),
            "got {:?}",
            err.messages,
        );
    }

    // (j) self-loop: root directory references itself as a child. Must
    // detect cycle, return Internal with "directory cycle detected".
    #[test]
    fn self_loop_returns_cycle_error() {
        let root = digest(1, 100);
        let mut tree = HashMap::new();
        tree.insert(
            root,
            ProtoDirectory {
                directories: vec![dir_node("me", proto_digest(1, 100))],
                ..Default::default()
            },
        );
        let err = assert_tree_complete(&tree, &root, "test: self-loop")
            .expect_err("self-loop must error");
        assert_eq!(err.code, Code::Internal, "got {err:?}");
        assert!(
            err.messages.iter().any(|m| m.contains("directory cycle detected")),
            "got {:?}",
            err.messages,
        );
    }

    // (k) multi-node cycle: A -> B -> C -> A. The DFS ancestor tracking
    // must detect the back-edge on entering A from C's child list.
    #[test]
    fn multi_node_cycle_returns_cycle_error() {
        let a = digest(1, 100);
        let b = digest(2, 200);
        let c = digest(3, 300);
        let mut tree = HashMap::new();
        tree.insert(
            a,
            ProtoDirectory {
                directories: vec![dir_node("b", proto_digest(2, 200))],
                ..Default::default()
            },
        );
        tree.insert(
            b,
            ProtoDirectory {
                directories: vec![dir_node("c", proto_digest(3, 300))],
                ..Default::default()
            },
        );
        tree.insert(
            c,
            ProtoDirectory {
                directories: vec![dir_node("a", proto_digest(1, 100))],
                ..Default::default()
            },
        );
        let err = assert_tree_complete(&tree, &a, "test: 3-cycle")
            .expect_err("3-node cycle must error");
        assert_eq!(err.code, Code::Internal, "got {err:?}");
        assert!(
            err.messages.iter().any(|m| m.contains("directory cycle detected")),
            "got {:?}",
            err.messages,
        );
    }

    // (l) diamond DAG: root -> {left, right}, both -> leaf. Leaf is
    // reached twice from root but is NOT a cycle (no path from leaf back
    // to any ancestor). Must succeed — this is the critical distinction
    // between finished-set revisit (diamond, OK) and ancestor-set revisit
    // (cycle, error).
    #[test]
    fn diamond_dag_is_not_a_cycle() {
        let root = digest(1, 100);
        let left = digest(2, 200);
        let right = digest(3, 300);
        let leaf = digest(4, 400);
        let mut tree = HashMap::new();
        tree.insert(
            root,
            ProtoDirectory {
                directories: vec![
                    dir_node("left", proto_digest(2, 200)),
                    dir_node("right", proto_digest(3, 300)),
                ],
                ..Default::default()
            },
        );
        tree.insert(
            left,
            ProtoDirectory {
                directories: vec![dir_node("leaf", proto_digest(4, 400))],
                ..Default::default()
            },
        );
        tree.insert(
            right,
            ProtoDirectory {
                directories: vec![dir_node("leaf", proto_digest(4, 400))],
                ..Default::default()
            },
        );
        tree.insert(leaf, ProtoDirectory::default());
        assert!(
            assert_tree_complete(&tree, &root, "test: diamond").is_ok(),
            "diamond DAG is not a cycle and must validate",
        );
    }
}

#[cfg(test)]
mod cleanup_wait_notify_parity_tests {
    //! Regression test for the `wait_for_cleanup_if_needed` lost-wakeup window.
    //!
    //! The loop body in `wait_for_cleanup_if_needed` observes a predicate
    //! (`cleaning_up_operations.contains(...)`) and then awaits a
    //! `tokio::sync::Notify` in a `select!` with a backoff sleep. If the
    //! `notified()` future is created AFTER the predicate observation, a
    //! permit issued in between is lost — the waiter falls through to the
    //! sleep arm, paying the backoff latency.
    //!
    //! The fix subscribes (`notified()` + `enable()`) BEFORE observing the
    //! predicate. This is critical because the producer side calls
    //! `notify_waiters()` (see `cleanup_action` -> `cleanup_complete_notify`
    //! call site), which — unlike `notify_one` — does NOT store a permit; it
    //! only wakes currently-registered waiters. Without `enable()`, a Notified
    //! future that has never been polled is not yet registered, so a
    //! `notify_waiters()` call during the predicate window is silently lost
    //! until the backoff sleep elapses.
    //!
    //! Note on mutation testing: removing `enable()` does NOT make this test
    //! fail in the simple single-threaded case, because tokio's `Notified`
    //! tracks a "notify epoch" captured at construction and checked on first
    //! poll — so a `notify_waiters` that fires between subscribe and poll
    //! is still observed once the future is finally polled. The real value
    //! of `enable()` is on multi-threaded runtimes where the producer and
    //! waiter race on the same `Notified`, and as defense-in-depth: it makes
    //! the subscribe-before-check ordering explicit and survives future code
    //! changes that might drop or refactor the polling site. This test
    //! therefore documents the contract (Notified must observe a notify
    //! issued between subscribe and await) without claiming to falsify a
    //! single-threaded mutation.
    use core::time::Duration;
    use std::sync::Arc;

    use tokio::sync::Notify;
    use tokio::time::Instant;

    #[tokio::test(flavor = "current_thread")]
    async fn enable_before_predicate_captures_concurrent_notify() {
        let notify = Arc::new(Notify::new());

        // Mirror the loop body shape: subscribe + enable BEFORE observing the
        // predicate. enable() arms the waker so that any notify issued from
        // this point on will be delivered to the pinned future.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Simulate the "predicate observation" window during which a
        // concurrent producer issues notify_waiters(). This call does NOT
        // store a permit — it only wakes currently-registered waiters. Our
        // pre-enable() registration is what makes this delivery succeed.
        notify.notify_waiters();

        let start = Instant::now();
        // The sleep arm is set to a duration far longer than any plausible
        // notified-future poll latency. If we hit it, the permit was lost.
        let backoff = Duration::from_secs(5);
        tokio::select! {
            () = notified.as_mut() => {
                let elapsed = start.elapsed();
                assert!(
                    elapsed < Duration::from_millis(100),
                    "notified arm took {elapsed:?} — permit issued during predicate window \
                     should have been captured by the pre-enabled Notified future",
                );
            }
            () = tokio::time::sleep(backoff) => {
                panic!(
                    "fell through to sleep arm — concurrent notify_one() was lost because \
                     the Notified future was not subscribed/enabled before the predicate \
                     window. This is the lost-wakeup regression."
                );
            }
        }
    }
}

#[cfg(test)]
mod fetched_notify_subscribe_before_predicate_tests {
    //! Regression test for #92: lost-wakeup window in the deferred-files
    //! producer loop in `download_to_directory` (file-materialization
    //! poll loop near line 1696-1755).
    //!
    //! The loop subscribes to `fetched_notify` AFTER snapshotting
    //! `fetched_set`. Today the producer uses `notify_one`, which DOES
    //! store a permit on a missed wakeup, so the immediate symptom of
    //! the after-snapshot-then-await order is at-most-one-extra
    //! iteration of the loop, not a hard deadlock. However:
    //!
    //!   1. Defense-in-depth: documenting the subscribe-before-predicate
    //!      contract makes the ordering survive future producer
    //!      changes (e.g., switching to `notify_waiters` for a
    //!      broadcast-style consumer fan-out as in
    //!      `cleanup_complete_notify`).
    //!   2. Sibling parity: the cleanup_wait_notify_parity_tests
    //!      reference at line ~5715-5807 documents the same contract
    //!      for the cleanup path; this test does the same for the
    //!      file-fetch path.
    //!
    //! The shape mirrors the production loop (subscribe → snapshot →
    //! dispatch → await) and the failure mode is the
    //! `notify_waiters`-flavored producer (no permit storage) firing
    //! during the snapshot window. With subscribe-before-predicate
    //! (the fix), the pre-enabled Notified observes the wakeup and the
    //! await completes promptly. With subscribe-after-predicate (the
    //! mutation), the Notified is constructed AFTER the wakeup
    //! evaporates and the await falls through to the deadlock
    //! detector.
    //!
    //! NOTE on test discipline (CLAUDE.md
    //! `feedback_lost_wakeup_test_theatre`): we use paused tokio time
    //! + a `tokio::time::timeout` deadlock detector, NOT `sleep` as a
    //! synchronization primitive. The Barrier coordinates the
    //! predicate window <-> notify-fire ordering deterministically.
    use core::time::Duration;
    use std::sync::Arc;

    use tokio::sync::{Barrier, Notify};

    /// Subscribe-before-predicate (the fix at #92): the Notified
    /// future is constructed BEFORE the snapshot window, so a
    /// `notify_waiters` issued during that window is delivered. With
    /// the contract violated (subscribe AFTER predicate), the
    /// `notify_waiters` evaporates because no waiter is registered,
    /// and the await blocks forever.
    ///
    /// We use `notify_waiters()` (not `notify_one()`) because
    /// `notify_waiters` has the no-permit-storage semantics that make
    /// the lost wakeup reproducible. The production producer uses
    /// `notify_one`, which stores one permit and so does not deadlock
    /// today; this test is defense-in-depth for the contract: any
    /// future producer change toward `notify_waiters` (broadcast
    /// fan-out) inherits the safety the subscribe-before order
    /// provides.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_before_predicate_captures_wakeup() {
        let notify = Arc::new(Notify::new());
        let barrier = Arc::new(Barrier::new(2));

        // Producer: wait at the barrier, then fire notify_waiters()
        // immediately. notify_waiters() does NOT store a permit — it
        // only wakes waiters currently registered.
        let prod_notify = notify.clone();
        let prod_barrier = barrier.clone();
        tokio::spawn(async move {
            prod_barrier.wait().await;
            prod_notify.notify_waiters();
        });

        // Consumer mirrors the production loop body shape (#92 fix):
        // subscribe FIRST, enable, then enter the snapshot window.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Snapshot window: release the producer to fire its
        // notify_waiters(). The barrier acts as a happens-before
        // synchronization point: the producer's notify is emitted
        // strictly after this point, while we are still in the
        // snapshot window — strictly before we reach the await
        // below.
        barrier.wait().await;

        // Award: the pre-enabled Notified must observe the wakeup
        // issued during the snapshot window. 2s real-wall-clock is a
        // deadlock detector, NOT synchronization.
        tokio::time::timeout(Duration::from_secs(2), notified.as_mut())
            .await
            .expect(
                "lost-wakeup race — must subscribe before predicate (#92): \
                 notify_waiters() fired during the snapshot window was lost",
            );
    }
}

#[cfg(test)]
mod cleanup_action_kill_all_lock_order_tests {
    //! Regression test for the `cleanup_action` <-> `kill_all` lock-order
    //! inversion deadlock that wedged worker `isotope` for ~1.5h.
    //!
    //! Two real primitives are involved (verified against the production
    //! struct `RunningActionsManagerImpl`):
    //!   - `running_actions: parking_lot::Mutex<HashMap<OperationId, ..>>`
    //!   - `action_done_tx: tokio::sync::watch::Sender<()>`
    //!
    //! `watch::Sender::send_modify` takes the watch channel's INTERNAL
    //! `parking_lot::RwLock` for WRITE (verified in tokio 1.49.0
    //! `watch.rs::send_if_modified`: `self.shared.value.write()`).
    //! `watch::Receiver::wait_for` takes that same internal RwLock for READ
    //! and evaluates the predicate WHILE HOLDING IT (verified in tokio 1.49.0
    //! `watch.rs::wait_for_inner`: `let inner = self.shared.value.read();`
    //! then `f(&inner)` inside the same block).
    //!
    //! The two production lock orders:
    //!   - `cleanup_action`: `running_actions.lock()` held, THEN
    //!     `action_done_tx.send_modify()` => Mutex -> watch-WRITE.
    //!   - `kill_all`: `wait_for(|()| running_actions.lock().is_empty())`
    //!     => watch-READ held across the predicate, THEN Mutex inside it
    //!     => watch-READ -> Mutex.
    //!
    //! Inverted order across the two real locks => a true cycle. The fix
    //! drops the `running_actions` guard in `cleanup_action` BEFORE calling
    //! `send_modify`, so the Mutex is never held across the watch-write and
    //! the cycle is broken.
    //!
    //! Test discipline (CLAUDE.md): no sleep-as-synchronization. The
    //! predicate-window happens-before is established with an `mpsc` channel
    //! the predicate sends on (while holding the watch-READ lock) immediately
    //! BEFORE it blocks acquiring the Mutex; the cleanup side waits on that
    //! channel before taking the watch-WRITE. A `tokio::time::timeout`
    //! wraps the whole interleave purely as a DEADLOCK DETECTOR (real wall
    //! clock), never as synchronization.
    //!
    //! Why a minimal struct rather than the real `RunningActionsManagerImpl`:
    //! `RunningActionsManagerImpl::new_with_callbacks` mandatorily downcasts
    //! `cas_store.fast_store()` to a concrete `FilesystemStore` (constructor
    //! `?`-errors otherwise), so building one requires a temp-dir filesystem
    //! fixture AND gives no hook to align the inversion window inside the
    //! opaque bodies of `cleanup_action`/`kill_all`. The deadlock is a
    //! property of `parking_lot::Mutex` x tokio `watch`'s internal RwLock,
    //! NOT of the manager's other fields, so this harness holds the SAME two
    //! real primitives and replicates the two real lock sequences verbatim.
    //! The functional `cleanup_action` contract (remove + wake) is covered
    //! against the REAL method in
    //! `cleanup_action_real_method_remove_and_wake_tests`.
    use core::time::Duration;
    use std::collections::HashMap;
    use std::sync::mpsc as std_mpsc;
    use std::sync::Arc;
    use std::thread;

    use parking_lot::Mutex;
    use tokio::sync::watch;

    /// Minimal mirror of the two production fields. Holds the SAME two real
    /// primitives as `RunningActionsManagerImpl`.
    struct LockPair {
        running_actions: Mutex<HashMap<u64, ()>>,
        action_done_tx: watch::Sender<()>,
    }

    /// Faithful shadow of the production `cleanup_action` LOCK SEQUENCE, kept in
    /// lock-order parity with `RunningActionsManagerImpl::cleanup_action`.
    ///
    /// The `read_held_rx` handshake makes the inversion window DETERMINISTIC:
    /// after taking the Mutex this side blocks (synchronously) until the
    /// kill_all side reports it holds the watch-READ lock. Only THEN does it
    /// reach `send_modify` (watch-WRITE). This forces the exact overlap the
    /// production deadlock needs.
    ///
    /// FIXED order (current production after the fix): the Mutex guard is
    /// dropped BEFORE the handshake + `send_modify`, so the watch-WRITE is
    /// never attempted while the Mutex is held — no cycle.
    ///
    /// MUTATION for the deadlock proof: move the `drop(running_actions)` to
    /// AFTER `send_modify` (i.e. hold the guard across the handshake +
    /// send_modify, the pre-fix order). With the guard held,
    /// `interleaved_with_kill_all_does_not_deadlock` surfaces a clean FAILED
    /// with the bespoke message because the two real locks form a cycle.
    fn cleanup_action_lock_sequence(
        pair: &LockPair,
        op: u64,
        mutex_held_tx: &std_mpsc::Sender<()>,
        read_held_rx: &std_mpsc::Receiver<()>,
    ) {
        let mut running_actions = pair.running_actions.lock();
        running_actions.remove(&op);
        // Tell the driver the Mutex is held so it can release the kill_all
        // side; kill_all's predicate will then attempt this same Mutex.
        mutex_held_tx
            .send(())
            .expect("driver must receive mutex_held");
        // FIXED: release the Mutex BEFORE the watch-WRITE. (Mutation: delete
        // this `drop` line and add `drop(running_actions);` AFTER send_modify
        // — i.e. hold the guard across the watch-WRITE, the pre-fix order.)
        drop(running_actions);
        // Wait until kill_all holds the watch-READ before taking the
        // watch-WRITE, so the test deterministically drives the inversion
        // window rather than relying on timing. (In the FIXED order the Mutex
        // is already released here, so kill_all's pending lock() succeeds and
        // no cycle forms; in the buggy order the guard above is still held and
        // send_modify below blocks on the watch-WRITE behind kill_all's READ.)
        let _ = read_held_rx.recv();
        // No need to copy anything, we just are telling the receivers an event
        // happened (mirror of the production comment + call).
        pair.action_done_tx.send_modify(|()| {});
    }

    /// Drives `kill_all`'s lock sequence: `wait_for` holds the watch-READ lock
    /// across the predicate, which acquires the Mutex. From inside the predicate
    /// (watch-READ held) it signals `read_held_tx` EXACTLY ONCE immediately
    /// before attempting `running_actions.lock()` — the cleanup side blocks on
    /// that signal, so the watch-WRITE is only attempted while this watch-READ
    /// is live.
    async fn kill_all_lock_sequence(
        pair: Arc<LockPair>,
        read_held_tx: std_mpsc::Sender<()>,
    ) {
        let mut rx = pair.action_done_tx.subscribe();
        let mut signalled = false;
        drop(
            rx.wait_for(move |()| {
                // Inside wait_for_inner: the watch-READ lock is held here.
                if !signalled {
                    signalled = true;
                    // Tell the cleanup side the watch-READ is held; it will now
                    // proceed toward the watch-WRITE.
                    let _ = read_held_tx.send(());
                }
                // watch-READ held -> acquire the Mutex (kill_all order). `pair`
                // is moved into this closure (last use), so no `Arc::clone` is
                // needed.
                pair.running_actions.lock().is_empty()
            })
            .await,
        );
    }

    /// Shared interleave driver. Drives the FIXED `cleanup_action` lock order
    /// against `kill_all`'s order, forcing the inversion window with the
    /// two-signal handshake. Returns `Ok(())` if both sides finish within the
    /// deadlock-detector window, `Err(())` if they deadlock.
    ///
    /// DIAGNOSABILITY (why dedicated `std::thread`s, not tokio workers): under
    /// the buggy mutation both lock sequences wedge forever — the cleanup side
    /// holds the parking_lot Mutex blocked on the watch-WRITE, and the kill
    /// side blocks SYNCHRONOUSLY on `running_actions.lock()` inside `wait_for`'s
    /// predicate. If those sat on the test's own tokio runtime (a wedged worker
    /// via `spawn`/`spawn_blocking`), the runtime could never reclaim them at
    /// test teardown and the binary would HANG past the panic — no
    /// `test result: FAILED`, no bespoke message in default capture, only an
    /// outer-wrapper `timeout` KILL. By confining each blocking sequence to a
    /// detached `std::thread` (the kill side drives its async `wait_for` on its
    /// OWN current-thread runtime), the deadlock leaves only plain OS threads
    /// wedged. The driver detects the deadlock via a bounded `recv_timeout` on a
    /// result channel and returns `Err`; the caller's `.expect` then panics on
    /// the MAIN test thread, printing the bespoke message and a clean `FAILED`.
    /// Detached non-daemon threads do not block process exit — the harness
    /// `exit()`s without joining them — so the binary terminates promptly.
    fn run_interleave() -> Result<(), ()> {
        let (tx, _rx_keepalive) = watch::channel(());
        let pair = Arc::new(LockPair {
            running_actions: Mutex::new(HashMap::from([(1_u64, ())])),
            action_done_tx: tx,
        });

        // C -> driver: "I hold the Mutex; start the kill_all side now."
        let (mutex_held_tx, mutex_held_rx) = std_mpsc::channel();
        // K -> C: "I hold the watch-READ; proceed toward the watch-WRITE."
        let (read_held_tx, read_held_rx) = std_mpsc::channel();
        // Each side -> driver: "I finished my lock sequence." Two completions
        // expected; absence within the window == deadlock.
        let (done_tx, done_rx) = std_mpsc::channel();

        // Cleanup side: a plain blocking sequence on a dedicated OS thread. In
        // the buggy order it holds a synchronous parking_lot Mutex across a sync
        // recv + watch-WRITE; on a detached std::thread that wedge cannot park
        // the test's tokio runtime.
        let cleanup_pair = Arc::clone(&pair);
        let cleanup_done = done_tx.clone();
        thread::Builder::new()
            .name("cleanup_lock_seq".into())
            .spawn(move || {
                cleanup_action_lock_sequence(&cleanup_pair, 1, &mutex_held_tx, &read_held_rx);
                let _ = cleanup_done.send(());
            })
            .expect("spawn cleanup thread");

        // Wait until cleanup holds the Mutex (signal sent from inside the
        // critical section), THEN start kill_all so its predicate contends for
        // that same Mutex.
        mutex_held_rx
            .recv()
            .expect("cleanup side must signal mutex_held before kill_all starts");

        // Kill side: its lock sequence is async (`wait_for().await`), so it runs
        // on its OWN current-thread runtime confined to a dedicated OS thread.
        // A wedge here parks only this thread + this private runtime, never the
        // test's runtime.
        let kill_done = done_tx;
        thread::Builder::new()
            .name("kill_all_lock_seq".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build kill_all current-thread runtime");
                rt.block_on(kill_all_lock_sequence(pair, read_held_tx));
                let _ = kill_done.send(());
            })
            .expect("spawn kill_all thread");

        // Bounded deadlock detector (real wall clock, NOT synchronization): both
        // sides must report completion within the window. `recv_timeout` returns
        // `Err` on the first missing completion => deadlock. The wedged OS
        // threads are intentionally left detached (joining them would hang);
        // they do not block process exit.
        let deadline = Duration::from_secs(5);
        for _ in 0..2 {
            if done_rx.recv_timeout(deadline).is_err() {
                return Err(());
            }
        }
        Ok(())
    }

    /// Load-bearing regression: with the FIXED `cleanup_action` lock order
    /// (Mutex guard dropped before `send_modify`), interleaving it with
    /// `kill_all`'s watch-READ -> Mutex order completes well under the
    /// deadlock-detector window.
    ///
    /// MUTATION: in `cleanup_action_lock_sequence`, move the
    /// `drop(running_actions)` to AFTER `send_modify` (hold the guard across the
    /// watch-WRITE — the pre-fix order). `run_interleave` then returns `Err`,
    /// this `.expect` panics on the MAIN test thread, and the test reports a
    /// clean `FAILED` with the bespoke message below in DEFAULT capture mode
    /// (no `--nocapture`) within the detector window — NOT an opaque hang.
    /// (Verified: the buggy order surfaces FAILED + this message in default
    /// capture; the fixed order returns immediately and reports `ok`.)
    #[test]
    fn interleaved_with_kill_all_does_not_deadlock() {
        run_interleave().expect(
            "cleanup_action/kill_all lock-order DEADLOCK: cleanup_action held the \
             running_actions Mutex across action_done_tx.send_modify (watch-WRITE) while \
             kill_all held the watch-READ across running_actions.lock() — the guard MUST be \
             dropped before send_modify to break the cycle",
        );
    }
}

#[cfg(test)]
mod cleanup_action_real_method_remove_and_wake_tests {
    //! Functional regression for the REAL
    //! `RunningActionsManagerImpl::cleanup_action`. The deadlock fix narrows
    //! the `running_actions` critical section so the Mutex is no longer held
    //! across `action_done_tx.send_modify`. This test pins BOTH halves of the
    //! method's contract against the actual production method so the fix
    //! cannot regress either:
    //!   (a) the operation is REMOVED from `running_actions`, and
    //!   (b) `action_done_tx` waiters are WOKEN (the watch version is bumped).
    //!
    //! It drives the real private method on a real `RunningActionsManagerImpl`
    //! (an in-crate test can reach private items). The manager is built with a
    //! temp-dir `FilesystemStore` fast tier + `MemoryStore` slow tier because
    //! `new_with_callbacks` mandatorily downcasts `cas_store.fast_store()` to a
    //! concrete `FilesystemStore`.
    //!
    //! MUTATION (wake half): delete the `self.action_done_tx.send_modify(...)`
    //! line in `cleanup_action`; the `wait_for` below never observes a change
    //! and the `tokio::time::timeout` fires the bespoke message.
    //! MUTATION (remove half): delete the `running_actions.remove(...)` line;
    //! the post-condition `assert!(... is_none())` fails with its message.
    use core::time::Duration;
    use std::sync::{Arc, Weak};

    use nativelink_config::cas_server::{
        UploadActionResultConfig, UploadCacheResultsStrategy,
    };
    use nativelink_config::stores::{
        FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
    };
    use nativelink_store::fast_slow_store::FastSlowStore;
    use nativelink_store::filesystem_store::FilesystemStore;
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::action_messages::OperationId;
    use nativelink_util::store_trait::Store;

    use super::{
        ExecutionConfiguration, RunningActionsManagerArgs, RunningActionsManagerImpl,
    };

    /// Build a minimal-but-REAL `RunningActionsManagerImpl`. The fast tier is a
    /// `FilesystemStore` rooted in a fresh `tempfile::TempDir` (returned so it
    /// outlives the manager), the slow tier is an in-memory store.
    async fn make_real_manager() -> (Arc<RunningActionsManagerImpl>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let content_path = tmp.path().join("content");
        let temp_path = tmp.path().join("temp");
        std::fs::create_dir_all(&content_path).expect("mk content_path");
        std::fs::create_dir_all(&temp_path).expect("mk temp_path");

        let fast_spec = FilesystemSpec {
            content_path: content_path.to_string_lossy().into_owned(),
            temp_path: temp_path.to_string_lossy().into_owned(),
            eviction_policy: None,
            ..Default::default()
        };
        let slow_spec = MemorySpec::default();
        let fast_store: Arc<FilesystemStore> = FilesystemStore::new(&fast_spec)
            .await
            .expect("FilesystemStore::new");
        let slow_store = MemoryStore::new(&slow_spec);
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_spec),
                slow: StoreSpec::Memory(slow_spec.clone()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
                slow_writes_in_flight_max_bytes: 0,
            },
            Store::new(fast_store),
            Store::new(slow_store),
        );

        let upload_cfg = UploadActionResultConfig {
            upload_ac_results_strategy: UploadCacheResultsStrategy::Never,
            ..Default::default()
        };
        let manager = RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory: tmp.path().join("root").to_string_lossy().into_owned(),
            execution_configuration: ExecutionConfiguration::default(),
            cas_store: cas_store.clone(),
            ac_store: None,
            ac_mirror_target: None,
            historical_store: Store::new(cas_store),
            upload_action_result_config: &upload_cfg,
            max_action_timeout: Duration::MAX,
            max_upload_timeout: Duration::MAX,
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
            deferred_output_uploads_enabled: false,
        })
        .expect("RunningActionsManagerImpl::new");
        (Arc::new(manager), tmp)
    }

    /// REAL `cleanup_action` must (a) remove the operation from
    /// `running_actions` and (b) wake `action_done_tx` waiters.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_action_removes_entry_and_wakes_waiters() {
        let (manager, _tmp) = make_real_manager().await;
        let operation_id = OperationId::default();

        // Seed the running_actions map. A dangling Weak is sufficient:
        // cleanup_action only `.remove()`s the key; it never upgrades the Weak.
        manager
            .running_actions
            .lock()
            .insert(operation_id.clone(), Weak::new());
        assert!(
            manager.running_actions.lock().contains_key(&operation_id),
            "precondition: seeded entry must be present before cleanup_action",
        );

        // (b) Subscribe a watch receiver BEFORE cleanup so we can observe the
        // wake. wait_for resolves only when the version is bumped by
        // send_modify (its closure is `|()| true` so a single change suffices).
        let mut rx = manager.action_done_tx.subscribe();

        manager
            .cleanup_action(&operation_id)
            .expect("cleanup_action must succeed for a present operation id");

        // (a) entry removed.
        assert!(
            manager.running_actions.lock().remove(&operation_id).is_none(),
            "cleanup_action must REMOVE the operation from running_actions",
        );

        // (b) waiter woken: changed() returns Ok within the deadlock-detector
        // window. The timeout is a DEADLOCK DETECTOR, not synchronization.
        tokio::time::timeout(Duration::from_secs(5), rx.changed())
            .await
            .expect(
                "cleanup_action wake DEADLOCK/LOST: action_done_tx.send_modify must bump the \
                 watch version so kill_all's wait_for observes the completion — the notify \
                 was not delivered",
            )
            .expect("watch sender must still be alive after cleanup_action");
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod cleanup_delete_concurrency_cap_tests {
    //! Isotope Bug 2: concurrent cleanup deletes on a worker must be BOUNDED.
    //!
    //! `do_cleanup` runs `fs::remove_dir_all` on the tokio blocking pool. On a
    //! mass-drop burst the per-action `RunningActionImpl::drop` background spawn
    //! fanned out into **157 blocking-pool threads in uninterruptible D-state**
    //! inside `remove_dir_all_recursive` (isotope wedge dump 2026-06-16, ~15% of
    //! the 1024-thread pool — see
    //! `.claude/audits/isotope-cleanup-fanout-evidence-2026-06-16.md`), starving
    //! the pool shared with hashing/sync-fs on the upload/download data path.
    //!
    //! The fix bounds concurrent deletes with the process-singleton
    //! `CLEANUP_DELETE_SEMAPHORE` (cap `CLEANUP_DELETE_INFLIGHT_CAP`) acquired
    //! INSIDE `bounded_remove_dir_all` before the blocking delete. This test
    //! drives a burst of M >> cap concurrent `bounded_remove_dir_all` calls
    //! through an injected counting delete hook and asserts:
    //!   1. observed max concurrent deletes never exceeds the cap, AND
    //!   2. all M calls eventually complete within a deadlock-detector timeout.
    //!
    //! No sleep-as-synchronization: the driver spins on a real atomic in-flight
    //! gauge until it observes the cap saturate, then releases the parked first
    //! wave via a `Notify` fuse — so the max-concurrency observation is real,
    //! not a timing artifact; `tokio::time::timeout` is only the deadlock
    //! detector, never a synchronizer.
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::time::Duration;
    use std::sync::Arc;

    use tokio::sync::Notify;

    use super::{
        CLEANUP_DELETE_INFLIGHT_CAP, bounded_remove_dir_all,
        install_cleanup_delete_test_hook, take_cleanup_delete_test_hook,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_deletes_never_exceed_cap_and_all_complete() {
        let cap = CLEANUP_DELETE_INFLIGHT_CAP;
        // Launch strictly more tasks than the cap so the bound is load-bearing.
        let total = cap * 3;

        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        // `release` gates the FIRST wave: every hook awaits it. The driver
        // notifies it (permanently, via a fuse flag) only AFTER observing the
        // in-flight gauge saturate at `cap` — proving the bound is actually
        // reached before any hook is allowed to return. Once released, every
        // subsequent wave's hooks see the fuse already tripped and return
        // immediately, so the remaining (total - cap) tasks drain in cap-sized
        // waves as permits free. NOT sleep-as-sync: the driver spin observes a
        // real atomic gauge; `release` is an explicit wakeup.
        let release = Arc::new(Notify::new());
        let released = Arc::new(core::sync::atomic::AtomicBool::new(false));

        let hook = {
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            let release = release.clone();
            let released = released.clone();
            Arc::new(move |_path: std::path::PathBuf| {
                let in_flight = in_flight.clone();
                let max_seen = max_seen.clone();
                let release = release.clone();
                let released = released.clone();
                Box::pin(async move {
                    let now = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
                    max_seen.fetch_max(now, Ordering::AcqRel);
                    // First wave parks here until the driver confirms the cap is
                    // saturated; later waves observe the fuse already tripped and
                    // fall straight through. Subscribe before checking the fuse
                    // to avoid a lost-wakeup window.
                    let notified = release.notified();
                    if !released.load(Ordering::Acquire) {
                        notified.await;
                    }
                    in_flight.fetch_sub(1, Ordering::AcqRel);
                    Ok(())
                }) as core::pin::Pin<Box<dyn core::future::Future<Output = Result<(), nativelink_error::Error>> + Send>>
            })
        };
        install_cleanup_delete_test_hook(hook);

        let mut handles = Vec::with_capacity(total);
        for i in 0..total {
            let completed = completed.clone();
            handles.push(tokio::spawn(async move {
                bounded_remove_dir_all(&format!("/tmp/isotope2-fake-{i}"))
                    .await
                    .expect("bounded_remove_dir_all hook returns Ok");
                completed.fetch_add(1, Ordering::AcqRel);
            }));
        }

        // Wait for the first wave to SATURATE the cap, then release. If the
        // semaphore bounded correctly, in_flight rises to exactly `cap` and
        // stays there (no permit for a (cap+1)th hook). If the bound were
        // broken, in_flight would climb past `cap` and the max assertion below
        // fires. The timeout is a DEADLOCK DETECTOR for "never reached cap".
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if in_flight.load(Ordering::Acquire) >= cap {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(
            "cleanup-delete cap DEADLOCK: the in-flight delete gauge never reached `cap` — \
             the CLEANUP_DELETE_SEMAPHORE permit was not acquired before the delete, so the \
             bounded wave never saturated",
        );
        // Fuse + wake: trip the flag first so any hook that subscribes after
        // this point sees it, then wake everyone already parked.
        released.store(true, Ordering::Release);
        release.notify_waiters();

        // All M tasks must drain.
        for h in handles {
            tokio::time::timeout(Duration::from_secs(10), h)
                .await
                .expect(
                    "cleanup-delete cap DEADLOCK: a bounded_remove_dir_all task did not \
                     complete within the window — bounded deletes must EVENTUALLY all finish \
                     (the semaphore must not leak permits or deadlock the drain)",
                )
                .expect("bounded_remove_dir_all task panicked");
        }

        assert_eq!(
            completed.load(Ordering::Acquire),
            total,
            "all {total} bounded deletes must complete — bound must not drop work",
        );
        let observed_max = max_seen.load(Ordering::Acquire);
        assert!(
            observed_max <= cap,
            "concurrent deletes ({observed_max}) exceeded CLEANUP_DELETE_INFLIGHT_CAP ({cap}) \
             — the CLEANUP_DELETE_SEMAPHORE did not bound blocking-pool delete fan-out; a \
             mass-drop burst can saturate the pool (157-thread D-state regression)",
        );
        assert_eq!(
            observed_max, cap,
            "expected the bounded wave to SATURATE the cap (observed_max {observed_max} != cap \
             {cap}); a max below cap means the test never actually exercised the bound",
        );

        // Clean up the process-global hook so sibling tests are unaffected.
        take_cleanup_delete_test_hook();
    }
}

#[cfg(test)]
mod upload_retry_classification_tests {
    //! FL-681 Fix B: a deferred output-blob upload retries until it
    //! SUCCEEDS — it NEVER gives up, because a give-up is permanent data
    //! loss (the blob is single-copy on the worker until the server has
    //! it durably). The classifier [`classify_upload_error`] decides, per
    //! error, whether the attempt can EVER succeed on a later try
    //! (`Retry`, forever) or is genuinely, permanently impossible
    //! (`PermanentGiveUp`, documented exemptions only).
    //!
    //! These tests pin the classification table so a future edit cannot
    //! silently re-add a transient class to the give-up set (which would
    //! re-open the leak). They also model the loop's retry-past-old-limit
    //! behavior: with the OLD `MAX_RETRIES = 4` finite limit, attempt 5
    //! gave up; the classifier is now attempt-independent for retryable
    //! classes.

    use core::time::Duration;

    use nativelink_error::{Code, Error, make_err};
    use nativelink_macro::nativelink_test;

    use super::{
        READ_LOCAL_BACKOFF_MAX, READ_LOCAL_BACKOFF_MIN, RepinMode, UploadFailureSide,
        UploadRetryDecision, classify_upload_error, next_retry_backoff, plan_retry_step,
    };

    /// Mirrors the loop's remote ramp start so the assertions read against the
    /// real production constant, not a magic number.
    const REMOTE_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
    /// The loop's synchronous-mode finite give-up bound.
    const SYNC_MAX_RETRIES: u32 = 4;

    fn err(code: Code) -> Error {
        make_err!(code, "synthetic upload error for classification test")
    }

    /// Transient / retryable classes MUST retry forever — these are the
    /// classes that CAN succeed on a later attempt (server backpressure,
    /// network blips, server restart windows).
    #[test]
    fn transient_classes_retry_forever() {
        for code in [
            Code::Aborted,
            Code::ResourceExhausted,
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::Internal,
            Code::Unknown,
            Code::Cancelled,
            Code::NotFound,
            Code::DataLoss,
            Code::FailedPrecondition,
        ] {
            assert_eq!(
                classify_upload_error(&err(code)),
                UploadRetryDecision::Retry,
                "code {code:?} CAN succeed on a later attempt and MUST retry forever \
                 (a give-up here is permanent data loss — FL-681 Fix B). If this fails, \
                 a transient class was moved to the permanent-give-up set, re-opening the leak."
            );
        }
    }

    /// AlreadyExists is success: the blob is durably on the server.
    #[test]
    fn already_exists_is_success_not_retry() {
        assert_eq!(
            classify_upload_error(&err(Code::AlreadyExists)),
            UploadRetryDecision::AlreadyDurable,
            "AlreadyExists means the slow tier already has the blob — treat as success, \
             not as a retryable failure"
        );
    }

    /// The ONLY genuinely-permanent classes (a malformed/forbidden request
    /// that cannot ever succeed by retrying the SAME bytes). Documented
    /// exemptions — everything else defaults to retry.
    #[test]
    fn permanent_request_errors_give_up() {
        for code in [
            Code::InvalidArgument,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::Unimplemented,
        ] {
            assert_eq!(
                classify_upload_error(&err(code)),
                UploadRetryDecision::PermanentGiveUp,
                "code {code:?} is a malformed/forbidden request that cannot succeed by \
                 retrying the same bytes — documented permanent exemption"
            );
        }
    }

    /// Models the OLD finite-limit bug: under `MAX_RETRIES = 4` a transient
    /// failure on attempts 1..=4 retried but attempt 5 gave up
    /// ("all retries exhausted"). The classifier is now attempt-independent
    /// for retryable classes — there is no attempt at which a transient
    /// failure flips to give-up. This is the core Fix B regression: a mock
    /// that fails the first (old-limit + K) attempts then succeeds MUST
    /// eventually succeed.
    #[test]
    fn upload_retries_past_old_limit() {
        const OLD_MAX_RETRIES: u32 = 4;
        const K: u32 = 6; // fail well past the old limit, then succeed.
        let transient = err(Code::Unavailable);

        // Every attempt up to and beyond the old limit must say "Retry"
        // — never "PermanentGiveUp" — proving the loop would keep going.
        for attempt in 0..=(OLD_MAX_RETRIES + K) {
            assert_eq!(
                classify_upload_error(&transient),
                UploadRetryDecision::Retry,
                "upload gave up; blob lost: a transient Unavailable on attempt {attempt} \
                 must STILL retry (old MAX_RETRIES={OLD_MAX_RETRIES} finite limit removed — FL-681 Fix B)"
            );
        }
    }

    // -----------------------------------------------------------------
    // #FL-688 W6: sync-mode give-up MUST arm the failed_slow_writes
    // backstop for retryable (transient) errors, and MUST NOT for
    // permanent errors. This models the give-up arm in
    // `spawn_upload_to_remote` exactly: `!flag && classify_upload_error(&e)
    // == Retry` → `requeue_failed_push`. Pre-#FL-688 the give-up recorded
    // NOTHING (the loop wrote to the bare slow_store), so the
    // doc-comment's "deferring to the failed_slow_writes backstop" was a
    // load-bearing falsehood for this loop.
    // -----------------------------------------------------------------

    /// Compose a real `FastSlowStore` (MemoryStore fast, MemoryStore slow)
    /// so the W6 give-up arm's `requeue_failed_push` side effect is
    /// observable via `failed_slow_writes_contains`. The slow tier is
    /// never written here — the test drives the GIVE-UP arm decision +
    /// re-queue, not the upload itself.
    fn w6_test_fss() -> std::sync::Arc<nativelink_store::fast_slow_store::FastSlowStore> {
        use nativelink_config::stores::{
            FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
        };
        use nativelink_store::fast_slow_store::FastSlowStore;
        use nativelink_store::memory_store::MemoryStore;
        use nativelink_util::store_trait::Store;
        let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
        let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
        FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Memory(MemorySpec::default()),
                slow: StoreSpec::Memory(MemorySpec::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
                slow_writes_in_flight_max_bytes: 0,
            },
            fast,
            slow,
        )
    }

    fn w6_digest(seed: u8) -> nativelink_util::common::DigestInfo {
        let mut h = [0u8; 32];
        h[0] = seed;
        nativelink_util::common::DigestInfo::new(h, 7)
    }

    /// Drives the EXACT production give-up arm side effect from
    /// `spawn_upload_to_remote` (W6): gate on the production
    /// `should_requeue_on_giveup` predicate, then perform the
    /// `requeue_failed_push`. Both the predicate and the re-queue are
    /// production code — a mutation that guts either red-fails this test.
    /// Returns whether the re-queue fired.
    fn w6_giveup_requeue(
        fss: &std::sync::Arc<nativelink_store::fast_slow_store::FastSlowStore>,
        e: &Error,
        flag: bool,
        digest: nativelink_util::common::DigestInfo,
    ) -> bool {
        if super::should_requeue_on_giveup(e, flag) {
            fss.requeue_failed_push(digest)
        } else {
            false
        }
    }

    /// W6: a SYNC-mode give-up on a TRANSIENT error (the budget was
    /// exhausted, but the error CAN succeed later) MUST arm the
    /// `failed_slow_writes` backstop — never silently drop the output blob.
    ///
    /// `#[nativelink_test]` because `FastSlowStore::new` spawns the BIS /
    /// watchdog background tasks and therefore needs a Tokio runtime.
    #[nativelink_test]
    async fn w6_sync_giveup_on_transient_arms_backstop() {
        let fss = w6_test_fss();
        let digest = w6_digest(0x61);
        let transient = err(Code::Unavailable);
        // The loop reaches the give-up arm with `flag == false` after
        // `SYNC_MAX_RETRIES` attempts (see plan_retry_step `:1295`).
        let requeued = w6_giveup_requeue(&fss, &transient, false, digest);
        assert!(
            requeued,
            "W6: sync-mode retry budget exhausted on a transient \
             ({:?}) MUST re-queue the output digest into failed_slow_writes \
             for retry-until-durable — the loop's doc claimed this backstop \
             but pre-#FL-688 never armed it (#FL-688 W6 false-backstop)",
            Code::Unavailable,
        );
        assert!(
            fss.failed_slow_writes_contains(&digest),
            "W6: after sync-mode give-up the digest MUST be present in \
             failed_slow_writes so the reconnect drainer re-attempts it",
        );
        // Reference SYNC_MAX_RETRIES so the test reads against the real
        // give-up bound (the give-up `flag == false` is produced by the
        // loop only AFTER this many attempts).
        assert_eq!(
            SYNC_MAX_RETRIES, 4,
            "give-up bound changed — re-confirm the W6 give-up arm still \
             fires at the documented sync budget",
        );
    }

    /// W6 (negative / asymmetric coverage): a PERMANENT request error
    /// (`InvalidArgument`) gives up too, but re-queuing it would be a
    /// dead-weight retry that can NEVER succeed. The give-up arm MUST NOT
    /// arm the backstop for permanent classes.
    #[nativelink_test]
    async fn w6_sync_giveup_on_permanent_does_not_arm_backstop() {
        let fss = w6_test_fss();
        let digest = w6_digest(0x62);
        let permanent = err(Code::InvalidArgument);
        let requeued = w6_giveup_requeue(&fss, &permanent, false, digest);
        assert!(
            !requeued,
            "W6: a permanent request error ({:?}) can never succeed by \
             retry — the give-up arm MUST NOT re-queue it (dead-weight \
             reconnect retry)",
            Code::InvalidArgument,
        );
        assert!(
            !fss.failed_slow_writes_contains(&digest),
            "W6: a permanently-failed digest MUST NOT be in failed_slow_writes",
        );
    }

    // -----------------------------------------------------------------
    // FL-681 Q2: re-pin before re-read on a worker-local eviction race.
    // -----------------------------------------------------------------

    /// Q2 (root cause): the retry loop re-reads the output from the worker's
    /// OWN fast store; on a read-side eviction race (`ReadLocal`) it MUST
    /// re-assert the source pin BEFORE the next read, using the SAME
    /// indefinite-until-BIS path FL-681 Fix A added — otherwise the retry
    /// re-runs the same eviction race the pin should have prevented. A
    /// remote-write failure did NOT lose the source, so it must NOT re-pin.
    #[test]
    fn upload_repins_before_reread_on_eviction_race() {
        // Deferred mode (production F2): a retryable read-side eviction race
        // must re-pin INDEFINITELY (the BIS-released path), never via a fresh
        // 120s TTL pin.
        let read_race = err(Code::Internal); // "writer dropped without commit"
        let step = plan_retry_step(
            classify_upload_error(&read_race),
            UploadFailureSide::ReadLocal,
            /* deferred_pin */ true,
            /* attempt */ 0,
            SYNC_MAX_RETRIES,
        );
        assert!(
            !step.give_up,
            "retry re-read raced the same eviction the pin should have prevented: a read-side \
             eviction race is retryable (FL-681 Fix B) — the loop must NOT give up"
        );
        assert_eq!(
            step.repin,
            RepinMode::Indefinite,
            "retry re-read raced the same eviction the pin should have prevented: a worker-local \
             read-side race must RE-PIN via the indefinite-until-BIS path (FL-681 Fix A) before \
             the next read, so the re-read does not hit the same evicted source"
        );

        // NotFound is the other read-side eviction-race code (small-blob and
        // streaming re-read) — same contract.
        let not_found_step = plan_retry_step(
            classify_upload_error(&err(Code::NotFound)),
            UploadFailureSide::ReadLocal,
            true,
            0,
            SYNC_MAX_RETRIES,
        );
        assert_eq!(
            not_found_step.repin,
            RepinMode::Indefinite,
            "retry re-read raced the same eviction the pin should have prevented: a NotFound \
             read-side race must RE-PIN indefinitely before the next read"
        );

        // Remote-write failure: the source was never lost, so the loop must
        // NOT re-pin (re-pinning a still-pinned digest is harmless but the
        // contract is that only a lost source triggers a re-pin).
        let remote_step = plan_retry_step(
            classify_upload_error(&err(Code::Unavailable)),
            UploadFailureSide::RemoteWrite,
            true,
            0,
            SYNC_MAX_RETRIES,
        );
        assert_eq!(
            remote_step.repin,
            RepinMode::None,
            "a remote-write failure did not lose the worker-local source — it must NOT re-pin"
        );

        // Synchronous mode re-pins via the TIME-BOUNDED path (matching the
        // schedule-time pin), not the indefinite path.
        let sync_step = plan_retry_step(
            classify_upload_error(&read_race),
            UploadFailureSide::ReadLocal,
            /* deferred_pin */ false,
            0,
            SYNC_MAX_RETRIES,
        );
        assert_eq!(
            sync_step.repin,
            RepinMode::TimeBounded,
            "synchronous-mode read race must re-pin via the time-bounded path (matching the \
             schedule-time pin whose failed_slow_writes backstop is live), not the indefinite path"
        );
    }

    // -----------------------------------------------------------------
    // FL-681 Q1: right-size the backoff by error class.
    // -----------------------------------------------------------------

    /// Q1 (tail): the worker-local read-side class waits a SHORT floor
    /// (~50-100 ms), not the 1 s remote ramp — a same-host disk re-read does
    /// not need second-scale backoff. The prod tail was ~15 s of pure 1 s-ramp
    /// backoff (INITIAL_BACKOFF=1s ×2 ×4) per evicted output. A genuine remote
    /// `Unavailable` still uses the ≥1 s ramp.
    #[test]
    fn read_side_failure_uses_short_backoff() {
        // The read-side class waits within [50ms, 100ms] for EVERY jitter
        // byte — never the 1 s remote ramp. (~15 s tail eliminated.)
        for jitter in [0u8, 1, 64, 127, 200, 255] {
            let wait = next_retry_backoff(
                UploadFailureSide::ReadLocal,
                /* current remote ramp */ REMOTE_INITIAL_BACKOFF,
                jitter,
            );
            assert!(
                wait >= READ_LOCAL_BACKOFF_MIN && wait <= READ_LOCAL_BACKOFF_MAX,
                "~15 s tail: a worker-local read-side re-read race waited {wait:?} (jitter \
                 {jitter}) — it MUST wait a short floor in \
                 [{READ_LOCAL_BACKOFF_MIN:?}, {READ_LOCAL_BACKOFF_MAX:?}], NOT the 1 s remote \
                 ramp; routing the read race through the 1 s floor re-creates the ~15 s tail"
            );
            assert!(
                wait < REMOTE_INITIAL_BACKOFF,
                "~15 s tail: a worker-local read race waited {wait:?} ≥ the 1 s remote ramp \
                 (jitter {jitter}); the read-side class must be sub-second"
            );
        }

        // The jitter spreads re-reads across the [min,max] span: the two
        // endpoints differ, proving the floor is jittered (a burst of distinct
        // evicted outputs does not re-read in lockstep).
        let low = next_retry_backoff(UploadFailureSide::ReadLocal, REMOTE_INITIAL_BACKOFF, 0);
        let high = next_retry_backoff(UploadFailureSide::ReadLocal, REMOTE_INITIAL_BACKOFF, 255);
        assert_eq!(
            low, READ_LOCAL_BACKOFF_MIN,
            "jitter 0 must map to the floor minimum {READ_LOCAL_BACKOFF_MIN:?}"
        );
        assert_eq!(
            high, READ_LOCAL_BACKOFF_MAX,
            "jitter 255 must map to the floor maximum {READ_LOCAL_BACKOFF_MAX:?}"
        );
        assert!(
            high > low,
            "the read-side backoff must be jittered across [{READ_LOCAL_BACKOFF_MIN:?}, \
             {READ_LOCAL_BACKOFF_MAX:?}] so a burst of evicted outputs spreads its re-reads"
        );

        // The remote class is untouched: it returns the caller's current ramp
        // (≥ 1 s) verbatim, so a genuine remote transient still backs off at
        // the slow remote interval and ramps toward the cap.
        for ramp in [
            REMOTE_INITIAL_BACKOFF,
            REMOTE_INITIAL_BACKOFF * 2,
            Duration::from_secs(30),
        ] {
            let wait = next_retry_backoff(UploadFailureSide::RemoteWrite, ramp, 200);
            assert_eq!(
                wait, ramp,
                "a genuine remote transient must keep the existing 1 s→cap ramp ({ramp:?}); \
                 the read-side short floor must NOT leak into the remote class"
            );
            assert!(
                wait >= REMOTE_INITIAL_BACKOFF,
                "remote transient backoff {wait:?} dropped below the 1 s remote floor — the \
                 remote ramp must stay second-scale"
            );
        }
    }
}

#[cfg(test)]
mod deferred_pin_indefinite_only_tests {
    //! (FL-688 v3 Stage B) The four `deferred_output_uploads_enabled` durability
    //! pin sites switch from `pin_digest_indefinite_or_time_bounded` (TTL
    //! fallback on cap-refusal) to `pin_digest_indefinite_with_result`
    //! (indefinite-ONLY) via the single-source-of-truth helper
    //! [`super::pin_deferred_output_digest`]. This module drives that EXACT
    //! production helper against a real `FilesystemStore` + the real
    //! `PIN_TIMEOUT_SECS` sweep, so a mutation of the helper body red-fails.
    //!
    //! Asymmetric coverage: the deferred admit direction (indefinite, survives
    //! the sweep) AND the deferred cap-refusal direction (fully evictable, NO
    //! time-bounded fallback) AND the synchronous direction (time-bounded
    //! `pin_digest_with_result`, untouched by Stage B).

    use std::sync::Arc;

    use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
    use nativelink_macro::nativelink_test;
    use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::StoreLike;

    use super::pin_deferred_output_digest;

    async fn make_store(indefinite_cap_bytes: u64) -> (Arc<FilesystemStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let content_path = tmp.path().join("content");
        let temp_path = tmp.path().join("temp");
        std::fs::create_dir_all(&content_path).expect("mk content_path");
        std::fs::create_dir_all(&temp_path).expect("mk temp_path");
        let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path: content_path.to_string_lossy().into_owned(),
            temp_path: temp_path.to_string_lossy().into_owned(),
            eviction_policy: Some(EvictionPolicy {
                max_bytes: 1024 * 1024,
                ..Default::default()
            }),
            pending_bis_pin_max_bytes: indefinite_cap_bytes,
            ..Default::default()
        })
        .await
        .expect("FilesystemStore::new");
        (store, tmp)
    }

    /// DEFERRED ADMIT: `pin_deferred_output_digest(store, deferred=true, d)`
    /// takes an INDEFINITE pin that SURVIVES the `PIN_TIMEOUT_SECS` sweep
    /// (held-until-BIS). The OLD `_or_time_bounded` helper, with the indefinite
    /// cap below the blob size, would have fallen to a TIME-BOUNDED pin and been
    /// demoted by this sweep.
    ///
    /// MUTATION: revert `pin_deferred_output_digest`'s deferred arm to
    /// `pin_digest_indefinite_or_time_bounded` AND set the cap below the blob
    /// (e.g. `make_store(1)`) → the fallback takes a time-bounded pin →
    /// rewind+sweep demotes it → this test red-fails with the bespoke message.
    #[nativelink_test]
    async fn deferred_pin_is_indefinite_and_survives_ttl_sweep() {
        let (fs_store, _tmp) = make_store(1024 * 1024).await;
        let output = DigestInfo::new([7u8; 32], 6);
        fs_store
            .as_pin()
            .update_oneshot(output, "hello!".into())
            .await
            .expect("write F2 output blob");

        assert!(
            pin_deferred_output_digest(fs_store.as_ref(), /* deferred */ true, &output),
            "deferred-mode durability pin should admit under an unsaturated indefinite cap"
        );

        // Confirm the indefinite pin is held (non-zero indefinite accounting).
        assert!(
            fs_store.indefinite_pinned_bytes() > 0,
            "the deferred durability pin must be an INDEFINITE pin (counts against indefinite \
             accounting)"
        );
        // Rewind the deadline past PIN_TIMEOUT_SECS and run the real sweep with
        // NO BIS-ack.
        assert!(
            fs_store.test_force_pin_expired(&output),
            "the durability pin must be present before the sweep"
        );
        fs_store.test_expire_stale_pins().await;

        // SURVIVAL = held-until-BIS. The sweep `continue`s indefinite pins, so
        // the entry is STILL in the pinned map afterwards
        // (`test_force_pin_expired` finds it again). A time-bounded fallback pin
        // would have been REMOVED (demoted to LRU) by the sweep.
        assert!(
            fs_store.test_force_pin_expired(&output),
            "deferred-mode durability pin was DEMOTED by the PIN_TIMEOUT_SECS sweep: \
             pin_deferred_output_digest's deferred arm must be indefinite-ONLY \
             (pin_digest_indefinite_with_result, held-until-BIS), exempt from the TTL sweep — a \
             time-bounded fallback is the deferred-mode loss window (F2 bypasses \
             in_flight_slow_writes/failed_slow_writes)"
        );
        assert!(
            fs_store.indefinite_pinned_bytes() > 0,
            "deferred-mode durability pin lost its indefinite accounting across the sweep — it \
             must remain indefinitely pinned until the BIS-ack"
        );

        fs_store.unpin_digest(&output);
    }

    /// DEFERRED CAP-REFUSAL (asymmetric): with the indefinite cap exhausted,
    /// `pin_deferred_output_digest` returns `false` and takes NO fallback pin —
    /// the blob is FULLY evictable (the accepted saturated-cap loss class). The
    /// OLD `_or_time_bounded` helper would have taken a TIME-BOUNDED pin here
    /// (non-zero pinned bytes).
    ///
    /// MUTATION: revert the deferred arm to `pin_digest_indefinite_or_time_bounded`
    /// → on cap-refusal it takes a time-bounded pin → `pinned_bytes() != 0` and
    /// `test_force_pin_expired` returns true → this test red-fails.
    #[nativelink_test]
    async fn deferred_cap_refusal_is_fully_evictable_not_time_bounded() {
        // Indefinite cap = 1 byte: the 6-byte blob's indefinite pin is REFUSED.
        let (fs_store, _tmp) = make_store(1).await;
        let output = DigestInfo::new([8u8; 32], 6);
        fs_store
            .as_pin()
            .update_oneshot(output, "hello!".into())
            .await
            .expect("write F2 output blob");

        assert!(
            !pin_deferred_output_digest(fs_store.as_ref(), /* deferred */ true, &output),
            "deferred indefinite-only pin must be REFUSED when the indefinite cap (1 byte) cannot \
             fit the 6-byte blob"
        );
        assert_eq!(
            fs_store.indefinite_pinned_bytes(),
            0,
            "a cap-refused indefinite pin must hold ZERO indefinite bytes"
        );
        assert!(
            !fs_store.test_force_pin_expired(&output),
            "cap-refusal must leave the F2 blob FULLY evictable (NO time-bounded fallback pin): \
             pin_deferred_output_digest accepts the saturated-cap loss class (producer backpressure \
             is the worker indefinite_pin_saturated NAK); it must NOT silently take a time-bounded \
             pin the way the removed _or_time_bounded helper did"
        );
    }

    /// SYNCHRONOUS mode is UNTOUCHED by Stage B: `deferred=false` takes a
    /// time-bounded `pin_digest_with_result` whose TTL→failed_slow_writes
    /// backstop is live, so it is DEMOTED by the sweep (the pre-existing
    /// synchronous behavior). Guards against accidentally making the
    /// synchronous arm indefinite.
    #[nativelink_test]
    async fn synchronous_pin_stays_time_bounded() {
        let (fs_store, _tmp) = make_store(1024 * 1024).await;
        let output = DigestInfo::new([9u8; 32], 6);
        fs_store
            .as_pin()
            .update_oneshot(output, "hello!".into())
            .await
            .expect("write output blob");

        assert!(
            pin_deferred_output_digest(fs_store.as_ref(), /* deferred */ false, &output),
            "synchronous-mode pin should succeed for a present blob"
        );
        // Time-bounded: zero indefinite bytes (it is NOT an indefinite pin).
        assert_eq!(
            fs_store.indefinite_pinned_bytes(),
            0,
            "synchronous-mode pin must NOT count against the indefinite-pin accounting"
        );
        assert!(
            fs_store.test_force_pin_expired(&output),
            "synchronous-mode time-bounded pin must be present (and time-bounded) before the sweep"
        );
        fs_store.test_expire_stale_pins().await;
        // A time-bounded pin is DEMOTED (removed from the pinned map) by the
        // sweep — so it is no longer pinned afterwards. An indefinite pin would
        // have survived. This guards against accidentally making the synchronous
        // arm indefinite.
        assert!(
            !fs_store.test_force_pin_expired(&output),
            "synchronous-mode pin must remain TIME-BOUNDED (demoted by the PIN_TIMEOUT_SECS sweep) \
             — Stage B must not make the synchronous arm indefinite"
        );
    }
}

#[cfg(test)]
mod deferred_upload_retry_loop_tests {
    //! FL-681 Q1+Q2: drive the REAL retry controller
    //! ([`DeferredUploadRetry::after_failure`]) — the exact code the per-digest
    //! upload loop runs after a failed attempt — and assert the two
    //! load-bearing contracts the inline loop now delegates to it:
    //!
    //!   Q2 (re-pin): on a worker-local read-side eviction race the controller
    //!   re-asserts the source pin BEFORE it sleeps (and therefore before the
    //!   loop's next read), via the SAME indefinite-until-BIS path FL-681 Fix A
    //!   added in deferred mode. The re-pin is observed through a recording
    //!   closure (the production closure calls
    //!   `FilesystemStore::pin_digest_indefinite_with_result`).
    //!
    //!   Q1 (backoff): the read-side class waits a short floor (~50-100 ms),
    //!   measured under `tokio::time::pause` so the assertion is on the actual
    //!   virtual-time sleep the loop performed — while a genuine remote
    //!   `Unavailable` still waits the ≥1 s ramp. No sleep-as-synchronization:
    //!   paused time only advances when the awaited `tokio::time::sleep`
    //!   yields, so the measured elapsed IS the backoff.

    use core::cell::RefCell;
    use core::time::Duration;

    use nativelink_error::{Code, Error, make_err};
    use nativelink_macro::nativelink_test;
    use nativelink_util::common::DigestInfo;

    use super::{DeferredUploadRetry, RepinMode, UploadFailureSide};

    const VALID_HASH: &str =
        "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    /// The loop's remote ramp start (`INITIAL_BACKOFF`) and cap (`MAX_BACKOFF`)
    /// and synchronous-mode finite bound — mirrored so assertions read against
    /// the production constants.
    const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    const SYNC_MAX_RETRIES: u32 = 4;

    fn digest() -> DigestInfo {
        DigestInfo::try_new(VALID_HASH, 13 * 1024 * 1024).unwrap()
    }

    fn err(code: Code, msg: &str) -> Error {
        make_err!(code, "{}", msg)
    }

    /// Q2: a read-side eviction race (`ReadLocal`) on attempt 0 must RE-PIN —
    /// via the indefinite-until-BIS path in deferred (production) mode —
    /// before the controller returns to the loop for the next read. The recorded
    /// re-pin mode proves the loop would re-read a re-pinned (un-evicted) source.
    ///
    /// MUTATION (remove the re-pin): in `after_failure`, delete the
    /// `repin_fn(step.repin);` call (or change the `ReadLocal` arm of
    /// `plan_retry_step` to `RepinMode::None`). `repins` stays empty and this
    /// test fails with the bespoke message below.
    #[nativelink_test(flavor = "current_thread", start_paused = true)]
    async fn upload_repins_before_reread_on_eviction_race() {
        let repins: RefCell<Vec<RepinMode>> = RefCell::new(Vec::new());
        let mut retry = DeferredUploadRetry::new(INITIAL_BACKOFF);

        // Attempt 0: the worker's own fast-store re-read lost the eviction race
        // (the streaming WriteHalfGuard synthesizes this Internal error).
        let read_race = err(Code::Internal, "buf_channel: writer dropped without commit");
        let outcome = retry
            .after_failure(
                &read_race,
                UploadFailureSide::ReadLocal,
                digest(),
                /* deferred_pin */ true,
                SYNC_MAX_RETRIES,
                MAX_BACKOFF,
                |mode| repins.borrow_mut().push(mode),
            )
            .await;

        assert_eq!(
            outcome, None,
            "retry re-read raced the same eviction the pin should have prevented: a read-side \
             eviction race is retryable (FL-681 Fix B) — the controller must CONTINUE the loop, \
             not give up"
        );
        assert_eq!(
            repins.borrow().as_slice(),
            &[RepinMode::Indefinite],
            "retry re-read raced the same eviction the pin should have prevented: before the next \
             read the controller MUST re-assert the source pin exactly once via the \
             indefinite-until-BIS path (FL-681 Fix A), so the re-read does not hit the same \
             evicted source"
        );
    }

    /// Q1: the read-side class waits a SHORT floor (~50-100 ms), NOT the 1 s
    /// remote ramp; a genuine remote `Unavailable` still waits ≥1 s. Measured
    /// under paused time so the elapsed IS the actual backoff the loop slept.
    ///
    /// MUTATION (route read-side through the 1 s floor): change the `ReadLocal`
    /// arm of `next_retry_backoff` to return `remote_backoff` (the 1 s ramp).
    /// The read-side elapsed jumps to ≥1 s and this test fails with the bespoke
    /// ~15 s-tail message.
    #[nativelink_test(flavor = "current_thread", start_paused = true)]
    async fn read_side_failure_uses_short_backoff() {
        // --- read-side class: short floor ---
        let mut read_retry = DeferredUploadRetry::new(INITIAL_BACKOFF);
        let read_race = err(Code::Internal, "buf_channel: writer dropped without commit");
        let start = tokio::time::Instant::now();
        let outcome = read_retry
            .after_failure(
                &read_race,
                UploadFailureSide::ReadLocal,
                digest(),
                true,
                SYNC_MAX_RETRIES,
                MAX_BACKOFF,
                |_mode| {},
            )
            .await;
        let read_elapsed = start.elapsed();
        assert_eq!(outcome, None, "a read-side race must continue retrying, not give up");
        assert!(
            read_elapsed < INITIAL_BACKOFF,
            "~15 s tail: a worker-local read-side re-read race slept {read_elapsed:?} \
             (≥ the 1 s remote ramp) — it MUST wait a short sub-second floor (~50-100 ms); \
             routing the read race through the 1 s floor re-creates the ~15 s tail \
             (INITIAL_BACKOFF=1s ×2 ×4)"
        );
        assert!(
            read_elapsed >= Duration::from_millis(50)
                && read_elapsed <= Duration::from_millis(100),
            "~15 s tail: the read-side floor slept {read_elapsed:?}, outside the expected \
             [50ms, 100ms] window"
        );

        // --- remote class: ≥1 s ramp, untouched ---
        let mut remote_retry = DeferredUploadRetry::new(INITIAL_BACKOFF);
        let unavailable = err(Code::Unavailable, "slow store unavailable");
        let start = tokio::time::Instant::now();
        let outcome = remote_retry
            .after_failure(
                &unavailable,
                UploadFailureSide::RemoteWrite,
                digest(),
                true,
                SYNC_MAX_RETRIES,
                MAX_BACKOFF,
                |_mode| {},
            )
            .await;
        let remote_elapsed = start.elapsed();
        assert_eq!(outcome, None, "a remote transient must continue retrying forever (Fix B)");
        assert!(
            remote_elapsed >= INITIAL_BACKOFF,
            "a genuine remote Unavailable slept {remote_elapsed:?} (< the 1 s remote ramp) — the \
             remote class must keep the second-scale 1 s→cap ramp; the read-side short floor must \
             NOT leak into the remote class"
        );
    }

    /// The remote ramp doubles ONLY on remote failures and is NOT reset/inflated
    /// by an interleaved read race — proving the two classes share no ramp state
    /// in a way that would punish a later genuine remote transient.
    #[nativelink_test(flavor = "current_thread", start_paused = true)]
    async fn remote_ramp_unaffected_by_interleaved_read_race() {
        let mut retry = DeferredUploadRetry::new(INITIAL_BACKOFF);
        let unavailable = err(Code::Unavailable, "slow store unavailable");
        let read_race = err(Code::NotFound, "evicted from fast store");

        // First remote failure: sleeps 1 s, then ramps to 2 s.
        let start = tokio::time::Instant::now();
        retry
            .after_failure(&unavailable, UploadFailureSide::RemoteWrite, digest(), true,
                SYNC_MAX_RETRIES, MAX_BACKOFF, |_| {})
            .await;
        assert_eq!(start.elapsed(), INITIAL_BACKOFF, "first remote failure sleeps the 1 s floor");

        // Interleaved read race: short floor, must NOT touch the remote ramp.
        retry
            .after_failure(&read_race, UploadFailureSide::ReadLocal, digest(), true,
                SYNC_MAX_RETRIES, MAX_BACKOFF, |_| {})
            .await;

        // Next remote failure: ramp must be 2 s (doubled once), NOT reset to 1 s
        // and NOT inflated to 4 s by the read race.
        let start = tokio::time::Instant::now();
        retry
            .after_failure(&unavailable, UploadFailureSide::RemoteWrite, digest(), true,
                SYNC_MAX_RETRIES, MAX_BACKOFF, |_| {})
            .await;
        assert_eq!(
            start.elapsed(),
            INITIAL_BACKOFF * 2,
            "the remote ramp must advance ONLY on remote failures: after one remote failure the \
             next remote wait is 2 s — an interleaved read race must neither reset it to 1 s nor \
             inflate it"
        );
    }
}

#[cfg(test)]
mod calib_probe_tests {
    //! Unit tests for the calibration-probe decision logic (P-A action-shape,
    //! P-B input-staging). Tests the PURE functions — sampling, classification,
    //! tree-byte/file sums, record building — since the `info!` emit is a thin
    //! wrapper over a populated record. Spec:
    //! `.claude/audits/scheduler-calibration-instrumentation-spec-v2-2026-06-30.md`
    //! §3/§4/§6/§9.
    use core::sync::atomic::{AtomicBool, Ordering};
    use std::collections::HashMap;

    use nativelink_macro::nativelink_test;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Directory as ProtoDirectory, FileNode,
    };
    use nativelink_util::common::DigestInfo;

    use super::{
        CALIB_LARGE_EXEC_MS_THRESHOLD, CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD,
        CALIB_LARGE_TREE_BYTES_THRESHOLD, CALIB_SAMPLE_PERIOD, CALIB_SUBTREE_MAX_DEPTH,
        CALIB_SUBTREE_MAX_PIDS, CalibActionShape, CalibActionRecord, CalibStagingRecord,
        calib_action_sampled, calib_build_action_record, calib_classify, calib_digest_sample_key,
        calib_poll_cpu_time_loop, calib_staging_sampled, calib_tree_totals, calib_uniformly_sampled,
    };

    /// Build a digest whose first-8-byte LE sampling key is exactly `key`.
    fn digest_with_key(key: u64) -> DigestInfo {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&key.to_le_bytes());
        DigestInfo::new(hash, 1)
    }

    // --- Numeric-constant pins (declaration-site authoritative) ------------

    #[test]
    fn constants_match_spec() {
        assert_eq!(
            CALIB_SAMPLE_PERIOD, 16,
            "spec §3/§9 B4 fixes the uniform calibration sample period at 1/16; \
             offline analysis scales counts by this exact value"
        );
        assert_eq!(
            CALIB_LARGE_EXEC_MS_THRESHOLD, 60_000,
            "spec §3 P-A 1/1 override fires at exec_duration > 60 s (= 60_000 ms)"
        );
        assert_eq!(
            CALIB_LARGE_TREE_BYTES_THRESHOLD,
            100 * 1024 * 1024,
            "spec §9 B4 P-B 1/1 override fires at input_tree_bytes > 100 MiB"
        );
        assert_eq!(
            CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD,
            500 * 1024 * 1024,
            "GAP-2 fix: the fetched-payload 1/1 override fires at payload > 500 MiB \
             (M1 soak large-input threshold); LTO archives sit above it and the \
             large-payload/small-tree/<60 s shape must never be dropped from R1"
        );
        // Subtree-walk defensive caps: pin both so a silent bump (which would
        // change the fan-out bound the perf argument rests on) is a deliberate,
        // reviewed test change, not an invisible edit (testing-czar G1).
        assert_eq!(
            CALIB_SUBTREE_MAX_DEPTH, 8,
            "descendant-walk depth cap = 8 (shallow toolchain trees are ~3 levels; \
             8 is headroom); a change here alters the pathological fan-out bound"
        );
        assert_eq!(
            CALIB_SUBTREE_MAX_PIDS, 512,
            "descendant-walk per-tick pid cap = 512 (bounds the proc_listchildpids \
             buffer + per-tick syscall count); a change here alters the worst-case \
             blocking-pool occupancy"
        );
    }

    // --- P-A mach-timebase tick→ns conversion (drop-don't-fabricate) -------

    #[test]
    fn ticks_to_ns_refuses_on_absent_or_bad_timebase() {
        // An unavailable timebase (mach_timebase_info failed) or a zero denom
        // MUST yield None — the probe drops the sample rather than emitting a
        // fabricated scale. A 1:1 fallback would silently under-report ~40× on
        // the M-series fleet and misclassify every action io_bound with no None
        // marker to drop it offline (auditor + red-team).
        assert_eq!(
            super::calib_ticks_to_ns(74_134_725, None),
            None,
            "absent timebase must refuse (None), never fabricate a 1:1 ns scale \
             that under-reports ~40× on Apple Silicon"
        );
        assert_eq!(
            super::calib_ticks_to_ns(74_134_725, Some((125, 0))),
            None,
            "denom == 0 must refuse (None) — no divide-by-zero, no fabricated scale"
        );
    }

    #[test]
    fn ticks_to_ns_converts_m4_empirical_ratio() {
        // The empirical M4 anchor: a 3.002 s single-threaded spin on worker-01
        // reported 74_134_725 mach-timebase ticks; ×125/3 = 3_088_946_875 ns
        // (3.089 s ✓). Raw ticks read as ns would be 0.074 s — the ~40× bug.
        assert_eq!(
            super::calib_ticks_to_ns(74_134_725, Some((125, 3))),
            Some(3_088_946_875),
            "74_134_725 ticks × 125/3 = 3_088_946_875 ns (3.089 s); this is the \
             empirical M4 conversion the units fix restores"
        );
        // A 1:1 timebase (x86 / Rosetta) passes ticks through unchanged.
        assert_eq!(
            super::calib_ticks_to_ns(1_000, Some((1, 1))),
            Some(1_000),
            "a 1:1 timebase (x86/Rosetta) leaves ticks == ns"
        );
    }

    // --- P-A sampling decision --------------------------------------------

    #[test]
    fn action_sampled_uniform_1_in_16() {
        // key % 16 == 0 sampled; key % 16 != 0 skipped (when under BOTH the
        // large-exec and large-payload overrides). Sub-threshold payload (0)
        // isolates the uniform gate.
        assert!(
            calib_action_sampled(0, 1_000, 0),
            "key 0 (0 % 16 == 0) must be sampled under the uniform 1/16 gate"
        );
        assert!(
            !calib_action_sampled(1, 1_000, 0),
            "key 1 (1 % 16 == 1) must be skipped under the uniform 1/16 gate \
             (no large-exec or large-payload override at 1 s / 0 B)"
        );
        assert!(
            calib_action_sampled(32, 1_000, 0),
            "key 32 (32 % 16 == 0) must be sampled"
        );
        assert!(
            !calib_action_sampled(17, 1_000, 0),
            "key 17 (17 % 16 == 1) must be skipped"
        );
    }

    #[test]
    fn action_sampled_rate_is_approximately_1_in_16() {
        // Over 1600 distinct keys the true-count must be ~100 (1600/16),
        // neither always-true nor always-false. Use sub-threshold exec so the
        // override never masks the uniform rate.
        let mut count: u64 = 0;
        for k in 0..1600u64 {
            if calib_action_sampled(k, 1_000, 0) {
                count += 1;
            }
        }
        assert!(
            (50..=150).contains(&count),
            "expected ~100 sampled out of 1600 (1/16 rate); got {count} — \
             if 1600: gate always-true (period became 1); if 0: gate always-false"
        );
    }

    #[test]
    fn action_large_exec_override_boundary() {
        // Exactly 60 s (= 60_000 ms) is NOT over-threshold (strict >), so an
        // unsampled key stays unsampled at the boundary.
        assert!(
            !calib_action_sampled(1, CALIB_LARGE_EXEC_MS_THRESHOLD, 0),
            "exec_duration == 60_000 ms is NOT over the threshold (strict >); \
             an unsampled key must remain unsampled at the boundary"
        );
        // One millisecond over → 1/1 override fires even for an unsampled key.
        assert!(
            calib_action_sampled(1, CALIB_LARGE_EXEC_MS_THRESHOLD + 1, 0),
            "exec_duration == 60_001 ms is over the 60 s threshold; the 1/1 \
             large-action override must force-sample even an unsampled key"
        );
    }

    #[test]
    fn action_large_payload_override_captures_lto_shape() {
        // GAP-2: the LTO-link shape — huge fetched payload, small proto tree,
        // <60 s exec, and an UNLUCKY digest-hash key (not in the uniform 1/16) —
        // is dropped by BOTH the exec floor (1 s < 60 s) and the tree floor
        // (which P-A never checks), so only the new payload floor can capture it.
        // key 1 is out of the 1/16 sample (1 % 16 == 1); exec 1 s is sub-60 s;
        // payload 500 MiB + 1 is over-threshold → MUST force-sample 1/1.
        assert!(
            calib_action_sampled(1, 1_000, CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD + 1),
            "large-payload/small-tree/<60 s LTO action (unsampled key, 1 s exec, \
             500 MiB + 1 payload) MUST be force-sampled by the payload floor — \
             it is dropped by the exec floor and invisible to the tree floor"
        );
        // A small-payload action at the same unlucky key + sub-60 s exec stays
        // subject to the uniform 1/16 (NOT force-sampled) — the floor must not
        // over-capture and flood the log with ordinary actions.
        assert!(
            !calib_action_sampled(1, 1_000, 4_096),
            "a 4 KiB-payload action at an unsampled key + 1 s exec must stay \
             subject to the uniform 1/16 gate — the payload floor must not \
             force-sample small-payload actions"
        );
    }

    #[test]
    fn action_large_payload_override_boundary() {
        // Exactly 500 MiB is NOT over-threshold (strict >), so an unsampled key
        // with sub-60 s exec stays unsampled at the boundary.
        assert!(
            !calib_action_sampled(1, 1_000, CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD),
            "payload == 500 MiB is NOT over the threshold (strict >); an \
             unsampled key + sub-60 s exec must remain unsampled at the boundary"
        );
        // One byte over → 1/1 override fires even for an unsampled key.
        assert!(
            calib_action_sampled(1, 1_000, CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD + 1),
            "payload == 500 MiB + 1 is over the threshold; the 1/1 large-payload \
             override must force-sample even an unsampled key"
        );
    }

    #[test]
    fn uniformly_sampled_is_the_1_in_16_gate_without_the_exec_override() {
        // `calib_uniformly_sampled` is the poll-start decision: it must be the
        // uniform 1/16 gate ONLY, with NO exec-duration override folded in (the
        // duration is not known upfront when the poll is armed).
        assert!(
            calib_uniformly_sampled(0),
            "key 0 (0 % 16 == 0) is in the uniform 1/16 sample"
        );
        assert!(
            !calib_uniformly_sampled(1),
            "key 1 (1 % 16 == 1) is NOT in the uniform 1/16 sample"
        );
        assert!(
            calib_uniformly_sampled(32),
            "key 32 (32 % 16 == 0) is in the uniform 1/16 sample"
        );
    }

    #[test]
    fn action_sampled_delegates_to_uniform_predicate_plus_override() {
        // `calib_action_sampled` must equal
        // `uniform || large-exec-override || large-payload-override` for every
        // combination — the extraction of `calib_uniformly_sampled` must not
        // change the decision, and neither the exec nor the payload override may
        // mask or be masked by the other. Cover uniform-in/out keys, sub- and
        // over-threshold exec, and sub- and over-threshold payload.
        for &key in &[0u64, 1, 15, 16, 17, 32] {
            for &exec_ms in &[
                0i64,
                1_000,
                CALIB_LARGE_EXEC_MS_THRESHOLD,
                CALIB_LARGE_EXEC_MS_THRESHOLD + 1,
            ] {
                for &payload in &[
                    0u64,
                    1_000,
                    CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD,
                    CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD + 1,
                ] {
                    let expected = calib_uniformly_sampled(key)
                        || exec_ms > CALIB_LARGE_EXEC_MS_THRESHOLD
                        || payload > CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD;
                    assert_eq!(
                        calib_action_sampled(key, exec_ms, payload),
                        expected,
                        "calib_action_sampled(key={key}, exec_ms={exec_ms}, \
                         payload={payload}) must equal uniform({key}) || \
                         exec>{CALIB_LARGE_EXEC_MS_THRESHOLD} || \
                         payload>{CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD}; a floor \
                         changed or masked the decision"
                    );
                }
            }
        }
    }

    // --- P-A completion-arm harvest gate (the fix's own failure mode) ------

    #[test]
    fn harvest_returns_none_when_never_captured() {
        // The completion arm must record `None` (NOT `Some(0)`) when the poll
        // never captured a sample (child died before the first tick / non-macOS
        // / spawn failure). This is the symmetric hazard of the shipped bug:
        // `Some(0)` would masquerade as a real zero-CPU action (ratio 0.0 →
        // io_bound) instead of being dropped as unavailable (testing-czar C1).
        let has_value = AtomicBool::new(false);
        let last = core::sync::atomic::AtomicU64::new(0);
        assert_eq!(
            super::calib_harvest_cpu_time(&has_value, &last),
            None,
            "a poll that never captured (has_value=false) must harvest None so \
             completion records None — a zero-CPU action must never masquerade \
             as a real sample via Some(0)"
        );
    }

    #[test]
    fn harvest_returns_last_when_captured() {
        // has_value=true → the harvest returns the last stored value (even when
        // that value is 0, a genuine live-but-idle child capture, distinct from
        // the never-captured None above).
        let has_value = AtomicBool::new(true);
        let last = core::sync::atomic::AtomicU64::new(1_234_000);
        assert_eq!(
            super::calib_harvest_cpu_time(&has_value, &last),
            Some(1_234_000),
            "has_value=true must harvest the last captured value verbatim"
        );
        let zero = core::sync::atomic::AtomicU64::new(0);
        let flagged = AtomicBool::new(true);
        assert_eq!(
            super::calib_harvest_cpu_time(&flagged, &zero),
            Some(0),
            "a live-but-idle child that captured Some(0) must harvest Some(0), \
             distinct from the never-captured None case"
        );
    }

    // --- P-A single-pid capture (Linux `None` contract) -------------------

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn capture_cpu_time_ms_is_none_on_non_macos() {
        // On non-macOS there is NO zero-behavior-change CPU-time path for a
        // tokio-managed child (reading `wait4` ourselves would double-reap), so
        // the single-pid helper deliberately yields `None` — the poll then never
        // sets `has_value` and the P-A record carries `cpu_time_ms = None`
        // (dropped in offline analysis, NOT counted as a zero-CPU sample). This
        // pins that contract on the build box and keeps the pure helper (and its
        // ns→ms truncation wrapper) unit-referenced there.
        assert_eq!(
            super::calib_capture_cpu_time_ms(1),
            None,
            "non-macOS calib_capture_cpu_time_ms must be a deliberate None (no \
             double-reap of the tokio-managed child), so no false Some(0) is recorded"
        );
    }

    // --- P-A poll-arm decision (bounds the probe's per-action cost) --------

    #[test]
    fn should_arm_poll_all_four_branches() {
        // The poll is armed iff the child has a queryable PID AND the action is
        // in the uniform 1/16 sample. All four (pid present/absent × sampled/
        // not) must be covered — the FALSE branch bounds the background-task
        // cost (testing-czar I2). key 0 is sampled (0 % 16 == 0); key 1 is not.
        assert!(
            super::calib_should_arm_poll(Some(42), 0),
            "pid present + sampled key → arm the poll"
        );
        assert!(
            !super::calib_should_arm_poll(Some(42), 1),
            "pid present + UNsampled key → do NOT arm (bounds per-action cost)"
        );
        assert!(
            !super::calib_should_arm_poll(None, 0),
            "no queryable pid → do NOT arm even for a sampled key"
        );
        assert!(
            !super::calib_should_arm_poll(None, 1),
            "no pid + unsampled → do NOT arm"
        );
    }

    // --- P-A subtree CPU accumulation (task-local pid→max-ns map) ----------

    #[test]
    fn subtree_accumulate_keeps_max_per_pid_and_survives_exit() {
        // The task-local map must (a) keep the MAX cpu_ns per pid (guards a
        // transient glitch; CPU is monotonic per live process), and (b) SURVIVE
        // a pid dropping out of a later poll (a child that ran then exited) so
        // its CPU stays counted — sequential children A-then-B are both summed
        // (auditor Claim 1 / red-team A1-2 subtree fix).
        let mut map: HashMap<i32, u64> = HashMap::new();

        // Tick 1: parent 100 accrued 10 ns, child 200 accrued 5 ns.
        super::calib_accumulate_subtree_cpu(&mut map, 100, 10);
        super::calib_accumulate_subtree_cpu(&mut map, 200, 5);
        assert_eq!(map.values().sum::<u64>(), 15, "tick 1 sum = 10 + 5");

        // Tick 2: child 200 exited (not observed this tick); parent 100 grew to
        // 25 ns; a new sequential child 300 accrued 7 ns. The exited child's 5
        // ns must remain counted.
        super::calib_accumulate_subtree_cpu(&mut map, 100, 25);
        super::calib_accumulate_subtree_cpu(&mut map, 300, 7);
        assert_eq!(
            map.values().sum::<u64>(),
            37,
            "exited child 200's 5 ns must persist (map survives exit); \
             25 (parent grown) + 5 (exited child) + 7 (new child) = 37"
        );

        // A transient glitch: pid 100 reports a SMALLER value than before. The
        // max must be retained, not the glitch.
        super::calib_accumulate_subtree_cpu(&mut map, 100, 3);
        assert_eq!(
            *map.get(&100).expect("pid 100 present"),
            25,
            "a smaller later reading is a glitch; max (25) must be retained — \
             CPU is monotonic per live process"
        );
    }

    // --- P-A CPU-time poll loop (state machine, platform-agnostic) ---------

    #[nativelink_test]
    async fn poll_loop_keeps_last_some_and_stops_on_first_none() {
        // The poll loop's contract (the bug that shipped: capture ran AFTER the
        // child was reaped so it was structurally always None). This drives the
        // loop's core with an INJECTED capture returning Some(10), Some(20),
        // Some(30), None — no real syscall, no real 250 ms sleep — and asserts:
        //   (1) the stored last-value is the LAST Some seen (30), NOT the first;
        //   (2) has_value is set (so completion reads Some);
        //   (3) the loop TERMINATES on the first None (does not hang).
        use core::sync::atomic::AtomicU64;
        use std::sync::Arc;

        let seq = Arc::new(parking_lot::Mutex::new(
            vec![Some(30u64), Some(20), Some(10)], // popped from the back → 10,20,30
        ));
        let last = Arc::new(AtomicU64::new(0));
        let has_value = Arc::new(AtomicBool::new(false));

        let seq_c = Arc::clone(&seq);
        // Zero interval so the loop runs to completion instantly; the injected
        // capture yields None on the 4th call, which must break the loop.
        tokio::time::timeout(
            core::time::Duration::from_secs(5),
            calib_poll_cpu_time_loop(
                core::time::Duration::ZERO,
                move || {
                    let next = seq_c.lock().pop().flatten();
                    core::future::ready(next)
                },
                Arc::clone(&last),
                Arc::clone(&has_value),
            ),
        )
        .await
        .expect("poll loop must terminate on the first None — it did not stop, so it would leak/hang");

        assert!(
            has_value.load(Ordering::Relaxed),
            "has_value must be set once any Some was captured — completion reads \
             this to decide whether to store Some(cpu_time_ms)"
        );
        assert_eq!(
            last.load(Ordering::Relaxed),
            30,
            "the stored value must be the LAST Some (30), not the first (10) — a \
             mid-execution poll is meaningless if it keeps a stale early sample"
        );
    }

    #[nativelink_test]
    async fn poll_loop_stores_some_zero_flagged_distinct_from_none() {
        // A live-but-idle child (I/O-blocked, ~0 CPU-ns) captures `Some(0)`. The
        // loop must STORE it and SET has_value — so the completion harvest reads
        // `Some(0)` (a real zero-CPU sample), NOT `None`. This locks in that
        // `Some(0)` (store + flag) is distinct from `None`→break; the two must
        // never collapse (testing-czar R2).
        use core::sync::atomic::AtomicU64;
        use std::sync::Arc;

        let seq = Arc::new(parking_lot::Mutex::new(vec![Some(0u64)])); // then None
        let last = Arc::new(AtomicU64::new(0));
        let has_value = Arc::new(AtomicBool::new(false));

        let seq_c = Arc::clone(&seq);
        tokio::time::timeout(
            core::time::Duration::from_secs(5),
            calib_poll_cpu_time_loop(
                core::time::Duration::ZERO,
                move || core::future::ready(seq_c.lock().pop().flatten()),
                Arc::clone(&last),
                Arc::clone(&has_value),
            ),
        )
        .await
        .expect("poll loop must terminate after the Some(0) then None");

        assert!(
            has_value.load(Ordering::Acquire),
            "capturing Some(0) must SET has_value so the harvest reads Some(0), \
             NOT leave it false (which would collapse Some(0) into the \
             never-captured None case)"
        );
        assert_eq!(
            last.load(Ordering::Relaxed),
            0,
            "the stored value must be the captured 0 (a real idle-child sample)"
        );
    }

    /// LIVE-PROCESS VALUE test — the one that catches BOTH the original bug
    /// (capture-after-reap → structurally always `None`) AND the mach-timebase
    /// unit bug (raw ticks read as ns → CPU ~40× under-reported → every action
    /// misclassified `io_bound`). Spawns a real child that BURNS CPU in a
    /// FORK-FREE busy spin (pure `sh` arithmetic — no forked `date`/`expr`, so
    /// the accounted CPU lands in the direct child the single-pid capture reads;
    /// `sleep` would accrue ~0 CPU and not exercise the accounting), polls its
    /// live PID during execution, and asserts the captured `cpu_time_ms` is a
    /// PLAUSIBLE fraction of the child's measured wall time — NOT merely `Some`.
    ///
    /// The band is deliberately loose (scheduling noise, the ≤250 ms last-sample
    /// undercount, and a shared build box all shave the ratio) but tight enough
    /// to catch a 40× unit error: a single-thread busy spin burns CPU ≈ wall, so
    /// a correct reading lands near the wall ms while a raw-ticks (unconverted)
    /// reading lands at wall/~42 — far below the floor. See red-team's
    /// "value-blind by construction" finding: the OLD assert (`has_value` only,
    /// explicitly declining `v > 0`) stayed green with the value 40× wrong.
    ///
    /// macOS-only: `proc_pidinfo` accounting is the macOS primitive; on Linux
    /// `calib_capture_cpu_time_ms` is a `const None`, so this can only be RUN on
    /// a macOS worker (absent on the Linux build box).
    #[cfg(target_os = "macos")]
    #[nativelink_test]
    async fn live_child_cpu_time_value_is_plausible_not_40x_off() {
        use core::sync::atomic::AtomicU64;
        use std::sync::Arc;
        use std::time::Instant;

        use super::calib_capture_cpu_time_ms;

        // FORK-FREE busy spin: `[` and `$((…))` are `sh` builtins, so ALL the
        // CPU burns in this direct child process (nothing forked), which is what
        // the single-pid capture reads. A large fixed iteration count guarantees
        // a multi-hundred-ms spin on any real box; the exact wall time is
        // measured below rather than assumed.
        let child_wall_start = Instant::now();
        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg("i=0; while [ $i -lt 40000000 ]; do i=$((i+1)); done")
            .spawn()
            .expect("must spawn the spinning child");
        let pid = child.id().expect("live child must have a PID before reap");

        let last = Arc::new(AtomicU64::new(0));
        let has_value = Arc::new(AtomicBool::new(false));

        // Poll the LIVE pid at a short interval so several samples land inside
        // the spin window (single-pid capture — the spin is fork-free).
        let last_c = Arc::clone(&last);
        let has_c = Arc::clone(&has_value);
        let poll = nativelink_util::background_spawn!("test_calib_poll", async move {
            calib_poll_cpu_time_loop(
                core::time::Duration::from_millis(50),
                move || core::future::ready(calib_capture_cpu_time_ms(pid)),
                last_c,
                has_c,
            )
            .await;
        });

        child.wait().await.expect("child must exit");
        let child_wall_ms = child_wall_start.elapsed().as_millis() as u64;
        // The loop self-terminates on the first None after reap; join it so the
        // asserts see the final stored value.
        poll.await.expect("poll task must join");

        assert!(
            has_value.load(Ordering::Acquire),
            "a live-process poll must capture at least one Some — capture-after-reap \
             (the original bug) yields None for every action"
        );
        let cpu_ms = last.load(Ordering::Relaxed);

        // Floor: a fork-free busy spin that ran `child_wall_ms` of wall time burns
        // ≈ that much single-thread CPU. Require the reading to be at least ~30%
        // of wall — comfortably above a 40×-under-reported raw-ticks reading
        // (which would be ~wall/42 ≈ 2.4% of wall) yet below a correct reading.
        let floor_ms = (child_wall_ms * 3) / 10;
        assert!(
            cpu_ms >= floor_ms,
            "captured cpu_time_ms {cpu_ms} is implausibly low vs {child_wall_ms} ms wall \
             (floor {floor_ms}); a fork-free single-thread spin burns CPU ≈ wall — a value \
             this low means the mach-timebase ns conversion regressed (raw ticks read as ns \
             under-report ~40×)"
        );
        // Ceiling: a single-thread spin cannot accrue much MORE CPU than wall;
        // allow 2× for measurement slop / multi-core accounting quirks. Catches
        // a wildly-scaled-up conversion.
        assert!(
            cpu_ms <= child_wall_ms * 2 + 500,
            "captured cpu_time_ms {cpu_ms} exceeds 2× the {child_wall_ms} ms wall + slop; \
             a single-thread spin cannot burn that much CPU — the conversion scaled up wrong"
        );
    }

    /// LIVE SUBTREE test — proves FIX 3: `calib_capture_subtree_cpu_ns` sums the
    /// FORKED-DESCENDANT CPU that a single-process `proc_pidinfo` read misses
    /// (rustc→rust-lld, cc-wrapper→cc1). A thin parent `sh` forks a grandchild
    /// that does the CPU spinning, then `wait`s (near-zero own CPU). Single-pid
    /// capture on the parent would read ≈0; the subtree walk must pick up the
    /// grandchild's CPU. Asserts the summed subtree ns is a plausible fraction of
    /// wall — i.e. the grandchild's burn IS counted.
    ///
    /// **Shape scope (deliberate):** this tests the parent-OUTLIVES-child shape —
    /// the `& wait` keeps the parent alive so the grandchild stays IN the subtree
    /// on every tick, the one shape the walk handles perfectly. It does NOT
    /// exercise the reparent-orphan shape (intermediate exits, descendant
    /// reparents to launchd and leaves the walk — see the
    /// `calib_capture_subtree_cpu_ns` residuals), which is a known under-count
    /// validated on-worker via the canary ratio grep, not by this test.
    ///
    /// macOS-only (same reason as the value test).
    #[cfg(target_os = "macos")]
    #[nativelink_test]
    async fn live_child_subtree_cpu_includes_grandchild_burn() {
        use std::collections::HashMap;
        use std::time::Instant;

        use super::calib_capture_subtree_cpu_ns;

        // Parent `sh` forks a grandchild that runs the fork-free busy spin, then
        // `wait`s for it (the parent itself burns ≈0 CPU — its work is the fork +
        // wait). So the parent's OWN task CPU is near-zero and the interesting
        // CPU lives entirely in the forked grandchild — exactly the shape a
        // single-pid `proc_pidinfo` read would miss.
        let child_wall_start = Instant::now();
        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg("sh -c 'i=0; while [ $i -lt 40000000 ]; do i=$((i+1)); done' & wait")
            .spawn()
            .expect("must spawn the parent that forks a spinning grandchild");
        let pid = child.id().expect("live child must have a PID before reap");

        // Poll the subtree directly (not through the generic loop) so we exercise
        // the descendant walk + the task-local accumulator across ticks. Keep the
        // last non-None subtree total; stop on the first None (root reaped).
        let mut map: HashMap<libc::pid_t, u64> = HashMap::new();
        let mut last_subtree_ns: Option<u64> = None;
        loop {
            match calib_capture_subtree_cpu_ns(pid, &mut map) {
                Some(ns) => last_subtree_ns = Some(ns),
                None => break,
            }
            tokio::time::sleep(core::time::Duration::from_millis(50)).await;
        }
        child.wait().await.expect("child must exit");
        let child_wall_ms = child_wall_start.elapsed().as_millis() as u64;

        let subtree_ns =
            last_subtree_ns.expect("subtree poll must have captured at least one live sample");
        let subtree_ms = subtree_ns / 1_000_000;

        // The grandchild burned CPU ≈ its wall time; the parent burned ≈0. If the
        // subtree walk correctly summed the grandchild, subtree_ms is a large
        // fraction of wall. A single-pid read of the parent alone would be ≈0 and
        // FAIL this floor — that is the FIX-3 contract.
        let floor_ms = (child_wall_ms * 3) / 10;
        assert!(
            subtree_ms >= floor_ms,
            "subtree cpu {subtree_ms} ms is below the {floor_ms} ms floor for a \
             {child_wall_ms} ms wall grandchild spin — the descendant CPU was NOT summed \
             (a single-pid read of the ~idle parent would read ≈0; FIX 3 must count the \
             forked grandchild)"
        );
    }

    // --- P-B sampling decision --------------------------------------------

    #[test]
    fn staging_sampled_uniform_and_large_tree_boundary() {
        // Sub-threshold missing-bytes (0) isolates the uniform + large-tree gates.
        assert!(
            calib_staging_sampled(0, 1_000, 0),
            "key 0 sampled under uniform 1/16"
        );
        assert!(
            !calib_staging_sampled(1, 1_000, 0),
            "key 1 skipped under uniform 1/16 (small tree, small fetch, no override)"
        );
        // Exactly 100 MiB is NOT over-threshold (strict >).
        assert!(
            !calib_staging_sampled(1, CALIB_LARGE_TREE_BYTES_THRESHOLD, 0),
            "input_tree_bytes == 100 MiB is NOT over the threshold (strict >); \
             an unsampled key must remain unsampled at the boundary"
        );
        // One byte over → 1/1 override fires.
        assert!(
            calib_staging_sampled(1, CALIB_LARGE_TREE_BYTES_THRESHOLD + 1, 0),
            "input_tree_bytes == 100 MiB + 1 is over the threshold; the 1/1 \
             large-tree override must force-sample even an unsampled key"
        );
    }

    #[test]
    fn staging_sampled_large_missing_bytes_captures_refetch_storm() {
        // GAP-2 R1: a re-fetch storm — huge network-fetch (missing) bytes but a
        // SMALL proto tree (< 100 MiB) — at an UNLUCKY digest-hash key is dropped
        // by both the uniform 1/16 and the tree floor; only the missing-bytes
        // floor captures it. key 1 is out of the sample; tree 1 KiB is sub-floor;
        // missing 500 MiB + 1 is over-threshold → MUST force-sample 1/1.
        assert!(
            calib_staging_sampled(1, 1_000, CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD + 1),
            "large-fetch/small-tree staging (unsampled key, 1 KiB proto tree, \
             500 MiB + 1 missing bytes) MUST be force-sampled by the missing-bytes \
             floor — the R1 re-fetch storm the tree floor cannot see"
        );
        // A small-fetch staging at the same unlucky key + small tree stays
        // subject to the uniform 1/16 (NOT force-sampled).
        assert!(
            !calib_staging_sampled(1, 1_000, 4_096),
            "a 4 KiB-fetch staging at an unsampled key + small tree must stay \
             subject to the uniform 1/16 gate — the missing-bytes floor must not \
             force-sample small-fetch staging records"
        );
    }

    #[test]
    fn staging_sampled_large_missing_bytes_boundary() {
        // Exactly 500 MiB missing is NOT over-threshold (strict >).
        assert!(
            !calib_staging_sampled(1, 1_000, CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD),
            "missing_bytes == 500 MiB is NOT over the threshold (strict >); an \
             unsampled key + small tree must remain unsampled at the boundary"
        );
        // One byte over → 1/1 override fires even for an unsampled key.
        assert!(
            calib_staging_sampled(1, 1_000, CALIB_LARGE_PAYLOAD_BYTES_THRESHOLD + 1),
            "missing_bytes == 500 MiB + 1 is over the threshold; the 1/1 \
             large-fetch override must force-sample even an unsampled key"
        );
    }

    #[test]
    fn digest_sample_key_is_first_8_bytes_le() {
        // The key extractor must round-trip the LE-packed key so the sampling
        // gate keys on the digest hash deterministically.
        assert_eq!(
            calib_digest_sample_key(&digest_with_key(0)),
            0,
            "all-zero hash → key 0"
        );
        assert_eq!(
            calib_digest_sample_key(&digest_with_key(0x0102_0304_0506_0708)),
            0x0102_0304_0506_0708,
            "first 8 hash bytes must decode LE into the sampling key"
        );
    }

    // --- Classification (§3 bands) ----------------------------------------

    #[test]
    fn classify_band_boundaries() {
        // < 0.5 → IoBound; [0.5, 1.5] → Ambiguous; > 1.5 → MultiCoreCpuBound.
        assert_eq!(
            calib_classify(0.49),
            CalibActionShape::IoBound,
            "0.49 < 0.5 is I/O-bound"
        );
        assert_eq!(
            calib_classify(0.5),
            CalibActionShape::Ambiguous,
            "0.5 is the closed lower edge of the ambiguous band, NOT I/O-bound"
        );
        assert_eq!(
            calib_classify(1.5),
            CalibActionShape::Ambiguous,
            "1.5 is the closed upper edge of the ambiguous band, NOT CPU-bound"
        );
        assert_eq!(
            calib_classify(1.51),
            CalibActionShape::MultiCoreCpuBound,
            "1.51 > 1.5 is multi-core CPU-bound"
        );
    }

    #[test]
    fn shape_labels_are_stable() {
        // Offline analysis keys on these literal strings.
        assert_eq!(CalibActionShape::IoBound.as_str(), "io_bound");
        assert_eq!(CalibActionShape::Ambiguous.as_str(), "ambiguous");
        assert_eq!(
            CalibActionShape::MultiCoreCpuBound.as_str(),
            "multi_core_cpu_bound"
        );
    }

    // --- P-A record builder -----------------------------------------------

    #[test]
    fn action_record_computes_ratio_and_shape() {
        // cpu 200 ms / wall 100 ms = 2.0 → MultiCoreCpuBound.
        let rec = calib_build_action_record(100, Some(200), 4_096, 512, 3);
        assert_eq!(rec.exec_duration_ms, 100);
        assert_eq!(rec.cpu_time_ms, Some(200));
        assert_eq!(
            rec.cpu_wall_ratio,
            Some(2.0),
            "ratio must be cpu_time_ms / exec_duration_ms = 200/100 = 2.0"
        );
        assert_eq!(rec.input_bytes, 4_096);
        assert_eq!(rec.output_bytes, 512);
        assert_eq!(rec.worker_running_actions_at_start, 3);
        assert_eq!(
            rec.shape(),
            Some(CalibActionShape::MultiCoreCpuBound),
            "ratio 2.0 > 1.5 classifies multi-core CPU-bound"
        );
    }

    #[test]
    fn action_record_ratio_absent_when_cpu_unavailable() {
        let rec = calib_build_action_record(100, None, 0, 0, 1);
        assert_eq!(
            rec.cpu_wall_ratio, None,
            "cpu_wall_ratio must be None when cpu_time_ms is unavailable — \
             offline analysis must drop the sample, not read it as ratio 0"
        );
        assert_eq!(
            rec.shape(),
            None,
            "no shape can be assigned without a cpu_wall_ratio"
        );
    }

    #[test]
    fn action_record_ratio_absent_on_nonpositive_wall() {
        // Clock skew can make execution_completed precede execution_start →
        // exec_duration_ms <= 0; a divide by that would be garbage/inf.
        let rec = calib_build_action_record(0, Some(50), 0, 0, 1);
        assert_eq!(
            rec.cpu_wall_ratio, None,
            "cpu_wall_ratio must be None when exec_duration_ms <= 0 (clock skew) \
             — must not divide by zero or produce inf"
        );
    }

    // --- P-B tree-totals sum ----------------------------------------------

    #[test]
    fn tree_totals_sums_dir_bytes_and_file_counts() {
        // Two directories of sizes 1000 and 24 bytes; 2 + 1 = 3 files total.
        let mut tree: HashMap<DigestInfo, ProtoDirectory> = HashMap::new();
        tree.insert(
            DigestInfo::new([1u8; 32], 1000),
            ProtoDirectory {
                files: vec![FileNode::default(), FileNode::default()],
                ..ProtoDirectory::default()
            },
        );
        tree.insert(
            DigestInfo::new([2u8; 32], 24),
            ProtoDirectory {
                files: vec![FileNode::default()],
                ..ProtoDirectory::default()
            },
        );
        let (bytes, files) = calib_tree_totals(&tree);
        assert_eq!(
            bytes, 1024,
            "input_tree_bytes must sum directory-digest sizes (1000 + 24 = 1024) \
             — the :552-553 pattern (§9 B3)"
        );
        assert_eq!(
            files, 3,
            "input_tree_files must sum per-directory file counts (2 + 1 = 3)"
        );
    }

    #[test]
    fn tree_totals_empty_tree_is_zero() {
        let tree: HashMap<DigestInfo, ProtoDirectory> = HashMap::new();
        assert_eq!(
            calib_tree_totals(&tree),
            (0, 0),
            "an empty resolved tree must yield (0 bytes, 0 files), not panic"
        );
    }

    // --- Record equality sanity (emit is a thin wrapper) ------------------

    #[test]
    fn records_are_value_types() {
        // Confirms the builder produces a fully-populated value; the emit()
        // methods only format these fields.
        let a = CalibActionRecord {
            exec_duration_ms: 10,
            cpu_time_ms: Some(5),
            cpu_wall_ratio: Some(0.5),
            input_bytes: 1,
            output_bytes: 2,
            worker_running_actions_at_start: 1,
        };
        assert_eq!(a.clone(), a);
        // Distinct values per byte axis so a field swap (payload↔missing↔tree)
        // is caught: payload (full) > missing (fetch-only) > tree (proto), the
        // physically-expected ordering for a partially-cached miss.
        let s = CalibStagingRecord {
            input_staging_ms: 7,
            dir_cache_hit: false,
            input_payload_bytes: 42,
            input_missing_bytes: 30,
            input_tree_bytes: 9,
            input_tree_files: 3,
        };
        assert_eq!(s.clone(), s);
        assert_eq!(
            s.input_missing_bytes, 30,
            "input_missing_bytes (network-fetch axis) must carry the missing-byte \
             value, distinct from input_payload_bytes (full payload)"
        );
        assert_ne!(
            s.input_missing_bytes, s.input_payload_bytes,
            "missing bytes (fetch-only) and payload bytes (fetch+hardlink) are \
             distinct axes; a swap would collapse the §6 fetch-vs-hardlink split"
        );
    }
}
