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

use core::hash::BuildHasher;
use core::pin::Pin;
use core::str;
use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::process::Stdio;
use std::sync::{Arc, Weak};
use std::time::Instant;

use futures::future::{BoxFuture, OptionFuture};
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt, TryFutureExt, select};
use nativelink_config::cas_server::{EnvironmentSource, LocalWorkerConfig};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BisAck, BlobDigestInfo, BlobsAvailableAck, BlobsAvailableChunk, BlobsAvailableNotification,
    BlobsInStableStorageChunk, ExecuteComplete, ExecuteResult, GoingAwayRequest, KeepAliveRequest,
    MirrorPinEntry, MissingBlobPeers, PeerHintsChunk, UpdateForWorker, chunked_message,
    execute_result,
};
use nativelink_store::fast_slow_store::{FastSlowStore, SlowTierMetricSink};
use nativelink_store::filesystem_store::{FilesystemStore, IndefinitePinOutcome};
use nativelink_util::action_messages::{ActionResult, ActionStage, OperationId};
use nativelink_util::blob_locality_map::{SharedBlobLocalityMap, Stamp};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::metrics_publisher::MetricsRegistry;
use nativelink_util::metrics_utils::{AsyncCounterWrapper, Counter, CounterWithTime};
use nativelink_util::phase0_metrics::worker_phase0_metrics;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::store_trait::{
    IS_WORKER_REQUEST, ItemCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{spawn, tls_utils};
use opentelemetry::context::Context;
use parking_lot::Mutex;
use tokio::process;
use tokio::sync::{Notify, Semaphore, broadcast, mpsc};
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Streaming;
use tracing::{Level, debug, error, event, info, info_span, instrument, trace, warn};

use crate::running_actions_manager::{
    ExecutionConfiguration, Metrics as RunningActionManagerMetrics, RunningAction,
    RunningActionsManager, RunningActionsManagerArgs, RunningActionsManagerImpl,
};
use crate::worker_api_client_wrapper::{WorkerApiClientTrait, WorkerApiClientWrapper};
use crate::worker_utils::{boot_epoch_id, make_connect_worker_request};

/// Maximum backstop interval for BlobsAvailable reports (milliseconds).
/// The send loop normally wakes immediately on blob changes via `Notify`,
/// but this backstop ensures subtree-only changes (which don't fire the
/// tracker notify) are still reported within a bounded time.
/// At 100ms with 10 workers the server sees ~100 msgs/s worst case, each
/// coalesced via drain-then-fire. Empty ticks are skipped (no send when
/// there are no changes), so idle workers generate zero traffic.
const BLOBS_AVAILABLE_MAX_INTERVAL_MS: u64 = 100;

/// (FL-688 v3 §3.8) Cap on the worker's `BlobsAvailable` resend buffer —
/// the bounded ring of unacked delta `BlobsAvailableChunk`s the worker
/// holds until the server's `BlobsAvailableAck` arrives (drain-on-ack).
/// Over this cap the worker CLEARS the buffer and forces a fresh FULL
/// SNAPSHOT broadcast, which supersedes every buffered delta (the
/// self-correcting reset — the convergence the removed 60s heartbeat used
/// to provide, now triggered by buffer pressure = an EVENT, not a timer).
///
/// Sized to one broadcast's worth of sequences: the chunker caps a single
/// broadcast at `MAX_SEQUENCES` (256, `blobs_available_chunking.rs`), so
/// 256 lets a full snapshot's chunks sit in flight without tripping the
/// reset, while still bounding worst-case buffer RSS at
/// 256 × `BLOBS_AVAILABLE_PER_CHUNK` (4096) digest entries ≈ ~63 MiB at
/// the per-entry worst case (a `BlobDigestInfo` ≈ 60 bytes); in steady
/// state a responsive server acks within an RTT so the buffer stays
/// near-empty and the typical chunk is far smaller. A non-acking server
/// (partition) is the only path that grows it, and that path is the one
/// the full-snapshot reset valve exists to bound.
// CAPPED AT BLOBS_AVAILABLE_RESEND_MAX_CHUNKS: a bounded ring of unacked
// worker→server delta chunks on the network path; over-cap → clear +
// force a full-snapshot broadcast (lossless: the snapshot is a superset).
pub const BLOBS_AVAILABLE_RESEND_MAX_CHUNKS: usize = 256;

/// Platform-specific cumulative CPU time reading.
#[cfg(target_os = "linux")]
mod cpu_impl {
    pub(super) struct CpuTimes {
        pub(super) busy: u64,
        pub(super) total: u64,
    }

    pub(super) fn read_cpu_times() -> Option<CpuTimes> {
        let contents = std::fs::read_to_string("/proc/stat").ok()?;
        let line = contents.lines().next()?;
        if !line.starts_with("cpu ") {
            return None;
        }
        // fields: user(0) nice(1) system(2) idle(3) iowait(4) irq(5) softirq(6) steal(7)
        let fields: Vec<u64> = line[4..]
            .split_whitespace()
            .filter_map(|s| s.parse().ok())
            .collect();
        if fields.len() < 8 {
            return None;
        }
        let busy = fields[0] + fields[1] + fields[2] + fields[5] + fields[6] + fields[7];
        let total = busy + fields[3] + fields[4];
        Some(CpuTimes { busy, total })
    }

    /// (#sched-blend) Linux does not split P/E logical CPUs here, so it
    /// reports `(0, 0)` ("unknown") → the scheduler falls back to its
    /// configured `assume_core_count`, preserving today's %-only behavior.
    /// (A future `available_parallelism()` count is a deferred follow-up.)
    pub(super) const fn core_counts() -> (u32, u32) {
        (0, 0)
    }

    /// (#task-resource-profile Phase-3 §6) Total physical RAM in KiB from
    /// `/proc/meminfo` `MemTotal` (already reported in kB by the kernel).
    /// `0` on any read/parse failure — best-effort, never crashes the worker;
    /// the scheduler treats `0` as "unknown" (contributes no RAISE-clamp
    /// capacity ceiling). Read ONCE at connect (static for the worker's life).
    pub(super) fn total_memory_kb() -> u64 {
        let Ok(contents) = std::fs::read_to_string("/proc/meminfo") else {
            return 0;
        };
        for line in contents.lines() {
            // Format: "MemTotal:       16327624 kB"
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                if let Some(kb) = rest.split_whitespace().next() {
                    return kb.parse().unwrap_or(0);
                }
            }
        }
        0
    }
}

#[cfg(target_os = "macos")]
mod cpu_impl {
    const CPU_STATE_USER: usize = 0;
    const CPU_STATE_SYSTEM: usize = 1;
    const CPU_STATE_IDLE: usize = 2;
    const CPU_STATE_NICE: usize = 3;
    const CPU_STATE_MAX: usize = 4;
    const PROCESSOR_CPU_LOAD_INFO: i32 = 2;

    unsafe extern "C" {
        fn mach_host_self() -> u32;
        fn mach_task_self() -> u32;
        fn host_processor_info(
            host: u32,
            flavor: i32,
            out_processor_count: *mut u32,
            out_processor_info: *mut *mut i32,
            out_processor_info_cnt: *mut u32,
        ) -> i32;
        fn vm_deallocate(target_task: u32, address: usize, size: usize) -> i32;
    }

    pub(super) struct CpuTimes {
        pub(super) busy: u64,
        pub(super) total: u64,
    }

    pub(super) struct PerTypeCpuTimes {
        pub(super) aggregate: CpuTimes,
        pub(super) p_core: CpuTimes,
        pub(super) e_core: CpuTimes,
        pub(super) has_e_cores: bool,
    }

    /// Returns the number of P-cores on Apple Silicon via sysctl.
    /// Returns 0 on Intel Macs (sysctl key doesn't exist).
    pub(super) fn p_core_count() -> u32 {
        use std::sync::OnceLock;
        static COUNT: OnceLock<u32> = OnceLock::new();
        *COUNT.get_or_init(|| sysctl_u32("hw.perflevel0.logicalcpu").unwrap_or(0))
    }

    /// Returns the number of E-cores on Apple Silicon via sysctl.
    /// Returns 0 on Intel Macs or P-core-only Apple Silicon.
    pub(super) fn e_core_count() -> u32 {
        use std::sync::OnceLock;
        static COUNT: OnceLock<u32> = OnceLock::new();
        *COUNT.get_or_init(|| sysctl_u32("hw.perflevel1.logicalcpu").unwrap_or(0))
    }

    /// (#sched-blend) Static (P, E) logical-CPU counts reported on the
    /// connect hello frame so the scheduler can rank workers by absolute
    /// free core capacity. Both `OnceLock`-cached — no per-call syscall.
    pub(super) fn core_counts() -> (u32, u32) {
        (p_core_count(), e_core_count())
    }

    /// (#task-resource-profile Phase-3 §6) Total physical RAM in KiB from the
    /// `hw.memsize` sysctl (reported in BYTES) divided by 1024. `0` on failure —
    /// best-effort, never crashes the worker. `OnceLock`-cached (static for the
    /// worker's life). The scheduler treats `0` as "unknown" (no RAISE-clamp
    /// capacity ceiling).
    pub(super) fn total_memory_kb() -> u64 {
        use std::sync::OnceLock;
        static KB: OnceLock<u64> = OnceLock::new();
        *KB.get_or_init(|| sysctl_u64("hw.memsize").map_or(0, |bytes| bytes / 1024))
    }

    fn sysctl_u64(name: &str) -> Option<u64> {
        use std::ffi::CString;
        let cname = CString::new(name).ok()?;
        let mut val: u64 = 0;
        let mut len = core::mem::size_of::<u64>();
        // SAFETY: sysctlbyname is a stable POSIX API on macOS.
        let ret = unsafe {
            libc::sysctlbyname(
                cname.as_ptr(),
                &raw mut val as *mut _,
                &mut len,
                core::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 { Some(val) } else { None }
    }

    fn sysctl_u32(name: &str) -> Option<u32> {
        use std::ffi::CString;
        let cname = CString::new(name).ok()?;
        let mut val: u32 = 0;
        let mut len = core::mem::size_of::<u32>();
        // SAFETY: sysctlbyname is a stable POSIX API on macOS.
        let ret = unsafe {
            libc::sysctlbyname(
                cname.as_ptr(),
                &raw mut val as *mut _,
                &mut len,
                core::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 { Some(val) } else { None }
    }

    /// Reads per-logical-CPU tick data via host_processor_info and splits
    /// into aggregate, P-core, and E-core buckets.
    pub(super) fn read_per_type_cpu_times() -> Option<PerTypeCpuTimes> {
        use std::sync::OnceLock;
        static HOST_PORT: OnceLock<u32> = OnceLock::new();

        let p_count = p_core_count();
        let e_count = e_core_count();

        // SAFETY: host_processor_info is a stable macOS kernel API.
        // We check the return code and deallocate the kernel-allocated buffer.
        unsafe {
            let host = *HOST_PORT.get_or_init(|| mach_host_self());
            let mut cpu_count: u32 = 0;
            let mut info_array: *mut i32 = core::ptr::null_mut();
            let mut info_count: u32 = 0;
            let ret = host_processor_info(
                host,
                PROCESSOR_CPU_LOAD_INFO,
                &mut cpu_count,
                &mut info_array,
                &mut info_count,
            );
            if ret != 0 || info_array.is_null() {
                return None;
            }

            // Materialize each logical CPU's busy/total from the kernel buffer,
            // then hand off to the pure, unit-tested `split_pe_ticks` for the
            // P/E bucketing. `per_cpu` is a local per-tick scratch buffer, not
            // network-reachable, bounded by `cpu_count` (logical CPU count,
            // ~10 on M4).
            let mut per_cpu = Vec::with_capacity(cpu_count as usize);
            for i in 0..cpu_count {
                let base = (i as usize) * CPU_STATE_MAX;
                let user = *info_array.add(base + CPU_STATE_USER) as u64;
                let system = *info_array.add(base + CPU_STATE_SYSTEM) as u64;
                let idle = *info_array.add(base + CPU_STATE_IDLE) as u64;
                let nice = *info_array.add(base + CPU_STATE_NICE) as u64;
                let busy = user + system + nice;
                per_cpu.push(super::CpuTicks {
                    busy,
                    total: busy + idle,
                });
            }

            let kr = vm_deallocate(
                mach_task_self(),
                info_array as usize,
                (info_count as usize) * core::mem::size_of::<i32>(),
            );
            debug_assert_eq!(kr, 0, "vm_deallocate failed: {kr}");

            let split = super::split_pe_ticks(cpu_count, p_count, e_count, &per_cpu);

            Some(PerTypeCpuTimes {
                aggregate: CpuTimes {
                    busy: split.aggregate.busy,
                    total: split.aggregate.total,
                },
                p_core: CpuTimes {
                    busy: split.p_core.busy,
                    total: split.p_core.total,
                },
                e_core: CpuTimes {
                    busy: split.e_core.busy,
                    total: split.e_core.total,
                },
                has_e_cores: e_count > 0,
            })
        }
    }

    pub(super) fn read_cpu_times() -> Option<CpuTimes> {
        read_per_type_cpu_times().map(|t| t.aggregate)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod cpu_impl {
    pub(super) struct CpuTimes {
        pub(super) busy: u64,
        pub(super) total: u64,
    }

    pub(super) fn read_cpu_times() -> Option<CpuTimes> {
        None
    }

    /// (#sched-blend) No P/E split on this platform → `(0, 0)` ("unknown");
    /// the scheduler uses its `assume_core_count` fallback.
    pub(super) const fn core_counts() -> (u32, u32) {
        (0, 0)
    }

    /// (#task-resource-profile Phase-3 §6) No RAM query on this platform →
    /// `0` ("unknown"); the scheduler contributes no RAISE-clamp ceiling.
    pub(super) const fn total_memory_kb() -> u64 {
        0
    }
}

/// One logical CPU's cumulative busy/total ticks — the per-CPU input element
/// to [`split_pe_ticks`]. Defined at module level (not inside the macOS
/// `cpu_impl` block) so the P/E split is unit-tested on ALL platforms, the
/// same reason [`compute_available_bytes`] lives here.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(super) struct CpuTicks {
    pub(super) busy: u64,
    pub(super) total: u64,
}

/// Aggregate + P-core + E-core cumulative tick buckets produced by
/// [`split_pe_ticks`]. Named fields (not a positional tuple) so the caller
/// can never transpose the P and E buckets — the exact class of bug this
/// function fixes.
pub(super) struct SplitCpuTicks {
    pub(super) aggregate: CpuTicks,
    pub(super) p_core: CpuTicks,
    pub(super) e_core: CpuTicks,
}

/// Split per-logical-CPU tick data into aggregate, P-core, and E-core buckets.
///
/// APPLE SILICON ENUMERATION (the load-bearing fact): `host_processor_info`
/// lists the E-cores FIRST — logical CPUs `0..e_count` — and the P-cores LAST
/// — `e_count..cpu_count` — on M-series chips. So the P bucket is the HIGH
/// indices `i >= e_count`, NOT the low indices `i < p_count`. The old
/// `i < p_count` predicate charged the first `p_count` E-cores into the P
/// bucket, SWAPPING `p_core_load_pct` and `e_core_load_pct`. Empirically
/// confirmed on M4 (P=4, E=6) by
/// `~/fl/bld/infra/nativelink/synthetic-core-metric-validate.sh`: pinning load
/// to the 4 P-cores read `p_core_load=0%, e_core_load=66%`, and pinning to the
/// 6 E-cores read `p_core_load=100%, e_core_load=34%` — exactly the index math
/// for E-first enumeration under the old buggy predicate.
///
/// Heterogeneous (a real P/E split) only when `p_count > 0` AND the P and E
/// counts partition every logical CPU (`p_count + e_count == cpu_count`). On
/// Intel Macs / Linux `p_count == 0`; a hypothetical third core class would
/// fail the sum. Either way the function folds all CPUs into the P bucket (the
/// aggregate) and leaves the E bucket zero — the scheduler's `has_e_cores`
/// policy is applied by the caller, not here.
///
/// Extracted as a pure function so unit tests exercise THIS production path
/// (not an inline replica), giving the mutation guard real coverage.
pub(super) fn split_pe_ticks(
    cpu_count: u32,
    p_count: u32,
    e_count: u32,
    per_cpu: &[CpuTicks],
) -> SplitCpuTicks {
    let is_heterogeneous = p_count > 0 && (p_count + e_count == cpu_count);

    let mut aggregate = CpuTicks::default();
    let mut p_core = CpuTicks::default();
    let mut e_core = CpuTicks::default();

    for (i, cpu) in per_cpu.iter().enumerate() {
        aggregate.busy += cpu.busy;
        aggregate.total += cpu.total;
        if !is_heterogeneous {
            continue;
        }
        // P-cores are the HIGH indices on Apple Silicon (E-cores enumerate
        // first); see the function-level doc comment for the empirical proof.
        if i as u32 >= e_count {
            p_core.busy += cpu.busy;
            p_core.total += cpu.total;
        } else {
            e_core.busy += cpu.busy;
            e_core.total += cpu.total;
        }
    }

    if !is_heterogeneous {
        // Linux / Intel / unknown-chip fallback: treat every core as a P-core
        // (identical to today's non-heterogeneous behavior).
        p_core = aggregate;
    }

    SplitCpuTicks {
        aggregate,
        p_core,
        e_core,
    }
}

/// (#task-memgate-twosignal) One sampler-tick read of the memory-pressure
/// signals — SPLIT into the three demand/supply counters the two-signal gate
/// keys off, PLUS the free-floor fail-safe. All come from a SINGLE platform
/// read so the signals are coherent for one tick.
///
/// The signal model (web-verified 2026-07-16): macOS decompresses ONLY on
/// page fault (demand-driven), so `decompressions` and `swapins` are RELIABLE
/// demand signals; `compressions` is a SUPPLY/proactive signal. They measure
/// different depths — `swapins` (RAM+compressor overflowed to DISK) is the
/// OOM-adjacent HARD signal; `decompressions` (working-set > uncompressed RAM)
/// is a per-fault LATENCY signal. `compressions` alone is confounded (proactive
/// cold-page reclaim), but `min(compressions, decompressions)` is high ONLY
/// when the compressor is CHURNING (pages compressed to reclaim AND immediately
/// faulted back = working set exceeds uncompressed RAM = genuine pressure) —
/// the graded perf scalar (operator insight, 2026-07-16).
#[derive(Clone, Copy)]
pub(super) struct MemorySignals {
    /// Reclaimable host RAM in bytes (the free-floor FAIL-SAFE): on macOS this
    /// is `(free_count + inactive_count + purgeable_count) * page_size` — the
    /// full pool of pages the kernel can reclaim without swapping. The old
    /// raw-`free_count` floor false-tripped fleet-wide (#64 incident
    /// `5132d6c9`): busy raw-free is normally 200-900 MiB even when 7+ GiB
    /// are available. Speculative pages are already counted in `free_count`
    /// (XNU vm_statistics.h:158-163) and are NOT added separately. macOS keeps
    /// `available` up BY compressing, so this floor is BLIND to
    /// compression-hidden overcommit (fires only near-exhaustion) — it is the
    /// last-ditch fail-safe, no longer the primary.
    pub(super) free_bytes: u64,
    /// Monotonic macOS `compressions` counter (Linux: PSI `full total=` µs, the
    /// same demand/stall analogue used for `decompressions`). SUPPLY/proactive
    /// on its own; folded with `decompressions` into the compressor-CHURN scalar
    /// (`min(compress_rate, decompress_rate)` is high only when BOTH are high).
    pub(super) compressions_cumulative: u64,
    /// Monotonic macOS `decompressions` counter (Linux: PSI `full total=` µs) —
    /// pages faulting BACK in from the compressor. The DEMAND/latency signal;
    /// half of the compressor-churn scalar. Does NOT hard-NAK (it is perf, not
    /// OOM).
    pub(super) decompressions_cumulative: u64,
    /// Monotonic macOS `swapins` counter (Linux: `/proc/vmstat` `pswpin`, real
    /// disk swap-in pages) — the OOM-adjacent HARD signal. Baseline is 0 (a full
    /// dbg build measured 0 swapins), so a SUSTAINED swapin rate needs no
    /// calibration: any sustained swapin = working set overflowed RAM+compressor
    /// to disk. Drives the OOM `MEMORY_PRESSURED` boolean (with the free-floor
    /// fail-safe).
    pub(super) swapins_cumulative: u64,
}

/// Compute available memory bytes from raw page counts and page size.
/// `available = (free + inactive + purgeable) * page_size`.
///
/// `speculative_count` is NOT included — speculative pages are already
/// accounted for in `free_count` (XNU vm_statistics.h:158-163). Adding
/// them again would double-count by ~200 MiB on real workers.
///
/// This is the production formula for the #64 fix (raw `free_count *
/// page_size` was the regression — it false-tripped on healthy workers
/// where inactive+purgeable dominate). Extracted as a pure function so
/// unit tests exercise THIS code path, not an inline replica, giving the
/// mutation guard real production coverage.
///
/// Defined at this level (not inside the `cfg(target_os = "macos")`
/// `mem_impl` block) so it is reachable by tests on all platforms.
pub(super) const fn compute_available_bytes(
    free_count: u64,
    inactive_count: u64,
    purgeable_count: u64,
    page_size: u64,
) -> u64 {
    // speculative already in free_count (XNU vm_statistics.h:158-163)
    free_count
        .saturating_add(inactive_count)
        .saturating_add(purgeable_count)
        .saturating_mul(page_size)
}

/// Platform-specific host memory-pressure sampling. Mirrors `cpu_impl`:
/// the macOS path reads `sysctl vm.swapusage` (observability) + a single
/// mach `host_statistics64(HOST_VM_INFO64)` (free-floor + re-fault); the
/// Linux path reads `/proc/meminfo` (`MemAvailable` free-floor + swap used)
/// and `/proc/pressure/memory` (PSI `full total=` re-fault analogue);
/// everything else is a no-op. Each call is a couple of syscalls / small
/// file reads (no per-tick `vm_stat` fork); the dedicated sampler thread
/// reads them on the existing 100 ms cadence and stores the derived values
/// into atomics the heartbeat + the worker-local gate read.
#[cfg(target_os = "linux")]
mod mem_impl {
    use super::MemorySignals;

    /// Host swap currently in use, in bytes. `/proc/meminfo` reports
    /// `SwapTotal`/`SwapFree` in KiB; used = (total - free) * 1024.
    pub(super) fn read_swap_used_bytes() -> Option<u64> {
        let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
        let mut total_kib: Option<u64> = None;
        let mut free_kib: Option<u64> = None;
        for line in contents.lines() {
            if let Some(rest) = line.strip_prefix("SwapTotal:") {
                total_kib = rest.split_whitespace().next().and_then(|v| v.parse().ok());
            } else if let Some(rest) = line.strip_prefix("SwapFree:") {
                free_kib = rest.split_whitespace().next().and_then(|v| v.parse().ok());
            }
        }
        let (total, free) = (total_kib?, free_kib?);
        Some(total.saturating_sub(free).saturating_mul(1024))
    }

    /// (#37 rev-4) Read the free-floor PRIMARY (`MemAvailable`) + the
    /// re-fault CORROBORATION (PSI `full total=`) in one pass.
    ///
    /// - PRIMARY: `/proc/meminfo` `MemAvailable` (KiB) — the kernel's own
    ///   "memory available to start a new app without swapping" estimate,
    ///   the Linux analogue of the macOS free-page floor. Required; the read
    ///   fails (returns `None`) if it is absent.
    /// - CORROBORATION: `/proc/pressure/memory` `full … total=<µs>` — the
    ///   MONOTONIC cumulative-stall-µs counter (NOT `avg10`, which is a
    ///   pre-normalized windowed fraction that the strictly-delta/elapsed
    ///   `compute_swap_pressure_rate` would read as ~0 under sustained
    ///   steady stall — design §0-rev4.4). Absent PSI (kernel without
    ///   `CONFIG_PSI`) ⇒ corroboration `0`; the free-floor primary still
    ///   gates.
    pub(super) fn read_memory_signals() -> Option<MemorySignals> {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let mut avail_kib: Option<u64> = None;
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemAvailable:") {
                avail_kib = rest.split_whitespace().next().and_then(|v| v.parse().ok());
                break;
            }
        }
        let free_bytes = avail_kib?.saturating_mul(1024);
        // (#task-memgate-twosignal) Linux analogues of the macOS three-counter
        // split. The production fleet is macOS; Linux is a fallback (dev/tests
        // read the pure fns, not this platform impl), so the mapping is a sane
        // approximation, not a calibrated port:
        // - swapins = `/proc/vmstat` `pswpin` (pages swapped IN from disk) — the
        //   direct analogue of macOS `Swapins` (real disk-spill demand).
        // - compressions == decompressions == PSI `full total=<µs>` (cumulative
        //   memory-stall). Stock Linux has no compressor-churn counter, so the
        //   churn scalar `min(comp, decomp)` collapses to the PSI stall rate — a
        //   reasonable "how pressured" analogue. Absent PSI (no CONFIG_PSI) ⇒ 0.
        let psi_stall = std::fs::read_to_string("/proc/pressure/memory")
            .ok()
            .and_then(|psi| read_psi_full_total(&psi))
            .unwrap_or(0);
        let swapins_cumulative = std::fs::read_to_string("/proc/vmstat")
            .ok()
            .and_then(|vmstat| read_vmstat_counter(&vmstat, "pswpin"))
            .unwrap_or(0);
        Some(MemorySignals {
            free_bytes,
            compressions_cumulative: psi_stall,
            decompressions_cumulative: psi_stall,
            swapins_cumulative,
        })
    }

    /// Parse the `full` line's `total=<µs>` cumulative-stall counter from
    /// the contents of `/proc/pressure/memory`. Returns `None` if the file
    /// has no `full` line or no `total=` token (caller treats as 0).
    fn read_psi_full_total(psi: &str) -> Option<u64> {
        for line in psi.lines() {
            if let Some(rest) = line.strip_prefix("full ") {
                for tok in rest.split_whitespace() {
                    if let Some(val) = tok.strip_prefix("total=") {
                        return val.parse().ok();
                    }
                }
            }
        }
        None
    }

    /// (#task-memgate-twosignal) Parse a named single-value counter (e.g.
    /// `pswpin <n>`) from `/proc/vmstat`. Returns `None` if the key is absent
    /// or unparsable (caller treats as 0).
    fn read_vmstat_counter(vmstat: &str, key: &str) -> Option<u64> {
        for line in vmstat.lines() {
            if let Some(rest) = line.strip_prefix(key) {
                return rest.split_whitespace().next().and_then(|v| v.parse().ok());
            }
        }
        None
    }
}

#[cfg(target_os = "macos")]
mod mem_impl {
    use super::MemorySignals;
    use libc::{
        host_statistics64, integer_t, mach_host_self, mach_msg_type_number_t, vm_statistics64,
        HOST_VM_INFO64, HOST_VM_INFO64_COUNT,
    };

    // `sysctl vm.swapusage` returns a `struct xsw_usage`. Layout from
    // <sys/sysctl.h>; we read `xsu_used` (bytes). repr(C) so the field
    // offsets match the kernel ABI.
    #[repr(C)]
    struct XswUsage {
        xsu_total: u64,
        xsu_avail: u64,
        xsu_used: u64,
        xsu_pagesize: u32,
        xsu_encrypted: u8,
    }

    /// Host swap currently in use, in bytes, via `sysctl vm.swapusage`.
    pub(super) fn read_swap_used_bytes() -> Option<u64> {
        use std::ffi::CString;
        let cname = CString::new("vm.swapusage").ok()?;
        let mut usage = XswUsage {
            xsu_total: 0,
            xsu_avail: 0,
            xsu_used: 0,
            xsu_pagesize: 0,
            xsu_encrypted: 0,
        };
        let mut len = core::mem::size_of::<XswUsage>();
        // SAFETY: sysctlbyname is a stable POSIX API on macOS; `usage`
        // is a correctly-sized repr(C) buffer matching `struct xsw_usage`.
        let ret = unsafe {
            libc::sysctlbyname(
                cname.as_ptr(),
                &raw mut usage as *mut _,
                &mut len,
                core::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 { Some(usage.xsu_used) } else { None }
    }

    /// Host page size in bytes, via `sysctl hw.pagesize`. Apple Silicon is
    /// 16 KiB, Intel Macs 4 KiB — NOT hardcoded (design §0-rev4.2: "read
    /// `host_page_size` or hw.pagesize, don't hardcode") so `free_count`
    /// (a PAGE count) converts to bytes correctly on either. `None` on
    /// syscall failure ⇒ the caller treats the read as unavailable.
    fn read_page_size() -> Option<u64> {
        use std::ffi::CString;
        let cname = CString::new("hw.pagesize").ok()?;
        // hw.pagesize is reported as a 32-bit int on Darwin.
        let mut value: i32 = 0;
        let mut len = core::mem::size_of::<i32>();
        // SAFETY: sysctlbyname is a stable POSIX API; `value` is a
        // correctly-sized i32 buffer; newp is null (read-only request).
        let ret = unsafe {
            libc::sysctlbyname(
                cname.as_ptr(),
                &raw mut value as *mut _,
                &mut len,
                core::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 && value > 0 {
            Some(value as u64)
        } else {
            None
        }
    }

    /// (#37 rev-4, re-enable follow-up) Read the available-memory PRIMARY
    /// (`free_count + inactive_count + purgeable_count` × page size) + the
    /// re-fault CORROBORATION (`decompressions + swapins`) from a SINGLE
    /// mach `host_statistics64(HOST_VM_INFO64)` call.
    ///
    /// - PRIMARY: `available = (free_count + inactive_count + purgeable_count)
    ///   * page_size`. This is the full reclaimable pool — the same "available"
    ///   shown by Activity Monitor and psutil. On a healthy 16 GiB worker under
    ///   load: raw `free_count` ≈ 200-900 MiB (sub-floor) while `available`
    ///   ≈ 7-8 GiB (healthy). The old raw-`free_count` floor was the #64
    ///   incident (`5132d6c9`): it false-tripped fleet-wide at 3.6k-NAK/min
    ///   because busy workers always park most RAM in `inactive`. `inactive`
    ///   includes both clean reclaimable file pages AND dirty-anonymous pages
    ///   that must compress first; the re-fault CORROBORATION is the backstop
    ///   for the dirty-anon over-count case. `speculative_count` is NOT added
    ///   — speculative pages are already in `free_count` (XNU
    ///   vm_statistics.h:158-163); adding them double-counts by ~200 MiB.
    /// - CORROBORATION: `decompressions + swapins` — pages faulting BACK in
    ///   (the thrash tell). Monotonic until reboot; the sampler turns it
    ///   into a per-second rate via the existing delta/elapsed → EWMA path.
    ///   `swapins` is included for completeness/cross-platform symmetry but
    ///   contributes ~0 on the compressor-dominant fleet; `decompressions`
    ///   carries the signal.
    ///
    /// All fields are read BY NAME off libc's `vm_statistics64` — no offset
    /// arithmetic, no hand-rolled struct, no `62/248` literals (design
    /// §0-rev4.7: those are canonical-XNU and would overrun the 152-byte
    /// libc struct; `HOST_VM_INFO64_COUNT` is computed from libc's own
    /// struct = 38, never 62).
    pub(super) fn read_memory_signals() -> Option<MemorySignals> {
        // Use libc's own `vm_statistics64` (`#[repr(packed(8))]`) rather
        // than a hand-rolled struct: `free_count` is the first field but
        // `decompressions`/`swapins` sit DEEP (well past `pageouts`@40), so
        // a hand-rolled copy that diverges in the tail cannot reach them at
        // the correct offset. libc tracks the upstream ABI, so a future
        // field shift is a noticed crate bump, not a silent offset slide.
        let mut stats = vm_statistics64 {
            free_count: 0,
            active_count: 0,
            inactive_count: 0,
            wire_count: 0,
            zero_fill_count: 0,
            reactivations: 0,
            pageins: 0,
            pageouts: 0,
            faults: 0,
            cow_faults: 0,
            lookups: 0,
            hits: 0,
            purges: 0,
            purgeable_count: 0,
            speculative_count: 0,
            decompressions: 0,
            compressions: 0,
            swapins: 0,
            swapouts: 0,
            compressor_page_count: 0,
            throttled_count: 0,
            external_page_count: 0,
            internal_page_count: 0,
            total_uncompressed_pages_in_compressor: 0,
        };
        let mut count: mach_msg_type_number_t = HOST_VM_INFO64_COUNT;
        // SAFETY: host_statistics64 is a stable macOS kernel API. `stats`
        // is libc's `vm_statistics64` (`#[repr(packed(8))]`) whose size
        // (HOST_VM_INFO64_COUNT words = 38, computed from libc's OWN struct
        // — never the canonical-XNU 62 that would overrun the 152-byte
        // buffer) is >= the running kernel's HOST_VM_INFO64 revision size,
        // and `count` is initialized to that word count. The kernel does
        // NOT write our full `count` words: host.c `vm_stats` clamps to its
        // OWN revision (REV0/REV1/REV2), writes only that many fields, never
        // overruns past its revision size (a larger caller buffer is left
        // untouched), and overwrites `*count` with the words actually
        // written. `free_count`, `inactive_count`, `purgeable_count`, and
        // `speculative_count` are ALL REV0 (before the `decompressions`
        // REV0/REV1 boundary in HOST_VM_INFO64_REV0_COUNT — auditor AA-7);
        // `decompressions`/`swapins` are REV1, present on every Apple
        // Silicon kernel. We read them only when ret == 0.
        let ret = unsafe {
            host_statistics64(
                mach_host_self(),
                HOST_VM_INFO64,
                std::ptr::addr_of_mut!(stats).cast::<integer_t>(),
                &mut count,
            )
        };
        if ret != 0 {
            return None;
        }
        // Copy packed fields to locals before arithmetic to avoid taking a
        // reference into the `#[repr(packed(8))]` struct.
        let free_count = u64::from(stats.free_count);
        // available = free + inactive + purgeable; speculative is already
        // in free_count (XNU vm_statistics.h:158-163), do NOT add it again.
        let inactive_count = u64::from(stats.inactive_count);
        let purgeable_count = u64::from(stats.purgeable_count);
        // (#task-memgate-twosignal) The three counters are now published
        // SEPARATELY (they were summed into a single `refault_cumulative` before):
        // `swapins` drives the OOM hard-gate; `compressions`+`decompressions` feed
        // the compressor-churn perf scalar.
        let compressions = stats.compressions;
        let decompressions = stats.decompressions;
        let swapins = stats.swapins;
        let page_size = read_page_size()?;
        Some(MemorySignals {
            free_bytes: super::compute_available_bytes(free_count, inactive_count, purgeable_count, page_size),
            compressions_cumulative: compressions,
            decompressions_cumulative: decompressions,
            swapins_cumulative: swapins,
        })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod mem_impl {
    use super::MemorySignals;

    pub(super) fn read_swap_used_bytes() -> Option<u64> {
        None
    }
    pub(super) fn read_memory_signals() -> Option<MemorySignals> {
        None
    }
}

static CPU_PCT: AtomicU32 = AtomicU32::new(0);
static P_CORE_PCT: AtomicU32 = AtomicU32::new(0);
static E_CORE_PCT: AtomicU32 = AtomicU32::new(0);
/// Host swap-used bytes, refreshed by the sampler thread every 100 ms.
/// `0` = no swap in use OR sampler unavailable (indistinguishable, per
/// the `cpu_load_pct = 0` unknown convention). Observability only — the
/// gate keys off the free-floor PRIMARY below, not this lingering LEVEL
/// gauge (swap occupancy can stay high long after pressure subsides).
static SWAP_USED_BYTES: AtomicU64 = AtomicU64::new(0);
/// (#task-memgate-twosignal) Worker GRADED perf scalar: the compressor-CHURN
/// EWMA (`min(compress_rate_ewma, decompress_rate_ewma)`, events/sec, saturating
/// `u32`). RE-KEYED from the old MiB-below-floor magnitude: this scalar is now
/// the "how pressured (perf)" signal the scheduler consumes — the least-pressured
/// `min_by_key` fail-open ranking (lower = less pressured) AND the NET-NEW DOWN
/// overcommit churn-throttle (a churning candidate backs off overcommit). Refreshed
/// by the sampler thread every 100 ms; carried on the wire (field 20). High ONLY
/// when compression AND decompression are BOTH high (the compressor is thrashing);
/// proactive cold-page reclaim (compress-high, decompress-low) and one-time
/// working-set shifts (decompress-high, compress-low) both yield a LOW `min` →
/// no false pressure. Does NOT hard-NAK (perf, not OOM — the OOM boolean
/// `MEMORY_PRESSURED` keys off SWAPINS + the free-floor). The scalar is published
/// unconditionally (regardless of `MEMORY_GATE_ENABLED`) so the scheduler ranking
/// + throttle see it even when the OOM gate is off.
static MEMORY_PRESSURE_LEVEL: AtomicU32 = AtomicU32::new(0);
/// (#37) Monotonic nanoseconds (since `PROCESS_START`) at which the
/// memory sampler last published a value. The worker-local gate reads this
/// to detect a wedged/dead sampler: if the most recent sample is older
/// than `SWAP_SAMPLE_MAX_AGE`, the gate's pressure state is UNKNOWN and
/// it FAILS OPEN (§3a rule 2 / §5 case 4). NET-NEW — the CPU sampler has
/// no age atomic to inherit. Monotonic source so an NTP step cannot
/// spuriously trip or suppress the fail-open. `0` = never sampled yet.
static LAST_SAMPLE_INSTANT: AtomicU64 = AtomicU64::new(0);
/// (#task-memgate-twosignal) Coarse 1-bit OOM gate verdict published by the
/// sampler: `true` when the free-page FLOOR is breached (fail-safe) OR the
/// SWAPIN rate has stayed above `SWAPIN_CONFIRM_RATE` for `SWAPIN_CONFIRM_WINDOW_TICKS`
/// consecutive ticks (the sustained-window OOM signal — a one-off spike touching
/// ancient swapped pages does NOT trip), AND the sample is fresh (OR logic).
/// The compressor-churn perf scalar does NOT participate here — it is perf, not
/// OOM. Mirrors `indefinite_pin_saturated`: the heartbeat carries it (advisory
/// matcher hint) and the worker-local StartAction NAK reads it (authoritative
/// gate — the local atomic, never the wire boolean, is the safety-critical
/// decision). `false` when fresh-and-healthy, stale (fail-open), or
/// sampler-unavailable.
static MEMORY_PRESSURED: AtomicBool = AtomicBool::new(false);
/// (#task-memgate-twosignal) Per-trip-source latch set by the sampler on
/// every tick where the gate is enabled AND pressured; cleared when not
/// pressured, when the gate is disabled, or when the signal read fails
/// (unreadable tick). Lets the StartAction NAK warn log WHICH signal tripped
/// so soak data distinguishes a free-floor NAK from a swapin NAK
/// unambiguously. Both are published independently — either or both can be
/// set when `MEMORY_PRESSURED` is true.
static MEMORY_GATE_TRIP_FREE_FLOOR: AtomicBool = AtomicBool::new(false);
/// See `MEMORY_GATE_TRIP_FREE_FLOOR` — sustained-SWAPIN OOM-path trip source latch.
static MEMORY_GATE_TRIP_SWAPIN: AtomicBool = AtomicBool::new(false);
static SAMPLER_STARTED: AtomicBool = AtomicBool::new(false);

/// (F4) Physical free bytes on the worker's CAS/work_directory volume, refreshed
/// by the sampler thread every 100 ms via `statvfs` (`f_bavail * f_frsize`).
/// `u64::MAX` is the never-sampled sentinel (so the gate fails OPEN before the
/// first sample rather than reading `0` = "full" and NAKing everything at boot);
/// the sampler overwrites it with the real free-bytes value on its first tick.
/// Wire field 22 (observability + the server's most-free fail-open ranking).
static DISK_FREE_BYTES: AtomicU64 = AtomicU64::new(u64::MAX);
/// (F4) Monotonic nanoseconds (since `PROCESS_START`) at which the disk-free
/// sampler last published a value. The worker-local gate reads this to detect a
/// wedged/dead sampler: if the most recent sample is older than
/// `DISK_SAMPLE_MAX_AGE`, the gate's disk state is UNKNOWN and — UNLIKE the swap
/// gate, which blind-fails-open — it consults the authoritative `statvfs`
/// fallback (SEC-2: disk has no other live bound behind the gate). `0` = never
/// sampled yet (also routes to the fallback). Monotonic source so an NTP step
/// cannot spuriously trip or suppress the fallback.
static LAST_DISK_SAMPLE_INSTANT: AtomicU64 = AtomicU64::new(0);
/// (F4) Coarse 1-bit disk-gate verdict published by the sampler: `true` when
/// the disk-free FLOOR is breached (with a hysteresis band), AND the gate is
/// ENABLED. Mirrors `MEMORY_PRESSURED`: the heartbeat carries it (advisory
/// matcher hint) and the worker-local StartAction NAK reads it (authoritative
/// gate — the local atomic + statvfs fallback, never the wire boolean). `false`
/// when fresh-and-healthy, never-sampled, or disabled.
static DISK_PRESSURED: AtomicBool = AtomicBool::new(false);

/// (F4) PRIMARY disk-admission floor: free bytes on the worker's
/// CAS/work_directory volume below which the gate trips. The CAS fast tier
/// (40 GiB cap, `cas_FAST_SLOW_STORE.fast` FilesystemStore on the prod workers)
/// OVERSHOOTS its byte cap because moka's weight-based eviction is
/// EVENTUALLY-CONSISTENT and trails sustained large-blob ingest by ~4× (the F3b
/// finding); the work_directory shares that same physical volume (config
/// invariant `cas_server::LocalWorkerConfig::work_directory`). So this gate is
/// the BACKSTOP that rejects new work BEFORE raw ENOSPC at
/// `make_action_directory` while eviction catches up.
// THRESHOLD 8 GiB: a CONSERVATIVE safe-enable value on the 40 GiB prod fast
// tier — 20% of the cap. It must sit ABOVE the worst-case eviction-lag overshoot
// (so the gate leads ENOSPC with real margin: the largest observed chunked-write
// class is ~295 MiB, and a single action's work_directory footprint + a burst of
// in-flight large blobs is bounded well under 8 GiB) yet far below the cap (so a
// busy-but-healthy worker with normal headroom is not falsely gated). NOT
// soak-validated — pending a busy-worker soak that refines the lead-time; the
// statvfs authoritative fallback + the fleet fail-open bound the failure modes.
// Mirrors the #37 free-floor's conservative-safe-enable rationale (and heeds the
// #64 lesson: a too-aggressive floor false-trips fleet-wide — 8 GiB on a 40 GiB
// disk is structurally unlikely to false-trip a healthy worker).
const DISK_FREE_FLOOR_BYTES: u64 = 8 * (1 << 30); // 8 GiB

/// (F4) Hysteresis band above `DISK_FREE_FLOOR_BYTES` for CLEARING the disk-free
/// trip: trip below the floor, clear only once free recovers above
/// `DISK_FREE_FLOOR_BYTES + DISK_FREE_FLOOR_HYSTERESIS`, so a worker hovering at
/// the floor (eviction freeing then ingest refilling) does not chatter
/// admit/refuse every tick. 2 GiB = a quarter of the floor (mirrors the #37
/// free-floor's quarter-band).
const DISK_FREE_FLOOR_HYSTERESIS: u64 = 2 * (1 << 30); // 2 GiB

/// (F4) Master enable for the worker-local disk-pressure admission gate and its
/// heartbeat boolean. ENABLED: unlike the #37 memory floor (which read raw
/// `free_count` and false-tripped on macOS — #64), the disk free-floor reads the
/// AUTHORITATIVE `statvfs f_bavail` (free blocks available to non-root), which is
/// not subject to the free-vs-available ambiguity that sank the memory gate; the
/// 8 GiB floor on a 40 GiB cap is conservative; and the statvfs fallback +
/// fleet fail-open bound the failure modes. When enabled the gate NAKs new
/// `StartAction`s under sustained disk pressure and the matcher proactively skips
/// a pressured worker.
const DISK_GATE_ENABLED: bool = true;

/// (F4) Max age of the most recent disk sample before the gate treats its disk
/// state as UNKNOWN and consults the authoritative `statvfs` fallback (NOT a
/// blind fail-open — SEC-2). MUST be strictly GREATER than the worst-case
/// legitimate sampler stall and strictly LESS than the time for an un-gated
/// worker to ENOSPC. At the 100 ms sampler cadence, 2 s = 20 missed ticks
/// (mirrors `SWAP_SAMPLE_MAX_AGE`).
const DISK_SAMPLE_MAX_AGE: Duration = Duration::from_secs(2);

/// (F4) Time-bounded fleet fail-open window (§5 case 3b), mirroring
/// `SWAP_FAIL_OPEN_AFTER`. A worker that has been NAKing new work under disk
/// pressure with NO in-flight actions for longer than this accepts ONE action
/// regardless of pressure, so an all-idle all-disk-pressured fleet cannot
/// deadlock. The worker-local statvfs fallback already prevents admitting into a
/// TRULY-full disk, so this clause only fires when the sampler-reported pressure
/// has not yet cleared but the disk is not at the literal wall.
const DISK_FAIL_OPEN_AFTER: Duration = Duration::from_secs(30);

/// (F4) The `statvfs` target path the disk sampler measures — the worker's
/// `work_directory`, which shares one physical volume with the CAS
/// FilesystemStore `content_path` by config invariant
/// (`cas_server::LocalWorkerConfig::work_directory`). Set ONCE at sampler
/// startup (`start_cpu_sampler`). `None` ⇒ no path configured / CString
/// conversion failed ⇒ the disk sampler is inert (publishes the never-sampled
/// sentinel, and the gate's stale path then drives the statvfs fallback, which
/// also no-ops on a missing path → last-resort fail-open).
static DISK_SAMPLE_PATH: std::sync::OnceLock<Option<std::ffi::CString>> =
    std::sync::OnceLock::new();

/// (F4) Free bytes available to non-root on the volume containing `path`, via
/// `statvfs` (`f_bavail * f_frsize`). POSIX — works on macOS and Linux (the only
/// worker targets) with the SAME ABI. `None` on syscall failure (the caller
/// treats it as "unmeasurable"). This is a BLOCKING syscall; it MUST only be
/// called on the dedicated sampler OS thread (per-tick) or inside
/// `spawn_blocking` (the gate's one-shot stale fallback) — NEVER directly on a
/// tokio worker (CLAUDE.md async-blocking rule).
fn read_disk_available_bytes(path: &std::ffi::CStr) -> Option<u64> {
    // SAFETY: `statvfs` is a stable POSIX API; `buf` is a correctly-sized
    // zeroed `libc::statvfs` and `path` is a valid NUL-terminated C string.
    let mut buf: libc::statvfs = unsafe { core::mem::zeroed() };
    let ret = unsafe { libc::statvfs(path.as_ptr(), &raw mut buf) };
    if ret != 0 {
        return None;
    }
    // `f_bavail` = free blocks available to non-privileged processes (the
    // admission-relevant figure, not `f_bfree` which includes root reserve);
    // `f_frsize` = fundamental block size in bytes. Both are u64 on the worker
    // targets but `c_ulong` in the binding, so go through u64 explicitly.
    let avail = u64::try_from(buf.f_bavail).ok()?;
    let frsize = u64::try_from(buf.f_frsize).ok()?;
    Some(avail.saturating_mul(frsize))
}

/// (F4) One disk-sampler tick: `statvfs` the configured volume off the hot path
/// and publish `DISK_FREE_BYTES` (free-bytes gauge), `LAST_DISK_SAMPLE_INSTANT`
/// (liveness anchor for the gate's stale→statvfs-fallback decision), and
/// `DISK_PRESSURED` (the free-floor-breached gate verdict with a hysteresis
/// band). Returns the new `currently_tripped` state to thread into the next
/// tick. Runs on the dedicated sampler thread alongside the CPU/memory samplers
/// — NEVER per-action on a tokio worker. `currently_tripped` carries the
/// hysteresis-band verdict across ticks.
fn sample_disk_pressure(currently_tripped: bool) -> bool {
    let now = Instant::now();
    // Publish the liveness anchor on EVERY tick BEFORE the syscall can fail, so
    // a one-off unreadable statvfs doesn't look like a dead sampler — only a
    // sampler that stops ticking entirely trips the age fallback.
    let since_start = now.duration_since(*PROCESS_START).as_nanos();
    LAST_DISK_SAMPLE_INSTANT.store(
        u64::try_from(since_start).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );

    let Some(Some(path)) = DISK_SAMPLE_PATH.get() else {
        // No path configured: leave the never-sampled sentinel + report no
        // pressure. The gate's stale path will drive the (also-no-op) statvfs
        // fallback → last-resort fail-open.
        DISK_PRESSURED.store(false, Ordering::Relaxed);
        return false;
    };

    let Some(free_bytes) = read_disk_available_bytes(path) else {
        // statvfs failed this tick: report no pressure for this tick and leave
        // the prior free-bytes value (don't publish a spurious 0 = "full").
        DISK_PRESSURED.store(false, Ordering::Relaxed);
        return false;
    };

    DISK_FREE_BYTES.store(free_bytes, Ordering::Relaxed);
    let tripped = disk_floor_breached(free_bytes, currently_tripped);
    // The published gate verdict is gated on DISK_GATE_ENABLED (a disabled gate
    // never advertises pressure → the matcher never proactively skips on an
    // unproven threshold), matching memory_gate_verdict.
    DISK_PRESSURED.store(DISK_GATE_ENABLED && tripped, Ordering::Relaxed);
    tripped
}

/// Process-start anchor for the monotonic `LAST_SAMPLE_INSTANT` atomic.
/// `Instant` is not `Copy`-into-an-atomic, so the sampler stores
/// `now.duration_since(*PROCESS_START)` nanos and the gate compares
/// against the same anchor. Both ends use `Instant` (CLOCK_MONOTONIC),
/// never wall-clock.
static PROCESS_START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

/// (#37 rev-4) PRIMARY (leading) gate trip: free-page FLOOR. The gate
/// trips when available host RAM (`vm_statistics64.free_count` × page size
/// on macOS / `MemAvailable` on Linux) falls below this margin.
///
/// CONSERVATIVE safe-enable value, chosen from the idle probe's ~100×
/// separation between healthy and at-the-wall (design §0-rev4.2):
///   - healthy IDLE   = 8969 MiB free (574018 pages × 16 KiB)
///   - at-the-RAM-wall =   68 MiB free (4359 pages × 16 KiB)
/// 1 GiB sits safely INSIDE that gap: 9× below healthy headroom (so a
/// busy-but-healthy worker is very unlikely to dip below it) and 15× above
/// the at-wall floor (so it leads the cliff with real margin). At <1 GiB
/// free, a 16 GiB worker genuinely cannot admit another multi-GiB link
/// without thrashing. NOT soak-validated — this is a CONSERVATIVE
/// safe-enable value PENDING a busy-worker soak that refines it (the soak
/// optimizes LEAD-TIME; it does NOT gate enablement, given the 100×
/// separation makes a false-trip on a healthy worker structurally
/// implausible at this margin). The companion sustained-SWAPIN OOM signal
/// (`SWAPIN_CONFIRM_RATE`) catches the compression-hidden overcommit case
/// where free reads non-floor (macOS keeps `available` up by compressing)
/// but the box has spilled to disk swap. See design §0-rev4.2/.8.
const FREE_FLOOR_BYTES: u64 = 1 << 30; // 1 GiB

/// (#37 rev-4) Hysteresis band above `FREE_FLOOR_BYTES` for CLEARING the
/// free-floor trip: trip below `FREE_FLOOR_BYTES`, clear only once free
/// recovers above `FREE_FLOOR_BYTES + FREE_FLOOR_HYSTERESIS`, so the level
/// does not chatter at the boundary (design §0-rev4.4 — the free-floor is
/// a LEVEL, so it uses a two-threshold band, not the re-fault EWMA). 256
/// MiB = a quarter of the floor.
const FREE_FLOOR_HYSTERESIS: u64 = 256 << 20; // 256 MiB

/// (#task-memgate-twosignal) OOM hard-gate threshold: the SWAPIN rate
/// (Δswapins/Δt, events/sec) at/above which a sampler tick counts toward the
/// sustained-swapin OOM trip. `Swapins` = working set overflowed RAM+compressor
/// to DISK = OOM-adjacent. Safety rests on swapin SEMANTICS (a disk spill is
/// strictly deeper pressure than compression) plus the sustained window, NOT on
/// the threshold number: the measured `0` baseline came from a `--config=dbg`
/// build on an IDLE fleet, so the busy-worker baseline is UNMEASURED and
/// `nak_swapin` must be watched through a busy soak after enabling. A ONE-OFF
/// spike (a single tick touching ancient swapped pages) does NOT trip: the trip
/// also requires `SWAPIN_CONFIRM_WINDOW_TICKS` CONSECUTIVE ticks at/above this rate.
///
/// Config-sourced (like `MEMORY_GATE_ENABLED`): set ONCE at worker startup from
/// `LocalWorkerConfig::memory_gate_swapin_confirm_rate` (default `100`). Relaxed
/// ordering is sufficient — the startup store happens-before the sampler-thread
/// spawn (same justification as `MEMORY_GATE_ENABLED`). `NonZeroU32` in config
/// (0 would trip on every tick = self-inflicted NAK storm, the #64 failure mode).
static SWAPIN_CONFIRM_RATE: AtomicU32 = AtomicU32::new(100);

/// (#task-memgate-twosignal) OOM hard-gate SUSTAINED WINDOW: the number of
/// CONSECUTIVE sampler ticks the swapin rate must stay at/above
/// `SWAPIN_CONFIRM_RATE` before the OOM boolean trips. At the 100 ms sampler
/// cadence, the default `10` ticks ≈ 1 s of sustained disk spill — enough to
/// reject a one-off spike (a lone tick touching ancient swapped pages resets the
/// consecutive-tick count to 0) while still leading a real OOM by ~1 s. The
/// count resets to 0 whenever a tick reads below the rate.
///
/// Config-sourced: set ONCE at worker startup from
/// `LocalWorkerConfig::memory_gate_swapin_confirm_window_ticks` (default `10`).
/// `NonZeroU32` in config (0 = no window = trip on the first tick = one-off
/// spikes trip, defeating the whole point).
static SWAPIN_CONFIRM_WINDOW_TICKS: AtomicU32 = AtomicU32::new(10);

/// (#37 re-enable follow-up) Master enable for the worker-local memory-pressure
/// admission gate and its proactive heartbeat boolean. Set ONCE at worker
/// startup from `LocalWorkerConfig::memory_gate_enabled` (default `false`).
/// Runtime config-flag so a single-worker canary soak is possible without
/// a rebuild: set `memory_gate_enabled: true` in the canary's individualized
/// `worker.json5`; flip the rest only after the soak passes.
///
/// When enabled: the gate NAKs new `StartAction`s under sustained pressure
/// (available below `FREE_FLOOR_BYTES` OR SWAPIN rate sustained above
/// `SWAPIN_CONFIRM_RATE` for `SWAPIN_CONFIRM_WINDOW_TICKS` ticks) and the
/// matcher proactively skips a pressured worker; the §3a sampler-dead fail-open
/// and the §5 fleet fail-open keep a dead sampler or an all-pressured fleet from
/// wedging. The sampler always publishes the churn scalar + swap-used for
/// observability regardless of this flag.
///
/// DEFAULT: `false` — DISABLED, zero production behavior change. The #64
/// incident (`5132d6c9`, 2026-06-24) disabled the gate because the old raw
/// `free_count` floor (now corrected to `available` = free+inactive+purgeable)
/// false-tripped fleet-wide (3.6k-NAK/min storm). The corrected formula is
/// landed here; re-enable requires a per-worker canary soak.
///
/// ROLLOUT: deploy binary fleet-wide FIRST (this static defaults false, no
/// config field needed). Then add `memory_gate_enabled: true` to ONE
/// worker's individualized config and observe for ≥ a peak concurrent build
/// cycle. `deny_unknown_fields` on `LocalWorkerConfig` means old binaries
/// reject configs containing this field — two-phase deploy is mandatory.
static MEMORY_GATE_ENABLED: AtomicBool = AtomicBool::new(false);

/// (#37) EWMA smoothing weight applied to each fresh re-fault-rate sample
/// (fast-ATTACK). A high weight on RISING samples means a post-action
/// peak (§3b) trips the estimate quickly; the slow-release below keeps it
/// from sticking. `estimate += ATTACK * (sample - estimate)` when sample
/// rises.
const SWAP_EWMA_ATTACK: f64 = 0.5;

/// (#37) EWMA smoothing weight applied when the fresh sample is BELOW the
/// running estimate (slow-RELEASE). Small weight ⇒ the estimate decays
/// slowly, so a single completed heavy action whose post-action peak
/// tripped the gate cannot trip-then-immediately-clear-then-retrip
/// (security S-MED-2: the release MUST outlast the post-action RSS-reclaim
/// time).
const SWAP_EWMA_RELEASE: f64 = 0.05;

/// (#37) Time-bounded fleet fail-open window (§5 case 3b). A worker that
/// has been NAKing new work under memory pressure with NO in-flight
/// actions for longer than this accepts ONE action regardless of pressure,
/// so an all-idle all-pressured fleet (e.g. a memory leak unrelated to
/// actions — pressure never decays because nothing completes) cannot
/// deadlock. This is the load-bearing anti-wedge clause: it works with a
/// stale server view and needs no fleet-global state. MUST exceed the
/// typical transient-pressure duration so it does not defeat the gate on
/// ordinary spikes.
const SWAP_FAIL_OPEN_AFTER: Duration = Duration::from_secs(30);

/// (#37) Max age of the most recent memory sample before the gate treats
/// its state as UNKNOWN and fails OPEN (§3a rule 2 / §5 case 4). MUST be
/// strictly GREATER than the worst-case legitimate sampler stall (a GC /
/// scheduler-starvation pause under the very pressure being measured) and
/// strictly LESS than the time for an un-gated worker to OOM (security
/// S-MED-1). At the 100 ms sampler cadence, 2 s = 20 missed ticks.
const SWAP_SAMPLE_MAX_AGE: Duration = Duration::from_secs(2);

/// Starts a dedicated OS thread that samples system-wide CPU utilization,
/// memory pressure, AND (F4) physical disk-free on the worker's
/// `work_directory` volume every 100ms. Idempotent — only the first call spawns
/// the thread. `work_directory` is the path the disk sampler `statvfs`'s (it
/// shares one physical volume with the CAS `content_path` by config invariant);
/// it is set ONCE into `DISK_SAMPLE_PATH` (a bad/non-NUL path leaves the disk
/// sampler inert → the gate's statvfs fallback covers it).
fn start_cpu_sampler(work_directory: &str) -> Result<(), Error> {
    if SAMPLER_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        return Ok(());
    }
    // (F4) Record the disk-sampler target path ONCE. A path containing an
    // interior NUL cannot become a CString → `None` → the disk sampler no-ops.
    let _ = DISK_SAMPLE_PATH.set(std::ffi::CString::new(work_directory).ok());
    std::thread::Builder::new()
        .name("cpu-sampler".into())
        .spawn(cpu_sample_loop)
        .map_err(|e| {
            make_err!(
                Code::Internal,
                "failed to spawn cpu-sampler thread: {:?}",
                e
            )
        })?;
    Ok(())
}

fn compute_pct(prev: &cpu_impl::CpuTimes, curr: &cpu_impl::CpuTimes) -> u32 {
    let total_delta = curr.total.wrapping_sub(prev.total);
    let busy_delta = curr.busy.wrapping_sub(prev.busy);
    if total_delta > 0 {
        ((busy_delta as f64 / total_delta as f64) * 100.0).round() as u32
    } else {
        0
    }
}

/// Derive the swap-pressure RATE (events/sec) from two cumulative-counter
/// samples and the wall-clock interval between them.
///
/// The cumulative swap-pressure counter is monotonic until reboot. A
/// counter reset (reboot mid-process, or a kernel that wrapped) shows up
/// as `curr < prev`; we clamp that to `0` rather than report a garbage
/// spike. A non-positive `elapsed_secs` (clock didn't advance) also
/// yields `0`. The result saturates into `u32` so a pathological burst
/// can't overflow the wire field.
fn compute_swap_pressure_rate(prev_count: u64, curr_count: u64, elapsed_secs: f64) -> u32 {
    if elapsed_secs <= 0.0 {
        return 0;
    }
    // saturating_sub clamps a counter reset (curr < prev) to 0.
    let delta = curr_count.saturating_sub(prev_count);
    let rate = (delta as f64 / elapsed_secs).round();
    if rate <= 0.0 {
        0
    } else if rate >= f64::from(u32::MAX) {
        u32::MAX
    } else {
        rate as u32
    }
}

/// Update the fast-attack / slow-release EWMA of the swap-pressure rate
/// (§3b). RISING samples are folded in with the large `SWAP_EWMA_ATTACK`
/// weight (a post-action peak trips the estimate fast); FALLING samples
/// use the small `SWAP_EWMA_RELEASE` weight (the estimate decays slowly,
/// so a one-action spike cannot retrip on the next tick — the steady-state
/// oscillation guard). Pure function of (prev_estimate, sample) so it is
/// unit-testable in isolation.
fn update_swap_ewma(prev_estimate: f64, sample: f64) -> f64 {
    let weight = if sample >= prev_estimate {
        SWAP_EWMA_ATTACK
    } else {
        SWAP_EWMA_RELEASE
    };
    prev_estimate + weight * (sample - prev_estimate)
}

/// (#37 rev-4) PRIMARY free-floor trip with a two-threshold hysteresis
/// band (design §0-rev4.4 — the free-floor is a LEVEL, so it cannot use the
/// re-fault EWMA; a band keeps it from chattering at the boundary).
///
/// - Trip when `free_bytes < FREE_FLOOR_BYTES`.
/// - Once tripped, stay tripped until free recovers ABOVE
///   `FREE_FLOOR_BYTES + FREE_FLOOR_HYSTERESIS` (so a worker hovering right
///   at the floor does not flap admit/refuse every tick).
/// - In the band (between the two thresholds), HOLD the prior state.
///
/// Pure function of `(free_bytes, currently_tripped)` so it is unit-testable
/// in isolation. The caller threads `currently_tripped` across ticks.
const fn free_floor_breached(free_bytes: u64, currently_tripped: bool) -> bool {
    if free_bytes < FREE_FLOOR_BYTES {
        true
    } else if free_bytes >= FREE_FLOOR_BYTES.saturating_add(FREE_FLOOR_HYSTERESIS) {
        false
    } else {
        // In the hysteresis band: hold the prior verdict.
        currently_tripped
    }
}

/// (#37 rev-4) The observability/ranking LEVEL scalar (wire field 20):
/// how far free headroom has fallen below `FREE_FLOOR_BYTES`, in MiB. `0`
/// whenever free is at or above the floor. Higher = more pressured, so the
/// server's `min_by_key` least-pressured fail-open ranking stays correct.
/// Saturates into `u32` (a 16 GiB shortfall fits trivially).
fn memory_pressure_level_mib(free_bytes: u64) -> u32 {
    let shortfall = FREE_FLOOR_BYTES.saturating_sub(free_bytes);
    u32::try_from(shortfall >> 20).unwrap_or(u32::MAX)
}

/// (#task-memgate-twosignal) The OOM gate verdict the sampler publishes:
/// pressured when the gate is ENABLED and EITHER the free-floor fail-safe is
/// breached OR the SUSTAINED-SWAPIN OOM signal confirms disk spill (OR logic).
/// The compressor-churn perf scalar does NOT participate — it is perf, not OOM.
/// Held `false` entirely while `enabled` is `false` so the proactive matcher
/// skip never fires on an unproven threshold. Pure function of the three
/// inputs so the OR logic is unit-testable AND so the production sampler and
/// the test bind to the SAME expression (a mutation deleting either disjunct
/// red-fails the test).
const fn memory_gate_verdict(enabled: bool, free_tripped: bool, swapin_sustained: bool) -> bool {
    enabled && (free_tripped || swapin_sustained)
}

/// (#task-memgate-twosignal) The SUSTAINED-WINDOW step for the SWAPIN OOM
/// signal. Returns `(new_consecutive_ticks, tripped)`:
/// - `new_consecutive_ticks` = `prev_ticks + 1` (saturating) when this tick's
///   swapin `rate >= threshold`, else `0` (a lone sub-threshold tick RESETS the
///   run — so a one-off spike touching ancient swapped pages never accumulates).
/// - `tripped` = the OOM signal fires ONLY when the run reaches `window`
///   consecutive at/above-threshold ticks. `window == 0` is guarded upstream
///   (config `NonZeroU32`), so `>= window` is never trivially true.
///
/// Pure → the sustained-window contract is unit-testable without the sampler's
/// atomics/`Instant`. Mutating `>= window` to `>= 1` (fire on the first tick)
/// makes a one-off spike trip → the sustained-window test red-fails.
const fn swapin_sustained_step(
    prev_ticks: u32,
    rate: u32,
    threshold: u32,
    window: u32,
) -> (u32, bool) {
    let ticks = if rate >= threshold {
        prev_ticks.saturating_add(1)
    } else {
        0
    };
    (ticks, ticks >= window)
}

/// (#task-memgate-twosignal, operator insight) The compressor-CHURN perf scalar:
/// `min(compress_rate_ewma, decompress_rate_ewma)`. High ONLY when compression
/// AND decompression are BOTH high (the compressor is thrashing: pages compressed
/// to reclaim RAM AND immediately faulted back = working set exceeds uncompressed
/// RAM = genuine pressure). It FILTERS the two confounds decompression-alone
/// can't: proactive cold-page reclaim (compress-high, decompress-low → low `min`)
/// and a one-time working-set shift (decompress-high, compress-low → low `min`).
/// Both EWMAs are already smoothed (fast-attack/slow-release), so the scalar is
/// damped, not a spike. Pure → unit-testable.
fn churn_scalar(compress_ewma: f64, decompress_ewma: f64) -> f64 {
    compress_ewma.min(decompress_ewma)
}

/// Per-tick memory-sampler state threaded through the sampler loop. `prev` is
/// the last cumulative `(compressions, decompressions, swapins)` counters + the
/// `Instant` they were read (so each rate uses the REAL elapsed interval, robust
/// to sampler jitter, not an assumed 100 ms). `compress_ewma` / `decompress_ewma`
/// are the running fast-attack/slow-release estimates whose `min` is the graded
/// churn scalar. `swapin_sustained_ticks` counts consecutive at/above-threshold
/// swapin ticks for the OOM sustained-window. `free_tripped` holds the free-floor
/// fail-safe hysteresis-band verdict across ticks.
#[derive(Clone, Copy)]
struct SwapSamplerState {
    prev: Option<(u64, u64, u64, Instant)>,
    compress_ewma: f64,
    decompress_ewma: f64,
    swapin_sustained_ticks: u32,
    free_tripped: bool,
}

impl SwapSamplerState {
    const fn new() -> Self {
        Self {
            prev: None,
            compress_ewma: 0.0,
            decompress_ewma: 0.0,
            swapin_sustained_ticks: 0,
            free_tripped: false,
        }
    }
}

/// Sample host memory pressure on the sampler thread's fixed cadence and
/// publish into the atomics the heartbeat + the worker-local gate read.
/// Updates: `SWAP_USED_BYTES` (level gauge, observability),
/// `MEMORY_PRESSURE_LEVEL` (the compressor-CHURN perf scalar, wire/observability
/// + the scheduler's least-pressured ranking + DOWN churn-throttle),
/// `LAST_SAMPLE_INSTANT` (sample-age liveness for the fail-open), and
/// `MEMORY_PRESSURED` (the two-signal OOM verdict). Returns the new
/// `SwapSamplerState` to thread into the next tick.
///
/// (#task-memgate-twosignal) Two-signal trip logic:
/// - OOM boolean `MEMORY_PRESSURED` = free-floor fail-safe breached OR SWAPIN
///   rate sustained above `SWAPIN_CONFIRM_RATE` for `SWAPIN_CONFIRM_WINDOW_TICKS`
///   consecutive ticks. Swapins are the OOM-adjacent disk-spill signal — safe by
///   SEMANTICS (deeper than compression) + the sustained window, not by the
///   threshold value; the busy-worker baseline is unmeasured (watch nak_swapin).
/// - GRADED churn scalar `MEMORY_PRESSURE_LEVEL` = `min(compress_ewma,
///   decompress_ewma)` — high only when the compressor is thrashing. Perf, not
///   OOM: it NEVER hard-NAKs; it feeds the scheduler ranking + throttle.
///
/// All three raw rates (compress/decompress/swapin) are mirrored to the
/// /metrics singleton for calibration (we have no compression-rate soak data
/// yet — this logging is what will let us set the churn-throttle band later).
fn sample_mem_pressure(state: SwapSamplerState) -> SwapSamplerState {
    // swap-used is an absolute gauge — publish whatever we read (0 if
    // unavailable), no prev-state needed. Observability only.
    let swap_used = mem_impl::read_swap_used_bytes().unwrap_or(0);
    SWAP_USED_BYTES.store(swap_used, Ordering::Relaxed);
    // (#64 dark-signals) Mirror to the /metrics singleton. Cost: 1 extra
    // AtomicStore(Relaxed) per ~100ms sampler tick, off the request path.
    nativelink_util::o11_probes::memory_gate_counters()
        .swap_used_bytes
        .store(swap_used, Ordering::Relaxed);

    let now = Instant::now();
    // Publish the sample-age anchor on EVERY successful tick (BEFORE the
    // signal read can early-return) so the gate's fail-open sees a live
    // sampler even on ticks where the signals themselves are unreadable.
    // Stored as monotonic nanos since PROCESS_START — never wall-clock.
    let since_start = now.duration_since(*PROCESS_START).as_nanos();
    LAST_SAMPLE_INSTANT.store(
        u64::try_from(since_start).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );

    let Some(signals) = mem_impl::read_memory_signals() else {
        // Signals unreadable this tick: report no pressure and drop the
        // prev anchor so the next successful read doesn't compute a rate
        // across an unknown-length gap. The EWMA is left intact (it will
        // decay on the next readable tick); the gate verdict and the level
        // scalar are cleared so an unreadable read never holds the gate
        // tripped. (Persistent unreadability is caught by the sample-age
        // fail-open via a NON-updated `LAST_SAMPLE_INSTANT`... but note we
        // DID update the anchor above, so a one-off unreadable tick does
        // not look like a dead sampler — only a sampler that stops ticking
        // entirely trips the age fail-open.)
        MEMORY_PRESSURE_LEVEL.store(0, Ordering::Relaxed);
        // (#64 dark-signals) Signals unreadable → no pressure; mirror 0 so the
        // gauges read 0 on the unreadable path (consistent with the statics).
        let counters = nativelink_util::o11_probes::memory_gate_counters();
        counters.pressure_level_mib.store(0, Ordering::Relaxed);
        counters.churn_ewma.store(0, Ordering::Relaxed);
        counters.compress_rate_last.store(0, Ordering::Relaxed);
        counters.decompress_rate_last.store(0, Ordering::Relaxed);
        counters.swapin_rate_last.store(0, Ordering::Relaxed);
        MEMORY_PRESSURED.store(false, Ordering::Relaxed);
        // Clear per-source trip latches for consistency: an unreadable tick
        // is not a tripped tick, and the NAK-path structured log would be
        // misleading if a previous trip's latch remained set.
        MEMORY_GATE_TRIP_FREE_FLOOR.store(false, Ordering::Relaxed);
        MEMORY_GATE_TRIP_SWAPIN.store(false, Ordering::Relaxed);
        // Drop the prev anchor (rate gap) and RESET the sustained-swapin run (an
        // unreadable tick breaks the consecutive-window streak). Keep the EWMAs
        // (they decay on the next readable tick).
        return SwapSamplerState {
            prev: None,
            compress_ewma: state.compress_ewma,
            decompress_ewma: state.decompress_ewma,
            swapin_sustained_ticks: 0,
            free_tripped: false,
        };
    };

    // FAIL-SAFE (leading, last-ditch): free-page floor, a LEVEL with a
    // hysteresis band. macOS keeps `available` up by compressing, so this is
    // BLIND to compression-hidden overcommit — the swapin OOM signal is the
    // primary; this is the belt-and-braces backstop.
    let free_tripped = free_floor_breached(signals.free_bytes, state.free_tripped);
    // Publish the free-floor shortfall magnitude for observability (NO LONGER the
    // wire scalar — the churn scalar below is; kept as a diagnostic gauge).
    let pressure_level = memory_pressure_level_mib(signals.free_bytes);
    let counters = nativelink_util::o11_probes::memory_gate_counters();
    counters.pressure_level_mib.store(pressure_level, Ordering::Relaxed);

    // Compute the three per-second rates from the cumulative counters over the
    // REAL elapsed interval (first sample: no interval → 0).
    let (compress_rate, decompress_rate, swapin_rate) = if let Some((
        prev_comp,
        prev_decomp,
        prev_swapin,
        prev_at,
    )) = state.prev
    {
        let elapsed = now.duration_since(prev_at).as_secs_f64();
        (
            compute_swap_pressure_rate(prev_comp, signals.compressions_cumulative, elapsed),
            compute_swap_pressure_rate(prev_decomp, signals.decompressions_cumulative, elapsed),
            compute_swap_pressure_rate(prev_swapin, signals.swapins_cumulative, elapsed),
        )
    } else {
        (0, 0, 0)
    };
    // GRADED perf scalar: fold compression + decompression through the existing
    // fast-attack/slow-release EWMA (damping/hysteresis), then take the `min`
    // (compressor-churn — high only when BOTH are high).
    let compress_ewma = update_swap_ewma(state.compress_ewma, f64::from(compress_rate));
    let decompress_ewma = update_swap_ewma(state.decompress_ewma, f64::from(decompress_rate));
    let churn = churn_scalar(compress_ewma, decompress_ewma);
    // `churn.round()` → i64 → saturating u32. Non-negative EWMAs of non-negative
    // rates → the value is in [0, u32::MAX]; observability + ranking only.
    let churn_rounded = u32::try_from(churn.round() as i64).unwrap_or(u32::MAX);
    MEMORY_PRESSURE_LEVEL.store(churn_rounded, Ordering::Relaxed);

    // (#64 dark-signals / #task-memgate-twosignal) Publish the churn scalar + all
    // three raw rates to the process-singleton so they appear on /metrics
    // unconditionally (NOT gated on MEMORY_GATE_ENABLED). The sampler runs
    // regardless of gate state, so these gauges populate on ALL workers and give
    // the calibration data for the churn-throttle band.
    counters.churn_ewma.store(churn_rounded, Ordering::Relaxed);
    counters.compress_rate_last.store(compress_rate, Ordering::Relaxed);
    counters.decompress_rate_last.store(decompress_rate, Ordering::Relaxed);
    counters.swapin_rate_last.store(swapin_rate, Ordering::Relaxed);

    // OOM SUSTAINED-SWAPIN signal: require the swapin rate to stay at/above the
    // threshold for a consecutive-tick window (rejects one-off spikes).
    let (swapin_ticks, swapin_sustained) = swapin_sustained_step(
        state.swapin_sustained_ticks,
        swapin_rate,
        SWAPIN_CONFIRM_RATE.load(Ordering::Relaxed),
        SWAPIN_CONFIRM_WINDOW_TICKS.load(Ordering::Relaxed),
    );

    // OOM verdict: free-floor fail-safe OR sustained swapins (the churn scalar
    // does NOT participate — perf, not OOM).
    let gate_enabled = MEMORY_GATE_ENABLED.load(Ordering::Relaxed);
    let pressured = memory_gate_verdict(gate_enabled, free_tripped, swapin_sustained);
    MEMORY_PRESSURED.store(pressured, Ordering::Relaxed);
    // Publish per-source trip latches for NAK-path structured logging so soak
    // data can distinguish a free-floor NAK from a swapin NAK.
    MEMORY_GATE_TRIP_FREE_FLOOR.store(gate_enabled && free_tripped, Ordering::Relaxed);
    MEMORY_GATE_TRIP_SWAPIN.store(gate_enabled && swapin_sustained, Ordering::Relaxed);

    SwapSamplerState {
        prev: Some((
            signals.compressions_cumulative,
            signals.decompressions_cumulative,
            signals.swapins_cumulative,
            now,
        )),
        compress_ewma,
        decompress_ewma,
        swapin_sustained_ticks: swapin_ticks,
        free_tripped,
    }
}

fn cpu_sample_loop() {
    // Monitoring thread — downgrade to UTILITY QoS so it doesn't
    // compete with real work for P-cores.
    #[cfg(target_os = "macos")]
    {
        const QOS_CLASS_UTILITY: u32 = 0x11;
        unsafe extern "C" {
            fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
        }
        unsafe { pthread_set_qos_class_self_np(QOS_CLASS_UTILITY, 0) };
    }

    // Try per-type sampling first (macOS with host_processor_info).
    #[cfg(target_os = "macos")]
    {
        if let Some(initial) = cpu_impl::read_per_type_cpu_times() {
            per_type_sample_loop(initial);
            return; // unreachable — loop is infinite
        }
    }

    // Fallback: aggregate-only sampling (Linux, non-macOS, or Intel Mac
    // where host_processor_info failed).
    let mut prev = cpu_impl::read_cpu_times();
    let mut mem_state = SwapSamplerState::new();
    let mut disk_tripped = false;
    loop {
        std::thread::sleep(Duration::from_millis(100));
        // Sample host swap pressure on the same fixed cadence as CPU so
        // the pressure rate has a stable denominator (the heartbeat
        // cadence varies 100 ms-6 s and would make the rate noisy).
        mem_state = sample_mem_pressure(mem_state);
        // (F4) Sample physical disk-free on the same cadence (off the hot
        // path — never a per-action statvfs). Threads the hysteresis-band
        // verdict across ticks.
        disk_tripped = sample_disk_pressure(disk_tripped);
        let curr = cpu_impl::read_cpu_times();
        match (&prev, &curr) {
            (Some(p), Some(c)) => {
                CPU_PCT.store(compute_pct(p, c).min(100), Ordering::Relaxed);
            }
            _ => CPU_PCT.store(0, Ordering::Relaxed),
        }
        prev = curr;
    }
}

#[cfg(target_os = "macos")]
fn per_type_sample_loop(initial: cpu_impl::PerTypeCpuTimes) {
    let mut prev = initial;
    let mut mem_state = SwapSamplerState::new();
    let mut disk_tripped = false;
    loop {
        std::thread::sleep(Duration::from_millis(100));
        // Sample host swap pressure FIRST so it keeps publishing even on
        // ticks where the CPU read fails and `continue`s below.
        mem_state = sample_mem_pressure(mem_state);
        // (F4) Sample physical disk-free on the same cadence, BEFORE the CPU
        // read can `continue`, so it keeps publishing on CPU-read failures.
        disk_tripped = sample_disk_pressure(disk_tripped);
        let Some(curr) = cpu_impl::read_per_type_cpu_times() else {
            CPU_PCT.store(0, Ordering::Relaxed);
            P_CORE_PCT.store(0, Ordering::Relaxed);
            E_CORE_PCT.store(0, Ordering::Relaxed);
            continue;
        };
        CPU_PCT.store(
            compute_pct(&prev.aggregate, &curr.aggregate).min(100),
            Ordering::Relaxed,
        );
        P_CORE_PCT.store(
            compute_pct(&prev.p_core, &curr.p_core).min(100),
            Ordering::Relaxed,
        );
        if curr.has_e_cores {
            E_CORE_PCT.store(
                compute_pct(&prev.e_core, &curr.e_core).min(100),
                Ordering::Relaxed,
            );
        } else {
            // No E-cores → report as fully saturated so scheduler
            // doesn't think idle E-cores are available.
            E_CORE_PCT.store(100, Ordering::Relaxed);
        }
        prev = curr;
    }
}

/// Returns the current system-wide CPU utilization as a percentage (0-100),
/// sampled every 100ms by a dedicated OS thread.
fn get_cpu_load_pct() -> u32 {
    CPU_PCT.load(Ordering::Relaxed)
}

/// Returns the P-core CPU utilization (0-100). 0 means unknown (Linux or
/// non-heterogeneous CPU where per-core-type data is unavailable).
fn get_p_core_load_pct() -> u32 {
    P_CORE_PCT.load(Ordering::Relaxed)
}

/// Returns the E-core CPU utilization (0-100). 0 means unknown.
/// 100 on CPUs without E-cores (all cores are P-cores).
fn get_e_core_load_pct() -> u32 {
    E_CORE_PCT.load(Ordering::Relaxed)
}

/// (#obs-tuning-construct-latency-conditioning) OBSERVABILITY-ONLY. DECAYED p95
/// of the worker's COLD
/// dir-cache construct latency, read from the process-global `DirCacheCounters`
/// `construct_fetch_p95` estimator (`o11_probes.rs`). Fed from the same
/// `record_construct_fetch_ms` observation as the cumulative `construct_fetch_ms`
/// sum+count, but conditioned: an exponentially time-decayed fixed-bucket
/// histogram whose p95 tracks the RECENT cold-construct regime (not a since-boot
/// fossil) and biases toward the expensive tail `T_SETUP` must not under-price.
/// The COLD full fetch+assemble span is recorded in `directory_cache.rs`
/// (`record_construct_fetch_ms`) — the real cold-tree reconstruct cost `T_SETUP`
/// should eventually equal. Returns `0` when no cold constructs have been
/// observed yet (empty histogram). Gossiped on the `BlobsAvailable` chunk-0
/// header so the scheduler can LOG it for `T_SETUP` tuning; NOT consumed by any
/// routing decision. Cheap: one mutex acquire + a fixed bucket walk, no await.
/// Saturating cast to `u32` (a p95 latency in ms fits `u32` for any realistic
/// construct; saturates rather than wraps defensively).
fn get_construct_latency_ms_p95() -> u32 {
    let counters = nativelink_util::o11_probes::dir_cache_counters();
    u32::try_from(counters.construct_fetch_p95.p95_ms()).unwrap_or(u32::MAX)
}

/// Returns host swap-used bytes sampled by the dedicated sampler thread.
/// `0` means no swap in use OR sampler unavailable. Absolute LEVEL gauge
/// (observability) — pair with [`get_memory_pressure_level`] (the
/// free-floor-shortfall magnitude behind the gate verdict).
fn get_swap_used_bytes() -> u64 {
    SWAP_USED_BYTES.load(Ordering::Relaxed)
}

/// (#task-memgate-twosignal) Returns the worker GRADED perf scalar — the
/// compressor-CHURN EWMA (`min(compress_rate_ewma, decompress_rate_ewma)`,
/// events/sec), refreshed by the sampler thread. `0` ⇒ the compressor is not
/// thrashing (healthy) OR sampler unavailable; higher ⇒ deeper churn. This is
/// the wire-field-20 scalar carried for observability + the server's
/// least-pressured fail-open ranking + the DOWN churn-throttle; the OOM GATE
/// verdict is [`swap_gate_pressured`] (free-floor OR sustained swapins, with a
/// sample-age fail-open) — the churn scalar does NOT hard-NAK.
fn get_memory_pressure_level() -> u32 {
    MEMORY_PRESSURE_LEVEL.load(Ordering::Relaxed)
}

/// (#37) Pure memory-gate verdict — the sample-age fail-open logic, factored
/// out of [`swap_gate_pressured`] so it is unit-testable with `enabled =
/// true` independent of the `MEMORY_GATE_ENABLED` runtime flag.
///
/// Returns `(true, _)` only when the gate is enabled, the most recent
/// sample is FRESH, and the sampler published a pressured verdict. FAILS
/// OPEN (`false`) when disabled, never-sampled (`last_nanos == 0`), or
/// STALE (`age > SWAP_SAMPLE_MAX_AGE`). `stale` is returned separately so
/// the caller can emit the `warn!` only on the real wedged-sampler path
/// (this function stays allocation/log-free for testability). All time
/// inputs are monotonic nanos since `PROCESS_START`.
fn swap_gate_verdict(
    enabled: bool,
    pressured: bool,
    last_nanos: u64,
    now_since_start: Duration,
    max_age: Duration,
) -> (bool /* pressured */, bool /* stale */) {
    if !enabled {
        return (false, false);
    }
    if last_nanos == 0 {
        // Sampler has never published a value — UNKNOWN, fail open.
        return (false, false);
    }
    let age = now_since_start.saturating_sub(Duration::from_nanos(last_nanos));
    if age > max_age {
        // Stale sample: the sampler is wedged/dead. UNKNOWN → fail open.
        return (false, true);
    }
    (pressured, false)
}

/// (#37) Authoritative worker-local memory-gate decision. Returns `true`
/// only when the gate is ENABLED, the most recent memory sample is FRESH
/// (within `SWAP_SAMPLE_MAX_AGE`), and the sampler published a pressured
/// verdict (free-floor breached OR re-fault EWMA over threshold). FAILS
/// OPEN — returns `false` — when the sampler is wedged/dead (stale or
/// never-published `LAST_SAMPLE_INSTANT`) so a dead pressure sampler never
/// wedges the worker into refusing all work (§3a rule 2 / §5 case 4). Reads
/// ONLY in-process atomics on a monotonic clock; never the wire boolean
/// (the safety-critical decision never crosses the worker→server trust
/// boundary, design §2 S-LOW-2).
fn swap_gate_pressured() -> bool {
    let (pressured, stale) = swap_gate_verdict(
        MEMORY_GATE_ENABLED.load(Ordering::Relaxed),
        MEMORY_PRESSURED.load(Ordering::Relaxed),
        LAST_SAMPLE_INSTANT.load(Ordering::Relaxed),
        Instant::now().duration_since(*PROCESS_START),
        SWAP_SAMPLE_MAX_AGE,
    );
    if stale {
        // The count-only gate + memory_kb admission remain the only bounds
        // (both still active) rather than gating all work forever.
        warn!(
            "stale memory sample: memory-pressure sampler appears wedged/dead, failing the gate OPEN"
        );
    }
    pressured
}

/// (#37) The three outcomes of the worker-local swap-gate evaluation on a
/// `StartAction`. Pure-function output of [`swap_gate_decision`] so the
/// admission/fail-open/hysteresis logic is unit-testable without the full
/// `LocalWorker::run` loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwapGateDecision {
    /// Accept the action normally (not pressured, or hysteresis latch
    /// already holds — a prior fail-open action is still draining).
    Accept,
    /// Accept via the time-bounded fleet fail-open (§5 case 3b): the
    /// worker has been idle-gating past `SWAP_FAIL_OPEN_AFTER`. Caller
    /// MUST set the hysteresis latch so re-gating is suppressed until the
    /// accepted action completes.
    AcceptFailOpen,
    /// Refuse the action with `ResourceExhausted` (sustained swap pressure
    /// with either in-flight work draining or before the fail-open window).
    Nak,
}

/// Pure swap-gate admission decision (§ Option A + §5 case 3b + hysteresis).
///
/// - Not pressured ⇒ `Accept`.
/// - Hysteresis `latched` (a fail-open action is still in flight) ⇒
///   `Accept` — suppress re-gating until it completes (monotonic progress,
///   security S §5-3b).
/// - Pressured with in-flight work ⇒ `Nak`: the worker is draining, and
///   `update_action_cs2`'s `has_actions()` pause re-selects it correctly,
///   so refusing new work here does NOT arm the idle spin.
/// - Pressured + IDLE (no in-flight work): `Nak` until the worker has been
///   idle-gating for `SWAP_FAIL_OPEN_AFTER`, then `AcceptFailOpen` — so an
///   all-idle all-pressured fleet cannot deadlock (the load-bearing
///   anti-wedge clause). `first_idle_gated_at` is the anchor the caller
///   threads across ticks.
fn swap_gate_decision(
    pressured: bool,
    in_flight: u64,
    latched: bool,
    first_idle_gated_at: Option<Instant>,
    now: Instant,
) -> SwapGateDecision {
    if !pressured || latched {
        return SwapGateDecision::Accept;
    }
    if in_flight > 0 {
        // Draining: refuse new work; the server-side has_actions() pause
        // (update_action_cs2) handles re-selection without spinning.
        return SwapGateDecision::Nak;
    }
    // Pressured AND idle: the time-bounded fail-open is the only thing that
    // un-wedges an all-idle all-pressured fleet.
    match first_idle_gated_at {
        Some(since) if now.duration_since(since) >= SWAP_FAIL_OPEN_AFTER => {
            SwapGateDecision::AcceptFailOpen
        }
        _ => SwapGateDecision::Nak,
    }
}

// ─────────────────────────── F4 disk-pressure gate ───────────────────────────

/// (F4) PRIMARY disk-free floor trip with a two-threshold hysteresis band
/// (mirrors `free_floor_breached`): the disk gate trips when free bytes on the
/// CAS/work_directory volume fall below `DISK_FREE_FLOOR_BYTES`.
///
/// - Trip when `free_bytes < DISK_FREE_FLOOR_BYTES`.
/// - Once tripped, stay tripped until free recovers ABOVE
///   `DISK_FREE_FLOOR_BYTES + DISK_FREE_FLOOR_HYSTERESIS` (so a worker hovering
///   at the floor — eviction freeing then ingest refilling — does not flap
///   admit/refuse every tick).
/// - In the band, HOLD the prior verdict.
///
/// Pure function of `(free_bytes, currently_tripped)` so it is unit-testable in
/// isolation. The caller threads `currently_tripped` across sampler ticks.
const fn disk_floor_breached(free_bytes: u64, currently_tripped: bool) -> bool {
    if free_bytes < DISK_FREE_FLOOR_BYTES {
        true
    } else if free_bytes >= DISK_FREE_FLOOR_BYTES.saturating_add(DISK_FREE_FLOOR_HYSTERESIS) {
        false
    } else {
        // In the hysteresis band: hold the prior verdict.
        currently_tripped
    }
}

/// (F4) Disk sample-age verdict: reports the sampler's pressure value AND
/// whether the most recent sample is STALE. UNLIKE the swap gate (which
/// blind-fails-open on stale), `stale` here is a signal that the caller MUST
/// consult the authoritative `statvfs` fallback (SEC-2: disk has no other live
/// bound behind the gate, so a blind fail-open on a stale sampler re-opens the
/// ENOSPC the gate prevents). Returns `(pressured, stale)`:
/// - disabled ⇒ `(false, false)` (never gate; no fallback).
/// - never-sampled (`last_nanos == 0`) ⇒ `(false, true)` (route to fallback).
/// - stale (`age > max_age`) ⇒ `(false, true)` (route to fallback).
/// - fresh ⇒ `(pressured, false)` (trust the live sampler verdict).
///
/// Pure function of the time inputs (monotonic nanos since `PROCESS_START`) so
/// it is unit-testable independent of the `DISK_GATE_ENABLED` ship flag.
fn disk_gate_verdict(
    enabled: bool,
    pressured: bool,
    last_nanos: u64,
    now_since_start: Duration,
    max_age: Duration,
) -> (bool /* pressured */, bool /* stale */) {
    if !enabled {
        return (false, false);
    }
    if last_nanos == 0 {
        // Never published a value — UNKNOWN → consult the statvfs fallback.
        return (false, true);
    }
    let age = now_since_start.saturating_sub(Duration::from_nanos(last_nanos));
    if age > max_age {
        // Stale sample: the sampler is wedged/dead — UNKNOWN → statvfs fallback.
        return (false, true);
    }
    (pressured, false)
}

/// (F4) Resolve the EFFECTIVE disk-pressure verdict, folding in the SEC-2
/// authoritative `statvfs` fallback on a stale sampler. This is the load-bearing
/// divergence from the swap gate:
///
/// - sampler FRESH (`!stale`) ⇒ trust the live sampler `pressured` value
///   (`authoritative_free` is ignored).
/// - sampler STALE ⇒ consult the one-shot authoritative `statvfs`:
///   - `Some(free)` below the floor ⇒ effective-pressured (still reject — do NOT
///     admit into a genuinely-full disk).
///   - `Some(free)` at/above the floor ⇒ not pressured (measured headroom).
///   - `None` (statvfs ITSELF failed) ⇒ not pressured = LAST-RESORT blind
///     fail-open: with no measurement at all, refusing all work would wedge the
///     worker, so admit (the only unmeasurable corner, mirroring the swap gate).
///
/// Pure function of `(pressured, stale, authoritative_free)` so the SEC-2 logic
/// is unit-testable without the syscall.
fn disk_effective_pressured(pressured: bool, stale: bool, authoritative_free: Option<u64>) -> bool {
    if !stale {
        return pressured;
    }
    match authoritative_free {
        Some(free) => free < DISK_FREE_FLOOR_BYTES,
        None => false,
    }
}

/// (F4) The three outcomes of the worker-local disk-gate evaluation on a
/// `StartAction`. Pure-function output of [`disk_gate_decision`] so the
/// admission / fleet-fail-open / hysteresis logic is unit-testable without the
/// full `LocalWorker::run` loop. Shape mirrors [`SwapGateDecision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskGateDecision {
    /// Accept the action normally (not pressured, or hysteresis latch holds).
    Accept,
    /// Accept via the time-bounded fleet fail-open (§5 case 3b): the worker has
    /// been idle-gating past `DISK_FAIL_OPEN_AFTER`. Caller MUST set the
    /// hysteresis latch so re-gating is suppressed until the action completes.
    AcceptFailOpen,
    /// Refuse the action with `ResourceExhausted` (sustained disk pressure with
    /// either in-flight work draining or before the fail-open window).
    Nak,
}

/// (F4) Pure disk-gate admission decision. `effective_pressured` already folds
/// in the stale→statvfs fallback ([`disk_effective_pressured`]); from there the
/// logic is byte-identical to [`swap_gate_decision`]:
///
/// - Not pressured ⇒ `Accept`.
/// - Hysteresis `latched` (a fail-open action is still in flight) ⇒ `Accept`.
/// - Pressured with in-flight work ⇒ `Nak` (the worker is draining; the
///   server-side `has_actions()` pause re-selects it correctly).
/// - Pressured + IDLE ⇒ `Nak` until idle-gated for `DISK_FAIL_OPEN_AFTER`, then
///   `AcceptFailOpen` — so an all-idle all-disk-pressured fleet cannot deadlock.
fn disk_gate_decision(
    effective_pressured: bool,
    in_flight: u64,
    latched: bool,
    first_idle_gated_at: Option<Instant>,
    now: Instant,
) -> DiskGateDecision {
    if !effective_pressured || latched {
        return DiskGateDecision::Accept;
    }
    if in_flight > 0 {
        return DiskGateDecision::Nak;
    }
    match first_idle_gated_at {
        Some(since) if now.duration_since(since) >= DISK_FAIL_OPEN_AFTER => {
            DiskGateDecision::AcceptFailOpen
        }
        _ => DiskGateDecision::Nak,
    }
}

/// (F4) Returns the worker's last-sampled free bytes on the CAS/work_directory
/// volume. `u64::MAX` sentinel (never-sampled) is mapped to `0` on the wire so
/// the server's most-free ranking treats an un-sampled worker as fully
/// pressured (conservative for the fail-open ranking; the gate itself fails open
/// pre-first-sample via the statvfs fallback, a separate path). Refreshed by the
/// sampler thread every 100 ms.
fn get_available_disk_bytes() -> u64 {
    match DISK_FREE_BYTES.load(Ordering::Relaxed) {
        u64::MAX => 0,
        v => v,
    }
}

/// (F4) The SAMPLER's coarse disk-pressure verdict (the wire boolean / advisory
/// matcher hint). This is NOT the authoritative gate — it does not include the
/// statvfs stale-fallback (that runs only on the StartAction path, where a
/// `spawn_blocking` syscall is acceptable). `false` whenever the sampler is
/// disabled, never-sampled, or fresh-and-healthy.
fn disk_gate_sampler_pressured() -> bool {
    DISK_GATE_ENABLED && DISK_PRESSURED.load(Ordering::Relaxed)
}

/// Build the advertised gRPC endpoint for peer blob sharing.
/// Uses the machine's hostname so a single config works across all workers.
/// The hostname is resolved once and cached for the lifetime of the process.
/// When `use_tls` is true, advertises `grpcs://` so the server connects with TLS.
fn cas_advertised_endpoint(port: u16, use_tls: bool) -> String {
    use std::sync::OnceLock;
    static HOSTNAME: OnceLock<String> = OnceLock::new();
    let hostname = HOSTNAME.get_or_init(|| {
        match hostname::get() {
            Ok(h) => {
                let name = h.to_string_lossy().into_owned();
                // Append .local for mDNS resolution if the hostname is bare
                // (no dots), so the server can resolve it via multicast DNS.
                if name.contains('.') {
                    name
                } else {
                    format!("{name}.local")
                }
            }
            Err(err) => {
                error!(
                    ?err,
                    "hostname::get() failed, using 'localhost' — peer blob sharing will not work across machines"
                );
                "localhost".to_string()
            }
        }
    });
    let scheme = if use_tls { "grpcs" } else { "grpc" };
    format!("{scheme}://{hostname}:{port}")
}

/// Build the worker's CAS-listener `Routes` from the three concrete
/// service implementations. This is the SHARED production-composition
/// assembly used by both the TCP and QUIC paths below — and by the
/// integration test `worker_cas_listener_ac_mount_test.rs`. Centralising
/// it ensures the test cannot drift from production by skipping a
/// service (testing-czar MAJOR-1, #463 fix-up).
///
/// `max_decoding_message_size` and `max_encoding_message_size` are
/// applied uniformly to every service (matching the original inline
/// production code at the call site).
pub fn build_cas_router(
    cas_server: nativelink_service::cas_server::CasServer,
    bytestream_server: nativelink_service::bytestream_server::ByteStreamServer,
    ac_server: Option<nativelink_service::ac_server::AcServer>,
    max_decoding_message_size: usize,
    max_encoding_message_size: usize,
) -> tonic::service::Routes {
    let cas_svc = cas_server
        .into_service()
        .max_decoding_message_size(max_decoding_message_size)
        .max_encoding_message_size(max_encoding_message_size);
    let bs_svc = bytestream_server
        .into_service()
        .max_decoding_message_size(max_decoding_message_size)
        .max_encoding_message_size(max_encoding_message_size);
    let mut routes = tonic::service::Routes::new(cas_svc).add_service(bs_svc);
    if let Some(ac_server) = ac_server {
        let ac_svc = ac_server
            .into_service()
            .max_decoding_message_size(max_decoding_message_size)
            .max_encoding_message_size(max_encoding_message_size);
        routes = routes.add_service(ac_svc);
    }
    routes
}

/// `google.rpc.PreconditionFailure` type URL — the structural detail
/// `running_actions_manager` attaches to input-fetch and command-fetch
/// NotFound errors (`make_precondition_failure_any`). The presence of this
/// detail is the load-bearing signal that the NotFound IS a CAS-blob-miss
/// the client can fix by re-uploading (REAPI v2 §2.2.4); message
/// substrings on the inner error chain are not.
const PRECONDITION_FAILURE_TYPE_URL: &str =
    "type.googleapis.com/google.rpc.PreconditionFailure";

/// Decide whether a `prepare_action`/`execute`/`upload_results` error
/// represents a CAS blob miss (missing input or command) eligible for
/// REAPI v2 §2.2.4 `Code::NotFound` → `Code::FailedPrecondition`
/// translation. Translation drives two downstream behaviours:
///   1. Bazel re-uploads the missing blob (client-recovery path).
///   2. `simple_scheduler_state_manager.rs:836` marks the action terminal
///      on attempt 1 instead of re-queueing it up to `max_job_retries`
///      (=3) times — closing the 51-deep `Queued`-tail stall observed in
///      production at 2026-05-11 20:10:34 PDT (pid 2980804). See
///      `.claude/audits/410-scheduler-stall-investigation-20260512.md`.
///
/// The predicate fires on EITHER of two signals:
///   * **Structural (preferred):** the error carries a
///     `google.rpc.PreconditionFailure` detail. The detail is attached
///     ONLY at the two REAPI-mandated sites (input fetch at
///     `running_actions_manager.rs:1828` and command fetch at `:2735`)
///     so its presence is a positive identifier of "CAS blob missing
///     for this action". Robust to upstream error-message wording
///     changes — the audit's load-bearing wedge surface.
///   * **Substring (legacy, retained for compat):** the chained error
///     message contains `"not found in"`. Pre-#428 production behaviour;
///     preserved so prepare_action callers that *don't* attach a detail
///     yet still translate. The Chesterton's-Fence intent of the
///     original commit `5b0cb9e8` was to AVOID translating non-CAS
///     NotFounds (missing binary, missing output file); both alternatives
///     here uphold that intent because neither attaches a PF detail and
///     neither produces "not found in" in their message.
///
/// Falsification: remove the `has_pf_detail` arm and run
/// `not_found_with_precondition_detail_translates_without_substring`
/// in `local_worker_test.rs` — it must red-fail with the bespoke
/// "input-fetch NotFound with PreconditionFailure detail must translate
/// to FailedPrecondition" assertion message.
fn is_cas_blob_miss(err: &Error) -> bool {
    if err.code != Code::NotFound {
        return false;
    }
    let has_pf_detail = err
        .details
        .iter()
        .any(|d| d.type_url == PRECONDITION_FAILURE_TYPE_URL);
    if has_pf_detail {
        return true;
    }
    // Legacy substring fallback. Match against the joined message
    // chain (`err_tip`-pushed tips are visible here) rather than the
    // `{e:?}` Debug rendering used pre-#428, so we don't rely on Debug
    // including a specific field.
    err.message_string().contains("not found in")
}

/// Start a QUIC/H3 server for the worker CAS, alongside the TCP server.
///
/// Generates a self-signed TLS certificate at startup (QUIC mandates TLS 1.3)
/// and binds a UDP socket on the same port as the TCP server. Peer workers
/// connecting with `use_http3: true` will use this QUIC endpoint for blob
/// fetches, benefiting from QUIC's built-in stream multiplexing.
#[cfg(feature = "quic")]
fn start_worker_quic_server(
    port: u16,
    worker_name: &str,
    routes: tonic::service::Routes,
) -> Result<JoinHandleDropGuard<Result<(), Error>>, Error> {
    use std::sync::Arc;

    use h3_quinn as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    // Generate self-signed certificate for this worker.
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), worker_name.to_string()])
            .map_err(|e| make_err!(Code::Internal, "Failed to generate self-signed cert: {e:?}"))?;

    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));

    let mut tls_config = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .map_err(|e| make_err!(Code::Internal, "Worker QUIC TLS version error: {e:?}"))?
    .with_no_client_auth()
    .with_single_cert(vec![cert_der], key_der)
    .map_err(|e| make_err!(Code::Internal, "Worker QUIC TLS config error: {e:?}"))?;
    tls_config.alpn_protocols = vec![b"h3".to_vec()];
    tls_config.max_early_data_size = u32::MAX;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
            .map_err(|e| make_err!(Code::Internal, "Worker Quinn server config error: {e:?}"))?,
    ));

    // Tune QUIC transport for LAN usage.
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window((16 * 1024 * 1024u32).into());
    transport.receive_window((128 * 1024 * 1024u32).into());
    transport.send_window(128 * 1024 * 1024);
    transport.max_concurrent_bidi_streams(1024u32.into());
    transport.max_concurrent_uni_streams(1024u32.into());
    transport.initial_rtt(Duration::from_micros(500));
    // Match server/client idle timeout for consistent behavior.
    transport.max_idle_timeout(Some(Duration::from_secs(60).try_into().unwrap()));
    // Send QUIC keepalives every 5s to detect dead connections and
    // prevent NAT/firewall timeouts on the server→worker path.
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    // Enable QUIC MTU discovery for jumbo frames on LAN.
    transport.initial_mtu(1200);
    let mut mtu_config = quinn::MtuDiscoveryConfig::default();
    mtu_config.upper_bound(8952);
    transport.mtu_discovery_config(Some(mtu_config));
    server_config.transport_config(Arc::new(transport));

    // Bind UDP socket with large buffers.
    let socket_addr: std::net::SocketAddr = ([0, 0, 0, 0], port).into();
    let udp_socket = std::net::UdpSocket::bind(socket_addr).map_err(|e| {
        make_err!(
            Code::Internal,
            "Worker QUIC UDP bind on {socket_addr}: {e:?}"
        )
    })?;
    let bufs = nativelink_util::tls_utils::tune_quic_udp_buffers(
        socket2::SockRef::from(&udp_socket),
        "worker_peer",
    );
    nativelink_util::tls_utils::warn_if_quic_udp_buffer_capped(bufs, "worker_peer");

    let quinn_endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server_config),
        udp_socket,
        quinn::default_runtime()
            .ok_or_else(|| make_err!(Code::Internal, "No async runtime for worker QUIC"))?,
    )
    .map_err(|e| {
        make_err!(
            Code::Internal,
            "Failed to create worker QUIC endpoint: {e:?}"
        )
    })?;

    let acceptor = tonic_h3::quinn::H3QuinnAcceptor::new(quinn_endpoint);
    let h3_router = tonic_h3::server::H3Router::new(routes);

    let worker_name = worker_name.to_string();
    info!(
        worker_name = %worker_name,
        %socket_addr,
        "Starting worker CAS QUIC/H3 server for peer blob sharing"
    );

    Ok(spawn!("worker_cas_quic", async move {
        if let Err(err) = h3_router.serve(acceptor).await {
            error!(?err, "Worker CAS QUIC/H3 server error");
            return Err(make_err!(Code::Internal, "Worker CAS QUIC server: {err:?}"));
        }
        Ok(())
    }))
}

/// (FL-688 v3 §3.8) Bounded resend buffer of unacked worker→server
/// `BlobsAvailableChunk` DELTAS, keyed `(broadcast_id, sequence)`.
///
/// The worker buffers each delta chunk it sends and clears the matching
/// slot ONLY when the server's `BlobsAvailableAck` arrives (drain-on-ack).
/// Chunks still unacked when the connection drops are replayed on the
/// next reconnect (symmetric to the scheduler's `BisResendBuffer` for the
/// reverse BIS direction). Without it, a lost worker→server delta send
/// (the tracker is drained on send) leaves the server's locality view
/// permanently stale — the orphaned-replica hole the reconcile /
/// eviction-gate self-heal (v3 §3.4) relies on this buffer to close.
///
/// **Only DELTAS are buffered, never FULL SNAPSHOTS.** A full snapshot is
/// self-correcting: a lost one is re-derived from a whole-store rescan on
/// the next reconnect/tick, so it needs no replay. A delta is a one-shot
/// difference that is NOT re-derived once `tracker.swap()` drained it, so
/// only deltas need the drain-on-ack guarantee. (This narrows the design
/// §3.8 four-site list to the two delta sites — see the impl note at the
/// send path.)
#[derive(Debug, Default)]
pub struct BlobsAvailableResendBuffer {
    /// (broadcast_id, sequence) → the unacked delta chunk.
    chunks: BTreeMap<(u64, u32), BlobsAvailableChunk>,
}

impl BlobsAvailableResendBuffer {
    /// Buffer one just-sent delta chunk. Returns `true` iff this push
    /// would exceed [`BLOBS_AVAILABLE_RESEND_MAX_CHUNKS`]: in that case
    /// the buffer is CLEARED and the caller MUST force a fresh full
    /// snapshot (which supersedes every dropped delta — lossless). The
    /// over-cap chunk itself is NOT inserted (the forced snapshot will
    /// re-advertise it).
    fn add(&mut self, chunk: BlobsAvailableChunk) -> bool {
        if self.chunks.len() >= BLOBS_AVAILABLE_RESEND_MAX_CHUNKS {
            self.chunks.clear();
            return true;
        }
        self.chunks.insert((chunk.broadcast_id, chunk.sequence), chunk);
        false
    }

    /// Drop the chunk matching one ack. PER-CHUNK: only the
    /// `(broadcast_id, sequence)` slot is removed, never the whole
    /// broadcast — an in-window ack must not drop sibling unacked deltas.
    /// An ack for a slot not present is a harmless no-op (idempotent
    /// under a resend that crosses an in-flight ack).
    fn ack(&mut self, broadcast_id: u64, sequence: u32) {
        self.chunks.remove(&(broadcast_id, sequence));
    }

    /// (FL-688 v3 §3.8 drain-on-ack flip) Snapshot every still-unacked
    /// chunk for retransmit. CLONES rather than drains: a chunk stays
    /// buffered until its `BlobsAvailableAck` arrives, so the same chunk
    /// is re-sent every tick until the server confirms it (drain-on-ACK,
    /// not drain-on-SEND). The `(broadcast_id, sequence)` and token are
    /// preserved verbatim so the server's accumulator is idempotent under
    /// the resend and the worker's `ack()` key matches.
    ///
    /// Returned in `(broadcast_id, sequence)` order so a multi-chunk
    /// broadcast is retransmitted with its terminal (`is_last`) chunk
    /// last — the server's accumulator only commits once it sees the
    /// terminal, so out-of-order replay would never assemble.
    fn unacked_chunks(&self) -> Vec<BlobsAvailableChunk> {
        self.chunks.values().cloned().collect()
    }

    fn len(&self) -> usize {
        self.chunks.len()
    }
}

/// (#locality-map-drift) Build a `BlobDigestInfo` carrying the digest plus its
/// `(boot_epoch, counter)` logical-LWW stamp. `Stamp::default()` (0,0) is the
/// unset/legacy sentinel the server treats as oldest.
#[inline]
fn bdi_with_stamp(digest: DigestInfo, stamp: Stamp) -> BlobDigestInfo {
    BlobDigestInfo {
        digest: Some(digest.into()),
        ts_boot_epoch: stamp.boot_epoch,
        ts_counter: stamp.counter,
    }
}

/// (#locality-map-drift) The PRESENT/ABSENT holdings state of a digest in a
/// `BlobChanges` window. A read (`on_get`, `touched`) and an insert
/// (`on_insert`, `added`) both map to `Present`; an eviction (`callback`) maps
/// to `Absent`. Reported to the server as `BlobDigestInfo`
/// (`Present`→`digest_infos`) or `evicted_digests` (`Absent`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobState {
    Present,
    Absent,
}

/// (#locality-map-drift) Accumulated per-digest holdings changes between
/// BlobsAvailable ticks, as a LAST-WRITER-WINS map keyed by digest.
///
/// PRIOR DESIGN (replaced): three disjoint `HashSet`s (`added`/`evicted`/
/// `touched`) whose membership was mutated by set add/remove. That lost the
/// causal order between a SYNC `on_insert` and moka's ASYNC eviction callback:
/// a late evict callback could clobber a re-admitted digest's `added` with
/// `evicted`, producing a persistent false-missing on the server.
///
/// NOW: each digest maps to `(state, Stamp)` where `Stamp = (boot_epoch,
/// counter)` is the per-mutation logical-LWW timestamp minted by the
/// `MokaEvictingMap` (frozen into the value at insert; carried on the evicted
/// value). A callback applies its `(state, stamp)` iff it strictly beats the
/// stored stamp — OR ties it with `Absent` beating `Present` (an evict-of-V is
/// causally after insert-of-V). A stale, out-of-order evict of a superseded
/// value therefore LOSES to the newer insert/read and cannot un-register a
/// held blob. `Present` entries flow to `digest_infos`; `Absent` to
/// `evicted_digests`. Both carry the winning stamp on the wire so the server
/// applies the same LWW.
#[derive(Debug, Default)]
pub struct BlobChanges {
    pub entries: HashMap<DigestInfo, (BlobState, Stamp)>,
}

impl BlobChanges {
    /// Apply `(state, stamp)` for `digest` under local LWW. Returns `true` if
    /// the map was mutated (so the caller wakes the broadcast loop only on a
    /// real transition). LWW: write iff `stamp` strictly newer than stored, OR
    /// (equal ts AND incoming `Absent` while stored `Present`) — ABSENT ≻
    /// PRESENT at equal ts, mirroring the server gate and `HoldingsFixedV2`.
    fn apply(&mut self, digest: DigestInfo, state: BlobState, stamp: Stamp) -> bool {
        match self.entries.get(&digest) {
            Some(&(stored_state, stored_stamp)) => {
                let newer = stamp.gt(stored_stamp);
                let tie_absent_wins = stamp.eq_ts(stored_stamp)
                    && state == BlobState::Absent
                    && stored_state == BlobState::Present;
                if newer || tie_absent_wins {
                    self.entries.insert(digest, (state, stamp));
                    true
                } else {
                    false
                }
            }
            None => {
                self.entries.insert(digest, (state, stamp));
                true
            }
        }
    }
}

/// Tracks inserts, evictions, and reads of the FilesystemStore between ticks.
/// Registered as a callback on the FilesystemStore's evicting map.
///
/// Contains a `Notify` that is signalled on every state transition so
/// the BlobsAvailable send loop can wake immediately instead of polling
/// on a fixed interval.
#[derive(Debug)]
pub struct BlobChangeTracker {
    pending: Mutex<BlobChanges>,
    /// Wakes the BlobsAvailable send loop when changes accumulate.
    notify: Arc<Notify>,
}

impl BlobChangeTracker {
    pub fn new(notify: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(BlobChanges::default()),
            notify,
        })
    }

    /// Atomically swap out accumulated changes as a flat list of
    /// `(digest, state, stamp)`, resetting the internal LWW map. Each entry is
    /// the winning `(state, stamp)` for its digest this window.
    pub fn swap(&self) -> Vec<(DigestInfo, BlobState, Stamp)> {
        let mut pending = self.pending.lock();
        let taken = std::mem::take(&mut *pending);
        taken
            .entries
            .into_iter()
            .map(|(d, (state, stamp))| (d, state, stamp))
            .collect()
    }
}

impl ItemCallback for BlobChangeTracker {
    // On evict: record ABSENT@stamp under LWW. `stamp` is the EVICTED value's
    // FROZEN (boot_epoch, counter) — never a fresh mint — so a re-ordered stale
    // evict of a superseded value loses to the newer insert/read.
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
        ts_boot_epoch: u64,
        ts_counter: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        if let StoreKey::Digest(digest) = store_key {
            let mut pending = self.pending.lock();
            if pending.apply(digest, BlobState::Absent, Stamp::new(ts_boot_epoch, ts_counter)) {
                self.notify.notify_one();
            }
        }
        Box::pin(core::future::ready(()))
    }

    // On insert: record PRESENT@stamp (the value's fresh insert counter) under
    // LWW.
    fn on_insert(&self, store_key: StoreKey<'_>, _size: u64, ts_boot_epoch: u64, ts_counter: u64) {
        if let StoreKey::Digest(digest) = store_key {
            let mut pending = self.pending.lock();
            if pending.apply(digest, BlobState::Present, Stamp::new(ts_boot_epoch, ts_counter)) {
                self.notify.notify_one();
            }
        }
    }

    // On read (cache hit): record PRESENT carrying the RESIDENT VALUE'S FROZEN
    // insert stamp (threaded from `fire_on_get`), NOT a fresh mint — so this
    // delta is idempotent with the value's own insert delta. It STILL supersedes
    // a STALE evict (which carries a PREVIOUS value's older stamp) — re-deriving
    // the old `evicted`-set self-suppression so a blob read every action can't be
    // stranded ABSENT by an earlier eviction — but it does NOT out-rank the
    // value's OWN genuine eviction (same stamp → ABSENT wins the tie). A fresh
    // mint WOULD have gate-killed that genuine eviction (systematic false-
    // positive; TLC HoldingsTouch: A_GateKill VIOLATES NoGateKilledGenuineEvict,
    // B HOLDS 3.43M states).
    fn on_get(&self, store_key: StoreKey<'_>, ts_boot_epoch: u64, ts_counter: u64) {
        if let StoreKey::Digest(digest) = store_key {
            let mut pending = self.pending.lock();
            if pending.apply(digest, BlobState::Present, Stamp::new(ts_boot_epoch, ts_counter)) {
                self.notify.notify_one();
            }
        }
    }
}

/// Amount of time to wait if we have actions in transit before we try to
/// consider an error to have occurred.
const ACTIONS_IN_TRANSIT_TIMEOUT_S: f32 = 10.;

/// If we lose connection to the worker api server we will wait this many seconds
/// before trying to connect.
const CONNECTION_RETRY_DELAY_S: f32 = 0.5;

/// Default endpoint timeout. If this value gets modified the documentation in
/// `cas_server.rs` must also be updated.
const DEFAULT_ENDPOINT_TIMEOUT_S: f32 = 5.;

/// TCP keepalive for the worker→scheduler WorkerApi control-plane
/// connection. Mirrors the data-channel default in `tls_utils::endpoint`
/// (which uses `Duration::from_secs(30)` when `tcp_keepalive_s` is unset,
/// `tls_utils.rs:171-175`). Without this the control plane was built via
/// `tls_utils::endpoint_from`, which sets only `tcp_nodelay` — so a
/// silently half-open scheduler connection (no GOAWAY / no RST) lets an
/// `execution_response` / `complete` / `blobs_available` send hang
/// indefinitely and the bidi stream never errors, so `inner.run` never
/// returns and the reconnect loop (`run`, `:~4450`) never fires.
///
/// `pub` (unlike the two HTTP/2 consts below) ONLY because the keepalive
/// regression test reads it back via tonic's `Endpoint::get_tcp_keepalive`
/// getter — tonic 0.14.5 exposes a getter for TCP keepalive but NONE for the
/// HTTP/2 params, so those two consts stay private and are unit-unverifiable
/// (see `worker_api_endpoint_keepalive_test.rs`). Do NOT widen the HTTP/2
/// consts to `pub` to "match" — there is no test that can read them.
pub const WORKER_API_TCP_KEEPALIVE: Duration = Duration::from_secs(30);

/// HTTP/2 keepalive ping interval for the WorkerApi control-plane
/// connection. Mirrors the data-channel default in `tls_utils::endpoint`
/// (`http2_keepalive_interval` falls back to `Duration::from_secs(30)`,
/// `tls_utils.rs:176-180`). The HTTP/2 ping detects a half-open
/// connection at the stream layer even when TCP keepalive has not yet
/// fired.
const WORKER_API_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// HTTP/2 keepalive ping timeout for the WorkerApi control-plane
/// connection. Mirrors the data-channel default in `tls_utils::endpoint`
/// (`http2_keepalive_timeout` falls back to `Duration::from_secs(20)`,
/// `tls_utils.rs:181-185`). If a keepalive ping is unacked within this
/// window the connection is declared dead and the bidi stream errors.
const WORKER_API_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Build the worker→scheduler WorkerApi control-plane (TCP/HTTP2) endpoint
/// WITH connection keepalive.
///
/// The WorkerApi endpoint historically went through `tls_utils::endpoint_from`
/// (the low-level URI-string builder), which sets only `tcp_nodelay` — NO
/// keepalive. The DATA channel goes through `tls_utils::endpoint`, which
/// adds `tcp_keepalive` + the HTTP/2 keepalive trio (added in #2152 "detect
/// dead connections"). The control plane never grew keepalive because it
/// uses a slimmer config type (`EndpointConfig`) that lacks the keepalive
/// fields `GrpcEndpoint` carries — drift, not intent. This helper closes
/// that gap by mirroring the data-channel defaults
/// (`WORKER_API_TCP_KEEPALIVE` / `_HTTP2_KEEPALIVE_INTERVAL` / `_TIMEOUT`).
///
/// Liveness here is connection KEEPALIVE (dead-connection propagation), NOT
/// a per-RPC deadline — consistent with the "no per-RPC timeouts" operator
/// directive. The pre-existing `connect_timeout`/`timeout` are preserved
/// unchanged (they predate this change).
///
/// Extracted from the connection-factory closure so the keepalive is
/// independently testable via `Endpoint::get_tcp_keepalive`.
pub fn build_worker_api_tcp_endpoint(
    uri: &str,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
    timeout_duration: Duration,
) -> Result<tonic::transport::Endpoint, Error> {
    Ok(tls_utils::endpoint_from(uri, tls_config)
        .map_err(|e| make_input_err!("Invalid URI for worker endpoint : {e:?}"))?
        .connect_timeout(timeout_duration)
        .timeout(timeout_duration)
        // Keepalive so a half-open scheduler connection (no GOAWAY / no RST)
        // surfaces as a stream error within a keepalive cycle, letting the
        // bidi stream error -> `inner.run` return -> reconnect.
        .tcp_keepalive(Some(WORKER_API_TCP_KEEPALIVE))
        .http2_keep_alive_interval(WORKER_API_HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(WORKER_API_HTTP2_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true))
}

/// Maximum decoded message size for the scheduler→worker `WorkerApi` stream.
///
/// Tonic's generated client default is 4 MiB. The worker receives the
/// `UpdateForWorker` oneof which today carries:
///   * `StartExecute` with pre-resolved directory trees up to 32 MiB
///     (`api_worker_scheduler::MAX_TREE_PROTO_BYTES`). Peer hints used to
///     ride here under a `MAX_PEER_HINTS = 16384` cap; #98 moved them to
///     `Update::ChunkedMessage(PeerHintsChunk)` so `StartExecute` no
///     longer balloons under high-locality workloads.
///   * `BlobsInStableStorage` with an unbounded `repeated Digest` list
///     (one entry per blob the server just persisted; a write burst of
///     thousands of blobs in a single message is plausible). #97 will
///     chunk this similarly.
///   * `Update::ChunkedMessage` payloads — capped per chunk by their
///     producer (e.g. `PEER_HINTS_PER_CHUNK = 256` ≈ 64 KiB), so the
///     decoder limit is not the bottleneck for chunked streams.
///
/// At the default 4 MiB limit, a large `StartExecute` or
/// `BlobsInStableStorage` would be silently rejected by the worker's tonic
/// decoder, breaking the connect_worker stream and forcing reconnection
/// (which in turn delays mirror unpinning and stalls dispatches).
///
/// 64 MiB matches the server-side listener default
/// (`DEFAULT_MAX_DECODING_MESSAGE_SIZE` in `src/bin/nativelink.rs`) and the
/// worker's CAS server (`WORKER_CAS_MAX_DECODING_MESSAGE_SIZE`), keeping
/// the cross-tier ceiling consistent.
pub const WORKER_API_MAX_DECODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Default maximum amount of time a task is allowed to run for.
/// If this value gets modified the documentation in `cas_server.rs` must also be updated.
const DEFAULT_MAX_ACTION_TIMEOUT: Duration = Duration::from_secs(1200); // 20 mins.
const DEFAULT_MAX_UPLOAD_TIMEOUT: Duration = Duration::from_secs(600); // 10 mins.

// CAPPED AT 256: per-worker limit on detached AC-write inflight tasks
// spawned by the #O15 publish-closure detach. Justification: workers
// run ~16 concurrent actions; 256 = 16 actions × 16x tail multiple,
// bounded but not artificially tight. Protects against AC-store
// stall × unbounded `tokio::spawn` accumulation; OOM-class regression
// guard per the 2026-05-08 `in_flight_slow_writes` incident
// (`.claude/audits/debacle-2026-05-08-rca/`). Over-cap behavior:
// `Semaphore::try_acquire_owned` returns `Err`; the publish closure
// logs `warn!(operation_id, "AC write detached-spawn cap reached; AC
// entry will be retried on next action ingress via cache-miss
// recovery")` and skips the AC write synchronously (graceful
// degradation: better to lose 1 AC entry — Bazel re-executes — than
// to wedge worker threads under AC-store stall). Falsification:
// synthetic 1000-action burst with paused AC store must NOT OOM
// within test budget. T5 `cap_saturated_logs_warn_and_skips_ac_write`
// exercises the over-cap path.
const AC_WRITE_DETACHED_INFLIGHT_CAP: usize = 256;

/// #O15 (2026-06-07): RAII decrement of the detached-AC-write inflight
/// gauge. Holds the semaphore permit for the spawn-body lifetime so the
/// cap is honored even if the body panics. The `_permit` field releases
/// the permit when the guard drops.
struct AcWriteInflightGuard {
    inflight_count: Arc<core::sync::atomic::AtomicI64>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for AcWriteInflightGuard {
    fn drop(&mut self) {
        self.inflight_count
            .fetch_sub(1, core::sync::atomic::Ordering::AcqRel);
    }
}

/// Couples the worker's AC `FastSlowStore` handle with its configured
/// store-id name. The Some-iff-Some invariant — both fields are present
/// only when the worker's AC store is wired as a `FastSlowStore` — is
/// encoded by the type itself rather than spread across paired
/// `Option<...>` fields, so callers can't introduce a half-Some shape
/// by accident.
#[derive(Clone, Debug)]
pub struct AcMirrorTarget {
    /// The AC store's `FastSlowStore` instance. Pin advertisement
    /// inserts go via `fss.insert_local_ac_pin`; the BIS-ack drain
    /// goes via `fss.remove_local_ac_pins`.
    pub fss: Arc<FastSlowStore>,
    /// The AC store's configured name (e.g. `"AC_MAIN_STORE"`). Used
    /// as the `store_id` field in `MirrorPinEntry` so the server-side
    /// AC pin registry keys correctly.
    pub store_id: Arc<str>,
    /// #37 Phase 2 (Q5): per-digest publish-time map for AC BIS-ack
    /// observability. Inserted on successful AC publish in
    /// `UploadActionResults::upload_ac_results`; consumed here in the
    /// BIS handler (`handle_blobs_in_stable_storage_for_store` AC arm)
    /// to compute and log `ack_delay_ms` per digest, AND by the
    /// background reaper task to surface missing BIS-acks past
    /// timeout.
    ///
    /// CAPPED AT 100_000 entries: see
    /// `running_actions_manager::AC_PUBLISH_PENDING_ACKS_MAX`. ~40 B
    /// per entry → ~4 MB worst-case. Over-cap insertion skips (purely
    /// observability — pin lifecycle in
    /// `dispatched_mirror_pins` is independent).
    pub ac_publish_pending_acks:
        Arc<parking_lot::Mutex<std::collections::HashMap<DigestInfo, tokio::time::Instant>>>,
    /// #37 Phase 2 (Q5): metrics handle so the BIS handler can
    /// increment per-event counters. Clone of
    /// `RunningActionsManagerImpl::metrics`.
    pub metrics: Arc<crate::running_actions_manager::Metrics>,
}

/// #37 Phase 2 (Q4 / F1): worker-side implementation of the
/// `SlowTierMetricSink` trait declared in `nativelink-store`. The
/// `FastSlowStore` invokes this on the spawned slow-tier Err arm to
/// bump the per-`store_class` counter
/// (`worker_slow_tier_async_fail_{ac,cas,unknown}`) on
/// `RunningActionsManagerImpl::metrics`. Without this plumbing the
/// per-class counters would be declared-but-never-incremented.
///
/// `pub` so the #37 Phase 2 T3 integration test can construct one
/// in production composition and verify the end-to-end store_class
/// label plumbing (FSS Err arm → sink → per-class counter).
#[derive(Debug)]
pub struct WorkerSlowTierMetricSink {
    pub metrics: Arc<crate::running_actions_manager::Metrics>,
}

impl SlowTierMetricSink for WorkerSlowTierMetricSink {
    fn record_async_fail(&self, store_class: &str) {
        self.metrics
            .worker_slow_tier_async_fail_by_class(store_class);
    }
}

/// (A1 fix-up F1; FL-688 v3 Stage A) Outcome of
/// [`apply_periodic_tick_memo_resets`] — whether the reconnect reset path
/// fired this tick. Returned so the caller can log the reconnect event and so
/// the reconnect-clear test (T6) can assert the clear without relying on log
/// capture.
///
/// Stage A removed the `HeartbeatResync` variant: the timer-driven
/// full-snapshot heartbeat is gone (convergence is now reconnect full snapshot
/// + the Stage-2B replay-until-acked reader + the over-cap force-snapshot
/// EVENT). Only `ReconnectClear` (an EVENT — a new `run()` re-entry) and `None`
/// remain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeriodicTickMemoReset {
    /// No reset fired this tick — normal delta path.
    None,
    /// `is_first=true` reconnect-clear fired at the function head.
    ReconnectClear,
}

/// (A1 fix-up F2; FL-688 v3 Stage A) Apply the reconnect memo-reset path at
/// the head of [`LocalWorkerImpl::send_periodic_blobs_available`]:
///
///   - On `is_first=true` (reconnect — a new `run()` re-entry, one per
///     connection), clear `last_sent_ac_pin_set` so the next delta reports
///     every current pin as `added`. The server's per-endpoint AC pin set was
///     wiped by the disconnect; a full replay is the only way to restore
///     parity.
///
/// Stage A removed the periodic full-snapshot heartbeat (the every-Nth-tick
/// forced memo clear). The out-of-band server-mutation classes that heartbeat
/// guarded (AcProxy NotFound-eviction / BIS-ack sweep / cap-truncation) now
/// re-converge via the reconnect full snapshot + the Stage-2B replay-until-
/// acked reader (`replay_unacked_chunks`) + the over-cap force-snapshot EVENT
/// + the SERVER-PUSHED [`Update::AcPinResync`] signal (the Stage A fix for the
/// pair-a BLOCK: reconnect alone is unbounded on a stable connection — see
/// [`force_ac_pin_resync`]). All event-driven, no timer. Returns which path
/// fired so the caller can emit the matching log. Takes only the one piece of
/// `BlobsAvailableState` the helper touches so the T6 test can drive the
/// contract directly without constructing a full state.
pub(crate) fn apply_periodic_tick_memo_resets(
    last_sent_ac_pin_set: &Mutex<HashSet<DigestInfo>>,
    is_first: bool,
) -> PeriodicTickMemoReset {
    if is_first {
        last_sent_ac_pin_set.lock().clear();
        return PeriodicTickMemoReset::ReconnectClear;
    }
    PeriodicTickMemoReset::None
}

/// (FL-688 v3 Stage A fix) Force the worker to re-advertise its FULL current
/// AC-pin set on the next periodic tick, in response to a server-pushed
/// [`Update::AcPinResync`]. The server sends that signal immediately after it
/// removes AC-pin entries for this worker's endpoint from its `AcPinRegistry`
/// OUT-OF-BAND (BIS-ack sweep / AcProxy peer-NotFound eviction /
/// cap-truncation). After such a removal the server's per-endpoint set has
/// FEWER entries than the worker's `last_sent_ac_pin_set`; the worker's own
/// AC-pin set is unchanged, so the next tick computes an EMPTY delta and the
/// skip-gate ([`should_skip_blobs_available_tick`]) SUPPRESSES it — leaving the
/// divergence to persist until the next reconnect (rare on a stable
/// connection). This is the BLOCK pair-a found: removing the periodic heartbeat
/// left no bounded-time convergence path for server-side removals.
///
/// The fix CLEARS `last_sent_ac_pin_set` (the exact mechanism the reconnect
/// `is_first` clear uses): the next tick's delta then reports the whole current
/// set as `added`, so `ac_pin_delta_empty=false`, the skip-gate no longer
/// suppresses, and field 17 (`pinned_ac_mirror_entries`, REPLACE-semantics) is
/// re-sent in full — the server's `replace_endpoint_ac_pins` restores parity.
/// Idempotent: multiple signals before the next tick collapse to one
/// re-snapshot (the clear is idempotent and the tick re-sends the same full
/// set), which is why the hot BIS-ack sweep can fire the push once per endpoint
/// per sweep cycle without coalescing.
pub(crate) fn force_ac_pin_resync(last_sent_ac_pin_set: &Mutex<HashSet<DigestInfo>>) {
    last_sent_ac_pin_set.lock().clear();
}

/// Counts how many AC-pin digests have been added (in `current` but not
/// `last`) and removed (in `last` but not `current`). Pure function so
/// unit tests can exercise the delta primitive without spinning up the
/// full `send_periodic_blobs_available` machinery. Used by
/// [`should_skip_blobs_available_tick`].
#[inline]
fn compute_ac_pin_delta_counts(
    current: &HashSet<DigestInfo>,
    last: &HashSet<DigestInfo>,
) -> (usize, usize) {
    let added = current.difference(last).count();
    let removed = last.difference(current).count();
    (added, removed)
}

/// (A1 fix) The pre-fix gate predicate read `pinned_ac_mirror_count == 0`,
/// which failed permanently once steady-state AC pins existed →
/// re-broadcast of an unchanged snapshot every 100 ms. The new
/// predicate keys on the delta vs the last-sent set.
///
/// Returns `true` iff `send_periodic_blobs_available` should skip
/// emitting this tick. Reconnect (`is_first=true`) ALWAYS sends.
#[inline]
fn should_skip_blobs_available_tick(
    is_first: bool,
    new_or_touched_count: usize,
    evicted_count: usize,
    added_subtree_count: usize,
    removed_subtree_count: usize,
    pinned_mirror_count: usize,
    ac_pin_delta_empty: bool,
) -> bool {
    !is_first
        && new_or_touched_count == 0
        && evicted_count == 0
        && added_subtree_count == 0
        && removed_subtree_count == 0
        && pinned_mirror_count == 0
        && ac_pin_delta_empty
}

/// Holds the FilesystemStore reference and change tracker needed for
/// BlobsAvailable reporting with drain-then-fire semantics.
#[derive(Clone, Debug)]
pub struct BlobsAvailableState {
    /// Reference to the worker's local FilesystemStore (the fast store in FastSlowStore).
    fs_store: Arc<FilesystemStore>,
    /// Tracks inserted and evicted digests between sends.
    tracker: Arc<BlobChangeTracker>,
    /// The worker's CAS endpoint for peer serving (e.g. "grpc://192.168.100.5:50081").
    cas_endpoint: String,
    /// Woken by the tracker on every insert/eviction so the send loop fires
    /// immediately instead of sleeping for a fixed interval.
    notify: Arc<Notify>,
    /// Backstop interval: even without blob changes, wake periodically to
    /// pick up subtree-only deltas that bypass the tracker notify.
    max_interval: Duration,
    /// The FastSlowStore backing the worker's CAS server. Used to clean up
    /// mirror blobs when `BlobsInStableStorage` is received.
    cas_server_fss: Option<Arc<FastSlowStore>>,
    /// The worker's AC store wired as a `FastSlowStore`, when configured
    /// that way. Source of `pinned_ac_mirror_entries` (proto field 17)
    /// in the `BlobsAvailable` snapshot, and target of the AC-pin
    /// removal on `BlobsInStableStorage` ack. `None` when the worker
    /// has no AC store, or its AC store is a direct GrpcStore (no FSS
    /// wrap), or any wrapper hides the FSS that the
    /// `find_fast_slow_for_pin` walker can't see through.
    ac_mirror_target: Option<AcMirrorTarget>,
    /// (#99) Worker-process nonce, randomized at construction. Stamped
    /// onto every `BlobsAvailableChunk` the worker emits so the
    /// server's per-broadcast accumulator can detect a worker-process
    /// restart mid-broadcast (impossible in steady state, but
    /// defensive). Mirrors #97's `server_instance_token` pattern in
    /// the opposite direction.
    worker_instance_token: u64,
    /// (#99) Lock-free monotonic counter for `BlobsAvailableChunk`
    /// `broadcast_id` allocation. Resets on worker restart (the new
    /// `worker_instance_token` makes this safe — the server's
    /// accumulator keys on `(broadcast_id, worker_instance_token)`).
    next_broadcast_id: Arc<AtomicU64>,
    /// (A1 fix) Last AC-pin set we successfully advertised to the
    /// server. `send_periodic_blobs_available` compares the current
    /// `dispatched_ac_pin_snapshot_for_store` result against this
    /// set; if added+removed are both empty (and no other deltas
    /// fired) the tick is suppressed. Cleared on `is_first=true`
    /// (reconnect) so the next tick re-sends the full snapshot.
    /// `parking_lot::Mutex` is sync-only — only held briefly in the
    /// send path, never across `.await`.
    last_sent_ac_pin_set: Arc<Mutex<HashSet<DigestInfo>>>,
    /// (Probe #3) Counter of empty-tick BlobsAvailable suppressions
    /// at the `send_periodic_blobs_available` skip-gate. Incremented
    /// each time the gate fires; persists across reconnects so a
    /// chronic empty-tick storm is visible without comparing to a
    /// baseline. `Relaxed` is sufficient — read for diagnostics, not
    /// load-bearing.
    blobs_available_skipped_counter: Arc<AtomicU64>,
    /// (FL-688 v3 §3.8) Bounded resend buffer of unacked worker→server
    /// DELTA `BlobsAvailableChunk`s. The send path buffers each delta
    /// chunk here on send and `handle_blobs_available_ack` clears the
    /// matching `(broadcast_id, sequence)` slot when the server's
    /// `BlobsAvailableAck` arrives; a reconnect (full snapshot) clears it
    /// wholesale (the snapshot supersedes every buffered delta).
    /// `parking_lot::Mutex` is sync-only — held only briefly in the send /
    /// ack paths, never across `.await`.
    blobs_available_resend: Arc<Mutex<BlobsAvailableResendBuffer>>,
    /// (FL-688 v3 §3.8) Set when the resend buffer overflows
    /// [`BLOBS_AVAILABLE_RESEND_MAX_CHUNKS`]; promotes the NEXT
    /// `send_periodic_blobs_available` tick to a full snapshot (which
    /// supersedes every dropped delta — lossless). This is the
    /// self-correcting convergence the removed 60s heartbeat used to
    /// provide, now triggered by buffer pressure (an EVENT, not a timer).
    /// `Relaxed` is sufficient — set and read only on the single send task.
    blobs_available_force_full_snapshot: Arc<AtomicBool>,
}

/// Test-only builder for [`BlobsAvailableState`]. Lets each test set only
/// the fields it cares about and rely on `Default` for the rest.
///
/// Replaces the previous `new_for_test` / `new_for_test_with_ac` factory
/// pair (#281 simplifier MAJOR-2): adding new optional state fields no
/// longer requires another constructor — extend this struct with a
/// sensible `Default` and existing callers stay green via
/// `..Default::default()`.
///
/// `fs_store` has no sensible default (every test needs its own
/// tempdir-backed store) so it's a required argument to
/// [`BlobsAvailableState::from_test_args`]; everything else defaults.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Default)]
pub struct BlobsAvailableTestArgs {
    /// CAS-server `FastSlowStore` for tests that exercise the
    /// CAS-mirror cleanup path. `None` for tests that only need a
    /// `BlobsAvailableState` to drive non-CAS code paths.
    pub cas_server_fss: Option<Arc<FastSlowStore>>,
    /// AC-mirror target for tests that exercise AC-pin advertisement /
    /// unpin behavior.
    pub ac_mirror_target: Option<AcMirrorTarget>,
    /// The worker's advertised CAS endpoint. Defaults to empty (the
    /// pre-existing behavior). Tests that assert the SERVER's locality
    /// view (which keys digests by this endpoint) set it non-empty so the
    /// committed notification registers under a real key.
    pub cas_endpoint: String,
}

impl BlobsAvailableState {
    /// Test-only: build a `BlobsAvailableState` from a
    /// [`BlobsAvailableTestArgs`] builder. The non-test path
    /// constructs this inline inside `new_local_worker`.
    ///
    /// `fs_store` is the only required argument (no sensible default).
    /// All other fields default via [`BlobsAvailableTestArgs::default`];
    /// override only the ones the test cares about, e.g.
    ///
    /// ```ignore
    /// BlobsAvailableState::from_test_args(
    ///     fs_store,
    ///     BlobsAvailableTestArgs {
    ///         ac_mirror_target: Some(target),
    ///         ..Default::default()
    ///     },
    /// )
    /// ```
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn from_test_args(fs_store: Arc<FilesystemStore>, args: BlobsAvailableTestArgs) -> Self {
        let BlobsAvailableTestArgs {
            cas_server_fss,
            ac_mirror_target,
            cas_endpoint,
        } = args;
        Self {
            fs_store,
            tracker: BlobChangeTracker::new(Arc::new(Notify::new())),
            cas_endpoint,
            notify: Arc::new(Notify::new()),
            max_interval: Duration::from_secs(60),
            cas_server_fss,
            ac_mirror_target,
            // (#99) Tests get a deterministic non-zero token so accumulator
            // identity assertions work without unwrapping random state.
            worker_instance_token: 0xA5A5_A5A5_A5A5_A5A5,
            next_broadcast_id: Arc::new(AtomicU64::new(0)),
            last_sent_ac_pin_set: Arc::new(Mutex::new(HashSet::new())),
            blobs_available_skipped_counter: Arc::new(AtomicU64::new(0)),
            blobs_available_resend: Arc::new(Mutex::new(BlobsAvailableResendBuffer::default())),
            blobs_available_force_full_snapshot: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Test-only: whether the over-cap valve has requested a forced full
    /// snapshot on the next tick (and clear it, mirroring the send path).
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_take_force_full_snapshot(&self) -> bool {
        self.blobs_available_force_full_snapshot
            .swap(false, Ordering::Relaxed)
    }

    /// (FL-688 v3 §3.8) Buffer one just-sent DELTA chunk into the resend
    /// buffer (drain-on-ack). On over-cap the buffer self-clears and we
    /// SET the force-full-snapshot flag so the next tick re-converges with
    /// a snapshot that supersedes every dropped delta. Returns `true` iff
    /// the over-cap reset fired (the caller logs it). The production send
    /// path and the test seam both go through here so the test crosses the
    /// exact over-cap → force-snapshot wiring.
    fn buffer_delta_chunk(&self, chunk: BlobsAvailableChunk) -> bool {
        let over_cap = self.blobs_available_resend.lock().add(chunk);
        if over_cap {
            self.blobs_available_force_full_snapshot
                .store(true, Ordering::Relaxed);
        }
        over_cap
    }

    /// Test-only: buffer one delta chunk as if it had just been sent (the
    /// production [`Self::buffer_delta_chunk`] seam). Returns the over-cap
    /// "force full snapshot" signal.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_buffer_delta_chunk(&self, chunk: BlobsAvailableChunk) -> bool {
        self.buffer_delta_chunk(chunk)
    }

    /// (FL-688 v3 §3.8) Clear the resend buffer wholesale — called when a
    /// full snapshot is sent (reconnect / forced), since the snapshot
    /// supersedes every buffered delta.
    fn clear_resend_buffer(&self) {
        self.blobs_available_resend.lock().chunks.clear();
    }

    /// (FL-688 v3 §3.8 drain-on-ack flip) Snapshot every still-unacked
    /// delta chunk for the per-tick replay reader. Held lock is released
    /// before the caller awaits the sends, so no lock crosses `.await`.
    fn unacked_chunks_for_replay(&self) -> Vec<BlobsAvailableChunk> {
        self.blobs_available_resend.lock().unacked_chunks()
    }

    /// Test-only: number of unacked chunks currently buffered.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_resend_buffer_len(&self) -> usize {
        self.blobs_available_resend.lock().len()
    }

    /// Test-only: whether `(broadcast_id, sequence)` is still buffered.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_resend_buffer_contains(&self, broadcast_id: u64, sequence: u32) -> bool {
        self.blobs_available_resend
            .lock()
            .chunks
            .contains_key(&(broadcast_id, sequence))
    }

    /// Test-only: record `digest` as a newly-added blob in the change
    /// tracker via the SAME `on_insert` path the FilesystemStore callback
    /// fires in production. The next `is_first=false` tick then computes a
    /// DELTA carrying this digest — used by the replay-convergence test to
    /// drive a delta without registering the tracker on a live store +
    /// racing a real write.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn test_record_added_digest(&self, digest: DigestInfo) {
        // (#locality-map-drift) Carry this process's boot_epoch + a fresh
        // counter so the delta records the digest PRESENT (the LWW is per
        // digest, so a shared counter is fine across distinct digests).
        self.tracker
            .on_insert(StoreKey::Digest(digest), 0, boot_epoch_id(), 1);
    }

    /// Test-only: seed `last_sent_ac_pin_set` to a known set. Models the
    /// post-ack state where the server has acked the worker's AC-pin
    /// advertisement, so the worker's memo records what it believes the server
    /// holds. Used by the AC-pin resync convergence test to arm the skip-gate
    /// (memo == current set ⇒ delta empty ⇒ tick suppressed) before simulating
    /// the server's out-of-band removal.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn test_seed_last_sent_ac_pin_set(&self, digests: &[DigestInfo]) {
        let mut set = self.last_sent_ac_pin_set.lock();
        set.clear();
        set.extend(digests.iter().copied());
    }

    /// Test-only: snapshot of the current `last_sent_ac_pin_set` memo.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_last_sent_ac_pin_set(&self) -> HashSet<DigestInfo> {
        self.last_sent_ac_pin_set.lock().clone()
    }

    /// Test-only: drive the `Update::AcPinResync` worker handler's core effect
    /// (clear the AC-pin memo) without standing up the full `run()` stream loop.
    /// Mirrors the production arm in `LocalWorkerImpl::run`. The mutation guard
    /// for the convergence test comments out the body of [`force_ac_pin_resync`];
    /// this seam keeps the test pinned to the production function.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn test_handle_ac_pin_resync(&self) {
        force_ac_pin_resync(&self.last_sent_ac_pin_set);
    }
}

/// Process a `BatchWriteSmallBlobs` push from the server's
/// `SmallBlobDispatcher` (Bug A small-CAS peer-mirror; task #153).
///
/// For each `SmallBlobEntry`:
///   * Decode the proto digest into `DigestInfo` (skip + warn on
///     malformed).
///   * Validate `data.len() == digest.size_bytes()` (preserves the
///     load-bearing invariant from `fast_slow_store.rs:771-784`).
///   * Call `cas_server_fss.insert_dispatched_mirror_blob(store_id,
///     digest, data)`. Errors (cap exceeded, etc.) are logged but the
///     batch continues — partial-batch acceptance is OK because the
///     server's per-store `EphemeralServerSidePin` TTL will reclaim
///     the unacked entries.
///
/// Extracted from the `Update::BatchWriteSmallBlobs` match arm in
/// `LocalWorkerImpl::run` so the handler is unit-testable without
/// standing up the full scheduler/worker stream stack. The dispatch
/// arm is a thin call site; all behavior lives here.
pub fn handle_batch_write_small_blobs(
    cas_server_fss: Option<&Arc<FastSlowStore>>,
    blobs: &[nativelink_proto::com::github::trace_machina::nativelink::remote_execution::SmallBlobEntry],
) {
    let blob_count = blobs.len();
    let total_bytes: usize = blobs.iter().map(|b| b.data.len()).sum();
    let Some(fss) = cas_server_fss else {
        warn!(
            blob_count,
            total_bytes,
            "BatchWriteSmallBlobs: no cas_server_fss on this worker; dropping batch \
             (worker has no CAS server / mirror store — server should not have \
             dispatched here; check locality registration)"
        );
        return;
    };
    let mut inserted = 0usize;
    let mut skipped = 0usize;
    for entry in blobs {
        // Wire-side store_id validation (#168 producer wire-up review):
        // the worker writes `dispatched_mirror_pins[(store_id, digest)]`
        // which is later iterated to populate
        // `BlobsAvailableNotification.pinned_mirror_entries` (proto
        // field 16). A malformed `store_id` from a buggy or untrusted
        // server would (a) leak unbounded keys into the BTreeMap, and
        // (b) propagate to the wire ack, where the server's
        // `is_valid_store_id`-keyed pin-set lookup would silently fail
        // to unpin — creating a memory-leak path on the server. Reject
        // here with the same regex `enqueue` enforces (Rust-ident
        // shape per plan C11).
        if !nativelink_store::small_blob_dispatcher::is_valid_store_id(&entry.store_id) {
            warn!(
                store_id = entry.store_id,
                "BatchWriteSmallBlobs: invalid store_id (must match \
                 `[a-zA-Z_][a-zA-Z0-9_]*` per plan C11); skipping"
            );
            skipped += 1;
            continue;
        }
        let Some(proto_digest) = entry.digest.as_ref() else {
            warn!(
                store_id = entry.store_id,
                "BatchWriteSmallBlobs: entry has no digest; skipping"
            );
            skipped += 1;
            continue;
        };
        let digest = match DigestInfo::try_from(proto_digest.clone()) {
            Ok(d) => d,
            Err(err) => {
                warn!(
                    ?err,
                    store_id = entry.store_id,
                    "BatchWriteSmallBlobs: invalid digest, skipping"
                );
                skipped += 1;
                continue;
            }
        };
        if let Err(err) =
            fss.insert_dispatched_mirror_blob(&entry.store_id, digest, entry.data.clone())
        {
            // insert_dispatched_mirror_blob already warns; bump the
            // skip counter and move on. Partial batches are fine
            // because the server's pin TTL recovers.
            warn!(
                ?err,
                store_id = entry.store_id,
                %digest,
                "BatchWriteSmallBlobs: insert_dispatched_mirror_blob failed; skipping"
            );
            skipped += 1;
            continue;
        }
        inserted += 1;
    }
    info!(
        blob_count,
        inserted, skipped, total_bytes, "BatchWriteSmallBlobs: batch processed"
    );
}

/// Outcome of one BIS unpin pass: how many digests were successfully
/// unpinned and how many failed (currently only digest-decode errors;
/// `unpin_digest` itself is infallible). Returned by
/// [`handle_blobs_in_stable_storage`] so the chunked caller
/// ([`handle_bis_chunk`]) can gate the ack on per-digest success — see
/// the doc comment on `handle_bis_chunk` for why partial failure must
/// suppress the ack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BisUnpinOutcome {
    /// Number of digests where the proto decoded AND every per-digest
    /// side effect (FilesystemStore::unpin_digest, mirror cleanup,
    /// failed_slow_writes ack) ran without error.
    pub unpinned: usize,
    /// Number of digests where the proto failed `DigestInfo::try_from`.
    /// All other unpin operations on this layer are infallible today,
    /// so this is the only failure mode currently observable; the count
    /// is exposed as a struct field to make future failure-mode growth
    /// non-breaking.
    pub failed: usize,
}

impl BisUnpinOutcome {
    /// True iff every digest in the input batch was processed without
    /// error. Used by `handle_bis_chunk` as the ack gate.
    #[inline]
    pub const fn all_succeeded(&self) -> bool {
        self.failed == 0
    }
}

/// Process a `BlobsInStableStorage` notification from the server:
///   * Unpin the digests on the local FilesystemStore so they become
///     eligible for eviction.
///   * Drop them from the pending-upload (`failed_slow_writes`) set
///     so a reconnect doesn't re-upload them.
///   * Drop the in-memory mirror copies from the CAS server's
///     FastSlowStore — the server now has its own durable copy and
///     the worker no longer needs to hold one.
///
/// Returns a [`BisUnpinOutcome`] reporting how many digests
/// succeeded vs. failed. Today the only failure mode is
/// `DigestInfo::try_from` returning Err on a malformed proto digest;
/// `FilesystemStore::unpin_digest` and the mirror cleanup are both
/// infallible. The `Result`-shaped return type is preserved so future
/// failure-mode growth (e.g. disk-IO-backed unpin) does not require a
/// breaking signature change at every call-site.
///
/// Extracted from the `Update::BlobsInStableStorage` match arm in
/// `LocalWorkerImpl::run` so the handler is unit-testable without
/// standing up the full scheduler/worker stream stack. The dispatch
/// arm is a thin call site; all behavior lives here.
pub fn handle_blobs_in_stable_storage(
    state: &BlobsAvailableState,
    cas_store: Option<&Arc<FastSlowStore>>,
    proto_digests: &[nativelink_proto::build::bazel::remote::execution::v2::Digest],
) -> BisUnpinOutcome {
    handle_blobs_in_stable_storage_for_store(state, cas_store, "", proto_digests)
}

/// Variant of [`handle_blobs_in_stable_storage`] that takes the
/// chunk's `store_id` and dispatches to:
///
/// - Empty `store_id` (`""`): the historic CAS path — unpins from the
///   FilesystemStore, calls `cas_store.ack_digests`, drops `mirror_blobs`
///   from the CAS FSS. **Forward-compatible default for pre-AC-BIS
///   servers.**
/// - `store_id` matching this worker's configured AC store name: the
///   AC pin drain path — calls `remove_local_ac_pins` ONLY. Does NOT
///   touch the FilesystemStore (AC entries never lived there) and
///   does NOT touch `mirror_blobs` (same — AC pins never registered
///   there in this Option-A design).
/// - Unknown non-empty `store_id`: `warn!` and treat as a no-op (the
///   chunk is still acked so the server's resend buffer drains). Being
///   asked to unpin against a store this worker doesn't know about is
///   benign on the worker side; the registry mismatch is a server-side
///   config drift problem and surfaces in the warn log.
pub fn handle_blobs_in_stable_storage_for_store(
    state: &BlobsAvailableState,
    cas_store: Option<&Arc<FastSlowStore>>,
    store_id: &str,
    proto_digests: &[nativelink_proto::build::bazel::remote::execution::v2::Digest],
) -> BisUnpinOutcome {
    let digest_count = proto_digests.len();
    let mut decoded = 0usize;
    let mut failed = 0usize;
    let mut acked_digests: Vec<DigestInfo> = Vec::with_capacity(digest_count);
    for proto_digest in proto_digests {
        if let Ok(digest) = DigestInfo::try_from(proto_digest.clone()) {
            acked_digests.push(digest);
            decoded += 1;
        } else {
            failed += 1;
            warn!(
                ?proto_digest,
                "BlobsInStableStorage: invalid digest, skipping unpin"
            );
        }
    }

    // Dispatch on store_id. CRITICAL: AC chunks (non-empty store_id
    // matching the configured AC store) MUST NOT walk the CAS path;
    // routing AC digests through `cas_fss.remove_mirror_blobs` would
    // (a) walk the wrong byte map (zero overlap with AC entries), and
    // (b) walk `dispatched_mirror_pins` removing matches keyed by
    // digest only — collateral damage to CAS pins for the same digest.
    //
    // `unpinned` is set ONLY in the branches that actually mutate pin
    // state. The unknown-store_id and no-AC-target branches return
    // `unpinned = 0` so observability accurately reflects "did we do
    // anything" — the `warn!` is the only signal that the chunk was
    // received but unrouted, and the metric must not contradict it.
    let unpinned = if store_id.is_empty() {
        // CAS path — historic shape. Every decoded digest is unpinned
        // and acked; `unpinned` equals `decoded` here by construction.
        let fs_store = &state.fs_store;
        for digest in &acked_digests {
            fs_store.unpin_digest(digest);
            // #547 Phase 0 instrumentation: record the pin-release
            // latency for any digest that went through
            // spawn_upload_to_remote on this worker (the side-channel
            // returns None for digests that didn't, which includes
            // Bazel-source uploads landing here only via the receive-
            // side mirror path — for those the metric is correctly
            // skipped). Pure observability; no behavior change.
            //
            // Gate `record_pin_released` on `record_bis_unpin` returning
            // `Some(_)` so acquire/release stay symmetric: the gauge
            // only decrements for digests that previously bumped it via
            // `record_pin_acquired` (which fires only from
            // `spawn_upload_to_remote`). Without this gating the gauge
            // could over-release for Bazel-source / receive-side-mirror
            // digests that arrived at this unpin loop without ever
            // having been acquired through the chunked-upload producer.
            if worker_phase0_metrics().record_bis_unpin(digest).is_some() {
                worker_phase0_metrics().record_pin_released(digest.size_bytes());
            }
        }
        if let Some(cas_store) = cas_store {
            cas_store.ack_digests(&acked_digests);
        }
        if let Some(cas_fss) = state.cas_server_fss.as_ref() {
            let before = cas_fss.mirror_blob_count();
            cas_fss.remove_mirror_blobs(&acked_digests);
            let removed = before - cas_fss.mirror_blob_count();
            if removed > 0 {
                // Per-chunk BIS-unpin firehose (~245 chunks/s live);
                // trace! so prod release builds compile it out.
                trace!(
                    removed,
                    remaining = cas_fss.mirror_blob_count(),
                    "BlobsInStableStorage CAS: removed mirror blobs from memory"
                );
            }
        }
        // Per-chunk BIS-unpin firehose (~245 chunks/s live); trace! so
        // prod release builds compile it out.
        trace!(
            unpinned = decoded,
            failed,
            digest_count,
            store_id = "",
            "BlobsInStableStorage CAS: unpinned digests from local CAS"
        );
        decoded
    } else if let Some(target) = state.ac_mirror_target.as_ref() {
        if target.store_id.as_ref() == store_id {
            target.fss.remove_local_ac_pins(&acked_digests);
            // #37 Phase 2 (Q5): per-digest BIS-ack info log with
            // ack_delay_ms. Lookup-and-remove against the publish-time
            // map populated in `UploadActionResults::upload_ac_results`.
            // Digests not in the map (e.g. publish landed before
            // worker restart) are skipped silently — no extra counter
            // (the `worker_bis_ack_received` counter covers events
            // observable to this worker; cross-restart correlation is
            // not in scope for this phase).
            //
            // F7: collect (digest, ack_delay_ms) tuples under the
            // lock, emit logs + bump counters AFTER the guard drops.
            // The same critical-section discipline the reaper uses.
            let acked_with_delays: Vec<(DigestInfo, u64)> = {
                let mut guard = target.ac_publish_pending_acks.lock();
                acked_digests
                    .iter()
                    .filter_map(|digest| {
                        guard
                            .remove(digest)
                            .map(|start| (*digest, start.elapsed().as_millis() as u64))
                    })
                    .collect()
            };
            for (digest, ack_delay_ms) in acked_with_delays {
                // Per-DIGEST BIS-ack firehose (~1,470 events/s live);
                // trace! so prod release builds compile it out. The
                // worker_bis_ack_received counter below is the durable
                // signal and is unaffected.
                trace!(
                    ?digest,
                    ack_delay_ms,
                    store_id,
                    "AC BIS-ack received",
                );
                target.metrics.worker_bis_ack_received.inc();
            }
            // Per-chunk BIS-unpin firehose; trace! so prod release
            // builds compile it out.
            trace!(
                unpinned = decoded,
                failed, digest_count, store_id, "BlobsInStableStorage AC: dropped local AC pins"
            );
            decoded
        } else {
            warn!(
                store_id,
                ac_store_id = %target.store_id,
                digest_count,
                "BlobsInStableStorage: store_id does not match this worker's \
                 configured AC store; treating as no-op (chunk will still be \
                 acked so server resend buffer drains)"
            );
            0
        }
    } else {
        warn!(
            store_id,
            digest_count,
            "BlobsInStableStorage: chunk carries non-empty store_id but this \
             worker has no AC mirror target; treating as no-op"
        );
        0
    };

    BisUnpinOutcome { unpinned, failed }
}

/// (#97) Process one `BlobsInStableStorageChunk` arriving on the
/// scheduler→worker stream and emit the matching `BisAck` IFF every
/// digest in the chunk was unpinned without error.
///
/// **Ack-on-success-only.** Per red-team finding #3 on the original
/// #97 PR: previously the ack fired unconditionally after the unpin
/// pass. If `handle_blobs_in_stable_storage` failed on any digest
/// (today: malformed proto), the ack still went out → the server
/// dropped the chunk from the resend buffer → on the next ConnectWorker
/// the worker never saw a replay → the failed digests stayed pinned
/// forever. Same outcome as the original #89 bug, different mechanism,
/// less detectable. The fix: the ack fires only when every digest in
/// the chunk was processed successfully (`outcome.all_succeeded()`).
/// On partial failure, an `error!` log records the chunk identity and
/// the failure count; the server's resend buffer keeps the chunk and
/// the next reconnect replays it.
///
/// **Empty-terminal still acks.** A chunk with zero digests
/// (`chunk_iter`'s empty-terminal contract) trivially succeeds — there
/// is nothing to fail on — so the ack fires and the server's resend
/// buffer slot is released. Without this, a broadcast whose final
/// chunk lands on the chunk-size boundary would leak its slot forever.
///
/// **Duplicate chunks ack on every delivery.** When a server resend
/// crosses an in-flight ack, the worker sees the same chunk twice;
/// every digest decodes again, every unpin is idempotent → outcome
/// is success → ack fires. The server's per-chunk slot is keyed on
/// `(broadcast_id, sequence)` so the second ack is a harmless
/// `HashMap::remove` on a missing key.
///
/// Returns the [`BisUnpinOutcome`] reporting per-digest success/failure
/// counts. Callers can use the failed-count for observability;
/// production callers MUST NOT bypass the ack-gate by calling
/// `ack_sink` themselves on partial failure.
pub fn handle_bis_chunk(
    state: &BlobsAvailableState,
    cas_store: Option<&Arc<FastSlowStore>>,
    chunk: &BlobsInStableStorageChunk,
    ack_sink: impl FnOnce(BisAck),
) -> BisUnpinOutcome {
    let outcome =
        handle_blobs_in_stable_storage_for_store(state, cas_store, &chunk.store_id, &chunk.digests);
    if outcome.all_succeeded() {
        // Echo the server_instance_token from the chunk into the ack
        // (red-team #5: scheduler validates the token to drop acks
        // across server-bounces).
        (ack_sink)(BisAck {
            broadcast_id: chunk.broadcast_id,
            sequence: chunk.sequence,
            server_instance_token: chunk.server_instance_token,
        });
    } else {
        // Loud-log so operators see the unpin-failure rate. The server
        // will retain the chunk in its per-worker resend buffer and
        // replay on the next ConnectWorker.
        error!(
            target: "nativelink::bis_chunk_unpin_failure",
            broadcast_id = chunk.broadcast_id,
            sequence = chunk.sequence,
            unpinned = outcome.unpinned,
            failed = outcome.failed,
            digest_count = chunk.digests.len(),
            "BIS chunk had unpin failures; SUPPRESSING ack so server replays \
             chunk on next reconnect — without this gate a single malformed \
             digest in the chunk would leak the chunk's pin state forever"
        );
    }
    outcome
}

/// (FL-688 v3 §3.8) Process one `BlobsAvailableAck` arriving on the
/// scheduler→worker stream: drop the matching `(broadcast_id, sequence)`
/// delta chunk from the worker's resend buffer (drain-on-ack).
///
/// This is the mirror image of [`handle_bis_chunk`]'s ack EMISSION: there
/// the worker emits a `BisAck` for a server BIS chunk; here the worker
/// RECEIVES the server's ack for a delta chunk it sent.
///
/// **Token guard (red-team #5 on #97, opposite direction).** The ack
/// echoes the `worker_instance_token` the worker stamped on the original
/// chunk. We drop any ack whose echo ≠ this worker process's CURRENT
/// token (regenerated each boot) — otherwise a server holding a STALE ack
/// across a worker bounce could drop an unrelated chunk from the NEW
/// process's resend buffer. A token of 0 is "uninitialised" and is
/// always treated as mismatched. (Without the guard, the
/// `stale_worker_token_ack_is_dropped` / `token_zero_ack_is_dropped`
/// tests red-fail.)
///
/// **Per-chunk drop (not per-broadcast).** Only the exact
/// `(broadcast_id, sequence)` slot is removed; sibling unacked deltas of
/// the same broadcast stay buffered. An ack for a slot not present is a
/// harmless no-op (idempotent under a resend that crosses an in-flight
/// ack). This is a thin function so the `Update::BlobsAvailableAck`
/// dispatch arm is a one-line call site and the guard logic is unit-
/// testable without standing up the full stream stack.
pub fn handle_blobs_available_ack(state: &BlobsAvailableState, ack: &BlobsAvailableAck) {
    if ack.worker_instance_token == 0 || ack.worker_instance_token != state.worker_instance_token {
        // Stale or uninitialised token — a different (or pre-fixup)
        // worker process. Dropping the buffered chunk here would
        // orphan the current process's still-unacked delta. Mirror of
        // the scheduler's `bis_ack_received` server-token guard.
        debug!(
            target: "nativelink::blobs_available_ack",
            broadcast_id = ack.broadcast_id,
            sequence = ack.sequence,
            ack_token = ack.worker_instance_token,
            current_token = state.worker_instance_token,
            "dropping BlobsAvailableAck with non-current worker_instance_token"
        );
        return;
    }
    state
        .blobs_available_resend
        .lock()
        .ack(ack.broadcast_id, ack.sequence);
}

/// Process one `PeerHintsChunk` arriving on the scheduler→worker stream:
/// register every (digest, endpoints) pair into the worker's global
/// `peer_locality_map` so subsequent `WorkerProxyStore` reads can route
/// to peer workers.
///
/// Direct-merge design: NO buffer keyed on `operation_id`, NO wait for a
/// chunk-count predicate, NO race-elimination machinery. Each chunk's
/// hints are independently meaningful — a chunk that arrives BEFORE the
/// matching `StartAction` works fine (the worker has the hints early); a
/// chunk that arrives AFTER `input_fetch` started works fine too (the
/// hints simply aren't consulted; the worker falls back to the server CAS
/// or whatever locality state was already present).
///
/// Logged at `info!` so the chunk arrival cadence is visible in
/// production journals; the per-chunk count + sequence + is_last let
/// reviewers reconstruct the scheduler's emit pattern from logs alone.
/// (#p2p-prefetch) Register the inline `StartExecute.missing_digest_peers`
/// into the worker's global `peer_locality_map` SYNCHRONOUSLY, at StartExecute
/// parse time — BEFORE input materialization (`download_to_directory`) issues
/// the first missing-blob `get_part`. This is the ONE new worker-side step of
/// the worker-driven P2P input prefetch: it makes the EXISTING
/// `WorkerProxyStore` peer-race (`worker_proxy_store.rs` `get_part`) fire for
/// THIS action's missing inputs on the first read — instead of losing to the
/// async `PeerHintsChunk` stream's timing (whose chunk "may arrive after
/// input_fetch started", `handle_peer_hints_chunk` doc) and demand-fetching
/// from the server.
///
/// Because this runs on the same action-setup path strictly UPSTREAM of the
/// fetch (parse StartExecute → prepare inputs → download), the registration is
/// a hard happens-before the read's `lookup_workers` — no buffer, no
/// chunk-count, no race. Uses the SAME idempotent `register_blobs` as
/// `handle_peer_hints_chunk`; a digest registered by both merges endpoints. The
/// inline set is a strict subset of what the async stream registers
/// (`all_missing ⊆ file_digests`), so this adds NO new keys to the map — it
/// only registers them earlier and guaranteed (design §5/D1: zero net growth).
///
/// No-op when the worker has no `peer_locality_map` (no `cas_server_port` →
/// peer sharing disabled), or when the flag is off server-side (the field
/// arrives empty). Entries with an unparseable digest are skipped.
///
/// Synchronous: one `peer_locality_map.write()` for the whole batch (mirrors
/// `handle_peer_hints_chunk` to avoid N× contention), no `.await`.
pub fn register_missing_blob_peers(
    peer_locality_map: Option<&SharedBlobLocalityMap>,
    missing_digest_peers: &[MissingBlobPeers],
) {
    if missing_digest_peers.is_empty() {
        return;
    }
    let Some(locality_map) = peer_locality_map else {
        // Worker built without peer-blob sharing (no `cas_server_port`).
        // Hints would be unused even if registered; drop them silently.
        trace!(
            entries = missing_digest_peers.len(),
            "StartExecute.missing_digest_peers received but worker has no \
             peer_locality_map (peer sharing disabled)"
        );
        return;
    };
    let mut total_registered = 0usize;
    {
        let mut map = locality_map.write();
        for entry in missing_digest_peers {
            let Some(ref digest_proto) = entry.digest else {
                continue;
            };
            let Ok(digest) = DigestInfo::try_from(digest_proto) else {
                continue;
            };
            for endpoint in &entry.peer_endpoints {
                map.register_blobs(endpoint, &[digest]);
                total_registered += 1;
            }
        }
    }
    info!(
        entries = missing_digest_peers.len(),
        registrations = total_registered,
        "registered inline StartExecute peer hints into locality map before input_fetch"
    );
}

pub fn handle_peer_hints_chunk(
    peer_locality_map: Option<&SharedBlobLocalityMap>,
    chunk: &PeerHintsChunk,
) {
    let Some(locality_map) = peer_locality_map else {
        // Worker built without peer-blob sharing (no `cas_server_port`).
        // Hints would be unused even if registered; drop them silently
        // at trace level.
        trace!(
            operation_id = %chunk.operation_id,
            sequence = chunk.sequence,
            is_last = chunk.is_last,
            hint_count = chunk.peer_hints.len(),
            "PeerHintsChunk received but worker has no peer_locality_map (peer sharing disabled)"
        );
        return;
    };
    let mut total_registered = 0usize;
    {
        // Single locked region per chunk so we don't pay N times the
        // contention cost for a 256-hint payload. The bottleneck of the
        // worker's read path is `WorkerProxyStore::lookup_workers`, which
        // takes a read lock; bursts of writes don't starve it because
        // parking_lot RwLock is fair.
        let mut map = locality_map.write();
        for hint in &chunk.peer_hints {
            let Some(ref digest_proto) = hint.digest else {
                continue;
            };
            let Ok(digest) = DigestInfo::try_from(digest_proto) else {
                continue;
            };
            for endpoint in &hint.peer_endpoints {
                map.register_blobs(endpoint, &[digest]);
                total_registered += 1;
            }
        }
    }
    // Per-chunk events are repetitive in the hot path (a 1M-hint dispatch
    // = ~3908 chunks). Demote to debug! for the per-chunk cadence; emit
    // an info! once per dispatch on the terminal chunk so journals still
    // record the state-transition "all hints for op_id are in".
    if chunk.is_last {
        info!(
            operation_id = %chunk.operation_id,
            sequence = chunk.sequence,
            hint_count = chunk.peer_hints.len(),
            registrations = total_registered,
            "PeerHintsChunk: terminal chunk applied; locality registrations complete"
        );
    } else {
        debug!(
            operation_id = %chunk.operation_id,
            sequence = chunk.sequence,
            hint_count = chunk.peer_hints.len(),
            registrations = total_registered,
            "PeerHintsChunk: registered hints into worker locality map"
        );
    }
}

struct LocalWorkerImpl<'a, T: WorkerApiClientTrait + 'static, U: RunningActionsManager> {
    config: &'a LocalWorkerConfig,
    // According to the tonic documentation it is a cheap operation to clone this.
    grpc_client: T,
    worker_id: String,
    running_actions_manager: Arc<U>,
    // Number of actions that have been received in `Update::StartAction`, but
    // not yet processed by running_actions_manager's spawn. This number should
    // always be zero if there are no actions running and no actions being waited
    // on by the scheduler.
    actions_in_transit: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
    /// State for periodic BlobsAvailable reporting. None if disabled (no CAS endpoint).
    blobs_available_state: Option<BlobsAvailableState>,
    /// Worker-global locality map shared with `WorkerProxyStore`. When
    /// present, `Update::ChunkedMessage(PeerHints)` arms register hints
    /// directly into this map. None if peer-blob sharing is disabled
    /// (no `cas_server_port`).
    peer_locality_map: Option<SharedBlobLocalityMap>,
    /// Reference to the CAS server shutdown signal for graceful shutdown.
    cas_shutdown_tx: &'a Option<tokio::sync::watch::Sender<bool>>,
    /// #O15 (2026-06-07): semaphore capping detached AC-write inflight
    /// tasks. Cloned in from the outer `LocalWorker` so the cap persists
    /// across scheduler reconnects (each reconnect rebuilds
    /// `LocalWorkerImpl`). See `AC_WRITE_DETACHED_INFLIGHT_CAP`.
    ac_write_semaphore: Arc<Semaphore>,
    /// #O15 (2026-06-07): inflight gauge for detached AC writes. Cloned
    /// from the outer `LocalWorker` (and surfaced via
    /// `Metrics::ac_write_detached_inflight_count`). Incremented when a
    /// permit is acquired; decremented when the spawn body exits (RAII
    /// `InflightGuard`).
    ac_write_detached_inflight_count: Arc<core::sync::atomic::AtomicI64>,

    /// (speculative-prefetch Increment 1) Single-in-flight guard for the
    /// speculative prefetch (G5: ≤1 per worker). `true` = a speculative
    /// fetch is currently running; `false` = idle. Set to `true` before
    /// spawning; the spawned task resets it to `false` on completion
    /// (normal, abort, or TTL). A second `Update::PrefetchInputs` arriving
    /// while this is `true` is dropped + counted as `speculative_prefetch_busy_drop`.
    /// No separate counter — presence (true) IS the busy signal.
    ///
    /// Shared via `Arc` so the spawned task can reset without holding self.
    // CAPPED AT 1: exactly one in-flight speculative fetch per worker; drop
    // the second (G5 / §1.10 confirm-necessity: map-presence = busy signal).
    speculative_prefetch_inflight: Arc<core::sync::atomic::AtomicBool>,
    /// (speculative-prefetch) The DirectoryCache entry-pin guard from the most
    /// recent speculative pre-construct. Held so the pre-warmed entry stays
    /// resident (ref_count > 0, evict-LAST) until the real StartAction adopts it
    /// via a HIT, or the TTL timer drops it (entry → normal LRU / evict-first).
    /// `core::mem::take`n on TTL wake; Drop is synchronous (`ref_count.fetch_sub`)
    /// so the pin releases deterministically on every exit path including
    /// cancellation.
    // UNBOUNDED-OK: single Option, one entry-pin per worker (G5 single-inflight
    // bounds it to 1). The guard holds only an Arc<AtomicUsize> ref_count handle,
    // no owned bytes.
    speculative_prefetch_guard:
        Arc<parking_lot::Mutex<Option<crate::directory_cache::DirectoryCachePinGuard>>>,
}

pub async fn preconditions_met<H: BuildHasher + Sync>(
    precondition_script: Option<String>,
    extra_envs: &HashMap<String, String, H>,
) -> Result<(), Error> {
    let Some(precondition_script) = &precondition_script else {
        // No script means we are always ok to proceed.
        return Ok(());
    };
    // TODO: Might want to pass some information about the command to the
    //       script, but at this point it's not even been downloaded yet,
    //       so that's not currently possible.  Perhaps we'll move this in
    //       future to pass useful information through?  Or perhaps we'll
    //       have a pre-condition and a pre-execute script instead, although
    //       arguably entrypoint already gives us that.

    let maybe_split_cmd = shlex::split(precondition_script);
    let (command, args) = match &maybe_split_cmd {
        Some(split_cmd) => (&split_cmd[0], &split_cmd[1..]),
        None => {
            return Err(make_input_err!(
                "Could not parse the value of precondition_script: '{}'",
                precondition_script,
            ));
        }
    };

    let precondition_process = process::Command::new(command)
        .args(args)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .env_clear()
        .envs(extra_envs)
        .spawn()
        .err_tip(|| format!("Could not execute precondition command {precondition_script:?}"))?;
    let output = precondition_process.wait_with_output().await?;
    let stdout = str::from_utf8(&output.stdout).unwrap_or("");
    trace!(status = %output.status, %stdout, "Preconditions script returned");
    if output.status.code() == Some(0) {
        Ok(())
    } else {
        Err(make_err!(
            Code::ResourceExhausted,
            "Preconditions script returned status {} - {}",
            output.status,
            stdout
        ))
    }
}

/// FL-681: resolve the configured `LocalWorkerConfig.max_concurrent_uploads`
/// to the effective per-call upload fan-out cap fed to `Semaphore::new`
/// in `handle_upload_missing_blobs`.
///
/// `0` is the "unset" sentinel: an absent config field deserializes to
/// `0`, which resolves to the historical hardcoded
/// `o11_probes::MAX_CONCURRENT_UPLOADS` (32) so existing deployed
/// configs keep their fan-out cap. Any non-zero value passes through
/// verbatim.
#[must_use]
pub fn effective_max_concurrent_uploads(configured: usize) -> usize {
    if configured == 0 {
        ::nativelink_util::o11_probes::MAX_CONCURRENT_UPLOADS
    } else {
        configured
    }
}

/// FL-681 fix-up: the single point that turns the resolved per-call
/// fan-out cap into the upload throttle semaphore. `handle_upload_missing_blobs`
/// constructs its semaphore ONLY through this function, so the
/// `Semaphore::new(N)` permit count is provably the resolved
/// `max_concurrent_uploads` and not a hardcoded constant. The fan-out
/// config test asserts `upload_fanout_semaphore(effective_max_concurrent_uploads(cfg))`
/// has exactly the expected `available_permits()` — crossing the
/// resolver → Semaphore seam, which a `Semaphore::new(32)` hardcode
/// mutation inside this function would fail.
#[must_use]
pub fn upload_fanout_semaphore(max_concurrent_uploads: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(max_concurrent_uploads))
}

/// (#FL-688) Per-blob outcome of the backfill upload loop in
/// [`LocalWorkerImpl::handle_upload_missing_blobs`], classified by durability
/// severity so the aggregate tally can feed the correctly-labeled counters.
/// (The re-check `None`/VANISHED case never reaches this enum — those digests
/// are filtered out of `present` before the upload loop and counted separately.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillOutcome {
    /// The blob was uploaded to the server successfully.
    Uploaded,
    /// The upload failed but the digest was re-queued into `failed_slow_writes`
    /// for retry-until-durable (RECOVERABLE — the blob still exists locally).
    Requeued,
    /// The upload failed AND the digest could not be re-queued
    /// (`failed_slow_writes` at cap) — dropped from the local retry path
    /// (IRRECOVERABLE via local retry; the server may still re-request).
    Dropped,
}

impl<'a, T: WorkerApiClientTrait + 'static, U: RunningActionsManager> LocalWorkerImpl<'a, T, U> {
    fn new(
        config: &'a LocalWorkerConfig,
        grpc_client: T,
        worker_id: String,
        running_actions_manager: Arc<U>,
        metrics: Arc<Metrics>,
        blobs_available_state: Option<BlobsAvailableState>,
        peer_locality_map: Option<SharedBlobLocalityMap>,
        cas_shutdown_tx: &'a Option<tokio::sync::watch::Sender<bool>>,
        ac_write_semaphore: Arc<Semaphore>,
        ac_write_detached_inflight_count: Arc<core::sync::atomic::AtomicI64>,
    ) -> Self {
        Self {
            config,
            grpc_client,
            worker_id,
            running_actions_manager,
            // Number of actions that have been received in `Update::StartAction`, but
            // not yet processed by running_actions_manager's spawn. This number should
            // always be zero if there are no actions running and no actions being waited
            // on by the scheduler.
            actions_in_transit: Arc::new(AtomicU64::new(0)),
            metrics,
            blobs_available_state,
            peer_locality_map,
            cas_shutdown_tx,
            ac_write_semaphore,
            ac_write_detached_inflight_count,
            speculative_prefetch_inflight: Arc::new(core::sync::atomic::AtomicBool::new(false)),
            speculative_prefetch_guard: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Upload blobs requested by the server's UploadMissingBlobs message.
    /// Reads from the local fast store and writes to the slow store (server CAS).
    ///
    /// `max_concurrent_uploads` is the per-call fan-out cap (already
    /// resolved from `LocalWorkerConfig.max_concurrent_uploads` via
    /// [`effective_max_concurrent_uploads`] by the caller, so the `0`
    /// sentinel is never seen here).
    async fn handle_upload_missing_blobs(
        running_actions_manager: &Arc<U>,
        digests: Vec<DigestInfo>,
        max_concurrent_uploads: usize,
    ) {
        let Some(cas_store) = running_actions_manager.get_cas_store() else {
            warn!("UploadMissingBlobs: no CAS store available, ignoring");
            return;
        };
        let slow_store = cas_store.slow_store();
        if slow_store
            .inner_store(None::<StoreKey<'_>>)
            .optimized_for(nativelink_util::store_trait::StoreOptimizations::NoopUpdates)
        {
            return;
        }
        // Use the FastSlowStore wrapper (not just `fast_store()`) so reads
        // transparently see mirror_blobs entries — the worker may hold a
        // pinned mirror copy that never landed on disk, and that is the
        // very copy the server is asking us to upload back.
        let cas_store_wrapped: Store = Store::new(cas_store.clone());

        // Check which blobs we actually have locally (disk OR mirror) before
        // uploading. FastSlowStore::has_with_results checks fast_store, the
        // in_flight_slow_writes map, and mirror_blobs.
        let keys: Vec<StoreKey<'_>> = digests.iter().map(|d| StoreKey::from(*d)).collect();
        let mut results = vec![None; keys.len()];
        if let Err(err) = cas_store_wrapped
            .has_with_results(&keys, &mut results)
            .await
        {
            warn!(?err, "UploadMissingBlobs: failed to check local store");
            return;
        }

        let present: Vec<DigestInfo> = digests
            .iter()
            .zip(results.iter())
            .filter_map(|(d, r)| if r.is_some() { Some(*d) } else { None })
            .collect();

        // (#FL-688) An advertised digest that returns `None` at the re-check is
        // VANISHED — the worker advertised the blob (the server only requests
        // what the worker advertised, worker_api_server.rs
        // request_missing_blob_uploads), then lost it (unpinned eviction +
        // mirror drop) and can no longer produce it. Since the server requested
        // it, no copy remains anywhere = sole-copy permanent loss. This is the
        // genuine FL-688 data-loss signal — warned + counted here ONCE for both
        // the all-vanished (present.is_empty) and partial-vanish cases. Nothing
        // downstream can recover a vanished blob (it is gone), so this fires
        // regardless of whether the surviving `present` subset uploads.
        let vanished = digests.len() - present.len();
        if vanished > 0 {
            warn!(
                vanished,
                requested = digests.len(),
                found = present.len(),
                "FL-688 data-loss signal: {vanished} advertised blob(s) VANISHED at reconcile \
                 re-check (no longer present on disk/in-flight/mirror — sole-copy permanent \
                 loss; the server requested them, so no copy remains)"
            );
            ::nativelink_util::o11_probes::reconcile_pin_counters()
                .vanished
                .fetch_add(vanished as u64, Ordering::Relaxed);
        }

        if present.is_empty() {
            // Everything requested vanished (or the request was empty); the
            // vanished signal above already fired. Nothing left to upload.
            if vanished == 0 {
                info!(
                    requested = digests.len(),
                    "UploadMissingBlobs: none of the requested blobs found locally"
                );
            }
            return;
        }

        info!(
            requested = digests.len(),
            found = present.len(),
            "UploadMissingBlobs: uploading blobs to server"
        );

        // #85 P1 (2026-06-08): per-call `Semaphore::new(...)` (pre-#85
        // semantics, restored). The `handle_upload_missing_blobs` call
        // has two spawn sites (`:2520` reconnect retry + `:2777`
        // server-driven push) that CAN overlap, so a process-singleton
        // would narrow the effective cap from N×cap → cap — that is an
        // architectural change requiring explicit sign-off. The
        // observation-only counters in `upload_inflight_counters()` SUM
        // across all concurrent calls so the aggregate inflight +
        // waiters is still scrapeable.
        //
        // FL-681: the permit count is operator-tunable via
        // `LocalWorkerConfig.max_concurrent_uploads`; `max_concurrent_uploads`
        // here is the already-resolved value (defaults to
        // `MAX_CONCURRENT_UPLOADS = 32` when unset).
        // CAPPED AT max_concurrent_uploads: per-call upload fan-out is
        // bounded by this many concurrent in-flight uploads; over-cap
        // uploads await a permit (bounded backpressure, never buffered).
        // Constructed via `upload_fanout_semaphore` (the single
        // resolver→Semaphore seam) so the permit count is provably the
        // resolved config value, not a hardcode (FL-681 fix-up).
        let upload_sem = upload_fanout_semaphore(max_concurrent_uploads);
        let upload_counters =
            ::nativelink_util::o11_probes::upload_inflight_counters();

        let mut uploads: FuturesUnordered<_> = present
            .iter()
            .map(|&digest| {
                let cas_store_wrapped = cas_store_wrapped.clone();
                let slow_store = slow_store.clone();
                let upload_sem = Arc::clone(&upload_sem);
                // #FL-688 W4 retry-until-durable: clone the FSS handle so the
                // per-blob Err arm can re-queue the digest into the shared
                // `failed_slow_writes` set instead of dropping it.
                let cas_store = cas_store.clone();
                // #FL-688 (B): set IS_WORKER_REQUEST=true for the whole per-blob
                // upload so GrpcStore stamps `x-nativelink-worker` on the wire
                // (GrpcStore::write :1801 / ::update_action_result gate the header
                // on this task-local). The server's bytestream G1 carve-out (A,
                // bytestream_server.rs:3364) and the batch carve-out
                // (cas_server.rs:458) BOTH key on is_worker; with the header absent
                // the upload arrives is_worker=false and the server skips it (G1) /
                // ack-gates it (batch) — never persisting the worker's sole-copy
                // backfill blob, so the server re-requests it every BlobsAvailable
                // tick forever.
                //
                // Production chain is SPAWN-FREE so this scope alone carries the
                // header: `slow_store` here is the WorkerProxyStore-wrapped slow
                // tier (local_worker.rs:5758, when cas_server_port.is_some()). WPS
                // has no `update_oneshot` override → StoreDriver default
                // `update_oneshot` (store_trait.rs:1215) = inline `try_join!(send,
                // self.update(..))` → `WPS::update` (:4638) inline passthrough →
                // `GrpcStore::update` (:3475) which for a <CHUNK_SIZE blob falls
                // through to the legacy ByteStream `write` (reads IS_WORKER_REQUEST
                // inline). It NEVER reaches `GrpcStore::update_oneshot`'s
                // BatchUpdateBlobs coalesce-queue spawn (the only spawn that would
                // strip the task-local) — so no GrpcStore-side change is needed.
                //
                // Per-FUTURE scoping (not wrapping the drain). CORRECT ONLY
                // because the chain above is spawn-free: tokio task-locals do
                // NOT propagate across `tokio::spawn`. If any link (here, WPS, or
                // GrpcStore) is later refactored to spawn the upload,
                // IS_WORKER_REQUEST is SILENTLY lost → the wire header drops →
                // FL-688 re-opens. Any new spawn MUST re-establish the scope
                // INSIDE the spawned task (see batch_read_coalescer.rs:528).
                // is_worker only — backfill is a worker upload, not a mirror push
                // (do NOT set IS_MIRROR_REQUEST). Mirrors the
                // `IS_WORKER_REQUEST.scope(captured, fut)` pattern at
                // batch_read_coalescer.rs:528.
                IS_WORKER_REQUEST.scope(true, async move {
                    let _permit = upload_counters.acquire(&upload_sem).await;
                    // Use in-memory transfer for small blobs, streaming for
                    // large ones to avoid OOM on multi-GB blobs. Reads go
                    // through the FastSlowStore wrapper so mirror_blobs
                    // entries are visible.
                    const STREAMING_THRESHOLD: u64 = 1024 * 1024; // 1 MiB
                    let result = if digest.size_bytes() <= STREAMING_THRESHOLD {
                        match cas_store_wrapped.get_part_unchunked(digest, 0, None).await {
                            Ok(data) => slow_store.update_oneshot(digest, data).await,
                            Err(err) => Err(err),
                        }
                    } else {
                        let (tx, rx) = make_buf_channel_pair();
                        // Phase-tagged tracing — same instrumentation pattern
                        // as RunningActionsManagerImpl::spawn_upload_to_remote.
                        // Names which half of the streaming upload wedges so a
                        // 30s+ stall on UploadMissingBlobs surfaces the
                        // specific phase (fast read vs. gRPC send) in the log.
                        const SLOW_PHASE_WARN: Duration = Duration::from_secs(5);
                        let upload_phase_start = std::time::Instant::now();
                        let read_fut = async {
                            let phase_start = std::time::Instant::now();
                            let res = cas_store_wrapped.get(digest, tx).await;
                            let elapsed = phase_start.elapsed();
                            if elapsed >= SLOW_PHASE_WARN {
                                warn!(
                                    ?digest,
                                    size_bytes = digest.size_bytes(),
                                    elapsed_ms = elapsed.as_millis() as u64,
                                    "UploadMissingBlobs: slow fast-store read phase",
                                );
                            }
                            res
                        };
                        let write_fut = async {
                            let phase_start = std::time::Instant::now();
                            let res = slow_store
                                .update(digest, rx, UploadSizeInfo::ExactSize(digest.size_bytes()))
                                .await;
                            let elapsed = phase_start.elapsed();
                            if elapsed >= SLOW_PHASE_WARN {
                                warn!(
                                    ?digest,
                                    size_bytes = digest.size_bytes(),
                                    elapsed_ms = elapsed.as_millis() as u64,
                                    "UploadMissingBlobs: slow slow-store write phase (gRPC send)",
                                );
                            }
                            res
                        };
                        let (read_res, write_res) = tokio::join!(read_fut, write_fut);
                        let total_elapsed = upload_phase_start.elapsed();
                        if total_elapsed >= SLOW_PHASE_WARN {
                            warn!(
                                ?digest,
                                size_bytes = digest.size_bytes(),
                                total_elapsed_ms = total_elapsed.as_millis() as u64,
                                read_ok = read_res.is_ok(),
                                write_ok = write_res.is_ok(),
                                "UploadMissingBlobs: slow streaming upload (combined)",
                            );
                        }
                        if write_res.is_ok() {
                            Ok(())
                        } else {
                            // `StoreLike::update` now yields the byte count
                            // (`Result<u64>`); this backfill path only cares
                            // about success/failure, so discard it to keep the
                            // combined result `Result<()>`.
                            read_res.merge(write_res.map(|_| ()))
                        }
                    };
                    match result {
                        Ok(()) => BackfillOutcome::Uploaded,
                        Err(err) => {
                            // #FL-688 W4 retry-until-durable: a failed
                            // worker→server backfill push MUST NOT be
                            // dropped. Re-queue the digest into the shared
                            // `failed_slow_writes` set (+ re-pin the fast
                            // tier) so the reconnect drainer
                            // (`drain_failed_digests` → this same handler)
                            // re-attempts it. Pre-fix this arm `warn!`-logged
                            // and dropped the blob, so the server re-requested
                            // the identical missing set every BlobsAvailable
                            // tick and the count never decreased (the stuck
                            // loop). The re-queue is bounded by
                            // `FAILED_SLOW_WRITES_MAX`; `requeue_failed_push`
                            // returns false only on the over-cap rejection.
                            if cas_store.requeue_failed_push(digest) {
                                // RECOVERABLE: the blob still exists locally
                                // and is re-queued for retry-until-durable.
                                // NOT yet loss — info!, not the data-loss WARN.
                                info!(
                                    ?digest,
                                    ?err,
                                    "UploadMissingBlobs: failed to transfer blob; \
                                     re-queued into failed_slow_writes for retry-until-durable"
                                );
                                BackfillOutcome::Requeued
                            } else {
                                // IRRECOVERABLE via the local retry path: the
                                // digest is DROPPED from the retry-until-durable
                                // set (failed_slow_writes at cap) — only the
                                // server's next BlobsAvailable re-request can
                                // recover it (and only while the blob still
                                // exists locally). Labeled with the FL-688
                                // data-loss signal so the fleet alarm keys on it.
                                warn!(
                                    ?digest,
                                    ?err,
                                    "FL-688 data-loss signal: failed to transfer blob AND \
                                     failed_slow_writes is at cap — digest NOT re-queued; \
                                     server BlobsAvailable re-request is the remaining retry path"
                                );
                                BackfillOutcome::Dropped
                            }
                        }
                    }
                })
            })
            .collect();

        let mut uploaded = 0usize;
        let mut requeued = 0usize;
        let mut dropped = 0usize;
        while let Some(outcome) = uploads.next().await {
            match outcome {
                BackfillOutcome::Uploaded => uploaded += 1,
                BackfillOutcome::Requeued => requeued += 1,
                BackfillOutcome::Dropped => dropped += 1,
            }
        }

        info!(
            uploaded,
            requeued,
            dropped,
            vanished,
            total = present.len(),
            "UploadMissingBlobs: backfill complete"
        );
        // (#FL-688 log-miscalibration fix) The upload-outcome severity split.
        // The server ONLY requests blobs the worker advertised
        // (worker_api_server.rs request_missing_blob_uploads), so every
        // classification is about a blob the worker CLAIMED to hold. VANISHED
        // (the genuine irrecoverable sole-copy loss) is warned + counted BEFORE
        // this loop — those digests were dropped from `present` at the re-check
        // and never reached the upload. Here we split the upload OUTCOMES:
        //
        // DROPPED is irrecoverable via the local retry path (failed_slow_writes
        // at cap) — a WARN-level data-loss signal. The per-digest arm above
        // already warned per blob; the counter carries the batch count.
        if dropped > 0 {
            ::nativelink_util::o11_probes::reconcile_pin_counters()
                .dropped
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
        // REQUEUED is RECOVERABLE (retry-until-durable) — info-level, not a
        // data-loss alarm. The counter lets operators watch the slow-tier
        // backpressure rate without it reading as loss.
        if requeued > 0 {
            ::nativelink_util::o11_probes::reconcile_pin_counters()
                .requeued
                .fetch_add(requeued as u64, Ordering::Relaxed);
        }
    }

    /// Starts a background spawn/thread that will send a message to the server every `timeout / 2`.
    async fn start_keep_alive(&self) -> Result<(), Error> {
        // According to tonic's documentation this call should be cheap and is the same stream.
        let mut grpc_client = self.grpc_client.clone();

        loop {
            let timeout = self
                .config
                .worker_api_endpoint
                .timeout
                .unwrap_or(DEFAULT_ENDPOINT_TIMEOUT_S);
            // We always send 2 keep alive requests per timeout. Http2 should manage most of our
            // timeout issues, this is a secondary check to ensure we can still send data.
            sleep(Duration::from_secs_f32(timeout / 2.)).await;
            let load = get_cpu_load_pct();
            let p_load = get_p_core_load_pct();
            let e_load = get_e_core_load_pct();
            debug!("KeepAlive cpu_load_pct={load} p_core={p_load} e_core={e_load}");
            if let Err(e) = grpc_client
                .keep_alive(KeepAliveRequest {
                    cpu_load_pct: load,
                    p_core_load_pct: p_load,
                    e_core_load_pct: e_load,
                })
                .await
            {
                return Err(make_err!(
                    Code::Internal,
                    "Failed to send KeepAlive in LocalWorker : {:?}",
                    e
                ));
            }
        }
    }

    /// (FL-688 v3 §3.8 — drain-on-ack flip) Per-tick REPLAY READER: the
    /// consumer of [`BlobsAvailableResendBuffer`]. Retransmits every
    /// still-unacked delta chunk verbatim (same `(broadcast_id, sequence,
    /// worker_instance_token)`) so the server's accumulator is idempotent
    /// under the resend and the worker's drain-on-ack key matches.
    ///
    /// Drives the SAME `chunked_message` wire path as the live delta send,
    /// so the server's `merge_chunk_outcome` ACCEPTs the replay and emits a
    /// `BlobsAvailableAck`; the matching `handle_blobs_available_ack` then
    /// drops the slot. Until that ack arrives the chunk is RE-sent every
    /// tick — drain-on-ACK, not drain-on-SEND.
    ///
    /// A send error propagates so the caller surfaces it (the connection is
    /// dropping; the reconnect's full snapshot will re-converge and the
    /// buffer is cleared then). On success NO buffer mutation happens here:
    /// the chunk stays buffered until its ack lands (which may have crossed
    /// this in-flight resend — idempotent).
    async fn replay_unacked_chunks(
        grpc_client: &mut T,
        state: &BlobsAvailableState,
    ) -> Result<(), Error> {
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            ChunkedMessage, chunked_message,
        };
        // Snapshot under the lock, then release it before awaiting (no lock
        // across `.await`).
        let unacked = state.unacked_chunks_for_replay();
        if unacked.is_empty() {
            return Ok(());
        }
        let replay_count = unacked.len();
        for chunk in unacked {
            let broadcast_id = chunk.broadcast_id;
            let sequence = chunk.sequence;
            let envelope = ChunkedMessage {
                payload: Some(chunked_message::Payload::BlobsAvailable(chunk)),
            };
            if let Err(err) = grpc_client.chunked_message(envelope).await {
                warn!(
                    target: "nativelink::blobs_available_ack",
                    ?err,
                    broadcast_id,
                    sequence,
                    replay_count,
                    "failed to retransmit unacked BlobsAvailable delta chunk; \
                     propagating to trigger reconnect (full snapshot re-converges)"
                );
                return Err(err);
            }
        }
        info!(
            target: "nativelink::blobs_available_ack",
            replay_count,
            "retransmitted unacked BlobsAvailable delta chunks (drain-on-ack replay)"
        );
        Ok(())
    }

    /// (FL-688 v3 §3.8 part 2) Send a POST-ACTION output-digest delta
    /// through the ACKED/buffered/replayed chunked path.
    ///
    /// When `state` is `Some`, the notification is chunked (a small
    /// post-action delta is one terminal chunk), each chunk is sent via
    /// `chunked_message` AND buffered for drain-on-ack via
    /// `state.buffer_delta_chunk` — so a lost post-action delta is
    /// retransmitted by [`Self::replay_unacked_chunks`] on the next
    /// periodic tick until the server acks it. The broadcast_id and
    /// worker_instance_token come from the SAME `state` the periodic delta
    /// path uses, so the server keys this broadcast identically.
    ///
    /// When `state` is `None` (worker has no fast-store BlobsAvailable
    /// reporting), falls back to the legacy fire-and-forget
    /// `blobs_available()` send — unchanged behaviour for that
    /// configuration.
    ///
    /// This is a DELTA, never a full snapshot, so it always chunks (never
    /// clears the buffer). An over-cap buffer accumulation sets the
    /// force-full-snapshot flag (the periodic path's next tick re-converges)
    /// exactly as the periodic delta path does.
    async fn send_post_action_blobs_available_delta(
        grpc_client: &mut T,
        state: Option<&BlobsAvailableState>,
        notification: BlobsAvailableNotification,
    ) -> Result<(), Error> {
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            ChunkedMessage, chunked_message,
        };
        use nativelink_util::blobs_available_chunking::{
            BLOBS_AVAILABLE_PER_CHUNK, chunk_blobs_available,
        };

        let Some(state) = state else {
            // No resend state on this worker — fall back to the raw send.
            return grpc_client.blobs_available(notification).await;
        };

        let broadcast_id = state.next_broadcast_id.fetch_add(1, Ordering::Relaxed);
        let worker_instance_token = state.worker_instance_token;
        let chunks = match chunk_blobs_available(
            notification,
            broadcast_id,
            worker_instance_token,
            String::new(),
            BLOBS_AVAILABLE_PER_CHUNK,
        ) {
            Ok(chunks) => chunks,
            Err(reason) => {
                // A single action's output-digest delta exceeding the
                // per-broadcast chunk cap is implausible, but if it ever
                // happens, log loudly and skip — the periodic full-snapshot
                // path re-advertises the worker's whole inventory anyway.
                warn!(
                    target: "nativelink::blobs_available_ack",
                    reason,
                    broadcast_id,
                    "post-action BlobsAvailable chunker rejected: output-digest delta too large \
                     for one broadcast; skipping (periodic snapshot re-advertises)"
                );
                return Ok(());
            }
        };
        for chunk in chunks {
            // Buffer the delta chunk (clone) for drain-on-ack BEFORE moving
            // it into the send envelope, mirroring the periodic delta path.
            let buffered_delta = chunk.clone();
            let envelope = ChunkedMessage {
                payload: Some(chunked_message::Payload::BlobsAvailable(chunk)),
            };
            grpc_client.chunked_message(envelope).await?;
            if state.buffer_delta_chunk(buffered_delta) {
                warn!(
                    target: "nativelink::blobs_available_ack",
                    broadcast_id,
                    cap = BLOBS_AVAILABLE_RESEND_MAX_CHUNKS,
                    "post-action BlobsAvailable resend buffer over cap; cleared + forcing a full \
                     snapshot next tick (server not acking deltas — partition?)"
                );
            }
        }
        Ok(())
    }

    /// Sends a periodic BlobsAvailable notification.
    /// - First tick: full snapshot of all digests with timestamps (scans store once).
    ///   Also sends a full subtree snapshot with ALL subtree digests.
    /// - Subsequent ticks: delta from callback-accumulated changes (no scan).
    ///   Sends delta-encoded subtree changes (added/removed).
    async fn send_periodic_blobs_available(
        grpc_client: &mut T,
        state: &BlobsAvailableState,
        running_actions_manager: &Arc<U>,
        is_first: bool,
    ) -> Result<(), Error> {
        // (FL-688 v3 §3.8) Over-cap promotion: if the resend buffer
        // overflowed on a prior tick, this tick is promoted to a full
        // snapshot (which supersedes every dropped delta — lossless). The
        // flag is taken (cleared) here so a single overflow promotes
        // exactly one tick. ORing into `is_first` reuses the existing
        // full-snapshot path; the flag itself is set by
        // `buffer_delta_chunk` when the buffer overflows.
        let is_first = is_first
            || state
                .blobs_available_force_full_snapshot
                .swap(false, Ordering::Relaxed);

        // (FL-688 v3 §3.8 — drain-on-ack flip, the per-tick REPLAY READER)
        // Retransmit every still-unacked delta chunk BEFORE computing this
        // tick's new delta and BEFORE the skip-gate. This is the consumer
        // of the resend buffer (Stage 2 only RECORDED + DROPPED-on-ack —
        // it had no reader, so a lost delta on a live connection stayed
        // buffered forever and the server's locality view stayed stale
        // until a reconnect). The invariant: a delta is RETAINED until the
        // server acks it; a lost send is RETRANSMITTED on the next tick,
        // converging the server's locality view WITHOUT a reconnect.
        //
        // Riding this existing per-tick call (NOT a new periodic timer)
        // keeps the no-new-timer constraint: the tick is already woken by
        // the change-`Notify` / mirror-`Notify` / AC-`Notify` / backstop
        // `select!` in `run`. Placed BEFORE the skip-gate so an OTHERWISE-
        // IDLE worker (no new blob changes) still retransmits its unacked
        // backlog — the exact stale-forever scenario.
        //
        // Skipped on a full snapshot (`is_first`): the snapshot supersedes
        // every buffered delta and the buffer is cleared on the snapshot
        // send below, so replaying stale deltas first would be redundant.
        // This touches ONLY the BlobsAvailable locality-delta path; it does
        // not interact with the slow-write watchdog (writes are never
        // aborted) or the BIS ack-gate.
        if !is_first {
            Self::replay_unacked_chunks(grpc_client, state).await?;
        }

        // (A1 fix + fix-up F2; FL-688 v3 Stage A) Apply the reconnect
        // memo-reset path at the function head: reconnect-clear
        // (`is_first=true`) wipes the AC-pin memo so the next delta replays the
        // full snapshot. Stage A removed the periodic full-snapshot heartbeat
        // (the every-Nth-tick forced memo clear); the out-of-band server-
        // mutation classes it guarded re-converge via reconnect + the Stage-2B
        // replay-until-acked reader + the over-cap force-snapshot EVENT (above).
        // See [`apply_periodic_tick_memo_resets`].
        let _ = apply_periodic_tick_memo_resets(&state.last_sent_ac_pin_set, is_first);
        // (#locality-map-drift) `evicted_blob_infos` = the ts-carrying eviction
        // list (BlobDigestInfo). Below we DUAL-EMIT: the legacy `evicted_digests`
        // (bare Digest, no ts) is derived from it so an old server still reads
        // the eviction, and a new server prefers `evicted_blob_infos`.
        let (digest_infos, evicted_blob_infos, pinned_mirror_digests) = if is_first {
            // Full snapshot: scan everything once.
            let all = state.fs_store.get_all_digests_with_timestamps();
            // Drain any changes that accumulated during startup.
            drop(state.tracker.swap());

            // (#locality-map-drift) The full snapshot enumerates residency,
            // not per-mutation deltas, and is applied server-side POST-wipe
            // (`remove_endpoint` on reconnect) — so it carries ts 0 (unset).
            // The first real ts-carrying delta refreshes each entry's stamp.
            let infos: Vec<BlobDigestInfo> = all
                .iter()
                .map(|(digest, _ts)| bdi_with_stamp(*digest, Stamp::default()))
                .collect();

            // Mirror digests: drain deltas FIRST, then take the snapshot
            // (atomically, under both mirror locks). If we snapshotted first
            // and then drained, a concurrent `remove_mirror_blobs` could land
            // between the two calls — its `removed` delta would be discarded
            // by the snapshot reset and the digest would never reach the
            // server's locality map cleanup. The snapshot covers all live
            // pins at the post-drain moment; drained `removed` deltas are
            // merged into `evicted_digests` so the locality map is cleaned.
            // (#locality-map-drift) Mirror digests carry no per-mutation logical
            // clock (they are in-memory server-pushed pins, drained
            // synchronously — not subject to the moka async-reorder bug), so
            // they ride ts 0: a mirror-removal@0 evicts a pure-mirror entry
            // (registered@0) via the equal-ts ABSENT≻PRESENT tie-break, but is
            // correctly SUPPRESSED for a digest the FS store still holds
            // (registered@(be,c) by a real delta) — the worker still has it on
            // disk.
            let (mirror_evicted_protos, mirror_pinned_protos) =
                if let Some(ref fss) = state.cas_server_fss {
                    let (mc, snap) = fss.snapshot_and_reset_mirror_changes();
                    let evicted: Vec<BlobDigestInfo> = mc
                        .removed
                        .into_iter()
                        .map(|d| bdi_with_stamp(d, Stamp::default()))
                        .collect();
                    let pinned: Vec<_> = snap.into_iter().map(|d| d.into()).collect();
                    (evicted, pinned)
                } else {
                    (Vec::new(), Vec::new())
                };

            (infos, mirror_evicted_protos, mirror_pinned_protos)
        } else {
            // Delta: swap out accumulated per-digest LWW changes. Each entry is
            // the winning (state, stamp) for its digest this window. PRESENT →
            // digest_infos, ABSENT → evicted_digests; both carry the winning
            // `(boot_epoch, counter)` stamp so the server applies the same LWW
            // and suppresses a re-ordered stale eviction of a re-admitted blob.
            let changes = state.tracker.swap();
            let mut infos: Vec<BlobDigestInfo> = Vec::new();
            let mut evicted_protos: Vec<BlobDigestInfo> = Vec::new();
            for (digest, state, stamp) in changes {
                match state {
                    BlobState::Present => infos.push(bdi_with_stamp(digest, stamp)),
                    BlobState::Absent => evicted_protos.push(bdi_with_stamp(digest, stamp)),
                }
            }

            // Mirror delta: drain → send `added` as `pinned_mirror_digests`
            // and merge `removed` into `evicted_digests` (ts 0; see the
            // full-snapshot mirror note above) so the server cleans up
            // pure-mirror locality entries we no longer hold.
            let mirror_added_protos: Vec<_> = if let Some(ref fss) = state.cas_server_fss {
                let mc = fss.drain_mirror_changes();
                for d in mc.removed {
                    evicted_protos.push(bdi_with_stamp(d, Stamp::default()));
                }
                mc.added.into_iter().map(|d| d.into()).collect()
            } else {
                Vec::new()
            };

            (infos, evicted_protos, mirror_added_protos)
        };

        // (FL-688 v3 Stage A) The FL-681 Follow-up B periodic pending-BIS CAS
        // re-advertise was REMOVED with the heartbeat tick that drove it. A
        // digest whose `mark_stable` was missed now re-converges via the
        // reconnect full snapshot (which enumerates the whole store, including
        // every indefinite pin) + the Stage-2B replay-until-acked reader (the
        // `added` delta that pinned it is retransmitted until the server acks)
        // + the over-cap force-snapshot EVENT — all event-driven, no timer.

        // Collect subtree delta or full snapshot.
        let (
            cached_directory_digests,
            added_subtree_digests,
            removed_subtree_digests,
            is_full_subtree_snapshot,
        ) = if is_first {
            // Full subtree snapshot: send ALL subtree digests in cached_directory_digests.
            // Also drain any pending changes accumulated during startup.
            drop(running_actions_manager.take_pending_subtree_changes().await);
            let all_subtrees = running_actions_manager.all_subtree_digests().await;
            let all_subtree_protos = all_subtrees.into_iter().map(|d| d.into()).collect();
            (all_subtree_protos, Vec::new(), Vec::new(), true)
        } else {
            // Delta: take pending subtree changes.
            let (added, removed) = running_actions_manager.take_pending_subtree_changes().await;
            let added_protos = added.into_iter().map(|d| d.into()).collect();
            let removed_protos = removed.into_iter().map(|d| d.into()).collect();
            (Vec::new(), added_protos, removed_protos, false)
        };

        let new_or_touched_count = digest_infos.len();
        let evicted_count = evicted_blob_infos.len();
        // (#locality-map-drift) INVARIANT: the legacy `evicted_digests` (bare
        // Digest, no ts) is DERIVED-FROM `evicted_blob_infos` (filter_map its
        // digest) — NEVER populate it independently. The wire-skew safety proof
        // (old server reads field 4, new server reads tag 24, both are the SAME
        // eviction set) depends on the two lists carrying identical digests in
        // identical order. An OLD server (pre-tag-24) reads this from field 4; a
        // NEW server prefers the ts-carrying `evicted_blob_infos`.
        let evicted_digests: Vec<_> = evicted_blob_infos
            .iter()
            .filter_map(|bdi| bdi.digest.clone())
            .collect();
        let cached_dir_count = cached_directory_digests.len();
        let added_subtree_count = added_subtree_digests.len();
        let removed_subtree_count = removed_subtree_digests.len();
        let pinned_mirror_count = pinned_mirror_digests.len();

        // Build the AC pin slice (proto field 17). Hard-partitioned from
        // the CAS pin slice (field 16): the AC entries flow into a
        // dedicated server-side `AcPinRegistry`, NOT the CAS-shared
        // `BlobLocalityMap`, so they cannot weaponize CAS upload-skip
        // short-circuits even on action_digest collisions. The snapshot
        // method filters by store_id; AC entries on a different store_id
        // would never reach this loop anyway under the type-system
        // invariants on `AcMirrorTarget`.
        //
        // (A1 fix + probe #7) Take the snapshot, then compute the delta
        // against `state.last_sent_ac_pin_set` so the skip-gate fires
        // when AC pins exist but the set is unchanged tick-to-tick.
        // Pre-fix the gate read `pinned_ac_mirror_count == 0`, which
        // failed permanently once any AC pin landed in steady state →
        // 100 ms re-broadcast of an unchanged snapshot → empty-tick
        // storm on the server's `handle_blobs_available`. `ac_pin_scan`
        // wraps the snapshot call so we can attribute scan cost.
        let ac_pin_scan_start = Instant::now();
        let current_ac_pin_digests: Vec<DigestInfo> = state
            .ac_mirror_target
            .as_ref()
            .map(|target| {
                target
                    .fss
                    .dispatched_ac_pin_snapshot_for_store(target.store_id.as_ref())
            })
            .unwrap_or_default();
        let ac_pin_scan_elapsed_us = ac_pin_scan_start.elapsed().as_micros() as u64;
        let pinned_ac_mirror_count = current_ac_pin_digests.len();

        // Compute add/remove vs last successfully-sent set.
        let current_ac_pin_set: HashSet<DigestInfo> =
            current_ac_pin_digests.iter().copied().collect();
        let (ac_pin_added_count, ac_pin_removed_count) = {
            let last = state.last_sent_ac_pin_set.lock();
            compute_ac_pin_delta_counts(&current_ac_pin_set, &*last)
        };
        let ac_pin_delta_empty = ac_pin_added_count == 0 && ac_pin_removed_count == 0;

        // Skip sending if there are truly no changes at all.
        if should_skip_blobs_available_tick(
            is_first,
            new_or_touched_count,
            evicted_count,
            added_subtree_count,
            removed_subtree_count,
            pinned_mirror_count,
            ac_pin_delta_empty,
        ) {
            state
                .blobs_available_skipped_counter
                .fetch_add(1, Ordering::Relaxed);
            trace!(
                pinned_ac_mirror_count,
                ac_pin_scan_elapsed_us,
                "BlobsAvailable: no changes since last tick, skipping"
            );
            return Ok(());
        }

        // Build the wire entries from the digests captured above.
        let pinned_ac_mirror_entries: Vec<MirrorPinEntry> = state
            .ac_mirror_target
            .as_ref()
            .map(|target| {
                current_ac_pin_digests
                    .iter()
                    .map(|digest| MirrorPinEntry {
                        digest: Some((*digest).into()),
                        store_id: target.store_id.to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let load = get_cpu_load_pct();
        let p_load = get_p_core_load_pct();
        let e_load = get_e_core_load_pct();
        // (FL-681) Snapshot the local CAS FilesystemStore's indefinite-pin
        // saturation at emit time. This is the SAME store the worker-side
        // admission gate (`running_actions_manager.rs` `create_and_add_action`)
        // checks; reporting it lets the scheduler's matcher skip a saturated
        // worker proactively rather than re-NAK-spinning it. Cheap: one relaxed
        // atomic load + compare, no lock, no await (`moka_evicting_map.rs`).
        let indefinite_pin_saturated = state.fs_store.indefinite_pin_saturated();
        // Host memory pressure, read from the sampler-thread atomics (same
        // cheap relaxed-load pattern as cpu_load_pct). `memory_pressure_level`
        // is the MiB-below-free-floor magnitude (observability + the server's
        // least-pressured fail-open ranking); swap-used is the coarse swap
        // LEVEL gauge. `memory_pressured` is the coarse 1-bit gate verdict the
        // matcher uses for a PROACTIVE skip (advisory only — the authoritative
        // NAK reads the local atomic, not this wire boolean, design §2).
        let swap_used_bytes = get_swap_used_bytes();
        let memory_pressure_level = get_memory_pressure_level();
        let memory_pressured = swap_gate_pressured();
        // (F4) Physical disk pressure on the CAS/work_directory volume. The
        // wire `disk_pressured` is the SAMPLER's coarse verdict only (advisory
        // matcher hint); the authoritative StartAction NAK additionally
        // consults the statvfs fallback on a stale sampler. `available_disk_bytes`
        // is the free-bytes gauge (observability + fail-open ranking).
        let available_disk_bytes = get_available_disk_bytes();
        let disk_pressured = disk_gate_sampler_pressured();
        // (#obs-tuning) OBSERVABILITY-ONLY: decayed p95 cold dir-cache construct
        // latency (ms) from the global DirCacheCounters. Rides chunk 0; LOGGED
        // by the scheduler for T_SETUP tuning; not a routing input.
        let construct_latency_ms_p95 = get_construct_latency_ms_p95();
        debug!("BlobsAvailable cpu_load_pct={load} p_core={p_load} e_core={e_load} indefinite_pin_saturated={indefinite_pin_saturated} swap_used_bytes={swap_used_bytes} memory_pressure_level={memory_pressure_level} memory_pressured={memory_pressured} available_disk_bytes={available_disk_bytes} disk_pressured={disk_pressured} construct_latency_ms_p95={construct_latency_ms_p95}");
        let notification = BlobsAvailableNotification {
            worker_cas_endpoint: state.cas_endpoint.clone(),
            digests: Vec::new(),
            is_full_snapshot: is_first,
            evicted_digests,
            // (#locality-map-drift) DUAL-EMIT the ts-carrying eviction list.
            evicted_blob_infos,
            digest_infos,
            cpu_load_pct: load,
            cached_directory_digests,
            added_subtree_digests,
            removed_subtree_digests,
            is_full_subtree_snapshot,
            p_core_load_pct: p_load,
            e_core_load_pct: e_load,
            pinned_mirror_digests,
            // Mirror capacity report (review #1): server's picker uses
            // these to filter peers that cannot fit a blob BEFORE
            // consuming the source stream. `(0, 0)` for workers with
            // no CAS server / mirror store ⇒ picker treats as unknown
            // and disables the filter for this endpoint.
            mirror_used_bytes: state
                .cas_server_fss
                .as_ref()
                .map_or(0, |fss| fss.mirror_blobs_used_bytes()),
            mirror_max_bytes: state
                .cas_server_fss
                .as_ref()
                .map_or(0, |fss| fss.mirror_blobs_max_bytes()),
            // Field 16 (task #168 item 5): the dispatcher-pushed
            // pin snapshot keyed by (store_id, digest). Iterates the
            // FastSlowStore's `dispatched_mirror_pins` BTreeMap so the
            // order is sorted by `store_id` ASCII (then by DigestInfo)
            // — the precondition for the server's binary-search
            // self-filter in `EphemeralServerSidePin::observe_pinned_mirror_ack`.
            // Empty when the dispatcher has pushed nothing OR when
            // there is no `cas_server_fss` on this worker.
            //
            // CAS-only by construction: the snapshot iterates the CAS
            // FSS's pin map, which contains exactly the CAS dispatcher
            // pins (`insert_dispatched_mirror_blob`). AC pins live on
            // a different `FastSlowStore` instance (the AC FSS) and
            // ride field 17 below; the two slices CANNOT overlap and
            // CAS readers consuming this field cannot see AC entries.
            pinned_mirror_entries: state
                .cas_server_fss
                .as_ref()
                .map(|fss| {
                    fss.dispatched_mirror_pin_snapshot()
                        .into_iter()
                        .map(|(store_id, digest)| MirrorPinEntry {
                            digest: Some(digest.into()),
                            store_id: store_id.to_string(),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            // AC pin snapshot, fed from a SEPARATE `FastSlowStore`
            // instance (the AC FSS). HARD-PARTITIONED from
            // `pinned_mirror_entries`: the server registers it in the
            // dedicated `AcPinRegistry` — never the CAS-shared
            // `BlobLocalityMap` — because `action_digest` IS by REAPI
            // design the same digest as the Action proto in CAS, so
            // routing AC pins through the locality map would cause CAS
            // upload short-circuits to silently skip uploads of the
            // Action proto bytes.
            pinned_ac_mirror_entries,
            // (FL-681) Snapshot taken above from the local CAS FilesystemStore.
            indefinite_pin_saturated,
            // Host memory pressure (read above). Periodic heartbeat carries
            // the authoritative sampler values + the coarse gate verdict.
            swap_used_bytes,
            memory_pressure_level,
            memory_pressured,
            // (F4) Physical disk pressure on the CAS/work_directory volume,
            // read from the sampler-thread atomics (same cheap relaxed-load
            // pattern). `available_disk_bytes` is the free-bytes magnitude
            // (observability + the server's least-pressured fail-open ranking);
            // `disk_pressured` is the coarse 1-bit gate verdict the matcher uses
            // for a PROACTIVE skip (advisory only — the authoritative NAK reads
            // the local atomic + statvfs fallback, not this wire boolean).
            available_disk_bytes,
            disk_pressured,
            // (#obs-tuning) OBSERVABILITY-ONLY cold-construct latency (p95 ms).
            construct_latency_ms_p95,
        };

        // (#99) Partition into bounded `BlobsAvailableChunk` envelopes and
        // send each via the unified `Update::ChunkedMessage` arm. The
        // server's per-broadcast accumulator buffers chunks and commits
        // atomically on `is_last=true` (Path A semantics).
        //
        // (FL-688 v3 §3.8, part 2 — small-delta coverage) A DELTA is
        // ALWAYS routed through the chunked path so it rides the
        // ACKED/buffered/replayed channel, even when it's small (the
        // common case: most ticks carry < 100 entries). The legacy
        // single-message `blobs_available()` send is fire-and-forget — the
        // server emits NO ack for it (`worker_api_server.rs` non-chunked
        // arm), so a lost small delta would otherwise be the orphaned-
        // replica hole for the COMMON case. The chunker produces exactly
        // one terminal chunk for a small payload (one extra envelope
        // wrapper), and the server reassembles before `handle_blobs_available`
        // so the path is wire-equivalent. A FULL SNAPSHOT stays on the
        // size-gated decision (`should_chunk`): it is NOT buffered (it is
        // self-correcting — re-derived from a whole-store rescan on the
        // next reconnect/tick), so the legacy single-message path remains
        // reliable enough for it.
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            ChunkedMessage, chunked_message,
        };
        use nativelink_util::blobs_available_chunking::{
            BLOBS_AVAILABLE_PER_CHUNK, chunk_blobs_available, should_chunk,
        };

        if !is_first || should_chunk(&notification) {
            let broadcast_id = state
                .next_broadcast_id
                .fetch_add(1, Ordering::Relaxed);
            let worker_instance_token = state.worker_instance_token;
            let chunks = match chunk_blobs_available(
                notification,
                broadcast_id,
                worker_instance_token,
                String::new(),
                BLOBS_AVAILABLE_PER_CHUNK,
            ) {
                Ok(chunks) => chunks,
                Err(reason) => {
                    // (Fix #2 / dsr BLOCK-1) The notification would
                    // require more chunks than the server's
                    // MAX_SEQUENCES cap accepts. Log loudly and skip
                    // this tick — better than emitting chunks the
                    // server will silently discard. The next tick
                    // will retry; if the worker's snapshot has
                    // grown past 1M entries the operator should
                    // investigate (FSS sizing pressure).
                    warn!(
                        reason,
                        new_or_touched_count,
                        evicted_count,
                        cached_dir_count,
                        added_subtree_count,
                        removed_subtree_count,
                        pinned_mirror_count,
                        is_first,
                        broadcast_id,
                        "BlobsAvailable chunker rejected: snapshot too large for one broadcast"
                    );
                    return Ok(());
                }
            };
            let chunk_count = chunks.len();
            // (FL-688 v3 §3.8) A full snapshot supersedes every buffered
            // delta, so clear the resend buffer before (re)advertising it.
            // For a DELTA, each chunk is buffered AFTER a successful send
            // so the server's `BlobsAvailableAck` can drop it
            // (drain-on-ack) and an over-cap accumulation forces the next
            // tick to re-converge with a full snapshot. A still-unacked
            // delta chunk is RETRANSMITTED (clone, not drain) on every
            // subsequent tick by `Self::replay_unacked_chunks` (called at
            // the head of this fn, before the skip-gate) until its ack
            // drains the slot — so a delta lost on a LIVE connection
            // converges the server's locality view WITHOUT waiting for a
            // reconnect. The over-cap valve caps the replay set at
            // `BLOBS_AVAILABLE_RESEND_MAX_CHUNKS` (256) → force a full
            // snapshot, and a send-error propagates to trigger reconnect;
            // those three stop conditions bound the replay.
            if is_first {
                state.clear_resend_buffer();
            }
            for chunk in chunks {
                // Buffer the DELTA chunk (clone) for drain-on-ack BEFORE
                // moving it into the send envelope. Full-snapshot chunks
                // are not buffered (self-correcting; the snapshot is
                // re-derived from a whole-store scan on reconnect).
                let buffered_delta = if is_first {
                    None
                } else {
                    Some(chunk.clone())
                };
                let envelope = ChunkedMessage {
                    payload: Some(chunked_message::Payload::BlobsAvailable(chunk)),
                };
                if let Err(err) = grpc_client.chunked_message(envelope).await {
                    warn!(
                        ?err,
                        new_or_touched_count,
                        evicted_count,
                        cached_dir_count,
                        added_subtree_count,
                        removed_subtree_count,
                        pinned_mirror_count,
                        is_first,
                        broadcast_id,
                        "Failed to send chunked BlobsAvailable"
                    );
                    return Err(err);
                }
                if let Some(delta_chunk) = buffered_delta {
                    if state.buffer_delta_chunk(delta_chunk) {
                        // Over-cap: the buffer self-cleared and the next
                        // tick is promoted to a full snapshot. Log so a
                        // chronic non-acking server (partition) is visible.
                        warn!(
                            target: "nativelink::blobs_available_ack",
                            broadcast_id,
                            cap = BLOBS_AVAILABLE_RESEND_MAX_CHUNKS,
                            "BlobsAvailable resend buffer over cap; cleared + forcing a full \
                             snapshot next tick (server not acking deltas — partition?)"
                        );
                    }
                }
            }
            info!(
                new_or_touched_count,
                evicted_count,
                cached_dir_count,
                added_subtree_count,
                removed_subtree_count,
                pinned_mirror_count,
                pinned_ac_mirror_count,
                ac_pin_added_count,
                ac_pin_removed_count,
                ac_pin_scan_elapsed_us,
                is_first,
                broadcast_id,
                chunk_count,
                "Sent chunked BlobsAvailable (#99 path)"
            );
            // (A1 fix) Successful send → memo the snapshot so the next
            // tick's delta is computed against what the server now
            // believes the worker holds. Chunked path is wire-equivalent
            // to the single-message path (server reassembles before
            // calling `handle_blobs_available`), so the memo update is
            // identical.
            //
            // (A1 fix-up F5) Mid-chunk failure handling: this update
            // fires only after ALL chunks succeed. On a mid-chunk Err
            // (chunk N > 0 fails), the inner loop returns Err WITHOUT
            // touching `last_sent_ac_pin_set` — next tick re-sends the
            // full delta. The server may have accumulator state from
            // successful chunks N=0..M-1, but chunked reassembly is
            // keyed on `(broadcast_id, worker_instance_token)`; the
            // next tick's broadcast_id is fresh, so the partial
            // sequence is overwritten by the retransmit. No torn
            // state.
            *state.last_sent_ac_pin_set.lock() = current_ac_pin_set;
        } else if let Err(err) = grpc_client.blobs_available(notification).await {
            warn!(
                ?err,
                new_or_touched_count,
                evicted_count,
                cached_dir_count,
                added_subtree_count,
                removed_subtree_count,
                pinned_mirror_count,
                pinned_ac_mirror_count,
                ac_pin_added_count,
                ac_pin_removed_count,
                ac_pin_scan_elapsed_us,
                is_first,
                "Failed to send periodic BlobsAvailable"
            );
            // Channel closed means the server dropped us — propagate to
            // trigger reconnect. The server also sends Update::Disconnect
            // when it detects "Worker not found", which is handled in run().
            return Err(err);
        } else {
            info!(
                new_or_touched_count,
                evicted_count,
                cached_dir_count,
                added_subtree_count,
                removed_subtree_count,
                pinned_mirror_count,
                pinned_ac_mirror_count,
                ac_pin_added_count,
                ac_pin_removed_count,
                ac_pin_scan_elapsed_us,
                is_first,
                "Sent periodic BlobsAvailable"
            );
            // (A1 fix) Successful send → memo the snapshot so the next
            // tick's delta is computed against what the server now
            // believes the worker holds.
            *state.last_sent_ac_pin_set.lock() = current_ac_pin_set;
        }
        Ok(())
    }

    async fn run(
        &self,
        update_for_worker_stream: Streaming<UpdateForWorker>,
        shutdown_rx: &mut broadcast::Receiver<ShutdownGuard>,
    ) -> Result<(), Error> {
        // This big block of logic is designed to help simplify upstream components. Upstream
        // components can write standard futures that return a `Result<(), Error>` and this block
        // will forward the error up to the client and disconnect from the scheduler.
        // It is a common use case that an item sent through update_for_worker_stream will always
        // have a response but the response will be triggered through a callback to the scheduler.
        // This can be quite tricky to manage, so what we have done here is given access to a
        // `futures` variable which because this is in a single thread as well as a channel that you
        // send a future into that makes it into the `futures` variable.
        // This means that if you want to perform an action based on the result of the future
        // you use the `.map()` method and the new action will always come to live in this spawn,
        // giving mutable access to stuff in this struct.
        // NOTE: If you ever return from this function it will disconnect from the scheduler.
        let mut futures = FuturesUnordered::new();
        futures.push(self.start_keep_alive().boxed());

        // Start BlobsAvailable reporting with drain-then-fire semantics.
        // The loop wakes immediately when blob changes are detected (via
        // Notify) and drains all accumulated changes in one send. Under
        // high load, changes accumulate while the previous send is in
        // flight and are picked up by the next iteration.
        if let Some(ref state) = self.blobs_available_state {
            let mut grpc_client = self.grpc_client.clone();
            let state = state.clone();
            // Pull a notify handle for mirror-blob inserts/removes so the
            // BlobsAvailable loop wakes promptly when the server pushes a
            // mirror copy to us. Pre-fix the loop only woke on FilesystemStore
            // changes — mirror writes were invisible until the next backstop
            // tick, and the mirror-TTL sweeper would sometimes drop the only
            // copy of a blob if the server was slow to ack stable storage.
            let mirror_notify = state
                .cas_server_fss
                .as_ref()
                .map(|f| f.mirror_changes_notify());
            // Sibling notify on the AC FSS: wake when an AC entry is
            // newly written (insert_local_ac_pin) or BIS-acked
            // (remove_local_ac_pins). The AC FSS is a DIFFERENT
            // FastSlowStore instance than the CAS FSS, so each Notify
            // has its own waiter (single-consumer invariant preserved).
            let ac_notify = state
                .ac_mirror_target
                .as_ref()
                .map(|t| t.fss.mirror_changes_notify());
            let ram = self.running_actions_manager.clone();
            futures.push(
                async move {
                    // Send full snapshot immediately on connect so the
                    // server has an accurate locality map right away.
                    Self::send_periodic_blobs_available(&mut grpc_client, &state, &ram, true)
                        .await?;
                    loop {
                        // Wait for any of:
                        // 1. A FilesystemStore blob insert/eviction (immediate wake)
                        // 2. A CAS mirror-blob insert/remove (immediate wake — only
                        //    armed if a CAS server FastSlowStore exists)
                        // 3. An AC FSS pin insert/remove (immediate wake — only
                        //    armed if the worker's AC store is a FastSlowStore)
                        // 4. The backstop interval (catches subtree-only changes)
                        //
                        // Stack-pinned Notified instead of `Box::pin` per
                        // iteration — saves one heap allocation per
                        // BlobsAvailable wakeup. A fresh `Notified` is
                        // semantically required each iteration (it consumes
                        // exactly one notification permit), so the future
                        // itself must be re-created; `tokio::pin!` keeps it
                        // on the stack.
                        let mirror_wait =
                            OptionFuture::from(mirror_notify.as_deref().map(Notify::notified));
                        tokio::pin!(mirror_wait);
                        let ac_wait =
                            OptionFuture::from(ac_notify.as_deref().map(Notify::notified));
                        tokio::pin!(ac_wait);
                        tokio::select! {
                            () = state.notify.notified() => {}
                            Some(()) = &mut mirror_wait => {}
                            Some(()) = &mut ac_wait => {}
                            () = sleep(state.max_interval) => {}
                        }
                        Self::send_periodic_blobs_available(&mut grpc_client, &state, &ram, false)
                            .await?;
                    }
                }
                .boxed(),
            );

            // NOTE: The mirror-TTL sweeper that previously expired pinned
            // mirror blobs after 120s has been REMOVED. Mirror blobs are
            // pinned indefinitely and only released when the server sends
            // `BlobsInStableStorage` for the digest. During a server
            // restart the worker holds the only durable copy; an aggressive
            // TTL would drop that copy and lose data. The 2 GiB
            // `MIRROR_BLOBS_MAX_BYTES` cap is the only bound, and silent
            // drops at the cap are now logged at warn! level.
        }

        // On (re)connect, retry any failed background slow-store writes
        // so blobs that couldn't reach the server are re-uploaded.
        {
            let ram = self.running_actions_manager.clone();
            if let Some(cas_store) = ram.get_cas_store() {
                let failed = cas_store.drain_failed_digests();
                if !failed.is_empty() {
                    let count = failed.len();
                    info!(count, "retrying failed slow-store uploads on reconnect");
                    // Re-pin to refresh the pin timeout before uploading. We
                    // pin on the inner fast (FilesystemStore) directly because
                    // that is the store whose eviction we are guarding against;
                    // pinning through the wrapper would also forward to the
                    // slow store, which is meaningless for a remote GrpcStore.
                    #[allow(clippy::disallowed_methods)]
                    cas_store.fast_store().pin_digests(&failed);
                    let max_concurrent_uploads =
                        effective_max_concurrent_uploads(self.config.max_concurrent_uploads);
                    tokio::spawn(async move {
                        Self::handle_upload_missing_blobs(&ram, failed, max_concurrent_uploads)
                            .await;
                        info!(count, "reconnect: failed upload retry complete");
                    });
                }
            }
        }

        let (add_future_channel, add_future_rx) = mpsc::unbounded_channel();
        let mut add_future_rx = UnboundedReceiverStream::new(add_future_rx).fuse();

        let mut update_for_worker_stream = update_for_worker_stream.fuse();
        // A notify which is triggered every time actions_in_flight is subtracted.
        let actions_notify = Arc::new(Notify::new());
        // #85 P2 (2026-06-07): share the in-flight counter with the
        // worker-process-global `WorkerActionsInFlight` singleton so
        // it is scrapeable as `worker_actions_in_flight`. Producer is
        // unchanged (existing fetch_add/fetch_sub below).
        let actions_in_flight = Arc::clone(
            &::nativelink_util::o11_probes::worker_actions_in_flight().counter,
        );
        // Set to true when shutting down, this stops any new StartAction.
        let mut shutting_down = false;

        // (FL-688 v3 Stage C — Ordering A) Startup reconcile gate.
        // The gate is armed AT CONSTRUCTION TIME when `startup_reconcile_gate:
        // true` is set in the FilesystemSpec (BLOCK-2 fix). This means the gate
        // is active before `add_files_to_cache` runs, before `start_background_
        // eviction` starts, and before this code runs — eliminating the race
        // window between construction-time boot drain and the former call to
        // `set_startup_reconcile_gate()` here (which was after the boot drain).
        //
        // The local `reconcile_complete` Arc is the SAME Arc that the evicting
        // map uses; `reconcile_complete_flag()` returns a clone of it. When the
        // ReconcileComplete handler calls `release_startup_reconcile_gate()` it
        // stores `true` into this Arc, which both:
        //  (a) unblocks the background drain tick in MokaEvictingMap, and
        //  (b) allows StartAction to proceed (the Acquire load below).
        //
        // MAJOR-1 fix (no per-reconnect re-arm): removing the explicit
        // `set_startup_reconcile_gate()` call here ensures that a reconnect
        // (another `run()` call after the first completes) does NOT re-arm
        // the gate on a store that was already released — which would permanently
        // suppress eviction if ReconcileComplete is never sent again.
        let reconcile_complete: Arc<AtomicBool> =
            if let Some(ref state) = self.blobs_available_state {
                // Gate was armed at construction (via startup_reconcile_gate:
                // true in config) and will be released by ReconcileComplete.
                state.fs_store.reconcile_complete_flag()
            } else {
                // No FilesystemStore fast tier — gate not needed; default true.
                Arc::new(AtomicBool::new(true))
            };

        // (#37) Worker-local swap-gate state, main-loop-local (the gate
        // decision happens synchronously on each StartAction). `since`
        // anchors the time-bounded fleet fail-open (§5 case 3b): a worker
        // that has been NAKing with NO in-flight work for longer than
        // `SWAP_FAIL_OPEN_AFTER` accepts ONE action so an all-idle
        // all-pressured fleet (e.g. a non-action memory leak) cannot wedge.
        // `latched` is the hysteresis latch (§5 case 3b / security): once
        // the fail-open accepts an action, re-gating is suppressed until
        // that action completes (in-flight returns to 0), guaranteeing
        // monotonic progress instead of accept→re-gate oscillation.
        let mut swap_first_idle_gated_at: Option<Instant> = None;
        let mut swap_fail_open_latched = false;
        // (F4) Disk-gate fleet fail-open + hysteresis latch, threaded across
        // loop iterations exactly like the swap state above.
        let mut disk_first_idle_gated_at: Option<Instant> = None;
        let mut disk_fail_open_latched = false;

        // (FL-688 v3 Stage C — MAJOR-2) Bounded fail-open for the startup
        // reconcile gate. R3's "no-timeout liveness" argument assumed the server
        // controls dispatch and never sends StartAction while the gate is armed.
        // That assumption breaks on a rolling deploy: an OLD server (pre-v3)
        // dispatches StartAction without ever sending ReconcileComplete → gate
        // stuck forever (action NAKs burn scheduler retries until max_retries,
        // then the job is lost).
        //
        // Fix: release the gate after 2 × DRAIN_INTERVAL_SECS (= 20s) if
        // ReconcileComplete has not arrived. The gate is already released in the
        // `true` (unneeded) case — the sleep fires but the load returns `true`
        // and no release is needed. This is FAIL-OPEN, not data-loss: durable
        // blobs stay reconcile-pinned until BIS-ack; only the LRU-suppression
        // window is lifted (blobs not specifically pinned become LRU-evictable
        // after 20s if the server never validates them).
        //
        // NOTE: the outer select! is `futures::select!` (not tokio::select!).
        // futures::select! does NOT support the `, if condition` guard syntax;
        // we use FutureExt::fuse() to make the timer a one-shot that is NEVER
        // re-polled after it fires.
        //
        // DRAIN_INTERVAL_SECS = 10 (moka_evicting_map.rs:72); 2× = 20s.
        const RECONCILE_FAIL_OPEN_SECS: u64 = 20;
        let reconcile_fail_open = sleep(core::time::Duration::from_secs(RECONCILE_FAIL_OPEN_SECS)).fuse();
        tokio::pin!(reconcile_fail_open);

        loop {
            select! {
                maybe_update = update_for_worker_stream.next() => if !shutting_down || maybe_update.is_some() {
                    let proto_update = maybe_update
                        .err_tip(|| "UpdateForWorker stream closed early")?
                        .err_tip(|| "Got error in UpdateForWorker stream")?
                        .update;
                    // Per plan B2 (USER OVERRIDE: no capability flag): when
                    // the server sends a NEW oneof variant that this worker
                    // does not know about, prost decodes the variant
                    // INSIDE the oneof but leaves the outer `update` as
                    // `None` (proto3 unknown-field skip). Pre-fix this
                    // path `?`-propagated "Expected update to exist in
                    // UpdateForWorker" and exited the connection task,
                    // creating an offline-worker-wakeup hot loop on
                    // server-side rollouts of new variants. Now we
                    // gracefully `warn!` + continue so old workers
                    // survive a rolling deploy of `BatchWriteSmallBlobs`
                    // (and any future variant added at the same site).
                    let Some(update) = proto_update else {
                        warn!(
                            "received UpdateForWorker with no recognized update variant; \
                             skipping (server may be running a newer build with a new oneof tag)"
                        );
                        continue;
                    };
                    match update {
                        Update::ConnectionResult(_) => {
                            return Err(make_input_err!(
                                "Got ConnectionResult in LocalWorker::run which should never happen"
                            ));
                        }
                        Update::Disconnect(()) => {
                            self.metrics.disconnects_received.inc();
                            return Err(make_err!(
                                Code::Internal,
                                "received disconnect from scheduler, will reconnect"
                            ));
                        }
                        Update::KeepAlive(()) => {
                            self.metrics.keep_alives_received.inc();
                        }
                        Update::KillOperationRequest(kill_operation_request) => {
                            let operation_id = OperationId::from(kill_operation_request.operation_id);
                            if let Err(err) = self.running_actions_manager.kill_operation(&operation_id).await {
                                error!(
                                    %operation_id,
                                    ?err,
                                    "Failed to send kill request for operation"
                                );
                            }
                        }
                        Update::TouchBlobs(touch_request) => {
                            // Touch blobs in the local store to update access times
                            // and prevent premature eviction of referenced blobs.
                            let digest_count = touch_request.digests.len();
                            trace!(digest_count, "Received TouchBlobs request");
                            if let Some(ref state) = self.blobs_available_state {
                                let fs_store = state.fs_store.clone();
                                let digests: Vec<DigestInfo> = touch_request
                                    .digests
                                    .into_iter()
                                    .filter_map(|d| DigestInfo::try_from(d).ok())
                                    .collect();
                                // Best-effort: call has() on each digest to update
                                // the EvictingMap's LRU access time.
                                let keys: Vec<StoreKey<'_>> = digests
                                    .iter()
                                    .map(|d| StoreKey::from(*d))
                                    .collect();
                                let mut results = vec![None; keys.len()];
                                if let Err(err) = Pin::new(fs_store.as_ref())
                                    .has_with_results(&keys, &mut results)
                                    .await
                                {
                                    warn!(
                                        ?err,
                                        digest_count,
                                        "TouchBlobs: failed to touch digests in FilesystemStore"
                                    );
                                } else {
                                    let found = results.iter().filter(|r| r.is_some()).count();
                                    trace!(
                                        digest_count,
                                        found,
                                        "TouchBlobs: touched digests in FilesystemStore"
                                    );
                                }
                            }
                        }
                        Update::BlobsInStableStorage(blobs) => {
                            let digest_count = blobs.digests.len();
                            // Per-broadcast BIS receive-arm firehose;
                            // trace! so prod release builds compile it out.
                            trace!(
                                target: "nativelink::stable_storage_received",
                                digest_count,
                                "BlobsInStableStorage: arm entered (BEFORE any gate)"
                            );
                            if let Some(ref state) = self.blobs_available_state {
                                trace!(
                                    target: "nativelink::stable_storage_gate",
                                    digest_count,
                                    "blobs_available_state present, processing"
                                );
                                let cas_store_for_ack =
                                    self.running_actions_manager.get_cas_store();
                                handle_blobs_in_stable_storage(
                                    state,
                                    cas_store_for_ack.as_ref(),
                                    &blobs.digests,
                                );
                            } else {
                                warn!(
                                    target: "nativelink::stable_storage_gate",
                                    digest_count,
                                    "blobs_available_state is None, dropping unpin (BUG?)"
                                );
                                trace!(
                                    digest_count,
                                    "BlobsInStableStorage: no FilesystemStore available, ignoring"
                                );
                            }
                        }
                        Update::ChunkedMessage(chunked) => {
                            // (#98 / #97) Streaming protocol envelope. PeerHints
                            // chunks register into the worker's peer_locality_map
                            // (#98); BlobsInStableStorage chunks unpin local CAS
                            // entries + emit a BisAck so the server's per-worker
                            // resend buffer can drop the matching slot (#97).
                            match chunked.payload {
                                Some(chunked_message::Payload::PeerHints(chunk)) => {
                                    handle_peer_hints_chunk(
                                        self.peer_locality_map.as_ref(),
                                        &chunk,
                                    );
                                }
                                Some(chunked_message::Payload::BlobsInStableStorage(chunk)) => {
                                    // #547 Phase 0 instrumentation: capture
                                    // the arrival timestamp at the earliest
                                    // moment after the chunk is matched from
                                    // the dispatch arm. The commit fires
                                    // immediately before handle_bis_chunk so
                                    // the recorded gap names the worker-side
                                    // dispatcher contribution only (NOT the
                                    // handler runtime). Pure observability.
                                    let phase0_bis_arrival_ts =
                                        worker_phase0_metrics().record_bis_chunk_arrival();
                                    let digest_count = chunk.digests.len();
                                    let broadcast_id = chunk.broadcast_id;
                                    let sequence = chunk.sequence;
                                    // Per-chunk BIS receive-arm firehose
                                    // (~245 chunks/s live); trace! so prod
                                    // release builds compile it out.
                                    trace!(
                                        target: "nativelink::stable_storage_chunked_received",
                                        broadcast_id,
                                        sequence,
                                        digest_count,
                                        is_last = chunk.is_last,
                                        "BIS chunk arm entered"
                                    );
                                    if let Some(ref state) = self.blobs_available_state {
                                        let cas_store_for_ack =
                                            self.running_actions_manager.get_cas_store();
                                        let mut grpc_client = self.grpc_client.clone();
                                        // Commit the arrival→handler timer
                                        // immediately before invoking the
                                        // handler so the gap matches the
                                        // dispatcher contribution.
                                        worker_phase0_metrics()
                                            .commit_arrival_to_handler(phase0_bis_arrival_ts);
                                        // Send the ack inline so the resend
                                        // buffer is released as soon as the
                                        // unpins land. The async send is
                                        // spawned to avoid blocking the
                                        // dispatch loop on a slow ack.
                                        handle_bis_chunk(
                                            state,
                                            cas_store_for_ack.as_ref(),
                                            &chunk,
                                            move |ack| {
                                                tokio::spawn(async move {
                                                    if let Err(err) = grpc_client.bis_ack(ack).await {
                                                        warn!(
                                                            ?err,
                                                            broadcast_id,
                                                            sequence,
                                                            "BIS ack send failed; server will resend on reconnect"
                                                        );
                                                    }
                                                });
                                            },
                                        );
                                    } else {
                                        warn!(
                                            target: "nativelink::stable_storage_chunked_gate",
                                            broadcast_id,
                                            sequence,
                                            digest_count,
                                            "blobs_available_state is None, dropping BIS chunk + ack (BUG?)"
                                        );
                                    }
                                }
                                Some(chunked_message::Payload::BlobsAvailable(_chunk)) => {
                                    // (#99) BlobsAvailable is the
                                    // worker→server direction; the
                                    // scheduler must never emit this
                                    // arm to a worker. Ignore + warn
                                    // for defensive observability.
                                    warn!(
                                        "Update::ChunkedMessage(BlobsAvailable) from scheduler; \
                                         wrong-direction payload — ignoring"
                                    );
                                }
                                None => {
                                    warn!(
                                        "Update::ChunkedMessage with empty payload from scheduler; ignoring"
                                    );
                                }
                            }
                        }
                        Update::BlobsAvailableAck(ack) => {
                            // (FL-688 v3 §3.8) Server acked one of the
                            // delta `BlobsAvailableChunk`s we sent. Drop
                            // the matching `(broadcast_id, sequence)` slot
                            // from the resend buffer (drain-on-ack). The
                            // token guard + per-chunk drop live in
                            // `handle_blobs_available_ack`.
                            if let Some(ref state) = self.blobs_available_state {
                                handle_blobs_available_ack(state, &ack);
                            } else {
                                // No BlobsAvailable reporting on this
                                // worker (no FilesystemStore fast tier) ⇒
                                // we never sent a delta, so an ack is
                                // unexpected; warn for observability.
                                warn!(
                                    target: "nativelink::blobs_available_ack",
                                    broadcast_id = ack.broadcast_id,
                                    sequence = ack.sequence,
                                    "BlobsAvailableAck received but blobs_available_state is None (BUG?)"
                                );
                            }
                        }
                        Update::AcPinResync(_) => {
                            // (FL-688 v3 Stage A fix) The server removed AC-pin
                            // entries for our endpoint OUT-OF-BAND (BIS-ack
                            // sweep / AcProxy peer-NotFound / cap-truncation) and
                            // is asking us to FORCE a full re-advertisement of
                            // our AC-pin set so its `AcPinRegistry` reconverges
                            // WITHOUT waiting for a reconnect. Clear
                            // `last_sent_ac_pin_set` (the reconnect-clear path):
                            // the next tick reports the full set as `added` so
                            // the skip-gate no longer suppresses, and field 17 is
                            // re-sent (replace-semantics) → server parity
                            // restored. See `force_ac_pin_resync`.
                            if let Some(ref state) = self.blobs_available_state {
                                force_ac_pin_resync(&state.last_sent_ac_pin_set);
                                // Wake the periodic loop so the resync rides the
                                // NEXT tick promptly rather than waiting out the
                                // current interval.
                                state.notify.notify_one();
                                debug!(
                                    target: "nativelink::ac_pin_resync",
                                    "AcPinResync received — cleared last_sent_ac_pin_set; \
                                     next tick re-advertises full AC-pin snapshot"
                                );
                            } else {
                                // No BlobsAvailable reporting on this worker (no
                                // FilesystemStore fast tier) ⇒ we never advertise
                                // AC pins, so a resync request is a no-op.
                                trace!(
                                    target: "nativelink::ac_pin_resync",
                                    "AcPinResync received but blobs_available_state is None; no-op"
                                );
                            }
                        }
                        Update::UploadMissingBlobs(request) => {
                            // Server is requesting we upload blobs it doesn't
                            // have. Read from local fast store and upload to
                            // the slow store (server CAS) in the background.
                            let digest_count = request.digests.len();
                            let digests: Vec<DigestInfo> = request
                                .digests
                                .into_iter()
                                .filter_map(|d| DigestInfo::try_from(d).ok())
                                .collect();
                            info!(
                                digest_count,
                                valid_count = digests.len(),
                                "UploadMissingBlobs: server requests blob backfill"
                            );
                            // (FL-688 v3 Stage C — PRIMARY: reconcile-pin)
                            // Pin each requested digest INDEFINITELY *before*
                            // spawning the upload task. This fires while the
                            // executor is still blocked (reconcile_complete=false
                            // above), so no runtime insert has happened yet and
                            // the per-insert moka eviction cannot race us.
                            // Cap-refusal leaves the blob UNPROTECTED (only the
                            // time-bounded fallback applies); this is the
                            // acknowledged item-D residual (R3).
                            if let Some(ref state) = self.blobs_available_state {
                                // (MINOR-1) Split counters: TimeBoundedFallback is
                                // recoverable backpressure (cap full); Refused is the
                                // actionable FL-688 data-loss signal (blob evicted
                                // before reconcile-pin).
                                let mut time_bounded_count: u64 = 0;
                                let mut refused_count: u64 = 0;
                                for d in &digests {
                                    match state.fs_store.pin_digest_indefinite_or_time_bounded(d) {
                                        IndefinitePinOutcome::Indefinite => {}
                                        IndefinitePinOutcome::TimeBoundedFallback => {
                                            time_bounded_count += 1;
                                        }
                                        IndefinitePinOutcome::Refused => {
                                            refused_count += 1;
                                        }
                                    }
                                }
                                // Collapse per-digest warns into ONE per-batch summary
                                // to avoid flooding logs on a sustained over-cap worker.
                                if time_bounded_count > 0 {
                                    warn!(
                                        time_bounded_count,
                                        "reconcile-pin: indefinite cap exhausted for \
                                         {time_bounded_count} blob(s), fell back to \
                                         time-bounded pin (exposed after PIN_TIMEOUT_SECS \
                                         if cap stays saturated)"
                                    );
                                    self.metrics
                                        .reconcile_pin_time_bounded_fallback_total
                                        .add(time_bounded_count);
                                }
                                if refused_count > 0 {
                                    // NOT the FL-688 data-loss signal: a `Refused`
                                    // pin means the digest is absent from the moka
                                    // in-memory eviction INDEX, but the upload does
                                    // NOT skip on this — `handle_upload_missing_blobs`
                                    // re-checks presence via
                                    // `FastSlowStore::has_with_results` (reads DISK +
                                    // mirror_blobs, NOT the moka index), so a
                                    // disk-backed blob still uploads (verified live:
                                    // 970 refused → found: 1000 → uploaded: 1000,
                                    // failed: 0). `pin_digest_indefinite_or_time_bounded`'s
                                    // own doc-comment names `Refused` an expected
                                    // self-healing eviction race. The genuine
                                    // data-loss signal is `failed > 0` at
                                    // `backfill complete` (below), keyed on the actual
                                    // upload outcome. Kept at info! (NOT debug!, which
                                    // the release_max_level_info prod binary compiles
                                    // out) so the rate stays visible; the
                                    // process-singleton counter
                                    // `reconcile_pin_refused_total` carries the count
                                    // without per-batch log volume.
                                    info!(
                                        refused_count,
                                        "reconcile-pin: {refused_count} blob(s) not in the \
                                         in-memory eviction index (benign eviction race; \
                                         disk-backed upload proceeds — see \
                                         reconcile_pin_vanished_total for actual loss)"
                                    );
                                    ::nativelink_util::o11_probes::reconcile_pin_counters()
                                        .refused
                                        .fetch_add(refused_count, Ordering::Relaxed);
                                }
                            }
                            let ram = self.running_actions_manager.clone();
                            let max_concurrent_uploads = effective_max_concurrent_uploads(
                                self.config.max_concurrent_uploads,
                            );
                            tokio::spawn(async move {
                                Self::handle_upload_missing_blobs(
                                    &ram,
                                    digests,
                                    max_concurrent_uploads,
                                )
                                .await;
                            });
                        }
                        Update::BatchWriteSmallBlobs(batch) => {
                            // Per plan §"Architecture summary": the server's
                            // SmallBlobDispatcher pushes a batch of small
                            // CAS/AC blobs (≤ SMALL_BLOB_THRESHOLD = 16 KiB)
                            // for the worker to hold in `mirror_blobs`. The
                            // worker advertises the snapshot via field 16
                            // `pinned_mirror_entries` on the next
                            // BlobsAvailableNotification, and the server's
                            // per-store `EphemeralServerSidePin` releases
                            // matching pins.
                            //
                            // The (store_id, digest) keying is INFORMATIONAL
                            // for now — per plan B5 the BTreeMap refactor
                            // is a follow-up; today the underlying mirror_blobs
                            // is keyed by DigestInfo. Multi-store collisions
                            // on the same digest will overwrite (last-writer
                            // wins). The dispatcher's feature flag is OFF in
                            // canary, so production is not yet exposed.
                            let cas_server_fss =
                                self.blobs_available_state.as_ref()
                                    .and_then(|s| s.cas_server_fss.as_ref());
                            handle_batch_write_small_blobs(cas_server_fss, &batch.blobs);
                        }
                        Update::ReconcileComplete(_) => {
                            // (FL-688 v3 Stage C — Ordering A, item 4)
                            // Server has processed the first full BlobsAvailable
                            // snapshot and finished requesting any missing uploads.
                            // Release the startup reconcile gate: unblock the
                            // background LRU drain and allow action execution.
                            //
                            // This signal is SEPARATE from UploadMissingBlobs so
                            // that a worker with NO missing blobs (nothing to
                            // upload) still gets the gate released — the early-
                            // return paths in `request_missing_blob_uploads` would
                            // otherwise suppress all traffic and wedge an over-cap
                            // worker forever (DOC-FIX-1).
                            if let Some(ref state) = self.blobs_available_state {
                                // `release_startup_reconcile_gate()` stores `true`
                                // to the shared `reconcile_complete` Arc — the same
                                // Arc that `reconcile_complete` here is cloned from
                                // (`reconcile_complete_flag()`). No second store needed.
                                state.fs_store.release_startup_reconcile_gate();
                                info!(
                                    "ReconcileComplete received: startup reconcile gate released, \
                                     action executor unblocked"
                                );
                            } else {
                                // No FilesystemStore fast tier; gate was never
                                // armed, so this is a no-op.
                                trace!(
                                    "ReconcileComplete received but no FilesystemStore (no-op)"
                                );
                            }
                        }
                        Update::PrefetchInputs(prefetch) => {
                            // (speculative-prefetch Increment 1) Pre-fetch this
                            // action's cold input blobs into the local CAS during
                            // the slot-wait so that when the real StartAction
                            // arrives the construct's populate finds them resident.
                            //
                            // G5 / §1.10: single in-flight per worker. Drop + count
                            // a 2nd concurrent PrefetchInputs. Guard = AtomicBool;
                            // no separate counter (presence = busy, confirmed necessary).
                            //
                            // Adoption is IMPLICIT: the real StartAction path's
                            // populate call hits the already-resident+pinned blobs →
                            // no-op. Pins are released by the TTL timer (self-fired
                            // `speculative_prefetch_ttl_s`).
                            if self.speculative_prefetch_inflight
                                .compare_exchange(
                                    false,
                                    true,
                                    core::sync::atomic::Ordering::AcqRel,
                                    core::sync::atomic::Ordering::Relaxed,
                                )
                                .is_err()
                            {
                                // Another speculative prefetch is in-flight.
                                self.metrics
                                    .speculative_prefetch_busy_drop
                                    .inc();
                                debug!(
                                    operation_id = prefetch.operation_id,
                                    "speculative prefetch: dropping (already in-flight, G5)"
                                );
                                continue;
                            }

                            // Register P2P peer hints so cold blobs can pull from
                            // a peer holding them (before resolve_directory_tree).
                            register_missing_blob_peers(
                                self.peer_locality_map.as_ref(),
                                &prefetch.missing_digest_peers,
                            );

                            // Resolve input_root_digest from the proto field.
                            let Some(ref proto_digest) = prefetch.input_root_digest else {
                                warn!(
                                    operation_id = prefetch.operation_id,
                                    "speculative prefetch: missing input_root_digest — skipping"
                                );
                                self.speculative_prefetch_inflight
                                    .store(false, core::sync::atomic::Ordering::Release);
                                continue;
                            };
                            let input_root_digest =
                                match nativelink_util::common::DigestInfo::try_from(proto_digest) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        warn!(
                                            operation_id = prefetch.operation_id,
                                            ?e,
                                            "speculative prefetch: invalid digest — skipping"
                                        );
                                        self.speculative_prefetch_inflight
                                            .store(false, core::sync::atomic::Ordering::Release);
                                        continue;
                                    }
                                };

                            // Get the DirectoryCache to PRE-CONSTRUCT the input-root
                            // entry (fetch + resolve + subtree-reuse + hardlink into
                            // the cache ENTRY) ahead of the real StartAction, so at
                            // dispatch `get_or_create` is a clonefile HIT
                            // (construct_resolve+construct_fetch ≈ 0). Replaces the
                            // prior fetch-half-into-CAS seam.
                            let Some(dir_cache) =
                                self.running_actions_manager.get_directory_cache()
                            else {
                                warn!(
                                    operation_id = prefetch.operation_id,
                                    "speculative prefetch: no DirectoryCache on this worker — skipping"
                                );
                                self.speculative_prefetch_inflight
                                    .store(false, core::sync::atomic::Ordering::Release);
                                continue;
                            };

                            let inflight_flag = self.speculative_prefetch_inflight.clone();
                            let guard_slot = self.speculative_prefetch_guard.clone();
                            // #speculative-prefetch: honor the operator-configured
                            // TTL forwarded on the wire (SimpleSpec.speculative_prefetch_ttl_s
                            // → PrefetchInputs.ttl_s). 0 means "use the worker default".
                            // NOT derived from worker_timeout_s (default=0 / disabled).
                            // The effective pin lifetime is clamped to PIN_TIMEOUT_SECS
                            // (120s) at the sleep below.
                            const SPECULATIVE_PREFETCH_TTL_DEFAULT_S: u64 = 60;
                            let ttl_s = if prefetch.ttl_s == 0 {
                                SPECULATIVE_PREFETCH_TTL_DEFAULT_S
                            } else {
                                prefetch.ttl_s
                            };
                            let operation_id_log = prefetch.operation_id.clone();
                            let metrics = self.metrics.clone();

                            // Spawn detached so we NEVER block the Update loop.
                            tokio::spawn(async move {
                                // The single-in-flight AtomicBool is held for the
                                // ENTIRE spawn (construct + TTL window) so exactly
                                // one speculative entry-pin exists at a time (G5).
                                // Release fires on every exit path below.
                                match dir_cache
                                    .prewarm(
                                        input_root_digest,
                                        crate::directory_cache::OpPriority::Speculative,
                                    )
                                    .await
                                {
                                    Ok(Some(guard)) => {
                                        // Hold the entry-pin so the pre-warmed entry
                                        // stays resident (evict-LAST) until adoption
                                        // (real StartAction HIT) or the TTL timer.
                                        *guard_slot.lock() = Some(guard);
                                        info!(
                                            operation_id = operation_id_log,
                                            "speculative prefetch: entry pre-constructed + pinned"
                                        );

                                        // Self-fired TTL timer. The real StartAction's
                                        // HIT adopts the warm entry (no explicit
                                        // hand-off needed — the entry is simply there);
                                        // on TTL wake we DROP the guard so an
                                        // un-adopted entry becomes normal-LRU
                                        // (evict-first). Drop is synchronous
                                        // (ref_count.fetch_sub), never caller-forgets.
                                        tokio::time::sleep(
                                            core::time::Duration::from_secs(ttl_s.min(120))
                                        ).await;
                                        let released = core::mem::take(&mut *guard_slot.lock());
                                        if released.is_some() {
                                            // Guard dropped here → entry ref_count
                                            // drops, entry is now LRU-evictable.
                                            info!(
                                                operation_id = operation_id_log,
                                                "speculative prefetch: TTL expired, entry-pin released"
                                            );
                                        }
                                    }
                                    Ok(None) => {
                                        // Entry raced eviction between construct and
                                        // pin re-check, or was not warmable. Skip;
                                        // the real action falls through normally.
                                        info!(
                                            operation_id = operation_id_log,
                                            "speculative prefetch: entry vanished/not-warmable — skip"
                                        );
                                    }
                                    Err(ref e) if e.code == nativelink_error::Code::Aborted => {
                                        // Over-pressure: the construct's own
                                        // POPULATE_BYTE_BUDGET semaphore returned
                                        // Aborted (yield-first). Fail-fast; the real
                                        // action's independent populate is unaffected.
                                        metrics.speculative_prefetch_aborted.inc();
                                        warn!(
                                            operation_id = operation_id_log,
                                            "speculative prefetch: aborted (over-pressure) — \
                                             real action unaffected"
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            operation_id = operation_id_log,
                                            ?e,
                                            "speculative prefetch: pre-construct failed"
                                        );
                                    }
                                }
                                // Release the single-in-flight guard on exit (any path).
                                inflight_flag.store(false, core::sync::atomic::Ordering::Release);
                            });
                        }
                        Update::StartAction(start_execute) => {
                            // (FL-688 v3 Stage C — Ordering A, item 4)
                            // Block new actions until the startup reconcile is
                            // complete. This ensures reconcile-pin calls in the
                            // UploadMissingBlobs handler fire before any runtime
                            // insert can race a per-insert moka eviction.
                            // The gate starts `false` at boot (set by
                            // `set_startup_reconcile_gate`) and is flipped to
                            // `true` by `ReconcileComplete`. For workers without
                            // a FilesystemStore fast tier the gate is never armed
                            // and starts `true` — no behavior change.
                            if !reconcile_complete.load(Ordering::Acquire) {
                                warn!(
                                    "NAKing StartAction: startup reconcile gate still open \
                                     (waiting for server ReconcileCompleteRequest)"
                                );
                                if let Some(instance_name) = start_execute.execute_request.map(|request| request.instance_name) {
                                    self.grpc_client.clone().execution_response(
                                        ExecuteResult{
                                            instance_name,
                                            operation_id: start_execute.operation_id,
                                            // ResourceExhausted is the scheduler's
                                            // backpressure-exempt code: it does NOT
                                            // count as an attempt (simple_scheduler_state_manager.rs:817).
                                            // Code::Unavailable WOULD burn the retry budget.
                                            result: Some(execute_result::Result::InternalError(make_err!(Code::ResourceExhausted, "Worker startup reconcile in progress").into())),
                                            // resource_usage is None on this NAK path and every
                                            // reject/error path: the action never ran, so no
                                            // resource usage exists to report. The SUCCESS
                                            // completion path is the sole producer — it populates
                                            // this from the calib measurements (task-resource-profile
                                            // Phase 1). Observe-only; the server consumer is a later
                                            // wave.
                                            resource_usage: None,
                                        }
                                    ).await?;
                                }
                                continue;
                            }

                            // Don't accept any new requests if we're shutting down.
                            if shutting_down {
                                if let Some(instance_name) = start_execute.execute_request.map(|request| request.instance_name) {
                                    self.grpc_client.clone().execution_response(
                                        ExecuteResult{
                                            instance_name,
                                            operation_id: start_execute.operation_id,
                                            result: Some(execute_result::Result::InternalError(make_err!(Code::ResourceExhausted, "Worker shutting down").into())),
                                            resource_usage: None,
                                        }
                                    ).await?;
                                }
                                continue;
                            }

                            // (#37) Worker-local swap admission gate. AUTHORITATIVE
                            // (reads the in-process atomic, never the wire boolean —
                            // design §2 trust boundary). ADDITIVE to the existing
                            // count-only + memory_kb admission: the reactive backstop
                            // for RSS-estimation error. Fails OPEN on a stale/dead
                            // sampler (inside `swap_gate_pressured`) and via the
                            // time-bounded fleet fail-open below.
                            let in_flight = actions_in_flight.load(Ordering::Acquire);
                            // Hysteresis latch self-clears once the fail-open action
                            // has drained (in-flight back to 0), re-arming the gate.
                            if swap_fail_open_latched && in_flight == 0 {
                                swap_fail_open_latched = false;
                                swap_first_idle_gated_at = None;
                            }
                            let pressured = swap_gate_pressured();
                            let now = Instant::now();
                            match swap_gate_decision(
                                pressured,
                                in_flight,
                                swap_fail_open_latched,
                                swap_first_idle_gated_at,
                                now,
                            ) {
                                SwapGateDecision::Nak => {
                                    if swap_first_idle_gated_at.is_none() && in_flight == 0 {
                                        // Start the fail-open clock on the first
                                        // idle-gated NAK.
                                        swap_first_idle_gated_at = Some(now);
                                    }
                                    // Log per-source trip signal so soak data
                                    // distinguishes a free-floor NAK from a swapin NAK.
                                    let trip_free_floor =
                                        MEMORY_GATE_TRIP_FREE_FLOOR.load(Ordering::Relaxed);
                                    let trip_swapin =
                                        MEMORY_GATE_TRIP_SWAPIN.load(Ordering::Relaxed);
                                    let gate_counters =
                                        ::nativelink_util::o11_probes::memory_gate_counters();
                                    if trip_free_floor {
                                        gate_counters
                                            .nak_free_floor
                                            .fetch_add(1, ::core::sync::atomic::Ordering::Relaxed);
                                    }
                                    if trip_swapin {
                                        gate_counters
                                            .nak_swapin
                                            .fetch_add(1, ::core::sync::atomic::Ordering::Relaxed);
                                    }
                                    // Log the trip-cause magnitudes (NOT the churn perf
                                    // scalar `memory_pressure_level`): the free-floor
                                    // shortfall and the last sampled swapin rate are what
                                    // drive this NAK, so the inline magnitude an operator
                                    // reads matches trip_free_floor / trip_swapin.
                                    let free_shortfall_mib = gate_counters
                                        .pressure_level_mib
                                        .load(::core::sync::atomic::Ordering::Relaxed);
                                    let swapin_rate = gate_counters
                                        .swapin_rate_last
                                        .load(::core::sync::atomic::Ordering::Relaxed);
                                    warn!(
                                        free_shortfall_mib,
                                        swapin_rate,
                                        in_flight,
                                        trip_free_floor,
                                        trip_swapin,
                                        "worker NAKing action: sustained host memory pressure (additive backstop to memory_kb admission)"
                                    );
                                    if let Some(instance_name) = start_execute.execute_request.map(|request| request.instance_name) {
                                        self.grpc_client.clone().execution_response(
                                            ExecuteResult{
                                                instance_name,
                                                operation_id: start_execute.operation_id,
                                                result: Some(execute_result::Result::InternalError(make_err!(Code::ResourceExhausted, "Worker under memory pressure").into())),
                                                resource_usage: None,
                                            }
                                        ).await?;
                                    }
                                    continue;
                                }
                                SwapGateDecision::AcceptFailOpen => {
                                    // Time-bounded fleet fail-open fired: accept ONE
                                    // action and latch so re-gating is suppressed
                                    // until it drains (monotonic progress).
                                    swap_fail_open_latched = true;
                                    warn!(
                                        memory_pressure_level = get_memory_pressure_level(),
                                        fail_open_after_secs = SWAP_FAIL_OPEN_AFTER.as_secs(),
                                        "worker memory gate FAILING OPEN: idle+pressured past the fail-open window, accepting one action to avoid a fleet wedge"
                                    );
                                }
                                SwapGateDecision::Accept => {
                                    // Not pressured (or latched): clear the idle clock
                                    // so a future pressure episode starts fresh.
                                    if !pressured {
                                        swap_first_idle_gated_at = None;
                                    }
                                }
                            }

                            // (F4) Worker-local DISK admission gate. AUTHORITATIVE
                            // (reads the in-process atomic + a statvfs fallback, never
                            // the wire boolean). The BACKSTOP that rejects new work
                            // BEFORE raw ENOSPC at make_action_directory while moka's
                            // eventually-consistent eviction catches up. SEC-2: unlike
                            // the swap gate (which blind-fails-open on a stale sampler
                            // because it degrades to other live bounds), disk has NO
                            // other live bound, so a stale sampler routes to a one-shot
                            // authoritative statvfs (off the hot path via spawn_blocking)
                            // and rejects only a TRULY-full disk.
                            if disk_fail_open_latched && in_flight == 0 {
                                disk_fail_open_latched = false;
                                disk_first_idle_gated_at = None;
                            }
                            let (disk_pressured_sampler, disk_stale) = disk_gate_verdict(
                                DISK_GATE_ENABLED,
                                DISK_PRESSURED.load(Ordering::Relaxed),
                                LAST_DISK_SAMPLE_INSTANT.load(Ordering::Relaxed),
                                Instant::now().duration_since(*PROCESS_START),
                                DISK_SAMPLE_MAX_AGE,
                            );
                            // On a stale/dead sampler, consult the authoritative
                            // statvfs ONCE off the hot path (never a per-action sync
                            // syscall on a healthy sampler — that path reads the atomic
                            // only). `None` ⇒ no path / syscall failed ⇒ unmeasurable.
                            let disk_authoritative_free = if disk_stale {
                                if let Some(Some(path)) = DISK_SAMPLE_PATH.get() {
                                    let path = path.clone();
                                    tokio::task::spawn_blocking(move || {
                                        read_disk_available_bytes(&path)
                                    })
                                    .await
                                    .ok()
                                    .flatten()
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            if disk_stale {
                                warn!(
                                    "stale disk sample: disk-free sampler appears wedged/dead, \
                                     consulting the authoritative statvfs fallback (SEC-2: disk \
                                     has no other live bound behind the gate)"
                                );
                            }
                            let disk_effective = disk_effective_pressured(
                                disk_pressured_sampler,
                                disk_stale,
                                disk_authoritative_free,
                            );
                            let now = Instant::now();
                            match disk_gate_decision(
                                disk_effective,
                                in_flight,
                                disk_fail_open_latched,
                                disk_first_idle_gated_at,
                                now,
                            ) {
                                DiskGateDecision::Nak => {
                                    if disk_first_idle_gated_at.is_none() && in_flight == 0 {
                                        disk_first_idle_gated_at = Some(now);
                                    }
                                    warn!(
                                        available_disk_bytes = get_available_disk_bytes(),
                                        disk_stale,
                                        in_flight,
                                        "worker NAKing action: physical disk pressure on the \
                                         CAS/work_directory volume (backstop before ENOSPC while \
                                         eviction catches up)"
                                    );
                                    if let Some(instance_name) = start_execute.execute_request.map(|request| request.instance_name) {
                                        self.grpc_client.clone().execution_response(
                                            ExecuteResult{
                                                instance_name,
                                                operation_id: start_execute.operation_id,
                                                result: Some(execute_result::Result::InternalError(make_err!(Code::ResourceExhausted, "Worker under disk pressure").into())),
                                                resource_usage: None,
                                            }
                                        ).await?;
                                    }
                                    continue;
                                }
                                DiskGateDecision::AcceptFailOpen => {
                                    disk_fail_open_latched = true;
                                    warn!(
                                        available_disk_bytes = get_available_disk_bytes(),
                                        fail_open_after_secs = DISK_FAIL_OPEN_AFTER.as_secs(),
                                        "worker disk gate FAILING OPEN: idle+pressured past the fail-open window, accepting one action to avoid a fleet wedge"
                                    );
                                }
                                DiskGateDecision::Accept => {
                                    if !disk_effective {
                                        disk_first_idle_gated_at = None;
                                    }
                                }
                            }

                            self.metrics.start_actions_received.inc();

                            // (#p2p-prefetch) Register the inline per-missing-blob
                            // peer endpoints into the worker's peer_locality_map
                            // SYNCHRONOUSLY, here at StartExecute-parse — strictly
                            // BEFORE `create_and_add_action` → prepare_action →
                            // `download_to_directory` issues the first
                            // missing-blob fetch. This makes the existing
                            // WorkerProxyStore peer-race consult the FRESH peers
                            // for this action's inputs on the first read (instead
                            // of losing to the async PeerHintsChunk stream). Empty
                            // when the scheduler's flag is off / no peer holds a
                            // blob / worker has no peer_locality_map → no-op. The
                            // peer_locality_map lives on `LocalWorkerImpl` (this
                            // scope); `create_and_add_action` /
                            // `RunningActionsManager` cannot reach it, which is
                            // why the registration lives here rather than at the
                            // RAM `server_missing_digests` parse site.
                            register_missing_blob_peers(
                                self.peer_locality_map.as_ref(),
                                &start_execute.missing_digest_peers,
                            );

                            let execute_request = start_execute.execute_request.as_ref();
                            let operation_id = start_execute.operation_id.clone();
                            let operation_id_to_log = operation_id.clone();
                            let maybe_instance_name = execute_request.map(|v| v.instance_name.clone());
                            let action_digest = execute_request.and_then(|v| v.action_digest.clone());
                            let digest_hasher = execute_request
                                .ok_or_else(|| make_input_err!("Expected execute_request to be set"))
                                .and_then(|v| DigestHasherFunc::try_from(v.digest_function))
                                .err_tip(|| "In LocalWorkerImpl::new()")?;

                            let start_action_fut = {
                                let precondition_script_cfg = self.config.experimental_precondition_script.clone();
                                let mut extra_envs: HashMap<String, String> = HashMap::new();
                                if let Some(ref additional_environment) = self.config.additional_environment {
                                    for (name, source) in additional_environment {
                                        let value = match source {
                                            EnvironmentSource::Property(property) => start_execute
                                                .platform.as_ref().and_then(|p|p.properties.iter().find(|pr| &pr.name == property))
                                                .map_or_else(|| Cow::Borrowed(""), |v| Cow::Borrowed(v.value.as_str())),
                                            EnvironmentSource::Value(value) => Cow::Borrowed(value.as_str()),
                                            EnvironmentSource::FromEnvironment => Cow::Owned(env::var(name).unwrap_or_default()),
                                            other => {
                                                debug!(?other, "Worker doesn't support this type of additional environment");
                                                continue;
                                            }
                                        };
                                        extra_envs.insert(name.clone(), value.into_owned());
                                    }
                                }
                                let actions_in_transit = self.actions_in_transit.clone();
                                let worker_id = self.worker_id.clone();
                                let running_actions_manager = self.running_actions_manager.clone();
                                self.metrics.clone().wrap(move |metrics| async move {
                                    metrics.preconditions.wrap(preconditions_met(precondition_script_cfg, &extra_envs))
                                    .and_then(|()| running_actions_manager.create_and_add_action(worker_id, start_execute))
                                    .map(move |r| {
                                        // Now that we either failed or registered our action, we can
                                        // consider the action to no longer be in transit.
                                        actions_in_transit.fetch_sub(1, Ordering::Release);
                                        r
                                    })
                                    .and_then(|action| {
                                        debug!(
                                            operation_id = %action.get_operation_id(),
                                            "Received request to run action"
                                        );
                                        // LOAD-BEARING: this `action.clone()` is what closes
                                        // the AC-poisoning lifecycle race between
                                        // `cleanup_action()` removing the running_actions
                                        // map entry and the publish closure reading
                                        // `cancelled`. See §A3.3 regression test
                                        // (`cancel_then_cleanup_race_does_not_poison_ac`).
                                        // DO NOT remove this clone-into-publish-future
                                        // even if it appears unused; the load is what
                                        // reads the AtomicBool. The Arc keeps
                                        // RunningActionImpl alive across the publish
                                        // closure's lifetime regardless of whether
                                        // cleanup has run.
                                        let action_for_publish = action.clone();
                                        // Box each phase to heap-allocate its future state
                                        // separately. Without this, the compiler generates a
                                        // single monolithic state machine for the entire
                                        // AndThen chain, which overflows the 8 MiB stack in
                                        // debug builds.
                                        Box::pin(action.clone().prepare_action())
                                            .and_then(|a| Box::pin(RunningAction::execute(a)))
                                            // upload_results uploads outputs synchronously by default
                                            // (full FastSlowStore, both fast and slow stores). When the
                                            // F2 deferred_output_uploads_enabled kill-switch is ON, it
                                            // writes to the local fast store only; the remote slow-store
                                            // upload is handled by spawn_upload_to_remote after
                                            // execution_complete frees the worker slot.
                                            .and_then(|a| Box::pin(RunningAction::upload_results(a)))
                                            .and_then(|a| Box::pin(RunningAction::get_finished_result(a)))
                                            .then(|result| async move {
                                                // Spawn cleanup in the background — it only removes
                                                // the work directory (files already renamed into CAS).
                                                // The cleaning_up_operations + wait_for_cleanup mechanism
                                                // handles the race if the same action is retried.
                                                tokio::spawn(async move {
                                                    if let Err(e) = action.cleanup().await {
                                                        error!(?e, "Background cleanup failed");
                                                    }
                                                });
                                                // Reshape: (ActionResult, Arc<RunningActionImpl>)
                                                // so the publish closure can read `cancelled`
                                                // from the captured Arc per AC-poisoning fix
                                                // IC1 in v3-final design.
                                                result.map(|action_result| (action_result, action_for_publish))
                                            })
                                    }).await
                                })
                            };

                            let make_publish_future = {
                                let mut grpc_client = self.grpc_client.clone();
                                let use_tls = self.config.cas_server_tls.is_some();
                                let cas_endpoint_for_notify = self.config.cas_server_port
                                    .map(|port| cas_advertised_endpoint(port, use_tls))
                                    .unwrap_or_default();

                                // (FL-688 v3 §3.8 part 2 — post-action delta coverage)
                                // Capture the BlobsAvailable resend state so the
                                // post-action output-digest publish rides the
                                // ACKED/buffered/replayed chunked path instead of the
                                // fire-and-forget `blobs_available()` (which the server
                                // never acks). `None` on workers with no fast-store
                                // BlobsAvailable reporting — those fall back to the raw
                                // send (unchanged).
                                let blobs_available_state = self.blobs_available_state.clone();
                                let running_actions_manager = self.running_actions_manager.clone();
                                // AC-poisoning fix IC1: signature reshaped to accept
                                // (ActionResult, Arc<U::RunningAction>) so the
                                // captured Arc closes the lifecycle race with
                                // cleanup. The error path doesn't carry the Arc
                                // because there's no AC write to suppress on error.
                                // `is_cancelled()` is a default-`false` method on
                                // the `RunningAction` trait so test stubs don't
                                // have to implement it.
                                move |res: Result<(ActionResult, Arc<U::RunningAction>), Error>| async move {
                                    // (Probe #2) Total wall-clock time spent inside the
                                    // post-action publish closure (tree expansion +
                                    // BlobsAvailable + execution_response + execution_complete
                                    // + cache_action_result + spawn_upload_to_remote).
                                    // Captures the worker-side latency the scheduler waits on
                                    // between action completion and "worker free" so the
                                    // critical path is attributable when post-action work
                                    // queues up. Logged at the closure exit alongside the
                                    // existing phase6 boundary.
                                    let publish_closure_start = Instant::now();
                                    // Sample CPU at completion time, not action start time.
                                    let exec_load = get_cpu_load_pct();
                                    let exec_p_load = get_p_core_load_pct();
                                    let exec_e_load = get_e_core_load_pct();
                                    debug!("ExecuteComplete cpu_load_pct={exec_load} p_core={exec_p_load} e_core={exec_e_load}");
                                    let complete = ExecuteComplete {
                                        operation_id: operation_id.clone(),
                                        cpu_load_pct: exec_load,
                                        p_core_load_pct: exec_p_load,
                                        e_core_load_pct: exec_e_load,
                                    };
                                    let instance_name = maybe_instance_name
                                        .err_tip(|| "`instance_name` could not be resolved; this is likely an internal error in local_worker.")?;
                                    match res {
                                        Ok((mut action_result, action_for_publish)) => {
                                            // External-consistency invariant (#129): every
                                            // blob the action produced MUST be observable
                                            // from the server (in CAS or via locality_map
                                            // peer-fetch) BEFORE the client sees the
                                            // ExecuteResult. The server populates the
                                            // locality_map from BlobsAvailable; the
                                            // worker→scheduler stream is processed in
                                            // arrival order, so sending BlobsAvailable
                                            // FIRST guarantees the locality_map is
                                            // populated by the time the server processes
                                            // ExecuteResult and forwards it to the client.
                                            //
                                            // Without this ordering, Bazel reads of any
                                            // tree-internal file digest in the race window
                                            // (between ExecuteResult delivery and
                                            // BlobsAvailable processing) hit NotFound:
                                            // When F2 deferred_output_uploads_enabled is ON,
                                            // the slow-tier upload is fire-and-forget (step 5)
                                            // and may not have completed; AND the locality_map
                                            // has no peer registered if this message is skipped.
                                            // When F2 is OFF (default), the slow-tier upload
                                            // completed synchronously in upload_results, but
                                            // the locality_map must still be populated so that
                                            // tree-internal file digests are routable.
                                            //
                                            // 1. Tree expansion + BlobsAvailable on the
                                            //    critical path. Tree expansion reads Tree
                                            //    blobs from local CAS (just produced by
                                            //    upload_results, almost always hot in OS
                                            //    cache).
                                            if !cas_endpoint_for_notify.is_empty() {
                                                let mut output_digests = Vec::new();
                                                for file in &action_result.output_files {
                                                    output_digests.push(file.digest.into());
                                                }
                                                for folder in &action_result.output_folders {
                                                    output_digests.push(folder.tree_digest.into());
                                                }
                                                if action_result.stdout_digest.size_bytes() > 0 {
                                                    output_digests.push(action_result.stdout_digest.into());
                                                }
                                                if action_result.stderr_digest.size_bytes() > 0 {
                                                    output_digests.push(action_result.stderr_digest.into());
                                                }
                                                // Expand Tree protos to include individual
                                                // file digests in the locality map. Without
                                                // this, the server can't proxy reads for
                                                // tree file blobs until the background
                                                // upload completes.
                                                let tree_file_digests = running_actions_manager
                                                    .expand_tree_file_digests(
                                                        &action_result,
                                                        Some(&action_for_publish),
                                                    )
                                                    .await;
                                                output_digests.extend(tree_file_digests.into_iter().map(Into::into));

                                                if !output_digests.is_empty() {
                                                    let load = get_cpu_load_pct();
                                                    let p_load = get_p_core_load_pct();
                                                    let e_load = get_e_core_load_pct();
                                                    debug!("BlobsAvailable cpu_load_pct={load} p_core={p_load} e_core={e_load}");
                                                    let post_action_notification =
                                                        BlobsAvailableNotification {
                                                            worker_cas_endpoint: cas_endpoint_for_notify.clone(),
                                                            digests: output_digests,
                                                            is_full_snapshot: false,
                                                            evicted_digests: Vec::new(),
                                                            evicted_blob_infos: Vec::new(),
                                                            digest_infos: Vec::new(),
                                                            cpu_load_pct: load,
                                                            cached_directory_digests: Vec::new(),
                                                            added_subtree_digests: Vec::new(),
                                                            removed_subtree_digests: Vec::new(),
                                                            is_full_subtree_snapshot: false,
                                                            p_core_load_pct: p_load,
                                                            e_core_load_pct: e_load,
                                                            pinned_mirror_digests: Vec::new(),
                                                            mirror_used_bytes: 0,
                                                            mirror_max_bytes: 0,
                                                            pinned_mirror_entries: Vec::new(),
                                                            pinned_ac_mirror_entries: Vec::new(),
                                                            // (FL-681 re-saturation gate)
                                                            // Carry the AUTHORITATIVE
                                                            // indefinite-pin saturation on
                                                            // this one-shot post-action
                                                            // delta, read from the local CAS
                                                            // FilesystemStore via the SAME
                                                            // accessor the worker-side
                                                            // admission gate uses
                                                            // (`running_actions_manager.rs`
                                                            // `create_and_add_action`). The
                                                            // server applies this field
                                                            // UNCONDITIONALLY, so a blanket
                                                            // `false` here would clobber a
                                                            // prior `true` the instant an
                                                            // action completes — re-opening
                                                            // the re-saturation spin until
                                                            // the next heartbeat. The
                                                            // periodic heartbeat carries the
                                                            // same value as the routine
                                                            // refresh (≤`BLOBS_AVAILABLE_MAX_INTERVAL_MS`
                                                            // = 100 ms); the admission NAK
                                                            // remains the backstop.
                                                            indefinite_pin_saturated:
                                                                running_actions_manager
                                                                    .indefinite_pin_saturated(),
                                                            // Host swap pressure read from the
                                                            // process-global sampler atomics.
                                                            // Unlike mirror_*_bytes (which need a
                                                            // CAS-FSS handle not in scope here,
                                                            // hence 0), the sampler is global, so
                                                            // this one-shot delta reports the
                                                            // AUTHORITATIVE current value. The
                                                            // post-action sample is a PEAK (RSS
                                                            // not yet reclaimed); `memory_pressured`
                                                            // comes from the gate verdict
                                                            // (`swap_gate_pressured`: free-floor OR
                                                            // re-fault EWMA), never the raw peak, so
                                                            // one heavy action cannot look like
                                                            // sustained pressure (§3b).
                                                            swap_used_bytes: get_swap_used_bytes(),
                                                            memory_pressure_level:
                                                                get_memory_pressure_level(),
                                                            memory_pressured: swap_gate_pressured(),
                                                            // (F4) Disk pressure: same
                                                            // process-global sampler atomics. The
                                                            // wire boolean is the sampler verdict
                                                            // (advisory matcher hint); the
                                                            // authoritative gate adds the statvfs
                                                            // fallback on a stale sampler.
                                                            available_disk_bytes:
                                                                get_available_disk_bytes(),
                                                            disk_pressured:
                                                                disk_gate_sampler_pressured(),
                                                            // (#obs-tuning)
                                                            // OBSERVABILITY-ONLY cold
                                                            // dir-cache construct latency
                                                            // (p95 ms); the one-shot
                                                            // post-action delta carries
                                                            // the AUTHORITATIVE current
                                                            // value (global counters),
                                                            // like the periodic heartbeat.
                                                            construct_latency_ms_p95:
                                                                get_construct_latency_ms_p95(),
                                                        };
                                                    // (FL-688 v3 §3.8 part 2) Route the
                                                    // post-action output-digest delta through
                                                    // the ACKED/buffered/replayed chunked path
                                                    // (when this worker reports BlobsAvailable);
                                                    // a lost post-action delta is then
                                                    // retransmitted by the periodic replay
                                                    // reader until the server acks it, instead
                                                    // of being permanently lost. Workers with no
                                                    // BlobsAvailable state fall back to the raw
                                                    // fire-and-forget send (unchanged). A send
                                                    // failure is logged and swallowed — the
                                                    // action succeeded and the slow-tier upload
                                                    // is still scheduled; no worse than the
                                                    // pre-fix best-effort behaviour, and the
                                                    // buffered chunk (chunked arm) will replay.
                                                    if let Err(err) =
                                                        Self::send_post_action_blobs_available_delta(
                                                            &mut grpc_client,
                                                            blobs_available_state.as_ref(),
                                                            post_action_notification,
                                                        )
                                                        .await
                                                    {
                                                        warn!(?err, "Failed to send blobs_available notification");
                                                    }
                                                }
                                            }

                                            // 2. Send execution response. The server
                                            //    processes worker stream messages in
                                            //    arrival order; BlobsAvailable above is
                                            //    already enqueued so the locality_map will
                                            //    be populated before this ExecuteResult is
                                            //    forwarded to the client.
                                            let action_stage = ActionStage::Completed(action_result.clone());
                                            // #36 Phase 6 §6 Phase 0 probe P-WORKER-BOUNDARY:
                                            // capture op_id_n BEFORE move into ExecuteResult so we
                                            // can emit the action-boundary log AFTER the tonic-Ok
                                            // await returns. This marks the wall-clock point at
                                            // which Phase 6 would dispatch PreemptInputFetch(N+1).
                                            // No behaviour change — observability only.
                                            let phase6_op_id_n = operation_id.clone();
                                            grpc_client.execution_response(
                                                ExecuteResult{
                                                    instance_name,
                                                    operation_id,
                                                    result: Some(execute_result::Result::ExecuteResponse(action_stage.into())),
                                                    // task-resource-profile Phase 1 producer: the sole
                                                    // populated ExecuteResult site (the action ran and
                                                    // produced results). `get_resource_usage` returns
                                                    // `None` for un-sampled actions (the 1/16 calib poll
                                                    // didn't capture), so most completions still carry
                                                    // `None`. operation_id/worker_id are left empty for
                                                    // the server to fill from its transport-derived
                                                    // values (the existing `record_action_resource_usage`
                                                    // consumer; observe-only origin event).
                                                    resource_usage: action_for_publish.get_resource_usage(),
                                                }
                                            )
                                            .await
                                            .err_tip(|| "Error while calling execution_response")?;
                                            let phase6_tonic_ok_at_us = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .map(|d| d.as_micros() as u64)
                                                .unwrap_or(0);
                                            info!(
                                                tag = "phase6_worker_action_boundary",
                                                op_id_n = %phase6_op_id_n,
                                                tonic_ok_at_us = phase6_tonic_ok_at_us,
                                                "phase6 worker action boundary (tonic-Ok returned for action N)"
                                            );

                                            // 3. Free the worker for new actions.
                                            drop(grpc_client.execution_complete(complete).await);

                                            // 4. CAS upload — fire-and-forget; peers can
                                            //    already serve the blobs directly. Per v2
                                            //    §A1.2: AC-only suppression — CAS upload
                                            //    remains UNCONDITIONAL because CAS is
                                            //    content-addressed and uploaded blobs
                                            //    cannot poison. Reordered before Step 5
                                            //    (#O15) so we can move `action_result`
                                            //    into the detached AC-write task without
                                            //    a redundant clone here.
                                            running_actions_manager.spawn_upload_to_remote(&action_result, Some(&action_for_publish));

                                            // 5. AC write — detached into a background
                                            //    task (#O15) so the closure returns as
                                            //    soon as execution_complete + CAS
                                            //    dispatch have fired. The closure's
                                            //    wall-clock is no longer bounded by the
                                            //    AC write tail.
                                            //
                                            //    AC-poisoning fix residual-window guard
                                            //    (composite invariant Phase D of base
                                            //    design): a cancel signal arriving via
                                            //    KillOperationRequest BEFORE this AC
                                            //    write fires must suppress the write.
                                            //    `RunningAction::is_cancelled()` is an
                                            //    Acquire load on the AtomicBool set by
                                            //    `RunningActionsManagerImpl::kill_operation`
                                            //    (Release store, paired). The Arc was
                                            //    captured BEFORE the
                                            //    `.then(spawn(cleanup))` chain so
                                            //    cleanup removing the running_actions
                                            //    map entry cannot race with this read.
                                            //    The check moves INSIDE the spawn so a
                                            //    kill arriving while the spawn is queued
                                            //    still suppresses the write.
                                            //
                                            //    Hoist the DigestInfo try_into OUTSIDE
                                            //    the spawn so an early-None exit avoids
                                            //    the spawn allocation entirely.
                                            let ac_write_digest_info: Option<DigestInfo> =
                                                action_digest.as_ref().and_then(|d| d.clone().try_into().ok());
                                            if let Some(digest_info) = ac_write_digest_info {
                                                // Non-blocking permit acquire: at-cap
                                                // saturation must NOT block the publish
                                                // closure (would defeat the whole
                                                // detach). Closure returns synchronously
                                                // on cap-reached; the AC entry is lost,
                                                // Bazel re-executes on next miss.
                                                match Arc::clone(&self.ac_write_semaphore).try_acquire_owned() {
                                                    Ok(permit) => {
                                                        let ac_write_action_digest = action_digest.clone();
                                                        let ac_write_running_actions_manager = running_actions_manager.clone();
                                                        let ac_write_worker_id = self.worker_id.clone();
                                                        let ac_write_action_for_publish = action_for_publish.clone();
                                                        let inflight_count = Arc::clone(&self.ac_write_detached_inflight_count);
                                                        // Increment AT permit acquire;
                                                        // decrement on spawn-body exit
                                                        // via RAII guard. Holds the
                                                        // permit alive for the whole
                                                        // body so the cap is honored.
                                                        inflight_count.fetch_add(
                                                            1,
                                                            core::sync::atomic::Ordering::AcqRel,
                                                        );
                                                        let guard = AcWriteInflightGuard {
                                                            inflight_count: Arc::clone(&inflight_count),
                                                            _permit: permit,
                                                        };
                                                        tokio::spawn(async move {
                                                            let _guard = guard;
                                                            // Re-check cancel INSIDE the
                                                            // spawn so a kill arriving
                                                            // between closure return and
                                                            // AC write still suppresses.
                                                            if ac_write_action_for_publish.is_cancelled() {
                                                                nativelink_util::metrics::CANCEL
                                                                    .ac_writes_suppressed_due_to_cancel
                                                                    .add(1, &[]);
                                                                warn!(
                                                                    operation_id = %ac_write_action_for_publish.get_operation_id(),
                                                                    "AC write suppressed: action was cancelled in residual window"
                                                                );
                                                                return;
                                                            }
                                                            // #37 Phase 2 (Q1): thread op_id + worker_id
                                                            // to the AC publish path so the FSS-level
                                                            // failure log carries action attribution.
                                                            let op_id_for_publish = ac_write_action_for_publish.get_operation_id().clone();
                                                            let started = std::time::Instant::now();
                                                            if let Err(err) = ac_write_running_actions_manager.cache_action_result(
                                                                digest_info,
                                                                &mut action_result,
                                                                digest_hasher,
                                                                &op_id_for_publish,
                                                                &ac_write_worker_id,
                                                            ).await {
                                                                error!(
                                                                    ?err,
                                                                    ac_write_action_digest = ?ac_write_action_digest,
                                                                    op_id = %op_id_for_publish,
                                                                    elapsed_ms = started.elapsed().as_millis() as u64,
                                                                    "Error saving action in store",
                                                                );
                                                            }
                                                        });
                                                    }
                                                    Err(_) => {
                                                        // Cap saturated: AC-store stall
                                                        // ×  burst. Skip synchronously;
                                                        // closure must not block.
                                                        nativelink_util::metrics::CANCEL
                                                            .ac_writes_dropped_due_to_cap
                                                            .add(1, &[]);
                                                        warn!(
                                                            operation_id = %action_for_publish.get_operation_id(),
                                                            "AC write detached-spawn cap reached; AC entry will be retried on next action ingress via cache-miss recovery"
                                                        );
                                                    }
                                                }
                                            }
                                            // #O15 (2026-06-07): probe marking publish-closure
                                            // body return. The parallel
                                            // `publish_closure_total_ms` timing wrapper
                                            // emits at the same wall-clock point; this
                                            // debug! gives the closure-detach contract
                                            // test a stable hook to assert the closure
                                            // returned within bounded time even when
                                            // the AC write is artificially gated.
                                            debug!(
                                                tag = "publish_closure_returned",
                                                operation_id = %action_for_publish.get_operation_id(),
                                                "publish closure body returned; ac write detached"
                                            );
                                        },
                                        Err(e) => {
                                            // Still notify completion on error so the worker
                                            // is freed for new work.
                                            drop(grpc_client.execution_complete(complete).await);

                                            // Only convert to FAILED_PRECONDITION if this
                                            // is a CAS blob miss (from FastSlowStore). Other
                                            // NotFound errors (e.g., command binary not found,
                                            // missing output files) should propagate as-is.
                                            // `is_cas_blob_miss` keys on the structural
                                            // `PreconditionFailure` detail (preferred) AND
                                            // the legacy `"not found in"` substring; see
                                            // doc-comment for #428 / #410 rationale.
                                            if is_cas_blob_miss(&e) {
                                                // Per REAPI spec, missing inputs should return
                                                // FAILED_PRECONDITION so the client re-uploads.
                                                warn!(
                                                    ?e,
                                                    "Missing CAS inputs, returning FAILED_PRECONDITION"
                                                );
                                                // Re-stamp the code without losing the
                                                // attached PreconditionFailure details —
                                                // `make_err!` would drop them, breaking
                                                // Bazel's REAPI v2 §2.2.4 recovery path.
                                                let mut translated = e;
                                                translated.code = Code::FailedPrecondition;
                                                let action_result = ActionResult {
                                                    error: Some(translated),
                                                    ..ActionResult::default()
                                                };
                                                let action_stage = ActionStage::Completed(action_result);
                                                grpc_client.execution_response(ExecuteResult{
                                                    instance_name,
                                                    operation_id,
                                                    result: Some(execute_result::Result::ExecuteResponse(action_stage.into())),
                                                    resource_usage: None,
                                                }).await.err_tip(|| "Error calling execution_response with missing inputs")?;
                                            } else {
                                                grpc_client.execution_response(ExecuteResult{
                                                    instance_name,
                                                    operation_id,
                                                    result: Some(execute_result::Result::InternalError(e.into())),
                                                    resource_usage: None,
                                                }).await.err_tip(|| "Error calling execution_response with error")?;
                                            }
                                        },
                                    }
                                    // (Probe #2) Closure exit boundary — total ms spent
                                    // inside this publish closure across both Ok and Err
                                    // arms. Single emission point catches every successful
                                    // exit; `?`-propagated errors inside the arms are rare
                                    // and skip this log intentionally.
                                    let publish_closure_total_ms =
                                        publish_closure_start.elapsed().as_millis() as u64;
                                    info!(
                                        publish_closure_total_ms,
                                        "publish closure complete"
                                    );
                                    Ok(())
                                }
                            };

                            self.actions_in_transit.fetch_add(1, Ordering::Release);

                            let add_future_channel = add_future_channel.clone();

                            info_span!(
                                "worker_start_action_ctx",
                                operation_id = operation_id_to_log,
                                digest_function = %digest_hasher.to_string(),
                            ).in_scope(|| {
                                let _guard = Context::current_with_value(digest_hasher)
                                    .attach();

                                let actions_in_flight = actions_in_flight.clone();
                                let actions_notify = actions_notify.clone();
                                let actions_in_flight_fail = actions_in_flight.clone();
                                let actions_notify_fail = actions_notify.clone();
                                actions_in_flight.fetch_add(1, Ordering::Release);

                                futures.push(
                                    spawn!("worker_start_action", start_action_fut).map(move |res| {
                                        let res = res.err_tip(|| "Failed to launch spawn")?;
                                        if let Err(err) = &res {
                                            error!(?err, "Error executing action");
                                        }
                                        add_future_channel
                                            .send(make_publish_future(res).then(move |res| {
                                                actions_in_flight.fetch_sub(1, Ordering::Release);
                                                actions_notify.notify_one();
                                                core::future::ready(res)
                                            }).boxed())
                                            .map_err(|_| make_err!(Code::Internal, "LocalWorker could not send future"))?;
                                        Ok(())
                                    })
                                    .or_else(move |err| {
                                        // If the make_publish_future is not run we still need to notify.
                                        actions_in_flight_fail.fetch_sub(1, Ordering::Release);
                                        actions_notify_fail.notify_one();
                                        core::future::ready(Err(err))
                                    })
                                    .boxed()
                                );
                            });
                        }
                    }
                },
                res = add_future_rx.next() => {
                    let fut = res.err_tip(|| "New future stream receives should never be closed")?;
                    futures.push(fut);
                },
                res = futures.next() => res.err_tip(|| "Keep-alive should always pending. Likely unable to send data to scheduler")??,
                complete_msg = shutdown_rx.recv().fuse() => {
                    warn!("Worker loop received shutdown signal. Shutting down worker...",);
                    // Signal the worker CAS server to stop accepting new
                    // connections and drain in-flight blob transfers.
                    if let Some(tx) = self.cas_shutdown_tx {
                        let _ = tx.send(true);
                    }
                    let mut grpc_client = self.grpc_client.clone();
                    let shutdown_guard = complete_msg.map_err(|e| make_err!(Code::Internal, "Failed to receive shutdown message: {e:?}"))?;
                    let actions_in_flight = actions_in_flight.clone();
                    let actions_notify = actions_notify.clone();
                    let shutdown_future = async move {
                        // Wait for in-flight operations to be fully completed.
                        // #95: subscribe-before-predicate. Construct the
                        // `notified()` future and arm it via `enable()`
                        // BEFORE loading `actions_in_flight`. Any decrement
                        // (and accompanying `notify_one()` from the
                        // running-action completion sites at line ~2455
                        // and ~2464) issued from this point on is
                        // captured by the pre-armed Notified, even if it
                        // fires between the load and the await. Same
                        // shape as the cleanup_wait_notify reference in
                        // running_actions_manager.rs (~line 5720-5759)
                        // and #92.
                        loop {
                            let notified = actions_notify.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            if actions_in_flight.load(Ordering::Acquire) == 0 {
                                break;
                            }
                            notified.as_mut().await;
                        }
                        // Sending this message immediately evicts all jobs from
                        // this worker, of which there should be none.
                        if let Err(e) = grpc_client.going_away(GoingAwayRequest {}).await {
                            error!("Failed to send GoingAwayRequest: {e}",);
                            return Err(e);
                        }
                        // Allow shutdown to occur now.
                        drop(shutdown_guard);
                        Ok::<(), Error>(())
                    };
                    futures.push(shutdown_future.boxed());
                    shutting_down = true;
                },
                () = &mut reconcile_fail_open => {
                    // (FL-688 v3 Stage C — MAJOR-2) Bounded fail-open for the
                    // startup reconcile gate. Fires after 2 × DRAIN_INTERVAL_SECS
                    // (= 20s) if ReconcileComplete has not been received. Protects
                    // against a rolling deploy where the old server (pre-v3) sends
                    // StartAction without sending ReconcileComplete — the gate would
                    // otherwise block action execution permanently and the NAKs
                    // (Code::ResourceExhausted) burn scheduler retries until the
                    // action is abandoned.
                    //
                    // `FutureExt::fuse()` ensures this arm fires EXACTLY ONCE and is
                    // never re-polled (futures::select! semantics). If ReconcileComplete
                    // was already received (gate released before the timer), the load
                    // returns `true` and the release calls are no-ops.
                    //
                    // FAIL-OPEN semantics: resume normal pinned-LRU eviction. Blobs
                    // that were reconcile-pinned (via UploadMissingBlobs) stay pinned
                    // until BIS-ack. Only blobs NOT pinned become LRU-evictable. No
                    // data loss beyond the acknowledged item-D residual (R3).
                    if !reconcile_complete.load(Ordering::Acquire) {
                        warn!(
                            "startup reconcile gate fail-open after {}s: ReconcileComplete \
                             not received from server (rolling deploy?); releasing gate to \
                             prevent permanent action-execution blockage",
                            RECONCILE_FAIL_OPEN_SECS
                        );
                        // (FL-688 v3 Stage C — over-cap metric) Count fail-opens so
                        // operators can alert on rolling-deploy / version-mismatch events
                        // where the 20s gate-armed window was exposed.
                        self.metrics.reconcile_gate_fail_open_total.inc();
                        if let Some(ref state) = self.blobs_available_state {
                            // `release_startup_reconcile_gate()` stores `true` to the
                            // shared `reconcile_complete` Arc — the same Arc that
                            // `reconcile_complete` here is cloned from
                            // (`reconcile_complete_flag()`). No second store needed
                            // (matches the ReconcileComplete handler at :4796).
                            state.fs_store.release_startup_reconcile_gate();
                        } else {
                            // No FilesystemStore fast tier: gate was never armed via the
                            // store, so write directly to the shared Arc (this is the
                            // ONLY release path when blobs_available_state is None).
                            reconcile_complete.store(true, Ordering::Release);
                        }
                    }
                },
            };
        }
        // Unreachable.
    }
}

/// FL-681 Follow-up B test seam: drive one
/// [`LocalWorkerImpl::send_periodic_blobs_available`] tick from the integration
/// test crate (where the `WorkerApiClientTrait` / `RunningActionsManager` mocks
/// live), so the heartbeat CAS-pin re-advertisement wiring is exercised
/// end-to-end without standing up the full worker `run` loop. Same module so it
/// can reach the private associated fn; `pub` + the test-utils cfg so the
/// integration crate can call it. `LocalWorkerImpl` itself stays private.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub async fn send_periodic_blobs_available_for_test<
    T: WorkerApiClientTrait + 'static,
    U: RunningActionsManager,
>(
    grpc_client: &mut T,
    state: &BlobsAvailableState,
    running_actions_manager: &Arc<U>,
    is_first: bool,
) -> Result<(), Error> {
    LocalWorkerImpl::<T, U>::send_periodic_blobs_available(
        grpc_client,
        state,
        running_actions_manager,
        is_first,
    )
    .await
}

/// (FL-688 v3 §3.8 part 2) Test seam: drive the production post-action
/// output-digest publish routing
/// ([`LocalWorkerImpl::send_post_action_blobs_available_delta`]) directly,
/// so the chunk-and-buffer-on-ACK behavior of the post-action delta is
/// exercised without standing up a full action through the publish closure.
/// `U` is named only to satisfy the associated-fn's type parameter (the fn
/// itself touches only `T` + the resend state); pass any
/// `RunningActionsManager` mock.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub async fn send_post_action_blobs_available_delta_for_test<
    T: WorkerApiClientTrait + 'static,
    U: RunningActionsManager,
>(
    grpc_client: &mut T,
    state: Option<&BlobsAvailableState>,
    notification: BlobsAvailableNotification,
) -> Result<(), Error> {
    LocalWorkerImpl::<T, U>::send_post_action_blobs_available_delta(
        grpc_client,
        state,
        notification,
    )
    .await
}

/// Test seam (#FL-688 §4): drive the production backfill upload handler
/// `LocalWorkerImpl::handle_upload_missing_blobs` against a real
/// `RunningActionsManager` whose `get_cas_store()` returns a
/// production-composed `FastSlowStore`. Exposed so the retry-until-durable
/// regression test exercises the EXACT per-blob Err arm (W4) — including
/// the re-queue into `failed_slow_writes` — rather than re-implementing the
/// upload sequence by hand (which the legacy
/// `end_to_end_mirror_reconciliation_test` does and which therefore cannot
/// observe the W4 drop-vs-requeue contract).
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub async fn handle_upload_missing_blobs_for_test<
    T: WorkerApiClientTrait + 'static,
    U: RunningActionsManager,
>(
    running_actions_manager: &Arc<U>,
    digests: Vec<DigestInfo>,
    max_concurrent_uploads: usize,
) {
    LocalWorkerImpl::<T, U>::handle_upload_missing_blobs(
        running_actions_manager,
        digests,
        max_concurrent_uploads,
    )
    .await;
}

type ConnectionFactory<T> = Box<dyn Fn() -> BoxFuture<'static, Result<T, Error>> + Send + Sync>;

pub struct LocalWorker<T: WorkerApiClientTrait + 'static, U: RunningActionsManager> {
    config: Arc<LocalWorkerConfig>,
    running_actions_manager: Arc<U>,
    connection_factory: ConnectionFactory<T>,
    sleep_fn: Option<Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>>,
    metrics: Arc<Metrics>,
    /// State for periodic BlobsAvailable reporting.
    blobs_available_state: Option<BlobsAvailableState>,
    /// Worker-global locality map shared with `WorkerProxyStore`. Forwarded
    /// to `LocalWorkerImpl` so `Update::ChunkedMessage(PeerHints)` chunks
    /// can register hints directly without going through the action
    /// manager (#98 — peer-hints chunking, direct-merge design).
    peer_locality_map: Option<SharedBlobLocalityMap>,
    /// Guards for the worker CAS server tasks (TCP + QUIC). Keeps the tasks
    /// alive as long as the `LocalWorker` is alive. When dropped, servers abort.
    _cas_server_guards: Vec<JoinHandleDropGuard<Result<(), Error>>>,
    /// Signals the worker CAS server to stop accepting connections during
    /// graceful shutdown. Sent `true` when the worker receives SIGTERM.
    cas_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// #O15 (2026-06-07): semaphore capping detached AC-write inflight
    /// tasks at `AC_WRITE_DETACHED_INFLIGHT_CAP`. Lives on `LocalWorker`
    /// (not `LocalWorkerImpl`) so the cap persists across scheduler
    /// reconnects.
    ac_write_semaphore: Arc<Semaphore>,
    /// #O15 (2026-06-07): inflight gauge for detached AC writes. Same
    /// `Arc<AtomicI64>` is registered into `Metrics` so scrapes see the
    /// live count.
    ac_write_detached_inflight_count: Arc<core::sync::atomic::AtomicI64>,
}

impl<
    T: WorkerApiClientTrait + core::fmt::Debug + 'static,
    U: RunningActionsManager + core::fmt::Debug,
> core::fmt::Debug for LocalWorker<T, U>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LocalWorker")
            .field("config", &self.config)
            .field("running_actions_manager", &self.running_actions_manager)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

/// Metrics registry prefix for the worker's EXECUTION `FastSlowStore`
/// subtree. Dots become underscores in the rendered Prometheus name, so
/// counters appear as `nativelink_WORKER_EXEC_FAST_SLOW_STORE_...`. Chosen
/// DISTINCT from `WORKER_FAST_SLOW_STORE` (the idle CAS-server instance
/// registered via `store_manager` in `src/bin/nativelink.rs`) so the two
/// subtrees are unmistakable on `/metrics` — see
/// [`register_execution_store_metrics`] for the two-instance topology.
pub const WORKER_EXEC_FSS_METRIC_PREFIX: &str = "nativelink.WORKER_EXEC_FAST_SLOW_STORE";

/// Register the worker's EXECUTION `FastSlowStore` metrics subtree into the
/// process [`MetricsRegistry`] so its previously-dark read-path and
/// peer-fetch counters render on `/metrics`.
///
/// # Two-`FastSlowStore`-instance topology (root cause of the dark counters)
///
/// A worker with `cas_server_port` set runs TWO `FastSlowStore` instances for
/// `WORKER_FAST_SLOW_STORE`:
///
/// 1. **Config-built container instance** — built by `build_store_manager`
///    from `cfg.stores`, wrapped in a `WorkerProxyStore`, and registered via
///    `metrics_registry.register("nativelink", store_manager)`
///    (`src/bin/nativelink.rs`). It is a tier-container from which the
///    execution instance (#2) and a SEPARATE read-only CAS-server instance
///    (`effective_cas_store_for_cas_server`, served at `cas_server_port`,
///    e.g. `:50051`) are derived; it does NOT itself serve the CAS server and
///    is otherwise near-idle, so its `nativelink_WORKER_FAST_SLOW_STORE_...`
///    subtree reads ~0.
///
/// 2. **Execution instance** — a FRESH `FastSlowStore` (`effective_cas_store`)
///    built inside [`new_local_worker`] whose slow tier is a worker-local
///    `WorkerProxyStore` fed by the worker-local peer-locality map. This is the
///    store the `RunningActionsManager` reads/writes to materialize action
///    inputs and store outputs. It is NOT in `store_manager`, so it never
///    reached the registry: its read-path (`fast_store_hit_count`,
///    `slow_store_hit_count`, `populate_spawn_count`) and peer-fetch
///    (`worker_proxy_peer_fetch_*`, `wps_worker_read_inner_hit_total`,
///    `singleflight_*`) counters were structurally dark while the shared
///    `FilesystemStore` fast tier (`evicting_map_*`) and the process-singleton
///    `dir_cache_*` / `o11_*` families rendered normally.
///
/// This registers instance #2 under [`WORKER_EXEC_FSS_METRIC_PREFIX`], and is
/// only wired when `cas_server_port` is set (the condition under which the
/// distinct instance #2 exists — see `src/bin/nativelink.rs`); with no port,
/// `effective_cas_store` IS instance #1 and re-registering it would duplicate
/// the whole subtree. Late registration is safe: `MetricsRegistry` is
/// `Arc<Mutex<Vec<..>>>` and `render_prometheus` snapshots the live component
/// list at scrape time, so a registration performed after the metrics service
/// was wired still renders.
///
/// NOTE (shared fast tier): instance #2 SHARES the `FilesystemStore` fast-tier
/// Arc with instance #1, so the fast-tier families (`evicting_map_*`,
/// `fast_store_*`) re-render under BOTH prefixes with identical values. Key on
/// the execution-instance-DISTINCT families — `fast_store_hit_count` /
/// `slow_store_hit_count` / `populate_spawn_count` (FSS-level) and
/// `worker_proxy_peer_fetch_*` / `wps_worker_read_inner_hit_total` /
/// `singleflight_*` (slow-tier WPS) — and do NOT sum `evicting_map_*` across
/// the two prefixes (that double-counts disk).
///
/// Observability-only: no runtime path changes — the counters already
/// increment on the execution instance; they were simply never rendered.
pub fn register_execution_store_metrics(
    metrics_registry: &MetricsRegistry,
    execution_fast_slow_store: Arc<FastSlowStore>,
) {
    metrics_registry.register(WORKER_EXEC_FSS_METRIC_PREFIX, execution_fast_slow_store);
}

/// Creates a new `LocalWorker`. The `cas_store` must be an instance of
/// `FastSlowStore` and will be checked at runtime.
///
/// `ac_store_name` is the configured store name (e.g. `"AC_MAIN_STORE"`)
/// for the AC store. When the AC store wraps a `FastSlowStore` (the
/// production shape), the worker registers AC pin entries in the FSS's
/// `dispatched_mirror_pins` index after each `upload_ac_results` so the
/// next `BlobsAvailable` tick advertises them via the dedicated
/// `pinned_ac_mirror_entries` field (proto field 17). `None` ⇒ no AC
/// store configured / no name to advertise.
pub async fn new_local_worker(
    config: Arc<LocalWorkerConfig>,
    cas_store: Store,
    ac_store: Option<Store>,
    ac_store_name: Option<String>,
    historical_store: Store,
) -> Result<LocalWorker<WorkerApiClientWrapper, RunningActionsManagerImpl>, Error> {
    // (#37 re-enable follow-up) Set the memory gate enable flag from config
    // ONCE before the sampler starts. `AtomicBool` `Relaxed` is sufficient
    // because `start_cpu_sampler` spawns the sampler thread after this store,
    // and the spawned thread's `Relaxed` reads are ordered after the spawn
    // (happens-before via the thread spawn). Default false = DISABLED.
    MEMORY_GATE_ENABLED.store(config.memory_gate_enabled, Ordering::Relaxed);
    // (#task-memgate-twosignal) Set the sustained-swapin OOM threshold + window
    // from config ONCE, also before the sampler starts. Same Relaxed
    // justification as MEMORY_GATE_ENABLED above: startup store happens-before the
    // sampler-thread spawn. Defaults 100/s + 10 ticks (see the statics). The
    // deprecated `memory_gate_refault_confirm_rate` config field is accepted for
    // back-compat (deny_unknown_fields) but is NO LONGER wired — the decompress
    // signal now feeds only the graded churn scalar, never a NAK.
    SWAPIN_CONFIRM_RATE.store(config.memory_gate_swapin_confirm_rate.get(), Ordering::Relaxed);
    SWAPIN_CONFIRM_WINDOW_TICKS.store(
        config.memory_gate_swapin_confirm_window_ticks.get(),
        Ordering::Relaxed,
    );

    // (F4) Pass work_directory so the disk-free sampler statvfs's the
    // CAS/work_directory volume (shared physical disk by config invariant).
    start_cpu_sampler(&config.work_directory)?;

    // #85 P4 (2026-06-07): periodic load_avg + mem_avail sampler.
    // macOS-only; no-op on Linux (server doesn't need it).
    ::nativelink_util::o11_probes::spawn_system_metrics_sampler();

    // F2 startup guard: deferred_output_uploads_enabled requires cas_server_port.
    // Without a CAS endpoint, BlobsAvailable is never sent
    // (local_worker.rs gating condition: `!cas_endpoint_for_notify.is_empty()`),
    // so the server locality map stays empty. During the deferred window a Bazel
    // FindMissingBlobs/Read returns NotFound with no client-visible error —
    // reopening the 2013977a hole in this config combination (#F2).
    if config.deferred_output_uploads_enabled && config.cas_server_port.is_none() {
        return Err(make_input_err!(
            "deferred_output_uploads_enabled requires cas_server_port to be set — \
            without a CAS endpoint, BlobsAvailable is never sent and deferred \
            outputs are unroutable during the upload window (#F2)"
        ));
    }

    let fast_slow_store = cas_store
        .downcast_ref::<FastSlowStore>(None)
        .err_tip(|| "Expected store for LocalWorker's store to be a FastSlowStore")?
        .get_arc()
        .err_tip(|| "FastSlowStore's Arc doesn't exist")?;

    // Log warning about CAS configuration for multi-worker setups
    event!(
        Level::INFO,
        worker_name = %config.name,
        "Starting worker '{}'. IMPORTANT: If running multiple workers, all workers \
        must share the same CAS storage path to avoid 'Object not found' errors.",
        config.name
    );

    if let Ok(path) = fs::canonicalize(&config.work_directory).await {
        fs::remove_dir_all(&path).await.err_tip(|| {
            format!(
                "Could not remove work_directory '{}' in LocalWorker",
                &path.as_path().to_str().unwrap_or("bad path")
            )
        })?;
    }

    fs::create_dir_all(&config.work_directory)
        .await
        .err_tip(|| format!("Could not make work_directory : {}", config.work_directory))?;

    // FL-1383 chunk 2a: worker-side portable rustc-incremental provisioning +
    // §12 startup asserts. INERT when the gate is off (returns immediately with
    // no filesystem touch). When on, this ONLY provisions FIXED_PREFIX + asserts
    // the host-provisioning invariants and gates; on any assert failure it logs
    // loudly and leaves the feature DISABLED (the worker is NOT panicked). The
    // execution-path rewire (make_action_directory → <FIXED_PREFIX>/<targetkey>,
    // chdir, wipe, lease) that CONSUMES this provision is chunk 2b —
    // TODO(#FL-1383). Runs AFTER work_directory exists so the EXDEV probe can
    // hardlink FIXED_PREFIX → execroot volume.
    let _portable_incr_provision = crate::portable_incr::provision_and_assert(
        config.portable_incr.enabled,
        config
            .portable_incr_fixed_prefix
            .as_deref()
            .map(std::path::Path::new),
        config
            .portable_incr_sysroot_path
            .as_deref()
            .map(std::path::Path::new),
        std::path::Path::new(&config.work_directory),
    )
    .await;

    let entrypoint = if config.entrypoint.is_empty() {
        None
    } else {
        Some(config.entrypoint.clone())
    };
    let max_action_timeout = if config.max_action_timeout_s == 0 {
        DEFAULT_MAX_ACTION_TIMEOUT
    } else {
        Duration::from_secs(config.max_action_timeout_s as u64)
    };
    let max_upload_timeout = if config.max_upload_timeout_s == 0 {
        DEFAULT_MAX_UPLOAD_TIMEOUT
    } else {
        Duration::from_secs(config.max_upload_timeout_s as u64)
    };

    // Whether the worker CAS server uses TLS (determines grpc:// vs grpcs:// in
    // the advertised endpoint).
    let use_tls = config.cas_server_tls.is_some();

    // If peer blob sharing is configured (cas_server_port is set), create a
    // worker-local locality map and wrap the slow store with WorkerProxyStore.
    // This enables workers to fetch blobs from peers instead of the central CAS.
    let (effective_cas_store, peer_locality_map) = if config.cas_server_port.is_some() {
        let locality_map = nativelink_util::blob_locality_map::new_shared_blob_locality_map();

        // Wrap the slow store (central CAS) with WorkerProxyStore.
        // Enable racing so the worker races peer fetches against server fetches.
        let slow_store = fast_slow_store.slow_store().clone();
        let mut proxy_arc = nativelink_store::worker_proxy_store::WorkerProxyStore::new(
            slow_store,
            locality_map.clone(),
        );
        Arc::get_mut(&mut proxy_arc)
            .expect("WorkerProxyStore just created, no other refs")
            .enable_race_peers();
        let proxy_store = Store::new(proxy_arc);

        // Build a new FastSlowStore: fast=local disk, slow=WorkerProxyStore(central CAS).
        // Preserve the original store's direction config so that e.g.
        // slow_direction=get prevents uploads from propagating to the server.
        //
        // Sibling-bug audit (review #7): `.fast_store()` here is store
        // *construction*, not a `has_with_results` lookup. We are wrapping
        // the on-disk `FilesystemStore` into a NEW `FastSlowStore` that
        // gets its own empty `mirror_blobs` map. There is no missed-mirror
        // hit risk because the new wrapper has no mirror state yet.
        // Construction-time wiring: extract the existing fast/slow handles to
        // re-wrap them in a new FastSlowStore — there is no wrapper to route
        // through here because the new wrapper does not exist yet.
        #[allow(clippy::disallowed_methods)]
        let fast_store = fast_slow_store.fast_store().clone();
        let fss_spec = nativelink_config::stores::FastSlowSpec {
            fast: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            slow: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            fast_direction: fast_slow_store.fast_direction(),
            slow_direction: fast_slow_store.slow_direction(),
            // Worker-side wrapper FSS; the chunked-read cascade lives
            // on the server, never on the worker, so leave OFF.
            chunked_reads_enabled: false,
            // #334 Fix B SLOW_WRITES_IN_FLIGHT cap intentionally
            // disabled on worker: the worker's slow tier is
            // GrpcStore→server, whose admission is bounded by upstream
            // h2 windowing + the server-side cap on its own
            // `FastSlowStore`. Double-capping here would short-circuit
            // the existing budget tracker without the typed-signal
            // benefit. The 2026-05-08 production OOM was on buildcache
            // (server), not on workers. If a future worker FastSlow
            // composition uses a cap-bounded slow tier (e.g. local
            // disk with its own backpressure), set this explicitly.
            //
            // Bundle fixup #6 (red-team #1) also changed the spec
            // default to 0; this `: 0` is now redundant with the
            // default but kept explicit so removing the default later
            // wouldn't silently re-introduce a cap on workers.
            slow_writes_in_flight_max_bytes: 0,
            // Upstream #2415 opt-in leader/follower dedup bypass for huge
            // blobs; 0 = disabled (default), preserving prior behavior.
            bypass_dedup_threshold_bytes: 0,
        };
        let new_fss = FastSlowStore::new(&fss_spec, fast_store, proxy_store);
        info!("Peer blob sharing enabled: wrapping slow store with WorkerProxyStore");

        (new_fss, Some(locality_map))
    } else {
        (fast_slow_store.clone(), None)
    };
    // #37 Phase 2 (Q4): tag the worker's CAS FastSlowStore with
    // store_class = "cas" so the background slow-tier failure log
    // carries the discriminator that lets operators grep
    // worker_slow_tier_async_fail{store_class=cas} separately from
    // the AC tier (tagged below at AC FSS construction).
    effective_cas_store.set_store_class("cas");
    // #37 Phase 2 (Q4 / F1): metric sink for the CAS FSS is installed
    // below at the same site as the AC FSS sink (after `ac_publish_metrics`
    // — the shared Metrics handle — is constructed).

    // Initialize directory cache if configured.
    // This is done after effective_cas_store is created so the cache can use
    // the same FastSlowStore (with WorkerProxyStore) for batch downloads.
    let directory_cache = if let Some(cache_config) = &config.directory_cache {
        use std::path::PathBuf;

        use crate::directory_cache::{
            DirectoryCache, DirectoryCacheConfig as WorkerDirCacheConfig,
        };

        let cache_root = if cache_config.cache_root.is_empty() {
            PathBuf::from(&config.work_directory).parent().map_or_else(
                || PathBuf::from("/tmp/nativelink_directory_cache"),
                |p| p.join("directory_cache"),
            )
        } else {
            PathBuf::from(&cache_config.cache_root)
        };

        let worker_cache_config = WorkerDirCacheConfig {
            max_entries: cache_config.max_entries,
            max_size_bytes: cache_config.max_size_bytes,
            cache_root,
            direct_use_mode: cache_config.direct_use_mode,
        };

        match DirectoryCache::new(
            worker_cache_config,
            Store::new(effective_cas_store.clone()),
            Some(effective_cas_store.clone()),
        )
        .await
        {
            Ok(cache) => {
                tracing::info!("Directory cache initialized successfully");
                Some(Arc::new(cache))
            }
            Err(e) => {
                tracing::warn!("Failed to initialize directory cache: {:?}", e);
                None
            }
        }
    } else {
        None
    };

    // The worker CAS server (which receives mirror writes from the server)
    // uses a separate FastSlowStore with slow_direction=ReadOnly. This
    // prevents mirror writes from being uploaded back to the server —
    // the blob is written to the local FilesystemStore only and pinned.
    // The server will ack via BlobsInStableStorage to unpin, or request
    // re-upload via UploadMissingBlobs on reconnect if it lost the blob.
    //
    // Both stores share the same failed_slow_writes set so that the
    // reconnect retry (which drains from the RunningActionsManager's
    // store) also picks up unacked mirror digests.
    //
    // `with_local_only_reads()` hard-codes p2p-source-only mode: the
    // public CAS server's reads MUST never fall through to the slow tier
    // (`GrpcStore`→server). On local miss we return NotFound so the
    // asking server routes to a different peer or serves from its own
    // CAS, instead of bouncing the request back through this worker's
    // slow tier — which would loop straight back to the same worker via
    // the locality map and wedge both ends. The regular `effective_cas_store`
    // above keeps its slow tier active for action input fetches inside
    // `RunningActionsManager`.
    let effective_cas_store_for_cas_server = {
        // Sibling-bug audit (review #7): `.fast_store()` here is store
        // *construction*. We rebuild a sibling FastSlowStore with the
        // same on-disk fast tier but ReadOnly slow direction. The new
        // wrapper has its own empty `mirror_blobs` map and is the one
        // that subsequently receives `IS_MIRROR_REQUEST` writes via the
        // CAS server, so the empty start state is correct.
        // Construction-time wiring: building a sibling FastSlowStore that
        // shares the same fast/slow store handles but flips slow_direction to
        // ReadOnly. We need the underlying Store handles, not a wrapper, so
        // there is nothing to route through.
        #[allow(clippy::disallowed_methods)]
        let fast_store = effective_cas_store.fast_store().clone();
        let slow_store = effective_cas_store.slow_store().clone();
        // `slow_direction = ReadOnly` is defensive only: with
        // `local_only_reads = true` the read paths short-circuit before
        // touching the slow tier, and `update()` early-returns under
        // `IS_MIRROR_REQUEST` before consulting `slow_direction`. Cost
        // is nil and the original 354 GB / 30 min bounce-loop is bad
        // enough to justify defense-in-depth.
        let fss_spec = nativelink_config::stores::FastSlowSpec {
            fast: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            slow: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            fast_direction: effective_cas_store.fast_direction(),
            slow_direction: nativelink_config::stores::StoreDirection::ReadOnly,
            // Worker-side wrapper FSS; the chunked-read cascade lives
            // on the server, never on the worker, so leave OFF.
            chunked_reads_enabled: false,
            // #334 Fix B cap moot here: `slow_direction = ReadOnly`
            // means `update()` short-circuits before consulting the
            // cap (no slow-tier write is admitted at all). Set 0 to
            // match the wrapper FSS above for consistency.
            slow_writes_in_flight_max_bytes: 0,
            // Upstream #2415 opt-in leader/follower dedup bypass for huge
            // blobs; 0 = disabled (default), preserving prior behavior.
            bypass_dedup_threshold_bytes: 0,
        };
        FastSlowStore::new_with_shared_failed_writes(
            &fss_spec,
            fast_store,
            slow_store,
            &effective_cas_store,
        )
        .with_local_only_reads()
    };
    // Keep a reference for mirror blob cleanup in BlobsInStableStorage.
    let cas_server_fss = effective_cas_store_for_cas_server.clone();
    // Diagnostic-only (observability): surface server-pushed mirror blobs
    // still held on this worker (un-acked by BlobsInStableStorage) past 30s.
    // Idempotent spawn; drops nothing.
    cas_server_fss.start_stale_mirror_alert();

    // Walk the AC store wrapper chain to find its underlying
    // `FastSlowStore`. Uses the same `find_fast_slow_for_pin` walker
    // that the CAS pin path uses, NOT a single-level downcast — a bare
    // downcast silently disables AC pin advertisement the moment any
    // wrapper (ExistenceCacheStore, VerifyStore, etc.) lands above the
    // FSS, since each wrapper presents its own Arc and a one-level
    // downcast misses the layered chain.
    //
    // Per the type-system invariant on `AcMirrorTarget`, both `fss`
    // and `store_id` are produced together — there is no "have one,
    // missing the other" half-Some shape.
    // #37 Phase 2 (Q5): pre-construct Metrics + pending_acks here so
    // both AcMirrorTarget (consumed at the BIS-ack receive site) and
    // RunningActionsManagerImpl::metrics (consumed at the AC publish
    // site) share the SAME Arc. Without this co-construction, the
    // publish path would insert into one map while the BIS handler
    // would observe a different (empty) map.
    let ac_publish_metrics = std::sync::Arc::new(
        crate::running_actions_manager::Metrics::default(),
    );
    let ac_publish_pending_acks = std::sync::Arc::new(parking_lot::Mutex::new(
        std::collections::HashMap::new(),
    ));
    // #37 Phase 2 (Q4 / F1): install the CAS FSS metric sink so the
    // spawned slow-tier Err arm bumps
    // `worker_slow_tier_async_fail_cas`. The AC FSS sink is installed
    // below at the AC FSS construction site.
    effective_cas_store.set_slow_tier_metric_sink(Arc::new(
        WorkerSlowTierMetricSink {
            metrics: ac_publish_metrics.clone(),
        },
    ));
    let ac_mirror_target: Option<AcMirrorTarget> =
        match (ac_store.as_ref(), ac_store_name.as_deref()) {
            (Some(store), Some(name)) => {
                // The walker borrows `&dyn StoreDriver` from the store
                // it's given; call `.inner_store(None)` to obtain a
                // borrow without requiring the store to clone its inner.
                let driver = store.inner_store(None::<StoreKey<'_>>);
                let fss_borrow =
                    nativelink_store::small_blob_dispatcher::find_fast_slow_for_pin(driver);
                match fss_borrow.and_then(|fss| fss.get_arc()) {
                    Some(fss) => {
                        // #37 Phase 2 (Q4): tag the AC FastSlowStore
                        // with store_class = "ac" so its background
                        // slow-tier failure log carries the
                        // discriminator (paired with the CAS tag at
                        // effective_cas_store above).
                        fss.set_store_class("ac");
                        // #37 Phase 2 (Q4 / F1): install the AC FSS
                        // metric sink so the spawned slow-tier Err
                        // arm bumps `worker_slow_tier_async_fail_ac`.
                        fss.set_slow_tier_metric_sink(Arc::new(
                            WorkerSlowTierMetricSink {
                                metrics: ac_publish_metrics.clone(),
                            },
                        ));
                        info!(
                            ac_store_name = name,
                            "AC pin advertisement enabled — found FastSlowStore in AC chain"
                        );
                        Some(AcMirrorTarget {
                            fss,
                            store_id: Arc::from(name),
                            ac_publish_pending_acks: ac_publish_pending_acks.clone(),
                            metrics: ac_publish_metrics.clone(),
                        })
                    }
                    None => {
                        warn!(
                            ac_store_name = name,
                            "AC pin advertisement DISABLED — no FastSlowStore found in AC chain. \
                         AC writes still complete normally, but BlobsAvailable will not \
                         carry AC pins for this worker. If the production AC chain has \
                         changed shape (new wrapper above the FSS), extend \
                         `find_fast_slow_for_pin` to recurse through it."
                        );
                        None
                    }
                }
            }
            _ => None,
        };

    // Keep a handle on the AC store and its registered name so the worker
    // CAS listener (below) can mount an `AcServer` against the same store.
    // The server-side `AcProxyStore` dials this listener for AC peer-fetch;
    // without a mounted handler, tonic synthesizes `Unimplemented` for every
    // `ActionCache/GetActionResult` call (#463: 1503 warns/day since #277).
    let ac_store_for_listener = ac_store.clone();
    let ac_store_name_for_listener = ac_store_name.clone();
    // #37 Phase 2 (Q5): pull the AC BIS-ack timeout from config (default
    // 60s — see `LocalWorkerConfig::bis_ack_timeout_secs` doc).
    let bis_ack_timeout_secs = if config.bis_ack_timeout_secs == 0 {
        60
    } else {
        config.bis_ack_timeout_secs
    };
    let bis_ack_timeout = Duration::from_secs(bis_ack_timeout_secs);
    // (#12 H4 phase 2) Compute the worker's advertised CAS endpoint so it can
    // be set on UpdateActionResultRequest. The server uses this to pre-register
    // output locality in pending_output_locality_registry BEFORE committing the
    // AC entry (H4 invariant). Empty when cas_server_port is not configured.
    let running_actions_cas_endpoint = config
        .cas_server_port
        .map(|port| cas_advertised_endpoint(port, use_tls))
        .unwrap_or_default();
    let running_actions_manager =
        Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory: config.work_directory.clone(),
            execution_configuration: ExecutionConfiguration {
                entrypoint,
                additional_environment: config.additional_environment.clone(),
            },
            cas_store: effective_cas_store,
            ac_store,
            ac_mirror_target: ac_mirror_target.clone(),
            historical_store,
            upload_action_result_config: &config.upload_action_result,
            max_action_timeout,
            max_upload_timeout,
            timeout_handled_externally: config.timeout_handled_externally,
            directory_cache,
            bis_ack_timeout,
            metrics: Some(ac_publish_metrics.clone()),
            cas_endpoint: running_actions_cas_endpoint,
            deferred_output_uploads_enabled: config.deferred_output_uploads_enabled,
        })?);

    // Set up BlobsAvailable reporting with drain-then-fire semantics.
    // The send loop wakes immediately on blob insert/eviction via Notify,
    // with a backstop interval to catch subtree-only changes.
    let blobs_available_state = if config.cas_server_port.is_some() {
        // Sibling-bug audit (review #7): fast-store-only is intentional.
        // BlobsAvailable advertises ON-DISK digests so peer workers can
        // fetch them. Mirror-blob digests are reported via a separate
        // `pinned_mirror_digests` field on the same proto, populated
        // from `cas_server_fss.snapshot_and_reset_mirror_changes()` —
        // the two snapshots have different lifetimes and routing
        // semantics on the server side and must NOT be merged here.
        // Concrete FilesystemStore needed for BlobChangeTracker registration;
        // the wrapper hides the concrete type so the downcast must read the
        // inner store directly.
        #[allow(clippy::disallowed_methods)]
        let fs_store_opt: Option<Arc<FilesystemStore>> = fast_slow_store
            .fast_store()
            .downcast_ref::<FilesystemStore>(None)
            .and_then(|fs| fs.get_arc());

        if let Some(fs_store) = fs_store_opt {
            let max_interval_ms = if config.blobs_available_interval_ms == 0 {
                BLOBS_AVAILABLE_MAX_INTERVAL_MS
            } else {
                config.blobs_available_interval_ms
            };
            let cas_endpoint = config
                .cas_server_port
                .map(|port| cas_advertised_endpoint(port, use_tls))
                .unwrap_or_default();

            // Shared notify: tracker fires it on insert/eviction, send loop
            // awaits it to wake immediately.
            let notify = Arc::new(Notify::new());

            // (#locality-map-drift) Stamp the FS store's eviction map with this
            // process's boot_epoch BEFORE the tracker starts producing consumed
            // deltas. Startup-loaded blobs (inserted inside `FilesystemStore::
            // new`, before this) are reported via the full SNAPSHOT (stamp 0,
            // applied post-wipe), so their pre-set stamp is irrelevant; every
            // RUNTIME insert/evict/read delta (produced only after the send
            // loop below starts) then carries `(boot_epoch, counter)` so a
            // restarted worker's fresh epoch dominates stale server state.
            fs_store.set_map_boot_epoch(boot_epoch_id());

            // Create change tracker and register it on the FilesystemStore.
            let tracker = BlobChangeTracker::new(notify.clone());
            if let Err(err) = fs_store.clone().register_item_callback(tracker.clone()) {
                warn!(
                    ?err,
                    "Failed to register blob change tracker on FilesystemStore"
                );
            } else {
                info!(
                    max_interval_ms,
                    "Registered BlobsAvailable drain-then-fire reporting with callback-based change tracking"
                );
            }

            Some(BlobsAvailableState {
                fs_store,
                tracker,
                cas_endpoint,
                notify,
                max_interval: Duration::from_millis(max_interval_ms),
                cas_server_fss: Some(cas_server_fss.clone()),
                ac_mirror_target: ac_mirror_target.clone(),
                // (#99) Random worker-process token. SystemTime nanos
                // XOR'd with PID gives an effectively-unique value per
                // worker process without pulling in `rand`. Equivalent
                // to #97's `server_instance_token` strategy in
                // `api_worker_scheduler.rs`.
                worker_instance_token: {
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);
                    let pid = std::process::id() as u64;
                    let t = nanos ^ pid.rotate_left(32);
                    // Defensive: 0 is the proto3 default; coerce to 1.
                    if t == 0 { 1 } else { t }
                },
                next_broadcast_id: Arc::new(AtomicU64::new(0)),
                last_sent_ac_pin_set: Arc::new(Mutex::new(HashSet::new())),
                blobs_available_skipped_counter: Arc::new(AtomicU64::new(0)),
                blobs_available_resend: Arc::new(Mutex::new(BlobsAvailableResendBuffer::default())),
                blobs_available_force_full_snapshot: Arc::new(AtomicBool::new(false)),
            })
        } else {
            warn!(
                "FastSlowStore's fast store is not a FilesystemStore; BlobsAvailable reporting disabled"
            );
            None
        }
    } else {
        None
    };

    // Start a CAS + ByteStream gRPC server for peer blob sharing if configured.
    // Serves the effective_cas_store (which includes WorkerProxyStore) so that
    // reads can be proxied to peers when the local store doesn't have the blob.
    let cas_server_guard = if let Some(cas_port) = config.cas_server_port {
        let cas_store = Store::new(effective_cas_store_for_cas_server);
        let store_manager = Arc::new(nativelink_store::store_manager::StoreManager::new());
        store_manager.add_store("worker_cas", cas_store);

        let cas_configs = vec![nativelink_config::cas_server::WithInstanceName {
            instance_name: String::new(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "worker_cas".to_string(),
                // Upstream CAS chunking config; None = disabled (default),
                // chunking RPCs rejected — identical to pre-existence.
                experimental_chunking: None,
            },
        }];
        let bytestream_configs = vec![nativelink_config::cas_server::WithInstanceName {
            instance_name: String::new(),
            config: nativelink_config::cas_server::ByteStreamConfig {
                cas_store: "worker_cas".to_string(),
                ..Default::default()
            },
        }];

        // If an AC store is plumbed (upload_action_result.ac_store), mount it
        // under "worker_ac" so the AcServer below can resolve it. The
        // server-side `AcProxyStore::create_worker_connection` uses
        // `instance_name: String::new()`, so the AcServer instance map keys
        // on `""` for AC peer-fetch RPCs to land. Without this branch, the
        // tonic Router synthesizes `Unimplemented` on every
        // `ActionCache/GetActionResult` against port `cas_server_port`.
        let mount_ac = match (
            ac_store_for_listener.as_ref(),
            ac_store_name_for_listener.as_ref(),
        ) {
            (Some(ac_store), Some(ac_name)) => {
                store_manager.add_store("worker_ac", ac_store.clone());
                info!(
                    ac_store_name = %ac_name,
                    "worker AC server mounted on cas_server_port for peer AC fetch (read_only)"
                );
                true
            }
            (Some(_), None) => {
                // Half-Some plumbing drift: ac_store is present but its
                // name was not threaded through. The mount key is
                // `ac_store_name_for_listener`, so without it we cannot
                // wire the AcServer; surface this as a warn so the
                // operator can correct the config (vs. silently degrading
                // to Unimplemented).
                warn!(
                    "worker AC server NOT mounted on cas_server_port — half-Some \
                     plumbing drift: `ac_store` present but `ac_store_name` missing. \
                     AcProxyStore peer-fetch will see Unimplemented from this worker."
                );
                false
            }
            (None, Some(ac_name)) => {
                warn!(
                    ac_store_name = %ac_name,
                    "worker AC server NOT mounted on cas_server_port — half-Some \
                     plumbing drift: `ac_store_name` present but `ac_store` missing. \
                     AcProxyStore peer-fetch will see Unimplemented from this worker."
                );
                false
            }
            (None, None) => {
                // No AC store configured on the worker — this is the
                // expected shape for workers that don't run AC. The
                // server's AcProxyStore peer-fetch will receive
                // Unimplemented for this worker, and the wrapper falls
                // back to the inner AC chain.
                debug!(
                    "worker AC server NOT mounted on cas_server_port — \
                     no AC store plumbed (ac_store=None, ac_store_name=None)"
                );
                false
            }
        };

        // Match the main server's message size limits so that mirror writes
        // from WorkerProxyStore (which may send BatchUpdateBlobs >4MiB) are
        // not rejected by tonic's default 4MiB limit.
        const WORKER_CAS_MAX_DECODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
        const WORKER_CAS_MAX_ENCODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

        // Workers do NOT participate in the SmallBlobDispatcher producer
        // path — workers RECEIVE dispatched bytes; they never push to
        // other workers. Pass `None` for `small_blob_dispatcher` here so
        // the dispatcher hook is entirely inert on the worker side.
        // Server-side wire-up lives in `src/bin/nativelink.rs:957-973`.
        //
        // We construct two parallel sets of *Server objects (one for
        // TCP, one for QUIC) and build them through `build_cas_router`
        // so the test composition cannot drift from production
        // (testing-czar MAJOR-1, #463 fix-up). Each *Server holds a
        // small HashMap of `Arc<dyn StoreDriver>` clones — fresh
        // construction is cheap and matches what the prior
        // `Server<S>::clone()` was effectively doing per route.
        //
        // `read_only: true` on the AC mount keeps the unauthenticated
        // LAN write surface closed; the only known caller for this
        // listener is `AcProxyStore::try_read_from_peer` (read). See
        // #463 security fix-up.
        let mk_ac_server = || -> Result<Option<nativelink_service::ac_server::AcServer>, Error> {
            if mount_ac {
                let ac_configs = vec![nativelink_config::cas_server::WithInstanceName {
                    instance_name: String::new(),
                    config: nativelink_config::cas_server::AcStoreConfig {
                        ac_store: "worker_ac".to_string(),
                        read_only: true,
                    },
                }];
                Ok(Some(
                    nativelink_service::ac_server::AcServer::new(&ac_configs, &store_manager)
                        .err_tip(|| "Failed to create worker AC server")?,
                ))
            } else {
                Ok(None)
            }
        };
        let build_router = || -> Result<tonic::service::Routes, Error> {
            let cas_server = nativelink_service::cas_server::CasServer::new(
                &cas_configs,
                &store_manager,
                None,
            )
            .err_tip(|| "Failed to create worker CAS server")?;
            let bytestream_server = nativelink_service::bytestream_server::ByteStreamServer::new(
                &bytestream_configs,
                &store_manager,
                None,
            )
            .err_tip(|| "Failed to create worker ByteStream server")?;
            let ac_server = mk_ac_server()?;
            Ok(build_cas_router(
                cas_server,
                bytestream_server,
                ac_server,
                WORKER_CAS_MAX_DECODING_MESSAGE_SIZE,
                WORKER_CAS_MAX_ENCODING_MESSAGE_SIZE,
            ))
        };

        let tcp_routes = build_router()?;
        #[cfg(feature = "quic")]
        let quic_routes = build_router()?;

        let addr: std::net::SocketAddr = ([0, 0, 0, 0, 0, 0, 0, 0], cas_port).into();
        let advertised = cas_advertised_endpoint(cas_port, use_tls);

        let worker_name = config.name.clone();

        // Start TCP server (with TLS if cas_server_tls is configured).
        let tcp_worker_name = worker_name.clone();
        let tls_server_config = if let Some(ref tls_cfg) = config.cas_server_tls {
            let cert = std::fs::read_to_string(&tls_cfg.cert_file)
                .err_tip(|| format!("Could not read CAS server cert: {}", tls_cfg.cert_file))?;
            let key = std::fs::read_to_string(&tls_cfg.key_file)
                .err_tip(|| format!("Could not read CAS server key: {}", tls_cfg.key_file))?;
            let identity = tonic::transport::Identity::from_pem(cert, key);
            let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);
            if let Some(ref ca_file) = tls_cfg.client_ca_file {
                let ca_cert = std::fs::read_to_string(ca_file)
                    .err_tip(|| format!("Could not read CAS server client CA: {ca_file}"))?;
                tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(ca_cert));
            }
            Some(tls)
        } else {
            None
        };
        // Shutdown signal for the worker CAS server. On SIGTERM, the worker
        // sends `true` so the CAS server stops accepting new connections and
        // drains in-flight requests before the process exits.
        let (cas_shutdown_tx, cas_shutdown_rx) = tokio::sync::watch::channel(false);
        let mut tcp_shutdown_rx = cas_shutdown_rx.clone();
        let tcp_guard = spawn!("worker_cas_tcp", async move {
            info!(
                worker_name = %tcp_worker_name,
                %addr,
                %advertised,
                tls = tls_server_config.is_some(),
                "Starting worker CAS TCP server for peer blob sharing"
            );
            let mut builder = tonic::transport::Server::builder();
            if let Some(tls) = tls_server_config {
                builder = builder.tls_config(tls).map_err(|e| {
                    make_err!(Code::Internal, "Worker CAS TCP TLS config failed: {e:?}")
                })?;
            }
            let router = builder.add_routes(tcp_routes);
            let result = router
                .serve_with_shutdown(addr, async move {
                    let _ = tcp_shutdown_rx.changed().await;
                    info!(%addr, "worker CAS server shutting down gracefully");
                })
                .await
                .map_err(|e| make_err!(Code::Internal, "Worker CAS TCP server failed: {e:?}"));
            if let Err(ref e) = result {
                error!(%addr, ?e, "Worker CAS TCP server exited with error");
            }
            result
        });

        // Start QUIC/H3 server on the same port (UDP) for peer blob sharing.
        #[cfg(feature = "quic")]
        let _quic_guard = {
            match start_worker_quic_server(cas_port, &worker_name, quic_routes) {
                Ok(guard) => Some(guard),
                Err(e) => {
                    warn!(
                        ?e,
                        "Failed to start worker QUIC CAS server, falling back to TCP only"
                    );
                    None
                }
            }
        };

        #[allow(unused_mut)]
        let mut guards = vec![tcp_guard];
        #[cfg(feature = "quic")]
        if let Some(quic_guard) = _quic_guard {
            guards.push(quic_guard);
        }
        (guards, Some(cas_shutdown_tx))
    } else {
        (Vec::new(), None)
    };
    let (cas_server_guard, cas_shutdown_tx) = cas_server_guard;

    // Start pprof HTTP server if configured and the feature is enabled.
    #[cfg(feature = "pprof")]
    if config.pprof_port != 0 {
        match nativelink_util::pprof_server::start_pprof_server(config.pprof_port) {
            Ok(guard) => {
                // Leak the guard so the server lives for the process lifetime.
                // The pprof server is a diagnostic tool that should outlive any
                // individual worker reconnection cycle.
                std::mem::forget(guard);
                info!(port = config.pprof_port, "pprof HTTP server started");
            }
            Err(e) => {
                warn!(
                    ?e,
                    port = config.pprof_port,
                    "failed to start pprof HTTP server"
                );
            }
        }
    }

    let local_worker = LocalWorker::new_with_connection_factory_actions_manager_and_locality(
        config.clone(),
        running_actions_manager,
        Box::new(move || {
            let config = config.clone();
            Box::pin(async move {
                // Check if QUIC/HTTP3 is requested for the worker API endpoint.
                #[cfg(feature = "quic")]
                if config.worker_api_endpoint.use_http3 {
                    let grpc_endpoint = nativelink_config::stores::GrpcEndpoint {
                        address: config.worker_api_endpoint.uri.clone(),
                        tls_config: None,
                        concurrency_limit: None,
                        connect_timeout_s: 0,
                        tcp_keepalive_s: 0,
                        http2_keepalive_interval_s: 0,
                        http2_keepalive_timeout_s: 0,
                        tcp_nodelay: true,
                        use_http3: true,
                    };
                    let quic_channel = tls_utils::h3_channel(&grpc_endpoint, 1).map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to create QUIC channel for worker API: {e:?}"
                        )
                    })?;
                    info!(
                        uri = %config.worker_api_endpoint.uri,
                        decode_limit_mib = WORKER_API_MAX_DECODING_MESSAGE_SIZE / (1024 * 1024),
                        "Worker API: using QUIC/HTTP3 transport with explicit decode limit"
                    );
                    return Ok(WorkerApiClient::new(quic_channel)
                        .max_decoding_message_size(WORKER_API_MAX_DECODING_MESSAGE_SIZE)
                        .into());
                }

                let timeout = config
                    .worker_api_endpoint
                    .timeout
                    .unwrap_or(DEFAULT_ENDPOINT_TIMEOUT_S);
                let timeout_duration = Duration::from_secs_f32(timeout);
                let tls_config =
                    tls_utils::load_client_config(&config.worker_api_endpoint.tls_config)
                        .err_tip(|| "Parsing local worker TLS configuration")?;
                let endpoint = build_worker_api_tcp_endpoint(
                    &config.worker_api_endpoint.uri,
                    tls_config,
                    timeout_duration,
                )?;

                let transport = endpoint.connect().await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Could not connect to endpoint {}: {e:?}",
                        config.worker_api_endpoint.uri
                    )
                })?;
                info!(
                    uri = %config.worker_api_endpoint.uri,
                    decode_limit_mib = WORKER_API_MAX_DECODING_MESSAGE_SIZE / (1024 * 1024),
                    "Worker API: using TCP/HTTP2 transport with explicit decode limit"
                );
                Ok(WorkerApiClient::new(transport)
                    .max_decoding_message_size(WORKER_API_MAX_DECODING_MESSAGE_SIZE)
                    .into())
            })
        }),
        Box::new(move |d| Box::pin(sleep(d))),
        blobs_available_state,
        peer_locality_map,
        cas_server_guard,
        cas_shutdown_tx,
    );
    Ok(local_worker)
}

impl<T: WorkerApiClientTrait + 'static, U: RunningActionsManager> LocalWorker<T, U> {
    pub fn new_with_connection_factory_and_actions_manager(
        config: Arc<LocalWorkerConfig>,
        running_actions_manager: Arc<U>,
        connection_factory: ConnectionFactory<T>,
        sleep_fn: Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>,
        blobs_available_state: Option<BlobsAvailableState>,
        cas_server_guards: Vec<JoinHandleDropGuard<Result<(), Error>>>,
        cas_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    ) -> Self {
        Self::new_with_connection_factory_actions_manager_and_locality(
            config,
            running_actions_manager,
            connection_factory,
            sleep_fn,
            blobs_available_state,
            None,
            cas_server_guards,
            cas_shutdown_tx,
        )
    }

    /// Same as `new_with_connection_factory_and_actions_manager` but plumbs
    /// through an optional `peer_locality_map` so the worker's
    /// `Update::ChunkedMessage(PeerHints)` arm can register hints
    /// directly. The legacy constructor preserved as a thin wrapper so
    /// existing test setups (which never enable peer sharing) compile
    /// unchanged.
    pub fn new_with_connection_factory_actions_manager_and_locality(
        config: Arc<LocalWorkerConfig>,
        running_actions_manager: Arc<U>,
        connection_factory: ConnectionFactory<T>,
        sleep_fn: Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>,
        blobs_available_state: Option<BlobsAvailableState>,
        peer_locality_map: Option<SharedBlobLocalityMap>,
        cas_server_guards: Vec<JoinHandleDropGuard<Result<(), Error>>>,
        cas_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    ) -> Self {
        let ac_write_detached_inflight_count =
            Arc::new(core::sync::atomic::AtomicI64::new(0));
        let metrics = Arc::new(Metrics::new(
            Arc::downgrade(running_actions_manager.metrics()),
            Arc::clone(&ac_write_detached_inflight_count),
        ));
        let ac_write_semaphore = Arc::new(Semaphore::new(AC_WRITE_DETACHED_INFLIGHT_CAP));
        Self {
            config,
            running_actions_manager,
            connection_factory,
            sleep_fn: Some(sleep_fn),
            metrics,
            blobs_available_state,
            peer_locality_map,
            _cas_server_guards: cas_server_guards,
            cas_shutdown_tx,
            ac_write_semaphore,
            ac_write_detached_inflight_count,
        }
    }

    /// #O15 fix-up (2026-06-07): test-only override of the detached-AC-
    /// write semaphore cap. T5 (`cap_saturated_logs_warn_and_skips_ac_write`)
    /// installs `cap=0` to force `try_acquire_owned` to fail and exercise
    /// the over-cap log+skip path. Gated behind `test-utils` so production
    /// callers can't accidentally pick the wrong cap.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_ac_write_semaphore_for_test(&mut self, cap: usize) {
        self.ac_write_semaphore = Arc::new(Semaphore::new(cap));
    }

    #[allow(
        clippy::missing_const_for_fn,
        reason = "False positive on stable, but not on nightly"
    )]
    pub fn name(&self) -> &String {
        &self.config.name
    }

    /// The worker's EXECUTION `FastSlowStore` — the `effective_cas_store` the
    /// `RunningActionsManager` reads/writes to materialize action inputs and
    /// store outputs. Returns `None` when the running-actions manager exposes
    /// no CAS store (test fakes). The binary uses this to register the
    /// execution instance's metrics on `/metrics`; see
    /// [`register_execution_store_metrics`] for why this instance is distinct
    /// from the idle CAS-server `WORKER_FAST_SLOW_STORE`.
    pub fn execution_fast_slow_store(&self) -> Option<Arc<FastSlowStore>> {
        self.running_actions_manager.get_cas_store()
    }

    async fn register_worker(
        &self,
        client: &mut T,
    ) -> Result<(String, Streaming<UpdateForWorker>), Error> {
        let mut extra_envs: HashMap<String, String> = HashMap::new();
        if let Some(ref additional_environment) = self.config.additional_environment {
            for (name, source) in additional_environment {
                let value = match source {
                    EnvironmentSource::Value(value) => Cow::Borrowed(value.as_str()),
                    EnvironmentSource::FromEnvironment => {
                        Cow::Owned(env::var(name).unwrap_or_default())
                    }
                    other => {
                        debug!(
                            ?other,
                            "Worker registration doesn't support this type of additional environment"
                        );
                        continue;
                    }
                };
                extra_envs.insert(name.clone(), value.into_owned());
            }
        }

        let use_tls = self.config.cas_server_tls.is_some();
        let cas_endpoint = self
            .config
            .cas_server_port
            .map_or_else(String::new, |port| cas_advertised_endpoint(port, use_tls));
        // (#sched-blend) Static P/E logical-CPU counts for absolute-capacity
        // scheduling. macOS reports real counts; Linux/other report (0,0)
        // → scheduler uses `assume_core_count`. OnceLock-cached, no syscall.
        let (p_core_count, e_core_count) = cpu_impl::core_counts();
        // (#task-resource-profile Phase-3 §6) Total physical RAM (KiB), static for
        // the worker's life, rides the same connect frame as the core counts. `0` on
        // an unsupported platform / query failure → scheduler treats as unknown.
        let total_memory_kb = cpu_impl::total_memory_kb();
        let connect_worker_request = make_connect_worker_request(
            self.config.name.clone(),
            &self.config.platform_properties,
            &extra_envs,
            self.config.max_inflight_tasks,
            cas_endpoint,
            p_core_count,
            e_core_count,
            total_memory_kb,
        )
        .await?;
        let mut update_for_worker_stream = client
            .connect_worker(connect_worker_request)
            .await
            .err_tip(|| "Could not call connect_worker() in worker")?
            .into_inner();

        let first_msg_update = update_for_worker_stream
            .next()
            .await
            .err_tip(|| "Got EOF expected UpdateForWorker")?
            .err_tip(|| "Got error when receiving UpdateForWorker")?
            .update;

        let worker_id = match first_msg_update {
            Some(Update::ConnectionResult(connection_result)) => connection_result.worker_id,
            other => {
                return Err(make_input_err!(
                    "Expected first response from scheduler to be a ConnectResult got : {:?}",
                    other
                ));
            }
        };
        Ok((worker_id, update_for_worker_stream))
    }

    #[instrument(skip(self), level = Level::INFO)]
    pub async fn run(
        mut self,
        mut shutdown_rx: broadcast::Receiver<ShutdownGuard>,
    ) -> Result<(), Error> {
        let sleep_fn = self
            .sleep_fn
            .take()
            .err_tip(|| "Could not unwrap sleep_fn in LocalWorker::run")?;
        let sleep_fn_pin = Pin::new(&sleep_fn);
        let error_handler = Box::pin(move |err| async move {
            error!(?err, "Error");
            (sleep_fn_pin)(Duration::from_secs_f32(CONNECTION_RETRY_DELAY_S)).await;
        });

        loop {
            // First connect to our endpoint.
            let mut client = match (self.connection_factory)().await {
                Ok(client) => client,
                Err(e) => {
                    (error_handler)(e).await;
                    continue; // Try to connect again.
                }
            };

            // Next register our worker with the scheduler.
            let (inner, update_for_worker_stream) = match self.register_worker(&mut client).await {
                Err(e) => {
                    (error_handler)(e).await;
                    continue; // Try to connect again.
                }
                Ok((worker_id, update_for_worker_stream)) => (
                    LocalWorkerImpl::new(
                        &self.config,
                        client,
                        worker_id,
                        self.running_actions_manager.clone(),
                        self.metrics.clone(),
                        self.blobs_available_state.clone(),
                        self.peer_locality_map.clone(),
                        &self.cas_shutdown_tx,
                        Arc::clone(&self.ac_write_semaphore),
                        Arc::clone(&self.ac_write_detached_inflight_count),
                    ),
                    update_for_worker_stream,
                ),
            };
            info!(
                worker_id = %inner.worker_id,
                "Worker registered with scheduler"
            );

            // Now listen for connections and run all other services.
            if let Err(err) = inner.run(update_for_worker_stream, &mut shutdown_rx).await {
                'no_more_actions: {
                    // Ensure there are no actions in transit before we try to kill
                    // all our actions.
                    const ITERATIONS: usize = 1_000;

                    const ERROR_MSG: &str = "Actions in transit did not reach zero before we disconnected from the scheduler";

                    let sleep_duration = ACTIONS_IN_TRANSIT_TIMEOUT_S / ITERATIONS as f32;
                    for _ in 0..ITERATIONS {
                        if inner.actions_in_transit.load(Ordering::Acquire) == 0 {
                            break 'no_more_actions;
                        }
                        (sleep_fn_pin)(Duration::from_secs_f32(sleep_duration)).await;
                    }
                    // Don't terminate the worker process — fall through to
                    // kill_all + reconnect. The stuck create_and_add_action
                    // futures will be cancelled when kill_all drops them.
                    warn!(ERROR_MSG);
                }
                error!(?err, "Worker disconnected from scheduler");
                // Kill off any existing actions because if we re-connect, we'll
                // get some more and it might resource lock us.
                self.running_actions_manager.kill_all().await;

                (error_handler)(err).await; // Try to connect again.
            }
        }
        // Unreachable.
    }
}

#[derive(Debug, MetricsComponent)]
pub struct Metrics {
    #[metric(
        help = "Total number of actions sent to this worker to process. This does not mean it started them, it just means it received a request to execute it."
    )]
    start_actions_received: CounterWithTime,
    #[metric(help = "Total number of disconnects received from the scheduler.")]
    disconnects_received: CounterWithTime,
    #[metric(help = "Total number of keep-alives received from the scheduler.")]
    keep_alives_received: CounterWithTime,
    #[metric(
        help = "Stats about the calls to check if an action satisfies the config supplied script."
    )]
    preconditions: AsyncCounterWrapper,
    #[metric]
    #[allow(
        clippy::struct_field_names,
        reason = "TODO Fix this. Triggers on nightly"
    )]
    running_actions_manager_metrics: Weak<RunningActionManagerMetrics>,
    /// #O15 (2026-06-07): live count of detached AC-write tasks the
    /// publish closure has spawned that have not yet exited. Should
    /// track the tail of AC-store update latency under load; pinning
    /// near `AC_WRITE_DETACHED_INFLIGHT_CAP` indicates AC-store stall.
    #[metric(
        help = "Count of currently-in-flight detached AC writes spawned by O15; should track tail of AC-store update latency."
    )]
    ac_write_detached_inflight_count: Arc<core::sync::atomic::AtomicI64>,
    /// (FL-688 v3 Stage C — MINOR-1) Count of blobs in `UploadMissingBlobs`
    /// where the reconcile-pin fell back to a time-bounded pin because the
    /// indefinite-pin cap was saturated. These blobs have 120s protection;
    /// non-zero indicates the cap is too small for the workload but the blobs
    /// are not immediately at risk (recoverable backpressure).
    #[metric(
        help = "Blobs reconcile-pinned with time-bounded fallback (indefinite cap full); 120s protection window, non-zero = cap saturation backpressure."
    )]
    reconcile_pin_time_bounded_fallback_total: Counter,
    // NOTE: `reconcile_pin_refused_total` was previously a per-instance Counter
    // field here. It has been MOVED to the process-singleton
    // `nativelink_util::o11_probes::reconcile_pin_counters()` and registered in
    // nativelink.rs so it is visible on /metrics. The per-instance `Metrics`
    // tree is never registered with MetricsRegistry (the
    // worker-metrics-exposure trap; same class as the #37 memory_gate move) —
    // the old field was dark. Its help-text ALSO miscalled `Refused` the
    // "FL-688 data-loss signal — upload will fail", which is false: a Refused
    // pin is a benign eviction race and the disk-backed upload proceeds. The
    // genuine data-loss signal is now `reconcile_pin_vanished_total` (an
    // advertised digest ABSENT at the has_with_results re-check = sole-copy
    // loss), with `reconcile_pin_dropped_total` (over-cap requeue drop) and
    // `reconcile_pin_requeued_total` (recoverable retry) split out by severity.
    // See #FL-688 log fix. (`reconcile_pin_time_bounded_fallback_total` above is
    // ALSO on this dark tree; it is a separate recoverable-backpressure signal,
    // left in place — making it render is a follow-up, filed in deferred_tasks.md.)

    /// (#37 re-enable follow-up) NAKs issued to the scheduler because the
    /// available-memory free-floor PRIMARY was breached (available
    /// free+inactive+purgeable < `FREE_FLOOR_BYTES = 1 GiB`). Monotonic.
    // NOTE: memory_gate_nak_free_floor_total and memory_gate_nak_swapin_total
    // were previously here as per-instance Counter fields. They have been moved
    // to the process-singleton `nativelink_util::o11_probes::memory_gate_counters()`
    // and registered in nativelink.rs so they are visible on /metrics. The NAK
    // path in `start_execute` now increments the singleton directly. The
    // per-instance fields were never registered with MetricsRegistry (the
    // worker-metrics-exposure trap); the singleton fixes that. See #37.

    /// (FL-688 v3 Stage C — over-cap metric) Counts how many times the
    /// startup reconcile gate was released by the fail-open timer
    /// (`RECONCILE_FAIL_OPEN_SECS = 20`) rather than by a `ReconcileComplete`
    /// from the server. Non-zero = rolling-deploy window where action execution
    /// was blocked for up to 20s; gate-armed window's over-cap exposure was
    /// invisible without this counter. This is a MONOTONIC counter — alert on
    /// the RATE (`rate(...[5m]) > 0` in steady state), NOT on the level: a
    /// level alert (`> 0`) stays permanently triggered after any rollout that
    /// legitimately fail-opened a worker, masking a later genuine
    /// never-updated-server mismatch (red-team pre-mortem).
    #[metric(
        help = "Times the startup reconcile gate released via fail-open timer (not ReconcileComplete); monotonic — alert on RATE not level; sustained rate>0 post-rollout = server version mismatch."
    )]
    reconcile_gate_fail_open_total: Counter,
    /// (speculative-prefetch Increment 1) Counts PrefetchInputs messages
    /// dropped because a prior speculative fetch was already in-flight
    /// (G5 / §1.10 single-in-flight guard). Non-zero = prefetch signals
    /// arriving faster than the worker can service them; may indicate
    /// the backlog threshold is too low or the TTL is too long.
    #[metric(
        help = "PrefetchInputs dropped: prior speculative fetch in-flight (G5 guard)."
    )]
    speculative_prefetch_busy_drop: Counter,
    /// (speculative-prefetch Increment 1) Counts speculative prefetches
    /// that aborted because the fast store was over-pressured (populate
    /// returned Code::Aborted). The real action is unaffected.
    #[metric(
        help = "Speculative prefetches aborted: fast store over-pressure (Aborted from populate)."
    )]
    speculative_prefetch_aborted: Counter,
}

impl RootMetricsComponent for Metrics {}

impl Metrics {
    fn new(
        running_actions_manager_metrics: Weak<RunningActionManagerMetrics>,
        ac_write_detached_inflight_count: Arc<core::sync::atomic::AtomicI64>,
    ) -> Self {
        Self {
            start_actions_received: CounterWithTime::default(),
            disconnects_received: CounterWithTime::default(),
            keep_alives_received: CounterWithTime::default(),
            preconditions: AsyncCounterWrapper::default(),
            running_actions_manager_metrics,
            ac_write_detached_inflight_count,
            reconcile_pin_time_bounded_fallback_total: Counter::default(),
            reconcile_gate_fail_open_total: Counter::default(),
            speculative_prefetch_busy_drop: Counter::default(),
            speculative_prefetch_aborted: Counter::default(),
        }
    }
}

impl Metrics {
    async fn wrap<U, T: Future<Output = U>, F: FnOnce(Arc<Self>) -> T>(
        self: Arc<Self>,
        fut: F,
    ) -> U {
        fut(self).await
    }
}

#[cfg(test)]
mod tests {
    use nativelink_macro::nativelink_test;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::StoreKey;
    use serial_test::serial;

    use super::*;

    /// #perf-obs render-test — pins the LITERAL Prometheus names of the
    /// worker EXECUTION `FastSlowStore` subtree that were structurally dark
    /// (the two-FSS-instance topology: the execution `effective_cas_store`
    /// built in `new_local_worker` is a SEPARATE instance from the idle
    /// CAS-server `WORKER_FAST_SLOW_STORE` registered via `store_manager`,
    /// so its read-path + peer-fetch counters never reached the registry).
    ///
    /// Builds the production-composition shape — `FastSlowStore` whose slow
    /// tier is a `WorkerProxyStore` — registers it through the production
    /// [`register_execution_store_metrics`], renders via the real
    /// `render_prometheus` path, and asserts the exact emitted names appear
    /// under the distinct `WORKER_EXEC_FAST_SLOW_STORE` prefix. Pins the
    /// counters that carry real execution activity (peer-fetch, singleflight
    /// dedup, worker inner-hit) plus the `slow_store` group placement.
    ///
    /// Guards the doubled-name / silent-zero trap (memory
    /// `worker-metrics-exposure-pattern` #86) AND the `CounterWithTime`
    /// `_counter` suffix artifact (peer-fetch is a `CounterWithTime`).
    ///
    /// Mutation (CLAUDE.md TDD #5): comment out the `metrics_registry.register`
    /// line in [`register_execution_store_metrics`] → the exec-instance names
    /// vanish from the render and this test red-fails with the bespoke
    /// `got:\n{body}` message naming the missing metric.
    #[nativelink_test]
    async fn execution_fss_metrics_render_under_exec_prefix() {
        use nativelink_config::stores::{FastSlowSpec, StoreDirection, StoreSpec};
        use nativelink_store::memory_store::MemoryStore;
        use nativelink_store::worker_proxy_store::WorkerProxyStore;
        use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
        use nativelink_util::metrics_publisher::render_prometheus;
        use nativelink_util::store_trait::Store;

        // Production-composition shape: FastSlowStore { fast: Memory,
        // slow: WorkerProxyStore(Memory) } — mirrors the execution
        // `effective_cas_store` built in `new_local_worker` (fast on-disk,
        // slow = worker-local WorkerProxyStore). `MemoryStore` fast tier
        // stands in for `FilesystemStore` (same MetricsComponent contract for
        // the counters under test, which live on the FSS + the slow-tier WPS).
        let fast = Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
        let inner_slow =
            Store::new(MemoryStore::new(&nativelink_config::stores::MemorySpec::default()));
        let proxy = WorkerProxyStore::new(inner_slow, new_shared_blob_locality_map());
        let slow = Store::new(proxy);
        // spec.fast / spec.slow are Noop because `FastSlowStore::new` takes the
        // concrete store handles directly; the spec only carries directions +
        // flags (same as the production construction in `new_local_worker`).
        let spec = FastSlowSpec {
            fast: StoreSpec::Noop(Default::default()),
            slow: StoreSpec::Noop(Default::default()),
            fast_direction: StoreDirection::Both,
            slow_direction: StoreDirection::Both,
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        };
        let exec_fss: Arc<FastSlowStore> = FastSlowStore::new(&spec, fast, slow);

        let registry = MetricsRegistry::new();
        // The production registration path — the load-bearing line the
        // mutation guard comments out.
        register_execution_store_metrics(&registry, exec_fss);
        let body = render_prometheus(&registry);

        // Peer-fetch is a `CounterWithTime`, so it renders with the `_counter`
        // suffix under the `slow_store` group of the exec FSS. Pinning the
        // literal string catches both a lost `slow_store` group and a lost
        // `_counter` suffix.
        assert!(
            body.contains(
                "nativelink_WORKER_EXEC_FAST_SLOW_STORE_slow_store_worker_proxy_peer_fetch_notfound_total_counter"
            ),
            "expected execution-instance peer-fetch counter \
             `nativelink_WORKER_EXEC_FAST_SLOW_STORE_slow_store_worker_proxy_peer_fetch_notfound_total_counter`, \
             got:\n{body}"
        );
        // Singleflight dedup hits (bare AtomicU64, nested slow_store→singleflight→inner).
        assert!(
            body.contains(
                "nativelink_WORKER_EXEC_FAST_SLOW_STORE_slow_store_singleflight_inner_total_dedup_hits"
            ),
            "expected execution-instance singleflight dedup counter \
             `nativelink_WORKER_EXEC_FAST_SLOW_STORE_slow_store_singleflight_inner_total_dedup_hits`, \
             got:\n{body}"
        );
        // Worker inner-hit (bare AtomicU64 `_total`, no CounterWithTime suffix).
        assert!(
            body.contains(
                "nativelink_WORKER_EXEC_FAST_SLOW_STORE_slow_store_wps_worker_read_inner_hit_total"
            ),
            "expected execution-instance worker inner-hit counter \
             `nativelink_WORKER_EXEC_FAST_SLOW_STORE_slow_store_wps_worker_read_inner_hit_total`, \
             got:\n{body}"
        );
        // Read-path hit counters render directly on the FSS (no group).
        assert!(
            body.contains("nativelink_WORKER_EXEC_FAST_SLOW_STORE_fast_store_hit_count"),
            "expected execution-instance `nativelink_WORKER_EXEC_FAST_SLOW_STORE_fast_store_hit_count`, \
             got:\n{body}"
        );
        // Regression guard: the inner-hit must NOT pick up a CounterWithTime
        // `_counter` suffix (it is a bare AtomicU64) — the doubled-artifact trap.
        assert!(
            !body.contains(
                "nativelink_WORKER_EXEC_FAST_SLOW_STORE_slow_store_wps_worker_read_inner_hit_total_counter"
            ),
            "inner-hit must render as bare `_total`, not with a `_counter` suffix, got:\n{body}"
        );
    }

    /// (A1 fix) T1 — empty-tick suppression. Stable steady state with
    /// N AC pins, no other deltas: the gate predicate MUST be true so
    /// the tick is suppressed.
    ///
    /// Mutation 2026-06-07: change the gate predicate in
    /// `should_skip_blobs_available_tick` to use
    /// `pinned_mirror_count == 0 && pinned_ac_mirror_count == 0`
    /// (i.e. revert to absolute snapshot check) → red-fail
    /// "empty-tick suppression failed: gate did NOT skip a stable-state
    /// tick with 5 AC pins unchanged".
    #[test]
    fn ac_pin_delta_t1_empty_tick_suppression() {
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 100);
        let d3 = DigestInfo::new([3u8; 32], 100);
        let d4 = DigestInfo::new([4u8; 32], 100);
        let d5 = DigestInfo::new([5u8; 32], 100);

        let stable: HashSet<DigestInfo> = [d1, d2, d3, d4, d5].into_iter().collect();
        // Same set both ticks.
        let (added, removed) = compute_ac_pin_delta_counts(&stable, &stable);
        assert_eq!(added, 0, "no adds when current == last");
        assert_eq!(removed, 0, "no removes when current == last");

        // With ac_pin_delta_empty=true and all other counts zero and is_first=false,
        // the gate MUST skip.
        let skip = should_skip_blobs_available_tick(
            /* is_first */ false,
            /* new_or_touched_count */ 0,
            /* evicted_count */ 0,
            /* added_subtree_count */ 0,
            /* removed_subtree_count */ 0,
            /* pinned_mirror_count */ 0,
            /* ac_pin_delta_empty */ true,
        );
        assert!(
            skip,
            "empty-tick suppression failed: gate did NOT skip a stable-state tick with 5 AC pins unchanged"
        );
    }

    /// (A1 fix) T2 — add-pin fires (over-action symmetry). Stable
    /// state with N pins; one new pin is added. The gate predicate
    /// MUST return false (fire the tick).
    ///
    /// Mutation 2026-06-07: short-circuit `current.difference(last)`
    /// in `compute_ac_pin_delta_counts` to always return an empty
    /// iterator → red-fail "add-pin tick suppressed: gate skipped a
    /// tick that added 1 new AC pin".
    #[test]
    fn ac_pin_delta_t2_add_pin_fires() {
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 100);
        let d3 = DigestInfo::new([3u8; 32], 100);
        let d4 = DigestInfo::new([4u8; 32], 100);
        let d5 = DigestInfo::new([5u8; 32], 100);
        let d6 = DigestInfo::new([6u8; 32], 100);

        let last: HashSet<DigestInfo> = [d1, d2, d3, d4, d5].into_iter().collect();
        let current: HashSet<DigestInfo> = [d1, d2, d3, d4, d5, d6].into_iter().collect();

        let (added, removed) = compute_ac_pin_delta_counts(&current, &last);
        assert_eq!(
            added, 1,
            "add-pin tick suppressed: expected added=1 (new digest d6), got added={added}"
        );
        assert_eq!(removed, 0, "no removes expected");

        let skip = should_skip_blobs_available_tick(
            false, 0, 0, 0, 0, 0,
            /* ac_pin_delta_empty */ added == 0 && removed == 0,
        );
        assert!(
            !skip,
            "add-pin tick suppressed: gate skipped a tick that added 1 new AC pin (expected 1 RPC with N+1 pins)"
        );
    }

    /// (A1 fix) T3 — remove-pin fires. Stable state; one pin removed.
    /// The gate predicate MUST return false.
    ///
    /// Mutation 2026-06-07: short-circuit `last.difference(current)`
    /// in `compute_ac_pin_delta_counts` to always return an empty
    /// iterator → red-fail "remove-pin tick suppressed: gate skipped
    /// a tick that removed 1 AC pin".
    #[test]
    fn ac_pin_delta_t3_remove_pin_fires() {
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 100);
        let d3 = DigestInfo::new([3u8; 32], 100);
        let d4 = DigestInfo::new([4u8; 32], 100);
        let d5 = DigestInfo::new([5u8; 32], 100);

        let last: HashSet<DigestInfo> = [d1, d2, d3, d4, d5].into_iter().collect();
        let current: HashSet<DigestInfo> = [d1, d2, d3, d4].into_iter().collect();

        let (added, removed) = compute_ac_pin_delta_counts(&current, &last);
        assert_eq!(added, 0, "no adds expected");
        assert_eq!(
            removed, 1,
            "remove-pin tick suppressed: expected removed=1 (d5 unpinned), got removed={removed}"
        );

        let skip = should_skip_blobs_available_tick(
            false, 0, 0, 0, 0, 0,
            /* ac_pin_delta_empty */ added == 0 && removed == 0,
        );
        assert!(
            !skip,
            "remove-pin tick suppressed: gate skipped a tick that removed 1 AC pin (expected 1 RPC with N-1 pins)"
        );
    }

    /// (A1 fix) T4 — reconnect re-sends snapshot (gate predicate
    /// shape). Asserts `is_first=true` bypasses
    /// `should_skip_blobs_available_tick` regardless of delta
    /// emptiness, and that against a cleared `last`, the snapshot
    /// reports every digest as added.
    ///
    /// NOTE: T4 exercises the gate predicate and the delta primitive,
    /// NOT the state-level clear at the function head of
    /// `send_periodic_blobs_available`. Mutation of the `is_first`
    /// clear is covered by [`t6_reconnect_clear_at_function_head_fires`].
    ///
    /// Mutation 2026-06-07: change `should_skip_blobs_available_tick`
    /// to ignore `is_first` (drop the `!is_first` clause) → red-fail
    /// "reconnect did not re-send full snapshot: is_first=true was not
    /// allowed past the gate".
    #[test]
    fn ac_pin_delta_t4_reconnect_resends_snapshot() {
        // (a) is_first=true bypasses the gate even when delta would suppress.
        let skip_on_first = should_skip_blobs_available_tick(
            /* is_first */ true,
            0,
            0,
            0,
            0,
            0,
            /* ac_pin_delta_empty */ true,
        );
        assert!(
            !skip_on_first,
            "reconnect did not re-send full snapshot: is_first=true was not allowed past the gate"
        );

        // (b) After clearing last_sent_ac_pin_set, current snapshot's
        // every digest is reported as added — the snapshot replay.
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 100);
        let d3 = DigestInfo::new([3u8; 32], 100);
        let current: HashSet<DigestInfo> = [d1, d2, d3].into_iter().collect();
        let cleared_last: HashSet<DigestInfo> = HashSet::new();
        let (added, removed) = compute_ac_pin_delta_counts(&current, &cleared_last);
        assert_eq!(
            added, 3,
            "reconnect did not re-send full snapshot: last_sent retained — expected 3 adds, got {added}"
        );
        assert_eq!(removed, 0, "no removes when last is empty");
    }

    /// (A1 fix-up F2) T6 — reconnect-clear at the function head fires.
    /// Pre-populate `last_sent_ac_pin_set` with {d1, d2}; drive
    /// [`apply_periodic_tick_memo_resets`] with `is_first=true`; assert
    /// the memo is cleared post-call AND the returned outcome is
    /// `ReconnectClear`. Closes the gap that T4 covered the gate
    /// predicate's `!is_first` clause but NOT the state-level clear.
    ///
    /// Mutation 2026-06-07: comment out the
    /// `if is_first { last_sent_ac_pin_set.lock().clear(); }` line at
    /// the head of `apply_periodic_tick_memo_resets` → red-fail
    /// "reconnect clear did not fire: last_sent retained pre-reconnect
    /// digests {d1, d2}".
    #[test]
    fn t6_reconnect_clear_at_function_head_fires() {
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 100);

        let last_sent = Mutex::new(HashSet::from([d1, d2]));

        let outcome = apply_periodic_tick_memo_resets(&last_sent, /* is_first */ true);

        let post_call: HashSet<DigestInfo> = last_sent.lock().iter().copied().collect();
        assert!(
            post_call.is_empty(),
            "reconnect clear did not fire: last_sent retained pre-reconnect digests {{d1, d2}} \
             (got {} entries)",
            post_call.len()
        );
        assert_eq!(
            outcome,
            PeriodicTickMemoReset::ReconnectClear,
            "reconnect clear did not fire: expected ReconnectClear outcome, got {outcome:?}"
        );
    }

    /// (FL-688 v3 Stage A) The PERIODIC full-snapshot heartbeat is REMOVED:
    /// across many steady-state (`is_first=false`) ticks with no blob changes,
    /// `apply_periodic_tick_memo_resets` must NEVER force-clear the
    /// `last_sent_ac_pin_set` memo (a forced clear was the periodic heartbeat,
    /// which re-sent the whole AC-pin set every Nth tick). Convergence now comes
    /// from reconnect full snapshot + the Stage-2B replay-until-acked reader +
    /// the over-cap force-snapshot EVENT — none of them a timer/tick.
    ///
    /// Drives 605 steady-state ticks: the OLD heartbeat would have fired at
    /// tick 600 (`60_000 / BLOBS_AVAILABLE_MAX_INTERVAL_MS`), clearing the memo.
    /// The memo is pre-populated so a forced clear is observable as an
    /// emptied set. Verified RED against the pre-Stage-A heartbeat code (the
    /// clear fires at tick 600); GREEN after the heartbeat is removed.
    ///
    /// Mutation: re-introduce the periodic clear (the
    /// `(tick + 1) % N == 0 { last_sent_ac_pin_set.lock().clear() }` line) →
    /// this test red-fails with its bespoke "periodic heartbeat fired" message.
    #[test]
    fn steady_state_ticks_never_force_full_snapshot() {
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 100);
        // Pre-populate the memo: a periodic forced clear would empty it.
        let last_sent = Mutex::new(HashSet::from([d1, d2]));

        for tick in 0..605u64 {
            let outcome =
                apply_periodic_tick_memo_resets(&last_sent, /* is_first */ false);
            assert_eq!(
                outcome,
                PeriodicTickMemoReset::None,
                "periodic heartbeat fired at steady-state tick {tick}: the timer-driven \
                 full-snapshot heartbeat must be REMOVED — steady-state ticks produce only \
                 event-driven deltas (got {outcome:?})"
            );
            assert_eq!(
                last_sent.lock().len(),
                2,
                "periodic heartbeat cleared the last-sent memo at steady-state tick {tick}: the \
                 forced full-snapshot heartbeat must be REMOVED (convergence is reconnect + \
                 replay-until-acked + over-cap event, NOT a periodic tick)"
            );
        }
    }

    /// (A1 fix) T5 — non-AC corner coverage. A fast-store eviction,
    /// pinned-mirror activity, touched digest, or subtree delta must
    /// fire the tick even if AC pin delta is empty. Without this we'd
    /// suppress legitimate CAS-side changes alongside the AC fix.
    ///
    /// Mutation 2026-06-07: replace any non-AC clause in
    /// `should_skip_blobs_available_tick` with `true` (e.g. drop
    /// `&& pinned_mirror_count == 0`) → red-fail one of the five
    /// `assert!(!should_skip_blobs_available_tick(...))` calls
    /// (Rust `assert!` panics with the predicate source — bespoke per
    /// clause).
    #[test]
    fn gate_does_not_swallow_non_ac_corners() {
        // pinned_mirror_count > 0 → tick fires.
        assert!(!should_skip_blobs_available_tick(
            false, 0, 0, 0, 0,
            /* pinned_mirror_count */ 1,
            true,
        ));
        // evicted_count > 0 → tick fires.
        assert!(!should_skip_blobs_available_tick(
            false, 0, 1, 0, 0, 0, true,
        ));
        // new_or_touched_count > 0 → tick fires.
        assert!(!should_skip_blobs_available_tick(
            false, 1, 0, 0, 0, 0, true,
        ));
        // added_subtree_count > 0 → tick fires.
        assert!(!should_skip_blobs_available_tick(
            false, 0, 0, 1, 0, 0, true,
        ));
        // removed_subtree_count > 0 → tick fires.
        assert!(!should_skip_blobs_available_tick(
            false, 0, 0, 0, 1, 0, true,
        ));
    }

    // (#locality-map-drift) Split a `swap()` result into (present, absent)
    // digest sets for assertion. Under the LWW map, `added` and `touched` both
    // collapse to `Present`; `evicted` is `Absent`.
    fn present_absent(
        changes: Vec<(DigestInfo, BlobState, Stamp)>,
    ) -> (HashSet<DigestInfo>, HashSet<DigestInfo>) {
        let mut present = HashSet::new();
        let mut absent = HashSet::new();
        for (d, state, _stamp) in changes {
            match state {
                BlobState::Present => {
                    present.insert(d);
                }
                BlobState::Absent => {
                    absent.insert(d);
                }
            }
        }
        (present, absent)
    }

    #[test]
    fn test_blob_change_tracker_eviction_collects_and_swaps() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        // Evict two digests via the callback (each carries its value's frozen
        // logical-LWW ts).
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d1), 1, 1));
        rt.block_on(tracker.callback(StoreKey::Digest(d2), 1, 2));

        // Swap should return both as absent (evicted).
        let (present, absent) = present_absent(tracker.swap());
        assert!(present.is_empty(), "Expected no present digests");
        assert_eq!(absent.len(), 2, "Expected 2 evicted digests");
        assert!(absent.contains(&d1), "Expected d1 in absent set");
        assert!(absent.contains(&d2), "Expected d2 in absent set");

        // Second swap should return empty.
        let (present2, absent2) = present_absent(tracker.swap());
        assert!(present2.is_empty());
        assert!(absent2.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_ignores_non_digest_keys() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));

        // Evict callback with a string key.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Str(Cow::Borrowed("some_key")), 1, 1));

        // Insert callback with a string key.
        tracker.on_insert(StoreKey::Str(Cow::Borrowed("other_key")), 42, 1, 2);

        let (present, absent) = present_absent(tracker.swap());
        assert!(present.is_empty());
        assert!(absent.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_insert_callback() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        tracker.on_insert(StoreKey::Digest(d1), 100, 1, 1);
        tracker.on_insert(StoreKey::Digest(d2), 200, 1, 2);

        let (present, absent) = present_absent(tracker.swap());
        assert_eq!(present.len(), 2, "Expected 2 present digests");
        assert!(present.contains(&d1));
        assert!(present.contains(&d2));
        assert!(absent.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_swap_returns_and_clears() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        // Accumulate an insert and an eviction.
        tracker.on_insert(StoreKey::Digest(d1), 100, 1, 1);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d2), 1, 2));

        // First swap returns the accumulated changes.
        let (present, absent) = present_absent(tracker.swap());
        assert_eq!(present.len(), 1);
        assert!(present.contains(&d1));
        assert_eq!(absent.len(), 1);
        assert!(absent.contains(&d2));

        // Second swap should be empty.
        let (present2, absent2) = present_absent(tracker.swap());
        assert!(present2.is_empty());
        assert!(absent2.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_insert_then_evict_records_eviction() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);

        // Insert@(1,1) then evict@(1,1) the SAME value (same frozen ts): the
        // eviction must win the ABSENT≻PRESENT tie-break so the server learns
        // the blob is gone.
        tracker.on_insert(StoreKey::Digest(d1), 100, 1, 1);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d1), 1, 1));

        let (present, absent) = present_absent(tracker.swap());
        assert!(
            !present.contains(&d1),
            "Expected d1 to NOT be present after insert+evict (same ts, ABSENT wins tie-break)"
        );
        assert!(
            absent.contains(&d1),
            "Expected d1 absent (it was evicted at its value's frozen ts)"
        );
    }

    #[test]
    fn test_blob_change_tracker_evict_then_reinsert_supersedes() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);

        // Evict V1@(1,1) then RE-INSERT V2@(1,2) (a NEW value, strictly-higher
        // counter — exactly what the real map mints). The re-insert's newer ts
        // must SUPERSEDE the stale evict: d1 ends PRESENT. This is the
        // false-missing fix at the tracker's local-LWW layer.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d1), 1, 1));
        tracker.on_insert(StoreKey::Digest(d1), 100, 1, 2);

        let (present, absent) = present_absent(tracker.swap());
        assert!(
            present.contains(&d1),
            "Expected d1 present after evict@1+reinsert@2 (newer ts supersedes stale evict)"
        );
        assert!(
            !absent.contains(&d1),
            "Expected d1 NOT absent after the higher-ts re-insert"
        );
    }

    #[test]
    fn test_blob_change_tracker_stale_evict_suppressed_by_prior_reinsert() {
        // (#locality-map-drift) The core false-missing guard at the tracker
        // layer: a re-insert V2@(1,2) followed by a LATE, out-of-order evict of
        // V1@(1,1) — the stale evict must LOSE, d1 stays PRESENT.
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([7u8; 32], 100);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        // Re-insert lands first (higher ts), then the stale evict arrives late.
        tracker.on_insert(StoreKey::Digest(d1), 100, 1, 2);
        rt.block_on(tracker.callback(StoreKey::Digest(d1), 1, 1));

        let (present, absent) = present_absent(tracker.swap());
        assert!(
            present.contains(&d1) && !absent.contains(&d1),
            "stale evict (ts=1) must NOT overwrite a re-insert (ts=2) at the \
             tracker LWW layer — d1 must stay present"
        );
    }

    // ---------------------------------------------------------------
    // Gap 4: BlobChangeTracker <-> MokaEvictingMap integration test
    // ---------------------------------------------------------------
    // Wires: MokaEvictingMap -> ItemCallbackHolder -> BlobChangeTracker
    // and verifies that inserts and evictions flow through correctly.
    #[test]
    fn test_blob_change_tracker_evicting_map_integration() {
        use std::time::SystemTime;

        use nativelink_config::stores::EvictionPolicy;
        use nativelink_store::callback_utils::ItemCallbackHolder;
        use nativelink_util::evicting_map::LenEntry;
        use nativelink_util::moka_evicting_map::MokaEvictingMap;
        use nativelink_util::store_trait::StoreKeyBorrow;

        // Simple value type for the MokaEvictingMap. (#locality-map-drift)
        // Carries the value's frozen logical-LWW counter in an `AtomicU64`,
        // exactly as `FileEntryImpl` does, so the eviction callback reports the
        // value's INSERT counter (not 0) — the value-carried ts the LWW needs.
        use core::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        #[derive(Debug)]
        struct TestValue {
            size: u64,
            stamp: AtomicU64,
        }
        impl TestValue {
            fn new(size: u64) -> Arc<Self> {
                Arc::new(Self {
                    size,
                    stamp: AtomicU64::new(0),
                })
            }
        }

        impl LenEntry for TestValue {
            fn len(&self) -> u64 {
                self.size
            }
            fn is_empty(&self) -> bool {
                self.size == 0
            }
            fn stamp(&self) -> u64 {
                self.stamp.load(AtomicOrdering::Acquire)
            }
            fn set_stamp(&self, s: u64) {
                self.stamp.store(s, AtomicOrdering::Release);
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async {
            // Create a MokaEvictingMap with max_count = 2 so the third
            // insert deterministically evicts the LRU. We avoid max_bytes
            // here because moka divides by an internal SCALE factor and
            // sub-1KB budgets are unstable across moka versions; max_count
            // is the predictable knob for unit tests.
            let evicting_map = std::sync::Arc::new(MokaEvictingMap::<
                StoreKeyBorrow,
                StoreKey<'static>,
                Arc<TestValue>,
                SystemTime,
                ItemCallbackHolder,
            >::with_anchor(
                &EvictionPolicy {
                    max_count: 2,
                    max_seconds: 0,
                    max_bytes: 0,
                    evict_bytes: 0,
                    pin_cap_bytes: 0,
                },
                SystemTime::now(),
            ));
            // Drain pending eviction events on a background task so the
            // tracker actually sees the eviction callback for d1 below.
            evicting_map.start_background_eviction();

            // Create a BlobChangeTracker and register it.
            let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
            let holder = ItemCallbackHolder::new(tracker.clone());
            evicting_map.add_item_callback(holder);

            let d1 = DigestInfo::new([1u8; 32], 30);
            let d2 = DigestInfo::new([2u8; 32], 40);

            // Insert two items at capacity for max_count=2.
            let key1: StoreKeyBorrow = StoreKey::Digest(d1).into();
            let key2: StoreKeyBorrow = StoreKey::Digest(d2).into();
            evicting_map.insert(key1, TestValue::new(30)).await;
            evicting_map.insert(key2, TestValue::new(40)).await;

            // Swap and verify both digests appear as present.
            let (present, absent) = present_absent(tracker.swap());
            assert_eq!(
                present.len(),
                2,
                "Expected 2 present digests after initial inserts"
            );
            assert!(present.contains(&d1), "Expected d1 present");
            assert!(present.contains(&d2), "Expected d2 present");
            assert!(absent.is_empty(), "Expected no evictions yet");

            // Now insert a third item — exceeds max_count=2 so the LRU
            // entry (d1) must be evicted. Promote d2 explicitly via get
            // so LRU order makes d1 the eviction victim.
            let d2_key = StoreKey::Digest(d2);
            let _ = evicting_map.get(&d2_key).await;
            let d3 = DigestInfo::new([3u8; 32], 50);
            let key3: StoreKeyBorrow = StoreKey::Digest(d3).into();
            evicting_map.insert(key3, TestValue::new(50)).await;

            // Wait for the background drainer to fire the eviction
            // callback. start_background_eviction owns the drain task; a
            // few yields are usually enough but give it generous slack
            // since current_thread runtime serializes.
            for _ in 0..50 {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // (#locality-map-drift) d1's eviction callback carries d1's frozen
            // insert counter (a lower value than d3's insert), and d1 was NOT
            // re-inserted, so the LWW records d1 ABSENT — the value-carried ts
            // makes a genuine LRU eviction land.
            let (present, absent) = present_absent(tracker.swap());
            assert!(
                present.contains(&d3),
                "Expected d3 present after third insert"
            );
            assert!(
                absent.contains(&d1),
                "Expected d1 absent (LRU eviction, value-carried ts)"
            );
            assert!(
                !absent.contains(&d2),
                "Expected d2 to NOT be evicted (most recently used)"
            );
        });
    }

    #[test]
    fn test_cas_advertised_endpoint_format() {
        let endpoint = cas_advertised_endpoint(50081, false);
        assert!(
            endpoint.starts_with("grpc://"),
            "Expected endpoint to start with 'grpc://', got: {endpoint}"
        );
        assert!(
            endpoint.ends_with(":50081"),
            "Expected endpoint to end with ':50081', got: {endpoint}"
        );

        // Extract hostname and verify it's non-empty.
        let without_prefix = endpoint.strip_prefix("grpc://").unwrap();
        let hostname = without_prefix.strip_suffix(":50081").unwrap();
        assert!(
            !hostname.is_empty(),
            "Expected non-empty hostname in endpoint: {endpoint}"
        );
    }

    #[test]
    fn test_cas_advertised_endpoint_tls() {
        let endpoint = cas_advertised_endpoint(40081, true);
        assert!(
            endpoint.starts_with("grpcs://"),
            "Expected endpoint to start with 'grpcs://', got: {endpoint}"
        );
        assert!(
            endpoint.ends_with(":40081"),
            "Expected endpoint to end with ':40081', got: {endpoint}"
        );
    }

    /// The swap-pressure RATE is the load-bearing dynamic signal: it is
    /// the DELTA of the cumulative swap-pressure counter
    /// (`compressions`/`pswpout`) divided by the REAL elapsed interval.
    /// This proves the delta-rate arithmetic that the sampler thread runs
    /// every tick.
    ///
    /// Mutation step (CLAUDE.md TDD #5): in `compute_swap_pressure_rate`,
    /// replace `curr_count.saturating_sub(prev_count)` with `curr_count`
    /// (report the ABSOLUTE counter instead of the delta). This test
    /// red-fails at the steady-rate assertion with the bespoke message
    /// naming the delta-not-absolute contract.
    #[test]
    fn compute_swap_pressure_rate_is_delta_over_interval() {
        // 1000 events over 2 s ⇒ 500 events/sec.
        assert_eq!(
            compute_swap_pressure_rate(10_000, 11_000, 2.0),
            500,
            "swap-pressure rate must be (curr - prev) / elapsed = 1000/2 = 500; a \
             non-500 value means the sampler reported the ABSOLUTE cumulative \
             counter instead of the per-interval DELTA (the lingering-counter bug)"
        );
        // No change in counter ⇒ no pressure.
        assert_eq!(
            compute_swap_pressure_rate(11_000, 11_000, 1.0),
            0,
            "a flat cumulative counter must report zero swap-pressure rate"
        );
        // Counter reset (reboot / wrap): curr < prev ⇒ clamp to 0, never
        // a garbage negative-turned-huge spike.
        assert_eq!(
            compute_swap_pressure_rate(11_000, 5, 1.0),
            0,
            "a counter reset (curr < prev) must clamp to 0, not report a \
             spurious spike from the underflow"
        );
        // Non-advancing clock ⇒ 0 (avoid divide-by-zero garbage).
        assert_eq!(
            compute_swap_pressure_rate(10_000, 11_000, 0.0),
            0,
            "a non-advancing wall clock must yield 0, not a divide-by-zero"
        );
    }

    /// The heartbeat reads host memory pressure via the process-global
    /// sampler atomics (`get_swap_used_bytes` / `get_memory_pressure_level`),
    /// exactly like `get_cpu_load_pct`. This fakes a sampler tick by
    /// storing into the atomics and asserts the reader observes it — the
    /// path the periodic + post-action heartbeat build sites use.
    ///
    /// Mutation step: change `get_memory_pressure_level` to read `CPU_PCT`
    /// instead of `MEMORY_PRESSURE_LEVEL` (wrong static). This test
    /// red-fails because the faked level value is not observed.
    ///
    /// `#[serial(swap_sampler_atomics)]`: this test and
    /// `sample_mem_pressure_first_tick_publishes_live_anchor` both mutate the
    /// process-global `SWAP_USED_BYTES` / `MEMORY_PRESSURE_LEVEL` statics.
    /// Under the default multi-threaded test runner the other test's stores
    /// race this read; serializing the two removes the cross-test interleave
    /// (no production-path change).
    #[test]
    #[serial(swap_sampler_atomics)]
    fn heartbeat_reads_swap_pressure_from_sampler_atomics() {
        // Fake a sampler tick.
        SWAP_USED_BYTES.store(7_654_321, Ordering::Relaxed);
        MEMORY_PRESSURE_LEVEL.store(1337, Ordering::Relaxed);
        assert_eq!(
            get_swap_used_bytes(),
            7_654_321,
            "heartbeat must read swap_used_bytes from the SWAP_USED_BYTES \
             sampler atomic"
        );
        assert_eq!(
            get_memory_pressure_level(),
            1337,
            "heartbeat must read memory_pressure_level from the \
             MEMORY_PRESSURE_LEVEL sampler atomic"
        );
    }

    /// `sample_mem_pressure` MUST publish the monotonic `LAST_SAMPLE_INSTANT`
    /// liveness anchor on EVERY tick (BEFORE the signal read can
    /// early-return), so the gate's sample-age fail-open (§3a) sees a live
    /// sampler. This guards the rev-4 invariant that a sampler that IS
    /// ticking never trips the age fail-open, while a sampler that STOPS
    /// ticking does.
    ///
    /// Note: `LAST_SAMPLE_INSTANT` is monotonic nanos since `PROCESS_START`.
    /// On the very FIRST sampler reference, `PROCESS_START` lazy-inits to a
    /// moment AT/AFTER the tick's `now`, so the first tick can legitimately
    /// publish 0 (read as never-sampled → fail-open, which is correct that
    /// early). We therefore initialize `PROCESS_START` first, then assert a
    /// SUBSEQUENT tick publishes a strictly positive anchor.
    ///
    /// `#[serial(swap_sampler_atomics)]`: shares the process-global
    /// `LAST_SAMPLE_INSTANT` static with the other sampler-atomic tests;
    /// serialized so a concurrent store cannot race this read.
    #[test]
    #[serial(swap_sampler_atomics)]
    fn sample_mem_pressure_first_tick_publishes_live_anchor() {
        // Initialize PROCESS_START so the tick's `now` is strictly after it.
        let start = *PROCESS_START;
        // Busy-wait a hair so `Instant::now()` inside the tick is strictly
        // greater than `start` (no sleep-as-synchronization — this is a
        // monotonic-clock advance guarantee, not cross-thread coordination).
        while Instant::now() <= start {
            core::hint::spin_loop();
        }
        LAST_SAMPLE_INSTANT.store(0, Ordering::Relaxed);
        let next = sample_mem_pressure(SwapSamplerState::new());
        let after = LAST_SAMPLE_INSTANT.load(Ordering::Relaxed);
        assert!(
            after > 0,
            "sample_mem_pressure must publish a fresh strictly-positive \
             monotonic LAST_SAMPLE_INSTANT anchor on every tick (once \
             PROCESS_START is initialized) so the sample-age fail-open sees \
             a live sampler; anchor was {after}"
        );
        // The returned state carries the anchor for the next tick. We don't
        // assert the cumulative anchor's presence because the no-op
        // `mem_impl` legitimately returns None (unsupported target).
        let _ = next;
    }

    /// (#37) The EWMA the gate keys off is fast-ATTACK / slow-RELEASE
    /// (§3b): a rising sample folds in with the large attack weight (a
    /// post-action peak trips the estimate fast), a falling sample with
    /// the small release weight (the estimate decays slowly so one spike
    /// cannot retrip on the next tick — the steady-state oscillation
    /// guard).
    ///
    /// Mutation step: swap the attack/release selection (use RELEASE on
    /// rising, ATTACK on falling). The attack assertion red-fails: a 0→N
    /// step would barely move the estimate instead of jumping it.
    #[test]
    fn swap_ewma_is_fast_attack_slow_release() {
        // Rising from 0 toward 10_000 uses the ATTACK weight (0.5):
        // 0 + 0.5*(10000-0) = 5000.
        let attacked = update_swap_ewma(0.0, 10_000.0);
        assert!(
            (attacked - 5_000.0).abs() < 1e-6,
            "rising sample must use the fast-ATTACK weight (estimate should \
             jump toward the peak); got {attacked}, expected 5000"
        );
        // Falling from 10_000 toward 0 uses the RELEASE weight (0.05):
        // 10000 + 0.05*(0-10000) = 9500 — barely moves (slow release).
        let released = update_swap_ewma(10_000.0, 0.0);
        assert!(
            (released - 9_500.0).abs() < 1e-6,
            "falling sample must use the slow-RELEASE weight (estimate should \
             decay slowly so one completed action cannot retrip the gate); \
             got {released}, expected 9500"
        );
        assert!(
            released > attacked,
            "slow-release must hold the estimate HIGHER after a drop than \
             fast-attack lifts it from idle — the asymmetry IS the hysteresis"
        );
    }

    /// (#37 rev-4, deliverable (a)) The PRIMARY free-floor trip: free BELOW
    /// `FREE_FLOOR_BYTES` ⇒ gate; free at/above the floor (out of the
    /// hysteresis band) ⇒ no gate. This is the leading-signal trip that
    /// reads the at-the-RAM-wall case (68 MiB free) as pressured while the
    /// re-fault rate is still silent (design §0-rev4.2).
    ///
    /// Mutation step (CLAUDE.md TDD #5): in `free_floor_breached`, invert
    /// the PRIMARY comparator (`free_bytes < FREE_FLOOR_BYTES` →
    /// `free_bytes > FREE_FLOOR_BYTES`). The at-the-wall assertion below
    /// red-fails with its bespoke "free below the floor must trip" message —
    /// proving the comparator direction is load-bearing.
    #[test]
    fn free_floor_trips_below_floor_not_above() {
        // At the RAM wall: 68 MiB free ≪ 1 GiB floor ⇒ MUST trip.
        assert!(
            free_floor_breached(68 << 20, false),
            "free below the floor must trip the gate: 68 MiB free is the \
             at-the-RAM-wall case the free-floor PRIMARY exists to catch \
             (the re-fault rate reads it as healthy) — comparator inverted?"
        );
        // Healthy headroom: 8969 MiB free ≫ floor+hysteresis ⇒ MUST NOT trip.
        assert!(
            !free_floor_breached(8969 << 20, false),
            "free far above the floor must NOT trip: 8969 MiB is the healthy \
             IDLE headroom; tripping here would false-gate a healthy worker"
        );
        // Exactly at the floor is NOT below it (strict `<`) ⇒ no trip from
        // a cold state.
        assert!(
            !free_floor_breached(FREE_FLOOR_BYTES, false),
            "free exactly at the floor is not BELOW it; the trip is strict <"
        );
        // One byte below the floor ⇒ trip.
        assert!(
            free_floor_breached(FREE_FLOOR_BYTES - 1, false),
            "one byte below the floor must trip"
        );
    }

    /// (#37 rev-4) The free-floor is a LEVEL with a two-threshold hysteresis
    /// band (design §0-rev4.4): once tripped it stays tripped until free
    /// recovers ABOVE `FREE_FLOOR_BYTES + FREE_FLOOR_HYSTERESIS`, so a worker
    /// hovering at the boundary does not flap admit/refuse every tick.
    #[test]
    fn free_floor_hysteresis_band_holds_prior_state() {
        // In the band (between floor and floor+hysteresis):
        let in_band = FREE_FLOOR_BYTES + (FREE_FLOOR_HYSTERESIS / 2);
        // ...holds tripped if it was tripped...
        assert!(
            free_floor_breached(in_band, true),
            "in the hysteresis band, a tripped gate must STAY tripped \
             (no admit/refuse chatter at the boundary)"
        );
        // ...and holds clear if it was clear.
        assert!(
            !free_floor_breached(in_band, false),
            "in the hysteresis band, a clear gate must STAY clear"
        );
        // Above the band ⇒ clears regardless of prior state.
        assert!(
            !free_floor_breached(FREE_FLOOR_BYTES + FREE_FLOOR_HYSTERESIS, false),
            "free recovered above floor+hysteresis must clear the trip"
        );
    }

    /// (#task-memgate-twosignal) The free-floor shortfall DIAGNOSTIC gauge
    /// (`pressure_level_mib`): MiB below the free-floor, `0` when at/above it.
    /// NO LONGER the wire scalar (the compressor-churn EWMA is) — kept as a
    /// standalone observability gauge for the free-floor fail-safe. Higher =
    /// deeper shortfall.
    #[test]
    fn memory_pressure_level_is_mib_below_floor() {
        // 256 MiB below the floor ⇒ level 256.
        assert_eq!(
            memory_pressure_level_mib(FREE_FLOOR_BYTES - (256 << 20)),
            256,
            "level must be MiB below the free-floor"
        );
        // At/above the floor ⇒ 0 (healthy).
        assert_eq!(
            memory_pressure_level_mib(FREE_FLOOR_BYTES),
            0,
            "at the floor, the shortfall (and thus the level) is 0"
        );
        assert_eq!(
            memory_pressure_level_mib(8969 << 20),
            0,
            "healthy headroom reports level 0"
        );
        // A deeper shortfall reports a HIGHER level (the ranking direction).
        assert!(
            memory_pressure_level_mib(10 << 20) > memory_pressure_level_mib(900 << 20),
            "a deeper free-shortfall must report a HIGHER level so the \
             server's min_by_key fail-open picks the least-pressured worker"
        );
    }

    /// (#37 rev-4, deliverable (b)) A DEAD sampler must fail OPEN even if
    /// the last published verdict was "pressured" (e.g. the last free
    /// reading was below the floor). A wedged sampler that froze with
    /// `MEMORY_PRESSURED = true` must NOT pin the worker gated forever —
    /// the sample-age fail-open is what releases it (§3a rule 2 / §5 case
    /// 4). This composes the value atomic (frozen "pressured") with a STALE
    /// age, exactly the production read in `swap_gate_pressured`.
    ///
    /// Mutation step (CLAUDE.md TDD #5): delete the
    /// `if age > max_age { return (false, true) }` arm in
    /// `swap_gate_verdict` (let the stale "pressured" value fall through).
    /// This test red-fails with its bespoke "dead sampler must fail open"
    /// message — proving the age check, not just the value, gates.
    #[test]
    fn dead_sampler_fails_open_even_when_last_reading_was_below_floor() {
        let max_age = SWAP_SAMPLE_MAX_AGE;
        // The value atomic is frozen at "pressured" (the free floor was
        // breached on the sampler's last live tick), but the sampler then
        // DIED: its last anchor is at t=1s and "now" is t=10s ⇒ age 9s ≫ 2s.
        let (pressured, stale) = swap_gate_verdict(
            true,                                      // gate enabled
            true,                                      // last verdict: pressured (below floor)
            Duration::from_secs(1).as_nanos() as u64,  // last sample at t=1s
            Duration::from_secs(10),                   // now at t=10s
            max_age,
        );
        assert!(
            !pressured,
            "dead sampler must fail open: a wedged sampler frozen at \
             'pressured' (its last free reading was below the floor) must \
             NOT keep the worker gated forever — the sample-age fail-open \
             releases it"
        );
        assert!(
            stale,
            "the stale path must be flagged so the wedged-sampler warn! fires"
        );
    }

    /// (#37 re-enable follow-up, deliverable (c) — numeric-constant discipline)
    /// Pin the CONSERVATIVE available-floor value at the declaration site so a
    /// doc-comment rewrite cannot drift it. 1 GiB is ~6.25% of a 16 GiB
    /// worker; healthy `available` (free+inactive+purgeable) is ~7-8 GiB →
    /// wide margin with near-zero false-trip risk (empirical fleet probe
    /// 2026-06-28: three workers all at 7.2-8.1 GiB available). The old raw
    /// `free_count` floor that caused the #64 NAK storm (290-903 MiB on those
    /// same workers) is now replaced by `available`. The floor constant itself
    /// is unchanged at 1 GiB; the formula that computes what is measured
    /// against it has changed.
    ///
    /// `#[serial(swap_sampler_atomics)]`: this test reads `SWAPIN_CONFIRM_RATE`,
    /// `SWAPIN_CONFIRM_WINDOW_TICKS`, and `MEMORY_GATE_ENABLED` — process-global
    /// statics mutated by `decompress_churn_does_not_hard_nak` /
    /// `sustained_swapin_trips_gate`. Serialized to prevent a concurrent store
    /// racing this test's load+assert.
    #[test]
    #[serial(swap_sampler_atomics)]
    fn free_floor_bytes_is_conservative_one_gib() {
        assert_eq!(
            FREE_FLOOR_BYTES,
            1_073_741_824,
            "FREE_FLOOR_BYTES must be 1 GiB (1 << 30): the conservative \
             safe-enable margin. Against `available` (free+inactive+purgeable \
             ≈ 7-8 GiB on a healthy 16 GiB worker), this floor is ~6.25% of \
             physical — near-zero false-trip risk. If this drifts, re-justify \
             from the probe data in the design doc"
        );
        // The hysteresis band is a quarter of the floor (256 MiB).
        assert_eq!(
            FREE_FLOOR_HYSTERESIS,
            268_435_456,
            "FREE_FLOOR_HYSTERESIS must be 256 MiB (256 << 20)"
        );
        // (#task-memgate-twosignal) The SWAPIN OOM threshold + window static
        // inits. 100/s is safe by swapin semantics + the sustained window (the
        // busy-worker baseline is unmeasured); 10 ticks ≈ 1 s at the 100 ms
        // cadence rejects one-off spikes.
        assert_eq!(
            SWAPIN_CONFIRM_RATE.load(Ordering::Relaxed), 100,
            "SWAPIN_CONFIRM_RATE static init must be 100/s (OOM threshold; safety \
             from swapin semantics + sustained window, busy-worker baseline unmeasured)"
        );
        assert_eq!(
            SWAPIN_CONFIRM_WINDOW_TICKS.load(Ordering::Relaxed), 10,
            "SWAPIN_CONFIRM_WINDOW_TICKS static init must be 10 ticks (≈1 s at the \
             100 ms sampler cadence — the sustained-window that rejects spikes)"
        );
        // The gate is config-driven and DEFAULTS OFF. The `MEMORY_GATE_ENABLED`
        // static is set from `LocalWorkerConfig::memory_gate_enabled` at startup
        // (default false). In a fresh test process (no startup call) the static
        // stays at its init value `false`. This asserts the DEFAULT is disabled
        // so a doc-comment rewrite or a default-value change cannot silently
        // re-enable the gate fleet-wide without a test failure.
        //
        // The re-enable condition has been met (floor now reads `available` =
        // free+inactive+purgeable, not raw free_count — the #64 incident's
        // explicit prescription); enablement is now a per-worker config field
        // gated by a canary soak, not a compile-time constant.
        assert!(
            !MEMORY_GATE_ENABLED.load(Ordering::Relaxed),
            "MEMORY_GATE_ENABLED must DEFAULT to false — gate is config-driven \
             (LocalWorkerConfig::memory_gate_enabled) and must remain disabled \
             until the per-worker canary soak passes. Do NOT change the AtomicBool \
             init value; flip via config on the canary worker only"
        );
    }

    /// (#37 re-enable follow-up) `available` floor math: the PRIMARY is now
    /// `(free_count + inactive_count + purgeable_count) * page_size`, not raw
    /// `free_count`. A fixture where raw free is LOW (300 MiB, sub-floor) but
    /// inactive+purgeable are HIGH (7 GiB) must NOT trip the floor, because
    /// `available` ≈ 7.3 GiB >> `FREE_FLOOR_BYTES = 1 GiB`.
    ///
    /// This directly tests the #64 regression: the incident condition was raw
    /// free ≈ 290-903 MiB on healthy workers → floor tripped → 3.6k-NAK/min
    /// storm. With the corrected formula, those same workers would measure
    /// ~7.2-8.1 GiB available and the floor would NOT trip.
    ///
    /// The test calls `mem_impl::compute_available_bytes` — the SAME helper
    /// that `read_memory_signals` calls on the production path. Any mutation
    /// to the helper body therefore red-fails this test (mutation guard is on
    /// production code, not an inline replica).
    ///
    /// Mutation (CLAUDE.md TDD #5): the test asserts BOTH directions —
    /// (a) the correct formula (available ≈ 7.3 GiB) does NOT trip, and
    /// (b) the wrong formula (raw free = 300 MiB) DOES trip. Both assertions
    /// are bespoke-messaged. Mutation: revert `compute_available_bytes` body to
    /// `free_count.saturating_mul(page_size)` → `available_bytes` drops to
    /// 300 MiB → `free_floor_breached` returns `true` → assertion (a) fires
    /// with "#64 regression".
    #[test]
    fn available_floor_does_not_false_trip_when_raw_free_is_low() {
        // Fixture: raw free = 300 MiB (sub-floor), inactive = 7 GiB,
        // purgeable = 32 MiB → available ≈ 7.332 GiB >> 1 GiB floor.
        let page_size_bytes: u64 = 16_384; // 16 KiB (Apple Silicon M4 page size)
        let raw_free_mib: u64 = 300;
        let inactive_mib: u64 = 7_000;
        let purgeable_mib: u64 = 32;
        let mib: u64 = 1 << 20;
        let free_pages = (raw_free_mib * mib) / page_size_bytes;
        let inactive_pages = (inactive_mib * mib) / page_size_bytes;
        let purgeable_pages = (purgeable_mib * mib) / page_size_bytes;

        // (a) CORRECT formula via the production helper: available = free + inactive + purgeable.
        let available_bytes =
            compute_available_bytes(free_pages, inactive_pages, purgeable_pages, page_size_bytes);
        let tripped_correct = free_floor_breached(available_bytes, false);
        assert!(
            !tripped_correct,
            "raw-free floor false-trips at {raw_free_mib} MiB raw free \
             (the #64 regression): free_floor_breached returned true with \
             available≈{} MiB — correct formula must NOT trip. \
             Mutation target: revert compute_available_bytes body to \
             free_count.saturating_mul(page_size)",
            available_bytes >> 20
        );

        // (b) WRONG formula: raw free only (the #64 formula). This SHOULD trip.
        let raw_free_bytes = free_pages.saturating_mul(page_size_bytes);
        let tripped_wrong = free_floor_breached(raw_free_bytes, false);
        assert!(
            tripped_wrong,
            "test fixture error: the raw-free-only formula ({raw_free_mib} MiB) \
             must trip the 1 GiB floor — the fixture is not reproducing the \
             #64 regression condition"
        );
    }

    /// (#37 re-enable follow-up) Speculative-excluded: `available` does NOT
    /// add `speculative_count` — speculative pages are already in `free_count`
    /// (XNU vm_statistics.h:158-163). A fixture with non-zero speculative pages
    /// must produce `available = free+inactive+purgeable`, NOT +speculative.
    ///
    /// This is the AA-6 auditor catch (3rd wrong-field error on this gate).
    ///
    /// The test calls `mem_impl::compute_available_bytes` — the SAME helper
    /// that `read_memory_signals` calls on the production path. Any mutation
    /// to the helper body therefore red-fails this test.
    ///
    /// Mutation (CLAUDE.md TDD #5): add `speculative_count` to the signature
    /// and body of `compute_available_bytes`. The `formula_result` will exceed
    /// `expected_available` by `speculative_count * page_size` (~293 MiB),
    /// causing the assert_eq below to fail with: "speculative pages must NOT
    /// be added to available — they are already in free_count (XNU
    /// vm_statistics.h:158-163); adding them double-counts by ~293 MiB".
    #[test]
    fn available_formula_excludes_speculative_count() {
        // Fixture modelling a real Apple Silicon M4 worker (worker-01 probe
        // values rounded): free=323 MiB, inactive=6621 MiB, purgeable=32 MiB,
        // speculative=293 MiB. The raw vm_stat "free" shown to users is
        // (free_count - speculative_count) * page_size because the kernel
        // already includes speculative in free_count.
        let page_size_bytes: u64 = 16_384; // 16 KiB
        let mib: u64 = 1 << 20;
        let free_mib: u64 = 323; // raw free_count * page_size (includes speculative)
        let inactive_mib: u64 = 6_621;
        let purgeable_mib: u64 = 32;
        let speculative_mib: u64 = 293; // ALREADY in free_count; must NOT be added again
        let free_count = (free_mib * mib) / page_size_bytes;
        let inactive_count = (inactive_mib * mib) / page_size_bytes;
        let purgeable_count = (purgeable_mib * mib) / page_size_bytes;
        let speculative_count = (speculative_mib * mib) / page_size_bytes;
        // Correct expected: free + inactive + purgeable (no speculative).
        let expected_available = free_count
            .saturating_add(inactive_count)
            .saturating_add(purgeable_count)
            .saturating_mul(page_size_bytes);
        // Wrong formula (what happens if speculative is added):
        let wrong_available = free_count
            .saturating_add(inactive_count)
            .saturating_add(purgeable_count)
            .saturating_add(speculative_count)
            .saturating_mul(page_size_bytes);
        // Verify the two differ by speculative_count * page_size (fixture sanity).
        let double_count_bytes = speculative_count.saturating_mul(page_size_bytes);
        assert_eq!(
            wrong_available - expected_available,
            double_count_bytes,
            "test fixture error: wrong_available - expected_available should \
             be exactly speculative_count * page_size"
        );
        // The production helper must produce `expected_available`, not `wrong_available`.
        // Calls compute_available_bytes — the SAME function read_memory_signals uses —
        // so a mutation to the helper body red-fails here (not an inline replica).
        let formula_result =
            compute_available_bytes(free_count, inactive_count, purgeable_count, page_size_bytes);
        assert_eq!(
            formula_result,
            expected_available,
            "speculative pages must NOT be added to available — they are already \
             in free_count (XNU vm_statistics.h:158-163); adding them double-counts \
             by ~{speculative_mib} MiB. Mutation target: add speculative_count to \
             compute_available_bytes"
        );
        // Belt-and-suspenders: formula_result must differ from wrong_available.
        assert_ne!(
            formula_result,
            wrong_available,
            "formula must exclude speculative_count — if this fires, the production \
             helper or the fixture both have speculative_count, masking the mutation"
        );
    }

    /// (#sched: fix swapped P/E core-load metric) THE regression guard.
    /// Apple Silicon `host_processor_info` enumerates E-cores FIRST (logical
    /// CPUs `0..e_count`) and P-cores LAST (`e_count..cpu_count`). The old
    /// `i < p_count` bucket test therefore charged the first `p_count`
    /// E-cores into the P bucket — SWAPPING `p_core_load_pct` /
    /// `e_core_load_pct`. Empirically confirmed on M4 by
    /// `~/fl/bld/infra/nativelink/synthetic-core-metric-validate.sh`.
    ///
    /// This test pins THE invariant that was wrong: on the real M4 layout
    /// (p_count=4, e_count=6, cpu_count=10), the P bucket must collect the
    /// HIGH indices `6..10` (the 4 P-cores), NOT the low indices. The fixture
    /// loads the P-cores heavily and the E-cores lightly with DISTINCT per-CPU
    /// busy values so an off-by-one on either bucket edge changes the sum.
    ///
    /// The test calls `split_pe_ticks` — the SAME pure helper that
    /// `read_per_type_cpu_times` calls on the macOS production path — so any
    /// mutation to its bucket predicate red-fails here (mutation guard is on
    /// production code, not an inline replica).
    ///
    /// Mutation (CLAUDE.md TDD #5): revert the bucket test from `i >= e_count`
    /// to `i < p_count`. The P bucket then collects the low E-core indices
    /// (busy 1 each ⇒ 4) and the E bucket collects the busy P-cores
    /// (⇒ 402) — both bespoke asserts below fire naming the P/E-index
    /// inversion.
    #[test]
    fn split_pe_ticks_p_cores_are_the_high_indices_m4_layout() {
        // Real M4: 6 E-cores enumerated first (idx 0..6), 4 P-cores last
        // (idx 6..10). E-cores lightly loaded (busy 1), P-cores heavily
        // loaded (busy 100). total=1000 each so per-bucket totals are
        // 6000 (E) and 4000 (P).
        let p_count = 4u32;
        let e_count = 6u32;
        let cpu_count = 10u32;
        let mut per_cpu = Vec::new();
        for i in 0..cpu_count {
            let busy = if i >= e_count { 100 } else { 1 };
            per_cpu.push(CpuTicks { busy, total: 1000 });
        }

        let split = split_pe_ticks(cpu_count, p_count, e_count, &per_cpu);

        // P bucket = the 4 HIGH indices (6..10): busy 4*100 = 400.
        assert_eq!(
            split.p_core.busy, 400,
            "P bucket must collect the HIGH logical-CPU indices (i >= e_count) \
             on Apple Silicon (E-cores enumerate FIRST). Got busy={} (expected \
             400 = 4 P-cores x 100). A value of 4 means the P/E indices are \
             INVERTED (bucketing the first e-cores as P — the swapped-metric \
             bug); revert of `i >= e_count` to `i < p_count` produces exactly \
             this.",
            split.p_core.busy
        );
        assert_eq!(
            split.p_core.total, 4000,
            "P bucket total must be the 4 P-cores' total (4*1000); got {}",
            split.p_core.total
        );
        // E bucket = the 6 LOW indices (0..6): busy 6*1 = 6.
        assert_eq!(
            split.e_core.busy, 6,
            "E bucket must collect the LOW logical-CPU indices (i < e_count) on \
             Apple Silicon. Got busy={} (expected 6 = 6 E-cores x 1). A value \
             of 402 means the P/E indices are INVERTED (the busy P-cores were \
             charged to E — the swapped-metric bug).",
            split.e_core.busy
        );
        assert_eq!(
            split.e_core.total, 6000,
            "E bucket total must be the 6 E-cores' total (6*1000); got {}",
            split.e_core.total
        );
        // Aggregate is bucket-independent: all 10 CPUs.
        assert_eq!(split.aggregate.busy, 406, "aggregate busy = 4*100 + 6*1");
        assert_eq!(split.aggregate.total, 10_000, "aggregate total = 10*1000");
    }

    /// (#sched: fix swapped P/E core-load metric) Reverse-load counterpart to
    /// the M4-layout test: load the LOW indices (the E-cores) and idle the
    /// HIGH indices (the P-cores). Proves the split is directional — the E
    /// bucket must equal the sum of the low `0..e_count` CPUs and the P bucket
    /// must be ~0 when only the E-cores are busy.
    ///
    /// Mutation (CLAUDE.md TDD #5): revert to `i < p_count` → the busy low
    /// E-cores land in the P bucket, flipping both asserts.
    #[test]
    fn split_pe_ticks_reverse_load_low_indices_are_e_cores() {
        let p_count = 4u32;
        let e_count = 6u32;
        let cpu_count = 10u32;
        let mut per_cpu = Vec::new();
        for i in 0..cpu_count {
            // E-cores (low indices) busy, P-cores (high indices) idle.
            let busy = if i < e_count { 100 } else { 0 };
            per_cpu.push(CpuTicks { busy, total: 1000 });
        }

        let split = split_pe_ticks(cpu_count, p_count, e_count, &per_cpu);

        assert_eq!(
            split.e_core.busy, 600,
            "with only the LOW indices busy, the E bucket must equal their sum \
             (6*100=600); got {}. A value of ~0/400 means low indices were \
             mis-bucketed as P (P/E inversion).",
            split.e_core.busy
        );
        assert_eq!(
            split.p_core.busy, 0,
            "with the HIGH-index P-cores idle, the P bucket busy must be 0; got \
             {} (P/E inversion charged busy E-cores to P).",
            split.p_core.busy
        );
    }

    /// (#sched: fix swapped P/E core-load metric) Non-heterogeneous fallback:
    /// on Intel Macs / Linux `p_count == 0`, so `is_heterogeneous` is false
    /// and every CPU folds into the P bucket (= the aggregate), with the E
    /// bucket left at zero. The caller's `has_e_cores` (from `e_count > 0`)
    /// handles the "no E-cores → report saturated" policy separately; the
    /// split itself must not invent an E bucket.
    ///
    /// Mutation (CLAUDE.md TDD #5): delete the `if !is_heterogeneous { p_core =
    /// aggregate; }` fallback → the P bucket stays zero on Intel/Linux and the
    /// first assert fires ("all CPUs must fold into the P bucket").
    #[test]
    fn split_pe_ticks_non_heterogeneous_folds_all_into_p() {
        let p_count = 0u32; // Intel Mac / Linux: perflevel sysctl absent
        let e_count = 0u32;
        let cpu_count = 8u32;
        let mut per_cpu = Vec::new();
        for i in 0..cpu_count {
            per_cpu.push(CpuTicks {
                busy: 10 * u64::from(i + 1),
                total: 1000,
            });
        }
        let expected_busy: u64 = (1..=cpu_count).map(|i| 10 * u64::from(i)).sum();

        let split = split_pe_ticks(cpu_count, p_count, e_count, &per_cpu);

        assert_eq!(
            split.p_core, split.aggregate,
            "non-heterogeneous (p_count==0): all CPUs must fold into the P \
             bucket (== aggregate). p_core={:?} aggregate={:?}",
            split.p_core, split.aggregate
        );
        assert_eq!(
            split.p_core.busy, expected_busy,
            "P bucket busy must equal the sum of all CPUs on the fallback path"
        );
        assert_eq!(
            split.e_core,
            CpuTicks::default(),
            "non-heterogeneous: the E bucket must stay zero (no P/E split); got \
             {:?}",
            split.e_core
        );
    }

    /// (#sched: fix swapped P/E core-load metric) Count-mismatch guard: a
    /// future chip could report a non-zero `p_count` whose P+E counts do NOT
    /// partition every logical CPU (e.g. a third core class). The
    /// `p_count + e_count == cpu_count` half of the heterogeneity predicate
    /// must reject that and fall back to all-P rather than mis-attribute the
    /// unaccounted CPUs.
    ///
    /// Mutation (CLAUDE.md TDD #5): drop the `&& p_count + e_count == cpu_count`
    /// clause → the function treats the mismatched layout as heterogeneous and
    /// splits with a wrong `e_count`, so `p_core != aggregate` and this assert
    /// fires.
    #[test]
    fn split_pe_ticks_count_mismatch_falls_back_to_all_p() {
        let p_count = 4u32;
        let e_count = 4u32; // 4 + 4 != 10 → not a clean partition
        let cpu_count = 10u32;
        let per_cpu: Vec<CpuTicks> = (0..cpu_count)
            .map(|_| CpuTicks { busy: 50, total: 100 })
            .collect();

        let split = split_pe_ticks(cpu_count, p_count, e_count, &per_cpu);

        assert_eq!(
            split.p_core, split.aggregate,
            "when p_count + e_count != cpu_count the layout is not a clean P/E \
             partition — must fall back to all-P (p_core == aggregate); got \
             p_core={:?} aggregate={:?}",
            split.p_core, split.aggregate
        );
        assert_eq!(
            split.e_core,
            CpuTicks::default(),
            "count-mismatch fallback must leave the E bucket zero; got {:?}",
            split.e_core
        );
    }

    /// (#37 re-enable follow-up) Config default OFF: an absent
    /// `memory_gate_enabled` field in `LocalWorkerConfig` deserialization
    /// must produce `false` (gate disabled). The `AtomicBool` init value
    /// must also be `false` so a fresh process (before `new_local_worker` is
    /// called) defaults to the gate being off.
    ///
    /// Mutation (CLAUDE.md TDD #5): change `#[serde(default)]` to
    /// `#[serde(default = "default_true")]` on the config field — the
    /// `serde_json5::from_str(...).expect()` at the parse site panics because
    /// the helper fn `default_true` does not exist, not because the assertion
    /// fires. Removing `#[serde(default)]` entirely also panics at the parse
    /// `expect()` (field absent with no default). For the `AtomicBool` path:
    /// change `AtomicBool::new(false)` to `AtomicBool::new(true)` → the static
    /// assertion in `free_floor_bytes_is_conservative_one_gib` red-fails with
    /// "MEMORY_GATE_ENABLED must DEFAULT to false".
    #[test]
    fn memory_gate_config_defaults_off() {
        use nativelink_config::cas_server::LocalWorkerConfig;
        // Minimal JSON5 config — only required fields, no memory_gate_enabled.
        // Required fields that have no #[serde(default)]:
        // worker_api_endpoint (uri required), cas_fast_slow_store, work_directory,
        // platform_properties (empty is valid as {}).
        let json5 = r#"{
            worker_api_endpoint: {uri: "grpc://localhost:50061"},
            cas_fast_slow_store: "cas_fast_slow",
            work_directory: "/tmp/work",
            platform_properties: {}
        }"#;
        let cfg: LocalWorkerConfig = serde_json5::from_str(json5).expect(
            "config parse must succeed for a valid minimal LocalWorkerConfig",
        );
        assert!(
            !cfg.memory_gate_enabled,
            "memory_gate_enabled must default to false when absent from config \
             — gate must be DISABLED by default (no prod behavior change). \
             Mutation target: change serde default on LocalWorkerConfig::memory_gate_enabled"
        );
        // The static init default is AtomicBool::new(false) — tested implicitly
        // by free_floor_bytes_is_conservative_one_gib which reads the static
        // directly. The config test verifies the serde default independently.
    }

    /// (#37 re-enable follow-up) The `available` floor correctly does NOT trip
    /// when `available` is a healthy 7+ GiB — verifying the wide margin between
    /// the healthy fleet empirical value and the 1 GiB floor.
    ///
    /// Mutation: change `FREE_FLOOR_BYTES` to 8 GiB → `free_floor_breached`
    /// returns true → assert fails with "threshold sanity: a healthy 7 GiB
    /// available must be far above FREE_FLOOR_BYTES".
    #[test]
    fn healthy_available_is_far_above_floor() {
        // Empirical healthy available from the 2026-06-28 fleet probe:
        // all three workers measured 7.2-8.1 GiB available. Use 7 GiB as
        // the conservative lower bound.
        let healthy_available_bytes: u64 = 7 * (1 << 30); // 7 GiB
        let clear_threshold = FREE_FLOOR_BYTES + FREE_FLOOR_HYSTERESIS; // 1.25 GiB
        assert!(
            healthy_available_bytes > clear_threshold,
            "threshold sanity: a healthy 7 GiB available ({healthy_available_bytes}) \
             must be far above FREE_FLOOR_BYTES + FREE_FLOOR_HYSTERESIS \
             ({clear_threshold}) — no false-trip risk on a healthy worker. \
             Mutation target: change FREE_FLOOR_BYTES to 8 GiB"
        );
        // Also assert via free_floor_breached directly.
        let tripped = free_floor_breached(healthy_available_bytes, false);
        assert!(
            !tripped,
            "threshold sanity: free_floor_breached must return false for healthy \
             available ({healthy_available_bytes} bytes = 7 GiB). \
             FREE_FLOOR_BYTES={FREE_FLOOR_BYTES}, FREE_FLOOR_HYSTERESIS={FREE_FLOOR_HYSTERESIS}"
        );
    }

    /// (#task-memgate-twosignal) The sustained-SWAPIN OOM disjunct: even when
    /// the free-floor is NOT breached, a sustained-swapin confirm trips the OOM
    /// gate (the OR logic). Binds to the SAME pure `memory_gate_verdict` the
    /// production sampler calls, proving the swapin signal is wired into the real
    /// verdict, not decorative.
    ///
    /// Mutation step (CLAUDE.md TDD #5): in `memory_gate_verdict`, drop the
    /// `|| swapin_sustained` disjunct (free-floor only). The swapin-confirm
    /// assertion below red-fails — and because `sample_mem_pressure` calls the
    /// same function, that mutation also breaks production.
    #[test]
    fn swapin_sustained_trips_gate_without_free_floor() {
        // Free-floor healthy, but sustained swapins (disk spill) ⇒ trip.
        assert!(
            memory_gate_verdict(true, false, true),
            "sustained swapins must trip the OOM gate even when the free-floor \
             is healthy: the OR logic catches the compression-hidden overcommit \
             case (macOS keeps `available` up by compressing) once it spills to disk"
        );
        // Free-floor breached, swapins healthy ⇒ trip (the fail-safe alone).
        assert!(
            memory_gate_verdict(true, true, false),
            "the free-floor fail-safe must trip the gate on its own (the belt+braces \
             last-ditch backstop)"
        );
        // Neither signal ⇒ no trip.
        assert!(
            !memory_gate_verdict(true, false, false),
            "neither signal tripped: a healthy worker must not gate"
        );
        // Disabled ⇒ never trip, regardless of signals.
        assert!(
            !memory_gate_verdict(false, true, true),
            "a disabled gate must never trip even with both signals active"
        );
    }

    /// (#task-memgate-twosignal, deliverable (a)) The SUSTAINED-WINDOW contract:
    /// a ONE-OFF swapin spike does NOT trip; only `window` CONSECUTIVE at/above-
    /// threshold ticks trip. Binds to the pure `swapin_sustained_step` the
    /// production sampler threads.
    ///
    /// Mutation step (CLAUDE.md TDD #5): in `swapin_sustained_step`, change
    /// `ticks >= window` to `ticks >= 1` (fire on the first tick / remove the
    /// window). The one-off-spike assertion below red-fails with its bespoke
    /// message — proving the sustained window is load-bearing.
    #[test]
    fn sustained_window_rejects_one_off_swapin_spike() {
        let threshold = 100u32;
        let window = 10u32;
        // A single tick above threshold from a cold run: NOT tripped (1 < 10).
        let (ticks_after_spike, tripped_after_spike) =
            swapin_sustained_step(0, 5_000, threshold, window);
        assert_eq!(ticks_after_spike, 1, "one above-threshold tick counts as 1");
        assert!(
            !tripped_after_spike,
            "a ONE-OFF swapin spike (1 tick above threshold, window=10) must NOT \
             trip the OOM gate — the sustained-window requirement rejects a lone \
             spike touching ancient swapped pages. If this trips, the window check \
             `ticks >= window` was weakened to `ticks >= 1`"
        );
        // A sub-threshold tick RESETS the run to 0 (breaks the streak).
        let (reset_ticks, reset_tripped) = swapin_sustained_step(9, 0, threshold, window);
        assert_eq!(reset_ticks, 0, "a sub-threshold tick resets the consecutive run");
        assert!(!reset_tripped, "a reset run cannot be tripped");
        // `window` consecutive at/above-threshold ticks DO trip on the last one.
        let mut ticks = 0u32;
        let mut tripped = false;
        for _ in 0..window {
            let step = swapin_sustained_step(ticks, threshold, threshold, window);
            ticks = step.0;
            tripped = step.1;
        }
        assert_eq!(ticks, window, "the run should reach exactly `window` ticks");
        assert!(
            tripped,
            "{window} CONSECUTIVE at/above-threshold ticks MUST trip the sustained \
             swapin OOM gate"
        );
    }

    /// (#task-memgate-twosignal, deliverable (b)) The compressor-churn perf
    /// scalar does NOT hard-NAK: a high-decompress / zero-swapin worker stays
    /// schedulable (the OOM boolean keys off SWAPINS + free-floor, never the
    /// churn scalar). Drives the REAL `sample_mem_pressure` through TWO ticks so
    /// the decompress rate is non-zero, then asserts `MEMORY_PRESSURED` is false
    /// (no swapins, free-floor healthy). Also proves `churn_scalar` is a `min`
    /// (decompress-only → low `min`).
    ///
    /// Mutation step: make `memory_gate_verdict`'s 3rd disjunct read the churn
    /// scalar instead of `swapin_sustained` → this test red-fails (churn would
    /// NAK).
    ///
    /// `#[serial(swap_sampler_atomics)]`: drives `sample_mem_pressure`, which
    /// stores the process-global sampler atomics.
    #[test]
    #[serial(swap_sampler_atomics)]
    fn decompress_churn_does_not_hard_nak() {
        // The `min` filters decompress-only pressure: compress low, decompress
        // high ⇒ churn low.
        assert!(
            (churn_scalar(3.0, 40_000.0) - 3.0).abs() < 1e-9,
            "churn_scalar must be min(compress, decompress) — decompress-alone \
             (compress low) yields a LOW churn, filtering the working-set-shift \
             confound"
        );
        // Pin the OTHER direction so a `min → compress_ewma` mutation cannot
        // escape: compress HIGH, decompress low ⇒ still LOW churn (proactive
        // cold-page reclaim confound filtered). Without this, returning
        // compress_ewma alone would survive the assertion above.
        assert!(
            (churn_scalar(40_000.0, 3.0) - 3.0).abs() < 1e-9,
            "churn_scalar must be min(compress, decompress) — compress-alone \
             (decompress low) yields a LOW churn, filtering the proactive \
             cold-page-reclaim confound"
        );
        // Enable the gate so a spurious NAK would show up in MEMORY_PRESSURED.
        MEMORY_GATE_ENABLED.store(true, Ordering::Relaxed);
        // On this (non-macOS/non-linux) build `read_memory_signals` returns None,
        // so drive the verdict through the pure path: high decompress, zero
        // swapin, healthy free-floor ⇒ NOT pressured.
        let (_ticks, swapin_sustained) = swapin_sustained_step(
            0,
            0, // zero swapin rate
            SWAPIN_CONFIRM_RATE.load(Ordering::Relaxed),
            SWAPIN_CONFIRM_WINDOW_TICKS.load(Ordering::Relaxed),
        );
        let free_tripped = free_floor_breached(8 << 30, false); // 8 GiB healthy
        assert!(
            !memory_gate_verdict(true, free_tripped, swapin_sustained),
            "a high-decompress / zero-swapin worker must NOT be gated: decompress \
             is a PERF signal (the graded churn scalar), never a hard NAK. Only \
             sustained swapins or the free-floor gate the OOM boolean"
        );
        MEMORY_GATE_ENABLED.store(false, Ordering::Relaxed);
    }

    /// (#task-memgate-twosignal, deliverable (g)) Config defaults: the NEW swapin
    /// OOM fields default to 100/s + 10 ticks and match the static inits; the
    /// DEPRECATED `memory_gate_refault_confirm_rate` field still defaults to 10000
    /// (retained for back-compat so a deployed config carrying it deserializes).
    ///
    /// Mutation (CLAUDE.md TDD #5):
    /// - Change `default_memory_gate_swapin_confirm_rate` to return a different value
    ///   → the `assert_eq!` fires with "swapin_confirm_rate config default must be 100".
    /// - Change `AtomicU32::new(100)` to a different value → the static-value assert fires.
    #[test]
    #[serial(swap_sampler_atomics)]
    fn swapin_confirm_config_defaults_match_statics() {
        use nativelink_config::cas_server::LocalWorkerConfig;
        // Minimal JSON5 — no memory_gate_* fields.
        let json5 = r#"{
            worker_api_endpoint: {uri: "grpc://localhost:50061"},
            cas_fast_slow_store: "cas_fast_slow",
            work_directory: "/tmp/work",
            platform_properties: {}
        }"#;
        let cfg: LocalWorkerConfig = serde_json5::from_str(json5).expect(
            "config parse must succeed for a valid minimal LocalWorkerConfig",
        );
        assert_eq!(
            cfg.memory_gate_swapin_confirm_rate.get(), 100,
            "swapin_confirm_rate config default must be 100/s (safety from swapin \
             semantics + sustained window; busy-worker baseline unmeasured). \
             Mutation target: change default_memory_gate_swapin_confirm_rate"
        );
        assert_eq!(
            cfg.memory_gate_swapin_confirm_window_ticks.get(), 10,
            "swapin_confirm_window_ticks config default must be 10 (≈1 s). \
             Mutation target: change default_memory_gate_swapin_confirm_window_ticks"
        );
        // The DEPRECATED refault field is retained for config back-compat (a
        // deployed worker.json5 still carries it); its default is unchanged.
        assert_eq!(
            cfg.memory_gate_refault_confirm_rate.get(), 10_000,
            "the DEPRECATED memory_gate_refault_confirm_rate must still default to \
             10000 so a deployed config carrying it deserializes (no-op, back-compat)"
        );
        // The runtime statics must match the config defaults (fresh process;
        // new_local_worker not called).
        assert_eq!(
            SWAPIN_CONFIRM_RATE.load(Ordering::Relaxed),
            100,
            "SWAPIN_CONFIRM_RATE static init must be 100 — must match the config \
             serde default so an absent field and a fresh process behave identically. \
             Mutation target: change AtomicU32::new(100)"
        );
        assert_eq!(
            SWAPIN_CONFIRM_WINDOW_TICKS.load(Ordering::Relaxed),
            10,
            "SWAPIN_CONFIRM_WINDOW_TICKS static init must be 10 — must match the \
             config serde default. Mutation target: change AtomicU32::new(10)"
        );
    }

    /// (#task-memgate-twosignal) The swapin OOM config fields reject `0`
    /// (`NonZeroU32`): a `0` rate would count every tick (rate >= 0 always) and a
    /// `0` window would trip on the first tick — both defeat the sustained-window
    /// design (the #64 storm class, self-inflicted).
    ///
    /// Mutation (CLAUDE.md TDD #5): revert either field type from `NonZeroU32` to
    /// `u32` → serde accepts `0` → this test's `is_err()` assertion fires.
    #[test]
    fn swapin_confirm_zero_rejected_by_serde() {
        use nativelink_config::cas_server::LocalWorkerConfig;
        let rate_zero = r#"{
            worker_api_endpoint: {uri: "grpc://localhost:50061"},
            cas_fast_slow_store: "cas_fast_slow",
            work_directory: "/tmp/work",
            platform_properties: {},
            memory_gate_swapin_confirm_rate: 0
        }"#;
        assert!(
            serde_json5::from_str::<LocalWorkerConfig>(rate_zero).is_err(),
            "memory_gate_swapin_confirm_rate: 0 must be rejected (NonZeroU32) — a \
             zero threshold counts every tick toward the window → a NAK storm."
        );
        let window_zero = r#"{
            worker_api_endpoint: {uri: "grpc://localhost:50061"},
            cas_fast_slow_store: "cas_fast_slow",
            work_directory: "/tmp/work",
            platform_properties: {},
            memory_gate_swapin_confirm_window_ticks: 0
        }"#;
        assert!(
            serde_json5::from_str::<LocalWorkerConfig>(window_zero).is_err(),
            "memory_gate_swapin_confirm_window_ticks: 0 must be rejected (NonZeroU32) \
             — a zero window trips on the first tick, defeating spike rejection."
        );
    }

    /// (#task-memgate-twosignal) Back-compat: a deployed config carrying the
    /// DEPRECATED `memory_gate_refault_confirm_rate` (the live fleet sets it to
    /// `4294967295`) still deserializes under `deny_unknown_fields`. If it were
    /// removed, the new binary would REJECT the live config → deploy break.
    #[test]
    fn deprecated_refault_field_still_deserializes() {
        use nativelink_config::cas_server::LocalWorkerConfig;
        let json5 = r#"{
            worker_api_endpoint: {uri: "grpc://localhost:50061"},
            cas_fast_slow_store: "cas_fast_slow",
            work_directory: "/tmp/work",
            platform_properties: {},
            memory_gate_refault_confirm_rate: 4294967295
        }"#;
        let cfg: LocalWorkerConfig = serde_json5::from_str(json5).expect(
            "the deployed config carrying the deprecated memory_gate_refault_confirm_rate \
             must still deserialize (retained field, deny_unknown_fields back-compat)",
        );
        assert_eq!(
            cfg.memory_gate_refault_confirm_rate.get(),
            u32::MAX,
            "the deprecated field round-trips (accepted, but no longer wired to any NAK)"
        );
    }

    /// (#task-memgate-twosignal, deliverable (a) integration) The PRODUCTION
    /// `sample_mem_pressure` publishes `MEMORY_GATE_TRIP_SWAPIN` from the
    /// sustained-swapin step — proving the swapin latch is wired to the real
    /// verdict, not decorative. Uses a degenerate threshold=0 / window=1 (stored
    /// DIRECTLY to the atomics, bypassing the config `NonZeroU32` guard) so a
    /// quiescent test box (real swapin rate ≈ 0) still exercises the sustained
    /// trip deterministically.
    ///
    /// `#[cfg(any(linux, macos))]`: only there does `read_memory_signals` return
    /// `Some` (the readable path that reaches the swapin step); the no-op fallback
    /// early-returns.
    ///
    /// Mutation (CLAUDE.md TDD #5): hard-code `MEMORY_GATE_TRIP_SWAPIN.store(false,…)`
    /// in `sample_mem_pressure` (or drop the `|| swapin_sustained` disjunct) → the
    /// assertion below red-fails.
    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[serial(swap_sampler_atomics)]
    fn sustained_swapin_publishes_trip_latch_through_production_sampler() {
        let orig_rate = SWAPIN_CONFIRM_RATE.load(Ordering::Relaxed);
        let orig_window = SWAPIN_CONFIRM_WINDOW_TICKS.load(Ordering::Relaxed);
        let orig_gate = MEMORY_GATE_ENABLED.load(Ordering::Relaxed);

        // Degenerate direct-atomic config: threshold 0 (every tick qualifies),
        // window 1 (one tick trips) — forces a deterministic sustained trip on a
        // quiescent box. Gate enabled so the latch reflects the verdict.
        SWAPIN_CONFIRM_RATE.store(0, Ordering::Relaxed);
        SWAPIN_CONFIRM_WINDOW_TICKS.store(1, Ordering::Relaxed);
        MEMORY_GATE_ENABLED.store(true, Ordering::Relaxed);

        let _ = sample_mem_pressure(SwapSamplerState::new());

        assert!(
            MEMORY_GATE_TRIP_SWAPIN.load(Ordering::Relaxed),
            "sample_mem_pressure must set MEMORY_GATE_TRIP_SWAPIN when the swapin \
             rate stays at/above threshold for the window: the production verdict \
             must thread swapin_sustained_step + publish the latch. If this fires, \
             the sampler ignores the swapin step or hard-codes the latch false."
        );

        // Restore.
        SWAPIN_CONFIRM_RATE.store(orig_rate, Ordering::Relaxed);
        SWAPIN_CONFIRM_WINDOW_TICKS.store(orig_window, Ordering::Relaxed);
        MEMORY_GATE_ENABLED.store(orig_gate, Ordering::Relaxed);
    }

    /// (#task-memgate-twosignal, pair-a fix-up) A REALISTIC swapin rate, computed
    /// by the PRODUCTION `compute_swap_pressure_rate`, must trip the sustained
    /// window at the PRODUCTION defaults (100 pages/s × 10 ticks) — and a
    /// sub-threshold rate must NOT. The sibling latch test uses a degenerate
    /// threshold=0 / window=1 (it proves the latch is WIRED); this proves a real
    /// rate actually crosses the rate-computation → window path with the shipped
    /// constants, and that benign non-spilling swapin stays quiet. Pure functions,
    /// so no atomics/`#[serial]`/platform gate needed.
    ///
    /// Mutation (CLAUDE.md TDD #5): change `swapin_sustained_step`'s `>= window`
    /// to `>= 1` → the "must NOT trip before 10 ticks" assertion red-fails; make
    /// `compute_swap_pressure_rate` ignore `elapsed_secs` → the 150/s and 50/s
    /// rate assertions red-fail.
    #[test]
    fn realistic_swapin_rate_trips_sustained_window_via_rate_computation() {
        let threshold = 100; // production SWAPIN_CONFIRM_RATE default
        let window = 10; // production SWAPIN_CONFIRM_WINDOW_TICKS default

        // A legit sustained spill: 150 swapins over a real 1.0 s interval → the
        // production rate-computation yields 150 pages/s (>= threshold).
        let hot_rate = compute_swap_pressure_rate(1_000_000, 1_000_150, 1.0);
        assert_eq!(
            hot_rate, 150,
            "compute_swap_pressure_rate must read 150 swapins over 1.0 s as 150/s \
             (the production Δcount/Δt the sampler feeds swapin_sustained_step)"
        );
        // The trip requires the FULL window of consecutive at/above-threshold
        // ticks — not fewer.
        let mut ticks = 0;
        for tick in 1..=window {
            let (next, tripped) = swapin_sustained_step(ticks, hot_rate, threshold, window);
            ticks = next;
            if tick < window {
                assert!(
                    !tripped,
                    "a sustained {hot_rate}/s swapin must NOT trip before {window} \
                     consecutive ticks (tripped at tick {tick})"
                );
            } else {
                assert!(
                    tripped,
                    "a sustained {hot_rate}/s swapin (>= {threshold}/s) MUST trip once \
                     {window} consecutive ticks accumulate"
                );
            }
        }

        // A sub-threshold rate (50/s < 100/s) must NEVER trip, no matter how long
        // it is sustained — the OOM gate stays quiet on benign, non-spilling churn.
        let cold_rate = compute_swap_pressure_rate(1_000_000, 1_000_050, 1.0);
        assert_eq!(
            cold_rate, 50,
            "50 swapins over 1.0 s must read as 50/s (sub-threshold control)"
        );
        let mut cold_ticks = 0;
        for _ in 0..(window * 3) {
            let (next, tripped) = swapin_sustained_step(cold_ticks, cold_rate, threshold, window);
            cold_ticks = next;
            assert!(
                !tripped,
                "a sub-threshold {cold_rate}/s swapin must NEVER trip the sustained \
                 OOM window (threshold {threshold}/s)"
            );
        }
        assert_eq!(
            cold_ticks, 0,
            "a sub-threshold swapin rate must RESET the consecutive-tick run each \
             tick (no slow accumulation toward a spurious trip)"
        );
    }

    /// (#37) The gate MUST fail OPEN when the sampler is wedged/dead: a
    /// STALE `LAST_SAMPLE_INSTANT` means UNKNOWN, and the gate must NOT
    /// report pressured (else a dead sampler wedges the worker into
    /// refusing all work forever — §3a rule 2 / §5 case 4). Drives the
    /// pure `swap_gate_verdict` with `enabled = true` so the age check is
    /// exercised regardless of the `MEMORY_GATE_ENABLED` ship flag.
    ///
    /// Mutation step (CLAUDE.md TDD #5): delete the
    /// `if age > max_age { return (false, true) }` arm in
    /// `swap_gate_verdict` (let a stale sample fall through to the
    /// pressured value). The STALE assertion below red-fails with its
    /// bespoke "stale swap sample must fail open" message.
    #[test]
    fn swap_gate_fails_open_on_stale_sample() {
        let max_age = Duration::from_secs(2);
        // FRESH + pressured ⇒ gate reports pressured (baseline).
        let (p, stale) = swap_gate_verdict(
            true,
            true,
            Duration::from_secs(10).as_nanos() as u64,
            Duration::from_secs(10) + Duration::from_millis(100),
            max_age,
        );
        assert!(p && !stale, "fresh pressured sample must gate (control)");

        // STALE + pressured ⇒ MUST fail open (not gate), and flag stale.
        let (p_stale, is_stale) = swap_gate_verdict(
            true,
            true, // the value atomic says "pressured"
            Duration::from_secs(1).as_nanos() as u64, // last sample at t=1s
            Duration::from_secs(10), // now at t=10s ⇒ age 9s > 2s max
            max_age,
        );
        assert!(
            !p_stale,
            "stale swap sample must fail open: a wedged/dead sampler must NOT \
             report pressured, or the worker refuses all work forever"
        );
        assert!(
            is_stale,
            "the stale path must be flagged so the wedged-sampler warn! fires"
        );

        // Never-sampled (0) ⇒ UNKNOWN ⇒ fail open.
        let (p0, _) = swap_gate_verdict(true, true, 0, Duration::from_secs(10), max_age);
        assert!(
            !p0,
            "never-sampled (LAST_SAMPLE_INSTANT == 0) must fail open (UNKNOWN)"
        );

        // Disabled ⇒ always fail open regardless of value/age.
        let (pd, _) = swap_gate_verdict(false, true, 1, Duration::from_secs(10), max_age);
        assert!(!pd, "disabled gate must always fail open");
    }

    /// (#37) The pure swap-gate decision: not-pressured accepts; pressured
    /// with in-flight work NAKs (the worker drains; `has_actions()` pause
    /// re-selects it); pressured+idle NAKs UNTIL the fail-open window, then
    /// fails open exactly once; a held latch keeps accepting (hysteresis).
    ///
    /// Mutation step: in `swap_gate_decision`, remove the
    /// `now.duration_since(since) >= SWAP_FAIL_OPEN_AFTER` arm (always
    /// return `Nak` for idle+pressured). The fail-open assertion red-fails
    /// with the bespoke wedge message — proving the time-bounded fail-open
    /// (3b) is load-bearing, not decorative.
    #[test]
    fn swap_gate_decision_admits_failopen_and_hysteresis() {
        let t0 = Instant::now();
        // Not pressured ⇒ Accept regardless of anything else.
        assert_eq!(
            swap_gate_decision(false, 0, false, None, t0),
            SwapGateDecision::Accept,
            "an unpressured worker must accept work"
        );
        // Pressured WITH in-flight work ⇒ Nak (drain; server re-selects).
        assert_eq!(
            swap_gate_decision(true, 2, false, None, t0),
            SwapGateDecision::Nak,
            "a pressured worker WITH in-flight work must NAK new work (it is \
             draining; the has_actions() pause handles re-selection)"
        );
        // Pressured + idle, BEFORE the fail-open window ⇒ Nak.
        assert_eq!(
            swap_gate_decision(true, 0, false, Some(t0), t0 + Duration::from_secs(1)),
            SwapGateDecision::Nak,
            "a pressured idle worker must NAK before the fail-open window"
        );
        // Pressured + idle, PAST the fail-open window ⇒ AcceptFailOpen.
        assert_eq!(
            swap_gate_decision(
                true,
                0,
                false,
                Some(t0),
                t0 + SWAP_FAIL_OPEN_AFTER + Duration::from_secs(1),
            ),
            SwapGateDecision::AcceptFailOpen,
            "composite invariant violated: a pressured idle worker past the \
             fail-open window must FAIL OPEN and accept one action, else an \
             all-idle all-pressured fleet wedges with no compensating fail-open"
        );
        // Hysteresis latch held ⇒ Accept (the fail-open action is still
        // draining; do not re-gate its follow-on work).
        assert_eq!(
            swap_gate_decision(true, 1, true, Some(t0), t0 + Duration::from_secs(1)),
            SwapGateDecision::Accept,
            "while the hysteresis latch holds, a pressured worker must keep \
             accepting so the accepted action makes monotonic progress"
        );
    }

    // ───────────────────────── F4 disk-pressure gate ─────────────────────────

    /// (F4) The PRIMARY disk-free floor trip is a LEVEL with a two-threshold
    /// hysteresis band (mirrors `free_floor_breached`): trip below
    /// `DISK_FREE_FLOOR_BYTES`, clear only once free recovers above
    /// `DISK_FREE_FLOOR_BYTES + DISK_FREE_FLOOR_HYSTERESIS`, hold the prior
    /// verdict in the band. Without the band a worker hovering at the floor
    /// flaps admit/refuse every sample tick.
    ///
    /// Mutation step (CLAUDE.md TDD #5): in `disk_floor_breached`, delete the
    /// `free_bytes >= FLOOR + HYSTERESIS` clear-arm (so any non-trip free
    /// value clears immediately). The in-band-hold assertion red-fails — a
    /// worker just above the floor would chatter.
    #[test]
    fn disk_floor_breached_has_hysteresis_band() {
        // Below the floor ⇒ trip regardless of prior state.
        assert!(
            disk_floor_breached(DISK_FREE_FLOOR_BYTES - 1, false),
            "free below the disk floor must trip the disk gate"
        );
        // Well above floor+hysteresis ⇒ clear regardless of prior state.
        assert!(
            !disk_floor_breached(
                DISK_FREE_FLOOR_BYTES + DISK_FREE_FLOOR_HYSTERESIS + 1,
                true
            ),
            "free well above floor+hysteresis must clear the disk gate"
        );
        // In the band, with prior TRIPPED ⇒ HOLD tripped (no chatter).
        assert!(
            disk_floor_breached(DISK_FREE_FLOOR_BYTES + 1, true),
            "in the hysteresis band the disk gate must HOLD its prior tripped \
             verdict, else a worker hovering at the floor flaps every tick"
        );
        // In the band, with prior CLEAR ⇒ HOLD clear.
        assert!(
            !disk_floor_breached(DISK_FREE_FLOOR_BYTES + 1, false),
            "in the hysteresis band the disk gate must HOLD its prior clear verdict"
        );
    }

    /// (F4) The sample-age verdict fails the disk gate's pressure state into
    /// UNKNOWN (`stale = true`) when the sampler is wedged/dead — but, unlike
    /// swap, UNKNOWN does NOT mean "admit". It means "the caller must consult
    /// the authoritative `statvfs` fallback" (SEC-2: disk has no other live
    /// bound behind the gate). This pure verdict only reports fresh/stale +
    /// the sampler value; the stale→statvfs resolution is
    /// `disk_effective_pressured`.
    ///
    /// Mutation step: delete the `age > max_age ⇒ (false, true)` arm in
    /// `disk_gate_verdict` (let a stale sample fall through to the live
    /// value). The STALE assertion red-fails — a wedged sampler would report
    /// its frozen value as authoritative instead of flagging stale, so the
    /// statvfs fallback would never run.
    #[test]
    fn disk_gate_verdict_flags_stale_for_statvfs_fallback() {
        let max_age = Duration::from_secs(2);
        // FRESH + pressured ⇒ pressured, not stale.
        let (p, stale) = disk_gate_verdict(
            true,
            true,
            Duration::from_secs(10).as_nanos() as u64,
            Duration::from_secs(10) + Duration::from_millis(100),
            max_age,
        );
        assert!(p && !stale, "a fresh pressured disk sample must gate (control)");
        // STALE ⇒ MUST flag stale so the caller runs the statvfs fallback.
        let (_, is_stale) = disk_gate_verdict(
            true,
            false,
            Duration::from_secs(1).as_nanos() as u64,
            Duration::from_secs(10),
            max_age,
        );
        assert!(
            is_stale,
            "a stale disk sample MUST flag stale so the caller consults the \
             authoritative statvfs fallback (disk has no other live bound — SEC-2)"
        );
        // Never-sampled (0) ⇒ also UNKNOWN/stale → statvfs fallback.
        let (_, stale0) = disk_gate_verdict(true, false, 0, Duration::from_secs(10), max_age);
        assert!(
            stale0,
            "a never-sampled disk gate must flag stale so the statvfs fallback runs"
        );
    }

    /// (F4) THE SEC-2 CONTRACT. On a stale/dead sampler the disk gate does
    /// NOT blind-accept (the swap gate's fail-open). It resolves an effective
    /// pressure from a one-shot authoritative `statvfs`: free below the floor
    /// ⇒ effective-pressured (the gate still rejects), free at/above ⇒ not
    /// pressured. Only when the statvfs itself fails (`None`) is there no
    /// measurement at all and the last-resort blind-accept applies.
    ///
    /// Mutation step: in `disk_effective_pressured`, change the stale branch
    /// to `=> false` (blind fail-open like swap). The "truly full" assertion
    /// red-fails — a stale sampler over a genuinely-full disk would admit work
    /// straight into the ENOSPC the gate exists to prevent.
    #[test]
    fn disk_effective_pressured_uses_statvfs_fallback_when_stale() {
        // FRESH sampler ⇒ trust the live sampler verdict, ignore statvfs.
        assert!(
            disk_effective_pressured(true, false, Some(u64::MAX)),
            "a FRESH pressured sampler must report pressured regardless of the \
             (unused) statvfs fallback value"
        );
        assert!(
            !disk_effective_pressured(false, false, None),
            "a FRESH unpressured sampler must report not-pressured"
        );
        // STALE + statvfs says TRULY FULL (below floor) ⇒ effective-pressured.
        assert!(
            disk_effective_pressured(false, true, Some(DISK_FREE_FLOOR_BYTES - 1)),
            "SEC-2: a STALE sampler over a genuinely-full disk (statvfs below \
             the floor) must report effective-pressured so the gate still \
             rejects — NOT blind-accept into ENOSPC"
        );
        // STALE + statvfs says HEALTHY (above floor) ⇒ not pressured.
        assert!(
            !disk_effective_pressured(false, true, Some(DISK_FREE_FLOOR_BYTES + 1)),
            "a STALE sampler over a healthy disk (statvfs above the floor) must \
             admit — the fallback measured headroom"
        );
        // STALE + statvfs ITSELF failed (None) ⇒ last-resort blind-accept
        // (no measurement available at all; mirrors the swap fail-open only
        // in this unmeasurable corner).
        assert!(
            !disk_effective_pressured(false, true, None),
            "a STALE sampler whose statvfs fallback ALSO failed has no \
             measurement; it must fail OPEN as the last resort (cannot wedge \
             the worker on an unmeasurable disk)"
        );
    }

    /// (F4) T1 (under-action) + T2 (over-action, asymmetric coverage): the
    /// pure disk-gate decision. Not-pressured ⇒ Accept (T2: healthy disk does
    /// NOT reject); pressured + in-flight ⇒ Nak (drain; the scheduler
    /// re-selects); pressured + idle ⇒ Nak until the fleet fail-open window,
    /// then AcceptFailOpen exactly once (so an all-pressured idle fleet cannot
    /// wedge); a held latch keeps accepting (hysteresis). Same shape as
    /// `swap_gate_decision`; the `effective_pressured` input already folds in
    /// the stale→statvfs resolution.
    ///
    /// Mutation step: remove the
    /// `now.duration_since(since) >= DISK_FAIL_OPEN_AFTER` arm in
    /// `disk_gate_decision` (always Nak for idle+pressured). The fleet
    /// fail-open assertion red-fails with the bespoke wedge message.
    #[test]
    fn disk_gate_decision_under_and_over_action() {
        let t0 = Instant::now();
        // T2 (OVER-action): a healthy/unpressured disk must NOT reject.
        assert_eq!(
            disk_gate_decision(false, 0, false, None, t0),
            DiskGateDecision::Accept,
            "asymmetric coverage: an unpressured (healthy-disk) worker must \
             ACCEPT work — the disk gate must not reject when there is headroom"
        );
        // T1 (under-action): pressured WITH in-flight work ⇒ Nak.
        assert_eq!(
            disk_gate_decision(true, 2, false, None, t0),
            DiskGateDecision::Nak,
            "a disk-pressured worker WITH in-flight work must NAK new work \
             (ResourceExhausted re-queue) so the scheduler re-queues it as \
             backpressure rather than ENOSPC'ing at make_action_directory"
        );
        // Pressured + idle, BEFORE the fail-open window ⇒ Nak.
        assert_eq!(
            disk_gate_decision(true, 0, false, Some(t0), t0 + Duration::from_secs(1)),
            DiskGateDecision::Nak,
            "a disk-pressured idle worker must NAK before the fleet fail-open window"
        );
        // Pressured + idle, PAST the window ⇒ AcceptFailOpen.
        assert_eq!(
            disk_gate_decision(
                true,
                0,
                false,
                Some(t0),
                t0 + DISK_FAIL_OPEN_AFTER + Duration::from_secs(1),
            ),
            DiskGateDecision::AcceptFailOpen,
            "composite invariant: a disk-pressured idle worker past the \
             fail-open window must FAIL OPEN once, else an all-idle \
             all-disk-pressured fleet wedges with no compensating corner"
        );
        // Hysteresis latch held ⇒ Accept (the fail-open action is draining).
        assert_eq!(
            disk_gate_decision(true, 1, true, Some(t0), t0 + Duration::from_secs(1)),
            DiskGateDecision::Accept,
            "while the disk hysteresis latch holds, a pressured worker must \
             keep accepting so the accepted action makes monotonic progress"
        );
    }

    /// (F4) The disk threshold is a `pub const`; the wire/gate behaviour binds
    /// to its declared value. Pins the numeric-constant (reviewer-dispatch
    /// numeric-constant block) so a doc-comment edit cannot silently drift the
    /// floor away from the asserted value.
    #[test]
    fn disk_free_floor_is_the_documented_threshold() {
        assert_eq!(
            DISK_FREE_FLOOR_BYTES,
            8 * (1 << 30),
            "the disk-free admission floor must be 8 GiB — the value the gate, \
             the heartbeat verdict, and the soak-tuning note are all written \
             against"
        );
    }

    /// (F4) The heartbeat reads disk pressure via the process-global sampler
    /// atomics (`get_available_disk_bytes` / `disk_gate_sampler_pressured`),
    /// exactly like the swap fields. Fakes a sampler tick by storing into the
    /// atomics and asserts the readers observe it — the path the periodic +
    /// post-action heartbeat build sites use.
    ///
    /// Mutation step: change `get_available_disk_bytes` to read
    /// `SWAP_USED_BYTES` (wrong static). This test red-fails because the faked
    /// disk value is not observed.
    ///
    /// `#[serial(disk_sampler_atomics)]`: this test mutates the process-global
    /// `DISK_FREE_BYTES` / `DISK_PRESSURED` statics; serialized so the other
    /// disk-atomic test cannot race the read.
    #[test]
    #[serial(disk_sampler_atomics)]
    fn heartbeat_reads_disk_pressure_from_sampler_atomics() {
        DISK_FREE_BYTES.store(42_949_672_960, Ordering::Relaxed); // 40 GiB
        DISK_PRESSURED.store(true, Ordering::Relaxed);
        assert_eq!(
            get_available_disk_bytes(),
            42_949_672_960,
            "heartbeat must read available_disk_bytes from the DISK_FREE_BYTES \
             sampler atomic"
        );
        assert!(
            disk_gate_sampler_pressured(),
            "heartbeat must read disk_pressured from the DISK_PRESSURED sampler \
             atomic (gated on DISK_GATE_ENABLED)"
        );
        // The never-sampled sentinel maps to 0 on the wire (conservative for
        // the server's most-free ranking).
        DISK_FREE_BYTES.store(u64::MAX, Ordering::Relaxed);
        assert_eq!(
            get_available_disk_bytes(),
            0,
            "the never-sampled u64::MAX sentinel must map to 0 on the wire so an \
             un-sampled worker ranks as fully-pressured in the fail-open"
        );
    }

    /// (F4) `sample_disk_pressure` MUST publish the monotonic
    /// `LAST_DISK_SAMPLE_INSTANT` liveness anchor on every tick (BEFORE the
    /// statvfs can fail), so the gate's stale→statvfs-fallback decision sees a
    /// live sampler. Guards the invariant that a sampler that IS ticking does
    /// not route to the fallback, while one that STOPS ticking does.
    ///
    /// `#[serial(disk_sampler_atomics)]`: shares the process-global
    /// `LAST_DISK_SAMPLE_INSTANT` / `DISK_PRESSURED` statics with the other
    /// disk-atomic test; serialized so a concurrent store cannot race the read.
    #[test]
    #[serial(disk_sampler_atomics)]
    fn sample_disk_pressure_publishes_live_anchor() {
        // Initialize PROCESS_START so the tick's `now` is strictly after it.
        let start = *PROCESS_START;
        while Instant::now() <= start {
            core::hint::spin_loop();
        }
        LAST_DISK_SAMPLE_INSTANT.store(0, Ordering::Relaxed);
        // No DISK_SAMPLE_PATH configured in the unit-test process → the tick
        // takes the no-path early-return, but it MUST still publish the anchor
        // first (the liveness guarantee is independent of the statvfs result).
        let _tripped = sample_disk_pressure(false);
        let after = LAST_DISK_SAMPLE_INSTANT.load(Ordering::Relaxed);
        assert!(
            after > 0,
            "sample_disk_pressure must publish a fresh strictly-positive \
             monotonic LAST_DISK_SAMPLE_INSTANT anchor on every tick (once \
             PROCESS_START is initialized) so the gate's stale→statvfs-fallback \
             decision sees a live sampler; anchor was {after}"
        );
    }

    /// (FL-688 v3 Stage C — over-cap metric render test)
    ///
    /// Verifies that `Metrics::reconcile_gate_fail_open_total` is wired into the
    /// `MetricsComponent` publish tree and emits its literal field name when the
    /// metric exporter walks the tree. Without this, operators alerting on the
    /// rolling-deploy fail-open event (RECONCILE_FAIL_OPEN_SECS = 20s window) see
    /// nothing — the field is dark.
    ///
    /// Mutation: comment out the `#[metric(help = "...")]` attribute on
    /// `reconcile_gate_fail_open_total` in the `Metrics` struct → this test must
    /// red-fail with "expected metric `reconcile_gate_fail_open_total` to be
    /// published".
    #[test]
    fn reconcile_gate_fail_open_total_visible_in_metric_tree() {
        use std::sync::Mutex;

        use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Debug, Default, Clone)]
        struct CapturedMetric {
            name: String,
            value: String,
        }

        #[derive(Default)]
        struct MetricCaptureLayer {
            events: Arc<Mutex<Vec<CapturedMetric>>>,
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for MetricCaptureLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if event.metadata().target() != "nativelink_metric" {
                    return;
                }
                struct Grabber {
                    name: String,
                    value: String,
                }
                impl tracing::field::Visit for Grabber {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn core::fmt::Debug,
                    ) {
                        let s = format!("{value:?}").trim_matches('"').to_string();
                        match field.name() {
                            "__name" => self.name = s,
                            "__value" => self.value = s,
                            _ => {}
                        }
                    }
                }
                let mut g = Grabber {
                    name: String::new(),
                    value: String::new(),
                };
                event.record(&mut g);
                if g.name.is_empty() {
                    return;
                }
                self.events.lock().unwrap().push(CapturedMetric {
                    name: g.name,
                    value: g.value,
                });
            }
        }

        let layer = MetricCaptureLayer::default();
        let captured = layer.events.clone();
        let subscriber = tracing_subscriber::Registry::default().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // Construct Metrics directly (same-module access to private constructor).
        let metrics = Metrics::new(
            std::sync::Weak::new(),
            Arc::new(core::sync::atomic::AtomicI64::new(0)),
        );
        // Drive one fail-open event as the production code does.
        metrics.reconcile_gate_fail_open_total.inc();

        MetricsComponent::publish(
            &metrics,
            MetricKind::Component,
            MetricFieldData::default(),
        )
        .expect("publish must succeed for derived MetricsComponent");

        drop(_guard);

        let events = captured.lock().unwrap().clone();
        let metric = events
            .iter()
            .find(|m| m.name == "reconcile_gate_fail_open_total")
            .unwrap_or_else(|| {
                panic!(
                    "expected metric `reconcile_gate_fail_open_total` to be published \
                     when Metrics::publish() walks the tree. Captured events: {events:#?}. \
                     Without #[metric(help = \"...\")] on the field, the counter stays \
                     invisible to operators alerting on rolling-deploy fail-open events \
                     (FL-688 v3 Stage C over-cap metric)."
                )
            });
        assert_eq!(
            metric.value, "1",
            "expected counter value 1 to flow through the publish chain — got {:?}. \
             If publish() returned Component without emitting Counter, the derive \
             output is silently mis-routing the field.",
            metric.value
        );
    }

    /// (FL-681 NAK boundary fix) The worker admission-NAK counter must render on
    /// the process metric tree under the literal name
    /// `worker_admission_nak_pin_saturated_total` — the signal that proves the
    /// pin-cap-saturation NAK gate is actually firing (its absence was the
    /// FL-681 incident's blind spot). Proves:
    ///   1. `worker_admission_nak_counters()` returns the same singleton the NAK
    ///      site in `running_actions_manager.rs::create_and_add_action` writes.
    ///   2. `worker_admission_nak_counters_arc()` (which `nativelink.rs` passes to
    ///      `MetricsRegistry::register`) publishes that literal field name.
    ///   3. The published value reflects the increment done via the singleton.
    ///
    /// Mutation (CLAUDE.md TDD #5): comment out
    /// `publish!("nak_pin_saturated_total", ...)` in
    /// `WorkerAdmissionNakCounters::publish` → the `unwrap_or_else` panic fires
    /// with the bespoke "the pin-cap-saturation NAK signal is dark on /metrics"
    /// message; or `record_nak_pin_saturated` → the delta assertion fails.
    #[test]
    fn worker_admission_nak_counter_visible_in_metric_tree() {
        use std::sync::Mutex;

        use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent};
        use nativelink_util::o11_probes::{
            worker_admission_nak_counters, worker_admission_nak_counters_arc,
        };
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Debug, Default, Clone)]
        struct CapturedMetric {
            name: String,
            value: String,
        }

        #[derive(Default)]
        struct MetricCaptureLayer {
            events: Arc<Mutex<Vec<CapturedMetric>>>,
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for MetricCaptureLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if event.metadata().target() != "nativelink_metric" {
                    return;
                }
                struct Grabber {
                    name: String,
                    value: String,
                }
                impl tracing::field::Visit for Grabber {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn core::fmt::Debug,
                    ) {
                        let s = format!("{value:?}").trim_matches('"').to_string();
                        match field.name() {
                            "__name" => self.name = s,
                            "__value" => self.value = s,
                            _ => {}
                        }
                    }
                }
                let mut g = Grabber {
                    name: String::new(),
                    value: String::new(),
                };
                event.record(&mut g);
                if g.name.is_empty() {
                    return;
                }
                self.events.lock().unwrap().push(CapturedMetric {
                    name: g.name,
                    value: g.value,
                });
            }
        }

        // Baseline BEFORE increment — the singleton is process-wide, shared
        // across all tests in the suite; a delta assertion avoids ordering
        // sensitivity with any parallel test that also NAKs.
        let before = worker_admission_nak_counters()
            .nak_pin_saturated
            .load(::core::sync::atomic::Ordering::Relaxed);

        // Drive one NAK via the PRODUCTION singleton path (the same
        // `record_nak_pin_saturated` the NAK site calls).
        worker_admission_nak_counters().record_nak_pin_saturated();

        let layer = MetricCaptureLayer::default();
        let captured = layer.events.clone();
        let subscriber = tracing_subscriber::Registry::default().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // `worker_admission_nak_counters_arc()` is what nativelink.rs passes to
        // `MetricsRegistry::register` — publish via that handle.
        MetricsComponent::publish(
            worker_admission_nak_counters_arc().as_ref(),
            MetricKind::Component,
            MetricFieldData::default(),
        )
        .expect("publish must succeed for WorkerAdmissionNakCountersHandle");

        drop(_guard);

        let events = captured.lock().unwrap().clone();

        // The rendered name is the BARE `nak_pin_saturated_total` at the metric
        // event (the "worker_admission" prefix is applied by the registry key at
        // render time; the doubled-prefix guard below pins that the name is not
        // `worker_admission_worker_admission_*`).
        let metric = events
            .iter()
            .find(|m| m.name == "nak_pin_saturated_total")
            .unwrap_or_else(|| {
                panic!(
                    "expected metric `nak_pin_saturated_total` (rendered \
                     worker_admission_nak_pin_saturated_total) to be published by \
                     WorkerAdmissionNakCountersHandle — the pin-cap-saturation NAK signal is \
                     dark on /metrics; the FL-681 dead-gate incident cannot be distinguished \
                     from a healthy pin budget. Captured events: {events:#?}. Mutation target: \
                     comment out publish!(\"nak_pin_saturated_total\", ...) in \
                     WorkerAdmissionNakCounters::publish."
                )
            });
        let published: u64 = metric
            .value
            .parse()
            .expect("nak_pin_saturated_total value must be numeric");
        assert_eq!(
            published,
            before + 1,
            "nak_pin_saturated_total must equal baseline+1 after one record_nak_pin_saturated — \
             got {published} (baseline was {before}). Singleton aliasing broken: \
             worker_admission_nak_counters() and worker_admission_nak_counters_arc() are not \
             observing the same AtomicU64."
        );

        // Doubled-prefix trap guard (as the memory_gate / dir_cache tests do):
        // the field name at the event must NOT already carry the registry prefix.
        assert!(
            !metric.name.contains("worker_admission"),
            "doubled metric name: the published field name `{}` already contains the registry \
             prefix `worker_admission` — the rendered name would be \
             worker_admission_worker_admission_nak_pin_saturated_total (do not group!() inside \
             WorkerAdmissionNakCounters::publish; the registry key already scopes it).",
            metric.name
        );
    }

    /// (#37 re-enable follow-up) Singleton-aliasing test — `memory_gate_counters()`
    /// and `memory_gate_counters_arc()` must observe the same underlying `AtomicU64`
    /// state, and `MemoryGateCountersHandle::publish` must emit the literal metric
    /// names operators alert on: `memory_gate_nak_free_floor_total` and
    /// `memory_gate_nak_swapin_total`.
    ///
    /// The counters were previously per-instance `Metrics` fields (never registered
    /// with `MetricsRegistry` — the worker-metrics-exposure trap) and are now a
    /// process-singleton in `o11_probes`. This test proves:
    ///   1. `memory_gate_counters()` returns the same singleton the NAK path writes.
    ///   2. `memory_gate_counters_arc()` (which `nativelink.rs` passes to
    ///      `MetricsRegistry::register`) publishes the literal field names.
    ///   3. The published value reflects the increment done via the singleton.
    ///
    /// Mutation (CLAUDE.md TDD #5): comment out the `publish!(\"nak_free_floor_total\", ...)`
    /// call in `MemoryGateCounters::publish` → the `unwrap_or_else` panic fires with
    /// "expected metric memory_gate_nak_free_floor_total to be published — canary soak
    /// cannot read trip-source signal". Same for `nak_swapin_total`.
    ///
    /// `#[serial(swap_sampler_atomics)]`: reads back the process-global
    /// `memory_gate_counters()` singleton (swap_used_bytes / pressure_level_mib
    /// sentinels). The sampler-driving tests (`sample_mem_pressure`) unconditionally
    /// overwrite that singleton, so this must serialize with them.
    #[test]
    #[serial(swap_sampler_atomics)]
    fn memory_gate_nak_counters_visible_in_metric_tree() {
        use std::sync::Mutex;

        use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent};
        use nativelink_util::o11_probes::{memory_gate_counters, memory_gate_counters_arc};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Debug, Default, Clone)]
        struct CapturedMetric {
            name: String,
            value: String,
        }

        #[derive(Default)]
        struct MetricCaptureLayer {
            events: Arc<Mutex<Vec<CapturedMetric>>>,
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for MetricCaptureLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if event.metadata().target() != "nativelink_metric" {
                    return;
                }
                struct Grabber {
                    name: String,
                    value: String,
                }
                impl tracing::field::Visit for Grabber {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn core::fmt::Debug,
                    ) {
                        let s = format!("{value:?}").trim_matches('"').to_string();
                        match field.name() {
                            "__name" => self.name = s,
                            "__value" => self.value = s,
                            _ => {}
                        }
                    }
                }
                let mut g = Grabber {
                    name: String::new(),
                    value: String::new(),
                };
                event.record(&mut g);
                if g.name.is_empty() {
                    return;
                }
                self.events.lock().unwrap().push(CapturedMetric {
                    name: g.name,
                    value: g.value,
                });
            }
        }

        // Read the baseline values BEFORE incrementing so parallel tests don't
        // interfere (the singleton is process-wide, shared across all tests in
        // the suite — delta-based assertion avoids ordering sensitivity).
        let before_free_floor = memory_gate_counters()
            .nak_free_floor
            .load(::core::sync::atomic::Ordering::Relaxed);
        let before_swapin = memory_gate_counters()
            .nak_swapin
            .load(::core::sync::atomic::Ordering::Relaxed);

        // Drive one NAK on each trip-source via the PRODUCTION singleton path.
        memory_gate_counters()
            .nak_free_floor
            .fetch_add(1, ::core::sync::atomic::Ordering::Relaxed);
        memory_gate_counters()
            .nak_swapin
            .fetch_add(1, ::core::sync::atomic::Ordering::Relaxed);

        // (#64 dark-signals) Drive the two new gauge fields through the SAME
        // production singleton the sampler writes to in `sample_mem_pressure`.
        // These are gauges (store, not fetch_add) — distinct sentinels so the
        // publish path proves the field-to-name wiring (not a stale 0). The
        // sampler thread does not run in unit tests, so no concurrent writer
        // overwrites these between the store and the publish below.
        const SWAP_SENTINEL: u64 = 8_123_456_789;
        const PRESSURE_SENTINEL: u32 = 4242;
        memory_gate_counters()
            .swap_used_bytes
            .store(SWAP_SENTINEL, ::core::sync::atomic::Ordering::Relaxed);
        memory_gate_counters()
            .pressure_level_mib
            .store(PRESSURE_SENTINEL, ::core::sync::atomic::Ordering::Relaxed);

        let layer = MetricCaptureLayer::default();
        let captured = layer.events.clone();
        let subscriber = tracing_subscriber::Registry::default().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // `memory_gate_counters_arc()` is what nativelink.rs passes to
        // `MetricsRegistry::register` — publish via that handle.
        MetricsComponent::publish(
            memory_gate_counters_arc().as_ref(),
            MetricKind::Component,
            MetricFieldData::default(),
        )
        .expect("publish must succeed for MemoryGateCountersHandle");

        drop(_guard);

        let events = captured.lock().unwrap().clone();

        let free_floor_metric = events
            .iter()
            .find(|m| m.name == "nak_free_floor_total")
            .unwrap_or_else(|| {
                panic!(
                    "expected metric `nak_free_floor_total` to be published by \
                     MemoryGateCountersHandle — canary soak cannot read free-floor trip \
                     signal. Captured events: {events:#?}. \
                     Mutation target: comment out publish!(\"nak_free_floor_total\", ...) \
                     in MemoryGateCounters::publish (#37 re-enable follow-up)"
                )
            });
        let published_free_floor: u64 = free_floor_metric
            .value
            .parse()
            .expect("nak_free_floor_total value must be numeric");
        assert_eq!(
            published_free_floor,
            before_free_floor + 1,
            "nak_free_floor_total must equal baseline+1 after one fetch_add — \
             got {published_free_floor} (baseline was {before_free_floor}). \
             Singleton aliasing broken: memory_gate_counters() and \
             memory_gate_counters_arc() are not observing the same AtomicU64."
        );

        let swapin_metric = events
            .iter()
            .find(|m| m.name == "nak_swapin_total")
            .unwrap_or_else(|| {
                panic!(
                    "expected metric `nak_swapin_total` to be published by \
                     MemoryGateCountersHandle — soak cannot read the swapin trip \
                     signal. Captured events: {events:#?}. \
                     Mutation target: comment out publish!(\"nak_swapin_total\", ...) \
                     in MemoryGateCounters::publish (#task-memgate-twosignal)"
                )
            });
        let published_swapin: u64 = swapin_metric
            .value
            .parse()
            .expect("nak_swapin_total value must be numeric");
        assert_eq!(
            published_swapin,
            before_swapin + 1,
            "nak_swapin_total must equal baseline+1 after one fetch_add — \
             got {published_swapin} (baseline was {before_swapin}). \
             Singleton aliasing broken: memory_gate_counters() and \
             memory_gate_counters_arc() are not observing the same AtomicU64."
        );

        // (#64 dark-signals) The two new gauges must reach /metrics through the
        // SAME MemoryGateCountersHandle path the sampler-written singleton feeds.
        // Proves the singleton-aliasing for swap_used_bytes / pressure_level_mib
        // (the o11_probes render tests use a fresh Arc<MemoryGateCounters>, which
        // does NOT exercise the static-backed handle delegation the worker uses).
        let swap_metric = events
            .iter()
            .find(|m| m.name == "swap_used_bytes")
            .unwrap_or_else(|| {
                panic!(
                    "expected metric `swap_used_bytes` to be published by \
                     MemoryGateCountersHandle — the sampler-written swap gauge is \
                     dark on /metrics. Captured events: {events:#?}. \
                     Mutation target: comment out publish!(\"swap_used_bytes\", ...) \
                     in MemoryGateCounters::publish (#64 dark-signals)"
                )
            });
        let published_swap: u64 = swap_metric
            .value
            .parse()
            .expect("swap_used_bytes value must be numeric");
        assert_eq!(
            published_swap, SWAP_SENTINEL,
            "swap_used_bytes must equal the sentinel stored via the singleton — \
             got {published_swap}, expected {SWAP_SENTINEL}. Singleton aliasing \
             broken: the sampler's memory_gate_counters().swap_used_bytes.store() \
             is not observed through memory_gate_counters_arc()'s publish."
        );

        let pressure_metric = events
            .iter()
            .find(|m| m.name == "pressure_level_mib")
            .unwrap_or_else(|| {
                panic!(
                    "expected metric `pressure_level_mib` to be published by \
                     MemoryGateCountersHandle — the sampler-written pressure gauge is \
                     dark on /metrics. Captured events: {events:#?}. \
                     Mutation target: comment out publish!(\"pressure_level_mib\", ...) \
                     in MemoryGateCounters::publish (#64 dark-signals)"
                )
            });
        let published_pressure: u32 = pressure_metric
            .value
            .parse()
            .expect("pressure_level_mib value must be numeric");
        assert_eq!(
            published_pressure, PRESSURE_SENTINEL,
            "pressure_level_mib must equal the sentinel stored via the singleton — \
             got {published_pressure}, expected {PRESSURE_SENTINEL}. Singleton \
             aliasing broken: the sampler's \
             memory_gate_counters().pressure_level_mib.store() is not observed \
             through memory_gate_counters_arc()'s publish."
        );
    }
}

#[cfg(test)]
mod actions_notify_subscribe_before_predicate_tests {
    //! Regression test for #95: lost-wakeup window in the
    //! shutdown-drain loop in the worker loop body
    //! (`local_worker.rs` near line 2491-2493).
    //!
    //! The pre-fix loop loaded `actions_in_flight` and then awaited
    //! `actions_notify.notified()`. Subscribe-after-predicate has the
    //! standard lost-wakeup race: a producer fired between the load
    //! and the await would be missed.
    //!
    //! The producer (running-action completion sites) uses
    //! `notify_one()` which DOES store one permit, so today's
    //! immediate symptom is at-most-one-extra loop iteration during
    //! shutdown drain rather than a hard deadlock. This test
    //! documents the contract — defense-in-depth for any future
    //! producer change toward broadcast-style `notify_waiters`. Same
    //! shape and rationale as the cleanup_wait_notify_parity_tests
    //! reference in `running_actions_manager.rs:5715-5807` and the
    //! `fetched_notify_subscribe_before_predicate_tests` for #92.
    //!
    //! NOTE on test discipline (CLAUDE.md
    //! `feedback_lost_wakeup_test_theatre`): we use a
    //! `tokio::sync::Barrier` for deterministic ordering and a
    //! `tokio::time::timeout` deadlock detector — NEVER `sleep` as
    //! synchronization.
    use core::time::Duration;
    use std::sync::Arc;

    use tokio::sync::{Barrier, Notify};

    /// Subscribe-before-predicate (the fix at #95): the Notified
    /// future is constructed BEFORE the predicate window, so a
    /// `notify_waiters` issued during that window is delivered. With
    /// the contract violated (subscribe AFTER predicate), the
    /// `notify_waiters` evaporates and the await blocks forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_before_predicate_captures_wakeup() {
        let notify = Arc::new(Notify::new());
        let barrier = Arc::new(Barrier::new(2));

        // Producer: wait at the barrier, then fire notify_waiters().
        // notify_waiters() does NOT store a permit — it only wakes
        // waiters currently registered.
        let prod_notify = notify.clone();
        let prod_barrier = barrier.clone();
        tokio::spawn(async move {
            prod_barrier.wait().await;
            prod_notify.notify_waiters();
        });

        // Consumer mirrors the production loop body shape (#95 fix):
        // subscribe FIRST, enable, then enter the predicate window.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Predicate window: release the producer to fire its
        // notify_waiters(). The barrier acts as a happens-before
        // synchronization point: the producer's notify is emitted
        // strictly after this point, while we are still in the
        // predicate window — strictly before we reach the await
        // below.
        barrier.wait().await;

        // Await: the pre-enabled Notified must observe the wakeup
        // issued during the predicate window. 2s real-wall-clock is
        // a deadlock detector, NOT synchronization.
        tokio::time::timeout(Duration::from_secs(2), notified.as_mut())
            .await
            .expect(
                "lost-wakeup race — must subscribe before predicate (#95): \
                 notify_waiters() fired during the predicate window was lost",
            );
    }
}
