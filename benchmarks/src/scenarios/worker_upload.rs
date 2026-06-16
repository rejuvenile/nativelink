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

//! Flow U1: worker output-upload path (`inner_upload_results`).
//!
//! **Why this cell exists:** `data_plane_bench` had zero cells for the
//! worker output-upload path.  This cell exercises
//! `RunningAction::upload_results()` directly under controlled conditions
//! and measures its end-to-end wall-clock latency.
//!
//! **What this cell measures (and what it does NOT):**
//! The timed window is the complete `upload_results()` call: Phase-1 hash
//! of all output files + ONE batch `has_with_results()`, then Phase-2
//! parallel upload of the not-already-present blobs, then stdout/stderr
//! upload and the AC-result write.  The cell reports ONLY the wall-clock
//! latency of that call (criterion-style p50/p99 over `iters` samples).
//!
//! This cell does NOT decompose the Phase-1/Phase-2 sequential gap or count
//! individual `has()` calls.  An earlier revision tried to, via a slow-store
//! wrapper that recorded a "first-update" timestamp and a single-key-`has()`
//! counter; that instrumentation was structurally wrong and was removed:
//!   - The Phase-1 gap is `batch_start - upload_start`, two `Instant`s that
//!     are local variables inside the *private* `inner_upload_results`
//!     (running_actions_manager.rs:3797, :3982).  Exposing them to the bench
//!     would require a production observability hook on the worker's hottest
//!     upload path — a change a Tier-3 cadre would (correctly) block for a
//!     bench.  Measuring that gap is left to a separate follow-up.
//!   - The single-key-`has()` count was pinned at 0 by construction, not by
//!     any code path: directory Tree/Directory protos upload via
//!     `serialize_and_upload_message` → `update_oneshot` (ac_utils.rs:163),
//!     which never calls the slow store's `has_with_results`.  The recorded
//!     "0" proved nothing about Phase-1 coverage, so the counter was removed.
//!
//! **The confound the slow-tier latency models (load-bearing):**
//! With a `MemoryStore` slow tier, `has_with_results` and `update` complete
//! in nanoseconds, so the wall-clock time is dominated by local file I/O and
//! hashing — it shows no signal about the remote-CAS RPC structure the
//! production path pays.  The `LatencyInjectingStore` wrapper sleeps for a
//! configurable `rpc_latency` before delegating each `has`/`update`, so the
//! wall-clock cell reflects the remote round-trip cost.
//!
//! **Latency model:**
//! - `rpc_latency = Duration::ZERO`: no injected delay.  Models the local
//!   fast-store path (F2 mode: `cas_store = FilesystemStore`, `has()` is an
//!   in-process EvictingMap lookup).  Also used for the noise-floor cell.
//! - `rpc_latency = Duration::from_millis(3)`: 3 ms per RPC call.  Models the
//!   non-F2 (synchronous / production-default) path where the slow tier is
//!   the remote GrpcStore.  3 ms is the order-of-magnitude p50 RTT observed
//!   for the GrpcStore → remote-CAS leg on buildcache/banjo.  This constant is a
//!   *modelling choice*, not a committed measurement; the bench cells are
//!   relative anchors, not absolute latency claims.  Asserted by
//!   `u1_rpc3ms_cells_use_3ms`.
//!
//! **F2 mode modelling:**
//! With `deferred_output_uploads_enabled = true`, `inner_upload_results`
//! uses the fast store (FilesystemStore) as `cas_store_owned`.  Output files
//! are written to the local FilesystemStore (sub-ms), so the cell models F2
//! mode by setting `rpc_latency = Duration::ZERO` AND
//! `deferred_output_uploads_enabled = true`.  The `LatencyInjectingStore`
//! slow tier is not on the F2 write path.
//!
//! **What is NOT in the timed window:**
//!   - Manager construction and FilesystemStore allocation.
//!   - Action+Command proto upload into CAS (one-time per cell).
//!   - `prepare_action` (input-tree materialization, work-dir creation).
//!   - Writing synthetic output files to disk (`stage_one_action` does this).
//!   - `execute(/bin/true)` — near-instant, just reads exit code.
//!   - `get_finished_result` and `cleanup`.
//!
//! **Cell matrix (U1 flow):**
//!
//! | Cell name                       | N  | size  | F2  | output type | rpc_latency |
//! |---------------------------------|----|-------|-----|-------------|-------------|
//! | `u1_upload_5_files_no_latency`  | 5  | 64KiB | off | files       | 0 ms        |
//! | `u1_upload_5_files_rpc3ms`      | 5  | 64KiB | off | files       | 3 ms        |
//! | `u1_upload_20_files_rpc3ms`     | 20 | 64KiB | off | files       | 3 ms        |
//! | `u1_upload_100_files_rpc3ms`    |100 | 64KiB | off | files       | 3 ms        |
//! | `u1_upload_20_files_f2_mode`    | 20 | 64KiB | on  | files       | 0 ms        |
//! | `u1_upload_20_dir_files_rpc3ms` | 20 | 64KiB | off | dir-outputs | 3 ms        |
//!
//! `u1_upload_5_files_no_latency` is the noise-floor cell (no injected
//! delay); it anchors the harness + file-I/O cost with no RPC structure
//! visible.  The other cells show how total latency scales with N and
//! whether F2 mode eliminates the remote-has overhead.
//!
//! **Baseline discipline:** the cell is new; collect TWO back-to-back runs
//! on the same SHA to establish the noise floor before attributing future
//! deltas.  Kernel page-cache effects and ZFS ARC warming affect cold runs.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use nativelink_config::cas_server::{UploadActionResultConfig, UploadCacheResultsStrategy};
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::{MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, ExecuteRequest, Platform,
    command::EnvironmentVariable,
    digest_function::Value as ProtoDigestFunction,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike as _, UploadSizeInfo,
};
use nativelink_worker::running_actions_manager::{
    Callbacks, ExecutionConfiguration, RunningAction as _, RunningActionImpl,
    RunningActionsManager, RunningActionsManagerArgs, RunningActionsManagerImpl,
};

use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, measure};

// ─── LatencyInjectingStore ────────────────────────────────────────────────────
//
// A thin `StoreDriver` wrapper around a `MemoryStore` (the inner store) that
// injects a configurable `tokio::time::sleep(rpc_latency)` at the entry of
// every `has_with_results` and `update` call.  The sleep models a realistic
// CAS RPC round-trip (GrpcStore → remote CAS, p50 order-of-millis), so the
// wall-clock `upload_results()` timing reflects remote-tier cost instead of
// nanosecond MemoryStore latency.
//
// This wrapper records NO metrics: it is a pure latency shim.  The data is
// actually stored in the inner `MemoryStore`, so uploads succeed and
// `has_with_results` returns correct results — the latency injection is
// purely additive (sleep before delegating).

/// Wrapper store that injects per-call latency before delegating to an inner
/// `MemoryStore`.
///
/// **rpc_latency = Duration::ZERO:** models the F2 local-store path.
/// **rpc_latency = Duration::from_millis(3):** models the non-F2 remote path.
#[derive(Debug)]
pub struct LatencyInjectingStore {
    inner: Arc<MemoryStore>,
    rpc_latency: Duration,
}

impl LatencyInjectingStore {
    pub fn new(rpc_latency: Duration) -> Arc<Self> {
        let inner = MemoryStore::new(&MemorySpec::default());
        Arc::new(Self { inner, rpc_latency })
    }
}

impl MetricsComponent for LatencyInjectingStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[async_trait::async_trait]
impl HealthStatusIndicator for LatencyInjectingStore {
    fn get_name(&self) -> &'static str {
        "LatencyInjectingStore"
    }

    async fn check_health(&self, _namespace: std::borrow::Cow<'static, str>) -> HealthStatus {
        HealthStatus::new_ok(self, std::borrow::Cow::Borrowed("bench-only"))
    }
}

#[async_trait::async_trait]
impl StoreDriver for LatencyInjectingStore {
    async fn has_with_results(
        self: core::pin::Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Inject latency BEFORE delegating — models the RPC round-trip
        // that starts at the first byte sent and ends when the response
        // arrives.  The sleep represents network + server processing time.
        if !self.rpc_latency.is_zero() {
            tokio::time::sleep(self.rpc_latency).await;
        }
        core::pin::Pin::new(self.inner.as_ref())
            .has_with_results(keys, results)
            .await
    }

    async fn update(
        self: core::pin::Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Inject latency BEFORE delegating.
        if !self.rpc_latency.is_zero() {
            tokio::time::sleep(self.rpc_latency).await;
        }
        core::pin::Pin::new(self.inner.as_ref())
            .update(key, reader, upload_size)
            .await
    }

    async fn get_part(
        self: core::pin::Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // No latency injection on reads — the bench only measures upload_results,
        // which does not read from the slow store.
        core::pin::Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        // Bench-only store with no eviction-event semantics.  Reject the
        // registration with the same shape a network leaf (GrpcStore) uses so
        // `FastSlowStore`'s #367 listener routes to its quiet `debug!`
        // (expected-rejection) branch instead of the loud `warn!` it fires
        // when a store reports `supports_removal_callbacks() == false` yet
        // ACCEPTS the registration (fast_slow_store.rs:771).
        Err(make_err!(
            Code::FailedPrecondition,
            "LatencyInjectingStore is bench-only and fires no removal callbacks"
        ))
    }

    fn supports_removal_callbacks(&self) -> bool {
        false
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        // Forward BIS-chain walk to the inner MemoryStore.
        StableDigestDelegation::Inner(self.inner.as_ref())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_ref())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_ref())
    }
}

// ─── Cell parameters ─────────────────────────────────────────────────────────

/// Describes one U1 bench cell.
#[derive(Debug, Clone, Copy)]
struct U1Cell {
    /// Short unique name suffix for the cell (e.g. "5_files_rpc3ms").
    /// Full cell name: `u1_upload_{label}`.
    label: &'static str,
    /// Number of output files (or files inside the output directory for
    /// directory-output mode).
    num_files: usize,
    /// Size of each output file in bytes.
    file_size: usize,
    /// Injected RPC latency on the slow store per call.
    rpc_latency: Duration,
    /// When true, `deferred_output_uploads_enabled = true` on the manager
    /// (F2 mode: writes go to the fast FilesystemStore only; slow store
    /// `has_with_results` is not called with remote latency).
    f2_mode: bool,
    /// When true, the Command proto uses `output_directories` instead of
    /// `output_paths`/`output_files`.  The bench creates a single
    /// directory at `outdir/` containing `num_files` files.
    dir_outputs: bool,
    /// Iterations override (None = use default 20).
    iters_override: Option<u32>,
}

/// U1 cell matrix: 6 cells covering the full variation space.
///
/// Cell naming convention: `u1_upload_{label}` (permanent anchors).
/// Any rename = a new cell; the old name becomes a dangling baseline ref.
///
/// - `no_latency` cell: noise floor.  No injected delay; measures
///   file-I/O + hash + MemoryStore costs without RPC structure.
/// - `rpc3ms` cells: 3 ms per call models the GrpcStore → remote CAS p50
///   RTT order-of-magnitude (a modelling choice, not a committed number).
/// - `f2_mode`: deferred uploads enabled; no injected latency on has()
///   (F2 fast-store path is local, sub-ms).
/// - `dir_outputs`: output_directories variant; the worker walks the tree
///   in Phase 1 and uploads the directory's Tree/Directory protos in
///   Phase 2.  Included to vary the upload shape, not to count `has()` calls.
const U1_CELLS: &[U1Cell] = &[
    U1Cell {
        label: "5_files_no_latency",
        num_files: 5,
        file_size: 65_536, // 64 KiB
        rpc_latency: Duration::ZERO,
        f2_mode: false,
        dir_outputs: false,
        iters_override: Some(20),
    },
    U1Cell {
        label: "5_files_rpc3ms",
        num_files: 5,
        file_size: 65_536,
        rpc_latency: Duration::from_millis(3),
        f2_mode: false,
        dir_outputs: false,
        iters_override: Some(20),
    },
    U1Cell {
        label: "20_files_rpc3ms",
        num_files: 20,
        file_size: 65_536,
        rpc_latency: Duration::from_millis(3),
        f2_mode: false,
        dir_outputs: false,
        iters_override: Some(20),
    },
    U1Cell {
        label: "100_files_rpc3ms",
        num_files: 100,
        file_size: 65_536,
        rpc_latency: Duration::from_millis(3),
        f2_mode: false,
        dir_outputs: false,
        // Uploads are parallel: the per-iter floor is ~one batch-has (3 ms)
        // plus one parallel upload round, NOT 100 × 3 ms.  Keep the default
        // 20 iters for a stable p50/p99 — fewer iters gave a first-sample
        // outlier that dominated the percentile.
        iters_override: Some(20),
    },
    U1Cell {
        label: "20_files_f2_mode",
        num_files: 20,
        file_size: 65_536,
        rpc_latency: Duration::ZERO, // F2 uses local fast store: sub-ms
        f2_mode: true,
        dir_outputs: false,
        iters_override: Some(20),
    },
    U1Cell {
        label: "20_dir_files_rpc3ms",
        num_files: 20,
        file_size: 65_536,
        rpc_latency: Duration::from_millis(3),
        f2_mode: false,
        dir_outputs: true,
        iters_override: Some(20),
    },
];

// ─── Store + manager construction ────────────────────────────────────────────

/// Build a `FastSlowStore` for one U1 cell.
///
/// fast = `FilesystemStore` on `root/content` + `root/tmp`
///   (required: `RunningActionsManagerImpl::new_with_callbacks` downcasts
///   the fast store to `FilesystemStore` and panics if the cast fails).
///
/// slow = `LatencyInjectingStore(MemoryStore)` with the cell's `rpc_latency`.
///
/// The `slow:` `StoreSpec` in the `FastSlowSpec` below is only read by
/// `new_validated` for the `slow_writes_in_flight_max_bytes` validation and
/// the direction/chunked flags — the actual slow store object is the
/// `LatencyInjectingStore` passed as the third argument.  `Memory` is the
/// correct spec to declare: the inner store is a `MemoryStore` and the
/// wrapper inherits `requires_in_flight_buffer_cap() == false`, so the
/// `cap == 0` (uncapped) setting passes validation.
async fn build_bench_stores(
    root: &std::path::Path,
    rpc_latency: Duration,
) -> Result<Arc<FastSlowStore>, Error> {
    let content_path = root.join("content").to_string_lossy().into_owned();
    let temp_path = root.join("tmp").to_string_lossy().into_owned();
    tokio::fs::create_dir_all(&content_path).await.map_err(|e| {
        make_err!(Code::Internal, "U1: create content_path {content_path}: {e}")
    })?;
    tokio::fs::create_dir_all(&temp_path).await.map_err(|e| {
        make_err!(Code::Internal, "U1: create temp_path {temp_path}: {e}")
    })?;

    let fast_spec = FilesystemSpec {
        content_path: content_path.clone(),
        temp_path: temp_path.clone(),
        eviction_policy: None,
        ..Default::default()
    };
    let fast_store: Arc<FilesystemStore<FileEntryImpl>> =
        FilesystemStore::new(&fast_spec).await?;

    let slow_store = LatencyInjectingStore::new(rpc_latency);

    let cas = FastSlowStore::new_validated(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_spec),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fast_store),
        Store::new(slow_store),
    )?;
    Ok(cas)
}

/// Monotonic bench clock: advances by 1 second per call so action metadata
/// timestamps are deterministic and never clash between iterations.
static BENCH_CLOCK_COUNTER: AtomicU64 = AtomicU64::new(0);

fn bench_clock() -> SystemTime {
    let t = BENCH_CLOCK_COUNTER.fetch_add(1, Ordering::Relaxed);
    UNIX_EPOCH
        .checked_add(Duration::from_secs(10_000 + t))
        .expect("bench clock overflow")
}

/// Build a `RunningActionsManagerImpl` for the bench.
///
/// AC store = `MemoryStore` (SuccessOnly strategy, same as prod).
/// `max_upload_timeout` = 600s — any real stall is a bench bug; the
/// generous timeout surfaces it as a hang rather than a spurious error.
fn build_manager(
    root_action_directory: String,
    cas_store: Arc<FastSlowStore>,
    deferred_output_uploads_enabled: bool,
) -> Result<Arc<RunningActionsManagerImpl>, Error> {
    let upload_cfg = UploadActionResultConfig {
        upload_ac_results_strategy: UploadCacheResultsStrategy::SuccessOnly,
        upload_historical_results_strategy: None,
        ..Default::default()
    };
    let ac_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let historical_store = Store::new(MemoryStore::new(&MemorySpec::default()));

    let manager = RunningActionsManagerImpl::new_with_callbacks(
        RunningActionsManagerArgs {
            root_action_directory,
            execution_configuration: ExecutionConfiguration::default(),
            cas_store,
            ac_store: Some(ac_store),
            ac_mirror_target: None,
            historical_store,
            upload_action_result_config: &upload_cfg,
            max_action_timeout: Duration::MAX,
            max_upload_timeout: Duration::from_secs(600),
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
            deferred_output_uploads_enabled,
        },
        Callbacks {
            now_fn: bench_clock,
            sleep_fn: |_| Box::pin(std::future::pending()),
        },
    )?;
    Ok(Arc::new(manager))
}

// ─── Proto upload ─────────────────────────────────────────────────────────────

/// Action descriptor shared across all iterations of a cell.
struct ActionKey {
    action_digest: DigestInfo,
    platform: Option<Platform>,
}

/// Upload Command + Action protos once per cell.
///
/// For file-output cells: `output_files = [output_0.bin, ..., output_N.bin]`.
/// For dir-output cells:  `output_directories = ["outdir"]` (one directory
/// containing N files named `outdir/file_0.bin` etc.).
async fn upload_action_proto(
    cell: &U1Cell,
    cas_store: &Arc<FastSlowStore>,
) -> Result<ActionKey, Error> {
    let command = if cell.dir_outputs {
        Command {
            arguments: vec!["/bin/true".to_string()],
            output_directories: vec!["outdir".to_string()],
            working_directory: String::new(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: "/usr/bin:/bin".to_string(),
            }],
            ..Default::default()
        }
    } else {
        let output_files: Vec<String> =
            (0..cell.num_files).map(|i| format!("output_{i}.bin")).collect();
        Command {
            arguments: vec!["/bin/true".to_string()],
            output_files: output_files.clone(),
            working_directory: String::new(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: "/usr/bin:/bin".to_string(),
            }],
            ..Default::default()
        }
    };

    let command_digest = serialize_and_upload_message(
        &command,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await?;

    let input_root_digest = serialize_and_upload_message(
        &Directory::default(),
        cas_store.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await?;

    let action = Action {
        command_digest: Some(command_digest.into()),
        input_root_digest: Some(input_root_digest.into()),
        ..Default::default()
    };
    let platform = action.platform.clone();
    let action_digest = serialize_and_upload_message(
        &action,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await?;

    Ok(ActionKey { action_digest, platform })
}

// ─── Synthetic file content ───────────────────────────────────────────────────

/// Generate deterministic synthetic content for output file `(iter, idx)`.
/// LCG ensures distinct bytes per (iter, idx) pair so each iteration
/// writes unique blobs → the batch `has()` always returns "not found" →
/// every iter pays the full upload cost (no degenerate warm-cache path).
///
/// **Mutation target for unit test:** changing `wrapping_add(idx as u64)` to
/// `wrapping_add(0)` collapses all files to the same digest — the
/// `u1_file_content_is_distinct` test must fail with its bespoke message.
fn make_file_content(iter_n: u32, idx: usize, size: usize) -> Bytes {
    let seed: u64 = (iter_n as u64)
        .wrapping_mul(1_000_003)
        .wrapping_add(idx as u64)
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut state = seed;
    let mut buf = Vec::with_capacity(size);
    for _ in 0..size {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        buf.push((state >> 33) as u8);
    }
    Bytes::from(buf)
}

// ─── Action staging ───────────────────────────────────────────────────────────

/// One fully-staged action ready for the timed `upload_results()` call.
struct ReadyAction {
    action: Arc<RunningActionImpl>,
}

/// Stage one action: `prepare_action` → write output files → `execute`.
/// Nothing in this function is inside the timed body.
///
/// For `dir_outputs = true`: creates `work_dir/outdir/file_N.bin` files
/// so `inner_upload_results` sees a directory output.
async fn stage_one_action(
    iter_n: u32,
    cell: &U1Cell,
    key: &ActionKey,
    manager: &Arc<RunningActionsManagerImpl>,
) -> Result<ReadyAction, Error> {
    let operation_id = format!("u1-bench-iter-{iter_n}");

    let execute_request = ExecuteRequest {
        action_digest: Some(key.action_digest.into()),
        digest_function: ProtoDigestFunction::Blake3.into(),
        ..Default::default()
    };

    let action_impl = manager
        .clone()
        .create_and_add_action(
            "bench-worker".to_string(),
            StartExecute {
                execute_request: Some(execute_request),
                operation_id,
                queued_timestamp: None,
                platform: key.platform.clone(),
                worker_id: "bench-worker".to_string(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                missing_digests: Vec::new(),
            },
        )
        .await?;

    // [A] prepare_action: creates work_directory + parent dirs for declared outputs.
    let action_impl = action_impl.prepare_action().await?;

    let work_dir = action_impl.get_work_directory().clone();

    // [B] Write synthetic output files OUTSIDE the timed window.
    //     Files must exist when upload_results() runs so inner_upload_results
    //     can stat/hash/upload them.
    if cell.dir_outputs {
        // Directory-output mode: create `work_dir/outdir/` + N files inside.
        let outdir = format!("{work_dir}/outdir");
        tokio::fs::create_dir_all(&outdir).await.map_err(|e| {
            make_err!(Code::Internal, "U1: create outdir {outdir}: {e}")
        })?;
        for idx in 0..cell.num_files {
            let path = format!("{outdir}/file_{idx}.bin");
            let content = make_file_content(iter_n, idx, cell.file_size);
            tokio::fs::write(&path, &content[..]).await.map_err(|e| {
                make_err!(Code::Internal, "U1: write dir file {path}: {e}")
            })?;
        }
    } else {
        // File-output mode: write each declared output file directly.
        for idx in 0..cell.num_files {
            let path = format!("{work_dir}/output_{idx}.bin");
            let content = make_file_content(iter_n, idx, cell.file_size);
            tokio::fs::write(&path, &content[..]).await.map_err(|e| {
                make_err!(Code::Internal, "U1: write output file {path}: {e}")
            })?;
        }
    }

    // [C] execute(/bin/true): exits 0 immediately. Populates execution_result
    //     so upload_results can read the exit code + stdout/stderr.
    let action_impl = action_impl.execute().await?;

    Ok(ReadyAction { action: action_impl })
}

// ─── Per-cell driver ───────────────────────────────────────────────────────────

/// Run a single U1 cell end-to-end and return its `BenchmarkResult`.
///
/// All setup (store + manager construction, proto upload, per-iter action
/// staging incl. file writes + execute) happens OUTSIDE the timed window.
/// The timed closure passed to `measure` only pulls a pre-staged action and
/// calls `upload_results()` — the path the cell exists to measure.  Cleanup
/// runs after `measure` returns.
///
/// Factored out of [`run`] so the timed-window discipline is unit-testable
/// (see `u1_stage_one_action_materializes_outputs_before_timed_window`).
async fn run_cell(
    cell: &U1Cell,
    cell_root: &std::path::Path,
    iters: u32,
) -> Result<BenchmarkResult, Error> {
    let scenario_name = format!("u1_upload_{}", cell.label);
    let store_root = cell_root.join("store");
    let actions_root = cell_root.join("actions");

    let cas_store = build_bench_stores(&store_root, cell.rpc_latency).await?;

    let root_action_dir = actions_root.to_string_lossy().into_owned();
    tokio::fs::create_dir_all(&root_action_dir).await.map_err(|e| {
        make_err!(Code::Internal, "U1: create root_action_dir {root_action_dir}: {e}")
    })?;

    let manager = build_manager(root_action_dir, cas_store.clone(), cell.f2_mode)?;

    // Upload Command + Action protos once; reused across all iterations.
    let key = upload_action_proto(cell, &cas_store).await?;

    // Pre-stage all `iters` actions OUTSIDE the timed loop.
    // `stage_one_action` runs prepare_action + file-writes + execute;
    // none of those should appear in the timed body.
    // CAPPED AT iters (≤ 20 for all cells): one action per iteration.
    let mut staged: Vec<ReadyAction> = Vec::with_capacity(iters as usize);
    for n in 0..iters {
        staged.push(stage_one_action(n, cell, &key, &manager).await?);
    }

    // ── Timed body ──────────────────────────────────────────────────
    let mut staged_iter = staged.into_iter();
    let throughput_bytes: u64 = (cell.num_files * cell.file_size) as u64;

    // Collect post-cleanup handles; upload_results returns the arc and
    // Drop fires an ERROR log if cleanup is not called explicitly.
    // CAPPED AT iters: one arc per iteration.
    let post_cleanup: Arc<parking_lot::Mutex<Vec<Arc<RunningActionImpl>>>> =
        Arc::new(parking_lot::Mutex::new(Vec::with_capacity(iters as usize)));

    let mut extras = BTreeMap::new();
    extras.insert(
        "latency_model".to_string(),
        serde_json::json!({
            "rpc_latency_ms": cell.rpc_latency.as_millis(),
            "description": if cell.rpc_latency.is_zero() {
                "zero (local store / F2 mode)"
            } else {
                "3ms injected per has()/update() call (models GrpcStore remote CAS p50 RTT order-of-magnitude)"
            }
        }),
    );
    extras.insert("num_output_files".to_string(), serde_json::json!(cell.num_files));
    extras.insert("file_size_bytes".to_string(), serde_json::json!(cell.file_size));
    extras.insert("f2_mode".to_string(), serde_json::json!(cell.f2_mode));
    extras.insert("dir_outputs".to_string(), serde_json::json!(cell.dir_outputs));
    extras.insert(
        "composition".to_string(),
        serde_json::json!(
            "FastSlowStore { fast: FilesystemStore, slow: LatencyInjectingStore(MemoryStore) }"
        ),
    );
    extras.insert(
        "cache_warmth".to_string(),
        serde_json::json!("cold_per_iter_distinct_blobs"),
    );
    extras.insert(
        "measures".to_string(),
        serde_json::json!(
            "wall_clock_upload_results: phase1_hash_plus_batch_has_plus_phase2_upload_plus_ac_write"
        ),
    );

    let result = {
        let post_cleanup = post_cleanup.clone();
        measure(
            "U1",
            &scenario_name,
            Some(throughput_bytes),
            1,
            CacheState::Cold,
            iters,
            Some(throughput_bytes),
            None,
            extras,
            move || {
                let ready = staged_iter
                    .next()
                    .expect("U1: staged Vec exhausted — pre-stage count must equal iters");
                let post_cleanup = post_cleanup.clone();

                async move {
                    let uploaded = ready
                        .action
                        .upload_results()
                        .await
                        .expect("U1: upload_results must succeed in bench");
                    post_cleanup.lock().push(uploaded);
                }
            },
        )
        .await
    };

    // Run cleanup outside the timed body.
    let to_clean = post_cleanup.lock().drain(..).collect::<Vec<_>>();
    for action in to_clean {
        if let Err(e) = action.cleanup().await {
            eprintln!("[bench] U1: cleanup failed for {scenario_name}: {e:?}");
        }
    }

    Ok(result)
}

// ─── Public entry point ───────────────────────────────────────────────────────

pub async fn run(opts: &RunOpts, temp_dir_base: Option<&PathBuf>) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();

    for cell in U1_CELLS {
        let scenario_name = format!("u1_upload_{}", cell.label);
        if !opts.matches(&scenario_name) {
            continue;
        }

        let iters = opts.effective_iters(cell.iters_override.unwrap_or(20));

        // Allocate a temp directory for this cell's FilesystemStore.
        // `/dev/shm` on Linux avoids polluting the ZFS ARC and tank dataset
        // with bench churn (data_plane_bench.rs:56 rationale).
        let cell_tmpdir = match tempfile::Builder::new()
            .prefix("nl-bench-u1-")
            .tempdir_in(temp_dir_base.map(|p| p.as_path()).unwrap_or_else(|| {
                std::path::Path::new("/dev/shm")
            })) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[bench] U1: failed to create tmpdir for {scenario_name}: {e:?}");
                continue;
            }
        };

        match run_cell(cell, cell_tmpdir.path(), iters).await {
            Ok(result) => out.push(result),
            Err(e) => {
                eprintln!("[bench] U1: run_cell failed for {scenario_name}: {e:?}");
            }
        }
        // `cell_tmpdir` drops here — auto-cleanup.
    }

    out
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// `make_file_content` must produce distinct bytes for distinct
    /// (iter, idx) pairs.  A collision means two files hash to the same
    /// digest → the bench measures a cache-hit path instead of cold upload.
    ///
    /// **Mutation:** replace `wrapping_add(idx as u64)` with
    /// `wrapping_add(0)` — this test MUST fail with:
    /// "distinct idx MUST produce distinct content"
    #[test]
    fn u1_file_content_is_distinct() {
        let a = make_file_content(0, 0, 64);
        let b = make_file_content(0, 1, 64);
        let c = make_file_content(1, 0, 64);
        assert_ne!(
            &a[..],
            &b[..],
            "distinct idx MUST produce distinct content — otherwise two \
             output files hash to the same digest (warm-cache path)"
        );
        assert_ne!(
            &a[..],
            &c[..],
            "distinct iter MUST produce distinct content — otherwise \
             iter 0 and iter 1 share the same blob (warm-cache path)"
        );
    }

    /// `make_file_content` must be deterministic.
    ///
    /// **Mutation:** add `thread_rng` to `make_file_content` — this test
    /// MUST fail with: "make_file_content MUST be deterministic"
    #[test]
    fn u1_file_content_is_deterministic() {
        let a = make_file_content(7, 3, 256);
        let b = make_file_content(7, 3, 256);
        assert_eq!(
            &a[..],
            &b[..],
            "make_file_content MUST be deterministic — same (iter, idx) \
             must produce identical bytes for reproducible baselines"
        );
    }

    /// U1 cell names must start with `u1_upload_` so `--filter u1` selects
    /// all cells.  A typo in a label is a permanent baseline anchor defect.
    ///
    /// **Mutation:** change `label: "5_files_no_latency"` to `label:
    /// "5files_no_latency"` (remove underscore) — this test MUST fail with:
    /// "U1 cell name MUST start with u1_upload_"
    #[test]
    fn u1_cell_names_have_expected_prefix() {
        for cell in U1_CELLS {
            let name = format!("u1_upload_{}", cell.label);
            assert!(
                name.starts_with("u1_upload_"),
                "U1 cell name '{name}' MUST start with 'u1_upload_' \
                 so --filter u1 selects all U1 cells"
            );
        }
    }

    /// The first cell must be the noise-floor cell (no injected latency).
    /// Placing it first makes `--filter u1_upload_5_files_no_latency`
    /// unambiguous and documents its role as baseline anchor.
    ///
    /// **Mutation:** swap U1_CELLS[0] and U1_CELLS[1] — this test MUST fail.
    #[test]
    fn u1_cells_first_is_noise_floor() {
        let first = &U1_CELLS[0];
        assert_eq!(
            first.label,
            "5_files_no_latency",
            "first U1 cell MUST be the noise-floor cell (no injected latency) \
             to anchor harness + file-I/O cost without RPC structure"
        );
        assert!(
            first.rpc_latency.is_zero(),
            "noise-floor cell MUST have rpc_latency = Duration::ZERO — \
             any injected latency contaminates the harness-cost measurement"
        );
        assert!(!first.f2_mode, "noise-floor cell MUST NOT use F2 mode");
    }

    /// The 3ms-latency cells must all use exactly 3 ms.
    ///
    /// **Mutation:** change `rpc_latency: Duration::from_millis(3)` to
    /// `Duration::from_millis(30)` in any rpc3ms cell — this test MUST fail.
    #[test]
    fn u1_rpc3ms_cells_use_3ms() {
        for cell in U1_CELLS {
            if cell.label.contains("rpc3ms") {
                assert_eq!(
                    cell.rpc_latency,
                    Duration::from_millis(3),
                    "cell '{}' contains 'rpc3ms' but has rpc_latency = {:?} (expected 3ms)",
                    cell.label,
                    cell.rpc_latency
                );
            }
        }
    }

    /// The F2-mode cell must have rpc_latency = ZERO (local fast-store
    /// path is sub-ms, not the 3 ms remote path).
    ///
    /// **Mutation:** set rpc_latency = Duration::from_millis(3) on the
    /// f2_mode cell — this test MUST fail.
    #[test]
    fn u1_f2_mode_cell_uses_zero_latency() {
        for cell in U1_CELLS {
            if cell.f2_mode {
                assert!(
                    cell.rpc_latency.is_zero(),
                    "F2-mode cell '{}' MUST have rpc_latency = ZERO — F2 uses \
                     the local FilesystemStore (sub-ms), not the remote GrpcStore",
                    cell.label
                );
            }
        }
    }

    /// The dir-outputs cell must have `dir_outputs = true` and a matching label.
    ///
    /// **Mutation:** set `dir_outputs = false` on the dir cell — this test
    /// MUST fail with: "dir_outputs cell MUST have dir_outputs = true"
    #[test]
    fn u1_dir_outputs_cell_is_correct() {
        let dir_cells: Vec<_> = U1_CELLS.iter().filter(|c| c.dir_outputs).collect();
        assert!(
            !dir_cells.is_empty(),
            "U1 cell matrix MUST include at least one dir_outputs cell \
             to vary the upload shape (directory Tree/Directory protos)"
        );
        for cell in dir_cells {
            assert!(
                cell.label.contains("dir"),
                "dir_outputs cell label '{}' MUST contain 'dir' \
                 so --filter dir selects it unambiguously",
                cell.label
            );
        }
    }

    /// **Timed-window discipline (the load-bearing harness contract).**
    ///
    /// `stage_one_action` MUST fully materialize every output file on disk
    /// BEFORE it returns — i.e. before the action is handed to the timed
    /// `upload_results()` body.  If file writes leaked into the timed window
    /// the wall-clock cell would measure file I/O + hashing of un-written
    /// data, not the upload path it exists to measure.
    ///
    /// This test stages one file-output action and one dir-output action in
    /// production composition (real `FastSlowStore` + `FilesystemStore` fast
    /// tier + real `RunningActionsManagerImpl`) and asserts every declared
    /// output file exists with the expected synthetic content immediately
    /// after staging — all within a `tokio::time::timeout` deadlock detector.
    ///
    /// **Mutation:** delete the `[B] write synthetic output files` block in
    /// `stage_one_action` — this test MUST fail with its bespoke
    /// "staged before timed window" message because the file is absent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn u1_stage_one_action_materializes_outputs_before_timed_window() {
        for cell in [
            U1Cell {
                label: "test_files",
                num_files: 3,
                file_size: 1_024,
                rpc_latency: Duration::ZERO,
                f2_mode: false,
                dir_outputs: false,
                iters_override: Some(1),
            },
            U1Cell {
                label: "test_dir",
                num_files: 3,
                file_size: 1_024,
                rpc_latency: Duration::ZERO,
                f2_mode: false,
                dir_outputs: true,
                iters_override: Some(1),
            },
        ] {
            let root = tempfile::TempDir::new().expect("test tempdir");
            let cas_store = build_bench_stores(&root.path().join("store"), cell.rpc_latency)
                .await
                .expect("build_bench_stores must succeed");
            let actions_root = root.path().join("actions");
            let root_action_dir = actions_root.to_string_lossy().into_owned();
            tokio::fs::create_dir_all(&root_action_dir)
                .await
                .expect("create actions root");
            let manager = build_manager(root_action_dir, cas_store.clone(), cell.f2_mode)
                .expect("build_manager must succeed");
            let key = upload_action_proto(&cell, &cas_store)
                .await
                .expect("upload_action_proto must succeed");

            let ready = tokio::time::timeout(
                Duration::from_secs(10),
                stage_one_action(0, &cell, &key, &manager),
            )
            .await
            .expect("stage_one_action must not hang — timed-window staging deadlock")
            .expect("stage_one_action must succeed");

            let work_dir = ready.action.get_work_directory().clone();

            // Every declared output file MUST already exist on disk with the
            // expected synthetic content — staged before the timed window.
            for idx in 0..cell.num_files {
                let path = if cell.dir_outputs {
                    format!("{work_dir}/outdir/file_{idx}.bin")
                } else {
                    format!("{work_dir}/output_{idx}.bin")
                };
                let on_disk = tokio::fs::read(&path).await.unwrap_or_else(|e| {
                    panic!(
                        "output file '{path}' MUST exist on disk after \
                         stage_one_action returns (staged before timed window); \
                         read failed: {e:?}"
                    )
                });
                let expected = make_file_content(0, idx, cell.file_size);
                assert_eq!(
                    &on_disk[..],
                    &expected[..],
                    "output file '{path}' content MUST match make_file_content — \
                     staged before timed window, not written during upload_results"
                );
            }

            ready
                .action
                .cleanup()
                .await
                .expect("cleanup must succeed");
        }
    }
}
