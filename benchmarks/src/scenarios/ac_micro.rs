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

//! Flows A1 + A2: ActionCache reads (`get_action_result`) and writes
//! (`update_action_result`).
//!
//! Per design Section 1 (`.claude/audits/495-data-plane-benchmark-design-2026-05-16.md`):
//!
//! > A1 | ActionCache get    | `ac_server.rs::get_action_result`
//! >                          | typically `FilesystemStore` (ac path)
//! >                          | TINY (<1KiB) — should be sub-millisecond p99
//! > A2 | ActionCache update | `ac_server.rs::update_action_result`
//! >                          | same
//! >                          | write rate << CAS so not load-bearing perf-wise
//!
//! A1 anchors the per-key read cost Bazel sees on `read-and-skip-action`
//! (a hot path: every action-cache lookup goes through this); A2 anchors
//! the much-less-frequent write path. Both go through
//! `StoreLike::get_part_unchunked` / `update_oneshot` on a single
//! `FilesystemStore` — the historical (pre-#366) AC backend, still a
//! valid local-disk anchor for the AC API surface even though prod is
//! now `CompletenessChecking → ExistenceCache → FastSlow{Memory, Redis}`
//! (see prod-server.json5 `AC_MAIN_STORE`). The bench-vs-prod composition
//! gap is flagged in `extras.composition_deviation =
//! "filesystem_only_not_prod_ac_chain"` so reviewers don't mis-compare
//! these numbers against full-chain prod AC latency.
//!
//! **Composition cut vs F1 (intentional):** the F1 cells run through the
//! full prod CAS wrapper chain via `build_prod_cas_composition`. AC has a
//! DIFFERENT prod chain — and bench-substituting a real Valkey/Redis is
//! impractical. We instead anchor the AC backend's leaf API (the
//! `StoreLike` get/update calls the server uses) atop a real
//! `FilesystemStore`. A future Phase 3 scenario could wire the full
//! `CompletenessChecking → ECS → FastSlow{Memory, Memory-substitute}`
//! chain; out of scope for the Phase 2 first-scenario.
//!
//! **Cells (A1 — 4):**
//!
//! - `a1_get_action_result_hit_batch1` — single key, pre-populated. Hot
//!   path; anchors Bazel's one-shot AC probe.
//! - `a1_get_action_result_miss_batch1` — single key, fresh. The store
//!   returns `NotFound` — anchors the negative-path cost.
//! - `a1_get_action_result_hit_batch16` — 16 known keys per iter; serial
//!   `get_and_decode_digest` over each. **No native AC batch RPC exists**
//!   — Bazel's `GetActionResult` is single-key. The batch cells anchor
//!   the cumulative cost of N consecutive single-key reads, which is
//!   what a Bazel build's first scan-through-actions phase actually
//!   emits. Documented in the `a1_run_get_batch_hit` doc comment.
//! - `a1_get_action_result_hit_batch128` — same with 128 known keys.
//!
//! **Cells (A2 — 3 size buckets):**
//!
//! - `a2_update_action_result_small` — minimal AR: one OutputFile, no
//!   logs. ~80 B serialized; mimics a "no-op rule" cache write.
//! - `a2_update_action_result_medium` — 10 OutputFiles, no logs. ~1 KiB
//!   serialized; mimics a typical compile-rule cache write.
//! - `a2_update_action_result_large` — 100 OutputFiles + inline stdout/
//!   stderr. ~10 KiB serialized; mimics a chatty link-step cache write.
//!
//! Throughput: A1 reports `ElementsPerSec` (ActionResults/sec — probe-
//! style, mirrors F1); A2 reports `BytesPerSec` over the serialized AR
//! size (per-iter write of N bytes).
//!
//! **Invariant being anchored:** the per-AC-op wall-clock through
//! `StoreLike::{get_part_unchunked, update_oneshot}` over a real
//! `FilesystemStore`. A regression here flags any change that adds work
//! to the AC read or write path — e.g. an extra fsync (CLAUDE.md
//! prohibits this; the cell would surface the violation as a measured
//! regression), an extra `has` probe, an `evicting_map` lookup that
//! grew O(N), proto-decode regression on the read side.
//!
//! **What this cell does NOT measure (honest scope-cut):**
//!
//! - **CompletenessCheckingStore composition.** Prod's
//!   `AC_MAIN_STORE` wraps the AC in
//!   `CompletenessCheckingStore { backend: ECS, cas_store: cas_STORE }`
//!   which performs a per-output-file CAS existence check on every hit.
//!   That layer adds per-AR cost proportional to `len(output_files)`.
//!   These cells skip it; a Phase 3 cell would need to wire a CAS
//!   composition alongside.
//! - **AC ExistenceCache stale-positive prevention.** ECS sits between
//!   `AC_MAIN_STORE` and `AC_BACKEND_CACHED` in prod (1M-entry cap).
//!   Bench skips; a Phase 3 cell would compose ECS atop the FS leaf to
//!   anchor the AC hot path.
//! - **Real prod Valkey/Redis tier.** The slow tier of `AC_BACKEND_CACHED`
//!   is Redis in prod; this bench uses FilesystemStore exclusively.
//!   A Phase 4 backend-matrix bench (`bench --features backend-redis`)
//!   would close this gap per design Section 6 Phase 4.
//! - **gRPC AC server (transport / `AcServer` wrapper).** The cells call
//!   the store-layer API the AC server invokes; they do not exercise
//!   the `tonic::Request<UpdateActionResultRequest>` decode, the
//!   `IS_AC_PEER_FETCH` task-local scoping, or the `StallGuard`. A
//!   Phase 1.5-style follow-up could wire the ByteStream + AC servers
//!   to anchor the gRPC overhead; out of scope here.
//! - **Output-file scaling beyond 100.** Real Bazel actions can emit
//!   thousands of OutputFiles for a single rule (`genrule` with a large
//!   `outs`). The 100-file `large` cell anchors a chatty-but-bounded
//!   shape; a Phase 3 cell could push to 1K / 10K to measure proto
//!   encode tail.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_error::Error;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest as ProtoDigest, OutputFile,
};
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use prost::Message;

use crate::output::{BenchmarkResult, CacheState};
use crate::scenarios::{RunOpts, make_blob_with_indices, measure};

/// Per-cell iter default for the A1 read cells. The read path is
/// `FilesystemStore::get_part_unchunked` + prost decode — typically
/// hundreds of microseconds per single-key hit. 200 iters gets the
/// measurement window into the tens-of-ms range so percentile arithmetic
/// has signal without the wall-clock dragging into seconds.
const A1_DEFAULT_ITERS: u32 = 200;

/// Per-cell iter default for the A2 write cells. Writes are heavier than
/// reads (tempfile + rename + index insert). 100 iters at the largest
/// (~10 KiB) cell keeps wall-clock under a few seconds while still
/// producing percentile signal.
const A2_DEFAULT_ITERS: u32 = 100;

/// Number of pre-populated AC entries the A1 hit cells round-robin
/// across. Sized so the FilesystemStore index has many entries (catches
/// any O(N) lookup regression in `evicting_map`) but stays trivially
/// below the bench cap so populate cannot itself trigger eviction
/// during the timed read pass. Mirrors C1's `C1_POPULATE_COUNT`.
const A1_POPULATE_COUNT: u32 = 4096;

/// Bench-only filesystem cap for the AC tempdir. Each populated AR is
/// ~1 KiB serialized; 4096 × 1 KiB = 4 MiB. A2 cells write up to 100
/// iters × 10 KiB ≈ 1 MiB more. 4 GiB is ~4000× headroom; eviction
/// cannot fire during populate or during any cell.
const A_BENCH_FS_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Composition-deviation tag emitted on every A1/A2 cell's `extras` so
/// diff tooling and reviewers know the FilesystemStore leaf here does
/// NOT match prod's CompletenessChecking + ECS + FastSlow{Memory, Redis}
/// chain. Mirrors C1's `micro_no_wrappers` convention.
const COMPOSITION_DEVIATION_TAG: &str = "filesystem_only_not_prod_ac_chain";

/// Build a standalone `FilesystemStore` rooted at a fresh tempdir under
/// `temp_dir_base`. Returns the typed handle (kept for direct trait-
/// method calls if needed) wrapped in a `Store`, plus the owning
/// `tempfile::TempDir` so the on-disk content_path survives for the
/// store's lifetime. Mirrors `composition::build_prod_cas_composition`'s
/// shape (tempdir-owning) but without the wrapper chain.
async fn build_ac_filesystem_store(
    temp_dir_base: Option<&PathBuf>,
) -> Result<(Store, tempfile::TempDir), Error> {
    let temp_dir = match temp_dir_base {
        Some(p) => tempfile::TempDir::new_in(p),
        None => tempfile::TempDir::new(),
    }
    .map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "tempdir creation: {e:?}",
        )
    })?;
    let content_path = temp_dir.path().join("content").to_string_lossy().into_owned();
    let temp_path = temp_dir.path().join("temp").to_string_lossy().into_owned();
    tokio::fs::create_dir_all(&content_path).await.map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "create content_path: {e:?}",
        )
    })?;
    tokio::fs::create_dir_all(&temp_path).await.map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "create temp_path: {e:?}",
        )
    })?;
    let spec = FilesystemSpec {
        content_path,
        temp_path,
        read_buffer_size: 3 * 1024 * 1024,
        eviction_policy: Some(EvictionPolicy {
            max_bytes: A_BENCH_FS_MAX_BYTES,
            ..Default::default()
        }),
        block_size: 4096,
        max_concurrent_writes: 0,
        // CLAUDE.md HARD-RULE: NO fsync / fdatasync / sync_file_range /
        // msync / O_SYNC / O_DSYNC anywhere in the codebase, including
        // bench. Production runs ZFS tank with sync=disabled; durability
        // is the mirror_blobs ≥2-replica + BlobsInStableStorage ack, not
        // per-write flushes. Today the field is dead (no code reads it),
        // but reviewers grep `sync_data` as severity BLOCK — keep this
        // false so the bench fixture mirrors the rule literally. #589.
        sync_data_only: false,
        // AC is NOT content-addressed by content (see ac_server.rs note
        // at :199-205). content_is_immutable=true would let a duplicate
        // write skip the rename + index-update, which is WRONG for AC:
        // the same key can legitimately hold different bytes (e.g. a
        // re-run with new metadata). Bench mirrors the prod AC config
        // (prod-server.json5 sets immutable=true ONLY on the CAS slow tier,
        // never on the AC chain).
        content_is_immutable: false,
        fadvise_dontneed: false,
        max_concurrent_large_reads: 0,
        large_read_threshold_bytes: 4 * 1024 * 1024,
        // 0 = use `pin_cap` (FilesystemSpec default). The AC bench fixture
        // has no indefinite-pin cap to exercise. Field added by #FL-681
        // follow-up `b73a1f94`; this explicit literal predated it.
        pending_bis_pin_max_bytes: 0,
    };
    let fs_arc: Arc<FilesystemStore> = FilesystemStore::new(&spec).await?;
    Ok((Store::new(fs_arc), temp_dir))
}

/// Construct a deterministic `ActionResult` of a target shape. The shape
/// is parameterized by:
/// - `output_file_count`: how many `OutputFile` entries to attach.
/// - `inline_stdout_stderr_bytes`: if `Some(n)`, fill `stdout_raw` and
///   `stderr_raw` with `n` bytes each of deterministic filler.
///
/// Every `OutputFile`'s `digest` field is populated with a 64-char hex
/// hash (SHA-256 shape) derived from `(scenario_name, i)` — distinct per
/// position. The digest IS NOT used to look up CAS in this bench (we're
/// not running CompletenessChecking); it exists to give the proto
/// realistic size + shape.
///
/// Returns the typed `ActionResult` AND its serialized bytes; callers
/// use the bytes for the `update_oneshot` payload and the typed value
/// for assertions / `encoded_len` reporting.
fn build_action_result_of_shape(
    scenario_name: &str,
    output_file_count: usize,
    inline_stdout_stderr_bytes: Option<usize>,
) -> (ActionResult, Bytes) {
    let mut output_files = Vec::with_capacity(output_file_count);
    for i in 0..output_file_count {
        // Reuse the deterministic blob factory to produce a digest hash;
        // we throw away the bytes (only need the digest hex form).
        let (digest_info, _) = make_blob_with_indices(scenario_name, 0, i as u32, 32);
        let hash_hex = hex_encode_lower(digest_info.packed_hash().as_ref());
        output_files.push(OutputFile {
            path: format!("bazel-out/k8-fastbuild/bin/pkg/file_{i}.o"),
            digest: Some(ProtoDigest {
                hash: hash_hex,
                size_bytes: 1024 + (i as i64),
            }),
            is_executable: false,
            contents: Bytes::new(),
            node_properties: None,
        });
    }
    let mut ar = ActionResult {
        output_files,
        output_file_symlinks: Vec::new(),
        output_symlinks: Vec::new(),
        output_directories: Vec::new(),
        output_directory_symlinks: Vec::new(),
        exit_code: 0,
        stdout_raw: Bytes::new(),
        stdout_digest: None,
        stderr_raw: Bytes::new(),
        stderr_digest: None,
        execution_metadata: None,
    };
    if let Some(n) = inline_stdout_stderr_bytes {
        let filler: Bytes = make_blob_with_indices(scenario_name, 1, 0, n).1;
        ar.stdout_raw = filler.clone();
        ar.stderr_raw = filler;
    }
    let mut buf = bytes::BytesMut::with_capacity(ar.encoded_len());
    ar.encode(&mut buf)
        .expect("ActionResult must serialize — proto fields populated locally");
    (ar, buf.freeze())
}

/// Lowercase hex encoder — avoids pulling in `hex` crate for one
/// formatter. Matches the format `Digest::hash` expects (lowercase hex
/// of the raw 32-byte SHA-256).
fn hex_encode_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(nibble_to_hex_lower(b >> 4));
        out.push(nibble_to_hex_lower(b & 0x0f));
    }
    out
}

#[inline]
fn nibble_to_hex_lower(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => unreachable!(),
    }
}

/// Generate `count` action digests deterministically seeded by
/// `scenario_name`. Mirrors C1's `populate()` shape: the digest's hash
/// content is a function only of the seed, so populate-then-probe is
/// reproducible across runs (and across an interrupted-then-resumed
/// session).
fn populate_digests(scenario_name: &str, count: u32) -> Vec<DigestInfo> {
    (0..count)
        .map(|j| make_blob_with_indices(scenario_name, 0, j, 32).0)
        .collect()
}

pub async fn run(opts: &RunOpts, temp_dir_base: Option<&PathBuf>) -> Vec<BenchmarkResult> {
    let mut out = Vec::new();

    // Single FilesystemStore shared across all A1 and A2 cells in one
    // run. A1 populate seeds with `"a1_populate"`; A2 cells generate
    // fresh-per-iter digests via their own per-cell seed. The two cells'
    // seed pools are disjoint (different scenario name strings into
    // make_blob_with_indices), so A2's writes cannot collide with the
    // A1 hit-path's pre-populated set.
    let (ac_store, _temp_dir) = match build_ac_filesystem_store(temp_dir_base).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[bench] A1/A2 FilesystemStore build failed: {e:?}");
            return out;
        }
    };

    // ---------- A1 (AC get) ----------
    //
    // Populate the FilesystemStore with `A1_POPULATE_COUNT` AC entries.
    // Each entry is the serialized form of a `medium`-shape ActionResult
    // (10 OutputFiles, ~1 KiB). Bazel-typical compile-rule shape; gives
    // the hit-path realistic proto-decode cost rather than measuring a
    // pathologically tiny payload.
    let a1_iters = opts.effective_iters(A1_DEFAULT_ITERS);
    let known_digests = populate_digests("a1_populate", A1_POPULATE_COUNT);
    // Single canonical AR payload for population — the AC chain doesn't
    // verify that bytes match the action_digest's hash (see ac_server.rs
    // :199-205), so re-using one serialized payload across N populate
    // entries is semantically valid and saves N proto-encode calls.
    let (_, populate_payload) = build_action_result_of_shape("a1_populate_ar", 10, None);
    for digest in &known_digests {
        if let Err(e) = ac_store
            .update_oneshot(*digest, populate_payload.clone())
            .await
        {
            eprintln!("[bench] A1 populate failed at digest={digest:?}: {e:?}");
            return out;
        }
    }

    // A1 hit cells across batch sizes 1, 16, 128.
    for &(batch, label) in &[(1u32, "1"), (16u32, "16"), (128u32, "128")] {
        let hit_name = format!("a1_get_action_result_hit_batch{label}");
        if opts.matches(&hit_name) {
            out.push(
                a1_run_get_batch_hit(&ac_store, &known_digests, batch as usize, a1_iters, &hit_name)
                    .await,
            );
        }
    }

    // A1 miss cell — single key, fresh digest per iter. Reusing the same
    // miss digest across iters would let the FilesystemStore's negative-
    // cache (if any) amortize the lookup; fresh digests anchor a true
    // per-call NotFound cost.
    let miss_name = "a1_get_action_result_miss_batch1";
    if opts.matches(miss_name) {
        out.push(a1_run_get_miss(&ac_store, a1_iters, miss_name).await);
    }

    // ---------- A2 (AC update) ----------
    //
    // Three size buckets: small (~80 B), medium (~1 KiB), large (~10 KiB
    // with inline stdout/stderr). Each iter writes a fresh AR under a
    // fresh digest — anchors the cold write path including tempfile
    // create + rename + index insert. Reusing one digest across iters
    // would let the FilesystemStore short-circuit on a duplicate key
    // (the store's update path overwrites; the cost shape is similar
    // but not identical to a true cold write).
    let a2_iters = opts.effective_iters(A2_DEFAULT_ITERS);
    for cell in A2_CELLS {
        if !opts.matches(cell.name) {
            continue;
        }
        out.push(a2_run_update(&ac_store, cell, a2_iters).await);
    }

    out
}

/// A2 cell shape. Each cell writes ARs of a fixed `(output_file_count,
/// inline_stdout_stderr_bytes)` shape. Sizes were picked to span typical
/// Bazel AR shapes:
/// - `small` ≈ "rule with one output, no logs" (~80 B serialized)
/// - `medium` ≈ "compile rule, 10 outputs, no logs" (~1 KiB)
/// - `large` ≈ "link/chatty rule with logs, 100 outputs" (~10 KiB)
struct A2Cell {
    name: &'static str,
    output_file_count: usize,
    inline_logs: Option<usize>,
}

const A2_CELLS: &[A2Cell] = &[
    A2Cell {
        name: "a2_update_action_result_small",
        output_file_count: 1,
        inline_logs: None,
    },
    A2Cell {
        name: "a2_update_action_result_medium",
        output_file_count: 10,
        inline_logs: None,
    },
    A2Cell {
        name: "a2_update_action_result_large",
        output_file_count: 100,
        inline_logs: Some(4 * 1024),
    },
];

/// **No native AC batch RPC exists in this codebase.** Bazel's
/// `GetActionResult` is single-key. This cell anchors the cumulative
/// cost of N consecutive single-key reads — what a Bazel build's
/// first scan-through-actions phase actually emits. Implementation
/// performs N serial `get_part_unchunked` + prost decode calls per
/// iter. (We could `futures::join_all` for in-task concurrency, but the
/// AC server itself processes one request per RPC; serial more closely
/// matches the per-call cost we want to anchor.)
async fn a1_run_get_batch_hit(
    store: &Store,
    known: &[DigestInfo],
    batch: usize,
    iters: u32,
    scenario_name: &str,
) -> BenchmarkResult {
    let mut extras = BTreeMap::new();
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!(COMPOSITION_DEVIATION_TAG),
    );
    extras.insert("batch_size".to_string(), serde_json::json!(batch as u64));
    extras.insert(
        "populate_count".to_string(),
        serde_json::json!(A1_POPULATE_COUNT),
    );
    extras.insert("hit_path".to_string(), serde_json::json!(true));
    extras.insert(
        "ac_batch_semantics".to_string(),
        serde_json::json!("serial_single_key_calls_no_native_batch_rpc"),
    );
    // Disclose timed-body work other than the store call so a future
    // investigator chasing "why is A1 batchN hit-path X µs" sees the
    // confound up front instead of doing a deep read. The prost decode
    // is honestly-named (matches production ac_server.rs's
    // get_and_decode_digest path), but at batch128 it's 128 sequential
    // decodes dominating the warm-cache FS read. #533 sibling-audit.
    extras.insert(
        "timed_body_includes".to_string(),
        serde_json::json!(
            "get_part_unchunked_plus_prost_message_decode_per_element"
        ),
    );

    // Round-robin across `known` so the cell doesn't measure a single-
    // entry hot-cacheline artifact. C1's `run_exists_hit` pattern.
    let known: Arc<Vec<DigestInfo>> = Arc::new(known.to_vec());
    let iter_counter = std::sync::atomic::AtomicU64::new(0);
    let len = known.len() as u64;
    let store = store.clone();
    let batch_u64 = batch as u64;

    measure(
        "A1",
        scenario_name,
        None,
        1,
        CacheState::Warm,
        iters,
        None,
        Some(batch_u64),
        extras,
        move || {
            let store = store.clone();
            let known = known.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                for i in 0..batch_u64 {
                    let slot = ((n.wrapping_mul(batch_u64)).wrapping_add(i)) % len;
                    let digest = known[slot as usize];
                    let key = StoreKey::Digest(digest);
                    // Match the production AC read path:
                    // ac_server.rs::inner_get_action_result calls
                    // get_and_decode_digest, which calls get_part_unchunked
                    // and prost-decodes. We replicate inline here to keep
                    // the bench self-contained.
                    let raw = store
                        .as_store_driver_pin()
                        .get_part_unchunked(key, 0, Some(10 * 1024 * 1024))
                        .await
                        .expect("A1 hit cell: pre-populated digest must read");
                    let _decoded = ActionResult::decode(raw)
                        .expect("A1 hit cell: populate payload must decode");
                }
            }
        },
    )
    .await
}

/// A1 miss cell — anchors the cost of a single AC NotFound. Fresh
/// digest per iter (seeded by `a1_miss`), disjoint from `a1_populate`.
/// The store returns `Code::NotFound`; the cell asserts that's the
/// error, not a transport/wiring failure.
async fn a1_run_get_miss(store: &Store, iters: u32, scenario_name: &str) -> BenchmarkResult {
    let mut extras = BTreeMap::new();
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!(COMPOSITION_DEVIATION_TAG),
    );
    extras.insert("batch_size".to_string(), serde_json::json!(1u64));
    extras.insert("hit_path".to_string(), serde_json::json!(false));

    // Pre-generate one fresh miss digest per iter. Seeded by
    // `a1_miss` — disjoint from `a1_populate` (different scenario name
    // strings into make_blob_with_indices) so no collision can fire.
    let pregen: Arc<Vec<DigestInfo>> = Arc::new(
        (0..iters)
            .map(|n| make_blob_with_indices("a1_miss", n as u64, 0, 32).0)
            .collect(),
    );
    let iter_counter = std::sync::atomic::AtomicU64::new(0);
    let store = store.clone();

    measure(
        "A1",
        scenario_name,
        None,
        1,
        CacheState::Cold,
        iters,
        None,
        Some(1),
        extras,
        move || {
            let store = store.clone();
            let pregen = pregen.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let digest = pregen[n as usize];
                let key = StoreKey::Digest(digest);
                let res = store
                    .as_store_driver_pin()
                    .get_part_unchunked(key, 0, Some(10 * 1024 * 1024))
                    .await;
                match res {
                    Err(e) if e.code == nativelink_error::Code::NotFound => {
                        // Expected.
                    }
                    Err(e) => panic!(
                        "A1 miss cell: expected NotFound, got code={:?} err={:?}",
                        e.code, e
                    ),
                    Ok(_) => panic!(
                        "A1 miss cell: fresh digest must NOT be present — got Ok \
                         (a1_miss seed collided with a1_populate or store state \
                         leaked across iters)"
                    ),
                }
            }
        },
    )
    .await
}

/// A2 update cell — writes a fresh `ActionResult` per iter. Each iter:
/// (1) generates a unique `action_digest` (seeded by cell name + iter),
/// (2) takes a pre-encoded payload of the cell's target shape, and
/// (3) calls `update_oneshot`. Throughput is reported in bytes/sec over
/// the serialized AR size.
async fn a2_run_update(store: &Store, cell: &A2Cell, iters: u32) -> BenchmarkResult {
    // Build one canonical AR of the target shape OUTSIDE the timed body
    // so per-iter proto-encode cost doesn't pollute the write-path
    // measurement. AC isn't content-addressed (ac_server.rs:199-205), so
    // the same payload under N distinct digests is semantically valid.
    let (canonical_ar, payload) = build_action_result_of_shape(cell.name, cell.output_file_count, cell.inline_logs);
    let payload_size = payload.len() as u64;
    let payload_size_for_iter = payload_size;

    let mut extras = BTreeMap::new();
    extras.insert(
        "composition_deviation".to_string(),
        serde_json::json!(COMPOSITION_DEVIATION_TAG),
    );
    extras.insert(
        "output_file_count".to_string(),
        serde_json::json!(cell.output_file_count as u64),
    );
    extras.insert(
        "inline_logs_bytes".to_string(),
        serde_json::json!(cell.inline_logs.unwrap_or(0) as u64),
    );
    extras.insert(
        "serialized_ar_bytes".to_string(),
        serde_json::json!(payload_size),
    );
    extras.insert(
        "ar_encoded_len".to_string(),
        serde_json::json!(canonical_ar.encoded_len() as u64),
    );

    // Pre-generate fresh per-iter digests; seeded so they're disjoint
    // from `a1_populate` and from any other A2 cell.
    let digests: Arc<Vec<DigestInfo>> = Arc::new(
        (0..iters)
            .map(|n| make_blob_with_indices(cell.name, n as u64, 0, 32).0)
            .collect(),
    );
    let iter_counter = std::sync::atomic::AtomicU64::new(0);
    let store = store.clone();

    measure(
        "A2",
        cell.name,
        Some(payload_size),
        1,
        CacheState::Cold,
        iters,
        Some(payload_size_for_iter),
        None,
        extras,
        move || {
            let store = store.clone();
            let digests = digests.clone();
            let payload = payload.clone();
            let n = iter_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let digest = digests[n as usize];
                store
                    .update_oneshot(digest, payload)
                    .await
                    .expect("A2 write must succeed");
            }
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `A1_POPULATE_COUNT` must be large enough that the round-robin in
    /// `a1_run_get_batch_hit` hits many distinct FilesystemStore index
    /// entries within one iter pass — without that, the cell measures a
    /// single-cacheline-hot artifact rather than the real lookup cost
    /// the hit path would see in production. Mirrors C1's analogous
    /// guard. Mutation: drop `A1_POPULATE_COUNT` below 64 — this
    /// red-fails with the bespoke message.
    #[test]
    fn a1_populate_count_large_enough_for_round_robin() {
        assert!(
            A1_POPULATE_COUNT >= 64,
            "A1_POPULATE_COUNT must be >= 64 so the round-robin in \
             a1_run_get_batch_hit hits many distinct FilesystemStore \
             entries (got {A1_POPULATE_COUNT}). Below 64 the cell may \
             measure a hot-cacheline artifact rather than the real \
             lookup."
        );
    }

    /// A1 iter default must clear the medium-confidence floor so a
    /// default-iter run produces percentile numbers worth diffing.
    /// Mutation: drop `A1_DEFAULT_ITERS` below 20 — red-fails.
    #[test]
    fn a1_default_iters_clear_medium_confidence() {
        assert!(
            A1_DEFAULT_ITERS >= 20,
            "A1 default iters ({A1_DEFAULT_ITERS}) must be >= 20 — below \
             that the p99 degenerates to `max` and the diff loses meaning"
        );
        assert!(
            A2_DEFAULT_ITERS >= 20,
            "A2 default iters ({A2_DEFAULT_ITERS}) must be >= 20"
        );
    }

    /// The A2 size buckets must be monotonically increasing in
    /// serialized payload bytes. A reviewer comparing the three cells
    /// across runs implicitly relies on this ordering; a mutation that
    /// swaps two buckets' shape would let a regression hide as a
    /// "different cell" rather than a "same cell, bigger payload"
    /// signal. Mutation: rename / swap the cell entries — red-fails.
    #[test]
    fn a2_cells_monotonically_increasing_in_size() {
        let mut prev_size: u64 = 0;
        for cell in A2_CELLS {
            let (_, payload) =
                build_action_result_of_shape(cell.name, cell.output_file_count, cell.inline_logs);
            let size = payload.len() as u64;
            assert!(
                size > prev_size,
                "A2 cell `{}` produced serialized size {} bytes which is \
                 NOT strictly greater than the previous cell ({} bytes). \
                 A2 cells MUST be monotonically increasing in payload size \
                 so reviewers can interpret a regression across cells as a \
                 size-dependent trend rather than a same-payload artifact.",
                cell.name,
                size,
                prev_size
            );
            prev_size = size;
        }
    }

    /// Sanity: the populate digest pool and the miss digest pool MUST
    /// be disjoint — otherwise the miss cell's assertion fires
    /// spuriously when a collision drops a populated digest into the
    /// "fresh" set. Mirrors C1's analogous guard.
    #[test]
    fn a1_miss_seed_disjoint_from_populate() {
        let populated = populate_digests("a1_populate", 64);
        let (miss_digest, _) = make_blob_with_indices("a1_miss", 0, 0, 32);
        assert!(
            !populated.contains(&miss_digest),
            "a1_miss seed produced a digest that collides with a1_populate's \
             pool — the miss cell would mis-assert"
        );
    }

    /// A populated ActionResult of shape `(10, None)` round-trips
    /// through `prost` — sanity-check the proto fields populated in
    /// `build_action_result_of_shape` don't violate any required-field
    /// constraints. Mutation: pass a malformed digest (e.g. odd-length
    /// hex) — `Digest`'s hex parser would later reject; this test
    /// pre-empts that surprise.
    #[test]
    fn build_action_result_round_trips_through_prost() {
        let (ar, bytes) = build_action_result_of_shape("test_smoke", 10, None);
        let decoded = ActionResult::decode(bytes).expect("must decode");
        assert_eq!(decoded.output_files.len(), 10);
        assert_eq!(decoded.exit_code, ar.exit_code);
        assert!(decoded.stdout_raw.is_empty());
    }

    /// The large cell's inline logs must actually inflate the payload.
    /// Mutation: drop `inline_logs` on the large cell — the bytes-per-
    /// sec measurement collapses to medium-cell shape, this red-fails.
    #[test]
    fn a2_large_cell_has_inline_logs() {
        let large = A2_CELLS
            .iter()
            .find(|c| c.name == "a2_update_action_result_large")
            .expect("large cell must exist");
        assert!(
            large.inline_logs.is_some(),
            "a2_update_action_result_large MUST include inline stdout/stderr \
             logs — without them the cell collapses to a glorified medium-cell"
        );
    }

    /// Smoke: end-to-end through the real FilesystemStore — populate +
    /// hit-read + miss-read + write — without invoking the bench
    /// harness. Catches wiring breakage (e.g. tempdir creation, store
    /// build failures, prost encode errors) before they surface as a
    /// silent zero-result bench output.
    #[tokio::test]
    async fn ac_filesystem_smoke_populate_hit_miss_write() {
        let temp_dir_base = PathBuf::from(std::env::temp_dir());
        let (store, _td) = build_ac_filesystem_store(Some(&temp_dir_base))
            .await
            .expect("store build");
        // Populate one entry.
        let digests = populate_digests("smoke_populate", 1);
        let (_, payload) = build_action_result_of_shape("smoke_ar", 10, None);
        store
            .update_oneshot(digests[0], payload.clone())
            .await
            .expect("populate");
        // Hit-read.
        let raw = store
            .as_store_driver_pin()
            .get_part_unchunked(StoreKey::Digest(digests[0]), 0, Some(1 << 20))
            .await
            .expect("hit read");
        let _decoded = ActionResult::decode(raw).expect("decode");
        // Miss-read.
        let (miss_digest, _) = make_blob_with_indices("smoke_miss", 0, 0, 32);
        let res = store
            .as_store_driver_pin()
            .get_part_unchunked(StoreKey::Digest(miss_digest), 0, Some(1 << 20))
            .await;
        assert!(
            matches!(res, Err(ref e) if e.code == nativelink_error::Code::NotFound),
            "smoke miss-read must return NotFound, got {res:?}",
        );
        // Write.
        let (_, fresh_payload) = build_action_result_of_shape("smoke_write_ar", 5, None);
        let (fresh_digest, _) = make_blob_with_indices("smoke_write", 0, 0, 32);
        store
            .update_oneshot(fresh_digest, fresh_payload)
            .await
            .expect("write");
    }
}
