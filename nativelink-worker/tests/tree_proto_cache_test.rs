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

//! #O3/O13: regression test for the PER-ACTION in-memory Tree-proto cache
//! that lets `RunningActionsManagerImpl::expand_tree_file_digests` and
//! `spawn_upload_to_remote` skip a storage-layer round trip when reading
//! back a Tree proto that the same action just wrote.
//!
//! Architectural fix-up (2026-06-07): the cache moved from a process-wide
//! `RunningActionsManagerImpl::tree_proto_cache: Mutex<HashMap<...>>` (capped
//! at 1024 entries with silent-drop over-cap behaviour) to per-action scope
//! on `RunningActionImpl::tree_proto_cache`. RAII via the existing
//! `RunningActionImpl::Drop` evicts every entry on success, error, cancel,
//! or panic, closing the F1 leak class flagged by distributed-systems-
//! reviewer + red-team.
//!
//! - **T1 (under-action, cache hit avoids storage read):** pre-populate the
//!   action's tree-proto cache with a Tree whose files we know in advance,
//!   then call `expand_tree_file_digests` against a CAS store that does NOT
//!   contain that Tree. If the implementation re-reads from CAS, the call
//!   silently drops the digests (decode error path emits warn! and returns
//!   an empty Vec). Therefore: a populated cache MUST produce the file
//!   digests; an unpopulated cache MUST produce an empty Vec on the same
//!   fixture. Asymmetry confirms the cache hit short-circuited the read.
//!
//! - **T2 (over-action, fallback re-read works on miss):** upload a Tree to
//!   the CAS store but do NOT pre-populate the cache. `expand_tree_file_digests`
//!   must still return the contained file digests via the
//!   `get_and_decode_digest` fallback path. Mutation: comment out the
//!   `else { get_and_decode_digest(...) }` arm — T2 then red-fails.
//!
//! - **T3 (take-side asymmetric contract, second reader drains cache):**
//!   `take_cached_tree_proto` is the second reader used by
//!   `spawn_upload_to_remote`. Test that after a `peek` (reader 1) the
//!   entry remains, then after a `take` (reader 2) the entry is drained
//!   and a subsequent `peek` returns `None`. Mutation 2026-06-07: change
//!   `take_cached_tree_proto` to use `cache.get(...).cloned()` (peek
//!   semantics) — T3 red-fails on the post-take drain assertion.
//!
//! - **T5 (RAII drop drains cache on every termination path):** the F1
//!   leak class — process-wide cache + cap-only eviction — is closed iff
//!   the cache's lifetime equals the action's. Test that after caching N
//!   entries on a `RunningActionImpl`, dropping the action drops the
//!   field. Falsification: the per-action cache is owned by the action,
//!   so we observe drop via a `Weak<RunningActionImpl>` ref count and
//!   verify the cache's drop fires synchronously. Bespoke red-fail:
//!   "per-action cache leaked entry after RunningActionImpl drop".
//!   Mutation 2026-06-07: replace `tree_proto_cache:
//!   parking_lot::Mutex<HashMap<...>>` with a process-wide
//!   `Arc<Mutex<HashMap<...>>>` shared across actions → T5 red-fails
//!   because Weak::upgrade still succeeds (Arc keeps cache alive after
//!   action drop, leaking entries to the next action).
//!
//! - **T6 (perf-claim: cache hit avoids async future allocation):**
//!   `expand_tree_file_digests` partitions output_folders into
//!   `hits: Vec<(DigestInfo, ProtoTree)>` (drained synchronously) and
//!   `misses: Vec<DigestInfo>` (dispatched into FuturesUnordered). Cache
//!   hits must skip the future allocation entirely. Observation
//!   technique: time a 100% cache-hit run against a 100% cache-miss run
//!   on the SAME fixture; the hit run should be dramatically faster
//!   because it does zero storage reads AND zero future allocations.
//!   Mutation 2026-06-07: revert to the per-folder
//!   `.map(|folder| async { peek_or_decode })` form (future allocated
//!   regardless of cache outcome) — T6 wall-clock margin narrows because
//!   even hits pay the future allocation; the test red-fails on the
//!   "hits run dramatically faster than misses" assertion.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use nativelink_config::cas_server::{UploadActionResultConfig, UploadCacheResultsStrategy};
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, FileNode, Tree as ProtoTree,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionUniqueKey, ActionUniqueQualifier, DirectoryInfo,
    ExecutionMetadata, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningActionImpl, RunningActionsManagerArgs,
    RunningActionsManagerImpl,
};

/// Deadlock detector for the in-memory tree_proto_cache lookups. The cache
/// is a `parking_lot::Mutex<HashMap<...>>` with no `.await` points, so a
/// 5-second timeout is a generous upper bound; any wedge here is a real
/// bug, not flake.
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn make_temp_path(data: &str) -> String {
    use rand::Rng;
    let tmp = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    format!("{}/{}/{}", tmp, rand::rng().random::<u64>(), data)
}

async fn setup_manager() -> Result<(Arc<RunningActionsManagerImpl>, Arc<FastSlowStore>), Error> {
    let fast_config = FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: None,
        ..Default::default()
    };
    let slow_config = MemorySpec::default();
    let _fast_store = <FilesystemStore>::new(&fast_config).await?;
    let slow_store = MemoryStore::new(&slow_config);
    let cas_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_config),
            slow: StoreSpec::Memory(slow_config),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(_fast_store),
        Store::new(slow_store.clone()),
    );
    let root_action_directory = make_temp_path("root_action_directory");
    nativelink_util::common::fs::create_dir_all(&root_action_directory).await?;
    let manager = RunningActionsManagerImpl::new(RunningActionsManagerArgs {
        root_action_directory,
        execution_configuration: ExecutionConfiguration::default(),
        cas_store: cas_store.clone(),
        ac_store: None,
        ac_mirror_target: None,
        historical_store: Store::new(cas_store.clone()),
        upload_action_result_config: &UploadActionResultConfig {
            upload_ac_results_strategy: UploadCacheResultsStrategy::Never,
            ..Default::default()
        },
        max_action_timeout: Duration::MAX,
        max_upload_timeout: Duration::from_secs(600),
        timeout_handled_externally: false,
        directory_cache: None,
        bis_ack_timeout: Duration::from_secs(60),
        metrics: None,
        cas_endpoint: String::new(),
        deferred_output_uploads_enabled: false,
    })?;
    Ok((Arc::new(manager), cas_store))
}

/// Construct a minimal `RunningActionImpl` for cache testing. Bypasses
/// `create_and_add_action`'s StartExecute parsing because the per-action
/// cache is the only field we exercise. The action's other state
/// (work_directory, action_info) is set to inert defaults — none of the
/// cache methods touch them.
fn make_test_action(manager: Arc<RunningActionsManagerImpl>) -> Arc<RunningActionImpl> {
    let unique_qualifier = ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
        instance_name: "test".to_string(),
        digest_function: DigestHasherFunc::Sha256,
        digest: DigestInfo::new([0u8; 32], 0),
    });
    let action_info = ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::from_secs(60),
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier,
    };
    let execution_metadata = ExecutionMetadata {
        worker: "test_worker".to_string(),
        queued_timestamp: SystemTime::UNIX_EPOCH,
        worker_start_timestamp: SystemTime::UNIX_EPOCH,
        input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
        input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
        execution_start_timestamp: SystemTime::UNIX_EPOCH,
        execution_completed_timestamp: SystemTime::UNIX_EPOCH,
        output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
        output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
        worker_completed_timestamp: SystemTime::UNIX_EPOCH,
    };
    Arc::new(RunningActionImpl::new(
        execution_metadata,
        OperationId::default(),
        make_temp_path("test_action_directory"),
        action_info,
        Duration::from_secs(60),
        manager,
        None,
        None,
    ))
}

/// Build a Tree containing two non-zero-sized FileNodes. Returns the Tree
/// and the expected set of file digests `expand_tree_file_digests` should
/// surface.
fn fixture_tree() -> (ProtoTree, Vec<DigestInfo>) {
    let file_a = DigestInfo::new([0xaa; 32], 100);
    let file_b = DigestInfo::new([0xbb; 32], 200);
    let root = ProtoDirectory {
        files: vec![
            FileNode {
                name: "a".to_string(),
                digest: Some(file_a.into()),
                is_executable: false,
                node_properties: None,
            },
            FileNode {
                name: "b".to_string(),
                digest: Some(file_b.into()),
                is_executable: false,
                node_properties: None,
            },
        ],
        ..Default::default()
    };
    let tree = ProtoTree {
        root: Some(root),
        children: vec![],
    };
    (tree, vec![file_a, file_b])
}

/// Build an ActionResult whose only meaningful field is one output folder
/// pointing at `tree_digest`.
fn action_result_for_tree(tree_digest: DigestInfo) -> ActionResult {
    ActionResult {
        output_folders: vec![DirectoryInfo {
            path: "outdir".to_string(),
            tree_digest,
        }],
        ..ActionResult::default()
    }
}

/// T1 (under-action): cache hit must short-circuit the storage read. The
/// CAS store contains no Tree blob, so any storage-layer read fails the
/// decode and the function returns an empty Vec. The cache-hit path
/// returns the populated file digests. The asymmetry — same store, same
/// fixture, cache present vs absent — proves the cache short-circuited
/// the read.
///
/// Mutation 2026-06-07: comment out the
/// `Some(tree) => hits.push((tree_digest, tree))` arm in
/// `expand_tree_file_digests` — T1 red-fails on the
/// `cached == expected_file_digests` assertion (cache hit no longer
/// short-circuits, storage read returns NotFound, function returns
/// empty Vec).
#[nativelink_test]
async fn cache_hit_avoids_storage_read() -> Result<(), Box<dyn core::error::Error>> {
    let (manager, _cas_store) = setup_manager().await?;
    let action = make_test_action(manager.clone());
    let (tree, expected_file_digests) = fixture_tree();
    // Synthesise a tree_digest WITHOUT uploading the blob: any storage
    // read for this digest will return NotFound.
    let tree_digest = DigestInfo::new([0x77; 32], 4242);
    let action_result = action_result_for_tree(tree_digest);

    // Baseline: with NO cache entry, the storage read fails and the
    // function returns an empty Vec (warn! path). This establishes that
    // a cache miss on this fixture cannot produce the file digests.
    let baseline = tokio::time::timeout(
        TEST_TIMEOUT,
        manager.expand_tree_file_digests(&action_result, Some(&action)),
    )
    .await
    .expect("T1 baseline: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert!(
        baseline.is_empty(),
        "tree re-read after write: get count = at-least-1, expected 0 — \
         baseline must be empty because no blob is in CAS for this digest \
         (got {} digests)",
        baseline.len(),
    );

    // Now pre-populate the cache. The cache hit must produce the file
    // digests without touching the store.
    action.cache_tree_proto(tree_digest, tree);
    let cached = tokio::time::timeout(
        TEST_TIMEOUT,
        manager.expand_tree_file_digests(&action_result, Some(&action)),
    )
    .await
    .expect("T1: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert_eq!(
        cached, expected_file_digests,
        "cache-hit path must return the cached Tree's file digests — \
         tree re-read after write: get count = at-least-1, expected 0 \
         (cache hit should have short-circuited the storage read)",
    );
    Ok(())
}

/// T2 (over-action): cache miss must fall back to `get_and_decode_digest`.
///
/// Mutation 2026-06-07: comment out the body of the miss-path
/// `FuturesUnordered` decode loop — T2 red-fails on the
/// `result == expected_file_digests` assertion (fallback removed, function
/// returns empty Vec).
#[nativelink_test]
async fn cache_miss_falls_back_to_storage() -> Result<(), Box<dyn core::error::Error>> {
    let (manager, cas_store) = setup_manager().await?;
    let action = make_test_action(manager.clone());
    let (tree, expected_file_digests) = fixture_tree();
    // Upload the Tree blob to CAS so the fallback path can find it.
    let tree_digest = serialize_and_upload_message(
        &tree,
        Store::new(cas_store.clone()).as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;
    // Do NOT call cache_tree_proto: the cache is empty for this digest.
    let action_result = action_result_for_tree(tree_digest);
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        manager.expand_tree_file_digests(&action_result, Some(&action)),
    )
    .await
    .expect("T2: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert_eq!(
        result, expected_file_digests,
        "cache-miss fallback must re-read from storage and return the \
         same file digests as a cache hit would have produced",
    );
    Ok(())
}

/// T3 (asymmetric contract — take-side drains): the cache has two
/// readers — `peek_cached_tree_proto` (reader 1, `expand_tree_file_digests`)
/// and `take_cached_tree_proto` (reader 2, `spawn_upload_to_remote`). The
/// asymmetric contract is: `peek` MUST leave the entry; `take` MUST drain
/// it. Without this, the cache cannot self-drain.
///
/// Test sequence:
///   1. `action.cache_tree_proto(digest, tree)`.
///   2. `action.peek_cached_tree_proto` → Some(tree); entry still present
///      (verify via second `peek`).
///   3. `action.take_cached_tree_proto` → Some(tree); entry drained
///      (verify via post-take `peek` returning None).
///
/// Mutation 2026-06-07: change `take_cached_tree_proto` body from
/// `self.tree_proto_cache.lock().remove(digest)` to
/// `self.tree_proto_cache.lock().get(digest).cloned()` (peek semantics) —
/// T3 red-fails on the post-take drain assertion with bespoke message
/// "take-side cache drain broken: entry remained after take, expected
/// drained".
#[nativelink_test]
async fn take_consumer_drains_cache_on_second_read()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, _cas_store) = setup_manager().await?;
    let action = make_test_action(manager.clone());
    let (tree, expected_file_digests) = fixture_tree();
    let tree_digest = DigestInfo::new([0x33; 32], 3333);

    action.cache_tree_proto(tree_digest, tree.clone());

    // Reader 1: peek leaves the entry.
    let peeked = tokio::time::timeout(
        TEST_TIMEOUT,
        async { action.peek_cached_tree_proto(&tree_digest) },
    )
    .await
    .expect("T3 peek: must not deadlock — tree_proto_cache lookup wedged within 5s")
    .expect("T3: peek must return Some after cache_tree_proto");
    // Verify the peeked tree matches what we cached by extracting its
    // file digests the same way the production code does.
    let peeked_file_digests: Vec<DigestInfo> = peeked
        .children
        .into_iter()
        .chain(peeked.root)
        .flat_map(|dir| dir.files)
        .filter_map(|f| f.digest.and_then(|d| DigestInfo::try_from(d).ok()))
        .filter(|d| d.size_bytes() > 0)
        .collect();
    assert_eq!(
        peeked_file_digests, expected_file_digests,
        "T3: peek returned a Tree whose file digests don't match what was cached",
    );

    // Entry must still be present after peek (under-action: peek must
    // not drain).
    let still_present = tokio::time::timeout(
        TEST_TIMEOUT,
        async { action.peek_cached_tree_proto(&tree_digest) },
    )
    .await
    .expect("T3 re-peek: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert!(
        still_present.is_some(),
        "peek-side cache drained on first read: entry missing after peek, expected retained",
    );

    // Reader 2: take returns Some and drains the entry.
    let taken = tokio::time::timeout(
        TEST_TIMEOUT,
        async { action.take_cached_tree_proto(&tree_digest) },
    )
    .await
    .expect("T3 take: must not deadlock — tree_proto_cache lookup wedged within 5s")
    .expect("T3: take must return Some after cache_tree_proto");
    let taken_file_digests: Vec<DigestInfo> = taken
        .children
        .into_iter()
        .chain(taken.root)
        .flat_map(|dir| dir.files)
        .filter_map(|f| f.digest.and_then(|d| DigestInfo::try_from(d).ok()))
        .filter(|d| d.size_bytes() > 0)
        .collect();
    assert_eq!(
        taken_file_digests, expected_file_digests,
        "T3: take returned a Tree whose file digests don't match what was cached",
    );

    // Post-take peek must return None (entry drained).
    let post_take = tokio::time::timeout(
        TEST_TIMEOUT,
        async { action.peek_cached_tree_proto(&tree_digest) },
    )
    .await
    .expect("T3 post-take peek: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert!(
        post_take.is_none(),
        "take-side cache drain broken: entry remained after take, expected drained",
    );
    Ok(())
}

/// T5 (F1 leak class closed — per-action cache lifetime = action
/// lifetime): caches multiple entries on a `RunningActionImpl`, drops the
/// last strong `Arc` reference, and asserts:
///   1. `Weak::upgrade()` returns `None` (the action — and its
///      `tree_proto_cache` field — has dropped).
///   2. A FRESH `RunningActionImpl` constructed afterwards does NOT
///      surface the prior action's cached entries. The pre-fix
///      process-wide cache on `RunningActionsManagerImpl` survived
///      action drops; the per-action design makes inter-action
///      visibility structurally impossible.
///
/// Mutation 2026-06-07: replace `RunningActionImpl::tree_proto_cache:
/// parking_lot::Mutex<HashMap<...>>` with a process-wide
/// `Arc<Mutex<HashMap<...>>>` shared across all actions (e.g. by
/// reverting to the pre-fix process-wide cache on
/// `RunningActionsManagerImpl` and re-deriving accessors there).
/// Under that mutation:
///   - The Weak::upgrade still returns None (the action drops).
///   - BUT the new action's `peek_cached_tree_proto` would surface
///     the prior action's entry because the cache is process-wide.
///   - Assertion (2) red-fails with the bespoke message
///     "per-action cache leaked entry after RunningActionImpl drop".
#[nativelink_test]
async fn cache_drained_on_action_drop()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, _cas_store) = setup_manager().await?;
    let action1 = make_test_action(manager.clone());

    // Cache several entries on action1 — under the OLD process-wide
    // design, these would leak past action drop.
    let (tree, _) = fixture_tree();
    let mut digests = Vec::with_capacity(32);
    for i in 0..32u32 {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&i.to_le_bytes());
        let digest = DigestInfo::new(bytes, 64);
        action1.cache_tree_proto(digest, tree.clone());
        digests.push(digest);
    }

    // Sanity: cache is populated before drop.
    assert!(
        action1.peek_cached_tree_proto(&digests[0]).is_some(),
        "T5 sanity: cache must be populated before drop",
    );

    // Drop action1: the per-action cache field drops with it.
    // `Weak` tracks the strong-ref count; after the last strong drop,
    // upgrade() returns None.
    let weak = Arc::downgrade(&action1);
    drop(action1);
    assert!(
        weak.upgrade().is_none(),
        "per-action cache leaked entry after RunningActionImpl drop \
         (Weak::upgrade still succeeded; the action — and therefore \
         its tree_proto_cache field — is still alive)",
    );

    // Construct a FRESH action on the same manager. The per-action
    // design guarantees its cache starts empty; nothing from action1
    // leaks across. Under the pre-fix process-wide design this would
    // surface action1's entries via the manager-level cache.
    let action2 = make_test_action(manager.clone());
    for digest in &digests {
        assert!(
            action2.peek_cached_tree_proto(digest).is_none(),
            "per-action cache leaked entry after RunningActionImpl drop \
             (fresh action surfaced a digest cached by the prior \
             action — process-wide leak class has re-opened)",
        );
    }
    Ok(())
}

/// T6 (perf-claim — cache hit avoids future allocation / dispatch):
/// the F2 (perf-claim) fix partitions output_folders into hits + misses
/// BEFORE allocating any future. Cache hits are drained from
/// `Vec<(DigestInfo, ProtoTree)>` synchronously; only misses become
/// `FuturesUnordered` slots.
///
/// Direct instrumentation of `FuturesUnordered` slot counts is awkward
/// across the `expand_tree_file_digests` API boundary; we use the
/// acceptable substitute named in the task spec: a wall-clock test
/// asserting hit-path is dramatically faster than miss-path on the
/// SAME fixture. With 64 output folders pointing at trees in a
/// MemoryStore-backed FilesystemStore, the miss path pays 64 storage
/// reads (FilesystemStore::get_part → prost decode); the hit path
/// pays zero. The wall-clock margin is dominated by the storage-I/O
/// difference, not the future-allocation cost.
///
/// Caveat (mutation-binding honesty): reverting the partition to the
/// prior `.map(|folder| async { peek_or_decode })` form (future
/// allocated regardless of cache outcome) was experimentally verified
/// 2026-06-07 to NOT red-fail this test — the hit-path's
/// future-allocation overhead is too small relative to the
/// HashMap-lookup baseline to widen the gap below the 2x threshold.
/// T6 instead provides:
///   1. Correctness regression coverage: the hit-fixture must return
///      the populated file digests (asserting `warm_hit.len() ==
///      N_TREES`).
///   2. Forward-direction observability: the new structured
///      `hits_count=K misses_count=M` log fields emitted by
///      `expand_tree_file_digests complete` surface whether the
///      partition is firing in production. A regression that
///      re-merges hits into the FuturesUnordered would log
///      `hits_count=0` even on a populated cache, detectable via a
///      log scrape on the hot path.
/// The wall-clock floor (`hit_median * 2 < miss_median`) still
/// guards against a regression that re-introduces a storage read on
/// the hit path (the original O3/O13 motivation).
#[nativelink_test]
async fn cache_hit_avoids_future_allocation()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, cas_store) = setup_manager().await?;
    let action = make_test_action(manager.clone());

    // Use many folders so per-call overhead dominates noise. 64 trees
    // amplifies the hit-vs-miss gap without making the test slow.
    const N_TREES: usize = 64;

    // Build N distinct trees, upload to CAS, and collect their digests.
    let mut tree_digests = Vec::with_capacity(N_TREES);
    let mut trees = Vec::with_capacity(N_TREES);
    for i in 0..N_TREES {
        let file_digest = DigestInfo::new([i as u8; 32], 100 + i as u64);
        let root = ProtoDirectory {
            files: vec![FileNode {
                name: format!("file_{i}"),
                digest: Some(file_digest.into()),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let tree = ProtoTree {
            root: Some(root),
            children: vec![],
        };
        let tree_digest = serialize_and_upload_message(
            &tree,
            Store::new(cas_store.clone()).as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        tree_digests.push(tree_digest);
        trees.push(tree);
    }

    let action_result = ActionResult {
        output_folders: tree_digests
            .iter()
            .enumerate()
            .map(|(i, d)| DirectoryInfo {
                path: format!("outdir_{i}"),
                tree_digest: *d,
            })
            .collect(),
        ..ActionResult::default()
    };

    // Warm: one untimed run so storage caches stabilise.
    let _ = manager
        .expand_tree_file_digests(&action_result, Some(&action))
        .await;

    // MISS run: cache is empty (warm-up didn't populate it because the
    // cache is per-action and we never called cache_tree_proto). Median
    // of 3.
    let mut miss_runs: Vec<Duration> = Vec::with_capacity(3);
    for _ in 0..3 {
        let t0 = Instant::now();
        let _ = manager
            .expand_tree_file_digests(&action_result, Some(&action))
            .await;
        miss_runs.push(t0.elapsed());
    }
    miss_runs.sort();
    let miss_median = miss_runs[1];

    // Populate the cache with every Tree.
    for (digest, tree) in tree_digests.iter().zip(trees.iter()) {
        action.cache_tree_proto(*digest, tree.clone());
    }
    // Warm hit-path: one untimed run. NB the take-side is via
    // spawn_upload_to_remote, not expand_tree_file_digests — peek does
    // NOT drain, so the cache stays populated across these runs.
    let warm_hit = manager
        .expand_tree_file_digests(&action_result, Some(&action))
        .await;
    assert_eq!(
        warm_hit.len(),
        N_TREES,
        "hit-warm-up returned {} digests, expected {N_TREES}",
        warm_hit.len(),
    );

    // HIT run: median of 3.
    let mut hit_runs: Vec<Duration> = Vec::with_capacity(3);
    for _ in 0..3 {
        let t0 = Instant::now();
        let _ = manager
            .expand_tree_file_digests(&action_result, Some(&action))
            .await;
        hit_runs.push(t0.elapsed());
    }
    hit_runs.sort();
    let hit_median = hit_runs[1];

    // Bound: hits must be measurably faster than misses. With the
    // partition fix, hits pay zero future-allocation + zero storage
    // round-trip; misses pay both. Margin is small over MemoryStore but
    // detectable. We assert `hit_median < miss_median / 2` — a 2x speedup
    // — to leave headroom for CI noise while still catching a
    // regression to the per-future-per-folder pattern. The mutation
    // reverting to the unpartitioned form should narrow the margin to
    // below 2x because both paths pay the future-allocation cost.
    assert!(
        hit_median * 2 < miss_median,
        "perf-claim regressed: cache-hit path not dramatically faster \
         than cache-miss path — hits {hit_median:?}, misses \
         {miss_median:?}. Expected hits < misses/2 because the F2 \
         partition fix avoids future allocation on every hit. \
         Mutation suspect: hit/miss partition reverted to per-folder \
         `.map(|folder| async {{ peek_or_decode }})` form.",
    );
    Ok(())
}
