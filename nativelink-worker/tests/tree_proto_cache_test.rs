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

//! #O3/O13: regression test for the in-memory Tree-proto cache that lets
//! `RunningActionsManagerImpl::expand_tree_file_digests` and
//! `spawn_upload_to_remote` skip a storage-layer round trip when reading
//! back a Tree proto that the same action just wrote.
//!
//! - **T1 (under-action, cache hit avoids storage read):** pre-populate the
//!   manager's tree-proto cache with a Tree whose files we know in advance,
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
//! - **T4 (CAPPED-AT contract, over-cap insert drops silently and read
//!   falls back to CAS):** the cap branch at
//!   `running_actions_manager.rs:4870` is `if cache.len() >=
//!   TREE_PROTO_CACHE_MAX_ENTRIES { return; }`. Populate to the cap with
//!   filler digests, then attempt to insert a 1025th digest whose blob IS
//!   in CAS. Assertions: the 1025th `peek` returns `None` (over-cap
//!   drop), and `expand_tree_file_digests` for that digest still returns
//!   the correct file digests via the CAS-fallback path. Mutation
//!   2026-06-07: change `>= TREE_PROTO_CACHE_MAX_ENTRIES` to `> 99999`
//!   (effectively no cap) — T4 red-fails because the 1025th entry is
//!   retained and `peek` returns `Some`.

use core::time::Duration;
use std::sync::Arc;

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
use nativelink_util::action_messages::{ActionResult, DirectoryInfo};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningActionsManagerArgs, RunningActionsManagerImpl,
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
    })?;
    Ok((Arc::new(manager), cas_store))
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
/// `if let Some(tree) = self.peek_cached_tree_proto(...) { Ok(tree) }`
/// branch in `expand_tree_file_digests` — T1 red-fails on the
/// `cached == expected_file_digests` assertion (cache hit no longer
/// short-circuits, storage read returns NotFound, function returns
/// empty Vec).
#[nativelink_test]
async fn cache_hit_avoids_storage_read() -> Result<(), Box<dyn core::error::Error>> {
    let (manager, _cas_store) = setup_manager().await?;
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
        manager.expand_tree_file_digests(&action_result),
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
    manager.cache_tree_proto(tree_digest, tree);
    let cached = tokio::time::timeout(
        TEST_TIMEOUT,
        manager.expand_tree_file_digests(&action_result),
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
/// Mutation 2026-06-07: comment out the `else { get_and_decode_digest(...) }`
/// arm in `expand_tree_file_digests` — T2 red-fails on the
/// `result == expected_file_digests` assertion (fallback removed, function
/// returns empty Vec).
#[nativelink_test]
async fn cache_miss_falls_back_to_storage() -> Result<(), Box<dyn core::error::Error>> {
    let (manager, cas_store) = setup_manager().await?;
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
        manager.expand_tree_file_digests(&action_result),
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
/// it. Without this, the cache cannot self-drain and the cap branch
/// becomes the only bound — defeating the "steady-state size = one
/// in-flight publish" design property.
///
/// Test sequence:
///   1. `cache_tree_proto(digest, tree)`.
///   2. `peek_cached_tree_proto` → Some(tree); entry still present
///      (verify via second `peek`).
///   3. `take_cached_tree_proto` → Some(tree); entry drained
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
    let (tree, expected_file_digests) = fixture_tree();
    let tree_digest = DigestInfo::new([0x33; 32], 3333);

    manager.cache_tree_proto(tree_digest, tree.clone());

    // Reader 1: peek leaves the entry.
    let peeked = tokio::time::timeout(
        TEST_TIMEOUT,
        async { manager.peek_cached_tree_proto(&tree_digest) },
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
        async { manager.peek_cached_tree_proto(&tree_digest) },
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
        async { manager.take_cached_tree_proto(&tree_digest) },
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
        async { manager.peek_cached_tree_proto(&tree_digest) },
    )
    .await
    .expect("T3 post-take peek: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert!(
        post_take.is_none(),
        "take-side cache drain broken: entry remained after take, expected drained",
    );
    Ok(())
}

/// T4 (CAPPED-AT contract — over-cap insert drops silently, read falls
/// back to CAS): the production cache is bounded by
/// `TREE_PROTO_CACHE_MAX_ENTRIES = 1024` at
/// `running_actions_manager.rs:4784`. The cap branch at `:4870` reads
/// `if cache.len() >= TREE_PROTO_CACHE_MAX_ENTRIES { return; }` and is
/// otherwise untested. Per CLAUDE.md "unbounded buffers" rule the cap
/// behavior MUST be falsifiable.
///
/// Test sequence:
///   1. Insert 1024 filler digests via `cache_tree_proto` (cache full).
///   2. Upload a 1025th distinct Tree to CAS.
///   3. Attempt to insert it via `cache_tree_proto` — over-cap drop.
///   4. Assert `peek_cached_tree_proto(digest_1025)` returns `None`.
///   5. Assert `expand_tree_file_digests(action_result_for(digest_1025))`
///      still returns the correct file digests via the CAS-fallback path.
///
/// Mutation 2026-06-07: change the cap branch at `:4870` from
/// `if cache.len() >= TREE_PROTO_CACHE_MAX_ENTRIES` to
/// `if cache.len() > 99999` (effectively no cap) — T4 red-fails because
/// the 1025th entry is retained and `peek` returns `Some` with bespoke
/// message "over-cap drop broken: 1025th entry retained OR fallback not
/// triggered".
#[nativelink_test]
async fn over_cap_drops_silently_and_read_falls_back()
-> Result<(), Box<dyn core::error::Error>> {
    let (manager, cas_store) = setup_manager().await?;

    // Fill the cache to the cap (1024 entries) with cheap filler trees.
    // Use distinct digests so each insert succeeds.
    let filler_tree = ProtoTree {
        root: Some(ProtoDirectory::default()),
        children: vec![],
    };
    for i in 0..1024u32 {
        let mut digest_bytes = [0u8; 32];
        digest_bytes[..4].copy_from_slice(&i.to_le_bytes());
        // High byte to keep these distinct from the 1025th digest below.
        digest_bytes[31] = 0x42;
        let digest = DigestInfo::new(digest_bytes, 64);
        manager.cache_tree_proto(digest, filler_tree.clone());
    }

    // Upload a distinct 1025th Tree to CAS so the fallback path can find
    // it. Use the fixture so we have an `expected_file_digests` to assert
    // against.
    let (tree, expected_file_digests) = fixture_tree();
    let tree_digest = serialize_and_upload_message(
        &tree,
        Store::new(cas_store.clone()).as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    // Attempt to insert past the cap; production code MUST drop silently.
    manager.cache_tree_proto(tree_digest, tree);

    // Over-cap drop assertion: peek MUST return None for the 1025th
    // digest because the insert was rejected by the cap branch.
    let peeked = tokio::time::timeout(
        TEST_TIMEOUT,
        async { manager.peek_cached_tree_proto(&tree_digest) },
    )
    .await
    .expect("T4 peek: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert!(
        peeked.is_none(),
        "over-cap drop broken: 1025th entry retained OR fallback not triggered \
         (peek returned Some, expected None because cap fired)",
    );

    // Fallback-triggered assertion: with the cache having dropped the
    // 1025th entry, `expand_tree_file_digests` MUST still return the
    // correct file digests via the `get_and_decode_digest` fallback
    // (the blob is in CAS).
    let action_result = action_result_for_tree(tree_digest);
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        manager.expand_tree_file_digests(&action_result),
    )
    .await
    .expect("T4 fallback: must not deadlock — tree_proto_cache lookup wedged within 5s");
    assert_eq!(
        result, expected_file_digests,
        "over-cap drop broken: 1025th entry retained OR fallback not triggered \
         (expected fallback to return the CAS-stored Tree's file digests)",
    );
    Ok(())
}
