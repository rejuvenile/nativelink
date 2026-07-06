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

//! Worker-side integration tests for the speculative input pre-fetch feature.
//!
//! These DRIVE the real `Update::PrefetchInputs` worker arm end-to-end through a
//! `LocalWorkerImpl` + a `MockRunningActionsManager` whose `get_directory_cache()`
//! returns a real `DirectoryCache` over a production-composed `FastSlowStore` —
//! replacing the prior T5/T6 which were stdlib tautologies (they re-implemented
//! `AtomicBool::compare_exchange` / `u64::min` in the test body and asserted on
//! the re-implementation, exercising ZERO production code; testing-czar C4).
//!
//!  arm-adoption – a PrefetchInputs makes the real arm PRE-CONSTRUCT the input
//!                 root, so a later get_or_create at dispatch is a HIT (the
//!                 end-to-end "scheduler emits → worker prewarms → adoption").
//!  arm-no-cache – a PrefetchInputs when get_directory_cache()==None is handled
//!                 gracefully (arm skips, worker survives + keeps processing).
//!
//! The single-in-flight AtomicBool, the self-fired TTL release, and the
//! Aborted→speculative_prefetch_aborted fail-fast are exercised by the arm
//! internally on these paths; the worker-LOCAL counters (busy_drop/aborted) are
//! not test-observable (they live on the worker's private Metrics, not the
//! actions-manager's), so the OBSERVABLE contract asserted here is the
//! pre-construct adoption + graceful-skip. The pin/TTL mechanism itself is
//! mutation-verified in the directory_cache mod tests
//! (t1_prewarm_makes_real_dispatch_a_hit, t7_prewarm_propagates_aborted_yield_first).

use std::collections::HashMap;
use std::sync::Arc;

use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreSpec};
use nativelink_error::{Error, make_input_err};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, FileNode,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, PrefetchInputs, UpdateForWorker,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::directory_cache::{DirectoryCache, DirectoryCacheConfig};
use prost::Message;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}
use utils::local_worker_test_utils::setup_local_worker;

/// Build a real `DirectoryCache` over a `FastSlowStore` (both tiers MemoryStore)
/// seeded with a one-file directory tree. Returns the cache + the input-root
/// digest a `PrefetchInputs` should carry. Mirrors the directory_cache mod-test
/// `setup_fast_slow_cache` so the arm drives production composition.
async fn build_seeded_directory_cache(
    cache_root: std::path::PathBuf,
) -> (Arc<DirectoryCache>, DigestInfo) {
    let file_digest = DigestInfo::try_new(
        "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
        13,
    )
    .unwrap();
    let directory = ProtoDirectory {
        files: vec![FileNode {
            name: "test.txt".to_string(),
            digest: Some(file_digest.into()),
            is_executable: false,
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut dir_data = Vec::new();
    directory.encode(&mut dir_data).unwrap();
    let dir_digest = DigestInfo::try_new(
        "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        dir_data.len() as i64,
    )
    .unwrap();

    // Slow tier (the DirectoryCache's cas_store) holds the tree + file.
    let store = Store::new(MemoryStore::new(&MemorySpec::default()));
    store
        .update_oneshot(file_digest, b"Hello, World!".to_vec().into())
        .await
        .unwrap();
    store
        .update_oneshot(dir_digest, dir_data.clone().into())
        .await
        .unwrap();

    // FastSlowStore fast tier seeded with the same blobs (resolve fetches
    // through the FSS).
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    fast.update_oneshot(file_digest, b"Hello, World!".to_vec().into())
        .await
        .unwrap();
    fast.update_oneshot(dir_digest, dir_data.into()).await.unwrap();
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss: Arc<FastSlowStore> = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: Default::default(),
            slow_direction: Default::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast,
        slow,
    );

    let config = DirectoryCacheConfig {
        max_entries: 10,
        max_size_bytes: 1024 * 1024,
        cache_root,
        direct_use_mode: false,
    };
    let cache = Arc::new(DirectoryCache::new(config, store, Some(fss)).await.unwrap());
    (cache, dir_digest)
}

/// Drive the real `Update::PrefetchInputs` arm: after the worker receives a
/// PrefetchInputs for `dir_digest`, the injected DirectoryCache must have the
/// input-root PRE-CONSTRUCTED, so a get_or_create at dispatch is a HIT. The
/// arm's single-in-flight guard, get_directory_cache lookup, detached spawn, and
/// prewarm call are all exercised on this path.
#[nativelink_test]
async fn arm_prefetch_inputs_preconstructs_input_root() -> Result<(), Error> {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let (cache, dir_digest) =
        build_seeded_directory_cache(temp_dir.path().join("cache")).await;

    let mut test_context = setup_local_worker(HashMap::new()).await;
    // Inject the real cache BEFORE the worker processes any PrefetchInputs.
    test_context.actions_manager.set_directory_cache(Arc::clone(&cache));

    let streaming_response = test_context.maybe_streaming_response.take().unwrap();
    // Drive the worker's connect handshake (props shape is asserted by the
    // dedicated local_worker_test; here we just need the worker registered).
    let _props = test_context
        .client
        .expect_connect_worker(Ok(streaming_response))
        .await;
    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    // Register (ConnectionResult) so the worker accepts updates.
    tx_stream
        .send(hyper::body::Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::ConnectionResult(ConnectionResult {
                    worker_id: "spec_prefetch_worker".to_string(),
                })),
            })
            .unwrap(),
        ))
        .await
        .map_err(|e| make_input_err!("send ConnectionResult failed: {e:?}"))?;

    // Send the PrefetchInputs for the input-root digest.
    tx_stream
        .send(hyper::body::Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::PrefetchInputs(PrefetchInputs {
                    operation_id: "op-arm-adoption".to_string(),
                    input_root_digest: Some(dir_digest.into()),
                    missing_digest_peers: vec![],
                    ttl_s: 60,
                })),
            })
            .unwrap(),
        ))
        .await
        .map_err(|e| make_input_err!("send PrefetchInputs failed: {e:?}"))?;

    // Wait for the arm's DETACHED prewarm to create the cache entry WITHOUT
    // constructing it ourselves: poll `stats().entries` (get_or_create would
    // self-construct on a MISS and make the test vacuous — the arm-prewarm and a
    // poll-construct are indistinguishable via get_or_create's HIT/MISS bool).
    // 5s timeout = never-warmed detector.
    let deadline = tokio::time::Instant::now() + core::time::Duration::from_secs(5);
    let mut warmed = false;
    while tokio::time::Instant::now() < deadline {
        if cache.stats().await.entries >= 1 {
            warmed = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(
        warmed,
        "arm-adoption (2026-07-05): the Update::PrefetchInputs worker arm did NOT pre-construct \
         the input-root entry — the cache stayed empty (0 entries) for 5s. The arm's prewarm \
         (single-inflight guard → get_directory_cache → prewarm) is broken. \
         (Mutation: skip the arm's prewarm call → this red-fails.)"
    );

    // Now EXACTLY ONE dispatch: because the arm already built the entry, this
    // MUST be a HIT (Ok(true)), not a construct-at-dispatch (Ok(false)). This is
    // the adoption contract: the real StartAction's get_or_create is a HIT.
    let dest = temp_dir.path().join("dispatch_dest");
    let hit = cache
        .get_or_create(dir_digest, &dest)
        .await
        .expect("get_or_create at dispatch returned Err");
    assert!(
        hit,
        "arm-adoption (2026-07-05): the first get_or_create at dispatch was a MISS (construct \
         ran at dispatch), not a HIT — the arm's pre-constructed entry was not adopted."
    );
    assert!(
        dest.join("test.txt").exists(),
        "arm-adoption: the adopted entry materialised an INCOMPLETE tree"
    );

    drop(tx_stream);
    Ok(())
}

/// The arm must handle `get_directory_cache() == None` gracefully: it skips the
/// prefetch, releases the single-in-flight guard, and the worker keeps running
/// (a subsequent message is still processed — here a disconnect that triggers
/// kill_all). Exercises the arm's no-cache early-out + inflight release.
#[nativelink_test]
async fn arm_prefetch_inputs_no_directory_cache_is_graceful() -> Result<(), Error> {
    // Default mock returns None for get_directory_cache().
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();
    let _props = test_context
        .client
        .expect_connect_worker(Ok(streaming_response))
        .await;
    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    tx_stream
        .send(hyper::body::Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::ConnectionResult(ConnectionResult {
                    worker_id: "spec_prefetch_nocache".to_string(),
                })),
            })
            .unwrap(),
        ))
        .await
        .map_err(|e| make_input_err!("send ConnectionResult failed: {e:?}"))?;

    // PrefetchInputs with no DirectoryCache on the worker → arm must skip.
    tx_stream
        .send(hyper::body::Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::PrefetchInputs(PrefetchInputs {
                    operation_id: "op-no-cache".to_string(),
                    input_root_digest: Some(
                        DigestInfo::try_new(
                            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                            42,
                        )
                        .unwrap()
                        .into(),
                    ),
                    missing_digest_peers: vec![],
                    ttl_s: 60,
                })),
            })
            .unwrap(),
        ))
        .await
        .map_err(|e| make_input_err!("send PrefetchInputs failed: {e:?}"))?;

    // The worker must still be alive and processing: dropping the stream must
    // trigger kill_all (proves the arm's skip did not wedge the recv loop).
    drop(tx_stream);
    tokio::time::timeout(
        core::time::Duration::from_secs(5),
        test_context.actions_manager.expect_kill_all(),
    )
    .await
    .expect(
        "arm-no-cache (2026-07-05): after a PrefetchInputs with no DirectoryCache, the worker \
         recv loop wedged — kill_all not called on disconnect within 5s (the no-cache early-out \
         must release the inflight guard + continue, not block the loop)",
    );

    Ok(())
}
