// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! #FL-688 W6 — action-output upload retry-until-durable, PRODUCTION
//! COMPOSITION.
//!
//! The in-lib W6 tests (`w6_sync_giveup_on_transient_arms_backstop` etc.)
//! drive the give-up DECISION through a test helper (`w6_giveup_requeue`)
//! that RE-IMPLEMENTS the production gate
//! `if should_requeue_on_giveup(e, flag) { requeue_failed_push(digest) }`.
//! They do NOT exercise the REAL give-up arm at
//! `running_actions_manager.rs:6914` inside `spawn_upload_to_remote`, so a
//! regression that passes the wrong `flag`/`e`/`digest` to the predicate, or
//! drops the `requeue_failed_push` call at the real site, would NOT be
//! caught by them. W4 already drives its real handler; this is W4-parity for
//! W6.
//!
//! This test drives the REAL `spawn_upload_to_remote` body end-to-end (via
//! the `spawn_upload_to_remote_for_test` JoinHandle seam — identical to the
//! production entry point except it returns the handle instead of dropping
//! it) and asserts the give-up arm re-queues the output digest into the
//! shared `failed_slow_writes` set.
//!
//! ## Production composition (the seam this test crosses)
//!
//! Producer:    `RunningActionsManagerImpl::spawn_upload_to_remote_impl` —
//!              the real per-digest synchronous-mode upload retry loop,
//!              including the #FL-688 W6 give-up arm
//!              (`running_actions_manager.rs:6914`):
//!              `if should_requeue_on_giveup(&e, flag) {
//!                 cas_store_ref.requeue_failed_push(digest) }`.
//! Store chain: real `FastSlowStore` whose fast tier is a real
//!              `FilesystemStore` (holds the output bytes the loop reads
//!              back) over a `RejectingSlowStore` that fails every upload
//!              with `Code::Unavailable` — the RETRYABLE class that
//!              `classify_upload_error` maps to `Retry`, so synchronous mode
//!              exhausts `SYNC_MAX_RETRIES = 4` and gives up
//!              (`flag == false`) — the exact arm under test.
//! Predicate:   real `should_requeue_on_giveup` (single source of truth,
//!              `running_actions_manager.rs:1330`).
//! Re-queue:    real `FastSlowStore::requeue_failed_push` on the SAME
//!              `cas_store` FSS the loop captured.
//! Observation: `FastSlowStore::failed_slow_writes_contains` — the set the
//!              reconnect drainer (`drain_failed_digests`) reads.
//!
//! `start_paused = true` collapses the 1 s→cap synchronous backoff ramp
//! (~15 s wall-clock across four attempts) to instant via tokio's
//! auto-advancing clock — no `sleep`-as-synchronization, and the JoinHandle
//! `.await` is the deterministic completion signal (the per-digest loop
//! finishes before the detached 30 s phase0-commit spawn, so awaiting the
//! handle observes the give-up arm directly).

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_util::action_messages::{ActionResult, FileInfo, NameOrPath};
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningActionsManagerArgs, RunningActionsManagerImpl,
};

/// Slow store that rejects every upload with `Code::Unavailable` — the
/// RETRYABLE class (`classify_upload_error == Retry`). Drives the
/// synchronous-mode loop to `SYNC_MAX_RETRIES` exhaustion → give-up. Reads
/// report missing/NotFound (the server does not hold the blob it is asking
/// for).
#[derive(MetricsComponent)]
struct RejectingSlowStore {
    update_invocations: AtomicUsize,
}

impl RejectingSlowStore {
    const fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoreDriver for RejectingSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut() {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Drain so a streaming-upload producer half does not deadlock on a
        // full channel before we return the Err.
        while let Ok(chunk) = reader.recv().await {
            if chunk.is_empty() {
                break;
            }
        }
        // Unavailable == retryable; synchronous mode keeps retrying until
        // SYNC_MAX_RETRIES, then gives up — exactly the W6 give-up arm.
        Err(make_err!(
            Code::Unavailable,
            "RejectingSlowStore: server transiently rejected the output upload"
        ))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::NotFound,
            "RejectingSlowStore: get_part not supported (output bytes live on the fast tier)"
        ))
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

default_health_status_indicator!(RejectingSlowStore);

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string()),
        rand::random::<u64>(),
        data,
    )
}

/// Compose a production-shaped CAS `FastSlowStore`: real `FilesystemStore`
/// fast tier (the same concrete type the worker downcasts to in
/// `RunningActionsManagerImpl::new_with_callbacks`) over a
/// `RejectingSlowStore` slow tier. Returns the concrete fast store so the
/// test can seed the output bytes onto it.
async fn make_fss_with_rejecting_slow()
-> Result<(Arc<FilesystemStore>, Arc<FastSlowStore>), Error> {
    let fast_config = FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: None,
        ..Default::default()
    };
    let fast_store = FilesystemStore::new(&fast_config).await?;
    let slow_store = Store::new(Arc::new(RejectingSlowStore::new()));
    let cas_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_config),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fast_store.clone()),
        slow_store,
    );
    Ok((fast_store, cas_store))
}

async fn build_manager(
    cas_store: Arc<FastSlowStore>,
) -> Result<Arc<RunningActionsManagerImpl>, Error> {
    let root_action_directory = make_temp_path("root_action_directory");
    nativelink_util::common::fs::create_dir_all(&root_action_directory).await?;
    Ok(Arc::new(RunningActionsManagerImpl::new(
        RunningActionsManagerArgs {
            root_action_directory,
            execution_configuration: ExecutionConfiguration::default(),
            cas_store: cas_store.clone(),
            ac_store: None,
            ac_mirror_target: None,
            historical_store: Store::new(cas_store),
            upload_action_result_config:
                &nativelink_config::cas_server::UploadActionResultConfig {
                    upload_ac_results_strategy:
                        nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
                    ..Default::default()
                },
            max_action_timeout: Duration::MAX,
            max_upload_timeout: Duration::from_secs(600),
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
            // SYNCHRONOUS mode — the only mode that GIVES UP on a retryable
            // class (deferred mode retries forever). The W6 give-up arm is
            // a synchronous-mode-only path.
            deferred_output_uploads_enabled: false,
        },
    )?))
}

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// W6 PRODUCTION COMPOSITION: a SYNCHRONOUS-mode action-output upload whose
/// remote write keeps failing with a RETRYABLE error MUST, on
/// `SYNC_MAX_RETRIES` exhaustion, re-queue the output digest into
/// `failed_slow_writes` via the REAL `spawn_upload_to_remote` give-up arm —
/// never silently drop the worker's single copy.
///
/// `start_paused = true` so the synchronous backoff ramp auto-advances
/// (no real wall-clock wait, no sleep-as-synchronization). `current_thread`
/// is required for tokio's auto-advancing paused clock.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn w6_real_giveup_arm_requeues_output_digest()
-> Result<(), Box<dyn core::error::Error>> {
    // A small (<1 MiB) output blob takes the pre-read + `update_oneshot`
    // batch upload branch; the same give-up arm covers the streaming branch.
    let payload = Bytes::from_static(b"w6_output_blob_must_retry_until_durable");
    let digest = mk_digest(0x71, payload.len());

    let (fast_store, cas_store) = make_fss_with_rejecting_slow().await?;

    // Seed the output bytes onto the FAST tier (FilesystemStore) so the
    // upload loop's pre-read (`cas_store.get_part_unchunked`) succeeds and
    // the loop reaches the remote-write step that the slow store rejects.
    fast_store
        .as_pin()
        .update_oneshot(digest.into(), payload.clone())
        .await?;

    let manager = build_manager(cas_store.clone()).await?;

    let action_result = ActionResult {
        output_files: vec![FileInfo {
            name_or_path: NameOrPath::Name("out".to_string()),
            digest,
            is_executable: false,
        }],
        ..Default::default()
    };

    // Drive the REAL give-up arm. `_for_test` returns the upload task's
    // JoinHandle so we can await it deterministically (production drops it).
    let handle = manager
        .spawn_upload_to_remote_for_test(&action_result, None)
        .expect("upload task must be scheduled (slow store is writable, one output digest present)");

    // The per-digest retry loop (incl. the give-up arm) completes before the
    // detached 30 s phase0-commit spawn, so awaiting the handle observes the
    // re-queue. The few-second timeout is the deadlock detector; under
    // paused time the four backoff sleeps auto-advance instantly.
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("spawn_upload_to_remote task must not deadlock — W6 retry-until-durable")
        .expect("spawn_upload_to_remote task must not panic");

    assert!(
        cas_store.failed_slow_writes_contains(&digest),
        "W6: a synchronous-mode output upload that exhausts SYNC_MAX_RETRIES on a \
         retryable (Unavailable) error MUST re-queue the output digest into \
         failed_slow_writes via the REAL spawn_upload_to_remote give-up arm — \
         dropping the call at the real site, or passing the wrong flag/e/digest, \
         silently loses the worker's only copy (#FL-688 W6 real-arm regression)",
    );

    Ok(())
}
