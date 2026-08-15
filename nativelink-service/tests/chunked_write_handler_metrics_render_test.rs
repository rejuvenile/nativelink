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

//! `#fl1786-server-side-digest-function-proving`: `ChunkedWriteHandlerMetrics`
//! renders, and every literal name is pinned.
//!
//! **Why this file exists.** Until this change `ChunkedWriteHandlerMetrics`
//! was constructed per-handler (`chunked_write_handler.rs`,
//! `Arc::new(ChunkedWriteHandlerMetrics::default())`) and **never handed to
//! any `MetricsRegistry`** — `src/bin/nativelink.rs` registers the store
//! manager, the scheduler trees, `worker_api` and the process-singleton
//! `ChunkingMetrics`, but nothing ever registered the chunked-write handler.
//! Every counter on it was therefore DARK on `/metrics`: computed, bumped,
//! and unreadable. That is the exact trap
//! `cas_server::register_chunking_metrics` was written to avoid for the
//! sibling counters, and it is why the new `digest_func_proven_total`
//! engaged-mechanism signal would have been worthless without the
//! registration this test guards.
//!
//! **Why names and not just presence.** A recent review showed that deleting
//! both of a change's counter `publish!` blocks left 297/297 tests green,
//! because nothing pinned the rendered strings. Metric names are a contract
//! with dashboards and alerts, so this file asserts the EXACT rendered line
//! for every field of the struct — a rename or a dropped field reds here.
//!
//! **Why it drives `install_chunked_write_handler` and not a hand-copied
//! registration.** The first version of this file built its own registry and
//! re-typed the `metrics_registry.register(..)` call that
//! `src/bin/nativelink.rs` makes. A reviewer's M8 mutation then DELETED that
//! production block outright and this suite stayed 6/6 GREEN — a rendering
//! test, not a wiring test, pinning a copy of the thing it claimed to guard.
//! The registration now lives in
//! `chunked_write_handler::install_chunked_write_handler`, which performs the
//! registry registration and the dispatch-map insert as one operation, and
//! that is the function both the binary and this file call. Deleting the
//! `register` inside it reds `installing_a_handler_registers_its_metrics`
//! below.
//!
//! **What is STILL uncovered, stated because the previous version of this
//! comment implied otherwise.** The ORIGINAL M8 — deleting the whole
//! `install_chunked_write_handler(..)` call from `src/bin/nativelink.rs:1200`
//! — still reds nothing. That file is in the ROOT crate, which
//! `cargo test -p nativelink-service` does not compile, and the root crate's
//! only integration tests (`execute_peer_sharing_test`,
//! `quic_reconnect_selfheal_test`) do not touch chunked wiring. The
//! compensating argument — that the deletion also removes the handler from
//! the dispatch map, taking `CasExtensionsServer` routing down loudly — is
//! read off `chunked_write_handler.rs:2009`/`:2034` and is NOT itself tested.
//! This suite pins the SEAM, not the binary's use of it.

#![cfg(feature = "chunked_fast_slow")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use nativelink_config::stores::FilesystemSpec;
use nativelink_macro::nativelink_test;
use nativelink_service::chunked_write_handler::{
    ChunkedWriteHandler, ChunkedWriteInFlight, chunked_write_metrics_prefix,
    install_chunked_write_handler,
};
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};

/// Every field of `ChunkedWriteHandlerMetrics`, as the metric name it
/// renders to under the production prefix. Adding a field to the struct
/// without adding it here leaves it unpinned; the completeness assertion
/// below is what makes that visible.
const EXPECTED_METRIC_NAMES: &[&str] = &[
    "chunked_write_cas_STORE_chunks_admitted_total",
    "chunked_write_cas_STORE_sha256_per_chunk_mismatches_total",
    "chunked_write_cas_STORE_sha256_e2e_mismatches_total",
    "chunked_write_cas_STORE_digest_func_proven_total",
    "chunked_write_cas_STORE_concurrent_same_digest_rejections_total",
    "chunked_write_cas_STORE_mpsc_full_rejections_total",
    "chunked_write_cas_STORE_global_budget_exhausted_rejections_total",
    "chunked_write_cas_STORE_pin_budget_exhausted_rejections_total",
    "chunked_write_cas_STORE_chunks_committed_total",
    "chunked_write_cas_STORE_commit_failures_total",
    "chunked_write_cas_STORE_commit_watchdog_fires_total",
    "chunked_write_cas_STORE_commit_watchdog_soft_warn_total",
    "chunked_write_cas_STORE_chunked_writers_per_digest_max",
    "chunked_write_cas_STORE_chunked_chunks_racing_loser_total",
    "chunked_write_cas_STORE_chunked_chunks_accepted_from_cross_writer_total",
    "chunked_write_cas_STORE_chunked_chunks_already_have_total",
    "chunked_write_cas_STORE_chunked_race_state_force_removed_total",
    "chunked_write_cas_STORE_chunked_reject_pre_probe",
    "chunked_write_cas_STORE_chunked_reject_at_offset0",
    "chunked_write_cas_STORE_chunked_reject_midstream",
    "chunked_write_cas_STORE_chunked_race_state_failed_publish_removed_total",
    "chunked_write_cas_STORE_chunked_race_state_evicted_success_reopened_total",
];

async fn make_handler() -> Arc<ChunkedWriteHandler> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: format!("{base}/{nonce}/fl1786-render/content"),
        temp_path: format!("{base}/{nonce}/fl1786-render/temp"),
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed");
    let budget: &'static ChunkBudget = Box::leak(Box::new(ChunkBudget::new()));
    Arc::new(ChunkedWriteHandler::new_with_state_and_chunk_size_for_test(
        store,
        ChunkedWriteInFlight::new(),
        budget,
        4 * 1024,
    ))
}

/// Drive the SAME wiring call `src/bin/nativelink.rs` makes, and hand back
/// both observables: the dispatch map (what serves writes) and the rendered
/// `/metrics` body (what an operator reads).
async fn install_as_production_does(
    handler: &Arc<ChunkedWriteHandler>,
) -> (HashMap<String, Arc<ChunkedWriteHandler>>, String) {
    let registry = MetricsRegistry::new();
    let mut handlers: HashMap<String, Arc<ChunkedWriteHandler>> = HashMap::new();
    install_chunked_write_handler(&mut handlers, &registry, "cas_STORE", Arc::clone(handler));
    let body = render_prometheus(&registry);
    (handlers, body)
}

#[nativelink_test]
async fn chunked_write_handler_metrics_render_under_the_production_prefix() {
    let handler = make_handler().await;

    // Give the new counter a distinctive non-default value so the assertion
    // reads the LIVE atomic rather than a coincidental zero — a metric that
    // renders `0` because the publish block was deleted and a metric that
    // renders `0` because nothing happened are indistinguishable.
    handler
        .metrics_component()
        .digest_func_proven_total
        .store(7, Ordering::Relaxed);

    let (_handlers, body) = install_as_production_does(&handler).await;

    for name in EXPECTED_METRIC_NAMES {
        assert!(
            body.contains(&format!("\n{name} ")),
            "#fl1786: metric `{name}` is NOT in the rendered /metrics body. Either \
             ChunkedWriteHandlerMetrics stopped being registered (it was dark for its entire \
             life before this change — see the module doc), the field was renamed, or its \
             publish was dropped. Dashboards and alerts key on these exact strings. body=\n{body}"
        );
    }

    assert!(
        body.contains("\nchunked_write_cas_STORE_digest_func_proven_total 7\n"),
        "#fl1786: `digest_func_proven_total` must render the LIVE counter value (7 was stored \
         above), not a constant. A metric that always renders 0 is indistinguishable from a \
         mechanism that never fires — which is precisely what this counter exists to \
         disambiguate. body=\n{body}"
    );
}

/// Completeness: the pinned list must cover every metric the struct
/// publishes. Without this, a field added later renders but stays unpinned
/// and a future rename of it goes unnoticed.
#[nativelink_test]
async fn every_rendered_chunked_write_metric_is_pinned_by_this_file() {
    let handler = make_handler().await;
    let (_handlers, body) = install_as_production_does(&handler).await;

    let rendered: Vec<&str> = body
        .lines()
        .filter(|l| l.starts_with("chunked_write_cas_STORE_"))
        .filter_map(|l| l.split(' ').next())
        .collect();
    for name in rendered {
        assert!(
            EXPECTED_METRIC_NAMES.contains(&name),
            "#fl1786: `{name}` renders on /metrics but is not in EXPECTED_METRIC_NAMES. Add it \
             there so a later rename or deletion of it red-fails the pinning test above; an \
             unpinned metric name is a silent contract with every dashboard that reads it"
        );
    }
}

/// **The M8 guard.** A reviewer deleted `src/bin/nativelink.rs`'s entire
/// `metrics_registry.register(..)` block and this suite stayed 6/6 green,
/// because the file above hand-copied the registration into a local registry
/// instead of exercising the code the binary runs. That is a rendering test
/// wearing a wiring test's doc-comment.
///
/// This asserts the two halves TOGETHER against the one function the binary
/// calls: a handler that reached the dispatch map (so it serves writes) is
/// also registered (so its counters are readable). Deleting the
/// `metrics_registry.register(..)` inside `install_chunked_write_handler`
/// reds this immediately; deleting the map insert reds it too. It does NOT
/// cover deleting the binary's CALL to that function — see the
/// module-level comment; that leg of M8 remains open.
#[nativelink_test]
async fn installing_a_handler_registers_its_metrics_and_makes_it_dispatchable() {
    let handler = make_handler().await;
    // Distinctive value again: this must read the SAME Arc the handler bumps,
    // not a default-constructed copy registered by accident.
    handler
        .metrics_component()
        .chunks_committed_total
        .store(11, Ordering::Relaxed);

    let (handlers, body) = install_as_production_does(&handler).await;

    assert!(
        handlers.contains_key("cas_STORE"),
        "#fl1786: install_chunked_write_handler must put the handler in the per-CAS-store \
         dispatch map — that map is what the per-listener loop reads to attach \
         CasExtensionsServer, so a handler missing from it means the worker's chunked stream \
         gets Code::Unimplemented"
    );
    assert!(
        body.contains("\nchunked_write_cas_STORE_chunks_committed_total 11\n"),
        "#fl1786 M8: the handler reached the dispatch map but its counters did NOT render. \
         That is the exact state ChunkedWriteHandlerMetrics spent its entire life in — \
         constructed, bumped by the live write path, and unreadable on /metrics — and it is \
         what makes digest_func_proven_total (the engaged-mechanism signal for this whole \
         change) worthless. Registration and installation are one operation precisely so this \
         cannot drift. body=\n{body}"
    );
}

/// The rendered prefix is a dashboard contract, so pin the function that
/// produces it rather than letting the binary and this file format it
/// independently.
#[nativelink_test]
async fn the_metric_prefix_is_derived_from_the_cas_store_name() {
    assert_eq!(
        chunked_write_metrics_prefix("cas_STORE"),
        "chunked_write.cas_STORE",
        "#fl1786: the chunked-write metric prefix must stay `chunked_write.<cas store name>` — \
         it is per-CAS-store because the handler map is keyed that way and two CAS stores would \
         otherwise publish colliding metric names, and every rendered name in \
         EXPECTED_METRIC_NAMES is derived from it"
    );
}
