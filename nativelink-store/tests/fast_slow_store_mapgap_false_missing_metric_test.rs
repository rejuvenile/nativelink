// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! (#mapgap) 3-way source split of the worker input-materialization
//! FALSE-MISSING probe on `FastSlowStore::probe_input_missing_sources`.
//!
//! ## Gap being measured
//!
//! The scheduler flags input blobs "missing on worker X" that X
//! demonstrably HAS. Prefetch pushes are NORMAL writes (hit disk + fire
//! the BlobChangeTracker → ARE reported); the ONLY worker holdings the
//! routing `blob_locality_map` is blind to are ≥2-replica MIRROR copies
//! held in the in-memory `mirror_blobs` buffer (mirror writes skip disk +
//! the tracker). So we must tell, for each server-flagged-missing digest
//! the worker actually held, WHETHER it was on disk (map lag/report bug)
//! or in the mirror buffer (THE mirror gap).
//!
//! ## Invariant under test
//!
//! `probe_input_missing_sources` MUST classify each digest exactly by its
//! real source — disk (`fast_store.has()`==Some), mirror (in
//! `mirror_blobs`), or fetched (neither) — mirroring `populate_fast_store`'s
//! 3-way branch, AND bump the matching `input_server_missing_hit_{disk,
//! mirror}_*` / `_fetched_*` counters, which MUST render on the production
//! `/metrics` path (worker: `nativelink_WORKER_FAST_SLOW_STORE_...`).
//!
//! ## Seams crossed
//!
//! Real `FastSlowStore` (real `MemoryStore` on both tiers). Disk-tier
//! presence is seeded by writing into the inner fast `MemoryStore`
//! directly; mirror presence via the production `insert_dispatched_mirror_blob`
//! entry point; the fetched case is a digest present in neither. Rendered
//! via the SAME `render_prometheus` walk `metrics_handler` uses in prod.
//!
//! ## Mutation step (CLAUDE.md mandatory)
//!
//! Per test, comment out the corresponding bucket's `fetch_add` in
//! `probe_input_missing_sources` (e.g. `input_server_missing_hit_mirror_count`).
//! The matching test MUST red-fail with its bespoke
//! "#mapgap: ... dark or wrong" message naming that source bucket.

use core::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};

/// Build a real `FastSlowStore` with `MemoryStore` on both tiers; also
/// return the inner fast-tier `MemoryStore` Arc so a test can seed the
/// DISK tier directly.
fn build_fss() -> (Arc<FastSlowStore>, Arc<MemoryStore>) {
    let fast_inner = MemoryStore::new(&MemorySpec::default());
    let slow_inner = MemoryStore::new(&MemorySpec::default());
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(fast_inner.clone()),
        Store::new(slow_inner),
    );
    (fss, fast_inner)
}

/// A digest whose `size_bytes` matches `data.len()` (mirror insert
/// validates this). Uses a distinctive hash byte per case so concurrent
/// tests never collide on store state.
fn digest_for(seed: u8, data: &[u8]) -> DigestInfo {
    DigestInfo::new([seed; 32], data.len() as u64)
}

/// (#mapgap) The 3-way probe classifies disk / mirror / fetched exactly,
/// and every bucket renders on `/metrics` with the recorded values.
#[nativelink_test]
async fn probe_splits_disk_mirror_fetched_and_renders() {
    let (fss, fast_inner) = build_fss();

    // DISK: seed the inner fast MemoryStore directly (bypasses the FSS
    // slow-write spawn / mirror path — this is a pure disk-tier presence).
    let disk_data = Bytes::from_static(b"on-disk-blob");
    let disk_digest = digest_for(0xD1, &disk_data);
    Pin::new(fast_inner.as_ref())
        .update_oneshot(StoreKey::Digest(disk_digest), disk_data.clone())
        .await
        .expect("seed disk (fast) tier");

    // MIRROR: push into the in-memory mirror buffer via the production
    // dispatcher entry point (validates data.len() == size_bytes).
    let mirror_data = Bytes::from_static(b"in-mirror-buffer-blob!");
    let mirror_digest = digest_for(0xC1, &mirror_data);
    fss.insert_dispatched_mirror_blob("store-a", mirror_digest, mirror_data.clone())
        .expect("seed mirror buffer");

    // FETCHED: a digest present in NEITHER tier nor mirror.
    let fetched_digest = DigestInfo::new([0xF1; 32], 999);

    // Probe the union — exactly the server-flagged-missing set shape.
    let probe = fss
        .probe_input_missing_sources(&[disk_digest, mirror_digest, fetched_digest])
        .await
        .expect("probe must not error against MemoryStore tiers");

    // Returned split is exact.
    assert_eq!(
        (probe.disk_count, probe.mirror_count, probe.fetched_count),
        (1, 1, 1),
        "#mapgap: probe misclassified the 3-way source split \
         (expected disk=1, mirror=1, fetched=1); got disk={}, mirror={}, \
         fetched={}. A mirror blob mis-scored as fetched would HIDE the \
         mirror gap.",
        probe.disk_count,
        probe.mirror_count,
        probe.fetched_count
    );
    assert_eq!(probe.disk_bytes, disk_data.len() as u64);
    assert_eq!(probe.mirror_bytes, mirror_data.len() as u64);
    assert_eq!(probe.fetched_bytes, 999);

    // Render through the production walk under a worker-shaped prefix.
    let registry = MetricsRegistry::new();
    registry.register("nativelink.WORKER_FAST_SLOW_STORE", fss.clone());
    let _warm = render_prometheus(&registry);
    let body = render_prometheus(&registry);

    assert!(
        body.contains(
            "\nnativelink_WORKER_FAST_SLOW_STORE_input_server_missing_hit_disk_count 1\n"
        ),
        "#mapgap: disk-hit count dark or wrong on /metrics (expected 1) — a \
         high value here means the map is stale about REPORTED disk holdings. \
         body=\n{body}"
    );
    assert!(
        body.contains(
            "\nnativelink_WORKER_FAST_SLOW_STORE_input_server_missing_hit_mirror_count 1\n"
        ),
        "#mapgap: mirror-hit count dark or wrong on /metrics (expected 1) — \
         THIS is the held-but-unreported mirror gap; a dark counter here \
         means the smoking-gun signal is invisible. body=\n{body}"
    );
    assert!(
        body.contains(
            "\nnativelink_WORKER_FAST_SLOW_STORE_input_server_missing_hit_mirror_bytes 22\n"
        ),
        "#mapgap: mirror-hit bytes dark or wrong on /metrics (expected 22). \
         body=\n{body}"
    );
    assert!(
        body.contains(
            "\nnativelink_WORKER_FAST_SLOW_STORE_input_server_missing_fetched_count 1\n"
        ),
        "#mapgap: fetched count dark or wrong on /metrics (expected 1) — the \
         false-missing-fraction denominator is wrong. body=\n{body}"
    );
}

/// (#mapgap) The mirror-hit bucket must be DISTINCT from the disk-hit
/// bucket: a blob that is in the mirror buffer but NOT on disk must score
/// as mirror, never disk. Isolates the branch order (has() first, then
/// mirror) so a mirror-only blob is never misattributed to disk.
#[nativelink_test]
async fn mirror_only_blob_scores_mirror_not_disk() {
    let (fss, _fast_inner) = build_fss();

    let mirror_data = Bytes::from_static(b"mirror-only");
    let mirror_digest = digest_for(0x7A, &mirror_data);
    fss.insert_dispatched_mirror_blob("s", mirror_digest, mirror_data.clone())
        .expect("seed mirror buffer");

    let probe = fss
        .probe_input_missing_sources(&[mirror_digest])
        .await
        .expect("probe ok");

    assert_eq!(
        (probe.disk_count, probe.mirror_count, probe.fetched_count),
        (0, 1, 0),
        "#mapgap: a mirror-only blob (in mirror_blobs, not on disk) must \
         score as mirror=1, disk=0, fetched=0; got disk={}, mirror={}, \
         fetched={}. Misattributing it to disk would falsely blame a \
         report-lag bug instead of the mirror gap.",
        probe.disk_count,
        probe.mirror_count,
        probe.fetched_count
    );
}

/// (#mapgap) The `mirror_blobs_{digest_count,total_bytes}` gauges render
/// on `/metrics` and TRACK insert + remove (read at scrape / stored as the
/// live len, not accumulated).
#[nativelink_test]
async fn mirror_blobs_size_gauges_render_and_track() {
    let (fss, _fast_inner) = build_fss();

    let d1_data = Bytes::from_static(b"mirror-one");
    let d1 = digest_for(0xA1, &d1_data);
    let d2_data = Bytes::from_static(b"mirror-two-longer");
    let d2 = digest_for(0xA2, &d2_data);
    fss.insert_dispatched_mirror_blob("s", d1, d1_data.clone())
        .expect("seed d1");
    fss.insert_dispatched_mirror_blob("s", d2, d2_data.clone())
        .expect("seed d2");

    let registry = MetricsRegistry::new();
    registry.register("nl.WORKER_FAST_SLOW_STORE", fss.clone());
    let _warm = render_prometheus(&registry);
    let before = render_prometheus(&registry);

    let expected_bytes = (d1_data.len() + d2_data.len()) as u64;
    assert!(
        before.contains("\nnl_WORKER_FAST_SLOW_STORE_mirror_blobs_digest_count 2\n"),
        "#mapgap: mirror_blobs_digest_count dark or wrong on /metrics \
         (expected 2) — we are blind to how much of the mirror durability \
         buffer sits unreported. body=\n{before}"
    );
    assert!(
        before.contains(&format!(
            "\nnl_WORKER_FAST_SLOW_STORE_mirror_blobs_total_bytes {expected_bytes}\n"
        )),
        "#mapgap: mirror_blobs_total_bytes dark or wrong on /metrics \
         (expected {expected_bytes}). body=\n{before}"
    );

    // Remove one; both gauges must decrease at the next scrape.
    fss.remove_mirror_blobs(&[d1]);
    let after = render_prometheus(&registry);
    assert!(
        after.contains("\nnl_WORKER_FAST_SLOW_STORE_mirror_blobs_digest_count 1\n"),
        "#mapgap: mirror_blobs_digest_count did not track the removal \
         (expected 1) — the gauge must reflect the live buffer len, not \
         accumulate. body=\n{after}"
    );
    assert!(
        after.contains(&format!(
            "\nnl_WORKER_FAST_SLOW_STORE_mirror_blobs_total_bytes {}\n",
            d2_data.len()
        )),
        "#mapgap: mirror_blobs_total_bytes did not track the removal \
         (expected {}). body=\n{after}",
        d2_data.len()
    );
}
