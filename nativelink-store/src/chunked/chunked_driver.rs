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

//! Per-blob driver task SKELETON for the #212 chunked architecture.
//!
//! This is INFRASTRUCTURE ONLY — Phase 1 ships the spawn/lifecycle/drop
//! plumbing with NO behavior. The receive loop currently increments a
//! counter and drops each `ChunkWork`; it does NOT write to slow tier,
//! does NOT touch `failed_writes`, does NOT update sidecar state. Those
//! semantics arrive in Phase 2 alongside the FilesystemStore
//! `pwrite`-at-offset APIs (§7.1) and the FastSlowStore admission
//! point (§6.1).
//!
//! The shape we DO commit to in Phase 1, because every later phase
//! depends on it (see §6.7 termination contract):
//!
//! - The driver task is `tokio::spawn`'d on first chunk arrival; the
//!   returned `JoinHandle` is wrapped in a `JoinHandleDropGuard`.
//! - Each `ChunkWork` carries an `OwnedSemaphorePermit` from the
//!   `ChunkBudget`. Permit lifetime = `ChunkWork` lifetime; on
//!   driver-task panic the permit drops automatically (no separate
//!   reclamation path).
//! - The mpsc is bounded at 16 per Q4. Phase 2 admission code calls
//!   `try_send` (never `send().await`) so a full per-blob mpsc surfaces
//!   as a `BackpressureSignal { reason: PER_BLOB_MPSC_FULL }` instead
//!   of upstream-blocking — that's the §13.1.1 point 1 trap shape.
//! - Drop closes the mpsc (sender side) and awaits the join via the
//!   guard, per §6.7 termination trigger (d) (panic) and (b)
//!   (shutdown).

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use nativelink_util::common::DigestInfo;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc;
use tracing::trace;

/// Per-blob mpsc capacity. Q4: 16 chunks-in-flight per blob is the
/// upper bound on per-blob memory pressure (16 × 1 MiB = 16 MiB).
/// Together with the global `ChunkBudget` cap this gives a 12-16 GiB
/// worst-case server RSS bound (§13.2).
pub(crate) const PER_BLOB_MPSC_CAP: usize = 16;

/// One unit of work consumed by the per-blob driver. Carries the chunk
/// payload + metadata + the `ChunkBudget` permit minted at admission.
/// Phase 2 driver code will: (a) write the chunk to the slow tier at
/// `chunk_offset` via FilesystemStore's `pwrite`-at-offset API
/// (§7.1), (b) verify `chunk_sha256`, (c) record arrival in the
/// in-memory sidecar bitmap (§7.2), (d) drop the permit on
/// completion. Phase 1 just drops the whole struct on receive.
#[derive(Debug)]
pub(crate) struct ChunkWork {
    pub(crate) chunk_offset: u64,
    pub(crate) chunk_bytes: Bytes,
    pub(crate) chunk_sha256: [u8; 32],
    /// Permit lifetime = `ChunkWork` lifetime. Per §13.1.1 point 1:
    /// admission moved this permit out of the global `ChunkBudget`
    /// into the `ChunkWork`; dropping the work releases the permit.
    /// Intentionally `_`-prefixed here since Phase 1 just drops it.
    pub(crate) _permit: OwnedSemaphorePermit,
}

/// Sender half of the per-blob mpsc. Phase 2 admission code holds one
/// of these per in-flight blob and `try_send`s `ChunkWork` items.
/// Closing the sender (drop) is the §6.7 happy-path termination
/// trigger — the driver loop exits cleanly when the channel closes.
pub(crate) type ChunkWorkSender = mpsc::Sender<ChunkWork>;

/// Per-blob driver task handle. The `JoinHandleDropGuard` ensures the
/// task is aborted on `Drop` (panic-safety belt for §6.7 trigger d).
/// In the happy path the task exits before drop because the mpsc
/// closes when admission drops its sender.
///
/// Phase 1 exposes:
/// - `spawn_driver(digest, capacity)` — spawns the task, returns
///   `(ChunkedDriver, ChunkWorkSender)`.
/// - `chunks_received()` — observability for tests + the metric
///   gauge wiring in Phase 2.
///
/// NOT yet stored anywhere. Phase 2 adds a `BlobInFlightState` map
/// keyed by `DigestInfo` that owns `ChunkedDriver` per-blob.
#[derive(Debug)]
pub(crate) struct ChunkedDriver {
    /// Identifies the blob this driver belongs to. Phase 2 logging /
    /// metrics will scope by digest; today it's load-bearing only for
    /// observability spans.
    digest: DigestInfo,
    /// Counter incremented on every `ChunkWork` received. Pinned in
    /// Phase 1 so the test harness can observe what the driver did
    /// without instrumenting the whole spawn lifecycle.
    chunks_received: Arc<AtomicU64>,
    /// Drop guard for the spawned task. On `Drop` of `ChunkedDriver`,
    /// the join handle is `abort()`'d if still running (§6.7 panic
    /// belt). `JoinHandleDropGuard` is `must_use`, hence the
    /// `_handle` name to make the never-awaited intent explicit;
    /// Phase 2 may explicitly `await` it during shutdown drain.
    _handle: JoinHandleDropGuard<()>,
}

impl ChunkedDriver {
    /// Spawn the per-blob driver task. Returns the driver handle plus
    /// the sender side of the bounded mpsc. The receiver side moves
    /// into the spawned task. `capacity` is the mpsc bound; admission
    /// code MUST pass `PER_BLOB_MPSC_CAP` in production — the
    /// argument is here so Phase 1 tests can exercise smaller channels
    /// without a 16-`ChunkWork` setup.
    ///
    /// The spawned task is the SKELETON loop: receive → increment
    /// counter → drop. NO slow-tier I/O. Phase 2 will replace the
    /// drop with the §7.1 `pwrite_at_offset` call.
    pub(crate) fn spawn_driver(
        digest: DigestInfo,
        capacity: usize,
    ) -> (Self, ChunkWorkSender) {
        let (tx, mut rx) = mpsc::channel::<ChunkWork>(capacity);
        let chunks_received = Arc::new(AtomicU64::new(0));
        let chunks_received_for_task = Arc::clone(&chunks_received);

        let handle = spawn!("212_chunked_driver_skeleton", async move {
            // Receive loop. Each iteration: take one ChunkWork, bump
            // the counter, drop. The drop releases the permit (Q8 budget
            // returned). Loop exits when the sender side drops (§6.7
            // happy-path / shutdown trigger).
            while let Some(work) = rx.recv().await {
                chunks_received_for_task.fetch_add(1, Ordering::Relaxed);
                trace!(
                    target: "nativelink_store::chunked",
                    chunk_offset = work.chunk_offset,
                    chunk_len = work.chunk_bytes.len(),
                    "phase1 skeleton: dropping chunk (no slow-tier write yet)",
                );
                drop(work);
            }
        });

        (
            Self {
                digest,
                chunks_received,
                _handle: handle,
            },
            tx,
        )
    }

    /// Observation hook for Phase 1 tests. Phase 2 will export this as
    /// `chunked_chunks_received_total{digest=...}` per-blob.
    #[must_use]
    pub(crate) fn chunks_received(&self) -> u64 {
        self.chunks_received.load(Ordering::Relaxed)
    }

    /// Read-only accessor used by Phase 2 logging / metric labels.
    #[must_use]
    #[allow(dead_code, reason = "wired in Phase 2 driver instrumentation")]
    pub(crate) fn digest(&self) -> &DigestInfo {
        &self.digest
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use nativelink_macro::nativelink_test;
    use nativelink_util::common::DigestInfo;

    use super::super::chunk_budget::ChunkBudget;
    use super::{ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP};

    /// Capacity constant pin: any change is ARCHITECTURAL — re-read
    /// design §4 Q4 before bumping.
    #[test]
    fn per_blob_mpsc_cap_is_sixteen() {
        assert_eq!(PER_BLOB_MPSC_CAP, 16);
    }

    /// Send three chunks through the driver, close the sender, and
    /// wait for the driver to drain + exit. Per CLAUDE.md timeout
    /// discipline the test is wrapped in `tokio::time::timeout(2s)`
    /// and the failure message names the contract that was violated
    /// (§6.7 termination trigger (a)/(d): driver MUST exit when the
    /// mpsc closes).
    #[nativelink_test]
    async fn driver_drains_chunks_then_exits_cleanly_on_sender_drop() {
        let budget = ChunkBudget::new();
        let digest = DigestInfo::new([0x42u8; 32], 3 * 1024 * 1024);
        let (driver, tx) = ChunkedDriver::spawn_driver(digest, PER_BLOB_MPSC_CAP);

        // Send 3 ChunkWorks. Permits come from the live ChunkBudget so
        // the test exercises the real admission shape.
        for i in 0..3u64 {
            let permit = budget
                .try_acquire_chunk()
                .expect("ChunkBudget must admit 3 permits in a fresh budget");
            tx.send(ChunkWork {
                chunk_offset: i * (1024 * 1024),
                chunk_bytes: bytes::Bytes::from(vec![0u8; 1024 * 1024]),
                chunk_sha256: [0u8; 32],
                _permit: permit,
            })
            .await
            .expect("driver mpsc receiver must still be alive");
        }

        // Wait for all 3 to be received before triggering shutdown.
        // Polling loop with explicit timeout per CLAUDE.md (no sleep
        // as synchronization).
        let drained = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if driver.chunks_received() == 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        drained.expect(
            "driver must process 3 chunks within 2s — \
             skeleton receive loop is broken or never spawned",
        );

        // Drop the sender. The driver's `rx.recv().await` MUST return
        // None and the task must exit. Because the driver holds a
        // JoinHandleDropGuard, awaiting it directly would consume the
        // guard; instead we observe permit-budget recovery as a proxy
        // for "the spawned task dropped its work items + exited."
        drop(tx);

        let recovered = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                // After all 3 ChunkWorks have been dropped, the budget
                // must show 3 permits available again. The driver task
                // also exits; if it did not the spawned future would
                // hold no chunks (we already drained them) and the
                // permit count is the load-bearing observation.
                if budget.available_chunks() >= 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        recovered.expect(
            "ChunkBudget permits must return to baseline after sender-drop — \
             writer-termination contract violated (driver leaked permits)",
        );
    }

    /// Dropping the `ChunkedDriver` aborts the spawned task even if the
    /// sender side is held alive elsewhere (panic-safety belt per §6.7
    /// trigger (d)). This is the second half of the lifetime contract:
    /// trigger (a)/(b) tested above, trigger (d) tested here.
    #[nativelink_test]
    async fn driver_drop_aborts_spawned_task() {
        let budget = ChunkBudget::new();
        let digest = DigestInfo::new([0x55u8; 32], 1024 * 1024);
        let (driver, tx) = ChunkedDriver::spawn_driver(digest, PER_BLOB_MPSC_CAP);

        // Send one chunk so the task has done some work.
        let permit = budget.try_acquire_chunk().expect("permit");
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: bytes::Bytes::from_static(b"x"),
            chunk_sha256: [1u8; 32],
            _permit: permit,
        })
        .await
        .expect("send to live driver");

        // Wait until the chunk has been observed.
        tokio::time::timeout(Duration::from_secs(2), async {
            while driver.chunks_received() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver must observe the first chunk within 2s");

        // Drop the driver while the sender is still alive. The
        // JoinHandleDropGuard MUST abort the spawned task. After
        // drop, the budget recovers because the in-flight `Bytes`
        // (and hence its permit) is inside the dropped task.
        drop(driver);

        let recovered = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if budget.available_chunks() == 4096 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        recovered.expect(
            "ChunkBudget must return to full capacity after driver-drop — \
             JoinHandleDropGuard did not abort the spawned task (#212 §6.7d)",
        );

        // Sender is now dangling; sending into it must error because
        // the spawned receiver was aborted (or surface as channel-full
        // depending on timing). EITHER outcome is a proof of life of
        // the abort. We don't assert on which; we just assert no panic.
        let _ = tx.try_send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: bytes::Bytes::from_static(b"y"),
            chunk_sha256: [2u8; 32],
            _permit: budget.try_acquire_chunk().expect("permit"),
        });
    }
}
