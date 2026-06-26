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

//! Worker-intake quiesce latch for graceful shutdown.
//!
//! Sibling of [`crate::bazel_reapi_quiesce::BazelReapiQuiesce`]. Where that gate
//! refuses NEW Bazel-facing REAPI requests on the public `:50051` listener, this
//! one suppresses the server's solicitation of NEW worker blob uploads (the
//! backfill / pinned-mirror-pull feeds in `worker_api_server.rs`'s
//! `handle_blobs_available`).
//!
//! WHY a separate gate from `BazelReapiQuiesce`: the worker-facing listeners
//! (`:50061` / `:50071` / `:50072`) are NEVER quiesced by `BazelReapiQuiesce`
//! (the shutdown worker-PULL needs them open). The worker connections stay open
//! during the unbounded shutdown drain and keep ticking `BlobsAvailable` every
//! ~100 ms; each tick runs `has_with_results` + (cooldown-gated) solicits
//! `UploadMissingBlobs` AND an un-throttled pinned-mirror pull. That continuous
//! worker-solicited intake re-feeds the at-risk set the drain is trying to
//! converge — the storm that makes the unbounded flush never reach a fixed
//! point (observed live 2026-06-26: `total=11887 missing=11887` mid-shutdown,
//! 111% CPU). See `.claude/audits/server-sigkill-gap-design-2026-06-26.md`.
//!
//! On SIGTERM the bin flips this latch at a new Phase 0b — BEFORE the unbounded
//! flush phases — so the handler-invoked solicitation early-returns and the
//! drain converges. The latch is read ONLY on the BlobsAvailable HANDLER path;
//! the server-INITIATED `ShutdownPuller::run` calls the SAME send-half
//! (`WorkerConnection::request_missing_blob_uploads`) but passes no latch, so
//! the shutdown PULL is unaffected (it is the drain, not the storm).
//!
//! `Ordering::Relaxed` is sufficient (same argument as
//! `bazel_reapi_quiesce.rs`): a one-way `false → true` latch with no other
//! memory it publishes; a tick racing the flip either does one more harmless
//! solicitation pass or stops — both correct shutdown behaviors.

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Shared worker-intake quiesce latch.
///
/// One instance is created inside `WorkerApiServer::new_with_now_fn` and CLONED
/// into:
///   - every [`crate::worker_api_server`] `WorkerConnection` (read side, checked
///     at the entry of the handler-invoked solicitation);
///   - the SIGTERM handler (write side, [`Self::quiesce`]) via
///     `WorkerApiServer::shutdown_quiesce_handle`.
///
/// Cheap to clone (one `Arc<AtomicBool>`).
#[derive(Clone, Debug)]
pub struct ShutdownQuiesce {
    quiesced: Arc<AtomicBool>,
}

impl ShutdownQuiesce {
    #[must_use]
    pub fn new() -> Self {
        Self {
            quiesced: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Latch the gate closed. Called ONCE at SIGTERM Phase 0b, BEFORE the
    /// unbounded flush + pull, so the worker-solicited intake stops and the
    /// drain converges to a fixed point. Idempotent.
    pub fn quiesce(&self) {
        self.quiesced.store(true, Ordering::Relaxed);
    }

    /// Whether NEW worker-upload solicitation (the handler-invoked backfill /
    /// pinned-mirror-pull feeds) should be suppressed. The server-initiated
    /// shutdown PULL does NOT consult this — it passes no latch.
    #[must_use]
    pub fn is_quiesced(&self) -> bool {
        self.quiesced.load(Ordering::Relaxed)
    }
}

impl Default for ShutdownQuiesce {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh latch is OPEN (steady state): worker intake is solicited
    /// normally.
    ///
    /// Mutation: make `new()` construct with `AtomicBool::new(true)` → this
    /// red-fails (a fresh server would refuse all backfill at boot).
    #[test]
    fn fresh_latch_is_open() {
        let q = ShutdownQuiesce::new();
        assert!(
            !q.is_quiesced(),
            "a fresh worker-intake latch MUST be open so steady-state backfill \
             is solicited; only SIGTERM Phase 0b closes it"
        );
    }

    /// `quiesce()` latches it closed; the close is observed through a CLONE
    /// (proving the SIGTERM handler's clone reaches the per-connection clones
    /// off the shared `Arc<AtomicBool>`).
    ///
    /// Mutation: make `Clone` allocate a FRESH `AtomicBool` (break the `Arc`
    /// share) → the clone never sees the latch → red-fails.
    #[test]
    fn quiesce_is_seen_through_a_clone() {
        let q = ShutdownQuiesce::new();
        let connection_side = q.clone();
        // SIGTERM handler holds its own clone and flips it.
        let sigterm_side = q.clone();
        sigterm_side.quiesce();
        assert!(
            connection_side.is_quiesced(),
            "quiescing through one clone MUST close the gate seen by every other \
             clone (shared Arc<AtomicBool>); otherwise SIGTERM Phase 0b would not \
             reach the per-connection solicitation gate"
        );
    }
}
