// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! Task #130 — singleflight/dedup map for concurrent same-key fetches.
//!
//! When N callers concurrently invoke `singleflight(key, fetcher)` for
//! the same `key`, exactly ONE of them runs `fetcher` (the *leader*);
//! the other N-1 (the *waiters*) await the leader's result and receive
//! a shared `Arc<Vec<Bytes>>` clone. This eliminates the
//! "channel-poisoning amplifier" pattern documented in
//! `docs/130-singleflight-peer-fetch-design.md`, where N concurrent
//! same-digest h2 reads on a single channel cause N-1 of them to fail
//! with `Tried to send while stream is closed`.
//!
//! ## Design (per `docs/130-singleflight-peer-fetch-design.md`, Option B)
//!
//! * **Key.** `StoreKey<'static>` — typically a `DigestInfo`. Callers
//!   that key on offset/length must build that into the key themselves
//!   (Option B: full-blob reads only at the WPS wire-up layer).
//! * **Result fan-out.** Leader buffers chunks into `Arc<Vec<Bytes>>`,
//!   preserving producer chunk boundaries (no copy). All waiters
//!   receive the same `Arc` via `tokio::sync::watch::Receiver`.
//! * **Cap.** `max_inflight_bytes` (default 256 MiB). When the cap is
//!   exceeded at slot-registration time, NEW callers bypass singleflight
//!   and run the fetcher directly. Bypass is best-effort; the cap is a
//!   soft ceiling to keep total fan-out memory bounded.
//! * **Cancel safety.** If the leader's outer Future is dropped while
//!   ≥1 waiter is alive, the watch::Sender drops, every waiter's
//!   `rx.changed()` returns `Err(closed)`. Each waiter releases its
//!   `Arc<InflightEntry>` and loops; the last release purges the slot
//!   from the map and the next caller (or the same one in its loop)
//!   becomes the new leader and re-runs `fetcher`.
//!
//! ## Failure semantics
//!
//! If the leader's fetcher returns `Err(...)`, ALL waiters receive a
//! clone of that same `Err`. Waiters do NOT independently retry — that's
//! the bug being fixed. If retry is desired, the fetcher itself owns it.
//!
//! ## Memory shape
//!
//! `Mutex<HashMap<StoreKeyBorrow, Weak<InflightEntry>>>` — slot drop is
//! refcount-driven (last `Arc<InflightEntry>` releases the slot). The
//! map lock is held only briefly for entry installation/lookup, never
//! across `.await`.

use core::future::Future;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::watch;
use tracing::warn;

use nativelink_error::Error;
use nativelink_util::store_trait::{StoreKey, StoreKeyBorrow};

/// Default soft cap on total in-flight singleflight payload bytes
/// (reserved at slot registration; released on slot drop). New leaders
/// past this ceiling bypass dedup. Tuned for the WorkerProxyStore use
/// case where per-blob payload is bounded by `MAX_CACHE_BLOB_SIZE`
/// (~1 MiB) and concurrent slot count is bounded by inflight-fan-out
/// width (typically 4-16, up to 64 in pathological bursts).
pub const DEFAULT_MAX_INFLIGHT_BYTES: u64 = 256 * 1024 * 1024;

/// Result type fanned out from the leader to all waiters. `Vec<Bytes>`
/// preserves producer chunk boundaries (avoids the O(blob) copy a
/// single-`Bytes` coalesce would require).
pub type SingleflightPayload = Arc<Vec<Bytes>>;

/// Internal slot. Cloned (Arc) into both the leader and every waiter.
/// When the last `Arc<InflightEntry>` drops, accounting in
/// `SingleflightMapInner::current_inflight_bytes` is reversed and the
/// slot's dead `Weak` is purged from the map.
///
/// `published` distinguishes "leader sent terminal value" from "leader
/// dropped without sending" so a waiter that wakes on `Err(closed)` can
/// choose between "consume the value" and "promote".
#[derive(Debug)]
struct InflightEntry {
    /// Receiver-side handle. Cloned by waiters at subscription.
    result_rx: watch::Receiver<Option<Arc<Result<SingleflightPayload, Error>>>>,
    /// Set true by the leader BEFORE it calls `result_tx.send`. If a
    /// waiter wakes via `result_rx.changed()` with `Err(closed)` AND
    /// `published` is false, the leader was canceled and the waiter
    /// must promote.
    published: AtomicBool,
    /// The map that owns this slot — used by Drop to reverse cap
    /// accounting and purge the dead `Weak` from the map.
    parent: Weak<SingleflightMapInner>,
    /// The key this slot is registered under.
    key: StoreKeyBorrow,
    /// Bytes reserved upfront against the cap. Released on Drop.
    reserved_bytes: u64,
}

impl Drop for InflightEntry {
    fn drop(&mut self) {
        let Some(parent) = self.parent.upgrade() else {
            return;
        };
        if self.reserved_bytes > 0 {
            parent
                .current_inflight_bytes
                .fetch_sub(self.reserved_bytes, Ordering::Relaxed);
        }
        // Remove the slot's dead `Weak` from the map. Lock is brief.
        // If a successor leader has already replaced our entry with
        // a fresh `Weak`, `strong_count() > 0` and we leave it alone.
        let mut map = parent.map.lock();
        if let Some(weak) = map.get(&self.key)
            && weak.strong_count() == 0
        {
            map.remove(&self.key);
        }
    }
}

/// Singleflight / in-flight dedup map keyed on `StoreKey<'static>`.
///
/// See module-level docs for design and contract. Cheap to clone via
/// `Arc<SingleflightMap>` if shared across tasks.
#[derive(Debug)]
pub struct SingleflightMap {
    inner: Arc<SingleflightMapInner>,
}

#[derive(Debug)]
struct SingleflightMapInner {
    /// Active slots keyed by `StoreKeyBorrow` (a `StoreKey<'static>`
    /// wrapper that has `Hash + Eq + Borrow<StoreKey<'a>>`). `Weak` so
    /// drop of the last leader/waiter `Arc<InflightEntry>` cleanly
    /// removes the slot.
    map: Mutex<HashMap<StoreKeyBorrow, Weak<InflightEntry>>>,
    /// Soft cap on total reserved bytes across all live slots. New
    /// callers that would push the counter past this value bypass
    /// singleflight (run the fetcher directly without registering a
    /// slot). Existing waiters are NEVER blocked by the cap.
    max_inflight_bytes: u64,
    /// Current sum of `reserved_bytes` across all live slots. Atomic
    /// because increment happens under the map lock but decrement
    /// happens lock-free in `InflightEntry::drop`.
    current_inflight_bytes: AtomicU64,
}

impl SingleflightMap {
    /// Create a singleflight map with the default cap
    /// (`DEFAULT_MAX_INFLIGHT_BYTES`).
    #[must_use]
    pub fn new() -> Arc<Self> {
        Self::with_cap(DEFAULT_MAX_INFLIGHT_BYTES)
    }

    /// Create a singleflight map with a custom cap (in bytes).
    #[must_use]
    pub fn with_cap(max_inflight_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(SingleflightMapInner {
                map: Mutex::new(HashMap::new()),
                max_inflight_bytes,
                current_inflight_bytes: AtomicU64::new(0),
            }),
        })
    }

    /// Run `fetcher` exactly once for `key` even when called concurrently.
    ///
    /// Behavior:
    /// * The first caller becomes the **leader**: `fetcher` is invoked
    ///   and its result is broadcast to all waiters as
    ///   `Arc<Result<Arc<Vec<Bytes>>, Error>>`.
    /// * Subsequent concurrent callers (while the leader is still
    ///   in-flight) become **waiters**: they receive a clone of the
    ///   leader's result.
    /// * If the leader's outer Future is dropped (its caller cancels)
    ///   AND ≥1 waiter is alive, the slot is purged and the next
    ///   waiter to wake promotes itself by re-running `fetcher`.
    /// * If the cap is exceeded, the caller bypasses (runs `fetcher`
    ///   directly without slot registration; no dedup for that call).
    ///
    /// `expected_size` is the caller's best estimate of payload size
    /// (e.g. `digest.size_bytes()`). Used for cap accounting only —
    /// not enforced as a maximum on the actual payload.
    pub async fn singleflight<F, Fut>(
        self: &Arc<Self>,
        key: StoreKey<'_>,
        expected_size: u64,
        fetcher: F,
    ) -> Result<SingleflightPayload, Error>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Vec<Bytes>, Error>> + Send,
    {
        let owned_key: StoreKeyBorrow = key.into_owned().into();
        // We hold `fetcher` in an Option so we can `take()` it on the
        // single path that consumes it (leader or bypass). FnOnce can
        // only be called once; the loop body either consumes it via the
        // leader/bypass arm or returns via the waiter arm without
        // consuming it.
        let mut fetcher_slot: Option<F> = Some(fetcher);
        loop {
            let role = self.acquire_role(&owned_key, expected_size);
            match role {
                Role::Bypass => {
                    let f = fetcher_slot
                        .take()
                        .expect("bypass: fetcher consumed at most once");
                    let chunks = f().await?;
                    return Ok(Arc::new(chunks));
                }
                Role::Leader { entry, tx } => {
                    let f = fetcher_slot
                        .take()
                        .expect("leader: fetcher consumed at most once");
                    return self.run_as_leader(entry, tx, f).await;
                }
                Role::Waiter(entry) => {
                    let entry_for_purge = Arc::clone(&entry);
                    match Self::run_as_waiter(entry).await {
                        WaitOutcome::Result(r) => return r,
                        WaitOutcome::Promote => {
                            // The leader was canceled. Forcibly purge
                            // the dead slot from the map so concurrent
                            // promoting waiters don't re-subscribe to
                            // it (which would bounce between Promote
                            // and re-park forever while siblings hold
                            // their own Arcs). The map entry is
                            // identified by this `entry_for_purge`'s
                            // identity (via Weak ptr_eq); we only
                            // purge if the map's Weak still points
                            // here — a successor leader's installation
                            // would have replaced it under the map
                            // lock.
                            self.purge_canceled_slot(&entry_for_purge);
                            drop(entry_for_purge);
                            continue;
                        }
                    }
                }
            }
        }
    }

    /// Atomically pick a role for this caller. Lock is held briefly.
    fn acquire_role(
        self: &Arc<Self>,
        owned_key: &StoreKeyBorrow,
        expected_size: u64,
    ) -> Role {
        let mut map = self.inner.map.lock();
        if let Some(weak) = map.get(owned_key)
            && let Some(entry) = weak.upgrade()
        {
            return Role::Waiter(entry);
        }
        // No live slot. Cap-check before becoming leader.
        let current = self.inner.current_inflight_bytes.load(Ordering::Relaxed);
        let new_total = current.saturating_add(expected_size);
        if new_total > self.inner.max_inflight_bytes {
            warn!(
                current_inflight_bytes = current,
                max_inflight_bytes = self.inner.max_inflight_bytes,
                expected_size,
                "singleflight cap exceeded — bypassing dedup, fetcher runs without registration"
            );
            return Role::Bypass;
        }
        // Reserve and install slot.
        self.inner
            .current_inflight_bytes
            .fetch_add(expected_size, Ordering::Relaxed);
        let (tx, rx) = watch::channel(None);
        let entry = Arc::new(InflightEntry {
            result_rx: rx,
            published: AtomicBool::new(false),
            parent: Arc::downgrade(&self.inner),
            key: owned_key.clone(),
            reserved_bytes: expected_size,
        });
        match map.entry(owned_key.clone()) {
            Entry::Vacant(v) => {
                v.insert(Arc::downgrade(&entry));
            }
            Entry::Occupied(mut o) => {
                // Pre-existing dead Weak — overwrite atomically under
                // the map lock.
                o.insert(Arc::downgrade(&entry));
            }
        }
        Role::Leader { entry, tx }
    }

    /// Leader path: run the fetcher, publish, return.
    async fn run_as_leader<F, Fut>(
        self: &Arc<Self>,
        entry: Arc<InflightEntry>,
        tx: watch::Sender<Option<Arc<Result<SingleflightPayload, Error>>>>,
        fetcher: F,
    ) -> Result<SingleflightPayload, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<Bytes>, Error>>,
    {
        let result: Result<SingleflightPayload, Error> = match fetcher().await {
            Ok(chunks) => Ok(Arc::new(chunks)),
            Err(e) => Err(e),
        };
        // Publish flag MUST flip BEFORE the watch send so any waiter
        // that races on `borrow_and_update` sees `published=true` and
        // can distinguish "leader-cancel" from "leader-published".
        entry.published.store(true, Ordering::Release);
        let arc_result = Arc::new(match &result {
            Ok(p) => Ok(Arc::clone(p)),
            Err(e) => Err(e.clone()),
        });
        // Send. Errors mean all receivers are gone (every waiter
        // dropped) — the leader still returns its own copy below.
        drop(tx.send(Some(arc_result)));
        // Hold `entry` until after publish so the Arc count stays
        // > 0 while waiters subscribe; otherwise a waiter that arrived
        // immediately after our send would find the slot gone.
        drop(entry);
        drop(tx);
        result
    }

    /// Waiter path: subscribe to leader's result. Returns Promote if
    /// the leader was canceled and the slot is gone (caller loops).
    async fn run_as_waiter(entry: Arc<InflightEntry>) -> WaitOutcome {
        let mut rx = entry.result_rx.clone();
        // Fast path: maybe the leader has already published before we
        // subscribed.
        {
            let current = rx.borrow_and_update().clone();
            if let Some(arc_result) = current {
                return WaitOutcome::Result(clone_result(&arc_result));
            }
        }
        match rx.changed().await {
            Ok(()) => {
                let v = rx.borrow_and_update().clone();
                if let Some(arc_result) = v {
                    return WaitOutcome::Result(clone_result(&arc_result));
                }
                // Sender produced None — shouldn't happen (initial
                // value already None and leader only sends Some).
                // Treat as cancel.
                drop(entry);
                WaitOutcome::Promote
            }
            Err(_recv_err) => {
                // Sender dropped. If `published` is true, the leader
                // sent the value but the channel closed before we
                // observed `changed()` — re-check borrow once.
                if entry.published.load(Ordering::Acquire) {
                    let v = rx.borrow().clone();
                    if let Some(arc_result) = v {
                        return WaitOutcome::Result(clone_result(&arc_result));
                    }
                }
                // Leader canceled. Drop our entry Arc to shrink the
                // slot refcount (potentially to 0 if we were the last
                // waiter), then signal promotion. The caller's loop
                // will re-acquire a role; if the slot has been fully
                // purged from the map, we become leader.
                drop(entry);
                WaitOutcome::Promote
            }
        }
    }

    /// Remove the dead slot identified by `canceled_entry` from the
    /// map IF the map still points at it. Idempotent across N
    /// concurrent waiters — the second caller's `Weak::ptr_eq` check
    /// fails (entry already removed) and is a no-op.
    fn purge_canceled_slot(self: &Arc<Self>, canceled_entry: &Arc<InflightEntry>) {
        let mut map = self.inner.map.lock();
        let still_present = map
            .get(&canceled_entry.key)
            .map(|w| w.as_ptr() == Arc::as_ptr(canceled_entry))
            .unwrap_or(false);
        if still_present {
            map.remove(&canceled_entry.key);
        }
    }

    /// Returns the current sum of reserved bytes across live slots.
    /// Test/observability only.
    #[must_use]
    pub fn current_inflight_bytes(&self) -> u64 {
        self.inner.current_inflight_bytes.load(Ordering::Relaxed)
    }

    /// Returns the current number of registered slots. Test only.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        let map = self.inner.map.lock();
        map.values().filter(|w| w.strong_count() > 0).count()
    }

    /// Returns the strong-ref count of the slot for `key`, or 0 if no
    /// slot exists. The leader holds 1 strong ref; each waiter parked
    /// inside `run_as_waiter` holds 1 more. So `strong_count_for_key
    /// == leader_present + n_subscribed_waiters`. Test/observability
    /// only — used by tests to deterministically wait for waiter
    /// subscription before exercising leader-cancel.
    #[must_use]
    pub fn strong_count_for_key(&self, key: &StoreKey<'_>) -> usize {
        let map = self.inner.map.lock();
        // Hash via the borrowed StoreKey — StoreKeyBorrow's Borrow impl
        // returns &StoreKey<'a>, so HashMap can look up by reference.
        // Use a temporary owned wrapper to avoid lifetime juggling.
        let owned: StoreKeyBorrow = key.borrow().into_owned().into();
        map.get(&owned).map_or(0, Weak::strong_count)
    }
}

enum Role {
    Bypass,
    Leader {
        entry: Arc<InflightEntry>,
        tx: watch::Sender<Option<Arc<Result<SingleflightPayload, Error>>>>,
    },
    Waiter(Arc<InflightEntry>),
}

enum WaitOutcome {
    Result(Result<SingleflightPayload, Error>),
    Promote,
}

/// Clone an `Arc<Result<Payload, Error>>` into a `Result<Payload, Error>`
/// where the Ok inner `Arc<Vec<Bytes>>` is reference-bumped (no copy)
/// and the Err is `Error::clone()` (cheap struct clone, refcounted
/// strings inside).
fn clone_result(
    arc_result: &Arc<Result<SingleflightPayload, Error>>,
) -> Result<SingleflightPayload, Error> {
    match &**arc_result {
        Ok(payload) => Ok(Arc::clone(payload)),
        Err(e) => Err(e.clone()),
    }
}
