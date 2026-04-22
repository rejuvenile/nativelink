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

//! Per-key construction coalescing with timeouts and error fan-out.
//!
//! Many caches in this codebase need to: when N tasks ask for value
//! `V` at key `K` at once and `V` is not yet computed, have ONE task
//! compute it and all N tasks receive the same result. This helper
//! provides that.
//!
//! Why a helper: every ad-hoc implementation has hit the same bugs:
//! - Leak the in-progress flag on cancellation (no RAII)
//! - Waiters retry from scratch when leader fails (livelock under
//!   bad upstream)
//! - No timeout on either leader or waiter (deadlock)
//!
//! [`with_construction_lock`] solves all three.
//!
//! # Example
//!
//! ```ignore
//! use core::time::Duration;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use parking_lot::Mutex;
//! use nativelink_util::coalesce::{with_construction_lock, CoalesceOptions, InFlightMap};
//!
//! let in_flight: InFlightMap<String, u64> = Arc::new(Mutex::new(HashMap::new()));
//! let result = with_construction_lock(
//!     &in_flight,
//!     "key".to_string(),
//!     CoalesceOptions::leader_only(Duration::from_secs(30)),
//!     || async { Ok(42_u64) },
//! ).await?;
//! # Ok::<(), nativelink_error::Error>(())
//! ```

use core::future::Future;
use core::hash::Hash;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use nativelink_error::{Code, Error, make_err};
use parking_lot::Mutex;
use tokio::sync::watch;
use tracing::warn;

/// Per-key in-flight map. The value channel fans out the leader's result
/// to all waiters. The receiver lives in the leader's task; on drop
/// (after the leader sends or panics), waiters wake up — `changed()`
/// returns `Err(RecvError)` if the channel was closed without a value.
///
/// Stored as `Receiver` (cloneable via subscribe) so new waiters can
/// subscribe without involving the leader directly.
pub type InFlightMap<K, V> = Arc<Mutex<HashMap<K, watch::Receiver<Option<Result<V, Error>>>>>>;

/// Coalescing options for [`with_construction_lock`].
///
/// `leader_timeout` bounds how long the leader's compute future may run.
/// `waiter_timeout` bounds how long each waiter may block awaiting the
/// leader's result. The waiter timeout should be slightly longer than
/// the leader timeout so waiters do not give up before the leader has
/// a chance to publish a (possibly error) result.
#[derive(Debug, Clone, Copy)]
pub struct CoalesceOptions {
    /// Timeout for the leader's compute future. `None` means no timeout.
    pub leader_timeout: Option<Duration>,
    /// Timeout for each waiter awaiting the leader's result. `None` means
    /// no timeout. Should typically be `leader_timeout + small_slack`.
    pub waiter_timeout: Option<Duration>,
}

impl CoalesceOptions {
    /// Convenience: bound the leader by `leader_timeout` and waiters by
    /// `leader_timeout + 1s`. The slack lets the leader publish its result
    /// (including a timeout error) before waiters give up independently.
    #[must_use]
    pub const fn leader_only(leader_timeout: Duration) -> Self {
        Self {
            leader_timeout: Some(leader_timeout),
            waiter_timeout: Some(Duration::from_secs(
                leader_timeout.as_secs().saturating_add(1),
            )),
        }
    }

    /// No timeouts. Callers must guarantee the compute future terminates
    /// (e.g., via an internal deadline) — otherwise both leader and
    /// waiters can hang forever.
    #[must_use]
    pub const fn no_timeout() -> Self {
        Self {
            leader_timeout: None,
            waiter_timeout: None,
        }
    }
}

/// RAII guard that removes the leader's entry from the in-flight map on
/// drop. Holds the [`watch::Sender`] so dropping the guard also drops
/// the sender, which wakes all waiters with a closed-channel error.
///
/// Identity check: the guard only removes the entry if it matches the
/// leader's own channel. This prevents a stale leader from removing a
/// fresh leader's entry after an ABA-style race (old leader cancelled,
/// new leader installed for the same key, old leader's drop fires).
struct LeaderGuard<K, V>
where
    K: Eq + Hash,
{
    in_flight: InFlightMap<K, V>,
    key: Option<K>,
    sender: Option<watch::Sender<Option<Result<V, Error>>>>,
}

impl<K, V> Drop for LeaderGuard<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let Some(sender) = self.sender.take() else {
            return;
        };
        // Subscribe BEFORE locking the map so we can do a cheap pointer
        // comparison against whatever Receiver is currently stored.
        let our_receiver = sender.subscribe();
        let mut map = self.in_flight.lock();
        if let std::collections::hash_map::Entry::Occupied(occupied) = map.entry(key) {
            // Only remove if the current entry's channel is OUR channel.
            // If a fresh leader has already replaced us, leave it alone.
            if occupied.get().same_channel(&our_receiver) {
                occupied.remove();
            }
        }
        // Drop the sender LAST so waiters subscribed to our channel
        // observe the closed-channel signal after the map entry is gone.
        // This ordering matters: a waiter that wakes up to a closed
        // channel and then re-enters with_construction_lock should see
        // the empty slot and become a fresh leader.
        drop(sender);
    }
}

/// Coalesce parallel callers requesting the same `key`.
///
/// - The first caller becomes the LEADER and runs `compute()`.
/// - All other callers become WAITERS and receive the leader's result via
///   a [`watch::Receiver`].
/// - On leader timeout: the leader sends a `DeadlineExceeded` error to
///   waiters and the entry is cleared. The next caller becomes a fresh
///   leader.
/// - On waiter timeout: that single waiter gets a `DeadlineExceeded`
///   error; other waiters and the leader are unaffected.
/// - On leader cancellation (future dropped) or panic: waiters wake up
///   to a closed channel and receive an `Aborted` error; the entry is
///   cleared by an internal RAII guard so the next caller becomes a
///   fresh leader.
///
/// # Invariants
///
/// - The `in_flight` lock is a synchronous `parking_lot::Mutex` and is
///   held only briefly — never across `await`.
/// - Insert-or-get-existing is atomic under the lock: either we install
///   ourselves as leader OR we receive an existing leader's receiver.
/// - The leader's compute future runs in the caller's task (not a
///   detached `tokio::spawn`) so cancellation propagates naturally.
pub async fn with_construction_lock<K, V, F, Fut>(
    in_flight: &InFlightMap<K, V>,
    key: K,
    options: CoalesceOptions,
    compute: F,
) -> Result<V, Error>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<V, Error>> + Send,
{
    // Atomic insert-or-subscribe under the sync lock.
    let role = {
        let mut map = in_flight.lock();
        match map.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(occupied) => {
                Role::Waiter(occupied.get().clone())
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let (tx, rx) = watch::channel(None);
                vacant.insert(rx);
                Role::Leader(tx)
            }
        }
    };

    match role {
        Role::Leader(sender) => run_as_leader(in_flight, key, options, sender, compute).await,
        Role::Waiter(receiver) => run_as_waiter(options, receiver).await,
    }
}

enum Role<V> {
    Leader(watch::Sender<Option<Result<V, Error>>>),
    Waiter(watch::Receiver<Option<Result<V, Error>>>),
}

async fn run_as_leader<K, V, F, Fut>(
    in_flight: &InFlightMap<K, V>,
    key: K,
    options: CoalesceOptions,
    sender: watch::Sender<Option<Result<V, Error>>>,
    compute: F,
) -> Result<V, Error>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<V, Error>> + Send,
{
    // Install the guard BEFORE awaiting compute so cancellation /
    // panic during compute still runs Drop and clears the slot.
    // The guard takes ownership of the Sender; we subscribe a local
    // Receiver so we can observe sends but don't have to share the
    // Sender across the await point.
    let guard = LeaderGuard {
        in_flight: Arc::clone(in_flight),
        key: Some(key),
        sender: Some(sender),
    };

    // Run compute, optionally with a timeout.
    let compute_result: Result<V, Error> = if let Some(timeout) = options.leader_timeout {
        match tokio::time::timeout(timeout, compute()).await {
            Ok(result) => result,
            Err(_) => {
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "coalesce: leader compute exceeded deadline",
                );
                Err(make_err!(
                    Code::DeadlineExceeded,
                    "coalesce: leader compute exceeded {}ms",
                    timeout.as_millis()
                ))
            }
        }
    } else {
        compute().await
    };

    // Publish the result to all waiters BEFORE the guard runs Drop
    // (which removes the entry from the map). After Drop, late waiters
    // calling with_construction_lock would otherwise install themselves
    // as fresh leaders and re-run compute. We want them to see the
    // result if the entry is still present at lookup time, so we send
    // first, then let Drop clear the slot.
    if let Some(sender) = guard.sender.as_ref() {
        // send_replace overwrites the value even if there are no
        // receivers. We deliberately ignore the result: it returns
        // the previous value, not an error.
        sender.send_replace(Some(compute_result.clone()));
    }

    // Guard Drop runs here, removing the entry from the map and
    // dropping the Sender (waking any subscribed waiters that have
    // not yet seen the value via changed()).
    compute_result
}

async fn run_as_waiter<V>(
    options: CoalesceOptions,
    mut receiver: watch::Receiver<Option<Result<V, Error>>>,
) -> Result<V, Error>
where
    V: Clone + Send + 'static,
{
    // Fast path: the leader may already have published a value before
    // we cloned the receiver. Check borrow() before awaiting changed().
    if let Some(result) = receiver.borrow().clone() {
        return result;
    }

    // Wait for the leader to publish (or the channel to close).
    let changed_fut = receiver.changed();
    let wait_outcome = if let Some(timeout) = options.waiter_timeout {
        match tokio::time::timeout(timeout, changed_fut).await {
            Ok(inner) => inner,
            Err(_) => {
                return Err(make_err!(
                    Code::DeadlineExceeded,
                    "coalesce: waiter timed out after {}ms waiting for leader",
                    timeout.as_millis()
                ));
            }
        }
    } else {
        changed_fut.await
    };

    if wait_outcome.is_err() {
        // Sender was dropped without publishing. This happens when the
        // leader's future is cancelled or panics. Caller may retry —
        // the entry has been (or will be) cleared by the guard.
        return Err(make_err!(
            Code::Aborted,
            "coalesce: leader was cancelled before publishing a result; caller may retry",
        ));
    }

    // changed() returned Ok — the leader published. Read and clone the
    // value. If the value is still None somehow (defensive), treat as
    // aborted.
    let value = receiver.borrow();
    match value.as_ref() {
        Some(result) => result.clone(),
        None => Err(make_err!(
            Code::Internal,
            "coalesce: changed() fired but value was still None",
        )),
    }
}
