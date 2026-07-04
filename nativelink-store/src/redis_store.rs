// Copyright 2024-2026 The NativeLink Authors. All rights reserved.
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

use core::cmp;
use core::fmt::{self, Debug};
use core::marker::PhantomData;
use core::ops::{Bound, RangeBounds};
use core::pin::Pin;
use core::str;
use core::str::FromStr;
use core::task::{Context, Poll};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use const_format::formatcp;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt, future};
use nativelink_config::stores::{RedisMode, RedisSpec};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_redis_tester::SubscriptionManagerNotify;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthRegistryBuilder, HealthStatus, HealthStatusIndicator};
use nativelink_util::{background_spawn, spawn};
use nativelink_util::store_trait::{
    BoolValue, ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, SchedulerCurrentVersionProvider,
    SchedulerIndexProvider, SchedulerStore, SchedulerStoreDataProvider, SchedulerStoreDecodeTo,
    SchedulerStoreKeyProvider, SchedulerSubscription, SchedulerSubscriptionManager,
    StableDigestDelegation, StoreDriver, StoreKey, UploadSizeInfo,
};
use nativelink_util::task::JoinHandleDropGuard;
use parking_lot::{Mutex, RwLock};
use patricia_tree::StringPatriciaMap;
use redis::aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::{
    AsyncCommands, AsyncIter, Client, IntoConnectionInfo, PushInfo, ScanOptions, Script, Value,
    pipe,
};
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, trace, warn};
use url::Url;
use uuid::Uuid;

use crate::cas_utils::is_zero_digest;
use crate::redis_utils::{
    FtAggregateCursor, FtAggregateOptions, FtCreateOptions, SearchSchema, ft_aggregate, ft_create,
};

/// The default size of the read chunk when reading data from Redis.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_READ_CHUNK_SIZE: usize = 64 * 1024;

/// The default size of the connection pool if not specified.
/// Note: If this changes it should be updated in the config documentation.
pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 3;

/// The default delay between retries if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_RETRY_DELAY: f32 = 0.1;

/// The default connection timeout in milliseconds if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 3000;

/// The default command timeout in milliseconds if not specified.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 10_000;

/// The default maximum number of chunk uploads per update.
/// Note: If this changes it should be updated in the config documentation.
pub const DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE: usize = 10;

/// The default COUNT value passed when scanning keys in Redis.
/// Note: If this changes it should be updated in the config documentation.
const DEFAULT_SCAN_COUNT: usize = 10_000;

/// The default COUNT value passed when scanning search indexes
/// Note: If this changes it should be updated in the config documentation.
pub const DEFAULT_MAX_COUNT_PER_CURSOR: u64 = 1_500;

/// Maximum number of keys per Redis pipeline batch. Larger batches are
/// chunked to avoid unbounded response buffering on the Redis connection.
const MAX_PIPELINE_BATCH: usize = 5000;

const DEFAULT_CLIENT_PERMITS: usize = 500;

/// Threshold above which a sampled redis call emits a slow-call warn
/// with `wall_ms` / `actor_ms` / `queue_ms` split. Tunable; 100ms is
/// roughly an order of magnitude above the typical Unix-socket Valkey
/// command (a few µs to ~100µs) so genuinely slow calls stand out.
const SLOW_CALL_WARN_THRESHOLD_MS: u64 = 100;

/// Sample 1-in-N redis calls for actor-poll timing instrumentation.
/// 100 → ~1% sample rate. The hot-path cost when SAMPLED is one
/// `Instant::now()` per `poll()` (single CLOCK_MONOTONIC syscall, ~10ns
/// on Linux). When NOT sampled the cost is one `AtomicUsize` modulo.
const TIMING_SAMPLE_RATE: usize = 100;

/// Process-wide counter that decides which redis calls get instrumented.
/// `Relaxed` is fine — we only need rough 1-in-N sampling, not strict
/// ordering.
static TIMING_SAMPLE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Returns true once every TIMING_SAMPLE_RATE invocations.
#[inline(always)]
fn should_sample_call() -> bool {
    TIMING_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed) % TIMING_SAMPLE_RATE == 0
}

/// Test-only: arrange that the next `should_sample_call()` call returns
/// true. Used by integration tests that need to deterministically hit
/// the sampled-and-logged branch of `instrument_redis_call` instead of
/// relying on probabilistic sampling.
#[doc(hidden)]
pub fn force_next_call_sampled() {
    // Set the counter so that the next `fetch_add(1) % TIMING_SAMPLE_RATE`
    // yields 0. After increment the counter advances; subsequent calls
    // resume normal 1-in-N sampling.
    let cur = TIMING_SAMPLE_COUNTER.load(Ordering::Relaxed);
    let target = cur.next_multiple_of(TIMING_SAMPLE_RATE);
    // Set so that the next fetch_add yields exactly target.
    TIMING_SAMPLE_COUNTER.store(target, Ordering::Relaxed);
}

/// Recorded timing for one redis call. `wall_ms` is end-to-end wall time
/// (issue → completion); `actor_ms` is the sum of poll-active durations
/// (CPU time inside the future); `queue_ms = wall_ms - actor_ms` is
/// time the future was suspended waiting to be polled.
///
/// Interpretation per red-team Step 1 of the valkey-pool-521c13d1
/// pivot recommendation:
/// - `queue_ms ≪ wall_ms`: the future is being polled but suspended
///   between polls — symptomatic of runtime starvation (e.g. tokio
///   workers tied up in a Moka mutex). Adding more redis connections
///   does not help; this is a calling-side bottleneck.
/// - `queue_ms ≈ 0, wall_ms ≈ actor_ms`: the future is actively polling
///   but the actor is slow returning a response — symptomatic of a
///   loaded socket / slow Valkey. A connection pool that round-robins
///   across N actors helps.
#[derive(Clone, Copy, Debug)]
pub struct RedisCallTiming {
    pub wall_ms: u64,
    pub actor_ms: u64,
    pub queue_ms: u64,
    pub poll_count: u32,
}

/// A `Future` adapter that records per-poll wall-clock timing and emits
/// a `tracing::warn!` (with `wall_ms`/`actor_ms`/`queue_ms` split) for
/// any wrapped call that exceeds [`SLOW_CALL_WARN_THRESHOLD_MS`]. Only
/// constructed for sampled calls (1-in-[`TIMING_SAMPLE_RATE`]) — the
/// hot path that picks the un-sampled branch sees zero allocation and
/// one atomic increment.
///
/// The `actor_ms` accounting is the sum of `poll()` durations: every
/// time the wrapped future is polled, we record `Instant::now()` on
/// entry and add the elapsed on exit (whether Ready or Pending). The
/// `wall_ms` is just `last_seen.elapsed()` from the issue-time
/// `start`. Difference is `queue_ms` — time the future was suspended
/// between polls.
pub struct TimedRedisCall<F> {
    inner: F,
    start: Instant,
    actor_total_nanos: u128,
    poll_count: u32,
}

impl<F> fmt::Debug for TimedRedisCall<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Omit `inner` since `F: Future` doesn't carry a `Debug` bound.
        f.debug_struct("TimedRedisCall")
            .field("start", &self.start)
            .field("actor_total_nanos", &self.actor_total_nanos)
            .field("poll_count", &self.poll_count)
            .field("inner", &"<future>")
            .finish()
    }
}

impl<F> TimedRedisCall<F> {
    /// Wrap `inner` in a timing recorder. Caller is responsible for
    /// reading `take_timing()` after the future resolves OR using
    /// `into_future_with_log` which logs on drop / on completion.
    pub fn new(inner: F) -> Self {
        Self {
            inner,
            start: Instant::now(),
            actor_total_nanos: 0,
            poll_count: 0,
        }
    }
}

impl<F: Future> Future for TimedRedisCall<F> {
    type Output = (F::Output, RedisCallTiming);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: we never move `inner` out; pin-projection via raw deref.
        let this = unsafe { self.get_unchecked_mut() };
        let inner_pin = unsafe { Pin::new_unchecked(&mut this.inner) };
        let poll_start = Instant::now();
        let result = inner_pin.poll(cx);
        let poll_elapsed = poll_start.elapsed().as_nanos();
        this.actor_total_nanos = this.actor_total_nanos.saturating_add(poll_elapsed);
        this.poll_count = this.poll_count.saturating_add(1);
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(out) => {
                let wall_ms_u128 = this.start.elapsed().as_millis();
                let actor_ms_u128 = this.actor_total_nanos / 1_000_000;
                let wall_ms = u64::try_from(wall_ms_u128).unwrap_or(u64::MAX);
                let actor_ms = u64::try_from(actor_ms_u128).unwrap_or(u64::MAX);
                let queue_ms = wall_ms.saturating_sub(actor_ms);
                Poll::Ready((
                    out,
                    RedisCallTiming {
                        wall_ms,
                        actor_ms,
                        queue_ms,
                        poll_count: this.poll_count,
                    },
                ))
            }
        }
    }
}

/// Helper: wrap a redis-call future in a `TimedRedisCall` and emit a
/// `tracing::warn!` if `wall_ms` exceeds the threshold. Returns the
/// inner future's `Output` unchanged. When sampling is OFF, the cost
/// per call is ONE atomic increment (Relaxed) — no allocation, no
/// extra Instant::now in the hot path.
///
/// The `key_for_log` argument is the cardinality-bound logging tag —
/// for pipelined batches it's `"pipelined STRLEN+EXISTS x{n}"`; for
/// per-key calls it's the encoded key string.
pub async fn instrument_redis_call<F, T>(cmd: &'static str, key_for_log: &str, fut: F) -> T
where
    F: Future<Output = T>,
{
    if should_sample_call() {
        let (output, timing) = TimedRedisCall::new(fut).await;
        if timing.wall_ms >= SLOW_CALL_WARN_THRESHOLD_MS {
            warn!(
                cmd,
                key = %key_for_log,
                wall_ms = timing.wall_ms,
                actor_ms = timing.actor_ms,
                queue_ms = timing.queue_ms,
                poll_count = timing.poll_count,
                "redis call slow: wall vs actor (sampled, see CLAUDE.md valkey-pool red-team Step 1)"
            );
        }
        output
    } else {
        fut.await
    }
}

/// A wrapper around Redis to allow it to be reconnected.
pub trait RedisManager<C>
where
    C: ConnectionLike + Clone,
{
    /// Get a connection manager and a unique identifier for this connection
    /// which may be used to issue a reconnect later.
    fn get_connection(&self) -> impl Future<Output = Result<(C, Uuid), Error>> + Send;

    /// Reconnect if the uuid matches the uuid returned from `get_connection()`.
    fn reconnect(&self, uuid: Uuid) -> impl Future<Output = Result<(C, Uuid), Error>> + Send;

    /// Get an invocation of the update version script for a given `key`.
    fn update_script(&self, key: &str) -> redis::ScriptInvocation<'_>;

    /// Configure the connection to have a psubscribe on it and perform the
    /// subscription on reconnect.
    fn psubscribe(&self, pattern: &str) -> impl Future<Output = Result<(), Error>> + Send;

    /// Record the `notify-keyspace-events` flag set the keyspace dispatcher
    /// requires. Implementations re-apply the set to the subscriber-slot
    /// connection on every reconnect so a Redis/Valkey restart that reverts
    /// the in-memory CONFIG does not silently disable the dispatcher. No-op
    /// for cluster mode (cluster-mode keyspace notifications are documented
    /// as undefined and the store force-disables them).
    fn set_keyspace_events_flags(&self, flags: String);
}

#[derive(Debug)]
pub struct ClusterRedisManager<C>
where
    C: ConnectionLike + Clone,
{
    /// A constant Uuid, we never reconnect.
    uuid: Uuid,

    /// Redis script used to update a value in redis if the version matches.
    /// This is done by incrementing the version number and then setting the new
    /// data only if the version number matches the existing version number.
    update_if_version_matches_script: Script,

    /// The client pool connecting to the backing Redis instance(s).
    connection_manager: C,
}

impl<C> ClusterRedisManager<C>
where
    C: ConnectionLike + Clone,
{
    pub async fn new(mut connection_manager: C) -> Result<Self, Error> {
        let update_if_version_matches_script = Script::new(LUA_VERSION_SET_SCRIPT);
        update_if_version_matches_script
            .load_async(&mut connection_manager)
            .await?;
        Ok(Self {
            uuid: Uuid::new_v4(),
            update_if_version_matches_script,
            connection_manager,
        })
    }
}

impl<C> RedisManager<C> for ClusterRedisManager<C>
where
    C: ConnectionLike + Clone + Send + Sync,
{
    fn get_connection(&self) -> impl Future<Output = Result<(C, Uuid), Error>> + Send {
        future::ready(Ok((self.connection_manager.clone(), self.uuid)))
    }

    fn reconnect(&self, _uuid: Uuid) -> impl Future<Output = Result<(C, Uuid), Error>> + Send {
        self.get_connection()
    }

    fn update_script(&self, key: &str) -> redis::ScriptInvocation<'_> {
        self.update_if_version_matches_script.key(key)
    }

    fn psubscribe(&self, _pattern: &str) -> impl Future<Output = Result<(), Error>> + Send {
        // This is a no-op for cluster connections.
        future::ready(Ok(()))
    }

    fn set_keyspace_events_flags(&self, _flags: String) {
        // No-op: keyspace notifications are force-disabled for cluster mode
        // (semantics undefined per Redis docs); see `RedisStore::new_cluster`.
    }
}

type RedisConnectFuture<C> = dyn Future<Output = Result<C, Error>> + Send;
type RedisConnectFn<C> = dyn Fn() -> Pin<Box<RedisConnectFuture<C>>> + Send + Sync;

/// The dedicated slot index used for pubsub subscriptions. Pubsub state is
/// per-connection in Redis (subscribing on N connections would deliver each
/// message N times), so we pin all psubscribe traffic to a single slot and
/// re-apply the subscription set when that slot reconnects.
const SUBSCRIBER_SLOT: usize = 0;

pub struct StandardRedisManager<C>
where
    C: ConnectionLike + Clone,
{
    /// Function used to re-connect to Redis.
    connect_func: Box<RedisConnectFn<C>>,

    /// Redis script used to update a value in redis if the version matches.
    /// This is done by incrementing the version number and then setting the new
    /// data only if the version number matches the existing version number.
    update_if_version_matches_script: Script,

    /// Pool of connection managers. Each entry is its own multiplexed
    /// connection with an independent Uuid (so reconnect can target the right
    /// slot without disturbing others). Round-robin across the pool spreads
    /// in-flight commands across N connections instead of pipelining
    /// everything through one. Slot [`SUBSCRIBER_SLOT`] is the dedicated
    /// pubsub subscriber connection — see the const doc for why.
    connections: Vec<tokio::sync::RwLock<(C, Uuid)>>,

    /// Round-robin selector for `get_connection`. Wraps via modulo and only
    /// needs `Relaxed` ordering — distribution doesn't have to be exact, just
    /// uniform on average.
    next_slot: AtomicUsize,

    /// A list of subscriptions that should be performed on reconnect of the
    /// subscriber slot.
    subscriptions: Mutex<HashSet<String>>,

    /// `notify-keyspace-events` flag set required for the keyspace dispatcher
    /// to function. When `Some`, every reconnect of the subscriber slot
    /// re-issues `CONFIG SET notify-keyspace-events <flags>` BEFORE replaying
    /// `PSUBSCRIBE`. This closes the silent-degradation hole where a Valkey
    /// restart reverts the in-memory CONFIG to whatever `valkey.conf` says
    /// (typically empty) — without this re-issue the dispatcher runs
    /// against a server that emits no keyevent traffic.
    keyspace_events_flags: Mutex<Option<String>>,
}

impl<C> Debug for StandardRedisManager<C>
where
    C: ConnectionLike + Clone,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StandardRedisManager")
            .field(
                "update_if_version_matches_script",
                &self.update_if_version_matches_script,
            )
            .field("pool_size", &self.connections.len())
            .field("subscriptions", &self.subscriptions)
            .finish()
    }
}

impl<C> StandardRedisManager<C>
where
    C: ConnectionLike + Clone + Send + Sync,
{
    async fn configure(&self, connection_manager: &mut C) -> Result<(), Error> {
        self.update_if_version_matches_script
            .load_async(connection_manager)
            .await?;
        Ok(())
    }

    /// Create a manager with a pool of `pool_size` connections. `pool_size`
    /// is clamped to at least 1 so the manager always has a slot to serve.
    /// Connections are dialed in parallel so total startup wall-clock equals
    /// the slowest single dial, not N × dial time — important for large
    /// pools (e.g. `connection_pool_size: 64` in production).
    pub async fn new_with_pool_size(
        connect_func: Box<RedisConnectFn<C>>,
        pool_size: usize,
    ) -> Result<Self, Error> {
        let pool_size = pool_size.max(1);
        let connect_futs = (0..pool_size).map(|_| connect_func());
        let raw_connections = future::try_join_all(connect_futs).await?;
        let connections = raw_connections
            .into_iter()
            .map(|c| tokio::sync::RwLock::new((c, Uuid::new_v4())))
            .collect::<Vec<_>>();
        let update_if_version_matches_script = Script::new(LUA_VERSION_SET_SCRIPT);
        let manager = Self {
            connect_func,
            update_if_version_matches_script,
            connections,
            next_slot: AtomicUsize::new(0),
            subscriptions: Mutex::new(HashSet::new()),
            keyspace_events_flags: Mutex::new(None),
        };
        // Configure (script preload) per slot. Sequential is fine here — the
        // dials are done; this is just sending one SCRIPT LOAD per slot.
        for slot in &manager.connections {
            let mut guard = slot.write().await;
            manager.configure(&mut guard.0).await?;
        }
        Ok(manager)
    }

    /// Number of connections in the pool. Useful for assertions and metrics.
    pub fn pool_size(&self) -> usize {
        self.connections.len()
    }

    /// Atomically pick the next round-robin slot index.
    ///
    /// Exposed primarily so tests can assert distribution without depending on
    /// the full `RedisManager` impl (which is `ConnectionManager`-specific
    /// because it needs to call the inherent `psubscribe` method).
    pub fn pick_slot(&self) -> usize {
        let pool_size = self.connections.len();
        // Modulo on usize wrap is safe — `Relaxed` is fine because we only
        // need a uniform distribution on average, not a strict ordering.
        self.next_slot.fetch_add(1, Ordering::Relaxed) % pool_size
    }

    /// Generic get_connection that works for any C; the trait impl below
    /// just delegates to this. Tests use it directly to assert distribution.
    pub async fn get_connection_generic(&self) -> Result<(C, Uuid), Error> {
        let idx = self.pick_slot();
        Ok(self.connections[idx].read().await.clone())
    }

    /// Test-only: take a read-lock on a specific slot index. Used by
    /// pubsub-pinning tests that observe which slot's write-lock
    /// `psubscribe_with` acquires by detecting blocking on a held read.
    /// Returns `None` if `slot_idx >= pool_size`.
    #[doc(hidden)]
    pub async fn debug_read_slot(
        &self,
        slot_idx: usize,
    ) -> Option<tokio::sync::RwLockReadGuard<'_, (C, Uuid)>> {
        let slot = self.connections.get(slot_idx)?;
        Some(slot.read().await)
    }

    /// Test/observability helper: returns the set of subscription patterns
    /// currently tracked for replay on subscriber-slot reconnect. Used to
    /// verify that `RedisStore::new_standard` subscribed at construction
    /// time when `experimental_pub_sub_channel` is configured.
    #[doc(hidden)]
    pub fn debug_subscriptions(&self) -> Vec<String> {
        self.subscriptions.lock().iter().cloned().collect()
    }

    /// Generic psubscribe that delegates the actual SUBSCRIBE-issuing call
    /// to the supplied async callback. The structural invariant — pubsub
    /// state is per-connection in Redis, so subscribing on N connections
    /// would deliver each message N times — is enforced here: only
    /// [`SUBSCRIBER_SLOT`]'s write-lock is acquired, the callback is
    /// invoked exactly once with that single slot's connection, and the
    /// pattern is recorded in `subscriptions` for replay on reconnect of
    /// that slot.
    ///
    /// `RedisManager<ConnectionManager>` calls this with a closure that
    /// invokes the inherent `ConnectionManager::psubscribe` method. Tests
    /// can pass a recording closure that observes which slot's connection
    /// was passed in to verify the pinning.
    pub async fn psubscribe_with<F>(
        &self,
        pattern: &str,
        psubscribe_call: F,
    ) -> Result<(), Error>
    where
        F: for<'a> FnOnce(
                &'a mut C,
                &'a str,
            ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>
            + Send,
    {
        // Pubsub is pinned to the subscriber slot — see SUBSCRIBER_SLOT doc.
        let new_subscription = self.subscriptions.lock().insert(String::from(pattern));
        if new_subscription {
            let mut guard = self.connections[SUBSCRIBER_SLOT].write().await;
            let result = psubscribe_call(&mut guard.0, pattern).await;
            if result.is_err() {
                self.subscriptions.lock().remove(pattern);
            }
            result?;
        }
        Ok(())
    }

    /// Generic reconnect that delegates the per-pattern psubscribe replay
    /// to the supplied async callback. Same structural invariant as
    /// `psubscribe_with`: only [`SUBSCRIBER_SLOT`]'s reconnect path
    /// re-applies subscriptions; all other slots never carry pubsub.
    ///
    /// When `keyspace_events_flags` is `Some(flags)` (set via
    /// [`Self::set_keyspace_events_flags`]) the subscriber slot ALSO has
    /// `CONFIG SET notify-keyspace-events <flags>` re-issued before the
    /// `PSUBSCRIBE` replay. This is mandatory because Redis `CONFIG SET`
    /// mutates the LIVE in-memory config only; a Valkey restart reverts to
    /// `valkey.conf` (typically empty), so without re-issue the dispatcher
    /// runs against a server that emits no keyevent traffic — the very wedge
    /// the keyspace dispatcher exists to close re-emerges silently.
    pub async fn reconnect_with<F>(
        &self,
        uuid: Uuid,
        psubscribe_call: F,
    ) -> Result<(C, Uuid), Error>
    where
        F: for<'a> Fn(
                &'a mut C,
                String,
            ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>
            + Send
            + Sync,
    {
        // Find the slot that owns this uuid. Linear scan is fine — pool size
        // is in the dozens at most.
        //
        // NOTE: the write-guard is held across the TCP dial
        // `(self.connect_func)().await?` and the `configure()` call (SCRIPT
        // LOAD, and for the subscriber slot also CONFIG SET + PSUBSCRIBE). All
        // concurrent `get_connection()` callers that hash to this slot stall
        // behind this write-guard for the duration — up to `connection_timeout_ms`.
        // With FU-10 adding 10 error-path callers, the blast radius is larger:
        // if `n` slots all timeout simultaneously every `get_connection()` waits
        // up to `n × connection_timeout_ms` in the worst case.
        // TODO(FU-13): build the new connection OUTSIDE the lock and swap under
        // it — eliminates this stall window entirely. See
        // `.claude/audits/followups-2026-06-13-batch.md`.
        for (slot_idx, slot) in self.connections.iter().enumerate() {
            let mut guard = slot.write().await;
            if guard.1 != uuid {
                continue;
            }
            let mut connection_manager = (self.connect_func)().await?;
            let new_uuid = Uuid::new_v4();
            self.configure(&mut connection_manager).await?;
            // Only the subscriber slot needs subscriptions re-applied. Other
            // slots never carry pubsub traffic.
            if slot_idx == SUBSCRIBER_SLOT {
                let flags = self.keyspace_events_flags.lock().clone();
                if let Some(flags) = flags {
                    let _: () = redis::cmd("CONFIG")
                        .arg("SET")
                        .arg("notify-keyspace-events")
                        .arg(&flags)
                        .query_async(&mut connection_manager)
                        .await
                        .err_tip(|| {
                            "CONFIG SET notify-keyspace-events on subscriber-slot reconnect \
                             failed; the operator should persist the flags in valkey.conf so \
                             reconnects do not depend on runtime ACL grants"
                        })?;
                }
                let subscriptions = {
                    let guard = self.subscriptions.lock();
                    guard.iter().cloned().collect::<Vec<_>>()
                };
                for subscription in subscriptions {
                    psubscribe_call(&mut connection_manager, subscription).await?;
                }
            }
            *guard = (connection_manager.clone(), new_uuid);
            return Ok((connection_manager, new_uuid));
        }
        // Uuid no longer matches any slot — caller's connection was already
        // rotated by a prior reconnect. Hand back a fresh one via round-robin.
        self.get_connection_generic().await
    }

    /// Record the `notify-keyspace-events` flag set the keyspace dispatcher
    /// requires. The set is re-applied to the subscriber slot on every
    /// reconnect so a Valkey restart that reverts the in-memory CONFIG does
    /// not silently disable the dispatcher.
    ///
    /// Set BEFORE the dispatcher psubscribes — once a reconnect interleaves
    /// with the initial CONFIG SET, the racing reconnect could subscribe
    /// before this flag is recorded, leaving us subscribed to a server with
    /// no notifications enabled. Production callers
    /// (`init_keyspace_dispatcher_eager`) call this immediately after the
    /// initial CONFIG SET succeeds.
    pub fn set_keyspace_events_flags(&self, flags: String) {
        *self.keyspace_events_flags.lock() = Some(flags);
    }
}

impl RedisManager<ConnectionManager> for StandardRedisManager<ConnectionManager> {
    async fn get_connection(&self) -> Result<(ConnectionManager, Uuid), Error> {
        self.get_connection_generic().await
    }

    async fn reconnect(&self, uuid: Uuid) -> Result<(ConnectionManager, Uuid), Error> {
        self.reconnect_with(uuid, |cm, pattern| {
            Box::pin(async move {
                cm.psubscribe(&pattern)
                    .await
                    .err_tip(|| format!("psubscribe replay on reconnect for pattern {pattern}"))
            })
        })
        .await
    }

    fn update_script(&self, key: &str) -> redis::ScriptInvocation<'_> {
        self.update_if_version_matches_script.key(key)
    }

    async fn psubscribe(&self, pattern: &str) -> Result<(), Error> {
        self.psubscribe_with(pattern, |cm, pattern| {
            Box::pin(async move {
                cm.psubscribe(pattern)
                    .await
                    .err_tip(|| format!("psubscribe for pattern {pattern}"))
            })
        })
        .await
    }

    fn set_keyspace_events_flags(&self, flags: String) {
        // Delegate to the inherent method on `StandardRedisManager` so the
        // trait method has the same effect regardless of how the caller
        // holds the manager.
        Self::set_keyspace_events_flags(self, flags);
    }
}

/// A [`StoreDriver`] implementation that uses Redis as a backing store.
#[derive(MetricsComponent)]
pub struct RedisStore<C, M>
where
    C: ConnectionLike + Clone,
    M: RedisManager<C>,
{
    /// The client pool connecting to the backing Redis instance(s).
    connection_manager: M,

    /// The underlying connection type in the connection manager.
    _connection_type: PhantomData<C>,

    /// A channel to publish updates to when a key is added, removed, or modified.
    #[metric(
        help = "The pubsub channel to publish updates to when a key is added, removed, or modified"
    )]
    pub_sub_channel: Option<String>,

    /// A function used to generate names for temporary keys.
    temp_name_generator_fn: fn() -> String,

    /// A common prefix to append to all keys before they are sent to Redis.
    ///
    /// See [`RedisStore::key_prefix`](`nativelink_config::stores::RedisStore::key_prefix`).
    #[metric(help = "Prefix to append to all keys before sending to Redis")]
    key_prefix: String,

    /// The amount of data to read from Redis at a time.
    #[metric(help = "The amount of data to read from Redis at a time")]
    read_chunk_size: usize,

    /// The maximum number of chunk uploads per update.
    /// This is used to limit the number of chunk uploads per update to prevent
    /// overloading when uploading large blocks of data
    #[metric(help = "The maximum number of chunk uploads per update")]
    max_chunk_uploads_per_update: usize,

    /// The COUNT value passed when scanning keys in Redis.
    /// This is used to hint the amount of work that should be done per response.
    #[metric(help = "The COUNT value passed when scanning keys in Redis")]
    scan_count: usize,

    /// The COUNT value used with search indexes
    #[metric(help = "The maximum number of results to return per cursor")]
    max_count_per_cursor: u64,

    /// A manager for subscriptions to keys in Redis.
    subscription_manager: tokio::sync::OnceCell<Arc<RedisSubscriptionManager>>,

    /// Channel for getting subscription messages.
    ///
    // UNBOUNDED-OK: Redis push events (`__keyevent@<db>__:{del,expired,evicted}`)
    // arrive on the redis crate's `ConnectionManager` push channel and feed
    // straight into `subscriber_channel`. Producer rate ceiling is the Valkey
    // server's `notify-keyspace-events` emit rate; under sustained eviction
    // storm a single Valkey instance dispatches O(10^4) events/sec at the
    // upper bound. The consumer side — `run_keyspace_dispatcher` — drains
    // this receiver in a tight `select!` loop and fan-outs to registered
    // `ItemCallback`s in a `JoinSet` capped by the per-callback work
    // (production CAS-side ECS callback `tokio::spawn`s a single `remove`
    // per event, drained at >1M events/sec across the 16 EvictingMap shards
    // per `feedback_blob_missing_investigation.md`). Worst-case queue depth =
    // emit_rate × callback_latency × 1/drain_concurrency ≈ 10^4/s × 10ms /
    // 16 = 6.25 entries steady-state; 1000× burst still ~ 6KB at one
    // pointer-sized PushInfo per slot. Cannot be attacker-controlled: keys
    // are NativeLink-prefixed under operator-supplied `key_prefix`, payload
    // size capped at `KEYSPACE_PAYLOAD_MAX_LEN`. (See perf-optimizer M1
    // analysis 2026-05-09 + DSR M2 + code-reviewer S1.)
    subscriber_channel: Mutex<Option<UnboundedReceiver<PushInfo>>>,

    /// Permits to limit inflight Redis requests. Technically only
    /// limits the calls to `get_client()`, but the requests per client
    /// are small enough that it works well enough.
    client_permits: Arc<Semaphore>,

    /// Per-command timeout safety net. Set to 2x the configured
    /// command_timeout_ms so the redis crate's internal response_timeout
    /// fires first under normal conditions. This outer timeout only
    /// triggers when the redis crate's timeout mechanism itself fails
    /// (reconnect races, cluster retries, connection pool stalls).
    /// Without this, a hung command could silently return empty data
    /// instead of an error.
    #[metric(help = "Per-command timeout safety net in milliseconds")]
    command_timeout: Duration,

    /// See [`RedisSpec::enable_keyspace_notifications`]. Set at construction;
    /// gates the keyspace-notification dispatcher in `register_item_callback`.
    enable_keyspace_notifications: bool,

    /// See [`RedisSpec::keyspace_notifications_db`]. Set at construction;
    /// determines the `__keyevent@<db>__:*` channel pattern.
    keyspace_notifications_db: u8,

    /// Sender into the keyspace dispatcher task. Initialized lazily on the
    /// first successful `register_item_callback`. The dispatcher task owns
    /// the `subscriber_channel`, the corresponding receiver, and the
    /// `Vec<Arc<dyn ItemCallback>>` of registered listeners; it exits when
    /// this sender (and any clones) drop.
    ///
    // UNBOUNDED-OK: producer side is `register_item_callback`, called once
    // per wrapper at construction time (current production: a single
    // `ExistenceCacheStore::new_with_time` per `RedisStore`). Cannot be
    // attacker-controlled: the only callers in the workspace are
    // wrapper-store constructors invoked from server bootstrap; an external
    // RPC has no path to `register_item_callback`. Steady-state queue depth
    // is 0–1 (one push at construction, drained immediately into
    // `callbacks: Vec<...>` in `run_keyspace_dispatcher`'s select loop).
    // (See code-reviewer S1 + DSR M2.)
    keyspace_dispatcher_tx: tokio::sync::OnceCell<UnboundedSender<Arc<dyn ItemCallback>>>,

    /// Counter: total keyevent payloads dispatched to the callback list.
    /// Incremented after the payload-validation gate; observable via metrics.
    #[metric(help = "Total Redis keyevent payloads dispatched to ItemCallbacks")]
    keyspace_events_dispatched: AtomicU64,

    /// Counter: payloads dropped because they exceeded
    /// [`KEYSPACE_PAYLOAD_MAX_LEN`]. Observable via metrics.
    #[metric(help = "Redis keyevent payloads dropped for exceeding the size cap")]
    keyspace_payload_too_long_dropped: AtomicU64,

    /// Counter: total `register_item_callback` successes. Used by the
    /// inert-dispatcher 60s startup warn (see `init_keyspace_dispatcher_eager`):
    /// when `enable_keyspace_notifications=true` but this counter is still 0
    /// at +60s, the dispatcher is firing notifications into a void and the
    /// operator gets a `warn!` so they can either disable keyspace
    /// notifications or wire up the wrapper that forgot to register.
    #[metric(help = "Total successful register_item_callback calls")]
    callbacks_registered_total: AtomicU64,
}

impl<C, M> Debug for RedisStore<C, M>
where
    C: ConnectionLike + Clone,
    M: RedisManager<C>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RedisStore")
            .field("temp_name_generator_fn", &self.temp_name_generator_fn)
            .field("key_prefix", &self.key_prefix)
            .field("read_chunk_size", &self.read_chunk_size)
            .field(
                "max_chunk_uploads_per_update",
                &self.max_chunk_uploads_per_update,
            )
            .field("scan_count", &self.scan_count)
            .field("subscription_manager", &self.subscription_manager)
            .field("subscriber_channel", &self.subscriber_channel)
            .field(
                "enable_keyspace_notifications",
                &self.enable_keyspace_notifications,
            )
            .field(
                "keyspace_notifications_db",
                &self.keyspace_notifications_db,
            )
            .field("client_permits", &self.client_permits)
            .finish()
    }
}

struct ClientWithPermit<C: ConnectionLike> {
    connection_manager: C,
    uuid: Uuid,

    // here so it sticks around with the client and doesn't get dropped until that does
    #[allow(dead_code)]
    semaphore_permit: OwnedSemaphorePermit,
}

impl<C: ConnectionLike + Clone> ClientWithPermit<C> {
    async fn reconnect<M: RedisManager<C> + Sync>(&mut self, manager: &M) -> Result<(), Error> {
        (self.connection_manager, self.uuid) = manager.reconnect(self.uuid).await?;
        Ok(())
    }

    /// Best-effort reconnect on the inner-`response_timeout` error path.
    /// Logs the outcome so an operator can distinguish "timeout+reconnect-ok"
    /// from "timeout+reconnect-failed" in production logs (FU-10 S1).
    async fn reconnect_on_timeout<M: RedisManager<C> + Sync>(
        &mut self,
        manager: &M,
        context: &'static str,
    ) {
        match self.reconnect(manager).await {
            Ok(()) => {
                debug!(context, "redis slot reconnect succeeded after inner response_timeout");
            }
            Err(ref e) => {
                warn!(
                    context,
                    err = ?e,
                    "redis slot reconnect failed after inner response_timeout \
                     — slot remains desynced until next reconnect attempt"
                );
            }
        }
    }
}

/// Best-effort reconnect by `uuid` on the inner-`response_timeout` error path for
/// call sites that hold a raw connection manager rather than a `ClientWithPermit`.
/// Logs the outcome so an operator can distinguish "timeout+reconnect-ok" from
/// "timeout+reconnect-failed" (FU-10 S1).
async fn reconnect_on_timeout_by_uuid<C, M>(manager: &M, uuid: Uuid, context: &'static str)
where
    C: ConnectionLike + Clone,
    M: RedisManager<C> + Sync,
{
    match manager.reconnect(uuid).await {
        Ok(_) => {
            debug!(context, "redis slot reconnect succeeded after inner response_timeout");
        }
        Err(ref e) => {
            warn!(
                context,
                err = ?e,
                "redis slot reconnect failed after inner response_timeout \
                 — slot remains desynced until next reconnect attempt"
            );
        }
    }
}

impl<C: ConnectionLike> Drop for ClientWithPermit<C> {
    fn drop(&mut self) {
        trace!(
            remaining = self.semaphore_permit.semaphore().available_permits(),
            "Dropping a client permit"
        );
    }
}

impl<C, M> RedisStore<C, M>
where
    C: ConnectionLike + Clone + Sync,
    M: RedisManager<C> + Sync,
{
    /// Used for testing when determinism is required.
    #[expect(clippy::too_many_arguments)]
    pub async fn new_from_builder_and_parts(
        pub_sub_channel: Option<String>,
        temp_name_generator_fn: fn() -> String,
        key_prefix: String,
        read_chunk_size: usize,
        max_chunk_uploads_per_update: usize,
        scan_count: usize,
        max_client_permits: usize,
        max_count_per_cursor: u64,
        command_timeout: Duration,
        subscriber_channel: UnboundedReceiver<PushInfo>,
        connection_manager: M,
        enable_keyspace_notifications: bool,
        keyspace_notifications_db: u8,
    ) -> Result<Self, Error> {
        info!("Redis index fingerprint: {FINGERPRINT_CREATE_INDEX_HEX}");

        Ok(Self {
            connection_manager,
            _connection_type: PhantomData,
            pub_sub_channel,
            temp_name_generator_fn,
            key_prefix,
            read_chunk_size,
            max_chunk_uploads_per_update,
            scan_count,
            subscription_manager: tokio::sync::OnceCell::new(),
            subscriber_channel: Mutex::new(Some(subscriber_channel)),
            client_permits: Arc::new(Semaphore::new(max_client_permits)),
            max_count_per_cursor,
            command_timeout,
            enable_keyspace_notifications,
            keyspace_notifications_db,
            keyspace_dispatcher_tx: tokio::sync::OnceCell::new(),
            keyspace_events_dispatched: AtomicU64::new(0),
            keyspace_payload_too_long_dropped: AtomicU64::new(0),
            callbacks_registered_total: AtomicU64::new(0),
        })
    }

    async fn get_client(&self) -> Result<ClientWithPermit<C>, Error> {
        let local_client_permits = self.client_permits.clone();
        let remaining = local_client_permits.available_permits();
        let semaphore_permit = local_client_permits.acquire_owned().await?;
        trace!(remaining, "Got a client permit");
        let (connection_manager, uuid) = self.connection_manager.get_connection().await?;
        Ok(ClientWithPermit {
            connection_manager,
            uuid,
            semaphore_permit,
        })
    }

    /// Encode a [`StoreKey`] so it can be sent to Redis.
    fn encode_key<'a>(&self, key: &'a StoreKey<'a>) -> Cow<'a, str> {
        let key_body = key.as_str();
        if self.key_prefix.is_empty() {
            key_body
        } else {
            // This is in the hot path for all redis operations, so we try to reuse the allocation
            // from `key.as_str()` if possible.
            match key_body {
                Cow::Owned(mut encoded_key) => {
                    encoded_key.insert_str(0, &self.key_prefix);
                    Cow::Owned(encoded_key)
                }
                Cow::Borrowed(body) => {
                    let mut encoded_key = String::with_capacity(self.key_prefix.len() + body.len());
                    encoded_key.push_str(&self.key_prefix);
                    encoded_key.push_str(body);
                    Cow::Owned(encoded_key)
                }
            }
        }
    }

    fn set_spec_defaults(spec: &mut RedisSpec) -> Result<(), Error> {
        if spec.addresses.is_empty() {
            return Err(make_err!(
                Code::InvalidArgument,
                "No addresses were specified in redis store configuration."
            ));
        }

        if spec.broadcast_channel_capacity != 0 {
            warn!("broadcast_channel_capacity in Redis spec is deprecated and ignored");
        }
        if spec.response_timeout_s != 0 {
            warn!(
                "response_timeout_s in Redis spec is deprecated and ignored, use command_timeout_ms"
            );
        }
        if spec.connection_timeout_s != 0 {
            if spec.connection_timeout_ms != 0 {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "Both connection_timeout_s and connection_timeout_ms were set, can only have one!"
                ));
            }
            warn!("connection_timeout_s in Redis spec is deprecated, use connection_timeout_ms");
            spec.connection_timeout_ms = spec.connection_timeout_s * 1000;
        }
        if spec.connection_timeout_ms == 0 {
            spec.connection_timeout_ms = DEFAULT_CONNECTION_TIMEOUT_MS;
        }
        if spec.command_timeout_ms == 0 {
            spec.command_timeout_ms = DEFAULT_COMMAND_TIMEOUT_MS;
        }
        if spec.connection_pool_size == 0 {
            spec.connection_pool_size = DEFAULT_CONNECTION_POOL_SIZE;
        }
        if spec.read_chunk_size == 0 {
            spec.read_chunk_size = DEFAULT_READ_CHUNK_SIZE;
        }
        if spec.max_count_per_cursor == 0 {
            spec.max_count_per_cursor = DEFAULT_MAX_COUNT_PER_CURSOR;
        }
        if spec.max_chunk_uploads_per_update == 0 {
            spec.max_chunk_uploads_per_update = DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE;
        }
        if spec.scan_count == 0 {
            spec.scan_count = DEFAULT_SCAN_COUNT;
        }
        if spec.max_client_permits == 0 {
            spec.max_client_permits = DEFAULT_CLIENT_PERMITS;
        }
        if spec.retry.delay == 0.0 {
            spec.retry.delay = DEFAULT_RETRY_DELAY;
        }
        if spec.retry.max_retries == 0 {
            spec.retry.max_retries = 1;
        }

        // Cluster mode does not support keyspace notifications via
        // server-global `CONFIG SET notify-keyspace-events` (each shard
        // has its own config; the cluster client cannot fan-out PSUBSCRIBE
        // across shards). `new_cluster` already hard-codes
        // `enable_keyspace_notifications=false` and `keyspace_notifications_db=0`
        // when it constructs the inner store, so any operator-supplied
        // values here would be silently overwritten downstream — including
        // the URL/db cross-check and empty-key_prefix warn below.
        //
        // Force-disable here too, with a `warn!`, so:
        //  - the operator sees the override loudly at startup (red-team C
        //    + security LOW-1: today the override happens silently inside
        //    `new_cluster`, leaving operators with no signal that their
        //    flag was honored or not),
        //  - the cross-check and key_prefix warn don't fire spuriously
        //    against cluster URLs (security LOW-1: cluster startup
        //    failing on a confusing keyspace-notification error message
        //    when keyspace notifications are silently disabled anyway).
        if spec.mode == RedisMode::Cluster && spec.enable_keyspace_notifications {
            warn!(
                "RedisSpec: cluster mode does not support keyspace notifications via \
                 server-global CONFIG SET; forcing enable_keyspace_notifications=false. \
                 Operators relying on cluster-mode keyspace invalidation must wire it \
                 up out-of-band (per-shard listeners or an external invalidation \
                 channel)."
            );
            spec.enable_keyspace_notifications = false;
            spec.keyspace_notifications_db = 0;
        }

        if spec.enable_keyspace_notifications {
            // BLOCKER 5: a single `subscriber_channel` slot cannot serve both
            // `SchedulerSubscriptionManager` and the keyspace dispatcher.
            // Refuse the combination at construction time so the operator
            // sees the conflict at deploy, not as silent stale-positive
            // caching after a race lands the wrong consumer first.
            if spec.experimental_pub_sub_channel.is_some() {
                return Err(make_err!(
                    Code::FailedPrecondition,
                    "RedisSpec: enable_keyspace_notifications=true and \
                     experimental_pub_sub_channel are mutually exclusive — both consumers want \
                     the single Redis push-sender channel and only one can win. Split into two \
                     RedisSpec entries (one for the scheduler, one for the keyspace-callback \
                     RedisStore) or set enable_keyspace_notifications=false on the scheduler \
                     RedisStore."
                ));
            }

            // Hardening: validate the configured `keyspace_notifications_db`
            // against the database embedded in the connection URL. A typo
            // (`redis://server/3` vs `keyspace_notifications_db: 0`) would
            // otherwise leave the dispatcher subscribed to the wrong db with
            // no observable signal — silent stale-positive cache returns.
            //
            // Use `redis::IntoConnectionInfo` directly so the cross-check
            // mirrors the actual db resolution the connection performs. This
            // matters because the redis crate uses different rules per scheme:
            //  - TCP `redis://host[:port][/N]` → db from path-segment
            //    (`url.path().trim_matches('/')`).
            //  - Unix `redis+unix:///path?db=N` → db from query-pair
            //    (`query.get("db")`).
            //  - Sentinel `redis+sentinel://...` is reduced to `redis://...`
            //    in `RedisStore::connect` before parsing, so we apply the
            //    same substitution here.
            // A path-only check (e.g. `Url::path_segments`) silently passes
            // for the production URL form `redis+unix:///run/valkey/valkey.sock?db=N`,
            // making the cross-check a footgun against the deployment shape it
            // exists to defend. `IntoConnectionInfo` is the same path
            // `RedisStore::connect` calls, so the validator and the runtime
            // cannot disagree by construction.
            // We only check the first address; multi-address standard mode is
            // rejected separately by `new_standard`.
            let url_str = spec.addresses[0]
                .replace("redis+sentinel://", "redis://");
            if let Ok(connection_info) = url_str.into_connection_info() {
                // `RedisConnectionInfo::db()` returns `i64`; the redis crate
                // does not bound it to `0..=255`, but Redis itself
                // conventionally supports 0..15 (`databases 16` default), so
                // the explicit config field is `u8`. A negative or > 255
                // value is a config-level error the redis crate will catch
                // on connect; we treat any mismatch as a fail-fast signal.
                let url_db = connection_info.redis_settings().db();
                if url_db != i64::from(spec.keyspace_notifications_db) {
                    return Err(make_err!(
                        Code::FailedPrecondition,
                        "RedisSpec: connection URL resolves to db={url_db} but \
                         keyspace_notifications_db={configured}. Either match them or omit \
                         the db from the URL. Mismatch silently subscribes to the wrong db \
                         and disables stale-positive invalidation.",
                        configured = spec.keyspace_notifications_db
                    ));
                }
            }

            // Hardening: empty `key_prefix` means `strip_prefix("")` matches
            // every Redis key — the dispatcher would dispatch foreign-tenant
            // DELs into our callback chain. Acceptable when the Redis is
            // dedicated to NativeLink (single-tenant), but worth a startup
            // warn so the operator sees the assumption explicitly.
            if spec.key_prefix.is_empty() {
                warn!(
                    "RedisSpec: enable_keyspace_notifications=true with empty key_prefix; \
                     foreign-tenant DEL/EXPIRE/EVICT events will be dispatched into \
                     ItemCallback consumers. Set a non-empty key_prefix unless this Redis \
                     instance is dedicated to NativeLink."
                );
            }
        }

        trace!(?spec, "redis spec is after setting defaults");
        Ok(())
    }

    // Only used by tests, because we need to make a real redis connection, then fix this to get fixed values
    pub fn replace_temp_name_generator(&mut self, replacement: fn() -> String) {
        self.temp_name_generator_fn = replacement;
    }

    /// Test/observability accessor for the underlying manager. Not for
    /// production code paths — every store operation already routes
    /// through the manager via the `M: RedisManager<C>` bound, this
    /// accessor just exposes it for direct contract assertions in tests
    /// (e.g. cluster-mode-ignores-pool, single-uuid-per-cluster-manager).
    #[doc(hidden)]
    pub fn connection_manager(&self) -> &M {
        &self.connection_manager
    }
}

/// Maximum byte length of a keyspace-notification payload we are willing to
/// process. Production keys look like `<prefix><blake3-hex>-<u64>` ≈ 80 bytes;
/// anything past 1 KiB is either misconfiguration or a hostile/cross-tenant
/// key — drop it rather than allocate or hash it. Counted in
/// `keyspace_payload_too_long_dropped`.
const KEYSPACE_PAYLOAD_MAX_LEN: usize = 1024;

/// Maximum number of in-flight `ItemCallback::callback` futures the
/// dispatcher will run concurrently. A slow callback shouldn't head-of-line
/// block other keyevents, but unbounded concurrency on a Redis-eviction
/// storm would let the dispatcher OOM. 16 matches the rough order of
/// magnitude of registered callbacks (typically 1-2) with headroom.
const KEYSPACE_DISPATCH_CONCURRENCY: usize = 16;

/// Required `notify-keyspace-events` flags. `E` enables keyevent channels,
/// `g` enables generic commands (`DEL`), `e` enables eviction, `x` enables
/// expiration. We merge with operator-set flags so we never trample.
const REQUIRED_NOTIFY_FLAGS: &[char] = &['E', 'g', 'e', 'x'];

/// Reverse of [`RedisStore::encode_key`] for keyspace-notification payloads.
///
/// Strips `key_prefix`, then attempts to parse the remainder as
/// `<hex>-<size_bytes>` into a [`StoreKey::Digest`]. Returns `None` when the
/// remainder is not digest-shaped — those payloads are dropped before reaching
/// any registered `ItemCallback`.
///
/// **Why digest-only.** The only `ItemCallback` consumer in this codebase is
/// `ExistenceCacheStore`, whose `callback` calls `StoreKey::into_digest()` on
/// whatever it receives. For a `StoreKey::Str` arrival the digest derived via
/// blake3-of-bytes will never match anything actually inserted into the cache
/// (cache inserts go through `From<DigestInfo>`), so the `remove` is a
/// no-op. Worse, an admin tool issuing `DEL cas:debug-foo` (or any non-digest
/// scheduler/index key under the same prefix) would dispatch a foreign string
/// key into `info!`/`debug!` logs at our boundary — pure noise and a
/// cross-tenant info-leak risk on shared Redis. Drop them at the parser.
///
/// **Why variant matters.** Wrapping caches like `ExistenceCacheStore` insert
/// keys as `StoreKey::Digest` (because all CAS operations go through the
/// `From<DigestInfo>` conversion). `StoreKey`'s `Hash` implementation salts
/// by variant tag, so a `StoreKey::Str("abcd-0")` hashes to a different
/// bucket than `StoreKey::Digest(...)` of the same value. Constructing the
/// wrong variant means the callback fires against an empty bucket and the
/// cache is never invalidated.
///
/// **Security.** Callers control `key_prefix`, but the Redis instance may be
/// shared with foreign tenants whose keys also start with `key_prefix`
/// (prefix-collision). This dispatch is best-effort invalidation — never
/// trust the resulting `StoreKey` for authorization decisions. Length-cap on
/// the payload is enforced by the caller (see `KEYSPACE_PAYLOAD_MAX_LEN`).
fn parse_keyspace_payload(payload: &str, key_prefix: &str) -> Option<StoreKey<'static>> {
    let stripped = payload.strip_prefix(key_prefix)?;
    let (hash, size) = stripped.rsplit_once('-')?;
    let size_bytes = size.parse::<u64>().ok()?;
    let digest = DigestInfo::try_new(hash, size_bytes).ok()?;
    Some(StoreKey::Digest(digest))
}

impl<C, M> RedisStore<C, M>
where
    C: ConnectionLike + Clone + Send + Sync + Unpin + 'static,
    M: RedisManager<C> + Unpin + Send + Sync + 'static,
{
    /// Eagerly initializes the keyspace-notification dispatcher.
    ///
    /// **Must be called by `RedisStore::new_standard` immediately after
    /// `Arc::new(self)`** so the dispatcher task can hold a `Weak<Self>`
    /// for clean drop on store-drop. The result populates the
    /// `keyspace_dispatcher_tx` `OnceCell` so subsequent
    /// `register_item_callback` calls can dispatch synchronously.
    ///
    /// This is the BLOCKER 1 fix from the 681ae250 reviewers: the previous
    /// design ran CONFIG GET/SET + PSUBSCRIBE inside an async task spawned
    /// from a synchronous `register_item_callback`, swallowing all failures
    /// into a `warn!` that ECS could not observe. Eager init surfaces
    /// ACL/RENAME-COMMAND/PSUBSCRIBE failures as `Err` from
    /// `RedisStore::new_standard`, so the server fails to start instead of
    /// silently running with stale-positive caching.
    ///
    /// Order of operations matters (BLOCKER 2): every fallible step runs
    /// BEFORE the irrecoverable `subscriber_channel.take()`. Failure of
    /// CONFIG SET or PSUBSCRIBE leaves the channel still in the slot so a
    /// retry would not hit the misleading "already consumed by
    /// SchedulerSubscriptionManager" error. With eager init at construction
    /// the retry path is unreachable in practice (we never construct a half-
    /// initialized store), but the order is preserved for defence in depth.
    ///
    /// On reconnect, `set_keyspace_events_flags` arms the manager so that
    /// `CONFIG SET notify-keyspace-events <flags>` is re-issued before each
    /// PSUBSCRIBE replay (BLOCKER 4) — without this a Valkey restart silently
    /// disables the dispatcher.
    async fn init_keyspace_dispatcher_eager(self: &Arc<Self>) -> Result<(), Error> {
        if !self.enable_keyspace_notifications {
            // Nothing to do — `register_item_callback` will return Err and
            // ECS construction will panic loudly. This is the desired
            // BLOCKER 1 fail-fast shape for the disabled path.
            return Ok(());
        }

        let mut client = self.get_client().await?;
        // CONFIG GET on RESP3 returns a map; on RESP2 a flat 2-element
        // array. The redis crate's HashMap decoder accepts either.
        let current: HashMap<String, String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("notify-keyspace-events")
            .query_async(&mut client.connection_manager)
            .await
            .err_tip(|| {
                "CONFIG GET notify-keyspace-events failed; the configured Redis user likely \
                 lacks the CONFIG ACL. Set enable_keyspace_notifications=false or grant the \
                 ACL"
            })?;
        let existing = current
            .get("notify-keyspace-events")
            .cloned()
            .unwrap_or_default();
        let mut merged = existing.clone();
        for ch in REQUIRED_NOTIFY_FLAGS {
            if !merged.contains(*ch) {
                merged.push(*ch);
            }
        }
        if merged != existing {
            // First-time mutation of operator-managed config — surface in
            // audit log so unexpected changes are noticed.
            warn!(
                previous = %existing,
                new = %merged,
                "mutating server notify-keyspace-events to enable ItemCallback dispatch"
            );
            let _: () = redis::cmd("CONFIG")
                .arg("SET")
                .arg("notify-keyspace-events")
                .arg(&merged)
                .query_async(&mut client.connection_manager)
                .await
                .err_tip(|| {
                    "CONFIG SET notify-keyspace-events failed; ACL or RENAME-COMMAND likely. \
                     Set enable_keyspace_notifications=false or grant the ACL"
                })?;
        }
        drop(client);

        // Arm the manager BEFORE the first PSUBSCRIBE so a reconnect that
        // fires concurrently with this init still has the flags to apply.
        // No-op on cluster mode (notifications force-disabled there).
        self.connection_manager
            .set_keyspace_events_flags(merged.clone());

        let db = self.keyspace_notifications_db;
        for event in ["del", "expired", "evicted"] {
            let pattern = format!("__keyevent@{db}__:{event}");
            self.connection_manager
                .psubscribe(&pattern)
                .await
                .err_tip(|| format!("psubscribe {pattern} failed"))?;
        }

        // BLOCKER 2 fix: take the subscriber channel ONLY after every fallible
        // step has succeeded. If anything above failed we returned `Err` and
        // the channel is still in `Mutex<Option<>>`. With eager init at
        // construction this matters mainly as defence-in-depth — `set_spec_defaults`
        // rejects the (keyspace + scheduler) sharing combo upstream.
        let subscriber_channel = self.subscriber_channel.lock().take().ok_or_else(|| {
            make_err!(
                Code::FailedPrecondition,
                "RedisStore subscriber_channel already consumed by SchedulerSubscriptionManager; \
                 keyspace dispatch and scheduler subscription cannot share a single RedisStore \
                 instance — use separate stores. (`set_spec_defaults` rejects this \
                 configuration at construction; reaching this branch means a caller \
                 bypassed `new_standard`.)"
            )
        })?;

        let (callback_tx, callback_rx) = unbounded_channel::<Arc<dyn ItemCallback>>();
        let weak_self = Arc::downgrade(self);
        let key_prefix = self.key_prefix.clone();
        // Spawn freestanding task. We use `background_spawn!` (NOT `spawn!`)
        // because the latter returns a `JoinHandleDropGuard` that aborts the
        // task on drop. This task is supposed to outlive its caller — it
        // owns the push channel, the receiver, and the callback list, and
        // exits naturally when `callback_tx` drops (when `RedisStore` drops).
        background_spawn!(
            "redis_keyspace_dispatcher",
            Self::run_keyspace_dispatcher(
                weak_self,
                key_prefix,
                subscriber_channel,
                callback_rx,
            )
        );

        // Populate the OnceCell. `set` returns Err if it was already
        // initialized — impossible here (we run exactly once at construction
        // and have an exclusive Arc handle), but treat it as Internal if
        // someone re-wires the call.
        self.keyspace_dispatcher_tx
            .set(callback_tx)
            .map_err(|_| {
                make_err!(
                    Code::Internal,
                    "init_keyspace_dispatcher_eager called twice; OnceCell already initialized"
                )
            })?;

        // Inert-dispatcher 60s startup warn (red-team #3 from #100 cadre review).
        // The dispatcher is now running and will fan-out PMessage events into
        // a `Vec<Arc<dyn ItemCallback>>` that starts empty and grows only when
        // wrapping stores call `register_item_callback` at THEIR construction.
        // Production AC chain (FastSlowStore { fast: MemoryStore, slow:
        // REDIS_AC_STORE }) currently has no `ExistenceCacheStore` caller, so
        // this dispatcher would fire into the void with no operator signal.
        // Wait one minute, then if the registration counter is still 0 emit a
        // single `warn!` so the operator sees what is happening. Single-shot.
        // Gated on `enable_keyspace_notifications=true` because the
        // disabled-on-purpose path returned early at the top of this function
        // and never started the dispatcher in the first place — but we keep
        // the inner check as defence-in-depth in case the early-return is
        // refactored.
        if self.enable_keyspace_notifications {
            let weak_for_warn = Arc::downgrade(self);
            background_spawn!("redis_keyspace_inert_dispatcher_warn", async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let Some(store) = weak_for_warn.upgrade() else {
                    // Store dropped within 60s of construction — nothing to
                    // warn about; the dispatcher is on its way down anyway.
                    return;
                };
                if store.callbacks_registered_total.load(Ordering::Relaxed) == 0 {
                    warn!(
                        target: "nativelink::redis_keyspace",
                        "RedisStore keyspace dispatcher running but no ItemCallbacks registered \
                         after 60s — keyspace notifications will be silently dropped. This is \
                         correct for non-ECS-wrapped uses (e.g. AC-only stores not yet wrapped \
                         in ExistenceCacheStore); verify intent. Set \
                         enable_keyspace_notifications=false to disable, or wrap this RedisStore \
                         in ExistenceCacheStore to register a callback."
                    );
                }
            });
        }
        Ok(())
    }

    async fn run_keyspace_dispatcher(
        weak_self: Weak<Self>,
        key_prefix: String,
        subscriber_channel: UnboundedReceiver<PushInfo>,
        mut callback_rx: UnboundedReceiver<Arc<dyn ItemCallback>>,
    ) {
        let mut subscriber_stream = UnboundedReceiverStream::new(subscriber_channel);
        let mut callbacks: Vec<Arc<dyn ItemCallback>> = Vec::new();
        let mut inflight: JoinSet<()> = JoinSet::new();

        loop {
            select! {
                maybe_cb = callback_rx.recv() => {
                    match maybe_cb {
                        Some(cb) => callbacks.push(cb),
                        None => {
                            debug!("RedisStore dropped; keyspace dispatcher exiting");
                            return;
                        }
                    }
                }
                maybe_push = subscriber_stream.next() => {
                    let Some(push_info) = maybe_push else {
                        debug!("redis push channel closed; keyspace dispatcher exiting");
                        return;
                    };
                    if push_info.kind != redis::PushKind::PMessage {
                        trace!(?push_info.kind, "redis push, not PMessage");
                        continue;
                    }
                    if push_info.data.len() < 3 {
                        trace!(?push_info, "redis PMessage missing fields");
                        continue;
                    }
                    let payload_bytes = match push_info.data.last().expect("len>=3") {
                        Value::SimpleString(s) => s.as_bytes(),
                        Value::BulkString(b) => b.as_slice(),
                        _ => continue,
                    };
                    if payload_bytes.len() > KEYSPACE_PAYLOAD_MAX_LEN {
                        if let Some(store) = weak_self.upgrade() {
                            store
                                .keyspace_payload_too_long_dropped
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        warn!(
                            len = payload_bytes.len(),
                            cap = KEYSPACE_PAYLOAD_MAX_LEN,
                            "redis keyevent payload too long; dropped"
                        );
                        continue;
                    }
                    if payload_bytes.iter().any(|b| *b < 0x20 || *b == 0x7f) {
                        // Reject control chars and NUL — these never appear
                        // in legitimate StoreKeys we generate.
                        trace!("redis keyevent payload has control chars; dropped");
                        continue;
                    }
                    let Ok(payload) = str::from_utf8(payload_bytes) else {
                        trace!("redis keyevent payload not utf8; dropped");
                        continue;
                    };
                    let Some(store_key) = parse_keyspace_payload(payload, &key_prefix) else {
                        // Foreign tenant or different prefix.
                        continue;
                    };

                    let Some(store) = weak_self.upgrade() else {
                        return;
                    };
                    store.keyspace_events_dispatched.fetch_add(1, Ordering::Relaxed);
                    drop(store);

                    debug!(key = %store_key, "redis keyevent for tracked key");

                    // Backpressure: if the per-callback work is piling up,
                    // wait for one to finish before queuing the next batch.
                    while inflight.len() >= KEYSPACE_DISPATCH_CONCURRENCY {
                        match inflight.join_next().await {
                            None => break,
                            Some(Err(join_err)) if join_err.is_panic() => {
                                error!(
                                    ?join_err,
                                    "redis keyspace ItemCallback panicked; continuing dispatch"
                                );
                            }
                            _ => {}
                        }
                    }
                    for cb in &callbacks {
                        let cb = Arc::clone(cb);
                        let key_for_cb = store_key.borrow().into_owned();
                        inflight.spawn(async move {
                            // (#locality-map-drift) Redis keyspace-notification
                            // callbacks carry no logical LWW ts → (0, 0).
                            cb.callback(key_for_cb, 0, 0).await;
                        });
                    }
                }
                Some(finished) = inflight.join_next(), if !inflight.is_empty() => {
                    if let Err(join_err) = finished
                        && join_err.is_panic()
                    {
                        error!(
                            ?join_err,
                            "redis keyspace ItemCallback panicked; continuing dispatch"
                        );
                    }
                }
            }
        }
    }
}

impl RedisStore<ConnectionManager, StandardRedisManager<ConnectionManager>> {
    /// Test/observability helper: returns the size of the underlying
    /// connection pool. The store exposes this so callers can verify
    /// `set_spec_defaults` produced the right pool size end-to-end (the
    /// only path that fills `connection_pool_size` when the spec value
    /// was 0). Hot-path callers should NOT use this in steady state — the
    /// pool size is fixed at construction.
    pub fn connection_manager_pool_size(&self) -> usize {
        self.connection_manager.pool_size()
    }

    /// Test/observability helper: returns the subscription patterns
    /// currently tracked by the underlying manager. Used to verify
    /// connect-time `psubscribe` ran for stores constructed with
    /// `experimental_pub_sub_channel`.
    pub fn connection_manager_subscriptions(&self) -> Vec<String> {
        self.connection_manager.debug_subscriptions()
    }

    /// Test-only entry point exposing the synchronous `set_spec_defaults`
    /// validator without requiring a live Redis. Used by integration
    /// tests that want to assert the spec-mutation/validation contract
    /// in isolation from the connect path (e.g., cluster-mode forced
    /// disable of `enable_keyspace_notifications`, BLOCKER 5 collision
    /// rejection, BLOCK-1 URL/db cross-check). Not for production code
    /// paths.
    #[doc(hidden)]
    pub fn set_spec_defaults_for_test(spec: &mut RedisSpec) -> Result<(), Error> {
        Self::set_spec_defaults(spec)
    }
}

impl RedisStore<ClusterConnection, ClusterRedisManager<ClusterConnection>> {
    pub async fn new_cluster(mut spec: RedisSpec) -> Result<Arc<Self>, Error> {
        if spec.mode != RedisMode::Cluster {
            return Err(Error::new(
                Code::InvalidArgument,
                "new_cluster only works for Cluster mode".to_string(),
            ));
        }
        Self::set_spec_defaults(&mut spec)?;

        let parsed_addrs: Vec<_> = spec
            .addresses
            .iter_mut()
            .map(|addr| {
                addr.clone().into_connection_info().map(|connection_info| {
                    let redis_settings = connection_info
                        .redis_settings()
                        .clone()
                        // We need RESP3 here because the cluster mode doesn't support RESP2 pubsub
                        // See also https://docs.rs/redis/latest/redis/cluster_async/index.html#pubsub
                        .set_protocol(redis::ProtocolVersion::RESP3);
                    connection_info.set_redis_settings(redis_settings)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let connection_timeout = Duration::from_millis(spec.connection_timeout_ms);
        let command_timeout = Duration::from_millis(spec.command_timeout_ms);
        let (tx, subscriber_channel) = unbounded_channel();

        let builder = ClusterClient::builder(parsed_addrs)
            .connection_timeout(connection_timeout)
            .response_timeout(command_timeout)
            .push_sender(tx)
            .retries(u32::try_from(spec.retry.max_retries)?);

        let client = builder.build()?;

        Self::new_from_builder_and_parts(
            spec.experimental_pub_sub_channel,
            || Uuid::new_v4().to_string(),
            spec.key_prefix.clone(),
            spec.read_chunk_size,
            spec.max_chunk_uploads_per_update,
            spec.scan_count,
            spec.max_client_permits,
            spec.max_count_per_cursor,
            command_timeout * 2,
            subscriber_channel,
            ClusterRedisManager::new(client.get_async_connection().await?).await?,
            // Keyspace notification semantics in cluster mode are documented
            // as undefined; force-disable so we never CONFIG SET against a
            // cluster node. Operators relying on cluster-mode keyspace
            // notifications must wire that up out-of-band.
            false,
            0,
        )
        .await
        .map(Arc::new)
    }
}

impl RedisStore<ConnectionManager, StandardRedisManager<ConnectionManager>> {
    async fn connect(
        spec: RedisSpec,
        tx: UnboundedSender<PushInfo>,
    ) -> Result<ConnectionManager, Error> {
        let connection_timeout = Duration::from_millis(spec.connection_timeout_ms);
        let command_timeout = Duration::from_millis(spec.command_timeout_ms);

        let addr = &spec.addresses[0];
        let local_addr = addr.clone();
        let mut parsed_addr = local_addr
            .replace("redis+sentinel://", "redis://")
            .into_connection_info()?;

        let redis_settings = parsed_addr
            .redis_settings()
            .clone()
            // We need RESP3 here because we want to do set_push_sender
            .set_protocol(redis::ProtocolVersion::RESP3);
        parsed_addr = parsed_addr.set_redis_settings(redis_settings);
        debug!(?parsed_addr, "Parsed redis addr");

        let client = timeout(
            connection_timeout,
            spawn!("connect", async move {
                match spec.mode {
                    RedisMode::Standard => Client::open(parsed_addr).map_err(Into::<Error>::into),
                    RedisMode::Cluster => {
                        return Err(Error::new(
                            Code::Internal,
                            "Use RedisStore::new_cluster for cluster connections".to_owned(),
                        ));
                    }
                    RedisMode::Sentinel => async {
                        let url_parsing = Url::parse(&local_addr)?;
                        let master_name = url_parsing
                            .query_pairs()
                            .find(|(key, _)| key == "sentinelServiceName")
                            .map_or_else(|| "master".into(), |(_, value)| value.to_string());

                        let redis_connection_info = parsed_addr.redis_settings().clone();
                        let sentinel_connection_info = SentinelNodeConnectionInfo::default()
                            .set_redis_connection_info(redis_connection_info);

                        // We fish this out because sentinels don't support db, we need to set it
                        // on the client only. See also https://github.com/redis-rs/redis-rs/issues/1950
                        let original_db = parsed_addr.redis_settings().db();
                        if original_db != 0 {
                            // sentinel_connection_info has the actual DB set
                            let revised_settings = parsed_addr.redis_settings().clone().set_db(0);
                            parsed_addr = parsed_addr.set_redis_settings(revised_settings);
                        }

                        SentinelClient::build(
                            vec![parsed_addr],
                            master_name,
                            Some(sentinel_connection_info),
                            SentinelServerType::Master,
                        )
                        .map_err(Into::<Error>::into)
                    }
                    .and_then(|mut s| async move { Ok(s.async_get_client().await) })
                    .await?
                    .map_err(Into::<Error>::into),
                }
                .err_tip_with_code(|_e| {
                    (
                        Code::InvalidArgument,
                        format!("While connecting to redis with url: {local_addr}"),
                    )
                })
            }),
        )
        .await
        .err_tip(|| format!("Timeout while connecting to redis with url: {addr}"))???;

        let connection_manager_config = {
            ConnectionManagerConfig::new()
                .set_number_of_retries(spec.retry.max_retries)
                .set_connection_timeout(Some(connection_timeout))
                .set_response_timeout(Some(command_timeout))
                .set_push_sender(tx)
        };

        let connection_manager =
            ConnectionManager::new_with_config(client, connection_manager_config)
                .await
                .err_tip(|| format!("While connecting to redis with url: {addr}"))?;

        // NOTE: psubscribe is intentionally NOT applied here. The manager owns
        // a pool of N connections, and pubsub is pinned to a single subscriber
        // slot (see `SUBSCRIBER_SLOT`). Subscribing here would attach pubsub
        // to every newly-connected slot and cause duplicate message delivery.
        Ok(connection_manager)
    }

    /// Create a new `RedisStore` from the given configuration.
    pub async fn new_standard(mut spec: RedisSpec) -> Result<Arc<Self>, Error> {
        Self::set_spec_defaults(&mut spec)?;

        if spec.addresses.len() != 1 {
            return Err(make_err!(
                Code::Unimplemented,
                "Connecting directly to multiple redis nodes in a cluster is currently unsupported. Please specify a single URL to a single node, and nativelink will use cluster discover to find the other nodes."
            ));
        }

        let (tx, subscriber_channel) = unbounded_channel();
        let command_timeout = Duration::from_millis(spec.command_timeout_ms);
        let pool_size = spec.connection_pool_size;
        // Clone non-Copy spec fields BEFORE the `spec` value moves into the
        // connect_func closure below.
        let pub_sub_channel = spec.experimental_pub_sub_channel.clone();
        let key_prefix = spec.key_prefix.clone();
        let read_chunk_size = spec.read_chunk_size;
        let max_chunk_uploads_per_update = spec.max_chunk_uploads_per_update;
        let scan_count = spec.scan_count;
        let max_client_permits = spec.max_client_permits;
        let max_count_per_cursor = spec.max_count_per_cursor;
        let enable_keyspace_notifications = spec.enable_keyspace_notifications;
        let keyspace_notifications_db = spec.keyspace_notifications_db;

        let manager = StandardRedisManager::new_with_pool_size(
            Box::new(move || Box::pin(Self::connect(spec.clone(), tx.clone()))),
            pool_size,
        )
        .await?;

        // Restore pre-patch contract: if a pub-sub channel was configured,
        // subscribe at construction time so callers that never invoke
        // `subscription_manager()` still receive messages. Pinning to
        // SUBSCRIBER_SLOT (slot 0) is enforced inside `psubscribe` — the
        // connect-time call no longer goes through every newly-dialed slot
        // (which would N-fold-deliver). See red-team P3 and
        // `pub_sub_channel_subscribed_at_construction_*` test for the
        // regression this guards against.
        if let Some(channel) = pub_sub_channel.as_deref() {
            manager
                .psubscribe(channel)
                .await
                .err_tip(|| format!("connect-time psubscribe for pub_sub_channel {channel}"))?;
        }

        let store = Arc::new(
            Self::new_from_builder_and_parts(
                pub_sub_channel,
                || Uuid::new_v4().to_string(),
                key_prefix,
                read_chunk_size,
                max_chunk_uploads_per_update,
                scan_count,
                max_client_permits,
                max_count_per_cursor,
                command_timeout * 2,
                subscriber_channel,
                manager,
                enable_keyspace_notifications,
                keyspace_notifications_db,
            )
            .await?,
        );
        // BLOCKER 1 fix: eager keyspace-dispatcher init at construction.
        // Failure of CONFIG GET / CONFIG SET / PSUBSCRIBE here propagates as
        // `Err` from `new_standard`, so the server fails to start with a
        // clear error rather than silently running with stale-positive
        // caching. No-op when `enable_keyspace_notifications=false`.
        store.init_keyspace_dispatcher_eager().await?;
        Ok(store)
    }
}

impl<C, M> RedisStore<C, M>
where
    C: ConnectionLike + Clone + Send + Sync + Unpin + 'static,
    M: RedisManager<C> + Unpin + Send + Sync + 'static,
{
    /// Fallback for `has_with_results` when pipelined batch fails (e.g. CrossSlot
    /// in cluster mode). Sends per-key STRLEN+EXISTS pipelines concurrently.
    async fn has_with_results_per_key(
        &self,
        pipeline_indices: &[usize],
        encoded_keys: &[String],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        pipeline_indices
            .iter()
            .zip(encoded_keys.iter())
            .map(|(&result_idx, encoded_key)| async move {
                let mut client = self.get_client().await?;

                let cmd_start = Instant::now();
                let pipeline_result = instrument_redis_call(
                    "STRLEN+EXISTS",
                    encoded_key,
                    timeout(
                        self.command_timeout,
                        pipe()
                            .strlen(encoded_key.as_str())
                            .exists(encoded_key.as_str())
                            .query_async::<(u64, bool)>(&mut client.connection_manager),
                    ),
                )
                .await;
                let (blob_len, exists) = match pipeline_result {
                    Err(_) => {
                        let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                        error!(cmd = "STRLEN+EXISTS", key = %encoded_key, elapsed_ms, "redis command timed out");
                        return Err(make_err!(
                            Code::Unavailable,
                            "Redis STRLEN+EXISTS timed out after {elapsed_ms}ms for key {encoded_key}"
                        ));
                    }
                    Ok(Err(ref err)) if err.is_timeout() => {
                        // Inner response_timeout fired — desynced slot, reconnect
                        // before returning so the next caller gets a clean pipeline.
                        // (FU-10: same desync mechanism as has_with_results.)
                        let mut nl_err: Error = err.clone().into();
                        nl_err.messages.push(
                            "In RedisStore::has_with_results_per_key".to_string(),
                        );
                        client.reconnect_on_timeout(&self.connection_manager, "has_with_results_per_key").await;
                        return Err(nl_err);
                    }
                    Ok(result) => result
                        .err_tip(|| "In RedisStore::has_with_results_per_key")?,
                };
                let elapsed = cmd_start.elapsed();
                if elapsed.as_secs() >= 5 {
                    error!(cmd = "STRLEN+EXISTS", key = %encoded_key, elapsed_ms = elapsed.as_millis() as u64, "redis command slow (>5s)");
                } else if elapsed.as_secs() >= 1 {
                    warn!(cmd = "STRLEN+EXISTS", key = %encoded_key, elapsed_ms = elapsed.as_millis() as u64, "redis command slow (>1s)");
                }

                let value = if exists { Some(blob_len) } else { None };
                Ok::<_, Error>((result_idx, value))
            })
            .collect::<FuturesUnordered<_>>()
            .try_for_each(|(result_idx, value)| {
                results[result_idx] = value;
                future::ready(Ok(()))
            })
            .await
    }
}

#[async_trait]
impl<C, M> StoreDriver for RedisStore<C, M>
where
    C: ConnectionLike + Clone + Send + Sync + Unpin + 'static,
    M: RedisManager<C> + Unpin + Send + Sync + 'static,
{
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }

        // Handle zero digests and collect non-zero keys that need Redis lookup.
        // Track which indices in the results array correspond to pipeline commands.
        let mut pipeline_indices = Vec::with_capacity(keys.len());
        let mut encoded_keys = Vec::with_capacity(keys.len());

        for (i, key) in keys.iter().enumerate() {
            if is_zero_digest(key.borrow()) {
                results[i] = Some(0);
            } else {
                let encoded = self.encode_key(key);
                encoded_keys.push(encoded.into_owned());
                pipeline_indices.push(i);
            }
        }

        if pipeline_indices.is_empty() {
            return Ok(());
        }

        // Process keys in chunks to avoid unbounded Redis response buffering.
        // Each chunk builds a pipeline with STRLEN+EXISTS for each key and
        // sends all commands in one round-trip.
        for chunk_start in (0..encoded_keys.len()).step_by(MAX_PIPELINE_BATCH) {
            let chunk_end = cmp::min(chunk_start + MAX_PIPELINE_BATCH, encoded_keys.len());
            let chunk_keys = &encoded_keys[chunk_start..chunk_end];
            let chunk_indices = &pipeline_indices[chunk_start..chunk_end];

            let mut pipeline = pipe();
            for encoded_key in chunk_keys {
                // Redis returns 0 when the key doesn't exist AND when the key
                // exists with value of length 0. We need both STRLEN and EXISTS
                // to distinguish the two cases.
                pipeline.strlen(encoded_key.as_str());
                pipeline.exists(encoded_key.as_str());
            }

            let mut client = self.get_client().await?;

            let cmd_start = Instant::now();
            // Synthesize a cardinality-bound logging key for the
            // sampled instrumentation: the per-key encoded string
            // would be too noisy at batch sizes of 5000.
            let pipeline_key_tag = format!("pipelined x{}", chunk_keys.len());
            let pipeline_result = instrument_redis_call(
                "pipelined STRLEN+EXISTS",
                &pipeline_key_tag,
                timeout(
                    self.command_timeout,
                    pipeline.query_async::<Vec<Value>>(&mut client.connection_manager),
                ),
            )
            .await;

            let raw_values = match pipeline_result {
                Err(_) => {
                    let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                    error!(
                        cmd = "pipelined STRLEN+EXISTS",
                        key_count = chunk_keys.len(),
                        elapsed_ms,
                        "redis pipeline timed out"
                    );
                    return Err(make_err!(
                        Code::Unavailable,
                        "Redis pipelined STRLEN+EXISTS timed out after {elapsed_ms}ms for {n} keys",
                        n = chunk_keys.len()
                    ));
                }
                Ok(Err(ref err))
                    if err.kind()
                        == redis::ErrorKind::Server(redis::ServerErrorKind::CrossSlot) =>
                {
                    // In cluster mode, keys may hash to different slots. Fall back
                    // to per-key pipelines sent concurrently.
                    info!(
                        key_count = encoded_keys.len(),
                        "CrossSlot error in has_with_results, falling back to per-key pipelines"
                    );
                    drop(client);
                    return self
                        .has_with_results_per_key(
                            &pipeline_indices,
                            &encoded_keys,
                            results,
                        )
                        .await;
                }
                Ok(Err(ref err)) if err.is_timeout() => {
                    // The ConnectionManager's inner response_timeout fired.
                    // The multiplexed TCP connection now has an orphaned
                    // in-flight response in the pipeline buffer — the next
                    // command on this slot would read the wrong frame and
                    // cause "Data length mismatch" or parse errors (FU-10,
                    // 2026-06-15 incident: 154 mismatch errors from desynced
                    // SETRANGE responses on the AC Valkey connection).
                    // Replace the slot with a fresh connection before returning
                    // the error so the caller can retry without desync risk.
                    // Best-effort: if reconnect itself fails, the error is
                    // silently dropped and the original timeout error returned.
                    let mut nl_err: Error = err.clone().into();
                    nl_err.messages.push(
                        "In RedisStore::has_with_results pipelined query (timeout reconnect)"
                            .to_string(),
                    );
                    client.reconnect_on_timeout(&self.connection_manager, "has_with_results").await;
                    return Err(nl_err);
                }
                Ok(result) => result
                    .err_tip(|| "In RedisStore::has_with_results pipelined query")?,
            };

            let elapsed = cmd_start.elapsed();
            if elapsed.as_secs() >= 5 {
                error!(
                    cmd = "pipelined STRLEN+EXISTS",
                    key_count = chunk_keys.len(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "redis pipeline slow (>5s)"
                );
            } else if elapsed.as_secs() >= 1 {
                warn!(
                    cmd = "pipelined STRLEN+EXISTS",
                    key_count = chunk_keys.len(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "redis pipeline slow (>1s)"
                );
            }

            // Each key contributes 2 values: [strlen_result, exists_result].
            let expected_len = chunk_keys.len() * 2;
            if raw_values.len() != expected_len {
                return Err(make_err!(
                    Code::Internal,
                    "Redis pipeline returned {actual} values, expected {expected} (2 per key for {n} keys)",
                    actual = raw_values.len(),
                    expected = expected_len,
                    n = chunk_keys.len()
                ));
            }

            for (pair_idx, &result_idx) in chunk_indices.iter().enumerate() {
                let strlen_val = &raw_values[pair_idx * 2];
                let exists_val = &raw_values[pair_idx * 2 + 1];

                let blob_len: u64 = redis::from_redis_value_ref(strlen_val)
                    .map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to parse STRLEN result for key {}: {:?}",
                            chunk_keys[pair_idx],
                            e
                        )
                    })?;
                let exists: bool = redis::from_redis_value_ref(exists_val)
                    .map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to parse EXISTS result for key {}: {:?}",
                            chunk_keys[pair_idx],
                            e
                        )
                    })?;

                results[result_idx] = if exists { Some(blob_len) } else { None };
            }
        }

        Ok(())
    }

    async fn list(
        self: Pin<&Self>,
        range: (Bound<StoreKey<'_>>, Bound<StoreKey<'_>>),
        handler: &mut (dyn for<'a> FnMut(&'a StoreKey) -> bool + Send + Sync + '_),
    ) -> Result<u64, Error> {
        let range = (
            range.0.map(StoreKey::into_owned),
            range.1.map(StoreKey::into_owned),
        );
        let pattern = match range.0 {
            Bound::Included(ref start) | Bound::Excluded(ref start) => match range.1 {
                Bound::Included(ref end) | Bound::Excluded(ref end) => {
                    let start = start.as_str();
                    let end = end.as_str();
                    let max_length = start.len().min(end.len());
                    let length = start
                        .chars()
                        .zip(end.chars())
                        .position(|(a, b)| a != b)
                        .unwrap_or(max_length);
                    format!("{}{}*", self.key_prefix, &start[..length])
                }
                Bound::Unbounded => format!("{}*", self.key_prefix),
            },
            Bound::Unbounded => format!("{}*", self.key_prefix),
        };
        let mut client = self.get_client().await?;
        trace!(%pattern, count=self.scan_count, "Running SCAN");
        let opts = ScanOptions::default()
            .with_pattern(pattern)
            .with_count(self.scan_count);
        let mut scan_stream: AsyncIter<Value> = client
            .connection_manager
            .scan_options(opts)
            .await
            .err_tip(|| "During scan_options")?;
        let mut iterations = 0;
        let mut errors = vec![];
        while let Some(key) = scan_stream.next_item().await {
            if let Ok(Value::BulkString(raw_key)) = key {
                let Ok(str_key) = str::from_utf8(&raw_key) else {
                    error!(?raw_key, "Non-utf8 key");
                    errors.push(format!("Non-utf8 key {raw_key:?}"));
                    continue;
                };
                if let Some(key) = str_key.strip_prefix(&self.key_prefix) {
                    let key = StoreKey::new_str(key);
                    if range.contains(&key) {
                        iterations += 1;
                        if !handler(&key) {
                            error!("Issue in handler");
                            errors.push("Issue in handler".to_string());
                        }
                    } else {
                        trace!(%key, ?range, "Key not in range");
                    }
                } else {
                    errors.push("Key doesn't match prefix".to_string());
                }
            } else {
                error!(?key, "Non-string in key");
                errors.push("Non-string in key".to_string());
            }
        }
        if errors.is_empty() {
            Ok(iterations)
        } else {
            error!(?errors, "Errors in scan stream");
            Err(Error::new(Code::Internal, format!("Errors: {errors:?}")))
        }
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let final_key = self.encode_key(&key);

        // While the name generation function can be supplied by the user, we need to have the curly
        // braces in place in order to manage redis' hashing behavior and make sure that the temporary
        // key name and the final key name are directed to the same cluster node. See
        // https://redis.io/blog/redis-clustering-best-practices-with-keys/
        //
        // The TL;DR is that if we're in cluster mode and the names hash differently, we can't use request
        // pipelining. By using these braces, we tell redis to only hash the part of the temporary key that's
        // identical to the final key -- so they will always hash to the same node.
        let temp_key = format!(
            "temp-{}-{{{}}}",
            (self.temp_name_generator_fn)(),
            &final_key
        );

        if is_zero_digest(key.borrow()) {
            let chunk = reader
                .peek()
                .await
                .err_tip(|| "Failed to peek in RedisStore::update")?;
            if chunk.is_empty() {
                reader
                    .drain()
                    .await
                    .err_tip(|| "Failed to drain in RedisStore::update")?;
                // Zero-digest keys are special -- we don't need to do anything with it.
                return Ok(());
            }
        }

        let mut client = self.get_client().await?;

        let mut read_stream = reader
            .scan(0u32, |bytes_read, chunk_res| {
                future::ready(Some(
                    chunk_res
                        .err_tip(|| "Failed to read chunk in update in redis store")
                        .and_then(|chunk| {
                            let offset = isize::try_from(*bytes_read).err_tip(|| "Could not convert offset to isize in RedisStore::update")?;
                            let chunk_len = u32::try_from(chunk.len()).err_tip(
                                || "Could not convert chunk length to u32 in RedisStore::update",
                            )?;
                            let new_bytes_read = bytes_read
                                .checked_add(chunk_len)
                                .err_tip(|| "Overflow protection in RedisStore::update")?;
                            *bytes_read = new_bytes_read;
                            Ok::<_, Error>((offset, *bytes_read, chunk))
                        }),
                ))
            })
            .map(|res| {
                let (offset, end_pos, chunk) = res?;
                let temp_key_ref = &temp_key;
                let cmd_timeout = self.command_timeout;
                Ok(async move {
                    let (mut connection_manager, connect_id) = self.connection_manager.get_connection().await?;
                    let chunk_len = chunk.len();
                    let cmd_start = Instant::now();
                    let setrange_result = timeout(
                        cmd_timeout,
                        connection_manager.setrange::<_, _, usize>(temp_key_ref, offset, chunk.to_vec()),
                    )
                    .await
                    .map_err(|_| {
                        let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                        error!(cmd = "SETRANGE", key = %temp_key_ref, elapsed_ms, "redis command timed out");
                        make_err!(
                            Code::Unavailable,
                            "Redis SETRANGE timed out after {elapsed_ms}ms for key {temp_key_ref}, offset = {offset}, end_pos = {end_pos}"
                        )
                    })?;
                    match setrange_result {
                        Ok(_) => {},
                        Err(err)
                            if err.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly) =>
                        {
                            let (mut connection_manager, _connect_id) = self.connection_manager.reconnect(connect_id).await?;
                            timeout(
                                cmd_timeout,
                                connection_manager.setrange::<_, _, usize>(temp_key_ref, offset, chunk.to_vec()),
                            )
                            .await
                            .map_err(|_| {
                                let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                                error!(cmd = "SETRANGE", key = %temp_key_ref, elapsed_ms, "redis command timed out after reconnect");
                                make_err!(
                                    Code::Unavailable,
                                    "Redis SETRANGE timed out after {elapsed_ms}ms (after reconnect) for key {temp_key_ref}, offset = {offset}, end_pos = {end_pos}"
                                )
                            })?
                            .err_tip(
                                || format!("(after reconnect) while appending to temp key ({temp_key_ref}) in RedisStore::update. offset = {offset}. end_pos = {end_pos}"),
                            )?;
                        }
                        Err(err) if err.is_timeout() => {
                            // Inner response_timeout fired — desynced slot,
                            // reconnect before returning. (FU-10: 2026-06-15
                            // incident: desynced SETRANGE responses caused 154
                            // "Data length mismatch" errors in RedisStore::update.)
                            reconnect_on_timeout_by_uuid(
                                &self.connection_manager,
                                connect_id,
                                "update::setrange",
                            ).await;
                            let mut error: Error = err.into();
                            error
                                .messages
                                .push(format!("While appending to temp key ({temp_key_ref}) in RedisStore::update. offset = {offset}. end_pos = {end_pos}"));
                            return Err(error);
                        }
                        Err(err) => {
                            let mut error: Error = err.into();
                            error
                                .messages
                                .push(format!("While appending to temp key ({temp_key_ref}) in RedisStore::update. offset = {offset}. end_pos = {end_pos}"));
                            return Err(error);
                        }
                    }
                    let elapsed = cmd_start.elapsed();
                    if elapsed.as_secs() >= 5 {
                        error!(cmd = "SETRANGE", key = %temp_key_ref, elapsed_ms = elapsed.as_millis() as u64, size_bytes = chunk_len, "redis command slow (>5s)");
                    } else if elapsed.as_secs() >= 1 {
                        warn!(cmd = "SETRANGE", key = %temp_key_ref, elapsed_ms = elapsed.as_millis() as u64, size_bytes = chunk_len, "redis command slow (>1s)");
                    }
                    Ok::<u32, Error>(end_pos)
                })
            })
            .try_buffer_unordered(self.max_chunk_uploads_per_update);

        let mut total_len: u32 = 0;
        while let Some(last_pos) = read_stream.try_next().await? {
            if last_pos > total_len {
                total_len = last_pos;
            }
        }

        // Enforce `UploadSizeInfo` BEFORE the RENAME — a truncated upstream
        // (e.g. a tonic deadline dropping the inbound buf_channel after
        // some chunks have flushed via SETRANGE) would otherwise commit a
        // partial entry to the final key and poison every future `get_part`
        // for this digest, persistently, in Valkey. Mirrors the MemoryStore
        // Bucket-B fix in commit `0ff03300`. `MaxSize` is advisory: overruns
        // are rejected, underruns are accepted (the caller declared a
        // ceiling, not a floor). NOISY-failure policy per CLAUDE.md: emit
        // `tracing::error!` with the declared/received pair before returning
        // `Code::InvalidArgument` so the invariant violation is impossible
        // to ignore in production logs.
        //
        // The check fires after the streaming loop and before STRLEN+RENAME,
        // so the temp key is left dangling rather than promoted — no
        // poisoning of the final key is possible. Defence-in-depth against
        // callers that construct a `RedisStore` without a `VerifyStore`
        // wrapper in front of it (the canonical pre-write enforcer).
        let total_bytes = u64::from(total_len);
        match upload_size {
            UploadSizeInfo::ExactSize(declared) if total_bytes != declared => {
                error!(
                    key = %final_key,
                    temp_key = %temp_key,
                    declared,
                    received = total_bytes,
                    "RedisStore::update: ExactSize mismatch — rejecting partial write before RENAME",
                );
                return Err(make_err!(
                    Code::InvalidArgument,
                    "RedisStore::update: ExactSize declared {declared} bytes but \
                     received {total_bytes} — refusing to RENAME temp key {temp_key} \
                     into {final_key} (would poison the CAS for every future read)"
                ));
            }
            UploadSizeInfo::MaxSize(max) if total_bytes > max => {
                error!(
                    key = %final_key,
                    temp_key = %temp_key,
                    max,
                    received = total_bytes,
                    "RedisStore::update: MaxSize exceeded — rejecting overrun before RENAME",
                );
                return Err(make_err!(
                    Code::InvalidArgument,
                    "RedisStore::update: MaxSize declared {max} bytes but \
                     received {total_bytes} — refusing to RENAME temp key {temp_key} \
                     into {final_key} (would poison the CAS for every future read)"
                ));
            }
            _ => {}
        }

        let cmd_start = Instant::now();
        let strlen_result = timeout(
            self.command_timeout,
            client.connection_manager.strlen(&temp_key),
        )
        .await;
        let blob_len: usize = match strlen_result {
            Err(_) => {
                let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                error!(cmd = "STRLEN", key = %final_key, elapsed_ms, "redis command timed out");
                return Err(make_err!(
                    Code::Unavailable,
                    "Redis STRLEN timed out after {elapsed_ms}ms for key {final_key}"
                ));
            }
            Ok(Err(ref err)) if err.is_timeout() => {
                // Inner response_timeout fired — reconnect before returning.
                // (FU-10: desynced slot would corrupt next STRLEN/RENAME.)
                let mut nl_err: Error = err.clone().into();
                nl_err.messages.push(format!(
                    "In RedisStore::update strlen check for {temp_key}"
                ));
                client.reconnect_on_timeout(&self.connection_manager, "update::strlen").await;
                return Err(nl_err);
            }
            Ok(result) => result
                .err_tip(|| format!("In RedisStore::update strlen check for {temp_key}"))?,
        };
        let elapsed = cmd_start.elapsed();
        if elapsed.as_secs() >= 5 {
            error!(cmd = "STRLEN", key = %final_key, elapsed_ms = elapsed.as_millis() as u64, "redis command slow (>5s)");
        } else if elapsed.as_secs() >= 1 {
            warn!(cmd = "STRLEN", key = %final_key, elapsed_ms = elapsed.as_millis() as u64, "redis command slow (>1s)");
        }
        // This is a safety check to ensure that in the event some kind of retry was to happen
        // and the data was appended to the key twice, we reject the data.
        if blob_len != usize::try_from(total_len).unwrap_or(usize::MAX) {
            return Err(make_input_err!(
                "Data length mismatch in RedisStore::update for {}({}) - expected {} bytes, got {} bytes",
                key.borrow().as_str(),
                temp_key,
                total_len,
                blob_len,
            ));
        }

        // Pipeline RENAME (and optionally PUBLISH) in a single round-trip.
        // Previously these were 1-2 separate commands; pipelining saves one RTT
        // when pub_sub is configured, and keeps the code consistent otherwise.
        let cmd_start = Instant::now();
        if let Some(pub_sub_channel) = &self.pub_sub_channel {
            // RENAME + PUBLISH in one pipeline round-trip.
            let rename_publish_result = timeout(
                self.command_timeout,
                pipe()
                    .rename(&temp_key, final_key.as_ref())
                    .publish(pub_sub_channel, final_key.as_ref())
                    .query_async::<((), ())>(&mut client.connection_manager),
            )
            .await;
            let result = match rename_publish_result {
                Err(_) => {
                    let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                    error!(cmd = "RENAME+PUBLISH", key = %final_key, elapsed_ms, "redis pipeline timed out");
                    return Err(make_err!(
                        Code::Unavailable,
                        "Redis RENAME+PUBLISH timed out after {elapsed_ms}ms for key {final_key}"
                    ));
                }
                Ok(Err(ref err)) if err.is_timeout() => {
                    // Inner response_timeout — desynced slot. (FU-10.)
                    let mut nl_err: Error = err.clone().into();
                    nl_err.messages.push(
                        "While pipelining RENAME+PUBLISH in RedisStore::update()".to_string(),
                    );
                    client.reconnect_on_timeout(&self.connection_manager, "update::rename_publish").await;
                    return Err(nl_err);
                }
                Ok(result) => result
                    .err_tip(|| "While pipelining RENAME+PUBLISH in RedisStore::update()")?,
            };
            let elapsed = cmd_start.elapsed();
            if elapsed.as_secs() >= 5 {
                error!(cmd = "RENAME+PUBLISH", key = %final_key, elapsed_ms = elapsed.as_millis() as u64, size_bytes = blob_len, "redis pipeline slow (>5s)");
            } else if elapsed.as_secs() >= 1 {
                warn!(cmd = "RENAME+PUBLISH", key = %final_key, elapsed_ms = elapsed.as_millis() as u64, size_bytes = blob_len, "redis pipeline slow (>1s)");
            }
            return Ok(result.1);
        }

        // No pub_sub — just RENAME.
        let rename_result = timeout(
            self.command_timeout,
            client.connection_manager.rename::<_, _, ()>(&temp_key, final_key.as_ref()),
        )
        .await;
        match rename_result {
            Err(_) => {
                let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                error!(cmd = "RENAME", key = %final_key, elapsed_ms, "redis command timed out");
                return Err(make_err!(
                    Code::Unavailable,
                    "Redis RENAME timed out after {elapsed_ms}ms for key {final_key}"
                ));
            }
            Ok(Err(ref err)) if err.is_timeout() => {
                // Inner response_timeout — desynced slot. (FU-10.)
                let mut nl_err: Error = err.clone().into();
                nl_err.messages.push(
                    "While queueing key rename in RedisStore::update()".to_string(),
                );
                client.reconnect_on_timeout(&self.connection_manager, "update::rename").await;
                return Err(nl_err);
            }
            Ok(result) => result
                .err_tip(|| "While queueing key rename in RedisStore::update()")?,
        };
        let elapsed = cmd_start.elapsed();
        if elapsed.as_secs() >= 5 {
            error!(cmd = "RENAME", key = %final_key, elapsed_ms = elapsed.as_millis() as u64, size_bytes = blob_len, "redis command slow (>5s)");
        } else if elapsed.as_secs() >= 1 {
            warn!(cmd = "RENAME", key = %final_key, elapsed_ms = elapsed.as_millis() as u64, size_bytes = blob_len, "redis command slow (>1s)");
        }

        Ok(())
    }

    // LINT: writer-termination contract for `get_part`.
    //
    // `RedisStore` is a leaf (own-bytes producer) and is wrapped by
    // `FastSlowStore` as the slow tier of `SMALL_CAS_CACHED` (≤16KB CAS
    // blobs on Valkey db=1) AND `AC_BACKEND_CACHED` (AC on db=0). The
    // wrapper layer (FastSlowStore) opens its own `get_part` with
    // `WriteHalfGuard::new(writer)` (active) and uses
    // `commit_delegated_if_ok(&res)` so that any leaf-level contract
    // violation is caught at the wrapper's Drop fallback (synthesized
    // Internal terminator unblocks the paired reader).
    //
    // Per the WriteHalfGuard #148 wave: leaf-side migrations were
    // explicitly REVERTED (commit `69bf59b2`: "WriteHalfGuard: revert
    // leaf-store migrations (decoration not load-bearing)") and the
    // `new_subordinate` constructor was DELETED (commit `d636b66f`),
    // because the wrapper's active guard is the load-bearing protection
    // and a leaf-level subordinate guard would be decorative noise.
    // See `nativelink-store/tests/redis_store_test.rs::
    // verify_store_around_redis_does_not_deadlock_on_get_part_notfound`
    // (the production-composition lock-in test for this property).
    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let offset = isize::try_from(offset).err_tip(|| "Could not convert offset to isize")?;
        let length = length
            .map(|v| usize::try_from(v).err_tip(|| "Could not convert length to usize"))
            .transpose()?;

        // To follow RBE spec we need to consider any digest's with
        // zero size to be existing.
        if is_zero_digest(key.borrow()) {
            return writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in redis store get_part");
        }

        let encoded_key = self.encode_key(&key);
        let encoded_key = encoded_key.as_ref();

        // N.B. the `-1`'s you see here are because redis GETRANGE is inclusive at both the start and end, so when we
        // do math with indices we change them to be exclusive at the end.

        // We want to read the data at the key from `offset` to `offset + length`.
        let data_start = offset;
        let data_end = data_start
            .saturating_add(length.unwrap_or(isize::MAX as usize) as isize)
            .saturating_sub(1);

        // Read in chunks of `read_chunk_size`. The outer loop handles a TOCTOU
        // race: GETRANGE on a missing key returns "" (not an error), so if a
        // concurrent `update` publishes the key (via RENAME) between our
        // GETRANGE and the EXISTS fallback check, we retry the read once.
        let mut client = self.get_client().await?;
        let mut retried = false;
        loop {
            let mut chunk_start = data_start;
            let mut chunk_end = cmp::min(
                data_start.saturating_add(self.read_chunk_size as isize) - 1,
                data_end,
            );

            loop {
                let cmd_start = Instant::now();
                let getrange_result = timeout(
                    self.command_timeout,
                    client.connection_manager.getrange(encoded_key, chunk_start, chunk_end),
                )
                .await;
                let chunk: Bytes = match getrange_result {
                    Err(_) => {
                        let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                        error!(cmd = "GETRANGE", key = %encoded_key, elapsed_ms, "redis command timed out");
                        return Err(make_err!(
                            Code::Unavailable,
                            "Redis GETRANGE timed out after {elapsed_ms}ms for key {encoded_key}"
                        ));
                    }
                    Ok(Err(ref err)) if err.is_timeout() => {
                        // Inner response_timeout — desynced slot. (FU-10.)
                        let mut nl_err: Error = err.clone().into();
                        nl_err.messages.push(
                            "In RedisStore::get_part::getrange".to_string(),
                        );
                        client.reconnect_on_timeout(&self.connection_manager, "get_part::getrange").await;
                        return Err(nl_err);
                    }
                    Ok(result) => result
                        .err_tip(|| "In RedisStore::get_part::getrange")?,
                };
                let elapsed = cmd_start.elapsed();
                if elapsed.as_secs() >= 5 {
                    error!(cmd = "GETRANGE", key = %encoded_key, elapsed_ms = elapsed.as_millis() as u64, size_bytes = chunk.len(), "redis command slow (>5s)");
                } else if elapsed.as_secs() >= 1 {
                    warn!(cmd = "GETRANGE", key = %encoded_key, elapsed_ms = elapsed.as_millis() as u64, size_bytes = chunk.len(), "redis command slow (>1s)");
                }

                let didnt_receive_full_chunk = chunk.len() < self.read_chunk_size;
                let reached_end_of_data = chunk_end == data_end;

                if didnt_receive_full_chunk || reached_end_of_data {
                    if !chunk.is_empty() {
                        writer
                            .send(chunk)
                            .await
                            .err_tip(|| "Failed to write data in RedisStore::get_part")?;
                    }

                    break; // No more data to read.
                }

                // We received a full chunk's worth of data, so write it...
                writer
                    .send(chunk)
                    .await
                    .err_tip(|| "Failed to write data in RedisStore::get_part")?;

                // ...and go grab the next chunk.
                chunk_start = chunk_end + 1;
                chunk_end = cmp::min(
                    chunk_start.saturating_add(self.read_chunk_size as isize) - 1,
                    data_end,
                );
            }

            // If we didn't write any data, check if the key exists, if not
            // return a NotFound error. This is required by spec.
            if writer.get_bytes_written() == 0 {
                let cmd_start = Instant::now();
                let exists_result = timeout(
                    self.command_timeout,
                    client.connection_manager.exists(encoded_key),
                )
                .await;
                let exists: bool = match exists_result {
                    Err(_) => {
                        let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                        error!(cmd = "EXISTS", key = %encoded_key, elapsed_ms, "redis command timed out");
                        return Err(make_err!(
                            Code::Unavailable,
                            "Redis EXISTS timed out after {elapsed_ms}ms for key {encoded_key}"
                        ));
                    }
                    Ok(Err(ref err)) if err.is_timeout() => {
                        // Inner response_timeout — desynced slot. (FU-10.)
                        let mut nl_err: Error = err.clone().into();
                        nl_err.messages.push(
                            "In RedisStore::get_part::zero_exists".to_string(),
                        );
                        client.reconnect_on_timeout(&self.connection_manager, "get_part::zero_exists").await;
                        return Err(nl_err);
                    }
                    Ok(result) => result
                        .err_tip(|| "In RedisStore::get_part::zero_exists")?,
                };
                let elapsed = cmd_start.elapsed();
                if elapsed.as_secs() >= 5 {
                    error!(cmd = "EXISTS", key = %encoded_key, elapsed_ms = elapsed.as_millis() as u64, "redis command slow (>5s)");
                } else if elapsed.as_secs() >= 1 {
                    warn!(cmd = "EXISTS", key = %encoded_key, elapsed_ms = elapsed.as_millis() as u64, "redis command slow (>1s)");
                }

                if !exists {
                    // Leaf-level Err exit: writer NOT terminated. The wrapper
                    // layer (FastSlowStore -> VerifyStore in production)
                    // catches this contract violation via its outer
                    // WriteHalfGuard's Drop fallback. See the function-level
                    // LINT comment + `redis_store_test.rs::
                    // verify_store_around_redis_does_not_deadlock_on_get_part_notfound`.
                    return Err(make_err!(
                        Code::NotFound,
                        "Data not found in Redis store for digest: {key:?}"
                    ));
                }

                // Key exists but GETRANGE returned empty — a concurrent RENAME
                // may have published the key between our GETRANGE and EXISTS.
                // Retry the entire read once.
                if !retried {
                    retried = true;
                    warn!(
                        ?key,
                        "GETRANGE returned empty but EXISTS=true, retrying (TOCTOU race)"
                    );
                    continue;
                }
                // Already retried — offset is genuinely past end of data (valid EOF).
            }

            break;
        }

        writer
            .send_eof()
            .err_tip(|| "Failed to write EOF in redis store get_part")
    }

    /// Pipelined batch read: sends all GETRANGE commands in a single Redis
    /// round-trip. Intended for small blobs (directory protos, action results)
    /// where each blob fits in a single GETRANGE chunk.
    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        let n = keys.len();
        if n == 0 {
            return Vec::new();
        }

        // Separate zero-digest keys from keys that need Redis lookup.
        let max_len = length.unwrap_or(isize::MAX as u64) as isize;
        let mut pipeline_indices: Vec<usize> = Vec::with_capacity(n);
        let mut encoded_keys: Vec<String> = Vec::with_capacity(n);
        let mut results: Vec<Result<Bytes, Error>> =
            (0..n).map(|_| Err(make_err!(Code::Internal, "batch slot not filled"))).collect();

        for (i, key) in keys.iter().enumerate() {
            if is_zero_digest(key.borrow()) {
                results[i] = Ok(Bytes::new());
            } else {
                let encoded = self.encode_key(key);
                encoded_keys.push(encoded.into_owned());
                pipeline_indices.push(i);
            }
        }

        if pipeline_indices.is_empty() {
            return results;
        }

        // Process keys in chunks to avoid unbounded Redis response buffering.
        // Each chunk builds a pipeline with GETRANGE+EXISTS and sends it in
        // one round-trip.
        for chunk_start in (0..encoded_keys.len()).step_by(MAX_PIPELINE_BATCH) {
            let chunk_end = cmp::min(chunk_start + MAX_PIPELINE_BATCH, encoded_keys.len());
            let chunk_keys = &encoded_keys[chunk_start..chunk_end];
            let chunk_indices = &pipeline_indices[chunk_start..chunk_end];

            let mut pipeline = pipe();
            for encoded_key in chunk_keys {
                pipeline.getrange(encoded_key.as_str(), 0isize, max_len.saturating_sub(1));
                pipeline.exists(encoded_key.as_str());
            }

            let mut client = match self.get_client().await {
                Ok(c) => c,
                Err(e) => {
                    for &idx in chunk_indices {
                        results[idx] = Err(make_err!(
                            Code::Unavailable,
                            "failed to get redis client for batch read: {:?}",
                            e
                        ));
                    }
                    return results;
                }
            };

            let cmd_start = Instant::now();
            let pipeline_result = timeout(
                self.command_timeout,
                pipeline.query_async::<Vec<Value>>(&mut client.connection_manager.clone()),
            )
            .await;

            let raw_values = match pipeline_result {
                Err(_) => {
                    let elapsed_ms = cmd_start.elapsed().as_millis() as u64;
                    error!(
                        cmd = "pipelined batch GETRANGE+EXISTS",
                        key_count = chunk_keys.len(),
                        elapsed_ms,
                        "redis batch pipeline timed out"
                    );
                    for &idx in chunk_indices {
                        results[idx] = Err(make_err!(
                            Code::Unavailable,
                            "Redis batch GETRANGE+EXISTS timed out after {elapsed_ms}ms"
                        ));
                    }
                    return results;
                }
                Ok(Err(ref err))
                    if err.kind()
                        == redis::ErrorKind::Server(redis::ServerErrorKind::CrossSlot) =>
                {
                    // Cluster mode: keys hash to different slots. Fall back to
                    // concurrent individual reads for ALL remaining keys.
                    info!(
                        key_count = n,
                        "CrossSlot error in batch_get_part_unchunked, falling back to per-key reads"
                    );
                    drop(client);
                    let futs: FuturesUnordered<_> = keys
                        .into_iter()
                        .enumerate()
                        .map(|(idx, key)| async move {
                            let result = self.get_part_unchunked(key, 0, length).await;
                            (idx, result)
                        })
                        .collect();
                    let mut fallback_results: Vec<Result<Bytes, Error>> =
                        (0..n).map(|_| Err(make_err!(Code::Internal, "batch slot not filled")))
                            .collect();
                    let mut stream = futs;
                    while let Some((idx, result)) = stream.next().await {
                        fallback_results[idx] = result;
                    }
                    return fallback_results;
                }
                Ok(Err(ref e)) if e.is_timeout() => {
                    // Inner response_timeout — desynced slot. (FU-10.)
                    client.reconnect_on_timeout(&self.connection_manager, "batch_get_part_unchunked").await;
                    for &idx in chunk_indices {
                        results[idx] = Err(make_err!(
                            Code::Unavailable,
                            "redis batch GETRANGE+EXISTS timed out (inner response_timeout)"
                        ));
                    }
                    return results;
                }
                Ok(Err(e)) => {
                    for &idx in chunk_indices {
                        results[idx] = Err(make_err!(
                            Code::Unavailable,
                            "redis batch GETRANGE+EXISTS failed: {:?}",
                            e
                        ));
                    }
                    return results;
                }
                Ok(Ok(v)) => v,
            };

            let elapsed = cmd_start.elapsed();
            if elapsed.as_secs() >= 5 {
                error!(
                    cmd = "pipelined batch GETRANGE+EXISTS",
                    key_count = chunk_keys.len(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "redis batch pipeline slow (>5s)"
                );
            } else if elapsed.as_secs() >= 1 {
                warn!(
                    cmd = "pipelined batch GETRANGE+EXISTS",
                    key_count = chunk_keys.len(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "redis batch pipeline slow (>1s)"
                );
            }

            // Each key contributes 2 values: [getrange_result, exists_result].
            let expected_len = chunk_keys.len() * 2;
            if raw_values.len() != expected_len {
                let err_msg = format!(
                    "Redis batch pipeline returned {} values, expected {} (2 per key for {} keys)",
                    raw_values.len(),
                    expected_len,
                    chunk_keys.len()
                );
                for &idx in chunk_indices {
                    results[idx] = Err(make_err!(Code::Internal, "{}", err_msg));
                }
                return results;
            }

            for (pair_idx, &result_idx) in chunk_indices.iter().enumerate() {
                let getrange_val = &raw_values[pair_idx * 2];
                let exists_val = &raw_values[pair_idx * 2 + 1];

                let data: Vec<u8> = match redis::from_redis_value_ref(getrange_val) {
                    Ok(v) => v,
                    Err(e) => {
                        results[result_idx] = Err(make_err!(
                            Code::Internal,
                            "failed to parse GETRANGE result for key {}: {:?}",
                            chunk_keys[pair_idx],
                            e
                        ));
                        continue;
                    }
                };
                let exists: bool = match redis::from_redis_value_ref(exists_val) {
                    Ok(v) => v,
                    Err(e) => {
                        results[result_idx] = Err(make_err!(
                            Code::Internal,
                            "failed to parse EXISTS result for key {}: {:?}",
                            chunk_keys[pair_idx],
                            e
                        ));
                        continue;
                    }
                };

                if data.is_empty() && !exists {
                    results[result_idx] = Err(make_err!(
                        Code::NotFound,
                        "Data not found in Redis store for key: {}",
                        chunk_keys[pair_idx]
                    ));
                } else {
                    results[result_idx] = Ok(Bytes::from(data));
                }
            }
        }

        results
    }

    /// Delete the key from Redis via a `DEL` command. Uses the same
    /// `encode_key` path as `update` so the key form is consistent.
    /// Returns `Ok(())` if one or more keys were deleted, `Code::NotFound`
    /// if Redis reports 0 deleted keys (key was absent).
    async fn remove(self: Pin<&Self>, key: StoreKey<'_>) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            return Ok(());
        }
        let encoded = self.encode_key(&key);
        let mut client = self
            .get_client()
            .await
            .err_tip(|| "RedisStore::remove get_client")?;
        let del_result = timeout(
            self.command_timeout,
            client.connection_manager.del::<_, u64>(encoded.as_ref()),
        )
        .await;
        let deleted: u64 = match del_result {
            Err(_) => {
                return Err(make_err!(
                    Code::Unavailable,
                    "RedisStore::remove DEL timed out for key {}",
                    encoded,
                ));
            }
            Ok(Err(ref err)) if err.is_timeout() => {
                // Inner response_timeout — desynced slot. (FU-10.)
                let mut nl_err: Error = err.clone().into();
                nl_err.messages.push(format!("RedisStore::remove DEL failed for key {encoded}"));
                client.reconnect_on_timeout(&self.connection_manager, "remove::del").await;
                return Err(nl_err);
            }
            Ok(result) => result
                .err_tip(|| format!("RedisStore::remove DEL failed for key {encoded}"))?,
        };
        if deleted == 0 {
            return Err(make_err!(
                Code::NotFound,
                "RedisStore::remove: key not found in Redis: {}",
                encoded,
            ));
        }
        Ok(())
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send> {
        self
    }

    fn register_health(self: Arc<Self>, registry: &mut HealthRegistryBuilder) {
        registry.register_indicator(self);
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        // The dispatcher is initialized eagerly during `RedisStore::new_standard`
        // (BLOCKER 1 fix). Either it succeeded — and the OnceCell holds a
        // sender ready to forward registrations — or `new_standard` itself
        // returned `Err` and the server failed to start. Reaching this code
        // with an empty OnceCell means `enable_keyspace_notifications=false`
        // (the disabled-on-purpose path or cluster-mode forced-disable) and
        // the caller is asking for invalidation that will never fire —
        // surface that as a typed `Code::FailedPrecondition` Err so wrappers
        // like `ExistenceCacheStore::new_with_time` can degrade gracefully
        // (log + vulnerable-mode flag) instead of silently retaining
        // stale-positive entries.
        let Some(tx) = self.keyspace_dispatcher_tx.get() else {
            return Err(make_err!(
                Code::FailedPrecondition,
                "RedisStore: enable_keyspace_notifications=false; ItemCallbacks will never fire \
                 and wrapper caches such as ExistenceCacheStore would silently retain \
                 stale-positive entries when keys are evicted. Set \
                 enable_keyspace_notifications=true (and grant CONFIG ACLs) to use \
                 invalidation, or remove the wrapper to acknowledge the trade-off."
            ));
        };
        tx.send(callback).map_err(|_| {
            make_err!(
                Code::Internal,
                "RedisStore keyspace dispatcher task has exited; ItemCallbacks cannot be \
                 registered"
            )
        })?;
        // Observable counter for the inert-dispatcher 60s startup warn (see
        // `init_keyspace_dispatcher_eager`). Bump only on successful send so
        // the warn correctly fires when every registration attempt failed.
        self.callbacks_registered_total
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Reports whether `register_item_callback` will dispatch real
    /// Redis-side eviction events (`del` / `expired` / `evicted`
    /// keyspace notifications) to registered listeners.
    ///
    /// Returns `self.enable_keyspace_notifications` — the OnceCell
    /// `keyspace_dispatcher_tx` is populated iff the spec opted in AND
    /// `init_keyspace_dispatcher_eager` succeeded at construction.
    /// `RedisStore::new_standard` returns `Err` if the eager init
    /// failed, so reaching the trait impl with
    /// `enable_keyspace_notifications=true` guarantees the dispatcher
    /// is wired. Cluster mode is forced to `false` in
    /// `set_spec_defaults` (cluster keyspace-notification semantics
    /// are documented as undefined in Redis).
    ///
    /// Wrappers like `FastSlowStore`'s #367
    /// `SlowEvictionInvalidatesStableSetListener` consult this flag at
    /// registration time to classify their warn arm correctly. When
    /// `false`, `register_item_callback` returns
    /// `Code::FailedPrecondition` so wrappers like
    /// `ExistenceCacheStore` can either fail or degrade explicitly
    /// rather than silently retain stale-positive entries.
    fn supports_removal_callbacks(&self) -> bool {
        self.enable_keyspace_notifications
    }

    /// RedisStore is a leaf — Redis is the persistent backing for small
    /// CAS blobs. The BIS pipeline is owned by the wrapping `FastSlowStore`,
    /// not this leaf. Treat as Leaf with empty drains.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    /// RedisStore is a leaf — pinning a key in Redis is not part of the
    /// store contract here (Redis has its own TTL semantics). No-op.
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    /// RedisStore is a leaf — `mark_stable` is a no-op (the BIS pipeline
    /// is owned by the wrapping `FastSlowStore`, not this leaf). (Task #157.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

#[async_trait]
impl<C, M> HealthStatusIndicator for RedisStore<C, M>
where
    C: ConnectionLike + Clone + Send + Sync + Unpin + 'static,
    M: RedisManager<C> + Send + Sync + Unpin + 'static,
{
    fn get_name(&self) -> &'static str {
        "RedisStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}

// -------------------------------------------------------------------
// Below this line are specific to the redis scheduler implementation.
// -------------------------------------------------------------------

/// The time in milliseconds that a redis cursor can be idle before it is closed.
const CURSOR_IDLE_MS: u64 = 30_000;
/// The name of the field in the Redis hash that stores the data.
const DATA_FIELD_NAME: &str = "data";
/// The name of the field in the Redis hash that stores the version.
const VERSION_FIELD_NAME: &str = "version";
/// The time to live of indexes in seconds. After this time redis may delete the index.
const INDEX_TTL_S: u64 = 60 * 60 * 24; // 24 hours.

#[allow(rustdoc::broken_intra_doc_links)]
/// Lua script to set a key if the version matches.
/// Args:
///   KEYS[1]: The key where the version is stored.
///   ARGV[1]: The expected version.
///   ARGV[2]: The new data.
///   ARGV[3*]: Key-value pairs of additional data to include.
/// Returns:
///   The new version if the version matches. nil is returned if the
///   value was not set.
pub const LUA_VERSION_SET_SCRIPT: &str = formatcp!(
    r"
local key = KEYS[1]
local expected_version = tonumber(ARGV[1])
local new_data = ARGV[2]
local new_version = redis.call('HINCRBY', key, '{VERSION_FIELD_NAME}', 1)
local i
local indexes = {{}}

if new_version-1 ~= expected_version then
    redis.call('HINCRBY', key, '{VERSION_FIELD_NAME}', -1)
    return {{ 0, new_version-1 }}
end
-- Skip first 2 argvs, as they are known inputs.
-- Remember: Lua is 1-indexed.
for i=3, #ARGV do
    indexes[i-2] = ARGV[i]
end

-- In testing we witnessed redis sometimes not update our FT indexes
-- resulting in stale data. It appears if we delete our keys then insert
-- them again it works and reduces risk significantly.
redis.call('DEL', key)
redis.call('HSET', key, '{DATA_FIELD_NAME}', new_data, '{VERSION_FIELD_NAME}', new_version, unpack(indexes))

return {{ 1, new_version }}
"
);

/// This is the output of the calculations below hardcoded into the executable.
const FINGERPRINT_CREATE_INDEX_HEX: &str = "3e762c15";

#[cfg(test)]
mod test {
    use super::FINGERPRINT_CREATE_INDEX_HEX;

    /// String of the `FT.CREATE` command used to create the index template.
    const CREATE_INDEX_TEMPLATE: &str = "FT.CREATE {} ON HASH PREFIX 1 {} NOOFFSETS NOHL NOFIELDS NOFREQS SCHEMA {} TAG CASESENSITIVE SORTABLE";

    /// Compile-time fingerprint of the `FT.CREATE` command used to create the
    /// index template. This is a simple CRC32 checksum of the command string.
    /// We don't care about it actually being a valid CRC32 checksum, just that
    /// it's a unique identifier with a low chance of collision.
    const fn fingerprint_create_index_template() -> u32 {
        const POLY: u32 = 0xEDB8_8320;
        const DATA: &[u8] = CREATE_INDEX_TEMPLATE.as_bytes();
        let mut crc = 0xFFFF_FFFF;
        let mut i = 0;
        while i < DATA.len() {
            let byte = DATA[i];
            crc ^= byte as u32;

            let mut j = 0;
            while j < 8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ POLY
                } else {
                    crc >> 1
                };
                j += 1;
            }
            i += 1;
        }
        crc
    }

    /// Verify that our calculation always evaluates to this fixed value.
    #[test]
    fn test_fingerprint_value() {
        assert_eq!(
            format!("{:08x}", &fingerprint_create_index_template()),
            FINGERPRINT_CREATE_INDEX_HEX,
        );
    }
}

/// Get the name of the index to create for the given field.
/// This will add some prefix data to the name to try and ensure
/// if the index definition changes, the name will get a new name.
macro_rules! get_index_name {
    ($prefix:expr, $field:expr, $maybe_sort:expr) => {
        format_args!(
            "{}_{}_{}_{}",
            $prefix,
            $field,
            $maybe_sort.unwrap_or(""),
            FINGERPRINT_CREATE_INDEX_HEX
        )
    };
}

/// Try to sanitize a string to be used as a Redis key.
/// We don't actually modify the string, just check if it's valid.
const fn try_sanitize(s: &str) -> Option<&str> {
    // Note: We cannot use for loops or iterators here because they are not const.
    // Allowing us to use a const function here gives the compiler the ability to
    // optimize this function away entirely in the case where the input is constant.
    let chars = s.as_bytes();
    let mut i: usize = 0;
    let len = s.len();
    loop {
        if i >= len {
            break;
        }
        let c = chars[i];
        if !c.is_ascii_alphanumeric() && c != b'_' {
            return None;
        }
        i += 1;
    }
    Some(s)
}

/// An individual subscription to a key in Redis.
#[derive(Debug)]
pub struct RedisSubscription {
    receiver: Option<tokio::sync::watch::Receiver<String>>,
    weak_subscribed_keys: Weak<RwLock<StringPatriciaMap<RedisSubscriptionPublisher>>>,
}

impl SchedulerSubscription for RedisSubscription {
    /// Wait for the subscription key to change.
    async fn changed(&mut self) -> Result<(), Error> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| make_err!(Code::Internal, "In RedisSubscription::changed::as_mut"))?;
        receiver
            .changed()
            .await
            .map_err(|_| make_err!(Code::Internal, "In RedisSubscription::changed::changed"))
    }
}

// If the subscription is dropped, we need to possibly remove the key from the
// subscribed keys map.
impl Drop for RedisSubscription {
    fn drop(&mut self) {
        let Some(receiver) = self.receiver.take() else {
            warn!("RedisSubscription has already been dropped, nothing to do.");
            return; // Already dropped, nothing to do.
        };
        let key = receiver.borrow().clone();
        // IMPORTANT: This must be dropped before receiver_count() is called.
        drop(receiver);
        let Some(subscribed_keys) = self.weak_subscribed_keys.upgrade() else {
            return; // Already dropped, nothing to do.
        };
        let mut subscribed_keys = subscribed_keys.write();
        let Some(value) = subscribed_keys.get(&key) else {
            error!(
                "Key {key} was not found in subscribed keys when checking if it should be removed."
            );
            return;
        };
        // If we have no receivers, cleanup the entry from our map.
        if value.receiver_count() == 0 {
            subscribed_keys.remove(key);
        }
    }
}

/// A publisher for a key in Redis.
#[derive(Debug)]
struct RedisSubscriptionPublisher {
    sender: Mutex<tokio::sync::watch::Sender<String>>,
}

impl RedisSubscriptionPublisher {
    fn new(
        key: String,
        weak_subscribed_keys: Weak<RwLock<StringPatriciaMap<Self>>>,
    ) -> (Self, RedisSubscription) {
        let (sender, receiver) = tokio::sync::watch::channel(key);
        let publisher = Self {
            sender: Mutex::new(sender),
        };
        let subscription = RedisSubscription {
            receiver: Some(receiver),
            weak_subscribed_keys,
        };
        (publisher, subscription)
    }

    fn subscribe(
        &self,
        weak_subscribed_keys: Weak<RwLock<StringPatriciaMap<Self>>>,
    ) -> RedisSubscription {
        let receiver = self.sender.lock().subscribe();
        RedisSubscription {
            receiver: Some(receiver),
            weak_subscribed_keys,
        }
    }

    fn receiver_count(&self) -> usize {
        self.sender.lock().receiver_count()
    }

    fn notify(&self) {
        // TODO(https://github.com/sile/patricia_tree/issues/40) When this is addressed
        // we can remove the `Mutex` and use the mutable iterator directly.
        self.sender.lock().send_modify(|_| {});
    }
}

#[derive(Debug, Clone)]
pub struct RedisSubscriptionManager {
    subscribed_keys: Arc<RwLock<StringPatriciaMap<RedisSubscriptionPublisher>>>,
    tx_for_test: UnboundedSender<String>,
    _subscription_spawn: Arc<Mutex<JoinHandleDropGuard<()>>>,
}

impl RedisSubscriptionManager {
    pub fn new(subscriber_channel: UnboundedReceiver<PushInfo>) -> Self {
        let subscribed_keys = Arc::new(RwLock::new(StringPatriciaMap::new()));
        let subscribed_keys_weak = Arc::downgrade(&subscribed_keys);
        let (tx_for_test, mut rx_for_test) = unbounded_channel();
        let mut local_subscriber_channel = UnboundedReceiverStream::new(subscriber_channel);
        Self {
            subscribed_keys,
            tx_for_test,
            _subscription_spawn: Arc::new(Mutex::new(spawn!(
                "redis_subscribe_spawn",
                async move {
                    loop {
                        loop {
                            let key = select! {
                                value = rx_for_test.recv() => {
                                    let Some(value) = value else {
                                        unreachable!("Channel should never close");
                                    };
                                    value
                                },
                                maybe_push_info = local_subscriber_channel.next() => {
                                    if let Some(push_info) = maybe_push_info {
                                        match push_info.kind {
                                            redis::PushKind::PMessage => {},
                                            redis::PushKind::PSubscribe => {
                                                trace!(?push_info, "PSubscribe, ignore");
                                                continue;
                                            }
                                            _ => {
                                                warn!(?push_info, "Other push_info message, discarded");
                                                continue;
                                            },
                                        }
                                        if push_info.data.len() != 3 {
                                            error!(?push_info, "Expected exactly 3 values on subscriber channel (pattern, channel, value)");
                                            continue;
                                        }
                                        match push_info.data.last().unwrap() {
                                            Value::SimpleString(s) => {
                                                s.clone()
                                            }
                                            Value::BulkString(v) => {
                                                String::from_utf8(v.clone()).expect("String message")
                                            }
                                            other => {
                                                error!(?other, "Received non-string message in RedisSubscriptionManager");
                                                continue;
                                            }
                                        }
                                    } else {
                                        error!("Error receiving message in RedisSubscriptionManager from subscriber_channel");
                                        break;
                                    }
                                }
                            };
                            trace!(key, "New subscription manager key");
                            let Some(subscribed_keys) = subscribed_keys_weak.upgrade() else {
                                warn!(
                                    "It appears our parent has been dropped, exiting RedisSubscriptionManager spawn"
                                );
                                return;
                            };
                            let subscribed_keys_mux = subscribed_keys.read();
                            subscribed_keys_mux
                                .common_prefix_values(&*key)
                                .for_each(RedisSubscriptionPublisher::notify);
                        }
                        // Sleep for a small amount of time to ensure we don't reconnect too quickly.
                        sleep(Duration::from_secs(1)).await;
                        // If we reconnect or lag behind we might have had dirty keys, so we need to
                        // flag all of them as changed.
                        let Some(subscribed_keys) = subscribed_keys_weak.upgrade() else {
                            warn!(
                                "It appears our parent has been dropped, exiting RedisSubscriptionManager spawn"
                            );
                            return;
                        };
                        let subscribed_keys_mux = subscribed_keys.read();
                        // Just in case also get a new receiver.
                        for publisher in subscribed_keys_mux.values() {
                            publisher.notify();
                        }
                    }
                }
            ))),
        }
    }
}

impl SubscriptionManagerNotify for RedisSubscriptionManager {
    fn notify_for_test(&self, value: String) {
        self.tx_for_test.send(value).unwrap();
    }
}

impl SchedulerSubscriptionManager for RedisSubscriptionManager {
    type Subscription = RedisSubscription;

    fn subscribe<K>(&self, key: K) -> Result<Self::Subscription, Error>
    where
        K: SchedulerStoreKeyProvider,
    {
        let weak_subscribed_keys = Arc::downgrade(&self.subscribed_keys);
        let mut subscribed_keys = self.subscribed_keys.write();
        let key = key.get_key();
        let key_str = key.as_str();
        let mut subscription = if let Some(publisher) = subscribed_keys.get(&key_str) {
            publisher.subscribe(weak_subscribed_keys)
        } else {
            let (publisher, subscription) =
                RedisSubscriptionPublisher::new(key_str.to_string(), weak_subscribed_keys);
            subscribed_keys.insert(key_str, publisher);
            subscription
        };
        subscription
            .receiver
            .as_mut()
            .ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "Receiver should be set in RedisSubscriptionManager::subscribe"
                )
            })?
            .mark_changed();

        Ok(subscription)
    }

    fn is_reliable() -> bool {
        false
    }
}

impl<C, M> SchedulerStore for RedisStore<C, M>
where
    C: Clone + ConnectionLike + Sync + Send + 'static,
    M: RedisManager<C> + Sync + Send + 'static,
{
    type SubscriptionManager = RedisSubscriptionManager;

    async fn subscription_manager(&self) -> Result<Arc<RedisSubscriptionManager>, Error> {
        self.subscription_manager
            .get_or_try_init(|| async move {
                let Some(subscriber_channel) = self.subscriber_channel.lock().take() else {
                    return Err(make_input_err!(
                        "Multiple attempts to obtain the subscription manager in RedisStore"
                    ));
                };
                let Some(pub_sub_channel) = &self.pub_sub_channel else {
                    return Err(make_input_err!(
                        "RedisStore must have a pubsub for Redis Scheduler if using subscriptions"
                    ));
                };
                self.connection_manager.psubscribe(pub_sub_channel).await?;
                Ok(Arc::new(RedisSubscriptionManager::new(subscriber_channel)))
            })
            .await
            .map(Clone::clone)
    }

    async fn update_data<T>(&self, data: T) -> Result<Option<i64>, Error>
    where
        T: SchedulerStoreDataProvider
            + SchedulerStoreKeyProvider
            + SchedulerCurrentVersionProvider
            + Send,
    {
        let key = data.get_key();
        let redis_key = self.encode_key(&key);
        let mut client = self.get_client().await?;
        let maybe_index = data.get_indexes().err_tip(|| {
            format!("Err getting index in RedisStore::update_data::versioned for {redis_key}")
        })?;
        if <T as SchedulerStoreKeyProvider>::Versioned::VALUE {
            let current_version = data.current_version();
            let data = data.try_into_bytes().err_tip(|| {
                format!("Could not convert value to bytes in RedisStore::update_data::versioned for {redis_key}")
            })?;
            let mut script = self.connection_manager.update_script(redis_key.as_ref());
            let mut script_invocation = script.arg(format!("{current_version}")).arg(data.to_vec());
            for (name, value) in maybe_index {
                script_invocation = script_invocation.arg(name).arg(value.to_vec());
            }
            let start = Instant::now();
            let (success, new_version): (bool, i64) = match script_invocation
                .invoke_async(&mut client.connection_manager)
                .await
            {
                Ok(v) => v,
                Err(err)
                    if err.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly) =>
                {
                    client.reconnect(&self.connection_manager).await?;
                    script_invocation
                        .invoke_async(&mut client.connection_manager)
                        .await
                        .err_tip(|| format!("(after reconnect) In RedisStore::update_data::versioned for {key:?}"))?
                }
                Err(err) if err.is_timeout() => {
                    // Inner response_timeout — desynced slot, reconnect before
                    // returning so the scheduler's next Lua invocation on this
                    // slot does not read an orphaned frame. (FU-10: same desync
                    // mechanism as the CAS SETRANGE path; scheduler state
                    // desync is equally damaging to AC desync.)
                    let mut error: Error = err.into();
                    error
                        .messages
                        .push(format!("In RedisStore::update_data::versioned (timeout) for {key:?}"));
                    client.reconnect_on_timeout(&self.connection_manager, "update_data::versioned").await;
                    return Err(error);
                }
                Err(err) => {
                    let mut error: Error = err.into();
                    error
                        .messages
                        .push(format!("In RedisStore::update_data::versioned for {key:?}"));
                    return Err(error);
                }
            };

            let elapsed = start.elapsed();

            if elapsed > Duration::from_millis(100) {
                warn!(
                    %redis_key,
                    ?elapsed,
                    "Slow Redis version-set operation"
                );
            }
            if !success {
                warn!(
                    %redis_key,
                    %key,
                    %current_version,
                    %new_version,
                    caller = core::any::type_name::<T>(),
                    "Redis version conflict - optimistic lock failed"
                );
                return Ok(None);
            }
            trace!(
                %redis_key,
                %key,
                old_version = %current_version,
                %new_version,
                "Updated redis key to new version"
            );
            // If we have a publish channel configured, send a notice that the key has been set.
            if let Some(pub_sub_channel) = &self.pub_sub_channel {
                return Ok(client
                    .connection_manager
                    .publish(pub_sub_channel, redis_key.as_ref())
                    .await?);
            }
            Ok(Some(new_version))
        } else {
            let data = data.try_into_bytes().err_tip(|| {
                format!("Could not convert value to bytes in RedisStore::update_data::noversion for {redis_key}")
            })?;
            let mut fields: Vec<(String, _)> = vec![];
            fields.push((DATA_FIELD_NAME.into(), data.to_vec()));
            for (name, value) in maybe_index {
                fields.push((name.into(), value.to_vec()));
            }
            match client
                .connection_manager
                .hset_multiple::<_, _, _, ()>(redis_key.as_ref(), &fields)
                .await
            {
                Ok(v) => v,
                Err(err)
                    if err.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly) =>
                {
                    client.reconnect(&self.connection_manager).await?;
                    client
                        .connection_manager
                        .hset_multiple::<_, _, _, ()>(redis_key.as_ref(), &fields)
                        .await
                        .err_tip(|| format!("(after reconnect) In RedisStore::update_data::noversion for {redis_key}"))?;
                }
                Err(err) if err.is_timeout() => {
                    // Inner response_timeout — desynced slot. (FU-10: scheduler
                    // hset desync is not lower-risk than CAS desync — the next
                    // hset on this slot reads the orphaned HSET response frame
                    // as data, corrupting the scheduler index entry.)
                    let mut error: Error = err.into();
                    error.messages.push(format!(
                        "In RedisStore::update_data::noversion (timeout) for {redis_key}"
                    ));
                    client.reconnect_on_timeout(&self.connection_manager, "update_data::noversion").await;
                    return Err(error);
                }
                Err(err) => {
                    let mut error: Error = err.into();
                    error.messages.push(format!(
                        "In RedisStore::update_data::noversion for {redis_key}"
                    ));
                    return Err(error);
                }
            }
            // If we have a publish channel configured, send a notice that the key has been set.
            if let Some(pub_sub_channel) = &self.pub_sub_channel {
                return Ok(client
                    .connection_manager
                    .publish(pub_sub_channel, redis_key.as_ref())
                    .await?);
            }
            Ok(Some(0)) // Always use "0" version since this is not a versioned request.
        }
    }

    async fn search_by_index_prefix<K>(
        &self,
        index: K,
    ) -> Result<
        impl Stream<Item = Result<<K as SchedulerStoreDecodeTo>::DecodeOutput, Error>> + Send,
        Error,
    >
    where
        K: SchedulerIndexProvider + SchedulerStoreDecodeTo + Send,
    {
        let index_value = index.index_value();
        let run_ft_aggregate = || {
            let sanitized_field = try_sanitize(index_value.as_ref()).err_tip(|| {
                format!("In RedisStore::search_by_index_prefix::try_sanitize - {index_value:?}")
            })?;
            Ok::<_, Error>(async move {
                ft_aggregate(
                    self.connection_manager.get_connection().await?.0,
                    format!(
                        "{}",
                        get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY)
                    ),
                    if sanitized_field.is_empty() {
                        "*".to_string()
                    } else {
                        format!("@{}:{{ {} }}", K::INDEX_NAME, sanitized_field)
                    },
                    FtAggregateOptions {
                        load: vec![DATA_FIELD_NAME.into(), VERSION_FIELD_NAME.into()],
                        cursor: FtAggregateCursor {
                            count: self.max_count_per_cursor,
                            max_idle: CURSOR_IDLE_MS,
                        },
                        sort_by: K::MAYBE_SORT_KEY.map_or_else(Vec::new, |v| vec![format!("@{v}")]),
                    },
                )
                .await
            })
        };

        let stream = run_ft_aggregate()?
            .or_else(|_| async move {
                let mut schema = vec![SearchSchema {
                    field_name: K::INDEX_NAME.into(),
                    sortable: false,
                }];
                if let Some(sort_key) = K::MAYBE_SORT_KEY {
                    schema.push(SearchSchema {
                        field_name: sort_key.into(),
                        sortable: true,
                    });
                }

                let create_result = ft_create(
                    self.connection_manager.get_connection().await?.0,
                    format!(
                        "{}",
                        get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY)
                    ),
                    FtCreateOptions {
                        prefixes: vec![K::KEY_PREFIX.into()],
                        nohl: true,
                        nofields: true,
                        nofreqs: true,
                        nooffsets: true,
                        temporary: Some(INDEX_TTL_S),
                    },
                    schema,
                )
                .await
                .err_tip(|| {
                    format!(
                        "Error with ft_create in RedisStore::search_by_index_prefix({})",
                        get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY),
                    )
                });
                let run_result = run_ft_aggregate()?.await.err_tip(|| {
                    format!(
                        "Error with second ft_aggregate in RedisStore::search_by_index_prefix({})",
                        get_index_name!(K::KEY_PREFIX, K::INDEX_NAME, K::MAYBE_SORT_KEY),
                    )
                });
                // Creating the index will race which is ok. If it fails to create, we only
                // error if the second ft_aggregate call fails and fails to create.
                run_result.or_else(move |e| create_result.merge(Err(e)))
            })
            .await?;
        Ok(stream.filter_map(|result| async move {
            let raw_redis_map = match result {
                Ok(v) => v,
                Err(e) => {
                    return Some(
                        Err(Error::from(e))
                            .err_tip(|| "Error in stream of in RedisStore::search_by_index_prefix"),
                    );
                }
            };

            let Some(redis_map) = raw_redis_map.as_sequence() else {
                return Some(Err(Error::new(
                    Code::Internal,
                    format!("Non-array from ft_aggregate: {raw_redis_map:?}"),
                )));
            };
            let mut redis_map_iter = redis_map.iter();
            let mut bytes_data: Option<Bytes> = None;
            let mut version: Option<i64> = None;
            loop {
                let Some(key) = redis_map_iter.next() else {
                    break;
                };
                let value = redis_map_iter.next().unwrap();
                let Value::BulkString(k) = key else {
                    return Some(Err(Error::new(
                        Code::Internal,
                        format!("Non-BulkString key from ft_aggregate: {key:?}"),
                    )));
                };
                let Ok(str_key) = str::from_utf8(k) else {
                    return Some(Err(Error::new(
                        Code::Internal,
                        format!("Non-utf8 key from ft_aggregate: {key:?}"),
                    )));
                };
                let Value::BulkString(v) = value else {
                    return Some(Err(Error::new(
                        Code::Internal,
                        format!("Non-BulkString value from ft_aggregate: {key:?}"),
                    )));
                };
                match str_key {
                    DATA_FIELD_NAME => {
                        bytes_data = Some(v.clone().into());
                    }
                    VERSION_FIELD_NAME => {
                        let Ok(str_v) = str::from_utf8(v) else {
                            return Some(Err(Error::new(
                                Code::Internal,
                                format!("Non-utf8 version value from ft_aggregate: {v:?}"),
                            )));
                        };
                        let Ok(raw_version) = str_v.parse::<i64>() else {
                            return Some(Err(Error::new(
                                Code::Internal,
                                format!("Non-integer version value from ft_aggregate: {str_v:?}"),
                            )));
                        };
                        version = Some(raw_version);
                    }
                    other => {
                        if K::MAYBE_SORT_KEY == Some(other) {
                            // ignore sort keys
                        } else {
                            return Some(Err(Error::new(
                                Code::Internal,
                                format!("Extra keys from ft_aggregate: {other}"),
                            )));
                        }
                    }
                }
            }
            let Some(found_bytes_data) = bytes_data else {
                return Some(Err(Error::new(
                    Code::Internal,
                    format!("Missing '{DATA_FIELD_NAME}' in ft_aggregate, got: {raw_redis_map:?}"),
                )));
            };
            Some(
                K::decode(version.unwrap_or(0), found_bytes_data)
                    .err_tip(|| "In RedisStore::search_by_index_prefix::decode"),
            )
        }))
    }

    async fn get_and_decode<K>(
        &self,
        key: K,
    ) -> Result<Option<<K as SchedulerStoreDecodeTo>::DecodeOutput>, Error>
    where
        K: SchedulerStoreKeyProvider + SchedulerStoreDecodeTo + Send,
    {
        let key = key.get_key();
        let key = self.encode_key(&key);
        let mut client = self.get_client().await?;
        let results: Vec<Value> = client
            .connection_manager
            .hmget::<_, Vec<String>, Vec<Value>>(
                key.as_ref(),
                vec![VERSION_FIELD_NAME.into(), DATA_FIELD_NAME.into()],
            )
            .await
            .err_tip(|| format!("In RedisStore::get_without_version::notversioned {key}"))?;
        let Some(Value::BulkString(data)) = results.get(1) else {
            return Ok(None);
        };
        #[allow(clippy::get_first)]
        let version = if let Some(raw_v) = results.get(0) {
            match raw_v {
                Value::Int(v) => *v,
                Value::BulkString(v) => i64::from_str(str::from_utf8(v).expect("utf-8 bulkstring"))
                    .expect("integer bulkstring"),
                Value::Nil => 0,
                _ => {
                    warn!(?raw_v, "Non-integer version!");
                    0
                }
            }
        } else {
            0
        };
        Ok(Some(
            K::decode(version, Bytes::from(data.clone())).err_tip(|| {
                format!("In RedisStore::get_with_version::notversioned::decode {key}")
            })?,
        ))
    }
}
