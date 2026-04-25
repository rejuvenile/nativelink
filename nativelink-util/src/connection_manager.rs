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

use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use futures::Future;
use futures::stream::{FuturesUnordered, StreamExt, unfold};
use nativelink_config::stores::Retry;
use nativelink_error::{Code, Error, make_err};
use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tonic::transport::{Channel, Endpoint, channel};
use tracing::{debug, error, warn};

use crate::background_spawn;
use crate::retry::{self, Retrier, RetryResult};

/// Per-endpoint exponential backoff state for the gap between successive
/// `Retrier` invocations on the same endpoint. The inner `Retrier` already
/// applies its own backoff during a single connection attempt cycle; this
/// state controls the wait *between* outer cycles when an endpoint is fully
/// unreachable (e.g. server is down for restart).
///
/// **Why this exists:** with a flat 1s sleep, a fleet of 10 workers × 32
/// `connections_per_endpoint` would emit ~320 reconnect attempts/sec
/// indefinitely after a server restart, flooding logs and burning CPU on
/// both sides. Exponential backoff caps that at ~1 attempt every 30s per
/// connection (~10/sec across the fleet) once the upper bound is reached,
/// while still recovering quickly (1s) on the first reconnect after a
/// successful link. Jitter (±25%) prevents thundering-herd alignment when
/// many workers retry simultaneously.
#[derive(Debug)]
struct ReconnectBackoff {
    current: Duration,
    max: Duration,
}

impl ReconnectBackoff {
    const INITIAL: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(30);

    const fn new() -> Self {
        Self {
            current: Self::INITIAL,
            max: Self::MAX,
        }
    }

    /// Return the next delay (jittered) and advance the schedule. The
    /// returned value is the *current* base scaled by ±25%, then `current`
    /// doubles for next time (capped at `max`).
    fn next_delay(&mut self) -> Duration {
        let base = self.current;
        let jitter_factor = 0.75 + rand::rng().random::<f64>() * 0.5;
        let jittered = Duration::from_secs_f64(base.as_secs_f64() * jitter_factor);
        self.current = self.current.saturating_mul(2).min(self.max);
        jittered
    }

    /// Reset the schedule back to the initial 1s delay. Called after a
    /// successful connection so the next failure starts fresh rather than
    /// at the saturated 30s cap.
    fn reset(&mut self) {
        self.current = Self::INITIAL;
    }
}

/// A helper utility that enables management of a suite of connections to an
/// upstream gRPC endpoint using Tonic.
#[derive(Debug)]
pub struct ConnectionManager {
    /// Worker request channel.
    worker_tx: mpsc::Sender<(String, oneshot::Sender<Connection>)>,
    /// Side channel for caller-driven `EvictIdle` (cloned from connection_tx).
    connection_tx: mpsc::UnboundedSender<ConnectionRequest>,
}

/// The index into `ConnectionManagerWorker::endpoints`.
type EndpointIndex = usize;
/// The identifier for a given connection to a given Endpoint, used to identify
/// when a particular connection has failed or becomes available.
type ConnectionIndex = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ChannelIdentifier {
    /// The index into `ConnectionManagerWorker::endpoints` that established this
    /// Channel.
    endpoint_index: EndpointIndex,
    /// A unique identifier for this particular connection to the Endpoint.
    connection_index: ConnectionIndex,
}

/// The requests that can be made from a Connection to the
/// `ConnectionManagerWorker` such as informing it that it's been dropped or that
/// an error occurred.
enum ConnectionRequest {
    /// Notify that a Connection was dropped, if it was dropped while the
    /// connection was still pending, then return the pending Channel to be
    /// added back to the available channels.
    Dropped(Option<EstablishedChannel>),
    /// Notify that a Connection was established, return the Channel to the
    /// available channels.
    Connected(EstablishedChannel),
    /// Notify that there was a transport error on the given Channel, the bool
    /// specifies whether the connection was in the process of being established
    /// or not (i.e. whether it's been returned to available channels yet).
    Error((ChannelIdentifier, bool)),
    /// Caller-driven eviction of one idle channel for `endpoint_uri`,
    /// queueing a fresh reconnect. Used by streaming retry paths where
    /// `ResponseFuture::poll` cannot observe the failure (it surfaced
    /// inside the streaming body). `reason` is logged at `warn!`.
    /// `None` endpoint targets any idle channel (legacy / single-pool).
    EvictIdle {
        endpoint_uri: Option<String>,
        reason: String,
    },
}

/// The result of a Future that connects to a given Endpoint.  This is a tuple
/// of the index into the `ConnectionManagerWorker::endpoints` that this
/// connection is for, the iteration of the connection and the result of the
/// connection itself.
type IndexedChannel = Result<EstablishedChannel, (ChannelIdentifier, Error)>;

/// A channel that has been established to an endpoint with some metadata around
/// it to allow identification of the Channel if it errors in order to correctly
/// remove it.
#[derive(Debug, Clone)]
struct EstablishedChannel {
    /// The Channel itself that the meta data relates to.
    channel: Channel,
    /// The identifier of the channel in the worker.
    identifier: ChannelIdentifier,
}

/// The context of the worker used to manage all of the connections.  This
/// handles reconnecting to endpoints on errors and multiple connections to a
/// given endpoint.
struct ConnectionManagerWorker {
    /// The endpoints to establish Channels and the identifier of the last
    /// connection attempt to that endpoint, paired with per-endpoint
    /// reconnect backoff state.
    endpoints: Vec<(ConnectionIndex, Endpoint, ReconnectBackoff)>,
    /// The channel used to communicate between a Connection and the worker.
    connection_tx: mpsc::UnboundedSender<ConnectionRequest>,
    /// The number of connections that are currently allowed to be made.
    available_connections: usize,
    /// Channels that are currently being connected.
    connecting_channels: FuturesUnordered<Pin<Box<dyn Future<Output = IndexedChannel> + Send>>>,
    /// Connected channels that are available for use.
    available_channels: VecDeque<EstablishedChannel>,
    /// Requests for a Channel when available - (reason, request)
    waiting_connections: VecDeque<(String, oneshot::Sender<Connection>)>,
    /// The retry configuration for connecting to an Endpoint, on failure will
    /// restart the retrier after a 1 second delay.
    retrier: Retrier,
    /// Per-endpoint last-eviction timestamp used to dedup `EvictIdle`
    /// requests within `EVICT_DEDUP_WINDOW`. Without this, a GOAWAY storm
    /// produces N concurrent retries each posting EvictIdle, evicting up
    /// to N healthy channels and causing thundering-herd reconnect (#147).
    /// Key is the endpoint URI string from `EvictIdle.endpoint_uri`, or
    /// `""` for "any endpoint" requests.
    last_evict_at: HashMap<String, Instant>,
}

/// Min interval between successive `EvictIdle` actions for the same
/// endpoint key. Smaller than `ReconnectBackoff::INITIAL` (1s) so a
/// genuinely-broken endpoint can be re-evicted after the first reconnect
/// cycle if the new channel also fails, but big enough to absorb the
/// concurrent burst from N parallel retries.
const EVICT_DEDUP_WINDOW: Duration = Duration::from_millis(500);

/// The maximum number of queued requests to obtain a connection from the
/// worker before applying back pressure to the requestor.
const WORKER_BACKLOG: usize = 256;

impl ConnectionManager {
    /// Create a connection manager that creates a balance list between a given
    /// set of Endpoints.  This will restrict the number of concurrent requests
    /// and automatically re-connect upon transport error.
    pub fn new(
        endpoints: impl IntoIterator<Item = Endpoint>,
        mut connections_per_endpoint: usize,
        mut max_concurrent_requests: usize,
        retry: Retry,
        jitter_fn: retry::JitterFn,
    ) -> Self {
        let (worker_tx, worker_rx) = mpsc::channel(WORKER_BACKLOG);
        // The connection messages always come from sync contexts (e.g. drop)
        // and therefore, we'd end up spawning for them if this was bounded
        // which defeats the object since there would be no backpressure
        // applied. Therefore it makes sense for this to be unbounded.
        let (connection_tx, connection_rx) = mpsc::unbounded_channel();
        let endpoints = endpoints
            .into_iter()
            .map(|endpoint| (0, endpoint, ReconnectBackoff::new()))
            .collect();

        if max_concurrent_requests == 0 {
            max_concurrent_requests = usize::MAX;
        }
        if connections_per_endpoint == 0 {
            connections_per_endpoint = 1;
        }
        let evict_tx = connection_tx.clone();
        let worker = ConnectionManagerWorker {
            endpoints,
            available_connections: max_concurrent_requests,
            connection_tx,
            connecting_channels: FuturesUnordered::new(),
            available_channels: VecDeque::new(),
            waiting_connections: VecDeque::new(),
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(tokio::time::sleep(duration))),
                jitter_fn,
                retry,
            ),
            last_evict_at: HashMap::new(),
        };
        background_spawn!("connection_manager_worker_spawn", async move {
            worker
                .service_requests(connections_per_endpoint, worker_rx, connection_rx)
                .await;
        });
        Self { worker_tx, connection_tx: evict_tx }
    }

    /// Get a Connection that can be used as a `tonic::Channel`, except it
    /// performs some additional counting to reconnect on error and restrict
    /// the number of concurrent connections.
    pub async fn connection(&self, reason: String) -> Result<Connection, Error> {
        let (tx, rx) = oneshot::channel();
        self.worker_tx
            .send((reason, tx))
            .await
            .map_err(|err| make_err!(Code::Unavailable, "Requesting a new connection: {err:?}"))?;
        rx.await
            .map_err(|err| make_err!(Code::Unavailable, "Waiting for a new connection: {err:?}"))
    }

    /// Like [`Self::connection`] but fast-fails with `Code::Unavailable` when
    /// the worker doesn't deliver a channel within `timeout`. Use this on call
    /// sites that must not stall indefinitely when the connection pool is
    /// exhausted (e.g. all upstream channels are stuck in long-running RPCs
    /// or in transport reconnect backoff).
    ///
    /// The error message is prefixed with `"ConnectionRefused"` so callers
    /// like `worker_proxy_store::is_definitive_unreachable` can fast-quarantine
    /// the unreachable peer. Do NOT change the prefix without updating those
    /// classifiers.
    pub async fn connection_with_timeout(
        &self,
        reason: String,
        timeout: Duration,
    ) -> Result<Connection, Error> {
        match tokio::time::timeout(timeout, self.connection(reason)).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(make_err!(
                Code::Unavailable,
                "ConnectionRefused: connection acquire timed out after {}ms",
                timeout.as_millis(),
            )),
        }
    }

    /// Evict one idle channel for `endpoint_uri` (or any if `None`) and
    /// queue a reconnect. Used by streaming retry sites where the failure
    /// surfaced inside the response body and `ResponseFuture::poll` could
    /// not catch it — see #147 (production wedge: post-GOAWAY pool kept
    /// reusing dead channel clones with `Code::Internal "Tried to send
    /// while stream is closed"`). Per-endpoint dedup'd via
    /// `EVICT_DEDUP_WINDOW` to absorb concurrent-retry bursts.
    pub fn evict_idle_channel(
        &self,
        endpoint_uri: Option<String>,
        reason: impl Into<String>,
    ) {
        let _ = self
            .connection_tx
            .send(ConnectionRequest::EvictIdle {
                endpoint_uri,
                reason: reason.into(),
            });
    }
}

impl ConnectionManagerWorker {
    async fn service_requests(
        mut self,
        connections_per_endpoint: usize,
        mut worker_rx: mpsc::Receiver<(String, oneshot::Sender<Connection>)>,
        mut connection_rx: mpsc::UnboundedReceiver<ConnectionRequest>,
    ) {
        // Make the initial set of connections, connection failures will be
        // handled in the same way as future transport failures, so no need to
        // do anything special.
        for endpoint_index in 0..self.endpoints.len() {
            for _ in 0..connections_per_endpoint {
                self.connect_endpoint(endpoint_index, None);
            }
        }

        // The main worker loop, when select resolves one of its arms the other
        // ones are cancelled, therefore it's important that they maintain no
        // state while `await`-ing.  This is enforced through the use of
        // non-async functions to do all of the work.
        loop {
            tokio::select! {
                request = worker_rx.recv() => {
                    let Some((reason, request)) = request else {
                        // The ConnectionManager was dropped, shut down the
                        // worker.
                        break;
                    };
                    self.handle_worker(reason, request);
                }
                maybe_request = connection_rx.recv() => {
                    if let Some(request) = maybe_request {
                        self.handle_connection(request);
                    }
                }
                maybe_connection_result = self.connect_next() => {
                    if let Some(connection_result) = maybe_connection_result {
                        self.handle_connected(connection_result);
                    }
                }
            }
        }
    }

    async fn connect_next(&mut self) -> Option<IndexedChannel> {
        if self.connecting_channels.is_empty() {
            // Make this Future never resolve, we will get cancelled by the
            // select if there's some change in state to `self` and can re-enter
            // and evaluate `connecting_channels` again.
            futures::future::pending::<()>().await;
        }
        self.connecting_channels.next().await
    }

    // This must never be made async otherwise the select may cancel it.
    fn handle_connected(&mut self, connection_result: IndexedChannel) {
        match connection_result {
            Ok(established_channel) => {
                // Reset the per-endpoint backoff on success so the next
                // failure starts again at 1s rather than at the saturated
                // 30s cap. Without this, an endpoint that flaps would stay
                // permanently slow to recover.
                if let Some((_, _, backoff)) = self
                    .endpoints
                    .get_mut(established_channel.identifier.endpoint_index)
                {
                    backoff.reset();
                }
                self.available_channels.push_back(established_channel);
                self.maybe_available_connection();
            }
            // When the retrier runs out of attempts start again from the
            // beginning of the retry period.  Never want to be in a
            // situation where we give up on an Endpoint forever.
            Err((identifier, _)) => {
                self.connect_endpoint(identifier.endpoint_index, Some(identifier.connection_index));
            }
        }
    }

    fn connect_endpoint(&mut self, endpoint_index: usize, connection_index: Option<usize>) {
        let Some((current_connection_index, endpoint, backoff)) =
            self.endpoints.get_mut(endpoint_index)
        else {
            // Unknown endpoint, this should never happen.
            error!(?endpoint_index, "Connection to unknown endpoint requested");
            return;
        };
        let is_backoff = connection_index.is_some();
        let connection_index = connection_index.unwrap_or_else(|| {
            *current_connection_index += 1;
            *current_connection_index
        });
        // Compute the inter-cycle backoff delay BEFORE spawning the retry
        // future. We must mutate the per-endpoint state under `&mut self`
        // here; the spawned future only owns the resulting `Duration`.
        // Without exponential backoff, a fleet of N workers × M
        // `connections_per_endpoint` (e.g. 10×32 = 320) generated 320
        // reconnects/sec after a server restart, flooding logs and the
        // network. See `ReconnectBackoff` doc for the rationale.
        let reconnect_delay = is_backoff.then(|| backoff.next_delay());
        if is_backoff {
            warn!(
                ?connection_index,
                endpoint = ?endpoint.uri(),
                ?reconnect_delay,
                "Connection failed, reconnecting"
            );
        } else {
            debug!(
                ?connection_index,
                endpoint = ?endpoint.uri(),
                "Creating new connection"
            );
        }
        let identifier = ChannelIdentifier {
            endpoint_index,
            connection_index,
        };
        let connection_stream = unfold(endpoint.clone(), move |endpoint| async move {
            let result = endpoint.connect().await.map_err(|err| {
                warn!(
                    endpoint = ?endpoint.uri(),
                    error = ?err,
                    "connection attempt failed"
                );
                make_err!(
                    Code::Unavailable,
                    "Failed to connect to {:?}: {err:?}",
                    endpoint.uri()
                )
            });
            Some((
                result.map_or_else(RetryResult::Retry, RetryResult::Ok),
                endpoint,
            ))
        });
        let retrier = self.retrier.clone();
        self.connecting_channels.push(Box::pin(async move {
            if let Some(delay) = reconnect_delay {
                // Sleep before retrying so we aren't in a hard loop and so
                // a fleet of workers doesn't synchronize-and-stampede an
                // endpoint that just came back online (see jitter in
                // `ReconnectBackoff::next_delay`).
                tokio::time::sleep(delay).await;
            }
            retrier.retry(connection_stream).await.map_or_else(
                |err| Err((identifier, err)),
                |channel| {
                    Ok(EstablishedChannel {
                        channel,
                        identifier,
                    })
                },
            )
        }));
    }

    // This must never be made async otherwise the select may cancel it.
    fn handle_worker(&mut self, reason: String, tx: oneshot::Sender<Connection>) {
        if let Some(channel) = (self.available_connections > 0)
            .then_some(())
            .and_then(|()| self.available_channels.pop_front())
        {
            debug!(reason, "ConnectionManager: request running");
            self.provide_channel(channel, tx);
        } else {
            debug!(
                available_connections = self.available_connections,
                available_channels = self.available_channels.len(),
                waiting_connections = self.waiting_connections.len(),
                reason,
                "ConnectionManager: no connection available, request queued",
            );
            self.waiting_connections.push_back((reason, tx));
        }
    }

    fn provide_channel(&mut self, channel: EstablishedChannel, tx: oneshot::Sender<Connection>) {
        // We decrement here because we create Connection, this will signal when
        // it is Dropped and therefore increment this again.
        self.available_connections -= 1;
        drop(tx.send(Connection {
            tx: self.connection_tx.clone(),
            pending_channel: Some(channel.channel.clone()),
            channel,
        }));
    }

    fn maybe_available_connection(&mut self) {
        while self.available_connections > 0
            && !self.waiting_connections.is_empty()
            && !self.available_channels.is_empty()
        {
            if let Some(channel) = self.available_channels.pop_front() {
                if let Some((reason, tx)) = self.waiting_connections.pop_front() {
                    debug!(reason, "ConnectionManager: channel available, running");
                    self.provide_channel(channel, tx);
                } else {
                    // This should never happen, but better than an unwrap.
                    self.available_channels.push_front(channel);
                }
            }
        }
    }

    // This must never be made async otherwise the select may cancel it.
    fn handle_connection(&mut self, request: ConnectionRequest) {
        match request {
            ConnectionRequest::Dropped(maybe_channel) => {
                if let Some(channel) = maybe_channel {
                    self.available_channels.push_back(channel);
                }
                self.available_connections += 1;
                self.maybe_available_connection();
            }
            ConnectionRequest::Connected(channel) => {
                self.available_channels.push_back(channel);
                self.maybe_available_connection();
            }
            // Handle a transport error on a connection by making it unavailable
            // for use and establishing a new connection to the endpoint.
            ConnectionRequest::Error((identifier, was_pending)) => {
                let should_reconnect = if was_pending {
                    true
                } else {
                    let original_length = self.available_channels.len();
                    self.available_channels
                        .retain(|channel| channel.identifier != identifier);
                    // Only reconnect if it wasn't already disconnected.
                    original_length != self.available_channels.len()
                };
                if should_reconnect {
                    self.connect_endpoint(identifier.endpoint_index, None);
                }
            }
            // See `ConnectionManager::evict_idle_channel` doc.
            ConnectionRequest::EvictIdle { endpoint_uri, reason } => {
                let dedup_key = endpoint_uri.clone().unwrap_or_default();
                let now = Instant::now();
                if let Some(prev) = self.last_evict_at.get(&dedup_key)
                    && now.duration_since(*prev) < EVICT_DEDUP_WINDOW
                {
                    debug!(
                        %reason,
                        ?endpoint_uri,
                        "ConnectionManager: EvictIdle deduped within window"
                    );
                    return;
                }
                // Find a victim: prefer one matching the requested endpoint,
                // else fall back to any idle channel for legacy callers.
                let victim_pos = endpoint_uri.as_deref().and_then(|uri| {
                    self.available_channels.iter().position(|c| {
                        self.endpoints
                            .get(c.identifier.endpoint_index)
                            .is_some_and(|(_, ep, _)| ep.uri().to_string() == uri)
                    })
                });
                let victim = match victim_pos {
                    Some(pos) => self.available_channels.remove(pos),
                    None if endpoint_uri.is_none() => self.available_channels.pop_front(),
                    None => None, // Endpoint specified but no matching idle channel.
                };
                if let Some(victim) = victim {
                    let endpoint_index = victim.identifier.endpoint_index;
                    let connection_index = victim.identifier.connection_index;
                    drop(victim);
                    self.last_evict_at.insert(dedup_key, now);
                    warn!(
                        %reason,
                        ?endpoint_index,
                        ?connection_index,
                        ?endpoint_uri,
                        "ConnectionManager: evicting idle channel (#147)"
                    );
                    self.connect_endpoint(endpoint_index, Some(connection_index));
                } else {
                    debug!(
                        %reason,
                        ?endpoint_uri,
                        "ConnectionManager: EvictIdle requested but no matching idle channel"
                    );
                }
            }
        }
    }
}

/// An instance of this is obtained for every communication with the gGRPC
/// service.  This handles the permit for limiting concurrency, and also
/// re-connecting the underlying channel on error.  It depends on users
/// reporting all errors.
/// NOTE: This should never be cloneable because its lifetime is linked to the
///       `ConnectionManagerWorker::available_connections`.
#[derive(Debug)]
pub struct Connection {
    /// Communication with `ConnectionManagerWorker` to inform about transport
    /// errors and when the Connection is dropped.
    tx: mpsc::UnboundedSender<ConnectionRequest>,
    /// If set, the Channel that will be returned to the worker when connection
    /// completes (success or failure) or when the Connection is dropped if that
    /// happens before connection completes.
    pending_channel: Option<Channel>,
    /// The identifier to send to `tx`.
    channel: EstablishedChannel,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let pending_channel = self
            .pending_channel
            .take()
            .map(|channel| EstablishedChannel {
                channel,
                identifier: self.channel.identifier,
            });
        drop(self.tx.send(ConnectionRequest::Dropped(pending_channel)));
    }
}

/// A wrapper around the `channel::ResponseFuture` that forwards errors to the `tx`.
pub struct ResponseFuture {
    /// The wrapped future that actually does the work.
    inner: channel::ResponseFuture,
    /// Communication with `ConnectionManagerWorker` to inform about transport
    /// errors.
    connection_tx: mpsc::UnboundedSender<ConnectionRequest>,
    /// The identifier to send to `connection_tx` on a transport error.
    identifier: ChannelIdentifier,
}

impl core::fmt::Debug for ResponseFuture {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ResponseFuture")
            .field("inner", &self.inner)
            .field("connection_tx", &self.connection_tx)
            .field("identifier", &self.identifier)
            .finish()
    }
}

/// This is mostly copied from `tonic::transport::channel` except it wraps it
/// to allow messaging about connection success and failure.
impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for Connection {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = tonic::transport::Error;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.channel.channel.poll_ready(cx);
        if let Poll::Ready(result) = &result {
            match result {
                Ok(()) => {
                    if let Some(pending_channel) = self.pending_channel.take() {
                        drop(
                            self.tx
                                .send(ConnectionRequest::Connected(EstablishedChannel {
                                    channel: pending_channel,
                                    identifier: self.channel.identifier,
                                })),
                        );
                    }
                }
                Err(err) => {
                    debug!(?err, "Error while creating connection on channel");
                    drop(self.tx.send(ConnectionRequest::Error((
                        self.channel.identifier,
                        self.pending_channel.take().is_some(),
                    ))));
                }
            }
        }
        result
    }

    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        ResponseFuture {
            inner: self.channel.channel.call(request),
            connection_tx: self.tx.clone(),
            identifier: self.channel.identifier,
        }
    }
}

/// This is mostly copied from `tonic::transport::channel` except it wraps it
/// to allow messaging about connection failure.
impl Future for ResponseFuture {
    type Output =
        Result<tonic::codegen::http::Response<tonic::body::Body>, tonic::transport::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.inner).poll(cx);
        if let Poll::Ready(Err(_)) = &result {
            drop(
                self.connection_tx
                    .send(ConnectionRequest::Error((self.identifier, false))),
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;
    use std::sync::Arc;

    use nativelink_config::stores::Retry;
    use nativelink_error::Code;
    use tonic::transport::Endpoint;

    use super::{ConnectionManager, EVICT_DEDUP_WINDOW, ReconnectBackoff};

    /// Assert `actual` falls within `±25%` of `expected_base`. The jittered
    /// delay is `base * uniform(0.75, 1.25)`, so the bounds are [0.75x, 1.25x].
    #[track_caller]
    fn assert_within_jitter(actual: Duration, expected_base: Duration) {
        let lower = expected_base.mul_f64(0.75);
        let upper = expected_base.mul_f64(1.25);
        assert!(
            actual >= lower && actual <= upper,
            "delay {actual:?} not within jitter window [{lower:?}, {upper:?}] of base {expected_base:?}"
        );
    }

    /// `ReconnectBackoff` doubles the base from 1s through 16s, saturates
    /// at 30s, and stays at 30s thereafter. Each returned delay is jittered
    /// ±25% of the *current base*.
    #[test]
    fn reconnect_backoff_doubles_until_saturation() {
        let mut b = ReconnectBackoff::new();
        // Bases: 1, 2, 4, 8, 16. Next would be 32 → clamped to 30.
        for base_secs in [1u64, 2, 4, 8, 16] {
            assert_within_jitter(b.next_delay(), Duration::from_secs(base_secs));
        }
        // Saturated at 30s for all subsequent calls.
        for _ in 0..5 {
            assert_within_jitter(b.next_delay(), Duration::from_secs(30));
        }
    }

    /// After `reset()`, the next delay is back near 1s regardless of how
    /// far the schedule had advanced. This is the SUCCESS path: the next
    /// failure should not pick up where the saturated schedule left off.
    #[test]
    fn reconnect_backoff_reset_returns_to_initial() {
        let mut b = ReconnectBackoff::new();
        // Advance well past the cap.
        for _ in 0..20 {
            let _ = b.next_delay();
        }
        b.reset();
        assert_within_jitter(b.next_delay(), Duration::from_secs(1));
        // And it climbs again from 1s.
        assert_within_jitter(b.next_delay(), Duration::from_secs(2));
    }

    /// When the pool has no available channels (here: a single endpoint that
    /// will never connect within the test window), `connection_with_timeout`
    /// must fast-fail with `Code::Unavailable` and a "ConnectionRefused"
    /// prefix that downstream classifiers (e.g.
    /// `worker_proxy_store::is_definitive_unreachable`) match on.
    #[tokio::test]
    async fn connection_with_timeout_returns_unavailable_when_pool_drained() {
        // RFC 5737 TEST-NET-1 (192.0.2.0/24): unroutable, so the background
        // connect attempt cannot succeed within the test's 100ms window —
        // the OS connect either hangs or returns EHOSTUNREACH and the
        // retrier sleeps 1s before retrying. Either way no channel is
        // delivered; the request stays queued in `waiting_connections`
        // and our `connection_with_timeout` timer fires.
        let endpoint = Endpoint::from_static("http://192.0.2.1:1");
        let cm = ConnectionManager::new(
            std::iter::once(endpoint),
            /* connections_per_endpoint */ 1,
            /* max_concurrent_requests */ 1,
            Retry::default(),
            Arc::new(|d| d),
        );

        let err = cm
            .connection_with_timeout(
                "test".to_string(),
                core::time::Duration::from_millis(100),
            )
            .await
            .expect_err("expected timeout error when no channel is available");

        assert_eq!(err.code, Code::Unavailable, "wrong code: {err:?}");
        assert!(
            err.messages
                .iter()
                .any(|m| m.starts_with("ConnectionRefused")),
            "expected message to start with 'ConnectionRefused', got: {:?}",
            err.messages
        );
    }

    /// #147 regression: after a pooled `Channel` becomes stale (h2
    /// GOAWAY post-`too_many_internal_resets`), `evict_idle_channel`
    /// removes one currently-idle channel from the pool and queues a
    /// reconnect for that endpoint slot. Verified end-to-end against a
    /// real local TCP server: count the distinct TCP accepts on the
    /// listener and assert that one extra `accept()` happens after the
    /// eviction call.
    ///
    /// **Why count TCP accepts and not channel identifiers:** the
    /// `EstablishedChannel` identifier is private and the `Connection`
    /// itself doesn't expose it. The observable side-effect of
    /// `evict_idle_channel` is "kill the underlying tonic Channel and
    /// replace it" — and replacing involves a fresh `Endpoint::connect`,
    /// which translates to a new TCP `accept()` on the server side.
    #[tokio::test]
    async fn evict_idle_channel_triggers_reconnect_against_real_endpoint() {
        // Bind an ephemeral TCP listener and count accepts. We don't run
        // a real h2/grpc handshake — `Endpoint::connect` is lazy in
        // tonic 0.13 (the channel is created but the underlying TCP
        // dial happens on first request), so for our purpose we want a
        // listener that accepts and immediately closes the socket. The
        // ConnectionManager's `Endpoint::connect().await` will succeed
        // (the TCP handshake completes) and then sit idle in the pool
        // until we either issue a request or evict.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        let accept_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accept_count_for_task = Arc::clone(&accept_count);
        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        accept_count_for_task
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Hold the connection open briefly so the client's
                        // h2 handshake can begin / fail, then drop it.
                        // We don't speak h2 — that's fine; the client's
                        // pool just needs the TCP connect to complete to
                        // populate `available_channels`. Subsequent
                        // request attempts will fail at h2 layer, but
                        // this test only cares about the reconnect count.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        drop(stream);
                    }
                    Err(_) => break,
                }
            }
        });

        let endpoint =
            Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
                .expect("valid endpoint")
                .connect_timeout(Duration::from_secs(2));
        // Single connection so we can pin down "before vs after" accept count.
        let cm = ConnectionManager::new(
            std::iter::once(endpoint),
            /* connections_per_endpoint */ 1,
            /* max_concurrent_requests */ 1,
            Retry {
                max_retries: 0,
                delay: 0.0,
                jitter: 0.0,
                ..Default::default()
            },
            Arc::new(|d| d),
        );

        // Wait for the initial connection to land in the pool. We do
        // this by polling `accept_count` until it advances past the
        // initial value, with a hard cap so the test can't hang.
        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if accept_count.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                break;
            }
            if tokio::time::Instant::now() > initial_deadline {
                panic!(
                    "initial TCP accept did not happen within 3s — \
                     ConnectionManager failed to dial the test listener"
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let baseline_accepts =
            accept_count.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            baseline_accepts >= 1,
            "expected at least 1 initial accept, got {baseline_accepts}"
        );

        // Trigger eviction. This MUST cause exactly one fresh TCP dial
        // (after the per-endpoint backoff sleep, jittered around 1s
        // for the first reconnect cycle).
        cm.evict_idle_channel(None, "test: simulate stale channel after GOAWAY");

        // Wait for the reconnect TCP dial. With a 1s base backoff +
        // 25% jitter, the dial happens between ~0.75s and ~1.25s after
        // the eviction call. Allow a generous 4s window for CI.
        let reconnect_deadline =
            tokio::time::Instant::now() + Duration::from_secs(4);
        loop {
            let now = accept_count.load(std::sync::atomic::Ordering::SeqCst);
            if now > baseline_accepts {
                break;
            }
            if tokio::time::Instant::now() > reconnect_deadline {
                panic!(
                    "expected a fresh TCP accept after evict_idle_channel; \
                     accept_count is still {now} (baseline {baseline_accepts}). \
                     Without the fix from #147 (idle channel never evicted, \
                     reconnect never queued), accept_count would stay at \
                     baseline forever."
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        accept_task.abort();
    }

    /// Helper: bind a local TCP listener and return (port, accept-count
    /// atomic, accept task handle). Each TCP `accept()` increments the
    /// counter; the test then watches the counter to observe reconnects.
    async fn bind_counting_listener() -> (
        u16,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_for_task = Arc::clone(&counter);
        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        counter_for_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        drop(stream);
                    }
                    Err(_) => break,
                }
            }
        });
        (port, counter, task)
    }

    /// #147: when N callers concurrently request `evict_idle_channel`
    /// for the same endpoint within `EVICT_DEDUP_WINDOW`, only ONE
    /// eviction happens — the remaining N-1 are absorbed by the dedup
    /// gate. Use a multi-connection pool (4) so the dedup gate can be
    /// validated independently of "no more idle channels to evict";
    /// without dedup, the first 4 storm requests would each pop a
    /// healthy channel and queue 4 reconnects.
    #[tokio::test]
    async fn evict_idle_storm_dedups_to_single_eviction() {
        let (port, accept_count, accept_task) = bind_counting_listener().await;
        let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .expect("valid endpoint")
            .connect_timeout(Duration::from_secs(2));
        let cm = ConnectionManager::new(
            std::iter::once(endpoint),
            /* connections_per_endpoint */ 4,
            /* max_concurrent_requests */ 4,
            Retry { max_retries: 0, delay: 0.0, jitter: 0.0, ..Default::default() },
            Arc::new(|d| d),
        );

        // Wait for ALL 4 initial dials.
        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if accept_count.load(std::sync::atomic::Ordering::SeqCst) >= 4 {
                break;
            }
            if tokio::time::Instant::now() > initial_deadline {
                panic!(
                    "expected 4 initial accepts in 5s, got {}",
                    accept_count.load(std::sync::atomic::Ordering::SeqCst)
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let baseline = accept_count.load(std::sync::atomic::Ordering::SeqCst);

        // 32 concurrent EvictIdle requests within the dedup window.
        for i in 0..32 {
            cm.evict_idle_channel(None, format!("storm-{i}"));
        }

        // Wait for the SINGLE reconnect to land (~1s ± jitter).
        let reconnect_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        loop {
            let now = accept_count.load(std::sync::atomic::Ordering::SeqCst);
            if now > baseline {
                break;
            }
            if tokio::time::Instant::now() > reconnect_deadline {
                panic!("no reconnect observed after evict storm; baseline={baseline}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wait 1s past the dedup window to make sure no stragglers evict.
        tokio::time::sleep(Duration::from_millis(500) + EVICT_DEDUP_WINDOW).await;
        let final_count = accept_count.load(std::sync::atomic::Ordering::SeqCst);
        let extra = final_count - baseline;
        assert_eq!(
            extra, 1,
            "expected exactly 1 reconnect from 32-way evict storm \
             (dedup window {EVICT_DEDUP_WINDOW:?}, pool size 4); got {extra} \
             additional accepts. Without dedup the first 4 storm requests \
             would each pop a channel and queue 4 reconnects",
        );

        accept_task.abort();
    }

    /// #147: per-endpoint targeting. With a 2-endpoint pool, an EvictIdle
    /// request specifying endpoint A's URI must NOT evict endpoint B's
    /// idle channel. Without per-endpoint filtering, `pop_front` could
    /// pick either, evicting a healthy channel on the wrong endpoint.
    #[tokio::test]
    async fn evict_idle_targets_specified_endpoint_only() {
        let (port_a, accepts_a, task_a) = bind_counting_listener().await;
        let (port_b, accepts_b, task_b) = bind_counting_listener().await;
        let ep_a = Endpoint::from_shared(format!("http://127.0.0.1:{port_a}"))
            .expect("ep a")
            .connect_timeout(Duration::from_secs(2));
        let ep_b = Endpoint::from_shared(format!("http://127.0.0.1:{port_b}"))
            .expect("ep b")
            .connect_timeout(Duration::from_secs(2));
        let uri_b = ep_b.uri().to_string();
        let cm = ConnectionManager::new(
            vec![ep_a, ep_b],
            /* connections_per_endpoint */ 1,
            /* max_concurrent_requests */ 2,
            Retry { max_retries: 0, delay: 0.0, jitter: 0.0, ..Default::default() },
            Arc::new(|d| d),
        );

        // Wait for both initial dials.
        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let a = accepts_a.load(std::sync::atomic::Ordering::SeqCst);
            let b = accepts_b.load(std::sync::atomic::Ordering::SeqCst);
            if a >= 1 && b >= 1 {
                break;
            }
            if tokio::time::Instant::now() > initial_deadline {
                panic!("initial dials did not complete in 3s (a={a}, b={b})");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let base_a = accepts_a.load(std::sync::atomic::Ordering::SeqCst);
        let base_b = accepts_b.load(std::sync::atomic::Ordering::SeqCst);

        // Evict ONLY endpoint B.
        cm.evict_idle_channel(Some(uri_b), "test: evict B only");

        // Wait for a reconnect on B.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        loop {
            let b = accepts_b.load(std::sync::atomic::Ordering::SeqCst);
            if b > base_b {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "endpoint B did not reconnect after targeted eviction; \
                     accepts_b={b} (baseline={base_b})"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Endpoint A must NOT have been touched.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let a_after = accepts_a.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            a_after, base_a,
            "endpoint A's accept count changed (base={base_a}, now={a_after}); \
             targeted eviction leaked across endpoints"
        );

        task_a.abort();
        task_b.abort();
    }
}
