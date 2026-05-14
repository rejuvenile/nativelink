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

use core::hash::BuildHasher;
use core::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

pub use nativelink_metric_macro_derive::MetricsComponent;
pub use tracing::{
    error as __metric_error, info as __metric_info, info_span as __metric_info_span,
};

/// Error type for the metrics library.
// Note: We do not use the nativelink-error struct because
// we'd end up in a circular dependency if we did, because
// nativelink-error uses the metrics library.
#[derive(Debug)]
pub struct Error(String);

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl core::error::Error for Error {}

/// Holds metadata about the field that is being published.
#[derive(Debug, Default, Clone)]
pub struct MetricFieldData<'a> {
    pub name: Cow<'a, str>,
    pub help: Cow<'a, str>,
    pub group: Cow<'a, str>,
}

/// The final primitive data that is being published with the kind.
#[derive(Debug)]
pub enum MetricPublishKnownKindData {
    Counter(u64),
    String(String),
    Component,
}

/// The kind of metric that is being published.
// Note: This enum will be translate in-and-out
// of a u64 when traversing the `tracing::event`
// boundary for efficiency reasons.
#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum MetricKind {
    Default = 0,
    Counter = 1,
    String = 2,
    Component = 3,
}

impl From<u64> for MetricKind {
    fn from(value: u64) -> Self {
        match value {
            0 | 4_u64..=u64::MAX => Self::Default,
            1 => Self::Counter,
            2 => Self::String,
            3 => Self::Component,
        }
    }
}

impl MetricKind {
    #[must_use]
    pub fn into_known_kind(&self, default_kind: Self) -> MetricPublishKnownKindData {
        let this = if matches!(self, Self::Default) {
            default_kind
        } else {
            *self
        };
        match this {
            Self::Counter => MetricPublishKnownKindData::Counter(0),
            Self::String => MetricPublishKnownKindData::String(String::new()),
            Self::Component => MetricPublishKnownKindData::Component,
            Self::Default => unreachable!("Default should have been handled"),
        }
    }
}

/// The trait that all components that can be published must implement.
pub trait MetricsComponent {
    /// # Errors
    ///
    /// Will return `Err` if we can't publish the metric.
    fn publish(
        &self,
        kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error>;
}

pub trait RootMetricsComponent: MetricsComponent + Send + Sync {
    /// # Errors
    ///
    /// Will return `Err` if we can't publish the metric.
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        MetricsComponent::publish(self, kind, field_metadata)
    }
}

impl<T: MetricsComponent> MetricsComponent for Option<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        self.as_ref()
            .map_or(Ok(MetricPublishKnownKindData::Component), |value| {
                value.publish(kind, field_metadata)
            })
    }
}

impl<T: MetricsComponent> MetricsComponent for tokio::sync::watch::Sender<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        self.borrow().publish(kind, field_metadata)
    }
}

impl<T: MetricsComponent + ?Sized> MetricsComponent for Arc<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        self.as_ref().publish(kind, field_metadata)
    }
}

impl<T: MetricsComponent, S: BuildHasher> MetricsComponent for HashSet<T, S> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        for (i, item) in self.iter().enumerate() {
            let guard = group!(i).entered();
            let publish_result = item.publish(kind, field_metadata.clone())?;
            drop(guard);
            match publish_result {
                MetricPublishKnownKindData::Counter(value) => {
                    publish!(
                        i,
                        &value,
                        MetricKind::Counter,
                        field_metadata.help.to_string()
                    );
                }
                MetricPublishKnownKindData::String(value) => {
                    publish!(
                        i,
                        &value,
                        MetricKind::String,
                        field_metadata.help.to_string()
                    );
                }
                MetricPublishKnownKindData::Component => {}
            }
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl<U: ToString, T: MetricsComponent, S: BuildHasher> MetricsComponent for HashMap<U, T, S> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        for (key, item) in self {
            let guard = group!(key).entered();
            let publish_result = item.publish(kind, field_metadata.clone())?;
            drop(guard);
            match publish_result {
                MetricPublishKnownKindData::Counter(value) => {
                    publish!(
                        key,
                        &value,
                        MetricKind::Counter,
                        field_metadata.help.to_string()
                    );
                }
                MetricPublishKnownKindData::String(value) => {
                    publish!(
                        key,
                        &value,
                        MetricKind::String,
                        field_metadata.help.to_string()
                    );
                }
                MetricPublishKnownKindData::Component => {}
            }
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl<U: ToString, T: MetricsComponent> MetricsComponent for BTreeMap<U, T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        for (key, item) in self {
            group!(key).in_scope(|| item.publish(kind, field_metadata.clone()))?;
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl<T: MetricsComponent> MetricsComponent for BTreeSet<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        for (i, item) in self.iter().enumerate() {
            group!(i).in_scope(|| item.publish(kind, field_metadata.clone()))?;
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl<T: MetricsComponent> MetricsComponent for Vec<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        for (i, item) in self.iter().enumerate() {
            group!(i).in_scope(|| item.publish(kind, field_metadata.clone()))?;
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl<T: MetricsComponent + ?Sized> MetricsComponent for Weak<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let Some(this) = self.upgrade() else {
            return Ok(MetricPublishKnownKindData::Component);
        };
        this.as_ref().publish(kind, field_metadata)
    }
}

impl<T, E> MetricsComponent for Result<T, E>
where
    T: MetricsComponent,
    E: MetricsComponent,
{
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        match self {
            Ok(value) => value.publish(kind, field_metadata),
            Err(value) => value.publish(kind, field_metadata),
        }
    }
}

impl MetricsComponent for Duration {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        self.as_secs_f64().publish(kind, field_metadata)
    }
}

impl MetricsComponent for SystemTime {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        Self::now().duration_since(UNIX_EPOCH).map_or_else(
            |_| Err(Error("SystemTime before UNIX EPOCH!".to_string())),
            |n| n.as_secs().publish(kind, field_metadata),
        )
    }
}

impl MetricsComponent for f64 {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        Ok(MetricPublishKnownKindData::String(self.to_string()))
    }
}

impl MetricsComponent for bool {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let value = u64::from(*self);
        value.publish(kind, field_metadata)
    }
}

impl MetricsComponent for i32 {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let value = u64::try_from(*self)
            .map_err(|_| Error(format!("Could not convert {self} to u64 in metrics lib")))?;
        value.publish(kind, field_metadata)
    }
}

impl MetricsComponent for u64 {
    fn publish(
        &self,
        kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let mut known_kind_data = kind.into_known_kind(MetricKind::Counter);
        match &mut known_kind_data {
            MetricPublishKnownKindData::Counter(data) => {
                *data = *self;
            }
            MetricPublishKnownKindData::String(data) => {
                *data = self.to_string();
            }
            MetricPublishKnownKindData::Component => {}
        }
        Ok(known_kind_data)
    }
}

impl MetricsComponent for i64 {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let value = u64::try_from(*self)
            .map_err(|_| Error(format!("Could not convert {self} to u64 in metrics lib")))?;
        value.publish(kind, field_metadata)
    }
}

impl MetricsComponent for u32 {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        u64::from(*self).publish(kind, field_metadata)
    }
}

impl MetricsComponent for usize {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let value = u64::try_from(*self)
            .map_err(|_| Error(format!("Could not convert {self} to u64 in metrics lib")))?;
        value.publish(kind, field_metadata)
    }
}

impl MetricsComponent for AtomicU64 {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        self.load(Ordering::Acquire).publish(kind, field_metadata)
    }
}

impl MetricsComponent for AtomicI64 {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        self.load(Ordering::Acquire).publish(kind, field_metadata)
    }
}

impl MetricsComponent for String {
    fn publish(
        &self,
        kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let mut known_kind_data = kind.into_known_kind(MetricKind::String);
        match &mut known_kind_data {
            MetricPublishKnownKindData::Counter(data) => {
                *data = self.parse::<u64>().map_err(|_| {
                    Error(format!(
                        "Could not convert String '{self}' to u64 in metrics lib"
                    ))
                })?;
            }
            MetricPublishKnownKindData::String(data) => {
                data.clone_from(self);
            }
            MetricPublishKnownKindData::Component => {}
        }
        Ok(known_kind_data)
    }
}

// CLAUDE.md "NEVER block a tokio worker thread" hard rule:
// `MetricsComponent::publish` is a sync trait method but is invoked
// from `metrics_handler` (`nativelink-util/src/metrics_publisher.rs`),
// an `async fn` running on a tokio runtime worker thread. The
// historical implementations of these blanket impls called
// `*_blocking()` / `lock()` / `read()`, which park the calling thread
// until any active writer releases. For `async_lock::{Mutex, RwLock}`
// this parks via `event_listener`'s FIFO queue — the parked tokio
// worker is removed from the runtime indefinitely; the queue can only
// drain after the writer releases, by which time the worker is lost
// for the lifetime of any subsequent reader queued behind it.
//
// Production wedge 2026-05-13 (PID 3818654): every `/metrics` scrape
// that raced an `ApiWorkerSchedulerImpl` writer permanently parked
// one of ~48 tokio worker threads. Accumulated leak wedged the runtime
// over ~3h, eventually starving the Bazel-port accept loop. RCA:
// `.claude/audits/wedge-2026-05-13-postmortem.md`.
//
// Fix: skip-on-contention. `try_*()` returns immediately without
// queuing on the FIFO listener, so the calling tokio worker is never
// parked. A scrape that races a writer publishes nothing for the
// contended subtree on this tick; the next scrape that catches the
// lock idle publishes fresh values. Stale-by-one-scrape metrics for
// observability >> permanently-parked worker thread for liveness.
//
// Same pattern applied to `parking_lot` variants: even though those
// don't use an event_listener queue, their `lock()`/`read()` still
// blocks the calling thread synchronously when a writer is active,
// which on a tokio worker means a parked worker. Same fix.
//
// `tokio::sync::watch::Sender::borrow` (above) is left as-is because
// the `Sender` is the producer end of a watch channel, so the
// metrics-side caller holding `&self` IS the writer; structural
// contention against it is not possible.
impl<T: MetricsComponent> MetricsComponent for async_lock::Mutex<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let Some(lock) = self.try_lock() else {
            return Ok(MetricPublishKnownKindData::Component);
        };
        lock.publish(kind, field_metadata)
    }
}

impl<T: MetricsComponent> MetricsComponent for async_lock::RwLock<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let Some(lock) = self.try_read() else {
            return Ok(MetricPublishKnownKindData::Component);
        };
        lock.publish(kind, field_metadata)
    }
}

impl<T: MetricsComponent> MetricsComponent for parking_lot::Mutex<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let Some(lock) = self.try_lock() else {
            return Ok(MetricPublishKnownKindData::Component);
        };
        lock.publish(kind, field_metadata)
    }
}

impl<T: MetricsComponent> MetricsComponent for parking_lot::RwLock<T> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, Error> {
        let Some(lock) = self.try_read() else {
            return Ok(MetricPublishKnownKindData::Component);
        };
        lock.publish(kind, field_metadata)
    }
}

#[macro_export]
macro_rules! group {
    ($name:expr) => {
        $crate::__metric_info_span!(target: "nativelink_metric", "", __name = $name.to_string())
    };
}

#[macro_export]
macro_rules! publish {
    ($name:expr, $value:expr, $metric_kind:expr, $help:expr) => {
        $crate::publish!($name, $value, $metric_kind, $help, "")
    };
    ($name:expr, $value:expr, $metric_kind:expr, $help:expr, $group:expr) => {
        {
            let _maybe_entered = if !$group.is_empty() {
                Some($crate::group!($group).entered())
            } else {
                None
            };
            let name = $name.to_string();
            let field_metadata = $crate::MetricFieldData {
                name: ::std::borrow::Cow::Borrowed(&name),
                help: $help.into(),
                group: $group.into(),
            };
            match $crate::MetricsComponent::publish($value, $metric_kind, field_metadata)? {
                $crate::MetricPublishKnownKindData::Counter(value) => {
                    $crate::__metric_info!(
                        target: "nativelink_metric",
                        __value = value,
                        __type = $crate::MetricKind::Counter as u8,
                        __help = $help.to_string(),
                        __name = name
                    );
                }
                $crate::MetricPublishKnownKindData::String(value) => {
                    $crate::__metric_info!(
                        target: "nativelink_metric",
                        __value = value,
                        __type = $crate::MetricKind::String as u8,
                        __help = $help.to_string(),
                        __name = name
                    );
                }
                $crate::MetricPublishKnownKindData::Component => {
                    // Do nothing, data already published.
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    //! Regression tests for the lock-blocking-on-tokio-worker class of
    //! bug that wedged production 2026-05-13 (RCA at
    //! `.claude/audits/wedge-2026-05-13-postmortem.md`).
    //!
    //! Each test reproduces the production seam: a long-held writer on
    //! a lock + a metrics-publish path running on a tokio worker
    //! thread. Pre-fix, `publish` parks the worker indefinitely on the
    //! event_listener / parking_lot wait queue; post-fix it returns a
    //! default `Component` immediately.
    //!
    //! Tests run on a `flavor = "multi_thread", worker_threads = 2`
    //! runtime so we can hold a writer on one worker while we drive
    //! `publish` on another. A `current_thread` runtime would
    //! self-deadlock at the writer hand-off, masking the contention
    //! shape we want to assert.
    //!
    //! Mutation procedure (per CLAUDE.md TDD): revert the corresponding
    //! `try_*()` skip-on-contention back to `*_blocking()` /
    //! `lock()` / `read()` and the test must red-fail with
    //! `"publish() blocked tokio worker thread on contended <lock>"`.
    //! Without the deadline assertion, the failure mode would be a
    //! 60s test-timeout — which `is_err()` masks.
    use core::time::Duration;
    use std::sync::Arc;

    use super::{
        Error, MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    };

    /// Counter-shaped publishable used to detect whether `publish`
    /// actually obtained the lock (live read) or returned the
    /// skip-on-contention default (Component).
    struct Counter(u64);

    impl MetricsComponent for Counter {
        fn publish(
            &self,
            kind: MetricKind,
            field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, Error> {
            self.0.publish(kind, field_metadata)
        }
    }

    fn empty_field_metadata() -> MetricFieldData<'static> {
        MetricFieldData::default()
    }

    /// Asserts `publish` returns within `deadline` when invoked from
    /// a tokio runtime task. Reproduces the production seam:
    /// `metrics_handler` is `async fn` (`metrics_publisher.rs:499`)
    /// scheduled on a runtime worker; pre-fix, calling `publish` on
    /// a contended `async_lock::RwLock` parked that worker via the
    /// `event_listener` FIFO queue — the worker was leaked from the
    /// runtime indefinitely.
    ///
    /// Crucially, this uses `tokio::spawn` (NOT `spawn_blocking`).
    /// `spawn_blocking` is the *fix* that should be applied to
    /// `metrics_handler`; using it here would mask the regression.
    /// Spawning a regular task forces the publish onto a runtime
    /// worker, exactly as production does today.
    async fn assert_publish_does_not_block<F>(deadline: Duration, label: &str, f: F)
    where
        F: FnOnce() -> Result<MetricPublishKnownKindData, Error> + Send + 'static,
    {
        // Wrap the sync call in an async block so it polls inside a
        // runtime task. The `f()` call itself is sync; whichever
        // worker thread polls the task IS the worker we want to
        // protect from parking.
        let handle = tokio::spawn(async move { f() });
        match tokio::time::timeout(deadline, handle).await {
            Ok(Ok(_)) => {
                // Task finished before the deadline — publish did not
                // park the worker indefinitely.
            }
            Ok(Err(join_err)) => panic!(
                "publish() spawned task panicked on contended {label}: {join_err:?}"
            ),
            Err(_elapsed) => panic!(
                "publish() blocked tokio worker thread on contended {label} \
                 (exceeded {deadline:?} deadline; pre-fix would park indefinitely)"
            ),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_does_not_block_on_contended_async_lock_rwlock() {
        let lock = Arc::new(async_lock::RwLock::new(Counter(7)));
        let writer_started = Arc::new(tokio::sync::Notify::new());
        let writer_release = Arc::new(tokio::sync::Notify::new());

        let writer_lock = Arc::clone(&lock);
        let writer_started_for_task = Arc::clone(&writer_started);
        let writer_release_for_task = Arc::clone(&writer_release);
        let writer = tokio::spawn(async move {
            let _guard = writer_lock.write().await;
            writer_started_for_task.notify_one();
            writer_release_for_task.notified().await;
        });

        // Wait for the writer to actually hold the guard before
        // calling publish — otherwise the test races the spawn.
        writer_started.notified().await;

        let publish_lock = Arc::clone(&lock);
        assert_publish_does_not_block(
            Duration::from_secs(1),
            "async_lock::RwLock",
            move || {
                publish_lock.publish(MetricKind::Component, empty_field_metadata())
            },
        )
        .await;

        // Release the writer so the spawned task drops the guard
        // cleanly; cargo test would otherwise leak the task.
        writer_release.notify_one();
        writer.await.expect("writer task must finish");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_does_not_block_on_contended_async_lock_mutex() {
        let lock = Arc::new(async_lock::Mutex::new(Counter(7)));
        let holder_started = Arc::new(tokio::sync::Notify::new());
        let holder_release = Arc::new(tokio::sync::Notify::new());

        let holder_lock = Arc::clone(&lock);
        let holder_started_for_task = Arc::clone(&holder_started);
        let holder_release_for_task = Arc::clone(&holder_release);
        let holder = tokio::spawn(async move {
            let _guard = holder_lock.lock().await;
            holder_started_for_task.notify_one();
            holder_release_for_task.notified().await;
        });

        holder_started.notified().await;

        let publish_lock = Arc::clone(&lock);
        assert_publish_does_not_block(
            Duration::from_secs(1),
            "async_lock::Mutex",
            move || {
                publish_lock.publish(MetricKind::Component, empty_field_metadata())
            },
        )
        .await;

        holder_release.notify_one();
        holder.await.expect("holder task must finish");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_does_not_block_on_contended_parking_lot_rwlock() {
        let lock = Arc::new(parking_lot::RwLock::new(Counter(7)));
        let holder_started = Arc::new(tokio::sync::Notify::new());
        // std::sync::mpsc lets the holder block on a `recv()` (sync)
        // while the test signals release from async context via
        // `send()`. Tokio's `oneshot::Receiver::blocking_recv` would
        // also work but adds an unused tokio feature dependency.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let holder_lock = Arc::clone(&lock);
        let holder_started_for_task = Arc::clone(&holder_started);
        let holder = tokio::task::spawn_blocking(move || {
            let _guard = holder_lock.write();
            holder_started_for_task.notify_one();
            // Hold the writer guard until the test releases us.
            // Sync `recv` is correct here — we are in spawn_blocking,
            // not on a runtime worker.
            release_rx.recv().expect("release channel must signal");
        });

        holder_started.notified().await;
        let publish_lock = Arc::clone(&lock);
        assert_publish_does_not_block(
            Duration::from_secs(1),
            "parking_lot::RwLock",
            move || {
                publish_lock.publish(MetricKind::Component, empty_field_metadata())
            },
        )
        .await;

        // Release the writer cleanly so the spawn_blocking thread
        // exits and cargo test doesn't leak it across tests.
        release_tx.send(()).expect("holder must still be alive");
        holder.await.expect("holder task must finish");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_does_not_block_on_contended_parking_lot_mutex() {
        let lock = Arc::new(parking_lot::Mutex::new(Counter(7)));
        let holder_started = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let holder_lock = Arc::clone(&lock);
        let holder_started_for_task = Arc::clone(&holder_started);
        let holder = tokio::task::spawn_blocking(move || {
            let _guard = holder_lock.lock();
            holder_started_for_task.notify_one();
            release_rx.recv().expect("release channel must signal");
        });

        holder_started.notified().await;
        let publish_lock = Arc::clone(&lock);
        assert_publish_does_not_block(
            Duration::from_secs(1),
            "parking_lot::Mutex",
            move || {
                publish_lock.publish(MetricKind::Component, empty_field_metadata())
            },
        )
        .await;

        release_tx.send(()).expect("holder must still be alive");
        holder.await.expect("holder task must finish");
    }

    /// Sanity check: the uncontended publish path still produces a
    /// real value (Counter(7)), not the skip-on-contention default.
    /// Without this we could pass the wedge tests by always returning
    /// `MetricPublishKnownKindData::Component`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_returns_real_value_when_uncontended() {
        let lock = Arc::new(async_lock::RwLock::new(Counter(7)));
        let publish_lock = Arc::clone(&lock);
        let result = tokio::spawn(async move {
            publish_lock.publish(MetricKind::Counter, empty_field_metadata())
        })
        .await
        .expect("spawned task must finish")
        .expect("publish must succeed");
        match result {
            MetricPublishKnownKindData::Counter(7) => {}
            other => panic!(
                "uncontended publish returned skip-default or wrong value: {other:?}"
            ),
        }
    }
}
