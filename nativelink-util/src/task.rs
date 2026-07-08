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
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::{Context as TaskContext, Poll};

use futures::Future;
use hyper::rt::Executor;
use hyper_util::rt::tokio::TokioExecutor;
use opentelemetry::context::{Context, FutureExt};
use tokio::runtime::Handle;
use tokio::task::{JoinError, JoinHandle, spawn_blocking};
pub use tracing::error_span as __error_span;
use tracing::{Instrument, Span};

use crate::rayon_pool::fallback_handle;

/// Cumulative count of spawns that took the off-runtime fallback-handle
/// branch. Off-runtime callers (rayon/cpu_pool workers whose tokio context
/// was not entered, and `Drop` impls firing on those threads) are expected
/// and benign — the fallback handle is the single production runtime's own
/// handle (`nativelink.rs` builds one runtime and passes its handle to
/// `init_rayon_pool`), so the spawned task runs on the intended runtime.
/// The counter drives sampled logging (see `fallback_log_gate`) and conveys
/// cumulative volume in each emitted line.
static FALLBACK_SPAWN_COUNT: AtomicU64 = AtomicU64::new(0);

/// Emit the fallback log on the first occurrence, then only every Nth. The
/// raw `eprintln!` bypasses tracing (the appender thread may itself lack a
/// runtime context on this path), so it cannot be rate-limited by the
/// tracing layer; this count-based gate throttles it directly. Under
/// chunked-write load the fallback bursts to hundreds/10 min (measured 762
/// in one 10-min window 2026-07-07) — the same digest-sampling precedent as
/// `CHUNKED_INFLIGHT_LOG_SAMPLE_PERIOD` in `chunked_write_handler.rs`.
const FALLBACK_LOG_SAMPLE_PERIOD: u64 = 256;

/// Returns `true` if the fallback log should be emitted for the given
/// cumulative occurrence `count` (1-based). Logs the first occurrence and
/// every `FALLBACK_LOG_SAMPLE_PERIOD`-th one thereafter; suppresses the
/// rest. Pure and deterministic (count → bool) for testability.
const fn fallback_log_gate(count: u64) -> bool {
    count == 1 || count % FALLBACK_LOG_SAMPLE_PERIOD == 0
}

pub fn __spawn_with_span_and_context<F, T>(f: F, span: Span, ctx: Option<Context>) -> JoinHandle<T>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let future = f.instrument(span);
    let future = if let Some(ctx) = ctx {
        future.with_context(ctx)
    } else {
        future.with_current_context()
    };

    // Foreign threads (e.g. rayon workers spawned before init_rayon_pool,
    // or std::thread spawns from third-party libs) have no tokio context,
    // so `tokio::spawn` would panic. Fall back to the global handle
    // captured at startup. This is defense-in-depth: init_rayon_pool's
    // spawn_handler entered tokio on every rayon worker, so this branch
    // should be unreachable in production.
    if let Ok(handle) = Handle::try_current() {
        #[expect(clippy::disallowed_methods, reason = "purpose of the method")]
        return handle.spawn(future);
    }
    if let Some(handle) = fallback_handle() {
        // Direct stderr — if we're here the appender thread may also have
        // no runtime context, and `tracing::error!` could lose the message.
        // Sampled: log the first occurrence + every FALLBACK_LOG_SAMPLE_PERIOD-th
        // thereafter (the cumulative count conveys volume) so a load-driven
        // burst does not spam journald. The fallback is benign — this handle
        // is the single production runtime's own handle, so the task runs on
        // the intended runtime.
        let count = FALLBACK_SPAWN_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if fallback_log_gate(count) {
            eprintln!(
                "spawn invoked from non-tokio thread, using fallback handle (cumulative={count})"
            );
        }
        return handle.spawn(future);
    }
    // Last resort: no runtime exists at all. This will still panic, but
    // the panic now has a clear log line preceding it instead of a bare
    // rayon abort. Use eprintln so the message survives even if tracing's
    // appender thread is itself unavailable.
    eprintln!("spawn invoked with no tokio runtime available, panic imminent");
    #[expect(clippy::disallowed_methods, reason = "purpose of the method")]
    tokio::spawn(future)
}

pub fn __spawn_with_span<F, T>(f: F, span: Span) -> JoinHandle<T>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let current_ctx = Context::current();
    __spawn_with_span_and_context(f, span, Some(current_ctx))
}

pub fn __spawn_blocking<F, T>(f: F, span: Span) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[expect(clippy::disallowed_methods, reason = "purpose of the method")]
    spawn_blocking(move || span.in_scope(f))
}

#[macro_export]
macro_rules! background_spawn {
    ($name:expr, $fut:expr) => {{
        $crate::task::__spawn_with_span($fut, $crate::task::__error_span!($name))
    }};
    ($name:expr, $fut:expr, $($fields:tt)*) => {{
        $crate::task::__spawn_with_span($fut, $crate::task::__error_span!($name, $($fields)*))
    }};
    (name: $name:expr, fut: $fut:expr, target: $target:expr, $($fields:tt)*) => {{
        $crate::task::__spawn_with_span($fut, $crate::task::__error_span!(target: $target, $name, $($fields)*))
    }};
    (span: $span:expr, ctx: $ctx:expr, fut: $fut:expr) => {{
        $crate::task::__spawn_with_span_and_context($fut, $span, $ctx)
    }};
}

#[macro_export]
macro_rules! spawn {
    ($name:expr, $fut:expr) => {{
        $crate::task::JoinHandleDropGuard::new($crate::background_spawn!($name, $fut))
    }};
    ($name:expr, $fut:expr, $($fields:tt)*) => {{
        $crate::task::JoinHandleDropGuard::new($crate::background_spawn!($name, $fut, $($fields)*))
    }};
    (name: $name:expr, fut: $fut:expr, target: $target:expr, $($fields:tt)*) => {{
        $crate::task::JoinHandleDropGuard::new($crate::background_spawn!($name, $fut, target: $target, $($fields)*))
    }};
}

#[macro_export]
macro_rules! spawn_blocking {
    ($name:expr, $fut:expr) => {{
        $crate::task::JoinHandleDropGuard::new($crate::task::__spawn_blocking($fut, $crate::task::__error_span!($name)))
    }};
    ($name:expr, $fut:expr, $($fields:tt)*) => {{
        $crate::task::JoinHandleDropGuard::new($crate::task::__spawn_blocking($fut, $crate::task::__error_span!($name, $($fields)*)))
    }};
    ($name:expr, $fut:expr, target: $target:expr) => {{
        $crate::task::JoinHandleDropGuard::new($crate::task::__spawn_blocking($fut, $crate::task::__error_span!(target: $target, $name)))
    }};
    ($name:expr, $fut:expr, target: $target:expr, $($fields:tt)*) => {{
        $crate::task::JoinHandleDropGuard::new($crate::task::__spawn_blocking($fut, $crate::task::__error_span!(target: $target, $name, $($fields)*)))
    }};
}

#[cfg(test)]
mod fallback_log_gate_tests {
    use super::{FALLBACK_LOG_SAMPLE_PERIOD, fallback_log_gate};

    /// The fallback branch fires an unconditional `eprintln!` per invocation.
    /// Under chunked-write load it bursts to hundreds/10 min (measured
    /// 762 in one 10-min window 2026-07-07). The gate throttles the log to
    /// the FIRST occurrence (so the operator sees it begin) plus every
    /// `FALLBACK_LOG_SAMPLE_PERIOD`-th thereafter, while a running counter
    /// (passed in as `count`) still conveys cumulative volume. This proves
    /// the sampling contract deterministically without touching threads or
    /// the runtime.
    #[test]
    fn emits_on_first_then_every_sample_period() {
        // count starts at 1 for the first invocation (fetch_add + 1).
        assert!(
            fallback_log_gate(1),
            "first fallback occurrence MUST log so the operator sees it start"
        );
        // Everything strictly between 1 and the period is suppressed.
        for c in 2..FALLBACK_LOG_SAMPLE_PERIOD {
            assert!(
                !fallback_log_gate(c),
                "occurrence {c} between first and period boundary MUST be suppressed (spam reduction)"
            );
        }
        assert!(
            fallback_log_gate(FALLBACK_LOG_SAMPLE_PERIOD),
            "occurrence at the period boundary MUST log (periodic heartbeat)"
        );
        assert!(
            !fallback_log_gate(FALLBACK_LOG_SAMPLE_PERIOD + 1),
            "occurrence just past the period boundary MUST be suppressed"
        );
        assert!(
            fallback_log_gate(2 * FALLBACK_LOG_SAMPLE_PERIOD),
            "second period boundary MUST log"
        );
    }

    /// The period must be a real throttle (>1); a period of 1 would emit
    /// every time and defeat the fix.
    #[test]
    fn sample_period_actually_throttles() {
        assert!(
            FALLBACK_LOG_SAMPLE_PERIOD > 1,
            "sample period must be >1 or the gate emits on every call — no throttle"
        );
    }
}

/// Simple wrapper that will abort a future that is running in another spawn in the
/// event that this handle gets dropped.
#[derive(Debug)]
#[must_use]
pub struct JoinHandleDropGuard<T> {
    inner: JoinHandle<T>,
}

impl<T> JoinHandleDropGuard<T> {
    pub const fn new(inner: JoinHandle<T>) -> Self {
        Self { inner }
    }
}

impl<T> Future for JoinHandleDropGuard<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<T> Drop for JoinHandleDropGuard<T> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

#[derive(Debug, Clone)]
pub struct TaskExecutor(TokioExecutor);

impl TaskExecutor {
    pub fn new() -> Self {
        Self(TokioExecutor::new())
    }
}

impl Default for TaskExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> Executor<F> for TaskExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        background_spawn!("http_executor", fut);
    }
}
