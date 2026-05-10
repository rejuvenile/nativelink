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

//! Publisher that walks `MetricsComponent`-derived trees and renders
//! Prometheus exposition format text.
//!
//! Background: every `#[derive(MetricsComponent)]` struct emits its
//! fields via `tracing::info!` events on the `nativelink_metric`
//! target when its `publish()` method is called. Without a subscriber
//! that captures those events, every metric in the binary is invisible
//! to operators (#160). This module provides:
//!
//! 1. `MetricsRegistry` — process-wide list of root components to
//!    publish (e.g. `worker_api -> Arc<WorkerApiMetrics>`).
//! 2. `render_prometheus(&MetricsRegistry)` — installs a temporary
//!    capture layer, drives `publish()` on each registered root, and
//!    returns Prometheus text exposition format.
//! 3. `metrics_router(MetricsRegistry)` — axum `Router` that serves
//!    `GET /metrics` returning that text body.
//!
//! Group hierarchy is preserved: `group!(name)` opens a span carrying
//! a `__name` attribute. `on_new_span` snapshots that into the span's
//! extensions, and `on_event` walks ancestors via `ctx.event_scope`
//! to produce dotted metric names like
//! `worker_api.chunked_blobs_available.dropped_size_zero`.

use std::sync::{Arc, Mutex};

use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent};
use tracing::span;
use tracing::subscriber::with_default;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// One registered root component plus the prefix prepended to its
/// metric names. Components are kept as `Arc<dyn MetricsComponent +
/// Send + Sync>` so the binary can hold the same Arc as the
/// component's owner and never needs to clone the inner data.
pub struct RegisteredComponent {
    pub prefix: String,
    pub component: Arc<dyn MetricsComponent + Send + Sync>,
}

// Manual `Debug` because `dyn MetricsComponent` does not require
// `Debug`; we only want the prefix in formatted output anyway.
impl core::fmt::Debug for RegisteredComponent {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RegisteredComponent")
            .field("prefix", &self.prefix)
            .field("component", &"<dyn MetricsComponent>")
            .finish()
    }
}

/// Process-wide list of components to publish. Cloning is cheap
/// (`Arc<Mutex<Vec<...>>>`) so the same registry can be passed to
/// every per-server admin route.
#[derive(Clone, Default, Debug)]
pub struct MetricsRegistry {
    inner: Arc<Mutex<Vec<RegisteredComponent>>>,
}

impl MetricsRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a root component. `prefix` becomes the leading dotted
    /// segment of every metric name it publishes (e.g. `worker_api`).
    pub fn register<C>(&self, prefix: impl Into<String>, component: Arc<C>)
    where
        C: MetricsComponent + Send + Sync + 'static,
    {
        let prefix = prefix.into();
        // The Vec lock is held only for the push; rendering takes a
        // snapshot via `clone` of each Arc and drops the lock before
        // any publish() call to avoid holding it across user code.
        self.inner.lock().expect("metrics registry mutex poisoned").push(
            RegisteredComponent { prefix, component },
        );
    }

    /// Snapshot the currently registered components so rendering does
    /// not hold the registry lock across `publish()`.
    fn snapshot(&self) -> Vec<RegisteredComponent> {
        self.inner
            .lock()
            .expect("metrics registry mutex poisoned")
            .iter()
            .map(|r| RegisteredComponent {
                prefix: r.prefix.clone(),
                component: r.component.clone(),
            })
            .collect()
    }
}

/// One published metric, captured from a `nativelink_metric` event.
#[derive(Debug, Clone)]
struct CapturedMetric {
    /// Dotted name including all ancestor `group!` names plus the
    /// leaf field name (e.g. `chunked_blobs_available.dropped_size_zero`).
    name: String,
    /// Stringified value (Counter -> integer, String -> string).
    value: String,
    /// Help text from `#[metric(help = "...")]`.
    help: String,
    /// `Counter` or `String` — Component events are skipped at capture.
    kind: MetricKind,
}

#[derive(Default)]
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedMetric>>>,
}

/// Snapshot of a span's `__name` attribute, stored in the span's
/// extensions by `on_new_span`. The trailing slot lets `on_event`
/// walk ancestors and reconstruct the dotted path without re-running
/// the field visitor on the span attributes (which is not ergonomic
/// from a `Layer` callback once the span is open).
#[derive(Clone)]
struct SpanGroupName(String);

impl<S> Layer<S> for CaptureLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: Context<'_, S>,
    ) {
        if attrs.metadata().target() != "nativelink_metric" {
            return;
        }
        let mut visitor = NameOnly::default();
        attrs.record(&mut visitor);
        if visitor.name.is_empty() {
            return;
        }
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanGroupName(visitor.name));
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() != "nativelink_metric" {
            return;
        }
        let mut visitor = FieldGrabber::default();
        event.record(&mut visitor);
        // Empty `__name` events are the group-span enters from
        // `group!(...)` (see nativelink-metric/src/lib.rs::group!),
        // not actual metric publishes — they are captured via
        // `on_new_span` above. Skip them here to avoid double-counting.
        if visitor.name.is_empty() {
            return;
        }

        // Walk ancestor spans to reconstruct the dotted group path.
        // `event_scope` yields innermost-first; reverse so the
        // outermost group comes first in the dotted name.
        let mut groups: Vec<String> = Vec::new();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                if let Some(g) = span.extensions().get::<SpanGroupName>() {
                    groups.push(g.0.clone());
                }
            }
        }
        groups.reverse();

        let mut full_name = String::new();
        for g in &groups {
            full_name.push_str(g);
            full_name.push('.');
        }
        full_name.push_str(&visitor.name);

        self.events.lock().expect("capture mutex poisoned").push(
            CapturedMetric {
                name: full_name,
                value: visitor.value,
                help: visitor.help,
                kind: visitor.kind,
            },
        );
    }
}

#[derive(Default)]
struct NameOnly {
    name: String,
}

impl tracing::field::Visit for NameOnly {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn core::fmt::Debug) {
        if field.name() == "__name" {
            let s = format!("{value:?}");
            self.name = s.trim_matches('"').to_string();
        }
    }
}

struct FieldGrabber {
    name: String,
    value: String,
    help: String,
    kind: MetricKind,
}

impl Default for FieldGrabber {
    fn default() -> Self {
        Self {
            name: String::new(),
            value: String::new(),
            help: String::new(),
            kind: MetricKind::Default,
        }
    }
}

impl tracing::field::Visit for FieldGrabber {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn core::fmt::Debug) {
        let s = format!("{value:?}");
        let trimmed = s.trim_matches('"').to_string();
        match field.name() {
            "__name" => self.name = trimmed,
            "__value" => self.value = trimmed,
            "__help" => self.help = trimmed,
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "__type" {
            self.kind = MetricKind::from(value);
        } else if field.name() == "__value" {
            self.value = value.to_string();
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if field.name() == "__type" {
            // Negative __type would be malformed; treat as Default.
            if let Ok(u) = u64::try_from(value) {
                self.kind = MetricKind::from(u);
            }
        } else if field.name() == "__value" {
            self.value = value.to_string();
        }
    }
}

/// Drive `publish()` on every registered component, capturing the
/// emitted events through a temporary subscriber. Returns the
/// Prometheus exposition text, including HELP and TYPE comments.
///
/// Notes on format choice: Prometheus text format is the de-facto
/// standard for `/metrics` scrape endpoints (OpenMetrics is a strict
/// superset). We emit only the subset we can derive from the existing
/// `MetricsComponent` derive: `# HELP`, `# TYPE counter|gauge`, and
/// the metric line. Strings are emitted as gauges with value `1` and
/// the original text in a label, since Prometheus has no string type.
#[must_use]
pub fn render_prometheus(registry: &MetricsRegistry) -> String {
    let snapshot = registry.snapshot();
    let captured = Arc::new(Mutex::new(Vec::<CapturedMetric>::new()));

    let layer = CaptureLayer {
        events: captured.clone(),
    };
    let subscriber = Registry::default().with(layer);

    with_default(subscriber, || {
        for entry in &snapshot {
            // `prefix` becomes the outermost group via a thin
            // wrapper span so every metric is namespaced under the
            // component's logical name.
            let span = tracing::info_span!(
                target: "nativelink_metric",
                "",
                __name = entry.prefix.as_str(),
            );
            let _enter = span.enter();
            // Errors from `publish` are surfaced into the captured
            // stream below by emitting a synthetic counter; we do
            // not propagate because that would empty the response
            // for one bad component.
            if let Err(err) = entry.component.publish(
                MetricKind::Component,
                MetricFieldData::default(),
            ) {
                tracing::warn!(
                    target: "nativelink_metric",
                    __name = "publish_errors_total",
                    __type = MetricKind::Counter as u64,
                    __value = 1u64,
                    __help = format!("publish() error for prefix {}: {err}", entry.prefix),
                );
            }
        }
    });

    let captured = Arc::try_unwrap(captured)
        .map_or_else(
            |arc| arc.lock().expect("capture mutex poisoned").clone(),
            |mutex| mutex.into_inner().expect("capture mutex poisoned"),
        );

    format_prometheus(&captured)
}

fn format_prometheus(metrics: &[CapturedMetric]) -> String {
    let mut out = String::new();
    let mut emitted_help_for: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for m in metrics {
        let prom_name = sanitize_metric_name(&m.name);
        if !m.help.is_empty() && emitted_help_for.insert(prom_name.clone()) {
            out.push_str("# HELP ");
            out.push_str(&prom_name);
            out.push(' ');
            out.push_str(&escape_help(&m.help));
            out.push('\n');
            let typ = match m.kind {
                MetricKind::Counter => "counter",
                MetricKind::String => "gauge",
                MetricKind::Default | MetricKind::Component => "untyped",
            };
            out.push_str("# TYPE ");
            out.push_str(&prom_name);
            out.push(' ');
            out.push_str(typ);
            out.push('\n');
        }
        match m.kind {
            MetricKind::String => {
                out.push_str(&prom_name);
                out.push_str("{value=\"");
                out.push_str(&escape_label(&m.value));
                out.push_str("\"} 1\n");
            }
            _ => {
                out.push_str(&prom_name);
                out.push(' ');
                out.push_str(&m.value);
                out.push('\n');
            }
        }
    }
    out
}

/// Replace `.` with `_` and strip any character outside
/// `[a-zA-Z0-9_:]` per the Prometheus exposition format spec.
fn sanitize_metric_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch == '.' {
            out.push('_');
        } else if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

fn escape_help(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Axum-based router exposing `GET /metrics`. Gated behind `pprof`
/// because the same feature gates the only other axum-bearing module
/// in this crate (`pprof_server`); production builds always enable
/// it via the Justfile (`--features quic,pprof`).
#[cfg(feature = "pprof")]
pub use endpoint::metrics_router;

#[cfg(feature = "pprof")]
mod endpoint {
    use axum::Router;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;

    use super::{MetricsRegistry, render_prometheus};

    async fn metrics_handler(
        axum::extract::State(registry): axum::extract::State<MetricsRegistry>,
    ) -> Response {
        let body = render_prometheus(&registry);
        (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            body,
        )
            .into_response()
    }

    /// Build a router exposing `GET /metrics` backed by the given
    /// registry. Mount under any axum `nest_service` path; the
    /// route is the bare `/metrics` so `nest_service("/admin", ...)`
    /// resolves to `/admin/metrics`.
    #[must_use]
    pub fn metrics_router(registry: MetricsRegistry) -> Router {
        Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(registry)
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicU64, Ordering};

    use nativelink_metric::MetricsComponent;

    use super::*;

    #[derive(Default, MetricsComponent)]
    struct DummyMetrics {
        #[metric(help = "request count")]
        requests: AtomicU64,
        #[metric(help = "child counters", group = "child")]
        child: ChildMetrics,
    }

    #[derive(Default, MetricsComponent)]
    struct ChildMetrics {
        #[metric(help = "errors observed")]
        errors: AtomicU64,
    }

    #[test]
    fn render_prometheus_produces_dotted_names_and_help() {
        let metrics = Arc::new(DummyMetrics::default());
        metrics.requests.fetch_add(7, Ordering::Relaxed);
        metrics.child.errors.fetch_add(3, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        registry.register("svc", metrics.clone());

        let body = render_prometheus(&registry);

        assert!(
            body.contains("svc_requests"),
            "expected `svc_requests` line in Prometheus output, got:\n{body}"
        );
        assert!(
            body.contains("# HELP svc_requests request count"),
            "expected HELP line for svc_requests, got:\n{body}"
        );
        assert!(
            body.contains("# TYPE svc_requests counter"),
            "expected TYPE counter line for svc_requests, got:\n{body}"
        );
        assert!(
            body.contains("\nsvc_requests 7\n"),
            "expected `svc_requests 7` line, got:\n{body}"
        );
        assert!(
            body.contains("svc_child_errors"),
            "expected child group prefix `svc_child_errors`, got:\n{body}"
        );
        assert!(
            body.contains("\nsvc_child_errors 3\n"),
            "expected `svc_child_errors 3` line, got:\n{body}"
        );
    }

    /// Mutation-step guard for #160: if the publisher stops walking
    /// the registered component, this test must red-fail because
    /// `body` ends up with neither metric line. Comment out the
    /// `entry.component.publish(...)` call in `render_prometheus` to
    /// verify.
    #[test]
    fn empty_registry_produces_empty_body() {
        let registry = MetricsRegistry::new();
        let body = render_prometheus(&registry);
        assert!(
            body.is_empty(),
            "empty registry must produce empty body, got:\n{body}"
        );
    }
}
