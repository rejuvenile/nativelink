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

// Re-export for downstream callers (binary, tests) that need to upcast
// `Arc<dyn SomeTrait: MetricsComponent>` for `register_dyn` without
// having to add a direct `nativelink-metric` dependency.
pub use nativelink_metric::MetricsComponent as MetricsComponentTrait;
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
        self.register_dyn(prefix, component);
    }

    /// Register an already-erased `Arc<dyn MetricsComponent + Send + Sync>`.
    ///
    /// Use this when the component arrives as a trait object whose trait
    /// has `MetricsComponent` as a supertrait (e.g. `Arc<dyn WorkerScheduler>`
    /// where `WorkerScheduler: RootMetricsComponent: MetricsComponent`).
    /// Rust trait upcasting (stable since 1.86) lets the caller cast to
    /// `Arc<dyn MetricsComponent + Send + Sync>` at the call site.
    pub fn register_dyn(
        &self,
        prefix: impl Into<String>,
        component: Arc<dyn MetricsComponent + Send + Sync>,
    ) {
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
    /// Counts events whose target is NOT `nativelink_metric` observed
    /// during the scrape. The capture layer has the global dispatcher
    /// for the scraping thread (see `with_default` in
    /// [`render_prometheus`]); any such event is silently dropped from
    /// the production tracing pipeline. We surface the count after
    /// `with_default` returns so accidental drift (a `warn!` added to
    /// a `publish()` body) is visible to operators rather than silent.
    non_metric_event_count: Arc<core::sync::atomic::AtomicU64>,
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
            // Drift detector for the `with_default` thread-local-swap
            // documented on `render_prometheus`. Counted now, surfaced
            // by the caller after `with_default` returns so the
            // operator-visible warning lands on the production
            // subscriber rather than this capture layer.
            self.non_metric_event_count
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
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
///
/// # Tracing isolation contract (DSR M2 / perf M1)
///
/// This function uses [`tracing::subscriber::with_default`] to install a
/// fresh capture-only `Registry` on the *current thread* for the
/// duration of `publish()`. While that capture is active, ANY
/// `tracing` event the publish closure (or any code it transitively
/// calls) emits is captured by this layer and **not** forwarded to
/// the production subscriber installed by [`crate::telemetry`]. In
/// particular, `warn!`/`error!` events emitted from inside a
/// `MetricsComponent::publish` impl during a `/metrics` scrape are
/// silently absorbed by the capture and never reach stdout, OTLP, or
/// any other production sink.
///
/// We accept this trade-off because:
/// - Existing `publish()` impls in this codebase emit ONLY
///   `nativelink_metric` target events (the derived ones from
///   `#[derive(MetricsComponent)]`); they do not log diagnostics from
///   user code.
/// - Composing the capture layer onto the existing global dispatcher
///   (via `tracing-subscriber`'s `reload::Layer` or a custom
///   `Dispatch::new` wrapper) requires structural changes to
///   `crate::telemetry` initialization and is deferred to a future
///   change.
///
/// **If you add a `warn!`/`error!`/`info!` to a `publish()` body for
/// non-`nativelink_metric` diagnostics, those events will be lost
/// during scrapes.** Emit such diagnostics from the construction or
/// hot-path side instead.
///
/// In debug builds, accidental drift is surfaced via a counter event
/// (`metrics_publisher_unexpected_events_total`) the layer emits if
/// it observes any non-`nativelink_metric` event during a scrape.
#[must_use]
pub fn render_prometheus(registry: &MetricsRegistry) -> String {
    let snapshot = registry.snapshot();
    let captured = Arc::new(Mutex::new(Vec::<CapturedMetric>::new()));
    let non_metric_event_count = Arc::new(core::sync::atomic::AtomicU64::new(0));

    let layer = CaptureLayer {
        events: captured.clone(),
        non_metric_event_count: non_metric_event_count.clone(),
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

    // Drift surfacing: emitted on the production subscriber (the
    // capture layer is no longer the default after `with_default`
    // returns). Non-zero count means a `publish()` body — or
    // something it called — emitted a non-`nativelink_metric` event
    // that this scrape silently absorbed; this is almost certainly
    // unintended (`warn!`/`error!` in `publish` is the typical
    // mistake) and operators should investigate.
    let dropped =
        non_metric_event_count.load(core::sync::atomic::Ordering::Relaxed);
    if dropped > 0 {
        tracing::warn!(
            dropped_events = dropped,
            "metrics publisher absorbed non-nativelink_metric tracing events during scrape (silent drop) — see render_prometheus rustdoc"
        );
    }

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

    use nativelink_macro::nativelink_test;
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

    /// #380 fix gate: end-to-end check that `MokaEvictingMap`'s
    /// hand-rolled `MetricsComponent::publish` actually emits
    /// `pinned_bytes` (and the other load-bearing fields) on a real
    /// `/metrics` scrape — not just the `Component` marker the prior
    /// hollow-stub impl returned.
    ///
    /// This unblocks #160 ship per the red-team RECONSIDER-PREMISE
    /// second pass (2026-05-11): the whole point of #160 was making
    /// `cas_FAST_SLOW_STORE.fast.memory.evicting_map.pinned_bytes`
    /// scrapable so #332's prophylactic pin-cap-headroom claim is
    /// operationally falsifiable post-deploy. With the publisher
    /// hollow, scrapes returned an empty body for this branch and the
    /// claim was unverifiable.
    ///
    /// Mutation step (per CLAUDE.md TDD discipline): comment out the
    /// `pinned_bytes` `nativelink_metric::publish!` call inside
    /// `MokaEvictingMap::publish` (`nativelink-util/src/moka_evicting_map.rs`,
    /// the impl block immediately following the `Debug` impl). The
    /// assertion below MUST red-fail with the bespoke message starting
    /// `#380 fix gate: ...` so an operator who later regresses the
    /// emission knows immediately which contract was broken.
    #[test]
    fn moka_evicting_map_publish_emits_pinned_bytes() {
        use core::time::Duration;
        use std::time::SystemTime;

        use nativelink_config::stores::EvictionPolicy;

        use crate::evicting_map::NoopCallback;
        use crate::moka_evicting_map::MokaEvictingMap;

        // Minimal LenEntry impl — value type is not exercised by the
        // publisher walk; we only need a constructable map.
        #[derive(Debug, Clone)]
        struct Entry(u64);
        impl crate::evicting_map::LenEntry for Entry {
            fn len(&self) -> u64 {
                self.0
            }
            fn is_empty(&self) -> bool {
                self.0 == 0
            }
        }

        let cfg = EvictionPolicy {
            max_bytes: 1024,
            evict_bytes: 0,
            max_seconds: 0,
            max_count: 0,
            pin_cap_bytes: 0,
        };
        let map: MokaEvictingMap<u64, u64, Entry, SystemTime, NoopCallback> =
            MokaEvictingMap::with_anchor(&cfg, SystemTime::now());

        let registry = MetricsRegistry::new();
        registry.register("memstore", Arc::new(map));

        let body = render_prometheus(&registry);

        // The bespoke message names #380 + the red-team RECONSIDER
        // origin so a future regression triage points straight at the
        // contract. Generic `is_ok()`-style messages would mask the
        // class of failure (e.g. wrong group nesting -> all
        // pinned_bytes lines go missing too, but the body is still
        // non-empty).
        assert!(
            body.contains("memstore_pinned_bytes"),
            "#380 fix gate: MokaEvictingMap::publish must emit pinned_bytes for #332 falsifiability via #160 publisher (red-team RECONSIDER 2026-05-11). body=\n{body}"
        );
        assert!(
            body.contains("memstore_pin_cap"),
            "#380 fix gate: MokaEvictingMap::publish must emit pin_cap for #332 falsifiability via #160 publisher (red-team RECONSIDER 2026-05-11). body=\n{body}"
        );
        assert!(
            body.contains("memstore_entry_count"),
            "#380 fix gate: MokaEvictingMap::publish must emit entry_count for #332 falsifiability via #160 publisher (red-team RECONSIDER 2026-05-11). body=\n{body}"
        );

        // Belt-and-braces: verify pin_cap matches the documented
        // PIN_CAP_FRACTION (25% of max_bytes = 256). This catches the
        // accidental wrong-field regression (e.g. someone swaps
        // `pin_cap` for `max_bytes` in publish! and the contains-check
        // still passes).
        assert!(
            body.contains("\nmemstore_pin_cap 256\n"),
            "#380 fix gate: pin_cap must equal max_bytes * 25% = 256 for max_bytes=1024 (red-team RECONSIDER 2026-05-11). body=\n{body}"
        );

        // Silence unused-import warnings on Duration when the test
        // body shrinks during edits — we keep the import for future
        // assertions about anchor-time-derived gauges.
        let _ = Duration::from_secs(0);
    }

    /// F3a render-test (production `/metrics` collection path): the moka
    /// weigher stores weights in KB-units (`value.len().div_ceil(1024)`),
    /// so `cache.weighted_size()` is a KB-weight count, NOT a byte count.
    /// The legacy `weighted_size` metric was published with a help-text
    /// claiming "(bytes)", which a reader took at face value and compared
    /// to on-disk bytes — producing a phantom 144 GB "orphan" (a 1024×
    /// units misread). This test pins, via the same `render_prometheus`
    /// walk `/metrics` uses, that:
    ///
    ///   1. an additive byte-accurate `weighted_size_bytes` gauge renders
    ///      the true byte usage (`weighted_size * 1024`) with a "(bytes)"
    ///      help-text, so byte↔disk comparisons are direct and correct; and
    ///   2. the legacy `weighted_size` gauge keeps its name + KB-weight
    ///      VALUE for dashboard back-compat, but its help-text no longer
    ///      claims bytes — it states KB-weight units.
    ///
    /// A 4096-byte blob weighs `4096.div_ceil(1024) = 4` KB-units, so the
    /// legacy gauge reads `4` and the byte-accurate gauge reads
    /// `4 * 1024 = 4096`.
    ///
    /// Mutation step (CLAUDE.md TDD): (a) drop the `weighted_size_bytes`
    /// `publish!` call OR (b) revert the `weighted_size` help-text back to
    /// the "(bytes)" wording in `MokaEvictingMap::publish`
    /// (`nativelink-util/src/moka_evicting_map.rs`). Each assertion below
    /// MUST red-fail with its bespoke `F3a:` message.
    #[nativelink_test("crate")]
    async fn moka_weighted_size_bytes_units_are_correct() {
        use std::time::SystemTime;

        use nativelink_config::stores::EvictionPolicy;

        use crate::evicting_map::NoopCallback;
        use crate::moka_evicting_map::MokaEvictingMap;

        #[derive(Debug, Clone)]
        struct Entry(u64);
        impl crate::evicting_map::LenEntry for Entry {
            fn len(&self) -> u64 {
                self.0
            }
            fn is_empty(&self) -> bool {
                self.0 == 0
            }
        }

        // 64 KiB cap so a single 4 KiB blob stays resident (no eviction).
        let cfg = EvictionPolicy {
            max_bytes: 64 * 1024,
            evict_bytes: 0,
            max_seconds: 0,
            max_count: 0,
            pin_cap_bytes: 0,
        };
        let map: MokaEvictingMap<u64, u64, Entry, SystemTime, NoopCallback> =
            MokaEvictingMap::with_anchor(&cfg, SystemTime::now());

        // 4096 bytes -> weighs 4096.div_ceil(1024) = 4 KB-units.
        map.insert(1, Entry(4096)).await;

        let registry = MetricsRegistry::new();
        registry.register("memstore", Arc::new(map));

        let body = render_prometheus(&registry);

        // (1) Byte-accurate gauge renders the TRUE byte usage = 4096
        //     (weighted_size 4 * SCALE 1024), not the KB-weight 4.
        assert!(
            body.contains("\nmemstore_weighted_size_bytes 4096\n"),
            "F3a: weighted_size_bytes must render the true byte usage (weighted_size * 1024 = 4096) so byte<->disk comparisons are direct. body=\n{body}"
        );
        // (1b) ...with a help-text that actually says bytes.
        let bytes_help = body
            .lines()
            .find(|l| l.starts_with("# HELP memstore_weighted_size_bytes "))
            .unwrap_or("");
        assert!(
            bytes_help.contains("(bytes)"),
            "F3a: weighted_size_bytes help-text must state it is in bytes. help line=\n{bytes_help}\nbody=\n{body}"
        );

        // (2) Legacy gauge keeps its KB-weight VALUE (=4) for back-compat.
        assert!(
            body.contains("\nmemstore_weighted_size 4\n"),
            "F3a: legacy weighted_size gauge must keep its KB-weight value (=4) for dashboard back-compat (name/value unchanged). body=\n{body}"
        );
        // (2b) ...but its help-text must NO LONGER claim bytes; it must
        //      state KB-weight units (this is the units-mislabel fix).
        let legacy_help = body
            .lines()
            .find(|l| {
                l.starts_with("# HELP memstore_weighted_size ")
                    && !l.starts_with("# HELP memstore_weighted_size_bytes ")
            })
            .unwrap_or("");
        assert!(
            legacy_help.contains("KB-WEIGHT") && !legacy_help.contains("(bytes)"),
            "F3a: legacy weighted_size help-text must state KB-WEIGHT units and must not claim '(bytes)' (the units mislabel that caused the phantom 144 GB orphan). help line=\n{legacy_help}\nbody=\n{body}"
        );
    }

    /// (FINDING 2 piece 2) Render-test pinning the eviction-wedge
    /// observability names on the SAME live-rendering tree as
    /// `weighted_size_bytes` (dark-counter trap: the production wedge sat
    /// at 3.3× over budget with 3K disk-NAKs and NOTHING paged, because
    /// no rendered metric carried the overshoot or the wedge verdict).
    /// Pins, via the same `render_prometheus` walk `/metrics` uses:
    ///
    ///   1. `overshoot_bytes` — bytes the live weighted size exceeds
    ///      `max_bytes` (0 at-or-under; reads the REAL cache state).
    ///   2. `eviction_wedge_detected` — 0/1 gauge, 1 while the self-heal
    ///      trigger condition holds.
    ///   3. `eviction_wedge_selfheal_total` — `CounterWithTime` firing
    ///      count (renders `_counter` + `_last_time`).
    ///
    /// Mutation step (CLAUDE.md TDD): drop any of the three `publish!`
    /// calls in `MokaEvictingMap::publish` → the matching assertion below
    /// red-fails with its bespoke `F2-wedge:` message.
    #[nativelink_test("crate")]
    async fn moka_wedge_observability_metrics_render() {
        use std::time::SystemTime;

        use nativelink_config::stores::EvictionPolicy;

        use crate::evicting_map::NoopCallback;
        use crate::moka_evicting_map::MokaEvictingMap;

        #[derive(Debug, Clone)]
        struct Entry(u64);
        impl crate::evicting_map::LenEntry for Entry {
            fn len(&self) -> u64 {
                self.0
            }
            fn is_empty(&self) -> bool {
                self.0 == 0
            }
        }

        // 64 KiB cap so the single 4 KiB blob stays resident.
        let cfg = EvictionPolicy {
            max_bytes: 64 * 1024,
            evict_bytes: 0,
            max_seconds: 0,
            max_count: 0,
            pin_cap_bytes: 0,
        };
        let map: Arc<MokaEvictingMap<u64, u64, Entry, SystemTime, NoopCallback>> =
            Arc::new(MokaEvictingMap::with_anchor(&cfg, SystemTime::now()));
        map.insert(1, Entry(4096)).await;

        let registry = MetricsRegistry::new();
        registry.register("memstore", Arc::clone(&map));

        // Baseline: healthy under-budget cache → 0 / 0 / 0.
        let body = render_prometheus(&registry);
        assert!(
            body.contains("\nmemstore_overshoot_bytes 0\n"),
            "F2-wedge: overshoot_bytes gauge must render (0 while at-or-under budget) so \
             a budget overshoot is visible on the live metrics tree. body=\n{body}"
        );
        assert!(
            body.contains("\nmemstore_eviction_wedge_detected 0\n"),
            "F2-wedge: eviction_wedge_detected gauge must render 0 on a healthy cache. \
             body=\n{body}"
        );
        assert!(
            body.contains("\nmemstore_eviction_wedge_selfheal_total_counter 0\n"),
            "F2-wedge: eviction_wedge_selfheal_total counter must render 0 before any \
             self-heal firing. body=\n{body}"
        );

        // Drive the wedge trigger (injected observation, real eviction —
        // see moka_evicting_map.rs test-seam docs) and re-render.
        map.test_force_wedge_observation(2 * 64 * 1024);
        for _ in 0..3 {
            map.maybe_selfheal_wedged_eviction().await;
        }
        let body = render_prometheus(&registry);
        assert!(
            body.contains("\nmemstore_eviction_wedge_detected 1\n"),
            "F2-wedge: eviction_wedge_detected must render 1 while the wedge trigger \
             condition holds (at-or-over budget + evictions frozen for the full \
             trigger window). body=\n{body}"
        );
        assert!(
            body.contains("\nmemstore_eviction_wedge_selfheal_total_counter 1\n"),
            "F2-wedge: eviction_wedge_selfheal_total must count the self-heal firing \
             (exactly one per firing). body=\n{body}"
        );
        // overshoot_bytes reads the REAL cache (the injected observation
        // is a trigger-only test seam): the heal evicted the resident
        // blob, so the real overshoot is still 0.
        assert!(
            body.contains("\nmemstore_overshoot_bytes 0\n"),
            "F2-wedge: overshoot_bytes must read the REAL weighted size (0 after the \
             heal evicted the resident blob), not the injected test observation. \
             body=\n{body}"
        );
    }
}
