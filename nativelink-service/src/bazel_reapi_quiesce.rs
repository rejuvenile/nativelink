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

//! Bazel-REAPI quiesce gate for graceful shutdown.
//!
//! Operator directive (2026-06-23, verbatim): *"during shutdown the server
//! needs to stop accepting new work so that the blobs eventually flush: that
//! means no more bazel REAPI once shutdown starts."*
//!
//! On SIGTERM the server's graceful-shutdown sequence must converge the
//! in-flight write set to zero (so the directive-1 flush + the directive-2
//! worker-pull can reach a fixed point) instead of chasing newly-arriving
//! Bazel writes. To do that we QUIESCE the Bazel-facing REAPI listeners
//! (`:50051` / `:50071` / `:50072` — CAS / AC / ByteStream / Execution /
//! Capabilities) at the very START of shutdown: any NEW request on those
//! listeners gets `Code::Unavailable`; requests already in-flight finish
//! normally (this layer only gates the entry of a new request, it does not
//! interrupt a running one — hyper's `GracefulShutdown` GOAWAY at the later
//! drain step handles in-flight completion).
//!
//! CRITICAL — the worker API listener (`:50061`) MUST NOT be quiesced. The
//! directive-2 pull phase needs CONNECTED workers to push their blobs to the
//! server; quiescing `:50061` would sever the very transport the pull relies
//! on. So this layer is applied ONLY to listeners whose `services.worker_api`
//! is `None` (every Bazel-facing listener), never to the worker_api listener.
//! The bin's listener loop owns that selection.
//!
//! Mechanism mirrors `nativelink_util::telemetry::OtlpLayer` /
//! `OtlpMiddleware` (the existing in-repo `tower::Layer` + `tower::Service`
//! applied to the axum router via `.layer(...)`): a thin middleware that, on
//! each `call`, reads a shared `AtomicBool` and either short-circuits with a
//! gRPC-framed `Code::Unavailable` response (`tonic::Status::into_http`, so a
//! Bazel/tonic client observes `grpc-status: 14`) or delegates to the inner
//! service unchanged.

use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};
use std::sync::Arc;

use hyper::Response;

/// Shared shutdown latch for the Bazel-facing REAPI listeners.
///
/// One instance is created in `inner_main` and CLONED into:
///   - each Bazel-facing listener's [`BazelReapiQuiesceLayer`] (read side);
///   - the SIGTERM handler (write side, [`Self::quiesce`]).
///
/// Cheap to clone (one `Arc<AtomicBool>`). `Relaxed` ordering is sufficient:
/// the flag is a one-way latch (false → true, never back) and there is no
/// other memory it publishes; a request that races the flip either sees the
/// old value (accepted, then drained by GOAWAY) or the new value (rejected) —
/// both are correct shutdown behaviors.
#[derive(Clone, Debug)]
pub struct BazelReapiQuiesce {
    quiesced: Arc<AtomicBool>,
}

impl BazelReapiQuiesce {
    #[must_use]
    pub fn new() -> Self {
        Self {
            quiesced: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Latch the gate closed. Called ONCE at the start of the SIGTERM handler,
    /// BEFORE the flush + pull, so the in-flight write set can converge to a
    /// fixed point. Idempotent.
    pub fn quiesce(&self) {
        self.quiesced.store(true, Ordering::Relaxed);
    }

    /// Whether new Bazel REAPI requests should be rejected with
    /// `Code::Unavailable`.
    #[must_use]
    pub fn is_quiesced(&self) -> bool {
        self.quiesced.load(Ordering::Relaxed)
    }

    /// Build the per-listener middleware layer. Apply to a Bazel-facing
    /// listener's router ONLY (never the `:50061` worker_api listener).
    #[must_use]
    pub fn layer(&self) -> BazelReapiQuiesceLayer {
        BazelReapiQuiesceLayer {
            quiesce: self.clone(),
        }
    }
}

impl Default for BazelReapiQuiesce {
    fn default() -> Self {
        Self::new()
    }
}

/// `tower::Layer` that wraps a service in [`BazelReapiQuiesceMiddleware`].
#[derive(Clone, Debug)]
pub struct BazelReapiQuiesceLayer {
    quiesce: BazelReapiQuiesce,
}

impl<S> tower::Layer<S> for BazelReapiQuiesceLayer {
    type Service = BazelReapiQuiesceMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BazelReapiQuiesceMiddleware {
            inner,
            quiesce: self.quiesce.clone(),
        }
    }
}

/// Middleware that short-circuits new requests with `Code::Unavailable` once
/// the shared [`BazelReapiQuiesce`] latch is closed.
#[derive(Clone, Debug)]
pub struct BazelReapiQuiesceMiddleware<S> {
    inner: S,
    quiesce: BazelReapiQuiesce,
}

impl<S, ReqBody, ResBody> tower::Service<hyper::http::Request<ReqBody>>
    for BazelReapiQuiesceMiddleware<S>
where
    S: tower::Service<hyper::http::Request<ReqBody>, Response = Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: From<String> + Send + 'static + Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::http::Request<ReqBody>) -> Self::Future {
        if self.quiesce.is_quiesced() {
            // New Bazel REAPI request during shutdown: reject with a
            // gRPC-framed UNAVAILABLE (`grpc-status: 14`). `into_http`
            // produces the gRPC trailer-only response a tonic client maps to
            // `Code::Unavailable` and retries against a healthy server.
            return Box::pin(async move {
                Ok(tonic::Status::unavailable(
                    "nativelink is shutting down; Bazel REAPI is quiesced — retry on a healthy server",
                )
                .into_http())
            });
        }
        // Take the ready `inner` (not the clone) per the tower cloning
        // guidance — same discipline as `OtlpMiddleware::call`.
        let clone = self.inner.clone();
        let mut inner = core::mem::replace(&mut self.inner, clone);
        Box::pin(async move { inner.call(req).await })
    }
}

#[cfg(test)]
mod tests {
    use core::convert::Infallible;
    use core::future::{Ready, ready};

    use http_body_util::Empty;
    use hyper::body::Bytes;
    use hyper::{Request, Response, StatusCode};
    use tower::{Layer, Service};

    use super::*;

    /// Trivial inner service that, when reached, returns 200 OK with a
    /// "served" body. The quiesce middleware either short-circuits it
    /// (quiesced) or delegates to it (open). Hand-rolled (no `tower::util`
    /// `service_fn`, which the workspace does not enable) so the test needs
    /// no feature change.
    #[derive(Clone)]
    struct OkInner;

    impl Service<Request<Empty<Bytes>>> for OkInner {
        type Response = Response<String>;
        type Error = Infallible;
        type Future = Ready<Result<Response<String>, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Empty<Bytes>>) -> Self::Future {
            ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(String::from("served"))
                .expect("response builds")))
        }
    }

    /// Drive a single request through `mw` (poll_ready, then call). Avoids
    /// `tower::ServiceExt::oneshot` (the `util` feature is not enabled).
    async fn drive<S>(mut mw: S, req: Request<Empty<Bytes>>) -> Response<String>
    where
        S: Service<Request<Empty<Bytes>>, Response = Response<String>, Error = Infallible>,
    {
        core::future::poll_fn(|cx| mw.poll_ready(cx))
            .await
            .expect("middleware poll_ready is infallible");
        mw.call(req).await.expect("middleware call is infallible")
    }

    /// Extract the gRPC status code from a response's `grpc-status` header
    /// (`tonic::Status::into_http` emits it as a header on the trailer-only
    /// response). Returns `None` if absent.
    fn grpc_status(resp: &Response<String>) -> Option<i32> {
        resp.headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i32>().ok())
    }

    /// OPEN gate (steady state): the middleware delegates to the inner
    /// service unchanged — a Bazel request is served.
    ///
    /// Mutation: make `call` ALWAYS short-circuit (ignore `is_quiesced`) →
    /// this test red-fails because the OK body never arrives.
    #[tokio::test]
    async fn open_gate_passes_request_through() {
        let gate = BazelReapiQuiesce::new();
        assert!(!gate.is_quiesced(), "fresh gate must be open");
        let mw = gate.layer().layer(OkInner);
        let resp = drive(mw, Request::new(Empty::<Bytes>::new())).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "open gate MUST pass the Bazel request through to the inner service"
        );
        assert_eq!(
            resp.into_body(),
            "served",
            "open gate must return the inner service's body, not a quiesce stub"
        );
    }

    /// CLOSED gate (shutdown): after `quiesce()`, a new Bazel REAPI request is
    /// rejected with gRPC `Code::Unavailable` (14) and the inner service is
    /// NOT reached.
    ///
    /// Mutation: delete the `if self.quiesce.is_quiesced()` short-circuit in
    /// `call` → the request reaches the inner OK service → `grpc-status` is
    /// absent / status is 200 → this test red-fails on the
    /// "must reject with UNAVAILABLE" assertions.
    #[tokio::test]
    async fn closed_gate_rejects_new_request_with_unavailable() {
        let gate = BazelReapiQuiesce::new();
        gate.quiesce();
        assert!(gate.is_quiesced(), "quiesce() must latch the gate closed");
        let mw = gate.layer().layer(OkInner);
        let resp = drive(mw, Request::new(Empty::<Bytes>::new())).await;
        assert_eq!(
            grpc_status(&resp),
            Some(14),
            "closed gate MUST reject new Bazel REAPI requests with gRPC \
             Code::Unavailable (14) so the client retries on a healthy server \
             (mutation: drop the is_quiesced short-circuit in call)"
        );
        assert_ne!(
            resp.into_body(),
            "served",
            "closed gate MUST NOT reach the inner service — the Bazel REAPI \
             request must be short-circuited before any CAS/AC write begins"
        );
    }

    /// The SAME gate instance shared by reference (clone) observes the latch:
    /// quiescing through one handle closes the gate seen by a layer built from
    /// a CLONE — proving the SIGTERM handler's `quiesce()` reaches every
    /// per-listener layer that was cloned off the same `BazelReapiQuiesce`.
    ///
    /// Mutation: make `BazelReapiQuiesce::clone` allocate a FRESH AtomicBool
    /// (break the `Arc` share) → the cloned layer never sees the latch →
    /// red-fails on the UNAVAILABLE assertion.
    #[tokio::test]
    async fn quiesce_through_one_handle_is_seen_by_a_clone() {
        let gate = BazelReapiQuiesce::new();
        // The per-listener layer is built from a CLONE (mirrors the bin: each
        // Bazel listener gets `quiesce.layer()` off a shared instance).
        let listener_layer = gate.clone().layer();
        // The SIGTERM handler holds its OWN clone and flips it.
        let sigterm_handle = gate.clone();
        sigterm_handle.quiesce();
        let mw = listener_layer.layer(OkInner);
        let resp = drive(mw, Request::new(Empty::<Bytes>::new())).await;
        assert_eq!(
            grpc_status(&resp),
            Some(14),
            "quiescing through one clone MUST close the gate seen by every \
             other clone (shared Arc<AtomicBool>) — otherwise the SIGTERM \
             handler's quiesce() would not reach the per-listener layers"
        );
    }
}
