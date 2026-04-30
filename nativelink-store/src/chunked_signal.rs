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

//! Encode / decode helpers for the wire-stable `BackpressureSignal`
//! proto detail. Used by Phase 2:
//!
//! - **Producer side (admission, FastSlowStore::update):**
//!   `encode_backpressure_signal_any(reason, retry_after_ms)` mints a
//!   `prost_types::Any` ready to drop into
//!   `Error::resource_exhausted_backpressure`.
//! - **Consumer side (`looks_like_dead_channel` in
//!   `nativelink-store/src/grpc_store.rs`):**
//!   `error_has_backpressure_signal(&err)` returns true iff the error
//!   carries our discriminator detail. Used to keep a tagged
//!   `ResourceExhausted` from being misclassified as a dead h2 channel
//!   (the §13.1.1 point 2 fix).
//!
//! Both helpers reference the single `BACKPRESSURE_SIGNAL_TYPE_URL`
//! constant from `nativelink-proto` so producer and consumer can never
//! drift on the wire string.

use nativelink_error::Error;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, backpressure_signal,
};
use prost::Message;

/// Build a `prost_types::Any` carrying an encoded `BackpressureSignal`.
/// `retry_after_ms` is a hint only — clients may apply jitter.
///
/// Phase 2 callers wrap the result in
/// `Error::resource_exhausted_backpressure(...)` to produce the
/// tagged status that the §13.1.1 point 2 classifier matches against.
#[must_use]
pub(crate) fn encode_backpressure_signal_any(
    reason: backpressure_signal::Reason,
    retry_after_ms: u64,
) -> prost_types::Any {
    let signal = BackpressureSignal {
        reason: reason as i32,
        retry_after_ms,
    };
    prost_types::Any {
        type_url: BACKPRESSURE_SIGNAL_TYPE_URL.to_string(),
        value: signal.encode_to_vec(),
    }
}

/// True iff `err.details` carries a `BackpressureSignal` (matched by
/// `type_url` only — the value bytes are not re-decoded here, just
/// checked for presence). Phase 2 consumer check; Phase 1 wires this
/// into `looks_like_dead_channel` so the Q8 backpressure path doesn't
/// evict h2 channels.
///
/// We deliberately do NOT decode the value: the `type_url` is the
/// load-bearing wire contract, and a producer that emits the
/// type_url with a malformed value is still asserting "this is
/// backpressure, not a dead channel" — the classifier should respect
/// that even if the body is unreadable.
#[must_use]
pub(crate) fn error_has_backpressure_signal(err: &Error) -> bool {
    // Short-circuit on the dominant case: errors without details
    // never carry our backpressure discriminator. Avoids iterator
    // setup on the hot classifier path (`looks_like_dead_channel`
    // runs on every gRPC client error).
    if err.details.is_empty() {
        return false;
    }
    err.details
        .iter()
        .any(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL)
}

#[cfg(test)]
mod tests {
    use nativelink_error::{Code, Error, make_err};
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, backpressure_signal,
    };
    use prost::Message;

    use super::{encode_backpressure_signal_any, error_has_backpressure_signal};

    /// Encode + decode round-trip preserves both fields. This is the
    /// load-bearing contract the wire-stability commitment promises:
    /// any peer that knows the type_url + tag layout can recover the
    /// reason and retry hint exactly.
    #[test]
    fn backpressure_signal_proto_roundtrip() {
        let any = encode_backpressure_signal_any(
            backpressure_signal::Reason::GlobalChunkBudgetExhausted,
            1234,
        );
        assert_eq!(any.type_url, BACKPRESSURE_SIGNAL_TYPE_URL);
        let decoded = BackpressureSignal::decode(&*any.value)
            .expect("encoded BackpressureSignal must decode cleanly");
        assert_eq!(
            decoded.reason,
            backpressure_signal::Reason::GlobalChunkBudgetExhausted as i32,
        );
        assert_eq!(decoded.retry_after_ms, 1234);

        // Second variant for completeness.
        let any2 = encode_backpressure_signal_any(
            backpressure_signal::Reason::PerBlobMpscFull,
            17,
        );
        let decoded2 = BackpressureSignal::decode(&*any2.value).expect("decode");
        assert_eq!(
            decoded2.reason,
            backpressure_signal::Reason::PerBlobMpscFull as i32,
        );
        assert_eq!(decoded2.retry_after_ms, 17);
    }

    /// Helper returns true when the discriminator detail is present,
    /// false when absent — load-bearing for the §13.1.1 point 2 fix.
    /// Tests (a) signal present, (b) signal absent, (c) different
    /// detail type_url present (not ours).
    #[test]
    fn error_has_backpressure_signal_present_and_absent() {
        let any = encode_backpressure_signal_any(
            backpressure_signal::Reason::GlobalChunkBudgetExhausted,
            10,
        );
        let with_signal = Error::resource_exhausted_backpressure("backpressure", any);
        assert!(
            error_has_backpressure_signal(&with_signal),
            "ResourceExhausted with our signal must be detected",
        );

        // No details at all → false.
        let no_details: Error = make_err!(Code::ResourceExhausted, "no signal");
        assert!(
            !error_has_backpressure_signal(&no_details),
            "ResourceExhausted without our signal must not be detected",
        );

        // Some other detail type_url → false (the wire contract is the
        // type_url, not just "any detail present").
        let mut with_other_detail: Error = make_err!(Code::ResourceExhausted, "other detail");
        with_other_detail.details.push(prost_types::Any {
            type_url: "type.googleapis.com/some.other.Type".into(),
            value: vec![1, 2, 3],
        });
        assert!(
            !error_has_backpressure_signal(&with_other_detail),
            "different detail type_url must not be classified as backpressure",
        );

        // Non-ResourceExhausted code carrying our signal: the helper
        // still returns true because callers already gate on Code; the
        // helper is purely a detail-presence check.
        let any2 = encode_backpressure_signal_any(
            backpressure_signal::Reason::PerBlobMpscFull,
            5,
        );
        let mut not_resource_exhausted: Error = make_err!(Code::Internal, "huh");
        not_resource_exhausted.details.push(any2);
        assert!(
            error_has_backpressure_signal(&not_resource_exhausted),
            "helper does NOT gate on Code; callers must do that",
        );
    }
}
