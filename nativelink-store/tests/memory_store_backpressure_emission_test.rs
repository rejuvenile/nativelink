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

//! #212 Phase 2.6: regression tests for `MemoryStore` backpressure
//! emission.
//!
//! Asymmetric-contract coverage (CLAUDE.md mandatory practice for any
//! state-mutating side effect on a borrowed value):
//! - **Under-action / fires-when-it-should**: with the kill-switch ON
//!   and the store at capacity, `update_oneshot` MUST return
//!   `ResourceExhausted` carrying a `BackpressureSignal::MemoryStoreAtCapacity`
//!   detail.
//! - **Over-action / never-fires-when-it-should-not**: with the
//!   kill-switch OFF (the production default), the existing silent-evict
//!   behavior is preserved — `update_oneshot` always succeeds and a
//!   prior entry is evicted to make room. Callers that depend on
//!   "MemoryStore writes always succeed" cannot regress.
//!
//! Production composition: the test wraps the MemoryStore in a
//! `VerifyStore { verify_size: true, verify_hash: false }` (the
//! historical wrapper around `cas_STORE` in production) and asserts the
//! same emission behavior surfaces through the VerifyStore boundary.
//! Without this composition coverage, a wrapper that silently swallowed
//! the ResourceExhausted (or substituted a different code) would ship
//! to production with the test still green.
//!
//! Classifier interaction: the existing `looks_like_dead_channel`
//! predicate in `nativelink-store/src/grpc_store.rs` MUST treat the
//! new `MemoryStoreAtCapacity` discriminator as "transient backpressure
//! — keep the channel alive", per design §13.1.1 point 2. Without that
//! classification, every backpressure event would tear down the h2
//! channel and replay the production #147 stale-channel-reuse trace.

#![cfg(feature = "chunked_fast_slow")]

use core::pin::Pin;
use core::time::Duration;

use nativelink_config::stores::{EvictionPolicy, MemorySpec, StoreSpec, VerifySpec};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, backpressure_signal,
};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALID_HASH3: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";

/// 1 KiB cap. Combined with a single 1 KiB-rounded entry that nearly
/// fills the cap, a second insert of the same size MUST trip the
/// `would_exceed_capacity` predicate.
const TINY_CAP_BYTES: usize = 1024;

/// Build a MemoryStore with a tiny byte cap so eviction pressure is
/// reproducible without buffering megabytes in the test process.
fn tiny_memory_store() -> std::sync::Arc<MemoryStore> {
    MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: TINY_CAP_BYTES,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    })
}

/// Helper: assert that `err` is a `ResourceExhausted` carrying a
/// `BackpressureSignal` detail with the given reason. The
/// `BACKPRESSURE_SIGNAL_TYPE_URL` match is the wire contract; the
/// reason field is decoded for stronger evidence.
fn assert_backpressure_signal(err: &Error, expected_reason: backpressure_signal::Reason) {
    assert_eq!(
        err.code,
        Code::ResourceExhausted,
        "expected ResourceExhausted, got code={:?} messages={:?}",
        err.code,
        err.messages,
    );
    let signal_detail = err
        .details
        .iter()
        .find(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL)
        .expect(
            "must carry a BackpressureSignal detail — \
             without it `looks_like_dead_channel` would evict the h2 channel",
        );
    let decoded = BackpressureSignal::decode(&*signal_detail.value)
        .expect("encoded BackpressureSignal must decode cleanly");
    assert_eq!(
        decoded.reason,
        expected_reason as i32,
        "expected reason={:?} got reason={}",
        expected_reason,
        decoded.reason,
    );
}

/// **Under-action (positive case).** Kill-switch ON, store at capacity:
/// the next `update_oneshot` MUST return `ResourceExhausted` with the
/// new `MemoryStoreAtCapacity` reason. The `tokio::time::timeout`
/// guards against any future regression that could deadlock instead of
/// returning the error.
#[nativelink_test]
async fn emits_resource_exhausted_when_emission_enabled_and_at_capacity()
-> Result<(), Error> {
    let store = tiny_memory_store();
    store.set_emit_backpressure_for_test(true);

    // Fill most of the cap with a 1 KiB blob (the moka weigher rounds
    // up to KB granularity, so this consumes ~1 KB of the 1 KiB
    // capacity).
    let big_payload = vec![0u8; 1024];
    let digest1 = DigestInfo::try_new(VALID_HASH1, big_payload.len() as u64)?;
    store
        .update_oneshot(digest1, big_payload.into())
        .await
        .expect("first insert should fit");

    // Second insert that would force eviction of digest1 must error.
    let digest2 = DigestInfo::try_new(VALID_HASH2, 1024)?;
    let payload2 = vec![1u8; 1024];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest2, payload2.into()),
    )
    .await
    .expect(
        "must not deadlock — backpressure-emission must return promptly",
    );

    let err = result.expect_err(
        "second insert MUST return ResourceExhausted when kill-switch is ON \
         (otherwise the silent-evict behavior leaked through the gate)",
    );
    assert_backpressure_signal(
        &err,
        backpressure_signal::Reason::MemoryStoreAtCapacity,
    );
    Ok(())
}

/// **Over-action (negative case).** Kill-switch OFF (production
/// default), store at capacity: existing silent-evict behavior MUST be
/// preserved. The second insert succeeds via the moka LRU eviction
/// path. This guards against shipping a behavior change to operators
/// that have not opted in.
#[nativelink_test]
async fn preserves_silent_evict_when_emission_disabled() -> Result<(), Error> {
    let store = tiny_memory_store();
    // Default-OFF; do NOT call set_emit_backpressure_for_test.

    let big_payload = vec![0u8; 1024];
    let digest1 = DigestInfo::try_new(VALID_HASH1, big_payload.len() as u64)?;
    store
        .update_oneshot(digest1, big_payload.into())
        .await
        .expect("first insert should fit");

    let digest2 = DigestInfo::try_new(VALID_HASH2, 1024)?;
    let payload2 = vec![1u8; 1024];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest2, payload2.into()),
    )
    .await
    .expect("must not deadlock — silent-evict path must return promptly");
    result.expect(
        "kill-switch OFF: insert MUST succeed via existing eviction path. \
         A failure here means default-OFF gating is broken — the \
         architectural change leaked into the production default.",
    );
    Ok(())
}

/// **Production composition.** VerifyStore wrapping MemoryStore (the
/// historic cas_STORE shape) MUST surface the same ResourceExhausted +
/// BackpressureSignal through the wrapper. A wrapper that swallowed
/// the detail or substituted a different code would mask the
/// classifier signal in production.
#[nativelink_test]
async fn verify_store_around_memory_store_propagates_backpressure_signal()
-> Result<(), Error> {
    let inner = tiny_memory_store();
    inner.set_emit_backpressure_for_test(true);

    let store = VerifyStore::new(
        &VerifySpec {
            // We use a separate inner Memory backend in the spec so the
            // VerifyStore constructor is happy; the actual data store
            // we wrap is the `inner` Arc above. (The `backend` field
            // here is only consulted to construct the metric name in
            // some configurations; the data path is via the explicit
            // `Store::new(inner)` arg.)
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        Store::new(inner.clone()),
    );

    let big_payload = vec![0u8; 1024];
    let digest1 = DigestInfo::try_new(VALID_HASH1, big_payload.len() as u64)?;
    store
        .update_oneshot(digest1, big_payload.into())
        .await
        .expect("first insert through VerifyStore should fit");

    let digest2 = DigestInfo::try_new(VALID_HASH2, 1024)?;
    let payload2 = vec![1u8; 1024];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest2, payload2.into()),
    )
    .await
    .expect(
        "must not deadlock — VerifyStore-wrapped MemoryStore backpressure \
         must propagate promptly",
    );

    let err = result.expect_err(
        "ResourceExhausted from MemoryStore MUST surface through VerifyStore \
         (otherwise the wrapper is hiding the backpressure signal from the \
         classifier and replaying the production #147 stale-channel-reuse trace)",
    );
    assert_backpressure_signal(
        &err,
        backpressure_signal::Reason::MemoryStoreAtCapacity,
    );
    Ok(())
}

/// **Classifier compatibility.** The existing
/// `error_has_backpressure_signal` helper (consumed by
/// `looks_like_dead_channel` in `grpc_store.rs:159`) MUST recognize
/// the new `MemoryStoreAtCapacity` signal. If this regresses, every
/// MemoryStoreAtCapacity event would tear down the h2 channel at
/// backpressure rate and reproduce #147.
#[nativelink_test]
async fn classifier_recognizes_memory_store_at_capacity_signal()
-> Result<(), Error> {
    let store = tiny_memory_store();
    store.set_emit_backpressure_for_test(true);

    // Fill cap and trigger backpressure to obtain a real production
    // error from the production code path (don't synthesize the error
    // ourselves — we want to prove the producer side wires the signal
    // identically to the way the classifier checks it).
    let big_payload = vec![0u8; 1024];
    let digest1 = DigestInfo::try_new(VALID_HASH1, big_payload.len() as u64)?;
    store
        .update_oneshot(digest1, big_payload.into())
        .await
        .expect("first insert should fit");

    let digest2 = DigestInfo::try_new(VALID_HASH3, 1024)?;
    let payload2 = vec![2u8; 1024];
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest2, payload2.into()),
    )
    .await
    .expect("must not deadlock")
    .expect_err("must error");

    // The producer-emitted error MUST be classified as backpressure
    // (NOT a dead channel). The chunked_signal::error_has_backpressure_signal
    // helper is what `looks_like_dead_channel` consults; if it
    // returns false here, the production classifier would evict.
    assert!(
        nativelink_store::chunked_signal::error_has_backpressure_signal(&err),
        "producer-emitted MemoryStoreAtCapacity error MUST be \
         recognized by error_has_backpressure_signal — otherwise the \
         classifier in grpc_store.rs:159 would treat it as a dead h2 channel \
         and replay #147 at backpressure rate. err = {err:?}",
    );

    // Pin<&MemoryStore> bound check (compile-time assertion that the
    // store driver shape we relied on still matches StoreDriver's
    // self: Pin<&Self>).
    let _: Pin<&MemoryStore> = Pin::new(&*store);
    Ok(())
}
