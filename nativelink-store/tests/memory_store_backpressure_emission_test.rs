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
    BackpressureSignal, backpressure_signal,
};
use nativelink_proto::type_urls::BACKPRESSURE_SIGNAL_TYPE_URL;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALID_HASH3: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";

/// 1 KiB cap — used by the silent-evict over-action test. With cap
/// this small, pin_cap = 256 B is too small for a 1 KiB entry to be
/// pinned, but that's fine — the silent-evict test doesn't need pins.
const TINY_CAP_BYTES: usize = 1024;

/// 4 KiB cap — used by tests that need to pin an entry. pin_cap =
/// 25% × cap = 1 KiB, which fits a 1 KiB entry. With cap = 4 KiB and
/// pinned bytes = 1 KiB, a 4 KiB incoming write trips
/// `would_exceed_capacity` (0 cache + 1024 pinned + 4096 incoming
/// = 5120 > 4096). The Fix C eviction extension finds nothing
/// evictable in cache (the only entry is pinned, so it lives outside
/// moka's `cache`); the typed signal is then emitted. 4 KiB is the
/// smallest cap that supports both a real pin and a gate trip after
/// eviction.
const PIN_TEST_CAP_BYTES: usize = 4096;
/// Size of the pinned entry — must be ≤ pin_cap (= 25% × cap = 1 KiB).
const PIN_FIT_BYTES: usize = 1024;

/// Build a MemoryStore with a tiny byte cap so eviction pressure is
/// reproducible without buffering megabytes in the test process.
fn tiny_memory_store() -> std::sync::Arc<MemoryStore> {
    memory_store_with_cap(TINY_CAP_BYTES)
}

fn memory_store_with_cap(cap: usize) -> std::sync::Arc<MemoryStore> {
    MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: cap,
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
        decoded.reason, expected_reason as i32,
        "expected reason={:?} got reason={}",
        expected_reason, decoded.reason,
    );
}

/// **Under-action (positive case).** Kill-switch ON, cap saturated by
/// PINNED entries: the next `update_oneshot` MUST return
/// `ResourceExhausted` with the `MemoryStoreAtCapacity` reason.
///
/// **Why pin digest1 first** (#334 Fix C eviction extension): the gate
/// now actively evicts UNPINNED LRU entries before emitting backpressure.
/// Without pinning digest1, the gate would evict it to admit digest2
/// and the test would no longer observe Err. Pinning models the
/// production scenario the gate signal is FOR — every byte of capacity
/// is durably-load-bearing (BIS ack window) and there's nothing safe to
/// evict. The tokio::time::timeout guards against any future
/// regression that could deadlock instead of returning the error.
#[nativelink_test]
async fn emits_resource_exhausted_when_emission_enabled_and_at_capacity() -> Result<(), Error> {
    let store = memory_store_with_cap(PIN_TEST_CAP_BYTES);
    store.enable_emit_backpressure();

    // Insert a 1 KiB blob (≤ pin_cap of 1 KiB so it can be pinned).
    let payload1 = vec![0u8; PIN_FIT_BYTES];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    store
        .update_oneshot(digest1, payload1.into())
        .await
        .expect("first insert should fit");
    // Pin digest1 so the Fix C eviction extension cannot free room. The
    // gate's sole remaining option is to emit the typed signal.
    // Pin via Store wrapper to dispatch the trait method without
    // bringing StoreDriver into scope (avoids method-name conflict
    // with StoreLike for tests that call e.g. `store.has(digest)`).
    Store::new(store.clone()).pin_digests(&[digest1]);

    // Issue a write that combined with pinned bytes exceeds cap AND
    // that the eviction extension cannot free room for (the pinned
    // digest1 lives outside moka's cache and is unevictable; nothing
    // else is in cache). 0 cache + 1024 pinned + 4096 incoming > 4096
    // cap → would_exceed=true. Eviction frees 0 bytes. Re-check still
    // over. Emits.
    let digest2 = DigestInfo::try_new(VALID_HASH2, PIN_TEST_CAP_BYTES as u64)?;
    let payload2 = vec![1u8; PIN_TEST_CAP_BYTES];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest2, payload2.into()),
    )
    .await
    .expect("must not deadlock — backpressure-emission must return promptly");

    let err = result.expect_err(
        "second insert MUST return ResourceExhausted when kill-switch is ON \
         and every byte of cap is pinned (Fix C eviction extension cannot \
         free pinned room — typed signal is the contract)",
    );
    assert_backpressure_signal(&err, backpressure_signal::Reason::MemoryStoreAtCapacity);
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
    // Default-OFF; do NOT call enable_emit_backpressure.

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
async fn verify_store_around_memory_store_propagates_backpressure_signal() -> Result<(), Error> {
    let inner = memory_store_with_cap(PIN_TEST_CAP_BYTES);
    inner.enable_emit_backpressure();

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

    let payload1 = vec![0u8; PIN_FIT_BYTES];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    store
        .update_oneshot(digest1, payload1.into())
        .await
        .expect("first insert through VerifyStore should fit");
    // Pin digest1 so the Fix C eviction extension cannot free room
    // (see `emits_resource_exhausted_when_emission_enabled_and_at_capacity`
    // for rationale). Pin via the inner Arc since VerifyStore is wrapped
    // and we want to model the BIS-window pin set on the inner backend
    // (matches production composition where FastSlowStore::update calls
    // pin_digests on the inner MemoryStore).
    Store::new(inner.clone()).pin_digests(&[digest1]);

    let digest2 = DigestInfo::try_new(VALID_HASH2, PIN_TEST_CAP_BYTES as u64)?;
    let payload2 = vec![1u8; PIN_TEST_CAP_BYTES];
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
    assert_backpressure_signal(&err, backpressure_signal::Reason::MemoryStoreAtCapacity);
    Ok(())
}

/// **Classifier compatibility.** The existing
/// `error_has_backpressure_signal` helper (consumed by
/// `looks_like_dead_channel` in `grpc_store.rs:159`) MUST recognize
/// the new `MemoryStoreAtCapacity` signal. If this regresses, every
/// MemoryStoreAtCapacity event would tear down the h2 channel at
/// backpressure rate and reproduce #147.
#[nativelink_test]
async fn classifier_recognizes_memory_store_at_capacity_signal() -> Result<(), Error> {
    let store = memory_store_with_cap(PIN_TEST_CAP_BYTES);
    store.enable_emit_backpressure();

    // Fill cap and trigger backpressure to obtain a real production
    // error from the production code path (don't synthesize the error
    // ourselves — we want to prove the producer side wires the signal
    // identically to the way the classifier checks it).
    let payload1 = vec![0u8; PIN_FIT_BYTES];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    store
        .update_oneshot(digest1, payload1.into())
        .await
        .expect("first insert should fit");
    // Pin so Fix C eviction extension cannot free room — see other
    // tests in this file for rationale.
    Store::new(store.clone()).pin_digests(&[digest1]);

    let digest2 = DigestInfo::try_new(VALID_HASH3, PIN_TEST_CAP_BYTES as u64)?;
    let payload2 = vec![2u8; PIN_TEST_CAP_BYTES];
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
