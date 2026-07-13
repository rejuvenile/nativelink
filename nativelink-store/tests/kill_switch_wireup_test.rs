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

//! #212 production-config wire-up regression. Each test starts a
//! store with a `chunked_*_enabled: true` field and asserts the
//! corresponding runtime kill-switch is observably ON post-construction.
//!
//! Without these, a future schema rename / wire-up regression could
//! silently leave the production binary at the historical default-OFF
//! state — the JSON would parse, the operator would believe the flip
//! had landed, but the runtime would still be on the legacy path.
//!
//! Coverage:
//! - `FastSlowStore::new` honors `chunked_reads_enabled: true`.
//! - `MemoryStore::new` honors `emit_backpressure_enabled: true` (via
//!   the production over-capacity reject behaviour).
//!
//! The `GrpcStore::new` path requires either a live tonic server or
//! mock plumbing; coverage of the `chunked_writes_enabled` field's
//! plumbing lives in the schema test (`kill_switches_test.rs`) plus
//! the manual test at the worker-side smoke check.
//!
//! All tests gated on `chunked_fast_slow` — the runtime kill-switch
//! state machinery is only present under that feature.

#![cfg(feature = "chunked_fast_slow")]

use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;

#[nativelink_test]
async fn fast_slow_store_new_honors_chunked_reads_enabled_true() -> Result<(), Error> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let spec = FastSlowSpec {
        fast: StoreSpec::Memory(MemorySpec::default()),
        slow: StoreSpec::Memory(MemorySpec::default()),
        fast_direction: StoreDirection::default(),
        slow_direction: StoreDirection::default(),
        chunked_reads_enabled: true,
        slow_writes_in_flight_max_bytes: 0,
        bypass_dedup_threshold_bytes: 0,
    };
    let fss = FastSlowStore::new(&spec, fast, slow);
    assert!(
        fss.chunked_reads_enabled(),
        "FastSlowStore::new with chunked_reads_enabled=true MUST flip the runtime kill-switch ON; \
         a wire-up regression would leave production silently on the legacy read path"
    );
    Ok(())
}

#[nativelink_test]
async fn fast_slow_store_new_honors_chunked_reads_enabled_false() -> Result<(), Error> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let spec = FastSlowSpec {
        fast: StoreSpec::Memory(MemorySpec::default()),
        slow: StoreSpec::Memory(MemorySpec::default()),
        fast_direction: StoreDirection::default(),
        slow_direction: StoreDirection::default(),
        chunked_reads_enabled: false,
        slow_writes_in_flight_max_bytes: 0,
        bypass_dedup_threshold_bytes: 0,
    };
    let fss = FastSlowStore::new(&spec, fast, slow);
    assert!(
        !fss.chunked_reads_enabled(),
        "FastSlowStore::new with chunked_reads_enabled=false MUST leave the runtime kill-switch OFF"
    );
    Ok(())
}

#[nativelink_test]
async fn memory_store_new_honors_emit_backpressure_enabled_true() -> Result<(), Error> {
    // 1 KiB cap so a 2 KiB write deterministically trips the gate.
    let spec = MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: true,
    };
    let store = MemoryStore::new(&spec);
    let store_handle: Store = Store::new(store);

    // Insert a 2 KiB blob. With the kill-switch ON we MUST get
    // ResourceExhausted instead of silent eviction.
    let mut hash = [0u8; 32];
    hash[0] = 0xab;
    let digest = DigestInfo::new(hash, 2048);
    let payload = bytes::Bytes::from(vec![0u8; 2048]);
    let result = store_handle.update_oneshot(digest, payload).await;
    let err = result.expect_err(
        "MemoryStore with emit_backpressure_enabled=true MUST reject over-capacity writes; \
         a wire-up regression would silent-evict and the kill-switch would no-op",
    );
    assert_eq!(
        err.code,
        Code::ResourceExhausted,
        "expected ResourceExhausted (BackpressureSignal::MemoryStoreAtCapacity), got {err:?}"
    );
    Ok(())
}

#[nativelink_test]
async fn memory_store_new_honors_emit_backpressure_enabled_false() -> Result<(), Error> {
    // Same 1 KiB cap; default-OFF behaviour silently evicts.
    let spec = MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    };
    let store = MemoryStore::new(&spec);
    let store_handle: Store = Store::new(store);

    let mut hash = [0u8; 32];
    hash[0] = 0xcd;
    let digest = DigestInfo::new(hash, 2048);
    let payload = bytes::Bytes::from(vec![0u8; 2048]);
    store_handle.update_oneshot(digest, payload).await.expect(
        "MemoryStore with emit_backpressure_enabled=false MUST silent-evict (legacy behaviour); \
             a regression would surface as ResourceExhausted on what was previously an Ok path",
    );
    Ok(())
}
