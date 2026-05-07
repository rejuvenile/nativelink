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

//! Tests for `MemoryStore`.
//!
//! # #284 part 1 — early-reject mutation log
//!
//! CLAUDE.md "Test-first development" mandates the mutation step be
//! RUN — comment out the key line, observe the bespoke red-fail
//! message, restore. Without it, you cannot claim the test guards
//! the behavior. The four #284 part 1 tests in this file were each
//! mutation-verified on 2026-05-06; the outcomes are recorded here
//! so a future reviewer sees evidence rather than a bare claim.
//!
//! ## `update_rejects_upfront_when_declared_size_exceeds_capacity`
//! (under-action, unit boundary)
//!
//!   * **Mutation:** comment out the
//!     `if let UploadSizeInfo::ExactSize(declared) = size_info { ... }`
//!     block in `nativelink-store/src/memory_store.rs::update`.
//!   * **Observed:** test red-fails at line 507 with the bespoke
//!     `"Timeout means update() blocked on reader.recv() instead of
//!     returning ResourceExhausted upfront"` message; the 5 s
//!     `tokio::time::timeout` fires because `MemoryStore::update`
//!     enters its recv loop and blocks waiting for a chunk that
//!     never arrives.
//!
//! ## `update_rejects_upfront_under_verify_store_does_not_deadlock`
//! (under-action, production composition)
//!
//!   * **Mutation:** same block comment as above.
//!   * **Observed:** test red-fails with the bespoke `"must not
//!     deadlock — early-reject must propagate ResourceExhausted
//!     upfront through VerifyStore"` message; the 5 s timeout fires
//!     because the inner MemoryStore blocks in its recv loop and
//!     VerifyStore's `tokio::join!(update_fut, check_fut)` never
//!     unblocks.
//!
//! ## `update_accepts_exactly_capacity_size`
//! (over-action of the boundary predicate)
//!
//!   * **Mutation:** flip `>` to `>=` in
//!     `nativelink-util/src/moka_evicting_map.rs::would_exceed_capacity`
//!     (`current_bytes.saturating_add(incoming_kb_bytes) >=
//!     self.max_bytes`).
//!   * **Observed:** test red-fails at the bespoke `"exactly-fits
//!     MUST drain and land, NOT trigger ResourceExhausted"` message
//!     with `Code::ResourceExhausted` — the boundary `current=0,
//!     incoming=cap` flips from accept to reject.
//!
//! ## `update_max_size_does_not_early_reject_when_payload_fits`
//! (over-action of the early-reject pattern)
//!
//!   * **Mutation:** extend the if-let pattern in
//!     `memory_store.rs::update` to
//!     `if let UploadSizeInfo::ExactSize(declared) |
//!     UploadSizeInfo::MaxSize(declared) = size_info`.
//!   * **Observed:** test red-fails with the bespoke `"MaxSize
//!     early-reject MUST NOT fire when the actual payload fits in
//!     cap"` message — `would_exceed_capacity(3000) = true` (3000
//!     rounds to ≥ cap=2048) emits `MemoryStoreAtCapacity` for the
//!     declared upper bound even though the actual 50-byte payload
//!     would have fit.
//!
//! Each mutation was reverted and the test was re-confirmed green
//! before commit. See the per-test docstrings for the production-
//! composition rationale and the asymmetric-contract framing.

use core::ops::RangeBounds;
use core::pin::Pin;

use bytes::{BufMut, Bytes, BytesMut};
use memory_stats::memory_stats;
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::spawn;
use nativelink_util::store_trait::{StoreKey, StoreLike};
use pretty_assertions::assert_eq;
use sha2::{Digest, Sha256};

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALID_HASH3: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";
const VALID_HASH4: &str = "0123456789abcdef000000000000000000040000000000000123456789abcdef";
const TOO_LONG_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdefff";
const TOO_SHORT_HASH: &str = "100000000000000000000000000000000000000000000000000000000000001";
const INVALID_HASH: &str = "g111111111111111111111111111111111111111111111111111111111111111";

#[nativelink_test]
async fn insert_one_item_then_update() -> Result<(), Error> {
    const VALUE1: &str = "13";
    const VALUE2: &str = "23";
    let store = MemoryStore::new(&MemorySpec::default());

    // Insert dummy value into store.
    store
        .update_oneshot(
            DigestInfo::try_new(VALID_HASH1, VALUE1.len() as u64)?,
            VALUE1.into(),
        )
        .await?;
    assert_eq!(
        store
            .has(DigestInfo::try_new(VALID_HASH1, VALUE1.len() as u64)?)
            .await,
        Ok(Some(VALUE1.len() as u64)),
        "Expected memory store to have hash: {}",
        VALID_HASH1
    );

    let store_data = {
        // Now change value we just inserted.
        store
            .update_oneshot(
                DigestInfo::try_new(VALID_HASH1, VALUE2.len())?,
                VALUE2.into(),
            )
            .await?;
        store
            .get_part_unchunked(DigestInfo::try_new(VALID_HASH1, VALUE2.len())?, 0, None)
            .await?
    };

    assert_eq!(
        store_data,
        VALUE2.as_bytes(),
        "Hash for key: {} did not update. Expected: {:#x?}, but got: {:#x?}",
        VALID_HASH1,
        VALUE2,
        store_data
    );
    Ok(())
}

// Regression test for: https://github.com/TraceMachina/nativelink/issues/289.
#[nativelink_test]
async fn ensure_full_copy_of_bytes_is_made_test() -> Result<(), Error> {
    // Arbitrary value, this may be increased if we find out that this is
    // too low for some kernels/operating systems.
    const MAXIMUM_MEMORY_USAGE_INCREASE_PERC: f64 = 1.3; // 30% increase.
    const MAX_STATS_ITERATIONS: usize = 100;

    let mut sum_memory_usage_increase_perc: f64 = 0.0;
    for _ in 0..MAX_STATS_ITERATIONS {
        let store_owned = MemoryStore::new(&MemorySpec::default());
        let store = Pin::new(&store_owned);

        let initial_virtual_mem = memory_stats()
            .err_tip(|| "Failed to read memory.")?
            .physical_mem;
        for (i, hash) in [VALID_HASH1, VALID_HASH2, VALID_HASH3, VALID_HASH4]
            .into_iter()
            .enumerate()
        {
            // User a variety of sizes increasing up to 10MB each iteration.
            // We do this to reduce the chance of memory page size masking the potential bug.
            let reserved_size = 10_usize.pow(u32::try_from(i).expect("Cast failed") + 4);
            let mut mut_data = BytesMut::with_capacity(reserved_size);
            mut_data.put_bytes(u8::try_from(i).expect("Cast failed"), 1);
            let data = mut_data.freeze();

            let digest = DigestInfo::try_new(hash, data.len())?;
            store
                .update_oneshot(digest, data)
                .await
                .err_tip(|| "Could not update store")?;
        }

        let new_virtual_mem = memory_stats()
            .err_tip(|| "Failed to read memory.")?
            .physical_mem;
        sum_memory_usage_increase_perc += new_virtual_mem as f64 / initial_virtual_mem as f64;
    }
    assert!(
        (sum_memory_usage_increase_perc / MAX_STATS_ITERATIONS as f64)
            < MAXIMUM_MEMORY_USAGE_INCREASE_PERC,
        "Memory usage increased by {sum_memory_usage_increase_perc} perc, which is more than {MAXIMUM_MEMORY_USAGE_INCREASE_PERC} perc",
    );
    Ok(())
}

#[nativelink_test]
async fn read_partial() -> Result<(), Error> {
    const VALUE1: &str = "1234";
    let store_owned = MemoryStore::new(&MemorySpec::default());
    let store = Pin::new(&store_owned);

    let digest = DigestInfo::try_new(VALID_HASH1, 4).unwrap();
    store.update_oneshot(digest, VALUE1.into()).await?;

    let store_data = store.get_part_unchunked(digest, 1, Some(2)).await?;

    assert_eq!(
        &VALUE1.as_bytes()[1..3],
        store_data,
        "Expected partial data to match, expected '{:#x?}' got: {:#x?}'",
        &VALUE1.as_bytes()[1..3],
        store_data,
    );
    Ok(())
}

// A bug was found where reading an empty value from memory store would result in an error
// due to internal EOF handling. This is an edge case test.
#[nativelink_test]
async fn read_zero_size_item_test() -> Result<(), Error> {
    const VALUE: &str = "";
    let store_owned = MemoryStore::new(&MemorySpec::default());
    let store = Pin::new(&store_owned);

    // Insert dummy value into store.
    store
        .update_oneshot(DigestInfo::try_new(VALID_HASH1, VALUE.len())?, VALUE.into())
        .await?;
    assert_eq!(
        store
            .get_part_unchunked(DigestInfo::try_new(VALID_HASH1, VALUE.len())?, 0, None,)
            .await,
        Ok("".into()),
        "Expected memory store to have empty value"
    );
    Ok(())
}

#[nativelink_test]
async fn errors_with_invalid_inputs() -> Result<(), Error> {
    const VALUE1: &str = "123";
    let store_owned = MemoryStore::new(&MemorySpec::default());
    let store = Pin::new(store_owned.as_ref());
    {
        // .has() tests.
        async fn has_should_fail(store: Pin<&MemoryStore>, hash: &str, expected_size: usize) {
            let digest = DigestInfo::try_new(hash, expected_size);
            assert!(
                digest.is_err() || store.has(digest.unwrap()).await.is_err(),
                ".has() should have failed: {hash} {expected_size}",
            );
        }
        has_should_fail(store, TOO_LONG_HASH, VALUE1.len()).await;
        has_should_fail(store, TOO_SHORT_HASH, VALUE1.len()).await;
        has_should_fail(store, INVALID_HASH, VALUE1.len()).await;
    }
    {
        // .update() tests.
        async fn update_should_fail<'a>(
            store: Pin<&'a MemoryStore>,
            hash: &'a str,
            expected_size: usize,
            value: &'static str,
        ) {
            let digest = DigestInfo::try_new(hash, expected_size);
            assert!(
                digest.is_err()
                    || store
                        .update_oneshot(digest.unwrap(), value.into(),)
                        .await
                        .is_err(),
                ".has() should have failed: {hash} {expected_size} {value}",
            );
        }
        update_should_fail(store, TOO_LONG_HASH, VALUE1.len(), VALUE1).await;
        update_should_fail(store, TOO_SHORT_HASH, VALUE1.len(), VALUE1).await;
        update_should_fail(store, INVALID_HASH, VALUE1.len(), VALUE1).await;
    }
    {
        // .update() tests.
        async fn get_should_fail<'a>(
            store: Pin<&'a MemoryStore>,
            hash: &'a str,
            expected_size: usize,
        ) {
            let digest = DigestInfo::try_new(hash, expected_size);
            assert!(
                digest.is_err()
                    || store
                        .get_part_unchunked(digest.unwrap(), 0, None)
                        .await
                        .is_err(),
                ".get() should have failed: {hash} {expected_size}",
            );
        }

        get_should_fail(store, TOO_LONG_HASH, 1).await;
        get_should_fail(store, TOO_SHORT_HASH, 1).await;
        get_should_fail(store, INVALID_HASH, 1).await;
        // With an empty store .get() should fail too.
        get_should_fail(store, VALID_HASH1, 1).await;
    }
    Ok(())
}

#[nativelink_test]
async fn get_part_is_zero_digest() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);

    let store = MemoryStore::new(&MemorySpec::default());
    let store_clone = store.clone();
    let (mut writer, mut reader) = make_buf_channel_pair();

    let _drop_guard = spawn!("get_part_is_zero_digest", async move {
        drop(
            Pin::new(store_clone.as_ref())
                .get_part(digest, &mut writer, 0, None)
                .await
                .err_tip(|| "Failed to get_part"),
        );
    });

    let file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading bytes")?;

    let empty_bytes = Bytes::new();
    assert_eq!(&file_data, &empty_bytes, "Expected file content to match");

    Ok(())
}

#[nativelink_test]
async fn has_with_results_on_zero_digests() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let keys = vec![digest.into()];
    let mut results = vec![None];

    let store_owned = MemoryStore::new(&MemorySpec::default());
    let store = Pin::new(&store_owned);

    drop(
        store
            .as_ref()
            .has_with_results(&keys, &mut results)
            .await
            .err_tip(|| "Failed to get_part"),
    );
    assert_eq!(results, vec![Some(0)]);

    Ok(())
}

#[nativelink_test]
async fn list_test() -> Result<(), Error> {
    async fn get_list(
        store: &MemoryStore,
        range: impl RangeBounds<StoreKey<'static>> + Send + Sync + 'static,
    ) -> Vec<StoreKey<'static>> {
        let mut found_keys = vec![];
        store
            .list(range, |key| {
                found_keys.push(key.borrow().into_owned());
                true
            })
            .await
            .unwrap();
        found_keys
    }

    const KEY1: StoreKey = StoreKey::new_str("key1");
    const KEY2: StoreKey = StoreKey::new_str("key2");
    const KEY3: StoreKey = StoreKey::new_str("key3");
    const VALUE: &str = "value1";

    let store = MemoryStore::new(&MemorySpec::default());
    store.update_oneshot(KEY1, VALUE.into()).await?;
    store.update_oneshot(KEY2, VALUE.into()).await?;
    store.update_oneshot(KEY3, VALUE.into()).await?;

    {
        // Test listing all keys.
        let keys = get_list(&store, ..).await;
        assert_eq!(keys, vec![KEY1, KEY2, KEY3]);
    }
    {
        // Test listing from key1 to all.
        let keys = get_list(&store, KEY1..).await;
        assert_eq!(keys, vec![KEY1, KEY2, KEY3]);
    }
    {
        // Test listing from key1 to key2.
        let keys = get_list(&store, KEY1..KEY2).await;
        assert_eq!(keys, vec![KEY1]);
    }
    {
        // Test listing from key1 including key2.
        let keys = get_list(&store, KEY1..=KEY2).await;
        assert_eq!(keys, vec![KEY1, KEY2]);
    }
    {
        // Test listing from key1 to key3.
        let keys = get_list(&store, KEY1..KEY3).await;
        assert_eq!(keys, vec![KEY1, KEY2]);
    }
    {
        // Test listing from all to key2.
        let keys = get_list(&store, ..KEY2).await;
        assert_eq!(keys, vec![KEY1]);
    }
    {
        // Test listing from key2 to key3.
        let keys = get_list(&store, KEY2..KEY3).await;
        assert_eq!(keys, vec![KEY2]);
    }

    Ok(())
}

#[nativelink_test]
async fn update_rejects_partial_write_with_exact_size() -> Result<(), Error> {
    // Regression test: if update() receives fewer bytes than ExactSize
    // declares, it must NOT insert the partial entry. A truncated upstream
    // (e.g., Redis timeout dropping the channel) would otherwise poison
    // the cache, causing all future reads to serve truncated data.
    use nativelink_util::store_trait::UploadSizeInfo;

    let store = std::sync::Arc::new(MemoryStore::new(&MemorySpec::default()));
    let digest = DigestInfo::try_new(
        "0123456789abcdef000000000000000000000000000000000123456789abcdef",
        100_000,
    )
    .unwrap();

    let (mut tx, rx) = make_buf_channel_pair();
    let store_pin = Pin::new(store.as_ref());
    let update_fut = store_pin.update(
        StoreKey::from(digest),
        rx,
        UploadSizeInfo::ExactSize(100_000),
    );
    let send_fut = async {
        tx.send(Bytes::from(vec![42u8; 1000])).await?;
        tx.send_eof()?;
        Ok::<_, Error>(())
    };
    let (update_res, send_res) = tokio::join!(update_fut, send_fut);
    send_res?;

    assert!(
        update_res.is_err(),
        "Expected update to reject partial write (1000 bytes vs 100000 declared)"
    );

    let has = Pin::new(store.as_ref())
        .has(StoreKey::from(digest))
        .await?;
    assert!(
        has.is_none(),
        "Store should not contain partial entry after rejected write, but has() returned {has:?}"
    );

    Ok(())
}

#[nativelink_test]
async fn update_accepts_correct_size_with_exact_size() -> Result<(), Error> {
    use nativelink_util::store_trait::UploadSizeInfo;

    let store = std::sync::Arc::new(MemoryStore::new(&MemorySpec::default()));
    let data = vec![42u8; 5000];
    let digest = DigestInfo::try_new(
        "0123456789abcdef000000000000000000000000000000000123456789abcdef",
        data.len() as u64,
    )
    .unwrap();

    let (mut tx, rx) = make_buf_channel_pair();
    let store_pin = Pin::new(store.as_ref());
    let update_fut = store_pin.update(
        StoreKey::from(digest),
        rx,
        UploadSizeInfo::ExactSize(data.len() as u64),
    );
    let send_fut = async {
        tx.send(Bytes::from(data.clone())).await?;
        tx.send_eof()?;
        Ok::<_, Error>(())
    };
    let (update_res, send_res) = tokio::join!(update_fut, send_fut);
    send_res?;
    update_res?;

    let result = Pin::new(store.as_ref())
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(result.as_ref(), data.as_slice());

    Ok(())
}

/// #284 part 1 — early-reject upfront when the declared upload size
/// would exceed capacity, BEFORE pulling any chunk off the wire.
///
/// **Wasted-work contract.** Before this fix, `MemoryStore::update`
/// drained the entire upload stream into a `Vec<Bytes>` and only THEN
/// rejected via `check_backpressure_gate(total_bytes)`. At ~5
/// over-capacity rejections/sec in production (18,692/hour during
/// today's incident), the pulled-then-thrown bytes amplified CPU,
/// network, and allocation load. The early-reject saves the entire
/// recv loop on the unhappy path.
///
/// **Production composition.** The sibling test
/// `update_rejects_upfront_under_verify_store_does_not_deadlock`
/// exercises the same condition through `VerifyStore::new(verify_size
/// = true, backend: memory)` — the historical `cas_STORE` shape —
/// to assert that the upstream wrapper surfaces the same
/// `ResourceExhausted` without mangling the classifier-visible detail
/// AND without deadlocking the wrapper's `tokio::join!(update_fut,
/// check_fut)` over its borrowed channel.
///
/// **Mutation step.** Comment out the `check_backpressure_gate` call
/// in `MemoryStore::update`'s early-reject block (`memory_store.rs`,
/// `if let UploadSizeInfo::ExactSize(declared) = size_info { ... }`).
/// The test below MUST red-fail with the bespoke message — a generic
/// `is_err()` would mask a `tokio::time::Elapsed` instead of catching
/// "function blocked on recv() instead of returning early."
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn update_rejects_upfront_when_declared_size_exceeds_capacity() -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_config::stores::EvictionPolicy;
    use nativelink_util::store_trait::UploadSizeInfo;

    // 1 KiB cap — same shape as `memory_store_backpressure_emission_test`.
    let store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    store.enable_emit_backpressure();

    // Fill the cap so any further write would force eviction.
    let payload1_len: u64 = 1024;
    let payload1 = vec![0u8; payload1_len as usize];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1_len)?;
    store
        .update_oneshot(digest1, payload1.into())
        .await
        .expect("first insert should fit");

    // Construct a buf-channel pair but NEVER send a chunk and NEVER
    // close the channel. If the early-reject works, `update` returns
    // `Err(ResourceExhausted)` BEFORE polling `recv()`. If the
    // early-reject is broken, `update` blocks on `reader.recv()`
    // forever — the `tokio::time::timeout` distinguishes the two.
    let (_tx, rx) = make_buf_channel_pair();
    let digest2 = DigestInfo::try_new(VALID_HASH2, 1024)?;
    let store_pin = Pin::new(store.as_ref());
    let update_fut = store_pin.update(
        StoreKey::from(digest2),
        rx,
        UploadSizeInfo::ExactSize(1024),
    );

    let result = tokio::time::timeout(Duration::from_secs(5), update_fut)
        .await
        .expect(
            "MemoryStore must reject over-capacity uploads at first byte, \
             not after drain — wasted-work contract violated. \
             Timeout means update() blocked on reader.recv() instead of \
             returning ResourceExhausted upfront.",
        );

    let err = result.expect_err(
        "ExactSize upload that alone exceeds capacity MUST return \
         ResourceExhausted before pulling any chunk off the wire",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::ResourceExhausted,
        "expected ResourceExhausted from early-reject, got code={:?} messages={:?}",
        err.code,
        err.messages,
    );

    // The store must contain only the original entry — the rejected
    // upload must have written nothing, and (more importantly for this
    // test) MUST have left the original entry intact (no silent
    // eviction snuck through the early-reject gate).
    let has1 = Pin::new(store.as_ref()).has(StoreKey::from(digest1)).await?;
    assert_eq!(
        has1,
        Some(payload1_len),
        "original entry MUST remain after early-reject — rejecting upfront \
         must not silently evict the very entry we are protecting"
    );
    let has2 = Pin::new(store.as_ref()).has(StoreKey::from(digest2)).await?;
    assert!(
        has2.is_none(),
        "rejected upload MUST NOT land in the store, got has2={has2:?}"
    );

    Ok(())
}

/// #284 part 1 — happy-path complement of the early-reject test:
/// when the declared size DOES fit, the recv loop runs and the blob
/// lands. Guards against the early-reject regressing into "always
/// reject" — the over-action sibling of the under-action above.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn update_drains_when_declared_size_fits() -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_config::stores::EvictionPolicy;
    use nativelink_util::store_trait::UploadSizeInfo;

    // 4 KiB cap — plenty of headroom for a 1 KiB upload.
    let store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 4096,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    store.enable_emit_backpressure();

    let data = vec![7u8; 1024];
    let digest = DigestInfo::try_new(VALID_HASH1, data.len() as u64)?;

    let (mut tx, rx) = make_buf_channel_pair();
    let store_pin = Pin::new(store.as_ref());
    let update_fut = store_pin.update(
        StoreKey::from(digest),
        rx,
        UploadSizeInfo::ExactSize(data.len() as u64),
    );
    let send_fut = async {
        tx.send(Bytes::from(data.clone())).await?;
        tx.send_eof()?;
        Ok::<_, Error>(())
    };
    let (update_res, send_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(update_fut, send_fut) },
    )
    .await
    .expect("must not deadlock — fitting upload should drain promptly");
    send_res?;
    update_res.expect(
        "ExactSize upload within capacity MUST drain and land in the store. \
         A failure here means the early-reject gate is over-firing on \
         uploads that fit — the over-action sibling of #284 part 1.",
    );

    let landed = Pin::new(store.as_ref())
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(landed.as_ref(), data.as_slice());

    Ok(())
}

/// #284 part 1 — production-composition coverage of the early-reject
/// gate. The historical `cas_STORE` chain wraps the inner store in a
/// `VerifyStore` (`verify_size = true`); that wrapper runs
/// `tokio::join!(update_fut, check_fut)` over an internal tx/rx pair.
/// If the early-reject inside `MemoryStore::update` left any borrowed
/// channel un-terminated, this composition would deadlock — the unit-
/// boundary tests above own the rx and would never see it.
///
/// The asymmetric-contract sibling of `update_rejects_upfront_when_
/// declared_size_exceeds_capacity`: the unit test guards "function
/// returns Err"; this composition test guards "no caller above the
/// store deadlocks because of how it returned." Both directions of the
/// borrowed-state contract are exercised under a `tokio::time::
/// timeout(5s)` deadlock detector with bespoke `.expect(...)` messages
/// so an `Elapsed` cannot masquerade as a real Err.
///
/// We send 1 chunk on the producer side WITHOUT EOF and never close
/// the channel. With the early-reject in place: `MemoryStore::update`
/// returns `ResourceExhausted` immediately on the declared 1024-byte
/// `ExactSize`, drops the inner-store rx, which causes VerifyStore's
/// `inner_check_update` to fail its `tx.send` on the next chunk and
/// return — the `tokio::join!` then unblocks. Without the early-
/// reject (mutation), `MemoryStore::update` enters its recv loop and
/// blocks forever (no EOF coming), VerifyStore's `check_fut` blocks
/// forever, and the 5 s timeout fires with the bespoke message —
/// proving the test guards the contract.
///
/// **Mutation step** (executed 2026-05-06): commenting out the
/// `if let UploadSizeInfo::ExactSize(declared) = size_info { ... }`
/// block in `memory_store.rs` red-fails this test with the bespoke
/// "must not deadlock — early-reject must propagate ResourceExhausted
/// upfront through VerifyStore" message at the 5 s timeout, confirming
/// the production-composition contract is guarded.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn update_rejects_upfront_under_verify_store_does_not_deadlock(
) -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_config::stores::{EvictionPolicy, StoreSpec, VerifySpec};
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::store_trait::{Store, UploadSizeInfo};

    // 1 KiB cap — same shape as the unit-boundary test above.
    let inner = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    inner.enable_emit_backpressure();

    // Fill the cap so any further write would force eviction.
    let payload1_len: u64 = 1024;
    let payload1 = vec![0u8; payload1_len as usize];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1_len)?;
    inner
        .update_oneshot(digest1, payload1.into())
        .await
        .expect("first insert should fit");

    // Wrap in VerifyStore with verify_size=true — the production
    // `cas_STORE` shape that joins `update_fut` and `check_fut` over
    // an internal channel. The `backend` spec is metadata; the
    // `inner_store` argument is what's actually wired in.
    let verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner.clone()),
    );

    let digest2 = DigestInfo::try_new(VALID_HASH2, 1024)?;
    let (mut tx, rx) = make_buf_channel_pair();
    let verify_pin = Pin::new(verify.as_ref());
    let update_fut = verify_pin.update(
        StoreKey::from(digest2),
        rx,
        UploadSizeInfo::ExactSize(1024),
    );
    // Producer: send a single chunk to give VerifyStore's
    // `inner_check_update` a chunk to forward to the inner store.
    // With the early-reject in place, the inner store's rx is dropped
    // before the forward completes, the forward fails, and the join
    // unblocks. WITHOUT the early-reject, the inner MemoryStore would
    // block in its recv loop waiting for EOF that never arrives, and
    // VerifyStore's join would hang — the 5 s timeout below catches
    // that mutation. We do NOT call `send_eof` so a buggy MemoryStore
    // cannot escape via the post-drain ExactSize-mismatch path.
    let send_fut = async {
        // Best-effort send; if the inner store dropped its rx the
        // send fails — we ignore it and just hold tx alive below.
        drop(tx.send(Bytes::from_static(b"x")).await);
        // Hold tx alive — never close. If the early-reject works, the
        // producer side will be cancelled once the join finishes.
        core::future::pending::<()>().await;
        Ok::<_, Error>(())
    };

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            r = update_fut => r,
            _ = send_fut => unreachable!("pending future"),
        }
    })
    .await
    .expect(
        "VerifyStore-wrapped MemoryStore must reject over-capacity \
         uploads at first byte, not after drain — must not deadlock — \
         early-reject must propagate ResourceExhausted upfront through \
         VerifyStore. Timeout means the inner MemoryStore blocked on \
         its recv loop and VerifyStore's tokio::join! never unblocked.",
    );

    let err = result.expect_err(
        "ExactSize upload that alone exceeds capacity MUST surface as \
         Err through VerifyStore — the wrapper preserved the inner \
         store's rejection, not silently dropped it",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::ResourceExhausted,
        "VerifyStore wrap MUST forward MemoryStore's \
         ResourceExhausted code without mangling — got code={:?} \
         messages={:?}. The classifier (looks_like_dead_channel in \
         grpc_store.rs) sees this wire shape and decides retry vs \
         terminal; any other code breaks the wire-level contract.",
        err.code,
        err.messages,
    );

    // The store must NOT have admitted the rejected upload.
    let has2 = Pin::new(inner.as_ref())
        .has(StoreKey::from(digest2))
        .await?;
    assert!(
        has2.is_none(),
        "rejected upload MUST NOT land in the store (even when wrapped \
         by VerifyStore), got has2={has2:?}"
    );

    Ok(())
}

/// #284 part 1 — boundary coverage of the early-reject predicate at
/// the "exactly-fits" cliff edge. `would_exceed_capacity` uses `>`
/// (strict greater-than) on the post-insert KB-rounded weight; the
/// boundary `current=0, incoming=cap` evaluates `cap > cap = false`
/// and is therefore an ACCEPTABLE write. An off-by-one mutation
/// (`>` → `>=`) would silently flip that boundary into a rejection
/// without any other test catching it: every other ExactSize test
/// uses cap-with-room or strict over-capacity.
///
/// We drive cap=1024, current=0, ExactSize(1024) through the empty
/// store with backpressure armed, expect success, and assert the
/// blob lands. With the off-by-one mutation:
/// `would_exceed_capacity(1024)` returns `0 + 1024 >= 1024 = true`,
/// `check_backpressure_gate` raises `ResourceExhausted`, and this
/// test red-fails on the bespoke `.expect(...)` message.
///
/// **Mutation step** (executed 2026-05-06): flipping `>` to `>=` in
/// `MokaEvictingMap::would_exceed_capacity` red-fails this test with
/// the "exactly-fits MUST drain and land" message; restored and
/// re-confirmed green.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn update_accepts_exactly_capacity_size() -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_config::stores::EvictionPolicy;
    use nativelink_util::store_trait::UploadSizeInfo;

    // 1 KiB cap, empty store. ExactSize(1024) is the exact boundary.
    let store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    store.enable_emit_backpressure();

    let data = vec![3u8; 1024];
    let digest = DigestInfo::try_new(VALID_HASH1, data.len() as u64)?;

    let (mut tx, rx) = make_buf_channel_pair();
    let store_pin = Pin::new(store.as_ref());
    let update_fut = store_pin.update(
        StoreKey::from(digest),
        rx,
        UploadSizeInfo::ExactSize(data.len() as u64),
    );
    let send_fut = async {
        tx.send(Bytes::from(data.clone())).await?;
        tx.send_eof()?;
        Ok::<_, Error>(())
    };
    let (update_res, _send_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(update_fut, send_fut) },
    )
    .await
    .expect(
        "exactly-fits upload must not deadlock — would_exceed_capacity \
         must use strict greater-than at the boundary so a 1024-byte \
         insert into a 1024-byte cap is accepted",
    );
    // Check update_res FIRST. The producer-side `send_res` may fail with
    // "receiver disconnected" if MemoryStore early-rejects (the inner rx
    // is dropped before our send completes); that derivative error
    // would mask the real bespoke message below if we propagated it
    // first via `?`.
    update_res.expect(
        "ExactSize upload that exactly equals capacity (current=0, \
         incoming=cap) MUST drain and land in the store. A failure \
         here means `would_exceed_capacity` regressed from `>` to `>=` \
         (or some equivalent off-by-one) — exactly-fits MUST drain \
         and land, NOT trigger ResourceExhausted.",
    );

    let landed = Pin::new(store.as_ref())
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(landed.as_ref(), data.as_slice());

    Ok(())
}

/// #284 part 1 — MaxSize over-action guard. `MemoryStore::update`'s
/// early-reject `if let UploadSizeInfo::ExactSize(declared) = size_info`
/// pattern intentionally SKIPS `MaxSize`: the actual payload may be
/// smaller than the declared upper bound, so an upfront reject would
/// be a false positive. Asymmetric-contract sibling of the under-
/// action ExactSize test: this test guards the OVER-action direction
/// — the early-reject must NOT fire on `MaxSize` even when
/// `MaxSize.declared` exceeds capacity.
///
/// Setup: cap=2048 bytes (must be >= 1024 since the weigher rounds to
/// KB granularity; otherwise even a 50-byte post-drain check would
/// reject). Empty store. `MaxSize(3000)` declares an upper bound that
/// exceeds the cap, but the actual payload is only 50 bytes — small
/// enough that the post-drain check passes (50 rounds up to 1024,
/// 1024 ≤ 2048). With the current correct code, `MaxSize` does not
/// match the early-reject if-let, the recv loop runs, the payload
/// drains, and the blob lands.
///
/// Mutation: extending the if-let pattern to also match `MaxSize`
/// (e.g. `if let UploadSizeInfo::ExactSize(declared) |
/// UploadSizeInfo::MaxSize(declared) = size_info`) would early-reject
/// on declared=3000 → `would_exceed_capacity(3000) = true` →
/// `ResourceExhausted` returned BEFORE the recv loop, even though
/// the actual payload would have fit. This test red-fails on that
/// mutation.
///
/// **Mutation step** (executed 2026-05-06): adding `| UploadSizeInfo::
/// MaxSize(declared)` to the if-let pattern in `memory_store.rs::
/// update` red-fails this test with the bespoke "MaxSize early-reject
/// MUST NOT fire" message; restored and re-confirmed green.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn update_max_size_does_not_early_reject_when_payload_fits(
) -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_config::stores::EvictionPolicy;
    use nativelink_util::store_trait::UploadSizeInfo;

    // 2 KiB cap. Payload is 50 bytes (rounds to 1 KiB; well within
    // cap). Declared MaxSize is 3000 (exceeds cap, but is only an
    // upper bound — the early-reject MUST skip it).
    let store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 2048,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    store.enable_emit_backpressure();

    let data = vec![9u8; 50];
    let digest = DigestInfo::try_new(VALID_HASH1, data.len() as u64)?;

    let (mut tx, rx) = make_buf_channel_pair();
    let store_pin = Pin::new(store.as_ref());
    let update_fut = store_pin.update(
        StoreKey::from(digest),
        rx,
        UploadSizeInfo::MaxSize(3000),
    );
    let send_fut = async {
        tx.send(Bytes::from(data.clone())).await?;
        tx.send_eof()?;
        Ok::<_, Error>(())
    };
    let (update_res, _send_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(update_fut, send_fut) },
    )
    .await
    .expect(
        "MaxSize upload that fits in cap must not deadlock — the \
         early-reject gate MUST skip MaxSize",
    );
    // Check update_res first (see boundary test for the rationale).
    update_res.expect(
        "MaxSize early-reject MUST NOT fire when the actual payload \
         fits in cap, even if MaxSize.declared exceeds cap. A failure \
         here means the if-let pattern in `MemoryStore::update`'s \
         early-reject was extended to also match MaxSize — that's an \
         over-action regression: MaxSize is an UPPER BOUND on the \
         payload, not a declared exact size, so an upfront reject is \
         a false positive when the actual payload would fit.",
    );

    let landed = Pin::new(store.as_ref())
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(landed.as_ref(), data.as_slice());

    Ok(())
}
