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

use futures::future::pending;
use futures::try_join;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec, VerifySpec};
use nativelink_error::{Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::spawn;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use opentelemetry::context::FutureExt;
use pretty_assertions::assert_eq;
use tracing::{Instrument, info_span};

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

#[nativelink_test]
async fn verify_size_false_passes_on_update() -> Result<(), Error> {
    const VALUE1: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let result = store.update_oneshot(digest, VALUE1.into()).await;
    assert_eq!(
        result,
        Ok(()),
        "Should have succeeded when verify_size = false, got: {:?}",
        result
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE1.len() as u64)),
        "Expected data to exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_size_true_fails_on_update() -> Result<(), Error> {
    const VALUE1: &str = "123";
    const EXPECTED_ERR: &str = "Expected size 100 but got size 3 on insert";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let (mut tx, rx) = make_buf_channel_pair();
    let send_fut = async move {
        tx.send(VALUE1.into()).await?;
        tx.send_eof()
    };
    let result = try_join!(
        send_fut,
        store.update(digest, rx, UploadSizeInfo::ExactSize(100))
    );
    assert!(result.is_err(), "Expected error, got: {:?}", &result);
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains(EXPECTED_ERR),
        "Error should contain '{EXPECTED_ERR}', got: {err:?}"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_size_true_succeeds_on_update() -> Result<(), Error> {
    const VALUE1: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    let result = store.update_oneshot(digest, VALUE1.into()).await;
    assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE1.len() as u64)),
        "Expected data to exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_size_true_succeeds_on_multi_chunk_stream_update() -> Result<(), Error> {
    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    let (mut tx, rx) = make_buf_channel_pair();

    let digest = DigestInfo::try_new(VALID_HASH1, 6).unwrap();
    let future = spawn!(
        "verify_size_true_succeeds_on_multi_chunk_stream_update",
        async move {
            Pin::new(&store)
                .update(digest, rx, UploadSizeInfo::ExactSize(6))
                .await
        },
    );
    tx.send("foo".into()).await?;
    tx.send("bar".into()).await?;
    tx.send_eof()?;
    let result = future.await.err_tip(|| "Failed to join spawn future")?;
    assert_eq!(result, Ok(6), "Expected success, got: {:?}", result);
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(6)),
        "Expected data to exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_sha256_hash_true_succeeds_on_update() -> Result<(), Error> {
    /// This value is sha256("123").
    const HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    const VALUE: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, 3).unwrap();
    let result = store.update_oneshot(digest, VALUE.into()).await;
    assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE.len() as u64)),
        "Expected data to exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_sha256_hash_true_fails_on_update() -> Result<(), Error> {
    /// This value is sha256("12").
    const HASH: &str = "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918";
    const VALUE: &str = "123";
    const ACTUAL_HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, 3).unwrap();
    let result = store.update_oneshot(digest, VALUE.into()).await;
    // `expect_err`, not `unwrap_err`: this is a genuine FAIL-OPEN guard —
    // under a mutation that lets VerifyStore accept an unprovable blob it is
    // one of the assertions that fires, and a bare `unwrap_err()`-on-`Ok`
    // panic says only "called Result::unwrap_err() on an Ok value: ()",
    // which names neither the contract nor the consequence.
    let err = result
        .expect_err(
            "VerifyStore FAIL-CLOSED: a blob whose bytes do not reproduce its declared digest \
             under the labelled digest function (SHA-256 here) must be REJECTED. This write was \
             ACCEPTED, which means hash verification degraded into 'accept anyway' — a fail-open \
             bypass of the CAS integrity contract that lets arbitrary bytes land under an \
             attacker-chosen digest",
        )
        .to_string();
    let expected_err =
        format!("Hashes do not match, got: {HASH} but digest hash was {ACTUAL_HASH}");
    assert!(
        err.contains(&expected_err),
        "Error should contain '{expected_err}', got: {err:?}"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_blake3_hash_true_succeeds_on_update() -> Result<(), Error> {
    /// This value is blake3("123").
    const HASH: &str = "b3d4f8803f7e24b8f389b072e75477cdbcfbe074080fb5e500e53e26e054158e";
    const VALUE: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, 3).unwrap();

    let result = store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("update_oneshot"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await;

    assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE.len() as u64)),
        "Expected data to exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_blake3_hash_true_fails_on_update() -> Result<(), Error> {
    /// This value is blake3("12").
    const HASH: &str = "b944a0a3b20cf5927e594ff306d256d16cd5b0ba3e27b3285f40d7ef5e19695b";
    const VALUE: &str = "123";
    const ACTUAL_HASH: &str = "b3d4f8803f7e24b8f389b072e75477cdbcfbe074080fb5e500e53e26e054158e";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, 3).unwrap();

    let result = store
        .update_oneshot(digest, VALUE.into())
        .instrument(info_span!("update_oneshot"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await;

    // let result = store.update_oneshot(digest, VALUE.into()).await;
    // `expect_err`, not `unwrap_err` — same fail-open guard duty as
    // `verify_sha256_hash_true_fails_on_update` above, one function over.
    let err = result
        .expect_err(
            "VerifyStore FAIL-CLOSED: a blob whose bytes do not reproduce its declared digest \
             under the labelled digest function (BLAKE3 here) must be REJECTED. This write was \
             ACCEPTED, which means hash verification degraded into 'accept anyway' — a fail-open \
             bypass of the CAS integrity contract that lets arbitrary bytes land under an \
             attacker-chosen digest",
        )
        .to_string();
    let expected_err =
        format!("Hashes do not match, got: {HASH} but digest hash was {ACTUAL_HASH}");
    assert!(
        err.contains(&expected_err),
        "Error should contain '{expected_err}', got: {err:?}"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store after update"
    );
    Ok(())
}

// A potential bug could happen if the down stream component ignores the EOF but will
// stop receiving data when the expected size is reached. We should ensure this edge
// case is double protected.
#[nativelink_test]
async fn verify_fails_immediately_on_too_much_data_sent_update() -> Result<(), Error> {
    const VALUE: &str = "123";
    const EXPECTED_ERR: &str = "Expected size 4 but already received 6 on insert";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(VALID_HASH1, 4).unwrap();
    let (mut tx, rx) = make_buf_channel_pair();
    let send_fut = async move {
        tx.send(VALUE.into()).await?;
        tx.send(VALUE.into()).await?;
        pending::<()>().await;
        panic!("Should not reach here");
        #[expect(unreachable_code, reason = "needed to avoid inference errors")]
        Ok(())
    };
    let result = try_join!(
        send_fut,
        store.update(digest, rx, UploadSizeInfo::ExactSize(4))
    );
    assert!(result.is_err(), "Expected error, got: {:?}", &result);
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains(EXPECTED_ERR),
        "Error should contain '{EXPECTED_ERR}', got: {err:?}"
    );
    assert_eq!(
        inner_store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_size_and_hash_succeeds_on_small_data() -> Result<(), Error> {
    /// This value is sha256("123").
    const HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    const VALUE: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, 3).unwrap();
    let result = store.update_oneshot(digest, VALUE.into()).await;
    assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
    assert_eq!(
        inner_store.has(digest).await,
        Ok(Some(VALUE.len() as u64)),
        "Expected data to exist in store after update"
    );
    Ok(())
}

#[nativelink_test]
async fn verify_hash_on_read_catches_corrupted_data() -> Result<(), Error> {
    /// This value is sha256("123").
    const CORRECT_HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    const CORRECT_VALUE: &str = "123";
    const CORRUPTED_VALUE: &str = "999";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    // Write corrupted data directly to the inner store, bypassing verification.
    let digest = DigestInfo::try_new(CORRECT_HASH, CORRECT_VALUE.len() as u64).unwrap();
    inner_store
        .update_oneshot(digest, CORRUPTED_VALUE.into())
        .await?;

    // Reading through the verify store should detect the hash mismatch.
    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(
        result.is_err(),
        "Expected hash mismatch error, got: {:?}",
        result
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Hash mismatch on read"),
        "Error should mention hash mismatch on read, got: {err:?}"
    );
    assert_eq!(err.code, Code::DataLoss, "Error code should be DataLoss");
    Ok(())
}

#[nativelink_test]
async fn verify_hash_on_read_passes_for_correct_data() -> Result<(), Error> {
    /// This value is sha256("123").
    const HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    const VALUE: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, VALUE.len() as u64).unwrap();
    inner_store.update_oneshot(digest, VALUE.into()).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert_eq!(
        result.as_deref(),
        Ok(VALUE.as_bytes()),
        "Expected correct data, got: {:?}",
        result
    );
    Ok(())
}

#[nativelink_test]
async fn verify_size_on_read_catches_wrong_size() -> Result<(), Error> {
    const VALUE_SHORT: &str = "12";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    // Create a digest that says 5 bytes, but store only 2 bytes in inner store.
    let digest = DigestInfo::try_new(VALID_HASH1, 5).unwrap();
    inner_store
        .update_oneshot(digest, VALUE_SHORT.into())
        .await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(
        result.is_err(),
        "Expected size mismatch error, got: {:?}",
        result
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("Expected size 5 but got size 2 on read"),
        "Error should mention size mismatch, got: {err:?}"
    );
    assert_eq!(err.code, Code::DataLoss, "Error code should be DataLoss");
    Ok(())
}

#[nativelink_test]
async fn verify_hash_on_partial_read_is_skipped() -> Result<(), Error> {
    /// This value is sha256("123").
    const HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    const VALUE: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, VALUE.len() as u64).unwrap();
    inner_store.update_oneshot(digest, VALUE.into()).await?;

    // Partial read with offset -- verification should be skipped.
    let result = store.get_part_unchunked(digest, 1, Some(2)).await;
    assert_eq!(
        result.as_deref(),
        Ok(&VALUE.as_bytes()[1..3]),
        "Partial read should succeed without verification, got: {:?}",
        result
    );
    Ok(())
}

#[nativelink_test]
async fn verify_blake3_hash_on_read_catches_corruption() -> Result<(), Error> {
    /// This value is blake3("123").
    const CORRECT_HASH: &str = "b3d4f8803f7e24b8f389b072e75477cdbcfbe074080fb5e500e53e26e054158e";
    const CORRECT_VALUE: &str = "123";
    const CORRUPTED_VALUE: &str = "abc";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(CORRECT_HASH, CORRECT_VALUE.len() as u64).unwrap();
    inner_store
        .update_oneshot(digest, CORRUPTED_VALUE.into())
        .await?;

    let result = store
        .get_part_unchunked(digest, 0, None)
        .instrument(info_span!("get_part_unchunked"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await;

    assert!(
        result.is_err(),
        "Expected hash mismatch error, got: {:?}",
        result
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Hash mismatch on read"),
        "Error should mention hash mismatch on read, got: {err:?}"
    );
    assert_eq!(err.code, Code::DataLoss, "Error code should be DataLoss");
    Ok(())
}

#[nativelink_test]
async fn verify_both_size_and_hash_on_read_succeeds() -> Result<(), Error> {
    /// This value is sha256("123").
    const HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    const VALUE: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(HASH, VALUE.len() as u64).unwrap();
    inner_store.update_oneshot(digest, VALUE.into()).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert_eq!(
        result.as_deref(),
        Ok(VALUE.as_bytes()),
        "Expected correct data when both verify_size and verify_hash pass, got: {:?}",
        result
    );
    Ok(())
}

/// Per-wrapper regression for testing-czar MAJOR-1 (#140 follow-up):
/// `VerifyStore::mark_stable` MUST delegate to its inner store.
/// Without an explicit override the trait's silent no-op default would
/// swallow the call at the verify layer — and `cas_STORE` in
/// production is `Verify(Ref(cas_INNER))`, so a regression would break
/// the entire BIS pipeline. The production-composition test exercises
/// this composition end-to-end, but a per-wrapper test pinpoints the
/// regression to this specific layer.
///
/// Wraps a FastSlowStore as the inner so `mark_stable` lands in its
/// observable `stable_digests` queue.
#[nativelink_test]
async fn mark_stable_delegates_to_inner_store_test() -> Result<(), Error> {
    let inner_fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        inner_fast_slow.clone(),
    );

    let digest = DigestInfo::new([0xAAu8; 32], 100);
    let outer = Store::new(verify);
    outer.as_store_driver().mark_stable(&[digest]);

    let drained = inner_fast_slow.as_store_driver().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "VerifyStore::mark_stable must delegate to inner_store. Without \
         this delegation the trait silent-default no-op swallows the \
         call and the BIS pipeline breaks at the cas_STORE outer layer. \
         Drained: {drained:?}",
    );

    Ok(())
}

/// #336 P1 production-composition test: verify the writer-termination
/// contract on the SIZE-MISMATCH `inner_check_get_part` Err path.
///
/// Mechanism guarded: when the underlying inner store returns fewer
/// bytes than the digest expects, `inner_check_get_part` returns
/// `Err(Code::DataLoss "Expected size N but got size M on read")`
/// without (pre-#336-fix) terminating the borrowed `writer`. Direct
/// callers (e.g. `get_part_unchunked`) own the writer and drop it on
/// function return so the bug is invisible at that boundary; any
/// wrapping caller that joins (get_fut, reader_fut) over the writer's
/// tx/rx pair deadlocks because the reader never observes EOF or
/// error.
///
/// This test sets up the wrapping composition by hand: it drives
/// VerifyStore::get_part with a borrowed writer and concurrently
/// reads from the matching rx in `tokio::join!`. The reader sees the
/// structured DataLoss error if-and-only-if `writer_guard.fail(err)`
/// fired in `inner_check_get_part`. A 5-second `tokio::time::timeout`
/// is the deadlock detector — without the fix, the reader future
/// hangs indefinitely.
///
/// Mutation step: comment out the `Err(writer_guard.fail(err))` /
/// replace with `Err(err)` for the size branch in
/// `verify_store::inner_check_get_part`. The test must red-fail
/// with the bespoke message below.
#[nativelink_test]
async fn verify_store_inner_check_get_part_size_mismatch_terminates_writer() -> Result<(), Error> {
    use core::time::Duration;

    use nativelink_util::buf_channel::DropCloserWriteHalf;
    use nativelink_util::store_trait::StoreKey;

    const VALUE_SHORT: &str = "12";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    // Digest claims 5 bytes but inner_store has only 2 — the
    // `if sum_size != expected_size` branch fires inside
    // `inner_check_get_part` after EOF from inner.
    let digest = DigestInfo::try_new(VALID_HASH1, 5).unwrap();
    inner_store.update_oneshot(digest, VALUE_SHORT.into()).await?;

    let (tx, mut rx) = make_buf_channel_pair();
    let mut tx: DropCloserWriteHalf = tx;

    // Drive get_part and reader concurrently — this is the production-
    // composition seam. Without writer termination the reader hangs.
    let pinned_store = Pin::new(&store);
    let get_fut = async {
        pinned_store
            .get_part(StoreKey::from(digest), &mut tx, 0, None)
            .await
    };
    let reader_fut = async {
        // Drain to EOF or first error. If `inner_check_get_part`
        // bailed without `fail(err)` (the bug being guarded), the
        // mpsc Sender is still alive and `recv` blocks until the
        // outer scope drops it — past the timeout below.
        loop {
            let chunk = rx.recv().await?;
            if chunk.is_empty() {
                return Result::<(), Error>::Ok(());
            }
        }
    };

    let timeout_res = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(get_fut, reader_fut) },
    )
    .await
    .expect(
        "VerifyStore::inner_check_get_part writer-termination contract violated \
         — wrapping caller deadlocked on un-EOF'd writer (size-mismatch path)",
    );

    let (get_res, reader_res) = timeout_res;
    let get_err = get_res.expect_err("get_part should return DataLoss size mismatch");
    assert_eq!(get_err.code, Code::DataLoss);
    assert!(
        get_err.to_string().contains("Expected size 5 but got size 2 on read"),
        "expected size-mismatch DataLoss, got: {get_err:?}"
    );

    // Reader observes the structured DataLoss as well (proving the
    // guard's `fail(err)` actually delivered the error to the
    // paired reader, not just terminated with the synthesized
    // Drop-fallback Internal).
    let reader_err = reader_res
        .expect_err("reader should observe the structured DataLoss from writer.send_error");
    assert!(
        reader_err.code == Code::DataLoss
            || reader_err.to_string().contains("Expected size 5 but got size 2 on read"),
        "reader should see DataLoss propagated by guard.fail; got: {reader_err:?}"
    );
    Ok(())
}

/// #336 P1 sibling production-composition test: HASH-MISMATCH branch
/// of `inner_check_get_part`. Same writer-termination contract.
#[nativelink_test]
async fn verify_store_inner_check_get_part_hash_mismatch_terminates_writer() -> Result<(), Error> {
    use core::time::Duration;

    use nativelink_util::buf_channel::DropCloserWriteHalf;
    use nativelink_util::store_trait::StoreKey;

    /// sha256("123")
    const CORRECT_HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
    const CORRECT_VALUE: &str = "123";
    const CORRUPTED_VALUE: &str = "999";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: true,
        },
        Store::new(inner_store.clone()),
    );

    let digest = DigestInfo::try_new(CORRECT_HASH, CORRECT_VALUE.len() as u64).unwrap();
    inner_store.update_oneshot(digest, CORRUPTED_VALUE.into()).await?;

    let (tx, mut rx) = make_buf_channel_pair();
    let mut tx: DropCloserWriteHalf = tx;

    let pinned_store = Pin::new(&store);
    let get_fut = async {
        pinned_store
            .get_part(StoreKey::from(digest), &mut tx, 0, None)
            .await
    };
    let reader_fut = async {
        loop {
            let chunk = rx.recv().await?;
            if chunk.is_empty() {
                return Result::<(), Error>::Ok(());
            }
        }
    };

    let timeout_res = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(get_fut, reader_fut) },
    )
    .await
    .expect(
        "VerifyStore::inner_check_get_part writer-termination contract violated \
         — wrapping caller deadlocked on un-EOF'd writer (hash-mismatch path)",
    );

    let (get_res, reader_res) = timeout_res;
    let get_err = get_res.expect_err("get_part should return DataLoss hash mismatch");
    assert_eq!(get_err.code, Code::DataLoss);
    assert!(
        get_err.to_string().contains("Hash mismatch on read"),
        "expected hash-mismatch DataLoss, got: {get_err:?}"
    );

    let reader_err = reader_res
        .expect_err("reader should observe the structured DataLoss from writer.send_error");
    assert!(
        reader_err.code == Code::DataLoss
            || reader_err.to_string().contains("Hash mismatch on read"),
        "reader should see DataLoss propagated by guard.fail; got: {reader_err:?}"
    );
    Ok(())
}

/// #336 P1 over-action test (testing-czar MAJOR-1): the inline doc-comment
/// at `verify_store.rs:222-231` records the design DECISION to NOT use
/// `WriteHalfGuard` around `inner_check_get_part`. Reason: when inner
/// returns a structured Err (e.g. NotFound), the `?`-propagation path
/// must surface that structured Err to the OUTER caller — wrapping it
/// in a synthesized `Code::Internal "buf_channel: writer dropped
/// without commit"` (which a WriteHalfGuard Drop fallback would do)
/// shadows the structured upstream code on the writer side and breaks
/// the `cdn_cache_failure_*` regression suite.
///
/// This test exercises the over-action direction: drive
/// `VerifyStore::get_part(digest, ..)` against an EMPTY inner store.
/// The inner `MemoryStore` returns `Code::NotFound`. With the current
/// design, the joined Result surfaces `Code::NotFound`. If a future
/// maintainer "simplifies" by re-introducing `WriteHalfGuard::new(writer)`
/// around `inner_check_get_part`, the test red-fails because the joined
/// Result would become `Code::Internal "buf_channel: writer dropped
/// without commit"` (the synthesized over-action that would mask
/// the structured NotFound).
///
/// Production-composition seam: VerifyStore wraps MemoryStore exactly as
/// the AC store chain `AcProxyStore → VerifyStore → MemoryStore` does
/// at the `cas_STORE` chain (the configuration that hit the original
/// 2026-04-25 deadlock class).
///
/// Mutation step (for falsification): in `verify_store.rs`,
/// `inner_check_get_part` signature, change
/// `writer: &mut DropCloserWriteHalf` → wrap with
/// `let mut writer = WriteHalfGuard::new(writer);` at function top. The
/// test must red-fail with the bespoke over-action message below
/// because the Drop fallback fires `Code::Internal "buf_channel: writer
/// dropped without commit"` instead of letting the upstream NotFound
/// flow through.
#[nativelink_test]
async fn verify_store_inner_check_get_part_inner_notfound_propagates_structured_err()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_util::store_trait::StoreKey;

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(inner_store.clone()),
    );

    // Inner store is empty — digest will NOT be found. Inner returns
    // `Code::NotFound`; the joined `(get_fut, check_fut)` must surface
    // that structured NotFound code to the caller. The wrapping caller
    // chain modeled here is `get_part_unchunked` (the canonical
    // production caller shape), which owns the writer in a closure and
    // drops it on closure exit. That tx-drop is the discipline that
    // makes the documented "no WriteHalfGuard around inner_check_get_part"
    // design safe today — see `verify_store.rs:222-231`.
    let digest = DigestInfo::try_new(VALID_HASH1, 5).unwrap();

    let timeout_res = tokio::time::timeout(Duration::from_secs(5), async {
        Pin::new(&store)
            .get_part_unchunked(StoreKey::from(digest), 0, None)
            .await
    })
    .await
    .expect(
        "VerifyStore::inner_check_get_part over-action regression: inner NotFound hung \
         the get_part_unchunked closure inside the deadlock-detector window — the \
         production caller shape (closure-drop tx) should NOT cause a deadlock, even \
         without WriteHalfGuard around inner_check_get_part",
    );

    let get_err = timeout_res
        .expect_err("get_part_unchunked should return NotFound from empty inner store");
    assert_eq!(
        get_err.code,
        Code::NotFound,
        "VerifyStore::inner_check_get_part over-action regression: inner NotFound was \
         masked by synthesized Drop-fallback Internal — expected Code::NotFound, got: \
         {get_err:?}. This means a WriteHalfGuard wrap was added around \
         inner_check_get_part and clobbered the structured upstream NotFound."
    );
    assert!(
        !get_err
            .to_string()
            .contains("buf_channel: writer dropped without commit"),
        "VerifyStore::inner_check_get_part over-action regression: synthesized Internal \
         (\"buf_channel: writer dropped without commit\") appears in the joined Result, \
         masking the structured upstream code. Got: {get_err:?}"
    );
    Ok(())
}
