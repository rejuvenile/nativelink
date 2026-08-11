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

use nativelink_config::stores::{DedupSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;
use nativelink_store::compression_store::{WINCODE_PREALLOC_LIMIT_BYTES, WincodeConfig};
use nativelink_store::dedup_store::{DedupIndex, DedupStore};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

fn make_default_config() -> DedupSpec {
    DedupSpec {
        index_store: StoreSpec::Memory(MemorySpec::default()),
        content_store: StoreSpec::Memory(MemorySpec::default()),
        min_size: 8 * 1024,
        normal_size: 32 * 1024,
        max_size: 128 * 1024,
        max_concurrent_fetch_per_get: 10,
    }
}

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const MEGABYTE_SZ: usize = 1024 * 1024;

#[nativelink_test]
async fn simple_round_trip_test() -> Result<(), Error> {
    let store = DedupStore::new(
        &make_default_config(),
        Store::new(MemoryStore::new(&MemorySpec::default())), // Index store.
        Store::new(MemoryStore::new(&MemorySpec::default())), // Content store.
    )?;

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH1, MEGABYTE_SZ).unwrap();

    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    let rt_data = store
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(rt_data, original_data, "Expected round trip data to match");
    Ok(())
}

#[nativelink_test]
async fn check_missing_last_chunk_test() -> Result<(), Error> {
    // This is the hash & size of the last chunk item in the content_store.
    const LAST_CHUNK_HASH: &str =
        "f6a29384357a77575b0a8cc79f731a4188d0155c00d5fb9a18becd92f6d1f074";
    const LAST_CHUNK_SIZE: usize = 10669;

    let content_store = MemoryStore::new(&MemorySpec::default());
    let store = DedupStore::new(
        &make_default_config(),
        Store::new(MemoryStore::new(&MemorySpec::default())), // Index store.
        Store::new(content_store.clone()),
    )?;

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH1, MEGABYTE_SZ).unwrap();

    store
        .update_oneshot(digest, original_data.into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    let did_delete = content_store
        .remove_entry(
            DigestInfo::try_new(LAST_CHUNK_HASH, LAST_CHUNK_SIZE)
                .unwrap()
                .into(),
        )
        .await;

    assert_eq!(did_delete, true, "Expected item to exist in store");

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "Expected result to be an error");
    assert_eq!(
        result.unwrap_err().code,
        Code::NotFound,
        "Expected result to not be found"
    );
    Ok(())
}

/// Test to ensure if we upload a bit of data then request just a slice of it, we get the
/// proper data out. Internal to `DedupStore` we only download the slices that contain the
/// requested data; this test covers that use case.
#[nativelink_test]
async fn fetch_part_test() -> Result<(), Error> {
    const DATA_SIZE: usize = MEGABYTE_SZ / 4;
    const ONE_THIRD_SZ: usize = DATA_SIZE / 3;

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(MemoryStore::new(&MemorySpec::default())), // Index store.
        Store::new(MemoryStore::new(&MemorySpec::default())), // Content store.
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    let rt_data = store
        .get_part_unchunked(digest, ONE_THIRD_SZ as u64, Some(ONE_THIRD_SZ as u64))
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(
        rt_data.len(),
        ONE_THIRD_SZ,
        "Expected round trip sizes to match"
    );
    assert_eq!(
        rt_data,
        original_data[ONE_THIRD_SZ..(ONE_THIRD_SZ * 2)],
        "Expected round trip data to match"
    );
    Ok(())
}

#[nativelink_test]
async fn check_length_not_set_with_chunk_read_beyond_first_chunk_regression_test()
-> Result<(), Error> {
    const DATA_SIZE: usize = 30;
    const START_READ_BYTE: usize = 7;

    let store = DedupStore::new(
        &DedupSpec {
            index_store: StoreSpec::Memory(MemorySpec::default()),
            content_store: StoreSpec::Memory(MemorySpec::default()),
            min_size: 5,
            normal_size: 6,
            max_size: 7,
            max_concurrent_fetch_per_get: 10,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())), // Index store.
        Store::new(MemoryStore::new(&MemorySpec::default())), // Content store.
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    // This value must be larger than `max_size` in the config above.
    let rt_data = store
        .get_part_unchunked(digest, START_READ_BYTE as u64, None)
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(
        rt_data.len(),
        DATA_SIZE - START_READ_BYTE,
        "Expected round trip sizes to match"
    );
    assert_eq!(
        rt_data,
        original_data[START_READ_BYTE..],
        "Expected round trip data to match"
    );
    Ok(())
}

#[nativelink_test]
async fn check_chunk_boundary_reads_test() -> Result<(), Error> {
    const DATA_SIZE: usize = 30;
    const START_READ_BYTE: usize = 10;

    let store = DedupStore::new(
        &DedupSpec {
            index_store: StoreSpec::Memory(MemorySpec::default()),
            content_store: StoreSpec::Memory(MemorySpec::default()),
            min_size: 5,
            normal_size: 6,
            max_size: 7,
            max_concurrent_fetch_per_get: 10,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())), // Index store.
        Store::new(MemoryStore::new(&MemorySpec::default())), // Content store.
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();
    store
        .update_oneshot(digest, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    for offset in 0..=DATA_SIZE {
        for len in 0..DATA_SIZE {
            // If reading at DATA_SIZE, we will set len to None to check that edge case.
            let maybe_len = if offset == DATA_SIZE {
                None
            } else {
                Some(len as u64)
            };
            let len = if maybe_len.is_none() { DATA_SIZE } else { len };

            let rt_data = store
                .get_part_unchunked(digest, offset as u64, maybe_len)
                .await
                .err_tip(|| "Failed to get_part from dedup store")?;

            let len_fenced = core::cmp::min(len, rt_data.len());
            assert_eq!(
                rt_data.len(),
                len_fenced,
                "Expected round trip sizes to match"
            );
            assert_eq!(
                rt_data,
                original_data[offset..(offset + len_fenced)],
                "Expected round trip data to match"
            );
        }
    }

    // This value must be larger than `max_size` in the config above.
    let rt_data = store
        .get_part_unchunked(digest, START_READ_BYTE as u64, None)
        .await
        .err_tip(|| "Failed to get_part from dedup store")?;

    assert_eq!(
        rt_data.len(),
        DATA_SIZE - START_READ_BYTE,
        "Expected round trip sizes to match"
    );
    assert_eq!(
        rt_data,
        original_data[START_READ_BYTE..],
        "Expected round trip data to match"
    );
    Ok(())
}

/// Ensure that when we run a `.has()` on a dedup store it will check to ensure all indexed
/// content items exist instead of just checking the entry in the index store.
#[nativelink_test]
async fn has_checks_content_store() -> Result<(), Error> {
    const DATA_SIZE: usize = MEGABYTE_SZ / 4;

    // MokaEvictingMap weighs entries in KB (ceil(bytes / 1024)) because moka's
    // weigher returns u32 and we need to support multi-GB caches. Two
    // consequences for this test:
    //   1. `max_bytes` is rounded down to the nearest KB to compute capacity.
    //   2. Each chunk's weight is rounded UP to the nearest KB.
    // Pick a cap that comfortably holds digest1's ~256 KiB worth of FastCDC
    // chunks (with up to ~128 bytes KB-rounding slack per chunk) but leaves
    // no room for the second blob, so writing digest2 must evict at least
    // one of digest1's chunks under the LRU policy.
    const CACHE_CAP_BYTES: usize = (DATA_SIZE * 3) / 2;

    let index_store = MemoryStore::new(&MemorySpec::default());
    let content_store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
            max_bytes: CACHE_CAP_BYTES,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store.clone()),
        Store::new(content_store.clone()),
    )?;

    let original_data = make_random_data(DATA_SIZE);
    let digest1 = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    store
        .update_oneshot(digest1, original_data.clone().into())
        .await
        .err_tip(|| "Failed to write data to dedup store")?;

    {
        // Check to ensure we our baseline `.has()` succeeds.
        let size_info = store.has(digest1).await.err_tip(|| "Failed to run .has")?;
        assert_eq!(size_info, Some(DATA_SIZE as u64), "Expected sizes to match");
    }
    {
        // Write a second blob whose content-addressed chunks differ from
        // digest1's. It must be large enough that the cache cannot hold
        // both blobs' chunks simultaneously, forcing eviction of at least
        // one of digest1's chunks. We construct distinct random bytes via
        // a different rand seed so blake3 chunk hashes differ from digest1.
        let data2 = {
            let mut value = vec![0u8; DATA_SIZE];
            let mut rng = SmallRng::seed_from_u64(2);
            rng.fill(&mut value[..]);
            value
        };
        let digest2 = DigestInfo::try_new(VALID_HASH2, data2.len()).unwrap();
        store
            .update_oneshot(digest2, data2.clone().into())
            .await
            .err_tip(|| "Failed to write data to dedup store")?;

        {
            // Check our recently added entry is still valid.
            let size_info = store.has(digest2).await.err_tip(|| "Failed to run .has")?;
            assert_eq!(
                size_info,
                Some(data2.len() as u64),
                "Expected sizes to match"
            );
        }
        {
            // Check our first added entry is now invalid (because part of it was evicted).
            let size_info = store.has(digest1).await.err_tip(|| "Failed to run .has")?;
            assert_eq!(
                size_info, None,
                "Expected .has() to return None (not found)"
            );
        }
    }

    Ok(())
}

/// Ensure that when we run a `.has()` on a dedup store and the index does not exist it will
/// properly return None.
#[nativelink_test]
async fn has_with_no_existing_index_returns_none_test() -> Result<(), Error> {
    const DATA_SIZE: usize = 10;

    let index_store = MemoryStore::new(&MemorySpec::default());
    let content_store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
            max_count: 10,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store.clone()),
        Store::new(content_store.clone()),
    )?;

    let digest = DigestInfo::try_new(VALID_HASH1, DATA_SIZE).unwrap();

    {
        let size_info = store.has(digest).await.err_tip(|| "Failed to run .has")?;
        assert_eq!(
            size_info, None,
            "Expected None to be returned, got {:?}",
            size_info
        );
    }
    Ok(())
}

/// Ensure that when we run a `.has()` on a dedup store to check for empty blobs it will
/// properly return Some(0).
#[nativelink_test]
async fn has_with_zero_digest_returns_some_test() -> Result<(), Error> {
    let index_store = MemoryStore::new(&MemorySpec::default());
    let content_store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
            max_count: 10,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });

    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store.clone()),
        Store::new(content_store.clone()),
    )?;

    let digest = ZERO_BYTE_DIGESTS[0];

    {
        let size_info = store.has(digest).await.err_tip(|| "Failed to run .has")?;
        assert_eq!(
            size_info,
            Some(0),
            "Expected Sone(0) to be returned, got {:?}",
            size_info
        );
    }
    Ok(())
}

/// Regression for red-team F3 + #140: DedupStore must delegate
/// `mark_stable` to its `index_store`. The outer (dedup-original) digest
/// lives in the index_store; per-chunk content digests are independent
/// and have their own BlobsAvailable advertisements. Without an explicit
/// override, the trait's silent no-op default would swallow the call.
///
/// Wraps a FastSlowStore as the index_store so its `stable_digests`
/// queue can be drained as the assertion target.
#[nativelink_test]
async fn mark_stable_delegates_to_index_store_test() -> Result<(), Error> {
    let index_fast_slow = Store::new(FastSlowStore::new(
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
    let content_store = Store::new(MemoryStore::new(&MemorySpec::default()));

    let dedup = DedupStore::new(
        &make_default_config(),
        index_fast_slow.clone(),
        content_store,
    )?;

    let digest = DigestInfo::new([8u8; 32], 100);
    let outer = Store::new(dedup);
    outer.as_store_driver().mark_stable(&[digest]);

    // The dedup layer should have forwarded the call to index_store
    // (a FastSlowStore), which pushes into its `stable_digests` queue.
    let drained = index_fast_slow.as_store_driver().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "DedupStore::mark_stable must delegate to index_store. \
         Without this delegation the trait silent-default no-op swallows \
         the call and the worker's pin (durable under v2) leaks. \
         Drained: {drained:?}"
    );

    Ok(())
}

/// #336 P1 production-composition test: the borrowed `writer` MUST be
/// terminated on every exit path — including the early-return when the
/// index_store has no entry for the digest.
///
/// Drive `Pin::new(&store).get_part(...)` with a borrowed writer paired
/// with a separate consumer reading from the matching rx. If
/// `dedup_store::get_part` early-returns Err on the index-store NotFound
/// without terminating the writer (the bug being guarded), the consumer
/// hangs and the 5-second `tokio::time::timeout` panics with the
/// bespoke message below.
///
/// Mutation step: replace `WriteHalfGuard::new(writer)` with the bare
/// `writer` parameter and remove the `commit_eof()` calls — the test
/// must red-fail with the bespoke "writer-termination contract
/// violated" message.
#[nativelink_test]
async fn dedup_store_get_part_terminates_writer_on_index_miss() -> Result<(), Error> {
    use core::pin::Pin;
    use core::time::Duration;

    use nativelink_util::buf_channel::{DropCloserWriteHalf, make_buf_channel_pair};
    use nativelink_util::store_trait::StoreKey;

    let index_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let content_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = DedupStore::new(&make_default_config(), index_store, content_store)?;

    // Digest never written to index_store — get_part will early-return
    // Err(NotFound) from `index_store.get_part_unchunked(...)?`.
    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();

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
        "DedupStore::get_part writer-termination contract violated — wrapping caller \
         deadlocked on un-EOF'd writer (index-store NotFound early-return path)",
    );

    let (get_res, _reader_res) = timeout_res;
    assert!(get_res.is_err(), "expected dedup get_part Err on index-store NotFound");
    Ok(())
}

/// #336 P1 sibling test (testing-czar MAJOR-2): the writer-termination
/// contract on `DedupStore::get_part` also fires on the
/// content-store-miss path — `?`-propagated content-store NotFound
/// inside the buffered stream's `.next().await` arm. Pre-fix, this
/// `?` left the outer writer un-terminated; the wrapping caller's
/// `tokio::join!` over the writer's tx/rx pair deadlocked.
///
/// Drive `Pin::new(&store).get_part(...)` against a populated
/// index_store whose entries point at digests that do NOT exist in
/// content_store. Read from the matching rx in `tokio::join!`.
/// Without `WriteHalfGuard`, the reader hangs and the 5-second
/// `tokio::time::timeout` fires the bespoke message below.
///
/// Mutation step: comment out `WriteHalfGuard::new(writer)` in
/// `dedup_store.rs::get_part` — test must red-fail with the bespoke
/// "content-store NotFound" deadlock message.
#[nativelink_test]
async fn dedup_store_get_part_terminates_writer_on_content_miss() -> Result<(), Error> {
    use core::pin::Pin;
    use core::time::Duration;

    use bincode::serde::encode_to_vec;
    use nativelink_util::buf_channel::{DropCloserWriteHalf, make_buf_channel_pair};
    use nativelink_util::store_trait::StoreKey;

    let index_store_inner = MemoryStore::new(&MemorySpec::default());
    let content_store_inner = MemoryStore::new(&MemorySpec::default());
    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store_inner.clone()),
        Store::new(content_store_inner.clone()),
    )?;

    // Pre-populate index_store with a valid `DedupIndex` pointing at a
    // content digest that does NOT exist in content_store. `get_part`
    // reaches the buffered-stream loop and the `?` on
    // `content_store.get_part_unchunked(...)` propagates NotFound.
    let outer_digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let missing_content_digest = DigestInfo::try_new(VALID_HASH2, 50).unwrap();
    let index = DedupIndex {
        entries: vec![missing_content_digest],
    };
    let bincode_cfg = bincode::config::legacy();
    let serialized = encode_to_vec(&index, bincode_cfg)
        .map_err(|e| nativelink_error::make_err!(Code::Internal, "encode failed: {e}"))?;
    index_store_inner
        .update_oneshot(outer_digest, serialized.into())
        .await?;

    let (tx, mut rx) = make_buf_channel_pair();
    let mut tx: DropCloserWriteHalf = tx;

    let pinned_store = Pin::new(&*store);
    let get_fut = async {
        pinned_store
            .get_part(StoreKey::from(outer_digest), &mut tx, 0, None)
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

    let timeout_res =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(get_fut, reader_fut) })
            .await
            .expect(
                "DedupStore::get_part writer-termination contract violated — wrapping caller \
                 deadlocked on un-EOF'd writer (content-store NotFound `?`-propagation path)",
            );

    let (get_res, _reader_res) = timeout_res;
    assert!(
        get_res.is_err(),
        "expected dedup get_part Err on content-store NotFound, got: {get_res:?}"
    );
    Ok(())
}

/// #336 P1 sibling test (testing-czar MAJOR-2): writer-termination
/// contract on `DedupStore::get_part` also fires on the index
/// deserialization error path — pre-populated index_store with
/// non-protobuf bytes, the `decode_from_slice::<DedupIndex, _>(..)?`
/// returns Err. Pre-fix, the `?` left the outer writer un-terminated;
/// wrapping caller deadlocked.
///
/// Mutation step: comment out `WriteHalfGuard::new(writer)` in
/// `dedup_store.rs::get_part` — test must red-fail with bespoke
/// "index deserialize" deadlock message.
#[nativelink_test]
async fn dedup_store_get_part_terminates_writer_on_index_deserialize_err() -> Result<(), Error> {
    use core::pin::Pin;
    use core::time::Duration;

    use nativelink_util::buf_channel::{DropCloserWriteHalf, make_buf_channel_pair};
    use nativelink_util::store_trait::StoreKey;

    let index_store_inner = MemoryStore::new(&MemorySpec::default());
    let content_store_inner = MemoryStore::new(&MemorySpec::default());
    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store_inner.clone()),
        Store::new(content_store_inner.clone()),
    )?;

    // Pre-populate index_store with arbitrary bytes that are NOT a
    // valid `DedupIndex`. The decode_from_slice in `get_part` returns
    // a "Failed to deserialize index" Internal Err via `?`.
    let outer_digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let garbage = vec![0xFFu8; 64];
    index_store_inner
        .update_oneshot(outer_digest, garbage.into())
        .await?;

    let (tx, mut rx) = make_buf_channel_pair();
    let mut tx: DropCloserWriteHalf = tx;

    let pinned_store = Pin::new(&*store);
    let get_fut = async {
        pinned_store
            .get_part(StoreKey::from(outer_digest), &mut tx, 0, None)
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

    let timeout_res =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(get_fut, reader_fut) })
            .await
            .expect(
                "DedupStore::get_part writer-termination contract violated — wrapping caller \
                 deadlocked on un-EOF'd writer (index deserialize `?`-propagation path)",
            );

    let (get_res, _reader_res) = timeout_res;
    assert!(
        get_res.is_err(),
        "expected dedup get_part Err on index deserialize, got: {get_res:?}"
    );
    Ok(())
}

// ─── #wincode-prealloc: untrusted-length preallocation guard ────────────────
//
// `DedupIndex` is decoded straight out of the index store, which for a
// dedup chain is ordinary (evictable, corruptible) CAS storage — the declared
// entry count is UNTRUSTED input. On the wire it is a `BincodeLen` u64 LE count
// followed by that many 40-byte `DigestInfo`s, so the tests below hand-craft the
// leading length and let wincode do the rest.

/// Bytes wincode charges per `DedupIndex` entry: `DigestInfo` is
/// `PackedHash([u8; 32])` + `size_bytes: u64`. `SeqLen::prealloc_check` compares
/// `len * size_of::<DigestInfo>()` against the configured limit, so this factor
/// converts the byte cap into an entry cap.
const DIGEST_INFO_WIRE_STRIDE: usize = size_of::<DigestInfo>();

/// Build the raw leading `BincodeLen` (u64 LE) of a `DedupIndex` declaring
/// `entries` elements, with NO element bytes following it.
fn dedup_index_bytes_declaring(entries: u64) -> Vec<u8> {
    entries.to_le_bytes().to_vec()
}

/// #wincode-prealloc: `DedupStore::has` carries a DELIBERATE graceful arm —
/// "index deserialize error → `Ok(None)`" (`dedup_store.rs:171`) — so a corrupt
/// index degrades to a cache MISS instead of an error. The bincode→wincode port
/// (`13c77abc`) set the preallocation limit to
/// `wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED`, which makes
/// `SeqLen::read_prealloc_check` a NO-OP, so `Vec::with_capacity` ran on the
/// untrusted u64 length → `capacity overflow` PANIC. That made this graceful arm
/// UNREACHABLE for oversized lengths: a corrupt index blob killed the thread
/// rather than degrading to a miss.
///
/// Mutation step: set `WINCODE_PREALLOC_LIMIT_BYTES` back to
/// `wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED` — the test must red-fail
/// with `capacity overflow` instead of returning `Ok(None)`.
#[nativelink_test]
async fn dedup_store_has_degrades_to_none_on_oversized_index_length() -> Result<(), Error> {
    use core::time::Duration;

    let index_store_inner = MemoryStore::new(&MemorySpec::default());
    let store = DedupStore::new(
        &make_default_config(),
        Store::new(index_store_inner.clone()),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    )?;

    let outer_digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    index_store_inner
        .update_oneshot(outer_digest, dedup_index_bytes_declaring(u64::MAX).into())
        .await?;

    let has_res = tokio::time::timeout(Duration::from_secs(5), store.has(outer_digest))
        .await
        .expect("DedupStore::has hung on an oversized declared index length");

    let maybe_size = has_res.expect(
        "DedupStore::has must not surface an Err for a corrupt index — the deliberate graceful \
         arm at dedup_store.rs:171 degrades a deserialize failure to a cache miss",
    );
    assert_eq!(
        maybe_size, None,
        "DedupStore::has must degrade an index declaring an impossible entry count to Ok(None) \
         (cache miss). Reaching Vec::with_capacity with an untrusted u64 length is the \
         capacity-overflow panic that PREALLOCATION_SIZE_LIMIT_DISABLED reintroduced in 13c77abc",
    );
    Ok(())
}

/// #wincode-prealloc boundary, OVER side: a declared length whose byte cost
/// exceeds `WINCODE_PREALLOC_LIMIT_BYTES` must be REJECTED as a graceful decode
/// `Err` naming the preallocation limit — never a panic, never a giant
/// allocation.
///
/// Mutation step: set `WINCODE_PREALLOC_LIMIT_BYTES` to
/// `PREALLOCATION_SIZE_LIMIT_DISABLED` — this red-fails with `capacity overflow`.
#[nativelink_test]
async fn dedup_index_decode_rejects_length_over_prealloc_cap() -> Result<(), Error> {
    // One entry PAST the cap: `(cap / stride) + 1` entries costs more than `cap`
    // bytes, so `prealloc_check` must reject before any allocation happens.
    let over_cap_entries = (WINCODE_PREALLOC_LIMIT_BYTES / DIGEST_INFO_WIRE_STRIDE) as u64 + 1;
    let err = wincode::config::deserialize::<DedupIndex, WincodeConfig>(
        &dedup_index_bytes_declaring(over_cap_entries),
        WincodeConfig::new(),
    )
    .expect_err(
        "a DedupIndex declaring more entries than WINCODE_PREALLOC_LIMIT_BYTES allows must be \
         rejected as a decode Err, not preallocated",
    );
    let err_dbg = format!("{err:?}");
    assert!(
        err_dbg.contains("PreallocationSizeLimit"),
        "over-cap declared length must be rejected by wincode's preallocation guard \
         (ReadError::PreallocationSizeLimit); got: {err_dbg}",
    );
    Ok(())
}

/// #wincode-prealloc boundary, UNDER side: a declared length whose byte cost is
/// exactly AT the cap must pass the preallocation gate and then fail on the
/// truncated element bytes — proving the boundary sits where
/// `WINCODE_PREALLOC_LIMIT_BYTES` says it does, and that the guard rejects only
/// what it is supposed to reject.
///
/// Mutation step: set `WINCODE_PREALLOC_LIMIT_BYTES` an order of magnitude lower
/// — this red-fails because the at-cap length now trips the preallocation guard.
#[nativelink_test]
async fn dedup_index_decode_admits_length_at_prealloc_cap() -> Result<(), Error> {
    let at_cap_entries = (WINCODE_PREALLOC_LIMIT_BYTES / DIGEST_INFO_WIRE_STRIDE) as u64;
    let err = wincode::config::deserialize::<DedupIndex, WincodeConfig>(
        &dedup_index_bytes_declaring(at_cap_entries),
        WincodeConfig::new(),
    )
    .expect_err("no element bytes follow the length, so the decode must still fail");
    let err_dbg = format!("{err:?}");
    assert!(
        !err_dbg.contains("PreallocationSizeLimit"),
        "a declared length costing exactly WINCODE_PREALLOC_LIMIT_BYTES must be ADMITTED by the \
         preallocation guard and fail on the truncated payload instead; the guard rejecting it \
         means the cap is lower than the constant claims. got: {err_dbg}",
    );
    Ok(())
}

/// #wincode-prealloc regression guard: the cap REJECTS rather than caps-and-grows,
/// so a cap set too tight silently breaks a WORKING store — a legitimate large
/// index would start erroring on both serialize and deserialize (wincode runs
/// `prealloc_check` on the write path too, via
/// `SeqLen::write_bytes_needed_prealloc_check`). 1_048_576 entries is a 40 MiB
/// index: at the production default `min_size` of 64 KiB that is a 64 GiB blob,
/// and 8 GiB even at this test config's 8 KiB `min_size`.
///
/// Mutation step: set `WINCODE_PREALLOC_LIMIT_BYTES` absurdly low (e.g. `4096`) —
/// this red-fails at the serialize step.
#[nativelink_test]
async fn dedup_index_large_but_legitimate_round_trips() -> Result<(), Error> {
    const LARGE_ENTRY_COUNT: usize = 1_048_576;

    let index = DedupIndex {
        entries: vec![DigestInfo::try_new(VALID_HASH2, 4096).unwrap(); LARGE_ENTRY_COUNT],
    };
    let encoded = wincode::config::serialize(&index, WincodeConfig::new()).expect(
        "serializing a legitimate large DedupIndex must succeed — wincode enforces the \
         preallocation cap on the WRITE path too, so a cap below the largest legitimate index \
         breaks uploads, which is worse than the panic it guards against",
    );
    let decoded =
        wincode::config::deserialize::<DedupIndex, WincodeConfig>(&encoded, WincodeConfig::new())
            .expect("deserializing a legitimate large DedupIndex must succeed");

    assert_eq!(
        decoded.entries.len(),
        LARGE_ENTRY_COUNT,
        "legitimate large index must round-trip every entry",
    );
    assert_eq!(
        decoded.entries.last(),
        index.entries.last(),
        "legitimate large index must round-trip entry contents",
    );
    Ok(())
}
