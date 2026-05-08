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

use std::sync::Arc;

use nativelink_config::stores::{
    FastSlowSpec, MemorySpec, SizePartitioningSpec, StoreDirection, StoreSpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;

const BASE_SIZE_PART: u64 = 5;

const SMALL_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const SMALL_VALUE: &str = "99";

const BIG_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const BIG_VALUE: &str = "123456789";

fn setup_stores(
    size: u64,
) -> (
    Arc<SizePartitioningStore>,
    Arc<MemoryStore>,
    Arc<MemoryStore>,
) {
    let lower_memory_store = MemoryStore::new(&MemorySpec::default());
    let upper_memory_store = MemoryStore::new(&MemorySpec::default());

    let size_part_store = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        Store::new(lower_memory_store.clone()),
        Store::new(upper_memory_store.clone()),
    );
    (size_part_store, lower_memory_store, upper_memory_store)
}

#[nativelink_test]
async fn has_test() -> Result<(), Error> {
    let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

    {
        // Insert data into lower store.
        lower_memory_store
            .update_oneshot(
                DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?,
                SMALL_VALUE.into(),
            )
            .await?;

        // Insert data into upper store.
        upper_memory_store
            .update_oneshot(
                DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?,
                BIG_VALUE.into(),
            )
            .await?;
    }
    {
        // Check if our partition store has small data.
        let small_has_result = size_part_store
            .has(DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?)
            .await;
        assert_eq!(
            small_has_result,
            Ok(Some(SMALL_VALUE.len() as u64)),
            "Expected size part store to have data in ref store : {}",
            SMALL_HASH
        );
    }
    {
        // Check if our partition store has big data.
        let small_has_result = size_part_store
            .has(DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?)
            .await;
        assert_eq!(
            small_has_result,
            Ok(Some(BIG_VALUE.len() as u64)),
            "Expected size part store to have data in ref store : {}",
            BIG_HASH
        );
    }
    Ok(())
}

#[nativelink_test]
async fn get_test() -> Result<(), Error> {
    let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

    {
        // Insert data into lower store.
        lower_memory_store
            .update_oneshot(
                DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?,
                SMALL_VALUE.into(),
            )
            .await?;

        // Insert data into upper store.
        upper_memory_store
            .update_oneshot(
                DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?,
                BIG_VALUE.into(),
            )
            .await?;
    }
    {
        // Read the partition store small data.
        let data = size_part_store
            .get_part_unchunked(DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            SMALL_VALUE.as_bytes(),
            "Expected size part store to have data in ref store : {}",
            SMALL_HASH
        );
    }
    {
        // Read the partition store big data.
        let data = size_part_store
            .get_part_unchunked(DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            BIG_VALUE.as_bytes(),
            "Expected size part store to have data in ref store : {}",
            BIG_HASH
        );
    }
    Ok(())
}

#[nativelink_test]
async fn update_test() -> Result<(), Error> {
    let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

    {
        // Insert small data into ref_store.
        size_part_store
            .update_oneshot(
                DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?,
                SMALL_VALUE.into(),
            )
            .await?;

        // Insert small data into ref_store.
        size_part_store
            .update_oneshot(
                DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?,
                BIG_VALUE.into(),
            )
            .await?;
    }
    {
        // Check if we read small data from size_partition_store it has same data.
        let data = lower_memory_store
            .get_part_unchunked(DigestInfo::try_new(SMALL_HASH, SMALL_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            SMALL_VALUE.as_bytes(),
            "Expected size part store to have data in memory store : {}",
            SMALL_HASH
        );
    }
    {
        // Check if we read big data from size_partition_store it has same data.
        let data = upper_memory_store
            .get_part_unchunked(DigestInfo::try_new(BIG_HASH, BIG_VALUE.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            BIG_VALUE.as_bytes(),
            "Expected size part store to have data in memory store : {}",
            BIG_HASH
        );
    }
    Ok(())
}

/// Per-wrapper regression for testing-czar MAJOR-1 (#140 follow-up):
/// `SizePartitioningStore::mark_stable` MUST route each digest to the
/// correct inner store by size threshold (`<partition_size` -> lower,
/// `>=partition_size` -> upper). The production-composition test in
/// `nativelink-service/tests/mark_stable_on_blobs_available_test.rs`
/// only ever drives digests below 16 KiB, so a regression that
/// SWAPPED the lower/upper destinations would still pass that test —
/// this per-wrapper test exists to catch that swap.
///
/// Approach: build a `SizePartitioningStore` whose lower and upper
/// inners are each a `FastSlowStore`. `FastSlowStore::mark_stable`
/// pushes into its own `stable_digests` queue, observable via
/// `drain_stable_digests`. Calling `mark_stable(&[small, large])` on
/// the partition wrapper must result in:
///   * the lower FastSlowStore's queue containing exactly `[small]`
///   * the upper FastSlowStore's queue containing exactly `[large]`
///
/// Mutation step: in `size_partitioning_store.rs::mark_stable`, swap
/// the lower/upper push targets (push `lower` to `upper_store` and
/// vice versa); this assertion fails with a specific message.
#[nativelink_test]
async fn mark_stable_routes_by_size_threshold_test() -> Result<(), Error> {
    // Use a threshold larger than the largest small digest used here
    // and smaller than the large digest. 100 keeps both digests far
    // from the boundary so an off-by-one in the comparison can't mask
    // a routing swap.
    const PARTITION: u64 = 100;

    // Lower and upper inner stores are FastSlowStores so we can drain
    // their `stable_digests` queues independently. The fast/slow tier
    // identities don't matter — we only observe the
    // `mark_stable` -> `stable_digests` sink.
    let lower_inner = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let upper_inner = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));

    let partition = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: PARTITION,
            // The spec fields here are unused by the test path — the
            // `lower_store` / `upper_store` arguments to `new` are the
            // load-bearing ones. Match types/shapes so the spec
            // validates.
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        lower_inner.clone(),
        upper_inner.clone(),
    );

    // size_bytes: 50 -> lower (< 100), 200 -> upper (>= 100).
    let small_digest = DigestInfo::new([1u8; 32], 50);
    let large_digest = DigestInfo::new([2u8; 32], 200);

    // Wrap in `Store` so the StoreLike `mark_stable` method on the
    // outer call surface drives the SizePartitioningStore impl, matching
    // how the production worker_api_server invokes it.
    let outer = Store::new(partition);
    outer
        .as_store_driver()
        .mark_stable(&[small_digest, large_digest]);

    let lower_drained = lower_inner.as_store_driver().drain_stable_digests();
    let upper_drained = upper_inner.as_store_driver().drain_stable_digests();

    assert_eq!(
        lower_drained,
        vec![small_digest],
        "SizePartitioningStore::mark_stable must route digests with \
         size_bytes < partition_size to lower_store; got lower={:?}, \
         upper={:?}. A regression that swapped the lower/upper push \
         targets in size_partitioning_store.rs::mark_stable would land \
         the small digest on the upper queue and the large digest on \
         the lower queue, silently misrouting BIS notifications. The \
         production-composition test in \
         mark_stable_on_blobs_available_test.rs uses only digests \
         below the 16 KiB threshold and would NOT catch the swap.",
        lower_drained, upper_drained,
    );
    assert_eq!(
        upper_drained,
        vec![large_digest],
        "SizePartitioningStore::mark_stable must route digests with \
         size_bytes >= partition_size to upper_store; got lower={:?}, \
         upper={:?}.",
        lower_drained, upper_drained,
    );

    Ok(())
}
