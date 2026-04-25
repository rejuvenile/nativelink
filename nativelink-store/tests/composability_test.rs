// Copyright 2024-2026 The NativeLink Authors. All rights reserved.
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

//! Generic composability harness asserting that wrapping a store in
//! `VerifyStore` (the canonical production composition) does NOT
//! deadlock when the inner `get_part` returns an error.
//!
//! Background: `VerifyStore::get_part` runs `tokio::join!(get_fut, check_fut)`
//! over a freshly-built tx/rx pair. `check_fut` blocks on `rx.recv().await`. If
//! the inner `get_part` returns Err WITHOUT calling `writer.send_eof()` or
//! `writer.send_error(err)`, the wrapping `tx` is never closed and the join!
//! deadlocks forever — Bazel builds wedge for hours. The `WriteHalfGuard` RAII
//! drop guard added in `nativelink-util/src/buf_channel.rs` enforces termination
//! at the type level so this class of bug becomes impossible by construction.
//!
//! Each test in this file constructs a sibling store, wraps it in
//! `VerifyStore`, triggers an error path, and asserts the wrapped get_part
//! returns Err WITHIN A FEW SECONDS. The `tokio::time::timeout` is the
//! deadlock detector — without it, the test would hang the CI runner instead
//! of producing a meaningful failure.

use core::time::Duration;
use std::env;

use nativelink_config::stores::{
    CompressionAlgorithm, CompressionSpec, DedupSpec, EvictionPolicy, FilesystemSpec, Lz4Config,
    MemorySpec, SizePartitioningSpec, StoreSpec, VerifySpec,
};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_store::compression_store::CompressionStore;
use nativelink_store::dedup_store::DedupStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
// Intentionally minimal imports — the harness wraps each leaf in
// VerifyStore (the production composition for the server CAS path);
// no direct buf_channel manipulation is needed.
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use rand::Rng;

/// Hash that `VerifyStore` will recognize as a digest key but the inner store
/// will reject as NotFound. Triggers the writer-termination contract on Err.
const MISSING_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Tight upper bound on a non-deadlocked Err. Production wedges shipped at 30s+;
/// a healthy `Err(NotFound)` round-trip through VerifyStore is sub-millisecond.
/// 5s leaves headroom for a slow CI runner without masking real deadlocks.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/composability-{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data
    )
}

/// Wrap `inner_store` in `VerifyStore` (verify_size=true, the canonical
/// production composition for the server's CAS path) and assert that
/// `get_part_unchunked` for `digest` returns Err **within `timeout`** — i.e.
/// does NOT deadlock. The `tokio::time::timeout` is the deadlock detector.
///
/// On success: returns the received `Error`. On deadlock or unexpected Ok:
/// panics with a detailed message naming the test so log scrapers can
/// attribute the failure quickly.
async fn assert_no_deadlock_under_verify_store(
    test_name: &'static str,
    inner_store: Store,
    digest: DigestInfo,
    expected_code: Code,
    timeout: Duration,
) -> Error {
    // backend in the spec is a placeholder; the actual delegate is the
    // `inner_store` arg passed to VerifyStore::new.
    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        inner_store,
    );

    let fut = verify_store.get_part_unchunked(digest, 0, None);
    let res = tokio::time::timeout(timeout, fut).await;

    match res {
        Err(_elapsed) => {
            panic!(
                "DEADLOCK DETECTED in {test_name}: VerifyStore-wrapped get_part_unchunked did \
                 not return within {timeout:?}. The inner store violated the writer-termination \
                 contract: `get_part` returned Err without calling writer.send_eof / send_error, \
                 so VerifyStore's check_fut blocked forever on rx.recv(). Apply WriteHalfGuard \
                 to the inner store's get_part exit paths."
            );
        }
        Ok(Ok(bytes)) => {
            panic!(
                "{test_name}: expected Err({expected_code:?}) but got Ok({} bytes). The harness \
                 must trigger an error path to exercise the contract.",
                bytes.len()
            );
        }
        Ok(Err(err)) => {
            assert_eq!(
                err.code, expected_code,
                "{test_name}: expected Err code {expected_code:?}, got {:?} ({err})",
                err.code,
            );
            err
        }
    }
}

// -----------------------------------------------------------------------
// Per-store composability tests. Each constructs the inner store in a
// state where get_part will Err, wraps in VerifyStore, asserts no deadlock.
// -----------------------------------------------------------------------

#[nativelink_test]
async fn verify_store_around_memory_does_not_deadlock_on_get_part_err() -> Result<(), Error> {
    // Empty MemoryStore: get_part for any non-zero digest returns NotFound.
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    assert_no_deadlock_under_verify_store(
        "verify_store_around_memory_does_not_deadlock_on_get_part_err",
        inner,
        digest,
        Code::NotFound,
        NO_DEADLOCK_TIMEOUT,
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn verify_store_around_filesystem_does_not_deadlock_on_get_part_err() -> Result<(), Error> {
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");
    let inner = Store::new(
        FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
            content_path,
            temp_path,
            eviction_policy: Some(EvictionPolicy {
                max_count: 1024,
                ..Default::default()
            }),
            block_size: 1,
            ..Default::default()
        })
        .await?,
    );
    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    assert_no_deadlock_under_verify_store(
        "verify_store_around_filesystem_does_not_deadlock_on_get_part_err",
        inner,
        digest,
        Code::NotFound,
        NO_DEADLOCK_TIMEOUT,
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn verify_store_around_worker_proxy_does_not_deadlock_on_get_part_err() -> Result<(), Error>
{
    // WorkerProxyStore backed by an empty MemoryStore + empty locality map:
    // get_part for any non-zero digest returns NotFound (no inner hit, no
    // peers to race).
    let inner_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner_mem, locality_map);
    let proxy_store = Store::new(proxy);
    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    assert_no_deadlock_under_verify_store(
        "verify_store_around_worker_proxy_does_not_deadlock_on_get_part_err",
        proxy_store,
        digest,
        Code::NotFound,
        NO_DEADLOCK_TIMEOUT,
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn verify_store_around_size_partitioning_does_not_deadlock_on_get_part_err()
-> Result<(), Error> {
    // SizePartitioningStore over two empty MemoryStores: get_part returns
    // NotFound from whichever side the partition selects.
    let spec = SizePartitioningSpec {
        size: 1024,
        lower_store: StoreSpec::Memory(MemorySpec::default()),
        upper_store: StoreSpec::Memory(MemorySpec::default()),
    };
    let lower = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper = Store::new(MemoryStore::new(&MemorySpec::default()));
    let part = SizePartitioningStore::new(&spec, lower, upper);
    let part_store = Store::new(part);
    // size >= 1024 selects upper.
    let digest = DigestInfo::try_new(MISSING_HASH, 4096)?;
    assert_no_deadlock_under_verify_store(
        "verify_store_around_size_partitioning_does_not_deadlock_on_get_part_err",
        part_store,
        digest,
        Code::NotFound,
        NO_DEADLOCK_TIMEOUT,
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn verify_store_around_compression_does_not_deadlock_on_get_part_err() -> Result<(), Error> {
    // CompressionStore over an empty inner: get_part returns NotFound from
    // the inner because no compressed blob exists for the digest.
    let spec = CompressionSpec {
        backend: StoreSpec::Memory(MemorySpec::default()),
        compression_algorithm: CompressionAlgorithm::Lz4(Lz4Config::default()),
    };
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = CompressionStore::new(&spec, inner)?;
    let store_handle = Store::new(store);
    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    assert_no_deadlock_under_verify_store(
        "verify_store_around_compression_does_not_deadlock_on_get_part_err",
        store_handle,
        digest,
        Code::NotFound,
        NO_DEADLOCK_TIMEOUT,
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn verify_store_around_dedup_does_not_deadlock_on_get_part_err() -> Result<(), Error> {
    // DedupStore over two empty MemoryStores: get_part hits index_store first
    // (NotFound) since no dedup index exists for the digest.
    let spec = DedupSpec {
        index_store: StoreSpec::Memory(MemorySpec::default()),
        content_store: StoreSpec::Memory(MemorySpec::default()),
        min_size: 8 * 1024,
        normal_size: 32 * 1024,
        max_size: 128 * 1024,
        max_concurrent_fetch_per_get: 10,
    };
    let index = Store::new(MemoryStore::new(&MemorySpec::default()));
    let content = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = DedupStore::new(&spec, index, content)?;
    let store_handle = Store::new(store);
    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    assert_no_deadlock_under_verify_store(
        "verify_store_around_dedup_does_not_deadlock_on_get_part_err",
        store_handle,
        digest,
        Code::NotFound,
        NO_DEADLOCK_TIMEOUT,
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn verify_store_around_verify_store_does_not_deadlock_on_get_part_err() -> Result<(), Error>
{
    // Defense in depth: nested VerifyStore. The inner-most layer Errs from
    // an empty MemoryStore; the middle and outer VerifyStores must each
    // honor the writer-termination contract.
    let memory_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let inner_verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        memory_store,
    );
    let inner_verify_store = Store::new(inner_verify);
    let digest = DigestInfo::try_new(MISSING_HASH, 100)?;
    assert_no_deadlock_under_verify_store(
        "verify_store_around_verify_store_does_not_deadlock_on_get_part_err",
        inner_verify_store,
        digest,
        Code::NotFound,
        NO_DEADLOCK_TIMEOUT,
    )
    .await;
    Ok(())
}

