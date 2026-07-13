// Copyright 2025 The NativeLink Authors. All rights reserved.
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

use core::time::Duration;
use std::sync::Arc;

use aws_sdk_s3::config::{BehaviorVersion, Builder, Region};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use http::status::StatusCode;
use nativelink_config::stores::{
    CommonObjectSpec, ExperimentalOntapS3Spec, FastSlowSpec, MemorySpec, OntapS3ExistenceCacheSpec,
    Retry, StoreDirection, StoreSpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::ontap_s3_existence_cache_store::OntapS3ExistenceCache;
use nativelink_store::ontap_s3_store::OntapS3Store;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::spawn;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

const BUCKET_NAME: &str = "ontap-test-bucket";
const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VSERVER_NAME: &str = "testvserver";

async fn create_test_store(mock_client: StaticReplayClient) -> Result<Store, Error> {
    // Rustls 0.23 requires a process-level CryptoProvider to be installed
    // before any TLS config is built (see `OntapS3Store::new` →
    // `ClientConfig::builder()`). The production binary installs one in
    // `src/bin/nativelink.rs:1068`; tests must do the same. Idempotent:
    // returns Err(_) if a provider is already installed by another test
    // running in parallel — that's fine, the existing one is used.
    drop(rustls::crypto::aws_lc_rs::default_provider().install_default());

    // Create a temporary directory for the cache file
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let cache_path = temp_dir
        .path()
        .join("cache_index.json")
        .to_str()
        .unwrap()
        .to_string();

    let _test_config = Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::from_static(VSERVER_NAME))
        .http_client(mock_client)
        .build();

    let ontap_s3_spec = ExperimentalOntapS3Spec {
        endpoint: "https://example.com".to_string(),
        vserver_name: VSERVER_NAME.to_string(),
        bucket: BUCKET_NAME.to_string(),
        root_certificates: None,
        common: CommonObjectSpec {
            key_prefix: None,
            retry: Retry::default(),
            consider_expired_after_s: 0,
            max_retry_buffer_per_request: None,
            multipart_max_concurrent_uploads: None,
            insecure_allow_http: false,
            disable_http2: false,
        },
    };

    let cache_spec = OntapS3ExistenceCacheSpec {
        index_path: cache_path,
        sync_interval_seconds: 10,
        backend: Box::new(ontap_s3_spec),
    };

    let store_manager = Arc::new(StoreManager::new());
    store_factory(
        &StoreSpec::OntapS3ExistenceCache(Box::new(cache_spec)),
        &store_manager,
        None,
    )
    .await
}

#[nativelink_test]
async fn test_zero_digest_handling() -> Result<(), Error> {
    // Setup a mock client that doesn't expect any calls (zero digest is handled locally)
    let mock_client = StaticReplayClient::new(vec![]);

    let store = create_test_store(mock_client).await?;

    // Create the empty/zero digest
    let zero_digest = DigestInfo::new(Sha256::new().finalize().into(), 0);

    // has/exists check
    let result = store.has(zero_digest).await?;
    assert_eq!(result, Some(0), "Zero digest should exist with size 0");

    // get_part check
    let (mut writer, mut reader) = make_buf_channel_pair();
    let store_clone = store.clone();
    let _drop_guard = spawn!("zero_digest_test", async move {
        store_clone
            .get_part(zero_digest, &mut writer, 0, None)
            .await
            .unwrap();
    });

    let data = reader.consume(Some(1024)).await?;
    assert_eq!(data, Bytes::new(), "Zero digest should return empty data");

    Ok(())
}

#[nativelink_test]
async fn test_get_part_not_in_cache() -> Result<(), Error> {
    // Setup client with empty response
    let mock_client = StaticReplayClient::new(vec![
        // List objects response (empty)
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/?list-type=2&max-keys=1000&prefix="
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .body(
                    SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Name>ontap-test-bucket</Name>
                            <Prefix></Prefix>
                            <KeyCount>0</KeyCount>
                            <MaxKeys>1000</MaxKeys>
                            <IsTruncated>false</IsTruncated>
                        </ListBucketResult>"#,
                    )
                )
                .unwrap(),
        ),
        // Head object request (not found)
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/{VALID_HASH1}-10?x-id=HeadObject"
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(SdkBody::empty())
                .unwrap(),
        ),
    ]);

    let store = create_test_store(mock_client).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let test_digest = DigestInfo::try_new(VALID_HASH1, 10)?;

    // Try to get part - should fail with NotFound
    let result = store.get_part_unchunked(test_digest, 0, None).await;
    assert!(
        result.is_err(),
        "get_part should fail for object not in cache"
    );
    assert_eq!(
        result.unwrap_err().code,
        nativelink_error::Code::NotFound,
        "Error should be NotFound"
    );

    Ok(())
}
#[nativelink_test]
async fn test_cache_population() -> Result<(), Error> {
    // Setup a mock client that returns a list of objects
    let mock_client = StaticReplayClient::new(vec![
        // List objects response with some test objects
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/?list-type=2&max-keys=1000&prefix="
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .body(
                    SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Name>ontap-test-bucket</Name>
                            <Prefix></Prefix>
                            <KeyCount>2</KeyCount>
                            <MaxKeys>1000</MaxKeys>
                            <IsTruncated>false</IsTruncated>
                            <Contents>
                                <Key>0123456789abcdef000000000000000000010000000000000123456789abcdef-100</Key>
                                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                                <Size>100</Size>
                            </Contents>
                            <Contents>
                                <Key>0123456789abcdef000000000000000000020000000000000123456789abcdef-200</Key>
                                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                                <Size>200</Size>
                            </Contents>
                        </ListBucketResult>"#,
                    )
                )
                .unwrap(),
        ),
        // Head object request for the first object (when checking it exists)
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/0123456789abcdef000000000000000000010000000000000123456789abcdef-100?x-id=HeadObject"
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", "100")
                .body(SdkBody::empty())
                .unwrap(),
        ),
        // Head object request for the second object (we'll check it)
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/0123456789abcdef000000000000000000020000000000000123456789abcdef-200?x-id=HeadObject"
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", "200")
                .body(SdkBody::empty())
                .unwrap(),
        ),
    ]);

    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let cache_path = temp_dir
        .path()
        .join("cache_index.json")
        .to_str()
        .unwrap()
        .to_string();

    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::from_static(VSERVER_NAME))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);

    let ontap_s3_store = OntapS3Store::new_with_client_and_jitter(
        &(ExperimentalOntapS3Spec {
            bucket: BUCKET_NAME.to_string(),
            vserver_name: VSERVER_NAME.to_string(),
            endpoint: "https://example.com".to_string(),
            ..Default::default()
        }),
        s3_client.clone(),
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let inner_store = Store::new(ontap_s3_store);

    let empty_digests = std::collections::HashSet::new();

    let existence_cache = OntapS3ExistenceCache::new_for_testing(
        inner_store,
        Arc::new(s3_client),
        cache_path.clone(),
        empty_digests,
        10,
        MockInstantWrapped::default,
    );

    let store = Store::new(existence_cache.clone());

    existence_cache.run_sync().await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify the first object was correctly added to the cache
    let digest1 = DigestInfo::try_new(
        "0123456789abcdef000000000000000000010000000000000123456789abcdef",
        100,
    )?;
    let result = store.has(digest1).await?;
    assert_eq!(
        result,
        Some(100),
        "Object should exist in cache after population"
    );

    // Verify the second object was parsed correctly
    let digest2 = DigestInfo::try_new(
        "0123456789abcdef000000000000000000020000000000000123456789abcdef",
        200,
    )?;

    let mut check_result = [None];
    store
        .has_with_results(&[digest2.into()], &mut check_result)
        .await?;
    let in_cache = check_result[0].is_some();

    assert!(in_cache, "Second object should be in the cache");

    // Check that the cache was persisted to disk
    let cache_content = tokio::fs::read_to_string(&cache_path).await?;
    assert!(
        cache_content.contains("0123456789abcdef000000000000000000010000000000000123456789abcdef"),
        "First digest should be persisted in cache file"
    );
    assert!(
        cache_content.contains("0123456789abcdef000000000000000000020000000000000123456789abcdef"),
        "Second digest should be persisted in cache file"
    );

    Ok(())
}

#[nativelink_test]
async fn test_cache_sync_multiple_objects() -> Result<(), Error> {
    // Setup a mock client with multiple objects
    let mock_client = StaticReplayClient::new(vec![
        // List objects response with multiple test objects
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/?list-type=2&max-keys=1000&prefix="
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .body(
                    SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Name>ontap-test-bucket</Name>
                            <Prefix></Prefix>
                            <KeyCount>3</KeyCount>
                            <MaxKeys>1000</MaxKeys>
                            <IsTruncated>false</IsTruncated>
                            <Contents>
                                <Key>0123456789abcdef000000000000000000010000000000000123456789abcdef-100</Key>
                                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                                <Size>100</Size>
                            </Contents>
                            <Contents>
                                <Key>0123456789abcdef000000000000000000020000000000000123456789abcdef-200</Key>
                                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                                <Size>200</Size>
                            </Contents>
                            <Contents>
                                <Key>0123456789abcdef000000000000000000030000000000000123456789abcdef-300</Key>
                                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                                <Size>300</Size>
                            </Contents>
                        </ListBucketResult>"#,
                    )
                )
                .unwrap(),
        ),
        // Head object requests for each object
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/0123456789abcdef000000000000000000010000000000000123456789abcdef-100?x-id=HeadObject"
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", "100")
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/0123456789abcdef000000000000000000020000000000000123456789abcdef-200?x-id=HeadObject"
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", "200")
                .body(SdkBody::empty())
                .unwrap(),
        ),
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/0123456789abcdef000000000000000000030000000000000123456789abcdef-300?x-id=HeadObject"
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", "300")
                .body(SdkBody::empty())
                .unwrap(),
        ),
    ]);

    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let cache_path = temp_dir
        .path()
        .join("cache_index.json")
        .to_str()
        .unwrap()
        .to_string();

    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::from_static(VSERVER_NAME))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);

    let ontap_s3_store = OntapS3Store::new_with_client_and_jitter(
        &(ExperimentalOntapS3Spec {
            bucket: BUCKET_NAME.to_string(),
            vserver_name: VSERVER_NAME.to_string(),
            endpoint: "https://example.com".to_string(),
            ..Default::default()
        }),
        s3_client.clone(),
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let existence_cache = OntapS3ExistenceCache::new_for_testing(
        Store::new(ontap_s3_store),
        Arc::new(s3_client),
        cache_path.clone(),
        std::collections::HashSet::new(),
        10,
        MockInstantWrapped::default,
    );

    // Manually trigger sync
    existence_cache.run_sync().await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let store = Store::new(existence_cache);

    // Verify all three objects were added to the cache
    let digests = [
        DigestInfo::try_new(
            "0123456789abcdef000000000000000000010000000000000123456789abcdef",
            100,
        )?,
        DigestInfo::try_new(
            "0123456789abcdef000000000000000000020000000000000123456789abcdef",
            200,
        )?,
        DigestInfo::try_new(
            "0123456789abcdef000000000000000000030000000000000123456789abcdef",
            300,
        )?,
    ];

    // Check existence of each digest
    let mut check_results = [None, None, None];
    store
        .has_with_results(&digests.map(Into::into), &mut check_results)
        .await?;

    // Verify sizes are correct
    assert_eq!(
        check_results.map(|r| r.unwrap_or(0)),
        [100, 200, 300],
        "Object sizes should match"
    );

    Ok(())
}
#[nativelink_test]
async fn test_empty_bucket_handling() -> Result<(), Error> {
    // #sibling-hunt V1 behavior change: `has()` on a cache MISS now falls
    // back to the inner store's `has()` (an S3 HEAD) instead of returning
    // NotFound unconditionally, so this test must mock the HEAD too. The
    // intent is unchanged — the object is genuinely absent, so the inner
    // HEAD returns NotFound and `has()` still returns `None` (now the
    // authoritative answer rather than a bare cache-miss).
    //
    // Build the inner store directly with the mock-client config (the
    // anonymous-credentials pattern used by the other cache tests). The
    // previous `create_test_store` helper routes through
    // `store_factory`/`DefaultCredentialsChain`, which resolves real AWS
    // credentials — fine while `has()` never reached the inner store on a
    // miss, but the V1 fallback now issues a real HEAD that needs a
    // signable client.
    let mock_client = StaticReplayClient::new(vec![
        // Cache-population sync: empty bucket.
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/?list-type=2&max-keys=1000&prefix="
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::OK)
                .body(
                    SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Name>ontap-test-bucket</Name>
                            <Prefix></Prefix>
                            <KeyCount>0</KeyCount>
                            <MaxKeys>1000</MaxKeys>
                            <IsTruncated>false</IsTruncated>
                        </ListBucketResult>"#,
                    )
                )
                .unwrap(),
        ),
        // Inner-store HEAD for the cache-miss fallback: object absent.
        ReplayEvent::new(
            http::Request::builder()
                .uri(format!(
                    "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/{VALID_HASH1}-100?x-id=HeadObject"
                ))
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(SdkBody::empty())
                .unwrap(),
        ),
    ]);

    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let cache_path = temp_dir
        .path()
        .join("cache_index.json")
        .to_str()
        .unwrap()
        .to_string();

    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2025_08_07())
        .region(Region::from_static(VSERVER_NAME))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);

    let ontap_s3_store = OntapS3Store::new_with_client_and_jitter(
        &(ExperimentalOntapS3Spec {
            bucket: BUCKET_NAME.to_string(),
            vserver_name: VSERVER_NAME.to_string(),
            endpoint: "https://example.com".to_string(),
            ..Default::default()
        }),
        s3_client.clone(),
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    let existence_cache = OntapS3ExistenceCache::new_for_testing(
        Store::new(ontap_s3_store),
        Arc::new(s3_client),
        cache_path,
        std::collections::HashSet::new(),
        10,
        MockInstantWrapped::default,
    );
    // Run the cache sync (empty bucket → empty cache).
    existence_cache.run_sync().await?;
    let store = Store::new(existence_cache);

    // Check that no objects exist. Cache miss → inner HEAD → NotFound → None.
    let test_digest = DigestInfo::try_new(VALID_HASH1, 100)?;
    let result = store.has(test_digest).await?;

    assert_eq!(
        result, None,
        "Empty bucket should not add any objects to cache"
    );
    Ok(())
}

/// #sibling-hunt V1: a cache MISS must fall back to the inner store's
/// `has()` rather than unconditionally returning NotFound. Without the
/// fallback, the async `sync_cache` full-overwrite (`sync_cache` sets
/// `self.digests = new_digests`) can DROP a digest `D` that a concurrent
/// `update(D)` inserted after the S3 listing snapshot (put→list lag): the
/// overwrite replaces the map with a listing that predates the insert, so
/// `has(D)` served a stale-NEGATIVE (NotFound for a blob that IS in S3).
///
/// This test reproduces the POST-OVERWRITE stale state directly: the
/// in-memory `digests` set does NOT contain `D` (as if a sync overwrite
/// just dropped it) while the inner OntapS3Store DOES have `D` (S3 HEAD
/// returns 200). The fix makes `has(D)` consult the inner store on the
/// cache miss and return `Some(size)`.
///
/// Mutation: comment out the inner-store fallback branch in
/// `has_with_results`; this test must fail with the bespoke
/// "stale-NEGATIVE" message (the cache miss would return `None`).
#[nativelink_test]
async fn has_falls_back_to_inner_store_on_cache_miss() -> Result<(), Error> {
    const MISSING_HASH: &str =
        "00000000000000000000000000000000000000000000000000000000000000ff";
    const BLOB_SIZE: i64 = 4242;

    // The inner OntapS3Store's `has()` issues an S3 HEAD; reply 200 with
    // the blob's content-length so the inner store reports it present.
    let mock_client = StaticReplayClient::new(vec![ReplayEvent::new(
        http::Request::builder()
            .uri(format!(
                "https://{BUCKET_NAME}.s3.{VSERVER_NAME}.amazonaws.com/{MISSING_HASH}-{BLOB_SIZE}?x-id=HeadObject"
            ))
            .body(SdkBody::empty())
            .unwrap(),
        http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Length", BLOB_SIZE.to_string())
            .body(SdkBody::empty())
            .unwrap(),
    )]);

    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let cache_path = temp_dir
        .path()
        .join("cache_index.json")
        .to_str()
        .unwrap()
        .to_string();

    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2025_08_07())
        .region(Region::from_static(VSERVER_NAME))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);

    let ontap_s3_store = OntapS3Store::new_with_client_and_jitter(
        &(ExperimentalOntapS3Spec {
            bucket: BUCKET_NAME.to_string(),
            vserver_name: VSERVER_NAME.to_string(),
            endpoint: "https://example.com".to_string(),
            ..Default::default()
        }),
        s3_client.clone(),
        Arc::new(move |_delay| Duration::from_secs(0)),
        MockInstantWrapped::default,
    )?;

    // Seed the existence cache with an EMPTY digest set — this is exactly
    // the post-overwrite stale state where a fresh `D` was dropped by a
    // racing `sync_cache`.
    let existence_cache = OntapS3ExistenceCache::new_for_testing(
        Store::new(ontap_s3_store),
        Arc::new(s3_client),
        cache_path,
        std::collections::HashSet::new(),
        10,
        MockInstantWrapped::default,
    );
    let store = Store::new(existence_cache);

    let missing_digest = DigestInfo::try_new(MISSING_HASH, BLOB_SIZE as u64)?;
    let mut results = [None];
    store
        .has_with_results(&[missing_digest.into()], &mut results)
        .await?;

    assert_eq!(
        results[0],
        Some(BLOB_SIZE as u64),
        "stale-NEGATIVE: has() returned {:?} for a digest absent from the \
         in-memory cache but present in the inner store (S3). The \
         sync_cache full-overwrite can drop a concurrently-inserted \
         digest; has() MUST fall back to inner_store.has() on a cache \
         miss instead of returning NotFound.",
        results[0],
    );

    Ok(())
}

/// Regression for red-team F3 + #140: OntapS3ExistenceCache must delegate
/// `mark_stable` to its inner_store. Without an explicit override, the
/// trait's silent no-op default would swallow the call, breaking the
/// BIS pin-release pipeline at this layer.
///
/// Wraps a FastSlowStore (Memory→Memory) inside the existence cache.
/// Calls mark_stable on the outer cache; drains the inner FastSlowStore.
#[nativelink_test]
async fn mark_stable_delegates_to_inner_store_test() -> Result<(), Error> {
    // Empty mock S3 client — the test never makes network calls because
    // mark_stable does not touch the cache or S3 paths.
    let mock_client = StaticReplayClient::new(vec![]);
    let test_config = Builder::new()
        .behavior_version(BehaviorVersion::v2025_08_07())
        .region(Region::from_static("test-region"))
        .http_client(mock_client.clone())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(test_config);

    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let cache_path = temp_dir
        .path()
        .join("cache_index.json")
        .to_str()
        .unwrap()
        .to_string();

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

    let existence_cache = OntapS3ExistenceCache::new_for_testing(
        inner_fast_slow.clone(),
        Arc::new(s3_client),
        cache_path,
        std::collections::HashSet::new(),
        10,
        MockInstantWrapped::default,
    );

    let digest = DigestInfo::new([9u8; 32], 100);
    let outer = Store::new(existence_cache);
    outer.as_store_driver().mark_stable(&[digest]);

    let drained = inner_fast_slow.as_store_driver().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "OntapS3ExistenceCache::mark_stable must delegate to inner_store. \
         Without this delegation the trait silent-default no-op swallows \
         the call and the worker's pin (durable under v2) leaks. \
         Drained: {drained:?}"
    );

    Ok(())
}
