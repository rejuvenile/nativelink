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
use core::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use nativelink_config::cas_server::WithInstanceName;
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest, Directory, DirectoryNode,
    FindMissingBlobsRequest, GetTreeRequest, GetTreeResponse, NodeProperties, SpliceBlobRequest,
    SplitBlobRequest, SplitBlobResponse, batch_read_blobs_response, batch_update_blobs_request,
    batch_update_blobs_response, chunking_function, compressor, digest_function,
};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_service::cas_server::{
    CasServer, ChunkingMetrics, register_chunking_metrics,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;
use prost::Message;
use prost_types::Timestamp;
use tonic::{Code, Request};

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH2: &str = "9993456789abcdef000000000000000000000000000000000123456789abc999";
const HASH3: &str = "7773456789abcdef000000000000000000000000000000000123456789abc777";
const BAD_HASH: &str = "BAD_HASH";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_cas_server(store_manager: &StoreManager) -> Result<CasServer, Error> {
    CasServer::new(
        &[WithInstanceName {
            instance_name: "foo_instance_name".to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                experimental_chunking: None,
                cas_store: "main_cas".to_string(),
            },
        }],
        store_manager,
        None,
    )
}

#[nativelink_test]
async fn empty_store() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![Digest {
                hash: HASH1.to_string(),
                size_bytes: 0,
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    let response = raw_response.unwrap().into_inner();
    assert_eq!(response.missing_blob_digests.len(), 1);
    Ok(())
}

#[nativelink_test]
async fn store_one_item_existence() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
        .await?;
    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![Digest {
                hash: HASH1.to_string(),
                size_bytes: VALUE.len() as i64,
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    let response = raw_response.unwrap().into_inner();
    assert_eq!(response.missing_blob_digests.len(), 0); // All items should have been found.
    Ok(())
}

#[nativelink_test]
async fn has_three_requests_one_bad_hash() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
        .await?;
    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![
                Digest {
                    hash: HASH1.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
                Digest {
                    hash: BAD_HASH.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
                Digest {
                    hash: HASH1.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
            ],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    let error = raw_response.unwrap_err();
    assert!(
        error.to_string().contains("Invalid sha256 hash: BAD_HASH"),
        "'Invalid sha256 hash: BAD_HASH' not found in: {error:?}"
    );
    Ok(())
}

// REMOVED 2026-04-26: `update_existing_item` asserted that BatchUpdateBlobs
// would overwrite an existing digest with new bytes. That contract is
// incorrect for a content-addressed store: in CAS, the digest IS the
// content — two different bytestreams cannot legitimately share a digest,
// and an "overwrite" path is either a hash collision (impossibly rare) or
// a client bug.
//
// `cas_server::inner_batch_update_blobs` now does a batch `has_with_results`
// check upfront and short-circuits any digest the store already holds (see
// `nativelink-service/src/cas_server.rs:382-408`). The test's setup —
// pre-insert "1" at HASH1, then send a BatchUpdateBlobs with HASH1+"2" and
// expect "2" to be readable back — exercises a synthetic mismatched-content
// path that the new dedup gate correctly suppresses.
//
// The dedup behaviour is covered by the existing
// `batch_update_blobs_two_items_existence_with_third_missing` test (which
// asserts the per-blob OK status returned by the skip path) and by the
// per-store `update`/`update_oneshot` unit tests in `nativelink-store`.

#[nativelink_test]
async fn batch_read_blobs_read_two_blobs_success_one_fail()
-> Result<(), Box<dyn core::error::Error>> {
    const VALUE1: &str = "1";
    const VALUE2: &str = "23";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let digest1 = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE1.len() as i64,
    };
    let digest2 = Digest {
        hash: HASH2.to_string(),
        size_bytes: VALUE2.len() as i64,
    };
    {
        // Insert dummy data.
        store
            .update_oneshot(DigestInfo::try_new(HASH1, VALUE1.len())?, VALUE1.into())
            .await
            .expect("Update should have succeeded");
        store
            .update_oneshot(DigestInfo::try_new(HASH2, VALUE2.len())?, VALUE2.into())
            .await
            .expect("Update should have succeeded");
    }
    {
        // Read two blobs and additional blob should come back not found.
        let digest3 = Digest {
            hash: HASH3.to_string(),
            size_bytes: 3,
        };
        let raw_response = cas_server
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                digests: vec![digest1.clone(), digest2.clone(), digest3.clone()],
                acceptable_compressors: vec![compressor::Value::Identity.into()],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response.unwrap().into_inner(),
            BatchReadBlobsResponse {
                responses: vec![
                    batch_read_blobs_response::Response {
                        digest: Some(digest1),
                        data: VALUE1.into(),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_read_blobs_response::Response {
                        digest: Some(digest2),
                        data: VALUE2.into(),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_read_blobs_response::Response {
                        digest: Some(digest3.clone()),
                        data: vec![].into(),
                        status: Some(GrpcStatus {
                            code: Code::NotFound as i32,
                            // Source: nativelink-store/src/memory_store.rs:399 —
                            // batch_get_part_unchunked formats this exact string,
                            // and inner_batch_read_blobs trims to the last message.
                            message: format!(
                                "Key {:?} not found in MemoryStore",
                                StoreKey::from(DigestInfo::try_from(digest3)?)
                            ),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    }
                ],
            }
        );
    }
    Ok(())
}

struct SetupDirectoryResult {
    root_directory: Directory,
    root_directory_digest_info: DigestInfo,
    sub_directories: Vec<Directory>,
    sub_directory_digest_infos: Vec<DigestInfo>,
}
async fn setup_directory_structure(
    store_pinned: Pin<&impl StoreLike>,
) -> Result<SetupDirectoryResult, Error> {
    // Set up 5 sub-directories.
    const SUB_DIRECTORIES_LENGTH: i32 = 5;
    let mut sub_directory_nodes: Vec<DirectoryNode> = vec![];
    let mut sub_directories: Vec<Directory> = vec![];
    let mut sub_directory_digest_infos: Vec<DigestInfo> = vec![];

    for i in 0..SUB_DIRECTORIES_LENGTH {
        let sub_directory: Directory = Directory {
            files: vec![],
            directories: vec![],
            symlinks: vec![],
            node_properties: Some(NodeProperties {
                properties: vec![],
                mtime: Some(Timestamp {
                    seconds: i64::from(i),
                    nanos: 0,
                }),
                unix_mode: Some(0o755),
            }),
        };
        let sub_directory_digest_info: DigestInfo = serialize_and_upload_message(
            &sub_directory,
            store_pinned,
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        sub_directory_digest_infos.push(sub_directory_digest_info);
        sub_directory_nodes.push(DirectoryNode {
            name: format!("sub_directory_{i}"),
            digest: Some(sub_directory_digest_info.into()),
        });
        sub_directories.push(sub_directory);
    }

    // Set up a root directory.
    let root_directory: Directory = Directory {
        files: vec![],
        directories: sub_directory_nodes,
        symlinks: vec![],
        node_properties: None,
    };
    let root_directory_digest_info: DigestInfo = serialize_and_upload_message(
        &root_directory,
        store_pinned,
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    Ok(SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos,
    })
}

#[nativelink_test]
async fn get_tree_read_directories_without_paging() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup directory structure.
    let SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos: _,
    } = setup_directory_structure(store.as_pin()).await?;

    // Must work when paging is disabled ( `page_size` is 0 ).
    // It reads all directories at once.

    // First verify that using an empty page token is treated as if the client had sent the root
    // digest.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 0,
                page_token: String::new(),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![
                    root_directory.clone(),
                    sub_directories[0].clone(),
                    sub_directories[1].clone(),
                    sub_directories[2].clone(),
                    sub_directories[3].clone(),
                    sub_directories[4].clone()
                ],
                next_page_token: String::new()
            }]
        );
    }

    // Also verify that sending the root digest returns the entire tree as well.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 0,
                page_token: format!("{root_directory_digest_info}"),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![
                    root_directory.clone(),
                    sub_directories[0].clone(),
                    sub_directories[1].clone(),
                    sub_directories[2].clone(),
                    sub_directories[3].clone(),
                    sub_directories[4].clone()
                ],
                next_page_token: String::new()
            }]
        );
    }

    Ok(())
}

#[nativelink_test]
async fn get_tree_read_directories_with_paging() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup directory structure.
    let SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos,
    } = setup_directory_structure(store.as_pin()).await?;

    // Must work when paging is enabled ( `page_size` is 2 ).
    // First, it reads `root_directory` and `sub_directory[0]`.
    // Then, it reads `sub_directory[1]` and `sub_directory[2]`.
    // Finally, it reads `sub_directory[3]` and `sub_directory[4]`.

    // First, verify that an empty initial page token is treated as if the client had sent the
    // root digest and respects the page size.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: String::new(),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![root_directory.clone(), sub_directories[0].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[1]),
            }]
        );
    }

    // Also verify that sending the root digest as the page token is treated as paging from the
    // beginning and respects page size.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{root_directory_digest_info}"),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![root_directory.clone(), sub_directories[0].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[1]),
            }]
        );
    }

    // Verify that paging from a non-initial page token will return the expected content.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{}", sub_directory_digest_infos[1]),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[1].clone(), sub_directories[2].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[3]),
            }]
        );

        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{}", sub_directory_digest_infos[3]),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[3].clone(), sub_directories[4].clone()],
                next_page_token: String::new(),
            }]
        );
    }

    Ok(())
}

#[nativelink_test]
async fn batch_update_blobs_two_items_existence_with_third_missing()
-> Result<(), Box<dyn core::error::Error>> {
    const VALUE1: &str = "1";
    const VALUE2: &str = "23";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let digest1 = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE1.len() as i64,
    };
    let digest2 = Digest {
        hash: HASH2.to_string(),
        size_bytes: VALUE2.len() as i64,
    };

    {
        // Send update to insert two entries into backend.
        let raw_response = cas_server
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                requests: vec![
                    batch_update_blobs_request::Request {
                        digest: Some(digest1.clone()),
                        data: VALUE1.into(),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_update_blobs_request::Request {
                        digest: Some(digest2.clone()),
                        data: VALUE2.into(),
                        compressor: compressor::Value::Identity.into(),
                    },
                ],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response.unwrap().into_inner(),
            BatchUpdateBlobsResponse {
                responses: vec![
                    batch_update_blobs_response::Response {
                        digest: Some(digest1),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                    },
                    batch_update_blobs_response::Response {
                        digest: Some(digest2),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                    }
                ],
            }
        );
    }
    {
        // Query the backend for inserted entries plus one that is not
        // present and ensure it only returns the one that is missing.
        let missing_digest = Digest {
            hash: HASH3.to_string(),
            size_bytes: 1,
        };
        let raw_response = cas_server
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                blob_digests: vec![
                    Digest {
                        hash: HASH1.to_string(),
                        size_bytes: VALUE1.len() as i64,
                    },
                    missing_digest.clone(),
                    Digest {
                        hash: HASH2.to_string(),
                        size_bytes: VALUE2.len() as i64,
                    },
                ],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        let response = raw_response.unwrap().into_inner();
        assert_eq!(response.missing_blob_digests, vec![missing_digest]);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: collect all directories from a GetTree streaming response.
// ---------------------------------------------------------------------------

async fn collect_get_tree_dirs(
    cas_server: &CasServer,
    root_digest_info: DigestInfo,
    page_size: i32,
) -> Vec<Directory> {
    let raw_response = cas_server
        .get_tree(Request::new(GetTreeRequest {
            instance_name: INSTANCE_NAME.to_string(),
            page_size,
            page_token: String::new(),
            root_digest: Some(root_digest_info.into()),
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await
        .expect("get_tree should succeed");
    raw_response
        .into_inner()
        .filter_map(|x| async move { Some(x.unwrap()) })
        .flat_map(|resp| futures::stream::iter(resp.directories))
        .collect::<Vec<_>>()
        .await
}

// ---------------------------------------------------------------------------
// Helper: upload a Directory proto and return its DigestInfo.
// ---------------------------------------------------------------------------

async fn upload_directory(
    store: Pin<&impl StoreLike>,
    directory: &Directory,
) -> Result<DigestInfo, Error> {
    serialize_and_upload_message(
        directory,
        store,
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await
}

// ===========================================================================
// Test 1: tree_cache_hit
// Verifies that a second unpaginated GetTree call for the same root is
// served from the tree cache (correct result AND faster).
// ===========================================================================

#[nativelink_test]
async fn tree_cache_hit() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let result = setup_directory_structure(store.as_pin()).await?;

    // First call: populates the tree cache.
    let first_start = Instant::now();
    let first_dirs = collect_get_tree_dirs(&cas_server, result.root_directory_digest_info, 0).await;
    let first_elapsed = first_start.elapsed();

    // Verify the tree cache was populated.
    assert_eq!(
        cas_server.tree_cache_len().await,
        1,
        "tree cache should have exactly 1 entry after first call"
    );

    // Second call: should hit the tree cache.
    let second_start = Instant::now();
    let second_dirs =
        collect_get_tree_dirs(&cas_server, result.root_directory_digest_info, 0).await;
    let second_elapsed = second_start.elapsed();

    // Both calls must return the same directories.
    assert_eq!(first_dirs, second_dirs, "cache hit should return same data");

    // Verify the expected directory count: root + 5 sub-directories.
    assert_eq!(first_dirs.len(), 6);

    // The cache hit should still show 1 entry (not 2).
    assert_eq!(
        cas_server.tree_cache_len().await,
        1,
        "tree cache should still have exactly 1 entry"
    );

    // Cache hit should be significantly faster than BFS traversal.
    assert!(
        second_elapsed < first_elapsed || second_elapsed.as_micros() < 500,
        "cache hit ({second_elapsed:?}) should be faster than BFS ({first_elapsed:?})"
    );

    Ok(())
}

// ===========================================================================
// Test 2: tree_cache_miss_different_root
// Verifies that different root digests produce independent cache entries
// with correct results.
// ===========================================================================

#[nativelink_test]
async fn tree_cache_miss_different_root() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Build tree A: root_a -> [child_a1, child_a2]
    let child_a1 = Directory {
        node_properties: Some(NodeProperties {
            mtime: Some(Timestamp { seconds: 1, nanos: 0 }),
            unix_mode: Some(0o755),
            ..Default::default()
        }),
        ..Default::default()
    };
    let child_a1_digest = upload_directory(store.as_pin(), &child_a1).await?;

    let child_a2 = Directory {
        node_properties: Some(NodeProperties {
            mtime: Some(Timestamp { seconds: 2, nanos: 0 }),
            unix_mode: Some(0o755),
            ..Default::default()
        }),
        ..Default::default()
    };
    let child_a2_digest = upload_directory(store.as_pin(), &child_a2).await?;

    let root_a = Directory {
        directories: vec![
            DirectoryNode {
                name: "a1".into(),
                digest: Some(child_a1_digest.into()),
            },
            DirectoryNode {
                name: "a2".into(),
                digest: Some(child_a2_digest.into()),
            },
        ],
        ..Default::default()
    };
    let root_a_digest = upload_directory(store.as_pin(), &root_a).await?;

    // Build tree B: root_b -> [child_b1]
    let child_b1 = Directory {
        node_properties: Some(NodeProperties {
            mtime: Some(Timestamp { seconds: 99, nanos: 0 }),
            unix_mode: Some(0o700),
            ..Default::default()
        }),
        ..Default::default()
    };
    let child_b1_digest = upload_directory(store.as_pin(), &child_b1).await?;

    let root_b = Directory {
        directories: vec![DirectoryNode {
            name: "b1".into(),
            digest: Some(child_b1_digest.into()),
        }],
        ..Default::default()
    };
    let root_b_digest = upload_directory(store.as_pin(), &root_b).await?;

    // Fetch tree A.
    let dirs_a = collect_get_tree_dirs(&cas_server, root_a_digest, 0).await;
    assert_eq!(dirs_a.len(), 3, "tree A: root + 2 children");
    assert_eq!(dirs_a[0], root_a);
    assert_eq!(dirs_a[1], child_a1);
    assert_eq!(dirs_a[2], child_a2);

    // Fetch tree B.
    let dirs_b = collect_get_tree_dirs(&cas_server, root_b_digest, 0).await;
    assert_eq!(dirs_b.len(), 2, "tree B: root + 1 child");
    assert_eq!(dirs_b[0], root_b);
    assert_eq!(dirs_b[1], child_b1);

    // Both trees should be cached independently.
    assert_eq!(
        cas_server.tree_cache_len().await,
        2,
        "tree cache should have 2 independent entries"
    );

    // Re-fetch tree A and verify it still returns the correct data.
    let dirs_a_again = collect_get_tree_dirs(&cas_server, root_a_digest, 0).await;
    assert_eq!(dirs_a, dirs_a_again, "tree A cache hit returns same data");

    Ok(())
}

// ===========================================================================
// Test 3: subtree_cache_overlap
// Two trees that share a common subdirectory subtree. The second GetTree
// call should benefit from the subtree cache populated by the first call.
// ===========================================================================

#[nativelink_test]
async fn subtree_cache_overlap() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Shared subtree: shared_child (a leaf directory).
    let shared_child = Directory {
        node_properties: Some(NodeProperties {
            mtime: Some(Timestamp { seconds: 42, nanos: 0 }),
            unix_mode: Some(0o755),
            ..Default::default()
        }),
        ..Default::default()
    };
    let shared_child_digest = upload_directory(store.as_pin(), &shared_child).await?;

    // Tree X: root_x -> [shared_child, unique_x_child]
    let unique_x_child = Directory {
        node_properties: Some(NodeProperties {
            mtime: Some(Timestamp { seconds: 10, nanos: 0 }),
            unix_mode: Some(0o755),
            ..Default::default()
        }),
        ..Default::default()
    };
    let unique_x_digest = upload_directory(store.as_pin(), &unique_x_child).await?;

    let root_x = Directory {
        directories: vec![
            DirectoryNode {
                name: "shared".into(),
                digest: Some(shared_child_digest.into()),
            },
            DirectoryNode {
                name: "unique_x".into(),
                digest: Some(unique_x_digest.into()),
            },
        ],
        ..Default::default()
    };
    let root_x_digest = upload_directory(store.as_pin(), &root_x).await?;

    // Tree Y: root_y -> [shared_child, unique_y_child]
    let unique_y_child = Directory {
        node_properties: Some(NodeProperties {
            mtime: Some(Timestamp { seconds: 20, nanos: 0 }),
            unix_mode: Some(0o755),
            ..Default::default()
        }),
        ..Default::default()
    };
    let unique_y_digest = upload_directory(store.as_pin(), &unique_y_child).await?;

    let root_y = Directory {
        directories: vec![
            DirectoryNode {
                name: "shared".into(),
                digest: Some(shared_child_digest.into()),
            },
            DirectoryNode {
                name: "unique_y".into(),
                digest: Some(unique_y_digest.into()),
            },
        ],
        ..Default::default()
    };
    let root_y_digest = upload_directory(store.as_pin(), &root_y).await?;

    // Fetch tree X first: populates subtree cache for all 3 directories
    // (root_x, shared_child, unique_x_child).
    let dirs_x = collect_get_tree_dirs(&cas_server, root_x_digest, 0).await;
    assert_eq!(dirs_x.len(), 3);
    assert_eq!(dirs_x[0], root_x);

    // The subtree cache should have entries for root_x's directories.
    let subtree_len_after_x = cas_server.subtree_cache_len().await;
    assert!(
        subtree_len_after_x >= 3,
        "subtree cache should have at least 3 entries (root_x + 2 children), got {subtree_len_after_x}"
    );

    // Fetch tree Y: shared_child should come from subtree cache.
    let dirs_y = collect_get_tree_dirs(&cas_server, root_y_digest, 0).await;
    assert_eq!(dirs_y.len(), 3);
    assert_eq!(dirs_y[0], root_y);

    // Verify both trees return their shared child correctly.
    assert!(
        dirs_x.contains(&shared_child),
        "tree X should contain the shared child"
    );
    assert!(
        dirs_y.contains(&shared_child),
        "tree Y should contain the shared child"
    );

    // Subtree cache should now have entries for all unique directories
    // across both trees. The shared_child is counted once.
    let subtree_len_after_y = cas_server.subtree_cache_len().await;
    // root_x, shared_child, unique_x, root_y, unique_y = 5 unique digests
    assert!(
        subtree_len_after_y >= 5,
        "subtree cache should have at least 5 entries after both trees, got {subtree_len_after_y}"
    );

    Ok(())
}

// ===========================================================================
// Test 4: coalescing_concurrent
// Spawns multiple concurrent GetTree calls for the same root. Verifies
// all return the same result and only 1 tree cache entry is created.
// ===========================================================================

#[nativelink_test]
async fn coalescing_concurrent() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = Arc::new(make_cas_server(&store_manager)?);
    let store = store_manager.get_store("main_cas").unwrap();

    let result = setup_directory_structure(store.as_pin()).await?;
    let root_digest_info = result.root_directory_digest_info;

    // Build expected directories list for comparison.
    let mut expected_dirs = vec![result.root_directory.clone()];
    expected_dirs.extend(result.sub_directories.iter().cloned());

    // Spawn 10 concurrent GetTree calls.
    let mut handles = Vec::with_capacity(10);
    for _ in 0..10 {
        let server = cas_server.clone();
        let handle = tokio::spawn(async move {
            let raw_response = server
                .get_tree(Request::new(GetTreeRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    page_size: 0,
                    page_token: String::new(),
                    root_digest: Some(root_digest_info.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                }))
                .await
                .expect("get_tree should succeed");
            raw_response
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .flat_map(|resp| futures::stream::iter(resp.directories))
                .collect::<Vec<_>>()
                .await
        });
        handles.push(handle);
    }

    // Collect all results.
    let mut results = Vec::with_capacity(10);
    for handle in handles {
        results.push(handle.await?);
    }

    // All 10 calls must return the same correct directories.
    for (i, dirs) in results.iter().enumerate() {
        assert_eq!(
            *dirs, expected_dirs,
            "concurrent call {i} returned wrong directories"
        );
    }

    // The tree cache should have exactly 1 entry, not 10.
    assert_eq!(
        cas_server.tree_cache_len().await,
        1,
        "coalescing should result in exactly 1 tree cache entry"
    );

    // No in-flight entries should remain after all calls complete.
    assert_eq!(
        cas_server.tree_inflight_len(),
        0,
        "no in-flight entries should remain after completion"
    );

    Ok(())
}

// ===========================================================================
// Test 5: coalescing_leader_failure
// When the leader BFS fails (missing root directory), waiters wake up
// and perform their own BFS. No deadlock should occur.
// ===========================================================================

#[nativelink_test]
async fn coalescing_leader_failure() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = Arc::new(make_cas_server(&store_manager)?);

    // Use a digest that does NOT exist in the store. The BFS will fail to
    // find the root directory. This tests that the leader properly signals
    // waiters even on failure, and no deadlock occurs.
    let missing_digest = DigestInfo::try_new(HASH1, 100)?;

    // Spawn 2 concurrent calls for the missing root.
    let mut handles = Vec::with_capacity(2);
    for _ in 0..2 {
        let server = cas_server.clone();
        handles.push(tokio::spawn(async move {
            let raw_response = server
                .get_tree(Request::new(GetTreeRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    page_size: 0,
                    page_token: String::new(),
                    root_digest: Some(missing_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                }))
                .await;
            // The call should succeed (GetTree returns a stream), but the
            // stream should yield a response with an empty directory list
            // (the root was missing, so BFS traversal produces nothing).
            match raw_response {
                Ok(resp) => {
                    let responses: Vec<_> = resp
                        .into_inner()
                        .filter_map(|x| async move { x.ok() })
                        .collect()
                        .await;
                    responses
                }
                Err(_status) => {
                    // An error status is also acceptable — the root doesn't exist.
                    vec![]
                }
            }
        }));
    }

    // All tasks should complete without deadlock. Use a timeout to detect
    // deadlock.
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for handle in handles {
            let _result = handle.await.expect("task should not panic");
        }
    })
    .await;
    assert!(
        timeout.is_ok(),
        "coalescing with leader failure should not deadlock"
    );

    // No in-flight entries should remain.
    assert_eq!(
        cas_server.tree_inflight_len(),
        0,
        "no in-flight entries should remain after failure"
    );

    // The tree cache should NOT have an entry because the BFS had missing
    // directories (total_missing_skipped > 0 prevents caching).
    assert_eq!(
        cas_server.tree_cache_len().await,
        0,
        "failed BFS should not populate tree cache"
    );

    Ok(())
}

// ===========================================================================
// Test 6: paginated_bypasses_cache
// Paginated GetTree calls (page_size > 0) should NOT cache results in
// the tree cache. A subsequent unpaginated call should do a fresh BFS.
// ===========================================================================

#[nativelink_test]
async fn paginated_bypasses_cache() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let result = setup_directory_structure(store.as_pin()).await?;

    // Make a paginated GetTree call (page_size = 2).
    let _paginated_dirs =
        collect_get_tree_dirs(&cas_server, result.root_directory_digest_info, 2).await;

    // The tree cache should NOT have been populated by a paginated call.
    assert_eq!(
        cas_server.tree_cache_len().await,
        0,
        "paginated GetTree should not populate tree cache"
    );

    // Now make an unpaginated call — it should do a fresh BFS and cache.
    let unpaginated_dirs =
        collect_get_tree_dirs(&cas_server, result.root_directory_digest_info, 0).await;
    assert_eq!(unpaginated_dirs.len(), 6, "unpaginated should return all 6 directories");

    assert_eq!(
        cas_server.tree_cache_len().await,
        1,
        "unpaginated GetTree should populate tree cache"
    );

    Ok(())
}

// ===========================================================================
// Test 7: subtree_cache_deduplication
// Verifies that when a tree has duplicate subtrees (same digest referenced
// by multiple parents), the BFS correctly deduplicates them and the
// subtree cache stores each unique directory exactly once.
// ===========================================================================

#[nativelink_test]
async fn subtree_cache_deduplication() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Create a shared leaf directory.
    let shared_leaf = Directory {
        node_properties: Some(NodeProperties {
            mtime: Some(Timestamp { seconds: 7, nanos: 0 }),
            unix_mode: Some(0o755),
            ..Default::default()
        }),
        ..Default::default()
    };
    let shared_leaf_digest = upload_directory(store.as_pin(), &shared_leaf).await?;

    // Create two mid-level directories that both reference the shared leaf.
    let mid_a = Directory {
        directories: vec![DirectoryNode {
            name: "leaf".into(),
            digest: Some(shared_leaf_digest.into()),
        }],
        ..Default::default()
    };
    let mid_a_digest = upload_directory(store.as_pin(), &mid_a).await?;

    let mid_b = Directory {
        directories: vec![DirectoryNode {
            name: "leaf".into(),
            digest: Some(shared_leaf_digest.into()),
        }],
        ..Default::default()
    };
    let mid_b_digest = upload_directory(store.as_pin(), &mid_b).await?;

    // Root references both mid-level directories.
    let root = Directory {
        directories: vec![
            DirectoryNode {
                name: "mid_a".into(),
                digest: Some(mid_a_digest.into()),
            },
            DirectoryNode {
                name: "mid_b".into(),
                digest: Some(mid_b_digest.into()),
            },
        ],
        ..Default::default()
    };
    let root_digest = upload_directory(store.as_pin(), &root).await?;

    let dirs = collect_get_tree_dirs(&cas_server, root_digest, 0).await;

    // BFS should return: root, mid_a, mid_b, shared_leaf.
    // Note: mid_a and mid_b have the SAME content but different names at
    // the parent level. However, since Directory proto content is
    // identical, they have the same digest and will be deduplicated.
    // Actually, mid_a and mid_b are structurally identical (same
    // directories field), so they'll have the same digest. Let's check.
    assert_eq!(
        mid_a_digest, mid_b_digest,
        "mid_a and mid_b have identical content, so same digest"
    );

    // With deduplication, we get: root, mid_a (=mid_b), shared_leaf = 3.
    assert_eq!(dirs.len(), 3, "deduplication should yield 3 unique directories");
    assert_eq!(dirs[0], root);

    // Subtree cache should have 3 unique entries.
    let subtree_len = cas_server.subtree_cache_len().await;
    assert_eq!(
        subtree_len, 3,
        "subtree cache should have 3 unique entries"
    );

    Ok(())
}

// ===========================================================================
// Test 8: tree_cache_returns_correct_next_page_token
// Verifies that cached GetTree results preserve the next_page_token
// (empty string for complete trees).
// ===========================================================================

#[nativelink_test]
async fn tree_cache_returns_correct_next_page_token() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let result = setup_directory_structure(store.as_pin()).await?;

    // First call: populates cache.
    let raw_response = cas_server
        .get_tree(Request::new(GetTreeRequest {
            instance_name: INSTANCE_NAME.to_string(),
            page_size: 0,
            page_token: String::new(),
            root_digest: Some(result.root_directory_digest_info.into()),
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await?;
    let first_responses: Vec<GetTreeResponse> = raw_response
        .into_inner()
        .filter_map(|x| async move { Some(x.unwrap()) })
        .collect()
        .await;
    assert_eq!(first_responses.len(), 1);
    assert_eq!(
        first_responses[0].next_page_token, "",
        "complete tree should have empty next_page_token"
    );

    // Second call: from cache. Should also have empty next_page_token.
    let raw_response = cas_server
        .get_tree(Request::new(GetTreeRequest {
            instance_name: INSTANCE_NAME.to_string(),
            page_size: 0,
            page_token: String::new(),
            root_digest: Some(result.root_directory_digest_info.into()),
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await?;
    let second_responses: Vec<GetTreeResponse> = raw_response
        .into_inner()
        .filter_map(|x| async move { Some(x.unwrap()) })
        .collect()
        .await;
    assert_eq!(second_responses.len(), 1);
    assert_eq!(
        second_responses[0].next_page_token, "",
        "cached result should preserve empty next_page_token"
    );

    // Verify the full response structure matches.
    assert_eq!(first_responses, second_responses);

    Ok(())
}

// ---------------------------------------------------------------------------
// REAPI content-defined chunking (SplitBlob/SpliceBlob, #2497) handler tests.
// ---------------------------------------------------------------------------

const CHUNK1_VALUE: &str = "hello ";
const CHUNK2_VALUE: &str = "world";

async fn make_chunking_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = make_store_manager().await?;
    store_manager.add_store(
        "chunk_index",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_chunking_cas_server(store_manager: &StoreManager) -> Result<CasServer, Error> {
    make_chunking_cas_server_with_avg(store_manager, 0)
}

fn make_chunking_cas_server_with_avg(
    store_manager: &StoreManager,
    avg_chunk_size_bytes: u64,
) -> Result<CasServer, Error> {
    // Fresh per-server ChunkingMetrics so counter assertions are isolated
    // from the process-wide singleton (and from other tests in this binary).
    CasServer::new_with_chunking_metrics(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "main_cas".to_string(),
                experimental_chunking: Some(nativelink_config::cas_server::CasChunkingConfig {
                    index_store: Some("chunk_index".to_string()),
                    avg_chunk_size_bytes,
                    max_chunk_count: 0,
                }),
            },
        }],
        store_manager,
        None,
        Arc::new(ChunkingMetrics::default()),
    )
}

/// Uploads the two test chunks to the store and returns their digests and
/// the digest of their concatenation.
async fn upload_test_chunks(store: &Store) -> Result<(Digest, Digest, Digest), Error> {
    let chunk1_digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: CHUNK1_VALUE.len() as i64,
    };
    let chunk2_digest = Digest {
        hash: HASH2.to_string(),
        size_bytes: CHUNK2_VALUE.len() as i64,
    };
    store
        .update_oneshot(
            DigestInfo::try_from(chunk1_digest.clone())?,
            CHUNK1_VALUE.into(),
        )
        .await?;
    store
        .update_oneshot(
            DigestInfo::try_from(chunk2_digest.clone())?,
            CHUNK2_VALUE.into(),
        )
        .await?;
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(CHUNK1_VALUE.as_bytes());
    hasher.update(CHUNK2_VALUE.as_bytes());
    let blob_digest: Digest = hasher.finalize_digest().into();
    Ok((chunk1_digest, chunk2_digest, blob_digest))
}

#[nativelink_test]
async fn splice_and_split_round_trip() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let (chunk1_digest, chunk2_digest, blob_digest) = upload_test_chunks(&store).await?;

    let splice_response = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            chunk_digests: vec![chunk1_digest.clone(), chunk2_digest.clone()],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    assert_eq!(splice_response.blob_digest.as_ref(), Some(&blob_digest));

    // The spliced blob must be materialized in the CAS so non-chunking
    // clients can read it.
    let blob_data = store
        .get_part_unchunked(DigestInfo::try_from(blob_digest.clone())?, 0, None)
        .await?;
    assert_eq!(blob_data, format!("{CHUNK1_VALUE}{CHUNK2_VALUE}"));

    let split_response = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    assert_eq!(
        split_response.chunk_digests,
        vec![chunk1_digest, chunk2_digest]
    );
    assert_eq!(
        split_response.chunking_function,
        i32::from(chunking_function::Value::FastCdc2020)
    );

    let metrics = cas_server.chunking_metrics();
    assert_eq!(metrics.splice_requests_total.load(Ordering::Relaxed), 1);
    assert_eq!(
        metrics.splice_bytes_total.load(Ordering::Relaxed),
        (CHUNK1_VALUE.len() + CHUNK2_VALUE.len()) as u64
    );
    assert_eq!(metrics.split_requests_total.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.split_hits.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.split_misses.load(Ordering::Relaxed), 0);
    Ok(())
}

#[nativelink_test]
async fn splice_blob_rejects_digest_mismatch() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let (chunk1_digest, chunk2_digest, _blob_digest) = upload_test_chunks(&store).await?;
    let total_size = chunk1_digest.size_bytes + chunk2_digest.size_bytes;
    let wrong_blob_digest = Digest {
        hash: HASH3.to_string(),
        size_bytes: total_size,
    };

    let status = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(wrong_blob_digest.clone()),
            chunk_digests: vec![chunk1_digest, chunk2_digest],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("does not match the expected digest"),
        "unexpected message: {}",
        status.message()
    );

    // The blob must not have been committed to the CAS.
    let blob_exists = store.has(DigestInfo::try_from(wrong_blob_digest)?).await?;
    assert_eq!(blob_exists, None);
    assert_eq!(
        cas_server
            .chunking_metrics()
            .splice_verification_failures
            .load(Ordering::Relaxed),
        1
    );
    Ok(())
}

#[nativelink_test]
async fn splice_blob_rejects_size_mismatch() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let (chunk1_digest, chunk2_digest, blob_digest) = upload_test_chunks(&store).await?;
    let wrong_blob_digest = Digest {
        size_bytes: blob_digest.size_bytes + 1,
        ..blob_digest
    };

    let status = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(wrong_blob_digest),
            chunk_digests: vec![chunk1_digest, chunk2_digest],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("does not match the expected blob size"),
        "unexpected message: {}",
        status.message()
    );
    Ok(())
}

#[nativelink_test]
async fn splice_blob_missing_chunk_returns_not_found() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Only upload the first chunk.
    let chunk1_digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: CHUNK1_VALUE.len() as i64,
    };
    store
        .update_oneshot(
            DigestInfo::try_from(chunk1_digest.clone())?,
            CHUNK1_VALUE.into(),
        )
        .await?;
    let missing_chunk_digest = Digest {
        hash: HASH2.to_string(),
        size_bytes: CHUNK2_VALUE.len() as i64,
    };
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(CHUNK1_VALUE.as_bytes());
    hasher.update(CHUNK2_VALUE.as_bytes());
    let blob_digest: Digest = hasher.finalize_digest().into();

    let status = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest),
            chunk_digests: vec![chunk1_digest, missing_chunk_digest],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    Ok(())
}

#[nativelink_test]
async fn split_blob_absent_blob_returns_not_found() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;

    // The blob was never uploaded.
    let status = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(Digest {
                hash: HASH1.to_string(),
                size_bytes: VALUE.len() as i64,
            }),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    let metrics = cas_server.chunking_metrics();
    assert_eq!(metrics.split_requests_total.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.split_hits.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.split_misses.load(Ordering::Relaxed), 1);
    Ok(())
}

#[nativelink_test]
async fn split_and_splice_disabled_return_unimplemented() -> Result<(), Box<dyn core::error::Error>>
{
    const VALUE: &str = "1";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE.len() as i64,
    };
    let split_status = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(digest.clone()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(split_status.code(), Code::Unimplemented);

    let splice_status = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(digest.clone()),
            chunk_digests: vec![digest],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(splice_status.code(), Code::Unimplemented);
    Ok(())
}

#[nativelink_test]
async fn split_blob_chunks_small_blob_on_demand() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Upload the blob whole (as a remote execution worker would) under its
    // real digest, without ever calling SpliceBlob.
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(VALUE.as_bytes());
    let blob_digest: Digest = hasher.finalize_digest().into();
    store
        .update_oneshot(DigestInfo::try_from(blob_digest.clone())?, VALUE.into())
        .await?;

    let split_response = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    // A blob smaller than the minimum chunk size is a single chunk whose
    // digest equals the blob digest.
    assert_eq!(split_response.chunk_digests, vec![blob_digest]);
    assert_eq!(
        split_response.chunking_function,
        i32::from(chunking_function::Value::FastCdc2020)
    );
    let metrics = cas_server.chunking_metrics();
    assert_eq!(metrics.split_chunked_on_demand.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.split_hits.load(Ordering::Relaxed), 0);
    Ok(())
}

#[nativelink_test]
async fn split_blob_chunks_large_blob_on_demand_and_reuses_layout()
-> Result<(), Box<dyn core::error::Error>> {
    // Use the smallest allowed average (1 KiB -> min 256, max 4096) so a
    // small test blob still produces multiple chunks.
    const AVG_CHUNK_SIZE: u64 = 1024;
    const BLOB_SIZE: usize = 16 * 1024;

    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server_with_avg(&store_manager, AVG_CHUNK_SIZE)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Deterministic pseudo-random content so FastCDC finds content-defined
    // boundaries.
    let mut state = 0x9e37_79b9_u32;
    let data: Vec<u8> = (0..BLOB_SIZE)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 24) as u8
        })
        .collect();
    let blob_digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: BLOB_SIZE as i64,
    };
    store
        .update_oneshot(
            DigestInfo::try_from(blob_digest.clone())?,
            bytes::Bytes::from(data.clone()),
        )
        .await?;

    let split_response = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    assert!(
        split_response.chunk_digests.len() > 1,
        "expected multiple chunks, got {}",
        split_response.chunk_digests.len()
    );

    // All chunks must be stored in the CAS and concatenate to the original
    // blob in order.
    let mut reassembled = Vec::with_capacity(BLOB_SIZE);
    for chunk_digest in &split_response.chunk_digests {
        let chunk_data = store
            .get_part_unchunked(DigestInfo::try_from(chunk_digest.clone())?, 0, None)
            .await?;
        reassembled.extend_from_slice(&chunk_data);
    }
    assert_eq!(reassembled, data);

    // A second split must be served from the stored layout.
    let second_response = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    assert_eq!(second_response.chunk_digests, split_response.chunk_digests);
    let metrics = cas_server.chunking_metrics();
    assert_eq!(metrics.split_requests_total.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.split_chunked_on_demand.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.split_hits.load(Ordering::Relaxed), 1);
    Ok(())
}

#[nativelink_test]
async fn split_blob_falls_back_when_layout_unusable() -> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();
    let index_store = store_manager.get_store("chunk_index").unwrap();

    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(VALUE.as_bytes());
    let blob_digest: Digest = hasher.finalize_digest().into();
    store
        .update_oneshot(DigestInfo::try_from(blob_digest.clone())?, VALUE.into())
        .await?;

    // Register a layout whose only chunk is not present in the CAS,
    // simulating a chunk that was evicted after the layout was stored.
    let stale_layout = SplitBlobResponse {
        chunk_digests: vec![Digest {
            hash: HASH2.to_string(),
            size_bytes: VALUE.len() as i64,
        }],
        chunking_function: chunking_function::Value::FastCdc2020.into(),
    };
    index_store
        .update_oneshot(
            DigestInfo::try_from(blob_digest.clone())?,
            stale_layout.encode_to_vec().into(),
        )
        .await?;

    // The unusable layout must be ignored and the blob re-chunked on demand.
    let split_response = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    assert_eq!(split_response.chunk_digests, vec![blob_digest]);
    let metrics = cas_server.chunking_metrics();
    assert_eq!(metrics.split_hits.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.split_chunked_on_demand.load(Ordering::Relaxed), 1);
    Ok(())
}

#[nativelink_test]
async fn chunking_rejects_index_store_same_as_cas_store() -> Result<(), Box<dyn core::error::Error>>
{
    let store_manager = make_store_manager().await?;
    let error = CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "main_cas".to_string(),
                experimental_chunking: Some(nativelink_config::cas_server::CasChunkingConfig {
                    index_store: Some("main_cas".to_string()),
                    avg_chunk_size_bytes: 0,
                    max_chunk_count: 0,
                }),
            },
        }],
        &store_manager,
        None,
    )
    .err()
    .expect("expected same-store index_store to be rejected");
    assert!(
        error
            .to_string()
            .contains("must not be the same store as 'cas_store'"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[nativelink_test]
async fn chunking_on_grpc_store_forbids_index_store() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "grpc_cas",
        store_factory(
            &StoreSpec::Grpc(nativelink_config::stores::GrpcSpec {
                instance_name: "backend".to_string(),
                endpoints: vec![nativelink_config::stores::GrpcEndpoint {
                    address: "http://localhost:1".to_string(),
                    tls_config: None,
                    concurrency_limit: None,
                    connect_timeout_s: 0,
                    tcp_keepalive_s: 0,
                    http2_keepalive_interval_s: 0,
                    http2_keepalive_timeout_s: 0,
                    tcp_nodelay: true,
                    use_http3: false,
                }],
                store_type: nativelink_config::stores::StoreType::Cas,
                retry: nativelink_config::stores::Retry::default(),
                max_concurrent_requests: 0,
                connections_per_endpoint: 0,
                rpc_timeout_s: 1,
                batch_update_threshold_bytes: 0,
                max_concurrent_batch_rpcs: 8,
                parallel_chunk_read_threshold: 0,
                parallel_chunk_count: 0,
                dual_transport: false,
                zstd_compression: false,
                connection_acquire_timeout_ms: None,
                chunked_writes_enabled: false,
                use_legacy_resource_names: false,
            }),
            &store_manager,
            None,
        )
        .await?,
    );

    let make_config = |index_store: Option<String>| {
        vec![WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "grpc_cas".to_string(),
                experimental_chunking: Some(nativelink_config::cas_server::CasChunkingConfig {
                    index_store,
                    avg_chunk_size_bytes: 0,
                    max_chunk_count: 0,
                }),
            },
        }]
    };

    // A local index store is meaningless when the RPCs are forwarded.
    let error = CasServer::new(
        &make_config(Some("grpc_cas".to_string())),
        &store_manager,
        None,
    )
    .err()
    .expect("expected index_store on grpc store to be rejected");
    assert!(
        error.to_string().contains("must not be set"),
        "unexpected error: {error}"
    );

    // Without an index_store the configuration is valid: SplitBlob and
    // SpliceBlob are forwarded to the backend.
    CasServer::new(&make_config(None), &store_manager, None)?;
    Ok(())
}

#[nativelink_test]
async fn max_chunk_count_limits_split_and_splice() -> Result<(), Box<dyn core::error::Error>> {
    // avg 1024 (min allowed) with max_chunk_count 2: the 16 KiB test blob
    // chunks to more than 2 pieces, so on-demand splitting must refuse.
    const AVG_CHUNK_SIZE: u64 = 1024;
    const BLOB_SIZE: usize = 16 * 1024;

    let store_manager = make_chunking_store_manager().await?;
    let cas_server = CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "main_cas".to_string(),
                experimental_chunking: Some(nativelink_config::cas_server::CasChunkingConfig {
                    index_store: Some("chunk_index".to_string()),
                    avg_chunk_size_bytes: AVG_CHUNK_SIZE,
                    max_chunk_count: 2,
                }),
            },
        }],
        &store_manager,
        None,
    )?;
    let store = store_manager.get_store("main_cas").unwrap();

    let mut state = 0x9e37_79b9_u32;
    let data: Vec<u8> = (0..BLOB_SIZE)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 24) as u8
        })
        .collect();
    let blob_digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: BLOB_SIZE as i64,
    };
    store
        .update_oneshot(
            DigestInfo::try_from(blob_digest.clone())?,
            bytes::Bytes::from(data),
        )
        .await?;

    let status = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    assert!(
        status.message().contains("max_chunk_count"),
        "unexpected message: {}",
        status.message()
    );

    // Splices above the cap are rejected outright.
    let chunk_digest = Digest {
        hash: HASH2.to_string(),
        size_bytes: 1,
    };
    let status = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest),
            chunk_digests: vec![chunk_digest.clone(), chunk_digest.clone(), chunk_digest],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("expected at most 2"),
        "unexpected message: {}",
        status.message()
    );
    Ok(())
}

// Bazel 9.1.1 with --digest_function=blake3 leaves digest_function unset in
// SplitBlob/SpliceBlob requests, which is length-ambiguous (SHA256 and
// BLAKE3 are both 32 bytes). The server must infer the function instead of
// assuming the default.
#[nativelink_test]
async fn chunking_infers_blake3_when_digest_function_unset()
-> Result<(), Box<dyn core::error::Error>> {
    const VALUE: &str = "blake3 blob content";

    let store_manager = make_chunking_store_manager().await?;
    let cas_server = make_chunking_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let mut hasher = DigestHasherFunc::Blake3.hasher();
    hasher.update(VALUE.as_bytes());
    let blob_digest: Digest = hasher.finalize_digest().into();

    // Splice: the single chunk is the blob itself, uploaded under its
    // BLAKE3 digest, with digest_function left unset.
    store
        .update_oneshot(DigestInfo::try_from(blob_digest.clone())?, VALUE.into())
        .await?;
    let splice_response = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            chunk_digests: vec![blob_digest.clone()],
            digest_function: 0,
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    assert_eq!(splice_response.blob_digest.as_ref(), Some(&blob_digest));

    // On-demand split of a fresh blob uploaded whole: the returned chunk
    // digests must be BLAKE3 (here a single chunk equal to the blob).
    let mut hasher = DigestHasherFunc::Blake3.hasher();
    hasher.update(b"other blake3 content");
    let other_digest: Digest = hasher.finalize_digest().into();
    store
        .update_oneshot(
            DigestInfo::try_from(other_digest.clone())?,
            bytes::Bytes::from_static(b"other blake3 content"),
        )
        .await?;
    let split_response = cas_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(other_digest.clone()),
            digest_function: 0,
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?
        .into_inner();
    assert_eq!(split_response.chunk_digests, vec![other_digest]);
    Ok(())
}

/// Reusable grpc CAS store spec pointing at an unreachable backend, so a
/// forwarded RPC fails with a connection error (never `Unimplemented`).
fn grpc_cas_spec() -> StoreSpec {
    StoreSpec::Grpc(nativelink_config::stores::GrpcSpec {
        instance_name: "backend".to_string(),
        endpoints: vec![nativelink_config::stores::GrpcEndpoint {
            address: "http://localhost:1".to_string(),
            tls_config: None,
            concurrency_limit: None,
            connect_timeout_s: 0,
            tcp_keepalive_s: 0,
            http2_keepalive_interval_s: 0,
            http2_keepalive_timeout_s: 0,
            tcp_nodelay: true,
            use_http3: false,
        }],
        store_type: nativelink_config::stores::StoreType::Cas,
        retry: nativelink_config::stores::Retry::default(),
        max_concurrent_requests: 0,
        connections_per_endpoint: 0,
        rpc_timeout_s: 1,
        batch_update_threshold_bytes: 0,
        max_concurrent_batch_rpcs: 8,
        parallel_chunk_read_threshold: 0,
        parallel_chunk_count: 0,
        dual_transport: false,
        zstd_compression: false,
        connection_acquire_timeout_ms: None,
        chunked_writes_enabled: false,
        use_legacy_resource_names: false,
    })
}

// #2497 D2: a grpc-backed CAS instance that did NOT opt into
// experimental_chunking must NOT forward SplitBlob/SpliceBlob to the backend.
// It returns Unimplemented — matching its advertised split/splice = false —
// rather than forwarding an RPC the operator never enabled.
//
// (The positive "forwards when opted in" path is not exercised here: it would
// attempt a real connection to the unreachable backend and block on retry.
// The mutation that proves this test bites — reverting the gate — makes the
// no-chunking server forward instead, yielding a NON-Unimplemented code.)
#[nativelink_test]
async fn grpc_forward_gated_on_chunking_config() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "grpc_cas",
        store_factory(&grpc_cas_spec(), &store_manager, None).await?,
    );

    let split_request = || {
        Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(Digest {
                hash: HASH1.to_string(),
                size_bytes: 4,
            }),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        })
    };

    // Chunking UNSET: no forward -> Unimplemented.
    let no_chunk_server = CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "grpc_cas".to_string(),
                experimental_chunking: None,
            },
        }],
        &store_manager,
        None,
    )?;
    let status = no_chunk_server
        .split_blob(split_request())
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::Unimplemented,
        "grpc instance WITHOUT chunking must not forward split_blob; got: {} / {}",
        status.code(),
        status.message()
    );
    let status = no_chunk_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(Digest {
                hash: HASH1.to_string(),
                size_bytes: 4,
            }),
            chunk_digests: vec![Digest {
                hash: HASH2.to_string(),
                size_bytes: 4,
            }],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(
        status.code(),
        Code::Unimplemented,
        "grpc instance WITHOUT chunking must not forward splice_blob; got: {} / {}",
        status.code(),
        status.message()
    );

    // Sanity: the with-chunking config (no index_store on a grpc store) is
    // accepted by CasServer::new — it is the config that DOES forward.
    CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "grpc_cas".to_string(),
                experimental_chunking: Some(nativelink_config::cas_server::CasChunkingConfig {
                    index_store: None,
                    avg_chunk_size_bytes: 0,
                    max_chunk_count: 0,
                }),
            },
        }],
        &store_manager,
        None,
    )?;
    Ok(())
}

// #2497 D1: experimental_chunking on a WorkerProxyStore-wrapped cas_store (the
// production cas_STORE topology) is rejected at CasServer::new — SpliceBlob's
// server-originated reassembly is unvalidated against the FL-688 ack-gate,
// which was designed for worker mirror uploads only.
#[nativelink_test]
async fn chunking_on_worker_proxy_store_rejected() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = Arc::new(StoreManager::new());
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let proxy = WorkerProxyStore::new(inner, new_shared_blob_locality_map());
    store_manager.add_store("wps_cas", Store::new(proxy));
    store_manager.add_store(
        "chunk_index",
        store_factory(&StoreSpec::Memory(MemorySpec::default()), &store_manager, None).await?,
    );

    let error = CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "wps_cas".to_string(),
                experimental_chunking: Some(nativelink_config::cas_server::CasChunkingConfig {
                    index_store: Some("chunk_index".to_string()),
                    avg_chunk_size_bytes: 0,
                    max_chunk_count: 0,
                }),
            },
        }],
        &store_manager,
        None,
    )
    .err()
    .expect("expected chunking on a WorkerProxyStore-wrapped cas_store to be rejected");
    assert!(
        error.to_string().contains("WorkerProxyStore-wrapped"),
        "unexpected error: {error}"
    );
    Ok(())
}

// #2497 D4: on-demand split of an over-cap blob must SHORT-CIRCUIT the chunk
// stream — it must NOT chunk the whole blob and write every chunk to the CAS
// before noticing the cap (which left orphan chunks + O(blob_size) waste).
// Verified by comparing CAS entry counts: a capped split writes strictly
// fewer chunks than an identical uncapped split of the same blob.
#[nativelink_test]
async fn split_over_cap_blob_short_circuits_without_writing_all_chunks()
-> Result<(), Box<dyn core::error::Error>> {
    const AVG_CHUNK_SIZE: u64 = 1024;
    const BLOB_SIZE: usize = 16 * 1024;
    const CAP: u64 = 2;

    // Deterministic pseudo-random blob so FastCDC produces many (> CAP) chunks.
    let mut state = 0x9e37_79b9_u32;
    let data: Vec<u8> = (0..BLOB_SIZE)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 24) as u8
        })
        .collect();
    let blob_digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: BLOB_SIZE as i64,
    };

    let make_server = |store_manager: &StoreManager, max_chunk_count: u64| {
        CasServer::new_with_chunking_metrics(
            &[WithInstanceName {
                instance_name: INSTANCE_NAME.to_string(),
                config: nativelink_config::cas_server::CasStoreConfig {
                    cas_store: "main_cas".to_string(),
                    experimental_chunking: Some(
                        nativelink_config::cas_server::CasChunkingConfig {
                            index_store: Some("chunk_index".to_string()),
                            avg_chunk_size_bytes: AVG_CHUNK_SIZE,
                            max_chunk_count,
                        },
                    ),
                },
            }],
            store_manager,
            None,
            Arc::new(ChunkingMetrics::default()),
        )
    };

    let cas_len = |store: &Store| {
        let store = store.clone();
        async move {
            store
                .downcast_ref::<MemoryStore>(None)
                .expect("main_cas is a MemoryStore")
                .len_for_test()
                .await
        }
    };

    // Reference: an uncapped split writes the FULL chunk set.
    let sm_full = make_chunking_store_manager().await?;
    let full_server = make_server(&sm_full, 100_000)?;
    let full_store = sm_full.get_store("main_cas").unwrap();
    full_store
        .update_oneshot(
            DigestInfo::try_from(blob_digest.clone())?,
            bytes::Bytes::from(data.clone()),
        )
        .await?;
    full_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?;
    let full_chunk_count = cas_len(&full_store).await - 1; // minus the blob itself.
    assert!(
        full_chunk_count > CAP as usize,
        "test blob only produced {full_chunk_count} chunks, not > cap {CAP}; \
         adjust BLOB_SIZE/AVG so it exceeds the cap"
    );

    // Capped: the split refuses AND must not have written the full set.
    let sm_capped = make_chunking_store_manager().await?;
    let capped_server = make_server(&sm_capped, CAP)?;
    let capped_store = sm_capped.get_store("main_cas").unwrap();
    capped_store
        .update_oneshot(
            DigestInfo::try_from(blob_digest.clone())?,
            bytes::Bytes::from(data),
        )
        .await?;
    let status = capped_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    assert!(
        status.message().contains("max_chunk_count"),
        "unexpected message: {}",
        status.message()
    );

    let capped_chunk_count = cas_len(&capped_store).await - 1;
    // The short-circuit stores at most `CAP` chunks (indices 0..CAP-1) before
    // aborting; every later chunk hits the guard before its store call. It
    // MUST be strictly fewer than the full set — reverting the fix (checking
    // the cap only after try_collect) writes all `full_chunk_count`.
    assert!(
        capped_chunk_count < full_chunk_count,
        "over-cap split wrote {capped_chunk_count} chunks (full set is \
         {full_chunk_count}); expected the stream to short-circuit and write \
         strictly fewer — the cap check ran AFTER writing all chunks"
    );
    assert!(
        capped_chunk_count <= CAP as usize,
        "over-cap split wrote {capped_chunk_count} chunks; expected at most \
         the cap ({CAP}) before the stream aborted"
    );
    Ok(())
}

// #2497 D4: over-cap error-code DETERMINISM for large streaming blobs. When
// the on-demand split of an over-cap blob is served from a store that streams
// the read (rather than a single-send oneshot), the blob is larger than the
// buf channel, so the in-stream cap guard drops `rx` while the read is still
// in flight — that read fails with `Code::Internal`, and `Error::merge`
// prefers the read's code. Without the post-join `over_cap` override in
// `chunk_blob_on_demand`, the RPC returns `Internal` instead of the
// REAPI-correct `NotFound`, so an operator alerting on the `NotFound` rate for
// absent-blob splits would go dark and clients would see opaque errors. This
// test feeds the MemoryStore the blob as thousands of small chunks (far more
// than the ~1024-slot buf channel), so `get_part` genuinely blocks mid-stream,
// then asserts the returned Code is `NotFound`. The 16 KiB single-send test
// above cannot catch this: a oneshot blob is delivered in ONE send that fits
// one slot, so the read completes `Ok` and `NotFound` survives the merge
// trivially. Mutation: delete the `over_cap` override -> this red-fails with
// `Internal`.
#[nativelink_test]
async fn split_over_cap_large_streaming_blob_returns_not_found()
-> Result<(), Box<dyn core::error::Error>> {
    const AVG_CHUNK_SIZE: u64 = 1024;
    const CAP: u64 = 4;
    const SEND_SIZE: usize = 64;
    // 2048 sends of 64 bytes = 128 KiB, far more than the ~1024-slot buf
    // channel, so the store read is still streaming when the guard fires at
    // chunk index CAP (~14 KiB consumed with CHUNK_CONCURRENCY=10 lookahead).
    const NUM_SENDS: usize = 2048;
    const BLOB_SIZE: usize = NUM_SENDS * SEND_SIZE;

    // Deterministic pseudo-random blob so FastCDC produces many (> CAP) chunks.
    let mut state = 0x9e37_79b9_u32;
    let data: Vec<u8> = (0..BLOB_SIZE)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 24) as u8
        })
        .collect();
    let blob_digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: BLOB_SIZE as i64,
    };

    let make_server = |store_manager: &StoreManager, max_chunk_count: u64| {
        CasServer::new_with_chunking_metrics(
            &[WithInstanceName {
                instance_name: INSTANCE_NAME.to_string(),
                config: nativelink_config::cas_server::CasStoreConfig {
                    cas_store: "main_cas".to_string(),
                    experimental_chunking: Some(
                        nativelink_config::cas_server::CasChunkingConfig {
                            index_store: Some("chunk_index".to_string()),
                            avg_chunk_size_bytes: AVG_CHUNK_SIZE,
                            max_chunk_count,
                        },
                    ),
                },
            }],
            store_manager,
            None,
            Arc::new(ChunkingMetrics::default()),
        )
    };

    let cas_len = |store: &Store| {
        let store = store.clone();
        async move {
            store
                .downcast_ref::<MemoryStore>(None)
                .expect("main_cas is a MemoryStore")
                .len_for_test()
                .await
        }
    };

    // Reference: an uncapped split of the same content writes the FULL set.
    let sm_full = make_chunking_store_manager().await?;
    let full_server = make_server(&sm_full, 100_000)?;
    let full_store = sm_full.get_store("main_cas").unwrap();
    full_store
        .update_oneshot(
            DigestInfo::try_from(blob_digest.clone())?,
            bytes::Bytes::from(data.clone()),
        )
        .await?;
    full_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest.clone()),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await?;
    let full_chunk_count = cas_len(&full_store).await - 1; // minus the blob.
    assert!(
        full_chunk_count > CAP as usize,
        "test blob only produced {full_chunk_count} chunks, not > cap {CAP}; \
         adjust BLOB_SIZE/AVG so it exceeds the cap"
    );

    // Capped: feed the blob to the MemoryStore as a STREAM of NUM_SENDS small
    // chunks. MemoryStore preserves each send as its own scatter-gather chunk,
    // so the subsequent split read (`get_part`) replays them one send at a
    // time and blocks once the buf channel fills — the read is therefore still
    // in flight (and will abort with `Code::Internal`) when the cap guard fires.
    let sm_capped = make_chunking_store_manager().await?;
    let capped_server = make_server(&sm_capped, CAP)?;
    let capped_store = sm_capped.get_store("main_cas").unwrap();
    let (mut tx, rx) = make_buf_channel_pair();
    let feed = async move {
        for piece in data.chunks(SEND_SIZE) {
            tx.send(bytes::Bytes::copy_from_slice(piece)).await?;
        }
        tx.send_eof()?;
        Ok::<(), Error>(())
    };
    let store_update = capped_store.update(
        DigestInfo::try_from(blob_digest.clone())?,
        rx,
        UploadSizeInfo::ExactSize(BLOB_SIZE as u64),
    );
    let (feed_res, update_res) = futures::join!(feed, store_update);
    feed_res?;
    update_res?;

    let status = capped_server
        .split_blob(Request::new(SplitBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(blob_digest),
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    // Load-bearing: over-cap MUST be NotFound even though the in-flight read
    // was aborted mid-stream with Code::Internal.
    assert_eq!(
        status.code(),
        Code::NotFound,
        "over-cap split of a large streaming blob returned {:?}, expected \
         NotFound — the aborted mid-stream read's Internal code must not win \
         the Error::merge. message: {}",
        status.code(),
        status.message()
    );
    assert!(
        status.message().contains("max_chunk_count"),
        "unexpected message: {}",
        status.message()
    );

    // And it must still short-circuit: strictly fewer than the full chunk set
    // written, at most the cap.
    let capped_chunk_count = cas_len(&capped_store).await - 1;
    assert!(
        capped_chunk_count < full_chunk_count,
        "over-cap split wrote {capped_chunk_count} chunks (full set is \
         {full_chunk_count}); expected strictly fewer — the stream must \
         short-circuit at the cap"
    );
    assert!(
        capped_chunk_count <= CAP as usize,
        "over-cap split wrote {capped_chunk_count} chunks; expected at most \
         the cap ({CAP}) before the stream aborted"
    );
    Ok(())
}

// #2497 D3: singleton-aliasing E2E. The dark-counter trap is that the
// production PRODUCER (every CAS instance's `chunking_metrics`) must be the
// SAME Arc that `register_chunking_metrics` publishes on `/metrics`. The
// `_pins_values` test below hand-sets a LOCAL Arc, and the handler tests build
// via `new_with_chunking_metrics` with a fresh Arc — so a regression pointing
// `CasServer::new` at a per-server Arc (re-introducing the exact dark-counter
// trap D3 fixes) would ship GREEN. This test closes that gap: it builds the
// server via the PRODUCTION `CasServer::new` (which wires the process-wide
// singleton), drives a REAL `splice_blob` that bumps
// `splice_verification_failures` through the handler, then asserts the value
// renders NON-ZERO on the actual `register_chunking_metrics` + render_prometheus
// path. Mutation: point `CasServer::new` at `Arc::new(ChunkingMetrics::default())`
// instead of `chunking_metrics_singleton()` -> the handler bumps a throwaway
// Arc, the registered singleton stays 0, and this test red-fails. (No other
// test in this binary bumps the singleton's `splice_verification_failures`, so
// the mutated render is deterministically 0.)
#[nativelink_test]
async fn chunking_metrics_singleton_renders_production_producer_counter()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_chunking_store_manager().await?;
    // PRODUCTION path: `new` (not `new_with_chunking_metrics`) wires the
    // instance's counters to the process-wide singleton.
    let cas_server = CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "main_cas".to_string(),
                experimental_chunking: Some(nativelink_config::cas_server::CasChunkingConfig {
                    index_store: Some("chunk_index".to_string()),
                    avg_chunk_size_bytes: 0,
                    max_chunk_count: 0,
                }),
            },
        }],
        &store_manager,
        None,
    )?;

    // Drive a real splice that fails verification (declared blob size does not
    // match the summed chunk sizes) -> the handler bumps
    // `splice_verification_failures` on whatever Arc the producer holds.
    let status = cas_server
        .splice_blob(Request::new(SpliceBlobRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digest: Some(Digest {
                hash: HASH1.to_string(),
                size_bytes: 100,
            }),
            chunk_digests: vec![Digest {
                hash: HASH2.to_string(),
                size_bytes: 1,
            }],
            digest_function: digest_function::Value::Sha256.into(),
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    // Register + render the PROCESS SINGLETON (the production `/metrics` path).
    // The producer above must have incremented THIS Arc, so the rendered value
    // is >= 1. Under the mutation (producer -> fresh Arc) it renders 0.
    let registry = MetricsRegistry::new();
    register_chunking_metrics(&registry);
    let body = render_prometheus(&registry);
    let rendered = body
        .lines()
        .find_map(|line| line.strip_prefix("cas_splice_verification_failures "))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or_else(|| {
            panic!(
                "#2497 D3: `cas_splice_verification_failures` absent from the \
                 render_prometheus walk — the singleton is not registered. body=\n{body}"
            )
        });
    assert!(
        rendered >= 1,
        "#2497 D3 dark-counter: the production `CasServer::new` producer bumped \
         a counter that did NOT reach the registered singleton — \
         `cas_splice_verification_failures` rendered {rendered}, expected >= 1. \
         `CasServer::new` must wire the process-wide chunking_metrics_singleton, \
         not a per-server Arc. body=\n{body}"
    );
    Ok(())
}

// #2497 D3: the ChunkingMetrics counters (including
// `cas_splice_verification_failures`, the CAS-poisoning-rejection signal)
// must render on the REAL /metrics path — MetricsRegistry::register (via the
// production `register_chunking_metrics`) + render_prometheus. Before this
// fix the per-instance ChunkingMetrics tree was never registered — DARK on
// /metrics (the worker-metrics-exposure trap). Mutation: gut
// `register_chunking_metrics` (or comment a publish! block) -> the pinned
// line vanishes -> this test red-fails.
#[test]
fn chunking_metrics_render_prometheus_exposes_names() {
    let registry = MetricsRegistry::new();
    register_chunking_metrics(&registry);
    let body = render_prometheus(&registry);
    for name in [
        "cas_splice_requests_total",
        "cas_splice_already_exists",
        "cas_splice_verification_failures",
        "cas_splice_bytes_total",
        "cas_split_requests_total",
        "cas_split_hits",
        "cas_split_misses",
        "cas_split_chunked_on_demand",
        "cas_split_bytes_total",
    ] {
        assert!(
            body.contains(&format!("\n{name} ")),
            "#2497 D3 dark on /metrics: chunking metric `{name}` ABSENT from the \
             render_prometheus walk — the CAS-poisoning-rejection signal (and its \
             siblings) would be invisible to operators. body=\n{body}"
        );
    }
    // Doubled-prefix trap guard (the register key + an inner group!()
    // concatenating).
    assert!(
        !body.contains("cas_cas_"),
        "#2497 D3 doubled metric-name prefix in rendered chunking metrics. body=\n{body}"
    );
}

// #2497 D3: pin exact rendered VALUES on the /metrics path so a mis-wired
// field (right name, wrong source atomic) is caught, not just an absent line.
// Local Arc (not the process singleton) to avoid cross-test value pollution.
#[test]
fn chunking_metrics_render_prometheus_pins_values() {
    let counters = Arc::new(ChunkingMetrics::default());
    counters
        .splice_verification_failures
        .fetch_add(5, Ordering::Relaxed);
    counters.split_hits.fetch_add(9, Ordering::Relaxed);
    let registry = MetricsRegistry::new();
    registry.register("cas", counters);
    let body = render_prometheus(&registry);
    assert!(
        body.contains("\ncas_splice_verification_failures 5\n"),
        "#2497 D3: expected `cas_splice_verification_failures 5` on the /metrics \
         render, absent or wrong value. body=\n{body}"
    );
    assert!(
        body.contains("\ncas_split_hits 9\n"),
        "#2497 D3: expected `cas_split_hits 9` on the /metrics render, absent or \
         wrong value. body=\n{body}"
    );
}
