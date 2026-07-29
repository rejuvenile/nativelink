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

use core::time::Duration;
use std::sync::Arc;

use rand::Rng;
#[cfg(feature = "dev-schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::serde_utils::{
    convert_boolean_with_shellexpand, convert_data_size_with_shellexpand,
    convert_duration_with_shellexpand, convert_numeric_with_shellexpand,
    convert_optional_data_size_with_shellexpand, convert_optional_numeric_with_shellexpand,
    convert_optional_string_with_shellexpand, convert_string_with_shellexpand,
    convert_vec_string_with_shellexpand,
};

/// Name of the store. This type will be used when referencing a store
/// in the `CasConfig::stores`'s map key.
pub type StoreRefName = String;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum ConfigDigestHashFunction {
    /// Use the sha256 hash function.
    /// <https://en.wikipedia.org/wiki/SHA-2>
    Sha256,

    /// Use the blake3 hash function.
    /// <https://en.wikipedia.org/wiki/BLAKE_(hash_function)>
    Blake3,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum StoreSpec {
    /// Cache metrics store wraps another store and emits low-cardinality
    /// OpenTelemetry cache operation metrics for the wrapped store.
    ///
    /// This wrapper is opt-in. Stores that are not explicitly wrapped by
    /// `cache_metrics` are constructed exactly as they are without this
    /// wrapper and do not pay its hot-path timing or recording cost.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "cache_metrics": {
    ///   "cache_type": "cas",
    ///   "backend": {
    ///     "filesystem": {
    ///       "content_path": "~/.cache/nativelink/content_path-cas",
    ///       "temp_path": "~/.cache/nativelink/tmp_path-cas"
    ///     }
    ///   }
    /// }
    /// ```
    ///
    CacheMetrics(Box<CacheMetricsSpec>),

    /// Memory store will store all data in a hashmap in memory.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "memory": {
    ///   "eviction_policy": {
    ///     "max_bytes": "10mb",
    ///   }
    /// }
    /// ```
    ///
    Memory(MemorySpec),

    /// A generic blob store that will store files on the cloud
    /// provider. This configuration will never delete files, so you are
    /// responsible for purging old files in other ways.
    /// It supports the following backends:
    ///
    /// 1. **Amazon S3:**
    ///    S3 store will use Amazon's S3 service as a backend to store
    ///    the files. This configuration can be used to share files
    ///    across multiple instances. Uses system certificates for TLS
    ///    verification via `rustls-platform-verifier`.
    ///
    ///   **Example JSON Config:**
    ///   ```json
    ///   "experimental_cloud_object_store": {
    ///     "provider": "aws",
    ///     "region": "eu-north-1",
    ///     "bucket": "crossplane-bucket-af79aeca9",
    ///     "key_prefix": "test-prefix-index/",
    ///     "retry": {
    ///       "max_retries": 6,
    ///       "delay": 0.3,
    ///       "jitter": 0.5
    ///     },
    ///     "multipart_max_concurrent_uploads": 10
    ///   }
    ///   ```
    ///
    /// 2. **Google Cloud Storage:**
    ///    GCS store uses Google's GCS service as a backend to store
    ///    the files. This configuration can be used to share files
    ///    across multiple instances.
    ///
    ///   **Example JSON Config:**
    ///   ```json
    ///   "experimental_cloud_object_store": {
    ///     "provider": "gcs",
    ///     "bucket": "test-bucket",
    ///     "key_prefix": "test-prefix-index/",
    ///     "retry": {
    ///       "max_retries": 6,
    ///       "delay": 0.3,
    ///       "jitter": 0.5
    ///     },
    ///     "multipart_max_concurrent_uploads": 10
    ///   }
    ///   ```
    ///
    /// 3. **Azure Blob Store:**
    ///    Azure Blob store will use Microsoft's Azure Blob service as a
    ///    backend to store the files. This configuration can be used to
    ///    share files across multiple instances.
    ///
    ///   **Example JSON Config:**
    ///   ```json
    ///   "experimental_cloud_object_store": {
    ///     "provider": "azure",
    ///     "account_name": "cloudshell1393657559",
    ///     "container": "simple-test-container",
    ///     "key_prefix": "folder/",
    ///     "retry": {
    ///         "max_retries": 6,
    ///         "delay": 0.3,
    ///         "jitter": 0.5
    ///     },
    ///     "multipart_max_concurrent_uploads": 10
    ///   }
    ///   ```
    ///
    /// 4. **`NetApp` ONTAP S3**
    ///    `NetApp` ONTAP S3 store will use ONTAP's S3-compatible storage as a backend
    ///    to store files. This store is specifically configured for ONTAP's S3 requirements
    ///    including custom TLS configuration, credentials management, and proper vserver
    ///    configuration.
    ///
    ///    This store uses AWS environment variables for credentials:
    ///    - `AWS_ACCESS_KEY_ID`
    ///    - `AWS_SECRET_ACCESS_KEY`
    ///    - `AWS_DEFAULT_REGION`
    ///
    ///    **Example JSON Config:**
    ///    ```json
    ///    "experimental_cloud_object_store": {
    ///      "provider": "ontap",
    ///      "endpoint": "https://ontap-s3-endpoint:443",
    ///      "vserver_name": "your-vserver",
    ///      "bucket": "your-bucket",
    ///      "root_certificates": "/path/to/certs.pem",  // Optional
    ///      "key_prefix": "test-prefix/",               // Optional
    ///      "retry": {
    ///        "max_retries": 6,
    ///        "delay": 0.3,
    ///        "jitter": 0.5
    ///      },
    ///      "multipart_max_concurrent_uploads": 10
    ///    }
    ///    ```
    ///
    /// 5. **Cloudflare R2:**
    ///    R2 store uses Cloudflare's R2 service as a backend. R2 speaks the
    ///    S3 API, so this is a thin wrapper that derives the account-scoped
    ///    endpoint (`https://{account_id}.r2.cloudflarestorage.com`) for you.
    ///
    ///    **Example JSON Config:**
    ///    ```json
    ///    "experimental_cloud_object_store": {
    ///      "provider": "r2",
    ///      "account_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
    ///      "bucket": "nativelink-cas",
    ///      "key_prefix": "test-prefix/",
    ///      "retry": {
    ///        "max_retries": 6,
    ///        "delay": 0.3,
    ///        "jitter": 0.5
    ///      },
    ///      "multipart_max_concurrent_uploads": 10
    ///    }
    ///    ```
    ///
    /// 6. **Oracle Cloud Infrastructure (OCI) Object Storage:**
    ///    OCI store uses Oracle Cloud Infrastructure's S3-compatible Object
    ///    Storage API. The path-style endpoint is derived from your Object
    ///    Storage `namespace` and `region` as
    ///    `https://{namespace}.compat.objectstorage.{region}.oci.customer-oci.com`.
    ///    Authenticate with a Customer Secret Key (Access Key/Secret Key pair
    ///    created under User Settings -> Customer secret keys in the OCI
    ///    console); the secret cannot be retrieved after generation, so read
    ///    it from an env var via shellexpand.
    ///
    ///    **Example JSON Config:**
    ///    ```json
    ///    "experimental_cloud_object_store": {
    ///      "provider": "oci",
    ///      "namespace": "your-object-storage-namespace",
    ///      "region": "us-phoenix-1",
    ///      "bucket": "nativelink-cas",
    ///      "access_key_id": "oci_access_key_id",
    ///      "secret_access_key": "oci_secret_access_key",
    ///      "key_prefix": "test-prefix/",
    ///      "retry": {
    ///        "max_retries": 6,
    ///        "delay": 0.3,
    ///        "jitter": 0.5
    ///      }
    ///    }
    ///    ```
    ExperimentalCloudObjectStore(ExperimentalCloudObjectSpec),

    /// ONTAP S3 Existence Cache provides a caching layer on top of the ONTAP S3 store
    /// to optimize repeated existence checks. It maintains an in-memory cache of object
    /// digests and periodically syncs this cache to disk for persistence.
    ///
    /// The cache helps reduce latency for repeated calls to check object existence,
    /// while still ensuring eventual consistency with the underlying ONTAP S3 store.
    ///
    /// Example JSON Config:
    /// ```json
    /// "ontap_s3_existence_cache": {
    ///   "index_path": "/path/to/cache/index.json",
    ///   "sync_interval_seconds": 300,
    ///   "backend": {
    ///     "endpoint": "https://ontap-s3-endpoint:443",
    ///     "vserver_name": "your-vserver",
    ///     "bucket": "your-bucket",
    ///     "key_prefix": "test-prefix/"
    ///   }
    /// }
    /// ```
    ///
    OntapS3ExistenceCache(Box<OntapS3ExistenceCacheSpec>),

    /// Verify store is used to apply verifications to an underlying
    /// store implementation. It is strongly encouraged to validate
    /// as much data as you can before accepting data from a client,
    /// failing to do so may cause the data in the store to be
    /// populated with invalid data causing all kinds of problems.
    ///
    /// The suggested configuration is to have the CAS validate the
    /// hash and size and the AC validate nothing.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "verify": {
    ///   "backend": {
    ///     "memory": {
    ///       "eviction_policy": {
    ///         "max_bytes": "500mb"
    ///       }
    ///     },
    ///   },
    ///   "verify_size": true,
    ///   "verify_hash": true
    /// }
    /// ```
    ///
    Verify(Box<VerifySpec>),

    /// Completeness checking store verifies if the
    /// output files & folders exist in the CAS before forwarding
    /// the request to the underlying store.
    /// Note: This store should only be used on AC stores.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "completeness_checking": {
    ///   "backend": {
    ///     "filesystem": {
    ///       "content_path": "~/.cache/nativelink/content_path-ac",
    ///       "temp_path": "~/.cache/nativelink/tmp_path-ac",
    ///       "eviction_policy": {
    ///         "max_bytes": "500mb",
    ///       }
    ///     }
    ///   },
    ///   "cas_store": {
    ///     "ref_store": {
    ///       "name": "CAS_MAIN_STORE"
    ///     }
    ///   }
    /// }
    /// ```
    ///
    CompletenessChecking(Box<CompletenessCheckingSpec>),

    /// A compression store that will compress the data inbound and
    /// outbound. There will be a non-trivial cost to compress and
    /// decompress the data, but in many cases if the final store is
    /// a store that requires network transport and/or storage space
    /// is a concern it is often faster and more efficient to use this
    /// store before those stores.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "compression": {
    ///   "compression_algorithm": {
    ///     "lz4": {}
    ///   },
    ///   "backend": {
    ///     "filesystem": {
    ///       "content_path": "/tmp/nativelink/data/content_path-cas",
    ///       "temp_path": "/tmp/nativelink/data/tmp_path-cas",
    ///       "eviction_policy": {
    ///         "max_bytes": "2gb",
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    ///
    Compression(Box<CompressionSpec>),

    /// A dedup store will take the inputs and run a rolling hash
    /// algorithm on them to slice the input into smaller parts then
    /// run a sha256 algorithm on the slice and if the object doesn't
    /// already exist, upload the slice to the `content_store` using
    /// a new digest of just the slice. Once all parts exist, an
    /// Action-Cache-like digest will be built and uploaded to the
    /// `index_store` which will contain a reference to each
    /// chunk/digest of the uploaded file. Downloading a request will
    /// first grab the index from the `index_store`, and forward the
    /// download content of each chunk as if it were one file.
    ///
    /// This store is exceptionally good when the following conditions
    /// are met:
    /// * Content is mostly the same (inserts, updates, deletes are ok)
    /// * Content is not compressed or encrypted
    /// * Uploading or downloading from `content_store` is the bottleneck.
    ///
    /// Note: This store pairs well when used with `CompressionSpec` as
    /// the `content_store`, but never put `DedupSpec` as the backend of
    /// `CompressionSpec` as it will negate all the gains.
    ///
    /// Note: When running `.has()` on this store, it will only check
    /// to see if the entry exists in the `index_store` and not check
    /// if the individual chunks exist in the `content_store`.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "dedup": {
    ///   "index_store": {
    ///     "memory": {
    ///       "eviction_policy": {
    ///          "max_bytes": "1GB",
    ///       }
    ///     }
    ///   },
    ///   "content_store": {
    ///     "compression": {
    ///       "compression_algorithm": {
    ///         "lz4": {}
    ///       },
    ///       "backend": {
    ///         "fast_slow": {
    ///           "fast": {
    ///             "memory": {
    ///               "eviction_policy": {
    ///                 "max_bytes": "500MB",
    ///               }
    ///             }
    ///           },
    ///           "slow": {
    ///             "filesystem": {
    ///               "content_path": "/tmp/nativelink/data/content_path-content",
    ///               "temp_path": "/tmp/nativelink/data/tmp_path-content",
    ///               "eviction_policy": {
    ///                 "max_bytes": "2gb"
    ///               }
    ///             }
    ///           }
    ///         }
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    ///
    Dedup(Box<DedupSpec>),

    /// Existence store will wrap around another store and cache calls
    /// to has so that subsequent `has_with_results` calls will be
    /// faster. This is useful for cases when you have a store that
    /// is slow to respond to has calls.
    /// Note: This store should only be used on CAS stores.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "existence_cache": {
    ///   "backend": {
    ///     "memory": {
    ///       "eviction_policy": {
    ///         "max_bytes": "500mb",
    ///       }
    ///     }
    ///   },
    ///   // Note this is the existence store policy, not the backend policy
    ///   "eviction_policy": {
    ///     "max_seconds": 100,
    ///   }
    /// }
    /// ```
    ///
    ExistenceCache(Box<ExistenceCacheSpec>),

    /// `FastSlow` store will first try to fetch the data from the `fast`
    /// store and then if it does not exist try the `slow` store.
    /// When the object does exist in the `slow` store, it will copy
    /// the data to the `fast` store while returning the data.
    /// This store should be thought of as a store that "buffers"
    /// the data to the `fast` store.
    /// On uploads it will mirror data to both `fast` and `slow` stores.
    ///
    /// WARNING: If you need data to always exist in the `slow` store
    /// for something like remote execution, be careful because this
    /// store will never check to see if the objects exist in the
    /// `slow` store if it exists in the `fast` store (ie: it assumes
    /// that if an object exists in the `fast` store it will exist in
    /// the `slow` store).
    ///
    /// ***Example JSON Config:***
    /// ```json
    /// "fast_slow": {
    ///   "fast": {
    ///     "filesystem": {
    ///       "content_path": "/tmp/nativelink/data/content_path-index",
    ///       "temp_path": "/tmp/nativelink/data/tmp_path-index",
    ///       "eviction_policy": {
    ///         "max_bytes": "500mb",
    ///       }
    ///     }
    ///   },
    ///   "slow": {
    ///     "filesystem": {
    ///       "content_path": "/tmp/nativelink/data/content_path-index",
    ///       "temp_path": "/tmp/nativelink/data/tmp_path-index",
    ///       "eviction_policy": {
    ///         "max_bytes": "500mb",
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    ///
    FastSlow(Box<FastSlowSpec>),

    /// Shards the data to multiple stores. This is useful for cases
    /// when you want to distribute the load across multiple stores.
    /// The digest hash is used to determine which store to send the
    /// data to.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "shard": {
    ///   "stores": [
    ///    {
    ///     "store": {
    ///       "memory": {
    ///         "eviction_policy": {
    ///             "max_bytes": "10mb"
    ///         },
    ///       },
    ///     },
    ///     "weight": 1
    ///   }]
    /// }
    /// ```
    ///
    Shard(ShardSpec),

    /// Stores the data on the filesystem. This store is designed for
    /// local persistent storage. Restarts of this program should restore
    /// the previous state, meaning anything uploaded will be persistent
    /// as long as the filesystem integrity holds.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "filesystem": {
    ///   "content_path": "/tmp/nativelink/data-worker-test/content_path-cas",
    ///   "temp_path": "/tmp/nativelink/data-worker-test/tmp_path-cas",
    ///   "eviction_policy": {
    ///     "max_bytes": "10gb",
    ///   }
    /// }
    /// ```
    ///
    Filesystem(FilesystemSpec),

    /// Store used to reference a store in the root store manager.
    /// This is useful for cases when you want to share a store in different
    /// nested stores. Example, you may want to share the same memory store
    /// used for the action cache, but use a `FastSlowSpec` and have the fast
    /// store also share the memory store for efficiency.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "ref_store": {
    ///   "name": "FS_CONTENT_STORE"
    /// }
    /// ```
    ///
    RefStore(RefSpec),

    /// Uses the size field of the digest to separate which store to send the
    /// data. This is useful for cases when you'd like to put small objects
    /// in one store and large objects in another store. This should only be
    /// used if the size field is the real size of the content, in other
    /// words, don't use on AC (Action Cache) stores. Any store where you can
    /// safely use `VerifySpec.verify_size = true`, this store should be safe
    /// to use (ie: CAS stores).
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "size_partitioning": {
    ///   "size": "128mib",
    ///   "lower_store": {
    ///     "memory": {
    ///       "eviction_policy": {
    ///         "max_bytes": "${NATIVELINK_CAS_MEMORY_CONTENT_LIMIT:-100mb}"
    ///       }
    ///     }
    ///   },
    ///   "upper_store": {
    ///     /// This store discards data larger than 128mib.
    ///     "noop": {}
    ///   }
    /// }
    /// ```
    ///
    SizePartitioning(Box<SizePartitioningSpec>),

    /// This store will pass-through calls to another GRPC store. This store
    /// is not designed to be used as a sub-store of another store, but it
    /// does satisfy the interface and will likely work.
    ///
    /// One major GOTCHA is that some stores use a special function on this
    /// store to get the size of the underlying object, which is only reliable
    /// when this store is serving the a CAS store, not an AC store. If using
    /// this store directly without being a child of any store there are no
    /// side effects and is the most efficient way to use it.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "grpc": {
    ///   "instance_name": "main",
    ///   "endpoints": [
    ///     {"address": "grpc://${CAS_ENDPOINT:-127.0.0.1}:50051"}
    ///   ],
    ///   "connections_per_endpoint": "5",
    ///   "rpc_timeout_s": "5m",
    ///   "store_type": "ac"
    /// }
    /// ```
    ///
    Grpc(GrpcSpec),

    /// Stores data in any stores compatible with Redis APIs.
    ///
    /// Pairs well with `SizePartitioning` and/or `FastSlow` stores.
    /// Ideal for accepting small object sizes as most redis store
    /// services have a max file upload of between 256Mb-512Mb.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "redis_store": {
    ///   "addresses": [
    ///     "redis://127.0.0.1:6379/",
    ///   ],
    ///   "max_client_permits": 1000,
    /// }
    /// ```
    ///
    RedisStore(RedisSpec),

    /// Noop store is a store that sends streams into the void and all data
    /// retrieval will return 404 (`NotFound`). This can be useful for cases
    /// where you may need to partition your data and part of your data needs
    /// to be discarded.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "noop": {}
    /// ```
    ///
    Noop(NoopSpec),

    /// Experimental `MongoDB` store implementation.
    ///
    /// This store uses `MongoDB` as a backend for storing data. It supports
    /// both CAS (Content Addressable Storage) and scheduler data with
    /// optional change streams for real-time updates.
    ///
    /// **Example JSON Config:**
    /// ```json
    /// "experimental_mongo": {
    ///     "connection_string": "mongodb://localhost:27017",
    ///     "database": "nativelink",
    ///     "cas_collection": "cas",
    ///     "key_prefix": "cas:",
    ///     "read_chunk_size": 65536,
    ///     "max_concurrent_uploads": 10,
    ///     "enable_change_streams": false,
    ///     "max_requests": "100"
    /// }
    /// ```
    ///
    ExperimentalMongo(ExperimentalMongoSpec),
}

/// Configuration for an individual shard of the store.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ShardConfig {
    /// Store to shard the data to.
    pub store: StoreSpec,

    /// The weight of the store. This is used to determine how much data
    /// should be sent to the store. The actual percentage is the sum of
    /// all the store's weights divided by the individual store's weight.
    ///
    /// Default: 1
    #[serde(deserialize_with = "convert_optional_numeric_with_shellexpand")]
    pub weight: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ShardSpec {
    /// Stores to shard the data to.
    pub stores: Vec<ShardConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CacheMetricsSpec {
    /// Low-cardinality cache type label for metrics, for example `cas` or `ac`.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cache_type: String,

    /// Store to wrap with cache operation metrics.
    pub backend: StoreSpec,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct SizePartitioningSpec {
    /// Size to partition the data on.
    #[serde(deserialize_with = "convert_data_size_with_shellexpand")]
    pub size: u64,

    /// Store to send data when object is < (less than) size.
    pub lower_store: StoreSpec,

    /// Store to send data when object is >= (less than eq) size.
    pub upper_store: StoreSpec,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct RefSpec {
    /// Name of the store under the root "stores" config object.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct FilesystemSpec {
    /// Path on the system where to store the actual content. This is where
    /// the bulk of the data will be placed.
    /// On service bootup this folder will be scanned and all files will be
    /// added to the cache. In the event one of the files doesn't match the
    /// criteria, the file will be deleted.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub content_path: String,

    /// A temporary location of where files that are being uploaded or
    /// deleted will be placed while the content cannot be guaranteed to be
    /// accurate. This location must be on the same block device as
    /// `content_path` so atomic moves can happen (ie: move without copy).
    /// All files in this folder will be deleted on every startup.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub temp_path: String,

    /// Buffer size to use when reading files. Generally this should be left
    /// to the default value except for testing.
    /// Default: 256k.
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub read_buffer_size: u32,

    /// Policy used to evict items out of the store. Failure to set this
    /// value will cause items to never be removed from the store causing
    /// infinite memory usage.
    pub eviction_policy: Option<EvictionPolicy>,

    /// The block size of the filesystem for the running machine
    /// value is used to determine an entry's actual size on disk consumed
    /// For a 4KB block size filesystem, a 1B file actually consumes 4KB
    /// Default: 4kb
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub block_size: u64,

    /// Maximum number of concurrent write operations allowed.
    /// Each write involves streaming data to a temp file and calling `sync_all()`,
    /// which can saturate disk I/O when many writes happen simultaneously.
    /// Limiting concurrency prevents disk saturation from blocking the async
    /// runtime.
    /// A value of 0 means unlimited (no concurrency limit).
    /// Default: unlimited
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_concurrent_writes: usize,

    /// If true, use sync_data() instead of sync_all() when flushing writes
    /// to disk. sync_data() only syncs the file data without metadata
    /// (timestamps, permissions), which is faster. For content-addressed
    /// storage where the content is verified by hash, metadata sync is
    /// unnecessary and this significantly reduces write latency.
    /// Default: true
    #[serde(default = "default_sync_data_only")]
    pub sync_data_only: bool,

    /// If true, skip writes when a blob with the same key already exists
    /// in the store. This is safe for content-addressed storage (CAS) where
    /// identical keys guarantee identical content. Do NOT enable this for
    /// stores where the same key can hold different content (e.g. action
    /// cache).
    /// When a duplicate write is skipped, the existing entry's access time
    /// is updated in the LRU to prevent premature eviction.
    /// Default: false
    #[serde(default)]
    pub content_is_immutable: bool,

    /// If true, call `posix_fadvise(POSIX_FADV_DONTNEED)` after completing
    /// reads and writes to hint the kernel to drop page-cache pages for the
    /// file. This is useful on deployments with limited RAM where keeping
    /// blobs in page cache would cause memory pressure. On machines with
    /// plenty of free RAM the page cache naturally handles LRU eviction, so
    /// this should be left disabled to allow frequently-accessed blobs to
    /// remain cached (measured: 76% of read I/O is re-reads within seconds).
    /// Only effective on Linux; no-op on other platforms.
    /// Default: false
    #[serde(default)]
    pub fadvise_dontneed: bool,

    /// Maximum concurrent reads for files larger than
    /// `large_read_threshold_bytes`. 0 = disabled (default).
    /// Prevents blocking thread pool exhaustion under high
    /// parallelism with large blobs.
    #[serde(default)]
    pub max_concurrent_large_reads: usize,

    /// Size threshold above which reads are subject to
    /// `max_concurrent_large_reads`. Default: 4 MiB.
    #[serde(default = "default_large_read_threshold")]
    pub large_read_threshold_bytes: u64,

    /// FL-681: byte budget for F2 output blobs pinned-until-BIS-durable in
    /// the worker's local fast store. These "indefinite" pins are exempt
    /// from the 120s pin TTL and are released only by the server's
    /// `BlobsInStableStorage` ack, so they must be bounded independently.
    /// This caps the bytes that may be held in the pending-BIS indefinite-
    /// pin set; over-cap is BACKPRESSURE (a new indefinite pin is refused,
    /// the blob keeps its normal 120s TTL pin and the upload retry loop
    /// keeps retrying), never a drop — no blob is lost.
    /// A value of 0 (the default) falls back to the eviction map's `pin_cap`
    /// (25% of the eviction policy's `max_bytes`), so existing configs are
    /// unchanged. Indefinite pins are a subset of all pins, so a configured
    /// value above `pin_cap` is allowed but cannot exceed the total pin
    /// budget in practice.
    /// Default: 0 (use `pin_cap`).
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub pending_bis_pin_max_bytes: u64,

    /// (FL-688 v3 Stage C — BLOCK-2) Arm the startup reconcile gate at
    /// construction time, BEFORE the on-disk content scan (`add_files_to_cache`)
    /// and the post-scan boot drain (`run_pending_tasks_and_drain`).
    ///
    /// WORKER CAS STORE ONLY. Set this to `true` on the FilesystemStore
    /// that backs the worker's fast CAS tier. The gate suppresses the periodic
    /// background LRU drain (and the one-shot boot drain) until the server
    /// sends `ReconcileCompleteRequest`, ensuring the reconcile-pin step in
    /// the `UploadMissingBlobs` handler fires BEFORE any LRU eviction can race
    /// it and remove needed blobs.
    ///
    /// SERVER FilesystemStores MUST leave this `false` (the default). A server
    /// store never receives `ReconcileCompleteRequest`, so an armed server store
    /// would suppress eviction FOREVER → server disk fills.
    ///
    /// Default: false (gate off — no change for server-side or non-reconcile stores).
    #[serde(default)]
    pub startup_reconcile_gate: bool,

    /// #F3 sibling (2026-07-28): idle TTL, in seconds, for reaping
    /// ABANDONED chunked-partial write state (`SpawnBlocking` entries in
    /// the in-process `chunked_partials` map: open fd + on-disk
    /// `.partial`). A WriteChunkedV2 session abort deliberately leaves
    /// the entry so the next retry resumes the same partial; when the
    /// writers never come back (client gone for good) the entry + fd +
    /// partial previously leaked until process restart and poisoned
    /// Path-A dispatch for the digest with `AlreadyExists`. Entries with
    /// an ACTIVE writer session are never reaped regardless of age;
    /// io_uring marker entries are handled separately (#F3 liveness
    /// takeover) and are exempt.
    ///
    /// Default: 600 (10 minutes). Rationale: the worker deferred-upload
    /// retry cadence is ~41 s — DERIVED from the F3 incident journal
    /// (2026-07-28: ≈3.6K aborts/hour across 41 wedged digests ≈ one
    /// retry per digest per 41 s; a derivation, not a direct
    /// measurement, and it assumes one retry per abort) — so a digest
    /// still being retried refreshes its activity stamp ~14x per TTL
    /// and can never idle out; 600 s is also 10x the 60 s chunked
    /// commit watchdog, so no live commit path can outlast it. A truly
    /// abandoned digest reclaims within TTL + one reap tick (≤ 60 s).
    ///
    /// 0 disables the reaper — an operational KILL-SWITCH, not a resting
    /// state (default stays ON per house policy).
    #[serde(default = "default_chunked_idle_partial_reap_ttl_s")]
    pub chunked_idle_partial_reap_ttl_s: u64,
}

const fn default_chunked_idle_partial_reap_ttl_s() -> u64 {
    600 // 10 min — see the field doc-comment for the cadence math.
}

fn default_large_read_threshold() -> u64 {
    4 * 1024 * 1024 // 4 MiB — reads below this complete too fast to threaten thread pool
}

impl Default for FilesystemSpec {
    fn default() -> Self {
        Self {
            content_path: String::new(),
            temp_path: String::new(),
            read_buffer_size: 0,
            eviction_policy: None,
            block_size: 0,
            max_concurrent_writes: 0,
            sync_data_only: true,
            content_is_immutable: false,
            fadvise_dontneed: false,
            max_concurrent_large_reads: 0,
            large_read_threshold_bytes: 4 * 1024 * 1024,
            pending_bis_pin_max_bytes: 0,
            startup_reconcile_gate: false,
            chunked_idle_partial_reap_ttl_s: default_chunked_idle_partial_reap_ttl_s(),
        }
    }
}

// NetApp ONTAP S3 Spec
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalOntapS3Spec {
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub endpoint: String,
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub vserver_name: String,
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub bucket: String,
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub root_certificates: Option<String>,

    /// Common retry and upload configuration
    #[serde(flatten)]
    pub common: CommonObjectSpec,
}

// Cloudflare R2 Spec
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalR2Spec {
    /// Cloudflare account ID. Endpoint is derived as
    /// `https://{account_id}.r2.cloudflarestorage.com`.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub account_id: String,

    /// Bucket name to use as the backend.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub bucket: String,

    /// Explicit R2 access key.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub access_key_id: Option<String>,

    /// Explicit R2 secret key.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub secret_access_key: Option<String>,

    /// Retry and upload settings.
    #[serde(flatten)]
    pub common: CommonObjectSpec,
}

// Oracle Cloud Infrastructure (OCI) Object Storage Spec.
//
// Uses the OCI Object Storage Amazon S3 Compatibility API. The store talks to
// the path-style compatibility endpoint, which embeds the Object Storage
// namespace in the host and the bucket in the request path:
// `https://{namespace}.compat.objectstorage.{region}.oci.customer-oci.com/{bucket}/{object}`.
// Authentication uses a Customer Secret Key (an Access Key/Secret Key pair
// generated under User Settings in the OCI console) signed with AWS SigV4.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalOciSpec {
    /// OCI Object Storage namespace. This is the immutable, system-generated
    /// top-level container assigned to the tenancy (the same name in every
    /// region). It is the host prefix of the derived path-style endpoint:
    /// `https://{namespace}.compat.objectstorage.{region}.oci.customer-oci.com`.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub namespace: String,

    /// OCI region identifier, for example `us-phoenix-1` or `us-ashburn-1`.
    /// Used both to build the endpoint host and as the AWS `SigV4` signing
    /// region. If your tooling cannot set an OCI region identifier, OCI also
    /// accepts `us-east-1` to target the tenancy home region.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub region: String,

    /// Bucket name to use as the backend. Bucket names must be unique within
    /// the Object Storage namespace.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub bucket: String,

    /// Customer Secret Key access key. When omitted (along with
    /// `secret_access_key`), the default AWS credential chain is used instead.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub access_key_id: Option<String>,

    /// Customer Secret Key secret. OCI does not allow retrieving a secret key
    /// after generation, so store it securely (for example via `${ENV_VAR}`
    /// shell expansion).
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub secret_access_key: Option<String>,

    /// Retry and upload settings.
    #[serde(flatten)]
    pub common: CommonObjectSpec,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct OntapS3ExistenceCacheSpec {
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub index_path: String,
    #[serde(deserialize_with = "convert_numeric_with_shellexpand")]
    pub sync_interval_seconds: u32,
    pub backend: Box<ExperimentalOntapS3Spec>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum StoreDirection {
    /// The store operates normally and all get and put operations are
    /// handled by it.
    #[default]
    Both,
    /// Update operations will cause persistence to this store, but Get
    /// operations will be ignored.
    /// This only makes sense on the fast store as the slow store will
    /// never get written to on Get anyway.
    Update,
    /// Get operations will cause persistence to this store, but Update
    /// operations will be ignored.
    Get,
    /// Operate as a read only store, only really makes sense if there's
    /// another way to write to it.
    ReadOnly,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct FastSlowSpec {
    /// Fast store that will be attempted to be contacted before reaching
    /// out to the `slow` store.
    pub fast: StoreSpec,

    /// How to handle the fast store.  This can be useful to set to Get for
    /// worker nodes such that results are persisted to the slow store only.
    #[serde(default)]
    pub fast_direction: StoreDirection,

    /// If the object does not exist in the `fast` store it will try to
    /// get it from this store.
    pub slow: StoreSpec,

    /// How to handle the slow store.  This can be useful if creating a diode
    /// and you wish to have an upstream read only store.
    #[serde(default)]
    pub slow_direction: StoreDirection,

    /// #212 Phase 2.5 read-cascade kill-switch on `FastSlowStore`.
    /// Set true to enable. Production-flipped 2026-05-02 per user
    /// sign-off.
    ///
    /// When true AND the `chunked_fast_slow` feature is compiled in
    /// AND a `ChunkedReadRegistry` has been installed via
    /// `FastSlowStore::set_chunked_read_registry`, `get_part`
    /// consults the registry between the in-flight slow-write check
    /// and the slow store. With this flag false the registry is
    /// ignored — preserves the pre-Phase-2.5 read path exactly.
    ///
    /// Default: false
    #[serde(default)]
    pub chunked_reads_enabled: bool,

    /// #334 Fix B: aggregate byte cap on the per-`FastSlowStore`
    /// `in_flight_slow_writes` map. The map pins every blob whose
    /// background slow-store write has not terminated; under slow-tier
    /// wedge (e.g. transient gRPC unreachability, ZFS txg pause) the
    /// map grows at upload rate × wedge duration with no bound, which
    /// produced the production OOM cascade observed 2026-05-08 (67 GB
    /// RSS at 1h15m uptime, climbing).
    ///
    /// When the next insert would push aggregate bytes past this cap,
    /// `update` / `update_oneshot` return
    /// `Code::ResourceExhausted` carrying the typed
    /// `BackpressureSignal::SlowWritesAtCapacity` discriminator (see
    /// `nativelink-proto/.../worker_api.proto`). The discriminator
    /// keeps `looks_like_dead_channel` from misclassifying the
    /// rejection as a dead h2 channel (#147 regression risk).
    ///
    /// **Default: 0 (uncapped).** Per red-team #1 (#334 bundle fixup
    /// #6), an 8 GiB inherited-by-default cap on workers (16-32 GiB
    /// RAM hosts where 8 GiB = 25-50% of host memory) would force
    /// upstream ByteStream RPCs to block on slow-tier latency — the
    /// exact pattern that caused the #203 OOM cascade. Server
    /// deployments MUST set this explicitly (see `buildcache-native.json5`
    /// for the production 8 GiB value). Workers explicitly override to
    /// 0 at `local_worker.rs:2674,2778` — that override is now
    /// redundant with this default but preserved for clarity.
    ///
    /// **Recommended values when set explicitly:**
    /// - Server (≥128 GiB RAM, 10GbE): 8 GiB. At ~1.2 GB/s CAS upload
    ///   throughput the cap absorbs ~6.4s of unrelieved slow-tier
    ///   pressure before firing — long enough for typical txg pauses
    ///   or transient gRPC blips, short enough that multi-minute
    ///   wedges can't drag into OOM territory.
    /// - Smaller hosts (32-64 GiB): scale proportionally to leave
    ///   headroom for the rest of the process.
    /// - Workers + tests: 0 (uncapped) — workers have a different
    ///   durability protocol (mirror_blobs + BIS ack) that decouples
    ///   slow-write completion from upstream RPC liveness.
    ///
    /// **Worst-case overshoot** above the cap is bounded by
    /// `concurrent_admissions × max_admission_bytes` (the cap-check is
    /// snapshot-consistent, not strongly-consistent — see the
    /// `in_flight_slow_writes_bytes` field doc on `FastSlowStore` for
    /// the synchronization model). With ByteStream chunks at ≤3 MiB
    /// and `parallel_chunk_count=64`, the worst-case overshoot is
    /// ~192 MiB above the cap — comfortably below OOM territory on
    /// any explicitly-capped server.
    #[serde(default = "default_slow_writes_in_flight_max_bytes")]
    #[serde(deserialize_with = "convert_data_size_with_shellexpand")]
    pub slow_writes_in_flight_max_bytes: u64,

    /// Reads of blobs at or above this size skip the leader/follower dedup
    /// map and stream straight from the slow store without populating the
    /// fast tier. `0` (the default) disables the bypass: every read goes
    /// through dedup, matching the prior behaviour. Enable it by setting a
    /// threshold — 256 MiB is a reasonable starting point for backends where
    /// large-blob dedup is a net loss (followers tend to time out anyway),
    /// but the right value is workload-dependent.
    /// Default: disabled (0)
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub bypass_dedup_threshold_bytes: u64,
}

/// Default cap for `FastSlowSpec::slow_writes_in_flight_max_bytes`.
/// 0 = uncapped — preserves historic behavior bit-identically. Server
/// deployments must opt in explicitly to OOM protection. See field
/// doc-comment for the rationale (red-team #1 finding on #334 bundle).
fn default_slow_writes_in_flight_max_bytes() -> u64 {
    0
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct MemorySpec {
    /// Policy used to evict items out of the store. Failure to set this
    /// value will cause items to never be removed from the store causing
    /// infinite memory usage.
    pub eviction_policy: Option<EvictionPolicy>,

    /// #212 Phase 2.6 — emit `BackpressureSignal::MemoryStoreAtCapacity`
    /// instead of silent eviction at capacity. Production-flipped
    /// 2026-05-02 per user sign-off.
    ///
    /// When true AND the `chunked_fast_slow` feature is compiled in,
    /// `update` / `update_oneshot` reject over-capacity writes with
    /// `Code::ResourceExhausted` carrying a structured
    /// `BackpressureSignal::MemoryStoreAtCapacity` detail INSTEAD of
    /// silently evicting a recent (potentially still-in-use) blob to
    /// make room. Default false preserves the historic silent-evict
    /// behavior.
    ///
    /// Default: false
    #[serde(default)]
    pub emit_backpressure_enabled: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct DedupSpec {
    /// Store used to store the index of each dedup slice. This store
    /// should generally be fast and small.
    pub index_store: StoreSpec,

    /// The store where the individual chunks will be uploaded. This
    /// store should generally be the slower & larger store.
    pub content_store: StoreSpec,

    /// Minimum size that a chunk will be when slicing up the content.
    /// Note: This setting can be increased to improve performance
    /// because it will actually not check this number of bytes when
    /// deciding where to partition the data.
    ///
    /// Default: 64k
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub min_size: u32,

    /// A best-effort attempt will be made to keep the average size
    /// of the chunks to this number. It is not a guarantee, but a
    /// slight attempt will be made.
    ///
    /// This value will also be about the threshold used to determine
    /// if we should even attempt to dedup the entry or just forward
    /// it directly to the `content_store` without an index. The actual
    /// value will be about `normal_size * 1.3` due to implementation
    /// details.
    ///
    /// Default: 256k
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub normal_size: u32,

    /// Maximum size a chunk is allowed to be.
    ///
    /// Default: 512k
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_size: u32,

    /// Due to implementation detail, we want to prefer to download
    /// the first chunks of the file so we can stream the content
    /// out and free up some of our buffers. This configuration
    /// will be used to to restrict the number of concurrent chunk
    /// downloads at a time per `get()` request.
    ///
    /// This setting will also affect how much memory might be used
    /// per `get()` request. Estimated worst case memory per `get()`
    /// request is: `max_concurrent_fetch_per_get * max_size`.
    ///
    /// Default: 10
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_concurrent_fetch_per_get: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExistenceCacheSpec {
    /// The underlying store wrap around. All content will first flow
    /// through self before forwarding to backend. In the event there
    /// is an error detected in self, the connection to the backend
    /// will be terminated, and early termination should always cause
    /// updates to fail on the backend.
    pub backend: StoreSpec,

    /// Policy used to evict items out of the store. Failure to set this
    /// value will cause items to never be removed from the store causing
    /// infinite memory usage.
    pub eviction_policy: Option<EvictionPolicy>,

    /// When `true`, emit `info!` log lines every time this
    /// `ExistenceCacheStore` returns `NotFound` to its caller — both
    /// the `inner_has_with_results` per-slot path and the `get_part`
    /// Err path. Operators can then correlate Bazel-reported AC
    /// misses with a specific digest at the AC layer without enabling
    /// tracing-level debug.
    ///
    /// **Set this only on AC instances.** The CAS-side ExistenceCache
    /// is hit by Bazel's `FindMissingBlobs` pre-action sweep at
    /// 1k-10k entries/sec during build-start bursts — info-level
    /// logging there would produce thousands of false drill-down
    /// candidates per minute (per the
    /// `existence_cache_eviction_codes_test.rs::
    /// get_part_not_found_does_not_log_for_never_cached_digest`
    /// contract test). The AC instance is one-call-per-action and
    /// stays within `info!` budget.
    ///
    /// Default: `false` (preserves the contract test + the CAS-side
    /// silent behavior).
    #[serde(default)]
    pub log_not_found_at_info: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct VerifySpec {
    /// The underlying store wrap around. All content will first flow
    /// through self before forwarding to backend. In the event there
    /// is an error detected in self, the connection to the backend
    /// will be terminated, and early termination should always cause
    /// updates to fail on the backend.
    pub backend: StoreSpec,

    /// If set the store will verify the size of the data before accepting
    /// an upload of data.
    ///
    /// This should be set to false for AC, but true for CAS stores.
    #[serde(default, deserialize_with = "convert_boolean_with_shellexpand")]
    pub verify_size: bool,

    /// If the data should be hashed and verify that the key matches the
    /// computed hash. The hash function is automatically determined based
    /// request and if not set will use the global default.
    ///
    /// This should be set to false for AC, but true for CAS stores.
    #[serde(default, deserialize_with = "convert_boolean_with_shellexpand")]
    pub verify_hash: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CompletenessCheckingSpec {
    /// The underlying store that will have it's results validated before sending to client.
    pub backend: StoreSpec,

    /// When a request is made, the results are decoded and all output digests/files are verified
    /// to exist in this CAS store before returning success.
    pub cas_store: StoreSpec,

    /// When `true`, the completeness check is skipped entirely on every
    /// `GetActionResult` / `has_with_results` — `CompletenessCheckingStore`
    /// becomes a transparent pass-through to the underlying `backend` store.
    ///
    /// **Phase 1 kill-switch (CCS-drop Option A).** Default: `false` (check active).
    ///
    /// Operators flip this to `true` for the soak period before the full CCS
    /// removal (Phase 2). With `true`:
    ///   - No AC-decode-for-verification.
    ///   - No per-referenced-digest `has_with_results` against the CAS.
    ///   - No Tree-proto fetch+decode for output directories.
    ///   - No `consult_pending_registry` call.
    ///
    /// Stale AC entries (referencing evicted CAS blobs) are served rather than
    /// filtered; Bazel handles re-execution via
    /// `--experimental_remote_cache_eviction_retries`. The worker-fetch path
    /// (`WorkerProxyStore` + `BlobLocalityMap`) is unaffected — it is
    /// AC-chain-independent and already serves fresh outputs.
    ///
    /// Roll back: set to `false` (no binary redeploy required).
    #[serde(default)]
    pub disable_completeness_check: bool,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct Lz4Config {
    /// Size of the blocks to compress.
    /// Higher values require more ram, but might yield slightly better
    /// compression ratios.
    ///
    /// Default: 65536 (64k).
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub block_size: u32,

    /// Maximum size allowed to attempt to deserialize data into.
    /// This is needed because the `block_size` is embedded into the data
    /// so if there was a bad actor, they could upload an extremely large
    /// `block_size`'ed entry and we'd allocate a large amount of memory
    /// when retrieving the data. To prevent this from happening, we
    /// allow you to specify the maximum that we'll attempt to deserialize.
    ///
    /// Default: value in `block_size`.
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_decode_block_size: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum CompressionAlgorithm {
    /// LZ4 compression algorithm is extremely fast for compression and
    /// decompression, however does not perform very well in compression
    /// ratio. In most cases build artifacts are highly compressible, however
    /// lz4 is quite good at aborting early if the data is not deemed very
    /// compressible.
    ///
    /// see: <https://lz4.github.io/lz4/>
    Lz4(Lz4Config),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CompressionSpec {
    /// The underlying store wrap around. All content will first flow
    /// through self before forwarding to backend. In the event there
    /// is an error detected in self, the connection to the backend
    /// will be terminated, and early termination should always cause
    /// updates to fail on the backend.
    pub backend: StoreSpec,

    /// The compression algorithm to use.
    pub compression_algorithm: CompressionAlgorithm,
}

/// Eviction policy always works on LRU (Least Recently Used). Any time an entry
/// is touched it updates the timestamp. Inserts and updates will execute the
/// eviction policy removing any expired entries and/or the oldest entries
/// until the store size becomes smaller than `max_bytes`.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct EvictionPolicy {
    /// Maximum number of bytes before eviction takes place.
    /// Default: 0. Zero means never evict based on size.
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_bytes: usize,

    /// When eviction starts based on hitting `max_bytes`, continue until
    /// `max_bytes - evict_bytes` is met to create a low watermark.  This stops
    /// operations from thrashing when the store is close to the limit.
    /// Default: 0
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub evict_bytes: usize,

    /// Maximum number of seconds for an entry to live since it was last
    /// accessed before it is evicted.
    /// Default: 0. Zero means never evict based on time.
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub max_seconds: u32,

    /// Maximum size of the store before an eviction takes place.
    /// Default: 0. Zero means never evict based on count.
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_count: u64,

    /// FL-681 NAK boundary fix: explicit ceiling (in bytes) on the total data
    /// that may be PINNED (held un-evictable) in this store's eviction map.
    /// This is the ceiling the worker's admission NAK gate (`indefinite_pin_saturated`)
    /// and the total pin refusal both measure against.
    ///
    /// 0 (the default) = derive the cap as `max_bytes * PIN_CAP_FRACTION` (25%),
    /// preserving the historical behavior for every existing config. A non-zero
    /// value overrides that derived cap directly, letting an operator raise the
    /// pin budget (e.g. to 50% of `max_bytes`) without changing `max_bytes`.
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub pin_cap_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "provider", rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum ExperimentalCloudObjectSpec {
    Aws(ExperimentalAwsSpec),
    Gcs(ExperimentalGcsSpec),
    Azure(ExperimentalAzureSpec),
    Ontap(ExperimentalOntapS3Spec),
    R2(ExperimentalR2Spec),
    Oci(ExperimentalOciSpec),
}

impl Default for ExperimentalCloudObjectSpec {
    fn default() -> Self {
        Self::Aws(ExperimentalAwsSpec::default())
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalAwsSpec {
    /// S3 region. Usually us-east-1, us-west-2, af-south-1, exc...
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub region: String,

    /// Bucket name to use as the backend.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub bucket: String,

    /// Common retry and upload configuration
    #[serde(flatten)]
    pub common: CommonObjectSpec,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalGcsSpec {
    /// Bucket name to use as the backend.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub bucket: String,

    /// Chunk size for resumable uploads.
    ///
    /// Default: 2MB
    #[serde(
        default,
        deserialize_with = "convert_optional_data_size_with_shellexpand"
    )]
    pub resumable_chunk_size: Option<usize>,

    /// Common retry and upload configuration
    #[serde(flatten)]
    pub common: CommonObjectSpec,

    /// Error if authentication was not found.
    #[serde(default, deserialize_with = "convert_boolean_with_shellexpand")]
    pub authentication_required: bool,

    /// Connection timeout in milliseconds.
    /// Default: 3000
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub connection_timeout_s: u64,

    /// Read timeout in milliseconds.
    /// Default: 3000
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub read_timeout_s: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalAzureSpec {
    /// The Azure Storage account name. Used to build the default container URL
    /// `https://{account_name}.blob.core.windows.net/{container}` when `sas_url`
    /// is not provided.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub account_name: String,

    /// The container name to use as the backend.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub container: String,

    /// Optional blob endpoint host override (for example an Azurite emulator host
    /// such as `http://127.0.0.1:10000/devstoreaccount1`). When set, this replaces
    /// the default `https://{account_name}.blob.core.windows.net` endpoint. The
    /// container is always appended to form the final container URL. Ignored when
    /// `sas_url` is set.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub endpoint: Option<String>,

    /// Optional pre-formed SAS URL pointing at the container. When set, the store
    /// uses it directly as the container URL with no credential (the SAS token is
    /// expected to already be present in the URL), and `account_name`, `container`,
    /// and `endpoint` are ignored for URL construction.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub sas_url: Option<String>,

    /// Common retry and upload configuration.
    #[serde(flatten)]
    pub common: CommonObjectSpec,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CommonObjectSpec {
    /// If you wish to prefix the location in the bucket. If None, no prefix will be used.
    #[serde(default)]
    pub key_prefix: Option<String>,

    /// Retry configuration to use when a network request fails.
    #[serde(default)]
    pub retry: Retry,

    /// If the number of seconds since the `last_modified` time of the object
    /// is greater than this value, the object will not be considered
    /// "existing". This allows for external tools to delete objects that
    /// have not been uploaded in a long time. If a client receives a `NotFound`
    /// the client should re-upload the object.
    ///
    /// There should be sufficient buffer time between how long the expiration
    /// configuration of the external tool is and this value. Keeping items
    /// around for a few days is generally a good idea.
    ///
    /// Default: 0. Zero means never consider an object expired.
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub consider_expired_after_s: u32,

    /// The maximum buffer size to retain in case of a retryable error
    /// during upload. Setting this to zero will disable upload buffering;
    /// this means that in the event of a failure during upload, the entire
    /// upload will be aborted and the client will likely receive an error.
    ///
    /// Default: 5MB.
    #[serde(
        default,
        deserialize_with = "convert_optional_data_size_with_shellexpand"
    )]
    pub max_retry_buffer_per_request: Option<usize>,

    /// Maximum number of concurrent `UploadPart` requests per `MultipartUpload`.
    ///
    /// Default: 10.
    ///
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub multipart_max_concurrent_uploads: Option<usize>,

    /// Allow unencrypted HTTP connections. Only use this for local testing.
    ///
    /// Default: false
    #[serde(default, deserialize_with = "convert_boolean_with_shellexpand")]
    pub insecure_allow_http: bool,

    /// Disable http/2 connections and only use http/1.1. Default client
    /// configuration will have http/1.1 and http/2 enabled for connection
    /// schemes. Http/2 should be disabled if environments have poor support
    /// or performance related to http/2. Safe to keep default unless
    /// underlying network environment, S3, or GCS API servers specify otherwise.
    ///
    /// Default: false
    #[serde(default, deserialize_with = "convert_boolean_with_shellexpand")]
    pub disable_http2: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum StoreType {
    /// The store is content addressable storage.
    Cas,
    /// The store is an action cache.
    Ac,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ClientTlsConfig {
    /// Path to the certificate authority to use to validate the remote.
    ///
    /// Default: None
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub ca_file: Option<String>,

    /// Path to the certificate file for client authentication.
    ///
    /// Default: None
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub cert_file: Option<String>,

    /// Path to the private key file for client authentication.
    ///
    /// Default: None
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub key_file: Option<String>,

    /// If set the client will use the native roots for TLS connections.
    ///
    /// Default: false
    #[serde(default)]
    pub use_native_roots: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct GrpcEndpoint {
    /// The endpoint address (i.e. grpc(s)://example.com:443).
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub address: String,
    /// The TLS configuration to use to connect to the endpoint (if grpcs).
    pub tls_config: Option<ClientTlsConfig>,
    /// The maximum concurrency to allow on this endpoint.
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub concurrency_limit: Option<usize>,

    /// Timeout for establishing a TCP connection to the endpoint (seconds).
    /// If not set or 0, defaults to 30 seconds.
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub connect_timeout_s: u64,

    /// TCP keepalive interval (seconds). Sends TCP keepalive probes at this
    /// interval to detect dead connections at the OS level.
    /// If not set or 0, defaults to 30 seconds.
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub tcp_keepalive_s: u64,

    /// HTTP/2 keepalive interval (seconds). Sends HTTP/2 PING frames at this
    /// interval to detect dead connections at the application level.
    /// If not set or 0, defaults to 30 seconds.
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub http2_keepalive_interval_s: u64,

    /// HTTP/2 keepalive timeout (seconds). If a PING response is not received
    /// within this duration, the connection is considered dead.
    /// If not set or 0, defaults to 20 seconds.
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub http2_keepalive_timeout_s: u64,

    /// Whether to set TCP_NODELAY on the connection socket.
    /// Disables Nagle's algorithm, reducing latency for small writes.
    /// Default: true
    #[serde(default = "default_tcp_nodelay")]
    pub tcp_nodelay: bool,

    /// When true, connect using QUIC/HTTP3 instead of TCP/HTTP2.
    /// Requires the `quic` feature flag and a server listening on an
    /// `http3` listener. `connections_per_endpoint` controls how many
    /// independent QUIC connections are opened to distribute streams
    /// across separate quinn Connection mutexes.
    /// Default: true
    #[serde(default = "default_use_http3")]
    pub use_http3: bool,
}

fn default_use_http3() -> bool {
    true
}

fn default_sync_data_only() -> bool {
    true
}

fn default_tcp_nodelay() -> bool {
    true
}

fn default_batch_update_threshold_bytes() -> u64 {
    1_048_576
}

const fn default_connections_per_endpoint() -> usize {
    32
}

fn default_parallel_chunk_read_threshold() -> u64 {
    8 * 1024 * 1024
}

fn default_parallel_chunk_count() -> u64 {
    // 64 stream-per-blob multipliers × race-mode `JoinHandle::abort()` on
    // the loser produced enough RST_STREAMs (>1024/sec) to trip hyper's
    // `max_local_error_reset_streams` and emit `GOAWAY(ENHANCE_YOUR_CALM)`
    // (#147 producer-side root cause). 16 keeps useful parallelism for
    // large blobs (4x vs single-stream) while shrinking the abort multiplier 4x.
    16
}

fn default_max_concurrent_batch_rpcs() -> u64 {
    32
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct GrpcSpec {
    /// Instance name for GRPC calls. Proxy calls will have the `instance_name` changed to this.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub instance_name: String,

    /// The endpoint of the grpc connection.
    pub endpoints: Vec<GrpcEndpoint>,

    /// The type of the upstream store, this ensures that the correct server calls are made.
    pub store_type: StoreType,

    /// Retry configuration to use when a network request fails.
    #[serde(default)]
    pub retry: Retry,

    /// Limit the number of simultaneous upstream requests to this many.  A
    /// value of zero is treated as unlimited.  If the limit is reached the
    /// request is queued.
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_concurrent_requests: usize,

    /// The number of connections to make to each specified endpoint to balance
    /// the load over multiple TCP connections.  Default 16.
    #[serde(
        default = "default_connections_per_endpoint",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub connections_per_endpoint: usize,

    /// Per-chunk no-progress timeout (seconds) for `ByteStream.Write`.
    ///
    /// The timer is reset each time a `WriteRequest` chunk is delivered
    /// from the upstream producer to the gRPC client. If no chunk arrives
    /// within this duration, the RPC is aborted with `DeadlineExceeded`.
    ///
    /// This is **not** a whole-RPC deadline. A slow-but-progressing
    /// producer (e.g. a 50 MB mirror upload streaming through a slow
    /// Bazel client at 2 MB/s, taking 25s end-to-end) will not be
    /// killed, only stuck transports are. The previous whole-RPC
    /// deadline broke the >=2-replica durability invariant for
    /// in-flight mirror writes.
    ///
    /// A value of 0 (the default) disables the per-chunk timer. Dead
    /// connections are still detected by the HTTP/2 and TCP keepalive
    /// mechanisms configured on each endpoint.
    ///
    /// Only the streaming `write()` path honours this field; non-
    /// streaming RPCs (`has`, `get_part`, `batch_*`, `get_tree`,
    /// `*_action_result`, `query_write_status`) have no per-RPC deadline.
    ///
    /// Default: 0 (disabled)
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub rpc_timeout_s: u64,

    /// Maximum blob size (in bytes) for using BatchUpdateBlobs instead of
    /// ByteStream.Write. Blobs at or below this size skip per-blob streaming
    /// overhead (UUID generation, resource_name, streaming setup). Only
    /// applies to CAS stores, not AC.
    ///
    /// Set to 0 to disable (all uploads use ByteStream.Write).
    ///
    /// Default: 1048576 (1 MiB)
    #[serde(
        default = "default_batch_update_threshold_bytes",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub batch_update_threshold_bytes: u64,

    /// Maximum number of BatchUpdateBlobs RPCs that can be in flight
    /// concurrently from the batch loop. Higher values reduce
    /// head-of-line blocking when many small blobs are queued, at the
    /// cost of more concurrent server load.
    ///
    /// Only takes effect when batching is enabled
    /// (`batch_update_threshold_bytes > 0`).
    ///
    /// Default: 32
    #[serde(
        default = "default_max_concurrent_batch_rpcs",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub max_concurrent_batch_rpcs: u64,

    /// Minimum blob size (in bytes) to trigger parallel chunked
    /// ByteStream reads. Blobs at or above this size are split into
    /// `parallel_chunk_count` concurrent Read RPCs, each fetching a
    /// different byte range, then reassembled in order. This bypasses
    /// per-stream flow control limits and saturates high-bandwidth links.
    ///
    /// Set to 0 to disable parallel reads entirely.
    ///
    /// Default: 8388608 (8 MiB)
    #[serde(
        default = "default_parallel_chunk_read_threshold",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub parallel_chunk_read_threshold: u64,

    /// Number of parallel ByteStream Read RPCs to issue when a blob
    /// exceeds `parallel_chunk_read_threshold`. Each chunk fetches
    /// `ceil(remaining / parallel_chunk_count)` bytes. More chunks
    /// increase parallelism but also RPC overhead.
    ///
    /// Default: 16 (lowered from 64 in #147 to keep h2 RST_STREAM emission
    /// per connection well under hyper's `max_local_error_reset_streams=1024`
    /// budget when race-loser tasks drop in-flight streams).
    #[serde(
        default = "default_parallel_chunk_count",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub parallel_chunk_count: u64,

    /// When true and `use_http3` is also true on an endpoint, create both
    /// TCP and QUIC transports. RPCs are routed to the best transport
    /// based on benchmark data: QUIC for small/batched RPCs (FindMissing,
    /// BatchUpdate, BatchRead, single-stream reads, AC lookups), TCP for
    /// high-concurrency parallel reads and large streaming writes.
    ///
    /// Requires the `quic` feature flag. Ignored when `use_http3` is false.
    ///
    /// Default: false
    #[serde(default)]
    pub dual_transport: bool,

    /// Enable zstd compression at the tonic (gRPC transport) level for
    /// this client connection. When enabled, the client sends
    /// `grpc-accept-encoding: zstd` so the server compresses responses,
    /// and sends `grpc-encoding: zstd` to compress outgoing requests.
    ///
    /// This is most valuable for worker<->server traffic over LAN where
    /// source files compress ~4:1, saving 10-80ms per action at 10GbE.
    /// CPU overhead is negligible on modern CPUs (zstd ~3GB/s).
    ///
    /// Requires the server listener to also accept zstd compression.
    ///
    /// Default: false
    #[serde(default)]
    pub zstd_compression: bool,

    /// Cap on `ConnectionManager::connection().await` for write-side RPCs
    /// (currently `bytestream_write` for both Tcp and Dual transports).
    /// `None` preserves the default behavior of waiting indefinitely until
    /// the connection_manager produces a channel — appropriate for general
    /// GrpcStore consumers where reads are on the critical path.
    ///
    /// `Some(ms)` is used by `WorkerProxyStore::create_worker_connection`
    /// (set to 3000) so that mirror writes to a dead worker fast-fail
    /// instead of queuing against the 256-slot connection backlog and
    /// holding per-worker mirror permits while waiting for a 1s reconnect
    /// backoff to retry.
    ///
    /// Default: None
    #[serde(default)]
    pub connection_acquire_timeout_ms: Option<u64>,

    /// #212 Phase 2.4 worker-side WriteChunked client kill-switch.
    /// Set true to enable. Production-flipped 2026-05-02 per user
    /// sign-off.
    ///
    /// When true AND the `chunked_fast_slow` feature is compiled in,
    /// blobs at or above `CHUNK_SIZE` (1 MiB) are dispatched through
    /// `chunked::chunked_client::write_chunked_stream` to the
    /// server's `WorkerApi/WriteChunked` RPC instead of the legacy
    /// in-order `ByteStream.Write` path. Smaller blobs continue to
    /// take the legacy path regardless.
    ///
    /// Default: false (legacy path)
    #[serde(default)]
    pub chunked_writes_enabled: bool,

    /// Use legacy `ByteStream` resource name format, omitting the digest
    /// function component from the path.
    ///
    /// Modern `NativeLink` generates resource names like:
    ///   `{instance}/blobs/{digest_function}/{hash}/{size}`
    ///
    /// Older backends (e.g. Buildbarn pre-v0.3) expect the original format:
    ///   `{instance}/blobs/{hash}/{size}`
    ///
    /// Set this to `true` when connecting to such backends to avoid
    /// `InvalidArgument: Unsupported digest function` errors.
    ///
    /// Default: false
    #[serde(default, deserialize_with = "convert_boolean_with_shellexpand")]
    pub use_legacy_resource_names: bool,
}

/// The possible error codes that might occur on an upstream request.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum ErrorCode {
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
    // Note: This list is duplicated from nativelink-error/lib.rs.
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct RedisSpec {
    /// The hostname or IP address of the Redis server.
    /// Ex: `["redis://username:password@redis-server-url:6380/99"]`
    /// 99 Represents database ID, 6380 represents the port.
    #[serde(deserialize_with = "convert_vec_string_with_shellexpand")]
    pub addresses: Vec<String>,

    /// DEPRECATED: use `command_timeout_ms`
    /// The response timeout for the Redis connection in seconds.
    ///
    /// Default: 10
    #[serde(default)]
    pub response_timeout_s: u64,

    /// DEPRECATED: use `connection_timeout_ms`
    ///
    /// The connection timeout for the Redis connection in seconds.
    ///
    /// Default: 10
    #[serde(default)]
    pub connection_timeout_s: u64,

    /// An optional and experimental Redis channel to publish write events to.
    ///
    /// If set, every time a write operation is made to a Redis node
    /// then an event will be published to a Redis channel with the given name.
    /// If unset, the writes will still be made,
    /// but the write events will not be published.
    ///
    /// Default: (Empty String / No Channel)
    #[serde(default)]
    pub experimental_pub_sub_channel: Option<String>,

    /// An optional prefix to prepend to all keys in this store.
    ///
    /// Setting this value can make it convenient to query or
    /// organize your data according to the shared prefix.
    ///
    /// Default: (Empty String / No Prefix)
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub key_prefix: String,

    /// Set the mode Redis is operating in.
    ///
    /// Available options are "cluster" for
    /// [cluster mode](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/),
    /// "sentinel" for [sentinel mode](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/),
    /// or "standard" if Redis is operating in neither cluster nor sentinel mode.
    ///
    /// Default: standard,
    #[serde(default)]
    pub mode: RedisMode,

    /// Deprecated as redis-rs doesn't use it
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub broadcast_channel_capacity: usize,

    /// The amount of time in milliseconds until the redis store considers the
    /// command to be timed out. This will trigger a retry of the command and
    /// potentially a reconnection to the redis server.
    ///
    /// Default: 10000 (10 seconds)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub command_timeout_ms: u64,

    /// The amount of time in milliseconds until the redis store considers the
    /// connection to unresponsive. This will trigger a reconnection to the
    /// redis server.
    ///
    /// Default: 3000 (3 seconds)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub connection_timeout_ms: u64,

    /// Per-call ceiling for the `check_health` PING in milliseconds.
    ///
    /// Default: 4000 (4 seconds)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub health_check_timeout_ms: u64,

    /// The amount of data to read from the redis server at a time.
    /// This is used to limit the amount of memory used when reading
    /// large objects from the redis server as well as limiting the
    /// amount of time a single read operation can take.
    ///
    /// IMPORTANT: If this value is too high, the `command_timeout_ms`
    /// might be triggered if the latency or throughput to the redis
    /// server is too low.
    ///
    /// Default: 64KiB
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub read_chunk_size: usize,

    /// The number of multiplexed connections to keep open to the redis
    /// server(s). For `Standard` and `Sentinel` modes, requests are
    /// round-robin distributed across this many `ConnectionManager`
    /// instances. Each entry is its own multiplexed connection — N
    /// connections multiplies the in-flight queue capacity by N. Use
    /// higher values when you observe Redis commands pipelining serially
    /// behind a single connection (e.g. STRLEN+EXISTS taking seconds
    /// under high concurrency).
    ///
    /// In `Cluster` mode the value is ignored — `redis-rs` maintains its
    /// own per-node connection routing internally.
    ///
    /// Default: 3
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub connection_pool_size: usize,

    /// The maximum number of upload chunks to allow per update.
    /// This is used to limit the amount of memory used when uploading
    /// large objects to the redis server. A good rule of thumb is to
    /// think of the data as:
    /// `AVAIL_MEMORY / (read_chunk_size * max_chunk_uploads_per_update) = THORETICAL_MAX_CONCURRENT_UPLOADS`
    /// (note: it is a good idea to divide `AVAIL_MAX_MEMORY` by ~10 to account for other memory usage)
    ///
    /// Default: 10
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_chunk_uploads_per_update: usize,

    /// The COUNT value passed when scanning keys in Redis.
    /// This is used to hint the amount of work that should be done per response.
    ///
    /// Default: 10000
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub scan_count: usize,

    /// Retry configuration to use when a network request fails.
    #[serde(default)]
    pub retry: Retry,

    /// Maximum number of permitted actions to the Redis store at any one time
    /// This stops problems with timeouts due to many, many inflight actions
    /// Default: 500
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_client_permits: usize,

    /// Maximum number of items returned per cursor for the search indexes
    /// May reduce thundering herd issues with worker provisioner at higher node counts,
    /// Default: 1500
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_count_per_cursor: u64,

    /// Whether the store should subscribe to Redis keyspace notifications
    /// (`__keyevent@<db>__:{del,expired,evicted}`) and dispatch them to
    /// every `ItemCallback` registered via `register_item_callback`.
    ///
    /// When `true`, `RedisStore::new_standard` eagerly issues `CONFIG GET`
    /// + `CONFIG SET notify-keyspace-events <merged>` (merged with any
    /// operator-set flags so we never trample) and `PSUBSCRIBE
    /// __keyevent@<db>__:{del,expired,evicted}` at construction. When
    /// `false`, those operations are skipped and `register_item_callback`
    /// returns `Err` — wrappers such as `ExistenceCacheStore` panic at
    /// construction rather than silently retain stale-positive entries
    /// when keys are evicted under `maxmemory-policy=allkeys-lru`.
    ///
    /// Set to `false` ONLY if the configured Redis/Valkey user lacks the
    /// `+config` ACL (a hardened-cluster default), the `CONFIG` command
    /// has been renamed/disabled, or notifications are configured
    /// out-of-band and you want NativeLink to skip the runtime mutation.
    /// In that case the operator MUST also remove any `ExistenceCacheStore`
    /// wrapping this `RedisStore` from the config; otherwise
    /// `ExistenceCacheStore::new_with_time` panics at construction
    /// because `register_item_callback` returns
    /// `Code::FailedPrecondition` (and the panic propagates up the
    /// production CAS chain `cas_INNER → SizePartitioning → FastSlow →
    /// REDIS_CAS_SMALL_STORE`, putting the server into a systemd restart
    /// loop).
    ///
    /// Default: `true`
    #[serde(default = "default_enable_keyspace_notifications")]
    pub enable_keyspace_notifications: bool,

    /// Logical Redis database index to subscribe to for keyspace
    /// notifications. Used to construct the `__keyevent@<db>__:*` channel
    /// pattern. Has no effect in cluster mode (cluster mode keyspace
    /// notification semantics are documented as undefined in Redis).
    ///
    /// Must agree with the database embedded in the connection URL (the path
    /// segment of `redis://host[/db]`). NativeLink rejects mismatches at
    /// startup so an operator who sets `redis://server/3` but leaves this at
    /// the default `0` cannot end up listening for keyevent notifications on
    /// the wrong database (silent stale-positive cache).
    ///
    /// Default: 0 (the standard Redis default db).
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub keyspace_notifications_db: u8,
}

const fn default_enable_keyspace_notifications() -> bool {
    true
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum RedisMode {
    /// Use Redis Cluster.
    Cluster,

    /// Use Redis Sentinel.
    Sentinel,

    /// Use a standalone Redis server.
    #[default]
    Standard,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct NoopSpec {}

/// Retry configuration. This configuration is exponential and each iteration
/// a jitter as a percentage is applied of the calculated delay. For example:
/// ```haskell
/// Retry{
///   max_retries: 7,
///   delay: 0.1,
///   jitter: 0.5,
/// }
/// ```
/// will result in:
/// Attempt - Delay
/// 1         0ms
/// 2         75ms - 125ms
/// 3         150ms - 250ms
/// 4         300ms - 500ms
/// 5         600ms - 1s
/// 6         1.2s - 2s
/// 7         2.4s - 4s
/// 8         4.8s - 8s
/// Remember that to get total results is additive, meaning the above results
/// would mean a single request would have a total delay of 9.525s - 15.875s.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct Retry {
    /// Maximum number of retries until retrying stops.
    /// Setting this to zero will always attempt 1 time, but not retry.
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_retries: usize,

    /// Delay in seconds for exponential back off.
    #[serde(default)]
    pub delay: f32,

    /// Amount of jitter to add as a percentage in decimal form. This will
    /// change the formula like:
    /// ```haskell
    /// random(
    ///    (2 ^ {attempt_number}) * {delay} * (1 - (jitter / 2)),
    ///    (2 ^ {attempt_number}) * {delay} * (1 + (jitter / 2)),
    /// )
    /// ```
    #[serde(default)]
    pub jitter: f32,

    /// A list of error codes to retry on, if this is not set then the default
    /// error codes to retry on are used.  These default codes are the most
    /// likely to be non-permanent.
    ///  - `Unknown`
    ///  - `Cancelled`
    ///  - `DeadlineExceeded`
    ///  - `ResourceExhausted`
    ///  - `Aborted`
    ///  - `Internal`
    ///  - `Unavailable`
    ///  - `DataLoss`
    #[serde(default)]
    pub retry_on_errors: Option<Vec<ErrorCode>>,
}

/// Configuration for `ExperimentalMongoDB` store.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalMongoSpec {
    /// `ExperimentalMongoDB` connection string.
    /// Example: <mongodb://localhost:27017> or <mongodb+srv://cluster.mongodb.net>
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub connection_string: String,

    /// The database name to use.
    /// Default: "nativelink"
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub database: String,

    /// The collection name for CAS data.
    /// Default: "cas"
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub cas_collection: String,

    /// The collection name for scheduler data.
    /// Default: "scheduler"
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler_collection: String,

    /// Prefix to prepend to all keys stored in `MongoDB`.
    /// Default: ""
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub key_prefix: Option<String>,

    /// The maximum amount of data to read from `MongoDB` in a single chunk (in bytes).
    /// Default: 65536 (64KB)
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub read_chunk_size: usize,

    /// Deprecated, unused
    /// Maximum number of concurrent uploads allowed.
    /// Default: 10
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_concurrent_uploads: usize,

    /// Connection timeout in milliseconds.
    /// Default: 3000
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub connection_timeout_ms: u64,

    /// Command timeout in milliseconds.
    /// Default: 10000
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub command_timeout_ms: u64,

    /// Enable `MongoDB` change streams for real-time updates.
    /// Required for scheduler subscriptions.
    /// Default: false
    #[serde(default, deserialize_with = "convert_boolean_with_shellexpand")]
    pub enable_change_streams: bool,

    /// Write concern 'w' parameter.
    /// Can be a number (e.g., 1) or string (e.g., "majority").
    /// Default: None (uses `MongoDB` default)
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub write_concern_w: Option<String>,

    /// Write concern 'j' parameter (journal acknowledgment).
    /// Default: None (uses `MongoDB` default)
    #[serde(default)]
    pub write_concern_j: Option<bool>,

    /// Write concern timeout in milliseconds.
    /// Default: None (uses `MongoDB` default)
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub write_concern_timeout_ms: Option<u32>,

    /// Limits the number of requests at any one time
    /// Default: Unlimited
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub max_requests: Option<usize>,
}

impl Retry {
    pub fn make_jitter_fn(&self) -> Arc<dyn Fn(Duration) -> Duration + Send + Sync> {
        if self.jitter == 0f32 {
            Arc::new(move |delay: Duration| delay)
        } else {
            let local_jitter = self.jitter;
            Arc::new(move |delay: Duration| {
                delay.mul_f32(local_jitter.mul_add(rand::rng().random::<f32>() - 0.5, 1.))
            })
        }
    }
}

/// Operator knobs for the process-wide AC pin registry. The registry caps
/// per-endpoint AC pin entries to bound server memory under a misbehaving
/// or compromised worker that advertises an unbounded pin set.
///
/// Why expose this: the default sizes the cap to a worker's AC fast-tier
/// capacity (~100K-entry MemoryStore × ~10 workers = ~1M tuples
/// server-wide), but a deployment with substantially larger or smaller
/// AC fast tiers, or a different worker count, must be able to
/// re-tension the cap without rebuilding. Hardcoding the cap turns a
/// capacity-planning decision into a code change.
///
/// The cap defaults to [`nativelink_util::ac_pin_registry::DEFAULT_MAX_AC_PINS_PER_ENDPOINT`]
/// (1_000_000). Unset = use the default.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct AcPinRegistryConfig {
    /// Maximum number of `(store_id, digest)` AC pin tuples retained per
    /// connected worker endpoint before the registry silently drops new
    /// entries. The drop is logged at `warn!` and rate-limited; entries
    /// are recoverable via the worker's next periodic `BlobsAvailable`
    /// resync, so a transient spike above the cap surfaces as a
    /// visibility delay rather than data loss.
    ///
    /// Set to bound server memory in the worst case
    /// (compromised / misbehaving worker advertising an unbounded pin
    /// set). Default: 1_000_000 — see
    /// [`nativelink_util::ac_pin_registry::DEFAULT_MAX_AC_PINS_PER_ENDPOINT`].
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub max_entries_per_endpoint: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default-value regression test (testing-czar MAJOR — 2026-05-10
    /// 9-reviewer cadre).
    ///
    /// `enable_keyspace_notifications` MUST default to `true` for the
    /// production CAS chain on buildcache to construct without panic. The
    /// chain is `cas_INNER (ExistenceCacheStore) → SizePartitioning →
    /// FastSlow { fast: Memory, slow: REDIS_CAS_SMALL_STORE }`. ECS calls
    /// `register_item_callback(...).expect("Register item callback should
    /// work")` at `existence_cache_store.rs:215-216`;
    /// SizePartitioning/FastSlow propagate the `Err`;
    /// `RedisStore::register_item_callback` returns
    /// `Code::FailedPrecondition` whenever
    /// `enable_keyspace_notifications=false`. Result: ECS construction
    /// panic → `nativelink.service` enters a systemd restart loop.
    /// Production Valkey grants `+@all` (verified by `valkey-cli ACL
    /// LIST` 2026-05-10), so the `true` default is correct for the
    /// deployment.
    ///
    /// Mutation step (CLAUDE.md TDD rule 5): flip
    /// `default_enable_keyspace_notifications` to return `false` — this
    /// test must red-fail with the bespoke
    /// `"enable_keyspace_notifications default MUST stay true ..."`
    /// message.
    #[test]
    fn enable_keyspace_notifications_defaults_to_true() {
        // Minimal RedisSpec JSON5 — only the required `addresses` field
        // is set so every other field, including
        // `enable_keyspace_notifications`, lands on its serde default.
        let spec: RedisSpec = serde_json5::from_str(
            r#"{ "addresses": ["redis://127.0.0.1:6379/"] }"#,
        )
        .expect("RedisSpec must deserialize from minimal JSON5");

        assert!(
            spec.enable_keyspace_notifications,
            "enable_keyspace_notifications default MUST stay true — flipping to \
             false will boot-panic the production CAS chain (ECS → SizePartitioning \
             → FSS → REDIS_CAS_SMALL_STORE) because ECS::new_with_time .expect()s \
             register_item_callback to succeed; verified by code-reviewer + DSR \
             cadre on 2026-05-11."
        );
    }

    /// Companion: confirm the `default_enable_keyspace_notifications()`
    /// const-fn itself returns `true`. Belt-and-suspenders coverage —
    /// the deserialization test above goes through serde while this one
    /// touches the const directly, so a misguided refactor that swaps in
    /// a different defaulting mechanism is still caught.
    #[test]
    fn default_enable_keyspace_notifications_const_returns_true() {
        assert!(
            default_enable_keyspace_notifications(),
            "default_enable_keyspace_notifications() MUST return true — see \
             enable_keyspace_notifications_defaults_to_true for the production \
             boot-panic mechanism this guards."
        );
    }

    /// #chunked-v1-removal: the `chunked_v2_writes_enabled` field was deleted
    /// once the v1 `WriteChunked` worker-upload path was removed (workers now
    /// always use v2). Post-removal contract (a): a `GrpcSpec` WITHOUT the key
    /// still deserializes cleanly — nothing selects it anymore.
    #[test]
    fn grpc_spec_parses_without_chunked_v2_writes_enabled_key() {
        let spec: GrpcSpec = serde_json5::from_str(
            r#"{
                "instance_name": "",
                "endpoints": [{"address": "http://localhost:50051"}],
                "store_type": "cas",
            }"#,
        )
        .expect(
            "GrpcSpec MUST deserialize when chunked_v2_writes_enabled is absent — \
             the field was removed with the v1 WriteChunked path",
        );
        // The sibling chunked flag (kept) still defaults false.
        assert!(
            !spec.chunked_writes_enabled,
            "chunked_writes_enabled MUST still default to false"
        );
    }

    /// Post-removal contract (b): because `GrpcSpec` is
    /// `#[serde(deny_unknown_fields)]`, a config that STILL carries the removed
    /// `chunked_v2_writes_enabled` key is now a HARD parse error. This is why
    /// the deployed config MUST be stripped of the key BEFORE this build ships
    /// — the parent owns that config migration and deploy ordering.
    #[test]
    fn grpc_spec_rejects_removed_chunked_v2_writes_enabled_key() {
        let result: Result<GrpcSpec, _> = serde_json5::from_str(
            r#"{
                "instance_name": "",
                "endpoints": [{"address": "http://localhost:50051"}],
                "store_type": "cas",
                "chunked_v2_writes_enabled": true,
            }"#,
        );
        let err = result.expect_err(
            "GrpcSpec MUST reject the removed chunked_v2_writes_enabled key — \
             deny_unknown_fields turns a lingering config key into a hard parse \
             error, which is why deployed configs must be stripped of it first",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("chunked_v2_writes_enabled"),
            "reject error MUST name the unknown field chunked_v2_writes_enabled, got: {msg}"
        );
    }
}
