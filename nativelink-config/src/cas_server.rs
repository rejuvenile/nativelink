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

use std::collections::HashMap;
use std::num::NonZeroU32;

use nativelink_error::{Code, Error, ResultExt, make_err};
#[cfg(feature = "dev-schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::schedulers::SchedulerSpec;
use crate::serde_utils::{
    convert_boolean_with_shellexpand, convert_data_size_with_shellexpand,
    convert_duration_with_shellexpand, convert_numeric_with_shellexpand,
    convert_optional_numeric_with_shellexpand, convert_optional_string_with_shellexpand,
    convert_string_with_shellexpand, convert_vec_string_with_shellexpand,
};
use crate::stores::{ClientTlsConfig, ConfigDigestHashFunction, StoreRefName, StoreSpec};

/// Name of the scheduler. This type will be used when referencing a
/// scheduler in the `CasConfig::schedulers`'s map key.
pub type SchedulerRefName = String;

/// Used when the config references `instance_name` in the protocol.
pub type InstanceName = String;

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct WithInstanceName<T> {
    /// Used when the config references `instance_name` in the protocol.
    #[serde(default)]
    pub instance_name: InstanceName,
    #[serde(flatten)]
    pub config: T,
}

impl<T> core::ops::Deref for WithInstanceName<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct NamedConfig<Spec> {
    pub name: String,
    #[serde(flatten)]
    pub spec: Spec,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum HttpCompressionAlgorithm {
    /// No compression.
    #[default]
    None,

    /// Gzip compression.
    Gzip,

    /// Zstandard compression.
    Zstd,
}

/// Note: Compressing data in the cloud rarely has a benefit, since most
/// cloud providers have very high bandwidth backplanes. However, for
/// clients not inside the data center, it might be a good idea to
/// compress data to and from the cloud. This will however come at a high
/// CPU and performance cost. If you are making remote execution share the
/// same CAS/AC servers as client's remote cache, you can create multiple
/// services with different compression settings that are served on
/// different ports. Then configure the non-cloud clients to use one port
/// and cloud-clients to use another.
#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HttpCompressionConfig {
    /// The compression algorithm that the server will use when sending
    /// responses to clients. Enabling this will likely save a lot of
    /// data transfer, but will consume a lot of CPU and add a lot of
    /// latency.
    /// see: <https://github.com/tracemachina/nativelink/issues/109>
    ///
    /// Default: `HttpCompressionAlgorithm::None`
    pub send_compression_algorithm: Option<HttpCompressionAlgorithm>,

    /// The compression algorithm that the server will accept from clients.
    /// The server will broadcast the supported compression algorithms to
    /// clients and the client will choose which compression algorithm to
    /// use. Enabling this will likely save a lot of data transfer, but
    /// will consume a lot of CPU and add a lot of latency.
    /// see: <https://github.com/tracemachina/nativelink/issues/109>
    ///
    /// Default: {no supported compression}
    pub accepted_compression_algorithms: Vec<HttpCompressionAlgorithm>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct AcStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub ac_store: StoreRefName,

    /// Whether the Action Cache store may be written to, this if set to false
    /// it is only possible to read from the Action Cache.
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CasStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,

    /// Optional and experimental: enables the REAPI `SplitBlob`/`SpliceBlob`
    /// RPCs used by content-defined chunking clients (e.g. Bazel's
    /// `--experimental_remote_cache_chunking`). When set, the capabilities
    /// service advertises blob split/splice support and `FastCDC` 2020
    /// parameters for this instance. When `cas_store` is a grpc store the
    /// RPCs are forwarded to the backend (which must support chunking with
    /// matching parameters); otherwise they are served locally.
    ///
    /// See `nativelink-config/examples/chunking_cas.json5` for a complete
    /// configuration example.
    ///
    /// Default: not set — chunking RPCs are rejected, nothing is advertised,
    /// and behavior is identical to when this option did not exist.
    #[serde(default)]
    pub experimental_chunking: Option<CasChunkingConfig>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CasChunkingConfig {
    /// The store name referenced in the `stores` map in the main config used
    /// to persist blob-to-chunks layouts. Keys are the digests of the
    /// original blobs and values are serialized chunk layouts (which do not
    /// hash to those digests), so this store MUST NOT perform content digest
    /// verification and MUST NOT be the same store as `cas_store` — writing
    /// layouts into the CAS would overwrite blob content. Using the same
    /// store name as `cas_store` is rejected at startup.
    ///
    /// Required unless `cas_store` is a grpc store: for proxied instances
    /// the `SplitBlob`/`SpliceBlob` RPCs are forwarded to the backend, which
    /// owns the chunk layouts, and setting an `index_store` is rejected at
    /// startup.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub index_store: Option<StoreRefName>,

    /// The average chunk size in bytes advertised to clients through the
    /// `FastCDC` 2020 capability parameters and used for server-side
    /// chunking in `SplitBlob`. Clients derive the minimum and maximum
    /// chunk sizes from this value (avg / 4 and avg * 4). The value must
    /// be between 1 KiB and 1 MiB.
    ///
    /// Default: 524288 (512 KiB)
    #[serde(default)]
    pub avg_chunk_size_bytes: u64,

    /// Maximum number of chunks accepted in a `SpliceBlob` request or
    /// produced by on-demand chunking in `SplitBlob`. Blobs that would
    /// produce more chunks are served without chunking (`SplitBlob` returns
    /// `NOT_FOUND` and clients fall back to a regular download). This bounds
    /// the size of stored chunk layouts and of `SplitBlobResponse` messages
    /// (roughly 80-140 bytes per chunk). At the default average chunk size
    /// the default cap supports blobs up to ~25 GiB; note that values above
    /// ~50000 may produce responses that exceed default gRPC message size
    /// limits on clients.
    ///
    /// Default: 50000
    #[serde(default)]
    pub max_chunk_count: u64,
}

impl CasChunkingConfig {
    /// Default for `avg_chunk_size_bytes`, the value recommended by the
    /// REAPI spec for `FastCdc2020Params`.
    pub const DEFAULT_AVG_CHUNK_SIZE_BYTES: u64 = 512 * 1024;
    /// Bounds for `avg_chunk_size_bytes` mandated by the REAPI spec for
    /// `FastCdc2020Params`.
    pub const MIN_AVG_CHUNK_SIZE_BYTES: u64 = 1024;
    pub const MAX_AVG_CHUNK_SIZE_BYTES: u64 = 1024 * 1024;
    /// Default for `max_chunk_count`.
    pub const DEFAULT_MAX_CHUNK_COUNT: u64 = 50_000;

    /// Returns `avg_chunk_size_bytes` with the default applied.
    #[must_use]
    pub const fn resolved_avg_chunk_size_bytes(&self) -> u64 {
        if self.avg_chunk_size_bytes == 0 {
            Self::DEFAULT_AVG_CHUNK_SIZE_BYTES
        } else {
            self.avg_chunk_size_bytes
        }
    }

    /// Returns `max_chunk_count` with the default applied.
    #[must_use]
    pub const fn resolved_max_chunk_count(&self) -> u64 {
        if self.max_chunk_count == 0 {
            Self::DEFAULT_MAX_CHUNK_COUNT
        } else {
            self.max_chunk_count
        }
    }

    /// Returns `avg_chunk_size_bytes` with the default applied, or an error
    /// when the configured value is outside the REAPI-mandated bounds.
    pub fn validated_avg_chunk_size_bytes(&self) -> Result<u64, Error> {
        let avg_chunk_size_bytes = self.resolved_avg_chunk_size_bytes();
        if !(Self::MIN_AVG_CHUNK_SIZE_BYTES..=Self::MAX_AVG_CHUNK_SIZE_BYTES)
            .contains(&avg_chunk_size_bytes)
        {
            return Err(make_err!(
                Code::InvalidArgument,
                "'experimental_chunking.avg_chunk_size_bytes' is {avg_chunk_size_bytes}, must be between {} and {}",
                Self::MIN_AVG_CHUNK_SIZE_BYTES,
                Self::MAX_AVG_CHUNK_SIZE_BYTES
            ));
        }
        Ok(avg_chunk_size_bytes)
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CapabilitiesRemoteExecutionConfig {
    /// Scheduler used to configure the capabilities of remote execution.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CapabilitiesConfig {
    /// Configuration for remote execution capabilities.
    /// If not set the capabilities service will inform the client that remote
    /// execution is not supported.
    pub remote_execution: Option<CapabilitiesRemoteExecutionConfig>,

    /// Whether this instance supports Bazel remote cache compression.
    /// When enabled, the capabilities service advertises zstd wire compression
    /// and the ByteStream/CAS services accept REAPI compressed-blobs/zstd data.
    ///
    /// Bazel clients enable this with `--remote_cache_compression`.
    #[serde(
        default,
        skip_serializing_if = "is_default",
        deserialize_with = "convert_boolean_with_shellexpand"
    )]
    pub remote_cache_compression: bool,
}

/// FL-1383 portable rustc-incremental gating (design §13). Default: the whole
/// feature is INERT — no `targetkey` derivation, no extra CAS fetch at
/// ingestion, and therefore no runtime behavior change on the current fleet.
/// Stage-1 (this chunk) uses this only to gate the ingestion-side `targetkey`
/// derivation threaded through `ActionInfo` (§10 first bullet); the worker
/// execution-path pinning (§4/§7) is a later chunk.
#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct PortableIncrConfig {
    /// Master switch for portable rustc-incremental. Default `false` → the
    /// feature is fully inert. This flag is an operational KILL-SWITCH; even
    /// when `true`, an action opts in only by matching `action_output_allowlist`
    /// (fail-closed), so enabling the switch alone changes nothing until an
    /// allowlist entry matches.
    #[serde(default)]
    pub enabled: bool,

    /// Allowlist of `Command.output_paths` PREFIXES that opt an action into
    /// portable rustc-incremental (design §13: allowlisted actions only). An
    /// action is eligible iff `enabled` is `true` AND its primary
    /// (sorted-first) output path starts with one of these prefixes. Empty
    /// (default) → NO action is eligible even when `enabled` is `true`.
    ///
    /// DoS note (design §9): the derivable-`targetkey` write surface is bounded
    /// by the `incr_seed_index` store's own eviction cap; this allowlist
    /// further narrows the surface to explicitly-opted output trees (e.g. the
    /// single stage-1 apple-a14 crate).
    #[serde(default)]
    pub action_output_allowlist: Vec<String>,
}

impl PortableIncrConfig {
    /// Whether an action whose primary (sorted-first) `Command.output_paths`
    /// entry is `primary_output` is allowlisted into portable rustc-incremental.
    /// True iff `primary_output` starts with any allowlist prefix. An empty
    /// allowlist (the default) always returns `false` — fail-closed.
    ///
    /// NOTE: the match is a raw prefix (`starts_with`), NOT a path-segment
    /// boundary — an entry SHOULD end with `/` to avoid over-matching sibling
    /// trees (e.g. `apple_a14` also matches `apple_a14_evil/…`, but
    /// `apple_a14/` does not).
    #[must_use]
    pub fn is_allowlisted(&self, primary_output: &str) -> bool {
        self.action_output_allowlist
            .iter()
            .any(|prefix| primary_output.starts_with(prefix.as_str()))
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExecutionConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    /// This value must be a CAS store reference.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,

    /// The scheduler name referenced in the `schedulers` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,

    /// FL-1383 portable rustc-incremental gating (design §13). Default:
    /// disabled → the feature is inert (see [`PortableIncrConfig`]).
    #[serde(default)]
    pub portable_incr: PortableIncrConfig,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct FetchConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub fetch_store: StoreRefName,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct PushConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub push_store: StoreRefName,

    /// Whether the Action Cache store may be written to, this if set to false
    /// it is only possible to read from the Action Cache.
    #[serde(default)]
    pub read_only: bool,
}

// From https://github.com/serde-rs/serde/issues/818#issuecomment-287438544
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == Default::default()
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ByteStreamConfig {
    /// Name of the store in the "stores" configuration.
    pub cas_store: StoreRefName,

    /// Max number of bytes to send on each grpc stream chunk.
    /// According to <https://github.com/grpc/grpc.github.io/issues/371>
    /// 16KiB - 64KiB is optimal.
    ///
    ///
    /// Default: 64MiB
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_bytes_per_stream: usize,

    /// In the event a client disconnects while uploading a blob, we will hold
    /// the internal stream open for this many seconds before closing it.
    /// This allows clients that disconnect to reconnect and continue uploading
    /// the same blob.
    ///
    /// Default: 10 seconds
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        skip_serializing_if = "is_default",
        alias = "persist_stream_on_disconnect_timeout"
    )]
    pub persist_stream_on_disconnect_timeout_s: usize,

    /// Enable read-while-write streaming: readers can begin consuming
    /// blob data from in-flight uploads before the write has committed
    /// to the store.  When disabled (default), reads always go through
    /// the store and will get NotFound until the write completes.
    ///
    /// Default: false
    #[serde(default)]
    pub streaming_read_while_write: bool,

    /// Maximum bytes buffered per in-flight streaming blob.  Only used
    /// when `streaming_read_while_write` is true.  Older chunks are
    /// evicted when the buffer exceeds this limit (sliding window).
    ///
    /// Default: 64 MiB
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_streaming_blob_buffer_bytes: usize,

    /// Maximum total bytes held across all partial (idle) uploads for this
    /// instance. When exceeded, the oldest idle streams are evicted first.
    /// 0 means unlimited (rely on time-based eviction only).
    ///
    /// Default: 256 MiB
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_partial_write_bytes: u64,
}

// Older bytestream config. All fields are as per the newer docs, but this requires
// the hashed cas_stores v.s. the WithInstanceName approach. This should _not_ be updated
// with newer fields, and eventually dropped
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct OldByteStreamConfig {
    pub cas_stores: HashMap<InstanceName, StoreRefName>,
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_bytes_per_stream: usize,
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_decoding_message_size: usize,
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        skip_serializing_if = "is_default",
        alias = "persist_stream_on_disconnect_timeout"
    )]
    pub persist_stream_on_disconnect_timeout_s: usize,
    #[serde(default)]
    pub streaming_read_while_write: bool,
    #[serde(default)]
    pub max_streaming_blob_buffer_bytes: usize,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct WorkerApiConfig {
    /// The scheduler name referenced in the `schedulers` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,

    /// (#216) Deployment-drift diagnostic. When set and non-empty,
    /// the server rejects any `connect_worker` whose
    /// `ConnectWorkerRequest.build_sha` is not in this list,
    /// returning `Code::FailedPrecondition` with a redeployment
    /// hint. The intent is operator-actionable visibility into
    /// "this worker is running an older binary than the server
    /// expects" — surface drift BEFORE it manifests as silent
    /// runtime errors / reconnect storms.
    ///
    /// Default: `None` — validation DISABLED, every worker
    /// accepted. This is the recommended default; operators opt
    /// in only when they want hard rejection of mismatched
    /// builds during a coordinated rollout.
    ///
    /// **IMPORTANT footgun**: An empty list
    /// (`compatible_build_shas: []`) is treated IDENTICALLY to
    /// `None` — validation disabled, every worker accepted. To
    /// enable validation while admitting legacy (empty-SHA)
    /// workers, the list MUST contain at least the empty string:
    /// `compatible_build_shas: [""]`. To reject every worker, the
    /// list must contain a sentinel value that no worker will
    /// ever produce — a literal empty list does NOT achieve that.
    ///
    /// Each non-empty entry must be a 16-character lowercase hex
    /// prefix of the SHA-256 digest of the worker binary, matching
    /// the format produced by
    /// `nativelink_util::build_sha::build_sha`. Operators rolling
    /// forward typically populate this with the SHA of the
    /// deploy-target binary plus the previous N SHAs to allow
    /// staggered rollouts.
    ///
    /// The empty string ("") reported by legacy workers (or any
    /// worker that fails to read its own binary) is matched
    /// against the list verbatim — to allow legacy workers,
    /// include "" in the list explicitly.
    #[serde(default)]
    pub compatible_build_shas: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct AdminConfig {
    /// Path to register the admin API. If path is "/admin", and your
    /// domain is "example.com", you can reach the endpoint with:
    /// <http://example.com/admin>.
    ///
    /// Default: "/admin"
    #[serde(default)]
    pub path: String,
}

/// Opt-in `/metrics` Prometheus exposition endpoint (#160).
///
/// Mounting `/metrics` is OFF by default. The endpoint is unauthenticated
/// — it shares the listener's auth posture with the gRPC services hosted
/// on the same socket — so exposing it on a public-facing listener (e.g.
/// the Bazel-facing 50051) leaks operational counters (request rates,
/// queue depths, store cardinalities, etc.) to anyone who can reach the
/// port. Today's payload is a small fixed set (chunked-blob drop counters,
/// worker-API state); future `MetricsComponent` additions inherit this
/// exposure surface unconditionally if the gate is implicit.
///
/// The fix is precedent: every operator who wants `/metrics` must
/// explicitly opt in per listener by adding `"metrics": {}` to the
/// `services` block. Place the listener behind a network ACL or, ideally,
/// only enable the flag on a dedicated internal listener.
///
/// Path is currently fixed at `/metrics` (de-facto Prometheus standard).
#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct MetricsConfig {}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HealthConfig {
    /// Path to register the health status check. If path is "/status", and your
    /// domain is "example.com", you can reach the endpoint with:
    /// <http://example.com/status>.
    ///
    /// Default: "/status"
    #[serde(default)]
    pub path: String,

    /// Timeout on health checks. Default: 5s.
    #[serde(default)]
    pub timeout_seconds: u64,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct BepConfig {
    /// The store to publish build events to.
    /// The store name referenced in the `stores` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub store: StoreRefName,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct IdentityHeaderSpec {
    /// The name of the header to look for the identity in.
    /// Default: "x-identity"
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub header_name: Option<String>,

    /// If the header is required to be set or fail the request.
    #[serde(default)]
    pub required: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct OriginEventsPublisherSpec {
    /// The store to publish nativelink events to.
    /// The store name referenced in the `stores` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub store: StoreRefName,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct OriginEventsSpec {
    /// The publisher configuration for origin events.
    pub publisher: OriginEventsPublisherSpec,

    /// The maximum number of events to queue before applying back pressure.
    /// IMPORTANT: Backpressure causes all clients to slow down significantly.
    /// Zero is default.
    ///
    /// Default: 65536 (zero defaults to this)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_event_queue_size: usize,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ServicesConfig {
    /// The Content Addressable Storage (CAS) backend config.
    /// The key is the `instance_name` used in the protocol and the
    /// value is the underlying CAS store config.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub cas: Option<Vec<WithInstanceName<CasStoreConfig>>>,

    /// The Action Cache (AC) backend config.
    /// The key is the `instance_name` used in the protocol and the
    /// value is the underlying AC store config.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub ac: Option<Vec<WithInstanceName<AcStoreConfig>>>,

    /// Capabilities service is required in order to use most of the
    /// bazel protocol. This service is used to provide the supported
    /// features and versions of this bazel GRPC service.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub capabilities: Option<Vec<WithInstanceName<CapabilitiesConfig>>>,

    /// The remote execution service configuration.
    /// NOTE: This service is under development and is currently just a
    /// place holder.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub execution: Option<Vec<WithInstanceName<ExecutionConfig>>>,

    /// This is the service used to stream data to and from the CAS.
    /// Bazel's protocol strongly encourages users to use this streaming
    /// interface to interact with the CAS when the data is large.
    #[serde(default, deserialize_with = "super::backcompat::opt_bytestream")]
    pub bytestream: Option<Vec<WithInstanceName<ByteStreamConfig>>>,

    /// These two are collectively the Remote Asset protocol, but it's
    /// defined as two separate services
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub fetch: Option<Vec<WithInstanceName<FetchConfig>>>,

    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub push: Option<Vec<WithInstanceName<PushConfig>>>,

    /// This is the service used for workers to connect and communicate
    /// through.
    /// NOTE: This service should be served on a different, non-public port.
    /// In other words, `worker_api` configuration should not have any other
    /// services that are served on the same port. Doing so is a security
    /// risk, as workers have a different permission set than a client
    /// that makes the remote execution/cache requests.
    pub worker_api: Option<WorkerApiConfig>,

    /// Experimental - Build Event Protocol (BEP) configuration. This is
    /// the service that will consume build events from the client and
    /// publish them to a store for processing by an external service.
    pub experimental_bep: Option<BepConfig>,

    /// This is the service for any administrative tasks.
    /// It provides a REST API endpoint for administrative purposes.
    pub admin: Option<AdminConfig>,

    /// This is the service for health status check.
    pub health: Option<HealthConfig>,

    /// Opt-in `/metrics` Prometheus exposition (#160). Off by default.
    /// See [`MetricsConfig`] for the security rationale.
    pub metrics: Option<MetricsConfig>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct TlsConfig {
    /// Path to the certificate file.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cert_file: String,

    /// Path to the private key file.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub key_file: String,

    /// Path to the certificate authority for mTLS, if client authentication is
    /// required for this endpoint.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub client_ca_file: Option<String>,

    /// Path to the certificate revocation list for mTLS, if client
    /// authentication is required for this endpoint.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub client_crl_file: Option<String>,
}

/// Advanced Http configurations. These are generally should not be set.
/// For documentation on what each of these do, see the hyper documentation:
/// See: <https://docs.rs/hyper/latest/hyper/server/conn/struct.Http.html>
///
/// Note: All of these default to hyper's default values unless otherwise
/// specified.
#[derive(Deserialize, Serialize, Debug, Default, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HttpServerConfig {
    /// Interval to send keep-alive pings via HTTP2.
    /// Note: This is in seconds.
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub http2_keep_alive_interval: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_pending_accept_reset_streams: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_initial_stream_window_size: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_initial_connection_window_size: Option<u32>,

    #[serde(default)]
    pub experimental_http2_adaptive_window: Option<bool>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_frame_size: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_concurrent_streams: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand",
        alias = "experimental_http2_keep_alive_timeout"
    )]
    pub experimental_http2_keep_alive_timeout_s: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_send_buf_size: Option<u32>,

    #[serde(default)]
    pub experimental_http2_enable_connect_protocol: Option<bool>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_header_list_size: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum ListenerConfig {
    /// Listener for HTTP/HTTPS/HTTP2 sockets.
    Http(HttpListener),

    /// Listener for QUIC/HTTP3 sockets. Requires TLS (mandatory in QUIC).
    /// Use self-signed certs with `skip_cert_verification` for internal networks.
    Http3(Http3Listener),
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Http3Listener {
    /// UDP address to listen on. Example: `0.0.0.0:50051`
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub socket_address: String,

    /// TLS certificate file (PEM). Required for QUIC.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cert_file: String,

    /// TLS private key file (PEM). Required for QUIC.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub key_file: String,

    /// Path to client CA certificate file for mTLS verification.
    /// When set, the QUIC server will require clients to present a
    /// certificate signed by this CA.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub client_ca_file: Option<String>,

    /// Maximum number of bytes to decode on each inbound gRPC message.
    /// Default: 4 MiB
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_decoding_message_size: usize,

    /// Maximum number of bytes to encode on each outbound gRPC message.
    /// Default: 4 MiB
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_encoding_message_size: usize,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HttpListener {
    /// Address to listen on. Example: `127.0.0.1:8080` or `:8080` to listen
    /// to all IPs.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub socket_address: String,

    /// Allow binding `socket_address` before it is assigned locally.
    ///
    /// Default: false
    #[serde(default)]
    pub freebind: bool,

    /// Data transport compression configuration to use for this service.
    #[serde(default)]
    pub compression: HttpCompressionConfig,

    /// Advanced Http server configuration.
    #[serde(default)]
    pub advanced_http: HttpServerConfig,

    /// Maximum number of bytes to decode on each inbound gRPC message.
    /// Default: 4 MiB
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_decoding_message_size: usize,

    /// Maximum number of bytes to encode on each outbound gRPC message.
    /// Default: 4 MiB (matches Bazel's Java gRPC client inbound limit).
    /// Workers with a higher `max_decoding_message_size` should use a
    /// separate listener with this value raised accordingly.
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_encoding_message_size: usize,

    /// Tls Configuration for this server.
    /// If not set, the server will not use TLS.
    ///
    /// Default: None
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// If true, the server will refuse to start unless TLS is configured
    /// on this listener. Use this to prevent accidental plaintext exposure
    /// when TLS is expected (e.g., production deployments).
    ///
    /// When TLS is configured, plaintext connections are already rejected
    /// at the TLS handshake layer -- this option adds a startup-time check
    /// to catch configuration mistakes early.
    ///
    /// Default: false
    #[serde(default)]
    pub require_tls: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ServerConfig {
    /// Name of the server. This is used to help identify the service
    /// for telemetry and logs.
    ///
    /// Default: {index of server in config}
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub name: String,

    /// Configuration
    pub listener: ListenerConfig,

    /// Services to attach to server.
    pub services: Option<ServicesConfig>,

    /// The config related to identifying the client.
    /// Default: {see `IdentityHeaderSpec`}
    #[serde(default)]
    pub experimental_identity_header: IdentityHeaderSpec,

    /// (#58 directive-2: server durability bundle) When `true`, this listener's
    /// Bazel-facing REAPI (CAS / AC / ByteStream / Execution / Capabilities) is
    /// QUIESCED at the very start of graceful shutdown: new requests are
    /// rejected with `Code::Unavailable` (the client retries on a healthy
    /// server) so the shutdown blob-flush + worker-pull can converge to a fixed
    /// point instead of chasing newly-arriving Bazel writes
    /// (operator directive 2026-06-23: "no more bazel REAPI once shutdown
    /// starts"). Existing in-flight requests still drain.
    ///
    /// Set this ONLY on the PUBLIC Bazel-client listener (e.g. `:50051`). It
    /// MUST NOT be set on the worker-facing CAS listeners (`:50071` / `:50072`)
    /// — the shutdown worker-pull phase needs those endpoints OPEN so workers
    /// can push their blobs into the server CAS — nor on the worker_api
    /// scheduler control-plane listener (`:50061`). A boolean on the listener
    /// (rather than an automatic `services.worker_api.is_none()` heuristic) is
    /// load-bearing: in production the worker-facing CAS listeners ALSO have no
    /// `worker_api` service, so an automatic heuristic would wrongly quiesce
    /// them and sever the worker-pull.
    ///
    /// Default: `false` (listener is NOT quiesced at shutdown).
    #[serde(default)]
    pub quiesce_on_shutdown: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum WorkerProperty {
    /// List of static values.
    /// Note: Generally there should only ever be 1 value, but if the platform
    /// property key is `PropertyType::Priority` it may have more than one value.
    #[serde(deserialize_with = "convert_vec_string_with_shellexpand")]
    Values(Vec<String>),

    /// A dynamic configuration. The string will be executed as a command
    /// (not shell) and will be split by "\n" (new line character).
    QueryCmd(String),
}

/// Generic config for an endpoint and associated configs.
#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct EndpointConfig {
    /// URI of the endpoint.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub uri: String,

    /// Timeout in seconds that a request should take.
    /// Default: 5 seconds
    pub timeout: Option<f32>,

    /// The TLS configuration to use to connect to the endpoint.
    pub tls_config: Option<ClientTlsConfig>,

    /// Use QUIC/HTTP3 transport instead of TCP/HTTP2.
    /// Requires the `quic` feature to be enabled at build time.
    /// Default: false
    #[serde(default)]
    pub use_http3: bool,
}

#[derive(Copy, Clone, Deserialize, Serialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum UploadCacheResultsStrategy {
    /// Only upload action results with an exit code of 0.
    #[default]
    SuccessOnly,

    /// Don't upload any action results.
    Never,

    /// Upload all action results that complete.
    Everything,

    /// Only upload action results that fail.
    FailuresOnly,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum EnvironmentSource {
    /// The name of the platform property in the action to get the value from.
    Property(String),

    /// The raw value to set.
    Value(#[serde(deserialize_with = "convert_string_with_shellexpand")] String),

    /// Take the value from the local environment corresponding to the name key
    FromEnvironment,

    /// The max amount of time in milliseconds the command is allowed to run
    /// (requested by the client).
    TimeoutMillis,

    /// A special file path will be provided that can be used to communicate
    /// with the parent process about out-of-band information. This file
    /// will be read after the command has finished executing. Based on the
    /// contents of the file, the behavior of the result may be modified.
    ///
    /// The format of the file contents should be json with the following
    /// schema:
    /// {
    ///   // If set the command will be considered a failure.
    ///   // May be one of the following static strings:
    ///   // "timeout": Will Consider this task to be a timeout.
    ///   "failure": "timeout",
    /// }
    ///
    /// All fields are optional, file does not need to be created and may be
    /// empty.
    SideChannelFile,

    /// A "root" directory for the action. This directory can be used to
    /// store temporary files that are not needed after the action has
    /// completed. This directory will be purged after the action has
    /// completed.
    ///
    /// For example:
    /// If an action writes temporary data to a path but nativelink should
    /// clean up this path after the job has executed, you may create any
    /// directory under the path provided in this variable. A common pattern
    /// would be to use `entrypoint` to set a shell script that reads this
    /// variable, `mkdir $ENV_VAR_NAME/tmp` and `export TMPDIR=$ENV_VAR_NAME/tmp`.
    /// Another example might be to bind-mount the `/tmp` path in a container to
    /// this path in `entrypoint`.
    ActionDirectory,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct UploadActionResultConfig {
    /// Underlying AC store that the worker will use to publish execution results
    /// into. Objects placed in this store should be reachable from the
    /// scheduler/client-cas after they have finished updating.
    /// Default: {No uploading is done}
    pub ac_store: Option<StoreRefName>,

    /// In which situations should the results be published to the `ac_store`,
    /// if set to `SuccessOnly` then only results with an exit code of 0 will be
    /// uploaded, if set to Everything all completed results will be uploaded.
    ///
    /// Default: `SuccessOnly`
    #[serde(default)]
    pub upload_ac_results_strategy: UploadCacheResultsStrategy,

    /// Store to upload historical results to. This should be a CAS store if set.
    ///
    /// Default: {CAS store of parent}
    pub historical_results_store: Option<StoreRefName>,

    /// In which situations should the results be published to the historical CAS.
    /// The historical CAS is where failures are published. These messages conform
    /// to the CAS key-value lookup format and are always a `HistoricalExecuteResponse`
    /// serialized message.
    ///
    /// Default: `FailuresOnly`
    #[serde(default)]
    pub upload_historical_results_strategy: Option<UploadCacheResultsStrategy>,

    /// Template to use for the `ExecuteResponse.message` property. This message
    /// is attached to the response before it is sent to the client. The following
    /// special variables are supported:
    /// - `digest_function`: Digest function used to calculate the action digest.
    /// - `action_digest_hash`: Action digest hash.
    /// - `action_digest_size`: Action digest size.
    /// - `historical_results_hash`: `HistoricalExecuteResponse` digest hash.
    /// - `historical_results_size`: `HistoricalExecuteResponse` digest size.
    ///
    /// A common use case of this is to provide a link to the web page that
    /// contains more useful information for the user.
    ///
    /// An example that is fully compatible with `bb_browser` is:
    /// <https://example.com/my-instance-name-here/blobs/{digest_function}/action/{action_digest_hash}-{action_digest_size}/>
    ///
    /// Default: "" (no message)
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub success_message_template: String,

    /// Same as `success_message_template` but for failure case.
    ///
    /// An example that is fully compatible with `bb_browser` is:
    /// <https://example.com/my-instance-name-here/blobs/{digest_function}/historical_execute_response/{historical_results_hash}-{historical_results_size}/>
    ///
    /// Default: "" (no message)
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub failure_message_template: String,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct LocalWorkerConfig {
    /// Name of the worker. This is give a more friendly name to a worker for logging
    /// and metric publishing. This is also the prefix of the worker id
    /// (ie: "{name}{uuidv6}").
    /// Default: {Index position in the workers list}
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub name: String,

    /// Endpoint which the worker will connect to the scheduler's `WorkerApiService`.
    pub worker_api_endpoint: EndpointConfig,

    /// The maximum time an action is allowed to run. If a task requests for a timeout
    /// longer than this time limit, the task will be rejected. Value in seconds.
    ///
    /// Default: 20 minutes
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        alias = "max_action_timeout"
    )]
    pub max_action_timeout_s: usize,

    /// Maximum time allowed for uploading action results to CAS after execution
    /// completes. If upload takes longer than this, the action fails with
    /// `DeadlineExceeded` and may be retried by the scheduler. Value in seconds.
    ///
    /// Default: 10 minutes
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        alias = "max_upload_timeout"
    )]
    pub max_upload_timeout_s: usize,

    /// Maximum time to wait for action directory cleanup before timing out.
    /// Value in seconds.
    ///
    /// Default: 30 seconds
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub max_cleanup_wait_s: usize,

    /// Maximum backoff duration for exponential backoff when waiting for cleanup.
    /// Value in milliseconds.
    ///
    /// Default: 500 milliseconds
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub max_cleanup_backoff_ms: usize,

    /// Maximum number of inflight tasks this worker can cope with.
    ///
    /// Default: 0 (infinite tasks)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_inflight_tasks: u64,

    /// FL-681: per-call cap on concurrent blob uploads the worker fans
    /// out to the server in `handle_upload_missing_blobs` (the path that
    /// pushes locally-held blobs back to the CAS, e.g. on reconnect
    /// retry or a server-driven `UploadMissingBlobs` backfill). The
    /// worker constructs a `Semaphore` with this many permits per call;
    /// each in-flight upload holds one permit.
    ///
    /// Throughput trade-off: a higher cap lets more blobs upload in
    /// parallel (higher worker→server throughput when many blobs are
    /// missing) at the cost of more concurrent streams and memory/CPU on
    /// both the worker and the receiving server. A lower cap reduces
    /// peak load but serializes the backfill. The cap is PER-CALL: two
    /// overlapping invocations can together run up to `2 × cap` uploads.
    ///
    /// Default: 0 (uses the built-in default of 32).
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_concurrent_uploads: usize,

    /// If timeout is handled in `entrypoint` or another wrapper script.
    /// If set to true `NativeLink` will not honor the timeout the action requested
    /// and instead will always force kill the action after `max_action_timeout`
    /// has been reached. If this is set to false, the smaller value of the action's
    /// timeout and `max_action_timeout` will be used to which `NativeLink` will kill
    /// the action.
    ///
    /// The real timeout can be received via an environment variable set in:
    /// `EnvironmentSource::TimeoutMillis`.
    ///
    /// Example on where this is useful: `entrypoint` launches the action inside
    /// a docker container, but the docker container may need to be downloaded. Thus
    /// the timer should not start until the docker container has started executing
    /// the action. In this case, action will likely be wrapped in another program,
    /// like `timeout` and propagate timeouts via `EnvironmentSource::SideChannelFile`.
    ///
    /// Default: false (`NativeLink` fully handles timeouts)
    #[serde(default)]
    pub timeout_handled_externally: bool,

    /// The command to execute on every execution request. This will be parsed as
    /// a command + arguments (not shell).
    /// Example: "run.sh" and a job with command: "sleep 5" will result in a
    /// command like: "run.sh sleep 5".
    /// Default: {Use the command from the job request}.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub entrypoint: String,

    /// An optional script to run before every action is processed on the worker.
    /// The value should be the full path to the script to execute and will pause
    /// all actions on the worker if it returns an exit code other than 0.
    /// If not set, then the worker will never pause and will continue to accept
    /// jobs according to the scheduler configuration.
    /// This is useful, for example, if the worker should not take any more
    /// actions until there is enough resource available on the machine to
    /// handle them.
    pub experimental_precondition_script: Option<String>,

    /// Underlying CAS store that the worker will use to download CAS artifacts.
    /// This store must be a `FastSlowStore`. The `fast` store must be a
    /// `FileSystemStore` because it will use hardlinks when building out the files
    /// instead of copying the files. The slow store must eventually resolve to the
    /// same store the scheduler/client uses to send job requests.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_fast_slow_store: StoreRefName,

    /// Configuration for uploading action results.
    #[serde(default)]
    pub upload_action_result: UploadActionResultConfig,

    /// The directory work jobs will be executed from. This directory will be fully
    /// managed by the worker service and will be purged on startup.
    /// This directory and the directory referenced in `local_filesystem_store_ref`'s
    /// `stores::FilesystemStore::content_path` must be on the same filesystem.
    /// Hardlinks will be used when placing files that are accessible to the jobs
    /// that are sourced from `local_filesystem_store_ref`'s `content_path`.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub work_directory: String,

    /// Properties of this worker. This configuration will be sent to the scheduler
    /// and used to tell the scheduler to restrict what should be executed on this
    /// worker.
    pub platform_properties: HashMap<String, WorkerProperty>,

    /// An optional mapping of environment names to set for the execution
    /// as well as those specified in the action itself.  If set, will set each
    /// key as an environment variable before executing the job with the value
    /// of the environment variable being the value of the property of the
    /// action being executed of that name or the fixed value.
    pub additional_environment: Option<HashMap<String, EnvironmentSource>>,

    /// Optional directory cache configuration for improving performance by caching
    /// reconstructed input directories and using hardlinks instead of rebuilding
    /// them from CAS for every action.
    /// Default: None (directory cache disabled)
    pub directory_cache: Option<DirectoryCacheConfig>,

    /// If set, the worker will start a CAS + ByteStream gRPC server on
    /// 0.0.0.0:<port> and advertise the endpoint to the scheduler and
    /// other workers for peer-to-peer blob sharing and mirror writes.
    /// When `cas_server_tls` is also set, the server uses TLS and
    /// advertises `grpcs://<hostname>:<port>`; otherwise it uses plain
    /// TCP and advertises `grpc://<hostname>:<port>`.
    /// The hostname is resolved at runtime via gethostname().
    /// Example: 40081
    /// Default: None (no peer CAS server)
    #[serde(default)]
    pub cas_server_port: Option<u16>,

    /// Optional TLS configuration for the worker CAS server started on
    /// `cas_server_port`. When set, the TCP listener uses TLS with the
    /// specified certificate and key. Requires `cas_server_port` to be
    /// set.
    ///
    /// Default: None (plain TCP, no TLS)
    #[serde(default)]
    pub cas_server_tls: Option<TlsConfig>,

    /// How often (in milliseconds) the worker should send a periodic
    /// BlobsAvailable snapshot to the scheduler, reporting which blobs
    /// are in the local CAS cache and their LRU timestamps.
    /// Interval in milliseconds. Default: 0 (uses built-in default of
    /// 500ms).
    ///
    /// Default: 0
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub blobs_available_interval_ms: u64,

    /// Port for the pprof HTTP debug server. When non-zero and the `pprof`
    /// feature is enabled, an HTTP server is started on `0.0.0.0:<port>`
    /// serving CPU profiling endpoints:
    ///   - `GET /debug/pprof/profile` — CPU profile (SVG flamegraph by
    ///     default, protobuf with `?format=pb`)
    ///   - `GET /debug/pprof/flamegraph` — SVG flamegraph directly
    ///
    /// Query parameter `?seconds=N` controls sampling duration (default 10).
    ///
    /// Default: 0 (disabled)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub pprof_port: u16,

    /// #37 Phase 2 (Q5): timeout in seconds for the AC BIS-ack
    /// missing-detection reaper. When the worker publishes an AC
    /// entry, it records the digest in a per-worker observability
    /// map. If the matching `BlobsInStableStorage` ack from the
    /// server does not arrive within this window, an `error!` log
    /// is emitted and the `worker_bis_ack_missing` counter is
    /// incremented. The map entry is then removed (one-shot fire).
    /// **Observability only** — the actual durability pin in
    /// `dispatched_mirror_pins` is managed separately by the
    /// FastSlowStore eviction + worker reconnect paths.
    ///
    /// Default: 0 (uses built-in default of 60s).
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub bis_ack_timeout_secs: u64,

    /// F2 kill-switch: defer worker output-blob uploads off the action-completion
    /// critical path.
    ///
    /// When `false` (default): `inner_upload_results` writes outputs through the
    /// full `FastSlowStore` (fast + slow / remote CAS) synchronously before
    /// `execution_response` is sent. This is the current behavior.
    ///
    /// When `true`: `inner_upload_results` writes outputs to the worker's local
    /// fast store (`FilesystemStore`) only. The remote CAS upload is deferred to
    /// `spawn_upload_to_remote`, which runs AFTER `execution_complete` frees the
    /// worker slot. Completion is gated only on the local disk write (~1ms),
    /// reducing action completion latency by p50≈152ms / p99≈720ms (n=140
    /// actions, worker-01 + worker-02, 2026-06-12; raw data
    /// `/tmp/workerlifecycle-phase-timing.log`).
    ///
    /// DURABILITY REGRESSION: enabling this flag regresses the ≥2-replica
    /// durability invariant to ≥1-replica (worker fast store only) for the
    /// window between `execution_response` and `spawn_upload_to_remote`
    /// completion (p50≈152ms, p99≈720ms). Worst case: permanently single-copy
    /// on the worker if background upload exhausts retries (max 4 retries × up
    /// to 30s backoff ≈ 120s) until `UploadMissingBlobs` recovers. This
    /// tradeoff is intentional and operator-authorized.
    ///
    /// HARD PREREQUISITE: `cas_server_port` MUST be set. Without it,
    /// `BlobsAvailable` is never sent, the server locality map stays empty, and
    /// deferred outputs are unroutable during the upload window (reopens the
    /// 2013977a hole). Worker startup REJECTS the combination
    /// `deferred_output_uploads_enabled = true` + `cas_server_port = None` with
    /// a loud error.
    ///
    /// CLIENT REQUIREMENT: Bazel clients MUST have `--remote_retries ≥ 1` to
    /// recover from the narrow crash-window miss (worker crashes between
    /// `execution_response` and worker reconnect, ~2-10s). Without retries, a
    /// crash in this window causes a client-visible cache miss (action re-runs;
    /// no permanent data loss).
    ///
    /// Safety mechanisms: outputs are pinned in the local FilesystemStore
    /// immediately after write (preventing LRU eviction until BIS-ack, via
    /// `filesystem_store.pin_digest` — the real anti-eviction pin held across
    /// the upload window). The server's locality map is populated via
    /// `BlobsAvailable` (sent before `execution_response`) so Bazel can read
    /// outputs via `WorkerProxyStore` peer-fetch. The H4 pending-registry (#12)
    /// rescues CCS completeness checks during the upload window.
    ///
    /// Prerequisite machinery: #129 BlobsAvailable ordering, #549/#551 pin
    /// budget, `spawn_upload_to_remote` with retries, `UploadMissingBlobs`
    /// backfill, and #12 H4 pending-output-locality-registry must all be live
    /// before enabling.
    ///
    /// See `.claude/audits/f2-deferred-output-uploads-design-2026-06-12.md` for
    /// the full durability analysis and rollout plan.
    ///
    /// Default: false (synchronous behavior; default-OFF kill-switch).
    #[serde(default)]
    pub deferred_output_uploads_enabled: bool,

    /// (#37 re-enable follow-up) Worker memory-pressure admission gate.
    ///
    /// When `false` (default): the gate is DISABLED — the worker never NAKs
    /// a `StartAction` on memory grounds. This matches the fleet state after
    /// incident `5132d6c9` (#64), where the gate was disabled because the
    /// old raw-`free_count` floor false-tripped fleet-wide (3.6k-NAK/min
    /// storm). The gate mechanism has since been corrected to read `available`
    /// (free+inactive+purgeable — the macOS reclaimable pool, ~7-8 GiB on a
    /// healthy 16 GiB worker), but re-enabling requires a per-worker canary
    /// soak to validate the refault EWMA threshold under multi-action build
    /// load before fleet-wide rollout.
    ///
    /// When `true`: the gate NAKs `StartAction` with `ResourceExhausted` when
    /// the available-memory floor (`1 GiB`) is breached OR the re-fault-rate
    /// EWMA crosses `memory_gate_refault_confirm_rate` (default `10000/s`;
    /// thrash corroboration). The gate fails OPEN on a stale/dead sampler and
    /// via the 30s idle fleet fail-open.
    ///
    /// ROLLOUT: deploy the new binary fleet-wide FIRST (this field absent from
    /// all configs → gate disabled). THEN add `memory_gate_enabled: true` to
    /// the canary worker's INDIVIDUALIZED config. Do NOT add this field to the
    /// shared canonical config until all workers run the new binary
    /// (`deny_unknown_fields` causes old binaries to reject configs containing
    /// this field — deploy-ops §13 two-phase sequence).
    ///
    /// Default: false (DISABLED — zero production behavior change).
    #[serde(default)]
    pub memory_gate_enabled: bool,

    /// DEPRECATED (#task-memgate-twosignal) — accepted for config back-compat but
    /// NO LONGER WIRED to any gate. The old refault (decompressions+swapins) NAK
    /// path is REPLACED by the sustained-SWAPIN OOM gate
    /// (`memory_gate_swapin_confirm_rate`); the decompress signal now feeds ONLY
    /// the graded compressor-churn perf scalar, never a NAK. This field is
    /// retained (rather than removed) so a deployed worker.json5 still carrying
    /// it — the live fleet sets `memory_gate_refault_confirm_rate: 4294967295` —
    /// continues to deserialize under `deny_unknown_fields`. Setting it has no
    /// runtime effect. Remove it from configs at leisure.
    ///
    /// `0` is still rejected at deserialization (`NonZeroU32`).
    ///
    /// Default: `10000` (unchanged; absent field behaves identically).
    #[serde(default = "default_memory_gate_refault_confirm_rate")]
    pub memory_gate_refault_confirm_rate: NonZeroU32,

    /// (#task-memgate-twosignal) SWAPIN OOM hard-gate threshold (pages/sec).
    ///
    /// The swapin RATE (Δswapins/Δt) at/above which a sampler tick counts toward
    /// the sustained-swapin OOM trip. `Swapins` = the working set overflowed
    /// RAM+compressor to DISK = OOM-adjacent. Its safety rests on swapin
    /// SEMANTICS (a disk spill is strictly deeper pressure than compression, which
    /// macOS hides) plus the sustained WINDOW below, NOT on the threshold number:
    /// `memory_gate_swapin_confirm_window_ticks` consecutive at/above ticks are
    /// required, so a one-off spike (a lone tick touching ancient swapped pages)
    /// never trips. The `100/s` default was picked against a measured baseline of
    /// 0, but that 0 came from a `--config=dbg` build on an IDLE fleet — the
    /// busy-worker baseline is UNMEASURED, so `nak_swapin` must be watched through
    /// a busy soak after enabling (a legit heavy phase could sustain ≥100/s).
    ///
    /// `0` is rejected at deserialization (`NonZeroU32`): a zero threshold would
    /// count EVERY tick (rate >= 0 always) toward the window → a sustained NAK
    /// once the window fills, regardless of actual swap state (the #64 storm
    /// class, self-inflicted).
    ///
    /// Default: `100` (conservative safe-enable; the free-floor + swapin gate
    /// only NAK when `memory_gate_enabled` is true — default false).
    #[serde(default = "default_memory_gate_swapin_confirm_rate")]
    pub memory_gate_swapin_confirm_rate: NonZeroU32,

    /// (#task-memgate-twosignal) SWAPIN OOM sustained WINDOW (consecutive ticks).
    ///
    /// The number of CONSECUTIVE sampler ticks the swapin rate must stay at/above
    /// `memory_gate_swapin_confirm_rate` before the OOM boolean trips. At the
    /// 100 ms sampler cadence, the default `10` ticks ≈ 1 s of sustained disk
    /// spill — enough to reject a one-off spike (a lone sub-threshold tick resets
    /// the consecutive count to 0) while leading a real OOM by ~1 s.
    ///
    /// `0` is rejected at deserialization (`NonZeroU32`): a zero window = no
    /// sustained requirement = trip on the FIRST tick above threshold, which
    /// defeats the spike-rejection the window exists for.
    ///
    /// Default: `10` (≈1 s at the 100 ms cadence).
    #[serde(default = "default_memory_gate_swapin_confirm_window_ticks")]
    pub memory_gate_swapin_confirm_window_ticks: NonZeroU32,

    /// Whether to use namespaces to isolate the execution.  This is only available
    /// on Linux.  It is highly recommended as it avoids a number of issues with
    /// zombie processes and also provides additional hermeticity.  If explicitly set
    /// to true and it is not supported the worker will exit with an error.
    ///
    /// Note: this will fail for non-privileged Dockerised workers, as workers in
    /// Docker don't have permissions to make a new user namespace. Privileged
    /// containers can do this.
    ///
    /// Default: False.
    pub use_namespaces: Option<bool>,

    /// Whether to use a mount namespace to isolate the worker root.  This is only
    /// available on Linux and when `use_namespaces` is true.  It is highly recommended
    /// provides additional hermeticity.  If explicitly set to true and it is not
    /// supported or `use_namespaces` is not set to true the worker will exit with an
    /// error.
    /// Default: False.
    pub use_mount_namespace: Option<bool>,

    /// FL-1383 portable rustc-incremental — WORKER-side gate (design §9/§12).
    ///
    /// This is the WORKER half of the split-brain-safe gate: the ingestion
    /// half is `ExecutionConfig.portable_incr` (chunk 1). Both halves reuse the
    /// SAME [`PortableIncrConfig`] policy struct so the `enabled` +
    /// `action_output_allowlist` semantics are configured identically on both
    /// sides — review-pair-a (chunk-1 MINOR) flagged that the two gates must
    /// enable TOGETHER or the feature is split-brain.
    ///
    /// Default (absent): disabled → the whole worker-side feature is INERT (no
    /// FIXED_PREFIX provisioning, no §12 startup asserts, no execution-path
    /// change — byte-identical worker startup).
    ///
    /// NOTE (chunk 2a): even when `enabled`, this chunk ONLY provisions
    /// FIXED_PREFIX + runs the §12 asserts + gates. The execution-path rewire
    /// (`make_action_directory` → `<FIXED_PREFIX>/<targetkey>`, chdir, wipe,
    /// lease) is chunk 2b — TODO(#FL-1383).
    #[serde(default)]
    pub portable_incr: PortableIncrConfig,

    /// FL-1383 (design §9) — the machine-LOCAL `<FIXED_PREFIX>` root under which
    /// the worker materializes `-incr` seed dirs at a byte-identical absolute
    /// path. Provisioned at worker startup (owned by the worker uid, mode 0755,
    /// on the EXECROOT volume) when `portable_incr.enabled` is `true`. This is
    /// worker deployment TOPOLOGY (like [`work_directory`], its sibling on the
    /// same physical volume), NOT shared policy — hence a `LocalWorkerConfig`
    /// field, not part of the shared [`PortableIncrConfig`].
    ///
    /// Default (absent): `None`. When `portable_incr.enabled` is `true` but this
    /// is unset, the §12 asserts fail-loud and the feature is left DISABLED (the
    /// worker does NOT panic).
    ///
    /// [`work_directory`]: LocalWorkerConfig::work_directory
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub portable_incr_fixed_prefix: Option<String>,

    /// FL-1383 (design §2/§12) — the absolute rustc sysroot path asserted
    /// byte-identical at boot (§12 assert (b)). The fleet invariant is that the
    /// sysroot resolves to the SAME absolute real path on every machine
    /// (`/Users/user/.rustup`, execroot-relative) so rustc's realpath does not
    /// diverge cross-machine. The worker-local check that stands in for the
    /// fleet invariant: the path is absolute AND canonicalizes to ITSELF (it is
    /// NOT reached via an `output_base` symlink). Worker host-provisioning
    /// topology → a `LocalWorkerConfig` field.
    ///
    /// Default (absent): `None`. When `portable_incr.enabled` is `true` but this
    /// is unset, the §12 asserts fail-loud and the feature is left DISABLED.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub portable_incr_sysroot_path: Option<String>,

    /// FL-1383 (design §6.1/§6.3) — the name of the store instance that backs
    /// the fleet-shared `incr_seed_index`: an AC-shaped MUTABLE index keyed by
    /// `hash(targetkey)` whose value references the current `-incr` seed Tree in
    /// CAS. Per design §6.1 this MUST be a DISTINCT instance from the action-cache
    /// (`instance_name = "incr_seed_index"`) sitting BEHIND a
    /// `completeness_checking` store so a dangling index entry (its `-incr`
    /// content evicted) resolves to CAS-NotFound → cold, never a
    /// live-digest-to-nowhere. See `examples/portable_incr_seed_index.json5`.
    ///
    /// The worker uses this store out-of-band (§6.3): it FETCHES the seed before
    /// rustc and PUBLISHES a new entry after a successful allowlisted build. This
    /// is worker deployment topology (which store instance), NOT shared policy —
    /// hence a `LocalWorkerConfig` field, not part of [`PortableIncrConfig`].
    ///
    /// Default (absent): `None`. When unset the worker performs NO seed fetch or
    /// publish — the seed path is INERT even if `portable_incr.enabled` is `true`
    /// (a cold-but-correct fallback). Both must be configured for the seed path
    /// to do work.
    #[serde(default)]
    pub portable_incr_seed_index_store: Option<StoreRefName>,
}

impl Default for LocalWorkerConfig {
    fn default() -> Self {
        Self {
            name: Default::default(),
            worker_api_endpoint: Default::default(),
            max_action_timeout_s: Default::default(),
            max_upload_timeout_s: Default::default(),
            max_cleanup_wait_s: Default::default(),
            max_cleanup_backoff_ms: Default::default(),
            max_inflight_tasks: Default::default(),
            max_concurrent_uploads: Default::default(),
            timeout_handled_externally: Default::default(),
            entrypoint: Default::default(),
            experimental_precondition_script: Default::default(),
            cas_fast_slow_store: Default::default(),
            upload_action_result: Default::default(),
            work_directory: Default::default(),
            platform_properties: Default::default(),
            additional_environment: Default::default(),
            directory_cache: Default::default(),
            cas_server_port: Default::default(),
            cas_server_tls: Default::default(),
            blobs_available_interval_ms: Default::default(),
            pprof_port: Default::default(),
            bis_ack_timeout_secs: Default::default(),
            deferred_output_uploads_enabled: Default::default(),
            memory_gate_enabled: Default::default(),
            // NonZeroU32 has no Default; use the serde defaults. These match what
            // serde produces for an absent field (0 is rejected as an operator value).
            memory_gate_refault_confirm_rate: default_memory_gate_refault_confirm_rate(),
            memory_gate_swapin_confirm_rate: default_memory_gate_swapin_confirm_rate(),
            memory_gate_swapin_confirm_window_ticks:
                default_memory_gate_swapin_confirm_window_ticks(),
            use_namespaces: Default::default(),
            use_mount_namespace: Default::default(),
            portable_incr: Default::default(),
            portable_incr_fixed_prefix: Default::default(),
            portable_incr_sysroot_path: Default::default(),
            portable_incr_seed_index_store: Default::default(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct DirectoryCacheConfig {
    /// Maximum number of cached directories.
    /// Default: 1000
    #[serde(default = "default_directory_cache_max_entries")]
    pub max_entries: usize,

    /// Maximum total size in bytes for all cached directories (0 = unlimited).
    /// Default: 10737418240 (10 GB)
    #[serde(
        default = "default_directory_cache_max_size_bytes",
        deserialize_with = "convert_data_size_with_shellexpand"
    )]
    pub max_size_bytes: u64,

    /// Base directory for cache storage. This directory will be managed by
    /// the worker and should be on the same filesystem as `work_directory`.
    /// Default: `{work_directory}/../directory_cache`
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub cache_root: String,

    /// When enabled, the action's work directory is symlinked directly to the
    /// cache directory instead of hardlinking/cloning files into it.
    /// This eliminates all copy/hardlink overhead but requires that actions
    /// do not modify their input tree (Bazel actions satisfy this).
    ///
    /// Subtree reuse is preserved: when a new root shares subtrees with
    /// already-cached roots, the new cache entry uses symlinks to point at
    /// the cached subtree directories.
    ///
    /// The existing `prepare_output_directories` logic handles read-only
    /// directories by replacing blocking symlinks with writable shallow-copy
    /// directories that preserve access to original content.
    ///
    /// Default: false. Direct-use mode is currently incompatible with
    /// Bazel's `cargo_build_script_runner` (it writes to the input root,
    /// creating runfiles symlinks + OUT_DIR files) which surfaces as
    /// EEXIST/EPERM/ENOENT/ELOOP. Reverted in `f1f8267f` (2026-03-11);
    /// re-enable once a copy-on-write approach lands.
    #[serde(default = "default_direct_use_mode")]
    pub direct_use_mode: bool,
}

fn default_memory_gate_refault_confirm_rate() -> NonZeroU32 {
    // SAFETY: 10_000 != 0.
    NonZeroU32::new(10_000).unwrap()
}

/// (#task-memgate-twosignal) Default SWAPIN OOM threshold: 100 pages/s. Safety
/// comes from swapin semantics (disk spill is strictly deeper than compression)
/// plus the sustained window, not from this number: the measured `0` baseline was
/// a dbg build on an idle fleet, so the busy-worker baseline is UNMEASURED — watch
/// `nak_swapin` post-enable. Numeric-constant rule: this literal is the
/// authoritative ship default.
fn default_memory_gate_swapin_confirm_rate() -> NonZeroU32 {
    // SAFETY: 100 != 0.
    NonZeroU32::new(100).unwrap()
}

/// (#task-memgate-twosignal) Default SWAPIN sustained window: 10 consecutive
/// ticks ≈ 1 s at the 100 ms sampler cadence. Numeric-constant rule: this literal
/// is the authoritative ship default.
fn default_memory_gate_swapin_confirm_window_ticks() -> NonZeroU32 {
    // SAFETY: 10 != 0.
    NonZeroU32::new(10).unwrap()
}

const fn default_direct_use_mode() -> bool {
    false
}

const fn default_directory_cache_max_entries() -> usize {
    1000
}

const fn default_directory_cache_max_size_bytes() -> u64 {
    10 * 1024 * 1024 * 1024 // 10 GB
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum WorkerConfig {
    /// A worker type that executes jobs locally on this machine.
    Local(LocalWorkerConfig),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct GlobalConfig {
    /// Maximum number of open files that can be opened at one time.
    /// This value is not strictly enforced, it is a best effort. Some internal libraries
    /// open files or read metadata from a files which do not obey this limit, however
    /// the vast majority of cases will have this limit be honored.
    /// This value must be larger than `ulimit -n` to have any effect.
    /// Any network open file descriptors is not counted in this limit, but is counted
    /// in the kernel limit. It is a good idea to set a very large `ulimit -n`.
    /// Note: This value must be greater than 10.
    ///
    /// Default: 24576 (= 24 * 1024)
    #[serde(deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_open_files: usize,

    /// Default hash function to use while uploading blobs to the CAS when not set
    /// by client.
    ///
    /// Default: `ConfigDigestHashFunction::sha256`
    pub default_digest_hash_function: Option<ConfigDigestHashFunction>,

    /// Default digest size to use for health check when running
    /// diagnostics checks. Health checks are expected to use this
    /// size for filling a buffer that is used for creation of
    /// digest.
    ///
    /// Default: 1024*1024 (1MiB)
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub default_digest_size_health_check: usize,

    /// Port to bind the pprof CPU profiling HTTP server on.
    /// Endpoints: `/debug/pprof/profile` (SVG or protobuf) and
    /// `/debug/pprof/flamegraph` (SVG).
    ///
    /// Query parameter `?seconds=N` controls sampling duration (default 10).
    ///
    /// Requires the `pprof` feature to be enabled at compile time.
    ///
    /// Default: 0 (disabled)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub pprof_port: u16,

    /// Disable OpenTelemetry OTLP exporters (logs, traces, metrics).
    /// When true (the default), only stdout logging is active.
    /// Set to false to enable OTLP export to a collector.
    ///
    /// Default: true (OTLP disabled)
    #[serde(default = "default_disable_otlp")]
    pub disable_otlp: bool,

    /// Use non-blocking async stdout writer for logging.
    /// When true (the default), log writes don't block tokio threads.
    /// Logs may be dropped under extreme load (>128K buffered lines).
    ///
    /// Default: true
    #[serde(default = "default_nonblocking_log")]
    pub nonblocking_log: bool,

    /// Path to the CA certificate file used by the server when connecting
    /// to worker CAS endpoints (port 40081) for mirror writes and peer
    /// blob sharing. When set, the server uses TLS (`grpcs://`) to
    /// connect to worker CAS servers. When not set, connections are
    /// plain TCP (`grpc://`).
    ///
    /// Default: None (plain TCP)
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub worker_proxy_tls_ca_file: Option<String>,

    /// Path to client certificate for mTLS when connecting to worker
    /// CAS endpoints. Requires `worker_proxy_tls_ca_file` to be set.
    ///
    /// Default: None
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub worker_proxy_tls_cert_file: Option<String>,

    /// Path to client private key for mTLS when connecting to worker
    /// CAS endpoints. Requires `worker_proxy_tls_cert_file` to be set.
    ///
    /// Default: None
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub worker_proxy_tls_key_file: Option<String>,

    /// #212 Phase 2.7 Bazel-facing internal-chunking kill-switch.
    /// Set true to enable. Production-flipped 2026-05-02 per user
    /// sign-off.
    ///
    /// Lives on `GlobalConfig` (rather than per-store) because the
    /// underlying gate is a process-wide `AtomicBool` in
    /// `nativelink_store::chunked` consulted by every `FastSlowStore`
    /// that has a `BazelChunkedDispatcher` installed. A per-store
    /// knob would be misleading.
    ///
    /// When true AND the `chunked_fast_slow` feature is compiled in
    /// AND a `BazelChunkedDispatcher` has been installed AND
    /// `digest.size_bytes() >= CHUNK_SIZE`, `FastSlowStore::update`
    /// internally chunks the in-order ByteStream into 1 MiB pieces
    /// and dispatches them through the per-blob `ChunkedDriver`
    /// machinery (β async-commit semantics). Smaller blobs and the
    /// default-OFF case continue to use the legacy single-stream
    /// path (fast tier in line + background `tokio::spawn` for slow
    /// tier).
    ///
    /// Per CLAUDE.md `feedback_async_to_sync_requires_explicit_signoff`,
    /// this is an architectural change and was explicitly signed off
    /// on 2026-05-02.
    ///
    /// Default: false (legacy single-stream path)
    #[serde(default)]
    pub bazel_facing_internal_chunking_enabled: bool,

    /// #168 SmallBlobDispatcher master feature flag. When `true`, the
    /// server's bytestream / cas / ac producer hooks fan out small
    /// (`<= SMALL_BLOB_THRESHOLD = 16 KiB`) CAS+AC blobs to every
    /// connected worker via the `SmallBlobDispatcher`. Workers receive
    /// the bytes via `BatchWriteSmallBlobs` push and insert directly
    /// into their local mirror map (no server callback). When `false`,
    /// the dispatcher's `enqueue` (and the new sync
    /// `schedule_dispatch_to_all_workers`) is an inert no-op even when
    /// the per-store `EphemeralServerSidePin` sets are registered.
    ///
    /// Wiring lands inert (default false) in this commit; canary
    /// requires an explicit JSON5 flag flip to `true`. Per CLAUDE.md
    /// `feedback_async_to_sync_requires_explicit_signoff`, the dispatch
    /// fan-out itself is fully async (`tokio::spawn`-based, see
    /// `SmallBlobDispatcher::schedule_dispatch_to_all_workers`) so
    /// flipping this on does NOT introduce sync coupling between Bazel
    /// ack and the worker fan-out.
    ///
    /// Default: false
    #[serde(default)]
    pub small_blob_mirror_enabled: bool,

    /// #494-v3 Phase 2 master feature flag for the bidi
    /// `WriteChunkedV2` multi-writer race path. When `true`, the
    /// per-listener `CasExtensionsServer` is registered with
    /// `ChunkedCasExtensionsAdapter` (which routes both v1 and v2
    /// RPCs); when `false`, the adapter is replaced by the bare
    /// `ChunkedWriteHandler` whose `write_chunked_v2` returns
    /// `Code::Unimplemented`.
    ///
    /// **Default ON** since 2026-05-15 (#497 cross-version coordination
    /// landed: Bazel ByteStream v1 + worker WriteChunked v1 + worker
    /// WriteChunkedV2 all coordinate through a single `single_stream_owner`
    /// gate on the per-digest race-state, so the v2 path is safe to
    /// enable concurrently with v1 paths). The v2 handler itself is
    /// feature-gated behind the `chunked_fast_slow` Cargo feature, so
    /// flipping this flag has no effect when that feature is compiled out.
    ///
    /// **Operator guidance:** production wiring at
    /// `bin/nativelink.rs:893-894` already installs the v2 BIS /
    /// failed-commit sinks via `with_v2_stable_digests_sink` /
    /// `with_v2_failed_commit_sink`. Operators may set
    /// `chunked_v2_enabled = false` to roll back to v2-disabled if
    /// production data surfaces a regression; the v1 paths continue to
    /// work unchanged.
    ///
    /// Default: true
    #[serde(default = "default_chunked_v2_enabled")]
    pub chunked_v2_enabled: bool,
}

/// #494-v3 Phase 2 + #497 Option 1: default for
/// `GlobalConfig.chunked_v2_enabled`. Returns `true` since 2026-05-15
/// after the cross-version coordination gate landed (Bazel/v1
/// WriteChunked/v2 all coordinate through `single_stream_owner` on
/// the per-digest race-state).
fn default_chunked_v2_enabled() -> bool {
    true
}

fn default_disable_otlp() -> bool {
    true
}

fn default_nonblocking_log() -> bool {
    true
}

pub type StoreConfig = NamedConfig<StoreSpec>;
pub type SchedulerConfig = NamedConfig<SchedulerSpec>;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CasConfig {
    /// List of stores available to use in this config.
    /// The keys can be used in other configs when needing to reference a store.
    pub stores: Vec<StoreConfig>,

    /// Worker configurations used to execute jobs.
    pub workers: Option<Vec<WorkerConfig>>,

    /// List of schedulers available to use in this config.
    /// The keys can be used in other configs when needing to reference a
    /// scheduler.
    pub schedulers: Option<Vec<SchedulerConfig>>,

    /// Servers to setup for this process.
    pub servers: Vec<ServerConfig>,

    /// Experimental - Origin events configuration. This is the service that will
    /// collect and publish nativelink events to a store for processing by an
    /// external service.
    pub experimental_origin_events: Option<OriginEventsSpec>,

    /// Any global configurations that apply to all modules live here.
    pub global: Option<GlobalConfig>,
}

impl CasConfig {
    /// # Errors
    ///
    /// Will return `Err` if we can't load the file.
    pub fn try_from_json5_file(config_file: &str) -> Result<Self, Error> {
        let json_contents = std::fs::read_to_string(config_file)
            .err_tip(|| format!("Could not open config file {config_file}"))?;
        let config: Self = serde_json5::from_str(&json_contents)?;
        for server in &config.servers {
            if let Some(services) = &server.services {
                Self::check_store_conflict(services)?;
            }
        }
        Ok(config)
    }

    fn check_store_conflict(services: &ServicesConfig) -> Result<(), Error> {
        if let Some(cas_config) = &services.cas
            && let Some(ac_config) = &services.ac
        {
            // Create a hashmap from the CAS configuration for quick lookup
            let cas_store_map: HashMap<_, _> = cas_config
                .iter()
                .map(|with_instance_name| {
                    (
                        &with_instance_name.instance_name,
                        &with_instance_name.cas_store,
                    )
                })
                .collect();

            for with_instance_name in ac_config {
                if let Some(cas_store) = cas_store_map.get(&with_instance_name.instance_name)
                    && cas_store == &&with_instance_name.ac_store
                {
                    return Err(make_err!(
                        Code::InvalidArgument,
                        "CAS and AC use the same store '{}' in the config",
                        cas_store
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_config_remote_cache_compression_deserializes_true() {
        let config: CapabilitiesConfig =
            serde_json5::from_str(r#"{"remote_cache_compression": true}"#).unwrap();

        assert!(config.remote_cache_compression);
    }

    #[test]
    fn capabilities_config_remote_cache_compression_defaults_false() {
        let config: CapabilitiesConfig = serde_json5::from_str("{}").unwrap();

        assert!(!config.remote_cache_compression);
    }

    // FL-1383 (§13): the portable-incr allowlist is fail-closed — an empty
    // allowlist matches nothing, so the master switch alone opts nothing in.
    #[test]
    fn portable_incr_empty_allowlist_matches_nothing() {
        let config = PortableIncrConfig {
            enabled: true,
            action_output_allowlist: vec![],
        };
        assert!(
            // path-mapped primary (StrippingPathMapper: mnemonic -> literal "cfg")
            !config.is_allowlisted("bazel-out/cfg/bin/pkg/libfoo-abc123.rlib"),
            "empty allowlist must match nothing (fail-closed)"
        );
    }

    #[test]
    fn portable_incr_allowlist_prefix_match() {
        let config = PortableIncrConfig {
            enabled: true,
            // path-mapped prefix: StrippingPathMapper replaces the mnemonic with "cfg".
            action_output_allowlist: vec![
                "bazel-out/cfg/bin/third_party/rust/apple_a14/".to_string(),
            ],
        };
        assert!(
            config.is_allowlisted("bazel-out/cfg/bin/third_party/rust/apple_a14/libfoo-abc123.rlib"),
            "a matching prefix must allowlist the primary output"
        );
        assert!(
            !config.is_allowlisted("bazel-out/cfg/bin/pkg/other-xyz.rlib"),
            "a non-matching prefix must not allowlist"
        );
    }

    // FL-1383: ExecutionConfig without `portable_incr` deserializes to the inert
    // default (disabled, empty allowlist) — chunk 1 lands dark-but-wired.
    #[test]
    fn execution_config_portable_incr_defaults_inert() {
        let config: ExecutionConfig =
            serde_json5::from_str(r#"{"cas_store": "cas", "scheduler": "sched"}"#).unwrap();
        assert!(
            !config.portable_incr.enabled,
            "portable_incr must default to disabled"
        );
        assert!(
            config.portable_incr.action_output_allowlist.is_empty(),
            "portable_incr allowlist must default to empty"
        );
    }

    // FL-1383 chunk 2a: LocalWorkerConfig without the portable_incr worker gate
    // deserializes to the fully-inert default — disabled gate, no FIXED_PREFIX,
    // no sysroot path. This is the config-level half of the flag-off inertness
    // contract (the provisioning-level half lives in
    // `nativelink-worker/tests/portable_incr_test.rs`).
    #[test]
    fn local_worker_config_portable_incr_defaults_inert() {
        let config: LocalWorkerConfig = serde_json5::from_str(
            r#"{"worker_api_endpoint": {"uri": "grpc://s"}, "cas_fast_slow_store": "cas", "work_directory": "/w", "platform_properties": {}}"#,
        )
        .unwrap();
        assert!(
            !config.portable_incr.enabled,
            "worker portable_incr gate must default to disabled"
        );
        assert!(
            config.portable_incr.action_output_allowlist.is_empty(),
            "worker portable_incr allowlist must default to empty"
        );
        assert!(
            config.portable_incr_fixed_prefix.is_none(),
            "portable_incr_fixed_prefix must default to None (inert)"
        );
        assert!(
            config.portable_incr_sysroot_path.is_none(),
            "portable_incr_sysroot_path must default to None (inert)"
        );
    }
}
