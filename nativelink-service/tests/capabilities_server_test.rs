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

use std::collections::HashMap;
use std::sync::Arc;

use futures::join;
use nativelink_config::cas_server::{
    CapabilitiesConfig, CapabilitiesRemoteExecutionConfig, CasChunkingConfig, CasStoreConfig,
    WithInstanceName,
};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_server::Capabilities;
use nativelink_proto::build::bazel::remote::execution::v2::digest_function::Value as DigestFunction;
use nativelink_proto::build::bazel::remote::execution::v2::{
    FastCdc2020Params, GetCapabilitiesRequest, ServerCapabilities,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::mock_scheduler::MockActionScheduler;
use nativelink_service::capabilities_server::CapabilitiesServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
use nativelink_util::digest_hasher::{
    DigestHasherFunc, default_digest_hasher_func, set_default_digest_hasher_func,
};
use pretty_assertions::assert_eq;
use tonic::Request;

const COMPRESSION_INSTANCE: &str = "compression";
const EXECUTION_INSTANCE: &str = "execution";
const SCHEDULER_NAME: &str = "main_scheduler";

fn capabilities_config(
    instance_name: &str,
    remote_cache_compression: bool,
    remote_execution: bool,
) -> WithInstanceName<CapabilitiesConfig> {
    WithInstanceName {
        instance_name: instance_name.to_string(),
        config: CapabilitiesConfig {
            remote_execution: remote_execution.then(|| CapabilitiesRemoteExecutionConfig {
                scheduler: SCHEDULER_NAME.to_string(),
            }),
            remote_cache_compression,
        },
    }
}

async fn get_capabilities(
    server: &CapabilitiesServer,
    instance_name: &str,
) -> Result<ServerCapabilities, tonic::Status> {
    server
        .get_capabilities(Request::new(GetCapabilitiesRequest {
            instance_name: instance_name.to_string(),
        }))
        .await
        .map(tonic::Response::into_inner)
}

/// Pin the process-global default digest function to BLAKE3, exactly as the
/// deployed `global.default_digest_hash_function` does on buildcache
/// (`prod-server.json5:748`) and on every worker (`worker.json5:368`).
///
/// `DEFAULT_DIGEST_HASHER_FUNC` is a `OnceLock` whose `get_or_init` fallback is
/// SHA-256 (`digest_hasher.rs:52-54`), so this MUST run before anything in the
/// binary calls `default_digest_hasher_func()`. The post-condition assert is
/// load-bearing: without it, a binary that had already pinned SHA-256 would
/// leave these tests asserting the SHA-256-shaped advertisement and still
/// reporting green.
///
/// **Called FIRST in EVERY test in this file, not only the two that assert on
/// `digest_functions`.** libtest runs a file's tests concurrently in one
/// process, and `get_capabilities` reaches `default_digest_hasher_func()` on
/// the CACHE path of every call — so any test in this binary can be the one
/// that loses the race and latches the `OnceLock` to the SHA-256 fallback.
/// Measured on this file: with the pin in only the two new tests, the target
/// failed **11 of 40** unfiltered multi-threaded runs (`--test-threads=1` and
/// `--filter` both hid it); with the pin in all seven, **0 of 40**. Same
/// convention and same reason as `server_digest_func_proving_chunked_test.rs`.
fn pin_production_blake3_default() {
    drop(set_default_digest_hasher_func(DigestHasherFunc::Blake3));
    assert_eq!(
        default_digest_hasher_func(),
        DigestHasherFunc::Blake3,
        "test harness must run with the deployed blake3 default; otherwise these \
         tests pin whatever function the harness happened to fall back to rather \
         than the one production actually accepts"
    );
}

#[nativelink_test]
async fn compression_advertisement_is_forced_off_until_handlers_land()
-> Result<(), Box<dyn core::error::Error>> {
    pin_production_blake3_default();

    // v1.6.1 merge (#2527 wire-compression): the CAS server does NOT serve
    // compressed uploads (our FL-688 bytestream path has no compressed-upload
    // handling), so `CapabilitiesServer` intentionally advertises NO
    // compressors regardless of the `remote_cache_compression` config knob,
    // to avoid advertising a capability the server can't honor. When the
    // #2527 handler pass lands, restore the Zstd advertisement + this test.
    let configs = [capabilities_config(COMPRESSION_INSTANCE, true, false)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);
    let server = CapabilitiesServer::new(
        &configs,
        &HashMap::new(),
        &remote_cache_compression_instances,
        &[],
    )
    .await?;

    let response = get_capabilities(&server, COMPRESSION_INSTANCE).await?;
    let cache_capabilities = response
        .cache_capabilities
        .expect("cache capabilities should be set");

    assert!(
        cache_capabilities.supported_compressors.is_empty(),
        "compression advertisement must stay forced-off until #2527 handlers land"
    );
    assert!(
        cache_capabilities
            .supported_batch_update_compressors
            .is_empty(),
        "batch-update compression advertisement must stay forced-off until #2527 handlers land"
    );
    Ok(())
}

#[nativelink_test]
async fn compression_only_instance_does_not_advertise_execution_capabilities()
-> Result<(), Box<dyn core::error::Error>> {
    pin_production_blake3_default();

    let configs = [capabilities_config(COMPRESSION_INSTANCE, true, false)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);
    let server = CapabilitiesServer::new(
        &configs,
        &HashMap::new(),
        &remote_cache_compression_instances,
        &[],
    )
    .await?;

    let response = get_capabilities(&server, COMPRESSION_INSTANCE).await?;

    assert!(
        response.execution_capabilities.is_none(),
        "compression-only instance should not advertise execution capabilities"
    );
    Ok(())
}

#[nativelink_test]
async fn remote_execution_instance_advertises_execution_capabilities_and_node_properties()
-> Result<(), Box<dyn core::error::Error>> {
    pin_production_blake3_default();

    let configs = [capabilities_config(EXECUTION_INSTANCE, false, true)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);

    let mock_scheduler = Arc::new(MockActionScheduler::new());
    let mut scheduler_map: HashMap<String, Arc<dyn KnownPlatformPropertyProvider>> = HashMap::new();
    scheduler_map.insert(SCHEDULER_NAME.to_string(), mock_scheduler.clone());

    let expected_properties = vec!["cpu".to_string(), "os".to_string()];
    let new_server_fut = CapabilitiesServer::new(
        &configs,
        &scheduler_map,
        &remote_cache_compression_instances,
        &[],
    );
    let expected_scheduler_call_fut =
        mock_scheduler.expect_get_known_properties(Ok(expected_properties.clone()));
    let (server, scheduler_instance_name) = join!(new_server_fut, expected_scheduler_call_fut);
    assert_eq!(scheduler_instance_name, EXECUTION_INSTANCE);
    let server = server?;

    let response = get_capabilities(&server, EXECUTION_INSTANCE).await?;
    let execution_capabilities = response
        .execution_capabilities
        .expect("execution capabilities should be set");

    assert!(execution_capabilities.exec_enabled);
    assert_eq!(
        execution_capabilities.supported_node_properties,
        expected_properties
    );
    Ok(())
}

// `digest_functions` is an INVITATION, not a wish list: a conforming REAPI
// client reads it, picks an entry, and keys every blob of a build under that
// function. It should name exactly the configured
// `global.default_digest_hash_function` -- the one function the rest of this
// fleet is keyed under.
//
// It is NOT an admission control, and these tests do not claim it is. The
// server still ACCEPTS a non-advertised function two ways: an explicit
// `sha256/` segment is hashed with the CLIENT's function
// (`bytestream_server.rs:3988-3995` installs it in the context,
// `verify_store.rs:880` reads it back), and an omitted or wrong label is
// still admitted whenever the declared digest reproduces under any
// `PROVABLE_DIGEST_FUNCS` entry (`verify_store.rs:386-478`, `#fl1786`).
// Narrowing the advertisement removes the invitation, not the door.
//
// The cost of the extra entry is therefore not a rejection, it is divergent
// keying: a client that takes the invitation deposits blobs no blake3 client
// sharing this cache can ever name, and they persist as residue that only a
// worker re-upload plus proving can converge (`deferred_tasks.md`,
// `#sha256-blob-eviction`).
#[nativelink_test]
async fn cache_capabilities_advertise_only_the_accepted_digest_function()
-> Result<(), Box<dyn core::error::Error>> {
    pin_production_blake3_default();

    let configs = [capabilities_config(COMPRESSION_INSTANCE, false, false)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);
    let server = CapabilitiesServer::new(
        &configs,
        &HashMap::new(),
        &remote_cache_compression_instances,
        &[],
    )
    .await?;

    let response = get_capabilities(&server, COMPRESSION_INSTANCE).await?;
    let cache_capabilities = response
        .cache_capabilities
        .expect("cache capabilities should be set");

    assert_eq!(
        cache_capabilities.digest_functions,
        vec![i32::from(DigestFunction::Blake3)],
        "cache capabilities must advertise EXACTLY [BLAKE3] -- the configured \
         `global.default_digest_hash_function`, the one function this fleet is \
         keyed under. Any extra entry tells a conforming REAPI client it may key \
         a whole build under that function; the server then admits those blobs \
         (explicitly-labelled, or via #fl1786 proving) into a cache where no \
         blake3 client can ever name them. Advertising a function we do not want \
         used is the bug this list exists to avoid"
    );
    Ok(())
}

#[nativelink_test]
async fn execution_capabilities_advertise_only_the_accepted_digest_function()
-> Result<(), Box<dyn core::error::Error>> {
    pin_production_blake3_default();

    let configs = [capabilities_config(EXECUTION_INSTANCE, false, true)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);

    let mock_scheduler = Arc::new(MockActionScheduler::new());
    let mut scheduler_map: HashMap<String, Arc<dyn KnownPlatformPropertyProvider>> = HashMap::new();
    scheduler_map.insert(SCHEDULER_NAME.to_string(), mock_scheduler.clone());

    let new_server_fut = CapabilitiesServer::new(
        &configs,
        &scheduler_map,
        &remote_cache_compression_instances,
        &[],
    );
    let expected_scheduler_call_fut = mock_scheduler.expect_get_known_properties(Ok(Vec::new()));
    let (server, _scheduler_instance_name) = join!(new_server_fut, expected_scheduler_call_fut);
    let server = server?;

    let response = get_capabilities(&server, EXECUTION_INSTANCE).await?;
    let execution_capabilities = response
        .execution_capabilities
        .expect("execution capabilities should be set");

    assert_eq!(
        execution_capabilities.digest_functions,
        vec![i32::from(DigestFunction::Blake3)],
        "execution capabilities must advertise EXACTLY [BLAKE3] -- the configured \
         `global.default_digest_hash_function`. An execution client that picks \
         another advertised function keys its Action/Command/input-root digests \
         under it, so its whole action graph is addressed differently from every \
         other client's and shares nothing with them -- the cache splits in two \
         rather than erroring. Advertising a function we do not want used is the \
         bug this list exists to avoid"
    );

    // REAPI deprecates the singular `digest_function` in favour of the plural
    // list. They are two renderings of one fact, so they must never disagree:
    // a client honouring the deprecated field and one honouring the list have
    // to negotiate the same function.
    assert_eq!(
        vec![execution_capabilities.digest_function],
        execution_capabilities.digest_functions,
        "the deprecated singular `digest_function` and the plural \
         `digest_functions` must name the same single function; if they drift, \
         which one a client honours decides whether its upload is accepted"
    );
    Ok(())
}

const CHUNKING_INSTANCE: &str = "chunking";

fn cas_config_with_chunking(
    instance_name: &str,
    experimental_chunking: Option<CasChunkingConfig>,
) -> WithInstanceName<CasStoreConfig> {
    WithInstanceName {
        instance_name: instance_name.to_string(),
        config: CasStoreConfig {
            cas_store: "main_cas".to_string(),
            experimental_chunking,
        },
    }
}

// #2497: with no experimental_chunking config for the instance, the
// capabilities service MUST advertise split/splice as unsupported and omit
// FastCDC params (behavior is unchanged when the feature is not opted in).
#[nativelink_test]
async fn chunking_disabled_instance_does_not_advertise_split_splice()
-> Result<(), Box<dyn core::error::Error>> {
    pin_production_blake3_default();

    let configs = [capabilities_config(CHUNKING_INSTANCE, false, false)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);
    // A CAS instance exists but does not opt into chunking.
    let cas_configs = [cas_config_with_chunking(CHUNKING_INSTANCE, None)];
    let server = CapabilitiesServer::new(
        &configs,
        &HashMap::new(),
        &remote_cache_compression_instances,
        &cas_configs,
    )
    .await?;

    let response = get_capabilities(&server, CHUNKING_INSTANCE).await?;
    let cache_capabilities = response
        .cache_capabilities
        .expect("cache capabilities should be set");

    assert!(
        !cache_capabilities.split_blob_support,
        "split_blob_support must be false when experimental_chunking is unset"
    );
    assert!(
        !cache_capabilities.splice_blob_support,
        "splice_blob_support must be false when experimental_chunking is unset"
    );
    assert_eq!(
        cache_capabilities.fast_cdc_2020_params, None,
        "fast_cdc_2020_params must be None when experimental_chunking is unset"
    );
    Ok(())
}

// #2497: with experimental_chunking configured, the capabilities service MUST
// advertise split/splice support and the FastCDC 2020 params derived from the
// configured average chunk size — gated on the config, not hardcoded false.
#[nativelink_test]
async fn chunking_enabled_instance_advertises_split_splice_and_fastcdc_params()
-> Result<(), Box<dyn core::error::Error>> {
    pin_production_blake3_default();

    const AVG_CHUNK_SIZE: u64 = 1024;
    let configs = [capabilities_config(CHUNKING_INSTANCE, false, false)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);
    let cas_configs = [cas_config_with_chunking(
        CHUNKING_INSTANCE,
        Some(CasChunkingConfig {
            index_store: Some("chunk_index".to_string()),
            avg_chunk_size_bytes: AVG_CHUNK_SIZE,
            max_chunk_count: 0,
        }),
    )];
    let server = CapabilitiesServer::new(
        &configs,
        &HashMap::new(),
        &remote_cache_compression_instances,
        &cas_configs,
    )
    .await?;

    let response = get_capabilities(&server, CHUNKING_INSTANCE).await?;
    let cache_capabilities = response
        .cache_capabilities
        .expect("cache capabilities should be set");

    assert!(
        cache_capabilities.split_blob_support,
        "split_blob_support must be true when experimental_chunking is set"
    );
    assert!(
        cache_capabilities.splice_blob_support,
        "splice_blob_support must be true when experimental_chunking is set"
    );
    assert_eq!(
        cache_capabilities.fast_cdc_2020_params,
        Some(FastCdc2020Params {
            avg_chunk_size_bytes: AVG_CHUNK_SIZE,
            seed: 0,
        }),
        "fast_cdc_2020_params must reflect the configured average chunk size"
    );
    Ok(())
}
