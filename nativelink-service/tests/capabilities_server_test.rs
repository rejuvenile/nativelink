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
use nativelink_proto::build::bazel::remote::execution::v2::{
    FastCdc2020Params, GetCapabilitiesRequest, ServerCapabilities, compressor,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::mock_scheduler::MockActionScheduler;
use nativelink_service::capabilities_server::CapabilitiesServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
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

#[nativelink_test]
async fn compression_only_instance_advertises_zstd_cache_capabilities()
-> Result<(), Box<dyn core::error::Error>> {
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

    assert_eq!(
        cache_capabilities.supported_compressors,
        vec![compressor::Value::Zstd as i32]
    );
    assert_eq!(
        cache_capabilities.supported_batch_update_compressors,
        vec![compressor::Value::Zstd as i32]
    );
    Ok(())
}

#[nativelink_test]
async fn compression_only_instance_does_not_advertise_execution_capabilities()
-> Result<(), Box<dyn core::error::Error>> {
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
