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

//! `#single-digest`: the advertised `digest_functions` must FOLLOW the
//! configured `global.default_digest_hash_function` — it must not be a
//! `[BLAKE3]` literal that happens to agree with it.
//!
//! `capabilities_server.rs` states that derivation as the deliberate design
//! choice: hardcoding would reintroduce the same defect mirrored, an operator
//! configuring sha256 running a server that advertises blake3. **Nothing
//! tested it.** Every `set_default_digest_hasher_func` call site in the
//! workspace sets BLAKE3, so replacing the derivation with
//! `vec![DigestFunction::Blake3.into()]` left all seven
//! `capabilities_server_test` cases AND
//! `digest_func_proving_advertised_set_test` green — mutation M7,
//! `.claude/reviews/26121990/pair-b.md`.
//!
//! **Why this is its own test binary.** `DEFAULT_DIGEST_HASHER_FUNC` is a
//! process-global `OnceLock` (`digest_hasher.rs:52-54`), so exactly one value
//! is observable per process, and `capabilities_server_test` already owns
//! BLAKE3 in its process — the deployed value, which is the right thing for it
//! to assert. A property that only a DIFFERENT configured value can
//! distinguish therefore needs a second process. Integration tests are one
//! process per file, so a second file is the mechanism.
//!
//! **What the assert can and cannot see.** `DigestHasherFunc` has exactly two
//! variants, and SHA-256 is also the `get_or_init` fallback, so observing
//! SHA-256 here does not prove the `set` call is what put it there. That is
//! fine for the property under test: what is pinned is that
//! `advertised_digest_functions()` reports whatever the process-global
//! actually is, which a `[BLAKE3]` literal cannot do. The pre-condition assert
//! is still load-bearing in the other direction — if anything in this binary
//! pinned BLAKE3 first, it fires instead of silently asserting the deployed
//! value twice.

use std::collections::HashMap;
use std::sync::Arc;

use futures::join;
use nativelink_config::cas_server::{
    CapabilitiesConfig, CapabilitiesRemoteExecutionConfig, WithInstanceName,
};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_server::Capabilities;
use nativelink_proto::build::bazel::remote::execution::v2::digest_function::Value as DigestFunction;
use nativelink_proto::build::bazel::remote::execution::v2::{
    GetCapabilitiesRequest, ServerCapabilities,
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

const CACHE_INSTANCE: &str = "cache";
const EXECUTION_INSTANCE: &str = "execution";
const SCHEDULER_NAME: &str = "main_scheduler";

/// Configure this process's global digest function to SHA-256 — deliberately
/// NOT the deployed BLAKE3, because BLAKE3 is the value a hardcoded list would
/// also produce.
fn configure_non_deployed_sha256_default() {
    drop(set_default_digest_hasher_func(DigestHasherFunc::Sha256));
    assert_eq!(
        default_digest_hasher_func(),
        DigestHasherFunc::Sha256,
        "this test binary must run with a SHA-256 process-global default; it exists \
         precisely to observe the advertisement under a NON-deployed configuration, \
         and if something pinned BLAKE3 first every assertion below would agree with \
         a hardcoded [BLAKE3] list and prove nothing"
    );
}

fn capabilities_config(
    instance_name: &str,
    remote_execution: bool,
) -> WithInstanceName<CapabilitiesConfig> {
    WithInstanceName {
        instance_name: instance_name.to_string(),
        config: CapabilitiesConfig {
            remote_execution: remote_execution.then(|| CapabilitiesRemoteExecutionConfig {
                scheduler: SCHEDULER_NAME.to_string(),
            }),
            remote_cache_compression: false,
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
async fn cache_advertisement_follows_a_sha256_configured_default()
-> Result<(), Box<dyn core::error::Error>> {
    configure_non_deployed_sha256_default();

    let configs = [capabilities_config(CACHE_INSTANCE, false)];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);
    let server = CapabilitiesServer::new(
        &configs,
        &HashMap::new(),
        &remote_cache_compression_instances,
        &[],
    )
    .await?;

    let cache_capabilities = get_capabilities(&server, CACHE_INSTANCE)
        .await?
        .cache_capabilities
        .expect("cache capabilities should be set");

    assert_eq!(
        cache_capabilities.digest_functions,
        vec![i32::from(DigestFunction::Sha256)],
        "cache capabilities must advertise the CONFIGURED default -- here SHA-256 -- \
         not a BLAKE3 literal. A hardcoded list mirrors the defect this change \
         removed: an operator who configures sha256 would run a server advertising \
         blake3, inviting every client to key blobs under a function the server \
         does not assume"
    );
    Ok(())
}

#[nativelink_test]
async fn execution_advertisement_follows_a_sha256_configured_default()
-> Result<(), Box<dyn core::error::Error>> {
    configure_non_deployed_sha256_default();

    let configs = [capabilities_config(EXECUTION_INSTANCE, true)];
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

    let execution_capabilities = get_capabilities(&server, EXECUTION_INSTANCE)
        .await?
        .execution_capabilities
        .expect("execution capabilities should be set");

    assert_eq!(
        execution_capabilities.digest_functions,
        vec![i32::from(DigestFunction::Sha256)],
        "execution capabilities must advertise the CONFIGURED default -- here \
         SHA-256 -- not a BLAKE3 literal. This site carries its own list, so a \
         hardcode here survives every cache-side assertion"
    );
    assert_eq!(
        execution_capabilities.digest_function,
        i32::from(DigestFunction::Sha256),
        "the deprecated singular `digest_function` must follow the configured \
         default too; a client honouring it and one honouring the plural list have \
         to negotiate the same function under EVERY configuration, not just the \
         deployed one"
    );
    Ok(())
}
