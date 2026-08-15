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

//! `#fl1786-server-side-digest-function-proving`: the candidate set is the
//! ADVERTISED set, verified at runtime rather than asserted from memory.
//!
//! Proving is only as complete as its candidate list. `PROVABLE_DIGEST_FUNCS`
//! is a compile-time array; the set the server tells clients it accepts comes
//! out of the live `GetCapabilities` RPC (`capabilities_server.rs`,
//! `CacheCapabilities.digest_functions` and
//! `ExecutionCapabilities.digest_functions`). Those are two independent
//! declarations of the same fact and nothing else makes them agree.
//!
//! The failure this pins is asymmetric, and silent in one direction:
//!
//! - A function ADVERTISED but not PROVABLE re-opens the latch. Clients are
//!   told "you may key blobs with this", they do, and any such write whose
//!   label is wrong is rejected forever with no candidate able to rescue it.
//! - A function PROVABLE but not advertised is harmless — a candidate that
//!   simply never matches.
//!
//! So the assertion is directional: `advertised ⊆ provable`. It reads the
//! RPC's actual response rather than the `vec![...]` literal in the handler,
//! because the response is the surface clients consume.

use std::collections::HashMap;

use nativelink_config::cas_server::{CapabilitiesConfig, WithInstanceName};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::GetCapabilitiesRequest;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_server::Capabilities;
use nativelink_proto::build::bazel::remote::execution::v2::digest_function::Value as ProtoDigestFunction;
use nativelink_service::capabilities_server::CapabilitiesServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
use nativelink_util::digest_hasher::{DigestHasherFunc, PROVABLE_DIGEST_FUNCS};
use tonic::Request;

const INSTANCE: &str = "main";

/// Assert `advertised ⊆ PROVABLE_DIGEST_FUNCS`, naming the surface the list
/// came from so a failure says which advertisement is uncovered.
fn assert_all_provable(advertised: &[i32], surface: &str) {
    assert!(
        !advertised.is_empty(),
        "#fl1786: GetCapabilities advertised NO digest functions on {surface} — the subset \
         assertion below would pass vacuously and prove nothing about the prover's coverage"
    );
    for raw in advertised {
        let func = DigestHasherFunc::try_from(*raw).unwrap_or_else(|err| {
            panic!(
                "#fl1786: GetCapabilities advertises digest function {raw:?} ({:?}) on \
                 {surface}, which does not even convert to a DigestHasherFunc: {err:?}",
                ProtoDigestFunction::try_from(*raw).map(|v| v.as_str_name())
            )
        });
        assert!(
            PROVABLE_DIGEST_FUNCS.contains(&func),
            "#fl1786 COVERAGE HOLE: GetCapabilities advertises {func:?} on {surface} but \
             PROVABLE_DIGEST_FUNCS = {PROVABLE_DIGEST_FUNCS:?} does not contain it. Every \
             advertised function MUST be a proving candidate: blobs a client keyed with \
             {func:?} that reach the server under any other label would be rejected forever \
             with no candidate able to rescue them — the FL-1786 latch, re-opened for a new \
             function. Add {func:?} to PROVABLE_DIGEST_FUNCS; proving is an identity check \
             against the blob's own declared digest, so an extra candidate can only fail to \
             match, never mis-accept."
        );
    }
}

#[nativelink_test]
async fn every_advertised_digest_function_is_a_proving_candidate()
-> Result<(), Box<dyn core::error::Error>> {
    let configs = [WithInstanceName {
        instance_name: INSTANCE.to_string(),
        config: CapabilitiesConfig::default(),
    }];
    let remote_cache_compression_instances =
        RemoteCacheCompressionInstances::from_capabilities_configs(&configs);
    let server = CapabilitiesServer::new(
        &configs,
        &HashMap::new(),
        &remote_cache_compression_instances,
        &[],
    )
    .await?;

    let response = server
        .get_capabilities(Request::new(GetCapabilitiesRequest {
            instance_name: INSTANCE.to_string(),
        }))
        .await?
        .into_inner();

    let cache_capabilities = response
        .cache_capabilities
        .expect("#fl1786: GetCapabilities must return cache_capabilities");
    assert_all_provable(&cache_capabilities.digest_functions, "CacheCapabilities");

    // `ExecutionCapabilities` carries its own independent list; it is only
    // populated for instances with remote execution configured, so its
    // absence here is expected and not a finding.
    if let Some(execution_capabilities) = response.execution_capabilities {
        assert_all_provable(
            &execution_capabilities.digest_functions,
            "ExecutionCapabilities",
        );
    }
    Ok(())
}
