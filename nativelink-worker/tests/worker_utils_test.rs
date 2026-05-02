#![cfg(target_family = "unix")]
use std::collections::HashMap;
use std::env;

use nativelink_config::cas_server::WorkerProperty;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_util::build_sha::BUILD_SHA_HEX_LEN;
use nativelink_worker::worker_utils::make_connect_worker_request;

#[nativelink_test]
async fn make_connect_worker_request_with_extra_envs() -> Result<(), Error> {
    let mut worker_properties: HashMap<String, WorkerProperty> = HashMap::new();
    worker_properties.insert(
        "test".into(),
        WorkerProperty::QueryCmd("bash -c \"echo $DEMO_ENV\"".to_string()),
    );
    let mut extra_envs = HashMap::new();
    extra_envs.insert("DEMO_ENV".into(), "test_value_for_demo_env".into());

    // So we have bash for nix cases, because the PATH gets reset
    extra_envs.insert("PATH".into(), env::var("PATH").unwrap());

    let res =
        make_connect_worker_request("1234".to_string(), &worker_properties, &extra_envs, 1, String::new()).await?;
    assert_eq!(
        res.properties.first(),
        Some(&Property {
            name: "test".into(),
            value: "test_value_for_demo_env".into()
        })
    );
    Ok(())
}

/// (#216) The hello frame the worker emits MUST populate `build_sha`
/// to a real SHA-256 prefix — never the empty string. The opt-in
/// scheduler-side allowlist is useless if the worker forgot to ship
/// the SHA at all (every connect would silently report `""` and pass
/// any allowlist that accepts the legacy marker). This test guards
/// the wire-side half of the contract that the integration tests in
/// `nativelink-service/tests/worker_api_build_sha_test.rs` assume.
#[nativelink_test]
async fn make_connect_worker_request_populates_build_sha() -> Result<(), Error> {
    let worker_properties: HashMap<String, WorkerProperty> = HashMap::new();
    let extra_envs: HashMap<String, String> = HashMap::new();
    let res = make_connect_worker_request(
        "build_sha_test_".to_string(),
        &worker_properties,
        &extra_envs,
        1,
        String::new(),
    )
    .await?;
    assert_eq!(
        res.build_sha.len(),
        BUILD_SHA_HEX_LEN,
        "ConnectWorkerRequest.build_sha MUST be exactly {BUILD_SHA_HEX_LEN} hex chars; \
         got {res:?} — empty / wrong length means the worker is not reporting its build, \
         and the scheduler-side compatible_build_shas allowlist is useless without it",
    );
    assert!(
        res.build_sha.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "ConnectWorkerRequest.build_sha MUST be lowercase hex (matches operator-typed allowlist \
         entries which are also lowercase); got {:?}",
        res.build_sha,
    );
    Ok(())
}
