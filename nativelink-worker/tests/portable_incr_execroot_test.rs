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

//! FL-1383 chunk 2b — the byte-identical execroot machinery (design §4/§5/§7/§9).
//! These tests pin the contracts the architectural core rests on:
//!  - eligibility/INERTNESS of [`PortableIncrContext::plan`] (§6 item — flag OFF /
//!    carrier absent / not-allowlisted / malformed key ⇒ NONE ⇒ normal path);
//!  - execroot path == `<FIXED_PREFIX>/<targetkey>` for an allowlisted action (§4);
//!  - the machine-local RAII lease: OWNER warm, same-targetkey CONTENDER isolated,
//!    release-on-drop → reuse (§5);
//!  - the wipe PRESERVES `-incr` / `-incr-metadata` and CONFINES the delete to
//!    FIXED_PREFIX (§7/§9);
//!  - the worker-side targetkey integrity verify (§11 item 1);
//!  - the contender cold-discard target + containment gate (§9).

use core::time::Duration;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use nativelink_config::cas_server::{
    PortableIncrConfig, UploadActionResultConfig, UploadCacheResultsStrategy,
};
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, ExecutionMetadata, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::Store;
use nativelink_util::targetkey::TargetKey;
use nativelink_worker::portable_incr::{
    CARRIER_PRIMARY_OUTPUT_PROP, CARRIER_TARGETKEY_PROP, ExecrootRole, PortableExecroot,
    PortableIncrContext, PortableIncrProvision, assert_under_prefix, ensure_and_wipe_execroot_at,
    is_incr_seed_entry,
};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningAction, RunningActionImpl, RunningActionsManagerArgs,
    RunningActionsManagerImpl,
};

/// Canonicalized temp root so the FIXED_PREFIX containment asserts are already
/// realpath-stable (`TMPDIR` may itself be a symlink on the test box).
fn canonical_tempdir() -> (tempfile::TempDir, PathBuf) {
    let td = tempfile::TempDir::new().expect("tempdir");
    let canon = fs::canonicalize(td.path()).expect("canonicalize tempdir");
    (td, canon)
}

/// Build an ENABLED context whose FIXED_PREFIX is `fixed_prefix`, allowlisting
/// every `allow` prefix.
fn enabled_context(fixed_prefix: &Path, allow: &[&str]) -> PortableIncrContext {
    let provision = PortableIncrProvision {
        fixed_prefix: fixed_prefix.to_path_buf(),
    };
    let config = PortableIncrConfig {
        enabled: true,
        action_output_allowlist: allow.iter().map(|s| (*s).to_string()).collect(),
    };
    PortableIncrContext::from_provision(Some(provision), config).expect("provisioned context")
}

/// Carrier platform properties for `primary_output`, whose `targetkey` is the
/// real worker-agreed `TargetKey::derive([primary_output]).key()`.
fn carrier_props(primary_output: &str) -> (String, HashMap<String, String>) {
    let key = TargetKey::derive(&[primary_output.to_string()])
        .expect("derive")
        .key()
        .to_string();
    let mut props = HashMap::new();
    props.insert(CARRIER_TARGETKEY_PROP.to_string(), key.clone());
    props.insert(
        CARRIER_PRIMARY_OUTPUT_PROP.to_string(),
        primary_output.to_string(),
    );
    (key, props)
}

// -- §7 preserve predicate ---------------------------------------------------

#[nativelink_test]
async fn is_incr_seed_entry_matches_incr_and_metadata_only() {
    assert!(is_incr_seed_entry("libfoo-incr"), "-incr tree must be preserved");
    assert!(
        is_incr_seed_entry("libfoo-incr-metadata"),
        "-incr-metadata tree must be preserved"
    );
    assert!(!is_incr_seed_entry("libfoo.rlib"), "outputs are not seed");
    assert!(!is_incr_seed_entry("bazel-out"), "sources are not seed");
    assert!(!is_incr_seed_entry("incr"), "bare 'incr' is not a seed dir");
}

// -- §4 execroot path + eligibility -----------------------------------------

#[nativelink_test]
async fn allowlisted_action_plans_owner_at_fixed_prefix_slash_targetkey() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["bazel-out/darwin_arm64-fastbuild/"]);
    let (key, props) = carrier_props("bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib");

    let plan = ctx.plan(&props).expect("allowlisted action must be eligible");
    assert_eq!(plan.role(), ExecrootRole::Owner, "first is the warm owner");
    assert_eq!(
        plan.execroot(),
        root.join(&key),
        "execroot must be <FIXED_PREFIX>/<targetkey>"
    );
}

// -- INERTNESS: every ineligible path returns None (→ normal per-op dir) ------

#[nativelink_test]
async fn flag_off_is_inert_even_with_carrier() {
    let (_td, root) = canonical_tempdir();
    let provision = PortableIncrProvision {
        fixed_prefix: root.clone(),
    };
    let config = PortableIncrConfig {
        enabled: false,
        action_output_allowlist: vec!["bazel-out/".to_string()],
    };
    let ctx =
        PortableIncrContext::from_provision(Some(provision), config).expect("context builds");
    let (_key, props) = carrier_props("bazel-out/bin/libfoo.rlib");
    assert!(
        ctx.plan(&props).is_none(),
        "flag OFF ⇒ INERT (normal path) even when the carrier is present"
    );
}

#[nativelink_test]
async fn unprovisioned_is_none_context() {
    let config = PortableIncrConfig {
        enabled: true,
        action_output_allowlist: vec!["bazel-out/".to_string()],
    };
    assert!(
        PortableIncrContext::from_provision(None, config).is_none(),
        "no provision (feature off / §12 fail-loud-DISABLE) ⇒ None ⇒ INERT"
    );
}

#[nativelink_test]
async fn carrier_absent_is_inert() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["bazel-out/"]);
    assert!(
        ctx.plan(&HashMap::new()).is_none(),
        "no carrier property ⇒ normal path"
    );
}

#[nativelink_test]
async fn not_allowlisted_is_inert() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["bazel-out/darwin_arm64-opt/"]);
    let (_key, props) = carrier_props("bazel-out/darwin_arm64-fastbuild/bin/libfoo.rlib");
    assert!(
        ctx.plan(&props).is_none(),
        "primary output not matching any allowlist prefix ⇒ normal path"
    );
}

#[nativelink_test]
async fn malformed_targetkey_is_inert() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["bazel-out/"]);
    let mut props = HashMap::new();
    // Not 64-hex: a traversal-shaped value must NEVER become a path segment.
    props.insert(
        CARRIER_TARGETKEY_PROP.to_string(),
        "../../etc/passwd".to_string(),
    );
    props.insert(
        CARRIER_PRIMARY_OUTPUT_PROP.to_string(),
        "bazel-out/bin/libfoo.rlib".to_string(),
    );
    assert!(
        ctx.plan(&props).is_none(),
        "malformed (non-64-hex) targetkey ⇒ normal path, never a FIXED_PREFIX join segment"
    );
}

// -- §5 machine-local lease: owner / contender / release-on-drop → reuse ------

#[nativelink_test]
async fn same_targetkey_contender_is_isolated_then_reusable_on_release() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["lease-test/"]);
    // A targetkey unique to this test so the process-global lease set cannot
    // collide with a sibling test running concurrently.
    let (key, props) = carrier_props("lease-test/unique-abc123/libx.rlib");

    let owner = ctx.plan(&props).expect("owner");
    assert_eq!(owner.role(), ExecrootRole::Owner);
    assert_eq!(owner.execroot(), root.join(&key));

    let contender = ctx.plan(&props).expect("contender");
    assert_eq!(
        contender.role(),
        ExecrootRole::Contender,
        "a second same-targetkey action on one machine is an isolated contender"
    );
    assert_ne!(
        contender.execroot(),
        owner.execroot(),
        "contender must get an ISOLATED dir, never the warm owner dir"
    );
    assert!(
        contender.execroot().starts_with(&root),
        "contender isolated dir stays under FIXED_PREFIX"
    );
    assert!(
        contender
            .execroot()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(&format!("{key}.")),
        "contender dir is <targetkey>.<uuid>"
    );

    // Contender owns nothing to release; owner does. Release the owner:
    drop(owner);
    // A fresh same-targetkey action can now become OWNER again (reuse warm dir).
    let reowner = ctx.plan(&props).expect("re-owner after release");
    assert_eq!(
        reowner.role(),
        ExecrootRole::Owner,
        "owner lease releases on drop ⇒ next same-targetkey action reuses the warm dir"
    );
    assert_eq!(reowner.execroot(), root.join(&key));
    drop(contender);
    drop(reowner);
}

// -- §9 contender discard target + owner-never-deleted ------------------------

#[nativelink_test]
async fn discard_target_is_contender_only() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["discard-test/"]);
    let (_key, props) = carrier_props("discard-test/uniq-9/liby.rlib");

    let owner = ctx.plan(&props).expect("owner");
    assert!(
        owner.contender_discard_target().is_none(),
        "an OWNER's warm dir must NEVER be a discard target (it is preserved)"
    );

    let contender = ctx.plan(&props).expect("contender");
    let (dir, prefix) = contender
        .contender_discard_target()
        .expect("a contender IS cold-discarded");
    assert_eq!(dir, contender.execroot().to_path_buf());
    assert_eq!(prefix, root);
    drop(owner);
    drop(contender);
}

// -- §11 worker-side targetkey integrity verify ------------------------------

#[nativelink_test]
async fn verify_matches_worker_derived_key() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["verify-test/"]);
    let primary = "verify-test/uniq-v/libfoo.rlib";
    let (_key, props) = carrier_props(primary);
    let plan = ctx.plan(&props).expect("owner");

    // The Command's sorted-smallest output equals the carrier primary.
    plan.verify_against_command_outputs(&[
        "verify-test/uniq-v/libfoo.rmeta".to_string(),
        primary.to_string(),
    ])
    .expect("worker-derived key equals carrier ⇒ Ok");
    drop(plan);
}

#[nativelink_test]
async fn verify_rejects_key_mismatch() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["verify-test/"]);
    let (_key, props) = carrier_props("verify-test/uniq-m/libfoo.rlib");
    let plan = ctx.plan(&props).expect("owner");

    // A DIFFERENT primary output ⇒ different worker-derived key ⇒ reject.
    let err = plan
        .verify_against_command_outputs(&["verify-test/uniq-m/OTHER.rlib".to_string()])
        .expect_err("carrier key != worker-derived ⇒ Err");
    assert!(
        format!("{err}").contains("worker-derived"),
        "bespoke mismatch message; got: {err}"
    );
    drop(plan);
}

#[nativelink_test]
async fn verify_rejects_empty_output_paths() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["verify-test/"]);
    let (_key, props) = carrier_props("verify-test/uniq-e/libfoo.rlib");
    let plan = ctx.plan(&props).expect("owner");
    let err = plan
        .verify_against_command_outputs(&[])
        .expect_err("no output_paths ⇒ Err");
    assert!(
        format!("{err}").contains("no output_paths"),
        "bespoke empty message; got: {err}"
    );
    drop(plan);
}

// -- §7 wipe preserves -incr, empties the rest -------------------------------

#[nativelink_test]
async fn wipe_preserves_incr_and_metadata_empties_rest() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["wipe-test/"]);
    let (key, props) = carrier_props("wipe-test/uniq-w/libfoo.rlib");
    let plan = ctx.plan(&props).expect("owner");
    let execroot = root.join(&key);

    // Simulate a prior build's residue in the warm execroot.
    fs::create_dir_all(execroot.join("libfoo-incr/dep-graph")).expect("mk incr");
    fs::write(execroot.join("libfoo-incr/dep-graph/x"), b"seed").expect("seed file");
    fs::create_dir_all(execroot.join("libfoo-incr-metadata")).expect("mk incr-metadata");
    fs::create_dir_all(execroot.join("bazel-out/bin")).expect("mk out");
    fs::write(execroot.join("bazel-out/bin/libfoo.rlib"), b"stale").expect("stale output");
    fs::write(execroot.join("stdout.txt"), b"junk").expect("junk file");

    plan.ensure_and_wipe_execroot().expect("wipe ok");

    assert!(
        execroot.join("libfoo-incr/dep-graph/x").exists(),
        "-incr seed tree (and its contents) MUST survive the wipe"
    );
    assert!(
        execroot.join("libfoo-incr-metadata").exists(),
        "-incr-metadata tree MUST survive the wipe"
    );
    assert!(
        !execroot.join("bazel-out").exists(),
        "non-incr output tree MUST be emptied"
    );
    assert!(
        !execroot.join("stdout.txt").exists(),
        "non-incr transient file MUST be emptied"
    );
    drop(plan);
}

#[nativelink_test]
async fn wipe_creates_execroot_when_absent() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["mk-test/"]);
    let (key, props) = carrier_props("mk-test/uniq-c/libfoo.rlib");
    let plan = ctx.plan(&props).expect("owner");
    let execroot = root.join(&key);
    assert!(!execroot.exists(), "precondition: execroot absent");
    plan.ensure_and_wipe_execroot().expect("ensure creates it");
    assert!(execroot.is_dir(), "execroot created (owner first build)");
    drop(plan);
}

// -- §9 delete-containment: a wipe target outside FIXED_PREFIX is refused -----

#[nativelink_test]
async fn wipe_refuses_target_outside_fixed_prefix() {
    let (_td, root) = canonical_tempdir();
    let prefix = root.join("prefix");
    let outside = root.join("outside");
    fs::create_dir_all(&prefix).expect("mk prefix");
    fs::create_dir_all(&outside).expect("mk outside");
    fs::write(outside.join("victim"), b"do-not-delete").expect("victim");

    let err = ensure_and_wipe_execroot_at(&outside, &prefix)
        .expect_err("execroot not under FIXED_PREFIX ⇒ refuse");
    assert!(
        format!("{err}").contains("escapes FIXED_PREFIX"),
        "bespoke containment message; got: {err}"
    );
    assert!(
        outside.join("victim").exists(),
        "the confinement gate must PREVENT the delete, not just report it"
    );
}

// -- §6 INERT invariant at PRODUCTION composition (RunningActionImpl seam) ----
//
// The critical inertness contract: whether an action is portable is decided by
// the `Option<PortableExecroot>` handed to `RunningActionImpl::new`. `None` (the
// entire fleet) MUST yield the byte-identical `<action_directory>/work` work dir;
// `Some` MUST make the execroot itself the work dir (NO `/work` segment, design
// §4). These two tests exercise the real constructor.

fn rand_temp(data: &str) -> String {
    use rand::Rng;
    let tmp = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    format!("{}/{}/{}", tmp, rand::rng().random::<u64>(), data)
}

async fn setup_manager() -> Arc<RunningActionsManagerImpl> {
    let fast_config = FilesystemSpec {
        content_path: rand_temp("content_path"),
        temp_path: rand_temp("temp_path"),
        eviction_policy: None,
        ..Default::default()
    };
    let slow_config = MemorySpec::default();
    let fast_store = <FilesystemStore>::new(&fast_config).await.expect("fast store");
    let slow_store = MemoryStore::new(&slow_config);
    let cas_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_config),
            slow: StoreSpec::Memory(slow_config),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(fast_store),
        Store::new(slow_store.clone()),
    );
    let root_action_directory = rand_temp("root_action_directory");
    nativelink_util::common::fs::create_dir_all(&root_action_directory)
        .await
        .expect("mkdir root");
    let manager = RunningActionsManagerImpl::new(RunningActionsManagerArgs {
        root_action_directory,
        execution_configuration: ExecutionConfiguration::default(),
        cas_store: cas_store.clone(),
        ac_store: None,
        ac_mirror_target: None,
        historical_store: Store::new(cas_store.clone()),
        upload_action_result_config: &UploadActionResultConfig {
            upload_ac_results_strategy: UploadCacheResultsStrategy::Never,
            ..Default::default()
        },
        max_action_timeout: Duration::MAX,
        max_upload_timeout: Duration::from_secs(600),
        timeout_handled_externally: false,
        directory_cache: None,
        bis_ack_timeout: Duration::from_secs(60),
        metrics: None,
        cas_endpoint: String::new(),
        deferred_output_uploads_enabled: false,
    })
    .expect("manager");
    Arc::new(manager)
}

fn inert_action_info() -> ActionInfo {
    ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::from_secs(60),
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
            instance_name: "test".to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: DigestInfo::new([0u8; 32], 0),
        }),
        targetkey: None,
    }
}

fn inert_metadata() -> ExecutionMetadata {
    ExecutionMetadata {
        worker: "test".to_string(),
        queued_timestamp: SystemTime::UNIX_EPOCH,
        worker_start_timestamp: SystemTime::UNIX_EPOCH,
        input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
        input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
        execution_start_timestamp: SystemTime::UNIX_EPOCH,
        execution_completed_timestamp: SystemTime::UNIX_EPOCH,
        output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
        output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
        worker_completed_timestamp: SystemTime::UNIX_EPOCH,
    }
}

fn make_action(
    manager: Arc<RunningActionsManagerImpl>,
    action_directory: &str,
    portable: Option<PortableExecroot>,
) -> Arc<RunningActionImpl> {
    Arc::new(RunningActionImpl::new(
        inert_metadata(),
        OperationId::default(),
        action_directory.to_string(),
        inert_action_info(),
        Duration::from_secs(60),
        manager,
        None,
        None,
        portable,
    ))
}

#[nativelink_test]
async fn non_portable_action_keeps_slash_work_workdir_unchanged() {
    let manager = setup_manager().await;
    let action_directory = rand_temp("action_dir");
    let action = make_action(manager, &action_directory, None);
    assert_eq!(
        action.get_work_directory(),
        &format!("{action_directory}/work"),
        "INERT: a non-portable action's work_directory is byte-for-byte <action_directory>/work"
    );
}

#[nativelink_test]
async fn portable_action_workdir_is_execroot_no_work_segment() {
    let manager = setup_manager().await;
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["compose-test/"]);
    let (key, props) = carrier_props("compose-test/uniq-p/libfoo.rlib");
    let plan = ctx.plan(&props).expect("allowlisted ⇒ portable");
    let expected_execroot = root.join(&key).to_string_lossy().into_owned();

    let action_directory = rand_temp("action_dir");
    let action = make_action(manager, &action_directory, Some(plan));
    assert_eq!(
        action.get_work_directory(),
        &expected_execroot,
        "portable action's work_directory IS <FIXED_PREFIX>/<targetkey> (no /work segment)"
    );
    assert!(
        !action.get_work_directory().ends_with("/work"),
        "portable action drops the /work segment (design §4)"
    );
}

#[nativelink_test]
async fn assert_under_prefix_rejects_prefix_itself_and_siblings() {
    let (_td, root) = canonical_tempdir();
    let prefix = root.join("fp");
    fs::create_dir_all(prefix.join("child")).expect("mk");
    let sibling = root.join("fp-evil");
    fs::create_dir_all(&sibling).expect("mk sibling");

    assert_under_prefix(&prefix.join("child"), &prefix).expect("a real child is under the prefix");
    assert!(
        assert_under_prefix(&prefix, &prefix).is_err(),
        "FIXED_PREFIX itself is not a valid discard target"
    );
    assert!(
        assert_under_prefix(&sibling, &prefix).is_err(),
        "a string-prefix sibling (fp-evil) must NOT count as under fp"
    );
}
