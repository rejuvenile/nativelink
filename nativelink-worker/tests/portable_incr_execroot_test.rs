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
//!  - the FULL-EMPTY wipe removes ALL children (nothing preserved — the `-incr`
//!    seed is re-fetched fresh after inputs) and CONFINES the delete to
//!    FIXED_PREFIX (§7/§9);
//!  - the worker-side targetkey integrity verify (§11 item 1);
//!  - the contender cold-discard target + containment gate (§9).

use core::time::Duration;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use prost::Message;

use nativelink_config::cas_server::{
    PortableIncrConfig, UploadActionResultConfig, UploadCacheResultsStrategy,
};
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult as ProtoActionResult, Command, Digest, Directory, ExecuteRequest,
    FileNode, Platform, Tree,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_store::ac_utils::{get_and_decode_digest, serialize_and_upload_message};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, ExecutionMetadata, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use nativelink_util::targetkey::TargetKey;
use nativelink_worker::incr_seed_fetch::{incr_seed_metrics, plan_seed_publish, seed_dest_dir};
use nativelink_worker::portable_incr::{
    CARRIER_PRIMARY_OUTPUT_PROP, CARRIER_TARGETKEY_PROP, DEFAULT_WARM_DIR_BUDGET_BYTES,
    EvictionOutcome, ExecrootRole, PortableExecroot, PortableIncrContext, PortableIncrProvision,
    assert_under_prefix, ensure_and_wipe_execroot_at,
};
use nativelink_worker::local_worker::portable_incr_startup_sweep;
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningAction, RunningActionImpl, RunningActionsManager,
    RunningActionsManagerArgs, RunningActionsManagerImpl,
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

#[nativelink_test]
async fn verify_rejects_forged_targetkey_with_matching_primary() {
    // Isolate the KEY guard (portable_incr.rs, `derived.key() != carrier_targetkey`)
    // from the primary-output guard. `verify_rejects_key_mismatch` swaps the
    // primary output, which changes the DERIVED key AND the derived primary — so
    // it trips whichever guard survives and both messages say "worker-derived",
    // making it unable to prove the KEY guard specifically. Here the carrier
    // primary output EQUALS the derived primary (primary guard would PASS), but
    // the carrier targetkey is a DIFFERENT valid 64-hex — a forged key routing a
    // build into a different crate's warm execroot (cross-crate seed poisoning).
    // Only the KEY guard can reject this.
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["forge-test/"]);
    let primary = "forge-test/uniq-f/libfoo.rlib";
    let real_key = TargetKey::derive(&[primary.to_string()])
        .expect("derive")
        .key()
        .to_string();
    // Forge a DIFFERENT valid 64-lowercase-hex key by flipping the first nibble
    // (0↔1 keeps it lowercase-hex and guarantees inequality).
    let mut forged = real_key.clone().into_bytes();
    forged[0] = if forged[0] == b'0' { b'1' } else { b'0' };
    let forged_key = String::from_utf8(forged).expect("hex is valid utf8");
    assert_ne!(forged_key, real_key, "forged key must differ from the real key");

    let mut props = HashMap::new();
    props.insert(CARRIER_TARGETKEY_PROP.to_string(), forged_key.clone());
    props.insert(CARRIER_PRIMARY_OUTPUT_PROP.to_string(), primary.to_string());
    let plan = ctx
        .plan(&props)
        .expect("forged key is still 64-hex + allowlisted ⇒ eligible to plan");

    // Command outputs derive to `real_key` with primary == carrier primary, so
    // the primary-output guard would PASS; only the KEY guard rejects.
    let err = plan
        .verify_against_command_outputs(&[primary.to_string()])
        .expect_err("forged carrier targetkey != worker-derived ⇒ Err");
    let msg = format!("{err}");
    assert!(
        msg.contains("carrier targetkey"),
        "must trip the KEY-specific guard, not the primary-output guard; got: {err}"
    );
    assert!(
        msg.contains("!= worker-derived"),
        "bespoke key-mismatch message; got: {err}"
    );
    drop(plan);
}

// -- §7 FULL-EMPTY wipe: EVERYTHING is removed, nothing preserved ------------

#[nativelink_test]
async fn wipe_full_empty_removes_all_including_incr() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["wipe-test/"]);
    let (key, props) = carrier_props("wipe-test/uniq-w/libfoo.rlib");
    let plan = ctx.plan(&props).expect("owner");
    let execroot = root.join(&key);

    // Simulate a prior build's residue in the reused execroot — INCLUDING a
    // nested `-incr` seed (the exact shape the chunk-2 top-level preserve missed).
    fs::create_dir_all(execroot.join("bazel-out/cfg/bin/pkg/libfoo-incr/dep-graph"))
        .expect("mk nested incr");
    fs::write(
        execroot.join("bazel-out/cfg/bin/pkg/libfoo-incr/dep-graph/x"),
        b"seed",
    )
    .expect("seed file");
    fs::create_dir_all(execroot.join("libfoo-incr")).expect("mk top-level incr");
    fs::write(execroot.join("stdout.txt"), b"junk").expect("junk file");

    plan.ensure_and_wipe_execroot().expect("wipe ok");

    // FULL-EMPTY (§7): the execroot still exists but holds NOTHING — the `-incr`
    // seed is NOT preserved (it is re-fetched fresh after inputs), a top-level
    // `-incr` is gone, and every transient is gone.
    assert!(execroot.is_dir(), "execroot itself must remain (ensured)");
    let remaining: Vec<_> = fs::read_dir(&execroot)
        .expect("read execroot")
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .collect();
    assert!(
        remaining.is_empty(),
        "full-empty wipe must leave the execroot with NO children, found: {remaining:?}"
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

// (Removed `incr_seed_survives_wipe_then_materialize`: with the FL-1383 §3
// full-empty wipe, the `-incr` seed is fetched FRESH after input materialization
// [B2]+[C], so the seed is never present on disk during the materialize — the
// "survive the wipe→materialize chain" property is moot. The clonefile fast path
// now fires on the empty execroot instead. See `wipe_full_empty_removes_all_*`
// and `portable_action_with_seeded_index_fetches_and_materializes`.)

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
    setup_manager_and_cas().await.0
}

/// Same as [`setup_manager`] but also returns the CAS `FastSlowStore` the manager
/// reads Action/Command protos from. The seed-wiring integration tests below need
/// this handle to upload the action protos (and pre-seed the `-incr` `Tree` +
/// blobs) into the SAME store `create_and_add_action` will fetch from.
async fn setup_manager_and_cas() -> (Arc<RunningActionsManagerImpl>, Arc<FastSlowStore>) {
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
    (Arc::new(manager), cas_store)
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

// -- §8 warm-dir eviction + contender-dir startup sweep ----------------------
//
// These lock the required-before-flag-flip disk-growth guard (chunk-2 review
// MAJOR-2/S2): every distinct `targetkey` leaves a persistent warm execroot dir
// with no GC, so `<FIXED_PREFIX>` grows unbounded at CI-widen. The sweep reaps
// crash-orphaned CONTENDER dirs at startup; the eviction bounds the warm-dir
// pool to a static budget (design v4 §8), never evicting a LEASED (live) dir.

/// A warm OWNER dir on disk at `<root>/<key>` holding a `filler` file of
/// `filler_bytes` bytes (so its apparent size is deterministic).
fn make_warm_dir(root: &Path, key: &str, filler_bytes: usize) {
    let d = root.join(key);
    fs::create_dir_all(&d).expect("mk warm dir");
    fs::write(d.join("filler"), vec![0u8; filler_bytes]).expect("filler");
}

/// Set a directory's own mtime (the eviction LRU signal) deterministically.
fn set_dir_mtime(dir: &Path, t: SystemTime) {
    let f = fs::File::open(dir).expect("open dir for mtime");
    f.set_modified(t).expect("set dir mtime");
}

fn epoch_plus(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

#[nativelink_test]
async fn startup_sweep_removes_contender_preserves_warm_incr_and_leased() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["sweep-test/"]);

    // A warm OWNER dir carrying an `-incr` seed — must be PRESERVED.
    let (key_warm, _) = carrier_props("sweep-test/uniq-warm/libfoo.rlib");
    let warm_incr = root.join(&key_warm).join("libfoo-incr/dep-graph");
    fs::create_dir_all(&warm_incr).expect("mk warm -incr");
    fs::write(warm_incr.join("x"), b"seed").expect("seed");

    // A leased OWNER dir (a live in-flight owner) — must be PRESERVED.
    let (key_leased, props_leased) = carrier_props("sweep-test/uniq-leased/libbar.rlib");
    fs::create_dir_all(root.join(&key_leased)).expect("mk leased dir");
    let leased = ctx.plan(&props_leased).expect("owner lease");
    assert_eq!(leased.execroot(), root.join(&key_leased));

    // A crash-orphaned CONTENDER dir `<64hex>.<32hex>` — must be REMOVED.
    let contender_name = format!("{}.{}", "a".repeat(64), "b".repeat(32));
    let contender = root.join(&contender_name);
    fs::create_dir_all(contender.join("junk")).expect("mk contender");
    fs::write(contender.join("junk/stale"), b"orphan").expect("orphan file");

    let removed = ctx
        .sweep_stale_contender_dirs()
        .expect("sweep must succeed");

    assert_eq!(removed, 1, "exactly the one contender dir is reaped");
    assert!(!contender.exists(), "stale contender orphan MUST be swept");
    assert!(
        warm_incr.join("x").exists(),
        "warm owner dir + its -incr seed MUST survive the sweep"
    );
    assert!(
        root.join(&key_leased).exists(),
        "a leased (live) owner dir MUST survive the sweep"
    );
    drop(leased);
}

#[nativelink_test]
async fn eviction_removes_lru_warm_dir_over_budget() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["evict-test/"]);
    let (key_a, _) = carrier_props("evict-test/uniq-a/libfoo.rlib");
    let (key_b, _) = carrier_props("evict-test/uniq-b/libfoo.rlib");
    let (key_c, _) = carrier_props("evict-test/uniq-c/libfoo.rlib");
    for key in [&key_a, &key_b, &key_c] {
        make_warm_dir(&root, key, 50);
    }
    // A oldest (LRU) → B → C newest.
    set_dir_mtime(&root.join(&key_a), epoch_plus(100));
    set_dir_mtime(&root.join(&key_b), epoch_plus(200));
    set_dir_mtime(&root.join(&key_c), epoch_plus(300));

    // total 150 > budget 120 → evict exactly one (the LRU): 150-50=100 ≤ 120.
    let outcome = ctx
        .evict_warm_dirs_over_budget(120)
        .expect("eviction must succeed");

    assert!(
        !root.join(&key_a).exists(),
        "the least-recently-used warm dir MUST be evicted first"
    );
    assert!(root.join(&key_b).exists(), "a newer dir stays under budget");
    assert!(root.join(&key_c).exists(), "the newest dir stays under budget");
    assert_eq!(outcome.dirs_evicted, 1, "exactly one dir evicted to fit budget");
    assert_eq!(outcome.bytes_freed, 50);
    assert_eq!(outcome.bytes_remaining, 100);
    assert!(!outcome.still_over_budget, "pool now within budget");
    assert_eq!(outcome.leased_skipped, 0);
}

#[nativelink_test]
async fn eviction_skips_leased_dir_and_evicts_next_lru() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["evict-lease/"]);
    let (key_a, props_a) = carrier_props("evict-lease/uniq-a/libfoo.rlib");
    let (key_b, _) = carrier_props("evict-lease/uniq-b/libfoo.rlib");
    let (key_c, _) = carrier_props("evict-lease/uniq-c/libfoo.rlib");
    for key in [&key_a, &key_b, &key_c] {
        make_warm_dir(&root, key, 50);
    }
    // A is the LRU (oldest) AND leased by a live owner → must be SKIPPED.
    set_dir_mtime(&root.join(&key_a), epoch_plus(100));
    set_dir_mtime(&root.join(&key_b), epoch_plus(200));
    set_dir_mtime(&root.join(&key_c), epoch_plus(300));
    let leased_a = ctx.plan(&props_a).expect("owner lease on A");
    assert_eq!(leased_a.execroot(), root.join(&key_a));

    // total 150 > 120: A (LRU) is leased → skip; evict B (next LRU) → 100 ≤ 120.
    let outcome = ctx
        .evict_warm_dirs_over_budget(120)
        .expect("eviction must succeed");

    assert!(
        root.join(&key_a).exists(),
        "a LEASED (live) warm dir MUST NOT be evicted even as the LRU"
    );
    assert!(
        !root.join(&key_b).exists(),
        "the next-LRU UNLEASED dir is evicted instead"
    );
    assert!(root.join(&key_c).exists(), "newest dir retained");
    assert_eq!(outcome.dirs_evicted, 1);
    assert_eq!(outcome.leased_skipped, 1, "the leased LRU dir is counted as skipped");
    assert!(!outcome.still_over_budget);
    drop(leased_a);
}

#[nativelink_test]
async fn eviction_all_leased_over_budget_refuses_backpressure() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["evict-all-lease/"]);
    let (key_a, props_a) = carrier_props("evict-all-lease/uniq-a/libfoo.rlib");
    let (key_b, props_b) = carrier_props("evict-all-lease/uniq-b/libfoo.rlib");
    for key in [&key_a, &key_b] {
        make_warm_dir(&root, key, 50);
    }
    set_dir_mtime(&root.join(&key_a), epoch_plus(100));
    set_dir_mtime(&root.join(&key_b), epoch_plus(200));
    let leased_a = ctx.plan(&props_a).expect("lease A");
    let leased_b = ctx.plan(&props_b).expect("lease B");

    // total 100 > budget 40, but BOTH dirs are leased → refuse, evict nothing.
    let outcome = ctx
        .evict_warm_dirs_over_budget(40)
        .expect("eviction must succeed (refusing, not erroring)");

    assert!(root.join(&key_a).exists(), "leased dir A not evicted");
    assert!(root.join(&key_b).exists(), "leased dir B not evicted");
    assert_eq!(outcome.dirs_evicted, 0, "no live dir may be evicted");
    assert_eq!(outcome.leased_skipped, 2);
    assert!(
        outcome.still_over_budget,
        "over budget with all-leased ⇒ backpressure signal, never a live eviction"
    );
    assert_eq!(outcome.bytes_remaining, 100);
    drop(leased_a);
    drop(leased_b);
}

#[nativelink_test]
async fn sweep_and_eviction_are_no_op_when_flag_off() {
    let (_td, root) = canonical_tempdir();
    // A provisioned-but-DISABLED context (the kill-switch state).
    let provision = PortableIncrProvision {
        fixed_prefix: root.clone(),
    };
    let config = PortableIncrConfig {
        enabled: false,
        action_output_allowlist: vec!["off-test/".to_string()],
    };
    let ctx =
        PortableIncrContext::from_provision(Some(provision), config).expect("context builds");

    // A contender orphan and an over-budget warm dir on disk.
    let contender = root.join(format!("{}.{}", "c".repeat(64), "d".repeat(32)));
    fs::create_dir_all(&contender).expect("mk contender");
    let (key_w, _) = carrier_props("off-test/uniq-w/libfoo.rlib");
    make_warm_dir(&root, &key_w, 5_000);

    assert_eq!(
        ctx.sweep_stale_contender_dirs().expect("sweep off"),
        0,
        "flag OFF ⇒ sweep is inert (removes nothing)"
    );
    assert_eq!(
        ctx.evict_warm_dirs_over_budget(1).expect("evict off"),
        EvictionOutcome::default(),
        "flag OFF ⇒ eviction is inert (default outcome, no filesystem touch)"
    );
    assert!(contender.exists(), "flag OFF ⇒ contender orphan untouched");
    assert!(
        root.join(&key_w).exists(),
        "flag OFF ⇒ over-budget warm dir untouched"
    );
}

/// The static budget default equals the design v4 §8 carve-out ceiling (20 GiB)
/// verified at the DECLARATION site — a doc-comment/metric could carry any
/// number; only this literal is authoritative.
#[nativelink_test]
async fn default_warm_dir_budget_is_20_gib() {
    assert_eq!(
        DEFAULT_WARM_DIR_BUDGET_BYTES,
        20 * 1024 * 1024 * 1024,
        "design v4 §8 static reservation = 20 GiB (≥9 GiB over the 10.9 GB full-CI-widen worst case)"
    );
}

// -- §8 WIRING: the call-sites this chunk adds -------------------------------
//
// The §8 API correctness is pinned above. These pin the CALL-SITE contracts:
//  - `portable_incr_startup_sweep` is the exact unit `new_local_worker` invokes
//    ONCE before accepting actions (gated on `Some(context)` = the fleet is
//    `None`); a failure must be swallowed.
//  - `RunningActionImpl::maybe_evict_warm_dirs_post_action` is the exact unit
//    `cleanup()` invokes after a PORTABLE action completes (gated on both this
//    action being portable AND a live context installed on the manager).

#[nativelink_test]
async fn startup_sweep_wiring_reaps_contender_when_context_present() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["sweep-wire/"]);

    let contender = root.join(format!("{}.{}", "a".repeat(64), "b".repeat(32)));
    fs::create_dir_all(contender.join("junk")).expect("mk contender");

    // The exact call `new_local_worker` makes once at startup.
    portable_incr_startup_sweep(Some(ctx)).await;

    assert!(
        !contender.exists(),
        "startup wiring MUST invoke the sweep when a live context is installed"
    );
}

#[nativelink_test]
async fn startup_sweep_wiring_is_inert_when_context_none() {
    let (_td, root) = canonical_tempdir();
    let contender = root.join(format!("{}.{}", "a".repeat(64), "b".repeat(32)));
    fs::create_dir_all(contender.join("junk")).expect("mk contender");

    // `None` is the whole-fleet state: no `spawn_blocking`, no disk touch.
    portable_incr_startup_sweep(None).await;

    assert!(
        contender.exists(),
        "startup wiring MUST NOT sweep when the context is None (the fleet default)"
    );
}

/// Install `ctx` on a freshly-built manager, mirroring `new_local_worker`
/// (which calls `set_portable_incr` before Arc-wrapping the manager).
async fn setup_manager_with_portable(
    ctx: Option<PortableIncrContext>,
) -> Arc<RunningActionsManagerImpl> {
    let mut manager = setup_manager().await;
    Arc::get_mut(&mut manager)
        .expect("freshly-built manager is uniquely owned")
        .set_portable_incr(ctx);
    manager
}

#[nativelink_test]
async fn post_action_evict_wiring_fires_for_portable_action() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["evict-wire/"]);
    // Two warm owner dirs; A is the LRU.
    let (key_a, _) = carrier_props("evict-wire/uniq-a/libfoo.rlib");
    let (key_b, _) = carrier_props("evict-wire/uniq-b/libfoo.rlib");
    make_warm_dir(&root, &key_a, 50);
    make_warm_dir(&root, &key_b, 50);
    set_dir_mtime(&root.join(&key_a), epoch_plus(100));
    set_dir_mtime(&root.join(&key_b), epoch_plus(200));

    // A portable action on a manager carrying the live context.
    let manager = setup_manager_with_portable(Some(ctx.clone())).await;
    let (_key, props) = carrier_props("evict-wire/uniq-live/libbar.rlib");
    let plan = ctx.plan(&props).expect("allowlisted ⇒ portable");
    let action = make_action(manager, &rand_temp("action_dir"), Some(plan));

    // total 100 > budget 60 → the post-action wiring evicts exactly the LRU (A).
    action.maybe_evict_warm_dirs_post_action(60).await;

    assert!(
        !root.join(&key_a).exists(),
        "post-action wiring MUST invoke eviction for a portable action (LRU warm dir removed)"
    );
    assert!(
        root.join(&key_b).exists(),
        "the newer warm dir stays within budget"
    );
}

#[nativelink_test]
async fn post_action_evict_wiring_skips_non_portable_action() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["evict-skip/"]);
    let (key_w, _) = carrier_props("evict-skip/uniq-w/libfoo.rlib");
    make_warm_dir(&root, &key_w, 5_000);

    // A NON-portable action (portable_execroot = None) on a portable-enabled
    // manager: the `portable_execroot.is_some()` gate MUST skip eviction.
    let manager = setup_manager_with_portable(Some(ctx)).await;
    let action = make_action(manager, &rand_temp("action_dir"), None);

    action.maybe_evict_warm_dirs_post_action(1).await;

    assert!(
        root.join(&key_w).exists(),
        "a NON-portable action must NOT trigger warm-dir eviction even over budget"
    );
}

#[nativelink_test]
async fn post_action_evict_wiring_is_inert_when_context_none() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["evict-none/"]);
    let (key_w, _) = carrier_props("evict-none/uniq-w/libfoo.rlib");
    make_warm_dir(&root, &key_w, 5_000);

    // A portable action, but the manager carries NO context (the fleet default).
    let manager = setup_manager_with_portable(None).await;
    let (_key, props) = carrier_props("evict-none/uniq-live/libbar.rlib");
    let plan = ctx.plan(&props).expect("plan");
    let action = make_action(manager, &rand_temp("action_dir"), Some(plan));

    action.maybe_evict_warm_dirs_post_action(1).await;

    assert!(
        root.join(&key_w).exists(),
        "context None (fleet default) ⇒ no eviction even for a portable action over budget"
    );
}

// -- §6.2/§6.3 WIRING: the fetch + publish call-sites this chunk adds ---------
//
// The building blocks (`plan_seed_publish`, `fetch_and_materialize_seed`) are
// unit-tested in `incr_seed_fetch.rs`; the §8 eviction/sweep wiring is pinned
// above. These two tests drive the REAL `RunningActionImpl` + manager
// composition end-to-end (`create_and_add_action` → `prepare_action` → …) with
// the feature ENABLED and an `incr_seed_index_store` installed — the exact
// wiring `new_local_worker` performs — so a mutation to the publish gate
// (`inner_upload_results`) or the fetch call (`inner_prepare_action`) is caught.

/// A process-global `IncrSeedMetrics` counter read (design §12 singleton).
fn incr_index_publish_count() -> u64 {
    incr_seed_metrics()
        .incr_index_publish
        .load(core::sync::atomic::Ordering::Relaxed)
}

/// Convert carrier platform properties into the REAPI `Platform` proto the
/// scheduler attaches to the `Action` (round-tripped into
/// `action_info.platform_properties`, which `plan_portable_execroot` reads).
fn platform_from_props(props: &HashMap<String, String>) -> Platform {
    Platform {
        properties: props
            .iter()
            .map(|(name, value)| Property {
                name: name.clone(),
                value: value.clone(),
            })
            .collect(),
    }
}

/// Blake3 `DigestInfo` for `bytes` — the seed path is blake3-keyed and blake3-
/// verified, so the pre-seeded `Tree`/blobs must be keyed the same way.
fn blake3_digest(bytes: &[u8]) -> DigestInfo {
    let mut hasher = DigestHasherFunc::Blake3.hasher();
    hasher.update(bytes);
    hasher.finalize_digest()
}

async fn put_blob(cas: &Store, digest: DigestInfo, bytes: Vec<u8>) {
    cas.update_oneshot(StoreKey::Digest(digest), bytes.into())
        .await
        .expect("cas blob write");
}

/// Install the portable context AND the seed index store on a freshly-built
/// manager (the exact pair `new_local_worker` sets before `Arc`-wrapping), and
/// return it alongside the CAS store the action protos must be uploaded to.
async fn setup_portable_manager_with_index(
    ctx: PortableIncrContext,
    index_store: Store,
) -> (Arc<RunningActionsManagerImpl>, Arc<FastSlowStore>) {
    let (mut manager, cas) = setup_manager_and_cas().await;
    {
        let m = Arc::get_mut(&mut manager).expect("freshly-built manager is uniquely owned");
        m.set_portable_incr(Some(ctx));
        m.set_incr_seed_index_store(Some(index_store));
    }
    (manager, cas)
}

/// Upload `command` + an empty input root + a wrapping `Action` (carrying
/// `platform`) into `cas`, returning the `StartExecute` the scheduler feeds to
/// `create_and_add_action`.
async fn upload_start_execute(
    cas: &Arc<FastSlowStore>,
    command: &Command,
    platform: Platform,
) -> StartExecute {
    let command_digest =
        serialize_and_upload_message(command, cas.as_pin(), &mut DigestHasherFunc::Sha256.hasher())
            .await
            .expect("upload command");
    let input_root_digest = serialize_and_upload_message(
        &Directory::default(),
        cas.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await
    .expect("upload input root");
    let action = Action {
        command_digest: Some(command_digest.into()),
        input_root_digest: Some(input_root_digest.into()),
        platform: Some(platform),
        ..Default::default()
    };
    let action_digest =
        serialize_and_upload_message(&action, cas.as_pin(), &mut DigestHasherFunc::Sha256.hasher())
            .await
            .expect("upload action");
    StartExecute {
        execute_request: Some(ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        }),
        operation_id: OperationId::default().to_string(),
        queued_timestamp: None,
        platform: action.platform.clone(),
        worker_id: "test-worker".to_string(),
        resolved_directories: Vec::new(),
        resolved_directory_digests: Vec::new(),
        missing_digests: Vec::new(),
        missing_digest_peers: Vec::new(),
    }
}

/// (a) A SUCCESSFUL portable action drives the `inner_upload_results` publish
/// gate: the index store gets an `update_oneshot` at `hash(targetkey)` AND the
/// §12 `incr_index_publish` counter increments (via `note_index_published`).
#[nativelink_test]
async fn portable_action_success_publishes_seed_index_and_bumps_counter() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["publish-wire/"]);
    let index_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, cas) =
        setup_portable_manager_with_index(ctx, index_store.clone()).await;

    // §2 exclusion at production composition: the `-incr` seed dir is a SAME-STEM
    // sibling (`aaa-incr`) that — because `-` (0x2D) < `.` (0x2E) — would sort
    // BEFORE `aaa.rlib` and mis-derive the key onto the config-blind `-incr` dir.
    // The worker-side `TargetKey::derive` EXCLUDES it (basename contains `-incr`),
    // so the derived key stays on `aaa.rlib` and agrees with the carrier — the §11
    // verify. (Pre-fix, this required an artificially later name like `zzz-incr`.)
    let primary = "publish-wire/aaa.rlib";
    let (_key, props) = carrier_props(primary);
    let tk = TargetKey::derive(&[primary.to_string()]).expect("targetkey derives");

    // A real action that SUCCEEDS and produces the primary output AND its `-incr`
    // seed dir (the folder `plan_seed_publish` selects when deciding to publish).
    let command = Command {
        arguments: vec![
            "sh".to_string(),
            "-c".to_string(),
            "mkdir -p publish-wire && : > publish-wire/aaa.rlib && \
             mkdir -p publish-wire/aaa-incr && printf seed > publish-wire/aaa-incr/dep-graph"
                .to_string(),
        ],
        output_paths: vec![
            "publish-wire/aaa.rlib".to_string(),
            "publish-wire/aaa-incr".to_string(),
        ],
        working_directory: ".".to_string(),
        environment_variables: vec![EnvironmentVariable {
            name: "PATH".to_string(),
            value: std::env::var("PATH").unwrap_or_default(),
        }],
        ..Default::default()
    };
    let start_execute = upload_start_execute(&cas, &command, platform_from_props(&props)).await;

    // The index KEY is a pure function of the targetkey (independent of the
    // `-incr` tree digest), so a throwaway `plan_seed_publish` yields the exact
    // key the production publish site writes under — without the private
    // `index_action_digest`.
    let index_key = plan_seed_publish(
        &tk,
        0,
        false,
        std::iter::once(("x-incr", DigestInfo::new([7u8; 32], 1))),
    )
    .expect("plan yields the index key")
    .index_digest;

    let publish_before = incr_index_publish_count();

    let action = manager
        .create_and_add_action("test-worker".to_string(), start_execute)
        .await
        .expect("portable action admitted");
    action
        .clone()
        .prepare_action()
        .await
        .expect("prepare_action")
        .execute()
        .await
        .expect("execute")
        .upload_results()
        .await
        .expect("upload_results");

    // (a1) the REAL production publish site landed an index entry at hash(targetkey).
    let stored =
        get_and_decode_digest::<ProtoActionResult>(&index_store, StoreKey::Digest(index_key))
            .await
            .expect(
                "inner_upload_results publish gate MUST update_oneshot the seed index at \
                 hash(targetkey) after a successful portable action — no entry means the \
                 `if let (Some(publish), Some(index_store))` wire never fired",
            );
    assert_eq!(
        stored.output_directories.first().map(|d| d.path.as_str()),
        Some(tk.primary_output()),
        "published index value's output_directories[0].path MUST be the primary output \
         (the chunk-3 fetch collision guard keys on it)"
    );

    // (a2) `note_index_published()` bumped the §12 publish counter by exactly 1.
    let publish_after = incr_index_publish_count();
    assert_eq!(
        publish_after,
        publish_before + 1,
        "note_index_published() MUST increment incr_index_publish exactly once per successful \
         portable publish — got delta {} (before {publish_before}, after {publish_after}); a \
         dropped note_index_published leaves the publish DARK on /metrics",
        publish_after.wrapping_sub(publish_before),
    );

    action.cleanup().await.expect("cleanup");
}

/// Process-global `incr_reuse_fired` counter read (design §12 singleton).
fn incr_reuse_fired_count() -> u64 {
    incr_seed_metrics()
        .incr_reuse_fired
        .load(core::sync::atomic::Ordering::Relaxed)
}

/// Drive a portable action end-to-end (`create_and_add_action` → prepare →
/// execute → upload_results → cleanup); its shell `script` produces `outputs`.
/// Mirrors `portable_action_success_publishes_seed_index_and_bumps_counter`,
/// parameterized so the three reuse-marker cases share one production
/// composition (real `RunningActionImpl` + portable manager).
async fn drive_portable_action(
    manager: &Arc<RunningActionsManagerImpl>,
    cas: &Arc<FastSlowStore>,
    primary: &str,
    script: &str,
    outputs: Vec<String>,
) {
    let (_key, props) = carrier_props(primary);
    // Non-empty `working_directory`: `output_paths` are relative to it, so the
    // marker's on-disk path is `{execroot}/wd/{output_path}`. This pins the
    // working_directory-aware join in the production read — an execroot-only join
    // (the wd-dropping bug) would read `{execroot}/{output_path}` → NotFound →
    // dark reuse-collapse. (Contrast the collapse of `""`/`"."` the seed_dest_dir
    // unit tests already cover.)
    let command = Command {
        arguments: vec!["sh".to_string(), "-c".to_string(), script.to_string()],
        output_paths: outputs,
        working_directory: "wd".to_string(),
        environment_variables: vec![EnvironmentVariable {
            name: "PATH".to_string(),
            value: std::env::var("PATH").unwrap_or_default(),
        }],
        ..Default::default()
    };
    let start_execute = upload_start_execute(cas, &command, platform_from_props(&props)).await;
    let action = manager
        .create_and_add_action("test-worker".to_string(), start_execute)
        .await
        .expect("portable action admitted");
    action
        .clone()
        .prepare_action()
        .await
        .expect("prepare_action")
        .execute()
        .await
        .expect("execute")
        .upload_results()
        .await
        .expect("upload_results");
    action.cleanup().await.expect("cleanup");
}

/// FL-1383 chunk 4 (design §12): the worker reads THIS action's own declared
/// `<label>-incr-reuse` marker (a SIBLING of the `-incr` tree the client's
/// process_wrapper always writes: content `1` = rustc incremental reuse fired,
/// `0` = cold) at the `Command.working_directory`-aware declared path, and bumps
/// the process-singleton `incr_reuse_fired` counter ONLY when the trimmed
/// content is `1`. All three drive the REAL portable composition end-to-end so a
/// mutation to the read/gate/counter in `inner_upload_results` is caught.
///
/// The three cases live in ONE test (sequential) because `incr_reuse_fired` is a
/// PROCESS-global singleton: splitting them would race a content-`1` bump against
/// a concurrent sibling's before/after read (the `0`/absent cases assert NO bump).
/// Only a portable action declaring an `-incr-reuse` output ever touches this
/// counter, so within one sequential test the deltas are exact.
#[nativelink_test]
async fn portable_action_reuse_marker_bumps_incr_reuse_fired_only_on_content_1() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["reuse-wire/"]);
    let index_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, cas) = setup_portable_manager_with_index(ctx, index_store).await;

    // (1) marker content `1` (with a trailing newline, exercising the trim) →
    // reuse fired → `incr_reuse_fired` increments by exactly 1.
    let before_one = incr_reuse_fired_count();
    drive_portable_action(
        &manager,
        &cas,
        "reuse-wire/one.rlib",
        "mkdir -p reuse-wire && : > reuse-wire/one.rlib && echo 1 > reuse-wire/one-incr-reuse",
        vec![
            "reuse-wire/one.rlib".to_string(),
            "reuse-wire/one-incr-reuse".to_string(),
        ],
    )
    .await;
    let after_one = incr_reuse_fired_count();
    assert_eq!(
        after_one,
        before_one + 1,
        "a portable action whose `-incr-reuse` marker reads `1` MUST increment incr_reuse_fired \
         exactly once — got delta {} (before {before_one}, after {after_one}); a dropped read/bump \
         leaves rustc-reuse DARK on /metrics (the canary this chunk lights)",
        after_one.wrapping_sub(before_one),
    );

    // (2) marker content `0` → cold → NO increment (the `0` case is implicit in
    // `materialized − reuse`).
    let before_zero = incr_reuse_fired_count();
    drive_portable_action(
        &manager,
        &cas,
        "reuse-wire/two.rlib",
        "mkdir -p reuse-wire && : > reuse-wire/two.rlib && printf 0 > reuse-wire/two-incr-reuse",
        vec![
            "reuse-wire/two.rlib".to_string(),
            "reuse-wire/two-incr-reuse".to_string(),
        ],
    )
    .await;
    let after_zero = incr_reuse_fired_count();
    assert_eq!(
        after_zero, before_zero,
        "a `-incr-reuse` marker reading `0` (cold) MUST NOT increment incr_reuse_fired — \
         got delta {} (before {before_zero}, after {after_zero})",
        after_zero.wrapping_sub(before_zero),
    );

    // (3) NO `-incr-reuse` output declared (pre-chunk-4 / non-producing action) →
    // NO increment; the read is inert and the counter stays put.
    let before_absent = incr_reuse_fired_count();
    drive_portable_action(
        &manager,
        &cas,
        "reuse-wire/three.rlib",
        "mkdir -p reuse-wire && : > reuse-wire/three.rlib",
        vec!["reuse-wire/three.rlib".to_string()],
    )
    .await;
    let after_absent = incr_reuse_fired_count();
    assert_eq!(
        after_absent, before_absent,
        "a portable action declaring NO `-incr-reuse` output MUST NOT increment incr_reuse_fired \
         — got delta {} (before {before_absent}, after {after_absent})",
        after_absent.wrapping_sub(before_absent),
    );
}

/// (b) A portable action whose targetkey has a PRE-SEEDED index entry drives the
/// `inner_prepare_action` fetch block: the `-incr` seed is materialized at the
/// action's DECLARED NESTED `-incr` output path (§3 option A) — proving both that
/// the fetch call actually fired AND that it lands at the nested declared path
/// whose parent [C] output-dir prep created (not a top-level `<stem>-incr`).
#[nativelink_test]
async fn portable_action_with_seeded_index_fetches_and_materializes() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["fetch-wire/"]);
    let index_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, cas) =
        setup_portable_manager_with_index(ctx, index_store.clone()).await;

    let primary = "fetch-wire/aaa.rlib";
    // The action's declared outputs: the `.rlib` AND its NESTED `-incr` seed dir
    // (a sibling under the same package). `derive` excludes the `-incr` basename,
    // so the targetkey still keys on the `.rlib` (matches `carrier_props`).
    let incr_output = "fetch-wire/aaa-incr";
    let output_paths = vec![primary.to_string(), incr_output.to_string()];
    let (_key, props) = carrier_props(primary);
    let tk = TargetKey::derive(&output_paths).expect("targetkey derives");
    assert_eq!(
        tk.primary_output(),
        primary,
        "targetkey must key on the .rlib after -incr exclusion, matching the carrier"
    );

    // Pre-seed CAS with a flat `-incr` `Tree` (blake3-keyed, as the seed path
    // verifies), then pre-seed the index to point `primary` at that Tree. Using
    // `plan_seed_publish` to shape BOTH the index key and value keeps this on the
    // public surface (no private `index_action_digest`).
    let cas_store = Store::new(cas.clone());
    let file_content: &[u8] = b"incremental-artifact";
    let file_digest = blake3_digest(file_content);
    put_blob(&cas_store, file_digest, file_content.to_vec()).await;
    let tree = Tree {
        root: Some(Directory {
            files: vec![FileNode {
                name: "dep-graph.bin".to_string(),
                digest: Some(Digest::from(&file_digest)),
                is_executable: false,
                node_properties: None,
            }],
            directories: vec![],
            symlinks: vec![],
            node_properties: None,
        }),
        children: Vec::<Directory>::new(),
    };
    let tree_bytes = tree.encode_to_vec();
    let tree_digest = blake3_digest(&tree_bytes);
    put_blob(&cas_store, tree_digest, tree_bytes).await;

    let seeded = plan_seed_publish(&tk, 0, false, std::iter::once(("z-incr", tree_digest)))
        .expect("seed index value");
    index_store
        .update_oneshot(StoreKey::Digest(seeded.index_digest), seeded.encoded)
        .await
        .expect("pre-seed index");

    // The action declares BOTH the `.rlib` and its nested `-incr` output, so the
    // fetch's `seed_dest_dir` resolves the nested declared path and [C] creates
    // its parent (`fetch-wire/`) before the fetch runs.
    let command = Command {
        arguments: vec!["true".to_string()],
        output_paths: output_paths.clone(),
        working_directory: ".".to_string(),
        environment_variables: vec![EnvironmentVariable {
            name: "PATH".to_string(),
            value: std::env::var("PATH").unwrap_or_default(),
        }],
        ..Default::default()
    };
    let start_execute = upload_start_execute(&cas, &command, platform_from_props(&props)).await;

    let action = manager
        .create_and_add_action("test-worker".to_string(), start_execute)
        .await
        .expect("portable action admitted");
    // The execroot IS the byte-identical work dir; the fetch materializes the
    // seed at the DECLARED NESTED `-incr` output joined onto the execroot.
    let execroot = action.get_work_directory().to_string();
    // The command's working_directory is "." (collapses in the join); pass it so
    // the test computes the dest with the same formula the production call-site
    // now uses (`seed_dest_dir` is working_directory-aware, pair-a #2).
    let dest = seed_dest_dir(Path::new(&execroot), ".", &output_paths)
        .expect("seed dest from the declared nested -incr output");
    assert_eq!(
        dest,
        Path::new(&execroot).join(incr_output),
        "seed dest must be the nested declared -incr output, not a top-level <stem>-incr"
    );

    action
        .clone()
        .prepare_action()
        .await
        .expect("prepare_action");

    assert!(
        dest.join("dep-graph.bin").exists(),
        "inner_prepare_action MUST call fetch_and_materialize_seed after [B2]+[C]: a pre-seeded \
         index hit should materialize the `-incr` tree into {} — its absence means the fetch \
         call-site never fired (or the nested parent was not created)",
        dest.display(),
    );
    assert_eq!(
        fs::read(dest.join("dep-graph.bin")).expect("materialized seed file"),
        file_content,
        "the materialized seed content MUST be the blake3-verified CAS blob"
    );

    action.cleanup().await.expect("cleanup");
}

// -- FL-1383 ask #4: SeedOutcome → flag → child-env-carrier WIRING -------------
//
// The pure helper `portable_incr_seed_child_env(seeded, targetkey)` is unit-
// tested inside `running_actions_manager.rs`. These tests exercise the load-
// bearing WIRING the unit tests can't reach: the prepare-site `SeedOutcome`→flag
// mapping, the `state.portable_incr_seeded` stash, the `inner_execute` read, and
// the `command_builder.env` injection — end to end, by SPAWNING the real child
// and reading the env it actually received. The child writes the two reserved
// carrier vars (with a shell default of `<unset>`) into `carrier-env.txt` in its
// cwd (the execroot), which we read back after `execute()`.

/// Pre-seed CAS with a flat `-incr` `Tree` (blake3-keyed, as the seed path
/// verifies) and point the index at it for `tk`, so `fetch_and_materialize_seed`
/// resolves to `SeedOutcome::Materialized`. Mirrors the (b)-test pre-seed block.
async fn pre_seed_incr_index(index_store: &Store, cas: &Arc<FastSlowStore>, tk: &TargetKey) {
    let cas_store = Store::new(cas.clone());
    let file_content: &[u8] = b"incremental-artifact";
    let file_digest = blake3_digest(file_content);
    put_blob(&cas_store, file_digest, file_content.to_vec()).await;
    let tree = Tree {
        root: Some(Directory {
            files: vec![FileNode {
                name: "dep-graph.bin".to_string(),
                digest: Some(Digest::from(&file_digest)),
                is_executable: false,
                node_properties: None,
            }],
            directories: vec![],
            symlinks: vec![],
            node_properties: None,
        }),
        children: Vec::<Directory>::new(),
    };
    let tree_bytes = tree.encode_to_vec();
    let tree_digest = blake3_digest(&tree_bytes);
    put_blob(&cas_store, tree_digest, tree_bytes).await;
    let seeded = plan_seed_publish(tk, 0, false, std::iter::once(("z-incr", tree_digest)))
        .expect("seed index value");
    index_store
        .update_oneshot(StoreKey::Digest(seeded.index_digest), seeded.encoded)
        .await
        .expect("pre-seed index");
}

/// A `Command` whose child dumps the two reserved `-incr` carrier vars into
/// `carrier-env.txt` (in the execroot), each defaulting to `<unset>` via shell
/// parameter expansion so ABSENCE is observable, not just presence. Declares the
/// primary `.rlib` and its nested `-incr` output so the seed-fetch is attempted
/// (`seed_dest_dir` resolves). `extra_env` lets a test forge a CLIENT-supplied
/// reserved carrier to prove the worker strips it.
fn carrier_dump_command(
    primary: &str,
    incr_output: &str,
    extra_env: &[(&str, &str)],
) -> Command {
    let mut environment_variables = vec![EnvironmentVariable {
        name: "PATH".to_string(),
        value: std::env::var("PATH").unwrap_or_default(),
    }];
    for (name, value) in extra_env {
        environment_variables.push(EnvironmentVariable {
            name: (*name).to_string(),
            value: (*value).to_string(),
        });
    }
    Command {
        arguments: vec![
            "sh".to_string(),
            "-c".to_string(),
            "printf 'SEEDED=[%s]\\n' \"${NL_PORTABLE_INCR_SEEDED:-<unset>}\" > carrier-env.txt && \
             printf 'TARGETKEY=[%s]\\n' \"${NL_INCR_TARGETKEY:-<unset>}\" >> carrier-env.txt"
                .to_string(),
        ],
        output_paths: vec![primary.to_string(), incr_output.to_string()],
        working_directory: ".".to_string(),
        environment_variables,
        ..Default::default()
    }
}

/// Drive a portable action through the REAL composition
/// (`create_and_add_action` → `prepare_action` → `execute`) and return what the
/// SPAWNED CHILD wrote to `carrier-env.txt` — i.e. the env the child actually
/// received. Cleans up before returning.
async fn execute_and_read_carrier_dump(
    manager: &Arc<RunningActionsManagerImpl>,
    start_execute: StartExecute,
) -> String {
    let action = manager
        .create_and_add_action("test-worker".to_string(), start_execute)
        .await
        .expect("portable action admitted");
    let execroot = action.get_work_directory().to_string();
    action
        .clone()
        .prepare_action()
        .await
        .expect("prepare_action")
        .execute()
        .await
        .expect("execute");
    let dump = fs::read_to_string(Path::new(&execroot).join("carrier-env.txt")).expect(
        "the spawned child MUST have written carrier-env.txt into the execroot — its absence \
         means the child never ran or wrote to the wrong cwd",
    );
    action.cleanup().await.expect("cleanup");
    dump
}

/// FL-1383 ask #4 — the SeedOutcome→flag→injection WIRING, end to end, for the
/// SAME action run seeded on one worker and cold on another:
///   (a) `Materialized`  → child env has `NL_PORTABLE_INCR_SEEDED=1` AND
///       `NL_INCR_TARGETKEY=<TargetKey::key()>`;
///   (b) cold (index-miss) → child env has NEITHER;
///   (c) DIGEST NON-LEAK → the client action digest is BYTE-IDENTICAL between the
///       seeded and cold runs (the carrier never enters the action identity).
///
/// Mutation coverage (both restored):
///   - INVERT the prepare-site guard (`:5468`, `matches!(_, Materialized)` →
///     `!matches!`): the cold run then sets the flag and the seeded run clears it,
///     so (a) and (b) BOTH flip → RED. (The exact false-positive-on-cold the 3
///     shipped helper-only tests miss: guard inversion passes 220/220 there.)
///   - MOVE the injection into `command_proto.environment_variables` instead of
///     `command_builder`: pushed AFTER the action-env loop it never reaches the
///     child; pushed BEFORE it, the sole-authority strip removes it (reserved
///     name) — either way the child loses the carrier → (a) RED.
#[nativelink_test]
async fn portable_seed_wiring_injects_carrier_into_child_env_only_when_materialized() {
    let primary = "wire-e2e/aaa.rlib";
    let incr_output = "wire-e2e/aaa-incr";
    let (_key, props) = carrier_props(primary);
    let tk = TargetKey::derive(&[primary.to_string(), incr_output.to_string()])
        .expect("targetkey derives");
    let command = carrier_dump_command(primary, incr_output, &[]);

    // Seeded worker: its index HAS the entry → prepare materializes → flag true.
    let (_td_s, root_s) = canonical_tempdir();
    let index_seeded = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager_seeded, cas_seeded) = setup_portable_manager_with_index(
        enabled_context(&root_s, &["wire-e2e/"]),
        index_seeded.clone(),
    )
    .await;
    pre_seed_incr_index(&index_seeded, &cas_seeded, &tk).await;
    let se_seeded = upload_start_execute(&cas_seeded, &command, platform_from_props(&props)).await;

    // Cold worker: EMPTY index → prepare's fetch misses → NoSeed → flag false.
    let (_td_c, root_c) = canonical_tempdir();
    let index_cold = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager_cold, cas_cold) =
        setup_portable_manager_with_index(enabled_context(&root_c, &["wire-e2e/"]), index_cold)
            .await;
    let se_cold = upload_start_execute(&cas_cold, &command, platform_from_props(&props)).await;

    // (c) DIGEST NON-LEAK: the client action digest is a pure function of the
    // Action/Command bytes and is IDENTICAL for the seeded and cold runs — seeding
    // (a pre-seeded index, external to the Action) does not perturb the action
    // identity. This is the make-or-break the carrier must never break: the vars
    // live on `command_builder` (the child), never on `command_proto` (the digest).
    assert_eq!(
        se_seeded
            .execute_request
            .as_ref()
            .and_then(|r| r.action_digest.clone()),
        se_cold
            .execute_request
            .as_ref()
            .and_then(|r| r.action_digest.clone()),
        "the SAME action must have a BYTE-IDENTICAL action digest whether seeded or cold — a \
         differing digest means the carrier leaked into the REAPI Command / action identity",
    );

    let seeded_dump = execute_and_read_carrier_dump(&manager_seeded, se_seeded).await;
    let cold_dump = execute_and_read_carrier_dump(&manager_cold, se_cold).await;

    // (a) Materialized → both carriers present with the exact worker values.
    assert!(
        seeded_dump.contains("SEEDED=[1]"),
        "a Materialized seed MUST inject NL_PORTABLE_INCR_SEEDED=1 into the SPAWNED CHILD so \
         process_wrapper skips its local-tool seed path — child dump was:\n{seeded_dump}",
    );
    assert!(
        seeded_dump.contains(&format!("TARGETKEY=[{}]", tk.key())),
        "a Materialized seed MUST inject NL_INCR_TARGETKEY=<TargetKey::key()> ({}) into the \
         spawned child — child dump was:\n{seeded_dump}",
        tk.key(),
    );

    // (b) cold → NEITHER carrier present (both read the `<unset>` default).
    assert!(
        cold_dump.contains("SEEDED=[<unset>]"),
        "a COLD outcome MUST NOT inject NL_PORTABLE_INCR_SEEDED — a false positive would make \
         process_wrapper skip a seed that is not present → a wrong/cold-slow build; child dump \
         was:\n{cold_dump}",
    );
    assert!(
        cold_dump.contains("TARGETKEY=[<unset>]"),
        "a COLD outcome MUST NOT inject NL_INCR_TARGETKEY either — child dump was:\n{cold_dump}",
    );
}

/// FL-1383 ask #4 (sole-authority hardening): the worker is the ONLY authority
/// for the reserved carrier names. A COLD portable action whose CLIENT env forges
/// `NL_PORTABLE_INCR_SEEDED=1` (+ a bogus `NL_INCR_TARGETKEY`) must have BOTH
/// STRIPPED from the child env — the worker injects nothing on cold, so an
/// un-stripped client `=1` would make process_wrapper skip a non-existent seed.
///
/// Mutation: remove the `is_reserved_incr_env` strip in the action-env loop → the
/// forged `NL_PORTABLE_INCR_SEEDED=client-forged-1` survives into the child →
/// `SEEDED=[<unset>]` assertion RED.
#[nativelink_test]
async fn portable_cold_strips_client_supplied_reserved_carrier() {
    let primary = "strip-wire/aaa.rlib";
    let incr_output = "strip-wire/aaa-incr";
    let (_key, props) = carrier_props(primary);

    let (_td, root) = canonical_tempdir();
    let index_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, cas) =
        setup_portable_manager_with_index(enabled_context(&root, &["strip-wire/"]), index_store)
            .await;

    // COLD (empty index) + a CLIENT action that forges BOTH reserved carriers in
    // its own REAPI env. The worker must strip them: it is the sole authority.
    let command = carrier_dump_command(
        primary,
        incr_output,
        &[
            ("NL_PORTABLE_INCR_SEEDED", "client-forged-1"),
            ("NL_INCR_TARGETKEY", "client-forged-key"),
        ],
    );
    let se = upload_start_execute(&cas, &command, platform_from_props(&props)).await;
    let dump = execute_and_read_carrier_dump(&manager, se).await;

    assert!(
        dump.contains("SEEDED=[<unset>]"),
        "the worker MUST strip a CLIENT-supplied NL_PORTABLE_INCR_SEEDED from a portable action's \
         env (worker is sole authority): on a cold outcome the child must see NO seeded signal, \
         else process_wrapper skips a non-existent seed → wrong/cold build; child dump was:\n{dump}",
    );
    assert!(
        dump.contains("TARGETKEY=[<unset>]"),
        "the worker MUST also strip a CLIENT-supplied NL_INCR_TARGETKEY — child dump was:\n{dump}",
    );
    assert!(
        !dump.contains("client-forged"),
        "no client-forged reserved carrier value may reach the child — child dump was:\n{dump}",
    );
}

/// (c) INERTNESS BY ASSERTION (pair-b): a NON-portable action (no carrier props →
/// `portable_execroot == None` → `portable_targetkey` never stashed, stays `None`)
/// on a portable-ENABLED manager with an index store installed AND a matching
/// pre-seeded index entry MUST NOT fetch/materialize the seed — the
/// `if let (Some(index_store), Some(targetkey))` gate is skipped because
/// `portable_targetkey` is `None`. This is the exact A/B partner of the (b) test
/// above (identical pre-seed + output_paths); (b) proves a portable action
/// MATERIALIZES the seed, this proves the non-portable action does NOT — so the
/// non-materialization is the gate's inertness, not a vacuous miss.
#[nativelink_test]
async fn non_portable_action_leaves_targetkey_none_and_skips_seed_fetch() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["inert-wire/"]);
    let index_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (manager, cas) = setup_portable_manager_with_index(ctx, index_store.clone()).await;

    let primary = "inert-wire/aaa.rlib";
    let incr_output = "inert-wire/aaa-incr";
    let output_paths = vec![primary.to_string(), incr_output.to_string()];
    let tk = TargetKey::derive(&output_paths).expect("targetkey derives");

    // Pre-seed CAS + index EXACTLY as the (b) test does, so the ONLY difference
    // between materialize-vs-not is the action's portability.
    let cas_store = Store::new(cas.clone());
    let file_content: &[u8] = b"incremental-artifact";
    let file_digest = blake3_digest(file_content);
    put_blob(&cas_store, file_digest, file_content.to_vec()).await;
    let tree = Tree {
        root: Some(Directory {
            files: vec![FileNode {
                name: "dep-graph.bin".to_string(),
                digest: Some(Digest::from(&file_digest)),
                is_executable: false,
                node_properties: None,
            }],
            directories: vec![],
            symlinks: vec![],
            node_properties: None,
        }),
        children: Vec::<Directory>::new(),
    };
    let tree_bytes = tree.encode_to_vec();
    let tree_digest = blake3_digest(&tree_bytes);
    put_blob(&cas_store, tree_digest, tree_bytes).await;
    let seeded = plan_seed_publish(&tk, 0, false, std::iter::once(("z-incr", tree_digest)))
        .expect("seed index value");
    index_store
        .update_oneshot(StoreKey::Digest(seeded.index_digest), seeded.encoded)
        .await
        .expect("pre-seed index");

    // NON-portable: an EMPTY platform carries no `nl_incr_*` carrier props, so
    // `plan_portable_execroot` returns None ⇒ portable_execroot None ⇒ the [B1]
    // derive-and-stash block never runs ⇒ portable_targetkey stays None.
    let command = Command {
        arguments: vec!["true".to_string()],
        output_paths: output_paths.clone(),
        working_directory: ".".to_string(),
        environment_variables: vec![EnvironmentVariable {
            name: "PATH".to_string(),
            value: std::env::var("PATH").unwrap_or_default(),
        }],
        ..Default::default()
    };
    let start_execute = upload_start_execute(&cas, &command, Platform::default()).await;

    let action = manager
        .create_and_add_action("test-worker".to_string(), start_execute)
        .await
        .expect("non-portable action admitted");
    let work_dir = action.get_work_directory().to_string();

    action
        .clone()
        .prepare_action()
        .await
        .expect("prepare_action");

    // The seed's would-be destination (same formula the fetch uses). For a
    // non-portable action the fetch gate is skipped, so nothing materializes here.
    let dest = seed_dest_dir(Path::new(&work_dir), ".", &output_paths)
        .expect("seed dest resolves from the declared nested -incr output");
    assert!(
        !dest.join("dep-graph.bin").exists(),
        "a NON-portable action (portable_targetkey None) MUST NOT fetch/materialize the \
         pre-seeded `-incr` tree — the fetch gate `if let (Some(index_store), Some(targetkey))` \
         must be skipped; a materialized {} means the gate fired without a portable execroot",
        dest.join("dep-graph.bin").display(),
    );

    action.cleanup().await.expect("cleanup");
}
