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
use nativelink_util::fs_util::hardlink_directory_tree;
use nativelink_util::store_trait::Store;
use nativelink_util::targetkey::TargetKey;
use nativelink_worker::portable_incr::{
    CARRIER_PRIMARY_OUTPUT_PROP, CARRIER_TARGETKEY_PROP, DEFAULT_WARM_DIR_BUDGET_BYTES,
    EvictionOutcome, ExecrootRole, PortableExecroot, PortableIncrContext, PortableIncrProvision,
    assert_under_prefix, ensure_and_wipe_execroot_at, is_incr_seed_entry,
};
use nativelink_worker::local_worker::portable_incr_startup_sweep;
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

// -- §7+B2 emergent property: -incr SURVIVES the wipe→materialize chain -------
//
// The whole warm-reuse benefit rests on an EMERGENT property (convergent
// distsys MAJOR-1 / red-team A1a): after the §7 wipe leaves `-incr` in place,
// the input-materialize step (`hardlink_directory_tree` → `try_clonefile` on
// macOS / hardlink path on Linux) must NOT recursively clear the pre-populated
// execroot, or it destroys the seed on EVERY build (a DARK perf regression —
// cold-not-wrong). No test locked this chain; every other test exercises
// `portable_incr` in isolation. This drives the REAL production materialize
// primitive against a seed-bearing warm execroot and asserts the seed (and its
// bytes) survive. A refactor of the materialize dst-clear to a RECURSIVE delete
// (fs_util.rs `try_clonefile` remove_dir→remove_dir_all on macOS; the Linux
// create_dir_all→destructive on this build box) turns this test RED.
#[nativelink_test]
async fn incr_seed_survives_wipe_then_materialize() {
    let (_td, root) = canonical_tempdir();
    let ctx = enabled_context(&root, &["materialize-test/"]);
    let (key, props) = carrier_props("materialize-test/uniq-m/libfoo.rlib");
    let plan = ctx.plan(&props).expect("owner");
    let execroot = root.join(&key);

    // A prior build's warm residue: a POPULATED `-incr` seed + stale outputs.
    fs::create_dir_all(execroot.join("libfoo-incr/dep-graph")).expect("mk incr");
    fs::write(execroot.join("libfoo-incr/dep-graph/x"), b"seed-bytes").expect("seed");
    fs::create_dir_all(execroot.join("bazel-out/bin")).expect("mk stale out");
    fs::write(execroot.join("bazel-out/bin/stale.rlib"), b"stale").expect("stale output");

    // B1: the §7 wipe — preserves `-incr`, empties the rest. Precondition for B2:
    // the execroot is now NON-EMPTY (it still holds `-incr`), which is exactly
    // the state whose non-recursive dst-clear the materialize silently depends on.
    plan.ensure_and_wipe_execroot().expect("wipe");
    assert!(
        execroot.join("libfoo-incr/dep-graph/x").exists(),
        "wipe must preserve the -incr seed (B1 precondition for this test)"
    );
    assert!(
        !execroot.join("bazel-out").exists(),
        "wipe must empty the stale non-incr output tree"
    );

    // B2: materialize fresh inputs INTO the warm, seed-bearing execroot via the
    // exact production primitive (`hardlink_directory_tree`).
    let src = root.join("inputs-src");
    fs::create_dir_all(src.join("bazel-out/bin")).expect("mk src");
    fs::write(src.join("bazel-out/bin/main.rs"), b"fn main(){}").expect("input");
    hardlink_directory_tree(&src, &execroot)
        .await
        .expect("materialize into the warm execroot");

    // The seed AND its bytes must survive the materialize; the fresh input landed.
    let seed = execroot.join("libfoo-incr/dep-graph/x");
    assert!(
        seed.exists(),
        "-incr seed dir DESTROYED by the input materialize (dst-clear went recursive?)"
    );
    assert_eq!(
        fs::read(&seed).expect("read seed"),
        b"seed-bytes",
        "-incr seed CONTENTS destroyed by the input materialize"
    );
    assert!(
        execroot.join("bazel-out/bin/main.rs").exists(),
        "fresh input must materialize into the warm execroot"
    );
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
