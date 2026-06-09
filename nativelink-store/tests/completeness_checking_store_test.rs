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

use std::sync::Arc;
use std::time::Duration;

use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, Directory, DirectoryNode, FileNode, OutputDirectory,
    OutputFile, Tree,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};

const ROOT_FILE: DigestInfo = DigestInfo::new([0u8; 32], 0);
const ROOT_DIRECTORY: DigestInfo = DigestInfo::new([1u8; 32], 0);
const CHILD_FILE: DigestInfo = DigestInfo::new([2u8; 32], 0);
const OUTPUT_FILE: DigestInfo = DigestInfo::new([4u8; 32], 0);
const STDOUT: DigestInfo = DigestInfo::new([5u8; 32], 0);
const STDERR: DigestInfo = DigestInfo::new([6u8; 32], 0);

async fn setup() -> Result<(Arc<CompletenessCheckingStore>, Arc<MemoryStore>, DigestInfo), Error> {
    let backend_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let cas_store = MemoryStore::new(&MemorySpec::default());
    let ac_store =
        CompletenessCheckingStore::new(backend_store.clone(), Store::new(cas_store.clone()));

    cas_store.update_oneshot(ROOT_FILE, "".into()).await?;
    // Note: Explicitly not uploading `ROOT_DIRECTORY`. See: TraceMachina/nativelink#747.
    cas_store.update_oneshot(CHILD_FILE, "".into()).await?;
    cas_store.update_oneshot(OUTPUT_FILE, "".into()).await?;
    cas_store.update_oneshot(STDOUT, "".into()).await?;
    cas_store.update_oneshot(STDERR, "".into()).await?;

    let tree = Tree {
        root: Some(Directory {
            files: vec![FileNode {
                digest: Some(ROOT_FILE.into()),
                ..Default::default()
            }],
            directories: vec![DirectoryNode {
                digest: Some(ROOT_DIRECTORY.into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        children: vec![Directory {
            files: vec![FileNode {
                digest: Some(CHILD_FILE.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let tree_digest = serialize_and_upload_message(
        &tree,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await?;

    let output_directory = OutputDirectory {
        tree_digest: Some(tree_digest.into()),
        ..Default::default()
    };

    serialize_and_upload_message(
        &output_directory,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await?;

    let action_result = ProtoActionResult {
        output_files: vec![OutputFile {
            digest: Some(OUTPUT_FILE.into()),
            ..Default::default()
        }],
        output_directories: vec![output_directory],
        stdout_digest: Some(STDOUT.into()),
        stderr_digest: Some(STDERR.into()),
        ..Default::default()
    };

    // The structure of the action result is not following the spec, but is simplified for testing purposes.
    let action_result_digest = serialize_and_upload_message(
        &action_result,
        ac_store.as_pin(),
        &mut DigestHasherFunc::Blake3.hasher(),
    )
    .await?;

    Ok((ac_store, cas_store, action_result_digest))
}

#[nativelink_test]
async fn verify_has_function_call_checks_cas() -> Result<(), Error> {
    {
        // Completeness check should succeed when all digests exist in CAS.

        let (ac_store, _cas_store, action_result_digest) = setup().await?;

        let res = ac_store
            .has_many(&[action_result_digest.into()])
            .await
            .unwrap();
        assert!(
            res[0].is_some(),
            "Results should be some with all items in CAS."
        );
    }

    {
        // Completeness check should fail when root file digest is missing.

        let (ac_store, cas_store, action_result_digest) = setup().await?;

        cas_store.remove_entry(ROOT_FILE.into()).await;

        let res = ac_store
            .has_many(&[action_result_digest.into()])
            .await
            .unwrap();
        assert!(
            res[0].is_none(),
            "Results should be none with missing root file."
        );
    }

    {
        // Completeness check should fail when child file digest is missing.

        let (ac_store, cas_store, action_result_digest) = setup().await?;

        cas_store.remove_entry(CHILD_FILE.into()).await;
        let res = ac_store
            .has_many(&[action_result_digest.into()])
            .await
            .unwrap();
        assert!(
            res[0].is_none(),
            "Results should be none with missing root file."
        );
    }

    {
        // Completeness check should fail when output file digest is missing.

        let (ac_store, cas_store, action_result_digest) = setup().await?;

        cas_store.remove_entry(OUTPUT_FILE.into()).await;
        let res = ac_store
            .has_many(&[action_result_digest.into()])
            .await
            .unwrap();
        assert!(
            res[0].is_none(),
            "Results should be none with missing root file."
        );
    }

    {
        // Completeness check should fail when stdout digest is missing.

        let (ac_store, cas_store, action_result_digest) = setup().await?;

        cas_store.remove_entry(STDOUT.into()).await;
        let res = ac_store
            .has_many(&[action_result_digest.into()])
            .await
            .unwrap();
        assert!(
            res[0].is_none(),
            "Results should be none with missing root file."
        );
    }

    {
        // Completeness check should fail when stderr digest is missing.

        let (ac_store, cas_store, action_result_digest) = setup().await?;

        cas_store.remove_entry(STDERR.into()).await;
        let res = ac_store
            .has_many(&[action_result_digest.into()])
            .await
            .unwrap();
        assert!(
            res[0].is_none(),
            "Results should be none with missing root file."
        );
    }

    Ok(())
}

#[nativelink_test]
async fn verify_completeness_get() -> Result<(), Error> {
    {
        // Completeness check in get call should succeed when all digests exist in CAS.

        let (ac_store, _cas_store, action_result_digest) = setup().await?;

        assert!(
            ac_store
                .get_part_unchunked(action_result_digest, 0, None)
                .await
                .is_ok(),
            ".get() should succeed with all items in CAS",
        );
    }

    {
        // Completeness check in get call should fail when digest is missing in CAS.

        let (ac_store, cas_store, action_result_digest) = setup().await?;

        cas_store.remove_entry(OUTPUT_FILE.into()).await;

        assert!(
            ac_store
                .get_part_unchunked(action_result_digest, 0, None)
                .await
                .is_err(),
            ".get() should fail with item missing in CAS",
        );
    }

    Ok(())
}

/// Test A — under-action direction: incomplete branch MUST emit warn! with
/// the exact message and the missing CAS digest's hex hash.
///
/// Derives assertions from the SPEC (#1: CCS get_part incomplete-branch warn,
/// closing #40 RCA §5(1) observability gap). The warn! is NOT present in the
/// unmodified production code, so this test is expected to FAIL (red) until
/// the instrumentation is added.
///
/// Timeout = deadlock detector: the error path in get_and_verify_single must
/// terminate; a hang here means the error path is blocking.
#[nativelink_test]
async fn get_part_incomplete_emits_warn_with_missing_digest() -> Result<(), Error> {
    let (ac_store, cas_store, action_result_digest) = setup().await?;

    // Remove OUTPUT_FILE from CAS so the completeness check detects an
    // incomplete ActionResult on the get_part path.
    cas_store.remove_entry(OUTPUT_FILE.into()).await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        ac_store.get_part_unchunked(action_result_digest, 0, None),
    )
    .await
    .expect("must not deadlock — CCS get_part error path must terminate");

    // The error code and stable substring must both hold.
    let err = result.expect_err(
        "expected Err(NotFound) when referenced CAS digest is missing from CAS",
    );
    assert_eq!(
        err.code,
        Code::NotFound,
        "CCS get_part incomplete path must return Code::NotFound, got {:?}",
        err.code,
    );
    assert!(
        err.messages.iter().any(|m| m.contains("not all parts were found")),
        "error message must contain stable substring 'not all parts were found'; \
         got messages: {:?}",
        err.messages,
    );

    // Message assertion — the exact string the SPEC mandates.
    // This is the primary guard: if the warn! is absent entirely, this fails
    // with the bespoke "#40 §5(1) instrumentation absent" message.
    assert!(
        logs_contain("ActionResult incomplete — referenced CAS digest(s) missing (get_part path)"),
        "missing-digest warn did not fire on CCS get_part incomplete path — \
         #40 §5(1) instrumentation absent",
    );

    // Same-line compound check (canonical pattern:
    // fast_slow_block_b_log_visibility_test.rs:239): the WARN level, the
    // ac_key + missing_digests fields, and the missing digest's hex hash
    // (OUTPUT_FILE = DigestInfo::new([4u8; 32], 0)) must all appear on ONE
    // captured line. A bare `logs_contain("0404…")` would false-positive on
    // the DEBUG-level MemoryStore setup lines that also contain the hex, and
    // a bare `logs_contain("WARN")` would false-positive on any unrelated
    // warn — the same-line conjunction is immune to both AND catches a
    // warn!→debug! level mutation.
    logs_assert(|lines: &[&str]| {
        let n = lines
            .iter()
            .filter(|l| {
                l.contains(" WARN ")
                    && l.contains(
                        "ActionResult incomplete — referenced CAS digest(s) missing (get_part path)",
                    )
                    && l.contains("ac_key=")
                    && l.contains("missing_digests=")
                    && l.contains("0404040404040404")
            })
            .count();
        if n == 0 {
            Err("missing-digest warn must fire at WARN level with ac_key + \
                 missing_digests fields and the missing digest's hash on one \
                 line — per-digest attribution is the whole point of #40 §5(1)"
                .to_string())
        } else {
            Ok(())
        }
    });

    Ok(())
}

/// Test B — over-action direction: a COMPLETE ActionResult must NOT trigger
/// the incomplete-branch warn!.
///
/// Falsification: if the warn fires unconditionally (not just on the
/// incomplete branch), this test red-fails with bespoke message.
#[nativelink_test]
async fn get_part_complete_emits_no_incomplete_warn() -> Result<(), Error> {
    let (ac_store, _cas_store, action_result_digest) = setup().await?;

    // Nothing deleted — all CAS blobs present; completeness check must pass.
    tokio::time::timeout(
        Duration::from_secs(5),
        ac_store.get_part_unchunked(action_result_digest, 0, None),
    )
    .await
    .expect("must not deadlock — CCS get_part complete path must terminate")
    .expect("get_part must succeed when all CAS digests are present");

    assert!(
        !logs_contain("ActionResult incomplete — referenced CAS digest(s) missing (get_part path)"),
        "incomplete warn fired on a COMPLETE ActionResult — over-action contract violation",
    );

    Ok(())
}
