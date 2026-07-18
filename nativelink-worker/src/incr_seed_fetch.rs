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

//! FL-1383 portable rustc-incremental — worker out-of-band `-incr` seed fetch +
//! materialize (design `docs/portable-rustc-incremental-v4.md` §6.3, §3b, §7).
//!
//! This is the pure fetch+materialize step: given a [`TargetKey`], look the
//! current `-incr` seed up in the fleet-shared mutable **index** (an AC-shaped
//! store on the `incr_seed_index` instance), verify it is not a blake3
//! collision, read the referenced REAPI [`Tree`] from the **main CAS**, and
//! materialize the `-incr` directory tree at a caller-supplied destination —
//! verifying every file blob's digest as it is written, and never leaving a
//! partial directory behind on any failure.
//!
//! Everything here is **best-effort with a bounded index timeout** (§6.2): a
//! slow or absent index, an evicted seed, a collision, or a corrupt blob all
//! resolve to a *cold* outcome ([`SeedOutcome::NoSeed`] / [`SeedOutcome::Collision`]
//! / [`SeedOutcome::TimedOut`]) — never a wrong seed and never a build stall.
//! rustc's per-query fingerprint re-validation is the correctness floor (§6.4):
//! a stale/torn/wrong seed re-validates to a cold compile, never a wrong
//! `.rlib`.
//!
//! This function is wired into the execroot setup (`running_actions_manager`):
//! it runs before rustc, AFTER the full-empty execroot wipe (§7) AND after input
//! materialization + output-dir creation, so the nested declared `-incr` path's
//! parent exists. The full-empty wipe removes any prior `-incr`, so this function
//! always (re)materializes the `-incr` dir fresh at the declared nested path
//! ([`seed_dest_dir`]), and maps the returned [`SeedOutcome`] into the registered
//! worker metrics
//! (`incr_seed_materialized` / `incr_reuse_fired` / `incr_seed_present_but_cold`
//! / `incr_seed_collision`, and the `incr_index_fetch_{hit,miss,timeout,error}`
//! family — see the `emit_counter` markers below).

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use futures::StreamExt;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, OutputDirectory, Tree,
};
use nativelink_store::ac_utils::get_and_decode_digest;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use nativelink_util::targetkey::TargetKey;
use prost::Message;
use tracing::{debug, warn};

/// The index-AC key preimage prefix (design §3b, KAT-locked). The index digest
/// is `blake3_hex(INDEX_KEY_PREFIX ++ targetkey.key())` with `size_bytes` equal
/// to the preimage length (`22 + 64 = 86` for a v1 64-hex key). `v1:` is the
/// rotation knob.
const INDEX_KEY_PREFIX: &str = "fl-incr-seed-index:v1:";

/// Bounded fan-out for the per-file fetch+verify+write in [`materialize_tree`].
/// Up to this many `-incr` file blobs are fetched, blake3-verified, and written
/// concurrently; the remaining files queue until an in-flight slot frees. Each
/// blob is dropped after its write, so peak resident bytes are bounded by
/// `PARALLEL_MATERIALIZE_CONCURRENCY × the largest single -incr file` (NOT the
/// whole seed). `-incr` files are per-CGU rustc incremental artifacts
/// (individually small); 16× the largest is acceptable worker memory for the
/// wall-clock win of overlapping the (§6.5) main-CAS reads. Every per-file
/// safety guard (blake3 verify, `O_NOFOLLOW|O_EXCL` byte-copy) runs unchanged
/// inside each concurrent [`fetch_and_write_file`].
const PARALLEL_MATERIALIZE_CONCURRENCY: usize = 16;

/// The outcome of a seed fetch+materialize attempt.
///
/// Every non-[`Materialized`](SeedOutcome::Materialized) variant is a *cold*
/// build signal: correct, just without incremental reuse.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeedOutcome {
    /// The `-incr` directory was materialized at the destination and every file
    /// blob's digest verified. Carries the REAPI `Tree` digest that was
    /// materialized (for the §10 residency-gossip hook and observability).
    Materialized {
        /// The blake3 digest of the REAPI `Tree` proto that was materialized.
        tree_digest: DigestInfo,
    },
    /// No usable seed: the index had no entry, the entry was malformed, the
    /// referenced `Tree`/blob was evicted or corrupt, or a digest mismatch was
    /// detected. Cold build; no partial directory left behind.
    NoSeed,
    /// The index entry's `OutputDirectory.path` did not match this action's
    /// primary output — a blake3 key collision (§3b). Never reused; cold build.
    Collision,
    /// The bounded index fetch did not complete within the caller's timeout
    /// (§6.2). Cold build; a slow/absent index must never stall.
    TimedOut,
}

/// Fetch the current `-incr` seed for `targetkey` from the fleet-shared index
/// and materialize it at `dest_incr_dir`.
///
/// - `index_store`: the AC-shaped mutable index (`incr_seed_index` instance).
/// - `cas_store`: the main content-addressed CAS (`main` instance) holding the
///   REAPI `Tree` and its file blobs.
/// - `targetkey`: the portable target identity (design §3).
/// - `dest_incr_dir`: where the `-incr` directory tree is materialized. On
///   success it is atomically replaced (tmp-then-rename); on any failure it is
///   left untouched and no partial temp directory survives.
/// - `timeout`: bounds the *entire* operation (§6.2/§6.5) — the index
///   `GetActionResult`, the main-CAS `Tree` read, and every file-blob read +
///   write are all covered by this ONE deadline. `GrpcStore` internal RPCs carry
///   `timeout=0` (invariant #10), so a server-CAS fall-through (§6.5) on the Tree
///   or any blob read would otherwise stall the pre-rustc execution path
///   unboundedly. On deadline expiry the operation degrades to a cold
///   [`SeedOutcome::TimedOut`] rather than stalling the build.
///
/// Errors are reserved for genuinely unexpected internal failures (a blocking
/// join failure, or an inability to clean up a partial temp directory — which
/// would leak disk). All *data* problems (missing/corrupt/collided seed) are
/// reported as a cold [`SeedOutcome`], never an `Err`, so the caller can always
/// proceed with a cold build.
pub async fn fetch_and_materialize_seed(
    index_store: &Store,
    cas_store: &Store,
    targetkey: &TargetKey,
    dest_incr_dir: &Path,
    timeout: Duration,
) -> Result<SeedOutcome, Error> {
    // D1: bound the WHOLE operation (index + Tree + all blob reads + writes)
    // under one overall deadline. The inner future records any temp directory it
    // creates into `temp_dir_slot`; on deadline expiry we best-effort wipe it so
    // the tmp-then-rename / no-partial-dir guarantee holds on the timeout path
    // too. `dest_incr_dir` itself is only ever touched by the final atomic rename
    // inside `swap_into_place`, so a cancelled materialize can never leave `dest`
    // partial (`!dest.exists()` is preserved).
    let mut temp_dir_slot: Option<PathBuf> = None;
    match tokio::time::timeout(
        timeout,
        fetch_and_materialize_seed_inner(
            index_store,
            cas_store,
            targetkey,
            dest_incr_dir,
            &mut temp_dir_slot,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_elapsed) => {
            if let Some(temp_dir) = temp_dir_slot {
                // remove_dir_all tolerates absence, so wiping a not-yet-created
                // or already-cleaned temp dir is a safe no-op.
                let _unused_cleanup = remove_dir_all(temp_dir).await;
            }
            emit_counter("incr_index_fetch_timeout");
            debug!(
                targetkey = targetkey.key(),
                "incr seed fetch/materialize deadline exceeded; treating as cold"
            );
            Ok(SeedOutcome::TimedOut)
        }
    }
}

/// The unbounded body of [`fetch_and_materialize_seed`]. The caller wraps this in
/// the overall deadline; this function performs NO per-step timeout of its own
/// (internal-RPC policy, invariant #10). `temp_dir_slot` is set to the temp
/// directory once materialization creates one, so the caller can clean it up if
/// the deadline cancels this future mid-flight.
async fn fetch_and_materialize_seed_inner(
    index_store: &Store,
    cas_store: &Store,
    targetkey: &TargetKey,
    dest_incr_dir: &Path,
    temp_dir_slot: &mut Option<PathBuf>,
) -> Result<SeedOutcome, Error> {
    let index_digest = index_action_digest(targetkey);

    // A NotFound / decode-error is a cold outcome, never an error. (A stalled
    // index cannot hang here: the caller's overall deadline covers this read.)
    let action_result = match get_and_decode_digest::<ActionResult>(
        index_store,
        StoreKey::Digest(index_digest),
    )
    .await
    {
        Err(err) if err.code == Code::NotFound => {
            emit_counter("incr_index_fetch_miss");
            debug!(targetkey = targetkey.key(), "incr seed index miss");
            return Ok(SeedOutcome::NoSeed);
        }
        Err(err) => {
            emit_counter("incr_index_fetch_error");
            warn!(
                targetkey = targetkey.key(),
                ?err,
                "incr seed index fetch errored; treating as cold"
            );
            return Ok(SeedOutcome::NoSeed);
        }
        Ok(action_result) => {
            emit_counter("incr_index_fetch_hit");
            action_result
        }
    };

    // §3b: the index value is an ActionResult whose single output_directories[0]
    // references the current `-incr` Tree. A malformed value is cold.
    let Some(output_dir) = action_result.output_directories.into_iter().next() else {
        warn!(
            targetkey = targetkey.key(),
            "incr seed index entry has no output_directories; treating as cold"
        );
        return Ok(SeedOutcome::NoSeed);
    };

    // §3b: blake3-collision guard — a different primary output under the same
    // key MUST NOT be reused. Cold, never wrong.
    if output_dir.path != targetkey.primary_output() {
        emit_counter("incr_seed_collision");
        warn!(
            targetkey = targetkey.key(),
            index_path = output_dir.path,
            primary_output = targetkey.primary_output(),
            "incr seed index path mismatch (blake3 collision); refusing reuse"
        );
        return Ok(SeedOutcome::Collision);
    }

    let Some(tree_digest_proto) = output_dir.tree_digest else {
        warn!(
            targetkey = targetkey.key(),
            "incr seed index OutputDirectory missing tree_digest; treating as cold"
        );
        return Ok(SeedOutcome::NoSeed);
    };
    let tree_digest = match DigestInfo::try_from(&tree_digest_proto) {
        Ok(digest) => digest,
        Err(err) => {
            warn!(
                targetkey = targetkey.key(),
                ?err,
                "incr seed index tree_digest invalid; treating as cold"
            );
            return Ok(SeedOutcome::NoSeed);
        }
    };

    materialize_tree(cas_store, targetkey, &tree_digest, dest_incr_dir, temp_dir_slot).await
}

/// Derive the index-AC [`DigestInfo`] for `targetkey` (design §3b).
///
/// `hash = blake3(INDEX_KEY_PREFIX ++ targetkey.key())`, `size_bytes =
/// len(preimage)`. Hashing the preimage bytes makes the hasher's tracked size
/// equal to the preimage length, so the produced `DigestInfo` size is exactly
/// the contract's `size_bytes`.
fn index_action_digest(targetkey: &TargetKey) -> DigestInfo {
    let mut preimage = String::with_capacity(INDEX_KEY_PREFIX.len() + targetkey.key().len());
    preimage.push_str(INDEX_KEY_PREFIX);
    preimage.push_str(targetkey.key());
    let mut hasher = DigestHasherFunc::Blake3.hasher();
    hasher.update(preimage.as_bytes());
    hasher.finalize_digest()
}

/// Fetch the REAPI `Tree` and materialize it at `dest_incr_dir`, verifying each
/// file blob's digest (§3b/§7). Any missing/corrupt content or digest mismatch
/// resolves to a cold [`SeedOutcome::NoSeed`] with no partial directory left.
async fn materialize_tree(
    cas_store: &Store,
    targetkey: &TargetKey,
    tree_digest: &DigestInfo,
    dest_incr_dir: &Path,
    temp_dir_slot: &mut Option<PathBuf>,
) -> Result<SeedOutcome, Error> {
    // Read the Tree proto from the main CAS. An evicted/dangling Tree (the
    // §6.1 CompletenessChecking / §6.5 eviction case) surfaces here as
    // NotFound → cold.
    let tree: Tree =
        match get_and_decode_digest::<Tree>(cas_store, StoreKey::Digest(*tree_digest)).await {
            Ok(tree) => tree,
            Err(err) => {
                emit_counter("incr_seed_present_but_cold");
                warn!(
                    targetkey = targetkey.key(),
                    ?tree_digest,
                    ?err,
                    "incr seed Tree unreadable (evicted/corrupt); treating as cold"
                );
                return Ok(SeedOutcome::NoSeed);
            }
        };

    // Plan the tree structure entirely in memory (no store I/O): the directory
    // creation order (parents before children) and the per-file blob digests.
    // A structurally-broken or cyclic tree is cold.
    let Some(root) = tree.root else {
        warn!(
            targetkey = targetkey.key(),
            "incr seed Tree has no root; treating as cold"
        );
        return Ok(SeedOutcome::NoSeed);
    };
    let plan = match build_materialize_plan(&root, &tree.children) {
        Ok(plan) => plan,
        Err(reason) => {
            warn!(
                targetkey = targetkey.key(),
                reason, "incr seed Tree malformed; treating as cold"
            );
            return Ok(SeedOutcome::NoSeed);
        }
    };

    // A unique sibling temp directory in dest's parent, so the final swap is an
    // atomic same-filesystem rename and no partial dir is ever visible at dest.
    let temp_dir = match sibling_temp_dir(dest_incr_dir) {
        Some(temp_dir) => temp_dir,
        None => {
            warn!(
                targetkey = targetkey.key(),
                ?dest_incr_dir,
                "incr seed dest has no parent directory; treating as cold"
            );
            return Ok(SeedOutcome::NoSeed);
        }
    };
    // Record the temp dir so the overall-deadline handler in the caller can wipe
    // it if this future is cancelled mid-materialize (D1). All of this function's
    // own cold/error paths clean it up directly; this slot covers only the
    // cancellation gap.
    *temp_dir_slot = Some(temp_dir.clone());

    // Create the temp root + the full directory skeleton in one blocking hop.
    if let Err(err) = create_skeleton(temp_dir.clone(), plan.dirs.clone()).await {
        // Best-effort cleanup: the temp root may or may not exist yet.
        let _unused_cleanup = remove_dir_all(temp_dir.clone()).await;
        warn!(
            targetkey = targetkey.key(),
            ?err,
            "incr seed skeleton creation failed; treating as cold"
        );
        return Ok(SeedOutcome::NoSeed);
    }

    // Fetch + verify + write each file with bounded fan-out
    // (PARALLEL_MATERIALIZE_CONCURRENCY in flight). Up to that many blobs are
    // resident at once (memory bounded to 16 × the largest single file, not the
    // whole seed); each blob is dropped after its write. Every per-file guard —
    // the blake3 verify and the `O_NOFOLLOW|O_EXCL` byte-copy — runs UNCHANGED
    // inside each concurrent `fetch_and_write_file`; parallelization only overlaps
    // the (§6.5) main-CAS reads. On the FIRST file's cold/error the helper CAPTURES
    // it, then DRAINS the rest to completion so no in-flight `spawn_blocking` write
    // is orphaned (spawn_blocking is uncancellable; an early drop would race the
    // wipe below → ENOTEMPTY + a leaked temp dir). Once every in-flight write has
    // quiesced we wipe the temp tree and return cold — never a partial dir at
    // `dest`. (With `buffer_unordered`, "first" is first-to-fail, not plan order;
    // the outcome — cold, wiped, no-partial-dir — is identical either way.)
    if let Some((failure, rel_path)) =
        materialize_files_parallel(cas_store, &temp_dir, &plan.files).await
    {
        match failure {
            FileMaterializeError::Cold(reason) => {
                cleanup_or_err(&temp_dir).await?;
                emit_counter("incr_seed_present_but_cold");
                warn!(
                    targetkey = targetkey.key(),
                    reason,
                    rel_path = %rel_path.display(),
                    "incr seed file materialization cold; wiped partial temp dir"
                );
                return Ok(SeedOutcome::NoSeed);
            }
            FileMaterializeError::Internal(err) => {
                cleanup_or_err(&temp_dir).await?;
                return Err(err).err_tip(|| "In incr_seed_fetch::materialize_tree");
            }
        }
    }

    // Atomically install the completed tree at the destination.
    if let Err(err) = swap_into_place(temp_dir.clone(), dest_incr_dir.to_path_buf()).await {
        let _unused_cleanup = remove_dir_all(temp_dir).await;
        return Err(err).err_tip(|| "In incr_seed_fetch::materialize_tree swap");
    }

    emit_counter("incr_seed_materialized");
    debug!(
        targetkey = targetkey.key(),
        ?tree_digest,
        dirs = plan.dirs.len(),
        files = plan.files.len(),
        "incr seed materialized"
    );
    Ok(SeedOutcome::Materialized {
        tree_digest: *tree_digest,
    })
}

/// A single file to materialize: its path relative to the seed root, the
/// expected blake3 digest of its content, and its executable bit.
struct PlannedFile {
    rel_path: PathBuf,
    digest: DigestInfo,
    is_executable: bool,
}

/// The in-memory materialization plan: directories to create (parents first)
/// and files to fetch+write.
struct MaterializePlan {
    // CAPPED AT ~10MB proto (MAX_ACTION_MSG_SIZE bounds node count)
    dirs: Vec<PathBuf>,
    // CAPPED AT ~10MB proto (MAX_ACTION_MSG_SIZE bounds node count)
    files: Vec<PlannedFile>,
}

/// Walk the `Tree` (root + digest-keyed children) into a flat
/// [`MaterializePlan`], validating names, structural completeness and acyclicity
/// as it goes. Returns a static reason string on any malformation (→ cold).
///
/// Children are keyed by the blake3 digest of their encoded proto (matching how
/// `DirectoryNode.digest` references them). Symlinks are rejected: an `-incr`
/// seed is symlink-free by construction, and refusing them keeps the
/// materialized tree free of symlink-injection surface (§9).
fn build_materialize_plan(
    root: &Directory,
    children: &[Directory],
) -> Result<MaterializePlan, &'static str> {
    // Map each child directory by its content digest (blake3 of encoded proto).
    // CAPPED AT ~10MB proto (MAX_ACTION_MSG_SIZE bounds node count)
    let mut by_digest: HashMap<DigestInfo, &Directory> = HashMap::with_capacity(children.len());
    for child in children {
        let mut hasher = DigestHasherFunc::Blake3.hasher();
        hasher.update(&child.encode_to_vec());
        by_digest.insert(hasher.finalize_digest(), child);
    }

    traverse_into_plan(root, &by_digest)
}

/// Walk the directory graph rooted at `root`, resolving `DirectoryNode`s through
/// `by_digest`, into a flat [`MaterializePlan`]. Split out from
/// [`build_materialize_plan`] so the cycle guard — which honest content-addressing
/// can never trigger (a self/ancestor reference is a hash fixpoint), but a hash
/// collision or in-memory corruption could — is directly exercisable in tests via
/// an adversarial `by_digest`.
fn traverse_into_plan(
    root: &Directory,
    by_digest: &HashMap<DigestInfo, &Directory>,
) -> Result<MaterializePlan, &'static str> {
    let mut plan = MaterializePlan {
        dirs: Vec::new(),
        files: Vec::new(),
    };

    // Explicit-stack DFS with Enter/Exit frames maintaining the set of digests
    // on the CURRENT path (`ancestors`). A digest reappearing on its own path
    // is a cycle (cryptographically impossible with honest content digests, but
    // defended: a corrupt/adversarial Tree would otherwise loop forever). A
    // digest reappearing OFF the current path (a legitimate diamond — two
    // parents referencing an identical-content child) is materialized at each
    // location.
    enum Frame<'a> {
        Enter {
            dir: &'a Directory,
            rel: PathBuf,
            digest: Option<DigestInfo>,
        },
        Exit(DigestInfo),
    }
    let mut ancestors: HashSet<DigestInfo> = HashSet::new();
    let mut stack: Vec<Frame<'_>> = vec![Frame::Enter {
        dir: root,
        rel: PathBuf::new(),
        digest: None,
    }];

    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Exit(digest) => {
                ancestors.remove(&digest);
            }
            Frame::Enter { dir, rel, digest } => {
                if let Some(digest) = digest {
                    if !ancestors.insert(digest) {
                        return Err("tree contains a directory cycle");
                    }
                    stack.push(Frame::Exit(digest));
                }
                if !dir.symlinks.is_empty() {
                    return Err("tree contains symlinks");
                }
                for file in &dir.files {
                    validate_component(&file.name)?;
                    let digest_proto = file.digest.as_ref().ok_or("file node missing digest")?;
                    let file_digest = DigestInfo::try_from(digest_proto)
                        .map_err(|_| "file node digest invalid")?;
                    plan.files.push(PlannedFile {
                        rel_path: rel.join(&file.name),
                        digest: file_digest,
                        is_executable: file.is_executable,
                    });
                }
                for node in &dir.directories {
                    validate_component(&node.name)?;
                    let digest_proto = node
                        .digest
                        .as_ref()
                        .ok_or("directory node missing digest")?;
                    let child_digest = DigestInfo::try_from(digest_proto)
                        .map_err(|_| "directory node digest invalid")?;
                    let child = by_digest
                        .get(&child_digest)
                        .ok_or("tree missing referenced child directory")?;
                    let child_rel = rel.join(&node.name);
                    plan.dirs.push(child_rel.clone());
                    stack.push(Frame::Enter {
                        dir: child,
                        rel: child_rel,
                        digest: Some(child_digest),
                    });
                }
            }
        }
    }

    Ok(plan)
}

/// Reject path components that could escape the seed root or follow a link:
/// empty, `.`, `..`, or anything containing a path separator or NUL.
fn validate_component(name: &str) -> Result<(), &'static str> {
    if name.is_empty() || name == "." || name == ".." {
        return Err("invalid path component");
    }
    if name.contains('/') || name.contains('\0') {
        return Err("path component contains a separator or NUL");
    }
    Ok(())
}

/// A unique sibling temp directory name in `dest`'s parent (same filesystem →
/// the final rename is atomic). Returns `None` if `dest` has no parent.
fn sibling_temp_dir(dest: &Path) -> Option<PathBuf> {
    let parent = dest.parent()?;
    let file_name = dest.file_name().map_or_else(
        || OsString::from("incr-seed"),
        std::ffi::OsStr::to_os_string,
    );
    let mut name = OsString::from(".");
    name.push(&file_name);
    // pid + nanos gives per-process uniqueness; the §5 machine-local lease and
    // O_EXCL creation guard against same-machine same-targetkey concurrency.
    let suffix = format!(
        ".incrtmp.{}.{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    );
    name.push(suffix);
    Some(parent.join(name))
}

/// Errors from writing one planned file.
enum FileMaterializeError {
    /// Cold (missing/corrupt blob or digest mismatch) — the caller wipes the
    /// temp dir and returns [`SeedOutcome::NoSeed`].
    Cold(&'static str),
    /// An unexpected internal failure to surface as `Err`.
    Internal(Error),
}

/// Drive the per-file fetch+verify+write with bounded fan-out
/// ([`PARALLEL_MATERIALIZE_CONCURRENCY`] futures in flight, via
/// `buffer_unordered`). Returns `Some((error, rel_path))` for the FIRST file that
/// fails (cold or internal) — or `None` when every file materialized. Each file's
/// blake3 verify and `O_NOFOLLOW|O_EXCL` byte-copy run unchanged inside
/// [`fetch_and_write_file`]; this only overlaps the reads.
///
/// DRAIN, don't abort: on a failure the stream is polled to COMPLETION (the first
/// error is captured and returned; later results are discarded) rather than
/// dropped early. [`write_file_nofollow`] runs the byte-copy on `spawn_blocking`,
/// which is NOT cancellable — dropping the stream on the first error would orphan
/// the ≤15 other in-flight blocking writes, which keep creating files under
/// `temp_dir` and race the caller's `remove_dir_all` (observed: `ENOTEMPTY` →
/// `cleanup_or_err` returns `Err` INSTEAD of the cold `SeedOutcome`, and a
/// half-written temp dir LEAKS on disk). Draining lets every in-flight write
/// quiesce before the caller wipes the tree, so the cold-wipe-no-partial-dir
/// contract holds deterministically ON THE INTERNAL FIRST-ERROR PATH (the
/// [`materialize_tree`] cold/`Err` arms that follow this function's return). It
/// does NOT extend to the outer [`fetch_and_materialize_seed`] deadline: if the
/// overall `tokio::time::timeout` fires MID-DRAIN it drops this future and can
/// re-orphan the ≤`PARALLEL_MATERIALIZE_CONCURRENCY` in-flight writes against the
/// timeout handler's own wipe — a known residual (low-severity: `dest` is never
/// touched on the timeout path, so the worst case is a leaked
/// `.<name>.incrtmp.PID.NANOS` sibling, self-healed by the §7 full-empty execroot
/// wipe on the next action; tracked as a follow-up). This mirrors the
/// `.collect().await` parallel-BFS pattern in `running_actions_manager`. The
/// memory bound is
/// unchanged: `buffer_unordered` caps in-flight fetches at
/// `PARALLEL_MATERIALIZE_CONCURRENCY` throughout the drain, and every blob is
/// dropped after its write.
///
/// Driven over OWNED per-file items (rather than borrowing `&plan.files`): a
/// borrowed stream item combined with the caller's `Send` bound (the worker's
/// `inner_prepare_action` awaits this on a `Send` execution future) trips rustc's
/// "implementation of `Send` is not general enough" HRTB inference. Owning each
/// file's plan data — so the buffered futures capture only the Copy
/// `cas_store`/`temp_dir` references (the proven parallel-BFS pattern in
/// `running_actions_manager`) — keeps the future `Send`. The failing path returns
/// an OWNED `PathBuf` for the same reason.
async fn materialize_files_parallel(
    cas_store: &Store,
    temp_dir: &Path,
    files: &[PlannedFile],
) -> Option<(FileMaterializeError, PathBuf)> {
    // Own each file's plan data up-front (the `files` borrow ends here), so no
    // borrowed stream-item lifetime is captured by the buffered futures.
    let owned: Vec<(DigestInfo, PathBuf, bool)> = files
        .iter()
        .map(|file| (file.digest, file.rel_path.clone(), file.is_executable))
        .collect();

    let mut writes = futures::stream::iter(owned.into_iter())
        .map(|(digest, rel_path, is_executable)| async move {
            let planned = PlannedFile {
                rel_path,
                digest,
                is_executable,
            };
            fetch_and_write_file(cas_store, temp_dir, &planned)
                .await
                .map_err(|err| (err, planned.rel_path))
        })
        .buffer_unordered(PARALLEL_MATERIALIZE_CONCURRENCY);

    // Capture the FIRST failure but keep draining, so no in-flight `spawn_blocking`
    // write is orphaned to race the caller's `remove_dir_all` (see the drain
    // rationale above). All later results are discarded — any failure is cold.
    let mut first_failure = None;
    while let Some(result) = writes.next().await {
        if let Err(failure) = result {
            if first_failure.is_none() {
                first_failure = Some(failure);
            }
        }
    }
    first_failure
}

/// Fetch a single blob, verify its blake3 digest, and write it under the temp
/// root. The blob is resident only for this call; the caller caps how many such
/// calls run concurrently ([`PARALLEL_MATERIALIZE_CONCURRENCY`]), so aggregate
/// residency is bounded by `16 × the largest -incr file`.
async fn fetch_and_write_file(
    cas_store: &Store,
    temp_dir: &Path,
    file: &PlannedFile,
) -> Result<(), FileMaterializeError> {
    // UNBOUNDED-OK: this call holds one trusted rustc -incr blob resident; the caller caps fan-out at PARALLEL_MATERIALIZE_CONCURRENCY (16), so peak residency is bounded by 16 × the largest -incr file; declared-size mismatch fails the post-read blake3+size verify → cold
    let bytes = match cas_store
        .get_part_unchunked(
            StoreKey::Digest(file.digest),
            0,
            Some(file.digest.size_bytes()),
        )
        .await
    {
        Ok(bytes) => bytes,
        // A missing/short blob is the §6.5 eviction / live-digest-to-nowhere
        // case: cold, never wrong.
        Err(_err) => return Err(FileMaterializeError::Cold("file blob unreadable")),
    };

    // Verify the content against the expected digest as we write it (§3b). A
    // mismatch (torn/forged/evicted-and-replaced) is cold.
    let mut hasher = DigestHasherFunc::Blake3.hasher();
    hasher.update(&bytes);
    if hasher.finalize_digest() != file.digest {
        return Err(FileMaterializeError::Cold("file blob digest mismatch"));
    }

    let abs_path = temp_dir.join(&file.rel_path);
    let mode: u32 = if file.is_executable { 0o755 } else { 0o644 };
    write_file_nofollow(abs_path, bytes, mode)
        .await
        .map_err(FileMaterializeError::Internal)
}

/// Wipe the temp dir; surface a cleanup failure as `Err` (leaked disk is a real
/// problem, not a cold outcome).
async fn cleanup_or_err(temp_dir: &Path) -> Result<(), Error> {
    remove_dir_all(temp_dir.to_path_buf())
        .await
        .err_tip(|| "cleaning up partial incr seed temp dir")
}

/// Create the temp root and every planned subdirectory (parents-before-children
/// order guaranteed by the plan) in one blocking hop. `create_dir` (not
/// `create_dir_all`) refuses to traverse a pre-existing symlink component.
async fn create_skeleton(temp_root: PathBuf, dirs: Vec<PathBuf>) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || {
        // O_EXCL semantics: create_dir fails if the unique temp root already
        // exists (a hostile pre-creation), which is the safe outcome.
        std::fs::create_dir(&temp_root)
            .err_tip(|| format!("creating incr seed temp root {temp_root:?}"))?;
        for rel in dirs {
            let abs = temp_root.join(&rel);
            std::fs::create_dir(&abs).err_tip(|| format!("creating incr seed subdir {abs:?}"))?;
        }
        Ok::<(), Error>(())
    })
    .await
    .map_err(|e| make_err!(Code::Internal, "incr seed skeleton join failed: {e:?}"))?
}

/// Write `bytes` to `abs_path` with `O_CREAT | O_EXCL | O_NOFOLLOW` (§9): never
/// follow a symlink, never overwrite. NO fsync (ZFS `sync=disabled`; durability
/// is not this cache's concern — a lost seed is a cold rebuild).
async fn write_file_nofollow(abs_path: PathBuf, bytes: Bytes, mode: u32) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(mode)
            .open(&abs_path)
            .err_tip(|| format!("opening incr seed file {abs_path:?}"))?;
        file.write_all(&bytes)
            .err_tip(|| format!("writing incr seed file {abs_path:?}"))?;
        Ok::<(), Error>(())
    })
    .await
    .map_err(|e| make_err!(Code::Internal, "incr seed file write join failed: {e:?}"))?
}

/// Atomically install `temp_dir` at `dest`: remove any existing `dest` (an empty
/// output dir the [C] output-dir prep may have pre-created at the declared `-incr`
/// path, or a leftover), then rename. Same-filesystem rename is atomic; a torn
/// rename is impossible.
async fn swap_into_place(temp_dir: PathBuf, dest: PathBuf) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || {
        match std::fs::remove_dir_all(&dest) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).err_tip(|| format!("removing stale incr seed dest {dest:?}"));
            }
        }
        std::fs::rename(&temp_dir, &dest)
            .err_tip(|| format!("renaming incr seed {temp_dir:?} -> {dest:?}"))?;
        Ok::<(), Error>(())
    })
    .await
    .map_err(|e| make_err!(Code::Internal, "incr seed swap join failed: {e:?}"))?
}

/// Recursively remove a directory tree, tolerating absence.
async fn remove_dir_all(path: PathBuf) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).err_tip(|| format!("removing incr seed temp dir {path:?}")),
    })
    .await
    .map_err(|e| make_err!(Code::Internal, "incr seed remove join failed: {e:?}"))?
}

/// FL-1383 three-state observability (design §12). PROCESS-SINGLETON counters
/// for the worker out-of-band seed path, exposed via [`incr_seed_metrics`] and
/// registered ONCE into the `MetricsRegistry` by `bin/nativelink.rs` (the
/// `WorkerPhase0Metrics` singleton pattern). A per-instance / unregistered tree
/// would be DARK — 0 events indistinguishable from "never called" — the
/// worker-metrics-exposure trap (`worker-metrics-exposure-pattern` memory); a
/// singleton registered in the binary is the only non-dark path. Every field
/// name below is pinned by a render/`publish`-visibility test so a rename cannot
/// silently re-dark it.
#[derive(Debug, Default, MetricsComponent)]
pub struct IncrSeedMetrics {
    /// `-incr` seed dir materialized at the execroot before rustc (a warm fetch).
    #[metric(help = "FL-1383: incr seed dirs materialized at the execroot before rustc")]
    pub incr_seed_materialized: AtomicU64,
    /// rustc incremental reuse actually fired. Populated by the rustc-side branch
    /// (rules_rust, chunk 4), NOT by the worker — registered here so the name is
    /// visible (not dark) even though the worker leaves it at 0.
    #[metric(help = "FL-1383: rustc incremental reuse fired (rustc-side branch; worker leaves 0)")]
    pub incr_reuse_fired: AtomicU64,
    /// Index resolved but the referenced content was unreadable (evicted/corrupt)
    /// or a per-file digest mismatch → cold.
    #[metric(help = "FL-1383: index resolved but content unusable (evicted/corrupt/mismatch) -> cold")]
    pub incr_seed_present_but_cold: AtomicU64,
    /// Index blake3 key collision (stored path != this action's primary) → cold.
    #[metric(help = "FL-1383: index blake3 key collision (path mismatch) -> cold")]
    pub incr_seed_collision: AtomicU64,
    /// Index `GetActionResult` hit.
    #[metric(help = "FL-1383: incr seed index GetActionResult hit")]
    pub incr_index_fetch_hit: AtomicU64,
    /// Index `GetActionResult` miss (NotFound) → cold.
    #[metric(help = "FL-1383: incr seed index GetActionResult miss (NotFound) -> cold")]
    pub incr_index_fetch_miss: AtomicU64,
    /// Index fetch exceeded the bounded deadline (§6.2) → cold.
    #[metric(help = "FL-1383: incr seed index fetch bounded-timeout -> cold")]
    pub incr_index_fetch_timeout: AtomicU64,
    /// Index fetch errored (non-NotFound) → cold.
    #[metric(help = "FL-1383: incr seed index fetch errored -> cold")]
    pub incr_index_fetch_error: AtomicU64,
    /// Seed index entries published after a successful allowlisted build (§6.2).
    #[metric(help = "FL-1383: incr seed index entries published after a successful build")]
    pub incr_index_publish: AtomicU64,
}

static INCR_SEED_METRICS: OnceLock<Arc<IncrSeedMetrics>> = OnceLock::new();

fn incr_seed_metrics_inner() -> &'static Arc<IncrSeedMetrics> {
    INCR_SEED_METRICS.get_or_init(|| Arc::new(IncrSeedMetrics::default()))
}

/// The process-global [`IncrSeedMetrics`] singleton (design §12). The producer
/// side ([`emit_counter`]) bumps these; `bin/nativelink.rs` registers the same
/// singleton into the `MetricsRegistry` so the counters render.
#[must_use]
pub fn incr_seed_metrics() -> &'static IncrSeedMetrics {
    incr_seed_metrics_inner()
}

/// `Arc` clone of the [`incr_seed_metrics`] singleton, for registration into the
/// `MetricsRegistry` at process start (the `worker_phase0_metrics_arc` pattern).
#[must_use]
pub fn incr_seed_metrics_arc() -> Arc<IncrSeedMetrics> {
    Arc::clone(incr_seed_metrics_inner())
}

/// Emit an observability counter for the seed path: bump the process-singleton
/// [`IncrSeedMetrics`] field of the same name AND leave a grep-stable `debug!`
/// marker. The `&'static str` name is matched against the registered fields so a
/// caller cannot introduce an un-registered (dark) counter unnoticed — an
/// unmapped name is logged loudly and NOT silently swallowed.
fn emit_counter(counter: &'static str) {
    let metrics = incr_seed_metrics();
    let field = match counter {
        "incr_seed_materialized" => &metrics.incr_seed_materialized,
        "incr_reuse_fired" => &metrics.incr_reuse_fired,
        "incr_seed_present_but_cold" => &metrics.incr_seed_present_but_cold,
        "incr_seed_collision" => &metrics.incr_seed_collision,
        "incr_index_fetch_hit" => &metrics.incr_index_fetch_hit,
        "incr_index_fetch_miss" => &metrics.incr_index_fetch_miss,
        "incr_index_fetch_timeout" => &metrics.incr_index_fetch_timeout,
        "incr_index_fetch_error" => &metrics.incr_index_fetch_error,
        "incr_index_publish" => &metrics.incr_index_publish,
        other => {
            warn!(counter = other, "incr seed fetch counter has no registered field (dark)");
            return;
        }
    };
    field.fetch_add(1, Ordering::Relaxed);
    debug!(counter, "incr seed fetch counter");
}

/// Record a successful seed-index publish in the §12 counters. Called by the
/// worker publish site after `update_oneshot` succeeds (the publish itself lives
/// in the execution path, not here, but the counter stays with its siblings).
pub fn note_index_published() {
    emit_counter("incr_index_publish");
}

/// Record that rustc incremental reuse actually fired in the §12 counters
/// (design §12, FL-1383 chunk 4). Called by the worker post-action site after it
/// reads THIS action's own `<label>-incr-reuse` marker (written by the client's
/// process_wrapper) and finds content `1`. Lights `incr_reuse_fired`, which was
/// registered-but-dark until this read exists (the worker leaves it at 0
/// otherwise); the read/bump lives in the execution path, but the counter stays
/// with its siblings so a rename cannot silently re-dark it.
pub fn note_reuse_fired() {
    emit_counter("incr_reuse_fired");
}

/// Whether `path`'s basename names the SINGULAR `-incr` seed dir. Stage-1 (§6.1)
/// materializes/publishes the singular `<label>-incr`, NOT the pipelined
/// `<label>-incr-metadata`; `ends_with("-incr")` already excludes both
/// `-incr-metadata` and `-incr-unused-inputs.txt`.
fn path_is_incr_seed_dir(path: &str) -> bool {
    let basename = path.rsplit('/').next().unwrap_or(path);
    basename.ends_with("-incr")
}

/// The on-disk destination for the fetched `-incr` seed (design §6.3/§7, FL-1383
/// §3): the current action's OWN declared `-incr` output path — a NESTED,
/// label-named `Command.output_paths` entry (`bazel-out/cfg/bin/<pkg>/<label>-incr`,
/// where `rustc -Cincremental` points) — joined onto the execroot, HONORING
/// `Command.working_directory`. Returns the FIRST `output_paths` entry whose
/// basename ends in `-incr` (the singular seed dir, per [`path_is_incr_seed_dir`]);
/// `None` if the action declares no such output → cold, no fetch.
///
/// `working_directory`-awareness (FL-1383 §3, pair-a #2): REAPI `output_paths` are
/// relative to `Command.working_directory`, so the dest MUST be computed with the
/// SAME formula [`prepare_output_directory`] uses when it creates the seed's parent
/// dir — `{execroot}/{working_directory}/{output_path}` (or `{execroot}/{output_path}`
/// when `working_directory` is empty). This is the single source of truth: any
/// divergence lands the seed where `prepare_output_directory` did NOT create a
/// parent (cold-not-wrong: fetch fails, or materializes where rustc — cwd
/// `execroot/working_directory` — never reads `-Cincremental` → dark reuse-collapse).
/// `working_directory ∈ {"", "."}` collapses in the join (the `.` component
/// normalizes away against the absolute execroot).
///
/// This is OPTION A (locked with the Bazel/rules_rust side): the seed CONTENT
/// comes from the index `tree_digest`; the DESTINATION is derived from THIS
/// action's declared `-incr` output, so the seed always lands where this action's
/// rustc reads. The index `OutputDirectory.path` stays the `.rlib` (the collision
/// guard, [`plan_seed_publish`]); it does NOT carry the `-incr` path. Contrast the
/// prior (chunk-2) `<execroot>/<stem>-incr` TOP-LEVEL model, which put the seed
/// where rustc never reads it → dark cold reuse.
///
/// Defense-in-depth (§9): a REAPI declared output path is execroot-relative and
/// Bazel-normalized; an absolute path or a `..` component is refused (`None` →
/// cold), so the join can never materialize outside the execroot.
///
/// [`prepare_output_directory`]: crate::running_actions_manager::prepare_output_directory
#[must_use]
pub fn seed_dest_dir(
    execroot: &Path,
    working_directory: &str,
    output_paths: &[String],
) -> Option<PathBuf> {
    let incr = output_paths
        .iter()
        .map(String::as_str)
        .find(|path| path_is_incr_seed_dir(path))?;
    let incr_path = Path::new(incr);
    if incr_path.is_absolute()
        || incr_path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return None;
    }
    // Single source of truth with `prepare_output_directory`, which builds the
    // parent as `{execroot}/{working_directory}/{output_path}` (empty
    // working_directory ⇒ no middle segment). Joining the same way keeps the
    // seed at exactly the path whose parent that function created.
    let base = if working_directory.is_empty() {
        execroot.to_path_buf()
    } else {
        execroot.join(working_directory)
    };
    Some(base.join(incr_path))
}

/// A planned seed-index publish (design §6.2): the AC-shaped index KEY under
/// `hash(targetkey)` and the encoded `ActionResult` VALUE to `update_oneshot`.
///
/// The seed CONTENT (the `-incr` REAPI `Tree` + its blobs) is NOT part of this —
/// it is a normal DECLARED OUTPUT of the rustc action, already uploaded to CAS by
/// the worker's regular output-upload path. This is purely the worker-side INDEX
/// write that points `targetkey` at that already-resident content.
#[derive(Debug)]
pub struct SeedPublish {
    /// The `incr_seed_index` AC key (`hash(targetkey)`, design §3b).
    pub index_digest: DigestInfo,
    /// The encoded `ActionResult` value (`output_directories[0]` = primary output
    /// path + `-incr` tree digest).
    pub encoded: Bytes,
}

/// Decide whether to publish a seed index entry for a just-completed action, and
/// with what key+value (design §6.2). Returns `None` — meaning DO NOT publish —
/// unless BOTH hold:
///
/// - the build SUCCEEDED (`exit_code == 0 && !has_error`). A failed or partial
///   build's `-incr` tree is torn/half-written; publishing it would seed peers
///   from a broken state, so publication is HARD-GATED on success; AND
/// - the action declared an `-incr` seed output directory — a folder whose final
///   path component ends in `-incr` (but NOT `-incr-metadata`, the distinct §4
///   pipelined-metadata tree the single-tree Stage-1 index does not carry).
///
/// The value's `output_directories[0].path` is the `targetkey`'s primary output
/// (NOT the `-incr` path) so the chunk-3 fetch collision guard
/// (`path == primary_output`) accepts it; its `tree_digest` is the `-incr` tree.
/// LWW by the stable `targetkey`: the caller `update_oneshot`-overwrites any
/// prior entry.
#[must_use]
pub fn plan_seed_publish<'folders>(
    targetkey: &TargetKey,
    exit_code: i32,
    has_error: bool,
    output_folders: impl IntoIterator<Item = (&'folders str, DigestInfo)>,
) -> Option<SeedPublish> {
    // HARD success gate — never publish a failed/partial build's torn seed.
    if exit_code != 0 || has_error {
        return None;
    }
    let incr_tree_digest = select_incr_seed_folder(output_folders)?;
    let action_result = ActionResult {
        output_directories: vec![OutputDirectory {
            // primary output (for the fetch collision guard), NOT the -incr path.
            path: targetkey.primary_output().to_string(),
            tree_digest: Some(Digest::from(&incr_tree_digest)),
            is_topologically_sorted: false,
        }],
        ..Default::default()
    };
    Some(SeedPublish {
        index_digest: index_action_digest(targetkey),
        encoded: Bytes::from(action_result.encode_to_vec()),
    })
}

/// Select the `-incr` seed tree digest from an action's output directories
/// (design §6.1 references the single "current -incr"). Matches the first folder
/// whose final path component ends in `-incr` and NOT `-incr-metadata` (the §4
/// pipelined-metadata tree is a separate seed, excluded from the Stage-1
/// single-tree index).
fn select_incr_seed_folder<'folders>(
    output_folders: impl IntoIterator<Item = (&'folders str, DigestInfo)>,
) -> Option<DigestInfo> {
    output_folders
        .into_iter()
        .find_map(|(path, digest)| path_is_incr_seed_dir(path).then_some(digest))
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use std::collections::HashMap;

    use bytes::Bytes;
    use nativelink_config::stores::MemorySpec;
    use nativelink_macro::nativelink_test;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        ActionResult, Digest, Directory, DirectoryNode, FileNode, OutputDirectory, SymlinkNode, Tree,
    };
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
    use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
    use nativelink_util::targetkey::TargetKey;
    use prost::Message;

    use super::{
        IncrSeedMetrics, SeedOutcome, build_materialize_plan, fetch_and_materialize_seed,
        incr_seed_metrics_arc, index_action_digest, plan_seed_publish, seed_dest_dir,
        traverse_into_plan, validate_component,
    };

    const PRIMARY: &str = "bazel-out/cfg/bin/third_party/rust/apple_a14/libfoo.rlib";

    fn blake3_digest(bytes: &[u8]) -> DigestInfo {
        let mut hasher = DigestHasherFunc::Blake3.hasher();
        hasher.update(bytes);
        hasher.finalize_digest()
    }

    fn new_store() -> Store {
        Store::new(MemoryStore::new(&MemorySpec::default()))
    }

    async fn put(store: &Store, digest: DigestInfo, bytes: Bytes) {
        store
            .update_oneshot(StoreKey::Digest(digest), bytes)
            .await
            .expect("store write should succeed");
    }

    fn targetkey() -> TargetKey {
        TargetKey::derive(&[PRIMARY.to_string()]).expect("targetkey derives")
    }

    /// Upload a blob and return the `FileNode` referencing it.
    async fn upload_file(cas: &Store, name: &str, content: &[u8], exec: bool) -> FileNode {
        let digest = blake3_digest(content);
        put(cas, digest, Bytes::copy_from_slice(content)).await;
        FileNode {
            name: name.to_string(),
            digest: Some(Digest::from(&digest)),
            is_executable: exec,
            node_properties: None,
        }
    }

    /// Upload a `Directory` proto to CAS and return its blake3 digest, so it can
    /// be referenced by a `DirectoryNode`.
    async fn upload_dir(cas: &Store, dir: &Directory) -> DigestInfo {
        let bytes = dir.encode_to_vec();
        let digest = blake3_digest(&bytes);
        put(cas, digest, Bytes::from(bytes)).await;
        digest
    }

    /// Upload a `Tree` proto and return its digest.
    async fn upload_tree(cas: &Store, tree: &Tree) -> DigestInfo {
        let bytes = tree.encode_to_vec();
        let digest = blake3_digest(&bytes);
        put(cas, digest, Bytes::from(bytes)).await;
        digest
    }

    /// Build a single-directory `Tree` with the given (name, content, exec)
    /// files, upload the blobs + `Tree`, and return the `Tree` digest.
    async fn upload_flat_tree(cas: &Store, files: &[(&str, &[u8], bool)]) -> DigestInfo {
        let mut file_nodes = Vec::new();
        for (name, content, exec) in files {
            file_nodes.push(upload_file(cas, name, content, *exec).await);
        }
        let tree = Tree {
            root: Some(Directory {
                files: file_nodes,
                directories: vec![],
                symlinks: vec![],
                node_properties: None,
            }),
            children: vec![],
        };
        upload_tree(cas, &tree).await
    }

    /// Publish an index entry pointing `path` at `tree_digest`.
    async fn publish_index(index: &Store, tk: &TargetKey, path: &str, tree_digest: &DigestInfo) {
        let action_result = ActionResult {
            output_directories: vec![OutputDirectory {
                path: path.to_string(),
                tree_digest: Some(Digest::from(tree_digest)),
                is_topologically_sorted: false,
            }],
            ..Default::default()
        };
        let digest = index_action_digest(tk);
        put(index, digest, Bytes::from(action_result.encode_to_vec())).await;
    }

    #[nativelink_test]
    async fn hit_materializes_and_verifies() {
        let index = new_store();
        let cas = new_store();
        let tk = targetkey();
        let tree_digest = upload_flat_tree(
            &cas,
            &[("a.bin", b"alpha", false), ("run.sh", b"#!/bin/sh\n", true)],
        )
        .await;
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_secs(5))
            .await
            .expect("fetch should not error on a valid hit");

        assert_eq!(outcome, SeedOutcome::Materialized { tree_digest });
        assert_eq!(std::fs::read(dest.join("a.bin")).unwrap(), b"alpha");
        assert_eq!(std::fs::read(dest.join("run.sh")).unwrap(), b"#!/bin/sh\n");
    }

    #[nativelink_test]
    async fn hit_materializes_nested_subdir() {
        let index = new_store();
        let cas = new_store();
        let tk = targetkey();

        // A child directory `sub/` containing `nested.bin`.
        let nested = upload_file(&cas, "nested.bin", b"deep", false).await;
        let child = Directory {
            files: vec![nested],
            directories: vec![],
            symlinks: vec![],
            node_properties: None,
        };
        let child_digest = upload_dir(&cas, &child).await;

        let top = upload_file(&cas, "top.bin", b"surface", false).await;
        let tree = Tree {
            root: Some(Directory {
                files: vec![top],
                directories: vec![DirectoryNode {
                    name: "sub".to_string(),
                    digest: Some(Digest::from(&child_digest)),
                }],
                symlinks: vec![],
                node_properties: None,
            }),
            // The Tree carries every child directory inline.
            children: vec![child],
        };
        let tree_digest = upload_tree(&cas, &tree).await;
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_secs(5))
            .await
            .expect("fetch should not error on a nested hit");

        assert_eq!(outcome, SeedOutcome::Materialized { tree_digest });
        assert_eq!(std::fs::read(dest.join("top.bin")).unwrap(), b"surface");
        assert_eq!(std::fs::read(dest.join("sub/nested.bin")).unwrap(), b"deep");
    }

    // Parallel materialize path: many files (> PARALLEL_MATERIALIZE_CONCURRENCY)
    // must ALL land with correct content AND executable bit through the bounded
    // fan-out. Exceeding the 16-in-flight window exercises the buffer_unordered
    // drain (queued files resume as slots free), proving parallelization is
    // outcome-transparent — same content, same modes, same Materialized outcome.
    #[nativelink_test]
    async fn hit_materializes_many_files_parallel() {
        use std::os::unix::fs::PermissionsExt as _;

        let index = new_store();
        let cas = new_store();
        let tk = targetkey();

        // 40 distinct files, comfortably past the 16-wide concurrency window, so
        // the stream must queue-and-drain rather than fit in one batch.
        let contents: Vec<(String, Vec<u8>, bool)> = (0..40u32)
            .map(|i| {
                (
                    format!("f{i:02}.bin"),
                    format!("content-{i}").into_bytes(),
                    i % 3 == 0,
                )
            })
            .collect();
        let specs: Vec<(&str, &[u8], bool)> = contents
            .iter()
            .map(|(name, body, exec)| (name.as_str(), body.as_slice(), *exec))
            .collect();
        let tree_digest = upload_flat_tree(&cas, &specs).await;
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_secs(10))
            .await
            .expect("fetch should not error on a many-file hit");

        assert_eq!(outcome, SeedOutcome::Materialized { tree_digest });
        for (name, body, exec) in &contents {
            let got = std::fs::read(dest.join(name))
                .unwrap_or_else(|e| panic!("file {name} must materialize under parallel path: {e}"));
            assert_eq!(&got, body, "content mismatch for {name}");
            let mode = std::fs::metadata(dest.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            let want = if *exec { 0o755 } else { 0o644 };
            assert_eq!(mode, want, "mode mismatch for {name} (executable-bit guard)");
        }
    }

    // Parallel first-failure cold path: with MANY files (past the 16-wide window)
    // and ONE whose blob mismatches its claimed digest, the bounded fan-out will
    // have written several good siblings into the temp dir by the time the bad
    // file's future returns Cold. The path must DRAIN the remaining in-flight
    // writes (so no uncancellable spawn_blocking write is orphaned to race the
    // wipe) AND wipe the WHOLE temp tree — including the already-written siblings
    // — so NO partial dir survives and `dest` never appears. This is the
    // parallel-specific risk the single-file
    // `file_digest_mismatch_is_cold_no_partial_dir` cannot exercise.
    //
    // TIMING NOTE: this exercises the PHYSICAL orphan-write→leftover contract, but
    // with in-memory stores each write completes in ~µs, so the orphan-lands-after-
    // wipe window is tiny and a buggy abort would MOSTLY also pass here — this test
    // is NOT a reliable drain-vs-abort discriminator on its own. The deterministic,
    // race-free discriminator is
    // `parallel_drain_awaits_inflight_not_abort_on_first_error` below; this test
    // remains as the end-to-end physical-contract check.
    #[nativelink_test]
    async fn parallel_one_bad_file_among_many_is_cold_no_partial_dir() {
        let index = new_store();
        let cas = new_store();
        let tk = targetkey();

        const BAD: u32 = 25;
        let mut file_nodes = Vec::new();
        for i in 0..40u32 {
            let name = format!("f{i:02}.bin");
            if i == BAD {
                // Claim a digest but store DIFFERENT bytes under it → the
                // post-read blake3+size verify fails → this file's future
                // returns Cold, aborting the rest.
                let claimed = blake3_digest(format!("claimed-{i}").as_bytes());
                put(&cas, claimed, Bytes::from_static(b"WRONG-length-and-content")).await;
                file_nodes.push(FileNode {
                    name,
                    digest: Some(Digest::from(&claimed)),
                    is_executable: false,
                    node_properties: None,
                });
            } else {
                let content = format!("content-{i}").into_bytes();
                file_nodes.push(upload_file(&cas, &name, &content, false).await);
            }
        }
        let tree = Tree {
            root: Some(Directory {
                files: file_nodes,
                directories: vec![],
                symlinks: vec![],
                node_properties: None,
            }),
            children: vec![],
        };
        let tree_digest = upload_tree(&cas, &tree).await;
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_secs(10))
            .await
            .expect("fetch should not error on one bad file among many");

        assert_eq!(
            outcome,
            SeedOutcome::NoSeed,
            "one bad file among many must yield a COLD outcome"
        );
        assert!(
            !dest.exists(),
            "dest must never appear after a parallel cold abort"
        );
        // The whole temp tree — including the successfully-written siblings — must
        // be wiped; no partial dir may survive in dest's parent.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();
        assert!(
            leftovers.is_empty(),
            "no partial temp dir may survive a parallel first-failure abort (the \
             successfully-written siblings must be wiped too); found: {leftovers:?}"
        );
    }

    // DETERMINISTIC drain-vs-abort discriminator (red-team fb4e4d4d blind-spot 2).
    //
    // `parallel_one_bad_file_among_many_is_cold_no_partial_dir` above exercises the
    // physical orphan-write→leftover contract, but with in-memory stores every
    // write completes in ~µs, so the orphan-lands-after-wipe window is tiny and a
    // BUGGY abort would MOSTLY also pass it (timing-flaky green) — not a reliable
    // regression guard. This test removes the timing dependence entirely.
    //
    // Mechanism the drain fix provides: `materialize_files_parallel` returns ONLY
    // after every in-flight buffered future has resolved (each future resolves only
    // after its `write_file_nofollow` `spawn_blocking` JoinHandle awaits), so no
    // uncancellable in-flight write can be orphaned to race the caller's wipe. The
    // buggy abort abandons that guarantee by RETURNING on the first error, dropping
    // the still-in-flight futures.
    //
    // Discriminator (race-free): one BAD file (immediate digest mismatch → the
    // first-and-only error) among GOOD files whose `get_part` HANGS FOREVER
    // (`SelectiveStore` `hang_on`). The good fetches never resolve, so:
    //   * the correct DRAIN awaits those in-flight futures and can therefore only
    //     leave the drain via the caller's OUTER deadline → `SeedOutcome::TimedOut`;
    //   * the buggy ABORT returns `SeedOutcome::NoSeed` the instant the bad file
    //     errors, abandoning the in-flight fetches.
    // Because the good fetches hang unconditionally, the outcome is fixed by which
    // code path runs — NOT by any write/wipe timing — so the mutation fails RED on
    // EVERY run. (The hung stage is the fetch rather than the uncancellable write —
    // which a CAS-store wrapper cannot gate — but the drain-loop `await`-all-futures
    // structure this pins is identical for a future stuck in fetch or in write, so
    // it is a faithful, deterministic proxy for the write-orphan guarantee.)
    #[nativelink_test]
    async fn parallel_drain_awaits_inflight_not_abort_on_first_error() {
        let inner = MemoryStore::new(&MemorySpec::default());
        let populate = Store::new(inner.clone());
        let tk = targetkey();

        // One bad file (wrong bytes under its claimed digest) + several good files
        // whose real blobs are uploaded but whose reads will be hung.
        let mut file_nodes = Vec::new();
        let mut hang_on = Vec::new();

        let bad_claimed = blake3_digest(b"bad-claimed");
        put(&populate, bad_claimed, Bytes::from_static(b"WRONG-bytes-mismatch")).await;
        file_nodes.push(FileNode {
            name: "bad.bin".to_string(),
            digest: Some(Digest::from(&bad_claimed)),
            is_executable: false,
            node_properties: None,
        });

        for i in 0..3u32 {
            let name = format!("good{i}.bin");
            let content = format!("good-content-{i}").into_bytes();
            let digest = blake3_digest(&content);
            put(&populate, digest, Bytes::from(content)).await;
            hang_on.push(digest);
            file_nodes.push(FileNode {
                name,
                digest: Some(Digest::from(&digest)),
                is_executable: false,
                node_properties: None,
            });
        }

        let tree = Tree {
            root: Some(Directory {
                files: file_nodes,
                directories: vec![],
                symlinks: vec![],
                node_properties: None,
            }),
            children: vec![],
        };
        let tree_bytes = tree.encode_to_vec();
        let tree_digest = blake3_digest(&tree_bytes);
        put(&populate, tree_digest, Bytes::from(tree_bytes)).await;

        // The CAS serves the Tree and the bad blob, but HANGS every good blob read.
        let cas = SelectiveStore::new_store(inner, hang_on);
        let index = new_store();
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        // Outer deadlock-detector: a correct drain exits via the 300ms overall
        // deadline; the buggy abort exits immediately with NoSeed. The 5s bound
        // catches a hypothetical regression that neither drains nor times out.
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_millis(300)),
        )
        .await
        .expect("drain-awaits test must settle within 5s")
        .expect("a hung in-flight fetch must degrade to cold, not error");

        assert_eq!(
            outcome,
            SeedOutcome::TimedOut,
            "drain MUST await in-flight fetches before returning (the uncancellable-write \
             quiesce guarantee): with one bad file among gated-hung in-flight fetches, the \
             correct drain blocks until the outer deadline (TimedOut); an abort that drops the \
             stream on the first error returns NoSeed early, abandoning in-flight ops -> \
             orphan-write race"
        );
        assert!(
            !dest.exists(),
            "dest must never appear on the drain-to-deadline cold path"
        );
        // The deadline handler must have wiped the partial temp skeleton.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();
        assert!(
            leftovers.is_empty(),
            "no partial temp dir may survive the drain-to-deadline path, found: {leftovers:?}"
        );
    }

    #[nativelink_test]
    async fn index_miss_is_cold() {
        let index = new_store();
        let cas = new_store();
        let tk = targetkey();
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_secs(5))
            .await
            .expect("fetch should not error on a miss");

        assert_eq!(outcome, SeedOutcome::NoSeed);
        assert!(!dest.exists(), "no dir should be created on a miss");
    }

    #[nativelink_test]
    async fn path_mismatch_is_collision() {
        let index = new_store();
        let cas = new_store();
        let tk = targetkey();
        let tree_digest = upload_flat_tree(&cas, &[("a.bin", b"alpha", false)]).await;
        // Publish under a DIFFERENT primary output → blake3 collision.
        publish_index(
            &index,
            &tk,
            "bazel-out/cfg/bin/third_party/rust/apple_a14/libOTHER.rlib",
            &tree_digest,
        )
        .await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_secs(5))
            .await
            .expect("fetch should not error on a collision");

        assert_eq!(outcome, SeedOutcome::Collision);
        assert!(!dest.exists(), "no dir on a collision");
    }

    #[nativelink_test]
    async fn file_digest_mismatch_is_cold_no_partial_dir() {
        let index = new_store();
        let cas = new_store();
        let tk = targetkey();

        // Build a Tree whose FileNode claims a digest that does NOT match the
        // blob actually stored under that digest (torn/forged content).
        let claimed = blake3_digest(b"the-claimed-content");
        // Store DIFFERENT bytes under the claimed digest key.
        put(&cas, claimed, Bytes::from_static(b"WRONG-different-length")).await;
        let tree = Tree {
            root: Some(Directory {
                files: vec![FileNode {
                    name: "a.bin".to_string(),
                    digest: Some(Digest::from(&claimed)),
                    is_executable: false,
                    node_properties: None,
                }],
                directories: vec![],
                symlinks: vec![],
                node_properties: None,
            }),
            children: vec![],
        };
        let tree_digest = upload_tree(&cas, &tree).await;
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_secs(5))
            .await
            .expect("fetch should not error on a digest mismatch");

        assert_eq!(outcome, SeedOutcome::NoSeed);
        assert!(!dest.exists(), "dest must not exist");
        // No partial temp dir must survive in the parent.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();
        assert!(
            leftovers.is_empty(),
            "no partial temp dir should survive, found: {leftovers:?}"
        );
    }

    #[nativelink_test]
    async fn slow_index_times_out_bounded() {
        // `SlowStore::get_part` never resolves, so the wrapped index fetch can
        // only complete via the bounded timeout — proving a slow/absent index
        // does not stall the build (§6.2).
        let index = SlowStore::new_store();
        let cas = new_store();
        let tk = targetkey();
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome =
            fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_millis(50))
                .await
                .expect("fetch should not error on a timeout");

        assert_eq!(outcome, SeedOutcome::TimedOut);
        assert!(!dest.exists());
    }

    // FIX 1 (D1): the overall deadline — not just the index fetch — must bound a
    // stalled main-CAS Tree read (§6.5 server-CAS fall-through, GrpcStore
    // timeout=0). Index hit, but the CAS never resolves the Tree read.
    #[nativelink_test]
    async fn stalled_cas_tree_read_times_out_bounded() {
        let index = new_store();
        let cas = SlowStore::new_store();
        let tk = targetkey();
        // A valid index entry pointing at a Tree the (stalled) CAS would serve.
        let tree_digest = blake3_digest(b"unreachable-tree");
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        // Outer deadlock-detector: if the overall deadline is removed, the Tree
        // read hangs forever and this 5s bound trips instead.
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_millis(50)),
        )
        .await
        .expect("overall deadline must bound a stalled CAS Tree read; the call must return within 5s")
        .expect("stalled CAS Tree read must degrade to cold, not error");

        assert_eq!(outcome, SeedOutcome::TimedOut);
        assert!(
            !dest.exists(),
            "dest must not exist after a deadline-triggered cold fallback"
        );
    }

    // FIX 1 (D1): the overall deadline must bound a stalled blob read that occurs
    // AFTER the temp skeleton is created, and the deadline handler must wipe the
    // partial temp dir (no partial directory survives a timeout).
    #[nativelink_test]
    async fn stalled_cas_blob_read_times_out_and_wipes_temp() {
        let inner = MemoryStore::new(&MemorySpec::default());
        let populate = Store::new(inner.clone());

        // A one-file flat tree: upload the blob and the Tree into `inner`.
        let content = b"incr-blob";
        let file_digest = blake3_digest(content);
        put(&populate, file_digest, Bytes::from_static(content)).await;
        let tree = Tree {
            root: Some(Directory {
                files: vec![FileNode {
                    name: "a.bin".to_string(),
                    digest: Some(Digest::from(&file_digest)),
                    is_executable: false,
                    node_properties: None,
                }],
                directories: vec![],
                symlinks: vec![],
                node_properties: None,
            }),
            children: vec![],
        };
        let tree_bytes = tree.encode_to_vec();
        let tree_digest = blake3_digest(&tree_bytes);
        put(&populate, tree_digest, Bytes::from(tree_bytes)).await;

        // The CAS serves the Tree but hangs ONLY on the file-blob read.
        let cas = SelectiveStore::new_store(inner, [file_digest]);
        let index = new_store();
        let tk = targetkey();
        publish_index(&index, &tk, PRIMARY, &tree_digest).await;

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("libfoo.incr");

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_and_materialize_seed(&index, &cas, &tk, &dest, Duration::from_millis(100)),
        )
        .await
        .expect("overall deadline must bound a stalled CAS blob read; the call must return within 5s")
        .expect("stalled CAS blob read must degrade to cold, not error");

        assert_eq!(outcome, SeedOutcome::TimedOut);
        assert!(
            !dest.exists(),
            "dest must not exist after a deadline-triggered cold fallback"
        );
        // The deadline handler must have wiped the partial temp skeleton.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();
        assert!(
            leftovers.is_empty(),
            "no partial temp dir should survive a deadline, found: {leftovers:?}"
        );
    }

    // FIX 5 T1: an adversarial `by_digest` where a child references the digest it
    // is stored under — a cycle honest content-addressing can never build (hash
    // fixpoint) but a collision/corruption could. Without the guard the DFS loops
    // forever; the guard rejects it with its bespoke reason.
    #[nativelink_test]
    async fn directory_cycle_is_rejected() {
        let anchor = blake3_digest(b"cycle-anchor");
        let looping = Directory {
            files: vec![],
            directories: vec![DirectoryNode {
                name: "self".to_string(),
                digest: Some(Digest::from(&anchor)),
            }],
            symlinks: vec![],
            node_properties: None,
        };
        let mut by_digest: HashMap<DigestInfo, &Directory> = HashMap::new();
        by_digest.insert(anchor, &looping);
        let root = Directory {
            files: vec![],
            directories: vec![DirectoryNode {
                name: "a".to_string(),
                digest: Some(Digest::from(&anchor)),
            }],
            symlinks: vec![],
            node_properties: None,
        };

        assert_eq!(
            traverse_into_plan(&root, &by_digest).err(),
            Some("tree contains a directory cycle"),
        );
    }

    // FIX 5 T1: a legitimate diamond — two parents referencing an identical child
    // — must be materialized at BOTH locations, NOT rejected as a cycle (the guard
    // is path-scoped via `ancestors.remove`, not global-visited).
    #[nativelink_test]
    async fn diamond_shared_child_materialized_per_location() {
        let shared = Directory {
            files: vec![FileNode {
                name: "leaf.bin".to_string(),
                digest: Some(Digest::from(&blake3_digest(b"leaf"))),
                is_executable: false,
                node_properties: None,
            }],
            directories: vec![],
            symlinks: vec![],
            node_properties: None,
        };
        let shared_digest = blake3_digest(&shared.encode_to_vec());
        let root = Directory {
            files: vec![],
            directories: vec![
                DirectoryNode {
                    name: "left".to_string(),
                    digest: Some(Digest::from(&shared_digest)),
                },
                DirectoryNode {
                    name: "right".to_string(),
                    digest: Some(Digest::from(&shared_digest)),
                },
            ],
            symlinks: vec![],
            node_properties: None,
        };

        let plan = build_materialize_plan(&root, &[shared])
            .expect("diamond must be accepted, not rejected as a cycle");

        let dirs: Vec<String> = plan.dirs.iter().map(|p| p.display().to_string()).collect();
        assert!(dirs.contains(&"left".to_string()), "left dir missing: {dirs:?}");
        assert!(dirs.contains(&"right".to_string()), "right dir missing: {dirs:?}");
        let files: Vec<String> = plan
            .files
            .iter()
            .map(|f| f.rel_path.display().to_string())
            .collect();
        assert!(
            files.contains(&"left/leaf.bin".to_string()),
            "left/leaf.bin missing: {files:?}"
        );
        assert!(
            files.contains(&"right/leaf.bin".to_string()),
            "right/leaf.bin missing: {files:?}"
        );
    }

    // FIX 5 T2: an in-tree symlink must be rejected (no symlink-injection surface,
    // §9).
    #[nativelink_test]
    async fn in_tree_symlink_is_rejected() {
        let root = Directory {
            files: vec![],
            directories: vec![],
            symlinks: vec![SymlinkNode {
                name: "link".to_string(),
                target: "elsewhere".to_string(),
                node_properties: None,
            }],
            node_properties: None,
        };

        assert_eq!(
            build_materialize_plan(&root, &[]).err(),
            Some("tree contains symlinks"),
        );
    }

    // FIX 5 T3: `validate_component` rejects `..`, path separators, and NUL, and
    // accepts a plain name.
    #[nativelink_test]
    async fn validate_component_rejects_traversal_separator_nul() {
        assert_eq!(validate_component(".."), Err("invalid path component"));
        assert_eq!(
            validate_component("a/b"),
            Err("path component contains a separator or NUL")
        );
        assert_eq!(
            validate_component("/"),
            Err("path component contains a separator or NUL")
        );
        assert_eq!(
            validate_component("a\0b"),
            Err("path component contains a separator or NUL")
        );
        assert_eq!(validate_component("libfoo.rlib"), Ok(()));
    }

    // -- FL-1383 §3 seed_dest_dir: the DECLARED NESTED -incr output -----------
    #[nativelink_test]
    async fn seed_dest_dir_uses_declared_nested_incr_output() {
        let execroot = std::path::Path::new("/Volumes/CrowAgent/fl-incr-execroots/deadbeef");
        // Empty working_directory: output_paths are execroot-relative as declared.
        let outputs = vec![
            "bazel-out/cfg/bin/pkg/libfoo-a1b2c3.rlib".to_string(),
            "bazel-out/cfg/bin/pkg/libfoo-a1b2c3.rmeta".to_string(),
            "bazel-out/cfg/bin/pkg/foo-incr".to_string(),
            "bazel-out/cfg/bin/pkg/foo-incr-metadata".to_string(),
        ];
        // The seed materializes at the NESTED declared -incr output joined onto
        // the execroot — NOT a top-level `<stem>-incr` child.
        assert_eq!(
            seed_dest_dir(execroot, "", &outputs),
            Some(execroot.join("bazel-out/cfg/bin/pkg/foo-incr")),
            "seed dest must be the declared nested -incr output joined onto the execroot"
        );
        // `working_directory == "."` collapses in the join (the `.` component
        // normalizes away against the absolute execroot), matching the empty case
        // and `prepare_output_directory`'s `{execroot}/./{output}` parent.
        assert_eq!(
            seed_dest_dir(execroot, ".", &outputs),
            Some(execroot.join("bazel-out/cfg/bin/pkg/foo-incr")),
            "working_directory == \".\" must collapse to the same dest as empty"
        );
        // No -incr output declared -> None (cold, no fetch).
        assert_eq!(
            seed_dest_dir(execroot, "", &["bazel-out/cfg/bin/pkg/libfoo.rlib".to_string()]),
            None,
            "no -incr output must yield None (cold, no fetch)"
        );
        // `-incr-metadata` alone is the pipelined tree, NOT the singular seed dir.
        assert_eq!(
            seed_dest_dir(execroot, "", &["pkg/foo-incr-metadata".to_string()]),
            None,
            "-incr-metadata alone is the pipelined tree, not the singular seed dir"
        );
        // Path-traversal defense (§9): absolute / `..` -incr entries are refused.
        assert_eq!(
            seed_dest_dir(execroot, "", &["/etc/evil-incr".to_string()]),
            None,
            "an absolute -incr output_path must be refused (execroot-escape guard)"
        );
        assert_eq!(
            seed_dest_dir(execroot, "", &["../../escape-incr".to_string()]),
            None,
            "a `..` -incr output_path must be refused (execroot-escape guard)"
        );
    }

    // -- FL-1383 §3 seed_dest_dir: HONORS Command.working_directory (pair-a #2) --
    //
    // REAPI `output_paths` are relative to `Command.working_directory`;
    // `prepare_output_directory` creates the seed's parent at
    // `{execroot}/{working_directory}/{output_path}`. `seed_dest_dir` MUST use the
    // SAME formula — otherwise the seed lands where no parent was created (cold) or
    // where rustc (cwd `execroot/working_directory`) never reads `-Cincremental`
    // (dark reuse-collapse: `incr_index_fetch_hit` climbs, `incr_reuse_fired` flat).
    #[nativelink_test]
    async fn seed_dest_dir_honors_non_empty_working_directory() {
        let execroot = std::path::Path::new("/Volumes/CrowAgent/fl-incr-execroots/deadbeef");
        let working_directory = "k8-fastbuild/bin";
        // output_paths are RELATIVE to working_directory (the REAPI contract).
        let outputs = vec![
            "pkg/libfoo-a1b2c3.rlib".to_string(),
            "pkg/foo-incr".to_string(),
        ];
        assert_eq!(
            seed_dest_dir(execroot, working_directory, &outputs),
            Some(execroot.join("k8-fastbuild/bin/pkg/foo-incr")),
            "seed dest must honor Command.working_directory (join it before the declared \
             -incr output), matching where prepare_output_directory creates the parent; \
             an execroot-only join DROPS working_directory and darkens reuse"
        );
    }

    // -- FL-1383 plan_seed_publish (design §6.2) ------------------------------
    #[nativelink_test]
    async fn plan_seed_publish_only_on_success_with_incr_folder() {
        let tk = targetkey();
        let incr = blake3_digest(b"the-incr-tree");
        // A rustc action declaring both a `-incr` seed tree and other outputs.
        let folders = || {
            [
                ("bazel-out/cfg/bin/pkg/libfoo-incr", incr),
                ("bazel-out/cfg/bin/pkg", blake3_digest(b"other")),
            ]
        };

        // SUCCESS + an -incr folder -> publish a fetch-compatible index value.
        let publish = plan_seed_publish(&tk, 0, false, folders())
            .expect("a clean build with an -incr folder must publish");
        assert_eq!(
            publish.index_digest,
            index_action_digest(&tk),
            "index key must be hash(targetkey)"
        );
        let action_result =
            ActionResult::decode(publish.encoded.clone()).expect("published value decodes");
        let output_dir = action_result
            .output_directories
            .first()
            .expect("value has an output_directory");
        assert_eq!(
            output_dir.path,
            tk.primary_output(),
            "value path MUST be the primary output so the chunk-3 fetch collision guard accepts it"
        );
        assert_eq!(
            DigestInfo::try_from(output_dir.tree_digest.as_ref().expect("tree_digest present"))
                .expect("tree_digest valid"),
            incr,
            "value tree_digest MUST be the -incr tree, not another output"
        );

        // FAILED build (nonzero exit) -> None: never publish a torn seed.
        assert!(
            plan_seed_publish(&tk, 1, false, folders()).is_none(),
            "a nonzero exit code must NOT publish (torn/partial seed)"
        );
        // Internal error -> None.
        assert!(
            plan_seed_publish(&tk, 0, true, folders()).is_none(),
            "an internal error must NOT publish"
        );
        // SUCCESS but no -incr folder -> None (nothing to seed).
        assert!(
            plan_seed_publish(&tk, 0, false, [("bazel-out/cfg/bin/pkg/libfoo.rlib", incr)])
                .is_none(),
            "no -incr output folder means nothing to publish"
        );
        // `-incr-metadata` is the distinct pipelined tree (§4), NOT the index seed.
        assert!(
            plan_seed_publish(&tk, 0, false, [("pkg/libfoo-incr-metadata", incr)]).is_none(),
            "-incr-metadata is the pipelined tree, not the single -incr the index carries"
        );
    }

    // -- FL-1383 §12 observability: counters render (not dark) ----------------
    #[nativelink_test]
    async fn incr_seed_metrics_render_pins_all_counter_names() {
        use core::sync::atomic::Ordering;

        use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};

        // A FRESH instance (not the process-global singleton) so the value
        // assertion is not raced by other tests that bump the singleton.
        let metrics = std::sync::Arc::new(IncrSeedMetrics::default());
        metrics.incr_seed_materialized.fetch_add(3, Ordering::Relaxed);
        let registry = MetricsRegistry::new();
        registry.register("incr_seed_index", metrics.clone());
        let body = render_prometheus(&registry);

        for name in [
            "incr_seed_materialized",
            "incr_reuse_fired",
            "incr_seed_present_but_cold",
            "incr_seed_collision",
            "incr_index_fetch_hit",
            "incr_index_fetch_miss",
            "incr_index_fetch_timeout",
            "incr_index_fetch_error",
            "incr_index_publish",
        ] {
            assert!(
                body.contains(name),
                "FL-1383 counter {name} must render — a rename must not silently re-dark it; body:\n{body}"
            );
        }
        assert!(
            body.contains("incr_seed_index_incr_seed_materialized 3"),
            "the materialized counter's VALUE must be wired into the render body, not merely a \
             struct field; body:\n{body}"
        );
    }

    #[nativelink_test]
    async fn incr_seed_metrics_singleton_arc_registers_and_renders() {
        use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};

        // The process-singleton arc (what bin/nativelink.rs registers) must be
        // registerable and render its names — the non-dark path.
        let registry = MetricsRegistry::new();
        registry.register("incr_seed_index", incr_seed_metrics_arc());
        let body = render_prometheus(&registry);
        assert!(
            body.contains("incr_seed_materialized"),
            "the process-singleton arc must register + render (non-dark); body:\n{body}"
        );
    }

    // A Store whose reads never resolve, to prove the bounded index timeout.
    use slow_store::SlowStore;
    // A Store that delegates to an inner MemoryStore but hangs `get_part` for a
    // configured digest set, to prove the overall deadline bounds a stalled blob
    // read mid-materialize.
    use selective_store::SelectiveStore;
    mod slow_store {
        use core::pin::Pin;
        use std::sync::Arc;

        use async_trait::async_trait;
        use futures::future::pending;
        use nativelink_error::Error;
        use nativelink_metric::{
            MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
        };
        use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
        use nativelink_util::health_utils::{
            HealthStatusIndicator, default_health_status_indicator,
        };
        use nativelink_util::store_trait::{
            DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation,
            StableDigestDelegation, Store, StoreDriver, StoreKey, UploadSizeInfo,
        };

        #[derive(Debug)]
        pub(super) struct SlowStore;

        impl MetricsComponent for SlowStore {
            fn publish(
                &self,
                _kind: MetricKind,
                _field_metadata: MetricFieldData,
            ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
                Ok(MetricPublishKnownKindData::Component)
            }
        }

        impl SlowStore {
            pub(super) fn new_store() -> Store {
                Store::new(Arc::new(SlowStore))
            }
        }

        #[async_trait]
        impl StoreDriver for SlowStore {
            async fn post_init(self: Arc<Self>) -> Result<(), Error> {
                Ok(())
            }

            async fn has_with_results(
                self: Pin<&Self>,
                _keys: &[StoreKey<'_>],
                results: &mut [Option<u64>],
            ) -> Result<(), Error> {
                for result in results.iter_mut() {
                    *result = None;
                }
                Ok(())
            }

            async fn update(
                self: Pin<&Self>,
                _key: StoreKey<'_>,
                _reader: DropCloserReadHalf,
                _size_info: UploadSizeInfo,
            ) -> Result<u64, Error> {
                pending().await
            }

            async fn get_part(
                self: Pin<&Self>,
                _key: StoreKey<'_>,
                _writer: &mut DropCloserWriteHalf,
                _offset: u64,
                _length: Option<u64>,
            ) -> Result<(), Error> {
                // Never resolves: the fetch can only end via the caller's
                // bounded timeout.
                pending().await
            }

            fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
                self
            }

            fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
                self
            }

            fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
                self
            }

            fn register_item_callback(
                self: Arc<Self>,
                _callback: Arc<dyn ItemCallback>,
            ) -> Result<(), Error> {
                Ok(())
            }

            fn stable_delegation(&self) -> StableDigestDelegation<'_> {
                StableDigestDelegation::Leaf
            }

            fn pin_delegation(&self) -> PinDelegation<'_> {
                PinDelegation::Leaf
            }

            fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
                MarkStableDelegation::Leaf
            }

            fn durable_delegation(&self) -> DurableDelegation<'_> {
                DurableDelegation::Leaf
            }
        }

        default_health_status_indicator!(SlowStore);
    }

    mod selective_store {
        use core::pin::Pin;
        use std::collections::HashSet;
        use std::sync::Arc;

        use async_trait::async_trait;
        use futures::future::pending;
        use nativelink_error::Error;
        use nativelink_metric::{
            MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
        };
        use nativelink_store::memory_store::MemoryStore;
        use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
        use nativelink_util::common::DigestInfo;
        use nativelink_util::health_utils::{
            HealthStatusIndicator, default_health_status_indicator,
        };
        use nativelink_util::store_trait::{
            DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation,
            StableDigestDelegation, Store, StoreDriver, StoreKey, UploadSizeInfo,
        };

        /// Wraps an inner [`MemoryStore`], delegating everything EXCEPT it hangs
        /// `get_part` forever for any digest in `hang_on`. Lets a test serve the
        /// Tree read but stall a specific blob read mid-materialize.
        #[derive(Debug)]
        pub(super) struct SelectiveStore {
            inner: Arc<MemoryStore>,
            hang_on: HashSet<DigestInfo>,
        }

        impl MetricsComponent for SelectiveStore {
            fn publish(
                &self,
                _kind: MetricKind,
                _field_metadata: MetricFieldData,
            ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
                Ok(MetricPublishKnownKindData::Component)
            }
        }

        impl SelectiveStore {
            pub(super) fn new_store(
                inner: Arc<MemoryStore>,
                hang_on: impl IntoIterator<Item = DigestInfo>,
            ) -> Store {
                Store::new(Arc::new(Self {
                    inner,
                    hang_on: hang_on.into_iter().collect(),
                }))
            }
        }

        #[async_trait]
        impl StoreDriver for SelectiveStore {
            async fn post_init(self: Arc<Self>) -> Result<(), Error> {
                Ok(())
            }

            async fn has_with_results(
                self: Pin<&Self>,
                keys: &[StoreKey<'_>],
                results: &mut [Option<u64>],
            ) -> Result<(), Error> {
                Pin::new(self.inner.as_ref())
                    .has_with_results(keys, results)
                    .await
            }

            async fn update(
                self: Pin<&Self>,
                key: StoreKey<'_>,
                reader: DropCloserReadHalf,
                size_info: UploadSizeInfo,
            ) -> Result<u64, Error> {
                Pin::new(self.inner.as_ref())
                    .update(key, reader, size_info)
                    .await
            }

            async fn get_part(
                self: Pin<&Self>,
                key: StoreKey<'_>,
                writer: &mut DropCloserWriteHalf,
                offset: u64,
                length: Option<u64>,
            ) -> Result<(), Error> {
                if let StoreKey::Digest(digest) = &key {
                    if self.hang_on.contains(digest) {
                        // Never resolves: only the caller's overall deadline ends it.
                        return pending().await;
                    }
                }
                Pin::new(self.inner.as_ref())
                    .get_part(key, writer, offset, length)
                    .await
            }

            fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
                self
            }

            fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
                self
            }

            fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
                self
            }

            fn register_item_callback(
                self: Arc<Self>,
                _callback: Arc<dyn ItemCallback>,
            ) -> Result<(), Error> {
                Ok(())
            }

            fn stable_delegation(&self) -> StableDigestDelegation<'_> {
                StableDigestDelegation::Leaf
            }

            fn pin_delegation(&self) -> PinDelegation<'_> {
                PinDelegation::Leaf
            }

            fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
                MarkStableDelegation::Leaf
            }

            fn durable_delegation(&self) -> DurableDelegation<'_> {
                DurableDelegation::Leaf
            }
        }

        default_health_status_indicator!(SelectiveStore);
    }
}
