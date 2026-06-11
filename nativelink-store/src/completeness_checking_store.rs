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

use core::pin::Pin;
use core::{iter, mem};
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{FutureExt, TryFutureExt, select};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, OutputDirectory as ProtoOutputDirectory, Tree as ProtoTree,
};
use nativelink_util::ac_pin_registry::SharedAcPinRegistry;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::metrics_utils::CounterWithTime;
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::Notify;
use tracing::{debug, warn};

/// Callable returning `true` iff the given `cas_endpoint` is currently
/// connected. Mirrors `SharedLivenessChecker` in `ac_server.rs` (defined
/// separately to avoid a circular dep between nativelink-store and
/// nativelink-service).
///
/// `None` in CCS = kill-switch: all consults are skipped → current behavior.
pub type SharedLivenessChecker = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Newtype wrapper around `SharedLivenessChecker` that provides a trivial
/// `Debug` impl so the parent struct can derive `Debug` without a blanket
/// `impl Debug for dyn Fn` (which Rust does not provide). The wrapper is
/// transparent at runtime — all operations delegate to the inner checker.
struct LivenessCheckerDebug(SharedLivenessChecker);

impl core::fmt::Debug for LivenessCheckerDebug {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LivenessChecker").finish_non_exhaustive()
    }
}

use crate::ac_utils::{get_and_decode_digest, get_size_and_decode_digest};

/// Safety bound for AC entry sizes fetched into memory.
const MAX_ACTION_MSG_SIZE: usize = 10 << 20; // 10mb.

/// Given a proto action result, return all relevant digests and
/// output directories that need to be checked.
fn get_digests_and_output_dirs(
    action_result: ProtoActionResult,
) -> Result<(Vec<StoreKey<'static>>, Vec<ProtoOutputDirectory>), Error> {
    // TODO(palfrey) When `try_collect()` is stable we can use it instead.
    let mut digest_iter = action_result
        .output_files
        .into_iter()
        .filter_map(|file| file.digest.map(DigestInfo::try_from))
        .chain(action_result.stdout_digest.map(DigestInfo::try_from))
        .chain(action_result.stderr_digest.map(DigestInfo::try_from));
    let mut digest_infos = Vec::with_capacity(digest_iter.size_hint().1.unwrap_or(0));
    digest_iter
        .try_for_each(|maybe_digest| {
            digest_infos.push(maybe_digest?.into());
            Result::<_, Error>::Ok(())
        })
        .err_tip(|| "Some digests could not be converted to DigestInfos")?;
    Ok((digest_infos, action_result.output_directories))
}

/// Given a list of output directories recursively get all digests
/// that need to be checked and pass them into `handle_digest_infos_fn`
/// as they are found.
#[expect(clippy::future_not_send)] // TODO(jhpratt) remove this
async fn check_output_directories<'a>(
    cas_store: &Store,
    output_directories: Vec<ProtoOutputDirectory>,
    handle_digest_infos_fn: &impl Fn(Vec<StoreKey<'a>>),
) -> Result<(), Error> {
    let mut futures = FuturesUnordered::new();

    let tree_digests = output_directories
        .into_iter()
        .filter_map(|output_dir| output_dir.tree_digest.map(DigestInfo::try_from));
    for maybe_tree_digest in tree_digests {
        let tree_digest = maybe_tree_digest
            .err_tip(|| "Could not decode tree digest CompletenessCheckingStore::has")?;
        futures.push(async move {
            let tree = get_and_decode_digest::<ProtoTree>(cas_store, tree_digest.into()).await?;
            // TODO(palfrey) When `try_collect()` is stable we can use it instead.
            // https://github.com/rust-lang/rust/issues/94047
            let mut digest_iter = tree.children.into_iter().chain(tree.root).flat_map(|dir| {
                dir.files
                    .into_iter()
                    .filter_map(|f| f.digest.map(DigestInfo::try_from))
            });

            let mut digest_infos = Vec::with_capacity(digest_iter.size_hint().1.unwrap_or(0));
            digest_iter
                .try_for_each(|maybe_digest| {
                    digest_infos.push(maybe_digest?.into());
                    Result::<_, Error>::Ok(())
                })
                .err_tip(|| "Expected digest to exist and be convertible")?;
            handle_digest_infos_fn(digest_infos);
            Ok(())
        });
    }

    while let Some(result) = futures.next().await {
        match result {
            Ok(()) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[derive(Debug, MetricsComponent)]
pub struct CompletenessCheckingStore {
    cas_store: Store,
    ac_store: Store,

    #[metric(help = "Incomplete entries hit in CompletenessCheckingStore")]
    incomplete_entries_counter: CounterWithTime,
    #[metric(help = "Complete entries hit in CompletenessCheckingStore")]
    complete_entries_counter: CounterWithTime,
    /// (#12 H4 phase 3) Optional registry for pending worker-output locality.
    ///
    /// When `Some` and CAS `has_with_results` reports a digest MISSING, the
    /// consult checks this registry: if any endpoint holds the digest AND that
    /// endpoint passes `liveness_checker`, the digest is treated as PRESENT
    /// (fetchable via `WorkerProxyStore` peer-fetch at actual read time).
    ///
    /// `None` = kill-switch: all consults are skipped → current behavior.
    ///
    /// SHORT-CIRCUIT BOUNDARY: the consult lives ONLY in the completeness gate
    /// (after CAS reports missing) — NOT in any general `has_with_results`
    /// path that bytestream/batch-update upload-dedup consults. CCS wraps the
    /// AC store; the CAS store is queried independently. Proof: the consult
    /// fires only inside `get_and_verify_single` (the `get_part` path) and
    /// inside `check_existence_fut` (the `has_with_results` path), BOTH of
    /// which are reachable only after decoding an AC entry — a code path that
    /// bytestream/batch-update never takes.
    /// Set once at startup by `inject_pending_registry` after the store
    /// chain is fully constructed. `OnceLock` gives a lock-free fast path
    /// (no mutex on reads). Not set = kill-switch: consult is skipped.
    pending_output_locality_registry: OnceLock<SharedAcPinRegistry>,
    /// Liveness checker companion. Set at the same time as the registry.
    /// Wrapped in `LivenessCheckerDebug` so the struct can derive `Debug`.
    /// Not set = kill-switch.
    liveness_checker: OnceLock<LivenessCheckerDebug>,
    // CAPPED AT 1: single monotonically-increasing u64; no bounding needed.
    /// (#12 H4 phase 3) Count of CCS completeness verdicts flipped from
    /// "missing" → "present" by the pending-registry consult. Non-zero rate
    /// is the H4-fix effectiveness gauge: each increment represents an H4-class
    /// false-dangling event (AR published before large blob was server-visible)
    /// that was rescued instead of triggering a re-execute.
    ///
    /// Sustained zero after phase 3 lands = pending registry not populated
    /// (phase 2 wiring not active) or no H4 events in this window. Sustained
    /// non-zero = H4 events still occurring but now rescued instead of failing.
    #[metric(help = "CCS completeness verdicts rescued by pending-registry consult (H4 fix gauge)")]
    ccs_pending_registry_rescues_total: CounterWithTime,
    /// Raw atomic for the same counter — exposed as `pending_registry_rescues_total()`
    /// for test assertions. Incremented from both `get_and_verify_single` and
    /// `inner_has_with_results` paths; `CounterWithTime` carries a parking_lot
    /// `Mutex` for the timestamp, so the atomic is the fast path for callers
    /// that only need the count.
    pending_registry_rescues_raw: Arc<AtomicU64>,
}

impl CompletenessCheckingStore {
    pub fn new(ac_store: Store, cas_store: Store) -> Arc<Self> {
        Self::new_with_pending_registry(ac_store, cas_store, None, None)
    }

    /// Construct with optional `pending_output_locality_registry` and
    /// `liveness_checker` (H4 phase 3 consult). Called from `nativelink.rs`
    /// after the store chain is built, mirroring the `AcServer`
    /// `new_with_pending_registry` injection pattern.
    ///
    /// `registry = None` or `liveness_checker = None` → kill-switch: consult
    /// is skipped entirely, preserving current behavior.
    pub fn new_with_pending_registry(
        ac_store: Store,
        cas_store: Store,
        pending_output_locality_registry: Option<SharedAcPinRegistry>,
        liveness_checker: Option<SharedLivenessChecker>,
    ) -> Arc<Self> {
        let rescues_raw = Arc::new(AtomicU64::new(0));
        let store = Arc::new(Self {
            cas_store,
            ac_store,
            incomplete_entries_counter: CounterWithTime::default(),
            complete_entries_counter: CounterWithTime::default(),
            pending_output_locality_registry: OnceLock::new(),
            liveness_checker: OnceLock::new(),
            ccs_pending_registry_rescues_total: CounterWithTime::default(),
            pending_registry_rescues_raw: rescues_raw,
        });
        if let (Some(reg), Some(chk)) = (pending_output_locality_registry, liveness_checker) {
            // Errors only if already set — impossible on a freshly-constructed Arc.
            let _ = store.pending_output_locality_registry.set(reg);
            let _ = store.liveness_checker.set(LivenessCheckerDebug(chk));
        }
        store
    }

    /// Post-construction injection for deployments where the registry and
    /// liveness checker are available only after the store chain is built
    /// (i.e. `default_store_factory` builds CCS before `nativelink.rs` has
    /// constructed `WorkerApiServer`). Safe to call at most once — silently
    /// ignored if the OnceLock is already populated.
    pub fn inject_pending_registry(
        &self,
        registry: SharedAcPinRegistry,
        liveness_checker: SharedLivenessChecker,
    ) {
        let _ = self.pending_output_locality_registry.set(registry);
        let _ = self.liveness_checker.set(LivenessCheckerDebug(liveness_checker));
    }

    /// Returns the cumulative count of completeness verdicts rescued by the
    /// pending-registry consult (H4 phase 3 effectiveness gauge).
    pub fn pending_registry_rescues_total(&self) -> u64 {
        self.pending_registry_rescues_raw.load(Ordering::Acquire)
    }

    /// Consult the pending-output-locality registry for a single CAS `digest`
    /// that the cas_store reported MISSING. Returns `true` iff the registry
    /// holds the digest under an endpoint that currently passes the liveness
    /// check — meaning the blob is fetchable from that worker via
    /// `WorkerProxyStore` peer-fetch when actually read.
    ///
    /// Fires the rescue counter and a `debug!` log on a successful rescue so
    /// operators can track H4 event rates without the noise of the incomplete
    /// warn that would otherwise fire.
    ///
    /// # Short-circuit boundary
    ///
    /// This method is called ONLY from the "digest missing from CAS" branch in
    /// `get_and_verify_single` and `inner_has_with_results`. It is NEVER called
    /// from general `has_with_results` (which bytestream/batch-update use for
    /// upload dedup) — those paths never decode AC entries and never reach this
    /// code.
    /// NOTE: a successful rescue does NOT guarantee the blob is immediately
    /// peer-fetchable. The actual fetch path (WPS::get_part) consults
    /// `BlobLocalityMap` (populated from BlobsAvailable ticks, ~100ms period).
    /// A rescue may fire within one tick of UpdateActionResult, before the blob
    /// appears in locality_map. In that window the Bazel client receives a
    /// successful AC response but a subsequent NotFound on the output blob. A
    /// retry within one BlobsAvailable period succeeds. Configure
    /// `--remote_retries` to handle this window.
    fn consult_pending_registry(
        &self,
        ac_key: &StoreKey<'_>,
        digest: &DigestInfo,
    ) -> bool {
        let (Some(registry), Some(checker)) = (
            self.pending_output_locality_registry.get(),
            self.liveness_checker.get(),
        ) else {
            return false;
        };

        // Walk the per-endpoint sets in the registry. For each endpoint that
        // holds this digest, re-check liveness (endpoint may have disconnected
        // after publishing; wipe_endpoint races are possible). First live hit
        // wins.
        //
        // The inner map is read under RwLock::read() inside the registry; we
        // iterate with `endpoint_holds_digest` which takes one read lock per
        // endpoint. We use `endpoint_counts()` to enumerate endpoints once
        // and then query per-endpoint — allocates one Vec per call (bounded
        // by worker count, ~10 in production).
        let endpoints: Vec<String> = registry.endpoint_counts().keys().cloned().collect();
        for endpoint in &endpoints {
            if registry.endpoint_holds_digest(endpoint, digest) && (checker.0)(endpoint.as_str()) {
                // Rescued: this digest is present on a live worker and
                // fetchable via WorkerProxyStore peer-fetch at read time.
                self.ccs_pending_registry_rescues_total.inc();
                self.pending_registry_rescues_raw
                    .fetch_add(1, Ordering::AcqRel);
                debug!(
                    %ac_key,
                    ?digest,
                    %endpoint,
                    "CCS pending-registry rescue: digest missing from CAS but present \
                     on live worker endpoint (H4 invariant — blob fetchable via peer-fetch)",
                );
                return true;
            }
        }
        false
    }

    /// AC-side backing store accessor. Used by the `#168` startup
    /// pin-set walker (`find_fast_slow_for_pin` in `src/bin/nativelink.rs`)
    /// to recurse through `CompletenessCheckingStore` into the AC chain
    /// (`AC_BACKEND_CACHED = FastSlow{fast: MemoryStore, slow: RefStore→Redis}`)
    /// to find the underlying `FastSlowStore`. Without this accessor
    /// the walker bails at `inner_store(None)` (which returns `self`)
    /// and the AC dispatcher pin-set is never registered — making the
    /// AC fan-out path silently inert in production.
    pub fn ac_store(&self) -> &Store {
        &self.ac_store
    }

    /// Check that all files and directories in action results
    /// exist in the CAS. Does this by decoding digests and
    /// checking their existence in two separate sets of futures that
    /// are polled concurrently.
    async fn inner_has_with_results(
        &self,
        action_result_digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Holds shared state between the different futures.
        // This is how get around lifetime issues.
        struct State<'a> {
            results: &'a mut [Option<u64>],
            digests_to_check: Vec<StoreKey<'a>>,
            digests_to_check_idxs: Vec<usize>,
            notify: Arc<Notify>,
            done: bool,
        }
        // Note: In theory Mutex is not needed, but lifetimes are
        // very tricky to get right here. Since we are using parking_lot
        // and we are guaranteed to never have lock collisions, it should
        // be nearly as fast as a few atomic operations.
        let state_mux = &Mutex::new(State {
            results,
            digests_to_check: Vec::new(),
            digests_to_check_idxs: Vec::new(),
            // Note: Any time `digests_to_check` or `digests_to_check_idxs` is
            // modified we must notify the subscriber here.
            notify: Arc::new(Notify::new()),
            done: false,
        });

        let mut futures = action_result_digests
            .iter()
            .enumerate()
            .map(|(i, digest)| {
                async move {
                    // Note: We don't err_tip here because often have NotFound here which is ok.
                    let (action_result, size) = get_size_and_decode_digest::<ProtoActionResult>(
                        &self.ac_store,
                        digest.borrow(),
                    )
                    .await?;

                    let (mut digest_infos, output_directories) =
                        get_digests_and_output_dirs(action_result)?;

                    {
                        let mut state = state_mux.lock();

                        // We immediately set the size of the digest here. Later we will unset it if
                        // we find that the digest has missing outputs.
                        state.results[i] = Some(size);
                        let rep_len = digest_infos.len();
                        if state.digests_to_check.is_empty() {
                            // Hot path: Most actions only have files and only one digest
                            // requested to be checked. So we can avoid the heap allocation
                            // by just swapping out our container's stack if our pending_digests
                            // is empty.
                            mem::swap(&mut state.digests_to_check, &mut digest_infos);
                        } else {
                            state.digests_to_check.extend(digest_infos);
                        }
                        state
                            .digests_to_check_idxs
                            .extend(iter::repeat_n(i, rep_len));
                        state.notify.notify_one();
                    }

                    // Hot path: It is very common for no output directories to be defined.
                    // So we can avoid any needless work by early returning.
                    if output_directories.is_empty() {
                        return Ok(());
                    }

                    check_output_directories(
                        &self.cas_store,
                        output_directories,
                        &move |digest_infos| {
                            let mut state = state_mux.lock();
                            let rep_len = digest_infos.len();
                            state.digests_to_check.extend(digest_infos);
                            state
                                .digests_to_check_idxs
                                .extend(iter::repeat_n(i, rep_len));
                            state.notify.notify_one();
                        },
                    )
                    .await?;

                    Result::<(), Error>::Ok(())
                }
                // Add a tip to the error to help with debugging and the index of the
                // digest that failed so we know which one to unset. Always
                // append the ActionResult digest (including for NotFound),
                // so the downstream `warn!` for incomplete entries can name
                // which AC entry CCS filtered.
                .map_err(move |mut e| {
                    if e.code == Code::NotFound {
                        e = e.append(
                            format!("CCS flagged ActionResult ({digest}) incomplete — at least one referenced CAS blob is missing"),
                        );
                    } else {
                        e = e.append(
                            format!("Error checking existence of digest ({digest}) in CompletenessCheckingStore::has"),
                        );
                    }
                    (e, i)
                })
            })
            .collect::<FuturesUnordered<_>>();

        // This future will wait for the notify to be notified and then
        // check the CAS store for the digest's existence.
        // For optimization reasons we only allow one outstanding call to
        // the underlying `has_with_results()` at a time. This is because
        // we want to give the ability for stores to batch requests together
        // whenever possible.
        // The most common case is only one notify will ever happen.
        let check_existence_fut = async {
            let mut has_results = vec![];
            let notify = state_mux.lock().notify.clone();
            loop {
                notify.notified().await;
                let (digests, indexes) = {
                    let mut state = state_mux.lock();
                    if state.done {
                        if state.digests_to_check.is_empty() {
                            break;
                        }
                        // Edge case: It is possible for our `digest_to_check` to have had
                        // data added, `notify_one` called, then immediately `done` set to
                        // true. We protect ourselves by checking if we have digests if done
                        // is set, and if we do, let ourselves know to run again, but continue
                        // processing the data.
                        notify.notify_one();
                    }
                    (
                        mem::take(&mut state.digests_to_check),
                        mem::take(&mut state.digests_to_check_idxs),
                    )
                };
                assert_eq!(
                    digests.len(),
                    indexes.len(),
                    "Expected sizes to match in CompletenessCheckingStore::has"
                );

                // Recycle our results vector to avoid needless allocations.
                has_results.clear();
                has_results.resize(digests.len(), None);
                self.cas_store
                    .has_with_results(&digests, &mut has_results[..])
                    .await
                    .err_tip(
                        || "Error calling has_with_results() inside CompletenessCheckingStore::has",
                    )?;
                // #332: post-verification CAS pin removed. Originally
                // (`f9566c82` / `2015b32b`, March 2026) the verified
                // digests were pinned here to narrow the TOCTOU window
                // between the existence check and the worker's eventual
                // read. The pin had no matching unpin and relied on the
                // 120s `MokaEvictingMap::PIN_TIMEOUT_SECS` to release.
                // Once `MemoryStore::pin_digests` was reinstated by
                // `#334` Fix C, this accumulator started consuming the
                // 12 GB pin cap on `cas_FAST_SLOW_STORE.fast` (25 % of
                // the configured 48 GB `max_bytes`), racing
                // the BIS-feeder pin path that protects the
                // ≥2-replica durability invariant. The protection was
                // in any case vestigial: the FilesystemStore slow tier
                // covers a fast-tier eviction transparently for any
                // CAS read in the check-to-fetch window.
                //
                // (#12 H4 phase 3) For each digest the CAS store reports
                // MISSING, consult the pending-output-locality registry.
                // If the digest is present on a live worker endpoint,
                // do NOT clear results[index] — treat as present.
                //
                // SHORT-CIRCUIT BOUNDARY: this consult fires only here,
                // inside the completeness gate after an AC entry decode —
                // bytestream/batch-update upload-dedup never reaches this
                // code path (they call has_with_results directly on the
                // CAS store, bypassing CCS entirely).
                {
                    // Synthetic AC key placeholder for the debug log in
                    // consult_pending_registry. The exact AC key isn't
                    // threaded into the check_existence_fut closure (the
                    // inner futures only queue digests, not AC keys), so
                    // we use a zero digest. The log is best-effort
                    // observability; the counter is the primary metric.
                    let placeholder_key = StoreKey::Digest(DigestInfo::new([0u8; 32], 0));
                    let mut state = state_mux.lock();
                    for (r, (digest, index)) in
                        has_results.iter().zip(digests.iter().zip(indexes))
                    {
                        if r.is_none() {
                            // CAS reports missing — consult pending registry.
                            let rescued = if let StoreKey::Digest(digest_info) = digest.borrow() {
                                self.consult_pending_registry(&placeholder_key, &digest_info)
                            } else {
                                false
                            };
                            if !rescued {
                                state.results[index] = None;
                            }
                        }
                    }
                }
            }
            Result::<(), Error>::Ok(())
        }
        .fuse();
        tokio::pin!(check_existence_fut);

        loop {
            // Poll both futures at the same time.
            select! {
                r = check_existence_fut => {
                    return Err(make_err!(
                        Code::Internal,
                        "CompletenessCheckingStore's check_existence_fut ended unexpectedly {r:?}"
                    ));
                }
                maybe_result = futures.next() => {
                    match maybe_result {
                        Some(Ok(())) => self.complete_entries_counter.inc(),
                        Some(Err((err, i))) => {
                            self.incomplete_entries_counter.inc();
                            state_mux.lock().results[i] = None;
                            // Always log — operators previously had no
                            // visibility into the common case where an
                            // ActionResult is reported incomplete because
                            // a referenced output blob is missing from CAS
                            // (NotFound). The counter ticked but no log
                            // fired, so operators couldn't tell which
                            // AC entry CCS filtered. This silently fed
                            // Bazel "cache miss → re-execute" for actions
                            // whose AC entry was intact but whose
                            // referenced CAS blob was absent. The err
                            // carries the AC digest via the append in the
                            // map_err above.
                            if err.code == Code::NotFound {
                                warn!(
                                    ?err,
                                    "ActionResult incomplete — referenced CAS digest missing",
                                );
                            } else {
                                warn!(
                                    ?err,
                                    "Error checking existence of digest",
                                );
                            }
                        }
                        None => {
                            // We are done, so flag it done and ensure we notify the
                            // subscriber future.
                            {
                                let mut state = state_mux.lock();
                                state.done = true;
                                state.notify.notify_one();
                            }
                            check_existence_fut
                                .await
                                .err_tip(|| "CompletenessCheckingStore's check_existence_fut ended unexpectedly on last await")?;
                            return Ok(());
                        }
                    }
                }
            }
        }
        // Unreachable.
    }

    /// Fetch a single AC entry, verify CAS completeness, and return the
    /// raw bytes of the entry. This avoids the double-fetch that would
    /// occur if we called `inner_has_with_results` then `ac_store.get_part`.
    async fn get_and_verify_single(
        &self,
        key: StoreKey<'_>,
    ) -> Result<Bytes, Error> {
        // Step 1: Fetch the raw AC entry bytes once.
        let store_data = self
            .ac_store
            .as_store_driver_pin()
            .get_part_unchunked(key.borrow(), 0, Some(MAX_ACTION_MSG_SIZE as u64))
            .await
            .err_tip(|| "Failed to fetch AC entry in CompletenessCheckingStore::get_and_verify_single")?;

        // Step 2: Decode the AC entry.
        let action_result = ProtoActionResult::decode(store_data.clone())
            .map_err(|e| {
                make_err!(
                    Code::NotFound,
                    "Stored value appears to be corrupt: {e} - {key:?}"
                )
            })?;

        // Step 3: Extract CAS digests and output directories.
        let (mut digest_infos, output_directories) =
            get_digests_and_output_dirs(action_result)?;

        // Step 4: Collect additional digests from output directories.
        if !output_directories.is_empty() {
            let mut futures = FuturesUnordered::new();
            let tree_digests = output_directories
                .into_iter()
                .filter_map(|output_dir| output_dir.tree_digest.map(DigestInfo::try_from));
            for maybe_tree_digest in tree_digests {
                let tree_digest = maybe_tree_digest
                    .err_tip(|| "Could not decode tree digest in get_and_verify_single")?;
                futures.push(async move {
                    let tree = get_and_decode_digest::<ProtoTree>(
                        &self.cas_store,
                        tree_digest.into(),
                    )
                    .await?;
                    let mut digests = Vec::new();
                    for dir in tree.children.into_iter().chain(tree.root) {
                        for file in dir.files {
                            if let Some(digest) = file.digest {
                                digests.push(
                                    DigestInfo::try_from(digest)
                                        .err_tip(|| "Expected digest to exist and be convertible")?
                                        .into(),
                                );
                            }
                        }
                    }
                    Result::<Vec<StoreKey<'static>>, Error>::Ok(digests)
                });
            }
            while let Some(result) = futures.next().await {
                digest_infos.extend(result?);
            }
        }

        // Step 5: Batch-check all CAS digests.
        if !digest_infos.is_empty() {
            let mut has_results = vec![None; digest_infos.len()];
            self.cas_store
                .has_with_results(&digest_infos, &mut has_results)
                .await
                .err_tip(|| "Error checking CAS existence in get_and_verify_single")?;

            // #332: post-verification CAS pin removed (see the matching
            // explanation above in `inner_has_with_results`). The
            // existence check still gates the AC entry's completeness;
            // missing CAS digests still surface as `Code::NotFound` so
            // callers fall back to a full re-execute.
            //
            // #40 §5(1): collect every missing digest so operators get
            // per-digest evidence (which AC entry, which CAS digest) —
            // the counter alone produced 10K+ ticks with zero log trace,
            // blocking slow-tier-eviction vs mirror-write-loss
            // attribution.
            //
            // (#12 H4 phase 3) SHORT-CIRCUIT BOUNDARY: before treating any
            // missing digest as incomplete, consult the pending-output-locality
            // registry. A worker publishes its AC entry before the large blob
            // is server-visible (H4 window); the registry holds a hint that
            // the blob is present on the worker's CAS and fetchable via peer-
            // fetch. If ALL missing digests are rescued by the registry, the
            // AC entry is complete — do NOT warn, do NOT delete.
            //
            // gate(delete) ⇒ consult-first:
            //   1. CAS reports missing
            //   2. Registry consult: if rescued → treat as present → no incomplete
            //   3. Only genuinely-missing digests (not rescued) reach the
            //      delete-on-detection branch.
            //
            // Composite invariant: a registry-resident digest must NEVER trigger
            // delete-on-detection. The consult precedes the delete-on-detection
            // branch structurally (missing_digests is built from the post-consult
            // result), so this invariant is enforced by construction.
            let missing_digests: Vec<&StoreKey<'_>> = digest_infos
                .iter()
                .zip(has_results.iter())
                .filter_map(|(digest, r)| {
                    if r.is_some() {
                        // Present in CAS — no consult needed.
                        return None;
                    }
                    // CAS reports missing. Consult the pending registry.
                    // If any live endpoint holds this digest, treat as present.
                    if let StoreKey::Digest(digest_info) = digest.borrow() {
                        if self.consult_pending_registry(&key, &digest_info) {
                            // Rescued — treat as present.
                            return None;
                        }
                    }
                    Some(digest)
                })
                .collect();
            if !missing_digests.is_empty() {
                self.incomplete_entries_counter.inc();
                // Cap the logged list — a tree-heavy ActionResult can
                // reference thousands of files; missing_count carries
                // the full total.
                const MAX_LOGGED_MISSING: usize = 10;
                warn!(
                    ac_key = ?key,
                    missing_count = missing_digests.len(),
                    total_count = digest_infos.len(),
                    missing_digests =
                        ?&missing_digests[..missing_digests.len().min(MAX_LOGGED_MISSING)],
                    "ActionResult incomplete — referenced CAS digest(s) missing (get_part path)"
                );
                // #40 §2 delete-on-detection: remove the dangling AC entry so
                // the next lookup is a clean ECS miss rather than a repeated
                // expensive CCS decode + has_with_results + warn cycle
                // (measured 1.18× repeat rate before this fix). Failure to
                // remove is non-fatal — the NotFound to Bazel stands regardless;
                // log a warn so operators can detect a broken remove path.
                if let Err(remove_err) = self.ac_store.remove(key.borrow()).await {
                    warn!(
                        ac_key = ?key,
                        ?remove_err,
                        "ActionResult incomplete — delete-on-detection remove failed"
                    );
                }
                return Err(make_err!(
                    Code::NotFound,
                    "Digest found, but not all parts were found in CompletenessCheckingStore::get_part (missing {} of {} referenced CAS digests; first missing: {:?})",
                    missing_digests.len(),
                    digest_infos.len(),
                    missing_digests[0]
                ));
            }
        }

        self.complete_entries_counter.inc();
        Ok(store_data)
    }
}

#[async_trait]
impl StoreDriver for CompletenessCheckingStore {
    /// Delegate remove to the AC store (#40 §2). CCS itself holds no state
    /// for the AC key — the state lives in the AC chain (ECS + FSS).
    async fn remove(self: Pin<&Self>, key: StoreKey<'_>) -> Result<(), Error> {
        self.ac_store.remove(key).await
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner_has_with_results(keys, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.ac_store.update(key, reader, size_info).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Fetch the AC entry once, verify CAS completeness, and serve
        // the already-fetched bytes — avoiding a redundant second read.
        let store_data = self
            .get_and_verify_single(key.borrow())
            .await
            .err_tip(|| "when calling CompletenessCheckingStore::get_part")?;

        // Apply offset/length slicing.
        let data_len = store_data.len();
        let start = usize::try_from(offset).unwrap_or(data_len).min(data_len);
        let end = match length {
            Some(len) => {
                let len = usize::try_from(len).unwrap_or(data_len);
                start.saturating_add(len).min(data_len)
            }
            None => data_len,
        };
        let slice = store_data.slice(start..end);

        if !slice.is_empty() {
            writer
                .send(slice)
                .await
                .err_tip(|| "Failed to send data in CompletenessCheckingStore::get_part")?;
        }
        writer
            .send_eof()
            .err_tip(|| "Failed to send eof in CompletenessCheckingStore::get_part")?;
        Ok(())
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
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        // Composite registration is not atomic — see FastSlowStore for the
        // contract notes. No unregister API; warn loudly on partial failure.
        self.ac_store.register_item_callback(callback.clone())?;
        if let Err(err) = self.cas_store.register_item_callback(callback) {
            warn!(
                ?err,
                "CompletenessCheckingStore: cas_store register_item_callback failed AFTER \
                 ac_store succeeded — composite is in an asymmetric state. Trait has no \
                 unregister API; restart to recover."
            );
            return Err(err);
        }
        Ok(())
    }

    /// CompletenessCheckingStore wraps the AC store (entry-point for AC
    /// queries). The CAS store is consulted for verification reads only.
    /// External `drain_stable_digests` / `stable_notify` flow through
    /// the AC store path.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.ac_store.as_store_driver())
    }

    /// External pin requests forward to the AC store. (#332 deleted the
    /// post-verification CAS pin loop that previously fired inside
    /// `inner_has_with_results` / `get_and_verify_single`; see the
    /// in-line comments at those sites for the rationale.)
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.ac_store.as_store_driver())
    }

    /// `mark_stable` forwards to `ac_store` (matching the existing
    /// `stable_delegation`/`pin_delegation` pattern). The CompletenessChecking
    /// AC-only path is not currently a CAS BIS contributor, but the
    /// declaration prevents the silent-default trap. (Task #157.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.ac_store.as_store_driver())
    }
}

default_health_status_indicator!(CompletenessCheckingStore);
