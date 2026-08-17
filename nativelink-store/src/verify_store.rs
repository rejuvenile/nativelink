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
use core::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{error, warn};

use nativelink_config::stores::VerifySpec;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, WriteHalfGuard, make_buf_channel_pair_with_size,
};
use nativelink_util::common::{DigestInfo, PackedHash};
use nativelink_util::digest_hasher::{
    DigestFuncProver, DigestHasher, DigestHasherFunc, digest_hasher_func_from_context,
};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::metrics_utils::CounterWithTime;
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike,
    UploadSizeInfo,
};

/// `#fl1786`: sampling period for the per-proven-write `warn!`. Emit the
/// FIRST occurrence and every Nth thereafter, with the always-incremented
/// `digest_func_proven` counter carried on each emitted line as `cumulative`
/// so the true rate stays readable.
///
/// 64 matches `V2_LIFECYCLE_LOG_SAMPLE_PERIOD` in
/// `nativelink-service/src/chunked_write_handler_v2.rs`, established by
/// `34a18cda` ("#perf: rate-limit hot-path per-request info logs") for the
/// same failure mode: an un-rate-limited per-request line under a build
/// burst backs up the nonblocking log writer, and this server has a standing
/// write-burst-stall incident.
pub const DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD: u64 = 64;

/// Whether to EMIT the proven-write `warn!` at cumulative occurrence `count`
/// (1-based). Pure and deterministic so it can be pinned directly.
#[must_use]
pub const fn digest_func_proven_log_decision(count: u64) -> bool {
    count == 1 || count % DIGEST_FUNC_PROVEN_LOG_SAMPLE_PERIOD == 0
}

/// Hash- and size-verifying wrapper. Re-hashes incoming/outgoing streams
/// and rejects values where `H(bytes) != key`.
///
/// **CAS-only.** `verify_hash` and `verify_size` only make sense on a
/// content-addressed store (where the key IS the hash of the bytes). The
/// AC chain is keyed by `action_digest` (the CAS digest of the *Action*
/// proto), but the value is the serialized *ActionResult* proto —
/// `H(value) != key` in general, so wrapping the AC chain in
/// `VerifyStore { verify_hash = true }` would reject every write.
/// Production wires `cas_STORE` through `VerifyStore` (intentional) and
/// the `AC_STORE` chain without it (intentional).
///
/// See `docs/ac-integrity-contract.md` for the AC contract and what
/// integrity primitives ARE applicable there.
#[derive(Debug, MetricsComponent)]
pub struct VerifyStore {
    #[metric(group = "inner_store")]
    inner_store: Store,
    #[metric(help = "If the verification store is verifying the size of the data")]
    verify_size: bool,
    #[metric(help = "If the verification store is verifying the hash of the data")]
    verify_hash: bool,

    // Metrics.
    #[metric(help = "Number of failures the verification store had due to size mismatches")]
    size_verification_failures: CounterWithTime,
    #[metric(help = "Number of failures the verification store had due to hash mismatches")]
    hash_verification_failures: CounterWithTime,
    /// `#fl1786-server-side-digest-function-proving` ENGAGED-MECHANISM
    /// signal. Bumped once per write whose bytes did NOT reproduce the
    /// declared digest under the function the write was LABELLED with, but
    /// DID reproduce it under another advertised function — i.e. a write
    /// that was rejected forever before this fix.
    ///
    /// NOT a data-integrity signal on the WRITE side:
    /// `hash_verification_failures` keeps that role and is deliberately NOT
    /// bumped for a proven write. (`hash_verification_failures` is also
    /// bumped by the READ-side mismatch in `get_part`, which after
    /// `810ef707` is the client-digest-function read check — so it already
    /// carries two roles and an operator cannot tell a rejected corrupt
    /// write from a failed read by that counter alone. Pre-existing; stated
    /// here because this field's doc used to claim otherwise.)
    ///
    /// This counter is the TRUE rate: the accompanying `warn!` is sampled
    /// (`digest_func_proven_log_decision`), so the log undercounts by design
    /// and this does not.
    #[metric(
        help = "Writes accepted only after PROVING the digest function from the blob's own bytes (declared digest did not reproduce under the labelled function but did under another advertised one). Do NOT alert — non-zero is mislabelled traffic being rescued rather than latched."
    )]
    digest_func_proven: CounterWithTime,

    /// `#fl1786-read-half` ENGAGED-MECHANISM signal for the READ side, the
    /// mirror of `digest_func_proven`. Bumped once per read whose served
    /// bytes did NOT reproduce the declared digest under the function
    /// resolved from the READER's context, but DID reproduce it under
    /// another advertised function on the proving re-read.
    ///
    /// This is the one signal that distinguishes "the read half never
    /// fires" from "the read half is rescuing traffic". Without it, the
    /// falsifier both review rounds asked for is unimplementable: a read
    /// rescued by proving leaves no trace anywhere else, because by
    /// construction it now looks exactly like a successful read.
    ///
    /// Like its write-side twin this is the TRUE rate; the accompanying
    /// `warn!` is sampled by `digest_func_proven_log_decision`.
    #[metric(
        help = "Reads served only after PROVING the digest function from the blob's own bytes on a re-read (the served bytes did not reproduce the declared digest under the function resolved from the reader's context, but did under another advertised one). Do NOT alert — non-zero is a mislabelled blob being served rather than latched behind DataLoss."
    )]
    digest_func_proven_on_read: CounterWithTime,

    /// `#fl1786-read-half`: the READ-side share of
    /// `hash_verification_failures`, which carries TWO roles — the write-side
    /// reject (`inner_check_update`'s no-candidate arm) and the read-side
    /// unprovable mismatch. An operator alerting on the aggregate could not
    /// tell a rejected corrupt WRITE from a failed READ, and both review
    /// rounds proposed a falsifier that depended on telling them apart.
    ///
    /// This is a DECOMPOSITION, not a move: the aggregate still ticks on
    /// every read failure, so every pre-existing alert and dashboard keeps
    /// working, and `hash_verification_failures - hash_verification_failures_on_read`
    /// is the write-side reject count.
    ///
    /// Bumped only when proving has ALREADY FAILED, so a rescued read never
    /// touches the integrity alarm.
    #[metric(
        help = "The READ-side share of hash_verification_failures: reads that failed hash verification AND could not be rescued by proving the digest function. Subtract from hash_verification_failures to get the write-side reject count."
    )]
    hash_verification_failures_on_read: CounterWithTime,
}

/// Why a proving re-read did not rescue a read. Kept as a type rather than a
/// bare `Option` so the failure `error!` can name WHICH of three very
/// different things happened — an operator triaging a `DataLoss` needs to
/// know whether the blob is corrupt, unreadable, or unstable, and all three
/// otherwise collapse into one indistinguishable log line.
#[derive(Debug)]
enum ReadProof {
    /// A candidate reproduced the declared digest. The blob was mislabelled,
    /// not corrupt.
    Proven(DigestHasherFunc),
    /// The re-read succeeded and NO advertised function reproduced the
    /// declared digest. The bytes are genuinely corrupt.
    Unprovable,
    /// The re-read itself failed (the blob was evicted between the two
    /// passes, or the inner store faulted). Proving did not RUN, so it did
    /// not find anything — and "could not look" must fail closed exactly
    /// like "looked and found nothing".
    Unreadable(Error),
    /// The re-read returned DIFFERENT bytes than the pass that was streamed
    /// to the caller: the labelled function's hash over pass 2 does not
    /// equal its hash over pass 1. Whatever pass 2 proves says nothing about
    /// the bytes the caller already holds, so this must fail closed. Only
    /// reachable if the inner store is non-deterministic for one key, which
    /// is itself a corruption.
    Inconsistent,
}

impl ReadProof {
    /// Operator-facing reason, attached to the failure `error!`.
    const fn reason(&self) -> &'static str {
        match self {
            Self::Proven(_) => "proven",
            Self::Unprovable => "no advertised digest function reproduces the declared digest",
            Self::Unreadable(_) => "the proving re-read of the inner store failed",
            Self::Inconsistent => {
                "the proving re-read returned different bytes than the pass that was served"
            }
        }
    }
}

/// Outcome of the streaming verification pass in [`inner_check_get_part`].
///
/// [`inner_check_get_part`]: VerifyStore::inner_check_get_part
#[derive(Debug)]
enum ReadVerifyOutcome {
    /// Verified, or verification disabled. The writer HAS been terminated
    /// (EOF) and the read is finished.
    Verified,
    /// The served bytes did not reproduce the declared hash under the
    /// function resolved from the reader's context. The writer has
    /// deliberately NOT been terminated — see
    /// [`VerifyStore::inner_check_get_part`].
    HashMismatch { computed: PackedHash },
}

impl VerifyStore {
    /// Returns a reference to the wrapped inner store.
    pub fn inner_store(&self) -> &Store {
        &self.inner_store
    }

    /// Writes accepted only after proving the digest function from the
    /// blob's own bytes. See the `digest_func_proven` field.
    pub fn digest_func_proven_count(&self) -> u64 {
        self.digest_func_proven.counter.load(Ordering::Acquire)
    }

    /// Writes rejected because no advertised digest function reproduced the
    /// declared digest — the data-integrity alarm.
    pub fn hash_verification_failure_count(&self) -> u64 {
        self.hash_verification_failures
            .counter
            .load(Ordering::Acquire)
    }

    /// Reads served only after proving the digest function from the blob's
    /// own bytes. See the `digest_func_proven_on_read` field.
    pub fn digest_func_proven_on_read_count(&self) -> u64 {
        self.digest_func_proven_on_read
            .counter
            .load(Ordering::Acquire)
    }

    /// The READ-side share of `hash_verification_failures`. See the
    /// `hash_verification_failures_on_read` field.
    pub fn hash_verification_failure_on_read_count(&self) -> u64 {
        self.hash_verification_failures_on_read
            .counter
            .load(Ordering::Acquire)
    }

    pub fn new(spec: &VerifySpec, inner_store: Store) -> Arc<Self> {
        Arc::new(Self {
            inner_store,
            verify_size: spec.verify_size,
            verify_hash: spec.verify_hash,
            size_verification_failures: CounterWithTime::default(),
            hash_verification_failures: CounterWithTime::default(),
            digest_func_proven: CounterWithTime::default(),
            digest_func_proven_on_read: CounterWithTime::default(),
            hash_verification_failures_on_read: CounterWithTime::default(),
        })
    }

    /// `#fl1786-server-side-digest-function-proving`: when `verify_hash` is
    /// on, `maybe_prover` carries a [`DigestFuncProver`] plus the function
    /// the write was LABELLED with (the resource name's digest-function
    /// segment, or the process default when the segment was omitted).
    ///
    /// The label is a CLAIM by the writer. The digest is a CHECKABLE one, and
    /// this function is holding the bytes, so at EOF acceptance is decided by
    /// "do these bytes reproduce the declared digest under ANY advertised
    /// function", not by "do they reproduce it under the claimed one".
    ///
    /// Unlike the chunked-commit site, proving here CANNOT be gated on the
    /// mismatch: the stream is consumed as it is forwarded to the inner
    /// store, so there is nothing to re-read on the rejection path. Buffering
    /// it to enable a retry would be an unbounded in-process buffer on a
    /// network path, and letting the inner write commit first so it could be
    /// re-read would open a window where an unverified blob is readable. So
    /// every candidate is folded in during the one pass.
    ///
    /// **Cost: one extra hash over the same bytes.** Measured on buildcache
    /// (EPYC 7302, `-C target-cpu=native`, load avg 2.3-2.7, two runs):
    /// BLAKE3 3.08-3.18 GB/s, SHA-256 1.611 GB/s single-thread, so the added
    /// SHA-256 pass is **+0.65 ms per MiB**. Sized against the server's real
    /// write-size distribution rather than a blob COUNT, because hashing cost
    /// is proportional to BYTES: the CAS chain's `SizePartitioning` splits at
    /// 16 KiB and the >16 KiB partition carries **99.92% of written bytes**
    /// (mean ~2.31 MiB, so ~1.5 ms of added CPU per mean blob), while the
    /// ≤16 KiB partition is 50% by count but **0.077% by bytes** (mean
    /// ~1.8 KiB, ~1.2 µs). At the measured 2.33 MB/s average server ingest
    /// that is ~0.0015 of one core; even at `eth0`'s 40 GbE line rate — which
    /// CAS ingest cannot reach, being bounded by 10 Mac workers and a SATA
    /// RAIDZ1 `tank` — it is ~3.1 of 64 cores. The choice is insensitive to
    /// the ingest-rate assumption over three orders of magnitude, which is
    /// why there is no size gate and no request-class gate.
    ///
    /// Do NOT re-import the "8 KiB blobs are 73% of traffic" figure from
    /// `nativelink-util/src/fs.rs:475-476`: that is a READ-path comment about
    /// the WORKER's unpartitioned local store, it never says 8 KiB, and
    /// `deferred_tasks.md:518` exists specifically to correct this
    /// substitution.
    ///
    /// The per-poll consequence, which the per-core framing hides: the
    /// deployed listeners set `experimental_http2_max_frame_size: 4194304`
    /// (`buildcache-native.json5:474/574/687`), so a single non-yielding
    /// `prover.update()` over a 4 MiB frame goes from ~1.4 ms (blake3 only)
    /// to ~4.1 ms. That is a fairness cost on the tokio worker, not a new
    /// CPU total.
    async fn inner_check_update(
        &self,
        mut tx: DropCloserWriteHalf,
        mut rx: DropCloserReadHalf,
        maybe_expected_digest_size: Option<u64>,
        digest: &DigestInfo,
        mut maybe_prover: Option<(DigestFuncProver, DigestHasherFunc)>,
    ) -> Result<(), Error> {
        let original_hash = digest.packed_hash();
        // Writer-termination contract for `tokio::join!(update_fut, check_fut)`:
        // `update_fut` (= `inner_store.update(digest, rx, ...)`) reads from
        // `rx`. If `inner_check_update` returns Err WITHOUT terminating
        // `tx`, `rx.recv()` from the inner store synthesizes a generic
        // `Code::Internal "Sender dropped before sending EOF"`
        // (`buf_channel.rs:582`). Because `tokio::join!` does not
        // short-circuit, the inner store's update keeps polling and
        // surfaces that derivative log
        // (`FastSlowStore::update (chunked): data stream failed`) instead
        // of the actionable upstream cause (size mismatch / hash
        // mismatch / etc.). #245 in production: ~15 events / 10-min on
        // ≥18 MB blobs.
        //
        // The function takes `tx` BY VALUE, so the guard wraps a local
        // `&mut` borrow of the owned `tx`. On every Err early return the
        // guard's `Drop` synthesizes a structured Internal so the paired
        // `rx.recv()` returns a structured error instead of the generic
        // "Sender dropped" one — and in the common case below we
        // EXPLICITLY `guard.fail(err.clone())` so the actionable upstream
        // err (e.g. "Hashes do not match") flows through to the merged
        // result rather than the synthesized fallback. Mirrors the
        // pattern in `verify_store::get_part` (the "Three-way commit"
        // doc-comment block — search the file for that phrase to land
        // on the precedent).
        let mut tx_guard = WriteHalfGuard::new(&mut tx);
        let mut sum_size: u64 = 0;
        loop {
            let chunk = rx
                .recv()
                .await
                .err_tip(|| "Failed to read chunk in check_update in verify store")
                .map_err(|err| tx_guard.fail(err))?;
            sum_size += chunk.len() as u64;

            // Ensure if a user sends us too much data we fail quickly.
            if let Some(expected_size) = maybe_expected_digest_size {
                match sum_size.cmp(&expected_size) {
                    core::cmp::Ordering::Greater => {
                        self.size_verification_failures.inc();
                        return Err(tx_guard.fail(make_input_err!(
                            "Expected size {} but already received {} on insert",
                            expected_size,
                            sum_size
                        )));
                    }
                    core::cmp::Ordering::Equal => {
                        // Ensure our next chunk is the EOF chunk.
                        // If this was an error it'll be caught on the .recv()
                        // on next cycle.
                        if let Ok(eof_chunk) = rx.peek().await {
                            if !eof_chunk.is_empty() {
                                self.size_verification_failures.inc();
                                return Err(tx_guard.fail(make_input_err!(
                                    "Expected EOF chunk when exact size was hit on insert in verify store - {}",
                                    expected_size,
                                )));
                            }
                        }
                    }
                    core::cmp::Ordering::Less => {}
                }
            }

            // If is EOF.
            if chunk.is_empty() {
                if let Some(expected_size) = maybe_expected_digest_size {
                    if sum_size != expected_size {
                        self.size_verification_failures.inc();
                        return Err(tx_guard.fail(make_input_err!(
                            "Expected size {} but got size {} on insert",
                            expected_size,
                            sum_size
                        )));
                    }
                }
                if let Some((prover, labelled_func)) = maybe_prover.take() {
                    // `#fl1786`: one finalize of every candidate. A match is
                    // unambiguous — two functions both reproducing a
                    // well-formed digest would be a hash collision — and the
                    // comparison is over the WHOLE `DigestInfo` (hash AND
                    // size), so a truncated or extended body proves nothing.
                    let candidates = prover.finalize_all();
                    // Match on the HASH alone, then check the size
                    // SEPARATELY. `verify_hash: true` + `verify_size: false`
                    // is a reachable configuration — both fields are
                    // `#[serde(default)]` and the `verify_hash` doc names no
                    // coupling — and in it the earlier size checks are
                    // skipped, so a blob whose bytes hash correctly but whose
                    // DECLARED size is wrong lands here. Comparing the whole
                    // `DigestInfo` folded that into the hash verdict and
                    // produced "Hashes do not match, got: X but digest hash
                    // was X" (identical hashes — nonsense to an operator)
                    // while charging a size-declaration bug to the
                    // data-integrity alarm. Both faults are still REJECTED;
                    // only the attribution changes.
                    match candidates
                        .iter()
                        .find(|(_, computed)| computed.packed_hash() == original_hash)
                    {
                        Some((_, computed)) if computed.size_bytes() != digest.size_bytes() => {
                            self.size_verification_failures.inc();
                            return Err(tx_guard.fail(make_input_err!(
                                "Expected size {} but got size {} on insert",
                                digest.size_bytes(),
                                computed.size_bytes()
                            )));
                        }
                        Some((proven_func, _)) if *proven_func == labelled_func => {}
                        Some((proven_func, _)) => {
                            // The label was wrong but the bytes are intact.
                            // Pre-proving this write was rejected, the
                            // producer re-solicited, and it failed
                            // identically forever.
                            self.digest_func_proven.inc();
                            // SAMPLED. Proving SUCCEEDING is what makes this
                            // hot: pre-fix a `--digest_function=sha256`
                            // client was rejected on every blob so it could
                            // not sustain traffic; post-fix it WORKS and one
                            // build uploads tens of thousands of blobs, each
                            // one landing here. `warn!` is NOT compiled out
                            // in release (`release_max_level_info`). The
                            // always-incremented counter carries the true
                            // rate; `cumulative` puts it on every emitted
                            // line. Same shape and constant as the
                            // WriteChunkedV2 lifecycle logs (`34a18cda`).
                            let cumulative = self.digest_func_proven_count();
                            if digest_func_proven_log_decision(cumulative) {
                                warn!(
                                    %original_hash,
                                    %labelled_func,
                                    %proven_func,
                                    cumulative,
                                    "accepted a write whose digest function was PROVEN from its \
                                     own bytes; the declared digest does not reproduce under the \
                                     function the write was labelled with. The producer \
                                     mislabelled (or omitted) the function"
                                );
                            }
                        }
                        None => {
                            // FAIL-CLOSED: no PROVABLE_DIGEST_FUNCS candidate
                            // reproduces the declared digest, so the bytes are
                            // CORRUPT rather than mislabelled. ("Candidate",
                            // not "advertised": since `#single-digest`
                            // `GetCapabilities` advertises only the configured
                            // default, while proving deliberately tries the
                            // wider set — see PROVABLE_DIGEST_FUNCS' doc.)
                            // Report the hash under the LABELLED function so
                            // the message is byte-identical to the pre-proving
                            // one — it is pinned by operators, dashboards and
                            // `zero_copy_write_corruption_test`.
                            self.hash_verification_failures.inc();
                            let hash_result = candidates
                                .iter()
                                .find(|(func, _)| *func == labelled_func)
                                // `PROVABLE_DIGEST_FUNCS` is the FULL
                                // `DigestHasherFunc` set (see its doc), so the
                                // labelled function is always a candidate;
                                // these fallbacks only keep the expression
                                // total.
                                .or_else(|| candidates.first())
                                .map_or(*original_hash, |(_, computed)| *computed.packed_hash());
                            return Err(tx_guard.fail(make_input_err!(
                                "Hashes do not match, got: {original_hash} but digest hash was {hash_result}",
                            )));
                        }
                    }
                }
                tx_guard
                    .commit_eof()
                    .err_tip(|| "In verify_store::check_update")?;
                break;
            }

            // This will allows us to hash while sending data to another thread.
            let write_future = (*tx_guard).send(chunk.clone());

            if let Some((prover, _)) = maybe_prover.as_mut() {
                prover.update(chunk.as_ref());
            }

            if let Err(err) = write_future
                .await
                .err_tip(|| "Failed to write chunk to inner store in verify store")
            {
                // Mid-stream `tx.send` failure means `rx` (the inner
                // store's read half) was already closed (inner store
                // errored or the join's other future dropped). The
                // `send_error` is harmless on a closed channel and the
                // structured err is preserved on the returned Result.
                return Err(tx_guard.fail(err));
            }
        }
        Ok(())
    }

    /// Verifies data read from the inner store by hashing and size-checking
    /// each chunk as it streams through to the caller's writer.
    ///
    /// Writer-termination contract (#336 P1): the borrowed `writer` is the
    /// OUTER caller's writer. Direct `StoreLike::get` callers own and drop
    /// the writer at function end, so missing termination is invisible at
    /// that boundary. But any wrapping caller that joins `(get_fut,
    /// check_fut)` over the writer's tx/rx pair (e.g. an upstream
    /// VerifyStore composed atop us, or any future composition that pairs
    /// our writer with a reader inside `tokio::join!`) deadlocks if we
    /// return Err mid-stream WITHOUT propagating a structured error to the
    /// reader. The size-mismatch and hash-mismatch branches now call
    /// `writer.send_error(err.clone())` explicitly before returning so the
    /// reader observes the structured DataLoss instead of a generic
    /// "Sender dropped" Internal.
    ///
    /// We do NOT use `WriteHalfGuard` here. Drop-fallback would set
    /// `terminal_error = synthesized Internal` on every `?`-propagation
    /// exit (e.g. `rx.recv().err_tip(...)?` when the inner store returned
    /// NotFound) — but in that case the OUTER `tokio::join!` in
    /// `verify_store::get_part` already propagates the structured upstream
    /// error to the caller via the joined Result. The reader would
    /// observe a noisy synthesized Internal that shadows the structured
    /// upstream error semantics on the writer side, breaking established
    /// test+production behavior. The over-action regression caught by
    /// `cdn_cache_failure_*` tests during #336 P1 development.
    async fn inner_check_get_part<D: DigestHasher>(
        &self,
        writer: &mut DropCloserWriteHalf,
        mut rx: DropCloserReadHalf,
        maybe_expected_size: Option<u64>,
        original_hash: &PackedHash,
        mut maybe_hasher: Option<&mut D>,
    ) -> Result<ReadVerifyOutcome, Error> {
        let mut sum_size: u64 = 0;
        loop {
            let chunk = rx
                .recv()
                .await
                .err_tip(|| "Failed to read chunk in check_get_part in verify store")?;

            // EOF
            if chunk.is_empty() {
                if let Some(expected_size) = maybe_expected_size {
                    if sum_size != expected_size {
                        self.size_verification_failures.inc();
                        error!(
                            expected_size,
                            actual_size = sum_size,
                            "size mismatch on read in verify store"
                        );
                        let err = make_err!(
                            Code::DataLoss,
                            "Expected size {} but got size {} on read",
                            expected_size,
                            sum_size
                        );
                        // #336 P1: terminate the OUTER writer with the
                        // structured DataLoss so any wrapping caller
                        // that joins on the writer's tx/rx pair sees
                        // the specific code instead of deadlocking on
                        // an un-EOF'd writer. Idempotent — safe even
                        // if the writer was already terminated.
                        writer.send_error(err.clone());
                        return Err(err);
                    }
                }
                if let Some(hasher) = maybe_hasher.as_mut() {
                    let digest = hasher.finalize_digest();
                    let hash_result = digest.packed_hash();
                    if original_hash != hash_result {
                        // `#fl1786-read-half`: do NOT decide here, do NOT
                        // count, and above all do NOT terminate the writer.
                        //
                        // Every payload byte has already been forwarded and
                        // the writer is still un-terminated (neither
                        // `send_eof` nor `send_error` has run), which is
                        // exactly the state the happy path is in three lines
                        // below. That is what makes a LATE `Ok` representable
                        // at all: `send_error` sets `terminal_error` (a
                        // `OnceLock`) and `eof_sent`
                        // (`buf_channel.rs:390-403`), both one-way, so a
                        // verdict published here could never be revised by
                        // the proving pass.
                        //
                        // The bytes are also not the problem — a mislabelled
                        // blob's bytes are CORRECT and are already correctly
                        // in the caller's hands. Only the label was wrong.
                        return Ok(ReadVerifyOutcome::HashMismatch {
                            computed: *hash_result,
                        });
                    }
                }
                writer
                    .send_eof()
                    .err_tip(|| "In verify_store::check_get_part sending eof")?;
                break;
            }

            sum_size += chunk.len() as u64;

            // Hash while forwarding to the caller's writer.
            let write_future = writer.send(chunk.clone());

            if let Some(hasher) = maybe_hasher.as_mut() {
                hasher.update(chunk.as_ref());
            }

            write_future
                .await
                .err_tip(|| "Failed to forward chunk to writer in verify store get_part")?;
        }
        Ok(ReadVerifyOutcome::Verified)
    }

    /// `#fl1786-read-half`: MISMATCH-GATED proving for the read path.
    ///
    /// Re-reads the blob from the inner store and folds every advertised
    /// candidate over it in ONE pass, then publishes the verdict on the
    /// still-un-terminated writer: `send_eof` when a candidate reproduces the
    /// declared digest, the unchanged structured `DataLoss` when none does.
    ///
    /// **Why mismatch-gated and not always-prove.** Reads dominate this
    /// system (writes measured at 2.33 MB/s average), so the happy path pays
    /// NOTHING: one hasher, one inner read, byte-for-byte the pre-change
    /// code. This mirrors the chunked-commit site, which re-reads `.holding`
    /// only after pass 1 mismatched. The write site could not do this — it
    /// consumes its stream as it forwards it, with nothing to re-read — but
    /// the read side has a re-readable source by construction, which is what
    /// makes the cheaper shape available here.
    ///
    /// **The cost that IS paid, stated plainly.** On the proving path this is
    /// one extra FULL read of the blob through the whole inner chain
    /// (ExistenceCache → SizePartitioning → {Memory/Redis, FastSlow}), which
    /// is more expensive per event than the write side's extra hash pass. And
    /// it is not a one-off: a mislabelled blob read repeatedly by a
    /// context-less reader pays it on EVERY read, because nothing records the
    /// proven function anywhere. That is the standing argument for recording
    /// the digest function beside the blob instead of re-deriving it at each
    /// boundary. It is still strictly better than the status quo, where those
    /// reads simply fail.
    ///
    /// **Fail-closed in every direction.** An unreadable re-read, an
    /// inconsistent re-read, and a re-read that proves nothing all produce
    /// the SAME `DataLoss` with the SAME message. "Could not look" is never
    /// "looked and found nothing".
    async fn prove_read_or_fail(
        &self,
        digest: DigestInfo,
        writer: &mut DropCloserWriteHalf,
        labelled_func: DigestHasherFunc,
        computed: &PackedHash,
    ) -> Result<(), Error> {
        let original_hash = digest.packed_hash();
        let proof = self.reread_and_prove(digest, labelled_func, computed).await;

        if let ReadProof::Proven(proven_func) = proof {
            self.digest_func_proven_on_read.inc();
            // SAMPLED, for the same reason as the write side: proving
            // SUCCEEDING is what makes this hot. A context-less internal
            // reader (e.g. the scheduler's prefetch / cache-warm / tree
            // resolution tasks, which cross a bare `tokio::spawn` and so
            // lose the ambient function) walks whole directory trees, and
            // `warn!` is NOT compiled out in release
            // (`release_max_level_info`). The counter carries the true rate.
            let cumulative = self.digest_func_proven_on_read_count();
            if digest_func_proven_log_decision(cumulative) {
                warn!(
                    %original_hash,
                    %labelled_func,
                    %proven_func,
                    cumulative,
                    "served a read whose digest function was PROVEN from the blob's own bytes; \
                     the declared digest does not reproduce under the function this reader \
                     resolved. The blob was admitted under a different advertised function"
                );
            }
            return writer
                .send_eof()
                .err_tip(|| "In verify_store::prove_read_or_fail sending eof");
        }

        // FAIL-CLOSED. Counter, log and error are all byte-identical to the
        // pre-proving ones except for the added `proving` attribution: FOUR
        // pre-existing assertions match on the message string
        // (`verify_store_test.rs:429/566/818/826`) and operators grep it.
        // (The "20 pre-existing cases in two files" figure this comment
        // carried until 2026-08-16 was `verify_store_test.rs`'s total test
        // count; the two `bytestream_read_digest_context_test.rs` hits are
        // doc-comments, not assertions. pair-a T-6.)
        self.hash_verification_failures.inc();
        self.hash_verification_failures_on_read.inc();
        let reread_err = match &proof {
            ReadProof::Unreadable(err) => Some(err.to_string()),
            _ => None,
        };
        error!(
            %original_hash,
            hash_result = %computed,
            proving = proof.reason(),
            ?reread_err,
            "hash mismatch on read in verify store"
        );
        let err = make_err!(
            Code::DataLoss,
            "Hash mismatch on read: expected {original_hash} but got {computed}",
        );
        // #336 P1: terminate the OUTER writer with the structured DataLoss so
        // any wrapping caller that joins on the writer's tx/rx pair sees the
        // specific code instead of deadlocking on an un-EOF'd writer.
        writer.send_error(err.clone());
        Err(err)
    }

    /// One extra read of the blob, folding every [`PROVABLE_DIGEST_FUNCS`]
    /// candidate in a single pass.
    ///
    /// `PROVABLE_DIGEST_FUNCS` is exactly the set the server advertises in
    /// its Capabilities response, so this can only accept a blob under a rule
    /// the server has announced.
    ///
    /// **Why this is re-entrancy-safe.** The first read is FULLY COMPLETE
    /// when this runs: `get_part`'s `tokio::join!` has already returned, so
    /// `get_fut` has finished and its `tx` has been dropped. This is a plain
    /// sequential second read of the same key — what any two concurrent
    /// clients of a CAS do — not a nested read inside a live one.
    ///
    /// **But it is NOT side-effect-free on the chain below, in both
    /// directions.** Deployed, this store is `cas_STORE` and `inner_store` is
    /// `cas_INNER`, an `ExistenceCacheStore` (`buildcache-native.json5:183-190`
    /// / `:220-236`), whose `get_part` mutates cache state on BOTH arms:
    ///
    /// - **`Unprovable`** (the blob really is corrupt): the digest is
    ///   re-inserted and its LRU recency refreshed
    ///   (`existence_cache_store.rs:884-892`) once per pass, so a failed read
    ///   now refreshes it TWICE. `VerifyStore` sits ABOVE the cache, so the
    ///   `DataLoss` it mints one frame up can never reach the eviction arm —
    ///   the read latch is self-reinforcing, and this doubles the rate for
    ///   exactly the population that should be evicted. Not a capacity
    ///   concern at `max_count: 50000000`; a correctness-of-signal one
    ///   (`has()` keeps answering present, harder).
    /// - **`Unreadable`** (the blob vanished between the passes): this is the
    ///   FIRST mechanism in this chain that CAN clear a stale entry. The
    ///   re-read's `NotFound` comes from BELOW the cache, so
    ///   `existence_cache_store.rs:893` (`is_unrecoverable_read_error`,
    ///   `:68`) fires and the stale entry is removed. The single-read path
    ///   could never do that.
    ///
    /// Pass 2 also re-enters `FastSlowStore::get_part` for the >16 KiB
    /// partition, which spawns or dedups onto a populate producer on a
    /// fast-tier miss (`fast_slow_store.rs:7767`). Like the extra store I/O,
    /// none of this amortises — nothing records the proven function.
    ///
    /// [`PROVABLE_DIGEST_FUNCS`]: nativelink_util::digest_hasher::PROVABLE_DIGEST_FUNCS
    async fn reread_and_prove(
        &self,
        digest: DigestInfo,
        labelled_func: DigestHasherFunc,
        first_pass_hash: &PackedHash,
    ) -> ReadProof {
        // 4 slots: the prover folds at memory speed, so the channel never
        // needs depth. Same figure and reasoning as `get_part`'s.
        let (tx, rx) = make_buf_channel_pair_with_size(4);
        let read_fut = async move {
            let mut tx = tx;
            let mut tx_guard = WriteHalfGuard::new(&mut tx);
            let res = self
                .inner_store
                .get_part(digest, &mut *tx_guard, 0, None)
                .await;
            match &res {
                Ok(()) => tx_guard.commit_delegated_if_ok(&res),
                Err(err) => {
                    let _ = tx_guard.fail(err.clone());
                }
            }
            res
        };
        let prove_fut = async move {
            let mut rx = rx;
            let mut prover = DigestFuncProver::new();
            loop {
                let chunk = rx
                    .recv()
                    .await
                    .err_tip(|| "Failed to read chunk in verify_store::reread_and_prove")?;
                if chunk.is_empty() {
                    break;
                }
                prover.update(chunk.as_ref());
            }
            Ok::<_, Error>(prover)
        };
        let (read_res, prove_res) = tokio::join!(read_fut, prove_fut);

        if let Err(err) = read_res {
            return ReadProof::Unreadable(err);
        }
        let prover = match prove_res {
            Ok(prover) => prover,
            Err(err) => return ReadProof::Unreadable(err),
        };
        let candidates = prover.finalize_all();

        // The two passes must have seen the SAME bytes. The caller already
        // holds pass 1's bytes; a proof over pass 2's bytes says nothing
        // about them unless they are identical, and comparing the LABELLED
        // candidate's hash across the passes is an exact, free check —
        // `finalize_all` computed it anyway. Without this, a store that
        // served corrupt bytes once and intact bytes the next time would have
        // its corrupt read certified `Ok`.
        let same_bytes = candidates
            .iter()
            .find(|(func, _)| *func == labelled_func)
            .is_some_and(|(_, pass2)| pass2.packed_hash() == first_pass_hash);
        if !same_bytes {
            return ReadProof::Inconsistent;
        }

        // Whole-`DigestInfo` equality (hash AND size), the same comparison
        // `DigestFuncProver::prove` makes: a truncated or over-long copy
        // proves nothing, which is the correct answer.
        candidates
            .into_iter()
            .find(|(_, candidate)| *candidate == digest)
            .map_or(ReadProof::Unprovable, |(func, _)| ReadProof::Proven(func))
    }
}

#[async_trait]
impl StoreDriver for VerifyStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        self.inner_store.clone().into_inner().post_init().await?;
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner_store.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        let StoreKey::Digest(digest) = key else {
            return Err(make_input_err!(
                "Only digests are supported in VerifyStore. Got {key:?}"
            ));
        };
        let digest_size = digest.size_bytes();
        if let UploadSizeInfo::ExactSize(expected_size) = size_info
            && self.verify_size
            && expected_size != digest_size
        {
            self.size_verification_failures.inc();
            return Err(make_input_err!(
                "Expected size to match. Got {} but digest says {} on update",
                expected_size,
                digest_size
            ));
        }

        // `#fl1786`: the labelled function is snapshotted here (as the single
        // hasher used to be) and carried alongside the prover so the
        // rejection message can still name the hash under the label the
        // writer claimed.
        let maybe_prover = if self.verify_hash {
            Some((DigestFuncProver::new(), digest_hasher_func_from_context()))
        } else {
            None
        };

        let maybe_digest_size = if self.verify_size {
            Some(digest_size)
        } else {
            None
        };
        let (tx, rx) = make_buf_channel_pair_with_size(256);

        let update_fut = self.inner_store.update(digest, rx, size_info);
        let check_fut =
            self.inner_check_update(tx, reader, maybe_digest_size, &digest, maybe_prover);

        let (update_res, check_res) = tokio::join!(update_fut, check_fut);

        match (update_res, check_res) {
            // Prioritize the check future's error, as it's more specific.
            (_, Err(e)) | (Err(e), Ok(_)) => Err(e),
            (Ok(size), Ok(_)) => Ok(size),
        }
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Only verify full reads with a digest key — partial reads cannot
        // be hash-verified and string keys have no expected digest.
        let should_verify = (self.verify_hash || self.verify_size)
            && offset == 0
            && length.is_none()
            && matches!(key, StoreKey::Digest(_));

        if !should_verify {
            return self.inner_store.get_part(key, writer, offset, length).await;
        }

        let StoreKey::Digest(digest) = key else {
            unreachable!("checked above");
        };

        // `#fl1786-read-half`: the function this READER resolved — the
        // resource name's digest-function segment when one was parsed into
        // the context, else the process default. Snapshotted (as the single
        // hasher used to be) and carried alongside it, because the proving
        // pass needs to name it in the rescue log and to cross-check that
        // both passes read the same bytes.
        //
        // It is a CLAIM about somebody else's blob, and a weak one: REAPI
        // says a SHA-256 client MUST OMIT the segment
        // (`remote_execution.proto:235-240`), and a bare `tokio::spawn` drops
        // the ambient context entirely (unlike `nativelink_util::task::spawn!`,
        // which re-attaches it at `task.rs:104-111`) — which is how the
        // scheduler's prefetch, cache-warm and tree-resolution reads reach
        // this store with nothing but the process default.
        let labelled_func = digest_hasher_func_from_context();
        let mut hasher = if self.verify_hash {
            Some(labelled_func.hasher())
        } else {
            None
        };

        let maybe_expected_size = if self.verify_size {
            Some(digest.size_bytes())
        } else {
            None
        };

        // The hasher processes at memory speed (~GB/s), so the channel
        // never needs deep buffering. 4 slots keeps memory low and avoids
        // excess context-switch overhead from a 256-slot channel.
        let (tx, rx) = make_buf_channel_pair_with_size(4);

        // Writer-termination contract for `tokio::join!(get_fut, check_fut)`:
        // if `inner_store.get_part` returns Err WITHOUT terminating `tx`,
        // `check_fut`'s `rx.recv().await` blocks forever and the join
        // deadlocks (multi-hour Bazel build wedges historically).
        //
        // We MOVE `tx` INTO `get_fut` (so it drops when get_fut completes,
        // not at outer-scope end after join) AND wrap with `WriteHalfGuard`
        // owned by the future. The move alone is enough to break the
        // deadlock (mpsc Sender drop wakes the receiver with a generic
        // "Sender dropped" Internal); the guard upgrades that to a
        // structured "buf_channel: writer dropped without commit" Internal
        // that operators can grep for in tonic::Status messages, with the
        // verbose verb-naming diagnostic logged separately via
        // `tracing::error!` (target=buf_channel::write_half_guard_drop).
        // See `WriteHalfGuard` rustdoc and the composability harness in
        // `nativelink-store/tests/composability_test.rs`.
        //
        // **Three-way commit (#186 fix, mirrors FastSlowStore #191):**
        //   - Ok(()): suppress Drop fallback; inner already sent EOF.
        //   - Err(e): EXPLICITLY terminate via `tx_guard.fail(e.clone())`
        //     to send the structured error to the receiver AND suppress
        //     the Drop fallback. Previously this used
        //     `commit_delegated_if_ok(&res)` which left the Drop fallback
        //     armed on every Err — firing the loud
        //     `"WriteHalfGuard fired Drop fallback: function exited
        //     without explicit commit"` `error!` log on every legitimate
        //     inner-store NotFound (600+/min sustained in production,
        //     contributing to the OOM trajectory at deploy +15-25 min).
        //     Drop fallback was originally a defensive net for inner
        //     stores that violated the contract themselves; per the
        //     contract-wide audit, all relevant leaf stores now satisfy
        //     the contract (filesystem, memory, redis, gcs, etc.), so
        //     the noisy Drop log was a false alarm on the COMMON case.
        //     Genuine inner-store contract violations now surface in the
        //     composability harness (`verify_store_around_*`) directly.
        let get_fut = async move {
            let mut tx = tx;
            let mut tx_guard = WriteHalfGuard::new(&mut tx);
            let res = self
                .inner_store
                .get_part(digest, &mut *tx_guard, 0, None)
                .await;
            match &res {
                Ok(()) => tx_guard.commit_delegated_if_ok(&res),
                Err(err) => {
                    let _ = tx_guard.fail(err.clone());
                }
            }
            res
        };
        let check_fut = self.inner_check_get_part(
            writer,
            rx,
            maybe_expected_size,
            digest.packed_hash(),
            hasher.as_mut(),
        );

        let (get_res, check_res) = tokio::join!(get_fut, check_fut);

        // `#fl1786-read-half`: the join has RETURNED, so the first read is
        // fully complete and its `tx` is dropped. Proving therefore happens
        // strictly after it — a sequential second read, not a nested one.
        // This is also the only point at which the writer's verdict can still
        // go either way.
        match check_res {
            Ok(ReadVerifyOutcome::Verified) => get_res,
            Ok(ReadVerifyOutcome::HashMismatch { computed }) => get_res.merge(
                self.prove_read_or_fail(digest, writer, labelled_func, &computed)
                    .await,
            ),
            Err(err) => get_res.merge(Err(err)),
        }
    }

    /// Delegates directly to the inner store **without** hash or size
    /// verification. The single-key [`get_part`] path streams data through
    /// [`inner_check_get_part`] which hashes every byte and checks the
    /// final size, but this batch path intentionally skips that work.
    ///
    /// This is acceptable for the current callers:
    ///
    /// - **GetTree BFS** (`get_tree_bfs`): directory protos returned by
    ///   this method are immediately decoded via `prost::Message::decode`,
    ///   which rejects malformed / truncated data.
    /// - **`BatchReadBlobs`**: blobs are returned to remote clients who
    ///   verify content hashes themselves per the REAPI contract.
    ///
    /// **Trade-off**: a corrupt or truncated blob could be served without
    /// detection by this store layer, whereas the streaming `get_part()`
    /// path would catch it. The risk is mitigated by the callers above
    /// but is not zero — a bit-flip that still parses as valid protobuf
    /// (or a blob consumed without client-side hash verification) would
    /// go unnoticed.
    ///
    /// TODO: optionally verify the blake3 hash of each blob returned
    /// here, at the cost of one hash computation per blob.
    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        Pin::new(self.inner_store.as_store_driver())
            .batch_get_part_unchunked(keys, length)
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        self.inner_store.register_item_callback(callback)
    }

    /// VerifyStore is a single-inner wrapper. `Inner` makes the trait
    /// defaults forward `drain_stable_digests` / `stable_notify` /
    /// `pin_digests` / `pin_digests_with_results` / `drain_failed_digests`
    /// unchanged to `inner_store`. No per-method override needed.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner_store.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner_store.as_store_driver())
    }

    /// `mark_stable` forwards unchanged to `inner_store` via the trait
    /// default's `Inner` arm (task #157 / C+D folded mark_stable into the
    /// forced-delegation enum mechanism).
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner_store.as_store_driver())
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner_store.as_store_driver())
    }
}

default_health_status_indicator!(VerifyStore);
