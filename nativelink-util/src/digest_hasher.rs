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

use std::sync::OnceLock;

use blake3::Hasher as Blake3Hasher;
use bytes::BytesMut;
use futures::Future;
use nativelink_config::stores::ConfigDigestHashFunction;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_proto::build::bazel::remote::execution::v2::digest_function::Value as ProtoDigestFunction;
use opentelemetry::context::Context;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::common::DigestInfo;
use crate::fs;

static DEFAULT_DIGEST_HASHER_FUNC: OnceLock<DigestHasherFunc> = OnceLock::new();

/// Utility function to make a context with a specific hasher function set.
pub fn make_ctx_for_hash_func<H>(hasher: H) -> Result<Context, Error>
where
    H: TryInto<DigestHasherFunc>,
    H::Error: Into<Error>,
{
    let digest_hasher_func = hasher
        .try_into()
        .err_tip(|| "Could not convert into DigestHasherFunc")?;

    let new_ctx = Context::current_with_value(digest_hasher_func);

    Ok(new_ctx)
}

/// Get the default hasher.
pub fn default_digest_hasher_func() -> DigestHasherFunc {
    *DEFAULT_DIGEST_HASHER_FUNC.get_or_init(|| DigestHasherFunc::Sha256)
}

/// Get the hasher requested by the client from the active context (set via
/// [`make_ctx_for_hash_func`]), falling back to the default hasher.
pub fn digest_hasher_func_from_context() -> DigestHasherFunc {
    Context::current()
        .get::<DigestHasherFunc>()
        .map_or_else(default_digest_hasher_func, |v| *v)
}

/// Sets the default hasher to use if no hasher was requested by the client.
pub fn set_default_digest_hasher_func(hasher: DigestHasherFunc) -> Result<(), Error> {
    DEFAULT_DIGEST_HASHER_FUNC
        .set(hasher)
        .map_err(|_| make_err!(Code::Internal, "default_digest_hasher_func already set"))
}

/// Supported digest hash functions.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DigestHasherFunc {
    Sha256,
    Blake3,
}

impl MetricsComponent for DigestHasherFunc {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        format!("{self:?}").publish(kind, field_metadata)
    }
}

impl DigestHasherFunc {
    pub fn hasher(&self) -> DigestHasherImpl {
        self.into()
    }

    #[must_use]
    pub const fn proto_digest_func(&self) -> ProtoDigestFunction {
        match self {
            Self::Sha256 => ProtoDigestFunction::Sha256,
            Self::Blake3 => ProtoDigestFunction::Blake3,
        }
    }
}

impl From<ConfigDigestHashFunction> for DigestHasherFunc {
    fn from(value: ConfigDigestHashFunction) -> Self {
        match value {
            ConfigDigestHashFunction::Sha256 => Self::Sha256,
            ConfigDigestHashFunction::Blake3 => Self::Blake3,
        }
    }
}

impl TryFrom<ProtoDigestFunction> for DigestHasherFunc {
    type Error = Error;

    fn try_from(value: ProtoDigestFunction) -> Result<Self, Self::Error> {
        match value {
            ProtoDigestFunction::Sha256 => Ok(Self::Sha256),
            ProtoDigestFunction::Blake3 => Ok(Self::Blake3),
            v => Err(make_input_err!(
                "Unknown or unsupported digest function for proto conversion {v:?}"
            )),
        }
    }
}

impl TryFrom<&str> for DigestHasherFunc {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "SHA256" => Ok(Self::Sha256),
            "BLAKE3" => Ok(Self::Blake3),
            v => Err(make_input_err!(
                "Unknown or unsupported digest function for string conversion: {v:?}"
            )),
        }
    }
}

impl core::fmt::Display for DigestHasherFunc {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Sha256 => write!(f, "SHA256"),
            Self::Blake3 => write!(f, "BLAKE3"),
        }
    }
}

impl TryFrom<i32> for DigestHasherFunc {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        // Zero means not-set.
        if value == 0 {
            return Ok(default_digest_hasher_func());
        }
        match ProtoDigestFunction::try_from(value) {
            Ok(ProtoDigestFunction::Sha256) => Ok(Self::Sha256),
            Ok(ProtoDigestFunction::Blake3) => Ok(Self::Blake3),
            value => Err(make_input_err!(
                "Unknown or unsupported digest function for int conversion: {:?}",
                value.map(|v| v.as_str_name())
            )),
        }
    }
}

impl From<&DigestHasherFunc> for DigestHasherImpl {
    fn from(value: &DigestHasherFunc) -> Self {
        let hash_func_impl = match value {
            DigestHasherFunc::Sha256 => DigestHasherFuncImpl::Sha256(Sha256::new()),
            DigestHasherFunc::Blake3 => DigestHasherFuncImpl::Blake3(Box::default()),
        };
        Self {
            hashed_size: 0,
            hash_func_impl,
        }
    }
}

/// Wrapper to compute a hash of arbitrary data.
pub trait DigestHasher {
    /// Update the hasher with some additional data.
    fn update(&mut self, input: &[u8]);

    /// Finalize the hash function and collect the results into a digest.
    fn finalize_digest(&mut self) -> DigestInfo;

    /// Specialized version of the hashing function that is optimized for
    /// handling files. These optimizations take into account things like,
    /// the file size and the hasher algorithm to decide how to best process
    /// the file and feed it into the hasher.
    fn digest_for_file(
        self,
        file_path: impl AsRef<std::path::Path>,
        file: fs::FileSlot,
        size_hint: Option<u64>,
    ) -> impl Future<Output = Result<(DigestInfo, fs::FileSlot), Error>>;

    /// Utility function to compute a hash from a generic reader.
    fn compute_from_reader<R: AsyncRead + Unpin + Send>(
        &mut self,
        mut reader: R,
    ) -> impl Future<Output = Result<DigestInfo, Error>> {
        async move {
            let mut chunk = BytesMut::with_capacity(fs::DEFAULT_READ_BUFF_SIZE);
            loop {
                reader
                    .read_buf(&mut chunk)
                    .await
                    .err_tip(|| "Could not read chunk during compute_from_reader")?;
                if chunk.is_empty() {
                    break; // EOF.
                }
                DigestHasher::update(self, &chunk);
                chunk.clear();
            }
            Ok(DigestHasher::finalize_digest(self))
        }
    }
}

#[expect(
    variant_size_differences,
    reason = "some variants are already boxed; this is acceptable"
)]
#[derive(Debug)]
pub enum DigestHasherFuncImpl {
    Sha256(Sha256),
    Blake3(Box<Blake3Hasher>), // Box because Blake3Hasher is 1.3kb in size.
}

/// The individual implementation of the hash function.
#[derive(Debug)]
pub struct DigestHasherImpl {
    hashed_size: u64,
    hash_func_impl: DigestHasherFuncImpl,
}

impl DigestHasherImpl {
    async fn hash_file(
        self,
        file: fs::FileSlot,
    ) -> Result<(DigestInfo, fs::FileSlot), Error> {
        let (mut hasher, file) = crate::spawn_blocking!("hash_file", move || {
            let mut f = file;
            let mut hasher = self;
            let mut buf = vec![0u8; fs::DEFAULT_READ_BUFF_SIZE];
            loop {
                let n = std::io::Read::read(f.as_std_mut(), &mut buf)
                    .err_tip(|| "Read error in hash_file")?;
                if n == 0 {
                    break;
                }
                DigestHasher::update(&mut hasher, &buf[..n]);
            }
            Ok::<_, Error>((hasher, f))
        })
        .await
        .map_err(|e| make_err!(Code::Internal, "hash_file spawn failed: {e:?}"))??;
        let digest = hasher.finalize_digest();
        Ok((digest, file))
    }
}

impl DigestHasher for DigestHasherImpl {
    #[inline]
    fn update(&mut self, input: &[u8]) {
        self.hashed_size += input.len() as u64;
        match &mut self.hash_func_impl {
            DigestHasherFuncImpl::Sha256(h) => sha2::digest::Update::update(h, input),
            DigestHasherFuncImpl::Blake3(h) => {
                Blake3Hasher::update(h, input);
            }
        }
    }

    #[inline]
    fn finalize_digest(&mut self) -> DigestInfo {
        let hash = match &mut self.hash_func_impl {
            DigestHasherFuncImpl::Sha256(h) => h.finalize_reset().into(),
            DigestHasherFuncImpl::Blake3(h) => h.finalize().into(),
        };
        DigestInfo::new(hash, self.hashed_size)
    }

    async fn digest_for_file(
        self,
        file_path: impl AsRef<std::path::Path>,
        mut file: fs::FileSlot,
        size_hint: Option<u64>,
    ) -> Result<(DigestInfo, fs::FileSlot), Error> {
        let file_position = std::io::Seek::stream_position(file.as_std_mut())
            .err_tip(|| "Couldn't get stream position in digest_for_file")?;
        if file_position != 0 {
            return self.hash_file(file).await;
        }
        // If we are a small file, it's faster to just do it the "slow" way.
        // Great read: https://github.com/david-slatinek/c-read-vs.-mmap
        if let Some(size_hint) = size_hint
            && size_hint <= fs::DEFAULT_READ_BUFF_SIZE as u64
        {
            return self.hash_file(file).await;
        }
        let file_path = file_path.as_ref().to_path_buf();
        match self.hash_func_impl {
            DigestHasherFuncImpl::Sha256(_) => self.hash_file(file).await,
            DigestHasherFuncImpl::Blake3(mut hasher) => {
                // Use rayon::spawn + oneshot instead of spawn_blocking so we
                // don't hold a tokio blocking thread while rayon's thread pool
                // does the parallel hashing work.
                //
                // CRITICAL: Capture the current tokio runtime handle and enter
                // it inside the rayon worker thread. Without this, any code
                // path inside the closure (or any Drop running on the rayon
                // worker thread, e.g. on `result` if `tx.send` fails) that
                // touches a tokio API panics with "there is no reactor
                // running". This was a fleet-wide worker crash bug: rayon
                // catches the panic and aborts the whole process. Entering
                // the runtime is cheap (a thread-local set/restore) and only
                // affects calls made from this rayon worker for the duration
                // of `_runtime_guard`'s scope.
                let runtime_handle = tokio::runtime::Handle::current();
                let (tx, rx) = tokio::sync::oneshot::channel();
                rayon::spawn(move || {
                    let _runtime_guard = runtime_handle.enter();
                    let result = match hasher.update_mmap_rayon(file_path) {
                        Ok(_) => Ok((
                            DigestInfo::new(hasher.finalize().into(), hasher.count()),
                            file,
                        )),
                        Err(e) => Err(make_err!(
                            Code::Internal,
                            "Error in blake3's update_mmap_rayon: {e:?}"
                        )),
                    };
                    drop(tx.send(result));
                });
                rx.await.map_err(|_| {
                    make_err!(Code::Internal, "Rayon task dropped in digest_for_file")
                })?
            }
        }
    }
}

/// The digest functions a holder of a blob's BYTES can PROVE a digest against.
///
/// Ordered BLAKE3-first so the fleet-common function is the first candidate
/// checked; the order is a readability nicety only — [`DigestFuncProver`]
/// hashes every candidate in one pass regardless.
///
/// This is deliberately the full [`DigestHasherFunc`] set rather than a
/// configured subset: proving is an identity check against the blob's own
/// declared digest, so a wrong candidate simply fails to match. Narrowing the
/// set can only turn a provable blob into an unprovable one.
///
/// **Widening it is a THROUGHPUT change, so size it.** Each candidate is one
/// additional full inline hash of every byte of every CAS write at
/// `verify_store.rs::inner_check_update` (which cannot be mismatch-gated —
/// see its doc), on the tokio worker thread. At the measured single-thread
/// rates on buildcache (BLAKE3 ~3.1 GB/s, SHA-256 ~1.61 GB/s) a third candidate
/// costs another ~0.3-0.65 ms per MiB of ingest, and the deployed 4 MiB HTTP/2
/// frame size makes it another ~1.3-2.7 ms of non-yielding work per poll.
/// REAPI's `SHA256TREE` is also 32 bytes and currently absent; adding it, or
/// SHA-512, is not a free correctness win.
pub const PROVABLE_DIGEST_FUNCS: [DigestHasherFunc; 2] =
    [DigestHasherFunc::Blake3, DigestHasherFunc::Sha256];

/// Compile-time link between [`DigestHasherFunc`] and
/// [`PROVABLE_DIGEST_FUNCS`]: every variant must name the SLOT it occupies
/// in the array.
///
/// Adding a variant makes this match non-exhaustive (E0004) — unlike a bare
/// `NewVariant => {}` arm, which is what the previous form got wrong: it
/// silenced the error with the array still at 2 and the length assert still
/// true, leaving the new function silently NOT a proving candidate.
///
/// **What each half actually guarantees, stated precisely because two
/// stronger claims made here were refuted by review mutation
/// (`.claude/reviews/3847750f/`, M-EXH-b and M-EXH-c):**
/// - COMPILE TIME, for the mechanical fix. `NewVariant =>
///   PROVABLE_DIGEST_FUNCS[2]` is rejected by `deny(unconditional_panic)`
///   ("this operation will panic at runtime … index out of bounds: the
///   length is 2 but the index is 2"). That is a deny-BY-DEFAULT LINT, not a
///   const-eval error, so it is suppressible with one `#[allow]` — a
///   deliberate act, not a sweep.
/// - RUNTIME, for everything else. `=> PROVABLE_DIGEST_FUNCS[0]` (claiming
///   BLAKE3's slot) and `=> DigestHasherFunc::Blake3` (not indexing the array
///   at all — the earlier claim that indexing was "the only way to satisfy
///   it" was false, and returning the variant directly is the MORE natural
///   mechanical fix) both compile clean. Both are caught by
///   `digest_func_prover_test::every_wire_reachable_digest_function_is_provable`,
///   which sweeps the proto enum through `DigestHasherFunc::try_from` and
///   needs no variant enumeration: a new variant is only reachable by a
///   client once it is added to those conversions, and the sweep reds the
///   moment it is. Review re-performed that kill on the `=> Blake3` form.
///
/// Why any of this matters: a variant that is not a candidate re-opens the
/// FL-1786 latch for exactly that function — blobs keyed with it that arrive
/// under any other label are rejected forever with nothing able to rescue
/// them. `digest_func_proving_advertised_set_test` covers the third surface
/// (advertised ⊆ provable).
const fn provable_slot(func: DigestHasherFunc) -> DigestHasherFunc {
    match func {
        DigestHasherFunc::Blake3 => PROVABLE_DIGEST_FUNCS[0],
        DigestHasherFunc::Sha256 => PROVABLE_DIGEST_FUNCS[1],
    }
}

const _: () = {
    // Drive the round-trip from the array so each declared slot is checked
    // to hold the variant that claims it.
    let mut i = 0;
    while i < PROVABLE_DIGEST_FUNCS.len() {
        let func = PROVABLE_DIGEST_FUNCS[i];
        assert!(
            matches!(
                (provable_slot(func), func),
                (DigestHasherFunc::Blake3, DigestHasherFunc::Blake3)
                    | (DigestHasherFunc::Sha256, DigestHasherFunc::Sha256)
            ),
            "PROVABLE_DIGEST_FUNCS and provable_slot disagree about which slot a \
             DigestHasherFunc variant occupies"
        );
        i += 1;
    }
};

/// Determines which digest function a blob was keyed with, from the blob's
/// bytes plus its declared digest.
///
/// A blob's digest function is not recorded anywhere in this system — the CAS
/// is keyed by hash BYTES (`DigestInfo` is hash + size, no function) and
/// `UploadMissingBlobsRequest` carries only digests. But a digest is a
/// *checkable* claim: the function is exactly the one whose hash of the bytes
/// reproduces the declared digest. A party holding the bytes can therefore
/// determine the function with certainty, without any protocol, provenance
/// record, or configuration.
///
/// Feeds every candidate in [`PROVABLE_DIGEST_FUNCS`] from one pass over the
/// data, so the caller reads the blob once regardless of how many candidates
/// exist. State is O(1) in the blob size — the hashers hold their own fixed
/// working state and the bytes are never retained.
///
/// See `#fl1732-backfill-mislabel-latch`: the worker answering a server
/// `UploadMissingBlobs` had no ambient digest function, stamped the process
/// default (blake3) onto SHA-256-keyed blobs, and the server rejected every
/// upload forever.
#[derive(Debug)]
pub struct DigestFuncProver {
    // CAPPED AT PROVABLE_DIGEST_FUNCS.len(): exactly one hasher per candidate
    // digest function, allocated once at construction. Each hasher holds fixed
    // internal state (Sha256 ~112 B, boxed Blake3 ~1.3 KiB); NO blob bytes are
    // buffered here — `update` folds each chunk in and drops it.
    hashers: Vec<(DigestHasherFunc, DigestHasherImpl)>,
}

impl Default for DigestFuncProver {
    fn default() -> Self {
        Self::new()
    }
}

impl DigestFuncProver {
    #[must_use]
    pub fn new() -> Self {
        Self {
            hashers: PROVABLE_DIGEST_FUNCS
                .iter()
                .map(|func| (*func, func.hasher()))
                .collect(),
        }
    }

    /// Folds the next chunk of the blob into every candidate hasher.
    pub fn update(&mut self, chunk: &[u8]) {
        for (_, hasher) in &mut self.hashers {
            DigestHasher::update(hasher, chunk);
        }
    }

    /// Returns the candidate whose hash of the fed bytes equals `expected`, or
    /// `None` when no candidate reproduces it.
    ///
    /// The comparison is against the WHOLE [`DigestInfo`] — hash and size —
    /// because `finalize_digest` carries the byte count it hashed. A truncated
    /// or over-long local copy therefore proves nothing, which is the correct
    /// answer: that blob is corrupt, not mislabeled.
    ///
    /// `None` is a real outcome and callers MUST handle it without fabricating
    /// a label. Two distinct functions cannot both match a well-formed digest
    /// (that would be a hash collision), so a match is unambiguous.
    #[must_use]
    pub fn prove(self, expected: &DigestInfo) -> Option<DigestHasherFunc> {
        self.finalize_all()
            .into_iter()
            .find(|(_, digest)| digest == expected)
            .map(|(func, _)| func)
    }

    /// Finalizes every candidate and returns its `(function, digest)` pair, in
    /// [`PROVABLE_DIGEST_FUNCS`] order.
    ///
    /// [`prove`](Self::prove) is the common case and callers should prefer it.
    /// This exists for the one caller that must ALSO report a specific
    /// candidate's computed hash when nothing matches: `VerifyStore`'s
    /// rejection message (`verify_store.rs`, "Hashes do not match, got: {…}
    /// but digest hash was {…}") names the hash under the function the write
    /// was LABELLED with, and that message is pinned by operators, dashboards
    /// and `zero_copy_write_corruption_test`. Recomputing it would mean a
    /// second pass over the blob on the rejection path.
    #[must_use]
    pub fn finalize_all(mut self) -> Vec<(DigestHasherFunc, DigestInfo)> {
        self.hashers
            .iter_mut()
            // `finalize_digest` takes `&mut self` and resets; `self` is
            // consumed by this call so no hasher is observed after
            // finalization.
            .map(|(func, hasher)| (*func, DigestHasher::finalize_digest(hasher)))
            .collect()
    }
}

/// One-shot [`DigestFuncProver`] for a blob already buffered in memory.
///
/// See [`DigestFuncProver`] for the semantics of `None`.
#[must_use]
pub fn prove_digest_func(expected: &DigestInfo, bytes: &[u8]) -> Option<DigestHasherFunc> {
    let mut prover = DigestFuncProver::new();
    prover.update(bytes);
    prover.prove(expected)
}
