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

use core::borrow::{Borrow, BorrowMut};
use core::convert::Into;
use core::fmt::{self, Debug, Display};
use core::future;
use core::hash::{Hash, Hasher};
use core::ops::{Bound, RangeBounds};
use core::pin::Pin;
use core::ptr::addr_eq;
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher as StdHasher;
use std::ffi::OsString;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Future, FutureExt, Stream, StreamExt, join, try_join};
use futures::stream::FuturesUnordered;
use nativelink_error::{Code, Error, ResultExt, error_if, make_err};
use smallvec::SmallVec;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

tokio::task_local! {
    /// Set to `true` when the current CAS request originates from a worker
    /// (not a client like Bazel). `WorkerProxyStore` checks this to decide
    /// between proxying blob data (for clients) and returning a redirect
    /// with peer endpoints (for workers).
    pub static IS_WORKER_REQUEST: bool;

    /// Set to `true` when the current write originates from a server-side
    /// mirror operation. The worker's `FastSlowStore` checks this to hold
    /// the blob in memory only (skip disk and server upload), avoiding
    /// disk I/O for data that is already persisted on the server.
    pub static IS_MIRROR_REQUEST: bool;
}

/// Prefix for redirect errors returned by `WorkerProxyStore` to worker callers.
/// The remainder of the message is a comma-separated list of peer gRPC endpoints
/// that have the requested blob. Example: `"NL_REDIRECT:grpc://w1:50081,grpc://w2:50081"`
pub const REDIRECT_PREFIX: &str = "NL_REDIRECT:";
use nativelink_metric::MetricsComponent;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
    make_buf_channel_pair_with_size,
};
use crate::common::DigestInfo;
use crate::digest_hasher::{DigestHasher, DigestHasherFunc, default_digest_hasher_func};
use crate::fs;
use crate::health_utils::{HealthRegistryBuilder, HealthStatus, HealthStatusIndicator};

static DEFAULT_DIGEST_SIZE_HEALTH_CHECK: OnceLock<usize> = OnceLock::new();
/// Default digest size for health check data. Any change in this value
/// changes the default contract. `GlobalConfig` should be updated to reflect
/// changes in this value.
pub const DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG: usize = 1024 * 1024;

// Get the default digest size for health check data, if value is unset a system wide default is used.
pub fn default_digest_size_health_check() -> usize {
    *DEFAULT_DIGEST_SIZE_HEALTH_CHECK.get_or_init(|| DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG)
}

/// Set the default digest size for health check data, this should be called once.
pub fn set_default_digest_size_health_check(size: usize) -> Result<(), Error> {
    DEFAULT_DIGEST_SIZE_HEALTH_CHECK.set(size).map_err(|_| {
        make_err!(
            Code::Internal,
            "set_default_digest_size_health_check already set"
        )
    })
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub enum UploadSizeInfo {
    /// When the data transfer amount is known to be exact size, this enum should be used.
    /// The receiver store can use this to better optimize the way the data is sent or stored.
    ExactSize(u64),

    /// When the data transfer amount is not known to be exact, the caller should use this enum
    /// to provide the maximum size that could possibly be sent. This will bypass the exact size
    /// checks, but still provide useful information to the underlying store about the data being
    /// sent that it can then use to optimize the upload process.
    MaxSize(u64),
}

/// Utility to send all the data to the store from a file.
// Note: This is not inlined because some code may want to bypass any underlying
// optimizations that may be present in the inner store.
pub async fn slow_update_store_with_file<S: StoreDriver + ?Sized>(
    store: Pin<&S>,
    digest: impl Into<StoreKey<'_>>,
    mut file: fs::FileSlot,
    upload_size: UploadSizeInfo,
) -> Result<fs::FileSlot, Error> {
    use std::io::Seek;
    file.as_std_mut()
        .seek(std::io::SeekFrom::Start(0))
        .err_tip(|| "Failed to rewind in upload_file_to_store")?;
    let (mut tx, rx) = make_buf_channel_pair();

    let update_fut = store
        .update(digest.into(), rx, upload_size)
        .map(|r| r.err_tip(|| "Could not upload data to store in upload_file_to_store"));
    let read_data_fut = async move {
        let file = fs::read_file_to_channel(file, &mut tx, u64::MAX, fs::DEFAULT_READ_BUFF_SIZE, 0)
            .await
            .err_tip(|| "Failed to read in upload_file_to_store")?;
        tx.send_eof()
            .err_tip(|| "Could not send EOF to store in upload_file_to_store")?;
        Ok::<_, Error>(file)
    };
    let (update_res, read_res) = tokio::join!(update_fut, read_data_fut);
    update_res?;
    let file = read_res?;
    Ok(file)
}

/// RAII wrapper that aborts a tokio `JoinHandle` on drop.
///
/// Used by [`MergedNotifyState`] to ensure forwarder tasks spawned by the
/// `Many` arm of [`StableDigestDelegation`] terminate when the wrapper
/// store is dropped. Without this, `tokio::spawn`'d forwarder loops would
/// hold strong `Arc<Notify>` references forever (one per child × stores
/// × test runs), preventing the merged Notify and child Notifies from
/// being deallocated.
#[derive(Debug)]
pub struct AbortOnDrop(JoinHandle<()>);

impl AbortOnDrop {
    pub fn new(handle: JoinHandle<()>) -> Self {
        Self(handle)
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// State owned by a `Many`-position wrapper for its merged Notify and the
/// per-child forwarder tasks that wake it.
///
/// The wrapper store holds `OnceLock<MergedNotifyState>`; the trait default
/// for `stable_notify` lazily initializes both the Notify and the forwarder
/// JoinHandles on first call. When the wrapper is dropped, the OnceLock
/// drops the state, which drops each `AbortOnDrop`, which aborts the
/// spawned forwarder task — releasing every Arc the forwarder held.
///
/// **Closes F2** (perf-optimizer): forwarder JoinHandles were previously
/// discarded, leaking ~2-8 tasks per wrapper drop in tests and a fixed
/// per-wrapper cost in production.
#[derive(Debug)]
pub struct MergedNotifyState {
    pub notify: Arc<Notify>,
    /// The field is only read on Drop (each `AbortOnDrop` calls
    /// `JoinHandle::abort` from its `Drop` impl). Rust's dead-code lint
    /// considers Drop-only fields "unused" because they are never
    /// explicitly read; allow the warning here so the field's purpose
    /// (preventing forwarder task leak via wrapper drop) stays visible.
    ///
    /// `pub(crate)` rather than `pub`: external callers have no business
    /// touching the abort handles directly — the Drop chain is the only
    /// load-bearing read. Construction goes through [`Self::new`] so a
    /// future external author cannot field-init this struct without the
    /// aborters and silently re-introduce F2.
    #[allow(dead_code)]
    pub(crate) aborters: Vec<AbortOnDrop>,
}

impl MergedNotifyState {
    /// Build the state captured by the lazy `Many`-arm initialization in
    /// [`StoreDriver::stable_notify`]. `aborters` MUST contain one
    /// `AbortOnDrop` per spawned forwarder task — the wrapper's Drop is
    /// the only mechanism that releases the `Arc<Notify>` clones held by
    /// those tasks (closes F2).
    pub(crate) fn new(notify: Arc<Notify>, aborters: Vec<AbortOnDrop>) -> Self {
        Self { notify, aborters }
    }
}

/// Inline-capacity for `Many`-arm child slices. Production wrappers are
/// 2 children deep (SizePartitioning, Dedup, FastSlowStore). ShardStore
/// can be larger but is typically 2-3 shards. Stack-allocate up to 4
/// children — beyond that SmallVec spills to heap, identical to Vec
/// behavior. Closes perf-optimizer Finding 2 (MAJOR — per-call Vec
/// allocation on the worker hot path that runs ~500-1000×/sec).
pub type DelegationChildren<'a> = SmallVec<[&'a (dyn StoreDriver + 'static); 4]>;

/// Delegation strategy for stable-digest aggregation methods on a store.
///
/// The `StoreDriver` trait once shipped silent no-op defaults for
/// `drain_stable_digests` / `stable_notify`, so wrapper stores added before
/// the BlobsInStableStorage feature (e.g. `SizePartitioningStore`) silently
/// returned empty drains and "never woken" notifies in production. The
/// resulting BIS broadcast was wedged for 31 days
/// (`.claude/reviews/why-bis-bug-not-caught/audit.md`).
///
/// To prevent that class of bug from recurring, every `StoreDriver` impl
/// MUST implement [`StoreDriver::stable_delegation`] (no default body) and
/// declare its position in the chain. The default bodies of
/// `drain_stable_digests` and `stable_notify` then dispatch via this enum,
/// which makes the wrapper-store author choose at compile time.
///
/// Reference: `src/bin/nativelink.rs:367-380` shows the production-side
/// merged-Notify pattern used here for the `Many` variant.
pub enum StableDigestDelegation<'a> {
    /// Leaf store — no inner store contributes to the stable-digest stream
    /// from this position. The default `drain_stable_digests` returns empty
    /// and the default `stable_notify` returns a never-woken Notify. Stores
    /// that produce digests directly (e.g. [`FastSlowStore`]) MUST also
    /// override the methods to return their own state.
    Leaf,
    /// Single-inner wrapper — forwards unchanged to one inner store.
    Inner(&'a (dyn StoreDriver + 'static)),
    /// Multi-inner wrapper — forwards to each inner store. For
    /// `drain_stable_digests`, results are concatenated. For
    /// `stable_notify`, the wrapper MUST own a `OnceLock<MergedNotifyState>`
    /// field and pass it via `merged_state`; the trait default lazily
    /// builds a merged Notify woken when ANY inner Notify fires AND
    /// captures the forwarder JoinHandles in the state so they get
    /// aborted when the wrapper drops (closes F2 task leak).
    Many {
        children: DelegationChildren<'a>,
        merged_state: &'a OnceLock<MergedNotifyState>,
    },
    /// Pure passthrough — same dispatch as [`Self::Inner`] but documents
    /// intent for stores that resolve to an inner store dynamically
    /// (e.g. `RefStore`).
    Passthrough(&'a (dyn StoreDriver + 'static)),
}

/// Delegation strategy for `pin_digests` / `pin_digests_with_results`.
///
/// Wrapper stores MUST declare which inner store(s) own the pin. The
/// default impls of [`StoreDriver::pin_digests`] and
/// [`StoreDriver::pin_digests_with_results`] dispatch via this enum so an
/// author cannot ship a wrapper that silently drops pin requests (which
/// would re-introduce eviction-during-fetch races).
///
/// Unlike [`StableDigestDelegation`], pinning is fire-and-forget so no
/// merged-Notify wiring is needed for the multi-inner case.
pub enum PinDelegation<'a> {
    /// Leaf store — pins resolve here. Stores that actually pin (e.g.
    /// [`FilesystemStore`]) MUST override `pin_digests_with_results` to
    /// report real per-digest results. The default body for `Leaf` is a
    /// no-op that reports `false` for every digest — "this store does
    /// not pin so it cannot claim true."
    ///
    /// **Why default-false (CRIT-1 / F3 from c-plus-d/testing-czar.md and
    /// c-plus-d/red-team.md):** the prior default returned
    /// `vec![true; n]` to "preserve prior semantics," but the
    /// [`Self::Many`] OR-merge then dominated the real per-digest
    /// `false` from a pinning sibling (e.g. FastSlowStore's
    /// MemoryStore-true masking FilesystemStore-false), silently
    /// hiding eviction. Default-false makes non-pinning leaves
    /// transparent in the OR-merge — only an actually-pinning child
    /// can flip a slot to `true`.
    Leaf,
    /// Single-inner wrapper — forwards unchanged.
    Inner(&'a (dyn StoreDriver + 'static)),
    /// Multi-inner wrapper — fans out the pin to every inner store. For
    /// `pin_digests_with_results`, the per-digest result is the OR across
    /// inner results (any-store-pinned counts as success).
    Many(DelegationChildren<'a>),
    /// Pure passthrough — same dispatch as [`Self::Inner`] but documents
    /// intent for resolved-by-name wrappers (e.g. `RefStore`).
    Passthrough(&'a (dyn StoreDriver + 'static)),
}

/// Delegation strategy for [`StoreDriver::mark_stable`] (BIS pipeline).
///
/// Used by the worker API server's BlobsAvailable handler to push
/// already-stably-stored digests into the BIS broadcast feeder so the
/// worker is told to release its pin. See
/// `.claude/reviews/bis-coverage-for-already-cached-outputs/audit.md`
/// for the pin-leak class this hook closes.
///
/// **No default body on [`StoreDriver::mark_stable_delegation`]** — every
/// store MUST declare its arm explicitly. The previous code shipped a
/// silent `_no-op` default on `mark_stable` itself; new wrappers
/// (Compression, Dedup, Shard, OntapS3ExistenceCache) inherited that
/// default and silently swallowed BIS-feeder pushes, exactly the silent-
/// default-trap class C+D was created to abolish (see red-team finding 3
/// in `.claude/reviews/a1-mark-stable/red-team.md`).
///
/// **Many-arm semantics (different from [`StableDigestDelegation::Many`]):**
/// for `mark_stable`, the `Many` arm fans out the entire digest slice to
/// every inner store. This is correct for wrappers whose inner stores all
/// participate in the BIS chain symmetrically (e.g., wrapper that owns
/// two equivalent backends). Wrappers that need per-digest routing
/// (SizePartitioningStore by size, ShardStore by hash) or selective
/// routing (DedupStore to index_store only) MUST declare [`Self::Leaf`]
/// AND override [`StoreDriver::mark_stable`] manually — same pattern
/// FastSlowStore uses to be the producer leaf for `drain_stable_digests`.
pub enum MarkStableDelegation<'a> {
    /// Leaf store — `mark_stable` is a no-op for stores that do not
    /// participate in the BIS feeder chain (Memory, Noop, Redis, S3,
    /// GCS, Mongo, Azure, Grpc, OntapS3, CompletenessChecking on the
    /// AC-only path, Filesystem). Stores that PRODUCE stable-digest
    /// notifications (e.g. [`FastSlowStore`]) ALSO declare `Leaf` here
    /// and override [`StoreDriver::mark_stable`] manually to push into
    /// their own queue. Per-digest-routing wrappers (SizePartitioning,
    /// Shard, Dedup) likewise declare `Leaf` and override.
    Leaf,
    /// Single-inner wrapper — forwards the entire slice to one inner
    /// store. Used by Compression, Verify, ExistenceCache,
    /// OntapS3ExistenceCache, WorkerProxy, CompletenessChecking
    /// (delegates to `ac_store`).
    Inner(&'a (dyn StoreDriver + 'static)),
    /// Multi-inner wrapper — fans the entire slice out to every inner
    /// store. Suitable when all inner stores participate in the BIS
    /// chain symmetrically. Wrappers that need per-digest routing or
    /// selective forwarding declare `Leaf` and override `mark_stable`.
    Many(DelegationChildren<'a>),
    /// Pure passthrough — same dispatch as [`Self::Inner`] but documents
    /// intent for resolved-by-name wrappers (e.g. `RefStore`).
    Passthrough(&'a (dyn StoreDriver + 'static)),
}

/// Optimizations that stores may want to expose to the callers.
/// This is useful for specific cases when the store can optimize the processing
/// of the data being processed.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum StoreOptimizations {
    /// The store can optimize the upload process when it knows the data is coming from a file.
    FileUpdates,

    /// If the store will ignore the data uploads.
    NoopUpdates,

    /// If the store will never serve downloads.
    NoopDownloads,

    /// If the store will determine whether a key has associated data once a read has been
    /// attempted instead of calling `.has()` first.
    LazyExistenceOnSync,

    /// The store provides an optimized `update_oneshot` implementation that bypasses
    /// channel overhead for direct Bytes writes. Stores with this optimization can
    /// accept complete data directly without going through the MPSC channel.
    SubscribesToUpdateOneshot,
}

/// A wrapper struct for [`StoreKey`] to work around
/// lifetime limitations in `HashMap::get()` as described in
/// <https://github.com/rust-lang/rust/issues/80389>
///
/// As such this is a wrapper type that is stored in the
/// maps using the workaround as described in
/// <https://blinsay.com/blog/compound-keys/>
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct StoreKeyBorrow(StoreKey<'static>);

impl From<StoreKey<'static>> for StoreKeyBorrow {
    fn from(key: StoreKey<'static>) -> Self {
        Self(key)
    }
}

impl From<StoreKeyBorrow> for StoreKey<'static> {
    fn from(key_borrow: StoreKeyBorrow) -> Self {
        key_borrow.0
    }
}

impl<'a> Borrow<StoreKey<'a>> for StoreKeyBorrow {
    fn borrow(&self) -> &StoreKey<'a> {
        &self.0
    }
}

impl<'a> Borrow<StoreKey<'a>> for &StoreKeyBorrow {
    fn borrow(&self) -> &StoreKey<'a> {
        &self.0
    }
}

/// Holds something that can be converted into a key the
/// store API can understand. Generally this is a digest
/// but it can also be a string if the caller wishes to
/// store the data directly and reference it by a string
/// directly.
#[derive(Debug, Eq)]
pub enum StoreKey<'a> {
    /// A string key.
    Str(Cow<'a, str>),

    /// A key that is a digest.
    Digest(DigestInfo),
}

impl<'a> StoreKey<'a> {
    /// Creates a new store key from a string.
    pub const fn new_str(s: &'a str) -> Self {
        StoreKey::Str(Cow::Borrowed(s))
    }

    /// Returns a shallow clone of the key.
    /// This is extremely cheap and should be used when clone
    /// is needed but the key is not going to be modified.
    #[must_use]
    #[allow(
        clippy::missing_const_for_fn,
        reason = "False positive on stable, but not on nightly"
    )]
    pub fn borrow(&'a self) -> Self {
        match self {
            StoreKey::Str(Cow::Owned(s)) => StoreKey::Str(Cow::Borrowed(s)),
            StoreKey::Str(Cow::Borrowed(s)) => StoreKey::Str(Cow::Borrowed(s)),
            StoreKey::Digest(d) => StoreKey::Digest(*d),
        }
    }

    /// Converts the key into an owned version. This is useful
    /// when the caller needs an owned version of the key.
    pub fn into_owned(self) -> StoreKey<'static> {
        match self {
            StoreKey::Str(Cow::Owned(s)) => StoreKey::Str(Cow::Owned(s)),
            StoreKey::Str(Cow::Borrowed(s)) => StoreKey::Str(Cow::Owned(s.to_owned())),
            StoreKey::Digest(d) => StoreKey::Digest(d),
        }
    }

    /// Converts the key into a digest. This is useful when the caller
    /// must have a digest key. If the data is not a digest, it may
    /// hash the underlying key and return a digest of the hash of the key
    pub fn into_digest(self) -> DigestInfo {
        match self {
            StoreKey::Digest(digest) => digest,
            StoreKey::Str(s) => {
                let mut hasher = DigestHasherFunc::Blake3.hasher();
                hasher.update(s.as_bytes());
                hasher.finalize_digest()
            }
        }
    }

    /// Returns the key as a string. If the key is a digest, it will
    /// return a string representation of the digest. If the key is a string,
    /// it will return the string itself.
    pub fn as_str(&'a self) -> Cow<'a, str> {
        match self {
            StoreKey::Str(Cow::Owned(s)) => Cow::Borrowed(s),
            StoreKey::Str(Cow::Borrowed(s)) => Cow::Borrowed(s),
            StoreKey::Digest(d) => Cow::Owned(format!("{d}")),
        }
    }
}

impl Clone for StoreKey<'static> {
    fn clone(&self) -> Self {
        match self {
            StoreKey::Str(s) => StoreKey::Str(s.clone()),
            StoreKey::Digest(d) => StoreKey::Digest(*d),
        }
    }
}

impl PartialOrd for StoreKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StoreKey<'_> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        match (self, other) {
            (StoreKey::Str(a), StoreKey::Str(b)) => a.cmp(b),
            (StoreKey::Digest(a), StoreKey::Digest(b)) => a.cmp(b),
            (StoreKey::Str(_), StoreKey::Digest(_)) => core::cmp::Ordering::Less,
            (StoreKey::Digest(_), StoreKey::Str(_)) => core::cmp::Ordering::Greater,
        }
    }
}

impl PartialEq for StoreKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StoreKey::Str(a), StoreKey::Str(b)) => a == b,
            (StoreKey::Digest(a), StoreKey::Digest(b)) => a == b,
            _ => false,
        }
    }
}

impl Hash for StoreKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        /// Salts the hash with the enum value that represents
        /// the type of the key.
        #[repr(u8)]
        enum HashId {
            Str = 0,
            Digest = 1,
        }
        match self {
            StoreKey::Str(s) => {
                (HashId::Str as u8).hash(state);
                s.hash(state);
            }
            StoreKey::Digest(d) => {
                (HashId::Digest as u8).hash(state);
                d.hash(state);
            }
        }
    }
}

impl<'a> From<&'a str> for StoreKey<'a> {
    fn from(s: &'a str) -> Self {
        StoreKey::Str(Cow::Borrowed(s))
    }
}

impl From<String> for StoreKey<'static> {
    fn from(s: String) -> Self {
        StoreKey::Str(Cow::Owned(s))
    }
}

impl From<DigestInfo> for StoreKey<'_> {
    fn from(d: DigestInfo) -> Self {
        StoreKey::Digest(d)
    }
}

impl From<&DigestInfo> for StoreKey<'_> {
    fn from(d: &DigestInfo) -> Self {
        StoreKey::Digest(*d)
    }
}

// mostly for use with tracing::Value
impl Display for StoreKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreKey::Str(s) => {
                write!(f, "{s}")
            }
            StoreKey::Digest(d) => {
                write!(f, "Digest: {d}")
            }
        }
    }
}

#[derive(Clone, MetricsComponent)]
#[repr(transparent)]
pub struct Store {
    #[metric]
    inner: Arc<dyn StoreDriver>,
}

impl Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    pub fn new(inner: Arc<dyn StoreDriver>) -> Self {
        Self { inner }
    }

    /// Returns the immediate inner store driver.
    /// Note: This does not recursively try to resolve underlying store drivers
    /// like `.inner_store()` does.
    #[inline]
    pub fn into_inner(self) -> Arc<dyn StoreDriver> {
        self.inner
    }

    /// Gets the underlying store for the given digest.
    /// A caller might want to use this to obtain a reference to the "real" underlying store
    /// (if applicable) and check if it implements some special traits that allow optimizations.
    /// Note: If the store performs complex operations on the data, it should return itself.
    #[inline]
    pub fn inner_store<'a, K: Into<StoreKey<'a>>>(&self, digest: Option<K>) -> &dyn StoreDriver {
        self.inner.inner_store(digest.map(Into::into))
    }

    /// Tries to cast the underlying store to the given type.
    #[inline]
    pub fn downcast_ref<U: StoreDriver>(&self, maybe_digest: Option<StoreKey<'_>>) -> Option<&U> {
        self.inner.inner_store(maybe_digest).as_any().downcast_ref()
    }

    /// Register health checks used to monitor the store.
    #[inline]
    pub fn register_health(&self, registry: &mut HealthRegistryBuilder) {
        self.inner.clone().register_health(registry);
    }

    #[inline]
    pub fn register_item_callback(
        &self,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        self.inner.clone().register_item_callback(callback)
    }

    /// Observe a `pinned_mirror_entries` (BlobsAvailableNotification field 16)
    /// ack broadcast. Delegates to the inner
    /// [`StoreDriver::observe_pinned_mirror_ack`]. See trait doc for
    /// semantics.
    #[inline]
    pub fn observe_pinned_mirror_ack(
        &self,
        entries: &[nativelink_proto::com::github::trace_machina::nativelink::remote_execution::MirrorPinEntry],
    ) {
        self.inner.observe_pinned_mirror_ack(entries);
    }

    /// Drain digests that have completed their write to stable storage.
    /// Delegates to the inner [`StoreDriver::drain_stable_digests`].
    #[inline]
    pub fn drain_stable_digests(&self) -> Vec<DigestInfo> {
        self.inner.drain_stable_digests()
    }

    /// Returns the notify handle that wakes the BlobsInStableStorage loop
    /// when new digests become available.
    /// Delegates to the inner [`StoreDriver::stable_notify`].
    #[inline]
    pub fn stable_notify(&self) -> Arc<Notify> {
        self.inner.stable_notify()
    }

    /// Externally mark digests as having reached stable storage. Used by
    /// the worker API server's BlobsAvailable handler: when a worker
    /// reports a digest it holds and the server has the same digest in
    /// stable storage, the server marks it stable so the BIS broadcast
    /// loop tells the worker it is safe to unpin.
    /// Delegates to the inner [`StoreDriver::mark_stable`].
    #[inline]
    pub fn mark_stable(&self, digests: &[DigestInfo]) {
        self.inner.mark_stable(digests);
    }

    /// Pin digests to prevent eviction while a worker is fetching them.
    /// Delegates to the inner [`StoreDriver::pin_digests`].
    #[inline]
    pub fn pin_digests(&self, digests: &[DigestInfo]) {
        self.inner.pin_digests(digests);
    }

    /// Pin digests and report per-digest success.
    /// Delegates to the inner [`StoreDriver::pin_digests_with_results`].
    #[inline]
    pub fn pin_digests_with_results(&self, digests: &[DigestInfo]) -> Vec<bool> {
        self.inner.pin_digests_with_results(digests)
    }

    /// Drain digests whose background slow-store write failed.
    /// Delegates to the inner [`StoreDriver::drain_failed_digests`].
    #[inline]
    pub fn drain_failed_digests(&self) -> Vec<DigestInfo> {
        self.inner.drain_failed_digests()
    }

    /// Re-insert digests into the failed-slow-writes set. Used by the
    /// server-side drain loop (`#287`: server-side `failed_slow_writes`
    /// drain → UploadMissingBlobs) when a digest is drained but cannot
    /// be dispatched (no worker in `BlobLocalityMap`, dispatch channel
    /// closed, etc.). The reinsert is the post-drain inverse so the
    /// digest stays observable until a worker can be picked.
    /// Delegates to the inner [`StoreDriver::reinsert_failed_digests`].
    #[inline]
    pub fn reinsert_failed_digests(&self, digests: &[DigestInfo]) {
        self.inner.reinsert_failed_digests(digests);
    }
}

impl StoreLike for Store {
    #[inline]
    fn as_store_driver(&self) -> &'_ dyn StoreDriver {
        self.inner.as_ref()
    }

    fn as_pin(&self) -> Pin<&Self> {
        Pin::new(self)
    }
}

impl<T> StoreLike for T
where
    T: StoreDriver + Sized,
{
    #[inline]
    fn as_store_driver(&self) -> &'_ dyn StoreDriver {
        self
    }

    fn as_pin(&self) -> Pin<&Self> {
        Pin::new(self)
    }
}

pub trait StoreLike: Send + Sync + Sized + Unpin + 'static {
    /// Returns the immediate inner store driver.
    fn as_store_driver(&self) -> &'_ dyn StoreDriver;

    /// Utility function to return a pinned reference to self.
    fn as_pin(&self) -> Pin<&Self>;

    /// Utility function to return a pinned reference to the store driver.
    #[inline]
    fn as_store_driver_pin(&self) -> Pin<&'_ dyn StoreDriver> {
        Pin::new(self.as_store_driver())
    }

    /// Look up a digest in the store and return None if it does not exist in
    /// the store, or Some(size) if it does.
    /// Note: On an AC store the size will be incorrect and should not be used!
    #[inline]
    fn has<'a>(
        &'a self,
        digest: impl Into<StoreKey<'a>>,
    ) -> impl Future<Output = Result<Option<u64>, Error>> + 'a {
        self.as_store_driver_pin().has(digest.into())
    }

    /// Look up a list of digests in the store and return a result for each in
    /// the same order as input.  The result will either be None if it does not
    /// exist in the store, or Some(size) if it does.
    /// Note: On an AC store the size will be incorrect and should not be used!
    #[inline]
    fn has_many<'a>(
        &'a self,
        digests: &'a [StoreKey<'a>],
    ) -> impl Future<Output = Result<Vec<Option<u64>>, Error>> + Send + 'a {
        if digests.is_empty() {
            return future::ready(Ok(vec![])).boxed();
        }
        self.as_store_driver_pin().has_many(digests)
    }

    /// The implementation of the above has and `has_many` functions.  See their
    /// documentation for details.
    #[inline]
    fn has_with_results<'a>(
        &'a self,
        digests: &'a [StoreKey<'a>],
        results: &'a mut [Option<u64>],
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        if digests.is_empty() {
            return future::ready(Ok(())).boxed();
        }
        self.as_store_driver_pin()
            .has_with_results(digests, results)
    }

    /// List all the keys in the store that are within the given range.
    /// `handler` is called for each key in the range. If `handler` returns
    /// false, the listing is stopped.
    ///
    /// The number of keys passed through the handler is the return value.
    #[inline]
    fn list<'a, 'b>(
        &'a self,
        range: impl RangeBounds<StoreKey<'b>> + Send + 'b,
        mut handler: impl for<'c> FnMut(&'c StoreKey) -> bool + Send + Sync + 'a,
    ) -> impl Future<Output = Result<u64, Error>> + Send + 'a
    where
        'b: 'a,
    {
        // Note: We use a manual async move, so the future can own the `range` and `handler`,
        // otherwise we'd require the caller to pass them in by reference making more borrow
        // checker noise.
        async move {
            self.as_store_driver_pin()
                .list(
                    (
                        range.start_bound().map(StoreKey::borrow),
                        range.end_bound().map(StoreKey::borrow),
                    ),
                    &mut handler,
                )
                .await
        }
    }

    /// Sends the data to the store.
    #[inline]
    fn update<'a>(
        &'a self,
        digest: impl Into<StoreKey<'a>>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.as_store_driver_pin()
            .update(digest.into(), reader, upload_size)
    }

    /// Any optimizations the store might want to expose to the callers.
    /// By default, no optimizations are exposed.
    #[inline]
    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        self.as_store_driver_pin().optimized_for(optimization)
    }

    /// Specialized version of `.update()` which takes a `FileSlot`.
    /// This is useful if the underlying store can optimize the upload process
    /// when it knows the data is coming from a file.
    #[inline]
    fn update_with_whole_file<'a>(
        &'a self,
        digest: impl Into<StoreKey<'a>>,
        path: OsString,
        file: fs::FileSlot,
        upload_size: UploadSizeInfo,
    ) -> impl Future<Output = Result<Option<fs::FileSlot>, Error>> + Send + 'a {
        self.as_store_driver_pin()
            .update_with_whole_file(digest.into(), path, file, upload_size)
    }

    /// Utility to send all the data to the store when you have all the bytes.
    #[inline]
    fn update_oneshot<'a>(
        &'a self,
        digest: impl Into<StoreKey<'a>>,
        data: Bytes,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.as_store_driver_pin()
            .update_oneshot(digest.into(), data)
    }

    /// Retrieves part of the data from the store and writes it to the given writer.
    #[inline]
    fn get_part<'a>(
        &'a self,
        digest: impl Into<StoreKey<'a>>,
        mut writer: impl BorrowMut<DropCloserWriteHalf> + Send + 'a,
        offset: u64,
        length: Option<u64>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        let key = digest.into();
        // Note: We need to capture `writer` just in case the caller
        // expects the drop() method to be called on it when the future
        // is done due to the complex interaction between the DropCloserWriteHalf
        // and the DropCloserReadHalf during drop().
        async move {
            self.as_store_driver_pin()
                .get_part(key, writer.borrow_mut(), offset, length)
                .await
        }
    }

    /// Utility that works the same as `.get_part()`, but writes all the data.
    #[inline]
    fn get<'a>(
        &'a self,
        key: impl Into<StoreKey<'a>>,
        writer: DropCloserWriteHalf,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        self.as_store_driver_pin().get(key.into(), writer)
    }

    /// Utility that will return all the bytes at once instead of in a streaming manner.
    #[inline]
    fn get_part_unchunked<'a>(
        &'a self,
        key: impl Into<StoreKey<'a>>,
        offset: u64,
        length: Option<u64>,
    ) -> impl Future<Output = Result<Bytes, Error>> + Send + 'a {
        self.as_store_driver_pin()
            .get_part_unchunked(key.into(), offset, length)
    }

    /// Reads multiple small blobs in a single batch. Delegates to
    /// [`StoreDriver::batch_get_part_unchunked`] which may pipeline the
    /// underlying I/O (e.g. a single Redis pipeline for N keys).
    #[inline]
    fn batch_get_part_unchunked<'a>(
        &'a self,
        keys: Vec<StoreKey<'a>>,
        length: Option<u64>,
    ) -> impl Future<Output = Vec<Result<Bytes, Error>>> + Send + 'a {
        self.as_store_driver_pin()
            .batch_get_part_unchunked(keys, length)
    }

    /// Default implementation of the health check. Some stores may want to override this
    /// in situations where the default implementation is not sufficient.
    #[inline]
    fn check_health(
        &self,
        namespace: Cow<'static, str>,
    ) -> impl Future<Output = HealthStatus> + Send {
        self.as_store_driver_pin().check_health(namespace)
    }
}

#[async_trait]
pub trait StoreDriver:
    Sync + Send + Unpin + MetricsComponent + HealthStatusIndicator + 'static
{
    /// See: [`StoreLike::has`] for details.
    #[inline]
    async fn has(self: Pin<&Self>, key: StoreKey<'_>) -> Result<Option<u64>, Error> {
        let mut result = [None];
        self.has_with_results(&[key], &mut result).await?;
        Ok(result[0])
    }

    /// See: [`StoreLike::has_many`] for details.
    #[inline]
    async fn has_many(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
    ) -> Result<Vec<Option<u64>>, Error> {
        let mut results = vec![None; digests.len()];
        self.has_with_results(digests, &mut results).await?;
        Ok(results)
    }

    /// See: [`StoreLike::has_with_results`] for details.
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error>;

    /// See: [`StoreLike::list`] for details.
    async fn list(
        self: Pin<&Self>,
        _range: (Bound<StoreKey<'_>>, Bound<StoreKey<'_>>),
        _handler: &mut (dyn for<'a> FnMut(&'a StoreKey) -> bool + Send + Sync + '_),
    ) -> Result<u64, Error> {
        // TODO(palfrey) We should force all stores to implement this function instead of
        // providing a default implementation.
        Err(make_err!(
            Code::Unimplemented,
            "Store::list() not implemented for this store"
        ))
    }

    /// See: [`StoreLike::update`] for details.
    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error>;

    /// See: [`StoreLike::optimized_for`] for details.
    fn optimized_for(&self, _optimization: StoreOptimizations) -> bool {
        false
    }

    /// See: [`StoreLike::update_with_whole_file`] for details.
    async fn update_with_whole_file(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        path: OsString,
        file: fs::FileSlot,
        upload_size: UploadSizeInfo,
    ) -> Result<Option<fs::FileSlot>, Error> {
        let inner_store = self.inner_store(Some(key.borrow()));
        if inner_store.optimized_for(StoreOptimizations::FileUpdates) {
            error_if!(
                addr_eq(inner_store, &raw const *self),
                "Store::inner_store() returned self when optimization present"
            );
            return Pin::new(inner_store)
                .update_with_whole_file(key, path, file, upload_size)
                .await;
        }
        let file = slow_update_store_with_file(self, key, file, upload_size).await?;
        Ok(Some(file))
    }

    /// See: [`StoreLike::update_oneshot`] for details.
    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        // TODO(palfrey) This is extremely inefficient, since we have exactly
        // what we need here. Maybe we could instead make a version of the stream
        // that can take objects already fully in memory instead?
        let (mut tx, rx) = make_buf_channel_pair_with_size(4);

        let data_len =
            u64::try_from(data.len()).err_tip(|| "Could not convert data.len() to u64")?;
        let send_fut = async move {
            // Only send if we are not EOF.
            if !data.is_empty() {
                tx.send(data)
                    .await
                    .err_tip(|| "Failed to write data in update_oneshot")?;
            }
            tx.send_eof()
                .err_tip(|| "Failed to write EOF in update_oneshot")?;
            Ok(())
        };
        try_join!(
            send_fut,
            self.update(key, rx, UploadSizeInfo::ExactSize(data_len))
        )?;
        Ok(())
    }

    /// See: [`StoreLike::get_part`] for details.
    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error>;

    /// See: [`StoreLike::get`] for details.
    #[inline]
    async fn get(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut writer: DropCloserWriteHalf,
    ) -> Result<(), Error> {
        self.get_part(key, &mut writer, 0, None).await
    }

    /// See: [`StoreLike::get_part_unchunked`] for details.
    async fn get_part_unchunked(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        offset: u64,
        length: Option<u64>,
    ) -> Result<Bytes, Error> {
        let length_usize = length
            .map(|v| usize::try_from(v).err_tip(|| "Could not convert length to usize"))
            .transpose()?;

        // TODO(palfrey) This is extremely inefficient, since we have exactly
        // what we need here. Maybe we could instead make a version of the stream
        // that can take objects already fully in memory instead?
        let (mut tx, mut rx) = make_buf_channel_pair_with_size(4);

        let (data_res, get_part_res) = join!(
            rx.consume(length_usize),
            // We use a closure here to ensure that the `tx` is dropped when the
            // future is done.
            async move { self.get_part(key, &mut tx, offset, length).await },
        );
        get_part_res
            .err_tip(|| "Failed to get_part in get_part_unchunked")
            .merge(data_res.err_tip(|| "Failed to read stream to completion in get_part_unchunked"))
    }

    /// Reads multiple small blobs in a single batch operation. Returns one
    /// `Result<Bytes, Error>` per key, in the same order as the input. The
    /// default implementation fans out via `FuturesUnordered`; stores that
    /// support pipelining (e.g. `RedisStore`) override this with a single
    /// round-trip.
    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        let futs: FuturesUnordered<_> = keys
            .into_iter()
            .enumerate()
            .map(|(idx, key)| async move {
                let result = self.get_part_unchunked(key, 0, length).await;
                (idx, result)
            })
            .collect();
        let mut results: Vec<Result<Bytes, Error>> =
            (0..futs.len()).map(|_| Err(make_err!(Code::Internal, "batch slot not filled")))
                .collect();
        let mut stream = futs;
        while let Some((idx, result)) = stream.next().await {
            results[idx] = result;
        }
        results
    }

    /// See: [`StoreLike::check_health`] for details.
    async fn check_health(self: Pin<&Self>, namespace: Cow<'static, str>) -> HealthStatus {
        let digest_data_size = default_digest_size_health_check();
        let mut digest_data = vec![0u8; digest_data_size];

        let mut namespace_hasher = StdHasher::new();
        namespace.hash(&mut namespace_hasher);
        self.get_name().hash(&mut namespace_hasher);
        let hash_seed = namespace_hasher.finish();

        // Fill the digest data with random data based on a stable
        // hash of the namespace and store name. Intention is to
        // have randomly filled data that is unique per store and
        // does not change between health checks. This is to ensure
        // we are not adding more data to store on each health check.
        let mut rng: StdRng = StdRng::seed_from_u64(hash_seed);
        rng.fill_bytes(&mut digest_data);

        let mut digest_hasher = default_digest_hasher_func().hasher();
        digest_hasher.update(&digest_data);
        let digest_data_len = digest_data.len() as u64;
        let digest_info = StoreKey::from(digest_hasher.finalize_digest());

        let digest_bytes = Bytes::from(digest_data);

        if let Err(e) = self
            .update_oneshot(digest_info.borrow(), digest_bytes.clone())
            .await
        {
            warn!(?e, "check_health Store.update_oneshot() failed");
            return HealthStatus::new_failed(
                self.get_ref(),
                format!("Store.update_oneshot() failed: {e}").into(),
            );
        }

        match self.has(digest_info.borrow()).await {
            Ok(Some(s)) => {
                if s != digest_data_len {
                    return HealthStatus::new_failed(
                        self.get_ref(),
                        format!("Store.has() size mismatch {s} != {digest_data_len}").into(),
                    );
                }
            }
            Ok(None) => {
                return HealthStatus::new_failed(
                    self.get_ref(),
                    "Store.has() size not found".into(),
                );
            }
            Err(e) => {
                return HealthStatus::new_failed(
                    self.get_ref(),
                    format!("Store.has() failed: {e}").into(),
                );
            }
        }

        match self
            .get_part_unchunked(digest_info, 0, Some(digest_data_len))
            .await
        {
            Ok(b) => {
                if b != digest_bytes {
                    return HealthStatus::new_failed(
                        self.get_ref(),
                        "Store.get_part_unchunked() data mismatch".into(),
                    );
                }
            }
            Err(e) => {
                return HealthStatus::new_failed(
                    self.get_ref(),
                    format!("Store.get_part_unchunked() failed: {e}").into(),
                );
            }
        }

        HealthStatus::new_ok(self.get_ref(), "Successfully store health check".into())
    }

    /// See: [`Store::inner_store`] for details.
    fn inner_store(&self, _digest: Option<StoreKey<'_>>) -> &dyn StoreDriver;

    /// Returns an Any variation of whatever Self is.
    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static);
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static>;

    // Register health checks used to monitor the store.
    fn register_health(self: Arc<Self>, _registry: &mut HealthRegistryBuilder) {}

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error>;

    /// Observe a `pinned_mirror_entries` ack broadcast (per Bug A small-CAS
    /// peer-mirror dispatcher; task #168 integration). The
    /// `WorkerApiServer::handle_blobs_available` calls this for every
    /// registered FastSlowStore on each `BlobsAvailable` tick that carries
    /// a non-empty `BlobsAvailableNotification.pinned_mirror_entries`
    /// (proto field 16). Each store inspects the slice for its own
    /// `store_id` (via binary search since `entries` is sorted by
    /// `store_id` ASCII), and removes confirmed-held entries from its
    /// `EphemeralServerSidePin` set.
    ///
    /// Default body is a no-op so the vast majority of stores (Memory,
    /// Filesystem, Compression, Dedup, Shard, etc.) never opt in. Only
    /// FastSlowStore overrides today; the dispatcher's `register_pin_set`
    /// glue side-steps the trait when calling directly into the per-store
    /// pin set, but the trait method exists so any wrapper layer can route
    /// the broadcast down its inner stores without special-casing.
    ///
    /// Per plan C10: matches the `register_item_callback` /
    /// `drain_stable_digests` "default no-op" precedent. The plan
    /// considered making this `No default body` per the C+D rule but the
    /// trait callers (broadcast loop) iterate ALL registered stores —
    /// silent no-op IS the contract for non-participating stores, not a
    /// trap. Wrapper stores that delegate to inner FastSlowStores should
    /// override and forward to inner via their existing delegation enum.
    fn observe_pinned_mirror_ack(
        &self,
        _entries: &[nativelink_proto::com::github::trace_machina::nativelink::remote_execution::MirrorPinEntry],
    ) {
        // Default no-op (per C10 + matches register_item_callback shape).
    }

    /// Declare how this store routes [`Self::drain_stable_digests`] /
    /// [`Self::stable_notify`] / [`Self::drain_failed_digests`] requests.
    ///
    /// **No default body** — every store MUST implement this so the author
    /// is forced at compile time to think about the BIS path. See
    /// [`StableDigestDelegation`] for variants and rationale.
    fn stable_delegation(&self) -> StableDigestDelegation<'_>;

    /// Declare how this store routes [`Self::pin_digests`] /
    /// [`Self::pin_digests_with_results`] requests.
    ///
    /// **No default body** — every store MUST implement this so the author
    /// is forced at compile time to think about pin propagation. See
    /// [`PinDelegation`] for variants and rationale.
    fn pin_delegation(&self) -> PinDelegation<'_>;

    /// Declare how this store routes [`Self::mark_stable`] requests
    /// (BIS-feeder push from the worker API server's BlobsAvailable
    /// handler).
    ///
    /// **No default body** — every store MUST implement this so the author
    /// is forced at compile time to think about BIS coverage. The previous
    /// silent `_no-op` default on `mark_stable` itself caused four wrapper
    /// stores (Compression, Dedup, Shard, OntapS3ExistenceCache) to
    /// silently swallow BIS-feeder pushes; this is the silent-default-trap
    /// class C+D was created to abolish (see
    /// `.claude/reviews/a1-mark-stable/red-team.md` finding 3 and task
    /// #157). See [`MarkStableDelegation`] for variants and rationale.
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_>;

    /// Drain digests that have completed their write to stable storage
    /// (e.g., FilesystemStore in a FastSlowStore).
    ///
    /// The default body dispatches via [`Self::stable_delegation`].
    /// Stores that produce digests directly (e.g. [`FastSlowStore`])
    /// declare `Leaf` and override this method to return their own state.
    fn drain_stable_digests(&self) -> Vec<DigestInfo> {
        match self.stable_delegation() {
            StableDigestDelegation::Leaf => Vec::new(),
            StableDigestDelegation::Inner(s) | StableDigestDelegation::Passthrough(s) => {
                s.drain_stable_digests()
            }
            StableDigestDelegation::Many { children, .. } => children
                .iter()
                .flat_map(|s| s.drain_stable_digests())
                .collect(),
        }
    }

    /// Returns a [`Notify`] that is woken when new stable digests are
    /// available.
    ///
    /// The default body dispatches via [`Self::stable_delegation`]. For
    /// `Many`, the wrapper-supplied `OnceLock<MergedNotifyState>` is
    /// lazily populated with a merged Notify woken when any inner store's
    /// Notify fires. The per-child forwarder tasks' JoinHandles are
    /// retained as `AbortOnDrop` inside `MergedNotifyState`, so when the
    /// wrapper store is dropped the OnceLock drops the state, the
    /// `AbortOnDrop`s drop, and each forwarder task is aborted —
    /// preventing the per-wrapper-drop task leak (F2).
    ///
    /// **Leaf safety note:** the static `NOOP_NOTIFY` is shared across
    /// every Leaf store in every store tree. This is safe because no
    /// code path ever calls `notify_one`/`notify_waiters` on a `Notify`
    /// returned from a Leaf delegation — the contract is "never woken,"
    /// callers only `.notified().await`. Sharing one static avoids
    /// per-Leaf allocation; if a future change introduced a way to wake
    /// it, the cascade across trees would be a real concern.
    ///
    /// **Runtime precondition (Many arm):** the `Many` branch calls
    /// `tokio::spawn` inside `get_or_init` to launch one forwarder task
    /// per child Notify. `tokio::spawn` panics outside a tokio runtime,
    /// so the FIRST call to `stable_notify()` for any `Many`-wrapped
    /// store MUST originate from inside a tokio runtime context. In
    /// production this is trivially satisfied (every BIS-feeder lives
    /// inside the tokio reactor). In tests, ensure the test attribute
    /// (`#[nativelink_test]` or `#[tokio::test]`) covers the call site,
    /// or wrap the call in `Runtime::new()?.block_on(...)`. Subsequent
    /// calls hit the `OnceLock`-cached state and do NOT spawn — only
    /// the first invocation has the runtime requirement.
    fn stable_notify(&self) -> Arc<Notify> {
        match self.stable_delegation() {
            StableDigestDelegation::Leaf => {
                static NOOP_NOTIFY: OnceLock<Arc<Notify>> = OnceLock::new();
                NOOP_NOTIFY
                    .get_or_init(|| Arc::new(Notify::new()))
                    .clone()
            }
            StableDigestDelegation::Inner(s) | StableDigestDelegation::Passthrough(s) => {
                s.stable_notify()
            }
            StableDigestDelegation::Many {
                children,
                merged_state,
            } => {
                merged_state
                    .get_or_init(|| {
                        let merged = Arc::new(Notify::new());
                        let mut aborters = Vec::with_capacity(children.len());
                        for child in children {
                            let child_notify = child.stable_notify();
                            let merged_clone = merged.clone();
                            let handle = tokio::spawn(async move {
                                loop {
                                    child_notify.notified().await;
                                    merged_clone.notify_one();
                                }
                            });
                            aborters.push(AbortOnDrop::new(handle));
                        }
                        MergedNotifyState::new(merged, aborters)
                    })
                    .notify
                    .clone()
            }
        }
    }

    /// Externally mark digests as having reached stable storage so the
    /// next BIS broadcast covers them. Used by the worker API server's
    /// BlobsAvailable handler: when a worker reports a digest it holds
    /// and the server has the same digest stably, the server marks it
    /// stable so the BIS broadcast loop tells the worker it is safe to
    /// unpin (the worker's pin is no longer load-bearing).
    ///
    /// Why a separate hook (vs. the existing FastSlowStore success arm
    /// in `update_oneshot`): the success arm only fires when the server
    /// actually ran `update_oneshot` for the digest. The audit at
    /// `.claude/reviews/bis-coverage-for-already-cached-outputs/audit.md`
    /// quantifies all the paths where the server has a digest stably
    /// without an `update_oneshot` having just run (deduplicated
    /// uploads where the BatchUpdateBlobs / ByteStream::write
    /// short-circuit, dedup hit on tree-children, mirror_blobs already
    /// stably stored, etc.). Without this hook the worker's pin
    /// (durable under pin v2, no TTL) leaks forever.
    ///
    /// The caller is responsible for verifying the server actually has
    /// each digest (via `has_with_results`) before calling this; the
    /// store cannot itself enforce that invariant cheaply on the hot
    /// path. Calling `mark_stable` for a digest the server does NOT
    /// have would tell the worker to unpin a digest whose only durable
    /// copy is the worker's `mirror_blobs`, causing data loss.
    ///
    /// The default body dispatches via [`Self::mark_stable_delegation`].
    /// Stores that produce BIS-feeder digests directly (e.g.
    /// [`FastSlowStore`]) declare `Leaf` and override this method to push
    /// into their own queue. Wrappers that need per-digest routing
    /// (SizePartitioning, Shard) or selective forwarding (Dedup) ALSO
    /// declare `Leaf` and override; the trait default no-ops in that
    /// arm because the override owns the dispatch.
    ///
    /// Closes task #157 (folds `mark_stable` into the C+D forced-
    /// delegation enum mechanism). The previous silent no-op default
    /// shipped the silent-default-trap class C+D was created to abolish.
    fn mark_stable(&self, digests: &[DigestInfo]) {
        match self.mark_stable_delegation() {
            MarkStableDelegation::Leaf => {
                // Non-producer leaves silently no-op; producer leaves and
                // custom-router wrappers (FastSlow, SizePartitioning,
                // Shard, Dedup) override this method to do their own
                // routing.
            }
            MarkStableDelegation::Inner(s) | MarkStableDelegation::Passthrough(s) => {
                s.mark_stable(digests);
            }
            MarkStableDelegation::Many(children) => {
                for child in children {
                    child.mark_stable(digests);
                }
            }
        }
    }

    /// Pin digests to prevent eviction while a worker is fetching them.
    ///
    /// The default body dispatches via [`Self::pin_delegation`]. Stores
    /// that support pinning (e.g. [`FilesystemStore`]) declare `Leaf` and
    /// override this to call `MokaEvictingMap::pin_keys()`.
    fn pin_digests(&self, digests: &[DigestInfo]) {
        match self.pin_delegation() {
            PinDelegation::Leaf => {
                // Leaves that don't pin (Memory, Noop) silently no-op.
            }
            PinDelegation::Inner(s) | PinDelegation::Passthrough(s) => {
                s.pin_digests(digests);
            }
            PinDelegation::Many(children) => {
                for child in children {
                    child.pin_digests(digests);
                }
            }
        }
    }

    /// Like `pin_digests` but reports per-digest success. The returned
    /// vec has one entry per input digest, in order: `true` if the digest
    /// was present in the store and is now pinned, `false` if it was
    /// absent (e.g. already evicted) and so could not be pinned.
    ///
    /// The default body dispatches via [`Self::pin_delegation`]. For
    /// `Many`, the per-digest result is the OR across inner results
    /// (any-store-pinned counts as success). Stores that support pinning
    /// (e.g. [`FilesystemStore`]) declare `Leaf` and override this to
    /// report per-key results from `MokaEvictingMap::pin_key()`.
    fn pin_digests_with_results(&self, digests: &[DigestInfo]) -> Vec<bool> {
        match self.pin_delegation() {
            PinDelegation::Leaf => {
                // Default-false for non-pinning leaves (CRIT-1 / F3). Stores
                // that actually pin override this method to report real
                // per-digest results. The OR-merge in `Many` below treats
                // non-pinning siblings as transparent — only a real
                // pinning child can flip a slot to `true`.
                self.pin_digests(digests);
                vec![false; digests.len()]
            }
            PinDelegation::Inner(s) | PinDelegation::Passthrough(s) => {
                s.pin_digests_with_results(digests)
            }
            PinDelegation::Many(children) => {
                let mut combined = vec![false; digests.len()];
                for child in children {
                    let per_child = child.pin_digests_with_results(digests);
                    debug_assert_eq!(per_child.len(), digests.len());
                    for (slot, result) in combined.iter_mut().zip(per_child) {
                        *slot |= result;
                    }
                }
                combined
            }
        }
    }

    /// Drain digests whose background slow-store write failed.
    /// Used by the worker to retry uploads on reconnect.
    ///
    /// The default body dispatches via [`Self::stable_delegation`] (the
    /// failed-digest stream rides the same chain). Stores that own a
    /// failed-write set (e.g. [`FastSlowStore`]) declare `Leaf` and
    /// override this method.
    fn drain_failed_digests(&self) -> Vec<DigestInfo> {
        match self.stable_delegation() {
            StableDigestDelegation::Leaf => Vec::new(),
            StableDigestDelegation::Inner(s) | StableDigestDelegation::Passthrough(s) => {
                s.drain_failed_digests()
            }
            StableDigestDelegation::Many { children, .. } => children
                .iter()
                .flat_map(|s| s.drain_failed_digests())
                .collect(),
        }
    }

    /// Re-insert digests into the failed-slow-writes set. Inverse of
    /// [`Self::drain_failed_digests`] for use by the server-side
    /// drain-then-dispatch loop (#287: server-side `failed_slow_writes`
    /// drain → UploadMissingBlobs). The non-`Leaf` arms delegate
    /// through the wrapper chain via [`Self::stable_delegation`]; the
    /// `Leaf` arm is loud about the contract gap.
    ///
    /// **`Leaf` stores that own a `failed_slow_writes` set MUST
    /// override.** Per the Charter dead-letter rule (mirror of
    /// `19a11ee9` for `broadcast_blobs_in_stable_storage_chunked`):
    /// silently dropping digests at the `Leaf` boundary is exactly
    /// the regression #287 was filed to fix. The default `Leaf` arm
    /// therefore `debug_assert!`s that input is empty — surfacing the
    /// contract gap loudly in tests / debug builds rather than
    /// letting the digest vanish into a dead-letter set. Production
    /// release builds preserve the silent-drop fallback (per
    /// CLAUDE.md "never panic in library code") so an unforeseen
    /// edge case can't take the server down.
    ///
    /// `Leaf` stores that do NOT own a `failed_slow_writes` set are
    /// safe under the default: their `drain_failed_digests` returns
    /// `Vec::new()`, so the wrapper traversal of
    /// `reinsert_failed_digests` never reaches them with non-empty
    /// input. The `debug_assert!` short-circuit lets benign callers
    /// (e.g. callers that drain → re-insert when 0 digests came out)
    /// traverse the chain without noise.
    ///
    /// Today only [`FastSlowStore`](crate::fast_slow_store) declares
    /// `Leaf` AND owns the set, and it overrides this method. Future
    /// `Leaf` stores that grow such a set MUST add their own
    /// override; the `debug_assert!` will fire the first time the
    /// drain-then-dispatch path delivers digests to them in any
    /// non-release test run, surfacing the gap before it ships.
    fn reinsert_failed_digests(&self, digests: &[DigestInfo]) {
        match self.stable_delegation() {
            StableDigestDelegation::Leaf => {
                debug_assert!(
                    digests.is_empty(),
                    "Leaf store inheriting default reinsert_failed_digests was \
                     called with {} digests; this Leaf must override the method \
                     (#287 dead-letter contract — see store_trait.rs doc)",
                    digests.len()
                );
            }
            StableDigestDelegation::Inner(s) | StableDigestDelegation::Passthrough(s) => {
                s.reinsert_failed_digests(digests);
            }
            StableDigestDelegation::Many { children, .. } => {
                for s in children {
                    s.reinsert_failed_digests(digests);
                }
            }
        }
    }
}

// Callback invoked when a store inserts or deletes an item.
pub trait ItemCallback: Debug + Send + Sync {
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Called synchronously when a new item is inserted.
    fn on_insert(&self, _store_key: StoreKey<'_>, _size: u64) {}

    /// Fired when a key is read (cache hit) via the public `get` /
    /// `get_many` paths. Intentionally NOT fired from `sizes_for_keys`
    /// (existence checks) or from internal cache.get calls used to
    /// capture replaced values inside `insert_inner`. Use this to track
    /// recent read activity per digest (worker-side LRU heat signal that
    /// flows back to the server's locality_map via BlobsAvailable).
    fn on_get(&self, _store_key: StoreKey<'_>) {}

    /// Fired when a pin auto-expires after `PIN_TIMEOUT_SECS` without
    /// being explicitly unpinned. Distinct from `callback` (eviction):
    /// the blob is still in the map, just demoted to LRU-evictable.
    /// `FastSlowStore` listens on this hook to record digests in
    /// `failed_slow_writes` so a slow-write that hangs past the pin
    /// deadline is still retried on reconnect — closing the durability
    /// gap that an auto-unpin would otherwise silently open.
    fn on_pin_expired(&self, _store_key: StoreKey<'_>, _size: u64) {}
}

/// The instructions on how to decode a value from a Bytes & version into
/// the underlying type.
pub trait SchedulerStoreDecodeTo {
    type DecodeOutput;
    fn decode(version: i64, data: Bytes) -> Result<Self::DecodeOutput, Error>;
}

pub trait SchedulerSubscription: Send + Sync {
    fn changed(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
}

pub trait SchedulerSubscriptionManager: Send + Sync {
    type Subscription: SchedulerSubscription;

    fn subscribe<K>(&self, key: K) -> Result<Self::Subscription, Error>
    where
        K: SchedulerStoreKeyProvider;

    fn is_reliable() -> bool;
}

/// The API surface for a scheduler store.
pub trait SchedulerStore: Send + Sync + 'static {
    type SubscriptionManager: SchedulerSubscriptionManager;

    /// Returns the subscription manager for the scheduler store.
    fn subscription_manager(
        &self,
    ) -> impl Future<Output = Result<Arc<Self::SubscriptionManager>, Error>> + Send;

    /// Updates or inserts an entry into the underlying store.
    /// Metadata about the key is attached to the compile-time type.
    /// If `StoreKeyProvider::Versioned` is `TrueValue`, the data will not
    /// be updated if the current version in the database does not match
    /// the version in the passed in data.
    /// No guarantees are made about when `Version` is `FalseValue`.
    /// Indexes are guaranteed to be updated atomically with the data.
    fn update_data<T>(&self, data: T) -> impl Future<Output = Result<Option<i64>, Error>> + Send
    where
        T: SchedulerStoreDataProvider
            + SchedulerStoreKeyProvider
            + SchedulerCurrentVersionProvider
            + Send;

    /// Searches for all keys in the store that match the given index prefix.
    fn search_by_index_prefix<K>(
        &self,
        index: K,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<<K as SchedulerStoreDecodeTo>::DecodeOutput, Error>> + Send,
            Error,
        >,
    > + Send
    where
        K: SchedulerIndexProvider + SchedulerStoreDecodeTo + Send;

    /// Returns data for the provided key with the given version if
    /// `StoreKeyProvider::Versioned` is `TrueValue`.
    fn get_and_decode<K>(
        &self,
        key: K,
    ) -> impl Future<Output = Result<Option<<K as SchedulerStoreDecodeTo>::DecodeOutput>, Error>> + Send
    where
        K: SchedulerStoreKeyProvider + SchedulerStoreDecodeTo + Send;
}

/// A type that is used to let the scheduler store know what
/// index is being requested.
pub trait SchedulerIndexProvider {
    /// Only keys inserted with this prefix will be indexed.
    const KEY_PREFIX: &'static str;

    /// The name of the index.
    const INDEX_NAME: &'static str;

    /// The sort key for the index (if any).
    const MAYBE_SORT_KEY: Option<&'static str> = None;

    /// If the data is versioned.
    type Versioned: BoolValue;

    /// The value of the index.
    fn index_value(&self) -> Cow<'_, str>;
}

/// Provides a key to lookup data in the store.
pub trait SchedulerStoreKeyProvider {
    /// If the data is versioned.
    type Versioned: BoolValue;

    /// Returns the key for the data.
    fn get_key(&self) -> StoreKey<'static>;
}

/// Provides data to be stored in the scheduler store.
pub trait SchedulerStoreDataProvider {
    /// Converts the data into bytes to be stored in the store.
    fn try_into_bytes(self) -> Result<Bytes, Error>;

    /// Returns the indexes for the data if any.
    fn get_indexes(&self) -> Result<Vec<(&'static str, Bytes)>, Error> {
        Ok(Vec::new())
    }
}

/// Provides the current version of the data in the store.
pub trait SchedulerCurrentVersionProvider {
    /// Returns the current version of the data in the store.
    fn current_version(&self) -> i64;
}

/// Default implementation for when we are not providing a version
/// for the data.
impl<T> SchedulerCurrentVersionProvider for T
where
    T: SchedulerStoreKeyProvider<Versioned = FalseValue>,
{
    fn current_version(&self) -> i64 {
        0
    }
}

/// Compile time types for booleans.
pub trait BoolValue {
    const VALUE: bool;
}
/// Compile time check if something is false.
pub trait IsFalse {}
/// Compile time check if something is true.
pub trait IsTrue {}

/// Compile time true value.
#[derive(Debug, Clone, Copy)]
pub struct TrueValue;
impl BoolValue for TrueValue {
    const VALUE: bool = true;
}
impl IsTrue for TrueValue {}

/// Compile time false value.
#[derive(Debug, Clone, Copy)]
pub struct FalseValue;
impl BoolValue for FalseValue {
    const VALUE: bool = false;
}
impl IsFalse for FalseValue {}
