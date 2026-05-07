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

use core::any::Any;
use core::borrow::Borrow;
use core::fmt::Debug;
use core::ops::Bound;
use core::pin::Pin;
#[cfg(feature = "chunked_fast_slow")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error, ResultExt, make_err};
use tracing::{debug, error};
use nativelink_metric::MetricsComponent;
#[cfg(feature = "chunked_fast_slow")]
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::backpressure_signal;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use nativelink_util::health_utils::{
    HealthRegistryBuilder, HealthStatusIndicator, default_health_status_indicator,
};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, StoreDriver,
    StoreKey, StoreKeyBorrow, StoreOptimizations, UploadSizeInfo,
};

use crate::callback_utils::ItemCallbackHolder;
use crate::cas_utils::is_zero_digest;
#[cfg(feature = "chunked_fast_slow")]
use crate::chunked_signal::encode_backpressure_signal_any;

/// #212 Phase 2.6: backoff hint clients should observe when
/// `MemoryStore::update*` rejects with `MemoryStoreAtCapacity`. Memory
/// eviction drains in milliseconds (faster than slow-tier ack), so the
/// suggested retry window is short. Operators may override; the
/// classifier behavior does not depend on this value.
#[cfg(feature = "chunked_fast_slow")]
const MEMORY_STORE_BACKPRESSURE_RETRY_MS: u64 = 25;

/// Scatter-gather buffer: stores data as a chain of `Bytes` chunks
/// (like BSD mbufs / Linux sk_buffs) to avoid concatenation copies.
/// Single-chunk and empty cases are common and handled without Vec overhead.
#[derive(Clone)]
pub struct BytesWrapper {
    /// Total byte length across all chunks.
    total_len: u64,
    /// The chunk chain. Single-element for oneshot writes, multi for streamed.
    chunks: Vec<Bytes>,
}

impl BytesWrapper {
    fn from_single(data: Bytes) -> Self {
        let total_len = data.len() as u64;
        if data.is_empty() {
            Self { total_len: 0, chunks: Vec::new() }
        } else {
            Self { total_len, chunks: vec![data] }
        }
    }

    fn from_chunks(chunks: Vec<Bytes>) -> Self {
        let total_len = chunks.iter().map(|c| c.len() as u64).sum();
        Self { total_len, chunks }
    }

    /// Returns a contiguous `Bytes` from the scatter-gather chunks,
    /// capped to at most `length` bytes. Zero-copy when there is a
    /// single chunk that fits within the cap.
    fn to_contiguous(&self, length: Option<u64>) -> Bytes {
        let cap = length
            .map(|v| v.min(self.total_len) as usize)
            .unwrap_or(self.total_len as usize);

        if cap == 0 || self.chunks.is_empty() {
            return Bytes::new();
        }

        // Single chunk that fits entirely — zero-copy (just Arc bump).
        if self.chunks.len() == 1 {
            let chunk = &self.chunks[0];
            if chunk.len() <= cap {
                return chunk.clone();
            }
            return chunk.slice(..cap);
        }

        // Multiple chunks: concatenate up to `cap` bytes.
        let mut buf = BytesMut::with_capacity(cap);
        let mut remaining = cap;
        for chunk in &self.chunks {
            if remaining == 0 {
                break;
            }
            let take = chunk.len().min(remaining);
            buf.extend_from_slice(&chunk[..take]);
            remaining -= take;
        }
        buf.freeze()
    }
}

impl Debug for BytesWrapper {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "BytesWrapper {{ len: {}, chunks: {} }}", self.total_len, self.chunks.len())
    }
}

impl LenEntry for BytesWrapper {
    #[inline]
    fn len(&self) -> u64 {
        self.total_len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.total_len == 0
    }
}

#[derive(Debug, MetricsComponent)]
pub struct MemoryStore {
    #[metric(group = "evicting_map")]
    evicting_map: Arc<MokaEvictingMap<
        StoreKeyBorrow,
        StoreKey<'static>,
        BytesWrapper,
        SystemTime,
        ItemCallbackHolder,
    >>,
    /// #212 Phase 2.6 kill-switch: when true, `update` / `update_oneshot`
    /// emit `Code::ResourceExhausted` carrying a
    /// `BackpressureSignal::MemoryStoreAtCapacity` detail INSTEAD of
    /// silently evicting a recent (potentially still-in-use) blob to
    /// make room. Default OFF preserves the historic silent-evict
    /// behavior so this architectural change is no-op until the
    /// operator explicitly opts in. Toggle via
    /// [`Self::enable_emit_backpressure`] /
    /// [`Self::disable_emit_backpressure`]; inspect via
    /// [`Self::emit_backpressure_enabled`]. The production wiring at
    /// `MemoryStore::new` honors `MemorySpec.emit_backpressure_enabled`.
    #[cfg(feature = "chunked_fast_slow")]
    emit_backpressure_enabled: AtomicBool,
}

impl MemoryStore {
    pub fn new(spec: &MemorySpec) -> Arc<Self> {
        let empty_policy = nativelink_config::stores::EvictionPolicy::default();
        let eviction_policy = spec.eviction_policy.as_ref().unwrap_or(&empty_policy);
        let evicting_map = Arc::new(MokaEvictingMap::with_anchor(eviction_policy, SystemTime::now()));
        evicting_map.start_background_eviction();
        let store = Arc::new(Self {
            evicting_map,
            #[cfg(feature = "chunked_fast_slow")]
            emit_backpressure_enabled: AtomicBool::new(false),
        });
        // #212 Phase 2.6: honor the production config knob. Tests +
        // admin tools toggle at runtime via
        // [`Self::enable_emit_backpressure`] /
        // [`Self::disable_emit_backpressure`].
        #[cfg(feature = "chunked_fast_slow")]
        if spec.emit_backpressure_enabled {
            store.enable_emit_backpressure();
        }
        store
    }

    /// Returns the number of key-value pairs that are currently in the the cache.
    /// Function is not for production code paths.
    pub async fn len_for_test(&self) -> usize {
        self.evicting_map.len_for_test().await
    }

    pub async fn remove_entry(&self, key: StoreKey<'_>) -> bool {
        self.evicting_map.remove(&key.into_owned()).await
    }

    /// #212 Phase 2.6 runtime kill-switch ARM for backpressure emission
    /// on over-capacity writes. Default is OFF (silent-evict, the
    /// historic behavior); calling this flips this `MemoryStore`
    /// instance to refuse over-capacity writes with
    /// `Code::ResourceExhausted + BackpressureSignal::MemoryStoreAtCapacity`.
    ///
    /// Mirrors the verb-pair pattern used by `WorkerProxyStore`
    /// (`enable_X` / `disable_X` / `X_enabled()`) and `FastSlowStore`
    /// (`enable_chunked_reads` / `disable_chunked_reads` /
    /// `chunked_reads_enabled()`); see `#220` D2 for the unification.
    ///
    /// Idempotent. Safe to call multiple times. Production opt-in is
    /// the JSON `MemorySpec.emit_backpressure_enabled = true` (auto-
    /// armed in `MemoryStore::new`). Tests and admin tooling call this
    /// directly. Takes effect on the next `update` / `update_oneshot`;
    /// in-flight operations are not affected.
    ///
    /// The relaxed orderings are deliberate: the gate is a single
    /// boolean read on a hot path. A torn read in either direction is
    /// safe — the worst case is one extra silent-evict (kill-switch
    /// transitioning ON→OFF) or one spurious backpressure response
    /// (transitioning OFF→ON) at the moment of the toggle. Both are
    /// transient and self-healing within the next call.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn enable_emit_backpressure(&self) {
        self.emit_backpressure_enabled.store(true, Ordering::Relaxed);
    }

    /// #212 Phase 2.6 runtime kill-switch RE-ARM for backpressure
    /// emission. Operator rollback path back to silent-evict; restores
    /// pre-Phase-2.6 behavior bit-identically. Idempotent.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn disable_emit_backpressure(&self) {
        self.emit_backpressure_enabled.store(false, Ordering::Relaxed);
    }

    /// #212 Phase 2.6 inspector: returns whether backpressure emission
    /// is currently armed on this `MemoryStore`. Used by tests + admin
    /// probes; matches the `*_enabled()` reader half of the verb-pair
    /// pattern.
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn emit_backpressure_enabled(&self) -> bool {
        self.emit_backpressure_enabled.load(Ordering::Relaxed)
    }

    /// #212 Phase 2.6: best-effort capacity check shared by `update`
    /// and `update_oneshot`. Returns `Ok(())` when the insert is
    /// permitted (the kill-switch is off, OR the cache has headroom);
    /// returns `Err(ResourceExhausted+BackpressureSignal)` when the
    /// kill-switch is ON and the predicted post-insert size would
    /// exceed `max_bytes`.
    ///
    /// The predicate is a snapshot of moka's `weighted_size` and is
    /// eventually-consistent — under heavy concurrent writers the
    /// answer can be stale by one batch's worth of work. That is
    /// acceptable for a backpressure signal: the kill-switch is opt-in
    /// and exists to STOP an over-capacity hot loop, not to enforce
    /// hard accounting.
    #[cfg(feature = "chunked_fast_slow")]
    fn check_backpressure_gate(
        &self,
        owned_key: &StoreKey<'static>,
        incoming_bytes: u64,
    ) -> Result<(), Error> {
        if !self.emit_backpressure_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.evicting_map.would_exceed_capacity(incoming_bytes) {
            return Ok(());
        }
        debug!(
            key = ?owned_key,
            incoming_bytes,
            "MemoryStore: emitting BackpressureSignal::MemoryStoreAtCapacity \
             (kill-switch ON, insert would exceed cap)",
        );
        let detail = encode_backpressure_signal_any(
            backpressure_signal::Reason::MemoryStoreAtCapacity,
            MEMORY_STORE_BACKPRESSURE_RETRY_MS,
        );
        Err(Error::resource_exhausted_backpressure(
            format!(
                "MemoryStore at capacity for key {owned_key:?}: \
                 incoming {incoming_bytes} bytes would force eviction. \
                 Retry after ~{MEMORY_STORE_BACKPRESSURE_RETRY_MS}ms."
            ),
            detail,
        ))
    }

    /// Compile-time no-op when `chunked_fast_slow` is OFF. Keeps the
    /// caller side a single line regardless of feature gate.
    #[cfg(not(feature = "chunked_fast_slow"))]
    #[inline]
    fn check_backpressure_gate(
        &self,
        _owned_key: &StoreKey<'static>,
        _incoming_bytes: u64,
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
impl StoreDriver for MemoryStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.evicting_map
            .sizes_for_keys(
                keys.iter().map(|sk| sk.borrow().into_owned()),
                results,
                false, /* peek */
            )
            .await;
        // We need to do a special pass to ensure our zero digest exist.
        keys.iter()
            .zip(results.iter_mut())
            .for_each(|(key, result)| {
                if is_zero_digest(key.borrow()) {
                    *result = Some(0);
                }
            });
        Ok(())
    }

    async fn list(
        self: Pin<&Self>,
        range: (Bound<StoreKey<'_>>, Bound<StoreKey<'_>>),
        handler: &mut (dyn for<'a> FnMut(&'a StoreKey) -> bool + Send + Sync + '_),
    ) -> Result<u64, Error> {
        let range = (
            range.0.map(StoreKey::into_owned),
            range.1.map(StoreKey::into_owned),
        );
        let iterations = self
            .evicting_map
            .range(range, move |key, _value| handler(key.borrow()))
            .await;
        Ok(iterations)
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let update_start = std::time::Instant::now();
        debug!(key = ?key, "MemoryStore::update: start");

        let owned_key = key.into_owned();

        // #284 part 1: early-reject when the declared upload size alone
        // would exceed capacity. Before this gate, the recv loop would
        // pull the entire stream off the gRPC wire (allocating Bytes
        // chunks the whole way) only to throw it away at the post-drain
        // gate below. At ~5 over-capacity rejections/sec in production,
        // the wasted CPU + network + allocation amplify load. Reject
        // BEFORE the first `recv()` whenever we have a tight enough size
        // declaration to do so without false positives:
        //   * `ExactSize(N)` — the upload is exactly N bytes; if N alone
        //     exceeds capacity, the post-drain gate would reject it too.
        //   * `MaxSize(N)` — the upload is AT MOST N bytes; an early
        //     reject here would risk false positives when the actual
        //     payload is smaller and would fit. Skip the early gate for
        //     MaxSize and fall through to the existing post-drain check.
        // Reuses `check_backpressure_gate` so the error format / detail
        // matches the post-drain rejection bit-identically (callers see
        // the same `MemoryStoreAtCapacity` reason + retry hint).
        if let UploadSizeInfo::ExactSize(declared) = size_info {
            self.check_backpressure_gate(&owned_key, declared)?;
        }

        // Collect chunks without concatenation (scatter-gather).
        // Each chunk stays as its own Bytes allocation — no copies.
        let mut chunks = Vec::new();
        loop {
            let chunk = reader
                .recv()
                .await
                .err_tip(|| "Failed to recv in memory_store::update")?;
            if chunk.is_empty() {
                break; // EOF
            }
            chunks.push(chunk);
        }

        let total_bytes: u64 = chunks.iter().map(|c| c.len() as u64).sum();

        // Enforce `ExactSize` upfront — a truncated upstream (e.g.
        // Redis timeout dropping the channel) would otherwise insert a
        // partial entry and poison the cache. `MaxSize` is advisory:
        // overruns are rejected, underruns are accepted (the caller
        // declared a ceiling, not a floor). Be NOISY when the
        // invariant fires so the bug is visible (per CLAUDE.md
        // belt-and-suspenders policy).
        match size_info {
            UploadSizeInfo::ExactSize(declared) if total_bytes != declared => {
                error!(
                    key = ?owned_key,
                    declared,
                    received = total_bytes,
                    "MemoryStore::update: ExactSize mismatch — rejecting partial write",
                );
                return Err(make_err!(
                    Code::InvalidArgument,
                    "MemoryStore::update: ExactSize declared {declared} bytes but \
                     received {total_bytes} — refusing to insert a partial entry \
                     (would corrupt every future read)"
                ));
            }
            UploadSizeInfo::MaxSize(max) if total_bytes > max => {
                error!(
                    key = ?owned_key,
                    max,
                    received = total_bytes,
                    "MemoryStore::update: MaxSize exceeded — rejecting overrun",
                );
                return Err(make_err!(
                    Code::InvalidArgument,
                    "MemoryStore::update: MaxSize declared {max} bytes but \
                     received {total_bytes} — refusing to insert an oversized entry"
                ));
            }
            _ => {}
        }

        // #212 Phase 2.6: kill-switched backpressure gate. No-op when
        // the operator hasn't opted in (the production default), so the
        // historic silent-evict behavior is preserved bit-identically
        // for callers that haven't called `enable_emit_backpressure`.
        self.check_backpressure_gate(&owned_key, total_bytes)?;

        self.evicting_map
            .insert(owned_key.clone().into(), BytesWrapper::from_chunks(chunks))
            .await;
        debug!(
            key = ?owned_key,
            total_bytes,
            elapsed_ms = update_start.elapsed().as_millis() as u64,
            "MemoryStore::update: complete",
        );
        Ok(())
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        optimization == StoreOptimizations::SubscribesToUpdateOneshot
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        let update_start = std::time::Instant::now();
        let data_len = data.len();
        debug!(key = ?key, data_len, "MemoryStore::update_oneshot: start");
        // Small blobs may be slices of a much larger tonic receive buffer.
        // Copy them to avoid pinning the entire backing allocation in the
        // EvictingMap (e.g., 100-byte blob pinning a 16KiB h2 frame).
        // Large blobs are typically standalone allocations and safe to keep.
        let data = if !data.is_empty() && data.len() < 4096 {
            Bytes::copy_from_slice(&data)
        } else {
            data
        };
        let owned_key = key.into_owned();

        // #212 Phase 2.6: kill-switched backpressure gate. No-op when
        // the operator hasn't opted in (the production default).
        self.check_backpressure_gate(&owned_key, data_len as u64)?;

        self.evicting_map
            .insert(owned_key.clone().into(), BytesWrapper::from_single(data))
            .await;
        debug!(
            key = ?owned_key,
            data_len,
            elapsed_ms = update_start.elapsed().as_millis() as u64,
            "MemoryStore::update_oneshot: complete",
        );
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let mut offset =
            usize::try_from(offset).err_tip(|| "Could not convert offset to usize")?;
        let length = length
            .map(|v| usize::try_from(v).err_tip(|| "Could not convert length to usize"))
            .transpose()?;

        let owned_key = key.into_owned();
        if is_zero_digest(owned_key.clone()) {
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in memory store get_part")?;
            return Ok(());
        }

        let value = self
            .evicting_map
            .get(&owned_key)
            .await
            .err_tip_with_code(|_| (Code::NotFound, format!("Key {owned_key:?} not found")))?;
        let total_len = usize::try_from(value.len())
            .err_tip(|| "Could not convert value.len() to usize")?;
        let default_len = total_len.saturating_sub(offset);
        let mut remaining = length.unwrap_or(default_len).min(default_len);

        // Walk the chunk chain, sending each relevant piece without copying.
        let num_chunks = value.chunks.len();
        let actual_data_len: usize = value.chunks.iter().map(|c| c.len()).sum();
        let mut chunks_sent = 0u32;
        let mut bytes_sent_total = 0usize;
        let initial_remaining = remaining;

        // Detect total_len vs actual data mismatch before iterating.
        if total_len != actual_data_len {
            error!(
                key = ?owned_key,
                total_len,
                actual_data_len,
                num_chunks,
                "memory_store::get_part: total_len != sum(chunk.len()) — BytesWrapper is corrupt"
            );
        }

        for chunk in &value.chunks {
            if remaining == 0 {
                break;
            }
            let chunk_len = chunk.len();
            if offset >= chunk_len {
                // Skip this chunk entirely.
                offset -= chunk_len;
                continue;
            }
            let start = offset;
            let end = chunk_len.min(start + remaining);
            let slice = chunk.slice(start..end);
            remaining -= slice.len();
            bytes_sent_total += slice.len();
            offset = 0;
            let send_result = writer.send(slice).await;
            if let Err(e) = send_result {
                error!(
                    key = ?owned_key,
                    total_len,
                    num_chunks,
                    chunks_sent,
                    bytes_sent_total,
                    remaining,
                    err = %e,
                    "memory_store::get_part: send failed mid-stream"
                );
                return Err(e).err_tip(|| "Failed to write data in memory store");
            }
            chunks_sent += 1;
        }
        if remaining > 0 {
            error!(
                key = ?owned_key,
                total_len,
                actual_data_len,
                num_chunks,
                chunks_sent,
                initial_remaining,
                remaining,
                bytes_sent_total,
                "memory_store::get_part: incomplete read — chunks exhausted before all data sent"
            );
            return Err(make_err!(
                Code::Internal,
                "MemoryStore: chunks exhausted with {remaining} bytes remaining \
                 (total_len={total_len}, actual_data={actual_data_len}, chunks={num_chunks}, sent={chunks_sent})"
            ));
        }
        writer
            .send_eof()
            .err_tip(|| "Failed to write EOF in memory store get_part")?;
        Ok(())
    }

    /// Batch read that bypasses buf_channel overhead. Looks up all keys
    /// in the evicting map in a tight loop and returns contiguous Bytes
    /// directly, avoiding per-key channel allocation + async task pairs.
    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        let owned_keys: Vec<StoreKey<'static>> = keys
            .into_iter()
            .map(|k| k.into_owned())
            .collect();

        let lookup_keys: Vec<StoreKey<'static>> = owned_keys
            .iter()
            .filter(|k| !is_zero_digest((*k).clone()))
            .cloned()
            .collect();

        let batch_results = self.evicting_map.get_many(lookup_keys.iter()).await;

        let mut batch_iter = batch_results.into_iter();
        owned_keys
            .iter()
            .map(|key| {
                if is_zero_digest((*key).clone()) {
                    return Ok(Bytes::new());
                }
                match batch_iter.next() {
                    Some(Some(wrapper)) => Ok(wrapper.to_contiguous(length)),
                    Some(None) | None => Err(make_err!(
                        Code::NotFound,
                        "Key {:?} not found in MemoryStore",
                        key
                    )),
                }
            })
            .collect()
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Sync + Send + 'static> {
        self
    }

    fn register_health(self: Arc<Self>, registry: &mut HealthRegistryBuilder) {
        registry.register_indicator(self);
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        self.evicting_map
            .add_item_callback(ItemCallbackHolder::new(callback));
        Ok(())
    }

    /// MemoryStore is a leaf — it does not produce stable-storage digests
    /// (only persistent stores like FilesystemStore do). The default
    /// `drain_stable_digests` returns empty for `Leaf`.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    /// MemoryStore is a leaf — pinning here is a no-op. Pin protection is
    /// useful only against eviction (the FilesystemStore case); a memory
    /// store either has the blob or has lost it via cap eviction, in which
    /// case the upper layer should re-fetch.
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    /// MemoryStore is a leaf — `mark_stable` is a no-op (memory storage is
    /// not stable, so no BIS-feeder push from this layer). (Task #157.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

default_health_status_indicator!(MemoryStore);
