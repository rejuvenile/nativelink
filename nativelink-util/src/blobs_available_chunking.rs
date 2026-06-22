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

//! (#99 — PR3 of #80) Worker-side chunker for `BlobsAvailableNotification`.
//! Splits the FIVE unbounded-by-design fields plus four secondary fields
//! into bounded `BlobsAvailableChunk` protos so a single fleet-wide
//! reconnect storm does not produce one 50K+-digest message that
//! exceeds the gRPC max-decoding-message-size and kills the worker→server
//! channel.
//!
//! **Path A semantics: defer wipe until terminal chunk.** The receiver
//! buffers all chunks of one broadcast and applies them atomically when
//! `is_last=true` arrives. Empty broadcasts still emit one terminal
//! chunk so the receiver always sees a commit marker; with no real data
//! this is a benign no-op (chunk 0 with empty slices and `is_last=true`).
//!
//! Scalar header fields (`worker_cas_endpoint`, `mirror_used_bytes`,
//! `mirror_max_bytes`, `cpu_load_pct`, `p_core_load_pct`,
//! `e_core_load_pct`, `is_full_subtree_snapshot`) only ride on chunk 0;
//! the receiver carries them forward across chunks. `is_full_snapshot`,
//! `broadcast_id`, `worker_instance_token`, `store_id` are repeated on
//! every chunk so a late-arriving chunk-0 doesn't drop the flag and so
//! the receiver can key the accumulator by
//! (`broadcast_id`, `worker_instance_token`).

extern crate alloc;

use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobDigestInfo, BlobsAvailableChunk, BlobsAvailableNotification, MirrorPinEntry,
};

/// Soft-limit on the number of payload entries (summed across ALL
/// repeated fields on a chunk) packed into one `BlobsAvailableChunk`.
///
/// Sized so one chunk's encoded payload stays well under the worker→
/// server gRPC max-decoding-message limit. Each entry is one of:
///   * `BlobDigestInfo` (~40 B serialized w/ framing)
///   * `Digest` (~40 B serialized w/ framing)
///   * `MirrorPinEntry` (~80 B serialized w/ framing — digest + store_id)
/// Worst case (all `MirrorPinEntry`) at 4096 entries = ~320 KiB
/// encoded; well below the 64 MiB worker→server decoder limit and
/// below typical h2/QUIC frame fragmentation thresholds.
pub const BLOBS_AVAILABLE_PER_CHUNK: usize = 4096;

/// Threshold (in estimated encoded bytes) at which `should_chunk`
/// flips from "use legacy single-message" to "use chunked envelope".
/// Pinned by design to be well below the 64 MiB worker→server limit
/// AND well above typical steady-state notification sizes (most ticks
/// carry < 100 entries, ~4 KiB encoded).
pub const BLOBS_AVAILABLE_CHUNK_THRESHOLD_BYTES: usize = 32 * 1024;

/// Hard cap on the number of chunks a single broadcast can emit.
/// MUST equal the server's `MAX_SEQUENCES` constant in
/// `nativelink-service/src/blobs_available_accumulator.rs`.
///
/// 256 chunks × `BLOBS_AVAILABLE_PER_CHUNK` (4096) = 1_048_576 entries
/// per broadcast, comfortably above the per-conn entries cap of 1M.
///
/// If a worker would emit > 256 chunks for one broadcast, the chunker
/// returns Err — better to fail loudly at the producer than to emit
/// chunks the server will silently discard at sequence ≥ 256.
pub const BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST: usize = 256;

/// Split a `BlobsAvailableNotification` into one-or-more
/// `BlobsAvailableChunk` envelopes.
///
/// Always produces at least one chunk (an empty terminal `is_last=true`
/// chunk for the empty input case); receivers MUST always see a terminal
/// chunk so they can commit + drop the per-broadcast accumulator.
///
/// Chunk 0 carries the scalar header fields; subsequent chunks leave
/// them at proto3 defaults. Every chunk repeats the broadcast-keying
/// identifiers (`broadcast_id`, `worker_instance_token`, `store_id`)
/// and the `is_full_snapshot` flag.
///
/// `max_per_chunk` is the soft cap on total payload entries per chunk;
/// the chunker fills each chunk by pulling entries in declaration order
/// from the 8 source slices until the cap is hit OR the source is
/// exhausted, whichever comes first.
///
/// Returns `Err` if the notification would require more than
/// `BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST` chunks. The server's
/// accumulator rejects sequences ≥ that cap; failing fast at the
/// producer is strictly better than emitting chunks the server will
/// silently discard.
pub fn chunk_blobs_available(
    notification: BlobsAvailableNotification,
    broadcast_id: u64,
    worker_instance_token: u64,
    store_id: String,
    max_per_chunk: usize,
) -> Result<Vec<BlobsAvailableChunk>, &'static str> {
    let max_per_chunk = max_per_chunk.max(1);

    let BlobsAvailableNotification {
        worker_cas_endpoint,
        digests: legacy_digests,
        is_full_snapshot,
        evicted_digests,
        digest_infos,
        cpu_load_pct,
        cached_directory_digests,
        added_subtree_digests,
        removed_subtree_digests,
        is_full_subtree_snapshot,
        p_core_load_pct,
        e_core_load_pct,
        pinned_mirror_digests,
        mirror_used_bytes,
        mirror_max_bytes,
        pinned_mirror_entries,
        pinned_ac_mirror_entries,
        indefinite_pin_saturated,
        swap_used_bytes,
        pageouts_per_sec,
    } = notification;

    // Fold legacy field 2 (`digests`) into `digest_infos` for backwards
    // compat with old workers that didn't populate `digest_infos`. The
    // canonical chunked wire shape carries `BlobDigestInfo` only.
    let mut all_digest_infos: Vec<BlobDigestInfo> = digest_infos;
    all_digest_infos.extend(legacy_digests.into_iter().map(|d| BlobDigestInfo {
        digest: Some(d),
    }));

    let mut src = ChunkSources {
        digests: all_digest_infos.into_iter(),
        cached_directory_digests: cached_directory_digests.into_iter(),
        pinned_mirror_entries: pinned_mirror_entries.into_iter(),
        pinned_ac_mirror_entries: pinned_ac_mirror_entries.into_iter(),
        evicted_digests: evicted_digests.into_iter(),
        added_subtree_digests: added_subtree_digests.into_iter(),
        removed_subtree_digests: removed_subtree_digests.into_iter(),
        pinned_mirror_digests: pinned_mirror_digests.into_iter(),
    };

    let mut chunks: Vec<BlobsAvailableChunk> = Vec::new();
    let mut sequence: u32 = 0;

    loop {
        let mut chunk = BlobsAvailableChunk {
            broadcast_id,
            sequence,
            is_last: false,
            worker_instance_token,
            store_id: store_id.clone(),
            is_full_snapshot,
            worker_cas_endpoint: if sequence == 0 {
                worker_cas_endpoint.clone()
            } else {
                String::new()
            },
            is_full_subtree_snapshot: if sequence == 0 {
                is_full_subtree_snapshot
            } else {
                false
            },
            cpu_load_pct: if sequence == 0 { cpu_load_pct } else { 0 },
            p_core_load_pct: if sequence == 0 { p_core_load_pct } else { 0 },
            e_core_load_pct: if sequence == 0 { e_core_load_pct } else { 0 },
            mirror_used_bytes: if sequence == 0 { mirror_used_bytes } else { 0 },
            mirror_max_bytes: if sequence == 0 { mirror_max_bytes } else { 0 },
            // (FL-681) Saturation rides chunk 0 only; the accumulator carries
            // it forward into the reassembled notification.
            indefinite_pin_saturated: if sequence == 0 {
                indefinite_pin_saturated
            } else {
                false
            },
            // Host swap/page-out pressure: chunk-0-only scalars (like
            // cpu_load_pct); the accumulator carries chunk 0's value
            // forward into the reassembled notification.
            swap_used_bytes: if sequence == 0 { swap_used_bytes } else { 0 },
            pageouts_per_sec: if sequence == 0 { pageouts_per_sec } else { 0 },
            digests: Vec::new(),
            cached_directory_digests: Vec::new(),
            pinned_mirror_entries: Vec::new(),
            pinned_ac_mirror_entries: Vec::new(),
            evicted_digests: Vec::new(),
            added_subtree_digests: Vec::new(),
            removed_subtree_digests: Vec::new(),
            pinned_mirror_digests: Vec::new(),
        };

        let mut budget = max_per_chunk;
        budget = drain_into(&mut src.digests, &mut chunk.digests, budget);
        budget = drain_into(
            &mut src.cached_directory_digests,
            &mut chunk.cached_directory_digests,
            budget,
        );
        budget = drain_into(
            &mut src.pinned_mirror_entries,
            &mut chunk.pinned_mirror_entries,
            budget,
        );
        budget = drain_into(
            &mut src.pinned_ac_mirror_entries,
            &mut chunk.pinned_ac_mirror_entries,
            budget,
        );
        budget = drain_into(
            &mut src.evicted_digests,
            &mut chunk.evicted_digests,
            budget,
        );
        budget = drain_into(
            &mut src.added_subtree_digests,
            &mut chunk.added_subtree_digests,
            budget,
        );
        budget = drain_into(
            &mut src.removed_subtree_digests,
            &mut chunk.removed_subtree_digests,
            budget,
        );
        budget = drain_into(
            &mut src.pinned_mirror_digests,
            &mut chunk.pinned_mirror_digests,
            budget,
        );

        let drained_this_chunk = max_per_chunk - budget;
        let exhausted = src.is_exhausted();

        if exhausted {
            chunk.is_last = true;
            chunks.push(chunk);
            break;
        }

        // Defensive: forward progress guarantee. The combination of
        // is_exhausted() == false AND drained_this_chunk == 0 should
        // be unreachable, but if a future malformed source ever lands
        // here, terminate rather than spin forever.
        if drained_this_chunk == 0 {
            chunk.is_last = true;
            chunks.push(chunk);
            break;
        }

        chunks.push(chunk);
        sequence = sequence.saturating_add(1);

        // (Fix #2 / dsr BLOCK-1) Hard cap on chunk count per broadcast
        // mirroring the server's MAX_SEQUENCES. If we are about to
        // emit a chunk at sequence == MAX_CHUNKS, fail loudly: the
        // server would silently discard sequences ≥ the cap, and the
        // worker would have no signal that the broadcast didn't
        // commit.
        if (sequence as usize) >= BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST {
            return Err(
                "BlobsAvailable broadcast would exceed BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST \
                 (256); chunker output cannot round-trip the server's MAX_SEQUENCES cap",
            );
        }
    }

    Ok(chunks)
}

/// Should the worker emit `notification` via the chunked path or the
/// legacy single-message path?
///
/// The chunked path adds per-chunk header overhead and a per-broadcast
/// accumulator allocation on the server, so prefer the legacy path when
/// the notification's encoded size comfortably fits in one gRPC message.
pub fn should_chunk(notification: &BlobsAvailableNotification) -> bool {
    estimated_encoded_bytes(notification) > BLOBS_AVAILABLE_CHUNK_THRESHOLD_BYTES
}

fn estimated_encoded_bytes(n: &BlobsAvailableNotification) -> usize {
    n.digests.len() * 40
        + n.digest_infos.len() * 40
        + n.cached_directory_digests.len() * 40
        + n.pinned_mirror_entries.len() * 80
        + n.pinned_ac_mirror_entries.len() * 80
        + n.evicted_digests.len() * 40
        + n.added_subtree_digests.len() * 40
        + n.removed_subtree_digests.len() * 40
        + n.pinned_mirror_digests.len() * 40
}

/// Source-of-truth for the 8 drainable slices. ExactSizeIterator over
/// `Vec::IntoIter` lets `is_exhausted()` be a pure-`len()` check
/// without consuming any items.
struct ChunkSources {
    digests: alloc::vec::IntoIter<BlobDigestInfo>,
    cached_directory_digests: alloc::vec::IntoIter<Digest>,
    pinned_mirror_entries: alloc::vec::IntoIter<MirrorPinEntry>,
    pinned_ac_mirror_entries: alloc::vec::IntoIter<MirrorPinEntry>,
    evicted_digests: alloc::vec::IntoIter<Digest>,
    added_subtree_digests: alloc::vec::IntoIter<Digest>,
    removed_subtree_digests: alloc::vec::IntoIter<Digest>,
    pinned_mirror_digests: alloc::vec::IntoIter<Digest>,
}

impl ChunkSources {
    fn is_exhausted(&self) -> bool {
        self.digests.len() == 0
            && self.cached_directory_digests.len() == 0
            && self.pinned_mirror_entries.len() == 0
            && self.pinned_ac_mirror_entries.len() == 0
            && self.evicted_digests.len() == 0
            && self.added_subtree_digests.len() == 0
            && self.removed_subtree_digests.len() == 0
            && self.pinned_mirror_digests.len() == 0
    }
}

fn drain_into<T>(
    src: &mut alloc::vec::IntoIter<T>,
    dst: &mut Vec<T>,
    mut budget: usize,
) -> usize {
    while budget > 0 {
        match src.next() {
            Some(item) => {
                dst.push(item);
                budget -= 1;
            }
            None => break,
        }
    }
    budget
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(i: u64) -> Digest {
        Digest {
            hash: format!("{:064x}", i),
            size_bytes: i64::try_from(i).unwrap_or(0),
        }
    }

    fn bdi(i: u64) -> BlobDigestInfo {
        BlobDigestInfo { digest: Some(d(i)) }
    }

    fn mpe(i: u64, store: &str) -> MirrorPinEntry {
        MirrorPinEntry {
            digest: Some(d(i)),
            store_id: store.to_string(),
        }
    }

    #[test]
    fn empty_notification_yields_one_terminal_chunk() {
        let n = BlobsAvailableNotification::default();
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 4096)
            .expect("empty notification must chunk successfully");
        assert_eq!(chunks.len(), 1, "empty input must still emit terminal");
        assert!(chunks[0].is_last);
        assert_eq!(chunks[0].sequence, 0);
        assert_eq!(chunks[0].broadcast_id, 1);
        assert_eq!(chunks[0].worker_instance_token, 99);
    }

    #[test]
    fn single_chunk_under_cap() {
        let n = BlobsAvailableNotification {
            digest_infos: (0..3).map(bdi).collect(),
            cached_directory_digests: (10..12).map(d).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 7, 42, String::new(), 4096)
            .expect("single chunk must succeed");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].sequence, 0);
        assert!(chunks[0].is_last);
        assert_eq!(chunks[0].digests.len(), 3);
        assert_eq!(chunks[0].cached_directory_digests.len(), 2);
    }

    #[test]
    fn multi_chunk_partitions_correctly() {
        let n = BlobsAvailableNotification {
            digest_infos: (0..5).map(bdi).collect(),
            cached_directory_digests: (10..15).map(d).collect(),
            pinned_mirror_entries: (20..25).map(|i| mpe(i, "store_a")).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 6)
            .expect("3-chunk partition must succeed");
        assert_eq!(chunks.len(), 3);
        assert!(!chunks[0].is_last);
        assert!(!chunks[1].is_last);
        assert!(chunks[2].is_last);
        let total_digests: usize = chunks.iter().map(|c| c.digests.len()).sum();
        let total_cdd: usize = chunks
            .iter()
            .map(|c| c.cached_directory_digests.len())
            .sum();
        let total_entries: usize = chunks.iter().map(|c| c.pinned_mirror_entries.len()).sum();
        assert_eq!(total_digests, 5);
        assert_eq!(total_cdd, 5);
        assert_eq!(total_entries, 5);
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.sequence as usize, i);
        }
    }

    #[test]
    fn header_scalars_only_on_chunk_0() {
        let n = BlobsAvailableNotification {
            worker_cas_endpoint: "grpc://w1:50081".to_string(),
            cpu_load_pct: 42,
            p_core_load_pct: 40,
            e_core_load_pct: 30,
            mirror_used_bytes: 12345,
            mirror_max_bytes: 65536,
            is_full_subtree_snapshot: true,
            digest_infos: (0..8).map(bdi).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 4)
            .expect("2+ chunks must succeed");
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0].worker_cas_endpoint, "grpc://w1:50081");
        assert_eq!(chunks[0].cpu_load_pct, 42);
        assert_eq!(chunks[0].mirror_used_bytes, 12345);
        assert!(chunks[0].is_full_subtree_snapshot);
        for c in &chunks[1..] {
            assert!(c.worker_cas_endpoint.is_empty());
            assert_eq!(c.cpu_load_pct, 0);
            assert_eq!(c.mirror_used_bytes, 0);
            assert!(!c.is_full_subtree_snapshot);
        }
    }

    #[test]
    fn indefinite_pin_saturated_rides_chunk_zero_only() {
        // (FL-681) Saturation is a chunk-0-only scalar (like cpu_load_pct):
        // the accumulator carries chunk 0's value forward. Subsequent chunks
        // MUST leave it at the proto3 default so a value isn't double-counted
        // or contradicted across chunks.
        let n = BlobsAvailableNotification {
            indefinite_pin_saturated: true,
            digest_infos: (0..10).map(bdi).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 3)
            .expect("multi-chunk must succeed");
        assert!(chunks.len() >= 3, "need >1 chunk to test scalar placement");
        assert!(
            chunks[0].indefinite_pin_saturated,
            "FL-681: chunk 0 must carry the saturation flag"
        );
        for c in &chunks[1..] {
            assert!(
                !c.indefinite_pin_saturated,
                "FL-681: non-zero chunks must leave indefinite_pin_saturated at \
                 the proto3 default (chunk-0-only scalar)"
            );
        }
    }

    #[test]
    fn swap_fields_ride_chunk_zero_only() {
        // swap_used_bytes + pageouts_per_sec are chunk-0-only scalars
        // (like cpu_load_pct): the accumulator carries chunk 0's value
        // forward. Subsequent chunks MUST leave them at the proto3 default
        // 0 so a value isn't double-counted or contradicted across chunks.
        let n = BlobsAvailableNotification {
            swap_used_bytes: 9_876_543_210,
            pageouts_per_sec: 4242,
            digest_infos: (0..10).map(bdi).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 3)
            .expect("multi-chunk must succeed");
        assert!(chunks.len() >= 3, "need >1 chunk to test scalar placement");
        assert_eq!(
            chunks[0].swap_used_bytes, 9_876_543_210,
            "chunk 0 must carry swap_used_bytes"
        );
        assert_eq!(
            chunks[0].pageouts_per_sec, 4242,
            "chunk 0 must carry pageouts_per_sec"
        );
        for c in &chunks[1..] {
            assert_eq!(
                c.swap_used_bytes, 0,
                "non-zero chunks must leave swap_used_bytes at the proto3 \
                 default (chunk-0-only scalar)"
            );
            assert_eq!(
                c.pageouts_per_sec, 0,
                "non-zero chunks must leave pageouts_per_sec at the proto3 \
                 default (chunk-0-only scalar)"
            );
        }
    }

    #[test]
    fn full_snapshot_repeated_on_every_chunk() {
        let n = BlobsAvailableNotification {
            is_full_snapshot: true,
            digest_infos: (0..10).map(bdi).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 3)
            .expect("multi-chunk must succeed");
        assert!(chunks.len() >= 3);
        for c in &chunks {
            assert!(
                c.is_full_snapshot,
                "is_full_snapshot must be repeated on every chunk so a \
                 late chunk-0 doesn't drop the flag"
            );
        }
        assert!(chunks.last().unwrap().is_last);
    }

    #[test]
    fn all_5_unbounded_fields_can_be_partitioned() {
        let n = BlobsAvailableNotification {
            digest_infos: (0..2).map(bdi).collect(),
            cached_directory_digests: (10..12).map(d).collect(),
            pinned_mirror_entries: vec![mpe(20, "cas_STORE")],
            pinned_ac_mirror_entries: vec![mpe(30, "AC_MAIN_STORE")],
            evicted_digests: (40..42).map(d).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 1)
            .expect("8-chunk partition must succeed");
        // 2 + 2 + 1 + 1 + 2 = 8 entries -> 8 chunks; last is is_last.
        assert_eq!(chunks.len(), 8);
        assert!(chunks.last().unwrap().is_last);
        let total: usize = chunks
            .iter()
            .map(|c| {
                c.digests.len()
                    + c.cached_directory_digests.len()
                    + c.pinned_mirror_entries.len()
                    + c.pinned_ac_mirror_entries.len()
                    + c.evicted_digests.len()
            })
            .sum();
        assert_eq!(total, 8);
    }

    #[test]
    fn one_million_digests_no_truncation() {
        // 1M / 4096 = ~245 chunks; well under the 256 cap.
        let n = BlobsAvailableNotification {
            digest_infos: (0..1_000_000).map(bdi).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 4096)
            .expect("1M digests must round-trip within the 256-chunk cap");
        let total: usize = chunks.iter().map(|c| c.digests.len()).sum();
        assert_eq!(total, 1_000_000);
        assert!(chunks.last().unwrap().is_last);
        assert!(chunks[..chunks.len() - 1].iter().all(|c| !c.is_last));
        assert!(
            chunks.len() <= BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST,
            "chunk count {} must stay within the per-broadcast cap {}",
            chunks.len(),
            BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST
        );
    }

    /// (Fix #2 / dsr BLOCK-1) When a notification would require more
    /// than `BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST` chunks, the
    /// chunker MUST fail loudly. Forcing `max_per_chunk = 1` lets
    /// us hit the cap with a tiny, fast test rather than allocating
    /// 1M+ entries.
    #[test]
    fn chunker_errors_when_exceeding_max_chunks_per_broadcast() {
        // 257 entries × 1-per-chunk = 257 chunks > 256 cap.
        let count = (BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST as u64) + 1;
        let n = BlobsAvailableNotification {
            digest_infos: (0..count).map(bdi).collect(),
            ..Default::default()
        };
        let result = chunk_blobs_available(n, 1, 99, String::new(), 1);
        assert!(
            result.is_err(),
            "chunker must reject broadcasts requiring > {} chunks; \
             saw Ok({:?}) — Fix #2 (dsr BLOCK-1) regression",
            BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST,
            result.as_ref().map(|c| c.len()),
        );
    }

    /// Boundary: exactly `BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST`
    /// chunks must succeed (the cap is "exceed", not "reach").
    #[test]
    fn chunker_allows_exact_max_chunks_per_broadcast() {
        let count = BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST as u64;
        let n = BlobsAvailableNotification {
            digest_infos: (0..count).map(bdi).collect(),
            ..Default::default()
        };
        let chunks = chunk_blobs_available(n, 1, 99, String::new(), 1)
            .expect("exactly-cap broadcast must succeed");
        assert_eq!(chunks.len(), BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST);
        assert!(chunks.last().unwrap().is_last);
    }

    #[test]
    fn should_chunk_below_threshold_returns_false() {
        let n = BlobsAvailableNotification {
            digest_infos: (0..100).map(bdi).collect(),
            ..Default::default()
        };
        assert!(!should_chunk(&n));
    }

    #[test]
    fn should_chunk_above_threshold_returns_true() {
        let n = BlobsAvailableNotification {
            digest_infos: (0..1000).map(bdi).collect(),
            ..Default::default()
        };
        assert!(should_chunk(&n));
    }
}
