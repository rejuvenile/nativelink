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

//! Shared chunking helper for the streaming protocol envelope used by tasks
//! #97 (`BlobsInStableStorageChunk`), #98 (`PeerHintsChunk`), and #99
//! (`BlobsAvailableChunk`). Each producer emits N protocol messages whose
//! `(sequence, is_last)` pair lets the receiver reassemble in the obvious
//! way; the empty-input case still yields a single `is_last = true` chunk
//! at the iterator level — but whether the caller actually transmits that
//! empty terminal on the wire is its decision (see below).
//!
//! Why a helper? Three independent producers (cas → worker, scheduler →
//! worker, worker → scheduler) all want the same envelope shape, and the
//! correctness-critical piece — "is_last is set on the FINAL chunk only"
//! — should live in one place so each call site doesn't reinvent it
//! subtly differently.
//!
//! Empty-terminal policy: the iterator's empty-input behavior (one
//! `is_last = true` empty chunk) is INFORMATIONAL ONLY for callers that
//! WANT to surface "I have nothing to send" as a wire message. It is NOT
//! a load-bearing protocol contract — no current receiver gates state on
//! the arrival of an empty terminal. The #98 (peer-hints) caller skips
//! emission entirely on an empty hint list to avoid one wire message per
//! StartAction in the no-hint case; #97/#99 callers may make the
//! opposite choice if they do want a heartbeat-style "still alive, no
//! data this tick" marker. Pick whichever is right for the consumer's
//! semantics — just don't assume the empty terminal will appear without
//! checking the producer's behavior.
//!
//! The helper is intentionally generic over the payload `T` — the proto
//! `oneof payload` arm decides what to wrap each chunk in. Callers pass in
//! `max_per_chunk` so different payload types can pick a chunk size based
//! on per-element size (PeerHint ~250B → 256 per chunk = ~64 KiB; Digest
//! ~32B → 128 K per chunk = ~4 MiB).

use core::iter::Iterator;

/// Iterator that yields `(sequence, is_last, items)` tuples chunking a
/// source iterator into batches of at most `max_per_chunk`.
///
/// Always yields at least one tuple, even when `source` is empty (the
/// single tuple is `(0, true, vec![])`). The empty-terminal yield is a
/// CONVENIENCE for callers that want to surface "no data" as a wire
/// message; receivers MUST NOT assume it always arrives — see the
/// module-level docs on the empty-terminal policy. Producers that have
/// nothing meaningful to send may legitimately skip emission entirely.
///
/// The `sequence` field is monotonically increasing 0, 1, 2, ... within
/// one stream and is informational only — receivers do NOT need to
/// reassemble in order under the direct-merge design (each chunk's
/// payload is independently meaningful), but logging and diagnostics
/// benefit from a stable index.
pub struct ChunkIter<I: Iterator> {
    source: I,
    max_per_chunk: usize,
    sequence: u32,
    /// Once we've yielded the terminal `is_last = true` chunk, we stop.
    done: bool,
}

impl<I: Iterator> ChunkIter<I> {
    /// Create a new chunking iterator. `max_per_chunk` must be at least 1;
    /// passing 0 is treated as 1 to avoid an infinite loop of empty
    /// chunks.
    pub fn new(source: I, max_per_chunk: usize) -> Self {
        Self {
            source,
            max_per_chunk: max_per_chunk.max(1),
            sequence: 0,
            done: false,
        }
    }
}

/// Yielded chunk: `(sequence, is_last, items)`.
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk<T> {
    pub sequence: u32,
    pub is_last: bool,
    pub items: Vec<T>,
}

impl<I: Iterator> Iterator for ChunkIter<I> {
    type Item = Chunk<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut buf: Vec<I::Item> = Vec::with_capacity(self.max_per_chunk);
        for _ in 0..self.max_per_chunk {
            match self.source.next() {
                Some(item) => buf.push(item),
                None => break,
            }
        }
        // Peek by trying one more pull to know if this is the final chunk.
        // We do this by looking at whether buf is full AND the next pull
        // succeeds. Cheaper alternative: declare a chunk "last" iff we
        // didn't fill it. That's correct because the loop above always
        // pulls until either max_per_chunk is reached or source is empty.
        let is_last = buf.len() < self.max_per_chunk;
        if is_last {
            self.done = true;
        }
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        Some(Chunk {
            sequence,
            is_last,
            items: buf,
        })
    }
}

/// Convenience: chunk a source iterator and return all chunks as a Vec.
/// Crate-private — only the in-module tests use this; production code uses
/// the streaming `ChunkIter::new` directly to avoid materializing all
/// chunks at once. If a future production caller wants the eager-collect
/// shape, promote to `pub` and add a caller in the same diff (see
/// `feedback_public_api_needs_caller_in_diff` — public API without an
/// in-diff caller is dead code).
pub(crate) fn chunk_iter<I: IntoIterator>(
    source: I,
    max_per_chunk: usize,
) -> Vec<Chunk<I::Item>> {
    ChunkIter::new(source.into_iter(), max_per_chunk).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_source_yields_one_terminal_chunk() {
        let chunks: Vec<Chunk<u32>> = chunk_iter(Vec::<u32>::new(), 16);
        assert_eq!(chunks.len(), 1, "empty input must still yield 1 chunk");
        assert_eq!(chunks[0].sequence, 0);
        assert!(chunks[0].is_last, "the single empty chunk must be terminal");
        assert!(chunks[0].items.is_empty());
    }

    #[test]
    fn single_partial_chunk() {
        let chunks: Vec<Chunk<u32>> = chunk_iter(0u32..3, 16);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].sequence, 0);
        assert!(chunks[0].is_last);
        assert_eq!(chunks[0].items, vec![0, 1, 2]);
    }

    #[test]
    fn exact_boundary_full_chunk_then_empty_terminal() {
        // 16 items with chunk size 16 should produce one full chunk +
        // one empty terminal chunk. Without the empty-terminal guarantee
        // the receiver couldn't distinguish "chunk of 16, more coming"
        // from "chunk of 16, end of stream".
        let chunks: Vec<Chunk<u32>> = chunk_iter(0u32..16, 16);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].sequence, 0);
        assert!(!chunks[0].is_last);
        assert_eq!(chunks[0].items.len(), 16);
        assert_eq!(chunks[1].sequence, 1);
        assert!(chunks[1].is_last);
        assert!(chunks[1].items.is_empty());
    }

    #[test]
    fn multi_chunk_with_partial_tail() {
        // 35 items at chunk size 16: 16 + 16 + 3.
        let chunks: Vec<Chunk<u32>> = chunk_iter(0u32..35, 16);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].sequence, 0);
        assert!(!chunks[0].is_last);
        assert_eq!(chunks[0].items.len(), 16);
        assert_eq!(chunks[1].sequence, 1);
        assert!(!chunks[1].is_last);
        assert_eq!(chunks[1].items.len(), 16);
        assert_eq!(chunks[2].sequence, 2);
        assert!(chunks[2].is_last);
        assert_eq!(chunks[2].items.len(), 3);
    }

    #[test]
    fn one_million_items_no_truncation() {
        // Stress the iterator at the size #98 cares about. 1M items at
        // chunk size 4096 -> 245 chunks. Verify totals + last-chunk flag.
        let chunks: Vec<Chunk<u32>> = chunk_iter(0u32..1_000_000, 4096);
        let total: usize = chunks.iter().map(|c| c.items.len()).sum();
        assert_eq!(total, 1_000_000, "no truncation");
        assert!(chunks.last().unwrap().is_last);
        assert!(chunks[..chunks.len() - 1].iter().all(|c| !c.is_last));
        // Sequences must be monotonic 0, 1, 2, ...
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.sequence as usize, i);
        }
    }

    #[test]
    fn zero_max_per_chunk_treated_as_one() {
        let chunks: Vec<Chunk<u32>> = chunk_iter(0u32..3, 0);
        // 3 single-item chunks + 1 empty terminal? No — last chunk is
        // detected as "didn't fill capacity 1", so the third item itself
        // doesn't fill a capacity-1 chunk... wait, it does. So 3 full
        // chunks + 1 empty terminal = 4. That's the contract: the empty
        // terminal is required so receivers see is_last.
        assert_eq!(chunks.len(), 4);
        assert!(!chunks[0].is_last && chunks[0].items == vec![0]);
        assert!(!chunks[1].is_last && chunks[1].items == vec![1]);
        assert!(!chunks[2].is_last && chunks[2].items == vec![2]);
        assert!(chunks[3].is_last && chunks[3].items.is_empty());
    }
}
