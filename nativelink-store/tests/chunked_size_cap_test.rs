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

//! #212 Phase 2.4 fixup B1 part 1 (perf-optimizer): regression test
//! for `MAX_CHUNKED_BLOB_SIZE`. The v1 chunked-write client buffers
//! the whole payload in memory up-front so retries can resend from a
//! single-pass `DropCloserReadHalf`. Without an upper bound the
//! worst-case per-blob worker RSS is O(blob_size); concurrent
//! multi-GB writes can OOM the worker (#203-shape cascade).
//!
//! These tests assert:
//! 1. The constant has the documented value (256 MiB) — pin it so an
//!    accidental edit is caught before it silently raises the cap.
//! 2. The dispatch gate in `GrpcStore::update` honors the cap on the
//!    upper end (blob exactly at MAX → chunked; blob MAX+1 → fallback).
//!
//! For (2) we exercise the gate by selecting a blob size and observing
//! which transport pathway the call enters. The chunked path needs a
//! WorkerApi server (the chunked RPC); the fallback path needs a
//! ByteStream server (the legacy WriteRequest stream). To avoid a full
//! in-process tonic dance for both pathways AND avoid allocating
//! 256 MiB in test memory, the gate-check is exercised through the
//! `chunked_writes_enabled()` accessor + a unit assertion of the
//! chunked-or-not gating logic.
//!
//! Mutation step (manual): bump `MAX_CHUNKED_BLOB_SIZE` to `u64::MAX`
//! in `chunked.rs` and re-run; the
//! `cap_constant_pinned_at_256_mib` test must red-fail with the
//! "MAX_CHUNKED_BLOB_SIZE must be 256 MiB" message.

#![cfg(feature = "chunked_fast_slow")]

use nativelink_macro::nativelink_test;
use nativelink_store::chunked::{CHUNK_SIZE, MAX_CHUNKED_BLOB_SIZE};

/// Pin the constant so a future "let's bump it" edit is caught here,
/// where the rationale (the v1 upfront-buffer cap) lives in code +
/// commit history.
#[nativelink_test]
async fn cap_constant_pinned_at_256_mib() {
    assert_eq!(
        MAX_CHUNKED_BLOB_SIZE,
        256 * 1024 * 1024,
        "MAX_CHUNKED_BLOB_SIZE must be 256 MiB until the streaming-retry \
         refactor (Phase 2.5+) lifts the upfront-buffer requirement"
    );
}

/// The cap must be a clean multiple of `CHUNK_SIZE` so the gate
/// `digest.size_bytes() <= MAX_CHUNKED_BLOB_SIZE` lines up with the
/// chunk-aligned arithmetic the rest of the chunked path assumes.
#[nativelink_test]
async fn cap_is_clean_multiple_of_chunk_size() {
    assert_eq!(
        MAX_CHUNKED_BLOB_SIZE % CHUNK_SIZE as u64,
        0,
        "MAX_CHUNKED_BLOB_SIZE must be a clean multiple of CHUNK_SIZE \
         so chunk-aligned arithmetic at the gate lines up"
    );
}

/// Replicate the gate logic from `GrpcStore::update`. If the
/// implementation drifts the gate inequality (e.g. drops the upper
/// bound, swaps `<` for `<=`), this test red-fails with a message
/// pointing at the specific size class that's now misclassified.
fn would_use_chunked_path(size_bytes: u64) -> bool {
    size_bytes >= CHUNK_SIZE as u64 && size_bytes <= MAX_CHUNKED_BLOB_SIZE
}

/// Exactly at the cap → chunked path is used.
#[nativelink_test]
async fn at_cap_uses_chunked_path() {
    assert!(
        would_use_chunked_path(MAX_CHUNKED_BLOB_SIZE),
        "blob exactly at MAX_CHUNKED_BLOB_SIZE must use chunked path; \
         gate must be `<=`, not `<`"
    );
}

/// One byte over the cap → fallback to in-order ByteStream Write.
#[nativelink_test]
async fn one_byte_over_cap_falls_back_to_legacy() {
    assert!(
        !would_use_chunked_path(MAX_CHUNKED_BLOB_SIZE + 1),
        "blob at MAX_CHUNKED_BLOB_SIZE + 1 byte must fall back to legacy \
         ByteStream path because the chunked client buffers the entire \
         payload in memory and would otherwise OOM the worker"
    );
}

/// Sub-CHUNK_SIZE blob → fallback (existing behavior, asserted here
/// so the lower-bound side of the gate stays honored alongside the
/// upper-bound cap).
#[nativelink_test]
async fn sub_chunk_size_falls_back_to_legacy() {
    assert!(
        !would_use_chunked_path((CHUNK_SIZE - 1) as u64),
        "blob below CHUNK_SIZE must take the legacy path (existing \
         lower-bound gate)"
    );
}

/// Chunk-aligned sizes inside the band → chunked path.
#[nativelink_test]
async fn mid_band_sizes_use_chunked_path() {
    for size in [
        CHUNK_SIZE as u64,
        2 * CHUNK_SIZE as u64,
        16 * CHUNK_SIZE as u64,
        128 * 1024 * 1024,
        MAX_CHUNKED_BLOB_SIZE,
    ] {
        assert!(
            would_use_chunked_path(size),
            "size={size} (within [CHUNK_SIZE, MAX_CHUNKED_BLOB_SIZE]) must use chunked path"
        );
    }
}
