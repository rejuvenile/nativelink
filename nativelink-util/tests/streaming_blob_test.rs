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

//! `StreamingBlob` regression tests.
//!
//! At present this file pins the `SLIDING_WINDOW_EVICTION_MARKER`
//! cross-crate contract: the substring is referenced by the `FastSlowStore`
//! D.1 fallback predicate (#325) to recognize sliding-window-eviction
//! errors. Any rename of the production message that drops the substring
//! would silently break the predicate; this test is the early-warning.

use bytes::Bytes;
use nativelink_error::Code;
use nativelink_macro::nativelink_test;
use nativelink_util::common::DigestInfo;
use nativelink_util::streaming_blob::{SLIDING_WINDOW_EVICTION_MARKER, StreamingBlob};

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Forward-compatibility contract: the production `Code::Unavailable`
/// message produced when a reader's cursor falls behind the sliding
/// window MUST contain `SLIDING_WINDOW_EVICTION_MARKER` so the
/// FastSlowStore D.1 fallback predicate (`fast_slow_store.rs` near the
/// streaming-buffer error handling) recognizes the eviction class and
/// splices in a fresh slow-store read.
///
/// Mutation step: change the value of `SLIDING_WINDOW_EVICTION_MARKER`
/// in `streaming_blob.rs` (or rename the substring out of the production
/// `make_err!` formatter without updating the const) — this test must
/// red-fail with the bespoke message below.
#[nativelink_test]
async fn production_sliding_window_message_contains_marker() {
    // Build a tiny streaming blob with a 4-byte sliding window. Send two
    // chunks larger than the window so the first chunk is evicted, then
    // try to read at the cursor (still pointed at chunk 0). The producer
    // path is the SAME as production (no failpoint, no synthetic
    // construction), so we exercise the actual `make_err!` site.
    let digest = DigestInfo::try_new(VALID_HASH, 32).unwrap();
    let (writer, mut reader) = StreamingBlob::new(digest, 4);

    // Push 2 × 8-byte chunks; the first chunk is evicted as the second
    // arrives because the running buffered total (16) exceeds
    // `max_buffer_bytes` (4) → sliding-window pop_front fires →
    // `earliest_chunk_idx` advances past the reader's cursor (which has
    // not yet read anything, so cursor=0).
    writer.send(Bytes::from(vec![0u8; 8])).await.unwrap();
    writer.send(Bytes::from(vec![1u8; 8])).await.unwrap();

    // Reader's cursor=0, earliest_chunk_idx > 0 → sliding-window
    // eviction trip. Production code path; no failpoint, no synthetic.
    let err = reader
        .next_chunk()
        .await
        .expect_err(
            "reader.next_chunk() must return Err when cursor < earliest_chunk_idx \
             (sliding-window eviction); test setup did not trigger the eviction path",
        );

    assert_eq!(
        err.code,
        Code::Unavailable,
        "sliding-window eviction must be Code::Unavailable so callers can retry"
    );

    let any_match = err
        .messages
        .iter()
        .any(|m| m.contains(SLIDING_WINDOW_EVICTION_MARKER));
    assert!(
        any_match,
        "production sliding-window error MUST contain the substring \
         SLIDING_WINDOW_EVICTION_MARKER ({:?}); the FastSlowStore D.1 \
         fallback predicate matches on this substring — a rename here \
         silently breaks the fallback. err.messages = {:?}",
        SLIDING_WINDOW_EVICTION_MARKER, err.messages,
    );

    // Pin the substring's literal bytes too so an accidental rename of
    // the constant itself (without touching the production message) is
    // caught at the source-of-truth.
    assert_eq!(
        SLIDING_WINDOW_EVICTION_MARKER, "reader fell behind sliding window",
        "SLIDING_WINDOW_EVICTION_MARKER's literal bytes are part of the \
         FastSlowStore D.1 cross-crate contract — coordinate any change \
         with the predicate at fast_slow_store.rs and the failpoint \
         predicate in fast_slow_store_325_regression_test.rs"
    );
}
