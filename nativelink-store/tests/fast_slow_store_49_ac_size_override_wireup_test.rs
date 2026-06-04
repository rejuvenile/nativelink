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

//! #49 production-composition test for the `FastSlowStore::run_producer` →
//! `StreamingBlobInner` wire-up that records the bytes-on-store size for
//! the #502 silent-short check.
//!
//! The streaming_blob.rs unit tests
//! (`ac_read_with_size_override_does_not_trip_silent_short` and siblings)
//! cover the `StreamingBlobInner` primitive in isolation: they call
//! `set_expected_size_on_store` directly on a hand-built `StreamingBlobInner`.
//! They do NOT cross the seam at `fast_slow_store.rs:3562` where the
//! producer reads `head_result` from `slow_store.has()` and plumbs it
//! into the streaming writer. Commenting out the `:3562` call leaves all
//! three unit tests green — the wire-up itself is unverified.
//!
//! This test composes the production CAS-chain shape against
//! `FastSlowStore::get_part` so the producer at `:3423` runs end-to-end
//! and the consumer (the caller's writer) observes the post-fix behavior:
//!  - Slow tier: `MemoryStore` with bytes inserted via `update(... MaxSize)`
//!    so the stored size (207 bytes) is LESS than the digest's declared
//!    `size_bytes()` (217). This mirrors the production AC case where the
//!    Action proto digest declares one size and the ActionResult bytes
//!    stored under that key are shorter.
//!  - Fast tier: empty `MemoryStore`.
//!  - `FastSlowStore::get_part(digest, writer, 0, None)` drives the
//!    populator at `fast_slow_store.rs:3423`:
//!     1. `head_result = slow.has(digest)` → `Ok(ExactSize(207))`.
//!     2. At `:3561-3563`, the producer calls
//!        `streaming_writer.set_expected_size_on_store(207)`.
//!     3. The body fetch sends 207 bytes through the streaming buffer.
//!     4. Reader's terminal poll compares `bytes_written (207) ==
//!        expected_size_on_store (207)` → no silent_short.
//!
//! Mutation: comment out `fast_slow_store.rs:3562` (the
//! `set_expected_size_on_store(n)` call inside the `ExactSize` arm). The
//! `OnceLock` stays unset → `expected_size_on_store()` falls back to
//! `digest.size_bytes() = 217` → the silent-short check fires because
//! `bytes_written = 207 < expected_size = 217` → `get_part` returns
//! `Err(Internal { messages: ["streaming_blob_silent_short: ..."] })`.
//!
//! Asserts via the bespoke `STREAMING_BLOB_SILENT_SHORT_MARKER` substring
//! so a future panic on this site greps to #49 / #502.

use core::time::Duration;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use nativelink_util::streaming_blob::STREAMING_BLOB_SILENT_SHORT_MARKER;

const VALID_HASH: &str =
    "9797979797979797979797979797979797979797979797979797979797979797";

/// Production observed values from 2026-06-04 05:55:15.290 PDT:
/// 217-byte action_digest whose stored ActionResult encoded to 207 bytes.
const ACTION_DIGEST_SIZE: u64 = 217;
const ACTION_RESULT_BYTES: u64 = 207;

/// Production-composition regression test for #49: `FastSlowStore::get_part`
/// against an AC-shaped asymmetry (digest.size_bytes != stored bytes)
/// MUST NOT trip the #502 silent-short check.
///
/// MUTATION VERIFIED (2026-06-04): commented out the body of
/// `fast_slow_store.rs:3561-3563` (`if let UploadSizeInfo::ExactSize(n) =
/// reader_stream_size { streaming_writer.set_expected_size_on_store(n); }`
/// → bind to `_n` + comment the call). The `OnceLock` stayed unset →
/// `expected_size_on_store()` accessor fell back to `digest.size_bytes()
/// = 217` → silent-short check fired because `bytes_written = 207 <
/// expected_size = 217` → this assertion fired with bespoke "FSS
/// wire-up broken: silent_short fired (bytes_written=207 <
/// expected_size=217) — set_expected_size_on_store not called at
/// fast_slow_store.rs:3562 ... drain_err = Some(\"Error { code:
/// Internal, messages: [\\\"streaming_blob_silent_short: terminal=Ok
/// but bytes_written=207 < expected_size=217 for digest 9797…-217\\\"]
/// }\")". Reverting the mutation restored green.
#[nativelink_test]
async fn ac_read_via_fast_slow_store_with_size_mismatch_does_not_trip_silent_short()
-> Result<(), Error> {
    let digest = DigestInfo::try_new(VALID_HASH, ACTION_DIGEST_SIZE).unwrap();

    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));

    // Insert ActionResult-shaped bytes (207) under the action_digest
    // (declared size 217) via `update` with `MaxSize` — this is the only
    // way to land an under-declared blob in MemoryStore without tripping
    // its `ExactSize` mismatch guard. Production AC slow tier is Redis,
    // which has no equivalent declared-size check and accepts whatever
    // bytes the producer writes; we simulate that asymmetry here.
    {
        let action_result_bytes = Bytes::from(vec![0xacu8; ACTION_RESULT_BYTES as usize]);
        let (mut tx, rx) = make_buf_channel_pair();
        let send_fut = async move {
            tx.send(action_result_bytes)
                .await
                .err_tip(|| "send ActionResult bytes")?;
            tx.send_eof().err_tip(|| "send EOF")?;
            Ok::<_, Error>(())
        };
        let update_fut = slow.update(
            digest,
            rx,
            UploadSizeInfo::MaxSize(ACTION_DIGEST_SIZE),
        );
        tokio::try_join!(send_fut, update_fut)?;
    }

    // Verify the slow-tier `has()` reports the STORED size, not the
    // declared digest size — this is the asymmetric value that the
    // producer at `fast_slow_store.rs:3472-3497` reads into
    // `UploadSizeInfo::ExactSize(207)` and plumbs into the streaming
    // writer at `:3562`.
    let slow_has = slow.has(digest).await?;
    assert_eq!(
        slow_has,
        Some(ACTION_RESULT_BYTES),
        "test fixture invariant: slow_store.has(digest_217) must return \
         Some(207) so the producer plumbs ExactSize(207) into \
         set_expected_size_on_store at fast_slow_store.rs:3562; if this \
         assertion fires, the test is exercising the wrong path",
    );

    // Production CAS chain seam: ExistenceCache → Verify → FastSlowStore.
    // For #49 the load-bearing seam is FastSlowStore::get_part →
    // run_producer → set_expected_size_on_store. The wrappers above
    // don't carry the size-on-store signal — testing the FastSlowStore
    // boundary directly is sufficient to verify the wire-up.
    let fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast,
        slow,
    ));

    // Read via `FastSlowStore::get_part(digest, writer, 0, None)` — the
    // fast tier is empty so this drives the populator path through
    // `run_producer` at `:3423`. The producer reads
    // `head_result = slow.has(digest) → Ok(ExactSize(207))`, then at
    // `:3561-3563` records `set_expected_size_on_store(207)` on the
    // streaming writer.
    let (writer, mut reader) = make_buf_channel_pair();
    let get_fut = async move {
        fast_slow
            .get_part(digest, writer, 0, None)
            .await
            .err_tip(|| "get_part via FastSlowStore on AC-shaped mismatch")
    };
    let drain_fut = async move {
        let mut total: u64 = 0;
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            total += chunk.len() as u64;
        }
        Ok::<u64, Error>(total)
    };

    // 5-second deadlock detector. Bespoke `.expect(...)` so a future
    // panic on this site greps to #49 production-composition wire-up.
    let (get_res, drain_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async move { tokio::join!(get_fut, drain_fut) },
    )
    .await
    .expect(
        "FSS get_part must not deadlock — production-composition \
         silent_short wire test for #49: AC read with declared \
         size_bytes (217) != stored bytes (207) should complete \
         within 5s; if this panic fires, the bug is composition/wiring \
         (not the silent-zero class)",
    );

    // Classify both seam errors against the bespoke silent-short marker.
    // With the BUG (line 3562 commented out), the producer terminates Ok
    // but the consumer's `StreamingBlobReader::next_chunk` terminal poll
    // synthesizes a Code::Internal carrying
    // `STREAMING_BLOB_SILENT_SHORT_MARKER`. In `FastSlowStore::get_part`
    // streaming-populate composition the Err can surface on EITHER seam:
    //   (a) `drain_res` — the reader's `recv()` returns Err when the
    //       streaming-buffer terminal is Err and we're past the buffered
    //       chunks; this is what the production-composition stack
    //       (bytestream tx → WorkerProxyStore → ... → FastSlowStore)
    //       actually observes.
    //   (b) `get_res` — same Err re-surfaces through `get_part`'s
    //       terminal merge.
    // Either-or classification: if EITHER carries the marker, the wire-up
    // is broken; assert green only when BOTH are clean.
    let drain_err_msg = drain_res.as_ref().err().map(|e| format!("{e:?}"));
    let get_err_msg = get_res.as_ref().err().map(|e| format!("{e:?}"));

    let either_has_marker = drain_err_msg
        .as_deref()
        .is_some_and(|m| m.contains(STREAMING_BLOB_SILENT_SHORT_MARKER))
        || get_err_msg
            .as_deref()
            .is_some_and(|m| m.contains(STREAMING_BLOB_SILENT_SHORT_MARKER));

    assert!(
        !either_has_marker,
        "FSS wire-up broken: silent_short fired (bytes_written=207 < \
         expected_size=217) — set_expected_size_on_store not called at \
         fast_slow_store.rs:3562. The producer's ExactSize(207) from \
         slow.has() never reached the streaming writer's OnceLock, so the \
         #502 check at StreamingBlobReader::next_chunk fell back to \
         digest.size_bytes()=217.\n\
         drain_err = {drain_err_msg:?}\n\
         get_err = {get_err_msg:?}",
    );

    // Composition broke for an unrelated reason.
    let bytes_drained = drain_res.expect(
        "reader drain returned Err with no silent_short marker — \
         composition broke between writer and reader for a DIFFERENT \
         reason (NOT the #49 silent-short class)",
    );
    get_res.expect(
        "get_part returned Err with no silent_short marker — \
         composition broke at the FSS terminal merge for a DIFFERENT \
         reason (NOT the #49 silent-short class)",
    );

    // Full ActionResult bytes were delivered.
    assert_eq!(
        bytes_drained, ACTION_RESULT_BYTES,
        "FSS get_part delivered {bytes_drained} bytes; expected the full \
         {ACTION_RESULT_BYTES} ActionResult bytes (the on-store size). \
         If short, the producer terminated early (NOT the #49 silent-short \
         class — would be a different regression in run_producer's body \
         fetch).",
    );

    Ok(())
}
