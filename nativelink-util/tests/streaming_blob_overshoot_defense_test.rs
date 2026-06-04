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

//! #44 symmetric overshoot defense — regression tests for the pipeline-2487
//! 2026-06-03 17:13:41 UTC inflation shape (Read returned N + Δ bytes whose
//! hash != X). Builds on the #49 `expected_size_on_store` infrastructure
//! shipped 2026-06-04 at `e5adb473`.
//!
//! Three orthogonal layers compose to enforce
//! `streaming-blob-read-bytes ≤ expected_size_on_store_or_digest`:
//!
//! * Layer A — `StreamingBlobWriter::send` rejects any chunk that would
//!   push `bytes_written` past the expected upper bound. PRIMARY load-
//!   bearing defense; the over-bytes never enter the buffer.
//! * Layer B — `StreamingBlobReader::next_chunk` surfaces an error if
//!   `bytes_written > expected_size_on_store_or_digest` somehow occurred
//!   (catches Layer A bypass via direct-inner mutation).
//! * Layer C — `bytestream_server.rs:inner_read` unfold caps the emitted
//!   byte total at `digest.size_bytes()`. Defends Bazel's parallel-chunk
//!   `Read(offset=N, limit=0)` shape independently of the streaming-blob
//!   primitive's contents.
//!
//! Tests T1, T2, T4 exercise A and B; T3 lives next to the server-side
//! seam in `nativelink-service` (see
//! `nativelink-service/tests/bytestream_server_inner_read_overshoot_cap_test.rs`).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use nativelink_macro::nativelink_test;
use nativelink_util::common::DigestInfo;
use nativelink_util::streaming_blob::{
    STREAMING_BLOB_SILENT_OVERSHOOT_MARKER, StreamingBlobInner, StreamingBlobReader,
    StreamingBlobWriter,
};

/// `DigestInfo` with `size_bytes = size` for tests where the digest-size
/// bound matters (Layer A falls back to `digest.size_bytes()` when
/// `expected_size_on_store` is unset; tests for AC-style mismatches set
/// the expected size explicitly via `set_expected_size_on_store`).
fn digest_with_size(seed_byte: u8, size: u64) -> DigestInfo {
    let mut hash = [0u8; 32];
    hash[0] = seed_byte;
    DigestInfo::new(hash, size)
}

/// Distinguishable byte payload — repeated `byte` of length `len`.
fn payload(byte: u8, len: usize) -> Bytes {
    Bytes::from(vec![byte; len])
}

// =====================================================================
// T1 — writer admission cap (Layer A)
// =====================================================================

/// `StreamingBlobWriter::send` MUST reject a chunk whose append would
/// push `bytes_written` past `expected_size_on_store_or_digest`. The
/// rejected chunk MUST NOT be buffered, so concurrent readers cannot
/// observe the over-bytes even via a fresh `StreamingBlobReader`. Error
/// MUST carry the `STREAMING_BLOB_SILENT_OVERSHOOT_MARKER` substring for
/// journal-grep stability (mirrors #502's `silent_short` shape).
///
/// Production composition: real `StreamingBlobInner` + real
/// `StreamingBlobWriter`, with `expected_size_on_store` set via the
/// same setter `FastSlowStore::run_producer` calls in production
/// (`fast_slow_store.rs:3573`).
///
/// Mutation: comment out the admission check in
/// `StreamingBlobWriter::send`; this test must red-fail with the
/// bespoke message "silent_overshoot defense fired but assertion
/// against missing-rejection passed (overshoot accepted)".
#[nativelink_test]
async fn writer_rejects_overshoot_past_expected_size() {
    const EXPECTED: u64 = 1024;
    let digest = digest_with_size(0xC0, EXPECTED);
    let inner = Arc::new(StreamingBlobInner::new(digest, 1024 * 1024));
    inner.set_expected_size_on_store(EXPECTED);
    let writer = StreamingBlobWriter::new(Arc::clone(&inner));

    let body = async {
        // Fill the buffer to exactly EXPECTED — must succeed.
        writer
            .send(payload(0xC0, EXPECTED as usize))
            .await
            .expect("baseline send up to expected_size must succeed");
        assert_eq!(
            inner.bytes_written_for_test(),
            EXPECTED,
            "post-baseline-send bytes_written must equal expected_size"
        );

        // Now send a single byte beyond the cap. Layer A must REJECT.
        let res = writer.send(payload(0xDE, 1)).await;
        let err = match res {
            Ok(()) => panic!(
                "silent_overshoot defense fired but assertion against \
                 missing-rejection passed (overshoot accepted) — Layer A \
                 admission cap missing; bytes_written climbed from {} \
                 with EXPECTED={EXPECTED}",
                inner.bytes_written_for_test()
            ),
            Err(e) => e,
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains(STREAMING_BLOB_SILENT_OVERSHOOT_MARKER),
            "writer.send Err must include {STREAMING_BLOB_SILENT_OVERSHOOT_MARKER} \
             substring — got {msg}"
        );
        assert!(
            msg.contains("expected_size=1024"),
            "writer.send Err must name the expected upper bound — got {msg}"
        );
        // Crucially: bytes_written did NOT advance. Over-bytes never landed in
        // the buffer.
        assert_eq!(
            inner.bytes_written_for_test(),
            EXPECTED,
            "rejected over-send must NOT advance bytes_written"
        );
    };

    tokio::time::timeout(Duration::from_secs(5), body)
        .await
        .expect("must not deadlock — Layer A admission cap is synchronous");

    // MUTATION VERIFIED (2026-06-04): comment out the
    // `would_overshoot` branch in `StreamingBlobWriter::send` →
    // red-fail with bespoke "silent_overshoot defense fired but
    // assertion against missing-rejection passed (overshoot
    // accepted)"; reverted, green again.
}

// =====================================================================
// T2 — reader emission cap (Layer B, asymmetric over-action)
// =====================================================================

/// `StreamingBlobReader::next_chunk` MUST surface
/// `STREAMING_BLOB_SILENT_OVERSHOOT_MARKER` if it observes
/// `bytes_written > expected_size_on_store_or_digest`. Reproduces the
/// "Layer A was bypassed" failure mode by mutating
/// `bytes_written` directly via the test-only setter — production
/// callers go through `StreamingBlobWriter::send`, which Layer A
/// rejects, but a future regression or alternate producer that pushed
/// past the cap must still surface to readers as an error rather than
/// serving the over-bytes byte-for-byte (which is the pipeline-2487
/// shape).
///
/// Mutation: comment out the `bytes_written > expected_size` check at
/// the top of `next_chunk`'s loop; this test must red-fail with
/// bespoke "Layer B emission cap missing — reader served bytes past
/// expected_size without surfacing silent_overshoot".
#[nativelink_test]
async fn reader_silent_overshoot_marker_fires_on_bypass() {
    const EXPECTED: u64 = 1024;
    const FORGED_BYTES_WRITTEN: u64 = 1100;
    let digest = digest_with_size(0xC1, EXPECTED);
    let inner = Arc::new(StreamingBlobInner::new(digest, 1024 * 1024));
    inner.set_expected_size_on_store(EXPECTED);
    // Simulate a Layer-A bypass: bytes_written advanced past expected_size
    // without going through `send` (e.g. an alternate producer wrote to
    // the atomic directly, or a hypothetical future caller skipped Layer
    // A). The reader must NOT serve any chunks — it must surface the
    // overshoot.
    inner.bytes_written_for_test_set(FORGED_BYTES_WRITTEN);

    let mut reader = StreamingBlobReader::new(Arc::clone(&inner));
    let body = async {
        let res = reader.next_chunk().await;
        let err = match res {
            Ok(b) => panic!(
                "Layer B emission cap missing — reader served bytes past \
                 expected_size without surfacing silent_overshoot (got \
                 Ok({} bytes); inner.bytes_written={FORGED_BYTES_WRITTEN}, \
                 expected_size={EXPECTED})",
                b.len()
            ),
            Err(e) => e,
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains(STREAMING_BLOB_SILENT_OVERSHOOT_MARKER),
            "Layer B Err must include {STREAMING_BLOB_SILENT_OVERSHOOT_MARKER} \
             substring — got {msg}"
        );
    };
    tokio::time::timeout(Duration::from_secs(5), body)
        .await
        .expect("must not deadlock — Layer B is a synchronous check");

    // MUTATION VERIFIED (2026-06-04): comment out the
    // `bytes_written > expected_size` overshoot branch at the top
    // of `next_chunk`'s loop → red-fail with bespoke "must not
    // deadlock — Layer B is a synchronous check" panic via the
    // 5-second tokio::time::timeout deadline detector (with the
    // cap removed and no chunks buffered, the reader parks on
    // notify_rx.changed() forever, which the timeout converts
    // into the bespoke red-fail). Reverted, green again.
}

// =====================================================================
// T4 — normal CAS write completes (negative test: no false-fire)
// =====================================================================

/// A producer that sends EXACTLY `expected_size` bytes and EOFs must
/// not trigger Layer A or B. Drains cleanly to `Ok(Bytes::new())`. This
/// is the steady-state contract: the defense must not interfere with
/// the dominant case.
///
/// Production composition: writer + reader over the same
/// `StreamingBlobInner`, chunked send (multiple `send` calls totaling
/// `expected_size`), EOF, drain to EOF.
#[nativelink_test]
async fn cas_normal_write_still_completes_under_overshoot_defense() {
    const TOTAL: u64 = 1024 * 1024;
    const CHUNK: usize = 64 * 1024;
    let digest = digest_with_size(0xC2, TOTAL);
    let inner = Arc::new(StreamingBlobInner::new(digest, 2 * 1024 * 1024));
    inner.set_expected_size_on_store(TOTAL);

    let writer_arc = Arc::clone(&inner);
    let reader = StreamingBlobReader::new(Arc::clone(&inner));

    let body = async move {
        let producer = tokio::spawn(async move {
            let mut writer = StreamingBlobWriter::new(writer_arc);
            let mut sent: u64 = 0;
            while sent < TOTAL {
                let n = core::cmp::min(CHUNK as u64, TOTAL - sent) as usize;
                writer
                    .send(payload(0xC2, n))
                    .await
                    .expect("steady-state send must succeed");
                sent += n as u64;
            }
            writer.send_eof().expect("send_eof on full payload must succeed");
        });

        let mut reader = reader;
        let mut got: u64 = 0;
        loop {
            let chunk = reader
                .next_chunk()
                .await
                .expect("steady-state reader must not surface overshoot");
            if chunk.is_empty() {
                break;
            }
            got += chunk.len() as u64;
        }
        producer.await.expect("producer task must complete cleanly");
        assert_eq!(
            got, TOTAL,
            "steady-state drain must yield exactly expected_size bytes"
        );
    };
    tokio::time::timeout(Duration::from_secs(10), body)
        .await
        .expect("steady-state composition must not deadlock");
}

// Helper trait extension: bytes_written load + test-only mutator on Inner.
// Production reads via `bytes_written.load(Ordering::Acquire)` at the
// reader/writer sites; for tests we need both a load and a forge.
//
// The test-only accessors live on `StreamingBlobInner` itself behind
// `#[cfg(any(test, feature = "test-utils"))]` (see streaming_blob.rs).
trait BytesWrittenAccess {
    fn bytes_written_for_test(&self) -> u64;
    fn bytes_written_for_test_set(&self, v: u64);
}

impl BytesWrittenAccess for StreamingBlobInner {
    fn bytes_written_for_test(&self) -> u64 {
        self.bytes_written_atomic().load(Ordering::Acquire)
    }
    fn bytes_written_for_test_set(&self, v: u64) {
        self.bytes_written_atomic().store(v, Ordering::Release);
    }
}
