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

//! Worker-side handler test for `Update::BatchWriteSmallBlobs` (Bug A
//! small-CAS peer-mirror dispatcher; task #153).
//!
//! Covers the contract that the worker:
//!   1. Decodes each `SmallBlobEntry` proto digest into `DigestInfo`,
//!   2. Calls `insert_dispatched_mirror_blob(store_id, digest, data)` on
//!      the CAS server's FastSlowStore so the bytes land in
//!      `mirror_blobs`, and
//!   3. Skips entries whose `data.len() != digest.size_bytes()` (the
//!      load-bearing invariant from `fast_slow_store.rs:771-784`) with
//!      a warn.
//!
//! Mutate-test guidance: comment out the `insert_dispatched_mirror_blob`
//! call inside `handle_batch_write_small_blobs`; the
//! `mirror_blob_inserted_when_proto_received` test below MUST fail.

use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::SmallBlobEntry;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
use nativelink_worker::local_worker::handle_batch_write_small_blobs;
use pretty_assertions::assert_eq;

fn make_fss_for_mirror() -> Arc<FastSlowStore> {
    let _fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let _slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    )
}

fn make_digest(seed: u8, size: u64) -> DigestInfo {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    DigestInfo::new(bytes, size)
}

#[nativelink_test]
async fn mirror_blob_inserted_when_proto_received() {
    let fss = make_fss_for_mirror();
    let d = make_digest(1, 100);
    let payload = Bytes::from(vec![0xAA; 100]);

    let entries = vec![SmallBlobEntry {
        digest: Some(ProtoDigest::from(d)),
        data: payload.clone(),
        store_id: "cas".to_string(),
    }];

    handle_batch_write_small_blobs(Some(&fss), &entries);

    assert_eq!(
        fss.mirror_blob_count(),
        1,
        "single-entry batch MUST insert exactly one mirror blob; \
         got {} (handler did not call insert_dispatched_mirror_blob)",
        fss.mirror_blob_count(),
    );
    assert_eq!(
        fss.mirror_blobs_used_bytes(),
        100,
        "mirror_blobs accounting MUST track inserted bytes"
    );
}

#[nativelink_test]
async fn mirror_blob_skips_entries_with_size_mismatch() {
    let fss = make_fss_for_mirror();
    // Digest claims 100 bytes but data is only 50.
    let d_bad = make_digest(1, 100);
    let d_good = make_digest(2, 50);

    let entries = vec![
        SmallBlobEntry {
            digest: Some(ProtoDigest::from(d_bad)),
            data: Bytes::from(vec![0u8; 50]),
            store_id: "cas".to_string(),
        },
        SmallBlobEntry {
            digest: Some(ProtoDigest::from(d_good)),
            data: Bytes::from(vec![0u8; 50]),
            store_id: "cas".to_string(),
        },
    ];

    handle_batch_write_small_blobs(Some(&fss), &entries);

    assert_eq!(
        fss.mirror_blob_count(),
        1,
        "size-mismatched entry MUST be skipped; only the well-formed \
         entry should land in mirror_blobs (got {} entries)",
        fss.mirror_blob_count(),
    );
}

#[nativelink_test]
async fn mirror_blob_handler_noop_when_no_fss() {
    // Worker without a CAS server (no cas_server_fss) — handler MUST
    // tolerate None gracefully and not panic.
    let d = make_digest(1, 50);
    let entries = vec![SmallBlobEntry {
        digest: Some(ProtoDigest::from(d)),
        data: Bytes::from(vec![0u8; 50]),
        store_id: "cas".to_string(),
    }];
    handle_batch_write_small_blobs(None, &entries);
    // No assertion beyond "did not panic"; the warn is observable in
    // the captured test output.
}

#[nativelink_test]
async fn mirror_blob_handler_skips_entries_with_missing_digest() {
    let fss = make_fss_for_mirror();
    let entries = vec![SmallBlobEntry {
        // No digest — handler MUST skip + warn, not insert a phantom.
        digest: None,
        data: Bytes::from(vec![0u8; 50]),
        store_id: "cas".to_string(),
    }];
    handle_batch_write_small_blobs(Some(&fss), &entries);
    assert_eq!(
        fss.mirror_blob_count(),
        0,
        "missing-digest entry MUST be skipped"
    );
}

/// #168 testing-czar MINOR-2: handler MUST reject entries whose
/// `store_id` does not match the dispatcher's regex
/// (`[a-zA-Z_][a-zA-Z0-9_]*`). A malformed `store_id` from a buggy or
/// untrusted server would (a) leak unbounded keys into the worker's
/// `dispatched_mirror_pins` BTreeMap, and (b) propagate to the wire
/// ack (`BlobsAvailableNotification.pinned_mirror_entries`), where
/// the server's `is_valid_store_id`-keyed pin-set lookup would
/// silently fail — creating an unbounded server-side memory leak.
///
/// Mutation step: remove the `is_valid_store_id` check at
/// `local_worker.rs:743` → this test red-fails because the handler
/// inserts a phantom entry.
#[nativelink_test]
async fn handler_skips_entries_with_invalid_store_id() {
    let fss = make_fss_for_mirror();
    let d_bad1 = make_digest(1, 50);
    let d_bad2 = make_digest(2, 50);
    let d_bad3 = make_digest(3, 50);
    let d_bad4 = make_digest(4, 50);
    let d_good = make_digest(5, 50);

    // Mix one well-formed entry between bad ones to assert
    // partial-batch acceptance (other valid entries in the same
    // batch MUST land).
    let entries = vec![
        SmallBlobEntry {
            digest: Some(ProtoDigest::from(d_bad1)),
            data: Bytes::from(vec![0u8; 50]),
            store_id: String::new(),
        },
        SmallBlobEntry {
            digest: Some(ProtoDigest::from(d_bad2)),
            data: Bytes::from(vec![0u8; 50]),
            store_id: "cas-bad".to_string(),
        },
        SmallBlobEntry {
            digest: Some(ProtoDigest::from(d_good)),
            data: Bytes::from(vec![0u8; 50]),
            store_id: "cas".to_string(),
        },
        SmallBlobEntry {
            digest: Some(ProtoDigest::from(d_bad3)),
            data: Bytes::from(vec![0u8; 50]),
            store_id: "cas store".to_string(),
        },
        SmallBlobEntry {
            digest: Some(ProtoDigest::from(d_bad4)),
            data: Bytes::from(vec![0u8; 50]),
            store_id: "$cas".to_string(),
        },
    ];

    handle_batch_write_small_blobs(Some(&fss), &entries);

    assert_eq!(
        fss.mirror_blob_count(),
        1,
        "#168 testing-czar MINOR-2: handler MUST reject entries with \
         invalid store_id (per regex `[a-zA-Z_][a-zA-Z0-9_]*`); only the \
         well-formed `cas` entry should land. Got {} mirror blobs.",
        fss.mirror_blob_count(),
    );
}
