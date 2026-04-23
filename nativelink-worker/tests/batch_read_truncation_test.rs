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

//! Regression tests for the worker-side batch-read silent-truncation bug.
//!
//! `execute_batch_read` parses `BatchReadBlobsResponse.responses` and
//! commits each `data` blob to the worker's fast store under its claimed
//! digest, sized by `data.len()`. Pre-fix it filtered only by
//! `status.code != 0`, with no check that `data.len() == digest.size_bytes()`.
//! A truncated peer response (status_code = OK, `data` shorter than the
//! advertised digest size) silently committed corrupt content under the
//! correct hash key but with a wrong length, breaking subsequent reads
//! that trust the committed length and producing data corruption that
//! propagates through the worker fast store with no signal.
//!
//! The worker fast store is a `FilesystemStore` with NO `VerifyStore`
//! in front of it (verification on the worker side is wasted CPU; the
//! server-side store chain has the verifier). The batch-read path is
//! the trust boundary — it must validate response sizes itself.
//!
//! The fix in `validate_batch_read_responses` rejects any response whose
//! `data.len() != digest.size_bytes()` (warns and drops). The dropped
//! digest falls into the retry path and is fetched from the server
//! store chain, which has its own verification.

use bytes::Bytes;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsResponse, Digest as ProtoDigest, batch_read_blobs_response,
};
use nativelink_util::common::DigestInfo;
use nativelink_worker::running_actions_manager::validate_batch_read_responses;
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALID_HASH3: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";

/// Build a single response entry with the given digest and data.
fn make_resp(
    hash: &str,
    advertised_size: i64,
    data: Vec<u8>,
    status_code: i32,
) -> batch_read_blobs_response::Response {
    batch_read_blobs_response::Response {
        digest: Some(ProtoDigest {
            hash: hash.to_string(),
            size_bytes: advertised_size,
        }),
        data: Bytes::from(data),
        compressor: 0,
        status: Some(nativelink_proto::google::rpc::Status {
            code: status_code,
            message: String::new(),
            details: vec![],
        }),
    }
}

// -------------------------------------------------------------------
// 1. Truncated response (data.len() < digest.size_bytes()) is REJECTED
//
// Pre-fix: returned (digest, Bytes::from(short_data)) — committed.
// Post-fix: dropped from the result vec; digest left for retry path.
// -------------------------------------------------------------------
#[nativelink_test]
async fn truncated_response_is_rejected() -> Result<(), Error> {
    // 100-byte digest claimed, only 60 bytes returned.
    let response = BatchReadBlobsResponse {
        responses: vec![make_resp(VALID_HASH1, 100, vec![0xAB; 60], /* OK */ 0)],
    };

    let validated = validate_batch_read_responses(response.responses);

    assert!(
        validated.is_empty(),
        "truncated response (60 bytes for 100-byte digest) must be rejected; \
         got {validated:?}"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 2. Over-long response (data.len() > digest.size_bytes()) is REJECTED
//
// Symmetric to truncation — extra bytes also fail the length contract.
// -------------------------------------------------------------------
#[nativelink_test]
async fn over_long_response_is_rejected() -> Result<(), Error> {
    let response = BatchReadBlobsResponse {
        responses: vec![make_resp(VALID_HASH1, 100, vec![0xCD; 200], 0)],
    };

    let validated = validate_batch_read_responses(response.responses);

    assert!(
        validated.is_empty(),
        "over-long response (200 bytes for 100-byte digest) must be rejected; \
         got {validated:?}"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 3. Valid response with matching size is ACCEPTED
// -------------------------------------------------------------------
#[nativelink_test]
async fn matching_size_response_is_accepted() -> Result<(), Error> {
    let payload = vec![0xEF; 50];
    let response = BatchReadBlobsResponse {
        responses: vec![make_resp(VALID_HASH1, 50, payload.clone(), 0)],
    };

    let validated = validate_batch_read_responses(response.responses);

    assert_eq!(validated.len(), 1, "valid response should be accepted");
    let (digest, data) = &validated[0];
    let expected = DigestInfo::try_new(VALID_HASH1, 50)?;
    assert_eq!(digest, &expected);
    assert_eq!(data, &Bytes::from(payload));

    Ok(())
}

// -------------------------------------------------------------------
// 4. Mixed batch: truncated entry dropped, valid entries kept
// -------------------------------------------------------------------
#[nativelink_test]
async fn mixed_batch_drops_only_truncated_entries() -> Result<(), Error> {
    let response = BatchReadBlobsResponse {
        responses: vec![
            // Valid: 30 bytes / 30 bytes
            make_resp(VALID_HASH1, 30, vec![0x01; 30], 0),
            // Truncated: 50 bytes / 100 bytes
            make_resp(VALID_HASH2, 100, vec![0x02; 50], 0),
            // Valid: 200 bytes / 200 bytes
            make_resp(VALID_HASH3, 200, vec![0x03; 200], 0),
        ],
    };

    let validated = validate_batch_read_responses(response.responses);

    assert_eq!(
        validated.len(),
        2,
        "should keep the two valid entries and drop the truncated one"
    );
    let kept_digests: Vec<DigestInfo> = validated.iter().map(|(d, _)| *d).collect();
    let d1 = DigestInfo::try_new(VALID_HASH1, 30)?;
    let d3 = DigestInfo::try_new(VALID_HASH3, 200)?;
    assert!(kept_digests.contains(&d1), "d1 should be kept");
    assert!(kept_digests.contains(&d3), "d3 should be kept");
    let d2 = DigestInfo::try_new(VALID_HASH2, 100)?;
    assert!(!kept_digests.contains(&d2), "d2 (truncated) must be dropped");

    Ok(())
}

// -------------------------------------------------------------------
// 5. Non-OK status entries are dropped (existing behavior preserved)
// -------------------------------------------------------------------
#[nativelink_test]
async fn non_ok_status_entries_are_dropped() -> Result<(), Error> {
    let response = BatchReadBlobsResponse {
        responses: vec![
            // status_code 5 = NotFound — drop even though data length matches.
            make_resp(VALID_HASH1, 30, vec![0x01; 30], /* NotFound */ 5),
        ],
    };

    let validated = validate_batch_read_responses(response.responses);
    assert!(
        validated.is_empty(),
        "non-OK status entries must be dropped"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 6. Zero-length blob: data.len() == 0 == digest.size_bytes() → accepted
// -------------------------------------------------------------------
#[nativelink_test]
async fn zero_length_blob_with_empty_data_is_accepted() -> Result<(), Error> {
    let response = BatchReadBlobsResponse {
        responses: vec![make_resp(VALID_HASH1, 0, vec![], 0)],
    };

    let validated = validate_batch_read_responses(response.responses);
    assert_eq!(validated.len(), 1, "zero-length blob is valid");
    assert_eq!(validated[0].1.len(), 0);

    Ok(())
}

// -------------------------------------------------------------------
// 7. Zero-length advertised but non-empty data → REJECTED
//
// Edge case: server claims zero-length but ships data. Refuse.
// -------------------------------------------------------------------
#[nativelink_test]
async fn zero_length_blob_with_non_empty_data_is_rejected() -> Result<(), Error> {
    let response = BatchReadBlobsResponse {
        responses: vec![make_resp(VALID_HASH1, 0, vec![0xFF; 5], 0)],
    };

    let validated = validate_batch_read_responses(response.responses);
    assert!(
        validated.is_empty(),
        "size-zero digest with non-empty data is corruption — must be rejected"
    );

    Ok(())
}
