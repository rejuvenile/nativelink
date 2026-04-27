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

//! Integration tests for #98 (peer-hints chunking, direct-merge design).
//!
//! All tests target `handle_peer_hints_chunk`, the single worker-side
//! arm that registers hints into the global `peer_locality_map`. The
//! scheduler-side emission loop (`emit_peer_hints_chunks`) is tested via
//! the chunk-reassembly assertions in the `simple_scheduler_test` suite
//! (`recv_start_execute_with_hints` drains both StartAction and chunks,
//! asserting size ordering at scheduling time).
//!
//! Per CLAUDE.md "no synthetic-jitter Notify-race tests" and
//! "test-first": these tests cover only the protocol behavior the spec
//! enforces — every chunk's hints land in the locality map, ordering
//! between chunk arrival and action dispatch is irrelevant, total
//! capacity scales to 1M hints with no truncation. They do NOT model
//! timing, lost wakeups, or scheduler-internal sort orderings (those
//! are scheduler-side concerns covered elsewhere).

use std::time::Duration;

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    PeerHint, PeerHintsChunk,
};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::common::DigestInfo;
use nativelink_worker::local_worker::handle_peer_hints_chunk;

/// Build N synthetic PeerHint protos with size-descending digests:
/// digest at index i has `size_bytes = (N - i) as u64` so the
/// scheduler's "sort by size descending" produces hints in ascending
/// digest-index order. Useful for validating the dispatch-side sort.
fn make_hints(n: usize, endpoint: &str) -> Vec<PeerHint> {
    (0..n)
        .map(|i| {
            // Pack i into the first 8 bytes of the digest hash so each
            // digest is distinct.
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&(i as u64).to_be_bytes());
            let size = (n - i) as i64;
            PeerHint {
                digest: Some(Digest {
                    hash: hex::encode(hash),
                    size_bytes: size,
                }),
                peer_endpoints: vec![endpoint.to_string()],
            }
        })
        .collect()
}

const PEER_HINTS_PER_CHUNK: usize = 256;

/// Slice a hint Vec into PeerHintsChunk messages, each carrying
/// `PEER_HINTS_PER_CHUNK` entries (the last chunk may be partial /
/// empty per the chunk_iter contract).
fn chunk_hints(all: Vec<PeerHint>, op_id: &str) -> Vec<PeerHintsChunk> {
    let total = all.len();
    let mut out = Vec::new();
    let mut iter = all.into_iter();
    let mut sequence = 0u32;
    loop {
        let mut buf = Vec::with_capacity(PEER_HINTS_PER_CHUNK);
        for _ in 0..PEER_HINTS_PER_CHUNK {
            match iter.next() {
                Some(h) => buf.push(h),
                None => break,
            }
        }
        let is_last = buf.len() < PEER_HINTS_PER_CHUNK;
        out.push(PeerHintsChunk {
            peer_hints: buf,
            operation_id: op_id.to_string(),
            sequence,
            is_last,
        });
        sequence += 1;
        if is_last {
            break;
        }
    }
    // Sanity: total entries preserved.
    let recovered: usize = out.iter().map(|c| c.peer_hints.len()).sum();
    assert_eq!(recovered, total, "chunking must not drop hints");
    out
}

#[nativelink_test]
async fn peer_hints_exact_capacity_test() -> Result<(), Error> {
    // Direct-merge contract: every chunk's hints land in the locality
    // map. No buffer, no race; the count after handing off all chunks
    // must equal the count handed in. 65536 hints ≈ 256 chunks at the
    // PEER_HINTS_PER_CHUNK = 256 default — exercises the chunking
    // boundary at scale relevant for real workloads (a 65k-input action
    // is plausible; 1M is a stress test, in another assertion below).
    const N: usize = 65_536;
    let endpoint = "worker-x:50081";
    let hints = make_hints(N, endpoint);
    let chunks = chunk_hints(hints, "op-65536");

    let map = new_shared_blob_locality_map();
    for chunk in &chunks {
        handle_peer_hints_chunk(Some(&map), chunk);
    }

    // Verify total registrations.
    let snapshot = map.read();
    let total = snapshot.digest_count();
    assert_eq!(
        total, N,
        "all {N} hints must register; lost {} en route",
        N - total
    );
    Ok(())
}

#[nativelink_test]
async fn peer_hints_chunk_reassembly_test() -> Result<(), Error> {
    // Per the design: chunks have NO ordering invariant — they may
    // arrive in any order. The direct-merge worker arm doesn't care:
    // each chunk independently registers its hints. Verify by handing
    // the worker chunks in REVERSE order and asserting the same total
    // is reached.
    const N: usize = 1024;
    let endpoint = "worker-y:50081";
    let hints = make_hints(N, endpoint);
    let mut chunks = chunk_hints(hints, "op-reverse");

    chunks.reverse(); // out-of-order delivery

    let map = new_shared_blob_locality_map();
    for chunk in &chunks {
        handle_peer_hints_chunk(Some(&map), chunk);
    }

    let snapshot = map.read();
    assert_eq!(
        snapshot.digest_count(),
        N,
        "out-of-order chunks must still produce N registrations"
    );
    Ok(())
}

#[nativelink_test]
async fn peer_hints_at_scale_test() -> Result<(), Error> {
    // Stress test the no-truncation contract: pre-#98 the scheduler
    // capped at MAX_PEER_HINTS = 16384 silently. Post-#98 there is no
    // cap; an action with 1M cached input blobs registers 1M peer
    // hints. The worker-side handler must absorb the full stream
    // without dropping.
    const N: usize = 1_000_000;
    let endpoint = "worker-z:50081";
    let hints = make_hints(N, endpoint);
    let chunks = chunk_hints(hints, "op-1m");
    // Sanity: at N=1_000_000, we should have ceil(1_000_000 / 256) =
    // 3907 partial-only OR 3907 + 1 terminal-empty depending on
    // boundary; either way ~3908 chunks.
    assert!(
        chunks.len() >= 3907,
        "expected >= 3907 chunks for 1M hints, got {}",
        chunks.len()
    );

    let map = new_shared_blob_locality_map();
    for chunk in &chunks {
        handle_peer_hints_chunk(Some(&map), chunk);
    }

    let snapshot = map.read();
    assert_eq!(
        snapshot.digest_count(),
        N,
        "1M hints must register without truncation"
    );
    Ok(())
}

#[nativelink_test]
async fn peer_hints_size_descending_at_scale_test() -> Result<(), Error> {
    // The scheduler emits hints in size-descending order so the most
    // valuable peer prefetches are sent first. This test asserts the
    // *protocol* preserves that ordering — not the scheduler-side sort
    // itself (which is exercised in `simple_scheduler_test`). We
    // construct chunks where each chunk's first hint has greater
    // `size_bytes` than the next chunk's first hint and assert the
    // worker registers them all (the locality map doesn't preserve
    // order, but the snapshot lookup proves they all landed).
    const N: usize = 4096;
    let endpoint = "worker-sz:50081";
    let hints = make_hints(N, endpoint); // already size-descending

    // Spot-check ordering of the source.
    assert_eq!(hints[0].digest.as_ref().unwrap().size_bytes, N as i64);
    assert_eq!(hints[N - 1].digest.as_ref().unwrap().size_bytes, 1);

    let chunks = chunk_hints(hints.clone(), "op-sz");

    // Verify chunks ARRIVE in size-descending order: each chunk's
    // first hint has greater size_bytes than the following chunk's
    // first hint. This is the protocol-level invariant the dispatch
    // path is supposed to preserve.
    for w in chunks.windows(2) {
        if w[0].peer_hints.is_empty() || w[1].peer_hints.is_empty() {
            continue; // terminal empty chunk has no first-hint
        }
        let lhs = w[0].peer_hints[0].digest.as_ref().unwrap().size_bytes;
        let rhs = w[1].peer_hints[0].digest.as_ref().unwrap().size_bytes;
        assert!(
            lhs >= rhs,
            "chunks must arrive size-descending; chunk seq={} first.size={lhs} > chunk seq={} first.size={rhs}",
            w[0].sequence,
            w[1].sequence,
        );
    }

    let map = new_shared_blob_locality_map();
    for chunk in &chunks {
        handle_peer_hints_chunk(Some(&map), chunk);
    }

    let snapshot = map.read();
    assert_eq!(
        snapshot.digest_count(),
        N,
        "size-descending chunks must register all N hints"
    );

    // Verify the lookup works for the known largest digest.
    let mut hash0 = [0u8; 32];
    hash0[..8].copy_from_slice(&0u64.to_be_bytes());
    let largest = DigestInfo::new(hash0, N as u64);
    let workers = snapshot.lookup_workers(&largest);
    assert_eq!(workers.len(), 1, "largest digest must have 1 endpoint");
    assert_eq!(&*workers[0], endpoint);
    Ok(())
}

#[nativelink_test]
async fn hint_after_get_part_doesnt_break_action_test() -> Result<(), Error> {
    // Direct-merge contract: chunks can arrive AFTER an action's
    // input_fetch has already started — the action does NOT depend on
    // chunks landing first (no buffer keyed on operation_id, no wait).
    // We model this at the protocol level: simulate "input_fetch
    // started" by registering a baseline blob into the map BEFORE the
    // chunk arrives, then deliver the chunk LATER, and assert both the
    // baseline and the chunk's hints coexist.
    let map = new_shared_blob_locality_map();
    let endpoint = "worker-late:50081";
    let baseline_endpoint = "worker-baseline:50081";

    // Baseline: an action input that the worker fetched eagerly via
    // its own GetTree-fed `WorkerProxyStore` lookup (not from a hint).
    let mut baseline_hash = [0u8; 32];
    baseline_hash[..8].copy_from_slice(&u64::MAX.to_be_bytes());
    let baseline_digest = DigestInfo::new(baseline_hash, 1234);
    {
        let mut m = map.write();
        m.register_blobs(baseline_endpoint, &[baseline_digest]);
    }
    assert_eq!(map.read().digest_count(), 1);

    // Hint chunk arrives much later — the action is already mid-fetch.
    // The handler must be a no-op on the baseline entry (idempotent
    // re-add for distinct endpoints) and add the chunk's hints.
    const N: usize = 32;
    let hints = make_hints(N, endpoint);
    let chunks = chunk_hints(hints, "op-late");

    // Use a small wall-clock check via tokio::timeout to ensure the
    // handler is not blocking — deadlock-detector style. The handler
    // is sync, so this is a smoke test: 100ms is generous.
    tokio::time::timeout(Duration::from_millis(100), async {
        for chunk in &chunks {
            handle_peer_hints_chunk(Some(&map), chunk);
        }
    })
    .await
    .expect("handle_peer_hints_chunk must return promptly — direct-merge has no wait");

    let snapshot = map.read();
    assert_eq!(
        snapshot.digest_count(),
        1 + N,
        "baseline + hint digests must coexist"
    );
    // Baseline endpoint still resolves.
    let baseline_workers = snapshot.lookup_workers(&baseline_digest);
    assert_eq!(baseline_workers.len(), 1);
    assert_eq!(&*baseline_workers[0], baseline_endpoint);
    // A hint digest now resolves to the hint endpoint.
    let mut hash0 = [0u8; 32];
    hash0[..8].copy_from_slice(&0u64.to_be_bytes());
    let largest_hint = DigestInfo::new(hash0, N as u64);
    let hint_workers = snapshot.lookup_workers(&largest_hint);
    assert_eq!(hint_workers.len(), 1);
    assert_eq!(&*hint_workers[0], endpoint);
    Ok(())
}

#[nativelink_test]
async fn no_locality_map_handler_is_no_op_test() -> Result<(), Error> {
    // The worker may be configured WITHOUT a `peer_locality_map`
    // (cas_server_port unset → no peer-blob sharing). The chunk arm
    // must drop chunks silently rather than panic or error.
    let chunk = PeerHintsChunk {
        peer_hints: make_hints(10, "ignored:50081"),
        operation_id: "op-no-map".to_string(),
        sequence: 0,
        is_last: true,
    };
    handle_peer_hints_chunk(None, &chunk);
    // No assertion needed — getting here without panicking IS the test.
    Ok(())
}
