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

//! (#p2p-prefetch) Tests for the worker-side inline peer-hint registration.
//!
//! Two layers:
//!
//! 1. **Unit** — `register_missing_blob_peers` correctly registers the
//!    inline `StartExecute.missing_digest_peers` into the worker's
//!    `peer_locality_map` (idempotent, skips bad digests, no-op on absent map),
//!    plus a proto encode/decode round-trip of `MissingBlobPeers`.
//!
//! 2. **Production composition (the load-bearing seam, design §8)** — after
//!    `register_missing_blob_peers`, the EXISTING `WorkerProxyStore` peer-race
//!    (`race_peers=true`, initiator mode) pulls the input from the registered
//!    PEER rather than the co-launched server. Composed at the `WorkerProxyStore`
//!    seam where the race lives (`inject_worker_connection` supplies the fake
//!    peer CAS; the inner store is the fake server CAS). Seams crossed:
//!    proto `MissingBlobPeers` → `register_missing_blob_peers` → `peer_locality_map`
//!    → `WorkerProxyStore::get_part` `lookup_workers` → peer-race → peer serves.
//!    Every byte-delivering assertion is wrapped in a `tokio::time::timeout`
//!    deadlock detector with a bespoke expect-message.
//!
//! The MUTATION for the seam: SKIP the `register_missing_blob_peers` call and
//! the SAME read must fall to the server (here: an empty server → NotFound),
//! proving the registration — not some ambient state — is what routes the read
//! to the peer.

use core::time::Duration;

use bytes::Bytes;
use nativelink_config::stores::MemorySpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::MissingBlobPeers;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::local_worker::register_missing_blob_peers;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const ASSERT_TIMEOUT: Duration = Duration::from_secs(5);

fn make_entry(digest: DigestInfo, endpoints: &[&str]) -> MissingBlobPeers {
    MissingBlobPeers {
        digest: Some(digest.into()),
        peer_endpoints: endpoints.iter().map(|e| (*e).to_string()).collect(),
    }
}

/// Proto round-trip: a `MissingBlobPeers` survives prost encode→decode
/// byte-identically. Guards the wire shape (tags 1/2) the whole feature rides.
#[nativelink_test]
async fn missing_blob_peers_proto_round_trip() -> Result<(), Error> {
    use prost::Message;

    let digest = DigestInfo::try_new(VALID_HASH1, 4242)?;
    let original = MissingBlobPeers {
        digest: Some(digest.into()),
        peer_endpoints: vec![
            "grpc://192.168.100.5:50081".to_string(),
            "grpc://192.168.100.7:50081".to_string(),
        ],
    };

    let mut buf = Vec::new();
    original
        .encode(&mut buf)
        .expect("MissingBlobPeers must encode");
    let decoded = MissingBlobPeers::decode(buf.as_slice())
        .expect("MissingBlobPeers must decode the bytes it encoded");

    assert_eq!(
        decoded, original,
        "MissingBlobPeers must round-trip byte-identically through prost"
    );
    // Spot-check the digest survives to a DigestInfo.
    let round = DigestInfo::try_from(
        decoded.digest.as_ref().expect("digest present"),
    )?;
    assert_eq!(round, digest, "digest must survive the round-trip");
    Ok(())
}

/// Unit: `register_missing_blob_peers` registers every (digest, endpoint) pair
/// into the map so a subsequent `lookup_workers` resolves the peer. Multiple
/// endpoints per entry all register.
#[nativelink_test]
async fn register_missing_blob_peers_populates_map() -> Result<(), Error> {
    let map = new_shared_blob_locality_map();

    let d1 = DigestInfo::new([0x11; 32], 100);
    let d2 = DigestInfo::new([0x22; 32], 200);
    let entries = vec![
        make_entry(d1, &["grpc://peer-a:50081", "grpc://peer-b:50081"]),
        make_entry(d2, &["grpc://peer-a:50081"]),
    ];

    register_missing_blob_peers(Some(&map), &entries);

    let snapshot = map.read();
    assert_eq!(
        snapshot.digest_count(),
        2,
        "both inline digests must register"
    );
    let d1_workers = snapshot.lookup_workers(&d1);
    assert_eq!(d1_workers.len(), 2, "d1 must resolve to both peer endpoints");
    let d2_workers = snapshot.lookup_workers(&d2);
    assert_eq!(d2_workers.len(), 1, "d2 must resolve to its single endpoint");
    assert_eq!(&*d2_workers[0], "grpc://peer-a:50081");
    Ok(())
}

/// Unit: a `None` map (worker without `cas_server_port` → peer sharing off) is
/// a silent no-op, not a panic. Empty input is also a no-op.
#[nativelink_test]
async fn register_missing_blob_peers_no_map_and_empty_are_noops() -> Result<(), Error> {
    let d1 = DigestInfo::new([0x11; 32], 100);
    // No map: must not panic.
    register_missing_blob_peers(None, &[make_entry(d1, &["grpc://peer:50081"])]);

    // Empty input against a real map: no registration.
    let map = new_shared_blob_locality_map();
    register_missing_blob_peers(Some(&map), &[]);
    assert_eq!(
        map.read().digest_count(),
        0,
        "empty inline set must register nothing"
    );
    Ok(())
}

/// Unit: an entry with a missing (`None`) digest is skipped without aborting
/// the rest of the batch. Guards against a bad-entry panicking the whole
/// registration.
#[nativelink_test]
async fn register_missing_blob_peers_skips_bad_entry() -> Result<(), Error> {
    let map = new_shared_blob_locality_map();
    let good = DigestInfo::new([0x33; 32], 300);
    let entries = vec![
        MissingBlobPeers {
            digest: None, // bad: no digest
            peer_endpoints: vec!["grpc://peer:50081".to_string()],
        },
        make_entry(good, &["grpc://peer:50081"]),
    ];
    register_missing_blob_peers(Some(&map), &entries);
    let snapshot = map.read();
    assert_eq!(
        snapshot.digest_count(),
        1,
        "the None-digest entry is skipped; the good entry still registers"
    );
    assert_eq!(snapshot.lookup_workers(&good).len(), 1);
    Ok(())
}

/// PRODUCTION-COMPOSITION SEAM (design §8, success case): register the inline
/// peers, then a read through the `WorkerProxyStore` race pulls the blob from
/// the PEER. Inner (server) is EMPTY, so ONLY the registered peer can serve —
/// proving the registration routed the read to the peer.
///
/// Seams crossed: proto `MissingBlobPeers` → `register_missing_blob_peers` →
/// `peer_locality_map` → `WorkerProxyStore::get_part` `lookup_workers` →
/// peer-race → peer serves the bytes.
///
/// Mutation (the sibling test below): SKIP the registration → the SAME read
/// hits the empty server → NotFound.
#[nativelink_test]
async fn inline_registration_routes_read_to_peer() -> Result<(), Error> {
    let result = tokio::time::timeout(ASSERT_TIMEOUT, async {
        // Inner = fake server CAS, EMPTY (so only the peer can serve).
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map.clone());
        proxy.enable_race_peers(); // initiator mode: race peers vs server
        let store = Store::new(proxy.clone());

        let value = Bytes::from_static(b"peer-served input blob");
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        // Fake peer CAS that HOLDS the blob.
        let peer_endpoint = "grpc://peer:50071";
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_store.update_oneshot(digest, value.clone()).await?;
        proxy.inject_worker_connection(peer_endpoint, peer_store);

        // THE FEATURE STEP: register the inline peer hint BEFORE the read
        // (mirrors the worker's StartExecute-parse registration upstream of
        // input_fetch).
        register_missing_blob_peers(
            Some(&locality_map),
            &[make_entry(digest, &[peer_endpoint])],
        );

        let bytes = store.get_part_unchunked(digest, 0, None).await?;
        Result::<Bytes, Error>::Ok(bytes)
    })
    .await;

    let bytes = result.expect(
        "inline-peer read must not deadlock — WorkerProxyStore race \
         writer-termination contract violated on the peer-served path",
    )?;
    assert_eq!(
        bytes,
        Bytes::from_static(b"peer-served input blob"),
        "after inline registration the WorkerProxyStore race must serve the \
         input from the registered PEER (inner server is empty) — inline peer \
         hint not registered before input_fetch, offload lost",
    );
    Ok(())
}

/// PRODUCTION-COMPOSITION SEAM (design §8, mutation/fallback case): the SAME
/// composition WITHOUT the registration step. `lookup_workers` returns empty
/// (the freshness gap), the read falls to the co-launched server sequential
/// path, and — because the server is empty — the read is NotFound. This is the
/// bespoke mutation for the success test above: it proves the registration,
/// not ambient state, is what routes the read to the peer.
///
/// It ALSO proves the never-worse-than-today fallback: no registration ⇒ the
/// read degrades to exactly the server path (no stall, no deadlock — the
/// deadlock detector fires the bespoke message if it hangs).
#[nativelink_test]
async fn without_registration_read_falls_to_server() -> Result<(), Error> {
    let outcome = tokio::time::timeout(ASSERT_TIMEOUT, async {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map.clone());
        proxy.enable_race_peers();
        let store = Store::new(proxy.clone());

        let value = Bytes::from_static(b"peer-served input blob");
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        // Peer HOLDS the blob and IS injected — but we do NOT register it into
        // the locality map, so `lookup_workers` cannot find it.
        let peer_endpoint = "grpc://peer:50071";
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_store.update_oneshot(digest, value.clone()).await?;
        proxy.inject_worker_connection(peer_endpoint, peer_store);

        // NO register_missing_blob_peers call here — the mutation.

        // Read must fall to the (empty) server sequential path → NotFound.
        let res = store.get_part_unchunked(digest, 0, None).await;
        Result::<_, Error>::Ok(res)
    })
    .await;

    let res = outcome.expect(
        "no-registration read must not deadlock — the server-fallback path \
         must terminate the writer even on NotFound",
    )?;
    assert!(
        res.is_err(),
        "without registration the read cannot find the peer in the locality \
         map and must fall to the empty server → NotFound; got Ok, which means \
         the read reached the peer WITHOUT the inline registration (ambient \
         state leak — the seam test is vacuous)",
    );
    Ok(())
}
