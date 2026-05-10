// Copyright 2024-2026 The NativeLink Authors. All rights reserved.
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

//! #332 — `CompletenessCheckingStore::pin_digests` deletion regression.
//!
//! ## The bug being closed
//!
//! `CompletenessCheckingStore::inner_has_with_results`
//! (`completeness_checking_store.rs:322`) and
//! `CompletenessCheckingStore::get_and_verify_single`
//! (`completeness_checking_store.rs:464`) both called
//! `self.cas_store.pin_digests(&verified_batch)` on every CAS digest
//! whose existence had just been verified. The original intent
//! (`f9566c82` / `2015b32b`, March 2026) was to narrow the TOCTOU window
//! between "completeness check verified the blob exists" and "the worker
//! arrives to read it." A 120 s `MokaEvictingMap::PIN_TIMEOUT_SECS`
//! handled cleanup.
//!
//! ## Why it had to go
//!
//! From `b9e40d8c` (2026-04-26) through `#334` Fix C (2026-05-08), the
//! `MemoryStore::pin_digests` override was deleted by a bulk-delegation
//! sweep — the trait default's `Leaf` arm was a documented no-op. CCS
//! pins propagated through `cas_STORE → VerifyStore →
//! ExistenceCacheStore → SizePartitioningStore → cas_FAST_SLOW_STORE` and
//! disappeared at the MemoryStore leaf. Harmless but inert.
//!
//! `#334` Fix C reinstated `MemoryStore::pin_digests`. CCS pins
//! immediately became real — and they had no matching `unpin` (only the
//! 120 s TTL released them). Under heavy completeness-check traffic,
//! every verified CAS digest was pinned for 120 s against the
//! `cas_FAST_SLOW_STORE.fast = MemoryStore (48 GB)` 12 GB pin cap (25 %
//! of `max_bytes`), starving the BIS-feeder pin path that protects the
//! ≥2-replica durability invariant. Once the pin cap fills,
//! `MokaEvictingMap::pin_keys` warns "pin cap exceeded" and skips the
//! pin — silently re-opening the durability gap that #334 Fix C had
//! just closed.
//!
//! The CCS pin was vestigial in any case: `cas_FAST_SLOW_STORE`'s slow
//! tier (FilesystemStore, 800 GB) holds every CAS blob durably, so a
//! fast-tier eviction during the check-to-fetch window is transparent
//! (the next read falls through to the slow tier). The chunked-read
//! pin (`fast_slow_store.rs:5326-5451`, gated on
//! `chunked_reads_enabled=true`) protects a different window
//! (in-flight chunked WRITE, not check-to-fetch).
//!
//! ## What this test file asserts (two tests)
//!
//! ### Test 1 (`ccs_does_not_pin_verified_cas_digests`) — pin-corner only
//!
//! Under-action: post-fix, `CompletenessCheckingStore::has_with_results`
//! (the production AC `has` entry-point) does NOT pin verified CAS
//! digests in the underlying `MemoryStore`. The `pinned_bytes_for_test`
//! accessor on the fast-tier `MemoryStore` MUST stay at 0 throughout
//! a sustained completeness-check workload that, pre-fix, would have
//! pushed `pinned_bytes` into the multi-KiB range against a tight cap.
//!
//! Production composition seam: `CompletenessCheckingStore { ac_store:
//! MemoryStore-AC, cas_store: VerifyStore → ExistenceCacheStore →
//! MemoryStore-CAS }`. The pin call propagates through the same
//! `pin_delegation` chain production wires through `cas_STORE`. The
//! CAS-side `MemoryStore` is the leaf where the pin would have landed.
//!
//! Mutation step: re-add the deleted `self.cas_store.pin_digests(...)`
//! at either CCS site. The test MUST red-fail with the bespoke
//! assertion message "CCS pins accumulate without unpin → MemoryStore
//! cap exhaustion → BackpressureSignal cascade. See #332." A 5 s
//! `tokio::time::timeout` wraps the workload as a deadlock detector
//! per CLAUDE.md "test in production composition" guidance.
//!
//! ### Test 2 (`ccs_pin_deletion_satisfies_composite_invariant`) — composite
//!
//! Test 1 leaves the `emit_backpressure_enabled` GATE corner OFF (default
//! `MemorySpec`), so `check_backpressure_gate` short-circuits at
//! `memory_store.rs:275-277` without ever consulting
//! `would_exceed_capacity`. That covers the local "pin contract" but not
//! the COMPOSITE invariant
//!
//!     `gate-active ⇒ (pin-works AND ttl-fires-within-bound) OR
//!                     explicit-eviction-fires-before-gate`
//!
//! per CLAUDE.md "Admission/Eviction/Pin Composability." Per
//! invariant-prover review on `7c288ca5`: a regression test that exercises
//! ONLY the pin corner satisfies *form* but not *substance* of the
//! composite-invariant requirement.
//!
//! Test 2 wires the gate corner ON (`emit_backpressure_enabled = true`),
//! drives the same CCS-shaped completeness-check workload, then issues a
//! controlled over-cap CAS write that would trip `would_exceed_capacity`.
//! With the post-#332 reality (CCS pins gone) AND the post-#334 Fix C
//! eviction extension at `memory_store.rs:282-322`, the gate evicts
//! unpinned cache entries first and admits the write — no cascade. The
//! TLA+ spec at `specs/CcsPinRemovalComposite.tla` formalizes this
//! reduction; the bespoke assertion below is the composite-invariant
//! statement the spec proves at runtime.
//!
//! Mutation step (mandatory per invariant-prover): comment out the
//! eviction extension at `memory_store.rs:282-322` (the corner #332
//! did NOT touch — but on which #332 relies). The under-test write then
//! returns `Code::ResourceExhausted +
//! BackpressureSignal::MemoryStoreAtCapacity`, the bespoke assertion
//! red-fails with "composite invariant violated: gate active without
//! compensating eviction/pin/TTL", and `logs_contain` finds the gate's
//! `MemoryStoreAtCapacity` `debug!` marker. After confirming the
//! red-fail, UNDO the comment-out and confirm the test passes again.
//!
//! Why this is the load-bearing scenario: it degrades TWO of the three
//! corners (gate stays ON, pin corner removed by #332 itself, eviction
//! extension mutated OFF) and asserts the THIRD (here: the BIS-feeder
//! pin path's continued availability of the pin cap, which #332 buys
//! by removing the spurious CCS accumulator) compensates. With the CCS
//! accumulator absent, the BIS-feeder pin path can claim its share of
//! the cap unmolested; the gate stays admissible until BIS-ack drains
//! genuine pins.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, MemorySpec, StoreSpec, VerifySpec,
};
use nativelink_error::{Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, OutputFile,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::{Store, StoreLike};

/// 5 s deadlock detector — see CLAUDE.md "test in production composition".
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// CAS leaf cap. Generous (1 MiB) so the LRU evictor never fires
/// during the test — every seeded CAS blob stays visible to
/// `has_with_results`, and any pinned-bytes accumulation observed at
/// the assertion is unambiguously attributable to CCS pin calls (vs
/// moka's approximate-weight LRU touching the boundary).
/// Pin cap = 25 % × `CAS_MEM_CAP` = 256 KiB ⇒ comfortably above the
/// PAIR_COUNT × CAS_BLOB_BYTES the pre-fix path would try to pin, so
/// `pin_keys: pin cap exceeded` does NOT fire and the post-fix
/// "pinned_bytes == 0" assertion is the load-bearing invariant.
const CAS_MEM_CAP: usize = 1024 * 1024;

/// Per-CAS-blob payload. 256 bytes keeps the test budget small while
/// still being big enough to register `entry_size` accounting in
/// `MokaEvictingMap::pinned_bytes` (which tracks raw byte sizes, not
/// the moka 1-KiB granularity).
const CAS_BLOB_BYTES: usize = 256;

/// Number of distinct (AC, CAS) pairs to drive through the
/// completeness check. Pre-fix this would yield 8 × 256 = 2048 bytes
/// of pinned weight — well above 0 and well within the 256 KiB pin cap
/// (so every pin succeeds and `pinned_bytes` reflects actual pin count
/// rather than a partial cap-exceeded mode).
const PAIR_COUNT: usize = 8;

/// Completeness-check repetitions per pair. With pin refresh on
/// already-pinned keys, repeating the same digest pre-fix does NOT
/// inflate `pinned_bytes` further — but it DOES keep the pin alive past
/// the natural insert/forget churn. Five passes leaves no doubt the
/// observed pinned-bytes value reflects intent rather than a transient
/// race.
const REPS: usize = 5;

/// Build the production-shaped CCS composition over a tight-cap
/// MemoryStore CAS leaf. Returns the wrapping `CompletenessCheckingStore`
/// (entry-point for `has_with_results`) plus a direct handle on the
/// CAS-side MemoryStore (for the `pinned_bytes_for_test` assertion —
/// the wrapping chain has no accessor).
fn build_chain() -> (Arc<CompletenessCheckingStore>, Arc<MemoryStore>) {
    // AC backing store. Production wires `AC_BACKEND_CACHED`
    // (FastSlowStore over Redis); a plain MemoryStore here is sufficient
    // because the test exercises the CAS-side pin path, not AC-side
    // durability.
    let ac_backend = Store::new(MemoryStore::new(&MemorySpec::default()));

    // CAS leaf with a tight cap. This is the load-bearing pin target.
    let cas_mem = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: CAS_MEM_CAP,
            ..Default::default()
        }),
        ..Default::default()
    });
    let cas_mem_store = Store::new(cas_mem.clone());

    // VerifyStore wraps the CAS leaf — the canonical CAS chain in
    // production is `cas_STORE → VerifyStore → cas_INNER → ...`.
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        cas_mem_store,
    ));

    // ExistenceCacheStore is the outermost CAS wrapper in production.
    // Both VerifyStore and ExistenceCacheStore declare
    // `pin_delegation = Inner(...)`, so a `pin_digests` call walks
    // straight through to the leaf MemoryStore — exactly what a
    // pre-fix CCS would have triggered.
    let cas_chain = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1_000_000,
                ..Default::default()
            }),
        },
        verify,
    ));

    let ccs = CompletenessCheckingStore::new(ac_backend, cas_chain);
    (ccs, cas_mem)
}

#[nativelink_test]
async fn ccs_does_not_pin_verified_cas_digests() -> Result<(), Error> {
    let (ccs, cas_mem) = build_chain();

    // Seed PAIR_COUNT distinct CAS blobs into the leaf MemoryStore so
    // the completeness check finds them. Each blob is `CAS_BLOB_BYTES`
    // of distinct content; the digests are deterministic from the
    // hasher input.
    let mut cas_digests = Vec::with_capacity(PAIR_COUNT);
    for i in 0..PAIR_COUNT {
        // Distinct content per iteration so the digests don't collide.
        let payload = Bytes::from(vec![u8::try_from(i).unwrap_or(0); CAS_BLOB_BYTES]);
        let mut hasher = DigestHasherFunc::Blake3.hasher();
        hasher.update(&payload);
        let digest = hasher.finalize_digest();
        cas_mem
            .update_oneshot(digest, payload)
            .await
            .err_tip(|| format!("seeding CAS leaf with blob {i}"))?;
        cas_digests.push(digest);
    }

    // Build PAIR_COUNT distinct AC entries, each referencing exactly one
    // of the seeded CAS blobs as `output_files[0].digest`. Uploaded
    // through the CCS so the AC backend has them.
    let mut ac_digests = Vec::with_capacity(PAIR_COUNT);
    for cas_digest in &cas_digests {
        let action_result = ProtoActionResult {
            output_files: vec![OutputFile {
                digest: Some((*cas_digest).into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let ac_digest = serialize_and_upload_message(
            &action_result,
            ccs.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await
        .err_tip(|| "uploading AC entry through CCS")?;
        ac_digests.push(ac_digest);
    }

    // Sanity: pinned_bytes is 0 BEFORE any has_with_results call. If
    // this is non-zero, an unrelated path is pinning and the test's
    // assertion below would be misattributed.
    assert_eq!(
        cas_mem.pinned_bytes_for_test(),
        0,
        "test setup invariant: nothing should be pinned in the CAS leaf \
         before the first completeness check fires. observed: {} bytes.",
        cas_mem.pinned_bytes_for_test(),
    );

    // Drive the completeness check. Each iteration calls has_many on
    // EVERY AC digest in one batch; pre-fix, every iteration's
    // `inner_has_with_results` lands a `pin_digests` call on the
    // verified CAS digests. Wrapped in tokio::time::timeout — the
    // deadlock detector. Per CLAUDE.md the assertion message must be
    // SPECIFIC, not is_ok()/is_err().
    let workload = async {
        for rep in 0..REPS {
            let ac_keys: Vec<_> = ac_digests.iter().map(|d| (*d).into()).collect();
            let res = ccs
                .has_many(&ac_keys)
                .await
                .err_tip(|| format!("ccs.has_many on rep {rep}"))?;
            assert_eq!(
                res.len(),
                ac_digests.len(),
                "has_many returned wrong slice length on rep {rep}",
            );
            for (i, r) in res.iter().enumerate() {
                assert!(
                    r.is_some(),
                    "AC entry {i} should be complete on rep {rep} \
                     (CAS blob seeded directly into leaf)",
                );
            }
        }
        Result::<(), Error>::Ok(())
    };
    tokio::time::timeout(NO_DEADLOCK_TIMEOUT, workload)
        .await
        .expect(
            "ccs.has_many wedged past 5 s — production-composition \
             regression in CCS / VerifyStore / ExistenceCacheStore chain",
        )
        .expect("ccs.has_many returned Err");

    // Load-bearing assertion. Post-fix the CCS pin call is gone; the
    // CAS-leaf MemoryStore should hold zero pinned bytes regardless of
    // how many completeness checks have fired. Pre-fix this would be
    // PAIR_COUNT × CAS_BLOB_BYTES = 8 × 256 = 2048 bytes (or as much
    // of that as the 1024-byte pin cap admits before pin_keys warns
    // and skips the rest — either way, > 0).
    let observed = cas_mem.pinned_bytes_for_test();
    assert_eq!(
        observed,
        0,
        "CCS pins accumulate without unpin → MemoryStore cap exhaustion \
         → BackpressureSignal cascade. See #332. If this fails, \
         CompletenessCheckingStore::pin_digests has been re-added at \
         completeness_checking_store.rs:322 or :464 (or an equivalent \
         post-verification CAS pin path was re-introduced). observed \
         pinned_bytes={} (expected 0); pre-fix this would land near \
         {} bytes. PAIR_COUNT={}, REPS={}, CAS_BLOB_BYTES={}, \
         CAS_MEM_CAP={}.",
        observed,
        PAIR_COUNT * CAS_BLOB_BYTES,
        PAIR_COUNT,
        REPS,
        CAS_BLOB_BYTES,
        CAS_MEM_CAP,
    );

    Ok(())
}

// =====================================================================
// Test 2: composite invariant (gate ⇒ pin OR ttl OR evict)
// =====================================================================

/// Tight cap for the composite test. 4 KiB matches
/// `memory_store_backpressure_eviction_test.rs` — the smallest cap that
/// supports both meaningful pinning (`pin_cap = 25 % × cap = 1 KiB`)
/// AND a triggerable gate on a realistic write size.
const GATED_CAS_MEM_CAP: usize = 4096;

/// Per-CAS-blob payload for the composite test. 1 KiB so 4 entries fill
/// the cap exactly (4 × 1 KiB = 4 KiB), leaving a single 1 KiB incoming
/// write to land precisely over the cap and force the gate's
/// would_exceed_capacity → eviction-extension → re-check sequence.
const GATED_CAS_BLOB_BYTES: usize = 1024;

/// Number of CAS blobs to seed into the CAS leaf for the composite
/// test. 4 × 1 KiB = exactly cap; the over-cap probe write below pushes
/// the gate to act.
const GATED_PAIR_COUNT: usize = 4;

/// Build the production-shaped CCS composition with the GATE corner ON.
/// Same chain as `build_chain` (CCS → ExistenceCache → Verify →
/// MemoryStore-CAS) but the CAS leaf is constructed with
/// `emit_backpressure_enabled` enabled at runtime via
/// `MemoryStore::enable_emit_backpressure` — matching the production
/// `cas_FAST_SLOW_STORE.fast.memory.emit_backpressure_enabled = true`
/// flag set in `prod-server.json5` (production sign-off 2026-05-02).
///
/// Returns the wrapping `CompletenessCheckingStore` plus a direct handle
/// on the CAS-side `MemoryStore` (for the `pinned_bytes_for_test`
/// assertion AND the over-cap probe write).
fn build_chain_with_gate_on() -> (Arc<CompletenessCheckingStore>, Arc<MemoryStore>) {
    let ac_backend = Store::new(MemoryStore::new(&MemorySpec::default()));

    // CAS leaf with tight cap + gate ON. emit_backpressure_enabled is
    // first set false in the spec then armed via the runtime API to
    // mirror the pattern used by `memory_store_backpressure_eviction_test`
    // — the production wiring is identical (the JSON flag auto-arms it
    // in `MemoryStore::new`; the runtime API is equivalent).
    let cas_mem = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: GATED_CAS_MEM_CAP,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    cas_mem.enable_emit_backpressure();
    debug_assert!(
        cas_mem.emit_backpressure_enabled(),
        "test setup invariant: emit_backpressure_enabled must be ON for \
         the composite-invariant test to exercise the gate corner"
    );
    let cas_mem_store = Store::new(cas_mem.clone());

    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        cas_mem_store,
    ));

    let cas_chain = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1_000_000,
                ..Default::default()
            }),
        },
        verify,
    ));

    let ccs = CompletenessCheckingStore::new(ac_backend, cas_chain);
    (ccs, cas_mem)
}

/// Composite-invariant regression for #332 + #334 Fix C.
///
/// Per CLAUDE.md "Admission/Eviction/Pin Composability" mandatory
/// practice step 3 (and invariant-prover review on `7c288ca5`): a
/// regression test that exercises ONLY the pin corner satisfies form,
/// not substance. This test wires the gate ON and asserts the composite
/// invariant
///
///   `gate-active ⇒ (pin-works AND ttl-fires-within-bound) OR
///                   explicit-eviction-fires-before-gate`
///
/// holds across the post-#332 reality (CCS pin path deleted) AND the
/// post-#334 Fix C reality (eviction extension at
/// `memory_store.rs:282-322`).
///
/// Scenario:
///   1. Build CCS chain with gate ON on the CAS leaf
///      (`emit_backpressure_enabled = true`), tight cap (4 KiB).
///   2. Seed `GATED_PAIR_COUNT` × 1 KiB unpinned CAS blobs (fills cache
///      exactly to cap) and matching AC entries.
///   3. Drive `CCS::has_many` repeatedly. Post-#332 this does NOT pin;
///      pre-#332 this would have pushed pinned_bytes towards cap.
///   4. Issue an over-cap CAS write directly. With the eviction
///      extension in place AND no CCS-driven pins, eviction frees an
///      unpinned LRU slot and the write is admitted.
///
/// Mutation step (mandatory per invariant-prover, regenerable):
///   In `nativelink-store/src/memory_store.rs`, comment out the eviction
///   extension at `:282-322` (the `let report = self.evicting_map
///   .evict_unpinned_lru_bytes(...)` block AND the subsequent
///   `if !self.evicting_map.would_exceed_capacity(incoming_bytes) {
///   return Ok(()); }` re-check). Re-run this test:
///
///     cargo test -p nativelink-store --features test-utils,chunked_fast_slow \
///       --test completeness_checking_pin_accumulation_test \
///       ccs_pin_deletion_satisfies_composite_invariant
///
///   It MUST red-fail at the over-cap-write `expect(...)` with the
///   bespoke message naming "composite invariant violated: gate active
///   without compensating eviction/pin/TTL", AND `logs_contain` must
///   find the gate's `MemoryStoreAtCapacity` debug! marker.
///
///   After confirming the red-fail, `git restore
///   nativelink-store/src/memory_store.rs` and re-run; the test passes.
///
/// Scope honest-statement (CLAUDE.md "two of three corners degraded;
/// third compensates"): this composition wires **gate** and
/// **eviction-extension** corners only — no FastSlowStore wrapper,
/// so the BIS-feeder pin path is NOT in the composition. The test
/// therefore proves "gate + eviction-extension is sufficient when
/// no other pin source exists", which is exactly the post-#332
/// state once CCS pins are gone. The TLA+ spec at
/// `specs/CcsPinRemovalComposite.tla` covers the broader gate × pin ×
/// eviction-extension cross-product including the BIS-feeder pin
/// corner; this Rust regression test exercises only the
/// gate × eviction-extension sub-cross-product accessible at the
/// CCS unit boundary. See #370 for a wider production-composition
/// regression that crosses all 5 wrappers including FastSlowStore +
/// BIS-feeder pin. The mutation step (eviction-extension OFF)
/// produces the cascade signature `NoCascadeUnlessAllPinned`
/// violates in `CcsPinRemovalCompositeBugged.cfg`.
#[nativelink_test]
async fn ccs_pin_deletion_satisfies_composite_invariant() -> Result<(), Error> {
    let (ccs, cas_mem) = build_chain_with_gate_on();

    // Seed GATED_PAIR_COUNT distinct CAS blobs to fill the cache
    // EXACTLY at cap (4 × 1 KiB = 4 KiB). Distinct hex hashes so the
    // digests are deterministic and distinct without computing real
    // Blake3 (the seed values just have to be valid 32-byte hex).
    let cas_digests: Vec<DigestInfo> = (0..GATED_PAIR_COUNT)
        .map(|i| {
            // 64-char hex string distinguishing each digest by its last
            // hex digit (1..GATED_PAIR_COUNT inclusive). VerifyStore is
            // configured with verify_hash = false, so the hash content
            // is not validated against the payload.
            let hex = format!("{:063}{:01x}", 0, i + 1);
            DigestInfo::try_new(&hex, GATED_CAS_BLOB_BYTES as u64)
                .expect("hex digest construction")
        })
        .collect();
    for (i, digest) in cas_digests.iter().enumerate() {
        let payload = Bytes::from(vec![u8::try_from(i).unwrap_or(0); GATED_CAS_BLOB_BYTES]);
        cas_mem
            .update_oneshot(*digest, payload)
            .await
            .err_tip(|| format!("seeding CAS leaf with blob {i}"))?;
    }

    // Build matching AC entries through CCS so the AC backend has them.
    let mut ac_digests = Vec::with_capacity(GATED_PAIR_COUNT);
    for cas_digest in &cas_digests {
        let action_result = ProtoActionResult {
            output_files: vec![OutputFile {
                digest: Some((*cas_digest).into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let ac_digest = serialize_and_upload_message(
            &action_result,
            ccs.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await
        .err_tip(|| "uploading AC entry through CCS")?;
        ac_digests.push(ac_digest);
    }

    // Setup invariant: nothing pinned before the gated workload.
    assert_eq!(
        cas_mem.pinned_bytes_for_test(),
        0,
        "test setup invariant: pinned_bytes must be 0 before the gated \
         workload — observed {} bytes (gate-on chain leaked a pin from \
         seeding/upload)",
        cas_mem.pinned_bytes_for_test(),
    );

    // Drive CCS has_many traffic. Post-#332 this lands NO pins on the
    // CAS leaf. Pre-#332 every iteration would have lit up the
    // post-verification pin path; with gate ON and the cache already
    // at cap, that pin accumulator would interact with the gate's cap
    // checks on subsequent writes (the cascade scenario the TLA+ spec
    // models with `CCS pin source = active`).
    let workload = async {
        for rep in 0..REPS {
            let ac_keys: Vec<_> = ac_digests.iter().map(|d| (*d).into()).collect();
            let res = ccs
                .has_many(&ac_keys)
                .await
                .err_tip(|| format!("ccs.has_many on rep {rep}"))?;
            assert_eq!(
                res.len(),
                ac_digests.len(),
                "has_many returned wrong slice length on rep {rep}",
            );
            for (i, r) in res.iter().enumerate() {
                assert!(
                    r.is_some(),
                    "AC entry {i} should be complete on rep {rep} \
                     (CAS blob seeded directly into leaf)",
                );
            }
        }
        Result::<(), Error>::Ok(())
    };
    tokio::time::timeout(NO_DEADLOCK_TIMEOUT, workload)
        .await
        .expect(
            "ccs.has_many wedged past 5 s with gate ON — composite \
             regression: a wedge here usually means the gate path \
             leaked a pin or got stuck in evict-and-retry under \
             completeness-check traffic",
        )
        .expect("ccs.has_many returned Err");

    // Pin-corner under-action: still 0 after the workload. This is the
    // same load-bearing assertion as test 1, repeated here so a wedge
    // in the gate-on chain doesn't mask a regression.
    let observed_after_workload = cas_mem.pinned_bytes_for_test();
    assert_eq!(
        observed_after_workload,
        0,
        "composite invariant violated: gate active without compensating \
         eviction/pin/TTL — pinned_bytes leaked through the gate-on \
         CCS chain (observed {} bytes; expected 0). Either CCS started \
         pinning again (re-introduced by an edit to \
         completeness_checking_store.rs) OR an upstream wrapper in the \
         CCS → ExistenceCache → Verify → MemoryStore chain learned to \
         pin on a path it shouldn't.",
        observed_after_workload,
    );

    // Composite-invariant probe: an over-cap write that the gate's
    // eviction extension MUST be able to admit. The cache is currently
    // 4 KiB (full); a 1 KiB incoming write would push to 5 KiB > 4 KiB
    // cap. With the eviction extension in place AND nothing pinned,
    // it evicts ≥ 1 KiB of unpinned LRU and the write is admitted.
    //
    // The probe digest must NOT collide with any seeded digest; using
    // last-hex 0xff ensures distinctness from the (i+1) seeded values
    // (1..=GATED_PAIR_COUNT).
    let probe_hex = format!("{:063}{:01x}", 0, 0xfu8);
    let probe_digest = DigestInfo::try_new(&probe_hex, GATED_CAS_BLOB_BYTES as u64)
        .expect("probe digest construction");
    let probe_payload = Bytes::from(vec![0xCCu8; GATED_CAS_BLOB_BYTES]);

    let probe_result = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        cas_mem.update_oneshot(probe_digest, probe_payload),
    )
    .await
    .expect(
        "composite invariant violated: gate active without compensating \
         eviction/pin/TTL — gate-driven over-cap write wedged past 5 s. \
         The eviction extension at memory_store.rs:282-322 should have \
         freed unpinned LRU bytes within microseconds. A timeout here \
         means the gate's eviction loop is holding a lock across an \
         await or otherwise wedging.",
    );

    if let Err(e) = &probe_result {
        if e.code == Code::ResourceExhausted {
            panic!(
                "composite invariant violated: gate active without \
                 compensating eviction/pin/TTL. The over-cap probe write \
                 returned MemoryStoreAtCapacity even though pinned_bytes \
                 = 0 and 4 KiB of unpinned LRU was available to evict. \
                 This means the eviction extension at \
                 memory_store.rs:282-322 is missing or broken: the gate \
                 is firing on `would_exceed_capacity` directly without \
                 the eviction-then-recheck step. Error: {e:?}. Mutation \
                 step verification: this is the EXACT failure shape \
                 expected when the eviction extension is commented out. \
                 If you are running the mutation step right now, this \
                 panic is the expected red-fail. If you are NOT, restore \
                 lines 282-322 of memory_store.rs."
            );
        }
    }
    probe_result.expect(
        "composite invariant violated: gate active without compensating \
         eviction/pin/TTL — gate-driven over-cap write returned \
         non-ResourceExhausted error",
    );

    // Pin corner stays clean post-probe: the eviction extension MUST
    // not have lifted any unpinned entry into pinned (it touches only
    // the cache, not the pinned DashMap).
    let final_pinned = cas_mem.pinned_bytes_for_test();
    assert_eq!(
        final_pinned,
        0,
        "composite invariant violated: gate active without compensating \
         eviction/pin/TTL — eviction-extension pathway leaked a pin \
         (observed {} bytes after over-cap probe; expected 0). The \
         extension MUST evict unpinned LRU only, never promote into \
         pinned.",
        final_pinned,
    );

    // Cascade-marker check: the gate's emit path logs at debug! with
    // the literal "MemoryStoreAtCapacity" reason name. Post-fix this
    // string MUST NOT appear in the captured trace — its presence is
    // the smoking gun for an over-cap probe that the eviction extension
    // failed to rescue. (Mutation step: with `:282-322` commented, this
    // assertion fires with the bespoke message AND the panic above
    // also fires — either is sufficient evidence of the composite-
    // invariant violation; both firing makes the failure unmistakable.)
    assert!(
        !logs_contain("emitting BackpressureSignal::MemoryStoreAtCapacity"),
        "composite invariant violated: gate active without compensating \
         eviction/pin/TTL — gate emitted MemoryStoreAtCapacity during a \
         workload that should have been fully serviceable by the \
         eviction extension. See `memory_store.rs:329` for the log \
         site; an appearance here means either CCS started pinning \
         again OR the eviction extension at `:282-322` is broken."
    );

    Ok(())
}
