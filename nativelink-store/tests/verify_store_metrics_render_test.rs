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

//! `#fl1786-server-side-digest-function-proving`: `VerifyStore`'s counters
//! render on `/metrics`, under the exact production names.
//!
//! **Why this file exists (pair-a F2).** The chunked site got a 22-name
//! render test. The `VerifyStore` site got none — every assertion on its new
//! `digest_func_proven` counter went through `digest_func_proven_count()`, a
//! direct atomic load that bypasses the `#[metric]` publish path entirely, so
//! nothing anywhere pinned the rendered string. And `VerifyStore` is the site
//! that closes the **observed** latch: all 591 rejections in the live 24 h
//! window were on `:50071` with `is_worker=true` — the bytestream →
//! `cas_STORE` → `VerifyStore` path (`deferred_tasks.md:3579`). The full
//! observability treatment had been applied to the site that has never fired.
//!
//! **Production composition reproduced.** `src/bin/nativelink.rs:583` does
//! `metrics_registry.register("nativelink", store_manager.clone())`, and
//! `StoreManager` carries `#[metric] stores: RwLock<HashMap<String, Store>>`
//! (`store_manager.rs`), so a store added under the name `cas_STORE`
//! (`buildcache-native.json5:183`) renders as `nativelink_cas_STORE_<field>`.
//! This file builds that exact shape rather than registering the
//! `VerifyStore` directly, because the prefix an operator's dashboard keys on
//! comes from the StoreManager registration, not from the store.
//!
//! Values are stored via a real write through the store (not a setter), so a
//! metric that renders `0` because its publish was deleted cannot be confused
//! with one that renders `0` because nothing happened.

use std::sync::Arc;

use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasherFunc, default_digest_hasher_func, make_ctx_for_hash_func,
    set_default_digest_hasher_func,
};
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::store_trait::{Store, StoreLike};
use opentelemetry::context::FutureExt;
use tracing::{Instrument, info_span};

const VALUE: &str = "123";
/// `sha256("123")` — the mislabelled-but-intact fixture (the live shape).
const SHA256_OF_VALUE: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
/// `sha256("12")` — the digest of DIFFERENT bytes; genuinely corrupt.
const SHA256_OF_OTHER: &str = "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918";

/// The production CAS store name (`buildcache-native.json5:183`), which is what
/// makes the rendered names below the ones dashboards actually read.
const CAS_STORE_NAME: &str = "cas_STORE";

/// Every metric `VerifyStore` publishes, as the exact name it renders to when
/// the process `StoreManager` is registered under `nativelink`. These strings
/// are a contract with dashboards and alerts.
/// Note the `_counter` / `_last_time` suffix pair: every `CounterWithTime`
/// field renders as TWO lines, and a dashboard keys on `_counter`. Pinning
/// the bare field name would have passed against a substring and told us
/// nothing about what is actually scrapeable.
const EXPECTED_METRIC_NAMES: &[&str] = &[
    "nativelink_cas_STORE_verify_size",
    "nativelink_cas_STORE_verify_hash",
    "nativelink_cas_STORE_size_verification_failures_counter",
    "nativelink_cas_STORE_size_verification_failures_last_time",
    "nativelink_cas_STORE_hash_verification_failures_counter",
    "nativelink_cas_STORE_hash_verification_failures_last_time",
    "nativelink_cas_STORE_digest_func_proven_counter",
    "nativelink_cas_STORE_digest_func_proven_last_time",
    "nativelink_cas_STORE_digest_func_proven_on_read_counter",
    "nativelink_cas_STORE_digest_func_proven_on_read_last_time",
    "nativelink_cas_STORE_hash_verification_failures_on_read_counter",
    "nativelink_cas_STORE_hash_verification_failures_on_read_last_time",
];

fn pin_production_default_blake3() {
    let _ = set_default_digest_hasher_func(DigestHasherFunc::Blake3);
    assert_eq!(
        default_digest_hasher_func(),
        DigestHasherFunc::Blake3,
        "#fl1786: this test binary must run with the PRODUCTION process-global digest function \
         (BLAKE3, buildcache-native.json5:748); reading SHA-256 means something set the OnceCell \
         first and the proving write below would not reproduce the live composition"
    );
}

/// Build the production registration shape: a `StoreManager` holding a real
/// `VerifyStore` under the production CAS store name, registered under the
/// `nativelink` prefix exactly as `src/bin/nativelink.rs:583` does.
fn register_as_production_does(store: Arc<VerifyStore>) -> MetricsRegistry {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(CAS_STORE_NAME, Store::new(store));
    let registry = MetricsRegistry::new();
    registry.register("nativelink", store_manager);
    registry
}

/// Returns the store AND its inner handle, so a test can plant bytes BELOW
/// the verification layer (the only way to produce a genuinely-corrupt READ,
/// since the write side would reject the same blob).
fn verify_store_with_inner() -> (Arc<VerifyStore>, Arc<MemoryStore>) {
    let inner = MemoryStore::new(&MemorySpec::default());
    let store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            // Production values: buildcache-native.json5:190-191.
            verify_size: true,
            verify_hash: true,
        },
        Store::new(inner.clone()),
    );
    (store, inner)
}

fn verify_store() -> Arc<VerifyStore> {
    verify_store_with_inner().0
}

#[nativelink_test]
async fn verify_store_counters_render_under_the_production_prefix() -> Result<(), Error> {
    pin_production_default_blake3();
    let (store, inner) = verify_store_with_inner();

    // Drive the counters through the REAL write path, not a setter: one
    // proven write (sha256-keyed blob labelled blake3 — the live FL-1786
    // shape) and one genuine corruption.
    let proven_digest = DigestInfo::try_new(SHA256_OF_VALUE, VALUE.len() as u64)?;
    store
        .update_oneshot(proven_digest, VALUE.into())
        .instrument(info_span!("proven_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await
        .expect("#fl1786: the proving write must succeed; without it the counter renders 0");
    let _ = store
        .update_oneshot(
            DigestInfo::try_new(SHA256_OF_OTHER, VALUE.len() as u64)?,
            VALUE.into(),
        )
        .instrument(info_span!("corrupt_write"))
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await;

    // `#fl1786-read-half`: and the same for the READ side — one read rescued
    // by proving (no ambient context, so blake3 is resolved against a
    // sha256-keyed blob) and one genuinely corrupt read. The corrupt bytes go
    // straight to the inner store because the write side would reject them.
    store
        .get_part_unchunked(proven_digest, 0, None)
        .await
        .expect("#fl1786-read-half: the proving read must succeed; else its counter renders 0");
    let corrupt_digest = DigestInfo::try_new(SHA256_OF_OTHER, VALUE.len() as u64)?;
    inner.update_oneshot(corrupt_digest, VALUE.into()).await?;
    let _ = store.get_part_unchunked(corrupt_digest, 0, None).await;

    let body = render_prometheus(&register_as_production_does(store));

    for name in EXPECTED_METRIC_NAMES {
        assert!(
            body.contains(&format!("\n{name} ")),
            "#fl1786: metric `{name}` is NOT in the rendered /metrics body. Either the field was \
             renamed, its `#[metric]` publish was dropped, or `StoreManager` stopped being \
             registered under `nativelink` (src/bin/nativelink.rs:583). VerifyStore is the site \
             that closes the OBSERVED latch — all 591 live rejections were on its path — so a \
             dark counter here is a dark counter on the only site that has ever fired. \
             body=\n{body}"
        );
    }

    assert!(
        body.contains("\nnativelink_cas_STORE_digest_func_proven_counter 1\n"),
        "#fl1786: `digest_func_proven_counter` must render the LIVE counter value (exactly one write was \
         proven above), not a constant. A metric that always renders 0 is indistinguishable from \
         a mechanism that never fires — which is precisely what this counter exists to \
         disambiguate, and the reason the whole engaged-mechanism argument for this change \
         rests on it. body=\n{body}"
    );
    assert!(
        body.contains("\nnativelink_cas_STORE_hash_verification_failures_counter 2\n"),
        "#fl1786: `hash_verification_failures_counter` must render the LIVE value (one genuinely \
         corrupt WRITE plus one genuinely corrupt READ above — it is the AGGREGATE and the \
         read-side split is a decomposition of it, not a move). It is the data-integrity alarm \
         and proving must not silence it; if it renders 0 or 1 here the fail-closed path stopped \
         being attributable on /metrics. body=\n{body}"
    );
    assert!(
        body.contains("\nnativelink_cas_STORE_digest_func_proven_on_read_counter 1\n"),
        "#fl1786-read-half: `digest_func_proven_on_read_counter` must render the LIVE value \
         (exactly one read was rescued by proving above). This is the ONLY engaged-mechanism \
         signal for the read half — a rescued read is by construction indistinguishable from an \
         ordinary successful read everywhere else — so a dark counter here means the read half's \
         entire safety argument rests on something nobody can observe. body=\n{body}"
    );
    assert!(
        body.contains("\nnativelink_cas_STORE_hash_verification_failures_on_read_counter 1\n"),
        "#fl1786-read-half: `hash_verification_failures_on_read_counter` must render the LIVE \
         value (exactly one unprovable read above). Without it `hash_verification_failures` \
         carries two roles and an operator cannot tell a rejected corrupt WRITE from a failed \
         READ — which is why the falsifier both review rounds proposed for this class was not \
         implementable. body=\n{body}"
    );
    Ok(())
}

/// Completeness: the pinned list must cover every metric `VerifyStore`
/// publishes. Without it, a field added later renders but stays unpinned and
/// a future rename of it goes unnoticed.
#[nativelink_test]
async fn every_rendered_verify_store_metric_is_pinned_by_this_file() {
    pin_production_default_blake3();
    let body = render_prometheus(&register_as_production_does(verify_store()));

    let rendered: Vec<&str> = body
        .lines()
        .filter(|l| l.starts_with("nativelink_cas_STORE_"))
        // The wrapped inner store publishes under `..._inner_store_*`; this
        // file's contract is VerifyStore's OWN fields.
        .filter(|l| !l.contains("_inner_store_"))
        .filter_map(|l| l.split(' ').next())
        .collect();
    assert!(
        !rendered.is_empty(),
        "#fl1786: NOTHING rendered under the production `nativelink_cas_STORE_` prefix, so the \
         pinning test above would pass vacuously. Either StoreManager's `#[metric] stores` \
         publish was dropped or the registration shape changed"
    );
    for name in rendered {
        assert!(
            EXPECTED_METRIC_NAMES.contains(&name),
            "#fl1786: `{name}` renders on /metrics but is not in EXPECTED_METRIC_NAMES. Add it \
             there so a later rename or deletion of it red-fails the pinning test above; an \
             unpinned metric name is a silent contract with every dashboard that reads it"
        );
    }
}
