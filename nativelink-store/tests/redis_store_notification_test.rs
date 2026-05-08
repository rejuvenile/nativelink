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

//! Integration tests for [`RedisStore::register_item_callback`].
//!
//! Spec (derived from the bug report, NOT from reading the implementation):
//!
//! 1. When Redis removes a key in our `key_prefix` (via `DEL`, `PEXPIRE`,
//!    or `maxmemory-policy` eviction), each registered `ItemCallback` MUST
//!    receive a `StoreKey` whose VARIANT and VALUE round-trip the original
//!    `StoreKey` we stored under. For a digest-shaped key (the CAS path),
//!    that variant MUST be `StoreKey::Digest`, NOT `StoreKey::Str` —
//!    `StoreKey`'s `Hash` impl salts by variant, so an `ExistenceCacheStore`
//!    that inserted under `Digest` will never invalidate from a `Str` event.
//! 2. Foreign-prefix keys MUST NOT trigger a callback.
//! 3. Multiple callbacks MUST all fire.
//! 4. Production-composition: an `ExistenceCacheStore` wrapping a
//!    real `RedisStore` MUST drop its cached positive within a few seconds
//!    when Redis evicts the key under `maxmemory` pressure. This is the
//!    canonical #100 test — the very bug the dispatcher exists to fix.
//! 5. (Fix-up regression for BLOCKER 3) DEL of a non-digest-shaped key
//!    inside our prefix (e.g. an admin `DEL cas:debug-foo`) MUST NOT
//!    propagate to any registered `ItemCallback`. The dispatcher drops
//!    non-digest payloads in `parse_keyspace_payload`. Without the filter
//!    we'd dispatch `StoreKey::Str(...)` whose blake3-hashed digest never
//!    matches any cache entry — pure noise plus a cross-tenant info leak
//!    on shared Redis.
//!
//! Gated behind the `redis-integration-tests` Cargo feature so the default
//! test invocation on dev boxes without `valkey-server` still passes. CI
//! enables this feature.

#![cfg(feature = "redis-integration-tests")]

use core::future::Future;
use core::pin::Pin;
use core::time::Duration;
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{ExistenceCacheSpec, NoopSpec, RedisSpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::redis_store::{RedisManager, RedisStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{ItemCallback, Store, StoreDriver, StoreKey, StoreLike};
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tokio::sync::Notify;

const TEST_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Either `valkey-server` or `redis-server` — we don't care which, as long
/// as it speaks RESP3 and supports keyspace notifications. Required by the
/// `redis-integration-tests` feature; a missing binary is a hard failure
/// (NOT a silent skip — silent skips erase test coverage in CI).
fn server_binary() -> &'static str {
    for candidate in &["valkey-server", "redis-server"] {
        if std::process::Command::new(candidate)
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return candidate;
        }
    }
    panic!("redis-integration-tests feature requires valkey-server or redis-server on $PATH");
}

fn allocate_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local_addr").port()
}

/// Async-aware child guard. Drop does `start_kill` + non-blocking `try_wait`
/// (does NOT block the runtime).
struct ChildGuard {
    child: Option<Child>,
    _tmpdir: TempDir,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Fire-and-forget kill: start_kill is async-friendly.
            drop(child.start_kill());
            // Non-blocking wait. Linux reaps the zombie via SIGCHLD; if it
            // hasn't been reaped yet that's fine, the OS will get to it.
            drop(child.try_wait());
        }
    }
}

/// Spawn a `valkey-server` / `redis-server` subprocess on a free port,
/// optionally with extra args (e.g. `--maxmemory 8mb`). Polls PING with a
/// 5s deadline (no flat sleep).
async fn spawn_server(extra_args: &[&str]) -> (u16, ChildGuard) {
    let server = server_binary();
    let tmpdir = TempDir::new().expect("tempdir");
    let port = allocate_port();
    let mut cmd = Command::new(server);
    cmd.arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .arg("--dir")
        .arg(tmpdir.path())
        .arg("--daemonize")
        .arg("no")
        .arg("--protected-mode")
        .arg("no")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for a in extra_args {
        cmd.arg(a);
    }
    let child = cmd.spawn().expect("spawn redis/valkey");
    let guard = ChildGuard {
        child: Some(child),
        _tmpdir: tmpdir,
    };

    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).expect("client");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut conn) = client.get_connection_manager().await {
            use redis::AsyncCommands;
            if conn.ping::<String>().await.is_ok() {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "redis/valkey did not start within 5s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (port, guard)
}

/// Captures every key the dispatcher passes us. `Notify` wakes the test
/// each time a callback fires.
#[derive(Debug)]
struct CapturingCallback {
    received: Mutex<Vec<StoreKey<'static>>>,
    notify: Arc<Notify>,
}

impl CapturingCallback {
    fn new() -> (Arc<Self>, Arc<Notify>) {
        let notify = Arc::new(Notify::new());
        let cb = Arc::new(Self {
            received: Mutex::new(Vec::new()),
            notify: notify.clone(),
        });
        (cb, notify)
    }
}

impl ItemCallback for CapturingCallback {
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let owned = store_key.into_owned();
        let notify = self.notify.clone();
        Box::pin(async move {
            self.received.lock().push(owned);
            notify.notify_waiters();
        })
    }
}

/// Wait until `predicate(cb.received)` becomes true, or `timeout_for`
/// elapses. Wakeup is `Notify`-driven (no flat sleep loop).
async fn wait_for(
    notify: Arc<Notify>,
    cb: &CapturingCallback,
    timeout_for: Duration,
    mut predicate: impl FnMut(&[StoreKey<'static>]) -> bool,
) {
    let deadline = tokio::time::Instant::now() + timeout_for;
    loop {
        if predicate(&cb.received.lock()) {
            return;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "predicate not satisfied within {timeout_for:?}; received={:?}",
                cb.received.lock()
            );
        }
        let _ = tokio::time::timeout(remaining, notify.notified()).await;
    }
}

fn make_spec(port: u16, key_prefix: &str) -> RedisSpec {
    RedisSpec {
        addresses: vec![format!("redis://127.0.0.1:{port}/")],
        key_prefix: key_prefix.to_string(),
        command_timeout_ms: 5_000,
        connection_timeout_ms: 5_000,
        enable_keyspace_notifications: true,
        keyspace_notifications_db: 0,
        ..Default::default()
    }
}

/// Poll `PUBSUB NUMPAT` until at least `expected` patterns are subscribed.
/// This is the synchronization point that replaces a flat `sleep`: we block
/// until the dispatcher has actually attached its psubscribes to the server.
async fn wait_until_subscribed(port: u16, expected: u64, timeout_for: Duration) {
    let mut conn = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    let deadline = std::time::Instant::now() + timeout_for;
    loop {
        let count: u64 = redis::cmd("PUBSUB")
            .arg("NUMPAT")
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        if count >= expected {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("PUBSUB NUMPAT={count} < expected={expected} after {timeout_for:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Issue a DEL on a separate connection.
async fn raw_del(port: u16, key: &str) {
    let mut client = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    use redis::AsyncCommands;
    let _: i64 = client.del(key).await.expect("del");
}

/// `SET key value` on a separate connection. Used to seed a key before
/// DEL so the DEL fires a `__keyevent@0__:del` (Valkey emits no
/// keyevent for DEL of a non-existent key).
async fn raw_set(port: u16, key: &str, value: &str) {
    let mut conn = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    use redis::AsyncCommands;
    let _: () = conn.set(key, value).await.expect("set");
}

/// `CONFIG SET notify-keyspace-events <flags>` on a separate connection.
/// Used by the BLOCKER 4 reconnect test to simulate a Valkey restart's
/// effect of wiping the in-memory `notify-keyspace-events` config.
async fn raw_config_set_notify_keyspace_events(port: u16, flags: &str) {
    let mut conn = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("notify-keyspace-events")
        .arg(flags)
        .query_async(&mut conn)
        .await
        .expect("CONFIG SET notify-keyspace-events");
}

/// Read back `notify-keyspace-events` to confirm the post-reconnect state.
async fn raw_config_get_notify_keyspace_events(port: u16) -> String {
    let mut conn = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    let map: std::collections::HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("notify-keyspace-events")
        .query_async(&mut conn)
        .await
        .expect("CONFIG GET notify-keyspace-events");
    map.get("notify-keyspace-events").cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Spec test 1: DEL of a digest-shaped key fires Digest variant.
// This is the regression test for the v1 type-mismatch bug — assert on
// equality with StoreKey::Digest, not as_str().
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn del_digest_key_fires_callback_with_digest_variant() -> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let store = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("store");

    let (cb, notify) = CapturingCallback::new();
    store
        .clone()
        .register_item_callback(cb.clone())
        .expect("register");

    // give dispatcher init time to PSUBSCRIBE before our DEL
    wait_until_subscribed(port, 3, Duration::from_secs(3)).await;

    let digest = DigestInfo::try_new(TEST_HASH, 7).unwrap();
    let original_key: StoreKey<'static> = StoreKey::Digest(digest);
    store
        .update_oneshot(digest, Bytes::from_static(b"data"))
        .await
        .expect("update");

    // Build the on-wire key the way RedisStore::encode_key would, then
    // delete via raw connection.
    let raw = format!("cas:{digest}");
    raw_del(port, &raw).await;

    wait_for(notify, &cb, Duration::from_secs(2), |keys| {
        !keys.is_empty()
    })
    .await;
    let received = cb.received.lock().clone();
    assert_eq!(
        received.len(),
        1,
        "expected exactly one event, got {received:?}"
    );
    // STRICT equality — catches Str-vs-Digest variant mismatch.
    assert_eq!(
        received[0], original_key,
        "variant mismatch (Str vs Digest)"
    );
    // Belt-and-suspenders: also assert the variant directly.
    assert!(
        matches!(received[0], StoreKey::Digest(_)),
        "expected Digest variant"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 2: foreign-prefix DEL must NOT fire callback.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn foreign_prefix_does_not_fire_callback() -> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let store = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("store");

    let (cb, _notify) = CapturingCallback::new();
    store
        .clone()
        .register_item_callback(cb.clone())
        .expect("register");
    wait_until_subscribed(port, 3, Duration::from_secs(3)).await;

    let mut client = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    use redis::AsyncCommands;
    let _: () = client.set("ac:other-tenant", "v").await.expect("set");
    let _: i64 = client.del("ac:other-tenant").await.expect("del");

    // Drain any in-flight notifications by issuing a positive control —
    // a DEL on our prefix that the dispatcher MUST report. If the
    // foreign-prefix event were going to fire, it would arrive before
    // (or alongside) the positive control.
    //
    // The positive control MUST be digest-shaped: per BLOCKER 3 fix,
    // `parse_keyspace_payload` only forwards `StoreKey::Digest` to callbacks
    // (non-digest payloads are dropped at the parser to avoid foreign-tenant
    // info leaks and Str-vs-Digest hash-bucket misses).
    let positive_digest = DigestInfo::try_new(TEST_HASH, 999).unwrap();
    let positive = format!("cas:{positive_digest}");
    let _: () = client.set(&positive, "v").await.expect("set");
    let _: i64 = client.del(&positive).await.expect("del");
    // wait for the positive control to land
    let cb_ref = cb.clone();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if cb_ref
                .received
                .lock()
                .iter()
                .any(|k| matches!(k, StoreKey::Digest(d) if *d == positive_digest))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("positive control did not fire");

    // Now assert the foreign-prefix event is NOT in the captured set.
    let captured = cb.received.lock();
    assert!(
        captured.iter().all(|k| !matches!(k, StoreKey::Str(s) if s.as_ref() == "other-tenant")),
        "foreign prefix triggered callback: {captured:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 3: multi-callback. N>1 callbacks all fire.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn multiple_callbacks_all_fire() -> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let store = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("store");

    let (cb_a, notify_a) = CapturingCallback::new();
    let (cb_b, notify_b) = CapturingCallback::new();
    let (cb_c, notify_c) = CapturingCallback::new();
    store.clone().register_item_callback(cb_a.clone()).expect("a");
    store.clone().register_item_callback(cb_b.clone()).expect("b");
    store.clone().register_item_callback(cb_c.clone()).expect("c");
    wait_until_subscribed(port, 3, Duration::from_secs(3)).await;

    let digest = DigestInfo::try_new(TEST_HASH, 11).unwrap();
    store
        .update_oneshot(digest, Bytes::from_static(b"y"))
        .await
        .expect("update");
    raw_del(port, &format!("cas:{digest}")).await;

    wait_for(notify_a, &cb_a, Duration::from_secs(2), |k| !k.is_empty()).await;
    wait_for(notify_b, &cb_b, Duration::from_secs(2), |k| !k.is_empty()).await;
    wait_for(notify_c, &cb_c, Duration::from_secs(2), |k| !k.is_empty()).await;

    let expected: StoreKey<'static> = StoreKey::Digest(digest);
    assert_eq!(cb_a.received.lock()[0], expected);
    assert_eq!(cb_b.received.lock()[0], expected);
    assert_eq!(cb_c.received.lock()[0], expected);
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 4: PEXPIRE -> expired event fires callback with Digest variant.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn expired_key_fires_callback() -> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let store = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("store");

    let (cb, notify) = CapturingCallback::new();
    store
        .clone()
        .register_item_callback(cb.clone())
        .expect("register");
    wait_until_subscribed(port, 3, Duration::from_secs(3)).await;

    let digest = DigestInfo::try_new(TEST_HASH, 17).unwrap();
    store
        .update_oneshot(digest, Bytes::from_static(b"z"))
        .await
        .expect("update");

    // Set a 1ms TTL via raw connection. Redis fires `expired` keyevents
    // on the next access OR via background expiration cycle.
    let mut client = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    use redis::AsyncCommands;
    let raw = format!("cas:{digest}");
    let _: bool = client.pexpire(&raw, 1).await.expect("pexpire");

    wait_for(notify, &cb, Duration::from_secs(3), |k| !k.is_empty()).await;
    let received = cb.received.lock()[0].clone();
    assert_eq!(
        received,
        StoreKey::Digest(digest),
        "expected Digest variant on expire"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 5: maxmemory eviction fires callback with Digest variant.
// Most damning regression: this is the production failure mode.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn evicted_key_fires_callback() -> Result<(), Error> {
    // 8 MiB cap; 1 MiB was too tight (RedisStore's chunked-write protocol
    // builds temp keys that themselves OOM before commit).
    let (port, _guard) = spawn_server(&[
        "--maxmemory",
        "8mb",
        "--maxmemory-policy",
        "allkeys-lru",
    ])
    .await;
    let store = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("store");

    let (cb, notify) = CapturingCallback::new();
    store
        .clone()
        .register_item_callback(cb.clone())
        .expect("register");
    wait_until_subscribed(port, 3, Duration::from_secs(3)).await;

    // Seed the target with a tiny payload so it's the LRU victim once the
    // larger blobs flood in.
    let target_digest = DigestInfo::try_new(TEST_HASH, 4).unwrap();
    store
        .update_oneshot(target_digest, Bytes::from_static(b"seed"))
        .await
        .expect("seed");

    // Flood with distinct large blobs. Stop early if the target evicts.
    for i in 0u64..400 {
        if cb
            .received
            .lock()
            .iter()
            .any(|k| matches!(k, StoreKey::Digest(d) if *d == target_digest))
        {
            break;
        }
        let mut hash = String::with_capacity(64);
        for b in 0..32 {
            hash.push_str(&format!("{:02x}", ((i + b as u64) & 0xff) as u8));
        }
        let d = DigestInfo::try_new(&hash, 65536).unwrap();
        // OOM in the middle of the flood is acceptable — we already
        // pressured the cache enough to evict.
        if store
            .update_oneshot(d, Bytes::from(vec![(i & 0xff) as u8; 65536]))
            .await
            .is_err()
        {
            break;
        }
    }

    wait_for(notify, &cb, Duration::from_secs(5), move |keys| {
        keys.iter()
            .any(|k| matches!(k, StoreKey::Digest(d) if *d == target_digest))
    })
    .await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 6 (#100, canonical): production composition.
// ExistenceCacheStore wrapping a real RedisStore must drop its cached
// positive within `tokio::time::timeout(5s)` of a Redis eviction.
// Without the dispatcher this test deadlocks at `wait_for_cache_drop`.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn existence_cache_drops_positive_after_redis_eviction() -> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let redis = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("redis");
    let inner = Store::new(redis.clone());
    let ec_spec = ExistenceCacheSpec {
        // backend is informational only here — the real wrapping is via the
        // `inner_store: Store` parameter to ExistenceCacheStore::new.
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: None,
    };
    // Wrap the redis store directly. ExistenceCacheStore::new_with_time
    // calls inner_store.register_item_callback for us — so wiring through
    // a single `ExistenceCacheStore::new_with_time` constructor is enough
    // to verify the back-edge.
    let ec = ExistenceCacheStore::new(&ec_spec, inner);

    // Seed the digest into Redis (and into the ExistenceCache via the
    // standard public path).
    let digest = DigestInfo::try_new(TEST_HASH, 5).unwrap();
    ec.clone()
        .update_oneshot(digest, Bytes::from_static(b"hello"))
        .await
        .expect("ec update");

    // Sanity check: the cache says "yes" before eviction.
    assert!(
        ec.exists_in_cache(&digest).await,
        "cache must hold positive after update"
    );

    // Now PSUBSCRIBE must be live (the ExistenceCacheStore's call to
    // register_item_callback was async; wait until the dispatcher's
    // 3 keyevent patterns are attached).
    wait_until_subscribed(port, 3, Duration::from_secs(5)).await;

    // Evict the key directly via raw connection.
    raw_del(port, &format!("cas:{digest}")).await;

    // Within `tokio::time::timeout` (CLAUDE.md production-composition
    // contract: bespoke message identifies the contract being guarded).
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !ec.exists_in_cache(&digest).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect(
        "must not deadlock — ExistenceCacheStore retained stale positive after Redis eviction",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 5 (BLOCKER 3 fix-up regression): non-digest-shaped key under our
// prefix MUST NOT propagate to ItemCallbacks.
//
// The pre-fix `parse_keyspace_payload` returned `StoreKey::Str(...)` as a
// fallback for any non-digest body. Downstream `ExistenceCacheStore::callback`
// calls `into_digest()` which blake3-hashes the bytes — the resulting
// synthetic digest never matches anything actually inserted (cache inserts go
// through `From<DigestInfo>`), so the callback churns CPU + log spam + leaks
// foreign-tenant key bytes into our boundary, all for a guaranteed no-op
// remove.
//
// Mutation step: revert `parse_keyspace_payload` to the old fall-through-to-Str
// behavior; this test must FAIL (CapturingCallback receives the foreign key).
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn non_digest_key_under_prefix_does_not_fire_callback() -> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let store = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("store");

    let (cb, _notify) = CapturingCallback::new();
    store
        .clone()
        .register_item_callback(cb.clone())
        .expect("register");
    wait_until_subscribed(port, 3, Duration::from_secs(3)).await;

    // Issue a DEL on a non-digest-shaped key INSIDE the prefix the dispatcher
    // is listening for. Pre-fix: dispatcher would forward it as
    // `StoreKey::Str("debug-foo")`. Post-fix: dropped at the parser.
    let mut client = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .expect("client")
        .get_connection_manager()
        .await
        .expect("conn");
    use redis::AsyncCommands;
    let _: () = client.set("cas:debug-foo", "v").await.expect("set");
    let _: i64 = client.del("cas:debug-foo").await.expect("del");

    // Drain any in-flight notifications by issuing a positive control —
    // a digest-shaped DEL on our prefix that the dispatcher MUST report.
    let digest = DigestInfo::try_new(TEST_HASH, 9).unwrap();
    let positive = format!("cas:{digest}");
    let _: () = client.set(&positive, "v").await.expect("set");
    let _: i64 = client.del(&positive).await.expect("del");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if cb
                .received
                .lock()
                .iter()
                .any(|k| matches!(k, StoreKey::Digest(d) if *d == digest))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("positive control did not fire");

    // Now assert the non-digest event is NOT in the captured set.
    let captured = cb.received.lock();
    assert!(
        captured
            .iter()
            .all(|k| !matches!(k, StoreKey::Str(s) if s.as_ref() == "debug-foo")),
        "non-digest key under prefix dispatched StoreKey::Str — \
         parse_keyspace_payload filter regressed: {captured:?}"
    );
    // Belt-and-suspenders: the captured set should contain ONLY the positive
    // control digest. No Str variants at all.
    assert!(
        captured.iter().all(|k| matches!(k, StoreKey::Digest(_))),
        "non-Digest variant leaked into dispatch: {captured:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 6 (BLOCKER 5 fix-up regression): startup-check refuses the
// keyspace-dispatcher + scheduler-subscription collision.
//
// Both consumers want the single Redis push-sender channel. Whichever wins
// the `take()` race silently disables the other. `set_spec_defaults` rejects
// the combination at construction so the operator sees the conflict at
// deploy time, not as silent stale-positive caching after a race.
//
// Mutation step: comment out the `experimental_pub_sub_channel.is_some()`
// branch in `set_spec_defaults`; this test must FAIL (no Err returned).
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn keyspace_and_scheduler_collision_refused_at_construction() -> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let mut spec = make_spec(port, "cas:");
    spec.experimental_pub_sub_channel = Some("scheduler-channel".to_string());
    // Both `enable_keyspace_notifications=true` (default in make_spec) and
    // `experimental_pub_sub_channel=Some(...)` set ⇒ collision.
    let err = RedisStore::new_standard(spec)
        .await
        .expect_err("expected collision rejection");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("mutually exclusive") || msg.contains("subscriber_channel"),
        "expected collision rejection error, got: {msg}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 7 (BLOCK-1 fix-up regression): URL-vs-config db cross-check
// works for the production URL form `redis+unix:///path?db=N`.
//
// Spec (derived from dsr BLOCK-1 finding):
// - Production buildcache uses `redis+unix:///run/valkey/valkey.sock?db=1` for
//   the small-CAS Redis store. The redis crate parses the db from the
//   `?db=N` query-pair for unix sockets.
// - The previous cross-check used `Url::path_segments()` only, which yields
//   `["run", "valkey", "valkey.sock"]` for the unix form — first segment
//   "run" is not a u8, so the check silently passed.
// - The fix uses `redis::IntoConnectionInfo`, the same call
//   `RedisStore::connect` performs, so the validator and runtime cannot
//   disagree.
//
// Test matrix (asserts BOTH directions of the contract):
// - `url_db_unix_query_param_mismatch_rejected`: `redis+unix:///path?db=1`
//   with `keyspace_notifications_db: 0` → Err. This is the case that
//   silently passed pre-fix; the regression that ships the production
//   wedge.
// - `url_db_tcp_path_segment_mismatch_rejected`: `redis://host/3` with
//   `keyspace_notifications_db: 0` → Err. This worked pre-fix and must
//   keep working.
// - `url_db_tcp_match_accepts_construction`: `redis://host/0` with
//   `keyspace_notifications_db: 0` → Ok (positive control).
//
// Mutation step: revert `set_spec_defaults` to the path_segments-only
// version (or any narrower form). The unix-mismatch test must FAIL with
// the bespoke "expected url_db_unix mismatch rejection" message.
// ---------------------------------------------------------------------------

/// Build a `RedisSpec` whose `addresses[0]` is set to the supplied URL,
/// with keyspace notifications enabled and `keyspace_notifications_db`
/// configured per the caller. Used for URL-form cross-check tests.
fn make_spec_with_url(url: &str, keyspace_db: u8) -> RedisSpec {
    RedisSpec {
        addresses: vec![url.to_string()],
        key_prefix: "cas:".to_string(),
        command_timeout_ms: 5_000,
        connection_timeout_ms: 5_000,
        enable_keyspace_notifications: true,
        keyspace_notifications_db: keyspace_db,
        ..Default::default()
    }
}

#[nativelink_test]
async fn url_db_unix_query_param_mismatch_rejected() -> Result<(), Error> {
    // Synthetic unix-socket URL with `?db=1` query — production form
    // (`redis+unix:///run/valkey/valkey.sock?db=1`). Use a deliberately
    // non-existent socket path so that any future mutation removing the
    // cross-check would surface a deterministic "no such file" error
    // post-validation, easy to distinguish from the validator's bespoke
    // FailedPrecondition rejection.
    let spec = make_spec_with_url(
        "redis+unix:///nonexistent/100-fixup-test/valkey.sock?db=1",
        0,
    );
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        RedisStore::new_standard(spec),
    )
    .await
    .expect("must not deadlock — set_spec_defaults is synchronous validation");
    let err = result.expect_err("expected url_db_unix mismatch rejection");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("connection URL resolves to db=1")
            && msg.contains("keyspace_notifications_db=0"),
        "expected unix-form URL/db mismatch rejection naming both db values, got: {msg}"
    );
    Ok(())
}

#[nativelink_test]
async fn url_db_tcp_path_segment_mismatch_rejected() -> Result<(), Error> {
    // TCP URL with `/3` path-segment db. Same flow as above — synchronous
    // validation, no live server needed. This case worked pre-fix; the
    // assertion guards against regressing it during the unix-form fix.
    let spec = make_spec_with_url("redis://127.0.0.1:6379/3", 0);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        RedisStore::new_standard(spec),
    )
    .await
    .expect("must not deadlock — set_spec_defaults is synchronous validation");
    let err = result.expect_err("expected url_db_tcp mismatch rejection");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("connection URL resolves to db=3")
            && msg.contains("keyspace_notifications_db=0"),
        "expected tcp-form URL/db mismatch rejection naming both db values, got: {msg}"
    );
    Ok(())
}

#[nativelink_test]
async fn url_db_tcp_match_accepts_construction() -> Result<(), Error> {
    // Positive control: when URL db matches the configured
    // `keyspace_notifications_db`, construction succeeds. We DO need a live
    // server for this one because successful `set_spec_defaults` proceeds
    // into `init_keyspace_dispatcher_eager` which CONFIG SETs + PSUBSCRIBEs
    // against the real Valkey.
    let (port, _guard) = spawn_server(&[]).await;
    let spec = make_spec_with_url(&format!("redis://127.0.0.1:{port}/0"), 0);
    let store = tokio::time::timeout(
        Duration::from_secs(5),
        RedisStore::new_standard(spec),
    )
    .await
    .expect("must not deadlock")
    .expect("matching url-db must construct OK");
    // Drop the store explicitly so the dispatcher exits before
    // `_guard` kills the server.
    drop(store);
    Ok(())
}

// ---------------------------------------------------------------------------
// Spec test 8 (BLOCKER 4 regression — testing-czar F3 / red-team B):
// `StandardRedisManager::reconnect_with` MUST re-issue
// `CONFIG SET notify-keyspace-events <flags>` on every subscriber-slot
// reconnect BEFORE replaying PSUBSCRIBE.
//
// Spec (derived from the bug report, NOT from reading the implementation):
// Redis `CONFIG SET` mutates the in-memory config only. A Valkey
// restart reverts to whatever is in `valkey.conf` (typically empty for
// notify-keyspace-events). Per Q2=NO, operators do NOT persist
// notify-keyspace-events in `valkey.conf` — so without runtime re-issue
// on every reconnect the dispatcher silently sees zero events post-
// restart, re-emerging the wedge the dispatcher exists to close.
//
// Test approach: simulate the restart effect (wipe in-memory config)
// then trigger a reconnect on the subscriber slot directly via
// `manager.reconnect(uuid)`. Without the BLOCKER 4 fix, the wipe
// persists; the post-reconnect `CONFIG GET` returns "" and DELs do not
// fire the callback. With the fix, the post-reconnect `CONFIG GET`
// returns the original flags ("Egex" or whatever was merged at
// init_keyspace_dispatcher_eager) and DELs continue to fire callbacks.
//
// Mutation step: comment out the `let flags = ...; if let Some(flags) =
// flags { CONFIG SET ... }` block in `reconnect_with`
// (`redis_store.rs:606-620`). The test must FAIL with the bespoke
// "BLOCKER 4 regression — DEL after reconnect did not fire callback"
// message because the wiped notify-keyspace-events flags were never
// restored after the simulated restart.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn config_set_notify_keyspace_events_reissued_on_subscriber_reconnect()
-> Result<(), Error> {
    let (port, _guard) = spawn_server(&[]).await;
    let store = RedisStore::new_standard(make_spec(port, "cas:"))
        .await
        .expect("store");

    let (cb, notify) = CapturingCallback::new();
    store
        .clone()
        .register_item_callback(cb.clone())
        .expect("register");
    wait_until_subscribed(port, 3, Duration::from_secs(3)).await;

    // Sanity: at this point notify-keyspace-events is non-empty (init
    // ran CONFIG SET). Capture the live flags so we can assert they're
    // restored post-reconnect.
    let flags_before = raw_config_get_notify_keyspace_events(port).await;
    assert!(
        !flags_before.is_empty(),
        "init_keyspace_dispatcher_eager should have CONFIG SET non-empty flags"
    );

    // Simulate the Valkey-restart effect: wipe the in-memory
    // notify-keyspace-events config. Operators per Q2=NO do not persist
    // this in valkey.conf, so the next reconnect MUST re-issue the
    // CONFIG SET or the dispatcher goes silently dormant.
    raw_config_set_notify_keyspace_events(port, "").await;
    assert_eq!(
        raw_config_get_notify_keyspace_events(port).await,
        "",
        "wipe must take effect"
    );

    // Trigger a reconnect on the subscriber slot directly. This is the
    // SAME path that `ClientWithPermit::reconnect` invokes when a store
    // operation hits a transport error post-restart, so the test
    // exercises the production code path (not a test-only shortcut).
    let manager = store.connection_manager();
    let subscriber_slot_uuid = {
        let slot = manager
            .debug_read_slot(0)
            .await
            .expect("subscriber slot exists");
        slot.1
    };
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        manager.reconnect(subscriber_slot_uuid),
    )
    .await
    .expect("must not deadlock — reconnect")
    .expect("reconnect must succeed");

    // Post-reconnect, BLOCKER 4's `CONFIG SET` re-issue should have
    // restored the wiped flags. If the test assertion below fails,
    // either the re-issue path is broken OR the reissued flags differ
    // from the originally-merged ones.
    let flags_after = raw_config_get_notify_keyspace_events(port).await;
    assert_eq!(
        flags_after, flags_before,
        "BLOCKER 4 regression — post-reconnect notify-keyspace-events differs from \
         pre-wipe value; CONFIG SET re-issue path is broken (live flags lost across \
         reconnect)"
    );

    // End-to-end behavior check: a DEL on the post-reconnect server
    // must fire the dispatcher callback. If `notify-keyspace-events`
    // were still wiped (BLOCKER 4 fix broken), Valkey would emit no
    // keyevent. With the fix, flags were restored above and DEL fires
    // a `__keyevent@0__:del` push that the post-reconnect subscriber
    // slot receives.
    //
    // We poll-DEL inside a deadline because the server may take a
    // brief moment to process the OLD connection's disconnect after
    // `manager.reconnect` replaces it; during that window the OLD
    // conn's PSUBSCRIBE may still match keyevent publishes that go to
    // the dropped sender. Re-issuing DEL until the NEW conn observes
    // it is the right resilience pattern for this race.
    let digest = DigestInfo::try_new(TEST_HASH, 99).unwrap();
    let key_to_del = format!("cas:{digest}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let predicate = |keys: &[StoreKey<'static>]| {
        keys.iter()
            .any(|k| matches!(k, StoreKey::Digest(d) if *d == digest))
    };
    let mut fired = false;
    // Loop SET+DEL until either the callback observes the keyevent or
    // the deadline expires. Each iteration seeds the key before DEL
    // because Valkey does not emit `__keyevent@0__:del` for a DEL of a
    // non-existent key. Looping handles the OLD-conn server-side
    // disconnect race: the OLD CM's PSUBSCRIBE may still match the
    // first publish (sent to the dropped tx), but subsequent
    // publishes hit the NEW conn after server processes the
    // disconnect.
    while tokio::time::Instant::now() < deadline {
        raw_set(port, &key_to_del, "v").await;
        raw_del(port, &key_to_del).await;
        let recv_check_until = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < recv_check_until {
            if predicate(&cb.received.lock()) {
                fired = true;
                break;
            }
            let remaining =
                recv_check_until.saturating_duration_since(tokio::time::Instant::now());
            let _ = tokio::time::timeout(remaining, notify.notified()).await;
        }
        if fired {
            break;
        }
    }
    assert!(
        fired,
        "BLOCKER 4 regression — DEL after reconnect did not fire callback within 10s; \
         CONFIG SET notify-keyspace-events re-issue on subscriber-slot reconnect is \
         broken. Post-reconnect notify-keyspace-events={flags_after}, received={:?}",
        cb.received.lock()
    );
    Ok(())
}
