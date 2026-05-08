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
use nativelink_store::redis_store::RedisStore;
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
    let positive = "cas:positive-control";
    let _: () = client.set(positive, "v").await.expect("set");
    let _: i64 = client.del(positive).await.expect("del");
    // wait for the positive control to land
    let cb_ref = cb.clone();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if cb_ref
                .received
                .lock()
                .iter()
                .any(|k| matches!(k, StoreKey::Str(s) if s.as_ref() == "positive-control"))
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
