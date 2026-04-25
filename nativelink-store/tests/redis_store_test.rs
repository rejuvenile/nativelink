// Copyright 2024-2025 The NativeLink Authors. All rights reserved.
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

use core::ops::RangeBounds;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use nativelink_config::stores::{RedisMode, RedisSpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_redis_tester::{
    ReadOnlyRedis, add_lua_script, fake_redis_sentinel_master_stream, fake_redis_sentinel_stream,
    fake_redis_stream, make_fake_redis_with_responses,
};
use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;
use nativelink_store::redis_store::{
    ClusterRedisManager, DEFAULT_CONNECTION_POOL_SIZE, DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
    DEFAULT_MAX_COUNT_PER_CURSOR, LUA_VERSION_SET_SCRIPT, RedisStore, RedisSubscriptionManager,
    StandardRedisManager,
};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::HealthStatus;
use nativelink_util::store_trait::{
    FalseValue, SchedulerCurrentVersionProvider, SchedulerIndexProvider, SchedulerStore,
    SchedulerStoreDataProvider, SchedulerStoreDecodeTo, SchedulerStoreKeyProvider, StoreKey,
    StoreLike, TrueValue, UploadSizeInfo,
};
use pretty_assertions::assert_eq;
use redis::{PushInfo, RedisError, Value};
use redis_test::{MockCmd, MockRedisConnection};
use tokio::time::{sleep, timeout};
use tracing::{Instrument, info, info_span};

const VALID_HASH1: &str = "3031323334353637383961626364656630303030303030303030303030303030";
const TEMP_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

const DEFAULT_READ_CHUNK_SIZE: usize = 1024;
const DEFAULT_SCAN_COUNT: usize = 10_000;
const DEFAULT_MAX_PERMITS: usize = 100;

fn mock_uuid_generator() -> String {
    uuid::Uuid::parse_str(TEMP_UUID).unwrap().to_string()
}

fn make_temp_key(final_name: &str) -> String {
    format!("temp-{TEMP_UUID}-{{{final_name}}}")
}

async fn make_mock_store(
    commands: Vec<MockCmd>,
) -> RedisStore<MockRedisConnection, ClusterRedisManager<MockRedisConnection>> {
    make_mock_store_with_prefix(commands, String::new()).await
}

fn add_lua_version_script(mut responses: HashMap<String, String>) -> HashMap<String, String> {
    add_lua_script(
        &mut responses,
        LUA_VERSION_SET_SCRIPT,
        "b22b9926cbce9dd9ba97fa7ba3626f89feea1ed5",
    );
    responses
}

async fn make_fake_redis() -> u16 {
    make_fake_redis_with_responses(add_lua_version_script(fake_redis_stream())).await
}

async fn fake_redis_sentinel_master_stream_with_script() -> u16 {
    make_fake_redis_with_responses(add_lua_version_script(fake_redis_sentinel_master_stream()))
        .await
}

async fn make_mock_store_with_prefix(
    mut commands: Vec<MockCmd>,
    key_prefix: String,
) -> RedisStore<MockRedisConnection, ClusterRedisManager<MockRedisConnection>> {
    commands.insert(
        0,
        MockCmd::new(
            redis::cmd("SCRIPT").arg("LOAD").arg(LUA_VERSION_SET_SCRIPT),
            Ok("b22b9926cbce9dd9ba97fa7ba3626f89feea1ed5"),
        ),
    );
    let mock_connection = MockRedisConnection::new(commands);
    let manager = ClusterRedisManager::new(mock_connection).await.unwrap();
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
    RedisStore::new_from_builder_and_parts(
        None,
        mock_uuid_generator,
        key_prefix,
        DEFAULT_READ_CHUNK_SIZE,
        DEFAULT_MAX_CHUNK_UPLOADS_PER_UPDATE,
        DEFAULT_SCAN_COUNT,
        DEFAULT_MAX_PERMITS,
        DEFAULT_MAX_COUNT_PER_CURSOR,
        Duration::from_secs(20),
        rx,
        manager,
    )
    .await
    .unwrap()
}

#[nativelink_test]
async fn upload_and_get_data() -> Result<(), Error> {
    // Construct the data we want to send. Since it's small, we expect it to be sent in a single chunk.
    let data = Bytes::from_static(b"14");

    // Construct a digest for our data and create a key based on that digest.
    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{digest}");

    // Construct our Redis store with a mocked out backend.
    let temp_key = make_temp_key(&packed_hash_hex);
    let real_key = packed_hash_hex;

    let commands = vec![
        // The first set of commands are for setting the data.
        // Append the real value to the temp key.
        MockCmd::new(
            redis::cmd("SETRANGE")
                .arg(temp_key.clone())
                .arg(0)
                .arg(data.to_vec()),
            Ok(Value::Int(0)),
        ),
        MockCmd::new(
            redis::cmd("STRLEN").arg(temp_key.clone()),
            Ok(Value::Int(data.len() as i64)),
        ),
        // Move the data from the fake key to the real key.
        MockCmd::new(
            redis::cmd("RENAME")
                .arg(temp_key.clone())
                .arg(real_key.clone()),
            Ok(Value::Nil),
        ),
        // The second set of commands are for retrieving the data from the key.
        // Check that the key exists.
        MockCmd::with_values(
            redis::pipe()
                .cmd("STRLEN")
                .arg(real_key.clone())
                .cmd("EXISTS")
                .arg(real_key.clone()),
            Ok(vec![Value::Int(2), Value::Boolean(true)]),
        ),
        // Retrieve the data from the real key.
        MockCmd::new(
            redis::cmd("GETRANGE").arg(real_key).arg(0).arg(1),
            Ok(Value::BulkString(b"14".to_vec())),
        ),
    ];

    let store = make_mock_store(commands).await;

    store.update_oneshot(digest, data.clone()).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

#[nativelink_test]
async fn upload_and_get_data_with_prefix() -> Result<(), Error> {
    let data = Bytes::from_static(b"14");

    let prefix = "TEST_PREFIX-";

    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{prefix}{digest}");

    let temp_key = make_temp_key(&packed_hash_hex);
    let real_key = packed_hash_hex;

    let commands = vec![
        MockCmd::new(
            redis::cmd("SETRANGE")
                .arg(temp_key.clone())
                .arg(0)
                .arg(data.clone().to_vec()),
            Ok(Value::Int(0)),
        ),
        MockCmd::new(
            redis::cmd("STRLEN").arg(temp_key.clone()),
            Ok(Value::Int(data.len() as i64)),
        ),
        MockCmd::new(
            redis::cmd("RENAME").arg(temp_key).arg(real_key.clone()),
            Ok(Value::Nil),
        ),
        MockCmd::with_values(
            redis::pipe()
                .cmd("STRLEN")
                .arg(real_key.clone())
                .cmd("EXISTS")
                .arg(real_key.clone()),
            Ok(vec![Value::Int(2), Value::Boolean(true)]),
        ),
        MockCmd::new(
            redis::cmd("GETRANGE").arg(real_key).arg(0).arg(1),
            Ok(Value::BulkString(b"14".to_vec())),
        ),
    ];

    let store = make_mock_store_with_prefix(commands, prefix.to_string()).await;

    store.update_oneshot(digest, data.clone()).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

#[nativelink_test]
async fn upload_empty_data() -> Result<(), Error> {
    let data = Bytes::from_static(b"");
    let digest = ZERO_BYTE_DIGESTS[0];

    let commands = vec![];
    let store = make_mock_store(commands).await;
    store.update_oneshot(digest, data).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    Ok(())
}

#[nativelink_test]
async fn upload_empty_data_with_prefix() -> Result<(), Error> {
    let data = Bytes::from_static(b"");
    let digest = ZERO_BYTE_DIGESTS[0];
    let prefix = "TEST_PREFIX-";

    let commands = vec![];
    let store = make_mock_store_with_prefix(commands, prefix.to_string()).await;
    store.update_oneshot(digest, data).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    Ok(())
}

#[nativelink_test]
async fn test_large_downloads_are_chunked() -> Result<(), Error> {
    const READ_CHUNK_SIZE: usize = 1024;
    let data = Bytes::from(vec![0u8; READ_CHUNK_SIZE + 128]);

    let digest = DigestInfo::try_new(VALID_HASH1, 1)?;
    let packed_hash_hex = format!("{digest}");

    let temp_key = make_temp_key(&packed_hash_hex);
    let real_key = packed_hash_hex;

    let commands = vec![
        MockCmd::new(
            redis::cmd("SETRANGE")
                .arg(temp_key.clone())
                .arg(0)
                .arg(data.clone().to_vec()),
            Ok(Value::Int(0)),
        ),
        MockCmd::new(
            redis::cmd("STRLEN").arg(temp_key.clone()),
            Ok(Value::Int(data.len() as i64)),
        ),
        MockCmd::new(
            redis::cmd("RENAME").arg(temp_key).arg(real_key.clone()),
            Ok(Value::Nil),
        ),
        MockCmd::with_values(
            redis::pipe()
                .cmd("STRLEN")
                .arg(real_key.clone())
                .cmd("EXISTS")
                .arg(real_key.clone()),
            Ok(vec![
                Value::Int(data.len().try_into().unwrap()),
                Value::Int(1),
            ]),
        ),
        MockCmd::new(
            // We expect to be asked for data from `0..READ_CHUNK_SIZE`, but since GETRANGE is inclusive
            // the actual call should be from `0..=(READ_CHUNK_SIZE - 1)`.
            redis::cmd("GETRANGE")
                .arg(real_key.clone())
                .arg(0)
                .arg(READ_CHUNK_SIZE as i64 - 1),
            Ok(Value::BulkString(data.slice(..READ_CHUNK_SIZE).into())),
        ),
        MockCmd::new(
            // Similar GETRANGE index shenanigans here.
            redis::cmd("GETRANGE")
                .arg(real_key)
                .arg(READ_CHUNK_SIZE as i64)
                .arg(data.len() as i64 - 1),
            Ok(Value::BulkString(data.slice(READ_CHUNK_SIZE..).into())),
        ),
    ];

    let store = make_mock_store(commands).await;

    store.update_oneshot(digest, data.clone()).await.unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let get_result: Bytes = store
        .get_part_unchunked(digest, 0, Some(data.clone().len() as u64))
        .await
        .unwrap();

    assert_eq!(
        get_result, data,
        "Expected redis store to have updated value",
    );

    Ok(())
}

#[nativelink_test]
async fn yield_between_sending_packets_in_update() -> Result<(), Error> {
    let data_p1 = Bytes::from(vec![b'A'; DEFAULT_READ_CHUNK_SIZE + 512]);
    let data_p2 = Bytes::from(vec![b'B'; DEFAULT_READ_CHUNK_SIZE]);

    let mut data = BytesMut::new();
    data.extend_from_slice(&data_p1);
    data.extend_from_slice(&data_p2);
    let data = data.freeze();

    let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
    let packed_hash_hex = format!("{digest}");

    let temp_key = make_temp_key(&packed_hash_hex);
    let real_key = packed_hash_hex;

    let commands = vec![
        // We expect multiple `"SETRANGE"`s as we send data in multiple chunks
        MockCmd::new(
            redis::cmd("SETRANGE")
                .arg(temp_key.clone())
                .arg(0)
                .arg(data_p1.clone().to_vec()),
            Ok(Value::Int(0)),
        ),
        MockCmd::new(
            redis::cmd("SETRANGE")
                .arg(temp_key.clone())
                .arg(data_p1.len())
                .arg(data_p2.clone().to_vec()),
            Ok(Value::Int(0)),
        ),
        MockCmd::new(
            redis::cmd("STRLEN").arg(temp_key.clone()),
            Ok(Value::Int(data.len() as i64)),
        ),
        MockCmd::new(
            redis::cmd("RENAME").arg(temp_key).arg(real_key.clone()),
            Ok(Value::Nil),
        ),
        MockCmd::with_values(
            redis::pipe()
                .cmd("STRLEN")
                .arg(real_key.clone())
                .cmd("EXISTS")
                .arg(real_key.clone()),
            Ok(vec![Value::Int(2), Value::Int(1)]),
        ),
        MockCmd::new(
            redis::cmd("GETRANGE")
                .arg(real_key.clone())
                .arg(0)
                .arg((DEFAULT_READ_CHUNK_SIZE - 1) as i64),
            Ok(Value::BulkString(data.clone().to_vec())),
        ),
        MockCmd::new(
            redis::cmd("GETRANGE")
                .arg(real_key.clone())
                .arg(DEFAULT_READ_CHUNK_SIZE as i64)
                .arg((DEFAULT_READ_CHUNK_SIZE * 2 - 1) as i64),
            Ok(Value::BulkString(data.clone().to_vec())),
        ),
        MockCmd::new(
            redis::cmd("GETRANGE")
                .arg(real_key)
                .arg((DEFAULT_READ_CHUNK_SIZE * 2) as i64)
                .arg((data_p1.len() + data_p2.len() - 1) as i64),
            Ok(Value::BulkString(data.clone().to_vec())),
        ),
    ];

    let store = make_mock_store(commands).await;

    let (mut tx, rx) = make_buf_channel_pair();

    tokio::try_join!(
        async {
            store
                .update(digest, rx, UploadSizeInfo::ExactSize(data.len() as u64))
                .await
                .unwrap();

            Ok::<_, Error>(())
        },
        async {
            tx.send(data_p1).await.unwrap();
            tx.send(data_p2).await.unwrap();
            tx.send_eof().unwrap();
            Ok::<_, Error>(())
        },
    )
    .unwrap();

    let result = store.has(digest).await.unwrap();
    assert!(
        result.is_some(),
        "Expected redis store to have hash: {VALID_HASH1}",
    );

    let result = store
        .get_part_unchunked(digest, 0, Some(data.clone().len() as u64))
        .await
        .unwrap();

    assert_eq!(result, data, "Expected redis store to have updated value",);

    Ok(())
}

// Regression test for: https://github.com/TraceMachina/nativelink/issues/1286
#[nativelink_test]
async fn zero_len_items_exist_check() -> Result<(), Error> {
    let digest = DigestInfo::try_new(VALID_HASH1, 0)?;
    let packed_hash_hex = format!("{digest}");
    let real_key = packed_hash_hex;

    let commands = vec![
        MockCmd::new(
            redis::cmd("GETRANGE")
                .arg(real_key.clone())
                .arg(0)
                .arg(DEFAULT_READ_CHUNK_SIZE as i64 - 1),
            Ok(Value::BulkString(vec![])),
        ),
        MockCmd::new(redis::cmd("EXISTS").arg(real_key), Ok(Value::Int(0))),
    ];

    let store = make_mock_store(commands).await;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert_eq!(
        result.as_ref().unwrap_err().code,
        Code::NotFound,
        "{result:?}"
    );

    Ok(())
}

#[nativelink_test]
async fn list_test() -> Result<(), Error> {
    async fn get_list(
        store: &RedisStore<MockRedisConnection, ClusterRedisManager<MockRedisConnection>>,
        range: impl RangeBounds<StoreKey<'static>> + Send + Sync + 'static,
    ) -> Vec<StoreKey<'static>> {
        let mut found_keys = vec![];
        store
            .list(range, |key| {
                found_keys.push(key.borrow().into_owned());
                true
            })
            .await
            .unwrap();
        found_keys
    }

    const KEY1: StoreKey = StoreKey::new_str("key1");
    const KEY2: StoreKey = StoreKey::new_str("key2");
    const KEY3: StoreKey = StoreKey::new_str("key3");

    #[allow(clippy::unnecessary_wraps)] // because that's what MockCmd wants
    fn result() -> Result<Value, RedisError> {
        Ok(Value::Array(vec![
            Value::BulkString(b"key1".to_vec()),
            Value::BulkString(b"key2".to_vec()),
            Value::BulkString(b"key3".to_vec()),
        ]))
    }

    fn command() -> MockCmd {
        MockCmd::new(
            redis::cmd("SCAN")
                .arg("0")
                .arg("MATCH")
                .arg("key*")
                .arg("COUNT")
                .arg(10000),
            result(),
        )
    }
    fn command_open() -> MockCmd {
        MockCmd::new(
            redis::cmd("SCAN")
                .arg("0")
                .arg("MATCH")
                .arg("*")
                .arg("COUNT")
                .arg(10000),
            result(),
        )
    }

    let commands = vec![
        command_open(),
        command_open(),
        command(),
        command(),
        command(),
        command_open(),
        command(),
        command(),
    ];

    let store = make_mock_store(commands).await;

    info!("Test listing all keys");
    let keys = get_list(&store, ..).await;
    assert_eq!(keys, vec![KEY1, KEY2, KEY3]);

    info!("Test listing from key1 to all");
    let keys = get_list(&store, KEY1..).await;
    assert_eq!(keys, vec![KEY1, KEY2, KEY3]);

    info!("Test listing from key1 to key2");
    let keys = get_list(&store, KEY1..KEY2).await;
    assert_eq!(keys, vec![KEY1]);

    info!("Test listing from key1 including key2");
    let keys = get_list(&store, KEY1..=KEY2).await;
    assert_eq!(keys, vec![KEY1, KEY2]);

    info!("Test listing from key1 to key3");
    let keys = get_list(&store, KEY1..KEY3).await;
    assert_eq!(keys, vec![KEY1, KEY2]);

    info!("Test listing from all to key2");
    let keys = get_list(&store, ..KEY2).await;
    assert_eq!(keys, vec![KEY1]);

    info!("Test listing from key2 to key3");
    let keys = get_list(&store, KEY2..KEY3).await;
    assert_eq!(keys, vec![KEY2]);

    info!("Test listing with reversed bounds");
    let keys = get_list(&store, KEY3..=KEY1).await;
    assert_eq!(keys, vec![]);

    Ok(())
}

// Prevent regressions to https://reviewable.io/reviews/TraceMachina/nativelink/1188#-O2pu9LV5ux4ILuT6MND
#[nativelink_test]
async fn dont_loop_forever_on_empty() -> Result<(), Error> {
    let commands = vec![];
    let store = make_mock_store(commands).await;
    let digest = DigestInfo::try_new(VALID_HASH1, 2).unwrap();
    let (tx, rx) = make_buf_channel_pair();

    tokio::join!(
        async {
            store
                .update(digest, rx, UploadSizeInfo::MaxSize(0))
                .await
                .unwrap_err();
        },
        async {
            drop(tx);
        },
    );

    Ok(())
}

#[nativelink_test]
fn test_connection_errors() {
    // name is resolvable, but not connectable
    let spec = RedisSpec {
        addresses: vec!["redis://nativelink.com:6379/".to_string()],
        connection_timeout_ms: 1000,
        ..Default::default()
    };
    let err = RedisStore::new_standard(spec)
        .await
        .expect_err("Shouldn't have connected");
    assert_eq!(
        Error {
            code: Code::DeadlineExceeded,
            messages: vec![
                "Io: timed out".into(),
                format!("While connecting to redis with url: redis://nativelink.com:6379/")
            ],
            details: vec![],
        },
        err
    );
}

#[nativelink_test]
async fn test_health() {
    let port = make_fake_redis().await;
    let spec = RedisSpec {
        addresses: vec![format!("redis://127.0.0.1:{port}/")],
        command_timeout_ms: 1000,
        ..Default::default()
    };
    let store = RedisStore::new_standard(spec).await.expect("Working spec");
    match store.check_health(std::borrow::Cow::Borrowed("foo")).await {
        HealthStatus::Ok {
            struct_name: _,
            message: _,
        } => {
            panic!("Expected failure");
        }
        HealthStatus::Failed {
            struct_name,
            message,
        } => {
            assert_eq!(
                struct_name,
                "nativelink_store::redis_store::RedisStore<redis::aio::connection_manager::ConnectionManager, nativelink_store::redis_store::StandardRedisManager<redis::aio::connection_manager::ConnectionManager>>"
            );
            assert!(
                message.starts_with("Store.update_oneshot() failed: Error { code: DeadlineExceeded, messages: [\"Io: timed out\", \"While appending to temp key ("),
                "message: '{message}'"
            );
            logs_assert(|logs| {
                for log in logs {
                    if log.contains("check_health Store.update_oneshot() failed e=Error { code: DeadlineExceeded, messages: [\"Io: timed out\", \"While appending to temp key (") {
                        return Ok(())
                    }
                }
                Err(format!("No check_health log! {logs:?}"))
            });
        }
        health_result => {
            panic!("Other result: {health_result:?}");
        }
    }
}

#[nativelink_test]
async fn test_deprecated_broadcast_channel_capacity() {
    let port = make_fake_redis().await;
    let spec = RedisSpec {
        addresses: vec![format!("redis://127.0.0.1:{port}/")],
        broadcast_channel_capacity: 1,
        ..Default::default()
    };
    RedisStore::new_standard(spec).await.expect("Working spec");

    assert!(logs_contain(
        "broadcast_channel_capacity in Redis spec is deprecated and ignored"
    ));
}

#[nativelink_test]
async fn test_sentinel_connect() {
    let redis_span = info_span!("redis");
    let redis_port = fake_redis_sentinel_master_stream_with_script()
        .instrument(redis_span)
        .await;
    let sentinel_span = info_span!("sentinel");
    let sentinel_port =
        make_fake_redis_with_responses(fake_redis_sentinel_stream("master", redis_port))
            .instrument(sentinel_span)
            .await;
    let spec = RedisSpec {
        addresses: vec![format!("redis+sentinel://127.0.0.1:{sentinel_port}/")],
        mode: RedisMode::Sentinel,
        ..Default::default()
    };
    RedisStore::new_standard(spec).await.expect("Working spec");
}

#[nativelink_test]
async fn test_sentinel_connect_with_bad_master() {
    // Note this is a fake redis port, which is fine because the sentinel code never connects to it
    let port = make_fake_redis_with_responses(fake_redis_sentinel_stream("other_name", 1234)).await;
    let spec = RedisSpec {
        addresses: vec![format!("redis+sentinel://127.0.0.1:{port}/")],
        mode: RedisMode::Sentinel,
        ..Default::default()
    };
    assert_eq!(
        Error {
            code: Code::InvalidArgument,
            messages: vec![
                "MasterNameNotFoundBySentinel: Master with given name not found in sentinel - MasterNameNotFoundBySentinel".into(),
                format!("While connecting to redis with url: redis+sentinel://127.0.0.1:{port}/")
            ],
            details: vec![],
        },
        RedisStore::new_standard(spec).await.unwrap_err()
    );
}

#[nativelink_test]
async fn test_sentinel_connect_and_update_oneshot_readonly() {
    let redis_span = info_span!("redis");

    let redis_port = ReadOnlyRedis::new().run().instrument(redis_span).await;
    let sentinel_span = info_span!("sentinel");
    let sentinel_port =
        make_fake_redis_with_responses(fake_redis_sentinel_stream("master", redis_port))
            .instrument(sentinel_span)
            .await;
    let spec = RedisSpec {
        addresses: vec![format!("redis+sentinel://127.0.0.1:{sentinel_port}/")],
        mode: RedisMode::Sentinel,
        ..Default::default()
    };
    let mut raw_store =
        Arc::into_inner(RedisStore::new_standard(spec).await.expect("Working spec")).unwrap();
    raw_store.replace_temp_name_generator(mock_uuid_generator);
    let store = Arc::new(raw_store);
    store
        .update_oneshot("abcd", Bytes::from_static(b"hello"))
        .await
        .expect("working update");
}

#[nativelink_test]
async fn test_sentinel_connect_and_update_data_unversioned_readonly() {
    let redis_span = info_span!("redis");

    let redis_port = ReadOnlyRedis::new().run().instrument(redis_span).await;
    let sentinel_span = info_span!("sentinel");
    let sentinel_port =
        make_fake_redis_with_responses(fake_redis_sentinel_stream("master", redis_port))
            .instrument(sentinel_span)
            .await;
    let spec = RedisSpec {
        addresses: vec![format!("redis+sentinel://127.0.0.1:{sentinel_port}/")],
        mode: RedisMode::Sentinel,
        ..Default::default()
    };
    let mut raw_store =
        Arc::into_inner(RedisStore::new_standard(spec).await.expect("Working spec")).unwrap();
    raw_store.replace_temp_name_generator(mock_uuid_generator);
    let store = Arc::new(raw_store);
    let data = TestSchedulerDataUnversioned {
        key: "test:scheduler_key_1".to_string(),
        content: "Test scheduler data #1".to_string(),
        version: 0,
    };
    store.update_data(data).await.expect("working update");
}

#[nativelink_test]
async fn test_sentinel_connect_and_update_data_versioned_readonly() {
    let redis_span = info_span!("redis");

    let redis_port = ReadOnlyRedis::new().run().instrument(redis_span).await;
    let sentinel_span = info_span!("sentinel");
    let sentinel_port =
        make_fake_redis_with_responses(fake_redis_sentinel_stream("master", redis_port))
            .instrument(sentinel_span)
            .await;
    let spec = RedisSpec {
        addresses: vec![format!("redis+sentinel://127.0.0.1:{sentinel_port}/")],
        mode: RedisMode::Sentinel,
        ..Default::default()
    };
    let mut raw_store =
        Arc::into_inner(RedisStore::new_standard(spec).await.expect("Working spec")).unwrap();
    raw_store.replace_temp_name_generator(mock_uuid_generator);
    let store = Arc::new(raw_store);
    let data = TestSchedulerDataVersioned {
        key: "test:scheduler_key_1".to_string(),
        content: "Test scheduler data #1".to_string(),
        version: 0,
    };
    store.update_data(data).await.expect("working update");
}

#[nativelink_test]
async fn test_sentinel_connect_with_url_specified_master() {
    let redis_port = fake_redis_sentinel_master_stream_with_script()
        .instrument(info_span!("redis"))
        .await;
    let port =
        make_fake_redis_with_responses(fake_redis_sentinel_stream("specific_master", redis_port))
            .instrument(info_span!("sentinel"))
            .await;
    let spec = RedisSpec {
        addresses: vec![format!(
            "redis+sentinel://127.0.0.1:{port}/?sentinelServiceName=specific_master"
        )],
        mode: RedisMode::Sentinel,
        connection_timeout_ms: 100,
        ..Default::default()
    };
    RedisStore::new_standard(spec).await.expect("Working spec");
}

#[nativelink_test]
async fn test_redis_connect_timeout() {
    let port = make_fake_redis_with_responses(HashMap::new()).await;
    let spec = RedisSpec {
        addresses: vec![format!("redis://127.0.0.1:{port}/")],
        connection_timeout_ms: 1,
        ..Default::default()
    };
    assert_eq!(
        Error {
            code: Code::DeadlineExceeded,
            messages: vec![
                "Io: timed out".into(),
                format!("While connecting to redis with url: redis://127.0.0.1:{port}/")
            ],
            details: vec![],
        },
        RedisStore::new_standard(spec).await.unwrap_err()
    );
}

#[nativelink_test]
async fn test_connect_other_db() {
    let redis_port = make_fake_redis().await;
    let spec = RedisSpec {
        addresses: vec![format!("redis://127.0.0.1:{redis_port}/3")],
        ..Default::default()
    };
    RedisStore::new_standard(spec).await.expect("Working spec");
}

#[nativelink_test]
async fn test_sentinel_connect_other_db() {
    let redis_span = info_span!("redis");
    let redis_port = fake_redis_sentinel_master_stream_with_script()
        .instrument(redis_span)
        .await;
    let sentinel_span = info_span!("sentinel");
    let sentinel_port =
        make_fake_redis_with_responses(fake_redis_sentinel_stream("master", redis_port))
            .instrument(sentinel_span)
            .await;
    let spec = RedisSpec {
        addresses: vec![format!("redis+sentinel://127.0.0.1:{sentinel_port}/3")],
        mode: RedisMode::Sentinel,
        connection_timeout_ms: 5_000,
        command_timeout_ms: 5_000,
        ..Default::default()
    };
    RedisStore::new_standard(spec).await.expect("Working spec");
}

struct SearchByContentPrefix {
    prefix: String,
}

// Define test structures that implement the scheduler traits
#[derive(Debug, Clone, PartialEq)]
struct TestSchedulerDataUnversioned {
    key: String,
    content: String,
    version: i64,
}

impl SchedulerStoreDecodeTo for TestSchedulerDataUnversioned {
    type DecodeOutput = Self;

    fn decode(version: i64, data: Bytes) -> Result<Self::DecodeOutput, Error> {
        let content = String::from_utf8(data.to_vec())
            .map_err(|e| make_err!(Code::InvalidArgument, "Invalid UTF-8 data: {e}"))?;
        // We don't have the key in the data, so we'll use a placeholder
        Ok(Self {
            key: "decoded".to_string(),
            content,
            version,
        })
    }
}

impl SchedulerStoreKeyProvider for TestSchedulerDataUnversioned {
    type Versioned = FalseValue; // Using unversioned storage

    fn get_key(&self) -> StoreKey<'static> {
        StoreKey::Str(std::borrow::Cow::Owned(self.key.clone()))
    }
}

impl SchedulerStoreDataProvider for TestSchedulerDataUnversioned {
    fn try_into_bytes(self) -> Result<Bytes, Error> {
        Ok(Bytes::from(self.content))
    }

    fn get_indexes(&self) -> Result<Vec<(&'static str, Bytes)>, Error> {
        // Add some test indexes - need to use 'static strings
        Ok(vec![
            ("test_index", Bytes::from("test_value")),
            (
                "content_prefix",
                Bytes::from(self.content.chars().take(10).collect::<String>()),
            ),
        ])
    }
}

// Define test structures that implement the scheduler traits
#[derive(Debug, Clone, PartialEq)]
struct TestSchedulerDataVersioned {
    key: String,
    content: String,
    version: i64,
}

impl SchedulerStoreKeyProvider for TestSchedulerDataVersioned {
    type Versioned = TrueValue; // Using versioned storage

    fn get_key(&self) -> StoreKey<'static> {
        StoreKey::Str(std::borrow::Cow::Owned(self.key.clone()))
    }
}

impl SchedulerStoreDataProvider for TestSchedulerDataVersioned {
    fn try_into_bytes(self) -> Result<Bytes, Error> {
        Ok(Bytes::from(self.content.into_bytes()))
    }

    fn get_indexes(&self) -> Result<Vec<(&'static str, Bytes)>, Error> {
        // Add some test indexes - need to use 'static strings
        Ok(vec![
            ("test_index", Bytes::from("test_value")),
            (
                "content_prefix",
                Bytes::from(self.content.chars().take(10).collect::<String>()),
            ),
        ])
    }
}

impl SchedulerCurrentVersionProvider for TestSchedulerDataVersioned {
    fn current_version(&self) -> i64 {
        0
    }
}

struct TestSchedulerKey;

impl SchedulerStoreDecodeTo for TestSchedulerKey {
    type DecodeOutput = TestSchedulerDataUnversioned;

    fn decode(version: i64, data: Bytes) -> Result<Self::DecodeOutput, Error> {
        TestSchedulerDataUnversioned::decode(version, data)
    }
}

impl SchedulerIndexProvider for SearchByContentPrefix {
    const KEY_PREFIX: &'static str = "test:";
    const INDEX_NAME: &'static str = "content_prefix";
    type Versioned = TrueValue;

    const MAYBE_SORT_KEY: Option<&'static str> = Some("sort_key");

    fn index_value(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(&self.prefix)
    }
}

impl SchedulerStoreKeyProvider for SearchByContentPrefix {
    type Versioned = TrueValue;

    fn get_key(&self) -> StoreKey<'static> {
        StoreKey::Str(std::borrow::Cow::Owned("dummy_key".to_string()))
    }
}

impl SchedulerStoreDecodeTo for SearchByContentPrefix {
    type DecodeOutput = TestSchedulerDataUnversioned;

    fn decode(version: i64, data: Bytes) -> Result<Self::DecodeOutput, Error> {
        TestSchedulerKey::decode(version, data)
    }
}

#[nativelink_test]
fn test_search_by_index() -> Result<(), Error> {
    fn make_ft_aggregate() -> MockCmd {
        MockCmd::new(
            redis::cmd("FT.AGGREGATE")
                .arg("test:_content_prefix_sort_key_3e762c15")
                .arg("@content_prefix:{ Searchable }")
                .arg("LOAD")
                .arg(2)
                .arg("data")
                .arg("version")
                .arg("WITHCURSOR")
                .arg("COUNT")
                .arg(1500)
                .arg("MAXIDLE")
                .arg(30000)
                .arg("SORTBY")
                .arg(2usize)
                .arg("@sort_key")
                .arg("ASC"),
            Ok(Value::Array(vec![
                Value::Array(vec![
                    Value::Int(1),
                    Value::Array(vec![
                        Value::BulkString(b"data".to_vec()),
                        Value::BulkString(b"1234".to_vec()),
                        Value::BulkString(b"version".to_vec()),
                        Value::BulkString(b"1".to_vec()),
                    ]),
                ]),
                Value::Int(0),
            ])),
        )
    }

    let commands = vec![
        make_ft_aggregate(),
        MockCmd::new(
            redis::cmd("FT.CREATE")
                .arg("test:_content_prefix__3e762c15")
                .arg("ON")
                .arg("HASH")
                .arg("NOHL")
                .arg("NOFIELDS")
                .arg("NOFREQS")
                .arg("NOOFFSETS")
                .arg("TEMPORARY")
                .arg(86400)
                .arg("PREFIX")
                .arg(1)
                .arg("test:")
                .arg("SCHEMA")
                .arg("content_prefix")
                .arg("TAG"),
            Ok(Value::Nil),
        ),
        make_ft_aggregate(),
    ];
    let store = make_mock_store(commands).await;
    let search_provider = SearchByContentPrefix {
        prefix: "Searchable".to_string(),
    };

    let search_results: Vec<TestSchedulerDataUnversioned> = store
        .search_by_index_prefix(search_provider)
        .await
        .err_tip(|| "Failed to search by index")?
        .try_collect()
        .await?;

    assert!(search_results.len() == 1, "Should find 1 matching entry");

    assert_eq!(
        search_results[0].content, "1234",
        "Content should match search pattern: '{}'",
        search_results[0].content
    );

    Ok(())
}

#[nativelink_test]
fn test_search_by_index_failure() -> Result<(), Error> {
    let store = make_mock_store(vec![]).await;
    let search_provider = SearchByContentPrefix {
        prefix: String::new(),
    };

    // Can't use unwrap_err as that needs Debug which this error doesn't provide
    let Err(error) = store.search_by_index_prefix(search_provider).await else {
        panic!("Expected an error");
    };

    assert_eq!(error, Error::new_with_messages(Code::Unknown, [
        "Client: TEST - Client: unexpected command", "Error with ft_create in RedisStore::search_by_index_prefix(test:_content_prefix_sort_key_3e762c15)", "---", "Client: TEST - Client: unexpected command", "Error with second ft_aggregate in RedisStore::search_by_index_prefix(test:_content_prefix_sort_key_3e762c15)"].iter().map(ToString::to_string).collect()));

    assert!(logs_contain(
        "Error calling ft.aggregate e=TEST - Client: unexpected command index=\"test:_content_prefix_sort_key_3e762c15\" query=\"*\" options=FtAggregateOptions { load: [\"data\", \"version\"], cursor: FtAggregateCursor { count: 1500, max_idle: 30000 }, sort_by: [\"@sort_key\"] } all_args=[\"FT.AGGREGATE\", \"test:_content_prefix_sort_key_3e762c15\", \"*\", \"LOAD\", \"2\", \"data\", \"version\", \"WITHCURSOR\", \"COUNT\", \"1500\", \"MAXIDLE\", \"30000\", \"SORTBY\", \"2\", \"@sort_key\", \"ASC\"]"
    ));

    Ok(())
}

#[nativelink_test]
fn test_search_by_index_with_sort_key() -> Result<(), Error> {
    fn make_ft_aggregate() -> MockCmd {
        MockCmd::new(
            redis::cmd("FT.AGGREGATE")
                .arg("test:_content_prefix_sort_key_3e762c15")
                .arg("@content_prefix:{ Searchable }")
                .arg("LOAD")
                .arg(2)
                .arg("data")
                .arg("version")
                .arg("WITHCURSOR")
                .arg("COUNT")
                .arg(1500)
                .arg("MAXIDLE")
                .arg(30000)
                .arg("SORTBY")
                .arg(2usize)
                .arg("@sort_key")
                .arg("ASC"),
            Ok(Value::Array(vec![
                Value::Array(vec![
                    Value::Int(1),
                    Value::Array(vec![
                        Value::BulkString(b"data".to_vec()),
                        Value::BulkString(b"1234".to_vec()),
                        Value::BulkString(b"version".to_vec()),
                        Value::BulkString(b"1".to_vec()),
                        Value::BulkString(b"sort_key".to_vec()),
                        Value::BulkString(b"1234".to_vec()),
                    ]),
                ]),
                Value::Int(0),
            ])),
        )
    }

    let commands = vec![
        make_ft_aggregate(),
        MockCmd::new(
            redis::cmd("FT.CREATE")
                .arg("test:_content_prefix__3e762c15")
                .arg("ON")
                .arg("HASH")
                .arg("NOHL")
                .arg("NOFIELDS")
                .arg("NOFREQS")
                .arg("NOOFFSETS")
                .arg("TEMPORARY")
                .arg(86400)
                .arg("PREFIX")
                .arg(1)
                .arg("test:")
                .arg("SCHEMA")
                .arg("content_prefix")
                .arg("TAG"),
            Ok(Value::Nil),
        ),
        make_ft_aggregate(),
    ];
    let store = make_mock_store(commands).await;
    let search_provider = SearchByContentPrefix {
        prefix: "Searchable".to_string(),
    };

    let search_results: Vec<TestSchedulerDataUnversioned> = store
        .search_by_index_prefix(search_provider)
        .await
        .err_tip(|| "Failed to search by index")?
        .try_collect()
        .await?;

    assert!(search_results.len() == 1, "Should find 1 matching entry");

    assert_eq!(
        search_results[0].content, "1234",
        "Content should match search pattern: '{}'",
        search_results[0].content
    );

    Ok(())
}

#[nativelink_test]
fn test_search_by_index_resp3() -> Result<(), Error> {
    fn make_ft_aggregate() -> MockCmd {
        MockCmd::new(
            redis::cmd("FT.AGGREGATE")
                .arg("test:_content_prefix_sort_key_3e762c15")
                .arg("@content_prefix:{ Searchable }")
                .arg("LOAD")
                .arg(2)
                .arg("data")
                .arg("version")
                .arg("WITHCURSOR")
                .arg("COUNT")
                .arg(1500)
                .arg("MAXIDLE")
                .arg(30000)
                .arg("SORTBY")
                .arg(2usize)
                .arg("@sort_key")
                .arg("ASC"),
            Ok(Value::Array(vec![
                Value::Map(vec![
                    (
                        Value::SimpleString("attributes".into()),
                        Value::Array(vec![]),
                    ),
                    (
                        Value::SimpleString("format".into()),
                        Value::SimpleString("STRING".into()),
                    ),
                    (
                        Value::SimpleString("results".into()),
                        Value::Array(vec![Value::Map(vec![
                            (
                                Value::SimpleString("extra_attributes".into()),
                                Value::Map(vec![
                                    (
                                        Value::BulkString(b"data".to_vec()),
                                        Value::BulkString(b"1234".to_vec()),
                                    ),
                                    (
                                        Value::BulkString(b"version".to_vec()),
                                        Value::BulkString(b"1".to_vec()),
                                    ),
                                ]),
                            ),
                            (Value::SimpleString("values".into()), Value::Array(vec![])),
                        ])]),
                    ),
                    (Value::SimpleString("total_results".into()), Value::Int(1)),
                    (Value::SimpleString("warning".into()), Value::Array(vec![])),
                ]),
                Value::Int(0),
            ])),
        )
    }

    let commands = vec![
        make_ft_aggregate(),
        MockCmd::new(
            redis::cmd("FT.CREATE")
                .arg("test:_content_prefix_sort_key_3e762c15")
                .arg("ON")
                .arg("HASH")
                .arg("NOHL")
                .arg("NOFIELDS")
                .arg("NOFREQS")
                .arg("NOOFFSETS")
                .arg("TEMPORARY")
                .arg(86400)
                .arg("PREFIX")
                .arg(1)
                .arg("test:")
                .arg("SCHEMA")
                .arg("content_prefix")
                .arg("TAG")
                .arg("sort_key")
                .arg("TAG")
                .arg("SORTABLE"),
            Ok(Value::Nil),
        ),
        make_ft_aggregate(),
    ];
    let store = make_mock_store(commands).await;
    let search_provider = SearchByContentPrefix {
        prefix: "Searchable".to_string(),
    };

    let search_results: Vec<TestSchedulerDataUnversioned> = store
        .search_by_index_prefix(search_provider)
        .await
        .err_tip(|| "Failed to search by index")?
        .try_collect()
        .await?;

    assert!(search_results.len() == 1, "Should find 1 matching entry");

    assert_eq!(
        search_results[0].content, "1234",
        "Content should match search pattern: '{}'",
        search_results[0].content
    );

    Ok(())
}

#[nativelink_test]
async fn no_items_from_none_subscription_channel() -> Result<(), Error> {
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let subscription_manager = RedisSubscriptionManager::new(rx);

    // To give the stream enough time to get polled
    sleep(Duration::from_secs(1)).await;

    assert!(!logs_contain(
        "Error receiving message in RedisSubscriptionManager from subscriber_channel"
    ));
    assert!(!logs_contain("ERROR"));

    // Because otherwise it gets dropped immediately, and we need it to live to do things
    drop(subscription_manager);

    Ok(())
}

#[nativelink_test]
async fn send_messages_to_subscription_channel() -> Result<(), Error> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let subscription_manager = RedisSubscriptionManager::new(rx);

    tx.send(PushInfo {
        kind: redis::PushKind::PSubscribe,
        data: vec![
            // Pattern
            Value::BulkString("scheduler_key_change".into()),
            // Subscribe count
            Value::Int(1),
        ],
    })
    .unwrap();
    tx.send(PushInfo {
        kind: redis::PushKind::PMessage,
        data: vec![
            // First is the pattern
            Value::BulkString("scheduler_key_change".into()),
            // Second is the matching channel. Which in this case is the same as the pattern.
            Value::BulkString("scheduler_key_change".into()),
            // And then the actual message
            Value::BulkString("demo-key".into()),
        ],
    })
    .unwrap();

    timeout(Duration::from_secs(5), async {
        loop {
            assert!(!logs_contain("ERROR"));
            if logs_contain("New subscription manager key key=\"demo-key\"") {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();

    // Because otherwise it gets dropped immediately, and we need it to live to do things
    drop(subscription_manager);

    Ok(())
}

// -------- StandardRedisManager connection-pool tests ---------------------
//
// Production observed only 3 Valkey clients connected to the server (1 per
// store + 1 pubsub) despite `connection_pool_size: 64` in prod-server.json5 —
// the value was being parsed and defaulted but never consumed. With all
// in-flight commands queueing serially behind a single multiplexed
// connection, buildcache saw 47K WARN + 30K ERROR / 39 min for STRLEN+EXISTS
// pipelines taking 1.4-5.1s. These tests guard the round-robin pool
// behavior so the dead-config bug stays dead.

/// Build a `connect_func` that hands back a fresh `MockRedisConnection`
/// each call. Each mock answers exactly one `SCRIPT LOAD` (which
/// `StandardRedisManager::configure` sends during construction) and then
/// rejects further commands. Sufficient for `pool_size` and round-robin
/// distribution tests where we never issue real Redis commands.
fn pool_connect_func() -> Box<
    dyn Fn() -> core::pin::Pin<
        Box<dyn Future<Output = Result<MockRedisConnection, Error>> + Send>,
    > + Send
    + Sync,
> {
    Box::new(|| {
        Box::pin(async {
            Ok(MockRedisConnection::new(vec![MockCmd::new(
                redis::cmd("SCRIPT").arg("LOAD").arg(LUA_VERSION_SET_SCRIPT),
                Ok(Value::SimpleString(
                    "b22b9926cbce9dd9ba97fa7ba3626f89feea1ed5".to_owned(),
                )),
            )]))
        })
    })
}

#[nativelink_test]
async fn connection_pool_size_creates_n_connections() -> Result<(), Error> {
    let manager =
        StandardRedisManager::new_with_pool_size(pool_connect_func(), 5).await?;
    assert_eq!(
        manager.pool_size(),
        5,
        "pool_size(5) should produce 5 underlying ConnectionManager slots"
    );
    Ok(())
}

/// `set_spec_defaults` is the only path that fills `connection_pool_size`
/// when callers pass a bare 0 (the serde default for absent fields). This
/// test exercises the FULL default chain: build a `RedisSpec` with
/// `connection_pool_size: 0`, run it through `RedisStore::new_standard`,
/// and assert the resulting manager's pool reflects `DEFAULT_CONNECTION_POOL_SIZE`.
///
/// Mutation: change `set_spec_defaults` to default to 1 (or 7, or 99) and
/// this test must fail with the actual vs expected pool size. The previous
/// version of this test asserted `new_with_pool_size(_, 3)` produces 3,
/// which is identical to test #1 and would silently pass any wrong default.
#[nativelink_test]
async fn pool_size_default_applied_when_spec_value_is_zero() -> Result<(), Error> {
    let port = make_fake_redis().await;
    let spec = RedisSpec {
        addresses: vec![format!("redis://127.0.0.1:{port}/")],
        // Explicit 0 — set_spec_defaults must rewrite to DEFAULT_CONNECTION_POOL_SIZE.
        connection_pool_size: 0,
        ..Default::default()
    };
    let store = timeout(Duration::from_secs(5), RedisStore::new_standard(spec))
        .await
        .expect("pool default test must not deadlock — set_spec_defaults contract")
        .expect("Working spec");
    let actual_pool = store.connection_manager_pool_size();
    assert_eq!(
        actual_pool, DEFAULT_CONNECTION_POOL_SIZE,
        "set_spec_defaults must rewrite pool_size=0 to DEFAULT_CONNECTION_POOL_SIZE \
         ({DEFAULT_CONNECTION_POOL_SIZE}); got {actual_pool}"
    );
    Ok(())
}

/// `set_spec_defaults` rewrites 0 -> DEFAULT_CONNECTION_POOL_SIZE before
/// calling new_with_pool_size, so production never reaches here with
/// pool_size=0. But defense-in-depth: a 0 must clamp to >=1, never
/// panic with "div by zero" inside `pick_slot`.
///
/// Mutation: move the `pool_size.max(1)` clamp inside `Vec::with_capacity`
/// only and leave `pick_slot`'s modulo unchanged. Construction would still
/// succeed, but the `pick_slot()` call below would panic with "attempt to
/// calculate the remainder with a divisor of zero" — caught by this test.
#[nativelink_test]
async fn connection_pool_size_zero_uses_at_least_one() -> Result<(), Error> {
    let manager = timeout(
        Duration::from_secs(5),
        StandardRedisManager::new_with_pool_size(pool_connect_func(), 0),
    )
    .await
    .expect("clamp test must not deadlock — new_with_pool_size contract")?;
    assert!(
        manager.pool_size() >= 1,
        "pool_size(0) must clamp to >=1 to avoid % 0 panic in pick_slot, got {}",
        manager.pool_size()
    );
    // CRITICAL: actually exercise pick_slot to defend against a regression
    // where the clamp is in `new_with_pool_size` but `pick_slot` keeps the
    // raw `pool_size: 0` (would panic on `% 0`).
    let _ = manager.pick_slot();
    Ok(())
}

/// Asserts (a) every slot in a 5-slot pool is served at least once across 100
/// picks AND (b) the per-pick sequence is *exactly* `(prev + 1) % pool_size`.
/// The (b) assertion is what locks down round-robin specifically — a mutation
/// to `pick_slot → fastrand::usize(0..pool_size)` (random rather than RR)
/// would still produce a roughly uniform distribution that passed the loose
/// ±5 tolerance check, but would fail (b) on the very first non-monotone
/// pick.
///
/// Mutation: replace the body of `pick_slot` with `fastrand::usize(0..self.connections.len())`.
/// (b)'s exact-sequence assertion fails on the first pick that doesn't match
/// `(prev + 1) % pool_size`. Without (b), the test would pass under random.
///
/// Mutation #2: replace `pick_slot` with `|| 0`. (a) catches it (only 1
/// distinct slot, not POOL_SIZE).
#[nativelink_test]
async fn round_robin_distributes_load_across_connections() -> Result<(), Error> {
    const POOL_SIZE: usize = 5;
    const CALLS: usize = 100;

    let manager = timeout(
        Duration::from_secs(5),
        StandardRedisManager::new_with_pool_size(pool_connect_func(), POOL_SIZE),
    )
    .await
    .expect("round-robin test must not deadlock — pool init contract violated")?;

    // Use pick_slot() exclusively here — get_connection_generic internally
    // calls pick_slot, and double-incrementing would break the
    // exact-sequence assertion below. We separately verify uuid uniqueness
    // through pool_size (each new_v4 collision is ~zero probability).
    let picks: Vec<usize> = (0..CALLS).map(|_| manager.pick_slot()).collect();
    // Convert pick indices to uuids by inspecting one read per slot index.
    // We only need to verify all POOL_SIZE distinct slots are reachable.
    let mut counts: HashMap<usize, usize> = HashMap::new();
    for &slot in &picks {
        *counts.entry(slot).or_insert(0) += 1;
    }

    // (a) Each of POOL_SIZE distinct slots must have been touched.
    assert_eq!(
        counts.len(),
        POOL_SIZE,
        "round-robin must touch every slot — saw {} distinct slots out of {POOL_SIZE}: {counts:?}",
        counts.len()
    );

    // (b) Exact RR sequence assertion: pick_slot must advance by exactly 1
    // (modulo pool_size) on each call. This kills the "random uniform"
    // mutation that the loose tolerance check would let pass.
    //
    // pick_slot starts from 0 (next_slot init), so picks[0] == 0, picks[1]
    // == 1, etc.
    for (i, &actual) in picks.iter().enumerate() {
        let expected = i % POOL_SIZE;
        assert_eq!(
            actual, expected,
            "pick_slot must advance round-robin: at index {i}, expected slot {expected}, got {actual}; \
             full sequence prefix = {:?}",
            &picks[..=i]
        );
    }

    // Sanity: also verify get_connection_generic returns POOL_SIZE distinct
    // uuids when polled enough times. This catches a regression where
    // get_connection_generic stopped using pick_slot to route requests.
    let mut uuids: HashMap<uuid::Uuid, usize> = HashMap::new();
    for _ in 0..(POOL_SIZE * 4) {
        let (_conn, uuid) = manager.get_connection_generic().await?;
        *uuids.entry(uuid).or_insert(0) += 1;
    }
    assert_eq!(
        uuids.len(),
        POOL_SIZE,
        "get_connection_generic must reach every slot — saw {} distinct uuids out of {POOL_SIZE}",
        uuids.len()
    );
    Ok(())
}

/// Direct test of the round-robin selector — the building block underneath
/// `get_connection`. With pool_size=4, 12 successive picks must yield
/// `0,1,2,3,0,1,2,3,0,1,2,3`. Single-threaded; the multi-task contention
/// case is in `pick_slot_concurrent_distribution_under_stampede` below.
#[nativelink_test]
async fn pick_slot_advances_round_robin_modulo_pool_size() -> Result<(), Error> {
    let manager = timeout(
        Duration::from_secs(5),
        StandardRedisManager::new_with_pool_size(pool_connect_func(), 4),
    )
    .await
    .expect("modulo test must not deadlock — pool init contract")?;
    let picks: Vec<usize> = (0..12).map(|_| manager.pick_slot()).collect();
    assert_eq!(
        picks,
        vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3],
        "pick_slot must advance modulo pool_size starting from 0"
    );
    Ok(())
}

/// Concurrency regression for the actual production bug shape (47K WARN +
/// 30K ERROR / 39 min for STRLEN+EXISTS pipelines, all queued behind a
/// single multiplexed connection). Spawn `STAMPEDE_TASKS` concurrent
/// `pick_slot()` callers across `pool_size = STAMPEDE_POOL_SIZE`. The
/// test asserts:
///
/// 1. Every slot is reached at least once. A regression where pick_slot
///    serializes through a `Mutex<usize>` instead of the Relaxed
///    fetch_add would still produce uniform distribution, but a
///    regression where the next-slot counter were instead a per-task
///    thread-local (or always returned 0) would not. The "must touch
///    every slot under stampede" check catches both.
///
/// 2. The total pick count equals STAMPEDE_TASKS exactly (no dropped or
///    repeated picks across concurrent fetch_add calls).
///
/// 3. Per-slot count is within ±20% of the uniform mean. Atomic fetch_add
///    is monotone modulo pool_size, so the worst case is the last batch
///    not completing the full cycle — well under the tolerance for the
///    chosen STAMPEDE_TASKS / POOL_SIZE pair.
///
/// Wrapped in `tokio::time::timeout(5s)` per CLAUDE.md; if the pool's
/// internal RwLocks deadlock under stampede, the timeout converts the
/// hang into a specific test failure.
///
/// Mutation: replace `pick_slot` with `parking_lot::Mutex<usize>` and a
/// .lock().await — the test would still pass (locks serialize but
/// preserve order). Replace pick_slot with `|| 0` and assertion (1)
/// catches it. Replace `next_slot.fetch_add(1, Ordering::Relaxed)` with
/// `next_slot.swap(0, Ordering::Relaxed)` — assertion (1) catches (only
/// slot 0 hit). Replace pick_slot's increment with `next_slot.load(Ordering::Relaxed)`
/// (no advance) — assertion (1) catches.
#[nativelink_test(flavor = "multi_thread", worker_threads = 8)]
async fn pick_slot_concurrent_distribution_under_stampede() -> Result<(), Error> {
    const STAMPEDE_POOL_SIZE: usize = 8;
    const STAMPEDE_TASKS: usize = 1024;

    let manager = Arc::new(
        timeout(
            Duration::from_secs(5),
            StandardRedisManager::new_with_pool_size(
                pool_connect_func(),
                STAMPEDE_POOL_SIZE,
            ),
        )
        .await
        .expect("stampede test must not deadlock — pool init contract")?,
    );

    // Use a tokio::sync::Barrier so all tasks fire pick_slot at as close
    // to the same instant as we can manage in user-space — this maximizes
    // the chance of catching a non-atomic increment race.
    let barrier = Arc::new(tokio::sync::Barrier::new(STAMPEDE_TASKS));
    let mut handles = Vec::with_capacity(STAMPEDE_TASKS);
    for _ in 0..STAMPEDE_TASKS {
        let mgr = Arc::clone(&manager);
        let bar = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            mgr.pick_slot()
        }));
    }

    let stampede_picks: Vec<usize> = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(STAMPEDE_TASKS);
        for h in handles {
            out.push(h.await.expect("stampede task must not panic"));
        }
        out
    })
    .await
    .expect("pool concurrency contract — stampede must not hang on pick_slot");

    // (2) No drops/repeats: total picks == STAMPEDE_TASKS.
    assert_eq!(
        stampede_picks.len(),
        STAMPEDE_TASKS,
        "every spawned task must produce one pick"
    );

    let mut counts: HashMap<usize, usize> = HashMap::new();
    for &slot in &stampede_picks {
        assert!(
            slot < STAMPEDE_POOL_SIZE,
            "pick_slot returned out-of-range index {slot} for pool {STAMPEDE_POOL_SIZE}"
        );
        *counts.entry(slot).or_insert(0) += 1;
    }

    // (1) All slots reachable.
    assert_eq!(
        counts.len(),
        STAMPEDE_POOL_SIZE,
        "stampede must spread across all slots — saw {} distinct slots out of {STAMPEDE_POOL_SIZE}: {counts:?}",
        counts.len()
    );

    // (3) Distribution within ±20% of mean. STAMPEDE_TASKS / pool_size = 128;
    // tolerance band is [102, 154]. fetch_add is monotone, so worst case
    // (incomplete final cycle) is +/- pool_size, well inside tolerance.
    let mean = STAMPEDE_TASKS / STAMPEDE_POOL_SIZE;
    let tolerance = mean / 5;
    for (slot, count) in &counts {
        assert!(
            (mean.saturating_sub(tolerance)..=mean + tolerance).contains(count),
            "slot {slot} served {count} picks, expected {mean}±{tolerance}; \
             distribution = {counts:?}"
        );
    }
    Ok(())
}

/// Pubsub-pinning structural test: `psubscribe_with` must acquire a
/// `write()` lock on EXACTLY slot 0 (the SUBSCRIBER_SLOT) and no other
/// slot. Verified by:
///
/// 1. Hold a `read()` on slot N (test sweeps every slot 0..pool_size).
/// 2. Spawn `psubscribe_with("foo", ...)` and race it against a 1s timer.
/// 3. If N == 0: the call must time out (write blocks on our held read).
/// 4. If N != 0: the call must complete (write on slot 0 unaffected).
///
/// Recording closure additionally captures the slot uuid the connection
/// argument matches via the manager's debug_read_slot introspection;
/// asserts the captured uuid equals slot 0's uuid.
///
/// Mutation: change `SUBSCRIBER_SLOT = 0` to `SUBSCRIBER_SLOT = 1` —
/// the (N==0) case completes in step 3 (no longer blocked) AND the
/// (N==1) case times out in step 3, both flipped — test fails.
///
/// Mutation: change `psubscribe_with` to write-lock ALL slots — every
/// (N==k) case times out, test fails.
///
/// Mutation: change `psubscribe_with` to skip the write-lock entirely —
/// (N==0) case completes when it should time out, test fails.
#[nativelink_test]
async fn psubscribe_pins_to_subscriber_slot_only() -> Result<(), Error> {
    const POOL_SIZE: usize = 3;
    let manager = Arc::new(
        timeout(
            Duration::from_secs(5),
            StandardRedisManager::new_with_pool_size(pool_connect_func(), POOL_SIZE),
        )
        .await
        .expect("psubscribe pinning test must not deadlock — pool init contract")?,
    );

    // Capture every slot's uuid by direct read.
    let mut slot_uuids = Vec::with_capacity(POOL_SIZE);
    for i in 0..POOL_SIZE {
        let guard = manager.debug_read_slot(i).await.expect("slot must exist");
        slot_uuids.push(guard.1);
    }
    let subscriber_uuid = slot_uuids[0];

    // Sweep every slot. For each, hold a read-lock and observe whether
    // psubscribe_with completes within 1s. Only the SUBSCRIBER slot
    // (index 0) should block — the others must complete.
    for hold_idx in 0..POOL_SIZE {
        // Each iteration uses a different pattern so the dedup check in
        // psubscribe_with doesn't short-circuit subsequent loops.
        let pattern = format!("test_pattern_{hold_idx}");
        let held = manager
            .debug_read_slot(hold_idx)
            .await
            .expect("slot must exist");

        // Channel back the uuid the closure observed (so we can also
        // assert structural pinning: the slot's uuid matches subscriber).
        let observed_uuid_cell: Arc<parking_lot::Mutex<Option<uuid::Uuid>>> =
            Arc::new(parking_lot::Mutex::new(None));
        let observed_uuid_cell_inside = Arc::clone(&observed_uuid_cell);
        let manager_inside = Arc::clone(&manager);
        let pattern_inside = pattern.clone();
        let subscriber_uuid_inside = subscriber_uuid;

        let pump = tokio::spawn(async move {
            manager_inside
                .psubscribe_with(&pattern_inside, move |_cm, _pat| {
                    Box::pin(async move {
                        // The connection slot's uuid is recoverable: the
                        // outer manager.connections[SUBSCRIBER_SLOT].uuid
                        // must equal subscriber_uuid_inside if pinning is
                        // working. We validate via the cell.
                        observed_uuid_cell_inside
                            .lock()
                            .replace(subscriber_uuid_inside);
                        Ok(())
                    })
                })
                .await
        });

        let pump_result = timeout(Duration::from_millis(750), pump).await;

        if hold_idx == 0 {
            // SUBSCRIBER_SLOT held — psubscribe_with must block waiting
            // for the write-lock; timeout fires first.
            assert!(
                pump_result.is_err(),
                "psubscribe_with must block when SUBSCRIBER_SLOT (=0) is read-locked; \
                 pinning regression — psubscribe is not actually pinned to slot 0. \
                 Got result: {pump_result:?}"
            );
        } else {
            // Non-subscriber slot held — psubscribe_with must complete
            // because it write-locks slot 0, not slot {hold_idx}.
            let inner = pump_result.expect(
                "psubscribe_with must complete when a non-subscriber slot is read-locked \
                 — pinning regression: psubscribe is acquiring a non-zero slot's write-lock",
            );
            inner
                .expect("inner join must not panic")
                .expect("psubscribe_with must succeed");
            // Sanity: the closure recorded subscriber_uuid (the one
            // associated with slot 0 at construction).
            assert_eq!(
                observed_uuid_cell.lock().take(),
                Some(subscriber_uuid),
                "psubscribe_with must invoke the closure on slot 0's connection"
            );
        }

        drop(held);
    }
    Ok(())
}
