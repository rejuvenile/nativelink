// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use core::cmp;
use core::pin::Pin;
use core::str::from_utf8;
use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    CompressionSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_store::compression_store::{
    CURRENT_STREAM_FORMAT_VERSION, CompressionStore, DEFAULT_BLOCK_SIZE, FOOTER_FRAME_TYPE, Footer,
    Header, Lz4Config, SliceIndex, WINCODE_PREALLOC_LIMIT_BYTES, WincodeConfig,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::spawn;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

/// Utility function that will build a Footer object from the input.
fn extract_footer(data: &[u8]) -> Result<Footer, Error> {
    let mut pos = data.len() - 1; // Skip version byte(u8)
    pos -= 4; // Skip block_size(u32).
    pos -= 8; // Skip uncompressed_data_size(u64).
    let index_count = u32::from_le_bytes(data[pos - 4..pos].try_into().unwrap());
    pos -= 4; // Skip index_count(u32).
    pos -= (index_count * 4) as usize; // Skip indexes(u32 * index_count).
    pos -= 8; // Skip bincode_index_count(u64).
    let footer_len = u32::from_le_bytes(data[pos - 4..pos].try_into().unwrap());
    assert_eq!(
        footer_len as usize,
        data.len() - pos,
        "Expected footer_len to match"
    );
    assert_eq!(
        data[pos - 4 - 1],
        FOOTER_FRAME_TYPE,
        "Expected frame_type to be footer"
    );

    let footer = wincode::config::deserialize::<Footer, _>(&data[pos..], WincodeConfig::new())
        .map_err(|e| make_err!(Code::Internal, "Failed to deserialize header : {:?}", e))?;
    Ok(footer)
}

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const DUMMY_DATA_SIZE: usize = 100; // Some dummy size to populate DigestInfo with.
const MEGABYTE_SZ: usize = 1024 * 1024;

#[nativelink_test]
async fn simple_smoke_test() -> Result<(), Error> {
    const RAW_INPUT: &str = "123";

    let store = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config::default(),
            ),
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
    )
    .err_tip(|| "Failed to create compression store")?;

    let digest = DigestInfo::try_new(VALID_HASH, DUMMY_DATA_SIZE).unwrap();
    store.update_oneshot(digest, RAW_INPUT.into()).await?;

    let store_data = store
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get from inner store")?;

    assert_eq!(
        from_utf8(&store_data[..]).unwrap(),
        RAW_INPUT,
        "Expected data to match"
    );
    Ok(())
}

#[nativelink_test]
async fn partial_reads_test() -> Result<(), Error> {
    const RAW_DATA: [u8; 30] = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, // BR.
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19, // BR.
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, // BR.
    ];

    let store_owned = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config {
                    block_size: 10,
                    ..Default::default()
                },
            ),
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
    )
    .err_tip(|| "Failed to create compression store")?;
    let store = Pin::new(&store_owned);

    let digest = DigestInfo::try_new(VALID_HASH, DUMMY_DATA_SIZE).unwrap();
    store
        .update_oneshot(digest, RAW_DATA.as_ref().into())
        .await?;

    // Read through the store forcing lots of decompression steps at different offsets
    // and different window sizes. This will ensure we get most edge cases for when
    // we go across block boundaries inclusive, on the fence and exclusive.
    for read_slice_size in 0..(RAW_DATA.len() + 5) {
        for offset in 0..(RAW_DATA.len() + 5) {
            let store_data = store
                .get_part_unchunked(digest, offset as u64, Some(read_slice_size as u64))
                .await
                .err_tip(|| {
                    format!("Failed to get from inner store at {offset} - {read_slice_size}")
                })?;

            let start_pos = cmp::min(RAW_DATA.len(), offset);
            let end_pos = cmp::min(RAW_DATA.len(), offset + read_slice_size);
            assert_eq!(
                &store_data,
                &RAW_DATA[start_pos..end_pos],
                "Expected data to match at {} - {}",
                offset,
                read_slice_size,
            );
        }
    }

    Ok(())
}

#[nativelink_test]
async fn rand_5mb_smoke_test() -> Result<(), Error> {
    let store_owned = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config::default(),
            ),
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
    )
    .err_tip(|| "Failed to create compression store")?;
    let store = Pin::new(&store_owned);

    let mut value = vec![0u8; 5 * MEGABYTE_SZ];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);

    let digest = DigestInfo::try_new(VALID_HASH, DUMMY_DATA_SIZE).unwrap();
    store.update_oneshot(digest, value.clone().into()).await?;

    let store_data = store
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get from inner store")?;

    assert_eq!(&store_data, &value, "Expected data to match");
    Ok(())
}

#[nativelink_test]
async fn sanity_check_zero_bytes_test() -> Result<(), Error> {
    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store_owned = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config::default(),
            ),
        },
        Store::new(inner_store.clone()),
    )
    .err_tip(|| "Failed to create compression store")?;
    let store = Pin::new(&store_owned);

    let digest = DigestInfo::try_new(VALID_HASH, DUMMY_DATA_SIZE).unwrap();
    store.update_oneshot(digest, vec![].into()).await?;

    let store_data = store
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get from inner store")?;

    assert_eq!(
        store_data.len(),
        0,
        "Expected store data to have no data in it"
    );

    let compressed_data = Pin::new(inner_store.as_ref())
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get from inner store")?;
    assert_eq!(
        extract_footer(&compressed_data)?,
        Footer {
            indexes: vec![],
            index_count: 0,
            uncompressed_data_size: 0,
            config: Lz4Config {
                block_size: DEFAULT_BLOCK_SIZE,
            },
            version: CURRENT_STREAM_FORMAT_VERSION,
        },
        "Expected footers to match"
    );
    Ok(())
}

#[nativelink_test]
async fn check_header_test() -> Result<(), Error> {
    const BLOCK_SIZE: u32 = 150;
    const MAX_SIZE_INPUT: u64 = 1024 * 1024; // 1MB.
    const RAW_INPUT: &str = "123";

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store_owned = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config {
                    block_size: BLOCK_SIZE,
                    ..Default::default()
                },
            ),
        },
        Store::new(inner_store.clone()),
    )
    .err_tip(|| "Failed to create compression store")?;
    let store = Pin::new(&store_owned);

    let digest = DigestInfo::try_new(VALID_HASH, DUMMY_DATA_SIZE).unwrap();

    let (mut tx, rx) = make_buf_channel_pair();
    let send_fut = async move {
        tx.send(RAW_INPUT.into()).await?;
        tx.send_eof()
    };
    let (res1, res2) = futures::join!(
        send_fut,
        store.update(digest, rx, UploadSizeInfo::MaxSize(MAX_SIZE_INPUT))
    );
    res1.merge(res2)?;

    let compressed_data = Pin::new(inner_store.as_ref())
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get from inner store")?;

    let mut reader = Cursor::new(&compressed_data);
    {
        // Check version in header.
        let version = reader.read_u8().await?;
        assert_eq!(
            version, CURRENT_STREAM_FORMAT_VERSION,
            "Expected header version to match current version"
        );
    }
    {
        // Check block size.
        let block_size = reader.read_u32_le().await?;
        assert_eq!(block_size, BLOCK_SIZE, "Expected block size to match");
    }
    {
        // Check upload_type and upload_size.
        const MAX_SIZE_OPT_CODE: u32 = 1;
        let upload_type = reader.read_u32_le().await?;
        assert_eq!(
            upload_type, MAX_SIZE_OPT_CODE,
            "Expected enum size type to match"
        );
        let upload_size = reader.read_u32_le().await?;
        assert_eq!(
            u64::from(upload_size),
            MAX_SIZE_INPUT,
            "Expected upload size to match"
        );
    }

    // As a sanity check lets check our footer.
    assert_eq!(
        extract_footer(&compressed_data)?,
        Footer {
            indexes: vec![],
            index_count: 0,
            uncompressed_data_size: RAW_INPUT.len() as u64,
            config: Lz4Config {
                block_size: BLOCK_SIZE
            },
            version: CURRENT_STREAM_FORMAT_VERSION,
        },
        "Expected footers to match"
    );

    Ok(())
}

#[nativelink_test]
async fn check_footer_test() -> Result<(), Error> {
    const BLOCK_SIZE: u32 = 32 * 1024;
    const EXPECTED_INDEXES: [u32; 7] = [32898, 32898, 32898, 32898, 140, 140, 140];

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store_owned = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config {
                    block_size: BLOCK_SIZE,
                    ..Default::default()
                },
            ),
        },
        Store::new(inner_store.clone()),
    )
    .err_tip(|| "Failed to create compression store")?;
    let store = Pin::new(&store_owned);

    let mut value = vec![0u8; MEGABYTE_SZ / 4];
    let data_len = value.len();

    let mut rng = SmallRng::seed_from_u64(1);
    // Fill first half of data with random data that is not compressible.
    rng.fill(&mut value[..(data_len / 2)]);

    let digest = DigestInfo::try_new(VALID_HASH, DUMMY_DATA_SIZE).unwrap();
    store.update_oneshot(digest, value.clone().into()).await?;

    let compressed_data = Pin::new(inner_store.as_ref())
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "Failed to get from inner store")?;

    let mut pos = compressed_data.len();
    {
        // Check version in footer.
        let version = compressed_data[pos - 1];
        pos -= 1;
        assert_eq!(
            version, CURRENT_STREAM_FORMAT_VERSION,
            "Expected footer version to match current version"
        );
    }
    {
        // Check block size in footer.
        let block_size = u32::from_le_bytes(compressed_data[pos - 4..pos].try_into().unwrap());
        pos -= 4;
        assert_eq!(
            block_size, BLOCK_SIZE,
            "Expected uncompressed_data_size to match original data size"
        );
    }
    {
        // Check data size in footer.
        let uncompressed_data_size =
            u64::from_le_bytes(compressed_data[pos - 8..pos].try_into().unwrap());
        pos -= 8;
        assert_eq!(
            uncompressed_data_size,
            value.len() as u64,
            "Expected uncompressed_data_size to match original data size"
        );
    }
    let index_count = {
        // Check index count in footer.
        let index_count = u32::from_le_bytes(compressed_data[pos - 4..pos].try_into().unwrap());
        pos -= 4;
        assert_eq!(
            index_count as usize,
            EXPECTED_INDEXES.len(),
            "Expected index_count to match"
        );
        index_count
    };
    {
        // Check indexes in footer.
        let byte_count = (index_count * 4) as usize;
        let index_vec_raw = &compressed_data[pos - byte_count..pos];
        pos -= byte_count;
        let mut cursor = Cursor::new(index_vec_raw);
        let mut i = 0;
        while let Ok(index_pos) = cursor.read_u32_le().await {
            assert_eq!(
                index_pos, EXPECTED_INDEXES[i],
                "Expected index to equal at position {}",
                i
            );
            i += 1;
        }
    }
    {
        // `wincode` adds the size again as a u64 before our index vector so check it too.
        let bincode_index_count =
            u64::from_le_bytes(compressed_data[pos - 8..pos].try_into().unwrap());
        pos -= 8;
        assert_eq!(
            bincode_index_count,
            u64::from(index_count),
            "Expected index_count and bincode_index_count to match"
        );
    }
    {
        // Check our footer length.
        let footer_len = u32::from_le_bytes(compressed_data[pos - 4..pos].try_into().unwrap());
        pos -= 4;
        assert_eq!(
            footer_len,
            1 + 4 + 8 + 4 + (index_count * 4) + 8,
            "Expected frame type to be footer"
        );
    }
    {
        // Check our frame type.
        let frame_type = u8::from_le_bytes(compressed_data[pos - 1..pos].try_into().unwrap());
        assert_eq!(
            frame_type, FOOTER_FRAME_TYPE,
            "Expected frame type to be footer"
        );
    }

    // Just as one last sanity check lets check our deserialized footer.
    assert_eq!(
        extract_footer(&compressed_data)?,
        Footer {
            indexes: EXPECTED_INDEXES
                .map(|v| SliceIndex {
                    position_from_prev_index: v
                })
                .to_vec(),
            index_count: u32::try_from(EXPECTED_INDEXES.len()).unwrap_or(u32::MAX),
            uncompressed_data_size: data_len as u64,
            config: Lz4Config {
                block_size: BLOCK_SIZE
            },
            version: CURRENT_STREAM_FORMAT_VERSION,
        },
        "Expected footers to match"
    );

    Ok(())
}

#[nativelink_test]
async fn get_part_is_zero_digest() -> Result<(), Error> {
    const BLOCK_SIZE: u32 = 32 * 1024;

    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store_owned = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config {
                    block_size: BLOCK_SIZE,
                    ..Default::default()
                },
            ),
        },
        Store::new(inner_store.clone()),
    )
    .err_tip(|| "Failed to create compression store")?;
    let store = Pin::new(Arc::new(store_owned));

    let (mut writer, mut reader) = make_buf_channel_pair();

    let _drop_guard = spawn!("get_part_is_zero_digest", async move {
        drop(
            store
                .as_ref()
                .get_part(digest, &mut writer, 0, None)
                .await
                .err_tip(|| "Failed to get_part"),
        );
    });

    let file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading bytes")?;

    let empty_bytes = Bytes::new();
    assert_eq!(&file_data, &empty_bytes, "Expected file content to match");

    Ok(())
}

// Regression test for the bug where start_pos > end_pos in the slice operation
#[nativelink_test]
async fn regression_test_range_start_not_greater_than_end() -> Result<(), Error> {
    // Create a store with a small block size to trigger multiple blocks
    const BLOCK_SIZE: u32 = 64 * 1024; // 64KB, same as DEFAULT_BLOCK_SIZE

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store_owned = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config {
                    block_size: BLOCK_SIZE,
                    ..Default::default()
                },
            ),
        },
        Store::new(inner_store.clone()),
    )
    .err_tip(|| "Failed to create compression store")?;
    let store = Pin::new(&store_owned);

    // Create a large buffer that spans multiple blocks
    let data_size = BLOCK_SIZE as usize * 3; // 3 blocks
    let mut data = vec![0u8; data_size];
    let mut rng = SmallRng::seed_from_u64(42);
    rng.fill(&mut data[..]);

    let digest = DigestInfo::try_new(VALID_HASH, data_size).unwrap();
    store.update_oneshot(digest, data.clone().into()).await?;

    // Try to read exactly at block boundaries with various offsets
    let boundary = u64::from(BLOCK_SIZE);

    // These specific offsets test the case in the bug report where
    // start_pos was 65536 and end_pos was 65535
    for (offset, length) in &[
        (boundary - 1, Some(2u64)),  // Read across block boundary
        (boundary, Some(1u64)),      // Read exactly at block boundary
        (boundary + 1, Some(10u64)), // Read just after block boundary
        // Specifically test the case where offset >= block size
        (u64::from(BLOCK_SIZE), Some(20u64)),
        // Specifically test the case that caused the bug (65536 and 65535)
        (u64::from(BLOCK_SIZE), Some(u64::from(BLOCK_SIZE) - 1)),
        // More edge cases around the block boundary to thoroughly test the issue
        (u64::from(BLOCK_SIZE) - 1, Some(1u64)), // Just before boundary
        (u64::from(BLOCK_SIZE), Some(0u64)),     // Zero length at boundary
        (u64::from(BLOCK_SIZE), Some(u64::MAX)), // Unlimited length at boundary
        (u64::from(BLOCK_SIZE) * 2, Some(u64::from(BLOCK_SIZE) - 1)), // Same issue at next block
    ] {
        // First test with get_part_unchunked
        let result = store.get_part_unchunked(digest, *offset, *length).await;

        // The bug was causing a panic, so just checking that it doesn't panic
        // means the fix is working
        assert!(
            result.is_ok(),
            "Reading with get_part_unchunked at offset {offset} with length {length:?} should not fail"
        );

        let store_data = result.unwrap();

        // Verify the data matches what we expect
        let expected_len = cmp::min(
            usize::try_from(length.unwrap_or(u64::MAX))?,
            data.len().saturating_sub(usize::try_from(*offset)?),
        );
        assert_eq!(
            store_data.len(),
            expected_len,
            "Expected data length to match when reading at offset {} with length {:?}",
            offset,
            length
        );

        if expected_len > 0 {
            let start = usize::try_from(*offset)?;
            let end = start + expected_len;
            assert_eq!(
                &store_data[..],
                &data[start..end],
                "Expected data content to match when reading at offset {} with length {:?}",
                offset,
                length
            );
        }

        // Now also test with the lower-level get_part method to ensure it doesn't panic
        // This is closer to what the bytestream server would call
        let (mut tx, mut rx) = make_buf_channel_pair();

        // The error was happening in this method call
        let get_part_result = store.get_part(digest, &mut tx, *offset, *length).await;
        assert!(
            get_part_result.is_ok(),
            "Reading with get_part at offset {offset} with length {length:?} should not fail"
        );

        // Just to consume the stream and ensure it behaves as expected
        let mut received_data = Vec::new();
        while let Ok(chunk) = rx.consume(Some(1024)).await {
            if chunk.is_empty() {
                break;
            }
            received_data.extend_from_slice(&chunk);
        }

        assert_eq!(
            received_data.len(),
            expected_len,
            "Expected get_part received data length to match when reading at offset {} with length {:?}",
            offset,
            length
        );

        if expected_len > 0 {
            let start = usize::try_from(*offset)?;
            let end = start + expected_len;
            assert_eq!(
                &received_data[..],
                &data[start..end],
                "Expected get_part data content to match when reading at offset {} with length {:?}",
                offset,
                length
            );
        }
    }

    Ok(())
}

/// Regression for red-team F3 + #140: CompressionStore must delegate
/// `mark_stable` to its inner store. Without an explicit override, the
/// trait's silent no-op default would swallow the call, leaving the
/// terminal FastSlowStore's `stable_digests` queue empty and the worker's
/// pin (durable under v2) leaked forever.
///
/// Wraps a FastSlowStore (Memory→Memory) inside CompressionStore. Calls
/// mark_stable on the outer compression layer. Drains the inner
/// FastSlowStore's stable_digests via the outer store's
/// `drain_stable_digests` chain — the digest must be present.
#[nativelink_test]
async fn mark_stable_delegates_to_inner_store_test() -> Result<(), Error> {
    let inner_fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));

    let compression = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config::default(),
            ),
        },
        inner_fast_slow,
    )
    .err_tip(|| "Failed to create compression store")?;

    let digest = DigestInfo::new([7u8; 32], 100);
    let outer = Store::new(compression);
    outer.as_store_driver().mark_stable(&[digest]);

    // The compression layer should have forwarded the call to the inner
    // FastSlowStore, which pushes into its `stable_digests` queue.
    let drained = outer.as_store_driver().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "CompressionStore::mark_stable must delegate to inner_store. \
         Without this delegation the trait silent-default no-op swallows \
         the call and BIS broadcast never wakes; worker pin leaks. \
         Drained: {drained:?}"
    );

    Ok(())
}

/// #336 P1 production-composition test: when CompressionStore's inner
/// store contains data that does NOT have a valid compression header,
/// `read_fut` early-returns Err mid-stream (deserialization or
/// `error_if!` trip). Pre-fix the OUTER `writer` was un-terminated
/// because `read_fut` captured the writer by `&mut` and propagated `?`
/// through every Err path. Wrapping callers (e.g. an upstream
/// VerifyStore composing atop CompressionStore) that joined `(get_fut,
/// reader_fut)` over the outer writer's tx/rx pair deadlocked.
///
/// Drive `Pin::new(&store).get_part(...)` against an inner store that
/// holds corrupt bytes (raw bytes not formatted with a Header). Read
/// from the matching rx in `tokio::join!`. Without the WriteHalfGuard
/// wrap, the reader hangs and the 5-second `tokio::time::timeout`
/// panics with the bespoke message below.
///
/// Mutation step: comment out the `WriteHalfGuard::new(writer)` line +
/// revert `commit_eof()` to `send_eof()` and `writer_guard.send` to
/// `writer.send` — the test must red-fail with the bespoke message.
#[nativelink_test]
async fn compression_store_get_part_terminates_writer_on_corrupt_inner() -> Result<(), Error> {
    use core::time::Duration;

    use nativelink_util::buf_channel::DropCloserWriteHalf;
    use nativelink_util::store_trait::StoreKey;

    let inner_store = MemoryStore::new(&MemorySpec::default());
    let store = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                nativelink_config::stores::Lz4Config::default(),
            ),
        },
        Store::new(inner_store.clone()),
    )
    .err_tip(|| "Failed to create compression store")?;

    // Bypass CompressionStore::update — write raw bytes (no Header) directly
    // to the inner store so CompressionStore::get_part's header decode fails.
    let corrupted = vec![0xAAu8; 256];
    let digest = DigestInfo::new([0xCCu8; 32], corrupted.len() as u64);
    inner_store.update_oneshot(digest, corrupted.into()).await?;

    let (tx, mut rx) = make_buf_channel_pair();
    let mut tx: DropCloserWriteHalf = tx;

    let pinned_store = Pin::new(&store);
    let get_fut = async {
        pinned_store
            .get_part(StoreKey::from(digest), &mut tx, 0, None)
            .await
    };
    let reader_fut = async {
        loop {
            let chunk = rx.recv().await?;
            if chunk.is_empty() {
                return Result::<(), Error>::Ok(());
            }
        }
    };

    let timeout_res = tokio::time::timeout(
        Duration::from_secs(5),
        async { tokio::join!(get_fut, reader_fut) },
    )
    .await
    .expect(
        "CompressionStore::get_part writer-termination contract violated — wrapping \
         caller deadlocked on un-EOF'd writer (corrupt-inner header decode path)",
    );

    let (get_res, _reader_res) = timeout_res;
    assert!(get_res.is_err(), "expected compression get_part Err on corrupt inner header");
    Ok(())
}

/// #336 P1 multi-shape coverage (testing-czar MAJOR-3): the
/// `CompressionStore::get_part` implementation has SEVEN `error_if!`
/// early-return branches (header version mismatch, block-size
/// overflow, init-frame underflow, frame-type mismatch, mid-frame
/// underflow, footer underflow, footer index-count / chunks-count /
/// size mismatches). The existing
/// `compression_store_get_part_terminates_writer_on_corrupt_inner`
/// test covers ONE of these (probably init-frame / header decode
/// against the all-0xAA payload). A regression that removes
/// `WriteHalfGuard` for ONE specific decode-error path (e.g. a future
/// maintainer factoring out the footer-validation block and
/// forgetting to thread the guard through) would survive a single-
/// shape test.
///
/// This table-driven helper exercises the writer-termination contract
/// across THREE distinct corrupt-blob shapes that hit different
/// header/footer/mid-frame decoder branches:
///
///   1. EMPTY inner blob: zero-length payload → header consume returns
///      empty → `error_if!(chunk.len() as u64 != header_size, ...)`
///      at `compression_store.rs:520`.
///   2. ONE-BYTE inner blob: payload too short for header → same
///      header-underflow `error_if!` arm, different boundary condition.
///   3. ALL-0xAA 256-byte inner blob: payload covers the header size
///      but the decoded `Header` has an invalid version field →
///      either decode_from_slice Err (`:527`) or
///      `header.version != CURRENT_STREAM_FORMAT_VERSION` `error_if!`
///      at `:534`.
///
/// Each shape must surface an Err to the wrapping caller inside the
/// 5-second deadlock detector with the SAME bespoke message. If the
/// `WriteHalfGuard` wrap is removed for any of these shapes, the
/// corresponding row red-fails with the shape's distinct timeout
/// message.
///
/// Mutation step: in `compression_store.rs::get_part`, comment out
/// `WriteHalfGuard::new(writer)` — every row of this test must
/// red-fail with its bespoke shape-named message.
#[nativelink_test]
async fn compression_store_get_part_terminates_writer_on_multiple_corrupt_shapes()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_util::buf_channel::DropCloserWriteHalf;
    use nativelink_util::store_trait::StoreKey;

    // (shape_name, payload_bytes)
    let shapes: Vec<(&str, Vec<u8>)> = vec![
        ("empty-inner-blob", Vec::new()),
        ("one-byte-inner-blob", vec![0x00u8]),
        ("all-0xAA-256-byte-inner-blob", vec![0xAAu8; 256]),
    ];

    for (shape_name, corrupted) in shapes {
        let inner_store = MemoryStore::new(&MemorySpec::default());
        let store = CompressionStore::new(
            &CompressionSpec {
                backend: StoreSpec::Memory(MemorySpec::default()),
                compression_algorithm: nativelink_config::stores::CompressionAlgorithm::Lz4(
                    nativelink_config::stores::Lz4Config::default(),
                ),
            },
            Store::new(inner_store.clone()),
        )
        .err_tip(|| "Failed to create compression store")?;

        let payload_len = corrupted.len() as u64;
        let digest = DigestInfo::new([0xCCu8; 32], payload_len);
        inner_store.update_oneshot(digest, corrupted.into()).await?;

        let (tx, mut rx) = make_buf_channel_pair();
        let mut tx: DropCloserWriteHalf = tx;

        let pinned_store = Pin::new(&store);
        let get_fut = async {
            pinned_store
                .get_part(StoreKey::from(digest), &mut tx, 0, None)
                .await
        };
        let reader_fut = async {
            loop {
                let chunk = rx.recv().await?;
                if chunk.is_empty() {
                    return Result::<(), Error>::Ok(());
                }
            }
        };

        let timeout_res = tokio::time::timeout(
            Duration::from_secs(5),
            async { tokio::join!(get_fut, reader_fut) },
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "CompressionStore::get_part writer-termination contract violated for \
                 shape={shape_name} — wrapping caller deadlocked on un-EOF'd writer \
                 (corrupt-inner decode `?`-propagation path)"
            )
        });

        let (get_res, _reader_res) = timeout_res;
        assert!(
            get_res.is_err(),
            "shape={shape_name}: expected compression get_part Err on corrupt inner \
             header/frame, got: {get_res:?}"
        );
    }
    Ok(())
}

// ─── #wincode-prealloc: the shared `WincodeConfig` alias, compression side ──
//
// `WincodeConfig` is SHARED with `DedupStore` (`dedup_store.rs` imports it from
// here), so the single `WINCODE_PREALLOC_LIMIT_BYTES` has to serve both
// consumers. On this side the sequence is `Footer.indexes: Vec<SliceIndex>` at
// 4 bytes per element — decoded from a footer frame read out of the (untrusted,
// corruptible) backend store. `Header` contains NO sequence at all
// (`version: u8` + `Lz4Config { block_size: u32 }` + `UploadSizeInfo`), so header
// decoding is unaffected by any value of the cap.

/// Bytes wincode charges per `Footer.indexes` element.
const SLICE_INDEX_WIRE_STRIDE: usize = size_of::<SliceIndex>();

/// Build the raw leading `BincodeLen` (u64 LE) of a `Footer` declaring `indexes`
/// elements, with NO element bytes following it.
fn footer_bytes_declaring(indexes: u64) -> Vec<u8> {
    indexes.to_le_bytes().to_vec()
}

/// #wincode-prealloc: the compression store's footer decode
/// (`compression_store.rs`, `deserialize::<Footer, WincodeConfig>`) reads an
/// untrusted u64 index count out of the backend blob. With the limit DISABLED —
/// as the bincode→wincode port (`13c77abc`) left it — that count reached
/// `Vec::with_capacity` and a corrupt footer panicked the thread with
/// `capacity overflow`. The bounded cap turns it into a graceful decode `Err`
/// that `get_part` maps to `Code::Internal`.
///
/// Mutation step: set `WINCODE_PREALLOC_LIMIT_BYTES` back to
/// `wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED` — this red-fails with
/// `capacity overflow`.
#[nativelink_test]
async fn compression_footer_decode_rejects_length_over_prealloc_cap() -> Result<(), Error> {
    let over_cap_indexes = (WINCODE_PREALLOC_LIMIT_BYTES / SLICE_INDEX_WIRE_STRIDE) as u64 + 1;
    let err = wincode::config::deserialize::<Footer, WincodeConfig>(
        &footer_bytes_declaring(over_cap_indexes),
        WincodeConfig::new(),
    )
    .expect_err(
        "a Footer declaring more indexes than WINCODE_PREALLOC_LIMIT_BYTES allows must be \
         rejected as a decode Err, not preallocated",
    );
    let err_dbg = format!("{err:?}");
    assert!(
        err_dbg.contains("PreallocationSizeLimit"),
        "over-cap declared footer index count must be rejected by wincode's preallocation guard \
         (ReadError::PreallocationSizeLimit); got: {err_dbg}",
    );
    Ok(())
}

/// #wincode-prealloc: the `Header` carries no sequence, so the shared cap can
/// NEVER reject a legitimate header no matter how tight it is set. This pins that
/// property — it is the load-bearing half of the "one cap can serve the shared
/// alias" argument, and it would break the moment someone adds a `Vec`/`String`
/// field to `Header` (at which point the alias needs re-analysis or a split).
#[nativelink_test]
async fn compression_header_round_trips_independent_of_prealloc_cap() -> Result<(), Error> {
    let header = Header {
        version: CURRENT_STREAM_FORMAT_VERSION,
        config: Lz4Config {
            block_size: DEFAULT_BLOCK_SIZE,
        },
        upload_size: UploadSizeInfo::ExactSize(u64::MAX),
    };
    let encoded = wincode::config::serialize(&header, WincodeConfig::new())
        .expect("Header serialize must not depend on the preallocation cap");
    let decoded =
        wincode::config::deserialize::<Header, WincodeConfig>(&encoded, WincodeConfig::new())
            .expect("Header deserialize must not depend on the preallocation cap");
    assert_eq!(
        decoded, header,
        "Header must round-trip under the bounded preallocation cap — it contains no sequence, \
         so no cap value can reject it",
    );
    Ok(())
}

/// #wincode-prealloc regression guard, compression side: a footer for a genuinely
/// large payload must still serialize AND deserialize. wincode enforces
/// `prealloc_check` on the write path too, so a cap set below the largest
/// legitimate `index_count` would break UPLOADS of large blobs — a working store
/// starting to error, which is worse than the panic being fixed. 2_097_152
/// indexes is an 8 MiB footer sequence (deliberately above wincode's own 4 MiB
/// `DEFAULT_PREALLOCATION_SIZE_LIMIT`) and corresponds to a 128 GiB payload at
/// the default 64 KiB `block_size`.
///
/// Mutation step: set `WINCODE_PREALLOC_LIMIT_BYTES` absurdly low (e.g. `4096`) —
/// this red-fails at the serialize step.
#[nativelink_test]
async fn compression_footer_large_but_legitimate_round_trips() -> Result<(), Error> {
    const LARGE_INDEX_COUNT: usize = 2_097_152;

    let footer = Footer {
        indexes: vec![
            SliceIndex {
                position_from_prev_index: 1234,
            };
            LARGE_INDEX_COUNT
        ],
        index_count: LARGE_INDEX_COUNT as u32,
        uncompressed_data_size: 128 * 1024 * 1024 * 1024,
        config: Lz4Config {
            block_size: DEFAULT_BLOCK_SIZE,
        },
        version: CURRENT_STREAM_FORMAT_VERSION,
    };
    let encoded = wincode::config::serialize(&footer, WincodeConfig::new()).expect(
        "serializing a legitimate large Footer must succeed — wincode enforces the preallocation \
         cap on the WRITE path too, so a cap below the largest legitimate index_count breaks \
         uploads of large blobs",
    );
    let decoded =
        wincode::config::deserialize::<Footer, WincodeConfig>(&encoded, WincodeConfig::new())
            .expect("deserializing a legitimate large Footer must succeed");
    assert_eq!(
        decoded.indexes.len(),
        LARGE_INDEX_COUNT,
        "legitimate large footer must round-trip every index",
    );
    Ok(())
}
