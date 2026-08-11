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
use std::sync::Arc;

use async_trait::async_trait;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, BytesMut};
use futures::future::FutureExt;
use lz4_flex::block::{compress_into, decompress_into, get_maximum_output_size};
use nativelink_config::stores::CompressionSpec;
use nativelink_error::{Code, Error, ResultExt, error_if, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, WriteHalfGuard, make_buf_channel_pair,
};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::spawn;
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike,
    UploadSizeInfo,
};
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::cas_utils::is_zero_digest;

// In the event the bytestream format changes this number should be incremented to prevent
// backwards compatibility issues.
pub const CURRENT_STREAM_FORMAT_VERSION: u8 = 1;

// Default block size that will be used to slice stream into.
pub const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024;

const U32_SZ: u64 = size_of::<u32>() as u64;

// We use a custom frame format here because I wanted the ability in the future to:
// * Read a random part of the data without needing to parse entire file.
// * Compress the data on the fly without needing to know the exact input size.
//
// The frame format that LZ4 uses does not contain an index of where the different
// blocks are located. This would mean in the event we only wanted the last byte of
// a file, we'd need to seek to the header of each block to find where the next block
// offset is until we got to the last block then decompress it.
//
// By using this custom frame format we keep the ability to have each block reference
// the next block (for efficiency), but also after all blocks we have a footer frame
// which contains an index of each block in the stream. This means that we can read a
// fixed number of bytes of the file, which always contain the size of the index, then
// with that size we know the exact size of the footer frame, which contains the entire
// index. Once this footer frame is loaded, we could then do some math to figure out
// which block each byte is in and the offset of each block within the compressed file.
//
// The frame format is as follows:
// |----------------------------------HEADER-----------------------------------------|
// |  version(u8) |  block_size (u32) |  upload_size_type (u32) |  upload_size (u32) |
// |----------------------------------BLOCK------------------------------------------|
// |  frame_type(u8) 0x00 |  compressed_data_size (u32) |        ...DATA...          |
// |                                ...DATA...                                       |
// | [Possibly repeat block]                                                         |
// |----------------------------------FOOTER-----------------------------------------|
// |  frame_type(u8) 0x01 |    footer_size (u32) |           index_count1 (u64)      |
// |      ...[pos_from_prev_index (u32) - repeat for count {index_count*}]...        |
// |  index_count2 (u32) |    uncompressed_data_sz (u64) |    block_size (u32)       |
// | version (u8) |------------------------------------------------------------------|
// |---------------------------------------------------------------------------------|
//
// version              - A constant number used to define what version of this format is being
//                        used. Version in header and footer must match.
// block_size           - Size of each block uncompressed except for last block. This means that
//                        every block uncompressed will be a constant size except last block may
//                        be variable size. Block size in header and footer must match.
// upload_size_type     - Value of 0 = UploadSizeInfo::ExactSize, 1 = UploadSizeInfo::MaxSize.
//                        This is for debug reasons only.
// upload_size          - The size of the data. WARNING: Do not rely on this being the uncompressed
//                        payload size. It is a debug field and a "best guess" on how large the data
//                        is. The header does not contain the upload data size. This value is the
//                        value counter part to what the `upload_size_type` field.
// frame_type           - Type of each frame. 0 = BLOCK frame, 1 = FOOTER frame. Header frame will
//                        always start with the first byte of the stream, so no magic number for it.
// compressed_data_size - The size of this block. The bytes after this field should be read
//                        in sequence to get all of the block's data in this block.
// footer_size          - Size of the footer for bytes after this field.
// index_count1         - Number of items in the index. ({index_count1} * 4) represents the number
//                        of bytes that should be read after this field in order to get all index
//                        data.
// pos_from_prev_index  - Index of an individual block in the stream relative to the previous
//                        block. This field may repeat {index_count} times.
// index_count2         - Same as {index_count1} and should always be equal. This field might be
//                        useful if you want to read a random byte from this stream, have random
//                        access to it and know the size of the stream during reading, because
//                        this field is always in the exact same place in the stream relative to
//                        the last byte.
// uncompressed_data_sz - Size of the original uncompressed data.
//
// Note: All fields fields little-endian.

/// Number representing a chunk.
pub const CHUNK_FRAME_TYPE: u8 = 0;

/// Number representing the footer.
pub const FOOTER_FRAME_TYPE: u8 = 1;

/// This is a partial mirror of `nativelink_config::stores::Lz4Config`.
/// We cannot use that natively here because it could cause our
/// serialized format to change if we added more configs.
#[derive(
    Serialize, Deserialize, PartialEq, Eq, Debug, Default, Copy, Clone, SchemaWrite, SchemaRead,
)]
pub struct Lz4Config {
    pub block_size: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy, SchemaWrite, SchemaRead)]
pub struct Header {
    pub version: u8,
    pub config: Lz4Config,
    pub upload_size: UploadSizeInfo,
}

#[derive(
    Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone, Copy, SchemaWrite, SchemaRead,
)]
pub struct SliceIndex {
    pub position_from_prev_index: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, SchemaWrite, SchemaRead)]
pub struct Footer {
    pub indexes: Vec<SliceIndex>,
    pub index_count: u32,
    pub uncompressed_data_size: u64,
    pub config: Lz4Config,
    pub version: u8,
}

/// `lz4_flex::block::get_maximum_output_size()` way over estimates, so we use the
/// one provided here: <https://github.com/torvalds/linux/blob/master/include/linux/lz4.h#L61>
/// Local testing shows this gives quite accurate worst case given random input.
const fn lz4_compress_bound(input_size: u64) -> u64 {
    input_size + (input_size / 255) + 16
}

fn serialized_size<C, V>(value: &V, config: C) -> Result<u64, Error>
where
    C: wincode::config::Config,
    V: SchemaWrite<C, Src = V>,
{
    wincode::config::serialized_size(value, config).map_err(|e| {
        make_err!(
            Code::Internal,
            "Failed to calculate serialized size: {:?}",
            e
        )
    })
}

struct UploadState {
    header: Header,
    footer: Footer,
    max_output_size: u64,
    input_max_size: u64,
}

impl UploadState {
    pub(crate) fn new(
        store: &CompressionStore,
        upload_size: UploadSizeInfo,
    ) -> Result<Self, Error> {
        let input_max_size = match upload_size {
            UploadSizeInfo::MaxSize(sz) | UploadSizeInfo::ExactSize(sz) => sz,
        };

        let max_index_count = (input_max_size / u64::from(store.config.block_size)) + 1;

        let header = Header {
            version: CURRENT_STREAM_FORMAT_VERSION,
            config: Lz4Config {
                block_size: store.config.block_size,
            },
            upload_size,
        };
        let footer = Footer {
            indexes: vec![
                SliceIndex::default();
                usize::try_from(max_index_count)
                    .err_tip(|| "Could not convert max_index_count to usize")?
            ],
            index_count: u32::try_from(max_index_count)
                .err_tip(|| "Could not convert max_index_count to u32")?,
            uncompressed_data_size: 0, // Updated later.
            config: header.config,
            version: CURRENT_STREAM_FORMAT_VERSION,
        };

        // This is more accurate of an estimate than what get_maximum_output_size calculates.
        let max_block_size = lz4_compress_bound(u64::from(store.config.block_size)) + U32_SZ + 1;

        let max_output_size = {
            let header_size = serialized_size(&header, store.wincode_config)?;
            let max_content_size = max_block_size * max_index_count;
            let max_footer_size = U32_SZ + 1 + serialized_size(&footer, store.wincode_config)?;
            header_size + max_content_size + max_footer_size
        };

        Ok(Self {
            header,
            footer,
            max_output_size,
            input_max_size,
        })
    }
}

/// Upper bound, in BYTES, on any single sequence wincode will preallocate while
/// DEserializing with [`WincodeConfig`].
///
/// Both sequences decoded under this alias — [`Footer::indexes`] here and
/// `DedupStore`'s `DedupIndex::entries` — come out of an ordinary (evictable,
/// corruptible) backing store, so their leading `BincodeLen` u64 count is
/// UNTRUSTED input. With the limit disabled
/// (`wincode::config::PREALLOCATION_SIZE_LIMIT_DISABLED`, as the bincode→wincode
/// port `13c77abc` left it) `SeqLen::read_prealloc_check` is a NO-OP and
/// `Vec::with_capacity` runs on that untrusted count → `capacity overflow`
/// PANIC. bincode's `NoLimit` was NOT equivalent: its serde `Vec` visitor uses a
/// cautious capacity hint and decodes element-by-element against the real reader,
/// so a bogus length surfaced as a decode `Err`. A bounded limit restores that
/// behavior — an over-long count is a graceful `Err`, which `DedupStore::has`
/// degrades to `Ok(None)` and `get_part` maps to `Code::Internal`.
///
/// SIZING. The limit REJECTS rather than caps-and-grows, and wincode applies it
/// on the WRITE path too (`SeqLen::write_bytes_needed_prealloc_check`), so a
/// value set too low would break serialization of a legitimate large blob — a
/// working store starting to error, worse than the panic it guards. wincode
/// charges `len * size_of::<Elem>()` bytes; at 64 MiB:
///
/// * [`Footer::indexes`] is `Vec<SliceIndex>`, 4 B/elem, and
///   `index_count = input_size / block_size + 1`. 64 MiB / 4 B = 16_777_216
///   indexes — a 1 TiB payload at the default 64 KiB `block_size`, still 64 GiB
///   at a 4 KiB one. (A footer that large is already a 64 MiB eager
///   `vec![SliceIndex::default(); n]` in `UploadState::new`.)
/// * `DedupIndex::entries` is `Vec<DigestInfo>`, 40 B/elem
///   (`PackedHash([u8; 32])` + `u64`). FastCDC never emits a chunk below
///   `min_size` except the final flush (`nativelink_util::fastcdc`, `:91`/`:119`),
///   so `entries <= blob_size / min_size + 1`. 64 MiB / 40 B = 1_677_721
///   entries — a 102 GiB blob at the default 64 KiB `min_size`, still 6.4 GiB at
///   a 4 KiB one.
/// * [`Header`] contains no sequence at all, so header decoding is unaffected by
///   any value here.
///
/// Both envelopes sit orders of magnitude above any REAPI CAS blob, so ONE limit
/// serves the shared alias and the two consumers do not need separate configs.
/// The value matches the workspace's two other bounded wincode configs
/// (`nativelink_scheduler::resource_profile_persist::SNAPSHOT_PREALLOC_LIMIT`,
/// `nativelink_scheduler::dag_criticality::DAG_PREALLOC_LIMIT`) and is 16x
/// wincode's own `DEFAULT_PREALLOCATION_SIZE_LIMIT` of 4 MiB.
// CAPPED AT 64 MiB: worst-case allocation for one corrupt index/footer decode on
// a network-reachable store path; see the sizing analysis above.
pub const WINCODE_PREALLOC_LIMIT_BYTES: usize = 64 << 20;

// Based off of an older Bincode config, but with a BOUNDED preallocation limit
// for untrusted stored data (see [`WINCODE_PREALLOC_LIMIT_BYTES`]).
pub type WincodeConfig = wincode::config::Configuration<
    true,
    WINCODE_PREALLOC_LIMIT_BYTES,
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    wincode::int_encoding::FixInt,
    u32,
>;

/// This store will compress data before sending it on to the inner store.
/// Note: Currently using `get_part()` and trying to read part of the data will
/// result in the entire contents being read from the inner store but will
/// only send the contents requested.
#[derive(MetricsComponent)]
pub struct CompressionStore {
    #[metric(group = "inner_store")]
    inner_store: Store,
    config: nativelink_config::stores::Lz4Config,
    wincode_config: WincodeConfig,
}

impl core::fmt::Debug for CompressionStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CompressionStore")
            .field("inner_store", &self.inner_store)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CompressionStore {
    pub fn new(spec: &CompressionSpec, inner_store: Store) -> Result<Arc<Self>, Error> {
        let lz4_config = match spec.compression_algorithm {
            nativelink_config::stores::CompressionAlgorithm::Lz4(mut lz4_config) => {
                if lz4_config.block_size == 0 {
                    lz4_config.block_size = DEFAULT_BLOCK_SIZE;
                }
                if lz4_config.max_decode_block_size == 0 {
                    lz4_config.max_decode_block_size = lz4_config.block_size;
                }
                lz4_config
            }
        };
        Ok(Arc::new(Self {
            inner_store,
            config: lz4_config,
            wincode_config: WincodeConfig::new(),
        }))
    }
}

#[async_trait]
impl StoreDriver for CompressionStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        self.inner_store.clone().into_inner().post_init().await?;
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner_store.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        let mut output_state = UploadState::new(&self, upload_size)?;

        let (mut tx, rx) = make_buf_channel_pair();

        let inner_store = self.inner_store.clone();
        let key = key.into_owned();
        let update_fut = spawn!("compression_store_update_spawn", async move {
            inner_store
                .update(
                    key,
                    rx,
                    UploadSizeInfo::MaxSize(output_state.max_output_size),
                )
                .await
                .err_tip(|| "Inner store update in compression store failed")
        })
        .map(
            |result| match result.err_tip(|| "Failed to run compression update spawn") {
                Ok(inner_result) => {
                    inner_result.err_tip(|| "Compression underlying store update failed")
                }
                Err(e) => Err(e),
            },
        );

        let write_fut = async move {
            // Writer-termination contract for `tokio::join!(write_fut, update_fut)`:
            // `update_fut` (= `inner_store.update(key, rx, ...)`) reads
            // `rx` paired with `tx`. Any `?`-propagated Err below
            // (header serialize, reader.consume, error_if size overshoot,
            // compress_into, mid-stream tx.send, footer serialize, footer
            // tx.send, send_eof) used to drop `tx` silently — `rx.recv()`
            // in the inner store then blocked until the OS dropped the
            // sender, then synthesized
            // `Code::Internal "Sender dropped before sending EOF"`
            // instead of returning a structured Err. Sibling of #245
            // (`verify_store::inner_check_update`), see audit at
            // `.claude/audits/245-fast-slow-store-sender-drop.md`.
            //
            // The `WriteHalfGuard` wrap below upgrades the Drop fallback
            // so on any `?` propagation the guard's `Drop`
            // (`buf_channel.rs:516-520`) synthesizes a structured Internal
            // carrying the greppable `"buf_channel: writer dropped without
            // commit"` marker — the paired `rx.recv()` reader observes
            // that structured Err immediately instead of the misleading
            // "Sender dropped" derivative. The OUTER `tokio::join!(...)`
            // already preserves the actionable upstream cause on its
            // returned Result; the Drop fallback on the reader is purely
            // defense-in-depth so it unblocks, not the carrier of the
            // upstream cause. On the success path, `commit_eof()`
            // suppresses the fallback after `send_eof()`.
            let mut tx_guard = WriteHalfGuard::new(&mut tx);
            {
                // Write Header.
                let serialized_header =
                    wincode::config::serialize(&output_state.header, self.wincode_config).map_err(
                        |e| make_err!(Code::Internal, "Failed to serialize header : {:?}", e),
                    )?;
                (*tx_guard)
                    .send(serialized_header.into())
                    .await
                    .err_tip(|| "Failed to write compression header on upload")?;
            }

            let mut received_amt = 0;
            let mut index_count: u32 = 0;
            for index in &mut output_state.footer.indexes {
                let chunk = reader
                    .consume(Some(self.config.block_size as usize))
                    .await
                    .err_tip(|| "Failed to read take in update in compression store")?;
                if chunk.is_empty() {
                    break; // EOF.
                }

                received_amt += u64::try_from(chunk.len())
                    .err_tip(|| "Could not convert chunk.len() to u64")?;
                error_if!(
                    received_amt > output_state.input_max_size,
                    "Got more data than stated in compression store upload request"
                );

                let max_output_size = get_maximum_output_size(self.config.block_size as usize);
                let mut compressed_data_buf = BytesMut::with_capacity(max_output_size);
                compressed_data_buf.put_u8(CHUNK_FRAME_TYPE);
                compressed_data_buf.put_u32_le(0); // Filled later.

                // For efficiency reasons we do some raw slice manipulation so we can write directly
                // into our buffer instead of having to do another allocation.
                let raw_compressed_data = unsafe {
                    core::slice::from_raw_parts_mut(
                        compressed_data_buf.chunk_mut().as_mut_ptr(),
                        max_output_size,
                    )
                };

                let compressed_data_sz = compress_into(&chunk, raw_compressed_data)
                    .map_err(|e| make_err!(Code::Internal, "Compression error {:?}", e))?;
                unsafe {
                    compressed_data_buf.advance_mut(compressed_data_sz);
                }

                // Now fill the size in our slice.
                LittleEndian::write_u32(
                    &mut compressed_data_buf[1..5],
                    u32::try_from(compressed_data_sz).unwrap_or(u32::MAX),
                );

                // Now send our chunk.
                (*tx_guard)
                    .send(compressed_data_buf.freeze())
                    .await
                    .err_tip(|| "Failed to write chunk to inner store in compression store")?;

                index.position_from_prev_index =
                    u32::try_from(compressed_data_sz).unwrap_or(u32::MAX);

                index_count += 1;
            }
            // Index 0 is actually a pointer to the second chunk. This is because we don't need
            // an index for the first item, since it starts at position `{header_len}`.
            // The code above causes us to create 1 more index than we actually need, so we
            // remove the last index from our vector here, because at this point we are always
            // one index too many.
            // Note: We need to be careful that if we don't have any data (zero bytes) it
            // doesn't go to -1.
            index_count = index_count.saturating_sub(1);
            output_state
                .footer
                .indexes
                .resize(index_count as usize, SliceIndex::default());
            output_state.footer.index_count =
                u32::try_from(output_state.footer.indexes.len()).unwrap_or(u32::MAX);
            output_state.footer.uncompressed_data_size = received_amt;
            {
                // Write Footer.
                let serialized_footer =
                    wincode::config::serialize(&output_state.footer, self.wincode_config).map_err(
                        |e| make_err!(Code::Internal, "Failed to serialize header : {:?}", e),
                    )?;

                let mut footer = BytesMut::with_capacity(1 + 4 + serialized_footer.len());
                footer.put_u8(FOOTER_FRAME_TYPE);
                footer.put_u32_le(u32::try_from(serialized_footer.len()).unwrap_or(u32::MAX));
                footer.extend_from_slice(&serialized_footer);

                (*tx_guard)
                    .send(footer.freeze())
                    .await
                    .err_tip(|| "Failed to write footer to inner store in compression store")?;
                tx_guard
                    .commit_eof()
                    .err_tip(|| "Failed writing EOF in compression store update")?;
            }

            Result::<u64, Error>::Ok(received_amt)
        };
        let (write_result, update_result) = tokio::join!(write_fut, update_fut);
        match (write_result, update_result) {
            (Ok(size), Ok(_)) => Ok(size),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }

    // LINT: writer-termination policy for `get_part` (#336 P1 fix).
    //
    // CompressionStore is a wrapper layer (sub-store: inner_store). Its
    // `get_part` writes to the OUTER caller's borrowed writer through
    // SEVEN `error_if!` early-returns (header version mismatch, block-size
    // overflow, init-frame underflow, frame-type mismatch, mid-frame
    // underflow, footer underflow, footer index-count / chunks-count /
    // size mismatches) AND eight `?`-propagated paths inside `read_fut`
    // (consume, deserialize, decompress, send). Without termination on
    // every exit path, any wrapping caller that joins `(get_fut,
    // reader_fut)` over the OUTER writer's tx/rx pair (e.g. an upstream
    // VerifyStore) deadlocks because the reader never observes EOF or
    // error. The bug was historically catastrophic: a corrupt blob's
    // header decode failure → `read_fut` returns Err → outer writer never
    // terminated → upstream deadlock.
    //
    // Wrap the borrowed `writer` in `WriteHalfGuard::new(writer)` and
    // move the guard into `read_fut` (which is the only future that
    // touches the writer). The Drop fallback fires the synthesized
    // `Code::Internal "buf_channel: writer dropped without commit"` on
    // any uncommitted exit so the paired reader unblocks. Happy-path
    // uses `commit_eof()` to suppress the fallback.
    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in filesystem store get_part")?;
            return Ok(());
        }

        let (tx, mut rx) = make_buf_channel_pair();

        let inner_store = self.inner_store.clone();
        let key = key.into_owned();
        let get_part_fut = spawn!("compression_store_get_part_spawn", async move {
            inner_store
                .get_part(key, tx, 0, None)
                .await
                .err_tip(|| "Inner store get in compression store failed")
        })
        .map(
            |result| match result.err_tip(|| "Failed to run compression get spawn") {
                Ok(inner_result) => {
                    inner_result.err_tip(|| "Compression underlying store get failed")
                }
                Err(e) => Err(e),
            },
        );
        let read_fut = async move {
            let mut writer_guard = WriteHalfGuard::new(writer);
            let header = {
                // Read header.
                const EMPTY_HEADER: Header = Header {
                    version: CURRENT_STREAM_FORMAT_VERSION,
                    config: Lz4Config { block_size: 0 },
                    upload_size: UploadSizeInfo::ExactSize(0),
                };
                let header_size = serialized_size(&EMPTY_HEADER, self.wincode_config)?;
                let chunk = rx
                    .consume(Some(usize::try_from(header_size).unwrap_or(usize::MAX)))
                    .await
                    .err_tip(|| "Failed to read header in get_part compression store")?;
                error_if!(
                    chunk.len() as u64 != header_size,
                    "Expected inner store to return the proper amount of data in compression store {} != {}",
                    chunk.len(),
                    header_size,
                );

                wincode::config::deserialize::<Header, WincodeConfig>(
                    &chunk,
                    self.wincode_config,
                )
                .map_err(|e| make_err!(Code::Internal, "Failed to deserialize header : {:?}", e))?
            };

            error_if!(
                header.version != CURRENT_STREAM_FORMAT_VERSION,
                "Expected header version to match in get compression, got {}, want {}",
                header.version,
                CURRENT_STREAM_FORMAT_VERSION
            );
            error_if!(
                header.config.block_size > self.config.max_decode_block_size,
                "Block size is too large in compression, got {} > {}",
                header.config.block_size,
                self.config.max_decode_block_size
            );

            let mut chunk = rx
                .consume(Some(1 + 4))
                .await
                .err_tip(|| "Failed to read init frame info in compression store")?;
            error_if!(
                chunk.len() < 1 + 4,
                "Received EOF too early while reading init frame info in compression store"
            );

            let mut frame_type = chunk.get_u8();
            let mut frame_sz = chunk.get_u32_le();

            let mut uncompressed_data_sz: u64 = 0;
            let mut remaining_bytes_to_send: u64 = length.unwrap_or(u64::MAX);
            let mut chunks_count: u32 = 0;
            while frame_type != FOOTER_FRAME_TYPE {
                error_if!(
                    frame_type != CHUNK_FRAME_TYPE,
                    "Expected frame to be BODY in compression store, got {} at {}",
                    frame_type,
                    chunks_count
                );

                let chunk = rx
                    .consume(Some(frame_sz as usize))
                    .await
                    .err_tip(|| "Failed to read chunk in get_part compression store")?;
                if chunk.len() < frame_sz as usize {
                    return Err(make_err!(
                        Code::Internal,
                        "Got EOF earlier than expected. Maybe the data is not compressed or different format?"
                    ));
                }
                {
                    let max_output_size =
                        get_maximum_output_size(header.config.block_size as usize);
                    let mut uncompressed_data = BytesMut::with_capacity(max_output_size);

                    // For efficiency reasons we do some raw slice manipulation so we can write directly
                    // into our buffer instead of having to do another allocation.
                    let raw_decompressed_data = unsafe {
                        core::slice::from_raw_parts_mut(
                            uncompressed_data.chunk_mut().as_mut_ptr(),
                            max_output_size,
                        )
                    };

                    let uncompressed_chunk_sz = decompress_into(&chunk, raw_decompressed_data)
                        .map_err(|e| make_err!(Code::Internal, "Decompression error {:?}", e))?;
                    unsafe { uncompressed_data.advance_mut(uncompressed_chunk_sz) };
                    let new_uncompressed_data_sz =
                        uncompressed_data_sz + uncompressed_chunk_sz as u64;
                    if new_uncompressed_data_sz >= offset && remaining_bytes_to_send > 0 {
                        let start_pos =
                            usize::try_from(offset.saturating_sub(uncompressed_data_sz))
                                .unwrap_or(usize::MAX);
                        let end_pos = cmp::min(
                            start_pos.saturating_add(
                                usize::try_from(remaining_bytes_to_send).unwrap_or(usize::MAX),
                            ),
                            uncompressed_chunk_sz,
                        );
                        if end_pos != start_pos {
                            // Make sure we don't send an EOF by accident.
                            writer_guard
                                .send(uncompressed_data.freeze().slice(start_pos..end_pos))
                                .await
                                .err_tip(|| "Failed sending chunk in compression store")?;
                        }
                        remaining_bytes_to_send -= (end_pos - start_pos) as u64;
                    }
                    uncompressed_data_sz = new_uncompressed_data_sz;
                }
                chunks_count += 1;

                let mut chunk = rx
                    .consume(Some(1 + 4))
                    .await
                    .err_tip(|| "Failed to read frame info in compression store")?;
                error_if!(
                    chunk.len() < 1 + 4,
                    "Received EOF too early while reading frame info in compression store"
                );

                frame_type = chunk.get_u8();
                frame_sz = chunk.get_u32_le();
            }
            // Index count will always be +1 (unless it is zero bytes long).
            chunks_count = chunks_count.saturating_sub(1);
            {
                // Read and validate footer.
                let chunk = rx
                    .consume(Some(frame_sz as usize))
                    .await
                    .err_tip(|| "Failed to read chunk in get_part compression store")?;
                error_if!(
                    chunk.len() < frame_sz as usize,
                    "Unexpected EOF when reading footer in compression store get_part"
                );

                let footer = wincode::config::deserialize::<Footer, WincodeConfig>(
                    &chunk,
                    self.wincode_config,
                )
                .map_err(|e| make_err!(Code::Internal, "Failed to deserialize footer : {:?}", e))?;

                error_if!(
                    header.version != footer.version,
                    "Expected header and footer versions to match compression store get_part, {} != {}",
                    header.version,
                    footer.version
                );
                error_if!(
                    footer.indexes.len() != footer.index_count as usize,
                    "Expected index counts to match in compression store footer in get_part, {} != {}",
                    footer.indexes.len(),
                    footer.index_count
                );
                error_if!(
                    footer.index_count != chunks_count,
                    concat!(
                        "Expected index counts to match received chunks count ",
                        "in compression store footer in get_part, {} != {}"
                    ),
                    footer.index_count,
                    chunks_count
                );
                error_if!(
                    footer.uncompressed_data_size != uncompressed_data_sz,
                    "Expected uncompressed data sizes to match in compression store footer in get_part, {} != {}",
                    footer.uncompressed_data_size,
                    uncompressed_data_sz
                );
            }

            writer_guard
                .commit_eof()
                .err_tip(|| "Failed to send eof in compression store write")?;
            Ok(())
        };

        let (read_result, get_part_fut_result) = tokio::join!(read_fut, get_part_fut);
        // Propagate errors from both futures. Previously, if read_fut
        // succeeded but get_part_fut failed (e.g., inner store returned
        // NotFound), the error was silently swallowed — masking real
        // data-loss errors from the caller.
        match (read_result, get_part_fut_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(e), Ok(())) => Err(e),
            (Ok(()), Err(e)) => Err(e),
            (Err(read_err), Err(get_err)) => Err(get_err.merge(read_err)),
        }
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        self.inner_store.register_item_callback(callback)
    }

    /// CompressionStore is a single-inner wrapper. The compression layer
    /// transforms data on the wire but does not own the BIS or pin chain
    /// — both forward unchanged via the trait defaults.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner_store.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner_store.as_store_driver())
    }

    /// `mark_stable` forwards unchanged to `inner_store` via the trait
    /// default's `Inner` arm (task #157 / C+D folded mark_stable into the
    /// forced-delegation enum mechanism).
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner_store.as_store_driver())
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner_store.as_store_driver())
    }
}

default_health_status_indicator!(CompressionStore);
