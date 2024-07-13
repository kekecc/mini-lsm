#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    fn new_without_params() -> Self {
        Self {
            offset: 0,
            first_key: KeyBytes::from_bytes(Bytes::new()),
            last_key: KeyBytes::from_bytes(Bytes::new()),
        }
    }
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);

            let first_key_len = meta.first_key.len();
            buf.put_u16(first_key_len as u16);
            buf.put(meta.first_key.raw_ref());

            let last_key_len = meta.last_key.len();
            buf.put_u16(last_key_len as u16);
            buf.put(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut meta_blocks = Vec::new();
        loop {
            let mut meta = BlockMeta::new_without_params();

            let offset = buf.get_u32();
            meta.offset = offset as usize;

            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            meta.first_key = KeyBytes::from_bytes(first_key);

            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            meta.last_key = KeyBytes::from_bytes(last_key);

            meta_blocks.push(meta);

            if !buf.has_remaining() {
                break;
            }
        }

        meta_blocks
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_offset = file.read(file.size() - 4, 4)?.as_slice().get_u32() as u64;
        let bloom_buf = file.read(bloom_offset, file.size() - bloom_offset - 4)?;
        let bloom = Bloom::decode(&bloom_buf[..])?;

        let block_meta_offset = file.read(bloom_offset - 4, 4)?.as_slice().get_u32() as u64;
        let meta_blocks_data =
            file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let meta_blocks = BlockMeta::decode_block_meta(&(meta_blocks_data[..]));

        let first_key = meta_blocks.first().unwrap().first_key.clone();
        let last_key = meta_blocks.last().unwrap().last_key.clone();

        Ok(Self {
            file,
            block_meta: meta_blocks,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            return Err(anyhow::Error::msg("block_idx out of range!"));
        }

        let meta = &self.block_meta[block_idx];

        // 确保读取的是完整的一个block，不多不少
        let begin_offset = meta.offset;
        let end_offset = if block_idx == self.block_meta.len() - 1 {
            self.block_meta_offset
        } else {
            (&self.block_meta[block_idx + 1]).offset
        };

        let block_data = self
            .file
            .read(begin_offset as u64, (end_offset - begin_offset) as u64)?;

        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;

            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut left = 0;
        let mut right = self.block_meta.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if key < self.block_meta[mid].first_key.as_key_slice() {
                right = mid;
            } else if key > self.block_meta[mid].first_key.as_key_slice() {
                left = mid + 1;
            } else {
                return mid;
            }
        }

        if right == 0 {
            0
        } else {
            right - 1
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
