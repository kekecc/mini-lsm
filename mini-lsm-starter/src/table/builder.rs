#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::BufMut;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    hash_keys: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            hash_keys: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        assert!(!key.is_empty());

        if self.first_key.is_empty() {
            self.first_key.clear();
            self.first_key.extend_from_slice(key.raw_ref());
        }

        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend_from_slice(key.raw_ref());
            self.hash_keys.push(farmhash::fingerprint32(&key.raw_ref()));
            return;
        }

        self.build_block();

        // 再次调用
        self.add(key, value);
    }

    fn build_block(&mut self) {
        // 替换builder
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block_data = builder.build();
        let first_key = self.first_key.clone();
        self.first_key.clear();
        let last_key = self.last_key.clone();
        self.last_key.clear();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(first_key.into()),
            last_key: KeyBytes::from_bytes(last_key.into()),
        });
        self.data.put(block_data.encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // ! Attention: build the last block
        self.build_block();

        let mut data = self.data;
        let meta_block_offset = data.len();
        let meta_data = BlockMeta::encode_block_meta(&self.meta, &mut data);
        data.put_u32(meta_block_offset as u32);
        let bloom_offset = data.len();
        let bits_per_key = Bloom::bloom_bits_per_key(self.hash_keys.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.hash_keys, bits_per_key);
        bloom.encode(&mut data);
        data.put_u32(bloom_offset as u32);

        Ok(SsTable {
            file: FileObject::create(path.as_ref(), data)?,
            block_meta_offset: meta_block_offset,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
