#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::min;

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn cmp_key_overlap(first: &KeyVec, second: &KeySlice) -> usize {
    let first = first.key_ref();
    let second = second.key_ref();

    let min_len = min(first.len(), second.len());

    for i in 0..min_len {
        if first[i] != second[i] {
            // ! 返回 i，不是 i + 1！！！
            return i;
        }
    }

    min_len
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty()
            && (self.data.len()
                + self.offsets.len() * 2
                + key.raw_len()
                + value.len()
                + std::mem::size_of::<u16>() * 3)
                > self.block_size
        {
            return false;
        }

        let overlap_key_len = cmp_key_overlap(&self.first_key, &key);
        let rest_key_len = key.key_len() as u16 - overlap_key_len as u16;
        let value_len = value.len() as u16;

        // key-value 的索引
        self.offsets.push(self.data.len() as u16);

        self.data.put_u16(overlap_key_len as u16);
        self.data.put_u16(rest_key_len);
        self.data
            .extend_from_slice(&key.key_ref()[overlap_key_len..]);
        // for mvcc, insert ts
        self.data.put_u64(key.ts());
        self.data.put_u16(value_len);
        self.data.extend_from_slice(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("BlockBuilder is empty when called build");
        }

        Block {
            data: self.data,
            offsets: self.offsets,
            first_key: self.first_key,
        }
    }

    /// For test
    pub fn size(&self) -> usize {
        self.data.len() + self.offsets.len() * 2
    }
}
