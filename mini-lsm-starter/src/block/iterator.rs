use std::{borrow::Borrow, sync::Arc, usize};

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block  
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let first_key = (&block).first_key.clone();
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn seek_to_index(&mut self, index: usize) {
        if index >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            self.idx = 0;
            return;
        }

        self.idx = index;

        // index位置的kv对在block data中的索引
        let index_offset = self.block.offsets[index] as usize;
        let index_next_offset = if index == self.block.offsets.len() - 1 {
            self.block.data.len()
        } else {
            self.block.offsets[index + 1] as usize
        };

        let mut pair = &self.block.data[index_offset..index_next_offset];

        // 获取key
        let overlap_key_len = pair.get_u16();
        let rest_key_len = pair.get_u16();
        let key_range = (SIZEOF_U16 * 2, SIZEOF_U16 * 2 + rest_key_len as usize);
        let mut key = (&self.first_key.raw_ref()[0..overlap_key_len as usize]).to_vec();
        key.extend(pair[key_range.0..key_range.1].to_vec().iter());
        self.key = KeyVec::from_vec(key);

        let pair = &pair[key_range.1..];

        // 获取value
        let value_len = get_u16_from_data(pair);
        let value_begin = index_offset + key_range.1 + SIZEOF_U16;
        let value_end = index_next_offset;
        self.value_range = (value_begin, value_end);
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // 二分查找，搜索key
        let mut left = 0;
        let mut right = self.block.offsets.len();

        while left < right {
            let mid = left + (right - left) / 2;
            self.seek_to_index(mid);

            if self.key.as_key_slice() > key {
                right = mid;
            } else if self.key.as_key_slice() < key {
                left = mid + 1;
            } else {
                return;
            }
        }

        self.seek_to_index(left);
    }
}

pub fn get_u16_from_data(data: &[u8]) -> u16 {
    ((data[0] as u16) << 8) | (data[1] as u16)
}
