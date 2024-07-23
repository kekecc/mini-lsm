use core::panic;
use std::ops::Bound;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    // for Sstable iters
    upper: Bound<Bytes>,
    // false while key > upper
    is_valid: bool,
    prev_key: Vec<u8>,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>, read_ts: u64) -> Result<Self> {
        let mut iter = iter;
        let mut prev_key = Vec::new();

        loop {
            // 跳过过期的key
            while iter.is_valid() && iter.key().key_ref() == prev_key.as_slice() {
                iter.next()?;
            }

            if !iter.is_valid() {
                break;
            }

            // 记录新的key
            prev_key = iter.key().key_ref().to_vec();

            // 确保新的key符合 key.ts() <= read_ts
            while iter.is_valid() && iter.key().key_ref() == prev_key && iter.key().ts() > read_ts {
                iter.next()?;
            }

            if !iter.is_valid() {
                break;
            }

            // 当前的key中没有符合 ts <= read_ts的
            if iter.key().key_ref() != prev_key {
                continue;
            }

            if !iter.value().is_empty() {
                break;
            }
        }

        let is_valid = if !iter.is_valid() {
            false
        } else {
            match upper.as_ref() {
                Bound::Included(key) => iter.key().key_ref() <= key.as_ref(),
                Bound::Excluded(key) => iter.key().key_ref() < key.as_ref(),
                Bound::Unbounded => true,
            }
        };

        Ok(Self {
            inner: iter,
            upper,
            is_valid,
            prev_key,
            read_ts,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;

        loop {
            while self.inner.is_valid() && self.inner.key().key_ref() == self.prev_key.as_slice() {
                self.inner.next()?;
            }

            if !self.inner.is_valid() {
                break;
            }

            self.prev_key = self.inner.key().key_ref().to_vec();

            while self.inner.is_valid()
                && self.inner.key().key_ref() == self.prev_key
                && self.inner.key().ts() > self.read_ts
            {
                self.inner.next()?;
            }

            if !self.inner.is_valid() {
                break;
            }

            if self.inner.key().key_ref() != self.prev_key {
                continue;
            }

            if !self.inner.value().is_empty() {
                break;
            }
        }

        self.is_valid = if !self.inner.is_valid() {
            false
        } else {
            match self.upper.as_ref() {
                Bound::Included(key) => self.inner.key().key_ref() <= key.as_ref(),
                Bound::Excluded(key) => self.inner.key().key_ref() < key.as_ref(),
                Bound::Unbounded => true,
            }
        };

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        // 必须先判断has_errored
        if self.has_errored || !self.iter.is_valid() {
            false
        } else {
            true
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored || !self.is_valid() {
            panic!("The iterator is corrupted!");
        }

        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.is_valid() {
            panic!("The iterator is corrupted!");
        }

        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::Error::msg("The iterator is corrupted!"));
        }

        if self.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
