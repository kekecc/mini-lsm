use core::panic;
use std::ops::Bound;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    // for Sstable iters
    upper: Bound<Bytes>,
    // false while key > upper
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        let mut iter = iter;
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }

        let is_valid = if !iter.is_valid() {
            false
        } else {
            match upper.as_ref() {
                Bound::Included(key) => iter.key().raw_ref() <= key.as_ref(),
                Bound::Excluded(key) => iter.key().raw_ref() < key.as_ref(),
                Bound::Unbounded => true,
            }
        };

        Ok(Self {
            inner: iter,
            upper,
            is_valid,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;

        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }

        self.is_valid = if !self.inner.is_valid() {
            false
        } else {
            match self.upper.as_ref() {
                Bound::Included(key) => self.inner.key().raw_ref() <= key.as_ref(),
                Bound::Excluded(key) => self.inner.key().raw_ref() < key.as_ref(),
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
