use std::sync::Arc;

use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn move_to_valid(&mut self) -> Result<()> {
        loop {
            if let Some(current) = self.current.as_ref() {
                if current.is_valid() {
                    break;
                }

                if self.next_sst_idx >= self.sstables.len() {
                    self.current = None;
                } else {
                    self.current = Some(SsTableIterator::create_and_seek_to_first(
                        self.sstables[self.next_sst_idx].clone(),
                    )?);
                    self.next_sst_idx += 1;
                }
            } else {
                break;
            }
        }

        Ok(())
    }
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.len() == 0 {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables: Vec::new(),
            });
        }

        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
        };

        iter.move_to_valid()?;

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        // 查找key的位置
        let mut left = 0;
        let mut right = sstables.len();
        let mut idx = 0;
        while left < right {
            let mid = (right - left) / 2 + left;

            if sstables[mid].first_key().raw_ref() == key.raw_ref() {
                idx = mid;
                break;
            } else if sstables[mid].first_key().raw_ref() < key.raw_ref() {
                left += 1;
            } else if sstables[mid].first_key().raw_ref() > key.raw_ref() {
                right = mid;
            }

            if left == right {
                idx = if left == 0 { 0 } else { left - 1 };
            }
        }

        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }

        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?),
            next_sst_idx: idx + 1,
            sstables,
        };

        iter.move_to_valid()?;

        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = self.current.as_ref() {
            current.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        if let Some(current) = self.current.as_mut() {
            current.next()?;
        }

        self.move_to_valid()?;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
