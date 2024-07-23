#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return MergeIterator {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut binary_heap = BinaryHeap::new();

        // to pass week1_day6 test3, consider to make current not None when all iters are invalid
        if iters.iter().all(|x| !x.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: binary_heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                binary_heap.push(HeapWrapper(index, iter));
            }
        }

        let current = binary_heap.pop().unwrap();

        // 如果iters内所有的iter都是in_valid，那么MergeIterator将为空，MergeIterator.is_valid返回false
        MergeIterator {
            iters: binary_heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|hw| hw.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // 先更新iters
        while let Some(mut iter) = self.iters.peek_mut() {
            // 如果和当前的current具有相同的key（即过时的key）
            if iter.1.key() == current.1.key() {
                if let Err(e) = iter.1.next() {
                    PeekMut::pop(iter);
                    return Err(e);
                }

                if !iter.1.is_valid() {
                    PeekMut::pop(iter);
                }
            } else {
                break;
            }
        }

        //  更新current
        if let Err(e) = current.1.next() {
            return Err(e);
        }

        if !current.1.is_valid() {
            self.current = self.iters.pop();
        } else {
            if let Some(mut iter) = self.iters.peek_mut() {
                // ! make sure current.index < iter.1.index
                if iter.1.key() < current.1.key()
                    || (iter.1.key() == current.1.key() && iter.0 < current.0)
                {
                    std::mem::swap(&mut *iter, current);
                }

                // ! follow way is better
                // if *iter > *current {
                //     std::mem::swap(&mut *iter, current);
                // }
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|iter| iter.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|iter| iter.1.num_active_iterators())
                .unwrap_or(0)
    }
}
