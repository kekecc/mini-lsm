#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers
            .entry(ts)
            .and_modify(|entry| *entry += 1)
            .or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let count = self.readers.get_mut(&ts).unwrap();
        *count -= 1;

        if *count == 0 {
            self.readers.remove(&ts);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(ts, _)| *ts)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
