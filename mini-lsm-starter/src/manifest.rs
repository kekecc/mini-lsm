#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Ok, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut records: Vec<ManifestRecord> = Vec::new();
        let mut buf = data.as_slice();

        while buf.has_remaining() {
            let record_len = buf.get_u64();
            let record_data = &buf[..record_len as usize];
            let record = serde_json::from_slice::<ManifestRecord>(record_data)?;
            buf.advance(record_len as usize);

            let check_sum = buf.get_u32();
            if crc32fast::hash(record_data) != check_sum {
                panic!("check crc32 error!");
            }

            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut record = serde_json::to_vec(&record)?;
        let crc32 = crc32fast::hash(&record);

        file.write_all(&(record.len() as u64).to_be_bytes())?;
        record.put_u32(crc32);
        file.write_all(&record)?;
        file.sync_all()?;
        Ok(())
    }
}
