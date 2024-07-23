#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::KeyBytes;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut data = &buf[..];
        while data.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();

            let raw_len = data.get_u16();
            hasher.write_u16(raw_len);
            let key = Bytes::copy_from_slice(&data[..(raw_len - 8) as usize]);
            hasher.write(&key);
            let ts = (&data[(raw_len - 8) as usize..raw_len as usize]).get_u64();
            hasher.write_u64(ts);
            data.advance(raw_len as usize);

            let value_len = data.get_u16();
            hasher.write_u16(value_len);
            let value = Bytes::copy_from_slice(&data[..value_len as usize]);
            hasher.write(&value);
            data.advance(value_len as usize);

            let check_sum = data.get_u32();
            if check_sum != hasher.finalize() {
                panic!("check crc32 error!");
            }

            skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeyBytes, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();

        let mut buf: Vec<u8> = Vec::with_capacity(
            key.raw_len()
                + value.len()
                + 2 * std::mem::size_of::<u16>()
                + std::mem::size_of::<u32>(),
        );

        let mut hasher = crc32fast::Hasher::new();

        buf.put_u16(key.raw_len() as u16);
        hasher.write_u16(key.raw_len() as u16);
        buf.put_slice(key.key_ref());
        hasher.write(key.key_ref());
        buf.put_u64(key.ts());
        hasher.write_u64(key.ts());

        buf.put_u16(value.len() as u16);
        hasher.write_u16(value.len() as u16);
        buf.put_slice(value);
        hasher.write(value);

        let check_sum = hasher.finalize();
        buf.put_u32(check_sum);

        file.write_all(&buf)?;

        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
