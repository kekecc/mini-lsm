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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
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

            let key_len = data.get_u16();
            hasher.write_u16(key_len);
            let key = Bytes::copy_from_slice(&data[..key_len as usize]);
            hasher.write(&key);
            data.advance(key_len as usize);

            let value_len = data.get_u16();
            hasher.write_u16(value_len);
            let value = Bytes::copy_from_slice(&data[..value_len as usize]);
            hasher.write(&value);
            data.advance(value_len as usize);

            let check_sum = data.get_u32();
            if check_sum != hasher.finalize() {
                panic!("check crc32 error!");
            }

            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();

        let mut buf: Vec<u8> = Vec::with_capacity(
            key.len() + value.len() + 2 * std::mem::size_of::<u16>() + std::mem::size_of::<u32>(),
        );

        let mut hasher = crc32fast::Hasher::new();

        buf.put_u16(key.len() as u16);
        hasher.write_u16(key.len() as u16);
        buf.put_slice(key);
        hasher.write(key);
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
