#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

const SIZEOF_U16: usize = std::mem::size_of::<u16>();
const SIZEOF_U32: usize = std::mem::size_of::<u32>();
const SIZEOF_U64: usize = std::mem::size_of::<u64>();

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
            let body_len = data.get_u32() as usize;

            let mut batch_data = &data[..body_len];
            data.advance(body_len);

            let crc32 = crc32fast::hash(batch_data);
            while batch_data.has_remaining() {
                let key_len = batch_data.get_u16() as usize;
                let key = Bytes::copy_from_slice(&batch_data[..key_len]);
                batch_data.advance(key_len);
                let ts = batch_data.get_u64();

                let value_len = batch_data.get_u16() as usize;
                let value = Bytes::copy_from_slice(&batch_data[..value_len]);
                batch_data.advance(value_len);

                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }

            let check_sum = data.get_u32();
            if check_sum != crc32 {
                panic!("check crc32 error!");
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut body_len = 0;

        for (key, value) in data.iter() {
            // key len and value len
            body_len += SIZEOF_U16 * 2;
            // ts
            body_len += SIZEOF_U64;
            // key and value
            body_len += key.key_len();
            body_len += value.len();
        }

        let mut buf = Vec::with_capacity(SIZEOF_U32 + body_len + SIZEOF_U32);

        buf.put_u32(body_len as u32);
        for (key, value) in data.iter() {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(&key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(&value);
        }
        let check_sum = crc32fast::hash(&buf[SIZEOF_U32..]);

        buf.put_u32(check_sum);
        file.write_all(&buf)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
