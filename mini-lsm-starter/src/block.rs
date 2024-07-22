#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::get_u16_from_data;
pub use iterator::BlockIterator;

use crate::key::KeyVec;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
    pub(crate) first_key: KeyVec,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = self.data.clone();
        let num_of_elements = self.offsets.len();

        for offset in &self.offsets {
            bytes.put_u16(*offset);
        }

        bytes.put_u16(num_of_elements as u16);

        bytes.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let chunk_size = 2;
        let offsets_end = data.len() - chunk_size;
        let num_of_elements = (&data[offsets_end..]).get_u16();

        let offsets_start = data.len() - chunk_size - (chunk_size * (num_of_elements as usize));
        let offsets: Vec<u16> = (&data[offsets_start..offsets_end])
            .chunks(chunk_size)
            .map(|mut offset_chunk| offset_chunk.get_u16())
            .collect();

        let mut buf = &data[0..offsets_start];
        buf.get_u16(); // overlap len = 0
        let first_key_len = buf.get_u16();
        let first_key = &data[chunk_size * 2..chunk_size * 2 + first_key_len as usize];
        let ts = (&data[chunk_size * 2 + first_key_len as usize..]).get_u64();
        let data = (&data[0..offsets_start]).to_vec();

        Self {
            data,
            offsets,
            first_key: KeyVec::from_vec_with_ts(first_key.into(), ts),
        }
    }
}
