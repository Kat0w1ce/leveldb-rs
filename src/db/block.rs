use std::{cmp::Ordering, mem::size_of, sync::Arc};

use crate::util::{
    coding::{
        decode_fixed_32, get_varint_32, get_varint_32_prefix_ptr, put_fixed_32, put_varint_32,
    },
    comparator::{BytewiseComparator, Comparator},
    status::Error,
};

use super::ldbiterator::LdbIterator;

const U32_LEN: usize = std::mem::size_of::<u32>();
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.
#[derive(Default)]
pub struct BlockBuilder<C: Comparator + Clone> {
    buffer: Vec<u8>,
    restarts: Vec<usize>,
    counter: usize,
    finished: bool,
    last_key: Vec<u8>,
    c: C,
    block_restart_interval: usize,
}

impl<C: Comparator + Clone> BlockBuilder<C> {
    pub fn new(n: usize, cmp: C) -> BlockBuilder<C> {
        BlockBuilder {
            c: cmp,
            block_restart_interval: n,
            buffer: vec![],
            restarts: vec![0; 1],
            counter: 0,
            finished: false,
            last_key: vec![],
        }
    }
}

impl<C: Comparator + Clone> BlockBuilder<C> {
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.finished = false;
        self.last_key.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn finish(&mut self) -> &[u8] {
        for i in &self.restarts {
            put_fixed_32(&mut self.buffer, *i as u32);
        }
        put_fixed_32(&mut self.buffer, self.restarts.len() as u32);
        self.finished = true;
        &self.buffer
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + size_of::<u32>() + size_of::<u32>() * self.restarts.len()
    }

    // An entry for a particular key-value pair has the form:
    //     shared_bytes: varint32
    //     unshared_bytes: varint32
    //     value_length: varint32
    //     key_delta: char[unshared_bytes]
    //     value: char[value_length]
    // shared_bytes == 0 for restart points.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let last_key_piece = &self.last_key[..];
        assert!(!self.finished);
        assert!(self.counter as usize <= self.block_restart_interval);
        assert!(self.buffer.is_empty() || self.c.compare(key, last_key_piece) == Ordering::Greater);
        let mut shared = 0;
        if self.counter < self.block_restart_interval {
            let min_len = usize::min(last_key_piece.len(), key.len());
            while shared < min_len && last_key_piece[shared] == key[shared] {
                shared += 1;
            }
        } else {
            // Restart compression
            self.restarts.push(self.buffer.len());
            self.counter = 0;
        }
        let not_shared = key.len() - shared;
        put_varint_32(&mut self.buffer, shared as u32);
        put_varint_32(&mut self.buffer, not_shared as u32);
        put_varint_32(&mut self.buffer, value.len() as u32);

        // Add string delta to buffer_ followed by value
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);

        self.last_key.resize(shared, 0);
        self.last_key.extend_from_slice(&key[shared..]);
        assert!(self.last_key.eq(key));
        self.counter += 1;
    }
}

pub struct Block {
    size: u32,
    data: Arc<Vec<u8>>,
    restart_offset: u32,
    owned: bool,
    // cmp: C,
}

impl Default for Block {
    fn default() -> Self {
        Block {
            size: 0,
            data: Arc::new(vec![]),
            restart_offset: 0,
            owned: true,
        }
    }
}
impl Block {
    #[inline]
    fn num_restarts(data: &[u8]) -> u32 {
        let size = data.len();
        decode_fixed_32(&data[size - size_of::<u32>()..])
    }
    //size:??
    pub fn new(data: Vec<u8>) -> Result<Self, Error> {
        let size = data.len();
        if size >= U32_LEN {
            let max_restarts_allowed = (size - U32_LEN) / U32_LEN;
            let num = Self::num_restarts(&data) as usize;
            if num <= max_restarts_allowed {
                let restart_offset = size - (1 + num) * U32_LEN;
                return Ok(Block {
                    data: Arc::new(data),
                    restart_offset: restart_offset as u32,
                    size: num as u32,
                    owned: true,
                });
            }
        }

        Err(Error::Corruption(
            "[block] read invalid block content".to_owned(),
        ))
    }

    pub fn iter<C: Comparator + Clone>(&self, c: C) -> BlockIterator<C> {
        BlockIterator::new(c, self.data.clone(), self.restart_offset, self.size)
    }
}

pub struct BlockIterator<C: Comparator + Clone> {
    cmp: C,
    data: Arc<Vec<u8>>,

    restarts: u32,      // restarts array starting offset
    nums_restarts: u32, // length of restarts array
    restart_index: u32, // current restart index

    key: Vec<u8>,
    err: Option<Error>, // status

    current: u32,    //current offset
    shared: u32,     // shared length
    not_shared: u32, // not shared length
    value_len: u32,  // value length
    key_offset: u32, // the offset of the current key in the block
}

impl<C: Comparator + Clone> LdbIterator for BlockIterator<C> {
    fn valid(&self) -> bool {
        self.err.is_none() && self.current < self.restarts
    }

    fn seek_to_first(&mut self) {
        todo!()
    }

    fn seek_to_last(&mut self) {
        todo!()
    }

    fn seek(&mut self, target: &[u8]) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn prev(&mut self) {
        todo!()
    }

    fn status(&self) {
        todo!()
    }
}

impl<C: Comparator + Clone> BlockIterator<C> {
    pub fn new(cmp: C, data: Arc<Vec<u8>>, restarts: u32, nums_restarts: u32) -> Self {
        Self {
            cmp,
            err: None,
            data,
            restarts,
            nums_restarts,
            restart_index: 0,
            current: restarts,
            shared: 0,
            not_shared: 0,
            value_len: 0,
            key_offset: 0,
            key: vec![],
        }
    }
    // decode an entry which starts from current
    fn decode_entry(&mut self) -> bool {
        if self.current >= self.restarts {
            self.current = self.restarts;
            self.restart_index = self.nums_restarts;
            return false;
        }

        let offset = self.current;
        let src = &self.data[offset as usize..];
        assert!(src.len() > 3);
        if src[0] | src[1] | src[2] > 128u8 {
            //fast path : all three values are encoded in one byte each
            self.shared = offset;
            self.not_shared = offset + 1;
            self.value_len = get_varint_32(&src[offset as usize + 2..]).unwrap().0;
            self.current += 3;
            return true;
        }
        let (shared, n0) = if let Some((_shared, _n0)) = get_varint_32_prefix_ptr(0, 0, src) {
            (_shared, _n0)
        } else {
            return false;
        };
        let (unshared, n1) =
            if let Some((_unshared, _n1)) = get_varint_32_prefix_ptr(0, 0, &src[n0..]) {
                (_unshared, _n1)
            } else {
                return false;
            };
        let (val_len, n2) =
            if let Some((_val_len, _n2)) = get_varint_32_prefix_ptr(0, 0, &src[n0 + n1..]) {
                (_val_len, _n2)
            } else {
                return false;
            };
        let n = (n1 + n2 + n0) as u32;
        if offset + n + val_len + unshared > self.restarts {
            self.corruption_error();
            return false;
        }
        self.key_offset = self.current + n;
        self.shared = shared;
        self.not_shared = unshared;
        self.value_len = val_len;
        let total_key_len = shared + unshared;
        self.key.resize(total_key_len as usize, 0);
        let delta =
            &self.data[self.key_offset as usize..(self.key_offset + self.not_shared) as usize];
        //decompress key

        for i in shared as usize..self.key.len() {
            self.key[i] = delta[i - shared as usize];
        }
        //update restart index
        while self.restart_index + 1 < self.nums_restarts
            && self.get_restart_point(self.restart_index + 1) < self.current
        {
            self.restart_index += 1;
        }
        true
    }
    fn corruption_error(&mut self) {
        self.err = Some(Error::Corruption("bad entry in block".to_owned()));
        self.key.clear();
        self.current = self.restarts;
        self.restart_index = self.nums_restarts;
    }
    #[inline]
    fn next_entry_offset(&self) -> u32 {
        self.key_offset + self.not_shared + self.value_len
    }
    #[inline]
    fn get_restart_point(&self, index: u32) -> u32 {
        assert!(index < self.nums_restarts);
        decode_fixed_32(&self.data[self.restarts as usize + (index as usize) << 2..])
    }
    #[inline]
    fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
    }
}

#[cfg(test)]
mod test {
    use crate::util::comparator::BytewiseComparator;

    use super::*;

    fn new_test_block() -> Vec<u8> {
        let mut samples = vec!["1", "12", "123", "abc", "abd", "acd", "bbb"];
        let mut builder = BlockBuilder::new(3, BytewiseComparator::default());
        for key in samples.drain(..) {
            builder.add(key.as_bytes(), key.as_bytes());
        }
        // restarts: [0, 18, 42]
        // entries data size: 51
        Vec::from(builder.finish())
    }

    #[test]
    fn test_corrupted_block() {
        // Invalid data size
        let res = Block::new(vec![0, 0, 0]);
        assert!(res.is_err());

        let mut data = vec![];
        let mut test_restarts = vec![0, 10, 20];
        let length = test_restarts.len() as u32;
        for restart in test_restarts.drain(..) {
            put_fixed_32(&mut data, restart);
        }
        // Append invalid length of restarts
        put_fixed_32(&mut data, length + 1);
        let res = Block::new(data);
        assert!(res.is_err());
    }

    #[test]
    fn test_new_empty_block() {
        let ucmp = BytewiseComparator::default();
        let ucmp2 = ucmp.clone();
        let mut builder = BlockBuilder::new(2, ucmp);
        let data = builder.finish();
        println!("{:?}", data);
        let length = data.len();
        let restarts_len = decode_fixed_32(&data[length - 4..length]);
        let restarts = &data[..length - 4];
        assert_eq!(restarts_len, 1);
        assert_eq!(restarts.len() as u32 / 4, restarts_len);
        assert_eq!(decode_fixed_32(restarts), 0);
        let block = Block::new(Vec::from(data)).unwrap();
        let iter = block.iter(ucmp2);
        assert!(!iter.valid());
    }

    #[test]
    fn test_new_block_from_bytes() {
        let data = new_test_block();
        assert_eq!(Block::num_restarts(&data), 3);
        let block = Block::new(data).unwrap();
        assert_eq!(block.restart_offset, 51);
    }

    #[test]
    fn test_simple_empty_key() {
        let ucmp = BytewiseComparator::default();
        let mut builder = BlockBuilder::new(2, ucmp);
        builder.add(b"", b"test");
        let data = builder.finish();
        let block = Block::new(Vec::from(data)).unwrap();
        let mut iter = block.iter(BytewiseComparator::default());
        iter.seek("".as_bytes());
        assert!(iter.valid());
        let k = iter.key();
        let v = iter.value();
        assert_eq!(std::str::from_utf8(k).unwrap(), "");
        assert_eq!(std::str::from_utf8(v).unwrap(), "test");
        iter.next();
        assert!(!iter.valid());
    }
}
