use std::{cmp::Ordering, mem::size_of};

use proc_macro::tracked_env::var;

use crate::util::{
    coding::{put_fixed_32, put_varint_32},
    comparator::{BytewiseComparator, Comparator},
};

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
            restarts: vec![],
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
            put_fixed_32(&mut self.buffer, *i);
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
}
