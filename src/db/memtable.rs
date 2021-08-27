use std::array;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::sync::Arc;

use crate::db::skiplist::SkipList;
use crate::util::arena::{self, ArenaTrait, BlockArena};
use crate::util::coding::{get_varint_32, put_fixed_64, put_varint_32, varint_length};
use crate::util::comparator::Comparator;

use super::format::{InternalKey, LookUpKey, ValueType};
use super::iterator::LevedbIterator;
use super::skiplist::SkipListIterator;
use super::SequenceNumber;
struct MemTable<C: Comparator> {
    key_comparator: C,
    refs: usize,
    table: Arc<SkipList<C, BlockArena>>,
}

impl<C: Comparator + Clone> MemTable<C> {
    pub fn new(c: C) -> Self {
        let a = arena::BlockArena::default();
        let table = Arc::new(SkipList::new(c.clone(), a));
        Self {
            key_comparator: c,
            refs: 0,
            table,
        }
    }
    pub fn refer(&mut self) {
        self.refs += 1;
    }

    pub fn unref(&mut self) {
        self.refs -= 1;
        assert!(self.refs >= 0, "ref should > 0");
        if self.refs <= 0 {
            std::mem::drop(self);
        }
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.table.as_ref().borrow().size()
    }

    pub fn add(&mut self, s: SequenceNumber, valueType: ValueType, key: &[u8], value: &[u8]) {
        // todo: use arena to allocate memory
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        let key_size = key.len();
        let val_size = value.len();
        let internal_key_size = key_size + 8;
        let encoded_len = varint_length(internal_key_size)
            + internal_key_size
            + varint_length(val_size)
            + val_size;
        let mut buf = vec![];
        put_varint_32(&mut buf, key_size as u32);
        // put InternalKey
        buf.extend_from_slice(key);
        put_fixed_64(&mut buf, (s << 8) | valueType as u64);

        //put value
        put_varint_32(&mut buf, val_size as u32);
        buf.extend_from_slice(value);
        // let a = self.table.as_ref();
        self.table.insert(buf);
    }

    pub fn get(&self, key: &LookUpKey) {
        let mem_key = key.memtable_key();
        let mut iter = SkipListIterator::new(Arc::clone(&self.table));
        iter.seek(mem_key);
        if iter.valid() {
            // entry format is:
            //    klength  varint32&
            //    userkey  char[klength]
            //    tag      uint64
            //    vlength  varint32
            //    value    char[vlength]
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            let entry = iter.key();
            let key_len = get_varint_32(&entry[..5]);
        }
        // None
    }
}
