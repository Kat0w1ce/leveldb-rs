use std::array;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::sync::Arc;

// use crate::db::skiplist::SkipList;
use crate::db::inlineskiplist::{InlineSkipList, InlineSkiplistIterator};
use crate::util::arena::{self, ArenaTrait, BlockArena, OffsetArena};
use crate::util::coding::*;
use crate::util::comparator::Comparator;
use crate::util::status::Error;

use super::format::{InternalKey, InternalKeyComparator, LookUpKey, ValueType};
use super::iterator::{self, LevedbIterator};
use super::ldbiterator::LdbIterator;
use super::skiplist::SkipListIterator;
use super::SequenceNumber;

// convert mem key to internalkey before compare

#[derive(Default, Clone)]
pub struct KeyComparator<C: Comparator + Clone> {
    icmp: InternalKeyComparator<C>,
}

impl<C: Comparator + Clone> Comparator for KeyComparator<C> {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let ia = extract_length_prefixed_slice(a);
        let ib = extract_length_prefixed_slice(b);
        if ia.is_empty() || ib.is_empty() {
            ia.cmp(&ib)
        } else {
            self.icmp.compare(ia, ib)
        }
    }
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let ia = extract_length_prefixed_slice(key);
        self.icmp.find_short_successor(ia)
    }
    fn find_shortest_separator(&self, start: &[u8], other: &[u8]) -> Vec<u8> {
        let ia = extract_length_prefixed_slice(start);
        let ib = extract_length_prefixed_slice(other);
        self.icmp.find_shortest_separator(start, other)
    }
    fn name() -> String {
        String::from("KeyComparator")
    }
}

// impl<C: Comparator> KeyComparator<C> {
//     fn new(c: InternalKeyComparator<C>) -> Self {
//         KeyComparator { icmp: c }
//     }
// }
struct MemTable<C: Comparator + Clone> {
    key_comparator: KeyComparator<C>,
    refs: usize,
    table: InlineSkipList<KeyComparator<C>, OffsetArena>,
}

impl<C: Comparator + Clone> MemTable<C> {
    pub fn new(c: InternalKeyComparator<C>, max_mem_size: usize) -> Self {
        let arena = OffsetArena::with_capacity(max_mem_size);
        let ic = KeyComparator { icmp: c };
        let a = arena::BlockArena::default();
        let table = InlineSkipList::new(ic.clone(), arena);
        // let table = Arc::new(SkipList::new(ic, a));
        Self {
            key_comparator: ic,
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
    pub fn iter(&self) -> MemTableIterator<C> {
        MemTableIterator::new(self.table.clone())
    }
    pub fn approximate_memory_usage(&self) -> usize {
        self.table.total_size()
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
        put_varint_32(&mut buf, internal_key_size as u32);
        // put InternalKey
        buf.extend_from_slice(key);
        put_fixed_64(&mut buf, (s << 8) | valueType as u64);

        //put value
        put_varint_32(&mut buf, val_size as u32);
        buf.extend_from_slice(value);

        //addtional lock needed
        //dead lock?
        self.table.insert(buf);
    }

    /// If memtable contains a value for key, returns it in `Some(Ok())`.
    /// If memtable contains a deletion for key, returns `Some(Err(Status::NotFound))` .
    /// If memtable does not contain the key, return `None`
    pub fn get(&self, key: &LookUpKey) -> Option<Result<Vec<u8>, Error>> {
        let mem_key = key.memtable_key();
        let mut iter = InlineSkiplistIterator::new(self.table.clone());
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

            let (klen, size) = get_varint_32_prefix_ptr(0, 5, entry).unwrap();
            let ikey_len = size + klen as usize;
            let user_key = &entry[size..ikey_len - 8];
            let val = &entry[ikey_len..];
            match self
                .key_comparator
                .icmp
                .user_comparator
                .compare(user_key, key.user_key())
            {
                std::cmp::Ordering::Equal => {
                    let tag = decode_fixed_64(&entry[ikey_len - 8..ikey_len]);
                    match ValueType::from(tag) {
                        ValueType::KTypeDeletion => return Some(Err(Error::NotFound(None))),
                        ValueType::KTypeValue => {
                            return Some(Ok(get_length_prefixed_slice(val).unwrap().to_vec()))
                        }
                    }
                }
                _ => return None,
            }
        }
        None
    }
}

pub struct MemTableIterator<C: Comparator + Clone> {
    iter: InlineSkiplistIterator<KeyComparator<C>, OffsetArena>,
    tmp: Vec<u8>,
}

impl<C: Comparator + Clone> MemTableIterator<C> {
    pub fn new(table: InlineSkipList<KeyComparator<C>, OffsetArena>) -> Self {
        let iter = InlineSkiplistIterator::new(table);
        Self { iter, tmp: vec![] }
    }
}
impl<C: Comparator + Clone> LdbIterator for MemTableIterator<C> {
    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }
    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }

    fn next(&mut self) {
        self.iter.next();
    }
    fn valid(&self) -> bool {
        self.iter.valid()
    }
    fn key(&self) -> &[u8] {
        let key = self.iter.key();
        extract_length_prefixed_slice(key)
    }
    fn value(&self) -> &[u8] {
        let key = self.iter.key();
        get_varint_32_prefix_ptr(0, 0, &key)
            .and_then(|(len, size)| {
                Some(extract_length_prefixed_slice(&key[len as usize + size..]))
            })
            .unwrap()
    }
    fn seek(&mut self, target: &[u8]) {
        self.tmp.clear();
        put_length_prefixed_slice(&mut self.tmp, target);
        self.iter.seek(&self.tmp);
    }
    fn prev(&mut self) {
        self.iter.prev();
    }
    fn status(&self) {
        unimplemented!()
    }
}
#[cfg(test)]
mod tests {
    use super::MemTable;
    use crate::db::format::LookUpKey;
    use crate::db::format::ParsedInteralKey;
    use crate::db::format::*;
    use crate::db::ldbiterator::LdbIterator;
    use crate::util::comparator::BytewiseComparator;
    use std::str;
    fn new_mem_table() -> MemTable<BytewiseComparator> {
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        MemTable::new(icmp, 1 << 32)
    }

    fn add_test_data_set(memtable: &mut MemTable<BytewiseComparator>) -> Vec<(&str, &str)> {
        let tests = vec![
            (2, ValueType::KTypeValue, "boo", "boo"),
            (4, ValueType::KTypeValue, "foo", "val3"),
            (3, ValueType::KTypeDeletion, "foo", ""),
            (2, ValueType::KTypeValue, "foo", "val2"),
            (1, ValueType::KTypeValue, "foo", "val1"),
        ];
        let mut results = vec![];
        for (seq, t, key, value) in tests.clone().drain(..) {
            memtable.add(seq, t, key.as_bytes(), value.as_bytes());
            results.push((key, value));
        }
        results
    }

    #[test]
    fn test_memtable_add_get() {
        let mut memtable = new_mem_table();
        memtable.add(1, ValueType::KTypeValue, b"foo", b"val1");
        memtable.add(2, ValueType::KTypeValue, b"foo", b"val2");
        memtable.add(3, ValueType::KTypeDeletion, b"foo", b"");
        memtable.add(4, ValueType::KTypeValue, b"foo", b"val3");
        memtable.add(2, ValueType::KTypeValue, b"boo", b"boo");

        let v = memtable.get(&LookUpKey::new(b"null", 10));
        assert!(v.is_none());
        let v = memtable.get(&LookUpKey::new(b"foo", 10));
        assert_eq!(b"val3", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookUpKey::new(b"foo", 0));
        assert!(v.is_none());
        let v = memtable.get(&LookUpKey::new(b"foo", 1));
        assert_eq!(b"val1", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookUpKey::new(b"foo", 3));
        assert!(v.unwrap().is_err());
        let v = memtable.get(&LookUpKey::new(b"boo", 3));
        assert_eq!(b"boo", v.unwrap().unwrap().as_slice());
    }

    #[test]
    fn test_memtable_iter() {
        let mut memtable = new_mem_table();
        let mut iter = memtable.iter();
        assert!(!iter.valid());
        let entries = add_test_data_set(&mut memtable);
        // Forward scan
        iter.seek_to_first();
        assert!(iter.valid());
        for (key, value) in entries.iter() {
            let k = iter.key();
            let pkey = ParsedInteralKey::decode_from(k).unwrap();
            assert_eq!(
                pkey.user_key(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.user_key()
            );
            assert_eq!(
                str::from_utf8(iter.value()).unwrap(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                str::from_utf8(iter.value()).unwrap()
            );
            iter.next();
        }
        assert!(!iter.valid());

        // Backward scan
        iter.seek_to_last();
        assert!(iter.valid());
        for (key, value) in entries.iter().rev() {
            let k = iter.key();
            let pkey = ParsedInteralKey::decode_from(k).unwrap();
            assert_eq!(
                pkey.user_key(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.user_key()
            );
            assert_eq!(
                str::from_utf8(iter.value()).unwrap(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                str::from_utf8(iter.value()).unwrap()
            );
            iter.prev();
        }
        assert!(!iter.valid());
    }
}
