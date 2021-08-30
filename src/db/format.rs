use crate::util::{
    coding::{self, decode_fixed_64, put_fixed_64, put_varint_32},
    comparator::{self, Comparator},
};
use integer_encoding::{self, FixedInt};
use std::{
    error::Error,
    fmt::{Debug, Formatter},
};

use super::SequenceNumber;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValueType {
    KTypeDeletion = 0,
    KTypeValue = 1,
}
impl From<u8> for ValueType {
    fn from(x: u8) -> Self {
        match x {
            0 => ValueType::KTypeDeletion,
            _ => ValueType::KTypeValue,
        }
    }
}
impl From<u64> for ValueType {
    fn from(x: u64) -> Self {
        match x & 0xff {
            0 => ValueType::KTypeDeletion,
            _ => ValueType::KTypeValue,
        }
    }
}
pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::KTypeValue;

pub const K_MAX_SEQUENCE_NUMBER: u64 = (1u64 << 56) - 1;

fn pack_sequence_and_type(seq: u64, value_type: ValueType) -> u64 {
    assert!(
        seq <= K_MAX_SEQUENCE_NUMBER,
        "[key seq] the sequence number should be <= {}, but got {}",
        K_MAX_SEQUENCE_NUMBER,
        seq
    );
    // assert!(t <= VALUE_TYPE_FOR_SEEK)
    seq << 8 | value_type as u64
}

fn append_Internal_key(dst: &mut Vec<u8>, key: &ParsedInteralKey) {
    dst.extend_from_slice(key.user_key);
    coding::put_fixed_64(dst, pack_sequence_and_type(key.sequence, key.value_type));
}

#[inline]
fn extract_user_key(key: &[u8]) -> &[u8] {
    let size = key.len();
    assert!(
        size >= 8,
        "[internal key] invalid size of internal key : expect >= {} but got {}",
        8,
        size
    );
    &key[..size - 8]
}
pub struct ParsedInteralKey<'a> {
    user_key: &'a [u8],
    sequence: SequenceNumber,
    value_type: ValueType,
}

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.

/// The format of `InternalKey`:
///
/// ```text
/// | ----------- n bytes ----------- | --- 7 bytes --- | - 1 byte - |
///              user key                  seq number        type
/// ```

// todo!
// lifecycle is  the main probelem for using &'a [u8]
#[derive(Default, Clone, PartialEq, Eq)]
pub struct InternalKey {
    rep: Vec<u8>,
}

impl<'a> Debug for ParsedInteralKey<'a> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{:?} @ {} : {:?}",
            self.user_key, self.sequence, self.value_type
        )
    }
}
impl<'a> ParsedInteralKey<'a> {
    pub fn new(key: &'a [u8], seq: SequenceNumber, value_type: ValueType) -> ParsedInteralKey<'a> {
        ParsedInteralKey {
            user_key: key,
            sequence: seq,
            value_type,
        }
    }
    pub fn from(internal_key: &'a InternalKey) -> ParsedInteralKey<'a> {
        internal_key.parse().unwrap()
    }
    pub fn decode_from(slice: &'a [u8]) -> Option<ParsedInteralKey<'a>> {
        let len = slice.len();
        if len < 8 {
            return None;
        }
        let num = decode_fixed_64(slice);
        let c = (num & 0xff) as u8;

        Some(ParsedInteralKey {
            user_key: &slice[..len - 8],
            sequence: num >> 8,
            value_type: ValueType::from(c),
        })
    }
    pub fn internal_key_encoding_length(&self) -> usize {
        self.user_key.len() + 8
    }
    pub fn user_key(&self) -> &str {
        std::str::from_utf8(&self.user_key).unwrap()
    }
}

impl InternalKey {
    // todo!()
    pub fn new(key: &[u8], seq: SequenceNumber, value_type: ValueType) -> Self {
        let mut v = Vec::from(key);
        put_fixed_64(&mut v, pack_sequence_and_type(seq, value_type));
        InternalKey::decode_from(v.as_slice())
    }

    pub fn decode_from(src: &[u8]) -> Self {
        //TODO: is there any way to avoid copy?
        InternalKey {
            rep: Vec::from(src),
        }
    }
    pub fn data(&self) -> &[u8] {
        &self.rep
    }
    pub fn encode(&self) -> &[u8] {
        self.rep.as_slice()
    }

    pub fn user_key(&self) -> &[u8] {
        assert!(self.rep.len() >= 8);
        &self.rep[0..self.rep.len() - 8]
    }

    pub fn clear(&mut self) {
        self.rep.clear();
    }
    pub fn parse(&self) -> Option<ParsedInteralKey<'_>> {
        let len = self.rep.len();
        if len < 8 {
            return None;
        }
        // let num = u64::decode_fixed(&self.rep[0..len - 8]);
        let num = coding::decode_fixed_64(&self.rep[len - 8..]);
        let c = (num & 0xff) as u8;

        Some(ParsedInteralKey {
            user_key: &self.rep[..len - 8],
            sequence: num >> 8,
            value_type: ValueType::from(c),
        })
    }
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
#[derive(Clone, Default)]
pub struct InternalKeyComparator<C: Comparator> {
    pub user_comparator: C,
}

impl<C: Comparator> InternalKeyComparator<C> {
    pub fn new(cmp: C) -> Self {
        InternalKeyComparator {
            user_comparator: cmp,
        }
    }
}

impl<C: Comparator + Clone> Comparator for InternalKeyComparator<C> {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        // Order by:
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)

        let ua = extract_user_key(a);
        let ub = extract_user_key(b);
        #[allow(clippy::comparison_chain)]
        match self.user_comparator.compare(ua, ub) {
            std::cmp::Ordering::Less => std::cmp::Ordering::Less,
            std::cmp::Ordering::Equal => {
                //extract sequence num
                let anum = coding::get_fixed_64(&a[a.len() - 8..]) >> 8;
                let bnum = coding::get_fixed_64(&b[b.len() - 8..]) >> 8;
                bnum.cmp(&anum)
            }
            _ => std::cmp::Ordering::Greater,
        }
    }
    fn name() -> String {
        String::from("leveldb.InternalKeyComparator")
    }

    // return a string which physically between start and limit
    //
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        let user_start = extract_user_key(start);
        let user_limit = extract_user_key(limit);
        // assert!(user_limit.len() >= user_start.len());
        let mut tmp = self
            .user_comparator
            .find_shortest_separator(user_start, user_limit);
        if tmp.len() < user_start.len()
            && self.user_comparator.compare(user_start, &tmp) == std::cmp::Ordering::Less
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            coding::put_fixed_64(
                &mut tmp,
                pack_sequence_and_type(K_MAX_SEQUENCE_NUMBER, ValueType::KTypeValue),
            );
            assert!(self.compare(start, &tmp) == std::cmp::Ordering::Less);
            assert!(self.compare(&tmp, limit) == std::cmp::Ordering::Less);
            return tmp;
        }
        start.to_owned()
    }

    // return a string > user_key
    // by writing biggest seq_num at the end of a string >=user_key
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        let ukey = extract_user_key(key);
        //get a string logically >= ukey
        let mut tmp = self.user_comparator.find_short_successor(ukey);
        if tmp.len() < ukey.len()
            && self.user_comparator.compare(ukey, &tmp) == std::cmp::Ordering::Less
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            coding::put_fixed_64(
                &mut tmp,
                pack_sequence_and_type(K_MAX_SEQUENCE_NUMBER, ValueType::KTypeValue),
            );
            tmp
        } else {
            key.to_owned()
        }
    }
}

// We construct a char array of the form:
//    klength  varint32               <-- start_
//    userkey  char[klength]          <-- kstart_
//    tag      uint64
//                                    <-- end_
// The array is a suitable MemTable key.
// The suffix starting with "userkey" can be used as an InternalKey.
pub struct LookUpKey {
    space: Vec<u8>,
    kstart: usize,
}

impl LookUpKey {
    pub fn memtable_key(&self) -> &[u8] {
        &self.space
    }
    pub fn internal_key(&self) -> &[u8] {
        &self.space[self.kstart..]
    }
    pub fn user_key(&self) -> &[u8] {
        &self.space[self.kstart..self.space.len() - 8]
    }
    pub fn new(user_key: &[u8], s: SequenceNumber) -> LookUpKey {
        let mut space = vec![];

        put_varint_32(&mut space, user_key.len() as u32 + 8);
        let kstart = space.len();
        space.extend_from_slice(user_key);
        put_fixed_64(&mut space, pack_sequence_and_type(s, ValueType::KTypeValue));
        LookUpKey { space, kstart }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::coding::*;
    use crate::util::comparator::BytewiseComparator;
    #[test]
    fn test_pack_seq_and_type() {
        let mut tests: Vec<(u64, ValueType, Vec<u8>)> = vec![
            (1, ValueType::KTypeValue, vec![1, 1, 0, 0, 0, 0, 0, 0]),
            (2, ValueType::KTypeDeletion, vec![0, 2, 0, 0, 0, 0, 0, 0]),
            (
                K_MAX_SEQUENCE_NUMBER,
                ValueType::KTypeDeletion,
                vec![0, 255, 255, 255, 255, 255, 255, 255],
            ),
        ];
        for (seq, t, expect) in tests.drain(..) {
            let u = decode_fixed_64(expect.as_slice());
            assert_eq!(pack_sequence_and_type(seq, t), u);
        }
    }
    #[test]
    #[should_panic]
    fn test_pack_seq_and_type_panic() {
        pack_sequence_and_type(1 << 56, ValueType::KTypeValue);
    }

    fn assert_encoded_decoded(key: &str, seq: u64, vt: ValueType) {
        let encoded = InternalKey::new(key.as_bytes(), seq, vt);
        assert_eq!(key.as_bytes(), encoded.user_key());
        let decoded = encoded.parse().expect("");
        assert_eq!(key, decoded.user_key());
        assert_eq!(seq, decoded.sequence);
        assert_eq!(vt, decoded.value_type);
    }

    #[test]
    fn test_internal_key_encode_decode() {
        let test_keys = ["", "k", "hello", "longggggggggggggggggggggg"];
        let test_seqs = [
            1,
            2,
            3,
            (1u64 << 8) - 1,
            1u64 << 8,
            (1u64 << 8) + 1,
            (1u64 << 16) - 1,
            1u64 << 16,
            (1u64 << 16) + 1,
            (1u64 << 32) - 1,
            1u64 << 32,
            (1u64 << 32) + 1,
        ];
        for i in 0..test_keys.len() {
            for j in 0..test_seqs.len() {
                assert_encoded_decoded(test_keys[i], test_seqs[j], ValueType::KTypeValue);
                assert_encoded_decoded(test_keys[i], test_seqs[j], ValueType::KTypeDeletion);
            }
        }
    }
    #[test]
    fn test_icmp_cmp() {
        use std::cmp::Ordering;
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        let tests = vec![
            (
                ("", 100, ValueType::KTypeValue),
                ("", 100, ValueType::KTypeValue),
                Ordering::Equal,
            ),
            (
                ("", 90, ValueType::KTypeValue),
                ("", 100, ValueType::KTypeValue),
                Ordering::Greater,
            ), // physically less but logically larger
            (
                ("", 90, ValueType::KTypeValue),
                ("", 90, ValueType::KTypeDeletion),
                Ordering::Equal,
            ), // Only cmp KTypeValue seq if the user keys are same
            (
                ("a", 90, ValueType::KTypeValue),
                ("b", 100, ValueType::KTypeValue),
                Ordering::Less,
            ),
        ];
        for (a, b, expected) in tests {
            let ka = InternalKey::new(a.0.as_bytes(), a.1, a.2);
            let kb = InternalKey::new(b.0.as_bytes(), b.1, b.2);
            assert_eq!(expected, icmp.compare(ka.data(), kb.data()));
        }
    }

    #[test]
    fn test_icmp_separator() {
        let tests = vec![
            // ukey are the same
            (
                ("foo", 100, ValueType::KTypeValue),
                ("foo", 99, ValueType::KTypeValue),
                ("foo", 100, ValueType::KTypeValue),
            ),
            (
                ("foo", 100, ValueType::KTypeValue),
                ("foo", 101, ValueType::KTypeValue),
                ("foo", 100, ValueType::KTypeValue),
            ),
            (
                ("foo", 100, ValueType::KTypeValue),
                ("foo", 100, ValueType::KTypeValue),
                ("foo", 100, ValueType::KTypeValue),
            ),
            // ukey are disordered
            (
                ("foo", 100, ValueType::KTypeValue),
                ("bar", 99, ValueType::KTypeValue),
                ("foo", 100, ValueType::KTypeValue),
            ),
            // ukey are different but correctly ordered
            (
                ("foo", 100, ValueType::KTypeValue),
                ("hello", 200, ValueType::KTypeValue),
                ("g", K_MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
            ),
            // When a's ukey is the prefix of b's
            (
                ("foo", 100, ValueType::KTypeValue),
                ("foobar", 200, ValueType::KTypeValue),
                ("foo", 100, ValueType::KTypeValue),
            ),
            // When b's ukey is the prefix of a's
            (
                ("foobar", 100, ValueType::KTypeValue),
                ("foo", 200, ValueType::KTypeValue),
                ("foobar", 100, ValueType::KTypeValue),
            ),
        ];
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        for (a, b, expected) in tests {
            let ka = InternalKey::new(a.0.as_bytes(), a.1, a.2);
            let kb = InternalKey::new(b.0.as_bytes(), b.1, b.2);
            assert_eq!(
                InternalKey::new(expected.0.as_bytes(), expected.1, expected.2).data(),
                icmp.find_shortest_separator(ka.data(), kb.data())
                    .as_slice()
            );
        }
    }

    #[test]
    fn test_icmp_successor() {
        let tests = vec![
            (
                (Vec::from("foo".as_bytes()), 100, ValueType::KTypeValue),
                (
                    Vec::from("g".as_bytes()),
                    K_MAX_SEQUENCE_NUMBER,
                    VALUE_TYPE_FOR_SEEK,
                ),
            ),
            (
                (vec![255u8, 255u8], 100, ValueType::KTypeValue),
                (vec![255u8, 255u8], 100, ValueType::KTypeValue),
            ),
        ];
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        for (k, expected) in tests {
            assert_eq!(
                icmp.find_short_successor(InternalKey::new(k.0.as_slice(), k.1, k.2).data()),
                InternalKey::new(expected.0.as_slice(), expected.1, expected.2).data()
            );
        }
    }
}
