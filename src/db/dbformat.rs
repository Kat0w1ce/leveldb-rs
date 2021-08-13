use super::{
    ldbslice::{self, Slice},
    SequenceNumber,
};
use integer_encoding::{FixedInt, FixedIntWriter, VarInt, VarIntWriter};
static kMaxSequenceNumber: SequenceNumber = 0x1 << 56 - 1;
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

// type ParsedInternelKey<'a> = (&'a ldbslice::Slice, SequenceNumber, ValueType);
// impl<'a> ParsedInternelKey<'a> {
//     pub fn New(user_key: &'a ldbslice::Slice) {}
// }

pub type InternelKey<'a> = &'a [u8];
pub type UserKey<'a> = &'a [u8];

// todo
pub fn ParseInternelKey(key: InternelKey) -> Option<(UserKey, SequenceNumber, ValueType)> {
    match key.len() {
        0 => Some((key, 0, ValueType::TypeDeletion)),
        x if x < 8 => None,
        _ => {
            let user_key = &key[0..key.len() - 8];
            let flag = &key[key.len() - 8..];
            let (valuetype, seq) = parse_key(FixedInt::decode_fixed(flag));
            Some((user_key, seq, valuetype))
        }
    }
}

pub fn parse_key(tag: u64) -> (ValueType, SequenceNumber) {
    let (sequence, valueType) = (tag >> 8 as u64, tag & 0xff as u64);

    match valueType {
        0 => (ValueType::TypeDeletion, sequence),
        _ => (ValueType::TypeValue, sequence),
    }
}
