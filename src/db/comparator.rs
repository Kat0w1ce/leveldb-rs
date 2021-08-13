use std::slice;

use super::ldbslice::Slice;

pub trait comparator {
    fn compare(&self, a: &Slice, b: &Slice) -> std::cmp::Ordering;

    //fn name()->String

    fn FindShortestSeparator(&self, start: &Slice, other: &Slice) -> Vec<u8>;

    /// Return the shortest byte string that compares "Greater" to the argument.
    fn FindShortSuccessor(&self, key: Slice) -> Slice;
}
