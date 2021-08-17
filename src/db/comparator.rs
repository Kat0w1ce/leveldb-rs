// use std::slice;

// use super::ldbslice::Slice;
use bytes::Bytes as Slice;
pub trait Comparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

    //fn name()->String

    fn find_shortest_separator(&self, start: &Slice, other: &Slice) -> Vec<u8>;

    /// Return the shortest byte string that compares "Greater" to the argument.
    fn find_short_successor(&self, key: Slice) -> Slice;
}
