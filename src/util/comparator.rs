// use std::slice;

// use super::ldbslice::Slice;
use bytes::{buf::Limit, Bytes as Slice};
pub trait Comparator {
    // Three-way comparison.  Returns value:
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

    fn name() -> String;

    // If start < limit, return a string in [start,limit).
    // Simple comparator implementations may return with *start unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    fn find_shortest_separator(&self, start: &[u8], other: &[u8]) -> Vec<u8>;

    // return  a short string which  >= *key.
    // Simple comparator implementations may return with *key unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8>;
}

#[derive(Default, Clone)]
pub struct BytewiseComparator {}

impl Comparator for BytewiseComparator {
    fn name() -> String {
        String::from("leveldb.BytewiseComparator")
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        // Find length of common prefix
        let min_len = usize::min(start.len(), limit.len());
        let mut diff_index = 0;
        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }
        // if one is not prefix of another
        if diff_index < min_len {
            let diff_byte = start[diff_index];
            if diff_byte < 0xff as u8 && diff_byte + 1 < limit[diff_index] {
                let mut rst = vec![0; diff_index + 1];
                rst[0..=diff_index].copy_from_slice(&start[0..=diff_index]);
                *(rst.last_mut()).unwrap() += 1;
                return rst;
            }
        }
        start.to_owned()
    }

    //leveldb inplace
    // fn find_short_successor(&self, key: &mut [u8]) -> Vec<u8> {
    //     // Find first character that can be incremented
    //     for i in 0..key.len() {
    //         if key[i] != 0xff as u8 {
    //             key[i] += 1;

    //         }
    //     }
    //     vec![]
    // }

    // wickdb return a new vec

    /// Given a feasible key s, Successor returns feasible key k such that Compare(k,
    /// a) >= 0.
    /// If the key is a run of \xff, returns itself
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        // Find first character that can be incremented
        for i in 0..key.len() {
            let byte = key[i];
            if byte != 0xff {
                let mut rst = vec![0; i + 1];
                rst[0..=i].copy_from_slice(&key[0..=i]);
                *(rst.last_mut()).unwrap() += 1;
                return rst;
            }
        }
        key.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytewise_comparator_separator() {
        let mut tests = vec![
            ("", "1111", ""),
            ("1111", "", "1111"),
            ("1111", "111", "1111"),
            ("123", "1234", "123"),
            ("1234", "1234", "1234"),
            ("1", "2", "1"),
            ("1357", "2", "1357"),
            ("1111", "12345", "1111"),
            ("1111", "13345", "12"),
        ];
        let c = BytewiseComparator::default();
        for (a, b, expect) in tests.drain(..) {
            let res = c.find_shortest_separator(a.as_bytes(), b.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }
        // special 0xff case
        let a: Vec<u8> = vec![48, 255];
        let b: Vec<u8> = vec![48, 49, 50, 51];
        let res = c.find_shortest_separator(a.as_slice(), b.as_slice());
        assert_eq!(res, a);
    }

    #[test]
    fn test_bytewise_comparator_successor() {
        let mut tests = vec![("", ""), ("111", "2"), ("222", "3")];
        let c = BytewiseComparator::default();
        for (input, expect) in tests.drain(..) {
            let res = c.find_short_successor(input.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }
        // special 0xff case
        let mut corner_tests = vec![
            (vec![0xff, 0xff, 1], vec![255u8, 255u8, 2]),
            (vec![0xff, 0xff, 0xff], vec![255u8, 255u8, 255u8]),
        ];
        for (input, expect) in corner_tests.drain(..) {
            let res = c.find_short_successor(input.as_slice());
            assert_eq!(res, expect)
        }
    }
}
