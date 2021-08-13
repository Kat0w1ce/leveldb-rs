//bytes(https://github.com/tokio-rs/bytes) could be a better solution

use std::{
    borrow::BorrowMut,
    ops::{Index, IndexMut, Range},
    vec,
};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Slice {
    data: Vec<u8>,
}

impl ToString for Slice {
    fn to_string(&self) -> String {
        // String::from_utf8(self.data.clone()).expect("find invalid utf8code")
        unsafe { String::from_utf8_unchecked(self.data.clone()) }
    }
}
//todo
//is there a possible way to convert vec[char] to vec[u8]
// impl From<Vec<char>> for Slice {
//     fn from(data: Vec<char>) -> Self {
//         let data = Vec::from_iter(data.into_iter().map(|x| x as u8));
//         Self { data }
//     }
// }

impl From<&str> for Slice {
    fn from(data: &str) -> Self {
        // let data = data.clone().as_bytes();
        let data = Vec::from(data);
        Self { data }
    }
}
impl From<String> for Slice {
    fn from(data: String) -> Self {
        let data = Vec::from(data);
        Self { data }
    }
}
impl From<Vec<u8>> for Slice {
    fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}
impl Index<usize> for Slice {
    type Output = u8;
    fn index(&self, index: usize) -> &Self::Output {
        assert!(index < self.data.len());
        &self.data[index]
    }
}
impl IndexMut<usize> for Slice {
    // type Output = u8;
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        assert!(index < self.data.len());
        self.data[index].borrow_mut()
    }
}
impl Slice {
    pub fn New() -> Slice {
        Slice { data: vec![] }
    }
    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn starts_with(&self, other: &Slice) -> bool {
        self.data.starts_with(&other.data)
    }
    pub fn clear(&mut self) {
        self.data.clear()
    }
    pub fn remove_prefix(mut self, n: usize) -> Self {
        assert!(n <= self.size());
        self.data.drain(0..n);
        self
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// impl PartialEq for Slice {
//     fn eq(&self, other: &Self) -> bool {
//         self.data.eq(&other.data)
//     }
// }

#[cfg(test)]
mod tests {
    use crate::db::ldbslice::Slice;
    #[test]
    fn test_to_string() {
        assert_eq!(Slice::from("hello").to_string(), String::from("hello"));
        assert_eq!(
            Slice::from(String::from("hello")).to_string(),
            String::from("hello")
        );
    }

    #[test]
    fn test_ord() {
        // assert!(Slice::from("hello").eq(&Slice::from("hello")));
        // assert_eq!(Slice::from("hell").eq(&Slice::from("hello")), false);
        assert_eq!(
            Slice::from(String::from("hello")) == Slice::from("hello"),
            true
        );
        assert!(Slice::from("4") > Slice::from(3.to_string()));
        assert!(Slice::from("abb") < Slice::from("cc"));
    }

    #[test]
    fn tests_remove_prefix() {
        assert!(Slice::from("hello").remove_prefix(4) == Slice::from("o"));
        assert!(Slice::from("hello").remove_prefix(5) == Slice::New());
    }

    #[test]
    fn test_index() {
        assert!(Slice::from("hello")[1] == 'e' as u8);
        assert!(Slice::from("hello")[2] == 'l' as u8);
    }
}
