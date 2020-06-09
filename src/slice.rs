use super::util::memory::memcmp;
use std::{cmp::Ordering, ops::Index, ptr, slice};
pub struct Slice {
    data: *const u8,
    size: usize,
}

impl Slice {
    pub fn new_empty() -> Self {
        Self {
            data: ptr::null(),
            size: 0,
        }
    }

    pub fn new(data: *const u8, size: usize) -> Self {
        Self {
            data: data,
            size: size,
        }
    }
    #[inline]
    pub fn data(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.size) }
    }
    #[inline]
    pub fn raw_data(&self) -> *const u8 {
        self.data
    }
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }
    #[inline]
    pub fn clear(&mut self) {
        self.data = ptr::null_mut();
        self.size = 0;
    }
    fn remove_prefix(&mut self, n: usize) {
        assert!(n < self.size());
        unsafe {
            self.data = self.data.offset(n as isize);
        }
        self.size -= n;
    }
    #[inline]
    pub fn to_str(&self) -> &str {
        unsafe { ::std::str::from_utf8_unchecked(self.data()) }
    }
    #[inline]
    pub fn to_string(&self) -> String {
        // unsafe { String::from_raw_parts(self.data as *mut u8, self.size(), self.size()) }
        self.to_str().to_string()
    }
    pub fn strat_with<'a>(&self, other: &'a Slice) -> bool {
        other.size() <= self.size() && unsafe { memcmp(self.data, other.data, other.size()) == 0 }
    }
    #[inline]
    pub fn compare(&self, other: &Slice) -> Ordering {
        let min_size = if self.size() < other.size() {
            self.size()
        } else {
            other.size()
        };
        let r = unsafe { memcmp(self.data, other.data, min_size) };
        match r {
            _ if r < 0 => Ordering::Less,
            _ if r > 0 => Ordering::Greater,
            0 if self.size() < other.size() => Ordering::Less,
            0 if self.size() > other.size() => Ordering::Greater,
            other => Ordering::Equal,
        }
    }
}

impl From<String> for Slice {
    #[inline]
    fn from(s: String) -> Self {
        Slice::new(s.as_ptr(), s.len())
    }
}
impl<'a> From<&'a str> for Slice {
    fn from(s: &'a str) -> Self {
        Slice::new(s.as_ptr(), s.len())
    }
}
impl Index<usize> for Slice {
    type Output = u8;
    fn index(&self, i: usize) -> &Self::Output {
        assert!(i < self.size());
        unsafe { &*self.data.offset(i as isize) }
    }
}
impl PartialEq for Slice {
    fn eq(&self, other: &Slice) -> bool {
        self.compare(other) == Ordering::Equal
    }
}

impl Eq for Slice {}
#[cfg(test)]
mod test_slice {
    use super::*;
    #[test]
    fn test_from_string() {
        let s = String::from("fuck");
        let slice = Slice::from(s);
        // println!("{:?}", s.to_string())
        assert_eq!(slice.to_string(), String::from("fuck"));
    }

    #[test]
    fn test_index() {
        let s = String::from("fuck");
        let slice = Slice::from(s.as_str());
        assert_eq!(slice[1] as char, 'u');
    }
    #[test]
    fn test_from_str() {
        let s = Slice::from("fuck");
        assert_eq!(s.to_str(), "fuck");
    }
    #[test]
    fn test_cmp() {
        let s1 = Slice::from("fuck");
        let s2 = Slice::from("fuck");
        let s3 = Slice::from("f");
        let s4 = Slice::from("fuck you");
        let s5 = Slice::from("guck");
        let s6 = Slice::from("c");
        assert!(s1 == s2);
        assert_eq!(s1.compare(&s3), Ordering::Greater);
        assert_eq!(s1.compare(&s4), Ordering::Less);
        assert_eq!(s1.compare(&s5), Ordering::Less);
        assert_eq!(s1.compare(&s6), Ordering::Greater);
    }
    #[test]
    //test how tp convert data out of function
    fn test_ptr() {
        let mut s = vec![0, 1, 2, 3].as_ptr();
        let v = vec![0, 1, 2, 3];

        let p = v.as_ptr();
        {
            let s = p;
        }
        unsafe {
            for i in 0..v.len() {
                assert_eq!(*s.add(i), i)
            }
        }
    }
}
