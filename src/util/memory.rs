pub fn memcpy(src: *const u8, dst: *mut u8, len: usize) {
    unsafe { std::ptr::copy_nonoverlapping(src, dst, len) }
}
extern "C" {
    #[inline]
    pub fn memcmp(s1: *const u8, s2: *const u8, len: usize) -> i32;
}
