// use std::env

use std::{cell::RefCell, mem, ptr, rc::Rc};

// 'static const BLOCK_SIZE usize=4096;
const K_BLOCK_SIZE: usize = 4096;
pub struct Arena {
    ptr: *mut u8,
    bytes_remaining: usize,
    memory_usage: i64,
    blocks: Vec<Vec<u8>>,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            ptr: ptr::null_mut(),
            memory_usage: 0,
            blocks: Vec::new(),
            bytes_remaining: 0,
        }
    }
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        assert!(bytes > 0);
        //let bytes_remaining = self.bytes_remaining;//why not use bytes<self.remaining
        if bytes <= self.bytes_remaining {
            assert!(!self.ptr.is_null());
            let result = self.ptr;
            unsafe {
                self.ptr = self.ptr.offset(bytes as isize);
                self.bytes_remaining -= bytes;
                return result;
            }
        }
        self.alloc_fallback(bytes)
    }
    pub fn memory_usage(&self) -> i64 {
        self.memory_usage
    }
    fn alloc_new(&mut self, bytes: usize) -> *mut u8 {
        let mut v: Vec<u8> = Vec::with_capacity(bytes);
        unsafe {
            v.set_len(bytes);
            ptr::write_bytes(v.as_mut_ptr(), 0, bytes);
        }
        let result = v.as_mut_ptr();
        self.blocks.push(v);
        let memory_usage: i64 = self.memory_usage + bytes as i64;
        self.memory_usage = memory_usage;
        unsafe { mem::transmute(result) }
    }
    fn alloc_fallback(&mut self, bytes: usize) -> *mut u8 {
        if bytes > K_BLOCK_SIZE / 4 {
            return self.alloc_new(bytes);
        }
        self.ptr = self.alloc_new(K_BLOCK_SIZE);
        self.bytes_remaining = K_BLOCK_SIZE;
        let result = self.ptr;
        unsafe {
            self.ptr = self.ptr.offset(bytes as isize);
            self.bytes_remaining -= bytes;
        }
        result
    }
    pub fn allo_aligned(&mut self, bytes: usize) -> *mut u8 {
        let ptr_size = mem::size_of::<usize>();
        let align = if ptr_size > 8 { ptr_size } else { 8 };
        assert_eq!(align & (align - 1), 0, "size of ptr should be a power of 2");
        let current_mod: usize = (self.ptr as usize) & (ptr_size - 1);
        let slop = if current_mod == 0 {
            0
        } else {
            align - current_mod
        };
        let needed = bytes + slop;
        let mut result = self.ptr;
        if needed <= self.bytes_remaining {
            unsafe {
                let result = self.ptr.offset(slop as isize);
                self.ptr = self.ptr.offset(bytes as isize);
                self.bytes_remaining -= needed;
                return result;
            }
        } else {
            result = self.alloc_fallback(needed);
        }
        assert_eq!(
            result as usize & (align - 1),
            0,
            "size of ptr should be a power of 2"
        );
        result
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_new() {
        println!("test new fn");
        let arena = Arena::new();
        assert_eq!(arena.memory_usage, 0);
    }
    #[test]
    fn test_alloc_new() {
        let mut arena = Arena::new();
        let _ = arena.alloc_new(128);
        assert_eq!(arena.memory_usage(), 128);
        let _ = arena.alloc_new(256);
        assert_eq!(arena.memory_usage(), 128 + 256);
        println!("alloc new passed");
    }

    #[test]
    fn test_allocfallback() {
        let mut arena = Arena::new();
        let _ = arena.alloc(K_BLOCK_SIZE);
    }
    fn check_current_block(arena: &Arena, is_null: bool, bytes: usize) {
        assert_eq!(arena.ptr.is_null(), is_null);
        assert_eq!(arena.bytes_remaining, bytes);
    }
}
