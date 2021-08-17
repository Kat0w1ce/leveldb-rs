use std::cell::RefCell;
use std::mem;
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const BLOCK_SIZE: usize = 4096;

pub trait ArenaTrait {
    // return the start pointer to  an allocated memory of size bytes

    unsafe fn allocate<T>(&mut self, size: usize, align: usize) -> *mut T;

    fn memory_used(&self) -> usize;
}

struct OffsetArenaInner {
    used: AtomicUsize,
    cap: usize,
    ptr: *mut u8,
}

pub struct OffsetArena {
    arenaInner: Arc<OffsetArenaInner>,
}

// impl ArenaTrait for ArenaInner {
//     unsafe fn allocate<T>(&self, size: usize, align: usize) -> *mut T {
//         unsafe { ptr::null() as *mut T }
//     }
//     fn memory_used(&self) -> usize {
//         0
//     }
// }

// reference from  leveldb arena
pub struct BlockArena {
    alloc_ptr: *mut u8,
    alloc_bytes_remaining: usize,
    blocks: RefCell<Vec<Vec<u8>>>,
    // Total memory usage of the arena.
    // comments in leveldb
    // TODO(costan): This member is accessed via atomics, but the others are
    //               accessed without any locking. Is this OK?
    memory_usage: AtomicUsize,
}
impl Default for BlockArena {
    fn default() -> Self {
        BlockArena {
            alloc_ptr: ptr::null_mut(),
            alloc_bytes_remaining: 0,
            blocks: RefCell::new(vec![]),
            memory_usage: AtomicUsize::new(0),
        }
    }
}

impl BlockArena {
    fn allocate_new_block(&self, bytes: usize) -> *mut u8 {
        let mut new_block = vec![0; bytes];
        let p = new_block.as_mut_ptr();
        self.blocks.borrow_mut().push(new_block);
        self.memory_usage.fetch_add(bytes, Ordering::Relaxed);
        p
    }

    //katowizz TODO
    //&mut self might cause ownership problem
    fn allocate_fallback(&mut self, size: usize) -> *mut u8 {
        if size > BLOCK_SIZE >> 2 {
            // Object is more than a quarter of our block size.  Allocate it separately
            // to avoid wasting too much space in leftover bytes.
            return self.allocate_new_block(size);
        }
        let new_block_ptr = self.allocate_new_block(BLOCK_SIZE);

        unsafe {
            let ptr = new_block_ptr.add(size);
            self.alloc_ptr = ptr;
        }
        self.alloc_bytes_remaining = BLOCK_SIZE - size;
        new_block_ptr
    }

    //TODO
    //only support 64-bit system
    fn allocate_aligned(&mut self, bytes: usize, align: usize) -> *mut u8 {
        // A & (B-1) = A % B
        let current_mod = self.alloc_ptr as usize & (align - 1);
        let slop = if current_mod == 0 {
            0
        } else {
            align - current_mod
        };
        let needed = bytes + slop;
        if needed <= self.alloc_bytes_remaining {
            unsafe {
                let result = self.alloc_ptr.add(slop);
                self.alloc_ptr = self.alloc_ptr.add(needed);
                self.alloc_bytes_remaining -= needed;
                return result;
            }
        } else {
            self.allocate_fallback(bytes)
        }
    }
}

impl ArenaTrait for BlockArena {
    unsafe fn allocate<T>(&mut self, size: usize, align: usize) -> *mut T {
        self.allocate_aligned(size, align) as *mut T
    }

    #[inline]
    fn memory_used(&self) -> usize {
        self.memory_usage.load(Ordering::Acquire)
    }
}

#[cfg(test)]

mod tests {
    // use self::arena::{Arena, BlockArena, BLOCK_SIZE};
    use crate::util::arena::{ArenaTrait, BlockArena, BLOCK_SIZE};
    use rand::Rng;
    use std::ptr;
    use std::sync::atomic::Ordering;
    #[test]
    fn test_new_arena() {
        let a = BlockArena::default();
        assert_eq!(a.memory_used(), 0);
        assert_eq!(a.alloc_bytes_remaining, 0);
        assert_eq!(a.alloc_ptr, ptr::null_mut());
        assert_eq!(a.blocks.borrow().len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_allocate_empty_should_panic() {
        let mut a = BlockArena::default();
        unsafe { a.allocate::<u8>(0, 0) };
    }

    #[test]
    fn test_allocate_new_block() {
        let a = BlockArena::default();
        let mut expect_size = 0;
        for (i, size) in [1, 128, 256, 1000, 4096, 10000].iter().enumerate() {
            a.allocate_new_block(*size);
            expect_size += *size;
            assert_eq!(a.memory_used(), expect_size, "memory used should match");
            assert_eq!(
                a.blocks.borrow().len(),
                i + 1,
                "number of blocks should match"
            )
        }
    }
    #[test]
    fn test_allocate_fallback() {
        let mut a = BlockArena::default();
        assert_eq!(a.memory_used(), 0);
        a.allocate_fallback(1);
        assert_eq!(a.memory_used(), BLOCK_SIZE);
        assert_eq!(a.alloc_bytes_remaining, BLOCK_SIZE - 1);
        a.allocate_fallback(BLOCK_SIZE / 4 + 1);
        assert_eq!(a.memory_used(), BLOCK_SIZE + BLOCK_SIZE / 4 + 1);
    }

    #[test]
    fn test_allocate_mixed() {
        let mut a = BlockArena::default();
        let mut allocated = vec![];
        let mut allocated_size = 0;
        let n = 10000;
        let mut r = rand::thread_rng();
        for i in 0..n {
            let size = if i % (n / 10) == 0 {
                if i == 0 {
                    continue;
                }
                i
            } else {
                if i == 1 {
                    1
                } else {
                    r.gen_range(1, i)
                }
            };
            let ptr = unsafe { a.allocate::<u8>(size, 8) };
            unsafe {
                for j in 0..size {
                    let np = ptr.add(j);
                    (*np) = (j % 256) as u8;
                }
            }
            allocated_size += size;
            allocated.push((ptr, size));
            assert!(
                a.memory_used() >= allocated_size,
                "the memory used {} should be greater or equal to expecting allocated {}",
                a.memory_used(),
                allocated_size
            );
        }
        for (ptr, size) in allocated.iter() {
            unsafe {
                for i in 0..*size {
                    let p = ptr.add(i);
                    assert_eq!(*p, (i % 256) as u8);
                }
            }
        }
    }
}
