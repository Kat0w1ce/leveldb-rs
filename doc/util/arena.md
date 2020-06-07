## arena.rs

内存分配模块，见leveldb/util/arena.h和leveldb/util/arena.cc

```c++
//c++结构
class Arena {
 public:

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  std::atomic<size_t> memory_usage_;
};
```



```rust
//rust结构
pub struct Arena {
    ptr: *mut u8,
    bytes_remaining: usize,
    memory_usage: i64,
    blocks: Vec<Vec<u8>>,
}
```

## AllocateNewBlock

1. 分配指定大小的内存
2. 将分配的内存压入Blocks:Vec<Vec<u8>>
3. 更新所用内存之和
4. //todo rust中的并发更新

## AllocateFallback

1. 如果需要的空间大于块大小的1/4，直接分配相应的大小。否则在新的块内分配

```c++
 if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }
```

## Allocate

1. 如果需要的空间<Block剩余空间，直接分配
2. 否则调用AllocateFallBack



## AllocateAligned

1. 对齐分配，大端对齐。计算请求内存大小需要的额外对齐空间，参照AllocateFallBack分配，类似于malloc
2. 







