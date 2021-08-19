//unthread-safe-edtion
//d
// use super::ldbslice::{self, Slice};
use crate::util::arena::ArenaTrait;
use bytes::Bytes;
use rand::Rng;
use std::borrow::BorrowMut;

use std::mem::{self, replace, size_of};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

use super::iterator::LevedbIterator;
use crate::util::comparator::Comparator;

pub const MAX_HEIGHT: usize = 12;
use bytes::Bytes as Slice;
fn random_height() -> usize {
    rand::thread_rng().gen_range(1, MAX_HEIGHT)
}

#[derive(Debug)]
#[repr(C)]
struct Node {
    // The pointer and length pointing to the memory location
    key: Slice, //todo
    // value: ldbslice::Slice,
    height: usize,
    // skips: Vec<atomic::Atomic<Option<*mut node>>>,
    next_nodes: [AtomicPtr<Node>; 0],
}

impl Node {
    //life circle ?
    fn new<A: ArenaTrait>(key: Slice, height: usize, arena: &mut A) -> *const Self {
        let pointer_size = height * size_of::<AtomicPtr<Self>>();
        let size = size_of::<Self>() + pointer_size;
        let align = mem::align_of::<Self>();
        let p = unsafe { arena.allocate(size, align) } as *const Self as *mut Self;
        unsafe {
            let node = &mut *p;
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.height, height);
            ptr::write_bytes(node.next_nodes.as_mut_ptr(), 0, height)
        }
        p as *const Self
    }

    fn next(&self, height: usize) -> *mut Node {
        unsafe {
            self.next_nodes
                .get_unchecked(height - 1)
                .load(Ordering::Acquire)
        }
    }

    fn set_next(&self, height: usize, node: *mut Node) {
        unsafe {
            // self.next_nodes[height].store(node, Ordering::Release);
            self.next_nodes
                .get_unchecked(height - 1)
                .store(node, Ordering::Release);
        }
    }

    fn no_barrier_next(&self, height: usize) -> *mut Node {
        assert!(height > 0);
        unsafe {
            self.next_nodes
                .get_unchecked(height - 1)
                .load(Ordering::Relaxed)
        }
    }
    fn no_barrier_set_next(&self, height: usize, node: *mut Node) {
        unsafe {
            self.next_nodes
                .get_unchecked(height - 1)
                .store(node, Ordering::Relaxed);
        }
    }
    fn key(&self) -> &[u8] {
        self.key.as_ref()
    }
}

pub struct SkipList<C: Comparator, A: ArenaTrait> {
    max_height: AtomicUsize,
    head: *const Node,
    // rand: StdRng,
    // arena contains all the nodes data
    pub(super) arena: A,
    compare: C,
    // differs in leveldb and wickdb
    // Note:
    // We only alloc space for `Node` in arena without the content of `key`
    // (only `Bytes` which is pretty small).
    size: AtomicUsize,

    count: AtomicUsize,
}
impl<C: Comparator, A: ArenaTrait> SkipList<C, A> {
    pub fn new(c: C, mut arena: A) -> Self {
        let head = Node::new(Bytes::new(), MAX_HEIGHT, arena.borrow_mut());
        SkipList {
            max_height: AtomicUsize::new(1),
            head,
            arena,
            compare: c,
            size: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
        }
    }

    // Return the earliest node that comes at or after key.
    // Return nullptr if there is no such node.
    //
    // If prev is non-null, fills prev[level] with pointer to previous
    // node at "level" for every level in [0..max_height_-1].

    fn find_greater_or_equal(&self, key: &[u8], mut prev: Option<&mut [*const Node]>) -> *mut Node {
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head;
        loop {
            unsafe {
                let next = (*node).next(level);
                if self.key_is_after_node(key, next) {
                    node = next;
                } else {
                    if let Some(ref mut p) = prev {
                        p[level - 1] = node;
                    }
                    if level == 1 {
                        return next;
                    } else {
                        level -= 1;
                    }
                }
                // if self.key_is_less_than_or_equal(key, next) {
                //     if let Some(ref mut p) = prev {
                //         p[level - 1] = node;
                //     }
                //     if level == 1 {
                //         return next;
                //     } else {
                //         level -= 1;
                //     }
                // } else {
                //     node = next;
                // }
            }
        }
    }

    // Insert key into the list.
    // REQUIRES: nothing that compares equal to key is currently in the list.
    fn insert(&mut self, key: impl Into<Bytes>) {
        let key = key.into();
        let len = key.len();
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));
        if !node.is_null() {
            assert_ne!(
                unsafe { (*node).key().eq(&key) },
                true,
                "[SkipList] duplicate insertion [key={:?}] is not allowed",
                &key
            )
        }
        let new_height = random_height();
        let cur_max_height = self.max_height.load(Ordering::Acquire);
        if new_height > cur_max_height {
            for p in prev.iter_mut().take(new_height).skip(cur_max_height) {
                *p = self.head;
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (nullptr), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since nullptr sorts after all
            // keys.  In the latter case the reader will use the new node.
            self.max_height.store(new_height, Ordering::Relaxed);
        }

        let new_node = Node::new(key, new_height, &mut self.arena) as *mut Node;
        for i in 1..=new_height {
            unsafe {
                (*new_node).no_barrier_set_next(i, (*prev[i - 1]).next(i));
                (*prev[i - 1]).set_next(i, new_node as *mut Node);
            }
        }
        self.count.fetch_add(1, Ordering::SeqCst);
        self.size.fetch_add(len, Ordering::SeqCst);
    }

    // Return the latest node with a key < key.
    // Return head_ if there is no such node.
    fn find_less_than(&self, key: &[u8]) -> *const Node {
        let mut node = self.head;
        let mut level = self.max_height.load(Ordering::Acquire) - 1;
        loop {
            unsafe {
                let ord = self.compare.compare((*node).key(), key);
                assert!(node == self.head || ord == std::cmp::Ordering::Less);
                let next = (*node).next(level);
                match (
                    next.is_null()
                        || self.compare.compare((*next).key(), key) != std::cmp::Ordering::Less,
                    level == 1,
                ) {
                    (true, true) => return node,
                    (true, false) => level -= 1,
                    _ => node = next,
                }
            }
        }
    }

    // Return true if key is greater than the data stored in "n"
    fn key_is_after_node(&self, key: &[u8], node: *mut Node) -> bool {
        unsafe {
            !node.is_null()
                && self.compare.compare((*node).key(), key.as_ref()) == std::cmp::Ordering::Less
        }
    }
    fn contains(&self, key: &[u8]) -> bool {
        let p = self.find_greater_or_equal(key, None);
        let rst: bool = if !p.is_null() && unsafe { (*p).key().eq(key) } {
            true
        } else {
            false
        };
        rst
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }
    fn find_last(&self) -> *const Node {
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head;
        loop {
            let next = unsafe { (*node).next(level) };
            match (next.is_null(), level == 1) {
                (true, true) => return node,
                (true, false) => level -= 1,
                _ => node = next,
            }
        }
    }
    fn key_is_less_than_or_equal(&self, key: &[u8], node: *const Node) -> bool {
        if node.is_null() {
            true
        } else {
            let node_key = unsafe { (*node).key() };
            !matches!(
                self.compare.compare(key, node_key),
                std::cmp::Ordering::Greater
            )
        }
    }
}

pub struct SkipListIterator<C: Comparator, A: ArenaTrait> {
    skl: Arc<SkipList<C, A>>,
    node: *const Node,
}
impl<C: Comparator, A: ArenaTrait> SkipListIterator<C, A> {
    pub fn new(skl: Arc<SkipList<C, A>>) -> Self {
        Self {
            skl,
            node: ptr::null_mut(),
        }
    }

    // If the head is nullptr, this method will panic. Otherwise return true.
    #[inline]
    fn panic_valid(&self) -> bool {
        assert!(self.valid(), "[skl] invalid iterator head",);
        true
    }
}

impl<C: Comparator, A: ArenaTrait> LevedbIterator for SkipListIterator<C, A> {
    #[inline]
    fn valid(&self) -> bool {
        !self.node.is_null()
    }
    #[inline]
    fn seek(&mut self, target: &[u8]) {
        self.node = self.skl.find_greater_or_equal(target, None)
    }
    #[inline]
    fn next(&mut self) {
        unsafe {
            self.node = (*(self.node)).next(1);
        }
    }
    #[inline]
    fn seek_to_first(&mut self) {
        self.node = unsafe {
            (*(self.skl.head))
                .next_nodes
                .get_unchecked(0)
                .load(Ordering::Acquire)
        };
    }
    #[inline]
    fn seek_to_last(&mut self) {
        self.node = self.skl.find_last();
    }
    #[inline]
    fn prev(&mut self) {
        // self.panic_valid();
        let key = self.key();
        println!("key :{:?}", std::str::from_utf8(key));
        self.node = self.skl.find_less_than(key);
        unsafe {
            println!("{:?}", (*self.node).key());
        }
        if self.node == self.skl.head {
            self.node = ptr::null_mut();
        }
    }
    fn key(&self) -> &[u8] {
        unsafe { (*self.node).key().as_ref() }
    }
    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    fn status(&mut self) -> Result<(), std::fmt::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{random_height, Bytes, Node, Ordering, SkipList, SkipListIterator, MAX_HEIGHT};
    use crate::db::iterator::LevedbIterator;
    use crate::util::arena::BlockArena;
    use crate::util::comparator::BytewiseComparator;
    use std::borrow::BorrowMut;
    use std::ptr;
    use std::sync::Arc;
    fn new_test_skl() -> SkipList<BytewiseComparator, BlockArena> {
        SkipList::new(BytewiseComparator::default(), BlockArena::default())
    }

    fn construct_skl_from_nodes(
        nodes: Vec<(&str, usize)>,
    ) -> SkipList<BytewiseComparator, BlockArena> {
        if nodes.is_empty() {
            return new_test_skl();
        }
        let mut skl = new_test_skl();
        // just use MAX_HEIGHT as capacity because it's the largest value that node.height can have
        let mut prev_nodes = vec![skl.head; MAX_HEIGHT];
        let mut max_height = 1;
        for (key, height) in nodes {
            let n = Node::new(
                Bytes::copy_from_slice(key.as_bytes()),
                height,
                &mut skl.arena,
            );
            for (h, prev_node) in prev_nodes[0..height].iter().enumerate() {
                unsafe {
                    (**prev_node).set_next(h + 1, n as *mut Node);
                }
            }
            for i in 0..height {
                prev_nodes[i] = n;
            }
            if height > max_height {
                max_height = height;
            }
        }
        // must update max_height
        skl.max_height.store(max_height, Ordering::Release);
        skl
    }
    #[test]
    fn test_rand_height() {
        for _ in 0..100 {
            let height = random_height();
            assert_eq!(height < MAX_HEIGHT, true);
        }
    }
    #[test]
    fn test_key_is_less_than_or_equal() {
        let mut skl = new_test_skl();
        let key = vec![1u8, 2u8, 3u8];

        let tests = vec![
            (vec![1u8, 2u8], false),
            (vec![1u8, 2u8, 4u8], true),
            (vec![1u8, 2u8, 3u8], true),
        ];
        // nullptr should be considered as the largest
        assert_eq!(true, skl.key_is_less_than_or_equal(&key, ptr::null_mut()));

        for (node_key, expected) in tests {
            let node = Node::new(Bytes::from(node_key), 1, &mut skl.arena);
            assert_eq!(expected, skl.key_is_less_than_or_equal(&key, node))
        }
    }

    #[test]
    fn test_key_is_after_node() {
        let key: Bytes = Bytes::from("key2");
        let mut a = BlockArena::default();
        let skl = new_test_skl();
        let node1 = Node::new(Bytes::from("key1"), 12, &mut a) as *mut Node;
        assert_eq!(skl.key_is_after_node(&key, node1), true);
    }
    #[test]
    fn test_find_greater_or_equal() {
        let inputs = vec![
            ("key1", 5),
            ("key3", 1),
            ("key5", 2),
            ("key7", 4),
            ("key9", 3),
        ];

        let skl = construct_skl_from_nodes(inputs);
        let mut prev_nodes = vec![ptr::null(); 5];
        // test the scenario for un-inserted key
        let res = skl.find_greater_or_equal("key4".as_bytes(), Some(&mut prev_nodes));
        unsafe {
            assert_eq!((*res).key(), "key5".as_bytes());
            // prev_nodes should be correct
            assert_eq!((*(prev_nodes[0])).key(), "key3".as_bytes());
            for node in prev_nodes[1..5].iter() {
                assert_eq!((**node).key(), "key1".as_bytes());
            }
        }
        prev_nodes = vec![ptr::null(); 5];
        // test the scenario for inserted key
        let res2 = skl.find_greater_or_equal("key5".as_bytes(), Some(&mut prev_nodes));
        unsafe {
            assert_eq!((*res2).key(), "key5".as_bytes());
            // prev_nodes should be correct
            assert_eq!((*(prev_nodes[0])).key(), "key3".as_bytes());
            for node in prev_nodes[1..5].iter() {
                assert_eq!((**node).key(), "key1".as_bytes());
            }
        }
    }
    #[test]
    fn test_contains() {
        let inputs = vec![
            ("key1", 5),
            ("key3", 1),
            ("key5", 2),
            ("key7", 4),
            ("key9", 3),
        ];
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key4");
        let key3 = Bytes::from("keys");
        let skl = construct_skl_from_nodes(inputs);
        assert!(skl.contains(&key1));
        assert!(!skl.contains(&key2));
        assert!(!skl.contains(&key3));
    }

    #[test]
    fn test_find_last() {
        let inputs = vec![
            ("key1", 5),
            ("key3", 1),
            ("key5", 2),
            ("key7", 4),
            ("key9", 3),
        ];
        let skl = construct_skl_from_nodes(inputs);
        let node = skl.find_last();
        println!("{:?}", *&node);
        unsafe {
            println!("{:?}", (*node).key().to_vec());
            assert_eq!(&Bytes::from("key9").to_vec(), (*node).key())
        }
    }

    #[test]
    #[should_panic]
    fn test_duplicate_insert_should_panic() {
        let inputs = vec!["key1", "key1"];
        let mut skl = new_test_skl();
        for key in inputs {
            skl.insert(key.as_bytes());
        }
    }
    #[test]
    fn test_empty_skiplist_iterator() {
        let skl = new_test_skl();
        let iter = SkipListIterator::new(Arc::new(skl));
        assert!(!iter.valid());
    }
    #[test]
    fn test_insert() {
        let inputs = vec!["key1", "key3", "key5", "key7", "key9"];
        let mut skl = new_test_skl();
        for key in inputs.clone() {
            skl.insert(key.as_bytes());
        }

        let mut node = skl.head;
        for input_key in inputs {
            unsafe {
                let next = (*node).next(1);
                let key = (*next).key();
                assert_eq!(key, input_key.as_bytes());
                node = next;
            }
        }
        unsafe {
            // should be the last node
            assert_eq!((*node).next(1), ptr::null_mut());
        }
    }

    #[test]
    fn test_skiplist_basic() {
        use std::str;
        let mut skl = new_test_skl();
        let inputs = vec!["key1", "key11", "key13", "key3", "key5", "key7", "key9"];
        for key in inputs.clone() {
            skl.insert(key.as_bytes())
        }
        let mut iter = SkipListIterator::new(Arc::new(skl));
        assert_eq!(ptr::null(), iter.node);

        iter.seek_to_first();
        assert_eq!("key1", std::str::from_utf8(iter.key()).unwrap());
        for key in inputs.clone() {
            if !iter.valid() {
                break;
            }
            assert_eq!(key, std::str::from_utf8(iter.key()).unwrap());
            iter.next();
        }
        assert!(!iter.valid());

        iter.seek_to_first();
        iter.next();
        iter.prev();
        assert_eq!(inputs[0], str::from_utf8(iter.key()).unwrap());
        iter.seek_to_first();
        iter.seek_to_last();
        for key in inputs.into_iter().rev() {
            if !iter.valid() {
                break;
            }
            assert_eq!(key, str::from_utf8(iter.key()).unwrap());
            iter.prev();
        }
        assert!(!iter.valid());
        iter.seek("key7".as_bytes());
        assert_eq!("key7", str::from_utf8(iter.key()).unwrap());
        iter.seek("key4".as_bytes());
        assert_eq!("key5", str::from_utf8(iter.key()).unwrap());
        iter.seek("".as_bytes());
        assert_eq!("key1", str::from_utf8(iter.key()).unwrap());
        iter.seek("llllllllllllllll".as_bytes());
        assert!(!iter.valid());
    }
}
