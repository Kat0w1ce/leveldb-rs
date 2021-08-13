use super::ldbslice::{self, Slice};
use atomic::Atomic;
use bumpalo::{collections::Vec as arena, Bump};
use rand::rngs::StdRng;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::mem::{replace, size_of};
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::vec;
//thread-safe or not?
pub struct Skiplist {
    innerSkiplist: Rc<RefCell<innerSkiplist>>,
}

//todo use arena to align
struct node {
    key: ldbslice::Slice, //todo
    // value: ldbslice::Slice,
    // skips: Vec<Option<*mut node>>,
    skips: Vec<atomic::Atomic<Option<*mut node>>>,
    next: Option<Box<node>>,
}

impl node {
    //life circle ?
    fn New(key: Slice) -> node {
        node {
            key,
            skips: vec![],
            next: None,
        }
    }
}
struct innerSkiplist {
    head: Box<node>,
    rand: StdRng,
    max_height: AtomicUsize,
    len: AtomicUsize,
    comparator: Box<dyn super::comparator::comparator>, // arena
}
