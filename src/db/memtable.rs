use crate::db::skiplist::SkipList;
use crate::util::arena::ArenaTrait;
use crate::util::comparator::Comparator;
struct memtable<C: Comparator, A: ArenaTrait> {
    key_comparator: C,
    arena: A,
    refs: usize,
    table: SkipList<C, A>,
}

impl<C: Comparator, A: ArenaTrait> memtable<C, A> {
    pub fn refer(&mut self) {
        self.refs += 1;
    }

    pub fn unref(&mut self) {
        self.refs -= 1;
        assert!(self.refs >= 0, "ref should > 0");
        if self.refs <= 0 {
            std::mem::drop(self);
        }
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.table.size()
    }
}
