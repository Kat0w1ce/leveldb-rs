use crate::db::comparator::Comparator;
use crate::db::skiplist::SkipList;
use crate::util::arena::ArenaTrait;
struct memtable<C: Comparator, A: ArenaTrait> {
    key_comparator: C,
    arena: A,
    refs: usize,
    table: SkipList<C, A>,
}

impl<C: Comparator, A: ArenaTrait> memtable<C, A> {}
