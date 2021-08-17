use super::ldbslice::Slice;

pub trait LdbIterator {
    fn valid() -> bool;
    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    fn seek_to_first(&self);

    fn seek_to_last(&self);
    fn seek(&self, target: &Slice);

    fn next();

    fn key(&self) -> Slice;

    fn value(&self) -> Slice;

    //cleanupfunction
    // todo
}
