use super::ldbslice::Slice;

pub trait LdbIterator {
    fn valid(&self) -> bool;
    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    fn seek_to_first(&mut self);

    fn seek_to_last(&mut self);
    fn seek(&mut self, target: &[u8]);

    fn next(&mut self);

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];
    fn prev(&mut self);
    fn status(&self);
}
