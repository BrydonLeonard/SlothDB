use std::iter::Peekable;
use crate::lsm::kv::KV;
use crate::io::table::TableErr;

/// The decision that comes from the comparison of the elements of two iterators
/// being merged. Left and Right take a boolean that indicates whether the next 
/// value in the other iterator should be consumed as well. That's useful when
/// (for example) both have the same value, but only the left should be added 
/// to the next list. In such situations, the right value should be dropped 
/// entirely.
pub enum MergeDecision {
    Left(bool),
    Right(bool),
    None,
}

pub struct MergeIter<T, I> 
    where T : Iterator<Item = I> {
    l: Peekable<T>,
    r: Peekable<T>,
    comparator: fn(&I, &I) -> MergeDecision,
}

pub fn kv_merge(left: &KV, right: &KV) -> MergeDecision {
    if left.key < right.key {
        MergeDecision::Left(false)
    } else if left.key > right.key {
        MergeDecision::Right(false)
    } else {
        MergeDecision::Left(true)
    }
}

pub fn result_merge<T, E>(maybe_left: &Result<T, E>, maybe_right: &Result<T, E>, merger: fn(&T, &T) -> MergeDecision) -> MergeDecision {
    match (maybe_left, maybe_right) {
        (Err(_), _) => MergeDecision::Left(false),
        (_, Err(_)) => MergeDecision::Right(false),
        (Ok(left), Ok(right)) => (merger)(&left, &right),
    }
}

impl <T, I> Iterator for MergeIter<T, I> 
    where T : Iterator<Item = I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        let which = match (self.l.peek(), self.r.peek()) {
            (Some(left), Some(right)) => { (self.comparator)(left, right) },
            (Some(_), None) => MergeDecision::Left(false),
            (None, Some(_)) => MergeDecision::Right(false),
            (None, None) => MergeDecision::None,
        };

        match which {
            MergeDecision::Left(consume_right) => { 
                if consume_right {
                    self.r.next();
                }
                self.l.next() 
            },
            MergeDecision::Right(consume_left) => { 
                if consume_left {
                    self.l.next();
                }
                self.r.next()
            },
            MergeDecision::None => { None },
        }
    }
}

impl <T, I> MergeIter<T, I>
    where T : Iterator<Item = I> {

    pub fn new(left: T, right: T, comparator: fn(&I, &I) -> MergeDecision) -> MergeIter<T, I> {
        MergeIter {
            l: left.peekable(),
            r: right.peekable(),
            comparator,
        }
    }

    /// Uses the order of T
    pub fn default(left: T, right: T) -> MergeIter<T, I> 
        where I : PartialOrd {
        MergeIter {
            l: left.peekable(),
            r: right.peekable(),
            comparator: |left, right| { 
                if left <= right {
                    MergeDecision::Left(false)
                } else {
                    MergeDecision::Right(false)
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::lsm::merge_iter::*;
    #[test]
    fn merges() -> Result<(), &'static str> {
        let left: [i32; 3] = [1, 3, 5];
        let right: [i32; 3] = [2, 6, 8];
        let expected: [i32; 6] = [1, 2, 3, 5, 6, 8];

        let merged: Vec<&i32> = MergeIter::default(left.iter(), right.iter()).collect();


        let mut i = 0;
        for merged_val in merged {
            assert_eq!(expected[i], *merged_val);
            i += 1;
        }

        Ok(())
    }
}
