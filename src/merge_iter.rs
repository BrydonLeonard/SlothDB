use std::iter::Peekable;

pub enum Which {
    Left,
    Right,
    None,
}

pub struct MergeIter<T, I> 
    where T : Iterator<Item = I> {
    l: Peekable<T>,
    r: Peekable<T>,
    comparator: fn(&I, &I) -> Which,
}

impl <T, I> Iterator for MergeIter<T, I> 
    where T : Iterator<Item = I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        let which = match (self.l.peek(), self.r.peek()) {
            (Some(left), Some(right)) => { (self.comparator)(left, right) },
            (Some(_), None) => Which::Left,
            (None, Some(_)) => Which::Right,
            (None, None) => Which::None,
        };

        match which {
            Which::Left => { self.l.next() },
            Which::Right => { self.r.next() },
            Which::None => { None },
        }
    }
}

impl <T, I> MergeIter<T, I>
    where T : Iterator<Item = I> {

    pub fn new(left: T, right: T, comparator: fn(&I, &I) -> Which) -> MergeIter<T, I> {
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
                if left < right {
                    Which::Left
                } else {
                    Which::Right
                }
            }
        }
    }

}

#[cfg(test)]
mod test {
    use crate::merge_iter::*;

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
