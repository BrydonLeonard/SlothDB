use std::iter::Peekable;

pub struct MergeIter<T, I> 
    where 
        T : Iterator<Item = I>,
        I : PartialOrd {
    l: Peekable<T>,
    r: Peekable<T>,
}

impl <T, I> Iterator for MergeIter<T, I> 
    where 
        T : Iterator<Item = I>,
        I : PartialOrd {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_left = self.l.peek();
        let maybe_right = self.r.peek();

        if maybe_left == None && maybe_right == None {
            return None;
        }

        let Some(_) = maybe_left else { 
            return self.r.next();
        };

        let Some(_) = maybe_right else { 
            return self.l.next();
        };

        if self.l.peek() < self.r.peek() {
            return self.l.next();
        }

        return self.r.next();
    }
}

impl <T, I> MergeIter<T, I>
    where 
        T : Iterator<Item = I>,
        I : PartialOrd {

    fn new(left: T, right: T) -> MergeIter<T, I> {
        MergeIter {
            l: left.peekable(),
            r: right.peekable(),
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

        let merged: Vec<&i32> = MergeIter::new(left.iter(), right.iter()).collect();

        for i in 0..5 {
            assert_eq!(expected[i], *merged[i]);
        }

        Ok(())
    }
}
