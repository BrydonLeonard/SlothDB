use crate::io::table;
use crate::io::table::TableErr;
use crate::lsm::kv::KV;
use crate::lsm::merge_iter::{ MergeIter, kv_merge, result_merge };
use std::collections::VecDeque;

struct LsmTree {
    name: String,
    levels: Vec<LsmLevel>,
}

struct LsmLevel {
    id: String,
    count: u32,
    tables: VecDeque<u32>,
    max_size: u32,  
}

fn kv_merge_iter<T>(l: T, r: T) -> MergeIter<T, Result<KV, TableErr>> 
    where T : Iterator<Item = Result<KV, TableErr>> {
    
    MergeIter::new(l, r, |l, r| { result_merge(l, r, kv_merge) })
}

const LEVEL_SCALING_FACTOR: u32 = 1;

impl LsmTree {
    /// The merge part of an LSM Tree. This is pretty inefficiently implemented for now, but
    /// it'll do the job.
    ///
    /// For the next implementation - this will all be much less confusing if the tables were
    /// stored in a struct with all of the methods in [table] hanging off of it.
    fn compact(&mut self) -> Result<(), TableErr> {
        for level_index in 0..self.levels.len() {
            if !self.levels[0].full() {
                return Ok(())
            }
           
            // We need another level
            if level_index + 1 >= self.levels.len() {
                self.add_level();
            }
            
            // A little confusing, admittedly, but return the two tables to be merged 
            // and create a new table in the next level to write to. Pass all of those
            // to the merger to actually perform the merge.
            let compaction_candidates = self.levels[0].oldest().expect("Couldn't pull oldest from the old level");
            let destination = self.levels[level_index + 1].new_table();

            let _ = table::merge_and_flush(&compaction_candidates.0, &compaction_candidates.1, &destination)?; 
            
            let _ = table::clean(&compaction_candidates.0)?;
            let _ = table::clean(&compaction_candidates.1)?;
        };
        
        Ok(())
    }

    fn add(&mut self, in_data: impl IntoIterator<Item = KV>) -> Result<(), TableErr> {
        if self.levels.len() == 0 {
            self.add_level();
        }
        let level = &mut self.levels[0];
        let new_table_name = level.new_table();

        table::flush(&new_table_name, in_data)
    }

    fn add_level(&mut self) {
        let new_index = self.levels.len();
        self.levels.push(LsmLevel { 
            id: format!("{}-{}", self.name, new_index),
            count: 0,
            tables: VecDeque::new(), 
            max_size: u32::try_from(new_index + 1).expect("Failed to convert usize -> u32") * LEVEL_SCALING_FACTOR 
        });
    }
}

/// Implemented by types that can read values for a key from _somewhere_
trait Scan {
    fn read(&self, key: &str) -> Result<String, TableErr>;
}

impl LsmLevel {
    fn table_names<'a>(&'a self) -> impl IntoIterator<Item = String> + 'a {
        let name = self.id.to_string();
        // Iterate backwards because we want to check the newest tables first
        self.tables.iter().map(move |index| { format!("{}-{}", name, index) })
    }

    fn full(&self) -> bool {
        self.tables.len() >= usize::try_from(self.max_size).expect("Failed to convert u32 -> usize")
    }

    fn oldest(&mut self) -> Result<(String, String), &'static str> {
        if self.tables.len() < 2 {
            return Err("Level is too small to compact from");
        }

        let first = self.tables.pop_front().expect("Failed to pop despite vec being large enough");
        let second = self.tables.pop_front().expect("Failed to pop despite vec being large enough");

        Ok((
            self.table_name(first),
            self.table_name(second),
        ))
    }

    fn new_table(&mut self) -> String {
        self.count += 1;
        self.tables.push_back(self.count);
        self.table_name(*self.tables.back().unwrap())
    }

    fn table_name(&self, index: u32) -> String {
        format!("{}-{}", self.id, index)
    }
}

impl Scan for LsmLevel {
    fn read(&self, key: &str) -> Result<String, TableErr> {
        for lsm_table in self.table_names() {
            if table::file_contains(&lsm_table, key)? {
                return table::read(&lsm_table, key);
            }
        }

        Err(TableErr::KeyNotFound)
    }
}

impl Scan for LsmTree {
    fn read(&self, key: &str) -> Result<String, TableErr> {
        for level in &self.levels {
            if let Ok(value) = level.read(key) {
                return Ok(value);
            }
        }
        
        Err(TableErr::KeyNotFound)
    }
}

#[cfg(test)]
mod test {
    use crate::lsm::tree::*;
    use std::fs;
    #[test]
    fn compacts() -> Result<(), TableErr> {
        let mut tree = LsmTree { 
            levels: Vec::new(),
            name: String::from("test_files/lsm_test"),
        };

        let _ = tree.add(vec![
                 KV { key: String::from("a"), value: 50.to_string() },
                 KV { key: String::from("c"), value: 10512.to_string() },
        ])?;

        let _ = tree.add(vec![
                 KV { key: String::from("b"), value: 12.to_string() },
                 KV { key: String::from("e"), value: 125.to_string() },
        ])?;

        let _ = tree.compact(); 

        let f = fs::read_to_string("test_files/lsm_test-1-1.data")?;

        assert_eq!("501210512125", f);

        Ok(())
    }
}
